#!/usr/bin/env python3
"""When each Slurm executor gets its next GPU: one remote call reads the whole GPU partition (every user's jobs, priority
components, partition and per-user QOS limits), a local simulation of Slurm's scheduling predicts when each queued job starts.
Every run keeps the raw snapshot, the predictions and the starts and ends it saw in the state dir (proto.ETA/<partition>/), and
first compares the last prediction with what happened; --backtest reruns the current method on every kept snapshot.
Executors sharing a partition and a hostname pattern are one cluster: read once, through the first one reachable.

  taskq site eta [machine...]   read, predict, keep, compare
  taskq site eta --record       read, predict, keep; print nothing (for a periodic job that builds the history)
  taskq site eta --backtest     rerun the current method on every kept snapshot against the actual starts
  taskq site eta --replay F     rerun one kept snapshot (raw/*.txt.gz), keep nothing
"""
import argparse
import datetime as dt
import gzip
import json
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "queue"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cfg  # noqa: E402
cfg.use_config()
import proto  # noqa: E402
import mtab as M  # noqa: E402

METHOD = 2  # raise by one whenever the simulation rules change
HORIZON = 14 * 86400
MIN_ENDS = 3
FETCH_TIMEOUT = 180
F = proto.ETA_FILES


# ---------- machines ----------

def clusters(only=()):
    """{partition: dict(sites={name: label}, via=[names])} for Slurm executors with a GPU partition and ssh."""
    out = {}
    for name in M.names("slurm"):
        m = M.machine(name)
        part = ((m.get("jobs") or {}).get("gpu") or {}).get("partition")
        if not part or not m.get("ssh") or (only and name not in only):
            continue
        c = out.setdefault((part, m.get("hostname")), dict(partition=part, sites={}))
        c["sites"][name] = m.get("label") or name
    return {c["partition"]: c for c in out.values()}


def home(part=None):
    p = cfg.state(proto.ETA, *([part] if part else []))
    os.makedirs(p, exist_ok=True)
    return p


def load_json(d, name, default):
    p = os.path.join(d, F[name])
    return json.load(open(p)) if os.path.exists(p) else default


def save_json(d, name, obj):
    tmp = os.path.join(d, F[name] + ".tmp")
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, os.path.join(d, F[name]))


def reachable(name):
    import relay_remote as RR
    return not RR.master_down(M.machine(name))


def rsh(name, cmd):
    import relay_remote as RR
    rc, out, err = RR.rsh(name, cmd, timeout=FETCH_TIMEOUT)
    return rc, out, err


def site_users(sites):
    """{machine: Slurm user name}, cached in users.json; read once per machine with `echo $USER`."""
    d = home()
    cache = load_json(d, "users", {})
    for name in sites:
        if name not in cache and reachable(name):
            rc, out, _ = rsh(name, "echo $USER")
            if rc == 0 and out.strip():
                cache[name] = out.strip().splitlines()[-1]
    save_json(d, "users", cache)
    return cache


def fetch_cmd(partition):
    p = partition
    return (f"date '+%s %z'; echo @@JOBS; scontrol show job -o | grep -E 'Partition=\\S*{p}' || true; "
            f"echo @@SPRIO; sprio -h -p {p} -o '%i|%u|%Y|%A'; "
            f"echo @@PART; scontrol show partition {p} -o; "
            f"echo @@QOS; sacctmgr -nP show qos format=name,maxtrespu; "
            f"echo @@CONF; scontrol show config | grep -E '^(PriorityWeightAge|PriorityMaxAge|SchedulerParameters) '")


def fetch(name, partition):
    rc, out, err = rsh(name, fetch_cmd(partition))
    if rc != 0 or "@@CONF" not in out:
        sys.exit(f"taskq site eta: reading {partition} through {name} failed: {(err or out).strip()[-500:]}")
    return out


# ---------- parsing ----------

def sections(raw):
    out, cur = {"HEAD": []}, "HEAD"
    for line in raw.splitlines():
        if line.startswith("@@"):
            cur = line[2:].strip()
            out[cur] = []
        elif line.strip():
            out[cur].append(line)
    return out


def kv(line):
    d = {}
    for k, v in re.findall(r"(?:^|\s)([A-Za-z][\w:/]*)=(\S*)", line):
        d.setdefault(k, v)
    return d


def dur(s):
    """Slurm duration ([days-]hh:mm:ss, mm:ss, mm) → seconds; unlimited or invalid → None."""
    if not s or s in ("UNLIMITED", "INVALID", "Partition_Limit"):
        return None
    days = 0
    if "-" in s:
        d, s = s.split("-", 1)
        days = int(d)
    p = [int(x) for x in s.split(":")]
    if len(p) == 3:
        sec = p[0] * 3600 + p[1] * 60 + p[2]
    elif len(p) == 2:
        sec = (p[0] * 3600 + p[1] * 60) if days else (p[0] * 60 + p[1])
    else:
        sec = p[0] * 60
    return days * 86400 + sec


def fmt_limit(sec):
    sec = sec or 0
    return f"{sec / 60:.0f} min" if sec < 3600 else f"{sec / 3600:.0f} h"


def mem_g(v):
    m = re.match(r"([\d.]+)([KMGT]?)", v or "")
    if not m:
        return 0.0
    return float(m.group(1)) * {"K": 1 / 2**20, "M": 1 / 1024, "": 1 / 1024, "G": 1, "T": 1024}[m.group(2)]


def tres(s):
    out = {"cpu": 0, "mem": 0.0, "gpu": 0}
    for part in (s or "").split(","):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        if k == "cpu":
            out["cpu"] = int(v)
        elif k == "mem":
            out["mem"] = mem_g(v)
        elif k == "gres/gpu":
            out["gpu"] = int(v)
    return out


def expand_nodes(s):
    m = re.match(r"^([^\[]+)\[([^\]]+)\]$", s)
    if not m:
        return s.split(",")
    out = []
    for part in m.group(2).split(","):
        if "-" in part:
            a, b = part.split("-")
            out += [f"{m.group(1)}{i:0{len(a)}d}" for i in range(int(a), int(b) + 1)]
        else:
            out.append(m.group(1) + part)
    return out


def parse(raw):
    sec = sections(raw)
    now_s, tz = sec["HEAD"][0].split()
    now = int(now_s)
    off = (1 if tz[0] == "+" else -1) * (int(tz[1:3]) * 3600 + int(tz[3:5]) * 60)

    def ts(s):
        if not s or s in ("Unknown", "None", "(null)"):
            return None
        return int(dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=dt.timezone.utc).timestamp()) - off

    part = kv(sec["PART"][0])
    nodes = expand_nodes(part["Nodes"])
    ptres = tres(part["TRES"])
    n = int(part["TotalNodes"])
    cap = {"cpu": ptres["cpu"] // n, "mem": ptres["mem"] / n, "gpu": ptres["gpu"] // n}

    qos = {}
    for line in sec.get("QOS", []):
        name, lim = (line.split("|") + [""])[:2]
        m = re.search(r"gres/gpu=(\d+)", lim)
        qos[name] = int(m.group(1)) if m else None

    conf = dict(re.split(r"\s*=\s*", ln.strip(), maxsplit=1) for ln in sec.get("CONF", []))
    age_w = float(conf.get("PriorityWeightAge", 0))
    max_age = dur(conf.get("PriorityMaxAge", "7-00:00:00")) or 7 * 86400

    sprio = {}
    for line in sec.get("SPRIO", []):
        f = [x.strip() for x in line.split("|")]
        if len(f) >= 4 and f[0].isdigit():
            sprio[f[0]] = dict(user=f[1], prio=float(f[2]), age=float(f[3]))

    jobs = []
    for line in sec.get("JOBS", []):
        d = kv(line)
        state = d.get("JobState")
        if state not in ("RUNNING", "PENDING", "COMPLETING"):
            continue
        j = dict(id=d["JobId"], user=d.get("UserId", "?").split("(")[0], state=state, qos=d.get("QOS"),
                 reason=d.get("Reason"), dep=d.get("Dependency"), limit=dur(d.get("TimeLimit")),
                 submit=ts(d.get("SubmitTime")), eligible=ts(d.get("EligibleTime")),
                 start=ts(d.get("StartTime")), end=ts(d.get("EndTime")), node=d.get("NodeList") or None,
                 prio=float(d.get("Priority", 0)))
        if state == "PENDING":
            j.update(tres(d.get("ReqTRES")))
            j["cpu"] += j["cpu"] % 2  # hyperthreads are allocated in pairs
            j["slurm_est"] = j["start"] if (j["start"] or 0) > now else None
            s = sprio.get(j["id"])
            j["base"], j["age"] = (s["prio"] - s["age"], s["age"]) if s else (j["prio"], 0.0)
        else:
            j.update(tres(d.get("AllocTRES")))
        jobs.append(j)
    bf = re.search(r"bf_min_age_reserve=(\d+)", conf.get("SchedulerParameters", ""))
    return dict(now=now, nodes=nodes, cap=cap, qos=qos, part_qos=part.get("QoS"), age_w=age_w, max_age=max_age, jobs=jobs,
                bf_min_age=60 * int(bf.group(1)) if bf else 0)


# ---------- simulation ----------

HELD = {"JobHeldUser", "JobHeldAdmin", "BeginTime", "ReqNodeNotAvail", "PartitionDown", "InvalidQOS",
        "AssocMaxJobsLimit", "QOSMaxWallDurationPerJobLimit"}


def est_ends(snap, ends):
    """End time of running jobs: among this user's ended jobs seen before the snapshot, those that ran longer than this one has
    so far; the median share of their time limit they used. Fewer than MIN_ENDS records: run to the limit."""
    by = {}
    for r in ends.values():
        if r.get("start") and r.get("limit_end", 0) > r["start"] and r["end_before"] <= snap["now"]:
            by.setdefault(r["user"], []).append(((r["end_after"] + r["end_before"]) / 2 - r["start"], r["limit_end"] - r["start"]))
    for j in snap["jobs"]:
        recs = by.get(j["user"], [])
        if j["state"] == "PENDING" or not j["start"] or not j["end"] or len(recs) < MIN_ENDS:
            continue
        longer = [d / lim for d, lim in recs if d > snap["now"] - j["start"]]
        if longer:
            j["est_end"] = min(j["end"], j["start"] + statistics.median(longer) * (j["end"] - j["start"]))


def user_limit(snap, qos):
    ls = [x for x in (snap["qos"].get(qos), snap["qos"].get(snap["part_qos"])) if x is not None]
    return min(ls) if ls else None


def simulate(snap):
    """→ (pending jobs with sim_start / sim_node / sim_prio where they start within HORIZON, timeline of starts)."""
    now, cap = snap["now"], snap["cap"]
    free = {n: dict(cap) for n in snap["nodes"]}
    usage = {}
    running, pending, started, ended = [], [], {}, {}
    for j in snap["jobs"]:
        if j["state"] == "PENDING":
            pending.append(dict(j))
            continue
        r = dict(j)
        r["end"] = max(r.get("est_end") or r["end"] or now + 60, now + 60)
        if r["node"] in free:
            for k in ("cpu", "mem", "gpu"):
                free[r["node"]][k] -= r[k]
        usage[r["user"]] = usage.get(r["user"], 0) + r["gpu"]
        running.append(r)
        started[r["id"]] = r["start"]
    known = {j["id"] for j in snap["jobs"]}

    def limit(j):
        x = user_limit(snap, j["qos"])
        return 10**6 if x is None else x

    def dep_ready(j):
        d = j["dep"]
        if not d or d == "(null)":
            return True
        for item in re.split(r"[,?]", d):
            m = re.match(r"(after\w*):([\d:+_]+)\((\w+)\)", item)
            if not m:
                return False
            if m.group(3) == "fulfilled":
                continue
            for jid in re.findall(r"\d+", m.group(2).split("+")[0]):
                if jid not in known:
                    return False
                if m.group(1) == "after":
                    if jid not in started:
                        return False
                elif jid not in ended:
                    return False
        return True

    def eligible_at(j, t):
        if j["reason"] in HELD and not (j["reason"] == "BeginTime" and (j["eligible"] or 0) <= t):
            return None
        if not dep_ready(j):
            return None
        if "elig_t" not in j:
            j["elig_t"] = t if j["dep"] and j["dep"] != "(null)" else (j["eligible"] or t)
        return j["elig_t"]

    def prio(j, t):
        if j["age"] or j.get("elig_t") is None:
            age = min(snap["age_w"], j["age"] + snap["age_w"] * (t - now) / snap["max_age"])
        else:
            age = min(snap["age_w"], snap["age_w"] * max(0, t - j["elig_t"]) / snap["max_age"])
        return j["base"] + age

    def fit(j):
        ok = [n for n, f in free.items() if f["gpu"] >= j["gpu"] and f["cpu"] >= j["cpu"] and f["mem"] >= j["mem"] - 1e-6]
        return min(ok, key=lambda n: (free[n]["gpu"], free[n]["cpu"])) if ok else None

    def shadow_time(j):
        f = {n: dict(v) for n, v in free.items()}
        for r in sorted(running, key=lambda r: r["end"]):
            if r["node"] in f:
                for k in ("cpu", "mem", "gpu"):
                    f[r["node"]][k] += r[k]
            if any(v["gpu"] >= j["gpu"] and v["cpu"] >= j["cpu"] and v["mem"] >= j["mem"] for v in f.values()):
                return r["end"]
        return None

    timeline = []
    t = now
    while True:
        freed = []
        for r in [r for r in running if r["end"] <= t]:
            running.remove(r)
            ended[r["id"]] = r["end"]
            freed.append(r)
            if r["node"] in free:
                for k in ("cpu", "mem", "gpu"):
                    free[r["node"]][k] += r[k]
            usage[r["user"]] -= r["gpu"]
        cands = [j for j in pending if "sim_start" not in j and eligible_at(j, t) is not None]
        cands.sort(key=lambda j: (-prio(j, t), j["submit"] or 0, int(j["id"])))
        shadow = None
        for j in cands:
            if j["gpu"] and usage.get(j["user"], 0) + j["gpu"] > limit(j):
                continue
            node = fit(j)
            if node and (shadow is None or t + (j["limit"] or HORIZON) <= shadow):
                j.update(sim_start=t, sim_node=node, sim_prio=prio(j, t))
                for k in ("cpu", "mem", "gpu"):
                    free[node][k] -= j[k]
                usage[j["user"]] = usage.get(j["user"], 0) + j["gpu"]
                running.append(dict(j, end=t + (j["limit"] or HORIZON), node=node))
                started[j["id"]] = t
                gave = [r for r in freed if r["gpu"]][:1] if j["gpu"] else []
                timeline.append(dict(t=t, job=j, freed=gave))
                freed = [r for r in freed if r not in gave]
            elif not node and (t - (j["elig_t"] or t)) >= snap["bf_min_age"]:
                s = shadow_time(j)
                if s is not None:
                    shadow = s if shadow is None else min(shadow, s)
        nxt = [r["end"] for r in running if r["end"] > t]
        if not nxt or min(nxt) > now + HORIZON:
            break
        t = min(nxt)
    return pending, timeline


# ---------- history ----------

def record(d, raw, snap, pending):
    """Keep the snapshot and predictions; update the starts, running and ended jobs seen. → jobs that ended since the last run."""
    os.makedirs(os.path.join(d, F["raw"]), exist_ok=True)
    with gzip.open(os.path.join(d, F["raw"], f"{snap['now']}.txt.gz"), "wt") as f:
        f.write(raw)
    with open(os.path.join(d, F["pred"]), "a") as f:
        f.write(json.dumps(dict(t=snap["now"], method=METHOD, pred={j["id"]: j.get("sim_start") for j in pending},
                                slurm={j["id"]: j["slurm_est"] for j in pending if j["slurm_est"]})) + "\n")
    starts, seen, ends = load_json(d, "starts", {}), load_json(d, "running", {}), load_json(d, "ends", {})
    cur = {}
    for j in snap["jobs"]:
        if j["state"] != "PENDING":
            starts.setdefault(j["id"], j["start"])
            cur[j["id"]] = dict(user=j["user"], start=j["start"], limit_end=j["end"], seen=snap["now"])
    gone = []
    for jid, r in seen.items():
        if jid not in cur and jid not in ends:
            ends[jid] = dict(r, end_after=r["seen"], end_before=snap["now"])
            gone.append(ends[jid])
    save_json(d, "starts", starts)
    save_json(d, "running", cur)
    save_json(d, "ends", ends)
    base = load_json(d, "base", {})
    for j in pending:
        if j["age"] or j["base"]:
            base[j["user"]] = dict(base=max(j["base"], 0), seen=snap["now"])
    save_json(d, "base", base)
    return gone


def predictions(d):
    p = os.path.join(d, F["pred"])
    return [json.loads(ln) for ln in open(p)] if os.path.exists(p) else []


def evaluate(starts, preds):
    """For each started job with a prediction, the latest one made 1-48 hours before it started: (job, made at, predicted,
    actual, method)."""
    out = []
    for jid, actual in starts.items():
        if actual is None:
            continue
        best = None
        for p in preds:
            v = p["pred"].get(jid)
            if v and 3600 <= actual - p["t"] <= 48 * 3600 and (best is None or p["t"] > best[0]):
                best = (p["t"], v, p.get("method"))
        if best:
            out.append((jid, best[0], best[1], actual, best[2]))
    return out


def err_summary(rows):
    if not rows:
        return "nothing to compare yet"
    e = [(a - p) / 3600 for _, _, p, a, _ in rows]
    return (f"{len(e)} jobs, actual - predicted median {statistics.median(e):+.1f} h, "
            f"absolute error median {statistics.median(abs(x) for x in e):.1f} h, {sum(abs(x) <= 2 for x in e)} within ±2 h")


# ---------- report ----------

def when(t, now):
    if t is None:
        return "not within two weeks (or waiting on a dependency or held)"
    return f"{dt.datetime.fromtimestamp(t):%m-%d %H:%M} (in {(t - now) / 3600:.1f} h)"


def hm(t):
    return f"{dt.datetime.fromtimestamp(t):%m-%d %H:%M}"


def report(d, part, snap, pending, timeline, sites, users, gone=None, prev=None):
    now = snap["now"]
    ours = {users[s]: s for s in sites if s in users}
    running = [j for j in snap["jobs"] if j["state"] != "PENDING"]
    total = snap["cap"]["gpu"] * len(snap["nodes"])
    print(f"{part} {hm(now)}: {total} GPUs, {sum(j['gpu'] for j in running)} in use, "
          f"{sum(1 for j in pending if j['gpu'])} queued GPU jobs (method v{METHOD}; assumes running jobs end at the share of "
          f"their limit that user's earlier jobs used (to the limit with fewer than {MIN_ENDS} records) and nobody submits more)")
    firsts = []
    for user, site in ours.items():
        mine_r = [j for j in running if j["user"] == user]
        mine_p = sorted([j for j in pending if j["user"] == user], key=lambda j: j.get("sim_start") or 1e18)
        lims = [x for x in (user_limit(snap, j["qos"]) for j in mine_r + mine_p) if x is not None]
        lim = min(lims) if lims else None
        r_txt = ", ".join(f"{j['id']} until {hm(j['end'])}" for j in mine_r) or "none"
        print(f"\n{sites[site]} ({user}): {sum(j['gpu'] for j in mine_r)} GPUs running ({r_txt}), "
              f"per-user limit {lim if lim is not None else 'none'}, {len(mine_p)} queued")
        if mine_p:
            j = mine_p[0]
            firsts.append((site, j))
            est = f"; Slurm's own estimate {when(j['slurm_est'], now)}" if j["slurm_est"] else "; no Slurm estimate"
            print(f"  next GPU: {when(j.get('sim_start'), now)}, job {j['id']}, priority then about {j.get('sim_prio', 0):.0f}{est}")
            rest = [f"{x['id']} {hm(x['sim_start'])}" for x in mine_p[1:] if x.get("sim_start")]
            if rest:
                print("  then: " + ", ".join(rest[:4]))

    last = max([j["sim_start"] for _, j in firsts if j.get("sim_start")] or [now])
    print("\nGPU starts until then (simulated):")
    for ev in timeline:
        if ev["t"] > last:
            break
        j = ev["job"]
        if not j["gpu"]:
            continue
        fr = ", ".join(f"{r['user']} {r['id']}" for r in ev["freed"] if r["gpu"]) or "freed earlier"
        mark = " <-" if j["user"] in ours else ""
        print(f"  {hm(ev['t'])}  {j['user']} {j['id']} ({j.get('sim_prio', 0):.0f}, {j['gpu']} GPU, "
              f"limit {fmt_limit(j['limit'])}) takes {fr}{mark}")

    base = load_json(d, "base", {})
    for site, j in firsts:
        p = j.get("sim_prio") or j["base"] + j["age"]
        who = sorted(((u, v["base"]) for u, v in base.items() if u not in ours and v["base"] > p and now - v["seen"] < 14 * 86400),
                     key=lambda x: -x[1])
        if who:
            print(f"\nJumping the queue ({sites[site]}): a new job from these users would start before {j['id']}: "
                  + ", ".join(f"{u} {b:.0f}" for u, b in who[:12]) + (f" and {len(who) - 12} more" if len(who) > 12 else ""))

    if gone:
        early = [g for g in gone if g["limit_end"] and g["end_before"] < g["limit_end"] - 3600]
        print(f"\n{len(gone)} jobs ended since the last run, {len(early)} of them over an hour before their limit"
              + ((": " + ", ".join(f"{g['user']} >= {(g['limit_end'] - g['end_before']) / 3600:.1f} h early" for g in early[:8]))
                 if early else ""))
    if prev is not None:
        starts, preds, ourjobs = prev
        rows = evaluate(starts, preds)
        mine = [r for r in rows if r[0] in ourjobs]
        print("\nLooking back (latest prediction made 1-48 h before vs actual start):")
        print(f"  ours: {err_summary(mine)}")
        for jid, pt, p, a, m in sorted(mine, key=lambda r: -r[3])[:5]:
            print(f"    {jid}: predicted at {hm(pt)} for {hm(p)}, started {hm(a)} ({(a - p) / 3600:+.1f} h, v{m})")
        print(f"  everyone: {err_summary(rows)}")


# ---------- main ----------

def main(argv=None):
    ap = argparse.ArgumentParser(prog="taskq site eta", description="When each Slurm executor gets its next GPU (simulated)")
    ap.add_argument("machines", nargs="*", help="only these Slurm executors (default: all with a GPU partition)")
    ap.add_argument("--record", action="store_true", help="read, predict and keep; print nothing")
    ap.add_argument("--backtest", action="store_true", help="rerun the current method on every kept snapshot")
    ap.add_argument("--replay", metavar="FILE", help="rerun one kept snapshot (raw/*.txt.gz); keeps nothing")
    a = ap.parse_args(argv)
    cs = clusters(a.machines)
    if not cs:
        return "taskq site eta: no Slurm executor with a GPU partition and ssh in the machine table"
    allsites = {n: lab for c in cs.values() for n, lab in c["sites"].items()}
    users = load_json(home(), "users", {}) if (a.replay or a.backtest) else site_users(allsites)
    rc = 0
    for part, c in cs.items():
        d = home(part)
        if a.backtest:
            files = sorted(os.listdir(os.path.join(d, F["raw"]))) if os.path.isdir(os.path.join(d, F["raw"])) else []
            ends = load_json(d, "ends", {})
            redo = []
            for fn in files:
                snap = parse(gzip.open(os.path.join(d, F["raw"], fn), "rt").read())
                est_ends(snap, ends)
                pending, _ = simulate(snap)
                redo.append(dict(t=snap["now"], method=METHOD, pred={j["id"]: j.get("sim_start") for j in pending}))
            starts, ours = load_json(d, "starts", {}), set(load_json(d, "ours", []))
            rows = evaluate(starts, redo)
            print(f"{part} backtest: {len(files)} snapshots, method v{METHOD}")
            print(f"  everyone: {err_summary(rows)}")
            print(f"  ours: {err_summary([r for r in rows if r[0] in ours])}")
            print(f"  predictions actually given then (all versions): {err_summary(evaluate(starts, predictions(d)))}")
            continue
        if a.replay:
            raw = gzip.open(a.replay, "rt").read() if a.replay.endswith(".gz") else open(a.replay).read()
            snap = parse(raw)
            est_ends(snap, load_json(d, "ends", {}))
            pending, timeline = simulate(snap)
            report(d, part, snap, pending, timeline, c["sites"], users)
            return 0
        via = next((n for n in c["sites"] if reachable(n)), None)
        if not via:
            print(f"taskq site eta: {part}: no machine reachable ({', '.join(c['sites'])}; ssh master down)", file=sys.stderr)
            rc = 1
            continue
        preds = predictions(d)
        raw = fetch(via, part)
        snap = parse(raw)
        est_ends(snap, load_json(d, "ends", {}))
        pending, timeline = simulate(snap)
        ourusers = {users[s] for s in c["sites"] if s in users}
        ourjobs = set(load_json(d, "ours", [])) | {j["id"] for j in snap["jobs"] if j["user"] in ourusers}
        save_json(d, "ours", sorted(ourjobs))
        gone = record(d, raw, snap, pending)
        if not a.record:
            report(d, part, snap, pending, timeline, c["sites"], users, gone=gone, prev=(load_json(d, "starts", {}), preds, ourjobs))
    return rc


if __name__ == "__main__":
    sys.exit(main())
