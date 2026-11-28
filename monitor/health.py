#!/usr/bin/env python3
"""Health check for the machines in the machine table: the hub's agent clock runs it every 30 minutes, pushes problems to the phone (ntfy)
and stores them in the hub database. Each problem has an id and is pushed once. gather() reads all machine state and is read-only.

    taskq tick              check, push, update state
    taskq tick --dry-run    only print what this round finds; push nothing, change no file
    taskq tick --test       push a test message
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduling code
sys.path.insert(0, os.path.join(CORE, "queue"))
sys.path.insert(0, os.path.join(CORE, "sites"))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
from proto import alert_id, alert_type  # noqa: E402
from push import push  # noqa: E402  (falls back to the default channel file when policy.py cannot be read)

NT = None
try:  # everything that reads the config dir, the experiment table or plugins: when any of it is broken the round must still push (load_failed)
    REPO = cfg.REPO  # root of the user's repo
    cfg.use_config()
    TQS = cfg.state("taskq", expand=False)
    import mtab as M  # noqa: E402
    import policy  # noqa: E402
    import remote as R  # noqa: E402  sites/remote.py (reads policy.py at import)
    import notice as NT  # noqa: E402
    OUT = getattr(policy, "OUTPUTS", {})
    HDIR = os.path.expanduser(OUT.get("health") or cfg.state("health", expand=False))
    NAME = OUT.get("name", "taskq")
    import spec  # noqa: E402  queue/spec.py: task specs, collection on each machine
    import relay_remote as RM  # noqa: E402  queue/relay_remote.py: runs site_ops on the store (reads plugins at import)
    import taskq  # noqa: E402  queue/taskq.py: hub queue (on the hub)
    import plain as PL  # noqa: E402  monitor/plain.py: push text in plain words
    import common as C  # noqa: E402  queue/common.py
except Exception as _e:
    if __name__ != "__main__":
        raise
    LOAD_ERROR = _e  # as a script, main() parses arguments first (--help pushes nothing) and reports after
else:
    LOAD_ERROR = None


def broken(msg, key, dry=False):
    """The round cannot do its work (msg): print it; push it once (the same msg is not pushed again: hub database meta key;
    at night wait until morning). Without a usable database (the config itself is broken) it is pushed every daytime round."""
    print(msg, file=sys.stderr)
    if dry or not proto.NIGHT[1] <= datetime.now().hour < proto.NIGHT[0]:
        return
    try:
        db = NT.open_db() if NT is not None else None
        w = db is not None and NT.D.writable(db)
    except Exception:  # noqa: BLE001
        db, w = None, False
    if w and NT.D.get_meta(db, key) == msg:
        return
    try:
        push(msg, priority="high", tags="warning")
    except Exception as e:  # noqa: BLE001  not recorded: tried again next round
        print(f"push failed: {e}", file=sys.stderr)
        return
    if w:
        with NT.D.tx(db):
            NT.D.set_meta(db, key, msg)


def load_failed(e, dry=False):
    """Loading the config, experiment table or plugins failed: the whole check (sending, fetching, downstream steps) is stopped."""
    broken("Health check cannot run: loading the config, experiment table or plugins failed; sending, fetching and downstream "
           f"steps are stopped.\n{type(e).__name__}: {e}", "load_error", dry)


def stopped(t):
    """Stopped by hand (pueue kill, or SIGTERM giving exit code 143): only logged, not pushed."""
    return t.get("result") == "Killed" or t.get("code") == 143


GPU_PROBE = r"""timeout 20 nvidia-smi --query-gpu=index,uuid,utilization.gpu,memory.used --format=csv,noheader,nounits 2>/dev/null | sed 's/^/GPU|/'
timeout 20 nvidia-smi --query-compute-apps=gpu_uuid,pid,used_memory --format=csv,noheader,nounits 2>/dev/null | while IFS=, read u p m; do
  echo "APP|$u|$p|$m|$(ps -o args= -p $p 2>/dev/null | cut -c1-400)"; done"""


def gpus_of(lines):
    """GPU|, APP| lines from the probe -> [dict(gpu=index, uuid, util, mem, apps=[dict(pid, mem, args)])]."""
    gpus = {}
    for ln in lines:
        p = [x.strip() for x in ln.split("|", 4)]
        if p[0] == "GPU" and len(p) >= 2 and p[1].count(",") >= 3:
            idx, uuid, util, mem = [x.strip() for x in p[1].split(",")][:4]
            gpus[uuid] = dict(gpu=idx, uuid=uuid, util=util, mem=mem, apps=[])
        elif p[0] == "APP" and len(p) >= 5 and p[1] in gpus:
            gpus[p[1]]["apps"].append(dict(pid=p[2], mem=p[3], args=p[4]))
    return list(gpus.values())


def probe_ok(rc, out):
    return rc == 0 and "BOOT|" in out


def own_state(m, *parts):
    """A path in machine m's own state dir as m sees it (cfg.machine_state over there: its root's state/, else the state
    dir), shell-quoted with a leading ~ left for m to expand."""
    base = f"{m['root'].rstrip('/')}/{proto.ROOT['state']}" if m.get("root") else cfg.state_home().rstrip("/")
    return R.q("/".join([base, *parts]))


def probe_script(m):
    """What to check on a machine with a pueue group (per machine-table fields), one "item|value" per line."""
    logs = cfg.outputs("logs")
    boot = R.q(os.path.join(logs, proto.BOOT_LOG + ".log")) if logs else own_state(m, proto.LOGS, proto.BOOT_LOG + ".log")
    lines = [f'echo "BOOT|$(uptime -s)|$(tail -1 {boot} 2>/dev/null)"', GPU_PROBE]
    if m["disk"]:
        lines.append(f'echo "DISK|$(df -B1 --output=avail {m["disk"][0]} | tail -1 | tr -d \' \')"')
    if m["gpu_log"]:
        lines.append(f'echo "GPULOG|$(tail -1 {own_state(m, proto.GPU_LOG_SUB)}/$(date +{proto.GPU_LOG_FILE}) 2>/dev/null | cut -d, -f1)"')
    return "\n".join(lines) + "\n"


def sh(cmd, inp=None, timeout=120):
    """Run a command (list) locally -> (exit code, stdout; stderr when it failed without output)."""
    rc, out, err = C.run(cmd, timeout=timeout, input=inp)
    return rc, out if rc == 0 or out else err


AGENT_DOWN = 30 * 60  # seconds without a call from a machine's agent before agent:<machine>
AGENT_REVIVE = 5 * 60  # seconds without a call before the round starts that agent again over ssh (AUTO["revive"])


def run_on(m, script, timeout=120):
    remote = f"timeout -k 10 {max(timeout - 15, 10)} bash -s"
    return sh(["bash", "-s"] if m["ssh"] is None else C.SSH + [m["ssh"], remote], inp=script, timeout=timeout)


# ---------------- collect ----------------
def gather(remote=True):
    """State of the machines (read-only). remote=False: only this machine (no network)."""
    s = dict(now=datetime.now(), ok={}, why={}, probe={}, gpus={}, src={}, taskq={})
    hosts = M.pueue_hosts()
    here = [n for n in hosts if (remote or M.local(n)) and not M.paused(n)]
    for n in hosts:
        if M.paused(n):
            s["ok"][n], s["why"][n] = False, f"paused: {M.paused(n)}"
    if here:  # qs without machine names asks all: only ask this round's machines
        rc, out = sh([os.path.join(CORE, "queue", "qs"), "--json"] + here, timeout=180)
        s["pueue"] = json.loads(out) if rc == 0 else {n: {"error": out} for n in here}
    else:
        s["pueue"] = {}
    s["records"] = C.sync_records(s["pueue"], C.load_records())  # task records (queue/common.py), written back in main()
    just = os.environ.get("TASKQ_TASK_DONE")  # set when a hub core task finishes and calls the check at once (its label): count it done this round
    for t in (s["pueue"].get(M.hub()) or {}).get("tasks", []) if just else []:
        if t["label"] == just and t["status"] == "Running":
            t.update(status="Success", result="Success", code=0)
    # the hub queue is on the hub: collecting job status needs the queued tasks
    hub = M.hub()
    try:
        s["taskq"][hub] = taskq.local_status()
    except taskq.DiskError as e:  # store unreachable or its bundle unusable: send nothing this round (relay still swaps the bundle)
        s["taskq"][hub], s["why"]["taskq"] = None, str(e)
    queue = list((s["taskq"][hub] or {}).get("tasks", []))
    for name in here:
        m = M.machine(name)
        rc, out = run_on(m, probe_script(m))
        ok = probe_ok(rc, out)
        if ok:
            lines = out.splitlines()
            s["probe"][name] = dict(ln.split("|", 1) for ln in lines if "|" in ln and not ln.startswith(("GPU|", "APP|")))
            s["gpus"][name] = gpus_of(lines)
        s["ok"][name] = ok
    specs = spec.task_specs(queue)
    st = M.store()  # each task's job status on the store (spec.collect, runs there); executor ones are read by relay status
    s["collect"] = {}  # collection failures: {store: reason} (unreachable, bundle unusable, storage stuck; not treated as machine down)
    if (remote or M.local(st)) and s["ok"].get(st, True):
        try:
            s["src"][st] = RM.store_op("collect", dict(specs=specs, wait=spec.COLLECT_WAIT), timeout=2 * spec.COLLECT_WAIT + 120)
        except RuntimeError as e:
            s["collect"][st] = str(e)[-300:]
    db = NT.open_db(write=False)
    s["notices"] = {n: v["text"] for n, v in NT.notices(db).items()} if db else {}
    if remote:  # resident agents (sync/agent_box.py): when each last called the hub; also restarts the file server they call
        sys.path.insert(0, os.path.join(CORE, "sync"))
        import agent_box
        s["agents"] = agent_box.ages()
        if s["agents"]:
            try:
                agent_box.server()
            except Exception as e:  # noqa: BLE001
                s["agents_err"] = str(e)
    return s


def pueue_seen(s, dry=False):
    """Machines where pueued answered this round: record time and running groups (proto.PUEUE_SEEN). -> records (including older ones)."""
    p = os.path.join(HDIR, proto.PUEUE_SEEN)
    try:
        seen = json.load(open(p))
    except (OSError, ValueError):
        seen = {}
    t = int(s["now"].timestamp())
    for name, d in (s.get("pueue") or {}).items():
        if isinstance(d, dict) and "error" not in d:
            seen[name] = dict(t=t, running=sorted(g for g, st in (d.get("groups") or {}).items() if st == "Running"))
    if not dry:
        C.write_json(p, seen)
    return seen


def revive_agents(s, dry=False):
    """Agents that stopped calling (switch AUTO["revive"]): start them again the way taskq site agent start does (real ssh, or
    here for the hub's own machine), only when that ssh can be opened safely (ssh_master: its master connection is up). Agents
    that never called (not installed) are left alone. -> (events, {machine: why it still fails})."""
    if not policy.AUTO.get("revive"):
        return [], {}
    sys.path.insert(0, os.path.join(CORE, "sync"))
    import agent_box
    events, still = [], {}
    for name, a in (s.get("agents") or {}).items():
        if a is None or a <= AGENT_REVIVE or M.paused(name):
            continue
        m = M.machine(name)
        if m.get("ssh_master") and m.get("ssh") and subprocess.run(
                [os.environ.get("TASKQ_REAL_SSH", "ssh"), "-O", "check", m["ssh"]], capture_output=True, timeout=30).returncode:
            still[name] = "not started again automatically: its ssh master connection is down"
            continue
        if dry:
            events.append(f"{L(name)} agent has not called for {int(a // 60)} minutes: it will be started again over ssh")
            continue
        t0 = time.time()
        rc = agent_box.start(name)
        back = False
        for _ in range(60 if rc == 0 else 0):
            b = agent_box.ages().get(name)
            if b is not None and b < time.time() - t0 + 1:
                back = True
                break
            time.sleep(1)
        if back:
            events.append(f"{L(name)} agent had not called for {int(a // 60)} minutes; started again automatically")
            import events as EV
            EV.add("health", events[-1], kind="revive", site=name)
        else:
            still[name] = f"starting it again over ssh failed (exit {rc})" if rc else "started again over ssh but it has not called within 60s"
    return events, still


def revive(s, seen, dry=False):
    """pueued not responding and the machine reachable (switch AUTO["revive"]): run on_boot.sh --since (last time it was seen)
    --resume (the groups open then) there, which starts pueued, puts back interrupted tasks and reopens those groups. on_boot.sh
    uses that machine's scheduling code (mtab.core: the bundle, or a clone of the repo).
    -> (events, {machine: why it still fails}). A recovered machine is not reported as pueued this round."""
    if not policy.AUTO.get("revive"):
        return [], {}
    events, still = [], {}
    for name in M.pueue_hosts():
        d = (s.get("pueue") or {}).get(name)
        if M.paused(name) or not s["ok"].get(name) or not (isinstance(d, dict) and "error" in d):
            continue
        m, last, core = M.machine(name), seen.get(name) or {}, M.core(name)
        if core is None:
            still[name] = "cannot tell where the scheduling code on that machine is (machine table has neither root nor repo), cannot recover automatically"
            continue
        since = int(last["t"]) - 60 if last.get("t") else int(s["now"].timestamp())  # no record: just start it, put no task back
        args = ["--since", str(since)] + (["--resume", ",".join(last["running"])] if last.get("running") else [])
        script = " ".join(["bash", core + "/queue/on_boot.sh"] + args
                          + (["--gpu-log"] if m.get("gpu_log") else []))
        if dry:
            events.append(f"{L(name)} pueued is not responding: it will be recovered automatically ({script})")
            continue
        rc, out = run_on(m, script, timeout=180)
        rc2, out2 = sh([os.path.join(CORE, "queue", "qs"), "--json", name], timeout=90)
        try:
            back = rc2 == 0 and "error" not in json.loads(out2).get(name, {"error": 1})
        except ValueError:
            back = False
        tail = " ".join(ln.split(" ", 2)[-1] for ln in out.strip().splitlines()[-3:])
        if back:
            events.append(f"{L(name)} pueued was not responding; recovered automatically: {tail or 'started pueued'}")
            import events as EV
            EV.add("health", events[-1], kind="revive", site=name)
        else:
            still[name] = f"automatic recovery failed (exit {rc}): {(out or out2).strip()[-200:]}"
    return events, still


def L(name):
    return M.MACHINES[name]["label"]


# ---------------- check ----------------
def problem(msg, level="normal", kind="state", after=0, data=None):
    """One problem: msg is the human description (ledger), data is the structured fact (plain.py words it)."""
    p = dict(msg=msg, level=level, kind=kind, after=after)
    if data:
        p["data"] = data
    return p


def check(s):
    """-> {problem id: dict(msg, level=urgent|normal|info, kind=event|state, after=minutes before reporting, data=structured facts (optional))}."""
    now, P = s["now"], {}
    L = lambda n: M.MACHINES[n]["label"]  # noqa: E731

    def add(pid, msg, level="normal", kind="state", after=0, data=None):
        P[pid] = problem(msg, level, kind, after, data)

    for name, ok in s["ok"].items():
        if not ok and not M.paused(name):
            add(alert_id("down", name), f"{L(name)} unreachable ({(s.get('why') or {}).get(name) or 'ssh ' + str(M.MACHINES[name]['ssh'])})",
                after=30)
    for name, d in (s.get("src") or {}).items():  # storage stuck, unreadable task files (spec.collect's stuck)
        if d.get("stuck"):
            add(alert_id("unread", name), f"{L(name)}: files of {len(d['stuck'])} tasks are unreadable (its storage is probably stuck), "
                f"not assigning or releasing them yet: {', '.join(d['stuck'][:5])}{' ...' if len(d['stuck']) > 5 else ''}",
                after=60, data=dict(n=len(d["stuck"]), keys=d["stuck"]))
        if d.get("errors"):
            er = sorted(d["errors"].items())
            add(alert_id("readerr", name), f"{L(name)}: reading status of {len(er)} tasks failed, not assigning or releasing them yet: "
                f"{'; '.join(f'{k} ({v})' for k, v in er[:3])}{' ...' if len(er) > 3 else ''}", after=60, data=dict(n=len(er)))
    for name, a in (s.get("agents") or {}).items():
        if (a is None or a > AGENT_DOWN) and not M.paused(name):
            add(alert_id("agent", name), f"The agent on {L(name)} has not called the hub {'yet' if a is None else f'for {int(a // 60)} minutes'} "
                f"(taskq site agent status {name}; start it again: taskq site agent start {name})", data=dict(seen=a))
    if s.get("agents_err"):
        add(alert_id("agent", M.hub()), f"The file server the agents call cannot start: {s['agents_err']}")
    # --- hub queue (on the hub) ---
    hub = M.hub()
    ctq = s.get("taskq", {}).get(hub)
    for name, why in (s.get("collect") or {}).items():
        add(alert_id("collect", name), f"Collecting job status on {L(name)} failed: {why}", after=30)
    if hub in s.get("taskq", {}) and ctq is None:
        add(alert_id("taskq", hub), f"{L(hub)} cannot read the hub queue ({TQS}/){(': ' + s['why']['taskq']) if s.get('why', {}).get('taskq') else ''}")
    elif ctq and not ctq.get("enabled"):
        add(alert_id("tqoff", hub), f"The hub queue on {L(hub)} is off ({TQS}/{proto.QUEUE['enabled']} missing); no tasks are sent to executors", after=60)
    # --- pueue ---
    for name in M.pueue_hosts():
        if not s["ok"].get(name):
            continue
        d = s["pueue"].get(name) or {"error": "no result"}
        if "error" in d:
            add(alert_id("pueued", name), f"pueue on {L(name)} is not responding: {d['error'][:120]}")
            continue
        own = set(M.groups(name))  # groups the machine table gives this machine; others belong to someone else's work on a shared pueue
        d = dict(d, tasks=[t for t in d["tasks"] if t["group"] in own])
        ident = {t["id"]: C.identify(name, t, s.get("records") or []) for t in d["tasks"]}
        newer = {}
        for i, r in ident.items():
            if r:
                newer[r["step"], r["key"]] = max(i, newer.get((r["step"], r["key"]), i))
        for t in d["tasks"]:
            r = ident[t["id"]]
            tname = t["label"] or f"#{t['id']}"
            st = t["status"]
            step = (r or {}).get("step") or tname.split(":", 1)[0]
            if st in proto.PUEUE_LIVE or st == "Success" or step in proto.RELAY_OWNED:
                continue
            if stopped(t) and tname.startswith(proto.WORKER_LABEL):  # worker stopped at hours end or to yield a card; worker errors are reported by relay as site:<machine>:worker
                continue
            end = C.local_time(t["end"])
            if end and now - end > timedelta(hours=24):
                continue
            if r and newer[r["step"], r["key"]] > t["id"]:  # the same step and key queued again later (re-queued after a failure): only the newest counts
                continue
            add(alert_id("pueue", name, t["id"], str(t["end"])), f"{L(name)} task {t['id']} {tname} {st}",
                level="info" if stopped(t) else "normal", kind="event", data=dict(label=tname, status=st, step=step))

    # --- disk, GPU log, reboot ---
    for name, pr in s["probe"].items():
        m = M.machine(name)
        if m["disk"] and pr.get("DISK", "").strip().isdigit():
            free, warn, urgent = int(pr["DISK"]) / 1e9, m["disk"][1], m["disk"][2]
            if free < warn:
                add(alert_id("disk", name), f"{L(name)} {m['disk'][0]} has only {free:.0f} GB left", level="urgent" if urgent and free < urgent else "normal",
                    data=dict(path=m["disk"][0], free_gb=free))
        if m["gpu_log"]:
            last = pr.get("GPULOG", "")
            try:
                age = now - datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                age = None
            if age is None or age > timedelta(minutes=10):
                add(alert_id("gpulog", name), f"GPU usage logging on {L(name)} stopped (last row {last or 'none'})")
        boot, _, line = pr.get("BOOT", "").partition("|")
        try:
            t = datetime.strptime(boot, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if now - t < timedelta(hours=24):
            add(alert_id("boot", name, boot), f"{L(name)} rebooted at {t:%m-%d %H:%M}: {line[20:] or 'no boot record (on_boot.sh did not run?)'}",
                kind="event", data=dict(time=boot))
    for name, text in (s.get("notices") or {}).items():
        add(alert_id("notice", name), text, data=dict(name=name))

    return P


# ---------------- push and state ----------------
def drop_acked(P, db, dry=False):
    """Acknowledged event problems (taskq notice ack) are not listed; acks of problems no longer raised are dropped."""
    d = NT.acked(db) if db else {}
    if d and not dry and NT.D.writable(db):
        NT.prune_acks(db, set(P))
    return {k: p for k, p in P.items() if not (k in d and p.get("kind") == "event")}


RANK = {"info": 0, "normal": 1, "urgent": 2}  # problem levels, least to most severe
LEVEL = {v: k for k, v in RANK.items()}


def night(now):
    return now.hour >= proto.NIGHT[0] or now.hour < proto.NIGHT[1]


def cause_line(ids, cur):
    """One push line for problems with the same cause: the worst, oldest one's wording, and how many more share it."""
    ids = sorted(ids, key=lambda k: (-RANK.get(cur[k]["level"], 1), cur[k]["first"], k))
    text = PL.problem(ids[0], cur[ids[0]])
    return text if len(ids) == 1 else f"{text} (+{len(ids) - 1} more with the same cause; taskq problems lists them)"


def update(P, now, db, dry=False):
    """Compare with the last state and push what became active, one line per cause (proto.ALERT_RULES; plain.py wording; one
    message per round). A cause already pushed is pushed again only when its active problems double or get worse; a round
    without any of them ends it (a state problem back within PROBLEM_KEEP is still not pushed again). At night only urgent
    ones are pushed; the rest go with the first check in the morning."""
    old = NT.problems(db)[0] if db else {}
    stamp = now.strftime("%Y-%m-%d %H:%M")
    keep = (now - timedelta(seconds=proto.PROBLEM_KEEP)).strftime("%Y-%m-%d %H:%M")
    gone = {k: v for k, v in (NT.D.get_meta(db, "gone_problems", {}) if db else {}).items() if v.get("gone", "") >= keep}
    causes = NT.D.get_meta(db, "notified_causes", {}) if db else {}
    log, cur, groups = [], {}, {}
    for pid, p in P.items():
        o = old.get(pid) or gone.get(pid) or {}  # a state problem gone for a while (flapping) keeps when it started and whether it was pushed
        p["first"] = o.get("first", stamp)
        p["notified"] = o.get("notified", False) and RANK.get(p["level"], 1) <= RANK.get(o.get("level"), 1)  # worse than when pushed: push again
        age = (now - datetime.strptime(p["first"], "%Y-%m-%d %H:%M")).total_seconds() / 60
        p["active"] = age >= p["after"]
        p["cause"] = proto.alert_cause(pid, p.get("data"))
        p["push"] = proto.alert_push(pid) and p["level"] != "info"
        if p["active"] and p["push"]:
            groups.setdefault(p["cause"], []).append(pid)
        if pid not in old:
            log.append(f"new problem {pid}: {p['msg']}")
        cur[pid] = p
    for pid, p in cur.items():
        p["members"] = len(groups.get(p["cause"], ())) or 1
    for pid in old:
        if pid not in cur:
            log.append(f"gone {pid}")
    causes, msgs = {c: v for c, v in causes.items() if c in groups}, []
    for c, ids in sorted(groups.items()):
        fresh = [k for k in ids if not cur[k]["notified"]]
        if not fresh or (night(now) and not any(cur[k]["level"] == "urgent" for k in fresh)):
            continue
        lvl = max(RANK.get(cur[k]["level"], 1) for k in ids)
        prev = causes.get(c)
        if prev is None or len(ids) >= 2 * prev.get("n", 1) or lvl > RANK.get(prev.get("level"), 1):
            msgs.append((c, ids, lvl))
        else:
            for k in fresh:
                cur[k]["notified"] = True  # covered by this cause's earlier push
    sent_ok = True
    if not dry:
        try:
            if msgs:
                urgent = any(lvl == RANK["urgent"] for _, _, lvl in msgs)
                texts = [cause_line(ids, cur) for _, ids, _ in msgs]
                push(texts[0] if len(texts) == 1 else "\n".join("- " + t for t in texts),
                     title=(f"{NAME}: urgent" if urgent else f"{NAME}: needs attention") + (f" ({len(texts)} items)" if len(texts) > 1 else ""),
                     priority="high" if urgent else "default", tags="rotating_light" if urgent else "warning")
                for c, ids, lvl in msgs:
                    for k in ids:
                        cur[k]["notified"] = True
                    causes[c] = dict(n=len(ids), level=LEVEL[lvl])
        except Exception as e:  # noqa: BLE001  push failed: try again next time
            sent_ok = False
            log.append(f"push failed: {e}")
        if db is not None and NT.D.writable(db):
            NT.save_problems(db, cur, stamp)
            gone = {**{k: v for k, v in gone.items() if k not in cur},
                    **{k: dict(v, gone=stamp) for k, v in old.items() if k not in cur and v.get("kind") == "state"}}
            with NT.D.tx(db):
                NT.D.set_meta(db, "gone_problems", gone)
                NT.D.set_meta(db, "notified_causes", causes)
        import events
        events.lines("health", log, db=db)
    return [cur[k] for _, ids, _ in msgs for k in ids], sent_ok


def progress_snapshot(s, db):
    """Each task's hub queue state, progress, machine and word (taskstate.py, as taskq status shows it) -> the progress table
    (only what changed). Machine: where taskstate puts it, else where its best copy is; progress likewise."""
    import events
    import overview
    tasks = [dict(key=t["key"], state=(t["queue"] or {}).get("state"), progress=t["progress"], site=t["where"] or (t["copy"] or {}).get("source"),
                  data=dict(epochs=(t["copy"] or {}).get("epochs"), total=t["total"], word=t["word"])) for t in overview.build(s)["tasks"]]
    return events.snapshot(db, tasks)


def from_step(v):
    """A stuck report from feed (relay) / advance: message (str), or dict(msg, data[, level]) -> problem."""
    if isinstance(v, dict):
        return problem(v["msg"], level=v.get("level", "normal"), data=v.get("data"), after=v.get("after", 0))  # level="info": only waiting, recorded but not pushed
    return problem(v)


def slowest(steps, n=3):
    """The n slowest parts of a round (tick_steps: {part: seconds}; feed includes the relay parts listed after it) as text."""
    skip = {"feed"} if any(k.startswith("relay ") for k in steps) else set()
    top = sorted(((v, k) for k, v in steps.items() if k not in skip and isinstance(v, (int, float))), reverse=True)[:n]
    return "; slowest parts: " + ", ".join(f"{k} {v / 60:.0f} min" if v >= 90 else f"{k} {v:.0f} s" for v, k in top) if top else ""


def round_lock():
    """Take the round lock (flock on <health dir>/tick.lock, held until this process exits) -> the open file; None when another
    round is still running: say so, and push an alert once that round has run longer than proto.TICK_STUCK (daytime only)."""
    import fcntl
    os.makedirs(HDIR, exist_ok=True)
    f = open(os.path.join(HDIR, proto.TICK + ".lock"), "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        f.seek(0)
        pid, t0 = (f.read().split() + ["?", ""])[:2]
        f.close()
        age = int(time.time()) - int(t0) if t0.isdigit() else 0
        print(f"another health check round (pid {pid}) has been running for {age // 60} min; skipping this round", file=sys.stderr)
        if age >= proto.TICK_STUCK and proto.NIGHT[1] <= datetime.now().hour < proto.NIGHT[0]:
            push(f"Health check stuck: the round started {age / 3600:.1f} h ago (pid {pid} on the hub) is still running, so no "
                 "checks, sending or fetching happen until it ends", priority="high", tags="warning")
        return None
    f.seek(0)
    f.truncate()
    f.write(f"{os.getpid()} {int(time.time())}\n")
    f.flush()
    return f


def main(argv=None):
    ap = argparse.ArgumentParser(description="One health check round: check machines, push problems, queue downstream steps, send tasks")
    ap.add_argument("-n", "--dry-run", action="store_true", help="what this round would do (taskq plan): problems, downstream steps, "
                    "tasks to send; push nothing, change no file")
    ap.add_argument("--test", action="store_true", help="push a test message")
    a = ap.parse_args(argv)
    if LOAD_ERROR is not None:
        load_failed(LOAD_ERROR, dry=a.dry_run)
        return 1
    try:
        return one_round(a)
    except Exception as e:  # noqa: BLE001  a crash must not end the checks silently: push it (once per distinct error)
        import traceback
        traceback.print_exc()
        broken(f"Health check crashed this round, so nothing was checked, sent or fetched: {type(e).__name__}: {e}"[:600], "crash_error", a.dry_run)
        return 1


def mark_live():
    """Record this round's project as the hub's live one (proto.LIVE), for read-only commands run in other checkouts."""
    import project
    root = os.environ.get("TASKQ_REPO")
    if not root or not os.path.isfile(os.path.join(root, project.NAME)):
        return
    path = os.path.expanduser(proto.LIVE)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path + ".tmp", "w") as f:
            json.dump(dict(repo=os.path.realpath(root), origin=project.origin(root), t=int(time.time())), f)
        os.replace(path + ".tmp", path)
    except OSError as e:
        print(f"{path} not written: {e}", file=sys.stderr)


def heartbeat():
    """OUTPUTS heartbeat: fetch that URL after a finished round, so an outside service notices when rounds stop (clock gone, the
    hub down, the config broken, a crash every round)."""
    url = cfg.outputs("heartbeat")
    if not url:
        return
    try:
        urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "taskq"}), timeout=10).read()
    except Exception as e:  # noqa: BLE001
        print(f"heartbeat to {url} failed: {e}", file=sys.stderr)


def format_refused(dry):
    """The hub state is not in this code's on-disk format (common.format_problem; a new hub, with no database yet, is stamped)
    -> True after saying so: a round does nothing until taskq self migrate has run (pushed once per message, by day);
    taskq plan only notes it and goes on."""
    fresh = not os.path.exists(NT.D.path())
    why = None if dry and fresh else C.format_problem(fresh=fresh)  # a plan on a new hub: its first round stamps it
    if not why:
        return False
    if dry:
        print(f"note: {why}; this plan reads state in another format", file=sys.stderr)
        return False
    msg = f"Health check not run: {why}. Nothing is checked, sent or fetched until then."
    print(msg, file=sys.stderr)
    if proto.NIGHT[1] <= datetime.now().hour < proto.NIGHT[0] and C.changed("format_refused", msg):
        try:
            push(msg, priority="high", tags="warning")
        except Exception as e:  # noqa: BLE001  not pushed: try again next round
            print(f"push failed: {e}", file=sys.stderr)
            C.changed("format_refused", None)
    return True


def one_round(a):
    t0 = time.time()
    steps, lap0 = {}, [t0]

    def lap(name):
        t = time.time()
        steps[name] = round(t - lap0[0], 1)
        lap0[0] = t
    lock = None if a.dry_run or a.test else round_lock()  # held until this returns: rounds never overlap (manual tick, slow round)
    if not (a.dry_run or a.test) and lock is None:
        return 0
    if not a.test and format_refused(a.dry_run):  # before anything opens the database (opening migrates its schema)
        return 1
    import events
    db = NT.open_db(write=not a.dry_run)
    if not a.dry_run and db is not None and NT.D.writable(db):
        NT.D.set_meta(db, "load_error", None)
        try:
            events.flush(db)
        except Exception as e:  # noqa: BLE001
            print(f"event spool not imported into the database: {e}", file=sys.stderr)
        try:
            n = NT.D.compact(db)
            if n:
                events.add("health", f"compacted {NT.D.path()}: {n / 1e6:.0f} MB of free pages given back", db=db)
        except Exception as e:  # noqa: BLE001  locked by a reader right now: next round
            print(f"database not compacted: {e}", file=sys.stderr)
    if a.test:
        push("test: a health check push reaches the phone", tags="tada")
        print("test message pushed")
        return 0
    lap("open")
    s = gather()
    if not a.dry_run:
        s["records"] = C.update_records(lambda recs: C.sync_records(s["pueue"], recs))
    lap("gather")
    P = check(s)
    try:
        revived, still = revive(s, pueue_seen(s, dry=a.dry_run), dry=a.dry_run)
    except Exception as e:  # noqa: BLE001
        revived, still = [], {}
        P[alert_id("revive", "error")] = problem(f"automatic pueued recovery failed: {type(e).__name__}: {e}"[:300])
    for line in revived:
        name = next((n for n in M.pueue_hosts() if line.startswith(L(n) + " ")), None)
        if name and "recovered automatically" in line:
            P.pop(alert_id("pueued", name), None)
    for name, why in still.items():
        pid = alert_id("pueued", name)
        if pid in P:
            P[pid] = dict(P[pid], msg=f"{P[pid]['msg']}; {why}")
    try:
        back, still_a = revive_agents(s, dry=a.dry_run)
    except Exception as e:  # noqa: BLE001
        back, still_a = [], {}
        P[alert_id("revive", "error")] = problem(f"automatic agent restart failed: {type(e).__name__}: {e}"[:300])
    revived = list(revived) + back
    for line in back:
        name = next((n for n in (s.get("agents") or {}) if line.startswith(L(n) + " ")), None)
        if name and "started again automatically" in line:
            P.pop(alert_id("agent", name), None)
    for name, why in still_a.items():
        pid = alert_id("agent", name)
        if pid in P:
            P[pid] = dict(P[pid], msg=f"{P[pid]['msg']}; {why}")
    lap("revive")
    # An error in these steps (including an import error) must not stop the health check itself (alerts and the ledger depend on it):
    # report it and carry on.
    events, stuck, after = list(revived), {}, []
    try:  # queue downstream steps first, so what this round queues is sent this round
        import advance  # queue/advance.py: queue the next step per experiment table
        done, stuck2 = advance.run(s, dry=a.dry_run, say=after)
        if done and not a.dry_run:
            try:
                s["taskq"][M.hub()] = taskq.local_status()
            except taskq.DiskError as e:
                s["taskq"][M.hub()], s["why"]["taskq"] = None, str(e)
    except Exception as e:  # noqa: BLE001
        done, stuck2 = [], {"advance:error": f"queue/advance.py failed: {type(e).__name__}: {e}"[:300]}
    lap("advance")
    relay_t = {}
    try:
        import feed  # queue/feed.py: the only way tasks are sent, hands over to relay.py
        ev, st = feed.run_all(s, dry=a.dry_run, times=relay_t)
        events.extend(ev)
        stuck.update(st)
    except Exception as e:  # noqa: BLE001
        stuck["feed:error"] = f"queue/feed.py failed: {type(e).__name__}: {e}"[:300]
    lap("feed")
    steps.update({f"relay {k}": v for k, v in relay_t.items()})
    for pid, v in {**stuck, **stuck2}.items():
        P[pid] = from_step(v)
    took = NT.D.get_meta(db, "tick_took") if db is not None else None
    if isinstance(took, (int, float)) and took > proto.TICK_SLOW:
        P[alert_id("health", "slow")] = problem(f"the last health check round took {took / 60:.0f} min (the clock starts one every 30 min): "
                                                "rounds started while one runs are skipped, so checks and sending happen less often"
                                                + slowest(NT.D.get_meta(db, "tick_steps") or {}), data=dict(took=took))
    if not a.dry_run and (db is None or not NT.D.writable(db)):
        P[alert_id("db", M.hub())] = problem(f"cannot write the hub database {NT.D.path()} (on a network disk, or created by a newer "
                                             "taskq): every check will push these problems again", level="urgent")
    P = drop_acked(P, db, dry=a.dry_run)
    update(P, s["now"], db, dry=a.dry_run)
    lap("push")
    if not a.dry_run and db is not None and NT.D.writable(db):
        try:
            progress_snapshot(s, db)
        except Exception as e:  # noqa: BLE001
            print(f"progress snapshot not recorded: {type(e).__name__}: {e}", file=sys.stderr)
        try:
            with NT.D.tx(db):
                NT.D.set_meta(db, "tick_took", round(time.time() - t0))
                NT.D.set_meta(db, "tick_steps", steps)
                NT.D.set_meta(db, "crash_error", None)
        except Exception as e:  # noqa: BLE001  locked by a reader right now: next round
            print(f"round time not recorded: {e}", file=sys.stderr)
    if not a.dry_run:
        heartbeat()
        mark_live()
    if a.dry_run:
        probs = []
        for pid, p in sorted(P.items()):
            wait = f" / reported after {p['after']} min" if p["after"] else ""
            probs.append(f"[{p['level']}/{p['kind']}{wait}] {pid}\n    {p['msg']}")
        parts = [(f"problems this round ({len(probs)})", probs or ["none"]), ("downstream steps (advance)", after or ["nothing to queue"]),
                 ("send tasks, top up workers (feed -> relay)", events or ["nothing"])]
        print("\n\n".join(f"== {t}\n" + "\n".join(lines) for t, lines in parts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
