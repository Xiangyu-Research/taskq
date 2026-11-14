#!/usr/bin/env python3
"""Health check for the machines in the machine table: cron on the hub runs it every 30 minutes, pushes problems to the phone (ntfy)
and stores them in the hub database. Each problem has an id and is pushed once. gather() reads all machine state and is read-only.

    taskq tick              check, push, update state
    taskq tick --dry-run    only print what this round finds; push nothing, change no file
    taskq tick --test       push a test message
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduling code
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg  # noqa: E402
import proto  # noqa: E402
from proto import alert_id  # noqa: E402
REPO = cfg.REPO  # root of the user's repo
cfg.use_config()
TQS = cfg.state("taskq", expand=False)
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import policy  # noqa: E402
sys.path.insert(0, HERE)
import notice as NT  # noqa: E402

OUT = getattr(policy, "OUTPUTS", {})
HDIR = os.path.expanduser(OUT.get("health") or cfg.state("health", expand=False))
NAME = OUT.get("name", "taskq")
NIGHT = (22, 8)  # 22:00-08:00: only urgent pushes


from push import push  # noqa: E402


def load_failed(e, dry=False):
    """Loading the experiment table or plugins failed: the whole check (sending, fetching, downstream steps) is stopped; push once
    (the same error is not pushed again; at night wait until morning)."""
    msg = f"Health check cannot run: loading the experiment table or plugins failed; sending, fetching and downstream steps are stopped.\n{type(e).__name__}: {e}"
    print(msg, file=sys.stderr)
    if dry or not NIGHT[1] <= datetime.now().hour < NIGHT[0]:
        return
    db = NT.open_db()
    if db is None or not NT.D.writable(db) or NT.D.get_meta(db, "load_error") != msg:
        push(msg, priority="high", tags="warning")
        if db is not None and NT.D.writable(db):
            with NT.D.tx(db):
                NT.D.set_meta(db, "load_error", msg)


try:
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


def stopped(t):
    """Stopped by hand (pueue kill, or SIGTERM giving exit code 143): only logged, not pushed."""
    return t.get("result") == "Killed" or t.get("code") == 143


LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")

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


def probe_script(m):
    """What to check on a machine with a pueue group (per machine-table fields), one "item|value" per line."""
    lines = ['echo "BOOT|$(uptime -s)|$(tail -1 ~/on_boot.log 2>/dev/null)"', GPU_PROBE]
    if m["disk"]:
        lines.append(f'echo "DISK|$(df -B1 --output=avail {m["disk"][0]} | tail -1 | tr -d \' \')"')
    if m["gpu_log"]:
        lines.append(f'echo "GPULOG|$(tail -1 {proto.GPU_LOG_DIR}/$(date +{proto.GPU_LOG_FILE}) 2>/dev/null | cut -d, -f1)"')
    return "\n".join(lines) + "\n"


def sh(cmd, inp=None, timeout=120):
    """Run a command (list) locally -> (exit code, stdout; stderr when it failed without output)."""
    rc, out, err = C.run(cmd, timeout=timeout, input=inp)
    return rc, out if rc == 0 or out else err


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
        ident = {t["id"]: C.identify(name, t, s.get("records") or []) for t in d["tasks"]}
        newer = {}
        for i, r in ident.items():
            if r:
                newer[r["step"], r["key"]] = max(i, newer.get((r["step"], r["key"]), i))
        for t in d["tasks"]:
            r = ident[t["id"]]
            tname = t["label"] or f"#{t['id']}"
            st = t["status"]
            if st in LIVE or st == "Success":
                continue
            if stopped(t) and tname.startswith(proto.WORKER_LABEL):  # worker stopped at hours end or to yield a card; worker errors are reported by relay as site:<machine>:worker
                continue
            end = C.local_time(t["end"])
            if end and now - end > timedelta(hours=24):
                continue
            if r and newer[r["step"], r["key"]] > t["id"]:  # the same step and key queued again later (re-queued after a failure): only the newest counts
                continue
            add(alert_id("pueue", name, t["id"], str(t["end"])), f"{L(name)} task {t['id']} {tname} {st}",
                level="info" if stopped(t) else "normal", kind="event", data=dict(label=tname, status=st))

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


QUIET = ("notice", "undeclared")  # problem kinds only listed in taskq problems, not pushed (the phone gets failures only; due notices are read at work time; writes outside the declared outputs)


def night(now):
    return now.hour >= NIGHT[0] or now.hour < NIGHT[1]


def update(P, now, db, dry=False):
    """Compare with the last state: push new problems once they are old enough (plain.py wording; several in one round go as one message).
    At night only urgent ones are pushed; the rest go with the first check in the morning."""
    old = NT.problems(db)[0] if db else {}
    msgs, log = [], []
    cur = {}
    for pid, p in P.items():
        o = old.get(pid, {})
        p["first"] = o.get("first", now.strftime("%Y-%m-%d %H:%M"))
        p["notified"] = o.get("notified", False)
        age = (now - datetime.strptime(p["first"], "%Y-%m-%d %H:%M")).total_seconds() / 60
        p["active"] = age >= p["after"]
        if p["active"] and not p["notified"] and p["level"] != "info" and pid.split(":")[0] not in QUIET \
                and (p["level"] == "urgent" or not night(now)):
            msgs.append((pid, p))
        if pid not in old:
            log.append(f"new problem {pid}: {p['msg']}")
        cur[pid] = p
    for pid in old:
        if pid not in cur:
            log.append(f"gone {pid}")
    sent_ok = True
    if not dry:
        try:
            if msgs:
                urgent = any(p["level"] == "urgent" for _, p in msgs)
                texts = [PL.problem(pid, p) for pid, p in msgs]
                push(texts[0] if len(texts) == 1 else "\n".join("- " + t for t in texts),
                     title=(f"{NAME}: urgent" if urgent else f"{NAME}: needs attention") + (f" ({len(texts)} items)" if len(texts) > 1 else ""),
                     priority="high" if urgent else "default", tags="rotating_light" if urgent else "warning")
                for _, p in msgs:
                    p["notified"] = True
        except Exception as e:  # noqa: BLE001  push failed: try again next time
            sent_ok = False
            log.append(f"push failed: {e}")
        if db is not None and NT.D.writable(db):
            NT.save_problems(db, cur, now.strftime("%Y-%m-%d %H:%M"))
        import events
        events.lines("health", log, db=db)
    return [p for _, p in msgs], sent_ok


def progress_snapshot(s, db):
    """Each task's state, progress and where its best copy is -> the progress table (only what changed)."""
    import events
    import overview
    tasks = [dict(key=t["key"], state=(t["queue"] or {}).get("state"), progress=t["progress"], site=(t["copy"] or {}).get("where"),
                  data=dict(epochs=(t["copy"] or {}).get("epochs"), total=t["total"])) for t in overview.build(s)["tasks"]]
    return events.snapshot(db, tasks)


def from_step(v):
    """A stuck report from feed (relay) / advance: message (str), or dict(msg, data[, level]) -> problem."""
    if isinstance(v, dict):
        return problem(v["msg"], level=v.get("level", "normal"), data=v.get("data"), after=v.get("after", 0))  # level="info": only waiting, recorded but not pushed
    return problem(v)


def main(argv=None):
    ap = argparse.ArgumentParser(description="One health check round: check machines, push problems, queue downstream steps, send tasks")
    ap.add_argument("-n", "--dry-run", action="store_true", help="only print problems found this round; push nothing, change no file")
    ap.add_argument("--test", action="store_true", help="push a test message")
    a = ap.parse_args(argv)
    if LOAD_ERROR is not None:
        load_failed(LOAD_ERROR, dry=a.dry_run)
        return 1
    import events
    db = NT.open_db(write=not a.dry_run)
    if not a.dry_run and db is not None and NT.D.writable(db):
        NT.D.set_meta(db, "load_error", None)
        try:
            events.flush(db)
        except Exception as e:  # noqa: BLE001
            print(f"event spool not imported into the database: {e}", file=sys.stderr)
    if a.test:
        push("test: a health check push reaches the phone", tags="tada")
        print("test message pushed")
        return 0
    s = gather()
    if not a.dry_run:
        s["records"] = C.update_records(lambda recs: C.sync_records(s["pueue"], recs))
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
    # An error in these steps (including an import error) must not stop the health check itself (alerts and the ledger depend on it):
    # report it and carry on.
    events, stuck = list(revived), {}
    try:  # queue downstream steps first, so what this round queues is sent this round
        import advance  # queue/advance.py: queue the next step per experiment table
        done, stuck2 = advance.run(s, dry=a.dry_run)
        if done and not a.dry_run:
            try:
                s["taskq"][M.hub()] = taskq.local_status()
            except taskq.DiskError as e:
                s["taskq"][M.hub()], s["why"]["taskq"] = None, str(e)
    except Exception as e:  # noqa: BLE001
        done, stuck2 = [], {"advance:error": f"queue/advance.py failed: {type(e).__name__}: {e}"[:300]}
    try:
        import feed  # queue/feed.py: the only way tasks are sent, hands over to relay.py
        ev, st = feed.run_all(s, dry=a.dry_run)
        events.extend(ev)
        stuck.update(st)
    except Exception as e:  # noqa: BLE001
        stuck["feed:error"] = f"queue/feed.py failed: {type(e).__name__}: {e}"[:300]
    for pid, v in {**stuck, **stuck2}.items():
        P[pid] = from_step(v)
    if not a.dry_run and (db is None or not NT.D.writable(db)):
        P[alert_id("db", M.hub())] = problem(f"cannot write the hub database {NT.D.path()} (on a network disk, or created by a newer "
                                             "taskq): every check will push these problems again", level="urgent")
    P = drop_acked(P, db, dry=a.dry_run)
    update(P, s["now"], db, dry=a.dry_run)
    if not a.dry_run and db is not None and NT.D.writable(db):
        try:
            progress_snapshot(s, db)
        except Exception as e:  # noqa: BLE001
            print(f"progress snapshot not recorded: {type(e).__name__}: {e}", file=sys.stderr)
    if a.dry_run:
        for pid, p in sorted(P.items()):
            wait = f" / reported after {p['after']} min" if p["after"] else ""
            print(f"[{p['level']}/{p['kind']}{wait}] {pid}\n    {p['msg']}")
        print("\nSending, topping up workers: " + "; ".join(events))
        print(f"{M.hub()} advance by state: " + ("; ".join(done) or "nothing to queue"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
