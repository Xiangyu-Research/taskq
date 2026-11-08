#!/usr/bin/env python3
"""One task's history: each training part (run_task.sh's part logs), event log rows about it, and hub task records.

    taskq log <key> [--data DIR] [--json]

Parts: each header line (proto.LOG_HEAD) in <data dir>/logs/tasks/<safe key>__<job>_part<part>.log is one part: job, machine
(by job name), host, part number, env, code, start; later lines give the end (proto.TASKLOG_STATES): done, command ended and
exit code, not started, GPU never free, stopped as stalled; with none, a log updated within FRESH_LOG is still running,
otherwise it has no end record. path is the log's full path; other content in the log (the task's own output) is the user's
to read.
Data dir: --data, else cfg.data_dir(). On the hub, parts trained on executors appear only after the fetch (the fetch brings
this task's part logs); for parts still on an executor see taskq status --all --json's parts, or taskq log --data
<root>/data there.
The event log (hub database) and task records (tasks.json) exist only on the hub; elsewhere they are empty.
--json: one JSON line, fields in proto's TASKLOG_*.
"""
import argparse
import glob
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402

HEAD = re.compile(proto.LOG_HEAD)
TS = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
LINE_TS = re.compile(r"^" + TS + r"(?:\.\d+)?:? ?(.*)$")
ENDED = re.compile(r"^" + TS + r" " + proto.LOG_ENDED_RE + r"$")
NOT_STARTED = re.compile(r"^" + TS + r" " + proto.LOG_NOT_STARTED_RE + r"$")
GPU_BUSY = re.compile(r"^" + TS + r" " + proto.LOG_GPU_BUSY_RE + r"$")
STALLED = re.compile(r"^" + re.escape(proto.STALL_LINE) + r" " + TS + r":? ?(.*)$")


def data_dir(arg=None):
    if arg:
        return os.path.expanduser(arg)
    try:
        return cfg.data_dir()
    except Exception:  # noqa: BLE001
        return ""


def machine_of(job):
    try:
        return cfg.mtab().machine_of_jobtag(job)
    except Exception:  # noqa: BLE001
        return None


def log_files(key, data):
    """This task's part logs (not another task's with a longer key: no SAFE after the prefix)."""
    pre = proto.safe(key) + proto.SAFE
    d = os.path.join(data, proto.TASK_LOGS)
    out = [p for p in glob.glob(os.path.join(glob.escape(d), glob.escape(pre) + "*.log"))
           if proto.SAFE not in os.path.basename(p)[len(pre):]]
    return sorted(out)


def parse(path, key, now=None):
    """One part log -> [parts] (proto.TASKLOG_ATTEMPT)."""
    out, cur = [], None
    with open(path, errors="replace") as f:
        for ln in f:
            ln = ln.rstrip("\n")
            m = HEAD.match(ln)
            if m:
                cur = None
                if m.group(3) != key:
                    continue
                job, part = m.group(1), int(m.group(4))
                job = job[:-len(f"_{part}")] if job.endswith(f"_{part}") else job
                cur = dict(file=os.path.basename(path), path=path, job=job, machine=machine_of(job), host=m.group(2), part=part,
                           env=m.group(5), code=m.group(6), start=m.group(7), end=None, last=m.group(7), state=None, exit=None, note=None)
                out.append(cur)
                continue
            if cur is None:
                continue
            t = LINE_TS.match(ln)
            if t:
                cur["last"] = t.group(1)
            if ln.startswith(proto.DONE_LINE):
                t = re.search(TS, ln)
                cur.update(state="done", end=t.group(1) if t else cur["last"], exit=0)
            elif STALLED.match(ln):
                m = STALLED.match(ln)
                cur.update(state="stalled", end=m.group(1), note=m.group(2))
            elif ENDED.match(ln):
                m = ENDED.match(ln)
                rc, rest = int(m.group(2)), m.group(3)
                st = "unreadable" if rest.startswith(proto.LOG_UNREADABLE) else ("not_done" if rc == 0 else "failed")
                cur.update(state=st, end=m.group(1), exit=rc)
            elif NOT_STARTED.match(ln):
                m = NOT_STARTED.match(ln)
                cur.update(state="not_started", end=m.group(1), note=m.group(2))
            elif GPU_BUSY.match(ln):
                cur.update(state="gpu_busy", end=GPU_BUSY.match(ln).group(1))
    if out and out[-1]["state"] is None:
        try:
            age = (now or time.time()) - os.path.getmtime(path)
        except OSError:
            age = None
        out[-1]["state"] = "running" if age is not None and age < proto.FRESH_LOG else None
    for a in out:
        a["state"] = a["state"] or "ended"
    return out


def attempts(key, data, now=None):
    out = [a for p in log_files(key, data) for a in parse(p, key, now)]
    return sorted(out, key=lambda a: (a["start"], a["part"]))


def events(key):
    """Event log rows about this key (hub database; none elsewhere). A key followed by a letter, digit or / is another task."""
    try:
        import db as D
        import events as EV
        db = D.connect(ro=True)
    except Exception:  # noqa: BLE001
        return []
    if db is None:
        return []
    try:
        if not D.has(db, "log"):
            return []
        return [dict(log=r["source"], time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["t"])), text=r["msg"])
                for r in EV.query(db, key=key)]
    finally:
        db.close()


def records(key):
    try:
        import common
        return [r for r in common.load_records() if r.get("key") == key]
    except Exception:  # noqa: BLE001
        return []


def collect(key, data, now=None):
    return dict(version=proto.VERSION, key=key, data=data, attempts=attempts(key, data, now) if data else [],
                events=events(key), records=records(key))


def show(r):
    print(f"task {r['key']} (data dir {r['data'] or 'unknown'}): {len(r['attempts'])} parts")
    for a in r["attempts"]:
        res = proto.TASKLOG_STATES.get(a["state"], a["state"])
        if a["exit"] not in (None, 0):
            res += f" (exit {a['exit']})"
        if a["note"]:
            res += f": {a['note']}"
        where = a["machine"] if a["machine"] and a["machine"] != a["job"] else ""
        print(f"  part {a['part']}  job {a['job']}{f' ({where})' if where else ''} @ {a['host']}  {a['start']} -> {a['end'] or a['last']}  "
              f"{res}  env {a['env']}  code {a['code']}")
    if r["events"]:
        print(f"\n{len(r['events'])} event log rows about it:")
        for e in r["events"]:
            print(f"  {e['time'] or '':19}  {e['log']:8} {e['text']}")
    if r["records"]:
        print(f"\n{len(r['records'])} hub task records:")
        for x in r["records"]:
            when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x["added"])) if isinstance(x.get("added"), (int, float)) else ""
            rest = {k: v for k, v in x.items() if k not in ("key", "added", "machine", "step", "label")}
            print(f"  {when}  {x.get('machine')}  {x.get('step')}  {x.get('label')}  {json.dumps(rest, ensure_ascii=False)}")


def main(argv):
    ap = argparse.ArgumentParser(description="One task's training parts, event log rows about it, and hub task records")
    ap.add_argument("key")
    ap.add_argument("--data", help="data dir (default cfg.data_dir())")
    ap.add_argument("--json", action="store_true", help="one JSON line (proto's TASKLOG_*)")
    a = ap.parse_args(argv)
    data = data_dir(a.data)
    if a.data and not os.path.isdir(data):
        print(f"taskq log: no data dir {data}", file=sys.stderr)
        return 2
    r = collect(a.key, data)
    if a.json:
        print(json.dumps(r, ensure_ascii=False))
    else:
        show(r)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
