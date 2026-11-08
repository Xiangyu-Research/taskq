#!/usr/bin/env python3
"""Test tasklog.py (taskq log): a few part logs, the event log (test database) and task records in a fake data dir.

    python3 queue/test_tasklog.py
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
T = fixture.tmpdir()
for n in ("relay", "feed", "advance", "health"):
    os.environ[n.upper() + "_LOG"] = os.path.join(T, n + ".log")
os.environ["TASKQ_TASKS"] = os.path.join(T, "tasks.json")
os.environ.pop("TASKQ_DATA", None)
sys.path.insert(0, HERE)
import proto  # noqa: E402
import tasklog as L  # noqa: E402

fails = 0


def ok(what, cond, got=None):
    global fails
    print(("PASS:" if cond else "FAIL:") + what + ("" if cond or got is None else f" (got {got!r})"))
    fails += 0 if cond else 1


KEY = "demo/a/0"
D = os.path.join(T, "data")
LD = os.path.join(D, proto.TASK_LOGS)
os.makedirs(LD)


def head(job, part, key=KEY, t="2026-11-01 10:00:00", env="base", code="abc1234"):
    return proto.LOG_HEAD_FMT % (f"{job}_{part}", "node1", key, part, env, code, t) + "\n"


def write(name, text, age=3600):
    p = os.path.join(LD, name)
    with open(p, "w") as f:
        f.write(text)
    t = time.time() - age
    os.utime(p, (t, t))
    return p


S = proto.safe(KEY)
write(f"{S}__1001_part1.log", head(1001, 1) + "2026-11-01 10:00:05 start\nEpoch 3\n"
      f"{proto.STALL_LINE} 2026-11-01 12:00:00: log and output dir unchanged for 30 minutes, job 1001 stopped it\n")
write(f"{S}__1002_part1.log", head(1002, 1, t="2026-11-02 08:00:00") + "2026-11-02 09:00:00 some output\n")
write(f"{S}__gpu-host-202611030900_part2.log", head("gpu-host-202611030900", 2, t="2026-11-03 09:00:00")
      + "2026-11-03 09:10:00 command ended (exit 1), not done (attempt 1)\n2026-11-03 09:15:00 command ended (exit 0), not done (attempt 2)\n")
write(f"{S}__1004_part1.log", head(1004, 1, t="2026-11-04 08:00:00", env="unknown") + "2026-11-04 08:00:00 not started: this machine has no env x\n")
write(f"{S}__1005_part1.log", head(1005, 1, t="2026-11-05 07:00:00") + f"{proto.DONE_LINE} 2026-11-05 09:30:00\n")
write(f"{S}__1006_part1.log", head(1006, 1, t="2026-11-05 10:00:00") + "2026-11-05 10:01:00 Epoch 0\n", age=10)
write(f"{S}__1007_part1.log", head(1007, 1, t="2026-11-05 11:00:00")
      + "2026-11-05 11:30:00 GPU 1 busy, waited 30 minutes, not running\n")
write(f"{S}__1008_part1.log", head(1008, 1, t="2026-11-05 12:00:00") + "2026-11-05 12:05:00 command ended (exit 1), disk unreadable, cannot tell whether training finished\n")
# not counted: another task with a longer key, a header for another key, another task
write(f"{S}__x__2001_part1.log", head(2001, 1, key=KEY + "/x"))
write(f"{S}__2002_part1.log", head(2002, 1, key="demo/zzz"))
write(f"{proto.safe('demo/b/0')}__2003_part1.log", head(2003, 1, key="demo/b/0"))

import db as DB  # noqa: E402
import events as EV  # noqa: E402
DB.connect(create=True).close()
_t = lambda *x: time.mktime(x + (0, 0, -1))  # noqa: E731
EV.add("relay", f"site-pueue claimed {KEY} (gpu1 group)", t=_t(2026, 11, 1, 9, 0, 0))  # rows without a key match on text
EV.add("relay", f"site-pueue claimed {KEY}0 (not counted)", t=_t(2026, 11, 1, 9, 0, 1))
EV.add("relay", f"site-pueue claimed {KEY}/x (not counted)", t=_t(2026, 11, 1, 9, 0, 2))
EV.add("advance", f"queued fetch: {KEY}", key=KEY, t=_t(2026, 10, 31, 8, 0, 0))
EV.add("note", "looked at it", key=KEY, kind="fix", t=_t(2026, 11, 2, 9, 0, 0))
EV.add("note", "other task", key=KEY + "/x")
with open(os.environ["TASKQ_TASKS"], "w") as f:
    json.dump([dict(machine="hub", id=5, label="fetch:a0", added=1762300000.0, step="fetch", key=KEY, src="site-pueue"),
               dict(machine="hub", id=6, label="fetch:b0", added=1762300001.0, step="fetch", key="demo/b/0")], f)

r = L.collect(KEY, D)
A = r["attempts"]
ok("only this task's parts (8, by start time)", [a["job"] for a in A] ==
   ["1001", "1002", "gpu-host-202611030900", "1004", "1005", "1006", "1007", "1008"], [a["job"] for a in A])
st = {a["job"]: a["state"] for a in A}
ok("states", st == {"1001": "stalled", "1002": "ended", "gpu-host-202611030900": "not_done", "1004": "not_started", "1005": "done",
                    "1006": "running", "1007": "gpu_busy", "1008": "unreadable"}, st)
ok("every state has a description", all(a["state"] in proto.TASKLOG_STATES for a in A))
a = A[2]
ok("job name without part number, part, host, env, code, start", (a["job"], a["part"], a["host"], a["env"], a["code"], a["start"]) ==
   ("gpu-host-202611030900", 2, "node1", "base", "abc1234", "2026-11-03 09:00:00"), a)
ok("several command ends: the last one wins", (a["exit"], a["end"]) == (0, "2026-11-03 09:15:00"), a)
ok("machine label from the job name (plain numbers are the shared prefix of Slurm labels, others by hostname)", A[0]["machine"] == "Slurm" and a["machine"] == "pueue", (A[0]["machine"], a["machine"]))
ok("stalled: end time and reason", (A[0]["end"], A[0]["note"]) == ("2026-11-01 12:00:00", "log and output dir unchanged for 30 minutes, job 1001 stopped it"), A[0])
ok("no end: last is the last line with a time", (A[1]["end"], A[1]["last"]) == (None, "2026-11-02 09:00:00"), A[1])
ok("not started: reason", A[3]["note"] == "this machine has no env x" and A[3]["env"] == "unknown", A[3])
ok("done: end time", (A[4]["end"], A[4]["exit"]) == ("2026-11-05 09:30:00", 0), A[4])
ok("fields", all(tuple(x) == proto.TASKLOG_ATTEMPT for x in A), list(A[0]))
ok("path is the full path", os.path.isfile(A[0]["path"]) and A[0]["file"] == os.path.basename(A[0]["path"]))
ev = [(e["log"], e["time"]) for e in r["events"]]
ok("event log rows about it (by key when recorded, else by text; a key followed by more is another task; by time)",
   ev == [("advance", "2026-10-31 08:00:00"), ("relay", "2026-11-01 09:00:00"), ("note", "2026-11-02 09:00:00")], ev)
ok("event fields", all(tuple(e) == proto.TASKLOG_EVENT for e in r["events"]))
ok("event text as stored", r["events"][0]["text"] == f"queued fetch: {KEY}", r["events"][0])
ok("task records only for this key, as stored", [x["id"] for x in r["records"]] == [5] and r["records"][0]["src"] == "site-pueue", r["records"])
ok("top-level fields", tuple(r) == proto.TASKLOG_TOP, list(r))

# command line: taskq log (--data, --json, text)
env = dict(os.environ)
tq = os.path.join(os.path.dirname(HERE), "bin", "taskq")
p = subprocess.run([sys.executable, tq, "log", KEY, "--data", D, "--json"], capture_output=True, text=True, env=env)
j = json.loads(p.stdout) if p.returncode == 0 else {}
ok("taskq log --json", p.returncode == 0 and len(j.get("attempts", [])) == 8 and j["data"] == D, p.stderr[-300:])
env["TASKQ_DATA"] = D
p = subprocess.run([sys.executable, tq, "log", KEY], capture_output=True, text=True, env=env)
ok("taskq log text (data dir from TASKQ_DATA)", p.returncode == 0 and "8 parts" in p.stdout and "part 2  job gpu-host-202611030900 (pueue)" in p.stdout
   and "stopped by the worker as stalled" in p.stdout and "3 event log rows about it" in p.stdout, p.stdout + p.stderr)
p = subprocess.run([sys.executable, tq, "log", "demo/none", "--data", os.path.join(T, "nope")], capture_output=True, text=True, env=env)
ok("--data missing -> exit 2", p.returncode == 2, p.returncode)

print(f"\n{'all pass' if not fails else f'{fails} failed'}")
sys.exit(1 if fails else 0)
