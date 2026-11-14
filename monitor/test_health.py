#!/usr/bin/env python3
"""Test health.py's check and push rules: fake queue, logs and clock; no machines, no real pushes.

    python3 monitor/test_health.py
"""
import json
import os
import re
import sys
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue", "testdata"))
import fixture  # noqa: E402
fixture.use()
os.environ["TASKQ_TASKS"] = os.path.join(fixture.tmpdir(), "tasks.json")  # do not touch real task records
os.environ["TASKQ_LAST"] = os.path.join(fixture.tmpdir(), "last")
os.environ["TASKQ_STATE"] = fixture.tmpdir()  # health's own state (pueue_seen, ...) does not land in HOME either
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "queue"))
import health as H  # noqa: E402
import cfg  # noqa: E402

H.policy.PAUSE = {}  # not affected by paused machines in the config

NOW = datetime(2026, 10, 16, 12, 0)
MET2 = "D116_gamma/V_save/2"
MEN0 = "D111_beta/V_save_n470/0"
PH = "site-pueue"  # the pueue executor in the test config (ssh fake-pueue-host, label pueue); hub is also the store
fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def pt(dt):  # pueue time format
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000000000+08:00")


def prog(final=False, epochs=100, end=NOW):
    return dict(final=final, runs=[dict(start=ts(end - timedelta(hours=5)), end=ts(end), epochs_done=epochs, tl=[])], metrics={}, debug={})


def fields(status):
    """result and code qs splits out of status (e.g. "Failed (143)" -> Failed, 143)."""
    m = re.fullmatch(r"(\w+)(?: \((-?\d+)\))?", status)
    done = m[1] not in H.LIVE
    return dict(result=m[1] if done else None, code=int(m[2]) if m[2] else 0 if m[1] == "Success" else None)


def task(i, group, status, cmd, label="", start=NOW - timedelta(hours=3), end=None):
    return dict(id=i, group=group, status=status, **fields(status), label=label, deps=[], start=pt(start) if start else None,
                end=pt(end) if end else None, command=cmd)


PH_BUSY = {"groups": {}, "tasks": [dict(id=90, group="gpu1", status="Running", label="busy_a", deps=[], start=None, end=None,
                                         command="bash a"),
                                    dict(id=91, group="gpu1", status="Queued", label="busy_b", deps=[], start=None, end=None,
                                         command="bash b")]}


def snap(**kw):
    """Shape of gather(): pueue machine (site-pueue) probed and pueue readable; job status collected on the store (hub)."""
    s = dict(now=NOW, ok={PH: True},
             probe={PH: {"DISK": str(int(200e9)), "GPULOG": ts(NOW - timedelta(minutes=1)), "BOOT": "2026-10-01 08:00:00|"}},
             src={"hub": {"tasks": {}}},
             pueue={PH: PH_BUSY})
    s.update(kw)
    return s


def ids(s):
    return set(H.check(s))


# ---------------- check ----------------
ok("everything fine: no problems", ids(snap()) == set())

s = snap(pueue={PH: {"groups": {}, "tasks": [
    task(1, "gpu1", "Failed (1)", "bash x", "infer_a", end=NOW - timedelta(hours=1)),
    task(2, "cpu", "DependencyFailed", "python y", "oof_a", end=NOW - timedelta(hours=1)),
    task(3, "gpu1", "Failed (1)", "bash z", "old", end=NOW - timedelta(hours=30)),
    task(4, "cpu", "Failed (143)", "bash w", "stopped", end=NOW - timedelta(hours=1)),
    task(5, "cpu", "Killed", "bash v", "killed", end=NOW - timedelta(hours=1)),
    task(6, "gpu1", "Failed (137)", "bash u", "oom", end=NOW - timedelta(hours=1)),
    task(8, "gpu0", "Killed", "bash t", "worker:w1", end=NOW - timedelta(hours=1)),
    task(9, "gpu0", "Failed (1)", "bash s", "worker:w2", end=NOW - timedelta(hours=1))]}})
P = H.check(s)
ok("failed tasks and dependency failures are both reported", any(p.startswith(f"pueue:{PH}:1:") for p in P) and any(p.startswith(f"pueue:{PH}:2:") for p in P))
ok("failures older than 24 hours are not reported", not any(p.startswith(f"pueue:{PH}:3:") for p in P))
_rr = [dict(machine=PH, id=i, label="relay_fetch:k", step="relay_fetch", key="k", added=0.0) for i in (21, 22)]
_P2 = H.check(snap(records=_rr, pueue={PH: {"groups": {}, "tasks": [
    task(21, "wait", "Failed (1)", "x", "relay_fetch:k", end=NOW - timedelta(hours=1)),
    task(22, "wait", "Running", "x", "relay_fetch:k")]}}))
ok("an old failed task whose step and key were queued again later is not reported", not any(p.startswith(f"pueue:{PH}:21:") for p in _P2))
ok("stopped by hand (143, pueue kill) is logged but not pushed", [p["level"] for k, p in P.items() if k.startswith((f"pueue:{PH}:4:", f"pueue:{PH}:5:"))] == ["info"] * 2)
ok("a job killed by a signal (137, usually out of memory) is still reported", [p["level"] for k, p in P.items() if k.startswith(f"pueue:{PH}:6:")] == ["normal"])
ok("a worker stopped by the scheduler (hours end, card yield) is not reported; a worker error is", not any(p.startswith(f"pueue:{PH}:8:") for p in P)
   and any(p.startswith(f"pueue:{PH}:9:") for p in P))

s = snap(src={"hub": {"tasks": {}, "errors": {MEN0: "OSError: [Errno 5] Input/output error"}}})
ok("reading task status on the store failed (collect errors) -> reported separately, not as unreadable", "readerr:hub" in ids(s) and "unread:hub" not in ids(s))


# ---------------- hub queue to executors ----------------
def tqrow(key, state, worker=None, since=None, log_age=None, kind="toy", task_log_age=None, out_age=None):
    return dict(key=key, state=state, worker=worker, host=worker, since=since, beat=None, note="", log_age=log_age, failures=0,
                kind=kind, nice=0, order=1, task_log_age=task_log_age, out_age=out_age)


INF = "infer/D103_alpha_one/x/frag_2"

ok("hub queue unreadable -> reported", "taskq:hub" in H.check(snap(taskq={"hub": None})))
ok("hub queue off (no ENABLED) -> reported after 60 minutes", H.check(snap(taskq={"hub": dict(enabled=False, tasks=[])})).get("tqoff:hub", {}).get("after") == 60)
bq = [dict(tqrow(INF, "blocked", None), note="waiting for x, failed 2 times"), dict(tqrow(MET2, "claimed", PH), assigned=PH)]
P = H.check(snap(taskq={"hub": dict(enabled=True, paused=False, tasks=bq)}))
ok("blocked tasks in the hub queue are not reported by the health check (relay reports tqblocked when all executors skipped it)", f"tqblocked:{INF}" not in P)


import subprocess  # noqa: E402

# ---------------- gather: cards and their processes, local only, job status collected on the store ----------------
fakebin = fixture.tmpdir()
sl = subprocess.Popen(["sleep", "30"])
with open(os.path.join(fakebin, "nvidia-smi"), "w") as f:
    f.write(f'#!/bin/bash\ncase "$*" in *query-gpu=*) echo "0, GPU-a, 37, 1200"; echo "1, GPU-b, 0, 5";;\n'
            f'*query-compute-apps*) echo "GPU-a, {sl.pid}, 1100"; echo "GPU-x, 1, 9";; esac\n')
os.chmod(os.path.join(fakebin, "nvidia-smi"), 0o755)
out = subprocess.run(["bash", "-s"], input=H.GPU_PROBE, capture_output=True, text=True,
                     env=dict(os.environ, PATH=fakebin + ":" + os.environ["PATH"])).stdout
sl.kill()
gp = H.gpus_of(out.splitlines())
ok("GPU probe real run: one item per card, processes carry the command line, processes for unknown cards are dropped",
   [(x["gpu"], x["util"], x["mem"]) for x in gp] == [("0", "37", "1200"), ("1", "0", "5")]
   and [(a["pid"], a["mem"], a["args"]) for a in gp[0]["apps"]] == [(str(sl.pid), "1100", "sleep 30")] and gp[1]["apps"] == [])
ok("the probe script has the GPU lines; machines without gpu_log, disk keep the default (not checked)", H.GPU_PROBE in H.probe_script(H.M.machine("hub"))
   and "GPULOG" not in H.probe_script(H.M.machine("hub")) and "GPULOG" in H.probe_script(H.M.machine(PH)))
calls = []
saved = (H.sh, H.run_on, H.RM.store_op, H.taskq.local_status)
H.taskq.local_status = lambda: dict(enabled=True, tasks=[])  # hub queue


def fake_sh(cmd, inp=None, timeout=120):
    calls.append(("sh", cmd))
    if cmd[0] == "git":
        return 0, "abc\n"
    return 0, json.dumps({n: {"groups": {}, "tasks": []} for n in (cmd[2:] or H.M.pueue_hosts())})


def fake_run_on(m, script, timeout=120):
    calls.append(("run_on", m["label"]))
    return 0, "BOOT|2026-10-01 08:00:00|\nDISK|123\nGPU|0, GPU-a, 37, 1200\nAPP|GPU-a| 42| 1100|python x|y\n"


COLLECTED = {"tasks": {MEN0: prog()}, "seglogs": []}


def fake_store_op(what, data, timeout=600, say=None):
    calls.append(("store_op", what, sorted(data)))
    return COLLECTED


H.sh, H.run_on, H.RM.store_op = fake_sh, fake_run_on, fake_store_op
g = H.gather()
ok("gather: GPU and APP lines go to gpus, not probe; a | inside the command line is kept",
   g["probe"][PH] == {"BOOT": "2026-10-01 08:00:00|", "DISK": "123"}
   and g["gpus"][PH] == [dict(gpu="0", uuid="GPU-a", util="37", mem="1200", apps=[dict(pid="42", mem="1100", args="python x|y")])])
ok("gather: only machines with a pueue group are probed and asked for pueue (pueue executor, hub; other machines are not connected)",
   set(H.M.pueue_hosts()) >= {"hub", PH} and set(g["ok"]) == set(g["pueue"]) == set(H.M.pueue_hosts())
   and {c[1] for c in calls if c[0] == "run_on"} == {H.M.MACHINES[n]["label"] for n in H.M.pueue_hosts()})
ok("gather: job status is collected on the store (site_ops collect), filed under the store's name",
   [c for c in calls if c[0] == "store_op"] == [("store_op", "collect", ["specs", "wait"])] and g["src"] == {"hub": COLLECTED})


def broken_store_op(what, data, timeout=600, say=None):
    raise RuntimeError("collect on the hub failed (124): timeout")


H.RM.store_op = broken_store_op
g = H.gather()
ok("gather: collection on the store failed -> record the reason, report collect (not machine down: relay still swaps its bundle), no error",
   g["ok"]["hub"] is not False and "timeout" in g["collect"]["hub"] and g["src"] == {}
   and "collect:hub" in H.check(dict(g, now=NOW)) and "down:hub" not in H.check(dict(g, now=NOW)))
H.RM.store_op = fake_store_op
calls.clear()
H.policy.PAUSE = {PH: (None, "maintenance")}
g = H.gather()
P_off = H.check(dict(g, now=NOW))
ok("gather: a paused machine is not connected (not probed), counts as unreachable, but down is not reported",
   not [c for c in calls if c[0] == "run_on" and c[1] == H.M.MACHINES[PH]["label"]] and g["ok"][PH] is False
   and "paused: maintenance" in g["why"][PH] and f"down:{PH}" not in P_off)
H.policy.PAUSE = {}
calls.clear()
g = H.gather(remote=False)
qs_call = next(c[1] for c in calls if c[0] == "sh" and c[1][0] != "git")
ok("gather(remote=False): only probe local machines with a pueue group (hub), qs asks only it (no other machines); the local store is still collected",
   [c for c in calls if c[0] == "run_on"] == [("run_on", "hub")] and qs_call[1:] == ["--json", "hub"]
   and set(g["pueue"]) == {"hub"} and set(g["taskq"]) == {"hub"} and g["src"] == {"hub": COLLECTED})
H.M.MACHINES["loc"] = dict(label="loc", hostname=r"\A\Z", ssh=None, scheduler="pueue", root="/tmp/loc_root", workers=[dict(cpus=2)])
calls.clear()
g = H.gather(remote=False)
qs_call = next(c[1] for c in calls if c[0] == "sh" and c[1][0] != "git")
ok("gather(remote=False): a local pueue executor (ssh=None) is probed too; qs asks the hub and it",
   sorted(c[1] for c in calls if c[0] == "run_on") == ["hub", "loc"] and sorted(qs_call[2:]) == ["hub", "loc"]
   and set(g["ok"]) == set(g["pueue"]) == {"hub", "loc"})
del H.M.MACHINES["loc"]
H.sh, H.run_on, H.RM.store_op = saved[:3]

# Really run collect on the store: code/current under the store root (~/demo_root in the temp HOME) is a hand-built bundle
# (fixture.store_bundle) and python3 is the one running the test; the store's data dir is temporary, a task just finished,
# its part log has been fetched to the store, outputs are still on the executor (not on the store)
_home, _data = fixture.tmpdir(), fixture.tmpdir()
_py = os.path.join(fixture.store_bundle(os.path.join(_home, "demo_root")), "bin")
_seg = H.proto.task_log(MEN0, "77")
os.makedirs(os.path.dirname(os.path.join(_data, _seg)))
open(os.path.join(_data, _seg), "w").write("job h_1 @ hub, task=k, part 1, env base, code abc1234, 2026-11-01 10:00:00\n")
_env, _mdata, _rpy = os.environ["HOME"], H.M.MACHINES["hub"]["data"], H.RM.py
os.environ["HOME"], H.M.MACHINES["hub"]["data"], H.RM.py = _home, _data, lambda name: os.path.join(_py, "python3")
try:
    H.sh, H.run_on = fake_sh, fake_run_on
    g = H.gather()
finally:
    os.environ["HOME"], H.M.MACHINES["hub"]["data"], H.RM.py = _env, _mdata, _rpy
    H.sh, H.run_on = saved[:2]
ok("gather really runs collect on the store: a task whose outputs are still on the executor has no progress, its part log on the store is seen",
   g["ok"].get("hub") is not False and MEN0 not in (g["src"].get("hub") or {}).get("tasks", {"x": 1})
   and [x["name"] for x in g["src"]["hub"]["seglogs"]] == [os.path.basename(_seg)])
if g["ok"].get("hub") is False:
    print(g["why"]["hub"])
H.taskq.local_status = saved[3]

# ---------------- notices, acks, problems: the hub database (monitor/notice.py) ----------------
import notice as NT  # noqa: E402
_nh = fixture.tmpdir()
_nenv = dict(os.environ, HOME=_nh)
TQ = os.path.join(os.path.dirname(HERE), "bin", "taskq")


def tqn(*a, inp=None):
    return subprocess.run([sys.executable, TQ, "notice", *a], capture_output=True, text=True, env=_nenv, input=inp, timeout=60)


r1 = tqn("set", "board", "board has new results", "waiting for confirmation")
r2 = tqn("set", "b-2_x", inp="from stdin\n")
r3 = tqn("list", "--json")
_nl = json.loads(r3.stdout)
_db = NT.open_db(write=False)
ok("notice set: text from arguments or stdin goes into the database; list --json lists them by name",
   r1.returncode == r2.returncode == r3.returncode == 0 and NT.notices(_db)["board"]["text"] == "board has new results waiting for confirmation"
   and tuple(_nl) == H.proto.NOTICE_TOP and [x["name"] for x in _nl["notices"]] == ["b-2_x", "board"]
   and _nl["notices"][0]["text"] == "from stdin" and all(tuple(x) == H.proto.NOTICE_FIELDS for x in _nl["notices"]))
r4 = tqn("set", "a/b", "x")
r5 = tqn("set", "x y", "x")
r6 = tqn("set", "empty", inp="")
ok("notice set: bad name or no text -> usage error, nothing written", r4.returncode == r5.returncode == r6.returncode == 2
   and sorted(NT.notices(_db)) == ["b-2_x", "board"])
r7 = tqn("clear", "b-2_x", "nope")
r8 = tqn("list")
ok("notice clear: missing ones are not an error; list as text", r7.returncode == 0 and "No such notice: nope" in r7.stdout
   and r8.stdout.startswith("board (") and "board has new results waiting for confirmation" in r8.stdout and "b-2_x" not in r8.stdout)
ok("notice --help", tqn("--help").returncode == 0 and "set" in tqn("--help").stdout)
r9 = tqn("set", "later", "--at", "3d", "do it later")
ok("notice set --at: listed with its time, not reported before then",
   r9.returncode == 0 and NT.notices(_db, due_only=False)["later"]["at"] > NT.time.strftime(NT.TF) and "later" not in NT.notices(_db))
tqn("clear", "later")
saved = H.sh, H.run_on, H.RM.store_op
H.sh, H.run_on, H.RM.store_op = fake_sh, fake_run_on, fake_store_op
g = H.gather(remote=False)
ok("gather: notices -> s['notices']", g["notices"] == {"board": "board has new results waiting for confirmation"})
tqn("clear", "board")
ok("gather: gone once cleared", H.gather(remote=False)["notices"] == {})
H.sh, H.run_on, H.RM.store_op = saved

# recorded problems: one event, one ongoing
_wdb = NT.open_db()
NT.save_problems(_wdb, {"boot:hub:2026-11-05 13:52:30": H.problem("x", kind="event"), "disk:hub": H.problem("y")}, "2026-11-05 13:00")

# taskq notice ack: only kind=event; acked ones are left out; acks of gone problems are dropped
ra, rb, rc_ = tqn("ack", "boot:hub"), tqn("ack", "disk:hub"), tqn("ack", "nosuch")
ok("notice ack: prefix finds the event; ongoing or unknown problems are not acked (exit 1)",
   ra.returncode == 0 and "boot:hub:2026-11-05 13:52:30" in NT.acked(_wdb) and rb.returncode == 1 and "ongoing problem" in rb.stderr
   and rc_.returncode == 1 and "no such problem" in rc_.stderr)
_P = {"boot:hub:2026-11-05 13:52:30": H.problem("x", kind="event"), "disk:hub": H.problem("y")}
ok("drop_acked: acked events left out, others kept", list(H.drop_acked(dict(_P), _wdb)) == ["disk:hub"])
NT.save_problems(_wdb, {"disk:hub": H.problem("y")}, "2026-11-05 14:00")
ok("an ack outlives the save of the filtered list: still left out on the next check",
   list(H.drop_acked(dict(_P), _wdb)) == ["disk:hub"] and "boot:hub:2026-11-05 13:52:30" in NT.acked(_wdb))
H.drop_acked({"disk:hub": H.problem("y")}, _wdb, dry=True)
ok("dry run: acks kept even when the event is gone", "boot:hub:2026-11-05 13:52:30" in NT.acked(_wdb))
H.drop_acked({"disk:hub": H.problem("y")}, _wdb)
ok("event no longer raised: its ack dropped", NT.acked(_wdb) == {})
rp = subprocess.run([sys.executable, TQ, "problems", "--json", "--all"], capture_output=True, text=True, env=_nenv, timeout=60)
_pj = json.loads(rp.stdout)
ok("taskq problems --json: proto.PROBLEMS_TOP, each problem proto.OVERVIEW_PROBLEM",
   tuple(_pj) == H.proto.PROBLEMS_TOP and [p["id"] for p in _pj["problems"]] == ["disk:hub"]
   and all(tuple(p) == H.proto.OVERVIEW_PROBLEM for p in _pj["problems"]))
NT.save_problems(_wdb, {}, None)

s = snap()
s["probe"][PH]["DISK"] = str(int(10e9))
P = H.check(s)
ok("site-pueue disk under 15 GB -> urgent", P[f"disk:{PH}"]["level"] == "urgent")
s["probe"][PH]["DISK"] = str(int(30e9))
ok("site-pueue disk under 40 GB -> notice", H.check(s)[f"disk:{PH}"]["level"] == "normal")
s["probe"][PH]["GPULOG"] = ts(NOW - timedelta(minutes=30))
P = H.check(s)
ok("GPU records stopped -> reported", f"gpulog:{PH}" in P)

s = snap(notices={"board": "board has new results, confirm before publishing"})
s["probe"][PH]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 boot: put back 3 4"
P = H.check(s)
ok("rebooted -> reported (with the tasks put back)", [p["msg"] for k, p in P.items() if k.startswith("boot:")] == ["pueue rebooted at 10-16 10:00: boot: put back 3 4"])
s["probe"][PH]["BOOT"] = "2026-10-01 08:00:00|"
ok("a reboot long ago is not reported", not [k for k in H.check(s) if k.startswith("boot:")])
ok("active notices -> one notice:<name> each, with the notice text",
   P.get("notice:board", {}).get("msg") == "board has new results, confirm before publishing" and P["notice:board"]["data"] == dict(name="board")
   and "board" not in P)


# ---------------- fifth machine: only add an entry to the table (missing fields use defaults) ----------------
H.M.MACHINES["fake"] = dict(label="fake", hostname=r"fake-host", ssh="fake", scheduler="pueue", root="~/q", conda=("~/c", "e"),
                            workers=[dict(gpu="0")], disk=("/data", 50, 10))
s = snap(pueue={PH: {"groups": {}, "tasks": []}, "fake": {"groups": {}, "tasks": []}},
         src={"hub": {"tasks": {}}, "fake": {"tasks": {}}, PH: {"tasks": {MEN0: prog(end=NOW - timedelta(minutes=5))}}})
s["ok"]["fake"] = True
s["probe"]["fake"] = {"DISK": str(int(5e9)), "BOOT": "2026-10-01 08:00:00|"}
P = H.check(s)
ok("fifth machine: disk checked per table fields; no gpu_log means no GPU records checked", P["disk:fake"]["level"] == "urgent" and "gpulog:fake" not in P)
s["ok"]["fake"] = False
ok("fifth machine unreachable: reported", "down:fake" in ids(s))
del H.M.MACHINES["fake"]

# ---------------- push rules: plain wording, one message, no pushes for recovery or summaries ----------------
_wdb = NT.open_db()
sent = []
H.push = lambda msg, **kw: sent.append((kw.get("title", ""), msg))


def run(P, now):
    sent.clear()
    H.update({k: dict(v) for k, v in P.items()}, now, _wdb)
    return list(sent)


A = {f"disk:{PH}": dict(msg="pueue disk", level="normal", kind="state", after=0, data=dict(path="/data", free_gb=12.0))}
U = {f"pueued:{PH}": dict(msg="not responding", level="urgent", kind="state", after=0)}
day1 = datetime(2026, 10, 16, 12, 0)
out = run(A, day1)
ok("a new daytime problem: one push in plain words", len(out) == 1 and out[0][0] == f"{H.NAME}: needs attention" and "disk fills" in out[0][1])
ok("the same problem is not pushed twice", run(A, day1 + timedelta(minutes=30)) == [])
ok("no push after recovery", run({}, day1 + timedelta(hours=1)) == [])
ok("a normal problem at night is not pushed", run(A, datetime(2026, 10, 16, 23, 0)) == [])
out = run({**A, **U}, datetime(2026, 10, 17, 1, 0))
ok("an urgent problem at night is pushed at once", len(out) == 1 and out[0][0] == f"{H.NAME}: urgent" and "not responding" in out[0][1])
out = run({**A, **U}, datetime(2026, 10, 17, 8, 5))
ok("the first morning check pushes what was held at night", len(out) == 1 and "disk fills" in out[0][1] and "not responding" not in out[0][1])
ok("no push after it was pushed", run(A, datetime(2026, 10, 17, 8, 35)) == [])
D = {"down:site-slurm": dict(msg="Slurm unreachable", level="normal", kind="state", after=60)}
ok("a problem that must last a while is not pushed when it first appears", run({**A, **D}, datetime(2026, 10, 17, 9, 0)) == [])
out = run({**A, **D}, datetime(2026, 10, 17, 10, 0))
ok("pushed once it lasted long enough", len(out) == 1 and "Cannot read the status of Slurm" in out[0][1])
B = {f"code:{PH}": dict(msg="g", level="normal", kind="state", after=0), f"gpulog:{PH}": dict(msg="l", level="normal", kind="state", after=0)}
out = run(B, datetime(2026, 10, 17, 10, 30))
ok("several in one round: one message", len(out) == 1 and " (2 items)" in out[0][0] and out[0][1].count("\n- ") == 1)
INFOS = {f"idle:{PH}:gpu1": dict(msg="only running jobs left", level="info", kind="state", after=0)}
ok("info level is not pushed", run(INFOS, datetime(2026, 10, 17, 11, 0)) == [])
N = {"notice:board": dict(msg="board waiting for confirmation", level="normal", kind="state", after=0)}
ok("notices are not pushed, only listed under problems", run(N, datetime(2026, 10, 17, 11, 30)) == []
   and "notice:board" in NT.problems(_wdb)[0])

# ---------------- feed / advance errors do not stop the health check ----------------
import types  # noqa: E402
got = {}
H.gather, H.update = (lambda: snap()), (lambda P, now, db, dry=False: got.update(P))
fd = types.ModuleType("feed")
feed_calls = []
fd.run_all = lambda s, dry=False: (feed_calls.append(dry), ([], {"x:y": "z"}))[1]
sys.modules["feed"] = fd
sys.modules["relay"] = None  # health.py does not call relay.py directly (sending only enters through feed.run_all)
adv = types.ModuleType("advance")
adv.run = lambda s, dry=False: 1 / 0
sys.modules["advance"] = adv
sys.argv = ["health.py"]
H.main()
ok("advance.py failed: reported, the health check still finishes",
   "advance:error" in got and "ZeroDivisionError" in got["advance:error"]["msg"])
ok("sending has one entry point: feed.run_all called once, its problems arrive, health does not import relay directly",
   feed_calls == [False] and got.get("x:y", {}).get("msg") == "z" and "relay:error" not in got)
sys.modules["feed"] = None  # import fails
got.clear()
H.main()
ok("feed.py import failed: feed:error reported, the health check still finishes", "feed:error" in got and "advance:error" in got)

# ---------------- plain wording (plain.py): labels, task key -> experiment name + task short name ----------------
import plain as PL  # noqa: E402
import experiments as EX  # noqa: E402
_k, _t = next(iter(EX.tasks().items()))
_e = EX.experiment(_t["exp"])
_n = PL.exp_name(_e)
ok("plain: task key -> experiment name + short name (e.g. f0)", PL.key_name(_k) == f"{_n} {EX.short(_t)}" and EX.short(_t))
ok("plain: a per-task step label -> (experiment name, step, short name); an experiment-level one -> short name None",
   PL.label(f"eval:{EX.label(_e, _t)}") == (_n, PL.STEPS["eval"], EX.short(_t)) and PL.label(f"eval:{_e['id']}") == (_n, PL.STEPS["eval"], None))
ok("plain: an unrecognized label -> Other", PL.label("eval:nope_f1") == ("Other", "eval:nope_f1", None))
_pm = "policy.py PLACE['x'] has ['y'], which is not an executor. Downstream steps were not queued this time; fix it, commit and push and the next check recovers automatically"
ok("plain: a bad PLACE (advance:policy) says what is wrong and how to recover, not as a task step failure",
   PL.problem("advance:policy", dict(msg=_pm)).startswith("Downstream steps are stopped") and "['y'], which is not an executor" in PL.problem("advance:policy", dict(msg=_pm)))
ok("plain: a push phrase carries the short name", PL.task(f"infer:{EX.label(_e, _t)}") == f"{_n} {EX.short(_t)} {PL.STEPS['infer']}")

# plain wording from structured data: the push text of each problem kind
_ki = "infer/" + _k
_inf = task(1, "gpu0", "Failed (1)", "bash x", f"infer:{EX.label(_e, _t)}", end=NOW - timedelta(hours=1))
s = snap(pueue={PH: {"groups": {}, "tasks": [_inf, task(2, "cpu", "Failed (1)", "bash y", "", end=NOW - timedelta(hours=1))]}},
         src={"hub": {"tasks": {}, "stuck": ["a/b/0", "c/d/1"]}, PH: {"tasks": {MEN0: prog(end=NOW - timedelta(hours=2))}}},
         notices={"board": "board has new results, confirm before publishing (3 files)."})
s["probe"][PH]["DISK"] = str(int(10e9))
s["probe"][PH]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 boot: put back 3 4"
P = H.check(s)
_rl = {f"site:{PH}:fail:{MEN0}": dict(msg="...failed twice, with progress...", data=dict(progress=True, failures=2)),
       f"site:{PH}:fail:{_ki}": dict(msg="...failed...", data=dict(progress=False)),
       f"site:{PH}:orphan:zz": dict(msg="...unrecognized...", data=dict(unknown=True)),
       f"site:{PH}:orphan:{MEN0}": dict(msg="...not claimed there...", data=dict(unknown=False)),
       f"tqblocked:{_ki}": dict(msg="...all skipped...", data=dict(key=_ki))}  # reported by relay
P.update({k: H.from_step(v) for k, v in _rl.items()})
_f, _l1, _pe = PL.key_name(MEN0), H.M.machine(PH)["label"], sorted(k for k in P if k.startswith(f"pueue:{PH}:"))
_lh = H.M.machine("hub")["label"]
want = {"unread:hub": f"2 tasks on {_lh} have unreadable files (its storage is probably stuck); they are not sent out for now, the rest continue",
        _pe[0]: f"{_l1} {PL.task(f'infer:{EX.label(_e, _t)}')} failed and stopped",
        _pe[1]: f"{_l1} one-off task #2 failed and stopped",
        f"tqblocked:{_ki}": f"{PL.key_name(_ki)}: every executor it can go to was tried and could not run it (skipped), so it will not start on its own",
        f"disk:{PH}": f"{_l1} /mnt/disk1 has only 10 GB left; if the disk fills, jobs and archiving fail",
        f"boot:{PH}:" + ts(NOW - timedelta(hours=2)): f"{_l1} rebooted at 10-16 10:00; interrupted tasks were put back in the queue, check that they resumed",
        "notice:board": "board has new results, confirm before publishing (3 files).",
        f"site:{PH}:fail:{MEN0}": f"{_f} failed on {_l1} with progress; decide whether to resume or drop it",
        f"site:{PH}:fail:{_ki}": f"No training started for {PL.key_name(_ki)} on {_l1}",
        f"site:{PH}:orphan:zz": f"There is an unrecognized task in the {_l1} queue",
        f"site:{PH}:orphan:{MEN0}": f"{_l1} is running {_f}, but the hub queue has not assigned it there; it may be training twice"}
got_ = {k: PL.problem(k, P[k]) for k in want if k in P}
ok("plain: each problem kind is worded from data", got_ == want and all("data" in P[k] for k in want))
if got_ != want:
    print("\n".join(f"  {k}\n    want {want[k]}\n    got {got_.get(k)}" for k in want if got_.get(k) != want[k]))
ok("plain: without data it falls back to the message",
   PL.problem("unread:hub", dict(msg="original message")) == "original message" and PL.problem(f"site:{PH}:fail:{MEN0}", dict(msg="m")) == "m")
ok("a stuck report from an executor may be a message or dict(msg, data)", H.from_step("m") == dict(msg="m", level="normal", kind="state", after=0)
   and H.from_step(dict(msg="m", data=dict(n=1)))["data"] == dict(n=1))
ok("an executor report that is only waiting (level=info) is recorded, not pushed; writes outside the declared outputs are not pushed",
   H.from_step(dict(msg="m", data=dict(n=1), level="info"))["level"] == "info" and "undeclared" in H.QUIET)
ok("plain: every step in the experiment table has a display name (core + plugin labels.py STEPS)",
   all(PL.step_name(n) != n for n in PL.table_steps()) and PL.step_name("nope") == "nope")

# The project part of plain wording comes from plugin labels.py: step names, steps not listed, analysis titles; experiment names omit the most common model
ok("plain: step names come from core and plugin STEPS, else the step name itself",
   PL.step_name("lprob") == PL.STEPS["lprob"] != "lprob" and PL.step_name("relay_fetch") == "fetch results" and PL.step_name(H.proto.ENV_BUILD) == "build env")
ok("plain: plugin QUIET follows the core one; a step not listed -> None",
   PL.QUIET == ["train", "archive", "infer", "prune"] and PL.label(f"prune:{_e['id']}") is None)
ok("plain: a final experiment-table step (no second half) -> All experiments; other no-second-half labels -> Other",
   PL.label("compare_all") == ("All experiments", PL.STEPS["compare_all"], None) and PL.label("nope") == ("Other", "nope", None)
   and PL.task("compare_all") == f"{PL.STEPS['compare_all']} (all experiments)")
ok("plain: analysis titles come from plugin PLAN_TITLES: <id> or <id>_... only",
   PL.label("sens:X1") == ("Analysis", f"X1 {PL.plan_items()['X1']}", None) and PL.label("sens:X12_b") == ("Analysis", f"X12 {PL.plan_items()['X12']}", None)
   and PL.label("sens:X1b") == ("Other", "sens:X1b", None) and PL.label("sens:X3_a") == ("Other", "sens:X3_a", None)
   and PL.task("sens:X1_a") == f"Analysis X1 {PL.plan_items()['X1']}")
_t0, _as = EX.experiment("t0"), EX.experiment("arch_s")
ok("plain: experiment names omit the most common model, other models are kept", PL.usual_model() == "base"
   and PL.exp_name(_t0) == f"{_t0['exp']} {_t0['label']}" and PL.exp_name(_as) == f"{_as['exp']} {_as['label']} s")
_ke = "eval/" + _k
ok("plain: all possible executors skipped (tqblocked) is worded by key; without data the key comes from the id",
   PL.problem(f"tqblocked:{_ke}", dict(msg="m", data=dict(key=_ke))) == f"{PL.key_name(_ke)}: every executor it can go to was tried and could not run it (skipped), so it will not start on its own"
   and PL.problem(f"tqblocked:{_k}", dict(msg="m")).startswith(f"{PL.key_name(_k)}: every executor"))
_np = subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, sys.argv[1]); import plain as PL; "
                      "print(PL.step_name('lprob'), PL.plan_items(), PL.QUIET, PL.label('sens:X1'))", HERE],
                     capture_output=True, text=True, env=dict(os.environ, TASKQ_PLUGINS=""), timeout=60)
ok("plain: without plugins only core step names, no analysis titles", _np.returncode == 0
   and _np.stdout.split() == ["lprob", "{}", "['train',", "'archive']", "('Other',", "'sens:X1',", "None)"], )
if _np.returncode:
    print(_np.stderr[-2000:])

# Loading the plugins or experiment table failed: health stops at import, must push once (the same error is not pushed again, not at night),
# and the mark is cleared after the fix
import shutil  # noqa: E402
_lt = fixture.tmpdir()
_bad = os.path.join(_lt, "bad")
shutil.copytree(os.environ["TASKQ_PLUGINS"].split(os.pathsep)[0], _bad, ignore=shutil.ignore_patterns("__pycache__"))
open(os.path.join(_bad, "readers", "zz_broken.py"), "w").write("PROGRESS = {\n")
_wrap = os.path.join(_lt, "wrap.py")
open(_wrap, "w").write("""import datetime, runpy, sys, urllib.request
H, LOG, HOUR = sys.argv[1], sys.argv[2], int(sys.argv[3])
class _D(datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime.datetime(2026, 11, 3, HOUR, 0)
datetime.datetime = _D
class _R:
    def read(self):
        return b""
def _open(req, timeout=0):
    open(LOG, "a").write(req.full_url + " " + req.data.decode().replace(chr(10), " ") + chr(10))
    return _R()
urllib.request.urlopen = _open
sys.argv = [H] + sys.argv[4:]
sys.path.insert(0, H.rsplit("/", 1)[0])
runpy.run_path(H, run_name="__main__")
""")


def _health(plugins, hour, *args):
    home = os.path.join(_lt, "home")
    os.makedirs(os.path.join(home, ".config", "taskq"), exist_ok=True)
    open(os.path.join(home, ".config", "taskq", "ntfy_topic"), "w").write("t0\n")
    env = dict(os.environ, HOME=home, TASKQ_PLUGINS=plugins, TASKQ_STATE=os.path.join(_lt, "st"),
               TASKQ_DIR=os.path.join(_lt, "q"))
    log = os.path.join(_lt, "pushed.log")
    r = subprocess.run([sys.executable, _wrap, os.path.join(HERE, "health.py"), log, str(hour), *args],
                       env=env, capture_output=True, text=True, timeout=60)
    return r.returncode, (open(log).read().splitlines() if os.path.exists(log) else [])


rc1, p1 = _health(_bad, 12)
rc2, p2 = _health(_bad, 12)
ok("a broken plugin: health exits non-zero, pushes once (with the bad file), the same error is not pushed again",
   rc1 != 0 and len(p1) == 1 and "zz_broken" in p1[0] and rc2 != 0 and len(p2) == 1)
rc3, _ = _health(os.environ["TASKQ_PLUGINS"], 12, "--test")  # no real push (urlopen is replaced)
ok("plugins fixed: imports again, the load error mark is cleared",
   rc3 == 0 and NT.D.get_meta(NT.open_db(write=False), "load_error") is None)
shutil.rmtree(os.path.join(_lt, "home"))
if os.path.exists(os.path.join(_lt, "pushed.log")):
    os.remove(os.path.join(_lt, "pushed.log"))
rc4, p4 = _health(_bad, 23)
ok("a broken plugin at night: no push (it waits for the first morning check)", rc4 != 0 and p4 == [])

# ---------------- pueued gone: automatic recovery ----------------
LOC = dict(label="loc", hostname=r"\A\Z", ssh=None, scheduler="pueue", root="/tmp/loc_root", workers=[dict(cpus=2)])
H.M.MACHINES["loc"] = LOC  # a local pueue executor (ssh=None)
_seen_s = snap(pueue={"loc": {"groups": {"cpu": "Running", H.proto.IO_GROUP: "Paused", "wait": "Running"}, "tasks": []},
                      PH: {"error": "Error: connection refused"}})
_sp = os.path.join(H.HDIR, H.proto.PUEUE_SEEN)
if os.path.exists(_sp):
    os.remove(_sp)
H.C.write_json(_sp, {PH: dict(t=111, running=["gpu1"])})
_seen = H.pueue_seen(_seen_s, dry=True)
ok("pueue_seen: records time and running groups of machines where pueued answered; a silent machine keeps its last record; dry does not write",
   _seen["loc"] == dict(t=int(NOW.timestamp()), running=["cpu", "wait"]) and _seen[PH]["t"] == 111
   and json.load(open(_sp)) == {PH: dict(t=111, running=["gpu1"])})
H.pueue_seen(_seen_s)
ok("pueue_seen: writes when not dry", json.load(open(_sp))["loc"]["running"] == ["cpu", "wait"])
_ran, _qs = [], {"out": ""}
_ro, _sh = H.run_on, H.sh
H.run_on = lambda m, script, timeout=120: (_ran.append((m["ssh"], script)), (0, "2026-11-06 08:17:00 pueued restarted: put back 3 4\n"))[1]
H.sh = lambda cmd, inp=None, timeout=120: (0, _qs["out"])
_ev_added = []
import events as _EV  # noqa: E402
_EV_add = _EV.add
_EV.add = lambda source, msg, db=None, **kw: _ev_added.append((source, msg, kw))
BOOT_SH = "/" + cfg.bundle_rel("queue", "on_boot.sh")
try:
    _down = snap(pueue={"loc": {"error": "Error: no socket"}, PH: {"error": "Error: no socket"}}, ok={PH: True, "loc": True})
    H.policy.AUTO["revive"] = False
    ok("revive: switch off does nothing", H.revive(_down, {}) == ([], {}) and not _ran)
    H.policy.AUTO["revive"] = True
    _qs["out"] = json.dumps({"loc": {"groups": {}, "tasks": []}, PH: {"groups": {}, "tasks": []}})
    ev, still = H.revive(_down, {"loc": dict(t=1000, running=["cpu", "wait"]), PH: dict(t=2000, running=[])})
    loc_cmd = next(sc for sh_, sc in _ran if sh_ is None)
    ph_cmd = next(sc for sh_, sc in _ran if sh_ == "fake-pueue-host")
    ok("revive: a local machine (ssh=None) also runs on_boot.sh from its root's code/current with --since <last seen - 60> --resume <groups open then>",
       loc_cmd == f"bash /tmp/loc_root/code/current{BOOT_SH} --since 940 --resume cpu,wait" and H.M.code("loc") == "/tmp/loc_root/code/current")
    ok("revive: other machines run on_boot.sh from their bundle (no --resume without open groups; --gpu-log when gpu_log is set)",
       ph_cmd == f"bash {H.M.code(PH)}{BOOT_SH} --since 1940 --gpu-log")
    ok("revive: recovered -> log an event (health/revive), no reason left", len(ev) == 2 and all("recovered automatically" in e and "put back 3 4" in e for e in ev)
       and still == {} and {x[2].get("site") for x in _ev_added} == {"loc", PH} and _ev_added[0][2]["kind"] == "revive")
    _ran.clear()
    ev, still = H.revive(_down, {}, dry=True)
    ok("revive: dry only says what it would do, runs nothing", not _ran and len(ev) == 2 and all("will be recovered automatically" in e for e in ev))
    _one = snap(pueue={"loc": {"error": "x"}, PH: {"groups": {}, "tasks": []}}, ok={PH: True, "loc": True})
    ev, still = H.revive(_one, {})
    ok("revive: no previous record -> only start it (--since now, no --resume); machines where pueued answers are left alone",
       len(_ran) == 1 and _ran[0][0] is None and f"--since {int(NOW.timestamp())}" in _ran[0][1] and "--resume" not in _ran[0][1])
    _qs["out"] = json.dumps({"loc": {"error": "still down"}})
    _ran.clear()
    ev, still = H.revive(_one, {})
    ok("revive: still not responding after recovery -> return the reason (pueued reported as usual)", ev == [] and "automatic recovery failed" in still.get("loc", ""))
    _ran.clear()
    H.revive(dict(_down, ok={"loc": True, PH: False}), {})
    ok("revive: unreachable machines are left alone", [x[0] for x in _ran] == [None])
    _ran.clear()
    H.policy.PAUSE = {PH: (None, "maintenance")}
    H.revive(_down, {})
    ok("revive: paused machines are left alone", [x[0] for x in _ran] == [None])
finally:
    H.run_on, H.sh, _EV.add = _ro, _sh, _EV_add
    H.policy.AUTO.pop("revive", None)
    H.policy.PAUSE = {}
    del H.M.MACHINES["loc"]

print("All passed" if fails == 0 else f"{fails} failed")
sys.exit(1 if fails else 0)
