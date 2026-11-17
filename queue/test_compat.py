#!/usr/bin/env python3
"""Frozen interfaces across versions (CLAUDE.md "Changes that go live without a cutover"), checked against literals written in this
file and against an older commit of the core, never against proto.py's own tables: a field deleted from proto.py and the code at
once still fails here.

  pinned    taskq status --json and taskq spec collect --json: version, the @@JSON mark and field names (add-only: more fields
            are fine, a missing one fails); the worker exit codes; every argument form of taskq worker next / beat / release /
            missing a worker sends, run for real through bin/taskq on a temp queue; the launcher path inside a bundle.
  old core  the core at OLD, unpacked with git archive into a temp dir and run with its own queue/testdata config (an old
            bundle carries the config of its commit):
            - its worker (consume.py's own claim / start / tq / fail code) against the current taskq.py: claim, beat, release,
              failed, missing inputs, lost claim, nothing to claim;
            - its worker with its own taskq.py on a queue the current taskq.py writes (add) and reads (status --json, next);
            - tasks added by the old taskq.py, claimed and released by the current worker;
            - bundle switch: its handoff is adopted by the current consume.py, which beats and releases the claim the old
              taskq.py made; and the reverse (a rollback to an older bundle);
            - old and current status --json / collect --json on the same input: every field the old one prints, at any
              depth, is still printed; old proto field tables and worker exit codes are kept by the current proto.py.
            Skipped with a SKIP line when OLD is not in the git history (a shallow clone; CI checks out with fetch-depth 0).
            When a cutover changes one of these formats on purpose, move OLD to the cutover commit.
Temp dirs only; no real task runs (run_task.sh is replaced by a script that exits 0); connects to no machine.
    python3 queue/test_compat.py
"""
import io
import json
import os
import subprocess
import sys
import tarfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
BIN = os.path.join(CORE, "bin", "taskq")
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, HERE)
import proto  # noqa: E402

OLD = "258547b"  # the core before the review series that added this test

# ---------------- pinned: written here on purpose, not read from proto.py ----------------
VERSION = 1
MARK = "@@JSON "
STATUS_TOP = ("version", "enabled", "tasks", "recent_failures", "paused", "recent_skips")
STATUS_TASK = ("order", "nice", "key", "exp", "recipe", "params", "spec", "added", "state", "note", "worker", "host", "since",
               "assigned", "input_since", "beat", "log_age", "task_log_age", "out_age", "failures", "skipped_by", "progress")
COLLECT_IN = ("key", "out", "done", "log", "progress", "info", "metrics", "resume")  # spec fields the hub sends to collect
COLLECT_TOP = ("version", "tasks", "seglogs")  # always there; "stuck" and "errors" only when there are some
COLLECT_TASK = ("runs", "final", "metrics", "debug", "resumable")
SEGLOG = ("name", "first", "trained")
WORKER_RC = dict(next_ok=0, nothing=10, wait=11, paused=12, lost=13)  # taskq worker next; beat --strict (lost)
MISSING_NOT_OURS = 1  # taskq worker missing on a claim that is not ours
LAUNCHER = "scripts/taskq/queue/worker_start.sh"  # queued Slurm jobs start <root>/code/current/<this>
HANDOFF_V = 1  # the handoff an old consumer gives the new one on a bundle switch

T = fixture.tmpdir(prefix="test_compat_")
PROJ = os.path.join(T, "proj")
os.makedirs(PROJ)
with open(os.path.join(PROJ, ".taskq.toml"), "w") as f:
    f.write(f'config = "{fixture.CONFIG}"\nplugins = ["{fixture.PLUGINS}"]\n')
MACHINE = "site-slurm"  # the queue is this executor's own queue (R/queue); the name exists in the old test config too
RUN_TASK = os.path.join(T, "run_task_stub.sh")  # stands in for run_task.sh: the worker starts it, it exits at once
with open(RUN_TASK, "w") as f:
    f.write("#!/bin/bash\nexit 0\n")
fails = 0

# A worker without its main loop: the given core's consume.Worker, its queue methods called step by step. argv: queue dir of that
# core, the taskq.py its tq / claim call, steps (JSON). Prints one line "@@RESULT <JSON>": one result per step.
DRIVER = r'''
import json, os, sys
qdir, taskq_py, steps = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
sys.path.insert(0, qdir)
import consume as C
C.TASKQ, C.RUN_TASK = taskq_py, os.environ["COMPAT_RUN_TASK"]
w = C.Worker.__new__(C.Worker)
w.t0 = w.changed = int(C.time.time())
w.W, w.device, w.envs = os.environ["COMPAT_WORKER"], "cpu", "base,alt"
w.task_cpus, w.mem_mb, w.slots, w.gpu_gb, w.tag_each, w.PATIENCE = 2, 4096, 2, 8.0, True, 0
w.tok, w.later, w.excl, w.fail_at, w.runs, w.zomb, w.plugins, w.bad = {}, {}, [], [], {}, {}, [], set()
w.idle_since, w.own, w.FAIL_RETRY, w.EARLY_FAIL, w.FAIL_WINDOW, w.MAX_OWN_FAIL = 0, 0, 1800, 600, 3600, 2
res = []
for st in steps:
    m, a = st[0], st[1:]
    if m == "claim":
        rc, out, err = w.claim(a[0])
        if rc == 0:
            w.start(out, a[0])
            r = list(w.runs.values())[-1]
            res.append([rc, r.key, w.tok.get(r.key)])
        else:
            res.append([rc, out, err[-300:]])
    elif m == "tq":
        res.append(w.tq(*a, quiet=True))
    elif m == "fail":
        res.append(w.fail(a[0], a[1], 99999))
    elif m == "token":
        w.tok[a[0]] = a[1]
        res.append(None)
    elif m == "exclude":
        w.exclude(a[0])
        res.append(None)
    elif m == "handoff":
        res.append(json.dumps(w.handoff()))
    elif m == "adopt":
        os.environ["TASKQ_HANDOFF"] = a[0]
        w.adopt()
        res.append([[r.key, w.tok.get(r.key)] for r in w.runs.values()])
for r in w.runs.values():
    if getattr(r, "proc", None):
        r.proc.wait()
print("@@RESULT " + json.dumps(res))
'''
DRIVER_PY = os.path.join(T, "driver.py")
with open(DRIVER_PY, "w") as f:
    f.write(DRIVER)


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def env_for(core, q, data, **kw):
    """Environment for running core's scripts on queue q with data dir data, core's own test config."""
    td = os.path.join(core, "queue", "testdata")
    e = dict(os.environ, TASKQ_CONFIG=os.path.join(td, "config"), TASKQ_PLUGINS=os.path.join(td, "plugins"), TASKQ_DIR=q,
             TASKQ_DATA=data, TASKQ_MACHINE=MACHINE, COMPAT_RUN_TASK=RUN_TASK, PYTHONDONTWRITEBYTECODE="1")
    e.update(kw)
    return e


def run(argv, env, stdin=None, cwd=None):
    return subprocess.run(argv, env=env, input=stdin, capture_output=True, text=True, timeout=300, cwd=cwd or T)


def marked(out):
    """The @@JSON lines of out -> [object]."""
    return [json.loads(ln[len(MARK):]) for ln in out.splitlines() if ln.startswith(MARK)]


def new_queue(name):
    q, data = os.path.join(T, name, "q"), os.path.join(T, name, "data")
    os.makedirs(q)
    os.makedirs(data)
    open(os.path.join(q, "tasks.jsonl"), "w").close()  # an executor's queue (setup creates it)
    return q, data


def add(core, q, data, *keys):
    """Add command tasks (device cpu, env base) with core's spec.py resolve and taskq.py add --spec-stdin, as the hub sends them."""
    env = env_for(core, q, data)
    for k in keys:
        r = run([sys.executable, os.path.join(core, "queue", "spec.py"), "resolve", "command", f"key={k}", "cmd=true", "env=base",
                 f"out=out/{k}", "done=ok.txt", "device=cpu"], env)
        assert r.returncode == 0, (core, r.stderr[-500:])
        a = run([sys.executable, os.path.join(core, "queue", "taskq.py"), "add", "--nice", "0", "--spec-stdin"], env, stdin=r.stdout)
        assert a.returncode == 0, (core, a.stdout[-300:], a.stderr[-500:])


def status(core, q, data):
    """core's taskq.py status --json -> (whole object, {key: task})."""
    r = run([sys.executable, os.path.join(core, "queue", "taskq.py"), "status", "--json"], env_for(core, q, data))
    js = marked(r.stdout)
    assert r.returncode == 0 and len(js) == 1, (core, r.returncode, r.stdout[-300:], r.stderr[-500:])
    return js[0], {t["key"]: t for t in js[0]["tasks"]}


def drive(core, taskq_core, q, data, worker, steps):
    """core's consume.Worker runs steps, its queue calls going to taskq_core's taskq.py -> one result per step."""
    r = run([sys.executable, DRIVER_PY, os.path.join(core, "queue"), os.path.join(taskq_core, "queue", "taskq.py"), json.dumps(steps)],
            env_for(core, q, data, COMPAT_WORKER=worker))
    res = [json.loads(ln[len("@@RESULT "):]) for ln in r.stdout.splitlines() if ln.startswith("@@RESULT ")]
    assert r.returncode == 0 and len(res) == 1, (core, r.returncode, r.stdout[-1500:], r.stderr[-1500:])
    return res[0]


def missing(old, new, path=""):
    """Dict keys in old that new lacks, at any depth (lists compared item by item) -> ["/tasks[0]/beat", ...]."""
    out = []
    if isinstance(old, dict) and isinstance(new, dict):
        for k, v in old.items():
            out += [f"{path}/{k}"] if k not in new else missing(v, new[k], f"{path}/{k}")
    elif isinstance(old, list) and isinstance(new, list):
        for i, (a, b) in enumerate(zip(old, new)):
            out += missing(a, b, f"{path}[{i}]")
    return out


# ================= pinned =================
ok("pinned exit codes and versions are proto's (status/collect version, @@JSON, worker next/beat, handoff)",
   (proto.VERSION, proto.JSON_MARK, proto.HANDOFF) == (VERSION, MARK, HANDOFF_V)
   and {k: proto.EXIT[k] for k in WORKER_RC} == WORKER_RC)
ok(f"the launcher path inside a bundle is {LAUNCHER}", f"{proto.BUNDLE_CORE}/queue/worker_start.sh" == LAUNCHER
   and os.path.isfile(os.path.join(CORE, "queue", "worker_start.sh")))

# taskq worker next / beat / release / missing: every argument form a worker sends, through bin/taskq, on the current taskq.py
QP, DP = new_queue("pinned")
KEYS = [f"pin/{c}" for c in "abcdefgh"]
add(CORE, QP, DP, *KEYS)
ENVP = env_for(CORE, QP, DP)


def worker(*args, **kw):
    return run([sys.executable, BIN, "worker", *args], dict(ENVP, **kw), cwd=PROJ)


def claimed(r):
    """taskq worker next output -> (key, run_task.sh arguments, token) when it is three lines, else None."""
    lines = r.stdout.split("\n")
    return tuple(lines[:3]) if r.returncode == 0 and len(lines) == 4 and lines[3] == "" else None


r = worker("next", "--worker", "pw", "--log-tag", "pw-1", "--envs", "base,alt", "--device", "cpu", "--cpus", "2", "--mem-mb", "2048",
           "--gpu-mem", "8.0", "--free-slots", "1", "--patience-until", "0", "--exclude", "pin/x", "pin/a")
c = claimed(r)
ok("worker next with every option a worker passes: exit 0, three lines (key, run_task.sh arguments, token); --exclude honoured",
   c is not None and c[0] == "pin/b" and c[1].startswith("--spec ") and c[2].strip() != "")
tb = c[2] if c else ""
r = worker("next", "--worker", "pw", "--envs", "base", "--device", "cpu", "--exclude")
c = claimed(r)
ok("worker next with an empty --exclude: claims the next task", c is not None and c[0] == "pin/a")
ta = c[2] if c else ""
r = worker("next", "--worker", "pw", "--envs", "nothing-here")
ok(f"worker next with no task for these envs: exit {WORKER_RC['nothing']}, prints nothing",
   r.returncode == WORKER_RC["nothing"] and r.stdout == "")
rcs = [worker("beat", "--worker", "pw", "--token", tb, "--strict", "--", "pin/b").returncode,
       worker("beat", "--worker", "pw", "--token", tb, "pin/b", "--strict").returncode,
       worker("beat", "--worker", "pw", "pin/b").returncode]
ok("worker beat: --strict before or after the key, with and without --token: exit 0", rcs == [0, 0, 0])
r = worker("beat", "--worker", "other", "--strict", "--", "pin/b")
ok(f"worker beat --strict on a claim that is not ours: exit {WORKER_RC['lost']}, prints the key",
   r.returncode == WORKER_RC["lost"] and r.stdout.split() == ["pin/b"])
r = worker("release", "--worker", "pw", "--token", ta, "--lock-wait", "5", "--", "pin/a")
_, st = status(CORE, QP, DP)
ok("worker release --lock-wait: exit 0, the claim is gone", r.returncode == 0 and st["pin/a"]["worker"] is None)


def claim_one():
    c = claimed(worker("next", "--worker", "pw", "--envs", "base", "--device", "cpu", "--exclude", "pin/a", "pin/b"))
    assert c, "worker next claimed nothing"
    return c[0], c[2]


k1, t1 = claim_one()
r = worker("release", "--worker", "pw", "--token", t1, "--failed", "exit 1 after 3 s", "--", k1)
_, st = status(CORE, QP, DP)
ok("worker release --failed: exit 0, one failure recorded, claim gone", r.returncode == 0 and st[k1]["failures"] == 1
   and st[k1]["worker"] is None)
k2, t2 = claim_one()
r = worker("release", "--worker", "pw", "--token", t2, "--skip", "needs more memory", k2)
_, st = status(CORE, QP, DP)
ok("worker release --skip (key before the option): exit 0, skipped by this worker", r.returncode == 0 and st[k2]["skipped_by"] == ["pw"])
k3, t3 = claim_one()
r0 = worker("missing", "--worker", "other", "--why", "x", "--", k3)
r = worker("missing", "--worker", "pw", "--token", t3, "--why", "job pw checked before start", "--", k3)
_, st = status(CORE, QP, DP)
ok(f"worker missing: not our claim -> exit {MISSING_NOT_OURS}; ours -> exit 0, inputs recorded missing, claim gone",
   r0.returncode == MISSING_NOT_OURS and r.returncode == 0 and st[k3]["input_since"] is not None and st[k3]["worker"] is None)
for _ in range(2):
    k, t = claim_one()
    worker("release", "--worker", "pw", "--token", t, "--failed", "boom", "--", k)
r = worker("next", "--worker", "pw", "--envs", "base", "--device", "cpu")
top, _ = status(CORE, QP, DP)
ok(f"worker next after three failures in the queue: exit {WORKER_RC['paused']} (paused)",
   r.returncode == WORKER_RC["paused"] and top["paused"] is True)

# taskq status --json / taskq spec collect --json: pinned fields present
r = run([sys.executable, BIN, "status", "--json"], ENVP, cwd=PROJ)
js = marked(r.stdout)
s = js[0] if len(js) == 1 else {}
ok("taskq status --json: exit 0, one @@JSON line, version 1, every pinned top-level field",
   r.returncode == 0 and len(js) == 1 and s.get("version") == VERSION and all(x in s for x in STATUS_TOP))
gone = sorted({x for t in s.get("tasks", []) for x in STATUS_TASK if x not in t})
ok(f"taskq status --json: every task has every pinned field, missing: {gone}", s.get("tasks") and not gone)

os.makedirs(os.path.join(DP, "out", "pin", "a"))
with open(os.path.join(DP, "out", "pin", "a", "train.log"), "w") as f:
    f.write("epoch 1\n")
open(os.path.join(DP, "out", "pin", "a", "ok.txt"), "w").close()
os.makedirs(os.path.join(DP, proto.TASK_LOGS))
with open(os.path.join(DP, proto.TASK_LOGS, "pin__a__pw_part1.log"), "w") as f:
    f.write("2026-01-01 00:00:00 start\n")
SPECS = [dict(key="pin/a", out="out/pin/a", done=["ok.txt"], log="*.log", progress=None, info=None, metrics={}, resume=None),
         dict(key="pin/bad", out=5, done=["ok.txt"], log="*.log", progress=None, info=None, metrics={}, resume=None)]
ok("the collect input holds only the pinned spec fields", all(tuple(x) == COLLECT_IN for x in SPECS))
CIN = json.dumps(dict(specs=SPECS, root=DP))
r = run([sys.executable, BIN, "spec", "collect", "--json"], ENVP, stdin=CIN, cwd=PROJ)
js = marked(r.stdout)
c = js[0] if len(js) == 1 else {}
ta_ = (c.get("tasks") or {}).get("pin/a") or {}
ok("taskq spec collect --json: exit 0, one @@JSON line, version 1, pinned top-level fields, a task with every pinned field",
   r.returncode == 0 and len(js) == 1 and c.get("version") == VERSION and all(x in c for x in COLLECT_TOP)
   and all(x in ta_ for x in COLLECT_TASK) and ta_.get("final") is True)
ok("taskq spec collect --json: each part log with the pinned fields; a spec that cannot be read is in errors",
   c.get("seglogs") and all(x in sl for sl in c["seglogs"] for x in SEGLOG) and "pin/bad" in (c.get("errors") or {}))


# ================= old core =================
def have(commit):
    return subprocess.run(["git", "-C", CORE, "cat-file", "-e", f"{commit}^{{commit}}"], capture_output=True).returncode == 0


def unpack(commit):
    d = os.path.join(T, "old")
    os.makedirs(d)
    tar = subprocess.run(["git", "-C", CORE, "archive", "--format=tar", commit, "queue", "sites", "sync", "monitor", "bin"],
                         capture_output=True, check=True).stdout
    with tarfile.open(fileobj=io.BytesIO(tar)) as tf:
        tf.extractall(d, **({"filter": "data"} if hasattr(tarfile, "data_filter") else {}))
    return d


def old_core_tests(OLDC):
    # the old proto's tables are kept (field names add-only, worker exit codes and handoff version unchanged)
    r = run([sys.executable, os.path.join(OLDC, "queue", "proto.py"), "json"], dict(os.environ))
    op = json.loads(r.stdout)
    lost = {t: sorted(set(op[t]) - set(getattr(proto, t))) for t in ("STATUS_TOP", "STATUS_FIELDS", "TASK_FIELDS", "COLLECT_FIELDS",
                                                                     "COLLECT_TOP", "COLLECT_TASK", "SEGLOG_FIELDS", "HANDOFF_TOP",
                                                                     "HANDOFF_TASK") if t in op}
    lost = {t: v for t, v in lost.items() if v}
    ok(f"old proto's status, collect and handoff field tables are all in the current proto.py, missing: {lost}", not lost)
    ok("old proto: same worker exit codes, versions, @@JSON mark and bundle core path",
       {k: op["EXIT"][k] for k in WORKER_RC} == WORKER_RC and (op["VERSION"], op["JSON_MARK"], op["HANDOFF"], op["BUNDLE_CORE"])
       == (VERSION, MARK, HANDOFF_V, proto.BUNDLE_CORE))

    # --- the old worker against the current taskq.py
    q, d = new_queue("old_worker")
    add(CORE, q, d, "ow/a", "ow/b", "ow/c", "ow/d")
    res = drive(OLDC, CORE, q, d, "ow", [["claim", "ow-1"], ["tq", "beat", "ow/a", "--strict"], ["tq", "release", "ow/a", "--lock-wait", "5"],
                                         ["claim", "ow-2"], ["fail", "ow/a", "exit 1"], ["claim", "ow-3"],
                                         ["tq", "missing", "ow/b", "--why", "job ow checked before start"], ["claim", "ow-4"]])
    ok("old worker -> current taskq.py: claims (its start() reads the three lines), beats (0), releases (0) in its own argument order",
       res[0][:2] == [0, "ow/a"] and res[0][2] and res[1] == 0 and res[2] == 0)
    _, st = status(CORE, q, d)
    ok("old worker -> current taskq.py: --failed recorded, missing inputs recorded, the last claim is the old worker's",
       res[3][:2] == [0, "ow/a"] and st["ow/a"]["failures"] == 1 and res[5][:2] == [0, "ow/b"] and res[6] == 0
       and st["ow/b"]["input_since"] is not None and res[7][:2] == [0, "ow/c"] and st["ow/c"]["worker"] == "ow"
       and st["ow/c"]["state"] == "claimed")
    tok_c = res[7][2]
    r = run([sys.executable, BIN, "worker", "next", "--worker", "nw", "--envs", "base", "--device", "cpu", "--exclude", "ow/a", "ow/b",
             "ow/d"], env_for(CORE, q, d, TASKQ_STALE_BEAT="0"), cwd=PROJ)
    res = drive(OLDC, CORE, q, d, "ow", [["token", "ow/c", tok_c], ["tq", "beat", "ow/c", "--strict"]] +
                [["exclude", k] for k in ("ow/a", "ow/b", "ow/c", "ow/d")] + [["claim", "ow-5"]])
    ok(f"old worker -> current taskq.py: its claim taken over after expiry -> beat --strict {WORKER_RC['lost']}; "
       f"nothing left to claim -> {WORKER_RC['nothing']}",
       r.stdout.startswith("ow/c\n") and res[1] == WORKER_RC["lost"] and res[-1][0] == WORKER_RC["nothing"])

    # --- the old worker with its own taskq.py on a queue the current taskq.py writes and reads
    q, d = new_queue("old_bundle")
    add(CORE, q, d, "ob/a", "ob/b", "ob/c", "ob/d")
    res = drive(OLDC, OLDC, q, d, "ob", [["claim", "ob-1"], ["tq", "beat", "ob/a", "--strict"]])
    _, st = status(CORE, q, d)
    r = run([sys.executable, BIN, "worker", "next", "--worker", "nw", "--envs", "base", "--device", "cpu", "--exclude", "ob/b", "ob/c",
             "ob/d"], env_for(CORE, q, d), cwd=PROJ)
    ok("old taskq.py claims a task the current taskq.py added; the current status --json shows its claim, the current next leaves it",
       res[0][:2] == [0, "ob/a"] and res[1] == 0 and st["ob/a"]["state"] == "claimed" and st["ob/a"]["worker"] == "ob"
       and st["ob/a"]["beat"] and r.returncode == WORKER_RC["nothing"])
    res = drive(OLDC, OLDC, q, d, "ob", [["token", "ob/a", res[0][2]], ["tq", "release", "ob/a"], ["claim", "ob-2"], ["fail", "ob/a", "exit 1"],
                                         ["claim", "ob-3"], ["tq", "missing", "ob/b", "--why", "inputs"]])
    top, st = status(CORE, q, d)
    ok("old taskq.py release / --failed / missing: the current status --json reads its records",
       res[1] == 0 and res[2][:2] == [0, "ob/a"] and res[4][:2] == [0, "ob/b"] and res[5] == 0 and st["ob/a"]["worker"] is None
       and st["ob/a"]["failures"] == 1 and top["recent_failures"] == 1 and st["ob/b"]["input_since"] is not None)
    res = drive(OLDC, OLDC, q, d, "ob", [["claim", "ob-4"]])
    old_top, _ = status(OLDC, q, d)
    top, _ = status(CORE, q, d)
    gone = missing(old_top, top)
    ok(f"status --json, old vs current on the same queue (claimed, released, failed, missing inputs): no field lost, missing: {gone}",
       res[0][0] == 0 and old_top["version"] == top["version"] == VERSION and not gone)

    # --- tasks the old taskq.py added, claimed and released by the current worker
    q, d = new_queue("old_tasks")
    add(OLDC, q, d, "ot/a", "ot/b")
    res = drive(CORE, CORE, q, d, "nw", [["claim", "nw-1"], ["tq", "beat", "ot/a", "--strict"], ["fail", "ot/a", "exit 1"],
                                         ["claim", "nw-2"], ["tq", "release", "ot/b"]])
    top, st = status(CORE, q, d)
    gone = sorted({x for t in top["tasks"] for x in STATUS_TASK if x not in t})
    ok(f"tasks added by the old taskq.py: the current worker claims, beats, fails and releases them; status --json complete, missing: {gone}",
       res[0][:2] == [0, "ot/a"] and res[1] == 0 and res[3][:2] == [0, "ot/b"] and res[4] == 0 and st["ot/a"]["failures"] == 1
       and st["ot/b"]["worker"] is None and not gone)

    # --- bundle switch: old consumer hands over to the current one, and back (rollback)
    q, d = new_queue("handoff")
    add(CORE, q, d, "ho/a", "ho/b")
    res = drive(OLDC, OLDC, q, d, "hw", [["claim", "hw-1"], ["handoff"]])
    h = res[1]
    res = drive(CORE, CORE, q, d, "hw", [["adopt", h], ["tq", "beat", "ho/a", "--strict"], ["tq", "release", "ho/a"]])
    _, st = status(CORE, q, d)
    ok("old consumer's handoff -> current consume.py adopts the task with its token, beats (0) and releases (0) the old taskq.py's claim",
       res[0] and [k for k, _ in res[0]] == ["ho/a"] and res[0][0][1] and res[1] == 0 and res[2] == 0 and st["ho/a"]["worker"] is None)
    res = drive(CORE, CORE, q, d, "hw", [["claim", "hw-2"], ["handoff"]])
    h = res[1]
    res = drive(OLDC, OLDC, q, d, "hw", [["adopt", h], ["tq", "beat", res[0][1], "--strict"], ["tq", "release", res[0][1]]])
    _, st = status(CORE, q, d)
    ok("rollback: the current consumer's handoff -> old consume.py adopts it, beats (0) and releases (0) the current taskq.py's claim",
       res[0] and len(res[0]) == 1 and res[0][0][1] and res[1] == 0 and res[2] == 0 and all(t["worker"] is None for t in st.values()))

    # --- collect --json, old vs current on the same input
    env_o, env_n = env_for(OLDC, QP, DP), env_for(CORE, QP, DP)
    ro = run([sys.executable, os.path.join(OLDC, "queue", "spec.py"), "collect", "--json"], env_o, stdin=CIN)
    rn = run([sys.executable, os.path.join(CORE, "queue", "spec.py"), "collect", "--json"], env_n, stdin=CIN)
    jo, jn = marked(ro.stdout), marked(rn.stdout)
    gone = missing(jo[0], jn[0]) if len(jo) == len(jn) == 1 else ["(no @@JSON line)"]
    ok(f"collect --json, old vs current on the same input (finished task, part log, unreadable spec): no field lost, missing: {gone}",
       ro.returncode == rn.returncode == 0 and not gone and jo[0]["version"] == jn[0]["version"] == VERSION
       and "errors" in jo[0] and jo[0]["seglogs"])


if have(OLD):
    t0 = time.time()
    old_core_tests(unpack(OLD))
    print(f"old core {OLD}: {int(time.time() - t0)} s")
else:
    print(f"SKIP:commit {OLD} is not in the git history (shallow clone): old worker / old core checks not run")

sys.exit(1 if fails else 0)
