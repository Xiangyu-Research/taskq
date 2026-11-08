#!/usr/bin/env python3
"""Tests for inputs.py and run_task.sh input checks and the local private view: fake data dir, fake blobs and fake squeue in a
temp dir.
  python3 scripts/taskq/queue/test_inputs.py"""
import hashlib
import json
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
PY = [sys.executable, os.path.join(HERE, "inputs.py")]
T = fixture.tmpdir()
ok_all = True


def ok(name, cond):
    global ok_all
    print(("PASS:" if cond else "FAIL:") + name)
    ok_all &= bool(cond)


def run(*args, env=None):
    r = subprocess.run(PY + list(args), capture_output=True, text=True, env=dict(os.environ, **(env or {})))
    return r.returncode, r.stdout.strip(), r.stderr.strip()


def blob(store, content):
    h = hashlib.sha256(content).hexdigest()
    p = os.path.join(store, h[:2], h)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb") as f:
        f.write(content)
    return p


# data dir: one view (links to blobs) + one regular file
DATA, STORE = os.path.join(T, "data"), os.path.join(T, "blobs")
V = os.path.join(DATA, "in", "ds1")
os.makedirs(os.path.join(V, "sub"))
files = {f"sub/c{i}.b2nd": os.urandom(3000 + i) for i in range(6)}
for rel, c in files.items():
    os.symlink(blob(STORE, c), os.path.join(V, rel))
open(os.path.join(V, "dataset.json"), "w").write('{"a": 1}')
NOENV = dict(STAGE_KEEP_GB="0", PATH=os.environ["PATH"])

rc, out, err = run("prepare", DATA, "k1", "", V)
ok("input check: all present → exit 0, prints the data dir", rc == 0 and out == DATA)
rc, out, err = run("prepare", DATA, "k1", "", V, os.path.join(DATA, "in", "nothere"))
ok("input check: missing input → exit 4 and says what is missing", rc == 4 and "nothere" in err and "missing inputs" in err)
rc, out, err = run("prepare", DATA, "k1", "", os.path.join(DATA, "in", "*", "sub", "c*.b2nd"))
ok("input check: a glob matches files → exit 0", rc == 0 and out == DATA)
rc, out, err = run("prepare", DATA, "k1", "", os.path.join(DATA, "in", "*", "nope*"))
ok("input check: a glob matches nothing → exit 4", rc == 4 and "no matches" in err)
open(os.path.join(V, ".incomplete"), "w").close()
rc, out, err = run("prepare", DATA, "k1", "", V)
ok("input check: half-built view (.incomplete) → exit 4", rc == 4 and "half-built" in err)
os.remove(os.path.join(V, ".incomplete"))
# a half-built level or an unreadable level below also counts as missing
open(os.path.join(V, "sub", ".incomplete"), "w").close()
rc, out, err = run("prepare", DATA, "k1", "", os.path.join(DATA, "in"))
ok("input check: .incomplete in a subdir → exit 4", rc == 4 and "half-built" in err and "ds1/sub" in err)
os.remove(os.path.join(V, "sub", ".incomplete"))
rc, out, err = run("prepare", DATA, "k1", "", os.path.join(DATA, "in"))
ok("input check: the parent is fine without a marker → exit 0", rc == 0 and out == DATA)
if os.getuid() != 0:
    os.chmod(os.path.join(V, "sub"), 0)
    rc, out, err = run("prepare", DATA, "k1", "", os.path.join(DATA, "in"))
    os.chmod(os.path.join(V, "sub"), 0o755)
    ok("input check: unreadable subdir → exit 4, not treated as empty", rc == 4 and "unreadable" in err)
gone =os.path.realpath(os.path.join(V, "sub/c0.b2nd"))
os.rename(gone, gone + ".x")
rc, out, err = run("prepare", DATA, "k1", "", V)
ok("input check: a link target blob was removed → exit 4 and names it", rc == 4 and "c0.b2nd" in err)
os.rename(gone + ".x", gone)

# local private view (fake squeue: only job 7 is alive)
BIN = os.path.join(T, "bin")
os.makedirs(BIN)
with open(os.path.join(BIN, "squeue"), "w") as f:
    f.write("#!/bin/sh\necho 7\n")
os.chmod(os.path.join(BIN, "squeue"), 0o755)
TMP = os.path.join(T, "tmp")
L7 = os.path.join(TMP, "taskq_stage_7")
old = os.path.join(TMP, "taskq_stage_5")  # left by a job that has ended
os.makedirs(os.path.join(old, "blobs"))
env = dict(NOENV, PATH=BIN + ":" + os.environ["PATH"])
rc, out, err = run("prepare", DATA, "k1", L7, V, env=env)
view = os.path.join(L7, "views", "k1")
lv = os.path.join(view, "in", "ds1")
ok("local view: prints the view root, same relative paths as the data dir, all pointing at the original at first", rc == 0 and out == view
   and all(os.path.realpath(os.path.join(lv, r)) == os.path.realpath(os.path.join(V, r)) for r in files)
   and os.path.realpath(os.path.join(lv, "dataset.json")) == os.path.join(V, "dataset.json"))
ok("local view: removes ended jobs' dirs on this node, keeps running ones", not os.path.exists(old) and os.path.isdir(L7))
todo = json.load(open(view + ".todo.json"))
ok("local view: the copy list has all 6 blobs (non-blob files are read in place)", sorted(x[0] for x in todo) == sorted("in/ds1/" + r for r in files))

# copy interrupted: a temp name left by a dead process, and one blob whose content does not match its name
os.makedirs(os.path.join(L7, "blobs", "ab"), exist_ok=True)
stale = os.path.join(L7, "blobs", "ab", f"abc.{fixture.dead_pid()}.tmp")
open(stale, "w").write("half")
bad_rel = "sub/c1.b2nd"
bad_real = os.path.realpath(os.path.join(V, bad_rel))
good = open(bad_real, "rb").read()
open(bad_real, "wb").write(b"x" * len(good))  # same size, different content
rc, out, err = run("copy", L7, "k1", env=env)
ok("copy: removes temp names left by a stopped copy", not os.path.exists(stale))
ok("copy: a content mismatch is not switched, still points at the original, and is reported", rc == 0 and os.path.realpath(os.path.join(lv, bad_rel)) == bad_real
   and "were not copied" in out and not any(f.endswith(".tmp") for _, _, fs in os.walk(os.path.join(L7, "blobs")) for f in fs))
ok("copy: the rest switch to local copies with the same content", all(os.path.realpath(os.path.join(lv, r)).startswith(os.path.join(L7, "blobs") + "/")
                                          and open(os.path.join(lv, r), "rb").read() == c for r, c in files.items() if r != bad_rel)
   and all(os.path.relpath(os.path.realpath(os.path.join(lv, r)), L7).split("/")[:2] == ["blobs", os.path.basename(os.path.realpath(os.path.join(lv, r)))[:2]]
           for r in files if r != bad_rel))
ok("copy: no temp links left in the view", not any(f.endswith(".tmp") for d, _, fs in os.walk(view) for f in fs))
open(bad_real, "wb").write(good)

# second task of the same job: already-copied files point local, copy only the rest; hard link what other jobs on this node have
rc, out, err = run("prepare", DATA, "k2", L7, V, env=env)
todo2 = json.load(open(os.path.join(L7, "views", "k2.todo.json")))
ok("second task: already-local files point local, the list has only the one that failed", rc == 0 and [x[0] for x in todo2] == ["in/ds1/" + bad_rel])
L8 = os.path.join(TMP, "taskq_stage_8")  # another job on this node
with open(os.path.join(BIN, "squeue"), "w") as f:
    f.write("#!/bin/sh\necho 7\necho 8\n")
rc, out, err = run("prepare", DATA, "k3", L8, V, env=env)
rc, out, err = run("copy", L8, "k3", env=env)
ino = lambda p: os.stat(p).st_ino  # noqa: E731
ok("other job: hard links what this node already has instead of copying again", rc == 0 and "hard-linked 5" in out and "new 1" in out
   and ino(os.path.join(L8, "views", "k3", "in", "ds1", "sub/c2.b2nd")) == ino(os.path.join(L7, "views", "k1", "in", "ds1", "sub/c2.b2nd")))

# free space: what other jobs on this node have (hard-linkable) does not count as bytes to copy; copies on this node are serialized (lock), so two concurrent checks do not each count alone
sys.path.insert(0, HERE)
import inputs as I  # noqa: E402
import fcntl  # noqa: E402


def _kill_raises(exc):
    def k(pid, sig):
        raise exc
    return k


_kill = I.os.kill
try:
    I.os.kill = _kill_raises(PermissionError())
    perm = I.alive(12345)
    I.os.kill = _kill_raises(ProcessLookupError())
    gone = I.alive(12345)
finally:
    I.os.kill = _kill
ok("process alive: PermissionError means alive (another user's process), ProcessLookupError means gone", perm is True and gone is False
   and I.alive(os.getpid()) and not I.alive(fixture.dead_pid()))
L10 = os.path.join(TMP, "taskq_stage_10")
run("prepare", DATA, "k10", L10, V, env=env)
_du, _keep = I.shutil.disk_usage, I.KEEP_GB
I.shutil.disk_usage = lambda p: shutil._ntuple_diskusage(0, 0, 1024 ** 3 + 10)
I.KEEP_GB = 1
try:
    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        I.copy(L10, "k10")
finally:
    I.shutil.disk_usage, I.KEEP_GB = _du, _keep
ok("free space: what other jobs on this node already have (hard links) does not count as bytes to copy", "hard-linked 6" in buf.getvalue() and "not copying" not in buf.getvalue())
L11 = os.path.join(TMP, "taskq_stage_11")
run("prepare", DATA, "k11", L11, V, env=env)
lk = open(os.path.join(TMP, f".taskq_stage_{os.getuid()}.lock"), "a")
fcntl.flock(lk, fcntl.LOCK_EX)
p11 = subprocess.Popen(PY + ["copy", L11, "k11"], stdout=subprocess.PIPE, text=True, env=dict(os.environ, **env))
try:
    p11.wait(timeout=2)
except subprocess.TimeoutExpired:
    pass
waited = p11.poll() is None
fcntl.flock(lk, fcntl.LOCK_UN)
lk.close()
out11 = p11.communicate(timeout=60)[0]
ok("copies on this node queue: wait while another copy holds the lock, then copy as usual", waited and p11.returncode == 0 and "hard-linked 6" in out11)

# job arrays: squeue %i is <array id>_<index>, while the local dir uses each subjob's own id (%A); another job must not remove it
with open(os.path.join(BIN, "squeue"), "w") as f:
    f.write('#!/bin/sh\ncase "$*" in *%A*) echo "7 7"; echo "8 3_0";; *) echo 7; echo 3_0;; esac\n')
rc, out, err = run("prepare", DATA, "k1", L7, V, env=env)
ok("an array subjob (job 8, shown as 3_0) is alive: keep its local dir", rc == 0 and os.path.isdir(L8))
with open(os.path.join(BIN, "squeue"), "w") as f:
    f.write("#!/bin/sh\necho 7\necho 8\n")

# non-blob files (produced by the executor itself): read in place, see changed content
dj = os.path.join(V, "dataset.json")
open(dj, "w").write('{"a": 2}')
rc, out, err = run("prepare", DATA, "k6", L7, V, env=env)
todo6 = json.load(open(os.path.join(L7, "views", "k6.todo.json")))
ok("non-blob files: not in the list, read fresh content in place", rc == 0 and "in/ds1/dataset.json" not in [x[0] for x in todo6]
   and open(os.path.join(L7, "views", "k6", "in", "ds1", "dataset.json")).read() == '{"a": 2}')
open(dj, "w").write('{"a": 1}')

# not enough disk: copy nothing, keep reading in place
rc, out, err = run("prepare", DATA, "k4", os.path.join(TMP, "taskq_stage_9"), V, env=dict(env, STAGE_KEEP_GB="1e9"))
rc, out, err = run("copy", os.path.join(TMP, "taskq_stage_9"), "k4", env=dict(env, STAGE_KEEP_GB="1e9"))
lv9 = os.path.join(TMP, "taskq_stage_9", "views", "k4", "in", "ds1")
ok("not enough disk: copy nothing, the view still points at the original", rc == 0 and "not copying" in out and all(os.path.realpath(os.path.join(lv9, r)).startswith(STORE) for r in files))

# when squeue fails, do not remove other jobs' dirs
with open(os.path.join(BIN, "squeue"), "w") as f:
    f.write("#!/bin/sh\nexit 1\n")
rc, out, err = run("prepare", DATA, "k5", L7, V, env=env)
ok("squeue fails: keep other jobs' dirs", rc == 0 and os.path.isdir(L8) and os.path.isdir(os.path.join(TMP, "taskq_stage_9")))

# run_task.sh: missing inputs → exit 4 without running the command; all present → the command reads from TASK_INPUTS_ROOT; with a staging dir (TASKQ_STAGE_ROOT) and local_inputs → read the local view
RT = os.path.join(HERE, "run_task.sh")
OUT = os.path.join(DATA, "jobs", "t")


def rtask(inputs, local, **e):
    args = ["command", "key=t/one", "env=stats", "out=jobs/t", "done=DONE", f"inputs={json.dumps(inputs)}",
            f"res={json.dumps(dict(local_inputs=local))}",
            'cmd=cat "$TASK_INPUTS_ROOT/in/ds1/dataset.json" > "$TASKQ_DATA/jobs/t/seen"; echo "$TASK_INPUTS_ROOT" > "$TASKQ_DATA/jobs/t/root"; touch "$TASKQ_DATA/jobs/t/DONE"']
    envx = dict(os.environ, TASKQ_DATA=DATA, RUN_TASK_TRIES="1", TMPDIR=TMP, PATH=BIN + ":" + os.environ["PATH"], STAGE_KEEP_GB="0", **e)
    envx.pop("SLURM_JOB_ID", None)
    envx.update(e)
    shutil.rmtree(OUT, ignore_errors=True)
    return subprocess.run(["bash", RT] + args, capture_output=True, text=True, env=envx)


envs_ok = subprocess.run([sys.executable, os.path.join(os.path.dirname(HERE), "sites", "mtab.py"), "activate", "stats"], capture_output=True).returncode == 0
if envs_ok:
    with open(os.path.join(BIN, "squeue"), "w") as f:
        f.write("#!/bin/sh\necho 7\n")
    r = rtask(["in/ds1", "in/nothere"], 0)
    ok("run_task.sh: missing inputs → exit 4, command did not run", r.returncode == 4 and not os.path.exists(os.path.join(OUT, "DONE")))
    r = rtask(["in/ds1"], 0)
    ok("run_task.sh: inputs present, no local copy → TASK_INPUTS_ROOT is the data dir", r.returncode == 0
       and open(os.path.join(OUT, "root")).read().strip() == DATA and open(os.path.join(OUT, "seen")).read() == '{"a": 1}')
    r = rtask(["in/ds1"], 1, SLURM_JOB_ID="7", TASKQ_STAGE_ROOT=L7)
    ok("run_task.sh: staging dir and local_inputs → read the private view on the node's local disk", r.returncode == 0
       and open(os.path.join(OUT, "root")).read().strip() == os.path.join(L7, "views", "t__one")
       and open(os.path.join(OUT, "seen")).read() == '{"a": 1}')
    r = rtask(["in/ds1"], 0, SLURM_JOB_ID="7", TASKQ_STAGE_ROOT=L7)
    ok("run_task.sh: task did not ask for local_inputs → read the data dir", r.returncode == 0 and open(os.path.join(OUT, "root")).read().strip() == DATA)
else:
    print("SKIP: run_task.sh checks: the test machine table does not recognize this machine (no stats env)")

shutil.rmtree(T, ignore_errors=True)
print("\nall passed" if ok_all else "\nfailed")
sys.exit(0 if ok_all else 1)
