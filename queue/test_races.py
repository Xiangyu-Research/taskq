#!/usr/bin/env python3
"""Concurrency and half-failed cases: lock takeover, relay only renewing when the hub's pueue is unreadable, cleanup racing
stage for pins, and lock wait timeouts.
Run: python3 queue/test_races.py [lock|lock_noowner|relay_err_release|relay_err_fetch|gc_pin|central_timeout] (all without arguments,
each reported separately).
Everything is in temp dirs: HOME, TASKQ_STATE and TASKQ_DIR point there, with fixture config and test_relay's fake ssh; no network."""
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata"))
import fixture  # noqa: E402
TMP = fixture.tmpdir(prefix="test_review_conc_")
os.environ["HOME"] = os.path.join(TMP, "home")
os.makedirs(os.environ["HOME"])
os.environ["TASKQ_DIR"] = os.path.join(TMP, "tq")
os.environ["TASKQ_LOCK_WAIT"] = "0.05"  # lock retry interval
sys.path.insert(0, HERE)
import test_relay as TR  # noqa: E402  (fixture.use(), temp TASKQ_STATE, fake ssh; runs module setup only, not the tests)
import common as C  # noqa: E402
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
import mtab as M  # noqa: E402
import relay  # noqa: E402
import relay_transfer as RT  # noqa: E402
import taskq  # noqa: E402

assert taskq.TQ == os.environ["TASKQ_DIR"], taskq.TQ


def test_lock():
    """Two lock waiters both see the same dead lock as expired: A takes it and gets a new lock, then B takes A's new lock as old
    and takes it again -> both hold the lock. tmpfs (/tmp) does not reuse inode numbers, ext4 hands a just-deleted one to a new
    dir right away: both are exercised."""
    _lock_race(taskq.TQ)
    if os.path.isdir("/var/tmp"):
        disk = fixture.tmpdir(prefix="test_races_", dir="/var/tmp")
        try:
            _lock_race(disk)
        finally:
            shutil.rmtree(disk, ignore_errors=True)


def _lock_race(base):
    os.makedirs(base, exist_ok=True)
    d = os.path.join(base, "lock")
    p = subprocess.Popen(["true"])
    p.wait()
    os.mkdir(d)
    with open(os.path.join(d, "owner"), "w") as f:  # the lock holder was SIGKILLed: local pid already gone
        f.write(f"{socket.gethostname()}\t{p.pid}\tdeadbeef0000\n")
    old = time.time() - 200
    os.utime(d, (old, old))
    A, B = taskq.Lock(tries=4), taskq.Lock(tries=4)
    A.d = B.d = d
    orig = taskq.Lock._holder
    state = dict(done=False)

    def hook(self):
        r = orig(self)
        if self is B and not state["done"]:  # after B decides the owner is dead but before its rename, A takes the old lock and gets a new one
            state["done"] = True
            A.__enter__()
        return r

    taskq.Lock._holder = hook
    try:
        both = False
        try:
            B.__enter__()
            both = True
        except SystemExit:
            pass
        a_owner_now = open(os.path.join(d, "owner")).read() if os.path.exists(os.path.join(d, "owner")) else None
        assert not both, (f"{base}: B got the lock while A held it (A={A.me!r}, B={B.me!r}, the lock now records {a_owner_now!r}): "
                          "B renamed A's freshly created lock as if it were an expired old one")
    finally:
        taskq.Lock._holder = orig
        shutil.rmtree(d, ignore_errors=True)


def test_lock_noowner():
    """Lock taken but owner not written (disk full etc.): on exit delete only the dir we created; do not delete one that was
    taken over and recreated by someone else."""
    _lock_noowner(taskq.TQ)
    if os.path.isdir("/var/tmp"):
        disk = fixture.tmpdir(prefix="test_races_", dir="/var/tmp")
        try:
            _lock_noowner(disk)
        finally:
            shutil.rmtree(disk, ignore_errors=True)


def _lock_noowner(base):
    os.makedirs(base, exist_ok=True)
    d = os.path.join(base, "lock")
    real_open = open

    def bad(p, *a, **k):
        if str(p).endswith(os.sep + "owner") and "w" in (a[0] if a else k.get("mode", "r")):
            raise OSError("cannot write")
        return real_open(p, *a, **k)
    try:
        for taken in (False, True):
            A = taskq.Lock(tries=4)
            A.d = d
            taskq.open = bad
            try:
                A.__enter__()
            finally:
                del taskq.open
            assert A.me is None and os.path.isdir(d), A.me
            if taken:  # A hung, lock expired: B took it over (renamed and removed), just mkdir'd a new lock, owner not written yet
                os.rmdir(d)
                os.mkdir(d)
            A.__exit__(None, None, None)
            if not taken:
                assert not os.path.exists(d), f"{base}: a lock without owner was not removed on exit"
            else:
                assert os.path.isdir(d), f"{base}: A's owner was unwritten and exit removed B's fresh lock"
                os.rmdir(d)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def _plan_with_hub_error(mt, rt):
    K = "D1_x/tr/0"
    S = TR.S0([TR.mtask(K, mt)])
    S["pueue"] = {M.hub(): {"error": "pueue does not respond"}}  # health.gather: this is what a failed qs --json or unreadable hub pueue looks like
    RS = dict(taskq=dict(tasks=[TR.rtask(K, rt)], paused=False), tasks={}, free=500e9)
    old_lookup, old_down = RT.lookup, C.backup_down
    RT.lookup = lambda k: dict(archive=True)
    C.backup_down = lambda ok=None: None
    try:
        return K, relay.plan(S, TR.NAME, RS, TR.keymap(K))
    finally:
        RT.lookup, C.backup_down = old_lookup, old_down


def test_relay_err_release():
    """The hub's pueue is unreadable (s["pueue"]["hub"] = {"error": ...}, s["ok"]["hub"] still True): a task that needs archiving
    shows done in the hub queue -> renew only and wait for next time; treating it as "never fetched, no archive needed" would
    delete the results there, release, and never queue the archive."""
    K, (acts, stuck) = _plan_with_hub_error("done", "done")
    assert "release" not in [a["op"] for a in acts], f"released a task needing archive while the hub pueue was unreadable: {TR.ops(acts)}, {acts}"


def test_relay_err_fetch():
    """The hub's pueue is unreadable: the other side finished and a fetch is already running on the hub (just unreadable) -> do
    not queue another fetch; this case would."""
    K, (acts, stuck) = _plan_with_hub_error("claimed", "done")
    assert "fetch" not in [a["op"] for a in acts], f"queued another fetch while the hub pueue was unreadable: {TR.ops(acts)}"


def test_gc_pin():
    """Cleanup reads the pins, then stage writes one (stage runs in the hub core group, parallel to the health check's cleanup):
    stage's missing already saw this blob as present and will not send it, cleanup deletes it anyway -> the view lacks a blob.
    Cleanup must check pins once more before deleting each blob."""
    R = os.path.join(TMP, "gcroot")
    h = "ab" + "0" * 62
    os.makedirs(os.path.join(R, "blobs", h[:2]))
    os.makedirs(os.path.join(R, "data", "views", "v"))
    os.makedirs(os.path.join(R, "state", "pins"))
    blob = os.path.join(R, "blobs", h[:2], h)
    with open(blob, "wb") as f:
        f.write(b"x" * 1000)
    old = time.time() - 3 * 86400
    os.utime(blob, (old, old))
    pin = os.path.join(R, "state", "pins", "stage_k.json")
    B = os.path.join(R, "blobs")
    code = (f"import os,json,sys\nsys.path.insert(0,{HERE!r})\nimport site_ops\n_w=os.walk\n"
            f"def _walk(top,*a,**k):\n"
            f"    if os.path.realpath(top)==os.path.realpath({B!r}) and not os.path.exists({pin!r}):\n"
            f"        json.dump([{h!r}],open({pin!r},'w'))\n"  # stage writes the pin after cleanup read pins and starts choosing blobs
            f"    return _w(top,*a,**k)\n"
            f"os.walk=_walk\nsite_ops.main(['gc'])\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=dict(os.environ, TASKQ_ROOT=R),
                       input=json.dumps(dict(soft_gb=1e6, keep=[], free=0, views=[], scan=["views/v"])))
    assert r.returncode == 0, r.stderr
    assert os.path.exists(pin), "the hook did not fire"
    assert os.path.exists(blob), (f"stage pinned while cleanup was running and cleanup still deleted the blob: {r.stdout.strip()}")


def test_central_timeout():
    import inspect
    wait = inspect.signature(taskq.Lock).parameters["tries"].default * 0.5
    t = inspect.signature(taskq.central).parameters["timeout"].default
    assert t > wait + 30, f"central() timeout {t}s is not longer than the lock's max wait {wait}s: a held lock gives the caller TimeoutExpired instead of an exit code"
    import advance
    assert "timeout=TQ_TIMEOUT" in inspect.getsource(advance.taskq_add)
    assert advance.TQ_TIMEOUT > wait + 30, f"advance.TQ_TIMEOUT {advance.TQ_TIMEOUT}s is not longer than the lock's max wait {wait}s"


TESTS = dict(lock=test_lock, lock_noowner=test_lock_noowner, relay_err_release=test_relay_err_release, relay_err_fetch=test_relay_err_fetch, gc_pin=test_gc_pin,
             central_timeout=test_central_timeout)

if __name__ == "__main__":
    names = sys.argv[1:] or list(TESTS)
    bad = 0
    for n in names:
        try:
            TESTS[n]()
            print(f"PASS:{n}")
        except AssertionError as e:
            bad += 1
            print(f"FAIL:{n}：{e}")
        except Exception:  # noqa: BLE001
            bad += 1
            print(f"failed: {n}")
            traceback.print_exc()
    shutil.rmtree(TMP, ignore_errors=True)
    shutil.rmtree(TR.T, ignore_errors=True)
    shutil.rmtree(TR.STATE_DIR, ignore_errors=True)
    sys.exit(1 if bad else 0)
