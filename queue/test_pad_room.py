#!/usr/bin/env python3
"""Tests taskq pad room (consumers/gpu_pad.py's make_room) with fake handshake files and another thread playing the GPU pad.

    python3 queue/test_pad_room.py
"""
import os
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
sys.path.insert(0, HERE)
import proto  # noqa: E402

MIB, READY, START = proto.GPU_PAD_FILES
TQ = os.path.join(os.path.dirname(HERE), "bin", "taskq")
T = fixture.tmpdir()
B = os.path.join(T, "pad")
fails = 0


def ok(what, cond, got=None):
    global fails
    print(("PASS:" if cond else "FAIL:") + what + ("" if cond or got is None else f" (got {got!r})"))
    fails += 0 if cond else 1


def room(*args, **env):
    e = {k: v for k, v in os.environ.items() if k not in ("SLURM_JOB_ID", "TASKQ_GPU_PAD_BASE")}
    e.update(env)
    return subprocess.run([sys.executable, TQ, "pad", "room", *args], capture_output=True, text=True, env=e, cwd=T, timeout=60)


def put(name, text, age=0):
    with open(B + name, "w") as f:
        f.write(text)
    t = time.time() - age
    os.utime(B + name, (t, t))


def clean():
    for x in proto.GPU_PAD_FILES:
        if os.path.exists(B + x):
            os.unlink(B + x)


p = room()
ok("not in a job: do nothing, exit 0", p.returncode == 0 and not os.path.exists(B + START), p.stderr)
p = room(TASKQ_GPU_PAD_BASE=B)
ok("no .mib (no pad): do nothing, exit 0", p.returncode == 0 and not os.path.exists(B + START), p.stderr)
put(MIB, "30000", age=300)
p = room(TASKQ_GPU_PAD_BASE=B)
ok(".mib not updated for 5 minutes (pad gone): do nothing", p.returncode == 0 and not os.path.exists(B + START), p.stderr)
clean()

# pad running: remove the old .ready, touch .start, wait for the pad to write .ready
put(MIB, "30000")
put(READY, "old")
seen = {}


def pad():
    t0 = time.time()
    while time.time() - t0 < 20:
        if os.path.exists(B + START) and not os.path.exists(B + READY):
            seen["start"] = True
            time.sleep(1.2)
            put(READY, "1")
            return
        time.sleep(0.05)


th = threading.Thread(target=pad)
th.start()
t0 = time.time()
p = room("--wait", "10", TASKQ_GPU_PAD_BASE=B)
th.join()
ok("pad running: removes the old .ready, touches .start, waits for a new .ready → exit 0", p.returncode == 0 and seen.get("start") and 1 <= time.time() - t0 < 10,
   (p.returncode, seen, p.stderr))
clean()

put(MIB, "0")
p = room(TASKQ_GPU_PAD_BASE=B)
ok("pad already at 0: ready at once", p.returncode == 0 and os.path.exists(B + START), p.stderr)
clean()

put(MIB, "30000")
t0 = time.time()
p = room("--wait", "2", SLURM_JOB_ID="x", TASKQ_GPU_PAD_BASE=B)
ok("pad does not respond: waits out --wait, exit 1, says why", p.returncode == 1 and "did not respond within 2 seconds" in p.stderr and time.time() - t0 < 8, (p.returncode, p.stderr))
clean()

# without TASKQ_GPU_PAD_BASE, find the handshake by SLURM_JOB_ID: in this account's private dir, else where a pad started by an
# older bundle keeps it (/tmp/gpu_pad_<job id>); files there are used only as regular files of this account, never through a link
import common as C  # noqa: E402
job = f"test{os.getpid()}"
pb, jb = proto.gpu_pad_base(job, os.getuid()), proto.GPU_PAD_BASE + job
made = not os.path.exists(os.path.dirname(pb))
VICTIM = os.path.join(T, "victim")


def clean_job():
    for b in (pb, jb):
        for x in proto.GPU_PAD_FILES:
            if os.path.lexists(b + x):
                os.unlink(b + x)


try:
    ok("private dir for the handshake is this account's (0700)", C.private_dir(os.path.dirname(pb)))
    with open(pb + MIB, "w") as f:
        f.write("0")
    p = room(SLURM_JOB_ID=job)
    ok("finds the handshake files in this account's private dir by SLURM_JOB_ID", p.returncode == 0 and os.path.exists(pb + START), p.stderr)
    clean_job()
    with open(jb + MIB, "w") as f:
        f.write("0")
    p = room(SLURM_JOB_ID=job)
    ok("a pad started by an older bundle (handshake at /tmp/gpu_pad_<job id>): still found, .start touched there",
       p.returncode == 0 and os.path.exists(jb + START) and not os.path.exists(pb + START), p.stderr)
    clean_job()
    with open(VICTIM, "w") as f:
        f.write("30000")
    os.symlink(VICTIM, jb + MIB)
    p = room(SLURM_JOB_ID=job)
    ok("a link planted at the older place's .mib: no pad (exit 0 at once), nothing touched", p.returncode == 0
       and not os.path.lexists(jb + START) and not os.path.lexists(pb + START), p.stderr)
    clean_job()
    with open(jb + MIB, "w") as f:
        f.write("0")
    os.utime(VICTIM, (1_000_000_000, 1_000_000_000))
    os.symlink(VICTIM, jb + START)
    p = room(SLURM_JOB_ID=job)
    ok("a link planted at .start: not followed (the file it points at keeps its time)", p.returncode == 0
       and os.stat(VICTIM).st_mtime == 1_000_000_000 and os.path.islink(jb + START), p.stderr)
    clean_job()
    if os.getuid() == 0:
        with open(jb + MIB, "w") as f:
            f.write("30000")
        os.chown(jb + MIB, 4242, 4242)
        p = room(SLURM_JOB_ID=job)
        ok("another account's .mib at the older place: no pad, nothing touched", p.returncode == 0 and not os.path.lexists(jb + START), p.stderr)
    else:
        print("SKIP: another account's .mib (needs root to make a file of another account)")
finally:
    clean_job()
    if made:
        os.rmdir(os.path.dirname(pb))

p = subprocess.run([sys.executable, TQ, "pad", "room", "--help"], capture_output=True, text=True, cwd=T)
ok("help", p.returncode == 0 and "--wait" in p.stdout, p.stderr)

print(f"\n{'all passed' if not fails else f'{fails} checks failed'}")
sys.exit(1 if fails else 0)
