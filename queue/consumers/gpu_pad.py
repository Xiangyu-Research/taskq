"""GPU pad (queue/gpu_pad.py): make room before starting; its share is subtracted when computing free GPU memory."""
import os
import sys
import time

import proto

MIB, READY, START = proto.GPU_PAD_FILES


def active(w):
    return bool(os.environ.get("TASKQ_GPU_PAD_BASE") or os.environ.get("SLURM_JOB_ID"))


def setup(w):
    w.pad_base = os.environ.get("TASKQ_GPU_PAD_BASE") or proto.GPU_PAD_BASE + w.W
    w.pad_now = lambda: pad_now(w.pad_base)


def before_start(w):
    make_room(w.pad_base, w.say)


def pad_now(base):
    """MiB the pad currently holds; 0 when <base>.mib has not been updated for 60 seconds (the pad is gone)."""
    try:
        v = open(base + MIB).read().strip()
        fresh = time.time() - os.stat(base + MIB).st_mtime <= 60
    except OSError:
        return 0
    return int(v) if v.isdigit() and fresh else 0


def make_room(base, say, wait=30):
    """Let the pad drop to floor, waiting at most wait seconds → True when room is made (or there is no pad), False on timeout."""
    if not os.path.isfile(base + MIB):
        return True
    try:
        os.unlink(base + READY)
    except OSError:
        pass
    open(base + START, "a").close()
    os.utime(base + START)
    for _ in range(max(int(wait), 1)):
        if os.path.exists(base + READY) or _read(base + MIB) == "0":
            return True
        time.sleep(1)
    say(f"GPU pad did not respond within {int(wait)} seconds ({base}{MIB} = {_read(base + MIB)}); starting anyway")
    return False


def _read(p):
    try:
        return open(p).read().strip()
    except OSError:
        return ""


def env_base():
    """Handshake file prefix for this job: TASKQ_GPU_PAD_BASE, else SLURM_JOB_ID; neither → None."""
    b = os.environ.get("TASKQ_GPU_PAD_BASE")
    j = os.environ.get("SLURM_JOB_ID")
    return b or (proto.GPU_PAD_BASE + j if j else None)


def main(argv):
    """taskq pad room [--wait sec]: when a job's command needs GPU memory itself, first let the pad drop to floor and wait until it is ready.
    With no running pad (not in a Slurm job, no <base>.mib, or not updated for 60 seconds) do nothing and exit 0; exit 1 when it did not respond in time."""
    import argparse
    ap = argparse.ArgumentParser(description="let this job's GPU pad drop to floor and wait until it is ready")
    ap.add_argument("--wait", type=float, default=30, help="seconds to wait at most (default 30)")
    a = ap.parse_args(argv)
    base = env_base()
    try:
        alive = base is not None and time.time() - os.stat(base + MIB).st_mtime <= 60
    except OSError:
        alive = False
    if not alive:
        return 0
    say = lambda m: print(f"taskq pad: {m}", file=sys.stderr)  # noqa: E731
    return 0 if make_room(base, say, a.wait) else 1
