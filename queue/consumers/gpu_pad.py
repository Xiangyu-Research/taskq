"""GPU pad (queue/gpu_pad.py): make room before starting; its share is subtracted when computing free GPU memory.
The handshake files sit in this account's private dir (proto.gpu_pad_base); they are read and touched only as regular files of this
account, never through a link."""
import os
import stat
import sys
import time

import proto

MIB, READY, START = proto.GPU_PAD_FILES


def active(w):
    return bool(os.environ.get("TASKQ_GPU_PAD_BASE") or os.environ.get("SLURM_JOB_ID"))


def setup(w):
    w.pad_bases = bases(w.W)
    w.pad_now = lambda: pad_now(find(w.pad_bases))


def before_start(w):
    make_room(find(w.pad_bases), w.say)


def bases(job=None):
    """Where this job's pad handshake can be: TASKQ_GPU_PAD_BASE; else this account's private dir (proto.gpu_pad_base), then where
    a pad started by an older bundle keeps it (proto.GPU_PAD_BASE<job>). No job → []."""
    b = os.environ.get("TASKQ_GPU_PAD_BASE")
    if b:
        return [b]
    job = job or os.environ.get("SLURM_JOB_ID")
    return [proto.gpu_pad_base(job, os.getuid()), proto.GPU_PAD_BASE + job] if job else []


def find(bs):
    """The base whose pad publishes its share (<base>.mib, a regular file of this account) → that base; none → the first (None when no bases)."""
    return next((b for b in bs if _own(b + MIB)), bs[0] if bs else None)


def _own(p):
    """os.lstat(p) when p is a regular file of this account (not a link), else None."""
    try:
        st = os.lstat(p)
    except OSError:
        return None
    return st if stat.S_ISREG(st.st_mode) and st.st_uid == os.getuid() else None


def _read(p):
    """Contents of p when it is a regular file of this account (opened without following a link), else ""."""
    try:
        fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        return ""
    try:
        st = os.fstat(fd)
        return os.read(fd, 100).decode(errors="replace").strip() if stat.S_ISREG(st.st_mode) and st.st_uid == os.getuid() else ""
    finally:
        os.close(fd)


def _touch(p):
    """Create p or set its mtime to now, without following a link; another account's file is left alone."""
    try:
        fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError:
        return
    try:
        if os.fstat(fd).st_uid == os.getuid():
            os.utime(fd)
    finally:
        os.close(fd)


def pad_now(base):
    """MiB the pad currently holds; 0 when <base>.mib has not been updated for 60 seconds (the pad is gone) or is not this account's file."""
    st = _own(base + MIB) if base else None
    if st is None:
        return 0
    v = _read(base + MIB)
    return int(v) if v.isdigit() and time.time() - st.st_mtime <= 60 else 0


def make_room(base, say, wait=30):
    """Let the pad drop to floor, waiting at most wait seconds → True when room is made (or there is no pad), False on timeout."""
    if not base or not _own(base + MIB):
        return True
    try:
        os.unlink(base + READY)  # removes a link itself, never its target
    except OSError:
        pass
    _touch(base + START)
    for _ in range(max(int(wait), 1)):
        if _own(base + READY) or _read(base + MIB) == "0":
            return True
        time.sleep(1)
    say(f"GPU pad did not respond within {int(wait)} seconds ({base}{MIB} = {_read(base + MIB)}); starting anyway")
    return False


def main(argv):
    """taskq pad room [--wait sec]: when a job's command needs GPU memory itself, first let the pad drop to floor and wait until it is ready.
    With no running pad (not in a Slurm job, no <base>.mib, or not updated for 60 seconds) do nothing and exit 0; exit 1 when it did not respond in time."""
    import argparse
    ap = argparse.ArgumentParser(description="let this job's GPU pad drop to floor and wait until it is ready")
    ap.add_argument("--wait", type=float, default=30, help="seconds to wait at most (default 30)")
    a = ap.parse_args(argv)
    base = find(bases())
    st = _own(base + MIB) if base else None
    if st is None or time.time() - st.st_mtime > 60:
        return 0
    say = lambda m: print(f"taskq pad: {m}", file=sys.stderr)  # noqa: E731
    return 0 if make_room(base, say, a.wait) else 1
