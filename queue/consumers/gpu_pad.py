"""显存填充（queue/gpu_pad.py）：开训前让出显存，算还空多少显存时扣掉它占的。"""
import os
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
    """填充现在占的 MiB；<base>.mib 超过 60 秒没更新（填充不在了）→ 0。"""
    try:
        v = open(base + MIB).read().strip()
        fresh = time.time() - os.stat(base + MIB).st_mtime <= 60
    except OSError:
        return 0
    return int(v) if v.isdigit() and fresh else 0


def make_room(base, say):
    """让填充让出显存，最多等 30 秒。"""
    if not os.path.isfile(base + MIB):
        return
    try:
        os.unlink(base + READY)
    except OSError:
        pass
    open(base + START, "a").close()
    os.utime(base + START)
    for _ in range(30):
        if os.path.exists(base + READY) or _read(base + MIB) == "0":
            return
        time.sleep(1)
    say(f"显存填充 30 秒内没回应（{base}{MIB} = {_read(base + MIB)}），照样开始")


def _read(p):
    try:
        return open(p).read().strip()
    except OSError:
        return ""
