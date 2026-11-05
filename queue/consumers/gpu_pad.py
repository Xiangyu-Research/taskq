"""显存填充（queue/gpu_pad.py）：开训前让出显存，算还空多少显存时扣掉它占的。"""
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
    """填充现在占的 MiB；<base>.mib 超过 60 秒没更新（填充不在了）→ 0。"""
    try:
        v = open(base + MIB).read().strip()
        fresh = time.time() - os.stat(base + MIB).st_mtime <= 60
    except OSError:
        return 0
    return int(v) if v.isdigit() and fresh else 0


def make_room(base, say, wait=30):
    """让填充放到 floor，最多等 wait 秒 → 放好了（或没有填充）True，没等到 False。"""
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
    say(f"显存填充 {int(wait)} 秒内没回应（{base}{MIB} = {_read(base + MIB)}），照样开始")
    return False


def _read(p):
    try:
        return open(p).read().strip()
    except OSError:
        return ""


def env_base():
    """这个作业的握手文件前缀：TASKQ_GPU_PAD_BASE，没有时按 SLURM_JOB_ID；都没有 → None。"""
    b = os.environ.get("TASKQ_GPU_PAD_BASE")
    j = os.environ.get("SLURM_JOB_ID")
    return b or (proto.GPU_PAD_BASE + j if j else None)


def main(argv):
    """taskq pad room [--wait 秒]：作业里的命令自己要显存时，先让填充放到 floor、等它说好了。
    没有在跑的填充（不在 Slurm 作业里、没有 <base>.mib、或它 60 秒没更新）什么都不做，退出码 0；没等到退出码 1。"""
    import argparse
    ap = argparse.ArgumentParser(description="让这个作业的显存填充放到 floor，等它说好了")
    ap.add_argument("--wait", type=float, default=30, help="最多等几秒（默认 30）")
    a = ap.parse_args(argv)
    base = env_base()
    try:
        alive = base is not None and time.time() - os.stat(base + MIB).st_mtime <= 60
    except OSError:
        alive = False
    if not alive:
        return 0
    say = lambda m: print(f"taskq pad：{m}", file=sys.stderr)  # noqa: E731
    return 0 if make_room(base, say, a.wait) else 1
