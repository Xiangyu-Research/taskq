#!/usr/bin/env python3
"""测 taskq pad room（consumers/gpu_pad.py 的 make_room，2026-11-05）：假的握手文件，另一个线程扮演显存填充。

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
    print(("PASS:" if cond else "FAIL:") + what + ("" if cond or got is None else f"（得到 {got!r}）"))
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
ok("不在作业里：什么都不做，退出码 0", p.returncode == 0 and not os.path.exists(B + START), p.stderr)
p = room(TASKQ_GPU_PAD_BASE=B)
ok("没有 .mib（没有填充）：什么都不做，退出码 0", p.returncode == 0 and not os.path.exists(B + START), p.stderr)
put(MIB, "30000", age=300)
p = room(TASKQ_GPU_PAD_BASE=B)
ok(".mib 5 分钟没更新（填充不在了）：什么都不做", p.returncode == 0 and not os.path.exists(B + START), p.stderr)
clean()

# 填充在：先删旧的 .ready、碰 .start，等填充写 .ready
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
ok("填充在：删掉旧的 .ready、碰 .start，等到新的 .ready → 退出码 0", p.returncode == 0 and seen.get("start") and 1 <= time.time() - t0 < 10,
   (p.returncode, seen, p.stderr))
clean()

put(MIB, "0")
p = room(TASKQ_GPU_PAD_BASE=B)
ok("填充已经是 0：马上好", p.returncode == 0 and os.path.exists(B + START), p.stderr)
clean()

put(MIB, "30000")
t0 = time.time()
p = room("--wait", "2", SLURM_JOB_ID="x", TASKQ_GPU_PAD_BASE=B)
ok("填充不回应：等满 --wait 退出码 1，说清楚", p.returncode == 1 and "2 秒内没回应" in p.stderr and time.time() - t0 < 8, (p.returncode, p.stderr))
clean()

# 没有 TASKQ_GPU_PAD_BASE 时按 SLURM_JOB_ID 找 /tmp/gpu_pad_<作业号>
job = f"test{os.getpid()}"
jb = proto.GPU_PAD_BASE + job
try:
    with open(jb + MIB, "w") as f:
        f.write("0")
    p = room(SLURM_JOB_ID=job)
    ok("按 SLURM_JOB_ID 找握手文件", p.returncode == 0 and os.path.exists(jb + START), p.stderr)
finally:
    for x in proto.GPU_PAD_FILES:
        if os.path.exists(jb + x):
            os.unlink(jb + x)

p = subprocess.run([sys.executable, TQ, "pad", "room", "--help"], capture_output=True, text=True, cwd=T)
ok("帮助", p.returncode == 0 and "--wait" in p.stdout, p.stderr)

print(f"\n{'都通过' if not fails else f'{fails} 项没通过'}")
sys.exit(1 if fails else 0)
