"""一张卡训第二个：最近一个开训 ADD_AFTER 秒以后，近 10 条记录的 GPU 平均使用率低于 UTIL_MAX、还空着一半显存（填充占的算空），再领一个。"""
import os
import re
import subprocess
import time
from collections import deque


def active(w):
    return w.device == "gpu"


def admit(w):
    n = w.busy()
    if not n:
        return None
    if time.time() - w.changed < w.ADD_AFTER:
        return "刚开始，先看看"
    util, cnt, used, total = gpu_stats(w.gpucsv, w.nvsel)
    pad = w.pad_now()
    if not (cnt >= 5 and total > 0):
        return "GPU 记录不够"
    if util >= w.UTIL_MAX:
        return f"GPU 使用率 {util}%"
    if total - used + pad < total // 2:
        return f"显存不够（训练占 {used - pad}MiB / {total}MiB）"
    w.say(f"GPU 使用率 {util}%、训练占 {used - pad}MiB / {total}MiB，再领一个")
    return None


def gpu_stats(csv, nvsel=()):
    """→ (近 10 条的平均使用率, 条数, 显存已用, 总量)（MiB）。"""
    t = os.environ.get("WORKER_TEST_GPU")
    if t:
        return tuple(int(x) for x in t.split()[:4])
    u = []
    try:
        with open(csv, errors="replace") as f:
            for ln in deque(f, 10):
                c = ln.rstrip("\n").split(",")
                d = re.sub(r"[^0-9]", "", c[4]) if len(c) > 4 else ""
                if d:
                    u.append(int(d))
    except OSError:
        pass
    used = total = 0
    try:
        out = subprocess.run(["nvidia-smi", *nvsel, "--query-gpu=memory.used,memory.total", "--format=csv,noheader,nounits"],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=60).stdout
        a = out.split("\n")[0].replace(" ", "").split(",")
        used, total = int(a[0]), int(a[1])
    except (OSError, subprocess.SubprocessError, ValueError, IndexError):
        pass
    return (sum(u) // len(u) if u else 0), len(u), used, total
