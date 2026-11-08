"""A second training on one GPU: once ADD_AFTER seconds have passed since the last start, the last 10 records show average GPU utilization below UTIL_MAX and half the memory is free (the pad's share counts as free), claim another."""
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
        return "just started, waiting a bit"
    util, cnt, used, total = gpu_stats(w.gpucsv, w.nvsel)
    pad = w.pad_now()
    if not (cnt >= 5 and total > 0):
        return "not enough GPU records"
    if util >= w.UTIL_MAX:
        return f"GPU utilization {util}%"
    if total - used + pad < total // 2:
        return f"not enough GPU memory (training uses {used - pad}MiB / {total}MiB)"
    w.say(f"GPU utilization {util}%, training uses {used - pad}MiB / {total}MiB; claiming another")
    return None


def gpu_stats(csv, nvsel=()):
    """→ (average utilization of the last 10 records, count, memory used, total) in MiB."""
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
