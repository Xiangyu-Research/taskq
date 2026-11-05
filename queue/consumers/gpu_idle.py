"""等卡空（常驻的 worker，CUDA_VISIBLE_DEVICES 一张卡）：卡上没有别人的进程、别人占的显存 ≤ 500 MiB，并且 GPU 使用记录里
最近 TASK_GPU_IDLE 分钟这张卡上没有别人，才去领任务；不放行就不领，空等时记一句（状态变了或每 30 分钟）。"""
import os
import subprocess
import time
from collections import deque

import proto


def active(w):
    return w.device == "gpu" and not os.environ.get("SLURM_JOB_ID") and bool(os.environ.get("CUDA_VISIBLE_DEVICES"))


def setup(w):
    w.gpu_wait = None  # (状态, 开始等的时刻, 上次记的时刻)


def admit(w):
    cvd = os.environ["CUDA_VISIBLE_DEVICES"]
    idle_min = os.environ.get("TASK_GPU_IDLE", "")
    seen = None
    if not gpu_free(cvd):
        state = "busy"
    else:
        seen = gpu_seen(cvd, idle_min)
        state = "seen" if seen else None
    if state is None:
        w.gpu_wait = None
        return None
    now = time.time()
    st, since, said = w.gpu_wait or (None, now, 0)
    if st != state or now - said >= 1800:
        waited = int((now - since) // 60)
        if state == "seen":
            w.say(f"卡 {cvd} 空着，别人 {seen} 还在用，空满 {idle_min} 分钟再跑（已等 {waited} 分钟）")
        else:
            w.say(f"卡 {cvd} 被别人占着，等它空出来（已等 {waited} 分钟）")
        said = now
    w.gpu_wait = (state, since, said)
    return "卡被别人占着" if state == "busy" else f"别人 {seen} 用过卡"


def _smi(cvd, *q):
    try:
        return subprocess.run(["nvidia-smi", "-i", cvd, *q], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                              timeout=60).stdout
    except (OSError, subprocess.SubprocessError):
        return ""


def gpu_free(cvd):
    """卡上没有别人的进程、别人占的显存 ≤ 500 MiB（自己账号的进程不算）。"""
    used = _smi(cvd, "--query-gpu=memory.used", "--format=csv,noheader,nounits").split("\n")[0].strip()
    mine = 0
    for ln in _smi(cvd, "--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits").split("\n"):
        a = [x.strip() for x in ln.split(",")]
        if not a[0]:
            continue
        try:
            if os.stat(f"/proc/{int(a[0])}").st_uid != os.getuid():
                return False
        except (OSError, ValueError):
            return False
        mine += int(a[1]) if len(a) > 1 and a[1].isdigit() else 0
    return (int(used) if used.isdigit() else 0) - mine <= 500


def gpu_seen(cvd, idle_min):
    """GPU 使用记录里最近 idle_min 分钟这张卡上别人最后一次出现的时刻（"%F %T"）；没有 → None。没有记录文件 → 只看现在。"""
    if not idle_min or idle_min == "0":
        return None
    d = os.environ.get("GPU_LOG_DIR") or os.path.expanduser(proto.GPU_LOG_DIR)
    t0 = time.time() - int(idle_min) * 60
    since = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t0))
    files = []
    for t in (t0, time.time()):
        f = os.path.join(d, time.strftime(proto.GPU_LOG_FILE, time.localtime(t)))
        if f not in files:
            files.append(f)
    import pwd
    me = pwd.getpwuid(os.getuid()).pw_name
    last = None
    for f in files:
        try:
            with open(f, errors="replace") as fh:
                for ln in deque(fh, 20000):
                    c = ln.rstrip("\n").split(",")
                    if len(c) > 5 and c[0] >= since and c[1] == cvd and c[5] and c[5] != me:
                        last = c[0]
        except OSError:
            pass
    return last
