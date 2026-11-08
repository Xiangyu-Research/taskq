"""Wait for a free GPU (resident worker, CUDA_VISIBLE_DEVICES is one GPU): claim only when no one else's processes are on it, others'
memory is <= 500 MiB, and the GPU usage records show no one else on it for the last TASK_GPU_IDLE minutes; otherwise do not claim and
log a line while waiting (when the state changes, or every 30 minutes)."""
import os
import subprocess
import time
from collections import deque

import proto


def active(w):
    return w.device == "gpu" and not os.environ.get("SLURM_JOB_ID") and bool(os.environ.get("CUDA_VISIBLE_DEVICES"))


def setup(w):
    w.gpu_wait = None  # (state, waiting since, last logged)


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
            w.say(f"GPU {cvd} free, but someone {seen} still used it; waiting {idle_min} minutes of quiet (waited {waited} minutes)")
        else:
            w.say(f"GPU {cvd} busy, waiting for it (waited {waited} minutes)")
        said = now
    w.gpu_wait = (state, since, said)
    return "GPU busy" if state == "busy" else f"someone {seen} used the GPU"


def _smi(cvd, *q):
    try:
        return subprocess.run(["nvidia-smi", "-i", cvd, *q], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                              timeout=60).stdout
    except (OSError, subprocess.SubprocessError):
        return ""


def gpu_free(cvd):
    """No one else's processes on the GPU, others' memory <= 500 MiB (our own account's processes do not count)."""
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
    """Last time someone else appeared on this GPU in the last idle_min minutes of GPU usage records ("%F %T"); None when never. No
    records -> only look now."""
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
