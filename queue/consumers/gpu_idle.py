"""Wait for free GPUs (resident worker, CUDA_VISIBLE_DEVICES: one or more GPUs, by index or UUID): claim only when every one of them
has no one else's processes, others' memory on it is <= 500 MiB, and the GPU usage records show no one else on it for the last
TASK_GPU_IDLE minutes; otherwise do not claim and log a line while waiting (when the state changes, or every 30 minutes). When
nvidia-smi cannot tell (missing, failing, timing out, unreadable output) the GPU does not count as free either."""
import os
import subprocess
import time
from collections import deque

import cfg
import proto


def active(w):
    return w.device == "gpu" and not os.environ.get("SLURM_JOB_ID") and bool(os.environ.get("CUDA_VISIBLE_DEVICES"))


def setup(w):
    w.gpu_wait = None  # (state, waiting since, last logged)


def admit(w):
    cvd = os.environ["CUDA_VISIBLE_DEVICES"]
    idle_min = os.environ.get(proto.RUN_VARS["gpu_idle"], "")
    seen = None
    state, idx, why = probe(cvd)
    if state == "free":
        seen = gpu_seen(idx, idle_min)
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
        elif state == "unknown":
            w.say(f"GPU {cvd} cannot be checked ({why}); not claiming until it can (waited {waited} minutes)")
        else:
            w.say(f"GPU {cvd} busy, waiting for it (waited {waited} minutes)")
        said = now
    w.gpu_wait = (state, since, said)
    if state == "seen":
        return f"someone {seen} used the GPU"
    return "GPU busy" if state == "busy" else f"GPU cannot be checked ({why})"


def _smi(gpu, *q):
    """nvidia-smi -i gpu's output; None when it cannot tell (missing, timed out, failed)."""
    try:
        r = subprocess.run(["nvidia-smi", "-i", gpu, *q], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout if r.returncode == 0 else None


def probe(cvd):
    """Every GPU in cvd (CUDA_VISIBLE_DEVICES: indices or UUIDs, comma separated) → (state, indices, why). state: "free" when on each
    GPU there are no one else's processes and others' memory is <= 500 MiB (our own account's processes do not count); "busy";
    "unknown" when nvidia-smi cannot tell (missing, failed, timed out, unreadable output), why says which. indices: the GPUs'
    nvidia-smi indices (the usage records name GPUs by index)."""
    idx = []
    for g in [x.strip() for x in cvd.split(",") if x.strip()]:
        out = _smi(g, "--query-gpu=index,memory.used", "--format=csv,noheader,nounits")
        if out is None:
            return "unknown", idx, f"nvidia-smi failed for GPU {g}"
        a = [x.strip() for x in (out.split("\n") + [""])[0].split(",")]
        if len(a) != 2 or not (a[0].isdigit() and a[1].isdigit()):
            return "unknown", idx, f"nvidia-smi gave {out.strip()[:60]!r} for GPU {g}"
        used = int(a[1])
        apps = _smi(g, "--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits")
        if apps is None:
            return "unknown", idx, f"nvidia-smi failed listing the processes on GPU {g}"
        mine = 0
        for ln in apps.split("\n"):
            b = [x.strip() for x in ln.split(",")]
            if not b[0]:
                continue
            if not b[0].isdigit():
                return "unknown", idx, f"nvidia-smi listed {ln.strip()[:60]!r} on GPU {g}"
            try:
                if os.stat(f"/proc/{b[0]}").st_uid != os.getuid():
                    return "busy", idx, ""
            except OSError:
                return "busy", idx, ""
            mine += int(b[1]) if len(b) > 1 and b[1].isdigit() else 0
        if used - mine > 500:
            return "busy", idx, ""
        idx.append(a[0])
    return ("free", idx, "") if idx else ("unknown", idx, "no GPU in CUDA_VISIBLE_DEVICES")


def gpu_seen(gpus, idle_min):
    """Last time someone else appeared on any of these GPUs (nvidia-smi indices) in the last idle_min minutes of GPU usage records
    ("%F %T"); None when never. No records -> only look now."""
    if not idle_min or idle_min == "0":
        return None
    d = cfg.gpu_log_dir()
    t0 = time.time() - int(idle_min) * 60
    since = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t0))
    files = []
    for t in (t0, time.time()):
        f = os.path.join(d, time.strftime(proto.GPU_LOG_FILE, time.localtime(t)))
        if f not in files:
            files.append(f)
    import pwd
    me = pwd.getpwuid(os.getuid()).pw_name
    gpus = set(gpus)
    last = None
    for f in files:
        try:
            with open(f, errors="replace") as fh:
                for ln in deque(fh, 20000):
                    c = ln.rstrip("\n").split(",")
                    if len(c) > 5 and c[0] >= since and c[1] in gpus and c[5] and c[5] != me:
                        last = c[0]
        except OSError:
            pass
    return last
