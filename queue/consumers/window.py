"""Hours window (worker hours in the machine table): do not claim outside it; when it ends, stop what is running, release it and exit 0 (another worker on the same machine resumes from the checkpoint).
The machine is TASKQ_MACHINE and this worker is WORKER_ID."""
import os
import time


def _me(w):
    """→ (machine name, this worker in the machine table) or (None, None)."""
    M = __import__("cfg").mtab()
    name = M.whoami()
    x = M.worker(name, w.W) if name else None
    return (name, x) if x else (None, None)


def _now():
    f = os.environ.get("TASKQ_TEST_NOW")
    return float(open(f).read().strip()) if f and os.path.isfile(f) else time.time()


def active(w):
    try:
        w.site, w.wconf = _me(w)
    except Exception as e:  # noqa: BLE001
        w.say(f"!! cannot read the machine table ({type(e).__name__}: {e}); not claiming until it can be read")
        w.site, w.wconf = None, None
        return True
    return bool(w.wconf and w.wconf.get("hours"))


def inside(w):
    """→ (inside the hours: True / False / None when unreadable, "start-end")."""
    if not w.site:
        return None, "cannot read the machine table"
    try:
        M = __import__("cfg").mtab()
        return M.in_hours(w.wconf["hours"], M.now_at(w.site, _now()))
    except Exception as e:  # noqa: BLE001
        return None, f"cannot read the hours: {type(e).__name__}: {e}"


def admit(w):
    ok, why = inside(w)
    return None if ok else ("outside the hours" if ok is False else why)


def tick(w):
    ok, span = inside(w)
    if ok is not False:  # when unreadable only stop claiming new tasks; keep running ones
        return
    if w.runs or w.zomb:
        w.shutdown(f"hours window ended ({span}); stopping training and releasing tasks", 0)
    else:
        w.shutdown(f"outside the hours ({span}); exiting", 0)
