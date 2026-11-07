"""时段（机器表里 worker 的 hours）：时段外不领；时段结束时停下手上的、交还、退出 0（同一台的别的 worker 从存档接着训）。
本机看 TASKQ_MACHINE，自己是哪个 worker 看 WORKER_ID。"""
import os
import time


def _me(w):
    """→ (机器名, 机器表里这个 worker) 或 (None, None)。"""
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
        w.say(f"!! 读不了机器表（{type(e).__name__}: {e}），读到之前不领")
        w.site, w.wconf = None, None
        return True
    return bool(w.wconf and w.wconf.get("hours"))


def inside(w):
    """→ (在不在时段里：True / False / 读不了是 None, "开始–结束")。"""
    if not w.site:
        return None, "读不了机器表"
    try:
        M = __import__("cfg").mtab()
        return M.in_hours(w.wconf["hours"], M.now_at(w.site, _now()))
    except Exception as e:  # noqa: BLE001
        return None, f"读不了时段：{type(e).__name__}: {e}"


def admit(w):
    ok, why = inside(w)
    return None if ok else ("时段外" if ok is False else why)


def tick(w):
    ok, span = inside(w)
    if ok is not False:  # 读不了时只是不领新的，手上的不停
        return
    if w.runs or w.zomb:
        w.shutdown(f"时段结束（{span}），停训练、交还任务", 0)
    else:
        w.shutdown(f"不在时段里（{span}），退出", 0)
