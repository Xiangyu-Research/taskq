"""时段（配置的 policy.SHIFT，queue/shift.py）：执行机按时段训练时，时段外不领；时段结束时停下手上的、交还、退出 0。
执行机的名字看 TASKQ_SITE，没设时按 WORKER_ID 在机器表里反查。"""
import datetime as dt
import os
import time


def _site(w):
    s = os.environ.get("TASKQ_SITE")
    if s:
        return s
    try:
        M = __import__("cfg").mtab()
        for name in M.SITES:
            try:
                x = M.worker_of(name, w.device)
            except (KeyError, TypeError):
                continue
            if isinstance(x, dict) and x.get("id") == w.W:
                return name
    except Exception:  # noqa: BLE001
        pass
    return None


def _now():
    f = os.environ.get("TASKQ_TEST_NOW")
    t = float(open(f).read().strip()) if f and os.path.isfile(f) else time.time()
    return dt.datetime.fromtimestamp(t, _shift().TZ)


def _shift():
    import shift
    return shift


def active(w):
    w.site = _site(w)
    if not w.site:
        return False
    try:
        return _shift().rule(w.site) is not None
    except Exception as e:  # noqa: BLE001
        w.say(f"!! 读不了时段（{type(e).__name__}: {e}），不按时段")
        return False


def inside(w):
    """→ (在不在时段里, "开始–结束")。"""
    sh, now = _shift(), _now()
    r = sh.rule(w.site, now)
    return bool(r and sh.dated(now, *r)[0]), f"{r[0]}–{r[1]}" if r else ""


def admit(w):
    ok, _ = inside(w)
    return None if ok else "时段外"


def tick(w):
    ok, span = inside(w)
    if ok:
        return
    if w.runs or w.zomb:
        w.shutdown(f"时段结束（{span}），停训练、交还任务", 0)
    else:
        w.shutdown(f"不在时段里（{span}），退出", 0)
