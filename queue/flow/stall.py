"""hub 上 taskq run 包着的步骤卡住了没有（2026-11-05，P3）：读 <状态文件夹>/runs/*.json 的心跳，
进程树的 CPU 时间和声明的产出都 STALL_IDLE 没长 → 卡住；包装进程没了、心跳也停了的心跳文件删掉（被 kill -9 时留下的）。"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import cfg  # noqa: E402
import proto as PR  # noqa: E402
from proto import alert_id  # noqa: E402


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def check(now=None):
    """→ {问题编号: 说明}。"""
    now = now or time.time()
    idle = float(os.environ.get("TASKQ_STALL_IDLE") or PR.STALL_IDLE)
    d = cfg.state(PR.RUNS)
    out = {}
    try:
        names = sorted(os.listdir(d))
    except FileNotFoundError:
        return out
    for n in names:
        if not n.endswith(".json"):
            continue
        p = os.path.join(d, n)
        try:
            b = json.load(open(p))
        except (OSError, ValueError):
            continue
        if not alive(b.get("wrapper") or b["pid"]) and now - b.get("beat", 0) > 3 * PR.RUN_BEAT:
            try:
                os.remove(p)
            except OSError:
                pass
            continue
        last = max(b.get("cpu_changed", 0), b.get("out_changed", 0))
        if now - last > idle:
            out[alert_id("stall", b["inst"])] = (f"{b['inst']}：{(now - last) / 60:.0f} 分钟没用 CPU、产出也没长"
                                                 f"（进程 {b['pid']}，{b.get('host', '')}；看 pueue log，确认卡死后 kill 它，下一次检查重排）")
    return out
