#!/usr/bin/env python3
"""送任务的唯一入口（2026-10-31）：hub 的健康检查（monitor/health.py，每 30 分钟）调用 run_all，
转给 queue/relay.py 处理配置文件夹里 machines.py 的 RELAY 里的执行机（pueue 执行机、Slurm 执行机，细节记 ~/relay.log，
~/feed.log 里记一行指过去）。另外查总队列里分给了不是执行机的名字的任务（执行机换了名字、撤掉以后留下的），报 feed:nobody。

    python3 queue/feed.py [-n]       看现在会做什么（不改任何东西；-n / --dry-run / --dry 写不写都一样；taskq plan 包括它）
"""
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "monitor"))
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
from proto import alert, alert_id, alert_msg  # noqa: E402



def log(lines):
    import events
    events.lines("feed", lines)


def nobody(ctq):
    """总队列里分给了不是执行机的名字的 → {问题编号: 说明（带 data）}。"""
    own = set(M.RELAY)
    by = {}
    for t in (ctq or {}).get("tasks") or []:
        if t.get("assigned") and t["assigned"] not in own and t["state"] != "done":
            by.setdefault(t["assigned"], []).append(t["key"])
    return {alert_id("feed", "nobody", w): alert(f"总队列里 {len(ks)} 个任务分给了 {w}，但 {w} 不是执行机，没有谁会做（{ks[0]}…）；"
                                                 f"交还：taskq worker release --worker {w} <key>…",
                                                 worker=w, n=len(ks), keys=ks) for w, ks in by.items()}


def run_all(s, dry=False):
    """health.py 调用 → (说明, 卡住的 {问题编号: 说明})。"""
    try:
        import relay as RL  # queue/relay.py
        events, stuck = RL.run_all(s, dry=dry)
    except Exception as e:  # noqa: BLE001
        events, stuck = [], {alert_id("relay", "error"): f"queue/relay.py 出错：{type(e).__name__}: {e}"[:300]}
    if events and not dry:
        log([f"执行机 {'、'.join(sorted(M.RELAY))}（relay.py）：{len(events)} 条，见 ~/relay.log"])
    stuck = dict(stuck)
    stuck.update(nobody((s.get("taskq") or {}).get(M.hub())))
    return events, stuck


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="往执行机送任务（health.py 每次检查调用 run_all）；这里只看、不改")
    ap.add_argument("-n", "--dry-run", "--dry", action="store_true", help="只看会做什么（默认就是）")
    ap.parse_args(argv)
    import health as H  # noqa: E402
    ev, st = run_all(H.gather(), dry=True)
    print("\n".join(ev + [f"卡住：{alert_msg(v)}" for v in st.values()]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
