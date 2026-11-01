#!/usr/bin/env python3
"""进度汇报（2026-10-16；10/21 起每 2 小时、改成白话）：box 上 cron 每 2 小时（08:30–20:30）在更新登记表和看板之后跑一次
（scripts/monitor/auto_refresh.sh registry），推送到手机（和健康检查同一个 ntfy 频道）：训完的任务数和上次以来多了几个任务、剩余天数、在训的任务和预计训完时间、
上次以来训完的任务和出了结果的实验、要处理的问题（说法见 scripts/monitor/plain.py）。

    python3 scripts/monitor/report.py            推送
    python3 scripts/monitor/report.py --dry-run  只打印
读：research/RQ1/实验登记_状态.json、实验登记表.csv（make_registry.py 刚生成的）、scripts/queue/qs --json、
  ~/.cache/phd-health/state.json（健康检查的当前问题）。上一次的训完任务数和时间记在 ~/.cache/phd-health/report.json。
"""
import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
import health as H  # noqa: E402  push、机器表
import common as C  # noqa: E402  scripts/queue/common.py（health 已经把路径加上）
import plain as PL  # noqa: E402  白话说法

RQ1 = os.path.join(REPO, "research", "RQ1")
LAST = os.path.expanduser("~/.cache/phd-health/report.json")
HOURS = 2  # 没有上一次的记录时，"上次以来"按这么多小时算


def short(r):
    return f"{r['实验']} {r['说明']} {r['模型'] if r['模型'] != 'nnU-Net' else ''} f{r['fold']}".replace("  ", " ")


def hm(ts):
    """'2026-10-16 18:30' → 今天的写 18:30，别的天写 10/17 18:30。"""
    t = datetime.strptime(ts[:16], "%Y-%m-%d %H:%M")
    return f"{t:%H:%M}" if t.date() == datetime.now().date() else f"{t.month}/{t.day} {t:%H:%M}"


def build(now):
    st = json.load(open(os.path.join(RQ1, "实验登记_状态.json"), encoding="utf-8"))
    rows = list(csv.DictReader(open(os.path.join(RQ1, "实验登记表.csv"), encoding="utf-8")))
    last = json.load(open(LAST)) if os.path.exists(LAST) else {}
    cut = datetime.strptime(last["time"], "%Y-%m-%d %H:%M") if "time" in last else now - timedelta(hours=HOURS)
    span = f"{(now - cut).total_seconds() / 3600:.0f} 小时"
    p = st["folds_done"]
    delta = f"（{span} +{p - last['folds_done']}）" if "folds_done" in last and p > last["folds_done"] else ""
    lines = [f"已训完 {p} 个任务{delta}" + (f"，还剩约 {st['days_left']:.0f} 天" if st.get("days_left") else "")]

    run = [r for r in rows if r["状态"] == "训练中" and r["实验"]]  # 临时任务（推理等）不算
    lines.append(f"正在训 {len(run)} 个任务" + ("：" if run else ""))
    for r in run:
        pct = 100 * int(r["epoch"] or 0) / int(r["num_epochs"] or 1000)
        eta = f"，约 {hm(r['预计训完'])} 训完" if r["预计训完"] else ""
        lines.append(f"- {short(r)}（{r['最新副本']}，{pct:.0f}%{eta}）")
    fin = {}
    for r in rows:
        if r["状态"] == "训完" and r["最后更新"] and datetime.strptime(r["最后更新"], "%Y-%m-%d %H:%M:%S") >= cut:
            fin.setdefault(short(r).rsplit(" f", 1)[0], []).append(f"f{r['fold']}")
    if fin:
        lines.append(f"{span}内训完：" + "；".join(f"{k} {'、'.join(v)}" for k, v in fin.items()))
    res, other = {}, set()
    try:
        q = json.loads(subprocess.run([os.path.join(REPO, "scripts", "queue", "qs"), "--json"], capture_output=True, text=True, timeout=180).stdout)
    except Exception:  # noqa: BLE001
        q = {}
    for name, d in q.items():
        for t in d.get("tasks", []):
            end = C.local_time(t["end"])
            r = PL.label(t["label"] or "") if t["status"] == "Success" and end and end >= cut else None
            if r and r[0] == "其他":
                other.add(r[1])
            elif r:
                res.setdefault(r[0], {}).setdefault(r[1], set()).add(r[2])
    if res or other:
        lines.append(f"{span}内出了结果：")
        for k, v in sorted(res.items(), key=lambda x: (x[0] in ("分析", "所有实验"), x[0])):
            items = [("、".join(sorted(fs - {None})) + " " if fs - {None} else "") + st for st, fs in v.items()]
            lines.append(f"- {k}：{'；'.join(items) if k == '分析' else '、'.join(items)}")
        if other:
            lines.append(f"- 另有 {len(other)} 个一次性任务")
    down = [name for name, d in q.items() if "error" in d]
    try:
        hs = json.load(open(H.STATE))
        probs = [PL.problem(pid, x) for pid, x in hs.get("problems", {}).items() if x.get("active") and x.get("level") != "info"]
    except Exception:  # noqa: BLE001
        probs = []
    probs += [f"{name} 的任务队列读不到" for name in down]
    lines.append("需要处理：" + ("\n" + "\n".join("- " + x for x in probs) if probs else "无"))
    return "\n".join(lines), p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    now = datetime.now()
    msg, p = build(now)
    if a.dry_run:
        print(msg)
        return
    H.push(msg, title=f"PhD 进度 {now:%H:%M}", priority="low", tags="bar_chart")
    os.makedirs(os.path.dirname(LAST), exist_ok=True)
    json.dump({"folds_done": p, "time": now.strftime("%Y-%m-%d %H:%M")}, open(LAST, "w"))


if __name__ == "__main__":
    main()
