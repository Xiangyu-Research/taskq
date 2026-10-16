#!/usr/bin/env python3
"""进度汇报（2026-10-16）：box 上 cron 每 3 小时（08:30–20:30）在更新登记表和看板之后跑一次（scripts/monitor/auto_refresh.sh registry），
推送到手机（和健康检查同一个 ntfy 频道）：总进度和 3 小时的变化、在训的折和预计训完时间、3 小时内训完 / 做完的、各处排队数、当前问题。

    python3 scripts/monitor/report.py            推送
    python3 scripts/monitor/report.py --dry-run  只打印
读：research/RQ1/实验登记_状态.json、实验登记表.csv（make_registry.py 刚生成的）、scripts/queue/qs --json、
  ~/.cache/phd-health/state.json（健康检查的当前问题）。上一次的总进度记在 ~/.cache/phd-health/report.json。
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
import health as H  # noqa: E402  push、mr_time、机器表

RQ1 = os.path.join(REPO, "research", "RQ1")
LAST = os.path.expanduser("~/.cache/phd-health/report.json")
HOURS = 3


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
    p = st["progress"]
    delta = f"（{HOURS} 小时 {100 * (p - last['progress']):+.1f}%）" if "progress" in last else ""
    lines = [f"总进度 {100 * p:.1f}%{delta}" + (f"，预计 {st['eta'][5:].replace('-', '/')} 全部训完" if st.get("eta") else "")]

    run = [r for r in rows if r["状态"] == "训练中"]
    if run:
        lines.append("在训：")
        lines += [f"- {r['最新副本']} {short(r)}：{r['epoch']}/{r['num_epochs'] or 1000}" + (f"，约 {hm(r['预计训完'])} 训完" if r["预计训完"] else "")
                  for r in run]
    cut = now - timedelta(hours=HOURS)
    fin = [short(r) for r in rows if r["状态"] == "训完" and r["最后更新"] and datetime.strptime(r["最后更新"], "%Y-%m-%d %H:%M:%S") >= cut]
    done, queued = [], []
    try:
        q = json.loads(subprocess.run([os.path.join(REPO, "scripts", "queue", "qs"), "--json"], capture_output=True, text=True, timeout=180).stdout)
    except Exception:  # noqa: BLE001
        q = {}
    for name, d in q.items():
        if "error" in d:
            queued.append(f"{name} 连不上")
            continue
        n = {}
        for t in d["tasks"]:
            end = H.mr_time(t["end"])
            if t["status"] == "Success" and end and end >= cut and not t["label"].startswith("wait"):
                done.append(t["label"] or f"#{t['id']}")
            if t["status"] == "Queued":
                n[t["group"]] = n.get(t["group"], 0) + 1
        queued.append(f"{name} " + ("、".join(f"{g} {k}" for g, k in sorted(n.items())) or "空"))
    for label, s in (st.get("slurm") or {}).items():
        if s:
            pend = sum(v for k, v in s["counts"].items() if "PENDING" in k)
            queued.append(f"{label} {pend}")
    if fin or done:
        lines.append(f"{HOURS} 小时内：" + "；".join(([f"训完 {'、'.join(fin)}"] if fin else []) +
                                              ([f"做完 {'、'.join(done[:8])}{'…' if len(done) > 8 else ''}"] if done else [])))
    lines.append("排队：" + "；".join(queued))
    try:
        hs = json.load(open(H.STATE))
        probs = [x["msg"] for x in hs.get("problems", {}).values() if x.get("active") and x.get("level") != "info"]
    except Exception:  # noqa: BLE001
        probs = []
    lines.append(f"问题：{len(probs)} 个，见登记表最上面" if probs else "问题：无")
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
    json.dump({"progress": p, "time": now.strftime("%Y-%m-%d %H:%M")}, open(LAST, "w"))


if __name__ == "__main__":
    main()
