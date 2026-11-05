#!/usr/bin/env python3
"""汇总 GPU 使用记录（monitor/gpu_log.sh 写的）：每张卡在一天各个钟点被谁占着、别人的每段使用从几点到几点（2026-10-14，11/5 搬进核心）。

    taskq gpu usage [目录] [--me 账号] [--days N] [--json]

目录默认是 gpu_log.sh 写的地方（环境变量 GPU_LOG_DIR，没设时 proto.GPU_LOG_DIR）；--me 默认本账号；--days 只看最近 N 天（0 = 全部，默认 14）。
三部分：
1. 每张卡按钟点（0–23 时）的占用比例，分"别人 / 自己 / 空闲"，工作日和周末分开（同一时刻卡上有别人就算别人，只有自己算自己）；
2. 别人的每段使用（同一账号在同一张卡上，中间断开 ≤ 10 分钟算同一段）：开始、结束、时长、最大显存、程序；
3. 每张卡每个别人用的总时长。
"空闲" = 卡上没有任何进程。--json：一行 JSON，字段见 proto 的 GPU_USAGE_*。只用标准库。
"""
import argparse
import csv
import glob
import json
import os
import pwd
import sys
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "queue"))
import proto  # noqa: E402

GAP = timedelta(minutes=10)
ORDER = {"idle": 0, "me": 1, "other": 2}
DAYTYPES = (("weekday", "工作日", lambda t: t.weekday() < 5), ("weekend", "周末", lambda t: t.weekday() >= 5))
FMT = "%Y-%m-%d %H:%M:%S"


def default_dir():
    return os.environ.get("GPU_LOG_DIR") or os.path.expanduser(proto.GPU_LOG_DIR)


def load(d, days):
    rows = []
    for f in sorted(glob.glob(os.path.join(glob.escape(d), "gpu_*.csv"))):
        with open(f, errors="replace") as fh:
            for r in csv.DictReader(fh):
                try:
                    r["t"] = datetime.strptime(r["time"], FMT)
                except (TypeError, ValueError):
                    continue
                rows.append(r)
    if days and rows:
        start = max(r["t"] for r in rows) - timedelta(days=days)
        rows = [r for r in rows if r["t"] >= start]
    return rows


def summarize(rows, me):
    """→ dict(hours={weekday|weekend: {(gpu, 时): {other, me, idle}}}, gpus, spans, totals)。"""
    state = {}
    for r in rows:
        k = (r["gpu"], r["t"])
        s = "idle" if not r["user"] else ("me" if r["user"] == me else "other")
        if ORDER[s] >= ORDER[state.get(k, "idle")]:
            state[k] = s
    gpus = sorted({g for g, _ in state})
    hours = {}
    for name, _, sel in DAYTYPES:
        cnt = defaultdict(lambda: defaultdict(int))
        for (g, t), s in state.items():
            if sel(t):
                cnt[(g, t.hour)][s] += 1
        hours[name] = cnt
    by = defaultdict(list)
    for r in rows:
        if r["user"] and r["user"] != me:
            by[(r["gpu"], r["user"])].append(r)
    seg = []
    for (g, u), rs in by.items():
        rs.sort(key=lambda r: r["t"])
        cur = None
        for r in rs:
            v = (r.get("proc_mem_mib") or "").strip()
            mem = int(v) if v.isdigit() else 0
            if cur and r["t"] - cur["end"] <= GAP:
                cur["end"] = r["t"]
                cur["mem"] = max(cur["mem"], mem)
                cur["progs"].add(r.get("program") or "")
            else:
                if cur:
                    seg.append(cur)
                cur = dict(gpu=g, user=u, start=r["t"], end=r["t"], mem=mem, progs={r.get("program") or ""})
        if cur:
            seg.append(cur)
    seg.sort(key=lambda s: s["start"])
    tot = defaultdict(float)
    for s in seg:
        tot[(s["gpu"], s["user"])] += (s["end"] - s["start"]).total_seconds() / 3600
    return dict(gpus=gpus, hours=hours, spans=seg, totals=tot)


def as_json(a, rows, S):
    out = dict(version=proto.VERSION, dir=a.dir, me=a.me, days=a.days, hours={}, spans=[], totals=[])
    out.update({"from": min(r["t"] for r in rows).strftime(FMT) if rows else None, "to": max(r["t"] for r in rows).strftime(FMT) if rows else None})
    out = {k: out[k] for k in proto.GPU_USAGE_TOP}
    if not rows:
        return out
    for name, _, _ in DAYTYPES:
        cnt = S["hours"][name]
        out["hours"][name] = [dict(gpu=g, hour=h, n=sum(c.values()), other=c["other"], me=c["me"], idle=c["idle"])
                              for g in S["gpus"] for h in range(24) for c in [cnt.get((g, h))] if c]
    out["spans"] = [dict(gpu=s["gpu"], user=s["user"], start=s["start"].strftime(FMT), end=s["end"].strftime(FMT),
                            hours=round((s["end"] - s["start"]).total_seconds() / 3600, 3), max_mem_mib=s["mem"],
                            programs=sorted(s["progs"])) for s in S["spans"]]
    out["totals"] = [dict(gpu=g, user=u, hours=round(h, 3)) for (g, u), h in sorted(S["totals"].items())]
    return out


def as_text(a, rows, S):
    print(f"记录：{min(r['t'] for r in rows):%m-%d %H:%M} 到 {max(r['t'] for r in rows):%m-%d %H:%M}，自己 = {a.me}\n")
    print("## 1. 按钟点的占用比例（别人 / 自己 / 空闲，%）")
    for name, title, _ in DAYTYPES:
        cnt = S["hours"][name]
        if not cnt:
            continue
        print(f"\n### {title}")
        print("时 | " + " | ".join(f"GPU{g} 别人/自己/空闲" for g in S["gpus"]))
        for h in range(24):
            cells = []
            for g in S["gpus"]:
                c = cnt.get((g, h))
                n = sum(c.values()) if c else 0
                cells.append("—" if not n else f"{100 * c['other'] // n:3d} / {100 * c['me'] // n:3d} / {100 * c['idle'] // n:3d}")
            print(f"{h:02d} | " + " | ".join(cells))
    print("\n## 2. 别人的每段使用")
    print("GPU | 账号 | 开始 | 结束 | 时长 | 最大显存 | 程序")
    for s in S["spans"]:
        dur = s["end"] - s["start"]
        print(f"{s['gpu']} | {s['user']} | {s['start']:%m-%d %a %H:%M} | {s['end']:%m-%d %H:%M} | "
              f"{dur.total_seconds() / 3600:.1f} h | {s['mem'] / 1024:.1f} GB | {', '.join(sorted(s['progs']))}")
    print("\n## 3. 别人用的总时长（小时）")
    for (g, u), h in sorted(S["totals"].items()):
        print(f"GPU{g} {u}: {h:.1f}")


def main(argv=None):
    ap = argparse.ArgumentParser(description="汇总 GPU 使用记录（gpu_log.sh 写的）")
    ap.add_argument("dir", nargs="?", default=None, help="记录的文件夹（默认 GPU_LOG_DIR，没设时 %s）" % proto.GPU_LOG_DIR)
    ap.add_argument("--me", default=None, help="自己的账号（默认本账号）")
    ap.add_argument("--days", type=int, default=14, help="只看最近 N 天（0 = 全部）")
    ap.add_argument("--json", action="store_true", help="一行 JSON（proto 的 GPU_USAGE_*）")
    a = ap.parse_args(argv)
    a.dir = os.path.expanduser(a.dir) if a.dir else default_dir()
    a.me = a.me or os.environ.get("USER") or pwd.getpwuid(os.getuid()).pw_name
    rows = load(a.dir, a.days)
    S = summarize(rows, a.me)
    if a.json:
        print(json.dumps(as_json(a, rows, S), ensure_ascii=False))
    elif not rows:
        print(f"没有记录（{a.dir}）")
    else:
        as_text(a, rows, S)
    return 0


if __name__ == "__main__":
    sys.exit(main())
