#!/usr/bin/env python3
"""Summarize the GPU usage log written by monitor/gpu_log.sh.

    taskq gpu usage [dir] [--me ACCOUNT] [--days N] [--json]

Share of each card by hour (others / me / idle), each other user's usage spans, and totals per card. Uses only the stdlib.
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
DAYTYPES = (("weekday", "Weekday", lambda t: t.weekday() < 5), ("weekend", "Weekend", lambda t: t.weekday() >= 5))
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
    """-> dict(hours={weekday|weekend: {(gpu, hour): {other, me, idle}}}, gpus, spans, totals)."""
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
    print(f"Records: {min(r['t'] for r in rows):%m-%d %H:%M} to {max(r['t'] for r in rows):%m-%d %H:%M}, me = {a.me}\n")
    print("## 1. Share by hour (others / me / idle, %)")
    for name, title, _ in DAYTYPES:
        cnt = S["hours"][name]
        if not cnt:
            continue
        print(f"\n### {title}")
        print("hour | " + " | ".join(f"GPU{g} others/me/idle" for g in S["gpus"]))
        for h in range(24):
            cells = []
            for g in S["gpus"]:
                c = cnt.get((g, h))
                n = sum(c.values()) if c else 0
                cells.append("-" if not n else f"{100 * c['other'] // n:3d} / {100 * c['me'] // n:3d} / {100 * c['idle'] // n:3d}")
            print(f"{h:02d} | " + " | ".join(cells))
    print("\n## 2. Each other user's usage spans")
    print("GPU | user | start | end | hours | max mem | programs")
    for s in S["spans"]:
        dur = s["end"] - s["start"]
        print(f"{s['gpu']} | {s['user']} | {s['start']:%m-%d %a %H:%M} | {s['end']:%m-%d %H:%M} | "
              f"{dur.total_seconds() / 3600:.1f} h | {s['mem'] / 1024:.1f} GB | {', '.join(sorted(s['progs']))}")
    print("\n## 3. Total hours by other users")
    for (g, u), h in sorted(S["totals"].items()):
        print(f"GPU{g} {u}: {h:.1f}")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Summarize the GPU usage log written by gpu_log.sh")
    ap.add_argument("dir", nargs="?", default=None, help="dir with the records (default GPU_LOG_DIR, or %s when unset)" % proto.GPU_LOG_DIR)
    ap.add_argument("--me", default=None, help="your account (default: this account)")
    ap.add_argument("--days", type=int, default=14, help="only the last N days (0 = all)")
    ap.add_argument("--json", action="store_true", help="one line of JSON (proto GPU_USAGE_*)")
    a = ap.parse_args(argv)
    a.dir = os.path.expanduser(a.dir) if a.dir else default_dir()
    a.me = a.me or os.environ.get("USER") or pwd.getpwuid(os.getuid()).pw_name
    rows = load(a.dir, a.days)
    S = summarize(rows, a.me)
    if a.json:
        print(json.dumps(as_json(a, rows, S), ensure_ascii=False))
    elif not rows:
        print(f"No records ({a.dir})")
    else:
        as_text(a, rows, S)
    return 0


if __name__ == "__main__":
    sys.exit(main())
