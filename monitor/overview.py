#!/usr/bin/env python3
"""全部状态的汇总（2026-11-04）：总队列、各机器（health.gather，只读）、每个任务进度最新的那份、分段日志、健康检查记下的问题，
给登记表这类汇总用，使用者那边不再 import 核心的内部函数。

    python3 monitor/overview.py [--json] [--no-remote]     --no-remote：只看本机

--json 打印一行 JSON，字段只加不改（proto.OVERVIEW_*）：
  version、now、hub、data（hub 的数据目录）、queue（enabled、readable）、
  sources：收集到训练状态的地方，按机器表的顺序，之后是执行机（relay.py 记下的、2 小时以内的）
  machines：机器表里 gather 看过的机器（gpus、pueue 的组和任务）
  tasks：实验表里的任务（按表的顺序），加上总队列里有、表里没有的；每个任务见 OVERVIEW_TASK
  health：健康检查记下的问题（checked、problems）；code：各机器的代码提交号（gather 的 code）
每个任务：
  queue    总队列里的情况（OVERVIEW_TASK_QUEUE；rank = 没做完的任务里排第几）；不在队列里 → null
  copy     几处都有时取训完的、做完的轮数最多、更新最晚的那份（OVERVIEW_COPY；where 是机器的 label）；哪里都没有 → null
  total    一共几轮：runs 里的 total，没有时进度读法 INFO 的 proto.TOTAL_INFO；不知道 → null
  progress 训完 = 1，否则 做完的轮数 / total（不超过 1）；不知道 → null
  eta      还在训的：按最近 3 小时的速度推算训完的时刻（%Y-%m-%d %H:%M）；算不出 → null
  parts    这个任务的分段日志（OVERVIEW_PART，同一作业的同一段只算一次）；commits：各段记的代码提交号（去重，按出现顺序）
"""
import argparse
import json
import os
import re
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(CORE, "queue"))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
import health as H  # noqa: E402
import common as C  # noqa: E402
import experiments as E  # noqa: E402
import plain as PL  # noqa: E402
import spec as S  # noqa: E402

M = H.M
RELAY_FRESH = 2 * 3600  # relay.py 记下的执行机状态这么久以内的才算


def relay_status_file():
    return os.path.join(os.path.expanduser(os.environ.get("RELAY_DIR") or cfg.state("relay", expand=False)), "status.json")


def relay_sources(now_ts):
    """relay.py 每次检查记下的执行机上各任务的进度 → {label: dict(name, time, tasks, seglogs)}（2 小时以内的）。"""
    try:
        st = json.load(open(relay_status_file()))
    except (OSError, ValueError):
        return {}
    out = {}
    for name, r in st.items():
        if not isinstance(r, dict) or now_ts - (r.get("time") or 0) >= RELAY_FRESH:
            continue
        tasks = {k: dict(f, archived=False, dice={}, resumable=None) for k, f in (r.get("tasks") or {}).items()}
        out[r.get("label") or name] = dict(name=name, time=r.get("time"), tasks=tasks, seglogs=[])
    return out


def machine_of(job):
    fn = getattr(M, "machine_of_jobtag", None)
    return fn(job) if fn else job


def best_copy(sources, key):
    """几处的同一个任务：训完的、做完的轮数最多、更新最晚的那份（一样时取前面的）→ (label, 内容) 或 (None, None)。"""
    best, where = None, None
    for lab, data in sources.items():
        f = (data.get("tasks") or {}).get(key)
        if not f:
            continue
        runs = f.get("runs") or []
        score = (bool(f.get("final")), max([r.get("epochs_done") or 0 for r in runs] or [0]), max([r.get("end") or "" for r in runs] or [""]))
        if best is None or score > best[0]:
            best, where = (score, f), lab
    return (where, best) if best else (None, None)


def parts_of(seglogs, key, sp):
    """这个任务的分段日志：<safe key>__<作业>_part<n>.log（以前 _seg<n>），和说明里 oldlogs 的 prefix 开头的。"""
    prefixes = [proto.safe(key) + proto.SAFE]
    old = sp.get("oldlogs") or {}
    if old.get("prefix"):
        prefixes.append(old["prefix"])
    kinds = "|".join(proto.LOG_PARTS)
    seen, out = set(), []
    for s in seglogs:
        n = s["name"]
        m = next((m for m in (re.match(re.escape(p) + r"(.+)_(?:%s)(\d+)\.log$" % kinds, n) for p in prefixes) if m), None)
        if not m or (m.group(1), m.group(2)) in seen:
            continue
        seen.add((m.group(1), m.group(2)))
        c = re.search(proto.LOG_HEAD_CODE, s.get("first") or "")
        out.append(dict(name=n, job=m.group(1), part=int(m.group(2)), machine=machine_of(m.group(1)), trained=bool(s.get("trained", True)),
                        commit=c.group(1) if c else None))
    return out


def total_of(f):
    runs = (f or {}).get("runs") or []
    t = max([r.get("total") or 0 for r in runs] or [0])
    if not t:
        t = ((f or {}).get("debug") or {}).get(proto.TOTAL_INFO) or 0
    return t if isinstance(t, (int, float)) and t > 0 else None


def pueue_view(d):
    if not d:
        return None
    if "error" in d:
        return dict(groups={}, tasks=[], error=d["error"])
    tasks = [dict(t, start_ts=C.pueue_time(t.get("start")), end_ts=C.pueue_time(t.get("end")), about=about(t.get("label")))
             for t in d.get("tasks") or []]
    return dict(groups=d.get("groups") or {}, tasks=tasks, error=None)


def about(label):
    """pueue 标签 → plain.label 的 [实验, 做了什么, 简称]（中间步骤 → None）；认不出 → ["其他", 标签, None]。"""
    try:
        r = PL.label(label or "")
    except Exception:  # noqa: BLE001
        return ["其他", label or "", None]
    return list(r) if r else None


def plain_problem(pid, p):
    try:
        return PL.problem(pid, p)
    except Exception:  # noqa: BLE001
        return p.get("msg")


def health_view():
    try:
        h = json.load(open(H.STATE))
    except (OSError, ValueError):
        return dict(checked=None, problems=[])
    ps = [dict(id=pid, **{k: p.get(k) for k in proto.OVERVIEW_PROBLEM if k not in ("id", "plain")}, plain=plain_problem(pid, p))
          for pid, p in (h.get("problems") or {}).items()]
    return dict(checked=h.get("checked"), problems=ps)


def build(s, relay=None):
    """gather() 的结果 → 汇总（proto.OVERVIEW_TOP）。relay：执行机的进度（默认读 relay.py 的 status.json）。"""
    now = s.get("now") or datetime.now()
    hub = M.hub()
    L = lambda n: (M.MACHINES.get(n) or M.SITES.get(n) or {}).get("label", n)  # noqa: E731
    q = (s.get("taskq") or {}).get(hub)
    queue = list((q or {}).get("tasks") or []) if q and q.get("enabled") else []
    rank = {t["key"]: i + 1 for i, t in enumerate(t for t in queue if t.get("state") != proto.S_DONE)}
    qt = {t["key"]: t for t in queue}
    seen = [n for n in M.MACHINES if n in (s.get("ok") or {})]
    sources, src_info = {}, []
    for n in seen:
        if n in (s.get("src") or {}):
            sources[L(n)] = s["src"][n]
            src_info.append(dict(name=n, label=L(n), kind="machine", time=None))
    for lab, r in (relay if relay is not None else relay_sources(now.timestamp())).items():
        sources[lab] = r
        src_info.append(dict(name=r.get("name"), label=lab, kind="relay", time=r.get("time")))
    seglogs = list({x["name"]: x for src in sources.values() for x in src.get("seglogs") or []}.values())
    machines = [dict(name=n, label=L(n), scheduler=M.MACHINES[n]["scheduler"], ok=bool(s["ok"].get(n)), why=(s.get("why") or {}).get(n),
                     gpus=(s.get("gpus") or {}).get(n, []), pueue=pueue_view((s.get("pueue") or {}).get(n))) for n in seen]
    tasks = []
    for t in S.all_tasks(queue):
        key, sp, e = t["key"], t["spec"], t["e"]
        x = qt.get(key)
        where, best = best_copy(sources, key)
        f = best[1] if best else None
        total = total_of(f)
        epochs = best[0][1] if best else 0
        runs = (f or {}).get("runs") or []
        progress = 1.0 if f and f.get("final") else (min(epochs / total, 1.0) if total and f else None)
        eta = S.eta_at(runs, total) if f and not f.get("final") and total else None
        o = S.outputs(sp)
        parts = parts_of(seglogs, key, sp)
        tasks.append(dict(
            key=key, exp=e["id"] if e else None, recipe=sp.get("recipe"), params=E.jsonable(sp.get("params") or {}),
            names=E.jsonable(sp.get("names") or {}), owner=sp.get("owner"), out=o["outs"][0],
            checkpoint=os.path.join(o["outs"][0], sp["checkpoint"]) if sp.get("checkpoint") else None,
            queue=dict(rank=rank.get(key), **{k: x.get(k) for k in proto.OVERVIEW_TASK_QUEUE if k != "rank"}) if x else None,
            copy=dict(where=where, source=next((i["name"] for i in src_info if i["label"] == where), None), final=bool(f.get("final")),
                      archived=f.get("archived"), epochs=epochs,
                      start=min([r["start"] for r in runs if r.get("start")] or [None], key=lambda v: v or ""),
                      last=best[0][2] or None, runs=runs, dice=f.get("dice") or {}, debug=f.get("debug") or {},
                      resumable=f.get("resumable")) if f else None,
            total=total, progress=progress, eta=eta.strftime("%Y-%m-%d %H:%M") if eta else None,
            parts=parts, commits=list(dict.fromkeys(p["commit"] for p in parts if p["commit"] and p["trained"]))))
    data = (M.MACHINES.get(hub) or {}).get("data")
    return dict(version=proto.VERSION, now=now.strftime("%Y-%m-%d %H:%M:%S"), hub=hub,
                data=os.environ.get("TASKQ_DATA") or (M.expand(data) if data else None),
                queue=dict(enabled=bool(q and q.get("enabled")), readable=q is not None), sources=src_info, machines=machines,
                tasks=tasks, health=health_view(), code=s.get("code") or {})


def text(o):
    """给人看的：每台机器一行、每个任务一行。"""
    lines = [f"{o['now']}，hub {o['hub']}，总队列{'开着' if o['queue']['enabled'] else '关着' if o['queue']['readable'] else '读不到'}"]
    for m in o["machines"]:
        g = "、".join(f"{x['gpu']}号卡 {x['util']}%" for x in m["gpus"]) or "-"
        lines.append(f"  {m['label']:<10} {'连得上' if m['ok'] else '连不上 ' + (m['why'] or '')}  {g}")
    lines.append(f"{'key':<44} {'状态':<8} {'进度':>5}  {'在哪':<10} 预计训完")
    for t in o["tasks"]:
        st = (t["queue"] or {}).get("state") or "-"
        p = f"{100 * t['progress']:.0f}%" if t["progress"] is not None else "-"
        lines.append(f"{t['key']:<44} {st:<8} {p:>5}  {(t['copy'] or {}).get('where') or '-':<10} {t['eta'] or ''}")
    ps = [p for p in o["health"]["problems"] if p.get("active")]
    if ps:
        lines.append(f"问题（{len(ps)}）：")
        lines += [f"  {p['id']}：{p['msg']}" for p in ps]
    return "\n".join(lines)


def main(argv):
    ap = argparse.ArgumentParser(description="总队列、各机器、每个任务的状态汇总（只读）")
    ap.add_argument("--json", action="store_true", help="一行 JSON（字段见 proto.OVERVIEW_*）")
    ap.add_argument("--no-remote", action="store_true", help="只看本机，不连别的机器")
    a = ap.parse_args(argv)
    o = build(H.gather(remote=not a.no_remote))
    if a.json:
        print(json.dumps(o, ensure_ascii=False, separators=(",", ":"), default=str))
    else:
        print(text(o))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
