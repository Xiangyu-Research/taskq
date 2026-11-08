#!/usr/bin/env python3
"""Summary of everything: hub queue, machines (health.gather, read-only), best copy per task, part logs, problems.

    python3 monitor/overview.py [--json] [--no-remote]     --no-remote: only this machine

--json prints one line of JSON with fields per proto.OVERVIEW_* (add-only). Each task: queue, copy, total, progress, eta, parts.
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
RELAY_FRESH = 2 * 3600  # executor state recorded by relay.py counts for this long


def relay_status_file():
    return cfg.state("relay", "status.json")


def relay_sources(now_ts):
    """Executor progress recorded by relay.py on each check -> {label: dict(name, time, tasks, seglogs)} (within 2 hours)."""
    try:
        st = json.load(open(relay_status_file()))
    except (OSError, ValueError):
        return {}
    out = {}
    for name, r in st.items():
        if not isinstance(r, dict) or now_ts - (r.get("time") or 0) >= RELAY_FRESH:
            continue
        tasks = {k: dict(f, metrics={}, resumable=None) for k, f in (r.get("tasks") or {}).items()}
        out[r.get("label") or name] = dict(name=name, time=r.get("time"), tasks=tasks, seglogs=[])
    return out


def machine_of(job):
    return M.machine_of_jobtag(job)


def best_copy(sources, key):
    """The same task in several places: the finished one, then most rounds done, then last update (first wins on a tie) -> (label, content) or (None, None)."""
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
    """Part logs of a task: <safe key>__<job>_part<n>.log."""
    prefixes = [proto.safe(key) + proto.SAFE]
    kinds = proto.LOG_PART
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
    """pueue label -> plain.label's [experiment, what, short] (intermediate step -> None); unknown -> ["Other", label, None]."""
    try:
        r = PL.label(label or "")
    except Exception:  # noqa: BLE001
        return ["Other", label or "", None]
    return list(r) if r else None


def health_view():
    db = H.NT.open_db(write=False)
    probs, checked = H.NT.problems(db) if db else ({}, None)
    return dict(checked=checked, problems=H.NT.problem_rows(probs, everything=True))


def build(s, relay=None):
    """gather() result -> summary (proto.OVERVIEW_TOP). relay: executor progress (default: relay.py's status.json)."""
    now = s.get("now") or datetime.now()
    hub = M.hub()
    L = lambda n: (M.MACHINES.get(n) or {}).get("label", n)  # noqa: E731
    q = (s.get("taskq") or {}).get(hub)
    queue = [t for t in (q or {}).get("tasks") or [] if not proto.is_smoke(t["key"])] if q and q.get("enabled") else []
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
    machines = [dict(name=n, label=L(n), scheduler=M.MACHINES[n].get("scheduler"), ok=bool(s["ok"].get(n)), why=(s.get("why") or {}).get(n),
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
                      epochs=epochs,
                      start=min([r["start"] for r in runs if r.get("start")] or [None], key=lambda v: v or ""),
                      last=best[0][2] or None, runs=runs, metrics=f.get("metrics") or {}, debug=f.get("debug") or {},
                      resumable=f.get("resumable")) if f else None,
            total=total, progress=progress, eta=eta.strftime("%Y-%m-%d %H:%M") if eta else None,
            parts=parts, commits=list(dict.fromkeys(p["commit"] for p in parts if p["commit"] and p["trained"]))))
    return dict(version=proto.VERSION, now=now.strftime("%Y-%m-%d %H:%M:%S"), hub=hub,
                data=dict(machine=M.store(), path=M.data()),
                queue=dict(enabled=bool(q and q.get("enabled")), readable=q is not None), sources=src_info, machines=machines,
                tasks=tasks, health=health_view(), code=s.get("code") or {})


def text(o):
    """For people: one line per machine, one per task."""
    lines = [f"{o['now']}, hub {o['hub']}, hub queue {'enabled' if o['queue']['enabled'] else 'disabled' if o['queue']['readable'] else 'unreadable'}"]
    for m in o["machines"]:
        g = ", ".join(f"GPU {x['gpu']} {x['util']}%" for x in m["gpus"]) or "-"
        lines.append(f"  {m['label']:<10} {'reachable' if m['ok'] else 'unreachable ' + (m['why'] or '')}  {g}")
    lines.append(f"{'key':<44} {'state':<8} {'progress':>8}  {'where':<10} eta")
    for t in o["tasks"]:
        st = (t["queue"] or {}).get("state") or "-"
        p = f"{100 * t['progress']:.0f}%" if t["progress"] is not None else "-"
        lines.append(f"{t['key']:<44} {st:<8} {p:>8}  {(t['copy'] or {}).get('where') or '-':<10} {t['eta'] or ''}")
    ps = [p for p in o["health"]["problems"] if p.get("active")]
    if ps:
        lines.append(f"Problems ({len(ps)}):")
        lines += [f"  {p['id']}: {p['msg']}" for p in ps]
    return "\n".join(lines)


def main(argv):
    ap = argparse.ArgumentParser(description="Summary of the hub queue, machines and each task (read-only)")
    ap.add_argument("--json", action="store_true", help="one line of JSON (fields per proto.OVERVIEW_*)")
    ap.add_argument("--no-remote", action="store_true", help="only this machine, do not connect to others")
    a = ap.parse_args(argv)
    o = build(H.gather(remote=not a.no_remote))
    if a.json:
        print(json.dumps(o, ensure_ascii=False, separators=(",", ":"), default=str))
    else:
        print(text(o))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
