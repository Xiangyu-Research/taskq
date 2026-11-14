#!/usr/bin/env python3
"""taskq status on the hub, for people: one screen answering "is it fine, what needs me, what runs where, when are results due".

    taskq status               the summary
    taskq status --all         every task, grouped by state, each group by machine
    taskq status <word>...     one machine (name or label), or the tasks whose key contains every word

Reads only what is on the hub: the hub queue, executor state recorded by relay.py each round (relay status.json), problems,
notices and progress snapshots in the hub database. Nothing connects to other machines, so the numbers are as of the last
health round (shown at the top). --json output stays with taskq status --json (the queue) and taskq status --all --json (overview).
"""
import json
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
import spec as S  # noqa: E402
import notice as NT  # noqa: E402
import overview as O  # noqa: E402

M = cfg.mtab()
P = proto

# plain words for where a task is (first match wins, in this order on the screen)
RUNNING, OVERDUE, SENDING, HELD, WAITING, BACK = "running", "overdue", "being sent", "held", "waiting there", "done, coming back"
FAILED, MISSING, STOPPED, BAD, NOT_SENT, NOT_QUEUED, DONE = ("failed", "missing inputs", "stopped", "spec unreadable", "not sent yet",
                                                             "not queued yet", "done")
ORDER = (OVERDUE, RUNNING, SENDING, HELD, WAITING, BACK, FAILED, MISSING, STOPPED, BAD, NOT_SENT, NOT_QUEUED, DONE)
HUB_WORDS = {P.S_FAILED: FAILED, P.S_INPUT: MISSING, P.S_STOPPED: STOPPED, P.S_BAD: BAD, P.S_DONE: DONE,
             P.S_FREE: NOT_SENT, P.S_STALE: NOT_SENT, P.S_BUSY: NOT_SENT}


def label(n):
    return (M.MACHINES.get(n) or {}).get("label", n)


def when(ts, now):
    """Epoch seconds -> HH:MM today, otherwise MM-DD HH:MM."""
    d = datetime.fromtimestamp(ts)
    return d.strftime("%H:%M") if d.date() == now.date() else d.strftime("%m-%d %H:%M")


def ago(sec):
    sec = max(0, int(sec))
    return f"{sec // 60} min" if sec < 2 * 3600 else f"{sec / 3600:.1f} h" if sec < 2 * 86400 else f"{sec / 86400:.1f} days"


# ---------------- reading ----------------
def read_local(now):
    """Everything the summary uses, from this machine only -> dict(queue, relay, problems, checked, notices, changes)."""
    import taskq
    try:
        queue = taskq.local_status()
    except taskq.DiskError:
        queue = None
    try:
        relay = json.load(open(O.relay_status_file()))
    except (OSError, ValueError):
        relay = {}
    db = NT.open_db(write=False)
    probs, checked = NT.problems(db) if db else ({}, None)
    import events
    changes = events.transitions(db, now.timestamp() - P.SUMMARY_WINDOW, (P.S_DONE, P.S_FAILED)) if db else []
    return dict(queue=queue, relay=relay, problems=NT.problem_rows(probs), checked=checked,
                notices=sorted(NT.notices(db)) if db else [], changes=changes)


# ---------------- building ----------------
def progress_of(f):
    """An executor's record of a task (relay status.json) -> (fraction or None, eta datetime or None, last progress datetime or None)."""
    runs = f.get("runs") or []
    total = O.total_of(f)
    done = max([r.get("epochs_done") or 0 for r in runs] or [0])
    if f.get("final"):
        return 1.0, None, None
    tl = max(runs, key=lambda r: r.get("end") or "").get("tl") if runs else None
    last = datetime.strptime(tl[-1][0], "%Y-%m-%d %H:%M:%S") if tl else None
    frac = min(done / total, 1.0) if total and done else None
    return frac, (S.eta_at(runs, total) if total else None), last


def build(d, now):
    """read_local() -> dict(now, checked, attention, notices, machines, tasks, steps, changes)."""
    q = d["queue"]
    qt = {t["key"]: t for t in (q or {}).get("tasks") or [] if not P.is_smoke(t["key"])}
    relay = d["relay"]
    tasks = {}
    for t in S.all_tasks(list(qt.values())):
        k, sp = t["key"], t["spec"]
        x = qt.get(k)
        tasks[k] = dict(key=k, step=sp.get("step") or sp.get("recipe") or "?", word=HUB_WORDS.get((x or {}).get("state"), NOT_QUEUED),
                        where=None, since=None, frac=None, eta=None, last=None)
    machines = []
    attention = [p["plain"] or p["msg"] for p in d["problems"] if not p["id"].startswith("notice:")]
    for n in M.executors():
        r = relay.get(n) if isinstance(relay.get(n), dict) else {}
        age = now.timestamp() - (r.get("time") or 0)
        there = r.get("tasks") or {}
        held = r.get("held") or {}
        for k, x in qt.items():
            if x.get("state") != P.S_CLAIMED or x.get("assigned") != n or k not in tasks:
                continue
            t = tasks[k]
            t["where"] = n
            f = there.get(k)
            if f is None:
                t["word"] = HELD if k in held else SENDING
                continue
            st = f.get("state")
            if st == P.S_DONE:
                t["word"] = BACK
            elif st == P.S_CLAIMED:
                t["word"], t["since"] = RUNNING, f.get("since")
                t["frac"], t["eta"], t["last"] = progress_of(f)
                if t["eta"] and t["eta"].timestamp() < now.timestamp() - P.SUMMARY_OVERDUE:
                    t["word"] = OVERDUE
                    last = f", last progress {t['last']:%m-%d %H:%M}" if t["last"] else ""
                    attention.append(f"{k} on {label(n)}: past its estimated finish ({t['eta']:%m-%d %H:%M}){last}")
            else:
                t["word"] = WAITING
        mine = [t for t in tasks.values() if t["where"] == n]
        count = {w: sum(t["word"] == w for t in mine) for w in ORDER}
        ws = M.workers(n)
        slots = sum(w.get("slots") or 1 for w in ws) if M.MACHINES[n].get("scheduler") == "pueue" else None
        jobs = [j[2] for j in r.get("squeue") or []]
        machines.append(dict(name=n, label=label(n), paused=M.paused(n), age=age if r else None, stale=not r or age >= O.RELAY_FRESH,
                             count=count, slots=slots, jobs=(jobs.count("RUNNING"), jobs.count("PENDING")) if r.get("squeue") is not None else None,
                             held=held, running=[t for t in mine if t["word"] in (RUNNING, OVERDUE)]))
        if r and age >= O.RELAY_FRESH:
            attention.append(f"no news from {label(n)} for {ago(age)} (last recorded {when(r['time'], now)})")
    steps = {}
    for t in tasks.values():
        s = steps.setdefault(t["step"], dict(total=0, **{w: 0 for w in ORDER}))
        s["total"] += 1
        s[t["word"]] += 1
    soon = sorted((t for t in tasks.values() if t["eta"] and 0 <= t["eta"].timestamp() - now.timestamp() <= P.SUMMARY_WINDOW),
                  key=lambda t: t["eta"])
    ch = [c for c in d["changes"] if not P.is_smoke(c["key"])]
    return dict(now=now, checked=d["checked"], queue_ok=q is not None, attention=attention, notices=d["notices"], machines=machines,
                tasks=tasks, steps=steps, soon=soon, done=sum(c["state"] == P.S_DONE for c in ch), failed=sum(c["state"] == P.S_FAILED for c in ch))


# ---------------- text ----------------
def task_line(t, now, where=False):
    """where=True (listings grouped by state): a machine column instead of the state word."""
    bits = [] if where else [t["word"]]
    if t["frac"] is not None:
        bits.append(f"{100 * t['frac']:.0f}%")
    if t["eta"] and t["word"] == RUNNING:
        bits.append(f"done ~{when(t['eta'].timestamp(), now)}")
    elif t["since"] and t["word"] in (RUNNING, OVERDUE) and t["frac"] is None:
        bits.append(f"for {ago(now.timestamp() - t['since'])}")
    w = f"{label(t['where']):<7}" if where and t["where"] else (" " * 7 if where else "")
    return f"    {w}{t['key']:<58} {'  '.join(bits)}".rstrip()


def machine_head(m):
    c = m["count"]
    run = c[RUNNING] + c[OVERDUE]
    bits = [f"running {run}"]
    if m["slots"] is not None:
        bits.append(f"free {max(0, m['slots'] - run)}/{m['slots']}")
    if m["jobs"] is not None:
        bits.append(f"jobs {m['jobs'][0]} running, {m['jobs'][1]} queued")
    wait = c[WAITING] + c[SENDING] + c[HELD]
    if wait:
        bits.append(f"waiting {wait}" + (f" ({c[SENDING]} being sent)" if c[SENDING] else "") + (f" ({c[HELD]} held)" if c[HELD] else ""))
    if c[BACK]:
        bits.append(f"coming back {c[BACK]}")
    if m["paused"]:
        bits.append(f"paused: {m['paused']}")
    return f"  {m['label']:<7}" + ", ".join(bits)


def text(o):
    now = o["now"]
    n = len(o["attention"])
    head = "running normally" if n == 0 and o["queue_ok"] else (f"{n} things need you" if n != 1 else "1 thing needs you") if o["queue_ok"] else "hub queue unreadable"
    out = [f"taskq  {now:%Y-%m-%d %H:%M}   {head}", f"as of the health check at {o['checked'] or '(none yet)'}"]
    if o["attention"]:
        out += ["", "Needs you"] + [f"  ! {a}" for a in o["attention"]]
    out += ["", "Machines"]
    for m in o["machines"]:
        out.append(machine_head(m))
        out += [task_line(t, now) for t in sorted(m["running"], key=lambda t: t["key"])]
        out += [f"    held: {k} ({h.get('why')})" for k, h in sorted(m["held"].items())]
    if o["soon"]:
        out += ["", f"Due in the next {P.SUMMARY_WINDOW // 3600} hours ({len(o['soon'])})"]
        out += [f"  {when(t['eta'].timestamp(), now):<12} {label(t['where']):<7} {t['key']}" for t in o["soon"]]
    out += ["", "Progress (done / all)"]
    for s, c in sorted(o["steps"].items(), key=lambda x: -x[1]["total"]):
        extra = [f"{c[w]} {w}" for w in (RUNNING, OVERDUE, FAILED, MISSING) if c[w]]
        left = c[WAITING] + c[SENDING] + c[HELD] + c[NOT_SENT]
        extra += [f"{left} waiting"] if left else []
        extra += [f"{c[NOT_QUEUED]} not queued yet"] if c[NOT_QUEUED] else []
        out.append(f"  {s:<10} {c[DONE]:>4} / {c['total']:<5}" + ("  " + ", ".join(extra) if extra else ""))
    out += ["", f"Last {P.SUMMARY_WINDOW // 3600} hours: {o['done']} done, {o['failed']} failed"]
    if o["notices"]:
        out.append(f"Notes ({len(o['notices'])}): {', '.join(o['notices'])}   (taskq notice list)")
    out.append("More: taskq status --all | taskq status <machine> | taskq status <part of a key>")
    return "\n".join(out)


def listing(o, tasks, title):
    """Tasks grouped by state, each group by machine then key."""
    now = o["now"]
    out = [f"taskq  {now:%Y-%m-%d %H:%M}   {title}, as of the health check at {o['checked'] or '(none yet)'}"]
    for w in ORDER:
        g = sorted((t for t in tasks if t["word"] == w), key=lambda t: (label(t["where"] or ""), t["key"]))
        if g:
            out += ["", f"{w} ({len(g)})"] + [task_line(t, now, where=True) for t in g]
    return "\n".join(out) if len(out) > 1 else out[0] + "\n\nno such task"


def main(argv):
    import argparse
    ap = argparse.ArgumentParser(prog="taskq status", description="The summary (hub); --all: every task; <word>: a machine or matching tasks")
    ap.add_argument("--all", action="store_true", help="every task, grouped by state")
    ap.add_argument("words", nargs="*", help="a machine name or label, or words every listed key must contain")
    a = ap.parse_args(argv)
    now = datetime.now()
    o = build(read_local(now), now)
    tasks = list(o["tasks"].values())
    if a.all:
        print(listing(o, tasks, "every task"))
    elif a.words:
        m = next((x for x in o["machines"] if len(a.words) == 1 and a.words[0] in (x["name"], x["label"])), None)
        if m:
            print(machine_head(m))
            print(listing(o, [t for t in tasks if t["where"] == m["name"]], f"tasks on {m['label']}"))
        else:
            print(listing(o, [t for t in tasks if all(w in t["key"] for w in a.words)], "tasks matching " + " ".join(a.words)))
    else:
        print(text(o))
    return 0 if o["queue_ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
