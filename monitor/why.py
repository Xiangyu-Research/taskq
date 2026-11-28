#!/usr/bin/env python3
"""taskq why: everything that decides where one task is and why it is or is not running, on one page; reads only.

    taskq why <key> [--json]

Its word (as taskq status shows it), where it is claimed and since when, stop and missing-input marks, failures that count toward
the retry limit and when an executor may take it again, why the last health round sent it to no executor (each executor's reason,
from the assignment that round recorded), switches that touch it (hub queue off, pause or drain of the executors it can go to,
hold patterns), operations under way on it (taskq intents), its last 10 events, the last failure's log tail, and one suggested
next command. Exit code 4 when the key is neither in the hub queue nor in the experiment table.
"""
import argparse
import fnmatch
import json
import os
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
sys.path.insert(0, HERE)
import proto as P  # noqa: E402

NAME = {v: k for k, v in P.TASK_WORDS.items()}


def hm(t):
    if isinstance(t, str):
        try:
            t = float(t)
        except ValueError:
            return t
    return time.strftime("%m-%d %H:%M", time.localtime(t)) if t else "?"


def gather(key):
    """-> dict with every part taskq why shows, or None when the key is unknown (neither in the hub queue nor the experiment table)."""
    import events
    import hubstate as HS
    import intents as I
    import notice as NT
    import policy
    import refill
    import switches
    import taskq
    import taskstate as TS
    q = taskq.local_status().get("tasks") or []
    x = next((t for t in q if t["key"] == key), None)
    db = NT.open_db(write=False)
    w = TS.compute(q, HS.sites(), events.stored(db) if db else set(), datetime.now()).get(key)
    if x is None and w is None:
        return None
    word = NAME.get(w["word"], w["word"]) if w else "not_queued"
    out = dict(key=key, word=word, text=P.TASK_WORDS.get(word, word), where=(w or {}).get("where"), frac=(w or {}).get("frac"),
               eta=(w or {}).get("eta").strftime("%m-%d %H:%M") if (w or {}).get("eta") else None, queued=x is not None)
    holds = [(pat, "policy.py HOLD") for pat in policy.HOLD] + [(r["target"], switches.why(r)) for r in switches.rows() if r["name"] == "hold"]
    out["holds"] = [dict(pattern=p, why=y) for p, y in holds if fnmatch.fnmatch(key, p)]
    places = []
    if x is not None:
        sp = x.get("spec")
        try:
            places = refill.places(x.get("recipe"), sp) if sp else []
        except Exception:  # noqa: BLE001
            places = []
        ci = {k: x.get(k) for k in ("worker", "host", "assigned", "since", "beat")}
        out.update(nice=x.get("nice"), exp=x.get("exp"), state=x.get("state"), note=x.get("note"), claim=ci if ci["worker"] or ci["assigned"] else None,
                   stopped=taskq.stopped_info(key) or None, input_since=x.get("input_since"), failures=x.get("failures") or 0,
                   skipped_by=x.get("skipped_by") or [], retry_at={n: hm(t) for n, t in (x.get("retry_at") or {}).items()},
                   fail_lines=[dict(time=hm(ts), site=s, why=y) for ts, s, y in sorted(taskq.fail_lines(key))[-3:]],
                   fail_tail=x.get("fail_tail"), places=places)
        pl = HS.placement()
        if pl:
            out["placement"] = dict(time=hm(pl.get("t")), why=(pl.get("keys") or {}).get(key),
                                    by_site=(pl.get("by_site") or {}).get(key) or [],
                                    sites={n: r for n, r in (pl.get("sites") or {}).items() if n in places and r})
    machines = set(places) | {(out.get("claim") or {}).get("assigned")} - {None}
    out["switches"] = [dict(name=r["name"], target=r["target"], value=r.get("value"), why=switches.why(r)) for r in switches.rows()
                       if r["name"] == "queue" or (r["name"] in ("pause", "drain") and r["target"].split("/")[0] in machines)]
    out["intents"] = [dict(id=r["id"], kind=r["kind"], key=r["key"], note=r["note"]) for r in I.rows()
                      if r["key"] == key or r["args"].get("victim") == key]
    out["events"] = [events.fmt(d).splitlines()[0][:240] for d in events.query(db, key=key, limit=10)] if db else []
    out["next"] = suggest(out)
    return out


def suggest(o):
    k, w = o["key"], o["word"]
    if o["holds"] and not o["queued"]:
        h = o["holds"][0]
        return (f"remove {h['pattern']} from policy.py HOLD" if h["why"] == "policy.py HOLD"
                else f"taskq switch clear hold {h['pattern']}") + " to let the downstream round queue it"
    if not o["queued"]:
        return None if w == "done" else "its upstream steps are not done yet (taskq status --all)"
    if any(s["name"] == "queue" for s in o["switches"]):
        return "the hub queue is off: taskq queue on"
    if w == "stopped" or o.get("stopped"):
        return f"taskq resume {k}"
    if w in ("failed", "bad"):
        return f"fix the cause shown above, then taskq retry {k}"
    if w == "missing":
        return f"its inputs are not there yet (taskq show {k} lists them); it starts once they appear"
    if w in ("running", "overdue"):
        return f"taskq log {k}"
    if w == "not_sent":
        places, skipped = o.get("places") or [], set(o.get("skipped_by") or [])
        if places and set(places) <= skipped and not o.get("retry_at"):
            return f"every executor it can go to failed it: fix the cause, then taskq retry {k}"
        p = o.get("placement") or {}
        if p.get("by_site") and all(r == "full" for _, r in p["by_site"]):
            return f"taskq now {k} runs it first (making room after {P.NOW_WAIT // 60} minutes)"
        return "it is sent at the next round once an executor it can go to has room" if p.get("why") else None
    return None


def show(o):
    lines = [f"{o['key']}: {o['text']}" + (f" on {o['where']}" if o.get("where") else "")
             + (f", {round(o['frac'] * 100)}%" if o.get("frac") is not None else "") + (f", expected {o['eta']}" if o.get("eta") else "")]
    if o["queued"]:
        lines.append(f"  hub queue: {o['state']}, nice {o['nice']}, experiment {o.get('exp') or '(none)'}"
                     + (f"; {o['note']}" if o.get("note") else ""))
        c = o.get("claim")
        if c:
            by = c.get("assigned") or c.get("host") or "?"
            lines.append(f"  claimed by {by}" + (f" worker {c['worker']}" if c.get("worker") and c["worker"] != by else "")
                         + (f" since {hm(c['since'])}" if c.get("since") else ""))
        if o.get("stopped"):
            lines.append(f"  stopped: {o['stopped'].strip()}")
        if o.get("input_since"):
            lines.append(f"  waiting for inputs since {o['input_since']}")
        lines.append(f"  can go to: {', '.join(o['places']) or 'no executor (PLACE, machines and worker kinds do not intersect)'}")
        if o.get("failures") or o.get("fail_lines"):
            lines.append(f"  failures counted: {o['failures']} (limit {P.RETRY['skip'][3]} per executor before it stops trying)")
            lines += [f"    {f['time']} {f['site']}: {f['why']}" for f in o["fail_lines"]]
        if o.get("skipped_by"):
            lines.append(f"  skipped by: {', '.join(o['skipped_by'])}")
        for n, t in sorted((o.get("retry_at") or {}).items()):
            lines.append(f"  {n} may take it again from {t} (taskq retry {o['key']} for now)")
        p = o.get("placement")
        if not p and o["word"] == "not_sent":
            lines.append("  last assignment: not recorded yet (each health round records it)")
        if p and o["word"] == "not_sent":
            lines.append(f"  last assignment ({p['time']}): " + (p["why"] or "nothing held it back (being sent now, or added since)"))
            lines += [f"    {n}: {r}" for n, r in p.get("by_site") or []]
            lines += [f"    {n} got nothing that round: {r}" for n, r in sorted((p.get("sites") or {}).items())
                      if n not in {s for s, _ in p.get("by_site") or []}]
    for h in o["holds"]:
        lines.append(f"  held by {h['pattern']} ({h['why']})")
    for s in o["switches"]:
        lines.append(f"  switch {s['name']} {s['target'] or ''} {s.get('value') or ''} ({s['why']})".replace("  (", " ("))
    for it in o["intents"]:
        lines.append(f"  under way: {it['kind']} {it['key']} (taskq intents, id {it['id']}): {it['note']}")
    ft = o.get("fail_tail")
    if ft and ft.get("lines"):
        lines.append(f"  last failure {ft.get('time')}" + (f" on {ft['machine']}" if ft.get("machine") else "") + ", its last lines:")
        lines += [f"    | {ln}" for ln in ft["lines"][-8:]]
    if o["events"]:
        lines.append("  recent events:")
        lines += [f"    {e}" for e in o["events"]]
    if o.get("next"):
        lines.append(f"  next: {o['next']}")
    return "\n".join(lines)


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq why", description=__doc__.split("\n\n")[0])
    ap.add_argument("key")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    import taskq
    if taskq.here() != taskq.queue_host():
        print(f"taskq why: runs on the hub ({taskq.queue_host()}); this is {taskq.here()}", file=sys.stderr)
        return 2
    o = gather(a.key)
    if o is None:
        print(f"{a.key}: neither in the hub queue nor in the experiment table", file=sys.stderr)
        return taskq.GONE
    if a.json:
        print(P.JSON_MARK + json.dumps(dict(version=P.VERSION, why=o), ensure_ascii=False, default=str))
    else:
        print(show(o))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
