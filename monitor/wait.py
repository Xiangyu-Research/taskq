#!/usr/bin/env python3
"""taskq wait: wait on the hub until tasks reach a state, then exit; reads only, ends by itself.

    taskq wait <key>... [--until running|done|failed|settled] [--timeout 6h] [--every SECONDS]

Each task's word is the one taskq status shows (queue/taskstate.py, as of the last health round for executors). --until (default
settled): running = running, overdue, coming back or done; done; failed = failed or spec unreadable; settled = done, failed, spec
unreadable or stopped. A task that settles in another word ends its wait early. Prints a line whenever a word changes. Checks every
--every seconds (default 60), doubling up to 10 minutes. --timeout (default 24h): 90s, 30m, 6h, 2d. Exit codes (proto.WAIT_EXIT):
0 every task reached, 1 some settled in another word, 4 a key not known or removed from the queue without results, 124 timed out.
"""
import argparse
import os
import re
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
sys.path.insert(0, HERE)
import proto as P  # noqa: E402

NAME = {v: k for k, v in P.TASK_WORDS.items()}
UNIT = dict(s=1, m=60, h=3600, d=86400)


def seconds(x):
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd]?)", x.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"not a duration: {x} (90s, 30m, 6h, 2d)")
    return float(m.group(1)) * UNIT[m.group(2) or "s"]


def words():
    """-> ({key: word name}, keys in the hub queue)."""
    import taskq
    import taskstate as TS
    import hubstate as HS
    import notice as NT
    import events
    q = taskq.local_status().get("tasks") or []
    db = NT.open_db(write=False)
    w = TS.compute(q, HS.sites(), events.stored(db) if db else set(), datetime.now())
    return {k: NAME.get(t["word"], t["word"]) for k, t in w.items()}, {t["key"] for t in q}


def verdict(want, w, seen):
    if w is None or (w == "not_queued" and seen):
        return "gone"
    if w in P.WAIT_UNTIL[want]:
        return "reached"
    return "other" if w in P.WAIT_SETTLED else None


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq wait", description=__doc__.split("\n\n")[0])
    ap.add_argument("keys", nargs="+")
    ap.add_argument("--until", choices=list(P.WAIT_UNTIL), default="settled")
    ap.add_argument("--timeout", type=seconds, default=P.WAIT_TIMEOUT)
    ap.add_argument("--every", type=seconds, default=P.WAIT_POLL[0])
    try:
        a = ap.parse_args(argv)
    except SystemExit as e:
        return P.WAIT_EXIT["usage"] if e.code else 0
    import taskq
    if taskq.here() != taskq.queue_host():
        print(f"taskq wait: runs on the hub ({taskq.queue_host()}); this is {taskq.here()}", file=sys.stderr)
        return P.WAIT_EXIT["usage"]
    end, step, last, seen, out = time.time() + a.timeout, a.every, {}, set(), {}
    while True:
        cur, queued = words()
        seen |= queued & set(a.keys)
        for k in a.keys:
            if k in out:
                continue
            w = cur.get(k)
            if w != last.get(k, ""):
                print(f"{datetime.now():%H:%M} {k}: {P.TASK_WORDS.get(w, w) if w else 'not known'}", flush=True)
                last[k] = w
            v = verdict(a.until, w, k in seen)
            if v:
                out[k] = v
        if len(out) == len(a.keys):
            break
        if time.time() >= end:
            print(f"taskq wait: timed out; still waiting on {', '.join(k for k in a.keys if k not in out)}", file=sys.stderr)
            return P.WAIT_EXIT["timeout"]
        time.sleep(max(0.0, min(step, end - time.time())))
        step = min(step * 2, max(P.WAIT_POLL[1], a.every))
    for r in ("gone", "other"):
        if r in out.values():
            return P.WAIT_EXIT[r]
    return P.WAIT_EXIT["reached"]


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
