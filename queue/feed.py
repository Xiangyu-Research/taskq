#!/usr/bin/env python3
"""The only entry point for sending tasks: the hub's health check (monitor/health.py, every 30 minutes) calls run_all,
which hands off to queue/relay.py to handle the executors in the machine table (pueue and Slurm executors; details go to
~/relay.log, with one line in ~/feed.log pointing there). It also checks tasks in the hub queue assigned to a name that is not
an executor (left over after a machine was renamed or removed) and reports feed:nobody.

    python3 queue/feed.py [-n]       show what it would do (changes nothing; -n / --dry-run / --dry are the same; taskq plan includes it)
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # scheduler core root
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
    """Tasks in the hub queue assigned to a name that is not an executor -> {problem id: message (with data)}."""
    own = set(M.executors())
    by = {}
    for t in (ctq or {}).get("tasks") or []:
        if t.get("assigned") and t["assigned"] not in own and t["state"] != "done":
            by.setdefault(t["assigned"], []).append(t["key"])
    return {alert_id("feed", "nobody", w): alert(f"{len(ks)} tasks in the hub queue are assigned to {w}, but {w} is not an executor and nobody will run them ({ks[0]}...); "
                                                 f"release them: taskq worker release --worker {w} <key>...",
                                                 worker=w, n=len(ks), keys=ks) for w, ks in by.items()}


def run_all(s, dry=False):
    """Called by health.py -> (messages, stuck {problem id: message})."""
    try:
        import relay as RL  # queue/relay.py
        events, stuck = RL.run_all(s, dry=dry)
    except Exception as e:  # noqa: BLE001
        events, stuck = [], {alert_id("relay", "error"): f"queue/relay.py failed: {type(e).__name__}: {e}"[:300]}
    if events and not dry:
        log([f"executors {(', '.join(sorted(M.executors())))} (relay.py): {len(events)} entries, see ~/relay.log"])
    stuck = dict(stuck)
    stuck.update(nobody((s.get("taskq") or {}).get(M.hub())))
    return events, stuck


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="Send tasks to executors (run_all, called by every health check); here it only reads")
    ap.add_argument("-n", "--dry-run", "--dry", action="store_true", help="only show what it would do (the default)")
    ap.parse_args(argv)
    import health as H  # noqa: E402
    ev, st = run_all(H.gather(), dry=True)
    print("\n".join(ev + [f"stuck: {alert_msg(v)}" for v in st.values()]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
