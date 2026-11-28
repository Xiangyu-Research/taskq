"""Drain (taskq drain on the hub, which writes <root>/ROOT["overrides"] here): while this worker (WORKER_ID) or the whole machine
("*") is drained, claim nothing; "on": stop what is running, release it and exit 0; "soft": exit once nothing is running. An
entry past its until no longer counts, so a drain ends on time even when the hub cannot reach the machine."""
import json
import os
import time

import proto


def entry(w, now=None):
    """→ this worker's drain entry in force ({value, until, why}) or None (no file, unreadable, none, expired)."""
    root = os.environ.get("TASKQ_ROOT") or os.path.dirname(os.path.abspath(w.qdir))
    try:
        with open(os.path.join(os.path.expanduser(root), proto.ROOT["overrides"])) as f:
            d = json.load(f).get("drain") or {}
    except (OSError, ValueError, AttributeError):
        return None
    now = time.time() if now is None else now
    for k in (w.W, "*"):
        e = d.get(k) if isinstance(d, dict) else None
        if isinstance(e, dict) and (not e.get("until") or e["until"] > now):
            return e
    return None


def why(e):
    return "drained" + (f" ({e['why']})" if e.get("why") else "")


def active(w):
    return True


def admit(w):
    e = entry(w)
    return why(e) if e else None


def tick(w):
    e = entry(w)
    if not e:
        return
    busy = bool(w.runs or w.zomb)
    if e.get("value") == "soft":
        if not busy:
            w.shutdown(f"{why(e)}; nothing running, exiting", 0)
    elif busy:
        w.shutdown(f"{why(e)}; stopping training and releasing tasks", 0)
    else:
        w.shutdown(f"{why(e)}; exiting", 0)
