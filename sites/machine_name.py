"""A machine named on the hub's command line -> its key in the machine table. Events, alerts and taskq status print labels, so
commands that take a machine accept either. Hub commands only: not in the bundle."""
import mtab as M


def key(name):
    """name when it is a key; else the one machine whose label is name (case ignored); else name unchanged, so the caller's
    "no such machine" message shows what was typed."""
    if not name or name in M.MACHINES:
        return name
    hits = [n for n, m in M.MACHINES.items() if (m.get("label") or "").casefold() == name.casefold()]
    return hits[0] if len(hits) == 1 else name
