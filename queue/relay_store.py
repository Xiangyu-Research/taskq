"""Relay bookkeeping on the hub (relay/state.json in the state dir, cfg.state: paths placed per machine and task, views,
bundles) and cleanup of executor roots.
Cleanup below the soft limit removes views the hub queue no longer wants (registered per-task views plus dirs under the dirs
where plugins build views, VIEW_ROOTS in sites/names.py; never half-built ones), then blobs with no symlink pointing at them,
least recently used first (never ones hard-linked elsewhere or pinned by a running stage), until free space is back above the
limit. Only touch things inside the root R, take the view lock when removing views, and compare real paths when R is reached
through a symlink."""
import fcntl
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduler code
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import relay_plugins as RN  # noqa: E402
from common import write_json  # noqa: E402

DIR = cfg.state("relay")
STATE = os.path.join(DIR, "state.json")


# ---------------- state file ----------------
def update_state(fn):
    os.makedirs(DIR, exist_ok=True)
    with open(STATE + ".lock", "a") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        st = load_state()
        out = fn(st)
        write_json(STATE, st)
    return out


def load_state():
    try:
        with open(STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def keys_of(st, name):
    """key → dict(recipe, params, inputs, staged, views, placed, outs, cleanup). inputs: the spec's inputs; placed: files placed
    outside views (relative to R/data); outs: output dirs."""
    return (st.get("keys") or {}).get(name) or {}


def paths_of(k):
    """Paths a task occupies over there (relative to R/data): views, inputs, outputs, cleanup."""
    return set(views_of(k)) | set(k.get("placed") or []) | set(k.get("outs") or []) | set(k.get("cleanup") or [])


def overlaps(p, used, anc):
    """True when p equals, contains or is contained by a path in used. anc: all ancestors of the paths in used."""
    if p in used or p in anc:
        return True
    parts = p.split("/")
    return any("/".join(parts[:i]) in used for i in range(1, len(parts)))


def ancestors(paths):
    return {"/".join(q.split("/")[:i]) for q in paths for i in range(1, len(q.split("/")))}


def droppable(keys, key, paths):
    """Paths in paths that can be removed when dropping key: keep ones still used by other tasks (same, parent or child)."""
    used = set().union(*[paths_of(k) for r, k in keys.items() if r != key]) if len(keys) > 1 else set()
    anc = ancestors(used)
    return sorted(p for p in paths if not overlaps(p, used, anc))


def views_of(k):
    """A task's view dirs."""
    return list(k.get("views") or [])


def safe_rel(rel):
    """A path relative to R/data: not absolute, no .., at least two levels (never remove the first level under data)."""
    r = os.path.normpath(rel)
    if r.startswith("/") or ".." in r.split("/") or len(r.split("/")) < 2:
        raise RN.RelayError(f"bad path: {rel}")
    return r


# ---------------- cleanup ----------------
def budget(name):
    return M.machine(name).get("budget")


def gc(name, keep_dirs, op, soft_gb=None, free=None, views=(), blob_free=None):
    """Cleanup for machine name → dict(freed, removed_views, removed_blobs, free); no budget → None.
    keep_dirs: views assigned to this machine that are needed or just built (relative to R/data). views: registered views
    (state.json views). op: relay_remote.op. free: remaining bytes (executors with cap pass relay.py's cap-based value; without
    it, look at the disk). blob_free: remaining on the blob disk when blobs are on another disk with blob_cap (computed by
    relay.py from blob_cap): removing blobs looks only at it; removing views looks at whichever side is below the soft limit;
    without it both use free. removed_views also includes registered views that are already gone (drop them from the registry)."""
    if soft_gb is None:
        b = budget(name)
        if not b:
            return None
        soft_gb = b[0]
    sys.path.insert(0, os.path.join(CORE, "sites"))
    import names as N
    return op(name, "gc", dict(soft_gb=soft_gb, keep=sorted(keep_dirs), free=free, views=sorted(views), scan=N.VIEW_ROOTS, blob_free=blob_free))
