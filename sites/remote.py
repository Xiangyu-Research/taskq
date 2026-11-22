"""Small helpers shared by executor adapters: one machine from the table, how many tasks to keep ready per machine,
how to quote a root path for the shell, and worker kinds in messages and problem ids."""
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of this scheduler code
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg  # noqa: E402
cfg.use_config()
import proto  # noqa: E402
import mtab as M  # noqa: E402
import policy as P  # noqa: E402


def conf(name):
    """One machine from the table (mtab.machine, defaults filled in)."""
    return M.machine(name)


def ready(name):
    """How many unclaimed tasks to keep ready on the executor (per worker kind): policy.py's READY, default 1."""
    return P.READY.get(name, 1)


def q(path):
    """Shell form of a path: a leading ~ is left for the other side to expand (roots carry no user name), the rest is quoted."""
    if path == "~":
        return "~"
    if path.startswith("~/"):
        return "~/" + shlex.quote(path[2:])
    return shlex.quote(path)


def kname(kind):
    return "" if kind == "gpu" else f"{kind.upper()} "


def wname(kind):
    return f"{kname(kind)}worker"


def kkey(kind, *args, wkind="gpu"):
    """Problem id: unchanged for GPU, other kinds get :<kind> appended."""
    return proto.alert_id(kind, *args, None if wkind == "gpu" else wkind)
