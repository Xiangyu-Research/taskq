"""Executor types: how a machine with `scheduler` set in the table runs workers. TYPES maps each scheduler value to the module
that implements it: built in are pueue (sites/pueue.py, resident workers) and slurm (sites/slurm.py, worker jobs); a project adds
types through a plugin executors.py exporting EXECUTORS = {type: module or namespace} (cfg.KINDS; a built-in name is an error).
The core reaches a type only through of(name) and get(type). Standard library only: site_ops.py loads it on the executor.

Every type provides the names in INTERFACE (run: fn(command) -> (exit code, output), run on that machine; m: its table entry):
  RESIDENT                     True when its workers are resident, one per workers entry of the table (slots, hours windows);
                               only such types may write workers
  check(name, m)               -> [problem]: what this type needs in the entry as written (taskq check)
  workers(name, m)             -> [dict(name, id, kind, group, gpu, cpus, slots, hours, idle, wait)] (mtab.workers; m with defaults)
  groups(name, ws)             -> {pueue group: (parallel, gpu, {env})} it runs on that machine, given its workers (mtab.groups)
  status_args(name)            -> {arg: value} the hub adds to site_ops status's args
  read_status(A)               -> raw scheduler output or None; runs on the executor inside site_ops status (A: its args)
  parse_status(name, raw)      -> {field: value} merged into the remote status (relay.parse_sched; raw is never None)
  plan(name, rs, run)          -> (subs, why none, {problem id: alert}): workers to start this round, the only place that decides
                               how many (relay.topup); the alerts are for earlier workers that failed
  describe(sub)                -> one sub in words (taskq plan)
  start(name, subs, run)       -> (messages, {problem id: alert}): start subs (relay.submit)
  bundles(cur, live)           -> bundles in use a sent spec must run on: cur is code/current, live those touched by running
                               workers (bundle_gate.in_use)
  env_launch(name, m, minutes) -> command prefix that runs an env build on a CPU there, or None for the local form (nice, no
                               GPUs, minutes limit); a bad entry raises ValueError (envs.runner)
  worker_cmd(name, w)          -> the command that starts worker w, or None (taskq machines show)
"""
import importlib
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import cfg  # noqa: E402

BUILTIN = {"pueue": "pueue", "slurm": "slurm"}  # type -> module in sites/, imported when first used
INTERFACE = ("RESIDENT", "check", "workers", "groups", "status_args", "read_status", "parse_status", "plan", "describe", "start",
             "bundles", "env_launch", "worker_cmd")
TYPES = {}


def types():
    """{type: module (or a built-in's module name until it is used)}: built-ins, then plugins' EXECUTORS; loaded once."""
    if not TYPES:
        ns = {"EXECUTORS": dict(BUILTIN)}
        cfg.extend(ns, "executors")
        TYPES.update({**BUILTIN, **ns["EXECUTORS"]})
    return TYPES


def get(t):
    """The module of type t; None or not registered -> None."""
    v = types().get(t) if t is not None else None
    if isinstance(v, str):
        v = TYPES[t] = importlib.import_module(v)
    return v


def of(name):
    """The type module of machine name (by its scheduler); no scheduler -> None, an unregistered one -> LookupError."""
    t = cfg.mtab().MACHINES[name].get("scheduler")
    if t is not None and t not in types():
        raise LookupError(f"{name}: scheduler {t!r} is not an executor type ({', '.join(types())})")
    return get(t)


def missing(mod):
    """Names of INTERFACE that mod lacks."""
    return [k for k in INTERFACE if not hasattr(mod, k)]


def resident():
    """Types whose workers are resident (RESIDENT)."""
    return [t for t in types() if getattr(get(t), "RESIDENT", False)]
