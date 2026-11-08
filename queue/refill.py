#!/usr/bin/env python3
"""How tasks are sent to executors (choose, Site) and how Slurm workers are topped up (topup_plan, cpu_plan, slurm_plan);
queue/relay.py calls this each round. The rules are described in queue/README.md; policy comes from policy.py (PLACE, SLURM).
"""
import os
import re
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduling code
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
from proto import S_CLAIMED, UNCLAIMED  # noqa: E402
cfg.use_config()
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import slurm as SL  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402

NOROOM = "no room"  # Site.fit reasons start with it when space is short (choose uses it to fill noroom)

_ENTRY = cfg.outputs("entry")  # main loop entry point (relative to the hub repo; policy.py OUTPUTS)
KICK = _ENTRY and (_ENTRY if _ENTRY.startswith(("/", "~")) else M.MACHINES[M.hub()]["repo"] + "/" + _ENTRY)  # kick and the timer share one lock; wait at most 5 minutes
LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")


def info(tq, key):
    """A queued task -> dict(recipe, params, spec) (spec as parsed by taskq.py; {} when it cannot be parsed)."""
    t = next((t for t in (tq or {}).get("tasks", []) if t["key"] == key), {})
    return dict(recipe=t.get("recipe"), params=t.get("params") or {}, spec=t.get("spec") or {})


def kind_of(sp):
    return sp.get("device") or "gpu"


def out_gb(sp):
    """GB to reserve for one task's output: the spec's out_gb, else proto.OUT_GB."""
    v = sp.get("out_gb")
    return float(v) if v is not None else float(proto.OUT_GB)


def places(recipe, sp):
    """Executors this task may go to, in preference order: PLACE[step name or recipe] (missing or "any" = executors with
    this worker kind, in machine-table order), intersected with the spec's machines (None = no limit)."""
    have = [n for n in M.executors() if kind_of(sp) in M.kinds(n)]
    p = (P.PLACE or {}).get(sp.get("step") or recipe, "any")
    out = have if p == "any" else [n for n in p if n in have]
    lim = sp.get("machines")
    return [n for n in out if lim is None or n in lim]


def progress(s, key, host=None):
    """Whether the collected state on the store shows progress for this task (done, some epochs, or a checkpoint: the
    spec's resume). host=None looks at every machine collected. A task whose files could not be read (recorded in stuck
    while collecting, the store hung) or failed to read (errors) counts as having progress: do not hand it out as new."""
    for h, d in (s.get("src") or {}).items():
        if host not in (None, h):
            continue
        if key in (d.get("stuck") or []) or key in (d.get("errors") or {}):
            return True
        f = (d.get("tasks") or {}).get(key)
        if f and (f.get("final") or f.get("resumable") or any((r.get("epochs_done") or 0) > 0 for r in f.get("runs") or [])):
            return True
    return False


def site_progress(f):
    """One task's state on an executor (relay.site_view / status.json tasks[key]) -> whether it has progress: done,
    finished epochs, a checkpoint (the spec's resume), or that side says it ran (ran: ran, released, did not fail,
    taskq.has_progress)."""
    return bool(f) and bool(f.get("final") or f.get("resumable") or f.get("ran")
                            or any((r.get("epochs_done") or 0) > 0 for r in f.get("runs") or []))


def holders(views, usable):
    """Per-executor task views {executor: {key: site_view item}} -> {key: [executors with progress that are usable now]}
    (usable(name): is an executor, not paused)."""
    out = {}
    for name, tasks in views.items():
        if usable(name):
            for key, f in (tasks or {}).items():
                if site_progress(f):
                    out.setdefault(key, []).append(name)
    return out


class Site:
    """One executor's remaining room for this round (relay.py builds it from that side's state).
    room {kind: free slots} (nice < 0 does not take one); ready: inputs already there (relative to the data dir);
    envs: built envs; root, blob: (disk id, GB free) for the root (outputs) and blobs (inputs), the same id when on
    one disk, and executors on one disk share the budget; root's free space already has each task's out_gb deducted
    and the outputs it has written added back; keep: GB that must stay free after inputs and outputs (the second
    item of machines.py budget); block(key, spec) -> why this executor will not take it this round (bundle cannot
    run it, smoke gate); took(key, spec): tell the gate it was taken; idle {kind: [dict(key, nice, out=its out_gb)]}:
    prepared but unclaimed tasks with no progress, the one most worth yielding first; when full, yield one per kind
    to make room for a more important task."""

    def __init__(self, name, room, ready=(), envs=(), root=(None, 0.0), blob=None, keep=0, block=None, took=None, idle=None):
        self.name, self.room, self.ready, self.envs = name, dict(room), set(ready), set(envs)
        self.root, self.blob, self.keep = root, blob or root, keep
        self.block, self._took, self.idle = block, took, {k: list(v) for k, v in (idle or {}).items()}
        self.take, self.swaps = [], []  # tasks taken this round; [(yielded, swapped in)]

    def fit(self, t, sp, sizes, planned):
        """Can t go to this executor -> None (yes, and record it) or why not. sizes {input: GB or None}; planned {disk:
        GB already planned this round}; space reserved for yielded tasks is added back."""
        k = kind_of(sp)
        if k not in self.room:
            return f"no {k} worker"
        envs = S.envs_of(sp)
        if not envs or not set(envs) <= self.envs:
            return f"env {', '.join(sorted(set(envs) - self.envs)) or '(not set)'} not ready"
        swap = None
        if t["nice"] >= 0 and self.room[k] <= 0:
            swap = next((x for x in self.idle.get(k, []) if x["nice"] > t["nice"]), None)
            if swap is None:
                return "full"
        send = [p for p in sp.get("inputs") or [] if p not in self.ready]
        if any(sizes.get(p) is None for p in send):
            return f"input not on the store: {next(p for p in send if sizes.get(p) is None)}"
        gb, out = sum(sizes[p] for p in send), out_gb(sp)
        back = swap["out"] if swap else 0.0
        same = self.blob[0] == self.root[0]
        left = self.root[1] - planned.get(self.root[0], 0.0) + back - out - (gb if same else 0.0)
        bleft = left if same else self.blob[1] - planned.get(self.blob[0], 0.0) - gb
        if min(left, bleft) < self.keep:
            return (f"{NOROOM} (after {gb:.0f} GB input and {out:.0f} GB output, {min(left, bleft) + self.keep:.0f} GB left, "
                    f"need to keep {self.keep:.0f} GB)")
        why = self.block(t["key"], sp) if self.block else None
        if why:
            return why
        planned[self.root[0]] = planned.get(self.root[0], 0.0) - back + out + (gb if same else 0.0)
        if not same:
            planned[self.blob[0]] = planned.get(self.blob[0], 0.0) + gb
        if swap:
            self.idle[k] = []  # one swap per kind per round
            self.swaps.append((swap, t))
        elif t["nice"] >= 0:
            self.room[k] -= 1
        self.ready |= set(send)
        self.take.append(t)
        if self._took:
            self._took(t["key"], sp)
        return None


def choose(tasks, sites, specs, held=None, sizes=None):
    """Tasks in the queue that can be sent (unclaimed, parseable, accepted by the plugin) -> pick an executor for each
    and record it in sites[name].take / swaps. specs {key: (recipe, spec)}; held: holders() result; sizes: {input: GB
    or None} (measured on the store, data_sizes). Order: nice, then half-done within the same nice, then queue order.
    -> dict(why={key: why it was not sent this round}, nowhere=[keys whose every candidate skipped it],
    noroom=[keys that fit on none of this round's available executors] (not full: waiting will not help, free space or
    lower out_gb))."""
    held, sizes, planned, why, nowhere, noroom = held or {}, sizes or {}, {}, {}, [], []
    for t in sorted(tasks, key=lambda t: (t["nice"], t["key"] not in held, t["order"])):
        rec, sp = specs[t["key"]]
        opts = places(rec, sp)
        if not opts:
            why[t["key"]] = f"no executor can take it (PLACE, machines and {kind_of(sp)} workers do not intersect)"
            continue
        skipped = set(t.get("skipped_by") or [])
        if set(opts) <= skipped:
            nowhere.append(t["key"])
            continue
        opts = [n for n in opts if n not in skipped]
        hs = held.get(t["key"])
        if hs:  # half-done goes only to the executor with progress (when it is paused and no longer an executor, holders drops it and it may start over elsewhere)
            opts = [n for n in opts if n in hs]
        live = [n for n in opts if n in sites]
        if not live:
            why[t["key"]] = f"candidates {', '.join(opts) or ', '.join(hs or [])} are all unavailable this round (unreachable or off)"
            continue
        data = sp.get("inputs") or []
        live.sort(key=lambda n: not all(p in sites[n].ready for p in data))  # inputs already there first; the rest by preference
        fails = []
        for n in live:
            r = sites[n].fit(t, sp, sizes, planned)
            if r is None:
                break
            fails.append(f"{n}: {r}")
        else:
            why[t["key"]] = "; ".join(fails)
            if all(f.split(": ", 1)[1].startswith(NOROOM) for f in fails):
                noroom.append(t["key"])
    return dict(why=why, nowhere=nowhere, noroom=noroom)


def data_sizes(paths, probe):
    """Inputs (relative to the data dir, may contain wildcards) on the store -> {path: GB or None (missing, wildcard
    matches nothing)}. probe(pats) -> site_ops probe result on the store (with size; symlinks count by content, hard
    links once, as after transfer)."""
    paths = sorted(set(paths))
    if not paths:
        return {}
    got = probe(paths)
    return {p: (got[p]["size"] / 1e9 if got.get(p) and got[p]["n"] and got[p]["size"] is not None else None) for p in paths}


# ---------------- running ----------------
def sh(cmd, host=None, timeout=300):
    """Run a shell command on the hub (host=None) or another machine -> (exit code, output)."""
    rc, out, err = C.run(cmd, host, timeout)
    return rc, (out + err).strip()


def q(name, args, run=sh):
    """Add a task with queue/q on a pueue machine -> job id."""
    m = M.MACHINES[name]
    qp = shlex.quote(cfg.local("queue", "q")) if m["ssh"] is None else cfg.rel("queue", "q")  # local: absolute path in the core
    rc, out = run(f"cd {m['repo']} && {qp} {args}", m["ssh"])
    # with -p it prints only the job id, without it "New task added (id 118)." (accept both)
    hit = re.fullmatch(r"(?:New task added \(id )?(\d+)\)?\.?", out.strip().splitlines()[-1].strip()) if out.strip() else None
    if rc or not hit:
        raise RuntimeError(f"failed to add a task on {name}: {out[-200:]}")
    return int(hit.group(1))


def add(name, label, step, key, args, run=sh, **rec):
    """Record the task first (queue/common.py), then add it on that machine -> job id."""
    r = C.new_record(name, label, step, key, **rec)
    tid = q(name, args, run)
    C.set_record_id(r, tid)
    return tid


def then_check(cmd, label):
    """Command for one of the core's own pueue jobs (transfer data, fetch, archive) -> run a health check right after
    it finishes (successfully), without waiting for the next round; it shares the timer's lock, and this task counts
    as done in that check (health.py reads TASKQ_TASK_DONE). A check error does not fail the task. Without an entry
    (OUTPUTS["entry"]) the command is returned as is."""
    return f"{cmd}{KICK_SEP}{shlex.quote(label)} bash {KICK} kick || true; }}" if KICK else cmd


KICK_SEP = " && { TASKQ_TASK_DONE="


# ---------------- Slurm top-up ----------------
def unclaimed(tq, device):
    """Number of unclaimed (free / stale) tasks in the queue whose device is this one."""
    return sum(1 for t in tq["tasks"] if t["state"] in UNCLAIMED and info(tq, t["key"])["spec"].get("device", "gpu") == device)


def training(tq):
    """Number of claimed (running) tasks in the queue that need a GPU."""
    return sum(1 for t in tq["tasks"] if t["state"] == S_CLAIMED and info(tq, t["key"])["spec"].get("device", "gpu") == "gpu")


def topup_plan(tq, counts, qos):
    """-> ([(QOS, time limit per part, parts to add)], why not). counts: {QOS: (queue cap or None, current parts, parts
    still queued)}. When this machine has GPU tasks (prepared but unclaimed, or running), fill each QOS quota so queued
    jobs keep accumulating queue time. How many run at once is up to the cluster QOS; extra jobs wait for running ones
    to finish, by which time that side has READY tasks prepared; a worker that starts with no task waits IDLE_MAX and
    ends (the slurm plugin in consume.py). If neither exists, add nothing."""
    if not tq:
        return [], "cannot read the task queue"
    if not tq.get("enabled"):
        return [], "task queue is disabled"
    if tq.get("paused"):
        return [], "task queue paused after too many failures"
    if not unclaimed(tq, "gpu") and not training(tq):
        return [], "no GPU tasks in the queue (none prepared, none running)"
    if any(q not in counts for q in qos):
        return [], f"cannot read the job count for {next(q for q in qos if q not in counts)}"
    out = SL.distribute(qos, counts, sum((counts[q][0] or SL.SUBMIT_CAP) for q in qos), "gpu")
    return out, "" if out else "all QOS full"


def cpu_plan(tq, have, conf, free=None):
    """Worker jobs for CPU-only tasks -> ([(QOS, time limit, how many, "cpu")], why not). have: our queued and running
    CPU worker jobs; free: submit slots left (None = cannot read, add none)."""
    q, t, most = conf
    if not tq or not tq.get("enabled") or tq.get("paused"):
        return [], "task queue disabled or paused"
    if have is None or free is None:
        return [], "cannot read job counts"
    if not unclaimed(tq, "cpu"):
        return [], "no unclaimed CPU-only tasks"
    n = min(min(unclaimed(tq, "cpu"), most) - have, free)
    return ([(q, t, n, "cpu")], "") if n > 0 else ([], f"{have} already" if free > 0 else "no submit slots left")


def slurm_plan(tq, st, conf):
    """Worker jobs a Slurm machine should add -> ([(QOS, time limit, how many, worker kind)], why not). st: sites/slurm.py
    status; conf: policy.py SLURM[machine] (gpu tops up GPU workers, cpu tops up CPU workers)."""
    subs, why = topup_plan(tq, st["counts"], conf["gpu"]) if conf.get("gpu") else ([], "")
    if conf.get("cpu"):
        csub, cwhy = cpu_plan(tq, st["cpu"], conf["cpu"], st["free"])
        subs += csub
        why = why if csub else "; ".join(x for x in (why, f"CPU: {cwhy}") if x)
    return subs, why
