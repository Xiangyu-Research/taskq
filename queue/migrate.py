"""taskq self migrate: bring on-disk state to the format this code reads and writes (proto.FORMAT), once per cutover.

    taskq self migrate [--list | --apply]

Every place records the format its state is in (proto.FORMAT_FILE): the hub state dir and each executor and store root; no
stamp is format 0. STEPS holds one numbered one-shot step per format: step N rewrites a place from format N-1 to N (its hub
part on the hub, its root part in each root) and then records N in that place's stamp, so no step runs twice. Steps up to
MIN_FORMAT are deleted once every place is past them; state that old is refused (migrate with an older taskq first). With no
step left (MIN_FORMAT == proto.FORMAT) every place in proto.FORMAT has nothing to do.

A format change bumps proto.FORMAT and, in the same commit, appends Step(proto.FORMAT, what, hub=..., root=...) to STEPS with
the functions it needs: hub(ctx, say, dry) runs on the hub (ctx: home, the state dir; queue, the hub queue dir), root(ctx, say,
dry) in each root (ctx: root; queue, an executor's queue dir or None; busy, the safe names of its claimed tasks; data, its data
dir and, on the store, the store's). Each only says what it would change when dry, finds nothing to do on state already
rewritten, and raises Stop when it cannot be done.

Without --apply (or with --list) it only lists: each place's format (roots read over ssh), the steps each still needs and
what the hub's steps would change. Nothing is written anywhere: no bundle built or pushed, no lock taken, no database opened
for writing. With --apply:
  1. hold the health check's round lock (no round runs meanwhile; the hub's clock should be stopped anyway);
  2. snapshot the hub queue dir, the hub database and the stamp into <state dir>/snapshots/<time>-format<N>/ (the newest
     proto.SNAPSHOT_KEEP stay);
  3. each root behind: push the new bundle beside the one in use and run `migrate.py root` from it there (relay_remote.op):
     an executor switches to it with its unclaimed tasks set aside for a moment (hold_and_switch; claimed tasks keep running
     under their workers), runs the root steps under the queue lock and records each format reached;
  4. the hub steps, each followed by the hub's stamp.
Every root is read and the hub steps are checked before anything changes: a root that cannot be reached, or a hub step that
cannot be done, stops the run with nothing changed. A run stopped later leaves each place at the last format it reached; run
it again to continue. Run again after a full run, it does nothing.

Until a place is stamped with this code's format, hub rounds refuse to run (taskq tick) and workers of this code claim
nothing there (common.format_problem); workers of older bundles never read the stamp. Cutover: stop the hub's clock, push the code,
(switch executors with taskq site bundle, or let this do it), taskq self migrate --apply, start the clock (taskq site setup).
"""
import argparse
import contextlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import proto  # noqa: E402
import common as C  # noqa: E402

safe = proto.safe
# Oldest format migrated from: the steps up to it are deleted (every place is past them).
MIN_FORMAT = 2


class Stop(Exception):
    """A situation that cannot be handled automatically: change nothing (more), list why."""


class Step:
    """Step to format `to` (from to - 1): hub(ctx, say, dry) on the hub, root(ctx, say, dry) in each root; either may be None.
    dry: say what would change and change nothing. A step that cannot be done raises Stop."""

    def __init__(self, to, what, hub=None, root=None):
        self.to, self.what, self.hub, self.root = to, what, hub, root


def pending(v, where="the state here"):
    """Steps a place in format v still needs, in order; v outside what this code migrates -> Stop."""
    if v is None:
        raise Stop(f"cannot read the format stamp of {where}")
    if v > proto.FORMAT:
        raise Stop(f"{where} is in format {v}, newer than this taskq (format {proto.FORMAT}): update taskq")
    if v < MIN_FORMAT:
        raise Stop(f"{where} is in format {v}: this taskq migrates from format {MIN_FORMAT} on (the older steps are deleted); "
                   "run self migrate of an older taskq first (git log queue/migrate.py)")
    return [s for s in STEPS if v < s.to]


# ---------------- the steps (each format change adds one; MIN_FORMAT moves up once every place is past a step) ----------------
STEPS = []
assert [s.to for s in STEPS] == list(range(MIN_FORMAT + 1, proto.FORMAT + 1)), "one step per format from MIN_FORMAT + 1 to proto.FORMAT"


# ---------------- in a root (inside the new bundle, run through relay_remote.op) ----------------
def this_bundle():
    """The bundle dir this script runs from (<root>/code/<bundle>)."""
    core = os.path.dirname(HERE)
    if not core.endswith("/" + proto.BUNDLE_CORE):
        raise Stop(f"{HERE} is not inside a bundle")
    return core[:-len(proto.BUNDLE_CORE) - 1]


def claimed(Q):
    """Safe names of the tasks claimed in queue dir Q."""
    d = os.path.join(Q, proto.QUEUE["claims"])
    return set(os.listdir(d)) if os.path.isdir(d) else set()


def hold_and_switch(R, Q, name, say):
    """Switch the root's bundle to this one without stopping anything: under the queue lock set the unclaimed tasks aside
    (queue held file: nobody can claim them), record the machine name and switch code/current; then wait SWITCH_HOLD_S so a
    worker of the old bundle that looked at the bundle just before cannot claim a task the steps rewrite (once it sees the
    switch it hands its running tasks to the new bundle before claiming). Already switched more than SWITCH_HOLD_S ago (taskq
    site bundle) -> nothing to do. Running tasks stay claimed and keep running. The held tasks go back in put_back."""
    import taskq
    b = this_bundle()
    if os.path.realpath(os.path.dirname(b)) != os.path.realpath(os.path.join(R, proto.ROOT["code"])):
        raise Stop(f"bundle {b} is not under {R}/{proto.ROOT['code']}")
    tp, hp = os.path.join(Q, proto.QUEUE["tasks"]), os.path.join(Q, proto.QUEUE["held"])
    cur = os.path.join(R, proto.ROOT["current"])
    hold = float(os.environ.get("TASKQ_SWITCH_HOLD") or proto.SWITCH_HOLD_S)
    with taskq.Lock():
        if os.path.realpath(cur) == os.path.realpath(b) and not os.path.exists(hp) and time.time() - os.lstat(cur).st_mtime >= hold:
            return
        if not os.path.exists(hp):  # an interrupted earlier run already set them aside
            busy = claimed(Q)
            tasks = taskq.read_tasks(tp)
            taskq.write_tasks([t for t in tasks if safe(t["key"]) not in busy], hp)
            taskq.write_tasks([t for t in tasks if safe(t["key"]) in busy], tp)
        C.atomic_write(os.path.join(R, proto.ROOT["machine"]), name + "\n")
        tmp = f"{cur}.tmp.{os.getpid()}"
        if os.path.lexists(tmp):
            os.remove(tmp)
        os.symlink(os.path.basename(b), tmp)
        os.replace(tmp, cur)
    say(f"bundle switched to {os.path.basename(b)}; {len(taskq.read_tasks(hp))} unclaimed tasks set aside for {hold:g} s")
    time.sleep(hold)


def put_back(Q):
    """Held tasks back into the queue (a task of the same name already there wins: released meanwhile). Call under the queue lock."""
    import taskq
    tp, hp = os.path.join(Q, proto.QUEUE["tasks"]), os.path.join(Q, proto.QUEUE["held"])
    if not os.path.exists(hp):
        return
    tasks = taskq.read_tasks(tp)
    have = {t["key"] for t in tasks}
    taskq.write_tasks(tasks + [t for t in taskq.read_tasks(hp) if t["key"] not in have], tp)
    os.remove(hp)


def op_root(A):
    """On this machine (TASKQ_ROOT is its root), from the new bundle: the root steps its stamp still needs, each followed by the
    stamp. A = dict(data: [more data dirs whose part logs belong here: the store's]) -> dict(log, was: format before, now)."""
    import cfg
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
    import mtab as M
    R = os.path.expanduser(os.environ["TASKQ_ROOT"]).rstrip("/")
    p = C.format_path()
    was = C.format_on_disk(p)
    steps = pending(was, R)
    log = []
    if not steps:
        return dict(log=[f"already in format {was}"], was=was, now=was)
    me = M.whoami()
    Q = cfg.state("taskq") if M.is_exec(me) else None
    if Q:
        import taskq
        hold_and_switch(R, Q, me, log.append)
    with taskq.Lock() if Q else contextlib.nullcontext():
        if Q:
            put_back(Q)
        ctx = dict(root=R, queue=Q, busy=claimed(Q) if Q else set(),
                   data=[os.path.join(R, proto.ROOT["data"])] + [os.path.expanduser(x) for x in A.get("data") or []])
        for s in steps:
            if s.root:
                s.root(ctx, log.append, False)
            C.write_format(s.to, p)
            log.append(f"format {s.to}")
    if ctx["busy"]:
        log.append(f"{len(ctx['busy'])} claimed task(s) keep running under their workers: " + ", ".join(sorted(ctx["busy"])[:8]))
    return dict(log=log, was=was, now=steps[-1].to)


def root_main(argv):
    if argv != ["root"]:
        sys.exit("usage: migrate.py root (parameters are JSON on stdin)")
    out, sys.stdout = sys.stdout, sys.stderr
    try:
        res = op_root(json.load(sys.stdin))
    except Stop as e:
        res = dict(stop=str(e))
    finally:
        sys.stdout = out
    print(proto.JSON_MARK + json.dumps(res, ensure_ascii=False), flush=True)


# ---------------- on the hub ----------------
def read_root(name):
    """That machine's root format, read over ssh -> int (0: no stamp), None (no root there yet: relay makes it, stamped), or a
    str saying why it cannot be read."""
    import relay_remote as RM
    import remote as R
    r = R.q(RM.root(name))
    f = f"{r}/{proto.ROOT['format']}"
    rc, out, err = RM.rsh(name, f"if [ -e {f} ]; then cat {f}; elif [ -d {r}/{proto.ROOT['queue']} ] || [ -d {r}/{proto.ROOT['code']} ]; "
                                "then echo 0; else echo -; fi", timeout=60)
    s = out.strip()
    if rc:
        return f"cannot read its root ({RM.err_tail(err or out)})"
    if s == "-":
        return None
    try:
        return int(s)
    except ValueError:
        return f"unreadable format stamp {f} ({s[:40]!r})"


def round_lock():
    """Hold the health check's round lock (monitor/health.py round_lock) until this process ends -> the open file; a round is
    running -> Stop."""
    import fcntl
    import cfg
    import policy
    d = os.path.expanduser(getattr(policy, "OUTPUTS", {}).get("health") or cfg.state("health", expand=False))
    os.makedirs(d, exist_ok=True)
    f = open(os.path.join(d, proto.TICK + ".lock"), "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        f.close()
        raise Stop("a health check round is running (stop the hub's clock, wait for it to end) - nothing changed") from None
    return f


def snapshot(home, v, say):
    """Copy the hub queue dir, the hub database (sqlite backup: consistent) and the stamp to <home>/snapshots/<time>-format<v>/
    (written under a temp name, renamed when complete); keep the newest proto.SNAPSHOT_KEEP -> its path."""
    import db as D
    import taskq
    base = os.path.join(home, proto.SNAPSHOTS)
    to = os.path.join(base, f"{time.strftime('%Y%m%d-%H%M%S')}-format{v}")
    while os.path.lexists(to):
        to += "+"
    tmp = C.tmp_of(to)
    os.makedirs(tmp)
    if os.path.isdir(taskq.TQ):
        shutil.copytree(taskq.TQ, os.path.join(tmp, os.path.basename(taskq.TQ)), symlinks=True)
    p = D.path()
    if os.path.exists(p):  # an ordinary connection: the last one to close removes the -wal / -shm files, a read-only one cannot
        src, dst = sqlite3.connect(p), sqlite3.connect(os.path.join(tmp, os.path.basename(p)))
        try:
            src.backup(dst)
        finally:
            src.close()
            dst.close()
    if os.path.exists(C.format_path()):
        shutil.copy2(C.format_path(), os.path.join(tmp, proto.FORMAT_FILE))
    os.rename(tmp, to)
    def age(x):  # by the time in the name, then mtime: two in the same second must not sort by their format label
        return os.path.basename(x)[:15], os.stat(x).st_mtime_ns, x
    old = sorted((os.path.join(base, f) for f in os.listdir(base) if not f.endswith(proto.TMP)), key=age)
    for x in old[:-proto.SNAPSHOT_KEEP] + [os.path.join(base, f) for f in os.listdir(base) if f.endswith(proto.TMP)]:
        shutil.rmtree(x, ignore_errors=True)
    say(f"snapshot of the hub queue, database and stamp: {to} (to undo: stop, copy them back, run the older taskq)")
    return to


def main(argv):
    if argv[:1] == ["root"]:
        return root_main(argv)
    ap = argparse.ArgumentParser(prog="taskq self migrate", description=__doc__.split("\n")[0])
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--list", action="store_true", help="only list what is pending and what it would change (the default)")
    g.add_argument("--apply", action="store_true", help="run the pending steps (snapshot of the hub state first)")
    a = ap.parse_args(argv)
    import cfg
    cfg.use_config()
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
    import mtab as M
    import relay_remote as RM
    import taskq
    say = print
    home = cfg.state().rstrip("/")
    ctx = dict(home=home, queue=taskq.TQ)
    roots = [n for n in M.MACHINES if M.MACHINES[n].get("root")]
    say(f"this taskq: on-disk format {proto.FORMAT} (migrates from format {MIN_FORMAT} on)")
    problems = []
    hub_v = C.format_on_disk()
    try:
        hub_steps = pending(hub_v, f"the hub state dir {home}")
    except Stop as e:
        hub_steps, _ = [], problems.append(str(e))
    say(f"hub {home}: " + (problems[-1] if hub_v is None or not hub_steps and problems else
                           f"format {hub_v}" + (" (no stamp)" if hub_v == 0 else "") + ("" if hub_steps else ", nothing to do")))
    for s in hub_steps:
        say(f"  step {s.to}: {s.what}")
        if s.hub:
            try:
                s.hub(ctx, lambda x: say("    " + x.replace("\n", "\n    ")), True)  # what it would change as the state is now
            except Stop as e:
                problems.append(f"hub step {s.to}: {e}")
                say(f"    cannot be done: {e}")
    todo = []
    for n in roots:
        v, label = read_root(n), RM.conf(n)["label"]
        if isinstance(v, str):
            problems.append(f"{label}: {v}")
            say(f"{label}: {v}")
            continue
        if v is None:
            say(f"{label}: no root there yet (relay makes it in format {proto.FORMAT})")
            continue
        try:
            steps = pending(v, f"the root of {label}")
        except Stop as e:
            problems.append(str(e))
            say(f"{label}: {e}")
            continue
        say(f"{label}: format {v}" + (" (no stamp)" if v == 0 else "") + ("" if steps else ", nothing to do"))
        for s in steps:
            say(f"  step {s.to}: " + ("its root part, after switching to the new bundle" if s.root else "the stamp only"))
        if steps:
            todo.append(n)
    try:
        if not a.apply:
            say(("cannot apply yet: " + "; ".join(problems) + "\n" if problems else "") + "(listed only, nothing changed; add --apply when ready)")
            return 0
        if problems:
            raise Stop("; ".join(problems) + " - nothing changed")
        if not hub_steps and not todo:
            say(f"everything is in format {proto.FORMAT} already; nothing to do")
            return 0
        lock = round_lock()  # noqa: F841  held until this process ends
        snapshot(home, hub_v, say)
        if todo:
            import relay as RL
            try:
                bid, path = RL.wanted_bundle()
            except Exception as e:  # noqa: BLE001  (relay_bundle.BundleError, git errors)
                raise Stop(f"cannot build the bundle for the roots: {type(e).__name__}: {e} (no root or hub state changed)") from None
            for n in todo:
                done = "places before this one are migrated and stamped; fix this and run again"
                try:
                    RL.deploy(n, bid, path, switch=False)  # beside the one in use: workers do not see it yet
                except (RuntimeError, OSError, subprocess.SubprocessError) as e:
                    raise Stop(f"cannot push bundle {bid} to {n}: {e} ({done})")
                try:
                    res = RM.op(n, "root", dict(data=[M.data()] if n == M.store() else []), timeout=3600, script="migrate.py", bundle=bid)
                except RuntimeError as e:
                    raise Stop(f"{e} (check {n} is reachable; {done})")
                if res.get("stop"):
                    raise Stop(f"{RM.conf(n)['label']}: {res['stop']} ({done})")
                say(f"== {RM.conf(n)['label']}: format {res['was']} -> {res['now']}" + "".join("\n  " + ln for ln in res["log"]))
        for s in hub_steps:
            if s.hub:
                s.hub(ctx, say, False)
            C.write_format(s.to)
            say(f"hub: format {s.to}")
    except Stop as e:
        print(f"taskq self migrate: {e}", file=sys.stderr)
        return 1
    say(f"migration complete, every place in format {proto.FORMAT} ({time.strftime('%F %T')}); start the clock again (taskq site setup)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
