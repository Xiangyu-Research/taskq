"""taskq self migrate: bring on-disk state to the format this code reads and writes (proto.FORMAT), once per cutover.

    taskq self migrate [--list | --apply]

Every place records the format its state is in (proto.FORMAT_FILE): the hub state dir and each executor and store root; no
stamp is format 0. STEPS holds one numbered one-shot step per format: step N rewrites a place from format N-1 to N (its hub
part on the hub, its root part in each root) and then records N in that place's stamp, so no step runs twice. Steps up to
MIN_FORMAT are deleted once every place is past them; state that old is refused (migrate with an older taskq first).

Without --apply (or with --list) it only lists: each place's format (roots read over ssh), the steps each still needs and
what the hub's steps would change. Nothing is written anywhere: no bundle built or pushed, no lock taken, no database opened
for writing. With --apply:
  1. hold the health check's round lock (no round runs meanwhile; cron should be stopped anyway);
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
nothing there (common.format_problem); workers of older bundles never read the stamp. Cutover: stop cron, push the code,
(switch executors with taskq site bundle, or let this do it), taskq self migrate --apply, start cron.
"""
import argparse
import contextlib
import glob
import json
import os
import re
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
# Oldest format migrated from: the steps up to it are deleted. 0 is the state the 2026-11-14 cutover (the last run of the
# unnumbered migrate) left, with what came after it but before stamps.
MIN_FORMAT = 0


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


# ---------------- step 1: what the unnumbered migrate left, and the hub bookkeeping into the database ----------------
# Old part log text from before the 2026-11-08 translation. Workers of bundles from before the 2026-11-14 cutover went on
# writing it until they switched, and logs of tasks claimed during the old migrate's runs were skipped; only this step reads it.
OLD_PART = "_seg"  # old part log name <safe key>__<job>_seg<part>.log
OLD_TS = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
OLD_HEAD = re.compile(r"^作业 (\S+) @ ([^，]+)，任务=([^，]*)，第 (\d+) 段，环境 ([^，]*)，代码 ([^，]*)，" + OLD_TS + r"$")
OLD_ENDED = re.compile(r"^" + OLD_TS + r" 命令结束（退出码 (-?\d+)），(.*)$")
OLD_NOT_STARTED = re.compile(r"^" + OLD_TS + r" 没开训：(.*)$")
OLD_GPU_GIVEUP_SEEN = re.compile(r"^" + OLD_TS + r" 卡 (\S+) 被别人占着（空着，别人 (.+?) 还在用，要空满 (\d+) 分钟），等了 (\d+) 分钟还没空出来，不跑$")
OLD_GPU_GIVEUP = re.compile(r"^" + OLD_TS + r" 卡 (\S+) 被别人占着，等了 (\d+) 分钟还没空出来，不跑$")
OLD_GPU_WAIT_SEEN = re.compile(r"^" + OLD_TS + r" 卡 (\S+) 空着，别人 (.+?) 还在用，空满 (\d+) 分钟再跑（已等 (\d+) 分钟）$")
OLD_GPU_WAIT = re.compile(r"^" + OLD_TS + r" 卡 (\S+) 被别人占着，等它空出来（已等 (\d+) 分钟(?:，最多 (\d+) 分钟)?）$")
OLD_STALL_MSG = re.compile(r"^( \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})：日志和结果文件夹 (\d+) 分钟没有变化，作业 (\S+) 停掉它$")
OLD_FAIL_JOB = "作业 "
# hub bookkeeping files from before it moved into the hub database (hubstate.py, 2026-11-15), under the state dir
OLD_RELAY_STATE = os.path.join("relay", "state.json")
OLD_LINKSETS = os.path.join("relay", "linksets")  # <id>.json per link set
OLD_SITES = os.path.join("relay", "status.json")
OLD_RECORDS = "tasks.json"
OLD_SNAPSHOTS = "code"  # <state dir>/code/<commit>: code snapshots of the old queue/q (old_snapshots)


def log_rename(name):
    """Old part log name <safe key>__<job>_seg<part>.log -> <safe key>__<job>_part<part>.log; None when it is not one."""
    i = name.rfind(OLD_PART)
    rest = name[i + len(OLD_PART):] if i >= 0 else ""
    if rest.endswith(".log") and rest[:-4].isdigit():
        return name[:i] + "_" + proto.LOG_PART + rest
    return None


def move(src, dst, say, dry):
    """src -> dst (dst exists -> leave it alone and say so)."""
    if os.path.lexists(dst):
        say(f"  kept {src} ({dst} exists)")
        return False
    if not dry:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        os.rename(src, dst)
    return True


def open_log(f, busy):
    """Is part log file f one of a claimed task (busy: safe names)? (A safe name may itself contain proto.SAFE: a longer task
    name sharing the prefix is skipped too.)"""
    return any(f.startswith(b + proto.SAFE) for b in busy)


def claimed(Q):
    """Safe names of the tasks claimed in queue dir Q."""
    d = os.path.join(Q, proto.QUEUE["claims"])
    return set(os.listdir(d)) if os.path.isdir(d) else set()


def rename_logs(D, say, dry, busy=()):
    """Rename old part log names (log_rename); logs of claimed tasks (busy: safe names) stay, they may still be written."""
    d = os.path.join(D, proto.TASK_LOGS)
    n = 0
    for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        nf = None if open_log(f, busy) else log_rename(f)
        if nf:
            n += move(os.path.join(d, f), os.path.join(d, nf), say, dry)
    if n:
        say(f"{d}: renamed {n} part logs (_{OLD_PART[1:]}<part> -> _{proto.LOG_PART}<part>)")


def english_line(ln):
    """One part log line in the old Chinese format -> English; anything else unchanged (line ending kept)."""
    s = ln.rstrip("\r\n")
    eol = ln[len(s):]
    m = OLD_HEAD.match(s)
    if m:
        job, host, key, part, env, code, ts = m.groups()
        return proto.LOG_HEAD_FMT % (job, host, key, part, env, code, ts) + eol
    if s.startswith("训完"):
        return proto.DONE_LINE + s[len("训完"):] + eol
    if s.startswith("当作卡住"):
        rest = s[len("当作卡住"):]
        m = OLD_STALL_MSG.match(rest)
        if m:
            rest = f"{m.group(1)}: log and output dir unchanged for {m.group(2)} minutes, job {m.group(3)} stopped it"
        else:
            rest = rest.replace("：", ": ", 1)
        return proto.STALL_LINE + rest + eol
    m = OLD_ENDED.match(s)
    if m:
        ts, rc, rest = m.groups()
        if rest.startswith("读不了盘"):
            rest = "disk unreadable, cannot tell whether training finished"
        else:
            r = re.match(r"^没训完（第 (\d+) 次）$", rest)
            if r:
                rest = f"not done (attempt {r.group(1)})"
        return f"{ts} " + proto.LOG_ENDED_FMT % (rc, rest) + eol
    m = OLD_NOT_STARTED.match(s)
    if m:
        ts, why = m.groups()
        if why.startswith("读不了盘"):
            why = "disk unreadable, cannot tell whether training finished"
        else:
            r = re.match(r"^这台机器没有环境 (\S+)(?:（机器表 machines.py）)?$", why)
            if r:
                why = f"this machine has no env {r.group(1)} (machine table machines.py)"
            elif why.startswith("任务说明有问题："):
                why = "bad task spec: " + why[len("任务说明有问题："):]
        return f"{ts} " + proto.LOG_NOT_STARTED_FMT % why + eol
    m = OLD_GPU_GIVEUP_SEEN.match(s)
    if m:
        ts, gpu, seen, idle, waited = m.groups()
        extra = f" (free, someone {seen} still using it, needs {idle} minutes free), waited {waited} minutes"
        return f"{ts} " + proto.LOG_GPU_BUSY_FMT % (gpu, extra) + eol
    m = OLD_GPU_GIVEUP.match(s)
    if m:
        ts, gpu, waited = m.groups()
        return f"{ts} " + proto.LOG_GPU_BUSY_FMT % (gpu, f", waited {waited} minutes") + eol
    m = OLD_GPU_WAIT_SEEN.match(s)
    if m:
        ts, gpu, seen, idle, waited = m.groups()
        return f"{ts} GPU {gpu} free, but someone {seen} still used it; waiting {idle} minutes of quiet (waited {waited} minutes)" + eol
    m = OLD_GPU_WAIT.match(s)
    if m:
        ts, gpu, waited, most = m.groups()
        return f"{ts} GPU {gpu} busy, waiting for it (waited {waited} minutes{', at most ' + most + ' minutes' if most else ''})" + eol
    return ln


def english_logs(D, say, dry, busy=()):
    """Rewrite part logs under <D>/logs/tasks from the old Chinese format to English; only matching lines change.
    busy: safe names of claimed tasks: their logs may still be appended to by an old run_task.sh (a rewrite would lose those
    lines), so they stay as they are and are listed (taskq log shows those lines as they are)."""
    d = os.path.join(D, proto.TASK_LOGS)
    n, left = 0, []
    for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        p = os.path.join(d, f)
        if not f.endswith(".log") or not os.path.isfile(p):
            continue
        with open(p, encoding="utf-8", errors="surrogateescape") as fh:
            old = fh.read()
        new = "".join(english_line(ln) for ln in old.splitlines(keepends=True))
        if new == old:
            continue
        if open_log(f, busy):  # only logs that still have old lines count
            left.append(f)
            continue
        n += 1
        if not dry:
            C.atomic_write(p, new)
    if n or left:
        say(f"{d}: {n} part logs rewritten to English" + (f"; {len(left)} logs of claimed tasks still in the old format left as they are: "
                                                          + ", ".join(left[:5]) + (" ..." if len(left) > 5 else "") if left else ""))


def english_failures(Q, say, dry):
    """Rewrite failure records <time>\\tjob <worker>\\t<reason>\\t<bundle> from the old job field."""
    d = os.path.join(Q, proto.QUEUE["failures"])
    n = 0
    for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        p = os.path.join(d, f)
        if not os.path.isfile(p):
            continue
        with open(p, encoding="utf-8", errors="surrogateescape") as fh:
            old = fh.read()
        out = []
        for ln in old.splitlines(keepends=True):
            s = ln.rstrip("\r\n")
            eol = ln[len(s):]
            x = s.split("\t")
            if len(x) > 1 and x[1].startswith(OLD_FAIL_JOB):
                x[1] = "job " + x[1][len(OLD_FAIL_JOB):]
                ln = "\t".join(x) + eol
            out.append(ln)
        new = "".join(out)
        if new != old:
            n += 1
            if not dry:
                C.atomic_write(p, new)
    if n:
        say(f"{d}: {n} failure records rewritten to English")


def v1_root(ctx, say, dry):
    """Step 1 in a root: part logs (this root's data dir, and the store's) and failure records still in the old format."""
    for d in ctx["data"]:
        rename_logs(d, say, dry, ctx["busy"])
        english_logs(d, say, dry, ctx["busy"])
    if ctx["queue"]:
        english_failures(ctx["queue"], say, dry)


def old_hub_files(home):
    """Hub bookkeeping still in files (OLD_*) -> dict(relay, sets {link set id: links}, sites, records), each None when its
    file is absent (already in the database)."""
    def load(p, kind):
        try:
            with open(os.path.join(home, p)) as f:
                v = json.load(f)
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as e:
            raise Stop(f"cannot read {os.path.join(home, p)} ({e}); fix or remove it and run again")
        if not isinstance(v, kind):
            raise Stop(f"{os.path.join(home, p)} is not a {kind.__name__}; fix or remove it and run again")
        return v
    d = os.path.join(home, OLD_LINKSETS)
    sets = {f[:-5]: load(os.path.join(OLD_LINKSETS, f), dict) for f in sorted(os.listdir(d)) if f.endswith(".json")} if os.path.isdir(d) else {}
    return dict(relay=load(OLD_RELAY_STATE, dict), sets=sets, sites=load(OLD_SITES, dict), records=load(OLD_RECORDS, list))


def db_has_relay(dry):
    """The hub database already holds relay bookkeeping. dry: read it as an immutable file (no -wal / -shm files made; rows
    still only in the write-ahead log are not seen, --apply looks again with a normal connection)."""
    import db as D
    p = D.path()
    if not os.path.exists(p):
        return False
    d = sqlite3.connect(f"file:{p}?mode=ro&immutable=1", uri=True) if dry else D.connect()
    if d is None:
        return False
    try:
        return any(D.has(d, t) and d.execute(f"SELECT 1 FROM {t} LIMIT 1").fetchone() is not None for t in ("placed", "relay"))
    finally:
        d.close()


def link_sets(st):
    """Ledger records that still carry their input links (from before link sets) name a link set instead (edited in place)
    -> {link set id: links} made; a record with neither gets the empty set."""
    import relay_store as ST
    out = {}
    for recs in (st.get("keys") or {}).values():
        for r in recs.values():
            if "linkset" not in r:
                lk = r.pop("links", None) or {}
                r["linkset"] = ST.links_id(lk)
                out[r["linkset"]] = lk
    return out


def fix_smoke(Q, say, dry):
    """Smoke records smoke/<machine>.json: kind names as smoke_gate.kind_of writes them (no repeated part); when two kinds
    become one, the newer record stays. Only files that change are written."""
    d = os.path.join(Q, proto.SMOKE_DIR)
    if not os.path.isdir(d):
        return
    import smoke_gate as SG
    out = {}
    for f in sorted(glob.glob(os.path.join(d, "*.json"))):
        try:
            with open(f) as fh:
                recs = json.load(fh)
        except (OSError, ValueError) as e:
            raise Stop(f"cannot read smoke records {f} ({e}); fix or remove it and run again")
        new = {}
        for k, r in recs.items():
            k = SG.kind_of(k.split("|"))
            r = dict(r, kind=k)
            if k not in new or (r.get("t") or 0) > (new[k].get("t") or 0):
                new[k] = r
        if new != recs:
            out[f] = (len(recs), new)
    for f, (n, new) in out.items():
        say(f"smoke records {os.path.basename(f)}: kind names without a repeated part, {n} -> {len(new)} kinds")
        if not dry:
            C.write_json(f, new)


def leftovers(home):
    """First-level entries in the state dir not in proto.STATE_NAMES -> delete; likewise entries of relay/ and health/ not in
    proto.RELAY_NAMES / HEALTH_NAMES (files of earlier versions, the old bookkeeping files once read in, stage locks of machines
    no longer in the table)."""
    snap = os.path.join(home, OLD_SNAPSHOTS)
    out = [os.path.join(home, f) for f in sorted(os.listdir(home))
           if (f != OLD_SNAPSHOTS or not os.path.isdir(snap) or not any(os.path.isdir(os.path.join(snap, x)) for x in os.listdir(snap)))
           and not any(f == n or f.startswith((n + ".", n + "-")) for n in proto.STATE_NAMES)]  # code/: once no snapshot is left
    import cfg
    locks = tuple(proto.STAGE_LOCK.format(proto.safe(n)) for n in cfg.mtab().MACHINES)
    for sub, names in (("relay", proto.RELAY_NAMES + locks), ("health", proto.HEALTH_NAMES)):
        d = os.path.join(home, sub)
        for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
            if not any(f in (n, n + ".lock") for n in names):
                out.append(os.path.join(d, f))
    return out


def old_snapshots(home):
    """Code snapshots <state dir>/code/<commit> (git worktrees the old queue/q made for manual pueue tasks) -> (to delete,
    kept: the working dir of a pueue task here that has not finished, or all of them when pueue cannot be read)."""
    d = os.path.join(home, OLD_SNAPSHOTS)
    snaps = sorted(os.path.join(d, f) for f in os.listdir(d) if os.path.isdir(os.path.join(d, f))) if os.path.isdir(d) else []
    if not snaps:
        return [], []
    try:
        import cfg
        M = cfg.mtab()
        pq = os.path.expanduser(M._value(M.whoami(), "pueue") or "")
        st = json.loads(subprocess.run([pq, "status", "--json"], capture_output=True, text=True, timeout=60).stdout)
        used = {os.path.realpath(t["path"]) for t in st["tasks"].values()
                if (next(iter(t["status"])) if isinstance(t["status"], dict) else t["status"]) != "Done"}
    except Exception:  # noqa: BLE001
        return [], snaps
    keep = [p for p in snaps if os.path.realpath(p) in used]
    return [p for p in snaps if p not in keep], keep


def drop_snapshots(paths):
    """Delete snapshot worktrees and prune them from their repos."""
    gits = set()
    for p in paths:
        try:
            g = open(os.path.join(p, ".git")).read().split("gitdir:", 1)[1].strip()
            gits.add(g.split("/worktrees/")[0])
        except (OSError, IndexError):
            pass
        shutil.rmtree(p, ignore_errors=True)
    for g in sorted(gits):
        subprocess.run(["git", "--git-dir", g, "worktree", "prune"], capture_output=True)


def v1_hub(ctx, say, dry):
    """Step 1 on the hub: relay's bookkeeping, link sets, what relay last read on each executor and the task records move from
    files into the hub database (records still carrying their links get link sets); smoke kind names; unused state-dir entries
    (the files read in among them) and old code snapshots no unfinished pueue task uses are deleted. Each part finds nothing
    to do when the state is already that way."""
    import relay_store as ST
    import hubstate as HS
    home = ctx["home"]
    files = old_hub_files(home)
    st = files["relay"]
    if st is not None and db_has_relay(dry):
        raise Stop(f"both {os.path.join(home, OLD_RELAY_STATE)} and the hub database hold relay bookkeeping (a newer taskq ran "
                   "rounds before this step): keep one (move the file away to keep the database's) and run again")
    sets = {**files["sets"], **(link_sets(st) if st is not None else {})}
    if st is not None:
        named = {r.get("linkset") for recs in (st.get("keys") or {}).values() for r in recs.values()}
        say(f"relay ledger ({sum(len(r) for r in (st.get('keys') or {}).values())} records) and {len(named)} link sets: from "
            f"{OLD_RELAY_STATE} and {OLD_LINKSETS}/ into the hub database")
    if files["sites"] is not None:
        say(f"what relay last read on {len(files['sites'])} executors: from {OLD_SITES} into the hub database")
    if files["records"] is not None:
        say(f"{len(files['records'])} task records: from {OLD_RECORDS} into the hub database")
    fix_smoke(ctx["queue"], say, dry)
    junk = leftovers(home)
    snaps, kept = old_snapshots(home)
    if junk:
        say(f"unused entries in the state dir: {len(junk)}" + "".join(f"\n  drop {p}" for p in junk))
    if snaps or kept:
        say(f"old code snapshots: delete {len(snaps)}, keep {len(kept)} (working dir of an unfinished pueue task)"
            + "".join(f"\n  keep {p}" for p in kept))
    if dry:
        return
    if st is not None:  # only link sets a record names are kept (update_relay)
        ST.update_state(lambda x: (x.clear(), x.update(st), [HS.put_links(i, lk) for i, lk in sets.items()]))
    for n, rec in (files["sites"] or {}).items():
        if isinstance(rec, dict):
            HS.put_site(n, rec)
    if files["records"] is not None:
        HS.update_records(lambda recs: files["records"] + recs)
    for p in junk:
        shutil.rmtree(p) if os.path.isdir(p) and not os.path.islink(p) else os.remove(p)
    drop_snapshots(snaps)


# ---------------- the steps (each format change adds one; MIN_FORMAT moves up once every place is past a step) ----------------
STEPS = [
    Step(1, "part logs and failure records workers of bundles from before the 2026-11-14 cutover left in the old format (logs of "
            "claimed tasks stay as they are); on the hub, relay's bookkeeping, link sets, executor reads and task records from "
            "files into the hub database, smoke kind names, unused state-dir entries and old code snapshots",
         hub=v1_hub, root=v1_root),
]
assert [s.to for s in STEPS] == list(range(MIN_FORMAT + 1, proto.FORMAT + 1)), "one step per format from MIN_FORMAT + 1 to proto.FORMAT"


# ---------------- in a root (inside the new bundle, run through relay_remote.op) ----------------
def this_bundle():
    """The bundle dir this script runs from (<root>/code/<bundle>)."""
    core = os.path.dirname(HERE)
    if not core.endswith("/" + proto.BUNDLE_CORE):
        raise Stop(f"{HERE} is not inside a bundle")
    return core[:-len(proto.BUNDLE_CORE) - 1]


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
        raise Stop("a health check round is running (stop cron, wait for it to end) - nothing changed") from None
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
    old = sorted(os.path.join(base, f) for f in os.listdir(base) if not f.endswith(proto.TMP))
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
            bid, path = RL.wanted_bundle()
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
    say(f"migration complete, every place in format {proto.FORMAT} ({time.strftime('%F %T')}); start cron again")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
