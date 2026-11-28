#!/usr/bin/env python3
"""Task queue: one queue dir per host; the hub holds the main queue.

Worker calls: next / beat / release / missing. Queue edits: add / set / show / remove / stop / resume / status.
Each task is a recipe plus params; the spec decides env, command, outputs and done.
"""
import argparse
import glob
import json
import os
import shlex
import shutil
import signal
import socket
import sqlite3
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduler code
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import spec as S  # noqa: E402
import proto  # noqa: E402
import qstore  # noqa: E402
from common import format_problem  # noqa: E402

TQ = os.path.expanduser(os.environ.get("TASKQ_DIR", cfg.state("taskq", expand=False)))  # TASKQ_DIR: tests and relay sites (relay.py)
Q = proto.QUEUE  # names inside the queue dir
TASKS = Q["tasks"]
STALE_BEAT = int(os.environ.get("TASKQ_STALE_BEAT", proto.STALE_BEAT))  # a heartbeat older than this is stale (env var for tests only)
FRESH_LOG = int(os.environ.get("TASKQ_FRESH_LOG", proto.FRESH_LOG))    # a part log updated this recently means someone is training
LOCK_WAIT = float(os.environ.get("TASKQ_LOCK_WAIT", proto.LOCK_WAIT))  # retry interval while waiting for the lock (seconds; env var for tests only)
LOCK_DEAD, LOCK_REMOTE, LOCK_MAX = proto.LOCK_DEAD, proto.LOCK_REMOTE, proto.LOCK_MAX  # take a lock dir untouched this long: holder dead / on another host / alive here
LOCK_KEEP = float(os.environ.get("TASKQ_LOCK_KEEP", proto.LOCK_KEEP))  # a holder touches its lock dir this often: only a dead or frozen one goes untouched (env var for tests only)
HALF_CLAIM = proto.HALF_CLAIM  # a claim that only made the dir, no info, is stale after this long
X = proto.EXIT
NOTHING, WAIT, PAUSED = X["nothing"], X["wait"], X["paused"]   # claim exit codes
REFUSED, GONE = X["refused"], X["gone"]  # remove exit codes: refused (assigned, claimed, busy, files unreadable) / gone
LOST = X["lost"]  # beat --strict: some claims are no longer ours
safe = proto.safe
_DB = qstore.hub_db()  # the hub queue in the hub database (format 3 on); None: the queue dir TQ


def ST():
    """This queue's store (qstore.py): the hub database's queue tables, or the queue dir TQ."""
    return _DB or qstore.FileStore(TQ)


def use_db(store):
    """From now on this process keeps the queue in store (a qstore.DbStore; self migrate, right after copying the queue in)."""
    global _DB
    _DB = store


def tasks_path():
    return ST().where()


def have_queue():
    return ST().exists()


# ---------------- tasks ----------------
_bad = {}  # tasks file -> its lines that could not be read [(line number, text)], as of the last read_tasks


def _bad_key(path):
    return "db" if _DB and not path else os.path.abspath(path or tasks_path())


def bad_lines(path=None):
    """Why the tasks file cannot be rewritten (it has lines read_tasks could not read; rewriting would drop them), or None."""
    bad = _bad.get(_bad_key(path))
    if not bad:
        return None
    more = f" and {len(bad) - 1} more" if len(bad) > 1 else ""
    return f"{path or tasks_path()}: line {bad[0][0]}{more} cannot be read ({bad[0][1][:80]}); fix or delete it by hand, the queue is not changed until then"


def read_tasks(path=None):
    """The queue's tasks by (nice, order); path: that tasks file instead (self migrate's held tasks)."""
    out, bad = qstore.FileStore(TQ).read_tasks(path) if path else ST().read_tasks()
    _bad[_bad_key(path)] = bad
    return sorted(out, key=lambda t: (t["nice"], t["order"]))


def write_tasks(tasks, path=None):
    """Replace the queue's tasks (a tasks file is written whole to a temp name and synced, then moved over). Stops instead when
    this process no longer holds the queue lock, or when the file had lines read_tasks could not read: they are not in tasks
    and would be gone."""
    if Lock.held:
        Lock.held.check()
    if bad_lines(path):
        raise SystemExit(bad_lines(path))
    recs = []
    for t in sorted(tasks, key=lambda t: (t["nice"], t["order"])):
        rec = {k: t.get(k) for k in proto.TASK_FIELDS if k != "spec"}
        if t.get("spec"):
            rec["spec"] = t["spec"]  # executor queue: the spec the hub resolved
        recs.append(rec)
    if path:
        qstore.FileStore(TQ).write_tasks(recs, path)
    else:
        ST().write_tasks(recs)


class Lock:
    """The queue lock (a dir with an owner file: host, pid, a random token). While held, a thread touches the dir every
    LOCK_KEEP seconds, so a holder that is working never looks stale however long it takes; one that is dead or frozen does
    and is taken over (LOCK_DEAD / LOCK_REMOTE / LOCK_MAX). A frozen holder may wake up later: check() before changing the
    queue stops it from writing what it read back then."""
    held = None  # the lock this process holds

    def __new__(cls, tries=600):
        if _DB is not None:
            return _DbLock(_DB, tries)
        return super().__new__(cls)

    def __init__(self, tries=600):
        self.d = os.path.join(TQ, Q["lock"])
        self.tries = tries
        self.me = None
        self.dir_id = None
        self.done = threading.Event()

    def __enter__(self):
        os.makedirs(TQ, exist_ok=True)
        for _ in range(self.tries):
            try:
                os.mkdir(self.d)
            except FileExistsError:
                try:
                    old = self._ident(self.d)
                    age = time.time() - old[1] / 1e9
                    if age > {"alive": LOCK_MAX, "remote": LOCK_REMOTE}.get(self._holder(), LOCK_DEAD):
                        stale = f"{self.d}.stale{os.getpid()}"
                        os.rename(self.d, stale)
                        if self._ident(stale) != old:  # someone took the lock after we checked: give it back
                            if not os.path.lexists(self.d):
                                os.rename(stale, self.d)
                        else:
                            shutil.rmtree(stale, ignore_errors=True)
                            continue
                except OSError:
                    pass
                time.sleep(LOCK_WAIT)
                continue
            me = f"{socket.gethostname()}\t{os.getpid()}\t{os.urandom(6).hex()}\n"
            try:
                with open(os.path.join(self.d, proto.LOCK_OWNER), "w") as f:
                    f.write(me)
                self.me = me
                threading.Thread(target=self._keep, daemon=True).start()
            except OSError:
                try:  # could not write owner: remember the dir itself, on exit remove only if still ours
                    st = os.stat(self.d)
                    self.dir_id = (st.st_ino, st.st_mtime_ns)
                except OSError:
                    pass
            Lock.held = self
            return self
        raise SystemExit("cannot acquire the queue lock")

    def _keep(self):
        while not self.done.wait(LOCK_KEEP):
            try:
                if not self.mine():
                    return
                os.utime(self.d)
            except OSError:
                return

    def mine(self):
        """Is the lock still ours (the owner file holds our token; without one, the dir is the one we made)?"""
        try:
            if self.me is not None:
                return open(os.path.join(self.d, proto.LOCK_OWNER)).read() == self.me
            st = os.stat(self.d)
            return (st.st_ino, st.st_mtime_ns) == self.dir_id
        except OSError:
            return False

    def check(self):
        """Call before changing the queue under the lock."""
        if not self.mine():
            raise SystemExit("lost the queue lock (this process stalled and the lock was taken over): nothing more is written")

    @staticmethod
    def _ident(d):
        """Identify a lock: inode (ext4 reuses it quickly), mtime (ns), owner contents."""
        st = os.stat(d)
        try:
            owner = open(os.path.join(d, proto.LOCK_OWNER)).read()
        except OSError:
            owner = None
        return (st.st_ino, st.st_mtime_ns, owner)

    def _holder(self):
        """Holder -> alive (this host, process still there) / remote (another host) / dead (no process, no owner)."""
        try:
            host, pid, _ = open(os.path.join(self.d, proto.LOCK_OWNER)).read().split("\t")
            if host != socket.gethostname():
                return "remote"
            os.kill(int(pid), 0)
        except PermissionError:
            return "alive"
        except (OSError, ValueError):
            return "dead"
        return "alive"

    def __exit__(self, *a):
        Lock.held = None
        self.done.set()
        p = os.path.join(self.d, proto.LOCK_OWNER)
        try:
            if self.me is not None:
                if open(p).read() != self.me:  # lock taken over by someone else
                    return
                os.remove(p)
            else:
                st = os.stat(self.d)
                if (st.st_ino, st.st_mtime_ns) != self.dir_id:
                    return
                if os.path.lexists(p):
                    os.remove(p)
            os.rmdir(self.d)
        except OSError:
            pass


class _DbLock:
    """The queue lock of the hub queue in the hub database: one write transaction (BEGIN IMMEDIATE, waiting as long as the
    queue dir's lock would), committed on leaving, rolled back on an error. Inside one already: part of it."""

    def __init__(self, store, tries):
        self.store, self.tries, self.outer = store, tries, False

    def __enter__(self):
        c = self.store.conn()
        if not c.in_transaction:
            end = time.time() + self.tries * LOCK_WAIT
            while True:
                try:
                    c.execute("BEGIN IMMEDIATE")
                    break
                except sqlite3.OperationalError as e:
                    if "locked" not in str(e) or time.time() >= end:
                        raise SystemExit(f"cannot acquire the queue lock ({e})") from None
                    time.sleep(LOCK_WAIT)
            self.outer = True
            Lock.held = self
        return self

    def mine(self):
        return True

    def check(self):
        pass

    def __exit__(self, et, e, tb):
        if self.outer:
            Lock.held = None
            self.store.conn().execute("ROLLBACK" if et else "COMMIT")
        return False


# ---------------- task specs ----------------
_specs = {}


def spec_of(t):
    """Task -> spec; None when it cannot be resolved (error in t["bad"]). A sent spec (executor queue, resolved by the hub) is used as is, without looking up the recipe."""
    k = json.dumps([t.get("spec")] if t.get("spec") else [t.get("recipe"), t.get("params"), t.get("exp")], sort_keys=True)
    if k not in _specs:
        try:
            if t.get("spec"):
                _specs[k] = S.from_sent(t["spec"])
            else:
                import experiments as E
                _specs[k] = E.resolve_task(t["recipe"], t["params"], t.get("exp"))
        except (KeyError, TypeError, ValueError, AttributeError) as e:  # one bad record only breaks itself, not the whole queue
            _specs[k] = None
            t["bad"] = str(e) if isinstance(e, S.SpecError) else f"{type(e).__name__}: {e}"
    s = _specs[k]
    if s is not None and s["key"] != t["key"]:
        t["bad"] = f"params resolve to task name {s['key']}, which does not match the recorded {t['key']}"
        return None
    return s


class DiskError(Exception):
    """The data dir cannot be read (disk not mounted, store unreachable): done and log age cannot be judged, so no claiming or assigning."""


def facts(tasks):
    """Results and logs in the data dir for these tasks (spec.facts) -> {task name: ...}; tasks with a bad spec are left out.
    An executor queue, or the hub when it is the store, uses the local data dir (cfg.data_dir); the hub queue with the store elsewhere uses the store (site_ops facts, same function). Unreadable -> DiskError."""
    items = []
    for t in tasks:
        s = spec_of(t)
        if s is not None:
            ci = claim_info(t["key"]) or {}
            items.append(dict(spec=s, since=t.get("added"), tags=[ci.get("log") or ci.get("worker")]))
    root, at = cfg.data_dir(), time.time()
    try:
        if root:
            return S.facts(items, root)
        if not items:
            return {}
        import relay_remote as RM
        out = RM.store_op("facts", dict(items=[dict(it, spec=S.sendable(it["spec"])) for it in items]), timeout=300)
        for f in out.values():
            f["at"] = at  # this machine's clock, taken before asking (state compares it with release times written here)
        return out
    except (OSError, RuntimeError) as e:
        raise DiskError(f"cannot read data dir {' ' + root if root else '(on the store)'}: {e}") from None


# ---------------- state ----------------
def claim_info(key):
    return ST().claim(key)


def code_now():
    """Name of the bundle in use here (code/current under the root); "" when not in an executor root (no TASKQ_ROOT or code/current)."""
    r = os.environ.get("TASKQ_ROOT")
    c = os.path.join(os.path.expanduser(r), proto.ROOT["current"]) if r else ""
    return os.path.basename(os.path.realpath(c)) if c and os.path.islink(c) else ""


def code_mine():
    """Name of the bundle this process runs from (this file is under code/<bundle> in the root); "" when not in a bundle."""
    r = os.environ.get("TASKQ_ROOT")
    if not r:
        return ""
    code = os.path.realpath(os.path.dirname(os.path.join(os.path.expanduser(r), proto.ROOT["current"])))
    me = os.path.realpath(HERE)
    return os.path.relpath(me, code).split(os.sep)[0] if me.startswith(code + os.sep) else ""


def fail_lines(key=None):
    """Countable failures in failures/ -> [(timestamp, job, reason)]: on the bundle in use, and ones not run from a bundle (empty bundle field);
    when the bundle in use here is unknown (not in an executor root), all count. key=None: all tasks."""
    now, out = code_now(), []
    for ln in ST().lines("failures", key):
        x = ln.split("\t")
        if len(x) != 4:
            continue
        if now and x[3] and x[3] != now:
            continue
        try:
            out.append((time.mktime(time.strptime(x[0], "%Y-%m-%d %H:%M:%S")), x[1], x[2]))
        except ValueError:
            pass
    return out


def failures(key):
    return len(fail_lines(key))


def read_tail(path):
    """The last lines of a part log for a failure record (proto.FAIL_TAIL_*): only what follows the last carriage return of each
    line (progress bars), oldest lines dropped first; unreadable -> []."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - proto.FAIL_TAIL_READ))
            text = f.read().decode("utf-8", "replace")
    except OSError:
        return []
    lines = [ln.rsplit("\r", 1)[-1].rstrip()[:proto.FAIL_TAIL_WIDTH] for ln in text.split("\n")]
    lines = [ln for ln in lines if ln][-proto.FAIL_TAIL_LINES:]
    while len(lines) > 1 and sum(len(ln) + 1 for ln in lines) > proto.FAIL_TAIL_BYTES:
        lines.pop(0)
    return lines


def write_fail_tail(key, rec):
    """Replace failtail/<key> with rec (proto.FAIL_TAIL_FIELDS); cannot write -> False."""
    try:
        ST().put("failtail", key, json.dumps({k: rec.get(k) for k in proto.FAIL_TAIL_FIELDS}, ensure_ascii=False))
        return True
    except (OSError, sqlite3.Error):
        return False


def fail_tail(key):
    """failtail/<key> -> dict (proto.FAIL_TAIL_FIELDS), or None when absent or unreadable."""
    try:
        r = json.loads(ST().get("failtail", key) or "")
    except ValueError:
        return None
    return r if isinstance(r, dict) and isinstance(r.get("lines"), list) else None


def tail_reason(tail, width=200):
    """The line of a failure tail that most likely says why: the last one that is not the runner's own end, stall or done line."""
    import re
    skip = [re.compile(proto.LOG_ENDED), re.compile(proto.LOG_STALLED), re.compile(r"^" + proto.LOG_TS + r"(?:\.\d+)?:? ?$")]
    for ln in reversed((tail or {}).get("lines") or []):
        if ln.strip() and not ln.startswith(proto.DONE_LINE) and not any(r.match(ln) for r in skip):
            return " ".join(ln.split())[:width]
    return ""


def skip_state(key, now=None):
    """Which executors do not take key now -> (set of workers, {worker: when its failure skip expires}). A skip for any other reason
    always counts; failure skips (proto.FAIL_SKIP, written by the relay) count while that executor's run of failures (each within
    RETRY_KEEP of the next) has used up RETRY skip, or until the last one plus its wait."""
    now = time.time() if now is None else now
    out, at, fails = set(), {}, {}
    for ts, w, why in skips(key):
        if why.startswith(proto.FAIL_SKIP):
            fails.setdefault(w, []).append(ts)
        else:
            out.add(w)
    for w, tss in fails.items():
        if w in out:
            continue
        tss.sort()
        n = 1
        while n < len(tss) and tss[-n] - tss[-n - 1] <= proto.RETRY_KEEP:
            n += 1
        until = tss[-1] + proto.retry_wait("skip", n)
        if n >= proto.RETRY["skip"][3]:
            out.add(w)
        elif now < until:
            out.add(w)
            at[w] = until
    return out, at


def skips(key=None):
    """Records in skips/ -> [(timestamp, worker, reason)]: a task this machine cannot run (usually does not fit); only this machine stops claiming it, and it is not a failure (does not affect MAX_FAIL or pausing). key=None: all tasks."""
    out = []
    for ln in ST().lines("skips", key):
        f = ln.split("\t")
        try:
            out.append((time.mktime(time.strptime(f[0], "%Y-%m-%d %H:%M:%S")), f[1], f[2] if len(f) > 2 else ""))
        except (ValueError, IndexError):
            pass
    return out


def stopped_info(key):
    """Stop record -> "time reason"; None if absent."""
    r = ST().get("stopped", key)
    return None if r is None else r.strip().replace("\t", " ")


def input_info(key):
    """Missing-input record -> (timestamp, note); None if absent."""
    try:
        f = ST().get("inputs", key).rstrip("\n").split("\t")
        return float(f[0]), " ".join(f[1:])
    except (AttributeError, ValueError, IndexError):
        return None


def released_info(key):
    """Last release -> (time, worker, why proto.RELEASE_WHY); None if absent. A worker of a bundle from before self migrate
    writes no reason (it may run for weeks after the switch): counts as an ordinary release."""
    try:
        f = ST().get("released", key).split()
        return float(f[0]), f[1], f[2] if len(f) > 2 else proto.RELEASE_WHY["back"]
    except (AttributeError, ValueError, IndexError):
        return None


def released_at(key):
    r = released_info(key)
    return r and r[0]


def _mark_released(key, worker, why):
    ST().put("released", key, f"{time.time()} {worker} {proto.RELEASE_WHY[why]}\n")


def recent_failures(window=proto.PAUSE_WINDOW):
    """Number of countable failures in the whole queue in the last window seconds (fail_lines)."""
    cut = time.time() - window
    return sum(t >= cut for t, _, _ in fail_lines())


def state(t, f):
    """-> (state, note): done / stopped (taskq.py stop) / input (missing inputs, taskq.py missing) / claimed / busy / failed / free / stale (claim expired, can be claimed again) / bad (spec cannot be resolved).
    f: on-disk facts for this task (one item from facts())."""
    if spec_of(t) is None:
        return "bad", t.get("bad", "")
    if f["done"]:
        return "done", ""
    stop = stopped_info(t["key"])
    if stop is not None:
        ci = claim_info(t["key"])
        return "stopped", f"stopped ({stop})" + (f", job {ci.get('worker')} still holds it and is stopping" if ci else "")
    inp = input_info(t["key"])
    if inp is not None and claim_info(t["key"]) is None:
        return "input", f"missing inputs ({time.strftime('%m-%d %H:%M', time.localtime(inp[0]))} {inp[1]})"
    age = f["log_age"]
    cs = claim_state(t["key"], age)
    if cs:
        return cs
    if age is not None and age < FRESH_LOG:
        rel = released_at(t["key"])
        if rel is None or f["at"] - age > rel + 5:  # the log moved on after the release (or was never released): someone outside the queue is training
            # (at and age come from the same measurement: time.time() here would drift by however long reading the facts took)
            return "busy", f"part log updated {age / 60:.0f} minutes ago"
    if failures(t["key"]) >= proto.MAX_FAIL:
        return "failed", f"failed {failures(t['key'])} times"
    return "free", ""


def claim_state(key, age):
    """Claim record (age: seconds since the part log last changed) -> None (absent) / ("claimed", note) (alive) / ("stale", note) (expired, can be claimed again)."""
    ci = claim_info(key)
    now = time.time()
    if ci is None:
        # only the dir was made, info was never written (the claiming process was killed): stale after a few minutes, the next claim takes it
        made = ST().claim_made(key)
        if made is None:
            return None
        return ("claimed", "being claimed") if proto.claim_alive(None, made, now, half=HALF_CLAIM) else ("stale", "incomplete claim (dir only, no info)")
    if ci.get("assigned"):
        return "claimed", f"assigned to {ci['assigned']}"
    if proto.claim_alive(ci, None, now, age, stale=STALE_BEAT, fresh=FRESH_LOG):
        return "claimed", f"job {ci.get('worker')} @ {ci.get('host')}"
    return "stale", f"claim by job {ci.get('worker')} expired"


# ---------------- on an executor: claim, heartbeat, release ----------------
def _claim(a, pick):
    """Claim the first free / stale task in order for which pick(t) is not None -> (exit code, pick result)."""
    if not have_queue():
        # not the same as "no tasks" (exit code 10): then every started job would exit at once and queued jobs would all be used up in minutes
        print(f"no {ST().where()}", file=sys.stderr)
        return X["no_queue"], None
    why = format_problem()
    if why:  # the state here is not in this code's on-disk format (self migrate has not reached it): claim nothing, as when paused
        print(why, file=sys.stderr)
        return PAUSED, None
    busy_ahead = 0  # ahead of us and just stopped (log updated in the last 15 minutes, unclaimed)
    try:  # outside the lock: reading every task's files takes long, and claims are checked again under the lock below
        F = facts(read_tasks())
    except DiskError as e:  # check the disk first: an unreadable disk is the likely cause of the failure
        print(e, file=sys.stderr)
        return PAUSED, None
    with Lock():
        tasks = read_tasks()  # a task added meanwhile has no facts yet: claimed next time
        if recent_failures() >= proto.PAUSE_N:
            return PAUSED, None
        for t in sorted((t for t in tasks if t["key"] in F), key=lambda t: (t["nice"], not has_progress(t, F[t["key"]]), t["order"])):
            if t["key"] in a.exclude:
                continue
            st, _ = state(t, F[t["key"]])
            if st == "busy" and pick(t) is not None:
                busy_ahead += 1
            if st not in ("free", "stale"):
                continue
            out = pick(t)
            if out is None:
                continue
            if busy_ahead >= a.free_slots and time.time() < a.patience_until:
                return WAIT, None  # keep the slot for an earlier task; wait until its log is older than 15 minutes
            Lock.held.check()  # looking at every task can take long
            if st == "stale" and not ST().claim_clear(t["key"]):
                continue
            now = time.time()
            a.claim_key, a.token = t["key"], os.urandom(6).hex()  # set first: a kill while claiming drops the claim (_unclaim)
            info = dict(worker=a.worker, host=socket.gethostname().split(".")[0], since=now, beat=now, token=a.token)
            if getattr(a, "log_tag", None):
                info["log"] = a.log_tag  # job name in the part log differs from the worker name (outside Slurm)
            if not ST().claim_new(t["key"], info):
                a.claim_key = None
                continue
            return 0, out
    return NOTHING, None


def has_progress(t, f):
    """This task is half-done on this machine: claim it first within the same nice, so released and adopted tasks are not queued behind unstarted ones. Same for training, inference and other steps:
    the output dir has a checkpoint (the spec's resume), or this machine's data dir has a part log from after the task was added (it ran, then was stopped / hit the time limit / stalled and released),
    no failure record, and the last release was not for missing inputs or skip (a redone task: logs from before `added` do not count). f: on-disk facts (facts())."""
    if f is None:
        return False
    if f["resumable"]:
        return True
    rel = released_info(t["key"])
    if rel and rel[2] in (proto.RELEASE_WHY["missing"], proto.RELEASE_WHY["skip"]):
        return False
    return failures(t["key"]) == 0 and f["ran"]


def run_args(t):
    """run_task.sh arguments: --spec <JSON> when a spec is attached, otherwise <recipe> k=v...."""
    if t.get("spec"):
        return ["--spec", json.dumps(t["spec"], ensure_ascii=False, separators=(",", ":"))]
    return S.kv_args(t["recipe"], t["params"])


def cmd_next(a):
    envs = set(e for e in a.envs.split(",") if e)
    me = here()

    def pick(t):
        s = spec_of(t)
        if not S.envs_of(s) or not set(S.envs_of(s)) <= envs or (s.get("machines") and me not in s["machines"]) or s.get("device", "gpu") != a.device:
            return None
        if not S.fits(s.get("res"), a.cpus, a.mem_mb, a.gpu_mem)[0]:
            return None
        return t["key"] + "\n" + " ".join(shlex.quote(x) for x in run_args(t))
    sigs = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)

    def on_sig(n, _f):
        raise _Killed(n)
    old = {n: signal.signal(n, on_sig) for n in sigs}
    printed = False
    try:
        rc, out = _claim(a, pick)
        if rc == 0:
            print(out + "\n" + a.token)  # third line: the token for this claim (beat, release --token)
            sys.stdout.flush()
            printed = True
        return rc
    except _Killed as e:
        # claimed but stopped before printing: the worker does not know which task it got; drop the claim instead of leaving it to expire
        for n in sigs:
            signal.signal(n, signal.SIG_IGN)
        if not printed and getattr(a, "claim_key", None):
            _unclaim(a)
        signal.signal(e.args[0], signal.SIG_DFL)
        os.kill(os.getpid(), e.args[0])
        raise SystemExit(128 + e.args[0])
    finally:
        for n, h in old.items():
            signal.signal(n, h)


class _Killed(Exception):
    pass


def _unclaim(a):
    try:
        with Lock(tries=40):
            ci = claim_info(a.claim_key)
            if ci is None or ci.get("token") == getattr(a, "token", None):
                ST().claim_drop(a.claim_key)
    except (SystemExit, OSError):
        pass


def owns(ci, a):
    """Is the claim record from this round of this worker: same worker; with --token the token must match too (a restarted worker with the same name starts a new round)."""
    if not ci or str(ci.get("worker")) != str(a.worker):
        return False
    return not getattr(a, "token", None) or not ci.get("token") or ci["token"] == a.token


def cmd_beat(a):
    lost = []
    with Lock():  # same lock as claiming: nobody can take over between reading our record and writing it back
        for key in a.keys:
            ci = claim_info(key)
            if owns(ci, a):
                ci["beat"] = time.time()
                ST().claim_put(key, ci)
            elif ci is not None or _no_claim(key):  # unreadable records do not count as lost
                lost.append(key)
    if a.strict and lost:
        print("\n".join(lost))
        return LOST
    return 0


def _no_claim(key):
    return ST().no_claim(key)


def unwanted(key, worker):
    """Why this task should not be sent to executor `worker` now, or None: it was stopped here, or it is no longer that executor's
    (released, or claimed by someone else). The hub asks while it moves a task's inputs. Read without the lock: each answer is one
    small file, and a stop that lands just after it is seen by the next check (the claim is still the executor's)."""
    stop = stopped_info(key)
    if stop is not None:
        return f"stopped ({stop})"
    ci = claim_info(key)
    if ci is None:
        return "no longer claimed" if _no_claim(key) else None  # a record that cannot be read does not count as lost
    return None if str(ci.get("worker")) == str(worker) else f"claimed by {ci.get('worker')}"


def cmd_release(a):
    with Lock(**({"tries": max(1, int(a.lock_wait / LOCK_WAIT))} if a.lock_wait else {})):
        for key in a.keys:
            _release(a, key)
    return 0


def _release(a, key):
    ci = claim_info(key)
    if getattr(a, "token", None) and not owns(ci, a):  # late release (a new round took over the claim, or it is gone): record nothing
        if ci is None and (a.failed or a.skip):
            print(f"{key}: this round's claim is gone (expired and claimed elsewhere, or already released); not recording failure / skip")
        return
    if a.skip:
        ST().add_line("skips", key, f"{time.strftime('%F %T')}\t{a.worker}\t{a.skip}")
    if a.failed:
        ST().add_line("failures", key, proto.FAIL_LINE_FMT % (time.strftime('%F %T'), a.worker, ' '.join(a.failed.split()), code_mine()))
        if owns(ci, a):
            _record_tail(key, ci, a)
    if owns(ci, a):
        _mark_released(key, a.worker, "failed" if a.failed else "skip" if a.skip else "back")
        ST().claim_drop(key)


def _record_tail(key, ci, a):
    """failtail/<key> for a failed run: the end of the part log of this claim (its log tag, else the worker); no log -> the reason."""
    log = f"{safe(key)}{proto.SAFE}{ci.get('log') or ci['worker']}_{proto.LOG_PART}1.log"
    try:
        data = cfg.data_dir()
    except Exception:  # noqa: BLE001  no data dir here: the failure is still recorded
        data = ""
    lines = read_tail(os.path.join(data, proto.TASK_LOGS, log)) if data else []
    write_fail_tail(key, dict(time=time.strftime('%F %T'), job=str(a.worker), log=log, lines=lines or [' '.join(a.failed.split())]))


def cmd_missing(a):
    with Lock():
        return _missing(a)


def _missing(a):
    """Missing inputs: record inputs/<key> and release like release does (not a failure)."""
    ci = claim_info(a.key)
    if not owns(ci, a):
        print(f"{a.key} is not claimed by {a.worker}", file=sys.stderr)
        return 1
    ST().put("inputs", a.key, f"{time.time()}\t{a.worker}\t{(a.why or '').replace(chr(9), ' ').replace(chr(10), ' ')}\n")
    _mark_released(a.key, a.worker, "missing")
    ST().claim_drop(a.key)
    return 0


def cmd_input_ok(a):
    for k in a.keys:
        if ST().drop("inputs", k):
            print(f"inputs complete: {k}")
    return 0


def cmd_unskip(a):
    with Lock():
        for k in a.keys:
            n = ST().keep_lines("skips", k, lambda ln: a.worker is not None and ln.split("\t")[1:2] != [a.worker])
            if n is None:
                print(f"{k}: no skip record")
                continue
            print(f"{k}: removed {n[0] - n[1]} skip record(s)")
    return 0


def _clear_failures(k):
    """Remove key's failure lines and failure tail -> what was removed, in words."""
    n = ST().keep_lines("failures", k, lambda ln: False)
    out = [f"{n[0]} failure(s)"] if n and n[0] else []
    return out + (["the failure tail"] if ST().drop("failtail", k) else [])


def cmd_forget(a):
    """Clear the failures of keys in this queue (failures and failtail), so a task that failed here is claimed again and resumes
    from its progress (the hub's taskq retry and automatic retries call it on the executor)."""
    with Lock():
        for k in a.keys:
            got = _clear_failures(k)
            print(f"{k}: removed {', '.join(got)}" if got else f"{k}: no failures recorded")
    return 0


def cmd_retry(a):
    """Let failed or skipped tasks run again: remove their skip lines (--worker: only that executor's), failures and failure tail
    here, and on the hub also clear the failures in the queue of the executor each one is assigned to (relay_remote.push_forget)."""
    if a.keys[:1] == ["worker"]:
        return _retry_workers(a.keys[1:])
    tasks = read_tasks()
    keys = list(a.keys) + [t["key"] for t in tasks if a.exp and t.get("exp") == a.exp]
    if a.all_failed:
        tails = ST().names("failtail")
        keys += [t["key"] for t in tasks if safe(t["key"]) in tails or ST().lines("failures", t["key"])
                 or any(why.startswith(proto.FAIL_SKIP) for _, _, why in skips(t["key"]))]
    keys = list(dict.fromkeys(keys))
    if not keys:
        print("nothing to retry (give keys, --exp or --all-failed)", file=sys.stderr)
        return GONE
    have = {t["key"] for t in tasks}
    sites = {}
    with Lock():
        for k in keys:
            n = ST().keep_lines("skips", k, lambda ln: a.worker is not None and ln.split("\t")[1:2] != [a.worker])
            got = ([f"{n[0] - n[1]} skip(s)"] if n and n[0] > n[1] else []) + _clear_failures(k)
            print(f"{k}: removed {', '.join(got)}" if got else f"{k}: nothing recorded here" + ("" if k in have else " (not in the queue)"))
            s = (claim_info(k) or {}).get("assigned") if k in have else None
            if s and (a.worker is None or a.worker == s):
                sites.setdefault(s, []).append(k)
    if here() != queue_host():
        return 0
    import events
    import relay_remote as RM
    for k in keys:
        events.add("taskq", f"taskq retry {k}: automatic retries count from here", kind=proto.EV_RETRY_CLEAR, key=k, site=a.worker)
    for s, ks in sorted(sites.items()):
        if cfg.mtab().is_exec(s):
            for ln in RM.push_forget(s, ks):
                print(ln)
    return 0


def _retry_workers(targets):
    """taskq retry worker <machine>/<worker>: its failed pueue worker is started again at the next round, with the automatic
    restarts counted from now."""
    if here() != queue_host() or not targets:
        print("taskq retry worker <machine>/<worker> ... runs on the hub", file=sys.stderr)
        return 2
    import events
    M, bad = cfg.mtab(), 0
    for t in targets:
        m, _, wn = t.partition("/")
        w = next((w for w in M.workers(m) if w["name"] == wn), None) if m in M.MACHINES else None
        if w is None:
            print(f"no worker {t} (as <machine>/<worker>: taskq machines names, workers in machines.py)", file=sys.stderr)
            bad = 1
            continue
        events.add("taskq", f"taskq retry worker {t}: restarts count from here", kind=proto.EV_RETRY_CLEAR, key=proto.RETRY_WORKER + w["id"], site=m)
        print(f"{t}: the next round starts it again when it has work")
    return bad


def cmd_take(a):
    """Assign one key to executor a.worker (called by relay.py): no expiry; only free / stale tasks; assigning one that is already its own succeeds again (since unchanged).
    The task's files are read before the lock (they may be on the store); its claim and records are checked again under it."""
    t = next((t for t in read_tasks() if t["key"] == a.key), None)
    if t is None:
        print(f"no {a.key} in the queue")
        return NOTHING
    try:
        f = facts([t]).get(a.key)
    except DiskError as e:
        print(e)
        return NOTHING
    with Lock():
        t = next((t for t in read_tasks() if t["key"] == a.key), None)
        if t is None:
            print(f"no {a.key} in the queue")
            return NOTHING
        ci = claim_info(a.key)
        mine = bool(ci) and str(ci.get("worker")) == str(a.worker)
        if ci and ci.get("assigned") and not mine:
            print(f"{a.key}: already assigned to {ci['assigned']}")
            return NOTHING
        st, note = state(t, f) if f else ("bad", t.get("bad", ""))
        if st in ("done", "stopped", "bad") or (not mine and st not in ("free", "stale")):
            print(f"{a.key}: {st} {note}")
            return NOTHING
        if ci or st == "stale":
            ST().claim_clear(a.key)
        now = time.time()
        info = dict(worker=a.worker, host=a.worker, since=ci.get("since", now) if mine else now, beat=now, assigned=a.worker)
        if not ST().claim_new(a.key, info):
            print(f"{a.key}: just claimed by someone else")
            return NOTHING
        print(t["key"])
        return 0


# ---------------- add, edit ----------------
def same_file(keys, have):
    """New keys whose file name (safe) equals another key already in the queue or in this batch -> list of notes (claims, failures and logs all use the file name)."""
    seen, bad = {safe(k): k for k in have}, []
    for k in keys:
        o = seen.setdefault(safe(k), k)
        if o != k:
            bad.append(f"{k} and {o} both map to file name {safe(k)}")
    return bad


def _resolve_new(t):
    if t.get("spec"):
        return S.from_sent(t["spec"])
    import experiments as E
    return E.resolve_task(t["recipe"], t["params"], t.get("exp"))


def _insert(new, nice, update_only=False):
    """Insert new tasks (dict(key, exp, recipe, params[, spec])) into the queue; skip existing ones (a sent spec replaces the old spec). Resolve everything first; bad params are not added.
    update_only (relay.py resend): only replace existing specs; report each unresolvable one (exit code 2), replace the rest; print keys not in the queue.
    A running task whose out / done changed is not replaced yet (the worker holds the old spec, so the done check would mismatch; the next resend replaces it)."""
    errs = []
    with Lock():
        tasks = read_tasks()
        have = {t["key"]: t for t in tasks}
        order = max([t["order"] for t in tasks] or [0])
        pairs = []
        for t in new:
            try:
                pairs.append((t, _resolve_new(t)))
            except S.SpecError as e:
                if not update_only:
                    raise
                errs.append(f"{(t.get('spec') or {}).get('key', '?')}: {e}")
        if update_only:
            for _, s in pairs:
                if s["key"] not in have:
                    print(f"not in queue, not replaced: {s['key']}", file=sys.stderr)
            pairs = [(t, s) for t, s in pairs if s["key"] in have]
        bad = same_file([s["key"] for t, s in pairs if not (t.get("spec") and s["key"] in have)], have)
        if bad:
            print("cannot add: " + "; ".join(bad), file=sys.stderr)
            return 2
        clash = 0
        for t, s in pairs:
            if s["key"] in have:
                old = have[s["key"]]
                if t.get("spec") and old.get("spec") != t["spec"]:
                    was = spec_of(dict(old))
                    if was and (was["out"], was["done"]) != (s["out"], s["done"]) and (claim_state(s["key"], None) or ("",))[0] == "claimed":
                        print(f"running, out / done changed, not replaced yet (next resend): {s['key']}", file=sys.stderr)
                        continue
                    old.update(spec=t["spec"], recipe=t["recipe"], params=t["params"])
                    if was and (was["out"], was["done"]) != (s["out"], s["done"]):
                        old["added"] = time.time()  # outputs changed: older logs do not count as half-done for this run
                    print(f"spec replaced: {s['key']}")
                elif not t.get("spec") and not t.get("exp") and _differs(spec_of(old), s):  # name clash with --recipe / --file
                    print(f"a task with this name and a different spec exists, not added: {s['key']} (existing {old.get('recipe')} {json.dumps(old.get('params'), ensure_ascii=False)})", file=sys.stderr)
                    clash += 1
                else:
                    print(f"exists, skipped: {s['key']}")
                continue
            order += 1
            tasks.append(dict(t, key=s["key"], order=order, nice=nice, added=time.time()))
            have[s["key"]] = tasks[-1]
            print(f"added: {nice:>5} {s['key']} ({t['recipe']}, env {','.join(S.envs_of(s))})")
        write_tasks(tasks)
    for e in errs:
        print(f"cannot replace: {e}", file=sys.stderr)
    return 2 if errs or clash else 0


def _differs(a, b):
    drop = ("exp", "params", "names", "recipe")
    return a is None or {k: v for k, v in a.items() if k not in drop} != {k: v for k, v in b.items() if k not in drop}


def read_sheets(path):
    """Task sheet file (JSON: one {field: value} object or a list) -> ([dict(exp, recipe, params)], nice). No recipe = command;
    nice goes in the sheet (all sheets must agree); every other field is a recipe param (see "other people's tasks" in queue/README.md)."""
    try:
        d = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise S.SpecError(f"cannot read task sheet {path}: {e}") from None
    sheets = d if isinstance(d, list) else [d]
    if not sheets or not all(isinstance(x, dict) for x in sheets):
        raise S.SpecError(f"task sheet {path} must be a JSON object or a list of objects")
    nices = {x.get("nice") for x in sheets}
    if len(nices) > 1:
        raise S.SpecError(f"task sheet {path} has different nice values: {sorted(map(str, nices))}")
    new = [dict(exp=None, recipe=x.get("recipe", "command"), params={k: v for k, v in x.items() if k not in ("recipe", "nice")})
           for x in sheets]
    nice = nices.pop()
    return new, (int(nice) if nice is not None else None)


def only_of(items):
    """["seed=1,2", ...] -> {"seed": ["1", "2"]}; absent -> None."""
    out = {}
    for x in items:
        k, sep, v = x.partition("=")
        if not sep or not k or not v:
            raise S.SpecError(f"--only needs param=value[,value...]: {x}")
        out.setdefault(k, []).extend(v.split(","))
    return out or None


def _err(msg, rc):
    print(msg, file=sys.stderr)
    return rc


def cmd_add(a):
    import experiments as E
    try:
        if a.exp:
            e = E.experiment(a.exp)
            if e is None:
                return _err(f"no {a.exp} in the experiment table (have: {', '.join(x['id'] for x in E.load()[0])})", GONE)
            new = [dict(exp=t["exp"], recipe=t["recipe"], params=t["params"]) for t in E.expand(e, only_of(a.only))]
            if a.nice is None:
                a.nice = e.get("nice")
                if a.nice is None:
                    return _err(f"{a.exp} in the experiment table has no nice, pass --nice", 2)
                print(f"using nice {a.nice} from the experiment table")
        elif a.spec_stdin:
            try:
                got = json.loads(sys.stdin.read())
            except ValueError as e:
                raise S.SpecError(f"spec is not JSON: {e}") from None
            got = got if isinstance(got, list) else [got]
            if not all(isinstance(x, dict) for x in got):
                raise S.SpecError("spec must be a JSON object or a list of objects")
            new = [dict(exp=None, recipe=x.get("recipe"), params=x.get("params") or {}, spec=x) for x in got]
        elif a.recipe:
            new = [dict(exp=None, recipe=a.recipe, params=S.parse_kv(a.args))]
        elif a.file:
            new, nice = read_sheets(a.file)
            if a.nice is None:
                a.nice = nice
        else:
            return _err("pass --exp, --recipe, --file or --spec-stdin", 2)
        if a.nice is None:
            return _err("pass --nice", 2)
        if not a.spec_stdin and not a.update_only and here() == queue_host():
            bad = _preflight(new)
            if bad:
                for b in bad:
                    print(f"cannot add: {b}", file=sys.stderr)
                return REFUSED
        return _insert(new, a.nice, update_only=a.update_only)
    except S.SpecError as e:
        print(f"cannot add: {e}", file=sys.stderr)
        return 2


def _preflight(new):
    """Hub only: what check.preflight finds in the specs about to be added (none when the check itself cannot run)."""
    import experiments as E
    try:
        import check
        return check.preflight([E.resolve_task(n["recipe"], n["params"], n.get("exp")) for n in new])
    except Exception:  # noqa: BLE001  a spec error is reported by _insert
        return []


def cmd_set(a):
    import experiments as E
    try:
        kv = S.parse_kv(a.kv)
    except S.SpecError as e:
        return _err(str(e), 2)
    with Lock():
        tasks = read_tasks()
        t = next((t for t in tasks if t["key"] == a.key), None)
        if t is None:
            print(f"no {a.key} in the queue", file=sys.stderr)
            return GONE
        if t.get("spec"):  # executor queue: the spec was resolved by the hub and sent here; resolving again from params here would lose it
            print(f"{a.key}: spec came from the hub; change the params in the hub queue, then taskq site resend", file=sys.stderr)
            return 2
        p = dict(t["params"])
        for k, v in kv.items():
            if v == "":
                p.pop(k, None)
            else:
                p[k] = v
        try:
            s = E.resolve_task(t["recipe"], p, t.get("exp"))
        except S.SpecError as e:
            print(f"cannot change: {e}", file=sys.stderr)
            return 2
        if s["key"] != t["key"]:
            print(f"after the change the task name would be {s['key']} (claims and results are keyed by task name): use add for a new task, then remove this one", file=sys.stderr)
            return 2
        print(f"{t['key']}: {json.dumps(t['params'], ensure_ascii=False)} -> {json.dumps(p, ensure_ascii=False)}")
        t["params"] = p
        write_tasks(tasks)
    site = (claim_info(a.key) or {}).get("assigned")
    if site:  # the executor holds the spec sent at staging
        print(f"{a.key} is assigned to {site}: taskq site resend {site} to send the new spec there")
    return 0


def cmd_set_nice(a):
    """Change nice in this queue; on the hub, also in the queue of the executor each task is assigned to (relay_remote.push_nice).
    -n: only print what would change, here and there."""
    would = "would set " if a.dry_run else ""
    with Lock():
        tasks = read_tasks()
        for t in tasks:
            if t["key"] in a.keys:
                print(f"{would}{t['key']}: {t['nice']} -> {a.nice}")
                t["nice"] = a.nice
        if not a.dry_run:
            write_tasks(tasks)
    have = {t["key"] for t in tasks}
    for k in sorted(set(a.keys) - have):
        print(f"not in queue: {k}", file=sys.stderr)
    sites = {}
    if here() == queue_host():
        for k in a.keys:
            s = (claim_info(k) or {}).get("assigned") if k in have else None
            if s and cfg.mtab().is_exec(s):
                sites.setdefault(s, []).append(k)
    if sites:
        import relay_remote as RM
        for s, ks in sorted(sites.items()):
            for ln in RM.push_nice(s, ks, a.nice, dry=a.dry_run):
                print(ln)
    return 0 if have >= set(a.keys) else GONE


RECORDS = ("failures", "stopped", "inputs", "released", "failtail")  # records removed together with the task (executor queues also remove skips/)


def cmd_remove(a):
    """Remove tasks nobody is working on (for example, move steps queued on a stopped executor back to the hub). Not removed when a live claim exists (any state) or the part log just changed. Removal also deletes failures, stop, missing-input and release records and the stale claim (the same key added later is a new task); skips/ are removed only in an executor queue (records with a spec), and kept in the hub queue (a machine that cannot run it may still be able to later).
    Result files are left alone. Exit code 0 removed (keys already absent are skipped); REFUSED not removed; GONE none of them were in the queue (nothing removed).
    The tasks' files are read before the lock (they may be on the store); claims and records are checked under it."""
    by = {t["key"]: t for t in read_tasks()}
    try:
        F = facts([by[k] for k in a.keys if k in by])
    except DiskError as e:
        print(f"not removed: {e}", file=sys.stderr)
        return REFUSED
    with Lock():
        tasks = read_tasks()
        by = {t["key"]: t for t in tasks}
        missing = [k for k in a.keys if k not in by]
        bad = []
        for k in a.keys:
            if k in by:
                f = F.get(k)
                cs = claim_state(k, f["log_age"] if f else None)  # ignore state: a stopped or done task may still be claimed by a worker
                ci = claim_info(k) or {}
                if ci.get("assigned"):
                    bad.append(f"{k}: assigned to {ci['assigned']}, stop it first, remove after it is released")
                elif cs and cs[0] == "claimed":
                    bad.append(f"{k}: claimed ({cs[1]}), someone holds it")
                elif f and state(by[k], f)[0] == "busy":
                    bad.append(f"{k}: busy, someone is working on it")
        for k in missing:
            print(f"not in queue: {k}", file=sys.stderr)
        for b in bad:
            print(f"not removed: {b}", file=sys.stderr)
        if bad:
            return REFUSED
        keys = [k for k in a.keys if k in by]
        if not keys:
            return GONE
        write_tasks([t for t in tasks if t["key"] not in keys])
        for k in keys:
            ST().forget(k, RECORDS + (("skips",) if by[k].get("spec") else ()))  # only stale claims are left
    for k in keys:
        print(f"removed: {k}")
    return 0


def cmd_stop(a):
    with Lock():
        by = {t["key"]: t for t in read_tasks()}
        missing = [k for k in a.keys if k not in by]
        for k in missing:
            print(f"not in queue: {k}", file=sys.stderr)
        if missing:
            return GONE
        for k in a.keys:
            ci = claim_info(k)
            ST().put("stopped", k, f"{time.strftime('%F %T')}\t{a.why or ''}\n")
            w = str((ci or {}).get("worker", ""))
            if (ci or {}).get("assigned"):
                print(f"stopped: {k} (assigned to {ci['assigned']}; the hub stops and releases it on its next check)")
            elif not ci:
                print(f"stopped: {k} (nobody is working on it; it will not be claimed again)")
            elif w.isdigit():
                print(f"stopped: {k} (job {w} stops and releases within 30 seconds)")
            else:
                print(f"stopped: {k} (held by {w}; the hub stops and releases it on its next check)")
    return 0


def cmd_resume(a):
    for k in a.keys:
        if ST().drop("stopped", k):
            print(f"stop undone: {k}")
        else:
            print(f"not stopped: {k}")
    return 0


def cmd_show(a):
    t = next((t for t in read_tasks() if t["key"] == a.key), None)
    if t is None:
        print(f"no {a.key} in the queue", file=sys.stderr)
        return GONE
    s = spec_of(t)
    if a.json:
        if s is None:
            print(t.get("bad"), file=sys.stderr)
            return 2
        S.json_out(s)
        return 0
    try:
        st, note = state(t, facts([t])[t["key"]]) if s else ("bad", t.get("bad", ""))
    except DiskError as e:
        st, note = "?", str(e)
    print(f"{t['key']}: nice {t['nice']}, {st} {note}; experiment {t.get('exp') or '(not in the experiment table)'}")
    print(f"  run_task.sh {' '.join(shlex.quote(x) for x in run_args(t))}")
    if s:
        for k in ("env", "cmd", "out", "done", "inputs"):
            print(f"  {k:6} {s[k]}")
    for ts, w, why in sorted(fail_lines(t["key"]))[-5:]:
        print(f"  failed {time.strftime('%F %T', time.localtime(ts))} {w}: {why}")
    for ts, w, why in sorted(skips(t["key"]))[-5:]:
        print(f"  skipped {time.strftime('%F %T', time.localtime(ts))} by {w}: {why}")
    for w, at in sorted(skip_state(t["key"])[1].items()):
        print(f"  {w} may take it again from {time.strftime('%F %T', time.localtime(at))} (automatic retry; taskq retry {t['key']} for now)")
    ft = fail_tail(t["key"])
    if ft:
        on = f" on {ft['machine']}" if ft.get("machine") else ""
        print(f"  last failure {ft.get('time')}{on}, job {ft.get('job')}, {proto.TASK_LOGS}/{ft.get('log')}, its last {len(ft['lines'])} lines:")
        for ln in ft["lines"]:
            print(f"    | {ln}")
    return 0


# ---------------- status ----------------
def local_status():
    """Queue and each task's state (taskq status --json). Unreadable data dir -> DiskError."""
    tasks = read_tasks()
    F = facts(tasks)
    out = []
    tails = ST().names("failtail")
    for t in tasks:
        f = F.get(t["key"])
        st, note = state(t, f) if f else ("bad", t.get("bad", ""))
        s = spec_of(t)
        ci = claim_info(t["key"]) or {}
        sk, at = skip_state(t["key"])
        out.append(dict(t, spec=s, state=st, note=note, worker=ci.get("worker"), host=ci.get("host"), since=ci.get("since"),
                        assigned=ci.get("assigned"), input_since=(input_info(t["key"]) or (None,))[0],
                        beat=ci.get("beat"), log_age=f["log_age"] if f else None,
                        task_log_age=(f["tag_age"].get(str(ci.get("log") or ci.get("worker"))) if f and ci else None),
                        out_age=f["out_age"] if f and st == "claimed" else None, failures=failures(t["key"]),
                        skipped_by=sorted(sk), progress=has_progress(t, f),
                        fail_tail=fail_tail(t["key"]) if safe(t["key"]) in tails else None, retry_at=at))
    nf = recent_failures()
    day = [w for ts, w, _ in skips() if ts >= time.time() - 86400]
    return dict(enabled=ST().enabled(), tasks=out, recent_failures=nf, paused=nf >= proto.PAUSE_N,
                recent_skips={w: day.count(w) for w in set(day)})


def run(args):
    """taskq.py <args> in this process (argument list or a shell string) -> (exit code, stdout, stderr): the hub's own rounds
    edit the hub queue this way (relay, smoke_gate, advance)."""
    import contextlib
    import io
    argv = shlex.split(args) if isinstance(args, str) else [str(x) for x in args]
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            rc = main(argv)
        except SystemExit as e:
            rc = e.code if isinstance(e.code, int) or e.code is None else 1
            if not isinstance(e.code, int) and e.code is not None:
                print(e.code, file=sys.stderr)
    return rc or 0, out.getvalue(), err.getvalue()


def queue_host():
    """Host of the hub queue: hub from machines.py in the config dir; without a hub (a relay site's bundle) this host."""
    M = cfg.mtab()
    try:
        return M.hub()
    except LookupError:
        return here()


def here():
    M = cfg.mtab()
    return os.environ.get("TASKQ_MACHINE") or M.whoami()  # TASKQ_MACHINE for tests only


def cmd_status(a):
    if not (have_queue() or here() == queue_host()):
        print("cannot read the queue", file=sys.stderr)
        return 1
    try:
        s = local_status()
    except DiskError as e:
        print(f"cannot read queue state: {e}", file=sys.stderr)
        return 1
    if a.json:
        if not a.internal:
            s["tasks"] = [t for t in s["tasks"] if not proto.is_smoke(t["key"])]
        S.json_out({"version": proto.VERSION, **s})
        return 0
    print(queue_text(s, internal=a.internal))
    if bad_lines():
        print(bad_lines(), file=sys.stderr)
    return 0


def queue_text(s, internal=False):
    """A queue's status (local_status(), or an executor's read over ssh) as text, the one format of taskq status --queue and
    taskq site status: the head, one line per task not done (nice, key, env, state, note), counts per state; smoke tasks
    (taskq smoke status) are only counted unless internal."""
    tasks = [t for t in s["tasks"] if internal or not proto.is_smoke(t["key"])]
    smoke = len(s["tasks"]) - len(tasks)
    out = [f"master switch: {'on' if s.get('enabled') else 'off'}; {len(tasks)} tasks"
           + (f"; {s.get('recent_failures')} failures in the last {proto.hours(proto.PAUSE_WINDOW)}, claiming paused" if s.get("paused") else "")]
    n = {}
    for t in tasks:
        n[t["state"]] = n.get(t["state"], 0) + 1
        if t["state"] != proto.S_DONE:
            env = ",".join(S.envs_of(t.get("spec"))) or "?"
            why = tail_reason(t.get("fail_tail"))
            out.append((f"  {t['nice']:>5} {t['key']:60} {env:10} {t['state']} {t['note']}".rstrip()
                        + (f"; last failure: {why}" if why else "")))
    out.append("; ".join(f"{k} {v}" for k, v in sorted(n.items())) + (f"; {smoke} smoke (taskq smoke status)" if smoke else ""))
    return "\n".join(out)


def main(argv):
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    def named(name, alias):  # show the name used to call it in help and usage (taskq rm, taskq nice)
        return sub.add_parser(alias, aliases=[name]) if argv[:1] == [alias] else sub.add_parser(name, aliases=[alias])
    p = sub.add_parser("next")
    p.add_argument("--worker", required=True)
    p.add_argument("--free-slots", type=int, default=1)
    p.add_argument("--patience-until", type=float, default=0)
    p.add_argument("--exclude", nargs="*", default=[])
    p.add_argument("--envs", required=True, help="envs available on this machine, comma separated (machines.py in the config dir)")
    p.add_argument("--device", default="gpu", choices=["gpu", "cpu"], help="claim only tasks whose spec has this device")
    p.add_argument("--cpus", type=int, help="CPUs this worker gives each task; tasks with a larger res.cpus are not claimed (no check when omitted)")
    p.add_argument("--mem-mb", type=int, help="memory per task (MB); tasks with a larger res.mem are not claimed")
    p.add_argument("--gpu-mem", type=float, help="GPU memory (GB); tasks with a larger res.gpu_mem are not claimed")
    p.add_argument("--log-tag", help="job name in the part log (run_task.sh TASK_JOBTAG), given when it differs from --worker; recorded as log in the claim")
    tok = "third line printed by next; when given, touch only this round's claim"
    p = sub.add_parser("beat")
    p.add_argument("--worker", required=True)
    p.add_argument("--token", help=tok)
    p.add_argument("keys", nargs="+")
    p.add_argument("--strict", action="store_true", help=f"some claims are no longer ours: print those keys and exit {LOST}")
    p = sub.add_parser("release")
    p.add_argument("--worker", required=True)
    p.add_argument("--token", help=tok)
    p.add_argument("keys", nargs="+")
    p.add_argument("--lock-wait", type=float, help="wait at most this many seconds for the lock (default same as other commands; workers use a short one when stopped)")
    p.add_argument("--failed")
    p.add_argument("--skip", help="this machine cannot run it and will not claim it again (recorded in skips/, not a failure)")
    p = sub.add_parser("missing")
    p.add_argument("--worker", required=True)
    p.add_argument("--token", help=tok)
    p.add_argument("--why", default="")
    p.add_argument("key")
    p = sub.add_parser("input-ok")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("unskip")
    p.add_argument("--worker")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("forget")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("retry", description="let failed or skipped tasks run again: clear their skips, failures and failure tail here and on the executor holding them")
    p.add_argument("--worker", "--site", dest="worker", metavar="EXECUTOR", help="only that executor's skips and claim")
    p.add_argument("--exp", help="every task of this experiment")
    p.add_argument("--all-failed", action="store_true", help="every task with failures, a failure tail or a failure skip")
    p.add_argument("keys", nargs="*")
    p = sub.add_parser("take")
    p.add_argument("--worker", required=True)
    p.add_argument("--assign", action="store_true", required=True, help="assign to an executor: never expires")
    p.add_argument("key")
    p = sub.add_parser("add")
    p.add_argument("--nice", type=int, help="nice; with --exp, use the table's nice when omitted")
    p.add_argument("--exp")
    p.add_argument("--only", action="append", default=[], metavar="PARAM=VALUE[,VALUE...]")
    p.add_argument("--recipe")
    p.add_argument("--file", help="task sheet (JSON); recipe defaults to command when omitted")
    p.add_argument("--spec-stdin", action="store_true", help="read resolved specs from stdin (one or a list, the format of spec.py resolve; used by the hub's relay to send tasks)")
    p.add_argument("--update-only", action="store_true", help="only replace existing specs, do not add new ones (relay.py resend)")
    p.add_argument("args", nargs="*", help="--recipe k=v params")
    p = sub.add_parser("set")
    p.add_argument("key")
    p.add_argument("kv", nargs="+")
    p = named("set-nice", "nice")
    p.add_argument("--nice", type=int, required=True)
    p.add_argument("-n", "--dry-run", action="store_true", help="only print what would change, in this queue and in the executor queues the tasks were sent to")
    p.add_argument("keys", nargs="+")
    p = named("remove", "rm")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("stop")
    p.add_argument("--why", default="")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("resume")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("show")
    p.add_argument("--json", action="store_true")
    p.add_argument("key")
    p = sub.add_parser("status")
    p.add_argument("--json", action="store_true", help="one line of JSON (prefix proto.JSON_MARK): {version, enabled, tasks, ...} (proto.STATUS_TOP, STATUS_FIELDS; for cross-version reads)")
    p.add_argument("--internal", action="store_true", help="also list smoke tasks (keys starting with proto.SMOKE_PREFIX; the hub uses this when reading executor queues)")
    a, extra = ap.parse_known_args(argv)  # parse first: --help and usage errors never touch the queue
    if extra:  # usage lists only this subcommand's arguments
        sub.choices[a.cmd].error(f"unrecognized arguments: {' '.join(extra)}")
    cmd = {"nice": "set-nice", "rm": "remove"}.get(a.cmd, a.cmd)
    if cmd in proto.HUB_EDITS and not os.environ.get("TASKQ_DIR") and cfg.refuse_in_task(a.cmd, TQ):
        return X["no_queue"]
    if cmd in ("add", "set", "set-nice", "take", "show", "remove", "stop", "resume", "status") \
            and not have_queue() and here() != queue_host():
        print(f"the hub queue is on {queue_host()}, run it there (no {ST().where()} here)", file=sys.stderr)
        return X["no_queue"]
    return dict(next=cmd_next, beat=cmd_beat, release=cmd_release, take=cmd_take, add=cmd_add, set=cmd_set,
                set_nice=cmd_set_nice, show=cmd_show, remove=cmd_remove, status=cmd_status,
                stop=cmd_stop, resume=cmd_resume,
                missing=cmd_missing, input_ok=cmd_input_ok, unskip=cmd_unskip, forget=cmd_forget, retry=cmd_retry)[cmd.replace("-", "_")](a)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
