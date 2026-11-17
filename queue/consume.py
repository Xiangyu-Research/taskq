#!/usr/bin/env python3
"""Task queue consumer (worker): claim tasks from the queue, start run_task.sh, collect exit codes, heartbeat, stop and release.
Differences between executors (Slurm, resident pueue, GPU pad, several per GPU, waiting for a free GPU, hours window) live in the plugins under queue/consumers/ and in the user's consumers.py; see the consumer section in queue/README.md.

    python3 consume.py              (worker_start.sh → here)
    python3 consume.py --selftest   run in a new bundle before switching: exit 0 when imports, script syntax and the handoff format are fine
"""
import importlib.util
import json
import os
import re
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402

CORE = cfg.CORE
X = proto.EXIT
PY = sys.executable or "python3"
TASKQ = os.path.join(HERE, "taskq.py")
RUN_TASK = os.path.join(HERE, "run_task.sh")
BUILTIN = ("slurm", "resident", "gpu_pad", "window", "gpu_idle", "gpu_share")
HOOKS = ("active", "setup", "admit", "env", "before_start", "tick", "idle", "on_exit")
# what the worker does instead when a hook raises (Worker.guard; idle and get_end say it where they are called)
HOOK_FALLBACK = dict(active="using the plugin anyway, so its admit still decides", setup="keeping the plugin, its settings may be incomplete",
                     admit="not claiming now", before_start="starting anyway", env="env vars for run_task.sh unchanged",
                     tick="skipped this round", on_exit="going on with the exit")
HOOK_ERR_EVERY = 1800  # the same hook error is logged again at most this often (seconds)
CURRENT = os.path.basename(proto.ROOT["current"])


def ts():
    return time.strftime("%F %T")


def say(msg):
    print(f"{ts()} {msg}", flush=True)


def num(name, default):
    v = os.environ.get(name, "")
    try:
        f = float(v)
    except ValueError:
        return default
    return int(f) if f == int(f) else f


def out_of(cmd, timeout=60, any_rc=False, **kw):
    try:
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=timeout, **kw)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout if r.returncode == 0 or any_rc else None


def pstat(pid):
    """→ (state, ppid, process group, session); None when there is no such process."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            s = f.read()
    except OSError:
        return None
    r = s[s.rindex(")") + 2:].split()
    return r[0], int(r[1]), int(r[2]), int(r[3])


def procs():
    for d in os.listdir("/proc"):
        if d.isdigit():
            st = pstat(int(d))
            if st:
                yield int(d), st


def session_alive(sid):
    return bool(sid) and any(st[3] == sid and st[0] != "Z" for _, st in procs())


def tree(pid):
    """pid and its descendants, leaves first."""
    kids = {}
    for p, st in procs():
        kids.setdefault(st[1], []).append(p)
    out = []

    def walk(p):
        for c in kids.get(p, ()):
            walk(c)
        out.append(p)
    walk(pid)
    return out


def signame(rc):
    """Exit code of a death by signal (128 + n, as the shell and run_task.sh report it) → the signal's name; None for any other code."""
    try:
        return signal.Signals(rc - 128).name if isinstance(rc, int) and rc > 128 else None
    except ValueError:
        return None


def kill(pid, sig):
    if not isinstance(pid, int) or pid <= 1:  # 0 / 1 / negative would hit our own group or every process of the account
        return
    try:
        os.kill(pid, sig)
    except OSError:
        pass


def load(path, name):
    s = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(s)
    sys.modules[name] = m
    s.loader.exec_module(m)
    return m


def plugin_list():
    """→ [(name, plugin)]: the core consumers/ in BUILTIN order, then the user's CONSUMERS."""
    out = [(n, load(os.path.join(HERE, "consumers", n + ".py"), "taskq_consumer_" + n)) for n in BUILTIN]
    ns = {"CONSUMERS": {}}
    cfg.extend(ns, "consumers")
    return out + list(ns["CONSUMERS"].items())


def hook(p, name):
    return p.get(name) if isinstance(p, dict) else getattr(p, name, None)


class Run:
    """A run_task.sh and the training it started."""

    def __init__(self, pid, key, proc=None, started=None, outdir="", safe="", jtag="", stalled=None, why="", sid=None):
        self.pid, self.key, self.proc = pid, key, proc
        self.started = started if started is not None else time.time()
        self.outdir, self.safe, self.jtag = outdir or "", safe or proto.safe(key), jtag or ""
        self.stalled, self.why, self.sid = stalled, why or "", sid
        self.ended, self.rc = False, None


class Worker:
    def __init__(self):
        self.t0 = int(time.time())
        self.repo = cfg.REPO
        self.qdir = os.environ.get("TASKQ_DIR") or cfg.state("taskq")
        self.device = os.environ.get(proto.RUN_VARS["device"]) or "gpu"
        self.W = os.environ.get(proto.RUN_VARS["worker"], "")
        self.host = socket.gethostname().split(".")[0]
        self._m = None
        self.data = cfg.data_dir()
        os.environ["TASKQ_DATA"] = self.data
        self.envs = os.environ.get("TASKQ_ENVS") or ",".join(self.mget("envs").split())
        try:
            self.ncpu = len(os.sched_getaffinity(0))
        except (AttributeError, OSError):
            self.ncpu = os.cpu_count() or 1
        self.mem_mb = 0
        self.slots = None
        self.end, self.end_text, self.get_end = 0, "none", None
        self.nvsel = []
        self.tag_each = False
        self.restart_on_change = False
        self.term_rc, self.term_why = 0, "pueue kill"
        self.pad_now = lambda: 0
        self.ADD_AFTER = num("TASKQ_ADD_AFTER", 900)
        self.UTIL_MAX = num("TASKQ_UTIL_MAX", 90)
        self.MIN_LEFT = num("TASKQ_MIN_LEFT", 3600)
        self.PATIENCE = num("TASKQ_PATIENCE", 0 if self.device == "cpu" else 1200)
        self.ERR_MAX = num("TASKQ_ERR_MAX", 600)
        self.IDLE_MAX = num("TASKQ_IDLE_MAX", 2400)
        self.IDLE_POLL = num("TASKQ_IDLE_POLL", 60)
        self.IDLE_EXIT = num("TASKQ_IDLE_EXIT", 1800)
        self.MAX_OWN_FAIL = 2
        self.EARLY_FAIL = num("TASKQ_EARLY_FAIL", 600)
        self.FAIL_WINDOW = num("TASKQ_FAIL_WINDOW", 3600)
        self.FAIL_RETRY = num("TASKQ_FAIL_RETRY", proto.FAIL_RETRY)
        self.STALL_MAX = num("TASKQ_STALL_MAX", 1800)
        self.STALL_FAILS = 2
        self.GIVEUP = num("TASKQ_GIVEUP", 180)
        self.BEAT = num("TASKQ_BEAT", proto.BEAT)
        self.STOP_POLL = num("TASKQ_STOP_POLL", 5)
        self.STOP_WAIT = num("TASKQ_STOP_WAIT", 20)
        self.TERM_WAIT = num("TASKQ_TERM_WAIT", 15)
        self.POLL = num("TASKQ_POLL", 30)
        self.runs, self.zomb = {}, {}
        self.tok, self.later, self.excl, self.fail_at = {}, {}, [], []
        self.changed = self.t0
        self.own = 0
        self.why_not = ""
        self.err = ""
        self.blocked = ""
        self.stopping = False
        self.bad = set()
        self.mon_stop = threading.Event()
        self.plugins = []
        self.hook_err = {}  # (plugin, hook) → (last error, when it was logged)
        self.end_by = "?"   # the plugin that set get_end
        try:
            cands = plugin_list()
        except Exception as e:  # noqa: BLE001
            say(f"!! cannot read consumer plugins ({e}); using the core ones only")
            cands = [(n, load(os.path.join(HERE, "consumers", n + ".py"), "taskq_consumer_" + n)) for n in BUILTIN]
        for name, p in cands:
            a = hook(p, "active")
            if a is not None:
                ok, on = self.guard(name, "active", a, self)
                if ok and not on:
                    continue
            self.plugins.append((name, p))
            s = hook(p, "setup")
            if s:
                had = self.get_end
                self.guard(name, "setup", s, self)
                if self.get_end is not had:
                    self.end_by = name
        if not self.W:
            print(f"set {proto.RUN_VARS['worker']} (a worker in machines.py in the config dir)", file=sys.stderr)
            sys.exit(1)
        if self.slots is None or self.device == "cpu":
            self.slots = 1
        self.task_cpus = max(1, self.ncpu // self.slots)
        self.logdir = os.path.join(self.data, proto.WORKER_LOGS)
        os.makedirs(self.logdir, exist_ok=True)
        self.gpucsv = os.path.join(self.logdir, f"worker_{self.W}_gpu.csv")
        self.clean_old()
        self.commit = self.code_name()
        self.head0 = self.head_now()
        self.idle_since = 0
        if self.bundle_cur():
            self.touch_repo()
        self.adopt()

    # ---------- machine table, code ----------
    def mget(self, field):
        try:
            if self._m is None:
                self._m = cfg.mtab()
            return self._m._value(self._m.whoami(), field)
        except Exception:  # noqa: BLE001
            return ""

    def code_name(self):
        top = (out_of(["git", "-C", self.repo, "rev-parse", "--show-toplevel"]) or "").strip()
        if top and top == os.path.realpath(self.repo):
            c = (out_of(["git", "-C", self.repo, "rev-parse", "--short", "HEAD"]) or "").strip() or "unknown"
            try:
                d = cfg.outputs("code_dirs")
            except Exception:  # noqa: BLE001
                d = None
            d = [d] if isinstance(d, str) else list(d or [])
            if d and subprocess.run(["git", "-C", self.repo, "diff", "--quiet", "HEAD", "--", *d],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
                c += "+dirty"
            return c
        return os.path.basename(os.path.realpath(self.repo))

    def bundle_cur(self):
        c = os.path.join(os.path.dirname(self.repo), CURRENT)
        return os.path.realpath(c) if os.path.islink(c) else None

    def new_core(self):
        cur = self.bundle_cur()
        if not cur:
            return None
        rel = os.path.relpath(CORE, self.repo) if CORE.startswith(self.repo.rstrip("/") + "/") else ""
        c = os.path.normpath(os.path.join(cur, rel))
        return c if os.path.isfile(os.path.join(c, "queue", "worker_start.sh")) else None

    def head_now(self):
        cmd = os.environ.get("TASKQ_HEAD_CMD")
        if cmd:
            return (out_of(["bash", "-c", cmd]) or "").strip()
        cur = self.bundle_cur()
        if cur:
            return cur
        return (out_of(["git", "-C", self.repo, "rev-parse", "HEAD"]) or "").strip()

    def code_changed(self, head):
        if not head or head == self.head0 or head in self.bad:
            return False
        return bool(self.bundle_cur()) or self.restart_on_change

    def touch_repo(self):
        try:
            os.utime(self.repo)
        except OSError:
            pass

    def clean_old(self):
        cut = time.time() - 7 * 86400
        try:
            for e in os.scandir(self.logdir):
                if re.fullmatch(r"worker_.*_next\.err", e.name) and e.stat().st_mtime < cut:
                    os.unlink(e.path)
        except OSError:
            pass

    # ---------- plugins ----------
    def hooks(self, name):
        """→ (plugin name, hook) for each active plugin that has hook name, in order."""
        for pname, p in self.plugins:
            f = hook(p, name)
            if f:
                yield pname, f

    def guard(self, pname, hname, f, *args, fallback=None):
        """Call one plugin hook → (True, its result), or (False, None) when it raised: a broken plugin never stops the worker, the caller
        falls back. The error is logged with the plugin and hook names and what the worker does instead (fallback, else
        HOOK_FALLBACK); the same error again at most every HOOK_ERR_EVERY seconds."""
        k = (pname, hname)
        try:
            r = f(*args)
        except Exception as e:  # noqa: BLE001
            err, now = f"{type(e).__name__}: {e}", time.time()
            last = self.hook_err.get(k)
            if not last or last[0] != err or now - last[1] >= HOOK_ERR_EVERY:
                say(f"!! consumer plugin {pname}: {hname} failed ({err}); {fallback or HOOK_FALLBACK.get(hname, 'going on')}")
                self.hook_err[k] = (err, now)
            return False, None
        if self.hook_err.pop(k, None):
            say(f"consumer plugin {pname}: {hname} works again")
        return True, r

    def end_of(self):
        """Time limit from the plugin that set get_end (epoch seconds); 0 when unknown or it failed (asked again next round)."""
        if not self.get_end:
            return 0
        ok, end = self.guard(self.end_by, "get_end", self.get_end, self, fallback="time limit unknown for now, asking again next round")
        return (end or 0) if ok else 0

    def idle_wait(self, why, now, err_since):
        """Nothing running: the first plugin with an idle hook decides → seconds to sleep, None = exit. When that hook fails: check
        again in POLL seconds and exit once idle for IDLE_MAX (not while unstoppable training is still followed)."""
        for pname, f in self.hooks("idle"):
            ok, d = self.guard(pname, "idle", f, self, why, now, self.idle_since, err_since,
                               fallback=f"checking again in {self.POLL} seconds, exiting once idle for {self.IDLE_MAX // 60} minutes")
            if ok:
                return d
            return self.POLL if self.zomb or now - self.idle_since < self.IDLE_MAX else None
        return None

    def say(self, msg):
        say(msg)

    def busy(self):
        return len(self.runs)

    def left(self):
        return 999999 if not self.end else self.end - time.time()

    # ---------- queue ----------
    def tq(self, c, key, *args, quiet=False):
        tok = self.tok.get(key)
        cmd = [PY, TASKQ, c, "--worker", self.W, *(["--token", tok] if tok else []), *args, "--", key]  # -- : a name is never an option
        sys.stdout.flush()
        return subprocess.run(cmd, stdout=subprocess.DEVNULL if quiet else None).returncode

    def exclude(self, key):
        if key not in self.excl:
            self.excl.append(key)

    def retry_later(self, key):
        self.later[key] = time.time() + self.FAIL_RETRY
        say(f"{key}: this job claims it again in {self.FAIL_RETRY // 60} minutes")

    def own_fails(self):
        now = time.time()
        self.fail_at = [t for t in self.fail_at if now - t < self.FAIL_WINDOW]
        self.own = len(self.fail_at)

    def fail(self, key, why, ran):
        self.tq("release", key, "--failed", why)
        self.retry_later(key)
        if ran >= self.EARLY_FAIL:
            return
        self.fail_at.append(time.time())
        self.own_fails()
        if self.own >= self.MAX_OWN_FAIL:
            say(f"{self.own} tasks failed shortly after starting on this job within {self.FAIL_WINDOW // 60} minutes; not claiming new ones (likely a problem on this node or its env)")

    def task_log(self, r):
        return os.path.join(self.data, proto.TASK_LOGS, f"{r.safe}{proto.SAFE}{r.jtag or self.W}_{proto.LOG_PART}1.log")

    def stalls(self, safe):
        d, n = os.path.join(self.data, proto.TASK_LOGS), 0
        cut = time.time() - 7 * 86400
        try:
            for e in os.scandir(d):
                if e.name.startswith(safe + proto.SAFE) and e.stat().st_mtime > cut:
                    with open(e.path, errors="replace") as f:
                        n += sum(1 for ln in f if ln.startswith(proto.STALL_LINE))
        except OSError:
            pass
        return n

    def release_stalled(self, key, n, ran):
        if n >= self.STALL_FAILS:
            say(f"{key} stalled again this week ({n} times); recording a failure")
            self.fail(key, f"stalled ({n} times this week)", ran)
        else:
            self.tq("release", key)
            self.retry_later(key)

    def quiet_since(self, r):
        """Seconds since the task's log or output dir last changed; unreadable → None."""
        now = int(time.time())
        m = (out_of(["timeout", "20", "stat", "-c", "%Y", self.task_log(r)]) or "").strip()
        if not m.isdigit():
            return None
        age = now - int(m)
        if age < self.STALL_MAX or not (r.outdir and os.path.isdir(r.outdir)):
            return age
        hit = out_of(["timeout", "60", "find", r.outdir, "-newermt", f"@{now - self.STALL_MAX}", "-print", "-quit"])
        if hit is None:
            return None
        return 0 if hit.strip() else age

    # ---------- processes ----------
    def ended(self, r):
        """Whether run_task.sh exited (exit code in r.rc; None when another process reaped it during a handoff)."""
        if r.ended:
            return True
        if r.proc is not None:
            rc = r.proc.poll()
            if rc is None:
                return False
        else:
            try:
                p, st = os.waitpid(r.pid, os.WNOHANG)
                if p == 0:
                    return False
                rc = os.waitstatus_to_exitcode(st)
            except ChildProcessError:
                s = pstat(r.pid)
                if s and s[0] != "Z":
                    return False
                rc = None
        if rc is not None and rc < 0:
            rc = 128 - rc
        r.ended, r.rc = True, rc
        return True

    def sid_of(self, r):
        if r.sid:
            return
        for p, st in procs():
            if st[1] == r.pid and st[3] == p:
                r.sid = p
                return

    def alive(self, r):
        return not self.ended(r) or session_alive(r.sid)

    def stop_tree(self, r, sig=signal.SIGTERM):
        """Signal the training's process group and session first, then run_task.sh and anything outside the session along the process tree."""
        if not r.ended:
            self.sid_of(r)
        if isinstance(r.sid, int) and r.sid > 1:  # killpg(1) is kill(-1): every process of the account
            try:
                os.killpg(r.sid, sig)
            except OSError:
                pass
            subprocess.run(["pkill", f"-{sig.name[3:]}", "-s", str(r.sid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if not r.ended:
            for p in tree(r.pid):
                kill(p, sig)

    def all_runs(self):
        return list(self.runs.values()) + list(self.zomb.values())

    def any_alive(self):
        return any(self.alive(r) for r in self.all_runs())

    def let_go(self):
        for r in self.all_runs():
            if r.why == "lost":
                continue
            if not self.alive(r):
                if self.tq("release", r.key, "--lock-wait", "5") != 0:
                    say(f"{r.key} not released (cannot get the queue lock); once the claim expires someone else can take it")
            else:
                say(f"{r.key} still running after the force kill; not releasing (someone else can take it only after the claim expires)")

    def finish(self):
        self.mon_stop.set()
        for pname, f in self.hooks("on_exit"):
            self.guard(pname, "on_exit", f, self)

    def shutdown(self, msg, rc):
        """Stop everything running, release what is confirmed stopped, run on_exit, then exit rc (whatever fails on the way)."""
        if self.stopping:
            return
        self.stopping = True
        try:
            for s in (signal.SIGTERM, signal.SIGINT):
                signal.signal(s, signal.SIG_IGN)
            say(msg)
            for r in self.all_runs():
                self.stop_tree(r)
            for _ in range(int(self.TERM_WAIT)):
                if not self.any_alive():
                    break
                time.sleep(1)
            if self.any_alive():
                say(f"did not stop within {self.TERM_WAIT} seconds; forcing")
                for r in self.all_runs():
                    if self.alive(r):
                        self.stop_tree(r, signal.SIGKILL)
                for _ in range(3):
                    if not self.any_alive():
                        break
                    time.sleep(1)
            self.let_go()
        except Exception as e:  # noqa: BLE001
            say(f"!! stopping failed ({type(e).__name__}: {e}); tasks not released yet can be taken once their claim expires")
        finally:
            self.finish()
            sys.stdout.flush()
            os._exit(rc)

    def on_signal(self, *_):
        self.shutdown(f"got stop signal ({self.term_why}); stopping training and releasing tasks", self.term_rc)

    # ---------- stop a task (taskq.py stop) ----------
    def check_stops(self):
        n = False
        for pid, r in list(self.runs.items()):
            if r.stalled or not os.path.exists(os.path.join(self.qdir, proto.QUEUE["stopped"], proto.safe(r.key))):
                continue
            say(f"{r.key} stopped (taskq.py stop); stopping training and releasing")
            self.stop_tree(r)
            for _ in range(int(self.STOP_WAIT)):
                if not self.alive(r):
                    break
                time.sleep(1)
            if self.alive(r):
                say(f"{r.key} did not stop within {self.STOP_WAIT} seconds; forcing")
                self.stop_tree(r, signal.SIGKILL)
                time.sleep(1)
            if not self.alive(r):
                self.tq("release", r.key)
                del self.runs[pid]
                n = True
            else:
                say(f"{r.key} still running after the force kill; treating it as unstoppable")
                r.stalled, r.why = r.stalled or time.time(), "stop"
        return n

    def nap(self, secs=None):
        end = time.time() + (self.POLL if secs is None else secs)
        while True:
            d = end - time.time()
            if d <= 0:
                break
            if self.check_stops():
                return
            time.sleep(min(d, self.STOP_POLL))
        self.check_stops()

    # ---------- GPU records ----------
    def nvsmi(self, *q, timeout=60):
        return out_of(["nvidia-smi", *self.nvsel, *q], timeout=timeout)

    def monitor(self):
        while not self.mon_stop.is_set():
            try:
                with open(self.gpucsv, "a") as f:
                    subprocess.run(["nvidia-smi", *self.nvsel, "--query-gpu=timestamp,index,memory.used,memory.total,utilization.gpu",
                                    "--format=csv,noheader"], stdout=f, stderr=subprocess.DEVNULL, timeout=60)
            except (OSError, subprocess.SubprocessError):
                pass
            self.mon_stop.wait(60)

    # ---------- starting training ----------
    def can_add(self):
        """→ (whether another task can be claimed, what blocks it); with training running the reason goes to why_not."""
        n = len(self.runs)
        self.why_not = ""
        if n >= self.slots:
            self.why_not = f"already {n} running"
            return False, self.why_not
        for pname, f in self.hooks("admit"):
            ok, why = self.guard(pname, "admit", f, self)
            if not ok:
                why = f"consumer plugin {pname}: admit failed"
            if why:
                if n:
                    self.why_not = why
                return False, why
        return True, None

    def start(self, out, jt):
        lines = (out.split("\n") + ["", "", ""])[:3]
        key, args, tok = lines
        self.tok[key] = tok
        for pname, f in self.hooks("before_start"):
            self.guard(pname, "before_start", f, self)
        V = proto.RUN_VARS
        env = dict(os.environ, **{V["key"]: key, V["jobtag"]: jt, V["part"]: "1", V["cpus"]: str(self.task_cpus)}, TASKQ_ADMITTED="1")
        for pname, f in self.hooks("env"):
            e = dict(env)  # a hook that fails half way leaves nothing behind
            if self.guard(pname, "env", f, self, e)[0]:
                env = e
        sys.stdout.flush()
        try:
            # run_task.sh (and spec.py from it) get the arguments on stdin, from a file only this account can read that is already
            # unlinked: the spec (tokens in vars) never goes on a command line, which every user of the node can read
            arg = json.dumps(shlex.split(args), ensure_ascii=False)
            with tempfile.TemporaryFile("w+", encoding="utf-8") as f:
                f.write(arg)
                f.flush()
                f.seek(0)
                proc = subprocess.Popen(["bash", RUN_TASK, "--stdin"], stdin=f, env=env)
        except (OSError, ValueError, TypeError) as e:  # TypeError: an env hook put in a value that is not a string
            say(f"!! {key}: cannot start run_task.sh ({type(e).__name__}: {e}); releasing and recording a failure")
            self.fail(key, f"cannot start run_task.sh: {type(e).__name__}: {e}", 0)
            return
        r = Run(proc.pid, key, proc=proc, jtag=jt)  # registered before anything else can fail: the run is always followed
        self.runs[proc.pid] = r
        self.changed = int(time.time())
        say(f"started {key} (pid {proc.pid})")  # the task name only: the spec does not go into logs either
        sh = out_of([PY, os.path.join(HERE, "spec.py"), "shell", "--stdin"], any_rc=True, input=arg) or ""
        out_var, safe_var = proto.SHELL_VARS["out"], proto.SHELL_VARS["safe"]
        for ln in sh.split("\n"):
            m = re.match(rf"({out_var}|{safe_var})=(.*)$", ln)
            if m:
                try:
                    v = (shlex.split(m.group(2)) or [""])[0]
                except ValueError:
                    v = ""
                if m.group(1) == out_var:
                    r.outdir = v
                elif v:
                    r.safe = v

    def claim(self, jt):
        now = time.time()
        self.later = {k: t for k, t in self.later.items() if t > now}
        res = ["--cpus", str(self.task_cpus)]
        if self.mem_mb > 0:
            res += ["--mem-mb", str(self.mem_mb // self.slots)]
        if self.gpu_gb:
            res += ["--gpu-mem", str(self.gpu_gb)]
        cmd = [PY, TASKQ, "next", "--worker", self.W, *(["--log-tag", jt] if self.tag_each else []), "--envs", self.envs,
               "--device", self.device, *res, "--free-slots", "1", "--patience-until", str(self.t0 + self.PATIENCE),
               "--exclude", *self.excl, *self.later]
        try:
            p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except OSError as e:
            return 127, "", str(e)
        return p.returncode, p.stdout, p.stderr

    # ---------- bundle switch ----------
    def handoff(self):
        tasks = [dict(pid=r.pid, key=r.key, token=self.tok.get(r.key) or None, started=r.started, outdir=r.outdir, safe=r.safe,
                      jtag=r.jtag, stalled=r.stalled, why=r.why, sid=r.sid, zomb=z, ended=r.ended)
                 for z, rs in ((False, self.runs), (True, self.zomb)) for r in rs.values()]
        return dict(v=proto.HANDOFF, t0=self.t0, changed=self.changed, excl=self.excl, later=self.later, fail_at=self.fail_at,
                    idle_since=0 if self.runs else self.idle_since, tasks=tasks)

    def adopt(self):
        h = os.environ.pop("TASKQ_HANDOFF", None)
        if h:
            try:
                d = json.loads(h)
                if d.get("v") != proto.HANDOFF:
                    raise ValueError(f"version {d.get('v')}")
            except ValueError as e:
                say(f"!! cannot read the handoff ({e}); running tasks were not adopted, and once their claim expires someone else can take them")
                d = {}
            self.t0 = int(d.get("t0") or self.t0)
            self.changed = int(d.get("changed") or self.changed)
            self.excl = list(d.get("excl") or [])
            self.later = dict(d.get("later") or {})
            self.fail_at = list(d.get("fail_at") or [])
            s = int(d.get("idle_since") or 0)
            if self.t0 <= s <= time.time():
                self.idle_since = s
                say(f"idle time carried over: idle for {int(time.time()) - s} s")
            for t in d.get("tasks") or []:
                r = Run(int(t["pid"]), t["key"], started=t.get("started"), outdir=t.get("outdir"), safe=t.get("safe"),
                        jtag=t.get("jtag"), stalled=t.get("stalled"), why=t.get("why"), sid=t.get("sid"))
                r.ended = bool(t.get("ended"))
                if t.get("token"):
                    self.tok[r.key] = t["token"]
                if t.get("zomb"):
                    self.zomb[r.pid] = r
                    say(f"following unstoppable {r.key} (pid {r.pid})")
                else:
                    self.runs[r.pid] = r
                    say(f"taking over {r.key} (pid {r.pid})")

    def selftest(self, core):
        try:
            r = subprocess.run([PY, os.path.join(core, "queue", "consume.py"), "--selftest"], stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, timeout=60)
        except (OSError, subprocess.SubprocessError) as e:
            return False, str(e)
        last = (r.stdout.strip().split("\n") or [""])[-1][:300]
        return r.returncode == 0, last or f"exit code {r.returncode}"

    def switch(self, head):
        """The bundle changed: selftest the new bundle, exec its worker_start.sh when it passes and hand over the running tasks; False when it cannot switch."""
        core = self.new_core()
        if not core:
            return False
        name = os.path.basename(self.bundle_cur())
        env = dict(os.environ)
        ok, why = self.selftest(core)
        if not ok:
            say(f"new bundle {name} failed its selftest ({why}); staying on {self.commit}")
            self.bad.add(head)
            return False
        env["TASKQ_HANDOFF"] = json.dumps(self.handoff(), ensure_ascii=False)
        say("code updated")
        n = len(self.runs)
        say(f"switching to new code {name} and continuing" + (f" ({n} running handed to the new code)" if n else ""))
        self.mon_stop.set()
        sys.stdout.flush()
        try:
            os.execvpe("bash", ["bash", os.path.join(core, "queue", "worker_start.sh")], env)
        except OSError as e:
            say(f"!! cannot switch to new code ({e}); staying on {self.commit}")
            self.bad.add(head)
        return False

    # ---------- main loop ----------
    def reap(self, now):
        for pid, r in list(self.runs.items()):
            if not self.ended(r):
                self.sid_of(r)
                continue
            if r.stalled and session_alive(r.sid):
                continue
            rc, key = r.rc, r.key
            ran = now - r.started
            del self.runs[pid]
            if r.why == "lost":
                say(f"{key} stopped (the claim is no longer this job's; not releasing)")
            elif r.why == "stall":
                say(f"stalled {key} stopped; releasing, this job will not claim it again")
                self.release_stalled(key, self.stalls(r.safe), ran)
            elif r.stalled:
                say(f"{key} stopped; releasing")
                self.tq("release", key)
            # every stop this worker sends is marked on the run above (why stall / lost, stalled) or released where it is sent (shutdown,
            # taskq stop); a signal death from here on came from elsewhere (an out-of-memory killer, an admin, ...) and counts as a
            # failure, so a task killed every time reaches the failure limit instead of being retried forever
            elif session_alive(r.sid) and (rc is None or rc > 128):
                subprocess.run(["pkill", "-TERM", "-s", str(r.sid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if rc is None:
                    say(f"{key} ended during the code switch (exit code not received) but training is still running: stopping it and releasing once it stops (not a failure); this job will not claim it again")
                    r.why = "killed"
                else:
                    say(f"{key}'s run_task.sh was killed ({signame(rc) or f'exit code {rc}'}), not by this worker, but training is still running: stopping it, then releasing and recording a failure; this job will not claim it again")
                    r.why = "signal"
                self.zomb[pid] = r
                self.exclude(key)
            elif rc is None:
                say(f"{key} ended during the code switch and its exit code was not received: releasing (not a failure)")
                self.tq("release", key)
            elif rc == X["run_done"]:
                say(f"done {key}")
                self.tq("release", key)
            elif rc == X["run_missing"]:
                say(f"{key} has missing inputs and did not start; releasing and recording missing inputs (not a failure, not claimed until they are there)")
                if self.tq("missing", key, "--why", f"job {self.W} checked before start") != 0:
                    self.tq("release", key)
                self.exclude(key)
            elif rc == X["run_unreadable"]:
                say(f"{key}: disk unreadable, cannot tell whether training finished; releasing (not a failure), this job will claim it again in 10 minutes")
                self.tq("release", key)
                self.later[key] = time.time() + 600
            elif rc == X["run_not_done"]:
                say(f"{key} command ended normally but training is not done; releasing and recording a failure")
                self.fail(key, "exit code 0 but not done", ran)
            elif signame(rc):  # run_term (143) included: run_task.sh or the training got SIGTERM, but not from this worker
                say(f"{key} was killed by {signame(rc)} (run_task.sh exit code {rc}), not stopped by this worker (an out-of-memory killer, an admin, ...); releasing and recording a failure")
                self.fail(key, f"killed by {signame(rc)} (exit code {rc}), not stopped by the worker", ran)
            else:
                say(f"{key} training failed (run_task.sh exit code {rc}); releasing and recording a failure")
                extra = ""
                if rc == X["run_bad"]:
                    try:
                        with open(self.task_log(r), errors="replace") as f:
                            last = (f.read().rstrip("\n").split("\n") or [""])[-1]
                        extra = (": " + re.sub(r"^[-0-9: ]*", "", last))[:300] if last else ""
                    except OSError:
                        pass
                self.fail(key, f"exit code {rc}{extra}", ran)
        for pid, r in list(self.zomb.items()):
            if self.alive(r):
                continue
            del self.zomb[pid]
            if r.why == "lost":
                say(f"unstoppable {r.key} finally stopped (the claim is no longer this job's; not releasing)")
            elif r.why == "stall":
                say(f"unstoppable {r.key} finally stopped; releasing")
                self.release_stalled(r.key, self.stalls(r.safe), 0)
            elif r.why == "signal":
                sig = signame(r.rc) or "a signal"
                say(f"{r.key} finally stopped; releasing and recording a failure (its run_task.sh was killed by {sig}, not by this worker)")
                self.fail(r.key, f"run_task.sh killed by {sig} while training ran, not stopped by the worker", now - r.started)
            else:
                say(f"unstoppable {r.key} finally stopped; releasing")
                self.tq("release", r.key)

    def watch_stalls(self, now):
        for pid, r in list(self.runs.items()):
            if r.stalled:
                if now - r.stalled >= self.GIVEUP:
                    say(f"{r.key} has not stopped for {self.GIVEUP} seconds (likely stuck in I/O); claiming other tasks for now, still heartbeating, releasing once it stops")
                    self.exclude(r.key)
                    self.zomb[pid] = self.runs.pop(pid)
                elif now - r.stalled >= self.GIVEUP // 3:
                    self.stop_tree(r, signal.SIGKILL)
                continue
            if now - r.started < self.STALL_MAX:
                continue
            q = self.quiet_since(r)
            if q is None or q < self.STALL_MAX:
                continue
            say(f"{r.key}: log and output dir unchanged for {q // 60} minutes; treating it as stalled: stopping, releasing, claiming the next")
            try:
                with open(self.task_log(r), "a") as f:
                    f.write(f"{proto.STALL_LINE} {ts()}: log and output dir unchanged for {q // 60} minutes, job {self.W} stopped it\n")
            except OSError:
                pass
            r.stalled, r.why = now, "stall"
            self.stop_tree(r)

    def beat(self, now):
        for r in self.all_runs():
            if r.why == "lost":
                continue
            if self.tq("beat", r.key, "--strict", quiet=True) != X["lost"]:
                continue
            say(f"{r.key}: claim is no longer this job's (taken over after expiry, or gone); stopping this training and not releasing")
            r.why = "lost"
            self.exclude(r.key)
            if r.pid in self.zomb or r.stalled:
                continue
            r.stalled = now
            self.stop_tree(r)
        if self.bundle_cur():
            self.touch_repo()

    def run(self):
        """The main loop. Anything unexpected in it goes through the normal shutdown path (stop the runs, release what is confirmed
        stopped, on_exit) and exits 1, instead of dying with claims held and training left unattended."""
        try:
            return self.loop()
        except BaseException as e:  # noqa: BLE001
            traceback.print_exc(file=sys.stdout)
            self.shutdown(f"!! worker error ({type(e).__name__}: {e}); stopping training and releasing tasks", 1)
            raise  # only when already shutting down (shutdown exits)

    def loop(self):
        if self.get_end:
            self.end = self.end_of()
        lim = time.strftime("%F %T", time.localtime(self.end)) if self.end else self.end_text
        say(f"task queue worker ({self.device}): job {self.W} @ {self.host}, at most {self.slots} at once, CPU {self.ncpu}, "
            f"memory {self.mem_mb}M ({self.task_cpus} CPUs per training), envs {self.envs}, time limit {lim}, code {self.commit}")
        self.gpu_gb = None
        if self.device == "gpu":
            sys.stdout.flush()
            try:
                subprocess.run(["nvidia-smi", *self.nvsel, "--query-gpu=index,name,memory.total", "--format=csv,noheader"],
                               stderr=subprocess.DEVNULL, timeout=60)  # record the GPU model in the log
            except (OSError, subprocess.SubprocessError):
                pass
            threading.Thread(target=self.monitor, daemon=True).start()
            g = ((self.nvsmi("--query-gpu=memory.total", "--format=csv,noheader,nounits") or "").split("\n")[0]).strip()
            if g.isdigit():
                self.gpu_gb = (int(g) + 512) // 1024
        signal.signal(signal.SIGTERM, self.on_signal)
        signal.signal(signal.SIGINT, self.on_signal)
        restart = False
        last_beat = time.time()
        err_since = 0
        self.idle_since = self.idle_since or int(time.time())
        last_why = ""
        while True:
            now = time.time()
            self.reap(now)
            now = time.time()
            self.watch_stalls(now)
            if not self.end:
                self.end = self.end_of()
            if now - last_beat >= self.BEAT:
                self.beat(now)
                last_beat = now
            for pname, f in self.hooks("tick"):
                self.guard(pname, "tick", f, self)
            head = self.head_now()
            changed = self.code_changed(head)
            why, tried = None, False
            self.own_fails()
            while self.left() > self.MIN_LEFT and self.own < self.MAX_OWN_FAIL:
                ok, block = self.can_add()
                if not ok:
                    if not self.runs:
                        why, self.blocked = "admit", block
                    break
                if changed:
                    self.switch(head)
                    changed = self.code_changed(head)
                    break
                tried = True
                jt = f"{self.W}-{time.strftime('%Y%m%d%H%M%S')}" if self.tag_each else self.W  # several workers on one machine starting in the same minute do not collide
                rc, out, err = self.claim(jt)
                if rc == X["next_ok"]:
                    err_since = 0
                    self.start(out, jt)
                    continue
                if rc in (X["nothing"], X["wait"], X["paused"]):
                    err_since = 0
                    why = {X["nothing"]: "nothing", X["wait"]: "wait", X["paused"]: "paused"}[rc]
                    break
                self.err = (err.strip().split("\n") or [""])[-1]
                if not err_since:
                    err_since = now
                    say(f"queue error (taskq.py exit code {rc}: {self.err}); retrying every round")
                why = "error"
                break
            if not tried:
                err_since = 0
            if self.why_not and self.why_not != last_why:
                say(f"not adding more training: {self.why_not}")
            last_why = self.why_not
            if self.runs:
                self.idle_since = 0
            else:
                self.idle_since = self.idle_since or int(now)
                if changed:
                    if self.new_core():
                        self.switch(head)
                        changed = self.code_changed(head)
                    elif self.restart_on_change and not self.zomb:
                        say("code updated")
                        restart = True
                        break
                d = self.idle_wait(why, now, err_since)
                if d is None:
                    break
                self.nap(d)
                continue
            self.nap()
        self.let_go()
        self.finish()
        if restart:
            say("exiting; relay.py restarts it with the new code")
        sys.stdout.flush()
        return 0


def selftest():
    for n in BUILTIN:
        load(os.path.join(HERE, "consumers", n + ".py"), "taskq_consumer_" + n)
    ns = {"CONSUMERS": {}}
    cfg.extend(ns, "consumers")
    for s in ("run_task.sh", "worker_start.sh"):
        subprocess.run(["bash", "-n", os.path.join(HERE, s)], check=True)
    for s in ("taskq.py", "spec.py"):
        with open(os.path.join(HERE, s)) as f:
            compile(f.read(), s, "exec")
    w = Worker.__new__(Worker)
    w.t0 = w.changed = 1
    w.excl, w.later, w.fail_at, w.tok, w.idle_since = ["a/c"], {}, [], {"a/b": "t"}, 0
    w.runs, w.zomb = {1: Run(1, "a/b", sid=2, why="stall")}, {}
    h = json.loads(json.dumps(w.handoff()))
    assert set(h) == set(proto.HANDOFF_TOP) and set(h["tasks"][0]) == set(proto.HANDOFF_TASK) and h["tasks"][0]["token"] == "t"
    print("selftest passed")
    return 0


def main(argv):
    if argv[:1] == ["--selftest"]:
        try:
            return selftest()
        except Exception as e:  # noqa: BLE001
            print(f"selftest failed: {type(e).__name__}: {e}")
            return 1
    return Worker().run()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
