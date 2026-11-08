#!/usr/bin/env python3
"""任务队列的消费者（worker）：从队列领任务、开 run_task.sh、收退出码、心跳、停、交还。
不同执行机的差别（Slurm、pueue 常驻、显存填充、一张卡几个、等卡空、时段）在插件 queue/consumers/ 和使用者的 consumers.py 里。
说明见 queue/README.md 的"消费者"。

    python3 consume.py              （worker_start.sh → 这里）
    python3 consume.py --selftest   换代码包前在新包里跑：import、脚本语法、交接格式都没问题时退出 0
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
import threading
import time

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
    """→ (状态, ppid, 进程组, 会话)；没有这个进程 → None。"""
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
    """pid 和它的子孙，叶子在前。"""
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


def kill(pid, sig):
    if not isinstance(pid, int) or pid <= 1:  # 0、1、负数会打到自己的进程组或账号的所有进程
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
    """→ [(名字, 插件)]：核心的 consumers/ 按 BUILTIN 的顺序，再接使用者的 CONSUMERS。"""
    out = [(n, load(os.path.join(HERE, "consumers", n + ".py"), "taskq_consumer_" + n)) for n in BUILTIN]
    ns = {"CONSUMERS": {}}
    cfg.extend(ns, "consumers")
    return out + list(ns["CONSUMERS"].items())


def hook(p, name):
    return p.get(name) if isinstance(p, dict) else getattr(p, name, None)


class Run:
    """一个 run_task.sh 和它开的训练。"""

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
        self.device = os.environ.get("WORKER_DEVICE") or "gpu"
        self.W = os.environ.get("WORKER_ID", "")
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
        self.end, self.end_text, self.get_end = 0, "无", None
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
        self.plugins = []
        try:
            cands = plugin_list()
        except Exception as e:  # noqa: BLE001
            say(f"!! 读不了消费者插件（{e}），只用核心的")
            cands = [(n, load(os.path.join(HERE, "consumers", n + ".py"), "taskq_consumer_" + n)) for n in BUILTIN]
        for name, p in cands:
            a = hook(p, "active")
            if a is None or a(self):
                self.plugins.append((name, p))
                s = hook(p, "setup")
                if s:
                    s(self)
        if not self.W:
            print("要设 WORKER_ID（配置文件夹里 machines.py 的 worker）", file=sys.stderr)
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

    # ---------- 机器表、代码 ----------
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
            c = (out_of(["git", "-C", self.repo, "rev-parse", "--short", "HEAD"]) or "").strip() or "未知"
            try:
                d = cfg.outputs("code_dirs")
            except Exception:  # noqa: BLE001
                d = None
            d = [d] if isinstance(d, str) else list(d or [])
            if d and subprocess.run(["git", "-C", self.repo, "diff", "--quiet", "HEAD", "--", *d],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
                c += "+改动"
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

    # ---------- 插件 ----------
    def each(self, name):
        for _, p in self.plugins:
            f = hook(p, name)
            if f:
                yield f

    def say(self, msg):
        say(msg)

    def busy(self):
        return len(self.runs)

    def left(self):
        return 999999 if not self.end else self.end - time.time()

    # ---------- 队列 ----------
    def tq(self, c, key, *args, quiet=False):
        tok = self.tok.get(key)
        cmd = [PY, TASKQ, c, "--worker", self.W, *(["--token", tok] if tok else []), key, *args]
        sys.stdout.flush()
        return subprocess.run(cmd, stdout=subprocess.DEVNULL if quiet else None).returncode

    def exclude(self, key):
        if key not in self.excl:
            self.excl.append(key)

    def retry_later(self, key):
        self.later[key] = time.time() + self.FAIL_RETRY
        say(f"{key} 本作业 {self.FAIL_RETRY // 60} 分钟后再领")

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
            say(f"本作业 {self.FAIL_WINDOW // 60} 分钟内已有 {self.own} 个任务开始不久就失败，不再领新任务（多半是这个节点或环境有问题）")

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
            say(f"{key} 一周内第 {n} 次卡住，记一次失败")
            self.fail(key, f"卡住（一周内第 {n} 次）", ran)
        else:
            self.tq("release", key)
            self.retry_later(key)

    def quiet_since(self, r):
        """任务的日志和结果文件夹多少秒没有变化；读不了 → None。"""
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

    # ---------- 进程 ----------
    def ended(self, r):
        """run_task.sh 退出了没有（退出码记在 r.rc；交接时被别的进程收走了 → None）。"""
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
        """先发给训练的进程组和会话，再按进程树补发给 run_task.sh 和不在会话里的。"""
        if not r.ended:
            self.sid_of(r)
        if isinstance(r.sid, int) and r.sid > 1:  # killpg(1) 就是 kill(-1)：账号的所有进程
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
                    say(f"{r.key} 没交还成（拿不到队列的锁），领取过期后别人能领")
            else:
                say(f"{r.key} 强制停之后还在，不交还（领取过期后才能被别人领）")

    def finish(self):
        self.mon_stop.set()
        for f in self.each("on_exit"):
            try:
                f(self)
            except Exception as e:  # noqa: BLE001
                say(f"!! 插件收尾出错：{e}")

    def shutdown(self, msg, rc):
        """停下手上所有的、确认停下的交还，然后退出 rc。"""
        if self.stopping:
            return
        self.stopping = True
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
            say(f"{self.TERM_WAIT} 秒没停下，强制停")
            for r in self.all_runs():
                if self.alive(r):
                    self.stop_tree(r, signal.SIGKILL)
            for _ in range(3):
                if not self.any_alive():
                    break
                time.sleep(1)
        self.let_go()
        self.finish()
        sys.stdout.flush()
        os._exit(rc)

    def on_signal(self, *_):
        self.shutdown(f"收到停止信号（{self.term_why}），停训练、交还任务", self.term_rc)

    # ---------- 停止某个任务（taskq.py stop） ----------
    def check_stops(self):
        n = False
        for pid, r in list(self.runs.items()):
            if r.stalled or not os.path.exists(os.path.join(self.qdir, proto.QUEUE["stopped"], proto.safe(r.key))):
                continue
            say(f"{r.key} 被停止（taskq.py stop），停训练、交还")
            self.stop_tree(r)
            for _ in range(int(self.STOP_WAIT)):
                if not self.alive(r):
                    break
                time.sleep(1)
            if self.alive(r):
                say(f"{r.key} {self.STOP_WAIT} 秒没停下，强制停")
                self.stop_tree(r, signal.SIGKILL)
                time.sleep(1)
            if not self.alive(r):
                self.tq("release", r.key)
                del self.runs[pid]
                n = True
            else:
                say(f"{r.key} 强制停之后还在，按停不下来处理")
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

    # ---------- GPU 记录 ----------
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

    # ---------- 开训 ----------
    def can_add(self):
        """→ (能不能再领一个, 挡住的原因)；手上有训练时原因记在 why_not。"""
        n = len(self.runs)
        self.why_not = ""
        if n >= self.slots:
            self.why_not = f"已经 {n} 个"
            return False, self.why_not
        for f in self.each("admit"):
            why = f(self)
            if why:
                if n:
                    self.why_not = why
                return False, why
        return True, None

    def start(self, out, jt):
        lines = (out.split("\n") + ["", "", ""])[:3]
        key, args, tok = lines
        self.tok[key] = tok
        targs = shlex.split(args)
        for f in self.each("before_start"):
            f(self)
        env = dict(os.environ, TASK_KEY=key, TASK_JOBTAG=jt, TASK_PART="1", TASK_CPUS=str(self.task_cpus), TASKQ_ADMITTED="1")
        for f in self.each("env"):
            f(self, env)
        sys.stdout.flush()
        proc = subprocess.Popen(["bash", RUN_TASK, *targs], stdin=subprocess.DEVNULL, env=env)
        sh = out_of([PY, os.path.join(HERE, "spec.py"), "shell", *targs], any_rc=True) or ""
        vals = {}
        for ln in sh.split("\n"):
            m = re.match(r"(TASK_OUT|TASK_SAFE)=(.*)$", ln)
            if m:
                v = shlex.split(m.group(2))
                vals[m.group(1)] = v[0] if v else ""
        r = Run(proc.pid, key, proc=proc, outdir=vals.get("TASK_OUT", ""), safe=vals.get("TASK_SAFE"), jtag=jt)
        self.runs[proc.pid] = r
        self.changed = int(time.time())
        say(f"开训 {key}（pid {proc.pid}：run_task.sh {args}）")

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

    # ---------- 换代码包 ----------
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
                    raise ValueError(f"版本 {d.get('v')}")
            except ValueError as e:
                say(f"!! 交接的内容读不了（{e}），在训的没接上，领取过期后别人能领")
                d = {}
            self.t0 = int(d.get("t0") or self.t0)
            self.changed = int(d.get("changed") or self.changed)
            self.excl = list(d.get("excl") or [])
            self.later = dict(d.get("later") or {})
            self.fail_at = list(d.get("fail_at") or [])
            s = int(d.get("idle_since") or 0)
            if self.t0 <= s <= time.time():
                self.idle_since = s
                say(f"接着算空闲：已经空了 {int(time.time()) - s} 秒")
            for t in d.get("tasks") or []:
                r = Run(int(t["pid"]), t["key"], started=t.get("started"), outdir=t.get("outdir"), safe=t.get("safe"),
                        jtag=t.get("jtag"), stalled=t.get("stalled"), why=t.get("why"), sid=t.get("sid"))
                r.ended = bool(t.get("ended"))
                if t.get("token"):
                    self.tok[r.key] = t["token"]
                if t.get("zomb"):
                    self.zomb[r.pid] = r
                    say(f"接着跟停不下来的 {r.key}（pid {r.pid}）")
                else:
                    self.runs[r.pid] = r
                    say(f"接着管 {r.key}（pid {r.pid}）")

    def selftest(self, core):
        try:
            r = subprocess.run([PY, os.path.join(core, "queue", "consume.py"), "--selftest"], stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, timeout=60)
        except (OSError, subprocess.SubprocessError) as e:
            return False, str(e)
        last = (r.stdout.strip().split("\n") or [""])[-1][:300]
        return r.returncode == 0, last or f"退出码 {r.returncode}"

    def switch(self, head):
        """代码包换了：在新包里自检，过了就 exec 新包的 worker_start.sh，手上的交给新进程；换不了 → False。"""
        core = self.new_core()
        if not core:
            return False
        name = os.path.basename(self.bundle_cur())
        env = dict(os.environ)
        ok, why = self.selftest(core)
        if not ok:
            say(f"新代码包 {name} 自检没过（{why}），留在 {self.commit}")
            self.bad.add(head)
            return False
        env["TASKQ_HANDOFF"] = json.dumps(self.handoff(), ensure_ascii=False)
        say("代码更新了")
        n = len(self.runs)
        say(f"换成新代码 {name} 接着跑" + (f"（在训的 {n} 个交给新代码接着管）" if n else ""))
        self.mon_stop.set()
        sys.stdout.flush()
        try:
            os.execvpe("bash", ["bash", os.path.join(core, "queue", "worker_start.sh")], env)
        except OSError as e:
            say(f"!! 换不成新代码（{e}），留在 {self.commit}")
            self.bad.add(head)
        return False

    # ---------- 主循环 ----------
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
                say(f"{key} 已停下（领取已经不是本作业的，不交还）")
            elif r.why == "stall":
                say(f"卡住的 {key} 已停下，交还，本作业不再领它")
                self.release_stalled(key, self.stalls(r.safe), ran)
            elif r.stalled:
                say(f"{key} 已停下，交还")
                self.tq("release", key)
            elif rc is not None and rc > 128 and session_alive(r.sid) or rc is None and session_alive(r.sid):
                say(f"{key} 的 run_task.sh 被停掉（退出码 {'没收到' if rc is None else rc}），训练还在：停掉它，停下了再交还（不算失败），本作业不再领它")
                subprocess.run(["pkill", "-TERM", "-s", str(r.sid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                r.why = "killed"
                self.zomb[pid] = r
                self.exclude(key)
            elif rc is None:
                say(f"{key} 换代码时已经结束，退出码没收到：交还（不算失败）")
                self.tq("release", key)
            elif rc == X["run_done"]:
                say(f"训完 {key}")
                self.tq("release", key)
            elif rc == X["run_missing"]:
                say(f"{key} 缺输入，没开训，交还，记为缺输入（不算失败，补齐前不再领）")
                if self.tq("missing", key, "--why", f"作业 {self.W} 开训前查到") != 0:
                    self.tq("release", key)
                self.exclude(key)
            elif rc == X["run_unreadable"]:
                say(f"{key} 读不了盘、判断不了训完没有，交还（不算失败），本作业 10 分钟后再领")
                self.tq("release", key)
                self.later[key] = time.time() + 600
            elif rc == X["run_not_done"]:
                say(f"{key} 命令正常结束，但没有训完的文件，交还并记一次失败")
                self.fail(key, "退出码 0 但没训完", ran)
            elif rc == X["run_term"]:
                say(f"{key} 被外部停掉（run_task.sh 收到停止信号），训练已停下：交还（不算失败），本作业不再领它")
                self.tq("release", key)
                self.exclude(key)
            else:
                say(f"{key} 训练失败（run_task.sh 退出码 {rc}），交还并记一次失败")
                extra = ""
                if rc == X["run_bad"]:
                    try:
                        with open(self.task_log(r), errors="replace") as f:
                            last = (f.read().rstrip("\n").split("\n") or [""])[-1]
                        extra = ("：" + re.sub(r"^[-0-9: ]*", "", last))[:300] if last else ""
                    except OSError:
                        pass
                self.fail(key, f"退出码 {rc}{extra}", ran)
        for pid, r in list(self.zomb.items()):
            if self.alive(r):
                continue
            del self.zomb[pid]
            if r.why == "lost":
                say(f"停不下来的 {r.key} 终于停下了（领取已经不是本作业的，不交还）")
            elif r.why == "stall":
                say(f"停不下来的 {r.key} 终于停下了，交还")
                self.release_stalled(r.key, self.stalls(r.safe), 0)
            else:
                say(f"停不下来的 {r.key} 终于停下了，交还")
                self.tq("release", r.key)

    def watch_stalls(self, now):
        for pid, r in list(self.runs.items()):
            if r.stalled:
                if now - r.stalled >= self.GIVEUP:
                    say(f"{r.key} 停了 {self.GIVEUP} 秒还没停下（多半卡在读写上），先领别的；接着心跳，停下了再交还")
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
            say(f"{r.key} 的日志和结果文件夹 {q // 60} 分钟没有变化，当作卡住：停掉、交还，领下一个")
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
            say(f"{r.key} 的领取已经不是本作业的（过期后被别人接管，或者已经没了），停掉这个训练，不交还")
            r.why = "lost"
            self.exclude(r.key)
            if r.pid in self.zomb or r.stalled:
                continue
            r.stalled = now
            self.stop_tree(r)
        if self.bundle_cur():
            self.touch_repo()

    def run(self):
        if self.get_end:
            self.end = self.get_end(self) or 0
        lim = time.strftime("%F %T", time.localtime(self.end)) if self.end else self.end_text
        say(f"任务队列 worker（{self.device}）：作业 {self.W} @ {self.host}，最多同时 {self.slots} 个，CPU {self.ncpu}，"
            f"内存 {self.mem_mb}M（每个训练 {self.task_cpus} 个 CPU）， 环境 {self.envs}，时限 {lim}，代码 {self.commit}")
        self.mon_stop = threading.Event()
        self.gpu_gb = None
        if self.device == "gpu":
            sys.stdout.flush()
            try:
                subprocess.run(["nvidia-smi", *self.nvsel, "--query-gpu=index,name,memory.total", "--format=csv,noheader"],
                               stderr=subprocess.DEVNULL, timeout=60)  # 卡的型号打在日志里
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
            if not self.end and self.get_end:
                self.end = self.get_end(self) or 0
            if now - last_beat >= self.BEAT:
                self.beat(now)
                last_beat = now
            for f in self.each("tick"):
                f(self)
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
                jt = f"{self.W}-{time.strftime('%Y%m%d%H%M%S')}" if self.tag_each else self.W  # 同一台几个 worker 同一分钟开训也不撞
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
                    say(f"队列出错（taskq.py 退出码 {rc}：{self.err}），之后每轮再试")
                why = "error"
                break
            if not tried:
                err_since = 0
            if self.why_not and self.why_not != last_why:
                say(f"先不再加训练：{self.why_not}")
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
                        say("代码更新了")
                        restart = True
                        break
                f = next(self.each("idle"), None)
                d = f(self, why, now, self.idle_since, err_since) if f else None
                if d is None:
                    break
                self.nap(d)
                continue
            self.nap()
        self.let_go()
        self.finish()
        if restart:
            say("退出，由 relay.py 用新代码重开")
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
    print("自检通过")
    return 0


def main(argv):
    if argv[:1] == ["--selftest"]:
        try:
            return selftest()
        except Exception as e:  # noqa: BLE001
            print(f"自检没过：{type(e).__name__}: {e}")
            return 1
    return Worker().run()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
