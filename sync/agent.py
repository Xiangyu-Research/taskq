#!/usr/bin/env python3
"""Resident agent: runs on every machine the hub drives (the hub's own machine too) and only connects out, to the hub's file
server (sync/tunnel.py) over HTTPS. It long-polls the hub's mailbox for commands, runs each in bash like an ssh login would,
streams stdin in and stdout / stderr back, and reports the exit code. The hub side (mailbox, server routes, the ssh stand-in
xssh) is sync/agent_box.py; the protocol is in the README ("Agents").

    agent.py ensure <machine>        start the agent in the background unless one already runs (cron, every minute)
    agent.py run <machine>           the agent itself (foreground; holds the lock)
    agent.py cron <machine> <python> [<agent.py>]  the one crontab line that keeps the agent running (marked "# taskq-agent
                                     <machine>"; replaces older lines with that mark); <agent.py> defaults to this file
    agent.py clock <machine>         set this machine's clock from stdin: JSON [[when, shell command], ...]
    agent.py stop <machine>          stop the running agent
    agent.py selftest                exit 0 (an update is checked with it before it replaces this file)

Clock: besides running the hub's commands, the agent runs this machine's own timed commands (taskq site setup writes them:
the hub's health check and the project's other OUTPUTS["cron"] entries, @reboot boot steps), so nothing else is in crontab.
when is a crontab time ("5,35 * * * *", local time) or "@reboot" (once per boot: run when the agent first starts after the
machine booted; the very first start, with no record of an earlier boot, runs nothing). Minutes missed while the agent was down
are made up for at most CATCHUP minutes back, each command once. Commands run in bash with the agent's clean environment
in their own session, are not waited for, and their output goes to agent-<machine>.clock.log next to the agent log.

Config: ~/.config/taskq/agent-<machine>.json {machine, url, key} (0600, written by taskq site agent install); TASKQ_AGENT_HOME
overrides ~ (tests). Every request is signed with the machine's key (sign()); commands and code from the hub carry the hub's
signature and are refused without it. Standard library only and Python 3.6 compatible: it runs on login nodes with an old python.
"""
import errno
import fcntl
import hashlib
import hmac
import http.client
import json
import os
import pwd
import select
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid

UA = "xfer/1"
WAIT = int(os.environ.get("TASKQ_AGENT_WAIT", 25))  # seconds a long poll may hang on the hub (well inside Cloudflare's 100)
ALIVE_EVERY = float(os.environ.get("TASKQ_AGENT_ALIVE", 10))  # seconds between reports of running commands
SKEW = 900  # a command older than this (by the hub's clock) is refused: a replay, or the caller gave up long ago
CHUNK = 8 << 20  # most bytes per request (Cloudflare refuses bodies over 100 MB)
MARK = "# taskq-agent"
CATCHUP = 10  # minutes: after a gap (agent down, machine asleep), clock entries due within this many minutes still run, once each
BOOT_ID = "/proc/sys/kernel/random/boot_id"
BASE_PATH = "/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin:/sbin"


def sign(key, *parts):
    return hmac.new(key.encode(), "\n".join(str(p) for p in parts).encode(), hashlib.sha256).hexdigest()


def sha256(b):
    return hashlib.sha256(b).hexdigest()


def home():
    return os.environ.get("TASKQ_AGENT_HOME") or os.path.expanduser("~")


def conf_path(m):
    return os.path.join(home(), ".config", "taskq", "agent-%s.json" % m)


def lock_path(m):
    return os.path.join(home(), ".local", "state", "taskq", "agent-%s.lock" % m)


def log_path(m):
    return os.path.join(home(), ".local", "state", "taskq", "agent-%s.log" % m)


def clock_path(m):
    return os.path.join(home(), ".config", "taskq", "agent-%s.clock.json" % m)


def clock_state_path(m):
    return os.path.join(home(), ".local", "state", "taskq", "agent-%s.clock" % m)


def clock_log_path(m):
    return os.path.join(home(), ".local", "state", "taskq", "agent-%s.clock.log" % m)


def code_sha():
    with open(os.path.abspath(__file__), "rb") as f:
        return sha256(f.read())


def log(msg):
    sys.stdout.write("%s %s\n" % (time.strftime("%F %T"), msg))
    sys.stdout.flush()


class Http:
    """One keep-alive connection per thread to the hub's server; a broken one is reopened once per request."""

    def __init__(self, url, m, key):
        u = urllib.parse.urlsplit(url)
        self.https, self.host, self.port = u.scheme == "https", u.hostname, u.port
        self.prefix = u.path.rstrip("/") + "/agent/" + m
        self.m, self.key, self.local = m, key, threading.local()

    def conn(self, timeout):
        c = getattr(self.local, "c", None)
        if c is None:
            cls = http.client.HTTPSConnection if self.https else http.client.HTTPConnection
            c = self.local.c = cls(self.host, self.port, timeout=timeout)
        c.timeout = timeout
        if c.sock is not None:
            c.sock.settimeout(timeout)
        return c

    def drop(self):
        c = getattr(self.local, "c", None)
        if c is not None:
            c.close()
        self.local.c = None

    def call(self, method, path, body=b"", timeout=WAIT + 30, headers=None):
        """→ (status, headers dict with lower-case names, body). Network trouble → OSError / HTTPException after one retry."""
        full = self.prefix + path
        for attempt in (0, 1):
            ts, nonce = str(int(time.time())), uuid.uuid4().hex
            h = {"User-Agent": UA, "Content-Length": str(len(body)), "X-Agent-Ts": ts, "X-Agent-Nonce": nonce,
                 "X-Agent-Sig": sign(self.key, "req", self.m, ts, nonce, method, full, sha256(body))}
            h.update(headers or {})
            try:
                c = self.conn(timeout)
                c.request(method, full, body=body, headers=h)
                r = c.getresponse()
                data = r.read()
                return r.status, {k.lower(): v for k, v in r.getheaders()}, data
            except (OSError, http.client.HTTPException):
                self.drop()
                if attempt:
                    raise


class Command:
    """One command from the hub, running here."""

    def __init__(self, agent, spec):
        self.a, self.id, self.cmd = agent, spec["id"], spec["cmd"]
        self.p = None
        self.cancelled = False

    def start(self):
        env = dict(self.a.env)
        rc = os.path.join(home(), ".bashrc")
        if os.path.isfile(rc):
            env["BASH_ENV"] = rc  # like bash started by sshd: ~/.bashrc first (it may set PATH), then the command
        self.p = subprocess.Popen(["bash", "-c", "unset BASH_ENV; cd ~ && " + self.cmd], cwd=home(), env=env,
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        threads = [threading.Thread(target=self.feed, daemon=True)]
        threads += [threading.Thread(target=self.drain, args=(f, s), daemon=True) for f, s in ((self.p.stdout, "out"), (self.p.stderr, "err"))]
        for t in threads:
            t.start()
        threading.Thread(target=self.finish, args=(threads[1:],), daemon=True).start()

    def kill(self):
        self.cancelled = True
        if self.p is None or self.p.poll() is not None:
            return
        for sig, wait in ((signal.SIGTERM, 5), (signal.SIGKILL, 0)):
            try:
                os.killpg(self.p.pid, sig)
            except OSError:
                return
            end = time.time() + wait
            while time.time() < end and self.p.poll() is None:
                time.sleep(0.1)

    def feed(self):
        """stdin from the hub, as it arrives (the caller may still be writing)."""
        off, fd = 0, self.p.stdin.fileno()
        try:
            while self.p.poll() is None and not self.cancelled:
                try:
                    st, h, b = self.a.http.call("GET", "/in/%s?off=%d" % (self.id, off))
                except (OSError, http.client.HTTPException):
                    time.sleep(2)
                    continue
                if st != 200:
                    return
                eof = h.get("x-eof") == "1"
                if not hmac.compare_digest(h.get("x-sig", ""), sign(self.a.key, "in", self.a.m, self.id, off, sha256(b), int(eof))):
                    log("%s: stdin chunk with a bad signature, stopping the command" % self.id)
                    self.kill()
                    return
                if h.get("x-cancel") == "1":
                    self.kill()
                    return
                while b:
                    n = os.write(fd, b)
                    b, off = b[n:], off + n
                if eof:
                    return
        except OSError as e:  # the command closed its stdin or exited
            if e.errno not in (errno.EPIPE, errno.EBADF):
                raise
        finally:
            try:
                self.p.stdin.close()
            except OSError:
                pass

    def drain(self, f, stream):
        fd, off = f.fileno(), 0
        while True:
            b = os.read(fd, 1 << 16)
            if not b:
                return
            while len(b) < CHUNK and select.select([fd], [], [], 0.05)[0]:  # take what is already there: fewer, larger requests
                more = os.read(fd, min(1 << 20, CHUNK - len(b)))
                if not more:
                    break
                b += more
            while True:
                try:
                    st, _, _ = self.a.http.call("POST", "/out/%s?s=%s&off=%d" % (self.id, stream, off), body=b, timeout=120)
                    if st == 200:
                        break
                    if st in (404, 409, 410):  # the hub forgot it: keep reading so the command is not blocked on a full pipe
                        b = b""
                        break
                except (OSError, http.client.HTTPException):
                    pass
                time.sleep(2)
            off += len(b)

    def finish(self, drains):
        for t in drains:
            t.join()
        rc = self.p.wait()
        rc = 128 - rc if rc < 0 else rc  # killed by a signal: like a shell reports it
        for _ in range(100):
            try:
                st, _, _ = self.a.http.call("POST", "/done/%s?rc=%d" % (self.id, rc), timeout=60)
                if st in (200, 404, 410):
                    break
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(3)
        self.a.forget(self.id)


class Agent:
    def __init__(self, m):
        with open(conf_path(m)) as f:
            c = json.load(f)
        self.m, self.key, self.url = c["machine"], c["key"], c["url"]
        self.http = Http(self.url, self.m, self.key)
        self.running, self.seen, self.lock = {}, {}, threading.Lock()
        self.sha, self.host = code_sha(), socket.gethostname()
        self.env = clean_env()

    def forget(self, cid):
        with self.lock:
            self.running.pop(cid, None)

    def info(self):
        return json.dumps(dict(sha=self.sha, host=self.host, pid=os.getpid(), py=sys.version.split()[0], running=len(self.running)))

    def alive_loop(self):
        while True:
            time.sleep(ALIVE_EVERY)
            with self.lock:
                ids = list(self.running)
            try:
                st, _, b = self.http.call("POST", "/alive", body=json.dumps(ids).encode(), timeout=60,
                                          headers={"X-Agent-Info": self.info()})
                if st == 200:
                    for cid in json.loads(b.decode()).get("cancel", []):
                        c = self.running.get(cid)
                        if c:
                            log("%s: the hub side gave up, stopping it" % cid)
                            threading.Thread(target=c.kill, daemon=True).start()
            except (OSError, http.client.HTTPException, ValueError):
                pass

    def accept(self, spec):
        cid = spec.get("id", "")
        if not hmac.compare_digest(spec.get("sig", ""), sign(self.key, "cmd", self.m, cid, spec.get("ts", ""), spec.get("cmd", ""))):
            log("%s: bad signature, refused" % cid)
            return
        now = time.time()
        if abs(now - float(spec["ts"])) > SKEW or cid in self.seen:
            log("%s: too old or already run, refused" % cid)
            return
        self.seen = {k: v for k, v in self.seen.items() if now - v < 2 * SKEW}
        self.seen[cid] = now
        c = Command(self, spec)
        with self.lock:
            self.running[cid] = c
        try:
            c.start()
        except OSError as e:
            self.forget(cid)
            self.http.call("POST", "/out/%s?s=err&off=0" % cid, body=("agent: cannot start bash: %s\n" % e).encode())
            self.http.call("POST", "/done/%s?rc=255" % cid)

    def update(self, want):
        """The hub has another agent.py (sha want): fetch it, check it, replace this file and exec it. Only when idle."""
        st, h, b = self.http.call("GET", "/code", timeout=120)
        if st != 200 or sha256(b) != want or not hmac.compare_digest(h.get("x-sig", ""), sign(self.key, "code", self.m, want)):
            log("update to %s refused (status %s, digest or signature wrong)" % (want[:12], st))
            return False
        me = os.path.abspath(__file__)
        tmp = me + ".new"
        with open(tmp, "wb") as f:
            f.write(b)
        if subprocess.call([sys.executable, tmp, "selftest"]) != 0:
            log("update to %s failed its selftest, keeping this version" % want[:12])
            return False
        os.replace(tmp, me)
        log("updated to %s, restarting" % want[:12])
        sys.stdout.flush()
        os.execv(sys.executable, [sys.executable, me, "run", self.m])

    def loop(self):
        threading.Thread(target=self.alive_loop, daemon=True).start()
        threading.Thread(target=clock_loop, args=(self.m,), daemon=True).start()
        pause, refused = 1, None
        log("agent %s up (pid %d, sha %s, hub %s)" % (self.m, os.getpid(), self.sha[:12], self.url))
        while True:
            try:
                st, h, b = self.http.call("GET", "/next?wait=%d" % WAIT, headers={"X-Agent-Info": self.info()})
            except (OSError, http.client.HTTPException) as e:
                log("hub unreachable (%s), retrying in %ds" % (e, pause))
                time.sleep(pause)
                pause = min(2 * pause, 30)
                continue
            pause = 1
            if st == 403:
                log("the hub refused this agent's signature (key changed?), retrying in 60s")
                time.sleep(60)
                continue
            if st == 200:
                self.accept(json.loads(b.decode()))
            elif st != 204:
                time.sleep(5)
            want = h.get("x-agent-code")
            if want and want != self.sha and want != refused and not self.running:
                if not self.update(want):
                    refused = want


# ---------------- clock ----------------
_SPAN = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))


def cron_field(f, lo, hi):
    """One crontab field → set of values; ValueError when it is not one."""
    out = set()
    for part in f.split(","):
        rng, _, step = part.partition("/")
        st = int(step) if step else 1
        if rng == "*":
            a, b = lo, hi
        elif "-" in rng:
            a, b = (int(x) for x in rng.split("-", 1))
        else:
            a = b = int(rng)
            if step:
                b = hi
        if st < 1 or a < lo or b > hi or a > b:
            raise ValueError("bad crontab field %r" % f)
        out.update(range(a, b + 1, st))
    return out


def cron_due(when, t):
    """Does crontab time when (5 fields) match local time t (seconds; its minute)? Day of month and day of week: either
    matches when both are restricted, as cron does."""
    fs = when.split()
    if len(fs) != 5:
        raise ValueError("a crontab time has 5 fields: %r" % when)
    mi, hr, dom, mon, dow = (cron_field(f, *_SPAN[i]) for i, f in enumerate(fs))
    if 7 in dow:
        dow.add(0)
    lt = time.localtime(t)
    wd = (lt.tm_wday + 1) % 7  # cron: 0 = Sunday
    if lt.tm_min not in mi or lt.tm_hour not in hr or lt.tm_mon not in mon:
        return False
    if fs[2] != "*" and fs[4] != "*":
        return lt.tm_mday in dom or wd in dow
    return lt.tm_mday in dom and wd in dow


def boot_id():
    try:
        with open(BOOT_ID) as f:
            return f.read().strip()
    except OSError:
        return None


def read_json(p, default):
    try:
        with open(p) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def write_json(p, d, mode=0o600):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    fd = os.open(p + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    os.write(fd, json.dumps(d).encode())
    os.close(fd)
    os.replace(p + ".tmp", p)


def set_clock(m, entries):
    """Check and store this machine's clock entries [[when, command], ...]."""
    if not isinstance(entries, list):
        raise ValueError("clock entries are a list")
    for e in entries:
        if not (isinstance(e, list) and len(e) == 2 and all(isinstance(x, str) and x.strip() for x in e)):
            raise ValueError("a clock entry is [when, command]: %r" % (e,))
        if e[0] != "@reboot":
            cron_due(e[0], time.time())
    write_json(clock_path(m), entries)


def clock_start(m, when, cmd):
    """Start one clock command in its own session and leave it (setsid -f: not this process's child, nothing to reap)."""
    os.makedirs(os.path.dirname(clock_log_path(m)), exist_ok=True)
    with open(clock_log_path(m), "ab") as lf:
        lf.write(("%s %s: %s\n" % (time.strftime("%F %T"), when, cmd)).encode())
        lf.flush()
        try:
            subprocess.call(["setsid", "-f", "bash", "-c", cmd], cwd=home(), env=clean_env(), stdin=subprocess.DEVNULL,
                            stdout=lf, stderr=subprocess.STDOUT)
        except OSError as e:
            log("clock: cannot start %r: %s" % (cmd, e))


def clock_tick(m, now, start=clock_start):
    """One clock step at time now: @reboot entries once per boot, crontab entries for each minute since the last step (at
    most CATCHUP back), each once. → the commands started, as (when, command)."""
    st = read_json(clock_state_path(m), None)
    entries = read_json(clock_path(m), [])
    cur = int(now // 60)
    ran = []
    if not isinstance(st, dict):
        st = dict(boot=boot_id(), minute=cur - 1)  # first start: no record of an earlier boot, so no @reboot
    b = boot_id()
    todo = []
    if b != st.get("boot"):
        todo += [e for e in entries if e[0] == "@reboot"]
        st["boot"] = b
    last = st.get("minute", cur - 1)
    mins = range(max(last + 1, cur - CATCHUP + 1), cur + 1)
    for when, cmd in entries:
        if when == "@reboot":
            continue
        try:
            if any(cron_due(when, x * 60) for x in mins):
                todo.append([when, cmd])
        except ValueError as e:
            log("clock: %s" % e)
    st["minute"] = max(last, cur)
    write_json(clock_state_path(m), st)  # recorded before starting: a crash in between skips a run rather than doubling it
    for when, cmd in todo:
        start(m, when, cmd)
        ran.append((when, cmd))
    return ran


def clock_loop(m):
    while True:
        try:
            clock_tick(m, time.time())
        except Exception as e:  # noqa: BLE001  the clock keeps going whatever one step does
            log("clock: step failed: %s" % e)
        time.sleep(60 - time.time() % 60 + 1)


def clean_env():
    """The same environment however the agent was started (cron, a shell, ssh): what an ssh login gets before ~/.bashrc."""
    pw = pwd.getpwuid(os.getuid())
    env = dict(HOME=home(), USER=pw.pw_name, LOGNAME=pw.pw_name, SHELL=pw.pw_shell or "/bin/bash", PATH=BASE_PATH,
               LANG=os.environ.get("LANG", "C.UTF-8"))
    for k in ("TZ", "TASKQ_AGENT_HOME"):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


def hold_lock(m):
    os.makedirs(os.path.dirname(lock_path(m)), exist_ok=True)
    fd = os.open(lock_path(m), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    os.ftruncate(fd, 0)
    os.write(fd, ("%d\n" % os.getpid()).encode())
    return fd


def running_pid(m):
    """Pid of the agent holding the lock, or None."""
    try:
        fd = os.open(lock_path(m), os.O_RDWR)
    except OSError:
        return None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return None
    except OSError:
        try:
            return int(os.read(fd, 32).decode().strip() or 0) or None
        except ValueError:
            return None
    finally:
        os.close(fd)


def ensure(m):
    if running_pid(m):
        return 0
    os.makedirs(os.path.dirname(log_path(m)), exist_ok=True)
    with open(log_path(m), "ab") as lf:
        subprocess.Popen([sys.executable, os.path.abspath(__file__), "run", m], cwd=home(), env=clean_env(), stdin=subprocess.DEVNULL,
                         stdout=lf, stderr=subprocess.STDOUT, start_new_session=True)
    for _ in range(50):
        if running_pid(m):
            return 0
        time.sleep(0.1)
    sys.stderr.write("agent %s did not start; see %s\n" % (m, log_path(m)))
    return 1


def cron(m, py, me=None):
    me = me or os.path.abspath(__file__)
    tag = "%s %s" % (MARK, m)
    r = subprocess.Popen(["crontab", "-l"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    old = r.communicate()[0].decode()
    keep = [ln for ln in old.splitlines() if not ln.rstrip().endswith(tag)]
    line = "%s %s ensure %s > /dev/null 2>&1 %s" % (py, me, m, tag)
    new = "\n".join(keep + ["* * * * * " + line]) + "\n"
    p = subprocess.Popen(["crontab", "-"], stdin=subprocess.PIPE)
    p.communicate(new.encode())
    return p.returncode


def stop(m):
    pid = running_pid(m)
    if pid:
        os.kill(pid, signal.SIGTERM)
        for _ in range(100):
            if not running_pid(m):
                break
            time.sleep(0.1)
    return 0


def main(argv):
    if argv[:1] == ["selftest"]:
        return 0
    if len(argv) >= 2 and argv[0] == "run":
        fd = hold_lock(argv[1])
        if fd is None:
            return 0  # another agent for this machine already runs here
        signal.signal(signal.SIGTERM, lambda *a: os._exit(0))
        Agent(argv[1]).loop()
    if len(argv) == 2 and argv[0] == "ensure":
        return ensure(argv[1])
    if len(argv) == 2 and argv[0] == "stop":
        return stop(argv[1])
    if len(argv) in (3, 4) and argv[0] == "cron":
        return cron(*argv[1:])
    if len(argv) == 2 and argv[0] == "clock":
        try:
            set_clock(argv[1], json.load(sys.stdin))
        except ValueError as e:
            sys.stderr.write("agent.py clock: %s\n" % e)
            return 2
        return 0
    sys.stderr.write(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
