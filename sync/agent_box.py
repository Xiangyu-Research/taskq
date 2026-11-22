#!/usr/bin/env python3
"""Hub side of the resident agents (sync/agent.py): the mailbox, the file server's /agent/ routes, the ssh stand-in xssh and
taskq site agent install|start|status.

Mailbox: <tunnel server dir>/agent/<machine>/ (private to this account, next to the file server's sessions)
    key                 the machine's secret (0600); the agent holds a copy in ~/.config/taskq/agent-<machine>.json
    seen, info.json     when the agent last called, and what it reported (host, pid, agent sha, python, commands running)
    inbox/<id>.json     a command waiting for the agent {id, cmd, ts, sig}; the agent's next poll moves it to run/
    run/<id>.json       claimed; run/<id>.in (+ .eof once the caller's stdin closed), .out, .err, .rc, .alive (touched by the
                        caller while it waits), .cancel
xssh takes ssh's arguments: for a machine with agent=True in the machine table (matched by its ssh name, or --machine <name>)
it runs the command through that machine's agent, streaming stdin, stdout and stderr both ways like ssh (rsync -e works);
for anything else it runs the real ssh. An agent not seen for STALE seconds is unreachable: exit 255 at once, like ssh.
"""
import json
import os
import re
import secrets
import shlex
import signal
import subprocess
import sys
import threading
import time
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import agent as A  # noqa: E402

STALE = float(os.environ.get("TASKQ_AGENT_STALE", 120))  # agent heartbeat older than this: unreachable
GONE = float(os.environ.get("TASKQ_AGENT_GONE", 30))  # caller's .alive older than this: it gave up (killed), stop the command
CLAIM = float(os.environ.get("TASKQ_AGENT_CLAIM", 60))  # a command not picked up within this: unreachable
DOWN = 30 * 60  # health check: agent not seen for this long -> agent:<machine> problem
SKEW = 300  # request timestamps further off are refused
NAME_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}")
ID_RE = re.compile(r"[0-9a-f]{24}")
CODE = os.environ.get("TASKQ_AGENT_CODE") or os.path.join(HERE, "agent.py")  # the variable: tests only
OPT_ARG = set("BbcDEeFIiJLlmOoPpQRSWw")  # ssh options that take a value


def mailbox(d, m=None, *parts):
    return os.path.join(d, "agent", *([m] if m else []), *parts)


def read_key(d, m):
    try:
        with open(mailbox(d, m, "key")) as f:
            return f.read().strip()
    except OSError:
        return None


def make_key(d, m):
    k = read_key(d, m)
    if k:
        return k
    for sub in ("inbox", "run"):
        os.makedirs(mailbox(d, m, sub), mode=0o700, exist_ok=True)
    p = mailbox(d, m, "key")
    fd = os.open(p + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(secrets.token_hex(32))
    os.replace(p + ".tmp", p)
    return read_key(d, m)


def age(path):
    try:
        return time.time() - os.stat(path).st_mtime
    except OSError:
        return None


def touch(path):
    with open(path, "a"):
        pass
    os.utime(path)


def write_atomic(path, data, mode=0o600):
    tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def resident(d):
    """The file server stays up while any machine has a mailbox (an agent may call at any time)."""
    try:
        return any(os.path.isfile(mailbox(d, m, "key")) for m in os.listdir(mailbox(d)))
    except OSError:
        return False


def code_bytes():
    with open(CODE, "rb") as f:
        return f.read()


# ---------------- server side (inside tunnel.py _serve) ----------------

class Box:
    def __init__(self, d):
        self.d, self.nonces, self.lock = d, {}, threading.Lock()
        self.code = (None, None, None)  # (mtime, bytes, sha)

    def agent_code(self):
        st = os.stat(CODE).st_mtime_ns
        if self.code[0] != st:
            b = code_bytes()
            self.code = (st, b, A.sha256(b))
        return self.code[1], self.code[2]

    def fresh_nonce(self, nonce, now):
        with self.lock:
            if nonce in self.nonces:
                return False
            if len(self.nonces) > 10000 or not hasattr(self, "pruned") or now - self.pruned > 60:
                self.nonces = {k: v for k, v in self.nonces.items() if now - v < 2 * SKEW}
                self.pruned = now
            self.nonces[nonce] = now
            return True

    def handle(self, h, method):
        u = urllib.parse.urlsplit(h.path)
        seg = u.path.split("/")
        q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
        n = int(h.headers.get("Content-Length") or 0)
        if n < 0 or n > A.CHUNK + (1 << 20):
            return send(h, 413)
        body = h.rfile.read(n) if n else b""
        if len(seg) < 4 or not NAME_RE.fullmatch(seg[2]):
            return send(h, 404)
        m = seg[2]
        key = read_key(self.d, m)
        ts, nonce, sig = (h.headers.get(x, "") for x in ("X-Agent-Ts", "X-Agent-Nonce", "X-Agent-Sig"))
        now = time.time()
        if not key or not ts.isdigit() or abs(now - int(ts)) > SKEW or not re.fullmatch(r"[0-9a-f]{32}", nonce) or \
                not A.hmac.compare_digest(sig, A.sign(key, "req", m, ts, nonce, method, h.path, A.sha256(body))) or \
                not self.fresh_nonce(nonce, now):
            return send(h, 403)
        mb = lambda *p: mailbox(self.d, m, *p)  # noqa: E731
        touch(mb("seen"))
        if h.headers.get("X-Agent-Info"):
            try:
                info = json.loads(h.headers["X-Agent-Info"])
                write_atomic(mb("info.json"), json.dumps(info).encode())
            except ValueError:
                pass
        op, cid = seg[3], (seg[4] if len(seg) > 4 else "")
        code, sha = self.agent_code()
        if op == "next" and method == "GET":
            end = now + min(float(q.get("wait", 0)), A.WAIT)
            while True:
                for f in sorted(os.listdir(mb("inbox")), key=lambda f: age(mb("inbox", f)) or 0, reverse=True):
                    if not f.endswith(".json"):
                        continue
                    try:
                        os.rename(mb("inbox", f), mb("run", f))
                    except OSError:
                        continue
                    with open(mb("run", f), "rb") as fh:
                        spec = fh.read()
                    try:
                        return send(h, 200, spec, {"X-Agent-Code": sha})
                    except OSError:  # the agent went away while it waited: leave the command for the next poll
                        os.rename(mb("run", f), mb("inbox", f))
                        return
                if time.time() >= end:
                    return send(h, 204, b"", {"X-Agent-Code": sha})
                time.sleep(0.05)
        if op == "code" and method == "GET":
            return send(h, 200, code, {"X-Sig": A.sign(key, "code", m, sha)})
        if op == "alive" and method == "POST":
            try:
                ids = [i for i in json.loads(body.decode()) if isinstance(i, str) and ID_RE.fullmatch(i)]
            except ValueError:
                return send(h, 400)
            return send(h, 200, json.dumps(dict(cancel=[i for i in ids if self.cancelled(m, i)])).encode())
        if not ID_RE.fullmatch(cid) or not os.path.exists(mb("run", cid + ".json")):
            return send(h, 410)
        if op == "in" and method == "GET":
            off = int(q.get("off", "0"))
            end = now + A.WAIT
            while True:
                eof = os.path.exists(mb("run", cid + ".eof"))  # before the size: once .eof exists, .in is complete
                size = os.path.getsize(mb("run", cid + ".in")) if os.path.exists(mb("run", cid + ".in")) else 0
                cancel = self.cancelled(m, cid)
                if size > off or eof or cancel or time.time() >= end:
                    break
                time.sleep(0.05)
            b = b""
            if size > off:
                with open(mb("run", cid + ".in"), "rb") as fh:
                    fh.seek(off)
                    b = fh.read(min(A.CHUNK, size - off))
            last = int(eof and off + len(b) >= size)
            return send(h, 200, b, {"X-Eof": str(last), "X-Cancel": str(int(cancel)),
                                    "X-Sig": A.sign(key, "in", m, cid, off, A.sha256(b), last)})
        if op == "out" and method == "POST" and q.get("s") in ("out", "err"):
            p, off = mb("run", f"{cid}.{q['s']}"), int(q.get("off", "-1"))
            size = os.path.getsize(p) if os.path.exists(p) else 0
            if off == size:
                with open(p, "ab") as fh:
                    fh.write(body)
            elif off < 0 or off + len(body) > size:  # a gap: the agent lost track
                return send(h, 409)
            return send(h, 200)
        if op == "done" and method == "POST" and re.fullmatch(r"-?\d+", q.get("rc", "")):
            write_atomic(mb("run", cid + ".rc"), q["rc"].encode())
            return send(h, 200)
        return send(h, 404)

    def cancelled(self, m, cid):
        run = mailbox(self.d, m, "run", cid)
        a = age(run + ".alive")
        return not os.path.exists(run + ".json") or os.path.exists(run + ".cancel") or a is None or a > GONE

    def prune(self):
        """Drop finished or abandoned commands' files (their caller is gone for an hour), and inbox entries nobody claimed."""
        try:
            ms = os.listdir(mailbox(self.d))
        except OSError:
            return
        for m in ms:
            for sub, limit in (("inbox", 2 * CLAIM), ("run", 3600)):
                try:
                    names = os.listdir(mailbox(self.d, m, sub))
                except OSError:
                    continue
                ids = {f.split(".", 1)[0] for f in names if ID_RE.fullmatch(f.split(".", 1)[0])}
                for i in ids:
                    base = mailbox(self.d, m, sub, i)
                    a = age(base + ".alive") if sub == "run" else age(base + ".json")
                    if a is None or a > limit:
                        for f in names:
                            if f.split(".", 1)[0] == i:
                                try:
                                    os.unlink(mailbox(self.d, m, sub, f))
                                except OSError:
                                    pass


def send(h, code, body=b"", headers=None):
    h.send_response(code)
    for k, v in (headers or {}).items():
        h.send_header(k, v)
    h.send_header("Content-Length", str(len(body)))
    h.end_headers()
    h.wfile.write(body)


# ---------------- caller side (xssh) ----------------

def tunnel():
    import tunnel as T
    return T


def server(start=True):
    """→ (server dir, ensured): make sure the file server runs (it serves the mailbox) when start."""
    T = tunnel()
    d = T.srv_dir()
    if not start:
        return d, True
    bind = os.environ.get("TUNNEL_BIND") or T.here()["transfer"][1]
    import contextlib
    with contextlib.redirect_stdout(sys.stderr):  # xssh's stdout is the command's output
        return d, T.ensure_server(bind, d, T.server_key(d))


def agent_machines():
    T = tunnel()
    M = T.machines()
    return M, {n: M.machine(n) for n in M.MACHINES if M.machine(n).get("agent")}


def parse_ssh(argv):
    """ssh's arguments → (options dict of the ones we act on, host, command words); None when there is no host."""
    opts, i = {}, 0
    while i < len(argv) and argv[i].startswith("-") and argv[i] != "-":
        a = argv[i]
        if a == "--":
            i += 1
            break
        if a == "--machine":
            opts["machine"] = argv[i + 1]
            i += 2
            continue
        j = 1
        while j < len(a):
            c = a[j]
            if c in OPT_ARG:
                v = a[j + 1:] or (argv[i + 1] if i + 1 < len(argv) else "")
                if not a[j + 1:]:
                    i += 1
                opts.setdefault(c, []).append(v)
                break
            opts[c] = True
            j += 1
        i += 1
    rest = argv[i:]
    if "machine" in opts:
        return opts, None, rest
    if not rest:
        return None
    return opts, rest[0], rest[1:]


def real_ssh(argv):
    p = os.environ.get("TASKQ_REAL_SSH", "ssh")
    os.execvp(p, [p, *argv])


def xssh_main(argv):
    p = parse_ssh(argv)
    if p is None:
        real_ssh(argv)
    opts, host, words = p
    try:
        M, ag = agent_machines()
        name = opts.get("machine")
        if name is None:
            bare = host.split("@", 1)[-1]
            name = next((n for n, m in ag.items() if m.get("ssh") in (host, bare)), None)
        if name is None or name not in ag:
            if host is None:
                sys.stderr.write(f"xssh: {opts.get('machine')} has no agent (agent=True in the machine table)\n")
                return 255
            real_ssh(argv)
        d, _ = server()
    except Exception as e:  # noqa: BLE001  no config, not the hub: ssh as before
        if host is None:
            sys.stderr.write(f"xssh: {e}\n")
            return 255
        real_ssh(argv)
    if "O" in opts:  # -O check / exit (an ssh master): the agent stands for it
        a = age(mailbox(d, name, "seen"))
        return 0 if a is not None and a < STALE else 255
    return Call(d, name, " ".join(words), stdin=not opts.get("n")).run()


class Call:
    def __init__(self, d, m, cmd, stdin=True):
        self.d, self.m, self.cmd, self.stdin = d, m, cmd, stdin
        self.id = secrets.token_hex(12)
        self.run_ = mailbox(d, m, "run", self.id)

    def fail(self, msg):
        sys.stderr.write(f"xssh: {self.m}: {msg}\n")
        return 255

    def seen_age(self):
        return age(mailbox(self.d, self.m, "seen"))

    def wait_seen(self):
        """The agent called within STALE seconds; right after the server (re)started, give it one retry interval to call."""
        a = self.seen_age()
        if a is not None and a < STALE:
            return True
        end = time.time() + min(STALE, 35)
        start = time.time()
        while time.time() < end:
            a = self.seen_age()
            if a is not None and a < time.time() - start + 1:
                return True
            time.sleep(0.2)
        return False

    def pump_stdin(self):
        fd = os.open(self.run_ + ".in", os.O_WRONLY | os.O_APPEND)
        try:
            if self.stdin and sys.stdin is not None and not os.isatty(0):
                while True:
                    try:
                        b = os.read(0, 1 << 20)
                    except OSError:
                        break
                    if not b:
                        break
                    os.write(fd, b)
        finally:
            os.close(fd)
            touch(self.run_ + ".eof")

    def cleanup(self):
        for f in os.listdir(os.path.dirname(self.run_)):
            if f.startswith(self.id + "."):
                try:
                    os.unlink(os.path.join(os.path.dirname(self.run_), f))
                except OSError:
                    pass
        try:
            os.unlink(mailbox(self.d, self.m, "inbox", self.id + ".json"))
        except OSError:
            pass

    def give_up(self, *_):
        touch(self.run_ + ".cancel")
        os._exit(255)

    def run(self):
        key = read_key(self.d, self.m)
        if not key:
            return self.fail("no agent key on the hub (taskq site agent install)")
        if not self.wait_seen():
            a = self.seen_age()
            return self.fail(f"agent not seen {'ever' if a is None else f'for {int(a)}s'} (taskq site agent status)")
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGPIPE):
            signal.signal(sig, self.give_up)
        touch(self.run_ + ".alive")
        open(self.run_ + ".in", "ab").close()
        ts = str(int(time.time()))
        spec = dict(id=self.id, cmd=self.cmd, ts=ts, sig=A.sign(key, "cmd", self.m, self.id, ts, self.cmd))
        write_atomic(mailbox(self.d, self.m, "inbox", self.id + ".json"), json.dumps(spec).encode())
        threading.Thread(target=self.pump_stdin, daemon=True).start()
        try:
            return self.stream()
        except BrokenPipeError:
            self.give_up()
        finally:
            if not os.path.exists(self.run_ + ".cancel"):
                self.cleanup()

    def stream(self):
        start, offs, beat, pause = time.time(), {"out": 0, "err": 0}, 0, 0.02
        inbox = mailbox(self.d, self.m, "inbox", self.id + ".json")
        while True:
            now = time.time()
            if now - beat > 5:
                touch(self.run_ + ".alive")
                beat = now
            rc = None
            if os.path.exists(self.run_ + ".rc"):
                with open(self.run_ + ".rc") as f:
                    rc = int(f.read().strip() or 255)
            got = False
            for s, fd in (("out", 1), ("err", 2)):
                try:
                    with open(f"{self.run_}.{s}", "rb") as f:
                        f.seek(offs[s])
                        b = f.read()
                except FileNotFoundError:
                    continue
                if b:
                    offs[s] += len(b)
                    got = True
                    while b:
                        b = b[os.write(fd, b):]
            if rc is not None:
                return rc
            if os.path.exists(inbox):
                if now - start > CLAIM:
                    os.unlink(inbox)
                    return self.fail(f"agent did not pick up the command within {int(CLAIM)}s")
            elif (self.seen_age() or STALE) >= STALE:
                touch(self.run_ + ".cancel")
                return self.fail("agent stopped calling while the command ran")
            pause = 0.02 if got else min(pause * 1.5, 0.2)
            time.sleep(pause)


# ---------------- taskq site agent ----------------

BOOT = r'''
import json, os, subprocess, sys
if sys.version_info < (3, 6):
    sys.exit("python %s is too old for the agent (3.6+)" % sys.version.split()[0])
d = json.load(sys.stdin)
h = os.environ.get("TASKQ_AGENT_HOME") or os.path.expanduser("~")
p = os.path.join(h, ".local", "share", "taskq", "agent.py")
c = os.path.join(h, ".config", "taskq")
os.makedirs(os.path.dirname(p), exist_ok=True)
os.makedirs(c, mode=0o700, exist_ok=True)
with open(p + ".tmp", "w") as f:
    f.write(d["code"])
os.replace(p + ".tmp", p)
cf = os.path.join(c, "agent-%s.json" % d["machine"])
fd = os.open(cf + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.write(fd, json.dumps(d["conf"]).encode())
os.close(fd)
os.replace(cf + ".tmp", cf)
py = sys.executable
subprocess.call([py, p, "stop", d["machine"]])
r1 = subprocess.call([py, p, "cron", d["machine"], py]) if d["cron"] else 0
r2 = subprocess.call([py, p, "ensure", d["machine"]])
print("crontab %s, agent %s, python %s %s" % ("ok" if r1 == 0 else "FAILED", "started" if r2 == 0 else "FAILED", py, sys.version.split()[0]))
sys.exit(1 if r2 else 0)
'''


def on(ent, script, inp, timeout=120):
    """Run a bash line on that machine over the real ssh (bootstrap only), or here for the hub's own machine."""
    if ent.get("ssh"):
        ssh = os.environ.get("TASKQ_REAL_SSH", "ssh")
        cmd = [ssh, "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", ent["ssh"], f"bash -c {shlex.quote(script)}"]
    else:
        cmd = ["bash", "-c", script]
    return subprocess.run(cmd, input=inp, capture_output=True, text=True, timeout=timeout)


def install(name, cron=True):
    T = tunnel()
    M = T.machines()
    if name not in M.MACHINES:
        print(f"{name}: not in the machine table", file=sys.stderr)
        return 2
    ent = M.machine(name)
    me = M.machine(M.whoami() or "")
    if not me.get("transfer"):
        print("run on the hub (the machine with the file server, transfer in the machine table)", file=sys.stderr)
        return 2
    d, _ = server()
    key = make_key(d, name)
    url, bind = me["transfer"]
    if not ent.get("ssh"):  # the hub's own machine: straight to the server, not around through the public address
        url = "http://" + bind
    url = os.environ.get("TASKQ_AGENT_URL", url)
    t0 = time.time()
    script = f"py=$(command -v python3) || {{ echo 'no python3 on PATH' >&2; exit 1; }}; exec \"$py\" -c {shlex.quote(BOOT)}"
    r = on(ent, script, json.dumps(dict(code=code_bytes().decode(), machine=name, cron=cron, conf=dict(machine=name, url=url, key=key))))
    print(f"{name}: {(r.stdout + r.stderr).strip()}")
    if r.returncode:
        return 1
    for _ in range(120):
        a = age(mailbox(d, name, "seen"))
        if a is not None and time.time() - a >= t0 - 1:
            print(f"{name}: agent calling the hub ({url})")
            return 0
        time.sleep(0.5)
    print(f"{name}: agent started but has not called the hub within 60s ({url}); see ~/.local/state/taskq/agent-{name}.log there",
          file=sys.stderr)
    return 1


def start(name):
    T = tunnel()
    M = T.machines()
    r = on(M.machine(name), f"exec \"$(command -v python3)\" ~/.local/share/taskq/agent.py ensure {shlex.quote(name)}", "")
    print(f"{name}: {'started (or already running)' if r.returncode == 0 else 'FAILED: ' + (r.stdout + r.stderr).strip()}")
    return r.returncode


def ages():
    """Machines with agent=True → seconds since their agent last called (None: never). For the health check."""
    try:
        M, ag = agent_machines()
        d = tunnel().srv_dir()
    except Exception:  # noqa: BLE001
        return {}
    return {n: age(mailbox(d, n, "seen")) for n in ag}


def status(names, as_json=False):
    M, ag = agent_machines()
    d, _ = server(start=False)
    try:
        boxed = set(os.listdir(mailbox(d)))
    except OSError:
        boxed = set()
    want = A.sha256(code_bytes())
    rows = []
    for n in names or sorted(set(ag) | (boxed & set(M.MACHINES))):
        info = {}
        try:
            with open(mailbox(d, n, "info.json")) as f:
                info = json.load(f)
        except (OSError, ValueError):
            pass
        a = age(mailbox(d, n, "seen"))
        rows.append(dict(machine=n, enabled=n in ag, installed=read_key(d, n) is not None, seen=None if a is None else round(a),
                         up=a is not None and a < STALE, host=info.get("host"), pid=info.get("pid"), python=info.get("py"),
                         running=info.get("running"), current=info.get("sha") == want))
    if as_json:
        print(json.dumps(rows))
        return 0
    for r in rows:
        st = "never called" if r["seen"] is None else (f"up (seen {r['seen']}s ago)" if r["up"] else f"DOWN (seen {r['seen']}s ago)")
        extra = f", {r['host']} pid {r['pid']}, python {r['python']}, {r['running']} running" if r["host"] else ""
        print(f"{r['machine']}: {st}{extra}{'' if r['current'] or not r['host'] else ', older agent (updates itself when idle)'}"
              f"{'' if r['enabled'] else ', not used yet (agent=True not in the machine table)'}"
              f"{'' if r['installed'] else ', not installed'}")
    return 0


USAGE = """taskq site agent install <machine>... [--no-cron]   put the agent there (ssh once; the hub's own machine: here) and start it
taskq site agent start <machine>...                  start it again over ssh (when cron cannot)
taskq site agent status [<machine>...] [--json]      when each agent last called the hub, its host, python and version"""


def main(argv, prog="taskq site agent"):
    if not argv or argv[0] in ("-h", "--help"):
        print(USAGE)
        return 0 if argv else 2
    cmd, rest = argv[0], argv[1:]
    names = [a for a in rest if not a.startswith("-")]
    if cmd == "install" and names:
        return max(install(n, cron="--no-cron" not in rest) for n in names)
    if cmd == "start" and names:
        return max(start(n) for n in names)
    if cmd == "status":
        return status(names, "--json" in rest)
    print(USAGE, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
