#!/usr/bin/env python3
"""Transfer large files between this machine (the store, or the hub when sending env bundles) and other machines over a
Cloudflare tunnel from here. The other side opens the connection and runs no service; this machine's address is its
`transfer` entry in the machine table (temporary: `quick`). push: the other side downloads from the file server here
(on Slurm a large transfer can download on a CPU node, see `transfer_job`); pull: it uploads in 90 MB chunks (Cloudflare
caps one request at 100 MB). Transfers for one machine run one at a time; different machines share one file server process.

  taskq tunnel rsync <rsync args> <source> <target>  drop-in rsync: transfer the files through the tunnel to a staging
                                                     dir next to the target, run rsync as usual (with --link-dest, so
                                                     matching files do not go over ssh), then delete the staging dir
  taskq tunnel push <machine> <source dir here> <target dir there> <list>   list: one path relative to the source dir
  taskq tunnel pull <machine> <source dir there> <target dir here> <list>   list: one "relpath<TAB>bytes" per line
"""
import fcntl
import hashlib
import http.client
import http.server
import json
import os
import re
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # scheduler code root
CHUNK = int(os.environ.get("TUNNEL_CHUNK", 90 * 2 ** 20))
NPAR = int(os.environ.get("TUNNEL_NPAR", 8))
MIN_JOB = float(os.environ.get("TUNNEL_MIN_JOB", 1e9))
JOB_WAIT = 600
STALL = int(os.environ.get("TUNNEL_STALL", 3600))
ROUNDS = 3  # retry missing or mismatched files within the same run (a few drop when there are many)
POLL = float(os.environ.get("TUNNEL_POLL", 60))
SSH = shlex.split(os.environ.get("TUNNEL_SSH", "ssh"))
LOCK = os.environ.get("TUNNEL_LOCK")
IDLE = float(os.environ.get("TUNNEL_IDLE", 300))
TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{16,}")
UA = {"User-Agent": "xfer/1"}
STAGE_RE = re.compile(r"\.tunnel_stage_[0-9a-f]{12}$")


def log(msg):
    print(f"{time.strftime('%F %T')} {msg}", flush=True)


def machines():
    sys.path.insert(0, os.path.join(CORE, "queue"))
    import cfg
    cfg.use_config()
    return cfg.mtab()


def state(*parts, **kw):
    machines()
    import cfg
    return cfg.state(*parts, **kw)


def ssh(host, cmd, check=True, input=None, timeout=300):
    """Run one short command on the other side, bounded there too so a hang in storage leaves no process."""
    r = subprocess.run(SSH + [host, f"timeout -k 10 {timeout - 15} bash -c {shlex.quote(cmd)}"], input=input, capture_output=True, text=True,
                       timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f"ssh {host} failed ({r.returncode}): {r.stderr.strip()[-300:]}")
    return r.stdout


def rq(path):
    return "~/" + shlex.quote(path[2:]) if path.startswith("~/") else shlex.quote(path)


def md5_of(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while b := f.read(1 << 20):
            h.update(b)
    return h.hexdigest()


# ---------------- file server on this machine ----------------
# All transfers share one file server process on bind (_serve): the first transfer starts it, another starts it again if
# it died, and it exits after IDLE seconds without sessions. Each transfer is a session:
# <server dir>/<token>.json holds the list, src / dst and the transfer process pid (the server drops sessions whose pid
# is gone); progress (bytes moved, MD5 of sent files, whether received files match) is appended to <token>.out, which
# the transfer process reads.

def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True


class Served:
    """One session inside the server process."""

    def __init__(self, path, d):
        self.files = {r: tuple(v) for r, v in d["files"].items()}
        self.src, self.dst, self.chunk, self.pid = d.get("src"), d.get("dst"), d.get("chunk", CHUNK), d["pid"]
        self.out = path[:-len(".json")] + ".out"
        self.lock = threading.Lock()
        self.pending, self.flushed = 0, time.time()

    def emit(self, *ev):
        fd = os.open(self.out, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(fd, (json.dumps(ev) + "\n").encode())
        finally:
            os.close(fd)

    def touch(self, n, flush=False):
        with self.lock:
            self.pending += n
            if self.pending and (flush or time.time() - self.flushed >= 2):
                self.emit("M", self.pending, time.time())
                self.pending, self.flushed = 0, time.time()


class Registry:
    def __init__(self, d):
        self.d, self.cache = d, {}

    def get(self, token):
        p = os.path.join(self.d, token + ".json")
        if not TOKEN_RE.fullmatch(token) or not os.path.exists(p):
            self.cache.pop(token, None)
            return None
        if token not in self.cache:
            try:
                with open(p) as f:
                    self.cache[token] = Served(p, json.load(f))
            except (OSError, ValueError, KeyError):
                return None
        return self.cache[token]

    def prune(self):
        """Delete sessions whose transfer process is gone → number left; server dir gone → None."""
        try:
            names = os.listdir(self.d)
        except FileNotFoundError:
            return None
        n = 0
        for f in names:
            if not f.endswith(".json"):
                continue
            t = f[:-len(".json")]
            s = self.get(t)
            if s and alive(s.pid):
                n += 1
                continue
            if s:
                for ext in (".json", ".out"):
                    try:
                        os.unlink(os.path.join(self.d, t + ext))
                    except FileNotFoundError:
                        pass
            self.cache.pop(t, None)
        return n


def handler(reg):
    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def parse_request(self):
            if not super().parse_request():
                return False
            if os.environ.get("TUNNEL_TEST_REQUIRE_UA") and self.headers.get("User-Agent") != UA["User-Agent"]:
                self.send_error(403)
                return False
            return True

        def reply(self, code, body=b""):
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def session(self):
            """/<token>/<relpath>?query → (session, relpath, params); bad token → session is None."""
            p = urllib.parse.urlsplit(self.path)
            parts = p.path.split("/", 2)
            if len(parts) < 3:
                return None, None, None
            return reg.get(parts[1]), urllib.parse.unquote(parts[2]), urllib.parse.parse_qs(p.query)

        def do_GET(self):
            if self.path == "/":
                return self.reply(200, b"ok")
            S, rel, _ = self.session()
            if S is not None and rel == "":
                return self.reply(200, b"ok")  # the transfer process checks that the server knows this session
            if S is None or S.src is None or rel not in S.files:
                return self.reply(404)
            size = S.files[rel][0]
            h, n = hashlib.md5(), 0
            with open(os.path.join(S.src, rel), "rb") as f:
                self.send_response(200)
                self.send_header("Content-Length", str(size))
                self.end_headers()
                while n < size and (b := f.read(min(1 << 20, size - n))):
                    self.wfile.write(b)
                    h.update(b)
                    n += len(b)
                    S.touch(len(b))
            S.touch(0, flush=True)
            if n == size:
                S.emit("S", rel, h.hexdigest())

        def do_PUT(self):
            S, rel, q = self.session()
            if S is None or S.dst is None or rel not in S.files:
                return self.reply(404)
            size = S.files[rel][0]
            n = int(self.headers.get("Content-Length", -1))
            off = int((q.get("off") or ["-1"])[0])
            if n < 0 or off < 0 or off + n > size or n > S.chunk:
                return self.reply(400)
            part = os.path.join(S.dst, rel) + ".tunnelpart"
            os.makedirs(os.path.dirname(part), exist_ok=True)
            fd = os.open(part, os.O_WRONLY | os.O_CREAT, 0o600)
            try:
                got = 0
                while got < n:
                    b = self.rfile.read(min(1 << 20, n - got))
                    if not b:
                        break
                    os.pwrite(fd, b, off + got)
                    got += len(b)
                    S.touch(len(b))
            finally:
                os.close(fd)
            S.touch(0, flush=True)
            self.reply(200 if got == n else 400)

        def do_POST(self):
            S, rel, q = self.session()
            if S is None or S.dst is None or rel not in S.files:
                return self.reply(404)
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            part = os.path.join(S.dst, rel) + ".tunnelpart"
            g = {k: v[0] for k, v in q.items()}
            ok = os.path.isfile(part) and os.path.getsize(part) == S.files[rel][0] and md5_of(part) == g.get("md5")
            if ok:
                m = int(g["mtime"])
                os.chmod(part, int(g["mode"]) & 0o7777)
                os.utime(part, ns=(m, m))
                os.replace(part, os.path.join(S.dst, rel))
            elif os.path.exists(part):
                os.unlink(part)
            S.emit("D", rel, ok)
            self.reply(200 if ok else 409)

    return H


def serve_main(bind, d):
    """Shared file server (tunnel.py _serve); exits if bind already has one."""
    reg = Registry(d)
    host, port = bind.rsplit(":", 1)
    try:
        srv = http.server.ThreadingHTTPServer((host, int(port)), handler(reg))
    except OSError as e:
        log(f"{bind} already has a server ({e}), not starting another")
        return 0
    srv.daemon_threads = True
    pidf = os.path.join(d, "serve.pid")
    with open(pidf, "w") as f:
        f.write(f"{os.getpid()}\n")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log(f"file server up on {bind} (pid {os.getpid()})")
    last = time.time()
    while True:
        time.sleep(min(10, IDLE / 3))
        n = reg.prune()
        if n is None:
            break
        if n:
            last = time.time()
        elif time.time() - last >= IDLE:
            break
    srv.shutdown()
    srv.server_close()
    try:
        if open(pidf).read().strip() == str(os.getpid()):
            os.unlink(pidf)
    except OSError:
        pass
    log("no sessions left, file server exits")
    return 0


def srv_dir():
    d = os.path.join(os.path.dirname(lock_path()), "tunnel_srv")
    os.makedirs(d, mode=0o700, exist_ok=True)
    return d


class Session:
    """One transfer's session on the shared file server (transfer-process side)."""

    def __init__(self, files, src=None, dst=None):
        self.token = secrets.token_urlsafe(24)
        self.files, self.src, self.dst = files, src, dst
        self.sent, self.done, self.moved, self.last, self.off = {}, {}, 0, time.time(), 0
        url, bind = here()["transfer"]
        self.url, self.bind = os.environ.get("TUNNEL_URL", url), os.environ.get("TUNNEL_BIND", bind)
        self.cid = None
        self.dir = srv_dir()
        self.base = os.path.join(self.dir, self.token)
        tmp = os.path.join(self.dir, f".{self.token}.tmp")
        with open(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f:
            json.dump(dict(files=files, src=src and os.path.abspath(src), dst=dst and os.path.abspath(dst), chunk=CHUNK, pid=os.getpid()), f)
        os.replace(tmp, self.base + ".json")

    def ensure(self):
        """Start a server if nothing listens on bind (of two started at once, the second cannot bind and exits)."""
        h, p = self.bind.rsplit(":", 1)
        try:
            with socket.create_connection(("127.0.0.1" if h in ("", "0.0.0.0") else h, int(p)), timeout=3):
                return
        except OSError:
            pass
        with open(os.path.join(self.dir, "serve.log"), "a") as lf:
            subprocess.Popen([sys.executable, os.path.abspath(__file__), "_serve", self.bind, self.dir],
                             cwd=self.dir, stdin=subprocess.DEVNULL, stdout=lf, stderr=subprocess.STDOUT, start_new_session=True)

    def open(self, quick=False):
        """→ address for the other side. quick: use a temporary address (this machine's quick, an executor's quick_tunnel) instead of this machine's fixed address."""
        quick = quick and "TUNNEL_URL" not in os.environ
        for attempt in range(3 if quick else 1):
            self.ensure()
            if quick:
                if self.cid:
                    subprocess.run(["docker", "stop", self.cid], capture_output=True)
                self.cid, self.url = quick_tunnel(self.bind)
            wait = 0.2
            for _ in range(60 if quick else 30):
                try:
                    if urllib.request.urlopen(urllib.request.Request(f"{self.url}/{self.token}/", headers=UA), timeout=10).read() == b"ok":
                        return self.url
                except Exception:
                    pass
                time.sleep(wait)
                wait = min(2 * wait, 2)
                self.ensure()
        raise RuntimeError(f"cannot reach the file server at {self.url} (traefik route or tunnel)")

    def poll(self):
        try:
            with open(self.base + ".out", "rb") as f:
                f.seek(self.off)
                b = f.read()
        except FileNotFoundError:
            return
        b = b[:b.rfind(b"\n") + 1]
        self.off += len(b)
        for ln in b.splitlines():
            k, a, c = json.loads(ln)
            if k == "M":
                self.moved, self.last = self.moved + a, max(self.last, c)
            elif k == "S":
                self.sent[a] = c
            elif k == "D":
                self.done[a] = c

    def close(self):
        if self.cid:
            subprocess.run(["docker", "stop", self.cid], capture_output=True)
        for ext in (".json", ".out"):
            try:
                os.unlink(self.base + ext)
            except FileNotFoundError:
                pass


def entry(host, site=None):
    """Machine entry for the other side: look up by name if site is given, else by ssh name; not in the machine table → {}."""
    M = machines()
    if site is not None:
        return M.machine(site) if site in M.MACHINES else {}
    return next((M.machine(n) for n, m in M.MACHINES.items() if m.get("ssh") == host), {})


def here():
    """Machine entry for this machine (the one running the transfer): TASKQ_MACHINE, or match the hostname."""
    M = machines()
    me = M.whoami()
    if me is None or not M.machine(me).get("transfer"):
        raise RuntimeError(f"this machine ({me or 'not in the machine table'}) has no transfer (file server address), cannot use the tunnel")
    return M.machine(me)


def quick_tunnel(bind):
    image, pat = here()["quick"]
    cid = subprocess.run(["docker", "run", "-d", "--rm", "--network", "host", image, "tunnel", "--no-autoupdate", "--url", f"http://{bind}"],
                         capture_output=True, text=True, check=True).stdout.strip()
    for _ in range(60):
        r = subprocess.run(["docker", "logs", cid], capture_output=True, text=True)
        m = re.search(pat, r.stdout + r.stderr)
        if m:
            return cid, m.group(0)
        time.sleep(1)
    subprocess.run(["docker", "stop", cid], capture_output=True)
    raise RuntimeError("temporary tunnel gave no address")


def lock_path(host=None):
    base = os.path.expanduser(LOCK) if LOCK is not None else state("tunnel_push.lock")
    return base if host is None else f"{base}.{re.sub(r'[^A-Za-z0-9_.-]', '_', host)}"


def take_lock(host):
    """Transfers for one machine (by ssh name) run one at a time; different machines run at once. → the held lock (released when the process exits)."""
    path = lock_path(host)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    f = open(path, "a")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log(f"another tunnel transfer to {host} is running, waiting for it")
        fcntl.flock(f, fcntl.LOCK_EX)
    return f


def status_cmd(wd):
    """The remote transfer: done / run / dead, plus the last "at ..." line. Check done again after the process is gone (exiting right after writing done is not dead)."""
    return (f"cd {wd} && if [ -e done ]; then echo done; elif pgrep -f '[b]ash {wd}/run.sh' > /dev/null; then echo run; "
            f"elif [ -e done ]; then echo done; else echo dead; fi; grep '^at ' log 2> /dev/null | tail -1")


def run_remote(host, mode, plan, serve, total, site=None):
    """Create the work dir on the other side, put the script and plan there, run in the background, wait for done; return
    results there (relpath → md5). Work dir: under the root's tmp when the machine has a root, else in the state dir.
    Python: the machine's conda if it has one, else python3."""
    sys.path.insert(0, os.path.join(CORE, "queue"))
    import proto
    m = entry(host, site)
    pyx = m["conda"][0] + "/bin/python3" if m.get("conda") else "python3"
    with open(__file__) as f:
        src = f.read()
    base = m["root"].rstrip("/") + "/" + proto.ROOT["tmp"] if m.get("root") else state("tunnel", expand=False)
    job = m.get("transfer_job") if mode == "_fetch" and total >= MIN_JOB else None
    script = "xfer.py"
    wd = ssh(host, f"d={base}/{secrets.token_hex(6)} && mkdir -p {base} && mkdir -m 700 $d && cd $d && pwd").strip()
    ssh(host, f"cat > {wd}/{script}", input=src)
    ssh(host, f"cat > {wd}/plan.json", input=json.dumps(plan))
    run = [f"cd {wd}"]
    if job:
        t = max(30, int(total / 5e6 / 60) + 10)
        sys.path.insert(0, os.path.join(CORE, "sites"))
        import slurm  # random job name each time
        run.append(f"srun -I{JOB_WAIT} {job} -t {t} -J {slurm.jobname(m.get('job_names'))} {pyx} {script} {mode} plan.json >> log 2>&1")
    run.append(f"grep -q '^complete' log 2> /dev/null || {pyx} {script} {mode} plan.json >> log 2>&1")
    run.append("touch done")
    ssh(host, f"cat > {wd}/run.sh", input="\n".join(run) + "\n")
    serve.ensure()
    ssh(host, f"nohup bash {wd}/run.sh > /dev/null 2>&1 < /dev/null &")
    t0, seen, where, wait = time.time(), time.time(), "", 2
    try:
        while True:
            time.sleep(min(wait, POLL))
            wait *= 2
            try:
                st = ssh(host, status_cmd(wd)).split("\n")
                seen = time.time()
            except RuntimeError:
                st = [""]
            serve.ensure()  # restart the shared server if it died; curl and uploads there retry
            serve.poll()
            if len(st) > 1 and st[1] and st[1] != where:
                where = st[1]
                log(f"remote {where}")
            if st[0] == "done":
                break
            if st[0] == "dead":
                raise RuntimeError("the transfer process on the other side is gone without writing done: " + ssh(host, f"tail -5 {wd}/log", check=False))
            if time.time() - seen > STALL:
                raise RuntimeError(f"cannot reach {host} for {STALL // 60} minutes")
            if time.time() - serve.last > STALL:
                raise RuntimeError(f"no data through the tunnel for {STALL // 60} minutes")
            dt = time.time() - t0
            if wait < POLL:
                continue
            log(f"transferred {serve.moved / 1e9:.1f} / {total / 1e9:.1f} GB, average {serve.moved / 1e6 / dt:.1f} MB/s")
        out = ssh(host, f"cat {wd}/results.tsv 2> /dev/null || true; echo ===; grep '^failed' {wd}/log || true")
        res, fails = out.split("===\n", 1)
        for ln in fails.splitlines():
            log(ln)
        return dict(ln.split("\t", 1) for ln in res.splitlines() if "\t" in ln)
    finally:
        ssh(host, f"rm -rf {wd}", check=False)


def home_path(p):
    """A path on the other side (~, absolute, or relative to home) → absolute path."""
    return os.path.join(os.path.expanduser("~"), os.path.expanduser(p))


def local_copy(src, dst, rels, link=False):
    """Executor on the same machine as this one: copy src/<rel> → dst/<rel> here directly, no tunnel.
    Write to .tunnelpart in the same dir, check the size (push also checks MD5), keep mtime and mode, then rename; skip
    files that exist with the same size and mtime. link (push): try a hard link first (blobs are named by content and
    never change, same as ingest), copy and check MD5 if on another disk. Return the files not copied (partials removed)."""
    bad = []
    for r in rels:
        a, b = os.path.join(src, r), os.path.join(dst, r)
        part = b + ".tunnelpart"
        try:
            st = os.stat(a)
            if os.path.isfile(b) and os.stat(b).st_size == st.st_size and os.stat(b).st_mtime_ns == st.st_mtime_ns:
                continue
            os.makedirs(os.path.dirname(b), exist_ok=True)
            if os.path.lexists(part):
                os.unlink(part)
            try:
                if not link:
                    raise OSError
                os.link(os.path.realpath(a), part)
                linked = True
            except OSError:
                shutil.copy2(a, part)
                linked = False
            if os.path.getsize(part) != st.st_size:
                raise IOError(f"copied only {os.path.getsize(part)} / {st.st_size} bytes")
            if link and not linked and md5_of(part) != md5_of(a):
                raise IOError("MD5 mismatch")
            os.replace(part, b)
        except OSError as e:
            log(f"failed {r}: {e}")
            bad.append(r)
        finally:
            if os.path.lexists(part):
                os.unlink(part)
    return bad


def push(host, src, dst, rels, site=None):
    """This machine's src/<rel> → dst/<rel> on the other side (dst relative to its home, or absolute). Retry files that
    did not arrive or do not match a few times (ROUNDS); return the ones still bad (already deleted). site: the machine
    name there (entry)."""
    files = {}
    for r in rels:
        st = os.stat(os.path.join(src, r))
        files[r] = (st.st_size, st.st_mtime_ns, st.st_mode & 0o7777)
    total = sum(v[0] for v in files.values())
    if host is None:
        log(f"local copy of {len(files)} files, {total / 1e9:.2f} GB to {dst}")
        return local_copy(src, home_path(dst), rels, link=True)
    lk = take_lock(host)
    serve = Session(files, src=src)
    try:
        url = serve.open(quick=bool(entry(host, site).get("quick_tunnel")))
        log(f"tunnel transfer of {len(files)} files, {total / 1e9:.2f} GB to {host}:{dst}")
        t0, bad = time.time(), list(files)
        for k in range(ROUNDS + 1):
            if k:
                log(f"retry {len(bad)} missing (round {k})")
            serve.poll()
            for r in bad:
                serve.sent.pop(r, None)
            plan = dict(base=f"{url}/{serve.token}", root=dst, npar=NPAR, round=k, files=[[r, *files[r]] for r in bad])
            try:
                res, stop = run_remote(host, "_fetch", plan, serve, sum(files[r][0] for r in bad), site), False
            except RuntimeError as e:
                if not k:
                    raise
                log(f"retry failed ({e}), not retrying again")
                res, stop = {}, True
            serve.poll()
            bad = [r for r in bad if res.get(r) != (serve.sent.get(r) or md5_of(os.path.join(src, r)))]
            if bad:
                ssh(host, "cd " + rq(dst) + " && rm -f -- " + " ".join(shlex.quote(r) for r in bad), check=False)
            if not bad or stop:
                break
    finally:
        serve.close()
        lk.close()
    dt = time.time() - t0
    log(f"done: {len(files) - len(bad)} files match, {len(bad)} missing or mismatched (deleted); {dt / 60:.0f} min, average {total / 1e6 / dt:.1f} MB/s")
    return bad


def pull(host, src, dst, items, site=None):
    """src/<rel> on the other side → dst/<rel> here. items: [(rel, bytes)]. Retry files that did not arrive a few times (ROUNDS); return the ones still missing."""
    files = {r: (int(n),) for r, n in items}
    total = sum(v[0] for v in files.values())
    os.makedirs(dst, exist_ok=True)
    if host is None:
        log(f"local copy of {len(files)} files, {total / 1e9:.2f} GB from {src}")
        return local_copy(home_path(src), dst, list(files))
    lk = take_lock(host)
    serve = Session(files, dst=dst)
    try:
        url = serve.open(quick=bool(entry(host, site).get("quick_tunnel")))
        log(f"tunnel transfer of {len(files)} files, {total / 1e9:.2f} GB from {host}:{src}")
        t0, bad = time.time(), list(files)
        for k in range(ROUNDS + 1):
            if k:
                log(f"retry {len(bad)} missing (round {k})")
            serve.poll()
            for r in bad:
                serve.done.pop(r, None)
            plan = dict(base=f"{url}/{serve.token}", root=src, chunk=CHUNK, npar=NPAR, round=k, files=[[r, files[r][0]] for r in bad])
            try:
                run_remote(host, "_send", plan, serve, sum(files[r][0] for r in bad), site)
                stop = False
            except RuntimeError as e:
                if not k:
                    raise
                log(f"retry failed ({e}), not retrying again")
                stop = True
            serve.poll()
            bad = [r for r in bad if not serve.done.get(r)]
            if not bad or stop:
                break
    finally:
        serve.close()
        lk.close()
        for r in files:
            p = os.path.join(dst, r) + ".tunnelpart"
            if os.path.exists(p):
                os.unlink(p)
    dt = time.time() - t0
    log(f"done: {len(files) - len(bad)} files match, {len(bad)} missing or mismatched; {dt / 60:.0f} min, average {total / 1e6 / dt:.1f} MB/s")
    return bad


# ---------------- remote side ----------------

def retry(f, what):
    for i in range(5):
        try:
            return f()
        except Exception as e:
            err = e
            time.sleep(10 * (i + 1))
    print(f"failed {what}: {getattr(err, 'stderr', '') or err}", flush=True)
    return None


def remote_fetch(plan):
    root = home_path(plan["root"])
    out = open("results.tsv", "a")
    lock = threading.Lock()
    bad = os.environ.get("TUNNEL_TEST_BADMD5")
    drop = (os.environ.get("TUNNEL_TEST_DROP") or "").split(",") if not plan.get("round") else []

    def one(item):
        rel, size, mtime, mode = item
        p = os.path.join(root, rel)
        if rel in drop:
            print(f"failed {rel}: test", flush=True)
            return
        if os.path.isfile(p) and os.stat(p).st_size == size and os.stat(p).st_mtime_ns == mtime:
            digest = md5_of(p)
        else:
            def get():
                os.makedirs(os.path.dirname(p), exist_ok=True)
                cfg = {"url": plan["base"] + "/" + urllib.parse.quote(rel), "user-agent": UA["User-Agent"], "output": p + ".tunnelpart"}
                cfg = "".join(f'{k} = "{v.replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"\n' for k, v in cfg.items())
                subprocess.run(["curl", "-sSf", "--retry", "5", "--retry-all-errors", "--speed-limit", "1024", "--speed-time", "180", "-K", "-"],
                               input=cfg, text=True, check=True, capture_output=True)  # address and token stay out of the process list
                n = os.path.getsize(p + ".tunnelpart")
                if n != size:
                    raise IOError(f"received only {n} / {size} bytes")
                digest = md5_of(p + ".tunnelpart")
                os.chmod(p + ".tunnelpart", mode)
                os.utime(p + ".tunnelpart", ns=(mtime, mtime))
                os.replace(p + ".tunnelpart", p)
                return digest
            digest = retry(get, rel)
            if digest is None:
                return
        if rel == bad:
            digest = "0" * 32
        with lock:
            out.write(f"{rel}\t{digest}\n")
            out.flush()

    with ThreadPoolExecutor(plan.get("npar", NPAR)) as ex:
        list(ex.map(one, plan["files"]))
    print("complete", flush=True)


def remote_send(plan):
    root = home_path(plan["root"])
    base = urllib.parse.urlsplit(plan["base"])
    chunk = plan["chunk"]
    bad = os.environ.get("TUNNEL_TEST_BADMD5")
    drop = (os.environ.get("TUNNEL_TEST_DROP") or "").split(",") if not plan.get("round") else []
    local = threading.local()

    def req(method, path, f=None, off=0, n=0, h=None):
        for i in range(5):
            try:
                c = getattr(local, "c", None)
                if c is None:
                    cls = http.client.HTTPSConnection if base.scheme == "https" else http.client.HTTPConnection
                    c = local.c = cls(base.netloc, timeout=300)
                hh = h.copy() if h else None
                c.putrequest(method, base.path + "/" + path)
                c.putheader("Content-Length", str(n))
                c.putheader("User-Agent", UA["User-Agent"])
                c.endheaders()
                if f:
                    f.seek(off)
                    left = n
                    while left:
                        b = f.read(min(1 << 20, left))
                        if not b:
                            raise IOError("file shrank")
                        c.send(b)
                        hh.update(b)
                        left -= len(b)
                r = c.getresponse()
                r.read()
                return r.status, hh
            except Exception as e:
                err = e
                local.c = None
                time.sleep(10 * (i + 1))
        raise IOError(err)

    def one(item):
        rel, size = item
        p = os.path.join(root, rel)
        if rel in drop:
            print(f"failed {rel}: test", flush=True)
            return
        try:
            st = os.stat(p)
            if st.st_size != size:
                raise IOError(f"size changed ({st.st_size}, list has {size})")
            h, off = hashlib.md5(), 0
            with open(p, "rb") as f:
                while True:
                    n = min(chunk, size - off)
                    code, h = req("PUT", urllib.parse.quote(rel) + f"?off={off}", f, off, n, h)
                    if code != 200:
                        raise IOError(f"chunk {off // chunk + 1} failed ({code})")
                    off += n
                    if off >= size:
                        break
            q = urllib.parse.urlencode(dict(md5="0" * 32 if rel == bad else h.hexdigest(), mtime=st.st_mtime_ns, mode=st.st_mode & 0o7777))
            if req("POST", urllib.parse.quote(rel) + "?" + q)[0] != 200:
                raise IOError("checksum mismatch")
        except Exception as e:
            print(f"failed {rel}: {e}", flush=True)

    with ThreadPoolExecutor(plan.get("npar", NPAR)) as ex:
        list(ex.map(one, plan["files"]))
    print("complete", flush=True)


# ---------------- rsync stand-in ----------------

VALUE_OPTS = {"-e", "--rsh", "-f", "--filter", "--exclude", "--include", "--files-from", "-T", "--temp-dir",
              "--link-dest", "--copy-dest", "--compare-dest", "--out-format", "--log-file", "-M", "--remote-option"}
NO_TUNNEL = {"-R", "--relative", "-n", "--dry-run", "--list-only", "--files-from", "--link-dest", "--copy-dest", "--compare-dest", "-e", "--rsh"}


def split_args(args):
    opts, pos, i = [], [], 0
    while i < len(args):
        a = args[i]
        if a.startswith("-") and pos:
            return None
        if a in VALUE_OPTS:
            opts += args[i:i + 2]
            i += 2
            continue
        (opts if a.startswith("-") else pos).append(a)
        i += 1
    return opts, pos


def plain(args):
    os.execvp("rsync", ["rsync"] + args)


def wrap(args):
    sp = split_args(args)
    if not sp or len(sp[1]) != 2:
        plain(args)
    opts, (src, dst) = sp
    flags = {o.split("=", 1)[0] for o in opts}
    if flags & NO_TUNNEL or any(re.fullmatch(r"-[a-zA-Z]*[Rn][a-zA-Z]*", o) for o in opts):
        plain(args)
    M = machines()
    me = M.whoami()
    if me is None or not M.machine(me).get("transfer"):  # no file server on this machine: run rsync as is
        plain(args)
    hosts = {m["ssh"]: m for m in M.MACHINES.values() if m.get("ssh")}
    rs, rd = (re.match(r"^([^/:]+):(.*)$", x) for x in (src, dst))
    if bool(rs) == bool(rd):
        plain(args)
    host = (rs or rd).group(1)
    way = "pull" if rs else "push"
    if host not in hosts or way not in (hosts[host].get("tunnel") or ()):
        plain(args)
    r = subprocess.run(["rsync"] + opts + ["-n", "--out-format=%i\t%l\t%n", src, dst], capture_output=True, text=True)
    if r.returncode:
        plain(args)
    items = []
    for ln in r.stdout.splitlines():
        p = ln.split("\t", 2)
        if len(p) == 3 and len(p[0]) > 1 and p[0][0] in "<>" and p[0][1] == "f":
            items.append((p[2], int(p[1])))
    if not items:
        plain(args)
    sroot = rs.group(2) if rs else src
    if not sroot.endswith("/"):
        plain(args)
    dpath = (rd.group(2) if rd else dst).rstrip("/")
    if not dpath or dpath in (".", ".."):
        plain(args)
    name = f".tunnel_stage_{secrets.token_hex(6)}"
    stage = os.path.join(os.path.dirname(dpath), name)
    try:
        if way == "pull":
            pull(host, sroot, stage, items)
        else:
            ssh(host, "mkdir -p " + rq(stage))
            push(host, sroot, stage, [i[0] for i in items])
    except Exception as e:
        log(f"tunnel transfer failed ({e}), sending everything with rsync over ssh")
    try:
        rc = subprocess.run(["rsync"] + opts + [f"--link-dest=../{name}", src, dst]).returncode
    finally:
        assert STAGE_RE.search(stage)
        if way == "pull":
            shutil.rmtree(stage, ignore_errors=True)
        else:
            ssh(host, "rm -rf " + rq(stage), check=False)
    return rc


def main(argv):
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    cmd, a = argv[0], argv[1:]
    helps = ("-h", "--help")
    if cmd in helps or (cmd == "rsync" and a[:1] and a[0] in helps) or (cmd in ("push", "pull") and set(a) & set(helps)):
        print(__doc__.strip())  # --help does nothing (rsync only takes the first argument right after it; a later -h belongs to rsync)
        return 0
    if cmd == "rsync":
        if not a:
            print(__doc__.strip(), file=sys.stderr)
            return 2
        return wrap(a)
    if cmd == "_serve" and len(a) == 2:
        return serve_main(*a)
    if cmd in ("_fetch", "_send"):
        print(f"at {os.uname().nodename}: {'download' if cmd == '_fetch' else 'upload'}", flush=True)
        plan = json.load(open(a[0]))
        (remote_fetch if cmd == "_fetch" else remote_send)(plan)
        return 0
    if cmd in ("push", "pull") and len(a) == 4:
        M = machines()
        from proto import EXIT
        prog = f"{os.path.basename(sys.argv[0])} {cmd}"  # tunnel.py push, or taskq tunnel push
        try:
            host, site = M.machine(a[0]).get("ssh"), (a[0] if M.MACHINES[a[0]].get("root") else None)
        except KeyError:
            print(f"{prog}: no {a[0]} in the machine table", file=sys.stderr)
            return EXIT["gone"]
        if not host and not site:
            print(f"{prog}: {a[0]} has no ssh, cannot transfer", file=sys.stderr)
            return 2
        try:
            lines = [ln for ln in open(a[3]).read().splitlines() if ln]
        except OSError as e:
            print(f"{prog}: cannot read list {a[3]}: {e.strerror}", file=sys.stderr)
            return 2
        if cmd == "push":
            return 1 if push(host, a[1], a[2], lines, site=site) else 0
        return 1 if pull(host, a[1], a[2], [ln.split("\t") for ln in lines], site=site) else 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
