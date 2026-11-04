#!/usr/bin/env python3
"""box 和别的机器之间经 box 的 Cloudflare 隧道传大批文件（2026-10-26），两个方向的连接都由那边发起，那边不开服务：
  push：box 临时开文件服务，那边下载（DICC 上 1 GB 以上的在 CPU 节点上下载，见 envs/machines.py 的 transfer_job）
  pull：box 临时开接收服务，那边按 90 MB 一块上传（Cloudflare 一次请求最多 100 MB）
做法、测速和安全措施见 scripts/sync/README.md。

  tunnel.py rsync <rsync 的参数> <源> <目标>     rsync 的替身：先把要传的文件经隧道传到目标旁边的暂存目录，再照原样跑 rsync
                                                （加 --link-dest，对上的文件不再经 ssh 传），最后删暂存目录
  tunnel.py push <机器> <box 上的源目录> <那边的目标目录> <清单>   清单每行一个相对源目录的路径
  tunnel.py pull <机器> <那边的源目录> <box 上的目标目录> <清单>   清单每行"相对路径<TAB>字节数"
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
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
CHUNK = int(os.environ.get("TUNNEL_CHUNK", 90 * 2 ** 20))
NPAR = int(os.environ.get("TUNNEL_NPAR", 8))
MIN_JOB = float(os.environ.get("TUNNEL_MIN_JOB", 1e9))
JOB_WAIT = 600
STALL = int(os.environ.get("TUNNEL_STALL", 3600))
ROUNDS = 3  # 没传到、对不上的文件同一次里再补传几轮（11/2：一万多个文件偶尔断几个）
POLL = float(os.environ.get("TUNNEL_POLL", 60))
SSH = shlex.split(os.environ.get("TUNNEL_SSH", "ssh"))
LOCK = os.environ.get("TUNNEL_LOCK")
UA = {"User-Agent": "xfer/1"}
STAGE_RE = re.compile(r"\.tunnel_stage_[0-9a-f]{12}$")


def log(msg):
    print(f"{time.strftime('%F %T')} {msg}", flush=True)


def machines():
    sys.path.insert(0, os.path.join(CORE, "queue"))
    import cfg
    cfg.use_config()
    import machines as M
    return M


def state(*parts, **kw):
    machines()
    import cfg
    return cfg.state(*parts, **kw)


def ssh(host, cmd, check=True, input=None, timeout=300):
    """那边跑一条短命令；那边也限时，卡在存储里时不留进程（11/1）。"""
    r = subprocess.run(SSH + [host, f"timeout -k 10 {timeout - 15} bash -c {shlex.quote(cmd)}"], input=input, capture_output=True, text=True,
                       timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f"ssh {host} 出错（{r.returncode}）：{r.stderr.strip()[-300:]}")
    return r.stdout


def rq(path):
    return "~/" + shlex.quote(path[2:]) if path.startswith("~/") else shlex.quote(path)


def md5_of(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while b := f.read(1 << 20):
            h.update(b)
    return h.hexdigest()


# ---------------- box 上的服务 ----------------

class Serve:
    def __init__(self, token, files, src=None, dst=None):
        self.token, self.files, self.src, self.dst = token, files, src, dst
        self.sent, self.done, self.moved, self.last = {}, {}, 0, time.time()
        self.lock = threading.Lock()

    def touch(self, n):
        with self.lock:
            self.moved += n
            self.last = time.time()

    def rel(self, path):
        p = urllib.parse.urlsplit(path)
        pre = f"/{self.token}/"
        if not p.path.startswith(pre):
            return None, None
        rel = urllib.parse.unquote(p.path[len(pre):])
        return rel, urllib.parse.parse_qs(p.query)

    def handler(self):
        S = self

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

            def do_GET(self):
                if self.path == "/":
                    return self.reply(200, b"ok")
                rel, _ = S.rel(self.path)
                if S.src is None or rel not in S.files:
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
                if n == size:
                    S.sent[rel] = h.hexdigest()

            def do_PUT(self):
                rel, q = S.rel(self.path)
                if S.dst is None or rel not in S.files:
                    return self.reply(404)
                size = S.files[rel][0]
                n = int(self.headers.get("Content-Length", -1))
                off = int((q.get("off") or ["-1"])[0])
                if n < 0 or off < 0 or off + n > size or n > CHUNK:
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
                self.reply(200 if got == n else 400)

            def do_POST(self):
                rel, q = S.rel(self.path)
                if S.dst is None or rel not in S.files:
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
                S.done[rel] = ok
                self.reply(200 if ok else 409)

        return H


def relay_of(host):
    """这个 ssh 名字是执行机（送去的脚本换名、不带注释，工作目录在根目录下）→ 它的条目；不是 → None。"""
    return next((m for m in machines().RELAY.values() if m.get("ssh") == host), None)


def quick_tunnel(bind):
    image, pat = machines().MACHINES[machines().hub()]["quick"]
    cid = subprocess.run(["docker", "run", "-d", "--rm", "--network", "host", image, "tunnel", "--no-autoupdate", "--url", f"http://{bind}"],
                         capture_output=True, text=True, check=True).stdout.strip()
    for _ in range(60):
        r = subprocess.run(["docker", "logs", cid], capture_output=True, text=True)
        m = re.search(pat, r.stdout + r.stderr)
        if m:
            return cid, m.group(0)
        time.sleep(1)
    subprocess.run(["docker", "stop", cid], capture_output=True)
    raise RuntimeError("临时隧道没给出地址")


def stop_server(srv):
    srv.shutdown()
    srv.server_close()
    if getattr(srv, "quick", None):
        subprocess.run(["docker", "stop", srv.quick], capture_output=True)


def start_server(serve, quick=False):
    """quick：用临时地址（envs/machines.py 里 hub 的 quick、RELAY 的 quick_tunnel），不用 box 的固定地址。"""
    M = machines()
    url, bind = M.MACHINES[M.hub()]["transfer"]
    quick = quick and "TUNNEL_URL" not in os.environ
    url, bind = os.environ.get("TUNNEL_URL", url), os.environ.get("TUNNEL_BIND", bind)
    host, port = bind.rsplit(":", 1)
    srv = http.server.ThreadingHTTPServer((host, int(port)), serve.handler())
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    for attempt in range(3 if quick else 1):
        if quick:
            if attempt:
                subprocess.run(["docker", "stop", srv.quick], capture_output=True)
            try:
                srv.quick, url = quick_tunnel(bind)
            except Exception:
                stop_server(srv)
                raise
        for _ in range(60 if quick else 30):
            try:
                if urllib.request.urlopen(urllib.request.Request(url + "/", headers=UA), timeout=10).read() == b"ok":
                    return srv, url
            except Exception:
                pass
            time.sleep(2)
    stop_server(srv)
    raise RuntimeError(f"{url} 连不到文件服务（traefik 的路由或隧道）")


def take_lock():
    lock = os.path.expanduser(LOCK) if LOCK is not None else state("tunnel_push.lock")
    os.makedirs(os.path.dirname(lock), exist_ok=True)
    f = open(lock, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("另一个隧道传输在跑，等它结束")
        fcntl.flock(f, fcntl.LOCK_EX)
    return f


def status_cmd(wd):
    """那边的传输：done / run / dead，再加最近一行"在 …"。进程没了时再看一次 done（刚写完 done 就退出的不算 dead）。"""
    return (f"cd {wd} && if [ -e done ]; then echo done; elif pgrep -f '[b]ash {wd}/run.sh' > /dev/null; then echo run; "
            f"elif [ -e done ]; then echo done; else echo dead; fi; grep '^在 ' log 2> /dev/null | tail -1")


def run_remote(host, mode, plan, serve, total):
    """在那边建工作目录、放脚本和清单、后台跑，等它写 done；返回那边的 results（相对路径 → md5）。"""
    M = machines()
    relay = relay_of(host)
    name = next((n for n, m in M.MACHINES.items() if m.get("ssh") == host), host)
    pyx = "python3"
    with open(__file__) as f:
        src = f.read()
    if relay:  # 执行机（scripts/taskq/queue/relay.py）：工作目录在它的根目录下；在哪里跑同其他 Slurm 机器
        sys.path.insert(0, os.path.join(CORE, "queue"))
        import relay_bundle
        base = relay["root"].rstrip("/")
        job = relay.get("transfer_job") if mode == "_fetch" and total >= MIN_JOB else None
        pyx, src = relay["conda"][0] + "/bin/python3", relay_bundle.home_paths(src, base)
        script = "xfer.py"
        base += "/tmp"
    else:
        job = M.MACHINES.get(name, {}).get("transfer_job") if mode == "_fetch" and total >= MIN_JOB else None
        script = "tunnel.py"
        base = state("tunnel", expand=False)
    wd = ssh(host, f"d={base}/{secrets.token_hex(6)} && mkdir -p {base} && mkdir -m 700 $d && cd $d && pwd").strip()
    ssh(host, f"cat > {wd}/{script}", input=src)
    ssh(host, f"cat > {wd}/plan.json", input=json.dumps(plan))
    run = [f"cd {wd}"]
    if job:
        t = max(30, int(total / 5e6 / 60) + 10)
        sys.path.insert(0, os.path.join(CORE, "sites"))
        import slurm  # 作业名每次随机
        run.append(f"srun -I{JOB_WAIT} {job} -t {t} -J {slurm.jobname((relay or M.MACHINES.get(name, {})).get('job_names'))} {pyx} {script} {mode} plan.json >> log 2>&1")
    run.append(f"grep -q '^完成' log 2> /dev/null || {pyx} {script} {mode} plan.json >> log 2>&1")
    run.append("touch done")
    ssh(host, f"cat > {wd}/run.sh", input="\n".join(run) + "\n")
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
            if len(st) > 1 and st[1] and st[1] != where:
                where = st[1]
                log(f"那边{where}")
            if st[0] == "done":
                break
            if st[0] == "dead":
                raise RuntimeError("那边的传输进程没了、没有写 done：" + ssh(host, f"tail -5 {wd}/log", check=False))
            if time.time() - seen > STALL:
                raise RuntimeError(f"连续 {STALL // 60} 分钟连不上 {host}")
            if time.time() - serve.last > STALL:
                raise RuntimeError(f"连续 {STALL // 60} 分钟没有数据经过隧道")
            dt = time.time() - t0
            if wait < POLL:
                continue
            log(f"已传 {serve.moved / 1e9:.1f} / {total / 1e9:.1f} GB，平均 {serve.moved / 1e6 / dt:.1f} MB/s")
        out = ssh(host, f"cat {wd}/results.tsv 2> /dev/null || true; echo ===; grep '^失败' {wd}/log || true")
        res, fails = out.split("===\n", 1)
        for ln in fails.splitlines():
            log(ln)
        return dict(ln.split("\t", 1) for ln in res.splitlines() if "\t" in ln)
    finally:
        ssh(host, f"rm -rf {wd}", check=False)


def push(host, src, dst, rels):
    """box 的 src/<rel> → 那边的 dst/<rel>（dst 相对那边的 home 或绝对路径）。没传到、对不上的再补传几轮（ROUNDS），
    返回最后还对不上、已删掉的文件。"""
    files = {}
    for r in rels:
        st = os.stat(os.path.join(src, r))
        files[r] = (st.st_size, st.st_mtime_ns, st.st_mode & 0o7777)
    total = sum(v[0] for v in files.values())
    token = secrets.token_urlsafe(24)
    serve = Serve(token, files, src=src)
    lk = take_lock()
    srv, url = start_server(serve, quick=bool((relay_of(host) or {}).get("quick_tunnel")))
    log(f"经隧道传 {len(files)} 个文件、{total / 1e9:.2f} GB 到 {host}:{dst}")
    t0, bad = time.time(), list(files)
    try:
        for k in range(ROUNDS + 1):
            if k:
                log(f"补传 {len(bad)} 个缺的文件（第 {k} 轮）")
            for r in bad:
                serve.sent.pop(r, None)
            plan = dict(base=f"{url}/{token}", root=dst, npar=NPAR, round=k, files=[[r, *files[r]] for r in bad])
            try:
                res, stop = run_remote(host, "_fetch", plan, serve, sum(files[r][0] for r in bad)), False
            except RuntimeError as e:
                if not k:
                    raise
                log(f"补传出错（{e}），不再补")
                res, stop = {}, True
            bad = [r for r in bad if res.get(r) != (serve.sent.get(r) or md5_of(os.path.join(src, r)))]
            if bad:
                ssh(host, "cd " + rq(dst) + " && rm -f -- " + " ".join(shlex.quote(r) for r in bad), check=False)
            if not bad or stop:
                break
    finally:
        stop_server(srv)
        lk.close()
    dt = time.time() - t0
    log(f"传完：{len(files) - len(bad)} 个一致，{len(bad)} 个没传到或对不上（已删）；用时 {dt / 60:.0f} 分钟，平均 {total / 1e6 / dt:.1f} MB/s")
    return bad


def pull(host, src, dst, items):
    """那边的 src/<rel> → box 的 dst/<rel>。items：[(rel, 字节数)]。没拉到的再补拉几轮（ROUNDS），返回最后还没拉到的文件。"""
    files = {r: (int(n),) for r, n in items}
    total = sum(v[0] for v in files.values())
    token = secrets.token_urlsafe(24)
    os.makedirs(dst, exist_ok=True)
    serve = Serve(token, files, dst=dst)
    lk = take_lock()
    srv, url = start_server(serve, quick=bool((relay_of(host) or {}).get("quick_tunnel")))
    log(f"经隧道从 {host}:{src} 拉 {len(files)} 个文件、{total / 1e9:.2f} GB")
    t0, bad = time.time(), list(files)
    try:
        for k in range(ROUNDS + 1):
            if k:
                log(f"补传 {len(bad)} 个缺的文件（第 {k} 轮）")
            for r in bad:
                serve.done.pop(r, None)
            plan = dict(base=f"{url}/{token}", root=src, chunk=CHUNK, npar=NPAR, round=k, files=[[r, files[r][0]] for r in bad])
            try:
                run_remote(host, "_send", plan, serve, sum(files[r][0] for r in bad))
                stop = False
            except RuntimeError as e:
                if not k:
                    raise
                log(f"补传出错（{e}），不再补")
                stop = True
            bad = [r for r in bad if not serve.done.get(r)]
            if not bad or stop:
                break
    finally:
        stop_server(srv)
        lk.close()
        for r in files:
            p = os.path.join(dst, r) + ".tunnelpart"
            if os.path.exists(p):
                os.unlink(p)
    dt = time.time() - t0
    log(f"传完：{len(files) - len(bad)} 个一致，{len(bad)} 个没传到或对不上；用时 {dt / 60:.0f} 分钟，平均 {total / 1e6 / dt:.1f} MB/s")
    return bad


# ---------------- 那边跑的 ----------------

def retry(f, what):
    for i in range(5):
        try:
            return f()
        except Exception as e:
            err = e
            time.sleep(10 * (i + 1))
    print(f"失败 {what}：{getattr(err, 'stderr', '') or err}", flush=True)
    return None


def remote_fetch(plan):
    root = os.path.expanduser(plan["root"]) if plan["root"].startswith("~") else os.path.join(os.path.expanduser("~"), plan["root"])
    out = open("results.tsv", "a")
    lock = threading.Lock()
    bad = os.environ.get("TUNNEL_TEST_BADMD5")
    drop = (os.environ.get("TUNNEL_TEST_DROP") or "").split(",") if not plan.get("round") else []

    def one(item):
        rel, size, mtime, mode = item
        p = os.path.join(root, rel)
        if rel in drop:
            print(f"失败 {rel}：测试", flush=True)
            return
        if os.path.isfile(p) and os.stat(p).st_size == size and os.stat(p).st_mtime_ns == mtime:
            digest = md5_of(p)
        else:
            def get():
                os.makedirs(os.path.dirname(p), exist_ok=True)
                cfg = {"url": plan["base"] + "/" + urllib.parse.quote(rel), "user-agent": UA["User-Agent"], "output": p + ".tunnelpart"}
                cfg = "".join(f'{k} = "{v.replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"\n' for k, v in cfg.items())
                subprocess.run(["curl", "-sSf", "--retry", "5", "--retry-all-errors", "--speed-limit", "1024", "--speed-time", "180", "-K", "-"],
                               input=cfg, text=True, check=True, capture_output=True)  # 地址、口令不出现在进程列表里
                n = os.path.getsize(p + ".tunnelpart")
                if n != size:
                    raise IOError(f"只收到 {n} / {size} 字节")
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
    print("完成", flush=True)


def remote_send(plan):
    root = os.path.expanduser(plan["root"]) if plan["root"].startswith("~") else os.path.join(os.path.expanduser("~"), plan["root"])
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
                            raise IOError("文件变短了")
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
            print(f"失败 {rel}：测试", flush=True)
            return
        try:
            st = os.stat(p)
            if st.st_size != size:
                raise IOError(f"大小变了（{st.st_size}，清单里 {size}）")
            h, off = hashlib.md5(), 0
            with open(p, "rb") as f:
                while True:
                    n = min(chunk, size - off)
                    code, h = req("PUT", urllib.parse.quote(rel) + f"?off={off}", f, off, n, h)
                    if code != 200:
                        raise IOError(f"第 {off // chunk + 1} 块传失败（{code}）")
                    off += n
                    if off >= size:
                        break
            q = urllib.parse.urlencode(dict(md5="0" * 32 if rel == bad else h.hexdigest(), mtime=st.st_mtime_ns, mode=st.st_mode & 0o7777))
            if req("POST", urllib.parse.quote(rel) + "?" + q)[0] != 200:
                raise IOError("box 上的校验和对不上")
        except Exception as e:
            print(f"失败 {rel}：{e}", flush=True)

    with ThreadPoolExecutor(plan.get("npar", NPAR)) as ex:
        list(ex.map(one, plan["files"]))
    print("完成", flush=True)


# ---------------- rsync 的替身 ----------------

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
    if M.whoami() != M.hub():
        plain(args)
    hosts = {m["ssh"]: m for m in [*M.SITES.values(), *M.MACHINES.values()] if m.get("ssh")}  # 同一个 ssh 名字以 MACHINES 的为准
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
    total = sum(n for _, n in items)
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
        log(f"隧道没传成（{e}），全部用 rsync 经 ssh 传")
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
    if cmd == "rsync":
        return wrap(a)
    if cmd in ("_fetch", "_send"):
        print(f"在 {os.uname().nodename} 上{'下载' if cmd == '_fetch' else '上传'}", flush=True)
        plan = json.load(open(a[0]))
        (remote_fetch if cmd == "_fetch" else remote_send)(plan)
        return 0
    if cmd in ("push", "pull") and len(a) == 4:
        M = machines()
        host = M.machine(a[0])["ssh"]
        lines = [ln for ln in open(a[3]).read().splitlines() if ln]
        if cmd == "push":
            return 1 if push(host, a[1], a[2], lines) else 0
        return 1 if pull(host, a[1], a[2], [ln.split("\t") for ln in lines]) else 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
