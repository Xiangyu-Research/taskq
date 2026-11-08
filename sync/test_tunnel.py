#!/usr/bin/env python3
"""Tests for sync/tunnel.py: simulate the other side here (a fake ssh runs commands under a local HOME), with the server on 127.0.0.1. Run on the hub."""
import atexit
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
T = tempfile.mkdtemp(prefix="test_tunnel_")
SRV = os.path.join(T, "tunnel_srv")  # shared file server dir (next to TUNNEL_LOCK)


def server_pid():
    try:
        return int(open(os.path.join(SRV, "serve.pid")).read())
    except (OSError, ValueError):
        return None


def cleanup():  # stop the server this test started and remove the temp dir even if the test fails midway
    pid = server_pid()
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    shutil.rmtree(T, ignore_errors=True)


atexit.register(cleanup)
HOME = os.path.join(T, "home")
RWD = os.path.join(HOME, ".local/state/taskq/tunnel")  # work dir on the other side (plainhost)
os.makedirs(HOME)
FAKE = os.path.join(T, "fake_ssh")
with open(FAKE, "w") as f:
    f.write('#!/bin/bash\nshift\n[ -n "$TUNNEL_TEST_SSH_SLEEP" ] && sleep "$TUNNEL_TEST_SSH_SLEEP"\ncd "$HOME" && exec bash -c "$*"\n')
os.chmod(FAKE, 0o755)
HOST, HOST2 = "plainhost", "plainhost2"  # machines with no root (like a backup machine; sitecustomize adds them to the machine table in child processes)
with open(os.path.join(T, "sitecustomize.py"), "w") as f:
    f.write(f"import sys\nsys.path.insert(0, {fixture.CONFIG!r})\nimport machines\n"
            f"for h in ({HOST!r}, {HOST2!r}):\n"
            "    machines.MACHINES[h] = dict(label='plain', hostname=h, ssh=h, transfer_job='-p x', tunnel=('push', 'pull'))\n")
s = socket.socket()
s.bind(("127.0.0.1", 0))
PORT = s.getsockname()[1]
s.close()
ENV = dict(os.environ, HOME=HOME, TUNNEL_SSH=FAKE, RSYNC_RSH=FAKE, TUNNEL_URL=f"http://127.0.0.1:{PORT}",
           TUNNEL_BIND=f"127.0.0.1:{PORT}", TUNNEL_POLL="0.1", TUNNEL_CHUNK=str(1 << 20),
           TUNNEL_LOCK=os.path.join(T, "lock"), TUNNEL_TEST_REQUIRE_UA="1", TUNNEL_IDLE="3", PATH=os.environ["PATH"], PYTHONPATH=T)
FAILS = []


def ok(name, cond):
    print(("PASS:" if cond else "FAIL:") + name)
    if not cond:
        FAILS.append(name)


def run(*args, env=None):
    return subprocess.run([sys.executable, os.path.join(HERE, "tunnel.py"), *args], env=env or ENV, capture_output=True, text=True)


def make(root, files):
    for rel, n in files.items():
        p = os.path.join(root, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as f:
            f.write(os.urandom(n))
        os.utime(p, ns=(1_700_000_000_123_456_789, 1_700_000_000_123_456_789))


def same(a, b, rels):
    for r in rels:
        pa, pb = os.path.join(a, r), os.path.join(b, r)
        if not os.path.isfile(pb):
            return False
        if open(pa, "rb").read() != open(pb, "rb").read() or os.stat(pa).st_mtime_ns != os.stat(pb).st_mtime_ns:
            return False
    return True


def tree(root):
    return sorted(os.path.relpath(os.path.join(d, f), root) for d, _, fs in os.walk(root) for f in fs)


FILES = {"a/x.bin": 2_500_000, "a/b/y.bin": 10, "z.bin": 0, "top.pth": 300_000}

# push: hub → "the other side"
src = os.path.join(T, "src")
make(src, FILES)
os.chmod(os.path.join(src, "a/b/y.bin"), 0o600)
lst = os.path.join(T, "list")
open(lst, "w").write("\n".join(FILES) + "\n")
r = run("push", HOST, src, "st/pushed", lst)
dst = os.path.join(HOME, "st/pushed")
ok("push exit code 0", r.returncode == 0)
ok("push content and mtime match", same(src, dst, FILES))
ok("push keeps the mode", os.stat(os.path.join(dst, "a/b/y.bin")).st_mode & 0o777 == 0o600)
ok("push: no .tunnelpart left, work dir there deleted", not any(p.endswith(".tunnelpart") for p in tree(dst)) and not os.listdir(RWD))

r = run("push", HOST, src, "st/pushed", lst)
ok("push again: existing files skipped, still all match", r.returncode == 0 and same(src, dst, FILES) and "4 files match" in r.stdout)

shutil.rmtree(dst)
r = run("push", HOST, src, "st/pushed", lst, env=dict(ENV, TUNNEL_TEST_BADMD5="a/x.bin"))
ok("push checksum mismatch: 3 retry rounds still bad → exit 1, that file deleted there, the rest stay", r.returncode == 1 and not os.path.exists(os.path.join(dst, "a/x.bin"))
   and same(src, dst, [f for f in FILES if f != "a/x.bin"]) and "retry 1 missing (round 3)" in r.stdout and "round 4" not in r.stdout)

shutil.rmtree(dst)
r = run("push", HOST, src, "st/pushed", lst, env=dict(ENV, TUNNEL_TEST_DROP="a/x.bin,z.bin"))
ok("push first round drops two: retry only those in the same run, exit 0, all match", r.returncode == 0 and same(src, dst, FILES)
   and "retry 2 missing (round 1)" in r.stdout and "round 2" not in r.stdout and "4 files match" in r.stdout)

shutil.rmtree(dst)
r = run("push", HOST, src, "st/pushed", lst, env=dict(ENV, TUNNEL_MIN_JOB="1"))
ok("push wants a compute-node download and srun is unavailable: fall back to the login node, all match", r.returncode == 0 and same(src, dst, FILES))

# pull: "the other side" → hub, in chunks (1 MB each, so the 2.5 MB file takes 3)
rsrc = os.path.join(HOME, "st/remote")
make(rsrc, FILES)
items = os.path.join(T, "items")
open(items, "w").write("".join(f"{r_}\t{n}\n" for r_, n in FILES.items()))
pdst = os.path.join(T, "pulled")
r = run("pull", HOST, "st/remote", pdst, items)
ok("pull exit code 0", r.returncode == 0)
ok("pull content and mtime match (including chunked and empty files)", same(rsrc, pdst, FILES))
ok("pull leaves no .tunnelpart", not any(p.endswith(".tunnelpart") for p in tree(pdst)))

shutil.rmtree(pdst)
r = run("pull", HOST, "st/remote", pdst, items, env=dict(ENV, TUNNEL_TEST_BADMD5="a/x.bin"))
ok("pull checksum mismatch: 3 retry rounds still bad → exit 1, no such file on the hub and no partial", r.returncode == 1
   and tree(pdst) == sorted(f for f in FILES if f != "a/x.bin") and "retry 1 missing (round 3)" in r.stdout)

shutil.rmtree(pdst)
r = run("pull", HOST, "st/remote", pdst, items, env=dict(ENV, TUNNEL_TEST_DROP="a/x.bin,top.pth"))
ok("pull first round drops two: retry only those in the same run, exit 0, all match", r.returncode == 0 and same(rsrc, pdst, FILES)
   and "retry 2 missing (round 1)" in r.stdout and "round 2" not in r.stdout and not any(p.endswith(".tunnelpart") for p in tree(pdst)))

# the shared server only accepts the session token and paths in the list
sys.path.insert(0, HERE)
os.environ.update({k: ENV[k] for k in ("TUNNEL_URL", "TUNNEL_BIND", "TUNNEL_LOCK", "TUNNEL_TEST_REQUIRE_UA", "TUNNEL_IDLE")})
import tunnel  # noqa: E402
sv = tunnel.Session({"a/x.bin": (5,)}, dst=os.path.join(T, "srv"))
url, tok = sv.open(), sv.token


def code_noua(path):
    try:
        return urllib.request.urlopen(url + path).status
    except urllib.error.HTTPError as e:
        return e.code


def code(method, path, data=b""):
    try:
        return urllib.request.urlopen(urllib.request.Request(url + path, data=data, method=method, headers=tunnel.UA)).status
    except urllib.error.HTTPError as e:
        return e.code


ok("path not in the list 404 (including ..)", code("PUT", f"/{tok}/../../etc/x?off=0", b"x") == 404 and code("PUT", f"/{tok}/b.bin?off=0", b"x") == 404
   and code("GET", f"/{tok}/a/x.bin") == 404)
ok("requests without User-Agent rejected (like Cloudflare)", code_noua("/") == 403)
ok("bad token 404 (including other file names in the server dir)", code("PUT", "/bad/a/x.bin?off=0", b"x") == 404 and code("GET", "/" + "A" * 32 + "/") == 404
   and code("GET", "/serve.pid/") == 404 and code("GET", "/../serve/") == 404 and code("PUT", f"/{tok[:-1]}/a/x.bin?off=0", b"x") == 404)
ok("good token: session recognized; chunk past the file size 400", code("GET", f"/{tok}/") == 200 and code("PUT", f"/{tok}/a/x.bin?off=3", b"xxxx") == 400)
ok("good token and chunk: accepted", code("PUT", f"/{tok}/a/x.bin?off=0", b"xxxxx") == 200)
sv.close()
ok("after the session closes its token is 404", code("GET", f"/{tok}/") == 404 and code("PUT", f"/{tok}/a/x.bin?off=0", b"xxxxx") == 404)
ok("session files deleted", not [f for f in os.listdir(SRV) if f.endswith((".json", ".out"))])

# rsync stand-in
big = {"frag/check/c1.prob": 1_500_000, "frag/check/c2.prob": 700_000, "frag/final.ckpt": 400_000, "frag/plans.json": 50}
rs = os.path.join(HOME, "st/res")
make(rs, big)
loc = os.path.join(T, "hub/res/frag")
r = run("rsync", "-rlt", "--partial", "--stats", "--include=/*/", "--exclude=/*", "plainhost:st/res/frag/", loc + "/")
inner = [f for f in big if f.count("/") > 1]
ok("stand-in pull, first pass (top-level files excluded): subdirs complete, marker file final.ckpt not there", r.returncode == 0
   and same(os.path.join(rs, "frag"), loc, [f[5:] for f in inner]) and not os.path.exists(os.path.join(loc, "final.ckpt")))
ok("stand-in pull: data over the tunnel (rsync sent 0 bytes over ssh)", "tunnel transfer" in r.stdout and "Literal data: 0 bytes" in r.stdout)
ok("stand-in pull: staging dir deleted", not [d for d in os.listdir(os.path.dirname(loc)) if d.startswith(".tunnel_stage_")])
r = run("rsync", "-rlt", "--partial", "plainhost:st/res/frag/", loc + "/")
ok("stand-in pull, second pass: everything there", r.returncode == 0 and same(os.path.join(rs, "frag"), loc, [f[5:] for f in big]))

shutil.rmtree(loc)
r = run("rsync", "-rlt", "--partial", "plainhost:st/res/frag/", loc + "/", env=dict(ENV, TUNNEL_TEST_BADMD5="check/c1.prob"))
ok("stand-in pull: bad files from the tunnel are filled in by rsync over ssh", r.returncode == 0 and same(os.path.join(rs, "frag"), loc, [f[5:] for f in big]))

bsrc = os.path.join(T, "hub/arch/frag")
make(bsrc, {k[5:]: v for k, v in big.items()})
r = run("rsync", "-rlt", "--partial", "--ignore-existing", "--stats", bsrc + "/", "plainhost:~/st/arch/frag/")
rdst = os.path.join(HOME, "st/arch/frag")
ok("stand-in push (target starting with ~/): all match, data over the tunnel", r.returncode == 0 and same(bsrc, rdst, [f[5:] for f in big])
   and "Literal data: 0 bytes" in r.stdout and not os.path.exists(os.path.join(HOME, "~")))
ok("stand-in push: staging dir there deleted", not [d for d in os.listdir(os.path.dirname(rdst)) if d.startswith(".tunnel_stage_")])

r = run("rsync", "-rltR", bsrc + "/./final.ckpt", "plainhost:st/arch3/")
ok("with -R: plain rsync", r.returncode == 0 and "tunnel transfer" not in r.stdout and os.path.isfile(os.path.join(HOME, "st/arch3/final.ckpt")))
r = run("rsync", "-rlt", "fake-pueue-host:x/", os.path.join(T, "pueue-host/"))
ok("machine without a tunnel (pueue-host): plain rsync", "tunnel transfer" not in r.stdout)

wd, fbin = os.path.join(T, "wd"), os.path.join(T, "fbin")
os.makedirs(wd)
os.makedirs(fbin)
with open(os.path.join(fbin, "pgrep"), "w") as f:
    f.write(f'#!/bin/bash\n[ -n "$FINISH" ] && touch {wd}/done\nexit 1\n')
os.chmod(os.path.join(fbin, "pgrep"), 0o755)
cmd = subprocess.run([sys.executable, "-c", f"import tunnel; print(tunnel.status_cmd({wd!r}))"], cwd=HERE, env=ENV,
                     capture_output=True, text=True).stdout.strip()
st = lambda **e: subprocess.run(["bash", "-c", cmd], env=dict(ENV, PATH=fbin + ":" + ENV["PATH"], **e),  # noqa: E731
                                capture_output=True, text=True).stdout.split("\n")[0]
ok("status: process gone, no done → dead", st() == "dead")
ok("status: done written just before exiting, after the done check but before the process check → done, not dead", st(FINISH="1") == "done")

sys.path.insert(0, HERE)
import tunnel  # noqa: E402
ok("machine lookup by name (site-slurm and site-slurm2 share a hostname): with site, that machine; without, find by ssh name; not in the machine table → {}",
   tunnel.entry("fake-slurm-login", site="site-slurm2") == tunnel.machines().machine("site-slurm2")
   and tunnel.entry("fake-pueue-host") == tunnel.machines().machine("site-pueue")
   and tunnel.entry("no-such-host") == {})

# transfer log protocol: the remote side prints "at <host>: download|upload", "complete" and "failed <path>: ..."; the hub reads them back
wdp = os.path.join(T, "proto_wd")
os.makedirs(wdp)
with open(os.path.join(wdp, "log"), "w") as f:
    f.write("at node1: download\ncomplete\nfailed a/x: test\n")
st = subprocess.run(["bash", "-c", tunnel.status_cmd(wdp)], env=ENV, capture_output=True, text=True).stdout.splitlines()
ok("transfer status reads back the remote 'at' line", st[-1] == "at node1: download")
planp = os.path.join(T, "proto_plan.json")
with open(planp, "w") as f:
    json.dump(dict(root=T, base="http://127.0.0.1:1", files=[["a/x", 1, 1, 420]], npar=1, chunk=1 << 20), f)
r = subprocess.run([sys.executable, os.path.join(HERE, "tunnel.py"), "_fetch", planp],
                   env=dict(ENV, TUNNEL_TEST_DROP="a/x"), cwd=T, capture_output=True, text=True)
ok("_fetch prints at/complete/failed in English", r.returncode == 0 and r.stdout.startswith(f"at {os.uname().nodename}: download\n")
   and "failed a/x: test" in r.stdout and r.stdout.rstrip().endswith("complete"))
with open(planp, "w") as f:
    json.dump(dict(root=T, base="http://127.0.0.1:1", files=[], npar=1, chunk=1 << 20), f)
r = subprocess.run([sys.executable, os.path.join(HERE, "tunnel.py"), "_send", planp], env=ENV, cwd=T, capture_output=True, text=True)
ok("_send prints at/complete in English", r.returncode == 0 and r.stdout.startswith(f"at {os.uname().nodename}: upload\n")
   and r.stdout.rstrip().endswith("complete"))

# different machines transfer at once, the same machine queues, a dead server is fine (the fake ssh takes 0.5 s per command, so a transfer takes seconds)
SLOW = dict(ENV, TUNNEL_TEST_SSH_SLEEP="0.5")


def start(*args, env=SLOW):
    return subprocess.Popen([sys.executable, os.path.join(HERE, "tunnel.py"), *args], env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)


def span(out):
    """Time from "tunnel transfer of" to "done:" in the log (seconds)."""
    ts = [time.mktime(time.strptime(ln[:19], "%Y-%m-%d %H:%M:%S")) for ln in out.splitlines() if "tunnel transfer of" in ln or "done:" in ln]
    return ts[0], ts[-1]


def finish(p):
    out = p.communicate(timeout=300)[0]
    return p.returncode, out


cdst = {h: os.path.join(HOME, f"st/conc_{h}") for h in (HOST, HOST2)}
ps = [start("push", h, src, f"st/conc_{h}", lst) for h in (HOST, HOST2)]
(r1, o1), (r2, o2) = finish(ps[0]), finish(ps[1])
(s1, e1), (s2, e2) = span(o1), span(o2)
ok("two different machines transfer at once: both finish, all match", r1 == 0 and r2 == 0 and all(same(src, d, FILES) for d in cdst.values()))
b0 = min(s1, s2)
ok(f"two different machines transfer at once: times overlap ({s1 - b0:.0f}-{e1 - b0:.0f} s, {s2 - b0:.0f}-{e2 - b0:.0f} s), no queueing",
   s1 < e2 and s2 < e1 and "waiting for it" not in o1 + o2)

for d in cdst.values():
    shutil.rmtree(d)
ps = [start("push", HOST, src, f"st/conc_{h}", lst) for h in (HOST, HOST2)]
(r1, o1), (r2, o2) = finish(ps[0]), finish(ps[1])
(s1, e1), (s2, e2) = span(o1), span(o2)
ok("two transfers to the same machine: both finish, all match", r1 == 0 and r2 == 0 and all(same(src, d, FILES) for d in cdst.values()))
ok("two transfers to the same machine: queue, one starts after the other ends", (s2 >= e1 or s1 >= e2) and "another tunnel transfer to plainhost is running, waiting for it" in o1 + o2)

shutil.rmtree(dst)
stale = os.path.join(SRV, "Z" * 32 + ".json")
with open(stale, "w") as f:
    tunnel.json.dump(dict(files={}, pid=fixture.dead_pid()), f)
p = start("push", HOST, src, "st/pushed", lst)
for _ in range(600):  # wait for the work dir there (server is up, session recognized)
    if server_pid() and os.listdir(RWD):
        break
    time.sleep(0.05)
killed = server_pid()
os.kill(killed, signal.SIGKILL)
r, o = finish(p)
ok("shared server killed mid-transfer: a new one starts, the transfer still finishes, all match", r == 0 and same(src, dst, FILES) and server_pid() not in (None, killed))
ok("sessions whose transfer process is gone are cleaned up by the server", not os.path.exists(stale))

os.kill(server_pid(), signal.SIGKILL)
shutil.rmtree(pdst)
r = run("pull", HOST, "st/remote", pdst, items)
ok("next transfer after the server is killed works (pull)", r.returncode == 0 and same(rsrc, pdst, FILES))

pid = server_pid()
for _ in range(100):
    if not tunnel.alive(pid):
        break
    time.sleep(0.1)
ok("server exits after a few idle seconds (TUNNEL_IDLE) with no sessions", not tunnel.alive(pid) and not [f for f in os.listdir(SRV) if f.endswith((".json", ".out"))])

# local executor (ssh is None): no tunnel, no lock, copy directly here
lsrc, ldst = os.path.join(T, "lsrc"), os.path.join(T, "lhome_dst")
make(lsrc, FILES)
os.chmod(os.path.join(lsrc, "a/b/y.bin"), 0o600)
bad = tunnel.push(None, lsrc, ldst, list(FILES))
ok("local push: all match, mode kept, hard-linked on the same disk, no .tunnelpart", not bad and same(lsrc, ldst, FILES)
   and os.stat(os.path.join(ldst, "a/b/y.bin")).st_mode & 0o777 == 0o600
   and os.stat(os.path.join(ldst, "a/x.bin")).st_ino == os.stat(os.path.join(lsrc, "a/x.bin")).st_ino
   and not any(p.endswith(".tunnelpart") for p in tree(ldst)))
lpull = os.path.join(T, "lpull")
bad = tunnel.pull(None, lsrc, lpull, [(r, n) for r, n in FILES.items()])
ok("local pull: all match (copied, not hard-linked)", not bad and same(lsrc, lpull, FILES)
   and os.stat(os.path.join(lpull, "a/x.bin")).st_ino != os.stat(os.path.join(lsrc, "a/x.bin")).st_ino)

real_copy = tunnel.shutil.copy2


def half_copy(a, b):  # interrupted halfway: half written, then an error
    with open(a, "rb") as f, open(b, "wb") as g:
        g.write(f.read(os.path.getsize(a) // 2))
    raise OSError("test: copied halfway")


shutil.rmtree(lpull)
os.makedirs(os.path.join(lpull, "a"))
open(os.path.join(lpull, "a/x.bin.tunnelpart"), "wb").write(b"old")  # partial left by a previous kill
tunnel.shutil.copy2 = half_copy
bad = tunnel.pull(None, lsrc, lpull, [("a/x.bin", FILES["a/x.bin"]), ("top.pth", FILES["top.pth"])])
tunnel.shutil.copy2 = real_copy
ok("local copy interrupted halfway: reported not copied (caller does not count it done), no partial at the target, .tunnelpart removed", sorted(bad) == ["a/x.bin", "top.pth"]
   and not os.path.exists(os.path.join(lpull, "a/x.bin")) and not os.path.exists(os.path.join(lpull, "top.pth"))
   and not any(p.endswith(".tunnelpart") for p in tree(lpull)))
bad = tunnel.pull(None, lsrc, lpull, [("a/x.bin", FILES["a/x.bin"]), ("top.pth", FILES["top.pth"])])
ok("local copy after a half-copy: completed", not bad and same(lsrc, lpull, ["a/x.bin", "top.pth"]))

print(f"\n{len(FAILS)} failures" if FAILS else "\nall passed")
sys.exit(1 if FAILS else 0)
