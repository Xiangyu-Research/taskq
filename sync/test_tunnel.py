#!/usr/bin/env python3
"""sync/tunnel.py 的测试：在本机模拟"那边"（假的 ssh 在本地 HOME 下跑命令），服务开在 127.0.0.1。在 hub 上跑。"""
import atexit
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
SRV = os.path.join(T, "tunnel_srv")  # 共享文件服务的文件夹（TUNNEL_LOCK 旁边）


def server_pid():
    try:
        return int(open(os.path.join(SRV, "serve.pid")).read())
    except (OSError, ValueError):
        return None


def cleanup():  # 测试中途出错也停掉本次起的服务、删临时目录
    pid = server_pid()
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    shutil.rmtree(T, ignore_errors=True)


atexit.register(cleanup)
HOME = os.path.join(T, "home")
RWD = os.path.join(HOME, ".local/state/taskq/tunnel")  # 那边（plainhost）的工作目录
os.makedirs(HOME)
FAKE = os.path.join(T, "fake_ssh")
with open(FAKE, "w") as f:
    f.write('#!/bin/bash\nshift\n[ -n "$TUNNEL_TEST_SSH_SLEEP" ] && sleep "$TUNNEL_TEST_SSH_SLEEP"\ncd "$HOME" && exec bash -c "$*"\n')
os.chmod(FAKE, 0o755)
HOST, HOST2 = "plainhost", "plainhost2"  # 没有根目录的机器（备份机那样；子进程里由 sitecustomize 加进机器表）
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
    print(("PASS:" if cond else "没PASS:") + name)
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

# push：hub → "那边"
src = os.path.join(T, "src")
make(src, FILES)
os.chmod(os.path.join(src, "a/b/y.bin"), 0o600)
lst = os.path.join(T, "list")
open(lst, "w").write("\n".join(FILES) + "\n")
r = run("push", HOST, src, "st/pushed", lst)
dst = os.path.join(HOME, "st/pushed")
ok("push 退出码 0", r.returncode == 0)
ok("push 内容和修改时间一致", same(src, dst, FILES))
ok("push 保留权限", os.stat(os.path.join(dst, "a/b/y.bin")).st_mode & 0o777 == 0o600)
ok("push 没有留下 .tunnelpart、那边的工作目录删了", not any(p.endswith(".tunnelpart") for p in tree(dst)) and not os.listdir(RWD))

r = run("push", HOST, src, "st/pushed", lst)
ok("push 再跑一次：已有的跳过、仍然一致", r.returncode == 0 and same(src, dst, FILES) and "4 个一致" in r.stdout)

shutil.rmtree(dst)
r = run("push", HOST, src, "st/pushed", lst, env=dict(ENV, TUNNEL_TEST_BADMD5="a/x.bin"))
ok("push 校验和对不上：补传 3 轮还不对 → 退出码 1，那边删掉那个文件，别的在", r.returncode == 1 and not os.path.exists(os.path.join(dst, "a/x.bin"))
   and same(src, dst, [f for f in FILES if f != "a/x.bin"]) and "补传 1 个缺的文件（第 3 轮）" in r.stdout and "第 4 轮" not in r.stdout)

shutil.rmtree(dst)
r = run("push", HOST, src, "st/pushed", lst, env=dict(ENV, TUNNEL_TEST_DROP="a/x.bin,z.bin"))
ok("push 第一轮断了两个：同一次里只补传那两个，退出码 0、一致", r.returncode == 0 and same(src, dst, FILES)
   and "补传 2 个缺的文件（第 1 轮）" in r.stdout and "第 2 轮" not in r.stdout and "4 个一致" in r.stdout)

shutil.rmtree(dst)
r = run("push", HOST, src, "st/pushed", lst, env=dict(ENV, TUNNEL_MIN_JOB="1"))
ok("push 要在计算节点下载、srun 不能用：退回登录节点，一致", r.returncode == 0 and same(src, dst, FILES))

# pull："那边" → hub，分块（1 MB 一块，2.5 MB 的文件 3 块）
rsrc = os.path.join(HOME, "st/remote")
make(rsrc, FILES)
items = os.path.join(T, "items")
open(items, "w").write("".join(f"{r_}\t{n}\n" for r_, n in FILES.items()))
pdst = os.path.join(T, "pulled")
r = run("pull", HOST, "st/remote", pdst, items)
ok("pull 退出码 0", r.returncode == 0)
ok("pull 内容和修改时间一致（含分块的、空文件）", same(rsrc, pdst, FILES))
ok("pull 没有留下 .tunnelpart", not any(p.endswith(".tunnelpart") for p in tree(pdst)))

shutil.rmtree(pdst)
r = run("pull", HOST, "st/remote", pdst, items, env=dict(ENV, TUNNEL_TEST_BADMD5="a/x.bin"))
ok("pull 校验和对不上：补传 3 轮还不对 → 退出码 1，hub 上没有那个文件、也没有半份", r.returncode == 1
   and tree(pdst) == sorted(f for f in FILES if f != "a/x.bin") and "补传 1 个缺的文件（第 3 轮）" in r.stdout)

shutil.rmtree(pdst)
r = run("pull", HOST, "st/remote", pdst, items, env=dict(ENV, TUNNEL_TEST_DROP="a/x.bin,top.pth"))
ok("pull 第一轮断了两个：同一次里只补拉那两个，退出码 0、一致", r.returncode == 0 and same(rsrc, pdst, FILES)
   and "补传 2 个缺的文件（第 1 轮）" in r.stdout and "第 2 轮" not in r.stdout and not any(p.endswith(".tunnelpart") for p in tree(pdst)))

# 共享服务只认会话的 token 和清单里的路径
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


ok("清单外的路径 404（含 ..）", code("PUT", f"/{tok}/../../etc/x?off=0", b"x") == 404 and code("PUT", f"/{tok}/b.bin?off=0", b"x") == 404
   and code("GET", f"/{tok}/a/x.bin") == 404)
ok("没有 User-Agent 的请求被拒（和 Cloudflare 一样）", code_noua("/") == 403)
ok("错的 token 404（含服务文件夹里的别的文件名）", code("PUT", "/bad/a/x.bin?off=0", b"x") == 404 and code("GET", "/" + "A" * 32 + "/") == 404
   and code("GET", "/serve.pid/") == 404 and code("GET", "/../serve/") == 404 and code("PUT", f"/{tok[:-1]}/a/x.bin?off=0", b"x") == 404)
ok("对的 token：认得会话；超出文件大小的块 400", code("GET", f"/{tok}/") == 200 and code("PUT", f"/{tok}/a/x.bin?off=3", b"xxxx") == 400)
ok("对的 token、对的块：收下", code("PUT", f"/{tok}/a/x.bin?off=0", b"xxxxx") == 200)
sv.close()
ok("会话关掉以后它的 token 404", code("GET", f"/{tok}/") == 404 and code("PUT", f"/{tok}/a/x.bin?off=0", b"xxxxx") == 404)
ok("会话的文件删了", not [f for f in os.listdir(SRV) if f.endswith((".json", ".out"))])

# rsync 的替身
big = {"frag/check/c1.prob": 1_500_000, "frag/check/c2.prob": 700_000, "frag/final.ckpt": 400_000, "frag/plans.json": 50}
rs = os.path.join(HOME, "st/res")
make(rs, big)
loc = os.path.join(T, "hub/res/frag")
r = run("rsync", "-rlt", "--partial", "--stats", "--include=/*/", "--exclude=/*", "plainhost:st/res/frag/", loc + "/")
inner = [f for f in big if f.count("/") > 1]
ok("替身拉回第一遍（不带最上层的文件）：子文件夹齐了、标志文件 final.ckpt 没到", r.returncode == 0
   and same(os.path.join(rs, "frag"), loc, [f[5:] for f in inner]) and not os.path.exists(os.path.join(loc, "final.ckpt")))
ok("替身拉回：数据经隧道（rsync 经 ssh 传 0 字节）", "经隧道从" in r.stdout and "Literal data: 0 bytes" in r.stdout)
ok("替身拉回：暂存目录删了", not [d for d in os.listdir(os.path.dirname(loc)) if d.startswith(".tunnel_stage_")])
r = run("rsync", "-rlt", "--partial", "plainhost:st/res/frag/", loc + "/")
ok("替身拉回第二遍：全齐", r.returncode == 0 and same(os.path.join(rs, "frag"), loc, [f[5:] for f in big]))

shutil.rmtree(loc)
r = run("rsync", "-rlt", "--partial", "plainhost:st/res/frag/", loc + "/", env=dict(ENV, TUNNEL_TEST_BADMD5="check/c1.prob"))
ok("替身拉回：隧道传坏的文件由 rsync 经 ssh 补上", r.returncode == 0 and same(os.path.join(rs, "frag"), loc, [f[5:] for f in big]))

bsrc = os.path.join(T, "hub/arch/frag")
make(bsrc, {k[5:]: v for k, v in big.items()})
r = run("rsync", "-rlt", "--partial", "--ignore-existing", "--stats", bsrc + "/", "plainhost:~/st/arch/frag/")
rdst = os.path.join(HOME, "st/arch/frag")
ok("替身推送（~/ 开头的目标）：一致、数据经隧道", r.returncode == 0 and same(bsrc, rdst, [f[5:] for f in big])
   and "Literal data: 0 bytes" in r.stdout and not os.path.exists(os.path.join(HOME, "~")))
ok("替身推送：那边的暂存目录删了", not [d for d in os.listdir(os.path.dirname(rdst)) if d.startswith(".tunnel_stage_")])

r = run("rsync", "-rltR", bsrc + "/./final.ckpt", "plainhost:st/arch3/")
ok("带 -R：直接 rsync", r.returncode == 0 and "经隧道" not in r.stdout and os.path.isfile(os.path.join(HOME, "st/arch3/final.ckpt")))
r = run("rsync", "-rlt", "fake-pueue-host:x/", os.path.join(T, "pueue-host/"))
ok("不走隧道的机器（pueue-host）：直接 rsync", "经隧道" not in r.stdout)

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
ok("状态：进程没了、没有 done → dead", st() == "dead")
ok("状态：查完 done 之后、查进程之前刚写完 done 退出 → done，不当 dead", st(FINISH="1") == "done")

sys.path.insert(0, HERE)
import tunnel  # noqa: E402
ok("机器按名字查（site-slurm、site-slurm2 主机名相同）：给 site 时拿到那一台；不给时按 ssh 名字找；机器表里没有 → {}",
   tunnel.entry("fake-slurm-login", site="site-slurm2") == tunnel.machines().machine("site-slurm2")
   and tunnel.entry("fake-pueue-host") == tunnel.machines().machine("site-pueue")
   and tunnel.entry("no-such-host") == {})

# 不同机器同时传、同一台排队、服务挂了照常（假的 ssh 每条命令慢 0.5 秒，一次传输要几秒）
SLOW = dict(ENV, TUNNEL_TEST_SSH_SLEEP="0.5")


def start(*args, env=SLOW):
    return subprocess.Popen([sys.executable, os.path.join(HERE, "tunnel.py"), *args], env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)


def span(out):
    """日志里"经隧道传"到"传完"的时间（秒）。"""
    ts = [time.mktime(time.strptime(ln[:19], "%Y-%m-%d %H:%M:%S")) for ln in out.splitlines() if "经隧道" in ln or "传完" in ln]
    return ts[0], ts[-1]


def finish(p):
    out = p.communicate(timeout=300)[0]
    return p.returncode, out


cdst = {h: os.path.join(HOME, f"st/conc_{h}") for h in (HOST, HOST2)}
ps = [start("push", h, src, f"st/conc_{h}", lst) for h in (HOST, HOST2)]
(r1, o1), (r2, o2) = finish(ps[0]), finish(ps[1])
(s1, e1), (s2, e2) = span(o1), span(o2)
ok("两台不同的机器同时传：都完成、一致", r1 == 0 and r2 == 0 and all(same(src, d, FILES) for d in cdst.values()))
b0 = min(s1, s2)
ok(f"两台不同的机器同时传：时间重叠（{s1 - b0:.0f}–{e1 - b0:.0f} 秒、{s2 - b0:.0f}–{e2 - b0:.0f} 秒）、没有排队",
   s1 < e2 and s2 < e1 and "等它结束" not in o1 + o2)

for d in cdst.values():
    shutil.rmtree(d)
ps = [start("push", HOST, src, f"st/conc_{h}", lst) for h in (HOST, HOST2)]
(r1, o1), (r2, o2) = finish(ps[0]), finish(ps[1])
(s1, e1), (s2, e2) = span(o1), span(o2)
ok("同一台机器两次传输：都完成、一致", r1 == 0 and r2 == 0 and all(same(src, d, FILES) for d in cdst.values()))
ok("同一台机器两次传输：排队，一个结束了另一个才开始", (s2 >= e1 or s1 >= e2) and "plainhost 的另一个隧道传输在跑，等它结束" in o1 + o2)

shutil.rmtree(dst)
stale = os.path.join(SRV, "Z" * 32 + ".json")
with open(stale, "w") as f:
    tunnel.json.dump(dict(files={}, pid=fixture.dead_pid()), f)
p = start("push", HOST, src, "st/pushed", lst)
for _ in range(600):  # 等那边的工作目录建好（服务已经起来、会话已经认得）
    if server_pid() and os.listdir(RWD):
        break
    time.sleep(0.05)
killed = server_pid()
os.kill(killed, signal.SIGKILL)
r, o = finish(p)
ok("传到一半共享服务被杀：重新起一个，照常完成、一致", r == 0 and same(src, dst, FILES) and server_pid() not in (None, killed))
ok("传输进程不在了的会话由服务清掉", not os.path.exists(stale))

os.kill(server_pid(), signal.SIGKILL)
shutil.rmtree(pdst)
r = run("pull", HOST, "st/remote", pdst, items)
ok("服务被杀后下一次传输照常（pull）", r.returncode == 0 and same(rsrc, pdst, FILES))

pid = server_pid()
for _ in range(100):
    if not tunnel.alive(pid):
        break
    time.sleep(0.1)
ok("没有会话后服务空闲几秒（TUNNEL_IDLE）自己退出", not tunnel.alive(pid) and not [f for f in os.listdir(SRV) if f.endswith((".json", ".out"))])

# 本机执行机（ssh 是 None）：不开隧道、不拿锁，直接在本机拷
lsrc, ldst = os.path.join(T, "lsrc"), os.path.join(T, "lhome_dst")
make(lsrc, FILES)
os.chmod(os.path.join(lsrc, "a/b/y.bin"), 0o600)
bad = tunnel.push(None, lsrc, ldst, list(FILES))
ok("本机 push：一致、保留权限、同盘的硬链接过去、没有 .tunnelpart", not bad and same(lsrc, ldst, FILES)
   and os.stat(os.path.join(ldst, "a/b/y.bin")).st_mode & 0o777 == 0o600
   and os.stat(os.path.join(ldst, "a/x.bin")).st_ino == os.stat(os.path.join(lsrc, "a/x.bin")).st_ino
   and not any(p.endswith(".tunnelpart") for p in tree(ldst)))
lpull = os.path.join(T, "lpull")
bad = tunnel.pull(None, lsrc, lpull, [(r, n) for r, n in FILES.items()])
ok("本机 pull：一致（拷过来，不是硬链接）", not bad and same(lsrc, lpull, FILES)
   and os.stat(os.path.join(lpull, "a/x.bin")).st_ino != os.stat(os.path.join(lsrc, "a/x.bin")).st_ino)

real_copy = tunnel.shutil.copy2


def half_copy(a, b):  # 拷到一半被打断：写了一半、出错
    with open(a, "rb") as f, open(b, "wb") as g:
        g.write(f.read(os.path.getsize(a) // 2))
    raise OSError("测试：拷到一半")


shutil.rmtree(lpull)
os.makedirs(os.path.join(lpull, "a"))
open(os.path.join(lpull, "a/x.bin.tunnelpart"), "wb").write(b"old")  # 上次被杀留下的半个
tunnel.shutil.copy2 = half_copy
bad = tunnel.pull(None, lsrc, lpull, [("a/x.bin", FILES["a/x.bin"]), ("top.pth", FILES["top.pth"])])
tunnel.shutil.copy2 = real_copy
ok("本机拷到一半：报没拷成（调用方不算做完）、目标没有半个文件、.tunnelpart 删了", sorted(bad) == ["a/x.bin", "top.pth"]
   and not os.path.exists(os.path.join(lpull, "a/x.bin")) and not os.path.exists(os.path.join(lpull, "top.pth"))
   and not any(p.endswith(".tunnelpart") for p in tree(lpull)))
bad = tunnel.pull(None, lsrc, lpull, [("a/x.bin", FILES["a/x.bin"]), ("top.pth", FILES["top.pth"])])
ok("本机拷到一半后再拷：补齐", not bad and same(lsrc, lpull, ["a/x.bin", "top.pth"]))

print(f"\n{len(FAILS)} 项失败" if FAILS else "\n全部通过")
sys.exit(1 if FAILS else 0)
