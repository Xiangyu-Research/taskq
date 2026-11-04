#!/usr/bin/env python3
"""scripts/taskq/sync/tunnel.py 的测试：在本机模拟"那边"（假的 ssh 在本地 HOME 下跑命令），服务开在 127.0.0.1。在 box 上跑。"""
import hashlib
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
T = tempfile.mkdtemp(prefix="test_tunnel_")
HOME = os.path.join(T, "home")
os.makedirs(HOME)
FAKE = os.path.join(T, "fake_ssh")
with open(FAKE, "w") as f:
    f.write('#!/bin/bash\nshift\ncd "$HOME" && exec bash -c "$*"\n')
os.chmod(FAKE, 0o755)
HOST = "plainhost"  # 不是执行机的 Slurm 机器（子进程里由 sitecustomize 加进机器表）
with open(os.path.join(T, "sitecustomize.py"), "w") as f:
    f.write(f"import sys\nsys.path.insert(0, {fixture.CONFIG!r})\nimport machines\n"
            f"machines.MACHINES[{HOST!r}] = dict(label='plain', ssh={HOST!r}, scheduler='slurm', transfer_job='-p x', tunnel=('push', 'pull'))\n")
s = socket.socket()
s.bind(("127.0.0.1", 0))
PORT = s.getsockname()[1]
s.close()
ENV = dict(os.environ, HOME=HOME, TUNNEL_SSH=FAKE, RSYNC_RSH=FAKE, TUNNEL_URL=f"http://127.0.0.1:{PORT}",
           TUNNEL_BIND=f"127.0.0.1:{PORT}", TUNNEL_POLL="0.1", TUNNEL_CHUNK=str(1 << 20),
           TUNNEL_LOCK=os.path.join(T, "lock"), TUNNEL_TEST_REQUIRE_UA="1", PATH=os.environ["PATH"], PYTHONPATH=T)
FAILS = []


def ok(name, cond):
    print(("ok   " if cond else "FAIL ") + name)
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

# push：box → "那边"
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
ok("push 没有留下 .tunnelpart、那边的工作目录删了", not any(p.endswith(".tunnelpart") for p in tree(dst)) and not os.listdir(os.path.join(HOME, ".cache/phd/tunnel")))

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

# pull："那边" → box，分块（1 MB 一块，2.5 MB 的文件 3 块）
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
ok("pull 校验和对不上：补传 3 轮还不对 → 退出码 1，box 上没有那个文件、也没有半份", r.returncode == 1
   and tree(pdst) == sorted(f for f in FILES if f != "a/x.bin") and "补传 1 个缺的文件（第 3 轮）" in r.stdout)

shutil.rmtree(pdst)
r = run("pull", HOST, "st/remote", pdst, items, env=dict(ENV, TUNNEL_TEST_DROP="a/x.bin,top.pth"))
ok("pull 第一轮断了两个：同一次里只补拉那两个，退出码 0、一致", r.returncode == 0 and same(rsrc, pdst, FILES)
   and "补传 2 个缺的文件（第 1 轮）" in r.stdout and "第 2 轮" not in r.stdout and not any(p.endswith(".tunnelpart") for p in tree(pdst)))

# 服务只认清单里的路径
sys.path.insert(0, HERE)
os.environ.update(TUNNEL_URL=ENV["TUNNEL_URL"], TUNNEL_BIND=ENV["TUNNEL_BIND"], TUNNEL_TEST_REQUIRE_UA="1")
import tunnel  # noqa: E402
sv = tunnel.Serve("tok", {"a/x.bin": (5,)}, dst=os.path.join(T, "srv"))
srv, url = tunnel.start_server(sv)


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


ok("清单外的路径 404（含 ..）", code("PUT", "/tok/../../etc/x?off=0", b"x") == 404 and code("PUT", "/tok/b.bin?off=0", b"x") == 404
   and code("GET", "/tok/a/x.bin") == 404)
ok("没有 User-Agent 的请求被拒（和 Cloudflare 一样）", code_noua("/") == 403)
ok("错的 token 404", code("PUT", "/bad/a/x.bin?off=0", b"x") == 404)
ok("超出文件大小的块 400", code("PUT", "/tok/a/x.bin?off=3", b"xxxx") == 400)
srv.shutdown()
srv.server_close()

# rsync 的替身
big = {"frag/check/c1.prob": 1_500_000, "frag/check/c2.prob": 700_000, "frag/final.ckpt": 400_000, "frag/plans.json": 50}
rs = os.path.join(HOME, "st/res")
make(rs, big)
loc = os.path.join(T, "box/res/frag")
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

bsrc = os.path.join(T, "box/arch/frag")
make(bsrc, {k[5:]: v for k, v in big.items()})
r = run("rsync", "-rlt", "--partial", "--ignore-existing", "--stats", bsrc + "/", "plainhost:~/st/arch/frag/")
rdst = os.path.join(HOME, "st/arch/frag")
ok("替身推送（~/ 开头的目标）：一致、数据经隧道", r.returncode == 0 and same(bsrc, rdst, [f[5:] for f in big])
   and "Literal data: 0 bytes" in r.stdout and not os.path.exists(os.path.join(HOME, "~")))
ok("替身推送：那边的暂存目录删了", not [d for d in os.listdir(os.path.dirname(rdst)) if d.startswith(".tunnel_stage_")])

r = run("rsync", "-rltR", bsrc + "/./final.ckpt", "plainhost:st/arch3/")
ok("带 -R：直接 rsync", r.returncode == 0 and "经隧道" not in r.stdout and os.path.isfile(os.path.join(HOME, "st/arch3/final.ckpt")))
r = run("rsync", "-rlt", "lab:x/", os.path.join(T, "lab/"))
ok("不走隧道的机器（lab）：直接 rsync", "经隧道" not in r.stdout)

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

shutil.rmtree(T)
print(f"\n{len(FAILS)} 项失败" if FAILS else "\n全部通过")
sys.exit(1 if FAILS else 0)
