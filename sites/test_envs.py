#!/usr/bin/env python3
"""测 sites/envs.py 和机器表里按描述建的环境（2026-10-31）：描述能读、激活语句、.ok 对不上时不送任务、
建环境的命令只在 CPU 上跑；最后在临时目录里真建一个只有 numpy 的 venv（不联网时跳过装包那一步之外的部分不跑）。

    python3 sites/test_envs.py [--real]
"""
import copy
import glob
import hashlib
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(CORE, "queue"))
sys.path.insert(0, os.path.join(CORE, "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
import proto  # noqa: E402
import envs as E  # noqa: E402
M = E.M
import refill  # noqa: E402

fails = 0


def ok(what, cond, info=None):
    global fails
    print(("通过：" if cond else "失败：") + what + ("" if cond or info is None else f"\n   {info}"))
    fails += 0 if cond else 1


# ---------- 描述 ----------
names = sorted(os.path.basename(p)[:-4] for p in glob.glob(os.path.join(M.SPECS, "*.yml")))
ok("有 base、alt、alt2、stats 的描述", {"base", "alt", "alt2", "stats"} <= set(names))
for n in names:
    try:
        sp = E.load(n)
        E.check_file(n)
        good = True
    except E.EnvError as e:
        print("  ", e)
        good = False
    ok(f"{n}.yml 能读、有检查", good)
    ok(f"{n}.check.py 能编译", bool(compile(open(E.check_file(n), encoding="utf-8").read(), n, "exec")))
ok("alt 建在 base 上", E.load("alt")["base"] == "base")
ok("base 有 cu128 的变体", "cu128" in E.load("base")["torch"])
ok("还没有锁：哈希 = 描述内容的 sha256（有锁时加上锁，见 test_envlock.py）",
   not os.path.exists(M.lock_path("base"))
   and M.spec_hash("base") == hashlib.sha256(open(os.path.join(M.SPECS, "base.yml"), "rb").read()).hexdigest())
ok("没有的描述 → 哈希 None", M.spec_hash("没有这个") is None)

# ---------- 假机器 ----------
SAVE_M, SAVE_R = copy.deepcopy(M.MACHINES), copy.deepcopy(M.RELAY)
M.RELAY["tsite"] = M.SITES["tsite"] = dict(label="测试", hostname=r"(?!)", ssh="tsite", scheduler="pueue", root="/r/x1",
                       conda=("/opt/c", "base"), envs={"base": ("spec", "cu128"), "alt": "spec", "old": ("conda", "o")},
                       roles=("train",))
M.RELAY["tslurm"] = M.SITES["tslurm"] = dict(label="测试 Slurm", hostname=r"(?!)", ssh="tslurm", scheduler="slurm", root="/s/y",
                        conda=("/app/c", "pt"), envs={"base": "spec"}, roles=("train",),
                        transfer_job="-p cpu-x --qos=normal -c 4 --mem=4G")
SAVE_S = copy.deepcopy(M.SITES)
M.SITES.update(tsite=M.RELAY["tsite"], tslurm=M.RELAY["tslurm"])  # machine() 读 SITES（10/31 起 RELAY 是它的一部分）

a = M.activate("tsite", "alt")
ok("spec 环境的激活语句 = 根目录下的 .activate", "source /r/x1/envs/alt/.activate" in a)
ok("conda 条目照旧", "conda activate o" in M.activate("tsite", "old"))
ok("没有 .ok 时 spec 环境不就绪，conda 的就绪", M.ready_envs("tsite") == {"old"})
h = M.spec_hash("base")
ok(".ok 对得上才就绪", M.ready_envs("tsite", {"base": h}) == {"old", "base"})
ok(".ok 是旧描述的哈希 → 不就绪", M.ready_envs("tsite", {"base": "0" * 64}) == {"old"})
ok("现有机器条目的环境都还就绪（没改成 spec）",
   all(M.ready_envs(n) == set(M.machine(n).get("envs") or {}) for n in list(SAVE_M) + list(SAVE_R)))

# ---------- 送任务的门槛（refill.candidates，relay 用它） ----------
def task(key, env):
    return dict(key=key, state="free", nice=0, order=0, spec=dict(env=env, device="gpu"))


tq = dict(tasks=[task("a", "base"), task("b", "old"), task("c", "alt")])
refill.info = lambda tq_, k: dict(spec=next(t["spec"] for t in tq_["tasks"] if t["key"] == k), recipe=None, params={})
refill.progress = lambda s, k, h=None: None
got = lambda oks: sorted(t["key"] for t in refill.candidates({}, "tsite", tq, set(), oks))
ok("没有 .ok：只送 conda 环境的任务", got(None) == ["b"])
ok("base 的 .ok 对上：也送 base 的", got({"base": h}) == ["a", "b"])
ok("alt 的 .ok 是旧的：不送", got({"base": h, "alt": "x"}) == ["a", "b"])

# ---------- 建环境的脚本和命令 ----------
s_ = E.build_script("tsite", "alt")
ok("venv：先激活基础环境（它的 .activate）", "source /r/x1/envs/base/.activate" in s_)
ok("venv：--system-site-packages", "--system-site-packages" in s_)
M.RELAY["tsite"]["envs"]["alt3"] = "spec"
s_ = E.build_script("tsite", "alt3")
del M.RELAY["tsite"]["envs"]["alt3"]
ok("占位换成按环境算的变量", "${SPEC_TORCH_MM}" in s_ and "{torch_mm}" not in s_)
ok("看不到卡", "export CUDA_VISIBLE_DEVICES=" in s_)
ok("检查过才写 .ok、换软链接", s_.index("CHECK_EOF") < s_.index('mv -T') < s_.index('.ok"'))
s_ = E.build_script("tsite", "base")
ok("conda：按变体装 torch（cu128）", "whl/cu128" in s_ and "torch==2.7.1" in s_)
ok("conda：建在根目录下的 <名>-<哈希>", 'P="$ROOT/$N-${H:0:12}"' in s_ and "ROOT=/r/x1/envs" in s_)
ok("检查拿到代码目录", "python3 - /r/x1/code/current" in s_)
cmd, host = E.runner("tsite")
ok("pueue 类：nice、看不到卡、在那台上", host == "tsite" and "nice -n 10 env CUDA_VISIBLE_DEVICES=" in cmd and "srun" not in cmd)
cmd, host = E.runner("tslurm")
ok("Slurm：CPU 分区的 srun，排着等（不设 -I）", cmd.count("srun -p cpu-x") == 1 and "gres" not in cmd and " -I" not in cmd)
M.RELAY["tslurm"]["env_job"] = "-p gpu-a --gres=gpu:1"
try:
    E.runner("tslurm")
    ok("env_job 要了 GPU → 报错", False)
except E.EnvError:
    ok("env_job 要了 GPU → 报错", True)
try:
    E.build_script("tsite", "old")
    ok("不是按描述建的环境 → 报错", False)
except E.EnvError:
    ok("不是按描述建的环境 → 报错", True)
M.RELAY["tsite"]["envs"]["alt"] = "spec"
ok("dry-run 不跑命令", E.build("tsite", "alt", dry=True,
                              run=lambda *a, **k: (_ for _ in ()).throw(AssertionError("跑了")))[0] == 0)
ok("生成的脚本 bash -n 过", all(subprocess.run(["bash", "-n"], input=E.build_script("tsite", n), text=True).returncode == 0
                                for n in ("base", "alt")))

# ---------- 状态 ----------
def fake(txt):
    return lambda cmd, host, timeout, input=None: (0, txt, "")


now = 2_000_000_000
st = E.states("tsite", run=fake(f"ENV|base|{h}|||1\nENV|alt||||0\nNOW|{now}\n"))
ok("状态：ok、没建、conda 的不是按描述建的", st == {"old": "不是按描述建的", "base": "ok", "alt": "没建"})
uh = M.spec_hash("alt")
st = E.states("tsite", run=fake(f"ENV|base|{'0'*64}|||1\nENV|alt||{uh}||0\nNOW|{now}\n"))
ok("状态：描述变了、检查失败", st["base"] == "描述变了" and st["alt"] == "检查失败")
st = E.states("tsite", run=fake(f"ENV|base|||{h} {now - 60}|0\nENV|alt|||{uh} {now - 7 * 3600}|0\nNOW|{now}\n"))
ok("状态：在建、在建很久", st["base"] == "在建" and st["alt"] == "在建（可能已停）")
ok("连不上 → None", E.states("tsite", run=lambda *a, **k: (255, "", "")) is None)
ok("oks_of 只放 ok 的（哈希按这台用的变体）", E.oks_of({"base": "ok", "alt": "没建"}, "tsite") == {"base": M.spec_hash("base", "cu128")})

# ---------- 代码包里的机器条目 ----------
import relay_bundle as RB  # noqa: E402
e = RB.machine_entry("tsite", M.RELAY["tsite"], "/r/x1")
ok("代码包的条目带 env_root，那边的激活语句指到根目录下", e["env_root"] == "/r/x1/envs")
src = RB.gen_machines(open(os.path.join(fixture.CONFIG, "machines.py"), encoding="utf-8").read(), "tsite", e)
ns = {"__name__": "x", "__file__": os.path.join(fixture.CONFIG, "machines.py")}
exec(compile(src, "machines.py", "exec"), ns)
M.MACHINES.update(ns["MACHINES"])
ok("生成的 machines.py 的条目能用，spec 环境的激活语句对", "source /r/x1/envs/alt/.activate" in M.activate("tsite", "alt"))
M.MACHINES.clear(); M.MACHINES.update(SAVE_M)
M.RELAY.clear(); M.RELAY.update(SAVE_R)
M.SITES.clear(); M.SITES.update(SAVE_S)

# ---------- 锁文件：不联网建（11/5） ----------
import base64  # noqa: E402
import json  # noqa: E402
import zipfile  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_transfer as RT  # noqa: E402


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def tiny_wheel(d, name="tinypkg", ver="0.1"):
    """一个能装的最小 wheel（纯 Python）→ 路径。"""
    fn = f"{d}/{name}-{ver}-py3-none-any.whl"
    di = f"{name}-{ver}.dist-info"
    files = {f"{name}/__init__.py": f"VERSION = {ver!r}\n",
             f"{di}/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: {ver}\n",
             f"{di}/WHEEL": "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n"}
    rec = [f"{k},sha256={base64.urlsafe_b64encode(hashlib.sha256(v.encode()).digest()).rstrip(b'=').decode()},{len(v.encode())}"
           for k, v in files.items()] + [f"{di}/RECORD,,"]
    with zipfile.ZipFile(fn, "w") as z:
        for k, v in files.items():
            z.writestr(k, v)
        z.writestr(f"{di}/RECORD", "\n".join(rec) + "\n")
    return fn


def put(wh, p):
    """文件放进 wheelhouse（<前两位>/<sha256>）→ 锁里的一项的公共字段。"""
    h = sha(p)
    os.makedirs(f"{wh}/{h[:2]}", exist_ok=True)
    shutil.copy(p, f"{wh}/{h[:2]}/{h}")
    return dict(fn=os.path.basename(p), size=os.path.getsize(p), sha256=h, url="file://" + p)


def write_lock(specs, env, v, conda=(), pip=(), base=None):
    lk = dict(format=1, name=env, variant=v, spec_sha256=sha(f"{specs}/{env}.yml"), platform={"subdir": "linux-64"}, base=base,
              conda=list(conda), pip=list(pip))
    json.dump(lk, open(f"{specs}/{env}.{v}.lock.json", "w"))
    return lk


LD = fixture.tmpdir(prefix="envlock.", dir=os.environ.get("TMPDIR"))
SAVE_SPECS, SAVE_BA, SAVE_ST = M.SPECS, E.base_activation, os.environ.get("TASKQ_STATE")
os.makedirs(f"{LD}/specs")
for n in ("cbase", "tiny"):
    open(f"{LD}/specs/{n}.check.py", "w").write("import sys\nprint('ok', sys.prefix)\n")
open(f"{LD}/specs/cbase.yml", "w").write("python: '3.12'\nchannels: [conda-forge]\n")
open(f"{LD}/specs/tiny.yml", "w").write("base: sysbase\npip:\n  - tinypkg\n")
M.SPECS = f"{LD}/specs"
os.environ["TASKQ_STATE"] = f"{LD}/state"
WH = f"{LD}/state/{proto.WHEELHOUSE}"
whl = tiny_wheel(fixture.tmpdir(prefix="whl.", dir=LD))
ok("wheel 的文件名：本地版本换成元数据的版本，别的不动", E.wheel_name(dict(fn="m_s-2.3.0+cu12torch2.5-cp311-cp311-linux_x86_64.whl", version="2.3.0"))
   == "m_s-2.3.0-cp311-cp311-linux_x86_64.whl" and E.wheel_name(dict(fn="a-1.0-py3-none-any.whl", version="1.0")) == "a-1.0-py3-none-any.whl"
   and E.wheel_name(dict(fn="x-1.0.conda", version="1.0")) == "x-1.0.conda")
pkg = f"{LD}/fake-1.0-0.conda"
open(pkg, "wb").write(b"not really a conda package")
cpk = dict(put(WH, pkg), name="fake", version="1.0", build="0", channel="https://x/linux-64", subdir="linux-64",
           md5=hashlib.md5(open(pkg, "rb").read()).hexdigest())
wpk = dict(put(WH, whl), name="tinypkg", version="0.1", no_deps=True)
write_lock(M.SPECS, "cbase", "default", conda=[cpk])
write_lock(M.SPECS, "tiny", "default", pip=[wpk])
M.RELAY["lsite"] = M.SITES["lsite"] = dict(label="锁测试", hostname=r"(?!)", ssh="lsite", scheduler="pueue", root="/r/l", cap=50,
                                          conda=("/opt/c", "base"), envs={"cbase": "spec", "tiny": "spec", "sysbase": ("conda", "s")},
                                          roles=("train",), ssh_master=True)
s_ = E.build_script("lsite", "cbase", store="/r/l/blobs")
ok("锁：conda 不联网建（--offline、explicit 文件、临时包目录）", 'conda create --offline -y -p "$P" --file "$T/explicit.txt"' in s_
   and 'CONDA_PKGS_DIRS="$T/pkgs"' in s_ and "CONDA_OFFLINE=1" in s_ and "conda-forge" not in s_)
ok("锁：每个包按 sha256 从 blobs 硬链接成原来的文件名，explicit 里带 md5",
   f"pkgs {cpk['sha256']} {cpk['fn']} {cpk['md5']}" in s_ and "BL=/r/l/blobs" in s_
   and 'file://$T/pkgs/$fn#$md5' in s_ and 'ln -f "$s"' in s_)
s_ = E.build_script("lsite", "tiny", store="/r/l/blobs")
ok("锁：pip 只从本地装、按哈希、不装依赖", "--no-index --no-deps --require-hashes" in s_ and '--find-links "$T/wheels"' in s_
   and f"tinypkg==0.1 --hash=sha256:{wpk['sha256']}" in s_ and "pip install --index-url" not in s_)
ok("锁：临时目录在 trap 里删、.built 在换软链接之前", 'rm -rf "$T"' in s_ and s_.index("CHECK_EOF") < s_.index('> "$P/.built"') < s_.index("mv -T"))
ok("锁：bash -n 过", all(subprocess.run(["bash", "-n"], input=E.build_script("lsite", n, store="~/q/blobs"), text=True).returncode == 0
                       for n in ("cbase", "tiny")))
try:
    E.build_script("lsite", "cbase")
    ok("锁：没给放包的目录 → 报错", False)
except E.EnvError:
    ok("锁：没给放包的目录 → 报错", True)
ok("锁：要的空间 = 包 × 2.5 + 一份临时的", E.lock_need(E.load_lock("tiny", "default")) == int(wpk["size"] * 3.5))
ok("锁：wheelhouse 里的路径", E.lock_files(E.load_lock("tiny", "default")) == {wpk["sha256"]: f"{WH}/{wpk['sha256'][:2]}/{wpk['sha256']}"})
json.dump(dict(format=1, name="tiny", variant="default", conda=[], pip=[dict(wpk, fn="../x")]), open(f"{M.SPECS}/tiny.v2.lock.json", "w"))
try:
    E.load_lock("tiny", "v2")
    ok("锁：fn 里有路径 → 报错", False)
except E.EnvError:
    ok("锁：fn 里有路径 → 报错", True)
os.remove(f"{M.SPECS}/tiny.v2.lock.json")
ok("没有这个变体的锁 → None（照旧联网建）", E.load_lock("tiny", "nolock") is None)

# 预建：条目还是 conda / venv 的也能建，venv 的基础环境用按描述建的那个
M.RELAY["lsite"]["envs"]["tiny"] = ("venv", "/old/tiny", "sysbase")
open(f"{LD}/specs/sysbase.yml", "w").write("python: '3.12'\n")
open(f"{LD}/specs/sysbase.check.py", "w").write("print('ok')\n")
s_ = E.build_script("lsite", "tiny", store="/r/l/blobs")
ok("预建：条目是 venv 的也能建，基础环境用 env_root 下按描述建的", "source /r/l/envs/sysbase/.activate" in s_)
ok("预建：要建的里基础环境排在前面", list(E.wanted("lsite", ["tiny"])) == ["cbase", "sysbase", "tiny"])
os.remove(f"{LD}/specs/sysbase.yml")
ok("基础环境没有描述：只建它自己", list(E.wanted("lsite", ["tiny"])) == ["cbase", "tiny"])
M.RELAY["lsite"]["envs"]["tiny"] = "spec"
ok("条目里 spec 的都要建、变体按条目", E.wanted("lsite") == {"cbase": "default", "tiny": "default"})

# 状态：.failed 的两种写法
hc, ht = E.vhash("cbase", "default"), E.vhash("tiny", "default")
now = 2_000_000_000
W = {"cbase": "default", "tiny": "default"}
rows = {"cbase": dict(failed=f"{hc} {now - 60} rc 1", log="a\nb\n坏了"), "tiny": dict(failed=f"{ht} {now - 60} nospace 900 100")}
st = E.classify(rows, W, now)
ok("状态：rc → 检查失败，nospace → 空间不够", st == {"cbase": "检查失败", "tiny": "空间不够"})
ok("状态：旧写法（只有哈希）还认", E.classify({"cbase": dict(failed=hc)}, {"cbase": "default"}, now) == {"cbase": "检查失败"})
ok("下一个：检查失败的不重建，空间不够的没过 6 小时不再试", E.next_build(W, st, rows, now) is None)
ok("下一个：空间不够的 6 小时后再试", E.next_build(W, st, rows, now + 7 * 3600) == ("tiny", "default"))
ok("下一个：有在建的就不建", E.next_build(W, {"cbase": "在建", "tiny": "没建"}, {}, now) is None)
ok("下一个：在建停了的重建", E.next_build(W, {"cbase": "在建（可能已停）", "tiny": "ok"}, {}, now) == ("cbase", "default"))
pr = dict(E.problems("lsite", W, st, rows))
ok("报：检查失败带日志最后几行，空间不够带要多少剩多少", "坏了" in pr["cbase"] and "0.0 GB" in pr["tiny"] and "空间不够" in pr["tiny"])
os.rename(f"{WH}/{wpk['sha256'][:2]}/{wpk['sha256']}", f"{LD}/moved")
pr = E.problems("lsite", {"tiny": "default"}, {"tiny": "没建"}, {})
ok("报：wheelhouse 里缺包", len(pr) == 1 and "wheelhouse" in pr[0][1])
os.rename(f"{LD}/moved", f"{WH}/{wpk['sha256'][:2]}/{wpk['sha256']}")

# build()：执行机先查空间、钉住、放 blobs，再跑；空间不够不跑；长连接断着不连
calls = []


class Pin:
    def __init__(self, name, key, hashes, say=print):
        calls.append(("pin", key, sorted(hashes)))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        calls.append(("unpin",))


SAVE_RT, SAVE_MD = (RT.pinned, RT.ensure_blobs), RM.master_down
RT.pinned = Pin
RT.ensure_blobs = lambda name, files, say=print, **kw: calls.append(("blobs", sorted(files))) or {h: f"/r/l/blobs/{h[:2]}/{h}" for h in files}
free = [10 ** 12]


def run_py(name, code, data):
    calls.append(("mark", data["mode"], data["need"], data["cap"]))
    return dict(ok=free[0] >= data["need"], free=free[0], need=data["need"])


def run_cmd(cmd, host, timeout, input=None):
    calls.append(("run", host))
    return 0, "建好", ""


RM.master_down = lambda m: False
rc, out = E.build("lsite", "tiny", run=run_cmd, run_py=run_py, say=lambda *_: None)
ok("执行机：查空间（按 cap）→ 钉住 → 放进 blobs → 跑 → 撤钉", rc == 0 and [c[0] for c in calls] == ["mark", "pin", "blobs", "run", "unpin"]
   and calls[0][3] == 50 and calls[1][2] == [wpk["sha256"]], calls)
calls.clear()
free[0] = 10
rc, out = E.build("lsite", "tiny", run=run_cmd, run_py=run_py, say=lambda *_: None)
ok("执行机：空间不够 → 不传、不跑、退出码 1", rc == 1 and [c[0] for c in calls] == ["mark"] and "空间不够" in out, (calls, out))
calls.clear()
free[0] = 10 ** 12


def noslot(*a, **k):
    raise RM.NoSlot("排不上")


RT.ensure_blobs = noslot
rc, out = E.build("lsite", "tiny", run=run_cmd, run_py=run_py, say=lambda *_: None)
ok("执行机：CPU 作业排不上 → stage_later、撤掉 .building、不跑", rc == proto.EXIT["stage_later"]
   and [c[:2] for c in calls] == [("mark", "reserve"), ("pin", "env.tiny"), ("unpin",), ("mark", "clear")], calls)
calls.clear()
RM.master_down = lambda m: True
try:
    E.build("lsite", "tiny", run=run_cmd, run_py=run_py)
    ok("长连接断着 → 报错，不连", False)
except E.EnvError:
    ok("长连接断着 → 报错，不连", calls == [])
RM.master_down = SAVE_MD
RT.pinned, RT.ensure_blobs = SAVE_RT
M.MACHINES["lremote"] = dict(label="远程", hostname=r"(?!)", ssh="lr", scheduler="pueue", conda=None, envs={"tiny": "spec"}, roles=())
try:
    E.build("lremote", "tiny", run=run_cmd)
    ok("有锁的不在 hub、执行机以外的远程机器上建", False)
except E.EnvError:
    ok("有锁的不在 hub、执行机以外的远程机器上建", True)
del M.MACHINES["lremote"]
for k in ("lsite",):
    M.RELAY.pop(k, None)
    M.SITES.pop(k, None)

# 本机真建（不联网）：venv 建在一个现成的 Python 上，pip 包只从 wheelhouse 来
pyd = next(p for p in (os.path.expanduser("~/miniforge3/bin"), os.path.dirname(sys.executable))
           if subprocess.run([f"{p}/python3", "-c", "import venv, ensurepip"], capture_output=True).returncode == 0)
E.base_activation = lambda name, base: f'export PATH={pyd}:"$PATH"'
open(f"{LD}/specs/tiny.check.py", "w").write("import tinypkg, sys\nprint('tinypkg', tinypkg.VERSION, sys.prefix)\n")
M.MACHINES["toff"] = dict(label="本机", hostname=r"(?!)", ssh=None, scheduler="pueue", root=f"{LD}/root", conda=None,
                          envs={"sysbase": ("conda", "unused"), "tiny": "spec"}, roles=("train",))
PX = {k: os.environ.get(k) for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "PIP_INDEX_URL")}
os.environ.update(http_proxy="http://127.0.0.1:9", https_proxy="http://127.0.0.1:9", HTTP_PROXY="http://127.0.0.1:9",
                  HTTPS_PROXY="http://127.0.0.1:9", PIP_INDEX_URL="http://127.0.0.1:9/simple")
rc, out = E.build("toff", "tiny")
ok("本机不联网建：退出码 0", rc == 0, out)
okf = f"{LD}/root/state/envs/tiny.ok"
ok("本机不联网建：.ok 是哈希，状态 ok，没留临时目录", os.path.isfile(okf) and open(okf).read().strip() == E.vhash("tiny", "default")
   and E.states("toff")["tiny"] == "ok" and not glob.glob(f"{LD}/root/envs/.tmp-*"))
r = subprocess.run(["bash", "-c", M.activate("toff", "tiny") + "\npython -c 'import tinypkg; print(tinypkg.__file__)'"],
                   capture_output=True, text=True)
ok("本机不联网建：激活后能导入 wheelhouse 里的包", r.returncode == 0 and f"{LD}/root/envs/tiny-" in r.stdout, r.stdout + r.stderr)
hb = "0" * 64  # 内容和锁里的哈希对不上
os.makedirs(f"{WH}/00", exist_ok=True)
shutil.copy(whl, f"{WH}/00/{hb}")
open(f"{LD}/specs/tiny.yml", "a").write("# 改了\n")
write_lock(M.SPECS, "tiny", "default", pip=[dict(wpk, sha256=hb)])
rc, out = E.build("toff", "tiny")
st = E.states("toff")["tiny"]
ok("包的哈希对不上 → pip 不装、检查失败、旧环境照旧在用", rc != 0 and st == "检查失败" and os.path.isfile(f"{LD}/root/envs/tiny/.activate"),
   (rc, st, out))
for k, v in PX.items():
    if v is None:
        os.environ.pop(k, None)
    else:
        os.environ[k] = v
del M.MACHINES["toff"]
M.SPECS, E.base_activation = SAVE_SPECS, SAVE_BA
if SAVE_ST is None:
    os.environ.pop("TASKQ_STATE", None)
else:
    os.environ["TASKQ_STATE"] = SAVE_ST

# ---------- 真建一个小环境（--real：在临时目录里，只有 numpy） ----------
if "--real" in sys.argv:
    d = fixture.tmpdir(prefix="envtest.", dir=os.environ.get("TMPDIR"))
    os.makedirs(f"{d}/specs")
    open(f"{d}/specs/tiny.yml", "w").write("# 测试\nbase: sysbase\npip:\n  - numpy\n")
    open(f"{d}/specs/tiny.check.py", "w").write("import numpy\nprint('numpy', numpy.__version__)\n")
    M.SPECS = f"{d}/specs"
    M.MACHINES["treal"] = dict(label="本机测试", hostname=r"(?!)", ssh=None, scheduler="pueue", root=f"{d}/root",
                           conda=None, envs={"sysbase": ("conda", "unused"), "tiny": "spec"}, roles=("train",))
    # 基础环境：本机一个带 venv、没有 numpy 的 python（hub 上是 miniforge 的 base）
    pyd = next(p for p in (os.path.expanduser("~/miniforge3/bin"), os.path.dirname(sys.executable))
               if subprocess.run([f"{p}/python3", "-c", "import venv, ensurepip"], capture_output=True).returncode == 0)
    E.base_activation = lambda name, base: f'export PATH={pyd}:"$PATH"'
    rc, out = E.build("treal", "tiny")
    print(out)
    ok("真建：退出码 0", rc == 0)
    okf = f"{d}/root/state/envs/tiny.ok"
    ok("真建：写了 .ok，内容是描述的哈希", os.path.isfile(okf) and open(okf).read().strip() == M.spec_hash("tiny"))
    ok("真建：软链接和 .activate 在", os.path.isfile(f"{d}/root/envs/tiny/.activate"))
    r = subprocess.run(["bash", "-c", M.activate("treal", "tiny") + "\npython -c 'import numpy; print(numpy.__file__)'"],
                       capture_output=True, text=True)
    ok("真建：激活后能导入 numpy，用的是新环境的", r.returncode == 0 and f"{d}/root/envs/tiny-" in r.stdout)
    ok("真建：状态 ok", E.states("treal") == {"sysbase": "不是按描述建的", "tiny": "ok"})
    open(f"{d}/specs/tiny.yml", "a").write("# 改一下\n")
    ok("真建：描述改了 → 描述变了、不就绪", E.states("treal")["tiny"] == "描述变了"
       and "tiny" not in M.ready_envs("treal", E.oks_of(E.states("treal"), "treal")))
    open(f"{d}/specs/tiny.check.py", "w").write("import sys\nsys.exit('故意失败')\n")
    rc, out = E.build("treal", "tiny")
    st = E.states("treal")["tiny"]
    ok("真建：检查失败 → 退出码非 0、状态检查失败、旧的 .ok 不变", rc != 0 and st == "检查失败"
       and open(okf).read().strip() != M.spec_hash("tiny"))
    shutil.rmtree(d)

    # conda + pip 都从锁、不联网（11/5）：先按 conda 解出的包从 conda-forge 下载进 wheelhouse，再断网建
    import urllib.request
    cd_ = os.path.expanduser("~/miniforge3")
    d = fixture.tmpdir(prefix="envtestc.", dir=os.environ.get("TMPDIR"))
    os.makedirs(f"{d}/specs")
    open(f"{d}/specs/creal.yml", "w").write("python: '3.10.12'\nchannels: [conda-forge]\npip:\n  - tinypkg\n")
    open(f"{d}/specs/creal.check.py", "w").write("import sys, tinypkg\nassert sys.version.startswith('3.10.12'), sys.version\nprint('ok', tinypkg.VERSION)\n")
    M.SPECS = f"{d}/specs"
    os.environ["TASKQ_STATE"] = f"{d}/state"
    wh = f"{d}/state/{proto.WHEELHOUSE}"
    js = json.loads(subprocess.run([f"{cd_}/bin/conda", "create", "--dry-run", "--json", "-p", f"{d}/solve", "-c", "conda-forge",
                                    "--override-channels", "python=3.10.12"], capture_output=True, text=True, timeout=600).stdout)
    dl = fixture.tmpdir(prefix="dl.", dir=d)
    cpks = []
    for x in js["actions"]["LINK"]:
        fn = x["dist_name"] + ".conda"
        url = f"{x['base_url']}/{x['platform']}/{fn}"
        urllib.request.urlretrieve(url, f"{dl}/{fn}")
        cpks.append(dict(put(wh, f"{dl}/{fn}"), name=x["name"], version=x["version"], build=x["build_string"], url=url,
                         channel=x["base_url"], subdir=x["platform"], md5=hashlib.md5(open(f"{dl}/{fn}", "rb").read()).hexdigest()))
    w2 = tiny_wheel(fixture.tmpdir(prefix="whl.", dir=d))
    w2l = w2.replace("tinypkg-0.1-", "tinypkg-0.1+local-")  # 文件名带本地版本、元数据没有（同 mamba_ssm 的 wheel，11/5）
    os.rename(w2, w2l)
    w2 = w2l
    write_lock(M.SPECS, "creal", "default", conda=cpks, pip=[dict(put(wh, w2), name="tinypkg", version="0.1", no_deps=True)])
    shutil.rmtree(dl)
    M.MACHINES["creal"] = dict(label="本机", hostname=r"(?!)", ssh=None, scheduler="pueue", root=f"{d}/root", conda=(cd_, "base"),
                               envs={"creal": "spec"}, roles=("train",))
    os.environ.update(http_proxy="http://127.0.0.1:9", https_proxy="http://127.0.0.1:9", HTTP_PROXY="http://127.0.0.1:9",
                      HTTPS_PROXY="http://127.0.0.1:9", PIP_INDEX_URL="http://127.0.0.1:9/simple", CONDA_OFFLINE="1")
    rc, out = E.build("creal", "creal")
    for k, v in dict(PX, CONDA_OFFLINE=None).items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    ok("真建 conda + pip（不联网，只用 wheelhouse）：退出码 0、状态 ok", rc == 0 and E.states("creal")["creal"] == "ok", out[-3000:])
    r = subprocess.run(["bash", "-c", M.activate("creal", "creal") + "\npython -c 'import sys, tinypkg; print(sys.prefix)'"],
                       capture_output=True, text=True)
    ok("真建 conda + pip：激活后是新环境的 Python 3.10.12、能导入 wheel", r.returncode == 0 and f"{d}/root/envs/creal-" in r.stdout,
       r.stdout + r.stderr)
    del M.MACHINES["creal"]
    M.SPECS = SAVE_SPECS
    if SAVE_ST is None:
        os.environ.pop("TASKQ_STATE", None)
    else:
        os.environ["TASKQ_STATE"] = SAVE_ST
    shutil.rmtree(d)

print("全部通过" if not fails else f"{fails} 项失败")
sys.exit(1 if fails else 0)
