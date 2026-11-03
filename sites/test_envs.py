#!/usr/bin/env python3
"""测 scripts/sites/envs.py 和机器表里按描述建的环境（2026-10-31）：描述能读、激活语句、.ok 对不上时不送任务、
建环境的命令只在 CPU 上跑；最后在临时目录里真建一个只有 numpy 的 venv（不联网时跳过装包那一步之外的部分不跑）。

    python3 scripts/sites/test_envs.py [--real]
"""
import copy
import glob
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "scripts", "queue"))
sys.path.insert(0, os.path.join(REPO, "scripts", "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
import envs as E  # noqa: E402
M = E.M
import refill  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
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
ok("哈希 = 描述内容的 sha256",
   M.spec_hash("base") == hashlib.sha256(open(os.path.join(M.SPECS, "base.yml"), "rb").read()).hexdigest())
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

# feed.py 的同一道门槛
import feed as F  # noqa: E402
t = dict(key="a", state="free", spec=dict(env="base"))
F.spec = lambda t_: t_["spec"]
ok("feed：没有 .ok 不送", not F.runnable("tsite", t))
ok("feed：.ok 对上送", F.runnable("tsite", t, {"base": h}))
ok("feed：从收集的状态里读 envs_ok", F.envs_ok({"src": {"tsite": {"envs_ok": {"base": h}}}}, "tsite") == {"base": h})

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
ok("Slurm：CPU 分区的 srun", cmd.count("srun -I600 -p cpu-x") == 1 and "gres" not in cmd)
M.RELAY["tslurm"]["env_job"] = "-p gpu-a100 --gres=gpu:1"
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
ok("oks_of 只放 ok 的", E.oks_of({"base": "ok", "alt": "没建"}) == {"base": h})

# ---------- 代码包里的机器条目 ----------
import relay_bundle as RB  # noqa: E402
e = RB.machine_entry("tsite", M.RELAY["tsite"], "/r/x1")
ok("代码包的条目带 env_root，那边的激活语句指到根目录下", e["env_root"] == "/r/x1/envs")
src = RB.gen_machines(open(os.path.join(fixture.CONFIG, "machines.py"), encoding="utf-8").read(), "tsite", e)
ns = {"__name__": "x", "__file__": os.path.join(fixture.CONFIG, "machines.py")}
exec(compile(src, "machines.py", "exec"), ns)
ok("生成的 machines.py 能用，spec 环境的激活语句对", "source /r/x1/envs/alt/.activate" in ns["activate"]("tsite", "alt"))
M.MACHINES.clear(); M.MACHINES.update(SAVE_M)
M.RELAY.clear(); M.RELAY.update(SAVE_R)
M.SITES.clear(); M.SITES.update(SAVE_S)

# ---------- 真建一个小环境（--real：在临时目录里，只有 numpy） ----------
if "--real" in sys.argv:
    d = tempfile.mkdtemp(prefix="envtest.", dir=os.environ.get("TMPDIR"))
    os.makedirs(f"{d}/specs")
    open(f"{d}/specs/tiny.yml", "w").write("# 测试\nbase: sysbase\npip:\n  - numpy\n")
    open(f"{d}/specs/tiny.check.py", "w").write("import numpy\nprint('numpy', numpy.__version__)\n")
    M.SPECS = f"{d}/specs"
    M.MACHINES["treal"] = dict(label="本机测试", hostname=r"(?!)", ssh=None, scheduler="pueue", root=f"{d}/root",
                           conda=None, envs={"sysbase": ("conda", "unused"), "tiny": "spec"}, roles=("train",))
    # 基础环境：本机一个带 venv、没有 numpy 的 python（box 上是 miniforge 的 base）
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
       and "tiny" not in M.ready_envs("treal", E.oks_of(E.states("treal"))))
    open(f"{d}/specs/tiny.check.py", "w").write("import sys\nsys.exit('故意失败')\n")
    rc, out = E.build("treal", "tiny")
    st = E.states("treal")["tiny"]
    ok("真建：检查失败 → 退出码非 0、状态检查失败、旧的 .ok 不变", rc != 0 and st == "检查失败"
       and open(okf).read().strip() != M.spec_hash("tiny"))
    shutil.rmtree(d)

print("全部通过" if not fails else f"{fails} 项失败")
sys.exit(1 if fails else 0)
