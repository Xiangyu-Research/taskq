#!/usr/bin/env python3
"""测 check.py：固定配置没有问题；改坏的副本（机器表、policy、环境描述）、改坏的插件、改坏的实验表（在进程里改）都报出来；
原始数据按 store 的数据目录查（真跑 site_ops 的 probe）。

    python3 queue/test_check.py
"""
import copy
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
os.environ["HOME"] = fixture.tmpdir()
sys.path.insert(0, HERE)

fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def run(*a, env=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "check.py"), *a], capture_output=True, text=True, env={**os.environ, **(env or {})})
    return r.returncode, r.stdout, r.stderr


FAST = ("machines", "policy", "plugins", "experiments", "flow", "tasks", "steps")
rc, out, err = run("--quick")
ok("固定配置：退出码 0、--quick 的每组都查了、没有\"错\"", rc == 0 and "错  " not in err and all(g in out for g in FAST))
ok("--quick：不跑 inputs、raw、bundle", not any(g in out for g in ("inputs ", "raw ", "bundle ")))
rc, out, err = run("--only", "inputs")
ok("inputs：每个写了 inputs_cmd 的步骤真跑一次（空的数据目录里），列得出输入", rc == 0 and "inputs" in out and "错  " not in err)
rc, out, err = run("--only", "policy")
ok("--only：只查那一组", rc == 0 and out.splitlines()[0].startswith("policy") and "machines" not in out)

# 改坏的配置副本
bad = fixture.tmpdir()
cfgdir = os.path.join(bad, "config")
shutil.copytree(os.environ["TASKQ_CONFIG"], cfgdir, ignore=shutil.ignore_patterns("__pycache__"))


def edit(name, old, new, d=cfgdir):
    p = os.path.join(d, name)
    t = open(p, encoding="utf-8").read()
    assert old in t, (name, old)
    open(p, "w", encoding="utf-8").write(t.replace(old, new, 1))


edit("policy.py", '"p1": ["site-slurm"],', '"p1": ["nosuch"], "nostep": "any", "eval": ["site-slurm2"],')
edit("policy.py", 'READY = {"site-slurm2": 2', 'READY = {"site-slurm2": -1, "hub": 1')
edit("policy.py", "AUTO = dict(advance=True,", "AUTO = dict(advance=True, ledger=True, site_pueue=False,")
edit("policy.py", "PAUSE = {\n}", 'PAUSE = {"site-pueue": ("11/8", "坏了"), "gone": (None, "x")}\nOFFLINE = {"site-pueue"}')
edit("machines.py", "dict(gpu=\"1\"),", "dict(gpu=\"1\", slot=2), dict(gpu=\"1\"),")
edit("machines.py", '"alt": ("venv", FAKE + "/venv/alt", "base"),\n', '"alt": ("venv", FAKE + "/venv/alt", "nobase"),\n')
edit("machines.py", r'r"https://[a-z0-9-]+\.trycloudflare\.com"', 'r"https://[a-z"')
edit("machines.py", 'cap=90, budget=(15, 30)', 'cap=20, budget=(15, 30), workers=[dict(gpu="0")], job_names="nosuch"')
edit("machines.py", 'roles=("backup",), backup="~/st/DEMO_DATA",', 'roles=("backup",), backup="~/st/DEMO_DATA", group="gpu",')
edit("machines.py", "\n}\n", '\n    "loc1": dict(label="L1", hostname=r"x1", scheduler="pueue", root="/data/others/DEMO_DATA/q", workers=[dict(gpu="0")]),\n'
     '    "loc2": dict(label="L2", hostname=r"x2", root="/tmp/loc2"),\n'
     '    "loc3": dict(label="L3", hostname=r"x3", ssh="fake-l3", scheduler="pueue", workers=[dict(gpu="0")]),\n'
     '    "loc4": dict(label="L4", hostname=r"x4", scheduler="pueue", root="/tmp/loc4", workers=[dict(gpu="0")]),\n}\n')
edit("specs/alt.yml", "pip: []", 'pip: ["pkg{bogus}"]')
edit("specs/alt2.check.py", "import sys", "def (")
rc, out, err = run("--quick", env={"TASKQ_CONFIG": cfgdir})
ok("改坏的配置：退出码 1", rc == 1)
ok("不认识的字段：说出是哪台、哪个字段，提示 taskq machines fields",
   "MACHINES['site-slurm'] 的 group" not in err and "MACHINES['site-slurm']：不认识的字段 group" in err
   and "workers[0]：不认识的字段 slot" in err and "taskq machines fields" in err)
ok("同一张卡写了两个 worker", "worker 名字重复" in err and "gpu1" in err)
ok("slurm 执行机不写 workers、job_names 要在 STYLES 里", "MACHINES['site-slurm2']：slurm 执行机不写 workers" in err
   and "job_names 'nosuch'" in err)
ok("budget 第二项不小于 cap", "budget 第二项 30 GB 不小于 cap 20 GB" in err)
ok("ssh=None 的执行机最多一个（只有根目录的不算执行机）", "执行机最多一个，现在 ['loc1', 'loc4']" in err)
ok("执行机的根目录不能在 store 的数据目录里", "MACHINES['loc1'] 的 root 在 store 的数据目录里面" in err
   and "MACHINES['loc4'] 的 root" not in err)
ok("写了 scheduler 要写 root", "MACHINES['loc3']：执行机（写了 scheduler）要写 root" in err)
ok("写了 root 没写 scheduler：只有 store、hub 可以", "MACHINES['loc2']：写了 root 没写 scheduler" in err
   and "MACHINES['hub']：写了 root" not in err)
ok("venv 的底子不在 envs 里", "nobase" in err)
ok("quick 第二项不是正则", "MACHINES['hub'] 的 quick 要写成" in err or "quick 第二项不是正则" in err)
ok("环境描述的 pip 里有不认识的占位、check.py 编译不过", "['bogus']" in err and "环境描述 alt2：SyntaxError" in err)
ok("PLACE：不是执行机、不是步骤或做法、能去的执行机没有这种 worker",
   "PLACE['p1'] 里的 ['nosuch'] 不是执行机" in err and "PLACE['nostep'] 不是任何步骤" in err
   and "PLACE['eval'] 的执行机 ['site-slurm2'] 都没有 cpu 的 worker" in err)
ok("READY 负数、不是执行机", "READY 的 -1" in err or "READY 要写成" in err and "READY 里的 ['hub'] 不是执行机" in err)
ok("AUTO 只放开关（旧的 ledger、机器名都报）", "AUTO 里的 'ledger' 不是开关" in err and "'site_pueue' 不是开关" in err and "PAUSE" in err)
ok("PAUSE：时间格式、不是机器", "PAUSE 要写成" in err and "PAUSE 里的 ['gone'] 不是机器" in err)
ok("policy 里不是字段的表（旧的 OFFLINE）报出来", "policy.py 的 OFFLINE 不是 policy 的字段" in err)
ok("问题在 stderr、每组的结果在 stdout", "个问题" in out and "错  " not in out)

# 只放一个错：缺了 store 时其余照查，不中途出错
one = os.path.join(fixture.tmpdir(), "config")
shutil.copytree(os.environ["TASKQ_CONFIG"], one, ignore=shutil.ignore_patterns("__pycache__"))
edit("machines.py", 'roles=("hub", "store")', 'roles=("hub",)', d=one)
rc, out, err = run("--only", "machines", env={"TASKQ_CONFIG": one})
ok("没有 store：报出来，检查自己不出错", rc == 1 and "roles 带 store 的机器要正好一台，现在 没有" in err and "自己出错" not in err)

# hub、store 的规则：每种情况一份配置副本，只查 machines
HUB = 'roles=("hub", "store"), repo="~/auto/demo",\n                root="~/demo_root", data="/data/others/DEMO_DATA",'
TRANSFER = 'transfer=("https://transfer.example.org", "172.18.0.1:18090"),'


def machines_err(*edits):
    d = os.path.join(fixture.tmpdir(), "config")
    shutil.copytree(os.environ["TASKQ_CONFIG"], d, ignore=shutil.ignore_patterns("__pycache__"))
    for old, new in edits:
        edit("machines.py", old, new, d=d)
    rc, out, err = run("--only", "machines", env={"TASKQ_CONFIG": d})
    return rc, err


rc, err = machines_err(('ssh=None, roles=("hub", "store")', 'ssh="fake-hub", roles=("hub", "store")'))
ok("hub 的 ssh 不是 None：报出来", rc == 1 and "hub（hub）的 ssh 写 None" in err and "自己出错" not in err)
rc, err = machines_err((HUB, 'roles=("hub", "store"), repo="~/auto/demo",'), (TRANSFER, ""))
ok("store 没写 data、root；执行机写了 tunnel 时 store 没写 transfer：都报", rc == 1 and "store（hub）要写 data" in err
   and "store（hub）要写 root" in err and "store（hub）要写 transfer：site-pueue" in err and "自己出错" not in err)
# hub 和 store 分开：store 在别的机器上（经 ssh），hub 只统筹
ST = ('\n}\n', '\n    "st": dict(label="ST", hostname=r"st-host", ssh="fake-st", roles=("store",), root="~/st_root",'
      ' data="/data/st", transfer=("https://t.example.org", "10.0.0.1:18090")),\n}\n')
rc, err = machines_err((HUB, 'roles=("hub",), repo="~/auto/demo",'), (TRANSFER, ""), ST)
ok("hub、store 分开、没有本机执行机、没有锁好的环境：hub 不用写 root、transfer", rc == 0 and "错  " not in err)
rc, err = machines_err((HUB, 'roles=("hub",), repo="~/auto/demo",'), (TRANSFER, ""), ST,
                       ('envs={"base": ("conda", "w0"),', 'envs={"sp": "spec", "base": ("conda", "w0"),'),
                       ('\n}\n', '\n    "loc": dict(label="L", hostname=r"xl", scheduler="pueue", root="/tmp/locx", workers=[dict(gpu="0")]),\n}\n'))
ok("有 ssh=None 的执行机、store 在别的机器上：store 也要在这台", "loc 和 hub 在同一台上（ssh 是 None），store（st）在别的机器上" in err)
ok("有用锁好环境的执行机：hub 要写 root，经隧道送时还要 transfer",
   "hub（hub）要写 root：site-pueue 用锁好的环境" in err and "hub（hub）要写 transfer：site-pueue" in err)

# 改坏的 labels 插件：值的类型不对、PLAN_TITLES() 出错，--only plugins 都报出来
lab = os.path.join(fixture.tmpdir(), "plugins")
os.makedirs(lab)
with open(os.path.join(lab, "labels.py"), "w", encoding="utf-8") as f:
    f.write('STEPS = {"zz": 3}\nQUIET = [1]\n\n\ndef PLAN_TITLES():\n    return 1 / 0\n')
rc, out, err = run("--only", "plugins", env={"TASKQ_PLUGINS": lab})
ok("labels 插件写坏：STEPS 的中文名、QUIET、PLAN_TITLES() 出错都报", rc == 1 and "['zz']" in err and "[1]" in err
   and "ZeroDivisionError" in err)
imp = os.path.join(fixture.tmpdir(), "plugins")
os.makedirs(imp)
with open(os.path.join(imp, "bad_import.py"), "w", encoding="utf-8") as f:
    f.write("import taskq_api\nimport refill\n")
rc, out, err = run("--only", "plugins", env={"TASKQ_PLUGINS": os.environ["TASKQ_PLUGINS"] + os.pathsep + imp})
ok("插件直接 import 核心的模块：报出来（只 import taskq_api）", "bad_import.py 直接 import 核心的 ['refill']" in err)

# store 上的操作真跑：store 根目录（~/demo_root）的 code/current 是手搭的代码包（fixture.store_bundle），conda 的 python3 是跑测试的这个
STORE_ROOT = os.path.join(os.environ["HOME"], "demo_root")
conda = fixture.store_bundle(STORE_ROOT)

# 原始数据（raw）：真跑 store 上的 probe；store 的数据目录指到一个临时目录
data = fixture.tmpdir()
rawc = os.path.join(fixture.tmpdir(), "config")
shutil.copytree(os.environ["TASKQ_CONFIG"], rawc, ignore=shutil.ignore_patterns("__pycache__"))
edit("machines.py", 'data="/data/others/DEMO_DATA", conda=(FAKE_CONDA, "base0")', f'data={data!r}, conda=({conda!r}, "base0")', d=rawc)
for p in ("raw/holdout/A/inputs/a.in", "raw/labels/x/1-lab.dat", "raw/ext/E/labels/1.dat", "demo/probe/p1/inputs/a",
          "demo/probe/p1/targets.csv", "demo/probe/p2/inputs/a", "demo/probe/p2/items/a"):
    os.makedirs(os.path.dirname(os.path.join(data, p)), exist_ok=True)
    open(os.path.join(data, p), "w").close()
rc, out, err = run("--only", "raw", env={"TASKQ_CONFIG": rawc})
miss = [ln for ln in err.splitlines() if ln.startswith("错  [raw]")]
ok("raw：store 上有的（通配也算）不报；没有的按路径报，说出谁要用",
   rc == 1 and miss and all("splits_final.json" in ln for ln in miss) and "prepared/D101_alpha/splits_final.json" in err
   and "p1/arch_m" in err and "raw/labels" not in err)
for d in ("D101_alpha", "D103_alpha_one", "D111_beta", "D116_gamma"):
    os.makedirs(os.path.join(data, "prepared", d))
    open(os.path.join(data, "prepared", d, "splits_final.json"), "w").close()
rc, out, err = run("--only", "raw", env={"TASKQ_CONFIG": rawc})
ok("raw：都在了 → 通过", rc == 0 and "错  " not in err)

# 改坏的实验表（在进程里改缓存的实验表和先后）
import check as K  # noqa: E402
import experiments as E  # noqa: E402
exps, final = E.load()
saved = copy.deepcopy(exps), copy.deepcopy(final)


def problems(*groups):
    K._FLOW.clear()
    return "\n".join(m for _, m in K.run(only=list(groups)).problems)


def restore():
    exps[:], final[:] = copy.deepcopy(saved[0]), copy.deepcopy(saved[1])
    K._FLOW.clear()


ok("没改：experiments、flow、steps 都没有问题", not problems("experiments", "flow", "steps"))
e0 = exps[0]
e0["after"][0] = dict(e0["after"][0], device="tpu", out=[])
e0["after"][-1] = dict(e0["after"][-1], cmd="echo {nosuch}")
e0["each"] = dict(frag="0")
msgs = problems("experiments", "steps")
ok("步骤的类型不对（device）、out 没写、each 不是列表", f"步骤 {e0['after'][0]['name']} 的 device 要写成gpu 或 cpu" in msgs
   and "out、done 至少各写一个" in msgs and f"实验 {e0['id']} 的 each['frag'] 要是非空列表" in msgs)
restore()
e0 = exps[0]
e0["after"][-1] = dict(e0["after"][-1], cmd="echo {nosuch}")
ok("步骤的命令里不认识的名字：变不成总队列里的任务", "{nosuch}" in problems("steps"))
restore()

ev = next(st for st in exps[0]["after"] if st["name"] == "eval")
for st in exps[0]["after"]:
    if st["name"] == "lprob":
        st["out"] = [ev["out"][0] + "/sub"]               # 产出在别的步骤的产出里
        st["inputs"] = st["inputs"] + [st["out"][0] + "/x"]  # 输入落在自己的产出里
msgs = problems("flow")
ok("先后：产出重叠、输入落在自己的产出里", "在 eval/t0_f0 的产出" in msgs and "lprob/t0_f0 的输入" in msgs and "落在自己的产出" in msgs)
restore()

oof = next(st for e in exps for st in e["group_after"] if st["name"] == "oof")
pai = next(st for e in exps for st in e["group_after"] if st["name"] == "paired")
oof["inputs"] = oof["inputs"] + [pai["out"][0]]  # oof ← paired ← oof
ok("先后有环", "先后有环" in problems("flow"))
restore()

for e in exps:
    for st in e["after"]:
        if st["name"] == "eval":
            st["env"] = ["base", "nosuch_env"]
ok("步骤要的环境没有一台有这种 worker 的执行机全有", "nosuch_env" in problems("steps"))
restore()
ok("改回来以后没有问题", not problems("experiments", "flow", "steps"))

# 冒烟定义：函数出错、环境参数没写进 by、产出不在 {smoke} 文件夹下，都按任务报出来
DEF = dict(by=["variant", "env"], params=dict(ds="SMOKE", frag="0", model="smoke/{smoke}"), vars={})
smk = lambda: "\n".join(m for _, m in K.run(only=["tasks"]).problems if "冒烟" in m)  # noqa: E731
try:
    K.SG.SMOKE["toy"] = DEF
    ok("冒烟定义没问题：不报", not smk())
    # 是函数的 params 在 store 上算：真跑 store 的 site_ops smoke，那边的插件（包里的 .taskq.toml）有同样的定义
    bad_smoke = fixture.tmpdir()
    with open(os.path.join(bad_smoke, "smoke.py"), "w", encoding="utf-8") as f:
        f.write('SMOKE = {"toy": dict(by=["variant", "env"], params=lambda tp, data: 1 / 0)}\n')
    fixture.store_bundle(STORE_ROOT, [bad_smoke])
    py, K.SG.RM.py = K.SG.RM.py, lambda name: os.path.join(conda, "bin", "python3")
    try:
        K.SG.SMOKE["toy"] = dict(DEF, params=lambda tp, data: 1 / 0)
        ok("params() 出错（在 store 上算）：报冒烟定义出错", "冒烟定义出错" in smk() and "ZeroDivisionError" in smk())
        K.SG.RM.py = lambda name: os.path.join(bad_smoke, "no_python3")
        lines = smk().splitlines()
        ok(f"store 那边出错（和任务无关）：每种做法只报一次（{lines}）", len(lines) == 1 and "没查完" in lines[0])
    finally:
        K.SG.RM.py = py
        fixture.store_bundle(STORE_ROOT)
    K.SG.SMOKE["toy"] = dict(DEF, by=["variant"], params=dict(DEF["params"], env="alt"))
    ok("冒烟任务换了环境：报环境不一样", "环境" in smk())
    K.SG.SMOKE["toy"] = dict(DEF, params=dict(ds="SMOKE", frag="0"))
    ok("产出不在 {smoke} 文件夹下：报出来", "{smoke}" in smk())
finally:
    K.SG.SMOKE.pop("toy", None)

print("全部通过" if not fails else f"{fails} 项失败")
sys.exit(1 if fails else 0)
