#!/usr/bin/env python3
"""测 check.py（2026-11-04）：固定配置没有问题；改坏的副本（机器表、policy）、改坏的实验表（在进程里改）都报出来。

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


rc, out, err = run()
ok("固定配置：退出码 0、每组都查了、没有\"错\"", rc == 0 and "错  " not in err and all(g in out for g in
   ("machines", "policy", "plugins", "experiments", "tasks", "steps", "needs", "bundle")))
rc, out, err = run("--quick")
ok("--quick：不跑 needs、bundle", rc == 0 and "needs" not in out and "bundle" not in out)
rc, out, err = run("--only", "policy")
ok("--only：只查那一组", rc == 0 and out.splitlines()[0].startswith("policy") and "machines" not in out)

# 改坏的配置副本
bad = fixture.tmpdir()
cfgdir = os.path.join(bad, "config")
shutil.copytree(os.environ["TASKQ_CONFIG"], cfgdir, ignore=shutil.ignore_patterns("__pycache__"))


def edit(name, old, new):
    p = os.path.join(cfgdir, name)
    t = open(p, encoding="utf-8").read()
    assert old in t, (name, old)
    open(p, "w", encoding="utf-8").write(t.replace(old, new, 1))


edit("policy.py", '"eval": ["hub:cpu"]', '"eval": ["pueue-host:gpu0"]')
edit("policy.py", 'READY = {"site-slurm2": 2', 'READY = {"site-slurm2": -1')
edit("policy.py", 'AUTO = {"advance": True,', 'AUTO = {"advance": True, "nosuch": True,')
edit("machines.py", 'worker=dict(group="gpu1", id="w7e2837")', 'worker=dict(group="gpuX", id="w7e2837")')
edit("machines.py", '"alt": ("venv", FAKE + "/venv/alt", "base"),', '"alt": ("venv", FAKE + "/venv/alt", "nobase"),')
edit("machines.py", r'r"https://[a-z0-9-]+\.trycloudflare\.com"', 'r"https://[a-z"')
edit("specs/alt.yml", "pip: []", 'pip: ["pkg{bogus}"]')
edit("specs/alt2.check.py", "import sys", "def (")
rc, out, err = run("--quick", env={"TASKQ_CONFIG": cfgdir})
ok("改坏的配置：退出码 1", rc == 1)
ok("PLACE 写错：说出哪一项、能写什么", "'pueue-host:gpu0'" in err and "能写的" in err)
ok("READY 负数、AUTO 不认识的机器", "READY['site-slurm2']" in err and "nosuch" in err)
ok("pueue 执行机的 worker 组不在 groups 里", "SITES['site-pueue']" in err and "gpuX" in err)
ok("venv 的底子不在 envs 里", "nobase" in err)
ok("hub 的 quick 第二项不是正则", "hub 的 quick 第二项不是正则" in err)
ok("环境描述的 pip 里有不认识的占位、check.py 编译不过（11/4）", "{'bogus'}" not in err and "['bogus']" in err and "环境描述 alt2：SyntaxError" in err)
ok("问题在 stderr、每组的结果在 stdout", "个问题" in out and "错  " not in out)

# 改坏的实验表（在进程里改缓存的实验表）
import check as K  # noqa: E402
import experiments as E  # noqa: E402
exps, final = E.load()
saved = copy.copy(exps), copy.copy(final)
e0 = exps[0]
st0 = dict(e0["after"][0])
st0.pop("done")
st1 = dict(e0["after"][-1], cmd="echo {nosuch}")
exps[0] = dict(e0, after=[st0] + e0["after"][1:-1] + [st1], nice="x")
r = K.run(only=["experiments", "steps"])
msgs = "\n".join(m for _, m in r.problems)
ok("步骤缺 done、nice 类型不对", f"步骤 {st0['name']} 缺 ['done']" in msgs and f"实验 {e0['id']} 的 nice 类型不对" in msgs)
ok("命令里不认识的名字", "{nosuch}" in msgs)
exps[:], final[:] = saved
ok("改回来以后没有问题", not K.run(only=["experiments", "steps"]).problems)

# 冒烟定义（11/5）：函数出错、环境参数没写进 by、产出不在 {smoke} 文件夹下，都按任务报出来
DEF = dict(by=["variant", "env"], params=dict(ds="SMOKE", frag="0", model="smoke/{smoke}"), vars={})
smk = lambda: "\n".join(m for _, m in K.run(only=["tasks"]).problems if "冒烟" in m)  # noqa: E731
try:
    K.SG.SMOKE["toy"] = DEF
    ok("冒烟定义没问题：不报", not smk())
    K.SG.SMOKE["toy"] = dict(DEF, params=lambda tp, data: 1 / 0)
    ok("params() 出错：报冒烟定义出错", "冒烟定义出错" in smk() and "ZeroDivisionError" in smk())
    K.SG.SMOKE["toy"] = dict(DEF, by=["variant"], params=dict(DEF["params"], env="alt"))
    ok("冒烟任务换了环境：报环境不一样", "环境" in smk())
    K.SG.SMOKE["toy"] = dict(DEF, params=dict(ds="SMOKE", frag="0"))
    ok("产出不在 {smoke} 文件夹下：报出来", "{smoke}" in smk())
finally:
    K.SG.SMOKE.pop("toy", None)

print("全部通过" if not fails else f"{fails} 项失败")
sys.exit(1 if fails else 0)
