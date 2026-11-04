#!/usr/bin/env python3
"""测 experiments.py 的命令（2026-11-04）：list、tasks（--json）、name-of、env-of，固定配置。

    python3 queue/test_experiments_cli.py
"""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
sys.path.insert(0, HERE)
import experiments as E  # noqa: E402
import proto  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def run(*a):
    r = subprocess.run([sys.executable, os.path.join(HERE, "experiments.py"), *a], capture_output=True, text=True)
    return r.returncode, r.stdout, r.stderr


rc, out, _ = run("list", "--json")
d = json.loads(out)
ok("list --json：一行、顶层字段 = proto.EXP_LIST_TOP", rc == 0 and out.count("\n") == 1 and tuple(d) == proto.EXP_LIST_TOP
   and d["version"] == proto.VERSION)
ok("list --json：实验和实验表的顺序一样", [e["id"] for e in d["experiments"]] == [e["id"] for e in E.load()[0]])
ok("list --json：函数写成名字（能 JSON）", "<function" not in out)
rc, out, _ = run("list")
ok("list：每个实验一行", rc == 0 and len(out.splitlines()) == len(E.load()[0]))

first = E.load()[0][0]["id"]
rc, out, _ = run("tasks", "--exp", first, "--json")
d = json.loads(out)
ok("tasks --json：顶层字段 = proto.EXP_TASKS_TOP，每个任务字段 = proto.EXP_TASK_FIELDS",
   rc == 0 and tuple(d) == proto.EXP_TASKS_TOP and all(tuple(t) == proto.EXP_TASK_FIELDS for t in d["tasks"]))
ok("tasks --exp：只有这个实验的任务", {t["exp"] for t in d["tasks"]} == {first}
   and [t["key"] for t in d["tasks"]] == [k for k, t in E.tasks().items() if t["exp"] == first])
rc, out, _ = run("tasks", "--json")
ok("tasks --json：全部任务", rc == 0 and len(json.loads(out)["tasks"]) == len(E.tasks()))
rc, out, err = run("tasks", "--exp", "nope")
ok("tasks --exp 不认识的实验 → 1，原因在 stderr", rc == 1 and "nope" in err and not out)

key = next(iter(E.tasks()))
rc, out, _ = run("name-of", key, "pueue")
rc2, out2, _ = run("name-of", key)
ok("name-of 照旧", rc in (0, 1) and rc2 == 2 and "用法" in (out2 + _))
rc, out, err = run("env-of", "nope")
ok("env-of 不认识的 → 1", rc == 1)
rc, out, err = run("bogus")
ok("不认识的命令 → 2", rc == 2)
ok("main(argv) 能直接调用", E.main(["list"]) == 0)

# 实验表会把自己的插件文件夹插到 sys.path[0]（research/RQ1/experiments.py），里面有同名的 recipes.py；
# check_fields 必须用核心的那份（11/4：剥离后 core 在仓库外，第一次 import 晚于插件，box 任务全挂）
shadow = tempfile.mkdtemp()
with open(os.path.join(shadow, "recipes.py"), "w", encoding="utf-8") as f:
    f.write("RECIPES = {'shadow': {}}\nHOOKS = {}\n")
code = f"""
import sys, types
sys.path.insert(0, {HERE!r})
import experiments
sys.path.insert(0, {shadow!r})
experiments.check_fields('x', types.SimpleNamespace(EXPERIMENTS=[], FINAL=[]))
print('ok')
"""
r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=os.environ.copy())
ok("实验表插进插件文件夹后，check_fields 仍用核心的 recipes", r.returncode == 0 and "ok" in r.stdout)

print("全部通过" if not fails else f"{fails} 项失败")
sys.exit(1 if fails else 0)
