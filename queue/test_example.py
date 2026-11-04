#!/usr/bin/env python3
"""示例项目（example/）的 README 里那几条命令照样能用（2026-11-04）：临时 HOME 里跑，不连别的机器。
  python3 queue/test_example.py"""
import os
import shutil
import subprocess
import sys
import tempfile

CORE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXAMPLE = os.path.join(CORE, "example")
HOME = tempfile.mkdtemp(prefix="tq_example_")
ENV = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8", "HOME": HOME}
fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def tq(*args):
    r = subprocess.run([sys.executable, os.path.join(CORE, "bin", "taskq"), "--project", EXAMPLE, *args], cwd=CORE, env=ENV,
                       capture_output=True, text=True, timeout=300)
    if r.returncode:
        print(f"  taskq {' '.join(args)} → {r.returncode}\n{r.stdout}{r.stderr}")
    return r


try:
    os.makedirs(os.path.join(HOME, "taskq-example", "data"))
    r = tq("check", "--quick")
    ok("check --quick：没有问题", r.returncode == 0 and "没有问题" in r.stdout)
    r = tq("exp", "tasks")
    ok("exp tasks：两个实验展开成 5 个任务", r.returncode == 0 and len(r.stdout.split("\n")) - 1 == 5)
    r = tq("add", "--exp", "small")
    ok("add --exp small：加了 2 个", r.returncode == 0 and r.stdout.count("加入：") == 2)
    ok("队列在示例自己的状态文件夹里", os.path.isfile(os.path.join(HOME, ".cache", "taskq-example", "taskq", "tasks.jsonl")))
    r = tq("status")
    ok("status：2 个任务", r.returncode == 0 and "任务 2 个" in r.stdout and "count/small/1" in r.stdout)
    r = tq("show", "count/small/0")
    ok("show：命令按做法填好", r.returncode == 0 and "bash bin/count.sh 100 {data}/results/small/seed_0" in r.stdout)
    r = tq("plan")
    ok("plan：执行机连不上，什么都不动", r.returncode == 0 and "连不上" in r.stdout and "没有要排的" in r.stdout)
    r = tq("rm", "count/small/0", "count/small/1")
    ok("rm：删掉", r.returncode == 0 and "任务 0 个" in tq("status").stdout)
    data = os.path.join(HOME, "taskq-example", "data", "results", "x")
    r = subprocess.run(["bash", "bin/count.sh", "25", data], cwd=EXAMPLE, capture_output=True, text=True)
    ok("count.sh：数完写 DONE、进度", r.returncode == 0 and os.path.isfile(os.path.join(data, "DONE"))
       and open(os.path.join(data, "PROGRESS.json")).read().strip() == '{"done": 25, "total": 25}')
finally:
    shutil.rmtree(HOME, ignore_errors=True)
print("\n全部通过" if not fails else f"\n{fails} 项失败")
sys.exit(1 if fails else 0)
