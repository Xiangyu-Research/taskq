#!/usr/bin/env python3
"""The commands in the example project's README work: run in a temp HOME, no other machines.
  python3 queue/test_example.py
"""
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
    print(("PASS:" if cond else "FAIL:") + what)
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
    ok("check --quick: no problems", r.returncode == 0 and "no problems" in r.stdout)
    r = tq("exp", "tasks")
    ok("exp tasks: two experiments expand to 5 tasks", r.returncode == 0 and len(r.stdout.split("\n")) - 1 == 5)
    r = tq("add", "--exp", "small")
    ok("add --exp small: 2 added", r.returncode == 0 and r.stdout.count("added:") == 2)
    ok("the queue lives in the example's own state dir", os.path.isfile(os.path.join(HOME, ".local", "state", "taskq-example", "taskq", "tasks.jsonl")))
    r = tq("status", "--queue")
    ok("status --queue: 2 tasks", r.returncode == 0 and "2 tasks" in r.stdout and "count/small/1" in r.stdout)
    r = tq("status")
    ok("status (the hub's summary): machines and progress per step", r.returncode == 0 and "Machines" in r.stdout and "Progress (done / all)" in r.stdout)
    r = tq("show", "count/small/0")
    ok("show: the command is filled from the recipe", r.returncode == 0 and "bash bin/count.sh 100 {data}/results/small/seed_0" in r.stdout)
    r = tq("plan")
    ok("plan: executors unreachable, no bundle on the store yet (cannot check results), nothing changes",
       r.returncode == 0 and "unreachable" in r.stdout and "cannot check results" in r.stdout and "not queueing this round" in r.stdout)
    r = tq("rm", "count/small/0", "count/small/1")
    ok("rm: removed", r.returncode == 0 and "0 tasks" in tq("status", "--queue").stdout)
    data = os.path.join(HOME, "taskq-example", "data", "results", "x")
    r = subprocess.run(["bash", "bin/count.sh", "25", data], cwd=EXAMPLE, capture_output=True, text=True)
    ok("count.sh: writes DONE and progress when finished", r.returncode == 0 and os.path.isfile(os.path.join(data, "DONE"))
       and open(os.path.join(data, "PROGRESS.json")).read().strip() == '{"done": 25, "total": 25}')
finally:
    shutil.rmtree(HOME, ignore_errors=True)
print("\nall pass" if not fails else f"\n{fails} failed")
sys.exit(1 if fails else 0)
