#!/usr/bin/env python3
"""Test the experiments.py commands: list, tasks (--json), name-of, env-of, fixed config.

    python3 queue/test_experiments_cli.py
"""
import json
import os
import subprocess
import sys

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
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def run(*a):
    r = subprocess.run([sys.executable, os.path.join(HERE, "experiments.py"), *a], capture_output=True, text=True)
    return r.returncode, r.stdout, r.stderr


rc, out, _ = run("list", "--json")
d = json.loads(out)
ok("list --json: one line, top-level fields = proto.EXP_LIST_TOP", rc == 0 and out.count("\n") == 1 and tuple(d) == proto.EXP_LIST_TOP
   and d["version"] == proto.VERSION)
ok("list --json: experiments in experiment table order", [e["id"] for e in d["experiments"]] == [e["id"] for e in E.load()[0]])
ok("list --json: functions written as names (JSON-ready)", "<function" not in out)
rc, out, _ = run("list")
ok("list: one line per experiment", rc == 0 and len(out.splitlines()) == len(E.load()[0]))

first = E.load()[0][0]["id"]
rc, out, _ = run("tasks", "--exp", first, "--json")
d = json.loads(out)
ok("tasks --json: top-level fields = proto.EXP_TASKS_TOP, each task's fields = proto.EXP_TASK_FIELDS",
   rc == 0 and tuple(d) == proto.EXP_TASKS_TOP and all(tuple(t) == proto.EXP_TASK_FIELDS for t in d["tasks"]))
ok("tasks --exp: only this experiment's tasks", {t["exp"] for t in d["tasks"]} == {first}
   and [t["key"] for t in d["tasks"]] == [k for k, t in E.tasks().items() if t["exp"] == first])
rc, out, _ = run("tasks", "--json")
ok("tasks --json: every task", rc == 0 and len(json.loads(out)["tasks"]) == len(E.tasks()))
rc, out, err = run("tasks", "--exp", "nope")
ok("tasks --exp with an unknown experiment -> 4, reason on stderr", rc == proto.EXIT["gone"] and "nope" in err and not out)

key = next(iter(E.tasks()))
rc, out, _ = run("name-of", key, "pueue")
rc2, out2, _ = run("name-of", key)
ok("name-of unchanged", rc in (0, proto.EXIT["gone"]) and rc2 == 2 and "usage" in (out2 + _))
rc, out, err = run("name-of", "/no/such/model", "pueue")
ok("name-of for a model not in the tables -> 4, reason on stderr", rc == proto.EXIT["gone"] and not out and "/no/such/model" in err)
rc, out, err = run("name-of")
ok("run directly: usage says experiments.py", rc == 2 and "experiments.py name-of" in err and "Traceback" not in err)
rc, out, err = run("env-of", "nope")
ok("env-of unknown -> 4", rc == proto.EXIT["gone"] and not out and "Traceback" not in err)
rc, out, err = run("bogus")
ok("unknown command -> 2", rc == 2)
ok("main(argv) is directly callable", E.main(["list"]) == 0)

# An experiment table adds its plugin dir to sys.path[0] with a same-named recipes.py there;
# check_fields must use the core's copy (after stripping, core sits outside the repo and the first import came after the plugin,
# which broke every hub task).
shadow = fixture.tmpdir()
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
ok("after an experiment table inserts its plugin dir, check_fields still uses the core recipes", r.returncode == 0 and "ok" in r.stdout)

print("all passed" if not fails else f"{fails} failed")
sys.exit(1 if fails else 0)
