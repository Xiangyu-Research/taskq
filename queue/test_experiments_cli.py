#!/usr/bin/env python3
"""Test the experiments.py commands: list, tasks (--json), name-of, env-of, fixed config; tables with errors next to the test table
(fixture.BAD_TABLES) leave out only their bad entries (load, resolve_task, the commands, taskq status).

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
import spec as S  # noqa: E402

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

# Each table loads on its own: tables with errors (fixture.BAD_TABLES) next to the test table leave out only their bad entries
ids, keys, t0 = [e["id"] for e in E.load()[0]], list(E.tasks()), next(t for t in E.tasks().values() if t["exp"] == "t0")
PAT = fixture.exp_tables(fixture.BAD_TABLES)
fixture.read_tables(PAT)
errs = E.errors()
ok(f"bad tables: every problem listed once, the rest loads ({errs})", len(errs) == 5 and "SyntaxError" in errs[0]
   and "unknown fields nicee" in errs[1] and "xfin" in errs[2] and "xg" in errs[3] and "X8/V_save/0" in errs[4]
   and [e["id"] for e in E.load()[0]] == ids + ["xa", "xb", "xc", "xd"] and list(E.tasks())[:len(keys)] == keys
   and [st["name"] for st in E.load()[1]] == ["compare_all"])
ok("bad tables: a queued task of the test table resolves as before (with its experiment's fields)",
   E.resolve_task(t0["recipe"], t0["params"], "t0") == t0["spec"] and t0["spec"]["metrics"])
try:
    E.resolve_task("toy", dict(ds="X5", frag="0"), "xe")
    got = None
except S.SpecError as err:
    got = err
ok(f"bad tables: a queued task whose experiment is not found is an error (it may be left out; its fields would change the spec): {got}",
   got is not None and not isinstance(got, E.NotFound) and "xe" in str(got) and "SyntaxError" in str(got))
fixture.read_tables(None)
ok("whole tables again: no errors", E.errors() == [] and [e["id"] for e in E.load()[0]] == ids)
BAD_ENV = dict(os.environ, TASKQ_CONFIG=fixture.config_reading(PAT))
r = subprocess.run([sys.executable, os.path.join(HERE, "experiments.py"), "list"], capture_output=True, text=True, env=BAD_ENV)
ok("bad tables: list prints what loaded, the entries left out and why on stderr, exit 1",
   r.returncode == 1 and r.stdout.startswith("t0\t") and "\nxd\t" in r.stdout and "SyntaxError" in r.stderr and "nicee" in r.stderr)
r = subprocess.run([sys.executable, os.path.join(HERE, "experiments.py"), "env-of", "/x/X5/V_save__PlanM__full"],
                   capture_output=True, text=True, env=BAD_ENV)
ok("bad tables: env-of a model not found -> 1 (it may be left out), not 4 (not in the tables)", r.returncode == 1 and "left out" in r.stderr)
r = subprocess.run([sys.executable, os.path.join(HERE, "experiments.py"), "env-of", "/x/X2/V_save__PlanM__full"],
                   capture_output=True, text=True, env=BAD_ENV)
ok("bad tables: env-of a model of a fine experiment in that table answers as usual", r.returncode == 0 and r.stdout == "base\n" and not r.stderr)
# taskq status on a queue holding a task of the test table and one of an experiment left out: it reads, only that task is bad
Q, D = fixture.tmpdir(), fixture.tmpdir()
with open(os.path.join(Q, "tasks.jsonl"), "w", encoding="utf-8") as f:
    f.write(json.dumps(dict(order=1, nice=0, key=t0["key"], exp="t0", recipe="toy", params=t0["params"])) + "\n")
    f.write(json.dumps(dict(order=2, nice=0, key="X5/V_save/0", exp="xe", recipe="toy", params=dict(ds="X5", frag="0"))) + "\n")
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json"], capture_output=True, text=True,
                   env=dict(BAD_ENV, TASKQ_DIR=Q, TASKQ_DATA=D, TASKQ_MACHINE="slurm-login"))
rows = {t["key"]: t for t in (S.json_in(r.stdout) or {}).get("tasks") or []}
ok(f"bad tables: taskq status reads; the test table's task as usual, the left-out experiment's task is bad ({r.returncode} {r.stderr[-300:]})",
   r.returncode == 0 and rows.get(t0["key"], {}).get("state") == proto.S_FREE and rows[t0["key"]]["spec"]["metrics"]
   and rows.get("X5/V_save/0", {}).get("state") == proto.S_BAD)

print("all passed" if not fails else f"{fails} failed")
sys.exit(1 if fails else 0)
