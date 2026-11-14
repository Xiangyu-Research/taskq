#!/usr/bin/env python3
"""Tests for advance.py: the testdata experiment table, the store is local (the hub entry), the data dir points to a temp
dir; store operations (done, probe, rm) really run site_ops.py.
  1. one finished source -> queue its first step; previous step done -> next; already queued -> not again; results still
     being fetched do not count as done;
  2. across experiments: paired/t3b waits for ens/t0; FINAL waits for all its upstream;
  3. rerun: an input newer than the done file -> delete the done file (on the store) and redo; upstream not done -> no redo;
  4. temp: every user finished -> delete temp files on the store, leave the rest; a downstream rerun -> do not delete;
  5. HOLD, AUTO["advance"]=False (only write what would be done), bad experiment table, unreachable store: reported right;
  6. adding to the queue really runs taskq.py add (temp state dir); queued tasks carry step, key is the instance id.
    python3 queue/test_advance.py
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
os.environ["HOME"] = fixture.tmpdir()
sys.path.insert(0, HERE)
import advance as A  # noqa: E402
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import proto  # noqa: E402
from flow import compile as FC  # noqa: E402
from flow.model import SOURCE  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


DATA = fixture.tmpdir()
M.MACHINES[M.hub()]["data"] = DATA
HUB = M.hub()
# code/current in the store's root (the hub entry, local) is a hand-built bundle: this core is at proto.BUNDLE_CORE and
# .taskq.toml points to the test config and plugins; python3 in the store's conda is the one running the tests. The data
# dir is passed by store_op (M.data())
CUR = os.path.join(os.environ["HOME"], "demo_root", proto.ROOT["current"])
os.makedirs(os.path.dirname(os.path.join(CUR, proto.BUNDLE_CORE)))
os.symlink(os.path.dirname(HERE), os.path.join(CUR, proto.BUNDLE_CORE))
with open(os.path.join(CUR, ".taskq.toml"), "w", encoding="utf-8") as f:
    f.write(f"config = {json.dumps(os.environ['TASKQ_CONFIG'])}\nplugins = {json.dumps([fixture.PLUGINS])}\n")
CONDA = fixture.tmpdir()
os.makedirs(os.path.join(CONDA, "bin"))
os.symlink(sys.executable, os.path.join(CONDA, "bin", "python3"))
M.MACHINES[HUB]["conda"] = (CONDA, "base0")
INST = FC.build().instances


def touch(rel, text="", age=0):
    p = os.path.join(DATA, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        f.write(text)
    if age:
        t = time.time() - age
        os.utime(p, (t, t))
    return p


def finish(iid, age=0):
    """Put the instance's result on the store (first alternative of each done item)."""
    i = INST[iid]
    it = A.done_items(i)
    for d in it["done"]:
        touch(f"{it['out']}/{d[0] if isinstance(d, list) else d}", age=age)


def snap(queue=(), pueue=(), records=()):
    return {"taskq": {HUB: dict(tasks=list(queue))}, "pueue": {HUB: dict(tasks=list(pueue))}, "records": list(records)}


def adds(todo):
    return sorted(t["id"] for t in todo if t["op"] == "add" and not t["queued"])


def src(exp, frag):
    return next(k for k, i in INST.items() if i.scope == SOURCE and i.exp == exp and i.spec["names"].get("frag") == frag)


t00 = src("t0", "0")

# ---- 1. steps within one task ----
todo, stuck = A.plan(snap())
ok("nothing finished: queue nothing", adds(todo) == [] and not stuck)
finish(t00)
todo, stuck = A.plan(snap())
ok("one source finished: queue only its first step (infer)", adds(todo) == ["infer/t0_f0"] and not stuck)
p = next(t for t in todo if t["id"] == "infer/t0_f0")
ok("parameters: recipe command key, step, cmd ({data} left), out, done, inputs, nice",
   p["params"]["key"] == "infer/t0_f0" and p["params"]["step"] == "infer" and "{data}/results/" in p["params"]["cmd"]
   and p["params"]["out"] == INST["infer/t0_f0"].outputs[0] and p["params"]["done"] == ["MANIFEST.json"]
   and p["nice"] == -100 and p["params"]["relay"] == "toy_infer" and "--stage needs:infer" in p["params"]["inputs_cmd"])
todo, _ = A.plan(snap(queue=[dict(key="infer/t0_f0", state=proto.S_FREE)]))
ok("already in the queue: not queued again", adds(todo) == [])
finish("infer/t0_f0")
todo, _ = A.plan(snap())
ok("infer done -> eval (lprob needs eval's report, not queued yet)", adds(todo) == ["eval/t0_f0"])
rec = dict(machine=HUB, id=7, label="relay_fetch infer/t0_f0", step="relay_fetch", key="infer/t0_f0")
todo, _ = A.plan(snap(pueue=[dict(id=7, label=rec["label"], status="Running")], records=[rec]))
ok("infer results still being fetched into the store: not done, eval not queued, infer not queued again", adds(todo) == [])
todo, _ = A.plan(snap(pueue=[dict(id=7, label=rec["label"], status="Success")], records=[rec]))
ok("fetch finished: queued as usual", adds(todo) == ["eval/t0_f0"])
finish("eval/t0_f0")
todo, _ = A.plan(snap())
ok("eval done -> lprob", adds(todo) == ["lprob/t0_f0"])

# ---- 2. experiment level, across experiments, FINAL ----
for f in "01234":
    for k in (src("t0", f), f"infer/t0_f{f}", f"eval/t0_f{f}", src("t3b", f), f"infer/t3b_f{f}", f"eval/t3b_f{f}"):
        finish(k)
todo, _ = A.plan(snap())
got = adds(todo)
ok("all five parts inferred -> ens; eval done -> oof; paired/t3b waits for ens/t0",
   {"ens/t0", "oof/t3b", "ens/t3b"} <= set(got) and "paired/t3b" not in got and "compare_all" not in got)
finish("oof/t3b")
got = adds(A.plan(snap())[0])
ok("oof/t3b done, ens/t0 not: paired/t3b not queued yet (cross-experiment input)", "paired/t3b" not in got)
finish("ens/t0")
got = adds(A.plan(snap())[0])
ok("ens/t0 done: paired/t3b queued", "paired/t3b" in got and "compare_all" not in got)

# ---- 3. rerun (FINAL compare_all) ----
ca = INST["compare_all"]
for k in A.Graph(FC.build()).ancestors("compare_all"):
    finish(k, age=7200)
finish("compare_all", age=3600)
todo, _ = A.plan(snap(queue=[dict(key="compare_all", state=proto.S_DONE)]))
ok("all compare_all inputs older than it: no redo", not [t for t in todo if t["id"] == "compare_all"])
touch(INST["paired/t3b"].outputs[0] + "/paired.csv")  # written now: newer than compare_all
todo, _ = A.plan(snap(queue=[dict(key="compare_all", state=proto.S_DONE)]))
rr = [t for t in todo if t["id"] == "compare_all"]
ok("input newer than the done file: delete the done file and redo (already in the queue, not added again)",
   [t["op"] for t in rr] == ["rerun", "add"] and rr[0]["files"] == [ca.outputs[0] + "/compare_all.rec.json"] and rr[1]["queued"])
todo, _ = A.plan(snap(queue=[dict(key="compare_all", state=proto.S_CLAIMED)]))
ok("running: no redo", not [t for t in todo if t["id"] == "compare_all"])
calls = []
did, stuck = A.run(snap(queue=[dict(key="compare_all", state=proto.S_DONE)]), tq=lambda n, p: (calls.append(p["key"]) or (0, "")))
ok("run: delete the done file on the store, do not add compare_all to the queue",
   not os.path.exists(os.path.join(DATA, ca.outputs[0], "compare_all.rec.json")) and "compare_all" not in calls and not stuck)
finish("compare_all")
os.remove(os.path.join(DATA, INST["ens/t18"].outputs[0], "report/report.md"))
touch(INST["paired/t3b"].outputs[0] + "/paired.csv")
todo, _ = A.plan(snap(queue=[dict(key="compare_all", state=proto.S_DONE)]))
ok("an upstream is not done (ens/t18): no redo", not [t for t in todo if t["id"] == "compare_all" and t["op"] == "rerun"])
finish("ens/t18")
finish("compare_all")  # now redone (newer than its inputs)

# ---- 4. temp ----
inf = INST["infer/t0_f0"]
users = sorted(A.Graph(FC.build()).users["infer/t0_f0"])
ok("infer downstream: eval, lprob, ens", users == ["ens/t0", "eval/t0_f0", "lprob/t0_f0"])
prob = touch(inf.outputs[0] + "/G-A/a.prob", "x" * 1000)
keep = touch(inf.outputs[0] + "/G-A/a.pred")
ok("a downstream is not done (lprob): not deleted", not [t for t in A.plan(snap())[0] if t["op"] == "temp" and t["id"] == "infer/t0_f0"])
finish("lprob/t0_f0")
todo, _ = A.plan(snap())
tp = [t for t in todo if t["op"] == "temp" and t["id"] == "infer/t0_f0"]
ok("all downstream done: delete infer's temp files", tp and tp[0]["pats"] == [inf.outputs[0] + "/*/*.prob"])
calls = []
A.run(snap(), tq=lambda n, p: (calls.append(p["key"]) or (0, "")))
ok("run: delete *.prob on the store, leave other files (predictions, done)",
   not os.path.exists(prob) and os.path.exists(keep) and os.path.exists(os.path.join(DATA, inf.outputs[0], "MANIFEST.json")))

# ---- 5. HOLD, AUTO, errors ----
os.remove(os.path.join(DATA, INST["eval/t0_f1"].outputs[0], "report/report.md"))
P.HOLD[:] = ["eval/t0_*"]
todo, _ = A.plan(snap())
ok("HOLD matches: not queued, written to the notes", "eval/t0_f1" not in adds(todo) and any("eval/t0_f1" in x and "HOLD" in x for x in A.NOTES))
P.HOLD[:] = []
P.AUTO["advance"] = False
calls = []
did, _ = A.run(snap(), tq=lambda n, p: (calls.append(p["key"]) or (0, "")))
ok("AUTO['advance']=False: add no tasks, only write what would be queued", not calls and "would queue eval/t0_f1" in did)
P.AUTO["advance"] = True
did, _ = A.run(snap(), dry=True, tq=lambda n, p: (calls.append(p["key"]) or (0, "")))
ok("dry: likewise adds nothing", not calls and "would queue eval/t0_f1" in did)
did, stuck = A.run(snap(), tq=lambda n, p: (1, "lock timeout"))
ok("adding to the queue fails: reported", any("eval/t0_f1" in v and "lock timeout" in v for v in stuck.values()))
saved = A.FC.build
A.FC.build = lambda **kw: type("F", (), dict(errors=["output a of x is also b's"], instances={}))()
todo, stuck = A.plan(snap())
A.FC.build = saved
ok("bad experiment table: queue nothing, report advance:flow", not todo and list(stuck) == [proto.alert_id("advance", "flow")])
M.MACHINES[HUB]["data"] = os.path.join(DATA, "nosuch")
todo, stuck = A.plan(snap())
M.MACHINES[HUB]["data"] = DATA
ok("store data dir missing: everything counts as not done (no error, nothing deleted)", not [t for t in todo if t["op"] in ("temp", "rerun")])
hub_root = M.MACHINES[HUB]["root"]
M.MACHINES[HUB].update(ssh="fake-nohost", root="~/q")
todo, stuck = A.plan(snap())
M.MACHINES[HUB].update(ssh=None, root=hub_root)
ok("store unreachable: queue nothing, report advance:store", not todo and list(stuck) == [proto.alert_id("advance", "store")])

# ---- 6. really run taskq.py add ----
rc, out = A.taskq_add(-100, A.queue_params(INST["eval/t0_f1"]))
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json"], capture_output=True, text=True, env=dict(os.environ, TASKQ_DATA=DATA))
rows = (S.json_in(r.stdout) or {}).get("tasks") or []
row = next((t for t in rows if t["key"] == "eval/t0_f1"), None)
ok(f"taskq.py add really runs: eval/t0_f1 is in the queue, recipe command, with step ({rc} {out[-200:]})",
   rc == 0 and row is not None and row["recipe"] == "command" and (row.get("spec") or {}).get("step") == "eval"
   and row.get("nice") == -100)
rc, out = A.taskq_add(-100, A.queue_params(INST["eval/t0_f1"]))
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json"], capture_output=True, text=True, env=dict(os.environ, TASKQ_DATA=DATA))
ok("same key added again: not duplicated", [t["key"] for t in S.json_in(r.stdout)["tasks"]].count("eval/t0_f1") == 1)
sp = S.resolve("command", S.parse_kv(S.kv_args("command", A.queue_params(INST["infer/t0_f0"]))[1:]))
ok("the step's spec becomes shell variables", bool(S.shell_lines(sp, "/d")))

print("all passed" if not fails else f"{fails} failures")
sys.exit(1 if fails else 0)
