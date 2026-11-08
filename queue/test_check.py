#!/usr/bin/env python3
"""Test check.py: the fixed config has no problems; broken copies (machine table, policy, env descriptions), broken plugins and
broken experiment tables (edited in-process) are all reported; raw data is checked against the store's data dir (really runs
site_ops probe).

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
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def run(*a, env=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "check.py"), *a], capture_output=True, text=True, env={**os.environ, **(env or {})})
    return r.returncode, r.stdout, r.stderr


FAST = ("machines", "policy", "plugins", "experiments", "flow", "tasks", "steps")
rc, out, err = run("--quick")
ok("fixed config: exit 0, every --quick group checked, no ERROR", rc == 0 and "ERROR [" not in err and all(g in out for g in FAST))
ok("--quick: does not run inputs, raw, bundle", not any(g in out for g in ("inputs ", "raw ", "bundle ")))
rc, out, err = run("--only", "inputs")
ok("inputs: every step with inputs_cmd really runs once (in an empty data dir) and lists inputs", rc == 0 and "inputs" in out and "ERROR [" not in err)
rc, out, err = run("--only", "policy")
ok("--only: checks just that group", rc == 0 and out.splitlines()[0].startswith("policy") and "machines" not in out)

# broken config copies
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
edit("policy.py", "PAUSE = {\n}", 'PAUSE = {"site-pueue": ("11/8", "broken"), "gone": (None, "x")}\nOFFLINE = {"site-pueue"}')
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
ok("broken config: exit 1", rc == 1)
ok("unknown field: names the machine and field, suggests taskq machines fields",
   "MACHINES['site-slurm']: unknown field group" in err
   and "workers[0] of MACHINES['site-pueue']: unknown field slot" in err and "taskq machines fields" in err)
ok("one GPU written as two workers", "duplicate worker names" in err and "gpu1" in err)
ok("slurm executor writes no workers, job_names must be in STYLES", "MACHINES['site-slurm2']: a slurm executor does not write workers" in err
   and "job_names 'nosuch'" in err)
ok("budget's second item is not below cap", "budget's second item 30 GB is not below cap 20 GB" in err)
ok("at most one executor may have ssh=None (a root-only machine is not an executor)", "at most one executor may have ssh=None (same machine as the hub), now ['loc1', 'loc4']" in err)
ok("an executor's root cannot be inside the store's data dir", "root of MACHINES['loc1'] is inside the store's data dir" in err
   and "root of MACHINES['loc4']" not in err)
ok("scheduler requires root", "MACHINES['loc3']: an executor (scheduler is set) needs root" in err)
ok("root without scheduler: only store and hub may", "MACHINES['loc2']: root is set but scheduler is not" in err
   and "root of MACHINES['hub']" not in err)
ok("venv base not among envs", "nobase" in err)
ok("quick's second item is not a regex", "MACHINES['hub']: quick's second item is not a regex" in err)
ok("unknown pip placeholder in an env description, check.py does not compile", "['bogus']" in err and "env description alt2: SyntaxError" in err)
ok("PLACE: not an executor, not a step or recipe, no executor has such a worker",
   "PLACE['p1'] has ['nosuch'], not executors" in err and "PLACE['nostep'] is not any step" in err
   and "none of PLACE['eval']'s executors ['site-slurm2'] has a cpu worker" in err)
ok("READY negative, not an executor", "READY must be" in err and "READY has ['hub'], not executors" in err)
ok("AUTO holds switches only (the old ledger and machine names are reported)", "AUTO['ledger'] is not a switch" in err and "AUTO['site_pueue'] is not a switch" in err and "PAUSE" in err)
ok("PAUSE: time format, not a machine", "PAUSE must be" in err and "PAUSE has ['gone'], not machines" in err)
ok("a table in policy that is not a field (the old OFFLINE) is reported", "policy.py OFFLINE is not a policy field" in err)
ok("problems on stderr, each group's result on stdout", "problems" in out and "ERROR [" not in out)

# only one error: with no store the rest is still checked and the check itself does not fail
one = os.path.join(fixture.tmpdir(), "config")
shutil.copytree(os.environ["TASKQ_CONFIG"], one, ignore=shutil.ignore_patterns("__pycache__"))
edit("machines.py", 'roles=("hub", "store")', 'roles=("hub",)', d=one)
rc, out, err = run("--only", "machines", env={"TASKQ_CONFIG": one})
ok("no store: reported, the check itself does not fail", rc == 1 and "exactly one machine must have role store, now none" in err and "the check of this group failed" not in err)

# hub and store rules: one config copy per case, only machines
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
ok("hub ssh is not None: reported", rc == 1 and "hub (hub) has ssh None" in err and "the check of this group failed" not in err)
rc, err = machines_err((HUB, 'roles=("hub", "store"), repo="~/auto/demo",'), (TRANSFER, ""))
ok("store without data, root; store without transfer when executors use a tunnel: all reported", rc == 1 and "store (hub) needs data" in err
   and "store (hub) needs root" in err and "store (hub) needs transfer: site-pueue" in err and "the check of this group failed" not in err)
# hub and store separate: the store is on another machine (over ssh), the hub only coordinates
ST = ('\n}\n', '\n    "st": dict(label="ST", hostname=r"st-host", ssh="fake-st", roles=("store",), root="~/st_root",'
      ' data="/data/st", transfer=("https://t.example.org", "10.0.0.1:18090")),\n}\n')
rc, err = machines_err((HUB, 'roles=("hub",), repo="~/auto/demo",'), (TRANSFER, ""), ST)
ok("hub and store separate, no local executor, no locked env: hub needs no root or transfer", rc == 0 and "ERROR [" not in err)
rc, err = machines_err((HUB, 'roles=("hub",), repo="~/auto/demo",'), (TRANSFER, ""), ST,
                       ('envs={"base": ("conda", "w0"),', 'envs={"sp": "spec", "base": ("conda", "w0"),'),
                       ('\n}\n', '\n    "loc": dict(label="L", hostname=r"xl", scheduler="pueue", root="/tmp/locx", workers=[dict(gpu="0")]),\n}\n'))
ok("an executor with ssh=None and the store on another machine: the store must also be here", "loc share the hub's machine (ssh is None); store (st) on another machine" in err)
ok("an executor with a locked env: hub needs root, and transfer when sent over a tunnel",
   "hub (hub) needs root: site-pueue use locked envs" in err and "hub (hub) needs transfer: site-pueue" in err)

# broken labels plugin: wrong value types, PLAN_TITLES() fails; --only plugins reports them
lab = os.path.join(fixture.tmpdir(), "plugins")
os.makedirs(lab)
with open(os.path.join(lab, "labels.py"), "w", encoding="utf-8") as f:
    f.write('STEPS = {"zz": 3}\nQUIET = [1]\n\n\ndef PLAN_TITLES():\n    return 1 / 0\n')
rc, out, err = run("--only", "plugins", env={"TASKQ_PLUGINS": lab})
ok("broken labels plugin: STEPS plain name, QUIET and PLAN_TITLES() errors are reported", rc == 1 and "['zz']" in err and "[1]" in err
   and "ZeroDivisionError" in err)
imp = os.path.join(fixture.tmpdir(), "plugins")
os.makedirs(imp)
with open(os.path.join(imp, "bad_import.py"), "w", encoding="utf-8") as f:
    f.write("import taskq_api\nimport refill\n")
rc, out, err = run("--only", "plugins", env={"TASKQ_PLUGINS": os.environ["TASKQ_PLUGINS"] + os.pathsep + imp})
ok("a plugin imports a core module directly: reported (import taskq_api only)", "bad_import.py imports core ['refill'] directly" in err)

# store operations really run: the store root's (~/demo_root) code/current is a hand-made bundle (fixture.store_bundle), conda's python3 is the one running the test
STORE_ROOT = os.path.join(os.environ["HOME"], "demo_root")
conda = fixture.store_bundle(STORE_ROOT)

# raw data: really run probe on the store; the store's data dir points at a temp dir
data = fixture.tmpdir()
rawc = os.path.join(fixture.tmpdir(), "config")
shutil.copytree(os.environ["TASKQ_CONFIG"], rawc, ignore=shutil.ignore_patterns("__pycache__"))
edit("machines.py", 'data="/data/others/DEMO_DATA", conda=(FAKE_CONDA, "base0")', f'data={data!r}, conda=({conda!r}, "base0")', d=rawc)
for p in ("raw/holdout/A/inputs/a.in", "raw/labels/x/1-lab.dat", "raw/ext/E/labels/1.dat", "demo/probe/p1/inputs/a",
          "demo/probe/p1/targets.csv", "demo/probe/p2/inputs/a", "demo/probe/p2/items/a"):
    os.makedirs(os.path.dirname(os.path.join(data, p)), exist_ok=True)
    open(os.path.join(data, p), "w").close()
rc, out, err = run("--only", "raw", env={"TASKQ_CONFIG": rawc})
miss = [ln for ln in err.splitlines() if ln.startswith("ERROR [raw]")]
ok("raw: files on the store (wildcards included) are not reported; missing ones are reported by path with who needs them",
   rc == 1 and miss and all("splits_final.json" in ln for ln in miss) and "prepared/D101_alpha/splits_final.json" in err
   and "p1/arch_m" in err and "raw/labels" not in err)
for d in ("D101_alpha", "D103_alpha_one", "D111_beta", "D116_gamma"):
    os.makedirs(os.path.join(data, "prepared", d))
    open(os.path.join(data, "prepared", d, "splits_final.json"), "w").close()
rc, out, err = run("--only", "raw", env={"TASKQ_CONFIG": rawc})
ok("raw: all there -> pass", rc == 0 and "ERROR [" not in err)

# a store whose bundle is older than this core (no site_ops.py): a note, not a problem; a store whose site_ops.py raises: a problem
def old_store(script=None):
    root = os.path.join(fixture.tmpdir(), "oldroot")
    conda = fixture.store_bundle(root)
    cur = os.path.join(root, "code", "current")
    old = os.path.join(root, "code", "old0000000000000")
    shutil.copytree(cur, old, symlinks=True)
    os.remove(os.path.join(old, "scripts", "taskq"))
    os.makedirs(os.path.join(old, "scripts", "taskq", "queue"))
    if script is not None:
        with open(os.path.join(old, "scripts", "taskq", "queue", "site_ops.py"), "w", encoding="utf-8") as f:
            f.write(script)
    shutil.rmtree(cur)
    os.symlink(old, cur)
    data = fixture.tmpdir()
    conf = os.path.join(fixture.tmpdir(), "config")
    shutil.copytree(os.environ["TASKQ_CONFIG"], conf, ignore=shutil.ignore_patterns("__pycache__"))
    edit("machines.py", 'root="~/demo_root", data="/data/others/DEMO_DATA", conda=(FAKE_CONDA, "base0")',
         f'root={root!r}, data={data!r}, conda=({conda!r}, "base0")', d=conf)
    plug = os.path.join(fixture.tmpdir(), "plugins")
    shutil.copytree(os.environ["TASKQ_PLUGINS"], plug, ignore=shutil.ignore_patterns("__pycache__"))
    with open(os.path.join(plug, "smoke.py"), "w", encoding="utf-8") as f:
        f.write('SMOKE = {"toy": dict(by=["variant"], params=lambda tp, data: dict(ds="SMOKE", frag="0"))}\n')
    return run("--quick", env={"TASKQ_CONFIG": conf, "TASKQ_PLUGINS": plug})


rc, out, err = old_store()
notes = [ln for ln in err.splitlines() if "older than this core" in ln]
ok("a store bundle older than the core: a note once per check run, no problem, exit 0",
   rc == 0 and "ERROR [" not in err and len(notes) == 1 and "NOTE  [tasks]" in notes[0]
   and "store-side smoke evaluation skipped" in notes[0])
rc, out, err = old_store('def op_smoke(A):\n    raise RuntimeError("store side failed")\n\n\nop_smoke(None)\n')
ok("the same store with a site_ops.py that raises: still a problem",
   rc == 1 and "ERROR [tasks]" in err and "was not checked to the end" in err and "store side failed" in err)

# broken experiment tables (edit cached experiments and order in-process)
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


ok("unchanged: experiments, flow, steps have no problems", not problems("experiments", "flow", "steps"))
e0 = exps[0]
e0["after"][0] = dict(e0["after"][0], device="tpu", out=[])
e0["after"][-1] = dict(e0["after"][-1], cmd="echo {nosuch}")
e0["each"] = dict(frag="0")
msgs = problems("experiments", "steps")
ok("step has the wrong type (device), out is empty, each is not a list", f"step {e0['after'][0]['name']}: device must be gpu or cpu" in msgs
   and "out and done each need at least one entry" in msgs and f"experiment {e0['id']}: each['frag'] must be a non-empty list" in msgs)
restore()
e0 = exps[0]
e0["after"][-1] = dict(e0["after"][-1], cmd="echo {nosuch}")
ok("unknown name in a step command: cannot become a hub queue task", "{nosuch}" in problems("steps"))
restore()

ev = next(st for st in exps[0]["after"] if st["name"] == "eval")
for st in exps[0]["after"]:
    if st["name"] == "lprob":
        st["out"] = [ev["out"][0] + "/sub"]               # output inside another step's output
        st["inputs"] = st["inputs"] + [st["out"][0] + "/x"]  # input inside its own output
msgs = problems("flow")
ok("order: overlapping outputs, an input inside its own output", "is inside output" in msgs and "of lprob/t0_f0" in msgs and "falls in its own output" in msgs)
restore()

oof = next(st for e in exps for st in e["group_after"] if st["name"] == "oof")
pai = next(st for e in exps for st in e["group_after"] if st["name"] == "paired")
oof["inputs"] = oof["inputs"] + [pai["out"][0]]  # oof <- paired <- oof
ok("order has a cycle", "cycle in the order" in problems("flow"))
restore()

for e in exps:
    for st in e["after"]:
        if st["name"] == "eval":
            st["env"] = ["base", "nosuch_env"]
ok("no executor with such a worker has all the envs a step needs", "nosuch_env" in problems("steps"))
restore()
ok("after restoring there are no problems", not problems("experiments", "flow", "steps"))

# smoke definition: function fails, env param not in by, output not under {smoke} -> reported per task
DEF = dict(by=["variant", "env"], params=dict(ds="SMOKE", frag="0", model="smoke/{smoke}"), vars={})
smk = lambda: "\n".join(m for _, m in K.run(only=["tasks"]).problems if "smoke" in m)  # noqa: E731
try:
    K.SG.SMOKE["toy"] = DEF
    ok("smoke definition fine: nothing reported", not smk())
    # params that are functions are computed on the store: really run the store's site_ops smoke; the plugin there (the bundle's .taskq.toml) has the same definition
    bad_smoke = fixture.tmpdir()
    with open(os.path.join(bad_smoke, "smoke.py"), "w", encoding="utf-8") as f:
        f.write('SMOKE = {"toy": dict(by=["variant", "env"], params=lambda tp, data: 1 / 0)}\n')
    fixture.store_bundle(STORE_ROOT, [bad_smoke])
    py, K.SG.RM.py = K.SG.RM.py, lambda name: os.path.join(conda, "bin", "python3")
    try:
        K.SG.SMOKE["toy"] = dict(DEF, params=lambda tp, data: 1 / 0)
        ok("params() fails (computed on the store): smoke definition failed reported", "smoke definition failed" in smk() and "ZeroDivisionError" in smk())
        K.SG.RM.py = lambda name: os.path.join(bad_smoke, "no_python3")
        lines = smk().splitlines()
        ok(f"store-side error (unrelated to the task): reported once per recipe ({lines})", len(lines) == 1 and "was not checked to the end" in lines[0])
    finally:
        K.SG.RM.py = py
        fixture.store_bundle(STORE_ROOT)
    K.SG.SMOKE["toy"] = dict(DEF, by=["variant"], params=dict(DEF["params"], env="alt"))
    ok("smoke task uses another env: env differs reported", "env" in smk())
    K.SG.SMOKE["toy"] = dict(DEF, params=dict(ds="SMOKE", frag="0"))
    ok("output not under a {smoke} dir: reported", "{smoke}" in smk())
finally:
    K.SG.SMOKE.pop("toy", None)

print("all passed" if not fails else f"{fails} failed")
sys.exit(1 if fails else 0)
