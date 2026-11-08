#!/usr/bin/env python3
"""Tests for cli_json.py's read-only JSON commands: taskq machines show, taskq exp steps, taskq spec export / import through bin/taskq
with a fixed config (testdata/). Fields = proto MSHOW_*, EXP_STEP*, RELAY_*; values match those computed in-process.

    python3 queue/test_cli_json.py
"""
import json
import os
import shlex
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
BIN = os.path.join(CORE, "bin", "taskq")
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

T = fixture.tmpdir(prefix="test_cli_json_")
HOME, PROJ, DATA = (os.path.join(T, x) for x in ("home", "proj", "data"))
for d in (HOME, PROJ, DATA):
    os.makedirs(d)
with open(os.path.join(PROJ, ".taskq.toml"), "w") as f:
    f.write(f'config = "{fixture.CONFIG}"\nplugins = ["{fixture.PLUGINS}"]\n')
os.environ.update(HOME=HOME, TASKQ_DATA=DATA, TASKQ_REPO=PROJ)  # paths in commands are relative to the project, as bin/taskq finds it
fixture.use()
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(CORE, "sites"))
import cfg  # noqa: E402
import experiments as E  # noqa: E402
import names as N  # noqa: E402
import proto  # noqa: E402
import pueue as PQ  # noqa: E402
import remote as R  # noqa: E402
import spec as S  # noqa: E402
from flow import compile as FC  # noqa: E402
from flow.model import EACH, GATHER, GLOBAL  # noqa: E402

M = cfg.mtab()
ENV = dict({k: v for k, v in os.environ.items() if not k.startswith("TASKQ_")}, HOME=HOME, TASKQ_DATA=DATA,
           PYTHONDONTWRITEBYTECODE="1")
fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def taskq(*args, stdin=None, env=None):
    return subprocess.run([sys.executable, BIN, *args], cwd=PROJ, env=env or ENV, input=stdin, capture_output=True, text=True,
                          timeout=300)


def js(*args, **kw):
    r = taskq(*args, **kw)
    assert r.returncode == 0, (args, r.returncode, r.stderr[-500:])
    assert r.stdout.count("\n") == 1, r.stdout[:300]
    return json.loads(r.stdout)


try:
    # ---------------- taskq machines show ----------------
    d = js("machines", "show", "--json")
    every = set(M.MACHINES)
    ok("machines show: one line, top-level fields = proto.MSHOW_TOP, each machine = MSHOW_MACHINE, each description = MSHOW_SPEC",
       tuple(d) == proto.MSHOW_TOP and d["version"] == proto.VERSION and set(d["machines"]) == every
       and all(tuple(m) == proto.MSHOW_MACHINE for m in d["machines"].values())
       and all(tuple(s) == proto.MSHOW_SPEC for s in d["specs"].values()))
    ok("machines show: hub, roles, executor, paused, kinds, workers, groups, ready envs and activate match the machine table",
       d["hub"] == M.hub() and all(
           m["roles"] == list(M.roles(n)) and m["executor"] == M.is_exec(n) and m["paused"] == M.paused(n)
           and m["kinds"] == list(M.kinds(n)) and m["workers"] == json.loads(json.dumps(M.workers(n)))
           and m["groups"] == json.loads(json.dumps(M.groups(n))) and m["ready_envs"] == sorted(M.ready_envs(n))
           and m["activate"] == {e: M.activate(n, e) for e in M.machine(n)["envs"] or {}} for n, m in d["machines"].items()))
    ms = d["machines"]
    ok("machines show: executors are site-pueue, site-slurm, site-slurm2; hub is not",
       [n for n, m in ms.items() if m["executor"]] == ["site-pueue", "site-slurm", "site-slurm2"] and ms["hub"]["roles"] == ["hub", "store"])
    ok("machines show: pueue executors get one start-worker command per worker (by worker id); Slurm and the hub get none",
       ms["site-pueue"]["worker_cmds"] == {w["id"]: PQ.worker_cmd("site-pueue", w) for w in M.workers("site-pueue")}
       and list(ms["site-pueue"]["worker_cmds"]) == ["site-pueue-gpu1", "site-pueue-gpu0", "site-pueue-cpu"]
       and ms["site-slurm"]["worker_cmds"] == {} and ms["hub"]["worker_cmds"] == {})
    ok("machines show: root in shell form (~ left for the other side to expand)", ms["site-slurm"]["root_q"] == R.q("~/q") and ms["hub"]["root_q"] == R.q("~/demo_root"))
    ok("machines show: statements added to tasks (task_vars)", ms["site-pueue"]["task_sh"] == S.machine_vars(M.machine("site-pueue"))
       and any("DEMO_FAST=1" in x for x in ms["site-pueue"]["task_sh"]) and ms["hub"]["task_sh"] == [])
    specs = sorted(f[:-4] for f in os.listdir(os.path.join(fixture.CONFIG, "specs")) if f.endswith(".yml"))
    ok("machines show: env description hashes and base", sorted(d["specs"]) == specs
       and all(s["hash"] == M.spec_hash(n) for n, s in d["specs"].items()) and d["specs"]["alt"]["base"] == "base")
    d2 = js("machines", "show", "site-pueue", "hub")
    ok("machines show NAME...: only those machines", set(d2["machines"]) == {"site-pueue", "hub"} and d2["machines"]["site-pueue"] == ms["site-pueue"])
    r = taskq("machines", "show", "nope")
    ok("machines show with an unknown machine -> 4 (proto.EXIT gone), reason on stderr", r.returncode == proto.EXIT["gone"] and "nope" in r.stderr and not r.stdout)
    r = taskq("machines", "get", "site-slurm", "root")
    ok("machines other commands unchanged", r.returncode == 0 and r.stdout.strip() == "~/q")

    # ---------------- taskq exp steps ----------------
    exps, final = E.load()
    tasks = E.tasks()
    flow = FC.build()
    inst = [i for i in flow.instances.values() if i.step]
    LV = {EACH: "task", GATHER: "group", GLOBAL: "final"}

    def filled(text, names):
        return S.fill(S.fill(text, names), dict(data=DATA, repo=cfg.REPO), keep=())

    d = js("exp", "steps", "--data", DATA)
    st = d["steps"]
    ok("exp steps: one line, top-level fields = proto.EXP_STEPS_TOP, each step = EXP_STEP_FIELDS, the test experiment table yields instances",
       tuple(d) == proto.EXP_STEPS_TOP and d["version"] == proto.VERSION and d["data"] == DATA and d["errors"] == flow.errors == []
       and all(tuple(s) == proto.EXP_STEP_FIELDS for s in st))
    n_task = sum(len(E.steps_of(t)) for t in tasks.values())
    n_group = sum(len(e["group_after"]) for e in exps)
    lvs = [s["level"] for s in st]
    ok(f"exp steps: every flow step instance is listed, in experiment table order (per task {n_task}, experiment level {n_group}, FINAL {len(final)})",
       [(s["id"], s["level"]) for s in st] == [(i.id, LV[i.scope]) for i in inst]
       and (lvs.count("task"), lvs.count("group"), lvs.count("final")) == (n_task, n_group, len(final)))
    ok("exp steps: each step's experiment, task, step name, names, outputs and declared inputs match the flow instance",
       all((s["exp"], s["key"], s["name"], s["names"], s["out"], s["inputs"])
           == (i.exp or None, i.task or None, i.rule, E.jsonable(i.names), i.outputs, i.inputs) for s, i in zip(st, inst)))
    k0, t0 = next((k, t) for k, t in tasks.items() if E.steps_of(t))
    names0 = E.task_names(t0)
    ok("exp steps: commands filled from names, {data} replaced by --data, no {...} left; done and needs are null when not requested",
       [s["cmd"] for s in st if s["key"] == k0] == [filled(x["cmd"], names0) for x in E.steps_of(t0)]
       and all(s["error"] is None and "{" not in s["cmd"] and s["done"] is None and s["needs"] is None for s in st))
    e0 = next(e for e in exps if any(x["name"] == "ens" for x in e["group_after"]))
    ens = next(s for s in st if s["exp"] == e0["id"] and s["name"] == "ens")
    oof_e = next(e for e in exps if any(x["name"] == "oof" for x in e["group_after"]))
    oof = next(s for s in st if s["exp"] == oof_e["id"] and s["name"] == "oof")
    ok("exp steps: experiment-level commands use <param>_list_q, and {frag} in inputs expands over each value",
       shlex.quote(" ".join(e0["each"]["frag"])) in ens["cmd"] and ens["key"] is None
       and all(any(x.endswith(f"/frag_{f}/check") for x in oof["inputs"]) for f in oof_e["each"]["frag"]))
    fin = [s for s in st if s["level"] == "final"]
    ok("exp steps: FINAL steps belong to no experiment and only have data", [s["name"] for s in fin] == [f["name"] for f in final]
       and all(s["exp"] is None and s["key"] is None and s["names"] == {} for s in fin))
    dk = js("exp", "steps", "--key", k0, "--data", DATA)["steps"]
    ok("exp steps --key: only this task's steps", [s["id"] for s in dk] == [s["id"] for s in st if s["key"] == k0])
    de = js("exp", "steps", "--exp", e0["id"], "--level", "group", "--data", DATA)["steps"]
    ok("exp steps --exp --level group: only this experiment's group-level steps", [s["name"] for s in de] == [x["name"] for x in e0["group_after"]]
       and all(s["exp"] == e0["id"] and s["level"] == "group" for s in de))
    ok("exp steps --exp: FINAL is not listed", not any(s["level"] == "final" for s in js("exp", "steps", "--exp", e0["id"])["steps"]))
    ok("exp steps without --data: use this machine's data dir (TASKQ_DATA)", js("exp", "steps", "--key", k0)["data"] == DATA)

    # --done: count done files under the first output; every entry must be there (only real files)
    dd = js("exp", "steps", "--key", k0, "--data", DATA, "--done")["steps"]
    ok("exp steps --done: nothing is done in an empty data dir", dd and all(s["done"] is False for s in dd))
    s0 = dd[0]
    mark = os.path.join(DATA, s0["out"][0], flow.instances[s0["id"]].step["done"][0].split("|")[0])
    os.makedirs(os.path.dirname(mark), exist_ok=True)
    open(mark, "w").close()
    dd = js("exp", "steps", "--key", k0, "--data", DATA, "--done")["steps"]
    ok(f"exp steps --done: {s0['name']}'s marker is there -> done; the rest are not", [s["done"] for s in dd] == [True] + [False] * (len(dd) - 1))
    oof_i = flow.instances[oof["id"]]
    lv = ["--exp", oof_e["id"], "--level", "group", "--data", DATA, "--done"]
    first = os.path.join(DATA, oof_i.outputs[0], oof_i.step["done"][0])
    os.makedirs(os.path.dirname(first), exist_ok=True)
    open(first, "w").close()
    half = next(s["done"] for s in js("exp", "steps", *lv)["steps"] if s["name"] == "oof")
    open(os.path.join(DATA, oof_i.outputs[0], oof_i.step["done"][1]), "w").close()
    full = next(s["done"] for s in js("exp", "steps", *lv)["steps"] if s["name"] == "oof")
    ok("exp steps --done: with two done entries, one present -> not done; both present -> done", len(oof_i.step["done"]) == 2 and half is False and full is True)
    shutil.rmtree(DATA)
    os.makedirs(DATA)

    # --needs: steps with inputs_cmd really run (every experiment in the test table; just trained, model still on another machine: data dir empty)
    dn = js("exp", "steps", "--data", DATA, "--needs")["steps"]
    has = [s for s in dn if flow.instances[s["id"]].step.get("inputs_cmd")]
    ok(f"exp steps --needs: every experiment step with inputs_cmd ({len(has)}) lists inputs; other needs are null",
       {s["exp"] for s in has} == {e["id"] for e in exps if any(x.get("inputs_cmd") for x in e["after"] + e["group_after"])}
       and all(s["needs"] and s["error"] is None for s in has) and all(s["needs"] is None for s in dn if s not in has))
    ok("exp steps --needs: paths are relative to the data dir and include final.ckpt for this part",
       all(not x.startswith("/") for s in has for x in s["needs"])
       and all(f"{s['out'][0].replace('preds/', 'results/', 1)}/final.ckpt" in s["needs"] for s in has if s["name"] == "infer"))
    ok("exp steps --needs: did not touch the data dir (only the empty data dir itself was made)", os.listdir(DATA) == [])
    r = taskq("exp", "steps", "--exp", "nope")
    ok("exp steps with an unknown experiment -> 4", r.returncode == proto.EXIT["gone"] and "nope" in r.stderr and not r.stdout)
    r = taskq("exp", "steps", "--key", "nope/x/0")
    ok("exp steps with an unknown key -> 4", r.returncode == proto.EXIT["gone"] and "nope/x/0" in r.stderr and not r.stdout)
    r = taskq("exp", "steps", "--level", "nope")
    ok("exp steps with bad args -> 2", r.returncode == 2)

    # ---------------- taskq spec export / import ----------------
    DS, DI = "D777_fake", "PlanM_full"
    CASES = ["s101", "s101_b", "s102", "x-00582-000-syn"]
    ROOT = os.path.join(T, "root")
    PRE = os.path.join(ROOT, "prepared", DS)

    def wj(p, obj):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            json.dump(obj, f)

    def wb(p, data):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as f:
            f.write(data)

    wj(os.path.join(PRE, "dataset.json"), dict(file_ending=".dat", name=DS))
    wj(os.path.join(PRE, "PlanM.json"), dict(configurations={"full": dict(data_identifier=DI)}))
    wj(os.path.join(PRE, "splits_final.json"), [dict(train=CASES[2:], val=CASES[:2]), dict(train=CASES[:2], val=CASES[2:])])
    wj(os.path.join(PRE, "fingerprint.json"), {})
    for c in CASES:
        wb(os.path.join(PRE, "gt", c + ".dat"), b"gt")
        for suf in (".bin", "_lab.bin", ".meta"):
            wb(os.path.join(PRE, DI, c + suf), b"x")
    sp = S.resolve("toy", dict(ds=DS, frag="0"))
    spj = json.dumps(sp)
    ex = js("spec", "export", "--spec", "-", "--root", ROOT, "--json", stdin=spj)
    want = N.export(sp, ROOT)
    ok("spec export: one line, top-level fields = proto.RELAY_EXPORT_TOP, matches the plugin export, logs = part log prefix",
       tuple(ex) == proto.RELAY_EXPORT_TOP and ex["version"] == proto.VERSION and ex["plugin"] == "toy"
       and ex["files"] == [list(f) for f in want["files"]] and ex["views"] == want["views"] and ex["outs"] == want["outs"]
       and ex["cleanup"] == want["cleanup"] and ex["params"] == want["params"] and ex["logs"] == S.outputs(sp)["logs"])
    ok("spec export --spec takes JSON directly", js("spec", "export", "--spec", spj, "--root", ROOT) == ex)

    src = os.path.join(T, "src")
    out = os.path.join(src, sp["out"])
    for c in CASES[:2]:
        wb(os.path.join(out, "check", c + ".dat"), b"p")
    wj(os.path.join(out, "check", "summary.json"), dict(metric_per_case=[{}, {}]))
    wb(os.path.join(out, "final.ckpt"), b"ck")
    wb(os.path.join(src, ex["logs"] + "1_part1.log"), b"log")
    im = js("spec", "import", "--spec", "-", "--src", src, "--dst", os.path.join(T, "dst"), "--root", ROOT, stdin=spj)
    w2 = N.import_(sp, src, os.path.join(T, "dst2"), ROOT)
    ok("spec import: top-level fields = proto.RELAY_IMPORT_TOP, writes match the plugin import and all land in --dst",
       tuple(im) == proto.RELAY_IMPORT_TOP and im["written"] == w2 and ex["logs"] + "1_part1.log" in im["written"]
       and all(os.path.isfile(os.path.join(T, "dst", w)) for w in im["written"]))
    os.remove(os.path.join(out, "check", CASES[1] + ".dat"))
    r = taskq("spec", "import", "--spec", "-", "--src", src, "--dst", os.path.join(T, "dst3"), "--root", ROOT, stdin=spj)
    ok("spec import check fails -> 1, reason on stderr, --dst untouched", r.returncode == 1 and "RelayError" in r.stderr and not r.stdout
       and not os.path.exists(os.path.join(T, "dst3")))
    wj(os.path.join(PRE, "splits_final.json"), [dict(train=CASES[2:], val=CASES[:2] + ["nobody"])])
    r = taskq("spec", "export", "--spec", "-", "--root", ROOT, stdin=spj)
    ok("spec export plugin error -> 1", r.returncode == 1 and "RelayError" in r.stderr and not r.stdout)
    r = taskq("spec", "export", "--spec", '{"x": 1}', "--root", ROOT)
    ok("spec export with a spec that is not resolved -> 2", r.returncode == 2 and "resolve" in r.stderr)
    r = taskq("spec", "export", "--spec", "/no/such.json", "--root", ROOT)
    ok("spec export --spec that is not JSON -> 2, one-line reason, no traceback", r.returncode == 2 and "--spec" in r.stderr and "Traceback" not in r.stderr)
    r = taskq("spec", "import", "--spec", "-", "--root", ROOT, stdin=spj)
    ok("spec import without --src and --dst -> 2", r.returncode == 2)
    ok("--help exits 0", all(taskq(*a, "--help").returncode == 0
                              for a in (("machines", "show"), ("exp", "steps"), ("spec", "export"), ("spec", "import"))))
finally:
    shutil.rmtree(T, ignore_errors=True)

print("all passed" if not fails else f"{fails} failed")
sys.exit(1 if fails else 0)
