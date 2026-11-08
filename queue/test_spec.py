#!/usr/bin/env python3
"""Test spec resolving (config and experiment tables from testdata/): queue/spec.py, recipes.py, experiments.py. No machine connection.

    python3 queue/test_spec.py
"""
import itertools
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
import experiments as E  # noqa: E402
import spec as S  # noqa: E402
import proto  # noqa: E402
import cfg  # noqa: E402
QR = cfg.rel("queue")  # queue/ under the repo root (scripts/taskq/queue when embedded, queue in a standalone checkout)

fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def raises(fn):
    try:
        fn()
    except S.SpecError:
        return True
    return False


# ---------------- recipe toy (a plugin recipe) ----------------
s = S.resolve("toy", {"ds": "D111_beta", "variant": "V_save_n470", "frag": "1"})
ok("toy: key, out and env defaults", s["key"] == "D111_beta/V_save_n470/1"
   and s["out"] == "results/D111_beta/V_save_n470__PlanM__full/frag_1"
   and s["env"] == "base" and s["inputs"] == ["prepared/D111_beta"])
ok("toy: command filled from params", s["cmd"] == f"bash {QR}/testdata/bin/toy_train.sh D111_beta full 1 PlanM V_save_n470")
ok("toy: {data}, {repo} left for run time", "{data}" in s["vars"]["DEMO_RESULTS"] and "{repo}" in s["vars"]["DEMO_CODE"])
ok("missing required param (frag): error", raises(lambda: S.resolve("toy", {"ds": "D101_alpha"})))
ok("misspelled param name: error", raises(lambda: S.resolve("toy", {"ds": "D101_alpha", "frag": "0", "variantt": "x"})))
ok("no such recipe: error", raises(lambda: S.resolve("nope", {})))
s2 = S.resolve("toy", {"ds": "D101_alpha", "frag": "0", "env": "alt2", "vars": {"SPLITS_DIR": "x"}})
ok("a param overrides a same-named field: env replaced, vars merged", s2["env"] == "alt2" and s2["vars"]["SPLITS_DIR"] == "x" and "DEMO_RESULTS" in s2["vars"])
ok("a plan param goes into the model dir", S.resolve("toy", {"ds": "D101_alpha", "frag": "0", "plan": "PlanP"})["out"]
   == "results/D101_alpha/V_save__PlanP__full/frag_0")

# ---------------- command recipe ----------------
c = S.resolve("command", {"key": "misc/a", "cmd": "python x.py --out {data}/misc/a", "env": "base", "out": "misc/a",
                          "done": ["model.pt"], "inputs": ["other/stuff"], "extra": "1"})
ok("command: task supplies key, cmd, out and done and may carry other params", c["key"] == "misc/a" and c["done"] == ["model.pt"]
   and c["inputs"] == ["other/stuff"] and "{data}" in c["cmd"] and c["archive"] is False)
ok("command: missing cmd is an error", raises(lambda: S.resolve("command", {"key": "a", "env": "x", "out": "a", "done": "x"})))

# ---------------- common spec fields ----------------
G = dict(key="guest/x", cmd="python3 t.py", env="base", out="guest/x", done="DONE")
g = S.resolve("command", dict(G, inputs="guest/in", res={"gpus": "1", "mem": "16G"}, resume="ckpt_*.pt", owner="bob",
                              progress="progress_json"))
ok("spec: inputs (string becomes a list), res converted to numbers, resume becomes a list, owner", g["inputs"] == ["guest/in"]
   and g["res"] == {"gpus": 1, "mem": "16G"} and g["resume"] == ["ckpt_*.pt"] and g["owner"] == "bob")
g0 = S.resolve("command", G)
ok("spec: command defaults to inputs in inputs (relay=path), needs a GPU, empty res, no checkpoint, no archive, any machine",
   g0["relay"] == "path" and g0["device"] == "gpu" and g0["res"] == {} and g0["resume"] is None and g0["archive"] is False
   and g0["machines"] is None and g0["inputs"] == [])
ok("res.gpus > 1 -> error (a worker gives one GPU)", raises(lambda: S.resolve("command", dict(G, res={"gpus": 2}))))
ok("fits: unstated items are not checked; CPU, memory or GPU memory over the limit -> not claimed", S.fits({"cpus": 64, "mem": "200G", "gpu_mem": 90})[0]
   and not S.fits({"cpus": 64}, cpus=16)[0] and not S.fits({"mem": "200G"}, mem_mb_=64 * 1024)[0]
   and not S.fits({"gpu_mem": 40}, gpu_mem=32)[0] and S.fits({"cpus": 8, "mem": "32G", "gpu_mem": 20}, 16, 64 * 1024, 80)[0]
   and S.mem_mb("2T") == 2 * 1024 ** 2)
ok("spec: bad res field, not a number, bad mem format -> error", raises(lambda: S.resolve("command", dict(G, res={"gpu": 1})))
   and raises(lambda: S.resolve("command", dict(G, res={"cpus": "many"})))
   and raises(lambda: S.resolve("command", dict(G, res={"mem": "lots"}))))
w = S.resolve("command", dict(G, res={"wait_gpu": "120"}))
ok("spec: res.wait_gpu becomes minutes, shell gets TASK_WAIT_GPU; empty when absent, negative is an error",
   w["res"] == {"wait_gpu": 120} and "TASK_WAIT_GPU=120" in S.shell_lines(w, "/d").splitlines()
   and "TASK_WAIT_GPU=" in S.shell_lines(S.resolve("command", G), "/d").splitlines()
   and raises(lambda: S.resolve("command", dict(G, res={"wait_gpu": -1}))))
ok("spec: .. in inputs -> error", raises(lambda: S.resolve("command", dict(G, inputs=["../etc"]))))
ok("params named data or repo (expanded at run time) -> error", raises(lambda: S.resolve("command", dict(G, data=["x"])))
   and raises(lambda: S.resolve("command", dict(G, repo="r"))))
ok("spec: unknown progress -> error", raises(lambda: S.resolve("command", dict(G, progress="tqdm"))))
ok("toy: also resume, one GPU, inputs copied locally, progress read by the plugin's reader", s["resume"]
   and s["res"] == {"gpus": 1, "local_inputs": 1} and s["progress"] == "toy_log" and s["relay"] == "toy")
sl = S.shell_lines(s, "/d").splitlines()
ok("shell: TASK_INPUTS holds absolute input paths (array), TASK_LOCAL follows res.local_inputs", f"TASK_INPUTS=(/d/prepared/{s['params']['ds'] if 'params' in s else s['inputs'][0].split('/')[-1]})" in sl
   and "TASK_LOCAL=1" in sl and "TASK_LOCAL=0" in S.shell_lines(S.resolve("command", G), "/d").splitlines())
raw = dict(S.resolve("command", G), params=dict(G, note="{model}_frag{frag}_"))
ok("shell: {names} left as-is in params are not expanded and do not error", "TASK_LOCAL=0" in S.shell_lines(raw, "/d").splitlines())
gr = fixture.tmpdir()
gs = {k: g.get(k) for k in S.COLLECT_FIELDS}
gd = os.path.join(gr, "guest/x")
os.makedirs(gd)
open(os.path.join(gd, "train.log"), "w").write("starting\n")
f0 = S.collect([gs], gr)["tasks"]["guest/x"]
ok("collect: no PROGRESS.json, no checkpoint -> no progress", not f0["resumable"] and all(r["epochs_done"] == 0 for r in f0["runs"]))
open(os.path.join(gd, "PROGRESS.json"), "w").write('{"done": 3, "total": 10}')
f1 = S.collect([gs], gr)["tasks"]["guest/x"]
ok("collect: PROGRESS.json -> 3 of 10 epochs done", max(r["epochs_done"] for r in f1["runs"]) == 3
   and any(r.get("total") == 10 for r in f1["runs"]))
open(os.path.join(gd, "PROGRESS.json"), "w").write("{broken")
ok("collect: broken PROGRESS.json -> no error, no progress", max(r["epochs_done"] for r in S.collect([gs], gr)["tasks"]["guest/x"]["runs"]) == 0)
open(os.path.join(gd, "ckpt_0003.pt"), "w").close()
ok("collect: a checkpoint in resume -> resumable", S.collect([gs], gr)["tasks"]["guest/x"]["resumable"])
# Storage hangs: one task's file cannot be read -> that task goes to stuck and is skipped; others read normally; total wait is bounded
import threading  # noqa: E402
hang = threading.Event()
S.PROGRESS["hang"] = lambda p: hang.wait(30) or {}
hs = dict(gs, key="guest/hang", out="guest/hang", progress="hang")
os.makedirs(os.path.join(gr, "guest/hang"), exist_ok=True)
open(os.path.join(gr, "guest/hang", "train.log"), "w").write("x\n")
t0 = time.time()
rh = S.collect([hs, gs], gr, wait=0.5)
ok("collect: one task cannot be read -> stuck, not waited for, other tasks fine", rh.get("stuck") == ["guest/hang"] and "guest/x" in rh["tasks"]
   and "guest/hang" not in rh["tasks"] and time.time() - t0 < 5)
ok("collect: all readable -> no stuck", "stuck" not in S.collect([gs], gr, wait=5))
hang.set()
del S.PROGRESS["hang"]
# Read error: recorded in errors (with reason), not stuck; without wait the whole collect does not fail either
S.INFO["boom"] = lambda fd: 1 / 0
bs = dict(gs, key="guest/boom", out="guest/x", info="boom")
for w in (None, 5):
    rb = S.collect([bs, gs], gr, wait=w)
    ok(f"collect (wait={w}): one task fails to read -> errors with reason, not stuck, others fine",
       "ZeroDivisionError" in rb.get("errors", {}).get("guest/boom", "") and "stuck" not in rb and "guest/x" in rb["tasks"]
       and "guest/boom" not in rb["tasks"])
del S.INFO["boom"]
# spec.py command-line errors: a bad spec -> one line, exit 2; done cannot read the data dir -> exit 5
SP = [sys.executable, os.path.join(HERE, "spec.py")]
bad = dict(S.sendable(S.resolve("command", G)), cmd="run {nope}")
r = subprocess.run(SP + ["shell", "--spec", __import__("json").dumps(bad)], capture_output=True, text=True)
ok("spec.py shell: unknown {name} in the spec -> one line, exit 2", r.returncode == 2 and "nope" in r.stderr and "Traceback" not in r.stderr)
r = subprocess.run(SP + ["shell"], capture_output=True, text=True)
ok("spec.py shell without args -> exit 2, no traceback", r.returncode == 2 and "Traceback" not in r.stderr)
gone = os.path.join(gr, "not_mounted")
r = subprocess.run(SP + ["done", "--spec", __import__("json").dumps(S.sendable(S.resolve("command", G)))], capture_output=True, text=True,
                   env=dict(os.environ, TASKQ_DATA=gone))
ok("spec.py done: data dir unreadable -> one line, exit 5 (check again later)", r.returncode == 5 and "Traceback" not in r.stderr and r.stderr.strip())
import refill as RF  # noqa: E402
ok("refill.site_progress: a checkpoint alone (no epochs) counts as progress", RF.site_progress(dict(final=False, runs=[dict(epochs_done=0)], resumable=True))
   and not RF.site_progress(dict(final=False, runs=[dict(epochs_done=0)], resumable=False)))
src = {"src": {"pueue-host": {"tasks": {"guest/x": dict(final=False, runs=[dict(epochs_done=0, end=None)], resumable=True)}},
               "slurm-login": {"tasks": {}}}}
ok("refill.progress: a checkpoint alone counts as progress, per machine", RF.progress(src, "guest/x", "pueue-host") and not RF.progress(src, "guest/x", "slurm-login"))

# ---------------- done check ----------------
root = fixture.tmpdir()
out = os.path.join(root, s["out"])
os.makedirs(os.path.join(out, "check"))
ok("nothing there: not done", not S.is_done(s, root))
open(os.path.join(out, "check", "summary.json"), "w").write("{}")
ok("only summary.json: not done", not S.is_done(s, root))
open(os.path.join(out, "final.ckpt"), "w").close()
ok("summary.json + final.ckpt: done", S.is_done(s, root))
def oserr(fn):
    try:
        fn()
    except OSError:
        return True
    return False


ok("data dir missing (disk not mounted): error, not treated as not done", oserr(lambda: S.is_done(s, os.path.join(root, "nope"))))
real_stat = os.stat


def broken_stat(p, *a, **k):
    if str(p).startswith(out):
        raise OSError(70, "Communication error on send")
    return real_stat(p, *a, **k)


os.stat = broken_stat
ok("result dir unreadable (BeeGFS down, errno 70): error", oserr(lambda: S.is_done(s, root)))
os.stat = real_stat

# ---------------- command-line args round-trip ----------------
args = S.kv_args("command", {"key": "a", "done": ["x", "y|z"]})
ok("kv_args -> parse_kv round-trips (lists as JSON)", S.parse_kv(args[1:]) == {"key": "a", "done": ["x", "y|z"]})

# ---------------- outputs (out, done, fetch, archive are interpreted only here) ----------------
o = S.outputs(dict(key="a/b/0", out="/res/a//b/", done="x|y", fetch=dict(also=["pred/a/"], exclude=["big.pth"], extra=["*.json"]),
                   archive=dict(drop=["*"])))
ok("outputs: out and also normalised to relative paths, string done becomes one item, | splits alternatives, archive is a switch, part log prefix",
   o == dict(outs=["res/a/b", "pred/a"], done=[["x", "y"]], exclude=["big.pth"], extra=["*.json"], archive=True,
             logs="logs/tasks/a__b__0__"))
o2 = S.outputs(dict(out="r", done=[], archive=None))
ok("outputs: no fetch, empty done, no archive, no key", o2 == dict(outs=["r"], done=[], exclude=[], extra=[], archive=False, logs=None))
o3 = S.outputs(dict(out="r", done=["a", "b|c"]))
ok("done_with: every item required, one alternative in an item suffices", S.done_with(o3, {"a", "c"}.__contains__) and not S.done_with(o3, {"b", "c"}.__contains__))
ok("done_missing: lists missing items (original form)", S.done_missing(o3, {"a"}.__contains__) == ["b|c"])
ok("done_with: empty done -> caller's empty", S.done_with(o2, lambda a: False) and not S.done_with(o2, lambda a: False, empty=False))
_os = dict(S.sendable(S.resolve("command", dict(G, archive=True, fetch=dict(extra=["*.json"])))))
_r = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "outputs", "--spec", json.dumps(_os), "--sh"], capture_output=True, text=True)
_sh = _r.stdout.splitlines()
ok("spec.py outputs --spec <JSON> --sh: KEY, OUTS, EXTRA, TEST, ARCHIVE for archive_task.sh",
   _r.returncode == 0 and "KEY=guest/x" in _sh and "OUTS=(guest/x)" in _sh and "EXTRA=('*.json')" in _sh
   and "ARCHIVE=1" in _sh and any(x.startswith("TEST=") and "DONE" in x for x in _sh))
_r = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "outputs", "--spec", json.dumps(_os)], capture_output=True, text=True)
ok("spec.py outputs --spec <JSON>: JSON same as outputs()", _r.returncode == 0 and json.loads(_r.stdout) == S.outputs(_os))


# ---------------- experiment table ----------------
ts = E.tasks()
exps = E.load()[0]
ok(f"experiment table expands to {len(ts)} tasks (= sum of each combinations), keys unique",
   len(ts) == sum(len(list(itertools.product(*(e.get("each") or {}).values()))) for e in exps) and len(set(ts)) == len(ts))
_rt = E.resolve_task
E.resolve_task = lambda recipe, params, exp=None: dict(key="/".join(f"{k}={v}" for k, v in sorted(params.items())),  # noqa: E731
                                                     names={k: str(v) for k, v in params.items()})
_ex = dict(id="gx", exp="GX", recipe="command", params=dict(p="1"), each=dict(seed=["1", "2"], frag=["0", "1", "2"]))
_t = E.expand(_ex)
ok("each: one task per combination (2 x 3), params hold the values, each records them",
   len(_t) == 6 and _t[0]["params"] == dict(p="1", seed="1", frag="0") and _t[-1]["each"] == dict(seed="2", frag="2"))
ok("each: only expands the given values; a param in only not in each -> error",
   [t["each"] for t in E.expand(_ex, dict(frag=["2"]))] == [dict(seed="1", frag="2"), dict(seed="2", frag="2")]
   and raises(lambda: E.expand(_ex, dict(lr=["1"]))))
ok("each: label = id + _<first letter><value> per param; no each -> id only",
   E.label(_ex, _t[1]) == "gx_s1_f1" and E.label(dict(id="gy"), E.expand(dict(id="gy", exp="GY", recipe="command", params={}))[0]) == "gy")
_g = E.group_names(_ex)
ok("each: experiment-wide names have <param>_list, <param>_list_q",
   _g["frag_list"] == "0 1 2" and _g["frag_list_q"] == "'0 1 2'" and _g["seed_list"] == "1 2" and "frag" not in _g and _g["p"] == "1")
E.resolve_task = _rt
_lab = {E.label(E.experiment(t["exp"]), t) for t in ts.values()}
ok("experiment table labels = <id>_f<part>",
   _lab == {f"{t['exp']}_f{t['params']['frag']}" if "frag" in t["params"] else t["exp"] for t in ts.values()})
ok("every task resolves and has env and output", all(t["spec"].get("env") and t["spec"].get("out") for t in ts.values()))
um = ts["D101_alpha/V_m/1"]["spec"]
sw = ts["D101_alpha/V_s/0"]["spec"]
ok("arch_m and arch_s state their own env", um["env"] == "alt2" and sw["env"] == "alt")
ok("toy task key = ds/variant/frag, out is results/<model>/frag_<part>",
   all(t["spec"]["out"] == "results/{0}/{1}__{p}__full/frag_{2}".format(*k.split("/"), p=t["params"].get("plan", "PlanM"))
       for k, t in ts.items() if t["recipe"] == "toy"))
ok("experiment metrics enter the spec (read by the ledger)", set(um.get("metrics", {})) == {"val_A", "val_B"})
ok("env_of: find a model's env from the experiment table",
   E.env_of("/x/D101_alpha/V_s__PlanM__full") == "alt")
try:
    E.env_of("/x/D999_x/foo")
    ok("env_of: a model not in the tables is an error (does not default to base)", False)
except S.SpecError as err:
    ok("env_of: a model not in the tables is an error (does not default to base), suggests MODEL_ENV", "MODEL_ENV" in str(err))
r = subprocess.run([sys.executable, os.path.join(HERE, "experiments.py"), "env-of", "/x/D999_x/foo"], capture_output=True, text=True)
ok("experiments.py env-of: not in the tables -> non-zero exit, message on stderr (predict.sh stops)", r.returncode != 0 and "MODEL_ENV" in r.stderr and not r.stdout)


# ---------------- experiment declarations: other scripts look them up by model dir, never guess by name ----------------
M = "/x/{}/V_save__PlanM__full"
ok("names_of: t24cov skips the four external sets G and uses t24_splits.json",
   E.names_of(M.format("D155_gamma_ecov"), "skip_ext", "splits") == ["G-A G-B G-C G-D", "t24_splits.json"])
ok("names_of: t22a only tests held-out G-A", E.names_of(M.format("D151_gamma_loA"), "only_ext") == ["G-A"])
ok("names_of: the t0 model skips nothing and has no split", E.names_of(M.format("D101_alpha"), "skip_ext", "only_ext", "splits") == ["", "", ""])
ok("names_of: a model not in the tables is an error (no guessing)", raises(lambda: E.names_of("/x/D999_x/foo", "skip_ext")))
ok("every experiment has a nice and board names", all(isinstance(e.get("nice"), int) and set(e.get("board") or {}) == {"group", "en", "zh"}
                                for e in exps if e["rq"] == "demo"))
ok("every experiment with an oof step has a split file", all(e["names"]["splits"] for e in exps if e["rq"] == "demo"
                                     and any(s["name"] == "oof" for s in e["group_after"])))
import importlib.util  # noqa: E402
spec_ = importlib.util.spec_from_file_location("demo_exps", os.path.join(fixture.EXPS, "demo", "experiments.py"))
R = importlib.util.module_from_spec(spec_)
spec_.loader.exec_module(R)
x = R._x("toy", "T99", "Toy", "base", "D999_x", ("G", "en", "zh"), 0, steps=R.OOF5 + ["ens"] + R.PROBE, splits="s.json", probe="p")


# field checks on experiment tables: unknown fields are errors naming the experiment, field and allowed ones
class _Mod:
    def __init__(self, exps, final=()):
        self.EXPERIMENTS, self.FINAL = exps, list(final)


def _field_err(mod):
    try:
        E.check_fields("x/experiments.py", mod)
    except S.SpecError as err:
        return str(err)
    return None


_e = R._x("toy", "T99", "Toy", "base", "D999_x", ("G", "en", "zh"), 0)
ok("field check: demo table experiments are known", _field_err(_Mod(R.EXPERIMENTS, R.FINAL)) is None and _field_err(_Mod([_e])) is None)
_bad = dict(_e, nicee=1)
_m = _field_err(_Mod([_bad]))
ok("field check: misspelled field in an experiment -> error naming file, experiment, field, allowed", bool(_m) and "x/experiments.py" in _m and "toy" in _m
   and "nicee" in _m and "group_after" in _m)
_m = _field_err(_Mod([dict(_e, after=[dict(R.INFER, out_bg=75)])]))
ok("field check: misspelled out_bg in a step -> error naming step, allowed (out_gb)",
   bool(_m) and "out_bg" in _m and "infer" in _m and "out_gb" in _m)
_m = _field_err(_Mod([dict(_e, group_after=[[R.OOF, R.PAIRED]])]))
ok("field check: a list of lists in group_after -> error (order follows inputs)", bool(_m) and "group_after" in _m)
_m = _field_err(_Mod([dict(_e, group_after=[dict(R.OOF, only=dict(frag=["0"]))])]))
ok("field check: only on an experiment-wide step -> error", bool(_m) and "only" in _m)
_m = _field_err(_Mod([dict(_e, after=[dict(R.INFER, only=dict(seed=["0"]))])]))
ok("field check: a param in only not in each -> error", bool(_m) and "only" in _m and "frag" in _m)
_m = _field_err(_Mod([dict(_e, after=[dict(R.EVAL, device="tpu")])]))
ok("field check: device not gpu / cpu -> error", bool(_m) and "device" in _m)
_m = _field_err(_Mod([dict(_e, after=[{k: v for k, v in R.INFER.items() if k != "done"}])]))
ok("field check: missing required done in a step -> error", bool(_m) and "done" in _m)
_m = _field_err(_Mod([_e], [dict(R.FINAL[0], rerun_if_newr=["x"])]))
ok("field check: misspelled field in a FINAL step -> error", bool(_m) and "rerun_if_newr" in _m)
ok("field check: res, device, relay, out_gb, temp, rerun may be written directly in a step",
   _field_err(_Mod([dict(_e, after=[dict(R.INFER, res={}, device="gpu", relay="path", out_gb=1, temp=["x"], rerun=True)])])) is None)

ok("_x: steps expand into per-part and experiment-level steps (flat list, order from inputs)",
   [s["name"] for s in x["after"]] == ["infer", "eval"]
   and [s["name"] for s in x["group_after"]] == ["oof", "paired", "ens", "p1", "p2", "p2cmp"])


def bad(**kw):
    try:
        R._x("toy", "T99", "Toy", "base", "D999_x", ("G", "en", "zh"), 0, **kw)
    except ValueError:
        return True
    return False


ok("_x: unknown step name, oof without splits, P1 without probe all error",
   bad(steps=["infer", "evl"]) and bad(steps=["oof"]) and bad(steps=["p1"]))

# Model-specific readers live in the plugin's readers/: present in the tables after load
ok("readers: toy_log, toy_debug loaded into PROGRESS, INFO; there is a one-epoch-done marker",
   "toy_log" in S.PROGRESS and "toy_debug" in S.INFO and S.TRAINED)
ok("readers: duplicate names -> error", raises(lambda: S.load_readers({"a.py": "PROGRESS={'toy_log': None}"})))
_dup = fixture.tmpdir()
os.makedirs(os.path.join(_dup, "readers"))
open(os.path.join(_dup, "readers", "toy.py"), "w").write("PROGRESS = {}\n")
_pl = os.environ.get("TASKQ_PLUGINS")
os.environ["TASKQ_PLUGINS"] = os.pathsep.join(filter(None, [_pl, _dup]))
try:
    ok("readers: two plugin dirs with the same readers/toy.py -> error, later does not replace earlier", raises(S.reader_sources))
finally:
    os.environ.pop("TASKQ_PLUGINS")
    if _pl is not None:
        os.environ["TASKQ_PLUGINS"] = _pl
rr = fixture.tmpdir()
ns = S.resolve("toy", {"ds": "D1_x", "variant": "tr", "frag": "0"})
od = os.path.join(rr, ns["out"])
os.makedirs(od)
open(os.path.join(od, "train_log_1.txt"), "w").write(
    "2026-10-01 10:00:00.1: Epoch 0\n2026-10-01 10:05:00.1: Epoch time: 300 s\n2026-10-01 10:05:01.1: Epoch 1\n")
_lg = os.path.join(rr, "logs", "tasks", S.safe(ns["key"]) + proto.SAFE + "123_" + proto.LOG_PART + "1.log")
os.makedirs(os.path.dirname(_lg))
open(_lg, "w").write("job 123_1\nx: Epoch time: 300 s\n")
rc = S.collect([{k: ns.get(k) for k in S.COLLECT_FIELDS}], rr)
ok("collect: toy_log reads 1 epoch done; a part log under logs/tasks counts as one epoch done",
   rc and rc["tasks"][ns["key"]]["runs"][0]["epochs_done"] == 1
   and [x["trained"] for x in rc["seglogs"] if x["name"] == os.path.basename(_lg)] == [True])

# spec.py collect --json: site_ops on the store and executors read progress through it
_r = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "collect", "--json"], capture_output=True, text=True,
                    input=json.dumps({"specs": [{k: ns.get(k) for k in S.COLLECT_FIELDS}], "root": rr, "wait": 30}))
_cj = S.json_in(_r.stdout)
ok("spec.py collect --json: stdin spec and data dir -> one @@JSON line with version, otherwise same as collect()",
   _r.returncode == 0 and len(_r.stdout.strip().splitlines()) == 1 and _cj["version"] == S.P.VERSION
   and {k: v for k, v in _cj.items() if k != "version"} == rc)
_r = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "collect"], capture_output=True, text=True, input="{}")
_r2 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "collect", "--json"], capture_output=True, text=True, input="[1]")
ok("spec.py collect: without --json, bad input -> exit 2", _r.returncode == 2 and _r2.returncode == 2)

# inputs_cmd can use core commands (bin/taskq) and lib like a task command: works when cron's PATH has no taskq
_path = os.environ["PATH"]
os.environ["PATH"] = "/usr/bin:/bin"
try:
    _li = S.listed_inputs("command -v taskq; echo $TASKQ_LIB", "/nonexistent")
finally:
    os.environ["PATH"] = _path
ok("inputs_cmd: core bin/ first on PATH, TASKQ_LIB is the core queue/ (like run_task.sh gives a task command)",
   _li == [os.path.join(os.path.dirname(HERE), "bin", "taskq"), HERE])

print("all passed" if fails == 0 else f"{fails} failed")
sys.exit(1 if fails else 0)
