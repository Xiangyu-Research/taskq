"""Test experiment table: same shape as a real one (per-part steps, experiment-level steps, cross-experiment inputs, FINAL,
nice, skipped external sets, out-of-sample splits, board names), recipe toy (testdata/plugins/recipes.py), step commands are fake
scripts under testdata/bin/. Fields in queue/schema.py.

Each step is written once: cmd, out, done (files under out), inputs (real paths, wildcards allowed, may be another experiment's
output); the core derives order from inputs and outputs. {data} is the data dir on the executor.

Each experiment (arguments of _x): id, exp, label, model, ds, variant, env, frags (which parts to train), steps (steps after
training, names in STEPS), nice, board, skip_ext, only_ext, splits, probe, hours, vars, plan, step_only.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(HERE)))))
import cfg  # noqa: E402
QR = cfg.rel("queue")  # scheduler code's queue/ under the repo root (scripts/taskq/queue when embedded, queue in a standalone checkout)

F5 = ["0", "1", "2", "3", "4"]
HOURS = {"base": 8.5, "m": 30.0, "s": 25.0, "st": 22.0}
METRICS = {"val_A": ["check/summary.json", ["mean", "(1, 2, 3)", "Score"]],
           "val_B": ["check/summary.json", ["mean", "(2, 3)", "Score"]]}

PE = "bash " + QR + "/testdata/bin/step.sh"
PY = "python3 " + QR + "/testdata/bin/"
M = "results/{model}"          # training results (the source's outputs)
AN = "demo/analysis/{model}"   # analysis results
ENS_TAG = "frags_0-1-2-3-4"
T0_ENS = "demo/analysis/D101_alpha/V_save__PlanM__full/" + ENS_TAG  # t0's ensemble: another experiment's paired compares against it
T18_ENS = "demo/analysis/D132_alpha_s18/V_save__PlanM__full/" + ENS_TAG

# ---------------- per-part steps (after) ----------------
INFER = dict(name="infer", nice=-100, env="base", relay="toy_infer", res=dict(wait_gpu=120), out_gb=75,
             cmd="bash " + QR + "/testdata/bin/infer_task.sh {data}/" + M + " {frag}",
             out="preds/{model}/frag_{frag}", done="MANIFEST.json", temp="*/*.prob",
             inputs=[M + "/frag_{frag}/final.ckpt", M + "/dataset.json", M + "/plans.json", "raw/holdout/A/inputs"],
             inputs_cmd=PE + " --stage needs:infer {data}/" + M + " {frag}")
EVAL = dict(name="eval", nice=-100, env=["base", "stats"], device="cpu",
            cmd=PE + " --stage eval {data}/" + M + " {frag}",
            out=AN + "/frag_{frag}", done="report/report.md",
            inputs=["preds/{model}/frag_{frag}", M + "/frag_{frag}/check", "raw/labels/*/*-lab.dat"])
LPROB = dict(name="lprob", nice=-100, env="base", device="cpu",
             cmd=PY + "lprob.py {data}/" + M + " {frag} --workers 12 --out {data}/demo/lprob/{model}/frag_{frag}",
             out="demo/lprob/{model}/frag_{frag}", done="lprob.rec.json",
             inputs=["preds/{model}/frag_{frag}", M + "/dataset.json", M + "/frag_{frag}/check", AN + "/frag_{frag}/report",
                     "raw/ext/*/labels/*.dat"])
EDIT = dict(name="edit", nice=-100, env="base",
            cmd=PY + "edit.py {data}/" + M + " {frag} --out {data}/demo/edit/{model}/frag_{frag}",
            out="demo/edit/{model}/frag_{frag}", done="edit.rec.json", inputs=[M + "/frag_{frag}/final.ckpt"])

# ---------------- experiment-level steps (group_after): {frag} expands over every value in frag_list ----------------
OOF = dict(name="oof", env="base", device="cpu",
           cmd=PY + "oof.py {data}/" + M + " --out {data}/" + AN + "/oof",
           out=AN + "/oof", done=["oof.rec.json", "check.txt"], inputs=[M + "/frag_{frag}/check", AN + "/frag_{frag}/report"])
PAIRED = dict(name="paired", env="base", device="cpu",
              cmd=PY + "paired.py --ref {data}/" + T0_ENS + " --cmp {exp}={data}/" + AN + "/oof --out {data}/" + AN + "/paired",
              out=AN + "/paired", done="paired.csv", inputs=[T0_ENS, AN + "/oof"])
ENS = dict(name="ens", nice=-100, env=["base", "stats"], device="cpu", out_gb=90,
           cmd="ENSEMBLE_ONLY=1 " + PE + " --stage all {data}/" + M + " {frag_list_q}",
           out=[AN + "/" + ENS_TAG, "preds/{model}/" + ENS_TAG], done="report/report.md",
           inputs=["preds/{model}/frag_{frag}", "raw/labels/*/*-lab.dat"])


def _probe(p, inputs):
    out = "demo/probe/" + p + "_{probe}"
    return dict(name=p, nice=-100, env="base", cmd="MODEL={data}/" + M + " TAG={probe} OUT={data}/" + out + " bash " + QR + "/testdata/bin/" + p + "_run.sh",
                out=out, done="probe.rec.json",
                inputs=inputs + [M + "/dataset.json", M + "/plans.json", M + "/frag_{frag}/final.ckpt", "prepared/{ds}/splits_final.json"])


P1 = _probe("p1", ["demo/probe/p1/inputs", "demo/probe/p1/targets.csv"])
P2 = _probe("p2", ["demo/probe/p2/inputs", "demo/probe/p2/items"])
P2CMP = dict(name="p2cmp", env="base", device="cpu", cmd=PY + "p2_compare.py --tags {probe} --out {data}/demo/probe/p2_compare_{probe}",
             out="demo/probe/p2_compare_{probe}", done="p2_compare.rec.json", inputs=["demo/probe/p2_{probe}"])
T18CMP = dict(name="t18cmp", env="base", device="cpu", cmd=PY + "cmp18.py --a {data}/" + T18_ENS + " --b {data}/" + AN + "/" + ENS_TAG
              + " --out {data}/" + AN + "/t18cmp", out=AN + "/t18cmp", done="cmp18.rec.json", inputs=[T18_ENS, AN + "/" + ENS_TAG])

# ---------------- after all experiments: redo when an input is newer than the output ----------------
FINAL = [dict(name="compare_all", env="base", device="cpu", rerun=True,
              cmd=PY + "compare_all.py --out {data}/demo/analysis/compare/all",
              out="demo/analysis/compare/all", done="compare_all.rec.json",
              inputs=["demo/analysis/*/*/paired", "demo/analysis/*/*/" + ENS_TAG])]

STEPS = {s["name"]: s for s in [INFER, EVAL, LPROB, EDIT, OOF, PAIRED, ENS, P1, P2, P2CMP, T18CMP]}
FRAG_STEPS = ("infer", "eval", "lprob", "edit")
KEEP = ["infer", "eval", "ens"]
KEEP_L = ["infer", "eval", "lprob", "ens"]
OOF5 = ["infer", "eval", "oof", "paired"]
OOF5E = OOF5 + ["ens"]
FRAG_ALL = ["infer", "eval", "lprob"]
PROBE = ["p1", "p2", "p2cmp"]
GS = ["G-A", "G-B", "G-C", "G-D"]
SNAP = {"DEMO_SNAPSHOTS": "100,250,500"}


def _x(id, exp, label, model, ds, board, nice, steps=KEEP, variant="V_save", frags=F5, env=None,
       skip_ext=(), only_ext=(), splits=None, probe=None, hours=None, design=None, vars=None, plan=None, step_only=None):
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise ValueError(f"{id}: unknown steps {unknown} (have: {', '.join(STEPS)})")
    if "oof" in steps and not splits:
        raise ValueError(f"{id}: steps with oof need splits")
    if any(s in PROBE for s in steps) and not probe:
        raise ValueError(f"{id}: steps with P1 / P2 need probe")
    params = dict(ds=ds, variant=variant, **({"env": env} if env else {}), **({"plan": plan} if plan else {}))
    after = [dict(STEPS[s], only=dict(frag=list(step_only[s]))) if s in (step_only or {}) else STEPS[s] for s in steps if s in FRAG_STEPS]
    names = dict(skip_ext=" ".join(skip_ext), only_ext=" ".join(only_ext), splits=splits or "", **({"probe": probe} if probe else {}))
    return dict(id=id, exp=exp, label=label, model=model, hours=hours or HOURS[model], recipe="toy", params=params,
                fields=dict(metrics=METRICS, **({"vars": vars} if vars else {})),
                each=dict(frag=list(frags)), after=after, group_after=[STEPS[s] for s in steps if s not in FRAG_STEPS],
                names=names, nice=nice, board=dict(zip(("group", "en", "zh"), board)), **({"design": design} if design else {}))


B0 = ("Baseline", "Base, 5 parts", "baseline")
B1 = ("Architectures", "Base, 5 parts", "baseline")
B2 = ("Data", "In-domain", "in-domain")
OOF_T3 = dict(steps=OOF5E, splits="t3_splits.json")

EXPERIMENTS = [
    _x("t0", "T0", "baseline", "base", "D101_alpha", B0, 0, steps=KEEP_L),
    _x("arch_s", "T0", "baseline", "s", "D101_alpha", B1, 3000, steps=["infer", "eval", "edit", "ens"], variant="V_s", env="alt",
       step_only={"edit": ["0"]}),
    _x("arch_m", "T0", "baseline", "m", "D101_alpha", B1, 50, steps=KEEP + PROBE, variant="V_m", env="alt2", probe="arch_m"),
    _x("arch_st", "T0", "baseline", "st", "D101_alpha", B1, 3000, steps=KEEP, variant="V_st"),
    _x("t0b", "T0b", "all data", "base", "D102_alpha_dev", B0, 1000, steps=FRAG_ALL, frags=["all"], vars=SNAP),
    _x("t3b", "T3", "in-domain B", "base", "D111_beta", B2, 0, steps=OOF5E + PROBE, splits="t3_splits.json", skip_ext=["B"], probe="t3b"),
    _x("t3g", "T3", "in-domain G", "base", "D116_gamma", B2, 0, steps=OOF5E + PROBE, splits="t3_splits.json", skip_ext=GS, probe="t3g"),
    _x("t14g", "T14", "A + G mix", "base", "D121_alphagamma", B2, 0, steps=OOF5E, splits="t3_splits.json", skip_ext=GS),
    _x("t14b", "T14", "A + B mix", "base", "D122_alphabeta", B2, 0, steps=OOF5E, splits="t3_splits.json", skip_ext=["B"]),
    _x("t15b", "T15", "B aligned to 470 cases", "base", "D111_beta", B2, 0, variant="V_save_n470", skip_ext=["B"], **OOF_T3),
    _x("t15g", "T15", "G aligned to 470 cases", "base", "D116_gamma", B2, 0, variant="V_save_n470", skip_ext=GS, **OOF_T3),
    _x("t16", "T16", "one channel only", "base", "D103_alpha_one", B0, 50, steps=KEEP + PROBE, probe="t16"),
    _x("t18", "T18", "synthetic samples", "base", "D132_alpha_s18", B0, 60),
    _x("t18b", "T18b", "synthetic sample control", "base", "D133_alpha_s18b", B0, 60, steps=KEEP + ["t18cmp"]),
    _x("t22a", "T22", "hold out G-A", "base", "D151_gamma_loA", B2, 80, steps=FRAG_ALL, frags=["0"], only_ext=["G-A"]),
    _x("t24cov", "T24", "G only seen cases", "base", "D155_gamma_ecov", B2, 80, steps=OOF5, splits="t24_splits.json", skip_ext=GS),
]
