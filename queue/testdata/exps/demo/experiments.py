"""测试用的实验表（2026-11-02）：形状同真的实验表（每份做的步骤、所有份做完以后的几串步骤、档位、跳过的外部集、折外划分、
看板名称），做法是 toy（testdata/plugins/recipes.py），步骤的命令是 testdata/bin/ 下的假脚本。字段说明见 queue/experiments.py。

每个实验（_x 的参数）：id、exp、label、model、ds、variant、env、frags（训哪几份）、steps（训完以后的步骤，STEPS 的名字）、
nice、board、skip_ext、only_ext、splits、probe、hours、vars、plan。
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(HERE)))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(HERE)), "plugins"))
from taskq_api import check, exists  # noqa: E402
from step_checks import record  # noqa: E402
import cfg  # noqa: E402
QR = cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）

F5 = ["0", "1", "2", "3", "4"]
HOURS = {"base": 8.5, "m": 30.0, "s": 25.0, "st": 22.0}
METRICS = {"val_A": ["check/summary.json", ["mean", "(1, 2, 3)", "Score"]],
           "val_B": ["check/summary.json", ["mean", "(2, 3)", "Score"]]}


def _manifest_ok(path):
    """推理做完了：MANIFEST 里每个测试集的输出、概率、输入数都对得上，概率已稀疏化（形状同真的核对）。"""
    import glob
    try:
        m = json.load(open(path))
    except (OSError, ValueError):
        return False
    if "probabilities_zeroed_below" not in m:
        return False
    removed = set(((m.get("probabilities_removed") or {}).get("cohorts") or {}))
    for c, v in (m.get("cohorts") or {}).items():
        src = (v.get("args") or {}).get("source")
        n_in = len(glob.glob(os.path.join(src, "*_0000.dat"))) if isinstance(src, str) and os.path.isdir(src) else None
        if not v["n_outputs"] or v["n_outputs"] != v["n_probs"] or (n_in and v["n_outputs"] != n_in):
            return False
        d = os.path.join(os.path.dirname(path), c)
        here = (len(glob.glob(os.path.join(d, "*.out"))), len([f for f in glob.glob(os.path.join(d, "*.prob")) if not f.endswith(".tmp.prob")]))
        if c in removed:
            if here[0] != v["n_outputs"] or here[1] > v["n_probs"]:
                return False
        elif here != (v["n_outputs"], v["n_probs"]):
            return False
    return True


# ---------------- 步骤（{data} = box 的数据目录，{python} = box 上 base 环境的 python） ----------------
PE = "bash " + QR + "/testdata/bin/step.sh"
AN = "demo/analysis/{model}"
T0_ENS = "D101_alpha/V_save__PlanM__full/frags_0-1-2-3-4"
INFER = dict(name="infer", group="gpu", cmd=PE + " --stage infer {data}/results/{model} {frag}",
             done=check("preds/{model}/frag_{frag}/MANIFEST.json", _manifest_ok),
             queue=dict(nice=-100, key="infer/{model}/frag_{frag}", env="base",
                        cmd="bash " + QR + "/testdata/bin/infer_task.sh {data}/results/{model} {frag}",
                        out="preds/{model}/frag_{frag}", done=[".infer_done"], relay="toy_infer", disk=75,
                        inputs_cmd=PE + " --stage needs:infer {data}/results/{model} {frag}",
                        res=dict(wait_gpu=120)))
EVAL = dict(name="eval", group="cpu", cmd=PE + " --stage eval {data}/results/{model} {frag}",
            done=record(AN + "/frag_{frag}/report/report.rec.json", "report.md"),
            queue=dict(nice=-100, key="eval/{model}/frag_{frag}", env=["base", "stats"], device="cpu", relay="path",
                       cmd=PE + " --stage eval {data}/results/{model} {frag} && touch {data}/" + AN + "/frag_{frag}/.eval_done",
                       out=AN + "/frag_{frag}", done=[".eval_done"],
                       inputs=["preds/{model}/frag_{frag}/.infer_done", "results/{model}/frag_{frag}/check"]))
OOF = dict(name="oof", group="cpu", cmd="{python} " + QR + "/testdata/bin/oof.py {data}/results/{model}",
           done=record(AN + "/oof/oof.rec.json", "check.txt"))
PAIRED = dict(name="paired", group="cpu",
              cmd="{python} " + QR + "/testdata/bin/paired.py --ref {data}/demo/analysis/" + T0_ENS
                  + " --cmp {exp}={data}/demo/analysis/{model}/oof/eval --out {data}/demo/analysis/{model}/oof/paired",
              done=record(AN + "/oof/paired/paired.rec.json", "paired.csv"))
ENS_CMD = "ENSEMBLE_ONLY=1 " + PE + " --stage all {data}/results/{model} {frag_list_q}"
ENS = dict(name="ens", group="cpu", needs="infer", cmd=ENS_CMD,
           done=record(AN + "/frags_0-1-2-3-4/report/report.rec.json", "report.md"),
           queue=dict(nice=-100, key="ens/{model}", env=["base", "stats"], device="cpu", relay="path",
                      cmd=ENS_CMD + " && touch {data}/" + AN + "/frags_0-1-2-3-4/.ens_done",
                      out=AN + "/frags_0-1-2-3-4", done=[".ens_done"], disk=90,
                      fetch=dict(exclude=[], extra=[], also=["preds/{model}/frags_0-1-2-3-4"]),
                      inputs=["preds/{model}/frag_{frag}/.infer_done"]))
LPROB_CMD = QR + "/testdata/bin/lprob.py {data}/results/{model} {frag} --workers 12"
LPROB = dict(name="lprob", group="cpu", cmd="{python} " + LPROB_CMD,
             done=record(AN + "/frag_{frag}/lprob/lprob.rec.json"),
             queue=dict(nice=-100, key="lprob/{model}/frag_{frag}", env="base", device="cpu", relay="path",
                        cmd="python " + LPROB_CMD + " && touch {data}/" + AN + "/frag_{frag}/lprob/.lprob_done",
                        out=AN + "/frag_{frag}/lprob", done=[".lprob_done"],
                        inputs=["preds/{model}/frag_{frag}", "results/{model}/dataset.json",
                                "results/{model}/frag_{frag}/check", AN + "/frag_{frag}/eval", "raw/ext/*/labels/*.dat"]))


def _pruned(pred):
    """各份的单份概率删过了（MANIFEST 的 probabilities_removed 是 done），盘上没有剩下的 .prob。"""
    import glob
    import re
    frags = [d for d in glob.glob(os.path.join(pred, "frag_*")) if re.fullmatch(r"frag_\d+", os.path.basename(d))]
    if not frags or glob.glob(os.path.join(pred, "frag_*", "*", "*.prob")):
        return False
    for d in frags:
        try:
            r = json.load(open(os.path.join(d, "MANIFEST.json"))).get("probabilities_removed") or {}
        except (OSError, ValueError):
            return False
        if r.get("status") != "done":
            return False
    return True


PRUNE = dict(name="prune", group="cpu", needs=["infer", "eval"],
             cmd="{python} " + QR + "/testdata/bin/prune.py {data}/preds/{model} --frags {frag_list_q}",
             done=check("preds/{model}", _pruned))
FINAL = [dict(name="compare_all", group="cpu", cmd="{python} " + QR + "/testdata/bin/compare_all.py",
              done=record("demo/analysis/compare/all/compare_all.rec.json"), rerun_if_newer=["paired", "ens"])]


def _probe(p, inputs):
    out = "demo/probe/" + p + "_{probe}"
    cmd = "MODEL={data}/results/{model} TAG={probe} bash " + QR + "/testdata/bin/" + p + "_run.sh && touch {data}/" + out + "/.probe_done"
    model = ["results/{model}/" + f for f in ["dataset.json", "plans.json"] + [f"frag_{k}/final.ckpt" for k in F5]]
    return dict(name=p, group="gpu", cmd=cmd, done=exists(out + "/.probe_done"),
                queue=dict(nice=-100, key="probe/" + p + "_{probe}", env="base", cmd=cmd, out=out, done=[".probe_done"],
                           inputs=inputs + model + ["prepared/{ds}/splits_final.json"]))


P1 = _probe("p1", ["demo/probe/p1/inputs", "demo/probe/p1/targets.csv"])
P2 = _probe("p2", ["demo/probe/p2/inputs", "demo/probe/p2/items"])
P2CMP = dict(name="p2cmp", group="cpu", cmd="{python} " + QR + "/testdata/bin/p2_compare.py --tags {probe} --out {data}/demo/probe/p2_compare_{probe}",
             done=record("demo/probe/p2_compare_{probe}/p2_compare.rec.json"))
T18CMP = dict(name="t18cmp", group="cpu", cmd="{python} " + QR + "/testdata/bin/cmp18.py --out {data}/demo/analysis/followups/T18",
              done=record("demo/analysis/followups/T18/cmp18.rec.json"))

STEPS = {s["name"]: s for s in [INFER, EVAL, LPROB, OOF, PAIRED, ENS, PRUNE, P1, P2, P2CMP, T18CMP]}
FRAG_STEPS = ("infer", "eval", "lprob")
CHAIN = {"paired": "oof", "prune": "ens", "p2cmp": "p2", "t18cmp": "prune"}
KEEP = ["infer", "eval", "ens"]
FULL = KEEP + ["prune"]
KEEP_L = ["infer", "eval", "lprob", "ens"]
OOF5 = ["infer", "eval", "oof", "paired"]
OOF5E = OOF5 + ["ens", "prune"]
FRAG_ALL = ["infer", "eval", "lprob"]
PROBE = ["p1", "p2", "p2cmp"]
GS = ["G-A", "G-B", "G-C", "G-D"]
SNAP = {"DEMO_SNAPSHOTS": "100,250,500"}

OLDLOGS = {"D101_alpha": dict(prefix="{variant}_frag{frag}_")}


def _x(id, exp, label, model, ds, board, nice, steps=FULL, variant="V_save", frags=F5, env=None,
       skip_ext=(), only_ext=(), splits=None, probe=None, hours=None, design=None, vars=None, plan=None):
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise ValueError(f"{id}：不认识的步骤 {unknown}（有：{', '.join(STEPS)}）")
    if "oof" in steps and not splits:
        raise ValueError(f"{id}：有 oof 步骤要给 splits")
    if any(s in PROBE for s in steps) and not probe:
        raise ValueError(f"{id}：有 P1 / P2 步骤要给 probe")
    if "lprob" in steps and steps.index("lprob") < steps.index("eval"):
        raise ValueError(f"{id}：lprob 要在 eval 后面")
    params = dict(ds=ds, variant=variant, **({"env": env} if env else {}), **({"plan": plan} if plan else {}))
    after = [STEPS[s] for s in steps if s in FRAG_STEPS]
    prune = dict(PRUNE, needs=PRUNE["needs"] + ["lprob"]) if "lprob" in steps else PRUNE
    groups = []
    for s in steps:
        if s in FRAG_STEPS:
            continue
        if s in CHAIN:
            if not groups or groups[-1][-1]["name"] != CHAIN[s]:
                raise ValueError(f"{id}：{s} 要紧跟在 {CHAIN[s]} 后面")
            groups[-1].append(prune if s == "prune" else STEPS[s])
        else:
            groups.append([prune if s == "prune" else STEPS[s]])
    names = dict(skip_ext=" ".join(skip_ext), only_ext=" ".join(only_ext), splits=splits or "", **({"probe": probe} if probe else {}))
    return dict(id=id, exp=exp, label=label, model=model, hours=hours or HOURS[model], recipe="toy", params=params,
                fields=dict(metrics=METRICS, **({"vars": vars} if vars else {}), **({"oldlogs": OLDLOGS[ds]} if ds in OLDLOGS else {})),
                each=dict(frag=list(frags)), after=after, group_after=groups, names=names, nice=nice,
                board=dict(zip(("group", "en", "zh"), board)), **({"design": design} if design else {}))


B0 = ("Baseline", "Base, 5 parts", "基线")
B1 = ("Architectures", "Base, 5 parts", "基线")
B2 = ("Data", "In-domain", "域内")
OOF_T3 = dict(steps=OOF5E, splits="t3_splits.json")

EXPERIMENTS = [
    _x("t0", "T0", "基线", "base", "D101_alpha", B0, 0, steps=KEEP_L),
    _x("arch_s", "T0", "基线", "s", "D101_alpha", B1, 3000, steps=KEEP, variant="V_s", env="alt"),
    _x("arch_m", "T0", "基线", "m", "D101_alpha", B1, 50, steps=KEEP + PROBE, variant="V_m", env="alt2", probe="arch_m"),
    _x("arch_st", "T0", "基线", "st", "D101_alpha", B1, 3000, steps=KEEP, variant="V_st"),
    _x("t0b", "T0b", "全部数据", "base", "D102_alpha_dev", B0, 1000, steps=FRAG_ALL, frags=["all"], vars=SNAP),
    _x("t3b", "T3", "域内 B", "base", "D111_beta", B2, 0, steps=OOF5E + PROBE, splits="t3_splits.json", skip_ext=["B"], probe="t3b"),
    _x("t3g", "T3", "域内 G", "base", "D116_gamma", B2, 0, steps=OOF5E + PROBE, splits="t3_splits.json", skip_ext=GS, probe="t3g"),
    _x("t14g", "T14", "A + G 混合", "base", "D121_alphagamma", B2, 0, steps=OOF5E, splits="t3_splits.json", skip_ext=GS),
    _x("t14b", "T14", "A + B 混合", "base", "D122_alphabeta", B2, 0, steps=OOF5E, splits="t3_splits.json", skip_ext=["B"]),
    _x("t15b", "T15", "B 对齐 470 例", "base", "D111_beta", B2, 0, variant="V_save_n470", skip_ext=["B"], **OOF_T3),
    _x("t15g", "T15", "G 对齐 470 例", "base", "D116_gamma", B2, 0, variant="V_save_n470", skip_ext=GS, **OOF_T3),
    _x("t16", "T16", "只用一个通道", "base", "D103_alpha_one", B0, 50, steps=FULL + PROBE, probe="t16"),
    _x("t18", "T18", "合成样本", "base", "D132_alpha_s18", B0, 60),
    _x("t18b", "T18b", "合成样本对照", "base", "D133_alpha_s18b", B0, 60, steps=FULL + ["t18cmp"]),
    _x("t22a", "T22", "留出 G-A", "base", "D151_gamma_loA", B2, 80, steps=FRAG_ALL, frags=["0"], only_ext=["G-A"]),
    _x("t24cov", "T24", "G 只留见过的", "base", "D155_gamma_ecov", B2, 80, steps=OOF5, splits="t24_splits.json", skip_ext=GS),
]
