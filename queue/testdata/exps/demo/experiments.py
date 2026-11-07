"""测试用的实验表：形状同真的实验表（每份做的步骤、实验一级的步骤、跨实验的输入、FINAL、档位、跳过的外部集、折外划分、
看板名称），做法是 toy（testdata/plugins/recipes.py），步骤的命令是 testdata/bin/ 下的假脚本。字段见 queue/schema.py。

每个步骤只写一遍：cmd、out、done（out 下的文件）、inputs（真路径，可以通配、可以是别的实验的产出）；先后由核心按输入和产出推。
{data} 是执行机上的数据目录。

每个实验（_x 的参数）：id、exp、label、model、ds、variant、env、frags（训哪几份）、steps（训完以后的步骤，STEPS 的名字）、
nice、board、skip_ext、only_ext、splits、probe、hours、vars、plan、step_only。
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(HERE)))))
import cfg  # noqa: E402
QR = cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）

F5 = ["0", "1", "2", "3", "4"]
HOURS = {"base": 8.5, "m": 30.0, "s": 25.0, "st": 22.0}
METRICS = {"val_A": ["check/summary.json", ["mean", "(1, 2, 3)", "Score"]],
           "val_B": ["check/summary.json", ["mean", "(2, 3)", "Score"]]}

PE = "bash " + QR + "/testdata/bin/step.sh"
PY = "python3 " + QR + "/testdata/bin/"
M = "results/{model}"          # 训练的结果（源头的产出）
AN = "demo/analysis/{model}"   # 分析结果
ENS_TAG = "frags_0-1-2-3-4"
T0_ENS = "demo/analysis/D101_alpha/V_save__PlanM__full/" + ENS_TAG  # t0 的集成：别的实验的 paired 拿它比
T18_ENS = "demo/analysis/D132_alpha_s18/V_save__PlanM__full/" + ENS_TAG

# ---------------- 每份的步骤（after） ----------------
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

# ---------------- 实验一级的步骤（group_after）：{frag} 按 frag_list 的每个取值展开 ----------------
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

# ---------------- 所有实验以后：输入比产出新就重做 ----------------
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
        raise ValueError(f"{id}：不认识的步骤 {unknown}（有：{', '.join(STEPS)}）")
    if "oof" in steps and not splits:
        raise ValueError(f"{id}：有 oof 步骤要给 splits")
    if any(s in PROBE for s in steps) and not probe:
        raise ValueError(f"{id}：有 P1 / P2 步骤要给 probe")
    params = dict(ds=ds, variant=variant, **({"env": env} if env else {}), **({"plan": plan} if plan else {}))
    after = [dict(STEPS[s], only=dict(frag=list(step_only[s]))) if s in (step_only or {}) else STEPS[s] for s in steps if s in FRAG_STEPS]
    names = dict(skip_ext=" ".join(skip_ext), only_ext=" ".join(only_ext), splits=splits or "", **({"probe": probe} if probe else {}))
    return dict(id=id, exp=exp, label=label, model=model, hours=hours or HOURS[model], recipe="toy", params=params,
                fields=dict(metrics=METRICS, **({"vars": vars} if vars else {})),
                each=dict(frag=list(frags)), after=after, group_after=[STEPS[s] for s in steps if s not in FRAG_STEPS],
                names=names, nice=nice, board=dict(zip(("group", "en", "zh"), board)), **({"design": design} if design else {}))


B0 = ("Baseline", "Base, 5 parts", "基线")
B1 = ("Architectures", "Base, 5 parts", "基线")
B2 = ("Data", "In-domain", "域内")
OOF_T3 = dict(steps=OOF5E, splits="t3_splits.json")

EXPERIMENTS = [
    _x("t0", "T0", "基线", "base", "D101_alpha", B0, 0, steps=KEEP_L),
    _x("arch_s", "T0", "基线", "s", "D101_alpha", B1, 3000, steps=["infer", "eval", "edit", "ens"], variant="V_s", env="alt",
       step_only={"edit": ["0"]}),
    _x("arch_m", "T0", "基线", "m", "D101_alpha", B1, 50, steps=KEEP + PROBE, variant="V_m", env="alt2", probe="arch_m"),
    _x("arch_st", "T0", "基线", "st", "D101_alpha", B1, 3000, steps=KEEP, variant="V_st"),
    _x("t0b", "T0b", "全部数据", "base", "D102_alpha_dev", B0, 1000, steps=FRAG_ALL, frags=["all"], vars=SNAP),
    _x("t3b", "T3", "域内 B", "base", "D111_beta", B2, 0, steps=OOF5E + PROBE, splits="t3_splits.json", skip_ext=["B"], probe="t3b"),
    _x("t3g", "T3", "域内 G", "base", "D116_gamma", B2, 0, steps=OOF5E + PROBE, splits="t3_splits.json", skip_ext=GS, probe="t3g"),
    _x("t14g", "T14", "A + G 混合", "base", "D121_alphagamma", B2, 0, steps=OOF5E, splits="t3_splits.json", skip_ext=GS),
    _x("t14b", "T14", "A + B 混合", "base", "D122_alphabeta", B2, 0, steps=OOF5E, splits="t3_splits.json", skip_ext=["B"]),
    _x("t15b", "T15", "B 对齐 470 例", "base", "D111_beta", B2, 0, variant="V_save_n470", skip_ext=["B"], **OOF_T3),
    _x("t15g", "T15", "G 对齐 470 例", "base", "D116_gamma", B2, 0, variant="V_save_n470", skip_ext=GS, **OOF_T3),
    _x("t16", "T16", "只用一个通道", "base", "D103_alpha_one", B0, 50, steps=KEEP + PROBE, probe="t16"),
    _x("t18", "T18", "合成样本", "base", "D132_alpha_s18", B0, 60),
    _x("t18b", "T18b", "合成样本对照", "base", "D133_alpha_s18b", B0, 60, steps=KEEP + ["t18cmp"]),
    _x("t22a", "T22", "留出 G-A", "base", "D151_gamma_loA", B2, 80, steps=FRAG_ALL, frags=["0"], only_ext=["G-A"]),
    _x("t24cov", "T24", "G 只留见过的", "base", "D155_gamma_ecov", B2, 80, steps=OOF5, splits="t24_splits.json", skip_ext=GS),
]
