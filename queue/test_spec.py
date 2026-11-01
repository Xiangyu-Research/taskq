#!/usr/bin/env python3
"""测任务说明的解析（2026-10-18）：scripts/queue/spec.py、recipes.py、experiments.py 和实验表。不连机器。

    python3 scripts/queue/test_spec.py
"""
import itertools
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import experiments as E  # noqa: E402
import spec as S  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def raises(fn):
    try:
        fn()
    except S.SpecError:
        return True
    return False


# ---------------- 做法 nnunet ----------------
s = S.resolve("nnunet", {"dataset": "Dataset111_MENrq1", "trainer": "nnUNetTrainer_save10_n470", "fold": "1"})
ok("nnunet：key、out、env 默认值", s["key"] == "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1"
   and s["out"] == "nnUNet/results/Dataset111_MENrq1/nnUNetTrainer_save10_n470__nnUNetResEncUNetMPlans__3d_fullres/fold_1"
   and s["env"] == "nnunet" and s["data"] == ["nnUNet/preprocessed/Dataset111_MENrq1"])
ok("nnunet：命令按参数填", s["cmd"] == "bash research/common/nnunet/train.sh Dataset111_MENrq1 3d_fullres 1 nnUNetResEncUNetMPlans nnUNetTrainer_save10_n470")
ok("nnunet：{data}、{repo} 留到运行时展开", "{data}" in s["vars"]["nnUNet_results"] and "{repo}" in s["vars"]["nnUNet_extTrainer"])
ok("缺必填参数（fold）：报错", raises(lambda: S.resolve("nnunet", {"dataset": "Dataset101_GLIrq1"})))
ok("写错参数名：报错", raises(lambda: S.resolve("nnunet", {"dataset": "Dataset101_GLIrq1", "fold": "0", "trainr": "x"})))
ok("没有这种做法：报错", raises(lambda: S.resolve("nope", {})))
s2 = S.resolve("nnunet", {"dataset": "Dataset101_GLIrq1", "fold": "0", "env": "umamba", "vars": {"SPLITS_DIR": "x"}})
ok("参数覆盖同名字段：env 换掉、vars 合并", s2["env"] == "umamba" and s2["vars"]["SPLITS_DIR"] == "x" and "nnUNet_results" in s2["vars"])

# ---------------- 做法 command ----------------
c = S.resolve("command", {"key": "misc/a", "cmd": "python x.py --out {data}/misc/a", "env": "nnunet", "out": "misc/a",
                          "done": ["model.pt"], "data": ["other/stuff"], "extra": "1"})
ok("command：任务自己给 key、cmd、out、done，可以带别的参数", c["key"] == "misc/a" and c["done"] == ["model.pt"]
   and c["data"] == ["other/stuff"] and "{data}" in c["cmd"] and c["archive"] is None)
ok("command：缺 cmd 报错", raises(lambda: S.resolve("command", {"key": "a", "env": "x", "out": "a", "done": "x"})))

# ---------------- 10/31：任务单的通用字段 ----------------
G = dict(key="guest/x", cmd="python3 t.py", env="nnunet", out="guest/x", done="DONE")
g = S.resolve("command", dict(G, inputs="guest/in", res={"gpus": "2", "mem": "16G"}, resume="ckpt_*.pt", owner="bob",
                              progress="progress_json"))
ok("任务单：inputs = data（字符串变列表）、res 转成数、resume 变列表、owner", g["data"] == ["guest/in"] and g["inputs"] == ["guest/in"]
   and g["res"] == {"gpus": 2, "mem": "16G"} and g["resume"] == ["ckpt_*.pt"] and g["owner"] == "bob")
ok("任务单：command 默认不交给执行机、一张卡、没有存档", S.resolve("command", G)["relay"] is None
   and S.resolve("command", G)["res"] == {"gpus": 1} and S.resolve("command", G)["resume"] is None)
ok("任务单：inputs 和 data 都给 → 报错", raises(lambda: S.resolve("command", dict(G, inputs=["a"], data=["b"]))))
ok("任务单：res 写错字段、不是数、mem 格式不对 → 报错", raises(lambda: S.resolve("command", dict(G, res={"gpu": 1})))
   and raises(lambda: S.resolve("command", dict(G, res={"cpus": "many"})))
   and raises(lambda: S.resolve("command", dict(G, res={"mem": "lots"}))))
w = S.resolve("command", dict(G, res={"wait_gpu": "120"}))
ok("任务单：res.wait_gpu 转成分钟数、和默认的卡数合在一起，shell 里给 TASK_WAIT_GPU；不写时为空、负数报错",
   w["res"] == {"gpus": 1, "wait_gpu": 120} and "TASK_WAIT_GPU=120" in S.shell_lines(w, "/d").splitlines()
   and "TASK_WAIT_GPU=" in S.shell_lines(S.resolve("command", G), "/d").splitlines()
   and raises(lambda: S.resolve("command", dict(G, res={"wait_gpu": -1}))))
ok("任务单：data 里有 .. → 报错", raises(lambda: S.resolve("command", dict(G, inputs=["../etc"]))))
ok("任务单：不认识的 progress → 报错", raises(lambda: S.resolve("command", dict(G, progress="tqdm"))))
ok("nnunet：也有 inputs、resume、一张卡、输入拷到本地，进度读 nnU-Net 日志", s["inputs"] == s["data"] and s["resume"]
   and s["res"] == {"gpus": 1, "local_inputs": 1} and s["progress"] == "nnunet_log" and s["relay"] == "nnunet")
sl = S.shell_lines(s, "/d").splitlines()
ok("shell：TASK_INPUTS 是输入的绝对路径（数组），TASK_LOCAL 跟 res.local_inputs", f"TASK_INPUTS=(/d/nnUNet/preprocessed/{s['params']['dataset'] if 'params' in s else s['data'][0].split('/')[-1]})" in sl
   and "TASK_LOCAL=1" in sl and "TASK_LOCAL=0" in S.shell_lines(S.resolve("command", G), "/d").splitlines())
gr = tempfile.mkdtemp()
gs = {k: g.get(k) for k in S.COLLECT_FIELDS}
gd = os.path.join(gr, "guest/x")
os.makedirs(gd)
open(os.path.join(gd, "train.log"), "w").write("starting\n")
f0 = S.collect([gs], gr)["tasks"]["guest/x"]
ok("collect：还没写 PROGRESS.json、没有存档 → 没进度", not f0["resumable"] and all(r["epochs_done"] == 0 for r in f0["runs"]))
open(os.path.join(gd, "PROGRESS.json"), "w").write('{"done": 3, "total": 10}')
f1 = S.collect([gs], gr)["tasks"]["guest/x"]
ok("collect：PROGRESS.json → 做完 3 轮、共 10 轮", max(r["epochs_done"] for r in f1["runs"]) == 3
   and any(r.get("total") == 10 for r in f1["runs"]))
open(os.path.join(gd, "PROGRESS.json"), "w").write("{broken")
ok("collect：PROGRESS.json 坏了 → 不报错、当没进度", max(r["epochs_done"] for r in S.collect([gs], gr)["tasks"]["guest/x"]["runs"]) == 0)
open(os.path.join(gd, "ckpt_0003.pt"), "w").close()
ok("collect：有 resume 里的存档 → resumable", S.collect([gs], gr)["tasks"]["guest/x"]["resumable"])
import relay as RL  # noqa: E402
import refill as RF  # noqa: E402
ok("relay.progress：只有存档（没有轮数）也算有进度", RL.progress(dict(final=False, runs=[dict(epochs_done=0)], resumable=True))
   and not RL.progress(dict(final=False, runs=[dict(epochs_done=0)], resumable=False)))
src = {"src": {"lab": {"tasks": {"guest/x": dict(final=False, runs=[dict(epochs_done=0, end=None)], resumable=True)}},
               "dicc": {"tasks": {}}}}
ok("refill.progress：只有存档也算有进度，按机器分", RF.progress(src, "guest/x", "lab") and not RF.progress(src, "guest/x", "dicc"))

# ---------------- 训完的判断 ----------------
root = tempfile.mkdtemp()
out = os.path.join(root, s["out"])
os.makedirs(os.path.join(out, "validation"))
ok("什么都没有：没训完", not S.is_done(s, root))
open(os.path.join(out, "validation", "summary.json"), "w").write("{}")
ok("只有 summary.json：没训完", not S.is_done(s, root))
open(os.path.join(out, "ARCHIVED"), "w").close()
ok("summary.json + ARCHIVED（| 里有一个就行）：训完", S.is_done(s, root))
def oserr(fn):
    try:
        fn()
    except OSError:
        return True
    return False


ok("数据目录不存在（盘没挂上）：报错，不当成没训完", oserr(lambda: S.is_done(s, os.path.join(root, "nope"))))
real_stat = os.stat


def broken_stat(p, *a, **k):
    if str(p).startswith(out):
        raise OSError(70, "Communication error on send")
    return real_stat(p, *a, **k)


os.stat = broken_stat
ok("结果目录读不了（BeeGFS 断开，errno 70）：报错", oserr(lambda: S.is_done(s, root)))
os.stat = real_stat

# ---------------- 命令行参数来回 ----------------
args = S.kv_args("command", {"key": "a", "done": ["x", "y|z"]})
ok("kv_args → parse_kv 来回一致（列表用 JSON）", S.parse_kv(args[1:]) == {"key": "a", "done": ["x", "y|z"]})

# ---------------- 实验表 ----------------
ts = E.tasks()
exps = E.load()[0]
ok(f"实验表展开 {len(ts)} 个任务（= 各实验 each 的组合数之和）、key 不重复",
   len(ts) == sum(len(list(itertools.product(*(e.get("each") or {}).values()))) for e in exps) and len(set(ts)) == len(ts))
_rt = E.resolve_task
E.resolve_task = lambda recipe, params, exp=None: dict(key="/".join(f"{k}={v}" for k, v in sorted(params.items())),  # noqa: E731
                                                     names={k: str(v) for k, v in params.items()})
_ex = dict(id="gx", exp="GX", recipe="command", params=dict(p="1"), each=dict(seed=["1", "2"], fold=["0", "1", "2"]))
_t = E.expand(_ex)
ok("each：按参数的每种组合展开（2 × 3），参数进 params、each 记下取值",
   len(_t) == 6 and _t[0]["params"] == dict(p="1", seed="1", fold="0") and _t[-1]["each"] == dict(seed="2", fold="2"))
ok("each：only 只展开给的取值；only 里的参数不在 each 里 → 报错",
   [t["each"] for t in E.expand(_ex, dict(fold=["2"]))] == [dict(seed="1", fold="2"), dict(seed="2", fold="2")]
   and raises(lambda: E.expand(_ex, dict(lr=["1"]))))
ok("each：标签 = id + 每个参数的 _<首字母><取值>；没有 each → 只有 id",
   E.label(_ex, _t[1]) == "gx_s1_f1" and E.label(dict(id="gy"), E.expand(dict(id="gy", exp="GY", recipe="command", params={}))[0]) == "gy")
_g = E.group_names(_ex, "/d", "py")
ok("each：实验一级的名字有 <参数>_list、<参数>_list_q",
   _g["fold_list"] == "0 1 2" and _g["fold_list_q"] == "'0 1 2'" and _g["seed_list"] == "1 2" and "fold" not in _g and _g["p"] == "1")
E.resolve_task = _rt
_lab = {E.label(E.experiment(t["exp"]), t) for t in ts.values()}
ok("实验表的标签和以前的写法一样（<id>_f<fold>）",
   _lab == {f"{t['exp']}_f{t['params']['fold']}" if "fold" in t["params"] else t["exp"] for t in ts.values()})
ok("每个任务都解析得了、有环境和输出", all(t["spec"].get("env") and t["spec"].get("out") for t in ts.values()))
um = ts["Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/1"]["spec"]
sw = ts["Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0"]["spec"]
ok("U-Mamba、SwinUNETR 写明了自己的环境", um["env"] == "umamba" and sw["env"] == "swinunetr")
ok("nnU-Net 的任务 key = 数据集/训练器/fold，out 和 10/18 前的文件夹一致",
   all(t["spec"]["out"] == "nnUNet/results/{0}/{1}__{p}__3d_fullres/fold_{2}".format(*k.split("/"), p=t["params"].get("plans", "nnUNetResEncUNetMPlans"))
       for k, t in ts.items() if t["recipe"] == "nnunet"))
ok("实验表给了 plans 的（T35 nnU-Net 原版）进了模型文件夹",
   ts["Dataset101_GLIrq1/nnUNetTrainer_save10_plain/0"]["spec"]["out"] == "nnUNet/results/Dataset101_GLIrq1/nnUNetTrainer_save10_plain__nnUNetPlans__3d_fullres/fold_0")
ok("实验表的 metrics 进了说明（登记表读）", set(um.get("metrics", {})) == {"val_WT", "val_TC", "val_ET"})
ok("env_of：按实验表找模型的环境",
   E.env_of("/x/Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR__nnUNetResEncUNetMPlans__3d_fullres") == "swinunetr")
try:
    E.env_of("/x/Dataset999_X/foo")
    ok("env_of：表里没有的模型报错（不默认 nnunet）", False)
except S.SpecError as err:
    ok("env_of：表里没有的模型报错（不默认 nnunet），提示设 MODEL_ENV", "MODEL_ENV" in str(err))
r = subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments.py"), "env-of", "/x/Dataset999_X/foo"], capture_output=True, text=True)
ok("experiments.py env-of：表里没有 → 退出码非 0、说明在标准错误（predict.sh 因此停下）", r.returncode != 0 and "MODEL_ENV" in r.stderr and not r.stdout)


# ---------------- 实验的声明（10/24）：别的脚本按模型文件夹查，不按名字猜 ----------------
M = "/x/{}/nnUNetTrainer_save10__nnUNetResEncUNetMPlans__3d_fullres"
ok("names_of：T24 跳过转移瘤四个队列、用 t24_splits.json",
   E.names_of(M.format("Dataset155_METrq1_ecov"), "skip_ext", "splits") == ["MET-Yale MET-UCSF MET-Multi MET-UCSD", "t24_splits.json"])
ok("names_of：T22 只测留出的机构", E.names_of(M.format("Dataset151_METrq1_loYale"), "only_ext") == ["MET-Yale"])
ok("names_of：GLI 模型什么都不跳过、没有划分", E.names_of(M.format("Dataset101_GLIrq1"), "skip_ext", "only_ext", "splits") == ["", "", ""])
ok("names_of：表里没有的模型报错（不猜）", raises(lambda: E.names_of("/x/Dataset999_X/foo", "skip_ext")))
ok("每个 RQ1 实验都有档位和看板名称", all(isinstance(e.get("nice"), int) and set(e.get("board") or {}) == {"group", "en", "zh"}
                                      for e in exps if e["rq"] == "RQ1"))
ok("有 oof 步骤的都给了划分文件", all(e["names"]["splits"] for e in exps if e["rq"] == "RQ1"
                                     and any(s["name"] == "oof" for c in e["group_after"] for s in c)))
import importlib.util  # noqa: E402
spec_ = importlib.util.spec_from_file_location("rq1_exps", os.path.join(E.REPO, "research", "RQ1", "experiments.py"))
R = importlib.util.module_from_spec(spec_)
spec_.loader.exec_module(R)
x = R._x("toy", "T99", "玩具", "nnU-Net", "Dataset999_X", ("G", "en", "zh"), 0, steps=R.OOF5 + ["ens"] + R.PROBE, splits="s.json", probe="p")
ok("_x：steps 展开成每折步骤和一串串实验级步骤（paired 接 oof、p2cmp 接 p2）",
   [s["name"] for s in x["after"]] == ["infer", "eval"]
   and [[s["name"] for s in c] for c in x["group_after"]] == [["oof", "paired"], ["ens"], ["p1"], ["p2", "p2cmp"]])


def bad(**kw):
    try:
        R._x("toy", "T99", "玩具", "nnU-Net", "Dataset999_X", ("G", "en", "zh"), 0, **kw)
    except ValueError:
        return True
    return False


ok("_x：写错步骤名、oof 没给 splits、paired 不跟在 oof 后面、P1 没给 probe 都报错",
   bad(steps=["infer", "evl"]) and bad(steps=["oof"]) and bad(steps=["paired"], splits="s.json") and bad(steps=["p1"]))

# 模型专用的读法放在 readers/（10/31）：本机加载后表里有；remote_collect 送到别处（经 stdin，没有 readers/ 目录）也能读
ok("readers：nnunet_log、nnunet_debug 加载进 PROGRESS、INFO，有训完一轮的字样",
   "nnunet_log" in S.PROGRESS and "nnunet_debug" in S.INFO and S.TRAINED)
ok("readers：重名的报错", raises(lambda: S.load_readers({"a.py": "PROGRESS={'nnunet_log': None}"})))
rr = tempfile.mkdtemp()
ns = S.resolve("nnunet", {"dataset": "Dataset1_x", "trainer": "tr", "fold": "0"})
od = os.path.join(rr, ns["out"])
os.makedirs(od)
open(os.path.join(od, "training_log_1.txt"), "w").write(
    "2026-10-01 10:00:00.1: Epoch 0\n2026-10-01 10:05:00.1: Epoch time: 300 s\n2026-10-01 10:05:01.1: Epoch 1\n")
os.makedirs(os.path.join(rr, ns["oldlogs"]["dir"]))
open(os.path.join(rr, ns["oldlogs"]["dir"], ns["oldlogs"]["prefix"] + "123_seg1.log"), "w").write("作业 123_1\nx: Epoch time: 300 s\n")
_run = S.subprocess.run
S.subprocess.run = lambda cmd, **kw: _run([sys.executable, "-"], **kw)
try:
    rc = S.remote_collect("host", [{k: ns.get(k) for k in S.COLLECT_FIELDS}], rr)
finally:
    S.subprocess.run = _run
ok("remote_collect：经 stdin 跑时 readers 一起送过去（nnunet_log 读出训完 1 轮，oldlogs 目录里的分段日志算训完一轮）",
   rc and rc["tasks"][ns["key"]]["runs"][0]["epochs_done"] == 1
   and [x["trained"] for x in rc["seglogs"] if x["name"].startswith(ns["oldlogs"]["prefix"])] == [True])

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
