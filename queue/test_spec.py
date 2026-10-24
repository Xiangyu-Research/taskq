#!/usr/bin/env python3
"""测任务说明的解析（2026-10-18）：scripts/queue/spec.py、recipes.py、experiments.py 和实验表。不连机器。

    python3 scripts/queue/test_spec.py
"""
import os
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
ok(f"实验表展开 {len(ts)} 个任务、key 不重复", len(ts) == 216 and len(set(ts)) == len(ts))
ok("每个任务都解析得了、有环境和输出", all(t["spec"].get("env") and t["spec"].get("out") for t in ts.values()))
um = ts["Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/1"]["spec"]
sw = ts["Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0"]["spec"]
ok("U-Mamba、SwinUNETR 写明了自己的环境", um["env"] == "umamba" and sw["env"] == "swinunetr")
ok("nnU-Net 的任务 key = 数据集/训练器/fold，out 和 10/18 前的文件夹一致",
   all(t["spec"]["out"] == "nnUNet/results/{0}/{1}__nnUNetResEncUNetMPlans__3d_fullres/fold_{2}".format(*k.split("/"))
       for k, t in ts.items() if t["recipe"] == "nnunet"))
ok("实验表的 metrics 进了说明（登记表读）", set(um.get("metrics", {})) == {"val_WT", "val_TC", "val_ET"})
ok("env_of：按实验表找模型的环境，表里没有用 nnunet",
   E.env_of("/x/Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR__nnUNetResEncUNetMPlans__3d_fullres") == "swinunetr"
   and E.env_of("/x/Dataset999_X/foo") == "nnunet")

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
