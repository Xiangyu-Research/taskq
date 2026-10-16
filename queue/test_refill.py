#!/usr/bin/env python3
"""测 refill.py（2026-10-16）：假的队列、Slurm 输出、训练进度，命令不真的执行（记下来核对）。

    python3 scripts/queue/test_refill.py
"""
import copy
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import refill as R  # noqa: E402

fails = 0
D = tempfile.mkdtemp()
R.LOG, R.STATE = f"{D}/refill.log", f"{D}/refill.json"
R.dataset_gb = lambda ds: {"Dataset103_GLIrq1_t1c": 4.0, "Dataset121_GLIMETrq1": 62.0}.get(ds, 10.0)


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def task(i, group, status, cmd, label="", prio=0):
    return dict(id=i, group=group, status=status, label=label, deps=[], start=None, end=None, command=cmd, priority=prio)


def train(i, ds, f, status="Queued", prio=0, tr=""):
    return task(i, "gpu1", status, f"bash scripts/queue/train_task.sh {ds} {f}{' ' + tr if tr else ''}", f"x{i}", prio)


SQ = [["rq1_t14met_f0", "486197_1", "PENDING", "0:00", "Priority", "0"],  # ① 但数据集 62 GB
      ["rq1_t1c_f0", "485145_1", "PENDING", "0:00", "Priority", "50"], ["rq1_t1c_f0", "485145_2", "PENDING", "0:00", "Priority", "50"],
      ["rq1_t1c_f1", "485146_1", "PENDING", "0:00", "Priority", "50"], ["rq1_t1c_f2", "485147_1", "PENDING", "0:00", "Priority", "50"],
      ["rq1_t1c_f3", "485148_1", "PENDING", "0:00", "Priority", "50"],
      ["rq1_t3peds_f0", "485150_1", "PENDING", "0:00", "Priority", "100"]]


def snap(lab_tasks, sq=SQ, prep="Dataset101_GLIrq1 Dataset111_MENrq1", disk=56e9, box_tasks=()):
    return dict(ok={"box": True, "lab": True, "dicc": True}, slurm={"dicc": dict(squeue=copy.deepcopy(sq), sacct=[])},
                probe={"lab": {"DISK": str(int(disk)), "PREP": prep}},
                src={"box": {"folds": {}}, "lab": {"folds": {}}, "dicc": {"folds": {}}},
                pueue={"box": {"groups": {}, "tasks": list(box_tasks)}, "lab": {"groups": {}, "tasks": lab_tasks}})


RUN = [train(1, 111, 0, "Running")]
p, why = R.plan(snap(RUN + [train(2, 111, 1)]), "lab")
ok("还有同档位排着的：不补", p is None and "够了" in why)
p, why = R.plan(snap(RUN), "lab")
ok("只剩在训的：补；①档数据集放不下就跳到下一个实验", p and p["exp"].startswith("T16") and p["nice"] == 50)
ok("同一实验凑够约 20 个折算小时（nnU-Net 3 折）", p and len(p["keys"]) == 3 and p["transfer"] == 4.0)
p, _ = R.plan(snap(RUN + [train(6, 101, 0, prio=-2000, tr="nnUNetTrainer_UMambaEnc")]), "lab")
ok("排着的是⑥档、待办里有更靠前的：补", p and p["nice"] == 50)
p, why = R.plan(snap(RUN, disk=43e9), "lab")
ok("盘不够（传完低于提醒线）：都跳过，说明原因", p is None and "放不下" in why)
p, _ = R.plan(snap(RUN, prep="Dataset101_GLIrq1 Dataset103_GLIrq1_t1c"), "lab")
ok("数据集已经在：不用传", p and p["transfer"] == 0)

s = snap(RUN)
s["src"]["dicc"]["folds"]["Dataset103_GLIrq1_t1c/nnUNetTrainer_save10/0"] = dict(final=False, runs=[dict(start="", end="2026-10-16 10:00:00", epochs_done=120, tl=[])])
p, _ = R.plan(s, "lab")
ok("开始过的折不取（要问）", p and "Dataset103_GLIrq1_t1c/nnUNetTrainer_save10/0" not in p["keys"])
sq = [r if r[0] != "rq1_t1c_f1" else r[:4] + ["JobHeldUser", "50"] for r in SQ]
p, _ = R.plan(snap(RUN, sq=sq), "lab")
ok("hold 的不取", p and "Dataset103_GLIrq1_t1c/nnUNetTrainer_save10/1" not in p["keys"])
fetch = task(30, "wait", "Running", "bash scripts/queue/fetch_fold.sh dicc Dataset103_GLIrq1_t1c/nnUNetTrainer_save10__nnUNetResEncUNetMPlans__3d_fullres 0", "f")
p, _ = R.plan(snap(RUN, box_tasks=[fetch]), "lab")
ok("box 上已经在等别处拉回的折不取", p and "Dataset103_GLIrq1_t1c/nnUNetTrainer_save10/0" not in p["keys"])
sync = task(31, "wait", "Running", "bash scripts/sync/tunnel_push.sh lab …", "sync_lab_Dataset103_GLIrq1_t1c")
ok("数据还在传：不补", R.plan(snap(RUN, box_tasks=[sync]), "lab")[0] is None)
stash = [train(7, 101, 1, "Stashed", -2000, "nnUNetTrainer_UMambaEnc"), train(8, 101, 2, "Stashed", -2000, "nnUNetTrainer_UMambaEnc")]
p, _ = R.plan(snap(RUN + stash, sq=[]), "lab")
ok("Slurm 上没有可取的：取暂存的（⑥档，重模型一次 1 折）", p and p["picks"][0]["src"] == "stash" and len(p["keys"]) == 1 and p["nice"] == 2000)
s = snap(RUN)
s["ok"]["dicc"] = False
del s["slurm"]["dicc"]
ok("Slurm 连不上：不补", R.plan(s, "lab")[0] is None)

# ---------------- 执行 ----------------
cmds = []


def fake(state="PENDING"):
    def run(cmd, host=None, timeout=0):
        cmds.append((host or "box", cmd))
        if "sort -u" in cmd:
            return 0, state
        if cmd.startswith("sleep 3; squeue"):
            return 0, ""
        return 0, str(100 + len(cmds))
    return run


p, _ = R.plan(snap(RUN), "lab")
msg = R.execute(p, "lab", run=fake())
sc = [c for h, c in cmds if c.startswith("scancel")]
ok("只取消还在排队的（--state=PENDING），每折一个数组", sc == ["scancel --state=PENDING 485145", "scancel --state=PENDING 485146", "scancel --state=PENDING 485147"])
lab = [c for h, c in cmds if h == "lab"]
ok("lab 上按档位加训练任务，要传数据时先暂存", len(lab) == 3 and all("--priority=-50 --stashed" in c for c in lab))
box = [c for h, c in cmds if h == "box"]
ok("box 上：传数据（传完放行）+ 每折拉回、推理、归档", len(box) == 10 and "tunnel_push.sh lab" in box[0] and "pueue enqueue" in box[0]
   and sum("fetch_fold.sh lab" in c for c in box) == 3 and sum("archive_fold.sh lab" in c for c in box) == 3)
ok("记日志、记下传过去的数据集", "自动接手" in open(R.LOG).read() and "Dataset103_GLIrq1_t1c" in open(R.STATE).read())
cmds.clear()
try:
    R.execute(p, "lab", run=fake("RUNNING"))
    ok("Slurm 上已经开始了：不取消、报错", False)
except RuntimeError as e:
    ok("Slurm 上已经开始了：不取消、报错", "没有取消" in str(e) and not any(c.startswith("scancel") for _, c in cmds))
cmds.clear()
p, _ = R.plan(snap(RUN + stash, sq=[]), "lab")
R.execute(p, "lab", run=fake())
ok("取暂存的：直接 enqueue，不用传数据", any(h == "lab" and "pueue enqueue 7" in c for h, c in cmds) and not any("tunnel" in c for _, c in cmds))

# ---------------- 清理 ----------------
cmds.clear()
busy = snap([train(1, 103, 3, "Running")])
ok("还有训练在用：不删", R.cleanup(busy, "lab", run=fake()) == [] and not cmds)
arch = task(40, "wait", "Queued", "bash scripts/queue/archive_fold.sh lab Dataset103_GLIrq1_t1c/x 0", "a")
ok("box 上还有这台机器的归档任务：不删", R.cleanup(snap([], box_tasks=[arch]), "lab", run=fake()) == [] and not cmds)
out = R.cleanup(snap([]), "lab", run=fake())
ok("都用完了：删掉自动传过去的数据集", len(out) == 1 and cmds[-1][1] == "rm -rf ~/BRAIN_TUMOR/nnUNet/preprocessed/Dataset103_GLIrq1_t1c")

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
