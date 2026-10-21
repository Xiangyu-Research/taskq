#!/usr/bin/env python3
"""测 health.py 的检查和推送规则（2026-10-16）：假的队列、训练日志、DICC 输出、时钟，不连任何机器、不真的推送。

    python3 scripts/monitor/test_health.py
"""
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import health as H  # noqa: E402

NOW = datetime(2026, 10, 16, 12, 0)
MET2 = "Dataset116_METrq1/nnUNetTrainer_save10/2"
MEN0 = "Dataset111_MENrq1/nnUNetTrainer_save10_n470/0"
fails = 0
time_orig = time.time


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def pt(dt):  # pueue 的时间格式
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000000000+08:00")


def fold(final=False, epochs=100, end=NOW):
    return dict(final=final, runs=[dict(start=ts(end - timedelta(hours=5)), end=ts(end), epochs_done=epochs, tl=[])], dice={}, debug={})


def task(i, group, status, cmd, label="", start=NOW - timedelta(hours=3), end=None):
    return dict(id=i, group=group, status=status, label=label, deps=[], start=pt(start) if start else None,
                end=pt(end) if end else None, command=cmd)


LAB_BUSY = {"groups": {}, "tasks": [dict(id=90, group="gpu1", status="Running", label="busy_a", deps=[], start=None, end=None,
                                         command="bash scripts/queue/train_task.sh 101 2 nnUNetTrainer_save10"),
                                    dict(id=91, group="gpu1", status="Queued", label="busy_b", deps=[], start=None, end=None,
                                         command="bash scripts/queue/train_task.sh 101 3 nnUNetTrainer_save10")]}


def snap(**kw):
    s = dict(now=NOW, ok={"box": True, "lab": True, "dicc": True}, slurm={"dicc": dict(squeue=[], sacct=[])},
             probe={"box": {"DISK": str(int(500e9)), "BOOT": "2026-10-01 08:00:00|"},
                    "lab": {"DISK": str(int(200e9)), "GPULOG": ts(NOW - timedelta(minutes=1)), "BOOT": "2026-10-01 08:00:00|",
                            "GITOK": str(int((NOW - timedelta(minutes=20)).timestamp())), "GITLOG": ""}},
             src={"box": {"folds": {}}, "lab": {"folds": {}}, "dicc": {"folds": {}}},
             pueue={"box": {"groups": {}, "tasks": []}, "lab": LAB_BUSY})
    s.update(kw)
    return s


def sq(*rows):
    return {"dicc": dict(squeue=[list(r) for r in rows], sacct=[])}


def ids(s):
    return set(H.check(s))


# ---------------- 检查 ----------------
ok("一切正常时没有问题", ids(snap()) == set())

s = snap(pueue={"box": {"groups": {}, "tasks": [
    task(1, "gpu", "Failed (1)", "bash x", "infer_a", end=NOW - timedelta(hours=1)),
    task(2, "cpu", "DependencyFailed", "python y", "oof_a", end=NOW - timedelta(hours=1)),
    task(3, "gpu", "Failed (1)", "bash z", "old", end=NOW - timedelta(hours=30)),
    task(4, "cpu", "Failed (143)", "bash w", "stopped", end=NOW - timedelta(hours=1))]},
    "lab": {"groups": {}, "tasks": [task(5, "gpu1", "Failed (75)", "bash scripts/queue/train_task.sh 101 0 nnUNetTrainer_UMambaEnc --epochs 50", "umamba_f0", end=NOW - timedelta(hours=2))]}})
P = H.check(s)
ok("任务失败、依赖失败都报", any(p.startswith("pueue:box:1:") for p in P) and any(p.startswith("pueue:box:2:") for p in P))
ok("24 小时前的失败不再报", not any(p.startswith("pueue:box:3:") for p in P))
ok("手动停的（143）只记不推", [p["level"] for k, p in P.items() if k.startswith("pueue:box:4:")] == ["info"])
ok("训满 N 个 epoch 停的（75）不报", not any(k.startswith("pueue:lab") for k in P))

s = snap(slurm=sq(["rq1_t3met_f2", "486189_1", "RUNNING", "3:00:00", "None"]),
         src={"box": {"folds": {}}, "lab": {"folds": {}}, "dicc": {"folds": {MET2: fold(end=NOW - timedelta(hours=2))}}})
ok("DICC 在跑但 90 分钟没有新 epoch → 卡住", "stuck:dicc:486189_1" in ids(s))
s["slurm"]["dicc"]["squeue"][0][3] = "40:00"
ok("刚开始不到 1 小时的不算卡住", "stuck:dicc:486189_1" not in ids(s))
s = snap(slurm=sq(["rq1_t3met_f2", "486189_2", "PENDING", "0:00", "Priority"]),
         src={"box": {"folds": {}}, "lab": {"folds": {}}, "dicc": {"folds": {MET2: fold(end=NOW - timedelta(hours=2))}}})
s = snap(slurm={"dicc": dict(squeue=[], sacct=[["486300_1", "rq1_t14met_f0", "FAILED", "2026-10-16T11:00:00", "1:0"]])})
ok("DICC 作业失败", "dicc:486300_1" in ids(s))

lab_train = task(7, "gpu1", "Running", "bash scripts/queue/train_task.sh 111 0 nnUNetTrainer_save10_n470", "t15men_f0")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_train]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "lab": {"folds": {MEN0: fold(end=NOW - timedelta(hours=2))}}})
ok("lab 训练 90 分钟没有新 epoch → 卡住", "stuck:lab:7" in ids(s))
s["src"]["lab"]["folds"][MEN0] = fold(end=NOW - timedelta(minutes=5))
ok("lab 训练正常时不报", "stuck:lab:7" not in ids(s))

fetch = task(9, "wait", "Running", "bash scripts/queue/fetch_fold.sh dicc Dataset116_METrq1/nnUNetTrainer_save10__nnUNetResEncUNetMPlans__3d_fullres 2", "fetch_t3met_f2")
s = snap(pueue={"box": {"groups": {}, "tasks": [fetch]}, "lab": {"groups": {}, "tasks": []}})
ok("在等的折不在任何地方排队 → 报", f"orphan:{MET2}" in ids(s))
s["slurm"] = sq(["rq1_t3met_f2", "486189_2", "PENDING", "0:00", "Priority"])
ok("在等的折在 DICC 排着 → 不报", f"orphan:{MET2}" not in ids(s))
s["slurm"], s["ok"]["dicc"] = {}, False
ok("DICC 连不上时不判断是否在排队", f"orphan:{MET2}" not in ids(s) and "down:dicc" in ids(s))
s = snap(pueue={"box": {"groups": {}, "tasks": [fetch]}, "lab": {"groups": {}, "tasks": []}},
         src={"box": {"folds": {}}, "lab": {"folds": {}}, "dicc": {"folds": {MET2: fold(final=True, epochs=1000, end=NOW - timedelta(hours=3))}}})
ok("训完 90 分钟还没拉回 → 报", f"fetch:{MET2}" in ids(s))

s = snap(slurm=sq(["rq1_t15men_f0", "486202_1", "PENDING", "0:00", "Priority"]),
         pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_train]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "lab": {"folds": {MEN0: fold(end=NOW - timedelta(minutes=5))}}})
P = H.check(s)
ok("同一折 DICC 排队 + lab 在训 → 报（不紧急）", P.get(f"dup:{MEN0}", {}).get("level") == "normal")
s["slurm"]["dicc"]["squeue"][0][2:4] = ["RUNNING", "2:00:00"]
s["src"]["dicc"]["folds"][MEN0] = fold(end=NOW - timedelta(minutes=5))
ok("同一折两边都在训 → 紧急", H.check(s).get(f"dup:{MEN0}", {}).get("level") == "urgent")

# ---------------- DICC 任务队列（10/17）+ lab 领的折（10/18）----------------
def tqrow(key, state, worker=None, since=None, log_age=None, kind="nnunet"):
    return dict(key=key, state=state, worker=worker, host=worker, since=since, beat=None, note="", log_age=log_age, failures=0,
                kind=kind, nice=0, order=1)


old = NOW.timestamp() - 3 * 3600  # 领了 3 小时
lab_men = task(7, "gpu1", "Running", "bash scripts/queue/train_task.sh 111 0 nnUNetTrainer_save10_n470", "t15men_f0")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_men]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "lab": {"folds": {MEN0: fold(end=NOW - timedelta(minutes=5))}}},
         taskq={"dicc": dict(enabled=True, paused=False, tasks=[tqrow(MEN0, "claimed", "lab", since=old)])})
H.time.time = lambda: NOW.timestamp()
P = H.check(s)
ok("lab 领的折在 lab 上训：不算同一折两处，也不按 DICC 的日志判断卡住", f"dup:{MEN0}" not in P and f"stuck:dicc:{MEN0}" not in P)
s["taskq"]["dicc"]["tasks"] = [tqrow(MEN0, "claimed", "486191_2", since=old, log_age=60)]
s["slurm"] = sq(["rq1_t3met_f2", "486191_2", "RUNNING", "3:00:00", "None", "0"])
ok("DICC 作业领了、lab 也在训同一折 → 紧急", H.check(s).get(f"dup:{MEN0}", {}).get("level") == "urgent")
s["taskq"]["dicc"]["tasks"] = [tqrow(MET2, "claimed", "lab", since=old)]
s["pueue"]["lab"]["tasks"] = []
ok("在跑的 DICC 作业没领到任务：lab 领的不算 DICC 作业", "tqidle:dicc" in H.check(s))
s["taskq"]["dicc"]["tasks"] = [tqrow(MET2, "free", None, since=old)]
s["slurm"] = sq()
ok("队列里有没人训的任务、DICC 上一个作业都没有：提醒（补作业的开关）", "taskq/DICC" in H.check(s).get("tqorphan:dicc", {}).get("msg", ""))
s["slurm"] = sq(["rq1_swin_v2_f0", "486300_1", "PENDING", "0:00", "Priority", "2000"])
ok("还有排着的作业（什么模型的都行）：不提醒", "tqorphan:dicc" not in H.check(s))
s["slurm"] = sq(["train_worker", "487000_1", "PENDING", "0:00", "Priority", "0"])
ok("排着不带折的 train_worker 作业也算：不提醒", "tqorphan:dicc" not in H.check(s))
s["taskq"]["dicc"]["tasks"] = [tqrow(MET2, "claimed", "lab", since=old)]
s["slurm"] = sq(["train_worker", "487000_1", "RUNNING", "3:00:00", "None", "0"])
ok("在跑的 train_worker 作业没领到任务：报有卡空着", "tqidle:dicc" in H.check(s))
H.time.time = time_orig

# ---------------- 通用的任务（10/18）：run_task.sh、fetch_task.sh、数据清单 ----------------
ok("run_task.sh 的训练命令 → key", H.task_key("bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 "
                                              "trainer=nnUNetTrainer_save10_n470 fold=0") == ("train", MEN0))
ok("q 加的、整条命令包在引号里的 run_task.sh 也认", H.task_key("bash -c 'bash scripts/queue/run_task.sh nnunet "
                                                           "dataset=Dataset116_METrq1 fold=2'") == ("train", MET2))
ok("command 做法的任务 → 它自己给的 key", H.task_key("bash scripts/queue/run_task.sh command key=x/y cmd=true env=nnunet "
                                                     "out=runs/x done=ok") == ("train", "x/y"))
ok("10/18 前的 train_task.sh 照样认", H.task_key("bash scripts/queue/train_task.sh 111 0 nnUNetTrainer_save10_n470") == ("train", MEN0))
ok("参数写错的 run_task.sh 不当成训练", H.task_key("bash scripts/queue/run_task.sh nnunet datset=x fold=0") == (None, None))
ok("fetch_task.sh <机器> <key> → 拉回", H.task_key(f"bash scripts/queue/fetch_task.sh dicc {MET2}") == ("fetch:dicc", MET2))
lab_rt = task(7, "gpu1", "Running", "bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=0",
              "train:" + MEN0)
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_rt]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "lab": {"folds": {MEN0: fold(end=NOW - timedelta(hours=2))}}})
ok("lab 上 run_task.sh 的训练 90 分钟没有新 epoch → 卡住", "stuck:lab:7" in ids(s))
cand = H.data_candidates([dict(key="x/y", spec=dict(data=["other/stuff"]))])
ok("要查的数据：实验表里的 + 队列里临时任务的", "nnUNet/preprocessed/Dataset111_MENrq1" in cand and "other/stuff" in cand)
ps = H.probe_script(H.M.MACHINES["lab"], ["nnUNet/preprocessed/Dataset111_MENrq1", "a b"])
ok("训练机的探查脚本按清单查数据（HAVE），带空格的路径加引号", "HAVE|" in ps and "'a b'" in ps and "PREP" not in ps)
ok("非训练机不查数据", "HAVE" not in H.probe_script(H.M.MACHINES["box"], ["x"]))
import subprocess  # noqa: E402
d = tempfile.mkdtemp()
for p_ in ("nnUNet/preprocessed/A", "nnUNet/preprocessed/B", "C"):
    os.makedirs(os.path.join(d, p_))
open(os.path.join(d, "nnUNet/preprocessed/B/.incomplete"), "w").close()
fake = dict(H.M.MACHINES["lab"], data=d, disk=None, gpu_log=False, roles=("train",))
out = subprocess.run(["bash", "-s"], input=H.probe_script(fake, ["nnUNet/preprocessed/A", "nnUNet/preprocessed/B", "C", "D"]),
                     capture_output=True, text=True).stdout
have = next(ln for ln in out.splitlines() if ln.startswith("HAVE|"))[5:].split()
ok("HAVE 实跑：在的列出来，没传完（.incomplete）和不在的不列", have == ["nnUNet/preprocessed/A", "C"])

s = snap()
s["probe"]["lab"]["DISK"] = str(int(10e9))
s["probe"]["box"]["DISK"] = str(int(50e9))
P = H.check(s)
ok("lab 盘不到 15 GB → 紧急；box 盘不到 100 GB → 提醒", P["disk:lab"]["level"] == "urgent" and P["disk:box"]["level"] == "normal")
s["probe"]["lab"]["GPULOG"] = ts(NOW - timedelta(minutes=30))
s["probe"]["lab"]["GITOK"] = str(int((NOW - timedelta(hours=3)).timestamp()))
ok("GPU 记录停了、代码没同步 → 报（只查表里有的机器）", {"gpulog:lab", "git:lab"} <= ids(s) and "git:box" not in ids(s))

s = snap(board_pending=["data/rq1_v50.json"])
s["probe"]["box"]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 开机：放回队列 3 4"
P = H.check(s)
ok("box 重启过 → 报（带上放回的任务），lab 很久前重启的不报", [p["msg"] for k, p in P.items() if k.startswith("boot:")] == ["box 10-16 10:00 重启过：开机：放回队列 3 4"])
ok("看板有结果等确认 → 报", "board" in P)

s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [
    dict(id=1, group="gpu1", status="Stashed", label="umamba_f0", deps=[], start=None, end=None, command="bash scripts/queue/train_task.sh 101 0 nnUNetTrainer_UMambaEnc")]}})
s["pueue"]["lab"]["tasks"] = s["pueue"]["lab"]["tasks"][:1]
ok("lab 1 号卡没有训练任务（暂存的不算）→ 报", "idle:lab:gpu1" in ids(s))
s2 = dict(s, taskq={"dicc": dict(enabled=True, paused=False, tasks=[tqrow(MET2, "claimed", "lab")])})
ok("lab 领着折（在传数据或停了等人处理，refill.py 另外报）：卡空着不另报", "idle:lab:gpu1" not in ids(s2))
s2["taskq"]["dicc"]["tasks"] = [tqrow(MET2, "done", "lab")]
ok("lab 领着的折已训完（等交还）：卡空着照报", "idle:lab:gpu1" in ids(s2))
s["pueue"]["lab"]["tasks"].append(dict(id=2, group="gpu1", status="Running", label="t15men_f4", deps=[], start=None, end=None,
                                       command="bash scripts/queue/train_task.sh 111 4 nnUNetTrainer_save10_n470"))
P = H.check(s)
ok("只剩在训的一个 → 只记（训完后自动领下一折），不推送", "只剩在训的 t15men_f4" in P.get("idle:lab:gpu1", {}).get("msg", "")
   and P["idle:lab:gpu1"]["level"] == "info")
ok("还有排着的 → 不报", "idle:lab:gpu1" not in ids(snap()))

# ---------------- 第四台机器：只在表里加一项 ----------------
H.M.MACHINES["fake"] = dict(label="fake", hostname=r"fake-host", ssh="fake", scheduler="pueue", data="~/BT", conda=("~/c", "e"),
                            groups={"gpu0": (1, "0", {})}, roles=("train", "autopull"), disk=("/data", 50, 10), gpu_log=False)
fk = task(3, "gpu0", "Running", "bash scripts/queue/train_task.sh 111 0 nnUNetTrainer_save10_n470", "t15men_f0_fake")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_train]}, "fake": {"groups": {}, "tasks": [fk]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "fake": {"folds": {}},
              "lab": {"folds": {MEN0: fold(end=NOW - timedelta(minutes=5))}}})
s["ok"]["fake"] = True
s["probe"]["fake"] = {"DISK": str(int(5e9)), "BOOT": "2026-10-01 08:00:00|", "GITOK": "", "GITLOG": ""}
P = H.check(s)
ok("第四台机器：同一折和 lab 两边都在训 → 紧急", P.get(f"dup:{MEN0}", {}).get("level") == "urgent")
ok("第四台机器：盘、代码同步按表里的字段查", P["disk:fake"]["level"] == "urgent" and "git:fake" in P and "gpulog:fake" not in P)
s["ok"]["fake"] = False
ok("第四台机器连不上：报", "down:fake" in ids(s))
del H.M.MACHINES["fake"]

# ---------------- 推送规则（10/21：白话、并成一条、不推已恢复和汇总）----------------
d = tempfile.mkdtemp()
H.STATE, H.LOG = f"{d}/state.json", f"{d}/health.log"
sent = []
H.push = lambda msg, **kw: sent.append((kw.get("title", ""), msg))


def run(P, now):
    sent.clear()
    H.update({k: dict(v) for k, v in P.items()}, now)
    return list(sent)


A = {"disk:lab": dict(msg="lab 盘", level="normal", kind="state", after=0)}
U = {"dup:x": dict(msg="两处", level="urgent", kind="state", after=0)}
day1 = datetime(2026, 10, 16, 12, 0)
out = run(A, day1)
ok("白天新问题：推送一条白话", len(out) == 1 and out[0][0] == "PhD：需要处理" and "盘满了" in out[0][1])
ok("同一个问题不重复推送", run(A, day1 + timedelta(minutes=30)) == [])
ok("恢复后不推送", run({}, day1 + timedelta(hours=1)) == [])
ok("夜里普通问题不推送", run(A, datetime(2026, 10, 16, 23, 0)) == [])
out = run({**A, **U}, datetime(2026, 10, 17, 1, 0))
ok("夜里紧急问题即时推送", len(out) == 1 and out[0][0] == "PhD：紧急" and "两处训练" in out[0][1])
out = run({**A, **U}, datetime(2026, 10, 17, 8, 5))
ok("早上第一次检查推送夜里攒下的", len(out) == 1 and "盘满了" in out[0][1] and "两处" not in out[0][1])
ok("推过之后不再推送", run(A, datetime(2026, 10, 17, 8, 35)) == [])
D = {"down:dicc": dict(msg="DICC 连不上", level="normal", kind="state", after=60)}
ok("要持续一段时间才报的，刚出现时不推送", run({**A, **D}, datetime(2026, 10, 17, 9, 0)) == [])
out = run({**A, **D}, datetime(2026, 10, 17, 10, 0))
ok("持续够久后推送", len(out) == 1 and "DICC 连不上了" in out[0][1])
B = {"git:lab": dict(msg="g", level="normal", kind="state", after=0), "gpulog:lab": dict(msg="l", level="normal", kind="state", after=0)}
out = run(B, datetime(2026, 10, 17, 10, 30))
ok("同一次查到几件：并成一条", len(out) == 1 and "（2 件）" in out[0][0] and out[0][1].count("\n- ") == 1)
I = {"idle:lab:gpu1": dict(msg="只剩在训的", level="info", kind="state", after=0)}
ok("info 级不推送", run(I, datetime(2026, 10, 17, 11, 0)) == [])

# ---------------- refill / advance 出错不让健康检查停掉 ----------------
import types  # noqa: E402
got = {}
H.gather, H.update = (lambda: snap()), (lambda P, now, dry=False: got.update(P))
sys.modules["refill"] = None  # import 出错
adv = types.ModuleType("advance")
adv.run = lambda s, dry=False: 1 / 0
sys.modules["advance"] = adv
sys.argv = ["health.py"]
H.main()
ok("refill.py import 出错、advance.py 运行出错：都报出来，健康检查照常写完",
   "refill:error" in got and "advance:error" in got and "ZeroDivisionError" in got["advance:error"]["msg"])
rf = types.ModuleType("refill")
rf.run_all = lambda s, dry=False: (["lab 领了 x", "lab 交还 y"], {}, set())
sys.modules["refill"] = rf
got.clear()
H.main()
ok("lab 领了 / 交还了：只记 refill.log，不进推送", not any(k.startswith("refill:") for k in got))

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
