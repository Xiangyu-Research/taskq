#!/usr/bin/env python3
"""测 health.py 的检查和推送规则（2026-10-16）：假的队列、训练日志、DICC 输出、时钟，不连任何机器、不真的推送。

    python3 scripts/monitor/test_health.py
"""
import os
import sys
import tempfile
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import health as H  # noqa: E402

NOW = datetime(2026, 10, 16, 12, 0)
MET2 = "Dataset116_METrq1/nnUNetTrainer_save10/2"
MEN0 = "Dataset111_MENrq1/nnUNetTrainer_save10_n470/0"
fails = 0


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


def snap(**kw):
    s = dict(now=NOW, ok={"box": True, "lab": True, "dicc": True}, slurm={"dicc": dict(squeue=[], sacct=[])},
             probe={"box": {"DISK": str(int(500e9)), "BOOT": "2026-10-01 08:00:00|"},
                    "lab": {"DISK": str(int(200e9)), "GPULOG": ts(NOW - timedelta(minutes=1)), "BOOT": "2026-10-01 08:00:00|",
                            "GITOK": str(int((NOW - timedelta(minutes=20)).timestamp())), "GITLOG": ""}},
             src={"box": {"folds": {}}, "lab": {"folds": {}}, "dicc": {"folds": {}}},
             pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": []}})
    s.update(kw)
    return s


def sq(*rows):
    return {"dicc": dict(squeue=[list(r) for r in rows], sacct=[])}


def ids(s):
    return set(H.check(s)[0])


# ---------------- 检查 ----------------
ok("一切正常时没有问题", ids(snap()) == set())

s = snap(pueue={"box": {"groups": {}, "tasks": [
    task(1, "gpu", "Failed (1)", "bash x", "infer_a", end=NOW - timedelta(hours=1)),
    task(2, "cpu", "DependencyFailed", "python y", "oof_a", end=NOW - timedelta(hours=1)),
    task(3, "gpu", "Failed (1)", "bash z", "old", end=NOW - timedelta(hours=30)),
    task(4, "cpu", "Failed (143)", "bash w", "stopped", end=NOW - timedelta(hours=1))]},
    "lab": {"groups": {}, "tasks": [task(5, "gpu1", "Failed (75)", "bash scripts/queue/train_task.sh 101 0 nnUNetTrainer_UMambaEnc --epochs 50", "umamba_f0", end=NOW - timedelta(hours=2))]}})
P, claims = H.check(s)
ok("任务失败、依赖失败都报", any(p.startswith("pueue:box:1:") for p in P) and any(p.startswith("pueue:box:2:") for p in P))
ok("24 小时前的失败不再报", not any(p.startswith("pueue:box:3:") for p in P))
ok("手动停的（143）只记不推", [p["level"] for k, p in P.items() if k.startswith("pueue:box:4:")] == ["info"])
ok("训满 N 个 epoch 停的（75）不报，也不算在训", not any(k.startswith("pueue:lab") for k in P)
   and not any(k == "Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0" for k, _ in claims["dicc"]))

s = snap(slurm=sq(["rq1_t3met_f2", "486189_1", "RUNNING", "3:00:00", "None"]),
         src={"box": {"folds": {}}, "lab": {"folds": {}}, "dicc": {"folds": {MET2: fold(end=NOW - timedelta(hours=2))}}})
ok("DICC 在跑但 90 分钟没有新 epoch → 卡住", "stuck:dicc:486189_1" in ids(s))
s["slurm"]["dicc"]["squeue"][0][3] = "40:00"
ok("刚开始不到 1 小时的不算卡住", "stuck:dicc:486189_1" not in ids(s))
s = snap(slurm=sq(["rq1_t3met_f2", "486189_2", "PENDING", "0:00", "Priority"]),
         src={"box": {"folds": {}}, "lab": {"folds": {}}, "dicc": {"folds": {MET2: fold(end=NOW - timedelta(hours=2))}}})
ok("DICC 排队的折也写给 lab（要在 lab 训先取消 DICC 作业）", (MET2, "DICC") in H.check(s)[1]["lab"])
ok("DICC 自己的清单里没有自己排着的折", H.check(s)[1]["dicc"] == [])
s = snap(slurm={"dicc": dict(squeue=[], sacct=[["486300_1", "rq1_t14met_f0", "FAILED", "2026-10-16T11:00:00", "1:0"]])})
ok("DICC 作业失败", "dicc:486300_1" in ids(s))

lab_train = task(7, "gpu1", "Running", "bash scripts/queue/train_task.sh 111 0 nnUNetTrainer_save10_n470", "t15men_f0")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_train]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "lab": {"folds": {MEN0: fold(end=NOW - timedelta(hours=2))}}})
ok("lab 训练 90 分钟没有新 epoch → 卡住", "stuck:lab:7" in ids(s))
ok("lab 在训的折写给 DICC", (MEN0, "lab") in H.check(s)[1]["dicc"])
s["src"]["lab"]["folds"][MEN0] = fold(end=NOW - timedelta(minutes=5))
ok("lab 训练正常时不报", "stuck:lab:7" not in ids(s))

fetch = task(9, "wait", "Running", "bash scripts/queue/fetch_fold.sh dicc Dataset116_METrq1/nnUNetTrainer_save10__nnUNetResEncUNetMPlans__3d_fullres 2", "fetch_t3met_f2")
s = snap(pueue={"box": {"groups": {}, "tasks": [fetch]}, "lab": {"groups": {}, "tasks": []}})
ok("在等的折不在任何地方排队 → 报", f"orphan:{MET2}" in ids(s))
s["slurm"] = sq(["rq1_t3met_f2", "486189_2", "PENDING", "0:00", "Priority"])
ok("在等的折在 DICC 排着 → 不报", f"orphan:{MET2}" not in ids(s))
s["slurm"], s["ok"]["dicc"] = {}, False
ok("DICC 连不上时不判断是否在排队，也不写清单", f"orphan:{MET2}" not in ids(s) and "down:dicc" in ids(s) and H.check(s)[1] == {})
s = snap(pueue={"box": {"groups": {}, "tasks": [fetch]}, "lab": {"groups": {}, "tasks": []}},
         src={"box": {"folds": {}}, "lab": {"folds": {}}, "dicc": {"folds": {MET2: fold(final=True, epochs=1000, end=NOW - timedelta(hours=3))}}})
ok("训完 90 分钟还没拉回 → 报", f"fetch:{MET2}" in ids(s))

s = snap(slurm=sq(["rq1_t15men_f0", "486202_1", "PENDING", "0:00", "Priority"]),
         pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_train]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "lab": {"folds": {MEN0: fold(end=NOW - timedelta(minutes=5))}}})
P = H.check(s)[0]
ok("同一折 DICC 排队 + lab 在训 → 报（不紧急）", P.get(f"dup:{MEN0}", {}).get("level") == "normal")
s["slurm"]["dicc"]["squeue"][0][2:4] = ["RUNNING", "2:00:00"]
s["src"]["dicc"]["folds"][MEN0] = fold(end=NOW - timedelta(minutes=5))
ok("同一折两边都在训 → 紧急", H.check(s)[0].get(f"dup:{MEN0}", {}).get("level") == "urgent")

s = snap()
s["probe"]["lab"]["DISK"] = str(int(10e9))
s["probe"]["box"]["DISK"] = str(int(50e9))
P = H.check(s)[0]
ok("lab 盘不到 15 GB → 紧急；box 盘不到 100 GB → 提醒", P["disk:lab"]["level"] == "urgent" and P["disk:box"]["level"] == "normal")
s["probe"]["lab"]["GPULOG"] = ts(NOW - timedelta(minutes=30))
s["probe"]["lab"]["GITOK"] = str(int((NOW - timedelta(hours=3)).timestamp()))
ok("GPU 记录停了、代码没同步 → 报（只查表里有的机器）", {"gpulog:lab", "git:lab"} <= ids(s) and "git:box" not in ids(s))

s = snap(board_pending=["data/rq1_v50.json"])
s["probe"]["box"]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 开机：放回队列 3 4"
P = H.check(s)[0]
ok("box 重启过 → 报（带上放回的任务），lab 很久前重启的不报", [p["msg"] for k, p in P.items() if k.startswith("boot:")] == ["box 10-16 10:00 重启过：开机：放回队列 3 4"])
ok("看板有结果等确认 → 报", "board" in P)

# ---------------- 第四台机器：只在表里加一项 ----------------
H.M.MACHINES["fake"] = dict(label="fake", hostname=r"fake-host", ssh="fake", scheduler="pueue", data="~/BT", conda=("~/c", "e"),
                            groups={"gpu0": (1, "0", {})}, roles=("train", "autopull"), disk=("/data", 50, 10), gpu_log=False)
fk = task(3, "gpu0", "Running", "bash scripts/queue/train_task.sh 111 0 nnUNetTrainer_save10_n470", "t15men_f0_fake")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_train]}, "fake": {"groups": {}, "tasks": [fk]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "fake": {"folds": {}},
              "lab": {"folds": {MEN0: fold(end=NOW - timedelta(minutes=5))}}})
s["ok"]["fake"] = True
s["probe"]["fake"] = {"DISK": str(int(5e9)), "BOOT": "2026-10-01 08:00:00|", "GITOK": "", "GITLOG": ""}
P, claims = H.check(s)
ok("第四台机器：同一折和 lab 两边都在训 → 紧急", P.get(f"dup:{MEN0}", {}).get("level") == "urgent")
ok("第四台机器：清单互相写、DICC 收到两台的", (MEN0, "lab") in claims["fake"] and (MEN0, "fake") in claims["lab"]
   and {(MEN0, "lab"), (MEN0, "fake")} <= set(claims["dicc"]))
ok("第四台机器：盘、代码同步按表里的字段查", P["disk:fake"]["level"] == "urgent" and "git:fake" in P and "gpulog:fake" not in P)
s["ok"]["fake"] = False
ok("第四台机器连不上：报，且不写清单", "down:fake" in ids(s) and H.check(s)[1] == {})
del H.M.MACHINES["fake"]

# ---------------- 推送规则 ----------------
d = tempfile.mkdtemp()
H.STATE, H.LOG, H.PRUNE_LOG = f"{d}/state.json", f"{d}/health.log", f"{d}/prune.log"
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
ok("当天第一次白天检查：新问题并进每日汇总", len(out) == 1 and "每日汇总" in out[0][0] and "lab 盘" in out[0][1])
ok("同一个问题不重复推送", run(A, day1 + timedelta(minutes=30)) == [])
out = run({}, day1 + timedelta(hours=1))
ok("恢复后推送一次已恢复", len(out) == 1 and "已恢复" in out[0][1])
out = run(A, datetime(2026, 10, 16, 23, 0))
ok("夜里普通问题不推送", out == [])
out = run({**A, **U}, datetime(2026, 10, 17, 1, 0))
ok("夜里紧急问题即时推送", [m for _, m in out] == ["两处"])
out = run(A, datetime(2026, 10, 17, 8, 0))
ok("早上汇总带上夜里的普通问题和已恢复的", len(out) == 1 and "lab 盘" in out[0][1] and "已恢复：两处" in out[0][1])
ok("汇总之后不再单独推送", run(A, datetime(2026, 10, 17, 8, 30)) == [])
D = {"down:dicc": dict(msg="DICC 连不上", level="normal", kind="state", after=60)}
ok("要持续一段时间才报的，刚出现时不推送", run({**A, **D}, datetime(2026, 10, 17, 9, 0)) == [])
ok("持续够久后推送", [m for _, m in run({**A, **D}, datetime(2026, 10, 17, 10, 0))] == ["DICC 连不上"])
E = {"pueue:box:1:t": dict(msg="任务失败", level="normal", kind="event", after=0)}
run(E, datetime(2026, 10, 17, 11, 0))
ok("事件类消失时不推送已恢复", run({}, datetime(2026, 10, 17, 11, 30)) == [] or
   all("任务失败" not in m for _, m in sent))

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
