#!/usr/bin/env python3
"""测 refill.py（2026-10-18，lab 从 DICC 的任务队列领任务）：假的队列状态、pueue、训练进度，命令不真的执行（记下来核对）。

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
R.LOG, R.STATE, R.SWITCH, R.DICC_SWITCH = f"{D}/refill.log", f"{D}/refill.json", f"{D}/LAB", f"{D}/DICC"
R.sh = lambda cmd, host=None, timeout=0: (1, "测试里不连 DICC")  # run_all 里补作业那一步
GB = {"Dataset103_GLIrq1_t1c": 4.0, "Dataset121_GLIMETrq1": 62.0, "stuff": 1.0}
R.data_gb = lambda paths: sum(GB.get(R.tag(p), 10.0) for p in paths)
NN, UM = "nnUNetTrainer_save10", "nnUNetTrainer_UMambaEnc"
DS = {101: "Dataset101_GLIrq1", 103: "Dataset103_GLIrq1_t1c", 111: "Dataset111_MENrq1"}
PP = "nnUNet/preprocessed/"


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def task(i, group, status, cmd, label=""):
    return dict(id=i, group=group, status=status, label=label, deps=[], start=None, end=None, command=cmd, priority=0)


def train(i, ds, f, status="Running", tr=""):
    extra = f" trainer={tr}" + (" env=umamba" if tr == UM else "") if tr else ""
    return task(i, "gpu1", status, f"bash scripts/queue/run_task.sh nnunet dataset={DS[ds]} fold={f}{extra}", f"x{i}")


def tq(key, nice, order, state="free", worker=None, skipped_by=()):
    return dict(key=key, nice=nice, order=order, state=state, worker=worker, kind="-", host=worker, skipped_by=list(skipped_by))


K_T14 = f"Dataset121_GLIMETrq1/{NN}/0"   # ① 但数据集 62 GB 放不下
K_T1C = [f"Dataset103_GLIrq1_t1c/{NN}/{f}" for f in range(3)]
K_UM = f"Dataset101_GLIrq1/{UM}/1"
QUEUE = [tq(K_T14, 0, 1), tq(K_UM, 50, 2), tq(K_T1C[0], 50, 3), tq(K_T1C[1], 50, 4), tq(K_T1C[2], 100, 5)]
SQ = [["rq1_t1c_f0", "485145_1", "PENDING", "0:00", "Priority", "50"]]  # DICC 上只有 nnunet 的作业


def snap(lab_tasks=(), queue=QUEUE, prep=f"{PP}Dataset101_GLIrq1 {PP}Dataset111_MENrq1", disk=56e9, box_tasks=(), lab_folds=None, enabled=True, skips=None):
    return dict(ok={"box": True, "lab": True, "dicc": True}, slurm={"dicc": dict(squeue=copy.deepcopy(SQ), sacct=[])},
                probe={"lab": {"DISK": str(int(disk)), "HAVE": prep}},
                src={"box": {"folds": {}}, "lab": {"folds": lab_folds or {}}, "dicc": {"folds": {}}},
                taskq={"dicc": dict(enabled=enabled, tasks=copy.deepcopy(list(queue)), recent_skips=skips or {})},
                pueue={"box": {"groups": {}, "tasks": list(box_tasks)}, "lab": {"groups": {}, "tasks": list(lab_tasks)}})


def ops(acts):
    return [(a["op"], a["key"]) for a in acts]


# ---------------- 领 ----------------
acts, _, why = R.plan(snap([train(1, 111, 0)]), "lab")
ok("卡上有训练任务：不领", acts == [] and "训练任务" in why)
acts, _, _ = R.plan(snap(), "lab")
ok("卡空：①档放不下就跳过，按（档位，先后）取", ops(acts) == [("take", K_UM)] and not acts[0]["paths"])
qf = [dict(t, skipped_by=["lab"]) if t["key"] == K_UM else t for t in QUEUE]
ok("lab 跳过的（多半放不下）不再领；别的机器跳过的照领",
   ops(R.plan(snap(queue=qf), "lab")[0]) == [("take", K_T1C[0])]
   and ops(R.plan(snap(queue=[dict(t, skipped_by=["box2"]) for t in QUEUE]), "lab")[0]) == [("take", K_UM)])
acts, stuck, why = R.plan(snap(skips={"lab": 2}), "lab")
ok("24 小时内跳过 2 个：先不领、报出来", acts == [] and "labskip:lab" in stuck and "没开始就失败" in why)
s = snap()
del s["src"]["dicc"]
ok("DICC 的训练进度读不到：什么都不做", R.plan(s, "lab")[0] == [] and R.plan(s, "lab")[2] == "连不上")
q2 = [t for t in QUEUE if t["key"] != K_UM]
acts, _, _ = R.plan(snap(queue=q2), "lab")
ok("数据集不在 lab 上要先传", ops(acts) == [("take", K_T1C[0])] and acts[0]["paths"] == [PP + "Dataset103_GLIrq1_t1c"])
acts, _, why = R.plan(snap(queue=q2, disk=43e9), "lab")
ok("盘不够：都跳过，说明原因", acts == [] and "放不下" in why)
s = snap(queue=q2)
s["src"]["dicc"]["folds"][K_T1C[0]] = dict(final=False, runs=[dict(start="", end="2026-10-16 10:00:00", epochs_done=120, tl=[])])
acts, _, _ = R.plan(s, "lab")
ok("有进度的折不领（留在原机器）", ops(acts) == [("take", K_T1C[1])])
q3 = [dict(t, state="claimed", worker="486191_2") if t["key"] == K_UM else t for t in QUEUE]
ok("别人领了的不领", ops(R.plan(snap(queue=q3), "lab")[0]) == [("take", K_T1C[0])])
ok("队列没开 / 读不到：什么都不做", R.plan(snap(enabled=False), "lab")[0] == [] and R.plan(dict(snap(), taskq={"dicc": None}), "lab")[0] == [])
s = snap()
s["ok"]["lab"] = False
ok("lab 连不上：什么都不做（不续、不交还）", R.plan(s, "lab")[0] == [])
SQW = [["train_worker", "490000_1", "PENDING", "0:00", "Priority", "0"]]
s = snap(queue=q2[1:] + [tq(K_UM, 50, 9)])
s["slurm"]["dicc"]["squeue"] = SQW
ok("DICC 有不带折的 worker 作业（什么都能领）：只按先后", ops(R.plan(s, "lab")[0]) == [("take", K_T1C[0])])
s = snap()
del s["taskq"]["dicc"]["recent_skips"]
acts, _, why = R.plan(s, "lab")
ok("DICC 上的 taskq.py 还是旧版（状态里没有 recent_skips）：什么都不做", acts == [] and "旧版" in why)
ok("实验表里没有、队列状态里也没有说明的任务不领", ops(R.plan(snap(queue=[tq("Dataset999_X/nnUNetTrainer_save10/0", -1, 0)] + QUEUE), "lab")[0]) == [("take", K_UM)])
HD = dict(tq("hdbet/x", -1, 0), recipe="command", params={}, spec=dict(key="hdbet/x", env="hdbet", data=[]))
ok("lab 没有任务要的环境（hdbet）：不领", ops(R.plan(snap(queue=[HD] + QUEUE), "lab")[0]) == [("take", K_UM)])
ok("队列标成有问题（bad）的任务不领", ops(R.plan(snap(queue=[dict(t, bad="x") if t["key"] == K_UM else t for t in QUEUE]), "lab")[0]) == [("take", K_T1C[0])])
CMD = dict(tq("misc/a", -1, 0), recipe="command", params={"key": "misc/a", "cmd": "python x.py", "env": "nnunet"},
           spec=dict(key="misc/a", env="nnunet", data=["other/stuff"]))
acts, _, _ = R.plan(snap(queue=[CMD] + QUEUE), "lab")
ok("任意做法的任务：按说明里的 data 判断要传什么", ops(acts) == [("take", "misc/a")] and acts[0]["paths"] == ["other/stuff"])
ok("要传的数据集：再给训练输出留 10 GB（盘剩 52 GB、数据 4 GB、提醒线 40 GB：不领）",
   R.plan(snap(queue=q2, disk=52e9), "lab")[0] == [] and ops(R.plan(snap(queue=q2, disk=55e9), "lab")[0]) == [("take", K_T1C[0])])

# ---------------- 维护 lab 领的 ----------------
mine = [dict(t, state="claimed", worker="lab", since=None) if t["key"] == K_UM else t for t in QUEUE]
acts, _, _ = R.plan(snap([train(5, 101, 1, "Running", UM)], queue=mine), "lab")
ok("lab 在训：续上，不再领", ops(acts) == [("beat", K_UM)])
acts, _, _ = R.plan(snap([train(5, 101, 1, "Success", UM)], queue=mine, lab_folds={K_UM: dict(final=True)}), "lab")
ok("lab 训完、DICC 上还没有：继续续着（不交还），同时领下一折", ops(acts)[0] == ("keep", K_UM) and ops(acts)[1][0] == "take")
done = [dict(t, state="done") if t["key"] == K_UM else t for t in mine]
acts, _, _ = R.plan(snap([train(5, 101, 1, "Success", UM)], queue=done, lab_folds={K_UM: dict(final=True)}), "lab")
ok("备份到 DICC 后（队列显示训完）：交还", ops(acts)[0] == ("release", K_UM))
acts, _, _ = R.plan(snap([train(5, 101, 1, "Killed", UM)], queue=mine), "lab")
ok("任务没了、lab 上没进度：交还队列", ops(acts)[0] == ("release", K_UM) and "交还队列" in acts[0]["why"] and not acts[0].get("skip"))
acts, _, _ = R.plan(snap([train(5, 101, 1, "Failed (1)", UM)], queue=mine), "lab")
ok("训练失败、没进度：交还并记跳过（不记失败）", ops(acts)[0] == ("release", K_UM) and "Failed (1)" in acts[0]["skip"] and "failed" not in acts[0])
old = dict(train(5, 101, 1, "Failed (1)", UM), start="2026-10-10T10:00:00.000000000+08:00")
mine_new = [dict(t, since=1792303200) if t["key"] == K_UM else t for t in mine]  # 2026-10-18 领的
acts, _, _ = R.plan(snap([old], queue=mine_new, prep=PP + "Dataset101_GLIrq1"), "lab")
ok("领取之前的旧训练任务不算：当成刚领，开始训", ops(acts) == [("beat", K_UM), ("start", K_UM)])
acts, _, _ = R.plan(snap([dict(old, status="Running")], queue=mine_new, prep=PP + "Dataset101_GLIrq1"), "lab")
ok("领取之前开始、还活着的任务（比如恢复的）：算在训，只续，不重复开始", ops(acts) == [("beat", K_UM)])
acts, _, _ = R.plan(snap([train(5, 101, 1, "Failed (137)", UM)], queue=mine), "lab")
ok("被杀掉（Failed (137)）、没进度：交还队列，不记跳过", ops(acts)[0] == ("release", K_UM) and not acts[0].get("skip"))
prog = {K_UM: dict(final=False, runs=[dict(start="", end="2026-10-18 09:00:00", epochs_done=300, tl=[])])}
acts, stuck, _ = R.plan(snap([train(5, 101, 1, "Failed (1)", UM)], queue=mine, lab_folds=prog), "lab")
ok("任务失败、lab 上有进度：领取保留（续上）、报出来、不领新的", ops(acts) == [("beat", K_UM)] and f"labclaim:{K_UM}" in stuck
   and "run_task.sh nnunet dataset=Dataset101_GLIrq1 trainer=nnUNetTrainer_UMambaEnc env=umamba fold=1" in stuck[f"labclaim:{K_UM}"])
mine2 = [dict(t, state="claimed", worker="lab") if t["key"] == K_T1C[0] else t for t in QUEUE]
acts, _, why = R.plan(snap(queue=mine2), "lab")
ok("刚领、数据不在 lab 上：传数据，不再领", ops(acts) == [("beat", K_T1C[0]), ("transfer", K_T1C[0])])
sync = task(31, "wait", "Running", "bash scripts/sync/tunnel_push.sh lab …", "sync_lab_Dataset103_GLIrq1_t1c")
acts, _, _ = R.plan(snap(queue=mine2, box_tasks=[sync]), "lab")
ok("数据在传：续上，不重复传，不领", ops(acts) == [("beat", K_T1C[0])])
acts, stuck, _ = R.plan(snap(queue=mine2, box_tasks=[dict(sync, status="Failed (1)")]), "lab")
ok("传数据出错：不重传，报出来", ops(acts) == [("beat", K_T1C[0])] and "labsync:Dataset103_GLIrq1_t1c" in stuck)
acts, stuck, _ = R.plan(snap(queue=mine2, prep=f"{PP}Dataset101_GLIrq1 {PP}Dataset103_GLIrq1_t1c", box_tasks=[dict(sync, status="Failed (1)")]), "lab")
ok("传了一半出错（lab 上有这个文件夹）：也不开始训", ops(acts) == [("beat", K_T1C[0])] and "labsync:Dataset103_GLIrq1_t1c" in stuck)
acts, _, _ = R.plan(snap(queue=mine2, prep=f"{PP}Dataset101_GLIrq1 {PP}Dataset103_GLIrq1_t1c",
                         box_tasks=[dict(sync, status="Failed (1)"), dict(sync, id=32, status="Success")]), "lab")
ok("出错后重传成功了：看最新的那次，开始训", ops(acts) == [("beat", K_T1C[0]), ("start", K_T1C[0])])
acts, _, _ = R.plan(snap(queue=mine2, prep=f"{PP}Dataset101_GLIrq1 {PP}Dataset103_GLIrq1_t1c", box_tasks=[dict(sync, status="Success")]), "lab")
ok("数据传完了：开始训", ops(acts) == [("beat", K_T1C[0]), ("start", K_T1C[0])])

# ---------------- 执行 ----------------
cmds, rem = [], []


def fake_run(cmd, host=None, timeout=0):
    cmds.append((host or "box", cmd))
    return 0, str(100 + len(cmds))


def fake_remote(rc=0, out=""):
    def f(a):
        rem.append(a)
        return (rc, out)
    return f


acts, _, _ = R.plan(snap(), "lab")
msgs = R.execute(acts, "lab", remote=fake_remote(), run=fake_run)
ok("领：take --worker lab --ttl 86400，成功后在 lab gpu1 加训练任务（不暂存、不带优先级）",
   rem == [f"take --worker lab --ttl 86400 {K_UM}"] and len(cmds) == 1 and cmds[0][0] == "lab"
   and f"gpu1 -p --label train:{K_UM} -- 'bash scripts/queue/run_task.sh nnunet dataset=Dataset101_GLIrq1 trainer=nnUNetTrainer_UMambaEnc env=umamba fold=1'" in cmds[0][1]
   and "--stashed" not in cmds[0][1] and "priority" not in cmds[0][1] and "领了" in msgs[0])
cmds.clear(), rem.clear()
msgs = R.execute(acts, "lab", remote=fake_remote(10, "刚被别人领走"), run=fake_run)
ok("take 没成功（DICC 刚领走）：不加训练任务", cmds == [] and "没领到" in msgs[0])
got = set()
R.execute(acts, "lab", remote=fake_remote(10, "x"), run=fake_run, ok=got)
got2 = set()
R.execute(acts, "lab", remote=fake_remote(), run=fake_run, ok=got2)
ok("ok 只记下领到的（领不到时不算这台机器补上了）", got == set() and got2 == {K_UM})
cmds.clear(), rem.clear()
acts, _, _ = R.plan(snap(queue=q2), "lab")
R.execute(acts, "lab", remote=fake_remote(), run=fake_run)
ok("领了要传数据的：box 上加传数据任务（不预排训练），记下传过去的数据集",
   len(cmds) == 1 and cmds[0][0] == "box" and "sync_lab_Dataset103_GLIrq1_t1c" in cmds[0][1] and "tunnel_push.sh lab" in cmds[0][1]
   and PP + "Dataset103_GLIrq1_t1c" in open(R.STATE).read())
cmds.clear(), rem.clear()
acts, _, _ = R.plan(snap([train(5, 101, 1, "Killed", UM)], queue=mine), "lab")
R.execute(acts, "lab", remote=fake_remote(), run=fake_run)
ok("交还：release --worker lab", rem[0] == f"release --worker lab {K_UM}" and rem[1].startswith("take "))
cmds.clear(), rem.clear()
acts, _, _ = R.plan(snap([train(5, 101, 1, "Failed (1)", UM)], queue=mine), "lab")
R.execute(acts, "lab", remote=fake_remote(), run=fake_run)
ok("失败交还：release --skip '<原因>'（不写 failures/）", rem[0].startswith(f"release --worker lab {K_UM} --skip '训练任务 5 Failed (1)'"))
cmds.clear(), rem.clear()
acts, _, _ = R.plan(snap([train(5, 101, 1, "Success", UM), train(6, 103, 0, "Running")], queue=mine, lab_folds={K_UM: dict(final=True)}), "lab")
R.execute(acts, "lab", remote=fake_remote(), run=fake_run)
ok("训完等备份的折：照样续（beat）", rem == [f"beat --worker lab {K_UM}"])
cmds.clear(), rem.clear()
acts, _, _ = R.plan(snap([train(5, 101, 1, "Running", UM)], queue=mine), "lab")
R.execute(acts, "lab", remote=fake_remote(), run=fake_run)
ok("续：一次 beat 带上所有在训的折", rem == [f"beat --worker lab {K_UM}"] and cmds == [])
ok("没有 Slurm 作业取消、没有 box 下游任务", not any("scancel" in c or "fetch_fold" in c for _, c in cmds))

# ---------------- 开关 ----------------
R.taskq.remote = lambda h, a: (_ for _ in ()).throw(AssertionError("开关没开时不应该连 DICC"))
ev, _, filled = R.run_all(snap())
ok("开关（LAB）没开：只记'会做什么'，不推送、不连 DICC、不加任务", ev == [] and not filled and "开关没开" in open(R.LOG).read())
R.run_all(snap())
ok("影子日志和上次一样就不再写", open(R.LOG).read().count("开关没开") == 1)
called = []
R.taskq.remote = lambda h, a: called.append(a) or (0, "")
ev, _, filled = R.run_all(snap([train(5, 101, 1, "Running", UM)], queue=mine))
ok("开关没开、lab 领着在训的折：照样续（不让领取过期），别的不做", called == [f"beat --worker lab {K_UM}"] and not filled)
R.taskq.remote = lambda h, a: (_ for _ in ()).throw(AssertionError("dry-run 不应该连 DICC"))
ev, _, _ = R.run_all(snap(), dry=True)
ok("--dry-run：打印会做什么", "会 take" in ev[0])

# ---------------- 清理 ----------------
cmds.clear()
busy = snap([train(1, 103, 3, "Running")])
ok("还有训练在用：不删", R.cleanup(busy, "lab", run=fake_run) == [] and not cmds)
arch = task(40, "wait", "Queued", "bash scripts/queue/archive_fold.sh lab Dataset103_GLIrq1_t1c/x 0", "a")
ok("box 上还有这台机器的归档任务：不删", R.cleanup(snap(box_tasks=[arch]), "lab", run=fake_run) == [] and not cmds)
ok("lab 上还有训完没归档的折：不删", R.cleanup(snap(lab_folds={K_T1C[0]: dict(final=True, archived=False)}), "lab", run=fake_run) == [] and not cmds)
ok("lab 还领着这个数据集的折：不删", R.cleanup(snap(queue=mine2), "lab", run=fake_run) == [] and not cmds)
out = R.cleanup(snap(), "lab", run=fake_run)
ok("都用完了：删掉自动传过去的数据集", len(out) == 1 and cmds[-1][1] == "rm -rf ~/BRAIN_TUMOR/nnUNet/preprocessed/Dataset103_GLIrq1_t1c")
import json  # noqa: E402
json.dump({"transferred": {"lab": ["Dataset103_GLIrq1_t1c"]}}, open(R.STATE, "w"))
out = R.cleanup(snap(), "lab", run=fake_run)
ok("10/18 前只记了数据集名的：照样认、删掉后从记录里去掉", len(out) == 1 and cmds[-1][1].endswith("nnUNet/preprocessed/Dataset103_GLIrq1_t1c")
   and json.load(open(R.STATE))["transferred"]["lab"] == [])

# ---------------- DICC 补作业 ----------------
QOS = {"long": "1-12:00:00", "normal": "08:00:00"}
p, why = R.topup_plan(snap()["taskq"]["dicc"], {"long": (10, 216), "normal": (10, 138)}, QOS)
ok("现在：旧作业远超上限，不补", p == [] and "排满" in why)
p, _ = R.topup_plan(snap()["taskq"]["dicc"], {"long": (10, 9), "normal": (10, 3)}, QOS)
ok("long 空 1 个、normal 空 7 个：各补满", p == [("long", "1-12:00:00", 1), ("normal", "08:00:00", 7)])
p, _ = R.topup_plan(snap()["taskq"]["dicc"], {"long": (None, 4), "normal": (10, 10)}, QOS)
ok("QOS 没设排队上限：按 SUBMIT_CAP", p == [("long", "1-12:00:00", R.SUBMIT_CAP - 4)])
done_q = [tq(K_T14, 0, 1, "done"), tq(K_UM, 50, 2, "claimed", "486191")]
ok("队列里没有没人领的任务：不补", R.topup_plan(snap(queue=done_q)["taskq"]["dicc"], {"long": (10, 0), "normal": (10, 0)}, QOS)[0] == [])
ok("领取过期的（stale）也算没人领", R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "stale")])["taskq"]["dicc"], {"long": (10, 0), "normal": (10, 10)}, QOS)[0] != [])
ok("总开关不在：不补", R.topup_plan(snap(enabled=False)["taskq"]["dicc"], {"long": (10, 0), "normal": (10, 0)}, QOS)[0] == [])
paused = snap()["taskq"]["dicc"] | {"paused": True}
ok("队列暂停：不补", R.topup_plan(paused, {"long": (10, 0), "normal": (10, 0)}, QOS)[0] == [])
ok("读不到某个 QOS 的作业数：都不补", R.topup_plan(snap()["taskq"]["dicc"], {"long": (10, 0)}, QOS)[0] == [])

sub = []


def topup_run(probe_out, sbatch_rc=0):
    def run(cmd, host=None, timeout=0):
        if cmd.startswith("set -e"):
            return 0, probe_out
        sub.append((host, cmd))
        return sbatch_rc, "487001" if sbatch_rc == 0 else "sbatch: error: QOSMaxSubmitJobPerUserLimit"
    return run


PROBE = "QOS|long|10|9\nQOS|normal|10|10\n"
if os.path.exists(R.DICC_SWITCH):
    os.remove(R.DICC_SWITCH)
ev, st = R.topup(snap(), "dicc", run=topup_run(PROBE))
ok("开关（DICC）没开：只记'会交什么'，不交", not sub and ev == [] and "会：交 long 1 段" in open(R.LOG).read())
ev, st = R.topup(snap(), "dicc", dry=True, run=topup_run(PROBE))
ok("--dry-run：打印会交什么，不交", not sub and ev == ["dicc：会 交 long 1 段"])
open(R.DICC_SWITCH, "w").close()
ev, st = R.topup(snap(), "dicc", run=topup_run(PROBE))
ok("开关开着：交 worker.sbatch（long，1 段，不限并发），记日志、不推送",
   sub == [("dicc", "cd ~/Projects/PhD/scripts/queue && sbatch --parsable --qos=long --time=1-12:00:00 --array=1-1 worker.sbatch")]
   and ev == [] and not st and "交了 worker 作业 487001" in open(R.LOG).read())
sub.clear()
ev, st = R.topup(snap(), "dicc", run=topup_run(PROBE, sbatch_rc=1))
ok("交不上：报成问题", list(st) == ["topup:dicc:long"] and "QOSMaxSubmit" in st["topup:dicc:long"])
sub.clear()
ev, st = R.topup(snap(), "dicc", run=lambda c, host=None, timeout=0: (255, "ssh 不通"))
ok("查作业数时连不上：什么都不做", not sub and ev == [] and not st)
ev, st = R.topup(snap(), "dicc", run=topup_run("QOS|long|10|\nQOS|normal|10|1\n"))
ok("作业数读不出来：不补（不按 0 算）", ev == [] and not st and not any("long" in c for _, c in sub))
sub.clear()
os.remove(R.DICC_SWITCH)

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
