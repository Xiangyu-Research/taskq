#!/usr/bin/env python3
"""测 health.py 的检查和推送规则（2026-10-16）：假的队列、训练日志、DICC 输出、时钟，不连任何机器、不真的推送。

    python3 scripts/monitor/test_health.py
"""
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["PHD_TASKS"] = os.path.join(tempfile.mkdtemp(), "tasks.json")  # 不碰真的任务记录
os.environ["PHD_LAST"] = os.path.join(tempfile.mkdtemp(), "last")
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


def fields(status):
    """qs 从 status 拆出的 result、code（如 "Failed (143)" → Failed、143）。"""
    m = re.fullmatch(r"(\w+)(?: \((-?\d+)\))?", status)
    done = m[1] not in H.LIVE
    return dict(result=m[1] if done else None, code=int(m[2]) if m[2] else 0 if m[1] == "Success" else None)


def task(i, group, status, cmd, label="", start=NOW - timedelta(hours=3), end=None):
    return dict(id=i, group=group, status=status, **fields(status), label=label, deps=[], start=pt(start) if start else None,
                end=pt(end) if end else None, command=cmd)


LAB_BUSY = {"groups": {}, "tasks": [dict(id=90, group="gpu1", status="Running", label="busy_a", deps=[], start=None, end=None,
                                         command="bash scripts/queue/run_task.sh nnunet dataset=Dataset101_GLIrq1 trainer=nnUNetTrainer_save10 fold=2"),
                                    dict(id=91, group="gpu1", status="Queued", label="busy_b", deps=[], start=None, end=None,
                                         command="bash scripts/queue/run_task.sh nnunet dataset=Dataset101_GLIrq1 trainer=nnUNetTrainer_save10 fold=3")]}


def snap(**kw):
    s = dict(now=NOW, ok={"box": True, "lab": True, "dicc": True}, slurm={"dicc": dict(squeue=[], sacct=[])},
             probe={"box": {"DISK": str(int(500e9)), "BOOT": "2026-10-01 08:00:00|"},
                    "lab": {"DISK": str(int(200e9)), "GPULOG": ts(NOW - timedelta(minutes=1)), "BOOT": "2026-10-01 08:00:00|"}},
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
    task(4, "cpu", "Failed (143)", "bash w", "stopped", end=NOW - timedelta(hours=1)),
    task(5, "cpu", "Killed", "bash v", "killed", end=NOW - timedelta(hours=1)),
    task(6, "gpu", "Failed (137)", "bash u", "oom", end=NOW - timedelta(hours=1))]}})
P = H.check(s)
ok("任务失败、依赖失败都报", any(p.startswith("pueue:box:1:") for p in P) and any(p.startswith("pueue:box:2:") for p in P))
ok("24 小时前的失败不再报", not any(p.startswith("pueue:box:3:") for p in P))
ok("手动停的（143、pueue kill）只记不推", [p["level"] for k, p in P.items() if k.startswith(("pueue:box:4:", "pueue:box:5:"))] == ["info"] * 2)
ok("训练命令被信号杀掉（137，多半是内存不够）照常报", [p["level"] for k, p in P.items() if k.startswith("pueue:box:6:")] == ["normal"])

s = snap(slurm={"dicc": dict(squeue=[], sacct=[["486300_1", "train_worker", "FAILED", "2026-10-16T11:00:00", "1:0"]])})
ok("DICC 作业失败", "dicc:486300_1" in ids(s))

lab_train = task(7, "gpu1", "Running", "bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=0", "t15men_f0")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_train]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "lab": {"folds": {MEN0: fold(end=NOW - timedelta(hours=2))}}})
ok("lab 训练 90 分钟没有新 epoch → 卡住", "stuck:lab:7" in ids(s))
s["src"]["lab"]["folds"][MEN0] = fold(end=NOW - timedelta(minutes=5))
ok("lab 训练正常时不报", "stuck:lab:7" not in ids(s))

fetch = task(9, "wait", "Running", f"bash scripts/queue/fetch_task.sh dicc {MET2}", "fetch_t3met_f2")
s = snap(pueue={"box": {"groups": {}, "tasks": [fetch]}, "lab": {"groups": {}, "tasks": []}})
ok("在等的折不在任何地方排队 → 报", f"orphan:{MET2}" in ids(s))
s["slurm"], s["ok"]["dicc"] = {}, False
ok("DICC 连不上时不判断是否在排队", f"orphan:{MET2}" not in ids(s) and "down:dicc" in ids(s))
s = snap(pueue={"box": {"groups": {}, "tasks": [fetch]}, "lab": {"groups": {}, "tasks": []}},
         src={"box": {"folds": {}}, "lab": {"folds": {}}, "dicc": {"folds": {MET2: fold(final=True, epochs=1000, end=NOW - timedelta(hours=3))}}})
ok("训完 90 分钟还没拉回 → 报", f"fetch:{MET2}" in ids(s))


# ---------------- DICC 任务队列（10/17）+ lab 领的折（10/18）----------------
def tqrow(key, state, worker=None, since=None, log_age=None, kind="nnunet", task_log_age=None, out_age=None):
    return dict(key=key, state=state, worker=worker, host=worker, since=since, beat=None, note="", log_age=log_age, failures=0,
                kind=kind, nice=0, order=1, task_log_age=task_log_age, out_age=out_age)


old = NOW.timestamp() - 3 * 3600  # 领了 3 小时
lab_men = task(7, "gpu1", "Running", "bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=0", "t15men_f0")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_men]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "lab": {"folds": {MEN0: fold(end=NOW - timedelta(minutes=5))}}},
         taskq={"box": dict(enabled=True, tasks=[dict(tqrow(MEN0, "claimed", "lab", since=old), assigned="lab")]),
                "dicc": dict(enabled=True, buffer=True, paused=False, tasks=[])})
H.time.time = lambda: NOW.timestamp()
P = H.check(s)
ok("总队列分给 lab、lab 在训：不算同一折两处，也不算卡住", f"dup:{MEN0}" not in P and not [k for k in P if k.startswith("stuck:") and MEN0 in k])
s["taskq"]["dicc"]["tasks"] = [tqrow(MEN0, "claimed", "486191_2", since=old, log_age=60)]
s["slurm"] = sq(["train_worker", "486191_2", "RUNNING", "3:00:00", "None", "0"])
ok("DICC 作业领了、lab 也在训同一折 → 紧急", H.check(s).get(f"dup:{MEN0}", {}).get("level") == "urgent")
INF = "infer/Dataset103_GLIrq1_t1c/x/fold_2"
s["taskq"]["dicc"]["tasks"] = [tqrow(INF, "claimed", "491480", since=old, task_log_age=120)]
ok("推理任务（没有训练日志）：任务日志刚更新过，不算卡住", f"stuck:dicc:{INF}" not in H.check(s))
s["taskq"]["dicc"]["tasks"] = [tqrow(INF, "claimed", "491480", since=old, task_log_age=6000)]
ok("推理任务：任务日志、结果文件夹 100 分钟没变，算卡住", "90 分钟没有变化" in H.check(s).get(f"stuck:dicc:{INF}", {}).get("msg", ""))
s["taskq"]["dicc"]["tasks"] = [tqrow(INF, "claimed", "491480", since=old, task_log_age=6000, out_age=300)]
ok("推理任务：日志 100 分钟没更新、但预测文件夹 5 分钟前还在变（10/24）→ 不算卡住", f"stuck:dicc:{INF}" not in H.check(s))
s["taskq"]["dicc"]["tasks"] = []
s["pueue"]["lab"]["tasks"] = []
ok("在跑的 DICC 作业、缓冲里没人领：报有卡空着", "tqidle:dicc" in H.check(s))
s["taskq"]["dicc"]["tasks"] = [tqrow(INF, "waiting", None), tqrow(MET2, "claimed", "491480", since=old, task_log_age=60)]
s["slurm"] = sq(["train_worker", "487000_1", "RUNNING", "3:00:00", "None", "0"])
ok("提前排的推理在等训练（waiting）：不报、不算没人训、不算有卡空着", not {f"tqblocked:{INF}", "tqorphan:dicc", "tqidle:dicc"} & ids(s))
s["taskq"]["dicc"]["tasks"][0] = dict(tqrow(INF, "blocked", None), note="等的 x 失败 2 次")
ok("等的任务失败了（blocked）：报出来", "失败 2 次" in H.check(s).get(f"tqblocked:{INF}", {}).get("msg", ""))
s["taskq"]["dicc"]["tasks"] = [tqrow(MET2, "free", None, since=old)]
s["slurm"] = sq()
ok("队列里有没人训的任务、DICC 上一个作业都没有：提醒（补作业的开关）", "AUTO" in H.check(s).get("tqorphan:dicc", {}).get("msg", ""))
s["slurm"] = sq(["train_worker", "486300_1", "PENDING", "0:00", "Priority", "2000"])
ok("还有排着的作业（什么模型的都行）：不提醒", "tqorphan:dicc" not in H.check(s))
s["slurm"] = sq(["train_worker", "487000_1", "PENDING", "0:00", "Priority", "0"])
ok("排着不带折的 train_worker 作业也算：不提醒", "tqorphan:dicc" not in H.check(s))
s["slurm"] = sq(["cpu_worker", "487001", "RUNNING", "1:00:00", "None", "0"])
ok("只有 CPU worker 在跑：还是提醒没人训（作业名按 envs/machines.py 的 jobs）", "tqorphan:dicc" in H.check(s))
s["taskq"]["dicc"]["tasks"] = [tqrow(MET2, "free", None)]
s["slurm"] = sq(["train_worker", "487000_1", "RUNNING", "3:00:00", "None", "0"])
ok("在跑的 train_worker 作业没领到任务：报有卡空着", "tqidle:dicc" in H.check(s))

# lab 的缓冲（10/31）：worker 组看缓冲里 worker 有没有在做的
LW = H.M.MACHINES["lab"]["worker"]
wk = task(30, LW["group"], "Running", f"WORKER_ID={LW['id']} bash scripts/queue/dicc_worker.sh", f"worker:{LW['id']}")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [wk]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "lab": {"folds": {MEN0: fold(end=NOW - timedelta(minutes=5))}}},
         taskq={"box": dict(enabled=True, tasks=[]), "lab": dict(enabled=True, buffer=True, paused=False, tasks=[tqrow(MEN0, "free")])})
s["slurm"] = {}
P = H.check(s)
ok("lab 的 worker 手上没有任务：报 idle（等 30 分钟）", P.get(f"idle:lab:{LW['group']}", {}).get("after") == 30)
s["taskq"]["lab"]["tasks"] = [tqrow(MEN0, "claimed", LW["id"], since=NOW.timestamp() - 600)]
P = H.check(s)
ok("lab 的 worker 在训：不报 idle", f"idle:lab:{LW['group']}" not in P)
s["pueue"]["lab"]["tasks"] = [wk, task(31, "gpu0", "Running", "bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=0", "t15men_f0")]
ok("lab 缓冲里 worker 在训、0 号卡的 pueue 也在训同一折 → 同一折两处、紧急", H.check(s).get(f"dup:{MEN0}", {}).get("level") == "urgent")
s["taskq"]["lab"].pop("buffer")
s["taskq"]["lab"]["enabled"] = False
s["pueue"]["lab"]["tasks"] = []
ok("lab 还不是缓冲：照旧按 pueue 组看卡空着", f"idle:lab:{LW['group']}" in H.check(s) and "after" in H.check(s)[f"idle:lab:{LW['group']}"])
H.time.time = time_orig

# 总队列在 box（10/31）
ok("box 的总队列读不到 → 报", "taskq:box" in H.check(snap(taskq={"box": None})))
ok("总队列关着（没有 ENABLED）→ 持续 60 分钟后报", H.check(snap(taskq={"box": dict(enabled=False, tasks=[])})).get("tqoff:box", {}).get("after") == 60)
bq = [dict(tqrow(INF, "blocked", None), note="等的 x 失败 2 次"), dict(tqrow(MET2, "claimed", "dicc"), assigned="dicc")]
P = H.check(snap(taskq={"box": dict(enabled=True, paused=False, tasks=bq)}))
ok("总队列里没分出去的任务 blocked → 报；分给执行机的不算同一折两处", f"tqblocked:{INF}" in P and f"dup:{MET2}" not in P)

# ---------------- 通用的任务（10/18）：run_task.sh、fetch_task.sh、数据清单 ----------------
def tk(cmd):
    r = H.C.from_command(cmd)
    return (r["step"], r["key"]) if r else (None, None)


ok("run_task.sh 的训练命令 → key", tk("bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 "
                                              "trainer=nnUNetTrainer_save10_n470 fold=0") == ("train", MEN0))
ok("q 加的、整条命令包在引号里的 run_task.sh 也认", tk("bash -c 'bash scripts/queue/run_task.sh nnunet "
                                                           "dataset=Dataset116_METrq1 fold=2'") == ("train", MET2))
ok("command 做法的任务 → 它自己给的 key", tk("bash scripts/queue/run_task.sh command key=x/y cmd=true env=nnunet "
                                                     "out=runs/x done=ok") == ("train", "x/y"))
ok("参数写错的 run_task.sh 不当成训练", tk("bash scripts/queue/run_task.sh nnunet datset=x fold=0") == (None, None))
ok("fetch_task.sh <机器> <key> → 拉回", H.C.from_command(f"bash scripts/queue/fetch_task.sh dicc {MET2}") == dict(step="fetch", src="dicc", key=MET2))
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
ok("HAVE 实跑：在的列出来，没传完（.incomplete）和不在的不列", H.C.parse_have(out) == {"nnUNet/preprocessed/A", "C"})
ok("HAVE 输出被截断（没有结束标记）→ 没读到", H.C.parse_have(out.split(H.C.HAVE_END)[0]) is None)

# ---------------- gather：各卡和卡上的进程、Slurm 分区、只看本机 ----------------
fakebin = tempfile.mkdtemp()
sl = subprocess.Popen(["sleep", "30"])
with open(os.path.join(fakebin, "nvidia-smi"), "w") as f:
    f.write(f'#!/bin/bash\ncase "$*" in *query-gpu=*) echo "0, GPU-a, 37, 1200"; echo "1, GPU-b, 0, 5";;\n'
            f'*query-compute-apps*) echo "GPU-a, {sl.pid}, 1100"; echo "GPU-x, 1, 9";; esac\n')
os.chmod(os.path.join(fakebin, "nvidia-smi"), 0o755)
out = subprocess.run(["bash", "-s"], input=H.GPU_PROBE, capture_output=True, text=True,
                     env=dict(os.environ, PATH=fakebin + ":" + os.environ["PATH"])).stdout
sl.kill()
gp = H.gpus_of(out.splitlines())
ok("GPU 探查实跑：每张卡一项，卡上的进程带命令行，对不上卡的进程不要",
   [(x["gpu"], x["util"], x["mem"]) for x in gp] == [("0", "37", "1200"), ("1", "0", "5")]
   and [(a["pid"], a["mem"], a["args"]) for a in gp[0]["apps"]] == [(str(sl.pid), "1100", "sleep 30")] and gp[1]["apps"] == [])
ok("探查脚本里有 GPU 那几行", H.GPU_PROBE in H.probe_script(H.M.MACHINES["box"]))
calls = []
saved = (H.sh, H.run_on, H.taskq.remote_status, H.spec.remote_collect, H.spec.collect, H.taskq.local_status)
H.taskq.local_status = lambda: dict(enabled=True, tasks=[])  # box 上的总队列（10/31）


def fake_sh(cmd, inp=None, timeout=120):
    calls.append(("sh", cmd))
    if cmd[0] == "git":
        return 0, "abc\n"
    return 0, json.dumps({n: {"groups": {}, "tasks": []} for n in (cmd[2:] or H.M.names("pueue"))})


def fake_run_on(m, script, timeout=120):
    calls.append(("run_on", m["label"]))
    if m["scheduler"] == "slurm":
        return 0, "train_worker|123|RUNNING|1:00|None|0|gpu-a100\nbad|line\n=====\n"
    have = "HAVE|nnUNet/x\nHAVE|a b\n@@END\n" if H.C.HAVE_END in script and not cut else ""
    return 0, "BOOT|2026-10-01 08:00:00|\nDISK|123\nGPU|0, GPU-a, 37, 1200\nAPP|GPU-a| 42| 1100|python x|y\n" + have


cut = False


H.sh, H.run_on = fake_sh, fake_run_on
H.taskq.remote_status = lambda n: dict(enabled=True, tasks=[])
H.spec.remote_collect = lambda ssh, specs, root: {"folds": {}, "seglogs": []}
H.spec.collect = lambda specs, root: {"folds": {}, "seglogs": []}
g = H.gather()
ok("gather：GPU、APP 行进 gpus，不进 probe；命令行里的 | 原样留着",
   g["probe"]["lab"] == {"BOOT": "2026-10-01 08:00:00|", "DISK": "123", "HAVE": "a b nnUNet/x"}
   and g["gpus"]["lab"] == [dict(gpu="0", uuid="GPU-a", util="37", mem="1200", apps=[dict(pid="42", mem="1100", args="python x|y")])])
ok("gather：squeue 每行 7 项（最后是分区），格式不对的行不要",
   g["slurm"]["dicc"]["squeue"] == [["train_worker", "123", "RUNNING", "1:00", "None", "0", "gpu-a100"]])
cut = True
g = H.gather()
ok("gather：训练机的 HAVE 没有结束标记（输出被截断）→ 这台算没读到", g["ok"]["lab"] is False and "lab" not in g["probe"])
cut = False
calls.clear()
g = H.gather(remote=False)
qs_call = next(c[1] for c in calls if c[0] == "sh" and c[1][0] != "git")
ok("gather(remote=False)：只探查本机，qs 只问本机，不碰 Slurm",
   [c for c in calls if c[0] == "run_on"] == [("run_on", "box")] and qs_call[1:] == ["--json", "box"]
   and set(g["ok"]) == {"box"} and not g["slurm"] and set(g["taskq"]) == {"box"} and set(g["pueue"]) == {"box"})
H.sh, H.run_on, H.taskq.remote_status, H.spec.remote_collect, H.spec.collect, H.taskq.local_status = saved

s = snap()
s["probe"]["lab"]["DISK"] = str(int(10e9))
s["probe"]["box"]["DISK"] = str(int(50e9))
P = H.check(s)
ok("lab 盘不到 15 GB → 紧急；box 盘不到 100 GB → 提醒", P["disk:lab"]["level"] == "urgent" and P["disk:box"]["level"] == "normal")
s["probe"]["lab"]["GPULOG"] = ts(NOW - timedelta(minutes=30))
s["code"] = dict(main="a" * 40, heads={"lab": "b" * 40, "dicc": "a" * 40, "gone": "c" * 40})
P = H.check(s)
ok("GPU 记录停了 → 报；代码停在别的提交 → 报（持续 2 小时才算），和 main 一样的、表里没有的机器不报",
   "gpulog:lab" in P and P.get("code:lab", {}).get("after") == 120 and "code:dicc" not in P and "code:gone" not in P)
s["code"]["heads"]["lab"] = ""
ok("连不上（提交号空着）→ 不报代码", "code:lab" not in H.check(s))

s = snap(board_pending=["data/rq1_v50.json"])
s["probe"]["box"]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 开机：放回队列 3 4"
P = H.check(s)
ok("box 重启过 → 报（带上放回的任务），lab 很久前重启的不报", [p["msg"] for k, p in P.items() if k.startswith("boot:")] == ["box 10-16 10:00 重启过：开机：放回队列 3 4"])
ok("看板有结果等确认 → 报", "board" in P)

s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [
    dict(id=1, group="gpu1", status="Stashed", label="umamba_f0", deps=[], start=None, end=None, command="bash scripts/queue/run_task.sh nnunet dataset=Dataset101_GLIrq1 trainer=nnUNetTrainer_UMambaEnc env=umamba fold=0")]}})
s["pueue"]["lab"]["tasks"] = s["pueue"]["lab"]["tasks"][:1]
ok("lab 1 号卡没有训练任务（暂存的不算）→ 报", "idle:lab:gpu1" in ids(s))
s3 = dict(s, pueue=dict(s["pueue"], lab=dict(s["pueue"]["lab"], groups={"gpu1": "Running", "gpu0": "Running"})))
ok("lab 0 号卡（只跑手动任务的组）开着、空着 → 不报", "idle:lab:gpu1" in ids(s3) and not any(k.startswith("idle:lab:gpu0") for k in ids(s3)))
s2 = dict(s, taskq={"box": dict(enabled=True, paused=False, tasks=[tqrow(MET2, "claimed", "lab")])})
ok("lab 领着折（在传数据或停了等人处理，refill.py 另外报）：卡空着不另报", "idle:lab:gpu1" not in ids(s2))
s2["taskq"]["box"]["tasks"] = [tqrow(MET2, "done", "lab")]
ok("lab 领着的折已训完（等交还）：卡空着照报", "idle:lab:gpu1" in ids(s2))
s["pueue"]["lab"]["tasks"].append(dict(id=2, group="gpu1", status="Running", label="t15men_f4", deps=[], start=None, end=None,
                                       command="bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=4"))
P = H.check(s)
ok("只剩在训的一个 → 只记（训完后自动领下一折），不推送", "只剩在训的 t15men_f4" in P.get("idle:lab:gpu1", {}).get("msg", "")
   and P["idle:lab:gpu1"]["level"] == "info")
ok("还有排着的 → 不报", "idle:lab:gpu1" not in ids(snap()))

# ---------------- 第四台机器：只在表里加一项 ----------------
H.M.MACHINES["fake"] = dict(label="fake", hostname=r"fake-host", ssh="fake", scheduler="pueue", data="~/BT", conda=("~/c", "e"),
                            groups={"gpu0": (1, "0", {})}, roles=("train",), disk=("/data", 50, 10), gpu_log=False)
fk = task(3, "gpu0", "Running", "bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=0", "t15men_f0_fake")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_train]}, "fake": {"groups": {}, "tasks": [fk]}},
         src={"box": {"folds": {}}, "dicc": {"folds": {}}, "fake": {"folds": {}},
              "lab": {"folds": {MEN0: fold(end=NOW - timedelta(minutes=5))}}})
s["ok"]["fake"] = True
s["probe"]["fake"] = {"DISK": str(int(5e9)), "BOOT": "2026-10-01 08:00:00|"}
s["code"] = dict(main="a" * 40, heads={"fake": "b" * 40})
P = H.check(s)
ok("第四台机器：同一折和 lab 两边都在训 → 紧急", P.get(f"dup:{MEN0}", {}).get("level") == "urgent")
ok("第四台机器：盘、代码按表里的字段查", P["disk:fake"]["level"] == "urgent" and "code:fake" in P and "gpulog:fake" not in P)
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
B = {"code:lab": dict(msg="g", level="normal", kind="state", after=0), "gpulog:lab": dict(msg="l", level="normal", kind="state", after=0)}
out = run(B, datetime(2026, 10, 17, 10, 30))
ok("同一次查到几件：并成一条", len(out) == 1 and "（2 件）" in out[0][0] and out[0][1].count("\n- ") == 1)
I = {"idle:lab:gpu1": dict(msg="只剩在训的", level="info", kind="state", after=0)}
ok("info 级不推送", run(I, datetime(2026, 10, 17, 11, 0)) == [])

# ---------------- refill / advance 出错不让健康检查停掉 ----------------
import types  # noqa: E402
got = {}
H.gather, H.update = (lambda: snap()), (lambda P, now, dry=False: got.update(P))
sys.modules["refill"] = None  # import 出错
fd = types.ModuleType("feed")
fd.run_all = lambda s, dry=False: ([], {})
sys.modules["feed"] = fd
adv = types.ModuleType("advance")
adv.run = lambda s, dry=False: 1 / 0
sys.modules["advance"] = adv
sys.argv = ["health.py"]
H.main()
ok("refill.py import 出错、advance.py 运行出错：都报出来，健康检查照常写完",
   "refill:error" in got and "advance:error" in got and "ZeroDivisionError" in got["advance:error"]["msg"])
rf = types.ModuleType("refill")
rf.run_all = lambda s, dry=False: (["lab 领了 x", "lab 交还 y"], {}, set())
rf.topup_all = lambda s, dry=False: ([], {})
sys.modules["refill"] = rf
got.clear()
H.main()
ok("lab 领了 / 交还了：只记 refill.log，不进推送", not any(k.startswith("refill:") for k in got))

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
