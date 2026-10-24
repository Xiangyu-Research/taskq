#!/usr/bin/env python3
"""advance.py 的测试（2026-10-18）：临时目录里的假 box 文件、假的 pueue 快照，不连任何机器、不加任务。
    python3 scripts/queue/test_advance.py
"""
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
T = tempfile.mkdtemp()
os.environ["ADVANCE_BT"] = T
os.environ["ADVANCE_LOG"] = os.path.join(T, "advance.log")
sys.path.insert(0, HERE)
import advance as A  # noqa: E402

A.STATE_DIR = os.path.join(T, "state")
A.taskq_add = lambda nice, args: (0, "")  # 不经 ssh 真的加进 DICC 的队列
# 前面几节测 box 上排的步骤：先去掉推理的 queue（10/22 起推理放 DICC 任务队列），最后一节再放回来测
QUEUED = [(st, st.pop("queue")) for e in A.E.load()[0] for st in e.get("after", []) if "queue" in st]
# 实验一级带 queue 的几串（10/23，P1 / P2 重跑）也先拿掉，第 11 节再放回来测
GQUEUED = [(e, e["group_after"]) for e in A.E.load()[0] if any(st.get("queue") for c in e.get("group_after", []) for st in c)]
for e_, g_ in GQUEUED:
    e_["group_after"] = [c for c in g_ if not any(st.get("queue") for st in c)]
P = "nnUNetResEncUNetMPlans__3d_fullres"
F5 = ["0", "1", "2", "3", "4"]
PY = A.python()
ok = True


def check(name, cond):
    global ok
    print(("通过：" if cond else "失败：") + name)
    ok &= bool(cond)


def touch(path, text=""):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w").write(text)


def trained(ds, tr, f):
    d = f"{T}/nnUNet/results/{ds}/{tr}__{P}/fold_{f}"
    touch(f"{d}/checkpoint_final.pth")
    touch(f"{d}/validation/summary.json", "{}")


def inferred(ds, tr, f):
    touch(f"{T}/predictions/{ds}/{tr}__{P}/fold_{f}/MANIFEST.json", json.dumps({"cohorts": {}, "probabilities_zeroed_below": {}}))


def evaluated(ds, tr, tag):
    touch(f"{T}/RQ1/analysis/{ds}/{tr}__{P}/{tag}/report/report.run.json", '{"status": "ok"}')


def snap(tasks=(), remote=None):
    rows = [dict(id=i, status=st, command=c, label="", group="x", end=end) for i, (st, c, end) in enumerate(tasks)]
    return dict(pueue={"box": dict(tasks=rows)}, src={h: dict(folds=f) for h, f in (remote or {}).items()},
                taskq={"dicc": dict(tasks=[dict(key=f"Dataset121_GLIMETrq1/nnUNetTrainer_save10/{k}", nice=0) for k in range(5)])})


def steps(todo):
    return sorted((t["step"], t["key"]) for t in todo)


MET = "Dataset121_GLIMETrq1/nnUNetTrainer_save10"   # T14：oof + 集成
T3 = "Dataset116_METrq1/nnUNetTrainer_save10"       # T3：只有 oof
FIN = dict(final=True, archived=False)

# 1. 别处训完、box 上没有 → 拉回（DICC 的从 dicc 拉，lab 的从 lab 拉）
todo, stuck = A.plan(snap(remote={"dicc": {f"{MET}/0": FIN}, "lab": {f"{T3}/1": FIN, f"{T3}/2": dict(final=False)}}))
check("别处训完的排拉回，没训完的不排", steps(todo) == [("fetch", f"{T3}/1"), ("fetch", f"{MET}/0")])
f = next(t for t in todo if t["key"] == f"{MET}/0")
check("拉回：wait 组、从训完的机器拉、标签 fetch:<实验>_f<fold>",
      f["group"] == "wait" and f["cmd"] == "bash scripts/queue/fetch_task.sh dicc Dataset121_GLIMETrq1/nnUNetTrainer_save10/0"
      and f["label"] == "fetch:t14met_f0")
check("按队列的档位排在前面（档位 0 的 T14 先于没有档位的 T3）", todo[0]["key"] == f"{MET}/0")

# 2. 已经有拉回任务在跑（以前手动排的）→ 不重复
old = ("Running", f"bash scripts/queue/fetch_task.sh dicc {MET}/0", None)
todo, stuck = A.plan(snap([old], remote={"dicc": {f"{MET}/0": FIN}}))
check("已经有活着的拉回任务：不重复排", todo == [] and not stuck)
other = ("Running", f"bash scripts/queue/fetch_task.sh lab {MET}/0", None)
todo, stuck = A.plan(snap([other], remote={"dicc": {f"{MET}/0": FIN}}))
check("活着的拉回任务在等另一台机器（以前预先排的链）：照样从训完的机器拉，旧任务报出来",
      steps(todo) == [("fetch", f"{MET}/0")] and "fetch_task.sh dicc" in todo[0]["cmd"] and f"advance:stale_fetch:{MET}/0:0" in stuck)
failed_lab = ("Failed (1)", other[1], None)
todo, stuck = A.plan(snap([failed_lab], remote={"dicc": {f"{MET}/0": FIN}}))
check("从另一台机器拉回失败过：不挡从训完的机器拉", steps(todo) == [("fetch", f"{MET}/0")] and not stuck)

# 3. box 上有了 → 推理（gpu）；lab 训的还要归档
trained("Dataset116_METrq1", "nnUNetTrainer_save10", "1")
todo, _ = A.plan(snap(remote={"lab": {f"{T3}/1": FIN}}))
check("拉回了：排推理和归档", steps(todo) == [("archive", f"{T3}/1"), ("infer", f"{T3}/1")])
inf = next(t for t in todo if t["step"] == "infer")
check("推理：gpu 组、--stage infer", inf["group"] == "gpu" and "--stage infer" in inf["cmd"] and inf["cmd"].endswith(f"__{P} 1"))
todo, _ = A.plan(snap(remote={"lab": {f"{T3}/1": dict(final=True, archived=True)}}))
check("lab 上已经归档的：不再归档", steps(todo) == [("infer", f"{T3}/1")])
stale = ("Queued", f"bash scripts/queue/fetch_task.sh lab {T3}/1", None)
todo, stuck = A.plan(snap([stale]))
check("box 上已经有了、还有在等的拉回任务：报出来", f"advance:stale_fetch:{T3}/1:0" in stuck)

# 4. 推理做完 → 评测（cpu）；评测做完 → 这一折没事了
inferred("Dataset116_METrq1", "nnUNetTrainer_save10", "1")
todo, _ = A.plan(snap())
check("推理做完：排评测（cpu 组、--stage eval）", steps(todo) == [("eval", f"{T3}/1")] and todo[0]["group"] == "cpu" and "--stage eval" in todo[0]["cmd"])
legacy = ("Queued", f"bash research/RQ1/analysis/run_predict_eval.sh {T}/nnUNet/results/Dataset116_METrq1/nnUNetTrainer_save10__{P} 1", None)
todo, _ = A.plan(snap([legacy]))
check("以前不带 --stage 的推理任务（推理 + 评测）排着：评测也不重复排", todo == [])
evaluated("Dataset116_METrq1", "nnUNetTrainer_save10", "fold_1")
todo, _ = A.plan(snap())
check("评测做完：没有要排的", todo == [])
touch(f"{T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/fold_2/report/report.md")
trained("Dataset116_METrq1", "nnUNetTrainer_save10", "2")
inferred("Dataset116_METrq1", "nnUNetTrainer_save10", "2")
todo, _ = A.plan(snap())
check("10/16 前的评测（没有 run.json、有 report.md）算做完", todo == [])

# 5. 失败过的不重排，报出来；成功跑完却没结果的也不重排
trained("Dataset116_METrq1", "nnUNetTrainer_save10", "3")
cmd3 = f"bash research/RQ1/analysis/run_predict_eval.sh --stage infer {T}/nnUNet/results/Dataset116_METrq1/nnUNetTrainer_save10__{P} 3"
now = datetime.now().astimezone()
todo, stuck = A.plan(snap([("Failed (1)", cmd3, now.isoformat())]))
check("推理失败过：不重排，报到卡住", todo == [] and f"advance:infer:{T3}/3" in stuck and "pueue remove 0" in stuck[f"advance:infer:{T3}/3"])
todo, stuck = A.plan(snap([("Success", cmd3, now.isoformat())]))
check("刚成功跑完但还是没结果：不重排，报到卡住", todo == [] and "成功跑完" in stuck[f"advance:infer:{T3}/3"])
todo, stuck = A.plan(snap([("DependencyFailed", cmd3, now.isoformat())]))
check("以前 --after 链里因依赖失败没跑的：照常排", steps(todo) == [("infer", f"{T3}/3")] and not stuck)
todo, stuck = A.plan(snap([("Success", cmd3, (now - timedelta(hours=8)).isoformat())]))
check("8 小时前成功过、结果没了：也报出来（不按时间放开，pueue remove 后重排）", todo == [] and "成功跑完" in stuck[f"advance:infer:{T3}/3"])
todo, stuck = A.plan(snap([("Failed (1)", cmd3, None), ("Success", cmd3, None), ("Success", "echo x", None)]))
check("失败之后又成功过：只看最后一次成功之后的（报的是成功跑完没结果，不是那次失败）",
      todo == [] and "任务 1 成功跑完" in stuck[f"advance:infer:{T3}/3"])

# 6. 五折到齐：T3 → oof → paired；T14 → oof 和集成；compare_all 等实验一级的都做完再排
for k in "0", "4":
    trained("Dataset116_METrq1", "nnUNetTrainer_save10", k)
    inferred("Dataset116_METrq1", "nnUNetTrainer_save10", k)
    evaluated("Dataset116_METrq1", "nnUNetTrainer_save10", f"fold_{k}")
inferred("Dataset116_METrq1", "nnUNetTrainer_save10", "3")
evaluated("Dataset116_METrq1", "nnUNetTrainer_save10", "fold_3")
todo, _ = A.plan(snap())
check("T3 五折到齐：排 oof（不排集成）", steps(todo) == [("oof", "t3met")] and todo[0]["group"] == "cpu" and "oof_eval.py" in todo[0]["cmd"])
touch(f"{T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/oof/oof_eval.run.json", '{"status": "ok"}')
todo, _ = A.plan(snap())
check("oof 做完：排配对（--cmp T3=…/oof/eval）", steps(todo) == [("paired", "t3met")] and "--cmp T3=" in todo[0]["cmd"]
      and todo[0]["cmd"].endswith("oof/paired"))
touch(f"{T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/oof/paired/paired_compare.run.json", '{"status": "ok"}')
todo, _ = A.plan(snap())
check("配对做完、compare_all 比它旧：排 compare_all", steps(todo) == [("compare_all", "all")])
cp = f"{T}/RQ1/analysis/compare/all/compare_all.run.json"
touch(cp, json.dumps({"status": "ok", "start": (datetime.now() - timedelta(minutes=10)).astimezone().isoformat(timespec="seconds")}))
os.utime(f"{T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/oof/paired/paired_compare.run.json",
         (os.path.getmtime(cp) - 60,) * 2)  # 配对在 compare_all 跑的过程中（开始之后、结束之前）做完
todo, _ = A.plan(snap())
check("配对在 compare_all 跑的过程中做完：按 compare_all 开始的时刻比，还要再排", steps(todo) == [("compare_all", "all")])
touch(cp, json.dumps({"status": "ok", "start": datetime.now().astimezone().isoformat(timespec="seconds")}))
todo, _ = A.plan(snap())
check("compare_all 比所有实验结果新：没有要排的", todo == [])
os.utime(f"{T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/oof/paired/paired_compare.run.json")
os.utime(cp, (os.path.getmtime(cp) - 120,) * 2)
touch(cp, json.dumps({"status": "ok", "start": (datetime.now() - timedelta(minutes=5)).astimezone().isoformat(timespec="seconds")}))
todo, stuck = A.plan(snap([("Success", "/py research/RQ1/analysis/compare_all.py", None)]))
check("compare_all 成功过、之后又有新结果：照常重排（不当成卡住）", steps(todo) == [("compare_all", "all")] and not stuck)
touch(cp, json.dumps({"status": "ok", "start": datetime.now().astimezone().isoformat(timespec="seconds")}))
for k in F5:
    trained("Dataset121_GLIMETrq1", "nnUNetTrainer_save10", k)
    inferred("Dataset121_GLIMETrq1", "nnUNetTrainer_save10", k)
    evaluated("Dataset121_GLIMETrq1", "nnUNetTrainer_save10", f"fold_{k}")
todo, _ = A.plan(snap())
ens = [t for t in todo if t["step"] == "ens"]
check("T14 五折到齐：oof 和五折集成都排，compare_all 先不排", steps(todo) == [("ens", "t14met"), ("oof", "t14met")]
      and ens[0]["cmd"].endswith("'0 1 2 3 4'") and ens[0]["cmd"].startswith("ENSEMBLE_ONLY=1 "))
old_ens = ("Running", f"bash research/RQ1/analysis/run_predict_eval.sh {T}/nnUNet/results/Dataset121_GLIMETrq1/nnUNetTrainer_save10__{P} '0 1 2 3 4'", None)
todo, _ = A.plan(snap([old_ens]))
check("集成任务在跑：认得出来，不重复", steps(todo) == [("oof", "t14met")])
todo, _ = A.plan(snap([("Running", ens[0]["cmd"], None)]))
check("带 ENSEMBLE_ONLY=1 的集成任务也认得出来", steps(todo) == [("oof", "t14met")])

# 6b. 推理的完整性：分割、概率图、输入病例数要对得上
src = f"{T}/raw/X/imagesTs"
for c in ("a", "b"):
    touch(f"{src}/{c}_0000.nii.gz")
man = f"{T}/predictions/Dataset116_METrq1/nnUNetTrainer_save10__{P}/fold_2/MANIFEST.json"
def manifest(nseg, nprob):
    touch(man, json.dumps({"probabilities_zeroed_below": 1e-5, "cohorts": {"X": dict(
        n_segmentations=nseg, n_probabilities=nprob, nnunet_args=dict(list_of_lists_or_source_folder=src))}}))
import experiments as E  # noqa: E402
INF = E.experiment("t3met")["after"][0]
inf_ok = lambda f: INF["done"].ok(T, E.task_names(E.tasks()[f"{T3}/{f}"], T, PY))  # noqa: E731
manifest(2, 1)
check("概率图比分割少：推理没做完", not inf_ok("2"))
manifest(1, 1)
check("比输入病例少：推理没做完", not inf_ok("2"))
manifest(2, 2)
check("都对得上：推理做完", inf_ok("2"))

# 6c. T0 五折在 lab 上的副本 10/23 手动归档过（有 ARCHIVED）：不再排归档
T0K = "Dataset101_GLIrq1/nnUNetTrainer_save10/0"
trained("Dataset101_GLIrq1", "nnUNetTrainer_save10", "0")
todo, _ = A.plan(snap(remote={"lab": {T0K: dict(final=True, archived=True)}}))
check("T0 nnU-Net 的 lab 副本已归档：不排归档", not [t for t in todo if t["step"] == "archive"])
todo, _ = A.plan(snap(remote={"lab": {T0K: FIN}}))
check("T0 nnU-Net 的 lab 副本没归档：照常归档", ("archive", T0K) in steps(todo))
UMK = "Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/3"
trained("Dataset101_GLIrq1", "nnUNetTrainer_UMambaEnc", "3")
todo, _ = A.plan(snap(remote={"lab": {UMK: FIN}}))
check("同属 T0 的 U-Mamba：照常归档", ("archive", UMK) in steps(todo))

# 7. 认命令
check("认命令：paired_compare 按 --out 认", A.step_of(f"py paired_compare.py --ref x --cmp T3=y --out {T}/RQ1/analysis/{T3.split('/')[0]}/nnUNetTrainer_save10__{P}/oof/paired") == [("paired", T3)])
check("认命令：归档", A.step_of("bash scripts/queue/archive_task.sh lab Dataset111_MENrq1/nnUNetTrainer_save10/4")
      == [("archive", "Dataset111_MENrq1/nnUNetTrainer_save10/4")])
check("认命令：拉回带源机器、前面有环境变量也认得",
      A.step_of("bash scripts/queue/fetch_task.sh dicc Dataset111_MENrq1/nnUNetTrainer_save10/4") == [("fetch@dicc", "Dataset111_MENrq1/nnUNetTrainer_save10/4")]
      and A.step_of(f"NO_TTA=1 bash research/RQ1/analysis/run_predict_eval.sh /x/Dataset111_MENrq1/nnUNetTrainer_save10__{P} 4")
      == [("infer", "Dataset111_MENrq1/nnUNetTrainer_save10/4"), ("eval", "Dataset111_MENrq1/nnUNetTrainer_save10/4")])
check("认命令：不认识的命令 → []", A.step_of("pueue parallel 2 --group cpu") == [] and A.step_of("echo 'unbalanced") == [])

# 7b. 新写法：拉回、归档按命令认；其他步骤按命令或标签认；生成的命令和 10/18 前逐字一样
check("认命令：fetch_task.sh、archive_task.sh",
      A.step_of("bash scripts/queue/fetch_task.sh lab Dataset111_MENrq1/nnUNetTrainer_save10/4") == [("fetch@lab", "Dataset111_MENrq1/nnUNetTrainer_save10/4")]
      and A.step_of("bash scripts/queue/archive_task.sh lab Dataset111_MENrq1/nnUNetTrainer_save10/4") == [("archive", "Dataset111_MENrq1/nnUNetTrainer_save10/4")])
M5 = f"{T}/nnUNet/results/Dataset116_METrq1/nnUNetTrainer_save10__{P}"
trained("Dataset116_METrq1", "nnUNetTrainer_save10", "3")
for p_ in (f"{T}/predictions/Dataset116_METrq1/nnUNetTrainer_save10__{P}/fold_3/MANIFEST.json",):
    os.remove(p_)
todo, _ = A.plan(snap())
check("推理命令逐字和以前一样", [t["cmd"] for t in todo if t["step"] == "infer" and t["key"] == f"{T3}/3"]
      == [f"bash research/RQ1/analysis/run_predict_eval.sh --stage infer {M5} 3"])
lab_task = [dict(id=0, status="Running", command="bash run_predict_eval.sh --other-form", label="infer:t3met_f3", group="gpu", end=None)]
todo, _ = A.plan(dict(snap(), pueue={"box": dict(tasks=lab_task)}))
check("按标签认得（命令写法不同）：不重复排", not [t for t in todo if t["step"] == "infer" and t["key"] == f"{T3}/3"])
inferred("Dataset116_METrq1", "nnUNetTrainer_save10", "3")
_, _, _, by_cmd, _, _ = A.candidates(T, PY, [])
want_cmds = {
    ("eval", f"{T3}/3"): f"bash research/RQ1/analysis/run_predict_eval.sh --stage eval {M5} 3",
    ("oof", "t3met"): f"{PY} research/RQ1/analysis/oof_eval.py {M5}",
    ("paired", "t3met"): f"{PY} research/RQ1/analysis/paired_compare.py --ref {T}/RQ1/analysis/Dataset101_GLIrq1/nnUNetTrainer_save10__{P}/folds_0-1-2-3-4 "
                         f"--cmp T3={T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/oof/eval --out {T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/oof/paired",
    ("ens", "t14met"): f"ENSEMBLE_ONLY=1 bash research/RQ1/analysis/run_predict_eval.sh {T}/nnUNet/results/Dataset121_GLIMETrq1/nnUNetTrainer_save10__{P} '0 1 2 3 4'",
    ("compare_all", "all"): f"{PY} research/RQ1/analysis/compare_all.py",
}
inv = {v: k for k, v in by_cmd.items()}
check("评测、oof、配对、集成、compare_all 的命令逐字和以前一样", all(inv.get(k) == v for k, v in want_cmds.items()))
check("10/18 前的实验一级任务（oof 按 数据集/训练器）认到实验 id：不重复排",
      not [t for t in A.plan(snap([("Running", f"{PY} research/RQ1/analysis/oof_eval.py {T}/nnUNet/results/Dataset121_GLIMETrq1/nnUNetTrainer_save10__{P}", None)]))[0]
           if t["step"] == "oof"])
adhoc = dict(key="runs/x/0", nice=5, recipe="command", exp=None,
             params=dict(key="runs/x/0", cmd="python t.py", env="nnunet", out="runs/x/0", done=["DONE"]))
s_ = snap(remote={"dicc": {"runs/x/0": FIN}})
s_["taskq"]["dicc"]["tasks"].append(adhoc)
todo, _ = A.plan(s_)
check("队列里不属于实验的任务：训完了也拉回（标签 fetch:<key>）",
      [(t["label"], t["cmd"]) for t in todo if t["key"] == "runs/x/0"] == [("fetch:runs/x/0", "bash scripts/queue/fetch_task.sh dicc runs/x/0")])
touch(f"{T}/runs/x/0/DONE")
todo, _ = A.plan(s_)
check("不属于实验的任务拉回以后：没有别的步骤", not [t for t in todo if t["key"] == "runs/x/0"])

# 8. 影子模式：只写日志，不加任务；有 LIVE 才加
calls = []
fake_q = lambda host, args: calls.append(args) or 900  # noqa: E731
done, _ = A.run(snap(remote={"lab": {"Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0": FIN}}), q=fake_q)
check("影子模式：不加任务，日志里写'会排'", calls == [] and "会排 fetch:umamba_f0" in done and "会排 fetch:umamba_f0" in open(os.environ["ADVANCE_LOG"]).read())
n = len(open(os.environ["ADVANCE_LOG"]).readlines())
A.run(snap(remote={"lab": {"Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0": FIN}}), q=fake_q)
check("影子模式：和上次一样就不重复写日志", len(open(os.environ["ADVANCE_LOG"]).readlines()) == n)
touch(os.path.join(A.STATE_DIR, "LIVE"))
done, _ = A.run(snap(remote={"lab": {"Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0": FIN}}), q=fake_q)
fc = [c for c in calls if "fetch:" in c]  # 第 6 节留下的 T14 五折也会排，这里只看拉回
check("有 LIVE：加任务（组、标签、命令都带上）", len(fc) == 1 and fc[0].startswith("wait -p --label fetch:umamba_f0 -- ")
      and "fetch_task.sh lab" in fc[0] and "fetch:umamba_f0（任务 900）" in done)
with open(os.path.join(A.STATE_DIR, "HOLD"), "w") as f:
    f.write("# 注释\nfetch:umamba_f*\n")
calls.clear()
done, _ = A.run(snap(remote={"lab": {"Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0": FIN}}), q=fake_q)
fc = [c for c in calls if "fetch:" in c]
other = [c for c in calls if "fetch:" not in c]
check("HOLD：对上的加成暂停的，别的照常", len(fc) == 1 and fc[0].startswith("wait -p --stashed --label fetch:umamba_f0 -- ")
      and "fetch:umamba_f0（任务 900，暂停（HOLD））" in done and all("--stashed" not in c for c in other))
os.remove(os.path.join(A.STATE_DIR, "HOLD"))
todo, _ = A.plan(dict(pueue={"box": {"error": "x"}}, src={}, taskq={}))
check("box 的 pueue 读不到：什么都不排", todo == [])

# 9. 分到别的机器（OFFLOAD）：传输 → 那边推理 → 拉回 → box 核对 → 清理那边
SW = "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0"
SWM = f"nnUNet/results/Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR__{P}"
NEEDS = [f"{SWM}/dataset.json", f"{SWM}/plans.json", f"{SWM}/fold_0/checkpoint_final.pth", "nnUNet/raw/RQ1_holdout/GLI/imagesTs"]
trained("Dataset101_GLIrq1", "nnUNetTrainer_SwinUNETR", "0")
REMOTE = {"have": set()}
A.step_needs = lambda cmd: NEEDS if "--stage infer" in cmd else None
A.remote_have = lambda name, paths: {p_ for p_ in paths if p_ in REMOTE["have"]}
A.local_gb = lambda paths, root: 5.0
GPU = {"free": True}
A.remote_gpu_free = lambda name, group: GPU["free"]
with open(os.path.join(A.STATE_DIR, "OFFLOAD"), "w") as f:
    f.write("# 大模型\ninfer:swin_v2_f*\ninfer:t3met_f* idle\n")


def osnap(box=(), lab=(), disk=600e9):
    s_ = snap()
    s_["pueue"]["box"]["tasks"] = [dict(id=i, status=st, command=c, label=lb, group=g, end=None) for i, st, c, lb, g in box]
    s_["pueue"]["lab"] = dict(tasks=[dict(id=i, status=st, command=c, label=lb, group=g, end=None) for i, st, c, lb, g in lab])
    s_["probe"] = {"lab": {"DISK": str(int(disk))}}
    return s_


def sw(todo):
    return [t for t in todo if t["label"].endswith("infer:swin_v2_f0")]


todo, stuck = A.plan(osnap())
x = sw(todo)
check("OFFLOAD：缺输入 → box wait 组 push:<标签>，传缺的全部", len(x) == 1 and x[0]["label"] == "push:infer:swin_v2_f0"
      and x[0]["group"] == "wait" and x[0]["cmd"].startswith("bash scripts/queue/offload_io.sh push lab ") and x[0]["paths"] == NEEDS
      and not x[0].get("host"))
check("OFFLOAD：传过去的文件记下来（以后删），文件夹不记", x[0]["record"][1]["pushed"] == [f"{SWM}/fold_0/checkpoint_final.pth"]
      if os.path.isfile(f"{T}/{SWM}/fold_0/checkpoint_final.pth") else False)
todo, stuck = A.plan(osnap(disk=60e9))
check("OFFLOAD：那边的盘不够 → 不传，报出来", not sw(todo) and "盘" in stuck.get("advance:offload:infer:swin_v2_f0", ""))
push = (5, "Running", "bash scripts/queue/offload_io.sh push lab x", "push:infer:swin_v2_f0", "wait")
todo, stuck = A.plan(osnap([push]))
check("OFFLOAD：在传 → 等", not sw(todo) and not stuck.get("advance:offload:infer:swin_v2_f0"))
todo, stuck = A.plan(osnap([(5, "Failed (1)", push[2], push[3], "wait")]))
check("OFFLOAD：传输失败过 → 不重排，报出来", not sw(todo) and "传输任务 5" in stuck["advance:offload:infer:swin_v2_f0"])
REMOTE["have"] = set(NEEDS)
todo, stuck = A.plan(osnap([(5, "Success", push[2], push[3], "wait")]))
x = sw(todo)
check("OFFLOAD：输入齐了 → lab gpu0 组排同样的标签，{data} 换成那边的", len(x) == 1 and x[0]["host"] == "lab" and x[0]["group"] == "gpu0"
      and x[0]["label"] == "infer:swin_v2_f0" and x[0]["cmd"] == f"bash research/RQ1/analysis/run_predict_eval.sh --stage infer ~/BRAIN_TUMOR/{SWM} 0")
rinf = (7, "Running", x[0]["cmd"], "infer:swin_v2_f0", "gpu0")
todo, stuck = A.plan(osnap(lab=[rinf]))
check("OFFLOAD：那边在跑 → 等（box 上也不排）", not sw(todo) and not stuck)
GPU["free"] = False
todo, stuck = A.plan(osnap(lab=[rinf]))
check("OFFLOAD：那边的任务在等别人用完卡（算在跑）→ 不重排、不报", not sw(todo) and not stuck)
GPU["free"] = True
todo, stuck = A.plan(osnap(lab=[(7, "Failed (1)", rinf[2], rinf[3], "gpu0")]))
check("OFFLOAD：那边失败 → 不重排，报出来", not sw(todo) and "lab 任务 7" in stuck["advance:offload:infer:swin_v2_f0"])
todo, stuck = A.plan(osnap(lab=[(7, "Success", rinf[2], rinf[3], "gpu0")]))
x = sw(todo)
check("OFFLOAD：那边跑完 → box wait 组拉回结果文件夹", len(x) == 1 and x[0]["label"] == "pull:infer:swin_v2_f0" and x[0]["group"] == "wait"
      and x[0]["cmd"] == f"bash scripts/queue/offload_io.sh pull lab predictions/Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR__{P}/fold_0")
pull = (9, "Success", x[0]["cmd"], "pull:infer:swin_v2_f0", "wait")
todo, stuck = A.plan(osnap([pull], lab=[(7, "Success", rinf[2], rinf[3], "gpu0")]))
check("OFFLOAD：拉回了但 box 上核对不过 → 报出来", not sw(todo) and "核对还是没做完" in stuck["advance:offload:infer:swin_v2_f0"])
json.dump({"infer:swin_v2_f0": dict(machine="lab", key=SW, out=f"predictions/Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR__{P}/fold_0",
                                     needs=NEEDS, pushed=[NEEDS[2], NEEDS[0]]),
           "infer:swin_v2_f1": dict(machine="lab", key=SW[:-1] + "1", out="o1", needs=[NEEDS[0]], pushed=[])},
          open(os.path.join(A.STATE_DIR, "offload.json"), "w"))
inferred("Dataset101_GLIrq1", "nnUNetTrainer_SwinUNETR", "0")
todo, stuck = A.plan(osnap())
x = [t for t in todo if t.get("op") == "clean"]
check("OFFLOAD：box 上做完 → 清理那边的结果和模型文件（别的分出去的步骤还要用的留着）", len(x) == 1 and x[0]["host"] == "lab"
      and x[0]["paths"] == [f"predictions/Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR__{P}/fold_0", NEEDS[2]])
calls.clear()
rm = []
import refill  # noqa: E402
refill.sh = lambda cmd, host=None, timeout=300: rm.append((cmd, host)) or (0, "")
done, _ = A.run(osnap(), q=fake_q)
st_ = json.load(open(os.path.join(A.STATE_DIR, "offload.json")))
check("OFFLOAD：清理时 ssh 那边 rm，记录里去掉", rm and rm[0][1] == "lab" and rm[0][0].startswith("rm -rf ~/BRAIN_TUMOR/predictions/")
      and "infer:swin_v2_f0" not in st_ and "infer:swin_v2_f1" in st_)
os.remove(os.path.join(A.STATE_DIR, "offload.json"))

# idle：那边空着才分；box 上排着没开始的挪过去，一次一个
for k in ("1", "3"):
    os.remove(f"{T}/predictions/Dataset116_METrq1/nnUNetTrainer_save10__{P}/fold_{k}/MANIFEST.json")
REMOTE["have"] = set(NEEDS)
busy = (3, "Running", "x", "manual", "gpu0")
todo, stuck = A.plan(osnap(lab=[busy]))
check("idle：那边组里有任务 → 照常在 box 排", sorted((t["step"], t["key"], t.get("host")) for t in todo if t["key"].startswith(T3))
      == [("infer", f"{T3}/1", None), ("infer", f"{T3}/3", None)])
GPU["free"] = False
todo, stuck = A.plan(osnap())
check("idle：组空着但卡上有别人 → 照常在 box 排", [t.get("host") for t in todo if t["key"].startswith(T3)] == [None, None])
GPU["free"] = True
todo, stuck = A.plan(osnap())
x = [t for t in todo if t["key"].startswith(T3)]
check("idle：那边空着 → 只分一个过去，另一个照常在 box", sorted((t["key"], t.get("host")) for t in x) == [(f"{T3}/1", "lab"), (f"{T3}/3", None)])
q1 = (20, "Queued", "bash research/RQ1/analysis/run_predict_eval.sh --stage infer x 1", "infer:t3met_f1", "gpu")
q3 = (21, "Queued", "bash research/RQ1/analysis/run_predict_eval.sh --stage infer x 3", "infer:t3met_f3", "gpu")
todo, stuck = A.plan(osnap([q1, q3]))
x = [t for t in todo if t["key"].startswith(T3)]
check("idle：box 上排着没开始 → 挪一个（删 box 任务、在那边排）", len(x) == 1 and x[0]["host"] == "lab" and x[0]["steal"] == [20])
todo, stuck = A.plan(osnap([(20, "Running", q1[2], q1[3], "gpu"), q3], lab=[busy]))
check("idle：那边忙 → 不挪", not [t for t in todo if t["key"].startswith(T3)])
calls.clear()
rm.clear()
done, _ = A.run(osnap([q1, q3]), q=fake_q)
check("idle 挪的时候：先 pueue remove box 任务，再在那边加", rm == [("~/.local/bin/pueue remove 20", None)]
      and any(c.startswith("gpu0 -p --label infer:t3met_f1 -- ") for c in calls))
os.remove(os.path.join(A.STATE_DIR, "OFFLOAD"))
os.remove(os.path.join(A.STATE_DIR, "offload.json"))

# 10. 带 queue 的步骤（10/22，推理）：放进 DICC 的任务队列，box 不跑
for st_, qd_ in QUEUED:
    st_["queue"] = qd_
TC = "Dataset103_GLIrq1_t1c/nnUNetTrainer_save10"
TCM = f"nnUNet/results/Dataset103_GLIrq1_t1c/nnUNetTrainer_save10__{P}"
TCQ = f"infer/Dataset103_GLIrq1_t1c/nnUNetTrainer_save10__{P}/fold_2"
trained("Dataset103_GLIrq1_t1c", "nnUNetTrainer_save10", "2")
TNEEDS = [f"{TCM}/dataset.json", f"{TCM}/fold_2/checkpoint_final.pth", "nnUNet/raw/RQ1_holdout/GLI/imagesTs"]
A.step_needs = lambda cmd: (None if "--stage infer" not in cmd else
                           TNEEDS if "Dataset103_GLIrq1_t1c" in cmd and cmd.endswith(" 2") else [])
REMOTE["have"] = set(TNEEDS)


def tc(todo):
    return [t for t in todo if t["key"] == f"{TC}/2"]


todo, stuck = A.plan(snap())
x = tc(todo)
check("queue：输入齐了 → 加进任务队列（档位 -100、只在 DICC、{data} 留给那边换）", len(x) == 1 and x[0]["op"] == "taskq"
      and x[0]["qnice"] == -100 and f"key={TCQ}" in x[0]["args"] and 'machines=["dicc"]' in x[0]["args"]
      and f"cmd=bash research/RQ1/analysis/infer_task.sh {{data}}/{TCM} 2" in x[0]["args"]
      and 'done=[".infer_done"]' in x[0]["args"] and not any(t["group"] == "gpu" for t in todo))
REMOTE["have"] = set(TNEEDS) - {f"{TCM}/fold_2/checkpoint_final.pth"}
trained("Dataset103_GLIrq1_t1c", "nnUNetTrainer_save10", "2")
todo, stuck = A.plan(snap())
x = tc(todo)
check("queue：DICC 上缺模型文件 → box wait 组先传过去，不加任务", len(x) == 1 and x[0]["step"] == "push" and x[0]["group"] == "wait"
      and x[0]["cmd"] == f"bash scripts/queue/offload_io.sh push dicc {TCM}/fold_2/checkpoint_final.pth")
pq = [("Failed (1)", x[0]["cmd"], None)]
s_ = snap(pq)
s_["pueue"]["box"]["tasks"][0]["label"] = "push:infer:t1c_f2"
todo, stuck = A.plan(s_)
check("queue：传输失败过 → 不重排，报出来", not tc(todo) and "传输任务 0" in stuck.get(f"advance:infer:{TC}/2", ""))
REMOTE["have"] = set(TNEEDS) - {"nnUNet/raw/RQ1_holdout/GLI/imagesTs"}
todo, stuck = A.plan(snap())
check("queue：DICC 上缺测试集文件夹 → 报出来", not tc(todo) and "imagesTs" in stuck.get(f"advance:infer:{TC}/2", ""))
REMOTE["have"] = set(TNEEDS)
s_ = snap()
s_["taskq"]["dicc"]["tasks"].append(dict(key=TCQ, nice=-100))
todo, stuck = A.plan(s_)
check("queue：队列里已经有 → 不再加", not tc(todo) and f"advance:infer:{TC}/2" not in stuck)
touch(f"{T}/predictions/Dataset103_GLIrq1_t1c/nnUNetTrainer_save10__{P}/fold_2/.infer_done")
todo, stuck = A.plan(s_)
check("queue：拉回了但 box 上核对不过 → 报出来", not tc(todo) and "核对还是没做完" in stuck.get(f"advance:infer:{TC}/2", ""))
os.remove(f"{T}/predictions/Dataset103_GLIrq1_t1c/nnUNetTrainer_save10__{P}/fold_2/.infer_done")
inferred("Dataset103_GLIrq1_t1c", "nnUNetTrainer_save10", "2")
s2 = dict(s_, pueue=snap([("Running", f"bash scripts/queue/fetch_task.sh dicc {TCQ}", None)])["pueue"])
todo, stuck = A.plan(s2)
check("queue：MANIFEST 到了但结果还在拉回（10/24）→ 不排评测、不报", not tc(todo) and f"advance:infer:{TC}/2" not in stuck)
todo, stuck = A.plan(s_)
check("queue：拉完了、DICC 上没有评测的输入 → 评测照常在 box 的 cpu 组排（or_here，10/24）",
      [(t["step"], t["group"], t.get("op")) for t in tc(todo)] == [("eval", "cpu", None)])
EIN = [f"predictions/Dataset103_GLIrq1_t1c/nnUNetTrainer_save10__{P}/fold_2/.infer_done", f"{TCM}/fold_2/validation"]
REMOTE["have"] = set(TNEEDS) | set(EIN)
x = tc(A.plan(s_)[0])
check("评测：DICC 上有这一折的预测 → 进任务队列，纯 CPU（device=cpu），结果在分析文件夹",
      len(x) == 1 and x[0]["op"] == "taskq" and x[0]["step"] == "eval" and "device=cpu" in x[0]["args"]
      and f"key=eval/Dataset103_GLIrq1_t1c/nnUNetTrainer_save10__{P}/fold_2" in x[0]["args"]
      and 'done=[".eval_done"]' in x[0]["args"] and not any(a_.startswith(("or_here", "inputs")) for a_ in x[0]["args"]))
old_rh = A.remote_have
A.remote_have = lambda name, paths: None
x = tc(A.plan(s_)[0])
check("评测：连不上 DICC → 照常在 box 上排，不报", [(t["step"], t["group"]) for t in x] == [("eval", "cpu")])
A.remote_have = old_rh
REMOTE["have"] = set(TNEEDS)
os.remove(f"{T}/predictions/Dataset103_GLIrq1_t1c/nnUNetTrainer_save10__{P}/fold_2/MANIFEST.json")
# OFFLOAD 对上的带 queue 的推理（10/24）：lab 0 号卡空着就分到 lab，不进任务队列；不空、或已经在队列里 → 照常
with open(os.path.join(A.STATE_DIR, "OFFLOAD"), "w") as f:
    f.write("infer:t1c_f* idle\n")
GPU["free"] = True
todo, stuck = A.plan(osnap())
x = tc(todo)
check("queue + OFFLOAD idle：那边空着 → 分到 lab gpu0，不加进任务队列", len(x) == 1 and x[0].get("host") == "lab"
      and x[0]["group"] == "gpu0" and x[0].get("op") != "taskq" and "--stage infer" in x[0]["cmd"])
GPU["free"] = False
todo, stuck = A.plan(osnap())
x = tc(todo)
check("queue + OFFLOAD idle：卡上有别人 → 照常进任务队列", len(x) == 1 and x[0].get("op") == "taskq")
GPU["free"] = True
s2 = osnap()
s2["taskq"]["dicc"]["tasks"].append(dict(key=TCQ, nice=-100))
todo, stuck = A.plan(s2)
check("queue + OFFLOAD idle：已经在任务队列里 → 不再分到 lab", not tc(todo))
# 开、停和时段（10/24）
from datetime import datetime as _dt  # noqa: E402
OFFP = os.path.join(A.STATE_DIR, "OFFLOAD")
open(OFFP, "w").write("infer:t1c_f* idle\nhours 20:00-08:00,12:00-13:00\n")
check("hours：跨午夜的时段、几段", A.offload_open(_dt(2026, 10, 24, 21, 0))[0] and A.offload_open(_dt(2026, 10, 24, 7, 59))[0]
      and A.offload_open(_dt(2026, 10, 24, 12, 30))[0] and not A.offload_open(_dt(2026, 10, 24, 8, 0))[0]
      and "不在时段" in A.offload_open(_dt(2026, 10, 24, 15, 0))[1] and A.offload_rules() == [("infer:t1c_f*", True)])
open(OFFP, "w").write("infer:t1c_f* idle\noff\n")
check("off：停着", A.offload_open() == (False, "停着（off）"))
todo, stuck = A.plan(osnap())
x = tc(todo)
check("off：没分过的 → 不分到 lab，照常进任务队列", len(x) == 1 and x[0].get("op") == "taskq")
json.dump({"infer:t1c_f2": dict(machine="lab", key=f"{TC}/2", out="o", needs=[], pushed=[])}, open(os.path.join(A.STATE_DIR, "offload.json"), "w"))
RINF = (7, "Running", "bash research/RQ1/analysis/run_predict_eval.sh --stage infer x 2", "infer:t1c_f2", "gpu0")
todo, stuck = A.plan(osnap(lab=[RINF]))
x = tc(todo)
check("off：那边在跑 → 停掉那边的任务、这一步进任务队列", sorted(t.get("op") for t in x) == ["stop", "taskq"]
      and next(t for t in x if t.get("op") == "stop")["ids"] == [7])
todo, stuck = A.plan(osnap(lab=[(7, "Success", RINF[2], RINF[3], "gpu0")]))
x = tc(todo)
check("off：那边已经做完 → 照常拉回，不进任务队列", [t["label"] for t in x] == ["pull:infer:t1c_f2"])
rm.clear()
calls.clear()
A.run(osnap(lab=[RINF]), q=fake_q, tq=lambda *a: (0, ""))
check("off：run 时 ssh lab 先 pueue kill、等停下、再 remove", rm and rm[0][1] == "lab" and rm[0][0].startswith("~/.local/bin/pueue kill 7;")
      and rm[0][0].endswith("~/.local/bin/pueue remove 7"))
open(OFFP, "w").write("infer:t1c_f* idle\n")
os.remove(os.path.join(A.STATE_DIR, "offload.json"))
os.remove(os.path.join(A.STATE_DIR, "OFFLOAD"))
old_box = ("Stashed", f"bash research/RQ1/analysis/run_predict_eval.sh --stage infer {T}/{TCM} 2", None)
todo, stuck = A.plan(snap([old_box]))
check("queue：box pueue 里已经有这一步（10/22 前排的）→ 不加", not tc(todo))
added = []
fake_tq = lambda nice, args: added.append((nice, args)) or (0, "加入")  # noqa: E731
done, _ = A.run(snap(), q=lambda *a: 0, tq=fake_tq)
check("queue：有 LIVE 时调用 taskq.py add（档位 -100）", any(n_ == -100 and f"key={TCQ}" in a_ for n_, a_ in added)
      and "任务队列 infer:t1c_f2" in done)
added.clear()
with open(os.path.join(A.STATE_DIR, "HOLD"), "w") as f:
    f.write("infer:*\n")
done, _ = A.run(snap(), q=lambda *a: 0, tq=fake_tq)
check("queue：HOLD 对上的先不加", not any(f"key={TCQ}" in a_ for _, a_ in added))
os.remove(os.path.join(A.STATE_DIR, "HOLD"))

# 11. 实验一级带 queue 的步骤（10/23，P1 / P2 换模型重跑）：五折都在 box 上以后放进 DICC 的任务队列，拉回后 box 上做比较
for e_, g_ in GQUEUED:
    e_["group_after"] = g_
MEN = "Dataset111_MENrq1"
MENM = f"nnUNet/results/{MEN}/nnUNetTrainer_save10__{P}"
for k in F5:
    trained(MEN, "nnUNetTrainer_save10", k)
PIN = (["RQ1/probe/p1/inputs", "RQ1/probe/p1/targets.csv", "RQ1/probe/p2/inputs", "RQ1/probe/p2/lesions",
        f"nnUNet/preprocessed/{MEN}/splits_final.json", f"{MENM}/dataset.json", f"{MENM}/plans.json"]
       + [f"{MENM}/fold_{k}/checkpoint_final.pth" for k in F5])
REMOTE["have"] = set(PIN)


PSTEPS = ("p1", "p2", "p2cmp", "push")


def pr(todo):
    return sorted((t["step"], t.get("op", t["group"])) for t in todo if t["key"] == "t3men" and t["step"] in PSTEPS)


todo, stuck = A.plan(snap())
x = {t["step"]: t for t in todo if t["key"] == "t3men" and t["step"] in PSTEPS}
check("probe：五折都在 box 上、DICC 上输入齐了 → p1、p2 都加进任务队列（档位 -100），p2cmp 先不排",
      pr(todo) == [("p1", "taskq"), ("p2", "taskq")] and x["p2"]["qnice"] == -100
      and "key=probe/p2_t3men" in x["p2"]["args"] and 'machines=["dicc"]' in x["p2"]["args"]
      and f"cmd=MODEL={{data}}/{MENM} TAG=t3men bash research/RQ1/probe/p2_predict_model.sh && touch {{data}}/RQ1/probe/p2_t3men/.probe_done"
      in x["p2"]["args"] and "out=RQ1/probe/p2_t3men" in x["p2"]["args"] and not any(a_.startswith("inputs=") for a_ in x["p2"]["args"]))
REMOTE["have"] = set(PIN) - {"RQ1/probe/p2/inputs"}
todo, stuck = A.plan(snap())
check("probe：DICC 上缺 P2 的输入文件夹 → 报出来，p1 照常加", pr(todo) == [("p1", "taskq")]
      and "RQ1/probe/p2/inputs" in stuck.get("advance:p2:t3men", ""))
REMOTE["have"] = set(PIN) - {f"{MENM}/fold_3/checkpoint_final.pth"}
todo, stuck = A.plan(snap())
x = [t for t in todo if t["key"] == "t3men" and t["step"] in PSTEPS]
check("probe：DICC 上缺一折的模型文件 → 先传过去（只排一次传输），不加任务",
      [t["step"] for t in x] == ["push"] and x[0]["cmd"] == f"bash scripts/queue/offload_io.sh push dicc {MENM}/fold_3/checkpoint_final.pth"
      and x[0]["label"] in ("push:p1:t3men", "push:p2:t3men"))
REMOTE["have"] = set(PIN)
s_ = snap()
s_["taskq"]["dicc"]["tasks"] += [dict(key="probe/p1_t3men", nice=-100), dict(key="probe/p2_t3men", nice=-100)]
todo, stuck = A.plan(s_)
check("probe：队列里已经有 → 不再加", pr(todo) == [] and "advance:p2:t3men" not in stuck)
touch(f"{T}/RQ1/probe/p2_t3men/.probe_done")
todo, stuck = A.plan(s_)
x = [t for t in todo if t["key"] == "t3men" and t["step"] in PSTEPS]
check("probe：p2 拉回了 → box cpu 组排和 T0 的比较", [(t["step"], t["group"]) for t in x] == [("p2cmp", "cpu")]
      and x[0]["cmd"] == f"{PY} research/RQ1/probe/p2_compare.py --tags t3men --out {T}/RQ1/probe/p2_compare_t3men"
      and x[0]["label"] == "p2cmp:t3men")
touch(f"{T}/RQ1/probe/p2_compare_t3men/p2_compare.run.json", '{"status": "ok"}')
touch(f"{T}/RQ1/probe/p1_t3men/.probe_done")
todo, stuck = A.plan(s_)
check("probe：p1、p2、p2cmp 都做完 → 没有要排的", pr(todo) == [] and not [k for k in stuck if k.split(":")[1:2] and k.split(":")[1] in PSTEPS and k.endswith(":t3men")])

print("\n全部通过" if ok else "\n有失败")
sys.exit(0 if ok else 1)
