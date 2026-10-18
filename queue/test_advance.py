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
P = A.PLANS
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
      f["group"] == "wait" and f["cmd"] == f"bash scripts/queue/fetch_fold.sh dicc Dataset121_GLIMETrq1/nnUNetTrainer_save10__{P} 0"
      and f["label"] == "fetch:t14met_f0")
check("按队列的档位排在前面（档位 0 的 T14 先于没有档位的 T3）", todo[0]["key"] == f"{MET}/0")

# 2. 已经有拉回任务在跑（以前手动排的）→ 不重复
old = ("Running", f"bash scripts/queue/fetch_fold.sh dicc Dataset121_GLIMETrq1/nnUNetTrainer_save10__{P} 0", None)
todo, stuck = A.plan(snap([old], remote={"dicc": {f"{MET}/0": FIN}}))
check("已经有活着的拉回任务：不重复排", todo == [] and not stuck)
other = ("Running", f"bash scripts/queue/fetch_fold.sh lab Dataset121_GLIMETrq1/nnUNetTrainer_save10__{P} 0", None)
todo, stuck = A.plan(snap([other], remote={"dicc": {f"{MET}/0": FIN}}))
check("活着的拉回任务在等另一台机器（以前预先排的链）：照样从训完的机器拉，旧任务报出来",
      steps(todo) == [("fetch", f"{MET}/0")] and "fetch_fold.sh dicc" in todo[0]["cmd"] and f"advance:stale_fetch:{MET}/0:0" in stuck)
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
stale = ("Queued", f"bash scripts/queue/fetch_fold.sh lab Dataset116_METrq1/nnUNetTrainer_save10__{P} 1", None)
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
check("T3 五折到齐：排 oof（不排集成）", steps(todo) == [("oof", T3)] and todo[0]["group"] == "cpu" and "oof_eval.py" in todo[0]["cmd"])
touch(f"{T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/oof/oof_eval.run.json", '{"status": "ok"}')
todo, _ = A.plan(snap())
check("oof 做完：排配对（--cmp T3=…/oof/eval）", steps(todo) == [("paired", T3)] and "--cmp T3=" in todo[0]["cmd"]
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
for k in A.F5:
    trained("Dataset121_GLIMETrq1", "nnUNetTrainer_save10", k)
    inferred("Dataset121_GLIMETrq1", "nnUNetTrainer_save10", k)
    evaluated("Dataset121_GLIMETrq1", "nnUNetTrainer_save10", f"fold_{k}")
todo, _ = A.plan(snap())
ens = [t for t in todo if t["step"] == "ens"]
check("T14 五折到齐：oof 和五折集成都排，compare_all 先不排", steps(todo) == [("ens", MET), ("oof", MET)]
      and ens[0]["cmd"].endswith("'0 1 2 3 4'") and ens[0]["cmd"].startswith("ENSEMBLE_ONLY=1 "))
old_ens = ("Running", f"bash research/RQ1/analysis/run_predict_eval.sh {T}/nnUNet/results/Dataset121_GLIMETrq1/nnUNetTrainer_save10__{P} '0 1 2 3 4'", None)
todo, _ = A.plan(snap([old_ens]))
check("集成任务在跑：认得出来，不重复", steps(todo) == [("oof", MET)])
todo, _ = A.plan(snap([("Running", ens[0]["cmd"], None)]))
check("带 ENSEMBLE_ONLY=1 的集成任务也认得出来", steps(todo) == [("oof", MET)])

# 6b. 推理的完整性：分割、概率图、输入病例数要对得上
src = f"{T}/raw/X/imagesTs"
for c in ("a", "b"):
    touch(f"{src}/{c}_0000.nii.gz")
man = f"{T}/predictions/Dataset116_METrq1/nnUNetTrainer_save10__{P}/fold_2/MANIFEST.json"
def manifest(nseg, nprob):
    touch(man, json.dumps({"probabilities_zeroed_below": 1e-5, "cohorts": {"X": dict(
        n_segmentations=nseg, n_probabilities=nprob, nnunet_args=dict(list_of_lists_or_source_folder=src))}}))
box = A.Box(T)
manifest(2, 1)
check("概率图比分割少：推理没做完", not box.inferred("Dataset116_METrq1", "nnUNetTrainer_save10", "2"))
manifest(1, 1)
check("比输入病例少：推理没做完", not box.inferred("Dataset116_METrq1", "nnUNetTrainer_save10", "2"))
manifest(2, 2)
check("都对得上：推理做完", box.inferred("Dataset116_METrq1", "nnUNetTrainer_save10", "2"))

# 6c. T0 五折在 lab 上的副本不自动归档（删不删还没定）
T0K = "Dataset101_GLIrq1/nnUNetTrainer_save10/0"
trained("Dataset101_GLIrq1", "nnUNetTrainer_save10", "0")
todo, _ = A.plan(snap(remote={"lab": {T0K: FIN}}))
check("T0 nnU-Net 的 lab 副本：不排归档", not [t for t in todo if t["step"] == "archive"])
UMK = "Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/3"
trained("Dataset101_GLIrq1", "nnUNetTrainer_UMambaEnc", "3")
todo, _ = A.plan(snap(remote={"lab": {UMK: FIN}}))
check("同属 T0 的 U-Mamba：照常归档", ("archive", UMK) in steps(todo))

# 7. 认命令
check("认命令：paired_compare 按 --out 认", A.step_of(f"py paired_compare.py --ref x --cmp T3=y --out {T}/RQ1/analysis/{T3.split('/')[0]}/nnUNetTrainer_save10__{P}/oof/paired") == [("paired", T3)])
check("认命令：以前的 archive_lab_fold.sh", A.step_of(f"bash scripts/queue/archive_lab_fold.sh Dataset111_MENrq1/nnUNetTrainer_save10__{P} 4")
      == [("archive", "Dataset111_MENrq1/nnUNetTrainer_save10/4")])
check("认命令：拉回带源机器、前面有环境变量也认得",
      A.step_of(f"bash scripts/queue/fetch_fold.sh dicc Dataset111_MENrq1/nnUNetTrainer_save10__{P} 4") == [("fetch@dicc", "Dataset111_MENrq1/nnUNetTrainer_save10/4")]
      and A.step_of(f"NO_TTA=1 bash research/RQ1/analysis/run_predict_eval.sh /x/Dataset111_MENrq1/nnUNetTrainer_save10__{P} 4")
      == [("infer", "Dataset111_MENrq1/nnUNetTrainer_save10/4"), ("eval", "Dataset111_MENrq1/nnUNetTrainer_save10/4")])
check("认命令：不认识的命令 → []", A.step_of("pueue parallel 2 --group cpu") == [] and A.step_of("echo 'unbalanced") == [])

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
      and "fetch_fold.sh lab" in fc[0] and "fetch:umamba_f0（任务 900）" in done)
todo, _ = A.plan(dict(pueue={"box": {"error": "x"}}, src={}, taskq={}))
check("box 的 pueue 读不到：什么都不排", todo == [])

print("\n全部通过" if ok else "\n有失败")
sys.exit(0 if ok else 1)
