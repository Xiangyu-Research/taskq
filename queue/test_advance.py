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
os.environ["PHD_TASKS"] = os.path.join(T, "tasks.json")
os.environ["PHD_LAST"] = os.path.join(T, "last")
sys.path.insert(0, HERE)
import advance as A  # noqa: E402
REAL_STEP_NEEDS = A.step_needs


def legacy_index():
    """10/25 以前按命令和标签认 box、lab 任务的规则 → ({命令: (步骤, key)}, {标签: (步骤, key)})。测试里用它给快照配记录。"""
    tasks, groups, final = A.candidates(T, PY, [])
    by_cmd, by_label = {}, {}
    for t in tasks:
        for st in (t["e"] or {}).get("after", []):
            by_cmd[A.S.fill(st["cmd"], t["names"], keep=())] = (st["name"], t["key"])
            by_label[f"{st['name']}:{t['lab']}"] = (st["name"], t["key"])
    for e in A.E.load()[0]:
        for chain in e.get("group_after", []):
            for st in chain:
                by_cmd[A.S.fill(st["cmd"], groups[e["id"]], keep=())] = (st["name"], e["id"])
                by_label[f"{st['name']}:{e['id']}"] = (st["name"], e["id"])
    for f in final:
        by_cmd[A.S.fill(f["cmd"], dict(data=T, python=PY), keep=())] = (f["name"], "all")
        by_label[f["name"]] = (f["name"], "all")
    return by_cmd, by_label


def legacy_records(s):
    """快照里每个 pueue 任务按旧规则认出来的记录（health.py 平时从任务记录读到的那种）。"""
    by_cmd, by_label = legacy_index()
    out = []
    for h, d in s.get("pueue", {}).items():
        for x in (d or {}).get("tasks") or []:
            lb, cmd = x.get("label") or "", (x.get("command") or "").strip()
            rec = dict(machine=h, id=x["id"], label=lb, added=0)
            if lb.startswith(("pull:", "push:")):
                hit = by_label.get(lb[5:])
                tok = cmd.split()
                where = tok[tok.index("offload_io.sh") + 2:] if "offload_io.sh" in tok else ["?"]
                rec.update(step=lb[:4], of=hit[0] if hit else lb[5:].split(":")[0], key=hit[1] if hit else lb[5:].split(":", 1)[-1])
                rec.update(dict(src=where[0]) if lb.startswith("pull:") else dict(to=where[0], paths=where[1:]))
            else:
                hit = by_cmd.get(cmd) or by_label.get(lb)
                if not hit:
                    continue
                rec.update(step=hit[0], key=hit[1])
            out.append(rec)
    return out


_plan = A.plan


def fields(status):
    """qs 从 status 拆出的 result、code（如 "Failed (137)" → Failed、137）。"""
    import re
    m = re.fullmatch(r"(\w+)(?: \((-?\d+)\))?", status)
    done = m[1] not in ("Running", "Queued", "Stashed", "Paused", "Locked")
    return dict(result=m[1] if done else None, code=int(m[2]) if m[2] else 0 if m[1] == "Success" else None)


def plan_with_records(s, *a, **kw):
    for d in (s.get("pueue") or {}).values():  # 测试里只写 status，result、code 每次按它补上（和 qs 一样）
        for x in (d or {}).get("tasks") or []:
            x.update(fields(x["status"]))
    if "records" not in s:  # 任务记录（run 写的）+ 按旧规则给快照里的任务配的
        s = dict(s, records=A.C.load_records() + legacy_records(s))
    return _plan(s, *a, **kw)


A.plan = plan_with_records

A.taskq_add = lambda nice, args: (0, "")  # 不真的加进总队列
A.feed.sites = lambda: ["dicc", "lab"]  # 执行机（10/31 第二阶段起 lab 也有缓冲）
REAL_PLACE = dict(A.P.PLACE, eval=["queue:dicc have", "box:cpu"], ens=["queue:dicc have", "box:cpu"])  # 按平时的写法测（DICC 停机时 policy 临时改过）
A.P.PLACE = {}  # 前面几节：每一步都在 box 上（实验表里写的组）；第 9 节起测别的地方
A.P.AUTO = dict(A.P.AUTO, advance=False)
A.P.HOLD = []
# 实验一级带 queue 的几串（10/23，P1 / P2 重跑）也先拿掉，第 11 节再放回来测
PROBE_STEPS = ("p1", "p2", "p2cmp")
GQUEUED = [(e, e["group_after"]) for e in A.E.load()[0] if any(st["name"] in PROBE_STEPS for c in e.get("group_after", []) for st in c)]
for e_, g_ in GQUEUED:
    e_["group_after"] = [c for c in g_ if not any(st["name"] in PROBE_STEPS for st in c)]
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


def pruned(ds, tr):
    """各折的单折概率图删过了（prune_fold_probs.py 写的记录）。"""
    import glob
    for mp in glob.glob(f"{T}/predictions/{ds}/{tr}__{P}/fold_*/MANIFEST.json"):
        m = json.load(open(mp))
        m["probabilities_removed"] = dict(status="done", cohorts={})
        json.dump(m, open(mp, "w"))


def snap(tasks=(), remote=None):
    rows = [dict(id=i, status=st, command=c, label="", group="x", end=end) for i, (st, c, end) in enumerate(tasks)]
    return dict(pueue={"box": dict(tasks=rows)}, src={h: dict(folds=f) for h, f in (remote or {}).items()},
                taskq={"box": dict(tasks=[dict(key=f"Dataset121_GLIMETrq1/nnUNetTrainer_save10/{k}", nice=0) for k in range(5)])})


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
check("活着的拉回任务在等另一台机器：照样从训完的机器拉", steps(todo) == [("fetch", f"{MET}/0")] and "fetch_task.sh dicc" in todo[0]["cmd"])
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

# 4. 推理做完 → 评测（cpu）；评测做完 → 这一折没事了
inferred("Dataset116_METrq1", "nnUNetTrainer_save10", "1")
todo, _ = A.plan(snap())
check("推理做完：排评测（cpu 组、--stage eval）", steps(todo) == [("eval", f"{T3}/1")] and todo[0]["group"] == "cpu" and "--stage eval" in todo[0]["cmd"])
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
check("依赖失败（DependencyFailed）也算失败：不重排，报出来", todo == [] and "DependencyFailed" in stuck.get(f"advance:infer:{T3}/3", ""))
todo, stuck = A.plan(snap([("Success", cmd3, (now - timedelta(hours=8)).isoformat())]))
check("8 小时前成功过、结果没了：也报出来（不按时间放开，pueue remove 后重排）", todo == [] and "成功跑完" in stuck[f"advance:infer:{T3}/3"])
todo, stuck = A.plan(snap([("Failed (1)", cmd3, None), ("Success", cmd3, None), ("Success", "echo x", None)]))
check("失败之后又成功过：只看最后一次成功之后的（报的是成功跑完没结果，不是那次失败）",
      todo == [] and "任务 1 成功跑完" in stuck[f"advance:infer:{T3}/3"])

# 6. 五折到齐：T3 → oof → paired，集成 → 删单折概率图；T14 一样；compare_all 等 paired、ens 和前面的步骤做完再排（prune 不等）
for k in "0", "4":
    trained("Dataset116_METrq1", "nnUNetTrainer_save10", k)
    inferred("Dataset116_METrq1", "nnUNetTrainer_save10", k)
    evaluated("Dataset116_METrq1", "nnUNetTrainer_save10", f"fold_{k}")
inferred("Dataset116_METrq1", "nnUNetTrainer_save10", "3")
evaluated("Dataset116_METrq1", "nnUNetTrainer_save10", "fold_3")
todo, _ = A.plan(snap())
oof_ = [t for t in todo if t["step"] == "oof"]
check("T3 五折到齐：排 oof 和集成，不排删单折概率图", steps(todo) == [("ens", "t3met"), ("oof", "t3met")]
      and oof_[0]["group"] == "cpu" and "oof_eval.py" in oof_[0]["cmd"])
evaluated("Dataset116_METrq1", "nnUNetTrainer_save10", "folds_0-1-2-3-4")
_er = f"{T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/folds_0-1-2-3-4/report/report.run.json"
os.utime(_er, (os.path.getmtime(_er) - 3600,) * 2)  # 集成比下面的 compare_all 早做完
touch(f"{T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/oof/oof_eval.run.json", '{"status": "ok"}')
todo, _ = A.plan(snap())
pa = [t for t in todo if t["step"] == "paired"]
check("oof、集成做完：排配对（--cmp T3=…/oof/eval）和删单折概率图", steps(todo) == [("paired", "t3met"), ("prune", "t3met")]
      and "--cmp T3=" in pa[0]["cmd"] and pa[0]["cmd"].endswith("oof/paired"))
touch(f"{T}/RQ1/analysis/Dataset116_METrq1/nnUNetTrainer_save10__{P}/oof/paired/paired_compare.run.json", '{"status": "ok"}')
todo, _ = A.plan(snap())
check("配对做完、compare_all 比它旧：排 compare_all，删单折概率图在排不挡它", steps(todo) == [("compare_all", "all"), ("prune", "t3met")])
todo, _ = A.plan(snap([("Queued", [t for t in todo if t["step"] == "prune"][0]["cmd"], None)]))
check("删单折概率图排着（HOLD 暂停的也算）：认得出来、不重复，compare_all 照常", steps(todo) == [("compare_all", "all")])
pruned("Dataset116_METrq1", "nnUNetTrainer_save10")
todo, _ = A.plan(snap())
check("单折概率图删完：只剩 compare_all", steps(todo) == [("compare_all", "all")])
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
check("T14 五折到齐：oof 和五折集成都排，删单折概率图、compare_all 先不排", steps(todo) == [("ens", "t14met"), ("oof", "t14met")]
      and ens[0]["cmd"].endswith("'0 1 2 3 4'") and ens[0]["cmd"].startswith("ENSEMBLE_ONLY=1 "))
todo, _ = A.plan(snap([("Running", ens[0]["cmd"], None)]))
check("集成任务在跑：认得出来，不重复", steps(todo) == [("oof", "t14met")])
# 删单折概率图（10/25）：集成做完、每折都评测完、没有在往 box 上拷的才排
M14 = f"Dataset121_GLIMETrq1/nnUNetTrainer_save10__{P}"
evaluated("Dataset121_GLIMETrq1", "nnUNetTrainer_save10", "folds_0-1-2-3-4")
os.remove(f"{T}/RQ1/analysis/{M14}/fold_2/report/report.run.json")
todo, _ = A.plan(snap())
check("删单折概率图：集成做完、但 fold 2 还没评测完 → 不删（先评测）", ("prune", "t14met") not in steps(todo) and ("eval", f"{MET}/2") in steps(todo))
evaluated("Dataset121_GLIMETrq1", "nnUNetTrainer_save10", "fold_2")
todo, _ = A.plan(snap())
pr_ = [t for t in todo if t["step"] == "prune"]
check("删单折概率图：集成和每折评测都做完 → 排（box cpu 组，命令带五折）",
      steps(todo) == [("oof", "t14met"), ("prune", "t14met")] and pr_[0]["group"] == "cpu" and pr_[0]["label"] == "prune:t14met"
      and pr_[0]["cmd"] == f"{PY} research/RQ1/analysis/prune_fold_probs.py {T}/predictions/{M14} --folds '0 1 2 3 4'")
s_ = snap()
s_["pueue"]["box"]["tasks"].append(dict(id=98, status="Running", command="bash scripts/queue/offload_io.sh pull lab x",
                                        label="pull:infer:t14met_f1", group="wait", end=None))
todo, _ = A.plan(s_)
check("删单折概率图：fold 1 的推理结果还在往 box 上拉 → 不删", ("prune", "t14met") not in steps(todo))
s_["pueue"]["box"]["tasks"][-1]["label"] = "pull:eval:t14met_f3"
todo, _ = A.plan(s_)
check("删单折概率图：fold 3 的评测结果还在往 box 上拉 → 不删", ("prune", "t14met") not in steps(todo))
todo, stuck = A.plan(snap([("Success", pr_[0]["cmd"], None)]))
check("删单折概率图成功跑完、盘上还有单折概率图 → 报出来，不重排", ("prune", "t14met") not in steps(todo) and "advance:prune:t14met" in stuck)
pruned("Dataset121_GLIMETrq1", "nnUNetTrainer_save10")
todo, stuck = A.plan(snap([("Success", pr_[0]["cmd"], None)]))
check("删完了：不再排，也不报", ("prune", "t14met") not in steps(todo) and "advance:prune:t14met" not in stuck)
touch(f"{T}/predictions/{M14}/fold_4/MEN/x.npz")
check("删完以后又拉回来一个单折概率图（sync）→ 不算做完", not A.E.experiment("t14met")["group_after"][1][1]["done"].ok(T, A.E.group_names(A.E.experiment("t14met"), T, PY)))
os.remove(f"{T}/predictions/{M14}/fold_4/MEN/x.npz")
# T0 和三种架构：五折、集成都做完也不删单折概率图（测完删掉这些假文件，后面几节要 Dataset101 还没训）
import shutil  # noqa: E402
for k in F5:
    for tr in ("nnUNetTrainer_save10", "nnUNetTrainer_UMambaEnc"):
        trained("Dataset101_GLIrq1", tr, k)
        inferred("Dataset101_GLIrq1", tr, k)
        evaluated("Dataset101_GLIrq1", tr, f"fold_{k}")
for tr in ("nnUNetTrainer_save10", "nnUNetTrainer_UMambaEnc"):
    evaluated("Dataset101_GLIrq1", tr, "folds_0-1-2-3-4")
todo, _ = A.plan(snap())
keep = ("t0", "swin_v2", "umamba", "stunet")
check("T0、U-Mamba 五折和集成都做完：不排删单折概率图", not [t for t in todo if t["step"] == "prune"])
for d in ("nnUNet/results", "predictions", "RQ1/analysis"):
    shutil.rmtree(f"{T}/{d}/Dataset101_GLIrq1")
check("实验表：T0 和三种架构没有 prune 步骤", all("prune" not in [st["name"] for c in A.E.experiment(i)["group_after"] for st in c] for i in keep))

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
check("MANIFEST 对得上、盘上没有这些文件（拷到一半，10/24）：推理没做完", not inf_ok("2"))
for c in ("a", "b"):
    touch(f"{os.path.dirname(man)}/X/{c}.nii.gz")
    touch(f"{os.path.dirname(man)}/X/{c}.npz")
check("都对得上：推理做完", inf_ok("2"))
m_ = json.load(open(man))
m_["probabilities_removed"] = dict(status="done", cohorts={"X": 2})
touch(man, json.dumps(m_))
os.remove(f"{os.path.dirname(man)}/X/a.npz")
os.remove(f"{os.path.dirname(man)}/X/b.npz")
check("单折概率图按记录删掉了（10/25）：推理仍算做完，不重新推理", inf_ok("2"))
os.remove(f"{os.path.dirname(man)}/X/a.nii.gz")
check("删了概率图、又少了分割：推理没做完", not inf_ok("2"))
del m_["probabilities_removed"]
touch(man, json.dumps(m_))
for c in ("a", "b"):
    touch(f"{os.path.dirname(man)}/X/{c}.nii.gz")
    touch(f"{os.path.dirname(man)}/X/{c}.npz")

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

# 7. 没有记录的任务按命令认：拉回（带源机器）、归档；别的命令认不出
K4 = "Dataset111_MENrq1/nnUNetTrainer_save10/4"
check("认命令：归档", A.C.from_command(f"bash scripts/queue/archive_task.sh lab {K4}") == dict(step="archive", src="lab", key=K4))
check("认命令：拉回带源机器", A.C.from_command(f"bash scripts/queue/fetch_task.sh dicc {K4}") == dict(step="fetch", src="dicc", key=K4))
check("认命令：不认识的命令 → None", A.C.from_command("pueue parallel 2 --group cpu") is None and A.C.from_command("echo 'unbalanced") is None)
nr = [dict(machine="box", id=7, label="eval:t3met_f4", added=0, step="eval", key=K4)]
check("有记录的按记录认（任务号和标签都对上）；任务号被别的任务重用（标签不同）→ 不认",
      A.C.identify("box", dict(id=7, label="eval:t3met_f4", command="x"), nr)["step"] == "eval"
      and A.C.identify("box", dict(id=7, label="other", command="x"), nr) is None
      and A.C.identify("lab", dict(id=7, label="eval:t3met_f4", command="x"), nr) is None)

# 7b. 生成的命令和 10/18 前逐字一样
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
by_cmd = legacy_index()[0]
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
adhoc = dict(key="runs/x/0", nice=5, recipe="command", exp=None,
             params=dict(key="runs/x/0", cmd="python t.py", env="nnunet", out="runs/x/0", done=["DONE"]))
s_ = snap(remote={"dicc": {"runs/x/0": FIN}})
s_["taskq"]["box"]["tasks"].append(adhoc)
todo, _ = A.plan(s_)
check("队列里不属于实验的任务：训完了也拉回（标签 fetch:<key>）",
      [(t["label"], t["cmd"]) for t in todo if t["key"] == "runs/x/0"] == [("fetch:runs/x/0", "bash scripts/queue/fetch_task.sh dicc runs/x/0")])
touch(f"{T}/runs/x/0/DONE")
todo, _ = A.plan(s_)
check("不属于实验的任务拉回以后：没有别的步骤", not [t for t in todo if t["key"] == "runs/x/0"])

# 8. AUTO["advance"] = False：只写日志，不加任务；True 才加
calls = []
fake_q = lambda host, args: calls.append(args) or 900  # noqa: E731
done, _ = A.run(snap(remote={"lab": {"Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0": FIN}}), q=fake_q)
check("只算不排：不加任务，日志里写'会排'", calls == [] and "会排 fetch:umamba_f0" in done and "会排 fetch:umamba_f0" in open(os.environ["ADVANCE_LOG"]).read())
n = len(open(os.environ["ADVANCE_LOG"]).readlines())
A.run(snap(remote={"lab": {"Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0": FIN}}), q=fake_q)
check("只算不排：和上次一样就不重复写日志", len(open(os.environ["ADVANCE_LOG"]).readlines()) == n)
A.P.AUTO["advance"] = True
done, _ = A.run(snap(remote={"lab": {"Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0": FIN}}), q=fake_q)
fc = [c for c in calls if "fetch:" in c]  # 第 6 节留下的 T14 五折也会排，这里只看拉回
check("AUTO 开着：加任务（组、标签、命令都带上）", len(fc) == 1 and fc[0].startswith("wait -p --label fetch:umamba_f0 -- ")
      and "fetch_task.sh lab" in fc[0] and "fetch:umamba_f0（任务 900）" in done)
A.P.HOLD = ["fetch:umamba_f*"]
calls.clear()
done, _ = A.run(snap(remote={"lab": {"Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0": FIN}}), q=fake_q)
fc = [c for c in calls if "fetch:" in c]
other = [c for c in calls if "fetch:" not in c]
check("HOLD：对上的加成暂停的，别的照常", len(fc) == 1 and fc[0].startswith("wait -p --stashed --label fetch:umamba_f0 -- ")
      and "fetch:umamba_f0（任务 900，暂停（HOLD））" in done and all("--stashed" not in c for c in other))
A.P.HOLD = []
todo, _ = A.plan(dict(pueue={"box": {"error": "x"}}, src={}, taskq={}))
check("box 的 pueue 读不到：什么都不排", todo == [])

# 9. 后面几节用的：别的机器上有哪些输入（REMOTE）、lab 的 pueue 快照；PLACE 写错了要报
inferred("Dataset101_GLIrq1", "nnUNetTrainer_save10", "0")  # 第 6c 节训完的两折：先推理完，后面几节不看它们
inferred("Dataset101_GLIrq1", "nnUNetTrainer_UMambaEnc", "3")
REMOTE = {"have": set()}
A.remote_have = lambda name, paths: {p_ for p_ in paths if p_ in REMOTE["have"]}
import refill  # noqa: E402
refill.sh = lambda cmd, host=None, timeout=300: (0, "")  # 清理时不真的 ssh


def osnap(box=(), lab=()):
    s_ = snap()
    s_["pueue"]["box"]["tasks"] = [dict(id=i, status=st, command=c, label=lb, group=g, end=None) for i, st, c, lb, g in box]
    s_["pueue"]["lab"] = dict(tasks=[dict(id=i, status=st, command=c, label=lb, group=g, end=None) for i, st, c, lb, g in lab])
    return s_


bad_ = []
for pl in ({"infer": ["box:gpu9"]}, {"infer": ["queue:dicc idel"]}, {"nosuch": ["box:cpu"]}, {"infer": ["dicc:x"]},
           {"infer": ["lab:gpu0"]}, {"infer": ["box:gpu idle"]}, {"infer": ["box:gpu hours=20:00-08:00"]}, {"infer": ["trainer manual"]}):
    A.P.PLACE = pl
    todo, stuck = A.plan(osnap())
    if todo or "advance:policy" not in stuck:
        bad_.append(pl)
check("PLACE 写错了（没有的组、不认识的条件、没有的步骤、不认识的地方，包括已经不用的 <机器>:<组>、idle、hours=、manual）→ 什么都不排，报出来",
      not bad_)
A.P.PLACE = {"infer": ["lab:gpu0"]}
check("PLACE 写 lab:gpu0：报不认识的地方", "不认识的地方" in A.plan(osnap())[1].get("advance:policy", ""))

# 10. 进任务队列（PLACE 写 queue:<机器>，现在的推理、评测）：缺的文件先传过去，box 不跑
BASE = {k: v for k, v in REAL_PLACE.items() if k not in ("p1", "p2")}  # P1 / P2 第 11 节再测
A.P.PLACE = dict(BASE)
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
s_["taskq"]["box"]["tasks"].append(dict(key=TCQ, nice=-100))
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
check("queue：拉完了、DICC 上没有评测的输入 → 评测用下一个地方（box 的 cpu 组，queue:dicc have）",
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
old_box = ("Stashed", f"bash research/RQ1/analysis/run_predict_eval.sh --stage infer {T}/{TCM} 2", None)
todo, stuck = A.plan(snap([old_box]))
check("queue：box pueue 里已经有这一步 → 不加", not tc(todo))
added = []
fake_tq = lambda nice, args: added.append((nice, args)) or (0, "加入")  # noqa: E731
done, _ = A.run(snap(), q=lambda *a: 0, tq=fake_tq)
check("queue：AUTO 开着时调用 taskq.py add（档位 -100）", any(n_ == -100 and f"key={TCQ}" in a_ for n_, a_ in added)
      and "任务队列 infer:t1c_f2" in done)
added.clear()
A.P.HOLD = ["infer:*"]
done, _ = A.run(snap(), q=lambda *a: 0, tq=fake_tq)
check("queue：HOLD 对上的先不加", not any(f"key={TCQ}" in a_ for _, a_ in added))
A.P.HOLD = []

# 10b. trainer：训这一折的执行机接着做（10/31：lab 训的折进 lab 的缓冲，由 1 号卡的 worker 推理；中转执行机训的进 DICC 的队列）
K3 = f"{TC}/3"
M3 = f"Dataset103_GLIrq1_t1c/nnUNetTrainer_save10__{P}"
Q3, PRED3 = f"infer/{M3}/fold_3", f"predictions/{M3}/fold_3"
TN3 = [f"{TCM}/dataset.json", f"{TCM}/fold_3/checkpoint_final.pth", "nnUNet/raw/RQ1_holdout/GLI/imagesTs"]
old_sn = A.step_needs
A.step_needs = lambda cmd: TN3 if "Dataset103_GLIrq1_t1c" in cmd and cmd.endswith(" 3") and "--stage infer" in cmd else old_sn(cmd)
REMOTE["have"] = set(TN3)
H3 = "a" * 40


def lsnap(assigned=None, state="claimed", fold=None, qrows=(), lab=(), heads=None, src3=None, dicc3=None):
    """K3 在总队列里分给了 assigned（None 且 claimed：没有这一行）；fold：lab 上 K3 的情况；qrows：总队列里另外的行；
    src3 / dicc3：lab / DICC 上别的任务的情况；lab：lab pueue 里的任务。"""
    s_ = osnap(lab=lab)
    if assigned or state != "claimed":
        s_["taskq"]["box"]["tasks"].append(dict(key=K3, nice=60, state=state, worker=assigned, assigned=assigned))
    s_["taskq"]["box"]["tasks"] += [dict(r) for r in qrows]
    s_["src"]["lab"] = dict(folds=dict({K3: fold} if fold else {}, **(src3 or {})))
    s_["src"]["dicc"] = dict(folds=dict(dicc3 or {}))
    s_["code"] = dict(main=H3, heads=heads or {"dicc": H3, "lab": H3})
    return s_


def t3(todo):
    return [t for t in todo if t["key"] in (K3, Q3)]


def qrow(item, state="claimed"):
    """要加进总队列的一项 → 总队列里的这一行（taskq.py local_status 的样子）。"""
    return dict(key=item["qk"], nice=item["qnice"], state=state, recipe="command", exp=None, params=A.S.parse_kv(item["args"]))


def no_recs():
    A.C.drop_records(lambda r: True)


x = t3(A.plan(lsnap("lab"))[0])
check("trainer：分给 lab 的折（1 号卡的 worker 训）→ 推理现在就进总队列，只在 lab、等训练（档位 -100）；评测写的是 DICC，不跟着提前排",
      [(t["step"], t.get("op")) for t in x] == [("infer", "taskq")] and 'machines=["lab"]' in x[0]["args"]
      and "after=" + json.dumps([K3]) in x[0]["args"] and x[0]["qnice"] == -100 and f"key={Q3}" in x[0]["args"])
INF3 = x[0]
x = t3(A.plan(lsnap("lab:gpu0"))[0])
check("trainer：0 号卡手动训的折（lab:gpu0）→ 也进 lab 的缓冲（1 号卡推理）", len(x) == 1 and 'machines=["lab"]' in x[0]["args"])
check("trainer：中转执行机（dicc2）训的折 → 不提前排", not t3(A.plan(lsnap("dicc2"))[0]))
todo, _ = A.plan(lsnap("lab", heads={"dicc": H3, "lab": "b" * 40}))
check("trainer：lab 的代码不是最新 → 不提前排，记下原因", not t3(todo) and any("lab 的代码不是最新" in n_ for n_ in A.NOTES))
check("trainer：推理已经在总队列里 → 不再加", not t3(A.plan(lsnap("lab", qrows=[qrow(INF3, "waiting")]))[0]))
trained("Dataset103_GLIrq1_t1c", "nnUNetTrainer_save10", "3")
x = t3(A.plan(lsnap("dicc2"))[0])
check("trainer：中转执行机训完、拉回 box → 用下一个地方（DICC 的队列）",
      [(t["step"], t.get("op")) for t in x] == [("infer", "taskq")] and 'machines=["dicc"]' in x[0]["args"])
todo, _ = A.plan(lsnap("lab", fold=FIN))
x = t3(todo)
check("trainer：lab 训完、拉回了、推理还没排（如那时代码不是最新）→ 进 lab 的缓冲，不等归档；归档等推理做完",
      [(t["step"], t.get("op")) for t in x] == [("infer", "taskq")] and 'machines=["lab"]' in x[0]["args"]
      and not any(a_.startswith("after=") for a_ in x[0]["args"]) and any("做完再归档" in n_ for n_ in A.NOTES))
x = t3(A.plan(lsnap(state="done", fold=FIN))[0])
check("trainer：训练已经交还（总队列里不再分给谁）、lab 上有训完没归档的模型 → 也进 lab 的缓冲",
      [t["step"] for t in x] == ["infer"] and 'machines=["lab"]' in x[0]["args"])
x = t3(A.plan(lsnap(fold=dict(final=True, archived=True)))[0])
check("trainer：lab 上已经归档（模型删了）、没有分配 → 用下一个地方（DICC 的队列）",
      [t["step"] for t in x] == ["infer"] and 'machines=["dicc"]' in x[0]["args"])
s_ = lsnap("lab", fold=FIN)
s_["pueue"]["box"]["tasks"].append(dict(id=97, status="Running", command=f"bash scripts/queue/archive_task.sh lab {K3}", label=f"archive:t1c_f3",
                                        group="wait", end=None))
x = t3(A.plan(s_)[0])
check("trainer：lab 上正在归档这一折 → 不用 lab，DICC 的队列等归档做完再加", not x)
REMOTE["have"] = set(TN3) - {"nnUNet/raw/RQ1_holdout/GLI/imagesTs"}
todo, stuck = A.plan(lsnap("lab", fold=FIN))
check("trainer：lab 上缺测试集文件夹 → 报出来（和 DICC 缺的一样）；没排上，照常归档（归档以后改用 DICC）",
      steps(t3(todo)) == [("archive", K3)] and "imagesTs" in stuck.get(f"advance:infer:{K3}", ""))
REMOTE["have"] = set(TN3)
q3 = qrow(INF3)
check("trainer：推理在 lab 的缓冲里排着、在做，box 上还没有结果 → 不归档、不重复加", not t3(A.plan(lsnap("lab", fold=FIN, qrows=[q3]))[0]))
x = t3(A.plan(lsnap("lab", fold=FIN, qrows=[q3], src3={Q3: FIN}))[0])
check("trainer：lab 上推理做完 → 拉回（fetch_task.sh lab，和 DICC 做完的一样），拉回、核对完以前不归档",
      [(t["step"], t["cmd"]) for t in x] == [("fetch", f"bash scripts/queue/fetch_task.sh lab {Q3}")])
# 拷到一半不算做完（10/24：T20 fold 3 拉回时 MANIFEST 先到，box 当成做完、删了 lab 那份）
PD3 = f"{T}/{PRED3}"
touch(f"{PD3}/MANIFEST.json", json.dumps({"cohorts": {"X": {"n_segmentations": 2, "n_probabilities": 2}}, "probabilities_zeroed_below": {}}))
touch(f"{PD3}/X/a.nii.gz")
touch(f"{PD3}/X/a.npz")
touch(f"{PD3}/.infer_done")
todo, stuck = A.plan(lsnap("lab", fold=FIN, qrows=[q3], src3={Q3: FIN}))
check("MANIFEST、.infer_done 都到了、盘上的文件比 MANIFEST 记的少 → 推理不算做完：不排评测、不删 lab 上的、不归档，报出来",
      not t3(todo) and "核对还是没做完" in stuck.get(f"advance:infer:{K3}", ""))
touch(f"{PD3}/X/b.nii.gz")
touch(f"{PD3}/X/b.npz")
s_ = lsnap("lab", fold=FIN, qrows=[q3], src3={Q3: FIN})
s_["pueue"]["box"]["tasks"].append(dict(id=99, status="Running", command=f"bash scripts/queue/fetch_task.sh lab {Q3}",
                                        label=f"fetch:{Q3}", group="wait", end=None))
todo, stuck = A.plan(s_)
check("文件数都对上了、但拉回还在跑 → 不算做完：不排评测、不删 lab 上的、不归档", not t3(todo) and not stuck.get(f"advance:infer:{K3}"))
s_["pueue"]["box"]["tasks"][-1]["status"] = "Success"
x = t3(A.plan(s_)[0])
cl = [t for t in x if t.get("op") == "clean"]
check("拉回做完、文件对上 → 归档（进 DICC 队列的评测等归档做完），删 lab 上的推理结果",
      steps(x) == [("archive", K3), ("clean", K3)] and [(c["host"], c["paths"]) for c in cl] == [("lab", [PRED3])])
x = t3(A.plan(lsnap("lab", fold=FIN, qrows=[q3], dicc3={Q3: FIN}))[0])
check("DICC 上做完的：不删 DICC 上的", not [t for t in x if t.get("op") == "clean"])

# 10/31 以前 trainer 排在 lab pueue 里的推理（任务记录里有 out、train_id）：照常认、拉回、清理，不再新排
shutil.rmtree(PD3)
os.remove(f"{T}/{TCM}/fold_3/checkpoint_final.pth")
REC30 = dict(machine="lab", id=30, label="infer:t1c_f3", added=1, step="infer", key=K3, group="gpu1", train_id=19, needs=[], out=PRED3)
A.C.update_records(lambda recs: [dict(REC30)])
INFL = "bash research/RQ1/analysis/run_predict_eval.sh --stage infer x 3"
todo, _ = A.plan(lsnap("lab", lab=[(30, "Queued", INFL, "infer:t1c_f3", "gpu1")]))
check("旧记录：lab 在训、后面排着旧的推理 → 不再提前排进缓冲", not t3(todo))
A.P.PLACE = dict(BASE, infer=["queue:dicc"])
check("旧记录：PLACE 改成别处，lab 上已经排着的照常（不挪、不停）", not t3(A.plan(lsnap(lab=[(30, "Queued", INFL, "infer:t1c_f3", "gpu1")]))[0]))
A.P.PLACE = dict(BASE)
trained("Dataset103_GLIrq1_t1c", "nnUNetTrainer_save10", "3")
todo, stuck = A.plan(lsnap("lab", fold=FIN, lab=[(30, "Running", INFL, "infer:t1c_f3", "gpu1")]))
check("旧记录：lab 训完、box 拉回了、旧的推理在跑 → 不归档、不进任务队列", not t3(todo) and not stuck.get("advance:remote:infer:t1c_f3"))
no_recs()
todo, stuck = A.plan(lsnap("lab", fold=FIN, lab=[(30, "Running", INFL, "infer:t1c_f3", "gpu1")]))
check("lab pueue 里有这一折的推理在跑、没有它的记录 → 不归档（归档会删那边的模型），报出来",
      not [t for t in t3(todo) if t["step"] == "archive"] and "不是这一折现在的记录" in stuck.get("advance:remote:infer:t1c_f3", ""))
A.C.update_records(lambda recs: [dict(REC30)])
DONEL = [(30, "Success", INFL, "infer:t1c_f3", "gpu1")]
x = t3(A.plan(lsnap("lab", fold=FIN, lab=DONEL))[0])
check("旧记录：那边做完 → 归档、拉回（box wait 组 pull:<标签>，offload_io.sh pull lab）", steps(x) == [("archive", K3), ("pull", K3)]
      and next(t for t in x if t["step"] == "pull")["cmd"] == f"bash scripts/queue/offload_io.sh pull lab {PRED3}")
todo, stuck = A.plan(lsnap("lab", fold=FIN, lab=[(30, "Failed (1)", INFL, "infer:t1c_f3", "gpu1")]))
check("旧记录：训完了、那边推理失败 → 报出来，不新排", not [t for t in t3(todo) if t["step"] != "archive"]
      and "任务 30" in stuck.get("advance:remote:infer:t1c_f3", ""))
x = t3(A.plan(lsnap(fold=dict(final=True, archived=True)))[0])
check("旧记录：那边的任务删掉了、已经归档 → 照常按 PLACE（DICC 的队列）", [t["step"] for t in x] == ["infer"] and 'machines=["dicc"]' in x[0]["args"])
os.remove(f"{T}/{TCM}/fold_3/checkpoint_final.pth")
todo, stuck = A.plan(lsnap("lab", lab=[(30, "Failed (1)", INFL, "infer:t1c_f3", "gpu1")]))
check("旧记录：训练中途停了、推理跟着失败 → 不算，按 PLACE 重新提前排（进 lab 的缓冲）",
      [t["step"] for t in t3(todo)] == ["infer"] and 'machines=["lab"]' in t3(todo)[0]["args"])
trained("Dataset103_GLIrq1_t1c", "nnUNetTrainer_save10", "3")
_auto = A.P.AUTO["advance"]
A.P.AUTO["advance"] = True
seen = {}


def peek_q(host, args):
    if "--label pull:infer:t1c_f3 " in args:
        seen["rec"] = [r for r in A.C.load_records() if r["label"] == "pull:infer:t1c_f3"]
    return 31


A.run(lsnap("lab", fold=FIN, lab=DONEL), q=peek_q, tq=lambda *a: (0, ""))
prec = [r for r in A.C.load_records() if r["label"] == "pull:infer:t1c_f3"]
check("先写记录、后加任务：加任务那一刻任务记录里已经有这一条（还没有任务号；中间断电时按标签和创建时间找回）",
      len(seen.get("rec") or []) == 1 and seen["rec"][0]["id"] is None and len(prec) == 1 and prec[0]["id"] == 31)
A.C.drop_records(lambda r: r["label"] == "pull:infer:t1c_f3")


def bad_q(host, args):
    raise RuntimeError("ssh 断了")


A.run(lsnap("lab", fold=FIN, lab=DONEL), q=bad_q, tq=lambda *a: (0, ""))
check("加任务失败：记录留着（没有任务号；其实加上了的话下次找回，找不到一小时后作废）",
      [r["id"] for r in A.C.load_records() if r["label"] == "pull:infer:t1c_f3"] == [None])
A.C.drop_records(lambda r: r["label"] == "pull:infer:t1c_f3")
inferred("Dataset103_GLIrq1_t1c", "nnUNetTrainer_save10", "3")
s_ = lsnap("lab", fold=dict(final=True, archived=True), lab=DONEL, qrows=[q3], src3={Q3: FIN})
x = [t for t in t3(A.plan(s_)[0]) if t.get("op") == "clean"]
check("旧记录：box 上判断做完 → 删 lab 上的结果（新的缓冲也在 lab 做过同一折时只删一次）", [(c["host"], c["paths"]) for c in x] == [("lab", [PRED3])])
A.run(s_, q=lambda *a: 0, tq=lambda *a: (0, ""))
check("旧记录：删完以后删掉这条记录", not [r for r in A.C.load_records() if r.get("out")])
A.P.AUTO["advance"] = _auto
shutil.rmtree(PD3)
os.remove(f"{T}/{TCM}/fold_3/checkpoint_final.pth")
no_recs()
A.step_needs = old_sn

# 11. 实验一级带 queue 的步骤（10/23，P1 / P2 换模型重跑）：五折都在 box 上以后放进 DICC 的任务队列，拉回后 box 上做比较
for e_, g_ in GQUEUED:
    e_["group_after"] = g_
A.P.PLACE = dict(REAL_PLACE)
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
s_["taskq"]["box"]["tasks"] += [dict(key="probe/p1_t3men", nice=-100), dict(key="probe/p2_t3men", nice=-100)]
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

# step_needs 运行出错（10/25：box 上还没有 lab 训的模型文件夹）和命令不支持分开报
A.step_needs = REAL_STEP_NEEDS
bad = "sh -c 'echo 没有这个模型文件夹 >&2; exit 3' --stage infer"
check("needs 出错：返回 None，原因带上错误原文", A.step_needs(bad) is None and A.no_needs(bad) == "--stage needs 出错（没有这个模型文件夹）")
check("needs 不支持：原样说不支持", A.step_needs("true") is None and A.no_needs("true") == "命令不支持 --stage needs")

# 每个实验的每折步骤都真跑一次 --stage needs，模型文件夹一个都不在本机（10/25：lab 训的新数据集第一折，box 上还没拉回）
with tempfile.TemporaryDirectory() as R:
    os.makedirs(f"{R}/nnUNet/raw")
    old_bt = os.environ.get("BRAIN_TUMOR")
    os.environ["BRAIN_TUMOR"] = R
    seen, bad = set(), []
    for t in A.candidates(R, sys.executable, [])[0]:
        for st in (t["e"] or {}).get("after", []):
            cmd = A.S.fill(st["cmd"], t["names"], keep=())
            if "--stage" not in cmd or (t["exp"], st["name"]) in seen:
                continue
            seen.add((t["exp"], st["name"]))
            got = REAL_STEP_NEEDS(cmd)
            if not got or not any(x.endswith("/checkpoint_final.pth") for x in got):
                bad.append(f"{t['lab']} {st['name']}：{A.no_needs(cmd)}")
    if old_bt is None:
        os.environ.pop("BRAIN_TUMOR")
    else:
        os.environ["BRAIN_TUMOR"] = old_bt
    check(f"needs：{len(seen)} 个实验步骤在模型还没拉回时都能列出输入", seen and not bad)
    for b in bad[:5]:
        print("   ", b)

# 删单折概率图的命令对每个有 prune 的实验真跑一次（--dry-run，10/25）：刚训完、集成还没做，要报"集成没做完"、一个都不删
import subprocess  # noqa: E402
with tempfile.TemporaryDirectory() as R:
    bad, n = [], 0
    for e in A.E.load()[0]:
        for c in e.get("group_after") or []:
            for st in c:
                if st["name"] != "prune":
                    continue
                n += 1
                cmd = A.S.fill(st["cmd"], A.E.group_names(e, R, sys.executable), keep=())
                r = subprocess.run(cmd + " --dry-run", shell=True, cwd=os.path.dirname(os.path.dirname(HERE)), capture_output=True, text=True)
                if r.returncode != 1 or "集成没做完" not in r.stdout or "Traceback" in r.stderr:
                    bad.append(f"{e['id']}：{r.returncode} {(r.stdout + r.stderr).strip()[-200:]}")
    check(f"prune：{n} 个实验的命令真跑一次，集成没做完时都报出来、不出错", n and not bad)
    for b in bad[:5]:
        print("   ", b)

# 状态文件写到一半（10/25）：出错时原来的文件不变、不留临时文件
import common  # noqa: E402
with tempfile.TemporaryDirectory() as R:
    f = os.path.join(R, "x.json")
    common.write_json(f, {"a": 1})
    try:
        common.write_json(f, {"a": object()})
    except TypeError:
        pass
    check("write_json：写到一半出错 → 原来的内容还在，没有留下临时文件", json.load(open(f)) == {"a": 1} and os.listdir(R) == ["x.json"])

# 12. DICC 的作业在训的折：推理、评测现在就进任务队列，after 接上前一步（10/25）
A.P.PLACE = dict(REAL_PLACE)
A.P.HOLD = []
SB = "Dataset133_GLIrq1_syn18b/nnUNetTrainer_save10"
SBK, SBM = f"{SB}/1", f"Dataset133_GLIrq1_syn18b/nnUNetTrainer_save10__{P}"
SBI = f"infer/{SBM}/fold_1"
HEAD = "a" * 40


def psnap(state="claimed", assigned="dicc", heads=None, extra=(), buffer=None):
    """总队列里这一折分给了 assigned（10/31）；buffer：DICC 缓冲里的行。"""
    s_ = snap()
    s_["taskq"]["box"]["tasks"] += [dict(key=SBK, nice=60, state=state, worker=assigned, assigned=assigned)] + list(extra)
    if buffer is not None:
        s_["taskq"]["dicc"] = dict(tasks=list(buffer))
    s_["code"] = dict(main=HEAD, heads={"dicc": HEAD, "lab": HEAD} if heads is None else heads)
    return s_


def sb(todo):
    return [t for t in todo if t["key"] == SBK]


x = sb(A.plan(psnap())[0])
check("DICC 在训：推理、评测一起进队列，推理等训练、评测等推理（档位 -100，评测纯 CPU）",
      [(t["step"], t.get("op")) for t in x] == [("infer", "taskq"), ("eval", "taskq")]
      and "after=" + json.dumps([SBK]) in x[0]["args"] and "after=" + json.dumps([SBI]) in x[1]["args"]
      and x[0]["qnice"] == x[1]["qnice"] == -100 and "device=cpu" in x[1]["args"] and 'machines=["dicc"]' in x[0]["args"])
x = sb(A.plan(psnap(assigned="lab"))[0])
check("分给 lab 的折：推理进 lab 的缓冲，评测写的是 DICC，不跟着提前排（10/31）",
      [t["step"] for t in x] == ["infer"] and 'machines=["lab"]' in x[0]["args"])
check("分给中转执行机的折：不提前排", not sb(A.plan(psnap(assigned="dicc2"))[0]))
check("还没分出去的折：不提前排", not sb(A.plan(psnap(state="free", assigned=None))[0]))
x = sb(A.plan(psnap(buffer=[dict(key=SBK, nice=0, state="claimed", worker="492500")]))[0])
check("缓冲里的同一个 key（档位不同）：以总队列为准", x and x[0]["nice"] == 60)
todo, _ = A.plan(psnap(heads={"dicc": "b" * 40}))
check("DICC 的代码不是最新（不认 after）：不提前排，记下原因", not sb(todo) and any("代码不是最新" in n_ for n_ in A.NOTES))
x = sb(A.plan(psnap(extra=[dict(key=SBI, nice=-100, state="waiting")]))[0])
check("推理已经在队列里：只加评测，等那个推理", [t["step"] for t in x] == ["eval"] and "after=" + json.dumps([SBI]) in x[0]["args"])
x = sb(A.plan(psnap(extra=[dict(key=SBI, nice=-100, state="waiting"), dict(key=f"eval/{SBM}/fold_1", nice=-100, state="waiting")]))[0])
check("推理、评测都在队列里：不再加", not x)
A.P.HOLD = ["infer:*"]
check("HOLD 对上推理：推理、评测都先不提前排", not sb(A.plan(psnap())[0]))
A.P.HOLD = []
A.P.PLACE = dict(REAL_PLACE, eval=["box:cpu"])
x = sb(A.plan(psnap())[0])
check("评测写的是 box：只提前排推理", [t["step"] for t in x] == ["infer"])
A.P.PLACE = dict(REAL_PLACE)

# 12b. 五折集成：各折都在 DICC 推理过（.infer_done 都在那边）→ DICC 的 CPU worker 做；缺一折 → box（10/25）
S18 = "Dataset132_GLIrq1_syn18"
S18M = f"{S18}/nnUNetTrainer_save10__{P}"
for k in F5:
    trained(S18, "nnUNetTrainer_save10", k)
    inferred(S18, "nnUNetTrainer_save10", k)
    evaluated(S18, "nnUNetTrainer_save10", f"fold_{k}")
ENSIN = {f"predictions/{S18M}/fold_{k}/.infer_done" for k in F5}
REMOTE["have"] = set(ENSIN)


def ens18(todo):
    return [t for t in todo if t["key"] == "t18" and t["step"] == "ens"]


x = ens18(A.plan(snap())[0])
check("集成：DICC 上五折的推理都在 → 进任务队列（纯 CPU、拉回集成的预测和报告）",
      len(x) == 1 and x[0].get("op") == "taskq" and f"key=ens/{S18M}" in x[0]["args"] and "device=cpu" in x[0]["args"]
      and f"out=RQ1/analysis/{S18M}/folds_0-1-2-3-4" in x[0]["args"] and 'done=[".ens_done"]' in x[0]["args"]
      and any(a_.startswith("fetch=") and f"predictions/{S18M}/folds_0-1-2-3-4" in a_ for a_ in x[0]["args"])
      and "ENSEMBLE_ONLY=1" in x[0]["cmd"] and "'0 1 2 3 4'" in x[0]["cmd"])
REMOTE["have"] = ENSIN - {f"predictions/{S18M}/fold_3/.infer_done"}
x = ens18(A.plan(snap())[0])
check("集成：DICC 上缺一折的推理 → box 的 cpu 组", [(t["group"], t.get("op")) for t in x] == [("cpu", None)])
REMOTE["have"] = set(ENSIN)
evaluated(S18, "nnUNetTrainer_save10", "folds_0-1-2-3-4")  # 报告先拷到了 box，集成的预测、.ens_done 还在拉
s_ = snap([("Running", f"bash scripts/queue/fetch_task.sh dicc ens/{S18M}", None)])
s_["taskq"]["box"]["tasks"].append(dict(key=f"ens/{S18M}", nice=-100, state="done"))
todo, stuck = A.plan(s_)
check("集成：还在从 DICC 拉回（报告已经到了）→ 不算做完，不排删单折概率图、不重排集成",
      not [t for t in todo if t["key"] == "t18"] and not any(k_.endswith(":t18") for k_ in stuck))
todo, _ = A.plan(snap())
check("集成：拉完 → 排删单折概率图（box）", [t["step"] for t in todo if t["key"] == "t18"] == ["prune"])

os.remove(f"{T}/RQ1/analysis/{S18M}/folds_0-1-2-3-4/report/report.run.json")
REMOTE["have"] = set()
# 12c. box 的任务做完马上跑一次健康检查（refill.then_check，锁最多等 5 分钟）；包过的命令照样认得出是哪一步
calls.clear()
A.run(snap(), q=fake_q, tq=lambda *a: (0, ""))
ec = [c for c in calls if "--label ens:t18 " in c]
check("box 的任务包上 kick（PHD_TASK_DONE=标签）", len(ec) == 1 and "PHD_TASK_DONE=ens:t18 bash ~/auto/PhD/scripts/monitor/auto_refresh.sh kick" in ec[0])
wrapped = A.refill.then_check(ens18(A.plan(snap())[0])[0]["cmd"], "ens:t18")
er = [r for r in A.C.load_records() if r["label"] == "ens:t18"]
s_ = dict(snap(), records=A.C.load_records())  # 只用 run 写的记录，不按旧规则配
s_["pueue"]["box"]["tasks"] = [dict(id=er[-1]["id"], status="Running", command=wrapped, label="ens:t18", group="cpu", end=None)]
check("run 加任务前写了记录（box、步骤、key、组）；包过的命令在跑按记录认出、不重排",
      er and er[-1]["machine"] == "box" and (er[-1]["step"], er[-1]["key"], er[-1]["group"]) == ("ens", "t18", "cpu") and not ens18(A.plan(s_)[0]))
s_["pueue"]["box"]["tasks"][0]["label"] = "other"
check("任务号对上、标签不同（任务号被重用）→ 不认，照常排", ens18(A.plan(s_)[0]))
REMOTE["have"] = set()

print("\n全部通过" if ok else "\n有失败")
sys.exit(0 if ok else 1)
