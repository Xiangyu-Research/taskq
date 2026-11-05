#!/usr/bin/env python3
"""advance.py 的测试（2026-10-18）：临时目录里的假 hub 文件、假的 pueue 快照，不连任何机器、不加任务。配置和实验表用
testdata/ 的（toy 做法、demo 实验表）；使用者真实验表的那几条在使用者自己的测试里。
    python3 queue/test_advance.py
"""
import json
import os
import shutil
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import cfg as _cfg  # noqa: E402  调度代码的根、使用者的仓库根
QR = _cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata"))
import fixture  # noqa: E402
T = fixture.tmpdir()
os.environ["ADVANCE_BT"] = T
os.environ["ADVANCE_LOG"] = os.path.join(T, "advance.log")
os.environ["TASKQ_TASKS"] = os.path.join(T, "tasks.json")
os.environ["TASKQ_LAST"] = os.path.join(T, "last")
fixture.use()
sys.path.insert(0, HERE)
import advance as A  # noqa: E402
REAL_STEP_NEEDS = A.step_needs


def legacy_index():
    """10/25 以前按命令和标签认 hub、pueue-host 任务的规则 → ({命令: (步骤, key)}, {标签: (步骤, key)})。测试里用它给快照配记录。"""
    tasks, groups, final = A.candidates(T, PY, [])
    by_cmd, by_label = {}, {}
    for t in tasks:
        for st in t["after"]:
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
            hit = by_cmd.get(cmd) or by_label.get(lb)
            if hit:
                out.append(dict(machine=h, id=x["id"], label=lb, added=0, step=hit[0], key=hit[1]))
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
A.taskq_remove = lambda ks: (0, "")  # 不真的从总队列删
REAL_PLACE = dict(A.P.PLACE, eval=["queue:site-slurm", "hub:cpu"], ens=["queue:site-slurm", "hub:cpu"])  # 夹具的 PLACE，评测、集成也先进总队列
A.P.PLACE = {}  # 前面几节：每一步都在 hub 上（实验表里写的组）；第 9 节起测别的地方
A.P.AUTO = dict(A.P.AUTO, advance=False)
A.P.OFFLINE = {}
A.P.HOLD = []
# 实验一级带 queue 的几串（10/23，P1 / P2 重跑）也先拿掉，第 11 节再放回来测
PROBE_STEPS = ("p1", "p2", "p2cmp")
GQUEUED = [(e, e["group_after"]) for e in A.E.load()[0] if any(st["name"] in PROBE_STEPS for c in e.get("group_after", []) for st in c)]
for e_, g_ in GQUEUED:
    e_["group_after"] = [c for c in g_ if not any(st["name"] in PROBE_STEPS for st in c)]
P = "PlanM__full"
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
    d = f"{T}/results/{ds}/{tr}__{P}/frag_{f}"
    touch(f"{d}/final.ckpt")
    touch(f"{d}/check/summary.json", "{}")


def inferred(ds, tr, f):
    touch(f"{T}/preds/{ds}/{tr}__{P}/frag_{f}/MANIFEST.json", json.dumps({"cohorts": {}, "probabilities_zeroed_below": {}}))


def evaluated(ds, tr, tag):
    touch(f"{T}/demo/analysis/{ds}/{tr}__{P}/{tag}/report/report.rec.json", '{"status": "ok"}')


def pruned(ds, tr):
    """各折的单折概率图删过了（prune_frag_probs.py 写的记录）。"""
    import glob
    for mp in glob.glob(f"{T}/preds/{ds}/{tr}__{P}/frag_*/MANIFEST.json"):
        m = json.load(open(mp))
        m["probabilities_removed"] = dict(status="done", cohorts={})
        json.dump(m, open(mp, "w"))


def snap(tasks=()):
    rows = [dict(id=i, status=st, command=c, label="", group="x", end=end) for i, (st, c, end) in enumerate(tasks)]
    return dict(pueue={"hub": dict(tasks=rows)},
                taskq={"hub": dict(tasks=[dict(key=f"D121_alphagamma/V_save/{k}", nice=0) for k in range(5)])})


def steps(todo):
    return sorted((t["step"], t["key"]) for t in todo)


MET = "D121_alphagamma/V_save"   # T14：oof + 集成
T3 = "D116_gamma/V_save"       # T3：只有 oof


def relay_fetch(s_, i, mach, qk, status="Running"):
    """hub pueue 里加一个 relay.py 排的拉回（带它写的任务记录）。"""
    s_["pueue"]["hub"]["tasks"].append(dict(id=i, status=status, command=f"python3 {QR}/relay.py fetch {mach} {qk}",
                                            label=f"relay_fetch:{qk}", group="wait", end=None))
    s_["records"] = (s_.get("records") or []) + [dict(machine="hub", id=i, label=f"relay_fetch:{qk}", step="relay_fetch", key=qk)]
    return s_

# 1. hub 上还没训完 → 不排（执行机训完的由 relay.py 拉回、归档）
todo, stuck = A.plan(snap())
check("hub 上还没有训完的任务：什么都不排", todo == [] and not stuck)

# 2. hub 上有了 → 推理（gpu）
trained("D116_gamma", "V_save", "1")
todo, _ = A.plan(snap())
check("hub 上训完了：排推理", steps(todo) == [("infer", f"{T3}/1")])
inf = todo[0]
check("推理：gpu 组、--stage infer", inf["group"] == "gpu" and "--stage infer" in inf["cmd"] and inf["cmd"].endswith(f"__{P} 1"))
trained("D121_alphagamma", "V_save", "0")
todo, _ = A.plan(snap())
check("按队列的档位排在前面（档位 0 的 T14 先于没有档位的 T3）", [t["key"] for t in todo] == [f"{MET}/0", f"{T3}/1"])
shutil.rmtree(f"{T}/results/D121_alphagamma")

# 4. 推理做完 → 评测（cpu）；评测做完 → 这一折没事了
inferred("D116_gamma", "V_save", "1")
todo, _ = A.plan(snap())
check("推理做完：排评测（cpu 组、--stage eval）", steps(todo) == [("eval", f"{T3}/1")] and todo[0]["group"] == "cpu" and "--stage eval" in todo[0]["cmd"])
evaluated("D116_gamma", "V_save", "frag_1")
todo, _ = A.plan(snap())
check("评测做完：没有要排的", todo == [])
touch(f"{T}/demo/analysis/D116_gamma/V_save__{P}/frag_2/report/report.md")
trained("D116_gamma", "V_save", "2")
inferred("D116_gamma", "V_save", "2")
todo, _ = A.plan(snap())
check("10/16 前的评测（没有 rec.json、有 report.md）算做完", todo == [])

# 5. 失败过的不重排，报出来；成功跑完却没结果的也不重排
trained("D116_gamma", "V_save", "3")
cmd3 = f"bash {QR}/testdata/bin/step.sh --stage infer {T}/results/D116_gamma/V_save__{P} 3"
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
    trained("D116_gamma", "V_save", k)
    inferred("D116_gamma", "V_save", k)
    evaluated("D116_gamma", "V_save", f"frag_{k}")
inferred("D116_gamma", "V_save", "3")
evaluated("D116_gamma", "V_save", "frag_3")
todo, _ = A.plan(snap())
oof_ = [t for t in todo if t["step"] == "oof"]
check("T3 五折到齐：排 oof 和集成，不排删单折概率图", steps(todo) == [("ens", "t3g"), ("oof", "t3g")]
      and oof_[0]["group"] == "cpu" and "oof.py" in oof_[0]["cmd"])
evaluated("D116_gamma", "V_save", "frags_0-1-2-3-4")
_er = f"{T}/demo/analysis/D116_gamma/V_save__{P}/frags_0-1-2-3-4/report/report.rec.json"
os.utime(_er, (os.path.getmtime(_er) - 3600,) * 2)  # 集成比下面的 compare_all 早做完
touch(f"{T}/demo/analysis/D116_gamma/V_save__{P}/oof/oof.rec.json", '{"status": "ok"}')
todo, _ = A.plan(snap())
pa = [t for t in todo if t["step"] == "paired"]
check("oof、集成做完：排配对（--cmp T3=…/oof/eval）和删单折概率图", steps(todo) == [("paired", "t3g"), ("prune", "t3g")]
      and "--cmp T3=" in pa[0]["cmd"] and pa[0]["cmd"].endswith("oof/paired"))
touch(f"{T}/demo/analysis/D116_gamma/V_save__{P}/oof/paired/paired.rec.json", '{"status": "ok"}')
todo, _ = A.plan(snap())
check("配对做完、compare_all 比它旧：排 compare_all，删单折概率图在排不挡它", steps(todo) == [("compare_all", "all"), ("prune", "t3g")])
todo, _ = A.plan(snap([("Queued", [t for t in todo if t["step"] == "prune"][0]["cmd"], None)]))
check("删单折概率图排着（HOLD 暂停的也算）：认得出来、不重复，compare_all 照常", steps(todo) == [("compare_all", "all")])
pruned("D116_gamma", "V_save")
todo, _ = A.plan(snap())
check("单折概率图删完：只剩 compare_all", steps(todo) == [("compare_all", "all")])
cp = f"{T}/demo/analysis/compare/all/compare_all.rec.json"
touch(cp, json.dumps({"status": "ok", "start": (datetime.now() - timedelta(minutes=10)).astimezone().isoformat(timespec="seconds")}))
os.utime(f"{T}/demo/analysis/D116_gamma/V_save__{P}/oof/paired/paired.rec.json",
         (os.path.getmtime(cp) - 60,) * 2)  # 配对在 compare_all 跑的过程中（开始之后、结束之前）做完
todo, _ = A.plan(snap())
check("配对在 compare_all 跑的过程中做完：按 compare_all 开始的时刻比，还要再排", steps(todo) == [("compare_all", "all")])
touch(cp, json.dumps({"status": "ok", "start": datetime.now().astimezone().isoformat(timespec="seconds")}))
todo, _ = A.plan(snap())
check("compare_all 比所有实验结果新：没有要排的", todo == [])
os.utime(f"{T}/demo/analysis/D116_gamma/V_save__{P}/oof/paired/paired.rec.json")
os.utime(cp, (os.path.getmtime(cp) - 120,) * 2)
touch(cp, json.dumps({"status": "ok", "start": (datetime.now() - timedelta(minutes=5)).astimezone().isoformat(timespec="seconds")}))
todo, stuck = A.plan(snap([("Success", f"/py {QR}/testdata/bin/compare_all.py", None)]))
check("compare_all 成功过、之后又有新结果：照常重排（不当成卡住）", steps(todo) == [("compare_all", "all")] and not stuck)
touch(cp, json.dumps({"status": "ok", "start": datetime.now().astimezone().isoformat(timespec="seconds")}))
for k in F5:
    trained("D121_alphagamma", "V_save", k)
    inferred("D121_alphagamma", "V_save", k)
    evaluated("D121_alphagamma", "V_save", f"frag_{k}")
todo, _ = A.plan(snap())
ens = [t for t in todo if t["step"] == "ens"]
check("T14 五折到齐：oof 和五折集成都排，删单折概率图、compare_all 先不排", steps(todo) == [("ens", "t14g"), ("oof", "t14g")]
      and ens[0]["cmd"].endswith("'0 1 2 3 4'") and ens[0]["cmd"].startswith("ENSEMBLE_ONLY=1 "))
todo, _ = A.plan(snap([("Running", ens[0]["cmd"], None)]))
check("集成任务在跑：认得出来，不重复", steps(todo) == [("oof", "t14g")])
# 删单折概率图（10/25）：集成做完、每折都评测完、没有在往 hub 上拷的才排
M14 = f"D121_alphagamma/V_save__{P}"
evaluated("D121_alphagamma", "V_save", "frags_0-1-2-3-4")
os.remove(f"{T}/demo/analysis/{M14}/frag_2/report/report.rec.json")
todo, _ = A.plan(snap())
check("删单折概率图：集成做完、但第 2 份还没评测完 → 不删（先评测）", ("prune", "t14g") not in steps(todo) and ("eval", f"{MET}/2") in steps(todo))
evaluated("D121_alphagamma", "V_save", "frag_2")
todo, _ = A.plan(snap())
pr_ = [t for t in todo if t["step"] == "prune"]
check("删单折概率图：集成和每折评测都做完 → 排（hub cpu 组，命令带五折）",
      steps(todo) == [("oof", "t14g"), ("prune", "t14g")] and pr_[0]["group"] == "cpu" and pr_[0]["label"] == "prune:t14g"
      and pr_[0]["cmd"] == f"{PY} {QR}/testdata/bin/prune.py {T}/preds/{M14} --frags '0 1 2 3 4'")
todo, _ = A.plan(relay_fetch(snap(), 98, "site-slurm", f"infer/{M14}/frag_1"))
check("删单折概率图：第 1 份的推理结果还在从执行机拉回（relay_fetch）→ 不删", ("prune", "t14g") not in steps(todo))
todo, _ = A.plan(relay_fetch(snap(), 98, "site-slurm", f"eval/{M14}/frag_3"))
check("删单折概率图：第 3 份的评测结果还在拉回 → 不删", ("prune", "t14g") not in steps(todo))
todo, stuck = A.plan(snap([("Success", pr_[0]["cmd"], None)]))
check("删单折概率图成功跑完、盘上还有单折概率图 → 报出来，不重排", ("prune", "t14g") not in steps(todo) and "advance:prune:t14g" in stuck)
pruned("D121_alphagamma", "V_save")
todo, stuck = A.plan(snap([("Success", pr_[0]["cmd"], None)]))
check("删完了：不再排，也不报", ("prune", "t14g") not in steps(todo) and "advance:prune:t14g" not in stuck)
touch(f"{T}/preds/{M14}/frag_4/B/x.prob")
check("删完以后又拉回来一个单折概率图（sync）→ 不算做完", not A.E.experiment("t14g")["group_after"][1][1]["done"].ok(T, A.E.group_names(A.E.experiment("t14g"), T, PY)))
os.remove(f"{T}/preds/{M14}/frag_4/B/x.prob")
# T0 和三种架构：五折、集成都做完也不删单折概率图（测完删掉这些假文件，后面几节要 D101_alpha 还没训）
for k in F5:
    for tr in ("V_save", "V_m"):
        trained("D101_alpha", tr, k)
        inferred("D101_alpha", tr, k)
        evaluated("D101_alpha", tr, f"frag_{k}")
for tr in ("V_save", "V_m"):
    evaluated("D101_alpha", tr, "frags_0-1-2-3-4")
todo, _ = A.plan(snap())
check("t0、arch_m 五折和集成都做完：不排删单折概率图", not [t for t in todo if t["step"] == "prune"])
for d in ("results", "preds", "demo/analysis"):
    shutil.rmtree(f"{T}/{d}/D101_alpha")

# 6b. 推理的完整性：输出、概率、输入数要对得上
src = f"{T}/raw/X/inputs"
for c in ("a", "b"):
    touch(f"{src}/{c}_0000.dat")
man = f"{T}/preds/D116_gamma/V_save__{P}/frag_2/MANIFEST.json"
def manifest(nout, nprob):
    touch(man, json.dumps({"probabilities_zeroed_below": 1e-5, "cohorts": {"X": dict(
        n_outputs=nout, n_probs=nprob, args=dict(source=src))}}))
import experiments as E  # noqa: E402
INF = E.experiment("t3g")["after"][0]
inf_ok = lambda f: INF["done"].ok(T, E.task_names(E.tasks()[f"{T3}/{f}"], T, PY))  # noqa: E731
manifest(2, 1)
check("概率比输出少：推理没做完", not inf_ok("2"))
manifest(1, 1)
check("比输入少：推理没做完", not inf_ok("2"))
manifest(2, 2)
check("MANIFEST 对得上、盘上没有这些文件（拷到一半，10/24）：推理没做完", not inf_ok("2"))
for c in ("a", "b"):
    touch(f"{os.path.dirname(man)}/X/{c}.out")
    touch(f"{os.path.dirname(man)}/X/{c}.prob")
check("都对得上：推理做完", inf_ok("2"))
m_ = json.load(open(man))
m_["probabilities_removed"] = dict(status="done", cohorts={"X": 2})
touch(man, json.dumps(m_))
os.remove(f"{os.path.dirname(man)}/X/a.prob")
os.remove(f"{os.path.dirname(man)}/X/b.prob")
check("单份概率按记录删掉了（10/25）：推理仍算做完，不重新推理", inf_ok("2"))
os.remove(f"{os.path.dirname(man)}/X/a.out")
check("删了概率、又少了输出：推理没做完", not inf_ok("2"))
del m_["probabilities_removed"]
touch(man, json.dumps(m_))
for c in ("a", "b"):
    touch(f"{os.path.dirname(man)}/X/{c}.out")
    touch(f"{os.path.dirname(man)}/X/{c}.prob")

# 7. 没有记录的任务按命令只认训练；拉回、归档由 relay.py 排，有它写的记录
K4 = "D111_beta/V_save/4"
check("认命令：拉回、归档的命令不认", A.C.from_command(f"bash {QR}/archive_task.sh hub {K4}") is None
      and A.C.from_command(f"python3 {QR}/relay.py fetch site-pueue {K4}") is None)
check("认命令：不认识的命令 → None", A.C.from_command("pueue parallel 2 --group cpu") is None and A.C.from_command("echo 'unbalanced") is None)
nr = [dict(machine="hub", id=7, label="eval:t3g_f4", added=0, step="eval", key=K4)]
check("有记录的按记录认（任务号和标签都对上）；任务号被别的任务重用（标签不同）→ 不认",
      A.C.identify("hub", dict(id=7, label="eval:t3g_f4", command="x"), nr)["step"] == "eval"
      and A.C.identify("hub", dict(id=7, label="other", command="x"), nr) is None
      and A.C.identify("pueue-host", dict(id=7, label="eval:t3g_f4", command="x"), nr) is None)

# 7b. 生成的命令和 10/18 前逐字一样
M5 = f"{T}/results/D116_gamma/V_save__{P}"
trained("D116_gamma", "V_save", "3")
for p_ in (f"{T}/preds/D116_gamma/V_save__{P}/frag_3/MANIFEST.json",):
    os.remove(p_)
todo, _ = A.plan(snap())
check("推理命令逐字和以前一样", [t["cmd"] for t in todo if t["step"] == "infer" and t["key"] == f"{T3}/3"]
      == [f"bash {QR}/testdata/bin/step.sh --stage infer {M5} 3"])
ph_task = [dict(id=0, status="Running", command="bash step.sh --other-form", label="infer:t3g_f3", group="gpu", end=None)]
todo, _ = A.plan(dict(snap(), pueue={"hub": dict(tasks=ph_task)}))
check("按标签认得（命令写法不同）：不重复排", not [t for t in todo if t["step"] == "infer" and t["key"] == f"{T3}/3"])
inferred("D116_gamma", "V_save", "3")
by_cmd = legacy_index()[0]
want_cmds = {
    ("eval", f"{T3}/3"): f"bash {QR}/testdata/bin/step.sh --stage eval {M5} 3",
    ("oof", "t3g"): f"{PY} {QR}/testdata/bin/oof.py {M5}",
    ("paired", "t3g"): f"{PY} {QR}/testdata/bin/paired.py --ref {T}/demo/analysis/D101_alpha/V_save__{P}/frags_0-1-2-3-4 "
                         f"--cmp T3={T}/demo/analysis/D116_gamma/V_save__{P}/oof/eval --out {T}/demo/analysis/D116_gamma/V_save__{P}/oof/paired",
    ("ens", "t14g"): f"ENSEMBLE_ONLY=1 bash {QR}/testdata/bin/step.sh --stage all {T}/results/D121_alphagamma/V_save__{P} '0 1 2 3 4'",
    ("compare_all", "all"): f"{PY} {QR}/testdata/bin/compare_all.py",
}
inv = {v: k for k, v in by_cmd.items()}
check("评测、oof、配对、集成、compare_all 的命令逐字和以前一样", all(inv.get(k) == v for k, v in want_cmds.items()))
adhoc = dict(key="runs/x/0", nice=5, recipe="command", exp=None,
             params=dict(key="runs/x/0", cmd="python t.py", env="base", out="runs/x/0", done=["DONE"]))
s_ = snap()
s_["taskq"]["hub"]["tasks"].append(adhoc)
todo, _ = A.plan(s_)
check("队列里不属于实验的任务：hub 上还没有结果 → 不排", not [t for t in todo if t["key"] == "runs/x/0"])
touch(f"{T}/runs/x/0/DONE")
todo, _ = A.plan(s_)
check("不属于实验的任务拉回以后：没有别的步骤", not [t for t in todo if t["key"] == "runs/x/0"])

# 8. AUTO["advance"] = False：只写日志，不加任务；True 才加
trained("D101_alpha", "V_m", "0")
calls = []
fake_q = lambda host, args: calls.append(args) or 900  # noqa: E731
done, _ = A.run(snap(), q=fake_q)
check("只算不排：不加任务，日志里写'会排'", calls == [] and "会排 infer:arch_m_f0" in done and "会排 infer:arch_m_f0" in open(os.environ["ADVANCE_LOG"]).read())
n = len(open(os.environ["ADVANCE_LOG"]).readlines())
A.run(snap(), q=fake_q)
check("只算不排：和上次一样就不重复写日志", len(open(os.environ["ADVANCE_LOG"]).readlines()) == n)
A.P.AUTO["advance"] = True
done, _ = A.run(snap(), q=fake_q)
fc = [c for c in calls if "infer:arch_m_f0" in c]  # 第 6 节留下的 T14 五折也会排，这里只看这一折的推理
check("AUTO 开着：加任务（组、标签、命令都带上）", len(fc) == 1 and fc[0].startswith("gpu -p --label infer:arch_m_f0 -- ")
      and "--stage infer" in fc[0] and "infer:arch_m_f0（任务 900）" in done)
A.P.HOLD = ["infer:arch_m_f*"]
calls.clear()
done, _ = A.run(snap(), q=fake_q)
fc = [c for c in calls if "infer:arch_m_f0" in c]
other = [c for c in calls if "infer:arch_m_f0" not in c]
check("HOLD：对上的加成暂停的，别的照常", len(fc) == 1 and fc[0].startswith("gpu -p --stashed --label infer:arch_m_f0 -- ")
      and "infer:arch_m_f0（任务 900，暂停（HOLD））" in done and all("--stashed" not in c for c in other))
A.P.HOLD = []
A.P.AUTO["advance"] = False
todo, _ = A.plan(dict(pueue={"hub": {"error": "x"}}, taskq={}))
check("hub 的 pueue 读不到：什么都不排", todo == [])

# 9. PLACE 写错了要报
inferred("D101_alpha", "V_m", "0")  # 第 8 节训完的一折：先推理完，后面几节不看它


def osnap(hubq=()):
    s_ = snap()
    s_["pueue"]["hub"]["tasks"] = [dict(id=i, status=st, command=c, label=lb, group=g, end=None) for i, st, c, lb, g in hubq]
    return s_


bad_ = []
for pl in ({"infer": ["hub:gpu9"]}, {"infer": ["queue:slurm-login idel"]}, {"nosuch": ["hub:cpu"]}, {"infer": ["slurm-login:x"]},
           {"infer": ["pueue-host:gpu0"]}, {"infer": ["hub:gpu idle"]}, {"infer": ["hub:gpu hours=20:00-08:00"]}, {"infer": ["trainer manual"]},
           {"infer": ["queue:pueue-host"]}, {"infer": ["queue:site-slurm have"]}):
    A.P.PLACE = pl
    todo, stuck = A.plan(osnap())
    if todo or "advance:policy" not in stuck:
        bad_.append(pl)
check("PLACE 写错了（没有的组、不认识的条件、没有的步骤、不认识的地方，包括已经不用的 <机器>:<组>、idle、hours=、manual、have、"
      "不是执行机的 queue:<机器>）→ 什么都不排，报出来",
      not bad_)
A.P.PLACE = {"infer": ["pueue-host:gpu0"]}
check("PLACE 写 pueue-host:gpu0：报不认识的地方", "不认识的地方" in A.plan(osnap())[1].get("advance:policy", ""))
A.P.PLACE = {"infer": ["hub:gpu", "hub:gpu"]}
_tg = A.targets("infer", None)
_hub, _hm = A.M.hub, dict(A.M.MACHINES)
A.M.hub = lambda: "central"
A.M.MACHINES["central"] = A.M.MACHINES.pop("hub")
A.P.PLACE = {"infer": ["central:gpu", "hub:gpu"]}
_tg2 = A.targets("infer", None)
A.P.PLACE = {"infer": ["main:gpu"]}
try:
    A.targets("infer", None)
    _bad = False
except ValueError as e:
    _bad = "不认识的地方" in str(e)
A.M.hub = _hub
A.M.MACHINES.clear()
A.M.MACHINES.update(_hm)
check("PLACE 的 hub:<组>、<hub 的名字>:<组> 都指总队列所在的机器（hub）；不是 hub 的名字（main:<组>）不认（11/5）",
      [(t["kind"], t["machine"], t["group"]) for t in _tg] == [("hub", "hub", "gpu")] * 2
      and [(t["kind"], t["machine"], t["group"]) for t in _tg2] == [("hub", "central", "gpu")] * 2 and _bad)

# 10. 进总队列（PLACE 写 queue:<执行机>）：relay.py 连同输入送过去，hub 不跑
BASE = {k: v for k, v in REAL_PLACE.items() if k not in ("p1", "p2")}  # P1 / P2 第 11 节再测
A.P.PLACE = dict(BASE, infer=["queue:site-slurm", "hub:gpu"])
TC = "D103_alpha_one/V_save"
TCM = f"results/D103_alpha_one/V_save__{P}"
TCQ = f"infer/D103_alpha_one/V_save__{P}/frag_2"
trained("D103_alpha_one", "V_save", "2")
TNEEDS = [f"{TCM}/dataset.json", f"{TCM}/frag_2/final.ckpt", "raw/holdout/A/inputs"]
A.step_needs = lambda cmd: (None if "--stage infer" not in cmd else
                           TNEEDS if "D103_alpha_one" in cmd and cmd.endswith(" 2") else [])


def tc(todo):
    return [t for t in todo if t["key"] == f"{TC}/2"]


A.M.RELAY["site-slurm"].pop("cap", None)  # site-slurm 的 cap 只在下面一项测，其余各节不受它限
x = tc(A.plan(snap())[0])
check("queue:site-slurm → 进总队列只给 site-slurm（档位 -100、{data} 留给那边换），hub 不跑",
      len(x) == 1 and x[0].get("op") == "taskq" and x[0]["qnice"] == -100 and f"key={TCQ}" in x[0]["args"]
      and 'machines=["site-slurm"]' in x[0]["args"] and f"cmd=bash {QR}/testdata/bin/infer_task.sh {{data}}/{TCM} 2" in x[0]["args"]
      and 'done=[".infer_done"]' in x[0]["args"])
check("给执行机的任务单带上命令列出的输入（--stage needs:<阶段>，relay.py 按真名传过去）",
      any(y.startswith("inputs=") and f"{TCM}/frag_2/final.ckpt" in y for y in x[0]["args"]))
A.M.RELAY["site-slurm"]["cap"] = 60
A.P.PLACE = dict(BASE, infer=["queue:site-slurm", "queue:site-pueue", "hub:gpu"])
x = tc(A.plan(snap())[0])
check("site-slurm 的根目录放不下推理（cap，11/2）→ 用下一个地方（site-pueue）",
      len(x) == 1 and x[0].get("op") == "taskq" and 'machines=["site-pueue"]' in x[0]["args"])
A.P.AUTO = dict(A.P.AUTO, **{"site-pueue": False})
x = tc(A.plan(snap())[0])
check("下一个执行机 site-pueue 的 AUTO 关着 → 也跳过，用 hub", len(x) == 1 and x[0].get("group") == "gpu")
A.P.AUTO = dict(A.P.AUTO, **{"site-pueue": True})
A.M.RELAY["site-slurm"].pop("cap")
A.P.PLACE = dict(BASE, infer=["queue:site-slurm", "hub:gpu"])
A.P.OFFLINE = {"site-slurm": "停机"}
x = tc(A.plan(snap())[0])
check("queue:site-slurm，site-slurm 暂停检查（OFFLINE）→ 跳过，用下一个地方", len(x) == 1 and x[0].get("group") == "gpu")
A.P.OFFLINE = {}
s_ = snap()
s_["taskq"]["hub"]["tasks"].append(dict(key=TCQ, nice=-100))
todo, stuck = A.plan(s_)
check("queue：队列里已经有 → 不再加", not tc(todo) and f"advance:infer:{TC}/2" not in stuck)
touch(f"{T}/preds/D103_alpha_one/V_save__{P}/frag_2/.infer_done")
todo, stuck = A.plan(s_)
check("queue：拉回了但 hub 上核对不过 → 报出来", not tc(todo) and "核对还是没做完" in stuck.get(f"advance:infer:{TC}/2", ""))
IQ = [st["queue"] for e in A.E.load()[0] for st in e.get("after", []) if st["name"] == "infer" and st.get("queue")]
old_done = [q["done"] for q in IQ]
for q in IQ:
    q["done"] = ".none|.infer_done"
todo, stuck = A.plan(s_)
check("queue：done 是字符串、| 分开的备选有一个就算做完（spec.outputs，11/4）", "核对还是没做完" in stuck.get(f"advance:infer:{TC}/2", ""))
for q in IQ:
    q["done"] = ".none|.nope"
todo, stuck = A.plan(s_)
check("queue：备选一个都没有 → 还没拉回，不报", f"advance:infer:{TC}/2" not in stuck)
for q in IQ:
    q["done"] = []
todo, stuck = A.plan(s_)
check("queue：done 是空的 → 看不出拉回了没有，不报", f"advance:infer:{TC}/2" not in stuck)
for q, d in zip(IQ, old_done):
    q["done"] = d
os.remove(f"{T}/preds/D103_alpha_one/V_save__{P}/frag_2/.infer_done")
inferred("D103_alpha_one", "V_save", "2")
todo, stuck = A.plan(relay_fetch(dict(s_), 98, "site-slurm", TCQ))
check("queue：MANIFEST 到了但结果还在拉回（relay_fetch，10/24）→ 不排评测、不报", not tc(todo) and f"advance:infer:{TC}/2" not in stuck)
x = tc(A.plan(s_)[0])
check("评测：进总队列给 site-slurm，纯 CPU（device=cpu），结果在分析文件夹",
      len(x) == 1 and x[0]["op"] == "taskq" and x[0]["step"] == "eval" and "device=cpu" in x[0]["args"]
      and f"key=eval/D103_alpha_one/V_save__{P}/frag_2" in x[0]["args"] and 'machines=["site-slurm"]' in x[0]["args"]
      and 'done=[".eval_done"]' in x[0]["args"])
A.P.AUTO = dict(A.P.AUTO, **{"site-slurm": False})
x = tc(A.plan(s_)[0])
check("评测：site-slurm 的 AUTO 关着 → hub 的 cpu 组", [(t["step"], t["group"]) for t in x] == [("eval", "cpu")])
A.P.AUTO = dict(A.P.AUTO, **{"site-slurm": True})
os.remove(f"{T}/preds/D103_alpha_one/V_save__{P}/frag_2/MANIFEST.json")
old_hub = ("Stashed", f"bash {QR}/testdata/bin/step.sh --stage infer {T}/{TCM} 2", None)
todo, stuck = A.plan(snap([old_hub]))
check("queue：hub pueue 里已经有这一步 → 不加", not tc(todo))
added = []
fake_tq = lambda nice, args: added.append((nice, args)) or (0, "加入")  # noqa: E731
A.P.AUTO["advance"] = True
done, _ = A.run(snap(), q=lambda *a: 0, tq=fake_tq)
check("queue：AUTO 开着时调用 taskq.py add（档位 -100）", any(n_ == -100 and f"key={TCQ}" in a_ for n_, a_ in added)
      and "任务队列 infer:t16_f2" in done)
added.clear()
A.P.HOLD = ["infer:*"]
done, _ = A.run(snap(), q=lambda *a: 0, tq=fake_tq)
check("queue：HOLD 对上的先不加", not any(f"key={TCQ}" in a_ for _, a_ in added))
A.P.HOLD = []
A.P.PLACE = dict(BASE)

# 10b. trainer：训这一折的执行机接着做（10/31：site-pueue2 训的折由 site-pueue 推理）
K3 = f"{TC}/3"
M3 = f"D103_alpha_one/V_save__{P}"
Q3, PRED3 = f"infer/{M3}/frag_3", f"preds/{M3}/frag_3"
TN3 = [f"{TCM}/dataset.json", f"{TCM}/frag_3/final.ckpt", "raw/holdout/A/inputs"]
old_sn = A.step_needs
A.step_needs = lambda cmd: TN3 if "D103_alpha_one" in cmd and cmd.endswith(" 3") and "--stage infer" in cmd else old_sn(cmd)


def lsnap(assigned=None, state="claimed", qrows=()):
    """K3 在总队列里分给了 assigned（None 且 claimed：没有这一行）；qrows：总队列里另外的行。"""
    s_ = osnap()
    if assigned or state != "claimed":
        s_["taskq"]["hub"]["tasks"].append(dict(key=K3, nice=60, state=state, worker=assigned, assigned=assigned))
    s_["taskq"]["hub"]["tasks"] += [dict(r) for r in qrows]
    return s_


def t3(todo):
    return [t for t in todo if t["key"] in (K3, Q3)]


def qrow(item, state="claimed"):
    """要加进总队列的一项 → 总队列里的这一行（taskq.py local_status 的样子）。"""
    return dict(key=item["qk"], nice=item["qnice"], state=state, recipe="command", exp=None, params=A.S.parse_kv(item["args"]))


x = t3(A.plan(lsnap("site-pueue"))[0])
check("trainer：分给 site-pueue 的折 → 推理现在就进总队列，只给 site-pueue、等训练（档位 -100）；评测在 site-slurm，不跟着提前排",
      [(t["step"], t.get("op")) for t in x] == [("infer", "taskq")] and 'machines=["site-pueue"]' in x[0]["args"]
      and "after=" + json.dumps([K3]) in x[0]["args"] and x[0]["qnice"] == -100 and f"key={Q3}" in x[0]["args"])
INF3 = x[0]
_ready = A.P.READY
A.P.READY = {k: v for k, v in _ready.items() if k != "site-pueue"}
check("READY 里没写 site-pueue：照样是执行机（缺项按 1，和 relay.py 一样，11/4），推理照样提前排给它",
      [t.get("args") for t in t3(A.plan(lsnap("site-pueue"))[0])] == [INF3["args"]])
A.P.READY = _ready
A.P.AUTO = dict(A.P.AUTO, **{"site-pueue": False})
check("trainer：site-pueue 关着 → site-pueue2 训的折不提前排", not t3(A.plan(lsnap("site-pueue2"))[0]))
A.P.AUTO = dict(A.P.AUTO, **{"site-pueue": True, "site-pueue2": True})
x = t3(A.plan(lsnap("site-pueue2"))[0])
check("trainer：按时段的 relay 机器（site-pueue2，晚班）训的折 → 推理给同一台主机上不按时段的 site-pueue，等训练",
      len(x) == 1 and 'machines=["site-pueue"]' in x[0]["args"] and "after=" + json.dumps([K3]) in x[0]["args"])
A.P.AUTO = dict(A.P.AUTO, **{"site-pueue": True, "site-pueue2": False})
CAPLF0 = A.M.RELAY["site-slurm2"].pop("cap", None)  # cap 在第 12 节单独测
for r_ in ("site-slurm2", "site-pueue"):
    x = t3(A.plan(lsnap(r_))[0])
    check(f"trainer：执行机（{r_}）训的折 → 推理现在就进总队列，只给它、等训练、带 relay=toy_infer",
          [(t["step"], t.get("op")) for t in x] == [("infer", "taskq")] and f'machines=["{r_}"]' in x[0]["args"]
          and "after=" + json.dumps([K3]) in x[0]["args"] and "relay=toy_infer" in x[0]["args"] and x[0]["qnice"] == -100)
if CAPLF0 is not None:
    A.M.RELAY["site-slurm2"]["cap"] = CAPLF0
check("trainer：推理已经在总队列里 → 不再加", not t3(A.plan(lsnap("site-pueue", qrows=[qrow(INF3, "waiting")]))[0]))
trained("D103_alpha_one", "V_save", "3")
x = t3(A.plan(lsnap("site-pueue"))[0])
check("trainer：执行机训完、拉回 hub、还没交还 → 推理给它，不接 after",
      [(t["step"], t.get("op")) for t in x] == [("infer", "taskq")] and 'machines=["site-pueue"]' in x[0]["args"]
      and not any(a_.startswith("after=") for a_ in x[0]["args"]))
x = t3(A.plan(lsnap(state="done"))[0])
check("trainer：执行机训的折已经交还（总队列里不再分给谁）→ 用下一个地方（site-slurm）",
      [(t["step"], t.get("op")) for t in x] == [("infer", "taskq")] and 'machines=["site-slurm"]' in x[0]["args"])
s_ = relay_fetch(lsnap("site-pueue", qrows=[qrow(INF3)]), 98, "site-pueue", Q3)
x = t3(A.plan(s_)[0])
check("trainer：执行机在拉回推理结果 → 不算做完、不再排", not x)
inferred("D103_alpha_one", "V_save", "3")
check("trainer：推理结果在 hub 上已经对得上、relay_fetch 还在跑 → 不算做完，不排评测", not t3(A.plan(s_)[0]))
s_["pueue"]["hub"]["tasks"][-1]["status"] = "Success"
check("trainer：relay_fetch 做完了 → 推理算做完，排评测", [t["step"] for t in t3(A.plan(s_)[0])] == ["eval"])
shutil.rmtree(f"{T}/{PRED3}")
# 变异审查（11/3）：relay 机器训完、hub 上已有训完的标志、但 relay_fetch（拉回训练）还在跑 → 推理不分给那台（拉回后那边的结果会删）
A.P.AUTO = dict(A.P.AUTO, **{"site-pueue": True})
x = t3(A.plan(relay_fetch(lsnap("site-pueue"), 96, "site-pueue", K3))[0])
check("trainer：relay_fetch 还在拉回训练结果 → 推理不分给那台", not any('machines=["site-pueue"]' in (t.get("args") or []) for t in x))
os.remove(f"{T}/{TCM}/frag_3/final.ckpt")
A.step_needs = old_sn

# 11. 实验一级带 queue 的步骤（10/23，P1 / P2 换模型重跑）：五折都在 hub 上以后进总队列给 site-slurm，拉回后 hub 上做比较
for e_, g_ in GQUEUED:
    e_["group_after"] = g_
A.P.PLACE = dict(REAL_PLACE)
MEN = "D111_beta"
MENM = f"results/{MEN}/V_save__{P}"
for k in F5:
    trained(MEN, "V_save", k)
PSTEPS = ("p1", "p2", "p2cmp")


def pr(todo):
    return sorted((t["step"], t.get("op", t["group"])) for t in todo if t["key"] == "t3b" and t["step"] in PSTEPS)


todo, stuck = A.plan(snap())
x = {t["step"]: t for t in todo if t["key"] == "t3b" and t["step"] in PSTEPS}
check("probe：五折都在 hub 上 → p1、p2 都进总队列给 site-slurm（档位 -100、带输入），p2cmp 先不排",
      pr(todo) == [("p1", "taskq"), ("p2", "taskq")] and x["p2"]["qnice"] == -100
      and "key=probe/p2_t3b" in x["p2"]["args"] and 'machines=["site-slurm"]' in x["p2"]["args"]
      and f"cmd=MODEL={{data}}/{MENM} TAG=t3b bash {QR}/testdata/bin/p2_run.sh && touch {{data}}/demo/probe/p2_t3b/.probe_done"
      in x["p2"]["args"] and "out=demo/probe/p2_t3b" in x["p2"]["args"]
      and any(a_.startswith("inputs=") and "demo/probe/p2/inputs" in a_ and f"{MENM}/frag_3/final.ckpt" in a_ for a_ in x["p2"]["args"]))
s_ = snap()
s_["taskq"]["hub"]["tasks"] += [dict(key="probe/p1_t3b", nice=-100), dict(key="probe/p2_t3b", nice=-100)]
todo, stuck = A.plan(s_)
check("probe：队列里已经有 → 不再加", pr(todo) == [] and "advance:p2:t3b" not in stuck)
touch(f"{T}/demo/probe/p2_t3b/.probe_done")
todo, stuck = A.plan(s_)
x = [t for t in todo if t["key"] == "t3b" and t["step"] in PSTEPS]
check("probe：p2 拉回了 → hub cpu 组排和 T0 的比较", [(t["step"], t["group"]) for t in x] == [("p2cmp", "cpu")]
      and x[0]["cmd"] == f"{PY} {QR}/testdata/bin/p2_compare.py --tags t3b --out {T}/demo/probe/p2_compare_t3b"
      and x[0]["label"] == "p2cmp:t3b")
touch(f"{T}/demo/probe/p2_compare_t3b/p2_compare.rec.json", '{"status": "ok"}')
touch(f"{T}/demo/probe/p1_t3b/.probe_done")
todo, stuck = A.plan(s_)
check("probe：p1、p2、p2cmp 都做完 → 没有要排的", pr(todo) == [] and not [k for k in stuck if k.split(":")[1:2] and k.split(":")[1] in PSTEPS and k.endswith(":t3b")])

# step_needs 运行出错（10/25：hub 上还没有 pueue-host 训的模型文件夹）和命令不支持分开报
A.step_needs = REAL_STEP_NEEDS
bad = "sh -c 'echo 没有这个模型文件夹 >&2; exit 3' --stage infer"
check("needs 出错：返回 None，原因带上错误原文", A.step_needs(bad) is None and A.no_needs(bad) == "--stage needs 出错（没有这个模型文件夹）")
check("needs 不支持：原样说不支持", A.step_needs("true") is None and A.no_needs("true") == "命令不支持 --stage needs")

# 每个实验的每份步骤都真跑一次 --stage needs，模型文件夹一个都不在本机（10/25：pueue-host 训的新数据集第一折，hub 上还没拉回）
with tempfile.TemporaryDirectory() as R:
    os.makedirs(f"{R}/raw")
    old_bt = {k: os.environ.get(k) for k in ("TASKQ_DATA",)}
    os.environ.update(TASKQ_DATA=R)
    seen, bad, todo = set(), [], []
    for t in A.candidates(R, sys.executable, [])[0]:
        for st in t["after"]:
            cmd = A.S.fill(st["cmd"], t["names"], keep=())
            if "--stage" not in cmd or (t["exp"], st["name"]) in seen:
                continue
            seen.add((t["exp"], st["name"]))
            todo.append((t, st, cmd))
    with ThreadPoolExecutor(8) as ex:
        res = list(ex.map(lambda x: REAL_STEP_NEEDS(x[2]), todo))
    for (t, st, cmd), got in zip(todo, res):
        want = "/final.ckpt" if "--stage infer" in cmd else "/dataset.json"
        if not got or not any(x.endswith(want) for x in got):
            bad.append(f"{t['lab']} {st['name']}：{A.no_needs(cmd)}")
    for k, v in old_bt.items():
        if v is None:
            os.environ.pop(k)
        else:
            os.environ[k] = v
    check(f"needs：{len(seen)} 个实验步骤在模型还没拉回时都能列出输入", seen and not bad)
    for b in bad[:5]:
        print("   ", b)

# 删单份概率的命令（testdata/bin/prune.py，接口同真的）对每个有 prune 的实验真跑一次（--dry-run，10/25）：刚训完、集成还没做，要报"集成没做完"、一个都不删
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
                r = subprocess.run(cmd + " --dry-run", shell=True, cwd=_cfg.REPO, capture_output=True, text=True)
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

# 12. 执行机在训的折：推理、评测现在就进总队列，after 接上前一步（10/25）
A.P.PLACE = dict(REAL_PLACE)
A.P.HOLD = []
SB = "D133_alpha_s18b/V_save"
SBK, SBM = f"{SB}/1", f"D133_alpha_s18b/V_save__{P}"
SBI = f"infer/{SBM}/frag_1"


def psnap(state="claimed", assigned="site-slurm", extra=()):
    """总队列里这一折分给了 assigned（10/31）。"""
    s_ = snap()
    s_["taskq"]["hub"]["tasks"] += [dict(key=SBK, nice=60, state=state, worker=assigned, assigned=assigned)] + list(extra)
    return s_


def sb(todo):
    return [t for t in todo if t["key"] == SBK]


x = sb(A.plan(psnap())[0])
check("site-slurm 在训：推理、评测一起进总队列，推理等训练、评测等推理（档位 -100，评测纯 CPU）",
      [(t["step"], t.get("op")) for t in x] == [("infer", "taskq"), ("eval", "taskq")]
      and "after=" + json.dumps([SBK]) in x[0]["args"] and "after=" + json.dumps([SBI]) in x[1]["args"]
      and x[0]["qnice"] == x[1]["qnice"] == -100 and "device=cpu" in x[1]["args"] and 'machines=["site-slurm"]' in x[0]["args"])
x = sb(A.plan(psnap(assigned="site-pueue"))[0])
check("分给 site-pueue 的折：推理给 site-pueue，评测写的是 site-slurm，不跟着提前排（10/31）",
      [t["step"] for t in x] == ["infer"] and 'machines=["site-pueue"]' in x[0]["args"])
CAPLF = A.M.RELAY["site-slurm2"].pop("cap", None)
x = sb(A.plan(psnap(assigned="site-slurm2"))[0])
check("分给中转执行机的折：推理进总队列只给它，评测写的是 site-slurm，不跟着提前排",
      [t["step"] for t in x] == ["infer"] and 'machines=["site-slurm2"]' in x[0]["args"])
A.M.RELAY["site-slurm2"]["cap"] = 60
x = sb(A.plan(psnap(assigned="site-slurm2"))[0])
check("分给中转执行机的折、它的根目录放不下推理（cap，11/2）→ 不提前排给它", not any('machines=["site-slurm2"]' in t.get("args", []) for t in x))
x = sb(A.plan(psnap(assigned="site-slurm2", extra=[dict(key=SBI, nice=-100, state="waiting", spec=dict(machines=["site-slurm2"]))]))[0])
check("已经提前排给它的推理，现在放不下 → 撤掉（unqueue）", any(t.get("op") == "unqueue" for t in x))
IR = dict(key=SBI, nice=-100, state="free", spec=dict(machines=["site-slurm"]))
check("训练做完了（done）、推理等着那边领 → 不撤", not any(t.get("op") == "unqueue" for t in A.plan(psnap(state="done", extra=[IR]))[0]))
x = [t for t in A.plan(psnap(state="free", assigned=None, extra=[IR]))[0] if t.get("op") == "unqueue"]
check("训练交还了（free）→ 先前提前排的推理撤掉", len(x) == 1 and x[0]["qks"] == [SBI])
rm_ = []
A.run(psnap(state="free", assigned=None, extra=[IR]), q=lambda *a: 0, tq=lambda n, a: (0, ""), rm=lambda ks: rm_.append(ks) or (0, ""))
check("撤掉的调用 taskq.py remove", rm_ == [[SBI]])
_, st_ = A.run(psnap(state="free", assigned=None, extra=[IR]), q=lambda *a: 0, tq=lambda n, a: (0, ""), rm=lambda ks: (A.taskq.GONE, "队列里没有"))
check("撤的时候队列里已经没有了（remove 退出码 GONE）：不算卡住（11/4）", not any(k.startswith("advance:unqueue") for k in st_))
_, st_ = A.run(psnap(state="free", assigned=None, extra=[IR]), q=lambda *a: 0, tq=lambda n, a: (0, ""), rm=lambda ks: (A.taskq.REFUSED, "不删"))
check("remove 拒绝（REFUSED）：报卡住", any(k.startswith("advance:unqueue") for k in st_))
A.M.RELAY["site-slurm2"].pop("cap")
if CAPLF is not None:
    A.M.RELAY["site-slurm2"]["cap"] = CAPLF
check("还没分出去的折：不提前排", not sb(A.plan(psnap(state="free", assigned=None))[0]))
x = sb(A.plan(psnap(extra=[dict(key=SBI, nice=-100, state="waiting")]))[0])
check("推理已经在队列里：只加评测，等那个推理", [t["step"] for t in x] == ["eval"] and "after=" + json.dumps([SBI]) in x[0]["args"])
x = sb(A.plan(psnap(extra=[dict(key=SBI, nice=-100, state="waiting"), dict(key=f"eval/{SBM}/frag_1", nice=-100, state="waiting")]))[0])
check("推理、评测都在队列里：不再加", not x)
A.P.HOLD = ["infer:*"]
check("HOLD 对上推理：推理、评测都先不提前排", not sb(A.plan(psnap())[0]))
A.P.HOLD = []
A.P.PLACE = dict(REAL_PLACE, eval=["hub:cpu"])
x = sb(A.plan(psnap())[0])
check("评测写的是 hub：只提前排推理", [t["step"] for t in x] == ["infer"])
A.P.PLACE = dict(REAL_PLACE)

# 12b. 五折集成：进总队列给 site-slurm（10/25）；结果还在拉回时不算做完
S18 = "D132_alpha_s18"
S18M = f"{S18}/V_save__{P}"
for k in F5:
    trained(S18, "V_save", k)
    inferred(S18, "V_save", k)
    evaluated(S18, "V_save", f"frag_{k}")


def ens18(todo):
    return [t for t in todo if t["key"] == "t18" and t["step"] == "ens"]


x = ens18(A.plan(snap())[0])
check("集成：进总队列给 site-slurm（纯 CPU、拉回集成的预测和报告）",
      len(x) == 1 and x[0].get("op") == "taskq" and f"key=ens/{S18M}" in x[0]["args"] and "device=cpu" in x[0]["args"]
      and 'machines=["site-slurm"]' in x[0]["args"] and f"out=demo/analysis/{S18M}/frags_0-1-2-3-4" in x[0]["args"]
      and 'done=[".ens_done"]' in x[0]["args"]
      and any(a_.startswith("fetch=") and f"preds/{S18M}/frags_0-1-2-3-4" in a_ for a_ in x[0]["args"])
      and "ENSEMBLE_ONLY=1" in x[0]["cmd"] and "'0 1 2 3 4'" in x[0]["cmd"])
evaluated(S18, "V_save", "frags_0-1-2-3-4")  # 报告先拷到了 hub，集成的预测、.ens_done 还在拉
s_ = relay_fetch(snap(), 0, "site-slurm", f"ens/{S18M}")
s_["taskq"]["hub"]["tasks"].append(dict(key=f"ens/{S18M}", nice=-100, state="done"))
todo, stuck = A.plan(s_)
check("集成：还在从 site-slurm 拉回（报告已经到了）→ 不算做完，不排删单折概率图、不重排集成",
      not [t for t in todo if t["key"] == "t18"] and not any(k_.endswith(":t18") for k_ in stuck))
todo, _ = A.plan(snap())
check("集成：拉完 → 排删单折概率图（hub）", [t["step"] for t in todo if t["key"] == "t18"] == ["prune"])

os.remove(f"{T}/demo/analysis/{S18M}/frags_0-1-2-3-4/report/report.rec.json")
A.P.PLACE = dict(REAL_PLACE, ens=["hub:cpu"])  # 12c 看 hub 上排的集成
refill = A.refill
# 12c. hub 的任务做完马上跑一次健康检查（refill.then_check，锁最多等 5 分钟）；包过的命令照样认得出是哪一步
calls.clear()
A.run(snap(), q=fake_q, tq=lambda *a: (0, ""))
ec = [c for c in calls if "--label ens:t18 " in c]
check("hub 的任务包上 kick（TASKQ_TASK_DONE=标签）", len(ec) == 1 and f"TASKQ_TASK_DONE=ens:t18 bash {refill.KICK} kick" in ec[0] and refill.KICK.startswith(refill.M.MACHINES["hub"]["repo"] + "/"))
wrapped = A.refill.then_check(ens18(A.plan(snap())[0])[0]["cmd"], "ens:t18")
er = [r for r in A.C.load_records() if r["label"] == "ens:t18"]
s_ = dict(snap(), records=A.C.load_records())  # 只用 run 写的记录，不按旧规则配
s_["pueue"]["hub"]["tasks"] = [dict(id=er[-1]["id"], status="Running", command=wrapped, label="ens:t18", group="cpu", end=None)]
check("run 加任务前写了记录（hub、步骤、key、组）；包过的命令在跑按记录认出、不重排",
      er and er[-1]["machine"] == "hub" and (er[-1]["step"], er[-1]["key"], er[-1]["group"]) == ("ens", "t18", "cpu") and not ens18(A.plan(s_)[0]))
s_["pueue"]["hub"]["tasks"][0]["label"] = "other"
check("任务号对上、标签不同（任务号被重用）→ 不认，照常排", ens18(A.plan(s_)[0]))

_qd = dict(inputs=["p/{m}/s{seed}/f{frag}/x", "p/{m}/all"])
check("实验一级步骤的 inputs：each 的参数按 <参数>_list 的组合展开，不认具体参数名；每个任务的步骤照填",
      A.inputs_of(_qd, dict(m="M", frag_list="0 1", seed_list="7"), {"frag": ["0", "1"], "seed": ["7"]})
      == ["p/M/s7/f0/x", "p/M/s7/f1/x", "p/M/all"]
      and A.inputs_of(_qd, dict(m="M", frag="1", seed="7"), {"frag": "1", "seed": "7"}) == ["p/M/s7/f1/x", "p/M/all"])

print("\n全部通过" if ok else "\n有失败")
sys.exit(0 if ok else 1)
