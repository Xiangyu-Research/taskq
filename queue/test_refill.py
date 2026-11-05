#!/usr/bin/env python3
"""测 refill.py（2026-10-18）：挑任务（candidates、progress、data_gb）和 Slurm 补作业的规则（topup_plan、cpu_plan、slurm_plan），
用假的队列状态，不连任何机器。

    python3 queue/test_refill.py
"""
import copy
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, HERE)
os.environ["TASKQ_TASKS"] = os.path.join(fixture.tmpdir(), "tasks.json")  # 不碰真的任务记录
os.environ["TASKQ_LAST"] = os.path.join(fixture.tmpdir(), "last")
import refill as R  # noqa: E402

fails = 0
D = fixture.tmpdir()
R.C.run = lambda *a, **k: (_ for _ in ()).throw(AssertionError("测试里不应该在真机器上跑命令"))
GB = {"D103_alpha_one": 4.0, "D121_alphagamma": 62.0, "stuff": 1.0}
_hub = R.M.MACHINES[R.M.hub()]
_d0 = _hub["data"]
_hub["data"] = D
os.makedirs(f"{D}/raw/A/x")
for n_, b_ in (("a-lbl.dat", 1000), ("b-lbl.dat", 2000), ("c.dat", 4000)):
    open(f"{D}/raw/A/x/{n_}", "wb").write(b"0" * b_)
_gb = (R.data_gb(["raw/A/*/*-lbl.dat"]), R.data_gb(["raw/A/x/c.dat"]), R.data_gb(["raw/B/*.dat"]))
_hub["data"] = _d0
assert _gb == (3e-6, 4e-6, None), _gb
print("ok data_gb：带通配符的输入展开后算大小，对不上 → None（11/2：评测的输入带通配符，之前一直当成不在）")
R.data_gb = lambda paths: sum(GB.get(R.tag(p), 10.0) for p in paths)
NN, UM = "V_save", "V_m"
DS = {101: "D101_alpha", 103: "D103_alpha_one", 111: "D111_beta"}
PP = "prepared/"


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def tq(key, nice, order, state="free", worker=None, skipped_by=()):
    return dict(key=key, nice=nice, order=order, state=state, worker=worker, kind="-", host=worker, skipped_by=list(skipped_by))


K_T14 = f"D121_alphagamma/{NN}/0"   # ① 但数据集 62 GB 放不下
K_T1C = [f"D103_alpha_one/{NN}/{f}" for f in range(3)]
K_UM = f"D101_alpha/{UM}/1"
QUEUE = [tq(K_T14, 0, 1), tq(K_UM, 50, 2), tq(K_T1C[0], 50, 3), tq(K_T1C[1], 50, 4), tq(K_T1C[2], 100, 5)]


def snap(queue=QUEUE, enabled=True):
    return dict(ok={"hub": True, "site-slurm": True}, src={"hub": {"tasks": {}}, "site-slurm": {"tasks": {}}},
                taskq={"hub": dict(enabled=enabled, tasks=copy.deepcopy(list(queue)), recent_skips={})}, pueue={"hub": {"tasks": []}})


# ---------------- Slurm 补作业 ----------------
QOS = {"long": "1-12:00:00", "normal": "08:00:00"}
p, why = R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 216, 0), "normal": (10, 138, 0)}, QOS)
ok("现在：旧作业远超上限，不补", p == [] and "排满" in why)
p, _ = R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 9, 0), "normal": (10, 3, 0)}, QOS)
ok("long 空 1 个、normal 空 7 个：按名额排满 long 1、normal 7（11/2 起不按备好的个数截）", p == [("long", "1-12:00:00", 1, "worker.sbatch"), ("normal", "08:00:00", 7, "worker.sbatch")])
p, _ = R.topup_plan(snap()["taskq"]["hub"], {"long": (None, 4, 0), "normal": (10, 10, 0)}, QOS)
ok("QOS 没设排队上限：按 SUBMIT_CAP", p == [("long", "1-12:00:00", R.SL.SUBMIT_CAP - 4, "worker.sbatch")])
p, _ = R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 6, 0)}, {"long": ("3-00:00:00", 2)})
ok("留 2 个名额：上限 10、现有 6，补 2", p == [("long", "3-00:00:00", 2, "worker.sbatch")])
p, why = R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 8, 0)}, {"long": ("3-00:00:00", 2)})
ok("留 2 个名额：现有 8 就不补", p == [] and "排满" in why)
done_q = [tq(K_T14, 0, 1, "done"), tq(K_UM, 50, 2, "claimed", "486191")]
ok("没人领的没有、有在训的：照样排满（攒排队时间）", R.topup_plan(snap(queue=done_q)["taskq"]["hub"], {"long": (10, 2, 0), "normal": (10, 0, 0)}, QOS)[0]
   == [("long", "1-12:00:00", 8, "worker.sbatch"), ("normal", "08:00:00", 10, "worker.sbatch")])
ok("没人领的、在训的都没有：不补", R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "done")])["taskq"]["hub"], {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
ok("领取过期的（stale）也算没人领", R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "stale")])["taskq"]["hub"], {"long": (10, 0, 0), "normal": (10, 10, 0)}, QOS)[0] != [])
ok("总开关不在：不补", R.topup_plan(snap(enabled=False)["taskq"]["hub"], {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
paused = snap()["taskq"]["hub"] | {"paused": True}
ok("队列暂停：不补", R.topup_plan(paused, {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
ok("读不到某个 QOS 的作业数：都不补", R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 0, 0)}, QOS)[0] == [])
# 按名额排满（11/2 定，之前排着的不超过备好的任务数）
two = [tq(K_T14, 0, 1), tq(K_UM, 50, 2), tq(K_T1C[0], 50, 3, "claimed", "486191")]
p, _ = R.topup_plan(snap(queue=two)["taskq"]["hub"], {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 0)})
ok("备好 2 个、空 10 个位置：交 10 个", p == [("long", "3-00:00:00", 10, "worker.sbatch")])
p, why = R.topup_plan(snap(queue=two)["taskq"]["hub"], {"long": (10, 4, 2)}, {"long": ("3-00:00:00", 1)})
ok("留 1 个、已有 4 个（2 个在排）：再交 5 个", p == [("long", "3-00:00:00", 5, "worker.sbatch")])
p, why = R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "done")])["taskq"]["hub"], {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 0)})
ok("没有要卡的任务：不交", p == [] and "没有要卡的任务" in why)
ok("slurm.parse：5 段才算，少一段当读不到", R.SL.parse("QOS|long|10|3|1\nQOS|normal|10|2\n")["counts"] == {"long": (10, 3, 1)})
ok("refill 里和 sites/slurm.py 重复的都删了", not any(hasattr(R, f) for f in ("topup_probe", "parse_qos")))

# 纯 CPU 任务（device=cpu，10/24）：GPU worker 不为它们补，有这类任务时才交 CPU worker
K_EV = "eval/D101_alpha/x/frag_0"
ev_spec = dict(key=K_EV, env="base", machines=["site-slurm"], device="cpu")
cpu_q = [tq(K_T14, 0, 1, "done"), dict(tq(K_EV, -100, 2), spec=ev_spec), dict(tq(K_EV + "b", -100, 3), spec=dict(ev_spec, key=K_EV + "b"))]
cq = snap(queue=cpu_q)["taskq"]["hub"]
p, why = R.topup_plan(cq, {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 2)})
ok("只有纯 CPU 任务没人领：不补 GPU worker", p == [] and "要卡" in why)
ok("纯 CPU 任务 2 个、最多 1 个、现有 0：交 1 个 CPU worker", R.cpu_plan(cq, 0, ("normal", "08:00:00", 1), 5) == ([("normal", "08:00:00", 1, "cpu_worker.sbatch")], ""))
ok("最多 3 个：按任务数交 2 个", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 5)[0] == [("normal", "08:00:00", 2, "cpu_worker.sbatch")])
ok("已有 1 个 CPU worker：不交", R.cpu_plan(cq, 1, ("normal", "08:00:00", 1), 5)[0] == [])
ok("读不到 CPU worker 作业数：不交", R.cpu_plan(cq, None, ("normal", "08:00:00", 1), 5)[0] == [])
ok("没有纯 CPU 任务：不交", R.cpu_plan(snap()["taskq"]["hub"], 0, ("normal", "08:00:00", 1), 5)[0] == [])
ok("队列暂停：不交", R.cpu_plan(cq | {"paused": True}, 0, ("normal", "08:00:00", 1), 5)[0] == [])
ok("最多 3 个、只剩 1 个提交名额：交 1 个", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 1)[0] == [("normal", "08:00:00", 1, "cpu_worker.sbatch")])
ok("提交名额用完：不交", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 0) == ([], "提交名额用完了"))
ok("读不到剩几个名额：不交", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), None)[0] == [])

CONF = dict(gpu={"long": ("3-00:00:00", 2)}, cpu=("normal", "08:00:00", 1))
ST0 = dict(counts={"long": (10, 0, 0)}, cpu=0, free=5)
for q_ in (cq, snap()["taskq"]["hub"]):
    p, why = R.slurm_plan(q_, ST0, CONF)
    g, gwhy = R.topup_plan(q_, ST0["counts"], CONF["gpu"])
    c, cwhy = R.cpu_plan(q_, 0, CONF["cpu"], 5)
    ok(f"slurm_plan = topup_plan + cpu_plan（{p}）", p == g + c and why == (gwhy if c else f"{gwhy}；CPU：{cwhy}".lstrip("；")))
p, why = R.slurm_plan(cq, ST0, dict(cpu=CONF["cpu"]))
ok("slurm_plan：只有 cpu 名额的机器只交 CPU worker", p == [("normal", "08:00:00", 1, "cpu_worker.sbatch")] and why == "")
p, why = R.slurm_plan(snap()["taskq"]["hub"], ST0, dict(cpu=CONF["cpu"]))
ok("slurm_plan：没有纯 CPU 任务时说原因", p == [] and why == "CPU：没有没人领的纯 CPU 任务")

ok("candidates：默认只要要卡的", R.candidates(snap(), "site-slurm", cq, set()) == [])
ok("candidates：kinds 里有 cpu 才要纯 CPU 任务",
   [t["key"] for t in R.candidates(snap(), "site-slurm", cq, set(), kinds=("gpu", "cpu"))] == [K_EV, K_EV + "b"])
lq = [dict(tq(K_EV, -100, 2), spec=dict(ev_spec, env=["base", "stats"])),
      dict(tq(K_EV + "b", -100, 3), spec=dict(ev_spec, key=K_EV + "b", env=["base", "没有的环境"]))]
ok("candidates：env 是列表时每个环境都要备好",
   [t["key"] for t in R.candidates(snap(), "site-slurm", snap(queue=lq)["taskq"]["hub"], set(), kinds=("cpu",))] == [K_EV])
R.SL.P.SLURM = {"site-slurm": dict(gpu={"long": ("3-00:00:00", 2)}, cpu=("normal", "08:00:00", 1))}
probe = R.SL.probe("site-slurm")
ok("查作业数用 policy.py 的 QOS、CPU worker 按交的脚本认",
   "show qos normal " in probe and "cpu_worker[.]sbatch" in probe and "for q in long;" in probe)


for f in (R.GPU_FILE, R.CPU_FILE):
    ok(f"{f} 不写 --job-name（作业名交的时候随机取，11/2）",
       not any(ln.startswith("#SBATCH --job-name") for ln in open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f))))
ok("refill 的 sbatch 文件名和 slurm.FILES 一致", {"gpu": R.GPU_FILE, "cpu": R.CPU_FILE} == R.SL.FILES)

# 那边的文件读不动（spec.collect 的 stuck，11/1）：当成有进度，不当新任务分出去
st = {"src": {"site-slurm": {"tasks": {}, "stuck": ["k/1"]}, "hub": {"tasks": {}}}}
ok("progress：读不动的任务当成有进度（看所有机器、看那台）；别的机器、别的任务照常",
   R.progress(st, "k/1") and R.progress(st, "k/1", "site-slurm") and not R.progress(st, "k/1", "hub") and not R.progress(st, "k/2"))
st = {"src": {"site-slurm": {"tasks": {}, "errors": {"k/1": "OSError: x"}}, "hub": {"tasks": {}}}}
ok("progress：读出错的任务（spec.collect 的 errors，11/4）同样当成有进度", R.progress(st, "k/1", "site-slurm") and not R.progress(st, "k/1", "hub"))

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
