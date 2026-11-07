#!/usr/bin/env python3
"""测 refill.py：派任务（choose：按容量挑执行机；places、holders、progress、data_sizes）和 Slurm 补作业的规则
（topup_plan、cpu_plan、slurm_plan），用假的队列状态，不连任何机器。

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
ok("long 空 1 个、normal 空 7 个：按名额排满 long 1、normal 7", p == [("long", "1-12:00:00", 1, "gpu"), ("normal", "08:00:00", 7, "gpu")])
p, _ = R.topup_plan(snap()["taskq"]["hub"], {"long": (None, 4, 0), "normal": (10, 10, 0)}, QOS)
ok("QOS 没设排队上限：按 SUBMIT_CAP", p == [("long", "1-12:00:00", R.SL.SUBMIT_CAP - 4, "gpu")])
p, _ = R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 6, 0)}, {"long": ("3-00:00:00", 2)})
ok("留 2 个名额：上限 10、现有 6，补 2", p == [("long", "3-00:00:00", 2, "gpu")])
p, why = R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 8, 0)}, {"long": ("3-00:00:00", 2)})
ok("留 2 个名额：现有 8 就不补", p == [] and "排满" in why)
done_q = [tq(K_T14, 0, 1, "done"), tq(K_UM, 50, 2, "claimed", "486191")]
ok("没人领的没有、有在训的：照样排满（攒排队时间）", R.topup_plan(snap(queue=done_q)["taskq"]["hub"], {"long": (10, 2, 0), "normal": (10, 0, 0)}, QOS)[0]
   == [("long", "1-12:00:00", 8, "gpu"), ("normal", "08:00:00", 10, "gpu")])
ok("没人领的、在训的都没有：不补", R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "done")])["taskq"]["hub"], {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
ok("领取过期的（stale）也算没人领", R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "stale")])["taskq"]["hub"], {"long": (10, 0, 0), "normal": (10, 10, 0)}, QOS)[0] != [])
ok("总开关不在：不补", R.topup_plan(snap(enabled=False)["taskq"]["hub"], {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
paused = snap()["taskq"]["hub"] | {"paused": True}
ok("队列暂停：不补", R.topup_plan(paused, {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
ok("读不到某个 QOS 的作业数：都不补", R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 0, 0)}, QOS)[0] == [])
# 按名额排满
two = [tq(K_T14, 0, 1), tq(K_UM, 50, 2), tq(K_T1C[0], 50, 3, "claimed", "486191")]
p, _ = R.topup_plan(snap(queue=two)["taskq"]["hub"], {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 0)})
ok("备好 2 个、空 10 个位置：交 10 个", p == [("long", "3-00:00:00", 10, "gpu")])
p, why = R.topup_plan(snap(queue=two)["taskq"]["hub"], {"long": (10, 4, 2)}, {"long": ("3-00:00:00", 1)})
ok("留 1 个、已有 4 个（2 个在排）：再交 5 个", p == [("long", "3-00:00:00", 5, "gpu")])
p, why = R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "done")])["taskq"]["hub"], {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 0)})
ok("没有要卡的任务：不交", p == [] and "没有要卡的任务" in why)
ok("slurm.parse：5 段才算，少一段当读不到", R.SL.parse("QOS|long|10|3|1\nQOS|normal|10|2\n")["counts"] == {"long": (10, 3, 1)})
ok("refill 里和 sites/slurm.py 重复的都删了", not any(hasattr(R, f) for f in ("topup_probe", "parse_qos")))

# 纯 CPU 任务（device=cpu）：GPU worker 不为它们补，有这类任务时才交 CPU worker
K_EV = "eval/D101_alpha/x/frag_0"
ev_spec = dict(key=K_EV, env="base", machines=["site-slurm"], device="cpu")
cpu_q = [tq(K_T14, 0, 1, "done"), dict(tq(K_EV, -100, 2), spec=ev_spec), dict(tq(K_EV + "b", -100, 3), spec=dict(ev_spec, key=K_EV + "b"))]
cq = snap(queue=cpu_q)["taskq"]["hub"]
p, why = R.topup_plan(cq, {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 2)})
ok("只有纯 CPU 任务没人领：不补 GPU worker", p == [] and "要卡" in why)
ok("纯 CPU 任务 2 个、最多 1 个、现有 0：交 1 个 CPU worker", R.cpu_plan(cq, 0, ("normal", "08:00:00", 1), 5) == ([("normal", "08:00:00", 1, "cpu")], ""))
ok("最多 3 个：按任务数交 2 个", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 5)[0] == [("normal", "08:00:00", 2, "cpu")])
ok("已有 1 个 CPU worker：不交", R.cpu_plan(cq, 1, ("normal", "08:00:00", 1), 5)[0] == [])
ok("读不到 CPU worker 作业数：不交", R.cpu_plan(cq, None, ("normal", "08:00:00", 1), 5)[0] == [])
ok("没有纯 CPU 任务：不交", R.cpu_plan(snap()["taskq"]["hub"], 0, ("normal", "08:00:00", 1), 5)[0] == [])
ok("队列暂停：不交", R.cpu_plan(cq | {"paused": True}, 0, ("normal", "08:00:00", 1), 5)[0] == [])
ok("最多 3 个、只剩 1 个提交名额：交 1 个", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 1)[0] == [("normal", "08:00:00", 1, "cpu")])
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
ok("slurm_plan：只有 cpu 名额的机器只交 CPU worker", p == [("normal", "08:00:00", 1, "cpu")] and why == "")
p, why = R.slurm_plan(snap()["taskq"]["hub"], ST0, dict(cpu=CONF["cpu"]))
ok("slurm_plan：没有纯 CPU 任务时说原因", p == [] and why == "CPU：没有没人领的纯 CPU 任务")


# ---------------- 派任务：choose ----------------
def site(name, room, **kw):
    kw.setdefault("envs", ("base", "stats"))
    kw.setdefault("root", ((name, "cap"), 1000.0))
    return R.Site(name, room, **kw)


def ctq(key, nice=50, order=1, **sp):
    return dict(tq(key, nice, order, skipped_by=sp.pop("skipped_by", ())), spec=dict(dict(key=key, env="base"), **sp))


def pick(tasks, sites, **kw):
    specs = {t["key"]: ("demo", t["spec"]) for t in tasks}
    res = R.choose(tasks, {s_.name: s_ for s_ in sites}, specs, **kw)
    got = {t["key"]: s_.name for s_ in sites for t in s_.take}
    return got, res


ok("places：没写的步骤 = 有这种 worker 的执行机，按机器表的顺序", R.places("demo", {}) == ["site-pueue", "site-slurm", "site-slurm2"])
ok("places：纯 CPU 的只去有 CPU worker 的", R.places("demo", dict(device="cpu")) == ["site-pueue", "site-slurm"])
ok("places：PLACE 写了的按它（infer：slurm 先）", R.places("x", dict(step="infer")) == ["site-slurm", "site-pueue"])
ok("places：再和说明的 machines 取交集", R.places("x", dict(step="infer", machines=["site-pueue"])) == ["site-pueue"])
four = [ctq(f"c/{i}", order=i) for i in range(4)]
got, res = pick(four, [site("site-pueue", dict(gpu=2)), site("site-slurm", dict(gpu=1)), site("site-slurm2", dict(gpu=1))])
ok("两三台都能去：按空位分（偏好的先填满，再给下一台）",
   got == {"c/0": "site-pueue", "c/1": "site-pueue", "c/2": "site-slurm", "c/3": "site-slurm2"} and res == dict(why={}, nowhere=[], noroom=[]))
got, res = pick(four, [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=1))])
ok("空位不够：剩下的这轮不派，说清每台为什么", len(got) == 2 and set(res["why"]) == {"c/2", "c/3"}
   and "site-pueue：排满了" in res["why"]["c/2"] and "site-slurm：排满了" in res["why"]["c/2"] and res["noroom"] == [])
got, res = pick([ctq("s/1", skipped_by=["site-pueue"])], [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=1))])
ok("被一台跳过：转去能去的别台", got == {"s/1": "site-slurm"})
got, res = pick([ctq("s/2", skipped_by=["site-pueue", "site-slurm", "site-slurm2"])], [site("site-pueue", dict(gpu=1))])
ok("能去的都跳过了：不派，记进 nowhere（relay 报警）", got == {} and res["nowhere"] == ["s/2"])
got, res = pick([ctq("s/3", step="p1", skipped_by=["site-slurm"])], [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=1))])
ok("PLACE 只许一台、它跳过了：同样报 nowhere，不去别台", got == {} and res["nowhere"] == ["s/3"])
ts = [ctq("h/1", order=2), ctq("h/2", order=1)]
got, res = pick(ts, [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=1))], held={"h/1": ["site-slurm"]})
ok("做了一半的：只给有进度的那台，同一档里先派", got == {"h/1": "site-slurm", "h/2": "site-pueue"})
got, res = pick([ctq("h/3")], [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=0))], held={"h/3": ["site-slurm"]})
ok("做了一半的、那台排满：等它，不给别台从头做", got == {} and "site-slurm：排满了" in res["why"]["h/3"])
got, res = pick([ctq("h/4")], [site("site-pueue", dict(gpu=1))], held={"h/4": ["site-slurm"]})
ok("做了一半的、那台这轮连不上：不派，说那台", got == {} and "site-slurm" in res["why"]["h/4"])
got, _ = pick([ctq("i/1", inputs=["in/a"])], [site("site-pueue", dict(gpu=1)), site("site-slurm2", dict(gpu=1), ready=["in/a"])],
              sizes={"in/a": 5.0})
ok("输入已经在那边的先（不管偏好）", got == {"i/1": "site-slurm2"})
got, res = pick([ctq("i/2", inputs=["in/b"])], [site("site-pueue", dict(gpu=1))], sizes={"in/b": None})
ok("store 上没有的输入：不派，说缺哪个", got == {} and "store 上没有输入 in/b" in res["why"]["i/2"])
# 空间：输入 + 产出（out_gb，没写 = proto.OUT_GB）后至少还剩 keep；同一个盘的几台合起来扣
disk = (("fake", "dev1"), 50.0)
got, res = pick([ctq("g/1", inputs=["in/c"]), ctq("g/2", order=2, inputs=["in/d"])],
                [site("site-pueue", dict(gpu=5), root=disk, keep=15), site("site-slurm", dict(gpu=5), root=disk, keep=15)],
                sizes={"in/c": 20.0, "in/d": 20.0})
ok("空间：第一个放下（50-20-10≥15），同盘的第二个哪台都放不下（记进 noroom：relay 报 tqroom；只是排满的不记）", got == {"g/1": "site-pueue"} and "放不下" in res["why"]["g/2"]
   and "site-slurm：放不下" in res["why"]["g/2"] and res["noroom"] == ["g/2"])
got, _ = pick([ctq("g/3", inputs=["in/c"], out_gb=1)], [site("site-pueue", dict(gpu=1), root=(("a", 1), 30.0), keep=5)],
              sizes={"in/c": 20.0})
ok("空间：说明写了 out_gb 时按它留", got == {"g/3": "site-pueue"})
got, res = pick([ctq("g/4", inputs=["in/c"], out_gb=1)],
                [site("site-pueue", dict(gpu=1), root=(("a", 1), 100.0), blob=(("a", 2), 10.0), keep=5)], sizes={"in/c": 20.0})
ok("空间：blobs 在另一个盘时输入扣在 blobs 那边", got == {} and "放不下" in res["why"]["g/4"])
idle = {"gpu": [dict(key="old/1", nice=100, out=10.0), dict(key="old/2", nice=100, out=10.0)]}
sp_ = site("site-pueue", dict(gpu=0), idle=idle)
got, res = pick([ctq("w/1", nice=50), ctq("w/2", nice=50, order=2)], [sp_])
ok("备满时：更优先的换下一个备好没人领的（每种每轮一个）", got == {"w/1": "site-pueue"}
   and [(a["key"], b["key"]) for a, b in sp_.swaps] == [("old/1", "w/1")] and "排满了" in res["why"]["w/2"])
got, _ = pick([ctq("n/1", nice=-1), ctq("n/2", nice=-1, order=2)], [site("site-pueue", dict(gpu=0))])
ok("档位 < 0 的不占空位", got == {"n/1": "site-pueue", "n/2": "site-pueue"})
got, res = pick([ctq("e/1", env="stats")], [site("site-pueue", dict(gpu=1), envs=("base",)), site("site-slurm", dict(gpu=1))])
ok("环境没就绪的那台不派，去下一台", got == {"e/1": "site-slurm"})
got, _ = pick([ctq("k/c", device="cpu")], [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=1, cpu=1))])
ok("没有这种 worker 的那台不派", got == {"k/c": "site-slurm"})
told = []
got, res = pick([ctq("b/1")], [site("site-pueue", dict(gpu=1), block=lambda k, sp: "代码包跑不了"),
                               site("site-slurm", dict(gpu=1), took=lambda k, sp: told.append(k))])
ok("block 挡住的那台不派，领了告诉 took", got == {"b/1": "site-slurm"} and told == ["b/1"])
ok("档位优先于做了一半", list(pick([ctq("o/1", nice=60), ctq("o/2", nice=50, order=2)], [site("site-pueue", dict(gpu=1))],
                                  held={"o/1": ["site-pueue"]})[0]) == ["o/2"])
ok("data_sizes：store 上量到的字节 → GB；不在、一个都没匹配上 → None",
   R.data_sizes(["a", "b", "c", "a"], lambda ps: {"a": dict(n=2, size=3e9), "b": dict(n=0, size=0)}) == {"a": 3.0, "b": None, "c": None})
held = R.holders({"site-pueue": {"h/c": {"ran": True}}, "site-slurm": {"h/b": {"resumable": True}, "h/d": {"ran": False}},
                  "site-slurm2": {"h/a": {"runs": [{"epochs_done": 3}]}}}, lambda n: n != "site-slurm2")
ok("holders：只算有进度、现在能用的执行机", held == {"h/c": ["site-pueue"], "h/b": ["site-slurm"]})
ok("site_progress：那边报的 ran 算进度，空的不算", R.site_progress({"ran": True}) and not R.site_progress({}) and not R.site_progress(None))
R.SL.P.SLURM = {"site-slurm": dict(gpu={"long": ("3-00:00:00", 2)}, cpu=("normal", "08:00:00", 1))}
probe = R.SL.probe("site-slurm")
ok("查作业数用 policy.py 的 QOS、CPU worker 按交的脚本认",
   "show qos normal " in probe and "cpu_worker[.]sbatch" in probe and "for q in long;" in probe)


for f in R.SL.FILES.values():
    ok(f"{f} 不写 --job-name（作业名交的时候随机取）",
       not any(ln.startswith("#SBATCH --job-name") for ln in open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f))))

# 那边的文件读不动（spec.collect 的 stuck）：当成有进度，不当新任务分出去
st = {"src": {"site-slurm": {"tasks": {}, "stuck": ["k/1"]}, "hub": {"tasks": {}}}}
ok("progress：读不动的任务当成有进度（看所有机器、看那台）；别的机器、别的任务照常",
   R.progress(st, "k/1") and R.progress(st, "k/1", "site-slurm") and not R.progress(st, "k/1", "hub") and not R.progress(st, "k/2"))
st = {"src": {"site-slurm": {"tasks": {}, "errors": {"k/1": "OSError: x"}}, "hub": {"tasks": {}}}}
ok("progress：读出错的任务（spec.collect 的 errors）同样当成有进度", R.progress(st, "k/1", "site-slurm") and not R.progress(st, "k/1", "hub"))

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
