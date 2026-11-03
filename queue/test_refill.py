#!/usr/bin/env python3
"""测 refill.py（2026-10-18；10/31 起只有 lab 上的数据清理、传数据和 DICC 补作业）：假的队列状态、pueue、训练进度，
命令不真的执行（记下来核对）。

    python3 scripts/queue/test_refill.py
"""
import copy
import json
import os
import re
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, HERE)
os.environ["PHD_TASKS"] = os.path.join(tempfile.mkdtemp(), "tasks.json")  # 不碰真的任务记录
os.environ["PHD_LAST"] = os.path.join(tempfile.mkdtemp(), "last")
import refill as R  # noqa: E402

fails = 0
D = tempfile.mkdtemp()
R.LOG, R.STATE = f"{D}/refill.log", f"{D}/refill.json"
# 测试用固定的配置（testdata/config）
R.P.AUTO = dict(advance=False, lab=False, dicc=False)
R.sh = lambda cmd, host=None, timeout=0: (1, "测试里不连 DICC")  # run_all 里补作业那一步
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


def fields(status):
    """qs 从 status 拆出的 result、code（如 "Failed (137)" → Failed、137）。"""
    m = re.fullmatch(r"(\w+)(?: \((-?\d+)\))?", status)
    done = m[1] not in ("Running", "Queued", "Stashed", "Paused", "Locked")
    return dict(result=m[1] if done else None, code=int(m[2]) if m[2] else 0 if m[1] == "Success" else None)


def task(i, group, status, cmd, label=""):
    return dict(id=i, group=group, status=status, **fields(status), label=label, deps=[], start=None, end=None, command=cmd)


def train(i, ds, f, status="Running", tr=""):
    extra = f" variant={tr}" + (" env=alt2" if tr == UM else "") if tr else ""
    return task(i, "gpu1", status, f"bash scripts/queue/run_task.sh toy ds={DS[ds]} frag={f}{extra}", f"x{i}")


def tq(key, nice, order, state="free", worker=None, skipped_by=()):
    return dict(key=key, nice=nice, order=order, state=state, worker=worker, kind="-", host=worker, skipped_by=list(skipped_by))


K_T14 = f"D121_alphagamma/{NN}/0"   # ① 但数据集 62 GB 放不下
K_T1C = [f"D103_alpha_one/{NN}/{f}" for f in range(3)]
K_UM = f"D101_alpha/{UM}/1"
QUEUE = [tq(K_T14, 0, 1), tq(K_UM, 50, 2), tq(K_T1C[0], 50, 3), tq(K_T1C[1], 50, 4), tq(K_T1C[2], 100, 5)]
SQ = [["demo_one_f0", "485145_1", "PENDING", "0:00", "Priority", "50", "long", "/x/run.sbatch"]]  # DICC 上只有一个排着的作业


def snap(lab_tasks=(), queue=QUEUE, prep=f"{PP}D101_alpha {PP}D111_beta", disk=56e9, box_tasks=(), lab_tasks_st=None, enabled=True, skips=None,
         groups=None, buf=()):
    """buf：lab 缓冲里的任务（None = 读不到）。"""
    tqs = {h: dict(enabled=enabled, tasks=copy.deepcopy(list(queue)), recent_skips=skips or {}) for h in ("box", "dicc")}  # 总队列、DICC 的缓冲
    tqs["lab"] = None if buf is None else dict(enabled=True, buffer=True, tasks=copy.deepcopy(list(buf)), recent_skips={})
    return dict(ok={"box": True, "lab": True, "dicc": True}, slurm={"dicc": dict(squeue=copy.deepcopy(SQ), sacct=[])},
                probe={"lab": {"DISK": str(int(disk)), "HAVE": prep}},
                src={"box": {"tasks": {}}, "lab": {"tasks": lab_tasks_st or {}}, "dicc": {"tasks": {}}},
                taskq=tqs,
                pueue={"box": {"groups": {}, "tasks": list(box_tasks)}, "lab": {"groups": groups or {}, "tasks": list(lab_tasks)}},
                records=[sync_record(t) for t in box_tasks if t["label"].startswith("sync_")])


def sync_record(t):  # box 上传数据的任务的记录（refill.transfer 写的那种）
    to, tag = t["label"][5:].split("_", 1)
    return dict(machine="box", id=t["id"], label=t["label"], step="sync", key=PP + tag, to=to, group=t["group"], created_at=0)


cmds = []


def fake_run(cmd, host=None, timeout=0):
    cmds.append((host or "box", cmd))
    return 0, str(100 + len(cmds))


R.taskq.central = lambda a: (_ for _ in ()).throw(AssertionError("refill.py 不应该改总队列"))
P103 = PP + "D103_alpha_one"
LABD = R.M.MACHINES["lab"]["data"]
DREPO = "cd " + re.escape(R.M.MACHINES["dicc"]["repo"])


def transferred(*paths):
    json.dump({"transferred": {"lab": list(paths)}}, open(R.STATE, "w"))


# ---------------- 传数据（feed.py 给 lab 传数据时用） ----------------
json.dump({}, open(R.STATE, "w"))
R.C.drop_records(lambda r: True)
tids = R.transfer("lab", [P103], run=fake_run)
ok("传数据：box 的 wait 组加 tunnel_push.sh 任务，记进 transferred",
   len(cmds) == 1 and cmds[0][0] == "box" and "sync_lab_D103_alpha_one" in cmds[0][1] and "tunnel_push.sh lab" in cmds[0][1]
   and " wait -p " in cmds[0][1] and json.load(open(R.STATE))["transferred"]["lab"] == [P103] and tids == [101])
sr = [r for r in R.C.load_records() if r["step"] == "sync"]
ok("传数据的任务记进任务记录（step=sync、to、key=路径、任务号）",
   len(sr) == 1 and sr[0]["to"] == "lab" and sr[0]["key"] == P103 and sr[0]["machine"] == "box"
   and sr[0]["label"] == "sync_lab_D103_alpha_one" and sr[0]["id"] == 101)
ok("传数据的任务传完后马上跑一次健康检查（带 PHD_TASK_DONE）",
   f"PHD_TASK_DONE=sync_lab_D103_alpha_one bash {R.KICK} kick" in cmds[0][1])
R.transfer("lab", [P103], run=fake_run)
ok("同一个数据集再传一次：transferred 里不重复", json.load(open(R.STATE))["transferred"]["lab"] == [P103])
SY = task(50, "wait", "Running", "bash scripts/sync/tunnel_push.sh lab …", "sync_lab_D103_alpha_one")
sync, bad = R.syncing(snap(box_tasks=[SY]), "lab")
ok("syncing：还在传的按数据集认出来", list(sync) == ["D103_alpha_one"] and bad == {})
sync, bad = R.syncing(snap(box_tasks=[dict(SY, status="Failed (1)")]), "lab")
ok("syncing：出错停了的单独列出", sync == {} and list(bad) == ["D103_alpha_one"])
R.C.drop_records(lambda r: True)

# ---------------- 清理自动传过去的数据 ----------------
NO103 = [t for t in QUEUE if not t["key"].startswith("D103")]
cmds.clear()
transferred(P103)
busy = snap([train(1, 103, 3, "Running")], queue=NO103)
ok("lab 上还有训练在用：不删", R.cleanup(busy, "lab", run=fake_run) == [] and not cmds)
arch = task(40, "wait", "Queued", "bash scripts/queue/archive_task.sh lab D103_alpha_one/x/0", "a")
ok("box 上还有这台机器的归档任务：不删", R.cleanup(snap(queue=NO103, box_tasks=[arch]), "lab", run=fake_run) == [] and not cmds)
ok("box 上还在往 lab 传这个数据集：不删", R.cleanup(snap(queue=NO103, box_tasks=[SY]), "lab", run=fake_run) == [] and not cmds)
ok("lab 上还有训完没归档的任务：不删",
   R.cleanup(snap(queue=NO103, lab_tasks_st={K_T1C[0]: dict(final=True, archived=False, runs=[])}), "lab", run=fake_run) == [] and not cmds)
ok("缓冲里任务的数据不删", R.cleanup(snap(queue=NO103, buf=[tq(K_T1C[1], 50, 4, "claimed", "lab-gpu1")]), "lab", run=fake_run) == [] and not cmds)
ok("缓冲里训完、还没删掉的记录的数据也不删", R.cleanup(snap(queue=NO103, buf=[tq(K_T1C[1], 50, 4, "done")]), "lab", run=fake_run) == [] and not cmds)
asg = NO103 + [dict(tq(K_T1C[2], 100, 5, "claimed", "lab"), assigned="lab")]
ok("总队列里分给 lab 的数据不删", R.cleanup(snap(queue=asg), "lab", run=fake_run) == [] and not cmds)
ok("缓冲读不到：不删", R.cleanup(snap(queue=NO103, buf=None), "lab", run=fake_run) == [] and not cmds)
ok("总队列读不到：不删", R.cleanup(dict(snap(queue=NO103), taskq={"box": None, "lab": dict(tasks=[])}), "lab", run=fake_run) == [] and not cmds)
s = snap(queue=NO103)
s["ok"]["lab"] = False
ok("lab 连不上：不删", R.cleanup(s, "lab", run=fake_run) == [] and not cmds)
s = snap(queue=NO103)
s["pueue"]["box"] = {"error": "x"}
ok("box 的 pueue 读不到：不删", R.cleanup(s, "lab", run=fake_run) == [] and not cmds)
asg = NO103 + [dict(tq(K_T1C[2], 100, 5, "claimed", "dicc"), assigned="dicc")]
out = R.cleanup(snap(queue=asg), "lab", run=fake_run)
ok("队列里没有 lab 的任务要用了（分给 DICC 的不算）：删掉自动传过去的数据集",
   len(out) == 1 and cmds == [("lab", f"rm -rf {LABD}/{P103}")] and "没有任务要用" in out[0]
   and json.load(open(R.STATE))["transferred"]["lab"] == [])
transferred(f"{PP}D101_alpha")
cmds.clear()
ok("D101（实验表的 KEEP_DATA）不删", R.cleanup(snap(queue=[]), "lab", run=fake_run) == [] and not cmds)
for bad in ["/", "/etc/x", "prepared/", "prepared", "prepared//x", "other/SET-A", "results/D103_x",
            "prepared/../../x"]:
    cmds.clear()
    transferred(bad)
    out = R.cleanup(snap(queue=NO103), "lab", run=fake_run)
    ok(f"记录里的路径不对（{bad!r}）：不删", not cmds and (out == [] or "不删" in out[0]))

# 空间预算：还有任务要用的留着，盘剩的低于软线（硬线 = 提醒线 40 + 10 GB，× 2 = 100 GB）时从最晚用到的删起
K_LATE = "misc/late"
LATE = dict(tq(K_LATE, 200, 9), recipe="command", params={"key": K_LATE, "cmd": "python x.py", "env": "base"},
            spec=dict(key=K_LATE, env="base", data=[PP + "D131_alpha_synth"]))
QA = [dict(tq(K_UM, 50, 2, "claimed", "lab"), assigned="lab"), tq(K_T1C[0], 50, 3), tq(K_T1C[1], 50, 4), tq(K_T1C[2], 100, 5), LATE]
RUN = train(5, 101, 1, "Running", UM)
P131 = PP + "D131_alpha_synth"
P3 = f"{PP}D101_alpha {P131} {P103}"


def snap_a(disk, prep=P3, **kw):
    return snap([RUN], queue=QA, prep=prep, disk=disk, **kw)


transferred(P131)
cmds.clear()
note = R.cleanup(snap_a(150e9), "lab", run=fake_run)
ok("还有任务要用、盘也够：留着", note == [] and not cmds)
transferred(P131, P103)
note = R.cleanup(snap_a(60e9, buf=[tq(K_T1C[0], 50, 3)]), "lab", run=fake_run)
ok("盘低于软线：从最晚用到的删起，缓冲里的任务要用的不删", cmds == [("lab", f"rm -rf {LABD}/{P131}")]
   and json.load(open(R.STATE))["transferred"]["lab"] == [P103] and "低于 100 GB" in note[0])
transferred(P131, P103)
cmds.clear()
note = R.cleanup(snap_a(60e9), "lab", run=fake_run)
ok("盘低于软线、缓冲里没有：最晚用到的（misc/late）先删，还不够再删下一个",
   cmds == [("lab", f"rm -rf {LABD}/{P131}"), ("lab", f"rm -rf {LABD}/{P103}")])
transferred(P131, P103)
cmds.clear()
note = R.cleanup(snap_a(95e9), "lab", run=fake_run)
ok("删一个就回到软线以上：只删最晚用到的", cmds == [("lab", f"rm -rf {LABD}/{P131}")])
transferred(P131)
cmds.clear()
note = R.cleanup(snap_a(60e9), "lab", run=fake_run, dry=True)
ok("dry：只说会删什么，不删", not cmds and "会删" in note[0] and json.load(open(R.STATE))["transferred"]["lab"] == [P131])

cmds.clear()

# ---------------- run_all（health.py 调用） ----------------
transferred(P103)
cmds.clear()
R.sh = fake_run  # cleanup 不传 run 时在调用时才取 R.sh
R.P.AUTO["lab"] = False
r = R.run_all(snap(queue=NO103))
ok("开关（lab）没开：只记'会删什么'，不删、不推送", r == ([], {}, set()) and not cmds and "（开关没开）lab 会：" in open(R.LOG).read()
   and "会删" in open(R.LOG).read())
R.run_all(snap(queue=NO103))
ok("影子日志和上次一样就不再写", open(R.LOG).read().count("开关没开") == 1)
ev, st, filled = R.run_all(snap(queue=NO103), dry=True)
ok("--dry-run：打印会删什么，不删", not cmds and any("会删" in x for x in ev) and st == {} and filled == set())
R.P.AUTO["lab"] = True
ev, st, filled = R.run_all(snap(queue=NO103))
ok("开关开着：删、记日志；不领任务（卡住的、补上的都是空的）", cmds == [("lab", f"rm -rf {LABD}/{P103}")] and "删掉了" in ev[0]
   and st == {} and filled == set() and "删掉了" in open(R.LOG).read())
R.P.AUTO["lab"] = False
R.sh = lambda cmd, host=None, timeout=0: (1, "测试里不连 DICC")
ok("lab 领训练的函数都删了", not any(hasattr(R, f) for f in ("plan", "execute", "start", "ahead", "slots", "PRETAKE_S", "SKIP_BREAK")))

# ---------------- DICC 补作业 ----------------
QOS = {"long": "1-12:00:00", "normal": "08:00:00"}
p, why = R.topup_plan(snap()["taskq"]["box"], {"long": (10, 216, 0), "normal": (10, 138, 0)}, QOS)
ok("现在：旧作业远超上限，不补", p == [] and "排满" in why)
p, _ = R.topup_plan(snap()["taskq"]["box"], {"long": (10, 9, 0), "normal": (10, 3, 0)}, QOS)
ok("long 空 1 个、normal 空 7 个：按名额排满 long 1、normal 7（11/2 起不按备好的个数截）", p == [("long", "1-12:00:00", 1, "worker.sbatch"), ("normal", "08:00:00", 7, "worker.sbatch")])
p, _ = R.topup_plan(snap()["taskq"]["box"], {"long": (None, 4, 0), "normal": (10, 10, 0)}, QOS)
ok("QOS 没设排队上限：按 SUBMIT_CAP", p == [("long", "1-12:00:00", R.SUBMIT_CAP - 4, "worker.sbatch")])
p, _ = R.topup_plan(snap()["taskq"]["box"], {"long": (10, 6, 0)}, {"long": ("3-00:00:00", 2)})
ok("留 2 个名额：上限 10、现有 6，补 2", p == [("long", "3-00:00:00", 2, "worker.sbatch")])
p, why = R.topup_plan(snap()["taskq"]["box"], {"long": (10, 8, 0)}, {"long": ("3-00:00:00", 2)})
ok("留 2 个名额：现有 8 就不补", p == [] and "排满" in why)
done_q = [tq(K_T14, 0, 1, "done"), tq(K_UM, 50, 2, "claimed", "486191")]
ok("没人领的没有、有在训的：照样排满（攒排队时间）", R.topup_plan(snap(queue=done_q)["taskq"]["box"], {"long": (10, 2, 0), "normal": (10, 0, 0)}, QOS)[0]
   == [("long", "1-12:00:00", 8, "worker.sbatch"), ("normal", "08:00:00", 10, "worker.sbatch")])
ok("没人领的、在训的都没有：不补", R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "done")])["taskq"]["box"], {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
ok("领取过期的（stale）也算没人领", R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "stale")])["taskq"]["box"], {"long": (10, 0, 0), "normal": (10, 10, 0)}, QOS)[0] != [])
ok("总开关不在：不补", R.topup_plan(snap(enabled=False)["taskq"]["box"], {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
paused = snap()["taskq"]["box"] | {"paused": True}
ok("队列暂停：不补", R.topup_plan(paused, {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
ok("读不到某个 QOS 的作业数：都不补", R.topup_plan(snap()["taskq"]["box"], {"long": (10, 0, 0)}, QOS)[0] == [])
# 按名额排满（11/2 定，之前排着的不超过备好的任务数）
two = [tq(K_T14, 0, 1), tq(K_UM, 50, 2), tq(K_T1C[0], 50, 3, "claimed", "486191")]
p, _ = R.topup_plan(snap(queue=two)["taskq"]["box"], {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 0)})
ok("备好 2 个、空 10 个位置：交 10 个", p == [("long", "3-00:00:00", 10, "worker.sbatch")])
p, why = R.topup_plan(snap(queue=two)["taskq"]["box"], {"long": (10, 4, 2)}, {"long": ("3-00:00:00", 1)})
ok("留 1 个、已有 4 个（2 个在排）：再交 5 个", p == [("long", "3-00:00:00", 5, "worker.sbatch")])
p, why = R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "done")])["taskq"]["box"], {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 0)})
ok("没有要卡的任务：不交", p == [] and "没有要卡的任务" in why)
ok("slurm.parse：5 段才算，少一段当读不到", R.SL.parse("QOS|long|10|3|1\nQOS|normal|10|2\n")["counts"] == {"long": (10, 3, 1)})
ok("refill 里和 scripts/sites/slurm.py 重复的都删了", not any(hasattr(R, f) for f in ("topup_probe", "parse_qos")))

# 纯 CPU 任务（device=cpu，10/24）：GPU worker 不为它们补，有这类任务时才交 CPU worker
K_EV = "eval/D101_alpha/x/frag_0"
ev_spec = dict(key=K_EV, env="base", machines=["dicc"], device="cpu")
cpu_q = [tq(K_T14, 0, 1, "done"), dict(tq(K_EV, -100, 2), spec=ev_spec), dict(tq(K_EV + "b", -100, 3), spec=dict(ev_spec, key=K_EV + "b"))]
cq = snap(queue=cpu_q)["taskq"]["box"]
p, why = R.topup_plan(cq, {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 2)})
ok("只有纯 CPU 任务没人领：不补 GPU worker", p == [] and "要卡" in why)
ok("纯 CPU 任务 2 个、最多 1 个、现有 0：交 1 个 CPU worker", R.cpu_plan(cq, 0, ("normal", "08:00:00", 1), 5) == ([("normal", "08:00:00", 1, "cpu_worker.sbatch")], ""))
ok("最多 3 个：按任务数交 2 个", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 5)[0] == [("normal", "08:00:00", 2, "cpu_worker.sbatch")])
ok("已有 1 个 CPU worker：不交", R.cpu_plan(cq, 1, ("normal", "08:00:00", 1), 5)[0] == [])
ok("读不到 CPU worker 作业数：不交", R.cpu_plan(cq, None, ("normal", "08:00:00", 1), 5)[0] == [])
ok("没有纯 CPU 任务：不交", R.cpu_plan(snap()["taskq"]["box"], 0, ("normal", "08:00:00", 1), 5)[0] == [])
ok("队列暂停：不交", R.cpu_plan(cq | {"paused": True}, 0, ("normal", "08:00:00", 1), 5)[0] == [])
ok("最多 3 个、只剩 1 个提交名额：交 1 个", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 1)[0] == [("normal", "08:00:00", 1, "cpu_worker.sbatch")])
ok("提交名额用完：不交", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 0) == ([], "提交名额用完了"))
ok("读不到剩几个名额：不交", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), None)[0] == [])

CONF = dict(gpu={"long": ("3-00:00:00", 2)}, cpu=("normal", "08:00:00", 1))
ST0 = dict(counts={"long": (10, 0, 0)}, cpu=0, free=5)
for q_ in (cq, snap()["taskq"]["box"]):
    p, why = R.slurm_plan(q_, ST0, CONF)
    g, gwhy = R.topup_plan(q_, ST0["counts"], CONF["gpu"])
    c, cwhy = R.cpu_plan(q_, 0, CONF["cpu"], 5)
    ok(f"slurm_plan = topup_plan + cpu_plan（{p}）", p == g + c and why == (gwhy if c else f"{gwhy}；CPU：{cwhy}".lstrip("；")))
p, why = R.slurm_plan(cq, ST0, dict(cpu=CONF["cpu"]))
ok("slurm_plan：只有 cpu 名额的机器只交 CPU worker", p == [("normal", "08:00:00", 1, "cpu_worker.sbatch")] and why == "")
p, why = R.slurm_plan(snap()["taskq"]["box"], ST0, dict(cpu=CONF["cpu"]))
ok("slurm_plan：没有纯 CPU 任务时说原因", p == [] and why == "CPU：没有没人领的纯 CPU 任务")

ok("candidates：默认只要要卡的", R.candidates(snap(), "dicc", cq, set()) == [])
ok("candidates：kinds 里有 cpu 才要纯 CPU 任务",
   [t["key"] for t in R.candidates(snap(), "dicc", cq, set(), kinds=("gpu", "cpu"))] == [K_EV, K_EV + "b"])
lq = [dict(tq(K_EV, -100, 2), spec=dict(ev_spec, env=["base", "stats"])),
      dict(tq(K_EV + "b", -100, 3), spec=dict(ev_spec, key=K_EV + "b", env=["base", "没有的环境"]))]
ok("candidates：env 是列表时每个环境都要备好",
   [t["key"] for t in R.candidates(snap(), "dicc", snap(queue=lq)["taskq"]["box"], set(), kinds=("cpu",))] == [K_EV])
mn0 = getattr(R.P, "MAX_NICE", None)
R.P.MAX_NICE = {"dicc": -101}
ok("candidates：档位超过 MAX_NICE 的不领（11/2）", R.candidates(snap(), "dicc", cq, set(), kinds=("gpu", "cpu")) == [])
R.P.MAX_NICE = {"dicc": -100}
ok("candidates：档位等于 MAX_NICE 的照领",
   [t["key"] for t in R.candidates(snap(), "dicc", cq, set(), kinds=("gpu", "cpu"))] == [K_EV, K_EV + "b"])
R.P.MAX_NICE = mn0 if mn0 is not None else {}

R.P.SLURM = {"dicc": dict(gpu={"long": ("3-00:00:00", 2)}, cpu=("normal", "08:00:00", 1))}
sub = []


def topup_run(probe_out, sbatch_rc=0):
    def run(cmd, host=None, timeout=0):
        if cmd.startswith("set -e"):
            return 0, probe_out
        sub.append((host, cmd))
        return sbatch_rc, "487001" if sbatch_rc == 0 else "sbatch: error: QOSMaxSubmitJobPerUserLimit"
    return run


PROBE = "QOS|long|10|7|0\nQOS|normal|10|10|0\n"
R.P.AUTO["dicc"] = False
ev, st = R.topup(snap(), "dicc", run=topup_run(PROBE))
ok("开关（DICC）没开：只记'会交什么'，不交", not sub and ev == [] and "会：交 long 1 段" in open(R.LOG).read())
ev, st = R.topup(snap(), "dicc", dry=True, run=topup_run(PROBE))
ok("--dry-run：打印会交什么，不交", not sub and ev == ["dicc：会 交 long 1 段"])
R.P.AUTO["dicc"] = True
ev, st = R.topup(snap(), "dicc", run=topup_run(PROBE))
ok("开关开着：交 worker.sbatch（long，1 个普通作业，不用数组），记日志、不推送",
   len(sub) == 1 and re.fullmatch(DREPO + r"/scripts/queue && sbatch --parsable -J [a-z]+-[a-z]+ --qos=long --time=3-00:00:00 worker.sbatch", sub[0][1])
   and ev == [] and not st and "交了 1 个 worker 作业 487001" in open(R.LOG).read())
sub.clear()


def topup_seq(probe_out, fail_at=None):
    def run(cmd, host=None, timeout=0):
        if cmd.startswith("set -e"):
            return 0, probe_out
        sub.append((host, cmd))
        if len(sub) == fail_at:
            return 1, "sbatch: error: QOSMaxSubmitJobPerUserLimit"
        return 0, str(487000 + len(sub))
    return run


ev, st = R.topup(snap(), "dicc", run=topup_seq("QOS|long|10|5|0\nQOS|normal|10|10|0\n"))
ok("空出 3 个：交 3 个普通作业，都不带 --array",
   len(sub) == 3 and not any("--array" in c for _, c in sub) and not st
   and "交了 3 个 worker 作业 487001、487002、487003" in open(R.LOG).read())
sub.clear()
ev, st = R.topup(snap(), "dicc", run=topup_seq("QOS|long|10|5|0\nQOS|normal|10|10|0\n", fail_at=2))
ok("第 2 个交不上：停下、报成问题，第 1 个照记",
   len(sub) == 2 and list(st) == ["topup:dicc:long"] and "交了 1 个 worker 作业 487001" in open(R.LOG).read().splitlines()[-1])
sub.clear()
ev, st = R.topup(snap(), "dicc", run=topup_run(PROBE, sbatch_rc=1))
ok("交不上：报成问题", list(st) == ["topup:dicc:long"] and "QOSMaxSubmit" in st["topup:dicc:long"])
sub.clear()
ev, st = R.topup(snap(), "dicc", run=lambda c, host=None, timeout=0: (255, "ssh 不通"))
ok("查作业数时连不上：什么都不做", not sub and ev == [] and not st)
ev, st = R.topup(snap(), "dicc", run=topup_run("QOS|long|10||0\nQOS|normal|10|1|0\n"))
ok("作业数读不出来：不补（不按 0 算）", ev == [] and not st and not any("long" in c for _, c in sub))
sub.clear()
ev, st = R.topup(snap(queue=cpu_q), "dicc", run=topup_run("QOS|long|10|5|0\nCPU|0\nALL|10|6\n"))
ok("开关开着、有纯 CPU 任务：只交 cpu_worker.sbatch（normal、8 小时），记日志",
   len(sub) == 1 and re.fullmatch(DREPO + r"/scripts/queue && sbatch --parsable -J [a-z]+-[a-z]+ --qos=normal --time=08:00:00 cpu_worker.sbatch", sub[0][1])
   and not st and "交了 1 个 CPU worker 作业 487001" in open(R.LOG).read())
sub.clear()
ev, st = R.topup(snap(queue=cpu_q), "dicc", run=topup_run("QOS|long|10|5|0\nCPU|1\nALL|10|7\n"))
ok("已经有 CPU worker 排着：不再交", not sub and not st)
ev, st = R.topup(snap(queue=cpu_q), "dicc", run=topup_run("QOS|long|10|8|0\nCPU|0\nALL|10|10\n"))
ok("所有作业已占满 10 个提交名额：不交 CPU worker、不报错", not sub and not st)
R.P.AUTO["dicc"] = False
probe = R.SL.probe("dicc")
ok("查作业数用 policy.py 的 QOS、机器表的作业名",
   "show qos normal " in probe and "cpu_worker[.]sbatch" in probe and "for q in long;" in probe)


for f in (R.GPU_FILE, R.CPU_FILE):
    ok(f"{f} 不写 --job-name（作业名交的时候随机取，11/2）",
       not any(ln.startswith("#SBATCH --job-name") for ln in open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f))))
ok("refill 的 sbatch 文件名和 slurm.FILES 一致", {"gpu": R.GPU_FILE, "cpu": R.CPU_FILE} == R.SL.FILES)

# 那边的文件读不动（spec.collect 的 stuck，11/1）：当成有进度，不当新任务分出去
st = {"src": {"dicc": {"tasks": {}, "stuck": ["k/1"]}, "box": {"tasks": {}}}}
ok("progress：读不动的任务当成有进度（看所有机器、看那台）；别的机器、别的任务照常",
   R.progress(st, "k/1") and R.progress(st, "k/1", "dicc") and not R.progress(st, "k/1", "box") and not R.progress(st, "k/2"))

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
