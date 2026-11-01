#!/usr/bin/env python3
"""shift.py 的测试：时段的判断、每个时段只恢复、暂停一次、有人在用不恢复、手动开的不暂停。"""
import datetime as dt
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import shift  # noqa: E402

TZ = shift.TZ
_d = tempfile.mkdtemp()
shift.STATE, shift.LOG = os.path.join(_d, "shift.json"), os.path.join(_d, "refill.log")
shift.P.SHIFT = {"lab": {"gpu0": ("18:00", "09:00")}}


def T(day, hm):
    h, m = map(int, hm.split(":"))
    return dt.datetime(2026, 10, day, h, m, tzinfo=TZ)


def ok(msg):
    print("通过：" + msg)


assert shift.window(T(30, "18:05"), "18:00", "09:00") == ("2026-10-30", None)
assert shift.window(T(31, "08:59"), "18:00", "09:00") == ("2026-10-30", None)
assert shift.window(T(31, "09:00"), "18:00", "09:00") == (None, "2026-10-30")
assert shift.window(T(31, "17:59"), "18:00", "09:00") == (None, "2026-10-30")
assert shift.window(T(31, "12:00"), "08:00", "11:00") == (None, "2026-10-31")
assert shift.window(T(31, "09:00"), "08:00", "11:00") == ("2026-10-31", None)
ok("时段：跨夜和不跨夜")
assert shift.dated(T(30, "10:00"), "09:00", "09:00", "2026-10-30", "2026-10-31") == ("2026-10-30", None)
assert shift.dated(T(31, "09:00"), "09:00", "09:00", "2026-10-30", "2026-10-31") == ("2026-10-31", "2026-10-30")
assert shift.dated(T(31, "12:00"), "09:00", "09:00", "2026-10-30", "2026-10-30") == (None, "2026-10-30")
ok("全天（开始 = 结束）：时段首尾相接，最后一个结束后算刚结束")

calls, world = [], dict(group="Paused", who=[], tasks={})
shift.group_tasks = lambda host, g, *a: (world["group"], world["tasks"])
shift.others_on = lambda host, gpu: world["who"]
shift.C.run = lambda cmd, host=None, timeout=0: (calls.append(cmd), (0, "", ""))[1]


def pause_stub(host, g, name, last=False):
    calls.append("LAST") if last else None
    calls.append("PAUSE")
    world["group"] = "Paused"
    return "停了"


real_pause = shift.pause
shift.pause = pause_stub

world["who"] = ["jun"]
shift.run(now=T(30, "18:05"))
assert not calls
world["who"] = []
shift.run(now=T(30, "18:35"))
assert any("start -g gpu0" in c for c in calls), calls
world["group"] = "Running"
calls.clear()
shift.run(now=T(30, "23:05"))
assert not calls
ok("时段开始：有人在用不恢复，空了再恢复，只恢复一次")

world["group"] = "Paused"  # 夜里手动暂停
shift.run(now=T(31, "02:05"))
assert not calls
world["group"] = "Running"
shift.run(now=T(31, "09:00"))
assert calls == ["PAUSE"], calls
calls.clear()
world["group"] = "Running"  # 白天手动恢复
shift.run(now=T(31, "09:35"))
shift.run(now=T(31, "15:05"))
assert not calls
ok("时段结束暂停一次；中间、之后手动的开关不管")

world["group"] = "Running"  # 时段开始时已经手动开着 → 结束时也不暂停
shift.run(now=T(31, "18:05"))
shift.run(now=dt.datetime(2026, 11, 1, 9, 5, tzinfo=TZ))
assert not calls, calls
ok("时段开始时已经手动开着：不动，结束时也不暂停")

# 真的 pause()：在训的训练交给缓冲（总队列改分、pueue 里删），别的放回组的队列；改分失败的也放回
seq = iter([("Running", {1: dict(state="Running", status="Running", label="train:D/t/1", priority=0),
                         2: dict(state="Running", status="Running", label="train:D/t/2", priority=0),
                         3: dict(state="Running", status="Running", label="p1:x", priority=0),
                         4: dict(state="Queued", status="Queued", label="train:D/t/3", priority=0)})]
           + [("Paused", {1: dict(state="Done", status={"Done": {"result": {"Failed": 143}}}, label="train:D/t/1", priority=0),
                          2: dict(state="Done", status={"Done": {"result": {"Failed": 143}}}, label="train:D/t/2", priority=0),
                          3: dict(state="Done", status={"Done": {"result": {"Failed": 143}}}, label="p1:x", priority=0),
                          4: dict(state="Queued", status="Queued", label="train:D/t/3", priority=0)})] * 3)
shift.group_tasks = lambda host, g, *a: next(seq)
moves = []
shift.hand_over = lambda key, frm, to: (moves.append((key, frm, to)), key != "D/t/2")[1]
calls.clear()
shift.M.RELAY = {"r1": dict(ssh="lab")}
shift.P.AUTO = dict(shift.P.AUTO, r1=True)
msg = real_pause("lab", "gpu0", "lab")
assert moves == [("D/t/1", "lab:gpu0", "r1"), ("D/t/2", "lab:gpu0", "r1")], moves  # 同一台上开着的 RELAY 执行机
assert any(c.endswith("remove 1") for c in calls) and any(c.endswith("restart --in-place 2 3") for c in calls), calls
assert not any("remove" in c and "4" in c for c in calls), calls
ok("时段结束：没训完的训练交给缓冲、pueue 里删掉；改分失败的和别的任务放回队列；排着的不动")

# 最后一个时段（SHIFT 的第 4 项）结束：排着的训练也交还总队列、从组里删掉
seq = iter([("Running", {4: dict(state="Queued", status="Queued", label="train:D/t/3", priority=0),
                         5: dict(state="Queued", status="Queued", label="p1:y", priority=0)})] * 3)
shift.group_tasks = lambda host, g, *a: next(seq)
gave = []
shift.give_back = lambda key, frm: (gave.append((key, frm)), True)[1]
calls.clear()
msg = real_pause("lab", "gpu0", "lab", last=True)
assert gave == [("D/t/3", "lab:gpu0")] and any(c.endswith("remove 4") for c in calls), (gave, calls)
ok("最后一个时段结束：排着的训练交还总队列、从组里删掉，别的任务留着")

# 执行机按时段（SHIFT 写 "<执行机>": (开始, 结束, 第一个, 最后一个)，lab0）：时段外不领任务；开始时卡空着才开；结束时交出没训完的折
shift.M.RELAY = {"z0": dict(ssh="lab", worker=dict(group="g0"), groups={"g0": (1, "0", {})}),
                 "z1": dict(ssh="lab", worker=dict(group="g1"), groups={"g1": (1, "1", {})})}
shift.P.SHIFT = {"z0": ("18:00", "09:00", "2026-10-31", None)}
shift.P.AUTO = dict(shift.P.AUTO, z0=True, z1=True)
shift.P.OFFLINE = {}
shift.save({})
shift.group_tasks = lambda host, g, *a: (world["group"], {})
world.update(group="Paused", who=[])
calls.clear()
assert shift.limited("z0") and not shift.limited("z1") and shift.day_site("lab", but="z0") == "z1"
assert not shift.allowed("z0", T(31, "12:00")) and shift.allowed("z1", T(31, "12:00"))
shift.run(now=T(30, "19:00"))
assert not calls and not shift.allowed("z0", T(30, "19:00"))
ok("执行机按时段：第一个时段之前不开；时段外 allowed() 不让领任务；不按时段的照常")
world["who"] = ["jun"]
shift.run(now=T(31, "18:05"))
assert not calls and not shift.allowed("z0", T(31, "18:05"))
world["who"] = []
shift.run(now=T(31, "18:35"))
assert any("start -g g0" in c for c in calls) and shift.allowed("z0", T(31, "18:40")), calls
calls.clear()
shift.run(now=T(31, "23:00"))
assert not calls
ok("执行机时段开始：卡上有别人不开；空了恢复 worker 组、allowed() 放行；只开一次")

stops = []
shift.stop_group = lambda host, g, pueue, wait=False: (stops.append((g, wait)), ([7], {}))[1]
shift.site_folds = lambda name: {"D/t/1": dict(state="claimed", runs=[dict(epochs_done=40)]), "D/t/2": dict(state="free", runs=[])}
shift.central_mine = lambda name: ["D/t/1", "D/t/2"]
moves, gave = [], []
shift.hand_over = lambda key, frm, to: (moves.append((key, frm, to)), True)[1]
msgs = shift.run(now=dt.datetime(2026, 11, 1, 9, 0, tzinfo=TZ))
assert stops == [("g0", True)] and moves == [("D/t/1", "z0", "z1")] and gave == [("D/t/2", "z0")], (stops, moves, gave, msgs)
assert not shift.allowed("z0", dt.datetime(2026, 11, 1, 9, 5, tzinfo=TZ))
stops.clear()
shift.run(now=dt.datetime(2026, 11, 1, 9, 30, tzinfo=TZ))
assert not stops
ok("执行机时段结束：停 worker（等它交还），有进度的折改分给同一台上不按时段的执行机（从存档接着训），其余交还；只做一次")

shift.P.OFFLINE = {"z0": "停机"}
calls.clear()
shift.run(now=dt.datetime(2026, 11, 1, 18, 30, tzinfo=TZ))
assert not calls
shift.P.OFFLINE = {}
shift.P.AUTO = dict(shift.P.AUTO, z0=False)
shift.run(now=dt.datetime(2026, 11, 1, 18, 30, tzinfo=TZ))
assert not calls
ok("执行机暂停检查（OFFLINE）或 AUTO 关着：时段开始也不动")

print("全部通过")
