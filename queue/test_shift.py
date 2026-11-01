#!/usr/bin/env python3
"""shift.py 的测试：时段的判断；按时段的执行机开始时卡空着才开、结束时交出没训完的任务、每个时段只做一次。"""
import datetime as dt
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import shift  # noqa: E402

TZ = shift.TZ
_d = tempfile.mkdtemp()
shift.STATE, shift.LOG = os.path.join(_d, "shift.json"), os.path.join(_d, "refill.log")


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
shift.others_on = lambda host, gpu: (world.setdefault("cards", set()).add(gpu), world["who"])[1]
shift.C.run = lambda cmd, host=None, timeout=0: (calls.append(cmd), (0, "", ""))[1]


# 执行机按时段（SHIFT 写 "<执行机>": (开始, 结束, 第一个, 最后一个)，lab0）：时段外不领任务；开始时卡空着才开；结束时交出没训完的折
shift.M.RELAY = {"z0": dict(ssh="lab", scheduler="pueue", worker=dict(group="g0"), cpu_worker=dict(group="c0"),
                        groups={"g0": (1, "0", {}), "c0": (1, None, {})}),
                 "z1": dict(ssh="lab", scheduler="pueue", worker=dict(group="g1"), groups={"g1": (1, "1", {})})}
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
assert any("start -g g0" in c for c in calls) and any("start -g c0" in c for c in calls) and shift.allowed("z0", T(31, "18:40")), calls
assert world["cards"] == {"0"}, world["cards"]
calls.clear()
shift.run(now=T(31, "23:00"))
assert not calls
ok("执行机时段开始：卡上有别人不开；空了恢复各种 worker 的组（GPU、CPU；只看 GPU 组的卡）、allowed() 放行；只开一次")

stops = []
shift.stop_group = lambda host, g, pueue, wait=False: (stops.append((g, wait)), ([7], {}))[1]
shift.site_tasks = lambda name: {"D/t/1": dict(state="claimed", runs=[dict(epochs_done=40)]), "D/t/2": dict(state="free", runs=[])}
shift.central_mine = lambda name: ["D/t/1", "D/t/2"]
moves, gave = [], []
shift.hand_over = lambda key, frm, to: (moves.append((key, frm, to)), True)[1]
shift.give_back = lambda key, frm: (gave.append((key, frm)), True)[1]
msgs = shift.run(now=dt.datetime(2026, 11, 1, 9, 0, tzinfo=TZ))
assert stops == [("g0", True), ("c0", True)] and moves == [("D/t/1", "z0", "z1")] and gave == [("D/t/2", "z0")], (stops, moves, gave, msgs)
assert not shift.allowed("z0", dt.datetime(2026, 11, 1, 9, 5, tzinfo=TZ))
stops.clear()
shift.run(now=dt.datetime(2026, 11, 1, 9, 30, tzinfo=TZ))
assert not stops
ok("执行机时段结束：停各种 worker（等它交还），有进度的折改分给同一台上不按时段的执行机（从存档接着训），其余交还；只做一次")

shift.P.OFFLINE = {"z0": "停机"}
calls.clear()
shift.run(now=dt.datetime(2026, 11, 1, 18, 30, tzinfo=TZ))
assert not calls
shift.P.OFFLINE = {}
shift.P.AUTO = dict(shift.P.AUTO, z0=False)
shift.run(now=dt.datetime(2026, 11, 1, 18, 30, tzinfo=TZ))
assert not calls
ok("执行机暂停检查（OFFLINE）或 AUTO 关着：时段开始也不动")

# 11/1（审查）：漏了时段结束那次检查也会停；停到一半失败的不记 paused，下次再停
shift.P.AUTO = dict(shift.P.AUTO, z0=True)
shift.P.SHIFT = {"z0": ("18:00", "09:00", "2026-11-01", "2026-11-03")}
D = lambda d, h, m=0: dt.datetime(2026, 11, d, h, m, tzinfo=TZ)  # noqa: E731
for started, now, why in [("2026-11-03", D(5, 10), "11/3 开过、11/5 才恢复检查（中间漏了几次）"),
                          ("2026-11-03", D(4, 20), "超出最后一个时段后恢复检查"),
                          ("2026-11-02", D(3, 20), "在新时段里恢复检查、卡上有别人")]:
    shift.save({"z0": dict(started=started)})
    world["who"] = ["jun"]
    stops.clear()
    shift.run(now=now)
    assert stops and shift.load()["z0"].get("paused") == started, (why, stops, shift.load())
world["who"] = []
ok("漏了时段结束那次检查：之后哪次检查发现不该开就停（不要求刚好是结束后那次）")

shift.save({"z0": dict(started="2026-11-03")})
shift.stop_group = lambda host, g, pueue, wait=False: (stops.append((g, wait)), ([7], {7: dict(state="Running")}))[1]
stops.clear()
shift.run(now=D(4, 9, 30))
assert stops and not shift.load()["z0"].get("paused"), shift.load()
shift.stop_group = lambda host, g, pueue, wait=False: (stops.append((g, wait)), ([], None))[1]
shift.run(now=D(4, 10))
assert not shift.load()["z0"].get("paused"), shift.load()
shift.stop_group = lambda host, g, pueue, wait=False: (stops.append((g, wait)), ([7], {7: dict(state="Done")}))[1]
shift.give_back = lambda key, frm: False
moves.clear()
shift.run(now=D(4, 10, 30))
assert not shift.load()["z0"].get("paused"), shift.load()
shift.give_back = lambda key, frm: True
stops.clear()
shift.run(now=D(4, 11))
assert stops and shift.load()["z0"].get("paused") == "2026-11-03", shift.load()
stops.clear()
shift.run(now=D(4, 11, 30))
assert not stops
ok("worker 没确认停下、读不到 pueue、任务没交出去：不记 paused，下次检查再停；都做完后只停一次")

print("全部通过")
