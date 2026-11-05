#!/usr/bin/env python3
"""shift.py 的测试：时段的判断；按时段的执行机开始时卡空着才开、结束时交出没训完的任务、每个时段只做一次。"""
import datetime as dt
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
import shift  # noqa: E402

TZ = shift.TZ
real_stop_group = shift.stop_group
real_site_tasks, real_hand_over = shift.site_tasks, shift.hand_over
_d = fixture.tmpdir()
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
shift.M.RELAY = {"z0": dict(ssh="fake-lab", scheduler="pueue", worker=dict(group="g0"), cpu_worker=dict(group="c0"),
                        groups={"g0": (1, "0", {}), "c0": (1, None, {})}),
                 "z1": dict(ssh="fake-lab", scheduler="pueue", worker=dict(group="g1"), groups={"g1": (1, "1", {})})}
shift.P.SHIFT = {"z0": ("18:00", "09:00", "2026-10-31", None)}
shift.P.AUTO = dict(shift.P.AUTO, z0=True, z1=True)
shift.P.OFFLINE = {}
shift.save({})
shift.group_tasks = lambda host, g, *a: (world["group"], {})
world.update(group="Paused", who=[])
calls.clear()
assert shift.limited("z0") and not shift.limited("z1") and shift.day_site("fake-lab", but="z0") == "z1"
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

# 11/3：一台写几段（全天到 11/3 的时段，11/4 起晚班 21:00–09:00）
shift.P.SHIFT = {"z0": [("09:00", "09:00", "2026-11-01", "2026-11-03"), ("21:00", "09:00", "2026-11-04", None)]}
assert shift.rule("z0", D(3, 19)) == ("09:00", "09:00", "2026-11-01", "2026-11-03")
assert shift.rule("z0", D(4, 12)) == ("21:00", "09:00", "2026-11-04", None) and shift.limited("z0")
shift.save({"z0": dict(started="2026-11-03")})
assert shift.allowed("z0", D(3, 19, 30)) and shift.allowed("z0", D(4, 8, 59))
assert not shift.allowed("z0", D(4, 9)) and not shift.allowed("z0", D(4, 20, 59))
stops.clear()
shift.run(now=D(4, 9, 30))
assert stops and shift.load()["z0"].get("paused") == "2026-11-03", shift.load()
calls.clear()
shift.run(now=D(4, 21, 5))
assert shift.load()["z0"].get("started") == "2026-11-04" and shift.allowed("z0", D(4, 21, 10)) and shift.allowed("z0", D(5, 8, 50))
assert not shift.allowed("z0", D(5, 9, 10)) and not shift.allowed("z0", D(5, 20, 50))
ok("几段时段：前一段到期后停，下一段按它的开始时间开")

# 11/3（审查）：暂停失败、组没暂停时不算停下，不交出任务；真跑 stop_group，只替换 C.run 和 group_tasks
shift.stop_group = real_stop_group
moves.clear(), gave.clear()
shift.hand_over = lambda key, frm, to: (moves.append((key, frm, to)), True)[1]
shift.give_back = lambda key, frm: (gave.append((key, frm)), True)[1]
shift.C.run = lambda cmd, host=None, timeout=0: (calls.append(cmd), (255, "", "连接断了") if " pause " in cmd else (0, "", ""))[1]
shift.group_tasks = lambda host, g, *a: ("Running", {7: dict(state="Queued")})
calls.clear()
msg, done = shift.stop_site("z0")
assert not done and not moves and not gave and not any(" kill " in c for c in calls), (msg, done, moves, gave, calls)
shift.C.run = lambda cmd, host=None, timeout=0: (calls.append(cmd), (0, "", ""))[1]
msg, done = shift.stop_site("z0")
assert not done and not moves and not gave, (msg, done, moves, gave)
ok("暂停失败、或暂停说成功但组仍 Running（有排着的 worker）：不算停下，不交出任务")
shift.group_tasks = lambda host, g, *a: ("Paused", {7: dict(state="Done")})
msg, done = shift.stop_site("z0")
assert done and moves == [("D/t/1", "z0", "z1")] and gave == [("D/t/2", "z0")], (msg, done, moves, gave)
ok("暂停成功、组 Paused、没有在跑的：照常交接")

# 变异审查（11/3）：SIGTERM 之后 worker 停了，但组已不是 Paused（被恢复了，排着的 worker 会开始）：不算停下，不交出任务
moves.clear(), gave.clear()
seq = {"g0": [("Paused", {7: dict(state="Running")}), ("Running", {7: dict(state="Done")})]}
shift.group_tasks = lambda host, g, *a: (seq[g].pop(0) if len(seq[g]) > 1 else seq[g][0]) if g in seq else ("Paused", {})
msg, done = shift.stop_site("z0")
assert not done and not moves and not gave, (msg, done, moves, gave)
ok("worker 停下后组变回 Running：不算停下，不交出任务")

# 11/5：交班时现读那边的进度（读不到就不交出）；改分一步做完（take --from）
moves.clear(), gave.clear()
shift.group_tasks = lambda host, g, *a: ("Paused", {7: dict(state="Done")})
shift.site_tasks = lambda name: None
msg, done = shift.stop_site("z0")
assert not done and not moves and not gave, (msg, done, moves, gave)
shift.site_tasks = real_site_tasks
import relay  # noqa: E402
rs0, ld0 = relay.remote_status, relay.ST.load_state
relay.remote_status = lambda name: dict(taskq=dict(tasks=[dict(key="D/t/1", state="free"), dict(key="D/t/2", state="free")]),
                                        tasks={"D/t/1": dict(runs=[], resumable=True), "D/t/2": dict(runs=[])})
relay.ST.load_state = lambda: dict(keys={"z0": {"D/t/1": {}, "D/t/2": {}}})
try:
    msg, done = shift.stop_site("z0")
finally:
    relay.remote_status, relay.ST.load_state = rs0, ld0
assert done and moves == [("D/t/1", "z0", "z1")] and gave == [("D/t/2", "z0")], (msg, done, moves, gave)
ok("交班时现读那边的进度：读不到不交出；只有存档（resumable）也算有进度")
import taskq  # noqa: E402
cen, c0 = [], taskq.central
taskq.central = lambda args, timeout=420: (cen.append(list(args)), (0, "", ""))[1]
try:
    assert real_hand_over("D/t/1", "z0", "z1") and cen == [["take", "--worker", "z1", "--assign", "--from", "z0", "D/t/1"]], cen
finally:
    taskq.central = c0
ok("交班改分只调一次 take --from（中间不交还，别的执行机领不走）")

shutil.rmtree(_d, ignore_errors=True)
print("全部通过")
