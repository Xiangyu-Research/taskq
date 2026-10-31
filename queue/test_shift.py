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

calls, world = [], dict(group="Paused", who=[], tasks={})
shift.group_tasks = lambda host, g: (world["group"], world["tasks"])
shift.others_on = lambda host, gpu: world["who"]
shift.C.run = lambda cmd, host=None, timeout=0: (calls.append(cmd), (0, "", ""))[1]


def pause_stub(host, g, name):
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
shift.group_tasks = lambda host, g: next(seq)
moves = []
shift.hand_over = lambda key, frm, to: (moves.append((key, frm, to)), key != "D/t/2")[1]
calls.clear()
msg = real_pause("lab", "gpu0", "lab")
assert moves == [("D/t/1", "lab:gpu0", "lab"), ("D/t/2", "lab:gpu0", "lab")], moves
assert any(c.endswith("remove 1") for c in calls) and any(c.endswith("restart --in-place 2 3") for c in calls), calls
assert not any("remove" in c and "4" in c for c in calls), calls
ok("时段结束：没训完的训练交给缓冲、pueue 里删掉；改分失败的和别的任务放回队列；排着的不动")

print("全部通过")
