#!/usr/bin/env python3
"""common.py 的测试（2026-10-25）：任务记录的读写、找回任务号、清理；pueue 的时间。临时文件，不连机器。
    python3 scripts/queue/test_common.py
"""
import json
import os
import sys
import tempfile
import threading

T = tempfile.mkdtemp()
os.environ["PHD_TASKS"] = os.path.join(T, "tasks.json")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def task(i, label, created="2026-10-25T10:00:05.123456+08:00"):
    return dict(id=i, label=label, status="Running", command="x", created_at=created)


ADDED = C.pueue_time("2026-10-25T10:00:00+08:00")

# pueue 的时间
ok("pueue 时间：没有小数 / 3 位 / 6 位 / 9 位都认",
   C.pueue_time("2026-10-25T10:00:00+08:00") == ADDED
   and abs(C.pueue_time("2026-10-25T10:00:00.123+08:00") - ADDED - 0.123) < 1e-6
   and abs(C.pueue_time("2026-10-25T10:00:00.123456+08:00") - ADDED - 0.123456) < 1e-6
   and abs(C.pueue_time("2026-10-25T10:00:00.123456789+08:00") - ADDED - 0.123456) < 1e-6)
ok("pueue 时间：没有 / 认不出 → None", C.pueue_time(None) is None and C.pueue_time("") is None and C.pueue_time("昨天") is None)

# 找回任务号
r0 = dict(machine="box", id=None, label="eval:t3_f1", added=ADDED, step="eval", key="k1")
pq = {"box": dict(tasks=[task(5, "eval:t3_f1", "2026-10-25T09:00:00+08:00"), task(6, "eval:t3_f1")])}
got = C.sync_records(pq, [r0], now=ADDED + 10)
ok("没有任务号：按同一台机器、同标签、创建时间不早于记录（减时钟差）找回，取最早的那个",
   [r["id"] for r in got] == [6])
pq2 = {"box": dict(tasks=[task(6, "eval:t3_f1"), task(7, "eval:t3_f1")])}
got = C.sync_records(pq2, [dict(r0, id=6), r0], now=ADDED + 10)
ok("已经被别的记录占着的任务号不再分给它", sorted(r["id"] for r in got) == [6, 7])
got = C.sync_records({"box": dict(tasks=[])}, [r0], now=ADDED + 10)
ok("没有任务号、还没找到：一小时内留着", got == [r0])
got = C.sync_records({"box": dict(tasks=[])}, [r0], now=ADDED + C.LOST + 1)
ok("没有任务号、一小时后还没找到：作废", got == [])
got = C.sync_records({"lab": dict(tasks=[])}, [dict(r0, machine="box")], now=ADDED + C.LOST + 1)
ok("那台机器这次没读（快照里没有）：记录不动", len(got) == 1)

# 任务不在了
r1 = dict(machine="box", id=8, label="oof:t3", added=ADDED, step="oof", key="t3")
ok("任务还在（任务号、标签都对上）：留着", C.sync_records({"box": dict(tasks=[task(8, "oof:t3")])}, [r1]) == [r1])
ok("任务删掉了：记录删掉", C.sync_records({"box": dict(tasks=[])}, [r1]) == [])
ok("任务号被别的任务重用（标签不同）：记录删掉", C.sync_records({"box": dict(tasks=[task(8, "other")])}, [r1]) == [])
ok("lab 读不到（error）：lab 的记录不删",
   C.sync_records({"lab": {"error": "ssh"}}, [dict(r1, machine="lab")]) == [dict(r1, machine="lab")])
r2 = dict(r1, machine="lab", label="infer:t1c_f3", out="predictions/x/fold_3")
ok("训练机上做的步骤（有 out）：那边的任务删了也留着，等清理完", C.sync_records({"lab": dict(tasks=[])}, [r2]) == [r2])

# 写文件
C.write_json(C.tasks_path(), [])
a = C.new_record("lab", "infer:t1c_f3", "infer", "k3", out="predictions/x/fold_3", group="gpu1")
C.set_record_id(a, 30)
b = C.new_record("lab", "infer:t1c_f3", "infer", "k3", out="predictions/x/fold_3", group="gpu1")
recs = C.load_records()
ok("新记录先没有任务号，set_record_id 补上；同一台机器同标签的旧的训练机步骤作废",
   len(recs) == 1 and recs[0]["id"] is None and recs[0]["added"] == b["added"])
C.new_record("box", "eval:t3_f1", "eval", "k1", src=None)
ok("值为 None 的字段不写", "src" not in C.load_records()[-1])
C.drop_records(lambda r: r["machine"] == "lab")
ok("drop_records：按条件删", [r["machine"] for r in C.load_records()] == ["box"])
open(C.tasks_path(), "w").write('[{"machine": "box", "id": 3')
ok("文件只写了一半（读不出）：当成没有记录，不报错", C.load_records() == [])
open(C.tasks_path(), "w").write('{"a": 1}')
ok("文件不是列表：当成没有记录", C.load_records() == [])


def many(n):
    for i in range(n):
        C.new_record("box", f"x{threading.get_ident()}_{i}", "eval", f"k{i}")


C.write_json(C.tasks_path(), [])
ths = [threading.Thread(target=many, args=(20,)) for _ in range(4)]
[t.start() for t in ths]
[t.join() for t in ths]
ok("几个进程同时加：加锁读改写，一条不丢", len(C.load_records()) == 80 and isinstance(json.load(open(C.tasks_path())), list))

print("\n全部通过" if not fails else f"\n{fails} 项失败")
sys.exit(1 if fails else 0)
