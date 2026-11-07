#!/usr/bin/env python3
"""测 overview.py：固定配置 + 假的 gather 结果，不连任何机器。

    python3 monitor/test_overview.py
"""
import json
import os
import sys
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue", "testdata"))
import fixture  # noqa: E402
fixture.use()
os.environ["HOME"] = fixture.tmpdir()  # 健康检查的状态、relay 的 status.json（cfg.state）都在临时 HOME 下
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import overview as O  # noqa: E402
import proto  # noqa: E402
import spec as S  # noqa: E402

NOW = datetime(2026, 10, 16, 12, 0)
fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def prog(final=False, epochs=100, end=NOW, total=1000):
    runs = [dict(start=ts(end - timedelta(hours=5)), end=ts(end), epochs_done=epochs, total=total,
                 tl=[[ts(end - timedelta(hours=3 - i / 10)), epochs - 30 + i] for i in range(30)])]
    return dict(final=final, runs=runs, metrics={"a": 0.8} if final else {}, debug={})


keys = [t["key"] for t in S.all_tasks()]
A, B, C = keys[0], keys[1], keys[2]
seg_first = "开始：代码 abc1234，hub"
s = dict(now=NOW, ok={"hub": True, "site-pueue": False}, why={"site-pueue": "连不上"},
         gpus={"hub": [dict(gpu=0, uuid="u", util=90, mem=1000, apps=[])]},
         src={"hub": {"tasks": {A: prog(epochs=200), B: prog(final=True, epochs=1000)},
                      "seglogs": [dict(name=proto.safe(A) + "__77_part1.log", first=seg_first, trained=True),
                                  dict(name=proto.safe(A) + "__77_part1.log", first=seg_first, trained=True),
                                  dict(name=proto.safe(A) + "__78_part2.log", first="", trained=False),
                                  dict(name="other.log", first=seg_first)]}},
         taskq={"hub": dict(enabled=True, tasks=[dict(key=B, state="done", nice=0, assigned=None, note="", worker=None, host=None),
                                                 dict(key=A, state="claimed", nice=0, assigned="site-pueue", note="", worker="w1", host="h"),
                                                 dict(key="extra/key", state="free", nice=5, assigned=None, note="", worker=None, host=None,
                                                      spec=dict(key="extra/key", out="extra/out", recipe="r"))])},
         pueue={"hub": {"groups": {"gpu": {}}, "tasks": [dict(id=1, start="2026-10-16T10:00:00.000000000+08:00", end=None)]},
                "site-pueue": {"error": "ssh"}},
         code={"main": "abc"})
relay = {"relay-site-pueue": dict(name="site-pueue", time=NOW.timestamp(), tasks={A: dict(prog(epochs=300), metrics={})}, seglogs=[])}
o = O.build(s, relay=relay)
line = json.dumps(o, ensure_ascii=False, default=str)
ok("顶层字段 = proto.OVERVIEW_TOP，version 对", tuple(o) == proto.OVERVIEW_TOP and o["version"] == proto.VERSION)
T = {t["key"]: t for t in o["tasks"]}
ok("每个任务的字段 = proto.OVERVIEW_TASK", all(tuple(t) == proto.OVERVIEW_TASK for t in o["tasks"]))
ok("实验表的任务都在，总队列里多出来的也在", set(keys) <= set(T) and "extra/key" in T)
ok("取轮数最多的那份（执行机 300 > store 200）", T[A]["copy"]["where"] == "relay-site-pueue" and T[A]["copy"]["epochs"] == 300
   and T[A]["copy"]["source"] == "site-pueue" and tuple(T[A]["copy"]) == proto.OVERVIEW_COPY)
ok("进度 = 轮数 / total；还在训的有 eta", abs(T[A]["progress"] - 0.3) < 1e-9 and T[A]["total"] == 1000 and T[A]["eta"])
ok("训完的进度 1、没有 eta", T[B]["progress"] == 1.0 and T[B]["eta"] is None and T[B]["copy"]["final"])
ok("哪里都没有的：copy、progress 都是 null", T[C]["copy"] is None and T[C]["progress"] is None)
ok("rank 只数没做完的", T[A]["queue"]["rank"] == 1 and T[B]["queue"]["rank"] is None and T["extra/key"]["queue"]["rank"] == 2)
ok("queue 字段 = proto.OVERVIEW_TASK_QUEUE", tuple(T[A]["queue"]) == proto.OVERVIEW_TASK_QUEUE and T[A]["queue"]["assigned"] == "site-pueue")
ok("分段日志：同一段只算一次，带提交号和机器", [(p["job"], p["part"], p["commit"]) for p in T[A]["parts"]] == [("77", 1, "abc1234"), ("78", 2, None)]
   and all(tuple(p) == proto.OVERVIEW_PART for p in T[A]["parts"]))
ok("commits 只算训了的段", T[A]["commits"] == ["abc1234"])
ok("data：store 和它的数据目录", o["data"] == dict(machine="hub", path="/data/others/DEMO_DATA"))
ok("机器：连不上的带原因，pueue 出错的带 error，任务带时间戳；hub 不是执行机，scheduler 是 null",
   [(m["name"], m["scheduler"]) for m in o["machines"]] == [("hub", None), ("site-pueue", "pueue")]
   and all(tuple(m) == proto.OVERVIEW_MACHINE for m in o["machines"])
   and o["machines"][1]["why"] == "连不上" and o["machines"][1]["pueue"]["error"] == "ssh"
   and isinstance(o["machines"][0]["pueue"]["tasks"][0]["start_ts"], float))
ok("pueue 的任务带 about（plain.label）：没有标签的算其他", o["machines"][0]["pueue"]["tasks"][0]["about"] == ["其他", "", None])
ok("sources：机器在前、执行机在后", [(x["label"], x["kind"]) for x in o["sources"]][-1] == ("relay-site-pueue", "relay"))
ok("没有健康检查记录时 problems 是空的", o["health"] == dict(checked=None, problems=[]))
ok("JSON 一行、能读回", "\n" not in line and json.loads(line)["version"] == proto.VERSION)
ok("给人看的表里有每个任务", all(k in O.text(o) for k in keys))

# 健康检查记下的问题、relay 的 status.json
import health as H  # noqa: E402
H.NT.save_problems(H.NT.open_db(), {"p:1": dict(msg="m", level="normal", kind="k", first=1, active=True, notified=None, data={})}, "x")
os.makedirs(os.path.dirname(O.relay_status_file()), exist_ok=True)
json.dump({"site-pueue": dict(time=NOW.timestamp() - 60, label="L1", tasks={C: prog(epochs=5)}),
           "site-slurm": dict(time=NOW.timestamp() - 3 * 3600, label="L0", tasks={C: prog(epochs=9)})},
          open(O.relay_status_file(), "w"))
o = O.build(s)
T = {t["key"]: t for t in o["tasks"]}
ok("problems 带 id、字段 = proto.OVERVIEW_PROBLEM", [tuple(p) for p in o["health"]["problems"]] == [proto.OVERVIEW_PROBLEM])
ok("problems 带白话 plain（认不出的 kind 用原来的说明）", o["health"]["problems"][0]["plain"] == "m")
ok("relay 的 status.json：2 小时以内的才算", T[C]["copy"]["where"] == "L1" and T[C]["copy"]["epochs"] == 5)
ok("问题出现在给人看的表里", "p:1" in O.text(o))

print("全部通过" if not fails else f"{fails} 项失败")
sys.exit(1 if fails else 0)
