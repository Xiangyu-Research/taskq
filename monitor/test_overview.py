#!/usr/bin/env python3
"""Test overview.py: fixed config + a fake gather result, no machines."""
import json
import os
import sys
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue", "testdata"))
import fixture  # noqa: E402
fixture.use()
os.environ["HOME"] = fixture.tmpdir()  # health state and relay status.json (cfg.state) live under the temp HOME
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import overview as O  # noqa: E402
import proto  # noqa: E402
import spec as S  # noqa: E402

NOW = datetime(2026, 10, 16, 12, 0)
fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def prog(final=False, epochs=100, end=NOW, total=1000):
    runs = [dict(start=ts(end - timedelta(hours=5)), end=ts(end), epochs_done=epochs, total=total,
                 tl=[[ts(end - timedelta(hours=3 - i / 10)), epochs - 30 + i] for i in range(30)])]
    return dict(final=final, runs=runs, metrics={"a": 0.8} if final else {}, debug={})


keys = [t["key"] for t in S.all_tasks()]
A, B, C = keys[0], keys[1], keys[2]
seg_first = "job 77_1 @ hub, task=x, part 1, env base, code abc1234, 2026-11-01 10:00:00"
s = dict(now=NOW, ok={"hub": True, "site-pueue": False}, why={"site-pueue": "unreachable"},
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
ok("top-level fields = proto.OVERVIEW_TOP, version matches", tuple(o) == proto.OVERVIEW_TOP and o["version"] == proto.VERSION)
T = {t["key"]: t for t in o["tasks"]}
ok("each task has the fields in proto.OVERVIEW_TASK", all(tuple(t) == proto.OVERVIEW_TASK for t in o["tasks"]))
ok("all experiment-table tasks are there, plus ones only in the hub queue", set(keys) <= set(T) and "extra/key" in T)
ok("the copy with the most rounds wins (executor 300 > store 200)", T[A]["copy"]["where"] == "relay-site-pueue" and T[A]["copy"]["epochs"] == 300
   and T[A]["copy"]["source"] == "site-pueue" and tuple(T[A]["copy"]) == proto.OVERVIEW_COPY)
ok("progress = rounds / total; a running task has eta", abs(T[A]["progress"] - 0.3) < 1e-9 and T[A]["total"] == 1000 and T[A]["eta"])
ok("a finished task has progress 1 and no eta", T[B]["progress"] == 1.0 and T[B]["eta"] is None and T[B]["copy"]["final"])
ok("a task found nowhere has copy and progress null", T[C]["copy"] is None and T[C]["progress"] is None)
ok("rank counts only unfinished tasks", T[A]["queue"]["rank"] == 1 and T[B]["queue"]["rank"] is None and T["extra/key"]["queue"]["rank"] == 2)
ok("queue fields = proto.OVERVIEW_TASK_QUEUE", tuple(T[A]["queue"]) == proto.OVERVIEW_TASK_QUEUE and T[A]["queue"]["assigned"] == "site-pueue")
ok("part logs: the same part counted once, with commit and machine", [(p["job"], p["part"], p["commit"]) for p in T[A]["parts"]] == [("77", 1, "abc1234"), ("78", 2, None)]
   and all(tuple(p) == proto.OVERVIEW_PART for p in T[A]["parts"]))
ok("commits only from parts that trained", T[A]["commits"] == ["abc1234"])
ok("data: the store and its data dir", o["data"] == dict(machine="hub", path="/data/others/DEMO_DATA"))
ok("machines: unreachable ones carry why, pueue errors carry error, tasks have timestamps; hub is not an executor and scheduler is null",
   [(m["name"], m["scheduler"]) for m in o["machines"]] == [("hub", None), ("site-pueue", "pueue")]
   and all(tuple(m) == proto.OVERVIEW_MACHINE for m in o["machines"])
   and o["machines"][1]["why"] == "unreachable" and o["machines"][1]["pueue"]["error"] == "ssh"
   and isinstance(o["machines"][0]["pueue"]["tasks"][0]["start_ts"], float))
ok("pueue tasks carry about (plain.label): an empty label is Other", o["machines"][0]["pueue"]["tasks"][0]["about"] == ["Other", "", None])
ok("sources: machines first, executors after", [(x["label"], x["kind"]) for x in o["sources"]][-1] == ("relay-site-pueue", "relay"))
ok("problems are empty when there is no health record", o["health"] == dict(checked=None, problems=[]))
ok("JSON is one line and reads back", "\n" not in line and json.loads(line)["version"] == proto.VERSION)
ok("the text table has every task", all(k in O.text(o) for k in keys))

# problems recorded by the health check, relay's status.json
import health as H  # noqa: E402
H.NT.save_problems(H.NT.open_db(), {"p:1": dict(msg="m", level="normal", kind="k", first=1, active=True, notified=None, data={})}, "x")
os.makedirs(os.path.dirname(O.relay_status_file()), exist_ok=True)
json.dump({"site-pueue": dict(time=NOW.timestamp() - 60, label="L1", tasks={C: prog(epochs=5)}),
           "site-slurm": dict(time=NOW.timestamp() - 3 * 3600, label="L0", tasks={C: prog(epochs=9)})},
          open(O.relay_status_file(), "w"))
o = O.build(s)
T = {t["key"]: t for t in o["tasks"]}
ok("problems carry id and the fields in proto.OVERVIEW_PROBLEM", [tuple(p) for p in o["health"]["problems"]] == [proto.OVERVIEW_PROBLEM])
ok("problems carry plain wording (unknown kind falls back to msg)", o["health"]["problems"][0]["plain"] == "m")
ok("relay status.json: only entries within 2 hours count", T[C]["copy"]["where"] == "L1" and T[C]["copy"]["epochs"] == 5)
ok("the problem appears in the text table", "p:1" in O.text(o))

print("All passed" if not fails else f"{fails} failed")
sys.exit(1 if fails else 0)
