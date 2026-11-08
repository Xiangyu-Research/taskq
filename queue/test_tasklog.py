#!/usr/bin/env python3
"""测 tasklog.py（taskq log）：假的数据目录里几段分段日志、事件日志（测试用的数据库）和任务记录。

    python3 queue/test_tasklog.py
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
T = fixture.tmpdir()
for n in ("relay", "feed", "advance", "health"):
    os.environ[n.upper() + "_LOG"] = os.path.join(T, n + ".log")
os.environ["TASKQ_TASKS"] = os.path.join(T, "tasks.json")
os.environ.pop("TASKQ_DATA", None)
sys.path.insert(0, HERE)
import proto  # noqa: E402
import tasklog as L  # noqa: E402

fails = 0


def ok(what, cond, got=None):
    global fails
    print(("PASS:" if cond else "FAIL:") + what + ("" if cond or got is None else f"（得到 {got!r}）"))
    fails += 0 if cond else 1


KEY = "demo/a/0"
D = os.path.join(T, "data")
LD = os.path.join(D, proto.TASK_LOGS)
os.makedirs(LD)


def head(job, part, key=KEY, t="2026-11-01 10:00:00", env="base", code="abc1234"):
    return f"作业 {job}_{part} @ node1，任务={key}，第 {part} 段，环境 {env}，代码 {code}，{t}\n"


def write(name, text, age=3600):
    p = os.path.join(LD, name)
    with open(p, "w") as f:
        f.write(text)
    t = time.time() - age
    os.utime(p, (t, t))
    return p


S = proto.safe(KEY)
write(f"{S}__1001_part1.log", head(1001, 1) + "2026-11-01 10:00:05 开始\nEpoch 3\n"
      f"{proto.STALL_LINE} 2026-11-01 12:00:00：日志和结果文件夹 30 分钟没有变化，作业 1001 停掉它\n")
write(f"{S}__1002_part1.log", head(1002, 1, t="2026-11-02 08:00:00") + "2026-11-02 09:00:00 一些输出\n")
write(f"{S}__gpu-host-202611030900_part2.log", head("gpu-host-202611030900", 2, t="2026-11-03 09:00:00")
      + "2026-11-03 09:10:00 命令结束（退出码 1），没训完（第 1 次）\n2026-11-03 09:15:00 命令结束（退出码 0），没训完（第 2 次）\n")
write(f"{S}__1004_part1.log", head(1004, 1, t="2026-11-04 08:00:00", env="未知") + "2026-11-04 08:00:00 没开训：这台机器没有环境 x\n")
write(f"{S}__1005_part1.log", head(1005, 1, t="2026-11-05 07:00:00") + f"{proto.DONE_LINE} 2026-11-05 09:30:00\n")
write(f"{S}__1006_part1.log", head(1006, 1, t="2026-11-05 10:00:00") + "2026-11-05 10:01:00 Epoch 0\n", age=10)
write(f"{S}__1007_part1.log", head(1007, 1, t="2026-11-05 11:00:00")
      + "2026-11-05 11:30:00 卡 1 被别人占着，等了 30 分钟还没空出来，不跑\n")
write(f"{S}__1008_part1.log", head(1008, 1, t="2026-11-05 12:00:00") + "2026-11-05 12:05:00 命令结束（退出码 1），读不了盘，判断不了训完没有\n")
# 不算：key 更长的别的任务、开头行写的是别的 key、别的任务
write(f"{S}__x__2001_part1.log", head(2001, 1, key=KEY + "/x"))
write(f"{S}__2002_part1.log", head(2002, 1, key="demo/zzz"))
write(f"{proto.safe('demo/b/0')}__2003_part1.log", head(2003, 1, key="demo/b/0"))

import db as DB  # noqa: E402
import events as EV  # noqa: E402
DB.connect(create=True).close()
_t = lambda *x: time.mktime(x + (0, 0, -1))  # noqa: E731
EV.add("relay", f"site-pueue 领了 {KEY}（gpu1 组）", t=_t(2026, 11, 1, 9, 0, 0))  # 没有 key 的按文字认
EV.add("relay", f"site-pueue 领了 {KEY}0（不算）", t=_t(2026, 11, 1, 9, 0, 1))
EV.add("relay", f"site-pueue 领了 {KEY}/x（不算）", t=_t(2026, 11, 1, 9, 0, 2))
EV.add("advance", f"排了 fetch：{KEY}", key=KEY, t=_t(2026, 10, 31, 8, 0, 0))
EV.add("note", "looked at it", key=KEY, kind="fix", t=_t(2026, 11, 2, 9, 0, 0))
EV.add("note", "other task", key=KEY + "/x")
with open(os.environ["TASKQ_TASKS"], "w") as f:
    json.dump([dict(machine="hub", id=5, label="fetch:a0", added=1762300000.0, step="fetch", key=KEY, src="site-pueue"),
               dict(machine="hub", id=6, label="fetch:b0", added=1762300001.0, step="fetch", key="demo/b/0")], f)

r = L.collect(KEY, D)
A = r["attempts"]
ok("只认这个任务的段（8 段，按开始时刻排）", [a["job"] for a in A] ==
   ["1001", "1002", "gpu-host-202611030900", "1004", "1005", "1006", "1007", "1008"], [a["job"] for a in A])
st = {a["job"]: a["state"] for a in A}
ok("结果", st == {"1001": "stalled", "1002": "ended", "gpu-host-202611030900": "not_done", "1004": "not_started", "1005": "done",
                 "1006": "running", "1007": "gpu_busy", "1008": "unreadable"}, st)
ok("每个结果都有说明", all(a["state"] in proto.TASKLOG_STATES for a in A))
a = A[2]
ok("作业名去掉段号，段、主机、环境、代码、开始", (a["job"], a["part"], a["host"], a["env"], a["code"], a["start"]) ==
   ("gpu-host-202611030900", 2, "node1", "base", "abc1234", "2026-11-03 09:00:00"), a)
ok("几次命令结束取最后一次", (a["exit"], a["end"]) == (0, "2026-11-03 09:15:00"), a)
ok("按作业名认机器的 label（纯数字是 Slurm 几台 label 的公共前缀，别的按 hostname）", A[0]["machine"] == "Slurm" and a["machine"] == "pueue", (A[0]["machine"], a["machine"]))
ok("卡住：结束时刻、原因", (A[0]["end"], A[0]["note"]) == ("2026-11-01 12:00:00", "日志和结果文件夹 30 分钟没有变化，作业 1001 停掉它"), A[0])
ok("没结束：last 是最后一行带时刻的", (A[1]["end"], A[1]["last"]) == (None, "2026-11-02 09:00:00"), A[1])
ok("没开训的原因", A[3]["note"] == "这台机器没有环境 x" and A[3]["env"] == "未知", A[3])
ok("训完：结束时刻", (A[4]["end"], A[4]["exit"]) == ("2026-11-05 09:30:00", 0), A[4])
ok("字段齐", all(tuple(x) == proto.TASKLOG_ATTEMPT for x in A), list(A[0]))
ok("path 是完整路径", os.path.isfile(A[0]["path"]) and A[0]["file"] == os.path.basename(A[0]["path"]))
ev = [(e["log"], e["time"]) for e in r["events"]]
ok("事件日志里关于它的（记着 key 的按 key、没有的按文字，key 后面接着别的的不算，按时刻排）",
   ev == [("advance", "2026-10-31 08:00:00"), ("relay", "2026-11-01 09:00:00"), ("note", "2026-11-02 09:00:00")], ev)
ok("事件的字段", all(tuple(e) == proto.TASKLOG_EVENT for e in r["events"]))
ok("事件的文字原样", r["events"][0]["text"] == f"排了 fetch：{KEY}", r["events"][0])
ok("任务记录只有这个 key 的，原样", [x["id"] for x in r["records"]] == [5] and r["records"][0]["src"] == "site-pueue", r["records"])
ok("顶层字段", tuple(r) == proto.TASKLOG_TOP, list(r))

# 命令行：taskq log（--data、--json、文字）
env = dict(os.environ)
tq = os.path.join(os.path.dirname(HERE), "bin", "taskq")
p = subprocess.run([sys.executable, tq, "log", KEY, "--data", D, "--json"], capture_output=True, text=True, env=env)
j = json.loads(p.stdout) if p.returncode == 0 else {}
ok("taskq log --json", p.returncode == 0 and len(j.get("attempts", [])) == 8 and j["data"] == D, p.stderr[-300:])
env["TASKQ_DATA"] = D
p = subprocess.run([sys.executable, tq, "log", KEY], capture_output=True, text=True, env=env)
ok("taskq log 的文字（数据目录取 TASKQ_DATA）", p.returncode == 0 and "8 段" in p.stdout and "第 2 段  作业 gpu-host-202611030900（pueue）" in p.stdout
   and "被 worker 当作卡住停掉" in p.stdout and "事件日志里关于它的 3 条" in p.stdout, p.stdout + p.stderr)
p = subprocess.run([sys.executable, tq, "log", "demo/none", "--data", os.path.join(T, "nope")], capture_output=True, text=True, env=env)
ok("--data 不存在 → 退出码 2", p.returncode == 2, p.returncode)

print(f"\n{'都通过' if not fails else f'{fails} 项没通过'}")
sys.exit(1 if fails else 0)
