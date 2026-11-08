#!/usr/bin/env python3
"""测 health.py 的检查和推送规则：假的队列、训练日志、时钟，不连任何机器、不真的推送。

    python3 monitor/test_health.py
"""
import json
import os
import re
import sys
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue", "testdata"))
import fixture  # noqa: E402
fixture.use()
os.environ["TASKQ_TASKS"] = os.path.join(fixture.tmpdir(), "tasks.json")  # 不碰真的任务记录
os.environ["TASKQ_LAST"] = os.path.join(fixture.tmpdir(), "last")
os.environ["TASKQ_STATE"] = fixture.tmpdir()  # 健康检查自己的状态（pueue_seen 等）也不落到 HOME
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "queue"))
import health as H  # noqa: E402
import cfg  # noqa: E402

H.policy.PAUSE = {}  # 不受配置里的暂停影响

NOW = datetime(2026, 10, 16, 12, 0)
MET2 = "D116_gamma/V_save/2"
MEN0 = "D111_beta/V_save_n470/0"
PH = "site-pueue"  # 测试配置里的 pueue 执行机（ssh fake-pueue-host，标签 pueue）；hub 兼 store
fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def pt(dt):  # pueue 的时间格式
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000000000+08:00")


def prog(final=False, epochs=100, end=NOW):
    return dict(final=final, runs=[dict(start=ts(end - timedelta(hours=5)), end=ts(end), epochs_done=epochs, tl=[])], metrics={}, debug={})


def fields(status):
    """qs 从 status 拆出的 result、code（如 "Failed (143)" → Failed、143）。"""
    m = re.fullmatch(r"(\w+)(?: \((-?\d+)\))?", status)
    done = m[1] not in H.LIVE
    return dict(result=m[1] if done else None, code=int(m[2]) if m[2] else 0 if m[1] == "Success" else None)


def task(i, group, status, cmd, label="", start=NOW - timedelta(hours=3), end=None):
    return dict(id=i, group=group, status=status, **fields(status), label=label, deps=[], start=pt(start) if start else None,
                end=pt(end) if end else None, command=cmd)


PH_BUSY = {"groups": {}, "tasks": [dict(id=90, group="gpu1", status="Running", label="busy_a", deps=[], start=None, end=None,
                                         command="bash a"),
                                    dict(id=91, group="gpu1", status="Queued", label="busy_b", deps=[], start=None, end=None,
                                         command="bash b")]}


def snap(**kw):
    """gather() 的样子：pueue 机器（site-pueue）探查过、pueue 读到了；store（hub）上收集了训练状态。"""
    s = dict(now=NOW, ok={PH: True},
             probe={PH: {"DISK": str(int(200e9)), "GPULOG": ts(NOW - timedelta(minutes=1)), "BOOT": "2026-10-01 08:00:00|"}},
             src={"hub": {"tasks": {}}},
             pueue={PH: PH_BUSY})
    s.update(kw)
    return s


def ids(s):
    return set(H.check(s))


# ---------------- 检查 ----------------
ok("一切正常时没有问题", ids(snap()) == set())

s = snap(pueue={PH: {"groups": {}, "tasks": [
    task(1, "gpu1", "Failed (1)", "bash x", "infer_a", end=NOW - timedelta(hours=1)),
    task(2, "cpu", "DependencyFailed", "python y", "oof_a", end=NOW - timedelta(hours=1)),
    task(3, "gpu1", "Failed (1)", "bash z", "old", end=NOW - timedelta(hours=30)),
    task(4, "cpu", "Failed (143)", "bash w", "stopped", end=NOW - timedelta(hours=1)),
    task(5, "cpu", "Killed", "bash v", "killed", end=NOW - timedelta(hours=1)),
    task(6, "gpu1", "Failed (137)", "bash u", "oom", end=NOW - timedelta(hours=1)),
    task(8, "gpu0", "Killed", "bash t", "worker:w1", end=NOW - timedelta(hours=1)),
    task(9, "gpu0", "Failed (1)", "bash s", "worker:w2", end=NOW - timedelta(hours=1))]}})
P = H.check(s)
ok("任务失败、依赖失败都报", any(p.startswith(f"pueue:{PH}:1:") for p in P) and any(p.startswith(f"pueue:{PH}:2:") for p in P))
ok("24 小时前的失败不再报", not any(p.startswith(f"pueue:{PH}:3:") for p in P))
ok("手动停的（143、pueue kill）只记不推", [p["level"] for k, p in P.items() if k.startswith((f"pueue:{PH}:4:", f"pueue:{PH}:5:"))] == ["info"] * 2)
ok("训练命令被信号杀掉（137，多半是内存不够）照常报", [p["level"] for k, p in P.items() if k.startswith(f"pueue:{PH}:6:")] == ["normal"])
ok("调度停的 worker（时段结束、让卡）不报；worker 出错照常报", not any(p.startswith(f"pueue:{PH}:8:") for p in P)
   and any(p.startswith(f"pueue:{PH}:9:") for p in P))

s = snap(src={"hub": {"tasks": {}, "errors": {MEN0: "OSError: [Errno 5] Input/output error"}}})
ok("store 上读任务状态出错（collect 的 errors）→ 单独报，不算读不动", "readerr:hub" in ids(s) and "unread:hub" not in ids(s))


# ---------------- 总队列分给执行机 ----------------
def tqrow(key, state, worker=None, since=None, log_age=None, kind="toy", task_log_age=None, out_age=None):
    return dict(key=key, state=state, worker=worker, host=worker, since=since, beat=None, note="", log_age=log_age, failures=0,
                kind=kind, nice=0, order=1, task_log_age=task_log_age, out_age=out_age)


INF = "infer/D103_alpha_one/x/frag_2"

ok("hub 的总队列读不到 → 报", "taskq:hub" in H.check(snap(taskq={"hub": None})))
ok("总队列关着（没有 ENABLED）→ 持续 60 分钟后报", H.check(snap(taskq={"hub": dict(enabled=False, tasks=[])})).get("tqoff:hub", {}).get("after") == 60)
bq = [dict(tqrow(INF, "blocked", None), note="等的 x 失败 2 次"), dict(tqrow(MET2, "claimed", PH), assigned=PH)]
P = H.check(snap(taskq={"hub": dict(enabled=True, paused=False, tasks=bq)}))
ok("总队列里的 blocked 不由健康检查报（能去的都跳过了的由 relay 报 tqblocked）", f"tqblocked:{INF}" not in P)


import subprocess  # noqa: E402

# ---------------- gather：各卡和卡上的进程、只看本机、store 上收集训练状态 ----------------
fakebin = fixture.tmpdir()
sl = subprocess.Popen(["sleep", "30"])
with open(os.path.join(fakebin, "nvidia-smi"), "w") as f:
    f.write(f'#!/bin/bash\ncase "$*" in *query-gpu=*) echo "0, GPU-a, 37, 1200"; echo "1, GPU-b, 0, 5";;\n'
            f'*query-compute-apps*) echo "GPU-a, {sl.pid}, 1100"; echo "GPU-x, 1, 9";; esac\n')
os.chmod(os.path.join(fakebin, "nvidia-smi"), 0o755)
out = subprocess.run(["bash", "-s"], input=H.GPU_PROBE, capture_output=True, text=True,
                     env=dict(os.environ, PATH=fakebin + ":" + os.environ["PATH"])).stdout
sl.kill()
gp = H.gpus_of(out.splitlines())
ok("GPU 探查实跑：每张卡一项，卡上的进程带命令行，对不上卡的进程不要",
   [(x["gpu"], x["util"], x["mem"]) for x in gp] == [("0", "37", "1200"), ("1", "0", "5")]
   and [(a["pid"], a["mem"], a["args"]) for a in gp[0]["apps"]] == [(str(sl.pid), "1100", "sleep 30")] and gp[1]["apps"] == [])
ok("探查脚本里有 GPU 那几行；没写 gpu_log、disk 的机器按默认（不查）", H.GPU_PROBE in H.probe_script(H.M.machine("hub"))
   and "GPULOG" not in H.probe_script(H.M.machine("hub")) and "GPULOG" in H.probe_script(H.M.machine(PH)))
calls = []
saved = (H.sh, H.run_on, H.RM.store_op, H.taskq.local_status)
H.taskq.local_status = lambda: dict(enabled=True, tasks=[])  # hub 上的总队列


def fake_sh(cmd, inp=None, timeout=120):
    calls.append(("sh", cmd))
    if cmd[0] == "git":
        return 0, "abc\n"
    return 0, json.dumps({n: {"groups": {}, "tasks": []} for n in (cmd[2:] or H.M.pueue_hosts())})


def fake_run_on(m, script, timeout=120):
    calls.append(("run_on", m["label"]))
    return 0, "BOOT|2026-10-01 08:00:00|\nDISK|123\nGPU|0, GPU-a, 37, 1200\nAPP|GPU-a| 42| 1100|python x|y\n"


COLLECTED = {"tasks": {MEN0: prog()}, "seglogs": []}


def fake_store_op(what, data, timeout=600, say=None):
    calls.append(("store_op", what, sorted(data)))
    return COLLECTED


H.sh, H.run_on, H.RM.store_op = fake_sh, fake_run_on, fake_store_op
g = H.gather()
ok("gather：GPU、APP 行进 gpus，不进 probe；命令行里的 | 原样留着",
   g["probe"][PH] == {"BOOT": "2026-10-01 08:00:00|", "DISK": "123"}
   and g["gpus"][PH] == [dict(gpu="0", uuid="GPU-a", util="37", mem="1200", apps=[dict(pid="42", mem="1100", args="python x|y")])])
ok("gather：探查、读 pueue 的是有 pueue 组的机器（pueue 执行机、hub；表里别的机器不连）",
   set(H.M.pueue_hosts()) >= {"hub", PH} and set(g["ok"]) == set(g["pueue"]) == set(H.M.pueue_hosts())
   and {c[1] for c in calls if c[0] == "run_on"} == {H.M.MACHINES[n]["label"] for n in H.M.pueue_hosts()})
ok("gather：训练状态在 store 上收集（site_ops 的 collect），按 store 的名字放",
   [c for c in calls if c[0] == "store_op"] == [("store_op", "collect", ["specs", "wait"])] and g["src"] == {"hub": COLLECTED})


def broken_store_op(what, data, timeout=600, say=None):
    raise RuntimeError("hub 上 collect 出错（124）：timeout")


H.RM.store_op = broken_store_op
g = H.gather()
ok("gather：store 上收集出错 → 记原因、报 collect（不当成整台连不上：relay 照样给它换包），不报错",
   g["ok"]["hub"] is not False and "timeout" in g["collect"]["hub"] and g["src"] == {}
   and "collect:hub" in H.check(dict(g, now=NOW)) and "down:hub" not in H.check(dict(g, now=NOW)))
H.RM.store_op = fake_store_op
calls.clear()
H.policy.PAUSE = {PH: (None, "停机维护")}
g = H.gather()
P_off = H.check(dict(g, now=NOW))
ok("gather：暂停着的机器不连（不探查），算连不上，但不报 down",
   not [c for c in calls if c[0] == "run_on" and c[1] == H.M.MACHINES[PH]["label"]] and g["ok"][PH] is False
   and "暂停着：停机维护" in g["why"][PH] and f"down:{PH}" not in P_off)
H.policy.PAUSE = {}
calls.clear()
g = H.gather(remote=False)
qs_call = next(c[1] for c in calls if c[0] == "sh" and c[1][0] != "git")
ok("gather(remote=False)：只探查本机有 pueue 组的机器（hub），qs 只问它（不连别的机器）；store 在本机照样收集",
   [c for c in calls if c[0] == "run_on"] == [("run_on", "hub")] and qs_call[1:] == ["--json", "hub"]
   and set(g["pueue"]) == {"hub"} and set(g["taskq"]) == {"hub"} and g["src"] == {"hub": COLLECTED})
H.M.MACHINES["loc"] = dict(label="loc", hostname=r"\A\Z", ssh=None, scheduler="pueue", root="/tmp/loc_root", workers=[dict(cpus=2)])
calls.clear()
g = H.gather(remote=False)
qs_call = next(c[1] for c in calls if c[0] == "sh" and c[1][0] != "git")
ok("gather(remote=False)：本机的 pueue 执行机（ssh=None）也探查，qs 问 hub 和它",
   sorted(c[1] for c in calls if c[0] == "run_on") == ["hub", "loc"] and sorted(qs_call[2:]) == ["hub", "loc"]
   and set(g["ok"]) == set(g["pueue"]) == {"hub", "loc"})
del H.M.MACHINES["loc"]
H.sh, H.run_on, H.RM.store_op = saved[:3]

# 真跑一次 store 上的 collect：store 根目录（临时 HOME 下的 ~/demo_root）的 code/current 是手搭的代码包（fixture.store_bundle），
# python3 是跑测试的这个；store 的数据目录是临时的，一个任务刚训完、分段日志已经拉回到 store，产出还在执行机上（store 上没有）
_home, _data = fixture.tmpdir(), fixture.tmpdir()
_py = os.path.join(fixture.store_bundle(os.path.join(_home, "demo_root")), "bin")
_seg = H.proto.task_log(MEN0, "77")
os.makedirs(os.path.dirname(os.path.join(_data, _seg)))
open(os.path.join(_data, _seg), "w").write("job h_1 @ hub, task=k, part 1, env base, code abc1234, 2026-11-01 10:00:00\n")
_env, _mdata, _rpy = os.environ["HOME"], H.M.MACHINES["hub"]["data"], H.RM.py
os.environ["HOME"], H.M.MACHINES["hub"]["data"], H.RM.py = _home, _data, lambda name: os.path.join(_py, "python3")
try:
    H.sh, H.run_on = fake_sh, fake_run_on
    g = H.gather()
finally:
    os.environ["HOME"], H.M.MACHINES["hub"]["data"], H.RM.py = _env, _mdata, _rpy
    H.sh, H.run_on = saved[:2]
ok("gather 真跑 store 上的 collect：产出还在执行机上的任务没有进度，store 上的分段日志收到了",
   g["ok"].get("hub") is not False and MEN0 not in (g["src"].get("hub") or {}).get("tasks", {"x": 1})
   and [x["name"] for x in g["src"]["hub"]["seglogs"]] == [os.path.basename(_seg)])
if g["ok"].get("hub") is False:
    print(g["why"]["hub"])
H.taskq.local_status = saved[3]

# ---------------- notices, acks, problems: the hub database (monitor/notice.py) ----------------
import notice as NT  # noqa: E402
_nh = fixture.tmpdir()
_nenv = dict(os.environ, HOME=_nh)
TQ = os.path.join(os.path.dirname(HERE), "bin", "taskq")


def tqn(*a, inp=None):
    return subprocess.run([sys.executable, TQ, "notice", *a], capture_output=True, text=True, env=_nenv, input=inp, timeout=60)


r1 = tqn("set", "board", "看板有新结果", "等确认")
r2 = tqn("set", "b-2_x", inp="from stdin\n")
r3 = tqn("list", "--json")
_nl = json.loads(r3.stdout)
_db = NT.open_db(write=False)
ok("notice set: text from arguments or stdin goes into the database; list --json lists them by name",
   r1.returncode == r2.returncode == r3.returncode == 0 and NT.notices(_db)["board"]["text"] == "看板有新结果 等确认"
   and tuple(_nl) == H.proto.NOTICE_TOP and [x["name"] for x in _nl["notices"]] == ["b-2_x", "board"]
   and _nl["notices"][0]["text"] == "from stdin" and all(tuple(x) == H.proto.NOTICE_FIELDS for x in _nl["notices"]))
r4 = tqn("set", "a/b", "x")
r5 = tqn("set", "x y", "x")
r6 = tqn("set", "empty", inp="")
ok("notice set: bad name or no text → usage error, nothing written", r4.returncode == r5.returncode == r6.returncode == 2
   and sorted(NT.notices(_db)) == ["b-2_x", "board"])
r7 = tqn("clear", "b-2_x", "nope")
r8 = tqn("list")
ok("notice clear: missing ones are not an error; list as text", r7.returncode == 0 and "没有这个提醒：nope" in r7.stdout
   and r8.stdout.startswith("board（") and "看板有新结果 等确认" in r8.stdout and "b-2_x" not in r8.stdout)
ok("notice --help", tqn("--help").returncode == 0 and "set" in tqn("--help").stdout)
r9 = tqn("set", "later", "--at", "3d", "do it later")
ok("notice set --at: listed with its time, not reported before then",
   r9.returncode == 0 and NT.notices(_db, due_only=False)["later"]["at"] > NT.time.strftime(NT.TF) and "later" not in NT.notices(_db))
tqn("clear", "later")
saved = H.sh, H.run_on, H.RM.store_op
H.sh, H.run_on, H.RM.store_op = fake_sh, fake_run_on, fake_store_op
g = H.gather(remote=False)
ok("gather: notices → s['notices']", g["notices"] == {"board": "看板有新结果 等确认"})
tqn("clear", "board")
ok("gather: gone once cleared", H.gather(remote=False)["notices"] == {})
H.sh, H.run_on, H.RM.store_op = saved

# 登记过的问题：一个事件、一个持续的
_wdb = NT.open_db()
NT.save_problems(_wdb, {"boot:hub:2026-11-05 13:52:30": H.problem("x", kind="event"), "disk:hub": H.problem("y")}, "2026-11-05 13:00")

# taskq notice ack: only kind=event; acked ones are left out; acks of gone problems are dropped
ra, rb, rc_ = tqn("ack", "boot:hub"), tqn("ack", "disk:hub"), tqn("ack", "nosuch")
ok("notice ack: prefix finds the event; ongoing or unknown problems are not acked (exit 1)",
   ra.returncode == 0 and "boot:hub:2026-11-05 13:52:30" in NT.acked(_wdb) and rb.returncode == 1 and "持续的问题" in rb.stderr
   and rc_.returncode == 1 and "没有这个问题" in rc_.stderr)
_P = {"boot:hub:2026-11-05 13:52:30": H.problem("x", kind="event"), "disk:hub": H.problem("y")}
ok("drop_acked: acked events left out, others kept", list(H.drop_acked(dict(_P), _wdb)) == ["disk:hub"])
NT.save_problems(_wdb, {"disk:hub": H.problem("y")}, "2026-11-05 14:00")
ok("an ack outlives the save of the filtered list: still left out on the next check",
   list(H.drop_acked(dict(_P), _wdb)) == ["disk:hub"] and "boot:hub:2026-11-05 13:52:30" in NT.acked(_wdb))
H.drop_acked({"disk:hub": H.problem("y")}, _wdb, dry=True)
ok("dry run: acks kept even when the event is gone", "boot:hub:2026-11-05 13:52:30" in NT.acked(_wdb))
H.drop_acked({"disk:hub": H.problem("y")}, _wdb)
ok("event no longer raised: its ack dropped", NT.acked(_wdb) == {})
rp = subprocess.run([sys.executable, TQ, "problems", "--json", "--all"], capture_output=True, text=True, env=_nenv, timeout=60)
_pj = json.loads(rp.stdout)
ok("taskq problems --json: proto.PROBLEMS_TOP, each problem proto.OVERVIEW_PROBLEM",
   tuple(_pj) == H.proto.PROBLEMS_TOP and [p["id"] for p in _pj["problems"]] == ["disk:hub"]
   and all(tuple(p) == H.proto.OVERVIEW_PROBLEM for p in _pj["problems"]))
NT.save_problems(_wdb, {}, None)

s = snap()
s["probe"][PH]["DISK"] = str(int(10e9))
P = H.check(s)
ok("site-pueue 盘不到 15 GB → 紧急", P[f"disk:{PH}"]["level"] == "urgent")
s["probe"][PH]["DISK"] = str(int(30e9))
ok("site-pueue 盘不到 40 GB → 提醒", H.check(s)[f"disk:{PH}"]["level"] == "normal")
s["probe"][PH]["GPULOG"] = ts(NOW - timedelta(minutes=30))
P = H.check(s)
ok("GPU 记录停了 → 报", f"gpulog:{PH}" in P)

s = snap(notices={"board": "看板有新结果等确认后发布"})
s["probe"][PH]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 开机：放回队列 3 4"
P = H.check(s)
ok("重启过 → 报（带上放回的任务）", [p["msg"] for k, p in P.items() if k.startswith("boot:")] == ["pueue 10-16 10:00 重启过：开机：放回队列 3 4"])
s["probe"][PH]["BOOT"] = "2026-10-01 08:00:00|"
ok("很久前重启的不报", not [k for k in H.check(s) if k.startswith("boot:")])
ok("挂着的提醒 → 每条报一个 notice:<名字>，说明是提醒的文字",
   P.get("notice:board", {}).get("msg") == "看板有新结果等确认后发布" and P["notice:board"]["data"] == dict(name="board")
   and "board" not in P)


# ---------------- 第五台机器：只在表里加一项（没写的字段按默认）----------------
H.M.MACHINES["fake"] = dict(label="fake", hostname=r"fake-host", ssh="fake", scheduler="pueue", root="~/q", conda=("~/c", "e"),
                            workers=[dict(gpu="0")], disk=("/data", 50, 10))
s = snap(pueue={PH: {"groups": {}, "tasks": []}, "fake": {"groups": {}, "tasks": []}},
         src={"hub": {"tasks": {}}, "fake": {"tasks": {}}, PH: {"tasks": {MEN0: prog(end=NOW - timedelta(minutes=5))}}})
s["ok"]["fake"] = True
s["probe"]["fake"] = {"DISK": str(int(5e9)), "BOOT": "2026-10-01 08:00:00|"}
P = H.check(s)
ok("第五台机器：盘按表里的字段查，没写 gpu_log 的不查 GPU 记录", P["disk:fake"]["level"] == "urgent" and "gpulog:fake" not in P)
s["ok"]["fake"] = False
ok("第五台机器连不上：报", "down:fake" in ids(s))
del H.M.MACHINES["fake"]

# ---------------- 推送规则：白话、并成一条、不推已恢复和汇总 ----------------
_wdb = NT.open_db()
sent = []
H.push = lambda msg, **kw: sent.append((kw.get("title", ""), msg))


def run(P, now):
    sent.clear()
    H.update({k: dict(v) for k, v in P.items()}, now, _wdb)
    return list(sent)


A = {f"disk:{PH}": dict(msg="pueue 盘", level="normal", kind="state", after=0, data=dict(path="/data", free_gb=12.0))}
U = {f"pueued:{PH}": dict(msg="没响应", level="urgent", kind="state", after=0)}
day1 = datetime(2026, 10, 16, 12, 0)
out = run(A, day1)
ok("白天新问题：推送一条白话", len(out) == 1 and out[0][0] == f"{H.NAME}：需要处理" and "盘满了" in out[0][1])
ok("同一个问题不重复推送", run(A, day1 + timedelta(minutes=30)) == [])
ok("恢复后不推送", run({}, day1 + timedelta(hours=1)) == [])
ok("夜里普通问题不推送", run(A, datetime(2026, 10, 16, 23, 0)) == [])
out = run({**A, **U}, datetime(2026, 10, 17, 1, 0))
ok("夜里紧急问题即时推送", len(out) == 1 and out[0][0] == f"{H.NAME}：紧急" and "没有响应" in out[0][1])
out = run({**A, **U}, datetime(2026, 10, 17, 8, 5))
ok("早上第一次检查推送夜里攒下的", len(out) == 1 and "盘满了" in out[0][1] and "没有响应" not in out[0][1])
ok("推过之后不再推送", run(A, datetime(2026, 10, 17, 8, 35)) == [])
D = {"down:site-slurm": dict(msg="Slurm 连不上", level="normal", kind="state", after=60)}
ok("要持续一段时间才报的，刚出现时不推送", run({**A, **D}, datetime(2026, 10, 17, 9, 0)) == [])
out = run({**A, **D}, datetime(2026, 10, 17, 10, 0))
ok("持续够久后推送", len(out) == 1 and "Slurm 读不到状态了" in out[0][1])
B = {f"code:{PH}": dict(msg="g", level="normal", kind="state", after=0), f"gpulog:{PH}": dict(msg="l", level="normal", kind="state", after=0)}
out = run(B, datetime(2026, 10, 17, 10, 30))
ok("同一次查到几件：并成一条", len(out) == 1 and "（2 件）" in out[0][0] and out[0][1].count("\n- ") == 1)
I = {f"idle:{PH}:gpu1": dict(msg="只剩在训的", level="info", kind="state", after=0)}
ok("info 级不推送", run(I, datetime(2026, 10, 17, 11, 0)) == [])
N = {"notice:board": dict(msg="看板等确认", level="normal", kind="state", after=0)}
ok("提醒（notice）不推送，只列在 problems 里", run(N, datetime(2026, 10, 17, 11, 30)) == []
   and "notice:board" in NT.problems(_wdb)[0])

# ---------------- feed / advance 出错不让健康检查停掉 ----------------
import types  # noqa: E402
got = {}
H.gather, H.update = (lambda: snap()), (lambda P, now, db, dry=False: got.update(P))
fd = types.ModuleType("feed")
feed_calls = []
fd.run_all = lambda s, dry=False: (feed_calls.append(dry), ([], {"x:y": "z"}))[1]
sys.modules["feed"] = fd
sys.modules["relay"] = None  # health.py 不直接调 relay.py（送任务只从 feed.run_all 进）
adv = types.ModuleType("advance")
adv.run = lambda s, dry=False: 1 / 0
sys.modules["advance"] = adv
sys.argv = ["health.py"]
H.main()
ok("advance.py 运行出错：报出来，健康检查照常写完",
   "advance:error" in got and "ZeroDivisionError" in got["advance:error"]["msg"])
ok("送任务只有一个入口：feed.run_all 调一次，它报的问题照常进来，健康检查不直接 import relay",
   feed_calls == [False] and got.get("x:y", {}).get("msg") == "z" and "relay:error" not in got)
sys.modules["feed"] = None  # import 出错
got.clear()
H.main()
ok("feed.py import 出错：报 feed:error，健康检查照常写完", "feed:error" in got and "advance:error" in got)

# ---------------- 白话（plain.py）：标签、任务 key → 实验名 + 任务简称 ----------------
import plain as PL  # noqa: E402
import experiments as EX  # noqa: E402
_k, _t = next(iter(EX.tasks().items()))
_e = EX.experiment(_t["exp"])
_n = PL.exp_name(_e)
ok("白话：任务 key → 实验名 + 简称（如 f0）", PL.key_name(_k) == f"{_n} {EX.short(_t)}" and EX.short(_t))
ok("白话：每个任务的步骤的标签 → (实验名, 步骤, 简称)；实验一级的 → 简称 None",
   PL.label(f"eval:{EX.label(_e, _t)}") == (_n, PL.STEPS["eval"], EX.short(_t)) and PL.label(f"eval:{_e['id']}") == (_n, PL.STEPS["eval"], None))
ok("白话：认不出的标签 → 其他", PL.label("eval:nope_f1") == ("其他", "eval:nope_f1", None))
_pm = "policy.py PLACE['x'] 里的 ['y'] 不是执行机。训完以后的步骤这次一个都没排；改好、提交推送后下一次检查自动恢复"
ok("白话：PLACE 写错（advance:policy）说清哪里错、怎么恢复，不当成某个任务的步骤出错",
   PL.problem("advance:policy", dict(msg=_pm)).startswith("训完以后的步骤停了") and "['y'] 不是执行机" in PL.problem("advance:policy", dict(msg=_pm)))
ok("白话：推送里的一句话带简称", PL.task(f"infer:{EX.label(_e, _t)}") == f"{_n} {EX.short(_t)} 的{PL.STEPS['infer']}")

# 白话从结构化的 data 说：每种问题的推送文字
_ki = "infer/" + _k
_inf = task(1, "gpu0", "Failed (1)", "bash x", f"infer:{EX.label(_e, _t)}", end=NOW - timedelta(hours=1))
s = snap(pueue={PH: {"groups": {}, "tasks": [_inf, task(2, "cpu", "Failed (1)", "bash y", "", end=NOW - timedelta(hours=1))]}},
         src={"hub": {"tasks": {}, "stuck": ["a/b/0", "c/d/1"]}, PH: {"tasks": {MEN0: prog(end=NOW - timedelta(hours=2))}}},
         notices={"board": "看板有新结果，要确认后才发布（3 个文件）。"})
s["probe"][PH]["DISK"] = str(int(10e9))
s["probe"][PH]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 开机：放回队列 3 4"
P = H.check(s)
_rl = {f"site:{PH}:fail:{MEN0}": dict(msg="…训练失败 2 次，有进度…", data=dict(progress=True, failures=2)),
       f"site:{PH}:fail:{_ki}": dict(msg="…failed…", data=dict(progress=False)),
       f"site:{PH}:orphan:zz": dict(msg="…认不出…", data=dict(unknown=True)),
       f"site:{PH}:orphan:{MEN0}": dict(msg="…不是它领着…", data=dict(unknown=False)),
       f"tqblocked:{_ki}": dict(msg="…都跳过了…", data=dict(key=_ki))}  # relay 报的
P.update({k: H.from_step(v) for k, v in _rl.items()})
_f, _l1, _pe = PL.key_name(MEN0), H.M.machine(PH)["label"], sorted(k for k in P if k.startswith(f"pueue:{PH}:"))
want = {"unread:hub": "hub 上 2 个任务的文件读不动（多半是那边的存储挂住），这些任务先不分出去，别的照常。",
        _pe[0]: f"{_l1} 上的 {_n} {EX.short(_t)} 的推理出错停了。",
        _pe[1]: f"{_l1} 上的一次性任务 #2 出错停了。",
        f"tqblocked:{_ki}": f"{PL.key_name(_ki)} 能去的执行机都试过、都训不了（跳过了），不会自己开始。",
        f"disk:{PH}": f"{_l1} /mnt/disk1 只剩 10 GB，盘满了训练和存档会失败。",
        f"boot:{PH}:" + ts(NOW - timedelta(hours=2)): f"{_l1} 10-16 10:00 重启过，被打断的任务已自动放回队列，要确认训练接上了。",
        "notice:board": "看板有新结果，要确认后才发布（3 个文件）。",
        f"site:{PH}:fail:{MEN0}": f"{_l1} 上 {_f} 训练失败了，有进度，要决定接着训还是放弃。",
        f"site:{PH}:fail:{_ki}": f"{_l1} 上 {PL.key_name(_ki)} 的任务出了问题，不会开始训练。",
        f"site:{PH}:orphan:zz": f"{_l1} 的队列里有一个认不出来的任务。",
        f"site:{PH}:orphan:{MEN0}": f"{_l1} 在训 {_f}，但总队列里没分给它，可能重复训练。"}
got_ = {k: PL.problem(k, P[k]) for k in want if k in P}
ok("白话：每种问题从 data 说", got_ == want and all("data" in P[k] for k in want))
if got_ != want:
    print("\n".join(f"  {k}\n    要 {want[k]}\n    得 {got_.get(k)}" for k in want if got_.get(k) != want[k]))
ok("白话：没有 data 的退回原来的说明",
   PL.problem("unread:hub", dict(msg="原来的说明")) == "原来的说明" and PL.problem(f"site:{PH}:fail:{MEN0}", dict(msg="m")) == "m")
ok("执行机报的卡住可以是说明或 dict(msg, data)", H.from_step("m") == dict(msg="m", level="normal", kind="state", after=0)
   and H.from_step(dict(msg="m", data=dict(n=1)))["data"] == dict(n=1))
ok("执行机报的只是在等的（level=info）只记不推；写到声明以外不推送",
   H.from_step(dict(msg="m", data=dict(n=1), level="info"))["level"] == "info" and "undeclared" in H.QUIET)
ok("白话：实验表里的步骤都有中文名（STEPS：核心的 + 插件 labels.py 的，或步骤的 zh）",
   all(PL.step_name(n) != n for n in PL.table_steps()) and PL.step_name("nope") == "nope")

# 白话的项目部分来自插件 labels.py：步骤名、不列的步骤、分析编号的标题；实验名里不提实验表里最多的模型
ok("白话：步骤名先查实验表的 zh，再查核心的和插件的 STEPS，再没有用原名",
   PL.step_name("lprob") == "概率汇总" and PL.step_name("relay_fetch") == "结果拉回" and PL.step_name(H.proto.ENV_BUILD) == "建环境")
ok("白话：插件的 QUIET 接在核心的后面；不列的步骤 → None",
   PL.QUIET == ["train", "archive", "infer", "prune"] and PL.label(f"prune:{_e['id']}") is None)
ok("白话：实验表最后一起做的步骤（没有后半）→ 所有实验；别的没有后半的 → 其他",
   PL.label("compare_all") == ("所有实验", "对比表", None) and PL.label("nope") == ("其他", "nope", None)
   and PL.task("compare_all") == "对比表（所有实验）")
ok("白话：分析编号的标题来自插件的 PLAN_TITLES：<编号> 或 <编号>_… 才算",
   PL.label("sens:X1") == ("分析", "X1 示例问题一", None) and PL.label("sens:X12_b") == ("分析", "X12 示例问题十二", None)
   and PL.label("sens:X1b") == ("其他", "sens:X1b", None) and PL.label("sens:X3_a") == ("其他", "sens:X3_a", None)
   and PL.task("sens:X1_a") == "分析 X1 示例问题一")
_t0, _as = EX.experiment("t0"), EX.experiment("arch_s")
ok("白话：实验名不提实验表里最多的模型，别的模型带上", PL.usual_model() == "base"
   and PL.exp_name(_t0) == "T0 基线" and PL.exp_name(_as) == "T0 基线 s")
_ke = "eval/" + _k
ok("白话：能去的执行机都跳过了（tqblocked）按 key 说；没有 data 时从编号取 key",
   PL.problem(f"tqblocked:{_ke}", dict(msg="m", data=dict(key=_ke))) == f"{PL.key_name(_ke)} 能去的执行机都试过、都训不了（跳过了），不会自己开始。"
   and PL.problem(f"tqblocked:{_k}", dict(msg="m")).startswith(f"{PL.key_name(_k)} 能去的执行机"))
_np = subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, sys.argv[1]); import plain as PL; "
                      "print(PL.step_name('lprob'), PL.plan_items(), PL.QUIET, PL.label('sens:X1'))", HERE],
                     capture_output=True, text=True, env=dict(os.environ, TASKQ_PLUGINS=""), timeout=60)
ok("白话：没有插件 → 只有核心的步骤名，没有分析标题", _np.returncode == 0
   and _np.stdout.split() == ["lprob", "{}", "['train',", "'archive']", "('其他',", "'sens:X1',", "None)"], )
if _np.returncode:
    print(_np.stderr[-2000:])

# 插件、实验表加载出错：health 在 import 时就停，要推送一次（同样的错不重复推，夜里不推），修好后清掉记录
import shutil  # noqa: E402
_lt = fixture.tmpdir()
_bad = os.path.join(_lt, "bad")
shutil.copytree(os.environ["TASKQ_PLUGINS"].split(os.pathsep)[0], _bad, ignore=shutil.ignore_patterns("__pycache__"))
open(os.path.join(_bad, "readers", "zz_broken.py"), "w").write("PROGRESS = {\n")
_wrap = os.path.join(_lt, "wrap.py")
open(_wrap, "w").write("""import datetime, runpy, sys, urllib.request
H, LOG, HOUR = sys.argv[1], sys.argv[2], int(sys.argv[3])
class _D(datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime.datetime(2026, 11, 3, HOUR, 0)
datetime.datetime = _D
class _R:
    def read(self):
        return b""
def _open(req, timeout=0):
    open(LOG, "a").write(req.full_url + " " + req.data.decode().replace(chr(10), " ") + chr(10))
    return _R()
urllib.request.urlopen = _open
sys.argv = [H] + sys.argv[4:]
sys.path.insert(0, H.rsplit("/", 1)[0])
runpy.run_path(H, run_name="__main__")
""")


def _health(plugins, hour, *args):
    home = os.path.join(_lt, "home")
    os.makedirs(os.path.join(home, ".config", "taskq"), exist_ok=True)
    open(os.path.join(home, ".config", "taskq", "ntfy_topic"), "w").write("t0\n")
    env = dict(os.environ, HOME=home, TASKQ_PLUGINS=plugins, TASKQ_STATE=os.path.join(_lt, "st"),
               TASKQ_DIR=os.path.join(_lt, "q"))
    log = os.path.join(_lt, "pushed.log")
    r = subprocess.run([sys.executable, _wrap, os.path.join(HERE, "health.py"), log, str(hour), *args],
                       env=env, capture_output=True, text=True, timeout=60)
    return r.returncode, (open(log).read().splitlines() if os.path.exists(log) else [])


rc1, p1 = _health(_bad, 12)
rc2, p2 = _health(_bad, 12)
ok("插件写错：health 退出码非零，推送一次（带出错的文件），同样的错下次不再推",
   rc1 != 0 and len(p1) == 1 and "zz_broken" in p1[0] and rc2 != 0 and len(p2) == 1)
rc3, _ = _health(os.environ["TASKQ_PLUGINS"], 12, "--test")  # 不推真的（urlopen 换掉了）
ok("plugins fixed: imports again, the load error mark is cleared",
   rc3 == 0 and NT.D.get_meta(NT.open_db(write=False), "load_error") is None)
shutil.rmtree(os.path.join(_lt, "home"))
if os.path.exists(os.path.join(_lt, "pushed.log")):
    os.remove(os.path.join(_lt, "pushed.log"))
rc4, p4 = _health(_bad, 23)
ok("插件写错、夜里：不推送（早上第一次检查时推）", rc4 != 0 and p4 == [])

# ---------------- pueued 没了：自动恢复 ----------------
LOC = dict(label="loc", hostname=r"\A\Z", ssh=None, scheduler="pueue", root="/tmp/loc_root", workers=[dict(cpus=2)])
H.M.MACHINES["loc"] = LOC  # 本机的 pueue 执行机（ssh=None）
_seen_s = snap(pueue={"loc": {"groups": {"cpu": "Running", H.proto.IO_GROUP: "Paused", "wait": "Running"}, "tasks": []},
                      PH: {"error": "Error: connection refused"}})
_sp = os.path.join(H.HDIR, H.proto.PUEUE_SEEN)
if os.path.exists(_sp):
    os.remove(_sp)
H.C.write_json(_sp, {PH: dict(t=111, running=["gpu1"])})
_seen = H.pueue_seen(_seen_s, dry=True)
ok("pueue_seen：记下 pueued 在的机器的时间和开着的组；没响应的那台留着上次的；dry 不写",
   _seen["loc"] == dict(t=int(NOW.timestamp()), running=["cpu", "wait"]) and _seen[PH]["t"] == 111
   and json.load(open(_sp)) == {PH: dict(t=111, running=["gpu1"])})
H.pueue_seen(_seen_s)
ok("pueue_seen：不是 dry 时写下", json.load(open(_sp))["loc"]["running"] == ["cpu", "wait"])
_ran, _qs = [], {"out": ""}
_ro, _sh = H.run_on, H.sh
H.run_on = lambda m, script, timeout=120: (_ran.append((m["ssh"], script)), (0, "2026-11-06 08:17:00 pueued 恢复：放回队列 3 4\n"))[1]
H.sh = lambda cmd, inp=None, timeout=120: (0, _qs["out"])
_ev_added = []
import events as _EV  # noqa: E402
_EV_add = _EV.add
_EV.add = lambda source, msg, db=None, **kw: _ev_added.append((source, msg, kw))
BOOT_SH = "/" + cfg.bundle_rel("queue", "on_boot.sh")
try:
    _down = snap(pueue={"loc": {"error": "Error: no socket"}, PH: {"error": "Error: no socket"}}, ok={PH: True, "loc": True})
    H.policy.AUTO["revive"] = False
    ok("revive：开关关着不动", H.revive(_down, {}) == ([], {}) and not _ran)
    H.policy.AUTO["revive"] = True
    _qs["out"] = json.dumps({"loc": {"groups": {}, "tasks": []}, PH: {"groups": {}, "tasks": []}})
    ev, still = H.revive(_down, {"loc": dict(t=1000, running=["cpu", "wait"]), PH: dict(t=2000, running=[])})
    loc_cmd = next(sc for sh_, sc in _ran if sh_ is None)
    ph_cmd = next(sc for sh_, sc in _ran if sh_ == "fake-pueue-host")
    ok("revive：本机的（ssh=None）也跑它根目录 code/current 里的 on_boot.sh --since <上次在的时间-60> --resume <那时开着的组>",
       loc_cmd == f"bash /tmp/loc_root/code/current{BOOT_SH} --since 940 --resume cpu,wait" and H.M.code("loc") == "/tmp/loc_root/code/current")
    ok("revive：别的机器跑它代码包里的 on_boot.sh（没有开着的组时不带 --resume；写了 gpu_log 的带 --gpu-log）",
       ph_cmd == f"bash {H.M.code(PH)}{BOOT_SH} --since 1940 --gpu-log")
    ok("revive：恢复成功 → 记事件（health/revive），不留原因", len(ev) == 2 and all("已自动恢复" in e and "放回队列 3 4" in e for e in ev)
       and still == {} and {x[2].get("site") for x in _ev_added} == {"loc", PH} and _ev_added[0][2]["kind"] == "revive")
    _ran.clear()
    ev, still = H.revive(_down, {}, dry=True)
    ok("revive：dry 只说会做什么，不跑", not _ran and len(ev) == 2 and all("会自动恢复" in e for e in ev))
    _one = snap(pueue={"loc": {"error": "x"}, PH: {"groups": {}, "tasks": []}}, ok={PH: True, "loc": True})
    ev, still = H.revive(_one, {})
    ok("revive：没有上次的记录 → 只启动（--since 现在、不带 --resume）；pueued 在的机器不动",
       len(_ran) == 1 and _ran[0][0] is None and f"--since {int(NOW.timestamp())}" in _ran[0][1] and "--resume" not in _ran[0][1])
    _qs["out"] = json.dumps({"loc": {"error": "still down"}})
    _ran.clear()
    ev, still = H.revive(_one, {})
    ok("revive：恢复后还是没响应 → 返回原因（照常报 pueued）", ev == [] and "自动恢复没成功" in still.get("loc", ""))
    _ran.clear()
    H.revive(dict(_down, ok={"loc": True, PH: False}), {})
    ok("revive：连不上的机器不动", [x[0] for x in _ran] == [None])
    _ran.clear()
    H.policy.PAUSE = {PH: (None, "停机")}
    H.revive(_down, {})
    ok("revive：暂停着的机器不动", [x[0] for x in _ran] == [None])
finally:
    H.run_on, H.sh, _EV.add = _ro, _sh, _EV_add
    H.policy.AUTO.pop("revive", None)
    H.policy.PAUSE = {}
    del H.M.MACHINES["loc"]

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
