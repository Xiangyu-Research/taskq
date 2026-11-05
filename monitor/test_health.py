#!/usr/bin/env python3
"""测 health.py 的检查和推送规则（2026-10-16）：假的队列、训练日志、时钟，不连任何机器、不真的推送。

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
os.environ["PHD_TASKS"] = os.path.join(fixture.tmpdir(), "tasks.json")  # 不碰真的任务记录
os.environ["PHD_LAST"] = os.path.join(fixture.tmpdir(), "last")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "queue"))
import health as H  # noqa: E402
import shift  # noqa: E402
import cfg  # noqa: E402
QR = cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）
_sd = fixture.tmpdir()
shift.LOG, shift.STATE = f"{_sd}/refill.log", f"{_sd}/shift.json"  # 不写真的 ~/refill.log、状态文件夹

H.policy.OFFLINE = {}  # 不受配置里的暂停检查影响

NOW = datetime(2026, 10, 16, 12, 0)
MET2 = "D116_gamma/V_save/2"
MEN0 = "D111_beta/V_save_n470/0"
fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def pt(dt):  # pueue 的时间格式
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000000000+08:00")


def prog(final=False, epochs=100, end=NOW):
    return dict(final=final, runs=[dict(start=ts(end - timedelta(hours=5)), end=ts(end), epochs_done=epochs, tl=[])], dice={}, debug={})


def fields(status):
    """qs 从 status 拆出的 result、code（如 "Failed (143)" → Failed、143）。"""
    m = re.fullmatch(r"(\w+)(?: \((-?\d+)\))?", status)
    done = m[1] not in H.LIVE
    return dict(result=m[1] if done else None, code=int(m[2]) if m[2] else 0 if m[1] == "Success" else None)


def task(i, group, status, cmd, label="", start=NOW - timedelta(hours=3), end=None):
    return dict(id=i, group=group, status=status, **fields(status), label=label, deps=[], start=pt(start) if start else None,
                end=pt(end) if end else None, command=cmd)


LAB_BUSY = {"groups": {}, "tasks": [dict(id=90, group="gpu1", status="Running", label="busy_a", deps=[], start=None, end=None,
                                         command=f"bash {QR}/run_task.sh toy ds=D101_alpha variant=V_save frag=2"),
                                    dict(id=91, group="gpu1", status="Queued", label="busy_b", deps=[], start=None, end=None,
                                         command=f"bash {QR}/run_task.sh toy ds=D101_alpha variant=V_save frag=3")]}


def snap(**kw):
    s = dict(now=NOW, ok={"box": True, "lab": True},
             probe={"box": {"DISK": str(int(500e9)), "BOOT": "2026-10-01 08:00:00|"},
                    "lab": {"DISK": str(int(200e9)), "GPULOG": ts(NOW - timedelta(minutes=1)), "BOOT": "2026-10-01 08:00:00|"}},
             src={"box": {"tasks": {}}, "lab": {"tasks": {}}},
             pueue={"box": {"groups": {}, "tasks": []}, "lab": LAB_BUSY})
    s.update(kw)
    return s


def ids(s):
    return set(H.check(s))


# ---------------- 检查 ----------------
ok("一切正常时没有问题", ids(snap()) == set())

s = snap(pueue={"box": {"groups": {}, "tasks": [
    task(1, "gpu", "Failed (1)", "bash x", "infer_a", end=NOW - timedelta(hours=1)),
    task(2, "cpu", "DependencyFailed", "python y", "oof_a", end=NOW - timedelta(hours=1)),
    task(3, "gpu", "Failed (1)", "bash z", "old", end=NOW - timedelta(hours=30)),
    task(4, "cpu", "Failed (143)", "bash w", "stopped", end=NOW - timedelta(hours=1)),
    task(5, "cpu", "Killed", "bash v", "killed", end=NOW - timedelta(hours=1)),
    task(6, "gpu", "Failed (137)", "bash u", "oom", end=NOW - timedelta(hours=1)),
    task(8, "gpu", "Killed", "bash t", "worker:w1", end=NOW - timedelta(hours=1)),
    task(9, "gpu", "Failed (1)", "bash s", "worker:w2", end=NOW - timedelta(hours=1))]}})
P = H.check(s)
ok("任务失败、依赖失败都报", any(p.startswith("pueue:box:1:") for p in P) and any(p.startswith("pueue:box:2:") for p in P))
ok("24 小时前的失败不再报", not any(p.startswith("pueue:box:3:") for p in P))
ok("手动停的（143、pueue kill）只记不推", [p["level"] for k, p in P.items() if k.startswith(("pueue:box:4:", "pueue:box:5:"))] == ["info"] * 2)
ok("训练命令被信号杀掉（137，多半是内存不够）照常报", [p["level"] for k, p in P.items() if k.startswith("pueue:box:6:")] == ["normal"])
ok("调度停的 worker（时段结束、让卡）不报；worker 出错照常报", not any(p.startswith("pueue:box:8:") for p in P)
   and any(p.startswith("pueue:box:9:") for p in P))

lab_train = task(7, "gpu1", "Running", f"bash {QR}/run_task.sh toy ds=D111_beta variant=V_save_n470 frag=0", "t15b_f0")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_train]}},
         src={"box": {"tasks": {}}, "lab": {"tasks": {MEN0: prog(end=NOW - timedelta(hours=2))}}})
ok("lab 训练 90 分钟没有新 epoch → 卡住", "stuck:lab:7" in ids(s))
s["src"]["lab"]["tasks"][MEN0] = prog(end=NOW - timedelta(minutes=5))
ok("lab 训练正常时不报", "stuck:lab:7" not in ids(s))
s["src"]["lab"]["errors"] = {MEN0: "OSError: [Errno 5] Input/output error"}
ok("读任务状态出错（spec.collect 的 errors，11/4）→ 单独报，不算读不动", "readerr:lab" in ids(s) and "unread:lab" not in ids(s))


# ---------------- 总队列分给执行机（10/31）----------------
def tqrow(key, state, worker=None, since=None, log_age=None, kind="toy", task_log_age=None, out_age=None):
    return dict(key=key, state=state, worker=worker, host=worker, since=since, beat=None, note="", log_age=log_age, failures=0,
                kind=kind, nice=0, order=1, task_log_age=task_log_age, out_age=out_age)


old = NOW.timestamp() - 3 * 3600  # 领了 3 小时
lab_men = task(7, "gpu1", "Running", f"bash {QR}/run_task.sh toy ds=D111_beta variant=V_save_n470 frag=0", "t15b_f0")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_men]}},
         src={"box": {"tasks": {}}, "lab": {"tasks": {MEN0: prog(end=NOW - timedelta(minutes=5))}}},
         taskq={"box": dict(enabled=True, tasks=[dict(tqrow(MEN0, "claimed", "lab", since=old), assigned="lab")])})
P = H.check(s)
ok("总队列分给 lab、lab 在训：不算同一折两处，也不算卡住", f"dup:{MEN0}" not in P and not [k for k in P if k.startswith("stuck:") and MEN0 in k])
INF = "infer/D103_alpha_one/x/frag_2"

# 总队列在 box（10/31）
ok("box 的总队列读不到 → 报", "taskq:box" in H.check(snap(taskq={"box": None})))
ok("总队列关着（没有 ENABLED）→ 持续 60 分钟后报", H.check(snap(taskq={"box": dict(enabled=False, tasks=[])})).get("tqoff:box", {}).get("after") == 60)
bq = [dict(tqrow(INF, "blocked", None), note="等的 x 失败 2 次"), dict(tqrow(MET2, "claimed", "lab1"), assigned="lab1")]
P = H.check(snap(taskq={"box": dict(enabled=True, paused=False, tasks=bq)}))
ok("总队列里没分出去的任务 blocked → 报；分给执行机的不算同一折两处", f"tqblocked:{INF}" in P and f"dup:{MET2}" not in P)

# ---------------- 通用的任务（10/18）：run_task.sh ----------------
def tk(cmd):
    r = H.C.from_command(cmd)
    return (r["step"], r["key"]) if r else (None, None)


ok("run_task.sh 的训练命令 → key", tk(f"bash {QR}/run_task.sh toy ds=D111_beta "
                                              "variant=V_save_n470 frag=0") == ("train", MEN0))
ok("q 加的、整条命令包在引号里的 run_task.sh 也认", tk(f"bash -c 'bash {QR}/run_task.sh toy "
                                                           "ds=D116_gamma frag=2'") == ("train", MET2))
ok("command 做法的任务 → 它自己给的 key", tk(f"bash {QR}/run_task.sh command key=x/y cmd=true env=base "
                                                     "out=runs/x done=ok") == ("train", "x/y"))
ok("参数写错的 run_task.sh 不当成训练", tk(f"bash {QR}/run_task.sh toy dss=x frag=0") == (None, None))
ok("别的脚本（如老的 fetch_task.sh）不认", H.C.from_command(f"bash {QR}/fetch_task.sh dicc {MET2}") is None)
lab_rt = task(7, "gpu1", "Running", f"bash {QR}/run_task.sh toy ds=D111_beta variant=V_save_n470 frag=0",
              "train:" + MEN0)
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_rt]}},
         src={"box": {"tasks": {}}, "lab": {"tasks": {MEN0: prog(end=NOW - timedelta(hours=2))}}})
ok("lab 上 run_task.sh 的训练 90 分钟没有新 epoch → 卡住", "stuck:lab:7" in ids(s))
import subprocess  # noqa: E402

# ---------------- gather：各卡和卡上的进程、只看本机 ----------------
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
ok("探查脚本里有 GPU 那几行", H.GPU_PROBE in H.probe_script(H.M.MACHINES["box"]))
calls = []
saved = (H.sh, H.run_on, H.spec.remote_collect, H.spec.collect, H.taskq.local_status)
H.taskq.local_status = lambda: dict(enabled=True, tasks=[])  # box 上的总队列（10/31）


def fake_sh(cmd, inp=None, timeout=120):
    calls.append(("sh", cmd))
    if cmd[0] == "git":
        return 0, "abc\n"
    return 0, json.dumps({n: {"groups": {}, "tasks": []} for n in (cmd[2:] or H.M.names("pueue"))})


def fake_run_on(m, script, timeout=120):
    calls.append(("run_on", m["label"]))
    return 0, "BOOT|2026-10-01 08:00:00|\nDISK|123\nGPU|0, GPU-a, 37, 1200\nAPP|GPU-a| 42| 1100|python x|y\n"


H.sh, H.run_on = fake_sh, fake_run_on
H.spec.remote_collect = lambda ssh, specs, root: {"tasks": {}, "seglogs": []}
H.spec.collect = lambda specs, root: {"tasks": {}, "seglogs": []}
g = H.gather()
ok("gather：GPU、APP 行进 gpus，不进 probe；命令行里的 | 原样留着",
   g["probe"]["lab"] == {"BOOT": "2026-10-01 08:00:00|", "DISK": "123"}
   and g["gpus"]["lab"] == [dict(gpu="0", uuid="GPU-a", util="37", mem="1200", apps=[dict(pid="42", mem="1100", args="python x|y")])])
ok("gather：只探查 pueue 机器（表里别的机器不连）", set(g["ok"]) == set(H.M.names("pueue"))
   and {c[1] for c in calls if c[0] == "run_on"} == {H.M.MACHINES[n]["label"] for n in H.M.names("pueue")})
calls.clear()
H.policy.OFFLINE = {"lab": "停机维护"}
g = H.gather()
P_off = H.check(dict(g, now=NOW))
ok("gather：暂停检查的机器不连（不探查），算连不上，但不报 down",
   not [c for c in calls if c[0] == "run_on" and c[1] == H.M.MACHINES["lab"]["label"]] and g["ok"]["lab"] is False
   and "暂停检查" in g["why"]["lab"] and "down:lab" not in P_off)
H.policy.OFFLINE = {}
calls.clear()
g = H.gather(remote=False)
qs_call = next(c[1] for c in calls if c[0] == "sh" and c[1][0] != "git")
ok("gather(remote=False)：只探查本机，qs 只问本机",
   [c for c in calls if c[0] == "run_on"] == [("run_on", "box")] and qs_call[1:] == ["--json", "box"]
   and set(g["ok"]) == {"box"} and set(g["taskq"]) == {"box"} and set(g["pueue"]) == {"box"})
H.sh, H.run_on, H.spec.remote_collect, H.spec.collect, H.taskq.local_status = saved

s = snap()
s["probe"]["lab"]["DISK"] = str(int(10e9))
s["probe"]["box"]["DISK"] = str(int(50e9))
P = H.check(s)
ok("lab 盘不到 15 GB → 紧急；box 盘不到 100 GB → 提醒", P["disk:lab"]["level"] == "urgent" and P["disk:box"]["level"] == "normal")
s["probe"]["lab"]["GPULOG"] = ts(NOW - timedelta(minutes=30))
P = H.check(s)
ok("GPU 记录停了 → 报", "gpulog:lab" in P)

s = snap(board_pending=["data/demo_v50.json"])
s["probe"]["box"]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 开机：放回队列 3 4"
P = H.check(s)
ok("box 重启过 → 报（带上放回的任务），lab 很久前重启的不报", [p["msg"] for k, p in P.items() if k.startswith("boot:")] == ["box 10-16 10:00 重启过：开机：放回队列 3 4"])
ok("看板有结果等确认 → 报", "board" in P)


# ---------------- 第四台机器：只在表里加一项 ----------------
H.M.MACHINES["fake"] = dict(label="fake", hostname=r"fake-host", ssh="fake", scheduler="pueue", data="~/BT", conda=("~/c", "e"),
                            groups={"gpu0": (1, "0", {})}, roles=("train",), disk=("/data", 50, 10), gpu_log=False, repo="~/R")
fk = task(3, "gpu0", "Running", f"bash {QR}/run_task.sh toy ds=D111_beta variant=V_save_n470 frag=0", "t15b_f0_fake")
s = snap(pueue={"box": {"groups": {}, "tasks": []}, "lab": {"groups": {}, "tasks": [lab_train]}, "fake": {"groups": {}, "tasks": [fk]}},
         src={"box": {"tasks": {}}, "fake": {"tasks": {}},
              "lab": {"tasks": {MEN0: prog(end=NOW - timedelta(minutes=5))}}})
s["ok"]["fake"] = True
s["probe"]["fake"] = {"DISK": str(int(5e9)), "BOOT": "2026-10-01 08:00:00|"}
P = H.check(s)
ok("第四台机器：同一折和 lab 两边都在训 → 紧急", P.get(f"dup:{MEN0}", {}).get("level") == "urgent")
ok("第四台机器：盘按表里的字段查", P["disk:fake"]["level"] == "urgent" and "gpulog:fake" not in P)
s["ok"]["fake"] = False
ok("第四台机器连不上：报", "down:fake" in ids(s))
del H.M.MACHINES["fake"]

# ---------------- 推送规则（10/21：白话、并成一条、不推已恢复和汇总）----------------
d = fixture.tmpdir()
H.STATE, H.LOG = f"{d}/state.json", f"{d}/health.log"
sent = []
H.push = lambda msg, **kw: sent.append((kw.get("title", ""), msg))


def run(P, now):
    sent.clear()
    H.update({k: dict(v) for k, v in P.items()}, now)
    return list(sent)


A = {"disk:lab": dict(msg="lab 盘", level="normal", kind="state", after=0)}
U = {"dup:x": dict(msg="两处", level="urgent", kind="state", after=0)}
day1 = datetime(2026, 10, 16, 12, 0)
out = run(A, day1)
ok("白天新问题：推送一条白话", len(out) == 1 and out[0][0] == f"{H.NAME}：需要处理" and "盘满了" in out[0][1])
ok("同一个问题不重复推送", run(A, day1 + timedelta(minutes=30)) == [])
ok("恢复后不推送", run({}, day1 + timedelta(hours=1)) == [])
ok("夜里普通问题不推送", run(A, datetime(2026, 10, 16, 23, 0)) == [])
out = run({**A, **U}, datetime(2026, 10, 17, 1, 0))
ok("夜里紧急问题即时推送", len(out) == 1 and out[0][0] == f"{H.NAME}：紧急" and "两处训练" in out[0][1])
out = run({**A, **U}, datetime(2026, 10, 17, 8, 5))
ok("早上第一次检查推送夜里攒下的", len(out) == 1 and "盘满了" in out[0][1] and "两处" not in out[0][1])
ok("推过之后不再推送", run(A, datetime(2026, 10, 17, 8, 35)) == [])
D = {"down:dicc": dict(msg="DICC 连不上", level="normal", kind="state", after=60)}
ok("要持续一段时间才报的，刚出现时不推送", run({**A, **D}, datetime(2026, 10, 17, 9, 0)) == [])
out = run({**A, **D}, datetime(2026, 10, 17, 10, 0))
ok("持续够久后推送", len(out) == 1 and "DICC 读不到状态了" in out[0][1])
B = {"code:lab": dict(msg="g", level="normal", kind="state", after=0), "gpulog:lab": dict(msg="l", level="normal", kind="state", after=0)}
out = run(B, datetime(2026, 10, 17, 10, 30))
ok("同一次查到几件：并成一条", len(out) == 1 and "（2 件）" in out[0][0] and out[0][1].count("\n- ") == 1)
I = {"idle:lab:gpu1": dict(msg="只剩在训的", level="info", kind="state", after=0)}
ok("info 级不推送", run(I, datetime(2026, 10, 17, 11, 0)) == [])

# ---------------- feed / advance 出错不让健康检查停掉 ----------------
import types  # noqa: E402
got = {}
H.gather, H.update = (lambda: snap()), (lambda P, now, dry=False: got.update(P))
fd = types.ModuleType("feed")
feed_calls = []
fd.run_all = lambda s, dry=False: (feed_calls.append(dry), ([], {"x:y": "z"}))[1]
sys.modules["feed"] = fd
sys.modules["relay"] = None  # health.py 不直接调 relay.py（10/31：送任务只从 feed.run_all 进）
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
ok("白话：任务 key → 实验名 + 简称（如 f0）", PL.fold(_k) == f"{_n} {EX.short(_t)}" and EX.short(_t))
ok("白话：每个任务的步骤的标签 → (实验名, 步骤, 简称)；实验一级的 → 简称 None",
   PL.label(f"eval:{EX.label(_e, _t)}") == (_n, PL.STEP["eval"], EX.short(_t)) and PL.label(f"eval:{_e['id']}") == (_n, PL.STEP["eval"], None))
ok("白话：认不出的标签 → 其他", PL.label("eval:nope_f1") == ("其他", "eval:nope_f1", None))
_pm = "envs/policy.py PLACE['x'] 的 'y'：不认识的地方。能写的：trainer。训完以后的步骤这次一个都没排；改好、提交推送后下一次检查自动恢复"
ok("白话：PLACE 写错（advance:policy）说清哪里错、怎么恢复，不当成某个任务的步骤出错",
   PL.problem("advance:policy", dict(msg=_pm)).startswith("训完以后的步骤停了") and "能写的：trainer" in PL.problem("advance:policy", dict(msg=_pm)))
ok("白话：推送里的一句话带简称", PL.task(f"infer:{EX.label(_e, _t)}") == f"{_n} {EX.short(_t)} 的{PL.STEP['infer']}")

# 白话从结构化的 data 说（11/4，不再从说明里抠数字）：每种问题的推送文字和改之前逐字相同
_ki = "infer/" + _k
_lt0 = task(7, "gpu1", "Running", f"bash {QR}/run_task.sh toy ds=D111_beta variant=V_save_n470 frag=0", "train:" + MEN0)
_inf = task(1, "gpu", "Failed (1)", "bash x", f"infer:{EX.label(_e, _t)}", end=NOW - timedelta(hours=1))
s = snap(pueue={"box": {"groups": {}, "tasks": [_inf, task(2, "cpu", "Failed (1)", "bash y", "", end=NOW - timedelta(hours=1))]},
                "lab": {"groups": {}, "tasks": [_lt0]}},
         src={"box": {"tasks": {}}, "lab": {"tasks": {MEN0: prog(end=NOW - timedelta(hours=2))}, "stuck": ["a/b/0", "c/d/1"]}},
         taskq={"box": dict(enabled=True, tasks=[dict(tqrow(_ki, "blocked"), note="等的 x 失败 2 次")])},
         board_pending=["a.json", "b.json", "c.json"])
s["probe"]["lab"]["DISK"] = str(int(10e9))
s["probe"]["box"]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 开机：放回队列 3 4"
P = H.check(s)
s = snap(pueue={"box": {"groups": {}, "tasks": [task(9, "gpu", "Running", _lt0["command"], "y")]},
                "lab": {"groups": {}, "tasks": [task(8, "gpu1", "Queued", _lt0["command"], "x")]}})
_dup = f"dup:{MEN0}"
_once = H.check(s)[_dup]
s["pueue"]["lab"]["tasks"][0]["status"] = "Running"
s["src"]["lab"]["tasks"] = {MEN0: prog(end=NOW - timedelta(minutes=5))}
_both = H.check(s)[_dup]
_rl = {f"site:lab1:fail:{MEN0}": dict(msg="…训练失败 2 次，有进度…", data=dict(progress=True, failures=2)),
       f"site:lab1:fail:{_ki}": dict(msg="…failed…", data=dict(progress=False)),
       "site:lab1:orphan:zz": dict(msg="…认不出…", data=dict(unknown=True)),
       f"site:lab1:orphan:{MEN0}": dict(msg="…不是它领着…", data=dict(unknown=False))}
P.update({k: H.from_step(v) for k, v in _rl.items()})
_f, _l1, _pe = PL.fold(MEN0), H.M.machine("lab1")["label"], sorted(k for k in P if k.startswith("pueue:box:"))
want = {"unread:lab": "lab 上 2 个任务的文件读不动（多半是那边的存储挂住），这些任务先不分出去，别的照常。",
        "stuck:lab:7": f"lab 上 {_f} 90 分钟没有进展，可能卡住了，GPU 白占着。",
        _pe[0]: f"box 上的 {_n} {EX.short(_t)} 的推理出错停了。",
        _pe[1]: "box 上的一次性任务 #2 出错停了。",
        f"tqblocked:{_ki}": f"{PL.fold(_ki)} 的推理在等的前一步失败了（或不在队列里），不会自己开始。",
        "disk:lab": "lab /mnt/disk1 只剩 10 GB，盘满了训练和存档会失败。",
        "boot:box:" + ts(NOW - timedelta(hours=2)): "box 10-16 10:00 重启过，被打断的任务已自动放回队列，要确认训练接上了。",
        "board": "看板有新结果，要确认后才发布（3 个文件）。",
        f"site:lab1:fail:{MEN0}": f"{_l1} 上 {_f} 训练失败了，有进度，要决定接着训还是放弃。",
        f"site:lab1:fail:{_ki}": f"{_l1} 上 {PL.fold(_ki)} 的任务出了问题，不会开始训练。",
        "site:lab1:orphan:zz": f"{_l1} 的队列里有一个认不出来的任务。",
        f"site:lab1:orphan:{MEN0}": f"{_l1} 在训 {_f}，但总队列里没分给它，可能重复训练。"}
got_ = {k: PL.problem(k, P[k]) for k in want if k in P}
ok("白话：每种问题从 data 说，文字和以前一样", got_ == want and all("data" in P[k] for k in want))
if got_ != want:
    print("\n".join(f"  {k}\n    要 {want[k]}\n    得 {got_.get(k)}" for k in want if got_.get(k) != want[k]))
ok("白话：两处排着 / 两处都在训", PL.problem(_dup, _once) == f"{_f} 在两处排着，要取消一边，不然会重复训练。"
   and PL.problem(_dup, _both) == f"{_f} 同时在两处训练，存档会互相覆盖，要尽快停掉一边。")
ok("白话：没有 data 的（旧状态文件里的）退回原来的说明",
   PL.problem("unread:lab", dict(msg="原来的说明")) == "原来的说明" and PL.problem(f"site:lab1:fail:{MEN0}", dict(msg="m")) == "m")
ok("执行机报的卡住可以是说明或 dict(msg, data)", H.from_step("m") == dict(msg="m", level="normal", kind="state", after=0)
   and H.from_step(dict(msg="m", data=dict(n=1)))["data"] == dict(n=1))
ok("白话：实验表里的步骤都有中文名（STEP 表或步骤的 zh）",
   all(PL.step_name(n) != n for n in PL.table_steps()) and PL.step_name("nope") == "nope")

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
    os.makedirs(os.path.join(home, ".config", "phd-health"), exist_ok=True)
    open(os.path.join(home, ".config", "phd-health", "ntfy_topic"), "w").write("t0\n")
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
_mark = os.path.join(_lt, "home", ".cache", "phd-health", "load_error.txt")
rc3, _ = _health(os.environ["TASKQ_PLUGINS"], 12, "--test")  # 不推真的（urlopen 换掉了）；--help 不做事，不清记录
ok("插件修好后：正常 import，清掉出错记录", rc3 == 0 and not os.path.exists(_mark))
shutil.rmtree(os.path.join(_lt, "home"))
if os.path.exists(os.path.join(_lt, "pushed.log")):
    os.remove(os.path.join(_lt, "pushed.log"))
rc4, p4 = _health(_bad, 23)
ok("插件写错、夜里：不推送（早上第一次检查时推）", rc4 != 0 and p4 == [])

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
