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
os.environ["TASKQ_TASKS"] = os.path.join(fixture.tmpdir(), "tasks.json")  # 不碰真的任务记录
os.environ["TASKQ_LAST"] = os.path.join(fixture.tmpdir(), "last")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "queue"))
import health as H  # noqa: E402
import shift  # noqa: E402
import cfg  # noqa: E402
QR = cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）
_sd = fixture.tmpdir()
shift.LOG, shift.STATE = f"{_sd}/refill.log", f"{_sd}/shift.json"  # 不写真的 ~/refill.log、状态文件夹
shift.group_tasks = lambda *a, **k: (None, None)  # 按时段开关不去那边查（11/5 前会 ssh 到真的 pueue-host 查 pueue）

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


PH_BUSY = {"groups": {}, "tasks": [dict(id=90, group="gpu1", status="Running", label="busy_a", deps=[], start=None, end=None,
                                         command=f"bash {QR}/run_task.sh toy ds=D101_alpha variant=V_save frag=2"),
                                    dict(id=91, group="gpu1", status="Queued", label="busy_b", deps=[], start=None, end=None,
                                         command=f"bash {QR}/run_task.sh toy ds=D101_alpha variant=V_save frag=3")]}


def snap(**kw):
    s = dict(now=NOW, ok={"hub": True, "pueue-host": True},
             probe={"hub": {"DISK": str(int(500e9)), "BOOT": "2026-10-01 08:00:00|"},
                    "pueue-host": {"DISK": str(int(200e9)), "GPULOG": ts(NOW - timedelta(minutes=1)), "BOOT": "2026-10-01 08:00:00|"}},
             src={"hub": {"tasks": {}}, "pueue-host": {"tasks": {}}},
             pueue={"hub": {"groups": {}, "tasks": []}, "pueue-host": PH_BUSY})
    s.update(kw)
    return s


def ids(s):
    return set(H.check(s))


# ---------------- 检查 ----------------
ok("一切正常时没有问题", ids(snap()) == set())

s = snap(pueue={"hub": {"groups": {}, "tasks": [
    task(1, "gpu", "Failed (1)", "bash x", "infer_a", end=NOW - timedelta(hours=1)),
    task(2, "cpu", "DependencyFailed", "python y", "oof_a", end=NOW - timedelta(hours=1)),
    task(3, "gpu", "Failed (1)", "bash z", "old", end=NOW - timedelta(hours=30)),
    task(4, "cpu", "Failed (143)", "bash w", "stopped", end=NOW - timedelta(hours=1)),
    task(5, "cpu", "Killed", "bash v", "killed", end=NOW - timedelta(hours=1)),
    task(6, "gpu", "Failed (137)", "bash u", "oom", end=NOW - timedelta(hours=1)),
    task(8, "gpu", "Killed", "bash t", "worker:w1", end=NOW - timedelta(hours=1)),
    task(9, "gpu", "Failed (1)", "bash s", "worker:w2", end=NOW - timedelta(hours=1))]}})
P = H.check(s)
ok("任务失败、依赖失败都报", any(p.startswith("pueue:hub:1:") for p in P) and any(p.startswith("pueue:hub:2:") for p in P))
ok("24 小时前的失败不再报", not any(p.startswith("pueue:hub:3:") for p in P))
_rr = [dict(machine="hub", id=i, label="relay_fetch:k", step="relay_fetch", key="k", added=0.0) for i in (21, 22)]
_P2 = H.check(snap(records=_rr, pueue={"hub": {"groups": {}, "tasks": [
    task(21, "wait", "Failed (1)", "x", "relay_fetch:k", end=NOW - timedelta(hours=1)),
    task(22, "wait", "Running", "x", "relay_fetch:k")]}}))
ok("出错后同一步又排过的旧任务不报", not any(p.startswith("pueue:hub:21:") for p in _P2))
ok("手动停的（143、pueue kill）只记不推", [p["level"] for k, p in P.items() if k.startswith(("pueue:hub:4:", "pueue:hub:5:"))] == ["info"] * 2)
ok("训练命令被信号杀掉（137，多半是内存不够）照常报", [p["level"] for k, p in P.items() if k.startswith("pueue:hub:6:")] == ["normal"])
ok("调度停的 worker（时段结束、让卡）不报；worker 出错照常报", not any(p.startswith("pueue:hub:8:") for p in P)
   and any(p.startswith("pueue:hub:9:") for p in P))

ph_train = task(7, "gpu1", "Running", f"bash {QR}/run_task.sh toy ds=D111_beta variant=V_save_n470 frag=0", "t15b_f0")
s = snap(pueue={"hub": {"groups": {}, "tasks": []}, "pueue-host": {"groups": {}, "tasks": [ph_train]}},
         src={"hub": {"tasks": {}}, "pueue-host": {"tasks": {MEN0: prog(end=NOW - timedelta(hours=2))}}})
ok("pueue-host 训练 90 分钟没有新 epoch → 卡住", "stuck:pueue-host:7" in ids(s))
s["src"]["pueue-host"]["tasks"][MEN0] = prog(end=NOW - timedelta(minutes=5))
ok("pueue-host 训练正常时不报", "stuck:pueue-host:7" not in ids(s))
s["src"]["pueue-host"]["errors"] = {MEN0: "OSError: [Errno 5] Input/output error"}
ok("读任务状态出错（spec.collect 的 errors，11/4）→ 单独报，不算读不动", "readerr:pueue-host" in ids(s) and "unread:pueue-host" not in ids(s))


# ---------------- 总队列分给执行机（10/31）----------------
def tqrow(key, state, worker=None, since=None, log_age=None, kind="toy", task_log_age=None, out_age=None):
    return dict(key=key, state=state, worker=worker, host=worker, since=since, beat=None, note="", log_age=log_age, failures=0,
                kind=kind, nice=0, order=1, task_log_age=task_log_age, out_age=out_age)


old = NOW.timestamp() - 3 * 3600  # 领了 3 小时
ph_men = task(7, "gpu1", "Running", f"bash {QR}/run_task.sh toy ds=D111_beta variant=V_save_n470 frag=0", "t15b_f0")
s = snap(pueue={"hub": {"groups": {}, "tasks": []}, "pueue-host": {"groups": {}, "tasks": [ph_men]}},
         src={"hub": {"tasks": {}}, "pueue-host": {"tasks": {MEN0: prog(end=NOW - timedelta(minutes=5))}}},
         taskq={"hub": dict(enabled=True, tasks=[dict(tqrow(MEN0, "claimed", "pueue-host", since=old), assigned="pueue-host")])})
P = H.check(s)
ok("总队列分给 pueue-host、pueue-host 在训：不算同一折两处，也不算卡住", f"dup:{MEN0}" not in P and not [k for k in P if k.startswith("stuck:") and MEN0 in k])
INF = "infer/D103_alpha_one/x/frag_2"

# 总队列在 hub（10/31）
ok("hub 的总队列读不到 → 报", "taskq:hub" in H.check(snap(taskq={"hub": None})))
ok("总队列关着（没有 ENABLED）→ 持续 60 分钟后报", H.check(snap(taskq={"hub": dict(enabled=False, tasks=[])})).get("tqoff:hub", {}).get("after") == 60)
bq = [dict(tqrow(INF, "blocked", None), note="等的 x 失败 2 次"), dict(tqrow(MET2, "claimed", "site-pueue"), assigned="site-pueue")]
P = H.check(snap(taskq={"hub": dict(enabled=True, paused=False, tasks=bq)}))
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
ok("别的脚本（如老的 fetch_task.sh）不认", H.C.from_command(f"bash {QR}/fetch_task.sh slurm-login {MET2}") is None)
ph_rt = task(7, "gpu1", "Running", f"bash {QR}/run_task.sh toy ds=D111_beta variant=V_save_n470 frag=0",
              "train:" + MEN0)
s = snap(pueue={"hub": {"groups": {}, "tasks": []}, "pueue-host": {"groups": {}, "tasks": [ph_rt]}},
         src={"hub": {"tasks": {}}, "pueue-host": {"tasks": {MEN0: prog(end=NOW - timedelta(hours=2))}}})
ok("pueue-host 上 run_task.sh 的训练 90 分钟没有新 epoch → 卡住", "stuck:pueue-host:7" in ids(s))
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
ok("探查脚本里有 GPU 那几行", H.GPU_PROBE in H.probe_script(H.M.MACHINES["hub"]))
calls = []
saved = (H.sh, H.run_on, H.spec.remote_collect, H.spec.collect, H.taskq.local_status)
H.taskq.local_status = lambda: dict(enabled=True, tasks=[])  # hub 上的总队列（10/31）


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
   g["probe"]["pueue-host"] == {"BOOT": "2026-10-01 08:00:00|", "DISK": "123"}
   and g["gpus"]["pueue-host"] == [dict(gpu="0", uuid="GPU-a", util="37", mem="1200", apps=[dict(pid="42", mem="1100", args="python x|y")])])
ok("gather：只探查 pueue 机器（表里别的机器不连）", set(g["ok"]) == set(H.M.names("pueue"))
   and {c[1] for c in calls if c[0] == "run_on"} == {H.M.MACHINES[n]["label"] for n in H.M.names("pueue")})
calls.clear()
H.policy.OFFLINE = {"pueue-host": "停机维护"}
g = H.gather()
P_off = H.check(dict(g, now=NOW))
ok("gather：暂停检查的机器不连（不探查），算连不上，但不报 down",
   not [c for c in calls if c[0] == "run_on" and c[1] == H.M.MACHINES["pueue-host"]["label"]] and g["ok"]["pueue-host"] is False
   and "暂停检查" in g["why"]["pueue-host"] and "down:pueue-host" not in P_off)
H.policy.OFFLINE = {}
calls.clear()
g = H.gather(remote=False)
qs_call = next(c[1] for c in calls if c[0] == "sh" and c[1][0] != "git")
ok("gather(remote=False)：只探查本机，qs 只问本机",
   [c for c in calls if c[0] == "run_on"] == [("run_on", "hub")] and qs_call[1:] == ["--json", "hub"]
   and set(g["ok"]) == {"hub"} and set(g["taskq"]) == {"hub"} and set(g["pueue"]) == {"hub"})
H.sh, H.run_on, H.spec.remote_collect, H.spec.collect, H.taskq.local_status = saved

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
ok("notice clear: missing ones are not an error; list as text", r7.returncode == 0 and "no notice nope" in r7.stdout
   and r8.stdout.startswith("board (") and "看板有新结果 等确认" in r8.stdout and "b-2_x" not in r8.stdout)
ok("notice --help", tqn("--help").returncode == 0 and "set" in tqn("--help").stdout)
r9 = tqn("set", "later", "--at", "3d", "do it later")
ok("notice set --at: listed with its time, not reported before then",
   r9.returncode == 0 and NT.notices(_db, due_only=False)["later"]["at"] > NT.time.strftime(NT.TF) and "later" not in NT.notices(_db))
tqn("clear", "later")
saved = H.sh, H.run_on, H.spec.remote_collect, H.spec.collect
H.sh, H.run_on = fake_sh, fake_run_on
H.spec.remote_collect = lambda ssh, specs, root: {"tasks": {}, "seglogs": []}
H.spec.collect = lambda specs, root: {"tasks": {}, "seglogs": []}
g = H.gather(remote=False)
ok("gather: notices → s['notices']", g["notices"] == {"board": "看板有新结果 等确认"})
tqn("clear", "board")
ok("gather: gone once cleared", H.gather(remote=False)["notices"] == {})
H.sh, H.run_on, H.spec.remote_collect, H.spec.collect = saved

# old files are taken over once
_hd = fixture.tmpdir()
os.makedirs(os.path.join(_hd, H.proto.NOTICES), exist_ok=True)
json.dump({"checked": "2026-11-05 13:00", "problems": {"boot:hub:2026-11-05 13:52:30": {"msg": "x", "kind": "event"},
                                                        "disk:hub": {"msg": "y", "kind": "state"}}},
          open(os.path.join(_hd, "state.json"), "w"))
json.dump({"gone:1": "2026-11-05 10:00:00"}, open(os.path.join(_hd, H.proto.ACKED), "w"))
open(os.path.join(_hd, H.proto.NOTICES, "old.txt"), "w").write("old notice\n")
_wdb = NT.open_db()
NT.adopt_files(_wdb, _hd)
ok("old state.json / acked.json / notices → database, files renamed *.migrated",
   sorted(NT.problems(_wdb)[0]) == ["boot:hub:2026-11-05 13:52:30", "disk:hub"] and NT.problems(_wdb)[1] == "2026-11-05 13:00"
   and NT.acked(_wdb) == {"gone:1": "2026-11-05 10:00:00"} and NT.notices(_wdb)["old"]["text"] == "old notice"
   and os.path.exists(os.path.join(_hd, "state.json.migrated")) and not os.path.exists(os.path.join(_hd, "state.json")))
NT.drop(_wdb, "old")

# taskq notice ack: only kind=event; acked ones are left out; acks of gone problems are dropped
ra, rb, rc_ = tqn("ack", "boot:hub"), tqn("ack", "disk:hub"), tqn("ack", "nosuch")
ok("notice ack: prefix finds the event; ongoing or unknown problems are not acked (exit 1)",
   ra.returncode == 0 and "boot:hub:2026-11-05 13:52:30" in NT.acked(_wdb) and rb.returncode == 1 and "ongoing" in rb.stderr
   and rc_.returncode == 1 and "no such problem" in rc_.stderr)
_P = {"boot:hub:2026-11-05 13:52:30": H.problem("x", kind="event"), "disk:hub": H.problem("y")}
ok("drop_acked: acked events left out, others kept", list(H.drop_acked(dict(_P), _wdb)) == ["disk:hub"])
NT.save_problems(_wdb, {"disk:hub": H.problem("y")}, "2026-11-05 14:00")
ok("an ack outlives the save of the filtered list: still left out on the next check (11/5: it came back one check later)",
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
s["probe"]["pueue-host"]["DISK"] = str(int(10e9))
s["probe"]["hub"]["DISK"] = str(int(50e9))
P = H.check(s)
ok("pueue-host 盘不到 15 GB → 紧急；hub 盘不到 100 GB → 提醒", P["disk:pueue-host"]["level"] == "urgent" and P["disk:hub"]["level"] == "normal")
s["probe"]["pueue-host"]["GPULOG"] = ts(NOW - timedelta(minutes=30))
P = H.check(s)
ok("GPU 记录停了 → 报", "gpulog:pueue-host" in P)

s = snap(notices={"board": "看板有新结果等确认后发布"})
s["probe"]["hub"]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 开机：放回队列 3 4"
P = H.check(s)
ok("hub 重启过 → 报（带上放回的任务），pueue-host 很久前重启的不报", [p["msg"] for k, p in P.items() if k.startswith("boot:")] == ["hub 10-16 10:00 重启过：开机：放回队列 3 4"])
ok("挂着的提醒 → 每条报一个 notice:<名字>，说明是提醒的文字",
   P.get("notice:board", {}).get("msg") == "看板有新结果等确认后发布" and P["notice:board"]["data"] == dict(name="board")
   and "board" not in P)


# ---------------- 第四台机器：只在表里加一项 ----------------
H.M.MACHINES["fake"] = dict(label="fake", hostname=r"fake-host", ssh="fake", scheduler="pueue", data="~/BT", conda=("~/c", "e"),
                            groups={"gpu0": (1, "0", {})}, roles=("train",), disk=("/data", 50, 10), gpu_log=False, repo="~/R")
fk = task(3, "gpu0", "Running", f"bash {QR}/run_task.sh toy ds=D111_beta variant=V_save_n470 frag=0", "t15b_f0_fake")
s = snap(pueue={"hub": {"groups": {}, "tasks": []}, "pueue-host": {"groups": {}, "tasks": [ph_train]}, "fake": {"groups": {}, "tasks": [fk]}},
         src={"hub": {"tasks": {}}, "fake": {"tasks": {}},
              "pueue-host": {"tasks": {MEN0: prog(end=NOW - timedelta(minutes=5))}}})
s["ok"]["fake"] = True
s["probe"]["fake"] = {"DISK": str(int(5e9)), "BOOT": "2026-10-01 08:00:00|"}
P = H.check(s)
ok("第四台机器：同一折和 pueue-host 两边都在训 → 紧急", P.get(f"dup:{MEN0}", {}).get("level") == "urgent")
ok("第四台机器：盘按表里的字段查", P["disk:fake"]["level"] == "urgent" and "gpulog:fake" not in P)
s["ok"]["fake"] = False
ok("第四台机器连不上：报", "down:fake" in ids(s))
del H.M.MACHINES["fake"]

# ---------------- 推送规则（10/21：白话、并成一条、不推已恢复和汇总）----------------
_wdb = NT.open_db()
sent = []
H.push = lambda msg, **kw: sent.append((kw.get("title", ""), msg))


def run(P, now):
    sent.clear()
    H.update({k: dict(v) for k, v in P.items()}, now, _wdb)
    return list(sent)


A = {"disk:pueue-host": dict(msg="pueue-host 盘", level="normal", kind="state", after=0)}
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
D = {"down:slurm-login": dict(msg="Slurm 连不上", level="normal", kind="state", after=60)}
ok("要持续一段时间才报的，刚出现时不推送", run({**A, **D}, datetime(2026, 10, 17, 9, 0)) == [])
out = run({**A, **D}, datetime(2026, 10, 17, 10, 0))
ok("持续够久后推送", len(out) == 1 and "Slurm 读不到状态了" in out[0][1])
B = {"code:pueue-host": dict(msg="g", level="normal", kind="state", after=0), "gpulog:pueue-host": dict(msg="l", level="normal", kind="state", after=0)}
out = run(B, datetime(2026, 10, 17, 10, 30))
ok("同一次查到几件：并成一条", len(out) == 1 and "（2 件）" in out[0][0] and out[0][1].count("\n- ") == 1)
I = {"idle:pueue-host:gpu1": dict(msg="只剩在训的", level="info", kind="state", after=0)}
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
ok("白话：任务 key → 实验名 + 简称（如 f0）", PL.key_name(_k) == f"{_n} {EX.short(_t)}" and EX.short(_t))
ok("白话：每个任务的步骤的标签 → (实验名, 步骤, 简称)；实验一级的 → 简称 None",
   PL.label(f"eval:{EX.label(_e, _t)}") == (_n, PL.STEPS["eval"], EX.short(_t)) and PL.label(f"eval:{_e['id']}") == (_n, PL.STEPS["eval"], None))
ok("白话：认不出的标签 → 其他", PL.label("eval:nope_f1") == ("其他", "eval:nope_f1", None))
_pm = "policy.py PLACE['x'] 的 'y'：不认识的地方。能写的：trainer。训完以后的步骤这次一个都没排；改好、提交推送后下一次检查自动恢复"
ok("白话：PLACE 写错（advance:policy）说清哪里错、怎么恢复，不当成某个任务的步骤出错",
   PL.problem("advance:policy", dict(msg=_pm)).startswith("训完以后的步骤停了") and "能写的：trainer" in PL.problem("advance:policy", dict(msg=_pm)))
ok("白话：推送里的一句话带简称", PL.task(f"infer:{EX.label(_e, _t)}") == f"{_n} {EX.short(_t)} 的{PL.STEPS['infer']}")

# 白话从结构化的 data 说（11/4，不再从说明里抠数字）：每种问题的推送文字和改之前逐字相同
_ki = "infer/" + _k
_lt0 = task(7, "gpu1", "Running", f"bash {QR}/run_task.sh toy ds=D111_beta variant=V_save_n470 frag=0", "train:" + MEN0)
_inf = task(1, "gpu", "Failed (1)", "bash x", f"infer:{EX.label(_e, _t)}", end=NOW - timedelta(hours=1))
s = snap(pueue={"hub": {"groups": {}, "tasks": [_inf, task(2, "cpu", "Failed (1)", "bash y", "", end=NOW - timedelta(hours=1))]},
                "pueue-host": {"groups": {}, "tasks": [_lt0]}},
         src={"hub": {"tasks": {}}, "pueue-host": {"tasks": {MEN0: prog(end=NOW - timedelta(hours=2))}, "stuck": ["a/b/0", "c/d/1"]}},
         taskq={"hub": dict(enabled=True, tasks=[dict(tqrow(_ki, "blocked"), note="等的 x 失败 2 次")])},
         notices={"board": "看板有新结果，要确认后才发布（3 个文件）。"})
s["probe"]["pueue-host"]["DISK"] = str(int(10e9))
s["probe"]["hub"]["BOOT"] = ts(NOW - timedelta(hours=2)) + "|2026-10-16 10:01:00 开机：放回队列 3 4"
P = H.check(s)
s = snap(pueue={"hub": {"groups": {}, "tasks": [task(9, "gpu", "Running", _lt0["command"], "y")]},
                "pueue-host": {"groups": {}, "tasks": [task(8, "gpu1", "Queued", _lt0["command"], "x")]}})
_dup = f"dup:{MEN0}"
_once = H.check(s)[_dup]
s["pueue"]["pueue-host"]["tasks"][0]["status"] = "Running"
s["src"]["pueue-host"]["tasks"] = {MEN0: prog(end=NOW - timedelta(minutes=5))}
_both = H.check(s)[_dup]
_rl = {f"site:site-pueue:fail:{MEN0}": dict(msg="…训练失败 2 次，有进度…", data=dict(progress=True, failures=2)),
       f"site:site-pueue:fail:{_ki}": dict(msg="…failed…", data=dict(progress=False)),
       "site:site-pueue:orphan:zz": dict(msg="…认不出…", data=dict(unknown=True)),
       f"site:site-pueue:orphan:{MEN0}": dict(msg="…不是它领着…", data=dict(unknown=False))}
P.update({k: H.from_step(v) for k, v in _rl.items()})
_f, _l1, _pe = PL.key_name(MEN0), H.M.machine("site-pueue")["label"], sorted(k for k in P if k.startswith("pueue:hub:"))
want = {"unread:pueue-host": "pueue-host 上 2 个任务的文件读不动（多半是那边的存储挂住），这些任务先不分出去，别的照常。",
        "stuck:pueue-host:7": f"pueue-host 上 {_f} 90 分钟没有进展，可能卡住了，GPU 白占着。",
        _pe[0]: f"hub 上的 {_n} {EX.short(_t)} 的推理出错停了。",
        _pe[1]: "hub 上的一次性任务 #2 出错停了。",
        f"tqblocked:{_ki}": f"{PL.key_name(_ki)} 的推理在等的前一步失败了（或不在队列里），不会自己开始。",
        "disk:pueue-host": "pueue-host /mnt/disk1 只剩 10 GB，盘满了训练和存档会失败。",
        "boot:hub:" + ts(NOW - timedelta(hours=2)): "hub 10-16 10:00 重启过，被打断的任务已自动放回队列，要确认训练接上了。",
        "notice:board": "看板有新结果，要确认后才发布（3 个文件）。",
        f"site:site-pueue:fail:{MEN0}": f"{_l1} 上 {_f} 训练失败了，有进度，要决定接着训还是放弃。",
        f"site:site-pueue:fail:{_ki}": f"{_l1} 上 {PL.key_name(_ki)} 的任务出了问题，不会开始训练。",
        "site:site-pueue:orphan:zz": f"{_l1} 的队列里有一个认不出来的任务。",
        f"site:site-pueue:orphan:{MEN0}": f"{_l1} 在训 {_f}，但总队列里没分给它，可能重复训练。"}
got_ = {k: PL.problem(k, P[k]) for k in want if k in P}
ok("白话：每种问题从 data 说，文字和以前一样", got_ == want and all("data" in P[k] for k in want))
if got_ != want:
    print("\n".join(f"  {k}\n    要 {want[k]}\n    得 {got_.get(k)}" for k in want if got_.get(k) != want[k]))
ok("白话：两处排着 / 两处都在训", PL.problem(_dup, _once) == f"{_f} 在两处排着，要取消一边，不然会重复训练。"
   and PL.problem(_dup, _both) == f"{_f} 同时在两处训练，存档会互相覆盖，要尽快停掉一边。")
ok("白话：没有 data 的（旧状态文件里的）退回原来的说明",
   PL.problem("unread:pueue-host", dict(msg="原来的说明")) == "原来的说明" and PL.problem(f"site:site-pueue:fail:{MEN0}", dict(msg="m")) == "m")
ok("执行机报的卡住可以是说明或 dict(msg, data)", H.from_step("m") == dict(msg="m", level="normal", kind="state", after=0)
   and H.from_step(dict(msg="m", data=dict(n=1)))["data"] == dict(n=1))
ok("执行机报的只是在等的（level=info）只记不推；写到声明以外不推送",
   H.from_step(dict(msg="m", data=dict(n=1), level="info"))["level"] == "info" and "undeclared" in H.QUIET)
ok("白话：实验表里的步骤都有中文名（STEPS：核心的 + 插件 labels.py 的，或步骤的 zh）",
   all(PL.step_name(n) != n for n in PL.table_steps()) and PL.step_name("nope") == "nope")

# 白话的项目部分来自插件 labels.py（11/5）：步骤名、不列的步骤、分析编号的标题；实验名里不提实验表里最多的模型
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
ok("白话：等的前一步失败（tqblocked）按 key 的步骤说，训练任务说任务",
   PL.problem(f"tqblocked:{_ke}", dict(msg="m", data=dict(key=_ke))) == f"{PL.key_name(_ke)} 的评测在等的前一步失败了（或不在队列里），不会自己开始。"
   and PL.problem(f"tqblocked:{_k}", dict(msg="m", data=dict(key=_k))).startswith(f"{PL.key_name(_k)} 的任务在等"))
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

# ---------------- pueued 没了：自动恢复（11/6）----------------
_seen_s = snap(pueue={"hub": {"groups": {"gpu": "Running", "cpu": "Paused", "wait": "Running"}, "tasks": []},
                      "pueue-host": {"error": "Error: connection refused"}})
_sp = os.path.join(H.HDIR, H.proto.PUEUE_SEEN)
if os.path.exists(_sp):
    os.remove(_sp)
H.C.write_json(_sp, {"pueue-host": dict(t=111, running=["gpu1"])})
_seen = H.pueue_seen(_seen_s, dry=True)
ok("pueue_seen：记下 pueued 在的机器的时间和开着的组；没响应的那台留着上次的；dry 不写",
   _seen["hub"] == dict(t=int(NOW.timestamp()), running=["gpu", "wait"]) and _seen["pueue-host"]["t"] == 111
   and json.load(open(_sp)) == {"pueue-host": dict(t=111, running=["gpu1"])})
H.pueue_seen(_seen_s)
ok("pueue_seen：不是 dry 时写下", json.load(open(_sp))["hub"]["running"] == ["gpu", "wait"])
_ran, _qs = [], {"out": ""}
_ro, _sh = H.run_on, H.sh
H.run_on = lambda m, script, timeout=120: (_ran.append((m["ssh"], script)), (0, "2026-11-06 08:17:00 pueued 恢复：放回队列 3 4\n"))[1]
H.sh = lambda cmd, inp=None, timeout=120: (0, _qs["out"])
_ev_added = []
import events as _EV  # noqa: E402
_EV_add = _EV.add
_EV.add = lambda source, msg, db=None, **kw: _ev_added.append((source, msg, kw))
try:
    _down = snap(pueue={"hub": {"error": "Error: no socket"}, "pueue-host": {"error": "Error: no socket"}})
    H.policy.AUTO["revive"] = False
    ok("revive：开关关着不动", H.revive(_down, {}) == ([], {}) and not _ran)
    H.policy.AUTO["revive"] = True
    _qs["out"] = json.dumps({"hub": {"groups": {}, "tasks": []}, "pueue-host": {"groups": {}, "tasks": []}})
    ev, still = H.revive(_down, {"hub": dict(t=1000, running=["gpu", "wait"]), "pueue-host": dict(t=2000, running=[])})
    hub_cmd = next(sc for sh_, sc in _ran if sh_ is None)
    ph_cmd = next(sc for sh_, sc in _ran if sh_ == "fake-pueue-host")
    ok("revive：hub 本机跑 on_boot.sh --since <上次在的时间-60> --resume <那时开着的组>",
       hub_cmd.endswith("queue/on_boot.sh --since 940 --resume gpu,wait") and hub_cmd.startswith("bash /"))
    ok("revive：别的机器跑它代码包里的 on_boot.sh（没有开着的组时不带 --resume）",
       ph_cmd.startswith("bash " + H.M.MACHINES["pueue-host"]["code"] + "/") and "--since 1940" in ph_cmd and "--resume" not in ph_cmd)
    ok("revive：恢复成功 → 记事件（health/revive），不留原因", len(ev) == 2 and all("已自动恢复" in e and "放回队列 3 4" in e for e in ev)
       and still == {} and {x[2].get("site") for x in _ev_added} == {"hub", "pueue-host"} and _ev_added[0][2]["kind"] == "revive")
    _ran.clear()
    ev, still = H.revive(_down, {}, dry=True)
    ok("revive：dry 只说会做什么，不跑", not _ran and len(ev) == 2 and all("会自动恢复" in e for e in ev))
    ev, still = H.revive(snap(pueue={"hub": {"error": "x"}, "pueue-host": {"groups": {}, "tasks": []}}), {})
    ok("revive：没有上次的记录 → 只启动（--since 现在、不带 --resume）；pueued 在的机器不动",
       len(_ran) == 1 and f"--since {int(NOW.timestamp())}" in _ran[0][1] and "--resume" not in _ran[0][1])
    _qs["out"] = json.dumps({"hub": {"error": "still down"}})
    _ran.clear()
    ev, still = H.revive(snap(pueue={"hub": {"error": "x"}, "pueue-host": {"groups": {}, "tasks": []}}), {})
    ok("revive：恢复后还是没响应 → 返回原因（照常报 pueued）", ev == [] and "自动恢复没成功" in still.get("hub", ""))
    _off = snap(pueue={"hub": {"error": "x"}, "pueue-host": {"error": "x"}}, ok={"hub": True, "pueue-host": False})
    _ran.clear()
    H.revive(_off, {})
    ok("revive：连不上的机器不动", [x[0] for x in _ran] == [None])
finally:
    H.run_on, H.sh, _EV.add = _ro, _sh, _EV_add
    H.policy.AUTO.pop("revive", None)

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
