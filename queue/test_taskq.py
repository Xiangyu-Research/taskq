#!/usr/bin/env python3
"""taskq.py 的测试（2026-10-17，10/18 改成任务说明）：临时目录里的假队列、假训练结果，不连任何机器。  python3 queue/test_taskq.py"""
import glob
import json
import os
import shutil
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import cfg as _cfg  # noqa: E402  调度代码的根、使用者的仓库根
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata"))
import fixture  # noqa: E402
T = fixture.tmpdir()
TQ, DATA = os.path.join(T, "taskq"), os.path.join(T, "data")
RES = os.path.join(DATA, "results")
ENV = dict(os.environ, TASKQ_DIR=TQ, TASKQ_DATA=DATA, TASKQ_MACHINE="slurm-login")  # 测试里本机当队列所在的机器
ENV.pop("DEMO_RESULTS", None)
sys.path.insert(0, HERE)
fixture.use()
ENV.update(TASKQ_CONFIG=fixture.CONFIG, TASKQ_PLUGINS=fixture.PLUGINS)
os.environ.update(TASKQ_DIR=TQ, TASKQ_LOCK_WAIT="0.05")  # 本进程里等锁的重试间隔（try_lock 预期拿不到的几项）
import taskq  # noqa: E402
import spec as S  # noqa: E402
import experiments as E  # noqa: E402
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
import mtab as M  # noqa: E402

ok = True


def check(name, cond):
    global ok
    print(("通过：" if cond else "失败：") + name)
    ok &= bool(cond)


def tq(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=ENV, capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()  # 消息在 stderr


def part(ds, tr, f):
    d = os.path.join(RES, ds, f"{tr}__PlanM__full", f"frag_{f}")
    os.makedirs(d, exist_ok=True)
    return d


ALL = "base,alt2,alt"


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for order, nice, key, *env in rows:
            ds, tr, fo = key.split("/")
            p = dict(ds=ds, variant=tr, frag=fo, **(dict(env=env[0]) if env else {}))
            f.write(json.dumps(dict(order=order, nice=nice, key=key, exp=None, recipe="toy", params=p)) + "\n")


tasks = [(10, 0, "D121_x/V_save/0"), (11, 0, "D121_x/V_save/1"),
         (12, 0, "D121_x/V_save/2"), (5, 50, "D103_y/V_save/0"),
         (1, 2000, "D101_z/V_s/0", "alt")]
write_jsonl(os.path.join(TQ, "tasks.jsonl"), tasks)
for t in tasks:
    part(*t[2].split("/"))

# 第 0 份已训完
d0 = part("D121_x", "V_save", "0")
open(os.path.join(d0, "final.ckpt"), "w").close()
os.makedirs(os.path.join(d0, "check"))
open(os.path.join(d0, "check", "summary.json"), "w").write("{}")
# 第 1 份的训练日志刚更新过（别处在训，比如附加训练）
open(os.path.join(part("D121_x", "V_save", "1"), "train_log_x.txt"), "w").write("x")

rc, out = tq("next", "--worker", "100", "--envs", ALL)
check("按档位先领、跳过训完的和日志刚更新的：领到第 2 份", rc == 0 and out.startswith("D121_x/V_save/2\n"))
rc, out = tq("next", "--worker", "101", "--envs", ALL)
check("已经被领的不再领：下一个是档位 50 的", rc == 0 and out.startswith("D103_y/"))
rc, out = tq("next", "--worker", "103", "--envs", ALL)
check("照顺序领 V_s（环境 alt）", rc == 0 and out.startswith("D101_z/V_s/0\n") and "env=alt" in out)
rc, out = tq("next", "--worker", "102", "--envs", ALL)
check("没有能领的：退出码 10", rc == 10 and out == "")

# 领取过期（心跳 30 分钟前、日志也没更新）→ 可以重新领
p = os.path.join(TQ, "claims", taskq.safe("D103_y/V_save/0"), "info")
ci = json.load(open(p))
ci["beat"] = time.time() - 1800
json.dump(ci, open(p, "w"))
rc, out = tq("next", "--worker", "104", "--envs", ALL)
check("心跳过期、日志也没更新的重新领", rc == 0 and out.startswith("D103_y/"))
check("重新领后记的是新的 worker", json.load(open(p))["worker"] == "104")

# 11/1：next 的第三行是这次领取的 token；同名 worker 重开（pueue-host 的 pueue worker 名不变）后，旧一轮带旧 token 的交还、心跳不动新一轮的领取
lines = out.splitlines()
check("next 第三行是 token，和领取记录里的一样", len(lines) == 3 and lines[2] == json.load(open(p))["token"])
K103 = "D103_y/V_save/0"
ci = json.load(open(p))
ci["beat"] = time.time() - 600
json.dump(ci, open(p, "w"))
tq("beat", "--worker", "104", "--token", "0ld0ld", K103)
check("旧一轮的 token：心跳不改记录", time.time() - json.load(open(p))["beat"] > 500)
tq("release", "--worker", "104", "--token", "0ld0ld", K103)
check("旧一轮的 token：交还不了", os.path.exists(p))
rc, _ = tq("missing", "--worker", "104", "--token", "0ld0ld", K103)
check("旧一轮的 token：记不了缺输入", rc == 1 and os.path.exists(p) and taskq.input_info(K103) is None)
tq("beat", "--worker", "104", "--token", lines[2], K103)
check("这一轮的 token：心跳更新", time.time() - json.load(open(p))["beat"] < 10)

# 心跳过期但训练日志刚更新 → 仍算在训
key2 = "D121_x/V_save/2"
p2 = os.path.join(TQ, "claims", taskq.safe(key2), "info")
ci = json.load(open(p2))
ci["beat"] = time.time() - 1800
json.dump(ci, open(p2, "w"))
open(os.path.join(part("D121_x", "V_save", "2"), "train_log_y.txt"), "w").write("y")
rc, out = tq("next", "--worker", "105", "--envs", ALL)
check("心跳过期但日志刚更新的不领", rc == 10)

# 心跳
rc, _ = tq("beat", "--worker", "100", key2)
check("心跳更新", rc == 0 and time.time() - json.load(open(p2))["beat"] < 10)
rc, _ = tq("beat", "--worker", "999", key2)
check("别的 worker 的心跳不改记录", json.load(open(p2))["worker"] == "100")

# 交还：只有自己领的能交还；失败记一次
rc, _ = tq("release", "--worker", "999", key2)
check("别的 worker 交还不了", os.path.exists(p2))
rc, _ = tq("release", "--worker", "100", key2, "--failed", "退出码 1")
check("交还后领取记录删掉、失败记一次", not os.path.exists(p2) and taskq.failures(key2) == 1)
tq("release", "--worker", "104", "D103_y/V_save/0", "--failed", "退出码 1")
tq("release", "--worker", "104", "D103_y/V_save/0", "--failed", "退出码 1")
rc, _ = tq("next", "--worker", "110", "--envs", ALL)
check("3 小时内失败 3 次：整个队列暂停领任务（退出码 12）", rc == 12)
for fp in glob.glob(os.path.join(TQ, "failures", "*")):  # 把失败记录改成 4 小时前
    old = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 4 * 3600))
    lines = open(fp).readlines()
    open(fp, "w").write("".join(old + ln[19:] for ln in lines))
for f in os.listdir(part("D121_x", "V_save", "2")):
    os.utime(os.path.join(part("D121_x", "V_save", "2"), f), (time.time() - 3600,) * 2)
rc, out = tq("next", "--worker", "106", "--envs", ALL)
check("失败 1 次的还能领、失败 2 次的不领", rc == 0 and out.startswith(key2))
tq("release", "--worker", "106", key2)
rc, out = tq("next", "--worker", "106", "--envs", ALL, "--exclude", key2)
check("本作业失败过的（--exclude）不再领", rc == 10)

# 交还之后日志没再更新：马上能领；交还之后日志又更新了：算队列外有人在训
open(os.path.join(part("D121_x", "V_save", "2"), "train_log_y.txt"), "w").write("y")
rc, out = tq("next", "--worker", "120", "--envs", ALL)
check("刚领的（日志刚更新）照常领到", rc == 0 and out.startswith(key2))
tq("release", "--worker", "120", key2)
rc, out = tq("next", "--worker", "121", "--envs", ALL)
check("刚交还、日志 15 分钟内更新过，也马上能领", rc == 0 and out.startswith(key2))
tq("release", "--worker", "121", key2)
rel = os.path.join(TQ, "released", taskq.safe(key2))
check("交还时记下了时间（released/<key>）", taskq.released_at(key2) is not None)
open(rel, "w").write(f"{time.time() - 10} 121\n")  # 交还记在 10 秒前，日志在那之后更新
open(os.path.join(part("D121_x", "V_save", "2"), "train_log_y.txt"), "w").write("y2")
rc, out = tq("next", "--worker", "122", "--envs", ALL)
check("交还之后日志又更新了（队列外在训）：不领", rc == 10)

# 排在前面的一份刚停下（日志新、没人领），后面还有能领的：开始后的等待时间内先等它
open(os.path.join(part("D121_x", "V_save", "1"), "train_log_x.txt"), "w").write("x")  # 第 1 份刚停下
os.remove(os.path.join(TQ, "failures", taskq.safe("D103_y/V_save/0")))  # 让后面有一个能领的
rc, out = tq("next", "--worker", "130", "--envs", ALL, "--free-slots", "1", "--patience-until", str(time.time() + 600))
check("空 1 个位置、前面有 1 个刚停下的：等（退出码 11）", rc == 11)
rc, out = tq("next", "--worker", "130", "--envs", ALL, "--free-slots", "3", "--patience-until", str(time.time() + 600))
check("空的位置比刚停下的多：照常领后面的", rc == 0 and out.startswith("D103_y/"))
tq("release", "--worker", "130", "D103_y/V_save/0")
rc, out = tq("next", "--worker", "131", "--envs", ALL, "--free-slots", "1", "--patience-until", str(time.time() - 1))
check("等待时间过了：领后面的", rc == 0 and out.startswith("D103_y/"))
tq("release", "--worker", "131", "D103_y/V_save/0")
os.remove(os.path.join(part("D121_x", "V_save", "1"), "train_log_x.txt"))

for f in os.listdir(part("D121_x", "V_save", "2")):
    os.utime(os.path.join(part("D121_x", "V_save", "2"), f), (time.time() - 3600,) * 2)
rc, out = tq("next", "--worker", "107", "--envs", ALL)
check("照顺序领第 1 份", rc == 0 and out.startswith("D121_x/V_save/1\n"))
rc, out = tq("next", "--worker", "108", "--envs", ALL)
check("再下一个是第 2 份（日志 1 小时前更新过，没人领）", rc == 0 and out.startswith(key2))
# 并发领：同时起 6 个，只有 1 个领到
procs = [subprocess.Popen([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", f"2{i}", "--envs", ALL],
                          env=ENV, stdout=subprocess.PIPE, text=True) for i in range(6)]
outs = [p.communicate()[0].strip() for p in procs]
check("同时领，只有一个领到", sum(1 for o in outs if o) == 1)

# ---------------- 10/18：take --assign、add、set-nice、kind any ----------------
TQ2 = os.path.join(T, "taskq2")
ENV2 = dict(ENV, TASKQ_DIR=TQ2)


def tq2(*args, env=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=env or ENV2, capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()  # 消息在 stderr


os.makedirs(TQ2)
write_jsonl(os.path.join(TQ2, "tasks.jsonl"), [(486191, 0, "D116_gamma/V_save/4"),
                                               (486300, 2000, "D101_alpha/V_s/0", "alt")])
os.environ["TASKQ_DIR"] = TQ2
rc, out = tq2("add", "--nice", "50", "--exp", "arch_m", "--only", "frag=0")
rc2, out2 = tq2("add", "--nice", "50", "--exp", "arch_m", "--only", "frag=0")
ts = taskq.read_tasks(os.path.join(TQ2, "tasks.jsonl"))
um = ts[1]
check("add --exp：照实验表（arch_m：环境 alt2、实验 arch_m），按档位排进去，已有的跳过",
      rc == 0 and [t["key"].split("/")[1] for t in ts] == ["V_save", "V_m", "V_s"]
      and um["exp"] == "arch_m" and um["params"]["env"] == "alt2" and um["order"] == 486301 and "已有，跳过" in out2)
rc, out = tq2("add", "--nice", "0", "--exp", "nosuch")
check("add：实验表里没有的实验报错", rc != 0)
rc, out = tq2("add", "--nice", "0", "D101_alpha/V_m/0")
check("add：不给 --exp / --recipe 报错", rc != 0)
rc, out = tq2("set-nice", "--nice", "100", "D101_alpha/V_s/0")
ts = taskq.read_tasks(os.path.join(TQ2, "tasks.jsonl"))
check("set-nice：改档位后重新排序", rc == 0 and "V_s" in ts[2]["key"] and ts[2]["nice"] == 100)
rc, out = tq2("set-nice", "--nice", "1", "D101_alpha/V_save/9")
check("set-nice：没有的 key 退出码 1", rc == 1)

UM = "D101_alpha/V_m/0"
SWK = "D101_alpha/V_s/0"
rc, out = tq2("take", "--worker", "pueue-host", "--assign", UM)
ci = json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))
check("take --assign：分到指定的一个任务，记下 worker、assigned", rc == 0 and out.startswith(UM) and ci["worker"] == "pueue-host" and ci["assigned"] == "pueue-host")
rc, out = tq2("next", "--worker", "500", "--envs", ALL, "--exclude", "D101_alpha/V_s/0", "D116_gamma/V_save/4")
check("分给 pueue-host 的，Slurm 作业不再领", rc == 10)
ci["beat"] = time.time() - 3 * 86400
json.dump(ci, open(os.path.join(TQ2, "claims", taskq.safe(UM), "info"), "w"))
rc, out = tq2("next", "--worker", "500", "--envs", ALL, "--exclude", "D101_alpha/V_s/0", "D116_gamma/V_save/4")
check("分出去的不过期：心跳 3 天前仍算领着", rc == 10)
rc, out = tq2("take", "--worker", "pueue-host", "--assign", UM)
ci2 = json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))
check("take 已分给自己的：续上、领取时间不变", rc == 0 and ci2["since"] == ci["since"] and time.time() - ci2["beat"] < 10)
rc, out = tq2("take", "--worker", "other", "--assign", UM)
check("take 分给别人的：分不到（退出码 10）", rc == 10)
rc, _ = tq2("take", "--worker", "pueue-host", UM)
check("take 不带 --assign（hub 替 pueue-host 领的老写法，11/4 删）：参数错", rc == 2)
rc, _ = tq2("take", "--worker", "other", "--assign", "--from", "nobody", UM)
check("take --from 不是现在分给的那台：分不到", rc == 10 and json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))["assigned"] == "pueue-host")
rc, _ = tq2("take", "--worker", "other", "--assign", "--from", "pueue-host", UM)
ci3 = json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))
check("take --from：分给 pueue-host 的一步改分给 other（交班，11/5），领取时间重算", rc == 0 and ci3["assigned"] == ci3["worker"] == "other" and ci3["since"] > ci["since"])
tq2("release", "--worker", "other", UM)
tq2("release", "--worker", "pueue-host", UM)
rc, out = tq2("next", "--worker", "501", "--envs", ALL, "--exclude", "D101_alpha/V_s/0", "D116_gamma/V_save/4")
check("交还以后 Slurm 作业能接着领", rc == 0 and out.startswith(UM))
rc, out = tq2("take", "--worker", "pueue-host", "--assign", "D116_gamma/V_save/9")
check("take 队列里没有的：退出码 10", rc == 10)
d = part("D116_gamma", "V_save", "4")
open(os.path.join(d, "final.ckpt"), "w").close()
os.makedirs(os.path.join(d, "check"), exist_ok=True)
open(os.path.join(d, "check", "summary.json"), "w").write("{}")
rc, out = tq2("take", "--worker", "pueue-host", "--assign", "D116_gamma/V_save/4")
check("take 已训完的：领不到", rc == 10 and "done" in out)
rc, out = tq2("next", "--worker", "502", "--envs", ALL)
check("kind any：什么种类都领（领到 V_s）", rc == 0 and out.startswith("D101_alpha/V_s/0"))
s2 = taskq.S.json_in(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=ENV2, capture_output=True, text=True).stdout)
row = next(t for t in s2["tasks"] if t["key"] == UM)
rc, _ = tq2("release", "--worker", "pueue-host", SWK, "--skip", "训练任务 5 Failed (1)")
s3 = taskq.S.json_in(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=ENV2, capture_output=True, text=True).stdout)
row3 = next(t for t in s3["tasks"] if t["key"] == SWK)
check("release --skip：记在 skips/、不算失败；--json-status 带 skipped_by 和 24 小时内的次数",
      rc == 0 and row3["skipped_by"] == ["pueue-host"] and row3["failures"] == 0
      and s3["recent_skips"] == {"pueue-host": 1} and not os.path.exists(os.path.join(TQ2, "failures", taskq.safe(SWK))))
check("--json-status 带 worker、host、beat", row["worker"] == "501" and "host" in row and row["beat"])

# worker 的调用方式（dicc_worker.sh 实际传的参数）不变：在跑的 worker 每次都调用仓库里的 taskq.py
rc, out = tq2("next", "--worker", "503", "--envs", ALL, "--free-slots", "2", "--patience-until", str(time.time() + 1200))
check("worker：next 带 --free-slots / --patience-until，没有能领的退出码 10", rc == 10)
rc, _ = tq2("beat", "--worker", "502", "D101_alpha/V_s/0")
check("worker：beat 退出码 0", rc == 0)
rc, _ = tq2("release", "--worker", "502", "D101_alpha/V_s/0")
check("worker：release 退出码 0、记录删掉", rc == 0 and not os.path.exists(os.path.join(TQ2, "claims", taskq.safe("D101_alpha/V_s/0"))))
rc, _ = tq2("release", "--worker", "501", UM, "--failed", "退出码 1")
check("worker：release --failed 记失败", rc == 0 and len(open(os.path.join(TQ2, "failures", taskq.safe(UM))).readlines()) == 1)
rc, out = tq2("next", "--worker", "504", "--envs", ALL, "--exclude", UM)
check("pueue-host 跳过的任务（skips/）Slurm 作业照样领", rc == 0 and out.startswith(SWK))
rc, out = tq2("beat", "--worker", "504", "--strict", SWK)
check("beat --strict：领取是自己的，退出码 0", rc == 0 and out == "")
rc, out = tq2("beat", "--worker", "999", "--strict", SWK, "gone/x/0")
check("beat --strict：领取是别人的、已经没了 → 退出码 13，打印这些 key（11/4）", rc == 13 and out.split("\n") == [SWK, "gone/x/0"])
os.makedirs(os.path.join(TQ2, "claims", taskq.safe("gone/x/0")))
rc, out = tq2("beat", "--worker", "999", "--strict", "gone/x/0")
check("beat --strict：领取目录在、记录读不了，不算丢", rc == 0)
os.rmdir(os.path.join(TQ2, "claims", taskq.safe("gone/x/0")))
os.makedirs(os.path.join(TQ2, "lock"))
open(os.path.join(TQ2, "lock", "owner"), "w").write(f"{socket.gethostname()}\t{os.getpid()}\tx\n")
t0 = time.time()
rc, _ = tq2("release", "--worker", "504", "--lock-wait", "1", SWK)
check("release --lock-wait 1：锁被占着，约 1 秒就放弃（退出码非 0），不交还（11/4）",
      rc != 0 and time.time() - t0 < 5 and os.path.exists(os.path.join(TQ2, "claims", taskq.safe(SWK), "info")))
shutil.rmtree(os.path.join(TQ2, "lock"))
info_p = os.path.join(TQ2, "claims", taskq.safe(SWK), "info")
ci3 = json.load(open(info_p))
ci3.pop("ttl", None)
ci3["beat"] = time.time() - 600
json.dump(ci3, open(info_p, "w"))
rc, _ = tq2("next", "--worker", "505", "--envs", ALL, "--exclude", UM)
check("worker：没有 ttl 的领取记录，心跳 10 分钟前仍算在训", rc == 10)
ci3["beat"] = time.time() - 1300
json.dump(ci3, open(info_p, "w"))
rc, out = tq2("next", "--worker", "505", "--envs", ALL, "--exclude", UM)
check("worker：没有 ttl 的领取记录，心跳超过 20 分钟就过期、别人能领", rc == 0 and out.startswith(SWK))
NOQ = os.path.join(T, "no_tasks")
os.makedirs(NOQ)
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "506", "--envs", ALL],
                   env=dict(ENV, TASKQ_DIR=NOQ), capture_output=True, text=True)
check("没有任务表：next 退出码 2（出错，不是 10「没有任务」，作业不会因此提前结束）", r.returncode == 2)

EMPTY = os.path.join(T, "hub_tq")  # hub 上：空的队列目录，没有任务表
os.makedirs(EMPTY)
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "add", "--nice", "100", "--recipe", "toy",
                    "ds=D113_delta", "variant=V_save", "frag=0"],
                   env=dict(os.environ, TASKQ_DIR=EMPTY, TASKQ_MACHINE="hub"), capture_output=True, text=True)
check("在 hub 上（总队列所在的机器）add：本机建总队列（10/31 起不再转发）", r.returncode == 0 and "加入" in r.stdout
      and "D113_delta" in open(os.path.join(EMPTY, "tasks.jsonl")).read())
NOWHERE = os.path.join(T, "no_queue_here")
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "add", "--nice", "100", "--recipe", "toy",
                    "ds=D113_delta", "variant=V_save", "frag=0"],
                   env=dict(os.environ, TASKQ_DIR=NOWHERE, TASKQ_MACHINE="pueue-host"), capture_output=True, text=True)
check("不在 hub、本机也没有队列：add 退出码 2，提示（标准错误）总队列在 hub，不建目录", r.returncode == 2 and "总队列在 hub" in r.stderr
      and not os.path.exists(NOWHERE))
J = '{"tasks": [], "enabled": true}'
check("@@JSON 行前后混着别的输出：只读那一行", taskq.S.json_in(f"警告：x {{1}}\n@@JSON {J}\n{{别的}}\n") == json.loads(J))
check("没有 @@JSON 行：不认（不找第一个 {）", taskq.S.json_in(f"x\n{J}\n") is None)


# ---------------- 10/18：任务说明（next、set、show、add --exp / --recipe） ----------------
TQ3 = os.path.join(T, "taskq3")
ENV3 = dict(ENV, TASKQ_DIR=TQ3)


def tq3(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=ENV3, capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


os.makedirs(TQ3)
open(os.path.join(TQ3, "tasks.jsonl"), "w").close()
rc, out, _ = tq3("add", "--nice", "0", "--exp", "t15b", "--only", "frag=1,2")
ts = taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))
check("add --exp --only：从实验表展开、记下实验 id", rc == 0 and [t["key"] for t in ts] ==
      ["D111_beta/V_save_n470/1", "D111_beta/V_save_n470/2"] and ts[0]["exp"] == "t15b")
rc, out, _ = tq3("add", "--nice", "5", "--recipe", "command", "key=toy/a", "cmd=python3 -c 1", "env=stats", "out=toy/a", 'done=["DONE"]')
check("add --recipe：实验表以外的任务", rc == 0 and "toy/a" in out and "环境 stats" in out)
rc, out, err = tq3("add", "--nice", "5", "--recipe", "toy", "ds=D101_alpha", "frag=0", "variantt=x")
check("add --recipe：写错参数名报错、不加", rc == 2 and "variantt" in err and len(taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))) == 3)
rc, out, err = tq3("add", "--nice", "5", "--recipe", "command", "key=toy__a", "cmd=python3 -c 1", "env=stats", "out=toy/a2", 'done=["DONE"]')
check("add：转成文件名和已有任务重名（toy/a 与 toy__a）→ 报错、不加", rc == 2 and "文件名" in err
      and len(taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))) == 3)
rc, out, _ = tq3("add", "--nice", "5", "--recipe", "command", "key=toy/a", "cmd=python3 -c 1", "env=stats", "out=toy/a", 'done=["DONE"]')
rc2, _, out2 = tq3("add", "--nice", "5", "--recipe", "command", "key=toy/a", "cmd=python3 -c 2", "env=stats", "out=toy/a", 'done=["DONE"]')
check("add --recipe 同名：说明一样 → 跳过；说明不一样 → 报出来、退出码 2、不改原来的",
      rc == 0 and "已有，跳过" in out and rc2 == 2 and "说明不一样" in out2
      and next(x for x in taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl")) if x["key"] == "toy/a")["params"]["cmd"] == "python3 -c 1")
check("same_file：同一批里两个 key 转成同一个文件名也拦", taskq.same_file(["x/y", "x__y"], set()) and not taskq.same_file(["x/y"], {"x/y"}))
rc, out, _ = tq3("next", "--worker", "700", "--envs", "base,alt")
lines = out.splitlines()
check("next：打印 key 和 run_task.sh 的参数", rc == 0 and lines[0] == "D111_beta/V_save_n470/1"
      and lines[1] == "toy ds=D111_beta variant=V_save_n470 frag=1")
rc, out, _ = tq3("next", "--worker", "701", "--envs", "base,alt")
rc2, out2, _ = tq3("next", "--worker", "702", "--envs", "base")
check("next：本机没有的环境（stats）不领", rc == 0 and rc2 == 10)
rc, out, _ = tq3("next", "--worker", "703", "--envs", "stats")
check("next：有这个环境的机器领得到；JSON 值原样带上", rc == 0 and out.splitlines()[1] == "command key=toy/a 'cmd=python3 -c 1' env=stats out=toy/a 'done=[\"DONE\"]'")
tq3("release", "--worker", "700", "D111_beta/V_save_n470/1")
rc, out, _ = tq3("set", "D111_beta/V_save_n470/1", "env=alt2")
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))}
check("set：改单个任务的参数", rc == 0 and ts["D111_beta/V_save_n470/1"]["params"]["env"] == "alt2")
rc, out, _ = tq3("next", "--worker", "705", "--envs", "alt2")
check("next：改了环境以后按新环境领", rc == 0 and out.startswith("D111_beta/V_save_n470/1"))
tq3("release", "--worker", "705", "D111_beta/V_save_n470/1")
rc, out, _ = tq3("set", "D111_beta/V_save_n470/1", "env=")
rc, out, _ = tq3("next", "--worker", "706", "--envs", "base")
check("set k=（空值）删掉参数、回到默认环境", rc == 0 and out.startswith("D111_beta/V_save_n470/1\n"))
rc, out, _ = tq3("set", "D111_beta/V_save_n470/1", "frag=3")
check("set：会改变任务名的不让改", rc == 2)
rc, out, _ = tq3("show", "--json", "toy/a")
sj = taskq.S.json_in(out)
check("show --json：完整说明，写在 @@JSON 开头的一行里", rc == 0 and out.startswith("@@JSON ") and sj["out"] == "toy/a" and sj["env"] == "stats")
rc, out, _ = tq3("show", "D111_beta/V_save_n470/2")
check("show：说明、状态、run_task.sh 的写法", rc == 0 and "claimed" in out and "run_task.sh toy" in out and "train.sh" in out)
d = os.path.join(DATA, "toy", "a")
os.makedirs(d)
open(os.path.join(d, "DONE"), "w").close()
s3 = taskq.S.json_in(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=ENV3, capture_output=True, text=True).stdout)
st3 = {t["key"]: t for t in s3["tasks"]}
check("--json-status：带说明；command 任务按 done 判断训完", st3["toy/a"]["state"] == "done" and st3["toy/a"]["spec"]["recipe"] == "command"
      and st3["D111_beta/V_save_n470/2"]["spec"]["out"].startswith("results/D111_beta/"))
with open(os.path.join(TQ3, "tasks.jsonl"), "a") as f:
    f.write(json.dumps(dict(order=99, nice=1, key="bad/x", exp=None, recipe="toy", params=dict(frag="0"))) + "\n")
rc, out, _ = tq3("status")
check("解析不了的任务显示 bad、不影响别的", rc == 0 and "bad" in out)

# 10/22：machines 限定在哪几台机器上领（推理只在 Slurm）
tq3("add", "--nice", "-100", "--recipe", "command", "key=toy/m", "cmd=true", "env=base", "out=toy/m", 'done=["DONE"]', 'machines=["slurm-login"]')
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "710", "--envs", "base"],
                   env=dict(ENV3, TASKQ_MACHINE="pueue-host"), capture_output=True, text=True)
check("next：machines 里没有本机的不领", r.returncode == 10)
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "711", "--envs", "base"],
                   env=dict(ENV3, TASKQ_MACHINE="slurm-login"), capture_output=True, text=True)
check("next：machines 里有本机的照档位先领", r.returncode == 0 and r.stdout.startswith("toy/m"))
# 10/24：device=cpu 的只给 CPU worker（--device cpu）；不带 --device 的（GPU worker、在跑的旧 worker）不领
tq3("add", "--nice", "-200", "--recipe", "command", "key=toy/c", "cmd=true", "env=base", "out=toy/c", 'done=["DONE"]', "device=cpu")
rc, out, _ = tq3("next", "--worker", "712", "--envs", "base")
check("next：不带 --device（GPU worker）不领 device=cpu 的，即使排在最前", rc != 0 or not out.startswith("toy/c"))
if rc == 0:
    tq3("release", "--worker", "712", out.splitlines()[0])
rc, out, _ = tq3("next", "--worker", "713", "--envs", "base", "--device", "cpu")
check("next --device cpu：只领 device=cpu 的", rc == 0 and out.startswith("toy/c\n") and "device=cpu" in out)
rc, out, _ = tq3("next", "--worker", "714", "--envs", "base", "--device", "cpu")
check("next --device cpu：没有别的纯 CPU 任务 → 10", rc == 10)
rc, out, _ = tq3("add", "--nice", "-200", "--recipe", "command", "key=toy/e", "cmd=true", 'env=["base","stats"]', "out=toy/e",
                 'done=["DONE"]', "device=cpu")
check("add：env 可以是几个环境的列表", rc == 0 and "环境 base,stats" in out)
rc, out, _ = tq3("next", "--worker", "715", "--envs", "base", "--device", "cpu")
check("next：env 列表里有没备好的环境 → 不领", rc == 10)
rc, out, _ = tq3("next", "--worker", "715", "--envs", "base,stats", "--device", "cpu")
check("next：env 列表里的环境都备好 → 领", rc == 0 and out.startswith("toy/e\n"))
rc, out, _ = tq3("status")
check("status：env 列表显示成 a,b", rc == 0 and "base,stats" in out)

# 10/23：主数据目录读不了时不领
# add 不给 --nice（10/24）：--exp 用实验表里的档位，别的报错（单独一份队列，不影响上面的）
TQ3B = os.path.join(T, "taskq3b")
os.makedirs(TQ3B)
open(os.path.join(TQ3B, "tasks.jsonl"), "w").close()


def tq3b(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ3B), capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


rc, out, _ = tq3b("add", "--exp", "t15g", "--only", "frag=0")
ts = taskq.read_tasks(os.path.join(TQ3B, "tasks.jsonl"))
check("add --exp 不给 --nice：用实验表里的档位", rc == 0 and [t["key"] for t in ts] == ["D116_gamma/V_save_n470/0"]
      and ts[0]["nice"] == E.experiment("t15g")["nice"])
rc, out, err = tq3b("add", "--recipe", "command", "key=toy/n", "cmd=true", "env=base", "out=toy/n", 'done=["DONE"]')
check("add --recipe 不给 --nice：报错、不加", rc != 0 and "--nice" in err and len(taskq.read_tasks(os.path.join(TQ3B, "tasks.jsonl"))) == 1)
want = [t["key"] for t in E.expand(E.experiment("t16"))]
rc, out, _ = tq3b("add", "--exp", "t16")
rc2, out2, _ = tq3b("add", "--exp", "t16")
ts = [t for t in taskq.read_tasks(os.path.join(TQ3B, "tasks.jsonl")) if t["exp"] == "t16"]
check("add --exp（整个实验）：实验表展开的任务都加上、档位用表里的，再加一次全部跳过",
      rc == 0 and rc2 == 0 and len(want) == 5 and sorted(t["key"] for t in ts) == sorted(want)
      and {t["nice"] for t in ts} == {E.experiment("t16")["nice"]} and out2.count("已有，跳过") == len(want)
      and len(taskq.read_tasks(os.path.join(TQ3B, "tasks.jsonl"))) == 1 + len(want))

TQ4, D4 = os.path.join(T, "taskq4"), os.path.join(T, "data4")
os.makedirs(D4)
ks4 = ["D121_x/V_save/0", "D121_x/V_save/1", "D121_x/V_save/2"]
write_jsonl(os.path.join(TQ4, "tasks.jsonl"), [(i, 0, k) for i, k in enumerate(ks4)])
os.makedirs(os.path.join(TQ4, "failures"))
for k in ks4[:2]:
    with open(os.path.join(TQ4, "failures", taskq.safe(k)), "w") as f:
        f.write(f"{time.strftime('%F %T')}\t作业 1\tx\n" * 2)


def tq4(*args, root=D4):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ4, TASKQ_DATA=root),
                       capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


rc, out, err = tq4("next", "--worker", "800", "--envs", ALL, root=os.path.join(T, "nosuch"))
check("next：数据目录读不了时不领（退出码 12，报原因）", rc == 12 and "读不了" in err)
rc, out, err = tq4("take", "--worker", "pueue-host", "--assign", ks4[2], root=os.path.join(T, "nosuch"))
check("take：数据目录读不了时不领", rc == 10 and "读不了" in out and not os.path.exists(os.path.join(TQ4, "claims", taskq.safe(ks4[2]))))
rc, out, err = tq4("next", "--worker", "801", "--envs", ALL)
check("最近 4 次失败：暂停", rc == 12)

# ---------------- 10/25：领取只建了目录、info 没写上（领的进程在中间被杀） ----------------
TQ5, D5 = os.path.join(T, "taskq5"), os.path.join(T, "data5")
k5 = "D121_x/V_save/3"
write_jsonl(os.path.join(TQ5, "tasks.jsonl"), [(1, 0, k5)])
half = os.path.join(TQ5, "claims", taskq.safe(k5))
os.makedirs(half)
os.makedirs(D5, exist_ok=True)


def tq5(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ5, TASKQ_DATA=D5),
                       capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()  # 消息在 stderr


rc, out = tq5("next", "--worker", "900", "--envs", ALL)
check("半建的领取：刚建的（5 分钟内）当作正在领，不抢", rc == 10)
rc, out = tq5("take", "--worker", "pueue-host", "--assign", k5)
check("半建的领取：take 也不抢刚建的", rc == 10)
os.utime(half, (time.time() - 600,) * 2)
rc, out = tq5("status")
check("半建的领取：10 分钟前建的算过期", "stale" in out or "过期" in out or "不完整" in out)
rc, out = tq5("next", "--worker", "901", "--envs", ALL)
ci5 = json.load(open(os.path.join(half, "info")))
check("半建的领取：过期后照常领走，info 写全", rc == 0 and out.startswith(k5 + "\n") and ci5["worker"] == "901")
shutil.rmtree(half)
os.makedirs(half)
os.utime(half, (time.time() - 600,) * 2)
rc, out = tq5("take", "--worker", "pueue-host", "--assign", k5)
ci5 = json.load(open(os.path.join(half, "info")))
check("半建的领取：take 也能领走过期的", rc == 0 and ci5["worker"] == "pueue-host" and ci5["assigned"] == "pueue-host")
check("info 写完不留临时文件", os.listdir(half) == ["info"])

# ---------------- 10/25：after（先要做完的任务）→ waiting / blocked ----------------
TQ6, D6 = os.path.join(T, "taskq6"), os.path.join(T, "data6")
kt = "D121_x/V_save/4"
os.makedirs(os.path.join(TQ6), exist_ok=True)
part(*kt.split("/"))


def cmdtask(order, key, after, device="gpu"):
    return dict(order=order, nice=-100, key=key, exp=None, recipe="command",
                params=dict(key=key, cmd="true", env="base", out=f"out/{key}", done=[".done"], after=after, device=device))


rows = [dict(order=1, nice=0, key=kt, exp=None, recipe="toy", params=dict(ds="D121_x", variant="V_save", frag="4")),
        cmdtask(2, "infer/x/frag_4", [kt]), cmdtask(3, "eval/x/frag_4", ["infer/x/frag_4"], "cpu"), cmdtask(4, "infer/y", ["nosuch/key"])]
with open(os.path.join(TQ6, "tasks.jsonl"), "w") as f:
    f.writelines(json.dumps(r) + "\n" for r in rows)


def tq6(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ6, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()


def st6():
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=dict(ENV, TASKQ_DIR=TQ6, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return {t["key"]: (t["state"], t["note"]) for t in taskq.S.json_in(r.stdout)["tasks"]}


s6 = st6()
check("after：训练没训完 → 推理 waiting、评测 waiting（等推理）", s6["infer/x/frag_4"][0] == "waiting" and kt in s6["infer/x/frag_4"][1]
      and s6["eval/x/frag_4"] == ("waiting", "等 infer/x/frag_4"))
check("after：等的任务不在队列里 → blocked", s6["infer/y"][0] == "blocked" and "不在队列里" in s6["infer/y"][1])
rc, out = tq6("next", "--worker", "950", "--envs", ALL)
check("after：next 先领训练（档位 0），不领在等的推理（档位 -100）", rc == 0 and out.startswith(kt + "\n"))
rc, out = tq6("next", "--worker", "951", "--envs", ALL)
check("after：训练被领走后没有别的能领", rc == 10)
d4 = part(*kt.split("/"))
open(os.path.join(d4, "final.ckpt"), "w").close()
os.makedirs(os.path.join(d4, "check"), exist_ok=True)
open(os.path.join(d4, "check", "summary.json"), "w").write("{}")
s6 = st6()
check("after：训练训完 → 推理 free，评测还在等推理", s6["infer/x/frag_4"][0] == "free" and s6["eval/x/frag_4"][0] == "waiting")
rc, out = tq6("next", "--worker", "952", "--envs", ALL)
check("after：训完后推理能领", rc == 0 and out.startswith("infer/x/frag_4\n"))
tq6("release", "--worker", "952", "infer/x/frag_4", "--failed", "退出码 1")
tq6("release", "--worker", "952", "infer/x/frag_4", "--failed", "退出码 1")
s6 = st6()
check("after：推理失败两次 → 评测 blocked", s6["eval/x/frag_4"][0] == "blocked" and "失败 2 次" in s6["eval/x/frag_4"][1])

# ---------------- 10/28：remove（Slurm 停机时把排在那边的任务删掉、改在别处做） ----------------
rc, out = tq6("remove", "infer/x/frag_4")
check("remove：别的任务的 after 里有它、又不一起删 → 不删（退出码 3）", rc == taskq.REFUSED and "eval/x/frag_4" in out and "infer/x/frag_4" in st6())
rc, out = tq6("remove", "nosuch/key")
check("remove：没有的 key 退出码 4", rc == taskq.GONE and "队列里没有" in out)
rows2 = rows + [dict(order=5, nice=-100, key="free/z", exp=None, recipe="command",
                     params=dict(key="free/z", cmd="true", env="base", out="out/free/z", done=[".done"], device="cpu"))]
with open(os.path.join(TQ6, "tasks.jsonl"), "a") as f:
    f.write(json.dumps(rows2[-1]) + "\n")
rc, out = tq6("next", "--worker", "970", "--envs", ALL, "--device", "cpu")
rc2, out2 = tq6("remove", "free/z")
check("remove：有人领着的不删（退出码 3）", rc == 0 and out.startswith("free/z\n") and rc2 == taskq.REFUSED and "claimed" in out2
      and "free/z" in st6())
rc, out = tq6("remove", "infer/x/frag_4", "eval/x/frag_4", "nosuch/key")
s6 = st6()
check("remove：连同依赖它的一起删 → 都删掉，别的不动；一批里已经不在的跳过，退出码 0（11/4）", rc == 0 and "infer/x/frag_4" not in s6
      and "eval/x/frag_4" not in s6 and kt in s6 and "free/z" in s6 and "队列里没有：nosuch/key" in out)

# ---------------- 10/31：总队列在 hub、分给执行机（take --assign） ----------------
TQ7 = os.path.join(T, "taskq7")
os.makedirs(TQ7)
kt7 = "D121_x/V_save/3"
r7 = [dict(order=1, nice=0, key=kt7, exp=None, recipe="toy", params=dict(ds="D121_x", variant="V_save", frag="3")),
      cmdtask(2, "infer/x/frag_3", [kt7])]
with open(os.path.join(TQ7, "tasks.jsonl"), "w") as f:
    f.writelines(json.dumps(r) + "\n" for r in r7)


def tq7(*args, env=None, stdin=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ7, TASKQ_DATA=DATA, **(env or {})),
                       capture_output=True, text=True, input=stdin)
    return r.returncode, (r.stdout + r.stderr).strip()


def st7(d=TQ7):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=dict(ENV, TASKQ_DIR=d, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return {t["key"]: t for t in taskq.S.json_in(r.stdout)["tasks"]}


rc, out = tq7("take", "--worker", "slurm-login", "--assign", "infer/x/frag_3")
s7 = st7()
check("take --assign：waiting 的也能分出去（提前送到执行机）", rc == 0 and s7["infer/x/frag_3"]["assigned"] == "slurm-login"
      and s7["infer/x/frag_3"]["state"] == "waiting")
rc, out = tq7("take", "--worker", "slurm-login", "--assign", kt7)
ci7 = json.load(open(os.path.join(TQ7, "claims", taskq.safe(kt7), "info")))
ci7["beat"] = time.time() - 30 * 86400
json.dump(ci7, open(os.path.join(TQ7, "claims", taskq.safe(kt7), "info"), "w"))
s7 = st7()
check("take --assign：分出去的不过期（心跳 30 天前也还是 claimed）", rc == 0 and s7[kt7]["state"] == "claimed" and "ttl" not in ci7
      and s7[kt7]["assigned"] == "slurm-login")
rc, out = tq7("take", "--worker", "slurm-login", "--assign", kt7)
check("take --assign：已经分给自己的再分一次照样成功", rc == 0)
rc, out = tq7("take", "--worker", "pueue-host", "--assign", kt7)
check("take --assign：分给别人的不能再分", rc == 10)
rc, out = tq7("next", "--worker", "960", "--envs", ALL)
check("总队列里分出去的任务 worker 不领", rc == 10)
rc, out = tq7("remove", kt7, "infer/x/frag_3")
check("remove：分出去的不删，提示先 stop", rc == taskq.REFUSED and "先 stop" in out)
rc, out = tq7("stop", kt7)
check("stop：分出去的提示由 hub 下次检查时停", rc == 0 and "分给了 slurm-login" in out)
rc, out = tq7("take", "--worker", "slurm-login", "--assign", kt7)  # 变异审查（11/3）：停了的，原来领着的也不能再领
check("take：停了的（就算是自己领着的）不领", rc == 10 and "stopped" in out)
tq7("resume", kt7)

# 缺输入（11/1）：worker 记 missing → input，不领、不算失败；input-ok 撤掉（执行机上的队列，relay.py 补传后撤）
EQ = os.path.join(T, "exec7")
os.makedirs(EQ)
open(os.path.join(EQ, "ENABLED"), "w").close()
with open(os.path.join(EQ, "tasks.jsonl"), "w") as f:
    f.writelines(json.dumps(r) + "\n" for r in r7)


def tqe(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=EQ, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()


rc, out = tqe("next", "--worker", "963", "--envs", ALL)
rc1, _ = tqe("missing", "--worker", "999", "--why", "x", kt7)
rc2, _ = tqe("missing", "--worker", "963", "--why", "作业 963 开训前查到", kt7)
sb = st7(EQ)
check("缺输入：不是自己领的不能记；自己领的记上 → input、交还、不算失败", rc == 0 and out.startswith(kt7 + "\n") and rc1 == 1 and rc2 == 0
      and sb[kt7]["state"] == "input" and "缺输入" in sb[kt7]["note"] and not sb[kt7]["worker"] and sb[kt7]["failures"] == 0
      and sb[kt7]["input_since"])
rc, out = tqe("next", "--worker", "964", "--envs", ALL)
check("缺输入：不再领它", rc == 10 or not out.startswith(kt7 + "\n"))
rc, out = tqe("input-ok", kt7)
check("缺输入：input-ok 撤掉标记 → 照常领", rc == 0 and st7(EQ)[kt7]["state"] == "free")

# ---------------- 10/31：add --file（任务单） ----------------
TQF = os.path.join(T, "taskqf")
os.makedirs(TQF)
open(os.path.join(TQF, "tasks.jsonl"), "w").close()


def tqf(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQF),
                       capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


sheet = dict(key="guest/alice/toy", owner="alice", env="base", cmd="python3 train.py --data {data}/guest/alice/in --out {data}/guest/alice/toy",
             inputs=["guest/alice/in"], out="guest/alice/toy", done=["DONE"], progress="progress_json", resume=["ckpt_*.pt"],
             res=dict(gpus=1, cpus=8, mem="32G"), nice=40)
fn = os.path.join(T, "job.json")
json.dump(sheet, open(fn, "w"))
rc, out, _ = tqf("add", "--file", fn)
tf = taskq.read_tasks(os.path.join(TQF, "tasks.jsonl"))
check("add --file：一张任务单 → command 任务，档位用单子里的", rc == 0 and len(tf) == 1 and tf[0]["key"] == "guest/alice/toy"
      and tf[0]["recipe"] == "command" and tf[0]["nice"] == 40 and "nice" not in tf[0]["params"])
sf = S.resolve(tf[0]["recipe"], tf[0]["params"])
check("add --file：inputs 当 data、res、resume、owner 进了任务说明", sf["data"] == ["guest/alice/in"] and sf["inputs"] == sf["data"]
      and sf["res"] == dict(gpus=1, cpus=8, mem="32G") and sf["resume"] == ["ckpt_*.pt"] and sf["owner"] == "alice")
check("add --file：任务单加的 command 任务默认只换路径交给执行机", sf["relay"] == "path")
json.dump([dict(sheet, key="guest/alice/b", out="guest/alice/b"), dict(sheet, key="guest/alice/c", out="guest/alice/c")], open(fn, "w"))
rc, out, _ = tqf("add", "--nice", "7", "--file", fn)
tf = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQF, "tasks.jsonl"))}
check("add --file：一列任务单、--nice 优先", rc == 0 and tf["guest/alice/b"]["nice"] == 7 and tf["guest/alice/c"]["nice"] == 7)
json.dump(dict(sheet, key="guest/alice/d", res=dict(gpu=1)), open(fn, "w"))
rc, _, err = tqf("add", "--file", fn)
check("add --file：res 写错字段报错、不加", rc == 2 and "res" in err and len(taskq.read_tasks(os.path.join(TQF, "tasks.jsonl"))) == 3)
json.dump(dict(sheet, key="guest/alice/e", nice=None), open(fn, "w"))
rc, out, err = tqf("add", "--file", fn)
check("add --file：单子和命令行都没有档位 → 报错、不加", rc != 0 and len(taskq.read_tasks(os.path.join(TQF, "tasks.jsonl"))) == 3)
open(fn, "w").write("{oops")
rc, _, err = tqf("add", "--nice", "1", "--file", fn)
check("add --file：不是 JSON → 报错", rc == 2 and "任务单" in err)

# ---------------- 11/3：锁、take --assign、旧 token 交还、after 的顺序 ----------------
TQR = os.path.join(T, "taskqr")
os.makedirs(TQR)


def tqr(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQR, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()


def strl():
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=dict(ENV, TASKQ_DIR=TQR, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return {t["key"]: (t["state"], t["note"]) for t in taskq.S.json_in(r.stdout)["tasks"]}


def try_lock(tries=4):
    try:
        lk = taskq.Lock(tries=tries).__enter__()
    except SystemExit:
        return None
    return lk


tq_orig, taskq.TQ = taskq.TQ, TQR
LK = os.path.join(TQR, "lock")
holder = subprocess.Popen([sys.executable, "-c", "import sys, taskq\nwith taskq.Lock():\n    print('held', flush=True)\n    sys.stdin.read()"],
                          env=dict(ENV, TASKQ_DIR=TQR, PYTHONPATH=HERE), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
check("锁：另一个进程拿到锁", holder.stdout.readline().strip() == "held")
os.utime(LK, (time.time() - 121,) * 2)
lk = try_lock()
check("锁：持有者在本机、进程还活着，锁目录 2 分钟没动也不抢", lk is None and os.path.isdir(LK))
if lk:
    lk.__exit__()
holder.stdin.close()
holder.wait(timeout=30)
check("锁：持有者退出时删掉自己的锁", not os.path.exists(LK))
dead = subprocess.Popen([sys.executable, "-c", "pass"])
dead.wait()
os.mkdir(LK)
open(os.path.join(LK, "owner"), "w").write(f"{socket.gethostname()}\t{dead.pid}\tx\n")
os.utime(LK, (time.time() - 121,) * 2)
lk = try_lock()
check("锁：持有者的进程已经不在，过 2 分钟照常抢", lk is not None)
if lk:
    lk.__exit__()
os.mkdir(LK)
open(os.path.join(LK, "owner"), "w").write(f"elsewhere\t{os.getpid()}\tx\n")
lk = try_lock()
check("锁：别的主机拿着、锁目录是新的 → 等", lk is None)
os.utime(LK, (time.time() - 121,) * 2)
lk = try_lock()
check("锁：别的主机拿着、2 分钟没动 → 还等（看不到那边的死活，11/4）", lk is None)
os.utime(LK, (time.time() - taskq.LOCK_REMOTE - 1,) * 2)
lk = try_lock()
check("锁：别的主机拿着、LOCK_REMOTE 没动 → 抢", lk is not None)
if lk:
    lk.__exit__()
os.mkdir(LK)
os.utime(LK, (time.time() - 121,) * 2)
lk = try_lock()
check("锁：旧版本留下的空锁目录，过 2 分钟照常抢", lk is not None)
if lk:
    lk.__exit__()
lk = taskq.Lock().__enter__()
shutil.rmtree(LK, ignore_errors=True)
os.mkdir(LK)
lk.__exit__()
check("锁：锁已经被旧版本的进程接管（空目录），原持有者退出时不删", os.path.isdir(LK))
shutil.rmtree(LK, ignore_errors=True)
lk = taskq.Lock().__enter__()
shutil.rmtree(LK, ignore_errors=True)
os.mkdir(LK)
open(os.path.join(LK, "owner"), "w").write("other\t1\tzz\n")
lk.__exit__()
check("锁：锁已经被别人接管，原持有者退出时不删", os.path.exists(os.path.join(LK, "owner")))
shutil.rmtree(LK, ignore_errors=True)
taskq.TQ = tq_orig

kp, kc, kr = "r/prereq", "r/child", "r/rel"
rows_r = [cmdtask(1, kp, []), cmdtask(2, kc, [kp]), cmdtask(3, kr, [])]
with open(os.path.join(TQR, "tasks.jsonl"), "w") as f:
    f.writelines(json.dumps(r) + "\n" for r in rows_r)
rc, _ = tqr("take", "--worker", "A", "--assign", kc)
rc2, out2 = tqr("take", "--worker", "B", "--assign", kc)
ci = json.load(open(os.path.join(TQR, "claims", taskq.safe(kc), "info")))
check("take --assign：waiting 的已分给 A，B 分不到（退出码 10），仍是 A", rc == 0 and rc2 == 10 and ci["assigned"] == "A")
rc, _ = tqr("take", "--worker", "A", "--assign", kc)
check("take --assign：A 再分自己的照样成功", rc == 0 and json.load(open(os.path.join(TQR, "claims", taskq.safe(kc), "info")))["assigned"] == "A")

rc, out = tqr("next", "--worker", "W", "--envs", ALL, "--exclude", kp)
tok1 = out.splitlines()[-1] if rc == 0 else None
pr = os.path.join(TQR, "claims", taskq.safe(kr), "info")
ci = json.load(open(pr))
ci["beat"] = time.time() - 1800
json.dump(ci, open(pr, "w"))
rc2, out2 = tqr("next", "--worker", "W", "--envs", ALL, "--exclude", kp)
tok2 = out2.splitlines()[-1] if rc2 == 0 else None
check("旧 token：同名 worker 重新领到过期的任务，token 换了", rc == 0 and rc2 == 0 and out.startswith(kr) and tok1 != tok2)
for _ in range(3):
    tqr("release", "--worker", "W", "--token", tok1, kr, "--failed", "旧一轮失败")
    tqr("release", "--worker", "W", "--token", tok1, kr, "--skip", "旧一轮放不下")
check("旧 token：release --failed / --skip 不记失败、跳过，领取还在", os.path.exists(pr) and json.load(open(pr))["token"] == tok2
      and not os.path.exists(os.path.join(TQR, "failures", taskq.safe(kr))) and not os.path.exists(os.path.join(TQR, "skips", taskq.safe(kr)))
      and not os.path.exists(os.path.join(TQR, "released", taskq.safe(kr))) and strl()[kr][0] == "claimed")
rc, _ = tqr("release", "--worker", "W", "--token", tok2, kr, "--failed", "这一轮失败")
check("这一轮的 token：release --failed 记一次、交还", rc == 0 and not os.path.exists(pr)
      and len(open(os.path.join(TQR, "failures", taskq.safe(kr))).readlines()) == 1)

rows_a = [cmdtask(1, kp, []), cmdtask(4, "r/failed2", []),
          cmdtask(5, "r/a1", [kp, "nosuch/x"]), cmdtask(6, "r/a2", ["nosuch/x", kp]),
          cmdtask(7, "r/a3", [kp, "r/failed2"]), cmdtask(8, "r/a4", [kp])]
with open(os.path.join(TQR, "tasks.jsonl"), "w") as f:
    f.writelines(json.dumps(r) + "\n" for r in rows_a)
os.makedirs(os.path.join(TQR, "failures"), exist_ok=True)
old = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 4 * 3600))
open(os.path.join(TQR, "failures", taskq.safe("r/failed2")), "w").write(f"{old}\tx\t1\n" * taskq.MAX_FAIL)
sr = strl()
check("after：没做完的 + 不在队列里的，两种顺序都是 blocked", sr["r/a1"][0] == "blocked" and sr["r/a2"][0] == "blocked"
      and "nosuch/x" in sr["r/a1"][1])
check("after：没做完的 + 失败满次的 → blocked", sr["r/a3"][0] == "blocked" and "r/failed2" in sr["r/a3"][1])
check("after：只有没做完的 → waiting", sr["r/a4"] == ("waiting", f"等 {kp}"))

# 变异审查（11/3）：同一个进程里队列文件改了（health 里 advance 加任务后 feed 第二遍），after 要按新文件算，不用旧的缓存
tq_orig, taskq.TQ = taskq.TQ, TQR
try:
    rows_c = [cmdtask(1, "r/c1", ["r/cnew"])]
    taskq.write_tasks([dict(r) for r in rows_c])
    t_c = taskq.read_tasks()[0]
    st_a = taskq.state(t_c, DATA)
    taskq.write_tasks(taskq.read_tasks() + [dict(cmdtask(2, "r/cnew", []))])
    os.utime(taskq.tasks_path(), (time.time() + 5, time.time() + 5))
    st_b = taskq.state(t_c, DATA)
    check("after：同一进程里加了等的任务 → 从 blocked 变成 waiting", st_a[0] == "blocked" and st_b[0] == "waiting")
    print("   ", st_a, st_b)
finally:
    taskq.TQ = tq_orig

# 变异审查（11/3）：有缺输入的标记、但已经有人领着（标记之后重新领了）→ 算 claimed，不算 input（input 的可以被 remove、被 restage）
tq_orig, taskq.TQ = taskq.TQ, TQR
try:
    taskq.write_tasks([dict(cmdtask(1, "r/inp", []))])
    t_i = taskq.read_tasks()[0]
    os.makedirs(os.path.join(TQR, "inputs"), exist_ok=True)
    open(os.path.join(TQR, "inputs", taskq.safe("r/inp")), "w").write(f"{time.time()}\tW\tx\n")
    os.makedirs(os.path.join(TQR, "claims", taskq.safe("r/inp")), exist_ok=True)
    taskq.write_json(os.path.join(TQR, "claims", taskq.safe("r/inp"), "info"), dict(worker="W2", host="h", since=time.time(), beat=time.time()), indent=None)
    st_i = taskq.state(t_i, DATA)
    check("缺输入的标记 + 有人领着 → claimed", st_i[0] == "claimed")
finally:
    taskq.TQ = tq_orig

# ---------------- 11/4：执行机的队列存 hub 解析好的说明（add --spec-stdin、next 给 --spec） ----------------
TQS = os.path.join(T, "taskq_spec")
ENVS_ = dict(ENV, TASKQ_DIR=TQS)
os.makedirs(TQS)
open(os.path.join(TQS, "tasks.jsonl"), "w").close()


def tqs(*args, stdin=None, env=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=env or ENVS_, capture_output=True, text=True, input=stdin)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


sp_a = S.sendable(S.resolve("command", S.parse_kv(["key=sp/a", "cmd=python3 -c 1", "env=stats", "out=sp/a", 'done=["DONE|ALT"]'])))
sp_b = S.sendable(S.resolve("toy", dict(ds="D101_alpha", frag="2")))
rc, out, _ = tqs("add", "--nice", "3", "--spec-stdin", stdin=json.dumps([sp_a, sp_b]))
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("add --spec-stdin：记下说明，做法和参数也留着（还在用旧包的 worker 照旧能领）",
      rc == 0 and set(ts) == {"sp/a", sp_b["key"]} and ts["sp/a"]["spec"]["cmd"] == "python3 -c 1"
      and ts["sp/a"]["recipe"] == "command" and ts["sp/a"]["params"]["key"] == "sp/a")
check("sendable：去掉只在 hub 上有意义的字段", not set(S.SENT_DROP) & set(sp_a))
nop = dict(ENVS_, TASKQ_PLUGINS=os.path.join(T, "no_plugins"))  # 那边没有做法、实验表的插件
os.makedirs(nop["TASKQ_PLUGINS"], exist_ok=True)
rc, out, err = tqs("next", "--worker", "s1", "--envs", "stats", env=nop)
lines = out.splitlines()
check("next：带说明的给 run_task.sh --spec <JSON>，不用查做法", rc == 0 and lines[0] == "sp/a" and lines[1].startswith("--spec ")
      and json.loads(__import__("shlex").split(lines[1])[1])["key"] == "sp/a")
rc, sh, err = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "shell", "--spec", json.dumps(sp_a)], env=nop,
                             capture_output=True, text=True).returncode, None, None
r1 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "shell", "--spec", json.dumps(sp_b)], env=nop, capture_output=True, text=True)
r2 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "shell", *S.kv_args("toy", dict(ds="D101_alpha", frag="2"))], env=ENVS_,
                    capture_output=True, text=True)
check("spec.py shell --spec 和 <做法> k=v 给出同样的 shell 语句（没有插件也行）", rc == 0 and r1.returncode == 0 and r1.stdout == r2.stdout)
os.makedirs(os.path.join(DATA, "sp/a"), exist_ok=True)
r3 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "done", "--spec", json.dumps(sp_a)], env=nop)
open(os.path.join(DATA, "sp/a", "ALT"), "w").close()
r4 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "done", "--spec", json.dumps(sp_a)], env=nop)
check("spec.py done --spec：按说明的 done 判断（a|b 有一个就行）", r3.returncode == 1 and r4.returncode == 0)
r5 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "shell", "--spec", '{"key": "x"}'], env=nop, capture_output=True, text=True)
check("spec.py shell --spec：缺字段退出码 2", r5.returncode == 2 and "缺字段" in r5.stderr)
tqs("set-nice", "--nice", "9", "sp/a")
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("set-nice 以后说明还在", ts["sp/a"]["nice"] == 9 and ts["sp/a"]["spec"] == sp_a)
sp_a2 = dict(sp_a, cmd="python3 -c 2")
rc, out, _ = tqs("add", "--nice", "3", "--spec-stdin", stdin=json.dumps(sp_a2))
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("add --spec-stdin 已有的：换成新说明，档位、顺序不变", rc == 0 and "换了说明" in out and ts["sp/a"]["spec"]["cmd"] == "python3 -c 2"
      and ts["sp/a"]["nice"] == 9)
open(os.path.join(TQS, "tasks.jsonl"), "a").write(json.dumps(dict(order=99, nice=1, key="old/c", exp=None, recipe="command",
                                                                    params=dict(key="old/c", cmd="true", env="stats", out="old/c", done="D"))) + "\n")
rc, out, _ = tqs("add", "--nice", "3", "--spec-stdin", stdin=json.dumps(S.sendable(S.resolve("command", dict(key="old/c", cmd="true", env="stats", out="old/c", done="D")))))
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("add --spec-stdin：已有的老记录（没有说明）补上说明", rc == 0 and ts["old/c"].get("spec", {}).get("key") == "old/c" and ts["old/c"]["nice"] == 1)
rc, out, _ = tqs("add", "--nice", "3", "--spec-stdin", "--update-only",
                stdin=json.dumps([dict(sp_a, cmd="python3 -c 3"), S.sendable(S.resolve("command", dict(key="gone/x", cmd="true", env="stats", out="gone/x", done="D")))]))
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("add --update-only：只换已有的说明，不在队列里的不加", rc == 0 and "gone/x" not in ts and ts["sp/a"]["spec"]["cmd"] == "python3 -c 3")
rc, _, err = tqs("set", sp_b["key"], "env=alt2")
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("set：hub 送来的说明不改（改总队列再 resend，11/4）", rc == 2 and "resend" in err and ts[sp_b["key"]]["spec"] == sp_b
      and "env" not in ts[sp_b["key"]]["params"])
rc, out, _ = tqs("show", "sp/a")
check("show：带说明的显示 run_task.sh --spec", rc == 0 and "run_task.sh --spec" in out)

# ---------------- 11/4 审查：remove 看领取、连记录一起删；坏说明；--update-only；迟到的交还 ----------------
TQX = os.path.join(T, "taskq_x")
os.makedirs(TQX)
open(os.path.join(TQX, "tasks.jsonl"), "w").close()


def tqx(*args, stdin=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQX, TASKQ_DATA=DATA),
                       capture_output=True, text=True, input=stdin)
    return r.returncode, (r.stdout + r.stderr).strip()


def sx():
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=dict(ENV, TASKQ_DIR=TQX, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return {t["key"]: t for t in taskq.S.json_in(r.stdout)["tasks"]}


def cspec(key, **kw):
    return S.sendable(S.resolve("command", dict(key=key, cmd="true", env="stats", out=f"x/{key}", done="DONE", **kw)))


def xp(sub, key):
    return os.path.join(TQX, sub, taskq.safe(key))


xa, xb = cspec("x/a"), cspec("x/b")
tqx("add", "--nice", "0", "--spec-stdin", stdin=json.dumps([xa, xb]))
rc, out = tqx("next", "--worker", "w1", "--envs", "stats")
tok = out.splitlines()[-1]
tqx("stop", "x/a")
rc2, out2 = tqx("remove", "x/a")
check("remove：停了、worker 还领着 → 不删（退出码 3），领取还在", rc == 0 and out.startswith("x/a\n") and rc2 == taskq.REFUSED
      and os.path.isdir(xp("claims", "x/a")) and sx()["x/a"]["state"] == "stopped")
os.makedirs(os.path.join(DATA, "x/x/a"), exist_ok=True)
open(os.path.join(DATA, "x/x/a", "DONE"), "w").close()
rc, out = tqx("remove", "x/a")
check("remove：done 齐了、worker 还领着（还在写结果）→ 不删", rc == taskq.REFUSED and sx()["x/a"]["state"] == "done")
shutil.rmtree(xp("claims", "x/a"))  # 领取过期后被别人领走、交还了
rc, out = tqx("release", "--worker", "w1", "--token", tok, "x/a", "--failed", "迟到的失败")
check("迟到的 release --failed（领取已经没了）：不记失败，说一句", rc == 0 and not os.path.exists(xp("failures", "x/a")) and "不记失败" in out)

now_s = time.strftime("%F %T")
for sub, line in (("failures", f"{now_s}\t作业 1\tx\n"), ("inputs", f"{time.time()}\tW\tx\n"), ("released", f"{time.time()} W\n"),
                  ("skips", f"{now_s}\tw\t放不下\n")):
    os.makedirs(os.path.join(TQX, sub), exist_ok=True)
    open(xp(sub, "x/b"), "w").write(line)
tqx("stop", "x/b")
os.makedirs(xp("claims", "x/b"))
taskq.write_json(os.path.join(xp("claims", "x/b"), "info"), dict(worker="old", host="h", since=time.time() - 7200, beat=time.time() - 7200), indent=None)
rc, out = tqx("remove", "x/b")
left = [sub for sub in ("failures", "stopped", "inputs", "released", "skips", "claims") if os.path.exists(xp(sub, "x/b"))]
tqx("add", "--nice", "0", "--spec-stdin", stdin=json.dumps(xb))
check("remove：连同失败、停止、缺输入、交还、跳过记录和过期的领取一起删；再加回来是新任务（free）", rc == 0 and not left
      and sx()["x/b"]["state"] == "free" and sx()["x/b"]["failures"] == 0)
tqx("add", "--nice", "0", "--recipe", "command", "key=x/c", "cmd=true", "env=stats", "out=x/x/c", "done=DONE")
for sub in ("failures", "skips"):
    open(xp(sub, "x/c"), "w").write(f"{now_s}\tw\tx\n")
rc, out = tqx("remove", "x/c")
check("remove：总队列的记录（没有 spec）删失败记录，跳过留着", rc == 0 and not os.path.exists(xp("failures", "x/c"))
      and os.path.exists(xp("skips", "x/c")))

rc, out = tqx("add", "--nice", "0", "--spec-stdin", stdin=json.dumps(dict(cspec("x/d"), done=5)))
rc2, out2 = tqx("add", "--nice", "0", "--spec-stdin", stdin=json.dumps(dict(cspec("x/e"), res="abc")))
rc3, out3 = tqx("add", "--nice", "0", "--spec-stdin", stdin="{oops")
rc4, out4 = tqx("add", "--nice", "0", "--spec-stdin", stdin=json.dumps(dict(cspec("x/e"), vars=["A=1"])))
check("add --spec-stdin：done、res、vars 类型不对、不是 JSON → 加不进去（退出码 2）", (rc, rc2, rc3, rc4) == (2, 2, 2, 2)
      and "done" in out and "res" in out2 and "JSON" in out3 and "vars" in out4 and not {"x/d", "x/e"} & set(sx()))
with open(os.path.join(TQX, "tasks.jsonl"), "a") as f:
    f.write(json.dumps(dict(order=99, nice=-5, key="x/f", exp=None, recipe="command", params={}, spec=dict(cspec("x/f"), done=5))) + "\n")
rc, out = tqx("status")
rc2, out2 = tqx("next", "--worker", "w2", "--envs", "stats")
check("队列里有一条坏说明：status 照常、显示 bad；next 跳过它领别的", rc == 0 and "x/f" in out and "bad" in out
      and rc2 == 0 and out2.startswith("x/b\n"))

rc, out = tqx("add", "--nice", "0", "--spec-stdin", "--update-only",
              stdin=json.dumps([dict(xb, out="x/new"), dict(cspec("x/h"), done=5), cspec("x/zz"), dict(xa, cmd="python3 -c 9")]))
tx = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQX, "tasks.jsonl"))}
check("--update-only：坏的逐个报、别的照换；队列里没有的打出来；在训的 out 变了先不换", rc == 2 and "换不了：x/h" in out
      and "队列里没有，不换：x/zz" in out and "换了说明：x/a" in out and "在训" in out and tx["x/a"]["spec"]["cmd"] == "python3 -c 9"
      and tx["x/b"]["spec"]["out"] == xb["out"] and "x/h" not in tx and "x/zz" not in tx)
rc, out = tqx("add", "--nice", "0", "--spec-stdin", "--update-only", stdin=json.dumps([dict(xb, cmd="python3 -c 8")]))
tx = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQX, "tasks.jsonl"))}
check("--update-only：在训的只改命令（out、done 不变）照换", rc == 0 and tx["x/b"]["spec"]["cmd"] == "python3 -c 8")

# ---------------- 11/4：next --log-tag（pueue-host 的分段日志名不是 worker 名）、status --json、proto.claim_alive ----------------
import proto  # noqa: E402
TQL = os.path.join(T, "taskq_l")
os.makedirs(TQL)
open(os.path.join(TQL, "tasks.jsonl"), "w").close()


def tql(*args, stdin=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQL, TASKQ_DATA=DATA),
                       capture_output=True, text=True, input=stdin)
    return r.returncode, (r.stdout + r.stderr).strip()


tql("add", "--nice", "0", "--spec-stdin", stdin=json.dumps([cspec("l/a"), cspec("l/b")]))
rc, out = tql("next", "--worker", "site-pueue", "--envs", "stats", "--log-tag", "gpuhost-202611041200")
rc2, out2 = tql("next", "--worker", "site-pueue", "--envs", "stats")
ia = json.load(open(os.path.join(TQL, "claims", taskq.safe("l/a"), "info")))
ib = json.load(open(os.path.join(TQL, "claims", taskq.safe("l/b"), "info")))
check("next --log-tag：领取记录多一个 log 字段；不给时没有这个字段（老格式不变）",
      rc == 0 and rc2 == 0 and ia.get("log") == "gpuhost-202611041200" and ia["worker"] == "site-pueue" and "log" not in ib
      and set(ia) <= set(proto.CLAIM_FIELDS) and set(ib) <= set(proto.CLAIM_FIELDS))
lg = os.path.join(DATA, proto.TASK_LOGS)
os.makedirs(lg, exist_ok=True)
open(os.path.join(lg, f"{taskq.safe('l/a')}__gpuhost-202611041200_part1.log"), "w").write("x\n")
open(os.path.join(lg, f"{taskq.safe('l/b')}__site-pueue_seg1.log"), "w").write("x\n")
old_t = time.time() - 4000
os.utime(os.path.join(lg, f"{taskq.safe('l/b')}__site-pueue_seg1.log"), (old_t, old_t))
check("task_log_age：有 log 按它找；没有按 worker 名找（_seg 老名字也认）；都没有 → None",
      taskq.task_log_age(DATA, "l/a", "site-pueue", "gpuhost-202611041200") < 60
      and taskq.task_log_age(DATA, "l/a", "site-pueue") is None
      and 3900 < taskq.task_log_age(DATA, "l/b", "site-pueue") < 4100
      and taskq.task_log_age(DATA, "l/b", None) is None)
rc, out = tql("status", "--json")
lines = out.splitlines()
js = taskq.S.json_in(out)
rows = {t["key"]: t for t in js["tasks"]}
rc_old, out_old = tql("--json-status")
check("status --json：一行 @@JSON，带 version 和 local_status 的顶层字段；task_log_age 按 log 字段找；老的 --json-status 照旧",
      rc == 0 and len(lines) == 1 and lines[0].startswith(proto.JSON_MARK) and js["version"] == proto.VERSION
      and set(proto.STATUS_TOP) <= set(js) and rows["l/a"]["task_log_age"] < 60 and rows["l/b"]["task_log_age"] > 3900
      and rc_old == 0 and "version" not in taskq.S.json_in(out_old) and {t["key"] for t in taskq.S.json_in(out_old)["tasks"]} == set(rows))


def old_alive(ci, made, now, age, stale, fresh, half):  # 11/4 前 claim_state 的写法
    if ci is None:
        return not now - made > half
    if ci.get("assigned"):
        return True
    return now - ci.get("beat", 0) < stale or (age is not None and age < fresh)


now = time.time()
grid = [(ci, made, age) for ci in (None, dict(assigned="site-pueue"), dict(beat=now - 10), dict(beat=now - 1199), dict(beat=now - 1201),
                                   dict(beat=now - 9000), dict(worker="w"))
        for made in (now - 10, now - 299, now - 301) for age in (None, 10, 899, 901)]
check("proto.claim_alive 和 11/4 前 claim_state 的判断一样（默认值和改过的阈值）",
      all(proto.claim_alive(ci, made, now, age) == old_alive(ci, made, now, age, 1200, 900, 300) for ci, made, age in grid)
      and all(proto.claim_alive(ci, made, now, age, stale=100, fresh=50, half=20) == old_alive(ci, made, now, age, 100, 50, 20)
              for ci, made, age in grid))

# next 领了、还没打印就被停（11/5）：撤掉这次领取，不留到过期；退出仍是被信号停的
TQK = os.path.join(T, "taskq_kill")
write_jsonl(os.path.join(TQK, "tasks.jsonl"), [(1, 0, "D103_y/V_save/0")])
KILL_NEXT = ("import os,signal,sys\nsys.path.insert(0, sys.argv.pop(1))\nimport taskq\n_w = taskq.write_json\n"
             "def w(p, *a, **k):\n    _w(p, *a, **k)\n    os.kill(os.getpid(), signal.SIGTERM)\n"
             "taskq.write_json = w\nsys.exit(taskq.main(sys.argv[1:]))")
r = subprocess.run([sys.executable, "-c", KILL_NEXT, HERE, "next", "--worker", "600", "--envs", ALL], env=dict(ENV, TASKQ_DIR=TQK),
                   capture_output=True, text=True)
left = os.listdir(os.path.join(TQK, "claims")) if os.path.isdir(os.path.join(TQK, "claims")) else []
r2 = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "601", "--envs", ALL], env=dict(ENV, TASKQ_DIR=TQK),
                    capture_output=True, text=True)
check("next 领了、打印前被 SIGTERM：撤掉领取（别的 worker 马上能领），退出码仍是被信号停的",
      r.returncode == -15 and not r.stdout and not left and r2.returncode == 0 and r2.stdout.startswith("D103_y/V_save/0"))

# ---------------- 11/7：同一档里有存档（接着训到一半的）先领，档位仍然优先 ----------------
TQP = os.path.join(T, "taskq_progress")
os.makedirs(TQP)
open(os.path.join(TQP, "tasks.jsonl"), "w").close()
EP = dict(ENV, TASKQ_DIR=TQP, TASKQ_DATA=DATA)


def tqp(*args, stdin=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=EP, capture_output=True, text=True, input=stdin)
    return r.returncode, r.stdout.strip()


def pspec(key):
    return dict(S.sendable(S.resolve("command", dict(key=key, cmd="true", env="stats", out=f"p/{key}", done="DONE"))), resume=["ckpt_*.pt"])


tqp("add", "--nice", "70", "--spec-stdin", stdin=json.dumps([pspec("p/new1"), pspec("p/half"), pspec("p/new2")]))
os.makedirs(os.path.join(DATA, "p/p/half"), exist_ok=True)
open(os.path.join(DATA, "p/p/half", "ckpt_latest.pt"), "w").close()
rc, out = tqp("next", "--worker", "p1", "--envs", "stats")
rc2, out2 = tqp("next", "--worker", "p2", "--envs", "stats")
check("next：同一档里有存档的先领（排在先加的、没开始的前面），其余照加的先后",
      rc == 0 and out.startswith("p/half\n") and rc2 == 0 and out2.startswith("p/new1\n"))
tqp("add", "--nice", "-100", "--spec-stdin", stdin=json.dumps(pspec("p/urgent")))
os.makedirs(os.path.join(DATA, "p/p/new2"), exist_ok=True)
open(os.path.join(DATA, "p/p/new2", "ckpt_latest.pt"), "w").close()
rc, out = tqp("next", "--worker", "p3", "--envs", "stats")
rc2, out2 = tqp("next", "--worker", "p4", "--envs", "stats")
check("next：更靠前的档（没有存档的新任务）仍然先领，有存档的只在同一档里提前",
      rc == 0 and out.startswith("p/urgent\n") and rc2 == 0 and out2.startswith("p/new2\n"))

# 推理之类没写 resume 的：这台跑过（有分段日志）、交还的同一档先领；失败过的不提前；a/b 的日志不算 a/b/c 的（11/7）
def sspec(key):
    return S.sendable(S.resolve("command", dict(key=key, cmd="true", env="stats", out=f"s/{key}", done="DONE")))


tqp("add", "--nice", "80", "--spec-stdin", stdin=json.dumps([sspec("s/new"), sspec("s/half"), sspec("s/bad"), sspec("s/half/sub")]))
LOGS = os.path.join(DATA, "logs/tasks")
os.makedirs(LOGS, exist_ok=True)
for k in ("s/half", "s/bad"):
    open(os.path.join(LOGS, k.replace("/", "__") + "__host-1107_part1.log"), "w").close()
os.makedirs(os.path.join(TQP, "failures"), exist_ok=True)
with open(os.path.join(TQP, "failures", "s__bad"), "w") as f:
    f.write("2026-11-07 10:00:00\t作业 p0\t退出码 1\n")
got = [tqp("next", "--worker", f"s{i}", "--envs", "stats")[1].split("\n")[0] for i in range(4)]
check("next：没有 resume 的步骤跑过、交还的同一档先领；失败过的、只有子 key 日志的照加的先后",
      got == ["s/half", "s/new", "s/bad", "s/half/sub"])

print("\n全部通过" if ok else "\n有失败")
sys.exit(0 if ok else 1)
