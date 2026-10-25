#!/usr/bin/env python3
"""taskq.py 的测试（2026-10-17，10/18 改成任务说明）：临时目录里的假队列、假训练结果，不连任何机器。  python3 scripts/queue/test_taskq.py"""
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
T = tempfile.mkdtemp()
TQ, DATA = os.path.join(T, "taskq"), os.path.join(T, "data")
RES = os.path.join(DATA, "nnUNet", "results")
ENV = dict(os.environ, TASKQ_DIR=TQ, BRAIN_TUMOR=DATA, TASKQ_MACHINE="dicc")  # 测试里本机当队列所在的机器
ENV.pop("nnUNet_results", None)
sys.path.insert(0, HERE)
os.environ.update(TASKQ_DIR=TQ)
import taskq  # noqa: E402
import experiments as E  # noqa: E402

ok = True


def check(name, cond):
    global ok
    print(("通过：" if cond else "失败：") + name)
    ok &= bool(cond)


def tq(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=ENV, capture_output=True, text=True)
    return r.returncode, r.stdout.strip()


def fold(ds, tr, f):
    d = os.path.join(RES, ds, f"{tr}__nnUNetResEncUNetMPlans__3d_fullres", f"fold_{f}")
    os.makedirs(d, exist_ok=True)
    return d


ALL = "nnunet,umamba,swinunetr"


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for order, nice, key, *env in rows:
            ds, tr, fo = key.split("/")
            p = dict(dataset=ds, trainer=tr, fold=fo, **(dict(env=env[0]) if env else {}))
            f.write(json.dumps(dict(order=order, nice=nice, key=key, exp=None, recipe="nnunet", params=p)) + "\n")


tasks = [(10, 0, "Dataset121_X/nnUNetTrainer_save10/0"), (11, 0, "Dataset121_X/nnUNetTrainer_save10/1"),
         (12, 0, "Dataset121_X/nnUNetTrainer_save10/2"), (5, 50, "Dataset103_Y/nnUNetTrainer_save10/0"),
         (1, 2000, "Dataset101_Z/nnUNetTrainer_SwinUNETR/0", "swinunetr")]
write_jsonl(os.path.join(TQ, "tasks.jsonl"), tasks)
for t in tasks:
    fold(*t[2].split("/"))

# fold 0 已训完
d0 = fold("Dataset121_X", "nnUNetTrainer_save10", "0")
open(os.path.join(d0, "checkpoint_final.pth"), "w").close()
os.makedirs(os.path.join(d0, "validation"))
open(os.path.join(d0, "validation", "summary.json"), "w").write("{}")
# fold 1 的训练日志刚更新过（别处在训，比如附加训练）
open(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "1"), "training_log_x.txt"), "w").write("x")

rc, out = tq("next", "--worker", "100", "--envs", ALL)
check("按档位先领、跳过训完的和日志刚更新的：领到 fold 2", rc == 0 and out.startswith("Dataset121_X/nnUNetTrainer_save10/2\n"))
rc, out = tq("next", "--worker", "101", "--envs", ALL)
check("已经被领的不再领：下一个是档位 50 的", rc == 0 and out.startswith("Dataset103_Y/"))
rc, out = tq("next", "--worker", "103", "--envs", ALL)
check("照顺序领 SwinUNETR（环境 swinunetr）", rc == 0 and out.startswith("Dataset101_Z/nnUNetTrainer_SwinUNETR/0\n") and "env=swinunetr" in out)
rc, out = tq("next", "--worker", "102", "--envs", ALL)
check("没有能领的：退出码 10", rc == 10 and out == "")

# 领取过期（心跳 30 分钟前、日志也没更新）→ 可以重新领
p = os.path.join(TQ, "claims", taskq.safe("Dataset103_Y/nnUNetTrainer_save10/0"), "info")
ci = json.load(open(p))
ci["beat"] = time.time() - 1800
json.dump(ci, open(p, "w"))
rc, out = tq("next", "--worker", "104", "--envs", ALL)
check("心跳过期、日志也没更新的重新领", rc == 0 and out.startswith("Dataset103_Y/"))
check("重新领后记的是新的 worker", json.load(open(p))["worker"] == "104")

# 心跳过期但训练日志刚更新 → 仍算在训
key2 = "Dataset121_X/nnUNetTrainer_save10/2"
p2 = os.path.join(TQ, "claims", taskq.safe(key2), "info")
ci = json.load(open(p2))
ci["beat"] = time.time() - 1800
json.dump(ci, open(p2, "w"))
open(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), "training_log_y.txt"), "w").write("y")
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
tq("release", "--worker", "104", "Dataset103_Y/nnUNetTrainer_save10/0", "--failed", "退出码 1")
tq("release", "--worker", "104", "Dataset103_Y/nnUNetTrainer_save10/0", "--failed", "退出码 1")
rc, _ = tq("next", "--worker", "110", "--envs", ALL)
check("3 小时内失败 3 次：整个队列暂停领任务（退出码 12）", rc == 12)
for fp in glob.glob(os.path.join(TQ, "failures", "*")):  # 把失败记录改成 4 小时前
    old = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 4 * 3600))
    lines = open(fp).readlines()
    open(fp, "w").write("".join(old + ln[19:] for ln in lines))
for f in os.listdir(fold("Dataset121_X", "nnUNetTrainer_save10", "2")):
    os.utime(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), f), (time.time() - 3600,) * 2)
rc, out = tq("next", "--worker", "106", "--envs", ALL)
check("失败 1 次的还能领、失败 2 次的不领", rc == 0 and out.startswith(key2))
tq("release", "--worker", "106", key2)
rc, out = tq("next", "--worker", "106", "--envs", ALL, "--exclude", key2)
check("本作业失败过的（--exclude）不再领", rc == 10)

# 交还之后日志没再更新：马上能领；交还之后日志又更新了：算队列外有人在训
open(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), "training_log_y.txt"), "w").write("y")
rc, out = tq("next", "--worker", "120", "--envs", ALL)
check("刚领的（日志刚更新）照常领到", rc == 0 and out.startswith(key2))
tq("release", "--worker", "120", key2)
rc, out = tq("next", "--worker", "121", "--envs", ALL)
check("刚交还、日志 15 分钟内更新过，也马上能领", rc == 0 and out.startswith(key2))
tq("release", "--worker", "121", key2)
time.sleep(6)
open(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), "training_log_y.txt"), "w").write("y2")
rc, out = tq("next", "--worker", "122", "--envs", ALL)
check("交还之后日志又更新了（队列外在训）：不领", rc == 10)

# 排在前面的折刚停下（日志新、没人领），后面还有能领的：开始后的等待时间内先等它
open(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "1"), "training_log_x.txt"), "w").write("x")  # fold 1 刚停下
os.remove(os.path.join(TQ, "failures", taskq.safe("Dataset103_Y/nnUNetTrainer_save10/0")))  # 让后面有一个能领的
rc, out = tq("next", "--worker", "130", "--envs", ALL, "--free-slots", "1", "--patience-until", str(time.time() + 600))
check("空 1 个位置、前面有 1 个刚停下的：等（退出码 11）", rc == 11)
rc, out = tq("next", "--worker", "130", "--envs", ALL, "--free-slots", "3", "--patience-until", str(time.time() + 600))
check("空的位置比刚停下的多：照常领后面的", rc == 0 and out.startswith("Dataset103_Y/"))
tq("release", "--worker", "130", "Dataset103_Y/nnUNetTrainer_save10/0")
rc, out = tq("next", "--worker", "131", "--envs", ALL, "--free-slots", "1", "--patience-until", str(time.time() - 1))
check("等待时间过了：领后面的", rc == 0 and out.startswith("Dataset103_Y/"))
tq("release", "--worker", "131", "Dataset103_Y/nnUNetTrainer_save10/0")
os.remove(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "1"), "training_log_x.txt"))

for f in os.listdir(fold("Dataset121_X", "nnUNetTrainer_save10", "2")):
    os.utime(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), f), (time.time() - 3600,) * 2)
rc, out = tq("next", "--worker", "107", "--envs", ALL)
check("照顺序领 fold 1", rc == 0 and out.startswith("Dataset121_X/nnUNetTrainer_save10/1\n"))
rc, out = tq("next", "--worker", "108", "--envs", ALL)
check("再下一个是 fold 2（日志 1 小时前更新过，没人领）", rc == 0 and out.startswith(key2))
# 并发领：同时起 6 个，只有 1 个领到
procs = [subprocess.Popen([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", f"2{i}", "--envs", ALL],
                          env=ENV, stdout=subprocess.PIPE, text=True) for i in range(6)]
outs = [p.communicate()[0].strip() for p in procs]
check("同时领，只有一个领到", sum(1 for o in outs if o) == 1)

# ---------------- 10/18：take（box 替 lab 领）、ttl、add、set-nice、kind any、remote ----------------
TQ2 = os.path.join(T, "taskq2")
ENV2 = dict(ENV, TASKQ_DIR=TQ2)


def tq2(*args, env=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=env or ENV2, capture_output=True, text=True)
    return r.returncode, r.stdout.strip()


os.makedirs(TQ2)
write_jsonl(os.path.join(TQ2, "tasks.jsonl"), [(486191, 0, "Dataset116_METrq1/nnUNetTrainer_save10/4"),
                                               (486300, 2000, "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0", "swinunetr")])
os.environ["TASKQ_DIR"] = TQ2
rc, out = tq2("add", "--nice", "50", "--exp", "umamba", "--folds", "0")
rc2, out2 = tq2("add", "--nice", "50", "--exp", "umamba", "--folds", "0")
ts = taskq.read_tasks(os.path.join(TQ2, "tasks.jsonl"))
um = ts[1]
check("add --exp：照实验表（U-Mamba：环境 umamba、实验 umamba），按档位排进去，已有的跳过",
      rc == 0 and [t["key"].split("/")[1] for t in ts] == ["nnUNetTrainer_save10", "nnUNetTrainer_UMambaEnc", "nnUNetTrainer_SwinUNETR"]
      and um["exp"] == "umamba" and um["params"]["env"] == "umamba" and um["order"] == 486301 and "已有，跳过" in out2)
rc, out = tq2("add", "--nice", "0", "--exp", "nosuch")
check("add：实验表里没有的实验报错", rc != 0)
rc, out = tq2("add", "--nice", "0", "Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0")
check("add：不给 --exp / --recipe 报错", rc != 0)
rc, out = tq2("set-nice", "--nice", "100", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0")
ts = taskq.read_tasks(os.path.join(TQ2, "tasks.jsonl"))
check("set-nice：改档位后重新排序", rc == 0 and "SwinUNETR" in ts[2]["key"] and ts[2]["nice"] == 100)
rc, out = tq2("set-nice", "--nice", "1", "Dataset101_GLIrq1/nnUNetTrainer_save10/9")
check("set-nice：没有的 key 退出码 1", rc == 1)

UM = "Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0"
SWK = "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0"
rc, out = tq2("take", "--worker", "lab", "--ttl", "86400", UM)
ci = json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))
check("take：领到指定的一折，记下 worker、ttl", rc == 0 and out.startswith(UM) and ci["worker"] == "lab" and ci["ttl"] == 86400)
rc, out = tq2("next", "--worker", "500", "--envs", ALL, "--exclude", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0", "Dataset116_METrq1/nnUNetTrainer_save10/4")
check("lab 领了的，DICC 作业不再领", rc == 10)
ci["beat"] = time.time() - 3 * 3600
json.dump(ci, open(os.path.join(TQ2, "claims", taskq.safe(UM), "info"), "w"))
rc, out = tq2("next", "--worker", "500", "--envs", ALL, "--exclude", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0", "Dataset116_METrq1/nnUNetTrainer_save10/4")
check("ttl 24 小时：心跳 3 小时前仍算在训（默认 20 分钟会过期）", rc == 10)
rc, out = tq2("take", "--worker", "lab", "--ttl", "86400", UM)
ci2 = json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))
check("take 自己领的：续上、领取时间不变", rc == 0 and ci2["since"] == ci["since"] and time.time() - ci2["beat"] < 10)
rc, out = tq2("take", "--worker", "other", UM)
check("take 别人领的：领不到（退出码 10）", rc == 10)
ci2["beat"] = time.time() - 2 * 86400
json.dump(ci2, open(os.path.join(TQ2, "claims", taskq.safe(UM), "info"), "w"))
rc, out = tq2("next", "--worker", "501", "--envs", ALL, "--exclude", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0", "Dataset116_METrq1/nnUNetTrainer_save10/4")
check("超过 ttl 的领取过期，DICC 作业能接着领", rc == 0 and out.startswith(UM))
rc, out = tq2("take", "--worker", "lab", "Dataset116_METrq1/nnUNetTrainer_save10/9")
check("take 队列里没有的：退出码 10", rc == 10)
d = fold("Dataset116_METrq1", "nnUNetTrainer_save10", "4")
open(os.path.join(d, "checkpoint_final.pth"), "w").close()
os.makedirs(os.path.join(d, "validation"), exist_ok=True)
open(os.path.join(d, "validation", "summary.json"), "w").write("{}")
rc, out = tq2("take", "--worker", "lab", "Dataset116_METrq1/nnUNetTrainer_save10/4")
check("take 已训完的：领不到", rc == 10 and "done" in out)
rc, out = tq2("next", "--worker", "502", "--envs", ALL)
check("kind any：什么种类都领（领到 SwinUNETR）", rc == 0 and out.startswith("Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0"))
s2 = taskq.S.json_in(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=ENV2, capture_output=True, text=True).stdout)
row = next(t for t in s2["tasks"] if t["key"] == UM)
rc, _ = tq2("release", "--worker", "lab", SWK, "--skip", "训练任务 5 Failed (1)")
s3 = taskq.S.json_in(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=ENV2, capture_output=True, text=True).stdout)
row3 = next(t for t in s3["tasks"] if t["key"] == SWK)
check("release --skip：记在 skips/、不算失败；--json-status 带 skipped_by 和 24 小时内的次数",
      rc == 0 and row3["skipped_by"] == ["lab"] and row3["failures"] == 0
      and s3["recent_skips"] == {"lab": 1} and not os.path.exists(os.path.join(TQ2, "failures", taskq.safe(SWK))))
check("--json-status 带 worker、host、beat", row["worker"] == "501" and "host" in row and row["beat"])

# worker 的调用方式（dicc_worker.sh 实际传的参数）不变：在跑的 worker 每次都调用仓库里的 taskq.py
rc, out = tq2("next", "--worker", "503", "--envs", ALL, "--free-slots", "2", "--patience-until", str(time.time() + 1200))
check("worker：next 带 --free-slots / --patience-until，没有能领的退出码 10", rc == 10)
rc, _ = tq2("beat", "--worker", "502", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0")
check("worker：beat 退出码 0", rc == 0)
rc, _ = tq2("release", "--worker", "502", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0")
check("worker：release 退出码 0、记录删掉", rc == 0 and not os.path.exists(os.path.join(TQ2, "claims", taskq.safe("Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0"))))
rc, _ = tq2("release", "--worker", "501", UM, "--failed", "退出码 1")
check("worker：release --failed 记失败", rc == 0 and len(open(os.path.join(TQ2, "failures", taskq.safe(UM))).readlines()) == 1)
rc, out = tq2("next", "--worker", "504", "--envs", ALL, "--exclude", UM)
check("lab 跳过的折（skips/）DICC 作业照样领", rc == 0 and out.startswith(SWK))
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

# box 上读远端状态：在 DICC 仓库里跑 taskq.py（假 ssh：把命令放到本机跑）
fake = os.path.join(T, "fakessh")
open(fake, "w").write("#!/bin/bash\n# 假 ssh：去掉选项和主机名，命令里的仓库路径换成本仓库\n"
                      "while [[ $1 == -o ]]; do shift 2; done; shift\n"
                      f"cmd=${{1//\\~\\/Projects\\/PhD/{os.path.dirname(os.path.dirname(HERE))}}}\n"
                      f"cmd=$(printf '%s' \"$cmd\" | sed 's#BRAIN_TUMOR=[^ ]*#BRAIN_TUMOR={DATA}#')\n"
                      f"export TASKQ_DIR={TQ2} TASKQ_MACHINE=dicc\nbash -c \"$cmd\"\n")
os.chmod(fake, 0o755)
os.environ["TASKQ_SSH"] = fake
st = taskq.remote_status("dicc")
check("remote_status：在远端仓库里跑", st is not None and len(st["tasks"]) == 3)
rc_out = taskq.remote("dicc", f"take --worker lab --ttl 86400 {UM}")
ci4 = json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))
check("remote：take 经 ssh 跑、领到，记下 worker=lab、ttl", rc_out is not None and rc_out[0] == 0 and ci4["worker"] == "lab" and ci4["ttl"] == 86400)
EMPTY = os.path.join(T, "box_tq")  # box 上：只有 LAB 开关，没有任务表
os.makedirs(EMPTY)
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "add", "--nice", "100", "--recipe", "nnunet",
                    "dataset=Dataset113_PEDsrq1", "trainer=nnUNetTrainer_save10", "fold=0"],
                   env=dict(os.environ, TASKQ_DIR=EMPTY, TASKQ_MACHINE="box"), capture_output=True, text=True)
check("在 box 上（不是队列所在的机器）add 转到 DICC 那份（不在本机另建）", r.returncode == 0 and "加入" in r.stdout
      and not os.path.exists(os.path.join(EMPTY, "tasks.jsonl")) and "Dataset113_PEDsrq1" in open(os.path.join(TQ2, "tasks.jsonl")).read())
r = taskq.remote("dicc", "nosuchcmd")
check("remote：标准输出、标准错误分开返回", r is not None and r[0] == 2 and r[1] == "" and "nosuchcmd" in r[2])
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "show", "--json", "nosuch/key"],
                   env=dict(os.environ, TASKQ_MACHINE="box"), capture_output=True, text=True)
check("转发：那边的标准输出、退出码照样传回来", r.returncode == 1 and "队列里没有" in r.stdout)
J = '{"tasks": [], "enabled": true}'
check("@@JSON 行前后混着别的输出：只读那一行", taskq.S.json_in(f"警告：x {{1}}\n@@JSON {J}\n{{别的}}\n") == json.loads(J))
check("没有 @@JSON 行：不认（不找第一个 {）", taskq.S.json_in(f"x\n{J}\n") is None)
remote_orig = taskq.remote
taskq.remote = lambda h, a, timeout=0: (0, f"别的输出\n{J}\n", "")
check("remote_status：输出里没有 @@JSON 行 → None", taskq.remote_status("dicc") is None)
taskq.remote = lambda h, a, timeout=0: (0, f"别的输出 {{x}}\n@@JSON {J}\n", "")
check("remote_status：读 @@JSON 那一行", taskq.remote_status("dicc") == json.loads(J))
taskq.remote = remote_orig
os.environ["TASKQ_SSH"] = "false"
check("ssh 不通（退出码非 0）：remote_status 返回 None", taskq.remote_status("dicc") is None)
open(fake + "255", "w").write("#!/bin/bash\nexit 255\n")
os.chmod(fake + "255", 0o755)
os.environ["TASKQ_SSH"] = fake + "255"
check("ssh 退出 255：remote 返回 None", taskq.remote("dicc", "status") is None)
del os.environ["TASKQ_SSH"]


# ---------------- 10/18：任务说明（next、set、show、add --exp / --recipe） ----------------
TQ3 = os.path.join(T, "taskq3")
ENV3 = dict(ENV, TASKQ_DIR=TQ3)


def tq3(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=ENV3, capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


os.makedirs(TQ3)
open(os.path.join(TQ3, "tasks.jsonl"), "w").close()
rc, out, _ = tq3("add", "--nice", "0", "--exp", "t15men", "--folds", "1", "2")
ts = taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))
check("add --exp --folds：从实验表展开、记下实验 id", rc == 0 and [t["key"] for t in ts] ==
      ["Dataset111_MENrq1/nnUNetTrainer_save10_n470/1", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/2"] and ts[0]["exp"] == "t15men")
rc, out, _ = tq3("add", "--nice", "5", "--recipe", "command", "key=toy/a", "cmd=python3 -c 1", "env=rstats", "out=toy/a", 'done=["DONE"]')
check("add --recipe：实验表以外的任务", rc == 0 and "toy/a" in out and "环境 rstats" in out)
rc, out, _ = tq3("add", "--nice", "5", "--recipe", "nnunet", "dataset=Dataset101_GLIrq1", "fold=0", "trainr=x")
check("add --recipe：写错参数名报错、不加", rc == 2 and "trainr" in out and len(taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))) == 3)
rc, out, _ = tq3("next", "--worker", "700", "--envs", "nnunet,swinunetr")
lines = out.splitlines()
check("next：打印 key 和 run_task.sh 的参数", rc == 0 and lines[0] == "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1"
      and lines[1] == "nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=1")
rc, out, _ = tq3("next", "--worker", "701", "--envs", "nnunet,swinunetr")
rc2, out2, _ = tq3("next", "--worker", "702", "--envs", "nnunet")
check("next：本机没有的环境（rstats）不领", rc == 0 and rc2 == 10)
rc, out, _ = tq3("next", "--worker", "703", "--envs", "rstats")
check("next：有这个环境的机器领得到；JSON 值原样带上", rc == 0 and out.splitlines()[1] == "command key=toy/a 'cmd=python3 -c 1' env=rstats out=toy/a 'done=[\"DONE\"]'")
tq3("release", "--worker", "700", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1")
rc, out, _ = tq3("set", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1", "env=umamba")
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))}
check("set：改单个任务的参数", rc == 0 and ts["Dataset111_MENrq1/nnUNetTrainer_save10_n470/1"]["params"]["env"] == "umamba")
rc, out, _ = tq3("next", "--worker", "705", "--envs", "umamba")
check("next：改了环境以后按新环境领", rc == 0 and out.startswith("Dataset111_MENrq1/nnUNetTrainer_save10_n470/1"))
tq3("release", "--worker", "705", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1")
rc, out, _ = tq3("set", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1", "env=")
rc, out, _ = tq3("next", "--worker", "706", "--envs", "nnunet")
check("set k=（空值）删掉参数、回到默认环境", rc == 0 and out.startswith("Dataset111_MENrq1/nnUNetTrainer_save10_n470/1\n"))
rc, out, _ = tq3("set", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1", "fold=3")
check("set：会改变任务名的不让改", rc == 2)
rc, out, _ = tq3("show", "--json", "toy/a")
sj = taskq.S.json_in(out)
check("show --json：完整说明，写在 @@JSON 开头的一行里", rc == 0 and out.startswith("@@JSON ") and sj["out"] == "toy/a" and sj["env"] == "rstats")
rc, out, _ = tq3("show", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/2")
check("show：说明、状态、run_task.sh 的写法", rc == 0 and "claimed" in out and "run_task.sh nnunet" in out and "train.sh" in out)
d = os.path.join(DATA, "toy", "a")
os.makedirs(d)
open(os.path.join(d, "DONE"), "w").close()
s3 = taskq.S.json_in(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=ENV3, capture_output=True, text=True).stdout)
st3 = {t["key"]: t for t in s3["tasks"]}
check("--json-status：带说明；command 任务按 done 判断训完", st3["toy/a"]["state"] == "done" and st3["toy/a"]["spec"]["recipe"] == "command"
      and st3["Dataset111_MENrq1/nnUNetTrainer_save10_n470/2"]["spec"]["out"].startswith("nnUNet/results/Dataset111_MENrq1/"))
with open(os.path.join(TQ3, "tasks.jsonl"), "a") as f:
    f.write(json.dumps(dict(order=99, nice=1, key="bad/x", exp=None, recipe="nnunet", params=dict(fold="0"))) + "\n")
rc, out, _ = tq3("status")
check("解析不了的任务显示 bad、不影响别的", rc == 0 and "bad" in out)

# 10/22：machines 限定在哪几台机器上领（推理只在 DICC）
tq3("add", "--nice", "-100", "--recipe", "command", "key=toy/m", "cmd=true", "env=nnunet", "out=toy/m", 'done=["DONE"]', 'machines=["dicc"]')
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "710", "--envs", "nnunet"],
                   env=dict(ENV3, TASKQ_MACHINE="lab"), capture_output=True, text=True)
check("next：machines 里没有本机的不领", r.returncode == 10)
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "711", "--envs", "nnunet"],
                   env=dict(ENV3, TASKQ_MACHINE="dicc"), capture_output=True, text=True)
check("next：machines 里有本机的照档位先领", r.returncode == 0 and r.stdout.startswith("toy/m"))
# 10/24：device=cpu 的只给 CPU worker（--device cpu）；不带 --device 的（GPU worker、在跑的旧 worker）不领
tq3("add", "--nice", "-200", "--recipe", "command", "key=toy/c", "cmd=true", "env=nnunet", "out=toy/c", 'done=["DONE"]', "device=cpu")
rc, out, _ = tq3("next", "--worker", "712", "--envs", "nnunet")
check("next：不带 --device（GPU worker）不领 device=cpu 的，即使排在最前", rc != 0 or not out.startswith("toy/c"))
if rc == 0:
    tq3("release", "--worker", "712", out.splitlines()[0])
rc, out, _ = tq3("next", "--worker", "713", "--envs", "nnunet", "--device", "cpu")
check("next --device cpu：只领 device=cpu 的", rc == 0 and out.startswith("toy/c\n") and "device=cpu" in out)
rc, out, _ = tq3("next", "--worker", "714", "--envs", "nnunet", "--device", "cpu")
check("next --device cpu：没有别的纯 CPU 任务 → 10", rc == 10)

# 10/23：主数据目录读不了时不领
# add 不给 --nice（10/24）：--exp 用实验表里的档位，别的报错（单独一份队列，不影响上面的）
TQ3B = os.path.join(T, "taskq3b")
os.makedirs(TQ3B)
open(os.path.join(TQ3B, "tasks.jsonl"), "w").close()


def tq3b(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ3B), capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


rc, out, _ = tq3b("add", "--exp", "t15met", "--folds", "0")
ts = taskq.read_tasks(os.path.join(TQ3B, "tasks.jsonl"))
check("add --exp 不给 --nice：用实验表里的档位", rc == 0 and [t["key"] for t in ts] == ["Dataset116_METrq1/nnUNetTrainer_save10_n470/0"]
      and ts[0]["nice"] == E.experiment("t15met")["nice"])
rc, out, err = tq3b("add", "--recipe", "command", "key=toy/n", "cmd=true", "env=nnunet", "out=toy/n", 'done=["DONE"]')
check("add --recipe 不给 --nice：报错、不加", rc != 0 and "--nice" in err and len(taskq.read_tasks(os.path.join(TQ3B, "tasks.jsonl"))) == 1)

TQ4, D4 = os.path.join(T, "taskq4"), os.path.join(T, "data4")
os.makedirs(D4)
ks4 = ["Dataset121_X/nnUNetTrainer_save10/0", "Dataset121_X/nnUNetTrainer_save10/1", "Dataset121_X/nnUNetTrainer_save10/2"]
write_jsonl(os.path.join(TQ4, "tasks.jsonl"), [(i, 0, k) for i, k in enumerate(ks4)])
os.makedirs(os.path.join(TQ4, "failures"))
for k in ks4[:2]:
    with open(os.path.join(TQ4, "failures", taskq.safe(k)), "w") as f:
        f.write(f"{time.strftime('%F %T')}\t作业 1\tx\n" * 2)


def tq4(*args, root=D4):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ4, BRAIN_TUMOR=root),
                       capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


rc, out, err = tq4("next", "--worker", "800", "--envs", ALL, root=os.path.join(T, "nosuch"))
check("next：数据目录读不了时不领（退出码 12，报原因）", rc == 12 and "读不了" in err)
rc, out, err = tq4("take", "--worker", "lab", ks4[2], root=os.path.join(T, "nosuch"))
check("take：数据目录读不了时不领", rc == 10 and "读不了" in out and not os.path.exists(os.path.join(TQ4, "claims", taskq.safe(ks4[2]))))
rc, out, err = tq4("next", "--worker", "801", "--envs", ALL)
check("最近 4 次失败：暂停", rc == 12)

# ---------------- 10/25：领取只建了目录、info 没写上（领的进程在中间被杀） ----------------
TQ5, D5 = os.path.join(T, "taskq5"), os.path.join(T, "data5")
k5 = "Dataset121_X/nnUNetTrainer_save10/3"
write_jsonl(os.path.join(TQ5, "tasks.jsonl"), [(1, 0, k5)])
half = os.path.join(TQ5, "claims", taskq.safe(k5))
os.makedirs(half)
os.makedirs(D5, exist_ok=True)


def tq5(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ5, BRAIN_TUMOR=D5),
                       capture_output=True, text=True)
    return r.returncode, r.stdout.strip()


rc, out = tq5("next", "--worker", "900", "--envs", ALL)
check("半建的领取：刚建的（5 分钟内）当作正在领，不抢", rc == 10)
rc, out = tq5("take", "--worker", "lab", k5)
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
rc, out = tq5("take", "--worker", "lab", "--ttl", "604800", k5)
ci5 = json.load(open(os.path.join(half, "info")))
check("半建的领取：take 也能领走过期的", rc == 0 and ci5["worker"] == "lab" and ci5["ttl"] == 604800)
check("info 写完不留临时文件", os.listdir(half) == ["info"])

# ---------------- 10/25：after（先要做完的任务）→ waiting / blocked ----------------
TQ6, D6 = os.path.join(T, "taskq6"), os.path.join(T, "data6")
kt = "Dataset121_X/nnUNetTrainer_save10/4"
os.makedirs(os.path.join(TQ6), exist_ok=True)
fold(*kt.split("/"))


def cmdtask(order, key, after, device="gpu"):
    return dict(order=order, nice=-100, key=key, exp=None, recipe="command",
                params=dict(key=key, cmd="true", env="nnunet", out=f"out/{key}", done=[".done"], after=after, device=device))


rows = [dict(order=1, nice=0, key=kt, exp=None, recipe="nnunet", params=dict(dataset="Dataset121_X", trainer="nnUNetTrainer_save10", fold="4")),
        cmdtask(2, "infer/x/fold_4", [kt]), cmdtask(3, "eval/x/fold_4", ["infer/x/fold_4"], "cpu"), cmdtask(4, "infer/y", ["nosuch/key"])]
with open(os.path.join(TQ6, "tasks.jsonl"), "w") as f:
    f.writelines(json.dumps(r) + "\n" for r in rows)


def tq6(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ6, BRAIN_TUMOR=DATA),
                       capture_output=True, text=True)
    return r.returncode, r.stdout.strip()


def st6():
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=dict(ENV, TASKQ_DIR=TQ6, BRAIN_TUMOR=DATA),
                       capture_output=True, text=True)
    return {t["key"]: (t["state"], t["note"]) for t in taskq.S.json_in(r.stdout)["tasks"]}


s6 = st6()
check("after：训练没训完 → 推理 waiting、评测 waiting（等推理）", s6["infer/x/fold_4"][0] == "waiting" and kt in s6["infer/x/fold_4"][1]
      and s6["eval/x/fold_4"] == ("waiting", "等 infer/x/fold_4"))
check("after：等的任务不在队列里 → blocked", s6["infer/y"][0] == "blocked" and "不在队列里" in s6["infer/y"][1])
rc, out = tq6("next", "--worker", "950", "--envs", ALL)
check("after：next 先领训练（档位 0），不领在等的推理（档位 -100）", rc == 0 and out.startswith(kt + "\n"))
rc, out = tq6("next", "--worker", "951", "--envs", ALL)
check("after：训练被领走后没有别的能领", rc == 10)
rc, out = tq6("take", "--worker", "lab", "infer/x/fold_4")
check("after：take 也不领在等的", rc == 10)
d4 = fold(*kt.split("/"))
open(os.path.join(d4, "checkpoint_final.pth"), "w").close()
os.makedirs(os.path.join(d4, "validation"), exist_ok=True)
open(os.path.join(d4, "validation", "summary.json"), "w").write("{}")
s6 = st6()
check("after：训练训完 → 推理 free，评测还在等推理", s6["infer/x/fold_4"][0] == "free" and s6["eval/x/fold_4"][0] == "waiting")
rc, out = tq6("next", "--worker", "952", "--envs", ALL)
check("after：训完后推理能领", rc == 0 and out.startswith("infer/x/fold_4\n"))
tq6("release", "--worker", "952", "infer/x/fold_4", "--failed", "退出码 1")
tq6("release", "--worker", "952", "infer/x/fold_4", "--failed", "退出码 1")
s6 = st6()
check("after：推理失败两次 → 评测 blocked", s6["eval/x/fold_4"][0] == "blocked" and "失败 2 次" in s6["eval/x/fold_4"][1])

print("\n全部通过" if ok else "\n有失败")
sys.exit(0 if ok else 1)
