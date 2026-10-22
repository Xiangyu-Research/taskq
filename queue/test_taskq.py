#!/usr/bin/env python3
"""taskq.py 的测试（2026-10-17，10/18 改成任务说明）：临时目录里的假队列、假训练结果，不连任何机器。  python3 scripts/queue/test_taskq.py"""
import glob
import json
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
T = tempfile.mkdtemp()
TQ, DATA = os.path.join(T, "taskq"), os.path.join(T, "data")
RES = os.path.join(DATA, "nnUNet", "results")
ENV = dict(os.environ, TASKQ_DIR=TQ, BRAIN_TUMOR=DATA)
ENV.pop("nnUNet_results", None)
sys.path.insert(0, HERE)
os.environ.update(TASKQ_DIR=TQ)
import taskq  # noqa: E402

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


def write_tsv(path, rows):
    """10/18 前的 tasks.tsv（7 列）。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("# order\tnice\tkey\tdsid\ttrainer\tfold\tkind\t（taskq.py）\n")
        for r in rows:
            f.write("\t".join(str(r[k]) for k in ("order", "nice", "key", "dsid", "trainer", "fold", "kind")) + "\n")


tasks = [dict(order=10, nice=0, key="Dataset121_X/nnUNetTrainer_save10/0", dsid=121, trainer="nnUNetTrainer_save10", fold="0", kind="nnunet"),
         dict(order=11, nice=0, key="Dataset121_X/nnUNetTrainer_save10/1", dsid=121, trainer="nnUNetTrainer_save10", fold="1", kind="nnunet"),
         dict(order=5, nice=50, key="Dataset103_Y/nnUNetTrainer_save10/0", dsid=103, trainer="nnUNetTrainer_save10", fold="0", kind="nnunet"),
         dict(order=1, nice=2000, key="Dataset101_Z/nnUNetTrainer_SwinUNETR/0", dsid=101, trainer="nnUNetTrainer_SwinUNETR", fold="0", kind="swin"),
         dict(order=12, nice=0, key="Dataset121_X/nnUNetTrainer_save10/2", dsid=121, trainer="nnUNetTrainer_save10", fold="2", kind="nnunet")]
write_tsv(os.path.join(TQ, "tasks.tsv"), tasks)
for t in tasks:
    fold(t["key"].split("/")[0], t["trainer"], t["fold"])

# fold 0 已训完
d0 = fold("Dataset121_X", "nnUNetTrainer_save10", "0")
open(os.path.join(d0, "checkpoint_final.pth"), "w").close()
os.makedirs(os.path.join(d0, "validation"))
open(os.path.join(d0, "validation", "summary.json"), "w").write("{}")
# fold 1 的训练日志刚更新过（别处在训，比如附加训练）
open(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "1"), "training_log_x.txt"), "w").write("x")

rc, out = tq("claim", "--worker", "100", "--kind", "nnunet")
check("按档位先领、跳过训完的和日志刚更新的：领到 fold 2", rc == 0 and out.startswith("Dataset121_X/nnUNetTrainer_save10/2 121"))
rc, out = tq("claim", "--worker", "101", "--kind", "nnunet")
check("已经被领的不再领：下一个是档位 50 的", rc == 0 and out.startswith("Dataset103_Y/"))
check("第一次读时 tasks.tsv 自动转成 tasks.jsonl（原文件留 .bak）",
      os.path.exists(os.path.join(TQ, "tasks.jsonl")) and os.path.exists(os.path.join(TQ, "tasks.tsv.bak"))
      and not os.path.exists(os.path.join(TQ, "tasks.tsv")))
ts = taskq.read_tasks(os.path.join(TQ, "tasks.jsonl"))
sw = next(t for t in ts if "SwinUNETR" in t["key"])
check("转过来顺序、档位不变；SwinUNETR 的写明 env=swinunetr",
      [t["order"] for t in ts] == [10, 11, 12, 5, 1] and sw["recipe"] == "nnunet" and sw["params"]["env"] == "swinunetr"
      and sw["params"]["dataset"] == "Dataset101_Z")
rc, out = tq("claim", "--worker", "103", "--kind", "swin")
check("--kind 不再起作用：照顺序领（SwinUNETR，环境对得上，旧 worker 能跑）", rc == 0 and out.startswith("Dataset101_Z/nnUNetTrainer_SwinUNETR/0 101"))
rc, out = tq("claim", "--worker", "102", "--kind", "nnunet")
check("没有能领的：退出码 10", rc == 10 and out == "")

# 领取过期（心跳 30 分钟前、日志也没更新）→ 可以重新领
p = os.path.join(TQ, "claims", taskq.safe("Dataset103_Y/nnUNetTrainer_save10/0"), "info")
ci = json.load(open(p))
ci["beat"] = time.time() - 1800
json.dump(ci, open(p, "w"))
rc, out = tq("claim", "--worker", "104", "--kind", "nnunet")
check("心跳过期、日志也没更新的重新领", rc == 0 and out.startswith("Dataset103_Y/"))
check("重新领后记的是新的 worker", json.load(open(p))["worker"] == "104")

# 心跳过期但训练日志刚更新 → 仍算在训
key2 = "Dataset121_X/nnUNetTrainer_save10/2"
p2 = os.path.join(TQ, "claims", taskq.safe(key2), "info")
ci = json.load(open(p2))
ci["beat"] = time.time() - 1800
json.dump(ci, open(p2, "w"))
open(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), "training_log_y.txt"), "w").write("y")
rc, out = tq("claim", "--worker", "105", "--kind", "nnunet")
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
rc, _ = tq("claim", "--worker", "110", "--kind", "any")
check("3 小时内失败 3 次：整个队列暂停领任务（退出码 12）", rc == 12)
for fp in glob.glob(os.path.join(TQ, "failures", "*")):  # 把失败记录改成 4 小时前
    old = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 4 * 3600))
    lines = open(fp).readlines()
    open(fp, "w").write("".join(old + ln[19:] for ln in lines))
for f in os.listdir(fold("Dataset121_X", "nnUNetTrainer_save10", "2")):
    os.utime(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), f), (time.time() - 3600,) * 2)
rc, out = tq("claim", "--worker", "106", "--kind", "nnunet")
check("失败 1 次的还能领、失败 2 次的不领", rc == 0 and out.startswith(key2))
tq("release", "--worker", "106", key2)
rc, out = tq("claim", "--worker", "106", "--kind", "nnunet", "--exclude", key2)
check("本作业失败过的（--exclude）不再领", rc == 10)

# 交还之后日志没再更新：马上能领；交还之后日志又更新了：算队列外有人在训
open(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), "training_log_y.txt"), "w").write("y")
rc, out = tq("claim", "--worker", "120", "--kind", "nnunet")
check("刚领的（日志刚更新）照常领到", rc == 0 and out.startswith(key2))
tq("release", "--worker", "120", key2)
rc, out = tq("claim", "--worker", "121", "--kind", "nnunet")
check("刚交还、日志 15 分钟内更新过，也马上能领", rc == 0 and out.startswith(key2))
tq("release", "--worker", "121", key2)
time.sleep(6)
open(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), "training_log_y.txt"), "w").write("y2")
rc, out = tq("claim", "--worker", "122", "--kind", "nnunet")
check("交还之后日志又更新了（队列外在训）：不领", rc == 10)

# 排在前面的折刚停下（日志新、没人领），后面还有能领的：开始后的等待时间内先等它
open(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "1"), "training_log_x.txt"), "w").write("x")  # fold 1 刚停下
os.remove(os.path.join(TQ, "failures", taskq.safe("Dataset103_Y/nnUNetTrainer_save10/0")))  # 让后面有一个能领的
rc, out = tq("claim", "--worker", "130", "--kind", "nnunet", "--free-slots", "1", "--patience-until", str(time.time() + 600))
check("空 1 个位置、前面有 1 个刚停下的：等（退出码 11）", rc == 11)
rc, out = tq("claim", "--worker", "130", "--kind", "nnunet", "--free-slots", "3", "--patience-until", str(time.time() + 600))
check("空的位置比刚停下的多：照常领后面的", rc == 0 and out.startswith("Dataset103_Y/"))
tq("release", "--worker", "130", "Dataset103_Y/nnUNetTrainer_save10/0")
rc, out = tq("claim", "--worker", "131", "--kind", "nnunet", "--free-slots", "1", "--patience-until", str(time.time() - 1))
check("等待时间过了：领后面的", rc == 0 and out.startswith("Dataset103_Y/"))
tq("release", "--worker", "131", "Dataset103_Y/nnUNetTrainer_save10/0")
os.remove(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "1"), "training_log_x.txt"))

for f in os.listdir(fold("Dataset121_X", "nnUNetTrainer_save10", "2")):
    os.utime(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), f), (time.time() - 3600,) * 2)
rc, out = tq("claim", "--worker", "107", "--kind", "nnunet")
check("照顺序领 fold 1", rc == 0 and out.startswith("Dataset121_X/nnUNetTrainer_save10/1 "))
rc, out = tq("claim", "--worker", "108", "--kind", "nnunet")
check("再下一个是 fold 2（日志 1 小时前更新过，没人领）", rc == 0 and out.startswith(key2))
# 并发领：同时起 6 个，只有 1 个领到
procs = [subprocess.Popen([sys.executable, os.path.join(HERE, "taskq.py"), "claim", "--worker", f"2{i}", "--kind", "nnunet"],
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
# 一份 10/17 init 生成的任务表（表头带说明），原样能读
open(os.path.join(TQ2, "tasks.tsv"), "w").write(
    "# order\tnice\tkey\tdsid\ttrainer\tfold\tkind\t（taskq.py init，2026-10-17 13:44:12）\n"
    "486191\t0\tDataset116_METrq1/nnUNetTrainer_save10/4\t116\tnnUNetTrainer_save10\t4\tnnunet\n"
    "486300\t2000\tDataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0\t101\tnnUNetTrainer_SwinUNETR\t0\tswin\n")
os.environ["TASKQ_DIR"] = TQ2
check("10/17 生成的任务表（表头带说明）也能转", [t["order"] for t in taskq.read_tasks(os.path.join(TQ2, "tasks.jsonl"))] == []
      and tq2("status")[0] == 0 and [t["order"] for t in taskq.read_tasks(os.path.join(TQ2, "tasks.jsonl"))] == [486191, 486300])
rc, out = tq2("add", "--nice", "50", "Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0", "Dataset116_METrq1/nnUNetTrainer_save10/4")
ts = taskq.read_tasks(os.path.join(TQ2, "tasks.jsonl"))
um = ts[1]
check("add key：实验表里有的照表（U-Mamba：环境 umamba、实验 umamba），按档位排进去，已有的跳过",
      rc == 0 and [t["key"].split("/")[1] for t in ts] == ["nnUNetTrainer_save10", "nnUNetTrainer_UMambaEnc", "nnUNetTrainer_SwinUNETR"]
      and um["exp"] == "umamba" and um["params"]["env"] == "umamba" and um["order"] == 486301 and "已有，跳过" in out)
rc, out = tq2("add", "--nice", "0", "Dataset101_GLIrq1/badkey")
check("add：key 格式不对报错", rc != 0)
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
rc, out = tq2("claim", "--worker", "500", "--kind", "any", "--exclude", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0", "Dataset116_METrq1/nnUNetTrainer_save10/4")
check("lab 领了的，DICC 作业不再领", rc == 10)
ci["beat"] = time.time() - 3 * 3600
json.dump(ci, open(os.path.join(TQ2, "claims", taskq.safe(UM), "info"), "w"))
rc, out = tq2("claim", "--worker", "500", "--kind", "any", "--exclude", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0", "Dataset116_METrq1/nnUNetTrainer_save10/4")
check("ttl 24 小时：心跳 3 小时前仍算在训（默认 20 分钟会过期）", rc == 10)
rc, out = tq2("take", "--worker", "lab", "--ttl", "86400", UM)
ci2 = json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))
check("take 自己领的：续上、领取时间不变", rc == 0 and ci2["since"] == ci["since"] and time.time() - ci2["beat"] < 10)
rc, out = tq2("take", "--worker", "other", UM)
check("take 别人领的：领不到（退出码 10）", rc == 10)
ci2["beat"] = time.time() - 2 * 86400
json.dump(ci2, open(os.path.join(TQ2, "claims", taskq.safe(UM), "info"), "w"))
rc, out = tq2("claim", "--worker", "501", "--kind", "any", "--exclude", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0", "Dataset116_METrq1/nnUNetTrainer_save10/4")
check("超过 ttl 的领取过期，DICC 作业能接着领", rc == 0 and out.startswith(UM))
rc, out = tq2("take", "--worker", "lab", "Dataset116_METrq1/nnUNetTrainer_save10/9")
check("take 队列里没有的：退出码 10", rc == 10)
d = fold("Dataset116_METrq1", "nnUNetTrainer_save10", "4")
open(os.path.join(d, "checkpoint_final.pth"), "w").close()
os.makedirs(os.path.join(d, "validation"), exist_ok=True)
open(os.path.join(d, "validation", "summary.json"), "w").write("{}")
rc, out = tq2("take", "--worker", "lab", "Dataset116_METrq1/nnUNetTrainer_save10/4")
check("take 已训完的：领不到", rc == 10 and "done" in out)
rc, out = tq2("claim", "--worker", "502", "--kind", "any")
check("kind any：什么种类都领（领到 SwinUNETR）", rc == 0 and out.startswith("Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0"))
s2 = json.loads(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=ENV2, capture_output=True, text=True).stdout)
row = next(t for t in s2["tasks"] if t["key"] == UM)
rc, _ = tq2("release", "--worker", "lab", SWK, "--skip", "训练任务 5 Failed (1)")
s3 = json.loads(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=ENV2, capture_output=True, text=True).stdout)
row3 = next(t for t in s3["tasks"] if t["key"] == SWK)
check("release --skip：记在 skips/、不算失败；--json-status 带 skipped_by 和 24 小时内的次数",
      rc == 0 and row3["skipped_by"] == ["lab"] and row3["failures"] == 0
      and s3["recent_skips"] == {"lab": 1} and not os.path.exists(os.path.join(TQ2, "failures", taskq.safe(SWK))))
check("--json-status 带 worker、host、beat", row["worker"] == "501" and "host" in row and row["beat"])

# DICC 上原有 worker 的调用方式（dicc_worker.sh 实际传的参数）不变
rc, out = tq2("claim", "--worker", "503", "--kind", "nnunet", "--free-slots", "2", "--patience-until", str(time.time() + 1200))
check("旧 worker：claim 带 --free-slots / --patience-until，没有能领的退出码 10", rc == 10)
rc, _ = tq2("beat", "--worker", "502", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0")
check("旧 worker：beat 退出码 0", rc == 0)
rc, _ = tq2("release", "--worker", "502", "Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0")
check("旧 worker：release 退出码 0、记录删掉", rc == 0 and not os.path.exists(os.path.join(TQ2, "claims", taskq.safe("Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0"))))
rc, _ = tq2("release", "--worker", "501", UM, "--failed", "退出码 1")
check("旧 worker：release --failed 记失败", rc == 0 and len(open(os.path.join(TQ2, "failures", taskq.safe(UM))).readlines()) == 1)
rc, out = tq2("claim", "--worker", "504", "--kind", "any", "--exclude", UM)
check("lab 跳过的折（skips/）DICC 作业照样领", rc == 0 and out.startswith(SWK))
info_p = os.path.join(TQ2, "claims", taskq.safe(SWK), "info")
ci3 = json.load(open(info_p))
ci3.pop("ttl", None)
ci3["beat"] = time.time() - 600
json.dump(ci3, open(info_p, "w"))
rc, _ = tq2("claim", "--worker", "505", "--kind", "any", "--exclude", UM)
check("旧 worker：没有 ttl 的领取记录，心跳 10 分钟前仍算在训", rc == 10)
ci3["beat"] = time.time() - 1300
json.dump(ci3, open(info_p, "w"))
rc, out = tq2("claim", "--worker", "505", "--kind", "any", "--exclude", UM)
check("旧 worker：没有 ttl 的领取记录，心跳超过 20 分钟就过期、别人能领", rc == 0 and out.startswith(SWK))
NOQ = os.path.join(T, "no_tasks")
os.makedirs(NOQ)
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "claim", "--worker", "506", "--kind", "nnunet"],
                   env=dict(ENV, TASKQ_DIR=NOQ), capture_output=True, text=True)
check("没有任务表：claim 退出码 2（出错，不是 10「没有任务」，作业不会因此提前结束）", r.returncode == 2)

# box 上读远端状态：在 DICC 仓库里跑 taskq.py（假 ssh：把命令放到本机跑）
fake = os.path.join(T, "fakessh")
open(fake, "w").write("#!/bin/bash\n# 假 ssh：去掉选项和主机名，命令里的仓库路径换成本仓库\n"
                      "while [[ $1 == -o ]]; do shift 2; done; shift\n"
                      f"cmd=${{1//\\~\\/Projects\\/PhD/{os.path.dirname(os.path.dirname(HERE))}}}\n"
                      f"export TASKQ_DIR={TQ2}\nbash -c \"$cmd\"\n")
os.chmod(fake, 0o755)
os.environ["TASKQ_SSH"] = fake
st = taskq.remote_status("dicc")
check("remote_status：在远端仓库里跑", st is not None and len(st["tasks"]) == 3)
rc_out = taskq.remote("dicc", f"take --worker lab --ttl 86400 {UM}")
ci4 = json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))
check("remote：take 经 ssh 跑、领到，记下 worker=lab、ttl", rc_out is not None and rc_out[0] == 0 and ci4["worker"] == "lab" and ci4["ttl"] == 86400)
EMPTY = os.path.join(T, "box_tq")  # box 上：只有 LAB 开关，没有任务表
os.makedirs(EMPTY)
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "add", "--nice", "100", "Dataset113_PEDsrq1/nnUNetTrainer_save10/0"],
                   env=dict(os.environ, TASKQ_DIR=EMPTY), capture_output=True, text=True)
check("box 上没有队列时 add 转到 DICC 那份（不在本机另建）", r.returncode == 0 and "加入" in r.stdout
      and not os.path.exists(os.path.join(EMPTY, "tasks.jsonl")) and "Dataset113_PEDsrq1" in open(os.path.join(TQ2, "tasks.jsonl")).read())
os.environ["TASKQ_SSH"] = "false"
check("ssh 不通（退出码非 0）：remote_status 返回 None", taskq.remote_status("dicc") is None)
del os.environ["TASKQ_SSH"]


# ---------------- 10/18：任务说明（next、set、show、add --exp / --recipe、旧 claim 只发旧 worker 能跑的） ----------------
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
rc, out, _ = tq3("claim", "--worker", "704", "--kind", "any")
check("旧 claim：环境和训练器名对不上的（旧 worker 会用错环境）不发", rc == 10)
rc, out, _ = tq3("next", "--worker", "705", "--envs", "umamba")
check("next：改了环境以后按新环境领", rc == 0 and out.startswith("Dataset111_MENrq1/nnUNetTrainer_save10_n470/1"))
tq3("release", "--worker", "705", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1")
rc, out, _ = tq3("set", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1", "env=")
rc, out, _ = tq3("claim", "--worker", "706", "--kind", "any")
check("set k=（空值）删掉参数、回到默认；旧 claim 又能发", rc == 0 and out == "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1 111 nnUNetTrainer_save10_n470 1")
rc, out, _ = tq3("set", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/1", "fold=3")
check("set：会改变任务名的不让改", rc == 2)
rc, out, _ = tq3("show", "--json", "toy/a")
check("show --json：完整说明", rc == 0 and json.loads(out)["out"] == "toy/a" and json.loads(out)["env"] == "rstats")
rc, out, _ = tq3("show", "Dataset111_MENrq1/nnUNetTrainer_save10_n470/2")
check("show：说明、状态、run_task.sh 的写法", rc == 0 and "claimed" in out and "run_task.sh nnunet" in out and "train.sh" in out)
d = os.path.join(DATA, "toy", "a")
os.makedirs(d)
open(os.path.join(d, "DONE"), "w").close()
s3 = json.loads(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "--json-status"], env=ENV3, capture_output=True, text=True).stdout)
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

print("\n全部通过" if ok else "\n有失败")
sys.exit(0 if ok else 1)
