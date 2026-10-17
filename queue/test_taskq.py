#!/usr/bin/env python3
"""taskq.py 的测试（2026-10-17）：临时目录里的假队列、假训练结果，不连任何机器。  python3 scripts/queue/test_taskq.py"""
import glob
import json
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
T = tempfile.mkdtemp()
TQ, RES = os.path.join(T, "taskq"), os.path.join(T, "results")
ENV = dict(os.environ, TASKQ_DIR=TQ, nnUNet_results=RES, RQ1_CLAIMS=os.path.join(T, "claims_elsewhere.txt"))
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


tasks = [dict(order=10, nice=0, key="Dataset121_X/nnUNetTrainer_save10/0", dsid=121, trainer="nnUNetTrainer_save10", fold="0", kind="nnunet"),
         dict(order=11, nice=0, key="Dataset121_X/nnUNetTrainer_save10/1", dsid=121, trainer="nnUNetTrainer_save10", fold="1", kind="nnunet"),
         dict(order=5, nice=50, key="Dataset103_Y/nnUNetTrainer_save10/0", dsid=103, trainer="nnUNetTrainer_save10", fold="0", kind="nnunet"),
         dict(order=1, nice=2000, key="Dataset101_Z/nnUNetTrainer_SwinUNETR/0", dsid=101, trainer="nnUNetTrainer_SwinUNETR", fold="0", kind="swin"),
         dict(order=12, nice=0, key="Dataset121_X/nnUNetTrainer_save10/2", dsid=121, trainer="nnUNetTrainer_save10", fold="2", kind="nnunet")]
taskq.write_tasks(tasks, os.path.join(TQ, "tasks.tsv"))
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
rc, out = tq("claim", "--worker", "102", "--kind", "nnunet")
check("没有能领的：退出码 10", rc == 10 and out == "")
rc, out = tq("claim", "--worker", "103", "--kind", "swin")
check("只领自己种类的", rc == 0 and out.startswith("Dataset101_Z/nnUNetTrainer_SwinUNETR/0"))

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
rc, _ = tq("claim", "--worker", "110", "--kind", "swin")
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

# 别的机器在训
open(ENV["RQ1_CLAIMS"], "w").write(f"# x\n{key2}\tlab\n")
for f in os.listdir(fold("Dataset121_X", "nnUNetTrainer_save10", "2")):
    os.utime(os.path.join(fold("Dataset121_X", "nnUNetTrainer_save10", "2"), f), (time.time() - 3600,) * 2)
rc, out = tq("claim", "--worker", "107", "--kind", "nnunet")
check("别的机器在训的不领：跳过 fold 2，领下一个", rc == 0 and out.startswith("Dataset121_X/nnUNetTrainer_save10/1 "))
rc, out = tq("claim", "--worker", "108", "--kind", "nnunet")
check("再下一个是档位 50 的", rc == 0 and out.startswith("Dataset103_Y/"))

# 并发领：同时起 6 个，只有 1 个领到
os.remove(ENV["RQ1_CLAIMS"])
procs = [subprocess.Popen([sys.executable, os.path.join(HERE, "taskq.py"), "claim", "--worker", f"2{i}", "--kind", "nnunet"],
                          env=ENV, stdout=subprocess.PIPE, text=True) for i in range(6)]
outs = [p.communicate()[0].strip() for p in procs]
check("同时领，只有一个领到", sum(1 for o in outs if o) == 1)

print("\n全部通过" if ok else "\n有失败")
sys.exit(0 if ok else 1)
