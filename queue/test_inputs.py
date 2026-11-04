#!/usr/bin/env python3
"""inputs.py 和 run_task.sh 查输入、本地私有视图的测试（2026-11-01）：临时目录里的假数据目录、假 blob、假 squeue。
  python3 scripts/taskq/queue/test_inputs.py"""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
PY = [sys.executable, os.path.join(HERE, "inputs.py")]
T = tempfile.mkdtemp()
ok_all = True


def ok(name, cond):
    global ok_all
    print(("通过：" if cond else "失败：") + name)
    ok_all &= bool(cond)


def run(*args, env=None):
    r = subprocess.run(PY + list(args), capture_output=True, text=True, env=dict(os.environ, **(env or {})))
    return r.returncode, r.stdout.strip(), r.stderr.strip()


def blob(store, content):
    h = hashlib.sha256(content).hexdigest()
    p = os.path.join(store, h[:2], h)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb") as f:
        f.write(content)
    return p


# 数据目录：一个视图（链接指向 blob）+ 一个普通文件
DATA, STORE = os.path.join(T, "data"), os.path.join(T, "blobs")
V = os.path.join(DATA, "in", "ds1")
os.makedirs(os.path.join(V, "sub"))
files = {f"sub/c{i}.b2nd": os.urandom(3000 + i) for i in range(6)}
for rel, c in files.items():
    os.symlink(blob(STORE, c), os.path.join(V, rel))
open(os.path.join(V, "dataset.json"), "w").write('{"a": 1}')
NOENV = dict(STAGE_KEEP_GB="0", PATH=os.environ["PATH"])

rc, out, err = run("prepare", DATA, "k1", "", V)
ok("查输入：齐了 → 退出码 0，打印数据目录", rc == 0 and out == DATA)
rc, out, err = run("prepare", DATA, "k1", "", V, os.path.join(DATA, "in", "nothere"))
ok("查输入：输入不存在 → 退出码 4，说缺什么", rc == 4 and "nothere" in err and "缺输入" in err)
rc, out, err = run("prepare", DATA, "k1", "", os.path.join(DATA, "in", "*", "sub", "c*.b2nd"))
ok("查输入：带通配符的匹配到文件 → 退出码 0", rc == 0 and out == DATA)
rc, out, err = run("prepare", DATA, "k1", "", os.path.join(DATA, "in", "*", "nope*"))
ok("查输入：通配符一个都没匹配到 → 退出码 4", rc == 4 and "一个都没有" in err)
open(os.path.join(V, ".incomplete"), "w").close()
rc, out, err = run("prepare", DATA, "k1", "", V)
ok("查输入：视图建到一半（.incomplete）→ 退出码 4", rc == 4 and "建到一半" in err)
os.remove(os.path.join(V, ".incomplete"))
# 下面某一层建到一半、某一层读不了也算缺（11/3）
open(os.path.join(V, "sub", ".incomplete"), "w").close()
rc, out, err = run("prepare", DATA, "k1", "", os.path.join(DATA, "in"))
ok("查输入：子文件夹里有 .incomplete → 退出码 4", rc == 4 and "建到一半" in err and "ds1/sub" in err)
os.remove(os.path.join(V, "sub", ".incomplete"))
rc, out, err = run("prepare", DATA, "k1", "", os.path.join(DATA, "in"))
ok("查输入：没有标记时上一级照常 → 退出码 0", rc == 0 and out == DATA)
if os.getuid() != 0:
    os.chmod(os.path.join(V, "sub"), 0)
    rc, out, err = run("prepare", DATA, "k1", "", os.path.join(DATA, "in"))
    os.chmod(os.path.join(V, "sub"), 0o755)
    ok("查输入：子文件夹读不了 → 退出码 4，不当成空", rc == 4 and "读不了" in err)
gone =os.path.realpath(os.path.join(V, "sub/c0.b2nd"))
os.rename(gone, gone + ".x")
rc, out, err = run("prepare", DATA, "k1", "", V)
ok("查输入：链接指向的 blob 被删了 → 退出码 4，举出是哪个", rc == 4 and "c0.b2nd" in err)
os.rename(gone + ".x", gone)

# 本地私有视图（假 squeue：只有作业 7 还在）
BIN = os.path.join(T, "bin")
os.makedirs(BIN)
with open(os.path.join(BIN, "squeue"), "w") as f:
    f.write("#!/bin/sh\necho 7\n")
os.chmod(os.path.join(BIN, "squeue"), 0o755)
TMP = os.path.join(T, "tmp")
L7 = os.path.join(TMP, "phd_stage_7")
old = os.path.join(TMP, "phd_stage_5")
os.makedirs(os.path.join(old, "blobs"))
env = dict(NOENV, PATH=BIN + ":" + os.environ["PATH"])
rc, out, err = run("prepare", DATA, "k1", L7, V, env=env)
view = os.path.join(L7, "views", "k1")
lv = os.path.join(view, "in", "ds1")
ok("本地视图：打印视图的根，相对路径和数据目录一样，先都指向原处", rc == 0 and out == view
   and all(os.path.realpath(os.path.join(lv, r)) == os.path.realpath(os.path.join(V, r)) for r in files)
   and open(os.path.join(lv, "dataset.json")).read() == '{"a": 1}')
ok("本地视图：删了本节点上已结束作业的目录，在跑的留着", not os.path.exists(old) and os.path.isdir(L7))
todo = json.load(open(view + ".todo.json"))
ok("本地视图：要拷的清单里是全部 7 个文件", len(todo) == 7)

# 拷到一半：死掉的进程留下的 .partial、一个 blob 内容和名字对不上
os.makedirs(os.path.join(L7, "blobs"), exist_ok=True)
stale = os.path.join(L7, "blobs", "abc.partial.999999")
open(stale, "w").write("half")
bad_rel = "sub/c1.b2nd"
bad_real = os.path.realpath(os.path.join(V, bad_rel))
good = open(bad_real, "rb").read()
open(bad_real, "wb").write(b"x" * len(good))  # 大小对、内容不对
rc, out, err = run("copy", L7, "k1", env=env)
ok("拷贝：被停掉的拷贝留下的 .partial 删了", not os.path.exists(stale))
ok("拷贝：内容核对不上的不换，仍指向原处，说明里写出来", rc == 0 and os.path.realpath(os.path.join(lv, bad_rel)) == bad_real
   and "没拷成" in out and not any(p.endswith(".partial." + str(os.getpid())) for p in os.listdir(os.path.join(L7, "blobs"))))
ok("拷贝：其余的都换成本地，内容一样", all(os.path.realpath(os.path.join(lv, r)).startswith(os.path.join(L7, "blobs") + "/")
                                          and open(os.path.join(lv, r), "rb").read() == c for r, c in files.items() if r != bad_rel)
   and os.path.realpath(os.path.join(lv, "dataset.json")).startswith(L7 + "/") and open(os.path.join(lv, "dataset.json")).read() == '{"a": 1}')
ok("拷贝：视图里没留下临时链接", not any(f.endswith(".taskq_tmp") for d, _, fs in os.walk(view) for f in fs))
open(bad_real, "wb").write(good)

# 同一作业的第二个任务：已拷好的直接指本地，只拷剩下的；本节点别的作业有的硬链接过来
rc, out, err = run("prepare", DATA, "k2", L7, V, env=env)
todo2 = json.load(open(os.path.join(L7, "views", "k2.todo.json")))
ok("第二个任务：已经在本地的直接指本地，清单里只剩没拷成的那个", rc == 0 and [x[0] for x in todo2] == ["in/ds1/" + bad_rel])
L8 = os.path.join(TMP, "phd_stage_8")
with open(os.path.join(BIN, "squeue"), "w") as f:
    f.write("#!/bin/sh\necho 7\necho 8\n")
rc, out, err = run("prepare", DATA, "k3", L8, V, env=env)
rc, out, err = run("copy", L8, "k3", env=env)
ino = lambda p: os.stat(p).st_ino  # noqa: E731
ok("别的作业：本节点已有的硬链接过来，不重拷", rc == 0 and "硬链接 6 个" in out and "新拷 1 个" in out
   and ino(os.path.join(L8, "views", "k3", "in", "ds1", "sub/c2.b2nd")) == ino(os.path.join(L7, "views", "k1", "in", "ds1", "sub/c2.b2nd")))

# 剩余空间：本节点别的作业有的（能硬链接）不算要拷的量；本节点的拷贝排队（锁），两个同时查不会各算各的（11/4）
sys.path.insert(0, HERE)
import inputs as I  # noqa: E402
import fcntl  # noqa: E402
L10 = os.path.join(TMP, "phd_stage_10")
run("prepare", DATA, "k10", L10, V, env=env)
_du, _keep = I.shutil.disk_usage, I.KEEP_GB
I.shutil.disk_usage = lambda p: shutil._ntuple_diskusage(0, 0, 1024 ** 3 + 10)
I.KEEP_GB = 1
try:
    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        I.copy(L10, "k10")
finally:
    I.shutil.disk_usage, I.KEEP_GB = _du, _keep
ok("剩余空间：本节点别的作业已有的（硬链接）不算进要拷的量", "硬链接 7 个" in buf.getvalue() and "不拷" not in buf.getvalue())
L11 = os.path.join(TMP, "phd_stage_11")
run("prepare", DATA, "k11", L11, V, env=env)
lk = open(os.path.join(TMP, f".phd_stage_{os.getuid()}.lock"), "a")
fcntl.flock(lk, fcntl.LOCK_EX)
p11 = subprocess.Popen(PY + ["copy", L11, "k11"], stdout=subprocess.PIPE, text=True, env=dict(os.environ, **env))
try:
    p11.wait(timeout=2)
except subprocess.TimeoutExpired:
    pass
waited = p11.poll() is None
fcntl.flock(lk, fcntl.LOCK_UN)
lk.close()
out11 = p11.communicate(timeout=60)[0]
ok("本节点的拷贝排队：别的拷贝拿着锁时等着，放开后照常拷", waited and p11.returncode == 0 and "硬链接 7 个" in out11)

# 作业数组：squeue 的 %i 是 <数组号>_<序号>，本地目录用的是每个子任务自己的作业号（%A）；别的作业不能删它
with open(os.path.join(BIN, "squeue"), "w") as f:
    f.write('#!/bin/sh\ncase "$*" in *%A*) echo "7 7"; echo "8 3_0";; *) echo 7; echo 3_0;; esac\n')
rc, out, err = run("prepare", DATA, "k1", L7, V, env=env)
ok("作业数组的子任务（作业号 8、显示成 3_0）还在：不删它的本地目录", rc == 0 and os.path.isdir(L8))
with open(os.path.join(BIN, "squeue"), "w") as f:
    f.write("#!/bin/sh\necho 7\necho 8\n")

# 同一路径、大小不变、内容改了（改动时间变了）：不复用本地旧副本（11/1）
dj = os.path.join(V, "dataset.json")
st0 = os.stat(dj)
open(dj, "w").write('{"a": 2}')
os.utime(dj, ns=(st0.st_atime_ns, st0.st_mtime_ns + 10 ** 9))
rc, out, err = run("prepare", DATA, "k6", L7, V, env=env)
todo6 = json.load(open(os.path.join(L7, "views", "k6.todo.json")))
ok("同一路径内容改了（大小不变）：不用本地旧副本，读到新内容", rc == 0 and "in/ds1/dataset.json" in [x[0] for x in todo6]
   and open(os.path.join(L7, "views", "k6", "in", "ds1", "dataset.json")).read() == '{"a": 2}')
open(dj, "w").write('{"a": 1}')

# 盘不够：不拷，照旧读原处
rc, out, err = run("prepare", DATA, "k4", os.path.join(TMP, "phd_stage_9"), V, env=dict(env, STAGE_KEEP_GB="1e9"))
rc, out, err = run("copy", os.path.join(TMP, "phd_stage_9"), "k4", env=dict(env, STAGE_KEEP_GB="1e9"))
lv9 = os.path.join(TMP, "phd_stage_9", "views", "k4", "in", "ds1")
ok("盘不够：不拷，视图仍指向原处", rc == 0 and "不拷" in out and all(os.path.realpath(os.path.join(lv9, r)).startswith(STORE) for r in files))

# squeue 出错时不删别的作业的目录
with open(os.path.join(BIN, "squeue"), "w") as f:
    f.write("#!/bin/sh\nexit 1\n")
rc, out, err = run("prepare", DATA, "k5", L7, V, env=env)
ok("squeue 出错：不删别的作业的目录", rc == 0 and os.path.isdir(L8) and os.path.isdir(os.path.join(TMP, "phd_stage_9")))

# run_task.sh：缺输入 → 退出码 4，不跑命令；齐了 → 命令从 TASK_INPUTS_ROOT 读；Slurm 里、STAGE_TMP 开、local_inputs → 读本地视图
RT = os.path.join(HERE, "run_task.sh")
OUT = os.path.join(DATA, "jobs", "t")


def rtask(inputs, local, **e):
    args = ["command", "key=t/one", "env=stats", "out=jobs/t", "done=DONE", f"inputs={json.dumps(inputs)}",
            f"res={json.dumps(dict(local_inputs=local))}",
            'cmd=cat "$TASK_INPUTS_ROOT/in/ds1/dataset.json" > "$TASKQ_DATA/jobs/t/seen"; echo "$TASK_INPUTS_ROOT" > "$TASKQ_DATA/jobs/t/root"; touch "$TASKQ_DATA/jobs/t/DONE"']
    envx = dict(os.environ, TASKQ_DATA=DATA, RUN_TASK_TRIES="1", TMPDIR=TMP, PATH=BIN + ":" + os.environ["PATH"], STAGE_KEEP_GB="0", **e)
    envx.pop("SLURM_JOB_ID", None)
    envx.update(e)
    shutil.rmtree(OUT, ignore_errors=True)
    return subprocess.run(["bash", RT] + args, capture_output=True, text=True, env=envx)


envs_ok = subprocess.run([sys.executable, os.path.join(os.path.dirname(HERE), "sites", "mtab.py"), "activate", "stats"], capture_output=True).returncode == 0
if envs_ok:
    with open(os.path.join(BIN, "squeue"), "w") as f:
        f.write("#!/bin/sh\necho 7\n")
    r = rtask(["in/ds1", "in/nothere"], 0)
    ok("run_task.sh：缺输入 → 退出码 4，命令没跑", r.returncode == 4 and not os.path.exists(os.path.join(OUT, "DONE")))
    r = rtask(["in/ds1"], 0)
    ok("run_task.sh：输入齐了、不拷本地 → TASK_INPUTS_ROOT 是数据目录", r.returncode == 0
       and open(os.path.join(OUT, "root")).read().strip() == DATA and open(os.path.join(OUT, "seen")).read() == '{"a": 1}')
    r = rtask(["in/ds1"], 1, SLURM_JOB_ID="7", TASKQ_STAGE_TMP="1")
    ok("run_task.sh：Slurm 里、STAGE_TMP 开、local_inputs → 读节点本地盘上的私有视图", r.returncode == 0
       and open(os.path.join(OUT, "root")).read().strip() == os.path.join(L7, "views", "t__one")
       and open(os.path.join(OUT, "seen")).read() == '{"a": 1}')
    r = rtask(["in/ds1"], 1, SLURM_JOB_ID="7", TASKQ_STAGE_TMP="0")
    ok("run_task.sh：STAGE_TMP 关 → 读数据目录", r.returncode == 0 and open(os.path.join(OUT, "root")).read().strip() == DATA)
else:
    print("跳过 run_task.sh 的几条：测试用的机器表认不出本机（没有 stats 环境）")

shutil.rmtree(T, ignore_errors=True)
print("\n全部通过" if ok_all else "\n有失败")
sys.exit(0 if ok_all else 1)
