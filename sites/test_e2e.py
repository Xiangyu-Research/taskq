#!/usr/bin/env python3
"""执行机全流程测试（2026-10-31）：本机模拟一台 pueue 类执行机（lab1 的条目，根目录、conda、pueue 都换成临时的），走一遍
建环境 → 任务单加任务 → 领、传数据 → 开 worker → 中途停、接着做 → 拉回 → 清理。
完全隔离：执行机根目录、box 的数据目录、总队列都在临时目录里；pueue 是自己起的 pueued（自己的配置和 socket），结束时只停它；
ssh 换成本机的 bash -c，不连任何机器；不改机器表（条目在进程里替换）。
跑法：python3 sites/test_e2e.py（box 上；E2E_TMP=<目录> 指定临时目录放在哪，E2E_KEEP=1 留下临时目录）"""
import glob
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(CORE, "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根

PUEUE = shutil.which("pueue") or os.path.expanduser("~/.local/bin/pueue")
PUEUED = shutil.which("pueued") or os.path.expanduser("~/.local/bin/pueued")

T = fixture.tmpdir(prefix="e2e_", dir=os.environ.get("E2E_TMP") or None)
BD = os.path.join(T, "box")        # box 的数据目录
# 执行机的根目录：实际在 T/r，经系统临时目录里的一个软链接用（根目录经软链接到达的情况也测到；结束时删掉）
os.makedirs(os.path.join(T, "r"))
RLINK = tempfile.mktemp(prefix="e2e_r_", dir="/tmp")
os.symlink(os.path.join(T, "r"), RLINK)
RROOT = RLINK
PQ = os.path.join(T, "p")          # 自己的 pueued
BIN = os.path.join(T, "bin")
FAKE = os.path.join(BIN, "ssh")
for d in (BD, PQ, BIN):
    os.makedirs(d)
with open(FAKE, "w") as f:  # 假的 ssh：跳过 -o 和机器名，在本机跑；box 这边的变量不带过去
    f.write('#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\nshift\n'
            f'cd / && exec env -u TASKQ_CONFIG -u TASKQ_PLUGINS -u TASKQ_DATA -u TASKQ_DIR -u RELAY_DIR -u PHD_TASKS -u PHD_LAST bash -c "$*"\n')
with open(os.path.join(BIN, "nvidia-smi"), "w") as f:  # 不碰真的卡
    f.write("#!/bin/bash\nexit 0\n")
for p in (FAKE, os.path.join(BIN, "nvidia-smi")):
    os.chmod(p, 0o755)
s_ = socket.socket()
s_.bind(("127.0.0.1", 0))
PORT = s_.getsockname()[1]
s_.close()
os.environ.update(
    RELAY_DIR=os.path.join(T, "relay"), TASKQ_DIR=os.path.join(T, "boxq"), TASKQ_DATA=BD,
    RELAY_SSH=FAKE, TUNNEL_SSH=FAKE, TUNNEL_URL=f"http://127.0.0.1:{PORT}", TUNNEL_BIND=f"127.0.0.1:{PORT}", TUNNEL_POLL="0.1",
    TUNNEL_LOCK=os.path.join(T, "lock"), PHD_TASKS=os.path.join(T, "tasks.json"), PHD_LAST=os.path.join(T, "last"),
    RELAY_LOG=os.path.join(T, "relay.log"), PATH=BIN + ":" + os.environ["PATH"],
    TMPDIR=T,  # 被停掉的 worker 留下的临时文件跟着 T 删
    # worker（dicc_worker.sh）、run_task.sh 的测试用开关：等得短一些
    TASKQ_POLL="1", TASKQ_IDLE_POLL="1", TASKQ_PATIENCE="0", TASKQ_IDLE_EXIT="6", TASKQ_STOP_POLL="1", TASKQ_STOP_WAIT="5",
    TASKQ_HEAD_CMD="echo same", RUN_TASK_POLL="1")
os.makedirs(os.environ["TASKQ_DIR"])
open(os.path.join(os.environ["TASKQ_DIR"], "ENABLED"), "w").close()

cfg.use_config()
for d in (cfg.rel("queue"), cfg.rel("sync"), cfg.rel("monitor"), cfg.rel("sites")):
    sys.path.insert(0, os.path.join(REPO, d))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import common as C  # noqa: E402
import spec as S  # noqa: E402
import taskq  # noqa: E402
import relay  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_store as ST  # noqa: E402
import relay_transfer as RT  # noqa: E402
import relay_bundle as RB  # noqa: E402
import envs as E  # noqa: E402

NAME = "lab1"
Q = taskq.queue_host()
CONDA = os.path.join(RROOT, "conda")       # 假的 conda：activate 只把 bin 放进 PATH（bin/python3 是本机一个有 numpy 的 Python）
PWRAP = os.path.join(RROOT, "bin", "pueue")  # 指到自己的 pueued
PCFG = os.path.join(PQ, "c.yml")
WID = "wtest"
ENVS = {"sys": ("conda", "base"), "toy": "spec"}
ENTRY = dict(conda=(CONDA, "base"), envs=ENVS, pueue=PWRAP, groups={"gpu1": (1, None, {})}, worker=dict(group="gpu1", id=WID),
             slots=1, env_root=RROOT + "/envs")
SPECS = os.path.join(T, "specs")
KEYS = [f"guest/e2e/t{i}" for i in range(1, 6)]
SLOW = KEYS[0]
LOG, ORDER, FAILS = [], {}, []
DAEMON = None


def step(msg, cond, why=""):
    if not cond and callable(why):  # 贵的说明只在失败时算
        why = why()
    print(("通过：" if cond else "失败：") + msg + ("" if cond else f"（{why}）"), flush=True)
    if not cond:
        FAILS.append(msg)
        raise AssertionError(msg)


def wb(p, data):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb" if isinstance(data, bytes) else "w") as f:
        f.write(data)


def pq(*args):
    r = subprocess.run([PUEUE, "-c", PCFG, *args], capture_output=True, text=True, timeout=60)
    return r.returncode, r.stdout + r.stderr


def pueue_tasks():
    rc, out = pq("status", "--json")
    return json.loads(out)["tasks"] if rc == 0 else None


def bundle_key(name, commit):
    """relay.py 记 bundle_for 用的键（执行机:使用者的提交:核心的提交），记上测试自己打的包，relay 就不按 origin/main 重打。"""
    core = subprocess.run(["git", "-C", cfg.CORE, "rev-parse", RB.CORE_REF], capture_output=True, text=True).stdout.strip()
    return f"{name}:{commit}:{core}"


def run_local(cmd, host, timeout, input=None):
    return C.run(cmd, host, timeout, input=input, ssh=[FAKE])


TOY = r'''import argparse, glob, json, os, time
import numpy as np
p = argparse.ArgumentParser()
p.add_argument("--data"); p.add_argument("--out"); p.add_argument("--rounds", type=int, default=3)
p.add_argument("--pause", type=float, default=0)
a = p.parse_args()
os.makedirs(a.out, exist_ok=True)
files = [a.data] if os.path.isfile(a.data) else sorted(os.path.join(d, f) for d, _, fs in os.walk(a.data) for f in fs)
text = "".join(open(f).read() for f in files)
start, total = 0, 0
ck = sorted(glob.glob(f"{a.out}/ckpt_*.json"))
if ck:
    st = json.load(open(ck[-1])); start, total = st["round"], st["total"]
for r in range(start, a.rounds):
    total += int(np.frombuffer(text.encode(), dtype=np.uint8).sum())
    json.dump(dict(round=r + 1, total=total), open(f"{a.out}/ckpt.tmp", "w"))
    os.replace(f"{a.out}/ckpt.tmp", f"{a.out}/ckpt_{r + 1:02d}.json")
    for old in sorted(glob.glob(f"{a.out}/ckpt_*.json"))[:-1]:
        os.remove(old)
    json.dump({"done": r + 1, "total": a.rounds}, open(f"{a.out}/PROGRESS.json", "w"))
    open(f"{a.out}/train.log", "a").write(f"round {r + 1}\n")
    time.sleep(0.3)
    if a.pause and r + 1 == 2 and start == 0:
        time.sleep(a.pause)
json.dump(dict(total=total, text=text, start=start), open(f"{a.out}/result.json", "w"))
open(f"{a.out}/DONE", "w").close()
'''


def numpy_python():
    cands = [RB.check_python(), sys.executable] + sorted(glob.glob(os.path.expanduser("~/miniforge3/envs/*/bin/python"))) \
        + [os.path.expanduser("~/miniforge3/bin/python3")]
    return next((p for p in cands if p and os.path.exists(p)
                 and subprocess.run([p, "-c", "import numpy, venv"], capture_output=True).returncode == 0), None)


def text_of(i):
    return f"数据 {i}\n" * i


def setup():
    # box 上的输入：共用的代码、每个任务自己的数据（t5 的是一个文件）
    wb(os.path.join(BD, "guest/e2e/code/toy.py"), TOY)
    for i in range(1, 5):
        wb(os.path.join(BD, f"guest/e2e/d{i}/a.txt"), text_of(i))
    wb(os.path.join(BD, "guest/e2e/one.txt"), text_of(5))
    sheets = []
    for i, k in enumerate(KEYS, 1):
        data = "{data}/guest/e2e/one.txt" if i == 5 else f"{{data}}/guest/e2e/d{i}"
        sheets.append(dict(key=k, owner="alice", env="toy", inputs=["guest/e2e/code", "guest/e2e/one.txt" if i == 5 else f"guest/e2e/d{i}"],
                           cmd=f"python3 {{data}}/guest/e2e/code/toy.py --data {data} --out {{data}}/guest/e2e/r/t{i}"
                               + (" --pause 600" if k == SLOW else ""),
                           out=f"guest/e2e/r/t{i}", done=["DONE"], progress="progress_json", resume=["ckpt_*.json"],
                           log="train.log", res={"gpus": 1}, nice=50))
    wb(os.path.join(T, "job.json"), json.dumps(sheets, ensure_ascii=False))
    # 环境描述：建在一个有 numpy 的基础环境上的 venv（pip 不用下载）
    wb(os.path.join(SPECS, "toy.yml"), "# 测试\nbase: sys\npip:\n  - numpy\n")
    wb(os.path.join(SPECS, "toy.check.py"), "import numpy\nprint('numpy', numpy.__version__)\n")
    M.SPECS = SPECS
    # 假的 conda、指到自己 pueued 的 pueue
    py = numpy_python()
    if not py:
        raise SystemExit("本机没有带 numpy 的 Python（RB.check_python、miniforge 的环境），跑不了")
    os.makedirs(os.path.join(CONDA, "bin"))
    for n in ("python3", "python"):
        os.symlink(py, os.path.join(CONDA, "bin", n))
    wb(os.path.join(CONDA, "etc/profile.d/conda.sh"),
       f'conda() {{ if [[ ${{1:-}} == activate ]]; then export PATH="{CONDA}/bin:$PATH"; fi; }}\n')
    wb(PWRAP, f'#!/bin/bash\nexec {PUEUE} -c {PCFG} "$@"\n')
    os.chmod(PWRAP, 0o755)
    wb(PCFG, f"shared:\n  pueue_directory: {PQ}\n  runtime_directory: {PQ}\n  unix_socket_path: {PQ}/s.sock\n"
             f"  use_unix_socket: true\ndaemon:\n  callback: null\n")
    global DAEMON
    DAEMON = subprocess.Popen([PUEUED, "-c", PCFG], stdout=open(os.path.join(PQ, "daemon.log"), "w"), stderr=subprocess.STDOUT,
                              start_new_session=True)
    for _ in range(50):
        if pueue_tasks() is not None:
            break
        time.sleep(0.2)
    rc, out = pq("group", "add", "gpu1")
    assert rc == 0, out
    # 进程里换掉 lab1 的条目（不改机器表）
    conf = dict(M.SITES[NAME], root=RROOT, ssh="fake-lab", adopt=None, budget=(0, 0), transfer_job=None, **ENTRY)
    M.SITES[NAME] = conf
    M.RELAY[NAME] = conf
    P.AUTO[NAME] = True
    P.READY[NAME] = len(KEYS)
    M.MACHINES[M.hub()] = dict(M.MACHINES[M.hub()], data=BD)  # box 的数据目录（relay.box_data、refill.data_gb 都读它）
    relay.refill.add = fake_add
    RT.place = place_rec
    # 代码包：工作区的代码，换成临时的根目录；run_one 认这个包（不按 origin/main 重打）
    bid, path = RB.build(NAME, None, os.path.join(T, "bundles"), root=RROOT, entry=ENTRY)
    relay.deploy(NAME, bid, path)
    commit = subprocess.run(["git", "-C", REPO, "rev-parse", "origin/main"], capture_output=True, text=True).stdout.strip()
    relay.BUNDLE_REF = commit  # 测试中途别处推送 main 也不重打包（11/4）
    ST.update_state(lambda st: st.setdefault("bundle_for", {}).update({bundle_key(NAME, commit): bid}))
    rs = relay.remote_status(NAME)
    step(f"准备：代码包 {bid} 推过去，那边的队列开着、是空的，自己的 pueued 起来了",
         rs is not None and rs["bundle"] == bid and rs["taskq"]["enabled"] and rs["taskq"]["tasks"] == [] and rs["jobs"] == [], rs)
    used = int(subprocess.run(["du", "-sb", os.path.join(T, "r")], capture_output=True, text=True).stdout.split()[0])
    M.RELAY[NAME]["cap"] = 1
    try:
        rs2 = relay.remote_status(NAME)
    finally:
        M.RELAY[NAME].pop("cap")
    step("cap（11/2）：剩余按 cap 减去根目录已占的算，不看整个盘", rs2 is not None and 0 < rs2["free"] <= 1e9 - used * 0.5, (rs2 or {}).get("free"))


# ---------------- box 的 wait 组：在进程里直接做 ----------------
_place = RT.place


def place_rec(src, D, written, real):
    order, rep = [], os.replace

    def r(a, b):
        order.append(os.path.relpath(b, D))
        rep(a, b)
    os.replace = r
    try:
        _place(src, D, written, real)
    finally:
        os.replace = rep
    ORDER[real["key"]] = order


def fake_add(host, lab, step_, key, args, run=None, **rec):
    if step_ == "relay_stage":
        RT.stage(rec.get("to") or NAME, key, nice=int(re.search(r"--nice (-?\d+)", args).group(1)), say=LOG.append)
    elif step_ == "relay_fetch":
        RT.fetch(rec.get("to") or NAME, key, say=LOG.append)
    else:
        raise AssertionError(f"没想到的 box 任务 {step_} {key}")
    LOG.append(f"box 任务 {step_} {key}")
    return len(LOG)


def snapshot():
    return dict(taskq={Q: taskq.local_status()}, pueue={M.hub(): dict(tasks=[])}, records=[], ok={M.hub(): True},
                src={n: dict(tasks={}) for n in M.names("train")})


def check(name=NAME):
    msgs, stuck = relay.run_one(snapshot(), name, False)
    LOG.extend(msgs)
    bad = [m for m in msgs if "出错" in m]
    if bad:
        raise AssertionError(f"检查出错：{bad}")
    return msgs, stuck


def box_tasks():
    return {t["key"]: t for t in taskq.local_status()["tasks"]}


def keys():
    return ST.keys_of(ST.load_state(), NAME)


def central(args):
    rc, out, err = taskq.central(args)
    assert rc == 0, out + err
    return out


def view_files():
    """那边每个视图里的文件 → 软链接指向。"""
    D = os.path.join(RROOT, "data")
    out = {}
    for k in keys().values():
        for v in k["views"]:
            for dp, _, fs in os.walk(os.path.join(D, v)):
                for f in fs:
                    p = os.path.join(dp, f)
                    out[os.path.relpath(p, D)] = os.readlink(p) if os.path.islink(p) else None
    return out


def wait(cond, timeout, what, every=1.0, act=None):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if act:
            act()
        if cond():
            return
        time.sleep(every)
    raise AssertionError(f"{timeout} 秒内没等到：{what}")


# ---------------- 各步 ----------------
def step_env_gate():
    out = central(f"add --file {T}/job.json")
    bt = box_tasks()
    step("任务单加了 5 个任务，默认 relay=path", sorted(bt) == KEYS and all(t["spec"]["relay"] == "path" for t in bt.values()), out)
    st = E.states(NAME, run=run_local)
    step("建环境前：toy 没建", st == {"sys": "不是按描述建的", "toy": "没建"}, st)
    check()
    bt, rs = box_tasks(), relay.remote_status(NAME)
    step("没有 .ok：一个都不领、不传，那边队列空，不开 worker",
         all(t["state"] == "free" and not t["worker"] for t in bt.values()) and rs["taskq"]["tasks"] == [] and pueue_tasks() == {},
         [(t["key"], t["state"], t["worker"]) for t in bt.values()])
    rc, out = E.build(NAME, "toy", run=run_local)
    okf = os.path.join(RROOT, "state/envs/toy.ok")
    step("taskq site env 的建法（envs.build）：建好、检查过、写 .ok（= 描述的哈希）",
         rc == 0 and open(okf).read().strip() == M.spec_hash("toy"), out)
    a = M.activate(NAME, "toy")
    r = subprocess.run(["bash", "-c", a + "\npython -c 'import numpy,sys; print(sys.prefix)'"], capture_output=True, text=True)
    step("激活后用的是建好的 venv，能导入 numpy", r.returncode == 0 and "/envs/toy-" in r.stdout, r.stdout + r.stderr)
    st = E.states(NAME, run=run_local)
    step("状态：toy ok", st["toy"] == "ok", st)


def step_stage():
    check()
    bt, rs = box_tasks(), relay.remote_status(NAME)
    step("有了 .ok：5 个都领给 lab1、传过去、加进那边的队列",
         all(t["state"] == "claimed" and t["worker"] == NAME for t in bt.values())
         and sorted(t["state"] for t in rs["taskq"]["tasks"]) == ["free"] * 5,
         lambda: ([(t["key"], t["state"]) for t in rs["taskq"]["tasks"]], relay.run_one(snapshot(), NAME, True)[0]))
    blobs = [os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(RROOT, "blobs")) for f in fs]
    vf = view_files()
    D = os.path.join(RROOT, "data")
    step(f"数据按内容放进 blobs（{len(blobs)} 个，共用的代码只一份），视图是指向 blobs 的软链接",
         len(blobs) == 6 and vf and all(t and t.startswith(os.path.join(RROOT, "blobs")) and os.path.isfile(os.path.join(D, p))
                                        for p, t in vf.items()), (len(blobs), vf))
    k1 = keys()[SLOW]
    rs = relay.remote_status(NAME)
    step("和 box 一样的路径：视图是输入的文件夹、结果在 out 原样、那边的 key 就是总队列的 key",
         k1["views"] == ["guest/e2e/code", "guest/e2e/d1"] and k1["params"]["out"] == "guest/e2e/r/t1" and k1["outs"] == ["guest/e2e/r/t1"]
         and sorted(t["key"] for t in rs["taskq"]["tasks"]) == KEYS, k1)
    step("那边还没开 worker（这次检查开始时还没有备好的任务）", pueue_tasks() == {}, pueue_tasks())
    # 两个任务的视图同时在：一个重新传数据只清自己的视图
    k2 = keys()[KEYS[1]]
    own2 = [v for v in k2["views"] if v not in k1["views"]][0]
    own1 = [v for v in k1["views"] if v not in k2["views"]][0]
    blob = next(os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(RROOT, "blobs")) for f in fs)
    os.symlink(blob, os.path.join(D, own2, "stray2.txt"))
    os.symlink(blob, os.path.join(D, own1, "stray1.txt"))
    os.symlink(blob, os.path.join(D, "guest/e2e/code", "stray0.txt"))
    before = view_files()
    RT.stage(NAME, SLOW, nice=50, add=False, say=LOG.append)
    after = view_files()
    step("几个任务的视图共存：t1 重新传数据只删自己独占的视图里多出来的链接，不碰 t2 的、大家共用的",
         not os.path.lexists(os.path.join(D, own1, "stray1.txt")) and os.path.lexists(os.path.join(D, own2, "stray2.txt"))
         and os.path.lexists(os.path.join(D, "guest/e2e/code", "stray0.txt"))
         and all(after.get(p) == t for p, t in before.items() if not p.endswith("stray1.txt")), after)
    os.remove(os.path.join(D, own2, "stray2.txt"))
    os.remove(os.path.join(D, "guest/e2e/code", "stray0.txt"))
    check()
    jobs = pueue_tasks()
    step("下一次检查：有备好的任务了，开了一个 worker", len(jobs) == 1 and next(iter(jobs.values()))["label"] == f"worker:{WID}", jobs)


def remote_out(key):
    return os.path.join(RROOT, "data", keys()[key]["params"]["out"])


def progress_of(key):
    try:
        return json.load(open(os.path.join(remote_out(key), "PROGRESS.json")))["done"]
    except (OSError, ValueError, KeyError):
        return 0


def step_stop():
    wait(lambda: progress_of(SLOW) == 2, 120, "t1 做完第 2 轮", every=0.2)
    central(f"stop --why 测试 {SLOW}")

    def released():
        t = box_tasks()[SLOW]
        return t["state"] == "stopped" and not t["worker"]
    wait(released, 90, "总队列里停了 t1 后，那边停下、box 交还", every=2, act=check)
    rs = relay.remote_status(NAME)
    a = next(t for t in rs["taskq"]["tasks"] if t["key"] == SLOW)
    f = (rs.get("tasks") or {}).get(SLOW)
    t = box_tasks()[SLOW]
    skips = os.path.join(os.environ["TASKQ_DIR"], "skips")
    step("停下后：那边是 stopped、没人领，按存档算有进度（resumable），box 交还时不带 skip",
         a["state"] == "stopped" and not a.get("worker") and relay.progress(f) and t["skipped_by"] == []
         and not (os.path.isdir(skips) and os.listdir(skips)), (a, f, t["skipped_by"]))
    step("那边留着第 2 轮的存档、没有 DONE",
         os.path.exists(os.path.join(remote_out(SLOW), "ckpt_02.json")) and not os.path.exists(os.path.join(remote_out(SLOW), "DONE")),
         os.listdir(remote_out(SLOW)))
    # 别的任务还在用视图时清理：只删不要的
    before = view_files()
    reg = (ST.load_state().get("views") or {}).get(NAME) or {}
    keep = {v for k in keys().values() for v in ST.paths_of(k)}
    g = ST.gc(NAME, keep, RM.rpy, soft_gb=1e9, views=reg)
    step("还有任务要用时清理（relay_store.gc）：要用的视图和 blob 一个不删",
         g["removed_views"] == [] and view_files() == before and all(os.path.isfile(os.path.join(RROOT, "data", p)) for p in before), g)
    central(f"resume {SLOW}")


def step_finish():
    seen_drop = []

    def all_done():
        bt = box_tasks()
        ks = keys()
        if ks and len(ks) < len(KEYS):  # 有的已经删了、有的还在：还在的视图都完好
            vf = view_files()
            assert all(os.path.isfile(os.path.join(RROOT, "data", p)) for p in vf), "删一个任务时动了别的任务的视图"
            seen_drop.append(len(ks))
        return all(t["state"] == "done" for t in bt.values()) and not ks
    wait(all_done, 300, "5 个都做完、拉回、那边删掉", every=2, act=check)
    step("一个任务做完删掉时，别的任务的视图都还在", bool(seen_drop), "没碰上有删有留的时候")
    for i, k in enumerate(KEYS, 1):
        out = os.path.join(BD, f"guest/e2e/r/t{i}")
        res = json.load(open(os.path.join(out, "result.json")))
        want = sum(text_of(i).encode())
        step(f"{k} 拉回：真名路径、内容对（3 轮的和、读到的输入）{'、从第 2 轮的存档接着做' if k == SLOW else ''}",
             res["text"] == text_of(i) and res["total"] == 3 * want and res["start"] == (2 if k == SLOW else 0)
             and json.load(open(os.path.join(out, "PROGRESS.json")))["done"] == 3 and os.path.exists(os.path.join(out, "DONE")), res)
        step(f"{k} 拉回时 DONE 最后放", ORDER.get(k) and ORDER[k][-1] == f"guest/e2e/r/t{i}/DONE", ORDER.get(k))
        logs = [f for f in os.listdir(os.path.join(BD, "logs/tasks")) if f.startswith(S.safe(k) + "__")]
        step(f"{k} 的分段日志拉回了", bool(logs), os.listdir(os.path.join(BD, "logs/tasks")))
    r = subprocess.run(["grep", "-rlF", BD, os.path.join(RROOT, "data")], capture_output=True, text=True)
    step("执行机的数据目录里没有 box 的绝对路径（命令里的 {data} 在那边展开）", r.stdout.strip() == "", r.stdout[:500])


def step_gc():
    RM.conf(NAME)["budget"] = (1e6, 0)  # 软线高得够不着：能删的都删
    relay.VIEW_KEEP_S = 0
    check()
    D = os.path.join(RROOT, "data")
    left = [os.path.relpath(os.path.join(dp, f), D) for dp, _, fs in os.walk(D) for f in fs]
    blobs = [f for _, _, fs in os.walk(os.path.join(RROOT, "blobs")) for f in fs]
    reg = (ST.load_state().get("views") or {}).get(NAME) or {}
    top = sorted(os.listdir(RROOT))
    step("全部做完后清理（relay_store.gc）：视图、blob 都删了，数据目录只剩 worker 的日志，登记的视图清空",
         not blobs and all(p.startswith("logs/workers/") for p in left) and not reg, (left[:10], blobs[:5], reg))
    step("根目录只剩还要用的：代码、队列、环境和它的状态", set(top) <= {"code", "queue", "blobs", "data", "tmp", "state", "envs", "conda", "bin"}
         and os.path.isfile(os.path.join(RROOT, "state/envs/toy.ok")), top)


# ---------------- 按时段的执行机（lab0 的晚班）：时段外不领，时段里领、开 worker，结束时交给同一台上的 lab1 接着做 ----------------
NAME0 = "lab0"
RLINK0 = tempfile.mktemp(prefix="e2e_r0_", dir="/tmp")
K6 = "guest/e2e/t6"


def keys0():
    return ST.keys_of(ST.load_state(), NAME0)


def out0(key):
    return os.path.join(RLINK0, "data", keys0()[key]["params"]["out"])


def step_shift():
    import datetime as dt
    import shift
    os.makedirs(os.path.join(T, "r0"))
    os.symlink(os.path.join(T, "r0"), RLINK0)
    shift.STATE, shift.LOG = os.path.join(T, "shift.json"), os.path.join(T, "shift.log")
    shift.others_on = lambda host, gpu: []
    shift.C = type("C", (), {"run": staticmethod(lambda cmd, host=None, timeout=60, **kw: run_local(cmd, host, timeout))})
    rc, out = pq("group", "add", "gpu0")
    assert rc == 0, out
    pq("pause", "-g", "gpu0")
    e0 = dict(ENTRY, groups={"gpu0": (1, None, {})}, worker=dict(group="gpu0", id="wtest0"), env_root=RLINK0 + "/envs")
    c0 = dict(M.SITES[NAME0], root=RLINK0, ssh="fake-lab", adopt=[NAME], budget=(0, 0), transfer_job=None, **e0)
    M.SITES[NAME0] = M.RELAY[NAME0] = c0
    RM.conf(NAME).update(adopt=[NAME0], budget=(0, 0))
    relay.VIEW_KEEP_S = 3600
    P.AUTO.update({NAME0: True, NAME: True})
    P.READY.update({NAME0: 1, NAME: 0})  # 夜里 lab1 不领（让 lab0 领到）；交接时直接改分，不用它领
    bid, path = RB.build(NAME0, None, os.path.join(T, "bundles0"), root=RLINK0, entry=e0)
    relay.deploy(NAME0, bid, path)
    commit = relay.BUNDLE_REF
    ST.update_state(lambda st: st.setdefault("bundle_for", {}).update({bundle_key(NAME0, commit): bid}))
    rc, out = E.build(NAME0, "toy", run=run_local)
    assert rc == 0, out
    sheet = dict(key=K6, owner="alice", env="toy", inputs=["guest/e2e/code", "guest/e2e/d1"],
                 cmd="python3 {data}/guest/e2e/code/toy.py --data {data}/guest/e2e/d1 --out {data}/guest/e2e/r/t6 --pause 600",
                 out="guest/e2e/r/t6", done=["DONE"], progress="progress_json", resume=["ckpt_*.json"], log="train.log",
                 res={"gpus": 1}, nice=50)
    wb(os.path.join(T, "job6.json"), json.dumps([sheet], ensure_ascii=False))
    central(f"add --file {T}/job6.json")
    now = dt.datetime.now(shift.TZ)
    hm = lambda d: (now + dt.timedelta(hours=d)).strftime("%H:%M")  # noqa: E731
    P.SHIFT = {NAME0: (hm(1), hm(2))}
    shift.run(now=now)
    check(NAME0)
    check(NAME0)
    t6 = box_tasks()[K6]
    gpu0 = [t for t in pueue_tasks().values() if t["group"] == "gpu0"]
    step("lab0 时段外：不领任务、不开 worker", t6["state"] == "free" and not t6["worker"] and not gpu0 and not shift.allowed(NAME0),
         (t6, gpu0))
    P.SHIFT = {NAME0: (hm(-1), hm(2))}
    shift.run(now=now)
    st = shift.load()
    step("lab0 时段开始、卡空着：恢复 worker 组，allowed() 放行", shift.allowed(NAME0) and st[NAME0].get("started"), st)
    check(NAME0)
    t6 = box_tasks()[K6]
    step("lab0 时段里：领了 t6、数据传过去", t6["worker"] == NAME0 and K6 in keys0(), (t6, LOG[-8:]))
    check(NAME0)
    jobs = [t for t in pueue_tasks().values() if t["group"] == "gpu0"]
    step("下一次检查：lab0 开了 worker", len(jobs) == 1 and jobs[0]["label"] == "worker:wtest0", jobs)
    wait(lambda: os.path.exists(os.path.join(out0(K6), "ckpt_02.json")), 120, "t6 在 lab0 上做完第 2 轮", every=0.2)
    check(NAME0)  # 读一次状态（status.json：在训）
    msgs = shift.run(now=now + dt.timedelta(hours=3))
    t6 = box_tasks()[K6]
    step("时段结束：停 lab0 的 worker，有进度的 t6 改分给 lab1", t6["worker"] == NAME and t6["state"] == "claimed", (t6, msgs))
    step("时段结束后 lab0 不再领任务", not shift.allowed(NAME0))
    wait(lambda: all(t["status"] != "Running" for t in pueue_tasks().values() if t["group"] == "gpu0"), 60, "lab0 的 worker 停下", every=0.2)
    check(NAME0)
    a0 = keys0().get(K6) or {}
    remote_q0 = relay.remote_status(NAME0)["taskq"]["tasks"]
    step("lab0：队列里删掉 t6（unq），结果目录留着给 lab1 收编",
         a0.get("unq") and not remote_q0 and os.path.exists(os.path.join(out0(K6), "ckpt_02.json")), (a0, remote_q0))

    def done6():
        return box_tasks()[K6]["state"] == "done"
    check()  # lab1 收编 lab0 的存档、加进队列
    o1 = os.path.join(RLINK, "data", keys()[K6]["params"]["out"])
    step("lab1 收编了 lab0 的存档，刚停的日志让那边的队列先当它有人在训（busy，15 分钟），不马上开 worker",
         os.path.exists(os.path.join(o1, "ckpt_02.json")) and relay.remote_status(NAME)["taskq"]["tasks"][0]["state"] == "busy")
    old = time.time() - 20 * 60  # 下一次检查（30 分钟后）时日志已经停了一阵
    for f in os.listdir(o1):
        os.utime(os.path.join(o1, f), (old, old))
    wait(done6, 300, "lab1 从 lab0 的存档接着做完、拉回", every=2, act=check)
    res = json.load(open(os.path.join(BD, "guest/e2e/r/t6/result.json")))
    step("t6：lab1 从第 2 轮的存档接着做（lab0 的存档带过来了），结果对",
         res["start"] == 2 and res["total"] == 3 * sum(text_of(1).encode()), res)
    check(NAME0)
    step("总队列训完以后，lab0 上 t6 的结果删了、记录清掉",
         K6 not in keys0() and not os.path.exists(os.path.join(RLINK0, "data", a0["params"]["out"])), keys0())


def cleanup():
    if DAEMON is not None:
        pq("kill", "--all")
        time.sleep(1)
        pq("shutdown")
        try:
            DAEMON.wait(10)
        except subprocess.TimeoutExpired:
            os.killpg(DAEMON.pid, signal.SIGKILL)
    # 只停这个临时目录里起的进程（worker 的子进程）
    r = subprocess.run(["pgrep", "-f", T], capture_output=True, text=True)
    for pid in r.stdout.split():
        if int(pid) != os.getpid():
            try:
                os.kill(int(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
    if not os.environ.get("E2E_KEEP"):
        os.remove(RLINK)
        if os.path.islink(RLINK0):
            os.remove(RLINK0)
        shutil.rmtree(T, ignore_errors=True)


def missing():
    """跑不了的原因（缺 pueue、带 numpy 的 Python）→ 说明；都有 → None。"""
    lack = [b for b in (PUEUE, PUEUED) if not os.access(b, os.X_OK)]
    if lack:
        return "本机没有 " + "、".join(map(os.path.basename, lack)) + "（queue/install_pueue.sh）"
    if not numpy_python():
        return "本机没有带 numpy 的 Python（RB.check_python、miniforge 的环境）"
    return None


def main():
    why = missing()
    if why:
        cleanup()
        print(f"跳过：{why}")
        return 77  # run_tests.py：跳过
    try:
        setup()
        step_env_gate()
        step_stage()
        step_stop()
        step_finish()
        step_gc()
        step_shift()
        print("真的训练做法的小任务：跳过（换名、拉回见 queue/test_relay.py）")
    except Exception as e:  # noqa: BLE001
        if not isinstance(e, AssertionError) or str(e) not in FAILS:
            print(f"失败：{type(e).__name__}: {e}")
            traceback.print_exc()
            FAILS.append(str(e))
        print("--- 最近的检查说明 ---\n" + "\n".join(LOG[-25:]))
        if os.environ.get("E2E_KEEP"):
            print(f"临时目录 {T}")
    finally:
        cleanup()
    print("全部通过" if not FAILS else f"{len(FAILS)} 项失败")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
