#!/usr/bin/env python3
"""全流程测试：加任务 → 派给执行机 → 传数据 → 训练 → 拉回 → 判断做完 → 下一步。两种布局各在一个子进程里跑（同时跑）：

  together  hub 兼 store、兼一台 pueue 执行机（ssh=None，两个 worker：1 号卡全天、0 号卡按时段），另有一台 pueue 执行机 site-pueue
            （假的 ssh，经隧道传）。site-pueue 上：建环境 → 任务单加任务 → 领、传数据 → 开 worker → 中途停、接着做 → 拉回 → 清理；
            hub 自己这台：同一个盘上硬链接进 blobs、不开隧道，任务环境里没有 hub 的变量；两个 worker：按时段的那个时段结束时交还，
            另一个从存档接着做完。
  store     hub、store、执行机三个根目录各不相同，store 经假的 ssh 连：store 直接送给执行机，拉回落到 store，做完在 store 上判断，
            实验表 FINAL 的两步接着排、做完后删掉中间产物（temp）；最后核对 hub 的盘上（状态目录里也算）没有写数据文件。

完全隔离：根目录、数据目录、总队列、机器表（临时的配置文件夹，TASKQ_CONFIG）都在临时目录里；pueue 是自己起的 pueued（自己的配置
和 socket），结束时只停它们；ssh 换成本机的 bash -c，不连任何机器。各机器跑的都是它根目录 code/current 里的代码包（工作区的代码）。
跑法：python3 sites/test_e2e.py（E2E_LAYOUT=together|store 只跑一种；E2E_TMP=<目录> 临时目录放在哪，E2E_KEEP=1 留下临时目录）"""
import datetime as dt
import glob
import hashlib
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
LAYOUTS = ("together", "store")
LAYOUT = os.environ.get("E2E_LAYOUT")
PUEUE = shutil.which("pueue") or os.path.expanduser("~/.local/bin/pueue")
PUEUED = shutil.which("pueued") or os.path.expanduser("~/.local/bin/pueued")


def numpy_python():
    cands = [sys.executable] + sorted(glob.glob(os.path.expanduser("~/miniforge3/envs/*/bin/python"))) \
        + [os.path.expanduser("~/miniforge3/bin/python3")]
    return next((p for p in cands if p and os.path.exists(p)
                 and subprocess.run([p, "-c", "import numpy, venv"], capture_output=True).returncode == 0), None)


def missing():
    """跑不了的原因（缺 pueue、带 numpy 的 Python）→ 说明；都有 → None。"""
    lack = [b for b in (PUEUE, PUEUED) if not os.access(b, os.X_OK)]
    if lack:
        return "本机没有 " + "、".join(map(os.path.basename, lack)) + "（queue/install_pueue.sh）"
    if not numpy_python():
        return "本机没有带 numpy 的 Python（miniforge 的环境）"
    return None


def runner():
    """两种布局各起一个子进程同时跑，跑完按布局打印各自的输出。"""
    why = missing()
    if why:
        print(f"SKIP:{why}")
        return 77  # run_tests.py：跳过
    ps = {n: subprocess.Popen([sys.executable, os.path.abspath(__file__)], env=dict(os.environ, E2E_LAYOUT=n),
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for n in LAYOUTS}
    bad = []
    for n, p in ps.items():
        out, _ = p.communicate()
        print(f"======== {n} ========\n{out.rstrip()}", flush=True)
        if p.returncode:
            bad.append(n)
    print("全部通过" if not bad else f"没通过的布局：{'、'.join(bad)}")
    return 1 if bad else 0


if __name__ == "__main__" and not LAYOUT:
    sys.exit(runner())

# ======================================================================================================
# 下面是一种布局的子进程（E2E_LAYOUT）
# ======================================================================================================
sys.path.insert(0, os.path.join(CORE, "queue", "testdata"))
sys.path.insert(0, os.path.join(CORE, "queue"))
import fixture  # noqa: E402
import proto  # noqa: E402  只用标准库，不读配置

fixture.use()
KEEP = bool(os.environ.get("E2E_KEEP"))
T = (tempfile.mkdtemp if KEEP else fixture.tmpdir)(prefix=f"e2e_{LAYOUT}_", dir=os.environ.get("E2E_TMP") or None)
LOCAL = re.escape(socket.gethostname().split(".")[0])
BIN = os.path.join(T, "bin")
FAKE = os.path.join(BIN, "ssh")
CONDA = os.path.join(T, "conda")  # 假的 conda：activate 只把 bin 放进 PATH（bin/python3 是本机一个有 numpy 的 Python）
ORDER = os.path.join(T, "order")  # sitecustomize 记下的 site_ops 操作（哪台、什么操作）、拉回时放文件的先后
HOOK = os.path.join(T, "hook")
CONF = os.path.join(T, "config")
HS = os.path.join(T, "hs")  # hub 的状态目录（policy 的 OUTPUTS["state"]）：总队列、relay 的记录、代码包
for d in (BIN, ORDER, HOOK, os.path.join(CONF, "specs"), os.path.join(HS, "taskq"), os.path.join(T, "tmp_hub")):
    os.makedirs(d)
open(os.path.join(HS, "taskq", proto.QUEUE["enabled"]), "w").close()
# 假的 ssh：跳过 -o 和机器名，在本机跑；hub 这边的变量不带过去（同 relay_remote.site_argv 对 ssh=None 的做法），临时目录每台各用各的
with open(FAKE, "w") as f:
    f.write('#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\nhost=$1; shift\n'
            f'export TMPDIR={T}/tmp_$host; mkdir -p "$TMPDIR"\n'
            f'cd "$HOME" && exec env {" ".join("-u " + v for v in proto.HUB_ONLY_ENV)} bash -c "$*"\n')
with open(os.path.join(BIN, "nvidia-smi"), "w") as f:  # 不碰真的卡
    f.write("#!/bin/bash\nexit 0\n")
for p in (FAKE, os.path.join(BIN, "nvidia-smi")):
    os.chmod(p, 0o755)
with open(os.path.join(HOOK, "sitecustomize.py"), "w") as f:  # 只记录、不改行为：哪台跑了哪个 site_ops 操作；pull 里 os.replace 的目标
    f.write('''import os, sys
_d = os.environ.get("E2E_ORDER")
if _d and sys.argv and sys.argv[0].endswith("site_ops.py") and len(sys.argv) > 1:
    with open(os.path.join(_d, "ops.log"), "a") as f:
        f.write("%s\\t%s\\t%d\\n" % (os.environ.get("TASKQ_MACHINE", ""), sys.argv[1], os.getpid()))
    if sys.argv[1] == "pull":
        _rep = os.replace
        def _r(a, b, *k, **kw):
            _rep(a, b, *k, **kw)
            with open(os.path.join(_d, "pull_%d.txt" % os.getpid()), "a") as f:
                f.write(os.path.abspath(b) + "\\n")
        os.replace = _r
''')
s_ = socket.socket()
s_.bind(("127.0.0.1", 0))
PORT = s_.getsockname()[1]
s_.close()
TRANSFER = (f"http://127.0.0.1:{PORT}", f"127.0.0.1:{PORT}")
ENVS = {"sys": ("conda", "base"), "toy": "spec"}


def hm(hours):
    return (dt.datetime.now() + dt.timedelta(hours=hours)).strftime("%H:%M")


def pueue_home(tag):
    """自己的 pueued 的目录、配置和指到它的 pueue（放在各根目录以外）。"""
    d = os.path.join(T, f"pq_{tag}")
    os.makedirs(d)
    cfgf = os.path.join(d, "c.yml")
    wrap = os.path.join(T, f"pw_{tag}", "pueue")
    os.makedirs(os.path.dirname(wrap))
    with open(wrap, "w") as f:
        f.write(f'#!/bin/bash\nexec {PUEUE} -c {cfgf} "$@"\n')
    os.chmod(wrap, 0o755)
    with open(cfgf, "w") as f:
        f.write(f"shared:\n  pueue_directory: {d}\n  runtime_directory: {d}\n  unix_socket_path: {SHORT}/{tag}.sock\n"
                f"  use_unix_socket: true\ndaemon:\n  callback: null\n")
    return dict(dir=d, cfg=cfgf, wrap=wrap)


# 系统临时目录里的一个短目录（结束时删掉）：pueued 的 socket（路径长度有限）；执行机 site-pueue 的根目录实际在 T/r，
# 经这里的一个软链接用（根目录经软链接到达的情况也测到）
SHORT = tempfile.mkdtemp(prefix="e2e_", dir="/tmp")
os.makedirs(os.path.join(T, "r"))
RLINK = os.path.join(SHORT, "r")
os.symlink(os.path.join(T, "r"), RLINK)
NAME = "site-pueue"
PQS = pueue_home("site")
SITE = dict(label="pueue", hostname=r"(?!)", ssh="fake-pueue-host", scheduler="pueue", root=RLINK, conda=(CONDA, "base"), envs=ENVS,
            pueue=PQS["wrap"], workers=[dict(gpu="1")], tunnel=("push", "pull"), budget=(0, 0))
if LAYOUT == "together":
    HR, HD = os.path.join(T, "hr"), os.path.join(T, "hd")  # hub 的根目录、数据目录（hub 兼 store）
    PQH = pueue_home("hub")
    MACHINES = {
        "hub": dict(label="hub", hostname=LOCAL, ssh=None, roles=("hub", "store"), root=HR, data=HD, conda=(CONDA, "base"), envs=ENVS,
                    scheduler="pueue", pueue=PQH["wrap"], workers=[dict(gpu="1"), dict(gpu="0", hours=[(hm(-2), hm(2), None, None)])],
                    transfer=TRANSFER, budget=(0, 0)),
        NAME: SITE,
    }
    READY = {NAME: 5, "hub": 1}
    EXPS = None
    SD = HD  # store 的数据目录
else:
    SR, SD = os.path.join(T, "sr"), os.path.join(T, "sd")  # store 的根目录、数据目录
    PQH = None
    MACHINES = {
        "hub": dict(label="hub", hostname=LOCAL, ssh=None, roles=("hub",)),
        "store": dict(label="store", hostname=r"(?!)", ssh="fake-store", roles=("store",), root=SR, data=SD, conda=(CONDA, "base"),
                      transfer=TRANSFER, budget=(0, 0)),
        NAME: SITE,
    }
    READY = {NAME: 2}
    EXPS = os.path.join(T, "exps", "e2e", "experiments.py")
with open(os.path.join(CONF, "machines.py"), "w") as f:
    f.write(f'"""全流程测试（{LAYOUT}）的机器表：test_e2e.py 写的。"""\nMACHINES = {MACHINES!r}\n')
with open(os.path.join(CONF, "policy.py"), "w") as f:
    f.write(f'"""全流程测试（{LAYOUT}）的 policy：test_e2e.py 写的。"""\nimport os\n\nimport cfg\n\n'
            'AUTO = dict(advance=True, envs=False, revive=False, smoke=False)\nPAUSE = {}\nHOLD = []\nPLACE = {}\n'
            f'READY = {READY!r}\nSLURM = {{}}\n'
            f'OUTPUTS = dict(state={HS + "/"!r}, bundle=(os.path.dirname(cfg.CORE_REL) or ".",), experiments={EXPS!r}, name="e2e")\n')
# 环境描述：建在一个有 numpy 的基础环境上的 venv（pip 不用下载）
with open(os.path.join(CONF, "specs", "toy.yml"), "w") as f:
    f.write("# 测试\nbase: sys\npip:\n  - numpy\n")
with open(os.path.join(CONF, "specs", "toy.check.py"), "w") as f:
    f.write("import numpy\nprint('numpy', numpy.__version__)\n")

os.environ.update(
    TASKQ_CONFIG=CONF, PATH=BIN + ":" + os.environ["PATH"], RELAY_SSH=FAKE, TUNNEL_SSH=FAKE, TUNNEL_POLL="0.1", TUNNEL_IDLE="5",
    TMPDIR=os.path.join(T, "tmp_hub"),  # 被停掉的 worker 留下的临时文件跟着 T 删
    # worker（consume.py）、run_task.sh 的测试用开关：等得短一些
    TASKQ_POLL="1", TASKQ_IDLE_POLL="1", TASKQ_PATIENCE="0", TASKQ_IDLE_EXIT="6", TASKQ_STOP_POLL="1", TASKQ_STOP_WAIT="5",
    TASKQ_HEAD_CMD="echo same", RUN_TASK_POLL="1",
    TASKQ_TEST_NOW=os.path.join(T, "now"),  # 按时段的 worker 看的"现在"（文件不在时是真的时间）
    E2E_ORDER=ORDER, PYTHONPATH=HOOK)
for v in ("TASKQ_DIR", "TASKQ_DATA", "TASKQ_ROOT", "TASKQ_MACHINE", "TASKQ_STATE", "TUNNEL_LOCK", "TUNNEL_URL", "TUNNEL_BIND"):
    os.environ.pop(v, None)

import cfg  # noqa: E402
cfg.use_config()
REPO = cfg.REPO  # 使用者的仓库根
for d in (cfg.rel("queue"), cfg.rel("sync"), cfg.rel("monitor"), cfg.rel("sites")):
    sys.path.insert(0, os.path.join(REPO, d))
import mtab as M  # noqa: E402
import common as C  # noqa: E402
import spec as S  # noqa: E402
import taskq  # noqa: E402
import relay  # noqa: E402
import refill  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_store as ST  # noqa: E402
import relay_transfer as RT  # noqa: E402
import relay_bundle as RB  # noqa: E402
import envs as E  # noqa: E402
import pueue as PQ  # noqa: E402

HUB = M.hub()
LOG, FAILS, DAEMONS = [], [], []
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


def step(msg, cond, why="", fatal=True):
    if not cond and callable(why):  # 贵的说明只在失败时算
        why = why()
    print(("PASS:" if cond else "FAIL:") + msg + ("" if cond else f"（{why}）"), flush=True)
    if not cond:
        FAILS.append(msg)
        if fatal:
            raise AssertionError(msg)


def wb(p, data):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb" if isinstance(data, bytes) else "w") as f:
        f.write(data)


def text_of(i):
    return f"数据 {i}\n" * i


def pq(home, *args):
    r = subprocess.run([PUEUE, "-c", home["cfg"], *args], capture_output=True, text=True, timeout=60)
    return r.returncode, r.stdout + r.stderr


def jobs(home):
    """自己的 pueued 里的任务 → [[任务号, 标签, 状态, 结果], …]（sites/pueue.py 的解析）；读不到 → None。"""
    rc, out = pq(home, "status", "--json")
    return PQ.parse(out, list(json.loads(out).get("groups") or {})) if rc == 0 else None


def start_pueued(home, groups):
    p = subprocess.Popen([PUEUED, "-c", home["cfg"]], stdout=open(os.path.join(home["dir"], "daemon.log"), "w"),
                         stderr=subprocess.STDOUT, start_new_session=True)
    DAEMONS.append((home, p))
    for _ in range(50):
        if pq(home, "status", "--json")[0] == 0:
            break
        time.sleep(0.2)
    for g in groups:
        rc, out = pq(home, "group", "add", g)
        assert rc == 0, out


def run_local(cmd, host, timeout, input=None):
    return C.run(cmd, host, timeout, input=input, ssh=[FAKE])


def setup_common():
    py = numpy_python()
    os.makedirs(os.path.join(CONDA, "bin"))
    for n in ("python3", "python"):
        os.symlink(py, os.path.join(CONDA, "bin", n))
    wb(os.path.join(CONDA, "etc/profile.d/conda.sh"),
       f'conda() {{ if [[ ${{1:-}} == activate ]]; then export PATH="{CONDA}/bin:$PATH"; fi; }}\n')
    start_pueued(PQS, ["gpu1"])
    if PQH:
        start_pueued(PQH, ["gpu1", "gpu0"])
    relay.refill.add = fake_add
    # 代码包：工作区的代码（还没进 git 的新文件经临时的 index 带上，不动真的 index），各台由检查部署
    idx = os.path.join(T, "index")
    shutil.copy(os.path.join(REPO, subprocess.run(["git", "-C", REPO, "rev-parse", "--git-path", "index"], capture_output=True,
                                                  text=True).stdout.strip()), idx)
    env = dict(os.environ, GIT_INDEX_FILE=idx)
    subprocess.run(["git", "-C", REPO, "add", "-N", "--", "queue", "sites", "sync", "monitor", "bin"], env=env, check=True, capture_output=True)
    os.environ["GIT_INDEX_FILE"] = idx
    try:
        bid, path = RB.build(None)
    finally:
        os.environ.pop("GIT_INDEX_FILE")
    commit = subprocess.run(["git", "-C", REPO, "rev-parse", "origin/main"], capture_output=True, text=True).stdout.strip()
    core = subprocess.run(["git", "-C", cfg.CORE, "rev-parse", RB.CORE_REF], capture_output=True, text=True).stdout.strip()
    relay.BUNDLE_REF = commit  # 测试中途别处推送 main 也不重打包
    ST.update_state(lambda st: st.setdefault("bundle_for", {}).update({f"{commit}:{core}": bid}))
    return bid


# ---------------- hub 的 IO 组：在进程里直接做 ----------------
ADDED = []


def fake_add(host, lbl, step_, key, args, run=None, **rec):
    ADDED.append((step_, key, rec.get("to")))
    if step_ == "relay_stage":
        RT.stage(rec["to"], key, nice=int(re.search(r"--nice (-?\d+)", args).group(1)), say=LOG.append)
    elif step_ == "relay_fetch":
        RT.fetch(rec["to"], key, say=LOG.append)
    else:
        raise AssertionError(f"没想到的 hub 任务 {step_} {key}")
    LOG.append(f"hub 任务 {step_} {key}")
    return len(LOG)


def snapshot():
    """health.gather 里调度要用的部分（出错时的处理同它）：总队列（读不了 → None，原因在 why）、store 上的训练状态
    （出错 → 原因在 collect，不当成 store 连不上）。"""
    s = dict(taskq={}, pueue={HUB: dict(tasks=[])}, records=[], ok={HUB: True}, why={}, src={}, collect={})
    try:
        q = s["taskq"][HUB] = taskq.local_status()
    except taskq.DiskError as e:
        s["taskq"][HUB], s["why"]["taskq"] = None, str(e)
        return s
    try:
        s["src"][M.store()] = RM.store_op("collect", dict(specs=S.task_specs(q["tasks"]), wait=5), timeout=120)
    except RuntimeError as e:
        s["collect"][M.store()] = str(e)[-300:]
    return s


def check(adv=False, src_ok=True):
    """一次健康检查里调度的部分（advance.run → relay.run_all）；有出错的说明、卡住的问题、store 上收集训练状态出错
    （src_ok 时）→ AssertionError。→ (说明, 这次的状态)。"""
    s = snapshot()
    if src_ok and (s["why"] or s["collect"]):
        raise AssertionError(f"检查出错：{s['why']} {s['collect']}")
    stuck = {}
    if adv:
        import advance
        did, st = advance.run(s)
        LOG.extend(f"advance：{x}" for x in did)
        stuck.update(st)
        s["taskq"][HUB] = taskq.local_status()
    msgs, st = relay.run_all(s, False)
    LOG.extend(msgs)
    stuck.update(st)
    bad = [m for m in msgs if "出错" in m]
    if bad or stuck:
        raise AssertionError(f"检查出错：{bad}；卡住的：{ {k: proto.alert_msg(v) for k, v in stuck.items()} }")
    return msgs, s


def hub_tasks():
    return {t["key"]: t for t in taskq.local_status()["tasks"]}


def keys(name=NAME):
    return ST.keys_of(ST.load_state(), name)


def central(args):
    rc, out, err = taskq.central(args)
    assert rc == 0, out + err
    return out


def ops():
    """sitecustomize 记下的 site_ops 操作 → [(机器, 操作)]。"""
    try:
        return [tuple(ln.split("\t")[:2]) for ln in open(os.path.join(ORDER, "ops.log")).read().splitlines()]
    except OSError:
        return []


def pulled(out):
    """最近一次拉回 out 时 os.replace 放进数据目录的先后（.relay_in 里的暂存不算）→ 相对数据目录的路径。"""
    seq = []
    for f in sorted(glob.glob(os.path.join(ORDER, "pull_*.txt")), key=os.path.getmtime):
        lines = [ln for ln in open(f).read().splitlines() if f"/{proto.RELAY_INBOX}/" not in ln]
        hit = [os.path.relpath(ln, SD) for ln in lines if ln.startswith(os.path.join(SD, out) + "/")]
        if hit:
            seq = hit
    return seq


def wait(cond, timeout, what, every=1.0, act=None):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if act:
            act()
        if cond():
            return
        time.sleep(every)
    raise AssertionError(f"{timeout} 秒内没等到：{what}")


def view_files(name=NAME):
    """那边每个任务的视图（文件夹）、单个的输入文件里的文件 → 软链接指向。"""
    D = os.path.join(M.MACHINES[name]["root"], "data")
    out = {}
    for k in keys(name).values():
        for v in k["views"] + k.get("placed", []):
            ps = [os.path.join(D, v)] if not os.path.isdir(os.path.join(D, v)) else \
                [os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(D, v)) for f in fs]  # 视图是一个文件或一个文件夹
            for p in ps:
                out[os.path.relpath(p, D)] = os.readlink(p) if os.path.islink(p) else None
    return out


def blob_files(root):
    """根目录 blobs 里的内容文件（旁边的 .st 记录不算）。"""
    return [os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(root, "blobs")) for f in fs if not f.endswith(".st")]


def step_deploy(bid):
    _, s = check(src_ok=False)  # store 上还没有代码包：store 上的操作（收集训练状态；store 不是 hub 时还有总队列的状态）都读不到
    step("第一次检查（还没部署）：读不到的只是 store 上的操作，不当成 hub、store 连不上",
         set(s["why"]) <= {"taskq"} and (s["why"] or s["collect"]) and all(s["ok"].values()), (s["why"], s["collect"], s["ok"]))
    cur = relay.current_bundle(M.store())
    step("第一次检查就把代码包部署到 store（读不到总队列时也先换包，之后的检查才读得到）", cur == bid, f"store 上的 current 是 {cur!r}")
    check()
    roots = [m["root"] for m in M.MACHINES.values() if m.get("root")]
    cur = {r: os.path.basename(os.path.realpath(os.path.join(r, proto.ROOT["current"]))) for r in roots}
    step(f"第一次检查：代码包 {bid} 部署到每台有根目录的机器（store、执行机；本机的也是）", all(v == bid for v in cur.values()), cur)
    rs = relay.remote_status(NAME)
    step("执行机那边的队列开着、是空的，自己的 pueued 起来了，状态里的代码包是这个",
         rs is not None and rs["bundle"] == bid and rs["taskq"]["enabled"] and rs["taskq"]["tasks"] == [] and rs["jobs"] == [], rs)
    used = int(subprocess.run(["du", "-sb", os.path.join(T, "r")], capture_output=True, text=True).stdout.split()[0])
    M.MACHINES[NAME]["cap"] = 1
    try:
        rs2 = relay.remote_status(NAME)
    finally:
        M.MACHINES[NAME].pop("cap")
    step("cap：剩余按 cap 减去根目录已占的算，不看整个盘", rs2 is not None and 0 < rs2["free"] <= 1e9 - used * 0.5, (rs2 or {}).get("free"))


# ======================================================================================================
# together：hub 兼 store、兼执行机
# ======================================================================================================
KEYS = [f"guest/e2e/t{i}" for i in range(1, 6)]
SLOW = KEYS[0]
RROOT = RLINK


def sheet(key, inputs, cmd, out, machines):
    return dict(key=key, owner="alice", env="toy", inputs=inputs, cmd=cmd, out=out, done=["DONE"], progress="progress_json",
                resume=["ckpt_*.json"], log="train.log", res={"gpus": 1}, machines=machines, nice=50,
                out_gb=0.01)  # 挑执行机时给产出留的空间：测试的盘上剩多少都放得下


def setup_together():
    # store（hub）上的输入：共用的代码、每个任务自己的数据（t5 的是一个文件）
    wb(os.path.join(HD, "guest/e2e/code/toy.py"), TOY)
    for i in range(1, 5):
        wb(os.path.join(HD, f"guest/e2e/d{i}/a.txt"), text_of(i))
    wb(os.path.join(HD, "guest/e2e/one.txt"), text_of(5))
    sheets = []
    for i, k in enumerate(KEYS, 1):
        data = "{data}/guest/e2e/one.txt" if i == 5 else f"{{data}}/guest/e2e/d{i}"
        sheets.append(sheet(k, ["guest/e2e/code", "guest/e2e/one.txt" if i == 5 else f"guest/e2e/d{i}"],
                            f"python3 {{data}}/guest/e2e/code/toy.py --data {data} --out {{data}}/guest/e2e/r/t{i}"
                            + (" --pause 600" if k == SLOW else ""), f"guest/e2e/r/t{i}", [NAME]))
    wb(os.path.join(T, "sheets/job.json"), json.dumps(sheets, ensure_ascii=False))


def step_env_gate():
    out = central(f"add --file {T}/sheets/job.json")
    bt = hub_tasks()
    step("任务单加了 5 个任务，默认 relay=path", sorted(bt) == KEYS and all(t["spec"]["relay"] == "path" for t in bt.values()), out)
    st = E.states(NAME, run=run_local)
    step("建环境前：toy 没建", st == {"sys": "不是按描述建的", "toy": "没建"}, st)
    check()
    bt, rs = hub_tasks(), relay.remote_status(NAME)
    step("没有 .ok：一个都不领、不传，那边队列空，不开 worker",
         all(t["state"] == "free" and not t["worker"] for t in bt.values()) and rs["taskq"]["tasks"] == [] and jobs(PQS) == [],
         [(t["key"], t["state"], t["worker"]) for t in bt.values()])
    rc, out = E.build(NAME, "toy", run=run_local)
    okf = os.path.join(RROOT, "state/envs/toy.ok")
    step("taskq site env 的建法（envs.build）：建好、检查过、写 .ok（= 描述加这台的变体的锁的哈希）",
         rc == 0 and open(okf).read().strip() == M.spec_hash("toy", M.spec_variant(NAME, "toy")), out)
    a = M.activate(NAME, "toy")
    r = subprocess.run(["bash", "-c", a + "\npython -c 'import numpy,sys; print(sys.prefix)'"], capture_output=True, text=True)
    step("激活后用的是建好的 venv，能导入 numpy", r.returncode == 0 and "/envs/toy-" in r.stdout, r.stdout + r.stderr)
    st = E.states(NAME, run=run_local)
    step("状态：toy ok", st["toy"] == "ok", st)


def step_stage():
    n0 = len(LOG)
    check()
    bt, rs = hub_tasks(), relay.remote_status(NAME)
    step("有了 .ok：5 个都领给 site-pueue、传过去、加进那边的队列",
         all(t["state"] == "claimed" and t["worker"] == NAME for t in bt.values())
         and sorted(t["state"] for t in rs["taskq"]["tasks"]) == ["free"] * 5,
         lambda: ([(t["key"], t["state"]) for t in rs["taskq"]["tasks"]], LOG[n0:][-20:]))
    pushes = [o for o in ops() if o[1] == "push"]
    step("数据由 store（hub）送给执行机：push 在 store 上跑、经隧道传",
         pushes and all(m == HUB for m, _ in pushes) and any("经隧道传" in x for x in LOG[n0:]), (pushes, LOG[n0:][-20:]))
    blobs = blob_files(RROOT)
    vf = view_files()
    D = os.path.join(RROOT, "data")
    step(f"数据按内容放进 blobs（{len(blobs)} 个，共用的代码只一份），视图是指向 blobs 的软链接",
         len(blobs) == 6 and len(vf) == 6 and all(t and os.path.dirname(os.path.dirname(os.path.realpath(t))) == os.path.realpath(os.path.join(RROOT, "blobs"))
                                                  and os.path.isfile(os.path.join(D, p))
                                        for p, t in vf.items()), (len(blobs), vf))
    k1 = keys()[SLOW]
    step("和 store 一样的路径：视图是输入的文件夹、结果在 out 原样、那边的 key 就是总队列的 key",
         k1["views"] == ["guest/e2e/code", "guest/e2e/d1"] and k1["params"]["out"] == "guest/e2e/r/t1" and k1["outs"] == ["guest/e2e/r/t1"]
         and sorted(t["key"] for t in rs["taskq"]["tasks"]) == KEYS, k1)
    step("那边还没开 worker（这次检查开始时还没有备好的任务）", jobs(PQS) == [], jobs(PQS))
    # 两个任务的视图同时在：一个重新传数据只清自己的视图
    k2 = keys()[KEYS[1]]
    own2 = [v for v in k2["views"] if v not in k1["views"]][0]
    own1 = [v for v in k1["views"] if v not in k2["views"]][0]
    blob = blobs[0]
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
    js = jobs(PQS)
    step("下一次检查：有备好的任务了，开了一个 worker", len(js) == 1 and js[0][1] == f"{proto.WORKER_LABEL}{NAME}-gpu1", js)


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
        t = hub_tasks()[SLOW]
        return t["state"] == "stopped" and not t["worker"]
    wait(released, 90, "总队列里停了 t1 后，那边停下、hub 交还", every=2, act=check)
    rs = relay.remote_status(NAME)
    a = next(t for t in rs["taskq"]["tasks"] if t["key"] == SLOW)
    f = (rs.get("tasks") or {}).get(SLOW)
    t = hub_tasks()[SLOW]
    skips = os.path.join(HS, "taskq", proto.QUEUE["skips"])
    step("停下后：那边是 stopped、没人领，按存档算有进度，hub 交还时不带 skip",
         a["state"] == "stopped" and not a.get("worker") and refill.site_progress(f) and t["skipped_by"] == []
         and not (os.path.isdir(skips) and os.listdir(skips)), (a, f, t["skipped_by"]))
    step("那边留着第 2 轮的存档、没有 DONE",
         os.path.exists(os.path.join(remote_out(SLOW), "ckpt_02.json")) and not os.path.exists(os.path.join(remote_out(SLOW), "DONE")),
         os.listdir(remote_out(SLOW)))
    # 别的任务还在用视图时清理：只删不要的
    before = view_files()
    reg = (ST.load_state().get("views") or {}).get(NAME) or {}
    keep = {v for k in keys().values() for v in ST.paths_of(k)}
    g = ST.gc(NAME, keep, RM.op, soft_gb=1e9, views=reg)
    step("还有任务要用时清理（relay_store.gc）：要用的视图和 blob 一个不删",
         g["removed_views"] == [] and view_files() == before and all(os.path.isfile(os.path.join(RROOT, "data", p)) for p in before), g)
    central(f"resume {SLOW}")


def step_finish():
    seen_drop = []

    def all_done():
        bt = hub_tasks()
        ks = keys()
        if ks and len(ks) < len(KEYS):  # 有的已经删了、有的还在：还在的视图都完好
            vf = view_files()
            assert all(os.path.isfile(os.path.join(RROOT, "data", p)) for p in vf), "删一个任务时动了别的任务的视图"
            seen_drop.append(len(ks))
        return all(t["state"] == "done" for t in bt.values()) and not ks
    wait(all_done, 300, "5 个都做完、拉回、那边删掉", every=2, act=check)
    step("一个任务做完删掉时，别的任务的视图都还在", bool(seen_drop), "没碰上有删有留的时候")
    pulls = [o for o in ops() if o[1] == "pull"]
    step("拉回在 store（hub）上跑", pulls and all(m == HUB for m, _ in pulls), pulls)
    for i, k in enumerate(KEYS, 1):
        out = os.path.join(HD, f"guest/e2e/r/t{i}")
        res = json.load(open(os.path.join(out, "result.json")))
        want = sum(text_of(i).encode())
        step(f"{k} 拉回 store：真名路径、内容对（3 轮的和、读到的输入）{'、从第 2 轮的存档接着做' if k == SLOW else ''}",
             res["text"] == text_of(i) and res["total"] == 3 * want and res["start"] == (2 if k == SLOW else 0)
             and json.load(open(os.path.join(out, "PROGRESS.json")))["done"] == 3 and os.path.exists(os.path.join(out, "DONE")), res)
        seq = pulled(f"guest/e2e/r/t{i}")
        step(f"{k} 拉回时 DONE 最后放", seq and seq[-1] == f"guest/e2e/r/t{i}/DONE", seq)
        logs = [f for f in os.listdir(os.path.join(HD, proto.TASK_LOGS)) if f.startswith(proto.safe(k) + proto.SAFE)]
        step(f"{k} 的分段日志拉回了", bool(logs), os.listdir(os.path.join(HD, proto.TASK_LOGS)))
    r = subprocess.run(["grep", "-rlF", HD, os.path.join(RROOT, "data")], capture_output=True, text=True)
    step("执行机的数据目录里没有 store 的绝对路径（命令里的 {data} 在那边展开）", r.stdout.strip() == "", r.stdout[:500])


def step_gc():
    M.MACHINES[NAME]["budget"] = (1e6, 0)  # 软线高得够不着：能删的都删
    relay.VIEW_KEEP_S = 0
    check()
    D = os.path.join(RROOT, "data")
    left = [os.path.relpath(os.path.join(dp, f), D) for dp, _, fs in os.walk(D) for f in fs]
    blobs = [f for _, _, fs in os.walk(os.path.join(RROOT, "blobs")) for f in fs]  # .st 记录也跟着删
    reg = (ST.load_state().get("views") or {}).get(NAME) or {}
    top = sorted(os.listdir(RROOT))
    step("全部做完后清理（relay_store.gc）：视图、blob 都删了，数据目录只剩 worker 的日志，登记的视图清空",
         not blobs and all(p.startswith("logs/workers/") for p in left) and not reg, (left[:10], blobs[:5], reg))
    step("根目录只剩还要用的：代码、队列、环境和它的状态", set(top) <= {"code", "queue", "blobs", "data", "tmp", "state", "envs"}
         and os.path.isfile(os.path.join(RROOT, "state/envs/toy.ok")), top)
    relay.VIEW_KEEP_S = 6 * 3600
    M.MACHINES[NAME]["budget"] = (0, 0)


K7 = "guest/e2e/t7"


def step_local():
    """hub 兼 store 兼执行机（ssh=None）：不经假的 ssh、不开隧道，数据在同一个盘上硬链接进 blobs。"""
    step("本机执行机的作业名（worker id-时间；手动跑的主机名-时间）算它的",
         M.machine_of_jobtag(M.workers(HUB)[0]["id"] + "-20261107120000") == M.MACHINES[HUB]["label"]
         and M.machine_of_jobtag(socket.gethostname().split(".")[0] + "-1107") == "hub")
    rc, out = E.build(HUB, "toy")  # 不给 run：按机器表（ssh=None）在本机跑
    step("本机执行机：按描述建环境（建在它自己的根目录下）", rc == 0 and os.path.isfile(os.path.join(HR, "state/envs/toy.ok")), out)
    out_t7 = "{data}/guest/e2e/r/t7"
    wb(os.path.join(T, "sheets/job7.json"), json.dumps([sheet(
        K7, ["guest/e2e/code", "guest/e2e/d2"],
        f"mkdir -p {out_t7} && env > {out_t7}/env.txt && python3 {{data}}/guest/e2e/code/toy.py --data {{data}}/guest/e2e/d2 --out {out_t7}",
        "guest/e2e/r/t7", [HUB])], ensure_ascii=False))
    central(f"add --file {T}/sheets/job7.json")
    n0, nlog = len(ops()), len(LOG)
    check()
    t7 = hub_tasks()[K7]
    blobs = blob_files(HR)
    src = {os.stat(os.path.join(HD, p)).st_ino for p in ("guest/e2e/code/toy.py", "guest/e2e/d2/a.txt")}
    step("本机执行机领了 t7，数据从 store 的数据目录硬链接进它的 blobs（同一个盘，不拷、不经隧道）",
         t7["worker"] == HUB and K7 in keys(HUB) and blobs and {os.stat(b).st_ino for b in blobs} == src
         and not [o for o in ops()[n0:] if o[1] == "push"] and not any("经隧道" in x for x in LOG[nlog:]), (t7, blobs, LOG[nlog:][-8:]))
    wait(lambda: hub_tasks()[K7]["state"] == "done", 300, "本机执行机做完 t7、拉回", every=2, act=check)
    out = os.path.join(HD, "guest/e2e/r/t7")
    res = json.load(open(os.path.join(out, "result.json")))
    step("t7 拉回：结果对", res["text"] == text_of(2) and res["total"] == 3 * sum(text_of(2).encode()), res)
    step("t7 拉回没经隧道（本机拷）", not any("经隧道" in x for x in LOG[nlog:]), LOG[nlog:][-10:])
    env = dict(ln.split("=", 1) for ln in open(os.path.join(out, "env.txt")).read().splitlines() if "=" in ln)
    leaked = {v: env[v] for v in proto.HUB_ONLY_ENV if os.environ.get(v) and env.get(v) == os.environ[v]}
    data = os.path.realpath(env.get("TASKQ_DATA", ""))
    step("本机执行机的任务环境里没有 hub 的配置、数据目录、队列；TASKQ_DATA 是它自己的 <根目录>/data",
         not leaked and data == os.path.realpath(os.path.join(HR, "data")), (leaked, env.get("TASKQ_DATA")))
    check()
    step("总队列做完以后，本机执行机上 t7 的结果删了", K7 not in keys(HUB) and not os.path.exists(os.path.join(HR, "data/guest/e2e/r/t7")),
         keys(HUB))


K6 = "guest/e2e/t6"


def step_two_workers():
    """一台机器两个 worker：0 号卡按时段，时段结束时交还；1 号卡的从存档接着做完。"""
    wait(lambda: all(j[2] == "Done" for j in jobs(PQH)), 60, "hub 上的 worker 都空下来退出", every=1)
    pq(PQH, "pause", "-g", "gpu1")  # 先只让 0 号卡的 worker 领到
    wb(os.path.join(T, "sheets/job6.json"), json.dumps([sheet(
        K6, ["guest/e2e/code", "guest/e2e/d1"],
        "python3 {data}/guest/e2e/code/toy.py --data {data}/guest/e2e/d1 --out {data}/guest/e2e/r/t6 --pause 600",
        "guest/e2e/r/t6", [HUB])], ensure_ascii=False))
    central(f"add --file {T}/sheets/job6.json")
    w0 = f"{proto.WORKER_LABEL}{HUB}-gpu0"
    first = max([j[0] for j in jobs(PQH)], default=-1)

    def running0():
        return any(j[1] == w0 and j[2] == "Running" and j[0] > first for j in jobs(PQH))
    wait(running0, 60, "hub 的 0 号卡 worker 开起来", every=1, act=check)
    o6 = os.path.join(HR, "data/guest/e2e/r/t6")
    wait(lambda: os.path.exists(os.path.join(o6, "ckpt_02.json")), 120, "t6 在 0 号卡上做完第 2 轮", every=0.2)
    rq = {t["key"]: t for t in relay.remote_status(HUB)["taskq"]["tasks"]}
    step("t6 由 0 号卡的 worker 领着", (rq.get(K6) or {}).get("worker") == f"{HUB}-gpu0", rq.get(K6))
    # 时段结束：worker 看 TASKQ_TEST_NOW（3 小时后）；hub 这边的机器表也换成已经结束的时段（不再开 0 号卡的 worker）
    wb(os.environ["TASKQ_TEST_NOW"], str(time.time() + 3 * 3600))
    M.MACHINES[HUB]["workers"][1]["hours"] = [(hm(-3), hm(-2), None, None)]
    jid = max(j[0] for j in jobs(PQH) if j[1] == w0)

    def job(i):
        return next(j for j in jobs(PQH) if j[0] == i)
    wait(lambda: job(jid)[2] == "Done", 90, "0 号卡的 worker 时段结束后退出", every=1)
    jd = job(jid)
    log0 = pq(PQH, "log", "--full", str(jid))[1]
    rq = {t["key"]: t for t in relay.remote_status(HUB)["taskq"]["tasks"]}
    step("时段结束：0 号卡的 worker 停训练、交还、正常退出（Success），t6 在那边的队列里没人领、留着第 2 轮的存档、没有 DONE",
         "时段结束" in log0 and jd[3] == "Success" and rq.get(K6) and not rq[K6].get("worker")
         and os.path.exists(os.path.join(o6, "ckpt_02.json")) and not os.path.exists(os.path.join(o6, "DONE")),
         lambda: (jd, rq.get(K6), log0[-1500:]))
    pq(PQH, "start", "-g", "gpu1")
    wait(lambda: hub_tasks()[K6]["state"] == "done", 300, "1 号卡的 worker 从存档接着做完 t6、拉回", every=2, act=check)
    res = json.load(open(os.path.join(HD, "guest/e2e/r/t6/result.json")))
    step("t6：1 号卡的 worker 从第 2 轮的存档接着做，结果对", res["start"] == 2 and res["total"] == 3 * sum(text_of(1).encode()), res)
    segs = sorted(f for f in os.listdir(os.path.join(HD, proto.TASK_LOGS)) if f.startswith(proto.safe(K6) + proto.SAFE))
    # each part starts with "job <job>_<part> @ host, task=<key>, ..."; two parts started in the same minute share a job name and file
    heads = [ln for f in segs for ln in open(os.path.join(HD, proto.TASK_LOGS, f), errors="replace").read().splitlines()
             if ln.startswith("job ") and f"task={K6}," in ln]
    step("t6 两个 worker 的两段都记在拉回的分段日志里", len(heads) >= 2, (segs, heads))
    later = [j for j in jobs(PQH) if j[1] == w0 and j[0] > jid]
    step("时段结束以后没再开 0 号卡的 worker", not later, later)


def main_together():
    bid = setup_common()
    setup_together()
    step_deploy(bid)
    step_env_gate()
    step_stage()
    step_stop()
    step_finish()
    step_gc()
    step_local()
    step_two_workers()


# ======================================================================================================
# store：hub、store、执行机三个根目录各不相同
# ======================================================================================================
KS = "guest/e2e/s1"
SRC_TXT = text_of(4)
EXP_PY = '''"""全流程测试（store）的实验表：test_e2e.py 写的。s1 训完 → mid（写中间产物 a.part）→ use（读 a.part）；use 做完后删掉 a.part。"""
EXPERIMENTS = []
FINAL = [
    dict(name="mid", env="toy", inputs=["e2e/r/s1", "e2e/code"], out="e2e/mid", done="keep.txt", temp=["*.part"], out_gb=0.01,
         cmd="mkdir -p {data}/e2e/mid && python3 {data}/e2e/code/total.py {data}/e2e/r/s1/result.json > {data}/e2e/mid/a.part"
             " && echo ok > {data}/e2e/mid/keep.txt"),
    dict(name="use", env="toy", inputs=["e2e/mid", "e2e/code"], out="e2e/use", done="use.txt", out_gb=0.01,
         cmd="mkdir -p {data}/e2e/use && cat {data}/e2e/mid/a.part {data}/e2e/mid/keep.txt > {data}/e2e/use/use.txt"),
]
'''
TOTAL_PY = "import json, sys\nprint(json.load(open(sys.argv[1]))['total'])\n"
OUT_NAMES = ("result.json", "PROGRESS.json", "keep.txt", "use.txt", "train.log", "DONE")


def setup_store():
    wb(EXPS, EXP_PY)
    wb(os.path.join(SD, "e2e/code/toy.py"), TOY)
    wb(os.path.join(SD, "e2e/code/total.py"), TOTAL_PY)
    wb(os.path.join(SD, "e2e/d1/a.txt"), SRC_TXT)
    wb(os.path.join(T, "sheets/job.json"), json.dumps([dict(
        key=KS, owner="alice", env="toy", inputs=["e2e/code", "e2e/d1"],
        cmd="python3 {data}/e2e/code/toy.py --data {data}/e2e/d1 --out {data}/e2e/r/s1", out="e2e/r/s1", done=["DONE"],
        progress="progress_json", resume=["ckpt_*.json"], log="train.log", res={"gpus": 1}, nice=50, out_gb=0.01)], ensure_ascii=False))


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def files_under(d, skip=()):
    out = []
    for dp, dns, fs in os.walk(d):
        dns[:] = [x for x in dns if os.path.join(dp, x) not in skip]
        out += [os.path.join(dp, f) for f in fs]
    return out


def main_store():
    bid = setup_common()
    setup_store()
    central(f"add --file {T}/sheets/job.json")
    step_deploy(bid)
    r = subprocess.run([sys.executable, os.path.join(CORE, "queue", "taskq.py"), "status", "--json"], capture_output=True, text=True)
    step("hub 不是 store、没有数据目录：总队列的状态在 store 上算（site_ops facts），读得出（taskq status --json）",
         r.returncode == 0 and S.json_in(r.stdout) is not None, (r.stdout + r.stderr).strip()[-300:])
    rc, out = E.build(NAME, "toy", run=run_local)
    step("执行机上按描述建好 toy", rc == 0, out)
    n0 = len(LOG)

    def finished():
        bt = hub_tasks()
        return all(bt.get(k, {}).get("state") == "done" for k in (KS, "mid", "use")) \
            and not os.path.exists(os.path.join(SD, "e2e/mid/a.part")) and not keys()
    wait(finished, 400, "s1 训完 → mid → use 都做完、拉回，a.part 删掉，执行机上的都删了", every=2, act=lambda: check(adv=True))
    bt = hub_tasks()
    res = json.load(open(os.path.join(SD, "e2e/r/s1/result.json")))
    total = 3 * sum(SRC_TXT.encode())
    step("s1 拉回 store：内容对", res["text"] == SRC_TXT and res["total"] == total and res["start"] == 0, res)
    step("FINAL 的两步按实验表排进总队列（advance）、在执行机上做完",
         bt["mid"]["spec"].get("step") == "mid" and bt["use"]["spec"].get("step") == "use"
         and open(os.path.join(SD, "e2e/use/use.txt")).read() == f"{total}\nok\n", (bt.get("mid"), bt.get("use")))
    step("用到 mid 的步骤都做完后，store 上删掉中间产物 a.part（temp），keep.txt 留着",
         not os.path.exists(os.path.join(SD, "e2e/mid/a.part")) and os.path.isfile(os.path.join(SD, "e2e/mid/keep.txt")),
         os.listdir(os.path.join(SD, "e2e/mid")))
    by = {}
    for m, o in ops():
        by.setdefault(o, set()).add(m)
    want = ("export", "push", "pull", "done", "rm", "collect", "probe")
    step("store 的操作（export、push、pull、done、rm、collect、probe）都在 store 上跑", all(by.get(o) == {"store"} for o in want), by)
    step("hub 自己一个 site_ops 操作都不跑（它没有根目录）", not any(m == HUB for m, _ in ops()), by)
    step("store 直接送给执行机（经隧道）", any("经隧道传" in x and "fake-pueue-host" in x for x in LOG[n0:]), LOG[n0:][-30:])
    for out in ("e2e/r/s1", "e2e/mid", "e2e/use"):
        seq = pulled(out)
        done = {"e2e/r/s1": "DONE", "e2e/mid": "keep.txt", "e2e/use": "use.txt"}[out]
        step(f"{out} 拉回落在 store 的数据目录、做完的标志最后放", seq and seq[-1] == f"{out}/{done}", seq)
    step("执行机上的任务、视图都删了", not keys() and relay.remote_status(NAME)["taskq"]["tasks"] == [], keys())
    left = sorted(os.path.relpath(p, SD) for p in files_under(SD))
    extra = [p for p in left if not p.startswith(("e2e/code/", "e2e/d1/", "e2e/r/s1/", "e2e/mid/", "e2e/use/", proto.TASK_LOGS + "/"))]
    step("store 的数据目录里只有输入、结果和分段日志", not extra, extra)
    # hub 的盘：T 里除了别的机器（store、执行机的根目录和数据目录、它们的临时目录、执行机的 pueued）以外的地方
    other = {os.path.join(T, x) for x in ("sr", "sd", "r", "pq_site")} | set(glob.glob(os.path.join(T, "tmp_fake-*")))
    datas = {sha(p): os.path.relpath(p, SD) for p in files_under(SD) if os.path.getsize(p) >= 16}
    mine = [p for p in files_under(T, skip=other | {ORDER}) if os.path.isfile(p) and not os.path.islink(p)]
    copies = [(os.path.relpath(p, T), datas[sha(p)]) for p in mine if os.path.getsize(p) >= 16 and sha(p) in datas]
    named = [os.path.relpath(p, T) for p in mine if os.path.basename(p) in OUT_NAMES
             or re.fullmatch(r"ckpt_\d+\.json|.*\.part", os.path.basename(p))]
    step("hub 的盘上没有写数据文件（输入、结果的副本都不在 hub 这边，状态目录里也没有）", not copies and not named, (copies[:5], named[:5]))


# ======================================================================================================
def cleanup():
    for home, p in DAEMONS:
        pq(home, "kill", "--all")
    time.sleep(1)
    for home, p in DAEMONS:
        pq(home, "shutdown")
        try:
            p.wait(10)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
    # 只停这个临时目录里起的进程（worker、训练、隧道的服务）
    pids = set()
    for pat in (T, SHORT):
        pids |= set(subprocess.run(["pgrep", "-f", pat], capture_output=True, text=True).stdout.split())
    for pid in pids:
        if int(pid) != os.getpid():
            try:
                os.kill(int(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
    shutil.rmtree(SHORT, ignore_errors=True)


def main():
    try:
        main_together() if LAYOUT == "together" else main_store()
    except Exception as e:  # noqa: BLE001
        if not isinstance(e, AssertionError) or str(e) not in FAILS:
            print(f"FAIL:{type(e).__name__}: {e}")
            traceback.print_exc(file=sys.stdout)
            FAILS.append(str(e))
        print("--- 最近的检查说明 ---\n" + "\n".join(LOG[-30:]))
        if KEEP:
            print(f"临时目录 {T}")
    finally:
        cleanup()
    print("全部通过" if not FAILS else f"{len(FAILS)} 项失败")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
