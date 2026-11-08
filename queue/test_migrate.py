#!/usr/bin/env python3
"""queue/migrate.py（taskq self migrate）的测试：testdata 的机器表和实验表，hub 的状态目录、各台的根目录、store 的数据目录
都在临时目录里，按旧格式造出来。
  1. log_rename：_seg<段> → _part<段>、按旧任务名改名，作业名里有 "__" 的（别的任务的日志）不动；
  2. 真跑 bin/taskq self migrate 的几种 Stop（记账里有机器表里没有的名字、--merge 指向非执行机）：退出码非 0，盘上不变；
  3. 默认（不加 --apply）只列出：盘上一个字节都不变；
  4. Stop：执行机队列有领着的任务、--merge 的旧根目录队列里有任务：退出码 1，盘上不变；
  5. --apply：总队列、按任务名的记录、领取 / 交还 / 失败 / 跳过里的机器名、冒烟、relay 记账、各台的队列和分段日志、并根目录、
     视图链接、钉住记录、状态目录里不用的文件、sha256 缓存都是新格式；目标已有的不覆盖、列出来；
  6. 再跑一次 --apply：盘上不再变。
机器那一半经 relay_remote.op 真跑各台根目录 code/current 里的 migrate.py root：代码包是手搭的（这份核心放在
proto.BUNDLE_CORE，.taskq.toml 指到测试的配置和插件）；hub（store，ssh=None）本机跑，别的几台经假 ssh 在本机跑。
hub 这一半由本文件 --run 起一个进程跑 migrate.main（进程里把各台的根目录、conda、store 的数据目录指到临时目录）。
    python3 queue/test_migrate.py
"""
import hashlib
import json
import re
import shutil
import os
import sqlite3
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

DRIVER = sys.argv[1:2] == ["--run"]
if DRIVER:  # 本文件起的子进程：环境（HOME、TASKQ_DB、RELAY_SSH、TQM_T）由父进程设好
    fixture.use()
    T = os.environ["TQM_T"]
else:
    fixture.use()
    T = fixture.tmpdir(prefix="tqm")
    os.environ["TQM_T"] = T
    os.environ["HOME"] = os.path.join(T, "home")
    os.makedirs(os.environ["HOME"])
HOME = os.environ["HOME"]
STATE = os.path.join(HOME, ".local", "state", "taskq")  # testdata 的 OUTPUTS["state"]
os.environ["TASKQ_DB"] = os.path.join(STATE, "taskq.db")
os.environ.pop("TASKQ_STATE", None)
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import proto  # noqa: E402

CONDA = os.path.join(T, "conda")
DATA = os.path.join(T, "data")
ROOTS = {"hub": os.path.join(HOME, "demo_root"), "site-pueue": os.path.join(T, "roots", "pueue"),
         "site-slurm": os.path.join(T, "roots", "slurm"), "site-slurm2": os.path.join(T, "roots", "slurm2")}
OLD1 = os.path.join(T, "roots", "lab1_old")  # 旧名 lab1 的根目录，并进 site-pueue
FAKE_SSH = os.path.join(T, "fake_ssh")


def patch():
    """进程里：各台的 conda 指到临时的（python3 是跑测试的这个）；hub 以外的根目录、store 的数据目录指到临时目录。"""
    for n, m in M.MACHINES.items():
        m["conda"] = (CONDA, m["conda"][1])
        if n != "hub":
            m["root"] = ROOTS[n]
    M.MACHINES["hub"]["data"] = DATA


patch()
if DRIVER:
    import migrate  # noqa: E402
    sys.exit(migrate.main(sys.argv[2:]))

os.environ["RELAY_SSH"] = FAKE_SSH
import advance as A  # noqa: E402
import spec as S  # noqa: E402
import experiments as E  # noqa: E402
from flow.compile import build  # noqa: E402
import migrate as MG  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what, flush=True)
    fails += 0 if cond else 1


# ---------------- 环境：conda、假 ssh、各台的代码包 ----------------
os.makedirs(os.path.join(CONDA, "bin"))
os.symlink(sys.executable, os.path.join(CONDA, "bin", "python3"))
with open(FAKE_SSH, "w") as f:  # 跳过 -o 选项；-O check（长连接）算通；去掉主机名，在本机跑，不带 hub 才有的变量
    f.write("#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\n[[ $1 == -O ]] && exit 0\nshift\n"
            "cd / && exec env " + " ".join(f"-u {v}" for v in proto.HUB_ONLY_ENV) + ' bash -c "$*"\n')
os.chmod(FAKE_SSH, 0o755)
for r in ROOTS.values():
    cur = os.path.join(r, proto.ROOT["current"])
    os.makedirs(os.path.dirname(os.path.join(cur, proto.BUNDLE_CORE)))
    os.symlink(CORE, os.path.join(cur, proto.BUNDLE_CORE))
    with open(os.path.join(cur, ".taskq.toml"), "w", encoding="utf-8") as f:
        f.write(f"config = {json.dumps(fixture.CONFIG)}\nplugins = {json.dumps([fixture.PLUGINS])}\n")


def write(p, text):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        f.write(text)
    return p


def read(p):
    with open(p, encoding="utf-8") as f:
        return f.read()


def jl(rows):
    return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)


def tasks_of(p):
    return [json.loads(ln) for ln in read(p).splitlines() if ln.strip()]


# ---------------- 1. log_rename ----------------
K = {"infer/base/piece_0": "infer/t0_f0"}
ok("log_rename：按旧任务名改名，_seg<段> 同时改成 _part<段>",
   MG.log_rename("infer__base__piece_0__job1_seg1.log", K) == "infer__t0_f0__job1_part1.log")
ok("log_rename：不在改名表里的只改 _seg → _part（段号多位也行）",
   MG.log_rename("D101_alpha__V_save__0__w_seg12.log", K) == "D101_alpha__V_save__0__w_part12.log")
ok("log_rename：已经是新名字的 → None", MG.log_rename("infer__t0_f0__job1_part1.log", K) is None)
ok("log_rename：前缀对上、后面作业名里有 __ 的（别的任务 infer/base/piece_0/x 的）不动",
   MG.log_rename("infer__base__piece_0__x__job_part1.log", K) is None
   and MG.log_rename("infer__base__piece_0__job_part1.log", {"infer/base": "zz/new"}) is None)
ok("log_rename：_seg 后面不是段号、不是 .log 的不动",
   MG.log_rename("a__job_segx.log", {}) is None and MG.log_rename("a__job_seg1.txt", {}) is None)
ok("log_rename: a job name that already has _seg only gets the last one changed",
   MG.log_rename("a__j_seg_seg2.log", {}) == "a__j_seg_part2.log")

# ---------------- English rewrite of old part logs and failure records ----------------
EL = os.path.join(T, "english")
ED = os.path.join(EL, "data")
out, say = [], [].append


def old_head(job, part, key, env="base", code="abc1234", t="2026-11-01 10:00:00"):
    return f"作业 {job}_{part} @ node1，任务={key}，第 {part} 段，环境 {env}，代码 {code}，{t}\n"


CASES = {
    "done": "done/key",
    "gpu": "gpu/key",
    "failed": "failed/key",
    "not_started": "not_started/key",
    "unreadable": "unreadable/key",
    "stalled": "stalled/key",
}
write(os.path.join(ED, proto.TASK_LOGS, f"{proto.safe('done/key')}__j_2_part1.log"),
      old_head("j", 2, "done/key") + "训完 2026-11-01 11:00:00\n")
write(os.path.join(ED, proto.TASK_LOGS, f"{proto.safe('gpu/key')}__j_1_part1.log"),
      old_head("j", 1, "gpu/key") + "2026-11-01 10:02:00 卡 0 被别人占着，等了 30 分钟还没空出来，不跑\n")
write(os.path.join(ED, proto.TASK_LOGS, f"{proto.safe('failed/key')}__j_1_part1.log"),
      old_head("j", 1, "failed/key") + "2026-11-01 10:03:00 命令结束（退出码 1），没训完（第 1 次）\n")
write(os.path.join(ED, proto.TASK_LOGS, f"{proto.safe('not_started/key')}__j_1_part1.log"),
      old_head("j", 1, "not_started/key") + "2026-11-01 10:04:00 没开训：这台机器没有环境 x\n")
write(os.path.join(ED, proto.TASK_LOGS, f"{proto.safe('unreadable/key')}__j_1_part1.log"),
      old_head("j", 1, "unreadable/key") + "2026-11-01 10:05:00 命令结束（退出码 1），读不了盘，判断不了训完没有\n")
write(os.path.join(ED, proto.TASK_LOGS, f"{proto.safe('stalled/key')}__w_1_part1.log"),
      old_head("w", 1, "stalled/key")
      + "当作卡住 2026-11-01 11:00:00：日志和结果文件夹 30 分钟没有变化，作业 w 停掉它\n")
MG.english_logs(ED, say, False)
import tasklog as TL  # noqa: E402
st = {k: TL.parse(os.path.join(ED, proto.TASK_LOGS, f"{proto.safe(CASES[k])}__"
                               f"{'w' if k == 'stalled' else 'j'}_1_part1.log"), CASES[k])[0]
      for k in CASES if k != "done"}
st["done"] = TL.parse(os.path.join(ED, proto.TASK_LOGS, f"{proto.safe('done/key')}__j_2_part1.log"), "done/key")[0]
ok("english_logs: migrated logs parse to the same states, exit codes and part number",
   {k: a["state"] for k, a in st.items()} == {"done": "done", "gpu": "gpu_busy", "failed": "failed", "not_started": "not_started",
                                               "unreadable": "unreadable", "stalled": "stalled"}
   and st["done"]["part"] == 2 and st["failed"]["exit"] == 1
   and st["not_started"]["note"] == "this machine has no env x (machine table machines.py)"
   and st["stalled"]["note"] == "log and output dir unchanged for 30 minutes, job w stopped it")
ok("english_logs: no Chinese left in the migrated logs",
   not any(re.search(r"[\u4e00-\u9fff]", read(os.path.join(ED, proto.TASK_LOGS, f))) for f in os.listdir(os.path.join(ED, proto.TASK_LOGS))))
BEFORE = {f: read(os.path.join(ED, proto.TASK_LOGS, f)) for f in os.listdir(os.path.join(ED, proto.TASK_LOGS))}
out2, say2 = [], [].append
MG.english_logs(ED, say2, False)
ok("english_logs: a second run changes nothing and reports nothing",
   {f: read(os.path.join(ED, proto.TASK_LOGS, f)) for f in os.listdir(os.path.join(ED, proto.TASK_LOGS))} == BEFORE and not out2)
HALF = write(os.path.join(ED, proto.TASK_LOGS, "half__j_part1.log"),
             old_head("j", 1, "half/key") + "DONE 2026-11-01 11:00:00\n" + "2026-11-01 11:01:00 命令结束（退出码 0），没训完（第 1 次）\n")
MG.english_logs(ED, say2, False)
ok("english_logs: a half-rewritten log comes out fully new",
   read(HALF) == (proto.LOG_HEAD_FMT % ("j_1", "node1", "half/key", 1, "base", "abc1234", "2026-11-01 10:00:00") + "\n"
                  + "DONE 2026-11-01 11:00:00\n" + "2026-11-01 11:01:00 command ended (exit 0), not done (attempt 1)\n"))

FQ = os.path.join(EL, "queue")
write(os.path.join(FQ, proto.QUEUE["failures"], proto.safe("k/a")),
      "2026-11-01 10:00:00\t作业 1\tboom\tA\n2026-11-01 10:01:00\tjob 2\tboom\tB\n")
MG.english_failures(FQ, say2, False)
import taskq  # noqa: E402
KEEP_TQ = taskq.TQ
taskq.TQ = FQ
ok("english_failures: old job field rewritten, new line kept, fail_lines counts both",
   read(os.path.join(FQ, proto.QUEUE["failures"], proto.safe("k/a")))
   == "2026-11-01 10:00:00\tjob 1\tboom\tA\n2026-11-01 10:01:00\tjob 2\tboom\tB\n" and taskq.failures("k/a") == 2)
taskq.TQ = KEEP_TQ

# ---------------- merge_root：目标还没有 blobs/ ----------------
MR, MO = os.path.join(T, "mr", "new"), os.path.join(T, "mr", "old")
for d in (MR, os.path.join(MO, proto.ROOT["blobs"], "ab")):
    os.makedirs(d)
open(os.path.join(MO, proto.ROOT["blobs"], "ab", "ab" * 32), "w").close()
MG.merge_root(MR, MO, lambda _: None, False)
ok("merge_root：目标根目录还没有 blobs/ 时照样硬链接过去",
   os.path.samefile(os.path.join(MR, proto.ROOT["blobs"], "ab", "ab" * 32), os.path.join(MO, proto.ROOT["blobs"], "ab", "ab" * 32)))
shutil.rmtree(os.path.join(T, "mr"))

# ---------------- 旧格式的盘上状态 ----------------
INST = build().instances
INF, EV, DUP, GONE = "infer/base/piece_0", "eval/base/piece_0", "infer/base/piece_0_dup", "old/gone"
NINF, NEV = "infer/t0_f0", "eval/t0_f0"
SRC = "D101_alpha/V_save/0"  # 训练（做法 toy），名字不变
TOY = E.expand(E.experiment("t0"))[0]
sf = proto.safe


def old_p(key, out):
    return dict(key=key, cmd="bash x.sh", out="{data}/" + out, done="MANIFEST.json", env="base")


P_INF, P_EV = old_p(INF, INST[NINF].outputs[0]), old_p(EV, INST[NEV].outputs[0])
P_DUP, P_GONE = old_p(DUP, INST[NINF].outputs[0]), old_p(GONE, "nowhere/x")
TQ = os.path.join(STATE, "taskq")
Q = proto.QUEUE
R1, R2 = ROOTS["site-pueue"], ROOTS["site-slurm"]
D1, D2 = os.path.join(R1, proto.ROOT["data"]), os.path.join(R2, proto.ROOT["data"])
L1, L2 = os.path.join(D1, proto.TASK_LOGS), os.path.join(D2, proto.TASK_LOGS)
LO = os.path.join(OLD1, proto.ROOT["data"], proto.TASK_LOGS)
VIEW = os.path.dirname(INST[NINF].outputs[0]).replace("preds/", "results/")  # infer 的输入视图（相对数据目录）
B1 = b"ckpt"
H1 = hashlib.sha256(B1).hexdigest()
B2 = b"blob of lab1"
H2 = hashlib.sha256(B2).hexdigest()

# 总队列
write(os.path.join(TQ, Q["tasks"]), jl([
    dict(order=1, nice=0, key=SRC, exp="t0", recipe="toy", params=dict(TOY["params"], machines=["lab0", "lab1"])),
    dict(order=2, nice=-100, key=INF, exp=None, recipe="command", params=P_INF),
    dict(order=3, nice=-100, key=EV, exp=None, recipe="command", params=P_EV, added=1234.5),
    dict(order=4, nice=-100, key=DUP, exp=None, recipe="command", params=P_DUP),
    dict(order=5, nice=-100, key=GONE, exp=None, recipe="command", params=P_GONE)]))
write(os.path.join(TQ, Q["claims"], sf(INF), proto.CLAIM_INFO),
      json.dumps(dict(worker="lab0", host="lab0", since=1.0, beat=2.0, assigned="lab0")))
write(os.path.join(TQ, Q["released"], sf(EV)), "1700000000.0 lab1\n")
write(os.path.join(TQ, Q["released"], sf(SRC)), f"1700000001.0 dicc {proto.RELEASE_WHY['failed']}\n")
write(os.path.join(TQ, Q["failures"], sf(INF)), "2026-11-01 10:00:00\t作业 lab0\tboom\n")
write(os.path.join(TQ, Q["skips"], sf(SRC)), "2026-11-01 10:00:00\tlab1\t放不下\n2026-11-01 11:00:00\tsite-slurm\t放不下\n")
write(os.path.join(TQ, Q["inputs"], sf(INF)), "1700000000\t缺 a\n")
write(os.path.join(TQ, Q["stopped"], sf(EV)), "2026-11-01 10:00:00\t不要了\n")
write(os.path.join(TQ, Q["released"], sf(GONE)), "1700000002.0 lab1\n")  # 丢掉的任务的记录：一并删
write(os.path.join(TQ, Q["claims"], sf(DUP), proto.CLAIM_INFO), json.dumps(dict(worker="lab1")))
SM = os.path.join(TQ, proto.SMOKE_DIR)
write(os.path.join(SM, "lab0.json"), json.dumps({"k1": dict(t=100, ok=True), "k2": dict(t=300, ok=True)}))
write(os.path.join(SM, "lab1.json"), json.dumps({"k1": dict(t=200, ok=False)}))
write(os.path.join(SM, "site-slurm.json"), json.dumps({"k3": dict(t=5, ok=True)}))
write(os.path.join(SM, "lab0.json.lock"), "")

# relay 的记账
REC_INF = dict(recipe="command", params=P_INF, data=["raw/in"], staged=1.0, views=[VIEW], inputs=[], outs=[INST[NINF].outputs[0]],
               cleanup=[], unq=7)
REC_EV = dict(recipe="command", params=P_EV, data=[], staged=2.0, views=[], inputs=[], outs=[INST[NEV].outputs[0]], cleanup=[], unq=2)
REC_SRC = dict(recipe="toy", params=TOY["spec"]["params"], data=[], staged=3.0, views=["prepared/D101_alpha"], inputs=[],
               outs=[TOY["spec"]["out"]], cleanup=[], links={"prepared/D101_alpha/x.dat": "ab" * 32})
write(os.path.join(STATE, "relay", "state.json"), json.dumps(dict(
    keys={"lab0": {INF: REC_INF}, "lab1": {GONE: dict(recipe="command", params=P_GONE, views=[], unq=1)},
          "dicc": {EV: REC_EV}, "site-slurm": {SRC: REC_SRC}},
    views={"lab0": {VIEW: 10}, "lab1": {"v/lab1": 20}, "dicc": {"v/dicc": 30}, "site-slurm": {"prepared/D101_alpha": 40}},
    last_ok={"lab0": 100, "lab1": 200, "dicc": 300, "site-slurm": 50},
    bundle_for={"site-slurm": "abc"}, held={GONE: 1.0})))

# 状态目录里不再用的、要留着的
JUNK = [write(os.path.join(STATE, "shift.json"), "{}"), write(os.path.join(STATE, "relay", "state.json.bak"), "{}"),
        write(os.path.join(STATE, "relay", "push_lab0.log"), "x"), os.path.dirname(write(os.path.join(STATE, "relay", "old", "a"), "x")),
        write(os.path.join(STATE, "health", "status.json"), "{}"), write(os.path.join(STATE, "health", "probs.json.migrated"), "{}"),
        write(os.path.join(STATE, "last", "a"), "x"), write(os.path.join(STATE, "last", "b"), "x")]
KEEP = [write(os.path.join(STATE, "tasks.json"), "{}"), write(os.path.join(STATE, "health", "problems.json"), "{}"),
        write(os.path.join(STATE, "relay", "bundles", "x", "f"), "x")]
OLD_HLOG = write(os.path.join(STATE, "health", "health.log"), "2026-11-01 00:00:00 旧的一行\n")

# hub 数据库（版本 1，有 sha256 缓存）
os.makedirs(STATE, exist_ok=True)
import db as DB  # noqa: E402
con = sqlite3.connect(os.environ["TASKQ_DB"])
con.executescript(DB.SCHEMA[0])
con.executemany("INSERT INTO hash(k, sha) VALUES(?, ?)", [("/d/a|1|2", "s1"), ("/d/b|3|4", "s2")])
con.execute("PRAGMA user_version=1")
con.commit()
con.close()

# site-pueue 的根目录（旧名 lab0 的）
sp_inf = S.sendable(S.resolve("command", P_INF))
sp_ev = S.sendable(S.resolve("command", P_EV))
Q1, Q2 = os.path.join(R1, proto.ROOT["queue"]), os.path.join(R2, proto.ROOT["queue"])
write(os.path.join(Q1, Q["tasks"]), jl([dict(order=1, nice=-100, key=INF, exp=None, recipe="command", params=P_INF, spec=sp_inf)]))
write(os.path.join(Q1, Q["released"], sf(INF)), "1700000000.0 site-pueue-gpu1\n")
write(os.path.join(Q1, Q["skips"], sf(INF)), "2026-11-01 10:00:00\tsite-pueue-gpu1\t放不下\n2026-11-01 10:00:00\tlab0-gpu\t放不下\n")
write(os.path.join(Q1, Q["skips"], sf(GONE)), "2026-11-01 10:00:00\tlab0-gpu\t放不下\n")
write(os.path.join(Q1, Q["failures"], sf(INF)), "2026-11-01 10:00:00\t作业 lab0-gpu\tboom\n")
write(os.path.join(R1, proto.ROOT["pins"], sf(INF) + ".json"), "{}")
write(os.path.join(R1, proto.ROOT["pins"], "other.json"), "{}")
write(os.path.join(L1, f"{sf(INF)}__lab0-gpu_seg1.log"), "a")
write(os.path.join(L1, f"{sf(INF)}__j2_part1.log"), "old")
write(os.path.join(L1, f"{sf(NINF)}__j2_part1.log"), "new")  # 目标已有：不覆盖
write(os.path.join(L1, f"{sf(SRC)}__j_seg2.log"), "b")
write(os.path.join(L1, f"{sf(INF)}__x__j_part1.log"),
      "作业 j_1 @ node1，任务=other/key，第 1 段，环境 base，代码 abc1234，2026-11-01 10:00:00\n"
      "2026-11-01 10:00:01 开始\n训完 2026-11-01 11:00:00\n")  # 作业名里有 __：别的任务的
write(os.path.join(R1, proto.blob(H1)), "ckpt")
write(os.path.join(D1, VIEW, "dataset.json"), "{}")
os.makedirs(os.path.join(D1, VIEW, "frag_0"))
os.symlink(os.path.join(R1, proto.blob(H1)), os.path.join(D1, VIEW, "frag_0", "final.ckpt"))
os.symlink("/etc/hostname", os.path.join(D1, VIEW, "plans.json"))  # 不是指向 blobs 的：不算链接
# 旧名 lab1 的根目录（同一台机器上，并进 site-pueue）
write(os.path.join(OLD1, proto.ROOT["queue"], Q["tasks"]), "")
write(os.path.join(LO, f"{sf(EV)}__lab1-gpu_seg1.log"), "d")
write(os.path.join(LO, f"{sf(NINF)}__j2_part1.log"), "lab1")  # site-pueue 已有同名的：留着
write(os.path.join(OLD1, proto.ROOT["data"], proto.WORKER_LOGS, "w_9.out"), "e")
write(os.path.join(OLD1, proto.blob(H2)), B2.decode())
write(os.path.join(OLD1, proto.blob(H2) + proto.BLOB_STAT), "12 1")
write(os.path.join(OLD1, proto.blob(H1)), "ckpt")  # site-pueue 已有
# site-slurm 的根目录（旧名 dicc 的记录也在这里）
write(os.path.join(Q2, Q["tasks"]), jl([dict(order=1, nice=0, key=SRC, exp="t0", recipe="toy", params=TOY["params"], spec=TOY["spec"]),
                                        dict(order=2, nice=-100, key=EV, exp=None, recipe="command", params=P_EV, spec=sp_ev)]))
write(os.path.join(Q2, Q["skips"], sf(SRC)), "2026-11-01 10:00:00\tsite-slurm-gpu\t放不下\n2026-11-01 10:00:00\tdicc-gpu\t放不下\n")
write(os.path.join(L2, f"{sf(EV)}__dicc_seg1.log"), "f")
# store（hub 那一项）：根目录的钉住记录、数据目录里拉回来的分段日志
write(os.path.join(ROOTS["hub"], proto.ROOT["pins"], sf(INF) + ".json"), "{}")
write(os.path.join(DATA, proto.TASK_LOGS, f"{sf(EV)}__dicc_seg1.log"), "g")
write(os.path.join(ROOTS["hub"], proto.ROOT["data"], proto.TASK_LOGS, f"{sf(INF)}__x_seg3.log"), "h")


def tree():
    """临时目录下的全部（不进软链接、不看 __pycache__）：路径 → 种类和内容。"""
    out = {}
    for dp, dns, fns in os.walk(T):
        dns[:] = [d for d in dns if d != "__pycache__"]
        for n in dns + fns:
            p = os.path.join(dp, n)
            if os.path.islink(p):
                out[p] = ("link", os.readlink(p))
            elif os.path.isdir(p):
                out[p] = ("dir",)
            else:
                with open(p, "rb") as f:
                    out[p] = ("file", f.read())
    return out


def diff(a, b):
    return sorted(p for p in set(a) | set(b) if a.get(p) != b.get(p))


def taskq_cli(*args):
    r = subprocess.run([os.path.join(CORE, "bin", "taskq"), "self", "migrate", *args], capture_output=True, text=True, timeout=300)
    return r.returncode, r.stdout + r.stderr


def run(*args):
    r = subprocess.run([sys.executable, os.path.abspath(__file__), "--run", *args], capture_output=True, text=True, timeout=900)
    return r.returncode, r.stdout, r.stderr


REN = ["--rename", "lab0=site-pueue", "lab1=site-pueue", "dicc=site-slurm"]
MERGE = ["--merge", f"lab1={OLD1}"]

# ---------------- 2. bin/taskq self migrate 的 Stop（不碰各台） ----------------
t0 = tree()
rc, out = taskq_cli("--help")
ok("bin/taskq self migrate --help", rc == 0 and "--apply" in out)
rc, out = taskq_cli("--apply")
ok(f"记账里有机器表里没有的名字、没给 --rename：退出码非 0，列出 lab0、lab1、dicc（{out.strip()[-200:]}）",
   rc != 0 and all(n in out for n in ("lab0", "lab1", "dicc")) and "--rename" in out)
rc, out = taskq_cli("--apply", "--rename", "lab0=site-pueue", "lab1=site-pueue")
ok("只缺一个（dicc）：只列出它", rc != 0 and "dicc" in out and "lab0" not in out.split("taskq self migrate：")[-1])
rc, out = taskq_cli("--apply", "--rename", "lab0=site-pueue", "lab1=nosuch", "dicc=site-slurm")
ok("--rename 到机器表里没有的名字：同样停下", rc != 0 and "lab1" in out)
rc, out = taskq_cli("--apply", *REN, "--merge", f"hub={OLD1}")
ok(f"--merge 指向非执行机（hub）：退出码非 0（{out.strip()[-120:]}）", rc != 0 and "--merge" in out and "hub" in out)
rc, out = taskq_cli("--apply", *REN, "--merge", "lab1")
ok("--merge 写错（没有 =）：退出码非 0", rc != 0 and "旧=新" in out)
ok("这几种都没改盘上的东西", diff(t0, tree()) == [])

# ---------------- 3. 默认只列出 ----------------
rc, out, err = run(*REN, *MERGE)
d = diff(t0, tree())
ok(f"不加 --apply：退出码 0，盘上一个字节都不变（{d[:5]} {err.strip()[-300:]}）", rc == 0 and d == [])
ok("列出：丢掉的旧任务和原因（对不上的、和别的对上同一个实例的）",
   f"丢掉 {GONE}：实验表里没有产出是它的步骤" in out and f"丢掉 {DUP}：和别的任务对上同一个步骤实例 {NINF}" in out)
ok("列出：各台做什么（改名的分段日志、目标已有留着的、并根目录、去掉的跳过记录、删的钉住记录）",
   "part logs" in out and "留着" in out and f"{OLD1}/{proto.ROOT['data']}/{proto.TASK_LOGS} → {R1}" in out
   and "skip records of workers that no longer exist" in out and "删钉住记录" in out)
ok("列出：冒烟记录、状态目录里删的、sha256 缓存，最后说没改",
   "冒烟记录：['lab0', 'lab1', 'site-slurm'] → ['site-pueue', 'site-slurm']" in out
   and all(f"删 {p}" in out for p in JUNK) and not any(f"删 {p}" in out for p in KEEP)
   and "2 条 sha256 缓存" in out and "只列出，没改" in out)

# ---------------- 4. Stop：各台有不能自动处理的 ----------------
claim = write(os.path.join(Q2, Q["claims"], sf(EV), proto.CLAIM_INFO), json.dumps(dict(worker="site-slurm-gpu")))
t1 = tree()
rc, out, err = run("--apply", *REN, *MERGE)
d = diff(t1, tree())
ok(f"执行机（Slurm）队列里有领着的任务：退出码 1、说是哪台，盘上不变（{d[:5]} {err.strip()[-200:]}）",
   rc == 1 and "Slurm" in err and "领着的任务" in err and d == [])
os.remove(claim)
os.rmdir(os.path.dirname(claim))
os.rmdir(os.path.dirname(os.path.dirname(claim)))
lab1_q = os.path.join(OLD1, proto.ROOT["queue"], Q["tasks"])
write(lab1_q, jl([dict(order=1, nice=0, key=GONE, exp=None, recipe="command", params=P_GONE)]))
t1 = tree()
rc, out, err = run("--apply", *REN, *MERGE)
d = diff(t1, tree())
ok(f"--merge 的旧根目录队列里还有任务：退出码 1，盘上不变（{d[:5]} {err.strip()[-200:]}）",
   rc == 1 and f"{OLD1} 的队列里还有 1 个任务" in err and d == [])
write(lab1_q, "")
cur2 = os.path.join(ROOTS["site-slurm2"], proto.ROOT["current"])
os.rename(cur2, cur2 + ".x")
t1 = tree()
rc, out, err = run("--apply", *REN, *MERGE)
d = diff(t1, tree())
ok(f"有一台没换上新代码包（跑不了 migrate.py root）：退出码 1、没有 traceback、提示先换包，盘上不变（{d[:5]} {err.strip()[-200:]}）",
   rc == 1 and "Traceback" not in err and "taskq site bundle site-slurm2" in err and "什么都还没改" in err and d == [])
os.rename(cur2 + ".x", cur2)
ok("Stop 的几种试完：盘上回到原样", diff(t0, tree()) == [])

# ---------------- 5. --apply ----------------
rc, out, err = run("--apply", *REN, *MERGE)
ok(f"--apply：退出码 0（{(err or out).strip()[-600:]}）", rc == 0 and "迁移完成" in out)
ok("最后列出删并掉的旧根目录的命令（旧根目录本身没删）", f"rm -rf {OLD1}" in out and os.path.isdir(OLD1))

# 总队列
hub = {t["key"]: t for t in tasks_of(os.path.join(TQ, Q["tasks"]))}
ok(f"总队列：旧任务改成步骤实例的名字，对不上的、重复的丢掉（{sorted(hub)}）", sorted(hub) == sorted([SRC, NINF, NEV]))
ok("改名的任务：参数换成 advance.queue_params、不属于实验",
   hub[NINF]["params"] == A.queue_params(INST[NINF]) and hub[NEV]["params"] == A.queue_params(INST[NEV])
   and hub[NINF]["exp"] is None and hub[NINF]["recipe"] == "command")
ok("added：缺的补 0，有的不动", hub[NINF]["added"] == 0 and hub[SRC]["added"] == 0 and hub[NEV]["added"] == 1234.5)
ok("params.machines 按 --rename 改名（并成一台）", hub[SRC]["params"]["machines"] == ["site-pueue"])
ok("改名后的任务都解析得了", all(E.resolve_task(t["recipe"], t["params"], t.get("exp")) for t in hub.values()))
ci = json.load(open(os.path.join(TQ, Q["claims"], sf(NINF), proto.CLAIM_INFO)))
ok("领取：目录按新任务名，worker / host / assigned 里的机器名改了",
   not os.path.exists(os.path.join(TQ, Q["claims"], sf(INF)))
   and ci == dict(worker="site-pueue", host="site-pueue", since=1.0, beat=2.0, assigned="site-pueue"))
ok("交还：按新任务名、机器改名、两段的补原因 back，三段的原因不动",
   read(os.path.join(TQ, Q["released"], sf(NEV))) == f"1700000000.0 site-pueue {proto.RELEASE_WHY['back']}\n"
   and read(os.path.join(TQ, Q["released"], sf(SRC))) == f"1700000001.0 site-slurm {proto.RELEASE_WHY['failed']}\n"
   and not os.path.exists(os.path.join(TQ, Q["released"], sf(EV))))
ok("失败记录：按新任务名，第 2 段（作业 <机器>）里的机器名改了",
   read(os.path.join(TQ, Q["failures"], sf(NINF))) == "2026-11-01 10:00:00\tjob site-pueue\tboom\tbefore-migrate\n")
ok("跳过记录：机器名改了",
   read(os.path.join(TQ, Q["skips"], sf(SRC))) == "2026-11-01 10:00:00\tsite-pueue\t放不下\n2026-11-01 11:00:00\tsite-slurm\t放不下\n")
ok("缺输入、停止记录：按新任务名",
   os.path.exists(os.path.join(TQ, Q["inputs"], sf(NINF))) and os.path.exists(os.path.join(TQ, Q["stopped"], sf(NEV)))
   and not os.path.exists(os.path.join(TQ, Q["inputs"], sf(INF))) and not os.path.exists(os.path.join(TQ, Q["stopped"], sf(EV))))

# 冒烟
ok("冒烟记录：几个旧名并成新名、同一种取 t 最新的；旧文件和 .lock 删掉",
   sorted(os.listdir(SM)) == ["site-pueue.json", "site-slurm.json"]
   and json.load(open(os.path.join(SM, "site-pueue.json"))) == {"k1": dict(t=200, ok=False), "k2": dict(t=300, ok=True)}
   and json.load(open(os.path.join(SM, "site-slurm.json"))) == {"k3": dict(t=5, ok=True)})

# relay 的记账
st = json.load(open(os.path.join(STATE, "relay", "state.json")))
ks = st.get("keys") or {}
ok(f"relay 记账：机器改名、几个旧名并成一个、并掉的旧根目录（lab1）的记录丢掉（{ {n: sorted(r) for n, r in ks.items()} }）",
   {n: sorted(r) for n, r in ks.items()} == {"site-pueue": [NINF], "site-slurm": sorted([SRC, NEV])})
ri, re_, rs = ks["site-pueue"][NINF], ks["site-slurm"][NEV], ks["site-slurm"][SRC]
ok("relay 记账：改名的任务参数换新、去掉 unq，别的字段照旧",
   ri["params"] == A.queue_params(INST[NINF]) and re_["params"] == A.queue_params(INST[NEV])
   and not any("unq" in r for r in (ri, re_, rs)) and ri["views"] == [VIEW] and ri["outs"] == REC_INF["outs"])
ok("relay 记账：旧的 data 改叫 inputs、旧的 inputs（放过去的文件）改叫 placed；任务参数 data 也改名（哪种做法都一样）",
   ri["inputs"] == ["raw/in"] and ri["placed"] == [] and not any("data" in r for r in (ri, re_, rs))
   and MG.renamed_params(dict(cmd="c", data=["x"])) == dict(cmd="c", inputs=["x"])
   and MG.renamed_params(dict(dataset="d", data=["y"])) == dict(dataset="d", inputs=["y"]) and MG.renamed_params(dict(a=1)) == dict(a=1))
ok(f"relay 记账：补 links（视图里指向本机 blobs 的软链接 → sha256；别处的链接、普通文件不算），已有的不动（{ri.get('links')}）",
   ri.get("links") == {f"{VIEW}/frag_0/final.ckpt": H1} and re_.get("links") == {} and rs["links"] == REC_SRC["links"])
ok("relay 记账：views 合并（并掉的旧根目录的不要）、last_ok 取大、bundle_for 去掉",
   st.get("views") == {"site-pueue": {VIEW: 10}, "site-slurm": {"v/dicc": 30, "prepared/D101_alpha": 40}}
   and st.get("last_ok") == {"site-pueue": 200, "site-slurm": 300} and "bundle_for" not in st and "held" not in st)
ok("丢掉的任务：hub 和执行机上的领取、交还、跳过记录一并删",
   not any(os.path.exists(os.path.join(TQ, Q[d], sf(k))) for d in ("claims", "released") for k in (DUP, GONE))
   and not os.path.exists(os.path.join(Q1, Q["skips"], sf(GONE))))

# 各台的队列
q1 = {t["key"]: t for t in tasks_of(os.path.join(Q1, Q["tasks"]))}
q2 = {t["key"]: t for t in tasks_of(os.path.join(Q2, Q["tasks"]))}
ok(f"执行机队列：任务改名、说明里的 key 跟着改、补 added（{sorted(q1)} {sorted(q2)}）",
   sorted(q1) == [NINF] and sorted(q2) == sorted([SRC, NEV]) and q1[NINF]["spec"]["key"] == NINF
   and q2[NEV]["spec"]["key"] == NEV and all(t.get("added") is not None for t in list(q1.values()) + list(q2.values())))
ok(f"执行机队列：重新送说明不重算 added（旧日志照样算做了一半；{[t.get('added') for t in q1.values()]}）",
   q1[NINF]["added"] == 0 and q2[NEV]["added"] == 0)
ok("执行机队列：hub 重新送了说明（按现在的做法解析的：带步骤名、产出是相对路径）",
   q1[NINF]["spec"].get("step") == "infer" and q1[NINF]["spec"]["out"] == INST[NINF].outputs[0]
   and q2[NEV]["spec"].get("step") == "eval")
ok("执行机队列：交还记录按新任务名、补原因；失败记录按新任务名",
   read(os.path.join(Q1, Q["released"], sf(NINF))) == f"1700000000.0 site-pueue-gpu1 {proto.RELEASE_WHY['back']}\n"
   and os.path.exists(os.path.join(Q1, Q["failures"], sf(NINF))) and not os.path.exists(os.path.join(Q1, Q["failures"], sf(INF))))
ok("执行机队列：跳过记录只留现在还有的 worker 的，全都没了的文件删掉",
   read(os.path.join(Q1, Q["skips"], sf(NINF))) == "2026-11-01 10:00:00\tsite-pueue-gpu1\t放不下\n"
   and not os.path.exists(os.path.join(Q1, Q["skips"], sf(GONE)))
   and read(os.path.join(Q2, Q["skips"], sf(SRC))) == "2026-11-01 10:00:00\tsite-slurm-gpu\t放不下\n")

# 分段日志、并根目录、钉住记录
ok(f"site-pueue 的分段日志：改名、_seg → _part；作业名里有 __ 的不动；目标已有的不覆盖、两个都留着（{sorted(os.listdir(L1))}）",
   sorted(os.listdir(L1)) == sorted([f"{sf(NINF)}__lab0-gpu_part1.log", f"{sf(INF)}__j2_part1.log", f"{sf(NINF)}__j2_part1.log",
                                     f"{sf(SRC)}__j_part2.log", f"{sf(INF)}__x__j_part1.log", f"{sf(NEV)}__lab1-gpu_part1.log"])
   and read(os.path.join(L1, f"{sf(NINF)}__j2_part1.log")) == "new" and read(os.path.join(L1, f"{sf(INF)}__j2_part1.log")) == "old")
ok("old part logs on an executor are rewritten to English",
   read(os.path.join(L1, f"{sf(INF)}__x__j_part1.log"))
   == "job j_1 @ node1, task=other/key, part 1, env base, code abc1234, 2026-11-01 10:00:00\n"
      "2026-11-01 10:00:01 开始\nDONE 2026-11-01 11:00:00\n")
ok("并根目录：旧根目录的分段日志、worker 日志搬过来；site-pueue 已有同名的留在旧根目录",
   read(os.path.join(L1, f"{sf(NEV)}__lab1-gpu_part1.log")) == "d"
   and read(os.path.join(D1, proto.WORKER_LOGS, "w_9.out")) == "e"
   and os.listdir(LO) == [f"{sf(NINF)}__j2_part1.log"] and read(os.path.join(LO, f"{sf(NINF)}__j2_part1.log")) == "lab1")
ok("并根目录：同一个盘的 blob（含 .st）硬链接过来，已有的不动",
   os.path.samefile(os.path.join(OLD1, proto.blob(H2)), os.path.join(R1, proto.blob(H2)))
   and os.path.samefile(os.path.join(OLD1, proto.blob(H2) + proto.BLOB_STAT), os.path.join(R1, proto.blob(H2) + proto.BLOB_STAT))
   and not os.path.samefile(os.path.join(OLD1, proto.blob(H1)), os.path.join(R1, proto.blob(H1))))
ok("site-slurm、store 的数据目录、store 根目录的数据目录：分段日志改名",
   os.listdir(L2) == [f"{sf(NEV)}__dicc_part1.log"]
   and os.listdir(os.path.join(DATA, proto.TASK_LOGS)) == [f"{sf(NEV)}__dicc_part1.log"]
   and os.listdir(os.path.join(ROOTS["hub"], proto.ROOT["data"], proto.TASK_LOGS)) == [f"{sf(NINF)}__x_part3.log"])
ok("钉住记录：换了名字的任务的删掉（执行机和 store），别的留着",
   os.listdir(os.path.join(R1, proto.ROOT["pins"])) == ["other.json"]
   and os.listdir(os.path.join(ROOTS["hub"], proto.ROOT["pins"])) == [])

# 状态目录、数据库
ok("状态目录：不再用的删掉，要用的留着",
   not any(os.path.lexists(p) for p in JUNK) and all(os.path.exists(p) for p in KEEP)
   and os.path.isdir(os.path.join(STATE, "last")) and os.listdir(os.path.join(STATE, "last")) == [])
ok("健康检查的纯文字日志：状态目录 health/ 里的那份接到 cfg.log 的 health.log 后面",
   not os.path.exists(OLD_HLOG) and "旧的一行" in open(cfg.log("health")).read())
con = sqlite3.connect(os.environ["TASKQ_DB"])
ver = con.execute("PRAGMA user_version").fetchone()[0]
has_hash = DB.has(con, "hash")
con.close()
hp = os.path.join(ROOTS["hub"], proto.ROOT["hashes"])
rows = []
if os.path.exists(hp):
    hc = sqlite3.connect(hp)
    rows = sorted(hc.execute("SELECT k, sha FROM hash").fetchall())
    hc.close()
ok(f"sha256 缓存搬到 store 根目录的 {proto.ROOT['hashes']}，hub 数据库迁移到现在的版本（没有 hash 表）（{ver} {rows}）",
   rows == [("/d/a|1|2", "s1"), ("/d/b|3|4", "s2")] and ver == DB.VERSION and not has_hash)

# ---------------- 6. 再跑一次 ----------------
t2 = tree()
rc, out, err = run("--apply", *REN, *MERGE)
d = diff(t2, tree())
ok(f"再跑一次 --apply：退出码 0，盘上不再变（{d[:5]} {(err or '').strip()[-300:]}）", rc == 0 and d == [])
ok("再跑一次：没有改名、丢掉的；名字没变的记录不列成\"留着\"（只剩旧根目录里那个同名的）",
   "0 个改成步骤实例的名字，丢掉 0 个" in out and "part logs" not in out
   and [ln for ln in out.splitlines() if "留着" in ln and LO not in ln] == [])

print("全部通过" if not fails else f"{fails} 项失败")
sys.exit(1 if fails else 0)
