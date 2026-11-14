#!/usr/bin/env python3
"""Tests for queue/migrate.py (taskq self migrate): the testdata machine table and experiment table, the hub state dir,
each machine's root and the store data dir all in temp dirs, built in the old format.
  1. log_rename: _seg<part> -> _part<part>, rename by old task name, leave job names containing "__" (another task's log);
  2. real runs of bin/taskq self migrate for several Stops (a name in the ledger not in the machine table, --merge to a
     non-executor): non-zero exit, disk unchanged;
  3. default (no --apply) only lists: not one byte changes on disk;
  4. Stop: an executor queue has claimed tasks, the --merge old root queue has tasks: exit 1, disk unchanged;
  5. --apply: queue, per-task records, machine names in claims / releases / failures / skips, smoke records, relay ledger,
     executor queues and part logs, merged roots, view links, pin records, unused state-dir files, sha256 cache all in the
     new format; existing targets are not overwritten and are listed;
  6. running --apply again: nothing on disk changes.
The machine half really runs migrate.py root from each root's code/current through relay_remote.op: the bundle is hand-built
(this core at proto.BUNDLE_CORE, .taskq.toml points to the test config and plugins); the hub (the store, ssh=None) runs
locally, other machines via fake ssh locally. The hub half runs migrate.main in a process started by this file with --run
(the process points each machine's root, conda and the store data dir at temp dirs).
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
if DRIVER:  # child process started by this file: environment (HOME, TASKQ_DB, RELAY_SSH, TQM_T) set by the parent
    fixture.use()
    T = os.environ["TQM_T"]
else:
    fixture.use()
    T = fixture.tmpdir(prefix="tqm")
    os.environ["TQM_T"] = T
    os.environ["HOME"] = os.path.join(T, "home")
    os.makedirs(os.environ["HOME"])
HOME = os.environ["HOME"]
STATE = os.path.join(HOME, ".local", "state", "taskq")  # testdata OUTPUTS["state"]
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
OLD1 = os.path.join(T, "roots", "lab1_old")  # root of old name lab1, merged into site-pueue
FAKE_SSH = os.path.join(T, "fake_ssh")
NEWB = "newbundle0000001"  # the bundle self migrate pushes (relay.wanted_bundle / deploy replaced in the driver process)
os.environ["TASKQ_SWITCH_HOLD"] = "0"


def make_bundle(root, bid):
    """Hand-built bundle <root>/code/<bid>: this core at proto.BUNDLE_CORE, .taskq.toml points to the test config and plugins."""
    b = os.path.join(root, proto.ROOT["code"], bid)
    if os.path.isdir(b):
        return b
    os.makedirs(os.path.dirname(os.path.join(b, proto.BUNDLE_CORE)))
    os.symlink(CORE, os.path.join(b, proto.BUNDLE_CORE))
    with open(os.path.join(b, ".taskq.toml"), "w", encoding="utf-8") as f:
        f.write(f"config = {json.dumps(fixture.CONFIG)}\nplugins = {json.dumps([fixture.PLUGINS])}\n")
    return b


def fake_deploy(name, bid, path, claims=None, switch=True):
    if os.path.exists(os.path.join(T, "unreachable_" + name)):
        raise RuntimeError(f"cannot reach {name}")
    r = os.path.expanduser(M.MACHINES[name]["root"])
    make_bundle(r, bid)
    with open(os.path.join(r, proto.ROOT["machine"]), "w") as f:
        f.write(name + "\n")


def patch():
    """In-process: each machine's conda points at a temp dir (python3 is the one running the tests); roots other than the hub and the store data dir point at temp dirs."""
    for n, m in M.MACHINES.items():
        m["conda"] = (CONDA, m["conda"][1])
        if n != "hub":
            m["root"] = ROOTS[n]
    M.MACHINES["hub"]["data"] = DATA


patch()
if DRIVER:
    import migrate  # noqa: E402
    import relay  # noqa: E402
    relay.wanted_bundle, relay.deploy = (lambda: (NEWB, None)), fake_deploy
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


# ---------------- environment: conda, fake ssh, each machine's bundle ----------------
os.makedirs(os.path.join(CONDA, "bin"))
os.symlink(sys.executable, os.path.join(CONDA, "bin", "python3"))
with open(FAKE_SSH, "w") as f:  # skip -o options; -O check (persistent connection) counts as reachable; drop the host name, run locally, without hub-only variables
    f.write("#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\n[[ $1 == -O ]] && exit 0\nshift\n"
            "cd / && exec env " + " ".join(f"-u {v}" for v in proto.HUB_ONLY_ENV) + ' bash -c "$*"\n')
os.chmod(FAKE_SSH, 0o755)
for r in ROOTS.values():
    make_bundle(r, "oldbundle")
    os.symlink("oldbundle", os.path.join(r, proto.ROOT["current"]))


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
ok("log_rename: rename by old task name, _seg<part> becomes _part<part> too",
   MG.log_rename("infer__base__piece_0__job1_seg1.log", K) == "infer__t0_f0__job1_part1.log")
ok("log_rename: names not in the rename map only change _seg -> _part (multi-digit part too)",
   MG.log_rename("D101_alpha__V_save__0__w_seg12.log", K) == "D101_alpha__V_save__0__w_part12.log")
ok("log_rename: already a new name -> None", MG.log_rename("infer__t0_f0__job1_part1.log", K) is None)
ok("log_rename: prefix matches but the job name after it contains __ (another task infer/base/piece_0/x): unchanged",
   MG.log_rename("infer__base__piece_0__x__job_part1.log", K) is None
   and MG.log_rename("infer__base__piece_0__job_part1.log", {"infer/base": "zz/new"}) is None)
ok("log_rename: _seg not followed by a part number and .log: unchanged",
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
   and st["done"]["part"] == 2 and st["done"]["code"] == "abc1234" and st["failed"]["exit"] == 1
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

# ---------------- merge_root: the target has no blobs/ yet ----------------
MR, MO = os.path.join(T, "mr", "new"), os.path.join(T, "mr", "old")
for d in (MR, os.path.join(MO, proto.ROOT["blobs"], "ab")):
    os.makedirs(d)
open(os.path.join(MO, proto.ROOT["blobs"], "ab", "ab" * 32), "w").close()
MG.merge_root(MR, MO, lambda _: None, False)
ok("merge_root: still hardlinks when the target root has no blobs/ yet",
   os.path.samefile(os.path.join(MR, proto.ROOT["blobs"], "ab", "ab" * 32), os.path.join(MO, proto.ROOT["blobs"], "ab", "ab" * 32)))
shutil.rmtree(os.path.join(T, "mr"))

# ---------------- old-format disk state ----------------
INST = build().instances
INF, EV, DUP, GONE = "infer/base/piece_0", "eval/base/piece_0", "infer/base/piece_0_dup", "old/gone"
NINF, NEV = "infer/t0_f0", "eval/t0_f0"
SRC = "D101_alpha/V_save/0"  # training (recipe toy), name unchanged
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
VIEW = os.path.dirname(INST[NINF].outputs[0]).replace("preds/", "results/")  # infer's input view (relative to the data dir)
B1 = b"ckpt"
H1 = hashlib.sha256(B1).hexdigest()
B2 = b"blob of lab1"
H2 = hashlib.sha256(B2).hexdigest()

# queue
write(os.path.join(TQ, Q["tasks"]), jl([
    dict(order=1, nice=0, key=SRC, exp="t0", recipe="toy", params=dict(TOY["params"], machines=["lab0", "lab1"], oldlogs=dict(dir="x"))),
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
write(os.path.join(TQ, Q["released"], sf(GONE)), "1700000002.0 lab1\n")  # record of a dropped task: deleted too
write(os.path.join(TQ, Q["claims"], sf(DUP), proto.CLAIM_INFO), json.dumps(dict(worker="lab1")))
SM = os.path.join(TQ, proto.SMOKE_DIR)
write(os.path.join(SM, "lab0.json"), json.dumps({"k1": dict(t=100, ok=True), "k2": dict(t=300, ok=True)}))
write(os.path.join(SM, "lab1.json"), json.dumps({"k1": dict(t=200, ok=False)}))
write(os.path.join(SM, "site-slurm.json"), json.dumps({"k3": dict(t=5, ok=True)}))
write(os.path.join(SM, "lab0.json.lock"), "")

# relay ledger
REC_INF = dict(recipe="command", params=P_INF, data=["raw/in"], staged=1.0, views=[VIEW], inputs=[], outs=[INST[NINF].outputs[0]],
               cleanup=[], unq=7)
REC_EV = dict(recipe="command", params=P_EV, data=[], staged=2.0, views=[], inputs=[], outs=[INST[NEV].outputs[0]], cleanup=[], unq=2)
REC_SRC = dict(recipe="toy", params=dict(TOY["spec"]["params"], oldlogs=dict(dir="x")), data=[], staged=3.0, views=["prepared/D101_alpha"], inputs=[],
               outs=[TOY["spec"]["out"]], cleanup=[], links={"prepared/D101_alpha/x.dat": "ab" * 32})
write(os.path.join(STATE, "relay", "state.json"), json.dumps(dict(
    keys={"lab0": {INF: REC_INF}, "lab1": {GONE: dict(recipe="command", params=P_GONE, views=[], unq=1)},
          "dicc": {EV: REC_EV}, "site-slurm": {SRC: REC_SRC}},
    views={"lab0": {VIEW: 10}, "lab1": {"v/lab1": 20}, "dicc": {"v/dicc": 30}, "site-slurm": {"prepared/D101_alpha": 40}},
    last_ok={"lab0": 100, "lab1": 200, "dicc": 300, "site-slurm": 50},
    bundle_for={"site-slurm": "abc"}, held={GONE: 1.0})))

# unused and kept entries in the state dir
JUNK = [write(os.path.join(STATE, "shift.json"), "{}"), write(os.path.join(STATE, "relay", "state.json.bak"), "{}"),
        write(os.path.join(STATE, "relay", "push_lab0.log"), "x"), os.path.dirname(write(os.path.join(STATE, "relay", "old", "a"), "x")),
        write(os.path.join(STATE, "health", "status.json"), "{}"), write(os.path.join(STATE, "health", "probs.json.migrated"), "{}"),
        write(os.path.join(STATE, "last", "a"), "x"), write(os.path.join(STATE, "last", "b"), "x")]
KEEP = [write(os.path.join(STATE, "tasks.json"), "{}"), write(os.path.join(STATE, "health", "problems.json"), "{}"),
        write(os.path.join(STATE, "relay", "bundles", "x", "f"), "x")]
OLD_HLOG = write(os.path.join(STATE, "health", "health.log"), "2026-11-01 00:00:00 an old line\n")

# hub database (version 1, with a sha256 cache)
os.makedirs(STATE, exist_ok=True)
import db as DB  # noqa: E402
con = sqlite3.connect(os.environ["TASKQ_DB"])
con.executescript(DB.SCHEMA[0])
con.executemany("INSERT INTO hash(k, sha) VALUES(?, ?)", [("/d/a|1|2", "s1"), ("/d/b|3|4", "s2")])
con.execute("PRAGMA user_version=1")
con.commit()
con.close()

# site-pueue's root (old name lab0)
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
write(os.path.join(L1, f"{sf(NINF)}__j2_part1.log"), "new")  # target exists already: do not overwrite
write(os.path.join(L1, f"{sf(SRC)}__j_seg2.log"), "b")
write(os.path.join(L1, f"{sf(INF)}__x__j_part1.log"),
      "作业 j_1 @ node1，任务=other/key，第 1 段，环境 base，代码 abc1234，2026-11-01 10:00:00\n"
      "2026-11-01 10:00:01 开始\n训完 2026-11-01 11:00:00\n")  # job name contains __: another task's
write(os.path.join(R1, proto.blob(H1)), "ckpt")
write(os.path.join(D1, VIEW, "dataset.json"), "{}")
os.makedirs(os.path.join(D1, VIEW, "frag_0"))
os.symlink(os.path.join(R1, proto.blob(H1)), os.path.join(D1, VIEW, "frag_0", "final.ckpt"))
os.symlink("/etc/hostname", os.path.join(D1, VIEW, "plans.json"))  # not pointing at blobs: not counted as a link
# old name lab1's root (on the same machine, merged into site-pueue)
write(os.path.join(OLD1, proto.ROOT["queue"], Q["tasks"]), "")
write(os.path.join(LO, f"{sf(EV)}__lab1-gpu_seg1.log"), "d")
write(os.path.join(LO, f"{sf(NINF)}__j2_part1.log"), "lab1")  # site-pueue already has the same name: keep it
write(os.path.join(OLD1, proto.ROOT["data"], proto.WORKER_LOGS, "w_9.out"), "e")
write(os.path.join(OLD1, proto.blob(H2)), B2.decode())
write(os.path.join(OLD1, proto.blob(H2) + proto.BLOB_STAT), "12 1")
write(os.path.join(OLD1, proto.blob(H1)), "ckpt")  # site-pueue already has it
# site-slurm's root (old name dicc's records are here too)
write(os.path.join(Q2, Q["tasks"]), jl([dict(order=1, nice=0, key=SRC, exp="t0", recipe="toy", params=TOY["params"], spec=TOY["spec"]),
                                        dict(order=2, nice=-100, key=EV, exp=None, recipe="command", params=P_EV, spec=sp_ev)]))
write(os.path.join(Q2, Q["skips"], sf(SRC)), "2026-11-01 10:00:00\tsite-slurm-gpu\t放不下\n2026-11-01 10:00:00\tdicc-gpu\t放不下\n")
write(os.path.join(L2, f"{sf(EV)}__dicc_seg1.log"), "f")
# store (the hub entry): root pin records, part logs fetched into the data dir
write(os.path.join(ROOTS["hub"], proto.ROOT["pins"], sf(INF) + ".json"), "{}")
write(os.path.join(DATA, proto.TASK_LOGS, f"{sf(EV)}__dicc_seg1.log"), "g")
write(os.path.join(ROOTS["hub"], proto.ROOT["data"], proto.TASK_LOGS, f"{sf(INF)}__x_seg3.log"), "h")


def tree():
    """Everything under the temp dir (no symlink following, skip __pycache__): path -> kind and content. The new bundle and
    the machine name file are left out (pushed beside the old bundle also when only listing; workers do not see them)."""
    out = {}
    for dp, dns, fns in os.walk(T):
        dns[:] = [d for d in dns if d != "__pycache__" and not (d == NEWB and os.path.basename(dp) == proto.ROOT["code"])]
        for n in dns + fns:
            p = os.path.join(dp, n)
            if n == proto.ROOT["machine"] and os.path.realpath(dp) in {os.path.realpath(r) for r in ROOTS.values()}:
                continue
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

# ---------------- 2. bin/taskq self migrate Stops (touch no machine) ----------------
t0 = tree()
rc, out = taskq_cli("--help")
ok("bin/taskq self migrate --help", rc == 0 and "--apply" in out)
rc, out = taskq_cli("--apply")
ok(f"a name in the ledger not in the machine table, no --rename: non-zero exit, lists lab0, lab1, dicc ({out.strip()[-200:]})",
   rc != 0 and all(n in out for n in ("lab0", "lab1", "dicc")) and "--rename" in out)
rc, out = taskq_cli("--apply", "--rename", "lab0=site-pueue", "lab1=site-pueue")
ok("only one missing (dicc): list only it", rc != 0 and "dicc" in out and "lab0" not in out.split("taskq self migrate:")[-1])
rc, out = taskq_cli("--apply", "--rename", "lab0=site-pueue", "lab1=nosuch", "dicc=site-slurm")
ok("--rename to a name not in the machine table: also stops", rc != 0 and "lab1" in out)
rc, out = taskq_cli("--apply", *REN, "--merge", f"hub={OLD1}")
ok(f"--merge points at a non-executor (hub): non-zero exit ({out.strip()[-120:]})", rc != 0 and "--merge" in out and "hub" in out)
rc, out = taskq_cli("--apply", *REN, "--merge", "lab1")
ok("--merge written wrong (no =): non-zero exit", rc != 0 and "old=new" in out)
ok("none of these changed anything on disk", diff(t0, tree()) == [])

# ---------------- 3. default only lists ----------------
rc, out, err = run(*REN, *MERGE)
d = diff(t0, tree())
ok(f"without --apply: exit code 0, not one byte on disk changes ({d[:5]} {err.strip()[-300:]})", rc == 0 and d == [])
ok("lists: dropped old tasks and why (no match, same instance as another)",
   f"dropped {GONE}: no step in the experiment table has this output" in out and f"dropped {DUP}: same step instance {NINF} as another task" in out)
ok("lists: what each machine does (renamed part logs, existing target kept, merged root, dropped skip records, dropped pin records)",
   "part logs" in out and "kept" in out and f"{OLD1}/{proto.ROOT['data']}/{proto.TASK_LOGS} -> {R1}" in out
   and "skip records of workers that no longer exist" in out and "drop pin record" in out)
ok("lists: smoke records, state-dir drops, sha256 cache, and says nothing was changed",
   "Smoke records: ['lab0', 'lab1', 'site-slurm'] -> ['site-pueue', 'site-slurm']" in out
   and all(f"drop {p}" in out for p in JUNK) and not any(f"drop {p}" in out for p in KEEP)
   and "2 sha256 cache rows" in out and "listed only, nothing changed" in out)

# ---------------- 4. Stop: a machine has something that cannot be handled automatically ----------------
claim = write(os.path.join(Q2, Q["claims"], sf(EV), proto.CLAIM_INFO), json.dumps(dict(worker="site-slurm-gpu")))
t1 = tree()
rc, out, err = run("--apply", *REN, *MERGE)
d = diff(t1, tree())
ok(f"an executor (Slurm) queue has a claimed task that would be renamed: exit 1, says which machine and task, disk unchanged ({d[:5]} {err.strip()[-200:]})",
   rc == 1 and "Slurm" in err and "would be renamed or dropped" in err and EV in err and d == [])
os.remove(claim)
os.rmdir(os.path.dirname(claim))
os.rmdir(os.path.dirname(os.path.dirname(claim)))
lab1_q = os.path.join(OLD1, proto.ROOT["queue"], Q["tasks"])
write(lab1_q, jl([dict(order=1, nice=0, key=GONE, exp=None, recipe="command", params=P_GONE)]))
t1 = tree()
rc, out, err = run("--apply", *REN, *MERGE)
d = diff(t1, tree())
ok(f"the --merge old root queue still has tasks: exit 1, disk unchanged ({d[:5]} {err.strip()[-200:]})",
   rc == 1 and f"{OLD1} still has 1 task(s) in its queue" in err and d == [])
write(lab1_q, "")
unreach = write(os.path.join(T, "unreachable_site-slurm2"), "")
t1 = tree()
rc, out, err = run("--apply", *REN, *MERGE)
d = diff(t1, tree())
ok(f"the new bundle cannot be pushed to one machine: exit 1, no traceback, says which, disk unchanged ({d[:5]} {err.strip()[-200:]})",
   rc == 1 and "Traceback" not in err and "cannot push bundle" in err and "site-slurm2" in err and "nothing has been changed yet" in err and d == [])
os.remove(unreach)
ok("after all the Stop cases: disk is back to how it was", diff(t0, tree()) == [])

# ---------------- 5. --apply ----------------
# a training claimed by a worker of the old bundle (name unchanged) keeps running: claim, task and its open part log stay
src_claim = write(os.path.join(Q2, Q["claims"], sf(SRC), proto.CLAIM_INFO), json.dumps(dict(worker="1234", token="tk")))
src_log = write(os.path.join(L2, f"{sf(SRC)}__1234_part1.log"), old_head("1234", 1, SRC) + "\n")
rc, out, err = run("--apply", *REN, *MERGE)
ok(f"--apply: exit code 0 ({(err or out).strip()[-600:]})", rc == 0 and "migration complete" in out)
ok("finally prints the command to delete the merged old root (the old root itself is kept)", f"rm -rf {OLD1}" in out and os.path.isdir(OLD1))

# queue
hub = {t["key"]: t for t in tasks_of(os.path.join(TQ, Q["tasks"]))}
ok(f"queue: old tasks renamed to step instance names, unmatched and duplicate ones dropped ({sorted(hub)})", sorted(hub) == sorted([SRC, NINF, NEV]))
ok("parameters the recipe no longer has (oldlogs) removed from the hub queue and the relay ledger",
   "oldlogs" not in hub[SRC]["params"] and hub[SRC]["params"].get("machines")
   and "oldlogs" not in json.load(open(os.path.join(STATE, "relay", "state.json")))["keys"]["site-slurm"][SRC]["params"])
ok("renamed tasks: parameters from advance.queue_params, no experiment",
   hub[NINF]["params"] == A.queue_params(INST[NINF]) and hub[NEV]["params"] == A.queue_params(INST[NEV])
   and hub[NINF]["exp"] is None and hub[NINF]["recipe"] == "command")
ok("added: missing filled with 0, existing unchanged", hub[NINF]["added"] == 0 and hub[SRC]["added"] == 0 and hub[NEV]["added"] == 1234.5)
ok("params.machines renamed per --rename (merged into one)", hub[SRC]["params"]["machines"] == ["site-pueue"])
ok("every renamed task resolves", all(E.resolve_task(t["recipe"], t["params"], t.get("exp")) for t in hub.values()))
ci = json.load(open(os.path.join(TQ, Q["claims"], sf(NINF), proto.CLAIM_INFO)))
ok("claims: dir by new task name, machine names in worker / host / assigned updated",
   not os.path.exists(os.path.join(TQ, Q["claims"], sf(INF)))
   and ci == dict(worker="site-pueue", host="site-pueue", since=1.0, beat=2.0, assigned="site-pueue"))
ok("releases: new task name, machine renamed, two-field lines get reason back, three-field reasons unchanged",
   read(os.path.join(TQ, Q["released"], sf(NEV))) == f"1700000000.0 site-pueue {proto.RELEASE_WHY['back']}\n"
   and read(os.path.join(TQ, Q["released"], sf(SRC))) == f"1700000001.0 site-slurm {proto.RELEASE_WHY['failed']}\n"
   and not os.path.exists(os.path.join(TQ, Q["released"], sf(EV))))
ok("failure records: new task name, machine name in field 2 (job <machine>) updated",
   read(os.path.join(TQ, Q["failures"], sf(NINF))) == "2026-11-01 10:00:00\tjob site-pueue\tboom\tbefore-migrate\n")
ok("skip records: machine names updated",
   read(os.path.join(TQ, Q["skips"], sf(SRC))) == "2026-11-01 10:00:00\tsite-pueue\t放不下\n2026-11-01 11:00:00\tsite-slurm\t放不下\n")
ok("missing-input and stopped records: new task name",
   os.path.exists(os.path.join(TQ, Q["inputs"], sf(NINF))) and os.path.exists(os.path.join(TQ, Q["stopped"], sf(NEV)))
   and not os.path.exists(os.path.join(TQ, Q["inputs"], sf(INF))) and not os.path.exists(os.path.join(TQ, Q["stopped"], sf(EV))))

# smoke
ok("smoke records: old names merged into new ones, newest t per kind; old files and .lock deleted",
   sorted(os.listdir(SM)) == ["site-pueue.json", "site-slurm.json"]
   and json.load(open(os.path.join(SM, "site-pueue.json"))) == {"k1": dict(t=200, ok=False), "k2": dict(t=300, ok=True)}
   and json.load(open(os.path.join(SM, "site-slurm.json"))) == {"k3": dict(t=5, ok=True)})

# relay ledger
st = json.load(open(os.path.join(STATE, "relay", "state.json")))
ks = st.get("keys") or {}
ok(f"relay ledger: machines renamed, old names merged, records of the merged old root (lab1) dropped ({ {n: sorted(r) for n, r in ks.items()} })",
   {n: sorted(r) for n, r in ks.items()} == {"site-pueue": [NINF], "site-slurm": sorted([SRC, NEV])})
ri, re_, rs = ks["site-pueue"][NINF], ks["site-slurm"][NEV], ks["site-slurm"][SRC]
ok("relay ledger: renamed task parameters replaced, unq removed, other fields unchanged",
   ri["params"] == A.queue_params(INST[NINF]) and re_["params"] == A.queue_params(INST[NEV])
   and not any("unq" in r for r in (ri, re_, rs)) and ri["views"] == [VIEW] and ri["outs"] == REC_INF["outs"])
ok("relay ledger: old data becomes inputs, old inputs (files placed there) becomes placed; task parameter data renamed too (any recipe)",
   ri["inputs"] == ["raw/in"] and ri["placed"] == [] and not any("data" in r for r in (ri, re_, rs))
   and MG.renamed_params(dict(cmd="c", data=["x"])) == dict(cmd="c", inputs=["x"])
   and MG.renamed_params(dict(dataset="d", data=["y"])) == dict(dataset="d", inputs=["y"]) and MG.renamed_params(dict(a=1)) == dict(a=1))
ok(f"relay ledger: links filled in (symlinks in views pointing at this machine's blobs -> sha256; links elsewhere and regular files not counted), existing ones unchanged ({ri.get('links')})",
   ri.get("links") == {f"{VIEW}/frag_0/final.ckpt": H1} and re_.get("links") == {} and rs["links"] == REC_SRC["links"])
ok("relay ledger: views merged (merged old roots dropped), last_ok max, bundle_for removed",
   st.get("views") == {"site-pueue": {VIEW: 10}, "site-slurm": {"v/dicc": 30, "prepared/D101_alpha": 40}}
   and st.get("last_ok") == {"site-pueue": 200, "site-slurm": 300} and "bundle_for" not in st and "held" not in st)
ok("dropped tasks: claim, release and skip records removed on hub and executors",
   not any(os.path.exists(os.path.join(TQ, Q[d], sf(k))) for d in ("claims", "released") for k in (DUP, GONE))
   and not os.path.exists(os.path.join(Q1, Q["skips"], sf(GONE))))

# executor queues
q1 = {t["key"]: t for t in tasks_of(os.path.join(Q1, Q["tasks"]))}
q2 = {t["key"]: t for t in tasks_of(os.path.join(Q2, Q["tasks"]))}
ok(f"executor queues: tasks renamed, spec key follows, added filled in ({sorted(q1)} {sorted(q2)})",
   sorted(q1) == [NINF] and sorted(q2) == sorted([SRC, NEV]) and q1[NINF]["spec"]["key"] == NINF
   and q2[NEV]["spec"]["key"] == NEV and all(t.get("added") is not None for t in list(q1.values()) + list(q2.values())))
ok(f"executor queues: replacing the spec does not recompute added (old logs still count as half-done; {[t.get('added') for t in q1.values()]})",
   q1[NINF]["added"] == 0 and q2[NEV]["added"] == 0)
ok("executor queues: the hub sent new specs (resolved with the current recipes: with step, relative output)",
   q1[NINF]["spec"].get("step") == "infer" and q1[NINF]["spec"]["out"] == INST[NINF].outputs[0]
   and q2[NEV]["spec"].get("step") == "eval")
ok("executor queues: release records by new task name, reason added; failure records by new task name",
   read(os.path.join(Q1, Q["released"], sf(NINF))) == f"1700000000.0 site-pueue-gpu1 {proto.RELEASE_WHY['back']}\n"
   and os.path.exists(os.path.join(Q1, Q["failures"], sf(NINF))) and not os.path.exists(os.path.join(Q1, Q["failures"], sf(INF))))
ok("executor queues: skip records keep only workers that still exist, files with none are deleted",
   read(os.path.join(Q1, Q["skips"], sf(NINF))) == "2026-11-01 10:00:00\tsite-pueue-gpu1\t放不下\n"
   and not os.path.exists(os.path.join(Q1, Q["skips"], sf(GONE)))
   and read(os.path.join(Q2, Q["skips"], sf(SRC))) == "2026-11-01 10:00:00\tsite-slurm-gpu\t放不下\n")

ok("a claimed training whose name stays: claim untouched, still in the queue, its open part log left as it is",
   read(src_claim) == json.dumps(dict(worker="1234", token="tk")) and SRC in q2 and read(src_log) == old_head("1234", 1, SRC) + "\n"
   and "1 claimed task(s) keep running" in out)
ok("executors switched to the new bundle, the machine name recorded in each root, no held tasks left",
   all(os.readlink(os.path.join(ROOTS[n], proto.ROOT["current"])) == NEWB
       and read(os.path.join(ROOTS[n], proto.ROOT["machine"])) == n + "\n"
       and not os.path.exists(os.path.join(ROOTS[n], proto.ROOT["queue"], Q["held"])) for n in ("site-pueue", "site-slurm", "site-slurm2")))
# part logs, merged roots, pin records
ok(f"site-pueue part logs: renamed, _seg -> _part; job names with __ unchanged; existing targets not overwritten, both kept ({sorted(os.listdir(L1))})",
   sorted(os.listdir(L1)) == sorted([f"{sf(NINF)}__lab0-gpu_part1.log", f"{sf(INF)}__j2_part1.log", f"{sf(NINF)}__j2_part1.log",
                                     f"{sf(SRC)}__j_part2.log", f"{sf(INF)}__x__j_part1.log", f"{sf(NEV)}__lab1-gpu_part1.log"])
   and read(os.path.join(L1, f"{sf(NINF)}__j2_part1.log")) == "new" and read(os.path.join(L1, f"{sf(INF)}__j2_part1.log")) == "old")
ok("old part logs on an executor are rewritten to English",
   read(os.path.join(L1, f"{sf(INF)}__x__j_part1.log"))
   == "job j_1 @ node1, task=other/key, part 1, env base, code abc1234, 2026-11-01 10:00:00\n"
      "2026-11-01 10:00:01 开始\nDONE 2026-11-01 11:00:00\n")
ok("merged root: part logs and worker logs moved from the old root; a same-name file site-pueue already has stays in the old root",
   read(os.path.join(L1, f"{sf(NEV)}__lab1-gpu_part1.log")) == "d"
   and read(os.path.join(D1, proto.WORKER_LOGS, "w_9.out")) == "e"
   and os.listdir(LO) == [f"{sf(NINF)}__j2_part1.log"] and read(os.path.join(LO, f"{sf(NINF)}__j2_part1.log")) == "lab1")
ok("merged root: blobs on the same disk (including .st) hardlinked, existing ones unchanged",
   os.path.samefile(os.path.join(OLD1, proto.blob(H2)), os.path.join(R1, proto.blob(H2)))
   and os.path.samefile(os.path.join(OLD1, proto.blob(H2) + proto.BLOB_STAT), os.path.join(R1, proto.blob(H2) + proto.BLOB_STAT))
   and not os.path.samefile(os.path.join(OLD1, proto.blob(H1)), os.path.join(R1, proto.blob(H1))))
ok("site-slurm, the store data dir, the store root's data dir: part logs renamed",
   sorted(os.listdir(L2)) == sorted([f"{sf(NEV)}__dicc_part1.log", os.path.basename(src_log)])
   and os.listdir(os.path.join(DATA, proto.TASK_LOGS)) == [f"{sf(NEV)}__dicc_part1.log"]
   and os.listdir(os.path.join(ROOTS["hub"], proto.ROOT["data"], proto.TASK_LOGS)) == [f"{sf(NINF)}__x_part3.log"])
ok("pin records: dropped for renamed tasks (executors and store), others kept",
   os.listdir(os.path.join(R1, proto.ROOT["pins"])) == ["other.json"]
   and os.listdir(os.path.join(ROOTS["hub"], proto.ROOT["pins"])) == [])

# state dir, database
ok("state dir: unused entries dropped, needed ones kept",
   not any(os.path.lexists(p) for p in JUNK) and all(os.path.exists(p) for p in KEEP)
   and os.path.isdir(os.path.join(STATE, "last")) and os.listdir(os.path.join(STATE, "last")) == [])
ok("health check plain-text log: the one in the state dir's health/ is appended to cfg.log's health.log",
   not os.path.exists(OLD_HLOG) and "an old line" in open(cfg.log("health")).read())
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
ok(f"sha256 cache moved to {proto.ROOT['hashes']} under the store root, hub database migrated to the current version (no hash table) ({ver} {rows})",
   rows == [("/d/a|1|2", "s1"), ("/d/b|3|4", "s2")] and ver == DB.VERSION and not has_hash)

# ---------------- 6. run again ----------------
t2 = tree()
rc, out, err = run("--apply", *REN, *MERGE)
d = diff(t2, tree())
ok(f"--apply again: exit code 0, nothing on disk changes ({d[:5]} {(err or '').strip()[-300:]})", rc == 0 and d == [])
ok("run again: no renames or drops; unchanged records are not listed as kept (only the same-name file in the old root remains)",
   "0 renamed to step instance names, 0 dropped" in out and not re.search(r"renamed \d+ part logs|[1-9]\d* part logs rewritten", out)
   and [ln for ln in out.splitlines() if "kept" in ln and LO not in ln] == [])

print("all passed" if not fails else f"{fails} failures")
sys.exit(1 if fails else 0)
