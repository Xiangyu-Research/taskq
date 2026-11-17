#!/usr/bin/env python3
"""Tests for queue/migrate.py (taskq self migrate) and the on-disk format stamp: the testdata machine table, the hub state dir,
each machine's root and the store data dir all in temp dirs, built in format 0 (no stamps, the state before them).
  1. log_rename, the English rewrite of old part logs and failure records, the format stamp helpers (common.format_*);
  2. STEPS are numbered one per format from MIN_FORMAT + 1 to proto.FORMAT;
  3. the default (and --list) only lists: not one byte changes anywhere (no bundle pushed, no lock, no database written);
  4. Stops (a root cannot be read, a root newer than this code, a hub step that cannot be done, both a ledger file and a
     ledger in the database): exit 1, disk unchanged;
  5. --apply: snapshot of the hub queue and database; each root switched to the new bundle, its part logs and failure records
     in the new format (claimed tasks' logs left), stamped; the hub bookkeeping in the database, smoke kinds, unused state-dir
     entries dropped; the hub stamped last;
  6. running --apply again: nothing to do, nothing on disk changes; a root left behind (stamp gone) is migrated alone;
  7. snapshots: only the newest proto.SNAPSHOT_KEEP stay.
The root half really runs migrate.py root from the new bundle through relay_remote.op: the bundle is hand-built (this core at
proto.BUNDLE_CORE, .taskq.toml points to the test config and plugins); the hub (the store, ssh=None) runs locally, other
machines via fake ssh locally (a marker file makes one unreachable). The hub half runs migrate.main in a process started by
this file with --run (the process points each machine's root, conda and the store data dir at temp dirs).
    python3 queue/test_migrate.py
"""
import json
import os
import re
import sqlite3
import subprocess
import sys
import time

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
EXECS = ("site-pueue", "site-slurm", "site-slurm2")
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
import common as C  # noqa: E402
import migrate as MG  # noqa: E402
import hubstate as HS  # noqa: E402
import relay_store as ST  # noqa: E402
import db as DB  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what, flush=True)
    fails += 0 if cond else 1


# ---------------- environment: conda, fake ssh, each machine's bundle ----------------
os.makedirs(os.path.join(CONDA, "bin"))
os.symlink(sys.executable, os.path.join(CONDA, "bin", "python3"))
with open(FAKE_SSH, "w") as f:  # skip -o options; -O check (persistent connection) counts as reachable; a host with a marker file is unreachable; drop the host name, run locally, without hub-only variables
    f.write("#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\n[[ $1 == -O ]] && exit 0\n"
            f'[[ -e {T}/unreachable_$1 ]] && {{ echo "ssh: connect to host $1 port 22: Connection refused" >&2; exit 255; }}\nshift\n'
            "cd / && exec env " + " ".join(f"-u {v}" for v in proto.HUB_ONLY_ENV) + ' bash -c "$*"\n')
os.chmod(FAKE_SSH, 0o755)
for r in ROOTS.values():
    make_bundle(r, "oldbundle")
    os.symlink("oldbundle", os.path.join(r, proto.ROOT["current"]))
    os.makedirs(os.path.join(r, proto.ROOT["state"]))


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


# ---------------- 1. log_rename, English rewrite, format stamp helpers ----------------
ok("log_rename: _seg<part> becomes _part<part> (multi-digit part too)",
   MG.log_rename("infer__base__piece_0__job1_seg1.log") == "infer__base__piece_0__job1_part1.log"
   and MG.log_rename("D101_alpha__V_save__0__w_seg12.log") == "D101_alpha__V_save__0__w_part12.log")
ok("log_rename: already a new name -> None", MG.log_rename("infer__t0_f0__job1_part1.log") is None)
ok("log_rename: _seg not followed by a part number and .log: unchanged",
   MG.log_rename("a__job_segx.log") is None and MG.log_rename("a__job_seg1.txt") is None)
ok("log_rename: a job name that already has _seg only gets the last one changed", MG.log_rename("a__j_seg_seg2.log") == "a__j_seg_part2.log")

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
   not any(re.search(r"[一-鿿]", read(os.path.join(ED, proto.TASK_LOGS, f))) for f in os.listdir(os.path.join(ED, proto.TASK_LOGS))))
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
BUSY_OLD = write(os.path.join(ED, proto.TASK_LOGS, f"{proto.safe('busy/old')}__j_part1.log"),
                 old_head("j", 1, "busy/old") + "2026-11-01 10:03:00 命令结束（退出码 1），没训完（第 1 次）\n")
BUSY_NEW = write(os.path.join(ED, proto.TASK_LOGS, f"{proto.safe('busy/new')}__j_part1.log"), read(HALF))
said = []
MG.english_logs(ED, said.append, False, busy=[proto.safe("busy/old"), proto.safe("busy/new")])
OLD_TEXT = read(BUSY_OLD)
said_new = []
MG.english_logs(ED, said_new.append, False, busy=[proto.safe("busy/new")])
ok(f"english_logs: logs of claimed tasks are not touched; only those still in the old format are listed ({said})",
   len(said) == 1 and "1 logs of claimed tasks still in the old format" in said[0] and os.path.basename(BUSY_OLD) in said[0]
   and "命令结束" in OLD_TEXT
   and said_new == [f"{os.path.join(ED, proto.TASK_LOGS)}: 1 part logs rewritten to English"] and "命令结束" not in read(BUSY_OLD))

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

# the format stamp helpers (the check on: TASKQ_FORMAT_ANY=0)
os.environ["TASKQ_FORMAT_ANY"] = "0"
FP = os.path.join(T, "fmt", proto.FORMAT_FILE)
why = C.format_problem(FP)
ok(f"format stamp: none -> format 0, the check says run taskq self migrate ({why})",
   C.format_on_disk(FP) == 0 and "no stamp" in why and "taskq self migrate" in why and not os.path.exists(FP))
ok("format stamp: fresh (nothing from an older taskq) -> stamped with this code's format, fine",
   C.format_problem(FP, fresh=True) is None and read(FP) == f"{proto.FORMAT}\n")
write(FP, f"{proto.FORMAT + 1}\n")
why = C.format_problem(FP, fresh=True)
ok(f"format stamp: newer than the code -> says update taskq; fresh never overwrites a stamp ({why})",
   "newer than this taskq" in why and read(FP) == f"{proto.FORMAT + 1}\n")
write(FP, "x\n")
ok("format stamp: unreadable -> None, the check says so", C.format_on_disk(FP) is None and "cannot read" in C.format_problem(FP))
os.environ["TASKQ_FORMAT_ANY"] = "1"
ok("format stamp: TASKQ_FORMAT_ANY=1 (tests) turns the check off", C.format_problem(FP) is None)
os.environ["TASKQ_FORMAT_ANY"] = "0"
os.environ["TASKQ_ROOT"] = "/r/x"
ok("format stamp: in a root it is <root>/state/format (proto.ROOT format)", C.format_path() == "/r/x/" + proto.ROOT["format"])
del os.environ["TASKQ_ROOT"]
ok("format stamp: on the hub it is <state dir>/format", C.format_path() == os.path.join(STATE, proto.FORMAT_FILE))
os.environ["TASKQ_FORMAT_ANY"] = "1"

# ---------------- 2. the steps ----------------
ok(f"STEPS: one per format from MIN_FORMAT + 1 to proto.FORMAT ({[s.to for s in MG.STEPS]}), each with something to do",
   [s.to for s in MG.STEPS] == list(range(MG.MIN_FORMAT + 1, proto.FORMAT + 1)) and all(s.hub or s.root for s in MG.STEPS))
ok("pending: the steps after the stamp, up to proto.FORMAT", [s.to for s in MG.pending(proto.FORMAT - 1)] == [proto.FORMAT]
   and [s.to for s in MG.pending(MG.MIN_FORMAT)] == [s.to for s in MG.STEPS] and MG.pending(proto.FORMAT) == [])
for bad, word in ((proto.FORMAT + 1, "newer"), (MG.MIN_FORMAT - 1, "older taskq"), (None, "cannot read")):
    try:
        MG.pending(bad)
        ok(f"pending({bad}) stops", False)
    except MG.Stop as e:
        ok(f"pending({bad}) stops: {e}", word in str(e))

# ---------------- old snapshot dir ----------------
JUNK_SNAP = os.path.dirname(write(os.path.join(STATE, MG.OLD_SNAPSHOTS, ".lock"), ""))  # old snapshot dir with no snapshot left
ok("old snapshot dir holding only files: no snapshots to delete; the dir itself is unused",
   MG.old_snapshots(STATE) == ([], []) and JUNK_SNAP in MG.leftovers(STATE))
SNAP = os.path.join(T, "snap")
os.makedirs(os.path.join(SNAP, MG.OLD_SNAPSHOTS, "abc"))
write(os.path.join(SNAP, MG.OLD_SNAPSHOTS, ".lock"), "")
ok("... while a snapshot dir is left, it is not unused", os.path.join(SNAP, MG.OLD_SNAPSHOTS) not in MG.leftovers(SNAP))

# ---------------- step 1: both a ledger file and a ledger in the database ----------------
BOTH = os.path.join(T, "both")
write(os.path.join(BOTH, MG.OLD_RELAY_STATE), json.dumps(dict(keys={"site-pueue": {"k": dict(linkset="x")}})))
KEEP_DB = os.environ["TASKQ_DB"]
os.environ["TASKQ_DB"] = os.path.join(BOTH, proto.DB)
HS.update_relay(lambda s: s["keys"].setdefault("site-pueue", {}).update(k2=dict(linkset="y")))
said = []
try:
    MG.v1_hub(dict(home=BOTH, queue=os.path.join(BOTH, "taskq")), said.append, True)
    stopped = None
except MG.Stop as e:
    stopped = str(e)
os.environ["TASKQ_DB"] = KEEP_DB
ok(f"step 1: relay/state.json while the database already holds a ledger -> Stop (no guess which is right) ({stopped})",
   stopped and "both" in stopped and "relay/state.json" in stopped)

# ---------------- format-0 disk state ----------------
sf = proto.safe
Q = proto.QUEUE
TQ = os.path.join(STATE, "taskq")
R1, R2, R3 = ROOTS["site-pueue"], ROOTS["site-slurm"], ROOTS["site-slurm2"]
Q1, Q2 = os.path.join(R1, proto.ROOT["queue"]), os.path.join(R2, proto.ROOT["queue"])
D1, D2 = os.path.join(R1, proto.ROOT["data"]), os.path.join(R2, proto.ROOT["data"])
L1, L2 = os.path.join(D1, proto.TASK_LOGS), os.path.join(D2, proto.TASK_LOGS)
A, B, CK, BUSY = "infer/t0_f0", "eval/t0_f0", "D101_alpha/V_save/0", "D101_alpha/V_save/1"

# hub queue (step 1 leaves it as it is)
HUBQ = write(os.path.join(TQ, Q["tasks"]), jl([dict(order=1, nice=0, key=CK, exp=None, recipe="command", params=dict(cmd="x"), added=0)]))
SM = os.path.join(TQ, proto.SMOKE_DIR)
write(os.path.join(SM, "site-pueue.json"), json.dumps({"k1": dict(t=100, ok=True, kind="k1"), "r|env=e|a=1|env=e": dict(t=50, ok=True),
                                                        "r|env=e|a=1": dict(t=60, ok=False)}))
SM_SLURM = write(os.path.join(SM, "site-slurm.json"), json.dumps({"k3": dict(t=5, ok=True, kind="k3")}))  # already current: not rewritten
# the hub's bookkeeping files from before the database held them (read in, then deleted)
A_LINKS = {"results/x/frag_0/final.ckpt": "ab" * 32}
EV_LINKS = {"x/y.dat": "cd" * 32}
REC_A = dict(recipe="command", params=dict(cmd="a"), inputs=["raw/in"], staged=1.0, views=["results/x"], placed=[], outs=["preds/a"],
             cleanup=[], links=A_LINKS)  # from before link sets: carries its links
REC_B = dict(recipe="command", params=dict(cmd="b"), inputs=[], staged=2.0, views=[], placed=[], outs=["preds/b"], cleanup=[], linkset="e0")
REC_CK = dict(recipe="toy", params=dict(ds="D"), inputs=[], staged=3.0, views=[], placed=[], outs=["ck"], cleanup=[])  # neither
LEDGER = dict(keys={"site-pueue": {A: REC_A}, "site-slurm": {B: REC_B, CK: REC_CK}}, views={"site-pueue": {"results/x": 10}},
              last_ok={"site-pueue": 100, "site-slurm": 50}, held={B: 1.0})
write(os.path.join(STATE, MG.OLD_RELAY_STATE), json.dumps(LEDGER))
OLD_LS = os.path.dirname(write(os.path.join(STATE, MG.OLD_LINKSETS, "e0.json"), json.dumps(EV_LINKS)))
write(os.path.join(OLD_LS, "dead.json"), json.dumps({"a": "ef" * 32}))  # named by no record: not kept
OLD_SITES = {"site-pueue": dict(time=5.0, label="L1", tasks={}, seglogs=[], bundle="b1")}
OLD_RECS = [dict(machine="hub", id=4, label="fetch:x", added=1.0, step="fetch", key=CK)]
OLD_HUB = [os.path.join(STATE, MG.OLD_RELAY_STATE), OLD_LS, write(os.path.join(STATE, MG.OLD_SITES), json.dumps(OLD_SITES)),
           write(os.path.join(STATE, MG.OLD_RECORDS), json.dumps(OLD_RECS)), write(os.path.join(STATE, "tasks.json.lock"), ""),
           write(os.path.join(STATE, "relay", "state.json.lock"), "")]
# unused and kept entries in the state dir
JUNK = [write(os.path.join(STATE, "shift.json"), "{}"), write(os.path.join(STATE, "relay", "state.json.bak"), "{}"),
        write(os.path.join(STATE, "relay", "push_lab0.log"), "x"), os.path.dirname(write(os.path.join(STATE, "relay", "old", "a"), "x")),
        write(os.path.join(STATE, "health", "status.json"), "{}"), write(os.path.join(STATE, "relay", "names.json.lock"), ""),
        write(os.path.join(STATE, "relay", "stage_gone.lock"), ""), os.path.dirname(write(os.path.join(STATE, "health", "notices", "a"), "x")),
        JUNK_SNAP]
KEEP = [write(os.path.join(STATE, "health", "pueue_seen.json"), "{}"),
        write(os.path.join(STATE, "relay", f"stage_{next(iter(cfg.mtab().MACHINES))}.lock"), ""),
        write(os.path.join(STATE, "relay", "bundles", "x", "f"), "x"), write(os.path.join(STATE, "last", "a.json"), "[]")]
# hub database in schema 3 (before the bookkeeping tables)
con = sqlite3.connect(os.environ["TASKQ_DB"])
for i in range(3):
    con.executescript(DB.SCHEMA[i])
con.execute("INSERT INTO meta(k, v) VALUES('mark', '\"before\"')")
con.execute("PRAGMA user_version=3")
con.commit()
con.close()

# executors: part logs and failure records an old bundle's worker wrote; a claimed task whose old log is still being written
SEG = write(os.path.join(L1, f"{sf(A)}__w1_seg1.log"), old_head("w1", 1, A) + "训完 2026-11-01 11:00:00\n")
write(os.path.join(L1, f"{sf(B)}__w1_seg2.log"), "old")
KEPT_NEW = write(os.path.join(L1, f"{sf(B)}__w1_part2.log"), "new")  # target exists already: not overwritten
OTHER = write(os.path.join(L1, f"{sf(A)}__x__j_part1.log"), old_head("j", 1, "other/key") + "2026-11-01 10:00:01 开始\n")
write(os.path.join(Q1, Q["tasks"]), jl([dict(order=1, nice=0, key=A, exp=None, recipe="command", params=dict(cmd="a"), added=0),
                                        dict(order=2, nice=0, key=BUSY, exp=None, recipe="command", params=dict(cmd="c"), added=0)]))
FAIL1 = write(os.path.join(Q1, Q["failures"], sf(A)), "2026-11-01 10:00:00\t作业 w1\tboom\toldbundle\n")
BUSY_CLAIM = write(os.path.join(Q1, Q["claims"], sf(BUSY), proto.CLAIM_INFO), json.dumps(dict(worker="1234", token="tk")))
BUSY_LOG = write(os.path.join(L1, f"{sf(BUSY)}__1234_seg1.log"), old_head("1234", 1, BUSY))
write(os.path.join(Q2, Q["tasks"]), "")
write(os.path.join(L2, f"{sf(B)}__dicc_seg1.log"), "f")
# the store (the hub entry): part logs fetched into the store data dir and in its root's data dir
write(os.path.join(DATA, proto.TASK_LOGS, f"{sf(B)}__dicc_seg1.log"), "g")
write(os.path.join(ROOTS["hub"], proto.ROOT["data"], proto.TASK_LOGS, f"{sf(A)}__x_seg3.log"), "h")


def tree():
    """Everything under the temp dir (no symlink following, skip __pycache__): path -> kind and content."""
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


def stamp(p):
    return C.format_on_disk(p)


ROOT_STAMP = {n: os.path.join(r, proto.ROOT["format"]) for n, r in ROOTS.items()}
HUB_STAMP = os.path.join(STATE, proto.FORMAT_FILE)

# ---------------- 3. listing changes nothing ----------------
rc, out = taskq_cli("--help")
ok("bin/taskq self migrate --help", rc == 0 and "--apply" in out and "--list" in out)
t0 = tree()
rc, out, err = run()
d = diff(t0, tree())
ok(f"default (list): exit code 0, not one byte changes anywhere: no bundle pushed, no lock, no database ({d[:5]} {err.strip()[-300:]})",
   rc == 0 and d == [] and not os.path.exists(os.path.join(R1, proto.ROOT["code"], NEWB)))
ok(f"lists: this code's format, the hub in format 0 with step 1 and what it would change ({out[:1500]})",
   f"this taskq: on-disk format {proto.FORMAT}" in out and f"hub {STATE}: format 0 (no stamp)" in out and "step 1:" in out
   and "relay ledger (3 records) and 3 link sets: from relay/state.json" in out and "on 1 executors: from relay/status.json" in out
   and "1 task records: from tasks.json" in out and "smoke records site-pueue.json: kind names without a repeated part, 3 -> 2 kinds" in out
   and "site-slurm.json" not in out
   and all(f"drop {p}" in out for p in JUNK + OLD_HUB) and not any(f"drop {p}" in out for p in KEEP))
ok("lists: each root's format and the step it needs; says nothing changed",
   all(f"{M.MACHINES[n]['label']}: format 0 (no stamp)" in out for n in ROOTS) and "its root part, after switching to the new bundle" in out
   and "listed only, nothing changed" in out)
rc, out2, err = run("--list")
ok("--list: the same listing", rc == 0 and out2 == out and diff(t0, tree()) == [])
rc, out2 = taskq_cli("--list", "--apply")
ok("--list and --apply together: usage error", rc == 2)

# ---------------- 4. Stops: nothing changes ----------------
unreach = write(os.path.join(T, "unreachable_fake-site-slurm2"), "")
t1 = tree()
rc, out, err = run()
ok(f"a root that cannot be read: the listing says so and that it cannot apply yet ({out.strip()[-300:]})",
   rc == 0 and "Slurm2: cannot read its root" in out and "cannot apply yet" in out)
rc, out, err = run("--apply")
d = diff(t1, tree())
ok(f"--apply with a root that cannot be read: exit 1, no traceback, says which, nothing changed ({d[:5]} {err.strip()[-300:]})",
   rc == 1 and "Traceback" not in err and "Slurm2" in err and "nothing changed" in err and d == [])
os.remove(unreach)
write(ROOT_STAMP["site-slurm2"], f"{proto.FORMAT + 1}\n")
t1 = tree()
rc, out, err = run("--apply")
d = diff(t1, tree())
ok(f"--apply with a root newer than this code: exit 1, says update taskq, nothing changed ({d[:5]} {err.strip()[-200:]})",
   rc == 1 and "newer than this taskq" in err and d == [])
os.remove(ROOT_STAMP["site-slurm2"])
bad_sites = os.path.join(STATE, MG.OLD_SITES)
good = read(bad_sites)
write(bad_sites, "{not json")
t1 = tree()
rc, out, err = run("--apply")
d = diff(t1, tree())
ok(f"--apply when a hub step cannot be done (unreadable old file): exit 1 before anything changes, roots untouched ({d[:5]} {err.strip()[-200:]})",
   rc == 1 and "hub step 1" in err and MG.OLD_SITES in err and d == [])
write(bad_sites, good)
ok("after the Stop cases: disk is back to how it was", diff(t0, tree()) == [])

# ---------------- 5. --apply ----------------
before_hubq = read(HUBQ)
rc, out, err = run("--apply")
ok(f"--apply: exit code 0 ({(err or out).strip()[-800:]})", rc == 0 and "migration complete" in out)
ok("every place stamped with this code's format: the hub and every root",
   stamp(HUB_STAMP) == proto.FORMAT and all(stamp(p) == proto.FORMAT for p in ROOT_STAMP.values())
   and read(HUB_STAMP) == f"{proto.FORMAT}\n")
SNAPS = os.path.join(STATE, proto.SNAPSHOTS)
snaps = sorted(os.listdir(SNAPS)) if os.path.isdir(SNAPS) else []
snap_ok = False
if len(snaps) == 1:
    sd = os.path.join(SNAPS, snaps[0])
    sc = sqlite3.connect(os.path.join(sd, proto.DB))
    snap_ok = (snaps[0].endswith("-format0") and read(os.path.join(sd, "taskq", Q["tasks"])) == before_hubq
               and os.path.exists(os.path.join(sd, "taskq", proto.SMOKE_DIR, "site-pueue.json"))
               and sc.execute("PRAGMA user_version").fetchone()[0] == 3
               and sc.execute("SELECT v FROM meta WHERE k='mark'").fetchone() == ('"before"',)
               and not os.path.exists(os.path.join(sd, proto.FORMAT_FILE)))
    sc.close()
ok(f"a snapshot before applying: the hub queue as it was, the database as it was (schema 3), no stamp then ({snaps})", snap_ok)
ok("the hub queue itself: step 1 leaves it as it was", read(HUBQ) == before_hubq)
ok("executors switched to the new bundle, the machine name recorded, no held tasks left",
   all(os.readlink(os.path.join(ROOTS[n], proto.ROOT["current"])) == NEWB
       and read(os.path.join(ROOTS[n], proto.ROOT["machine"])) == n + "\n"
       and not os.path.exists(os.path.join(ROOTS[n], proto.ROOT["queue"], Q["held"])) for n in EXECS))
ok(f"executor part logs: _seg -> _part and English; an existing target not overwritten (both kept) ({sorted(os.listdir(L1))})",
   sorted(os.listdir(L1)) == sorted([f"{sf(A)}__w1_part1.log", f"{sf(B)}__w1_seg2.log", f"{sf(B)}__w1_part2.log",
                                     f"{sf(A)}__x__j_part1.log", os.path.basename(BUSY_LOG)])
   and read(os.path.join(L1, f"{sf(A)}__w1_part1.log")) == proto.LOG_HEAD_FMT % ("w1_1", "node1", A, 1, "base", "abc1234",
                                                                                   "2026-11-01 10:00:00") + "\nDONE 2026-11-01 11:00:00\n"
   and read(KEPT_NEW) == "new" and "kept" in out
   and read(OTHER) == "job j_1 @ node1, task=other/key, part 1, env base, code abc1234, 2026-11-01 10:00:00\n2026-11-01 10:00:01 开始\n")
ok("a claimed task: claim, queue entry and its open old log left as they are (listed)",
   read(BUSY_CLAIM) == json.dumps(dict(worker="1234", token="tk")) and read(BUSY_LOG) == old_head("1234", 1, BUSY)
   and BUSY in read(os.path.join(Q1, Q["tasks"])) and "1 claimed task(s) keep running" in out and "still in the old format" in out)
ok("executor failure records: the old job field in English", read(FAIL1) == "2026-11-01 10:00:00\tjob w1\tboom\toldbundle\n")
ok("site-slurm, the store data dir, the store root's data dir: part logs renamed",
   os.listdir(L2) == [f"{sf(B)}__dicc_part1.log"] and os.listdir(os.path.join(DATA, proto.TASK_LOGS)) == [f"{sf(B)}__dicc_part1.log"]
   and os.listdir(os.path.join(ROOTS["hub"], proto.ROOT["data"], proto.TASK_LOGS)) == [f"{sf(A)}__x_part3.log"])
ok("the store root (not an executor): not switched here (relay does it), stamped",
   os.readlink(os.path.join(ROOTS["hub"], proto.ROOT["current"])) == "oldbundle" and stamp(ROOT_STAMP["hub"]) == proto.FORMAT)
st = HS.relay()
ks = st.get("keys") or {}
ra, rb, rck = ks.get("site-pueue", {}).get(A, {}), ks.get("site-slurm", {}).get(B, {}), ks.get("site-slurm", {}).get(CK, {})
ok(f"relay ledger in the database: every record and field as in the file; links carried by a record became its link set ({st})",
   {n: sorted(r) for n, r in ks.items()} == {"site-pueue": [A], "site-slurm": sorted([B, CK])}
   and {k: v for k, v in ra.items() if k != "linkset"} == {k: v for k, v in REC_A.items() if k != "links"}
   and ST.links_of(ra) == A_LINKS and rb == REC_B and ST.links_of(rb) == EV_LINKS and ST.links_of(rck) == {} and "linkset" in rck
   and st.get("views") == LEDGER["views"] and st.get("last_ok") == LEDGER["last_ok"] and st.get("held") == LEDGER["held"])
con = sqlite3.connect(os.environ["TASKQ_DB"])
LSIDS = sorted(i for (i,) in con.execute("SELECT id FROM linkset"))
ver = con.execute("PRAGMA user_version").fetchone()[0]
con.close()
ok("link sets: those records name kept (the dead one dropped); the database migrated to the current schema",
   LSIDS == sorted({ra.get("linkset"), rb.get("linkset"), rck.get("linkset")}) and "dead" not in LSIDS and ver == DB.VERSION)
ok("what relay last read and the task records read into the database; the old files are gone",
   HS.sites() == OLD_SITES and HS.records() == OLD_RECS and not any(os.path.lexists(p) for p in OLD_HUB))
ok("smoke records: kind names without a repeated part, the newer record kept; a file already current not rewritten",
   json.load(open(os.path.join(SM, "site-pueue.json"))) == {"k1": dict(t=100, ok=True, kind="k1"), "r|env=e|a=1": dict(t=60, ok=False, kind="r|env=e|a=1")}
   and read(SM_SLURM) == json.dumps({"k3": dict(t=5, ok=True, kind="k3")}))
ok("state dir: unused entries dropped; live ones kept (stamp, snapshots, last/ records, health round lock among them)",
   not any(os.path.lexists(p) for p in JUNK) and all(os.path.exists(p) for p in KEEP) and os.path.isdir(SNAPS))

# ---------------- 6. run again; a root left behind ----------------
t2 = tree()
rc, out, err = run("--apply")
d = diff(t2, tree())
ok(f"--apply again: exit code 0, nothing to do, nothing on disk changes (no snapshot either) ({d[:5]} {(err or '').strip()[-300:]})",
   rc == 0 and "nothing to do" in out and d == [])
os.remove(ROOT_STAMP["site-slurm2"])  # say it was unreachable at the cutover and came back without a stamp
rc, out, err = run()
ok("listing: only that root still needs step 1", rc == 0 and "Slurm2: format 0 (no stamp)" in out and f"hub {STATE}: format {proto.FORMAT}, nothing to do" in out
   and "pueue: format 1, nothing to do" in out)
t3 = tree()
rc, out, err = run("--apply")
d = diff(t3, tree())
ok(f"--apply: only that root migrated (and a new snapshot), nothing else changes ({d})",
   rc == 0 and stamp(ROOT_STAMP["site-slurm2"]) == proto.FORMAT
   and all(p == ROOT_STAMP["site-slurm2"] or p.startswith(SNAPS) for p in d) and len(os.listdir(SNAPS)) == 2)

# ---------------- 7. snapshots kept ----------------
for _ in range(proto.SNAPSHOT_KEEP + 1):
    MG.snapshot(STATE, proto.FORMAT, lambda _: None)
    time.sleep(0.01)
left = sorted(os.listdir(SNAPS))
ok(f"snapshots: only the newest {proto.SNAPSHOT_KEEP} stay, no half-written one ({left})",
   len(left) == proto.SNAPSHOT_KEEP and not any(x.endswith(proto.TMP) for x in left))

# ---------------- 8. the stamp binds this code: workers claim nothing, hub rounds and relay steps refuse; new roots stamped ----------------
ENVX = {k: v for k, v in os.environ.items() if k not in ("TASKQ_DIR", "TASKQ_STATE", "TASKQ_DATA", "TASKQ_ROOT", "TASKQ_MACHINE")}
ENVX["TASKQ_FORMAT_ANY"] = "0"
WR = os.path.join(T, "wroot")  # an executor root as a worker of this code sees it
os.makedirs(os.path.join(WR, proto.ROOT["data"], proto.TASK_LOGS))
os.makedirs(os.path.join(WR, proto.ROOT["state"]))
write(os.path.join(WR, proto.ROOT["queue"], Q["tasks"]),
      jl([dict(order=1, nice=0, key="D9/V/0", exp=None, recipe="toy", params=dict(ds="D9", variant="V", frag="0"))]))


def worker_next():
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "w1", "--envs", "base,alt,alt2"],
                       env=dict(ENVX, TASKQ_ROOT=WR, TASKQ_MACHINE="site-pueue"), capture_output=True, text=True, timeout=120)
    return r.returncode, r.stdout + r.stderr


rc, o = worker_next()
ok(f"worker next of this code on a root without a stamp: exit 12 (paused, an exit code it already has), says run taskq self "
   f"migrate, claims nothing ({rc} {o.strip()[-200:]})",
   rc == proto.EXIT["paused"] and "taskq self migrate" in o and not os.path.exists(os.path.join(WR, proto.ROOT["queue"], Q["claims"], sf("D9/V/0"))))
write(os.path.join(WR, proto.ROOT["format"]), f"{proto.FORMAT + 1}\n")
rc, o = worker_next()
ok(f"... on a root in a newer format: also paused, says update taskq ({o.strip()[-120:]})", rc == proto.EXIT["paused"] and "newer" in o)
write(os.path.join(WR, proto.ROOT["format"]), f"{proto.FORMAT}\n")
rc, o = worker_next()
ok(f"... stamped with this code's format: claims ({rc} {o.strip()[-200:]})", rc == 0 and o.startswith("D9/V/0\n"))

OH = os.path.join(T, "oldhub")  # a hub state dir from before stamps, with its database
OHDB = os.path.join(OH, proto.DB)
os.makedirs(OH)
con = sqlite3.connect(OHDB)
con.executescript(DB.SCHEMA[0])
con.execute("PRAGMA user_version=1")
con.commit()
con.close()
with open(OHDB, "rb") as f:
    OHDB_BYTES = f.read()
r = subprocess.run([os.path.join(CORE, "bin", "taskq"), "tick"], env=dict(ENVX, TASKQ_STATE=OH, TASKQ_DB=OHDB),
                   capture_output=True, text=True, timeout=300)
with open(OHDB, "rb") as f:
    same = f.read() == OHDB_BYTES
ok(f"taskq tick on a hub without a stamp: exit 1, says run taskq self migrate, the database untouched (its schema not migrated), "
   f"no queue made, no stamp written ({r.returncode} {r.stderr.strip()[-300:]})",
   r.returncode == 1 and "Health check not run" in r.stderr and "taskq self migrate" in r.stderr and same
   and not os.path.exists(os.path.join(OH, "taskq")) and not os.path.exists(os.path.join(OH, proto.FORMAT_FILE)))
r = subprocess.run([sys.executable, os.path.join(HERE, "relay.py"), "stage", "site-pueue", "D9/V/0", "--nice", "0"],
                   env=dict(ENVX, TASKQ_STATE=OH, TASKQ_DB=OHDB), capture_output=True, text=True, timeout=300)
ok(f"relay stage (a step a hub round queues) on that hub: exit 1, not run ({r.returncode} {r.stderr.strip()[-200:]})",
   r.returncode == 1 and "not run" in r.stderr and "taskq self migrate" in r.stderr)

import relay as RLY  # noqa: E402
BSRC = os.path.dirname(write(os.path.join(T, "bsrc", "x.txt"), "x"))
FRESH = os.path.join(T, "roots", "fresh")
saved = M.MACHINES["site-pueue"]["root"]
M.MACHINES["site-pueue"]["root"] = FRESH
try:
    RLY.deploy("site-pueue", "bfresh", BSRC)
    first = C.format_on_disk(os.path.join(FRESH, proto.ROOT["format"]))
    os.remove(os.path.join(FRESH, proto.ROOT["format"]))
    RLY.deploy("site-pueue", "bfresh2", BSRC)
    again = os.path.exists(os.path.join(FRESH, proto.ROOT["format"]))
finally:
    M.MACHINES["site-pueue"]["root"] = saved
ok(f"relay deploy making a new root stamps it with this code's format; an existing root is left to self migrate ({first} {again})",
   first == proto.FORMAT and not again)

print("all passed" if not fails else f"{fails} failures")
sys.exit(1 if fails else 0)
