#!/usr/bin/env python3
"""Tests for queue/migrate.py (taskq self migrate) and the on-disk format stamp: the testdata machine table, the hub state dir,
each machine's root and the store data dir all in temp dirs, stamped with this code's format.
  1. the format stamp helpers (common.format_*);
  2. STEPS are numbered one per format from MIN_FORMAT + 1 to proto.FORMAT; pending;
  3-7 run the framework with a fake step (FAKE_SRC: to format proto.FORMAT + 1, it appends one line to a marker file in each
     place), installed in the hub process and in the new bundle's migrate.py, since every real step is deleted once all places
     are past it:
  3. the default (and --list) only lists: not one byte changes anywhere (no bundle pushed, no lock, no database written);
  4. Stops (a root cannot be read, a root newer than this code, a root older than MIN_FORMAT, a hub step that cannot be done,
     a health check round running): exit 1, disk unchanged;
  5. --apply: snapshot of the hub queue, database and stamp; each root switched to the new bundle (claimed tasks left), the
     root step applied once, stamped; the hub step applied once, the hub stamped last;
  6. running --apply again: nothing to do, nothing on disk changes; a root left behind is migrated alone;
  7. snapshots: only the newest proto.SNAPSHOT_KEEP stay; this code itself (no fake step) finds nothing to do;
  8. the stamp binds this code: workers claim nothing, hub rounds and relay steps refuse; new roots are stamped.
The root half really runs migrate.py root from the new bundle through relay_remote.op: the bundle is hand-built (this core at
proto.BUNDLE_CORE, .taskq.toml points to the test config and plugins); the hub (the store, ssh=None) runs locally, other
machines via fake ssh locally (a marker file makes one unreachable). The hub half runs migrate.main in a process started by
this file with --run (fake step) or --run-real (the process points each machine's root, conda and the store data dir at
temp dirs).
    python3 queue/test_migrate.py
"""
import json
import os
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

DRIVER = sys.argv[1:2] in (["--run"], ["--run-real"])
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
F = proto.FORMAT  # this code's format: every place starts there
FAKE = F + 1      # the fake step's
# A step to format proto.FORMAT + 1, run inside migrate's module namespace: on the hub and in each root it appends one JSON
# line (what the framework handed it) to a marker file; a hub state dir holding fake_stop makes the hub step impossible.
FAKE_SRC = '''
def _fake_mark(p, rec, say, dry):
    say(f"fake step: append to {p}")
    if not dry:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "a") as f:
            f.write(json.dumps(rec) + "\\n")


def _fake_hub(ctx, say, dry):
    if os.path.exists(os.path.join(ctx["home"], "fake_stop")):
        raise Stop("the fake hub step cannot be done")
    _fake_mark(os.path.join(ctx["home"], "fake_step"), dict(queue=ctx["queue"]), say, dry)


def _fake_root(ctx, say, dry):
    _fake_mark(os.path.join(ctx["root"], proto.ROOT["state"], "fake_step"),
               dict(queue=ctx["queue"], busy=sorted(ctx["busy"]), data=ctx["data"]), say, dry)


MIN_FORMAT = proto.FORMAT
proto.FORMAT += 1
STEPS = [Step(proto.FORMAT, "fake step (test_migrate.py)", hub=_fake_hub, root=_fake_root)]
'''
MAIN = '\nif __name__ == "__main__":'


def install_fake(mod):
    exec(FAKE_SRC, mod.__dict__)


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


def make_bundle(root, bid, fake=False):
    """Hand-built bundle <root>/code/<bid>: this core at proto.BUNDLE_CORE, .taskq.toml points to the test config and plugins.
    fake: the core's entries linked one by one, except queue/migrate.py: a copy with FAKE_SRC before its main."""
    b = os.path.join(root, proto.ROOT["code"], bid)
    if os.path.isdir(b):
        return b
    bc = os.path.join(b, proto.BUNDLE_CORE)
    if not fake:
        os.makedirs(os.path.dirname(bc))
        os.symlink(CORE, bc)
    else:
        os.makedirs(os.path.join(bc, "queue"))
        for f in os.listdir(CORE):
            if f not in ("queue", ".git", "__pycache__"):
                os.symlink(os.path.join(CORE, f), os.path.join(bc, f))
        for f in os.listdir(HERE):
            if f not in ("migrate.py", "__pycache__"):
                os.symlink(os.path.join(HERE, f), os.path.join(bc, "queue", f))
        src = read(os.path.join(HERE, "migrate.py"))
        assert src.count(MAIN) == 1
        write(os.path.join(bc, "queue", "migrate.py"), src.replace(MAIN, "\n" + FAKE_SRC + MAIN))
    with open(os.path.join(b, ".taskq.toml"), "w", encoding="utf-8") as f:
        f.write(f"config = {json.dumps(fixture.CONFIG)}\nplugins = {json.dumps([fixture.PLUGINS])}\n")
    return b


def fake_deploy(name, bid, path, claims=None, switch=True):
    r = os.path.expanduser(M.MACHINES[name]["root"])
    make_bundle(r, bid, fake=True)
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
    if sys.argv[1] == "--run":
        install_fake(migrate)
    sys.exit(migrate.main(sys.argv[2:]))

os.environ["RELAY_SSH"] = FAKE_SSH
import common as C  # noqa: E402
import migrate as MG  # noqa: E402
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
            f'[[ -e {T}/unreachable_$1 ]] && {{ echo "ssh: connect to host $1 port 22: Connection refused" >&2; exit 255; }}\nh=$1; shift\n'
            "cd / && exec env " + " ".join(f"-u {v}" for v in proto.HUB_ONLY_ENV) + f' HOME={T}/home_$h bash -c "$*"\n')
os.chmod(FAKE_SSH, 0o755)  # each host its own home dir
for n, m in M.MACHINES.items():
    if m.get("ssh"):
        os.makedirs(os.path.join(T, "home_" + m["ssh"]), exist_ok=True)
for r in ROOTS.values():
    make_bundle(r, "oldbundle")
    os.symlink("oldbundle", os.path.join(r, proto.ROOT["current"]))
    os.makedirs(os.path.join(r, proto.ROOT["state"]))


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
ok("pending: nothing for a place in this code's format", MG.pending(proto.FORMAT) == [])
for bad, word in ((proto.FORMAT + 1, "newer"), (MG.MIN_FORMAT - 1, "older taskq"), (None, "cannot read")):
    try:
        MG.pending(bad)
        ok(f"pending({bad}) stops", False)
    except MG.Stop as e:
        ok(f"pending({bad}) stops: {e}", word in str(e))
KEEP_STEPS = (MG.STEPS, MG.MIN_FORMAT, proto.FORMAT)
install_fake(MG)
ok("pending with a step: the steps after the stamp, up to proto.FORMAT",
   [s.to for s in MG.pending(F)] == [FAKE] and MG.pending(FAKE) == [] and proto.FORMAT == FAKE)
MG.STEPS, MG.MIN_FORMAT, proto.FORMAT = KEEP_STEPS

# ---------------- disk state in this code's format ----------------
sf = proto.safe
Q = proto.QUEUE
TQ = os.path.join(STATE, "taskq")
R1, R2, R3 = ROOTS["site-pueue"], ROOTS["site-slurm"], ROOTS["site-slurm2"]
Q1, Q2 = os.path.join(R1, proto.ROOT["queue"]), os.path.join(R2, proto.ROOT["queue"])
A, CK, BUSY = "infer/t0_f0", "D101_alpha/V_save/0", "D101_alpha/V_save/1"
HUBQ = write(os.path.join(TQ, Q["tasks"]), jl([dict(order=1, nice=0, key=CK, exp=None, recipe="command", params=dict(cmd="x"), added=0)]))
# executors: an unclaimed task and a claimed one (its worker keeps it)
write(os.path.join(Q1, Q["tasks"]), jl([dict(order=1, nice=0, key=A, exp=None, recipe="command", params=dict(cmd="a"), added=0),
                                        dict(order=2, nice=0, key=BUSY, exp=None, recipe="command", params=dict(cmd="c"), added=0)]))
BUSY_CLAIM = write(os.path.join(Q1, Q["claims"], sf(BUSY), proto.CLAIM_INFO), json.dumps(dict(worker="1234", token="tk")))
write(os.path.join(Q2, Q["tasks"]), "")
# hub database (only the snapshot reads it)
con = sqlite3.connect(os.environ["TASKQ_DB"])
for i in range(3):
    con.executescript(DB.SCHEMA[i])
con.execute("INSERT INTO meta(k, v) VALUES('mark', '\"before\"')")
con.execute("PRAGMA user_version=3")
con.commit()
con.close()
ROOT_STAMP = {n: os.path.join(r, proto.ROOT["format"]) for n, r in ROOTS.items()}
HUB_STAMP = os.path.join(STATE, proto.FORMAT_FILE)
for p in [HUB_STAMP, *ROOT_STAMP.values()]:
    write(p, f"{F}\n")
HUB_MARK = os.path.join(STATE, "fake_step")
ROOT_MARK = {n: os.path.join(r, proto.ROOT["state"], "fake_step") for n, r in ROOTS.items()}
LABEL = {n: M.MACHINES[n]["label"] for n in ROOTS}


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


def run(*args, real=False):
    r = subprocess.run([sys.executable, os.path.abspath(__file__), "--run-real" if real else "--run", *args],
                       capture_output=True, text=True, timeout=900)
    return r.returncode, r.stdout, r.stderr


def stamp(p):
    return C.format_on_disk(p)


def marks(p):
    """The marker file's records, one per time the fake step ran there; None when it never ran."""
    return [json.loads(x) for x in read(p).splitlines()] if os.path.exists(p) else None


# ---------------- 3. listing changes nothing ----------------
rc, out = taskq_cli("--help")
ok("bin/taskq self migrate --help", rc == 0 and "--apply" in out and "--list" in out)
t0 = tree()
rc, out, err = run()
d = diff(t0, tree())
ok(f"default (list): exit code 0, not one byte changes anywhere: no bundle pushed, no lock, no database ({d[:5]} {err.strip()[-300:]})",
   rc == 0 and d == [] and not os.path.exists(os.path.join(R1, proto.ROOT["code"], NEWB)))
ok(f"lists: this code's format, the hub in format {F} with the step and what its hub part would change ({out[:1500]})",
   f"this taskq: on-disk format {FAKE} (migrates from format {F} on)" in out and f"hub {STATE}: format {F}\n" in out
   and f"step {FAKE}: fake step" in out and f"fake step: append to {HUB_MARK}" in out)
ok("lists: each root's format and the step it needs; says nothing changed",
   all(f"{LABEL[n]}: format {F}\n" in out for n in ROOTS) and "its root part, after switching to the new bundle" in out
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
write(ROOT_STAMP["site-slurm2"], f"{FAKE + 1}\n")
t1 = tree()
rc, out, err = run("--apply")
d = diff(t1, tree())
ok(f"--apply with a root newer than this code: exit 1, says update taskq, nothing changed ({d[:5]} {err.strip()[-200:]})",
   rc == 1 and "newer than this taskq" in err and d == [])
os.remove(ROOT_STAMP["site-slurm2"])
t1 = tree()
rc, out, err = run("--apply")
d = diff(t1, tree())
ok(f"--apply with a root older than MIN_FORMAT (no stamp: format 0): exit 1, says use an older taskq, nothing changed ({d[:5]} {err.strip()[-200:]})",
   rc == 1 and f"from format {F} on" in err and "older taskq" in err and d == [])
write(ROOT_STAMP["site-slurm2"], f"{F}\n")
STOP = write(os.path.join(STATE, "fake_stop"), "")
t1 = tree()
rc, out, err = run("--apply")
d = diff(t1, tree())
ok(f"--apply when a hub step cannot be done: exit 1 before anything changes, roots untouched ({d[:5]} {err.strip()[-200:]})",
   rc == 1 and f"hub step {FAKE}: the fake hub step cannot be done" in err and d == [])
os.remove(STOP)
LOCK = MG.round_lock()  # a health check round holds its lock
t1 = tree()
rc, out, err = run("--apply")
d = diff(t1, tree())
LOCK.close()
ok(f"--apply while a health check round runs: exit 1, says so, nothing changed ({d[:5]} {err.strip()[-200:]})",
   rc == 1 and "a health check round is running" in err and d == [])
LOCKFILE = LOCK.name
t0[os.path.dirname(LOCKFILE)], t0[LOCKFILE] = ("dir",), ("file", b"")
ok("after the Stop cases: disk is back to how it was (but for the round lock file)", diff(t0, tree()) == [])

# ---------------- 5. --apply ----------------
before_hubq = read(HUBQ)
with open(os.environ["TASKQ_DB"], "rb") as f:
    before_db = f.read()
rc, out, err = run("--apply")
ok(f"--apply: exit code 0 ({(err or out).strip()[-800:]})", rc == 0 and "migration complete" in out)
ok("every place stamped with the step's format: the hub and every root",
   stamp(HUB_STAMP) == FAKE and all(stamp(p) == FAKE for p in ROOT_STAMP.values()) and read(HUB_STAMP) == f"{FAKE}\n")
SNAPS = os.path.join(STATE, proto.SNAPSHOTS)
snaps = sorted(os.listdir(SNAPS)) if os.path.isdir(SNAPS) else []
snap_ok = False
if len(snaps) == 1:
    sd = os.path.join(SNAPS, snaps[0])
    sc = sqlite3.connect(os.path.join(sd, proto.DB))
    snap_ok = (snaps[0].endswith(f"-format{F}") and read(os.path.join(sd, "taskq", Q["tasks"])) == before_hubq
               and sc.execute("PRAGMA user_version").fetchone()[0] == 3
               and sc.execute("SELECT v FROM meta WHERE k='mark'").fetchone() == ('"before"',)
               and read(os.path.join(sd, proto.FORMAT_FILE)) == f"{F}\n")
    sc.close()
ok(f"a snapshot before applying: the hub queue, the database and the stamp as they were ({snaps})", snap_ok)
with open(os.environ["TASKQ_DB"], "rb") as f:
    ok("the hub queue and database themselves: the step did not touch them, nor did the run", read(HUBQ) == before_hubq and f.read() == before_db)
ok(f"the hub step ran once, on the hub queue ({marks(HUB_MARK)})", marks(HUB_MARK) == [dict(queue=TQ)])
ok("executors switched to the new bundle, the machine name recorded, no held tasks left",
   all(os.readlink(os.path.join(ROOTS[n], proto.ROOT["current"])) == NEWB
       and read(os.path.join(ROOTS[n], proto.ROOT["machine"])) == n + "\n"
       and not os.path.exists(os.path.join(ROOTS[n], proto.ROOT["queue"], Q["held"])) for n in EXECS))
RM1 = marks(ROOT_MARK["site-pueue"])
ok(f"the root step ran once in each executor, with its queue dir, its claimed tasks and its data dir ({RM1})",
   RM1 == [dict(queue=Q1, busy=[sf(BUSY)], data=[os.path.join(R1, proto.ROOT["data"])])]
   and all(marks(ROOT_MARK[n]) == [dict(queue=os.path.join(ROOTS[n], proto.ROOT["queue"]), busy=[],
                                        data=[os.path.join(ROOTS[n], proto.ROOT["data"])])] for n in ("site-slurm", "site-slurm2")))
ok("a claimed task: claim and queue entry left as they are, listed; the unclaimed one back in the queue",
   read(BUSY_CLAIM) == json.dumps(dict(worker="1234", token="tk")) and BUSY in read(os.path.join(Q1, Q["tasks"]))
   and A in read(os.path.join(Q1, Q["tasks"])) and "1 claimed task(s) keep running" in out)
ok(f"the store root (not an executor): not switched here (relay does it), the step ran once with the store's data dir, stamped "
   f"({marks(ROOT_MARK['hub'])})",
   os.readlink(os.path.join(ROOTS["hub"], proto.ROOT["current"])) == "oldbundle" and stamp(ROOT_STAMP["hub"]) == FAKE
   and marks(ROOT_MARK["hub"]) == [dict(queue=None, busy=[], data=[os.path.join(ROOTS["hub"], proto.ROOT["data"]), DATA])])
ok(f"each root reports its format change ({out[out.find('== '):][:400]})",
   all(f"== {LABEL[n]}: format {F} -> {FAKE}" in out for n in ROOTS) and f"hub: format {FAKE}" in out)

# ---------------- 6. run again; a root left behind ----------------
t2 = tree()
rc, out, err = run("--apply")
d = diff(t2, tree())
ok(f"--apply again: exit code 0, nothing to do, nothing on disk changes (no snapshot either) ({d[:5]} {(err or '').strip()[-300:]})",
   rc == 0 and "nothing to do" in out and d == [])
write(ROOT_STAMP["site-slurm2"], f"{F}\n")  # say it was unreachable at the cutover and came back behind
rc, out, err = run()
ok("listing: only that root still needs the step", rc == 0 and f"Slurm2: format {F}\n" in out and f"hub {STATE}: format {FAKE}, nothing to do" in out
   and f"pueue: format {FAKE}, nothing to do" in out)
t3 = tree()
rc, out, err = run("--apply")
d = diff(t3, tree())
ok(f"--apply: only that root migrated (and a new snapshot), nothing else changes ({d})",
   rc == 0 and stamp(ROOT_STAMP["site-slurm2"]) == FAKE and len(marks(ROOT_MARK["site-slurm2"])) == 2
   and all(p in (ROOT_STAMP["site-slurm2"], ROOT_MARK["site-slurm2"]) or p.startswith(SNAPS) for p in d) and len(os.listdir(SNAPS)) == 2)

# ---------------- 7. snapshots kept; this code itself has nothing to do ----------------
made = []
for _ in range(proto.SNAPSHOT_KEEP + 1):  # the ones above carry the higher label format<FAKE>, likely from the same second
    made.append(os.path.basename(MG.snapshot(STATE, proto.FORMAT, lambda _: None)))
    time.sleep(0.01)
left = sorted(os.listdir(SNAPS))
ok(f"snapshots: only the newest {proto.SNAPSHOT_KEEP} stay, by time not by label, no half-written one ({left})",
   left == sorted(made[-proto.SNAPSHOT_KEEP:]) and not any(x.endswith(proto.TMP) for x in left))
for p in [HUB_STAMP, *ROOT_STAMP.values()]:
    write(p, f"{F}\n")
t4 = tree()
rc, out, err = run(real=True)
ok(f"this code (no fake step), every place in its format: the listing says nothing to do everywhere ({out.strip()[-500:]} {err.strip()[-300:]})",
   rc == 0 and f"hub {STATE}: format {F}, nothing to do" in out and all(f"{LABEL[n]}: format {F}, nothing to do" in out for n in ROOTS)
   and "step " not in out)
rc, out, err = run("--apply", real=True)
d = diff(t4, tree())
ok(f"... and --apply: exit 0, nothing to do, nothing on disk changes ({d[:5]} {out.strip()[-200:]} {err.strip()[-300:]})",
   rc == 0 and f"everything is in format {F} already; nothing to do" in out and d == [])

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
