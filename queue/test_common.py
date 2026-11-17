#!/usr/bin/env python3
"""Tests for common.py: reading/writing task records, recovering task ids, cleanup; pueue times. Temp files, no machines.
    python3 queue/test_common.py
"""
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata"))
import fixture  # noqa: E402
T = fixture.tmpdir()
os.environ["TASKQ_DB"] = os.path.join(T, "taskq.db")  # task records live in the hub database
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def task(i, label, created="2026-10-25T10:00:05.123456+08:00"):
    return dict(id=i, label=label, status="Running", command="x", created_at=created)


ADDED = C.pueue_time("2026-10-25T10:00:00+08:00")

# pueue times
ok("pueue time: 0, 3, 6 and 9 fractional digits are all read",
   C.pueue_time("2026-10-25T10:00:00+08:00") == ADDED
   and abs(C.pueue_time("2026-10-25T10:00:00.123+08:00") - ADDED - 0.123) < 1e-6
   and abs(C.pueue_time("2026-10-25T10:00:00.123456+08:00") - ADDED - 0.123456) < 1e-6
   and abs(C.pueue_time("2026-10-25T10:00:00.123456789+08:00") - ADDED - 0.123456) < 1e-6)
ok("pueue time: missing / unreadable -> None", C.pueue_time(None) is None and C.pueue_time("") is None and C.pueue_time("yesterday") is None)

# recover task ids
r0 = dict(machine="hub", id=None, label="eval:t3_f1", added=ADDED, step="eval", key="k1")
pq = {"hub": dict(tasks=[task(5, "eval:t3_f1", "2026-10-25T09:00:00+08:00"), task(6, "eval:t3_f1")])}
got = C.sync_records(pq, [r0], now=ADDED + 10)
ok("no task id: recovered by same machine, same label, created no earlier than the record (minus clock skew), earliest one",
   [r["id"] for r in got] == [6])
pq2 = {"hub": dict(tasks=[task(6, "eval:t3_f1"), task(7, "eval:t3_f1")])}
got = C.sync_records(pq2, [dict(r0, id=6), r0], now=ADDED + 10)
ok("a task id already taken by another record is not assigned again", sorted(r["id"] for r in got) == [6, 7])
got = C.sync_records({"hub": dict(tasks=[])}, [r0], now=ADDED + 10)
ok("no task id and not found yet: kept for an hour", got == [r0])
got = C.sync_records({"hub": dict(tasks=[])}, [r0], now=ADDED + C.LOST + 1)
ok("no task id and still not found after an hour: dropped", got == [])
got = C.sync_records({"pueue-host": dict(tasks=[])}, [dict(r0, machine="hub")], now=ADDED + C.LOST + 1)
ok("machine not read this time (absent from the snapshot): record untouched", len(got) == 1)

# task is gone
r1 = dict(machine="hub", id=8, label="oof:t3", added=ADDED, step="oof", key="t3")
ok("task still there (id and label match): kept", C.sync_records({"hub": dict(tasks=[task(8, "oof:t3")])}, [r1]) == [r1])
ok("task deleted: record deleted", C.sync_records({"hub": dict(tasks=[])}, [r1]) == [])
ok("task id reused by another task (different label): record deleted", C.sync_records({"hub": dict(tasks=[task(8, "other")])}, [r1]) == [])
ok("pueue-host unreadable (error): its records are not deleted",
   C.sync_records({"pueue-host": {"error": "ssh"}}, [dict(r1, machine="pueue-host")]) == [dict(r1, machine="pueue-host")])

# writing records (hub database)
ok("no database yet: no records, nothing created", C.load_records() == [] and not os.path.exists(os.environ["TASKQ_DB"]))
a = C.new_record("pueue-host", "infer:t16_f3", "infer", "k3", group="gpu1")
C.set_record_id(a, 30)
b = C.new_record("pueue-host", "infer:t16_f3", "infer", "k3", group="gpu1")
recs = C.load_records()
ok("a new record has no task id; set_record_id fills it in", [r["id"] for r in recs] == [30, None] and recs[1]["added"] == b["added"])
C.new_record("hub", "eval:t3_f1", "eval", "k1", src=None)
ok("fields with value None are not written", "src" not in C.load_records()[-1])
C.drop_records(lambda r: r["machine"] == "pueue-host")
ok("drop_records: delete by predicate", [r["machine"] for r in C.load_records()] == ["hub"])

def boom(recs):
    recs.append(dict(machine="hub", id=9))
    raise RuntimeError("x")


try:
    C.update_records(boom)
except RuntimeError:
    pass
ok("a writer that fails changes nothing (one transaction)", [r["machine"] for r in C.load_records()] == ["hub"])


def many(n):
    for i in range(n):
        C.new_record("hub", f"x{threading.get_ident()}_{i}", "eval", f"k{i}")


C.update_records(lambda recs: [])
ths = [threading.Thread(target=many, args=(20,)) for _ in range(4)]
[t.start() for t in ths]
[t.join() for t in ths]
ok("several processes adding at once: locked read-modify-write, none lost", len(C.load_records()) == 80)

# commands on another machine are time-limited there too: after this side times out, no stuck process is left there
_sp = __import__("subprocess")
_run = _sp.run
seen = []
_sp.run = lambda cmd, **kw: (seen.append((cmd, kw.get("timeout"))), _run(["true"]))[1]
try:
    C.run("cat ~/x | wc -l", "fake-slurm-login", timeout=120)
finally:
    _sp.run = _run
cmd, to = seen[0]
ok("run: with host -> wrapped in timeout -k there, 15 seconds earlier, command passed to bash -c unchanged", cmd[-1] == "timeout -k 10 105 bash -c 'cat ~/x | wc -l'"
   and cmd[-2] == "fake-slurm-login" and to == 120)
ok("run: local commands are not wrapped", C.run("echo hi")[1] == "hi\n")

# private_dir: node-local dirs with predictable names (shared temp dirs) are used only when they are this account's real dirs
import stat  # noqa: E402
pd = os.path.join(T, "shared", "mine")
ok("private_dir: missing -> created 0700 (with its parents)", C.private_dir(pd) and stat.S_IMODE(os.lstat(pd).st_mode) == 0o700)
os.chmod(pd, 0o775)
ok("private_dir: ours but open to others -> closed (0700)", C.private_dir(pd) and stat.S_IMODE(os.lstat(pd).st_mode) == 0o700)
os.makedirs(os.path.join(T, "target"))
os.chmod(os.path.join(T, "target"), 0o755)
os.symlink(os.path.join(T, "target"), os.path.join(T, "shared", "planted"))
open(os.path.join(T, "shared", "file"), "w").close()
ok("private_dir: a planted link or a file -> not ours (False), the link's target left as it was",
   not C.private_dir(os.path.join(T, "shared", "planted")) and not C.private_dir(os.path.join(T, "shared", "file"))
   and stat.S_IMODE(os.stat(os.path.join(T, "target")).st_mode) == 0o755)
_getuid = os.getuid
os.getuid = lambda: _getuid() + 1
try:
    other = not C.private_dir(pd)
finally:
    os.getuid = _getuid
ok("private_dir: another account's dir -> False, left as it is", other and stat.S_IMODE(os.lstat(pd).st_mode) == 0o700)

# backup_down: backup paused or unreachable this time -> reason (relay.py then does not queue archives)
fixture.use()
import cfg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sites"))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
bk = next(b for b in M.backups() if b != M.hub())
P.PAUSE = {bk: (None, "maintenance")}
ok("backup_down: backup paused (PAUSE without deadline) -> reason", C.backup_down() == f"{bk} is paused (maintenance)")
P.PAUSE = {bk: ("2000-01-01 00:00", "maintenance")}
ok("backup_down: PAUSE deadline passed -> not paused", C.backup_down() is None)
P.PAUSE = {bk: ("2999-01-01 00:00", "maintenance")}
ok("backup_down: PAUSE deadline not reached -> reason", C.backup_down() == f"{bk} is paused (maintenance)")
P.PAUSE = {}
ok("backup_down: unreachable this time -> reason; usable -> None", C.backup_down({bk: False}) == f"{bk} is unreachable this time"
   and C.backup_down({bk: True}) is None and C.backup_down() is None)

print("\nall pass" if not fails else f"\n{fails} failed")
sys.exit(1 if fails else 0)
