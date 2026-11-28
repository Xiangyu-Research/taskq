#!/usr/bin/env python3
"""The hub queue in the hub database (qstore.DbStore) against the queue dir (qstore.FileStore), and self migrate's step 3:
  1. the same taskq.py commands (add, stop, take, beat, release --failed / --skip, missing, nice, resume, unskip, input-ok, rm)
     run against a queue dir and against a database leave the same `status --json` (times aside);
  2. a half-made claim (made, info never written) is stale in both after the same time;
  3. hub_db() picks the database only on the hub once it holds the queue (meta qstore.QUEUE_META), TASKQ_QSTORE forces it;
  4. migrate.hub_to_db: the dry run changes nothing; applied, status is unchanged, the files are gone, the queue switch
     follows ENABLED, a second run does nothing; a copy that differs rolls back and leaves the files; rows already in the
     queue tables stop it.
    python3 queue/test_hubq.py"""
import json
import os
import re
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
T = fixture.tmpdir(prefix="tqhubq")
os.environ["HOME"] = os.path.join(T, "home")
os.makedirs(os.environ["HOME"])
DATA = os.path.join(T, "data")
os.makedirs(DATA)
os.environ.update(TASKQ_DATA=DATA, TASKQ_MACHINE="hub", TASKQ_LOCK_WAIT="0.05")
os.environ.pop("TASKQ_QSTORE", None)
sys.path.insert(0, HERE)
import proto  # noqa: E402
import spec as S  # noqa: E402

fails = 0


def ok(name, cond, extra=""):
    global fails
    print(("PASS:" if cond else "FAIL:") + name + ("" if cond else f" ({extra})"))
    fails += 0 if cond else 1


def env_of(where):
    """Environment of a taskq.py run on the hub queue kept in a dir (files) or a database file (db)."""
    kind, p = where
    if kind == "files":
        return dict(os.environ, TASKQ_DIR=p, TASKQ_QSTORE="files")
    return dict(os.environ, TASKQ_DIR=os.path.join(T, "unused_q"), TASKQ_QSTORE="db", TASKQ_DB=p)


def tq(where, *a):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *a], env=env_of(where), capture_output=True, text=True)
    return r.returncode, r.stdout + r.stderr


def status(where):
    rc, out = tq(where, "status", "--json")
    return S.json_in(out) if rc == 0 else out


VARIABLE = ("since", "beat", "input_since", "added", "log_age", "task_log_age", "out_age")


def norm(s):
    if not isinstance(s, dict):
        return s
    s = dict(s)
    tasks = {}
    for t in s.pop("tasks"):
        t = {k: v for k, v in t.items() if k not in VARIABLE}
        t["note"] = re.sub(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", "<time>", t.get("note") or "")
        if t.get("fail_tail"):
            t["fail_tail"] = {k: v for k, v in t["fail_tail"].items() if k != "time"}
        tasks[t["key"]] = t
    s["tasks"] = tasks
    return s


def toy(f):
    return ["add", "--nice", "100", "--recipe", "toy", "ds=D113_delta", "variant=V_save", f"frag={f}"]


def script(where):
    """The same edits on either store -> the keys (k0..k3) and each command's exit code."""
    rcs = [tq(where, *toy(f))[0] for f in range(4)]
    keys = sorted(t["key"] for t in status(where)["tasks"])
    k0, k1, k2, k3 = keys
    steps = [("stop", "--why", "paused by hand", k0),
             ("take", "--worker", "siteA", "--assign", k1),
             ("beat", "--worker", "siteA", k1),
             ("release", "--worker", "siteA", "--failed", "boom: exit 1", k1),
             ("take", "--worker", "siteB", "--assign", k2),
             ("release", "--worker", "siteB", "--skip", "no such env here", k2),
             ("take", "--worker", "siteA", "--assign", k3),
             ("missing", "--worker", "siteA", "--why", "input x not found", k3),
             ("nice", "--nice", "7", k0),
             ("resume", k0),
             ("stop", "--why", "again", k0),
             ("take", "--worker", "siteC", "--assign", k2),
             ("unskip", "--worker", "siteB", k2),
             ("input-ok", k3),
             ("take", "--worker", "siteA", "--assign", k1),
             ("rm", k3)]
    for a in steps:
        rcs.append(tq(where, *a)[0])
    return keys, rcs


# ---- 1. same commands, same status ----
FQ, DB = ("files", os.path.join(T, "q1")), ("db", os.path.join(T, "q1.db"))
kf, rf = script(FQ)
open(os.path.join(FQ[1], proto.QUEUE["enabled"]), "w").close()  # a new queue dir is off until ENABLED is made, a new database queue on
kd, rd = script(DB)
sf, sd = norm(status(FQ)), norm(status(DB))
ok("the same edits on a queue dir and on the database: same exit codes", rf == rd, f"{rf} vs {rd}")
ok("... and the same status --json (times aside)", sf == sd and len(sf["tasks"]) == 3,
   [(k, f, sf["tasks"].get(k, {}).get(f), sd["tasks"].get(k, {}).get(f)) for k in set(sf["tasks"]) | set(sd["tasks"])
    for f in set(sf["tasks"].get(k, {})) | set(sd["tasks"].get(k, {})) if sf["tasks"].get(k, {}).get(f) != sd["tasks"].get(k, {}).get(f)]
   + [(f, sf[f], sd[f]) for f in sf if f != "tasks" and sf[f] != sd.get(f)])
ok("... which really holds each kind of record (stopped, failure with its tail, skip undone by unskip, claim)",
   sf["tasks"][kf[0]]["state"] == "stopped" and sf["tasks"][kf[1]]["failures"] == 1
   and sf["tasks"][kf[1]]["fail_tail"] and sf["tasks"][kf[1]]["worker"] == "siteA" and sf["tasks"][kf[2]]["worker"] == "siteC",
   json.dumps(sf, default=str)[:800])
c = sqlite3.connect(DB[1])
kinds = {k for (k,) in c.execute("SELECT DISTINCT kind FROM mark")} | {k for (k,) in c.execute("SELECT DISTINCT kind FROM line")}
c.close()
ok("the database keeps them as rows of the queue tables", {"stopped", "released", "failtail", "failures"} <= kinds, kinds)

# ---- 2. half-made claim ----
H = ("files", os.path.join(T, "q2"))
HD = ("db", os.path.join(T, "q2.db"))
for w in (H, HD):
    tq(w, *toy(0))
key = status(H)["tasks"][0]["key"]
open(os.path.join(H[1], proto.QUEUE["enabled"]), "w").close()
old = time.time() - 3600
os.makedirs(os.path.join(H[1], proto.QUEUE["claims"], proto.safe(key)))
os.utime(os.path.join(H[1], proto.QUEUE["claims"], proto.safe(key)), (old, old))
c = sqlite3.connect(HD[1])
c.execute("INSERT INTO claim(name, info, t) VALUES(?, NULL, ?)", (proto.safe(key), old))
c.commit()
c.close()
a, b = norm(status(H)), norm(status(HD))
ok("a claim made but never written, an hour old: the same state in both", a == b and a["tasks"][key]["state"] == b["tasks"][key]["state"], (a, b))
ok("... and the next take replaces it in the database", tq(HD, "take", "--worker", "siteZ", "--assign", key)[0] == 0
   and status(HD)["tasks"][0]["worker"] == "siteZ")

# ---- 3. hub_db() ----
SEL = os.path.join(T, "sel.db")
probe = "import qstore; print(type(qstore.hub_db()).__name__)"


def pick(**kw):
    e = {k: v for k, v in os.environ.items() if k not in ("TASKQ_DIR", "TASKQ_ROOT", "TASKQ_QSTORE", "TASKQ_STATE")}
    e["TASKQ_DB"] = SEL
    e.update(kw)
    return subprocess.run([sys.executable, "-c", probe], cwd=HERE, env=e, capture_output=True, text=True).stdout.strip()


sys.path.insert(0, HERE)
import db as D  # noqa: E402
c = D.connect(create=True, p=SEL)
c.close()
before = pick()
c = sqlite3.connect(SEL)
c.execute("INSERT INTO meta(k, v) VALUES(?, ?)", (__import__("qstore").QUEUE_META, json.dumps("db")))
c.commit()
c.close()
ok("hub_db(): queue dir until the database holds the queue, then the database; never with TASKQ_DIR or an executor root;"
   " TASKQ_QSTORE forces either",
   (before, pick(), pick(TASKQ_DIR="/x"), pick(TASKQ_ROOT="/x"), pick(TASKQ_QSTORE="files"), pick(TASKQ_DIR="/x", TASKQ_QSTORE="db"))
   == ("NoneType", "DbStore", "NoneType", "NoneType", "NoneType", "DbStore"))
NEW = os.path.join(T, "newhub")
st_dir = os.path.join(NEW, ".local", "state", "taskq")
os.makedirs(st_dir)
with open(os.path.join(st_dir, proto.FORMAT_FILE), "w") as f:
    f.write(f"{__import__('qstore').DB_FROM}\n")
ok("a new hub stamped with the database format (no database yet): the database",
   pick(HOME=NEW, TASKQ_DB=os.path.join(st_dir, "taskq.db")) == "DbStore")

# ---- 4. migrate.hub_to_db ----
MIG = r'''
import json, os, sys
sys.path.insert(0, %(here)r)
import migrate, taskq, qstore
out = []
try:
    migrate.hub_to_db(dict(home=%(home)r, queue=taskq.TQ), out.append, %(dry)s)
    print(json.dumps(dict(ok=True, say=out)))
except migrate.Stop as e:
    print(json.dumps(dict(ok=False, stop=str(e), say=out)))
'''
BREAK = r'''
import qstore
real = qstore.DbStore.get
qstore.DbStore.get = lambda self, kind, key: (real(self, kind, key) or "") + "x"
'''


def migrate(q, dbp, dry=False, pre=""):
    e = dict(os.environ, TASKQ_DIR=q, TASKQ_DB=dbp)
    e.pop("TASKQ_QSTORE", None)
    code = pre + MIG % dict(here=HERE, home=os.path.join(T, "home"), dry=dry)
    r = subprocess.run([sys.executable, "-c", code], cwd=HERE, env=e, capture_output=True, text=True)
    try:
        return json.loads(r.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return dict(ok=False, stop=r.stdout + r.stderr)


def tree(d):
    out = {}
    for root, _, files in os.walk(d):
        for f in files:
            p = os.path.join(root, f)
            out[os.path.relpath(p, d)] = (open(p, "rb").read(), os.stat(p).st_mtime)
    return out


def queue_rows(dbp):
    c = sqlite3.connect(dbp)
    try:
        return {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("task", "claim", "mark", "line", "smoke", "switch")}
    except sqlite3.OperationalError:
        return {}
    finally:
        c.close()


def smoke_file(q):
    os.makedirs(os.path.join(q, proto.SMOKE_DIR), exist_ok=True)
    with open(os.path.join(q, proto.SMOKE_DIR, "site-pueue.json"), "w") as f:
        json.dump({"envkind": {"state": "ok", "key": "_smoke/x"}}, f)
    open(os.path.join(q, proto.SMOKE_DIR, "site-pueue.json.lock"), "w").close()


Q4 = os.path.join(T, "q4")
D4 = os.path.join(T, "q4.db")
script(("files", Q4))
smoke_file(Q4)
open(os.path.join(Q4, proto.QUEUE["enabled"]), "w").close()
was = norm(status(("files", Q4)))
snap = tree(Q4)
r = migrate(Q4, D4, dry=True)
ok("dry run: says what it would copy, changes nothing (no database made, files the same)",
   r["ok"] and "copy the hub queue" in " ".join(r["say"]) and not os.path.exists(D4) and tree(Q4) == snap, r)
r = migrate(Q4, D4)
now = norm(status(("db", D4)))
ok("applied: status from the database equals the queue dir's before", r["ok"] and now == was,
   json.dumps([r, was, now], default=str)[:1500])
ok("... the database holds the queue (meta), the switch row is absent (ENABLED was there), smoke records copied",
   D.get_meta(sqlite3.connect(D4), __import__("qstore").QUEUE_META) == "db" and queue_rows(D4)["switch"] == 0
   and queue_rows(D4)["smoke"] == 1, queue_rows(D4))
left = sorted(os.listdir(Q4))
ok("... the queue dir's records are removed (tasks, claims, record dirs, smoke, ENABLED)", left == [], left)
rows = queue_rows(D4)
r2 = migrate(Q4, D4)
ok("a second run does nothing", r2["ok"] and queue_rows(D4) == rows and not r2["say"], r2)

Q5, D5 = os.path.join(T, "q5"), os.path.join(T, "q5.db")
script(("files", Q5))
smoke_file(Q5)
snap = tree(Q5)
r = migrate(Q5, D5, pre=BREAK)
ok("a copy that differs: stops, says what differs, rolls back (queue tables empty, no meta), files untouched",
   not r["ok"] and "differs" in r.get("stop", "") and set(queue_rows(D5).values()) <= {0}
   and D.get_meta(sqlite3.connect(D5), __import__("qstore").QUEUE_META) is None and tree(Q5) == snap, r)
r = migrate(Q5, D5)
ok("the queue dir without ENABLED: copied with the queue switch off", r["ok"] and not status(("db", D5))["enabled"]
   and queue_rows(D5)["switch"] == 1, r)

Q6, D6 = os.path.join(T, "q6"), os.path.join(T, "q6.db")
script(("files", Q6))
tq(("db", D6), *toy(9))
snap = tree(Q6)
r = migrate(Q6, D6)
ok("rows already in the queue tables (without the meta): stops, nothing changed", not r["ok"] and "already has rows" in r.get("stop", "")
   and tree(Q6) == snap and queue_rows(D6)["task"] == 1, r)

print("all passed" if not fails else f"{fails} failures")
sys.exit(1 if fails else 0)
