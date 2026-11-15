#!/usr/bin/env python3
"""Hub database (db.py) and event log (events.py): never created by readers, refused on a network filesystem, spool when it
cannot be written, concurrent writers, a newer schema opens read-only, TEXT_LOGS sources also get a text line, progress rows only on
change, no sha256 cache in the hub database (it is on the store: site_ops.hashes), the hub bookkeeping (hubstate.py).

    python3 queue/test_db.py
"""
import json
import os
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
T = fixture.tmpdir()
os.environ["TASKQ_DB"] = os.path.join(T, "hub", "taskq.db")
sys.path.insert(0, HERE)
import db as D  # noqa: E402
import events as EV  # noqa: E402
import proto  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


P = D.path()
ok("readers never create it: connect() and connect(ro=True) → None, no file", D.connect() is None and D.connect(ro=True) is None
   and not os.path.exists(os.path.dirname(P)))
EV.add("relay", "x happened")
ok("events.add without a database: nothing written, no spool (executors)", not os.path.exists(P) and not os.path.exists(EV.spool_path()))
ok("TEXT_LOGS source: also a plain line in its text log", "x happened" in open(os.environ["RELAY_LOG"]).read())

real = D.fstype
D.fstype = lambda p: "nfs4"
ok("network filesystem: refused even with create=True", D.connect(create=True) is None and not os.path.exists(P))
D.fstype = real

db = D.connect(create=True)
ok("create=True: made and migrated", db is not None and db.execute("PRAGMA user_version").fetchone()[0] == D.VERSION
   and db.execute("PRAGMA journal_mode").fetchone()[0] == "wal" and D.writable(db))
ok("read-only connection is not writable", not D.writable(D.connect(ro=True)))

EV.add("relay", "site-a claimed demo/a/0", key="demo/a/0", kind="claim", site="site-a")
EV.lines("advance", ["queued x", "queued y"])
rows = EV.query(db)
ok("add / lines → rows with source, kind, key, site", [(r["source"], r["kind"], r["msg"]) for r in rows] ==
   [("relay", "claim", "site-a claimed demo/a/0"), ("advance", None, "queued x"), ("advance", None, "queued y")])
ok("query by key, source, grep", [r["msg"] for r in EV.query(db, key="demo/a/0")] == ["site-a claimed demo/a/0"]
   and len(EV.query(db, source="advance")) == 2 and [r["msg"] for r in EV.query(db, grep="y$")] == ["queued y"])

# spool: a database that cannot be written right now
lk = D.connect()
lk.execute("BEGIN IMMEDIATE")
old_connect = D.connect
D.connect = lambda *a, **k: (lambda c: (c.execute("PRAGMA busy_timeout=100"), c)[1])(old_connect(*a, **k))
EV.add("feed", "while locked")
D.connect = old_connect
lk.execute("ROLLBACK")
ok("locked: the row goes to the spool, nothing raised", os.path.exists(EV.spool_path()) and "while locked" in open(EV.spool_path()).read())
n = EV.flush(db)
ok("flush: spool imported and removed", n == 1 and not os.path.exists(EV.spool_path()) and EV.query(db, grep="while locked"))

# concurrent writers
code = ("import sys; sys.path.insert(0, %r); import events as EV\n"
        "for i in range(50): EV.add('relay', 'w%%s-%%d' %% (sys.argv[1], i))") % HERE
ps = [subprocess.Popen([sys.executable, "-c", code, str(i)], env=os.environ) for i in range(4)]
rcs = [p.wait(timeout=120) for p in ps]
ok("4 processes × 50 rows at once: all there, no spool", rcs == [0] * 4 and len(EV.query(db, grep="^w[0-9]-")) == 200
   and not os.path.exists(EV.spool_path()))

# notes
r = subprocess.run([sys.executable, os.path.join(os.path.dirname(HERE), "bin", "taskq"), "note", "fixed", "--key", "demo/c/1", "--kind", "fix",
                    "--uid", "u1", "it"], capture_output=True, text=True, env=os.environ, timeout=60)
r2 = subprocess.run([sys.executable, os.path.join(os.path.dirname(HERE), "bin", "taskq"), "note", "--uid", "u1", "again"],
                    capture_output=True, text=True, env=os.environ, timeout=60)
ok("taskq note: written with key and kind (text on both sides of options); same uid ignored", r.returncode == 0 and r2.returncode == 0
   and [(x["kind"], x["msg"]) for x in EV.query(db, source="note")] == [("fix", "fixed it")])
r = subprocess.run([sys.executable, os.path.join(os.path.dirname(HERE), "bin", "taskq"), "events", "--since", "all", "--key", "demo/c/1",
                    "--json"], capture_output=True, text=True, env=os.environ, timeout=60)
js = [json.loads(x) for x in r.stdout.splitlines()]
ok("taskq events --json: one row per line with proto.EVENT_FIELDS", r.returncode == 0 and len(js) == 1
   and tuple(js[0]) == EV.proto.EVENT_FIELDS and js[0]["msg"] == "fixed it")

r = subprocess.run([sys.executable, os.path.join(os.path.dirname(HERE), "bin", "taskq"), "note", "--kind", "report", "--data", '{"n": 3}',
                    "sent"], capture_output=True, text=True, env=os.environ, timeout=60)
r2 = subprocess.run([sys.executable, os.path.join(os.path.dirname(HERE), "bin", "taskq"), "note", "--data", "{bad", "x"],
                    capture_output=True, text=True, env=os.environ, timeout=60)
ok("taskq note --data: stored and read back; bad JSON refused", r.returncode == 0 and r2.returncode == 2
   and [json.loads(x["data"]) for x in EV.query(db, source="note", kind="report")] == [{"n": 3}])

# taskq notify: pushes through monitor/push.py and logs a notify event
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "monitor"))
import push as PU  # noqa: E402
sent = []
PU.push = lambda msg, **kw: sent.append((msg, kw))
rc = PU.main(["--title", "T", "--kind", "report", "--data", '{"n": 5}', "hello"])
ev = EV.query(db, source="notify")
ok("taskq notify: pushed with the title, logged as a notify event with --data", rc == 0
   and sent == [("hello", dict(title="T", priority="default", tags=""))] and [(x["kind"], x["msg"]) for x in ev] == [("report", "hello")]
   and json.loads(ev[0]["data"]) == dict(n=5, title="T", priority="default"))

# progress snapshots
t0 = time.time()
n1 = EV.snapshot(db, [dict(key="k1", state="running", progress=0.1, site="s"), dict(key="k2", state="queued", progress=None)], t=t0)
n2 = EV.snapshot(db, [dict(key="k1", state="running", progress=0.1, site="s"), dict(key="k2", state="queued", progress=None)], t=t0 + 1)
n3 = EV.snapshot(db, [dict(key="k1", state="running", progress=0.2, site="s"), dict(key="k2", state="queued", progress=None)], t=t0 + 2)
ok("progress: a row only when state, progress or site changed", (n1, n2, n3) == (2, 0, 1)
   and [r["progress"] for r in EV.progress_rows(db, key="k1")] == [0.1, 0.2])

# changes into done / failed, failures released on executors, results on the store
t1 = t0 + 100
EV.snapshot(db, [dict(key="a", state=None), dict(key="b", state="free"), dict(key="c", state="done")], t=t1)
EV.snapshot(db, [dict(key="a", state="done"), dict(key="b", state="done"), dict(key="c", state="done")], t=t1 + 1)
ok("transitions: from not queued (no state) or another state counts, a task's first record does not",
   sorted(c["key"] for c in EV.transitions(db, t1, ("done",))) == ["a", "b"])
EV.snapshot(db, [dict(key="b", state="failed")], t=t1 + 2)
EV.add("relay", "x failed on site-a", kind=proto.EV_FAILED, key="x", site="site-a", db=db, t=t1 + 3)
ok("failed_since: hub queue failures and failures released by an executor, oldest first",
   [(r["key"], r["site"]) for r in EV.failed_since(db, t1)] == [("b", None), ("x", "site-a")])
ok("schema 3: stored is empty before the first advance round, then holds exactly the last set", EV.stored(db) == set()
   and (EV.set_stored(db, {"a", "b"}), EV.set_stored(db, {"b", "c"}), EV.stored(db))[-1] == {"b", "c"})

# compaction: only when most of the file is free pages
_cp = os.path.join(T, "compact.db")
_c = D.connect(create=True, p=_cp)
ok("compact: a database with no free pages is left alone", D.compact(_c) == 0)
_c.execute("CREATE TABLE big(x BLOB)")
_c.executemany("INSERT INTO big VALUES(?)", [(b"x" * 4000,) for _ in range(3000)])
_c.execute("DROP TABLE big")
_saved = proto.DB_COMPACT
proto.DB_COMPACT = (0.5, 1 << 20)
_n = D.compact(_c)
proto.DB_COMPACT = _saved
ok("compact: mostly free pages (a dropped table) -> VACUUM gives them back, tables kept",
   _n > 10e6 and _c.execute("PRAGMA freelist_count").fetchone()[0] == 0 and D.has(_c, "log"))
_c.close()

# the sha256 cache is on the store (site_ops.hashes), not in the hub database
ok("schema 2: no hash table in the hub database",
   db.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='hash'").fetchone()[0] == 0)
import site_ops as SO  # noqa: E402
R = os.path.join(T, "store_root")
f = os.path.join(T, "blob.dat")
open(f, "wb").write(b"abc")
h = SO.hashes(R, [f])[f]
HD = SO.sub(R, "hashes")
rows = lambda: sqlite3.connect(HD).execute("SELECT k, sha FROM hash").fetchall()  # noqa: E731
ok("site_ops.hashes: right hash, cached in the root's state/hashes.db keyed by path, size, mtime",
   h.startswith("ba7816bf") and HD == os.path.join(R, "state", "hashes.db") and len(rows()) == 1 and rows()[0][1] == h)
with sqlite3.connect(HD) as c:
    c.execute("UPDATE hash SET sha='cached'")
ok("site_ops.hashes: an unchanged file is not read again", SO.hashes(R, [f])[f] == "cached")
open(f, "wb").write(b"abcd")
ok("site_ops.hashes: a changed file (size, mtime) is hashed again", SO.hashes(R, [f])[f].startswith("88d4266f") and len(rows()) == 2)

# hub bookkeeping (hubstate.py)
import hubstate as HS  # noqa: E402
HS.put_site("a", dict(time=1, tasks={"k": {"ran": True}}))
HS.put_site("a", dict(time=2, tasks={}))
ok("sites: one row per executor, the last write wins", HS.sites() == {"a": dict(time=2, tasks={})})


def place(st):
    st.setdefault("keys", {}).setdefault("a", {})["k1"] = dict(recipe="r", linkset=HS.put_links("L1", {"p": "h"}) or "L1")
    st["keys"]["a"]["k2"] = dict(recipe="r", linkset="L2")
    HS.put_links("L2", {"q": "h"})
    HS.put_links("L9", {"z": "h"})  # named by no record
    st["views"] = {"a": {"v": 1}}
    return "out"


ok("update_relay: returns what fn returned; records, views and the link sets they name are kept, others removed",
   HS.update_relay(place) == "out" and HS.relay() == dict(keys={"a": {"k1": dict(recipe="r", linkset="L1"), "k2": dict(recipe="r", linkset="L2")}},
                                                         views={"a": {"v": 1}})
   and HS.links("L1") == {"p": "h"} and HS.links("L9") == {} and HS.links("nope") == {})


def bad(st):
    st["keys"]["a"].pop("k2")
    raise RuntimeError("half way")


try:
    HS.update_relay(bad)
except RuntimeError:
    pass
ok("update_relay: fn fails half way -> nothing changes (one transaction)", set(HS.relay()["keys"]["a"]) == {"k1", "k2"} and HS.links("L2") == {"q": "h"})
HS.update_relay(lambda st: st["keys"]["a"].pop("k2"))
ok("update_relay: a dropped record goes, and so does the link set only it named", set(HS.relay()["keys"]["a"]) == {"k1"} and HS.links("L2") == {})
ok("records: read-modify-write -> the new list", HS.update_records(lambda r: r + [dict(id=1)]) == [dict(id=1)] and HS.records() == [dict(id=1)])

# newer schema → read-only
db.execute(f"PRAGMA user_version={D.VERSION + 1}")
nd = D.connect(create=True)
ok("database newer than the code: opened read-only, never migrated down", nd is not None and not D.writable(nd))
EV.add("relay", "after newer")
ok("events.add on a newer database: spooled, not lost", "after newer" in open(EV.spool_path()).read())
try:
    HS.put_site("a", {})
    refused = False
except RuntimeError as e:
    refused = "newer" in str(e)
ok("hubstate writers on a newer database: refused with a clear error (not written to a schema they do not know)", refused)
db.execute(f"PRAGMA user_version={D.VERSION}")

print("all passed" if not fails else f"{fails} failed")
sys.exit(1 if fails else 0)
