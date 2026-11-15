"""Hub bookkeeping kept in the hub database (db.py), each piece with one writer:

    sites      what relay last read from each executor (relay.write_status, one row per executor); taskq status, taskq
               status --all --json, bundle pruning and refill read it
    records    the core's own pueue tasks (common.update_records: refill.add writes, health syncs with pueue)
    relay      relay's placement records per executor and task, input link sets and the rest of relay's bookkeeping
               (relay_store.update_state; relay, relay_transfer and bundle_gate change it through there)

Only on the hub. Writers create or migrate the database (connect(create=True)) and raise when it cannot be used (network
file system, newer schema); readers return empty values when it is absent.
"""
import json
import os
import sys
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import db as D  # noqa: E402

TABLES = ("site", "record", "placed", "linkset", "relay")  # db.SCHEMA 4
_TX = threading.local()  # the connection of the update_relay running in this thread (put_links writes through it)


def _dump(v):
    """Key order kept as written (what is built from a record, such as the specs sent to executors, stays byte for byte the same)."""
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def _read():
    db = D.connect()
    return db if db is not None and D.has(db, "placed") else (db.close() if db else None)


def _write():
    db = D.connect(create=True)
    if db is None or not D.has(db, "placed") or not D.writable(db):
        if db is not None:
            db.close()
        raise RuntimeError(f"cannot write the hub database {D.path()} (on a network file system, or created by a newer taskq)")
    return db


# ---------------- sites ----------------
def sites():
    """-> {executor: what relay last read there (time, label, tasks, seglogs, squeue, free, bundle, live, held, envs_ok)}."""
    db = _read()
    if db is None:
        return {}
    try:
        return {n: json.loads(b) for n, b in db.execute("SELECT name, body FROM site")}
    finally:
        db.close()


def put_site(name, rec):
    db = _write()
    try:
        with D.tx(db):
            db.execute("INSERT INTO site(name, body) VALUES(?, ?) ON CONFLICT(name) DO UPDATE SET body=excluded.body", (name, _dump(rec)))
    finally:
        db.close()


# ---------------- records ----------------
def _records(db):
    return [json.loads(b) for (b,) in db.execute("SELECT body FROM record ORDER BY seq")]


def records():
    db = _read()
    if db is None:
        return []
    try:
        return _records(db)
    finally:
        db.close()


def update_records(fn):
    """fn(records) -> new list, read-modify-write in one transaction -> the new list."""
    db = _write()
    try:
        with D.tx(db):
            recs = fn(_records(db))
            db.execute("DELETE FROM record")
            db.executemany("INSERT INTO record(seq, body) VALUES(?, ?)", [(i, _dump(r)) for i, r in enumerate(recs)])
        return recs
    finally:
        db.close()


# ---------------- relay ----------------
def _relay(db):
    st = {n: json.loads(b) for n, b in db.execute("SELECT name, body FROM relay")}
    keys = {}
    for site, key, b in db.execute("SELECT site, key, body FROM placed"):
        keys.setdefault(site, {})[key] = json.loads(b)
    st["keys"] = keys
    return st


def _rows(st):
    rows = {("relay", n, ""): _dump(v) for n, v in st.items() if n != "keys"}
    rows.update({("placed", site, key): _dump(r) for site, recs in (st.get("keys") or {}).items() for key, r in recs.items()})
    return rows


def relay():
    """-> relay's bookkeeping as one dict: keys {executor: {task key: record}}, plus last_ok, views, held, bundle_since,
    bundle_for."""
    db = _read()
    if db is None:
        return {}
    try:
        return _relay(db)
    finally:
        db.close()


def update_relay(fn):
    """fn(state) edits relay() in place, in one transaction (other writers wait); only rows that changed are written, and link
    sets no record names are removed -> what fn returned."""
    db = _write()
    try:
        with D.tx(db):
            st = _relay(db)
            before = _rows(st)
            _TX.db = db
            try:
                out = fn(st)
            finally:
                _TX.db = None
            after = _rows(st)
            for t, a, b in before.keys() - after.keys():
                if t == "relay":
                    db.execute("DELETE FROM relay WHERE name=?", (a,))
                else:
                    db.execute("DELETE FROM placed WHERE site=? AND key=?", (a, b))
            for (t, a, b), v in after.items():
                if before.get((t, a, b)) == v:
                    continue
                if t == "relay":
                    db.execute("INSERT INTO relay(name, body) VALUES(?, ?) ON CONFLICT(name) DO UPDATE SET body=excluded.body", (a, v))
                else:
                    db.execute("INSERT INTO placed(site, key, body) VALUES(?, ?, ?) ON CONFLICT(site, key) DO UPDATE SET body=excluded.body",
                               (a, b, v))
            used = {r.get("linkset") for recs in (st.get("keys") or {}).values() for r in recs.values()}
            for (i,) in db.execute("SELECT id FROM linkset").fetchall():
                if i not in used:
                    db.execute("DELETE FROM linkset WHERE id=?", (i,))
        return out
    finally:
        db.close()


def put_links(i, links):
    """Store link set i (inside update_relay's fn, through its transaction; elsewhere in one of its own)."""
    db = getattr(_TX, "db", None)
    if db is not None:
        db.execute("INSERT OR IGNORE INTO linkset(id, links) VALUES(?, ?)", (i, _dump(links)))
        return
    db = _write()
    try:
        with D.tx(db):
            db.execute("INSERT OR IGNORE INTO linkset(id, links) VALUES(?, ?)", (i, _dump(links)))
    finally:
        db.close()


def links(i):
    """Link set i -> {relative path: sha256}; unknown -> {}."""
    db = getattr(_TX, "db", None) or _read()
    if db is None:
        return {}
    try:
        r = db.execute("SELECT links FROM linkset WHERE id=?", (i,)).fetchone()
        return json.loads(r[0]) if r else {}
    finally:
        if db is not getattr(_TX, "db", None):
            db.close()
