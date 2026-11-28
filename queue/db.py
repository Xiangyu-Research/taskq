"""Hub database (proto.DB under the state dir): event log, health problems, notices, acks, progress snapshots, results on the
store, and the hub's bookkeeping (hubstate.py: what relay read from each executor, task records, relay's placement records).

Only exists on the hub. Only create=True calls (a real tick, `taskq db migrate`) create or migrate; other calls open an existing file only and return None, so executor bundles never create it. Not used on network filesystems (SQLite locking is unreliable there). A database newer than this code opens read-only.
"""
import json
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402

NETFS = ("nfs", "nfs4", "lustre", "beegfs", "cifs", "smb3", "smbfs", "gpfs", "fuse.sshfs", "9p", "ceph", "glusterfs")
SCHEMA = [
    # 1
    """
    CREATE TABLE log(id INTEGER PRIMARY KEY, t REAL NOT NULL, source TEXT NOT NULL, kind TEXT, level TEXT, key TEXT, site TEXT,
                     msg TEXT NOT NULL, data TEXT, uid TEXT UNIQUE);
    CREATE INDEX log_t ON log(t);
    CREATE INDEX log_key ON log(key, t);
    CREATE INDEX log_source ON log(source, t);
    CREATE TABLE problem(id TEXT PRIMARY KEY, body TEXT NOT NULL);
    CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT);
    CREATE TABLE ack(id TEXT PRIMARY KEY, t TEXT NOT NULL);
    CREATE TABLE notice(name TEXT PRIMARY KEY, text TEXT NOT NULL, since TEXT NOT NULL, at TEXT);
    CREATE TABLE hash(k TEXT PRIMARY KEY, sha TEXT NOT NULL);
    CREATE TABLE progress(t REAL NOT NULL, key TEXT NOT NULL, state TEXT, progress REAL, site TEXT, data TEXT, PRIMARY KEY(key, t));
    CREATE INDEX progress_t ON progress(t);
    """,
    # 2: the sha256 cache moved to the store (site_ops.hashes)
    """
    DROP TABLE hash;
    """,
    # 3: keys whose done files are on the store (advance writes the whole set each round; taskq status reads it for tasks not in the queue)
    """
    CREATE TABLE stored(key TEXT PRIMARY KEY, t REAL NOT NULL);
    """,
    # 4: hub bookkeeping that lived in JSON files (hubstate.py; formerly relay/state.json, relay/linksets/,
    # relay/status.json and tasks.json)
    """
    CREATE TABLE site(name TEXT PRIMARY KEY, body TEXT NOT NULL);
    CREATE TABLE record(seq INTEGER PRIMARY KEY, body TEXT NOT NULL);
    CREATE TABLE placed(site TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(site, key));
    CREATE TABLE linkset(id TEXT PRIMARY KEY, links TEXT NOT NULL);
    CREATE TABLE relay(name TEXT PRIMARY KEY, body TEXT NOT NULL);
    """,
    # 5: the hub queue (qstore.DbStore; formerly the hub queue dir: tasks.jsonl, claims/, the record dirs, smoke/ and ENABLED;
    # self migrate step 3 copies it in). Record rows are named like the files were (proto.safe(key)).
    """
    CREATE TABLE task(key TEXT PRIMARY KEY, ord INTEGER NOT NULL, nice INTEGER NOT NULL, exp TEXT, recipe TEXT, params TEXT NOT NULL,
                      spec TEXT, added REAL);
    CREATE TABLE claim(name TEXT PRIMARY KEY, info TEXT, t REAL NOT NULL);
    CREATE TABLE mark(kind TEXT NOT NULL, name TEXT NOT NULL, body TEXT NOT NULL, t REAL NOT NULL, PRIMARY KEY(kind, name));
    CREATE TABLE line(id INTEGER PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL, body TEXT NOT NULL, t REAL NOT NULL);
    CREATE INDEX line_name ON line(kind, name);
    CREATE TABLE smoke(site TEXT PRIMARY KEY, body TEXT NOT NULL);
    CREATE TABLE switch(name TEXT NOT NULL, target TEXT NOT NULL, value TEXT NOT NULL, t REAL NOT NULL, until REAL, why TEXT, by TEXT,
                        PRIMARY KEY(name, target));
    """,
    # 6: operations that take more than one round (queue/intents.py)
    """
    CREATE TABLE intent(id INTEGER PRIMARY KEY, kind TEXT NOT NULL, key TEXT, args TEXT NOT NULL, state TEXT NOT NULL, t REAL NOT NULL,
                        deadline REAL, note TEXT, by TEXT);
    CREATE INDEX intent_state ON intent(state);
    """,
]
VERSION = len(SCHEMA)


def path():
    return os.environ.get("TASKQ_DB") or cfg.state(proto.DB)


def fstype(p):
    """Filesystem type of p's mount point (longest mount point prefix in /proc/self/mounts); unknown -> ""."""
    p = os.path.realpath(p)
    best, kind = "", ""
    try:
        for ln in open("/proc/self/mounts"):
            f = ln.split()
            if len(f) < 3:
                continue
            mp = f[1].replace("\\040", " ")
            if (p == mp or p.startswith(mp.rstrip("/") + "/")) and len(mp) >= len(best):
                best, kind = mp, f[2]
    except OSError:
        pass
    return kind


def on_netfs(p):
    t = fstype(p)
    return t in NETFS or t.startswith("nfs")


def connect(create=False, ro=False, p=None):
    """-> sqlite3 connection (autocommit; use tx() to write); None when the database is absent (and create is False) or on a network filesystem.
    create=True creates it and applies pending migrations."""
    p = p or path()
    d = os.path.dirname(p) or "."
    if not os.path.exists(p):
        if not create or ro:
            return None
        os.makedirs(d, exist_ok=True)
    if on_netfs(d):
        return None
    if ro:
        db = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=30, isolation_level=None)
    else:
        db = sqlite3.connect(p, timeout=30, isolation_level=None)
        db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    v = db.execute("PRAGMA user_version").fetchone()[0]
    if v > VERSION and not ro:
        db.close()
        return connect(ro=True, p=p)
    if v < VERSION:
        if not create or ro:
            return db if v else (db.close() or None)
        migrate(db, v)
    return db


def migrate(db, v):
    with tx(db):
        cur = db.execute("PRAGMA user_version").fetchone()[0]
        for i in range(cur, VERSION):
            for stmt in SCHEMA[i].split(";"):
                if stmt.strip():
                    db.execute(stmt)
        db.execute(f"PRAGMA user_version={VERSION}")


class tx:
    """with tx(db): ... -> BEGIN IMMEDIATE ... COMMIT (ROLLBACK on error). Inside a transaction already (a hub queue edit,
    qstore.DbStore.tx): part of it, committed or rolled back with it."""

    def __init__(self, db):
        self.db = db
        self.outer = False

    def __enter__(self):
        if not self.db.in_transaction:
            self.db.execute("BEGIN IMMEDIATE")
            self.outer = True
        return self.db

    def __exit__(self, et, e, tb):
        if self.outer:
            self.db.execute("ROLLBACK" if et else "COMMIT")
        return False


def writable(db):
    """Read-only connection (mode=ro, or a database newer than this code) -> False."""
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute(f"PRAGMA user_version={db.execute('PRAGMA user_version').fetchone()[0]}")
        db.execute("ROLLBACK")
        return True
    except sqlite3.OperationalError:
        if db.in_transaction:
            db.execute("ROLLBACK")
        return False


def has(db, table):
    return bool(db and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def get_meta(db, k, default=None):
    if not has(db, "meta"):
        return default
    r = db.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return json.loads(r[0]) if r else default


def compact(db):
    """VACUUM when most of the file is free pages (left by dropped tables or deleted rows; SQLite does not shrink the file by
    itself) -> bytes given back (0 when not needed)."""
    pages, free, size = (db.execute(f"PRAGMA {x}").fetchone()[0] for x in ("page_count", "freelist_count", "page_size"))
    share, least = proto.DB_COMPACT
    if not pages or free < pages * share or free * size < least:
        return 0
    db.execute("VACUUM")
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # in WAL mode the file shrinks only once the log is written back
    return (pages - db.execute("PRAGMA page_count").fetchone()[0]) * size


def set_meta(db, k, v):
    db.execute("INSERT INTO meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, json.dumps(v, ensure_ascii=False)))


def main(argv):
    import argparse
    ap = argparse.ArgumentParser(prog="taskq db", description="hub database")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="create the database or apply pending migrations (hub only)")
    sub.add_parser("path", help="where it is")
    sub.add_parser("version", help="schema version on disk and in this code")
    a = ap.parse_args(argv)
    if a.cmd == "path":
        print(path())
        return 0
    if a.cmd == "version":
        db = connect(ro=True)
        print(f"on disk {db.execute('PRAGMA user_version').fetchone()[0] if db else 'none'}, code {VERSION}")
        return 0
    if on_netfs(os.path.dirname(path())):
        print(f"taskq db: {path()} is on a network filesystem ({fstype(os.path.dirname(path()))}), not creating it", file=sys.stderr)
        return 1
    connect(create=True).close()
    print(f"{path()}: version {VERSION}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
