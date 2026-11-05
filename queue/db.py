"""Hub database (proto.DB under the state dir): event log, health problems, notices, acks, sha256 cache, progress snapshots.

Only the hub has it. It is created or migrated only by create=True callers (a real tick, `taskq db migrate`); everyone else
opens an existing file and gets None when there is none, so code shipped in executor bundles never creates one. A file on a
network filesystem is refused (SQLite locking is unsafe there). A database newer than this code is opened read-only.
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
]
VERSION = len(SCHEMA)


def path():
    return os.environ.get("TASKQ_DB") or cfg.state(proto.DB)


def fstype(p):
    """Filesystem type of the mount holding p (longest mount-point prefix in /proc/self/mounts); unknown → ""."""
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
    """→ sqlite3 connection (autocommit; use tx() for writes), or None when there is no database (and create is False)
    or it sits on a network filesystem. create=True creates it and applies pending migrations."""
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
    """with tx(db): ... → BEGIN IMMEDIATE … COMMIT (ROLLBACK on error)."""

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.execute("BEGIN IMMEDIATE")
        return self.db

    def __exit__(self, et, e, tb):
        self.db.execute("ROLLBACK" if et else "COMMIT")
        return False


def writable(db):
    """False for a read-only connection (mode=ro, or a database newer than this code)."""
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


def set_meta(db, k, v):
    db.execute("INSERT INTO meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, json.dumps(v, ensure_ascii=False)))


def main(argv):
    import argparse
    ap = argparse.ArgumentParser(prog="taskq db", description="The hub database")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="create the database or apply pending migrations (hub only)")
    sub.add_parser("path", help="print where it is")
    sub.add_parser("version", help="schema version on disk and in this code")
    a = ap.parse_args(argv)
    if a.cmd == "path":
        print(path())
        return 0
    if a.cmd == "version":
        db = connect(ro=True)
        print(f"disk {db.execute('PRAGMA user_version').fetchone()[0] if db else 'none'}, code {VERSION}")
        return 0
    if on_netfs(os.path.dirname(path())):
        print(f"taskq db: {path()} is on a network filesystem ({fstype(os.path.dirname(path()))}); refusing", file=sys.stderr)
        return 1
    connect(create=True).close()
    print(f"{path()}: version {VERSION}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
