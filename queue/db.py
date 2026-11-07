"""hub 的数据库（状态目录下的 proto.DB）：事件日志、健康检查的问题、提醒、确认（ack）、进度快照。

只有 hub 上有。只有 create=True 的调用（真做的一轮 tick、`taskq db migrate`）会建库或做迁移；别的调用只打开已有的文件，
没有就得到 None，所以执行机代码包里的代码不会建库。放在网络文件系统上的不用（那里 SQLite 的锁不可靠）。
比这份代码新的数据库只读打开。
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
    # 2：sha256 缓存搬到了 store（site_ops.hashes）；taskq self migrate 在这之前先把它拷过去
    """
    DROP TABLE hash;
    """,
]
VERSION = len(SCHEMA)


def path():
    return os.environ.get("TASKQ_DB") or cfg.state(proto.DB)


def fstype(p):
    """p 所在挂载点的文件系统类型（/proc/self/mounts 里最长的挂载点前缀）；认不出 → ""。"""
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
    """→ sqlite3 连接（自动提交；写的时候用 tx()）；没有数据库（且 create 为 False）或它在网络文件系统上时 → None。
    create=True 时建库并做还没做的迁移。"""
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
    """with tx(db): ... → BEGIN IMMEDIATE … COMMIT（出错时 ROLLBACK）。"""

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.execute("BEGIN IMMEDIATE")
        return self.db

    def __exit__(self, et, e, tb):
        self.db.execute("ROLLBACK" if et else "COMMIT")
        return False


def writable(db):
    """只读的连接（mode=ro，或比这份代码新的数据库）→ False。"""
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
    ap = argparse.ArgumentParser(prog="taskq db", description="hub 的数据库")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="建库或做还没做的迁移（只在 hub 上）")
    sub.add_parser("path", help="在哪")
    sub.add_parser("version", help="盘上和这份代码的表结构版本")
    a = ap.parse_args(argv)
    if a.cmd == "path":
        print(path())
        return 0
    if a.cmd == "version":
        db = connect(ro=True)
        print(f"盘上 {db.execute('PRAGMA user_version').fetchone()[0] if db else '没有'}，代码 {VERSION}")
        return 0
    if on_netfs(os.path.dirname(path())):
        print(f"taskq db: {path()} 在网络文件系统上（{fstype(os.path.dirname(path()))}），不建库", file=sys.stderr)
        return 1
    connect(create=True).close()
    print(f"{path()}：版本 {VERSION}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
