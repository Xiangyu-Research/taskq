"""Where one task queue keeps its records (taskq.py reads and judges them the same way in both):

  FileStore: a queue dir (proto.QUEUE): every executor, a queue named by TASKQ_DIR, and the hub before format 3.
  DbStore:   the hub database's queue tables (db.py schema 5): the hub from format 3 on (self migrate step 3 copies the
             queue dir in, then sets meta QUEUE_META; hub_db() picks it from then on).

Both keep the same records under the same names (proto.safe(key)): a record file there is a row here, its text the row's
body, so a record reads the same whichever store holds it. The hub queue's master switch is the file ENABLED there and a
"queue" row in the switch table here (off when its value is "off").
"""
import contextlib
import glob
import json
import os
import shutil
import threading
import time

import proto
from common import atomic_write, write_json

Q = proto.QUEUE
safe = proto.safe
ONE = ("stopped", "inputs", "released", "failtail")  # one record per task (mark rows)
MANY = ("failures", "skips")                          # lines added per task (line rows)
COLS = ("key", "ord", "nice", "exp", "recipe", "params", "spec", "added")


class FileStore:
    db = False

    def __init__(self, d):
        self.d = d

    def path(self, *a):
        return os.path.join(self.d, *a)

    def where(self):
        return self.path(Q["tasks"])

    def exists(self):
        return os.path.exists(self.where())

    # ---- tasks ----
    def read_tasks(self, path=None):
        """-> (tasks, unreadable lines [(line number, text)])."""
        path = path or self.where()
        out, bad = [], []
        if os.path.exists(path):
            for n, ln in enumerate(open(path, encoding="utf-8"), 1):
                if ln.startswith("#") or not ln.strip():
                    continue
                try:
                    t = json.loads(ln)
                    t["order"], t["nice"] = int(t["order"]), int(t["nice"])
                    t.setdefault("exp", None)
                    t.setdefault("params", {})
                    out.append(t)
                except (ValueError, KeyError, TypeError):
                    bad.append((n, ln.strip()))
        return out, bad

    def write_tasks(self, recs, path=None):
        atomic_write(path or self.where(), "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in recs))

    # ---- claims ----
    def _cd(self, key):
        return self.path(Q["claims"], safe(key))

    def claim(self, key):
        try:
            return json.load(open(os.path.join(self._cd(key), proto.CLAIM_INFO)))
        except Exception:  # noqa: BLE001
            return None

    def claim_made(self, key):
        """When the claim was made (its dir), None when there is none."""
        try:
            return os.stat(self._cd(key)).st_mtime
        except OSError:
            return None

    def no_claim(self, key):
        """True: certainly no claim; False: there is one, or the claims cannot be read."""
        try:
            return safe(key) not in os.listdir(self.path(Q["claims"]))
        except OSError:
            return False

    def claim_names(self):
        d = self.path(Q["claims"])
        return set(os.listdir(d)) if os.path.isdir(d) else set()

    def claim_new(self, key, info):
        """Make the claim unless one exists -> made?"""
        d = self._cd(key)
        try:
            os.makedirs(os.path.dirname(d), exist_ok=True)
            os.mkdir(d)
        except FileExistsError:
            return False
        write_json(os.path.join(d, proto.CLAIM_INFO), info, indent=None)
        return True

    def claim_clear(self, key):
        """Take away a stale claim -> done? (False: it could not be moved away; someone else may be on it)."""
        d = self._cd(key)
        gone = f"{d}.old{os.getpid()}"
        try:
            os.rename(d, gone)
        except OSError:
            return False
        shutil.rmtree(gone, ignore_errors=True)
        return True

    def claim_put(self, key, info):
        write_json(os.path.join(self._cd(key), proto.CLAIM_INFO), info, indent=None)

    def claim_drop(self, key):
        shutil.rmtree(self._cd(key), ignore_errors=True)

    # ---- records ----
    def get(self, kind, key):
        try:
            return open(self.path(Q[kind], safe(key)), encoding="utf-8").read()
        except (OSError, ValueError):
            return None

    def put(self, kind, key, text):
        os.makedirs(self.path(Q[kind]), exist_ok=True)
        if kind == "failtail":
            atomic_write(self.path(Q[kind], safe(key)), text)
            return
        with open(self.path(Q[kind], safe(key)), "w", encoding="utf-8") as f:
            f.write(text)

    def drop(self, kind, key):
        try:
            os.remove(self.path(Q[kind], safe(key)))
            return True
        except FileNotFoundError:
            return False

    def names(self, kind):
        try:
            return set(os.listdir(self.path(Q[kind])))
        except OSError:
            return set()

    def add_line(self, kind, key, line):
        os.makedirs(self.path(Q[kind]), exist_ok=True)
        with open(self.path(Q[kind], safe(key)), "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def lines(self, kind, key=None):
        out = []
        for p in glob.glob(self.path(Q[kind], safe(key) if key else "*")):
            out += open(p, encoding="utf-8").read().splitlines()
        return out

    def keep_lines(self, kind, key, keep):
        """Keep only the lines of key's record for which keep(line) -> (lines before, after); no record -> None."""
        p = self.path(Q[kind], safe(key))
        try:
            lines = open(p, encoding="utf-8").read().splitlines()
        except OSError:
            return None
        kept = [ln for ln in lines if keep(ln)]
        if kept:
            atomic_write(p, "".join(ln + "\n" for ln in kept))
        else:
            os.remove(p)
        return len(lines), len(kept)

    def forget(self, key, kinds):
        """Drop key's records of these kinds and its claim (taskq.py remove)."""
        for kind in kinds:
            self.drop(kind, key)
        self.claim_drop(key)

    def enabled(self):
        return os.path.exists(self.path(Q["enabled"]))

    # ---- smoke records (hub; smoke_gate.py) ----
    def smoke_get(self, site):
        try:
            with open(self.path(proto.SMOKE_DIR, f"{site}.json")) as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def smoke_put(self, site, recs):
        os.makedirs(self.path(proto.SMOKE_DIR), exist_ok=True)
        write_json(self.path(proto.SMOKE_DIR, f"{site}.json"), recs)

    def smoke_sites(self):
        return sorted(f[:-5] for f in self.names_in(proto.SMOKE_DIR) if f.endswith(".json"))

    def names_in(self, sub):
        try:
            return os.listdir(self.path(sub))
        except OSError:
            return []


class DbStore:
    db = True

    def __init__(self, path=None):
        import db as D
        self.p = path or D.path()
        self._local = threading.local()

    def conn(self):
        c = getattr(self._local, "c", None)
        if c is None:
            import db as D
            c = D.connect(create=True, p=self.p)
            if c is None:
                raise SystemExit(f"cannot open the hub database {self.p}")
            self._local.c = c
        return c

    def q(self, sql, args=()):
        return self.conn().execute(sql, args)

    @contextlib.contextmanager
    def tx(self):
        """One write transaction (BEGIN IMMEDIATE ... COMMIT, ROLLBACK on error); inside one already, part of it."""
        c = self.conn()
        if c.in_transaction:
            yield c
            return
        c.execute("BEGIN IMMEDIATE")
        try:
            yield c
        except BaseException:
            c.execute("ROLLBACK")
            raise
        c.execute("COMMIT")

    def where(self):
        return f"{self.p} (queue tables)"

    def exists(self):
        return True

    # ---- tasks ----
    def read_tasks(self, path=None):
        out = []
        for key, o, n, exp, recipe, params, spec, added in self.q(f"SELECT {','.join(COLS)} FROM task"):
            t = dict(order=o, nice=n, key=key, exp=exp, recipe=recipe, params=json.loads(params), added=added)
            if spec is not None:
                t["spec"] = json.loads(spec)
            out.append(t)
        return out, []

    @staticmethod
    def task_row(r):
        return (r["key"], int(r["order"]), int(r["nice"]), r.get("exp"), r.get("recipe"),
                json.dumps(r.get("params"), ensure_ascii=False, sort_keys=True),
                json.dumps(r["spec"], ensure_ascii=False, sort_keys=True) if r.get("spec") else None, r.get("added"))

    def write_tasks(self, recs, path=None):
        """Replace the queue with recs: rows that changed are written, rows not in recs deleted."""
        rows = {r["key"]: self.task_row(r) for r in recs}
        with self.tx() as c:
            have = {row[0]: tuple(row) for row in c.execute(f"SELECT {','.join(COLS)} FROM task")}
            c.executemany("DELETE FROM task WHERE key=?", [(k,) for k in set(have) - set(rows)])
            c.executemany(f"INSERT OR REPLACE INTO task({','.join(COLS)}) VALUES({','.join('?' * len(COLS))})",
                          [r for k, r in rows.items() if have.get(k) != r])

    # ---- claims ----
    def claim(self, key):
        r = self.q("SELECT info FROM claim WHERE name=?", (safe(key),)).fetchone()
        try:
            return json.loads(r[0]) if r and r[0] is not None else None
        except ValueError:
            return None

    def claim_made(self, key):
        r = self.q("SELECT t FROM claim WHERE name=?", (safe(key),)).fetchone()
        return r[0] if r else None

    def no_claim(self, key):
        return self.q("SELECT 1 FROM claim WHERE name=?", (safe(key),)).fetchone() is None

    def claim_names(self):
        return {r[0] for r in self.q("SELECT name FROM claim")}

    def claim_new(self, key, info):
        return self.q("INSERT OR IGNORE INTO claim(name, info, t) VALUES(?,?,?)",
                      (safe(key), json.dumps(info, ensure_ascii=False), time.time())).rowcount == 1

    def claim_clear(self, key):
        return self.q("DELETE FROM claim WHERE name=?", (safe(key),)).rowcount > 0

    def claim_put(self, key, info):
        self.q("INSERT INTO claim(name, info, t) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET info=excluded.info",
               (safe(key), json.dumps(info, ensure_ascii=False), time.time()))

    def claim_drop(self, key):
        self.claim_clear(key)

    # ---- records ----
    def get(self, kind, key):
        r = self.q("SELECT body FROM mark WHERE kind=? AND name=?", (kind, safe(key))).fetchone()
        return r[0] if r else None

    def put(self, kind, key, text):
        self.q("INSERT OR REPLACE INTO mark(kind, name, body, t) VALUES(?,?,?,?)", (kind, safe(key), text, time.time()))

    def drop(self, kind, key):
        return self.q("DELETE FROM mark WHERE kind=? AND name=?", (kind, safe(key))).rowcount > 0

    def names(self, kind):
        return {r[0] for r in self.q("SELECT name FROM mark WHERE kind=?", (kind,))}

    def add_line(self, kind, key, line):
        self.q("INSERT INTO line(kind, name, body, t) VALUES(?,?,?,?)", (kind, safe(key), line, time.time()))

    def lines(self, kind, key=None):
        if key:
            rows = self.q("SELECT body FROM line WHERE kind=? AND name=? ORDER BY id", (kind, safe(key)))
        else:
            rows = self.q("SELECT body FROM line WHERE kind=? ORDER BY name, id", (kind,))
        return [ln for r in rows for ln in r[0].splitlines()]

    def keep_lines(self, kind, key, keep):
        with self.tx() as c:
            rows = c.execute("SELECT id, body FROM line WHERE kind=? AND name=? ORDER BY id", (kind, safe(key))).fetchall()
            if not rows:
                return None
            gone = [(i,) for i, body in rows if not keep(body)]
            c.executemany("DELETE FROM line WHERE id=?", gone)
        return len(rows), len(rows) - len(gone)

    def forget(self, key, kinds):
        with self.tx() as c:
            for kind in kinds:
                c.execute(f"DELETE FROM {'line' if kind in MANY else 'mark'} WHERE kind=? AND name=?", (kind, safe(key)))
            c.execute("DELETE FROM claim WHERE name=?", (safe(key),))

    def enabled(self):
        return self.q("SELECT 1 FROM switch WHERE name='queue' AND target='' AND value='off' AND (until IS NULL OR until > ?)",
                      (time.time(),)).fetchone() is None

    # ---- smoke records ----
    def smoke_get(self, site):
        r = self.q("SELECT body FROM smoke WHERE site=?", (site,)).fetchone()
        try:
            return json.loads(r[0]) if r else {}
        except ValueError:
            return {}

    def smoke_put(self, site, recs):
        self.q("INSERT OR REPLACE INTO smoke(site, body) VALUES(?,?)", (site, json.dumps(recs, ensure_ascii=False, indent=1)))

    def smoke_sites(self):
        return sorted(r[0] for r in self.q("SELECT site FROM smoke"))


QUEUE_META = "queue_store"  # meta row: "db" once the hub queue lives in the database (self migrate step 3)
DB_FROM = 3                 # hub state stamped with this format or later keeps its queue in the database (a new hub from the start)


def hub_db():
    """The hub queue's DbStore when this process works on the hub queue (no TASKQ_DIR, no executor root) and the hub
    database holds it (meta QUEUE_META, or the hub stamped DB_FROM or later); otherwise None (the queue dir). TASKQ_QSTORE=db
    forces it (tests)."""
    if os.environ.get("TASKQ_QSTORE") == "db":
        return DbStore()
    if os.environ.get("TASKQ_DIR") or os.environ.get("TASKQ_ROOT") or os.environ.get("TASKQ_QSTORE") == "files":
        return None
    try:
        import db as D
        c = D.connect(ro=True)
    except Exception:  # noqa: BLE001  no hub database here (an executor's bundle has no db.py)
        return None
    try:
        on = c is not None and D.has(c, "task") and D.get_meta(c, QUEUE_META) == "db"
    finally:
        if c is not None:
            c.close()
    if not on:
        import common as C
        on = (C.format_on_disk() or 0) >= DB_FROM
    return DbStore() if on else None
