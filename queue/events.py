"""The event log: one table (log) in the hub database for everything the scheduler and people record.

    taskq events [--since T] [--until T] [--key K] [--site S] [--source S] [--kind K] [--level L] [--grep RE] [-n N] [-f] [--json]
    taskq note "<text>" [--key K] [--site S] [--kind K] [--at T] [--uid U]
    taskq internal import-logs [name…]

add() never raises: when the database is busy or broken the event goes to a spool file next to it, imported on the next tick
(flush()). Where there is no database (executors) nothing is stored. Until the old text logs are retired, sources listed in
proto.LEGACY_LOGS also get their line appended to the old file (cfg.log(source)).
Times (--since/--until/--at): "YYYY-MM-DD[ HH:MM[:SS]]", or relative "30m", "2h", "3d".
"""
import argparse
import hashlib
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
import db as D  # noqa: E402

COLS = ("t", "source", "kind", "level", "key", "site", "msg", "data", "uid")


def spool_path():
    return D.path() + ".spool"


def _row(source, msg, kind=None, level=None, key=None, site=None, data=None, t=None, uid=None):
    return dict(t=time.time() if t is None else float(t), source=source, kind=kind, level=level, key=key, site=site, msg=str(msg),
                data=json.dumps(data, ensure_ascii=False) if data is not None else None, uid=uid)


def _insert(db, rows):
    with D.tx(db):
        db.executemany(f"INSERT OR IGNORE INTO log({','.join(COLS)}) VALUES({','.join('?' * len(COLS))})",
                       [tuple(r[c] for c in COLS) for r in rows])


def _legacy(rows):
    by = {}
    for r in rows:
        if r["source"] in proto.LEGACY_LOGS and r.get("uid") is None:
            by.setdefault(r["source"], []).append(r)
    for src, rs in by.items():
        try:
            with open(legacy_path(proto.LEGACY_LOGS[src]), "a") as f:
                f.writelines(f"{time.strftime('%F %T', time.localtime(r['t']))} {r['msg']}\n" for r in rs)
        except OSError:
            pass


def add_many(rows, db=None):
    """rows: from _row(). Never raises."""
    rows = list(rows)
    if not rows:
        return
    _legacy(rows)
    own = db is None
    try:
        db = db or D.connect()
        if db is None:
            return
        if not D.has(db, "log"):
            raise OSError("database not migrated")
        _insert(db, rows)
    except Exception:  # noqa: BLE001
        try:
            with open(spool_path(), "a") as f:
                f.writelines(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        except OSError:
            pass
    finally:
        if own and db is not None:
            db.close()


def add(source, msg, db=None, **kw):
    add_many([_row(source, msg, **kw)], db)


def lines(source, msgs, db=None, **kw):
    """Several plain lines from one source at the same time."""
    now = time.time()
    add_many([_row(source, m, t=now, **kw) for m in msgs], db)


def flush(db):
    """Import the spool (hub tick, after connect(create=True))."""
    p = spool_path()
    if not os.path.exists(p):
        return 0
    tmp = f"{p}.{os.getpid()}"
    os.replace(p, tmp)
    rows = []
    for ln in open(tmp, errors="replace"):
        try:
            rows.append(json.loads(ln))
        except ValueError:
            continue
    try:
        _insert(db, rows)
    except Exception:  # noqa: BLE001
        with open(p, "a") as f:
            f.writelines(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        raise
    finally:
        os.remove(tmp)
    return len(rows)


# ---------------- reading ----------------
def when(s, now=None):
    """'2h' / '3d' / '30m' / 'YYYY-MM-DD[ HH:MM[:SS]]' → epoch seconds."""
    now = now or time.time()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([mhd])", s.strip())
    if m:
        return now - float(m.group(1)) * dict(m=60, h=3600, d=86400)[m.group(2)]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return time.mktime(time.strptime(s.strip(), fmt))
        except ValueError:
            continue
    raise ValueError(f"not a time: {s!r} (YYYY-MM-DD[ HH:MM], or 30m / 2h / 3d)")


def ahead(s, now=None):
    """'2h' / '3d' / '30m' from now, or an absolute time → epoch seconds."""
    now = now or time.time()
    return 2 * now - when(s, now) if re.fullmatch(r"\d+(?:\.\d+)?[mhd]", s.strip()) else when(s, now)


def query(db, since=None, until=None, key=None, site=None, source=None, kind=None, level=None, grep=None, limit=None, after_id=None):
    """→ [row dict with id] oldest first. key matches the key column, or (rows with no key, e.g. imported lines) the text."""
    if not D.has(db, "log"):
        return []
    w, a = [], []
    for col, v in (("site", site), ("source", source), ("kind", kind), ("level", level)):
        if v:
            w.append(f"{col}=?")
            a.append(v)
    if since is not None:
        w.append("t>=?")
        a.append(since)
    if until is not None:
        w.append("t<?")
        a.append(until)
    if after_id is not None:
        w.append("id>?")
        a.append(after_id)
    if key:
        w.append("(key=? OR (key IS NULL AND instr(msg, ?)>0))")
        a += [key, key]
    sql = f"SELECT id,{','.join(COLS)} FROM log" + (" WHERE " + " AND ".join(w) if w else "") + " ORDER BY t DESC, id DESC"
    pat = re.compile(grep) if grep else None
    kpat = re.compile(re.escape(key) + r"(?![\w/])") if key else None
    out = []
    for r in db.execute(sql, a):
        d = dict(zip(("id",) + COLS, r))
        if kpat and d["key"] != key and not kpat.search(d["msg"]):
            continue
        if pat and not pat.search(d["msg"]):
            continue
        out.append(d)
        if limit and len(out) >= limit:
            break
    return out[::-1]


def fmt(d):
    where = " ".join(x for x in (d.get("site"), d.get("key")) if x)
    tag = d["source"] + (f"/{d['kind']}" if d.get("kind") else "")
    lvl = f" [{d['level']}]" if d.get("level") and d["level"] not in ("info",) else ""
    return f"{time.strftime('%F %T', time.localtime(d['t']))}  {tag:16}{lvl} {where + ': ' if where and where not in d['msg'] else ''}{d['msg']}"


def as_json(d):
    out = {k: d.get(k) for k in proto.EVENT_FIELDS}
    out["time"] = time.strftime("%F %T", time.localtime(d["t"]))
    out["data"] = json.loads(d["data"]) if d.get("data") else None
    return out


def ledger_rows(since=None, key=None, limit=None):
    """Flow-ledger events (flow/ledger.py's own table) in the same shape, source "ledger"."""
    try:
        from flow import ledger as LG
        if not os.path.exists(LG.db_path()):
            return []
        lg = LG.connect()
    except Exception:  # noqa: BLE001
        return []
    try:
        w, a = [], []
        if since is not None:
            w.append("t>=?")
            a.append(since)
        if key:
            w.append("inst=?")
            a.append(key)
        sql = "SELECT t, inst, path, kind, level, detail FROM event" + (" WHERE " + " AND ".join(w) if w else "") + " ORDER BY t DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [dict(id=None, t=t, source="ledger", kind=k, level=lv, key=i, site=None, msg=f"{k} {p}", data=dt, uid=None)
                for t, i, p, k, lv, dt in lg.execute(sql, a)][::-1]
    except Exception:  # noqa: BLE001
        return []
    finally:
        lg.close()


def cmd_events(argv):
    ap = argparse.ArgumentParser(prog="taskq events", description="The event log (scheduler, health check, notes)")
    ap.add_argument("--since", help="from (default: 24h ago; 'all' for everything)")
    ap.add_argument("--until")
    ap.add_argument("--key")
    ap.add_argument("--site")
    ap.add_argument("--source", help="relay, feed, advance, shift, health, ledger, engine, note, …")
    ap.add_argument("--kind")
    ap.add_argument("--level")
    ap.add_argument("--grep", help="regex on the text")
    ap.add_argument("-n", type=int, help="only the last N")
    ap.add_argument("-f", "--follow", action="store_true", help="keep printing new events")
    ap.add_argument("--json", action="store_true", help="one JSON object per line (proto.EVENT_FIELDS)")
    a = ap.parse_args(argv)
    try:
        since = None if a.since == "all" else when(a.since) if a.since else (None if a.n or a.key else time.time() - 86400)
        until = when(a.until) if a.until else None
        if a.grep:
            re.compile(a.grep)
    except (ValueError, re.error) as e:
        print(f"taskq events: {e}", file=sys.stderr)
        return 2
    db = D.connect(ro=True)
    if db is None:
        print(f"taskq events: no database at {D.path()} (created by the first tick on the hub)", file=sys.stderr)
        return 1
    q = dict(since=since, until=until, key=a.key, site=a.site, source=a.source, kind=a.kind, level=a.level, grep=a.grep)
    rows = query(db, limit=a.n, **q)
    if a.source in (None, "ledger") and not (a.site or a.kind or a.level or a.grep or a.until):
        rows = sorted(rows + ledger_rows(since, a.key, a.n), key=lambda d: d["t"])
        if a.n:
            rows = rows[-a.n:]
    show = (lambda d: print(json.dumps(as_json(d), ensure_ascii=False))) if a.json else (lambda d: print(fmt(d)))
    for d in rows:
        show(d)
    if not a.follow:
        return 0
    last = max([d["id"] for d in rows if d["id"]] or [db.execute("SELECT coalesce(max(id),0) FROM log").fetchone()[0]])
    db.close()
    try:
        while True:
            time.sleep(2)
            db = D.connect(ro=True)
            new = query(db, after_id=last, **{**q, "since": None})
            db.close()
            for d in new:
                show(d)
                last = max(last, d["id"])
            sys.stdout.flush()
    except KeyboardInterrupt:
        return 0


def cmd_note(argv):
    ap = argparse.ArgumentParser(prog="taskq note", description="Write a note into the event log (what went wrong, what was done)")
    ap.add_argument("text", nargs="*", help="the note (read from stdin when not given)")
    ap.add_argument("--key", help="task key it is about")
    ap.add_argument("--site", help="machine it is about")
    ap.add_argument("--kind", default="note", help="e.g. incident, fix, change, decision, remark (default: note)")
    ap.add_argument("--at", help="when it happened (default: now)")
    ap.add_argument("--uid", help="unique id: a second note with the same uid is ignored (for imports)")
    ap.add_argument("--source", default="note", help=argparse.SUPPRESS)
    a = ap.parse_args(argv)
    text = " ".join(a.text).strip()
    if not a.text and not sys.stdin.isatty():
        text = sys.stdin.read().strip()
    if not text:
        print("taskq note: no text", file=sys.stderr)
        return 2
    try:
        t = when(a.at) if a.at else None
    except ValueError as e:
        print(f"taskq note: {e}", file=sys.stderr)
        return 2
    db = D.connect()
    if db is None or not D.has(db, "log"):
        print(f"taskq note: no database at {D.path()} (run on the hub; `taskq db migrate` creates it)", file=sys.stderr)
        return 1
    n = db.total_changes
    _insert(db, [_row(a.source, text, kind=a.kind, key=a.key, site=a.site, t=t, uid=a.uid)])
    added = db.total_changes > n
    db.close()
    print("noted" if added else f"already there (uid {a.uid})")
    return 0


# ---------------- importing the old text logs ----------------
LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:\.\d+)?:? ?(.*)$")


def import_file(db, source, p):
    """Lines 'YYYY-MM-DD HH:MM:SS text' → events (uid = hash of source, line, occurrence: re-running adds nothing)."""
    seen, rows = {}, []
    with open(p, errors="replace") as f:
        for ln in f:
            ln = ln.rstrip("\n")
            m = LINE.match(ln)
            if not m or not m.group(2).strip():
                if rows and ln.strip():  # continuation line
                    rows[-1]["msg"] += "\n" + ln
                continue
            n = seen[ln] = seen.get(ln, 0) + 1
            uid = hashlib.sha1(f"{source}\0{ln}\0{n}".encode()).hexdigest()
            rows.append(_row(source, m.group(2).strip(), t=time.mktime(time.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")), uid=uid))
    n0 = db.total_changes
    _insert(db, rows)
    return db.total_changes - n0


def cmd_import(argv):
    ap = argparse.ArgumentParser(prog="taskq internal import-logs", description="Import the old text logs into the event log (idempotent)")
    ap.add_argument("sources", nargs="*", help=f"default: {' '.join(proto.LEGACY_LOGS)}")
    a = ap.parse_args(argv)
    db = D.connect(create=True)
    if db is None:
        print(f"taskq: cannot open {D.path()}", file=sys.stderr)
        return 1
    for src in a.sources or proto.LEGACY_LOGS:
        p = legacy_path(proto.LEGACY_LOGS.get(src, src))
        if not os.path.exists(p):
            print(f"{src}: no {p}")
            continue
        before = db.total_changes
        n = import_file(db, src, p)
        print(f"{src}: {n} lines read from {p}, {db.total_changes - before} new")
    return 0


def legacy_path(name):
    """Old text log file by its cfg.log name; health.log lives in the health state dir (unless HEALTH_LOG is set)."""
    if name == "health" and not os.environ.get("HEALTH_LOG"):
        return os.path.join(os.path.expanduser(cfg.outputs("health") or cfg.state("health", expand=False)), "health.log")
    return cfg.log(name)


# ---------------- progress snapshots ----------------
def snapshot(db, tasks, t=None):
    """tasks: [dict(key, state, progress, site, data)] → a row for each task whose state, progress or site changed. → rows added."""
    t = time.time() if t is None else t
    last = {k: (s, p, w) for k, s, p, w in db.execute(
        "SELECT key, state, progress, site FROM progress p WHERE t=(SELECT max(t) FROM progress WHERE key=p.key)")}
    rows = [(t, x["key"], x.get("state"), x.get("progress"), x.get("site"), json.dumps(x.get("data"), ensure_ascii=False))
            for x in tasks if last.get(x["key"]) != (x.get("state"), x.get("progress"), x.get("site"))]
    with D.tx(db):
        db.executemany("INSERT OR REPLACE INTO progress(t, key, state, progress, site, data) VALUES(?, ?, ?, ?, ?, ?)", rows)
    return len(rows)


def progress_rows(db, key=None, since=None):
    w, a = [], []
    if key:
        w.append("key=?")
        a.append(key)
    if since is not None:
        w.append("t>=?")
        a.append(since)
    sql = "SELECT t, key, state, progress, site, data FROM progress" + (" WHERE " + " AND ".join(w) if w else "") + " ORDER BY t, key"
    return [dict(time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r[0])), t=r[0], key=r[1], state=r[2], progress=r[3], site=r[4],
                 data=json.loads(r[5]) if r[5] else None) for r in db.execute(sql, a)] if D.has(db, "progress") else []


def cmd_progress(argv):
    ap = argparse.ArgumentParser(prog="taskq progress", description="Task progress over time (a row whenever state, progress or site changed)")
    ap.add_argument("--key", help="one task")
    ap.add_argument("--since", help="from (YYYY-MM-DD[ HH:MM], or 30m / 2h / 3d ago; default: everything)")
    ap.add_argument("--json", action="store_true", help="one JSON line (proto.PROGRESS_TOP; each row proto.PROGRESS_FIELDS)")
    a = ap.parse_args(argv)
    try:
        since = when(a.since) if a.since else None
    except ValueError as e:
        print(f"taskq progress: {e}", file=sys.stderr)
        return 2
    db = D.connect(ro=True)
    rows = progress_rows(db, a.key, since) if db else []
    if a.json:
        print(json.dumps(dict(version=proto.VERSION, rows=rows), ensure_ascii=False))
        return 0
    for r in rows:
        p = f"{100 * r['progress']:.0f}%" if r["progress"] is not None else "-"
        print(f"{r['time']}  {r['key']}  {r['state'] or '-'}  {p}  {r['site'] or ''}")
    return 0


def main(argv):
    if argv[:1] == ["progress"]:
        return cmd_progress(argv[1:])
    if argv[:1] == ["events"]:
        return cmd_events(argv[1:])
    if argv[:1] == ["note"]:
        return cmd_note(argv[1:])
    if argv[:1] == ["import"]:
        return cmd_import(argv[1:])
    print(__doc__.strip(), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
