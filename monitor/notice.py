#!/usr/bin/env python3
"""Health problems, notices and acks, kept in the hub database (queue/db.py).

    taskq problems [--all] [--json]             what the last health check found
    taskq notice set <name> [--at T] [text…]    put up a notice (text from stdin when not given); --at: only from then on
    taskq notice clear <name>…                  take notices down (missing ones are not an error)
    taskq notice list [--json]                  notices, including the ones waiting for their --at
    taskq notice ack <problem id or prefix>…    acknowledge a "something happened" problem (kind=event, e.g. boot:<machine>)

The health check reports each due notice as problem notice:<name> until it is cleared. Ongoing problems (disk full,
unreachable) cannot be acked; they go away once solved. Names: letters, digits, _ and -.
The first write on the hub takes over the old files (state.json, acked.json, notices/*.txt → *.migrated).
"""
import argparse
import glob
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import cfg  # noqa: E402
import proto  # noqa: E402
import db as D  # noqa: E402

TF = "%Y-%m-%d %H:%M:%S"


def health_dir():
    return os.path.expanduser(cfg.outputs("health") or cfg.state("health", expand=False))


def open_db(write=True):
    """Hub database → connection or None. write=True creates it and takes over the old files."""
    db = D.connect(create=write, ro=not write)
    if db is not None and write and D.writable(db):
        adopt_files(db)
    return db


def adopt_files(db, hdir=None):
    """Old health files → database, then renamed *.migrated."""
    hdir = hdir or health_dir()
    st, ak = os.path.join(hdir, "state.json"), os.path.join(hdir, proto.ACKED)
    nts = sorted(glob.glob(os.path.join(hdir, proto.NOTICES, "*.txt")))
    if not (os.path.exists(st) or os.path.exists(ak) or nts):
        return
    with D.tx(db):
        if os.path.exists(st):
            try:
                h = json.load(open(st, encoding="utf-8"))
            except ValueError:
                h = {}
            for pid, p in (h.get("problems") or {}).items():
                db.execute("INSERT OR IGNORE INTO problem(id, body) VALUES(?, ?)", (pid, json.dumps(p, ensure_ascii=False)))
            if h.get("checked") and D.get_meta(db, "checked") is None:
                D.set_meta(db, "checked", h["checked"])
        if os.path.exists(ak):
            try:
                for pid, t in json.load(open(ak, encoding="utf-8")).items():
                    db.execute("INSERT OR IGNORE INTO ack(id, t) VALUES(?, ?)", (pid, t))
            except ValueError:
                pass
        for p in nts:
            name = os.path.splitext(os.path.basename(p))[0]
            if valid(name):
                since = time.strftime(TF, time.localtime(os.path.getmtime(p)))
                text = open(p, encoding="utf-8").read().strip() or name
                db.execute("INSERT OR IGNORE INTO notice(name, text, since, at) VALUES(?, ?, ?, NULL)", (name, text, since))
    for p in [st, ak] + nts:
        if os.path.exists(p):
            os.replace(p, p + ".migrated")


# ---------------- problems ----------------
def problems(db):
    """→ ({id: problem dict}, time of the last check or None)."""
    if not D.has(db, "problem"):
        return {}, None
    return {pid: json.loads(b) for pid, b in db.execute("SELECT id, body FROM problem")}, D.get_meta(db, "checked")


def save_problems(db, cur, checked):
    """Replace the problem list (acked ones already left out)."""
    with D.tx(db):
        db.execute("DELETE FROM problem")
        db.executemany("INSERT INTO problem(id, body) VALUES(?, ?)", [(k, json.dumps(v, ensure_ascii=False)) for k, v in cur.items()])
        D.set_meta(db, "checked", checked)


def prune_acks(db, raised):
    """Acks of problems this check no longer raises at all are dropped (an acked one is still raised, just not listed)."""
    with D.tx(db):
        for pid in [k for k in acked(db) if k not in raised]:
            db.execute("DELETE FROM ack WHERE id=?", (pid,))


def acked(db):
    return dict(db.execute("SELECT id, t FROM ack")) if D.has(db, "ack") else {}


def ack(db, prefixes):
    """Problems of the last check with an id starting with one of prefixes and kind=event → acked. → (acked ids, rejected)."""
    probs, _ = problems(db)
    got, bad = [], []
    now = time.strftime(TF)
    with D.tx(db):
        for pre in prefixes:
            hit = [k for k in probs if k.startswith(pre)]
            ev = [k for k in hit if probs[k].get("kind") == "event"]
            if not ev:
                bad.append(pre + (" (ongoing problem: it goes away once solved)" if hit else " (no such problem)"))
            for k in ev:
                db.execute("INSERT INTO ack(id, t) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET t=excluded.t", (k, now))
                got.append(k)
    return got, bad


# ---------------- notices ----------------
def valid(name):
    return bool(re.fullmatch(proto.NOTICE_NAME, name or ""))


def notices(db, due_only=True, now=None):
    """→ {name: dict(text, since, at)} by name; due_only leaves out the ones whose --at is still ahead."""
    if not D.has(db, "notice"):
        return {}
    now = now or time.strftime(TF)
    return {name: dict(text=text, since=since, at=at)
            for name, text, since, at in db.execute("SELECT name, text, since, at FROM notice ORDER BY name")
            if not (due_only and at and at > now)}


def put(db, name, text, at=None):
    with D.tx(db):
        db.execute("INSERT INTO notice(name, text, since, at) VALUES(?, ?, ?, ?) ON CONFLICT(name) DO UPDATE "
                   "SET text=excluded.text, since=excluded.since, at=excluded.at", (name, text.strip(), time.strftime(TF), at))


def drop(db, name):
    with D.tx(db):
        return db.execute("DELETE FROM notice WHERE name=?", (name,)).rowcount > 0


# ---------------- command line ----------------
def problem_rows(probs, everything=False):
    """→ [proto.OVERVIEW_PROBLEM dicts] with the plain wording; by default only the ones that count now."""
    sys.path.insert(0, HERE)
    try:
        import plain as PL
    except Exception:  # noqa: BLE001
        PL = None
    rows = []
    for pid, p in sorted(probs.items()):
        if not everything and not (p.get("active") and p.get("level") != "info"):
            continue
        try:
            pl = PL.problem(pid, p) if PL else p.get("msg")
        except Exception:  # noqa: BLE001
            pl = p.get("msg")
        rows.append(dict(id=pid, **{k: p.get(k) for k in proto.OVERVIEW_PROBLEM if k not in ("id", "plain")}, plain=pl))
    return rows


def cmd_problems(argv):
    ap = argparse.ArgumentParser(prog="taskq problems", description="What the last health check found")
    ap.add_argument("--all", action="store_true", help="also info and the ones not reported yet (waiting to last long enough)")
    ap.add_argument("--json", action="store_true", help="one JSON line (proto.PROBLEMS_TOP; each problem proto.OVERVIEW_PROBLEM)")
    a = ap.parse_args(argv)
    db = open_db(write=False)
    probs, checked = problems(db) if db else ({}, None)
    rows = problem_rows(probs, a.all)
    if a.json:
        print(json.dumps(dict(version=proto.VERSION, checked=checked, problems=rows), ensure_ascii=False))
        return 0
    print(f"health check {checked or 'never ran'}: {len(rows)} problem(s)")
    for r in rows:
        print(f"  [{r['level']}] {r['id']} (since {r['first']})\n      {r['plain'] or r['msg']}")
    return 0


def main(argv):
    if argv[:1] == ["problems"]:
        return cmd_problems(argv[1:])
    ap = argparse.ArgumentParser(prog="taskq notice", description="Notices reported by the health check")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("set", help="put up a notice or change its text")
    p.add_argument("name")
    p.add_argument("text", nargs="*", help="the text (read from stdin when not given)")
    p.add_argument("--at", help="only report it from then on (YYYY-MM-DD[ HH:MM], or 2h / 3d from now)")
    p = sub.add_parser("clear", help="take notices down")
    p.add_argument("names", nargs="+")
    p = sub.add_parser("ack", help='acknowledge "something happened" problems (e.g. a reboot); no longer listed')
    p.add_argument("ids", nargs="+", metavar="ID", help="problem id or prefix (e.g. boot:hub)")
    p = sub.add_parser("list", help="notices")
    p.add_argument("--json", action="store_true", help="one JSON line (proto.NOTICE_TOP, NOTICE_FIELDS)")
    a = ap.parse_args(argv)
    bad = [n for n in ([a.name] if a.cmd == "set" else a.names if a.cmd == "clear" else []) if not valid(n)]
    if bad:
        print(f"names may only use letters, digits, _ and -: {', '.join(bad)}", file=sys.stderr)
        return 2
    if a.cmd == "list":
        db = open_db(write=False)
        got = notices(db, due_only=False) if db else {}
        if a.json:
            print(json.dumps(dict(version=proto.VERSION, notices=[dict(name=n, **v) for n, v in got.items()]), ensure_ascii=False))
            return 0
        for n, v in got.items():
            print(f"{n} ({v['since']}{', from ' + v['at'] if v['at'] else ''}): {v['text']}")
        if not got:
            print("no notices")
        return 0
    if a.cmd == "set":
        text = " ".join(a.text).strip()
        if not a.text and not sys.stdin.isatty():
            text = sys.stdin.read().strip()
        if not text:
            print("give the text (arguments or stdin)", file=sys.stderr)
            return 2
        at = None
        if a.at:
            import events
            try:
                at = time.strftime(TF, time.localtime(events.ahead(a.at)))
            except ValueError as e:
                print(e, file=sys.stderr)
                return 2
    db = open_db()
    if db is None or not D.writable(db):
        print(f"taskq: cannot write the database at {D.path()} (network filesystem, or made by a newer taskq)", file=sys.stderr)
        return 1
    if a.cmd == "set":
        put(db, a.name, text, at)
        print(f"set {a.name}{' from ' + at if at else ''}: {text}")
        return 0
    if a.cmd == "ack":
        got, rej = ack(db, a.ids)
        for k in got:
            print(f"acked {k} (not listed from the next health check on)")
        for b in rej:
            print(f"not acked: {b}", file=sys.stderr)
        return 0 if got else 1
    for n in a.names:
        print(f"cleared {n}" if drop(db, n) else f"no notice {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
