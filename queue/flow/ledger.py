"""台账（2026-11-05）：每个实例的每次尝试、产出的指纹，sqlite（标准库），在状态文件夹下（proto.LEDGER_DB）。
做完 = 最近一次尝试算做完（proto.A_DONE），并且标志文件的指纹对得上（文件：大小和 mtime_ns；目录：在就行）。
指纹是 (大小, mtime_ns)，不用 inode。表的列只加不改。

P1 只做影子：每轮健康检查拿现在的做完判断（advance.py 算过的，没算过的这里算）和台账比，
  旧的判断说做完、台账没有 → 记一次 legacy 尝试；
  两边都说做完、标志变了 → 这期间调度跑过它（或改它产出的实例）就记成新的一次，否则记一笔"有人改过"（只记一次）；
  台账说做完、旧的判断说没做完 → 记一笔（标志不见了是 error）。
说不清原因的写进 <状态文件夹>/shadow/ledger-<日期>.jsonl，一行一件。"""
import json
import os
import sqlite3
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import cfg  # noqa: E402
import proto as PR  # noqa: E402
import spec as S  # noqa: E402
from flow.model import SOURCE  # noqa: E402

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS instance(id TEXT PRIMARY KEY, rule TEXT, key TEXT, exp TEXT, first_seen REAL);
CREATE TABLE IF NOT EXISTS attempt(id INTEGER PRIMARY KEY AUTOINCREMENT, inst TEXT NOT NULL, state TEXT NOT NULL,
    place TEXT, machine TEXT, ref TEXT, code TEXT, bundle TEXT, start REAL, end REAL, source TEXT, note TEXT);
CREATE INDEX IF NOT EXISTS attempt_inst ON attempt(inst, id);
CREATE TABLE IF NOT EXISTS output(id INTEGER PRIMARY KEY AUTOINCREMENT, attempt INTEGER NOT NULL, inst TEXT NOT NULL,
    path TEXT NOT NULL, role TEXT, kind TEXT, size INTEGER, mtime_ns INTEGER, sha256 TEXT, state TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS output_inst ON output(inst, state);
CREATE INDEX IF NOT EXISTS output_path ON output(path, state);
CREATE TABLE IF NOT EXISTS input(attempt INTEGER NOT NULL, path TEXT NOT NULL, producer TEXT, size INTEGER, mtime_ns INTEGER);
CREATE TABLE IF NOT EXISTS event(id INTEGER PRIMARY KEY AUTOINCREMENT, t REAL, inst TEXT, path TEXT, kind TEXT, level TEXT,
    detail TEXT);
CREATE INDEX IF NOT EXISTS event_inst ON event(inst, kind);
"""
VERSION = "1"
LEVEL = {"missing": "error", "size": "warn", "mtime": "info"}


def db_path():
    return os.environ.get("TASKQ_LEDGER") or cfg.state(PR.LEDGER_DB)


def connect(path=None):
    p = path or db_path()
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    db = sqlite3.connect(p, timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(SCHEMA)
    db.execute("INSERT OR IGNORE INTO meta VALUES('version', ?)", (VERSION,))
    return db


# ---------------- 指纹 ----------------
def fingerprint(p):
    """→ dict(kind f|d|-, size, mtime_ns)；读不了（盘断开等）抛 OSError。"""
    try:
        st = os.stat(p)
    except FileNotFoundError:
        return dict(kind="-", size=None, mtime_ns=None)
    import stat
    if stat.S_ISDIR(st.st_mode):
        return dict(kind="d", size=None, mtime_ns=st.st_mtime_ns)
    return dict(kind="f", size=st.st_size, mtime_ns=st.st_mtime_ns)


def differ(was, now):
    """存的指纹 → 现在的：None（一样）、missing、size、mtime。目录只看在不在。"""
    if now["kind"] == "-":
        return None if was["kind"] == "-" else "missing"
    if was["kind"] == "d" or now["kind"] == "d":
        return None if was["kind"] == now["kind"] else "size"
    if was["size"] != now["size"]:
        return "size"
    return "mtime" if was["mtime_ns"] != now["mtime_ns"] else None


# ---------------- 旧的做完判断 ----------------
def legacy_ok(flow, i, root):
    """advance.py 用的判断：源头任务看说明的 done，别的看规则的 done。"""
    if flow.rules[i.rule].scope == SOURCE:
        return S.is_done(i.spec, root) if i.spec else False
    d = flow.rules[i.rule].done
    return bool(d and d.ok(root, i.names))


# ---------------- 读写 ----------------
def last_attempt(db, iid):
    return db.execute("SELECT * FROM attempt WHERE inst=? ORDER BY id DESC LIMIT 1", (iid,)).fetchone()


def marker_row(db, iid):
    return db.execute("SELECT * FROM output WHERE inst=? AND role='marker' AND state=? ORDER BY id DESC LIMIT 1",
                      (iid, PR.O_LIVE)).fetchone()


def record(db, i, state, source, root, note=None, t=None, **meta):
    """记一次尝试；做完的（A_DONE）同时记产出（标志和声明的产出，各 stat 一次），旧的产出记录改成 superseded。→ 尝试号。"""
    t = t or time.time()
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute("INSERT OR IGNORE INTO instance VALUES(?,?,?,?,?)", (i.id, i.rule, i.key, i.exp, t))
        cur = db.execute("INSERT INTO attempt(inst,state,place,machine,ref,code,bundle,start,end,source,note) "
                         "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                         (i.id, state, meta.get("place"), meta.get("machine"), meta.get("ref"), meta.get("code"),
                          meta.get("bundle"), meta.get("start"), t, source, note))
        aid = cur.lastrowid
        if state in PR.A_DONE:
            db.execute("UPDATE output SET state=? WHERE inst=? AND state=?", (PR.O_SUPERSEDED, i.id, PR.O_LIVE))
            rows = [("marker", i.marker)] if i.marker else []
            rows += [("output", o) for o in i.outputs if o != i.marker]
            for role, rel in rows:
                fp = fingerprint(os.path.join(root, rel))
                db.execute("INSERT INTO output(attempt,inst,path,role,kind,size,mtime_ns,state) VALUES(?,?,?,?,?,?,?,?)",
                           (aid, i.id, rel, role, fp["kind"], fp["size"], fp["mtime_ns"], PR.O_LIVE))
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    return aid


def begin(db, i, t=None, **meta):
    """taskq run（或发布）开始：记一次 running 的尝试 → 尝试号。"""
    t = t or time.time()
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute("INSERT OR IGNORE INTO instance VALUES(?,?,?,?,?)", (i.id, i.rule, i.key, i.exp, t))
        aid = db.execute("INSERT INTO attempt(inst,state,place,machine,ref,code,bundle,start,source,note) VALUES(?,?,?,?,?,?,?,?,?,?)",
                         (i.id, PR.A_RUNNING, meta.get("place"), meta.get("machine"), meta.get("ref"), meta.get("code"),
                          meta.get("bundle"), t, meta.get("source") or "run", meta.get("note"))).lastrowid
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    return aid


def finish(db, aid, i, state, root, files=(), note=None, t=None, retire=()):
    """taskq run 结束：尝试改成 state；做完的记产出（标志、声明的产出，加上这次实际写了的文件 files，相对数据目录）。
    retire：发布时归档了的路径（相对数据目录），同一个事务里改成 retired。"""
    t = t or time.time()
    db.execute("BEGIN IMMEDIATE")
    try:
        for g in retire:
            db.execute("UPDATE output SET state=? WHERE path=? AND state=?", (PR.O_RETIRED, g, PR.O_LIVE))
        db.execute("UPDATE attempt SET state=?, end=?, note=coalesce(?, note) WHERE id=?", (state, t, note, aid))
        if state in PR.A_DONE:
            db.execute("UPDATE output SET state=? WHERE inst=? AND state=?", (PR.O_SUPERSEDED, i.id, PR.O_LIVE))
            rows = [("marker", i.marker)] if i.marker else []
            rows += [("output", o) for o in i.outputs if o != i.marker]
            seen = {r for _, r in rows}
            rows += [("file", f) for f in files if f not in seen]
            for role, rel in rows:
                fp = fingerprint(os.path.join(root, rel))
                db.execute("INSERT INTO output(attempt,inst,path,role,kind,size,mtime_ns,state) VALUES(?,?,?,?,?,?,?,?)",
                           (aid, i.id, rel, role, fp["kind"], fp["size"], fp["mtime_ns"], PR.O_LIVE))
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


def event(db, iid, path, kind, level, detail):
    db.execute("INSERT INTO event(t,inst,path,kind,level,detail) VALUES(?,?,?,?,?,?)",
               (time.time(), iid, path, kind, level, json.dumps(detail, ensure_ascii=False, sort_keys=True)))


def seen_event(db, iid, kind, detail):
    return db.execute("SELECT 1 FROM event WHERE inst=? AND kind=? AND detail=? LIMIT 1",
                      (iid, kind, json.dumps(detail, ensure_ascii=False, sort_keys=True))).fetchone() is not None


def done(db, i, root):
    """→ (做完没有, 不一样的地方 None|missing|size|mtime)。"""
    a = last_attempt(db, i.id)
    if not a or a["state"] not in PR.A_DONE:
        return False, None
    m = marker_row(db, i.id)
    if not m:
        return True, None
    d = differ(dict(m), fingerprint(os.path.join(root, m["path"])))
    return d in (None, "mtime"), d


# ---------------- adopt、verify、accept ----------------
def adopt(flow, root, db, dry=False, ids=None):
    """现在的判断说做完、台账里还没有做完的 → 记成 adopted。只 stat、不重跑，重复跑结果一样。→ {adopted, already, not_done}。"""
    n = dict(adopted=0, already=0, not_done=0)
    for i in flow.instances.values():
        if ids and i.id not in ids:
            continue
        a = last_attempt(db, i.id)
        if a and a["state"] in PR.A_DONE:
            n["already"] += 1
            continue
        if not legacy_ok(flow, i, root):
            n["not_done"] += 1
            continue
        n["adopted"] += 1
        if not dry:
            record(db, i, PR.A_ADOPTED, "adopt", root)
    return n


def verify(flow, root, db, ids=None):
    """台账说做完的，产出现在还对不对 → [dict(id, path, what, level)]。"""
    out = []
    for i in flow.instances.values():
        if ids and i.id not in ids:
            continue
        a = last_attempt(db, i.id)
        if not a or a["state"] not in PR.A_DONE:
            continue
        for r in db.execute("SELECT * FROM output WHERE inst=? AND state=?", (i.id, PR.O_LIVE)):
            d = differ(dict(r), fingerprint(os.path.join(root, r["path"])))
            if d:
                out.append(dict(id=i.id, path=r["path"], what=d, level=LEVEL[d]))
    return out


def accept(flow, graph, root, db, target, reason):
    """有意改过的：按现在的样子重新记下（实例 id，或路径 → 它的实例），写明原因。→ [实例 id]。"""
    if target in flow.instances:
        ids = [target]
    else:
        rel = os.path.relpath(os.path.realpath(target), os.path.realpath(root)) if os.path.isabs(target) else target
        w, _ = graph.who(rel)
        ids = [] if w is None else (w if isinstance(w, list) else [w])
    for x in ids:
        i = flow.instances[x]
        record(db, i, PR.A_OK if legacy_ok(flow, i, root) else PR.A_FAILED, "accept", root, note=reason)
        event(db, x, target, "accept", "info", dict(reason=reason))
    return ids


# ---------------- 影子 ----------------
def _ts(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(v).timestamp()
    except ValueError:
        return None


def activity(s, hub, identify):
    """hub 的 pueue 里调度跑过、在跑的 → {(步骤, key): 最近结束的时刻（在跑 = inf）}。"""
    out = {}
    tasks = ((s.get("pueue") or {}).get(hub) or {}).get("tasks") or []
    for t in tasks:
        r = identify(hub, t, s.get("records") or [])
        if not r:
            continue
        end = float("inf") if t.get("status") in ("Running", "Queued", "Paused") else (_ts(t.get("end")) or 0)
        k = (r["step"], r["key"])
        out[k] = max(out.get(k, 0), end)
    return out


def shadow(flow, graph, root, db, s=None, seen=None, hub=None, identify=None, budget=60, day=None):
    """一轮影子对比（见开头）。seen：advance.py 这一轮算过的 {(步骤, key) 或 key: 做完没有}。→ dict(各类的个数, unexplained=[…])。"""
    t0 = time.time()
    seen = seen or {}
    act = activity(s, hub, identify) if s and hub and identify else {}
    by_marker = {}
    for j in flow.instances.values():
        for m in j.mutates:
            by_marker.setdefault(m, []).append(j.id)
    n = dict(agree=0, new=0, rerun=0, changed=0, regressed=0, skipped=0)
    bad = []

    def ran_since(i, since):
        keys = [(i.rule, i.key), ("relay_fetch", i.qk or i.key)]
        for m, js in by_marker.items():
            if i.marker and (i.marker == m or i.marker.startswith(m + "/")):
                keys += [(flow.instances[j].rule, flow.instances[j].key) for j in js]
        return any(act.get(k, 0) > since for k in keys)

    for i in flow.instances.values():
        if time.time() - t0 > budget:
            n["skipped"] += 1
            continue
        k = i.key if flow.rules[i.rule].scope == SOURCE else (i.rule, i.key)
        try:
            ok = seen[k] if k in seen else legacy_ok(flow, i, root)
            ldone, diff = done(db, i, root)
        except OSError:
            n["skipped"] += 1
            continue
        a = last_attempt(db, i.id)
        if ok and not (a and a["state"] in PR.A_DONE):
            record(db, i, PR.A_LEGACY, "shadow", root)
            n["new"] += 1
        elif ok and diff is None:
            n["agree"] += 1
        elif ok:
            if ran_since(i, a["end"]):
                record(db, i, PR.A_LEGACY, "shadow", root, note="调度重跑过")
                n["rerun"] += 1
            else:
                fp = fingerprint(os.path.join(root, i.marker))
                d = dict(path=i.marker, what=diff, size=fp["size"], mtime_ns=fp["mtime_ns"])
                n["changed"] += 1
                if not seen_event(db, i.id, "changed", d):
                    event(db, i.id, i.marker, "changed", LEVEL[diff], d)
                    bad.append(dict(id=i.id, kind="changed", level=LEVEL[diff], **d))
        elif ldone or (a and a["state"] in PR.A_DONE):
            if ran_since(i, 0 if not a else a["end"]):
                n["rerun"] += 1  # 在重跑：做完以前旧的判断说没做完
                continue
            d = dict(path=i.marker, what=diff or "check")
            n["regressed"] += 1
            if not seen_event(db, i.id, "regressed", d):
                lvl = "error" if diff == "missing" else "warn"
                event(db, i.id, i.marker, "regressed", lvl, d)
                bad.append(dict(id=i.id, kind="regressed", level=lvl, **d))
        else:
            n["agree"] += 1
    if bad:
        d = cfg.state(PR.LEDGER_SHADOW)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, f"ledger-{day or datetime.now():%Y%m%d}.jsonl"), "a") as f:
            for b in bad:
                f.write(json.dumps(dict(t=f"{datetime.now():%F %T}", **b), ensure_ascii=False) + "\n")
    n["unexplained"] = bad
    n["seconds"] = round(time.time() - t0, 1)
    return n
