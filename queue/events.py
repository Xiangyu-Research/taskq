"""事件日志：hub 数据库里的一张表（log），调度和人记下的事都在里面。

    taskq events [--since T] [--until T] [--key K] [--site S] [--source S] [--kind K] [--level L] [--grep RE] [-n N] [-f] [--json]
    taskq note "<文字>" [--key K] [--site S] [--kind K] [--at T] [--uid U] [--data JSON]
    taskq progress [--key K] [--since T] [--json]

add() 不抛异常：数据库忙或坏了时，事件写进它旁边的 spool 文件，下一轮 tick 再导入（flush()）。没有数据库的地方（执行机）
什么都不存。proto.TEXT_LOGS 里列的来源还在各自的文本文件（cfg.log(source)）里写一行"时间 消息"，给 tail 看；没有代码读回这些文件。
时间（--since/--until/--at）："YYYY-MM-DD[ HH:MM[:SS]]"，或相对的 "30m"、"2h"、"3d"。
"""
import argparse
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


def _text(rows):
    by = {}
    for r in rows:
        if r["source"] in proto.TEXT_LOGS and r.get("uid") is None:
            by.setdefault(r["source"], []).append(r)
    for src, rs in by.items():
        try:
            with open(cfg.log(proto.TEXT_LOGS[src]), "a") as f:
                f.writelines(f"{time.strftime('%F %T', time.localtime(r['t']))} {r['msg']}\n" for r in rs)
        except OSError:
            pass


def add_many(rows, db=None):
    """rows：_row() 生成的。不抛异常。"""
    rows = list(rows)
    if not rows:
        return
    _text(rows)
    own = db is None
    try:
        db = db or D.connect()
        if db is None:
            return
        if not D.has(db, "log"):
            raise OSError("数据库还没迁移")
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
    """同一个来源、同一时刻的几行文字。"""
    now = time.time()
    add_many([_row(source, m, t=now, **kw) for m in msgs], db)


def flush(db):
    """导入 spool（hub 的一轮 tick 里，在 connect(create=True) 之后）。"""
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


# ---------------- 读 ----------------
def when(s, now=None):
    """'2h' / '3d' / '30m'（多久以前）/ 'YYYY-MM-DD[ HH:MM[:SS]]' → epoch 秒。"""
    now = now or time.time()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([mhd])", s.strip())
    if m:
        return now - float(m.group(1)) * dict(m=60, h=3600, d=86400)[m.group(2)]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return time.mktime(time.strptime(s.strip(), fmt))
        except ValueError:
            continue
    raise ValueError(f"不是时间：{s!r}（YYYY-MM-DD[ HH:MM]，或 30m / 2h / 3d）")


def ahead(s, now=None):
    """从现在起 '2h' / '3d' / '30m' 以后，或一个绝对时间 → epoch 秒。"""
    now = now or time.time()
    return 2 * now - when(s, now) if re.fullmatch(r"\d+(?:\.\d+)?[mhd]", s.strip()) else when(s, now)


def query(db, since=None, until=None, key=None, site=None, source=None, kind=None, level=None, grep=None, limit=None, after_id=None):
    """→ [带 id 的行 dict]，旧的在前。key 对的是 key 列；没有 key 的行对文字。"""
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


def cmd_events(argv):
    ap = argparse.ArgumentParser(prog="taskq events", description="事件日志：发生过什么（调度、健康检查、记的笔记）")
    ap.add_argument("--since", help="从什么时候起（默认 24 小时前，给了 -n 或 --key 时不限；'all' 是全部）")
    ap.add_argument("--until", help="到什么时候（同 --since 的写法）")
    ap.add_argument("--key", help="只看这个任务的（任务名；没记任务名的事件按文字里提到它算）")
    ap.add_argument("--site", help="只看这台机器的")
    ap.add_argument("--source", help="relay、feed、advance、health、note、…")
    ap.add_argument("--kind", help="事件的种类（--json 里的 kind）")
    ap.add_argument("--level", help="info、normal、urgent")
    ap.add_argument("--grep", help="按正则筛文字")
    ap.add_argument("-n", type=int, help="只看最后 N 条")
    ap.add_argument("-f", "--follow", action="store_true", help="一直打印新来的事件")
    ap.add_argument("--json", action="store_true", help="每行一个 JSON 对象（proto.EVENT_FIELDS）")
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
        print(f"taskq events: {D.path()} 没有数据库（hub 上第一轮 tick 会建）", file=sys.stderr)
        return 1
    q = dict(since=since, until=until, key=a.key, site=a.site, source=a.source, kind=a.kind, level=a.level, grep=a.grep)
    rows = query(db, limit=a.n, **q)
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
    ap = argparse.ArgumentParser(prog="taskq note", description="往事件日志里记一条（出了什么事、做了什么）")
    ap.add_argument("text", nargs="*", help="要记的话（不给时从标准输入读）")
    ap.add_argument("--key", help="关于哪个任务（任务名）")
    ap.add_argument("--site", help="关于哪台机器")
    ap.add_argument("--kind", default="note", help="例如 incident、fix、change、decision、remark（默认 note）")
    ap.add_argument("--at", help="什么时候发生的（默认现在）")
    ap.add_argument("--uid", help="唯一编号：同一个 uid 的第二条不记（导入用）")
    ap.add_argument("--data", help="和这条一起存的 JSON 对象（taskq events --json 里能看到）")
    ap.add_argument("--source", default="note", help=argparse.SUPPRESS)
    a = ap.parse_args(argv)
    text = " ".join(a.text).strip()
    if not a.text and not sys.stdin.isatty():
        text = sys.stdin.read().strip()
    if not text:
        print("taskq note: 没有文字", file=sys.stderr)
        return 2
    try:
        t = when(a.at) if a.at else None
        data = json.loads(a.data) if a.data else None
    except ValueError as e:
        print(f"taskq note: {e}", file=sys.stderr)
        return 2
    db = D.connect()
    if db is None or not D.has(db, "log"):
        print(f"taskq note: {D.path()} 没有数据库（要在 hub 上跑；`taskq db migrate` 会建）", file=sys.stderr)
        return 1
    n = db.total_changes
    _insert(db, [_row(a.source, text, kind=a.kind, key=a.key, site=a.site, t=t, uid=a.uid, data=data)])
    added = db.total_changes > n
    db.close()
    print("记下了" if added else f"已经有了（uid {a.uid}）")
    return 0


# ---------------- 进度快照 ----------------
def snapshot(db, tasks, t=None):
    """tasks：[dict(key, state, progress, site, data)] → 状态、进度或机器变了的任务各记一行。→ 记了几行。"""
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
    ap = argparse.ArgumentParser(prog="taskq progress", description="任务进度随时间的变化（状态、进度或机器一变就记一行）")
    ap.add_argument("--key", help="只看一个任务")
    ap.add_argument("--since", help="从什么时候起（YYYY-MM-DD[ HH:MM]，或 30m / 2h / 3d 以前；默认全部）")
    ap.add_argument("--json", action="store_true", help="一行 JSON（proto.PROGRESS_TOP；每行记录 proto.PROGRESS_FIELDS）")
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
    print(__doc__.strip(), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
