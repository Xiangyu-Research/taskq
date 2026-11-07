#!/usr/bin/env python3
"""健康检查的问题、提醒和确认，存在 hub 的数据库里（queue/db.py）。

    taskq problems [--all] [--json]           最近一次健康检查查到的问题
    taskq notice set <名字> [--at T] [文字…]  挂上提醒（不给文字时读标准输入）；--at：到那时才开始报
    taskq notice clear <名字>…                撤掉提醒（没有的不算错）
    taskq notice list [--json]                挂着的提醒，包括还在等 --at 的
    taskq notice ack <问题编号或前缀>…        确认"发生过的事"这类问题（kind=event，如 boot:<机器>）

健康检查把每条到了时候的提醒报成问题 notice:<名字>，直到撤掉。持续的问题（盘满、连不上）不能确认，解决了就消失。
名字只用字母、数字、_ 和 -。
"""
import argparse
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import proto  # noqa: E402
import db as D  # noqa: E402

TF = "%Y-%m-%d %H:%M:%S"


def open_db(write=True):
    """hub 的数据库 → 连接或 None。write=True 时没有就建。"""
    return D.connect(create=write, ro=not write)


# ---------------- 问题 ----------------
def problems(db):
    """→ ({编号: 问题 dict}, 最近一次检查的时间或 None)。"""
    if not D.has(db, "problem"):
        return {}, None
    return {pid: json.loads(b) for pid, b in db.execute("SELECT id, body FROM problem")}, D.get_meta(db, "checked")


def save_problems(db, cur, checked):
    """换掉问题列表（确认过的已经去掉了）。"""
    with D.tx(db):
        db.execute("DELETE FROM problem")
        db.executemany("INSERT INTO problem(id, body) VALUES(?, ?)", [(k, json.dumps(v, ensure_ascii=False)) for k, v in cur.items()])
        D.set_meta(db, "checked", checked)


def prune_acks(db, raised):
    """这次检查根本不再报的问题，删掉它们的确认（确认过的问题还在报，只是不列出）。"""
    with D.tx(db):
        for pid in [k for k in acked(db) if k not in raised]:
            db.execute("DELETE FROM ack WHERE id=?", (pid,))


def acked(db):
    return dict(db.execute("SELECT id, t FROM ack")) if D.has(db, "ack") else {}


def ack(db, prefixes):
    """上次检查的问题里，编号以 prefixes 之一开头、kind=event 的 → 确认。→ (确认了的编号, 没确认的)。"""
    probs, _ = problems(db)
    got, bad = [], []
    now = time.strftime(TF)
    with D.tx(db):
        for pre in prefixes:
            hit = [k for k in probs if k.startswith(pre)]
            ev = [k for k in hit if probs[k].get("kind") == "event"]
            if not ev:
                bad.append(pre + ("（持续的问题：解决了就消失）" if hit else "（没有这个问题）"))
            for k in ev:
                db.execute("INSERT INTO ack(id, t) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET t=excluded.t", (k, now))
                got.append(k)
    return got, bad


# ---------------- 提醒 ----------------
def valid(name):
    return bool(re.fullmatch(proto.NOTICE_NAME, name or ""))


def notices(db, due_only=True, now=None):
    """→ 按名字排的 {名字: dict(text, since, at)}；due_only 时去掉 --at 还没到的。"""
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


# ---------------- 命令行 ----------------
def problem_rows(probs, everything=False):
    """→ [proto.OVERVIEW_PROBLEM dict]，带白话说明；默认只要现在算数的。"""
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
    ap = argparse.ArgumentParser(prog="taskq problems", description="最近一次健康检查查到的问题")
    ap.add_argument("--all", action="store_true", help="也列 info 和还没报的（在等持续够久）")
    ap.add_argument("--json", action="store_true", help="一行 JSON（proto.PROBLEMS_TOP；每个问题 proto.OVERVIEW_PROBLEM）")
    a = ap.parse_args(argv)
    db = open_db(write=False)
    probs, checked = problems(db) if db else ({}, None)
    rows = problem_rows(probs, a.all)
    if a.json:
        print(json.dumps(dict(version=proto.VERSION, checked=checked, problems=rows), ensure_ascii=False))
        return 0
    print(f"健康检查 {checked or '还没跑过'}：{len(rows)} 个问题")
    for r in rows:
        print(f"  [{r['level']}] {r['id']}（从 {r['first']} 起）\n      {r['plain'] or r['msg']}")
    return 0


def main(argv):
    if argv[:1] == ["problems"]:
        return cmd_problems(argv[1:])
    ap = argparse.ArgumentParser(prog="taskq notice", description="挂在健康检查上的提醒")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("set", help="挂上提醒或改它的文字")
    p.add_argument("name")
    p.add_argument("text", nargs="*", help="文字（不给时从标准输入读）")
    p.add_argument("--at", help="到那时才开始报（YYYY-MM-DD[ HH:MM]，或 2h / 3d 以后）")
    p = sub.add_parser("clear", help="撤掉提醒")
    p.add_argument("names", nargs="+")
    p = sub.add_parser("ack", help='确认"发生过的事"这类问题（如重启），之后不再列出')
    p.add_argument("ids", nargs="+", metavar="ID", help="问题编号或前缀（如 boot:hub）")
    p = sub.add_parser("list", help="挂着的提醒")
    p.add_argument("--json", action="store_true", help="一行 JSON（proto.NOTICE_TOP、NOTICE_FIELDS）")
    a = ap.parse_args(argv)
    bad = [n for n in ([a.name] if a.cmd == "set" else a.names if a.cmd == "clear" else []) if not valid(n)]
    if bad:
        print(f"名字只能用字母、数字、_ 和 -：{'、'.join(bad)}", file=sys.stderr)
        return 2
    if a.cmd == "list":
        db = open_db(write=False)
        got = notices(db, due_only=False) if db else {}
        if a.json:
            print(json.dumps(dict(version=proto.VERSION, notices=[dict(name=n, **v) for n, v in got.items()]), ensure_ascii=False))
            return 0
        for n, v in got.items():
            print(f"{n}（{v['since']}{'，从 ' + v['at'] + ' 起' if v['at'] else ''}）：{v['text']}")
        if not got:
            print("没有提醒")
        return 0
    if a.cmd == "set":
        text = " ".join(a.text).strip()
        if not a.text and not sys.stdin.isatty():
            text = sys.stdin.read().strip()
        if not text:
            print("要给文字（参数或标准输入）", file=sys.stderr)
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
        print(f"taskq: 写不了 {D.path()} 的数据库（在网络文件系统上，或是更新的 taskq 建的）", file=sys.stderr)
        return 1
    if a.cmd == "set":
        put(db, a.name, text, at)
        print(f"挂上了 {a.name}{'，从 ' + at + ' 起' if at else ''}：{text}")
        return 0
    if a.cmd == "ack":
        got, rej = ack(db, a.ids)
        for k in got:
            print(f"已确认 {k}（从下一轮健康检查起不再列出）")
        for b in rej:
            print(f"没确认：{b}", file=sys.stderr)
        return 0 if got else 1
    for n in a.names:
        print(f"撤掉了 {n}" if drop(db, n) else f"没有这个提醒：{n}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
