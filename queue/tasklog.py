#!/usr/bin/env python3
"""一个任务的经过：每段训练（run_task.sh 的分段日志）、事件日志里关于它的、hub 的任务记录。

    taskq log <key> [--data 目录] [--json]

段：<数据目录>/logs/tasks/<safe key>__<作业>_part<段>.log里每个开头行（proto.LOG_HEAD）算一段：作业、机器
（按作业名认）、主机、第几段、环境、代码、开始；后面的行里认结束（proto.TASKLOG_STATES）：训完、命令结束和退出码、没开训、
等不到卡、被当作卡住；都没有时日志 FRESH_LOG 内更新过算还在跑，否则算没有结束的记录。path 是日志的完整路径，日志里别的内容
（任务自己的输出）由使用者自己读。
数据目录：--data，没给时 cfg.data_dir()。hub 上执行机训的段在拉回以后才有（拉回带着这个任务的
分段日志）；还在执行机上的看 taskq status --all --json 的 parts，或在那边 taskq log --data <根目录>/data。
事件日志（hub 的数据库）和任务记录（tasks.json）只在 hub 上有，别的机器上是空的。
--json：一行 JSON，字段见 proto 的 TASKLOG_*。
"""
import argparse
import glob
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402

HEAD = re.compile(proto.LOG_HEAD)
TS = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
LINE_TS = re.compile(r"^" + TS + r"(?:\.\d+)?:? ?(.*)$")
ENDED = re.compile(r"^" + TS + r" 命令结束（退出码 (-?\d+)），(.*)$")
NOT_STARTED = re.compile(r"^" + TS + r" 没开训：(.*)$")
GPU_BUSY = re.compile(r"^" + TS + r" 卡 \S+ 被别人占着.*不跑$")


def data_dir(arg=None):
    if arg:
        return os.path.expanduser(arg)
    try:
        return cfg.data_dir()
    except Exception:  # noqa: BLE001
        return ""


def machine_of(job):
    try:
        return cfg.mtab().machine_of_jobtag(job)
    except Exception:  # noqa: BLE001
        return None


def log_files(key, data):
    """这个任务的分段日志（不含 key 更长的别的任务的：前缀后面不再有 SAFE）。"""
    pre = proto.safe(key) + proto.SAFE
    d = os.path.join(data, proto.TASK_LOGS)
    out = [p for p in glob.glob(os.path.join(glob.escape(d), glob.escape(pre) + "*.log"))
           if proto.SAFE not in os.path.basename(p)[len(pre):]]
    return sorted(out)


def parse(path, key, now=None):
    """一个分段日志 → [段]（proto.TASKLOG_ATTEMPT）。"""
    out, cur = [], None
    with open(path, errors="replace") as f:
        for ln in f:
            ln = ln.rstrip("\n")
            m = HEAD.match(ln)
            if m:
                cur = None
                if m.group(3) != key:
                    continue
                job, part = m.group(1), int(m.group(4))
                job = job[:-len(f"_{part}")] if job.endswith(f"_{part}") else job
                cur = dict(file=os.path.basename(path), path=path, job=job, machine=machine_of(job), host=m.group(2), part=part,
                           env=m.group(5), code=m.group(6), start=m.group(7), end=None, last=m.group(7), state=None, exit=None, note=None)
                out.append(cur)
                continue
            if cur is None:
                continue
            t = LINE_TS.match(ln)
            if t:
                cur["last"] = t.group(1)
            if ln.startswith(proto.DONE_LINE):
                t = re.search(TS, ln)
                cur.update(state="done", end=t.group(1) if t else cur["last"], exit=0)
            elif ln.startswith(proto.STALL_LINE):
                t = re.search(TS, ln)
                cur.update(state="stalled", end=t.group(1) if t else cur["last"], note=ln.split("：", 1)[-1].strip())
            elif ENDED.match(ln):
                m = ENDED.match(ln)
                rc, rest = int(m.group(2)), m.group(3)
                st = "unreadable" if rest.startswith("读不了盘") else ("not_done" if rc == 0 else "failed")
                cur.update(state=st, end=m.group(1), exit=rc)
            elif NOT_STARTED.match(ln):
                m = NOT_STARTED.match(ln)
                cur.update(state="not_started", end=m.group(1), note=m.group(2))
            elif GPU_BUSY.match(ln):
                cur.update(state="gpu_busy", end=GPU_BUSY.match(ln).group(1))
    if out and out[-1]["state"] is None:
        try:
            age = (now or time.time()) - os.path.getmtime(path)
        except OSError:
            age = None
        out[-1]["state"] = "running" if age is not None and age < proto.FRESH_LOG else None
    for a in out:
        a["state"] = a["state"] or "ended"
    return out


def attempts(key, data, now=None):
    out = [a for p in log_files(key, data) for a in parse(p, key, now)]
    return sorted(out, key=lambda a: (a["start"], a["part"]))


def events(key):
    """Event log rows about this key (hub database; none elsewhere). A key followed by a letter, digit or / is another task."""
    try:
        import db as D
        import events as EV
        db = D.connect(ro=True)
    except Exception:  # noqa: BLE001
        return []
    if db is None:
        return []
    try:
        if not D.has(db, "log"):
            return []
        return [dict(log=r["source"], time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["t"])), text=r["msg"])
                for r in EV.query(db, key=key)]
    finally:
        db.close()


def records(key):
    try:
        import common
        return [r for r in common.load_records() if r.get("key") == key]
    except Exception:  # noqa: BLE001
        return []


def collect(key, data, now=None):
    return dict(version=proto.VERSION, key=key, data=data, attempts=attempts(key, data, now) if data else [],
                events=events(key), records=records(key))


def show(r):
    print(f"任务 {r['key']}（数据目录 {r['data'] or '不知道'}）：{len(r['attempts'])} 段")
    for a in r["attempts"]:
        res = proto.TASKLOG_STATES.get(a["state"], a["state"])
        if a["exit"] not in (None, 0):
            res += f"（退出码 {a['exit']}）"
        if a["note"]:
            res += f"：{a['note']}"
        where = a["machine"] if a["machine"] and a["machine"] != a["job"] else ""
        print(f"  第 {a['part']} 段  作业 {a['job']}{f'（{where}）' if where else ''} @ {a['host']}  {a['start']} → {a['end'] or a['last']}  "
              f"{res}  环境 {a['env']}  代码 {a['code']}")
    if r["events"]:
        print(f"\n事件日志里关于它的 {len(r['events'])} 条：")
        for e in r["events"]:
            print(f"  {e['time'] or '':19}  {e['log']:8} {e['text']}")
    if r["records"]:
        print(f"\nhub 的任务记录 {len(r['records'])} 条：")
        for x in r["records"]:
            when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x["added"])) if isinstance(x.get("added"), (int, float)) else ""
            rest = {k: v for k, v in x.items() if k not in ("key", "added", "machine", "step", "label")}
            print(f"  {when}  {x.get('machine')}  {x.get('step')}  {x.get('label')}  {json.dumps(rest, ensure_ascii=False)}")


def main(argv):
    ap = argparse.ArgumentParser(description="一个任务的每段训练、事件日志里关于它的、hub 的任务记录")
    ap.add_argument("key")
    ap.add_argument("--data", help="数据目录（默认 cfg.data_dir()）")
    ap.add_argument("--json", action="store_true", help="一行 JSON（proto 的 TASKLOG_*）")
    a = ap.parse_args(argv)
    data = data_dir(a.data)
    if a.data and not os.path.isdir(data):
        print(f"taskq log：没有数据目录 {data}", file=sys.stderr)
        return 2
    r = collect(a.key, data)
    if a.json:
        print(json.dumps(r, ensure_ascii=False))
    else:
        show(r)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
