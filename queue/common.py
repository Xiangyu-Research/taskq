"""调度脚本共用的小工具（2026-10-25），只用标准库（lab、DICC 上用系统的 python3）。"""
import json
import os


def write_json(path, obj, indent=1):
    """整份写到临时文件、落盘以后再换过去：断电、被杀时文件要么是旧的、要么是新的，不会只写一半。"""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(obj, f, ensure_ascii=False, indent=indent)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


# ---------------- 任务记录（2026-10-26）：box、lab 的 pueue 任务"是谁"只看这里 ----------------
def tasks_path():
    return os.path.expanduser(os.environ.get("PHD_TASKS") or "~/.cache/phd/tasks.json")  # 环境变量只给测试用


def load_records():
    try:
        d = json.load(open(tasks_path()))
        return d if isinstance(d, list) else []
    except (OSError, ValueError):
        return []


def update_records(fn):
    """fn(记录列表) → 新的列表，加锁读改写 → 新的列表。"""
    import fcntl
    p = tasks_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p + ".lock", "a") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        recs = fn(load_records())
        write_json(p, recs)
    return recs


def new_record(machine, label, step, key, **kw):
    """加任务之前先记下（id 还是 None）→ 记录；加上以后用 set_record_id 补任务号。"""
    import time
    rec = dict(machine=machine, id=None, label=label, added=time.time(), step=step, key=key,
               **{k: v for k, v in kw.items() if v is not None})

    def add(recs):
        if rec.get("out"):  # 训练机上做的步骤：同一个标签以前的记录作废
            recs = [r for r in recs if not (r.get("out") and r["machine"] == machine and r["label"] == label)]
        return recs + [rec]
    update_records(add)
    return rec


def set_record_id(rec, tid):
    def f(recs):
        for r in recs:
            if r["machine"] == rec["machine"] and r["label"] == rec["label"] and r["added"] == rec["added"]:
                r["id"] = tid
        return recs
    rec["id"] = tid
    update_records(f)


def drop_records(pred):
    update_records(lambda recs: [r for r in recs if not pred(r)])


def pueue_time(x):
    """pueue 的时间（带时区；秒的小数部分可能是 0、3、6、9 位）→ 时间戳；没有 → None。"""
    import re
    from datetime import datetime
    if not x:
        return None
    m = re.fullmatch(r"(.+T\d\d:\d\d:\d\d)(\.\d+)?(.*)", x)
    return datetime.fromisoformat(m.group(1) + (m.group(2) or "")[:7] + m.group(3)).timestamp() if m else None


SKEW = 300  # 机器之间的时钟差
LOST = 3600  # 加任务时出错、找不到任务的记录留多久


def sync_records(pueue, recs, now=None):
    """各机器的 pueue（qs --json）+ 记录 → 新的记录（不写文件）：没有任务号的按"同一台机器、同标签、创建时间不早于
    记录的时间"找回；任务不在了的删掉，训练机上做的步骤（有 out）留到那边的结果清理完。读不到的机器的记录不动。"""
    import time
    now = now or time.time()
    taken = {(r["machine"], r["id"]) for r in recs if r.get("id") is not None}
    out = []
    for r in recs:
        tasks = (pueue.get(r["machine"]) or {}).get("tasks")
        if tasks is None:
            out.append(r)
            continue
        if r.get("id") is None:
            hit = sorted((t for t in tasks if (t.get("label") or "") == r["label"] and (r["machine"], t["id"]) not in taken
                          and (pueue_time(t.get("created_at")) or 0) >= r["added"] - SKEW), key=lambda t: t["id"])
            if hit:
                r = dict(r, id=hit[0]["id"])
                taken.add((r["machine"], r["id"]))
            elif now - r["added"] > LOST:
                continue
            out.append(r)
            continue
        if r.get("out") or any(t["id"] == r["id"] and (t.get("label") or "") == r["label"] for t in tasks):
            out.append(r)
    return out


def identify(machine, task, recs):
    """这台机器上的 pueue 任务是谁 → dict(step, key, 可能还有 src、to、of、paths、group、out…)；认不出 → None。
    先看记录（任务号和标签都对上）；没有记录的（手动加的）按命令认训练（run_task.sh）、拉回（fetch_task.sh <源> <key>）、
    归档（archive_task.sh <机器> <key>）。"""
    label = task.get("label") or ""
    for r in recs:
        if r["machine"] == machine and r.get("id") == task["id"] and r["label"] == label:
            return r
    return from_command(task.get("command") or "")


def from_command(cmd):
    import shlex
    try:
        tok = shlex.split(cmd)
    except ValueError:
        return None
    flat = []
    for x in tok:  # 整条命令被引号包成一个参数时（q 加的任务）再拆一层
        try:
            flat += shlex.split(x) if " " in x else [x]
        except ValueError:
            flat.append(x)
    for i, x in enumerate(flat):
        b, rest = os.path.basename(x), flat[i + 1:]
        if b == "run_task.sh" and rest:
            try:
                import spec
                return dict(step="train", key=spec.resolve(rest[0], spec.parse_kv([r for r in rest[1:] if "=" in r]))["key"])
            except Exception:  # noqa: BLE001
                return None
        if b in ("fetch_task.sh", "archive_task.sh") and len(rest) >= 2:
            return dict(step="fetch" if b == "fetch_task.sh" else "archive", src=rest[0], key=rest[1])
    return None
