"""调度脚本共用的小工具（2026-10-25），只用标准库（执行机上用系统的 python3）。"""
import json
import os


def _proto():
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import proto
    return proto


def _cfg():
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import cfg
    return cfg


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


# ---------------- 跑命令（本机或经 ssh） ----------------
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]


def remote_timeout(cmd, timeout):
    """ssh 到那边跑的命令外面套一层那边的 timeout：本机这边超时断开后，那边卡在存储里的进程也结束，不留在登录节点上（11/1）。"""
    return f"timeout -k 10 {max(int(timeout) - 15, 10)} {cmd}"


def run(cmd, host=None, timeout=120, input=None, ssh=None):
    """跑命令 → (退出码, 标准输出, 标准错误)。cmd 是列表时在本机直接跑；是字符串时本机用 bash -c、给了 host（ssh 名）就到那台跑。
    超时、起不来 → (124, "", 原因)。ssh 不通时退出码是 255。到那边跑的命令那边也限时（remote_timeout），不留卡住的进程。"""
    import shlex
    import subprocess
    if isinstance(cmd, str):
        cmd = ["bash", "-c", cmd] if host is None else (ssh or SSH) + [host, remote_timeout(f"bash -c {shlex.quote(cmd)}", timeout)]
    elif host is not None:
        raise ValueError("列表形式的命令只在本机跑")
    try:
        r = subprocess.run(cmd, input=input, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except Exception as e:  # noqa: BLE001
        return _proto().EXIT["timeout"], "", str(e)


def pueue_time(x):
    """pueue 的时间（带时区；秒的小数部分可能是 0、3、6、9 位）→ 时间戳；没有 → None。"""
    import re
    from datetime import datetime
    if not x:
        return None
    m = re.fullmatch(r"(.+T\d\d:\d\d:\d\d)(\.\d+)?(.*)", x)
    return datetime.fromisoformat(m.group(1) + (m.group(2) or "")[:7] + m.group(3)).timestamp() if m else None


def local_time(x):
    """pueue 的时间 → 本机时区、不带时区的 datetime；没有 → None。"""
    from datetime import datetime
    ts = pueue_time(x)
    return None if ts is None else datetime.fromtimestamp(ts)


# ---------------- 和上次一样就不再写日志 ----------------
def changed(name, value):
    """value 和上次记下的不一样 → 记下、返回 True；一样 → False。记在状态目录的 last/<name>.json（如 ~/.local/state/taskq/last/）。"""
    import re
    d = os.path.expanduser(os.environ.get("TASKQ_LAST") or _cfg().state("last", expand=False))  # 环境变量只给测试用
    p = os.path.join(d, re.sub(r"[^\w.-]", "_", name) + ".json")
    try:
        if json.load(open(p)) == value:
            return False
    except (OSError, ValueError):
        pass
    write_json(p, value)
    return True


# ---------------- 任务记录（2026-10-25）：hub 和 pueue 执行机的 pueue 任务"是谁"只看这里 ----------------
def tasks_path():
    return os.path.expanduser(os.environ.get("TASKQ_TASKS") or _cfg().state("tasks.json", expand=False))  # 环境变量只给测试用


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
    update_records(lambda recs: recs + [rec])
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


SKEW = 300  # 机器之间的时钟差
LOST = 3600  # 加任务时出错、找不到任务的记录留多久


def sync_records(pueue, recs, now=None):
    """各机器的 pueue（qs --json）+ 记录 → 新的记录（不写文件）：没有任务号的按"同一台机器、同标签、创建时间不早于
    记录的时间"找回；任务不在了的删掉。读不到的机器的记录不动。"""
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
        if any(t["id"] == r["id"] and (t.get("label") or "") == r["label"] for t in tasks):
            out.append(r)
    return out


def identify(machine, task, recs):
    """这台机器上的 pueue 任务是谁 → dict(step, key, …)；认不出 → None。
    先看记录（任务号和标签都对上）；没有记录的（手动加的）按命令只认训练（run_task.sh）。"""
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
    for x in tok:  # 整条命令被引号包成一个参数时（q 加的任务）再拆一层；--spec 后面的 JSON 不拆
        try:
            flat += shlex.split(x) if " " in x and x[:1] not in "{[" else [x]
        except ValueError:
            flat.append(x)
    for i, x in enumerate(flat):
        b, rest = os.path.basename(x), flat[i + 1:]
        if b == "run_task.sh" and rest:
            try:
                import spec
                if rest[0] == "--spec":
                    return dict(step="train", key=spec.from_sent(rest[1])["key"])
                return dict(step="train", key=spec.resolve(rest[0], spec.parse_kv([r for r in rest[1:] if "=" in r]))["key"])
            except Exception:  # noqa: BLE001
                return None
    return None


def backup_down(ok=None):
    """备份机（machines.py 里角色 backup 的）暂停检查（policy.py 的 OFFLINE），或这次检查连不上（ok：快照的 s["ok"]）
    → 说明；能用 → None。连不上时先不排归档，免得排上去的归档都失败、停住（11/1）。"""
    M = _cfg().mtab()
    import policy as P
    for b in M.backups():
        if b == M.hub():
            continue
        if b in P.OFFLINE:
            return f"{b} 暂停检查（{P.OFFLINE[b]}）"
        if ok is not None and ok.get(b) is False:
            return f"{b} 这次连不上"
    return None
