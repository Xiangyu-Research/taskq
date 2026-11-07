"""调度脚本共用的小工具，只用标准库（执行机上用系统的 python3）。"""
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


def tmp_of(path):
    """path 写到一半时的名字（同一个文件夹、带进程号，结尾 proto.TMP）：写完再 os.replace 过去。"""
    return f"{path}.{os.getpid()}{_proto().TMP}"


def atomic_put(path, make):
    """make(临时名) 在同目录的临时名上建出文件或链接，再换到 path：path 要么是旧的、要么是建好的。make 出错 → 异常照抛，path 不动。"""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = tmp_of(path)
    try:
        if os.path.lexists(tmp):
            os.remove(tmp)
        make(tmp)
        os.replace(tmp, path)
    finally:
        if os.path.lexists(tmp):
            os.remove(tmp)


def atomic_write(path, data):
    """整份写到临时名、落盘以后再换过去：断电、被杀时文件要么是旧的、要么是新的，不会只写一半。data：str 或 bytes。"""
    def make(tmp):
        with open(tmp, "wb") as f:
            f.write(data.encode() if isinstance(data, str) else data)
            f.flush()
            os.fsync(f.fileno())
    atomic_put(path, make)


def atomic_link(target, path):
    """path 换成指向 target 的软链接。"""
    atomic_put(path, lambda tmp: os.symlink(target, tmp))


def atomic_copy(src, dst, link=False, size=None, sha=None):
    """src 放到 dst：link=True 时先试硬链接（同一个盘才成），否则拷贝（带修改时间）；先放到临时名，核对大小（size）、
    sha256（sha，拷贝时边拷边算）后再换过去。对不上 → OSError，dst 不动。→ 是不是硬链接的。"""
    import hashlib
    import shutil
    linked = []

    def make(tmp):
        if link:
            try:
                os.link(src, tmp)
                linked.append(1)
            except OSError:
                pass
        if not linked:
            h = hashlib.sha256() if sha else None
            with open(src, "rb") as fi, open(tmp, "wb") as fo:
                for b in iter(lambda: fi.read(1 << 22), b""):
                    fo.write(b)
                    if h:
                        h.update(b)
            shutil.copystat(src, tmp)
            if h and h.hexdigest() != sha:
                raise OSError(f"{src}：内容和 sha256 {sha[:12]}… 对不上")
        elif sha and sha256(tmp) != sha:
            raise OSError(f"{src}：内容和 sha256 {sha[:12]}… 对不上")
        got = os.path.getsize(tmp)
        if size is not None and got != size:
            raise OSError(f"{src}：大小 {got}，应为 {size}")
    atomic_put(dst, make)
    return bool(linked)


def write_json(path, obj, indent=1):
    atomic_write(path, json.dumps(obj, ensure_ascii=False, indent=indent))


def sha256(path):
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 22), b""):
            h.update(b)
    return h.hexdigest()


def inside(path, base):
    """path（解开软链接后）是 base 本身或在它下面。"""
    p, b = os.path.realpath(path), os.path.realpath(base)
    return p == b or p.startswith(b.rstrip("/") + "/")


def under(p, d):
    """相对路径 p 等于 d 或在 d 下面（按路径的段，不解开链接）。"""
    return p == d or p.startswith(d.rstrip("/") + "/")


def du(paths, skip_top=(), follow=False):
    """paths（文件或文件夹）占的盘（字节）：硬链接只算一次 → (总共, 其中不在 skip_top 里的)。
    skip_top：第一个 path 下第一级的名字（根目录的 blobs、code）不算进第二个数。
    follow：软链接按指到的内容算（要传多少：传过去的是内容）；否则不算。"""
    used, rest, seen = 0, 0, set()
    for top in paths:
        if os.path.isfile(top) and (follow or not os.path.islink(top)):
            walk = [(os.path.dirname(top), [], [os.path.basename(top)])]
        else:
            walk = os.walk(top, followlinks=follow)
        for dp, _, fns in walk:
            first = os.path.relpath(dp, top).split(os.sep)[0]
            for f in fns:
                p = os.path.join(dp, f)
                try:
                    x = os.stat(p) if follow else os.lstat(p)
                except OSError:
                    continue
                if not os.path.isfile(p) or os.path.islink(p) and not follow:
                    continue
                if (x.st_dev, x.st_ino) in seen:
                    continue
                seen.add((x.st_dev, x.st_ino))
                used += x.st_blocks * 512
                if first not in skip_top:
                    rest += x.st_blocks * 512
    return used, rest


# ---------------- 跑命令（本机或经 ssh） ----------------
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]


def remote_timeout(cmd, timeout):
    """ssh 到那边跑的命令外面套一层那边的 timeout：本机这边超时断开后，那边卡在存储里的进程也结束，不留在登录节点上。"""
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


# ---------------- 任务记录：核心加进 pueue 的任务（hub 核心组里的传数据、拉回、归档、建环境）"是谁"只看这里 ----------------
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
    """这台机器上的 pueue 任务是谁 → 它的记录 dict(step, key, …)（任务号和标签都对上）；没有记录的（手动加的）→ None。"""
    label = task.get("label") or ""
    return next((r for r in recs if r["machine"] == machine and r.get("id") == task["id"] and r["label"] == label), None)


def backup_down(ok=None):
    """备份机（machines.py 里角色 backup 的）暂停着（policy.py 的 PAUSE），或这次检查连不上（ok：快照的 s["ok"]）
    → 说明；能用 → None。连不上时先不排归档，免得排上去的归档都失败、停住。"""
    M = _cfg().mtab()
    for b in M.backups():
        if b == M.hub():
            continue
        if M.paused(b):
            return f"{b} 暂停着（{M.paused(b)}）"
        if ok is not None and ok.get(b) is False:
            return f"{b} 这次连不上"
    return None
