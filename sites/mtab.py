#!/usr/bin/env python3
"""机器表：配置文件夹里的 machines.py 只放数据（一张 MACHINES，一台物理机器一项，字段见 queue/schema.py），怎么用在这里。
核心里 import mtab as M，不直接 import machines。

一台机器的角色：
  hub     总队列、健康检查（只统筹，不跑任务）
  store   结果的家：数据目录 data；hub 对它和对执行机一样经 rsh + site_ops.py 操作
  backup  归档的备份放在 backup
  执行机  有 root 和 scheduler 的：pueue 的按 workers 开常驻 worker，slurm 的按 jobs 交 worker 作业
worker 的名字（gpu<卡号>、cpu）、id（<机器>-<名字>）、pueue 组名（同名字）由这里算，不手写。

命令行：
  mtab.py whoami                 本机的名字（认不出时退出码 1）
  mtab.py get <名|.> <字段>      字段的值（没写的给默认值）；元组、列表用空格连起来；本机的路径字段（EXPAND：conda、repo、root、data、code、core、backup、pueue）把 ~ 展开；
                                 另认 code（这台的代码包 <root>/code/current）、core（这台上调度代码的根）；没有这台机器时退出码 4，没有这个字段时 1
  mtab.py names [角色|调度方式]  机器名，每行一个（如 names pueue、names exec、names hub）
  mtab.py backups                角色 backup 的机器，每行一个
  mtab.py q-env <组>             本机这个组的环境变量（export 语句，queue/q 用）；没有这个组时退出码 2
  mtab.py groups [名|.]          这台（默认本机）的 pueue 组，每行"组名:同时跑几个"（queue/setup.sh 用）；没有这台机器时退出码 4
  mtab.py activate <环境> [名|.] 激活这个环境的 shell 语句（eval "$(...)"）；没有这台机器或这个环境时退出码 4
  mtab.py fields                 机器表、worker、policy 的字段表
  mtab.py fields --markdown [表…] 字段表的 Markdown（schema.TABLES 的名字，默认全部），带 README 用的标记
  （taskq machines show [名…]：核心按机器表算出来的值，一行 JSON，在 queue/cli_json.py）
"""
import datetime as dt
import os
import re
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "queue"))
import cfg  # noqa: E402
cfg.use_config()
import machines as _data  # noqa: E402
import proto  # noqa: E402
import schema  # noqa: E402

MACHINES = _data.MACHINES
KINDS = ("gpu", "cpu")
SPECS = os.path.join(os.path.dirname(os.path.abspath(_data.__file__)), "specs")  # 按描述建的环境：<配置文件夹>/specs/<环境>.yml


class FieldError(KeyError):
    """机器表里这台机器没有这个字段。"""

    def __str__(self):
        return self.args[0]


def machine(name):
    """一台机器的条目，没写的字段补上默认值。"""
    return schema.defaults(schema.MACHINE_FIELDS, MACHINES[name])


def expand(path):
    return os.path.expanduser(path)


# ---------------- 角色 ----------------

def roles(name):
    return tuple(MACHINES[name].get("roles") or ())


def _role(r):
    got = [n for n in MACHINES if r in roles(n)]
    if len(got) != 1:
        raise LookupError(f"机器表里角色 {r} 要正好一台（现在：{got or '没有'}）")
    return got[0]


def hub():
    return _role("hub")


def store():
    return _role("store")


def data():
    """store 的数据目录（结果的家，不展开 ~）。"""
    return MACHINES[store()]["data"]


def is_exec(name):
    """执行机：有 root 和 scheduler。机器表里没有的名字 → False。"""
    m = MACHINES.get(name) or {}
    return bool(m.get("root") and m.get("scheduler"))


def executors():
    return [n for n in MACHINES if is_exec(n)]


def names(kind=None):
    """机器名：kind 是角色（hub、store、backup）、exec（执行机）或调度方式（pueue、slurm）；None = 全部。"""
    if kind == "exec":
        return executors()
    return [n for n in MACHINES if kind is None or kind in roles(n) or kind == MACHINES[n].get("scheduler")]


def backups():
    return names("backup")


def pueue_hosts():
    """有 pueue 组（groups 非空）的机器：pueue 执行机、hub、store、写了 manual_groups 的。pueued 跑在这些机器上（setup.sh 装），
    健康检查探查、读 pueue、恢复 pueued 都按它。"""
    return [n for n in MACHINES if groups(n)]


def local(name):
    """这台就是 hub 所在的机器（ssh 是 None）。"""
    return MACHINES[name].get("ssh") is None


# ---------------- worker ----------------

def workers(name, kind=None):
    """这台的 worker（有序）：[dict(name, id, kind, group, gpu, cpus, slots, hours, idle, wait)]。
    pueue：每个 workers 项一个，名字 gpu<卡号> 或 cpu（有几个 CPU worker 时 cpu<序号>），pueue 组同名字；
    slurm：jobs 的每种一个（名字、种类同 jobs 的键，没有 pueue 组）。kind 给了只要这种的。"""
    m = machine(name)
    out = []
    if m.get("scheduler") == "slurm":
        for k, o in (m["jobs"] or {}).items():
            out.append(dict(name=k, id=f"{name}-{k}", kind=k, group=None, gpu=None, cpus=o.get("cpus"),
                            slots=None, hours=(), idle=None, wait=None))
    elif m.get("scheduler") == "pueue":
        ws = m["workers"] or ()
        ncpu = sum(1 for w in ws if w.get("gpu") is None)
        icpu = 0
        for w in ws:
            w = schema.defaults(schema.WORKER_FIELDS, w)
            if w["gpu"] is not None:
                wn = "gpu" + str(w["gpu"]).replace(",", "")
            else:
                icpu += 1
                wn = "cpu" if ncpu == 1 else f"cpu{icpu}"
            out.append(dict(w, name=wn, id=f"{name}-{wn}", kind="gpu" if w["gpu"] is not None else "cpu", group=wn))
    return [w for w in out if kind is None or w["kind"] == kind]


def worker(name, wid):
    """这台 id 是 wid 的 worker；没有 → None。"""
    return next((w for w in workers(name) if w["id"] == wid), None)


def kinds(name):
    """这台能跑的任务种类（有序）。"""
    have = {w["kind"] for w in workers(name)}
    return tuple(k for k in KINDS if k in have)


def worker_env(w):
    """pueue worker 的环境变量 {名: 值}（卡、核、同时训几个、等卡的规矩）。"""
    env = {"WORKER_ID": w["id"], "WORKER_DEVICE": w["kind"], "CUDA_VISIBLE_DEVICES": w["gpu"] or ""}
    if w.get("cpus"):
        env["TASKQ_CPUS"] = str(w["cpus"])
    if w.get("slots"):
        env["TASKQ_SLOTS"] = str(w["slots"])
    if w.get("idle"):
        env["TASK_GPU_IDLE"] = str(w["idle"])
    if w.get("wait"):
        env["TASK_WAIT_GPU"] = str(w["wait"])
    return env


def groups(name):
    """这台的 pueue 组 {组名: (同时跑几个（0 = 不限）, 卡号或 None, {环境变量})}：每个 pueue worker 一个组（一次一个 worker），
    pueue 机器、hub、store 另有核心自己的组（proto.IO_GROUP：传数据、拉回、归档），再加手动用的 manual_groups。"""
    m = machine(name)
    out = {}
    for w in workers(name):
        if w["group"]:
            out[w["group"]] = (1, w["gpu"], {})
    if m.get("scheduler") == "pueue" or {"hub", "store"} & set(roles(name)):
        out[proto.IO_GROUP] = (0, None, {})
    out.update(m["manual_groups"] or {})
    return out


def _tz(name):
    t = machine(name).get("tz")
    if not t:
        return None
    from zoneinfo import ZoneInfo
    return ZoneInfo(t)


def now_at(name, t=None):
    """这台机器（按 tz）此刻的本地时间（带时区）。t：秒（None = 现在）。"""
    import time
    t = time.time() if t is None else t
    tz = _tz(name)
    return dt.datetime.fromtimestamp(t, tz) if tz else dt.datetime.fromtimestamp(t).astimezone()


def in_hours(hours, now):
    """now（带时区的时间）在不在 hours 里 → (在不在, 这个时段 "开始–结束" 或 "")。hours 空 = 全天。
    每段 (开始, 结束, 第一天, 最后一天)：结束不晚于开始时跨夜（开始 = 结束是首尾相接的 24 小时）；日期是时段开始那天，None = 不限。"""
    if not hours:
        return True, ""
    for start, end, first, last in hours:
        for d in (now.date(), now.date() - dt.timedelta(days=1)):
            h, mi = map(int, start.split(":"))
            s = now.replace(year=d.year, month=d.month, day=d.day, hour=h, minute=mi, second=0, microsecond=0)
            h, mi = map(int, end.split(":"))
            ed = d + dt.timedelta(days=1) if end <= start else d
            e = now.replace(year=ed.year, month=ed.month, day=ed.day, hour=h, minute=mi, second=0, microsecond=0)
            ds = d.isoformat()
            if s <= now < e and (first is None or ds >= first) and (last is None or ds <= last):
                return True, f"{start}–{end}"
    return False, " / ".join(f"{a}–{b}" for a, b, _, _ in hours)


def working(name, w, t=None):
    """这个 worker 现在在不在它的时段里。"""
    return in_hours(w.get("hours") or (), now_at(name, t))[0]


# ---------------- 暂停 ----------------

def paused(name, t=None):
    """policy.py 的 PAUSE 里这台现在暂停着 → 原因；没有或已经到期 → None。到期时间是 hub 的本地时间 "YYYY-MM-DD HH:MM"。"""
    import policy as P
    x = (getattr(P, "PAUSE", None) or {}).get(name)
    if not x:
        return None
    until, why = x
    import time
    now = dt.datetime.fromtimestamp(time.time() if t is None else t).strftime("%Y-%m-%d %H:%M")
    return why if until is None or now < until else None


# ---------------- 本机、路径 ----------------

def whoami():
    """本机的名字：环境变量 TASKQ_MACHINE（代码包、worker 设的）；没有时按主机名，只认唯一匹配的那台；都不是时 None。"""
    e = os.environ.get("TASKQ_MACHINE")
    if e:
        return e if e in MACHINES else None
    h = socket.gethostname().split(".")[0]
    hit = [n for n, m in MACHINES.items() if re.fullmatch(m["hostname"], h)]
    return hit[0] if len(hit) == 1 else None


def code(name):
    """这台的代码包（不展开 ~）：<root>/code/current；没有 root 的（只当 hub 的）→ None（从 repo 跑）。"""
    r = MACHINES[name].get("root")
    return r.rstrip("/") + "/" + proto.ROOT["current"] if r else None


def core(name):
    """这台上调度代码的根（不展开 ~）：有 root → 代码包里的 <code>/scripts/taskq（cfg.bundle_rel）；没有 → 仓库的克隆 repo 下和本机
    同一位置；都没有 → None。"""
    c = code(name)
    if c:
        return c + "/" + cfg.bundle_rel()
    r = machine(name)["repo"]
    return r.rstrip("/") + "/" + cfg.CORE_REL if r else None


def env_root(name):
    """"spec" 环境建在哪个目录下（不展开 ~）：<root>/envs。"""
    return MACHINES[name]["root"].rstrip("/") + "/envs"


def machine_of_jobtag(tag):
    """训练分段日志里的作业名 → 机器的 label：纯数字是 Slurm 的作业号；常驻 worker 写的是"worker id-时间"（id 以机器名开头）；
    不经 worker 手动跑 run_task.sh 的是"主机名-时间"。"""
    if re.fullmatch(r"\d+", tag):
        labels = sorted({MACHINES[n]["label"] for n in names("slurm")})
        pre = os.path.commonprefix(labels).strip()
        return labels[0] if len(labels) == 1 else (pre or "Slurm")
    for n in MACHINES:
        if any(tag == w["id"] or tag.startswith(w["id"] + "-") for w in workers(n)):
            return MACHINES[n]["label"]
    for n in names("pueue"):
        if re.match(MACHINES[n]["hostname"], tag):
            return MACHINES[n]["label"]
    return tag


# ---------------- 环境 ----------------

def is_spec(e):
    return e == "spec" or (isinstance(e, (tuple, list)) and len(e) > 0 and e[0] == "spec")


def lock_path(env, variant="default"):
    """环境描述旁边的锁 <配置文件夹>/specs/<env>.<变体>.lock.json（sites/envlock.py 写）。"""
    return os.path.join(SPECS, f"{env}.{variant}{proto.LOCK_SUFFIX}")


def lock_bytes(path):
    """锁去掉 LOCK_UNHASHED 的键以后的规范字节（键排序、紧凑）；没有这个文件 → None。"""
    import json
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except OSError:
        return None
    d = {k: v for k, v in d.items() if k not in proto.LOCK_UNHASHED}
    return json.dumps(d, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def spec_hash(env, variant="default"):
    """sha256(<env>.yml 的字节 + 这个变体的锁的规范字节)；还没有锁时只算描述；没有这个描述 → None。"""
    import hashlib
    try:
        with open(os.path.join(SPECS, f"{env}.yml"), "rb") as f:
            h = hashlib.sha256(f.read())
    except OSError:
        return None
    lk = lock_bytes(lock_path(env, variant))
    if lk is not None:
        h.update(lk)
    return h.hexdigest()


def spec_base(env):
    """描述里的 base（venv 建在哪个环境上）；没有 → None。"""
    try:
        with open(os.path.join(SPECS, f"{env}.yml"), encoding="utf-8") as f:
            m = re.search(r"""^base:[ \t]*["']?([^\s"'#]+)""", f.read(), re.M)
    except OSError:
        return None
    return m.group(1) if m else None


def spec_variant(name, env):
    """这台机器上 env 用哪个变体：条目写了 ("spec", 变体) 就是它；venv（有 base）跟着这台的基础环境的变体（有那个变体的锁时），
    否则 default。"""
    envs = machine(name)["envs"] or {}
    e = envs.get(env)
    if is_spec(e) and isinstance(e, (tuple, list)) and len(e) > 1:
        return e[1]
    b = spec_base(env)
    if b and is_spec(envs.get(b)):
        v = spec_variant(name, b)
        if v != "default" and os.path.isfile(lock_path(env, v)):
            return v
    return "default"


def ready_envs(name, oks=None):
    """这台机器上能用的环境名：conda / venv 条目都算；"spec" 条目要那边的 <名>.ok（oks：{名: 哈希}）和现在的描述（加这台用的
    变体的锁）对得上。"""
    out = set()
    for env, e in (machine(name)["envs"] or {}).items():
        if not is_spec(e) or ((oks or {}).get(env) and oks[env] == spec_hash(env, spec_variant(name, env))):
            out.add(env)
    return out


def activate(name, env):
    """这台机器上激活环境 env 的 shell 语句（先退出已经激活的 venv）；没有这个环境 → None。"""
    m = machine(name)
    e = (m["envs"] or {}).get(env)
    if e is None:
        return None
    lines = ['if [[ -n ${VIRTUAL_ENV:-} ]]; then PATH=${PATH//"$VIRTUAL_ENV/bin:"/}; unset VIRTUAL_ENV; fi']
    if is_spec(e):  # 建环境时写好的激活语句（sites/envs.py）
        return "\n".join(lines + [f'source {env_root(name)}/{env}/.activate'])
    if e[0] == "venv":
        base = activate(name, e[2])
        if base is None:
            return None
        return base + "\n" + f'source {e[1]}/bin/activate'
    lines += [f'source {m["conda"][0]}/etc/profile.d/conda.sh', f'conda activate {e[1]}']
    return "\n".join(lines)


# ---------------- 命令行 ----------------

EXPAND = ("conda", "repo", "root", "data", "code", "core", "backup", "pueue")  # mtab get 在本机时把 ~ 展开的字段


def _value(name, field):
    if field in ("code", "core"):  # 算出来的：代码包的 current、这台上调度代码的根（core()）
        v = code(name) if field == "code" else core(name)
    else:
        if field not in schema.MACHINE_FIELDS:
            raise FieldError(f"机器表没有字段 {field}（有：{'、'.join(schema.MACHINE_FIELDS)}）")
        v = machine(name)[field]
    if field in EXPAND and name == whoami() and v:
        v = expand(v) if isinstance(v, str) else (expand(v[0]),) + tuple(v[1:])
    if v is None or v is False:
        return ""
    if v is True:
        return "1"
    if isinstance(v, (tuple, list)):
        return " ".join(str(x) for x in v if x is not None)
    if isinstance(v, dict):
        return " ".join(v)
    return str(v)


def fields_text():
    """字段表的文字（taskq machines fields）。"""
    def show(d):
        return "\n".join(f"  {k:<14} {t:<10} {'必写' if df is schema.REQUIRED else repr(df):<22} {doc}" for k, (t, df, doc) in d.items())
    return "\n\n".join([f"机器表（machines.py 的 MACHINES，每台一项）：\n{show(schema.MACHINE_FIELDS)}",
                        f"pueue 执行机的 worker（workers 的每项）：\n{show(schema.WORKER_FIELDS)}",
                        f"policy.py：\n{show(schema.POLICY_FIELDS)}",
                        f"policy.py 的 OUTPUTS：\n{show(schema.OUTPUT_FIELDS)}"])


def main(argv):
    from proto import EXIT
    prog = os.path.basename(sys.argv[0])  # mtab.py，或 taskq machines
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__.split("命令行：\n")[1].replace("mtab.py", prog), file=sys.stderr if not argv else sys.stdout)
        return 2 if not argv else 0
    cmd, args = argv[0], argv[1:]
    me = whoami()

    def err(msg, rc):
        print(f"{prog} {cmd}：{msg}", file=sys.stderr)
        return rc

    def gone(name):
        if name in (".", None):
            return err("认不出本机", 1)
        return err(f"机器表里没有 {name}（有：{'、'.join(MACHINES)}；本机：{me or '认不出'}）", EXIT["gone"])
    usage = {"get": "<名|.> <字段>", "q-env": "<组>", "activate": "<环境> [名|.]", "groups": "[名|.]", "names": "[角色|调度方式]"}
    nargs = {"whoami": (0, 0), "get": (2, 2), "names": (0, 1), "backups": (0, 0), "q-env": (1, 1), "groups": (0, 1),
             "activate": (1, 2), "fields": (0, len(schema.TABLES) + 1)}
    if cmd not in nargs:
        return err(f"不认识的命令（{prog} --help）", 2)
    lo, hi = nargs[cmd]
    if not lo <= len(args) <= hi:
        print(f"用法：{prog} {cmd} {usage.get(cmd, '')}".rstrip(), file=sys.stderr)
        return 2
    if cmd == "whoami":
        if not me:
            return 1
        print(me)
    elif cmd == "fields":
        if not args:
            print(fields_text())
            return 0
        tabs = args[1:] or list(schema.TABLES)
        bad = [n for n in tabs if n not in schema.TABLES]
        if args[0] != "--markdown" or bad:
            return err(f"用法：{prog} fields [--markdown [{' '.join(schema.TABLES)}]]", 2)
        print("\n\n".join(f"<!-- fields:{n} -->\n{schema.markdown(n)}\n<!-- /fields -->" for n in tabs))
    elif cmd == "get":
        name = me if args[0] == "." else args[0]
        if name not in MACHINES:
            return gone(name)
        try:
            print(_value(name, args[1]))
        except FieldError as e:
            return err(str(e), 1)
    elif cmd == "names":
        print("\n".join(names(args[0] if args else None)))
    elif cmd == "backups":
        print("\n".join(backups()))
    elif cmd == "q-env":
        gs = groups(me) if me else {}
        if args[0] not in gs:
            return err(f"{me or '这台机器'} 上没有组 {args[0]}（有：{'、'.join(gs) or '没有'}）", 2)
        _, gpu, env = gs[args[0]]
        w = next((w for w in workers(me) if w["group"] == args[0]), None)
        env = {**(worker_env(w) if w else {"CUDA_VISIBLE_DEVICES": gpu or ""}), **env}
        for k, v in env.items():
            print(f"export {k}=${{{k}:-{v}}}" if k != "CUDA_VISIBLE_DEVICES" else f"export {k}={v}")
    elif cmd == "activate":
        name = me if len(args) < 2 or args[1] == "." else args[1]
        if name not in MACHINES:
            return gone(name)
        a = activate(name, args[0])
        if a is None:
            return err(f"{name} 上没有环境 {args[0]}（有：{'、'.join(machine(name)['envs'] or {}) or '没有'}）", EXIT["gone"])
        print(a)
    elif cmd == "groups":
        name = me if not args or args[0] == "." else args[0]
        if name not in MACHINES:
            return gone(name)
        for g, (n, _, _) in groups(name).items():
            print(f"{g}:{n}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
