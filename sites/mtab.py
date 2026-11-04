#!/usr/bin/env python3
"""机器表（2026-11-04）：配置文件夹里的 machines.py 只放数据（MACHINES、SITES，可以有 RELAY），怎么用这些数据在这里。
数据的字段见 queue/README.md 的"开关和配置"。核心里 import mtab as M，不直接 import machines。

命令行：
  mtab.py whoami                 本机的名字（认不出时退出码 1）
  mtab.py get <名|.> <字段>      字段的值（MACHINES、SITES 都认）；元组、列表用空格连起来；本机的 data、conda、repo、code 把 ~ 展开；
                                 没有这台机器或这个字段时退出码 1
  mtab.py names [角色|调度方式]  MACHINES 里的机器名，每行一个（如 names pueue、names train）
  mtab.py backups                角色 backup 的机器（MACHINES 和 SITES），每行一个
  mtab.py q-env <组>             本机这个组的环境变量（export 语句，queue/q 用）；没有这个组时退出码 2
  mtab.py groups [名|.]          这台（默认本机）的 pueue 组，每行"组名:同时跑几个"（queue/setup.sh 用）
  mtab.py activate <环境> [名|.] 激活这个环境的 shell 语句（eval "$(...)"）；这台机器没有这个环境时退出码 3
"""
import os
import re
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "queue"))
import cfg  # noqa: E402
cfg.use_config()
import machines as _data  # noqa: E402

MACHINES = _data.MACHINES
SITES = getattr(_data, "SITES", {})
RELAY = getattr(_data, "RELAY", None)
if RELAY is None:
    RELAY = {k: v for k, v in SITES.items() if v.get("relay")}
KINDS = ("gpu", "cpu")
SPECS = os.path.join(os.path.dirname(os.path.abspath(_data.__file__)), "specs")  # 按描述建的环境：<配置文件夹>/specs/<环境>.yml


class FieldError(KeyError):
    """机器表里这台机器没有这个字段。"""

    def __str__(self):
        return self.args[0]


def adopt_sources(name):
    """这台的收编来源 → [dict(path=目录（不展开 ~）, site=另一台执行机的名字或 None)]：老数据目录没有 blobs/，执行机的根目录有。"""
    a = machine(name).get("adopt") or []
    out = []
    for x in [a] if isinstance(a, str) else a:
        e = SITES.get(x)
        out.append(dict(path=e["root"].rstrip("/"), site=x) if e else dict(path=x.rstrip("/"), site=None))
    return out


def serves(name, machines):
    """任务说明的 machines（None = 都行）里有这台。"""
    return not machines or name in machines


def worker_of(name, kind):
    """这台跑 kind（任务说明的 device）任务的 worker：pueue → worker / <kind>_worker 条目，Slurm → dict(kind=种类)；没有 → None。"""
    m = machine(name)
    if m["scheduler"] == "slurm":
        return dict(kind=kind) if kind in (m.get("jobs") or ()) else None
    return m.get("worker" if kind == "gpu" else f"{kind}_worker")


def kinds(name):
    """这台能跑的任务种类（有序）。"""
    return tuple(k for k in KINDS if worker_of(name, k))


def expand(path):
    return os.path.expanduser(path)


def machine(name):
    """MACHINES、RELAY 或 SITES 里的一台（RELAY 先于 SITES：测试会替换里面的条目）。"""
    return MACHINES.get(name) or RELAY.get(name) or SITES[name]


def whoami():
    """本机的名字：先按主机名，认不出时按哪台机器的数据目录在本机存在。都不是时返回 None。"""
    h = socket.gethostname().split(".")[0]
    for name, m in MACHINES.items():
        if re.match(m["hostname"], h):
            return name
    for name, m in MACHINES.items():
        if m.get("data") and os.path.isdir(expand(m["data"])):
            return name
    return None


def hub():
    return next(n for n, m in MACHINES.items() if "hub" in m["roles"])


def names(kind=None):
    return [n for n, m in MACHINES.items() if kind is None or kind in m["roles"] or kind == m["scheduler"]]


def backups():
    """角色 backup 的机器：MACHINES 里的，加上 SITES 里的（11/3 起备份在执行机上）。"""
    return [n for n, m in {**MACHINES, **SITES}.items() if "backup" in (m.get("roles") or ())]


def machine_of_jobtag(tag):
    """训练分段日志里的作业名 → 机器的 label：纯数字是 Slurm 的作业号，其他是 run_task.sh 写的"主机名-时间"。"""
    if re.fullmatch(r"\d+", tag):
        labels = sorted({m["label"] for m in {**MACHINES, **SITES}.values() if m["scheduler"] == "slurm"})
        pre = os.path.commonprefix(labels).strip()
        return labels[0] if len(labels) == 1 else (pre or "Slurm")
    for m in MACHINES.values():
        if m["scheduler"] != "slurm" and re.match(m["hostname"], tag):
            return m["label"]
    return tag


def is_spec(e):
    return e == "spec" or (isinstance(e, (tuple, list)) and len(e) > 0 and e[0] == "spec")


def env_root(m):
    """"spec" 环境建在哪个目录下（不展开 ~）。"""
    return (m.get("env_root") or (m["root"].rstrip("/") + "/envs" if m.get("root") else "~/envs/spec")).rstrip("/")


def spec_hash(env):
    """<配置文件夹>/specs/<env>.yml 内容的 sha256；没有这个描述 → None。"""
    import hashlib
    try:
        with open(os.path.join(SPECS, f"{env}.yml"), "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except OSError:
        return None


def ready_envs(name, oks=None):
    """这台机器上能用的环境名：conda / venv 条目都算；"spec" 条目要那边的 <名>.ok（oks：{名: 哈希}）和现在的描述对得上。"""
    out = set()
    for env, e in (machine(name).get("envs") or {}).items():
        if not is_spec(e) or ((oks or {}).get(env) and oks[env] == spec_hash(env)):
            out.add(env)
    return out


def activate(name, env):
    """这台机器上激活环境 env 的 shell 语句（先退出已经激活的 venv）；没有这个环境 → None。"""
    m = machine(name)
    e = (m.get("envs") or {}).get(env)
    if e is None:
        return None
    lines = ['if [[ -n ${VIRTUAL_ENV:-} ]]; then PATH=${PATH//"$VIRTUAL_ENV/bin:"/}; unset VIRTUAL_ENV; fi']
    if is_spec(e):  # 建环境时写好的激活语句（sites/envs.py）
        return "\n".join(lines + [f'source {env_root(m)}/{env}/.activate'])
    if e[0] == "venv":
        base = activate(name, e[2])
        if base is None:
            return None
        return base + "\n" + f'source {e[1]}/bin/activate'
    lines += [f'source {m["conda"][0]}/etc/profile.d/conda.sh', f'conda activate {e[1]}']
    return "\n".join(lines)


def _value(name, field):
    m = machine(name)
    if field not in m:
        raise FieldError(f"机器表里 {name} 没有字段 {field}")
    v = m[field]
    if field in ("data", "conda", "repo", "code") and name == whoami() and v:
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


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__.split("命令行：\n")[1], file=sys.stderr if not argv else sys.stdout)
        return 2 if not argv else 0
    cmd, args = argv[0], argv[1:]
    me = whoami()
    if cmd == "whoami":
        if not me:
            return 1
        print(me)
    elif cmd == "get":
        if len(args) != 2:
            print("用法：mtab.py get <名|.> <字段>", file=sys.stderr)
            return 2
        name = me if args[0] == "." else args[0]
        if name not in MACHINES and name not in SITES:
            print(f"machines.py：不认识的机器 {args[0]}（本机：{me or '认不出'}）", file=sys.stderr)
            return 1
        try:
            print(_value(name, args[1]))
        except FieldError as e:
            print(f"machines.py：{e}", file=sys.stderr)
            return 1
    elif cmd == "names":
        print("\n".join(names(args[0] if args else None)))
    elif cmd == "backups":
        print("\n".join(backups()))
    elif cmd == "q-env":
        groups = MACHINES[me]["groups"] if me else {}
        if args[0] not in groups:
            print(f"{me or '这台机器'} 上没有组 {args[0]}（有：{'、'.join(groups) or '没有'}）", file=sys.stderr)
            return 2
        _, gpu, env = groups[args[0]]
        print(f"export CUDA_VISIBLE_DEVICES={gpu or ''}")
        for k, v in env.items():
            print(f"export {k}=${{{k}:-{v}}}")
    elif cmd == "activate":
        name = me if len(args) < 2 or args[1] == "." else args[1]
        if name not in MACHINES:
            print(f"machines.py：不认识的机器（本机：{me or '认不出'}）", file=sys.stderr)
            return 1
        a = activate(name, args[0])
        if a is None:
            print(f"{name} 上没有环境 {args[0]}（有：{'、'.join(MACHINES[name].get('envs') or {}) or '没有'}）", file=sys.stderr)
            return 3
        print(a)
    elif cmd == "groups":
        name = me if not args or args[0] == "." else args[0]
        for g, (n, _, _) in ((machine(name).get("groups") or {}) if name else {}).items():
            print(f"{g}:{n}")
    else:
        print(f"machines.py：不认识的命令 {cmd}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
