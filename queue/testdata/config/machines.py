#!/usr/bin/env python3
"""测试用的机器表（2026-11-02）：形状和字段同真的机器表（字段说明见 scripts/queue/README.md 的"开关和配置"），
机器名同真的，环境名、目录、作业参数是虚构的。conda 指向 testdata/conda（假的 conda.sh：activate 只记下环境名）。
LOGIC 以下的函数和真的机器表逐字相同（research/common/taskq_plugins/tests/test_fixture_sync.py 核对）。

命令行同真的机器表：whoami、get、site-root、names、q-env、groups、activate。
"""
import os
import re
import socket
import sys

FAKE = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if "__file__" in globals() else "/nonexistent"
FAKE_CONDA = FAKE + "/conda"
WS_TASK_VARS = {"DEMO_FAST": "1", "DEMO_FAST_LOADER": "1"}

MACHINES = {
    "box": dict(label="box", hostname=r"hub-host", ssh=None, scheduler="pueue", repo="~/auto/demo",
                code="~/.cache/phd/code/current",
                data="/data/others/DEMO_DATA", conda=(FAKE_CONDA, "base0"),
                envs={"base": ("conda", "base0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                      "alt2": ("venv", FAKE + "/venv/alt2", "base"), "stats": ("conda", "stats0"),
                      "x1": ("venv", FAKE + "/venv/x1", "base"), "x2": ("conda", "x2")},
                groups={"gpu": (1, "0", {"TASK_WAIT_GPU": "600"}), "cpu": (1, None, {}), "wait": (0, None, {})},
                roles=("hub",), disk=("/data/others/DEMO_DATA", 100, None), gpu_log=False,
                transfer=("https://transfer.example.org", "172.18.0.1:18090"),
                quick=("cloudflare/cloudflared:2026.10.4", r"https://[a-z0-9-]+\.trycloudflare\.com")),
    "lab": dict(label="lab", hostname=r"lab-host", ssh="lab", scheduler="pueue", repo="~/demo", code=None,
                data="~/DEMO_DATA", conda=(FAKE_CONDA, "w0"),
                envs={"base": ("conda", "w0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                      "alt2": ("venv", FAKE + "/venv/alt2", "base")},
                groups={"gpu1": (1, "1", {}), "gpu0": (1, "0", {}), "cpu": (1, None, {}),
                        "wait": (0, None, {})},
                roles=("train",), disk=("/mnt/disk1", 40, 15), gpu_log=True, tunnel=("push",), task_vars=WS_TASK_VARS),
    "dicc": dict(label="DICC", hostname=r"(login|gpu|cpu)\d+", ssh="dicc", scheduler="slurm", repo="~/Projects/demo", code=None,
                 data="~/st/DEMO_DATA", conda=(FAKE_CONDA, "base0"),
                 envs={"base": ("conda", "base0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                       "alt2": ("venv", FAKE + "/venv/alt2", "base"), "x3": ("venv", FAKE + "/venv/x3", "base"),
                       "x1": ("venv", FAKE + "/venv/x1", "base"), "stats": ("conda", "stats0")},
                 groups={}, roles=("train", "backup"), disk=None, gpu_log=False,
                 jobs=("gpu", "cpu"), tunnel=("push", "pull"),
                 transfer_job="-p cpu-part --qos=normal -c 2 --mem=2G",
                 env_job="-p cpu-part --qos=normal -c 8 --mem=16G -t 120"),
}

SITES = {
    "dicc2": dict(label="DICC2", hostname=r"(login|gpu|cpu)\d+", ssh="dicc2", scheduler="slurm", root="~/q", cap=90, budget=(15, 30),
                   conda=(FAKE_CONDA, "pt"), envs={"base": ("conda", "pt")}, roles=("train",),
                   jobs=("gpu",), ssh_master=True, sbatch=dict(gpu=dict(cpus=30, mem="60G")), tunnel=("push", "pull"), quick_tunnel=True,
                   transfer_job="-p cpu-part --qos=normal -c 4 --mem=4G", relay=True),
    "lab1": dict(label="lab 1 号卡", hostname=r"(?!)", ssh="lab", scheduler="pueue", root="/mnt/disk1/datasets/ee09fdd2",
                 conda=(FAKE_CONDA, "w0"), envs={"base": ("conda", "w0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                       "alt2": ("venv", FAKE + "/venv/alt2", "base")}, roles=("train",),
                 groups={"gpu1": (1, "1", {}), "cpu": (1, None, {})}, worker=dict(group="gpu1", id="w7e2837"), slots=1,
                 cpu_worker=dict(group="cpu", id="w1dadee", cpus=8),
                 pueue="~/.local/bin/pueue", tunnel=("push", "pull"), adopt=["~/DEMO_DATA", "lab0"], budget=(100, 50),
                 task_vars=WS_TASK_VARS, stage_tmp=False, relay=True),
    "dicc1": dict(label="DICC", hostname=r"(login|gpu|cpu)\d+", ssh="dicc", scheduler="slurm", root="~/q", cap=85, budget=(15, 30),
                  conda=(FAKE_CONDA, "base0"),
                  envs={"base": ("conda", "base0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                        "alt2": ("venv", FAKE + "/venv/alt2", "base"), "x3": ("venv", FAKE + "/venv/x3", "base"),
                        "x1": ("venv", FAKE + "/venv/x1", "base"), "stats": ("conda", "stats0")}, roles=("train",),
                  jobs=("gpu", "cpu"),
                  ssh_master=True, tunnel=("push", "pull"), replaces="dicc",
                  transfer_job="-p cpu-part --qos=normal -c 2 --mem=2G", relay=True),
    "lab0": dict(label="lab 0 号卡", hostname=r"(?!)", ssh="lab", scheduler="pueue", root="/mnt/disk1/datasets/d0eab39f",
                 conda=(FAKE_CONDA, "w0"), envs={"base": ("conda", "w0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                       "alt2": ("venv", FAKE + "/venv/alt2", "base")}, roles=("train",),
                 groups={"gpu0": (1, "0", {"TASK_GPU_IDLE": "60"})}, worker=dict(group="gpu0", id="wd2c85f"), slots=1,
                 pueue="~/.local/bin/pueue", tunnel=("push", "pull"), adopt=["~/DEMO_DATA", "lab1"],
                 budget=(100, 50), task_vars=WS_TASK_VARS, stage_tmp=False, relay=True),
}
RELAY = {k: v for k, v in SITES.items() if v.get("relay")}


# ---------------- LOGIC ----------------
def adopt_sources(name):
    """这台的收编来源 → [dict(path=目录（不展开 ~）, site=另一台执行机的名字或 None)]：老数据目录没有 blobs/，执行机的根目录有。"""
    a = machine(name).get("adopt") or []
    out = []
    for x in [a] if isinstance(a, str) else a:
        e = globals().get("SITES", {}).get(x)
        out.append(dict(path=e["root"].rstrip("/"), site=x) if e else dict(path=x.rstrip("/"), site=None))
    return out


def successor(name):
    """接替 MACHINES 里 name 的执行机（SITES 的 replaces，relay=True）；没有 → None。"""
    return next((n for n, e in globals().get("SITES", {}).items() if e.get("replaces") == name and e.get("relay")), None)


def serves(name, machines):
    """任务说明的 machines（None = 都行）里有这台，或者有它接替的那台。"""
    return not machines or name in machines or machine(name).get("replaces") in machines


KINDS = ("gpu", "cpu")


def worker_of(name, kind):
    """这台跑 kind（任务说明的 device）任务的 worker：pueue → worker / <kind>_worker 条目，Slurm → dict(kind=种类)；没有 → None。"""
    m = machine(name)
    if m["scheduler"] == "slurm":
        return dict(kind=kind) if kind in (m.get("jobs") or ()) else None
    return m.get("worker" if kind == "gpu" else f"{kind}_worker")


def kinds(name):
    """这台能跑的任务种类（有序）。"""
    return tuple(k for k in KINDS if worker_of(name, k))


def site_root(name):
    """接替 MACHINES 里 name 的执行机（SITES 的 replaces）的根目录，~ 展开；没有 → None。"""
    n = successor(name)
    return expand(SITES[n]["root"]) if n else None


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
        if os.path.isdir(expand(m["data"])):
            return name
    return None


def hub():
    return next(n for n, m in MACHINES.items() if "hub" in m["roles"])


def names(kind=None):
    return [n for n, m in MACHINES.items() if kind is None or kind in m["roles"] or kind == m["scheduler"]]


def machine_of_jobtag(tag):
    """训练分段日志里的作业名 → 机器的 label：纯数字是 Slurm 的作业号，其他是 run_task.sh（10/18 前 train_folds.sbatch）写的"主机名-时间"。"""
    if re.fullmatch(r"\d+", tag):
        slurm = names("slurm")
        return MACHINES[slurm[0]]["label"] if len(slurm) == 1 else "Slurm"
    for m in MACHINES.values():
        if m["scheduler"] != "slurm" and re.match(m["hostname"], tag):
            return m["label"]
    return tag


SPECS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "specs") if "__file__" in globals() else ""


def is_spec(e):
    return e == "spec" or (isinstance(e, (tuple, list)) and len(e) > 0 and e[0] == "spec")


def env_root(m):
    """"spec" 环境建在哪个目录下（不展开 ~）。"""
    return (m.get("env_root") or (m["root"].rstrip("/") + "/envs" if m.get("root") else "~/envs/spec")).rstrip("/")


def spec_hash(env):
    """envs/specs/<env>.yml 内容的 sha256；没有这个描述 → None。"""
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
    if is_spec(e):  # 建环境时写好的激活语句（scripts/sites/envs.py）
        return "\n".join(lines + [f'source {env_root(m)}/{env}/.activate'])
    if e[0] == "venv":
        base = activate(name, e[2])
        if base is None:
            return None
        return base + "\n" + f'source {e[1]}/bin/activate'
    lines += [f'source {m["conda"][0]}/etc/profile.d/conda.sh', f'conda activate {e[1]}']
    return "\n".join(lines)


def _value(name, field):
    v = MACHINES[name][field]
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
    if not argv:
        print(__doc__.split("命令行")[1], file=sys.stderr)
        return 2
    cmd, args = argv[0], argv[1:]
    me = whoami()
    if cmd == "whoami":
        if not me:
            return 1
        print(me)
    elif cmd == "get":
        name = me if args[0] == "." else args[0]
        if name not in MACHINES:
            print(f"machines.py：不认识的机器 {args[0]}（本机：{me or '认不出'}）", file=sys.stderr)
            return 1
        print(_value(name, args[1]))
    elif cmd == "site-root":
        r = site_root(me if args[0] == "." else args[0]) if args else None
        if not r:
            return 1
        print(r)
    elif cmd == "names":
        print("\n".join(names(args[0] if args else None)))
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
        for g, (n, _, _) in (MACHINES[me]["groups"] if me else {}).items():
            print(f"{g}:{n}")
    else:
        print(f"machines.py：不认识的命令 {cmd}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
