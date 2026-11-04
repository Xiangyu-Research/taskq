#!/usr/bin/env python3
"""执行机（envs/machines.py 的 RELAY）用的代码包（2026-10-30；11/4 起只装执行机要用的、只按内容命名）：
调度核心里执行机要用的文件（RUNTIME：worker、run_task.sh、队列、任务说明），加上提交里跟踪的 envs/policy.py 的 OUTPUTS["bundle"]
（使用者的文件夹或文件）下的代码（.py .sh .R .sbatch，1 MB 以内的 .json .csv .txt .yaml .yml），去掉 OUTPUTS["bundle_skip"] 的通配
（测试、只在 box 上用的）；另外生成只有这一台的 envs/machines.py（去掉说明和整行注释）、envs/policy.py（OUTPUTS 只留 experiments），
Slurm 类按 jobs 的每种 worker 生成 sbatch（gpu → worker.sbatch，cpu → cpu_worker.sbatch，不靠 git 找代码）；家目录下的状态文件夹
（cfg.py 的 STATE_HOME）换成那边根目录下的 queue/、state/。
核对：本机和 OUTPUTS["bundle_check"] 那个环境的两个 Python 版本都能编译，bash -n 通过，没有钥匙（私钥、常见的 token）。
包名 = 内容哈希的前 16 位，不带提交号：只改 box 上的代码或文档时包名不变，执行机不换包；同样的内容只建一次。
哪个提交建的包记在 relay.py 的状态 bundle_for（<名字>:<提交> → 包名）。

    relay_bundle.py build <名字> [--commit 提交] [--out 目录]   → 打印"包名 路径"
    （--commit 默认 origin/main；--commit '' = 工作区，只给测试用）
"""
import argparse
import ast
import fnmatch
import hashlib
import io
import os
import pprint
import re
import shutil
import subprocess
import sys
import tempfile
import tokenize

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import machines as M  # noqa: E402
import policy as P  # noqa: E402
import remote as RM  # noqa: E402

OUT = cfg.state("relay", "bundles", expand=False)
TOPS = tuple(getattr(P, "OUTPUTS", {}).get("bundle", ("envs",)))  # envs/policy.py 的 OUTPUTS：使用者的文件夹或文件
SKIP = tuple(getattr(P, "OUTPUTS", {}).get("bundle_skip", ()))  # 不进包的通配（fnmatch，* 跨目录）
# 调度核心里执行机要用的：worker 的入口和循环、跑任务、队列、任务说明、实验表、显存填充、输入的本地副本；
# 其余（box 上的计划、传数据、健康检查、同步）不进包
RUNTIME = tuple(os.path.normpath(os.path.join(os.path.relpath(CORE, REPO), "queue", f)) for f in (
    "worker_start.sh", "dicc_worker.sh", "run_task.sh", "gpu_pad_start.sh", "gpu_pad.py", "taskq.py", "spec.py", "common.py",
    "cfg.py", "taskq_api.py", "recipes.py", "experiments.py", "steps.py", "inputs.py", "worker.sbatch", "cpu_worker.sbatch"))
CODE = (".py", ".sh", ".R", ".sbatch")
SMALL = (".json", ".csv", ".txt", ".yaml", ".yml")
SMALL_MAX = 1 << 20
SBATCH = {"gpu": cfg.rel("queue", "worker.sbatch"), "cpu": cfg.rel("queue", "cpu_worker.sbatch")}  # worker 的种类 → 模板（也是包里的位置）
# 已经交给 Slurm、还在排队的老作业脚本里写的是改名时的 start.sh；running 的老 worker 换包时找 loop.sh
# 等 2026-11 前交的作业都结束后删
LINKS = {"scripts/queue/start.sh": "worker_start.sh", "scripts/queue/loop.sh": "dicc_worker.sh"}
SECRETS = [
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{36}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{60,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"),
    re.compile(r"\bsk-(?:ant-|proj-)?[A-Za-z0-9_-]{32,}"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
    re.compile(r"\btk_[a-z0-9]{29}\b"),
    re.compile(r"\beyJ[A-Za-z0-9_-]{20,}\.eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}"),
]


class BundleError(Exception):
    pass


# ---------------- 取文件 ----------------
def git(*args, binary=False, input=None):
    r = subprocess.run(["git", "-C", REPO, *args], capture_output=True, input=input)
    if r.returncode:
        raise BundleError(f"git {' '.join(args)}：{r.stderr.decode(errors='replace').strip()}")
    return r.stdout if binary else r.stdout.decode()


def _wanted(path, size):
    if "__pycache__" in path or not (path.endswith(CODE) or path.endswith(SMALL) and size <= SMALL_MAX):
        return False
    return path in RUNTIME or not any(fnmatch.fnmatchcase(path, x) for x in SKIP)


def source_files(commit):
    """{相对路径: (内容 bytes, 可执行)}；commit 为 None 时读工作区（git 跟踪的文件）。"""
    out = {}
    if commit is None:
        for ent in git("ls-files", "-z", "-s", "--", *TOPS, *RUNTIME).split("\0"):
            if not ent:
                continue
            meta, path = ent.split("\t", 1)
            p = os.path.join(REPO, path)
            if meta.startswith("100") and os.path.isfile(p) and _wanted(path, os.path.getsize(p)):
                out[path] = (open(p, "rb").read(), os.access(p, os.X_OK))
        return out
    objs = []
    for ent in git("ls-tree", "-r", "-l", "-z", commit, "--", *TOPS, *RUNTIME).split("\0"):
        if not ent:
            continue
        meta, path = ent.split("\t", 1)
        mode, kind, oid, size = meta.split()
        if kind == "blob" and mode.startswith("100") and _wanted(path, int(size)):
            objs.append((path, oid, mode == "100755"))
    data = git("cat-file", "--batch", binary=True, input="".join(f"{o}\n" for _, o, _ in objs).encode())
    pos = 0
    for path, oid, exe in objs:
        nl = data.index(b"\n", pos)
        n = int(data[pos:nl].split()[2])
        out[path] = (data[nl + 1:nl + 1 + n], exe)
        pos = nl + 1 + n + 1
    return out


def read(commit, path):
    return open(os.path.join(REPO, path), "rb").read().decode() if commit is None else git("show", f"{commit}:{path}")


def read_machines(commit):
    """配置文件夹在仓库里时取提交里的 machines.py，在仓库外（TASKQ_CONFIG）时读文件。"""
    rel = os.path.relpath(os.path.realpath(cfg.config_dir()), os.path.realpath(REPO))
    if rel == ".." or rel.startswith("../"):
        return open(os.path.join(cfg.config_dir(), "machines.py"), encoding="utf-8").read()
    return read(commit, f"{rel}/machines.py")


def relay_entry(commit, name):
    ns = {"__name__": "relay_machines"}
    exec(compile(read_machines(commit), "machines.py", "exec"), ns)
    relay = ns.get("RELAY") or {}
    if name not in relay:
        raise BundleError(f"{commit or '工作区'} 的 envs/machines.py 的 RELAY 里没有 {name}")
    return relay[name]


# ---------------- 生成的文件 ----------------
def machine_entry(name, e, root, entry=None):
    m = dict(label=name, hostname=e["hostname"], ssh=None, scheduler=e["scheduler"], repo=f"{root}/code/current", code=None,
             data=f"{root}/data", conda=tuple(e["conda"]), envs=dict(e["envs"]), groups=dict(e.get("groups") or {}),
             roles=("train",), disk=None, gpu_log=False, jobs=tuple(e.get("jobs") or ()), sbatch=dict(e.get("sbatch") or {}))
    m.update({k: e[k] for k in ("slots", "worker", "cpu_worker", "task_vars") if k in e})
    m["env_root"] = M.env_root(e)  # 按描述建的环境（scripts/taskq/sites/envs.py）在根目录下
    m.update(entry or {})
    return m


def gen_machines(src, name, m):
    """把 envs/machines.py 的 MACHINES 换成只有这一项，SITES、RELAY 换成空的（查别的机器是 KeyError，11/4），
    开头的说明换成一句、删掉整行注释（改说明时包名不变）。"""
    lines = src.split("\n")
    spans = {}
    body = ast.parse(src).body
    for node in body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            spans[node.targets[0].id] = (node.lineno - 1, node.end_lineno - 1)
    if "MACHINES" not in spans:
        raise BundleError("envs/machines.py 里找不到 MACHINES")
    repl = {spans["MACHINES"]: "MACHINES = " + pprint.pformat({name: m}, sort_dicts=False, width=120)}
    for k in ("SITES", "RELAY"):
        if k in spans:
            repl[spans[k]] = f"{k} = {{}}"
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
        repl[(body[0].lineno - 1, body[0].end_lineno - 1)] = f'"""{name} 的机器表（relay_bundle.py 生成）。\n\n命令行：见仓库里的 envs/machines.py。\n"""'
    for (r0, r1), text in sorted(repl.items(), reverse=True):
        lines[r0:r1 + 1] = [] if text is None else text.split("\n")
    return drop_comment_lines("\n".join(lines))


def drop_comment_lines(src):
    """去掉只有注释的行（第一行 #! 留着）。"""
    comment, code = set(), set()
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            comment.add(tok.start[0])
        elif tok.type not in (tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.ENDMARKER):
            code.update(range(tok.start[0], tok.end[0] + 1))
    drop = {n for n in comment - code if n > 1}
    return "\n".join(ln for n, ln in enumerate(src.split("\n"), 1) if n not in drop)


def gen_policy(name, stage_tmp=True):
    out = {k: P.OUTPUTS[k] for k in ("experiments",) if k in getattr(P, "OUTPUTS", {})}
    return (f"AUTO = {{}}\nHOLD = []\nPLACE = {{}}\n"
            f"STAGE_TMP = {{{name!r}: {stage_tmp!r}}}\n"
            "SLURM = {}\n" + (f"OUTPUTS = {out!r}\n" if out else ""))


def sbatch_over(m, kind):
    """machines.py 条目的 sbatch 里这种 worker 的 CPU、内存（老格式 sbatch=dict(cpus=…, mem=…) 算 gpu 的）。"""
    sb = m.get("sbatch") or {}
    if isinstance(sb.get(kind), dict):
        return sb[kind]
    return sb if kind == "gpu" and not any(isinstance(v, dict) for v in sb.values()) else {}


def gen_sbatch(real, m, root, kind="gpu"):
    over = {"cpus-per-task": sbatch_over(m, kind).get("cpus"), "mem": sbatch_over(m, kind).get("mem")}
    keep = []
    for ln in real.splitlines():
        o = re.match(r"#SBATCH\s+--([\w-]+)", ln)
        if not o or o.group(1) in ("job-name", "output"):
            continue
        keep.append(f"#SBATCH --{o.group(1)}={over[o.group(1)]}" if over.get(o.group(1)) else ln)
    if kind not in (m.get("jobs") or ()):
        raise BundleError(f"RELAY 的 jobs 里没有 {kind}")
    # Slurm 不展开 ~：根目录以 ~ 开头时日志写相对路径（在 <根>/code/current/<核心>/queue 交，嵌着时往上 5 级是根目录）
    up = "/".join([".."] * (2 + len(cfg.rel("queue").split("/"))))
    logs = (up if root.startswith("~") else root) + "/data/logs/workers/%x_%j.out"
    return "\n".join(["#!/bin/bash", *keep,
                      f"#SBATCH --output={logs}",
                      f"R={RM.q(root)}",
                      'REPO=$(readlink -f "$R/code/current")',
                      'export TASKQ_DIR=$R/queue TASKQ_GPU_PAD_SWITCH=$R/queue/gpu_pad_DISABLED'
                      + (" WORKER_DEVICE=cpu" if kind == "cpu" else ""),
                      f'exec bash "$REPO/{cfg.rel("queue", "worker_start.sh")}"', ""])


HOME_MAP = {"taskq": "queue", "gpu_pad/DISABLED": "queue/gpu_pad_DISABLED"}  # cfg.py 的 STATE_PLACES → 根目录下的位置；其余 → state/


def home_paths(text, root, path=""):
    """家目录下的状态文件夹（cfg.py 的 STATE_HOME 的默认写法）换成根目录下的：任务队列 → queue/，显存填充的开关 →
    queue/gpu_pad_DISABLED，其余 → state/。只换代码（.py 的注释、文档字符串和别的文件里 # 开头的行不换，11/4）。"""
    h = cfg.STATE_HOME[2:]

    def sub(s):
        for pre in ("$HOME/", "~/"):
            r = pre + root[2:] if root.startswith("~/") else root  # 根目录在家目录下时沿用原来的写法（双引号里的 ~ 不展开）
            for k, v in HOME_MAP.items():
                s = s.replace(pre + h + k, f"{r}/{v}")
            s = s.replace(pre + h, f"{r}/state/")
        return s
    keep = _prose(text) if path.endswith(".py") else None
    if keep is None:
        return "\n".join(ln if ln.lstrip().startswith("#") and not ln.startswith("#!") else sub(ln) for ln in text.split("\n"))
    out, pos = [], 0
    for a, b in keep:
        out += [sub(text[pos:a]), text[a:b]]
        pos = b
    return "".join(out + [sub(text[pos:])])


def _prose(src):
    """Python 源码里注释和文档字符串的位置 [(起, 止)]（字符偏移）；解析不了 → None。"""
    starts = [0]
    for ln in io.StringIO(src).readlines():
        starts.append(starts[-1] + len(ln))
    off = lambda rc: starts[rc[0] - 1] + rc[1]  # noqa: E731
    spans = []
    try:
        toks = list(tokenize.generate_tokens(io.StringIO(src).readline))
    except (tokenize.TokenError, SyntaxError):
        return None
    prev = tokenize.NEWLINE
    for i, tok in enumerate(toks):
        if tok.type == tokenize.COMMENT:
            spans.append((off(tok.start), off(tok.end)))
        elif tok.type == tokenize.STRING and prev in (tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT) \
                and toks[i + 1].type in (tokenize.NEWLINE, tokenize.ENDMARKER):
            spans.append((off(tok.start), off(tok.end)))
        if tok.type not in (tokenize.NL, tokenize.COMMENT):
            prev = tok.type
    return spans


# ---------------- 核对 ----------------
def check_python():
    """本机 OUTPUTS["bundle_check"] 那个环境的 python（和那边的 conda 环境同一个 Python 版本），用来核对能编译；没写时 None。"""
    me = M.whoami()
    m = M.MACHINES.get(me) or {}
    e = (m.get("envs") or {}).get(getattr(P, "OUTPUTS", {}).get("bundle_check"))
    if not e or e[0] != "conda":
        return None
    p = os.path.join(M.expand(m["conda"][0]), "envs", e[1], "bin", "python")
    return p if os.path.exists(p) else None


def secret_hits(files):
    """{路径: (文本, …)} 里像钥匙的地方 → [(路径, 行号, 命中的前 12 个字符)]。"""
    out = []
    for f, (text, *_) in sorted(files.items()):
        for n, ln in enumerate(text.split("\n"), 1):
            out += [(f, n, m.group(0)[:12]) for rx in SECRETS for m in rx.finditer(ln)]
    return out


def check(d, files):
    pys = [os.path.join(d, f) for f in files if f.endswith(".py")]
    prog = "import sys\nfor f in sys.argv[1:]:\n    compile(open(f, 'rb').read(), f, 'exec')\n"
    for py in filter(None, [sys.executable, check_python()]):
        r = subprocess.run([py, "-c", prog, *pys], capture_output=True, text=True)
        if r.returncode:
            raise BundleError(f"{py} 编译不过：{r.stderr.strip()[-500:]}")
    for f in files:
        if f.endswith((".sh", ".sbatch")):
            r = subprocess.run(["bash", "-n", os.path.join(d, f)], capture_output=True, text=True)
            if r.returncode:
                raise BundleError(f"bash -n {f}：{r.stderr.strip()}")


# ---------------- 建包 ----------------
def contents(name, commit="origin/main", root=None, entry=None):
    """→ {相对路径: (文本, 可执行)}（不含 LINKS 的软链接）。root、entry（覆盖生成的 machines.py 那一项的字段）只给测试用。"""
    e = relay_entry(commit, name)
    root = (root or e["root"]).rstrip("/")
    m = machine_entry(name, e, root, entry)
    out = {}
    for path, (raw, exe) in source_files(commit).items():
        try:
            text = raw.decode()
        except UnicodeDecodeError as err:
            raise BundleError(f"{path} 不是 UTF-8：{err}") from None
        out[path] = (home_paths(text, root, path), exe or path.endswith(".sh"))
    out["envs/machines.py"] = (home_paths(gen_machines(read_machines(commit), name, m), root, "envs/machines.py"), True)
    out["envs/policy.py"] = (gen_policy(name, e.get("stage_tmp", True)), False)
    for tpl in SBATCH.values():
        out.pop(tpl, None)
    if m["scheduler"] == "slurm":
        for kind, tpl in SBATCH.items():
            if kind in (m.get("jobs") or ()):
                out[tpl] = (gen_sbatch(read(commit, tpl), m, root, kind), False)
    hits = secret_hits(out)
    if hits:
        raise BundleError("包里有像钥匙的内容：" + "；".join(f"{f}:{n} {w}…" for f, n, w in hits[:10]))
    return out


def bundle_id(files):
    h = hashlib.sha256()
    for path in sorted(files):
        data = files[path][0].encode()
        h.update(f"{path}\0{len(data)}\0{int(bool(files[path][1]))}\0".encode() + data)
    for path, target in sorted(LINKS.items()):
        h.update(f"{path}\0->{target}\0".encode())
    return h.hexdigest()[:16]


def build(name, commit="origin/main", out=None, root=None, entry=None):
    """建包 → (包名, 路径)。commit=None：读工作区（只给测试用）。已经有同名的包时直接用。"""
    files = contents(name, commit, root, entry)
    bid = bundle_id(files)
    out = os.path.expanduser(out or OUT)
    path = os.path.join(out, bid)
    if os.path.isdir(path):
        return bid, path
    os.makedirs(out, exist_ok=True)
    tmp = tempfile.mkdtemp(prefix=f".{bid}.", dir=out)
    try:
        for rel, (text, exe) in files.items():
            p = os.path.join(tmp, rel)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "w", encoding="utf-8") as f:
                f.write(text)
            os.chmod(p, 0o755 if exe else 0o644)
        for rel, target in LINKS.items():
            if os.path.dirname(rel) + "/" + target in files and rel not in files:
                os.symlink(target, os.path.join(tmp, rel))
        check(tmp, sorted(files))
        os.chmod(tmp, 0o755)
        os.rename(tmp, path)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        if os.path.isdir(path):
            return bid, path
        raise
    return bid, path


def main(argv):
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("build")
    p.add_argument("name")
    p.add_argument("--commit", default="origin/main")
    p.add_argument("--out")
    a = ap.parse_args(argv)
    try:
        bid, path = build(a.name, a.commit or None, a.out)
    except BundleError as e:
        print(f"建不了包：{e}", file=sys.stderr)
        return 1
    print(bid, path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
