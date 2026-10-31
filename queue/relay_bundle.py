#!/usr/bin/env python3
"""中转执行机（envs/machines.py 的 RELAY）用的代码包（2026-10-30）：从某个提交取出 worker 训练要用的脚本，去掉注释和文档字符串，
envs/machines.py 换成只有那个账号的一项、envs/policy.py 只留 STAGE_TMP、worker.sbatch 不靠 git 找代码；家目录下的
~/.cache/phd/… 换成那边根目录下的 queue/、state/。改动都核对过：去注释前后语法树相同（Python 比 ast，shell 比
declare -f），两个 Python 版本都能编译，bash -n 通过，没有 FORBIDDEN 里的字样。包名 = 内容的哈希，同样的内容只建一次。

    relay_bundle.py build <名字> [--commit 提交] [--out 目录]   → 打印"包名 路径"
    （--commit 默认 origin/main；--commit '' = 工作区，只给测试用）
"""
import argparse
import ast
import hashlib
import io
import os
import pprint
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import tokenize

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "envs"))
import machines as M  # noqa: E402

OUT = "~/.cache/phd/relay/bundles"
KEEP = [f"scripts/queue/{f}" for f in ("worker_start.sh", "gpu_pad_start.sh", "gpu_pad.py", "dicc_worker.sh", "taskq.py",
                                       "run_task.sh", "spec.py", "recipes.py", "experiments.py", "common.py")]
KEEP += ["research/common/nnunet/train.sh", "research/common/nnunet/stage_tmp.sh"]
# 推理（nnunet_infer，lab1 训的折在那边推理，10/31）
KEEP += ["research/RQ1/analysis/infer_task.sh", "research/RQ1/analysis/run_predict_eval.sh", "research/RQ1/analysis/write_manifest.py",
         "research/RQ1/nnunet/predict.sh", "research/common/sparsify_bg.sh", "research/common/sparsify_probs.py", "envs/nnunet_paths.sh"]
KEEP_DIRS = ["research/common/nnunet/trainers", "research/common/nnunet/nets"]
FORBIDDEN = ["user-x", "name-y", "st/BRAIN_TUMOR", "github", "transfer-x", "lab-host", "BEGIN OPENSSH",
             "PRIVATE KEY", "ntfy", "cloudflare", "trycloudflare", "cfargotunnel", "/data/others", "auto/PhD",
             "Projects/PhD", "miniforge", "vps-x", "jump-x", "203.0.113.10", "203.0.113.20", ".cache/phd", "@gmail",
             "name-z", "BRAIN_TUMOR", "RQ1", "rq1", "phd", "PHD", "/mnt/disk1", "lesion-miss"]
# 包里所有文件统一换掉的标识符（环境变量名、目录名）；relay.py 在那边设变量、加任务时用同样的换法（rename）
RENAME = [("BRAIN_TUMOR", "SEG_DATA"), ("RQ1", "SEG"), ("phd_stage", "seg_stage"), ("PHD_", "SEG_")]
# 包里改名的脚本（常驻进程的名字和 DICC 的分开，10/30 DICC 管理员要求）：文件名和所有引用一起换
FILES = [("dicc_worker.sh", "loop.sh"), ("run_task.sh", "job_run.sh"), ("gpu_pad_start.sh", "mem_keep_start.sh"),
         ("gpu_pad.py", "mem_keep.py"), ("worker_start.sh", "start.sh"), ("train.sh", "fit.sh"), ("stage_tmp.sh", "copy_local.sh")]
# 只在包里改的几处（原样字符串 → 新字符串，每处必须正好出现一次）；{data} 是那边的数据目录
PE_SH = "research/RQ1/analysis/run_predict_eval.sh"
PATCHES = [  # 推理：留出集的输入由 relay 建好送过去，那边不从训练集现建；评测不在那边做
    (PE_SH, 'mkdir -p "$H"\n    python - "$RQ1/splits/gli_split_v2.json" "$nnUNet_raw/Dataset101_GLIrq1/imagesTr" "$H" << \'EOF\'',
     'echo "没有留出集的输入 $H" >&2; exit 1\n    python - << \'EOF\''),
    (PE_SH, '"$RQ1/analysis/rq1_model.R"', '"$RQ1/analysis/model.R"'),
]


class BundleError(Exception):
    pass


# ---------------- 取文件 ----------------
def git(*args, binary=False):
    r = subprocess.run(["git", "-C", REPO, *args], capture_output=True)
    if r.returncode:
        raise BundleError(f"git {' '.join(args)}：{r.stderr.decode(errors='replace').strip()}")
    return r.stdout if binary else r.stdout.decode()


def source_files(commit):
    """{相对路径: (内容 bytes, 可执行)}；commit 为 None 时读工作区（git 跟踪的文件）。"""
    if commit is None:
        names = git("ls-files", "--", *KEEP, *KEEP_DIRS).splitlines()
    else:
        names = [ln.split("\t", 1)[1] for ln in git("ls-tree", "-r", commit, "--", *KEEP, *KEEP_DIRS).splitlines()]
    names = [n for n in names if "__pycache__" not in n and not os.path.basename(n).startswith("test_")
             and n.endswith((".py", ".sh"))]
    missing = sorted(set(KEEP) - set(names))
    if missing:
        raise BundleError(f"{commit or '工作区'} 里没有：{', '.join(missing)}")
    out = {}
    for n in names:
        if commit is None:
            p = os.path.join(REPO, n)
            out[n] = (open(p, "rb").read(), os.access(p, os.X_OK))
        else:
            out[n] = (git("show", f"{commit}:{n}", binary=True), n.endswith(".sh"))
    return out


def read(commit, path):
    return open(os.path.join(REPO, path), "rb").read().decode() if commit is None else git("show", f"{commit}:{path}")


def relay_entry(commit, name):
    ns = {"__name__": "relay_machines"}
    exec(compile(read(commit, "envs/machines.py"), "machines.py", "exec"), ns)
    relay = ns.get("RELAY") or {}
    if name not in relay:
        raise BundleError(f"{commit or '工作区'} 的 envs/machines.py 的 RELAY 里没有 {name}")
    return relay[name]


# ---------------- 去注释 ----------------
def _docstrings(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.body:
            b = node.body[0]
            if isinstance(b, ast.Expr) and isinstance(b.value, ast.Constant) and isinstance(b.value.value, str):
                yield node, b


def _no_docstrings(tree):
    for node, b in list(_docstrings(tree)):
        node.body = node.body[1:] or [ast.Pass()]
    return tree


def strip_py(src):
    """去掉注释（第一行的 #! 留下）和文档字符串（函数体只有文档字符串时换成 pass），别的字符原样。"""
    lines = src.split("\n")
    drop = set()
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT and not (tok.start[0] == 1 and tok.string.startswith("#!")):
            r, c = tok.start[0] - 1, tok.start[1]
            lines[r] = lines[r][:c].rstrip()
            if not lines[r]:
                drop.add(r)
    for node, b in _docstrings(ast.parse(src)):
        r0, r1 = b.lineno - 1, b.end_lineno - 1
        c0 = len(lines[r0].encode()[:b.col_offset].decode())
        c1 = len(lines[r1].encode()[:b.end_col_offset].decode())
        new = lines[r0][:c0] + ("pass" if len(node.body) == 1 else "") + lines[r1][c1:]
        lines[r0] = new
        drop.update(range(r0 + 1, r1 + 1))
        if not new.strip():
            drop.add(r0)
    out = "\n".join(ln for i, ln in enumerate(lines) if i not in drop)
    if ast.dump(ast.parse(out)) != ast.dump(_no_docstrings(ast.parse(src))):
        raise BundleError("去注释后语法树变了")
    return out


_WORD_END = " \t;&|()<>"
_HEREDOC = re.compile(r"<<(-?)\s*(?:'([^']+)'|\"([^\"]+)\"|\\?([^\s;&|<>()'\"]+))")


def strip_sh(src):
    """去掉 shell 的整行注释（第一行 #! 和 #SBATCH 留下）和引号、heredoc 外的行尾注释；核对 declare -f 的结果不变。"""
    out, stack, pending, here, cont = [], [], [], None, False
    for i, line in enumerate(src.split("\n")):
        if here is not None:
            out.append(line)
            if (line.lstrip("\t") if here[1] else line) == here[0]:
                here = pending.pop(0) if pending else None
            continue
        s = line.lstrip()
        if not stack and not cont and s.startswith("#") and not (i == 0 and s.startswith("#!")) \
                and not s.startswith("#SBATCH"):
            continue
        keep = cont or (not stack and (i == 0 and s.startswith("#!") or s.startswith("#SBATCH")))
        cut, j, n = None, 0, 0 if keep and not stack and s.startswith("#") else len(line)
        while j < n:
            ch, ctx = line[j], (stack[-1][0] if stack else "top")
            two = line[j:j + 2]
            if ctx in ("top", "cmd", "brace"):
                if ch == "\\":
                    j += 2
                    continue
                if two == "$(":
                    stack.append(["cmd", 1])
                    j += 2
                    continue
                if two == "${":
                    stack.append(["brace", 0])
                    j += 2
                    continue
                if two == "$'":
                    stack.append(["asq", 0])
                    j += 2
                    continue
                if ch == "'" and not (ctx == "brace" and len(stack) > 1 and stack[-2][0] == "dq"):
                    stack.append(["sq", 0])
                elif ch == '"':
                    stack.append(["dq", 0])
                elif ch == "`":
                    stack.append(["bt", 0])
                elif ctx == "brace" and ch == "}":
                    stack.pop()
                elif ctx == "cmd" and ch == "(":
                    stack[-1][1] += 1
                elif ctx == "cmd" and ch == ")":
                    stack[-1][1] -= 1
                    if stack[-1][1] == 0:
                        stack.pop()
                elif ctx != "brace" and ch == "#" and (j == 0 or line[j - 1] in _WORD_END):
                    if not stack:
                        cut = j
                    break
                elif ctx != "brace" and line[j:j + 3] == "<<<":
                    j += 3
                    continue
                elif ctx != "brace" and two == "<<":
                    m = _HEREDOC.match(line, j)
                    if m:
                        pending.append((m.group(2) or m.group(3) or m.group(4), bool(m.group(1))))
                        j = m.end()
                        continue
            elif ctx == "dq":
                if ch == "\\":
                    j += 2
                    continue
                if two == "$(":
                    stack.append(["cmd", 1])
                    j += 2
                    continue
                if two == "${":
                    stack.append(["brace", 0])
                    j += 2
                    continue
                if ch == '"':
                    stack.pop()
                elif ch == "`":
                    stack.append(["bt", 0])
            elif ctx == "sq":
                if ch == "'":
                    stack.pop()
            elif ctx in ("asq", "bt"):
                if ch == "\\":
                    j += 2
                    continue
                if ch == ("'" if ctx == "asq" else "`"):
                    stack.pop()
            j += 1
        if cut is not None and not keep:
            line = line[:cut].rstrip()
        cont = not stack and line.endswith("\\") and not line.endswith("\\\\")
        out.append(line)
        if pending and here is None:
            here = pending.pop(0)
    res = "\n".join(out)
    if _declared(src) != _declared(res):
        raise BundleError("去注释后 bash 解析的结果变了")
    return res


def _declared(text):
    with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False) as f:
        f.write(text)
    try:
        r = subprocess.run(["bash", "-c", 'eval "__relay_f() {\n$(cat "$1")\n}" && declare -f __relay_f', "_", f.name],
                           capture_output=True, text=True)
    finally:
        os.remove(f.name)
    if r.returncode:
        raise BundleError(f"bash 读不了：{r.stderr.strip()}")
    return r.stdout


# ---------------- 生成的文件 ----------------
def machine_entry(name, e, root, entry=None):
    m = dict(label=name, hostname=e["hostname"], ssh=None, scheduler=e["scheduler"], repo=f"{root}/code/current", code=None,
             data=f"{root}/data", conda=tuple(e["conda"]), envs=dict(e["envs"]), groups=dict(e.get("groups") or {}),
             roles=("train",), disk=None, gpu_log=False, jobs=dict(e.get("jobs") or {}), sbatch=dict(e.get("sbatch") or {}))
    m.update({k: e[k] for k in ("slots", "worker") if k in e})
    m["env_root"] = M.env_root(e)  # 按描述建的环境（scripts/sites/envs.py）在根目录下
    m.update(entry or {})
    return m


def gen_machines(src, name, m):
    """把 envs/machines.py 的 MACHINES 换成只有这一项，删掉 SITES、RELAY。"""
    lines = src.split("\n")
    spans = {}
    for node in ast.parse(src).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            spans[node.targets[0].id] = (node.lineno - 1, node.end_lineno - 1)
    if "MACHINES" not in spans:
        raise BundleError("envs/machines.py 里找不到 MACHINES")
    repl = {spans["MACHINES"]: "MACHINES = " + pprint.pformat({name: m}, sort_dicts=False, width=120)}
    for k in ("SITES", "RELAY"):  # 执行机表（10/31 起 RELAY = SITES 里 relay=True 的）
        if k in spans:
            repl[spans[k]] = None
    for (r0, r1), text in sorted(repl.items(), reverse=True):
        lines[r0:r1 + 1] = [] if text is None else text.split("\n")
    return "\n".join(lines)


def gen_policy(name, stage_tmp=True, gpu_aug=False, gpu_aug_loader=False):
    return (f"AUTO = {{}}\nHOLD = []\nPLACE = {{}}\nGPU_AUG = {{{name!r}: {gpu_aug!r}}}\nGPU_AUG_LOADER = {{{name!r}: {gpu_aug_loader!r}}}\n"
            f"STAGE_TMP = {{{name!r}: {stage_tmp!r}}}\n"
            "MANUAL = {}\nSLURM = {}\n")


def gen_sbatch(real, m, root):
    sb = m.get("sbatch") or {}
    over = {"cpus-per-task": sb.get("cpus"), "mem": sb.get("mem")}
    keep = []
    for ln in real.splitlines():
        o = re.match(r"#SBATCH\s+--([\w-]+)", ln)
        if not o or o.group(1) in ("job-name", "output"):
            continue
        keep.append(f"#SBATCH --{o.group(1)}={over[o.group(1)]}" if over.get(o.group(1)) else ln)
    job = (m.get("jobs") or {}).get("gpu")
    if not job:
        raise BundleError("RELAY 里没有 jobs.gpu（worker 的作业名）")
    r = shlex.quote(root)
    return "\n".join(["#!/bin/bash", f"#SBATCH --job-name={job}", *keep,
                      f"#SBATCH --output={root}/data/logs/workers/%x_%j.out",
                      f"R={r}",
                      'REPO=$(readlink -f "$R/code/current")',
                      'export TASKQ_DIR=$R/queue RQ1_GPU_PAD_SWITCH=$R/queue/gpu_pad_DISABLED',
                      'exec bash "$REPO/scripts/queue/worker_start.sh"', ""])


def home_paths(text, root):
    """家目录下的 ~/.cache/phd/… 换成根目录下的：任务队列 → queue/，显存填充的开关 → queue/gpu_pad_DISABLED，其余 → state/。"""
    for pre in ("$HOME/", "~/"):
        text = text.replace(pre + ".cache/phd/taskq", f"{root}/queue")
        text = text.replace(pre + ".cache/phd/gpu_pad/DISABLED", f"{root}/queue/gpu_pad_DISABLED")
        text = text.replace(pre + ".cache/phd/", f"{root}/state/")
    return text


def rename(text):
    for a, b in RENAME:
        text = text.replace(a, b)
    for a, b in FILES:
        text = re.sub(r"(?<![\w.-])" + re.escape(a), b, text)
    return text


def rename_path(path):
    """文件名按 FILES 换，目录名按 RENAME 换（和文件内容里的引用一致，如 research/RQ1/ → research/SEG/）。"""
    d, f = os.path.split(path)
    d = "/".join(rename_dir(x) for x in d.split("/")) if d else d
    return os.path.join(d, dict(FILES).get(f, f))


def rename_dir(x):
    for a, b in RENAME:
        x = x.replace(a, b)
    return x


# ---------------- 核对 ----------------
def nnunet_python():
    """本机 nnunet 环境的 python（和那边的 conda 环境同一个 Python 版本），用来核对能编译。"""
    me = M.whoami()
    m = M.MACHINES.get(me) or {}
    e = (m.get("envs") or {}).get("nnunet")
    if not e or e[0] != "conda":
        return None
    p = os.path.join(M.expand(m["conda"][0]), "envs", e[1], "bin", "python")
    return p if os.path.exists(p) else None


def check(d, files, allow=()):
    pys = [os.path.join(d, f) for f in files if f.endswith(".py")]
    prog = "import sys\nfor f in sys.argv[1:]:\n    compile(open(f, 'rb').read(), f, 'exec')\n"
    for py in filter(None, [sys.executable, nnunet_python()]):
        r = subprocess.run([py, "-c", prog, *pys], capture_output=True, text=True)
        if r.returncode:
            raise BundleError(f"{py} 编译不过：{r.stderr.strip()[-500:]}")
    for f in files:
        if f.endswith((".sh", ".sbatch")):
            r = subprocess.run(["bash", "-n", os.path.join(d, f)], capture_output=True, text=True)
            if r.returncode:
                raise BundleError(f"bash -n {f}：{r.stderr.strip()}")
    hits = forbidden_hits(d, files, allow)
    if hits:
        raise BundleError("包里有不该出现的字样：" + "；".join(f"{f}:{n} {w}" for f, n, w in hits[:20]))


def forbidden_hits(d, files, allow=()):
    """allow：这台机器自己的根目录，可以出现。"""
    out = []
    for f in files:
        text = open(os.path.join(d, f), encoding="utf-8").read()
        for a in allow:
            text = text.replace(a, "")
        for n, ln in enumerate(text.lower().split("\n"), 1):
            out += [(f, n, w) for w in FORBIDDEN if w.lower() in ln]
    return out


# ---------------- 建包 ----------------
def contents(name, commit="origin/main", root=None, entry=None):
    """→ {相对路径: (文本, 可执行)}。root、entry（覆盖生成的 machines.py 那一项的字段）只给测试用。"""
    e = relay_entry(commit, name)
    root = (root or e["root"]).rstrip("/")
    m = machine_entry(name, e, root, entry)
    out = {}
    for path, (raw, exe) in source_files(commit).items():
        text = raw.decode()
        for f, old, new in PATCHES:
            if f == path:
                if text.count(old) != 1:
                    raise BundleError(f"{path} 里 {old!r} 出现了 {text.count(old)} 次（应为 1 次）")
                text = text.replace(old, new.format(data=m["data"]))
        text = strip_py(text) if path.endswith(".py") else strip_sh(text)
        out[path] = (home_paths(text, root), exe or path.endswith(".sh"))
    out["envs/machines.py"] = (home_paths(strip_py(gen_machines(read(commit, "envs/machines.py"), name, m)), root), True)
    out["envs/policy.py"] = (gen_policy(name, e.get("stage_tmp", True), e.get("gpu_aug", False), e.get("gpu_aug_loader", False)), False)
    if m["scheduler"] == "slurm":
        out["scripts/queue/worker.sbatch"] = (gen_sbatch(read(commit, "scripts/queue/worker.sbatch"), m, root), False)
    return {rename_path(p): (rename(x), e) for p, (x, e) in out.items()}


def bundle_id(files):
    h = hashlib.sha256()
    for path in sorted(files):
        data = files[path][0].encode()
        h.update(f"{path}\0{len(data)}\0".encode() + data)
    return h.hexdigest()[:12]


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
        check(tmp, sorted(files), ((root or relay_entry(commit, name)["root"]).rstrip("/"),))
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
