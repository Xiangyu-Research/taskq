#!/usr/bin/env python3
"""执行机用的代码包：所有执行机同一个包，只按内容命名。
内容：调度核心里执行机要用的文件（RUNTIME：worker、run_task.sh、队列、任务说明；取自核心自己的仓库的提交，放在包里的 scripts/taskq/
（proto.BUNDLE_CORE），和本机的核心装在哪无关）、核心的提交号（scripts/taskq/VERSION，不算进包名）、使用者仓库根目录下的 .taskq.toml
（没有时生成一个）、配置文件夹（machines.py、policy.py 原样），加上使用者仓库的提交里跟踪的 policy.py 的 OUTPUTS["bundle"]
（使用者的文件夹或文件）下的代码（.py .sh .R .sbatch，1 MB 以内的 .json .csv .txt .yaml .yml），去掉 OUTPUTS["bundle_skip"] 的通配。
包里不生成、不改写任何文件：那边是哪台由命令带的 TASKQ_MACHINE 告诉它，根目录由 TASKQ_ROOT 告诉它（cfg.py）。
核对：本机和 OUTPUTS["bundle_check"] 那个环境的两个 Python 版本都能编译，bash -n 通过，没有钥匙（私钥、常见的 token）。
包名 = 内容哈希的前 16 位，不带提交号：只改 hub 上的代码或文档时包名不变，执行机不换包；同样的内容只建一次。
hub 上的包（OUT）只留各执行机在用的和最近建的 proto.CODE_KEEP 个（prune）。

    relay_bundle.py build [--commit 提交] [--core-commit 提交] [--out 目录]   → 打印"包名 路径"
    （--commit 是使用者仓库的，默认 origin/main；--core-commit 是核心仓库的，默认 HEAD；--commit '' = 两边都读工作区，只给测试用）
"""
import argparse
import fnmatch
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402

OUT = cfg.state("relay", "bundles", expand=False)
def conf_rel():
    """包里配置文件夹的位置（相对包的根）：.taskq.toml 的 config；没有 .taskq.toml 时，配置文件夹在仓库里就是它在仓库里的位置
    （原样带上），在仓库外（config_files 拷进包）是 config。"""
    conf = cfg.project_conf()
    if conf is not None:
        rel = os.path.normpath(str(conf.get("config") or "."))
    elif config_in_repo():
        rel = os.path.relpath(os.path.realpath(cfg.config_dir()), os.path.realpath(REPO))
    else:
        rel = "config"
    return "" if rel == "." else rel


def conf_path(name):
    return f"{conf_rel()}/{name}" if conf_rel() else name


def _tops():
    """使用者仓库里进包的文件夹或文件：policy.py 的 OUTPUTS["bundle"]，配置文件夹不在里面时加上它。"""
    tops = list(getattr(P, "OUTPUTS", {}).get("bundle", ()) or ())
    c = conf_rel() or "."
    if config_in_repo() and not any(t in ("", ".") or c == t.rstrip("/") or c.startswith(t.rstrip("/") + "/") for t in tops):
        tops.append(c)
    return tuple(tops)


def config_in_repo():
    r = os.path.relpath(os.path.realpath(cfg.config_dir()), os.path.realpath(REPO))
    return not (r == ".." or r.startswith("../"))


TOPS = _tops()
SKIP = tuple(getattr(P, "OUTPUTS", {}).get("bundle_skip", ()))  # 不进包的通配（fnmatch，* 跨目录）
BC = proto.BUNDLE_CORE  # 包里核心的位置（固定，和本机的核心在哪无关）
CORE_REF = "HEAD"  # 核心文件取自核心自己的仓库的这个提交（装好的那一份）
# 调度核心里执行机要用的（相对核心的根；包里放在 BC/ 下）：worker 的入口和循环、跑任务、队列、任务说明、实验表、显存填充、
# 输入的本地副本；机器级的（开机、装 pueue、建组、GPU 使用记录和汇总；pueue 执行机也只用代码包，不用仓库克隆）；命令行 bin/taskq（有就带上）和它在那边用的 taskq log；
# 其余（hub 上的计划、传数据、健康检查、同步）不进包
RUNTIME = (
    "queue/worker_start.sh", "queue/run_task.sh", "queue/gpu_pad_start.sh", "queue/gpu_pad.py",
    "queue/consume.py", "queue/consumers/__init__.py", "queue/consumers/slurm.py", "queue/consumers/resident.py",
    "queue/consumers/gpu_pad.py", "queue/consumers/gpu_share.py", "queue/consumers/gpu_idle.py", "queue/consumers/window.py",
    "queue/taskq.py", "queue/spec.py", "queue/common.py", "queue/cfg.py", "queue/taskq_api.py", "queue/recipes.py", "queue/experiments.py",
    "queue/site_ops.py", "queue/schema.py", "queue/inputs.py", "queue/worker.sbatch", "queue/cpu_worker.sbatch",
    "queue/on_boot.sh", "queue/setup.sh", "queue/install_pueue.sh", "queue/proto.py", "queue/project.py", "sites/mtab.py",
    "monitor/gpu_log.sh", "monitor/gpu_usage.py", "queue/tasklog.py", "bin/taskq",
    # store 上的操作（site_ops 的 export、push、pull、collect、smoke、archive）用到的
    "sites/names.py", "queue/relay_plugins.py", "sync/tunnel.py", "queue/archive_task.sh", "sites/slurm.py", "sites/remote.py")
CODE = (".py", ".sh", ".R", ".sbatch")
SMALL = (".json", ".csv", ".txt", ".yaml", ".yml")
SMALL_MAX = 1 << 20
PROJECT_FILE = ".taskq.toml"  # 使用者仓库根目录下的项目说明，有就带上
VERSION = BC + "/VERSION"  # 核心的提交号（短）；不算进包名：只改 hub 上的核心代码时包名不变、执行机不换包
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
def git(*args, binary=False, input=None, repo=None):
    r = subprocess.run(["git", "-C", repo or REPO, *args], capture_output=True, input=input)
    if r.returncode:
        raise BundleError(f"git {' '.join(args)}：{r.stderr.decode(errors='replace').strip()}")
    return r.stdout if binary else r.stdout.decode()


def _wanted(path, size):
    """path：包里的路径。"""
    if "__pycache__" in path:
        return False
    if path in RUNTIME_B:
        return True
    if not (path.endswith(CODE) or path.endswith(SMALL) and size <= SMALL_MAX):
        return False
    return not any(fnmatch.fnmatchcase(path, x) for x in SKIP)


RUNTIME_B = frozenset(f"{BC}/{f}" for f in RUNTIME)


def _inside(specs, sub):
    """路径里落在子目录 sub 下的 → 相对 sub 的路径；sub 本身或它的上级目录 → 整个 sub（"."）。"""
    out = []
    for p in (x.rstrip("/") for x in specs):
        if p in ("", ".") or p == sub or sub.startswith(p + "/"):
            out.append(".")
        elif p.startswith(sub + "/"):
            out.append(p[len(sub) + 1:])
    return out


def core_in_repo():
    """核心在使用者仓库里的位置（相对仓库根；"." = 核心就是这个仓库）；核心在仓库外 → None。"""
    r = os.path.relpath(os.path.realpath(CORE), os.path.realpath(REPO))
    return None if r == ".." or r.startswith("../") else r


def _entries(commit, specs, repo, prefix="", skip=()):
    """git 跟踪的文件 → [(路径（相对 repo，带 prefix）, mode, oid, size, 所在仓库, 工作区里的文件)]；子模块（gitlink）进去取它钉的提交，
    路径（带 prefix）在 skip 里的子模块不进（核心另取，见 CORE_REF）。commit 为 None 时读工作区（子模块读它的工作区，oid 为 None）。"""
    out = []
    if not specs:
        return out
    if commit is None:
        raw = git("ls-files", "-z", "-s", "--", *specs, repo=repo)
        links = [e.split("\t", 1)[1] for e in git("ls-files", "-z", "-s", repo=repo).split("\0") if e.startswith("160000 ")]
    else:
        raw = git("ls-tree", "-r", "-l", "-z", commit, "--", *specs, repo=repo)
        links = [(e.split("\t", 1)[1], e.split()[2]) for e in git("ls-tree", "-r", "-z", commit, repo=repo).split("\0")
                 if e.startswith("160000 ")]
    for ent in raw.split("\0"):
        if not ent:
            continue
        meta, path = ent.split("\t", 1)
        f = meta.split()
        if f[0] == "160000":
            continue
        if commit is None:
            p = os.path.join(repo, path)
            if f[0].startswith("100") and os.path.isfile(p):
                out.append((prefix + path, f[0], None, os.path.getsize(p), repo, p))
        elif f[1] == "blob":
            out.append((prefix + path, f[0], f[2], int(f[3]), repo, None))
    for link in links:
        path, sha = (link, None) if commit is None else link
        if prefix + path in skip:
            continue
        sub = _inside(specs, path)
        if sub:
            out += _entries(sha, sub, os.path.join(repo, path), prefix + path + "/")
    return out


def _load(ents):
    """_entries 的结果 → {路径: (内容 bytes, 可执行)}。"""
    out, objs = {}, {}
    for path, mode, oid, _, repo, wd in ents:
        if oid is None:
            out[path] = (open(wd, "rb").read(), os.access(wd, os.X_OK))
        else:
            objs.setdefault(repo, []).append((path, oid, mode == "100755"))
    for repo, lst in objs.items():
        data = git("cat-file", "--batch", binary=True, input="".join(f"{o}\n" for _, o, _ in lst).encode(), repo=repo)
        pos = 0
        for path, oid, exe in lst:
            nl = data.index(b"\n", pos)
            n = int(data[pos:nl].split()[2])
            out[path] = (data[nl + 1:nl + 1 + n], exe)
            pos = nl + 1 + n + 1
    return out


def _core_owned(path):
    return path == BC or path.startswith(BC + "/")


def project_entries(commit, specs=None):
    """使用者仓库里的文件（specs 默认 TOPS，相对仓库根、放在包里同样的位置）。核心在仓库里时（子模块）跳过它，
    包里 BC/ 下归核心；核心就是这个仓库时（单独的检出跑核心的测试）照样取。"""
    cr = core_in_repo()
    specs = list(TOPS) if specs is None else specs
    if cr == ".":
        return _entries(commit, specs, REPO)
    return [e for e in _entries(commit, specs, REPO, skip={x for x in (cr, BC) if x}) if not _core_owned(e[0])]


def core_entries(core):
    """核心的文件，取自核心自己的仓库（core 是提交；None = 工作区），放在包里的 BC/ 下：RUNTIME，加上 TOPS 里落在核心里的
    （核心在使用者的仓库里时，如测试用的配置）。"""
    cr = core_in_repo()
    extra = [] if cr is None else list(TOPS) if cr == "." else _inside(TOPS, cr)
    return _entries(core, list(RUNTIME) + extra, CORE, prefix=BC + "/")


def source_files(commit, core=CORE_REF):
    """{包里的路径: (内容 bytes, 可执行)}：使用者仓库 commit 的文件 + 核心仓库 core 的文件（commit 为 None 时两边都读工作区）。"""
    core = None if commit is None else core
    ents = [e for e in project_entries(commit) + core_entries(core) if e[1].startswith("100") and _wanted(e[0], e[3])]
    return dict(sorted(_load(ents).items()))


def read(commit, path):
    """使用者仓库 commit 里的一个文件（文本）。"""
    if commit is None:
        return open(os.path.join(REPO, path), "rb").read().decode()
    hit = _entries(commit, [path], REPO)
    if not hit:
        raise BundleError(f"{commit} 里没有 {path}")
    return git("cat-file", "blob", hit[0][2], repo=hit[0][4])


def core_version(core):
    """核心的提交号（短）：core 为 None（工作区）时是 HEAD 的。"""
    return git("rev-parse", "--short", core or "HEAD", repo=CORE).strip()


def project_file(commit):
    """使用者仓库根目录下的 .taskq.toml（有就返回文本）。"""
    try:
        return read(commit, PROJECT_FILE)
    except (BundleError, FileNotFoundError):
        return None


def config_files():
    """配置文件夹在仓库外（TASKQ_CONFIG）时：文件夹最上层的 .py 原样 → {包里的路径: (内容 bytes, 可执行)}；在仓库里时由 TOPS 带上 → {}。"""
    if config_in_repo():
        return {}
    d = cfg.config_dir()
    return {conf_path(f): (open(os.path.join(d, f), "rb").read(), False) for f in sorted(os.listdir(d)) if f.endswith(".py")}


def gen_project():
    """使用者没有 .taskq.toml（配置由 TASKQ_CONFIG 给）时包里生成一个：配置在 conf_rel()，插件取进了包的那些。"""
    plug = []
    for d in cfg.plugin_dirs():
        rel = os.path.relpath(os.path.realpath(d), os.path.realpath(REPO))
        if not rel.startswith("..") and any(t.rstrip("/") in ("", ".") or rel == t or rel.startswith(t.rstrip("/") + "/") for t in TOPS):
            plug.append(rel)
    return f"config = {conf_rel() or '.'!r}\nplugins = {plug!r}\n".replace("'", '"')


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
def contents(commit="origin/main", core=CORE_REF):
    """→ {包里的路径: (文本, 可执行)}。使用者的文件取自 commit，核心的取自核心仓库的 core（commit 为 None 时两边都读工作区）。"""
    core = None if commit is None else core
    out = {}
    for path, (raw, exe) in {**source_files(commit, core), **config_files()}.items():
        try:
            text = raw.decode()
        except UnicodeDecodeError as err:
            raise BundleError(f"{path} 不是 UTF-8：{err}") from None
        out[path] = (text, exe or path.endswith(".sh"))
    for f in ("machines.py", "policy.py"):
        if conf_path(f) not in out:
            raise BundleError(f"包里没有配置文件夹的 {f}（{conf_path(f)}）：配置文件夹要在 git 里跟踪")
    toml = project_file(commit)
    out[PROJECT_FILE] = (toml if toml is not None else gen_project(), False)
    hits = secret_hits(out)
    if hits:
        raise BundleError("包里有像钥匙的内容：" + "；".join(f"{f}:{n} {w}…" for f, n, w in hits[:10]))
    out[VERSION] = (core_version(core) + "\n", False)
    return out


def bundle_id(files):
    h = hashlib.sha256()
    for path in sorted(set(files) - {VERSION}):
        data = files[path][0].encode()
        h.update(f"{path}\0{len(data)}\0{int(bool(files[path][1]))}\0".encode() + data)
    return h.hexdigest()[:16]


def build(commit="origin/main", out=None, core=CORE_REF):
    """建包 → (包名, 路径)。commit=None：读工作区（只给测试用）。已经有同名的包时直接用（碰一下，prune 按它算新旧）。"""
    files = contents(commit, core)
    bid = bundle_id(files)
    out = os.path.expanduser(out or OUT)
    path = os.path.join(out, bid)
    if os.path.isdir(path):
        os.utime(path)
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
        check(tmp, sorted(files))
        os.chmod(tmp, 0o755)
        os.rename(tmp, path)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        if os.path.isdir(path):
            return bid, path
        raise
    return bid, path


def prune(keep, out=None, n=proto.CODE_KEEP):
    """hub 上的包只留 keep（各执行机在用的）和最近建、用过的 n 个；建到一半的临时目录 1 天后删 → [删掉的包名]。"""
    out = os.path.expanduser(out or OUT)
    try:
        names = os.listdir(out)
    except OSError:
        return []
    now, gone = time.time(), []
    dirs = sorted((d for d in names if not d.startswith(".") and os.path.isdir(os.path.join(out, d))),
                  key=lambda d: os.stat(os.path.join(out, d)).st_mtime, reverse=True)
    for d in dirs[n:]:
        if d not in keep:
            shutil.rmtree(os.path.join(out, d), ignore_errors=True)
            gone.append(d)
    for d in names:
        p = os.path.join(out, d)
        if d.startswith(".") and os.path.isdir(p) and now - os.stat(p).st_mtime > 86400:
            shutil.rmtree(p, ignore_errors=True)
    return gone


def main(argv):
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("build")
    p.add_argument("--commit", default="origin/main")
    p.add_argument("--core-commit", default=CORE_REF)
    p.add_argument("--out")
    a = ap.parse_args(argv)
    try:
        bid, path = build(a.commit or None, a.out, core=a.core_commit)
    except BundleError as e:
        print(f"建不了包：{e}", file=sys.stderr)
        return 1
    print(bid, path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
