#!/usr/bin/env python3
"""Code bundles for executors: one bundle for all executors, named by content hash.
It contains the scheduler files executors need (RUNTIME), the core commit, the project .taskq.toml and config dir, and the code
under the project's OUTPUTS["bundle"] (minus bundle_skip). Nothing is generated or rewritten: TASKQ_MACHINE and TASKQ_ROOT
tell it which machine and root to use (cfg.py). A bundle is checked by compiling, bash -n and a secret scan; building is
content-addressed, and the hub keeps bundles in use plus the newest proto.CODE_KEEP (prune).

    relay_bundle.py build [--commit COMMIT] [--core-commit COMMIT] [--out DIR]   → print "bundle path"
    (--commit is the project repo's, default origin/main; --core-commit is the core repo's, default HEAD; --commit '' reads both worktrees, for tests only)
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
CORE = os.path.dirname(HERE)  # root of the scheduler code
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
REPO = cfg.REPO  # project repo root
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402

OUT = cfg.state("relay", "bundles", expand=False)
def conf_rel():
    """Where the config dir sits in the bundle (relative to the bundle root): .taskq.toml's config; without .taskq.toml, the
    config dir's location in the repo when it is inside it (included as is), otherwise config (config_files copies it in)."""
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


def _tops(outputs):
    """Dirs or files in the project repo that go into the bundle: policy.py's OUTPUTS["bundle"], plus the config dir when it is
    not already inside it."""
    tops = list(outputs.get("bundle", ()) or ())
    c = conf_rel() or "."
    if config_in_repo() and not any(t in ("", ".") or c == t.rstrip("/") or c.startswith(t.rstrip("/") + "/") for t in tops):
        tops.append(c)
    return tuple(tops)


def config_in_repo():
    r = os.path.relpath(os.path.realpath(cfg.config_dir()), os.path.realpath(REPO))
    return not (r == ".." or r.startswith("../"))


TOPS = _tops(getattr(P, "OUTPUTS", {}) or {})
SKIP = tuple(getattr(P, "OUTPUTS", {}).get("bundle_skip", ()))  # globs kept out of the bundle (fnmatch, * crosses dirs)


def rules(commit):
    """(TOPS, SKIP) for a bundle of commit: read from policy.py as of that commit, so the dirs and skips always match the files
    taken from it (a process that loaded an older policy.py still builds the right bundle). The loaded ones when commit is None
    or the config dir is outside the repo (config_files then copies the loaded config)."""
    if commit is None or not config_in_repo():
        return TOPS, SKIP
    path = conf_path("policy.py")
    ns = {"__name__": "policy_at_commit", "__file__": os.path.join(cfg.config_dir(), "policy.py")}
    try:
        exec(compile(read(commit, path), f"{commit}:{path}", "exec"), ns)
    except Exception as err:
        raise BundleError(f"cannot read OUTPUTS from {path} at {commit}: {type(err).__name__}: {err}") from None
    out = ns.get("OUTPUTS") or {}
    return _tops(out), tuple(out.get("bundle_skip", ()))
BC = proto.BUNDLE_CORE  # where the core sits in the bundle (fixed, independent of where the local core lives)
CORE_REF = "HEAD"  # core files come from this commit of the core repo (the installed checkout)
# Scheduler files executors need (relative to the core root; placed under BC/ in the bundle): worker entry point and loop,
# task runner, queue, task specs, experiment table, GPU pad, local copies of inputs; machine-level pieces (boot, pueue setup,
# group setup, GPU usage log and summary; pueue executors also use the bundle, not a repo clone); bin/taskq (if present) and
# the taskq log it uses over there; the rest (hub planning, staging, health checks, sync) does not go in.
RUNTIME = (
    "queue/worker_start.sh", "queue/run_task.sh", "queue/gpu_pad_start.sh", "queue/gpu_pad.py",
    "queue/consume.py", "queue/consumers/__init__.py", "queue/consumers/slurm.py", "queue/consumers/resident.py",
    "queue/consumers/gpu_pad.py", "queue/consumers/gpu_share.py", "queue/consumers/gpu_idle.py", "queue/consumers/window.py",
    "queue/taskq.py", "queue/spec.py", "queue/common.py", "queue/cfg.py", "queue/taskq_api.py", "queue/recipes.py", "queue/experiments.py",
    "queue/site_ops.py", "queue/schema.py", "queue/inputs.py", "queue/worker.sbatch", "queue/cpu_worker.sbatch",
    "queue/on_boot.sh", "queue/setup.sh", "queue/install_pueue.sh", "queue/proto.py", "queue/project.py", "sites/mtab.py",
    "monitor/gpu_log.sh", "monitor/gpu_usage.py", "queue/tasklog.py", "bin/taskq",
    # used by store operations (site_ops export, push, pull, collect, smoke, archive)
    "sites/names.py", "queue/relay_plugins.py", "sync/tunnel.py", "queue/archive_task.sh", "sites/slurm.py", "sites/remote.py",
    # run over there by self migrate (migrate.py root, from the new bundle before it is switched)
    "queue/migrate.py")
CODE = (".py", ".sh", ".R", ".sbatch")
SMALL = (".json", ".csv", ".txt", ".yaml", ".yml")
SMALL_MAX = 1 << 20
PROJECT_FILE = ".taskq.toml"  # project description at the project repo root, included when present
VERSION = BC + "/VERSION"  # short core commit; not part of the bundle name: hub-only core changes keep the name and executors keep the bundle
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


# ---------------- taking files ----------------
def git(*args, binary=False, input=None, repo=None):
    r = subprocess.run(["git", "-C", repo or REPO, *args], capture_output=True, input=input)
    if r.returncode:
        raise BundleError(f"git {' '.join(args)}: {r.stderr.decode(errors='replace').strip()}")
    return r.stdout if binary else r.stdout.decode()


def _wanted(path, size, skip=None):
    """path: path in the bundle; skip: bundle_skip globs (default SKIP)."""
    if "__pycache__" in path:
        return False
    if path in RUNTIME_B:
        return True
    if not (path.endswith(CODE) or path.endswith(SMALL) and size <= SMALL_MAX):
        return False
    return not any(fnmatch.fnmatchcase(path, x) for x in (SKIP if skip is None else skip))


RUNTIME_B = frozenset(f"{BC}/{f}" for f in RUNTIME)


def _inside(specs, sub):
    """Paths under the sub dir sub → paths relative to sub; sub itself or one of its parents → the whole sub (".")."""
    out = []
    for p in (x.rstrip("/") for x in specs):
        if p in ("", ".") or p == sub or sub.startswith(p + "/"):
            out.append(".")
        elif p.startswith(sub + "/"):
            out.append(p[len(sub) + 1:])
    return out


def core_in_repo():
    """Where the core sits in the project repo (relative to the repo root; "." = the core is this repo); None when outside."""
    r = os.path.relpath(os.path.realpath(CORE), os.path.realpath(REPO))
    return None if r == ".." or r.startswith("../") else r


def _entries(commit, specs, repo, prefix="", skip=()):
    """git-tracked files → [(path (relative to repo, with prefix), mode, oid, size, repo it lives in, file in the worktree)];
    recurse into submodules (gitlink) at their pinned commit; submodules whose prefixed path is in skip are left out (the core
    is taken separately, see CORE_REF). commit None reads the worktree (submodules read theirs, oid is None)."""
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
    """_entries result → {path: (content bytes, executable)}."""
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


def project_entries(commit, specs=None, tops=None):
    """Files in the project repo (specs defaults to TOPS, relative to the repo root, placed at the same path in the bundle). Skip
    the core when it is a submodule; BC/ in the bundle belongs to the core. When the core is this repo (a standalone checkout
    running the core's tests), take it as usual."""
    cr = core_in_repo()
    specs = list(TOPS if tops is None else tops) if specs is None else specs
    if cr == ".":
        return _entries(commit, specs, REPO)
    return [e for e in _entries(commit, specs, REPO, skip={x for x in (cr, BC) if x}) if not _core_owned(e[0])]


def core_entries(core, tops=None):
    """Core files from the core repo (core is a commit; None = worktree), placed under BC/ in the bundle: RUNTIME plus whatever
    in TOPS falls inside the core (when the core is in the project repo, such as test config)."""
    cr = core_in_repo()
    tops = TOPS if tops is None else tops
    extra = [] if cr is None else list(tops) if cr == "." else _inside(tops, cr)
    return _entries(core, list(RUNTIME) + extra, CORE, prefix=BC + "/")


def source_files(commit, core=CORE_REF):
    """{path in the bundle: (content bytes, executable)}: files from the project repo at commit + files from the core repo at
    core (commit None reads both worktrees)."""
    core = None if commit is None else core
    tops, skip = rules(commit)
    ents = [e for e in project_entries(commit, tops=tops) + core_entries(core, tops) if e[1].startswith("100") and _wanted(e[0], e[3], skip)]
    return dict(sorted(_load(ents).items()))


def read(commit, path):
    """One file from the project repo at commit (text)."""
    if commit is None:
        return open(os.path.join(REPO, path), "rb").read().decode()
    hit = _entries(commit, [path], REPO)
    if not hit:
        raise BundleError(f"{commit} has no {path}")
    return git("cat-file", "blob", hit[0][2], repo=hit[0][4])


def core_version(core):
    """Short core commit: the one HEAD points at when core is None (worktree)."""
    return git("rev-parse", "--short", core or "HEAD", repo=CORE).strip()


def project_file(commit):
    """.taskq.toml at the project repo root (its text when present)."""
    try:
        return read(commit, PROJECT_FILE)
    except (BundleError, FileNotFoundError):
        return None


def config_files():
    """When the config dir is outside the repo (TASKQ_CONFIG): its top-level .py files as is → {path in the bundle: (content
    bytes, executable)}; when inside the repo, TOPS brings it in → {}."""
    if config_in_repo():
        return {}
    d = cfg.config_dir()
    return {conf_path(f): (open(os.path.join(d, f), "rb").read(), False) for f in sorted(os.listdir(d)) if f.endswith(".py")}


def gen_project(tops=None):
    """Generate .taskq.toml in the bundle when the project has none (config comes from TASKQ_CONFIG): config at conf_rel(),
    plus the plugins that made it into the bundle."""
    plug = []
    for d in cfg.plugin_dirs():
        rel = os.path.relpath(os.path.realpath(d), os.path.realpath(REPO))
        if not rel.startswith("..") and any(t.rstrip("/") in ("", ".") or rel == t or rel.startswith(t.rstrip("/") + "/") for t in (TOPS if tops is None else tops)):
            plug.append(rel)
    return f"config = {conf_rel() or '.'!r}\nplugins = {plug!r}\n".replace("'", '"')


# ---------------- checks ----------------
def check_python():
    """python from the local OUTPUTS["bundle_check"] env (the same Python version as the remote conda env), used to check that
    the bundle compiles; None when not configured."""
    me = M.whoami()
    m = M.MACHINES.get(me) or {}
    e = (m.get("envs") or {}).get(getattr(P, "OUTPUTS", {}).get("bundle_check"))
    if not e or e[0] != "conda":
        return None
    p = os.path.join(M.expand(m["conda"][0]), "envs", e[1], "bin", "python")
    return p if os.path.exists(p) else None


def secret_hits(files):
    """{path: (text, ...)} → look-alike secrets as [(path, line number, first 12 chars of the match)]."""
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
            raise BundleError(f"{py} cannot compile: {r.stderr.strip()[-500:]}")
    for f in files:
        if f.endswith((".sh", ".sbatch")):
            r = subprocess.run(["bash", "-n", os.path.join(d, f)], capture_output=True, text=True)
            if r.returncode:
                raise BundleError(f"bash -n {f}: {r.stderr.strip()}")


# ---------------- building ----------------
def contents(commit="origin/main", core=CORE_REF):
    """→ {path in the bundle: (text, executable)}. Project files come from commit, core files from core in the core repo
    (commit None reads both worktrees)."""
    core = None if commit is None else core
    out = {}
    for path, (raw, exe) in {**source_files(commit, core), **config_files()}.items():
        try:
            text = raw.decode()
        except UnicodeDecodeError as err:
            raise BundleError(f"{path} is not UTF-8: {err}") from None
        out[path] = (text, exe or path.endswith(".sh"))
    for f in ("machines.py", "policy.py"):
        if conf_path(f) not in out:
            raise BundleError(f"the bundle has no config dir {f} ({conf_path(f)}): the config dir must be tracked by git")
    toml = project_file(commit)
    out[PROJECT_FILE] = (toml if toml is not None else gen_project(rules(commit)[0]), False)
    hits = secret_hits(out)
    if hits:
        raise BundleError("the bundle contains something that looks like a secret: " + "; ".join(f"{f}:{n} {w}…" for f, n, w in hits[:10]))
    rel = os.path.relpath(f"{BC}/queue/worker_start.sh", os.path.dirname(proto.OLD_LAUNCHER))
    out[proto.OLD_LAUNCHER] = ("#!/bin/bash\n# generated by taskq: workers started from a bundle older than the core's place in the bundle switch here\n"
                               f'exec bash "$(dirname "$(readlink -f "${{BASH_SOURCE[0]}}")")/{rel}" "$@"\n', True)
    out[VERSION] = (core_version(core) + "\n", False)
    return out


def bundle_id(files):
    h = hashlib.sha256()
    for path in sorted(set(files) - {VERSION}):
        data = files[path][0].encode()
        h.update(f"{path}\0{len(data)}\0{int(bool(files[path][1]))}\0".encode() + data)
    return h.hexdigest()[:16]


def build(commit="origin/main", out=None, core=CORE_REF):
    """Build a bundle → (bundle name, path). commit=None: read the worktree (tests only). Reuse a bundle with the same name
    (touch it; prune uses the mtime to decide age)."""
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
    """On the hub keep only keep (bundles executors use) and the n newest built or used; remove half-built temp dirs after a
    day → [removed bundle names]."""
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
        print(f"cannot build the bundle: {e}", file=sys.stderr)
        return 1
    print(bid, path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
