#!/usr/bin/env python3
"""环境的锁：把 <specs>/<名>.yml 解成 <名>.<变体>.lock.json，记下每个 conda 包、pip 包的确切版本、网址、sha256，
文件下载到状态文件夹的 wheelhouse/<sha256 前 2 位>/<sha256>。不往任何真的环境里装。说明见 queue/README.md 的"环境的锁"。

    taskq env lock <名> [--variant V]   锁一个描述的一个变体（默认 default）
    taskq env lock --all                每个描述 × 机器表里用到的变体（加 default）
"""
import argparse
import datetime
import glob
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(CORE, "queue"))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import proto as P  # noqa: E402
import common as C  # noqa: E402
import envs as EV  # noqa: E402
M = EV.M

GLIBC_FLOOR = "2.34"  # conda 解（CONDA_OVERRIDE_GLIBC）和 pip 平台（manylinux_2_X，X ≤ 这个）的上限；.taskq.toml 的 glibc_floor 可以改
CONDA_SUBDIR = "linux-64"
HELPER_CHANNEL = "conda-forge"  # 帮手 Python（跑 pip 的解析）从哪里装
PYPI = "https://pypi.org/simple"
SOURCE_EPOCH = "315532800"  # 本地建 wheel 时的 SOURCE_DATE_EPOCH（1980-01-01），建出来的文件不带当时的时间
MAX_BUILDS = 40
JOBS = 8


class LockError(Exception):
    pass


def glibc_floor():
    return str((cfg.project_conf() or {}).get("glibc_floor") or GLIBC_FLOOR)


def pip_platforms(floor):
    """glibc 上限 → pip 的 --platform 梯子（从高到低，manylinux_2_X 一个个列出来，再加老名字和 linux_x86_64）。"""
    major, minor = (int(x) for x in floor.split("."))
    if major != 2 or minor < 17:
        raise LockError(f"glibc 上限 {floor} 不认识（要 2.17 以上）")
    return ([f"manylinux_2_{i}_x86_64" for i in range(minor, 16, -1)]
            + ["manylinux2014_x86_64", "manylinux_2_12_x86_64", "manylinux2010_x86_64", "manylinux_2_5_x86_64",
               "manylinux1_x86_64", "linux_x86_64"])


def canon(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def mm(ver):
    return ".".join(ver.split(".")[:2]) if ver else None


def cp(ver):
    return "cp" + "".join(ver.split(".")[:2])


def wheel_tags(fn):
    """wheel 文件名 → (python, abi, platform)。"""
    parts = fn[:-4].split("-")
    if not fn.endswith(".whl") or len(parts) not in (5, 6):
        raise LockError(f"不是 wheel 的文件名：{fn}")
    return tuple(parts[-3:])


def lock_text(d):
    return json.dumps(d, ensure_ascii=False, indent=1, sort_keys=True) + "\n"


def lock_digest(path):
    b = M.lock_bytes(path)
    return hashlib.sha256(b).hexdigest() if b is not None else None


def placeholders(pkgs):
    return {x for p in pkgs for x in re.findall(r"\{(\w+)\}", str(p))}


def run_cmd(cmd, env=None, timeout=3600):
    e = dict(os.environ, **(env or {}))
    r = subprocess.run(cmd, capture_output=True, text=True, env=e, timeout=timeout, stdin=subprocess.DEVNULL)
    return r.returncode, r.stdout, r.stderr


def tail(txt, n=12):
    return "\n".join((txt or "").strip().splitlines()[-n:])


def http_get(url, dest):
    req = urllib.request.Request(url, headers={"User-Agent": "taskq-envlock"})
    with urllib.request.urlopen(req, timeout=120) as r, open(dest, "wb") as f:
        shutil.copyfileobj(r, f, 1 << 20)


class Locker:
    """run(cmd, env=None) → (退出码, 标准输出, 标准错误)；get(url, 目标文件) 下载。测试替换这两个。"""

    def __init__(self, conda=None, run=None, get=None, home=None, log=None):
        self.run = run or run_cmd
        self.get = get or http_get
        self.home = home or cfg.state()
        self.wh = os.path.join(self.home, P.WHEELHOUSE)
        self.conda = conda or find_conda()
        self.log = log or (lambda msg: print(msg, file=sys.stderr, flush=True))
        self._ver = {}

    # ---------- wheelhouse ----------
    def path(self, sha):
        return os.path.join(self.home, P.wheel(sha))

    def fetch(self, url, sha256=None, md5=None):
        """下载到 wheelhouse（已经有就不下），核对 sha256（没有时核对 md5）→ (sha256, 字节数)。"""
        if sha256 and os.path.isfile(self.path(sha256)):
            return sha256, os.path.getsize(self.path(sha256))
        tmpd = os.path.join(self.wh, "tmp")
        os.makedirs(tmpd, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=tmpd)
        os.close(fd)
        try:
            for i in range(3):
                try:
                    self.get(url, tmp)
                    break
                except OSError as e:
                    if i == 2:
                        raise LockError(f"下载不了 {url}：{e}") from None
            got = C.sha256(tmp)
            if sha256 and got != sha256:
                raise LockError(f"{url} 的 sha256 对不上：应为 {sha256}，下到的是 {got}")
            if md5 and not sha256:
                h = hashlib.md5()
                with open(tmp, "rb") as f:
                    for b in iter(lambda: f.read(1 << 20), b""):
                        h.update(b)
                if h.hexdigest() != md5:
                    raise LockError(f"{url} 的 md5 对不上")
            dest = self.path(got)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            os.replace(tmp, dest)
            return got, os.path.getsize(dest)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

    def store(self, src):
        """本地文件放进 wheelhouse → (sha256, 字节数)。"""
        h = C.sha256(src)
        dest = self.path(h)
        if not os.path.isfile(dest):
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            C.atomic_copy(src, dest)
        return h, os.path.getsize(dest)

    def fetch_all(self, items):
        """锁里的条目（有 url 的）都下到 wheelhouse，补上 sha256、size。"""
        todo = [e for e in items if e.get("url")]

        def one(e):
            h, n = self.fetch(e["url"], e.get("sha256"), e.get("md5"))
            e["sha256"], e["size"] = h, n
        with ThreadPoolExecutor(JOBS) as ex:
            list(ex.map(one, todo))
        for e in items:
            if not e.get("url") and not os.path.isfile(self.path(e["sha256"])):
                raise LockError(f"wheelhouse 里没有 {e['fn']}（{e['sha256']}）")

    # ---------- 工具 ----------
    def version(self, cmd):
        k = tuple(cmd)
        if k not in self._ver:
            rc, out, err = self.run(cmd)
            m = re.search(r"(\d+(?:\.\d+)+)", out + err) if rc == 0 else None
            self._ver[k] = m.group(1) if m else None
        return self._ver[k]

    def python(self, full):
        """跑 pip 解析用的 Python（版本同环境里的）：状态文件夹下 envlock/python-<版本>，没有就用 conda 建。"""
        d = os.path.join(self.home, "envlock", f"python-{full}")
        py = os.path.join(d, "bin", "python")
        if not os.path.isfile(py):
            self.log(f"建帮手 Python {full}（{d}）")
            rc, out, err = self.run([self.conda, "create", "-y", "-q", "-p", d, "-c", HELPER_CHANNEL, "--override-channels",
                                     f"python={full}", "pip"])
            if rc or not os.path.isfile(py):
                raise LockError(f"建不了帮手 Python {full}：{tail(out + err)}")
        return py

    # ---------- conda ----------
    def conda_solve(self, sp, floor):
        chans = list(sp.get("channels") or [])
        pk = ([f"python={sp['python']}"] if sp.get("python") else []) + [str(x) for x in sp.get("conda") or []]
        cmd = [self.conda, "create", "--dry-run", "--json", "-p", os.path.join(self.home, "envlock", "none")]
        for c in chans:
            cmd += ["-c", c]
        if chans:
            cmd += ["--override-channels"]
        env = dict(CONDA_OVERRIDE_GLIBC=floor, CONDA_SUBDIR=CONDA_SUBDIR,
                   CONDA_PKGS_DIRS=os.path.join(self.home, "envlock", "conda-pkgs"))  # 只放 repodata，dry-run 不下包：每个包都在 FETCH 里
        rc, out, err = self.run(cmd + pk, env)
        try:
            d = json.loads(out)
        except ValueError:
            raise LockError(f"conda 解不出：{tail(out + err)}") from None
        if rc or not d.get("success"):
            raise LockError(f"conda 解不出：{d.get('message') or d.get('error') or tail(err)}")
        acts = d.get("actions") or {}
        fetch = {(x["name"], x["version"], x["build"]): x for x in acts.get("FETCH") or []}
        out_ = []
        for x in acts.get("LINK") or []:
            f = fetch.get((x["name"], x["version"], x.get("build_string") or x.get("build")))
            if f is None:
                raise LockError(f"conda 的 FETCH 里没有 {x.get('dist_name') or x['name']}")
            out_.append(dict(name=x["name"], version=x["version"], build=f["build"], channel=x.get("channel"),
                             subdir=f.get("subdir"), url=f["url"], fn=f["fn"], size=f.get("size"), md5=f.get("md5"),
                             sha256=f.get("sha256"), noarch=f.get("noarch")))
        return out_

    # ---------- pip ----------
    def pip_base(self, py, rv):
        return [py, "-m", "pip", "--isolated", "--disable-pip-version-check", "--python", os.path.join(rv, "bin", "python")]

    def built_index(self):
        """本地建过的纯 Python wheel：{文件名: 记录}（wheelhouse/built/<源码包 sha256>.json）。"""
        out = {}
        for p in sorted(glob.glob(os.path.join(self.wh, "built", "*.json"))):
            b = json.load(open(p, encoding="utf-8"))
            out[b["fn"]] = b
        return out

    def links(self, fl):
        os.makedirs(fl, exist_ok=True)
        for fn, b in self.built_index().items():
            dst = os.path.join(fl, fn)
            if not os.path.exists(dst) and os.path.isfile(self.path(b["sha256"])):
                shutil.copyfile(self.path(b["sha256"]), dst)

    def build_pure(self, py, rv, req, index, fl, floor, tmp):
        """只有源码包的 req：建 wheel，是纯 Python（none-any）的放进 wheelhouse 和 find-links；要编译的报错。"""
        self.log(f"  {req} 没有能用的 wheel，从源码包建")
        rpt = os.path.join(tmp, "sdist.json")
        rc, out, err = self.run(self.pip_base(py, rv) + ["install", "--dry-run", "--quiet", "--ignore-installed", "--no-deps",
                                                         "--no-binary", ":all:", "--report", rpt, "--index-url", index, req])
        if rc or not os.path.isfile(rpt):
            raise LockError(f"{req}：没有 glibc ≤ {floor} 能用的 wheel，也找不到能用的源码包：{tail(err)}")
        it = json.load(open(rpt, encoding="utf-8"))["install"][0]
        di = it["download_info"]
        url = di["url"]
        sh, _ = self.fetch(url, ((di.get("archive_info") or {}).get("hashes") or {}).get("sha256"))
        rec = os.path.join(self.wh, "built", f"{sh}.json")
        if os.path.isfile(rec):
            b = json.load(open(rec, encoding="utf-8"))
        else:
            src = os.path.join(tmp, "sdist", urllib.parse.unquote(os.path.basename(urllib.parse.urlparse(url).path)))
            outd = os.path.join(tmp, "built")
            os.makedirs(os.path.dirname(src), exist_ok=True)
            shutil.rmtree(outd, ignore_errors=True)
            shutil.copyfile(self.path(sh), src)
            rc, out, err = self.run([py, "-m", "pip", "--isolated", "--disable-pip-version-check", "wheel", "--no-deps",
                                     "--wheel-dir", outd, src], dict(SOURCE_DATE_EPOCH=SOURCE_EPOCH))
            whl = sorted(glob.glob(os.path.join(outd, "*.whl")))
            if rc or len(whl) != 1:
                raise LockError(f"{req}：只有源码包，建 wheel 失败：{tail(out + err)}")
            fn = os.path.basename(whl[0])
            _, abi, plat = wheel_tags(fn)
            if (abi, plat) != ("none", "any"):
                raise LockError(f"{it['metadata']['name']} {it['metadata']['version']}：只有源码包，而且要编译（建出来的是 {fn}），"
                                f"锁不了；给它找 glibc ≤ {floor} 的预编译 wheel，在描述里直接写 wheel 的网址")
            h, n = self.store(whl[0])
            b = dict(fn=fn, sha256=h, size=n, built_from=url, built_from_sha256=sh)
            os.makedirs(os.path.dirname(rec), exist_ok=True)
            C.atomic_write(rec, lock_text(b))
        shutil.copyfile(self.path(b["sha256"]), os.path.join(fl, b["fn"]))
        return b

    def resolve(self, py, rv, reqs, index, no_deps, fl, floor, full, tmp):
        """一步 pip install --dry-run --report → [条目]（只有这一步要装的，已经装着的不算）。"""
        built = set()
        for _ in range(MAX_BUILDS):
            rpt = os.path.join(tmp, "report.json")
            if os.path.exists(rpt):
                os.remove(rpt)
            cmd = self.pip_base(py, rv) + ["install", "--dry-run", "--quiet", "--report", rpt, "--only-binary", ":all:",
                                           "--python-version", mm(full), "--implementation", "cp", "--abi", cp(full)]
            for p in pip_platforms(floor):
                cmd += ["--platform", p]
            cmd += ["--index-url", index, "--find-links", fl] + (["--no-deps"] if no_deps else []) + list(reqs)
            rc, out, err = self.run(cmd)
            if rc == 0 and os.path.isfile(rpt):
                return self.entries(json.load(open(rpt, encoding="utf-8")))
            miss = re.findall(r"No matching distribution found for (\S[^\n]*)", err)
            if not miss:
                raise LockError(f"pip 解不出：{tail(out + err)}")
            req = miss[-1].strip()
            if req in built:
                raise LockError(f"{req}：建了 wheel 以后 pip 还是解不出：{tail(err)}")
            built.add(req)
            self.build_pure(py, rv, req, index, fl, floor, tmp)
        raise LockError(f"要从源码包建的太多（> {MAX_BUILDS}）")

    def entries(self, report):
        built = {b["fn"]: b for b in self.built_index().values()}
        out = []
        for it in report.get("install") or []:
            md, di = it["metadata"], it["download_info"]
            if "archive_info" not in di:
                raise LockError(f"{md['name']}：只认 wheel 文件（{di.get('url')} 是目录或版本库）")
            url = di["url"]
            ai = di["archive_info"]
            h = (ai.get("hashes") or {}).get("sha256")
            if not h and str(ai.get("hash", "")).startswith("sha256="):
                h = ai["hash"].split("=", 1)[1]
            fn = urllib.parse.unquote(os.path.basename(urllib.parse.urlparse(url).path))
            e = dict(name=md["name"], version=md["version"], url=url, fn=fn, sha256=h, size=None, built_from=None)
            if url.startswith("file:"):
                b = built.get(fn)
                if b is None:
                    raise LockError(f"{fn} 是本地文件，但不是这里建的")
                e.update(url=None, sha256=b["sha256"], size=b["size"], built_from=b["built_from"],
                         built_from_sha256=b["built_from_sha256"])
            out.append(e)
        return out

    def fill(self, pkgs, have, full):
        """{torch_mm}、{abi}、{cp} → 按已经解出来的 torch 和 Python 版本填（同 envs.py 建环境时的算法）。"""
        need = placeholders(pkgs)
        if not need:
            return list(pkgs)
        val = {"cp": cp(full)}
        if need & {"torch_mm", "abi"}:
            t = next((e for e in reversed(have) if canon(e["name"]) == "torch"), None)
            if t is None:
                raise LockError("描述里有 {torch_mm} / {abi}，但环境里没有 torch")
            val["torch_mm"] = mm(t["version"].split("+")[0])
            if "abi" in need:
                val["abi"] = self.torch_abi(t)
        return [re.sub(r"\{(\w+)\}", lambda m: val[m.group(1)], str(p)) for p in pkgs]

    def torch_abi(self, e):
        if e.get("url"):
            e["sha256"], e["size"] = self.fetch(e["url"], e.get("sha256"))
        with zipfile.ZipFile(self.path(e["sha256"])) as z:
            for n in z.namelist():
                if n.endswith("TorchConfig.cmake"):
                    m = re.search(r"_GLIBCXX_USE_CXX11_ABI=([01])", z.read(n).decode("utf-8", "replace"))
                    if m:
                        return "TRUE" if m.group(1) == "1" else "FALSE"
        raise LockError(f"{e['fn']} 里找不到 _GLIBCXX_USE_CXX11_ABI")

    def pip_solve(self, steps, full, installed, have, floor):
        """steps：[(步骤名, 包, index, no_deps)]，按建环境的顺序；installed：{规范名: (名, 版本)} 已经装着的（conda 的
        Python 包、base 的锁）。每步在一个只有假的 dist-info 的 venv 上解，pip 只报这一步要装的。"""
        py = self.python(full)
        tmp = tempfile.mkdtemp(prefix="envlock.")
        try:
            rv = os.path.join(tmp, "rv")
            rc, out, err = self.run([py, "-m", "venv", "--without-pip", rv])
            if rc:
                raise LockError(f"建不了解析用的 venv：{tail(err)}")
            spd = glob.glob(os.path.join(rv, "lib", "python*", "site-packages"))
            if len(spd) != 1:
                raise LockError(f"解析用的 venv 里找不到 site-packages：{rv}")
            fl = os.path.join(tmp, "links")
            self.links(fl)
            for name, ver in installed.values():
                fake_dist(spd[0], name, ver)
            got = []
            for step, pkgs, index, no_deps in steps:
                reqs = self.fill(pkgs, have + got, full)
                self.log(f"  pip {step}：{len(reqs)} 个要求")
                for e in self.resolve(py, rv, reqs, index, no_deps, fl, floor, full, tmp):
                    c = canon(e["name"])
                    got = [x for x in got if canon(x["name"]) != c]
                    if c in installed:
                        unfake_dist(spd[0], installed[c][0])
                    fake_dist(spd[0], e["name"], e["version"])
                    installed[c] = (e["name"], e["version"])
                    e.update(step=step, no_deps=no_deps)
                    got.append(e)
            return got
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    # ---------- 一个锁 ----------
    def lock(self, env, variant="default"):
        sp = EV.load(env)
        spec = os.path.join(M.SPECS, f"{env}.yml")
        yml = open(spec, "rb").read()
        floor = glibc_floor()
        tv = sp.get("torch") or {}
        if not sp.get("base") and variant not in (tv or {"default": 1}):
            raise LockError(f"{env}.yml 没有变体 {variant}（有：{'、'.join(tv) or 'default'}）")
        out = dict(format=P.LOCK_FORMAT, name=env, variant=variant, spec_sha256=hashlib.sha256(yml).hexdigest(), base=None,
                   conda=[], pip=[], channels=list(sp.get("channels") or []))
        installed, have = {}, []
        self.log(f"锁 {env}（{variant}）")
        if sp.get("base"):
            b = sp["base"]
            bp = M.lock_path(b, variant)
            if not os.path.isfile(bp):
                raise LockError(f"{env} 建在 {b} 上，先锁 {b}：taskq env lock {b} --variant {variant}")
            bl = json.load(open(bp, encoding="utf-8"))
            if bl.get("spec_sha256") != hashlib.sha256(open(os.path.join(M.SPECS, f"{b}.yml"), "rb").read()).hexdigest():
                raise LockError(f"{os.path.basename(bp)} 是旧描述的，先重新锁 {b}")
            out["base"] = dict(name=b, variant=variant, lock_sha256=lock_digest(bp))
            full = (bl.get("platform") or {}).get("python_full")
            conda_py = [c for c in bl.get("conda") or [] if c.get("noarch") == "python"]
            have = list(bl.get("pip") or [])
            out["channels"] = bl.get("channels") or []
        else:
            out["conda"] = self.conda_solve(sp, floor)
            py = next((c for c in out["conda"] if c["name"] == "python"), None)
            full = py["version"] if py else None
            conda_py = [c for c in out["conda"] if c.get("noarch") == "python"]
        for c in conda_py:
            installed[canon(c["name"])] = (c["name"], c["version"])
        for e in have:
            installed[canon(e["name"])] = (e["name"], e["version"])
        steps = []
        if tv:
            t = tv[variant]
            steps.append(("torch", [str(x) for x in t["packages"]], t["index_url"], False))
        if sp.get("pip"):
            steps.append(("pip", [str(x) for x in sp["pip"]], PYPI, False))
        if sp.get("pip_no_deps"):
            steps.append(("pip_no_deps", [str(x) for x in sp["pip_no_deps"]], PYPI, True))
        if steps:
            if not full:
                raise LockError(f"{env}：环境里没有 python，装不了 pip 包")
            out["pip"] = self.pip_solve(steps, full, installed, have, floor)
        out["platform"] = dict(glibc_floor=floor, pip_platforms=pip_platforms(floor), python=mm(full), python_full=full,
                               implementation="cp" if full else None, abi=cp(full) if full else None, conda_subdir=CONDA_SUBDIR)
        self.log(f"  下载 {len(out['conda']) + len(out['pip'])} 个文件到 {self.wh}")
        self.fetch_all(out["conda"] + out["pip"])
        out["tools"] = dict(conda=self.version([self.conda, "--version"]),
                            pip=self.version([self.python(full), "-m", "pip", "--version"]) if steps else None)
        out["created"] = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        p = M.lock_path(env, variant)
        C.atomic_write(p, lock_text(out))
        return p, out


def fake_dist(spd, name, ver):
    d = os.path.join(spd, f"{re.sub(r'[-_.]+', '_', name)}-{ver}.dist-info")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "METADATA"), "w", encoding="utf-8") as f:
        f.write(f"Metadata-Version: 2.1\nName: {name}\nVersion: {ver}\n")
    for n, t in (("INSTALLER", "envlock\n"), ("RECORD", "")):
        with open(os.path.join(d, n), "w", encoding="utf-8") as f:
            f.write(t)


def unfake_dist(spd, name):
    for d in glob.glob(os.path.join(spd, "*.dist-info")):
        if canon(os.path.basename(d)[:-len(".dist-info")].rsplit("-", 1)[0]) == canon(name):
            shutil.rmtree(d)


def find_conda():
    v = os.environ.get("CONDA_EXE") or shutil.which("conda")
    if v:
        return v
    try:
        c = M.machine(M.hub()).get("conda")
    except (KeyError, StopIteration):
        c = None
    if c:
        return os.path.join(os.path.expanduser(c[0]), "bin", "conda")
    raise LockError("找不到 conda（设 CONDA_EXE，或给 --conda）")


def spec_names():
    return sorted(os.path.basename(p)[:-4] for p in glob.glob(os.path.join(M.SPECS, "*.yml")))


def targets():
    """--all：[(名, 变体)]，基础环境在前。非 venv：default 加机器条目里写的变体；venv：default 加条目里写的，pip 里有
    {torch_mm} / {abi} 时再加基础环境的每个变体。"""
    used = {}
    for n in M.MACHINES:
        for env, e in (M.machine(n)["envs"] or {}).items():
            if M.is_spec(e):
                used.setdefault(env, set()).add(EV.variant(e))
    names = spec_names()
    vs = {}

    def of(n, seen=()):
        if n in vs:
            return vs[n]
        if n in seen or n not in names:
            raise LockError(f"描述 {n} 不在，或者 base 绕成了圈")
        sp = EV.load(n)
        v = {"default"} | used.get(n, set())
        if sp.get("base"):
            if placeholders((sp.get("pip") or []) + (sp.get("pip_no_deps") or [])) & {"torch_mm", "abi"}:
                v |= of(sp["base"], seen + (n,))
        else:
            v &= set(sp.get("torch") or {"default": 1})
        vs[n] = v
        return v
    for n in names:
        of(n)
    depth = {}

    def d(n):
        if n not in depth:
            b = M.spec_base(n)
            depth[n] = 0 if not b else d(b) + 1
        return depth[n]
    return [(n, v) for n in sorted(names, key=lambda x: (d(x), x)) for v in sorted(vs[n], key=lambda x: (x != "default", x))]


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq env", description="环境的锁：描述 → <名>.<变体>.lock.json，文件下到 wheelhouse")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("lock", help="把描述解成锁（不装进任何环境）")
    p.add_argument("name", nargs="?", metavar="名")
    p.add_argument("--variant", default="default", help="变体（默认 default）；venv 的变体 = 基础环境的变体")
    p.add_argument("--all", action="store_true", help="每个描述 × 机器表里用到的变体（加 default）")
    p.add_argument("--conda", help="用哪个 conda（默认 CONDA_EXE、PATH 里的，或 hub 条目的 conda）")
    a = ap.parse_args(argv)
    if bool(a.name) == a.all:
        ap.error("给一个名字，或 --all")
    try:
        lk = Locker(conda=a.conda)
        todo = targets() if a.all else [(a.name, a.variant)]
    except (LockError, EV.EnvError) as e:
        print(f"taskq env lock：{e}", file=sys.stderr)
        return 2
    bad = 0
    for n, v in todo:
        try:
            p, d = lk.lock(n, v)
        except (LockError, EV.EnvError) as e:
            print(f"失败\t{n}.{v}\t{e}", file=sys.stderr)
            bad += 1
            continue
        mb = sum(e.get("size") or 0 for e in d["conda"] + d["pip"]) / 1e6
        print(f"{os.path.relpath(p, cfg.REPO)}\tconda {len(d['conda'])}\tpip {len(d['pip'])}\t{mb:.0f} MB")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
