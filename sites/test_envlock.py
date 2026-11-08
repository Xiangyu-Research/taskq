#!/usr/bin/env python3
"""测 sites/envlock.py：假的 conda、pip（换掉 Locker 的 run、get），不联网。--real：真锁一个只有 six 的小描述
（真的 conda、pip，下到临时目录）。

    python3 sites/test_envlock.py [--real]
"""
import glob
import hashlib
import io
import json
import os
import re
import shutil
import sys
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(CORE, "queue"))
sys.path.insert(0, os.path.join(CORE, "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
import common as C  # noqa: E402
import envlock as L  # noqa: E402
M = L.M

fails = 0


def ok(what, cond, info=None):
    global fails
    print(("PASS:" if cond else "FAIL:") + what + ("" if cond or info is None else f"\n    {info}"))
    fails += 0 if cond else 1


def sha(b):
    return hashlib.sha256(b).hexdigest()


def torch_zip(abi):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        zi = zipfile.ZipInfo("torch/share/cmake/Torch/TorchConfig.cmake", date_time=(1980, 1, 1, 0, 0, 0))
        z.writestr(zi, f'set(TORCH_CXX_FLAGS "-D_GLIBCXX_USE_CXX11_ABI={abi}")\n')
    return buf.getvalue()


def content(url):
    if "/torch-" in url:
        return torch_zip(1 if "cu128" in url else 0)
    return url.encode()


# 假的包索引：名 → (版本, 依赖, 种类)；种类 wheel / pure（只有源码包，纯 Python）/ compiled（只有源码包，要编译）
INDEX = {
    "six": ("1.16.0", [], "wheel"), "numpy": ("2.0.0", [], "wheel"), "newpkg": ("1.0", ["six"], "wheel"),
    "purepkg": ("1.0", ["six"], "pure"), "cext": ("2.0", [], "compiled"),
}
TORCH = {"cu121": "2.5.1+cu121", "cu128": "2.7.1+cu128"}
CALLS = []


def site_packages(rvpy):
    rv = os.path.dirname(os.path.dirname(rvpy))
    return glob.glob(os.path.join(rv, "lib", "python*", "site-packages"))[0]


def installed(spd):
    out = {}
    for d in glob.glob(os.path.join(spd, "*.dist-info")):
        n, v = os.path.basename(d)[:-len(".dist-info")].rsplit("-", 1)
        out[L.canon(n)] = v
    return out


VALUED = {"--report", "--only-binary", "--python-version", "--implementation", "--abi", "--platform", "--index-url",
          "--find-links", "--no-binary", "--python", "--wheel-dir"}


def parse(args):
    opts, pos, i = {}, [], 0
    while i < len(args):
        a = args[i]
        if a in VALUED:
            opts.setdefault(a, []).append(args[i + 1])
            i += 2
        elif a.startswith("-"):
            opts[a] = True
            i += 1
        else:
            pos.append(a)
            i += 1
    return opts, pos


def item(name, ver, url, hashed=True):
    ai = {"hashes": {"sha256": sha(content(url))}} if hashed else {}
    return dict(metadata=dict(name=name, version=ver), download_info=dict(url=url, archive_info=ai), requested=True)


def fake_pip(cmd, env):
    i = cmd.index("pip") + 1
    g, rest = parse(cmd[i:i + 6])
    sub = next(a for a in cmd[i:] if a in ("install", "wheel", "--version"))
    if sub == "--version":
        return 0, "pip 99.1 from x\n", ""
    o, reqs = parse(cmd[cmd.index(sub) + 1:])
    if sub == "wheel":
        src = reqs[0]
        n, v = re.match(r"(.+)-([^-]+)\.tar\.gz$", os.path.basename(src)).groups()
        CALLS.append(("wheel", env.get("SOURCE_DATE_EPOCH")))
        tag = "py3-none-any" if INDEX[n][2] == "pure" else "cp311-cp311-linux_x86_64"
        os.makedirs(o["--wheel-dir"][0], exist_ok=True)
        open(os.path.join(o["--wheel-dir"][0], f"{n}-{v}-{tag}.whl"), "wb").write(b"built " + n.encode())
        return 0, "", ""
    spd = site_packages(g["--python"][0])
    have = installed(spd)
    rpt = o["--report"][0]
    if "--no-binary" in o:
        n = re.split(r"[=<>!~]", reqs[0])[0]
        if n not in INDEX:
            return 1, "", f"ERROR: No matching distribution found for {reqs[0]}\n"
        v = INDEX[n][0]
        json.dump(dict(install=[item(n, v, f"https://files/{n}-{v}.tar.gz")]), open(rpt, "w"))
        return 0, "", ""
    CALLS.append(("install", cmd))
    fl = o.get("--find-links", [None])[0]
    local = {re.match(r"([^-]+)-", f).group(1): f for f in os.listdir(fl)} if fl and os.path.isdir(fl) else {}
    out, todo, seen = [], list(reqs), set()
    while todo:
        r = todo.pop(0)
        if r.startswith("https://"):
            fn = r.rsplit("/", 1)[1]
            n, v = fn.split("-")[:2]
            out.append(item(n, v, r, hashed=False))
            continue
        n = re.split(r"[=<>!~]", r)[0]
        if L.canon(n) in seen:
            continue
        seen.add(L.canon(n))
        pin = r.split("==")[1] if "==" in r else None
        if n == "torch":
            vv = TORCH[o["--index-url"][0].rsplit("/", 1)[1]]
            if have.get("torch") == vv:
                continue
            out.append(item("torch", vv, f"{o['--index-url'][0]}/torch-{vv}-cp311-cp311-linux_x86_64.whl"))
            todo += [] if "--no-deps" in o else ["numpy"]
            continue
        if n not in INDEX:
            return 1, "", f"ERROR: No matching distribution found for {r}\n"
        v, deps, kind = INDEX[n]
        if have.get(L.canon(n)) and (pin is None or have[L.canon(n)] == pin):
            continue
        if kind == "wheel":
            out.append(item(n, v, f"https://files/{n}-{v}-py3-none-any.whl"))
        elif n in local:
            out.append(item(n, v, "file://" + os.path.join(fl, local[n])))
        else:
            return 1, "", f"ERROR: Could not find a version that satisfies the requirement {r} (from versions: none)\n" \
                          f"ERROR: No matching distribution found for {r}\n"
        todo += [] if "--no-deps" in o else deps
    json.dump(dict(version="1", install=out), open(rpt, "w"))
    return 0, "", ""


def fake_run(cmd, env=None):
    env = env or {}
    if cmd[0].endswith("conda"):
        if cmd[1] == "--version":
            return 0, "conda 9.9.9\n", ""
        if "--dry-run" in cmd:
            CALLS.append(("conda", cmd, env))
            pk = [dict(name="python", version="3.11.9", build="h1_0", noarch=None, subdir="linux-64"),
                  dict(name="pip", version="24.0", build="pyh_0", noarch="python", subdir="noarch")]
            link = [dict(name=p["name"], version=p["version"], build_string=p["build"], channel="conda-forge") for p in pk]
            fetch = []
            for p in reversed(pk):
                url = f"https://conda/{p['subdir']}/{p['name']}-{p['version']}-{p['build']}.conda"
                fetch.append(dict(name=p["name"], version=p["version"], build=p["build"], subdir=p["subdir"], url=url,
                                  fn=url.rsplit("/", 1)[1], size=len(content(url)), md5="x", sha256=sha(content(url)),
                                  noarch=p["noarch"]))
            return 0, json.dumps(dict(success=True, actions=dict(LINK=link, FETCH=fetch))), ""
        d = cmd[cmd.index("-p") + 1]  # 帮手 Python
        os.makedirs(os.path.join(d, "bin"), exist_ok=True)
        open(os.path.join(d, "bin", "python"), "w").close()
        return 0, "", ""
    if cmd[1:3] == ["-m", "venv"]:
        os.makedirs(os.path.join(cmd[-1], "lib", "python3.11", "site-packages"))
        return 0, "", ""
    if cmd[1:3] == ["-m", "pip"]:
        return fake_pip(cmd, env)
    raise AssertionError(f"没想到的命令 {cmd}")


def fake_get(url, dest):
    open(dest, "wb").write(content(url))


d = fixture.tmpdir(prefix="envlock.")
SPECS = os.path.join(d, "specs")
os.makedirs(SPECS)
M.SPECS = SPECS


def spec(name, text):
    open(os.path.join(SPECS, f"{name}.yml"), "w").write(text)
    open(os.path.join(SPECS, f"{name}.check.py"), "w").write("print('ok')\n")


spec("base", "python: \"3.11\"\nchannels: [conda-forge]\nconda: []\ntorch:\n  default:\n    index_url: https://dl/cu121\n"
             "    packages: [torch==2.5.1]\n  cu128:\n    index_url: https://dl/cu128\n    packages: [torch==2.7.1]\n"
             "pip:\n  - six==1.16.0\n  - purepkg==1.0\n")
spec("layer", "base: base\npip:\n  - six==1.16.0\n  - newpkg==1.0\n")
spec("ext", "base: base\npip_no_deps:\n  - https://gh/rel/mamba-1.0+cu12torch{torch_mm}cxx11abi{abi}-{cp}-{cp}-linux_x86_64.whl\n")
spec("bad", "python: \"3.11\"\nchannels: [conda-forge]\npip:\n  - cext==2.0\n")
lk = L.Locker(conda="/x/bin/conda", run=fake_run, get=fake_get, home=os.path.join(d, "home"), log=lambda m: None)

# ---------- 平台 ----------
pl = L.pip_platforms("2.34")
ok("manylinux 梯子：2_34 起一个个往下到 2_17，再加 manylinux2014、老名字、linux_x86_64",
   pl[0] == "manylinux_2_34_x86_64" and "manylinux_2_28_x86_64" in pl and "manylinux_2_17_x86_64" in pl
   and "manylinux2014_x86_64" in pl and pl[-1] == "linux_x86_64" and len([p for p in pl if p.startswith("manylinux_2_")]) == 18 + 2)
ok("梯子里没有比上限新的", not any(re.fullmatch(r"manylinux_2_(\d+)_x86_64", p) and int(p.split("_")[2]) > 34 for p in pl))

# ---------- 锁 base ----------
h0 = M.spec_hash("base")
p, base = lk.lock("base")
cmds = [c[1] for c in CALLS if c[0] == "install"]
ok("conda：CONDA_OVERRIDE_GLIBC = 上限、dry-run、--override-channels",
   any(c[0] == "conda" and c[2].get("CONDA_OVERRIDE_GLIBC") == "2.34" and "--override-channels" in c[1] for c in CALLS))
ok("pip：每次都带 --only-binary :all:、--python-version 3.11、--abi cp311、整个平台梯子",
   cmds and all(" --only-binary :all: " in " ".join(c) + " " and "--python-version 3.11" in " ".join(c)
                and "--abi cp311" in " ".join(c) and all(f"--platform {x}" in " ".join(c) for x in pl) for c in cmds))
ok("torch 那一步只用变体的 index", any("https://dl/cu121" in c and "torch==2.5.1" in c for c in cmds))
ok("锁的文件名 <名>.<变体>.lock.json", os.path.basename(p) == "base.default.lock.json")
ok("锁的键（和 B 部分说好的）", set(L.P.LOCK_TOP) <= set(base) and base["format"] == 1 and base["base"] is None)
ok("conda 照 LINK 的顺序，字段齐", [c["name"] for c in base["conda"]] == ["python", "pip"]
   and all(set(L.P.LOCK_CONDA) <= set(c) for c in base["conda"]))
names = [e["name"] for e in base["pip"]]
ok("pip：torch、它的依赖、pip 的包都在；conda 装过的 pip 不重复", names == ["torch", "numpy", "six", "purepkg"], names)
ok("pip 条目字段齐、sha256 是文件的", all(set(L.P.LOCK_PIP) <= set(e) for e in base["pip"])
   and all(os.path.isfile(lk.path(e["sha256"])) and C.sha256(lk.path(e["sha256"])) == e["sha256"] for e in base["pip"]))
pp = next(e for e in base["pip"] if e["name"] == "purepkg")
ok("只有源码包的纯 Python：本地建 wheel，url 为 null，built_from 是源码包", pp["url"] is None
   and pp["built_from"] == "https://files/purepkg-1.0.tar.gz" and pp["fn"] == "purepkg-1.0-py3-none-any.whl"
   and open(lk.path(pp["sha256"]), "rb").read() == b"built purepkg")
ok("建 wheel 时设了 SOURCE_DATE_EPOCH", ("wheel", L.SOURCE_EPOCH) in CALLS)
ok("平台信息写进锁", base["platform"]["glibc_floor"] == "2.34" and base["platform"]["pip_platforms"] == pl
   and base["platform"]["python"] == "3.11" and base["platform"]["python_full"] == "3.11.9")
ok("spec_sha256 = 描述的 sha256", base["spec_sha256"] == sha(open(os.path.join(SPECS, "base.yml"), "rb").read()))

# ---------- 哈希 ----------
yml = open(os.path.join(SPECS, "base.yml"), "rb").read()
canon = json.dumps({k: v for k, v in base.items() if k != "created"}, sort_keys=True, separators=(",", ":"),
                   ensure_ascii=False).encode()
ok("有锁以后哈希 = sha256(描述 + 锁去掉 created)", M.spec_hash("base") == sha(yml + canon) != h0)
ok("没有锁的变体：哈希只算描述", M.spec_hash("base", "cu128") == h0 == sha(yml))
ok("没有的描述 → None", M.spec_hash("没有这个") is None)
first = open(p).read()
os.utime(p)
CALLS.clear()
lk.lock("base")
again = open(p).read()
strip = lambda t: re.sub(r'"created": "[^"]*"', "", t)  # noqa: E731
ok("同样的输入再锁一次：文件除 created 外一样，哈希不变", strip(first) == strip(again) and M.spec_hash("base") == sha(yml + canon))
ok("第二次不再建 wheel（用 wheelhouse/built 里记下的）", not any(c[0] == "wheel" for c in CALLS))
ok(".lock.json 不当描述", L.spec_names() == ["bad", "base", "ext", "layer"] and glob.glob(os.path.join(SPECS, "*.lock.json")))
lk.lock("base", "cu128")
ok("变体 cu128：torch 换成 cu128 的", next(e for e in json.load(open(M.lock_path("base", "cu128")))["pip"]
                                         if e["name"] == "torch")["version"] == "2.7.1+cu128")

# ---------- venv（base） ----------
p2, lay = lk.lock("layer")
ok("venv：只记 base 里没有的（six 同版本不记）", [e["name"] for e in lay["pip"]] == ["newpkg"], lay["pip"])
ok("venv：base 记名字、变体、base 锁的哈希", lay["base"] == dict(name="base", variant="default",
                                                                  lock_sha256=L.lock_digest(M.lock_path("base"))))
ok("venv：没有 conda，Python 版本跟 base", lay["conda"] == [] and lay["platform"]["python_full"] == "3.11.9")
try:
    lk.lock("layer", "nope")
    ok("base 没有这个变体的锁 → 报错", False)
except L.LockError as e:
    ok("base 没有这个变体的锁 → 报错", "先锁 base" in str(e))
_, ext = lk.lock("ext")
_, ext8 = lk.lock("ext", "cu128")
ok("占位按 base 的 torch 填（{torch_mm}、{abi} 从 wheel 里读、{cp}）",
   ext["pip"][0]["url"] == "https://gh/rel/mamba-1.0+cu12torch2.5cxx11abiFALSE-cp311-cp311-linux_x86_64.whl"
   and ext8["pip"][0]["url"] == "https://gh/rel/mamba-1.0+cu12torch2.7cxx11abiTRUE-cp311-cp311-linux_x86_64.whl")
ok("直接写网址的 wheel：报告里没有 sha256 时下载后算", ext["pip"][0]["sha256"] == sha(content(ext["pip"][0]["url"]))
   and ext["pip"][0]["no_deps"] is True)

# ---------- 要编译的源码包 ----------
try:
    lk.lock("bad")
    ok("只有源码包、要编译 → 报错，说出是哪个包", False)
except L.LockError as e:
    ok("只有源码包、要编译 → 报错，说出是哪个包", "cext 2.0" in str(e) and "要编译" in str(e), e)
ok("失败时不写锁", not os.path.exists(M.lock_path("bad")))

# ---------- 机器上用哪个变体、--all ----------
SAVE = {k: dict(v) for k, v in M.MACHINES.items()}
M.MACHINES["tl"] = dict(label="t", hostname=r"tl-host", ssh="fake-tl", scheduler="pueue", root="/r",
                        envs={"base": ("spec", "cu128"), "layer": "spec", "ext": "spec"}, workers=[dict(gpu="0")])
ok("venv 跟着基础环境的变体（有那个变体的锁时），没有时 default",
   M.spec_variant("tl", "ext") == "cu128" and M.spec_variant("tl", "layer") == "default" and M.spec_variant("tl", "base") == "cu128")
ok("就绪按这台的变体算哈希", M.ready_envs("tl", {"base": M.spec_hash("base", "cu128")}) >= {"base"}
   and "base" not in M.ready_envs("tl", {"base": M.spec_hash("base")}))
t = L.targets()
ok("--all：基础环境在前；base 用 default 和机器上写的变体；有占位的 venv 两个变体都锁；别的 default",
   t.index(("base", "default")) + 1 == t.index(("base", "cu128")) < min(t.index(("ext", "cu128")), t.index(("layer", "default")))
   and ("bad", "default") in t
   and ("layer", "cu128") not in t, t)
del M.MACHINES["tl"]

# ---------- taskq check：旧锁 ----------
import check as CK  # noqa: E402


class R:
    def __init__(self):
        self.problems, self.notes = [], []

    def bad(self, g, m):
        self.problems.append(m)

    def note(self, g, m):
        self.notes.append(m)

    def checked(self, g, n=1):
        pass


r = R()
CK.check_locks(r, M, L.spec_names())
ok("check：锁都对得上时没问题", r.problems == [], r.problems)
open(os.path.join(SPECS, "layer.yml"), "a").write("# 改了\n")
r = R()
CK.check_locks(r, M, L.spec_names())
ok("check：描述改了、锁是旧的 → 报", len(r.problems) == 1 and "layer.default.lock.json" in r.problems[0], r.problems)
M.MACHINES.clear()
M.MACHINES.update(SAVE)

# ---------- 真锁（--real）：python 3.11 + six ----------
if "--real" in sys.argv:
    rd = fixture.tmpdir(prefix="envlock-real.")
    M.SPECS = os.path.join(rd, "specs")
    os.makedirs(M.SPECS)
    open(os.path.join(M.SPECS, "tiny.yml"), "w").write("python: \"3.11\"\nchannels: [conda-forge]\npip:\n  - six==1.16.0\n")
    open(os.path.join(M.SPECS, "tiny.check.py"), "w").write("import six\n")
    rl = L.Locker(home=os.path.join(rd, "home"))
    p, lock = rl.lock("tiny")
    six = [e for e in lock["pip"] if e["name"] == "six"]
    ok("真锁：conda 有 python，pip 有 six 1.16.0，文件都在 wheelhouse、sha256 对",
       any(c["name"] == "python" and c["version"].startswith("3.11") for c in lock["conda"])
       and six and six[0]["version"] == "1.16.0" and six[0]["url"].startswith("https://")
       and all(C.sha256(rl.path(e["sha256"])) == e["sha256"] for e in lock["conda"] + lock["pip"]), lock["pip"])
    shutil.rmtree(rd, ignore_errors=True)

shutil.rmtree(d, ignore_errors=True)
print("全部通过" if not fails else f"{fails} 项失败")
sys.exit(1 if fails else 0)
