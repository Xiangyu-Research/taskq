#!/usr/bin/env python3
"""relay_bundle.py 的测试：取文件、钥匙、包里的文件原样（不生成、不改写）、包名、包里的 taskq.py 和 site_ops.py 完整走一遍、
包里的 worker 跑 test_worker*.sh 的几个情况。
包从工作区建（commit=None），根目录是临时目录；配置用测试用的（testdata/，OUTPUTS["bundle"] 是调度代码所在的文件夹）。
包里的命令不带本机的 TASKQ_CONFIG、TASKQ_PLUGINS：配置和插件按包里的 .taskq.toml 找，是哪台由 TASKQ_MACHINE、根目录由 TASKQ_ROOT 告诉它。
    python3 queue/test_relay_bundle.py
"""
import json
import os
import re
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, HERE)
import cfg  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
QR = cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）
BQ = cfg.bundle_rel("queue")  # 包里调度代码的 queue/（总是 scripts/taskq/queue）

cfg.use_config()
import proto  # noqa: E402
import relay_bundle as B  # noqa: E402
import spec as S  # noqa: E402

NAME = "site-slurm2"
STEP = f"{BQ}/testdata/bin/step.sh"  # 包里的
DROP = ("TASKQ_DATA", "TASKQ_DIR", "TASKQ_MACHINE", "TASKQ_CONFIG", "TASKQ_PLUGINS", "TASKQ_ROOT", "TASKQ_STATE", "TASKQ_DB")


def ok(msg):
    print(f"通过：{msg}")


def bare_env(**kw):
    """包里的命令的环境：去掉本机的配置、队列、数据目录（包按自己的 .taskq.toml 找配置和插件）。"""
    e = {k: v for k, v in os.environ.items() if k not in DROP}
    e.update(kw)
    return e


def run(cmd, env=None, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, env=bare_env(**(env or {})), **kw)


def test_source_files_from_commit():
    files = B.source_files("HEAD")
    for f in (f"{BQ}/taskq.py", f"{BQ}/worker_start.sh", f"{BQ}/consume.py", STEP, f"{BQ}/testdata/exps/demo/experiments.py",
              f"{BQ}/testdata/config/machines.py", f"{QR}/testdata/exps/demo/experiments.py"):
        assert f in files, f
    assert B._wanted("x/a.R", 10) and not B._wanted("x/big.json", B.SMALL_MAX + 1) and not B._wanted("x/__pycache__/a.py", 1)
    assert not any(f.startswith(("papers/", "admin/", "talks/")) for f in files)
    assert not any("__pycache__" in f for f in files) and all(len(files[f][0]) <= B.SMALL_MAX for f in files if f.endswith(B.SMALL))
    (_, mode, oid, _, repo, _), = B._entries("HEAD", ["queue/testdata/bin/step.sh"], CORE)  # 核心的文件取自核心的仓库
    head = subprocess.run(["git", "-C", repo, "cat-file", "blob", oid], capture_output=True).stdout
    assert head and files[STEP] == (head, mode == "100755")
    ok("从提交取文件：OUTPUTS[\"bundle\"] 下的代码（含 .R）和小数据文件，内容和可执行位同提交（.sh 在包里另设可执行），没有 papers/ 等")


def test_source_files_submodule(T):
    """使用者的文件取自使用者仓库的提交（别的子模块进去取它钉的提交，核心那个子模块跳过）；核心的文件取自核心自己的仓库的 HEAD
    （不看使用者仓库钉的提交），放在包里的 BUNDLE_CORE/ 下。核心嵌在仓库里、在仓库外，取到的核心文件相同。"""
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
    BC = B.BC

    def g(*a, cwd):
        return subprocess.run(["git", "-c", "protocol.file.allow=always", *a], cwd=cwd, env=env, check=True,
                              capture_output=True, text=True).stdout.strip()

    def put(d, files):
        for f, body, exe in files:
            os.makedirs(os.path.dirname(os.path.join(d, f)), exist_ok=True)
            with open(os.path.join(d, f), "w") as fh:
                fh.write(body)
            os.chmod(os.path.join(d, f), 0o755 if exe else 0o644)

    core, lib, sup, flat = (os.path.join(T, "sub", x) for x in ("core", "lib", "sup", "flat"))
    for d, files in ((core, (("q/a.sh", "echo a\n", True), ("q/b.py", "B = 1\n", False), ("big.json", "x" * 10, False),
                             ("q/hub_only.py", "", False))),
                     (lib, (("l.py", "L = 1\n", False),))):
        os.makedirs(d)
        g("init", "-q", "-b", "main", cwd=d)
        put(d, files)
        g("add", "-A", cwd=d)
        g("commit", "-qm", "c", cwd=d)
    proj = (("cfg/m.py", "M = 2\n", False), ("x/y.py", "Y = 3\n", False), ("other/c.py", "", False),
            ("scripts/queue/worker_start.sh", "echo old\n", True))
    for d in (sup, flat):
        os.makedirs(d)
        g("init", "-q", "-b", "main", cwd=d)
        put(d, proj)
        g("submodule", "add", "-q", lib, "x/lib", cwd=d)
    g("submodule", "add", "-q", core, BC, cwd=sup)
    for d in (sup, flat):
        g("add", "-A", cwd=d)
        g("commit", "-qm", "p", cwd=d)
    emb = os.path.join(sup, BC)
    put(emb, (("q/b.py", "B = 2\n", False),))  # 核心的检出里新提交、使用者仓库没跟着钉：取核心的 HEAD
    g("commit", "-qam", "c2", cwd=emb)
    put(emb, (("q/a.sh", "echo wd\n", True),))  # 核心工作区没提交的：只有 commit=None 时读
    put(core, (("q/b.py", "B = 2\n", False),))
    g("commit", "-qam", "c2", cwd=core)
    put(core, (("q/a.sh", "echo wd\n", True),))
    saved = B.REPO, B.CORE, B.TOPS, B.RUNTIME, B.RUNTIME_B
    try:
        B.TOPS, B.RUNTIME = ("cfg", "x", "scripts"), ("q/a.sh", "big.json")
        B.RUNTIME_B = frozenset(f"{BC}/{f}" for f in B.RUNTIME)
        got = {}
        for way, repo, c in (("嵌着", sup, emb), ("在仓库外", flat, core)):
            B.REPO, B.CORE = repo, c
            got[way] = (B.source_files("HEAD"), B.source_files(None), B.read("HEAD", "x/y.py"), B.core_version("HEAD"))
    finally:
        B.REPO, B.CORE, B.TOPS, B.RUNTIME, B.RUNTIME_B = saved
    files, wd, y, ver = got["嵌着"]
    assert sorted(files) == ["cfg/m.py", "scripts/queue/worker_start.sh", f"{BC}/big.json", f"{BC}/q/a.sh", f"{BC}/q/b.py",
                             f"{BC}/q/hub_only.py", "x/lib/l.py", "x/y.py"], sorted(files)  # OUTPUTS["bundle"] 盖住了核心：整个核心
    assert files[f"{BC}/q/b.py"] == (b"B = 2\n", False) and files[f"{BC}/q/a.sh"] == (b"echo a\n", True), files
    assert wd[f"{BC}/q/a.sh"] == (b"echo wd\n", True) and sorted(wd) == sorted(files), wd
    assert y == "Y = 3\n" and re.fullmatch(r"[0-9a-f]{7,}", ver)
    files2, wd2, _, _ = got["在仓库外"]
    assert sorted(files2) == ["cfg/m.py", "scripts/queue/worker_start.sh", f"{BC}/big.json", f"{BC}/q/a.sh", "x/lib/l.py", "x/y.py"], \
        sorted(files2)
    assert {k: v for k, v in files.items() if k not in (f"{BC}/q/b.py", f"{BC}/q/hub_only.py")} == files2 and wd2[f"{BC}/q/a.sh"] == (b"echo wd\n", True)
    ok("取文件：使用者的从提交（别的子模块取它钉的提交，核心的子模块跳过），核心的从核心仓库的 HEAD（不看钉的提交）放在 "
       f"{BC}/ 下；commit=None 时两边都读工作区；核心在仓库外时只取 RUNTIME")


def test_secrets():
    sk = "-----BEGIN OPENSSH " + "PRIVATE KEY-----"
    tok = "ghp_" + "a1" * 18
    assert B.secret_hits({"a.sh": ("x\n" + sk + "\n", False), "b.py": (f"T = '{tok}'", False), "c.py": ("ok = 'ghp_short'", False)}) \
        == [("a.sh", 2, sk[:12]), ("b.py", 1, tok[:12])]
    saved = B.source_files
    B.source_files = lambda *a: dict(saved(*a), **{"scripts/x.sh": (f"echo {tok}".encode(), False)})
    try:
        B.contents(None)
        raise AssertionError("有钥匙应报错")
    except B.BundleError as e:
        assert "scripts/x.sh:1" in str(e) and tok not in str(e), e
    finally:
        B.source_files = saved
    ok("钥匙：私钥、GitHub token 报错（报错里不带整个钥匙），短的不算")


def test_bundle(T):
    R = os.path.join(T, "R")
    bid, path = B.build(None, out=os.path.join(R, "code"))
    assert path == os.path.join(R, "code", bid) and re.fullmatch(r"[0-9a-f]{16}", bid), bid
    bid2, path2 = B.build(None, out=os.path.join(R, "code"))
    assert (bid2, path2) == (bid, path)
    assert not [n for n in os.listdir(os.path.join(R, "code")) if n.startswith(".")]
    ok("建包：包名是 16 位内容哈希，同样的内容再建一次用原来的包、不留临时目录")

    files, links = set(), []
    for d, ds, fs in os.walk(path):
        links += [os.path.join(d, x) for x in ds + fs if os.path.islink(os.path.join(d, x))]
        files |= {os.path.relpath(os.path.join(d, f), path) for f in fs}
    want = B.contents(None)
    assert files == set(want) and not links, (sorted(set(want) ^ files)[:20], links)
    src = B.source_files(None)
    for f in B.RUNTIME:
        if os.path.isfile(os.path.join(CORE, f)):
            assert f"{B.BC}/{f}" in files, f"{f} 不在包里（没在 git 里跟踪？）"
    for f in (f"{BQ}/site_ops.py", f"{BQ}/schema.py", f"{BQ}/worker.sbatch", f"{BQ}/cpu_worker.sbatch", f"{BQ}/worker_start.sh",
              f"{BQ}/testdata/bin/toy_train.sh", STEP, f"{BQ}/testdata/exps/demo/experiments.py", B.VERSION, B.PROJECT_FILE):
        assert f in files, f
    for f, (raw, exe) in src.items():
        assert open(os.path.join(path, f), "rb").read() == raw, f"{f} 和工作区的不一样"
        assert os.access(os.path.join(path, f), os.X_OK) == (exe or f.endswith(".sh")), f
    for f in ("machines.py", "policy.py"):
        assert open(os.path.join(path, B.conf_path(f)), "rb").read() == open(os.path.join(cfg.config_dir(), f), "rb").read(), f
    sha = subprocess.run(["git", "-C", CORE, "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    assert open(os.path.join(path, B.VERSION)).read() == sha + "\n"
    plug = os.path.relpath(os.path.realpath(fixture.PLUGINS), os.path.realpath(REPO))
    toml = open(os.path.join(path, B.PROJECT_FILE)).read()
    assert toml == f'config = "{B.conf_rel()}"\nplugins = ["{plug}"]\n', toml
    ok("包里的文件：工作区的代码、配置文件夹（machines.py、policy.py）原样，一个都不生成、不改写，"
       "没有软链接、没有旧入口；RUNTIME 全在（含 site_ops.py）；VERSION 是核心的提交号；.taskq.toml 写配置文件夹和插件在包里的位置")
    return bid, path


def test_in_bundle(T, bid, path):
    """包里的 taskq.py、mtab.py、site_ops.py：只靠 TASKQ_MACHINE、TASKQ_ROOT 和包里的 .taskq.toml。"""
    R = os.path.dirname(os.path.dirname(path))
    os.symlink(bid, os.path.join(R, "code", "current"))
    cur = os.path.join(R, "code", "current")
    os.makedirs(os.path.join(R, proto.ROOT["data"]))
    os.makedirs(os.path.join(R, "queue"))
    open(os.path.join(R, "queue", "tasks.jsonl"), "w").close()
    env = dict(TASKQ_ROOT=R, TASKQ_MACHINE=NAME)
    mp = os.path.join(cur, cfg.bundle_rel("sites"), "mtab.py")
    assert run(["python3", mp, "whoami"], env).stdout.strip() == NAME
    assert NAME in run(["python3", mp, "names", "slurm"], env).stdout.split()
    got = run(["python3", mp, "get", ".", "no_such_field"], env)
    assert got.returncode == 1 and "Traceback" not in got.stderr and "no_such_field" in got.stderr, got
    ok("包里的 mtab.py：读包里原样的机器表，按 TASKQ_MACHINE 认出本机；没有的字段说清、退出码 1")

    tq = ["python3", os.path.join(cur, BQ, "taskq.py")]

    def status():
        r = run(tq + ["status", "--json"], env)
        assert r.returncode == 0, r
        return json.loads(next(ln for ln in r.stdout.splitlines() if ln.startswith(proto.JSON_MARK))[len(proto.JSON_MARK):])
    key = "D999_abcdef/V_save/0"
    r = run(tq + ["add", "--nice", "0", "--recipe", "toy", "ds=D999_abcdef", "frag=0"], env)
    assert r.returncode == 0 and key in r.stdout, r
    st = status()
    assert st["version"] == proto.VERSION and [t["key"] for t in st["tasks"]] == [key] and st["tasks"][0]["state"] == "free", st
    assert st["tasks"][0]["spec"]["relay"] == "toy" and st["tasks"][0]["exp"] is None
    r = run(tq + ["next", "--worker", "123", "--envs", "base", "--device", "gpu"], env)
    assert r.returncode == 0, r
    lines = r.stdout.strip().split("\n")
    assert lines[0] == key and lines[1].startswith("toy ") and "ds=D999_abcdef" in lines[1], lines
    assert os.listdir(os.path.join(R, "queue", "claims"))
    assert run(tq + ["next", "--worker", "124", "--envs", "base", "--device", "gpu"], env).returncode == 10
    assert run(tq + ["beat", "--worker", "123", key], env).returncode == 0
    assert run(tq + ["stop", key], env).returncode == 0
    assert os.path.exists(os.path.join(R, "queue", "stopped", key.replace("/", "__")))
    assert status()["tasks"][0]["state"] == "stopped"
    assert run(tq + ["release", "--worker", "123", key], env).returncode == 0
    assert run(tq + ["resume", key], env).returncode == 0
    assert run(tq + ["remove", key], env).returncode == 0
    assert not open(os.path.join(R, "queue", "tasks.jsonl")).read().strip()
    assert status()["enabled"] is False
    ok("包里的 taskq.py：add（做法 toy 来自 .taskq.toml 写的插件）→ status --json → next（0；再领 10）→ beat → stop → release → "
       "resume → remove；队列在 TASKQ_ROOT 下的 queue/")

    ops = os.path.join(cur, BQ, "site_ops.py")

    def op(what, data, **kw):
        r = run(["python3", ops, what], dict(env, **kw), input=json.dumps(data))
        out = [ln for ln in r.stdout.splitlines() if ln.startswith(proto.JSON_MARK)]
        return r.returncode, json.loads(out[-1][len(proto.JSON_MARK):]) if out else None, r.stderr
    rc, st, err = op("status", {})
    assert rc == 0 and st["bundle"] == bid and st["taskq"]["tasks"] == [], (rc, err[-2000:])
    assert op("states", {})[:2] == (0, {})
    D = os.path.join(T, "store_data")
    os.makedirs(D)
    sp = S.resolve("toy", dict(ds="D999_abcdef", frag="0"))
    o = S.outputs(sp)
    sto = dict(TASKQ_MACHINE="hub")
    rc, got, err = op("done", dict(data=D, items={key: dict(out=sp["out"], done=o["done"])}), **sto)
    assert rc == 0 and got == {key: ["|".join(x) for x in o["done"]]}, (rc, got, err[-2000:])
    for alts in o["done"]:
        p = os.path.join(D, sp["out"], alts[0])
        os.makedirs(os.path.dirname(p), exist_ok=True)
        open(p, "w").close()
    assert op("done", dict(data=D, items={key: dict(out=sp["out"], done=o["done"])}), **sto)[:2] == (0, {key: []})
    rc, got, err = op("collect", dict(data=D, specs=[sp]), **sto)
    assert rc == 0 and "tasks" in got, (rc, got, err[-2000:])
    assert op("smoke", dict(data=D, recipe="toy", names={}), **sto)[:2] == (0, {})
    rc, got, err = op("export", dict(data=D, spec=sp), **sto)
    assert rc != 0 and "dataset.json" in err and not re.search(r"ModuleNotFoundError|ImportError", err), err[-2000:]
    ok("包里的 site_ops.py：执行机的 status（包名是这个包）、states；store 的 done（按 done 的每一项数盘上的文件）、collect、"
       "smoke、export（走到插件，缺的是数据不是模块）")


def test_scope_and_id(T):
    """包里只有执行机要用的调度文件（RUNTIME）加 OUTPUTS["bundle"] 减 bundle_skip；包名只按内容。
    这里把 bundle 换成 testdata（当 research 用），bundle_skip 换成 */bin/*、*/test_* 和 RUNTIME 里的 run_task.sh。"""
    saved = B.TOPS, B.SKIP
    B.TOPS, B.SKIP = (f"{QR}/testdata",), ("*/test_*", "*/bin/*", "*/run_task.sh")
    try:
        src = B.source_files(None)
        files = B.contents(None)
        bid = B.bundle_id(files)
    finally:
        B.TOPS, B.SKIP = saved
    for f in B.RUNTIME:
        if os.path.isfile(os.path.join(CORE, f)):
            assert f"{B.BC}/{f}" in src, f
    assert f"{BQ}/run_task.sh" in src, "RUNTIME 里的不受 bundle_skip 影响"
    for f in (f"{BQ}/relay.py", f"{BQ}/advance.py", f"{BQ}/relay_transfer.py", f"{BQ}/README.md",
              f"{BQ}/test_relay_bundle.py", STEP, f"{BQ}/testdata/bin/toy_train.sh"):
        assert f not in src, f
    assert not [f for f in src if f.startswith(B.BC + "/") and not f.startswith(f"{BQ}/testdata/") and f not in B.RUNTIME_B], src
    assert f"{QR}/testdata/exps/demo/experiments.py" in src
    ok("包里的文件：RUNTIME 全在（bundle_skip 不管它们），hub 才用的（relay.py、advance.py、relay_transfer.py）、说明、测试、"
       "bundle_skip 匹配的不在")

    def id_with(change):
        f = dict(files)
        change(f)
        return B.bundle_id(f)

    def edit(path):
        return lambda f: f.__setitem__(path, (f[path][0] + "\n# x\n", f[path][1]))
    assert id_with(lambda f: None) == bid
    assert id_with(edit(f"{BQ}/run_task.sh")) != bid
    assert id_with(edit(f"{QR}/testdata/exps/demo/experiments.py")) != bid
    assert id_with(edit(B.conf_path("machines.py"))) != bid, "配置原样带上：改了就换包"
    assert id_with(lambda f: f.__setitem__(f"{BQ}/run_task.sh", (f[f"{BQ}/run_task.sh"][0], False))) != bid
    assert id_with(lambda f: f.__setitem__(B.VERSION, ("0000000\n", False))) == bid, "核心的提交号不算进包名"
    ok("包名：只按内容（16 位），改执行机用的 run_task.sh、改 research 里的文件、改配置、改可执行位都换；核心的提交号（VERSION）改了不换")


SMOKE = {"test_worker.sh": "1", "test_worker_stop.sh": "3 4b", "test_worker_code.sh": "6e"}
KEEP_FIXTURE = ("test_worker_code.sh",)  # 6e 自己拿核心再搭两个代码包（不带 .taskq.toml），配置、插件经 fixture.sh 设的 TASKQ_CONFIG、TASKQ_PLUGINS 找


def checks_in(text, ids):
    """测试文件里 want <情况> 那几段的 check 个数。"""
    blocks = dict(re.findall(r"^if want (\S+); then\n(.*?)^fi$", text, re.M | re.S))
    assert set(ids.split()) <= set(blocks), ids
    return sum(len(re.findall(r"^check ", blocks[i], re.M)) for i in ids.split())


def test_worker_in_bundle(T):
    """包里的 worker.sbatch、worker_start.sh、consume.py、run_task.sh、taskq.py 跑 test_worker*.sh 的几个情况：
    Slurm 作业的调用方式、领、训、交还（1），停止信号交还（3），CPU worker（4b），代码包换新（6e），够看出包里原样的配置、
    .taskq.toml、taskq.py 的调用和代码包都能用；别的情况测的是 worker 本身的逻辑，和打不打包无关，单独跑那几个文件。三个文件同时跑。"""
    R = os.path.join(T, "R2")
    _, path = B.build(None, out=os.path.join(R, "code"))
    fake = os.path.join(path, os.path.dirname(B.conf_rel()))  # 包里机器表的 FAKE（testdata/）
    shutil.copytree(os.path.join(fixture.FIX, "venv"), os.path.join(fake, "venv"))  # 假的 venv（bin/activate 不是代码，不进包）
    fx ='source "$HERE/testdata/fixture.sh"'
    bare = 'PATH=$(python3 "$HERE/testdata/fixture.py" path "$PATH") || exit 1; export PATH'  # 不设 TASKQ_CONFIG、TASKQ_PLUGINS
    procs = {}
    for name, ids in SMOKE.items():
        test = open(os.path.join(HERE, name)).read()
        assert fx in test, name
        dst = os.path.join(path, BQ, "in_bundle_" + name)
        with open(dst, "w") as f:
            f.write(test if name in KEEP_FIXTURE else test.replace(fx, bare))
        p = subprocess.Popen(["bash", dst], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             env=bare_env(WORKER_TEST_ONLY=ids))
        procs[name] = (checks_in(test, ids), p)
    total = 0
    for name, (want, p) in procs.items():
        out, err = p.communicate(timeout=900)
        got = out.count("通过：")
        assert p.returncode == 0 and "全部通过" in out and not [ln for ln in out.splitlines() if ln.startswith("失败：")] and got == want, \
            f"{name}：{got}/{want}\n" + out[-3000:] + err[-2000:]
        total += got
    ok(f"包里的 worker 跑 test_worker*.sh 的几个情况（{'，'.join(f'{n} {i}' for n, i in SMOKE.items())}）：{total} 项全部通过")


def test_prune(T):
    """hub 上建的包：留 keep（各执行机在用的、闸门挡着的）和最近的 proto.CODE_KEEP 个；建到一半的临时目录 1 天后删。"""
    import time
    out = os.path.join(T, "prune")
    now = time.time()
    names = [f"b{i}" for i in range(proto.CODE_KEEP + 3)]  # b0 最新
    for i, d in enumerate(names + [".tmp_old", ".tmp_new"]):
        os.makedirs(os.path.join(out, d))
        t = now - 3 * 86400 if d == ".tmp_old" else now - 60 if d == ".tmp_new" else now - 3600 * i
        os.utime(os.path.join(out, d), (t, t))
    gone = B.prune({names[-1]}, out)
    left = set(os.listdir(out))
    assert sorted(gone) == sorted(names[proto.CODE_KEEP:-1]) and left == set(names[:proto.CODE_KEEP]) | {names[-1], ".tmp_new"}, (gone, left)
    ok(f"hub 上的包：留在用的和最近的 {proto.CODE_KEEP} 个，别的删掉；建到一半的临时目录 1 天后删")


def main():
    T = fixture.tmpdir(prefix="test_relay_bundle.")
    try:
        test_source_files_from_commit()
        test_secrets()
        test_source_files_submodule(T)
        bid, path = test_bundle(T)
        test_in_bundle(T, bid, path)
        test_scope_and_id(T)
        test_worker_in_bundle(T)
        test_prune(T)
    except BaseException:
        print(f"失败（文件在 {T}）")
        raise
    shutil.rmtree(T)
    print("全部通过")


if __name__ == "__main__":
    main()
