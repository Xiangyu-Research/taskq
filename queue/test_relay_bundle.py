#!/usr/bin/env python3
"""relay_bundle.py 的测试（2026-10-30；11/1 起不改名）：取文件、钥匙、生成的文件、包里的 taskq.py 完整走一遍、包里的 dicc_worker.sh 跑 test_dicc_worker*.sh 的几个情况。
包从工作区建（commit=None），根目录换成临时目录；配置用测试用的（testdata/，OUTPUTS["bundle"] 只有 scripts）。
    python3 scripts/taskq/queue/test_relay_bundle.py
"""
import ast
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, HERE)
import cfg  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
QR = cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）
CR = "" if cfg.CORE_REL == "." else cfg.CORE_REL + "/"  # 调度代码的文件在包里的前缀

cfg.use_config()
import machines as M  # noqa: E402
import relay_bundle as B  # noqa: E402

NAME = "dicc2"
STEP = f"{QR}/testdata/bin/step.sh"
FIX_PLUGINS = f"{QR}/testdata/plugins"  # 包里的测试用插件（相对包）


def ok(msg):
    print(f"通过：{msg}")


def run(cmd, env=None, **kw):
    """包里的命令：不带本机的配置（TASKQ_CONFIG），用包里生成的 envs/。"""
    e = {k: v for k, v in os.environ.items()
         if k not in ("TASKQ_DATA", "TASKQ_DIR", "TASKQ_MACHINE", "TASKQ_CONFIG", "TASKQ_PLUGINS")}
    e.update(env or {})
    return subprocess.run(cmd, capture_output=True, text=True, env=e, **kw)


def test_source_files_from_commit():
    files = B.source_files("HEAD")
    for f in (f"{QR}/taskq.py", f"{QR}/dicc_worker.sh", STEP, f"{QR}/testdata/exps/demo/experiments.py",
              f"{QR}/testdata/config/machines.py"):
        assert f in files, f
    assert B._wanted("x/a.R", 10) and not B._wanted("x/big.json", B.SMALL_MAX + 1) and not B._wanted("x/__pycache__/a.py", 1)
    assert not any(f.startswith(("papers/", "admin/", "talks/")) for f in files)
    assert not any("__pycache__" in f for f in files) and all(len(files[f][0]) <= B.SMALL_MAX for f in files if f.endswith(B.SMALL))
    head = subprocess.run(["git", "-C", REPO, "show", f"HEAD:{STEP}"], capture_output=True).stdout
    mode = subprocess.run(["git", "-C", REPO, "ls-tree", "HEAD", STEP], capture_output=True, text=True).stdout.split()[0]
    assert files[STEP] == (head, mode == "100755")
    ok("从提交取文件：OUTPUTS[\"bundle\"] 下的代码（含 .R）和小数据文件，内容和可执行位同提交（.sh 在包里另设可执行），没有 papers/ 等")


def test_source_files_submodule(T):
    """核心是子模块（11/4）：从提交、从工作区取到的文件和普通目录时一样（包名不变）。"""
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")

    def g(*a, cwd):
        subprocess.run(["git", "-c", "protocol.file.allow=always", *a], cwd=cwd, env=env, check=True, capture_output=True)

    def fill(d):
        for f, body, exe in (("core/q/a.sh", "echo a\n", True), ("core/q/b.py", "B = 1\n", False), ("core/big.json", "x" * 10, False),
                             ("cfg/m.py", "M = 2\n", False), ("other/c.py", "", False)):
            os.makedirs(os.path.dirname(os.path.join(d, f)), exist_ok=True)
            with open(os.path.join(d, f), "w") as fh:
                fh.write(body)
            os.chmod(os.path.join(d, f), 0o755 if exe else 0o644)

    flat, core, sup = (os.path.join(T, "sub", x) for x in ("flat", "core", "sup"))
    for d in (flat, core, sup):
        os.makedirs(d)
        g("init", "-q", "-b", "main", cwd=d)
    fill(flat)
    g("add", "-A", cwd=flat)
    g("commit", "-qm", "f", cwd=flat)
    fill(core)
    shutil.rmtree(os.path.join(core, "cfg"))
    shutil.rmtree(os.path.join(core, "other"))
    for f in os.listdir(os.path.join(core, "core")):
        os.rename(os.path.join(core, "core", f), os.path.join(core, f))
    os.rmdir(os.path.join(core, "core"))
    g("add", "-A", cwd=core)
    g("commit", "-qm", "c", cwd=core)
    fill(sup)
    shutil.rmtree(os.path.join(sup, "core"))
    g("submodule", "add", "-q", core, "core", cwd=sup)
    g("add", "-A", cwd=sup)
    g("commit", "-qm", "s", cwd=sup)
    saved = B.REPO, B.TOPS, B.RUNTIME
    try:
        B.TOPS, B.RUNTIME = ("cfg",), ("core/q/a.sh", "core/q/b.py", "core/big.json")
        got = {}
        for repo in (flat, sup):
            B.REPO = repo
            got[repo] = (B.source_files("HEAD"), B.source_files(None), B.read("HEAD", "core/q/b.py"))
    finally:
        B.REPO, B.TOPS, B.RUNTIME = saved
    files = got[flat][0]
    assert sorted(files) == ["cfg/m.py", "core/big.json", "core/q/a.sh", "core/q/b.py"], sorted(files)
    assert files["core/q/a.sh"] == (b"echo a\n", True) and files["core/q/b.py"] == (b"B = 1\n", False)
    assert got[sup] == got[flat], (got[sup], got[flat])
    ok("核心是子模块：从提交（取它钉的提交）、从工作区取到的文件、可执行位和普通目录时相同")


def test_secrets():
    sk = "-----BEGIN OPENSSH " + "PRIVATE KEY-----"
    tok = "ghp_" + "a1" * 18
    assert B.secret_hits({"a.sh": ("x\n" + sk + "\n", False), "b.py": (f"T = '{tok}'", False), "c.py": ("ok = 'ghp_short'", False)}) \
        == [("a.sh", 2, sk[:12]), ("b.py", 1, tok[:12])]
    saved = B.source_files
    B.source_files = lambda c: dict(saved(c), **{"scripts/x.sh": (f"echo {tok}".encode(), False)})
    try:
        B.contents(NAME, None)
        raise AssertionError("有钥匙应报错")
    except B.BundleError as e:
        assert "scripts/x.sh:1" in str(e) and tok not in str(e), e
    finally:
        B.source_files = saved
    ok("钥匙：私钥、GitHub token 报错（报错里不带整个钥匙），短的不算")


def test_bundle(T):
    R = os.path.join(T, "R")
    bid, path = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R)
    assert path == os.path.join(R, "code", bid) and re.fullmatch(r"[0-9a-f]{16}", bid), bid
    bid2, path2 = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R)
    assert (bid2, path2) == (bid, path)
    assert not [n for n in os.listdir(os.path.join(R, "code")) if n.startswith(".")]
    ok("建包：包名是 16 位内容哈希，同样的内容再建一次用原来的包、不留临时目录")

    files = sorted(os.path.relpath(os.path.join(d, f), path) for d, _, fs in os.walk(path) for f in fs)
    src = B.source_files(None)
    for f in ("envs/machines.py", "envs/policy.py", f"{QR}/worker.sbatch", f"{QR}/taskq.py", f"{QR}/dicc_worker.sh",
              f"{QR}/testdata/bin/toy_train.sh", STEP, f"{QR}/testdata/exps/demo/experiments.py"):
        assert f in files, f
    assert f"{QR}/cpu_worker.sbatch" not in files, "这台没有 jobs.cpu"
    for f in (STEP, f"{QR}/testdata/bin/infer_task.sh", f"{QR}/taskq.py"):
        assert open(os.path.join(path, f), encoding="utf-8").read() == B.home_paths(src[f][0].decode(), R, f), f"{f} 和提交的不一样"
        assert os.access(os.path.join(path, f), os.X_OK) == (src[f][1] or f.endswith(".sh")), f
    assert open(os.path.join(path, STEP), "rb").read() == src[STEP][0]
    for ln, tgt in B.LINKS.items():
        if os.path.dirname(ln) + "/" + tgt not in files:  # 包里没有旧位置的转交（单独的检出）：不建链接
            assert not os.path.lexists(os.path.join(path, ln)), ln
            continue
        assert os.readlink(os.path.join(path, ln)) == tgt and os.path.isfile(os.path.join(path, ln)), ln
    assert ".cache/phd" in src[f"{QR}/gpu_pad_start.sh"][0].decode()
    gp = open(os.path.join(path, f"{QR}/gpu_pad_start.sh"), encoding="utf-8").read()
    assert ".cache/phd" not in gp, gp[:300]
    ok("包里的文件：提交里的代码原样（step.sh、infer_task.sh 等不改），~/.cache/phd 换成根目录下的，start.sh、loop.sh 指向新名字")

    os.symlink(bid, os.path.join(R, "code", "current"))
    for d in ("data", "queue"):
        os.makedirs(os.path.join(R, d))
    cur = os.path.join(R, "code", "current")
    mp = os.path.join(cur, "envs", "machines.py")
    assert run(["python3", mp, "whoami"]).stdout.strip() == NAME
    assert run(["python3", mp, "names", "slurm"]).stdout.split() == [NAME]
    assert run(["python3", mp, "get", ".", "data"]).stdout.strip() == f"{R}/data"
    e = B.relay_entry(None, NAME)  # 包按机器表的源码读（测试用的机器表在源码里读时 conda 是 /nonexistent/…）
    assert run(["python3", mp, "get", ".", "conda"]).stdout.split() == list(e["conda"])
    act = run(["python3", mp, "activate", "base"]).stdout
    assert f"conda activate {e['envs']['base'][1]}" in act and e["conda"][0] in act
    pol = run(["python3", "-c", "import sys; sys.path.insert(0, sys.argv[1]); import machines, policy; "
               "print(int(bool(policy.STAGE_TMP.get(machines.whoami()))), int(bool(machines.MACHINES[machines.whoami()].get('task_vars'))))",
               os.path.join(cur, "envs")]).stdout.split()
    assert pol == ["1", "0"], pol
    ok("生成的 machines.py、policy.py：只认得这个账号（按数据目录认出本机），环境、数据目录对；STAGE_TMP 开、没有 task_vars")
    other = next(n for n in M.RELAY if n != NAME)
    got = run(["python3", "-c", "import sys; sys.path.insert(0, sys.argv[1]); import machines\ntry:\n    machines.machine(sys.argv[2])\n"
               "except KeyError:\n    print('KeyError')", os.path.join(cur, "envs"), other])
    assert got.stdout.strip() == "KeyError", got
    got = run(["python3", mp])
    assert got.returncode == 2 and "Traceback" not in got.stderr and "命令行" in open(mp).read(), got
    ok("生成的 machines.py：查别的机器是 KeyError（不是 NameError），不带参数运行打印用法、退出码 2")

    tq, cf = open(os.path.join(cur, QR, "taskq.py")).read(), open(os.path.join(cur, QR, "cfg.py")).read()
    assert tq.split("\n")[:10] == src[f"{QR}/taskq.py"][0].decode().split("\n")[:10] and "~/.cache/phd" in "\n".join(tq.split("\n")[:10])
    assert "~/.cache/phd" not in "\n".join(ln for ln in cf.split("\n") if ln.startswith("STATE_")) and f'STATE_HOME = "{R}/state/"' in cf, cf
    sh = B.home_paths("#!/bin/bash\n# 队列在 ~/.cache/phd/taskq\nQ=${Q:-$HOME/.cache/phd/taskq}\n", R, "x.sh").split("\n")
    assert sh[1] == "# 队列在 ~/.cache/phd/taskq" and sh[2] == f"Q=${{Q:-{R}/queue}}", sh
    ok("换家目录下的路径只换代码：taskq.py 开头的说明、shell 的注释行不换，cfg.py 的 STATE_HOME 等照常换")

    sb = open(os.path.join(cur, f"{QR}/worker.sbatch")).read().split("\n")
    real = open(os.path.join(REPO, f"{QR}/worker.sbatch")).read()
    for ln in real.split("\n"):
        if ln.startswith("#SBATCH") and not any(o in ln for o in ("--output", "--job-name", "--cpus-per-task", "--mem", "--partition", "--gres")):
            assert ln in sb, ln
    assert "#SBATCH --cpus-per-task=30" in sb and "#SBATCH --mem=60G" in sb, sb
    assert "#SBATCH --partition=gpu-a" in sb and "#SBATCH --gres=gpu:a:1" in sb, sb  # 条目的 sbatch 覆盖模板（11/4）
    assert not any(x.startswith("#SBATCH --job-name") for x in sb), sb
    assert f"#SBATCH --output={R}/data/logs/workers/%x_%j.out" in sb
    assert f"R={R}" in sb and 'REPO=$(readlink -f "$R/code/current")' in sb
    assert "export TASKQ_DIR=$R/queue TASKQ_GPU_PAD_SWITCH=$R/queue/gpu_pad_DISABLED" in sb
    assert sb[-2] == f'exec bash "$REPO/{QR}/worker_start.sh"' and "git" not in "\n".join(sb)
    assert run(["bash", "-n", os.path.join(cur, f"{QR}/worker.sbatch")]).returncode == 0
    fake = os.path.join(T, "fakeR")
    os.makedirs(os.path.join(fake, "code", "b1", QR))
    top = "${BASH_SOURCE[0]}"
    for _ in range(1 + len(QR.split("/"))):  # worker_start.sh 往上到包的根
        top = f'$(dirname "{top}")'
    with open(os.path.join(fake, "code", "b1", QR, "worker_start.sh"), "w") as f:
        f.write(f'echo "$TASKQ_DIR|$TASKQ_GPU_PAD_SWITCH|{top}"\n')
    os.symlink("b1", os.path.join(fake, "code", "current"))
    body = "\n".join(sb).replace(f"R={R}", f"R={fake}")
    out = run(["bash", "-c", body]).stdout.strip()
    assert out == f"{fake}/queue|{fake}/queue/gpu_pad_DISABLED|{fake}/code/b1", out
    ok("worker.sbatch：CPU、内存、作业名、输出位置按账号，其余资源和原来一样；开始时按 current 的实际包跑，队列和显存开关在根目录下")

    env = {"TASKQ_DATA": f"{R}/data", "TASKQ_DIR": f"{R}/queue", "TASKQ_PLUGINS": os.path.join(cur, FIX_PLUGINS)}
    tq = ["python3", os.path.join(cur, f"{QR}/taskq.py")]
    key = "D999_abcdef/V_save/0"
    r = run(tq + ["add", "--nice", "0", "--recipe", "toy", "ds=D999_abcdef", "frag=0"], env)
    assert r.returncode == 0 and key in r.stdout, r
    st = json.loads(next(ln for ln in run(tq + ["--json-status"], env).stdout.splitlines() if ln.startswith("@@JSON"))[6:])
    assert [t["key"] for t in st["tasks"]] == [key] and st["tasks"][0]["state"] == "free", st
    assert st["tasks"][0]["spec"]["relay"] == "toy" and st["tasks"][0]["exp"] is None
    r = run(tq + ["next", "--worker", "123", "--envs", "base", "--device", "gpu"], env)
    assert r.returncode == 0, r
    lines = r.stdout.strip().split("\n")
    assert lines[0] == key and lines[1].startswith("toy ") and "ds=D999_abcdef" in lines[1], lines
    assert run(tq + ["next", "--worker", "124", "--envs", "base", "--device", "gpu"], env).returncode == 10
    assert run(tq + ["beat", "--worker", "123", key], env).returncode == 0
    assert run(tq + ["stop", key], env).returncode == 0
    assert os.path.exists(f"{R}/queue/stopped/{key.replace('/', '__')}")
    st = json.loads(next(ln for ln in run(tq + ["--json-status"], env).stdout.splitlines() if ln.startswith("@@JSON"))[6:])
    assert st["tasks"][0]["state"] == "stopped", st["tasks"][0]["state"]
    assert run(tq + ["release", "--worker", "123", key], env).returncode == 0
    assert run(tq + ["resume", key], env).returncode == 0
    assert run(tq + ["remove", key], env).returncode == 0
    assert not open(f"{R}/queue/tasks.jsonl").read().strip()
    r = run(tq + ["--json-status"], {"TASKQ_DATA": f"{R}/data"})
    assert r.returncode == 0 and '"enabled": false' in r.stdout
    ok("包里的 taskq.py：add → --json-status → next（0；再领 10）→ beat → stop → release → resume → remove；"
       "不设 TASKQ_DIR 也用根目录下的 queue/")
    return path


def test_scope_and_id(T):
    """11/4 起：包里只有执行机要用的调度文件（RUNTIME）加 OUTPUTS["bundle"] 减 bundle_skip；包名只按内容。
    这里把 bundle 换成 testdata（当 research 用），bundle_skip 换成 */bin/*、*/test_* 和 RUNTIME 里的 run_task.sh。"""
    saved = B.TOPS, B.SKIP
    B.TOPS, B.SKIP = (f"{QR}/testdata",), ("*/test_*", "*/bin/*", "*/run_task.sh")
    try:
        src = B.source_files(None)
        files = B.contents(NAME, None, root=os.path.join(T, "S"))
        bid = B.bundle_id(files)
    finally:
        B.TOPS, B.SKIP = saved
    for f in B.RUNTIME:
        if os.path.isfile(os.path.join(REPO, f)) and not f.endswith(".sbatch"):
            assert f in src, f
    assert f"{QR}/run_task.sh" in src, "RUNTIME 里的不受 bundle_skip 影响"
    for f in (f"{QR}/relay.py", f"{QR}/advance.py", f"{QR}/README.md",
              f"{QR}/test_relay_bundle.py", STEP, f"{QR}/testdata/bin/toy_train.sh"):
        assert f not in src, f
    assert not [f for f in src if f.startswith(CR) and not f.startswith(f"{QR}/testdata/") and f not in B.RUNTIME], src
    assert f"{QR}/testdata/exps/demo/experiments.py" in src
    ok("包里的文件：RUNTIME 全在（bundle_skip 不管它们），box 才用的（relay.py、advance.py）、说明、测试、bundle_skip 匹配的不在")

    def id_with(change):
        f = dict(files)
        change(f)
        return B.bundle_id(f)

    def edit(path):
        return lambda f: f.__setitem__(path, (f[path][0] + "\n# x\n", f[path][1]))
    assert id_with(lambda f: None) == bid
    assert id_with(edit(f"{QR}/run_task.sh")) != bid
    assert id_with(edit(f"{QR}/testdata/exps/demo/experiments.py")) != bid
    assert id_with(lambda f: f.__setitem__(f"{QR}/run_task.sh", (f[f"{QR}/run_task.sh"][0], False))) != bid
    src_m = B.read_machines(None)
    first = ast.get_docstring(ast.parse(src_m))
    m = B.machine_entry(NAME, B.relay_entry(None, NAME), os.path.join(T, "S"), None)
    a = B.gen_machines(src_m, NAME, m)
    b = B.gen_machines(src_m.replace(first, first + "\n多一行说明", 1).replace("\n\n", "\n\n# 新加的整行注释\n", 1), NAME, m)
    assert a == b and first not in a, a[:300]
    ok("包名：只按内容（16 位），改执行机用的 run_task.sh、改 research 里的文件、改可执行位都换；"
       "机器表开头的说明、整行注释改了不换")


def test_cpu_sbatch(T):
    """jobs 里有 cpu 的 Slurm 执行机：另外生成 cpu_worker.sbatch（作业名、资源按 sbatch.cpu，WORKER_DEVICE=cpu）。"""
    R = "~/st/abc"
    entry = dict(jobs=("gpu", "cpu"), sbatch=dict(gpu=dict(cpus=30, mem="60G"), cpu=dict(cpus=8, mem="16G")))
    files = B.contents(NAME, None, root=R, entry=entry)
    sb = files[f"{QR}/cpu_worker.sbatch"][0].split("\n")
    real = open(os.path.join(REPO, f"{QR}/cpu_worker.sbatch")).read()
    for ln in real.split("\n"):
        if ln.startswith("#SBATCH") and not any(o in ln for o in ("--output", "--job-name", "--cpus-per-task", "--mem")):
            assert ln in sb, ln
    assert not any(x.startswith("#SBATCH --job-name") for x in sb) and "#SBATCH --cpus-per-task=8" in sb and "#SBATCH --mem=16G" in sb, sb
    up = "/".join([".."] * (2 + len(QR.split("/"))))  # 在 <根>/code/current/<QR> 交，往上到根目录
    assert f"#SBATCH --output={up}/data/logs/workers/%x_%j.out" in sb and "R=~/st/abc" in sb, sb
    assert "export TASKQ_DIR=$R/queue TASKQ_GPU_PAD_SWITCH=$R/queue/gpu_pad_DISABLED WORKER_DEVICE=cpu" in sb
    assert sb[-2] == f'exec bash "$REPO/{QR}/worker_start.sh"' and "git" not in "\n".join(sb)
    gsb = files[f"{QR}/worker.sbatch"][0]
    assert "#SBATCH --cpus-per-task=30" in gsb and "WORKER_DEVICE" not in gsb
    old = B.contents(NAME, None, root=R, entry=dict(jobs=("gpu", "cpu"), sbatch=dict(cpus=30, mem="60G")))
    assert "#SBATCH --cpus-per-task=30" in old[f"{QR}/worker.sbatch"][0]
    assert "#SBATCH --cpus-per-task=30" not in old[f"{QR}/cpu_worker.sbatch"][0]
    ok("cpu_worker.sbatch：jobs.cpu 有时生成，不带作业名，资源按 sbatch.cpu（老写法的 sbatch 只算 gpu），WORKER_DEVICE=cpu，入口 worker_start.sh")


def test_pueue_site(T):
    """pueue 的执行机（lab 那样，10/31）：不生成 sbatch；slots、worker、组照抄。"""
    R = os.path.join(T, "pueue_site")
    entry = dict(scheduler="pueue", groups={"g1": (1, "1", {})}, slots=1, worker=dict(group="g1", id="w1"))
    files = B.contents(NAME, None, root=R, entry=entry)
    assert not [f for f in files if f.startswith(f"{QR}/") and f.endswith(".sbatch")], sorted(files)
    assert f"{QR}/worker_start.sh" in files
    _, path = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R, entry=entry)
    os.makedirs(os.path.join(R, "data"))
    mp = os.path.join(path, "envs", "machines.py")
    assert run(["python3", mp, "get", ".", "slots"]).stdout.strip() == "1"
    assert run(["python3", mp, "names", "pueue"]).stdout.split() == [NAME]
    ok("pueue 的执行机：没有 worker.sbatch、cpu_worker.sbatch，slots、组照抄")

    R1 = os.path.join(T, "lab1_site")
    _, path = B.build("lab1", commit=None, out=os.path.join(R1, "code"), root=R1)
    got = run(["python3", "-c", "import sys, json; sys.path.insert(0, sys.argv[1]); import machines, policy; "
               "print(json.dumps([machines.MACHINES['lab1'].get('task_vars'), policy.STAGE_TMP]))", os.path.join(path, "envs")])
    assert json.loads(got.stdout) == [M.RELAY["lab1"]["task_vars"], {"lab1": False}], got
    assert M.RELAY["lab1"]["task_vars"] == {"DEMO_FAST": "1", "DEMO_FAST_LOADER": "1"}
    ok("生成的 machines.py 带上 RELAY 里写的 task_vars（lab1），STAGE_TMP 按 stage_tmp")


SMOKE = {"test_dicc_worker.sh": "1", "test_dicc_worker_stop.sh": "3 4b", "test_dicc_worker_code.sh": "6e"}


def checks_in(text, ids):
    """测试文件里 want <情况> 那几段的 check 个数。"""
    blocks = dict(re.findall(r"^if want (\S+); then\n(.*?)^fi$", text, re.M | re.S))
    assert set(ids.split()) <= set(blocks), ids
    return sum(len(re.findall(r"^check ", blocks[i], re.M)) for i in ids.split())


def test_dicc_worker_in_bundle(T):
    """包里的 dicc_worker.sh、run_task.sh、taskq.py 跑 test_dicc_worker*.sh 的几个情况（环境换成本机的，好真激活）：
    两个位置领、训、交还（1），停止信号交还（3），CPU worker（4b），代码包换新（6e），够看出包里生成的 envs/、改过的路径、
    taskq.py 的调用和代码包都能用；别的情况测的是 worker 本身的逻辑，和打不打包无关，单独跑那几个文件。三个文件同时跑。"""
    R = os.path.join(T, "R2")
    me = M.MACHINES[M.whoami()]
    _, path = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R, entry=dict(conda=me["conda"], envs=me["envs"]))
    os.makedirs(os.path.join(R, "data"))
    fx = 'source "$HERE/testdata/fixture.sh"'
    e = {k: v for k, v in os.environ.items()
         if k not in ("TASKQ_DATA", "TASKQ_DIR", "TASKQ_MACHINE", "TASKQ_CONFIG", "TASKQ_PLUGINS")}
    procs = {}
    for name, ids in SMOKE.items():
        test = open(os.path.join(HERE, name)).read()
        assert "TASKQ_MACHINE=dicc " in test and fx in test, name
        test = test.replace(fx, f'export TASKQ_CONFIG={shlex.quote(os.path.join(path, "envs"))} TASKQ_PLUGINS=$HERE/testdata/plugins')  # 机器表用包里生成的 envs/
        dst = os.path.join(path, QR, "in_bundle_" + name)
        with open(dst, "w") as f:
            f.write(test.replace("TASKQ_MACHINE=dicc ", f"TASKQ_MACHINE={NAME} "))
        p = subprocess.Popen(["bash", dst], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=dict(e, WORKER_TEST_ONLY=ids))
        procs[name] = (checks_in(test, ids), p)
    total = 0
    for name, (want, p) in procs.items():
        out, err = p.communicate(timeout=900)
        got = out.count("通过：")
        assert p.returncode == 0 and "全部通过" in out and not [ln for ln in out.splitlines() if ln.startswith("失败：")] and got == want, \
            f"{name}：{got}/{want}\n" + out[-3000:] + err[-2000:]
        total += got
    ok(f"包里的 dicc_worker.sh 跑 test_dicc_worker*.sh 的几个情况（{'，'.join(f'{n} {i}' for n, i in SMOKE.items())}）：{total} 项全部通过")


def main():
    T = tempfile.mkdtemp(prefix="test_relay_bundle.")
    try:
        test_source_files_from_commit()
        test_secrets()
        test_source_files_submodule(T)
        test_bundle(T)
        test_scope_and_id(T)
        test_cpu_sbatch(T)
        test_pueue_site(T)
        test_dicc_worker_in_bundle(T)
    except BaseException:
        print(f"失败（文件在 {T}）")
        raise
    shutil.rmtree(T)
    print("全部通过")


if __name__ == "__main__":
    main()
