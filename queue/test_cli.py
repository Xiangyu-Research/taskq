#!/usr/bin/env python3
"""bin/taskq 的测试（2026-11-04）：每个命令的 --help 退出码 0、不碰任何文件；转给模块的命令和老脚本的结果一样；--version、
path、找项目、self update 和 install.sh（临时 HOME、本地的来源仓库）。不连任何机器。  python3 queue/test_cli.py"""
import contextlib
import importlib.machinery
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
BIN = os.path.join(CORE, "bin", "taskq")
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

T = tempfile.mkdtemp()
HOME, PROJ, TQ, DATA, TMP = (os.path.join(T, x) for x in ("home", "proj", "tq", "data", "tmp"))
for d in (HOME, PROJ, TQ, DATA, TMP):
    os.makedirs(d)
with open(os.path.join(PROJ, ".taskq.toml"), "w") as f:
    f.write(f'config = "{fixture.CONFIG}"\nplugins = ["{fixture.PLUGINS}"]\n')
CLEAN = {k: v for k, v in os.environ.items() if not k.startswith("TASKQ_")}
ENV = dict(CLEAN, HOME=HOME, TMPDIR=TMP, TASKQ_DIR=TQ, TASKQ_DATA=DATA, TASKQ_MACHINE="box", PYTHONDONTWRITEBYTECODE="1")
OLD_ENV = dict(ENV, TASKQ_CONFIG=fixture.CONFIG, TASKQ_PLUGINS=fixture.PLUGINS, TASKQ_REPO=PROJ)  # 老脚本：直接给（找项目导出的）变量
GIT = dict(GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
TASKS = [(1, 5, "D1/V/0"), (2, 7, "D1/V/1"), (3, 9, "D2/V/0")]


def seed():
    with open(os.path.join(TQ, "tasks.jsonl"), "w") as f:
        for order, nice, key in TASKS:
            ds, var, frag = key.split("/")
            f.write(json.dumps(dict(order=order, nice=nice, key=key, exp=None, recipe="toy",
                                    params=dict(ds=ds, variant=var, frag=frag))) + "\n")


def taskq(*args, cwd=PROJ, env=None, exe=BIN):
    return subprocess.run([sys.executable, exe, *args], cwd=cwd, env=env or ENV, capture_output=True, text=True, timeout=120)


def old(script, *args):
    return subprocess.run([sys.executable, os.path.join(CORE, script), *args], cwd=CORE, env=OLD_ENV,
                          capture_output=True, text=True, timeout=120)


def tree(root):
    """root 下所有文件和文件夹 → 修改时间、大小。"""
    out = {}
    for d, ds, fs in os.walk(root):
        for x in ds + fs:
            p = os.path.join(d, x)
            st = os.lstat(p)
            out[os.path.relpath(p, root)] = (st.st_mtime_ns, st.st_size)
    return out


def cli():
    """bin/taskq 当模块载入（文件名没有 .py）。"""
    loader = importlib.machinery.SourceFileLoader("taskq_cli", BIN)
    spec = importlib.util.spec_from_loader("taskq_cli", loader)
    m = importlib.util.module_from_spec(spec)
    loader.exec_module(m)
    return m


def all_commands():
    m = cli()
    out = [[]]
    for name, node in m.TREE.items():
        out.append([name])
        if isinstance(node, m.Group):
            out += [[name, sub] for sub in node.cmds]
    return out


class Help(unittest.TestCase):
    def test_every_help(self):
        seed()
        before = tree(T)
        for path in all_commands():
            for h in ("--help", "-h"):
                r = taskq(*path, h)
                self.assertEqual(r.returncode, 0, (path, h, r.stderr[-500:]))
                self.assertTrue(r.stdout.strip(), (path, h))
        self.assertEqual(tree(T), before, "--help 不改任何文件")

    def test_help_word(self):
        self.assertEqual(taskq("help", "site", "status").stdout, taskq("site", "status", "--help").stdout)

    def test_top_page_hides_plumbing(self):
        out = taskq("--help").stdout
        for name in ("add", "status", "plan", "tick", "site", "exp", "self"):
            self.assertIn(f"  {name} ", out)
        for name in ("worker", "spec", "internal"):
            self.assertNotIn(f"  {name} ", out)

    def test_old_scripts_help_does_not_act(self):
        before = tree(T)
        for script in ("queue/shift.py", "queue/advance.py", "queue/feed.py", "monitor/health.py", "sites/site.py",
                       "queue/qs", "queue/relay.py", "queue/taskq.py"):
            r = old(script, "--help")
            self.assertEqual(r.returncode, 0, (script, r.stderr[-500:]))
        for args in (("push", "--help"), ("rsync", "--help"), ("--help",)):
            self.assertEqual(old("sync/tunnel.py", *args).returncode, 0, args)
        self.assertEqual(tree(T), before)


class Routing(unittest.TestCase):
    def setUp(self):
        seed()

    def test_status_json_same_as_script(self):
        a, b = taskq("status", "--json"), old("queue/taskq.py", "status", "--json")
        self.assertEqual((a.returncode, a.stdout), (b.returncode, b.stdout))
        self.assertTrue(a.stdout.startswith("@@JSON "))
        self.assertEqual(len(json.loads(a.stdout[len("@@JSON "):])["tasks"]), len(TASKS))

    def test_status_text_same_as_script(self):
        self.assertEqual(taskq("status").stdout, old("queue/taskq.py", "status").stdout)

    def test_nice_and_rm(self):
        r = taskq("nice", "--nice", "3", "D1/V/1")
        self.assertEqual(r.returncode, 0, r.stderr)
        rows = [json.loads(x) for x in open(os.path.join(TQ, "tasks.jsonl"))]
        self.assertEqual({t["key"]: t["nice"] for t in rows}["D1/V/1"], 3)
        self.assertEqual(taskq("rm", "D2/V/0").returncode, 0)
        self.assertEqual(taskq("rm", "D2/V/0").returncode, 4, "退出码照旧（4 = 队列里没有）")

    def test_worker_next_exit_code(self):
        r = taskq("worker", "next", "--worker", "w1", "--envs", "nothing-here")
        self.assertEqual(r.returncode, 10)

    def test_spec_resolve_same_as_script(self):
        args = ("resolve", "toy", "ds=D1", "variant=V", "frag=0")
        a, b = taskq("spec", *args), old("queue/spec.py", *args)
        self.assertEqual((a.returncode, a.stdout), (b.returncode, b.stdout))

    def test_exp_env_of_same_as_script(self):
        a, b = taskq("exp", "env-of", "/no/such/model"), old("queue/experiments.py", "env-of", "/no/such/model")
        self.assertEqual((a.returncode, a.stdout), (b.returncode, b.stdout))
        self.assertNotEqual(a.returncode, 0)

    def test_no_queue_message_on_stderr(self):
        r = taskq("add", "--nice", "1", "--recipe", "toy", "ds=D9", "variant=V", "frag=0",
                  env=dict(ENV, TASKQ_DIR=os.path.join(T, "none"), TASKQ_MACHINE="lab1"))
        self.assertEqual(r.returncode, 2)
        self.assertIn("总队列在", r.stderr)
        self.assertEqual(r.stdout, "")

    def test_unknown(self):
        r = taskq("statsu")
        self.assertEqual(r.returncode, 2)
        self.assertIn("status", r.stderr)
        self.assertEqual(taskq("site", "nope").returncode, 2)
        self.assertEqual(taskq().returncode, 2)
        self.assertEqual(taskq("--bogus").returncode, 2)

    def test_plan_sections_read_only(self):
        """plan：同一份 gather() 给 shift、feed、advance，都是 dry。"""
        fixture.use()
        os.environ.update(TASKQ_DIR=TQ, TASKQ_DATA=DATA)
        m = cli()
        m.paths()
        import health
        import shift
        import feed
        import advance
        calls, s = [], {"x": 1}
        saved = health.gather, shift.run, feed.run_all, advance.plan
        try:
            health.gather = lambda: s
            shift.run = lambda st, dry=False: calls.append(("shift", st is s, dry)) or ["开 lab0"]
            feed.run_all = lambda st, dry=False: calls.append(("feed", st is s, dry)) or (["送 a"], {"x": "卡 b"})
            advance.plan = lambda st: calls.append(("advance", st is s, True)) or ([], {})
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = m.main(["plan", "-n"])
        finally:
            health.gather, shift.run, feed.run_all, advance.plan = saved
        self.assertEqual(rc, 0)
        self.assertEqual(sorted(calls), [("advance", True, True), ("feed", True, True), ("shift", True, True)])
        out = buf.getvalue()
        for x in ("== 按时段开关", "开 lab0", "== 送任务", "送 a", "卡住：卡 b", "== 训完以后", "没有要排的"):
            self.assertIn(x, out)


class Project(unittest.TestCase):
    def test_flag_and_env(self):
        seed()
        elsewhere = os.path.join(T, "elsewhere")
        os.makedirs(elsewhere, exist_ok=True)
        want = taskq("status", "--json").stdout
        self.assertEqual(taskq("--project", PROJ, "status", "--json", cwd=elsewhere).stdout, want)
        self.assertEqual(taskq("status", "--json", cwd=elsewhere, env=dict(ENV, TASKQ_PROJECT=PROJ)).stdout, want)
        sub = os.path.join(PROJ, "a", "b")
        os.makedirs(sub, exist_ok=True)
        self.assertEqual(taskq("status", "--json", cwd=sub).stdout, want, "从下级目录往上找 .taskq.toml")
        r = taskq("--project", elsewhere, "status")
        self.assertEqual(r.returncode, 2)

    @unittest.skipIf(CORE.endswith(os.path.join("scripts", "taskq")), "嵌在仓库里时老布局能找到配置")
    def test_no_project(self):
        empty = os.path.join(T, "empty")
        os.makedirs(empty, exist_ok=True)
        r = taskq("status", cwd=empty)
        self.assertEqual(r.returncode, 2)
        self.assertIn("找不到项目", r.stderr)
        self.assertEqual(taskq("path", "lib", cwd=empty).returncode, 0, "path 不要项目")


class Self(unittest.TestCase):
    def test_path(self):
        self.assertEqual(taskq("path", "lib").stdout.strip(), os.path.join(CORE, "queue"))
        self.assertEqual(taskq("path", "api").stdout.strip(), os.path.join(CORE, "queue"))
        self.assertEqual(taskq("path", "core").stdout.strip(), CORE)
        self.assertEqual(taskq("path", "x").returncode, 2)
        link = os.path.join(T, "linkbin")
        os.makedirs(link, exist_ok=True)
        if not os.path.exists(os.path.join(link, "taskq")):
            os.symlink(BIN, os.path.join(link, "taskq"))
        self.assertEqual(taskq("path", "lib", exe=os.path.join(link, "taskq")).stdout.strip(), os.path.join(CORE, "queue"),
                         "经软链接调用时找到真正的核心")

    def test_version(self):
        vf = os.path.join(CORE, "VERSION")
        want = open(vf).read().strip() if os.path.exists(vf) else subprocess.run(  # noqa: SIM115
            ["git", "-C", CORE, "describe", "--always", "--dirty"], capture_output=True, text=True).stdout.strip()
        self.assertEqual(taskq("--version").stdout.strip(), f"taskq {want}")
        self.assertEqual(taskq("self", "version").stdout.strip(), want)
        fake = os.path.join(T, "bundle_core")
        os.makedirs(os.path.join(fake, "bin"), exist_ok=True)
        shutil.copy(BIN, os.path.join(fake, "bin", "taskq"))
        with open(os.path.join(fake, "VERSION"), "w") as f:
            f.write("abc1234\n")
        self.assertEqual(taskq("--version", exe=os.path.join(fake, "bin", "taskq")).stdout.strip(), "taskq abc1234")
        r = taskq("self", "update", exe=os.path.join(fake, "bin", "taskq"))
        self.assertEqual(r.returncode, 1, "不是 git 检出：不更新")

    def test_install_and_update(self):
        origin = os.path.join(T, "origin")
        os.makedirs(origin)
        files = subprocess.run(["git", "-C", CORE, "ls-files", "-co", "--exclude-standard"], capture_output=True, text=True).stdout.split()
        for f in files:
            if os.path.isfile(os.path.join(CORE, f)):
                os.makedirs(os.path.dirname(os.path.join(origin, f)), exist_ok=True)
                shutil.copy2(os.path.join(CORE, f), os.path.join(origin, f))
        env = dict(ENV, **GIT)
        g = lambda *a: subprocess.run(["git", "-C", origin, *a], env=env, capture_output=True, text=True, check=True)  # noqa: E731
        g("init", "-q", "-b", "main")
        g("add", "-A")
        g("commit", "-qm", "v1")
        dest = os.path.join(HOME, ".local", "share", "taskq")
        ienv = dict(env, TASKQ_ORIGIN=origin)
        r = subprocess.run(["bash", os.path.join(CORE, "install.sh")], env=ienv, capture_output=True, text=True, timeout=120)
        self.assertEqual(r.returncode, 0, r.stderr)
        link = os.path.join(HOME, ".local", "bin", "taskq")
        self.assertEqual(os.path.realpath(link), os.path.join(dest, "bin", "taskq"))
        v1 = g("rev-parse", "--short", "HEAD").stdout.strip()
        self.assertEqual(taskq("self", "version", exe=link).stdout.strip(), v1)
        self.assertEqual(taskq("path", "core", exe=link).stdout.strip(), dest)
        with open(os.path.join(origin, "NEWS"), "w") as f:
            f.write("x\n")
        g("add", "-A")
        g("commit", "-qm", "v2")
        v2 = g("rev-parse", "--short", "HEAD").stdout.strip()
        r = taskq("self", "update", exe=link, env=env)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn(v2, r.stderr)
        self.assertEqual(taskq("self", "version", exe=link).stdout.strip(), v2)
        r = subprocess.run(["bash", os.path.join(CORE, "install.sh")], env=ienv, capture_output=True, text=True, timeout=120)
        self.assertEqual(r.returncode, 0, "再装一次没关系")
        self.assertEqual(os.path.realpath(link), os.path.join(dest, "bin", "taskq"))


if __name__ == "__main__":
    try:
        unittest.main()
    finally:
        shutil.rmtree(T, ignore_errors=True)
