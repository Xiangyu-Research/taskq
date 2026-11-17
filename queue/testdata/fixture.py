"""Fixed config for core tests: machine table and policy (config/), recipes and plugins (plugins/), experiment table
(exps/demo/), fake conda and venvs, step scripts (bin/).

    sys.path.insert(0, os.path.join(HERE, "testdata")); import fixture; fixture.use()

Call use() before importing modules that read config (machines, policy, recipes, relay_plugins, spec, experiments...): it sets
TASKQ_CONFIG and TASKQ_PLUGINS here (overriding existing values) and puts noremote/ before the real ssh on PATH (ssh and scp fail
at once, so a test that forgets its fake ssh still cannot reach a real machine; a test's own fake ssh earlier on PATH works as
usual); child processes inherit it. Bash tests source fixture.sh.
"""
import atexit
import os
import shutil
import subprocess
import sys
import tempfile

FIX = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(FIX, "config")
PLUGINS = os.path.join(FIX, "plugins")
BIN = os.path.join(FIX, "bin")
EXPS = os.path.join(FIX, "exps")
NOREMOTE = os.path.join(FIX, "noremote")


def _real_ssh(d):
    """Dir d has a real ssh (an executable binary; a test's fake ssh is a script)."""
    p = os.path.join(d or ".", "ssh")
    try:
        with open(p, "rb") as f:
            return os.access(p, os.X_OK) and f.read(4) == b"\x7fELF"
    except OSError:
        return False


def guard_path(path):
    """PATH -> insert NOREMOTE before the first dir with a real ssh (unchanged when already present or no real ssh)."""
    parts = path.split(os.pathsep) if path else []
    if NOREMOTE in parts:
        return path
    for i, d in enumerate(parts):
        if _real_ssh(d):
            return os.pathsep.join(parts[:i] + [NOREMOTE] + parts[i:])
    return path


def use():
    os.environ["TASKQ_CONFIG"] = CONFIG
    os.environ["TASKQ_PLUGINS"] = PLUGINS
    os.environ["PATH"] = guard_path(os.environ.get("PATH", ""))
    if not os.environ.get("TASKQ_DB"):  # the hub database and plain text logs: tests never use the real ones
        sys.path.insert(0, os.path.dirname(FIX)) if os.path.dirname(FIX) not in sys.path else None
        import proto
        d = tmpdir(prefix="taskq-logs-")
        os.environ["TASKQ_DB"] = os.path.join(d, "taskq.db")
        for n in sorted(set(proto.TEXT_LOGS.values())):
            os.environ.setdefault(n.upper() + "_LOG", os.path.join(d, n + ".log"))
    return CONFIG, PLUGINS


def free_port(lo, hi):
    """A port in [lo, hi) free right now, for a server a test starts later. Pick the range below the kernel's ephemeral range
    (net.ipv4.ip_local_port_range, usually from 32768): bind(0) and outgoing connections never take such a port meanwhile, and
    giving each test file its own range keeps two files running at once from picking the same one."""
    import random
    import socket
    for p in random.sample(range(lo, hi), min(200, hi - lo)):
        s = socket.socket()
        try:
            s.bind(("127.0.0.1", p))
            return p
        except OSError:
            continue
        finally:
            s.close()
    raise RuntimeError(f"no free port in {lo}-{hi}")


def tmpdir(**kw):
    """tempfile.mkdtemp(**kw), removed when the process that made it exits (pass, fail or error); tests that made it unreadable chmod first."""
    d, pid = tempfile.mkdtemp(**kw), os.getpid()

    def rm():
        if os.getpid() == pid:
            subprocess.run(["chmod", "-R", "u+rwx", d], capture_output=True)
            shutil.rmtree(d, ignore_errors=True)
    atexit.register(rm)
    return d


def store_bundle(root, plugins=(), config=None):
    """Hand-built bundle for running store operations (site_ops.py) for real: scripts/taskq in root (the store's expanded root)
    /code/current links to this core, and .taskq.toml points at config (default TASKQ_CONFIG) and PLUGINS plus plugins (hub-only
    variables are stripped when running locally, config is read from the bundle only); a temp conda is built too (bin/python3 is
    the python running the tests). Later calls only rewrite .taskq.toml. -> the conda dir (point the store's conda at it, or
    replace relay_remote's py with lambda name: <it>/bin/python3)."""
    import json
    if os.path.dirname(FIX) not in sys.path:
        sys.path.insert(0, os.path.dirname(FIX))
    import proto
    cur = os.path.join(root, proto.ROOT["current"])
    core = os.path.join(cur, proto.BUNDLE_CORE)
    if not os.path.lexists(core):
        os.makedirs(os.path.dirname(core), exist_ok=True)
        os.symlink(os.path.dirname(os.path.dirname(FIX)), core)
    with open(os.path.join(cur, ".taskq.toml"), "w", encoding="utf-8") as f:
        f.write(f"config = {json.dumps(config or os.environ.get('TASKQ_CONFIG') or CONFIG)}\nplugins = {json.dumps([PLUGINS, *plugins])}\n")
    if root not in _CONDA:
        _CONDA[root] = os.path.join(tmpdir(), "conda")
        os.makedirs(os.path.join(_CONDA[root], "bin"))
        os.symlink(sys.executable, os.path.join(_CONDA[root], "bin", "python3"))
    return _CONDA[root]


_CONDA = {}


# Experiment tables with errors (exp_tables(BAD_TABLES)): one that cannot be imported, and one whose entries each have a different
# problem next to a fine one: xa's first step names {frg} in cmd (xa2 comes after it), xb is fine, xc's output names {modl} (xd reads
# that output), xe has an unknown field, two experiments have the id xg, xh and xi make the same task name, FINAL step xfin has no cmd.
BAD_TABLES = dict(broken="EXPERIMENTS = [\n", extra='''
def st(name, out, inputs, cmd="true", **kw):
    return dict(name=name, env="base", device="cpu", cmd=cmd, out=out, done="ok.txt", inputs=inputs, **kw)


CK = "results/{model}/frag_{frag}/final.ckpt"
EXPERIMENTS = [
    dict(id="xa", recipe="toy", params=dict(ds="X1"), each=dict(frag=["0"]),
         after=[st("xa1", "x/{model}/a1", [CK], cmd="run {frg}"), st("xa2", "x/{model}/a2", ["x/{model}/a1"])]),
    dict(id="xb", recipe="toy", params=dict(ds="X2"), each=dict(frag=["0"]), after=[st("xb1", "x/{model}/b1", [CK])]),
    dict(id="xc", recipe="toy", params=dict(ds="X3"), each=dict(frag=["0"]), after=[st("xc1", "x/{modl}/c1", [CK])]),
    dict(id="xd", recipe="toy", params=dict(ds="X4"), each=dict(frag=["0"]), after=[st("xd1", "x/{model}/d1", ["x/X3/V_save__PlanM__full/c1"])]),
    dict(id="xe", recipe="toy", params=dict(ds="X5", frag="0"), nicee=1),
    dict(id="xg", recipe="toy", params=dict(ds="X6", frag="0")),
    dict(id="xg", recipe="toy", params=dict(ds="X7", frag="0")),
    dict(id="xh", recipe="toy", params=dict(ds="X8", frag="0")),
    dict(id="xi", recipe="toy", params=dict(ds="X8", frag="0")),
]
FINAL = [dict(name="xfin", env="base", out="x/fin", done="ok.txt")]
''')


def exp_tables(more):
    """The test experiment table (exps/demo, linked) plus more tables {dir name: module source} in a temp dir -> the glob that
    reads them all (absolute, so OUTPUTS["experiments"] can be it)."""
    d = tmpdir(prefix="exps-")
    for name, src in dict(more, demo=None).items():
        os.makedirs(os.path.join(d, name))
        p = os.path.join(d, name, "experiments.py")
        if src is None:
            os.symlink(os.path.join(EXPS, "demo", "experiments.py"), p)
        else:
            with open(p, "w", encoding="utf-8") as f:
                f.write(src)
    return os.path.join(d, "*", "experiments.py")


def read_tables(pat):
    """In this process, experiments.py reads the experiment tables matching pat from now on (its cache cleared); pat None -> the
    test table again."""
    import cfg
    import experiments
    if not hasattr(cfg, "_fixture_outputs"):
        cfg._fixture_outputs = cfg.outputs
    real = cfg._fixture_outputs
    cfg.outputs = real if pat is None else (lambda key: pat if key == "experiments" else real(key))
    experiments._cache.clear()


def config_reading(pat):
    """A copy of config/ whose policy.py reads the experiment tables matching pat (for commands run as child processes)."""
    d = os.path.join(tmpdir(prefix="config-"), "config")
    shutil.copytree(CONFIG, d)
    p = os.path.join(d, "policy.py")
    with open(p, encoding="utf-8") as f:
        text = f.read()
    old = 'experiments=QR + "/testdata/exps/*/experiments.py",'
    assert old in text, "config/policy.py no longer names the test experiment table this way"
    with open(p, "w", encoding="utf-8") as f:
        f.write(text.replace(old, f"experiments={pat!r},"))
    return d


def dead_pid():
    """pid of a finished, reaped child (os.kill(pid, 0) raises ProcessLookupError). Do not use a "probably unused" number such as
    999999: some sandboxes raise PermissionError for pids they cannot see, which POSIX treats as "process exists"."""
    p = subprocess.Popen(["true"])
    p.wait()
    return p.pid


if __name__ == "__main__" and sys.argv[1:2] == ["path"]:  # used by fixture.sh
    print(guard_path(sys.argv[2] if len(sys.argv) > 2 else os.environ.get("PATH", "")))
