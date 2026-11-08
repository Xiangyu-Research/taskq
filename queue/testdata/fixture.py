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


def dead_pid():
    """pid of a finished, reaped child (os.kill(pid, 0) raises ProcessLookupError). Do not use a "probably unused" number such as
    999999: some sandboxes raise PermissionError for pids they cannot see, which POSIX treats as "process exists"."""
    p = subprocess.Popen(["true"])
    p.wait()
    return p.pid


if __name__ == "__main__" and sys.argv[1:2] == ["path"]:  # used by fixture.sh
    print(guard_path(sys.argv[2] if len(sys.argv) > 2 else os.environ.get("PATH", "")))
