#!/usr/bin/env python3
"""Tests for relay_bundle.py: taking files, secrets, bundle contents unchanged (nothing generated or rewritten), bundle name,
a full run of taskq.py and site_ops.py from the bundle, and the bundled worker running a few test_worker*.sh cases.
Bundles are built from the worktree (commit=None) into a temp root; config is the test one (testdata/; OUTPUTS["bundle"] is the
dir holding the scheduler code). Commands in the bundle do not get the local TASKQ_CONFIG or TASKQ_PLUGINS: config and plugins
come from the bundle's .taskq.toml, TASKQ_MACHINE says which machine it is and TASKQ_ROOT gives the root.
    python3 queue/test_relay_bundle.py
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduler code
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, HERE)
import cfg  # noqa: E402
REPO = cfg.REPO  # project repo root
QR = cfg.rel("queue")  # queue/ under the repo root (scripts/taskq/queue when nested, queue in a standalone checkout)
BQ = cfg.bundle_rel("queue")  # queue/ of the scheduler code in a bundle (always scripts/taskq/queue)

cfg.use_config()
import proto  # noqa: E402
import relay_bundle as B  # noqa: E402
import spec as S  # noqa: E402

NAME = "site-slurm2"
STEP = f"{BQ}/testdata/bin/step.sh"  # inside the bundle
DROP = ("TASKQ_DATA", "TASKQ_DIR", "TASKQ_MACHINE", "TASKQ_CONFIG", "TASKQ_PLUGINS", "TASKQ_ROOT", "TASKQ_STATE", "TASKQ_DB")


def ok(msg):
    print(f"PASS:{msg}")


def bare_env(**kw):
    """Environment for commands in the bundle: drop local config, queue and data dir (the bundle finds config and plugins via
    its own .taskq.toml)."""
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
    (_, mode, oid, _, repo, _), = B._entries("HEAD", ["queue/testdata/bin/step.sh"], CORE)  # core files come from the core repo
    head = subprocess.run(["git", "-C", repo, "cat-file", "blob", oid], capture_output=True).stdout
    assert head and files[STEP] == (head, mode == "100755")
    ok("take files from a commit: code (including .R) and small data files under OUTPUTS[\"bundle\"], content and executable bit match the commit (.sh is made executable in the bundle), and no papers/ etc")


def test_source_files_submodule(T):
    """Project files come from the project repo commit (other submodules at their pinned commit; the core submodule is
    skipped); core files come from HEAD in the core repo (not the project repo's pinned commit), placed under BUNDLE_CORE/ in
    the bundle. The same core files are taken whether the core is nested in the repo or outside it."""
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
    put(emb, (("q/b.py", "B = 2\n", False),))  # new commit in the core checkout, project repo not repinned: take the core HEAD
    g("commit", "-qam", "c2", cwd=emb)
    put(emb, (("q/a.sh", "echo wd\n", True),))  # uncommitted in the core worktree: read only with commit=None
    put(core, (("q/b.py", "B = 2\n", False),))
    g("commit", "-qam", "c2", cwd=core)
    put(core, (("q/a.sh", "echo wd\n", True),))
    saved = B.REPO, B.CORE, B.TOPS, B.RUNTIME, B.RUNTIME_B
    try:
        B.TOPS, B.RUNTIME = ("cfg", "x", "scripts"), ("q/a.sh", "big.json")
        B.RUNTIME_B = frozenset(f"{BC}/{f}" for f in B.RUNTIME)
        got = {}
        for way, repo, c in (("nested", sup, emb), ("outside", flat, core)):
            B.REPO, B.CORE = repo, c
            got[way] = (B.source_files("HEAD"), B.source_files(None), B.read("HEAD", "x/y.py"), B.core_version("HEAD"))
    finally:
        B.REPO, B.CORE, B.TOPS, B.RUNTIME, B.RUNTIME_B = saved
    files, wd, y, ver = got["nested"]
    assert sorted(files) == ["cfg/m.py", "scripts/queue/worker_start.sh", f"{BC}/big.json", f"{BC}/q/a.sh", f"{BC}/q/b.py",
                             f"{BC}/q/hub_only.py", "x/lib/l.py", "x/y.py"], sorted(files)  # OUTPUTS["bundle"] covers the core: the whole core
    assert files[f"{BC}/q/b.py"] == (b"B = 2\n", False) and files[f"{BC}/q/a.sh"] == (b"echo a\n", True), files
    assert wd[f"{BC}/q/a.sh"] == (b"echo wd\n", True) and sorted(wd) == sorted(files), wd
    assert y == "Y = 3\n" and re.fullmatch(r"[0-9a-f]{7,}", ver)
    files2, wd2, _, _ = got["outside"]
    assert sorted(files2) == ["cfg/m.py", "scripts/queue/worker_start.sh", f"{BC}/big.json", f"{BC}/q/a.sh", "x/lib/l.py", "x/y.py"], \
        sorted(files2)
    assert {k: v for k, v in files.items() if k not in (f"{BC}/q/b.py", f"{BC}/q/hub_only.py")} == files2 and wd2[f"{BC}/q/a.sh"] == (b"echo wd\n", True)
    ok("taking files: project files from the commit (other submodules at their pinned commit; core submodule skipped), core files from the core repo HEAD (not the pinned commit) under "
       f"{BC}/; commit=None reads both worktrees; with the core outside the repo only RUNTIME is taken")


def test_secrets():
    sk = "-----BEGIN OPENSSH " + "PRIVATE KEY-----"
    tok = "ghp_" + "a1" * 18
    assert B.secret_hits({"a.sh": ("x\n" + sk + "\n", False), "b.py": (f"T = '{tok}'", False), "c.py": ("ok = 'ghp_short'", False)}) \
        == [("a.sh", 2, sk[:12]), ("b.py", 1, tok[:12])]
    saved = B.source_files
    B.source_files = lambda *a: dict(saved(*a), **{"scripts/x.sh": (f"echo {tok}".encode(), False)})
    try:
        B.contents(None)
        raise AssertionError("a secret should fail")
    except B.BundleError as e:
        assert "scripts/x.sh:1" in str(e) and tok not in str(e), e
    finally:
        B.source_files = saved
    ok("secrets: private keys and GitHub tokens fail (the error omits the whole secret); short look-alikes do not")


def test_bundle(T):
    R = os.path.join(T, "R")
    bid, path = B.build(None, out=os.path.join(R, "code"))
    assert path == os.path.join(R, "code", bid) and re.fullmatch(r"[0-9a-f]{16}", bid), bid
    bid2, path2 = B.build(None, out=os.path.join(R, "code"))
    assert (bid2, path2) == (bid, path)
    assert not [n for n in os.listdir(os.path.join(R, "code")) if n.startswith(".")]
    ok("build: the name is a 16-char content hash; the same content reuses the bundle and leaves no temp dir")

    files, links = set(), []
    for d, ds, fs in os.walk(path):
        links += [os.path.join(d, x) for x in ds + fs if os.path.islink(os.path.join(d, x))]
        files |= {os.path.relpath(os.path.join(d, f), path) for f in fs}
    want = B.contents(None)
    assert files == set(want) and not links, (sorted(set(want) ^ files)[:20], links)
    src = B.source_files(None)
    for f in B.RUNTIME:
        if os.path.isfile(os.path.join(CORE, f)):
            assert f"{B.BC}/{f}" in files, f"{f} is not in the bundle (not tracked by git?)"
    for f in (f"{BQ}/site_ops.py", f"{BQ}/schema.py", f"{BQ}/worker.sbatch", f"{BQ}/cpu_worker.sbatch", f"{BQ}/worker_start.sh",
              f"{BQ}/testdata/bin/toy_train.sh", STEP, f"{BQ}/testdata/exps/demo/experiments.py", B.VERSION, B.PROJECT_FILE):
        assert f in files, f
    fwd = os.path.join(path, proto.OLD_LAUNCHER)
    assert os.access(fwd, os.X_OK)
    with tempfile.TemporaryDirectory() as d:  # the forwarder runs the real launcher next to it, with the same arguments
        for f, body in ((proto.OLD_LAUNCHER, open(fwd).read()), (f"{B.BC}/queue/worker_start.sh", 'echo "real $*"\n')):
            os.makedirs(os.path.dirname(os.path.join(d, f)), exist_ok=True)
            with open(os.path.join(d, f), "w") as fh:
                fh.write(body)
        r = run(["bash", os.path.join(d, proto.OLD_LAUNCHER), "a b"])
        assert r.stdout == "real a b\n", (r.stdout, r.stderr)
    ok("every bundle has the old launcher path forwarding to the real launcher (workers started from older bundles switch there)")
    for f, (raw, exe) in src.items():
        assert open(os.path.join(path, f), "rb").read() == raw, f"{f} differs from the worktree"
        assert os.access(os.path.join(path, f), os.X_OK) == (exe or f.endswith(".sh")), f
    for f in ("machines.py", "policy.py"):
        assert open(os.path.join(path, B.conf_path(f)), "rb").read() == open(os.path.join(cfg.config_dir(), f), "rb").read(), f
    sha = subprocess.run(["git", "-C", CORE, "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    assert open(os.path.join(path, B.VERSION)).read() == sha + "\n"
    plug = os.path.relpath(os.path.realpath(fixture.PLUGINS), os.path.realpath(REPO))
    toml = open(os.path.join(path, B.PROJECT_FILE)).read()
    assert toml == f'config = "{B.conf_rel()}"\nplugins = ["{plug}"]\n', toml
    ok("bundle contents: worktree code and the config dir (machines.py, policy.py) as is, nothing generated or rewritten, "
       "no symlinks or old entry points; all of RUNTIME is there (including site_ops.py); VERSION is the core commit; .taskq.toml records the config dir and plugin locations in the bundle")
    return bid, path


def test_in_bundle(T, bid, path):
    """taskq.py, mtab.py and site_ops.py in the bundle: only TASKQ_MACHINE, TASKQ_ROOT and the bundle's .taskq.toml."""
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
    ok("mtab.py in the bundle: reads the machine table as packaged, recognizes this machine by TASKQ_MACHINE; a missing field is explained with exit 1")

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
    ok("taskq.py in the bundle: add (recipe toy from the plugin in .taskq.toml) → status --json → next (0; claiming again gives 10) → beat → stop → release → "
       "resume → remove; the queue is under TASKQ_ROOT/queue/")

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
    ok("site_ops.py in the bundle: executor status (the bundle name is this bundle) and states; store done (checks each done item on disk), collect, "
       "smoke, export (reaches the plugin; what is missing is data, not modules)")


def test_scope_and_id(T):
    """The bundle has only the scheduler files executors need (RUNTIME) plus OUTPUTS["bundle"] minus bundle_skip; the name depends
    only on content. Here bundle is replaced with testdata (as the project) and bundle_skip with */bin/*, */test_* and
    run_task.sh from RUNTIME."""
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
    assert f"{BQ}/run_task.sh" in src, "RUNTIME files are not affected by bundle_skip"
    for f in (f"{BQ}/relay.py", f"{BQ}/advance.py", f"{BQ}/relay_transfer.py", f"{BQ}/README.md",
              f"{BQ}/test_relay_bundle.py", STEP, f"{BQ}/testdata/bin/toy_train.sh"):
        assert f not in src, f
    assert not [f for f in src if f.startswith(B.BC + "/") and not f.startswith(f"{BQ}/testdata/") and f not in B.RUNTIME_B], src
    assert f"{QR}/testdata/exps/demo/experiments.py" in src
    ok("bundle contents: all of RUNTIME is there (bundle_skip ignores them); hub-only files (relay.py, advance.py, relay_transfer.py), docs, tests and "
       "bundle_skip matches are not")

    def id_with(change):
        f = dict(files)
        change(f)
        return B.bundle_id(f)

    def edit(path):
        return lambda f: f.__setitem__(path, (f[path][0] + "\n# x\n", f[path][1]))
    assert id_with(lambda f: None) == bid
    assert id_with(edit(f"{BQ}/run_task.sh")) != bid
    assert id_with(edit(f"{QR}/testdata/exps/demo/experiments.py")) != bid
    assert id_with(edit(B.conf_path("machines.py"))) != bid, "config is included as is: a change changes the bundle"
    assert id_with(lambda f: f.__setitem__(f"{BQ}/run_task.sh", (f[f"{BQ}/run_task.sh"][0], False))) != bid
    assert id_with(lambda f: f.__setitem__(B.VERSION, ("0000000\n", False))) == bid, "the core commit is not part of the bundle name"
    ok("bundle name: content only (16 chars); changing run_task.sh used by executors, a project file, config or the executable bit changes it; changing the core commit (VERSION) does not")


SMOKE = {"test_worker.sh": "1", "test_worker_stop.sh": "3 4b", "test_worker_code.sh": "6e"}
KEEP_FIXTURE = ("test_worker_code.sh",)  # 6e builds two more bundles from the core (without .taskq.toml) and finds config and plugins through TASKQ_CONFIG and TASKQ_PLUGINS set by fixture.sh


def checks_in(text, ids):
    """Number of checks in the want <case> blocks of a test file."""
    blocks = dict(re.findall(r"^if want (\S+); then\n(.*?)^fi$", text, re.M | re.S))
    assert set(ids.split()) <= set(blocks), ids
    return sum(len(re.findall(r"^check ", blocks[i], re.M)) for i in ids.split())


def test_worker_in_bundle(T):
    """The bundled worker.sbatch, worker_start.sh, consume.py, run_task.sh and taskq.py run a few test_worker*.sh cases:
    Slurm job invocation, claim, run, release (1), release on a stop signal (3), CPU worker (4b), bundle switch (6e), enough to
    show that config as packaged, .taskq.toml, taskq.py invocation and bundles all work; other cases test worker logic unrelated
    to bundling and run those files separately. Three files run at once."""
    R = os.path.join(T, "R2")
    _, path = B.build(None, out=os.path.join(R, "code"))
    fake = os.path.join(path, os.path.dirname(B.conf_rel()))  # FAKE from the bundle machine table (testdata/)
    shutil.copytree(os.path.join(fixture.FIX, "venv"), os.path.join(fake, "venv"))  # fake venv (bin/activate is not code, so not in the bundle)
    fx ='source "$HERE/testdata/fixture.sh"'
    bare = 'PATH=$(python3 "$HERE/testdata/fixture.py" path "$PATH") || exit 1; export PATH'  # no TASKQ_CONFIG or TASKQ_PLUGINS
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
        got = out.count("PASS:")
        assert p.returncode == 0 and "all passed" in out and not [ln for ln in out.splitlines() if ln.startswith("FAIL:")] and got == want, \
            f"{name}: {got}/{want}\n" + out[-3000:] + err[-2000:]
        total += got
    ok(f"the bundled worker ran a few test_worker*.sh cases ({', '.join(f'{n} {i}' for n, i in SMOKE.items())}): all {total} passed")


def test_prune(T):
    """Bundles on the hub: keep keep (used by executors or held by the gate) and the newest proto.CODE_KEEP; remove half-built
    temp dirs after a day."""
    import time
    out = os.path.join(T, "prune")
    now = time.time()
    names = [f"b{i}" for i in range(proto.CODE_KEEP + 3)]  # b0 is newest
    for i, d in enumerate(names + [".tmp_old", ".tmp_new"]):
        os.makedirs(os.path.join(out, d))
        t = now - 3 * 86400 if d == ".tmp_old" else now - 60 if d == ".tmp_new" else now - 3600 * i
        os.utime(os.path.join(out, d), (t, t))
    gone = B.prune({names[-1]}, out)
    left = set(os.listdir(out))
    assert sorted(gone) == sorted(names[proto.CODE_KEEP:-1]) and left == set(names[:proto.CODE_KEEP]) | {names[-1], ".tmp_new"}, (gone, left)
    ok(f"bundles on the hub: keep ones in use and the newest {proto.CODE_KEEP}, remove the rest; remove half-built temp dirs after a day")


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
        print(f"failed (files in {T})")
        raise
    shutil.rmtree(T)
    print("all passed")


if __name__ == "__main__":
    main()
