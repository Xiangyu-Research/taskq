#!/usr/bin/env python3
"""Tests sites/envs.py and spec-built envs in the machine table: specs load, activation, no sending while .ok does not
match, build commands run on the CPU only; finally a real venv with only numpy is built in a temp dir.

    python3 sites/test_envs.py [--real]
"""
import copy
import glob
import hashlib
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of this scheduler code
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(CORE, "queue"))
sys.path.insert(0, os.path.join(CORE, "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
import proto  # noqa: E402
import envs as E  # noqa: E402
M = E.M
import refill  # noqa: E402

fails = 0


def ok(what, cond, info=None):
    global fails
    print(("PASS:" if cond else "FAIL:") + what + ("" if cond or info is None else f"\n   {info}"))
    fails += 0 if cond else 1


# ---------- specs ----------
names = sorted(os.path.basename(p)[:-4] for p in glob.glob(os.path.join(M.SPECS, "*.yml")))
ok("specs for base, alt, alt2 and stats", {"base", "alt", "alt2", "stats"} <= set(names))
for n in names:
    try:
        sp = E.load(n)
        E.check_file(n)
        good = True
    except E.EnvError as e:
        print("  ", e)
        good = False
    ok(f"{n}.yml loads and has a check", good)
    ok(f"{n}.check.py compiles", bool(compile(open(E.check_file(n), encoding="utf-8").read(), n, "exec")))
ok("alt builds on base", E.load("alt")["base"] == "base")
ok("base has a cu128 variant", "cu128" in E.load("base")["torch"])
ok("no lock yet: the hash = sha256 of the spec (a lock is added when present, see test_envlock.py)",
   not os.path.exists(M.lock_path("base"))
   and M.spec_hash("base") == hashlib.sha256(open(os.path.join(M.SPECS, "base.yml"), "rb").read()).hexdigest())
ok("no such spec → hash None", M.spec_hash("no-such-spec") is None)

# ---------- fake machines ----------
SAVE_M = copy.deepcopy(M.MACHINES)
M.MACHINES["tsite"] = dict(label="test", hostname=r"tsite-host", ssh="tsite", scheduler="pueue", root="/r/x1",
                           conda=("/opt/c", "base"), envs={"base": ("spec", "cu128"), "alt": "spec", "old": ("conda", "o")},
                           workers=[dict(gpu="0")])
M.MACHINES["tslurm"] = dict(label="test slurm", hostname=r"tslurm-host", ssh="tslurm", scheduler="slurm", root="/s/y",
                            conda=("/app/c", "pt"), envs={"base": "spec"}, jobs=dict(gpu=dict(partition="gpu-x")),
                            transfer_job="-p cpu-x --qos=normal -c 4 --mem=4G")

a = M.activate("tsite", "alt")
ok("a spec env activates through .activate under the root", "source /r/x1/envs/alt/.activate" in a)
ok("a conda entry works as before", "conda activate o" in M.activate("tsite", "old"))
M.MACHINES["tsite"]["conda"] = ("/opt/c", "w0")
ok("worker env not in envs → a conda env of that install",
   M.worker_activate("tsite") == "source /opt/c/etc/profile.d/conda.sh\nconda activate w0")
M.MACHINES["tsite"]["conda"] = ("/opt/c", "alt")
ok("worker env named in envs → activated as for tasks (spec: its .activate, no path to write)",
   M.worker_activate("tsite") == M.activate("tsite", "alt"))
M.MACHINES["tsite"]["conda"] = ("/opt/c", "base")
ok("without .ok a spec env is not ready, a conda one is", M.ready_envs("tsite") == {"old"})
h = M.spec_hash("base")
ok("ready only when .ok matches", M.ready_envs("tsite", {"base": h}) == {"old", "base"})
ok(".ok from an old spec → not ready", M.ready_envs("tsite", {"base": "0" * 64}) == {"old"})
ok("all envs of the existing machine entries are still ready (not turned into specs)",
   all(M.ready_envs(n) == set(M.machine(n).get("envs") or {}) for n in SAVE_M))

# ---------- sending gate (refill.choose: this machine's Site.envs = ready envs) ----------
TASKS = [dict(key=k, nice=0, order=i) for i, k in enumerate("abc")]
SPECS = {k: (None, dict(env=e, device="gpu")) for k, e in zip("abc", ("base", "old", "alt"))}


def got(oks):
    site = refill.Site("tsite", {"gpu": 9}, envs=M.ready_envs("tsite", oks), root=(1, 1000.0))
    refill.choose(TASKS, {"tsite": site}, SPECS)
    return sorted(t["key"] for t in site.take)


ok("no .ok: only tasks for the conda env are sent", got(None) == ["b"])
ok("base .ok matches: base tasks are sent too", got({"base": h}) == ["a", "b"])
ok("alt .ok is old: not sent", got({"base": h, "alt": "x"}) == ["a", "b"])

# ---------- build script and command ----------
s_ = E.build_script("tsite", "alt")
ok("venv: activate the base env first (its .activate)", "source /r/x1/envs/base/.activate" in s_)
ok("venv: --system-site-packages", "--system-site-packages" in s_)
M.MACHINES["tsite"]["envs"]["alt3"] = "spec"
s_ = E.build_script("tsite", "alt3")
del M.MACHINES["tsite"]["envs"]["alt3"]
ok("placeholders become variables computed from the env", "${SPEC_TORCH_MM}" in s_ and "{torch_mm}" not in s_)
ok("the GPUs are not visible", "export CUDA_VISIBLE_DEVICES=" in s_)
ok("write .ok and switch the symlink only after the check", s_.index("CHECK_EOF") < s_.index('mv -T') < s_.index('.ok"'))
s_ = E.build_script("tsite", "base")
ok("conda: install torch per variant (cu128)", "whl/cu128" in s_ and "torch==2.7.1" in s_)
ok("conda: built as <name>-<hash> under the root", 'P="$ROOT/$N-${H:0:12}"' in s_ and "ROOT=/r/x1/envs" in s_)
ok("the check gets the code dir", "python3 - /r/x1/code/current" in s_)
cmd, host = E.runner("tsite")
ok("pueue kind: nice, no GPUs visible, runs on that machine", host == "tsite" and "nice -n 10 env CUDA_VISIBLE_DEVICES=" in cmd and "srun" not in cmd)
cmd, host = E.runner("tslurm")
ok("Slurm: srun in the CPU partition, queue and wait (no -I)", cmd.count("srun -p cpu-x") == 1 and "gres" not in cmd and " -I" not in cmd)
M.MACHINES["tslurm"]["env_job"] = "-p gpu-a --gres=gpu:1"
try:
    E.runner("tslurm")
    ok("env_job asks for a GPU → error", False)
except E.EnvError:
    ok("env_job asks for a GPU → error", True)
try:
    E.build_script("tsite", "old")
    ok("an env not built from a spec → error", False)
except E.EnvError:
    ok("an env not built from a spec → error", True)
M.MACHINES["tsite"]["envs"]["alt"] = "spec"
ok("dry run runs no command", E.build("tsite", "alt", dry=True,
                              run=lambda *a, **k: (_ for _ in ()).throw(AssertionError("ran")))[0] == 0)
ok("the generated script passes bash -n", all(subprocess.run(["bash", "-n"], input=E.build_script("tsite", n), text=True).returncode == 0
                                for n in ("base", "alt")))

# ---------- states ----------
def fake(txt):
    return lambda cmd, host, timeout, input=None: (0, txt, "")


now = 2_000_000_000
st = E.states("tsite", run=fake(f"ENV|base|{h}|||1\nENV|alt||||0\nNOW|{now}\n"))
ok("states: ok, not built, a conda one is not spec-built", st == {"old": "not spec-built", "base": "ok", "alt": "not built"})
uh = M.spec_hash("alt")
st = E.states("tsite", run=fake(f"ENV|base|{'0'*64}|||1\nENV|alt||{uh}||0\nNOW|{now}\n"))
ok("states: spec changed, check failed", st["base"] == "spec changed" and st["alt"] == "check failed")
st = E.states("tsite", run=fake(f"ENV|base|||{h} {now - 60}|0\nENV|alt|||{uh} {now - 7 * 3600}|0\nNOW|{now}\n"))
ok("states: building, building for a long time", st["base"] == "building" and st["alt"] == "building (may have stopped)")
ok("unreachable → None", E.states("tsite", run=lambda *a, **k: (255, "", "")) is None)
ok("oks_of keeps only ok envs (hash by the variant used here)", E.oks_of({"base": "ok", "alt": "not built"}, "tsite") == {"base": M.spec_hash("base", "cu128")})

M.MACHINES.clear()
M.MACHINES.update(SAVE_M)

# ---------- lock files: offline builds ----------
import base64  # noqa: E402
import json  # noqa: E402
import zipfile  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_transfer as RT  # noqa: E402


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def tiny_wheel(d, name="tinypkg", ver="0.1"):
    """A minimal installable wheel (pure Python) → path."""
    fn = f"{d}/{name}-{ver}-py3-none-any.whl"
    di = f"{name}-{ver}.dist-info"
    files = {f"{name}/__init__.py": f"VERSION = {ver!r}\n",
             f"{di}/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: {ver}\n",
             f"{di}/WHEEL": "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n"}
    rec = [f"{k},sha256={base64.urlsafe_b64encode(hashlib.sha256(v.encode()).digest()).rstrip(b'=').decode()},{len(v.encode())}"
           for k, v in files.items()] + [f"{di}/RECORD,,"]
    with zipfile.ZipFile(fn, "w") as z:
        for k, v in files.items():
            z.writestr(k, v)
        z.writestr(f"{di}/RECORD", "\n".join(rec) + "\n")
    return fn


def put(wh, p):
    """Put a file into the wheelhouse (<first two>/<sha256>) → the common fields of a lock entry."""
    h = sha(p)
    os.makedirs(f"{wh}/{h[:2]}", exist_ok=True)
    shutil.copy(p, f"{wh}/{h[:2]}/{h}")
    return dict(fn=os.path.basename(p), size=os.path.getsize(p), sha256=h, url="file://" + p)


def write_lock(specs, env, v, conda=(), pip=(), base=None):
    lk = dict(format=1, name=env, variant=v, spec_sha256=sha(f"{specs}/{env}.yml"), platform={"subdir": "linux-64"}, base=base,
              conda=list(conda), pip=list(pip))
    json.dump(lk, open(f"{specs}/{env}.{v}.lock.json", "w"))
    return lk


LD = fixture.tmpdir(prefix="envlock.", dir=os.environ.get("TMPDIR"))
SAVE_SPECS, SAVE_BA, SAVE_ST = M.SPECS, E.base_activation, os.environ.get("TASKQ_STATE")
os.makedirs(f"{LD}/specs")
for n in ("cbase", "tiny"):
    open(f"{LD}/specs/{n}.check.py", "w").write("import sys\nprint('ok', sys.prefix)\n")
open(f"{LD}/specs/cbase.yml", "w").write("python: '3.12'\nchannels: [conda-forge]\n")
open(f"{LD}/specs/tiny.yml", "w").write("base: sysbase\npip:\n  - tinypkg\n")
M.SPECS = f"{LD}/specs"
os.environ["TASKQ_STATE"] = f"{LD}/state"
WH = f"{LD}/state/{proto.WHEELHOUSE}"
whl = tiny_wheel(fixture.tmpdir(prefix="whl.", dir=LD))
ok("wheel file name: the local version becomes the metadata version, nothing else changes", E.wheel_name(dict(fn="m_s-2.3.0+cu12torch2.5-cp311-cp311-linux_x86_64.whl", version="2.3.0"))
   == "m_s-2.3.0-cp311-cp311-linux_x86_64.whl" and E.wheel_name(dict(fn="a-1.0-py3-none-any.whl", version="1.0")) == "a-1.0-py3-none-any.whl"
   and E.wheel_name(dict(fn="x-1.0.conda", version="1.0")) == "x-1.0.conda")
pkg = f"{LD}/fake-1.0-0.conda"
open(pkg, "wb").write(b"not really a conda package")
cpk = dict(put(WH, pkg), name="fake", version="1.0", build="0", channel="https://x/linux-64", subdir="linux-64",
           md5=hashlib.md5(open(pkg, "rb").read()).hexdigest())
wpk = dict(put(WH, whl), name="tinypkg", version="0.1", no_deps=True)
write_lock(M.SPECS, "cbase", "default", conda=[cpk])
write_lock(M.SPECS, "tiny", "default", pip=[wpk])
M.MACHINES["lsite"] = dict(label="lock test", hostname=r"lsite-host", ssh="lsite", scheduler="pueue", root="/r/l", cap=50,
                           conda=("/opt/c", "base"), envs={"cbase": "spec", "tiny": "spec", "sysbase": ("conda", "s")},
                           workers=[dict(cpus=4)], ssh_master=True)
s_ = E.build_script("lsite", "cbase", store="/r/l/blobs")
ok("lock: conda builds offline (--offline, explicit file, temp package dir)", 'conda create --offline -y -p "$P" --file "$T/explicit.txt"' in s_
   and 'CONDA_PKGS_DIRS="$T/pkgs"' in s_ and "CONDA_OFFLINE=1" in s_ and "conda-forge" not in s_)
ok("lock: each package is hard-linked from blobs by sha256 under its original name, with md5 in explicit",
   f"pkgs {cpk['sha256']} {cpk['fn']} {cpk['md5']}" in s_ and "BL=/r/l/blobs" in s_
   and 'file://$T/pkgs/$fn#$md5' in s_ and 'ln -f "$s"' in s_)
s_ = E.build_script("lsite", "tiny", store="/r/l/blobs")
ok("lock: pip installs from local files only, by hash, without deps", "--no-index --no-deps --require-hashes" in s_ and '--find-links "$T/wheels"' in s_
   and f"tinypkg==0.1 --hash=sha256:{wpk['sha256']}" in s_ and "pip install --index-url" not in s_)
ok("lock: the temp dir is removed in the trap, .built is written before the symlink switch", 'rm -rf "$T"' in s_ and s_.index("CHECK_EOF") < s_.index('> "$P/.built"') < s_.index("mv -T"))
ok("lock: passes bash -n", all(subprocess.run(["bash", "-n"], input=E.build_script("lsite", n, store="~/q/blobs"), text=True).returncode == 0
                       for n in ("cbase", "tiny")))
try:
    E.build_script("lsite", "cbase")
    ok("lock: no package dir given → error", False)
except E.EnvError:
    ok("lock: no package dir given → error", True)
ok("lock: space needed = packages x 2.5 + one temp copy", E.lock_need(E.load_lock("tiny", "default")) == int(wpk["size"] * 3.5))
ok("lock: paths in the wheelhouse", E.lock_files(E.load_lock("tiny", "default")) == {wpk["sha256"]: f"{WH}/{wpk['sha256'][:2]}/{wpk['sha256']}"})
json.dump(dict(format=1, name="tiny", variant="default", conda=[], pip=[dict(wpk, fn="../x")]), open(f"{M.SPECS}/tiny.v2.lock.json", "w"))
try:
    E.load_lock("tiny", "v2")
    ok("lock: a path in fn → error", False)
except E.EnvError:
    ok("lock: a path in fn → error", True)
os.remove(f"{M.SPECS}/tiny.v2.lock.json")
ok("no lock for that variant → None (builds from the network as before)", E.load_lock("tiny", "nolock") is None)

# prebuild: entries still conda / venv can be built too, and a venv base uses the spec-built one
M.MACHINES["lsite"]["envs"]["tiny"] = ("venv", "/old/tiny", "sysbase")
open(f"{LD}/specs/sysbase.yml", "w").write("python: '3.12'\n")
open(f"{LD}/specs/sysbase.check.py", "w").write("print('ok')\n")
s_ = E.build_script("lsite", "tiny", store="/r/l/blobs")
ok("prebuild: a venv entry can be built, with the spec-built base under env_root", "source /r/l/envs/sysbase/.activate" in s_)
ok("prebuild: the base env comes first", list(E.wanted("lsite", ["tiny"])) == ["cbase", "sysbase", "tiny"])
os.remove(f"{LD}/specs/sysbase.yml")
ok("the base env has no spec: only it is built", list(E.wanted("lsite", ["tiny"])) == ["cbase", "tiny"])
M.MACHINES["lsite"]["envs"]["tiny"] = "spec"
ok("every spec entry is built, with its variant from the entry", E.wanted("lsite") == {"cbase": "default", "tiny": "default"})

# states: the two forms of .failed
hc, ht = E.vhash("cbase", "default"), E.vhash("tiny", "default")
now = 2_000_000_000
W = {"cbase": "default", "tiny": "default"}
rows = {"cbase": dict(failed=f"{hc} {now - 60} rc 1", log="a\nb\nbroken"), "tiny": dict(failed=f"{ht} {now - 60} nospace 900 100")}
st = E.classify(rows, W, now)
ok("states: rc → check failed, nospace → no space", st == {"cbase": "check failed", "tiny": "no space"})
ok("next: no rebuild after a failed check, no retry for lack of space before 6 hours", E.next_build(W, st, rows, now) is None)
ok("next: a no-space build is retried after 6 hours", E.next_build(W, st, rows, now + 7 * 3600) == ("tiny", "default"))
ok("next: nothing while one is building", E.next_build(W, {"cbase": "building", "tiny": "not built"}, {}, now) is None)
ok("next: a stopped build is rebuilt", E.next_build(W, {"cbase": "building (may have stopped)", "tiny": "ok"}, {}, now) == ("cbase", "default"))
pr = dict(E.problems("lsite", W, st, rows))
ok("report: a failed check includes the last log lines, no space includes need and free", "broken" in pr["cbase"] and "0.0 GB" in pr["tiny"] and "no space" in pr["tiny"])
os.rename(f"{WH}/{wpk['sha256'][:2]}/{wpk['sha256']}", f"{LD}/moved")
pr = E.problems("lsite", {"tiny": "default"}, {"tiny": "not built"}, {})
ok("report: the wheelhouse is missing packages", len(pr) == 1 and "wheelhouse" in pr[0][1])
os.rename(f"{LD}/moved", f"{WH}/{wpk['sha256'][:2]}/{wpk['sha256']}")

# build(): the executor checks the space first, pins, the hub sends the wheelhouse packages into blobs, then it runs; no run without space; ssh master down means no connection
calls = []


class Pin:
    def __init__(self, name, key, hashes, say=print):
        calls.append(("pin", key, sorted(hashes)))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        calls.append(("unpin",))


SAVE_RT, SAVE_MD = (RT.pinned, RT.ensure_blobs), RM.master_down
RT.pinned = Pin
RT.ensure_blobs = lambda name, files, say=print, **kw: calls.append(("blobs", files, kw.get("src"))) or {h: f"/r/l/blobs/{h[:2]}/{h}" for h in files}
free = [10 ** 12]


def run_op(name, what, data):
    calls.append(("mark", data["mode"], data["need"], data["cap"], what))
    return dict(ok=free[0] >= data["need"], free=free[0], need=data["need"])


def run_cmd(cmd, host, timeout, input=None):
    calls.append(("run", host))
    return 0, "built", ""


RM.master_down = lambda m: False
rc, out = E.build("lsite", "tiny", run=run_cmd, run_op=run_op, say=lambda *_: None)
ok("executor: check space (site_ops env_mark, by cap) → pin → put into blobs → run → unpin",
   rc == 0 and [c[0] for c in calls] == ["mark", "pin", "blobs", "run", "unpin"]
   and calls[0][3] == 50 and calls[0][4] == "env_mark" and calls[1][2] == [wpk["sha256"]], calls)
ok("executor: packages come from the hub's wheelhouse (src = hub, {sha256: [None, bytes, wheelhouse path]})",
   calls[2][2] == M.hub() and calls[2][1] == {wpk["sha256"]: [None, wpk["size"], f"{WH}/{wpk['sha256'][:2]}/{wpk['sha256']}"]}, calls)
calls.clear()
free[0] = 10
rc, out = E.build("lsite", "tiny", run=run_cmd, run_op=run_op, say=lambda *_: None)
ok("executor: no space → no transfer, no run, exit 1", rc == 1 and [c[0] for c in calls] == ["mark"] and "no space" in out, (calls, out))
calls.clear()
free[0] = 10 ** 12


def noslot(*a, **k):
    raise RM.NoSlot("no slot")


RT.ensure_blobs = noslot
rc, out = E.build("lsite", "tiny", run=run_cmd, run_op=run_op, say=lambda *_: None)
ok("executor: no CPU job slot → stage_later, .building removed, no run", rc == proto.EXIT["stage_later"]
   and [c[:2] for c in calls] == [("mark", "reserve"), ("pin", "env.tiny"), ("unpin",), ("mark", "clear")], calls)
calls.clear()
RM.master_down = lambda m: True
try:
    E.build("lsite", "tiny", run=run_cmd, run_op=run_op)
    ok("ssh master down → error, no connection", False)
except E.EnvError:
    ok("ssh master down → error, no connection", calls == [])
RM.master_down = SAVE_MD
RT.pinned, RT.ensure_blobs = SAVE_RT
M.MACHINES["lremote"] = dict(label="remote", hostname=r"lr-host", ssh="lr", conda=None, envs={"tiny": "spec"})
try:
    E.build("lremote", "tiny", run=run_cmd)
    ok("a locked env is not built on a remote machine that is neither hub nor executor", False)
except E.EnvError:
    ok("a locked env is not built on a remote machine that is neither hub nor executor", True)
del M.MACHINES["lremote"]
del M.MACHINES["lsite"]

# A real build on this machine as executor (offline): the venv builds on an existing Python, pip packages go from the
# hub's wheelhouse into this machine's blobs (same machine as the hub: hard links). The site_ops operations (env_mark,
# pin, missing, ingest, push on the hub) run in this process, with TASKQ_ROOT / TASKQ_MACHINE as relay_remote.op prefixes
# (the local entry exists only in this process's machine table, not in the config the child reads)
import site_ops as SO  # noqa: E402
ops_run = []


def local_op(name, what, data=None, timeout=600, job=None, say=None):
    ops_run.append((name, what))
    keep = {k: os.environ.get(k) for k in ("TASKQ_ROOT", "TASKQ_MACHINE")}
    os.environ.update(TASKQ_ROOT=M.machine(name)["root"], TASKQ_MACHINE=name)
    try:
        return json.loads(json.dumps(SO.OPS[what](json.loads(json.dumps(data)))))
    except SO.Bad as e:
        raise RuntimeError(f"{what} failed on {name}: {e}") from None
    finally:
        for k, v in keep.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


SAVE_OP, SAVE_HUBROOT = RM.op, M.MACHINES[M.hub()]["root"]
RM.op = local_op
M.MACHINES[M.hub()]["root"] = f"{LD}/hubroot"  # the temp dir the hub uses for push lives under its root: never touch the real home
pyd = next(p for p in (os.path.expanduser("~/miniforge3/bin"), os.path.dirname(sys.executable))
           if subprocess.run([f"{p}/python3", "-c", "import venv, ensurepip"], capture_output=True).returncode == 0)
E.base_activation = lambda name, base: f'export PATH={pyd}:"$PATH"'
open(f"{LD}/specs/tiny.check.py", "w").write("import tinypkg, sys\nprint('tinypkg', tinypkg.VERSION, sys.prefix)\n")
M.MACHINES["toff"] = dict(label="local", hostname=r"toff-host", ssh=None, scheduler="pueue", root=f"{LD}/root", conda=None,
                          envs={"sysbase": ("conda", "unused"), "tiny": "spec"}, workers=[dict(cpus=2)])
PX = {k: os.environ.get(k) for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "PIP_INDEX_URL")}
os.environ.update(http_proxy="http://127.0.0.1:9", https_proxy="http://127.0.0.1:9", HTTP_PROXY="http://127.0.0.1:9",
                  HTTPS_PROXY="http://127.0.0.1:9", PIP_INDEX_URL="http://127.0.0.1:9/simple")
rc, out = E.build("toff", "tiny")
ok("offline build on this machine: exit 0", rc == 0, out)
ok("a local executor takes the executor path: env_mark, pin, missing here, push on the hub, unpin after the build",
   [w for n, w in ops_run if n == "toff"][:3] == ["env_mark", "pin", "missing"] and ("hub", "push") in ops_run
   and ops_run[-1] == ("toff", "pin") and not os.listdir(f"{LD}/root/{proto.ROOT['pins']}"), ops_run)
ok("local executor: packages are hard-linked into this machine's blobs", os.path.samefile(f"{LD}/root/{proto.blob(wpk['sha256'])}",
                                                      f"{WH}/{wpk['sha256'][:2]}/{wpk['sha256']}"))
okf = f"{LD}/root/state/envs/tiny.ok"
ok("offline build on this machine: .ok holds the hash, state ok, no temp dir left", os.path.isfile(okf) and open(okf).read().strip() == E.vhash("tiny", "default")
   and E.states("toff")["tiny"] == "ok" and not glob.glob(f"{LD}/root/envs/.tmp-*"))
r = subprocess.run(["bash", "-c", M.activate("toff", "tiny") + "\npython -c 'import tinypkg; print(tinypkg.__file__)'"],
                   capture_output=True, text=True)
ok("offline build on this machine: the wheelhouse package imports after activation", r.returncode == 0 and f"{LD}/root/envs/tiny-" in r.stdout, r.stdout + r.stderr)
hb = "0" * 64  # content does not match the hash in the lock
os.makedirs(f"{WH}/00", exist_ok=True)
shutil.copy(whl, f"{WH}/00/{hb}")
open(f"{LD}/specs/tiny.yml", "a").write("# changed\n")
write_lock(M.SPECS, "tiny", "default", pip=[dict(wpk, sha256=hb)])
rc, out = E.build("toff", "tiny")
st = E.states("toff")["tiny"]
ok("package hash mismatch → pip installs nothing, check failed, the old env stays in use", rc != 0 and st == "check failed" and os.path.isfile(f"{LD}/root/envs/tiny/.activate"),
   (rc, st, out))
for k, v in PX.items():
    if v is None:
        os.environ.pop(k, None)
    else:
        os.environ[k] = v
del M.MACHINES["toff"]
M.SPECS, E.base_activation = SAVE_SPECS, SAVE_BA
if SAVE_ST is None:
    os.environ.pop("TASKQ_STATE", None)
else:
    os.environ["TASKQ_STATE"] = SAVE_ST

# ---------- a real small build (--real: temp dir, numpy only) ----------
if "--real" in sys.argv:
    d = fixture.tmpdir(prefix="envtest.", dir=os.environ.get("TMPDIR"))
    os.makedirs(f"{d}/specs")
    open(f"{d}/specs/tiny.yml", "w").write("# test\nbase: sysbase\npip:\n  - numpy\n")
    open(f"{d}/specs/tiny.check.py", "w").write("import numpy\nprint('numpy', numpy.__version__)\n")
    M.SPECS = f"{d}/specs"
    M.MACHINES["treal"] = dict(label="local test", hostname=r"treal-host", ssh=None, scheduler="pueue", root=f"{d}/root",
                               conda=None, envs={"sysbase": ("conda", "unused"), "tiny": "spec"}, workers=[dict(cpus=2)])
    # base env: a local python with venv and without numpy (the hub uses miniforge's base)
    pyd = next(p for p in (os.path.expanduser("~/miniforge3/bin"), os.path.dirname(sys.executable))
               if subprocess.run([f"{p}/python3", "-c", "import venv, ensurepip"], capture_output=True).returncode == 0)
    E.base_activation = lambda name, base: f'export PATH={pyd}:"$PATH"'
    rc, out = E.build("treal", "tiny")
    print(out)
    ok("real build: exit 0", rc == 0)
    okf = f"{d}/root/state/envs/tiny.ok"
    ok("real build: .ok written with the spec hash", os.path.isfile(okf) and open(okf).read().strip() == M.spec_hash("tiny"))
    ok("real build: the symlink and .activate are there", os.path.isfile(f"{d}/root/envs/tiny/.activate"))
    r = subprocess.run(["bash", "-c", M.activate("treal", "tiny") + "\npython -c 'import numpy; print(numpy.__file__)'"],
                       capture_output=True, text=True)
    ok("real build: numpy imports after activation, from the new env", r.returncode == 0 and f"{d}/root/envs/tiny-" in r.stdout)
    ok("real build: state ok", E.states("treal") == {"sysbase": "not spec-built", "tiny": "ok"})
    open(f"{d}/specs/tiny.yml", "a").write("# changed\n")
    ok("real build: after a spec change → spec changed, not ready", E.states("treal")["tiny"] == "spec changed"
       and "tiny" not in M.ready_envs("treal", E.oks_of(E.states("treal"), "treal")))
    open(f"{d}/specs/tiny.check.py", "w").write("import sys\nsys.exit('fails on purpose')\n")
    rc, out = E.build("treal", "tiny")
    st = E.states("treal")["tiny"]
    ok("real build: failed check → nonzero exit, state check failed, the old .ok unchanged", rc != 0 and st == "check failed"
       and open(okf).read().strip() != M.spec_hash("tiny"))
    shutil.rmtree(d)

    # conda + pip both from the lock, offline: first fetch the packages conda resolved from conda-forge into the wheelhouse, then build without a network
    import urllib.request
    cd_ = os.path.expanduser("~/miniforge3")
    d = fixture.tmpdir(prefix="envtestc.", dir=os.environ.get("TMPDIR"))
    os.makedirs(f"{d}/specs")
    open(f"{d}/specs/creal.yml", "w").write("python: '3.10.12'\nchannels: [conda-forge]\npip:\n  - tinypkg\n")
    open(f"{d}/specs/creal.check.py", "w").write("import sys, tinypkg\nassert sys.version.startswith('3.10.12'), sys.version\nprint('ok', tinypkg.VERSION)\n")
    M.SPECS = f"{d}/specs"
    os.environ["TASKQ_STATE"] = f"{d}/state"
    wh = f"{d}/state/{proto.WHEELHOUSE}"
    js = json.loads(subprocess.run([f"{cd_}/bin/conda", "create", "--dry-run", "--json", "-p", f"{d}/solve", "-c", "conda-forge",
                                    "--override-channels", "python=3.10.12"], capture_output=True, text=True, timeout=600).stdout)
    dl = fixture.tmpdir(prefix="dl.", dir=d)
    cpks = []
    for x in js["actions"]["LINK"]:
        fn = x["dist_name"] + ".conda"
        url = f"{x['base_url']}/{x['platform']}/{fn}"
        urllib.request.urlretrieve(url, f"{dl}/{fn}")
        cpks.append(dict(put(wh, f"{dl}/{fn}"), name=x["name"], version=x["version"], build=x["build_string"], url=url,
                         channel=x["base_url"], subdir=x["platform"], md5=hashlib.md5(open(f"{dl}/{fn}", "rb").read()).hexdigest()))
    w2 = tiny_wheel(fixture.tmpdir(prefix="whl.", dir=d))
    w2l = w2.replace("tinypkg-0.1-", "tinypkg-0.1+local-")  # the file name has a local version, the metadata does not (like the mamba_ssm wheel)
    os.rename(w2, w2l)
    w2 = w2l
    write_lock(M.SPECS, "creal", "default", conda=cpks, pip=[dict(put(wh, w2), name="tinypkg", version="0.1", no_deps=True)])
    shutil.rmtree(dl)
    M.MACHINES["creal"] = dict(label="local", hostname=r"creal-host", ssh=None, scheduler="pueue", root=f"{d}/root", conda=(cd_, "base"),
                               envs={"creal": "spec"}, workers=[dict(cpus=2)])
    os.environ.update(http_proxy="http://127.0.0.1:9", https_proxy="http://127.0.0.1:9", HTTP_PROXY="http://127.0.0.1:9",
                      HTTPS_PROXY="http://127.0.0.1:9", PIP_INDEX_URL="http://127.0.0.1:9/simple", CONDA_OFFLINE="1")
    rc, out = E.build("creal", "creal")
    for k, v in dict(PX, CONDA_OFFLINE=None).items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    ok("real conda + pip build (offline, wheelhouse only): exit 0, state ok", rc == 0 and E.states("creal")["creal"] == "ok", out[-3000:])
    r = subprocess.run(["bash", "-c", M.activate("creal", "creal") + "\npython -c 'import sys, tinypkg; print(sys.prefix)'"],
                       capture_output=True, text=True)
    ok("real conda + pip build: activation gives the new env's Python 3.10.12 and imports the wheel", r.returncode == 0 and f"{d}/root/envs/creal-" in r.stdout,
       r.stdout + r.stderr)
    del M.MACHINES["creal"]
    M.SPECS = SAVE_SPECS
    if SAVE_ST is None:
        os.environ.pop("TASKQ_STATE", None)
    else:
        os.environ["TASKQ_STATE"] = SAVE_ST
    shutil.rmtree(d)

RM.op, M.MACHINES[M.hub()]["root"] = SAVE_OP, SAVE_HUBROOT

print("all passed" if not fails else f"{fails} failures")
sys.exit(1 if fails else 0)
