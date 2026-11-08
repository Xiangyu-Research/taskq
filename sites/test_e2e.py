#!/usr/bin/env python3
"""End-to-end test: add tasks → send to an executor → transfer data → train → fetch back → decide done → next step.
Two layouts (together: the hub is also the store and a pueue executor; store: separate hub, store and executor) each run
in their own subprocess. Everything (roots, data dirs, queue, machine table) lives in a temp dir; ssh is a local bash -c
and pueue is a private pueued, so no real machine or state is touched.

    python3 sites/test_e2e.py (E2E_LAYOUT=together|store runs one layout; E2E_TMP dir; E2E_KEEP=1 keeps it)"""
import datetime as dt
import glob
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of this scheduler code
LAYOUTS = ("together", "store")
LAYOUT = os.environ.get("E2E_LAYOUT")
PUEUE = shutil.which("pueue") or os.path.expanduser("~/.local/bin/pueue")
PUEUED = shutil.which("pueued") or os.path.expanduser("~/.local/bin/pueued")


def numpy_python():
    cands = [sys.executable] + sorted(glob.glob(os.path.expanduser("~/miniforge3/envs/*/bin/python"))) \
        + [os.path.expanduser("~/miniforge3/bin/python3")]
    return next((p for p in cands if p and os.path.exists(p)
                 and subprocess.run([p, "-c", "import numpy, venv"], capture_output=True).returncode == 0), None)


def missing():
    """Why it cannot run (no pueue, no Python with numpy) → explanation; all present → None."""
    lack = [b for b in (PUEUE, PUEUED) if not os.access(b, os.X_OK)]
    if lack:
        return "no pueue on this machine (" + ", ".join(map(os.path.basename, lack)) + "), see queue/install_pueue.sh"
    if not numpy_python():
        return "no Python with numpy on this machine (a miniforge env)"
    return None


def runner():
    """Start one subprocess per layout at the same time and print each layout's output when done."""
    why = missing()
    if why:
        print(f"SKIP:{why}")
        return 77  # run_tests.py: skip
    ps = {n: subprocess.Popen([sys.executable, os.path.abspath(__file__)], env=dict(os.environ, E2E_LAYOUT=n),
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for n in LAYOUTS}
    bad = []
    for n, p in ps.items():
        out, _ = p.communicate()
        print(f"======== {n} ========\n{out.rstrip()}", flush=True)
        if p.returncode:
            bad.append(n)
    print("all passed" if not bad else f"layouts that failed: {', '.join(bad)}")
    return 1 if bad else 0


if __name__ == "__main__" and not LAYOUT:
    sys.exit(runner())

# ======================================================================================================
# The subprocess for one layout (E2E_LAYOUT)
# ======================================================================================================
sys.path.insert(0, os.path.join(CORE, "queue", "testdata"))
sys.path.insert(0, os.path.join(CORE, "queue"))
import fixture  # noqa: E402
import proto  # noqa: E402  stdlib only, reads no config

fixture.use()
KEEP = bool(os.environ.get("E2E_KEEP"))
T = (tempfile.mkdtemp if KEEP else fixture.tmpdir)(prefix=f"e2e_{LAYOUT}_", dir=os.environ.get("E2E_TMP") or None)
LOCAL = re.escape(socket.gethostname().split(".")[0])
BIN = os.path.join(T, "bin")
FAKE = os.path.join(BIN, "ssh")
CONDA = os.path.join(T, "conda")  # fake conda: activate only puts bin on PATH (bin/python3 is a local Python with numpy)
ORDER = os.path.join(T, "order")  # site_ops operations recorded by sitecustomize (which machine, which op) and the file order on fetch
HOOK = os.path.join(T, "hook")
CONF = os.path.join(T, "config")
HS = os.path.join(T, "hs")  # the hub's state dir (policy's OUTPUTS["state"]): hub queue, relay records, bundles
for d in (BIN, ORDER, HOOK, os.path.join(CONF, "specs"), os.path.join(HS, "taskq"), os.path.join(T, "tmp_hub")):
    os.makedirs(d)
open(os.path.join(HS, "taskq", proto.QUEUE["enabled"]), "w").close()
# fake ssh: skip -o and the host name, run here; the hub's variables are not passed (like relay_remote.site_argv for ssh=None); each machine gets its own temp dir
with open(FAKE, "w") as f:
    f.write('#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\nhost=$1; shift\n'
            f'export TMPDIR={T}/tmp_$host; mkdir -p "$TMPDIR"\n'
            f'cd "$HOME" && exec env {" ".join("-u " + v for v in proto.HUB_ONLY_ENV)} bash -c "$*"\n')
with open(os.path.join(BIN, "nvidia-smi"), "w") as f:  # never touch the real GPUs
    f.write("#!/bin/bash\nexit 0\n")
for p in (FAKE, os.path.join(BIN, "nvidia-smi")):
    os.chmod(p, 0o755)
with open(os.path.join(HOOK, "sitecustomize.py"), "w") as f:  # record only, change nothing: which machine ran which site_ops op; os.replace targets during pull
    f.write('''import os, sys
_d = os.environ.get("E2E_ORDER")
if _d and sys.argv and sys.argv[0].endswith("site_ops.py") and len(sys.argv) > 1:
    with open(os.path.join(_d, "ops.log"), "a") as f:
        f.write("%s\\t%s\\t%d\\n" % (os.environ.get("TASKQ_MACHINE", ""), sys.argv[1], os.getpid()))
    if sys.argv[1] == "pull":
        _rep = os.replace
        def _r(a, b, *k, **kw):
            _rep(a, b, *k, **kw)
            with open(os.path.join(_d, "pull_%d.txt" % os.getpid()), "a") as f:
                f.write(os.path.abspath(b) + "\\n")
        os.replace = _r
''')
s_ = socket.socket()
s_.bind(("127.0.0.1", 0))
PORT = s_.getsockname()[1]
s_.close()
TRANSFER = (f"http://127.0.0.1:{PORT}", f"127.0.0.1:{PORT}")
ENVS = {"sys": ("conda", "base"), "toy": "spec"}


def hm(hours):
    return (dt.datetime.now() + dt.timedelta(hours=hours)).strftime("%H:%M")


def pueue_home(tag):
    """The private pueued's dir, config and a pueue pointing at it (outside the roots)."""
    d = os.path.join(T, f"pq_{tag}")
    os.makedirs(d)
    cfgf = os.path.join(d, "c.yml")
    wrap = os.path.join(T, f"pw_{tag}", "pueue")
    os.makedirs(os.path.dirname(wrap))
    with open(wrap, "w") as f:
        f.write(f'#!/bin/bash\nexec {PUEUE} -c {cfgf} "$@"\n')
    os.chmod(wrap, 0o755)
    with open(cfgf, "w") as f:
        f.write(f"shared:\n  pueue_directory: {d}\n  runtime_directory: {d}\n  unix_socket_path: {SHORT}/{tag}.sock\n"
                f"  use_unix_socket: true\ndaemon:\n  callback: null\n")
    return dict(dir=d, cfg=cfgf, wrap=wrap)


# A short dir in the system temp dir (removed at the end): pueued's socket (path length is limited); the site-pueue
# executor's root is really T/r, used through a symlink here (so a root reached through a symlink is tested too)
SHORT = tempfile.mkdtemp(prefix="e2e_", dir="/tmp")
os.makedirs(os.path.join(T, "r"))
RLINK = os.path.join(SHORT, "r")
os.symlink(os.path.join(T, "r"), RLINK)
NAME = "site-pueue"
PQS = pueue_home("site")
SITE = dict(label="pueue", hostname=r"(?!)", ssh="fake-pueue-host", scheduler="pueue", root=RLINK, conda=(CONDA, "base"), envs=ENVS,
            pueue=PQS["wrap"], workers=[dict(gpu="1")], tunnel=("push", "pull"), budget=(0, 0))
if LAYOUT == "together":
    HR, HD = os.path.join(T, "hr"), os.path.join(T, "hd")  # the hub's root, data dir (the hub is also the store)
    PQH = pueue_home("hub")
    MACHINES = {
        "hub": dict(label="hub", hostname=LOCAL, ssh=None, roles=("hub", "store"), root=HR, data=HD, conda=(CONDA, "base"), envs=ENVS,
                    scheduler="pueue", pueue=PQH["wrap"], workers=[dict(gpu="1"), dict(gpu="0", hours=[(hm(-2), hm(2), None, None)])],
                    transfer=TRANSFER, budget=(0, 0)),
        NAME: SITE,
    }
    READY = {NAME: 5, "hub": 1}
    EXPS = None
    SD = HD  # the store's data dir
else:
    SR, SD = os.path.join(T, "sr"), os.path.join(T, "sd")  # the store's root, data dir
    PQH = None
    MACHINES = {
        "hub": dict(label="hub", hostname=LOCAL, ssh=None, roles=("hub",)),
        "store": dict(label="store", hostname=r"(?!)", ssh="fake-store", roles=("store",), root=SR, data=SD, conda=(CONDA, "base"),
                      transfer=TRANSFER, budget=(0, 0)),
        NAME: SITE,
    }
    READY = {NAME: 2}
    EXPS = os.path.join(T, "exps", "e2e", "experiments.py")
with open(os.path.join(CONF, "machines.py"), "w") as f:
    f.write(f'"""Machine table for the end-to-end test ({LAYOUT}): written by test_e2e.py."""\nMACHINES = {MACHINES!r}\n')
with open(os.path.join(CONF, "policy.py"), "w") as f:
    f.write(f'"""Policy for the end-to-end test ({LAYOUT}): written by test_e2e.py."""\nimport os\n\nimport cfg\n\n'
            'AUTO = dict(advance=True, envs=False, revive=False, smoke=False)\nPAUSE = {}\nHOLD = []\nPLACE = {}\n'
            f'READY = {READY!r}\nSLURM = {{}}\n'
            f'OUTPUTS = dict(state={HS + "/"!r}, bundle=(os.path.dirname(cfg.CORE_REL) or ".",), experiments={EXPS!r}, name="e2e")\n')
# env spec: a venv on a base env that has numpy (pip has nothing to download)
with open(os.path.join(CONF, "specs", "toy.yml"), "w") as f:
    f.write("# test\nbase: sys\npip:\n  - numpy\n")
with open(os.path.join(CONF, "specs", "toy.check.py"), "w") as f:
    f.write("import numpy\nprint('numpy', numpy.__version__)\n")

os.environ.update(
    TASKQ_CONFIG=CONF, PATH=BIN + ":" + os.environ["PATH"], RELAY_SSH=FAKE, TUNNEL_SSH=FAKE, TUNNEL_POLL="0.1", TUNNEL_IDLE="5",
    TMPDIR=os.path.join(T, "tmp_hub"),  # temp files left by stopped workers go with T
    # test switches for worker (consume.py) and run_task.sh: shorter waits
    TASKQ_POLL="1", TASKQ_IDLE_POLL="1", TASKQ_PATIENCE="0", TASKQ_IDLE_EXIT="6", TASKQ_STOP_POLL="1", TASKQ_STOP_WAIT="5",
    TASKQ_HEAD_CMD="echo same", RUN_TASK_POLL="1",
    TASKQ_TEST_NOW=os.path.join(T, "now"),  # the "now" the worker on an hours window sees (real time when the file is absent)
    E2E_ORDER=ORDER, PYTHONPATH=HOOK)
for v in ("TASKQ_DIR", "TASKQ_DATA", "TASKQ_ROOT", "TASKQ_MACHINE", "TASKQ_STATE", "TUNNEL_LOCK", "TUNNEL_URL", "TUNNEL_BIND"):
    os.environ.pop(v, None)

import cfg  # noqa: E402
cfg.use_config()
REPO = cfg.REPO  # the user's repo root
for d in (cfg.rel("queue"), cfg.rel("sync"), cfg.rel("monitor"), cfg.rel("sites")):
    sys.path.insert(0, os.path.join(REPO, d))
import mtab as M  # noqa: E402
import common as C  # noqa: E402
import spec as S  # noqa: E402
import taskq  # noqa: E402
import relay  # noqa: E402
import refill  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_store as ST  # noqa: E402
import relay_transfer as RT  # noqa: E402
import relay_bundle as RB  # noqa: E402
import envs as E  # noqa: E402
import pueue as PQ  # noqa: E402

HUB = M.hub()
LOG, FAILS, DAEMONS = [], [], []
TOY = r'''import argparse, glob, json, os, time
import numpy as np
p = argparse.ArgumentParser()
p.add_argument("--data"); p.add_argument("--out"); p.add_argument("--rounds", type=int, default=3)
p.add_argument("--pause", type=float, default=0)
a = p.parse_args()
os.makedirs(a.out, exist_ok=True)
files = [a.data] if os.path.isfile(a.data) else sorted(os.path.join(d, f) for d, _, fs in os.walk(a.data) for f in fs)
text = "".join(open(f).read() for f in files)
start, total = 0, 0
ck = sorted(glob.glob(f"{a.out}/ckpt_*.json"))
if ck:
    st = json.load(open(ck[-1])); start, total = st["round"], st["total"]
for r in range(start, a.rounds):
    total += int(np.frombuffer(text.encode(), dtype=np.uint8).sum())
    json.dump(dict(round=r + 1, total=total), open(f"{a.out}/ckpt.tmp", "w"))
    os.replace(f"{a.out}/ckpt.tmp", f"{a.out}/ckpt_{r + 1:02d}.json")
    for old in sorted(glob.glob(f"{a.out}/ckpt_*.json"))[:-1]:
        os.remove(old)
    json.dump({"done": r + 1, "total": a.rounds}, open(f"{a.out}/PROGRESS.json", "w"))
    open(f"{a.out}/train.log", "a").write(f"round {r + 1}\n")
    time.sleep(0.3)
    if a.pause and r + 1 == 2 and start == 0:
        time.sleep(a.pause)
json.dump(dict(total=total, text=text, start=start), open(f"{a.out}/result.json", "w"))
open(f"{a.out}/DONE", "w").close()
'''


def step(msg, cond, why="", fatal=True):
    if not cond and callable(why):  # the expensive explanation is only computed on failure
        why = why()
    print(("PASS:" if cond else "FAIL:") + msg + ("" if cond else f" ({why})"), flush=True)
    if not cond:
        FAILS.append(msg)
        if fatal:
            raise AssertionError(msg)


def wb(p, data):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb" if isinstance(data, bytes) else "w") as f:
        f.write(data)


def text_of(i):
    return f"data {i}\n" * i


def pq(home, *args):
    r = subprocess.run([PUEUE, "-c", home["cfg"], *args], capture_output=True, text=True, timeout=60)
    return r.returncode, r.stdout + r.stderr


def jobs(home):
    """The tasks in the private pueued → [[id, label, state, result], ...] (sites/pueue.py's parser); unreadable → None."""
    rc, out = pq(home, "status", "--json")
    return PQ.parse(out, list(json.loads(out).get("groups") or {})) if rc == 0 else None


def start_pueued(home, groups):
    p = subprocess.Popen([PUEUED, "-c", home["cfg"]], stdout=open(os.path.join(home["dir"], "daemon.log"), "w"),
                         stderr=subprocess.STDOUT, start_new_session=True)
    DAEMONS.append((home, p))
    for _ in range(50):
        if pq(home, "status", "--json")[0] == 0:
            break
        time.sleep(0.2)
    for g in groups:
        rc, out = pq(home, "group", "add", g)
        assert rc == 0, out


def run_local(cmd, host, timeout, input=None):
    return C.run(cmd, host, timeout, input=input, ssh=[FAKE])


def setup_common():
    py = numpy_python()
    os.makedirs(os.path.join(CONDA, "bin"))
    for n in ("python3", "python"):
        os.symlink(py, os.path.join(CONDA, "bin", n))
    wb(os.path.join(CONDA, "etc/profile.d/conda.sh"),
       f'conda() {{ if [[ ${{1:-}} == activate ]]; then export PATH="{CONDA}/bin:$PATH"; fi; }}\n')
    start_pueued(PQS, ["gpu1"])
    if PQH:
        start_pueued(PQH, ["gpu1", "gpu0"])
    relay.refill.add = fake_add
    # the bundle: this worktree's code (new files not in git yet go in through a temporary index, the real index is untouched); each machine gets it deployed by the check
    idx = os.path.join(T, "index")
    shutil.copy(os.path.join(REPO, subprocess.run(["git", "-C", REPO, "rev-parse", "--git-path", "index"], capture_output=True,
                                                  text=True).stdout.strip()), idx)
    env = dict(os.environ, GIT_INDEX_FILE=idx)
    subprocess.run(["git", "-C", REPO, "add", "-N", "--", "queue", "sites", "sync", "monitor", "bin"], env=env, check=True, capture_output=True)
    os.environ["GIT_INDEX_FILE"] = idx
    try:
        bid, path = RB.build(None)
    finally:
        os.environ.pop("GIT_INDEX_FILE")
    commit = subprocess.run(["git", "-C", REPO, "rev-parse", "origin/main"], capture_output=True, text=True).stdout.strip()
    core = subprocess.run(["git", "-C", cfg.CORE, "rev-parse", RB.CORE_REF], capture_output=True, text=True).stdout.strip()
    relay.BUNDLE_REF = commit  # a push to main elsewhere during the test does not rebuild
    ST.update_state(lambda st: st.setdefault("bundle_for", {}).update({f"{commit}:{core}": bid}))
    return bid


# ---------------- the hub's IO group: done in-process ----------------
ADDED = []


def fake_add(host, lbl, step_, key, args, run=None, **rec):
    ADDED.append((step_, key, rec.get("to")))
    if step_ == "relay_stage":
        RT.stage(rec["to"], key, nice=int(re.search(r"--nice (-?\d+)", args).group(1)), say=LOG.append)
    elif step_ == "relay_fetch":
        RT.fetch(rec["to"], key, say=LOG.append)
    else:
        raise AssertionError(f"unexpected hub task {step_} {key}")
    LOG.append(f"hub task {step_} {key}")
    return len(LOG)


def snapshot():
    """The parts of health.gather the scheduling needs (error handling as there): hub queue (unreadable → None, reason in
    why), remote task state on the store (errors → reason in collect, not treated as the store being down)."""
    s = dict(taskq={}, pueue={HUB: dict(tasks=[])}, records=[], ok={HUB: True}, why={}, src={}, collect={})
    try:
        q = s["taskq"][HUB] = taskq.local_status()
    except taskq.DiskError as e:
        s["taskq"][HUB], s["why"]["taskq"] = None, str(e)
        return s
    try:
        s["src"][M.store()] = RM.store_op("collect", dict(specs=S.task_specs(q["tasks"]), wait=5), timeout=120)
    except RuntimeError as e:
        s["collect"][M.store()] = str(e)[-300:]
    return s


def check(adv=False, src_ok=True):
    """The scheduling part of one health check (advance.run → relay.run_all); error messages, stuck problems, errors
    collecting remote task state on the store (when src_ok) → AssertionError. → (messages, this state)."""
    s = snapshot()
    if src_ok and (s["why"] or s["collect"]):
        raise AssertionError(f"check failed: {s['why']} {s['collect']}")
    stuck = {}
    if adv:
        import advance
        did, st = advance.run(s)
        LOG.extend(f"advance: {x}" for x in did)
        stuck.update(st)
        s["taskq"][HUB] = taskq.local_status()
    msgs, st = relay.run_all(s, False)
    LOG.extend(msgs)
    stuck.update(st)
    bad = [m for m in msgs if "failed" in m or "could not" in m]
    if bad or stuck:
        raise AssertionError(f"check failed: {bad}; stuck: { {k: proto.alert_msg(v) for k, v in stuck.items()} }")
    return msgs, s


def hub_tasks():
    return {t["key"]: t for t in taskq.local_status()["tasks"]}


def keys(name=NAME):
    return ST.keys_of(ST.load_state(), name)


def central(args):
    rc, out, err = taskq.central(args)
    assert rc == 0, out + err
    return out


def ops():
    """The site_ops operations recorded by sitecustomize → [(machine, op)]."""
    try:
        return [tuple(ln.split("\t")[:2]) for ln in open(os.path.join(ORDER, "ops.log")).read().splitlines()]
    except OSError:
        return []


def pulled(out):
    """The order os.replace placed files into the data dir during the latest fetch of out (staging in .relay_in excluded)
    → paths relative to the data dir."""
    seq = []
    for f in sorted(glob.glob(os.path.join(ORDER, "pull_*.txt")), key=os.path.getmtime):
        lines = [ln for ln in open(f).read().splitlines() if f"/{proto.RELAY_INBOX}/" not in ln]
        hit = [os.path.relpath(ln, SD) for ln in lines if ln.startswith(os.path.join(SD, out) + "/")]
        if hit:
            seq = hit
    return seq


def wait(cond, timeout, what, every=1.0, act=None):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if act:
            act()
        if cond():
            return
        time.sleep(every)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


def view_files(name=NAME):
    """Each remote task's views (dirs) and the files of single input files → symlink targets."""
    D = os.path.join(M.MACHINES[name]["root"], "data")
    out = {}
    for k in keys(name).values():
        for v in k["views"] + k.get("placed", []):
            ps = [os.path.join(D, v)] if not os.path.isdir(os.path.join(D, v)) else \
                [os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(D, v)) for f in fs]  # a view is a file or a dir
            for p in ps:
                out[os.path.relpath(p, D)] = os.readlink(p) if os.path.islink(p) else None
    return out


def blob_files(root):
    """Content files in the root's blobs (the .st records beside them do not count)."""
    return [os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(root, "blobs")) for f in fs if not f.endswith(".st")]


def step_deploy(bid):
    _, s = check(src_ok=False)  # no bundle on the store yet: operations on the store (collecting remote task state; hub queue state too when the store is not the hub) are all unreadable
    step("first check (not deployed yet): the only unreadable things are operations on the store, not the hub or store being down",
         set(s["why"]) <= {"taskq"} and (s["why"] or s["collect"]) and all(s["ok"].values()), (s["why"], s["collect"], s["ok"]))
    cur = relay.current_bundle(M.store())
    step("the first check deploys the bundle to the store (the bundle is switched even when the hub queue is unreadable; later checks can read it)", cur == bid, f"current on the store is {cur!r}")
    check()
    roots = [m["root"] for m in M.MACHINES.values() if m.get("root")]
    cur = {r: os.path.basename(os.path.realpath(os.path.join(r, proto.ROOT["current"]))) for r in roots}
    step(f"first check: bundle {bid} deployed to every machine with a root (store, executors; this machine too)", all(v == bid for v in cur.values()), cur)
    rs = relay.remote_status(NAME)
    step("the executor's queue is enabled and empty, its own pueued is up, and the status shows this bundle",
         rs is not None and rs["bundle"] == bid and rs["taskq"]["enabled"] and rs["taskq"]["tasks"] == [] and rs["jobs"] == [], rs)
    used = int(subprocess.run(["du", "-sb", os.path.join(T, "r")], capture_output=True, text=True).stdout.split()[0])
    M.MACHINES[NAME]["cap"] = 1
    try:
        rs2 = relay.remote_status(NAME)
    finally:
        M.MACHINES[NAME].pop("cap")
    step("cap: free space is the cap minus what the root already uses, not the whole disk", rs2 is not None and 0 < rs2["free"] <= 1e9 - used * 0.5, (rs2 or {}).get("free"))


# ======================================================================================================
# together: the hub is also the store and an executor
# ======================================================================================================
KEYS = [f"guest/e2e/t{i}" for i in range(1, 6)]
SLOW = KEYS[0]
RROOT = RLINK


def sheet(key, inputs, cmd, out, machines):
    return dict(key=key, owner="alice", env="toy", inputs=inputs, cmd=cmd, out=out, done=["DONE"], progress="progress_json",
                resume=["ckpt_*.json"], log="train.log", res={"gpus": 1}, machines=machines, nice=50,
                out_gb=0.01)  # space left for outputs when choosing an executor: anything fits on the test disk


def setup_together():
    # inputs on the store (hub): shared code, each task's own data (t5's is one file)
    wb(os.path.join(HD, "guest/e2e/code/toy.py"), TOY)
    for i in range(1, 5):
        wb(os.path.join(HD, f"guest/e2e/d{i}/a.txt"), text_of(i))
    wb(os.path.join(HD, "guest/e2e/one.txt"), text_of(5))
    sheets = []
    for i, k in enumerate(KEYS, 1):
        data = "{data}/guest/e2e/one.txt" if i == 5 else f"{{data}}/guest/e2e/d{i}"
        sheets.append(sheet(k, ["guest/e2e/code", "guest/e2e/one.txt" if i == 5 else f"guest/e2e/d{i}"],
                            f"python3 {{data}}/guest/e2e/code/toy.py --data {data} --out {{data}}/guest/e2e/r/t{i}"
                            + (" --pause 600" if k == SLOW else ""), f"guest/e2e/r/t{i}", [NAME]))
    wb(os.path.join(T, "sheets/job.json"), json.dumps(sheets, ensure_ascii=False))


def step_env_gate():
    out = central(f"add --file {T}/sheets/job.json")
    bt = hub_tasks()
    step("the sheet added 5 tasks, default relay=path", sorted(bt) == KEYS and all(t["spec"]["relay"] == "path" for t in bt.values()), out)
    st = E.states(NAME, run=run_local)
    step("before the env is built: toy not built", st == {"sys": "not spec-built", "toy": "not built"}, st)
    check()
    bt, rs = hub_tasks(), relay.remote_status(NAME)
    step("no .ok: nothing is claimed or sent, the remote queue is empty, no worker is started",
         all(t["state"] == "free" and not t["worker"] for t in bt.values()) and rs["taskq"]["tasks"] == [] and jobs(PQS) == [],
         [(t["key"], t["state"], t["worker"]) for t in bt.values()])
    rc, out = E.build(NAME, "toy", run=run_local)
    okf = os.path.join(RROOT, "state/envs/toy.ok")
    step("how taskq site env builds (envs.build): built, checked, .ok written (= hash of the spec plus this machine's variant lock)",
         rc == 0 and open(okf).read().strip() == M.spec_hash("toy", M.spec_variant(NAME, "toy")), out)
    a = M.activate(NAME, "toy")
    r = subprocess.run(["bash", "-c", a + "\npython -c 'import numpy,sys; print(sys.prefix)'"], capture_output=True, text=True)
    step("after activation the built venv is used and numpy imports", r.returncode == 0 and "/envs/toy-" in r.stdout, r.stdout + r.stderr)
    st = E.states(NAME, run=run_local)
    step("state: toy ok", st["toy"] == "ok", st)


def step_stage():
    n0 = len(LOG)
    check()
    bt, rs = hub_tasks(), relay.remote_status(NAME)
    step("with .ok: all 5 are claimed for site-pueue, sent over and added to the remote queue",
         all(t["state"] == "claimed" and t["worker"] == NAME for t in bt.values())
         and sorted(t["state"] for t in rs["taskq"]["tasks"]) == ["free"] * 5,
         lambda: ([(t["key"], t["state"]) for t in rs["taskq"]["tasks"]], LOG[n0:][-20:]))
    pushes = [o for o in ops() if o[1] == "push"]
    step("data goes from the store (hub) to the executor: push runs on the store, over the tunnel",
         pushes and all(m == HUB for m, _ in pushes) and any("tunnel transfer" in x for x in LOG[n0:]), (pushes, LOG[n0:][-20:]))
    blobs = blob_files(RROOT)
    vf = view_files()
    D = os.path.join(RROOT, "data")
    step(f"data goes into blobs by content ({len(blobs)} files, shared code only once), views are symlinks into blobs",
         len(blobs) == 6 and len(vf) == 6 and all(t and os.path.dirname(os.path.dirname(os.path.realpath(t))) == os.path.realpath(os.path.join(RROOT, "blobs"))
                                                  and os.path.isfile(os.path.join(D, p))
                                        for p, t in vf.items()), (len(blobs), vf))
    k1 = keys()[SLOW]
    step("same paths as the store: views are the input dirs, results stay at out, the remote keys are the hub queue keys",
         k1["views"] == ["guest/e2e/code", "guest/e2e/d1"] and k1["params"]["out"] == "guest/e2e/r/t1" and k1["outs"] == ["guest/e2e/r/t1"]
         and sorted(t["key"] for t in rs["taskq"]["tasks"]) == KEYS, k1)
    step("no worker started over there yet (nothing was ready when this check began)", jobs(PQS) == [], jobs(PQS))
    # views of two tasks at the same time: restaging one clears only its own views
    k2 = keys()[KEYS[1]]
    own2 = [v for v in k2["views"] if v not in k1["views"]][0]
    own1 = [v for v in k1["views"] if v not in k2["views"]][0]
    blob = blobs[0]
    os.symlink(blob, os.path.join(D, own2, "stray2.txt"))
    os.symlink(blob, os.path.join(D, own1, "stray1.txt"))
    os.symlink(blob, os.path.join(D, "guest/e2e/code", "stray0.txt"))
    before = view_files()
    RT.stage(NAME, SLOW, nice=50, add=False, say=LOG.append)
    after = view_files()
    step("several task views coexist: restaging t1 removes only extra links in views it owns alone, not t2's or shared ones",
         not os.path.lexists(os.path.join(D, own1, "stray1.txt")) and os.path.lexists(os.path.join(D, own2, "stray2.txt"))
         and os.path.lexists(os.path.join(D, "guest/e2e/code", "stray0.txt"))
         and all(after.get(p) == t for p, t in before.items() if not p.endswith("stray1.txt")), after)
    os.remove(os.path.join(D, own2, "stray2.txt"))
    os.remove(os.path.join(D, "guest/e2e/code", "stray0.txt"))
    check()
    js = jobs(PQS)
    step("next check: tasks are ready, so one worker is started", len(js) == 1 and js[0][1] == f"{proto.WORKER_LABEL}{NAME}-gpu1", js)


def remote_out(key):
    return os.path.join(RROOT, "data", keys()[key]["params"]["out"])


def progress_of(key):
    try:
        return json.load(open(os.path.join(remote_out(key), "PROGRESS.json")))["done"]
    except (OSError, ValueError, KeyError):
        return 0


def step_stop():
    wait(lambda: progress_of(SLOW) == 2, 120, "t1 finishes round 2", every=0.2)
    central(f"stop --why test {SLOW}")

    def released():
        t = hub_tasks()[SLOW]
        return t["state"] == "stopped" and not t["worker"]
    wait(released, 90, "after t1 is stopped in the hub queue, it stops over there and the hub releases it", every=2, act=check)
    rs = relay.remote_status(NAME)
    a = next(t for t in rs["taskq"]["tasks"] if t["key"] == SLOW)
    f = (rs.get("tasks") or {}).get(SLOW)
    t = hub_tasks()[SLOW]
    skips = os.path.join(HS, "taskq", proto.QUEUE["skips"])
    step("after the stop: the remote task is stopped, unclaimed, has progress by its checkpoint, and the hub release carries no skip",
         a["state"] == "stopped" and not a.get("worker") and refill.site_progress(f) and t["skipped_by"] == []
         and not (os.path.isdir(skips) and os.listdir(skips)), (a, f, t["skipped_by"]))
    step("the remote side keeps the round-2 checkpoint and has no DONE",
         os.path.exists(os.path.join(remote_out(SLOW), "ckpt_02.json")) and not os.path.exists(os.path.join(remote_out(SLOW), "DONE")),
         os.listdir(remote_out(SLOW)))
    # clean up while other tasks still use views: remove only what is unwanted
    before = view_files()
    reg = (ST.load_state().get("views") or {}).get(NAME) or {}
    keep = {v for k in keys().values() for v in ST.paths_of(k)}
    g = ST.gc(NAME, keep, RM.op, soft_gb=1e9, views=reg)
    step("cleanup while tasks still need them (relay_store.gc): no view or blob in use is removed",
         g["removed_views"] == [] and view_files() == before and all(os.path.isfile(os.path.join(RROOT, "data", p)) for p in before), g)
    central(f"resume {SLOW}")


def step_finish():
    seen_drop = []

    def all_done():
        bt = hub_tasks()
        ks = keys()
        if ks and len(ks) < len(KEYS):  # some deleted, some still there: the remaining views are intact
            vf = view_files()
            assert all(os.path.isfile(os.path.join(RROOT, "data", p)) for p in vf), "deleting one task touched another task's views"
            seen_drop.append(len(ks))
        return all(t["state"] == "done" for t in bt.values()) and not ks
    wait(all_done, 300, "all 5 done, fetched back, removed over there", every=2, act=check)
    step("when one done task is deleted, the other tasks' views stay", bool(seen_drop), "never saw a delete while others stayed")
    pulls = [o for o in ops() if o[1] == "pull"]
    step("fetch runs on the store (hub)", pulls and all(m == HUB for m, _ in pulls), pulls)
    for i, k in enumerate(KEYS, 1):
        out = os.path.join(HD, f"guest/e2e/r/t{i}")
        res = json.load(open(os.path.join(out, "result.json")))
        want = sum(text_of(i).encode())
        step(f"{k} fetched to the store: real paths, right content (3 rounds' sum, the inputs it read){', carried on from the round-2 checkpoint' if k == SLOW else ''}",
             res["text"] == text_of(i) and res["total"] == 3 * want and res["start"] == (2 if k == SLOW else 0)
             and json.load(open(os.path.join(out, "PROGRESS.json")))["done"] == 3 and os.path.exists(os.path.join(out, "DONE")), res)
        seq = pulled(f"guest/e2e/r/t{i}")
        step(f"{k} puts DONE last on fetch", seq and seq[-1] == f"guest/e2e/r/t{i}/DONE", seq)
        logs = [f for f in os.listdir(os.path.join(HD, proto.TASK_LOGS)) if f.startswith(proto.safe(k) + proto.SAFE)]
        step(f"{k}'s part log was fetched back", bool(logs), os.listdir(os.path.join(HD, proto.TASK_LOGS)))
    r = subprocess.run(["grep", "-rlF", HD, os.path.join(RROOT, "data")], capture_output=True, text=True)
    step("the executor's data dir has no absolute store paths ({data} is expanded over there)", r.stdout.strip() == "", r.stdout[:500])


def step_gc():
    M.MACHINES[NAME]["budget"] = (1e6, 0)  # a soft limit out of reach: delete everything possible
    relay.VIEW_KEEP_S = 0
    check()
    D = os.path.join(RROOT, "data")
    left = [os.path.relpath(os.path.join(dp, f), D) for dp, _, fs in os.walk(D) for f in fs]
    blobs = [f for _, _, fs in os.walk(os.path.join(RROOT, "blobs")) for f in fs]  # the .st records go too
    reg = (ST.load_state().get("views") or {}).get(NAME) or {}
    top = sorted(os.listdir(RROOT))
    step("cleanup after everything is done (relay_store.gc): views and blobs removed, only worker logs left in the data dir, registered views cleared",
         not blobs and all(p.startswith("logs/workers/") for p in left) and not reg, (left[:10], blobs[:5], reg))
    step("the root keeps only what is still needed: code, queue, blobs, data, tmp, state, envs", set(top) <= {"code", "queue", "blobs", "data", "tmp", "state", "envs"}
         and os.path.isfile(os.path.join(RROOT, "state/envs/toy.ok")), top)
    relay.VIEW_KEEP_S = 6 * 3600
    M.MACHINES[NAME]["budget"] = (0, 0)


K7 = "guest/e2e/t7"


def step_local():
    """The hub is also store and executor (ssh=None): no fake ssh, no tunnel, data is hard-linked into blobs on the same disk."""
    step("the local executor's job names (worker id-time; hostname-time when started by hand) map to it",
         M.machine_of_jobtag(M.workers(HUB)[0]["id"] + "-20261107120000") == M.MACHINES[HUB]["label"]
         and M.machine_of_jobtag(socket.gethostname().split(".")[0] + "-1107") == "hub")
    rc, out = E.build(HUB, "toy")  # no run given: run here per the machine table (ssh=None)
    step("local executor: build the env from its spec (under its own root)", rc == 0 and os.path.isfile(os.path.join(HR, "state/envs/toy.ok")), out)
    out_t7 = "{data}/guest/e2e/r/t7"
    wb(os.path.join(T, "sheets/job7.json"), json.dumps([sheet(
        K7, ["guest/e2e/code", "guest/e2e/d2"],
        f"mkdir -p {out_t7} && env > {out_t7}/env.txt && python3 {{data}}/guest/e2e/code/toy.py --data {{data}}/guest/e2e/d2 --out {out_t7}",
        "guest/e2e/r/t7", [HUB])], ensure_ascii=False))
    central(f"add --file {T}/sheets/job7.json")
    n0, nlog = len(ops()), len(LOG)
    check()
    t7 = hub_tasks()[K7]
    blobs = blob_files(HR)
    src = {os.stat(os.path.join(HD, p)).st_ino for p in ("guest/e2e/code/toy.py", "guest/e2e/d2/a.txt")}
    step("the local executor claimed t7, data is hard-linked from the store data dir into its blobs (same disk, no copy, no tunnel)",
         t7["worker"] == HUB and K7 in keys(HUB) and blobs and {os.stat(b).st_ino for b in blobs} == src
         and not [o for o in ops()[n0:] if o[1] == "push"] and not any("tunnel transfer" in x for x in LOG[nlog:]), (t7, blobs, LOG[nlog:][-8:]))
    wait(lambda: hub_tasks()[K7]["state"] == "done", 300, "local executor finishes t7 and fetches it back", every=2, act=check)
    out = os.path.join(HD, "guest/e2e/r/t7")
    res = json.load(open(os.path.join(out, "result.json")))
    step("t7 fetched: right result", res["text"] == text_of(2) and res["total"] == 3 * sum(text_of(2).encode()), res)
    step("t7 was fetched without a tunnel (local copy)", not any("tunnel transfer" in x for x in LOG[nlog:]), LOG[nlog:][-10:])
    env = dict(ln.split("=", 1) for ln in open(os.path.join(out, "env.txt")).read().splitlines() if "=" in ln)
    leaked = {v: env[v] for v in proto.HUB_ONLY_ENV if os.environ.get(v) and env.get(v) == os.environ[v]}
    data = os.path.realpath(env.get("TASKQ_DATA", ""))
    step("the local executor's task env has no hub config, data dir or queue; TASKQ_DATA is its own <root>/data",
         not leaked and data == os.path.realpath(os.path.join(HR, "data")), (leaked, env.get("TASKQ_DATA")))
    check()
    step("after the hub queue is done, t7's results on the local executor are deleted", K7 not in keys(HUB) and not os.path.exists(os.path.join(HR, "data/guest/e2e/r/t7")),
         keys(HUB))


K6 = "guest/e2e/t6"


def step_two_workers():
    """One machine with two workers: card 0 on an hours window, releases when it ends; card 1 finishes from the checkpoint."""
    wait(lambda: all(j[2] == "Done" for j in jobs(PQH)), 60, "the workers on the hub go idle and exit", every=1)
    pq(PQH, "pause", "-g", "gpu1")  # let only the card 0 worker claim first
    wb(os.path.join(T, "sheets/job6.json"), json.dumps([sheet(
        K6, ["guest/e2e/code", "guest/e2e/d1"],
        "python3 {data}/guest/e2e/code/toy.py --data {data}/guest/e2e/d1 --out {data}/guest/e2e/r/t6 --pause 600",
        "guest/e2e/r/t6", [HUB])], ensure_ascii=False))
    central(f"add --file {T}/sheets/job6.json")
    w0 = f"{proto.WORKER_LABEL}{HUB}-gpu0"
    first = max([j[0] for j in jobs(PQH)], default=-1)

    def running0():
        return any(j[1] == w0 and j[2] == "Running" and j[0] > first for j in jobs(PQH))
    wait(running0, 60, "the hub's card 0 worker starts", every=1, act=check)
    o6 = os.path.join(HR, "data/guest/e2e/r/t6")
    wait(lambda: os.path.exists(os.path.join(o6, "ckpt_02.json")), 120, "t6 finishes round 2 on card 0", every=0.2)
    rq = {t["key"]: t for t in relay.remote_status(HUB)["taskq"]["tasks"]}
    step("t6 is claimed by the card 0 worker", (rq.get(K6) or {}).get("worker") == f"{HUB}-gpu0", rq.get(K6))
    # the window ends: the worker sees TASKQ_TEST_NOW (3 hours later); the hub's table also gets a window that already ended (no more card 0 worker)
    wb(os.environ["TASKQ_TEST_NOW"], str(time.time() + 3 * 3600))
    M.MACHINES[HUB]["workers"][1]["hours"] = [(hm(-3), hm(-2), None, None)]
    jid = max(j[0] for j in jobs(PQH) if j[1] == w0)

    def job(i):
        return next(j for j in jobs(PQH) if j[0] == i)
    wait(lambda: job(jid)[2] == "Done", 90, "the card 0 worker exits after its window ends", every=1)
    jd = job(jid)
    log0 = pq(PQH, "log", "--full", str(jid))[1]
    rq = {t["key"]: t for t in relay.remote_status(HUB)["taskq"]["tasks"]}
    step("window end: the card 0 worker stops training, releases, exits normally (Success), t6 is unclaimed in the remote queue, keeps the round-2 checkpoint and has no DONE",
         "hours window ended" in log0 and jd[3] == "Success" and rq.get(K6) and not rq[K6].get("worker")
         and os.path.exists(os.path.join(o6, "ckpt_02.json")) and not os.path.exists(os.path.join(o6, "DONE")),
         lambda: (jd, rq.get(K6), log0[-1500:]))
    pq(PQH, "start", "-g", "gpu1")
    wait(lambda: hub_tasks()[K6]["state"] == "done", 300, "the card 1 worker finishes t6 from the checkpoint and fetches it back", every=2, act=check)
    res = json.load(open(os.path.join(HD, "guest/e2e/r/t6/result.json")))
    step("t6: the card 1 worker carries on from the round-2 checkpoint, right result", res["start"] == 2 and res["total"] == 3 * sum(text_of(1).encode()), res)
    segs = sorted(f for f in os.listdir(os.path.join(HD, proto.TASK_LOGS)) if f.startswith(proto.safe(K6) + proto.SAFE))
    # each part starts with "job <job>_<part> @ host, task=<key>, ..."; two parts started in the same minute share a job name and file
    heads = [ln for f in segs for ln in open(os.path.join(HD, proto.TASK_LOGS, f), errors="replace").read().splitlines()
             if ln.startswith("job ") and f"task={K6}," in ln]
    step("both parts from t6's two workers are in the fetched part log", len(heads) >= 2, (segs, heads))
    later = [j for j in jobs(PQH) if j[1] == w0 and j[0] > jid]
    step("no card 0 worker was started after the window ended", not later, later)


def main_together():
    bid = setup_common()
    setup_together()
    step_deploy(bid)
    step_env_gate()
    step_stage()
    step_stop()
    step_finish()
    step_gc()
    step_local()
    step_two_workers()


# ======================================================================================================
# store: the hub, store and executor have different roots
# ======================================================================================================
KS = "guest/e2e/s1"
SRC_TXT = text_of(4)
EXP_PY = '''"""Experiment table for the end-to-end test (store): written by test_e2e.py. s1 done → mid (writes temp file a.part) → use (reads a.part); a.part is deleted after use is done."""
EXPERIMENTS = []
FINAL = [
    dict(name="mid", env="toy", inputs=["e2e/r/s1", "e2e/code"], out="e2e/mid", done="keep.txt", temp=["*.part"], out_gb=0.01,
         cmd="mkdir -p {data}/e2e/mid && python3 {data}/e2e/code/total.py {data}/e2e/r/s1/result.json > {data}/e2e/mid/a.part"
             " && echo ok > {data}/e2e/mid/keep.txt"),
    dict(name="use", env="toy", inputs=["e2e/mid", "e2e/code"], out="e2e/use", done="use.txt", out_gb=0.01,
         cmd="mkdir -p {data}/e2e/use && cat {data}/e2e/mid/a.part {data}/e2e/mid/keep.txt > {data}/e2e/use/use.txt"),
]
'''
TOTAL_PY = "import json, sys\nprint(json.load(open(sys.argv[1]))['total'])\n"
OUT_NAMES = ("result.json", "PROGRESS.json", "keep.txt", "use.txt", "train.log", "DONE")


def setup_store():
    wb(EXPS, EXP_PY)
    wb(os.path.join(SD, "e2e/code/toy.py"), TOY)
    wb(os.path.join(SD, "e2e/code/total.py"), TOTAL_PY)
    wb(os.path.join(SD, "e2e/d1/a.txt"), SRC_TXT)
    wb(os.path.join(T, "sheets/job.json"), json.dumps([dict(
        key=KS, owner="alice", env="toy", inputs=["e2e/code", "e2e/d1"],
        cmd="python3 {data}/e2e/code/toy.py --data {data}/e2e/d1 --out {data}/e2e/r/s1", out="e2e/r/s1", done=["DONE"],
        progress="progress_json", resume=["ckpt_*.json"], log="train.log", res={"gpus": 1}, nice=50, out_gb=0.01)], ensure_ascii=False))


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def files_under(d, skip=()):
    out = []
    for dp, dns, fs in os.walk(d):
        dns[:] = [x for x in dns if os.path.join(dp, x) not in skip]
        out += [os.path.join(dp, f) for f in fs]
    return out


def main_store():
    bid = setup_common()
    setup_store()
    central(f"add --file {T}/sheets/job.json")
    step_deploy(bid)
    r = subprocess.run([sys.executable, os.path.join(CORE, "queue", "taskq.py"), "status", "--json"], capture_output=True, text=True)
    step("the hub is not the store and has no data dir: the hub queue state is computed on the store (site_ops facts) and readable (taskq status --json)",
         r.returncode == 0 and S.json_in(r.stdout) is not None, (r.stdout + r.stderr).strip()[-300:])
    rc, out = E.build(NAME, "toy", run=run_local)
    step("toy built on the executor from its spec", rc == 0, out)
    n0 = len(LOG)

    def finished():
        bt = hub_tasks()
        return all(bt.get(k, {}).get("state") == "done" for k in (KS, "mid", "use")) \
            and not os.path.exists(os.path.join(SD, "e2e/mid/a.part")) and not keys()
    wait(finished, 400, "s1 done → mid → use all done and fetched, a.part deleted, everything on the executor removed", every=2, act=lambda: check(adv=True))
    bt = hub_tasks()
    res = json.load(open(os.path.join(SD, "e2e/r/s1/result.json")))
    total = 3 * sum(SRC_TXT.encode())
    step("s1 fetched to the store: right content", res["text"] == SRC_TXT and res["total"] == total and res["start"] == 0, res)
    step("the two FINAL steps are queued by the experiment table (advance) and done on the executor",
         bt["mid"]["spec"].get("step") == "mid" and bt["use"]["spec"].get("step") == "use"
         and open(os.path.join(SD, "e2e/use/use.txt")).read() == f"{total}\nok\n", (bt.get("mid"), bt.get("use")))
    step("after the steps using mid are done, the temp file a.part is deleted on the store, keep.txt stays",
         not os.path.exists(os.path.join(SD, "e2e/mid/a.part")) and os.path.isfile(os.path.join(SD, "e2e/mid/keep.txt")),
         os.listdir(os.path.join(SD, "e2e/mid")))
    by = {}
    for m, o in ops():
        by.setdefault(o, set()).add(m)
    want = ("export", "push", "pull", "done", "rm", "collect", "probe")
    step("store operations (export, push, pull, done, rm, collect, probe) all run on the store", all(by.get(o) == {"store"} for o in want), by)
    step("the hub runs no site_ops operation itself (it has no root)", not any(m == HUB for m, _ in ops()), by)
    step("the store sends straight to the executor (over the tunnel)", any("tunnel transfer" in x and "fake-pueue-host" in x for x in LOG[n0:]), LOG[n0:][-30:])
    for out in ("e2e/r/s1", "e2e/mid", "e2e/use"):
        seq = pulled(out)
        done = {"e2e/r/s1": "DONE", "e2e/mid": "keep.txt", "e2e/use": "use.txt"}[out]
        step(f"{out} fetched into the store's data dir, the done marker placed last", seq and seq[-1] == f"{out}/{done}", seq)
    step("the executor's tasks and views are gone", not keys() and relay.remote_status(NAME)["taskq"]["tasks"] == [], keys())
    left = sorted(os.path.relpath(p, SD) for p in files_under(SD))
    extra = [p for p in left if not p.startswith(("e2e/code/", "e2e/d1/", "e2e/r/s1/", "e2e/mid/", "e2e/use/", proto.TASK_LOGS + "/"))]
    step("the store's data dir has only inputs, results and part logs", not extra, extra)
    # the hub's disk: everything under T except the other machines (the store's and executor's root and data dir, their temp dirs, the executor's pueued)
    other = {os.path.join(T, x) for x in ("sr", "sd", "r", "pq_site")} | set(glob.glob(os.path.join(T, "tmp_fake-*")))
    datas = {sha(p): os.path.relpath(p, SD) for p in files_under(SD) if os.path.getsize(p) >= 16}
    mine = [p for p in files_under(T, skip=other | {ORDER}) if os.path.isfile(p) and not os.path.islink(p)]
    copies = [(os.path.relpath(p, T), datas[sha(p)]) for p in mine if os.path.getsize(p) >= 16 and sha(p) in datas]
    named = [os.path.relpath(p, T) for p in mine if os.path.basename(p) in OUT_NAMES
             or re.fullmatch(r"ckpt_\d+\.json|.*\.part", os.path.basename(p))]
    step("no data files were written on the hub's disk (no input or result copies on the hub, none in the state dir)", not copies and not named, (copies[:5], named[:5]))


# ======================================================================================================
def cleanup():
    for home, p in DAEMONS:
        pq(home, "kill", "--all")
    time.sleep(1)
    for home, p in DAEMONS:
        pq(home, "shutdown")
        try:
            p.wait(10)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
    # stop only processes started from this temp dir (workers, training, tunnel servers)
    pids = set()
    for pat in (T, SHORT):
        pids |= set(subprocess.run(["pgrep", "-f", pat], capture_output=True, text=True).stdout.split())
    for pid in pids:
        if int(pid) != os.getpid():
            try:
                os.kill(int(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
    shutil.rmtree(SHORT, ignore_errors=True)


def main():
    try:
        main_together() if LAYOUT == "together" else main_store()
    except Exception as e:  # noqa: BLE001
        if not isinstance(e, AssertionError) or str(e) not in FAILS:
            print(f"FAIL:{type(e).__name__}: {e}")
            traceback.print_exc(file=sys.stdout)
            FAILS.append(str(e))
        print("--- latest check messages ---\n" + "\n".join(LOG[-30:]))
        if KEEP:
            print(f"temp dir {T}")
    finally:
        cleanup()
    print("all passed" if not FAILS else f"{len(FAILS)} failures")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
