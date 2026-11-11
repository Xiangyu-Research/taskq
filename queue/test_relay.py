#!/usr/bin/env python3
"""Tests for relay.py: per-check decisions (plan, assign, topup) with fake hub and remote state; the order results are moved
into the data dir (site_ops.place); an end-to-end run with a local store and a fake remote (site-slurm2, fake ssh and srun,
tunnel on 127.0.0.1); and single site_ops operations (view, list, ingest, drop with reference counting). Config is a copy of
testdata/ with temp roots and data dirs; bundled Python files are checked with the local bundle_check env, or the current Python.
Run: python3 queue/test_relay.py"""
import contextlib
import io
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cfg as _cfg  # noqa: E402  core root and project repo root
QR = _cfg.rel("queue")  # queue/ under the repo root (scripts/taskq/queue when nested, queue in a standalone checkout)
BQ = _cfg.bundle_rel("queue")  # scheduler queue/ inside a bundle (always scripts/taskq/queue)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
STATE_DIR = fixture.tmpdir(prefix="test_relay_state_")
os.environ["TASKQ_STATE"] = STATE_DIR  # local state (queue, records, ...) in a temp dir
import test_relay_plugins as TN  # noqa: E402  (creates temp dir TN.TMP; TN.ROOT is the store data dir)

T = TN.TMP
HROOT = os.path.join(T, "hub_root")  # store (hub) root: store operations run the bundle here
RROOT = os.path.join(T, "remote")  # site-slurm2 root
CONDA = os.path.join(T, "conda")
CONF = os.path.join(T, "config")
shutil.copytree(fixture.CONFIG, CONF, ignore=shutil.ignore_patterns("__pycache__"))
with open(os.path.join(CONF, "machines.py")) as f:
    mt = re.sub(r"^FAKE = .*$", f"FAKE = {fixture.FIX!r}", f.read(), count=1, flags=re.M)
with open(os.path.join(CONF, "machines.py"), "w") as f:
    f.write(mt + f'\nMACHINES["hub"].update(root={HROOT!r}, data={TN.ROOT!r}, conda=({CONDA!r}, "base0"))\n'
            f'MACHINES["site-slurm2"].update(root={RROOT!r}, conda=({CONDA!r}, "pt"), transfer_job="-p test")\n')
os.environ["TASKQ_CONFIG"] = CONF

FAKE = os.path.join(T, "fake_ssh")
BIN = os.path.join(T, "bin")
os.makedirs(BIN)
s = socket.socket()
s.bind(("127.0.0.1", 0))
PORT = s.getsockname()[1]
s.close()
os.environ.update(RELAY_SSH=FAKE, TUNNEL_SSH=FAKE, TUNNEL_URL=f"http://127.0.0.1:{PORT}", TUNNEL_BIND=f"127.0.0.1:{PORT}",
                  TUNNEL_POLL="0.1", TUNNEL_LOCK=os.path.join(T, "lock"), TASKQ_TASKS=os.path.join(T, "tasks.json"),
                  TASKQ_LAST=os.path.join(T, "last"), RELAY_LOG=os.path.join(T, "relay.log"),
                  PATH=BIN + ":" + os.environ["PATH"])

import cfg  # noqa: E402

cfg.use_config()
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
import mtab as M  # noqa: E402
import proto  # noqa: E402
import spec as S  # noqa: E402
import relay  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_store as ST  # noqa: E402
import relay_transfer as RT  # noqa: E402
import relay_bundle  # noqa: E402
import site_ops as SO  # noqa: E402

assert M.machine("hub")["root"] == HROOT and M.data() == TN.ROOT, "the copied config was not used"
NAME = "site-slurm2"
UNSET = " ".join(f"-u {v}" for v in proto.HUB_ONLY_ENV)
with open(FAKE, "w") as f:  # fake ssh: skip -o, answer -O check for master_down, run the rest locally (hub-only vars removed, as after a login)
    f.write(f'#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\n[[ $1 == -O ]] && {{ [[ -e {T}/master_down ]] && exit 255; exit 0; }}\n'
            f'shift\ncd / && exec env {UNSET} bash -c "$*"\n')
os.chmod(FAKE, 0o755)
with open(os.path.join(BIN, "srun"), "w") as f:  # fake srun: skip options and run from the python3 / bash argument on
    f.write(f"#!{sys.executable}\nimport os,sys\na=sys.argv[1:]\n"
            f"open({os.path.join(T, 'srun_args')!r},'a').write(' '.join(a)+chr(10))\n"
            f"if os.path.exists({os.path.join(T, 'no_slot')!r}):\n    sys.stderr.write('srun: error: Unable to allocate resources: Immediate')\n    sys.exit(1)\n"
            "i=next(i for i,x in enumerate(a) if x.endswith('python3') or x=='bash')\nos.execvp(a[i],a[i:])\n")
os.chmod(os.path.join(BIN, "srun"), 0o755)
os.makedirs(os.path.join(CONDA, "bin"))
os.symlink(relay_bundle.check_python() or sys.executable, os.path.join(CONDA, "bin", "python3"))
Q = "hub"  # machine holding the hub queue (taskq.queue_host())
assert relay.taskq.queue_host() == Q


def ok(msg):
    print("PASS:" + msg)


@contextlib.contextmanager
def patched(obj, **kw):
    """Temporarily replace attributes on obj and restore them on exit."""
    old = {k: getattr(obj, k) for k in kw}
    for k, v in kw.items():
        setattr(obj, k, v)
    try:
        yield
    finally:
        for k, v in old.items():
            setattr(obj, k, v)


@contextlib.contextmanager
def mpatch(name, **kw):
    """Temporarily change machine table entry name (a None value removes that field)."""
    m0 = dict(M.MACHINES[name])
    M.MACHINES[name].update({k: v for k, v in kw.items() if v is not None})
    for k in [k for k, v in kw.items() if v is None]:
        M.MACHINES[name].pop(k, None)
    try:
        yield
    finally:
        M.MACHINES[name].clear()
        M.MACHINES[name].update(m0)


@contextlib.contextmanager
def pdict(d, **kw):
    """Temporarily change a table in policy."""
    d0 = dict(d)
    d.update(kw)
    try:
        yield
    finally:
        d.clear()
        d.update(d0)


# ---------------- 1. decisions ----------------
def test_pueue_worker():
    g1, g0, cw = M.workers("site-pueue")
    assert (g1["id"], g0["id"], cw["id"], cw["kind"]) == ("site-pueue-gpu1", "site-pueue-gpu0", "site-pueue-cpu", "cpu"), (g1, g0, cw)
    lab = lambda w: proto.WORKER_LABEL + w["id"]  # noqa: E731
    ids = lambda subs: [w["id"] for _, w in subs]  # noqa: E731
    ready = dict(tasks=[dict(key="a/x/0", state="free"), dict(key="a/x/1", state="claimed")])
    none = dict(tasks=[dict(key="a/x/1", state="claimed")])
    nocpu = f"no staged unclaimed CPU task over there for {cw['id']}"
    assert relay.topup("site-pueue", dict(jobs=None, taskq=ready)) == ([], "cannot read pueue", [])
    subs, why, _ = relay.topup("site-pueue", dict(jobs=[], taskq=ready))
    assert ids(subs) == [g1["id"], g0["id"]] and why == nocpu, (subs, why)
    subs, why, _ = relay.topup("site-pueue", dict(jobs=[[3, lab(g1), "Running", None]], taskq=ready))
    assert ids(subs) == [g0["id"]] and why == f"{g1['id']} is running; {nocpu}", (subs, why)
    subs, why, _ = relay.topup("site-pueue", dict(jobs=[[3, lab(g1), "Done", "Killed"], [4, "other", "Running", None]], taskq=ready))
    assert ids(subs) == [g1["id"], g0["id"]], (subs, why)
    subs, why, failed = relay.topup("site-pueue", dict(jobs=[[3, lab(g1), "Done", "{'Failed': 1}"]], taskq=ready))
    assert ids(subs) == [g0["id"]] and why.startswith(f"previous {g1['id']} (pueue task 3) failed ({{'Failed': 1}})")
    assert failed == [(g1["id"], 3, "{'Failed': 1}")], failed
    ok("pueue executor (site-pueue): each worker in the machine table is judged separately; start one only when none is alive, and not after the previous one failed")
    gpu_none = "; ".join(f"no staged unclaimed task over there for {w['id']}" for w in (g1, g0))
    assert relay.topup("site-pueue", dict(jobs=[], taskq=none)) == ([], f"{gpu_none}; {nocpu}", [])
    assert relay.topup("site-pueue", dict(jobs=[[3, lab(g1), "Done", "Success"]], taskq=none)) == ([], f"{gpu_none}; {nocpu}", [])
    ok("pueue executor: no worker is started without such a staged task (an idle exit is not a failure), and one starts when work appears")
    cpu = dict(tasks=[dict(key="e/x/0", state="free", spec=dict(device="cpu")), dict(key="a/x/1", state="claimed")])
    subs, why, _ = relay.topup("site-pueue", dict(jobs=[], taskq=cpu))
    assert ids(subs) == [cw["id"]] and why == gpu_none, (subs, why)
    subs, why, _ = relay.topup("site-pueue", dict(jobs=[[5, lab(cw), "Done", "{'Failed': 1}"]], taskq=dict(tasks=cpu["tasks"] + ready["tasks"][:1])))
    assert ids(subs) == [g1["id"], g0["id"]] and why == f"previous {cw['id']} (pueue task 5) failed ({{'Failed': 1}})", (subs, why)
    with patched(relay.M, working=lambda n, w, t=None: w["id"] != g0["id"]):
        subs, why, _ = relay.topup("site-pueue", dict(jobs=[], taskq=ready))
    assert ids(subs) == [g1["id"]] and f"{g0['id']} is outside its hours window" in why, (subs, why)
    ok("pueue executor: a CPU worker only looks at pure CPU tasks; workers outside their hours window are not started (machines.py hours)")


def test_slurm_topup_ready():
    two = dict(enabled=True, tasks=[dict(key="a/x/0", state="free"), dict(key="a/x/1", state="free"), dict(key="a/x/2", state="claimed")])
    probe = lambda out: (lambda name, cmd: (0, out, ""))  # noqa: E731
    q = next(iter(relay.P.SLURM[NAME]["gpu"]))
    keep = relay.P.SLURM[NAME]["gpu"][q][1]
    subs, _, _ = relay.topup(NAME, dict(taskq=two), run=probe(f"QOS|{q}|10|0|0\n"))
    assert [n for _, _, n, _ in subs] == [10 - keep] and subs[0][3] == "gpu", subs
    subs, why, _ = relay.topup(NAME, dict(taskq=two), run=probe(f"QOS|{q}|10|{10 - keep}|2\n"))
    assert subs == [] and "full" in why
    subs, why, _ = relay.topup(NAME, dict(taskq=dict(enabled=True, tasks=[])), run=probe(f"QOS|{q}|10|0|0\n"))
    assert subs == [] and "no GPU tasks in the queue" in why
    assert relay.topup(NAME, dict(taskq=two), run=lambda n, c: (1, "", "ssh down")) == ([], "cannot read job counts", [])
    ok("Slurm executor (site-slurm2): with tasks, fill the quota (to build queue time); submit nothing when full, without tasks, or when job counts cannot be read")
    cpu = dict(enabled=True, tasks=[dict(key="e/x/0", state="free", spec=dict(device="cpu"))])
    subs, why, _ = relay.topup(NAME, dict(taskq=cpu), run=probe(f"QOS|{q}|10|0|0\n"))
    assert subs == [] and "CPU" not in why, (subs, why)
    slurm0 = relay.P.SLURM
    with pdict(relay.P.SLURM, **{"site-slurm": dict(gpu={"long": ("3-00:00:00", 0)}, cpu=("normal", "08:00:00", 2)),
                                 NAME: dict(slurm0[NAME], cpu=("normal", "08:00:00", 2))}):
        subs, why, _ = relay.topup("site-slurm", dict(taskq=cpu), run=probe("QOS|long|10|0|0\nCPU|0\nALL|10|3\n"))
        assert subs == [("normal", "08:00:00", 1, "cpu")], (subs, why)
        subs, why, _ = relay.topup("site-slurm", dict(taskq=dict(two, tasks=two["tasks"] + cpu["tasks"])), run=probe("QOS|long|10|0|0\nCPU|1\nALL|10|3\n"))
        assert subs == [("long", "3-00:00:00", 10, "gpu")] and "CPU: 1 already" in why, (subs, why)
        subs, why, _ = relay.topup(NAME, dict(taskq=cpu), run=probe(f"QOS|{q}|10|0|0\nCPU|0\nALL|10|3\n"))
        assert subs == [] and "CPU" not in why, (subs, why)
    with pdict(relay.P.SLURM):
        relay.P.SLURM.pop(NAME)
        assert relay.topup(NAME, dict(taskq=two), run=probe("")) == ([], "not in policy.py SLURM", [])
    ok("Slurm executor: one with cpu in jobs (site-slurm) submits CPU workers for pure CPU tasks; one without (site-slurm2) does not even with cpu in SLURM; "
       "a machine not in SLURM submits nothing")


def mtask(key, state="claimed", worker=NAME, nice=5, since=1000.0, relay_="toy", order=1, inputs=None, **kw):
    return dict(dict(key=key, state=state, worker=worker, nice=nice, order=order, since=since, note="",
                     spec=dict(env="base", inputs=inputs or [f"prepared/{key.split('/')[0]}"], relay=relay_, device="gpu", machines=None),
                     skipped_by=[]), **kw)


def rtask(key, state, worker=None):
    return dict(key=key, state=state, worker=worker, nice=5, order=1, note="", failures=0, spec=dict(out=f"out/{key}"))


def record(k, **kw):
    return dict(dict(recipe="toy", params={}, inputs=[f"prepared/{k.split('/')[0]}"], views=[f"prepared/{k.split('/')[0]}"],
                     outs=[f"out/{k}"]), **kw)


def S0(mt, hubq=(), recs=()):
    return dict(taskq={Q: dict(enabled=True, tasks=mt)}, pueue={M.hub(): dict(tasks=list(hubq))}, records=list(recs),
                ok={M.hub(): True}, src={n: dict(tasks={}) for n in M.executors()})


def keymap(*keys, name=NAME):
    return dict(keys={name: {k: record(k) for k in keys}})


def ops(acts):
    return sorted((a["op"], a["key"]) for a in acts)


def RS(*ts, **kw):
    """Remote state (remote_status result): ts are tasks in the remote queue."""
    return dict(dict(taskq=dict(tasks=list(ts), paused=kw.pop("paused", False)), tasks=kw.pop("tasks", {}), free=500e9, out_used=0,
                     envs_ok={}, bundle=None), **kw)


def test_plan():
    K = A = "D1_x/tr/0"
    st = keymap(K)

    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "claimed", "123")), st)
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "free")), st)
    assert ops(acts) == [("beat", K)], acts
    ok("queued and running over there → only beat")
    acts, _ = relay.plan(S0([mtask(K, nice=-101)]), NAME, RS(rtask(A, "free")), st)
    assert ops(acts) == [("beat", K), ("rnice", K)] and [a for a in acts if a["op"] == "rnice"][0]["nice"] == -101, acts
    acts, _ = relay.plan(S0([mtask(K, nice=-101)]), NAME, RS(rtask(A, "done")), st)
    assert "rnice" not in [a["op"] for a in acts], acts
    calls = []
    old_rtq = RM.rtq
    RM.rtq = lambda name, args, timeout=180, input=None: calls.append(args) or (0, "")
    try:
        msgs = relay.execute(NAME, [dict(op="rnice", key=K, rkey=A, nice=-101)], S0([mtask(K)]))
    finally:
        RM.rtq = old_rtq
    assert calls == [f"set-nice --nice -101 {A}"] and "nice -101" in msgs[0], (calls, msgs)
    ok("nice changed in the hub queue → follow over there (set-nice); done ones are not changed")

    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(), {})
    assert ops(acts) == [("beat", K), ("stage", K)], acts
    rec = dict(machine=M.hub(), id=7, label="relay_stage:" + K, step="relay_stage", key=K, added=2000.0)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=7, label=rec["label"], status="Running")], [rec]), NAME, RS(), {})
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=7, label=rec["label"], status="Failed")], [rec]), NAME, RS(), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    r3 = [dict(rec, id=i) for i in (5, 6, 7)]
    f3 = [dict(id=i, label=rec["label"], status="Failed") for i in (5, 6, 7)]
    acts, stuck = relay.plan(S0([mtask(K)], f3, r3), NAME, RS(), {})
    assert ops(acts) == [("beat", K)] and f"relaystage:{K}" in stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=7, label=rec["label"], status="Killed", result="Killed")], [rec]), NAME, RS(), {})
    assert ops(acts) == [("beat", K)] and f"relaystage:{K}" in stuck, (acts, stuck)
    old = dict(rec, added=10.0)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=7, label=rec["label"], status="Failed")], [old]), NAME, RS(), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    ok("not staged yet → queue staging; staging → wait; failure → alert and keep the claim; old tasks from before the claim do not count")
    ns = [dict(id=7, label=rec["label"], status=f"Failed ({relay.NOSLOT_RC})", code=relay.NOSLOT_RC)]
    acts, stuck = relay.plan(S0([mtask(K, since=time.time() - 600)], ns, [rec]), NAME, RS(), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K, since=time.time() - 4 * 3600)], ns, [dict(rec, added=time.time() - 3600)]), NAME, RS(), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and f"site:{NAME}:stage" in stuck, (acts, stuck)
    ok("staging cannot get a CPU job (exit 75) → retry next check without alerting; alert only after 3 hours of claims without a slot")

    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K), ("fetch", K)], acts
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(dict(rtask(A, "done", "123"), beat=time.time() - 60)), st)
    assert ops(acts) == [("beat", K)], acts
    ok("done marker exists and the worker still claims it (the command is still writing results) → only beat, no fetch")
    for kw in (dict(beat=time.time() - 3600), dict(beat=time.time() - 3600, task_log_age=3600), dict()):
        acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(dict(rtask(A, "done", "123"), **kw)), st)
        assert ops(acts) == [("beat", K), ("fetch", K)], (kw, acts)
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(dict(rtask(A, "done", "123"), beat=time.time() - 3600, task_log_age=30)), st)
    assert ops(acts) == [("beat", K)], acts
    ok("done marker with an expired claim (worker died without releasing) → fetch as usual; old heartbeat but the log is still being written → still claimed")
    frec = dict(machine=M.hub(), id=8, label="relay_fetch:" + K, step="relay_fetch", key=K, added=2000.0)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=8, label=frec["label"], status="Running")], [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=8, label=frec["label"], status="Failed")], [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K)] and f"relayfetch:{K}" in stuck, stuck
    ok("done over there → queue one fetch; fetching → wait; failure → alert")

    acts, _ = relay.plan(S0([mtask(K, "done")]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("drop", K), ("release", K)], acts
    fdone = [dict(id=8, label=frec["label"], status="Success")]
    acts, _ = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("drop", K), ("release", K)], acts
    acts, stuck = relay.plan(S0([mtask(K, "done")], [dict(fdone[0], status="Running")], [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, stuck = relay.plan(S0([mtask(K, "done")], [dict(fdone[0], status="Failed")], [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
    ok("hub queue says done (the store has it) → release and drop the remote task and results; fetch/archive still running → wait; failure → alert without dropping")

    import common as C
    old_lookup, old_down = RT.lookup, C.backup_down
    RT.lookup = lambda k: dict(archive=True)
    arec = dict(machine=M.hub(), id=9, label="relay_archive:" + K, step="relay_archive", key=K, added=2000.0)
    plain_fail = [dict(fdone[0], status="Failed")]
    try:
        C.backup_down = lambda ok=None: "site-slurm checks paused (down)"
        acts, stuck = relay.plan(S0([mtask(K, "done")], plain_fail, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
        acts, stuck = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("drop", K), ("release", K)] and not stuck, (acts, stuck)
        assert "not archived" in [a for a in acts if a["op"] == "release"][0]["why"], acts
        C.backup_down = lambda ok=None: "site-slurm unreachable this time" if ok is not None else None
        acts, stuck = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K), ("drop", K)] and not stuck, (acts, stuck)
        C.backup_down = lambda ok=None: None
        acts, stuck = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("archive", K), ("beat", K), ("drop", K)] and not stuck, (acts, stuck)
        two = [fdone[0], dict(id=9, label=arec["label"], status="Running")]
        acts, stuck = relay.plan(S0([mtask(K, "done")], two, [frec, arec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K)] and not stuck, (acts, stuck)
        two[1]["status"] = "Success"
        acts, _ = relay.plan(S0([mtask(K, "done")], two, [frec, arec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("drop", K), ("release", K)], acts
        assert ", archived" in [a for a in acts if a["op"] == "release"][0]["why"], acts
        acts, stuck = relay.plan(S0([mtask(K, "done")], [fdone[0], dict(id=9, label=arec["label"], status="Failed")], [frec, arec]), NAME,
                                 RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K), ("drop", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
    finally:
        RT.lookup, C.backup_down = old_lookup, old_down
    ok("tasks to archive: once the store has them, drop the remote ones; when the backup machine is usable, queue archive separately (relay_archive) and release only after it succeeds; archive failure → alert and keep the claim; "
       "backup machine paused → release without archive; only unreachable this time → wait; a separate fetch failure → alert")

    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "failed")), st)
    assert ops(acts) == [("drop", K), ("release", K)] and [a for a in acts if a["op"] == "release"][0].get("skip"), acts
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "failed"), tasks={A: dict(final=False, runs=[dict(epochs_done=3)])}), st)
    assert ops(acts) == [("beat", K)] and f"site:{NAME}:fail:{K}" in stuck, acts
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "bad")), st)
    assert ops(acts) == [("beat", K)] and f"site:{NAME}:fail:{K}" in stuck, stuck
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "claimed", "123"), paused=True), st)
    assert f"site:{NAME}:fail" in stuck, stuck
    ok("remote FAIL: no progress → release (--skip) and drop; progress or bad → keep and alert site:<name>:fail:<key>; remote paused → site:<name>:fail")

    ri = dict(rtask(A, "input"), input_since=5000.0)
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(ri), st)
    assert ops(acts) == [("beat", K), ("restage", K)] and not stuck, acts
    srec = dict(machine=M.hub(), id=9, label="relay_stage:" + K, step="relay_stage", key=K, added=6000.0)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=9, label=srec["label"], status="Running")], [srec]), NAME, RS(ri), st)
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=9, label=srec["label"], status="Failed")], [srec]), NAME, RS(ri), st)
    assert ops(acts) == [("beat", K), ("restage", K)] and not stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=i, label=srec["label"], status="Failed") for i in (9, 10, 11)],
                                [dict(srec, id=i) for i in (9, 10, 11)]), NAME, RS(ri), st)
    assert ops(acts) == [("beat", K)] and f"relaystage:{K}" in stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=9, label=srec["label"], status="Failed")], [dict(srec, added=1500.0)]), NAME, RS(ri), st)
    assert ops(acts) == [("beat", K), ("restage", K)] and not stuck, (acts, stuck)
    added = []
    msgs = relay.execute(NAME, [dict(op="restage", key=K)], S0([mtask(K)]), run_add=lambda *a, **k: added.append((a, k)) or 77)
    (host, lab, step, k_, args), kw = added[0]
    assert host == M.hub() and step == "relay_stage" and kw == dict(to=NAME, group=proto.IO_GROUP), added
    assert args.startswith(proto.IO_GROUP + " ") and "--no-add" in args and "--nice" not in args and "restage" in msgs[0], (added, msgs)
    ok("remote side reports missing inputs → queue a restage in the hub IO group (stage --no-add: only missing ones, clear the marker); restaging → wait; failure → alert; old tasks from before the marker do not count")

    def fail_add(*a, **k):
        raise RuntimeError("adding a hub task FAIL: git fetch failed")
    st_ = {}
    msgs = relay.execute(NAME, [dict(op="restage", key=K), dict(op="fetch", key=K)], S0([mtask(K)]), run_add=fail_add, stuck=st_)
    assert set(st_) == {f"relayadd:{NAME}:relay_stage:{K}", f"relayadd:{NAME}:relay_fetch:{K}"} and len(msgs) == 2 and all("failed" in m for m in msgs), (st_, msgs)
    import plain
    assert "Could not queue" in plain.problem(next(iter(st_)), dict(msg=st_[next(iter(st_))]["msg"], data=st_[next(iter(st_))]["data"])), st_
    ok("failing to queue staging or fetch on the hub → alert relayadd (retried next check) and keep running later actions")

    acts, _ = relay.plan(S0([mtask(K, "stopped")]), NAME, RS(rtask(A, "claimed", "123")), st)
    assert ops(acts) == [("beat", K), ("rstop", K)], acts
    acts, _ = relay.plan(S0([mtask(K, "stopped")]), NAME, RS(rtask(A, "stopped", "123")), st)
    assert ops(acts) == [("beat", K)], acts
    acts, _ = relay.plan(S0([mtask(K, "stopped")]), NAME, RS(rtask(A, "stopped")), st)
    assert ops(acts) == [("release", K)], acts
    acts, _ = relay.plan(S0([mtask(K, "stopped", worker=None)]), NAME, RS(rtask(A, "stopped")), st)
    assert ops(acts) == [], acts
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "stopped")), st)
    assert ops(acts) == [("beat", K), ("rresume", K)], acts
    ok("stopped in the hub queue → stop over there and release once it no longer claims it; claimed again → resume over there")

    acts, _ = relay.plan(S0([mtask(K, "free", worker=None)]), NAME, RS(rtask(A, "free")), st)
    assert ops(acts) == [("rstop", K)], acts
    acts, _ = relay.plan(S0([mtask(K, "done", worker=None)]), NAME, RS(rtask(A, "stopped")), st)
    assert ops(acts) == [("drop", K)], acts
    acts, _ = relay.plan(S0([mtask(K, "free", worker=None)]), NAME, RS(rtask(A, "stopped")), st)
    assert ops(acts) == [("drop", K)], acts
    acts, _ = relay.plan(S0([mtask(K, "free", worker=None)]), NAME, RS(rtask(A, "stopped"), tasks={A: dict(final=False, resumable=True)}), st)
    assert ops(acts) == [], acts
    acts, stuck = relay.plan(S0([]), NAME, RS(rtask("D999_zz/tr/0", "free")), st)
    assert ops(acts) == [] and list(stuck) == [f"site:{NAME}:orphan:D999_zz/tr/0"], stuck
    acts, stuck = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(rtask(A, "claimed", "123")), st)
    assert ops(acts) == [] and list(stuck) == [f"site:{NAME}:orphan:{K}"], stuck
    ok("present over there but not assigned to it: do not let it run; drop when the hub queue is done, or resumed unassigned while the remote side has no progress; unmapped or running ones alert site:<name>:orphan")

    for state in ("free", "stopped"):
        acts, stuck = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(rtask(A, state)), st)
        assert ops(acts) == [("drop", K)] and acts[0]["rkey"] == A and not acts[0].get("gone") and not stuck, (state, acts)
    acts, _ = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(), st)
    assert ops(acts) == [], acts
    dst = dict(keys={NAME: {K: dict(st["keys"][NAME][K], dropped=1.0)}})
    for mt in ([mtask(K, "claimed", "other")], [mtask(K, "done", "other")], []):
        acts, _ = relay.plan(S0(mt), NAME, RS(), dst)
        assert ops(acts) == [("drop", K)] and acts[0].get("gone") and acts[0]["rkey"] == K, (mt, acts)
    ok("reassigned to another executor in the hub queue (stopped over there too) → drop the remote task and results; remote task dropped but paths left (dropped) → remove only the paths")

    acts, stuck = relay.plan(S0([mtask(K)]), NAME, None, dict(st, last_ok={NAME: time.time() - 3 * 3600}))
    assert ops(acts) == [("beat", K)] and f"site:{NAME}:down" in stuck, stuck
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, None, dict(st, last_ok={NAME: time.time() - 600}))
    assert ops(acts) == [("beat", K)] and not stuck
    ok("unreachable: only renew hub claims; alert after 2 hours")




def test_assign():
    """Assign every unclaimed hub task to executors in one pass (relay.assign → refill.choose): each machine's room comes from
    its remote state (site_for), and input sizes are measured on the store."""
    K, K2, K3, K5, K0 = "D1_x/tr/0", "D2_y/tr/0", "D3_z/tr/0", "D5_d/tr/0", "D6_e/tr/0"
    st, st2 = keymap(K), keymap(K, K5)
    cands = [mtask(K2, "free", None, nice=3, order=2), mtask(K3, "free", None, nice=4, order=3),
             mtask("D4_c/tr/0", "free", None, relay_=None, order=4)]
    probed = []

    def probe(what, data, **kw):
        assert what == "probe" and data["size"], (what, data)
        probed.append(sorted(data["pats"]))
        return {p: dict(n=0 if "nowhere" in p else 1, newest=None, size=None if "nowhere" in p else 10e9) for p in data["pats"]}

    def run(mt, ctxs, s=None):
        stuck = {}
        out, why = relay.assign(s or S0(mt), {n: dict(rs=rs, st=c) for n, (rs, c) in ctxs.items()}, stuck, {})
        return {n: [(a["op"], a["key"]) for a in v] for n, v in out.items()}, why, stuck
    status0 = open(relay.STATUS).read() if os.path.exists(relay.STATUS) else None
    if status0 is not None:
        os.remove(relay.STATUS)
    with patched(relay.RM, store_op=probe), pdict(relay.P.AUTO, smoke=False), pdict(relay.P.READY):
        out, why, _ = run([mtask(K)] + cands, {NAME: (RS(rtask(K, "claimed", "1")), st)})
        assert out == {NAME: [("take", K2), ("take", K3)]}, (out, why)
        assert probed[-1] == ["prepared/D2_y", "prepared/D3_z"], probed  # do not measure what is already over there (D1_x)
        berr = S0([mtask(K)] + cands)
        berr["pueue"] = {M.hub(): {"error": "pueue not responding"}}
        out, why, _ = run(None, {NAME: (RS(rtask(K, "claimed", "1")), st)}, s=berr)
        assert out == {} and "pueue" in why[NAME], (out, why)
        ok("assignment: claim up to READY by nice, only resolvable recipes; assign nothing when hub pueue is unreadable (staging is queued in hub pueue)")
        out, why, _ = run([mtask(K), cands[0], dict(cands[1], nice=6)], {NAME: (RS(rtask(K, "free")), st)})
        assert out == {NAME: [("take", K2)]}, (out, why)
        out, why, _ = run([mtask(K), mtask(K5)] + [dict(t, nice=t["nice"] + 3) for t in cands], {NAME: (RS(rtask(K, "free")), st)})
        assert out == {NAME: []} and "full" in why[NAME], (out, why)
        tight = dict(RS(rtask(K, "claimed", "1")), free=(30 + 10 + 10 + 10 - 5) * 1e9)  # one task reserves 10 GB; another needs 10+10, leaving under 30
        out, why, _ = run([mtask(K)] + cands, {NAME: (tight, st)})
        assert out == {NAME: []} and "no room" in why[NAME], (out, why)
        out, why, _ = run([mtask(K), mtask(K2, "free", None, nice=3, inputs=["prepared/nowhere"])], {NAME: (RS(rtask(K, "claimed", "1")), st)})
        assert out == {NAME: []} and "input not on the store: prepared/nowhere" in why[NAME], (out, why)
        ok("assignment: claim only while fewer than READY are reserved over there (including not yet staged); do not assign when full, out of room or inputs are missing on the store; record the reason in why")
        full = [mtask(K), mtask(K5), mtask(K0, "free", None, nice=-100, order=5)] + [dict(t, nice=t["nice"] + 3) for t in cands]
        out, _, _ = run(full, {NAME: (RS(rtask(K, "free")), st)})
        assert out == {NAME: [("take", K0)]}, out
        out, _, _ = run([mtask(K), mtask(K0, "free", None, nice=-100, order=5)] + cands[:1], {NAME: (RS(rtask(K, "free")), st)})
        assert out == {NAME: [("take", K0), ("take", K2)]}, out
        ok("nice < 0 (post-training inference) takes no READY slot: claim it even when full, first")

        late = [mtask(K2, "free", None, nice=6), mtask(K3, "free", None, nice=7)]
        out, why, _ = run([mtask(K), mtask(K5, nice=6)] + late, {NAME: (RS(rtask(K, "stopped"), rtask(K5, "free")), st2)})
        assert out == {NAME: []} and "full" in why[NAME], (out, why)
        ok("stopped over there and about to resume also counts as reserved")
        one = [mtask(K2, "free", None, nice=3)]
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + one, {NAME: (RS(rtask(K, "free"), rtask(K5, "free")), st2)})
        assert out == {NAME: [("drop", K5), ("release", K5), ("take", K2)]}, out
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + one, {NAME: (RS(rtask(K, "claimed", "1"), rtask(K5, "free")), st2)})
        assert out == {NAME: [("take", K2)]}, out
        for f in (dict(runs=[dict(epochs_done=300)]), dict(resumable=True, runs=[])):  # half-done: hit the time limit and the worker released it
            out, _, _ = run([mtask(K), mtask(K5, nice=6)] + one, {NAME: (RS(rtask(K, "free"), rtask(K5, "free"), tasks={K5: f}), st2)})
            assert out == {NAME: [("drop", K), ("release", K), ("take", K2)]}, (f, out)  # swap in the one without progress
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + cands, {NAME: (RS(rtask(K, "free"), rtask(K5, "free")), st2)})
        assert out == {NAME: [("drop", K5), ("release", K5), ("take", K2)]}, out  # only one swap per kind per round
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + [mtask(K2, "free", None, nice=6)], {NAME: (RS(rtask(K, "free"), rtask(K5, "free")), st2)})
        assert out == {NAME: []}, out
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + one, {NAME: (RS(rtask(K, "free", "9"), rtask(K5, "free", "9")), st2)})
        assert out == {NAME: []}, out
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + one, {NAME: (dict(RS(rtask(K, "free"), rtask(K5, "free")), free=45e9), st2)})
        assert out == {NAME: []}, out
        ok("full and a higher-priority hub task exists: release a staged unclaimed one (drop remotely first, then release) and swap it in; never swap a running or claimed one, the same nice, or one with progress (released at the time limit); no swap without room; one per kind per round")

        E1, E2 = "D7_f/ev/0", "D8_g/ev/0"
        ctask = lambda key, nice=3: dict(mtask(key, "free", None, nice=nice), spec=dict(mtask(key)["spec"], device="cpu"))  # noqa: E731
        out, why, _ = run([mtask(K), ctask(E1), ctask(E2, 4)], {NAME: (RS(rtask(K, "free")), st)})
        assert out == {NAME: []} and "no task in the queue can go to it" in why[NAME], (out, why)
        out, why, _ = run([mtask(K), ctask(E1), ctask(E2, 4)] + cands, {NAME: (RS(rtask(K, "free")), st), "site-slurm": (RS(), {})})
        assert out == {NAME: [], "site-slurm": [("take", E1), ("take", K2), ("take", E2), ("take", K3)]}, out
        ok("pure CPU tasks go only to machines with a CPU worker (site-slurm); GPU and CPU each reserve READY slots independently")

        both = lambda a=None, b=None: {NAME: (a or RS(), {}), "site-slurm": (b or RS(), {})}  # noqa: E731
        out, _, _ = run(cands[:1], both())
        assert out == {NAME: [], "site-slurm": [("take", K2)]}, out
        out, why, _ = run(cands[:1], both(b=RS(free=40e9)))
        assert out == {NAME: [("take", K2)], "site-slurm": []}, out
        out, _, _ = run(cands[:1], {NAME: (RS(), keymap("D2_y/ev/9")), "site-slurm": (RS(), {})})
        assert out == {NAME: [("take", K2)], "site-slurm": []}, out
        out, _, _ = run([dict(cands[0], skipped_by=["site-slurm"])], both())
        assert out == {NAME: [("take", K2)], "site-slurm": []}, out
        out, why, stuck = run([dict(cands[0], skipped_by=["site-pueue", "site-slurm", NAME])], both())
        assert out == {NAME: [], "site-slurm": []} and list(stuck) == [f"tqblocked:{K2}"], (out, stuck)
        ok("several machines can take it: follow PLACE preference (absent = machine table order); skip one without room; prefer the machine that already has the inputs; "
           "never go to one that skipped it (skipped_by); if all possible ones skipped it → alert tqblocked and assign nothing")

        with mpatch(NAME, cap=None, ssh="fake-shared"), mpatch("site-slurm", cap=None, ssh="fake-shared"):
            small = RS(free=(30 + 10 + 10 + 5) * 1e9, dev=7)  # one task (inputs 10 + outputs 10) fits, two do not
            out, why, _ = run(cands[:2], {NAME: (small, {}), "site-slurm": (dict(small), {})})
            assert out == {NAME: [], "site-slurm": [("take", K2)]} and K3 not in str(out), (out, why)
            out, _, _ = run(cands[:2], {NAME: (small, {}), "site-slurm": (dict(small, dev=8), {})})
            assert out == {NAME: [("take", K3)], "site-slurm": [("take", K2)]}, out
        ok("machines on the same disk (same ssh, no cap, same disk id): deduct what this round already planned together; different disks count separately")

        on = (rtask(K, "claimed", "1"), rtask("D9_h/tr/0", "done"), rtask("D9_h/tr/1", "claimed", "2"))
        OUT = proto.OUT_GB
        sep = RS(*on, free=(3 * OUT + 35) * 1e9, out_used=12e9, dev=1, blob_dev=2, blob_free=500e9)
        out, _, _ = run([mtask(K)] + cands, {NAME: (sep, st)})
        assert out == {NAME: [("take", K2)]}, out  # 3×OUT+35+12 free; after reserving OUT for each of the 3 remote tasks 47 is left: one claim (reserve OUT) leaves 37, the second 27 < 30
        out, why, _ = run([mtask(K)] + cands, {NAME: (dict(sep, out_used=0), st)})
        assert out == {NAME: []} and "no room" in why[NAME], (out, why)
        out, why, _ = run([mtask(K)] + cands, {NAME: (dict(sep, free=500e9, blob_free=45e9), st)})
        assert out == {NAME: [("take", K2)]}, (out, why)  # inputs are checked on the blob disk: 45 − 10 leaves 35, the second 25
        same = RS(*on, free=(3 * OUT + 55) * 1e9, dev=1, blob_dev=1, blob_free=500e9)
        out, _, _ = run([mtask(K)] + cands, {NAME: (same, st)})
        assert out == {NAME: [("take", K2)]}, out  # same disk: inputs are deducted here too, 55 − 20 leaves 35, the second 15
        out, _, _ = run([mtask(K)] + cands, {NAME: (dict(same, blob_dev=2), st)})
        assert out == {NAME: [("take", K2), ("take", K3)]}, out
        ok("reserve out_gb (default proto.OUT_GB) for every remote task (done but not fetched, and claimed this round) and add back what they already wrote; "
           "when blobs are on another disk, check inputs there; on the same disk, deduct together")

        with patched(relay.M, working=lambda n, w, t=None: False):
            out, why, _ = run([mtask(K)] + cands, {NAME: (RS(rtask(K, "free")), st)})
        assert out == {} and why[NAME] == "no worker is inside its hours window (machines.py hours)", (out, why)
        out, why, _ = run([mtask(K)] + cands, {NAME: (None, st)})
        assert out == {} and why[NAME] == "unreachable", (out, why)
        ok("no assignment to machines with all workers outside their hours window, or unreachable")
    if status0 is not None:
        open(relay.STATUS, "w").write(status0)


def test_refcount():
    """Dropping a task removes only paths no other task uses: several parts of one dataset share a view, and inference inputs
    live under training outputs."""
    keys = {"t0": dict(views=["pre/X"], outs=["res/X/tr/frag_0"]),
            "t1": dict(views=["pre/X"], outs=["res/X/tr/frag_1"]),
            "inf": dict(placed=["res/X/tr/frag_0/final.ckpt", "res/X/tr/dataset.json", "raw/A/img/a.dat"],
                        outs=["pred/X/tr/frag_0"]),
            "ev": dict(placed=["pred/X/tr/frag_0/a.dat", "raw/A/gt/a.dat"], outs=["an/X/frag_0/eval"])}
    P = lambda k: ST.paths_of(keys[k])  # noqa: E731
    assert ST.droppable(keys, "t0", P("t0")) == []
    assert ST.droppable(keys, "t1", P("t1")) == ["res/X/tr/frag_1"]
    assert ST.droppable(keys, "inf", P("inf")) == ["raw/A/img/a.dat", "res/X/tr/dataset.json"]
    assert ST.droppable(keys, "ev", P("ev")) == ["an/X/frag_0/eval", "raw/A/gt/a.dat"]
    k2 = {k: v for k, v in keys.items() if k not in ("t1", "inf")}
    assert ST.droppable(k2, "t0", P("t0")) == ["pre/X", "res/X/tr/frag_0"]
    assert ST.overlaps("a/b", {"a/b/c"}, ST.ancestors({"a/b/c"})) and ST.overlaps("a/b/c/d", {"a/b"}, set())
    assert not ST.overlaps("a/bc", {"a/b"}, ST.ancestors({"a/b"}))
    ok("reference counting: shared views and outputs holding other tasks' inputs are kept; a shared name prefix alone does not count")

    seen = []
    with patched(RM, op=lambda name, what, data=None, **kw: (seen.append((what, data)), 1)[1], rtq=lambda name, cmd: (0, "")), \
            patched(ST, load_state=lambda: dict(keys={NAME: keys}), update_state=lambda fn: None):
        msgs = relay.execute(NAME, [dict(op="drop", key="inf", rkey="inf")], S0([]))
    assert seen == [("drop", dict(paths=["raw/A/img/a.dat", "res/X/tr/dataset.json"], logs="inf"))], (seen, msgs)
    ok("drop removes only the paths computed as removable, plus its own log")


def test_drop_refused():
    """When the remote side refuses to drop a task (someone is running it), remove no results and do not release in the hub
    queue; a task dropped last time without its results is cleaned as usual."""
    K = "D1_x/tr/0"
    seen, central = [], []
    acts = [dict(op="drop", key=K, rkey=K), dict(op="release", key=K, why="test")]
    rop = lambda name, what, data=None, **kw: (seen.append((what, data)), 1)[1]  # noqa: E731
    with patched(RM, op=rop), patched(relay.taskq, central=lambda args, timeout=300: (central.append(args), (0, "", ""))[1]):
        try:
            for r in [(relay.taskq.REFUSED, f"not dropping: {K}: claimed, someone is working on it"), (1, f"not in the queue: {K}"), (1, "Traceback"), None]:
                seen.clear()
                central.clear()
                with patched(RM, rtq=lambda name, cmd, r=r: r):
                    msgs = relay.execute(NAME, acts, S0([]))
                assert not seen, (r, seen)
                assert not central, (r, central, msgs)
            with patched(RM, rtq=lambda name, cmd: (relay.taskq.GONE, "")):
                msgs = relay.execute(NAME, acts, S0([]))
            assert seen and central, (seen, central, msgs)
            central.clear()
            ST.update_state(lambda st: st.setdefault("keys", {}).setdefault(NAME, {}).__setitem__(K, record(K)))

            def broken(name, what, data=None, **kw):
                raise RuntimeError("ssh down")
            with patched(RM, rtq=lambda name, cmd: (0, ""), op=broken):
                msgs = relay.execute(NAME, acts, S0([]))
            assert central and any("will retry next check" in m for m in msgs), (central, msgs)
            st = ST.load_state()
            assert st["keys"][NAME][K].get("dropped"), st
            acts2, _ = relay.plan(S0([]), NAME, RS(), st)
            assert [(a["op"], a["key"], a.get("gone")) for a in acts2] == [("drop", K, True)], acts2
            seen.clear()
            relay.execute(NAME, acts2, S0([]))
            assert [w for w, _ in seen] == ["drop"] and K not in ST.load_state()["keys"].get(NAME, {}), (seen, ST.load_state())
        finally:
            ST.update_state(lambda st: st.get("keys", {}).get(NAME, {}).pop(K, None))
    ok("remote refuses to drop (exit 3), or another failure/unreachable → remove no results and do not release; task already dropped (exit 4) → clean results as usual; "
       "task dropped but paths left → release as usual, record dropped, remove paths next round")


def test_beat_lost():
    """The hub claim is no longer its (beat --strict exit 13): leave these keys alone this round (no stage, fetch, drop or
    release); everything else proceeds."""
    K, K2 = "D1_x/tr/0", "D1_x/tr/1"
    calls, removed = [], []

    def central(args, timeout=300):
        calls.append(list(args) if not isinstance(args, str) else args.split())
        if calls[-1][0] == "beat":
            return relay.taskq.LOST, K + "\n", ""
        return 0, "", ""
    acts = [dict(op="beat", key=K), dict(op="beat", key=K2), dict(op="drop", key=K, rkey=K), dict(op="release", key=K, why="x"),
            dict(op="drop", key=K2, rkey=K2), dict(op="release", key=K2, why="x")]
    with patched(relay.taskq, central=central), patched(RT, drop_remote=lambda name, k, rk, gone=False: removed.append(k)):
        msgs = relay.execute(NAME, acts, S0([]))
    assert calls[0][:2] == ["beat", "--strict"] and set(calls[0][-2:]) == {K, K2}, calls
    assert removed == [K2] and [c for c in calls if c[0] == "release"] == [["release", "--worker", NAME, K2]], (removed, calls)
    assert any("no longer holds the claim" in m and K in m for m in msgs), msgs
    ok("relay renews with beat --strict: keys whose claim it no longer holds are left alone this round; others proceed")


def test_fresh_site():
    """First time on a new machine: reachable but no bundle in the root (status fails) → deploy first, then read status; when
    deploying fails, treat it as unreachable this round. With an old bundle, a failed switch (push error, timeout or build
    error) also counts as unreachable: only beat."""
    W = "site-pueue"
    s = {"ok": {M.hub(): True}, "taskq": {Q: {"tasks": []}}}
    # cur: remote code/current when status cannot be read ("" = none; "b0" = old bundle cannot run the new status script; "b1" = already new; None = unreachable)
    for fail, old, cur in ((None, False, ""), ("rt", False, ""), ("rt", True, ""), ("timeout", True, ""), ("build", True, ""),
                           (None, False, "b0"), ("rt", False, "b0"), ("same", False, "b1"), ("down", False, None)):
        calls = []
        seq = [{"bundle": "b0", "jobs": [], "squeue": None}] if old else [None, {"bundle": "b1", "jobs": [], "squeue": None}]

        def wanted():
            if fail == "build":
                raise RuntimeError("build failed")
            return "b1", "/nonexistent"

        def dep(n, b, p, claims=None):
            if fail == "rt":
                raise RuntimeError("push failed")
            if fail == "timeout":
                raise subprocess.TimeoutExpired("ssh", 600)
            calls.append(("deploy", n, b))
        with patched(relay, remote_status=lambda n: seq.pop(0) if seq else None, current_bundle=lambda n: calls.append(("cur",)) or cur,
                     wanted_bundle=wanted, deploy=dep, write_status=lambda n, rs, st: calls.append(("status", rs.get("bundle"))),
                     plan=lambda s, n, rs, st: calls.append(("plan", rs and rs.get("bundle"))) or ([], {})), \
                patched(ST, update_state=lambda fn: None, load_state=lambda: {}, gc=lambda *a, **kw: None):
            c = relay.prepare(s, W, False)
        assert (("cur",) in calls) == (not old), calls
        calls = [x for x in calls if x != ("cur",)]
        if fail:
            assert calls == [("plan", None)] and c["rs"] is None, (fail, old, cur, calls)
        else:
            assert calls == [("deploy", W, "b1"), ("status", "b1"), ("plan", "b1")] and c["rs"]["bundle"] == "b1", (cur, calls)
    ok("a new machine with no bundle deploys first, then reads status; a failed deploy (new machine or bundle switch) counts as unreachable this round")
    ok("status unreadable with an old remote bundle (it cannot run the new status script) → try one switch and read again; already new but unreachable → no deploy, treat as unreachable")


def test_paused():
    """Executors paused in policy.py PAUSE: do not read, assign or submit jobs this round; open their half-done tasks to other
    executors."""
    seen = []
    s = {"ok": {M.hub(): True}, "taskq": {Q: {"enabled": True, "tasks": []}}}
    ctx = lambda n: dict(rs={}, st={}, acts=[], stuck={}, notes=[])  # noqa: E731
    with pdict(relay.P.PAUSE, **{NAME: (None, "test")}), \
            patched(relay, update_code=lambda stuck: [], prepare=lambda s, n, dry: (seen.append(n), ctx(n))[1],
                    assign=lambda s, ctxs, stuck, notes: (seen.append(("assign", sorted(ctxs))), ({}, {}))[1],
                    finish=lambda s, n, c, more, why, sn, dry: (seen.append(("finish", n)), ([], {}))[1]):
        relay.run_all(s, dry=True)
        assert NAME not in seen and ("assign", sorted(n for n in M.executors() if n != NAME)) in seen, seen
        assert ("finish", NAME) not in seen and ("finish", "site-pueue") in seen, seen
    ok("paused executors (PAUSE): no read, assignment or job submission")


def test_held_by():
    """Half-done tasks return to their original machine: use what was read this round and status.json for the rest; open them
    up when that machine is paused or not an executor."""
    other = next(n for n in M.executors() if n != NAME)
    old = open(relay.STATUS).read() if os.path.exists(relay.STATUS) else None
    try:
        os.makedirs(os.path.dirname(relay.STATUS), exist_ok=True)
        json.dump({other: {"tasks": {"h/x": {"resumable": True}, "h/y": {}}}, NAME: {"tasks": {"h/old": {"ran": True}}},
                   "gone-site": {"tasks": {"h/z": {"ran": True}}}}, open(relay.STATUS, "w"))
        with patched(relay, site_view=lambda n, rs, st: {"h/me": {"ran": True}}):
            h = relay.held_all({NAME: dict(rs={}, st={})})
            assert h == {"h/x": [other], "h/me": [NAME]}, h
            with pdict(relay.P.PAUSE, **{other: (None, "test")}):
                assert relay.held_all({NAME: dict(rs={}, st={})}) == {"h/me": [NAME]}
            assert relay.held_all({NAME: dict(rs=None, st={})}) == {"h/x": [other], "h/old": [NAME]}
    finally:
        if old is None:
            os.remove(relay.STATUS)
        else:
            open(relay.STATUS, "w").write(old)
    ok("half-done tasks return to their original machine: use this round's read (or status.json when unreadable); open them up when the machine is paused or gone from the table")


def test_tilde_root():
    """A root written as ~/...: leave the leading ~ for the remote side to expand and quote the rest."""
    with mpatch(NAME, root="~/st/t 123"), mpatch("site-pueue", root="~/pq"):
        p = RM.prefix(NAME)
        assert p == f"cd ~/'st/t 123'/code/current && TASKQ_ROOT=~/'st/t 123' TASKQ_MACHINE={NAME}", p
        c = relay.SL.submit_cmd(NAME, "long", "1:00:00", "gpu")
        assert c.startswith("R=~/'st/t 123'; ") and '"~' not in c and '--chdir="$R"' in c, c
        w = relay.PQ.worker_cmd("site-pueue", M.workers("site-pueue")[0])
        assert "R=~/pq;" in w and '"~' not in w, w
        assert RM.root(NAME) == "~/st/t 123"
    ok("root ~/...: the ~ is unquoted in commands (expanded remotely), including job submission and worker start commands")


def test_adapters_in_relay():
    """All scheduler reads and writes go through sites/pueue.py and slurm.py."""
    g1 = M.workers("site-pueue")[0]
    lab = lambda w: proto.WORKER_LABEL + w["id"]  # noqa: E731
    raw = json.dumps(dict(tasks={"3": dict(group=g1["group"], label=lab(g1), status=dict(Done=dict(result="Success"))),
                                 "4": dict(group="other", label="x", status="Running")}))
    rs = relay.parse_sched("site-pueue", dict(sched_raw=raw, taskq={}))
    assert rs["squeue"] is None and [j[0] for j in rs["jobs"]] == [3] and rs["jobs"][0][3] == "Success", rs
    assert relay.parse_sched("site-pueue", dict(sched_raw=None))["jobs"] is None
    rs = relay.parse_sched(NAME, dict(sched_raw="101|w|RUNNING|1:00|long|n1\n"))
    assert rs["jobs"] is None and rs["squeue"] == [["101", "w", "RUNNING", "1:00", "long", "n1"]]
    seen = []
    with patched(RM, rsh=lambda name, cmd, input=None, timeout=300: (seen.append((name, cmd)), (0, "4870\n", ""))[1]):
        msgs, stuck = relay.submit(NAME, [("long", "1:00:00", 2, "gpu")])
        assert not stuck and len(seen) == 2 and all(re.search(r"sbatch --parsable -J \S+ --qos=long --time=1:00:00 --chdir=", c) for _, c in seen), seen
        assert all(c.startswith(f"R={relay.R.q(RM.root(NAME))}; ") for _, c in seen), seen
        seen.clear()
        msgs, stuck = relay.submit("site-slurm", [("normal", "08:00:00", 1, "cpu")])
        assert not stuck and len(seen) == 1 and "--qos=normal --time=08:00:00" in seen[0][1] and "cpu_worker.sbatch" in seen[0][1] \
            and "CPU worker" in msgs[0], (seen, msgs)
        seen.clear()
        cw = M.workers("site-pueue")[2]
        msgs, stuck = relay.submit("site-pueue", [("pueue", g1), ("pueue", cw)])
        assert not stuck and len(seen) == 2 and f" add -g {g1['group']} -l {lab(g1)} -- " in seen[0][1] \
            and f" add -g {cw['group']} -l {lab(cw)} -- " in seen[1][1], seen
    with patched(RM, rsh=lambda name, cmd, input=None, timeout=300: (1, "boom", "")):
        _, stuck = relay.submit("site-pueue", [("pueue", g1), ("pueue", cw)])
        assert list(stuck) == [f"site:site-pueue:worker:{g1['id']}", f"site:site-pueue:worker:{cw['id']}"], stuck
        _, stuck = relay.submit(NAME, [("long", "1:00:00", 1, "gpu")])
        assert list(stuck) == [f"topup:{NAME}:long"], stuck
        _, stuck = relay.submit("site-slurm", [("normal", "08:00:00", 1, "cpu")])
        assert list(stuck) == ["topup:site-slurm:normal:cpu"], stuck
    ok("relay: one ssh reads raw output and the hub parses it with adapters (this machine's worker groups); starting workers and submitting jobs use adapters; "
       "alert ids: pueue by worker, Slurm GPU unchanged and CPU with :cpu")


# ---------------- 2. order when moving fetched results into the data dir ----------------
@contextlib.contextmanager
def spy_replace(D, fail_at=None):
    """Record paths moved under D (os.replace targets; symlinks are also built at a temp name then moved); fail on the fail_at-th."""
    seen, real = [], os.replace

    def spy(a, b):
        if os.path.commonpath([os.path.abspath(b), D]) == D:
            seen.append(os.path.relpath(b, D))
            if len(seen) == fail_at:
                raise OSError("disk full")
        real(a, b)
    SO.os.replace = spy
    try:
        yield seen
    finally:
        SO.os.replace = real


def test_place():
    src, D = os.path.join(T, "place_src"), os.path.join(T, "place_dst")
    sp = dict(key="X/tr/0", out="results/X/tr/frag_0", done=["check/summary.json", "final.ckpt|ARCHIVED"], fetch=dict(extra=["*.json"]))
    written = ["results/X/tr/frag_0/check/a.dat", "results/X/tr/plans.json",
               "logs/tasks/X__tr__0__1_part1.log", "results/X/tr/frag_0/final.ckpt",
               "results/X/tr/frag_0/check/summary.json"]
    for w in written:
        TN.wb(os.path.join(src, w), b"new")
    TN.wb(os.path.join(D, "results/X/tr/plans.json"), b"old")
    with spy_replace(D, fail_at=3) as seen:  # the first marker after 2 regular files
        try:
            SO.place(src, D, written, sp)
            raise AssertionError("expected a failure")
        except OSError:
            pass
    assert not os.path.exists(os.path.join(D, sp["out"], "check/summary.json"))
    assert not os.path.exists(os.path.join(D, sp["out"], "final.ckpt"))
    SO.place(src, D, [w for w in written if os.path.exists(os.path.join(src, w))], sp)
    assert open(os.path.join(D, "results/X/tr/plans.json"), "rb").read() == b"old"
    assert seen[:2] == ["results/X/tr/frag_0/check/a.dat", "logs/tasks/X__tr__0__1_part1.log"], seen
    assert os.path.exists(os.path.join(D, sp["out"], "check/summary.json"))
    ok("fetch: done markers move last (a mid-way failure is not done), and existing JSON in the parent is not overwritten")

    src, D = os.path.join(T, "place2_src"), os.path.join(T, "place2_dst")
    sp = dict(key="m/0", out="preds/m/frag_0", done=["DONE"], fetch=dict(extra=[]))
    written = ["preds/m/frag_0/DONE", "preds/m/frag_0/MANIFEST.json", "preds/m/frag_0/c1.dat", "preds/m/note.txt"]
    for w in written:
        TN.wb(os.path.join(src, w), b"n")
    with spy_replace(D) as seen:
        SO.place(src, D, written, sp)
    assert seen == ["preds/m/frag_0/c1.dat", "preds/m/note.txt", "preds/m/frag_0/MANIFEST.json", "preds/m/frag_0/DONE"], seen
    ok("fetch: MANIFEST.json moves before done markers and after regular files")

    src, D = os.path.join(T, "place3_src"), os.path.join(T, "place3_dst")
    sp = dict(key="o/0", out="o/f0", done=["first.flag", "second.flag|alt.flag", "third.flag"], fetch=dict(extra=[]))
    written = ["o/f0/first.flag", "o/f0/second.flag", "o/f0/third.flag", "o/f0/x.dat"]
    for w in written:
        TN.wb(os.path.join(src, w), b"n")
    TN.wb(os.path.join(D, "prepared/D1/a.dat"), b"in")
    with spy_replace(D) as seen:
        SO.place(src, D, written, sp, links=[("o/f0/sub/in.dat", "prepared/D1/a.dat"), ("o/f0/last", "o/f0/x.dat")])
    assert seen == ["o/f0/x.dat", "o/f0/sub/in.dat", "o/f0/last", "o/f0/third.flag", "o/f0/second.flag", "o/f0/first.flag"], seen
    assert os.readlink(os.path.join(D, "o/f0/sub/in.dat")) == "../../../prepared/D1/a.dat" and os.readlink(os.path.join(D, "o/f0/last")) == "x.dat"
    assert open(os.path.join(D, "o/f0/sub/in.dat"), "rb").read() == b"in"
    ok("fetch: done items are placed in reverse with the first last; output symlinks are written relative and placed after regular files, before markers")


# ---------------- 3. end to end ----------------
def walk(d):
    return sorted((os.path.relpath(os.path.join(dp, f), d), os.readlink(os.path.join(dp, f)) if os.path.islink(os.path.join(dp, f)) else None)
                  for dp, _, fs in os.walk(d) for f in fs)


def nblobs():
    return sum(1 for _, _, fs in os.walk(os.path.join(RROOT, "blobs")) for f in fs if not f.endswith(proto.BLOB_STAT))


def test_e2e():
    TN.setup()
    sp = S.resolve("toy", dict(ds=TN.DS, frag="0"))
    RT.lookup = lambda k: sp
    bid, path = relay_bundle.build(None, os.path.join(T, "bundles"))
    for n in (Q, NAME):
        relay.deploy(n, bid, path)
    assert relay.current_bundle(Q) == bid
    rs = relay.remote_status(NAME)
    assert rs and rs["bundle"] == bid and rs["taskq"]["enabled"] and rs["taskq"]["tasks"] == [], rs
    ok(f"bundle {bid} pushed to the store (hub root) and the executor, current switched; the remote queue is enabled and empty")

    assert M.machine(NAME).get("ssh_master")
    open(os.path.join(T, "master_down"), "w").close()
    assert relay.remote_status(NAME) is None
    os.remove(os.path.join(T, "master_down"))
    assert relay.remote_status(NAME)["bundle"] == bid
    ok("ssh_master: a down master connection counts as unreachable without opening another; when up, read as usual")

    if os.path.exists(os.path.join(T, "srun_args")):
        os.remove(os.path.join(T, "srun_args"))
    assert RM.op(NAME, "missing", ["ab" * 32], job="-p cpu -t 10") == ["ab" * 32]
    sa = open(os.path.join(T, "srun_args")).read()
    assert f"-I{RM.JOB_WAIT} -p cpu -t 10 -J " in sa and " bash -c " in sa, sa
    open(os.path.join(T, "no_slot"), "w").close()
    try:
        RM.op(NAME, "missing", [], job="-p cpu")
        raise AssertionError("expected NoSlot")
    except RM.NoSlot:
        pass
    finally:
        os.remove(os.path.join(T, "no_slot"))
    ok("transfer_job: ingest runs in an srun CPU job (-I waits in the queue); no slot → NoSlot (no staging this time)")

    said = []
    os.remove(os.path.join(T, "srun_args"))
    assert RT.stage(NAME, sp["key"], nice=5, say=said.append) == sp["key"]
    assert "-p test -J " in open(os.path.join(T, "srun_args")).read()  # ingest uses the machine table transfer_job
    pre = os.path.join(RROOT, "data", "prepared", TN.DS)
    links = [os.path.join(dp, f) for dp, _, fs in os.walk(pre) for f in fs]
    assert links and all(os.path.islink(p) and os.path.isfile(p) for p in links), links
    assert all(os.path.realpath(p).startswith(os.path.join(RROOT, "blobs") + "/") for p in links)
    assert not os.path.exists(os.path.join(pre, proto.INCOMPLETE))
    gt = sorted(os.listdir(os.path.join(pre, "gt")))
    assert gt == sorted(c + ".dat" for c in TN.CASES), gt
    assert open(os.path.join(pre, "gt", TN.CASES[0] + ".dat"), "rb").read() == b"gt" + TN.CASES[0].encode()
    blobs = nblobs()
    rs = relay.remote_status(NAME)
    assert [(t["key"], t["state"]) for t in rs["taskq"]["tasks"]] == [(sp["key"], "free")], rs["taskq"]["tasks"]
    k = ST.keys_of(ST.load_state(), NAME)[sp["key"]]
    assert k["views"] == [f"prepared/{TN.DS}"] and k["outs"] == [sp["out"]] and k["params"] == sp["params"], k
    assert k["links"] and all(SO.HASH_RE.fullmatch(h) for h in k["links"].values()), k
    assert os.path.isfile(os.path.join(HROOT, proto.ROOT["hashes"])) and not os.path.exists(os.path.join(TN.ROOT, ".relay_in"))
    ok(f"staging: {blobs} files stored by content, symlinks at the same paths as the store, key and params unchanged, added to the remote queue; sha256 cached in the store state/hashes.db")

    rq = os.path.join(RROOT, "queue", "tasks.jsonl")
    recs = [json.loads(x) for x in open(rq) if x.strip()]
    want = S.sendable(S.resolve(sp["recipe"], sp["params"]))
    assert len(recs) == 1 and recs[0]["spec"] == want and recs[0]["recipe"] == sp["recipe"] and recs[0]["params"] == sp["params"], recs
    assert not {"machines", "after", "names", "exp"} & set(recs[0]["spec"]), recs[0]["spec"]
    ok("remote queue records carry the hub-resolved spec (without machines/after/names/exp), and keep the recipe and params")

    with open(rq + ".new", "w") as f:
        f.write(json.dumps(dict(recs[0], spec=dict(recs[0]["spec"], env="old")), ensure_ascii=False) + "\n")
    os.replace(rq + ".new", rq)
    RT.resend(NAME, say=lambda m: None)
    recs = [json.loads(x) for x in open(rq) if x.strip()]
    assert len(recs) == 1 and recs[0]["spec"] == want, recs
    ok("resend: replace remote queue specs with the current resolution, add no new tasks")

    sp2 = S.resolve("toy", dict(sp["params"], code="other/code"))  # taskq set on the hub after staging, same key
    RT.lookup = lambda k: sp2
    said = []
    RT.resend(NAME, say=said.append)
    recs = [json.loads(x) for x in open(rq) if x.strip()]
    assert len(recs) == 1 and recs[0]["spec"] == S.sendable(S.resolve("toy", sp2["params"])) and recs[0]["spec"] != want, (recs, said)
    assert ST.keys_of(ST.load_state(), NAME)[sp["key"]]["params"] == sp2["params"]

    def gone(k):
        raise RT.Gone(k)
    RT.lookup = gone
    RT.resend(NAME, say=lambda m: None)
    assert [json.loads(x) for x in open(rq) if x.strip()][0]["spec"] == recs[0]["spec"]
    RT.lookup = lambda k: sp
    real_op = RM.store_op
    RM.store_op = lambda what, data, **kw: dict(real_op(what, data, **kw), views=["prepared/other"])
    said.clear()
    try:
        RT.resend(NAME, say=said.append)
    finally:
        RM.store_op = real_op
    assert [json.loads(x) for x in open(rq) if x.strip()][0]["spec"] == recs[0]["spec"] and any("inputs changed" in m for m in said), said
    RT.resend(NAME, say=lambda m: None)
    assert [json.loads(x) for x in open(rq) if x.strip()][0]["spec"] == want
    ok("resend: takes the hub's current params (taskq set after staging) and records them; key gone from the hub → the recorded params; inputs changed since staging → not replaced")

    said.clear()
    assert RT.stage(NAME, sp["key"], nice=5, say=said.append) == sp["key"] and nblobs() == blobs
    assert any("0 missing over there" in m for m in said), said
    recs = [json.loads(x) for x in open(rq) if x.strip()]
    assert len(recs) == 1, recs
    ok("staging again: no retransfer and no duplicate add")

    D = os.path.join(RROOT, "data")
    rout = os.path.join(D, sp["out"])
    for c in TN.CASES[:2]:
        TN.wb(os.path.join(rout, "check", c + ".dat"), b"p")
    TN.wj(os.path.join(rout, "check", "summary.json"), dict(metric_per_case=[{}, {}]))
    TN.wb(os.path.join(rout, "final.ckpt"), b"ck")
    TN.wb(os.path.join(rout, "latest.ckpt"), b"latest")
    TN.wb(os.path.join(rout, "debug.json"), b"{}")
    TN.wj(os.path.join(os.path.dirname(rout), "plans.json"), {})
    g0 = os.path.join(pre, "gt", TN.CASES[0] + ".dat")
    os.makedirs(os.path.join(rout, "refs"))
    os.symlink(os.path.realpath(g0), os.path.join(rout, "refs", "blob.dat"))  # an unpacked input (points straight at a blob)
    os.symlink(os.path.relpath(g0, os.path.join(rout, "refs")), os.path.join(rout, "refs", "view.dat"))  # an input in a view
    os.symlink("../final.ckpt", os.path.join(rout, "refs", "best.ckpt"))  # one in the outputs
    log = S.safe(sp["key"]) + "__12345_part1.log"
    TN.wb(os.path.join(D, "logs", "tasks", log), b"log")
    out = os.path.join(TN.ROOT, sp["out"])
    inbox = os.path.join(TN.ROOT, proto.RELAY_INBOX, S.safe(sp["key"]))
    os.environ["TUNNEL_TEST_BADMD5"] = sp["out"] + "/final.ckpt"
    try:
        RT.fetch(NAME, sp["key"], say=lambda m: None)
        raise AssertionError("a file that was not fetched should fail")
    except RT.RN.RelayError as e:  # a failed store pull is also RelayError; the site_ops line survives (tunnel progress may follow it)
        assert "pull: " in str(e) and ("not fetched" in str(e) or "do not match" in str(e)), e
    finally:
        del os.environ["TUNNEL_TEST_BADMD5"]
    assert not os.path.exists(out) and os.path.isfile(os.path.join(inbox, sp["out"], "check", "summary.json"))
    sj = os.path.join(inbox, sp["out"], "check", "summary.json")
    TN.wb(sj, b"x" * os.path.getsize(sj))  # same size, different mtime
    TN.wb(os.path.join(inbox, sp["out"], "check", "old.dat"), b"old")
    TN.wb(os.path.join(inbox, sp["out"], "debug.json.tunnelpart"), b"{")
    said.clear()
    RT.fetch(NAME, sp["key"], say=said.append)
    assert any("tunnel transfer of 2 files" in m for m in said), said
    assert not os.path.exists(os.path.join(out, "check", "old.dat")) and not os.path.exists(os.path.join(out, "debug.json.tunnelpart"))
    assert json.load(open(os.path.join(out, "check", "summary.json"))) == dict(metric_per_case=[{}, {}])
    ok("fetch interrupted (previous run failed on a missing file, nothing placed on the store): retry fetches only missing files and size/mtime mismatches; old or partial files do not mix into results")
    assert sorted(f for f in os.listdir(os.path.join(out, "check")) if f.endswith(".dat")) == sorted(c + ".dat" for c in TN.CASES[:2])
    assert not os.path.exists(os.path.join(out, "latest.ckpt"))
    assert open(os.path.join(out, "final.ckpt"), "rb").read() == b"ck"
    assert os.path.exists(os.path.join(os.path.dirname(out), "plans.json"))
    assert not os.listdir(os.path.join(TN.ROOT, proto.RELAY_INBOX))
    logs = os.listdir(os.path.join(TN.ROOT, "logs", "tasks"))
    assert logs == [log], logs
    ok("fetch: same relative paths, latest.ckpt not fetched, parent json and part logs included, temp dirs removed")
    gin = os.path.join(TN.ROOT, "prepared", TN.DS, "gt", TN.CASES[0] + ".dat")
    for f in ("blob.dat", "view.dat"):
        p = os.path.join(out, "refs", f)
        assert os.readlink(p) == os.path.relpath(gin, os.path.dirname(p)) and os.path.samefile(p, gin), (f, os.readlink(p))
    assert os.readlink(os.path.join(out, "refs", "best.ckpt")) == "../final.ckpt"
    ok("fetch with symlinks: links to inputs (straight to a blob or into a view) become relative links to that input on the store; output links stay as is")

    relay.write_status(NAME, relay.remote_status(NAME), ST.load_state())
    st = json.load(open(relay.STATUS))[NAME]
    assert sp["key"] in st["tasks"] and st["tasks"][sp["key"]]["final"], st
    ok("the task is in status.json")
    return pre


def test_e2e_path(pre):
    """A generic task under a different name: it shares the remote side with the training view above, and staging touches
    only its own views; a partial fetch is not done."""
    before = walk(pre)
    sp = TN.path_setup()
    RT.lookup = lambda k: sp
    rk = RT.stage(NAME, sp["key"], add=False, say=lambda m: None)
    k = ST.keys_of(ST.load_state(), NAME)[sp["key"]]
    D = os.path.join(RROOT, "data")
    assert rk == sp["key"] and k["views"] == ["proj/inputs/cohortA"], k
    assert set(k["outs"]) == {sp["out"], "proj/analysis/thing/shared"}, k
    assert "proj/lists/cases.txt" in k["placed"] and "raw/Set/c1/c1-seg.dat" in k["placed"] and not any("t1" in os.path.basename(x) for x in k["placed"]), k
    assert open(os.path.join(D, "proj/inputs/cohortA", "y_0000.dat"), "rb").read() == b"yy"
    assert open(os.path.join(D, "proj/inputs/cohortA", "a", "x_0000.dat"), "rb").read() == b"xa"
    assert open(os.path.join(D, "raw/Set/c2/c2-seg.dat"), "rb").read() == b"sc2"
    assert not os.path.exists(os.path.join(D, "proj/inputs/cohortA", proto.INCOMPLETE))
    assert walk(pre) == before, "the training view was modified"
    TN.wb(os.path.join(D, "proj/inputs/cohortA", "stray.txt"), b"?")
    blob = next(os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(RROOT, "blobs")) for f in fs if not f.endswith(proto.BLOB_STAT))
    os.symlink(blob, os.path.join(D, "proj/inputs/cohortA", "stale"))
    RT.stage(NAME, sp["key"], add=False, say=lambda m: None)
    assert not os.path.lexists(os.path.join(D, "proj/inputs/cohortA", "stale"))
    assert os.path.exists(os.path.join(D, "proj/inputs/cohortA", "stray.txt"))  # not a link into blobs: leave it alone
    os.remove(os.path.join(D, "proj/inputs/cohortA", "stray.txt"))
    ok("generic plugin staging: dirs are views; files and expanded globs go to the same paths; only extra links into blobs are removed from a view; the training view is untouched")

    TN.wb(os.path.join(D, sp["out"], "a.csv"), b"1")
    TN.wb(os.path.join(D, os.path.dirname(sp["out"]), "plans.json"), b"{}")
    log = S.safe(sp["key"]) + "__9_part1.log"
    TN.wb(os.path.join(D, "logs", "tasks", log), b"log")
    try:
        RT.fetch(NAME, sp["key"], say=lambda m: None)
        raise AssertionError("missing .done should report not done")
    except relay.RN.RelayError as e:
        assert "not done" in str(e), e
    assert not os.path.exists(os.path.join(TN.ROOT, sp["out"]))
    os.symlink("/etc/hostname", os.path.join(D, sp["out"], "outside"))
    TN.wb(os.path.join(D, sp["out"], ".done"), b"")
    try:
        RT.fetch(NAME, sp["key"], say=lambda m: None)
        raise AssertionError("a link outside the data dir should fail")
    except relay.RN.RelayError as e:
        assert "pointing elsewhere" in str(e) and "outside" in str(e), e
    assert not os.path.exists(os.path.join(TN.ROOT, sp["out"]))
    os.remove(os.path.join(D, sp["out"], "outside"))
    RT.fetch(NAME, sp["key"], say=lambda m: None)
    out = os.path.join(TN.ROOT, sp["out"])
    assert sorted(os.listdir(out)) == [".done", "a.csv"], os.listdir(out)
    assert os.path.exists(os.path.join(TN.ROOT, "proj/analysis/thing/plans.json"))
    assert os.path.exists(os.path.join(TN.ROOT, "logs", "tasks", log))
    ok("generic plugin fetch: no fetch when not done or when outputs link outside the data dir; once done, results, parent files and part logs go to the store")

    keys = ST.keys_of(ST.load_state(), NAME)
    tk = next(r for r in keys if r != sp["key"])
    assert ST.droppable(keys, sp["key"], ST.paths_of(keys[sp["key"]])) == sorted(ST.paths_of(keys[sp["key"]]))
    RT.drop_remote(NAME, sp["key"], sp["key"], gone=True)
    assert not os.path.lexists(os.path.join(D, "raw/Set/c1/c1-seg.dat")) and not os.path.exists(os.path.join(D, "proj/inputs"))
    assert not os.path.exists(os.path.join(D, sp["out"])) and os.listdir(pre) and sp["key"] not in ST.keys_of(ST.load_state(), NAME)
    assert not [f for f in os.listdir(os.path.join(D, "logs", "tasks")) if f.startswith(S.safe(sp["key"]))]
    ok(f"drop: its exclusive inputs, outputs and logs are removed, empty parents removed; another task's ({tk}) views are untouched")

    for bad in ("/etc", "a/../../x", "in"):
        try:
            ST.safe_rel(bad)
            raise AssertionError(bad)
        except relay.RN.RelayError:
            pass
    ok("paths to remove or build stay inside the data dir and are at least two levels deep")


def other_disk(R):
    """A temp dir not on R's disk (/dev/shm); None when unavailable."""
    if not os.path.isdir("/dev/shm"):
        return None
    d = tempfile.mkdtemp(prefix="test_relay_", dir="/dev/shm")
    if os.stat(d).st_dev == os.stat(R).st_dev:
        shutil.rmtree(d)
        return None
    return d


def test_status_blob_cap():
    """Status: blobs on another disk with blob_cap → blob_free = min(free on that disk, blob_cap − blobs used); without it → free
    on that disk."""
    B0 = os.path.join(RROOT, "blobs")
    shm = other_disk(RROOT)
    if not shm:
        print("SKIP: no other disk available (/dev/shm)")
        return
    arg = dict(sched="pueue", pueue="/bin/false", cap=None)
    saved = B0 + ".save"
    os.rename(B0, saved)
    try:
        os.makedirs(os.path.join(shm, "xx"))
        with open(os.path.join(shm, "xx", "xx1"), "wb") as f:
            f.write(b"x" * 3_000_000)
        os.symlink(shm, B0)
        used_b = os.lstat(os.path.join(shm, "xx", "xx1")).st_blocks * 512
        st = RM.op(NAME, "status", dict(arg, blob_cap=0.002))
        assert st["blob_free"] == 0 and st["blob_dev"] != st["dev"] and st["sched_raw"] is None, st
        st = RM.op(NAME, "status", dict(arg, blob_cap=1))
        assert st["blob_free"] == min(shutil.disk_usage(shm).free, int(1e9) - used_b), (st["blob_free"], used_b)
        st = RM.op(NAME, "status", arg)
        assert abs(st["blob_free"] - shutil.disk_usage(shm).free) < 1e6, st["blob_free"]
    finally:
        os.remove(B0)
        os.rename(saved, B0)
        shutil.rmtree(shm)
    ok("status: blobs on another disk with blob_cap → blob_free is blob_cap minus blobs used; without it → free on that disk")


def test_env_remote():
    """End to end (fake ssh): status reads env marker files; a pre-build space check writes .building / .failed (by cap);
    ensure_blobs puts store files into blobs."""
    EV = relay.EV
    with mpatch(NAME, envs={"base": ("spec", "cu128")}):
        h = EV.vhash("base", "cu128")
        r = EV.mark(NAME, "base", h, "reserve", need=1)
        sd = os.path.join(RROOT, proto.ROOT["envs_ok"])
        assert r["ok"] and open(os.path.join(sd, "base.building")).read().split()[0] == h, r
        rs = relay.remote_status(NAME)
        assert rs["envs_st"]["base"]["building"].startswith(h) and not rs["envs_st"]["base"]["active"] and rs["now"] > 0, rs["envs_st"]
        EV.mark(NAME, "base", h, "clear")
        assert not os.path.exists(os.path.join(sd, "base.building"))
        r = EV.mark(NAME, "base", h, "reserve", need=10 ** 18)
        fl = open(os.path.join(sd, "base.failed")).read().split()
        assert not r["ok"] and fl[0] == h and fl[2] == "nospace" and not os.path.exists(os.path.join(sd, "base.building")), (r, fl)
        with mpatch(NAME, cap=1e-9):
            assert not EV.mark(NAME, "base", h, "reserve", need=10 ** 6)["ok"]
        rs = relay.remote_status(NAME)
        st = EV.classify(rs["envs_st"], {"base": "cu128"}, rs["now"])
        assert st == {"base": "no space"}, st
        os.remove(os.path.join(sd, "base.failed"))
    ok("remote side: writes .building or .failed nospace based on required space (with cap, cap minus what is used); status reads it back")
    src = os.path.join(T, "envblob.bin")
    open(src, "wb").write(b"wheel-bytes")
    hh = SO.hashes(HROOT, [src])[src]
    assert hh == relay.C.sha256(src) and SO.hashes(HROOT, [src]) == {src: hh}
    said = []
    got = RT.ensure_blobs(NAME, {hh: [None, os.path.getsize(src), src]}, say=said.append)
    assert got == {hh: f"{RM.root(NAME)}/{proto.blob(hh)}"} and open(os.path.join(RROOT, proto.blob(hh)), "rb").read() == b"wheel-bytes"
    said.clear()
    assert RT.ensure_blobs(NAME, {hh: [None, os.path.getsize(src), src]}, say=said.append) == got
    assert any("0 missing over there" in m for m in said), said
    ok("ensure_blobs: puts store files into remote blobs, transfers nothing already present, returns remote paths")


def test_env_auto():
    """Auto-build envs from their spec: an unbuilt one queues a hub task (base env first, one at a time per machine); do not
    requeue while one is queued on the hub or being built remotely; with the switch off, only log; no space or a broken build
    alerts site:<machine>:env:<env>; a down master connection is not contacted."""
    EV = relay.EV
    adds, logs = [], []
    hub = M.hub()
    now = 2_000_000_000
    s = {"pueue": {hub: {"tasks": []}}}
    lab = lambda env: f"{proto.ENV_BUILD}:{NAME}:{env}"  # noqa: E731
    with patched(relay.refill, add=lambda host, lbl, step, k, args, **kw: adds.append((host, lbl, step, k, args, kw)) or 77), \
            patched(relay, log=lambda lines: logs.extend(lines)), mpatch(NAME, envs={"base": ("spec", "cu128"), "alt": "spec"}), \
            pdict(relay.P.AUTO, envs=True):
        st = {}
        msgs = relay.env_builds(s, NAME, {}, now, False, st)
        assert len(adds) == 1 and not st, (adds, st)
        host, lbl, step, k, args, kw = adds[0]
        assert host == hub and lbl == lab("base") and step == proto.ENV_BUILD and k == f"{NAME}:base" and kw == dict(to=NAME, group=proto.IO_GROUP)
        assert args.startswith(f"{proto.IO_GROUP} -p --label {lab('base')} -- ") and f"envs_cli.py env {NAME} base --variant cu128" in args, args
        assert msgs and "base" in msgs[0]
        ok(f"not built: queue taskq site env in the hub {proto.IO_GROUP} group (base env first, variants per entry), one at a time")
        adds.clear()
        relay.env_builds({"pueue": {hub: {"tasks": [dict(id=3, label=lab("base"), status="Running")]}}}, NAME, {}, now, False, {})
        hb = EV.vhash("base", "cu128")
        relay.env_builds(s, NAME, {"base": dict(building=f"{hb} {now - 60}")}, now, False, {})
        relay.env_builds({"pueue": {hub: {"error": "x"}}}, NAME, {}, now, False, {})
        assert adds == [], adds
        ok("already queued on the hub, remote .building is fresh, or hub pueue unreadable: do not requeue")
        relay.env_builds(s, NAME, {"base": dict(ok=hb, active=True)}, now, False, {})
        assert len(adds) == 1 and adds[0][1] == lab("alt") and "--variant default" in adds[0][4], adds
        adds.clear()
        ok("build the venv on top of the base env only after the base env is ready")
        st = {}
        relay.env_builds(s, NAME, {"base": dict(failed=f"{hb} {now - 60} nospace 5000000000 1000000000")}, now, False, st)
        a = st.get(f"site:{NAME}:env:base")
        assert adds == [] and a and proto.alert_data(a)["state"] == "no space" and "5.0 GB" in proto.alert_msg(a), st
        st = {}
        relay.env_builds(s, NAME, {"base": dict(failed=f"{hb} {now - 60} rc 1", log="...\npip broke\n")}, now, False, st)
        assert adds == [] and "pip broke" in proto.alert_msg(st[f"site:{NAME}:env:base"]), st
        ok("no space or broken build: do not requeue, alert site:<machine>:env:<env> (with required/free space and the last log lines)")
        relay.P.AUTO["envs"] = False
        st = {}
        relay.env_builds(s, NAME, {}, now, False, st)
        relay.env_builds(s, NAME, {}, now, False, st)
        assert adds == [] and not st and len([x for x in logs if "(switch off)" in x]) == 1, logs
        assert relay.env_builds(s, NAME, {}, now, True, {}) and adds == []
        ok("AUTO['envs'] off: no queueing, no alerts, log once to ~/relay.log; dry only says what would be built")
        relay.P.AUTO["envs"] = {NAME: ["alt"]}
        M.MACHINES[NAME]["envs"] = {"base": ("conda", "x")}
        relay.env_builds(s, NAME, {}, now, False, {})
        assert [x[1] for x in adds] == [lab("base")] and "--variant default" in adds[0][4], adds
        adds.clear()
        ok("AUTO['envs'] = {machine: [env]}: prebuild entries that are not spec-based too (their venv uses the spec-built base env, base first)")
        M.MACHINES[NAME]["envs"] = {"base": ("spec", "cu128")}
        relay.P.AUTO["envs"] = True
        ts = {"ok": {hub: True}, "taskq": {Q: {"tasks": []}}, "pueue": {hub: {"tasks": []}}}
        with patched(relay, remote_status=lambda n: dict(envs_st={}, now=now, bundle="bX"), wanted_bundle=lambda: ("bX", "/nonexistent"),
                     write_status=lambda *a: None, plan=lambda *a: ([], {})), patched(ST, gc=lambda *a, **kw: None):
            relay.prepare(ts, NAME, False)
        assert [x[1] for x in adds] == [lab("base")], adds
        adds.clear()
        open(os.path.join(T, "master_down"), "w").close()
        try:
            c = relay.prepare(ts, NAME, False)
            assert c["rs"] is None and adds == []
            assert EV.read_rows(NAME, ["base"]) is None
        finally:
            os.remove(os.path.join(T, "master_down"))
        ok("each check also builds envs while reading status (even without tasks using them); with the ssh master down: unreadable, no build, no new connection")


def test_run_lines():
    """Stream stderr while running: every line goes to say and into the returned stderr (no racing with the stdout reader);
    stdin is fed and stdout is complete; timeout → 124."""
    prog = ("import sys\nd = sys.stdin.read()\nfor i in range(3000):\n    print(f'progress {i}', file=sys.stderr, flush=True)\n"
            "print('pull: last line', file=sys.stderr)\nprint(d * 2)\nsys.exit(3)\n")
    for _ in range(5):
        said = []
        rc, out, err = RM.run_lines([sys.executable, "-c", prog], "ab", 60, said.append)
        lines = err.splitlines()
        assert rc == 3 and out == "abab\n" and len(lines) == 3001 and lines[-1] == "pull: last line" and said == lines, (rc, out, len(lines))
    rc, out, err = RM.run_lines([sys.executable, "-c", "import time; time.sleep(30)"], "", 1, lambda m: None)
    assert rc == 124 and "timed out after 1 s" in err, (rc, err)
    ok("run_lines: no stderr line is lost (say gets the same as the return value), stdin is fed, stdout is complete; timeout → 124")


def test_err_tail():
    """Remote failure output: when the error line is pushed out of the last 300 chars by later progress, put it in front;
    when it is in the tail, return only the tail."""
    prog = "\n".join(f"tunnel: {i} MB/s" for i in range(100))
    tb = "Traceback (most recent call last):\n  File \"x.py\", line 1\nFileNotFoundError: [Errno 2] no a.dat"
    for text, want in ((tb + "\n" + prog, "FileNotFoundError: [Errno 2] no a.dat"), ("pull: 3 files were not fetched\n" + prog, "pull: 3 files were not fetched"),
                       ("错误: disk full\n" + prog, "错误: disk full"), ("srun: error: node gone\n" + prog, "srun: error: node gone")):
        got = RM.err_tail(text, own=("pull: ",))
        assert got.startswith(want + " …… ") and got.endswith(prog[-100:]) and len(got) <= len(want) + 306, got
    assert RM.err_tail(prog) == prog[-300:] and RM.err_tail("x\n" + tb) == ("x\n" + tb)[-300:]
    assert not RM.err_tail("pull: not fetched\n" + prog).startswith("pull:")  # not this operation's prefix: do not match
    ok("remote failure output: the last error line (last Traceback line, Chinese word for error, <program>: error, site_ops <op>:) is put in front when progress pushes it out, then the tail")

    def op_fail(*a, **kw):
        raise RuntimeError("site_ops failed on that machine (1): x")
    with patched(RT, lookup=lambda k: dict(key=k, out="out/k", done=[], recipe="toy", params={})), \
            patched(ST, keys_of=lambda st, n: {"k": dict(outs=["out/k"])}), \
            patched(RM, op=lambda *a, **kw: dict(done=True, bad=[], items=[], links=[]), store_op=op_fail):
        try:
            RT.fetch(NAME, "k", say=lambda m: None)
            raise AssertionError("a failed store pull should raise RelayError")
        except RT.RN.RelayError as e:
            assert "to the store" in str(e) and "site_ops failed" in str(e), e
    with patched(RT, lookup=lambda k: dict(key=k, out="out/k", done=[], recipe="toy", params={})), \
            patched(ST, keys_of=lambda st, n: {"k": dict(outs=["out/k"])}), patched(RM, op=op_fail):
        try:
            RT.fetch(NAME, "k", say=lambda m: None)
            raise AssertionError("failing to list remote outputs should raise RelayError")
        except RT.RN.RelayError as e:
            assert "could not list outputs of k" in str(e), e
    ok("fetch: failing to list remotely or pull on the store raises RelayError (like not done or no record), so callers handle one error")


# ---------------- 4. single site_ops operations ----------------
def run_op(what, R, data, wait=True):
    """Run one site_ops.py operation locally (root R) → (exit code, result, stderr); wait=False → the process."""
    argv = [sys.executable, os.path.join(HERE, "site_ops.py"), what]
    env = dict(os.environ, TASKQ_ROOT=R)
    if not wait:
        p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        p.stdin.write(json.dumps(data))
        p.stdin.close()
        return p
    r = subprocess.run(argv, input=json.dumps(data), capture_output=True, text=True, env=env)
    return r.returncode, S.json_in(r.stdout), r.stderr


def put_blob(R, body):
    """Put a blob into R's blobs (record size and mtime, as after ingest) → sha256."""
    import hashlib
    h = hashlib.sha256(body).hexdigest()
    p = os.path.join(R, proto.blob(h))
    TN.wb(p, body)
    SO.record(R, h)
    return h


def blocked_on_lock(pid):
    """This process is waiting for a lock in /proc/locks (a "->" line)."""
    with open("/proc/locks") as f:
        return any(ln.split()[1] == "->" and ln.split()[5] == str(pid) for ln in f if len(ln.split()) > 5)


def test_view_lock():
    """Only one view builds over there at a time (two parts of one dataset building views at once would delete each other's
    .incomplete)."""
    import fcntl
    R = fixture.tmpdir(dir=T, prefix="vl_")
    os.makedirs(os.path.join(R, "state"))
    lk = open(os.path.join(R, proto.ROOT["view_lock"]), "w")
    fcntl.flock(lk, fcntl.LOCK_EX)
    p = run_op("view", R, dict(views=["prepared/D1"], links={}, texts={"prepared/D1/a.json": "e30="}), wait=False)
    t0 = time.time()
    while p.poll() is None and time.time() - t0 < 30 and not blocked_on_lock(p.pid):
        time.sleep(0.05)
    waited = p.poll() is None and blocked_on_lock(p.pid)
    fcntl.flock(lk, fcntl.LOCK_UN)
    lk.close()
    p.wait(timeout=30)
    d = os.path.join(R, "data", "prepared/D1")
    assert waited and p.returncode == 0 and open(os.path.join(d, "a.json")).read() == "{}" and not os.path.exists(os.path.join(d, proto.INCOMPLETE))
    ok("view build: waits for the lock while another builds, then removes .incomplete")


def test_view_no_overwrite():
    """A view never overwrites a real file: another real file at a path (output written over there) → fail, leave the file
    alone, keep .incomplete (not built); if it is the blob itself (same inode, previous step's output ingested on the same disk)
    → leave it and build as usual. Missing blob → exit without placing anything."""
    R = fixture.tmpdir(dir=T, prefix="vo_")
    D = os.path.join(R, "data")
    h, h2 = put_blob(R, b"input-a"), put_blob(R, b"input-b")
    links = {"prepared/D1/a.dat": h}
    assert run_op("view", R, dict(views=["prepared/D1"], links=links, texts={}))[:2] == (0, 1)
    TN.wb(os.path.join(D, "prepared/D1/out.dat"), b"real output")
    rc, _, err = run_op("view", R, dict(views=["prepared/D1", "prepared/D2"], links=dict(links, **{"prepared/D1/out.dat": h2, "prepared/D2/b.dat": h2}),
                                        texts={}))
    assert rc != 0 and "not overwriting" in err and "out.dat" in err, (rc, err)
    assert open(os.path.join(D, "prepared/D1/out.dat"), "rb").read() == b"real output" and not os.path.islink(os.path.join(D, "prepared/D1/out.dat"))
    assert os.path.exists(os.path.join(D, "prepared/D1", proto.INCOMPLETE))
    assert sorted(os.listdir(os.path.join(D, "prepared/D2"))) == ["b.dat"], os.listdir(os.path.join(D, "prepared/D2"))
    shutil.rmtree(os.path.join(D, "prepared/D2"))
    os.remove(os.path.join(D, "prepared/D1/out.dat"))
    os.link(os.path.join(R, proto.blob(h2)), os.path.join(D, "prepared/D1/out.dat"))
    assert run_op("view", R, dict(views=["prepared/D1"], links=dict(links, **{"prepared/D1/out.dat": h2}), texts={}))[0] == 0
    assert not os.path.islink(os.path.join(D, "prepared/D1/out.dat")) and not os.path.exists(os.path.join(D, "prepared/D1", proto.INCOMPLETE))
    ok("a view does not overwrite real files: another real file there → fail, leave it, that view is not built (keeps .incomplete), while other views in the same run build and "
       "lose .incomplete; if it is the blob itself (hard link) → leave it")

    before = walk(os.path.join(D, "prepared/D1"))
    rc, _, err = run_op("view", R, dict(views=["prepared/D1"], links=dict(links, **{"prepared/D1/b.dat": "cd" + "0" * 62}),
                                        texts={"prepared/D1/x.json": "e30="}))
    assert rc != 0 and "missing blob" in err, (rc, err)
    assert walk(os.path.join(D, "prepared/D1")) == before, walk(os.path.join(D, "prepared/D1"))
    ok("view build with a missing blob: exit without writing .incomplete or changing the old view (restage rebuilds it)")


def test_export_plugin_error():
    """A plugin raises another error during export on the store (cannot read a file): report one line (plugin, function,
    error) without a traceback."""
    R, D = fixture.tmpdir(dir=T, prefix="ex_"), fixture.tmpdir(dir=T, prefix="exd_")
    rc, res, err = run_op("export", R, dict(spec=S.resolve("toy", dict(ds="D404_none", frag="0")), data=D))
    assert rc != 0 and res is None and "Traceback" not in err and len(err.strip().splitlines()) == 1, (rc, err)
    assert "plugin toy export_toy failed: FileNotFoundError" in err and "dataset.json" in err, err
    ok("export when a plugin cannot read a file: one line (plugin, function, error), no traceback")


def test_list():
    """List results before fetching: the done decision (string, a|b, missing item, empty done); symlinks become logical
    paths; an unreadable subdir → error."""
    R = fixture.tmpdir(dir=T, prefix="ls_")
    D = os.path.join(R, "data")
    out = os.path.join(D, "out", "k")
    os.makedirs(os.path.join(out, "check"))
    os.makedirs(os.path.join(out, "part"))
    open(os.path.join(out, "check", "DONE"), "w").close()
    open(os.path.join(out, "part", "a"), "w").close()
    spec = dict(key="k", out="out/k", done=["check/DONE"])

    def run(outs, s=spec, links=None):
        o = S.outputs(s)
        return run_op("list", R, dict(out=s["out"], done=o["done"], exclude=o["exclude"], extra=o["extra"], outs=outs, key="k", links=links or {}))
    rc, res, err = run(["out/k", "out/none"])
    assert rc == 0 and res["done"] and sorted(i[0] for i in res["items"]) == ["out/k/check/DONE", "out/k/part/a"], (rc, res, err)
    for done, want in (("check/DONE", True), (["x|check/DONE"], True), (["check/DONE", "x"], False), ([], True)):
        res = run(["out/k"], dict(spec, done=done))[1]
        assert res["done"] is want, (done, res)
    ok("list before fetch: the done decision matches spec.is_done (string, a|b, missing item, empty done); missing output dirs are skipped")

    h, h2 = put_blob(R, b"in"), put_blob(R, b"stray")
    TN.wb(os.path.join(D, "prepared/D1/a.dat"), b"")
    os.remove(os.path.join(D, "prepared/D1/a.dat"))
    os.symlink(os.path.join(R, proto.blob(h)), os.path.join(D, "prepared/D1/a.dat"))
    os.symlink(os.path.join(R, proto.blob(h)), os.path.join(out, "l_blob"))
    os.symlink("../../prepared/D1/a.dat", os.path.join(out, "l_view"))
    os.symlink("check/DONE", os.path.join(out, "l_own"))
    os.symlink("part", os.path.join(out, "l_dir"))
    os.symlink(os.path.join(R, proto.blob(h2)), os.path.join(out, "l_stray"))
    os.symlink("/etc/hostname", os.path.join(out, "l_out"))
    os.symlink("/etc/hostname", os.path.join(out, "l_skip.tmp"))  # excluded names (exclude, half-written): skip links too, not bad
    os.symlink("part", os.path.join(out, "x_dir"))
    rc, res, err = run(["out/k"], dict(spec, fetch=dict(exclude=["x_*"])), links={"prepared/D1/a.dat": h})
    assert rc == 0 and sorted(res["links"]) == [["out/k/l_blob", "prepared/D1/a.dat"], ["out/k/l_dir", "out/k/part"],
                                                 ["out/k/l_own", "out/k/check/DONE"], ["out/k/l_view", "prepared/D1/a.dat"]], (res, err)
    assert sorted(res["bad"]) == ["out/k/l_out", "out/k/l_stray"] and not any(i[0].startswith("out/k/l_") for i in res["items"]), res
    for f in ("l_blob", "l_view", "l_own", "l_dir", "l_stray", "l_out", "l_skip.tmp", "x_dir"):
        os.remove(os.path.join(out, f))
    ok("list before fetch: output symlinks become logical paths (blob links map back through this task's views to the input path; data-dir links by path); "
       "other blobs and paths outside the data dir → bad; excluded or half-written names are filtered first and their links not fetched")

    os.chmod(os.path.join(out, "part"), 0)
    try:
        rc, res, err = run(["out/k"])
    finally:
        os.chmod(os.path.join(out, "part"), 0o755)
    assert rc != 0 and res is None, (rc, res, err)
    ok("list before fetch: unreadable subdir → error (no fetch, not done)")


def test_logs_exact():
    """Fetch and drop only take this task's own part logs: key a/b does not take a/b/c's."""
    R = fixture.tmpdir(dir=T, prefix="lg_")
    L = os.path.join(R, "data", proto.TASK_LOGS)
    os.makedirs(L)
    mine = ["X__tr__123_part1.log", "X__tr__w7e2837_part2.log", "X__tr__gpuhost-202611041200_part1.log"]
    other = ["X__tr__0__123_part1.log", "X__tr__0__w7e2837_part1.log", "X__trx__1_part1.log"]
    for f in mine + other:
        open(os.path.join(L, f), "w").close()
    os.makedirs(os.path.join(R, "data", "out", "X"))
    rc, res, err = run_op("list", R, dict(out="out/X", done=[], exclude=[], extra=[], outs=["out/X"], key="X/tr"))
    assert rc == 0 and sorted(os.path.basename(i[0]) for i in res["items"]) == sorted(mine), (rc, res, err)
    rc, n, err = run_op("drop", R, dict(paths=[], logs=S.safe("X/tr")))
    assert rc == 0 and sorted(os.listdir(L)) == sorted(other), (rc, err, os.listdir(L))
    ok("part logs are recognized as <key>__<job>_part<N>.log: logs of a key that extends the prefix (a/b and a/b/c) are not fetched or removed")


def test_ingest():
    """Ingest into blobs: same disk with matching size and sha256 → hard link in and record; when blobs are on another disk, do
    not read the source or compute sha256, just skip. When a hard-link source was modified in place the blob changed too:
    missing notices it, drops the record and blob (source untouched), and a later ingest checks the new content."""
    import hashlib
    body = os.urandom(5000)
    h = hashlib.sha256(body).hexdigest()
    R = fixture.tmpdir(dir=T, prefix="ing_")
    src = os.path.join(R, "data", "res", "ck.pth")
    TN.wb(src, body)
    srcs = [os.path.join(R, "data")]
    assert run_op("ingest", R, dict(srcs=srcs, files={h: ["res/ck.pth", 4999]}))[:2] == (0, [])
    assert run_op("ingest", R, dict(srcs=srcs, files={"ab" * 32: ["res/ck.pth", 5000]}))[:2] == (0, [])
    assert run_op("ingest", R, dict(srcs=srcs, files={h: [None, 5000]}))[:2] == (0, [])
    rc, got, err = run_op("ingest", R, dict(srcs=srcs, files={h: ["res/ck.pth", 5000]}))
    b = os.path.join(R, proto.blob(h))
    assert rc == 0 and got == [h] and os.path.samefile(b, src) and os.path.isfile(b + proto.BLOB_STAT), (rc, got, err)
    assert run_op("missing", R, [h])[:2] == (0, [])
    ok("ingest: same disk with matching size and sha256 is hard-linked in and size/mtime recorded; size or sha256 mismatches and missing relative paths are not taken")

    with open(src, "r+b") as f:  # modify the source in place (same size): the blob is the same inode and changes too
        f.write(b"Z" * 10)
    rc, miss, err = run_op("missing", R, [h])
    assert rc == 0 and miss == [h] and "dropping it to re-ingest" in err, (rc, miss, err)
    assert not os.path.lexists(b) and not os.path.lexists(b + proto.BLOB_STAT) and open(src, "rb").read(10) == b"Z" * 10
    assert run_op("ingest", R, dict(srcs=srcs, files={h: ["res/ck.pth", 5000]}))[:2] == (0, [])  # content no longer matches h: skip (the store resends)
    TN.wb(src, body)
    assert run_op("ingest", R, dict(srcs=srcs, files={h: ["res/ck.pth", 5000]}))[:2] == (0, [h])
    assert run_op("missing", R, [h])[:2] == (0, []) and open(b, "rb").read() == body
    ok("a hard-link source modified in place: missing notices it and drops the blob and record (source untouched); mismatched content is not taken, matching content is ingested again")

    R2 = fixture.tmpdir(dir=T, prefix="ing2_")
    shm = other_disk(R2)
    if not shm:
        print("SKIP: no other disk available (/dev/shm)")
        return
    try:
        os.symlink(shm, os.path.join(R2, "blobs"))
        src2 = os.path.join(R2, "data", "res", "ck.pth")
        TN.wb(src2, body)
        os.chmod(src2, 0)  # reading it fails
        try:
            rc, got, err = run_op("ingest", R2, dict(srcs=[os.path.join(R2, "data")], files={h: ["res/ck.pth", 5000]}))
        finally:
            os.chmod(src2, 0o644)
        assert rc == 0 and got == [] and not os.listdir(shm), (rc, got, err)
    finally:
        shutil.rmtree(shm)
    ok("ingest: with blobs on another disk, do not read the source or compute sha256, just skip (the store sends it)")


def test_pin_refresh():
    """While staging, rewrite the pin periodically (remote cleanup honours pins for 12 hours) and clear it when done."""
    import threading
    pins = []
    done = threading.Event()
    ex = dict(params={}, views=[], outs=[], cleanup=[], texts={}, files=[["a/b.dat", "h1", 1, "a/b.dat", "/x/b.dat"]])
    with patched(RT, PIN_EVERY=0.05, lookup=lambda key: {}, in_use=lambda name: [],
                 _stage=lambda *a: (time.sleep(0.4), done.set(), "k")[2]), \
            patched(RM, op=lambda name, what, data=None, **kw: pins.append(data["hashes"]) if what == "pin" else None,
                    store_op=lambda what, data, **kw: ex if what == "export" else None):
        assert RT.stage(NAME, "k", 0) == "k"
    assert done.is_set() and len(pins) >= 4 and all(p == ["h1"] for p in pins[:-1]) and pins[-1] is None, pins
    n = len(pins)
    time.sleep(0.2)
    assert len(pins) == n, "no more writes after staging ends"
    ok("the pin is rewritten while staging, cleared when done and not written again")


# ---------------- 5. pushing bundles ----------------
def test_deploy_cleanup():
    """Prune old bundles: keep current, the newest 3, anything touched within 4 days (a running worker touches its bundle
    every loop), and anything touched since an hour before the earliest claim among tasks still claimed (after a worker
    switches bundles, tasks it carries on still use the bundle from claim time); claims=None (unknown) keeps all old bundles;
    temp files left by interrupted deploys are removed after a day."""
    src = os.path.join(T, "dk_src")
    TN.wb(os.path.join(src, "f"), b"x")
    old = time.time() - 10 * 86400
    assert relay.claim_times(None) is None and relay.claim_times(dict(bundle="x")) is None
    assert relay.claim_times(dict(taskq=dict(tasks=[dict(since=5.0), dict(since=None), dict()]))) == [5.0]
    for touched, claims, want in ((None, [], {"n", "c", "d"}), ("a", [], {"n", "c", "d", "a"}), (None, [old + 1800], {"n", "c", "d", "a"}),
                                  (None, [time.time() - 600], {"n", "c", "d"}), (None, None, {"n", "a", "b", "c", "d"})):
        r = fixture.tmpdir(dir=T, prefix="dk_")
        with mpatch(NAME, root=r):
            for i, d in enumerate("abcd"):
                os.makedirs(os.path.join(r, "code", d, BQ))
                t = old - 2 * 86400 * i if d in "ab" else time.time() - 3600 * (4 - i)
                t = time.time() - 86400 if d == touched else t
                os.utime(os.path.join(r, "code", d), (t, t))
            for d, t in (("a.tmp.1", old), ("a.tmp.1.tar", old), ("a.tmp.2", time.time())):
                os.makedirs(os.path.join(r, "code", d)) if not d.endswith(".tar") else open(os.path.join(r, "code", d), "w").close()
                os.utime(os.path.join(r, "code", d), (t, t))
            relay.deploy(NAME, "n", src, claims)
            got = set(os.listdir(os.path.join(r, "code"))) - {"current"}
            assert got == want | {"a.tmp.2"} and os.readlink(os.path.join(r, "code", "current")) == "n", (touched, claims, got)
    ok("prune: keep current, the newest 3, anything touched within 4 days, and anything touched since an hour before the earliest claim (old bundles for tasks carried on after a switch); "
       "keep all when claims are unknown; remove temp files from interrupted deploys after a day")


def test_deploy_safety():
    """Push bundles: a partial transfer installs nothing and leaves current alone; re-pushing an existing old bundle does not
    delete it in the same run; concurrent pushes leave current pointing at a complete bundle."""
    import tarfile
    import threading
    src = os.path.join(T, "ds_src")
    n_files = 200
    for i in range(n_files):
        TN.wb(os.path.join(src, "d", f"f{i}"), os.urandom(4096))
    full = lambda r, b: sum(len(fs) for _, _, fs in os.walk(os.path.join(r, "code", b)))  # noqa: E731
    tb = subprocess.run(["tar", "-C", src, "-cf", "-", "."], capture_output=True, check=True).stdout
    at = tarfile.open(fileobj=io.BytesIO(tb)).getmembers()[100].offset  # break between two files: tar does not complain
    cut = os.path.join(T, "cut_ssh")
    with open(cut, "w") as f:
        f.write(f"#!/bin/bash\nhead -c {at} | {FAKE} \"$@\"\n")
    os.chmod(cut, 0o755)
    r = fixture.tmpdir(dir=T, prefix="ds_")
    with mpatch(NAME, root=r):
        relay.deploy(NAME, "good", src)
        os.environ["RELAY_SSH"] = cut
        try:
            relay.deploy(NAME, "half", src)
            raise AssertionError("a partial transfer should fail")
        except RuntimeError:
            pass
        finally:
            os.environ["RELAY_SSH"] = FAKE
        assert os.readlink(os.path.join(r, "code", "current")) == "good" and not os.path.exists(os.path.join(r, "code", "half")), \
            os.listdir(os.path.join(r, "code"))
        ok("push bundle: partial transfer → fail, install nothing, leave current alone")

        old = time.time() - 10 * 86400
        for i, d in enumerate("abcd"):
            os.makedirs(os.path.join(r, "code", d))
            t = old if d == "a" else time.time() - 600 * (4 - i)
            os.utime(os.path.join(r, "code", d), (t, t))
        relay.deploy(NAME, "a", src)
        assert os.readlink(os.path.join(r, "code", "current")) == "a" and os.path.isdir(os.path.join(r, "code", "a")), \
            os.listdir(os.path.join(r, "code"))
        ok("re-push an old bundle already over there (rollback): switch to it and do not prune it in the same run")

    r = fixture.tmpdir(dir=T, prefix="ds_")
    with mpatch(NAME, root=r):
        errs = []

        def go(b):
            try:
                relay.deploy(NAME, b, src)
            except Exception as e:  # noqa: BLE001
                errs.append(e)
        for _ in range(3):
            th = [threading.Thread(target=go, args=(b,)) for b in ("x", "x", "y", "y")]
            [t.start() for t in th]
            [t.join() for t in th]
            cur = os.readlink(os.path.join(r, "code", "current"))
            assert not errs and full(r, cur) == n_files and all(full(r, b) == n_files for b in "xy"), (errs, cur, full(r, cur))
    ok("concurrent pushes (same and different bundles): all complete, current points at a complete bundle")


if __name__ == "__main__":
    test_drop_refused()
    test_beat_lost()
    test_refcount()
    test_fresh_site()
    test_paused()
    test_held_by()
    test_plan()
    test_assign()
    test_pueue_worker()
    test_slurm_topup_ready()
    test_tilde_root()
    test_adapters_in_relay()
    test_place()
    test_run_lines()
    test_err_tail()
    test_e2e_path(test_e2e())
    test_status_blob_cap()
    test_env_remote()
    test_env_auto()
    test_view_lock()
    test_view_no_overwrite()
    test_export_plugin_error()
    test_list()
    test_logs_exact()
    test_ingest()
    test_pin_refresh()
    test_deploy_cleanup()
    test_deploy_safety()
    shutil.rmtree(T)
    shutil.rmtree(STATE_DIR)
    print("all passed")
