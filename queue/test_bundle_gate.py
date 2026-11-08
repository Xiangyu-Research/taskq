#!/usr/bin/env python3
"""Tests for bundle_gate.py: build bundles from the worktree and an old commit, run the specs the hub queue would send
(testdata's experiment table, the command recipe) through the gate: runnable ones pass, others are blocked; an unknown bundle or
one absent on the hub passes; a timeout passes and is not cached; relay.plan holds and reports bundlegate, then sends after a bundle
switch; relay.assign skips executors whose bundle cannot run it; stage checks once more before sending. Temp dirs are removed.
Checks for the old bundle are skipped in a shallow clone without the old commit.
    python3 queue/test_bundle_gate.py
"""
import contextlib
import os
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
TMP = fixture.tmpdir(prefix="bundle_gate_")
os.environ["TASKQ_STATE"] = os.path.join(TMP, "state")
sys.path.insert(0, HERE)
import cfg  # noqa: E402

cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import proto  # noqa: E402
import spec as S  # noqa: E402
import experiments as E  # noqa: E402
import relay_bundle as RB  # noqa: E402
import relay_store as ST  # noqa: E402
import bundle_gate as BG  # noqa: E402
import relay  # noqa: E402

NAME = next(n for n in M.executors() if M.machine(n)["scheduler"] == "slurm")
PNAME = next((n for n in M.executors() if M.machine(n)["scheduler"] == "pueue"), None)
OUT = os.path.expanduser(RB.OUT)
OLD = "c5dace4"  # old core: its spec format differs (inputs were called data)
CMD = S.sendable(S.resolve("command", dict(key="job/a", cmd="echo hi > {data}/out/a/x", env="pt", out="out/a", done="x", relay="path")))
TMPL = dict(CMD, params=dict(CMD["params"], note="{other}"))  # a {…} left in params for the recipe itself: current bundles do not expand it, the fake picky() bundle blocks it


def ok(msg):
    print(f"PASS:{msg}")


def have(commit):
    return subprocess.run(["git", "-C", CORE, "cat-file", "-e", f"{commit}^{{commit}}"], capture_output=True).returncode == 0


@contextlib.contextmanager
def patched(obj, **kw):
    """Temporarily replace attributes of obj and put them back on exit."""
    old = {k: getattr(obj, k) for k in kw}
    for k, v in kw.items():
        setattr(obj, k, v)
    try:
        yield
    finally:
        for k, v in old.items():
            setattr(obj, k, v)


def build(commit=None):
    """commit=None: the worktree's user files and core; otherwise both come from commit (the core and the test project share one repo)."""
    bid, path = RB.build(commit, core=commit or RB.CORE_REF)
    if commit and not os.path.exists(os.path.join(path, "envs")):  # the old core only knew <repo>/envs (a config dir without .taskq.toml)
        os.symlink(RB.conf_rel() or ".", os.path.join(path, "envs"))
    return bid


def test_check(new, old):
    assert OUT.startswith(TMP + "/"), OUT
    specs = {k: S.sendable(t["spec"]) for k, t in E.tasks().items()}
    assert specs
    for k, sp in list(specs.items()) + [("command", CMD), ("tmpl", TMPL)]:
        assert BG.check(sp, new) is None, (k, BG.check(sp, new))
    err = BG.check(dict(CMD, res=dict(CMD.get("res") or {}, bogus=1)), new)
    assert err and "spec.py shell" in err, err
    ok(f"current bundle: all {len(specs)} specs the hub queue would send and the command recipe pass; a resource it does not know -> blocked")
    if old is None:
        print(f"SKIP:no commit {OLD} (shallow clone)")
        return
    err = BG.check(CMD, old)
    assert err and "spec.py shell" in err, err
    bad = [k for k, sp in specs.items() if BG.check(sp, old)]
    assert len(bad) == len(specs), bad
    ok(f"bundle {OLD} (old spec format): every spec sent now is blocked ({len(bad)}/{len(specs)})")
    assert BG.check_all(CMD, [new, old]) == (old, err) and BG.check_all(CMD, [new]) is None
    ok("check_all: reports the first bundle that cannot run it")


def test_pass_through(new):
    for bid in (None, "", "0" * 16, "../x", ".tmp"):
        assert BG.check(TMPL, bid) is None, bid
    assert BG.check({}, new) is None
    assert BG.in_use(NAME, None, None) == [] and BG.in_use(NAME, "a", ["b", "a", ""]) == ["a", "b"]
    if PNAME:
        assert BG.in_use(PNAME, "a", ["b"]) == ["a"]
    ok("unknown bundle, absent on the hub, or no spec -> pass; Slurm checks current plus running workers' bundles, pueue only current")


def test_cache_timeout(new):
    calls, real = [], BG.run
    BG.run = lambda d, sp: calls.append(d) or real(d, sp)
    try:
        sp = dict(CMD, key="job/cache")
        assert BG.check(sp, new) is None and BG.check(sp, new) is None and len(calls) == 1
    finally:
        BG.run = real
    ok("the same bundle and spec run once")
    slow = os.path.join(OUT, "f" * 16)
    os.makedirs(os.path.dirname(os.path.join(slow, BG.SPEC_PY)))
    with open(os.path.join(slow, BG.SPEC_PY), "w") as f:
        f.write("import time\ntime.sleep(5)\n")
    t0, BG.TIMEOUT = BG.TIMEOUT, 0.5
    try:
        assert BG.check(CMD, "f" * 16) is None and ("f" * 16, BG.spec_hash(CMD)) not in BG._CACHE
    finally:
        BG.TIMEOUT = t0
    with open(os.path.join(slow, BG.SPEC_PY), "w") as f:
        f.write("import sys\nsys.exit(3)\n")
    err = BG.check(CMD, "f" * 16)
    assert err and "exited 3" in err, err
    assert BG.main(["0" * 16]) == 0
    ok("timeout -> pass and do not cache (run again next time); wrong exit code -> block; command line: absent bundle passes")


def test_current():
    """current there plus bundles touched within STALE_BEAT (fake ssh runs locally); no bundle yet -> ""; unreachable -> None."""
    fake, root = os.path.join(TMP, "fake_ssh"), os.path.join(TMP, "remote")
    with open(fake, "w") as f:
        f.write('#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\n[[ $1 == -O ]] && exit 0\nshift\nexec bash -c "$*"\n')
    os.chmod(fake, 0o755)
    conf0, env0 = dict(M.MACHINES[NAME]), os.environ.get("RELAY_SSH")
    M.MACHINES[NAME]["root"] = root
    os.environ["RELAY_SSH"] = fake
    try:
        assert BG.current(NAME) == ("", [])
        code = os.path.join(root, proto.ROOT["code"])
        for d in ("a" * 16, "b" * 16, "." + "c" * 16 + ".tmp1"):
            os.makedirs(os.path.join(code, d))
        old_t = time.time() - proto.STALE_BEAT - 600
        os.utime(os.path.join(code, "b" * 16), (old_t, old_t))
        os.symlink(os.path.join(code, "a" * 16), os.path.join(root, proto.ROOT["current"]))
        assert BG.current(NAME) == ("a" * 16, ["a" * 16]), BG.current(NAME)
        os.utime(os.path.join(code, "b" * 16))
        assert sorted(BG.current(NAME)[1]) == ["a" * 16, "b" * 16]
        with open(fake, "w") as f:
            f.write("#!/bin/bash\nexit 255\n")
        assert BG.current(NAME) is None
    finally:
        M.MACHINES[NAME].clear()
        M.MACHINES[NAME].update(conf0)
        if env0 is None:
            os.environ.pop("RELAY_SSH", None)
        else:
            os.environ["RELAY_SSH"] = env0
    ok("bundles in use there: current plus those touched within STALE_BEAT (temp dirs excluded); no bundle -> \"\"; unreachable -> None")


def S0(mt):
    return dict(taskq={relay.taskq.queue_host(): dict(enabled=True, tasks=mt)}, pueue={M.hub(): dict(tasks=[])}, records=[],
                ok={M.hub(): True}, src={n: dict(tasks={}) for n in M.executors()})


def mtask(key, sp, state="claimed", worker=NAME, nice=5):
    return dict(key=key, state=state, worker=worker, nice=nice, order=1, since=1000.0, note="", spec=dict(sp, key=key), skipped_by=[])


def RS(bid, live=(), *ts):
    return dict(taskq=dict(tasks=list(ts), paused=False), tasks={}, free=500e9, out_used=0, envs_ok={}, bundle=bid, live=list(live))


def ops(acts):
    return sorted((a["op"], a["key"]) for a in acts)


def test_plan(new, old):
    """old: the fake picky bundle, which only blocks specs containing {other}."""
    K = "job/t"
    acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, RS(new), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, RS(None), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    ok("plan: bundle can run it, or unknown -> send as usual")
    for rs in (RS(old), RS(new, [old])):
        acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, rs, {})
        assert ops(acts) == [("beat", K), ("hold", K)] and acts[-1]["bundle"] == old, acts
        assert list(stuck) == [f"bundlegate:{NAME}:{K}"] and proto.alert_data(stuck[f"bundlegate:{NAME}:{K}"])["key"] == K, stuck
    ok("plan: current or a running Slurm worker's bundle cannot run it -> do not send, hold, report bundlegate:<name>:<key>, claim renewed")
    msgs = relay.execute(NAME, [a for a in acts if a["op"] == "hold"], S0([]))
    st = ST.load_state()
    h = BG.held(st, NAME)[K]
    assert h["why"] == proto.HELD["bundle"] == "bundle-incompatible" and h["bundle"] == old and msgs, (h, msgs)
    acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, RS(old), st)
    assert ops(acts) == [("beat", K)] and stuck, acts
    ok("after recording: the same bundle is not recorded again, still reported")
    acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, RS(new), st)
    assert ops(acts) == [("beat", K), ("stage", K), ("unhold", K)] and not stuck, (acts, stuck)
    relay.execute(NAME, [a for a in acts if a["op"] == "unhold"], S0([]))
    assert BG.held(ST.load_state(), NAME) == {}
    ok("bundle switched there -> send, clear held")
    BG.hold(NAME, K, new, "blocked by stage")
    acts, stuck = relay.plan(S0([mtask(K, CMD)]), NAME, RS(new), ST.load_state())
    assert ops(acts) == [("beat", K)] and f"bundlegate:{NAME}:{K}" in stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K, CMD)]), NAME, RS(old), ST.load_state())
    assert ops(acts) == [("beat", K), ("stage", K), ("unhold", K)] and not stuck, (acts, stuck)
    BG.unhold(NAME, K)
    ok("blocked by stage for the spec to send: while that bundle is in use do not stage, keep reporting; stage after a switch")
    if PNAME:
        acts, _ = relay.plan(S0([mtask(K, TMPL, worker=PNAME)]), PNAME, RS(new, [old]), {})
        assert ops(acts) == [("beat", K), ("stage", K)], acts
        ok("pueue executor: only current counts (a worker stops claiming after its code changes)")


def test_assign(new, old):
    """relay.assign (refill.choose's block): do not assign to an executor whose bundle cannot run it, try the next; when none can, why names the bundle. old is the fake picky bundle."""
    K2, K3 = "job/f2", "job/f3"
    env = sorted(M.ready_envs(NAME))[0]  # an env this machine has without an extra build
    cands = [mtask(K2, dict(TMPL, env=env), "free", None, nice=3), mtask(K3, dict(CMD, env=env), "free", None, nice=4)]

    def probe(what, data, **kw):
        return {p: dict(n=1, newest=None, size=1e9) for p in data["pats"]}

    def run(mt, rs):
        out, why = relay.assign(S0(mt), {NAME: dict(rs=rs, st={})}, {}, {})
        return [a["key"] for a in out.get(NAME, []) if a["op"] == "take"], why

    with patched(relay.RM, store_op=probe), patched(relay, relayable=lambda i: True), \
            patched(relay.SG, on=lambda: False), patched(relay.R, ready=lambda name: 2):
        took, why = run(cands, RS(new))
        assert took == [K2, K3], (took, why)
        took, why = run(cands, RS(old))
        assert took == [K3], (took, why)
        took, why = run(cands, RS(new, [old]))
        assert took == [K3], (took, why)
        took, why = run(cands[:1], RS(old))
        assert took == [] and f"the bundle {old} in use cannot run this spec" in why[NAME] and "{other}" in why[NAME], (took, why)
    ok("assign: skip executors whose bundle (current, running Slurm workers') cannot run it, try the next; none can -> why names the bundle")


def test_stage(new):
    """RT.stage: resolve the spec to send from export's params and check once more with the bundle in use there. A fake bundle (spec.py exits 3) cannot run it."""
    import relay_transfer as RT
    k = next(iter(E.tasks()))
    bad = "e" * 16
    os.makedirs(os.path.dirname(os.path.join(OUT, bad, BG.SPEC_PY)), exist_ok=True)
    with open(os.path.join(OUT, bad, BG.SPEC_PY), "w") as f:
        f.write("import sys\nsys.exit(3)\n")
    said, asked = [], []

    def export(what, data, **kw):  # the gate runs before data is touched: only params are needed
        asked.append(what)
        return dict(params=data["spec"]["params"], files=[], views=[], outs=[], texts={}, cleanup=[])
    with patched(BG, current=lambda name: (new, [bad])), patched(RT.RM, store_op=export):
        assert RT.stage(NAME, k, nice=5, say=said.append) is None and asked == ["export"], asked
        h = BG.held(ST.load_state(), NAME)[k]
        assert h["bundle"] == bad and "exited 3" in h["err"] and "not sending yet" in said[-1], (h, said)
        assert ST.keys_of(ST.load_state(), NAME).get(k) is None
        sp = S.sendable(S.resolve(E.tasks()[k]["recipe"], E.tasks()[k]["params"]))
        BG.current = lambda name: (new, [])
        assert BG.check_all(sp, RT.in_use(NAME)) is None
        BG.current = lambda name: None
        assert RT.in_use(NAME) == []
    BG.unhold(NAME, k)
    ok("stage: check again before sending; cannot run -> do not transfer, do not add, hold (retry next check); unreachable -> no check")


def picky():
    """Fake bundle: specs with {other} cannot run (spec.py exits 2), others can. plan and assign tests need a bundle that blocks only some specs."""
    bid = "f" * 16
    p = os.path.join(OUT, bid, BG.SPEC_PY)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        f.write("import sys\nif '{other}' in sys.argv[-1]:\n    sys.exit('unknown {other}')\n"
                f"sys.exit({proto.EXIT['done_no']} if sys.argv[1] == 'done' else 0)\n")
    return bid


def main():
    try:
        new = build()
        old = build(OLD) if have(OLD) else None
        test_check(new, old)
        test_pass_through(new)
        test_current()
        test_cache_timeout(new)
        test_plan(new, picky())
        test_assign(new, picky())
        test_stage(new)
        print("all pass")
    finally:
        shutil.rmtree(TMP, ignore_errors=True)


if __name__ == "__main__":
    main()
