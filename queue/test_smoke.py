#!/usr/bin/env python3
"""Tests for smoke_gate.py: no record -> queue a smoke task, hold the real task back; passed -> send; failed -> hold back
and report smoke; an env, definition or bundle change, or taskq smoke retry, queues it again; a real task done marks the
kind passed; adopt is idempotent; a recipe with no definition sends one task at a time; smoke tasks are not fetched, are
removed from the queue, and their dirs are cleaned up there; status, all_tasks, advance and flow do not show smoke tasks;
with the switch off only the shadow is logged; every smoke task gets a new dir; functions in definitions run on the store.
The queue is real, in a temp dir (taskq.py subprocess), and no other machine is contacted.
    python3 queue/test_smoke.py
"""
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
TMP = fixture.tmpdir(prefix="smoke_")
os.environ["TASKQ_STATE"] = os.path.join(TMP, "state")
os.environ["TASKQ_DATA"] = os.path.join(TMP, "data")
os.environ["TASKQ_LAST"] = os.path.join(TMP, "last")
os.environ["RELAY_LOG"] = os.path.join(TMP, "relay.log")
os.makedirs(os.environ["TASKQ_DATA"])
sys.path.insert(0, HERE)
import cfg  # noqa: E402

cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import proto  # noqa: E402
import spec as S  # noqa: E402
import taskq  # noqa: E402
import relay  # noqa: E402
import relay_store as ST  # noqa: E402
import smoke_gate as SG  # noqa: E402

NAME = M.names("slurm")[0]
HUB = taskq.queue_host()
DEF = dict(by=["variant"], params=dict(ds="SMOKE", frag="0", model="smoke/{smoke}"),
           vars=dict(DEMO_RESULTS="{data}/smoke/{smoke}/results"))
ADDS = []


def ok(msg):
    print(f"PASS:{msg}")


def central(*args):
    rc, out, err = taskq.central(list(args))
    assert rc == 0, (args, out, err)
    return out


def add(ds, frag, variant="V_save", nice=5):
    central("add", f"--nice={nice}", "--recipe", "toy", f"ds={ds}", f"frag={frag}", f"variant={variant}")
    return f"{ds}/{variant}/{frag}"


def mtq():
    st = taskq.local_status()
    return dict(st, enabled=True)


def S0():
    return dict(taskq={HUB: mtq()}, pueue={M.hub(): dict(tasks=[])}, records=[], ok={M.hub(): True}, src={})


def RS(*ts, bundle=None, oks=None):
    return dict(taskq=dict(tasks=list(ts), paused=False), tasks={}, free=500e9, bundle=bundle, live=[], envs_ok=oks or {})


def fill(s=None, rs=None):
    """Sending with only this executor (relay.assign) -> (actions for it, why nothing was claimed, alerts, shadow)."""
    stuck, notes = {}, {}
    out, why = relay.assign(s or S0(), {NAME: dict(rs=rs or RS(), st={})}, stuck, notes)
    return out.get(NAME, []), why.get(NAME), stuck, notes.get(NAME, [])


def run(acts, s=None):
    return relay.execute(NAME, acts, s or S0(), run_add=lambda *a, **k: ADDS.append(a) or "7", stuck={})


def kinds_of(more, op):
    return [a for a in more if a["op"] == op]


def setup():
    os.makedirs(taskq.TQ, exist_ok=True)
    open(os.path.join(taskq.TQ, proto.QUEUE["enabled"]), "w").close()
    open(taskq.tasks_path(), "w").close()
    relay.relayable = lambda i: True
    relay.refill.data_sizes = lambda paths, probe: {p: 1.0 for p in paths}
    relay.P.READY[NAME] = 2
    relay.P.AUTO["smoke"] = True
    SG.SMOKE["toy"] = DEF


def test_enqueue_pass():
    T = add("D1", "0")
    more, why, stuck, _ = fill()
    adds = kinds_of(more, "smoke_add")
    assert not kinds_of(more, "take") and len(adds) == 1 and not stuck, (more, why)
    a = adds[0]
    sid = SG.sid_of(a["key"])
    assert a["key"].startswith(f"_smoke/{NAME}/") and sid.startswith(f"{NAME}-") and a["params"]["variant"] == "V_save", a
    assert a["params"]["model"] == f"smoke/{sid}" and a["params"]["vars"]["DEMO_RESULTS"] == f"{{data}}/smoke/{sid}/results", a
    assert a["params"]["machines"] == [NAME], a
    ok("no record -> queue one smoke task (by parameters from the real task, {smoke} replaced by this run's name, only for this executor); the real task is not claimed this time")
    msgs = run(more)
    K = a["key"]
    t = next(t for t in mtq()["tasks"] if t["key"] == K)
    assert t["nice"] == proto.SMOKE_NICE and t.get("assigned") == NAME and t["spec"]["out"] == f"results/smoke/{sid}/frag_0", (t, msgs)
    assert ADDS and ADDS[-1][2] == "relay_stage" and ADDS[-1][3] == K, ADDS
    r = SG.load(NAME)
    assert len(r) == 1 and list(r.values())[0]["state"] == "queued" and list(r.values())[0]["key"] == K, r
    ok("smoke_add: added to the queue (nice -1000), assigned to this executor, stage queued; recorded as queued")
    more, why, stuck, _ = fill()
    assert not kinds_of(more, "take") and not kinds_of(more, "smoke_add"), more
    ok("smoke task still running -> the real task stays held, no second smoke task")
    st = {"keys": {NAME: {K: dict(outs=[t["spec"]["out"]], views=[], inputs=[], cleanup=[t["spec"]["out"]])}}}
    acts, stuck = relay.plan(S0(), NAME, RS(dict(key=K, state="done", worker=None, nice=proto.SMOKE_NICE)), st)
    assert [x["op"] for x in acts] == ["beat", "smoke_rec", "drop", "smoke_end"] and not stuck, acts
    assert acts[2]["cleanup"] == [f"results/smoke/{sid}", f"smoke/{sid}"], acts[2]
    ok("done on that side -> record the result, delete it there (including dirs whose name has {smoke}), no fetch, no archive")
    ST.update_state(lambda x: x.update(st))
    dropped = []
    saved = relay.RT.drop_remote
    relay.RT.drop_remote = lambda name, k, rk, gone=False: dropped.append((k, ST.paths_of(ST.keys_of(ST.load_state(), name)[k])))
    try:
        run(acts)
    finally:
        relay.RT.drop_remote = saved
    assert dropped and {f"results/smoke/{sid}", f"smoke/{sid}"} <= dropped[0][1], dropped
    assert K not in {t["key"] for t in mtq()["tasks"]}, mtq()
    assert not os.path.exists(os.path.join(taskq.TQ, proto.QUEUE["claims"], proto.safe(K)))
    assert list(SG.load(NAME).values())[0]["state"] == "ok"
    ok("smoke_end: removed from the queue (claim and record too); recorded as ok")
    more, why, stuck, _ = fill()
    assert [a["key"] for a in kinds_of(more, "take")] == [T] and not kinds_of(more, "smoke_add"), more
    ok("smoke passed -> send the real task")
    return T


def test_fail_retry():
    T = add("D2", "0", variant="V_fail")
    more, *_ = fill()
    run(more)
    K = kinds_of(more, "smoke_add")[0]["key"]
    st = {"keys": {NAME: {K: dict(outs=[], views=[], inputs=[], cleanup=[])}}}
    relay.smoke_tail = lambda name, rk: "Traceback: boom"
    acts, _ = relay.plan(S0(), NAME, RS(dict(key=K, state="failed", worker=None, nice=-1000, note="failed 3 times")), st)
    acts = [a for a in acts if a["key"] == K]
    assert [x["op"] for x in acts] == ["beat", "smoke_rec", "drop", "smoke_end"] and acts[1]["ok"] is False, acts
    ST.update_state(lambda x: x.update(st))
    saved = relay.RT.drop_remote
    relay.RT.drop_remote = lambda *a, **k: None
    try:
        run(acts)
    finally:
        relay.RT.drop_remote = saved
    m = SG.mark(NAME, next(t for t in mtq()["tasks"] if t["key"] == T)["spec"])
    r = SG.load(NAME)[m["kind"]]
    assert r["state"] == "failed" and "boom" in r["why"] and "failed 3 times" in r["why"], r
    more, why, stuck, _ = fill()
    assert T not in [a["key"] for a in more] and not kinds_of(more, "smoke_add"), more
    assert list(stuck) == [f"smoke:{NAME}:{m['kid']}"], stuck
    import plain
    msg = plain.problem(list(stuck)[0], dict(msg=proto.alert_msg(list(stuck.values())[0]),
                                              data=proto.alert_data(list(stuck.values())[0])))
    assert "toy" in msg and "V_fail" in msg, msg
    ok("smoke failed -> record failed (with the tail of that side's log), hold the real task, report smoke:<executor>:<kind id> (plain words name the kind)")
    m0 = M.MACHINES[NAME]["envs"]["base"]
    M.MACHINES[NAME]["envs"]["base"] = ("conda", "pt-new")
    try:
        more, *_ = fill()
        assert len(kinds_of(more, "smoke_add")) == 1, more
    finally:
        M.MACHINES[NAME]["envs"]["base"] = m0
    ok("env changed -> smoke again")
    SG.SMOKE["toy"] = dict(DEF, params=dict(DEF["params"], cfg="lite"))
    try:
        more, *_ = fill()
        assert len(kinds_of(more, "smoke_add")) == 1 and kinds_of(more, "smoke_add")[0]["params"]["cfg"] == "lite", more
    finally:
        SG.SMOKE["toy"] = DEF
    ok("smoke definition changed -> smoke again")
    more, *_ = fill(rs=RS(bundle="newbundle"))
    assert len(kinds_of(more, "smoke_add")) == 1, more
    ok("bundle changed -> failed kinds smoke again")
    more, *_ = fill()
    assert not kinds_of(more, "smoke_add")
    assert SG.main(["retry", NAME, m["kid"]]) == 0 and m["kind"] not in SG.load(NAME)
    more, *_ = fill()
    adds = kinds_of(more, "smoke_add")
    assert len(adds) == 1, more
    run(adds)
    adds2 = [dict(adds[0], key=adds[0]["key"] + "x")]
    k1 = SG.load(NAME)[m["kind"]]["key"]
    SG.retry(NAME)
    g = SG.Gate(NAME, RS(), mtq(), live=True)
    g.want[m["kind"]] = dict(sp=next(t for t in mtq()["tasks"] if t["key"] == T)["spec"], m=m)
    k2 = g.ops()[0]["key"]
    assert k1 != k2 and SG.sid_of(k1) != SG.sid_of(k2) and adds2
    ok("taskq smoke retry -> drop the failed record and queue again; every smoke task gets a new name (a new dir)")


def test_real_adopt():
    T = add("D3", "0", variant="V_real")
    central("take", "--worker", NAME, "--assign", T)
    s = S0()
    for t in s["taskq"][HUB]["tasks"]:
        if t["key"] == T:
            t["state"] = proto.S_DONE
    acts, _ = relay.plan(s, NAME, RS(), {})
    real = [a for a in acts if a["op"] == "smoke_real"]
    assert real and [a["op"] for a in acts if a["key"] == T][-2:] == ["release", "smoke_real"], acts
    run(real)
    r = SG.load(NAME)[real[0]["mark"]["kind"]]
    assert r["state"] == "ok" and r["src"] == "real", r
    ok("the real task finished on this executor -> the kind is recorded as passed (src=real)")
    T2 = add("D4", "0", variant="V_adopt")
    central("take", "--worker", NAME, "--assign", T2)
    new = SG.adopt(dry=True)
    assert (NAME, SG.mark(NAME, next(t for t in mtq()["tasks"] if t["key"] == T2)["spec"])["kind"]) in new, new
    assert not any(r.get("src") == "adopted" for r in SG.load(NAME).values())
    new = SG.adopt()
    assert new and SG.adopt() == [], new
    assert any(r.get("src") == "adopted" for r in SG.load(NAME).values())
    ok("adopt: mark kinds assigned to executors as passed (--dry-run writes nothing), a second run adds nothing")
    central("release", "--worker", NAME, T2)
    central("release", "--worker", NAME, T)


def test_env_variant():
    """The env id hashes the variant this executor uses (not that side's .ok); once smoke passed, a real task finishing does not rewrite the record."""
    T = add("D8", "0", variant="V_env")
    sp = next(t for t in mtq()["tasks"] if t["key"] == T)["spec"]
    e0, h0, v0 = M.MACHINES[NAME]["envs"]["base"], M.spec_hash, M.spec_variant
    M.MACHINES[NAME]["envs"]["base"] = ("spec", "gpu2")
    M.spec_hash = lambda env, variant="default": f"{env}-{variant}"
    try:
        assert M.spec_variant(NAME, "base") == "gpu2"
        m = SG.mark(NAME, sp)
        assert m["env"] == f"base:{SG.h12('base-gpu2')}", m
        g = SG.Gate(NAME, RS(oks={"base": "stale"}), mtq(), live=True)
        assert g.verdict(T, sp) and SG.mark(NAME, sp)["env"] == m["env"]
        SG.record(NAME, m, state="ok", src="smoke", key="_smoke/x", why="")
        assert SG.real_done(NAME, SG.mark(NAME, sp)) is False and SG.load(NAME)[m["kind"]]["src"] == "smoke"
        assert SG.Gate(NAME, RS(), mtq(), live=True).verdict(T, sp) is None
        assert SG.Gate(NAME, RS(oks={"base": "base-gpu2"}), mtq(), live=True).verdict(T, sp) is None
    finally:
        M.MACHINES[NAME]["envs"]["base"], M.spec_hash, M.spec_variant = e0, h0, v0
    ok("env id hashes the variant this executor uses, not whether that side's .ok was read; smoke passed -> a real task finishing does not rewrite it, no more smoke")


def test_canary():
    for t in mtq()["tasks"]:  # release what earlier tests claimed (a canary's kind only depends on recipe and env)
        if t.get("assigned") == NAME:
            central("release", "--worker", NAME, t["key"])
        if proto.is_smoke(t["key"]):
            central("rm", t["key"])
    SG.SMOKE.pop("toy")
    relay.P.READY[NAME] = 20
    try:
        T1, T2 = add("D5", "0", variant="V_can"), add("D5", "1", variant="V_can")
        more, *_ = fill()
        took = [a["key"] for a in kinds_of(more, "take")]
        assert len(took) == 1 and not kinds_of(more, "smoke_add"), more
        central("take", "--worker", NAME, "--assign", took[0])
        more, *_ = fill()
        assert not kinds_of(more, "take"), more
        ok("a recipe with no smoke definition (canary): send one at a time, others held back while it runs")
        s = S0()
        for t in s["taskq"][HUB]["tasks"]:
            if t["key"] == took[0]:
                t["state"] = proto.S_DONE
        acts, _ = relay.plan(s, NAME, RS(), {})
        run([a for a in acts if a["op"] == "smoke_real"])
        central("release", "--worker", NAME, took[0])
        central("rm", took[0])
        more, *_ = fill()
        assert {T1, T2} <= {a["key"] for a in kinds_of(more, "take")}, more
        ok("after one canary task finished, no more limit")
    finally:
        SG.SMOKE["toy"] = DEF
        relay.P.READY[NAME] = 2


def test_hidden():
    g = SG.Gate(NAME, RS(), mtq(), live=True)
    T = add("D6", "0", variant="V_hide")
    g.verdict(T, next(t for t in mtq()["tasks"] if t["key"] == T)["spec"])
    op = g.ops()[0]
    run([op])
    K = op["key"]
    q = mtq()["tasks"]
    assert K in {t["key"] for t in q}
    assert K not in {t["key"] for t in S.all_tasks(q)}
    out = central("status")
    assert K not in out and "taskq smoke status" in out, out
    keys = lambda *a: {t["key"] for t in S.json_in(central("status", "--json", *a))["tasks"]}  # noqa: E731
    assert K not in keys() and K in keys("--internal") and keys() | {K} == keys("--internal")
    out = central("status", "--internal")
    assert K in out and "smoke" not in out.splitlines()[-1], out
    import flow.compile as FC
    fl = FC.build(queue_tasks=q)
    assert fl.instances and K not in str(fl.instances), fl
    import advance
    s = S0()
    todo, _ = advance.plan(s)
    assert not any(K in str(x) for x in todo), todo
    import overview
    o = overview.build(dict(s, now=None, ok={}, src={}), relay={})
    assert K not in {t["key"] for t in o["tasks"]}
    ok("smoke tasks are invisible in taskq status text and --json, all_tasks (registry, board), overview, advance and flow (only status --internal lists them)")
    rs = RS(dict(key=K, state="claimed", worker="w", nice=-1000, beat=9e18))
    acts, _ = relay.plan(S0(), NAME, rs, {"keys": {NAME: {K: dict(outs=[])}}})
    assert [a["op"] for a in acts if a["key"] == K] == ["beat"], acts
    ok("smoke task running -> only heartbeat")
    return K


def test_off():
    relay.P.AUTO["smoke"] = False
    relay.P.READY[NAME] = 50
    try:
        T = add("D8", "0", variant="V_off")
        more, why, stuck, notes = fill()
        assert T in [a["key"] for a in kinds_of(more, "take")] and not kinds_of(more, "smoke_add") and not stuck, more
        assert notes and "would queue smoke" in notes[0] and "V_off" in notes[0], notes
        before = os.path.getsize(os.environ["RELAY_LOG"]) if os.path.exists(os.environ["RELAY_LOG"]) else 0
        relay.log(notes)
        assert os.path.getsize(os.environ["RELAY_LOG"]) > before
    finally:
        relay.P.AUTO["smoke"] = True
        relay.P.READY[NAME] = 2
    ok("switch off: send as usual, only log the smoke tasks it would queue and the tasks it would hold back to relay.log")


def test_plugin_table():
    assert "smoke" in cfg.KINDS and "smoke" in proto.AUTO_SWITCHES and proto.is_smoke("_smoke/a/b") and not proto.is_smoke("a/_smoke/")
    for bad in (dict(by="variant"), dict(by=["v"], extra=1)):
        SG.SMOKE["bad"] = bad
        try:
            SG.definition("bad")
            raise AssertionError(bad)
        except ValueError:
            pass
        finally:
            SG.SMOKE.pop("bad")
    ok("plugin table SMOKE: only by, params, vars; by is a list of parameter names")


def store_smoke(what, data):
    """The store's smoke operation (site_ops.op_smoke) run in this process: definitions come from SG.SMOKE as modified by
    this test, the store's data dir is TASKQ_DATA (a real run on the store is in test_check)."""
    import site_ops
    assert what == "smoke", what
    ext = cfg.extend
    cfg.extend = lambda ns, kind: ns.update(SMOKE=SG.SMOKE) if kind == "smoke" else ext(ns, kind)
    try:
        return site_ops.op_smoke(dict(data, data=os.environ["TASKQ_DATA"]))
    finally:
        cfg.extend = ext


def test_callable():
    seen = []

    def params(tp, data):
        seen.append(data)
        return dict(DEF["params"], ds="SMOKE_X" if tp["ds"].endswith("X") else "SMOKE")
    SG.SMOKE["toy"] = dict(DEF, params=params, vars=lambda tp, data: dict(DEMO_RESULTS="{data}/smoke/{smoke}/" + tp["ds"]))
    saved, SG.RM.store_op = SG.RM.store_op, store_smoke
    try:
        sp = S.resolve("toy", dict(ds="D9X", frag="0", variant="V_call"))
        sp2 = S.resolve("toy", dict(ds="D9", frag="0", variant="V_call"))
        m, m2 = SG.mark(NAME, sp), SG.mark(NAME, sp2)
        assert m["kind"] != m2["kind"] and "ds=SMOKE_X" in m["kind"] and m["defn"] != m2["defn"], (m, m2)
        SG.real_done(NAME, m2)
        assert SG.real_done(NAME, m) and SG.load(NAME)[m2["kind"]]["state"] == "ok"
        assert SG.Gate(NAME, RS(), mtq(), live=True).verdict("D9/V_call/0", sp2) is None
        SG.update(NAME, lambda recs: [recs.pop(k) for k in (m["kind"], m2["kind"])])
        assert seen and seen[0] == os.environ["TASKQ_DATA"], seen
        g = SG.Gate(NAME, RS(), mtq(), live=True)
        g.want[m["kind"]] = dict(sp=sp, m=m)
        op = g.ops()[0]
        sid = SG.sid_of(op["key"])
        assert op["params"]["ds"] == "SMOKE_X" and op["params"]["vars"]["DEMO_RESULTS"] == f"{{data}}/smoke/{sid}/D9X", op
        SG.real_done(NAME, m)
        assert SG.Gate(NAME, RS(), mtq(), live=True).verdict("D9X/V_call/0", sp) is None
        SG.SMOKE["toy"] = dict(DEF, params=lambda tp, data: dict(DEF["params"], ds="SMOKE_Y"))
        assert SG.mark(NAME, sp)["defn"] != m["defn"]
        assert SG.Gate(NAME, RS(), mtq(), live=True).verdict("D9X/V_call/0", sp), "a changed function result needs a new smoke test"
        ok("params and vars may be functions f(real task parameters, store data dir): different swapped data are two kinds recorded separately; the definition id follows the computed result, a changed result smokes again")

        got = {}
        SG.SMOKE["toy"] = dict(DEF, params=lambda tp, data: got.update(tp) or dict(DEF["params"]))
        SG.mark(NAME, sp)
        assert got.get("inputs") == list(sp.get("inputs") or []) and "inputs" not in (sp.get("names") or {}), got
        ok("functions in a smoke definition get the real task's inputs (the smoke task takes the inputs it needs from there)")

        def boom(tp, data):
            raise KeyError("no labels")
        SG.SMOKE["toy"] = dict(DEF, params=boom)
        me = SG.mark(NAME, sp)
        g = SG.Gate(NAME, RS(), mtq(), live=True)
        why = g.verdict("D9X/V_call/0", sp)
        assert why and "no labels" in why and list(g.alerts) == [f"smoke:{NAME}:{me['kid']}"], (why, g.alerts)
        fails = [o for o in g.ops() if o["op"] == "smoke_fail"]
        assert len(fails) == 1 and not [o for o in g.ops() if o["op"] == "smoke_add"], g.ops()
        run(fails)
        r = SG.load(NAME)[me["kind"]]
        assert r["state"] == "failed" and "no labels" in r["why"], r
        g = SG.Gate(NAME, RS(), mtq(), live=True)
        assert g.verdict("D9X/V_call/0", sp) and g.alerts and not g.ops()
        SG.SMOKE["toy"] = dict(DEF, params=lambda tp, data: dict(DEF["params"], ds="SMOKE_Z"))
        g = SG.Gate(NAME, RS(), mtq(), live=True)
        g.verdict("D9X/V_call/0", sp)
        assert [o["op"] for o in g.ops()] == ["smoke_add"], g.ops()
    finally:
        SG.SMOKE["toy"] = DEF
        SG.RM.store_op = saved
    ok("function error -> record the kind as failed (why = the exception), report smoke, carry on this round; after a fix (changed result) smoke again")


def test_race():
    """Two checks running at once: the later one read the queue before the earlier one added the smoke task, so it must not add another."""
    add("D8", "0", variant="V_race")
    stale = S0()
    more, *_ = fill(stale)
    a = [x for x in kinds_of(more, "smoke_add") if x["params"]["variant"] == "V_race"]
    assert len(a) == 1, more
    run(a)
    kid = a[0]["mark"]["kid"]

    def smokes():
        return [t["key"] for t in mtq()["tasks"] if t["key"].startswith(f"_smoke/{NAME}/{kid}-")]
    assert smokes() == [a[0]["key"]], smokes()
    more, *_ = fill(stale)
    assert not [x for x in more if x["key"] == "D8/V_race/0" or x.get("params", {}).get("variant") == "V_race"], more
    ok("the later check's queue snapshot predates this smoke task: it still counts as queued, adds none, and the real task stays held")
    k2 = a[0]["key"] + "x"
    dup = dict(a[0], key=k2, params=dict(a[0]["params"], key=k2))
    got, msg = SG.enqueue(NAME, dup)
    assert got is None and "already has smoke task" in msg and smokes() == [a[0]["key"]], (got, msg, smokes())
    msgs = run([dup])
    assert smokes() == [a[0]["key"]] and SG.load(NAME)[a[0]["mark"]["kind"]]["key"] == a[0]["key"], (msgs, smokes())
    ok("both checks decided to queue one: the later add finds it already queued, adds none, claims none, raises no alert, and the record still points at the first one")


def main():
    try:
        setup()
        test_plugin_table()
        test_enqueue_pass()
        test_fail_retry()
        test_real_adopt()
        test_env_variant()
        test_canary()
        test_hidden()
        test_off()
        test_callable()
        test_race()
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    print("all passed")


if __name__ == "__main__":
    main()
