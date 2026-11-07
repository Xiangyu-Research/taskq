#!/usr/bin/env python3
"""smoke_gate.py 的测试（11/5）：没有记录 → 排冒烟、挡下真任务；冒烟过了 → 送；没过 → 挡下、报 smoke；环境、定义、代码包变了或
taskq smoke retry 后重排；真任务训完记成过了；adopt 重复跑不多记；没有定义的做法一次只送一个；冒烟任务不拉回、从总队列里去掉、
清理那边的文件夹；status、all_tasks、advance、flow 看不见冒烟任务；交班要接手的那台过了；开关关着只记影子；每次一个新文件夹。
总队列是临时目录里真的（taskq.py 子进程），不连别的机器。
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
import shift  # noqa: E402

NAME = next(iter(M.RELAY))
HUB = taskq.queue_host()
DEF = dict(by=["variant"], params=dict(ds="SMOKE", frag="0", model="smoke/{smoke}"),
           vars=dict(DEMO_RESULTS="{data}/smoke/{smoke}/results"))
ADDS = []


def ok(msg):
    print(f"通过：{msg}")


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
    return dict(taskq={HUB: mtq()}, pueue={M.hub(): dict(tasks=[])}, records=[], ok={M.hub(): True})


def RS(*ts, bundle=None, oks=None):
    return dict(taskq=dict(tasks=list(ts), paused=False), tasks={}, free=500e9, bundle=bundle, live=[], envs_ok=oks or {})


def fill(s=None, rs=None):
    stuck, notes = {}, []
    relay.PLANNED.clear()
    more, why = relay.fill(s or S0(), NAME, rs or RS(), {}, [], stuck, notes)
    return more, why, stuck, notes


def run(acts, s=None):
    return relay.execute(NAME, acts, s or S0(), run_add=lambda *a, **k: ADDS.append(a) or "7", stuck={})


def kinds_of(more, op):
    return [a for a in more if a["op"] == op]


def setup():
    os.makedirs(taskq.TQ, exist_ok=True)
    open(os.path.join(taskq.TQ, proto.QUEUE["enabled"]), "w").close()
    open(taskq.tasks_path(), "w").close()
    relay.refill.candidates = lambda s, name, mtq, ready, envs_ok, kinds=None, held=None: [
        t for t in mtq["tasks"] if t["state"] == proto.S_FREE and M.serves(name, (t.get("spec") or {}).get("machines"))]
    relay.relayable = lambda i: True
    relay.refill.data_gb = lambda paths: 1.0
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
    ok("没有记录 → 排一个冒烟（by 的参数取真任务的，{smoke} 换成这次的名字，只给这台），真任务这次不领")
    msgs = run(more)
    K = a["key"]
    t = next(t for t in mtq()["tasks"] if t["key"] == K)
    assert t["nice"] == proto.SMOKE_NICE and t.get("assigned") == NAME and t["spec"]["out"] == f"results/smoke/{sid}/frag_0", (t, msgs)
    assert ADDS and ADDS[-1][2] == "relay_stage" and ADDS[-1][3] == K, ADDS
    r = SG.load(NAME)
    assert len(r) == 1 and list(r.values())[0]["state"] == "queued" and list(r.values())[0]["key"] == K, r
    ok("smoke_add：加进总队列（档位 -1000）、分给这台、排传数据；记成 queued")
    more, why, stuck, _ = fill()
    assert not kinds_of(more, "take") and not kinds_of(more, "smoke_add"), more
    ok("冒烟还在做 → 真任务照样挡着，不再排第二个")
    st = {"keys": {NAME: {K: dict(outs=[t["spec"]["out"]], views=[], inputs=[], cleanup=[t["spec"]["out"]])}}}
    acts, stuck = relay.plan(S0(), NAME, RS(dict(key=K, state="done", worker=None, nice=proto.SMOKE_NICE)), st)
    assert [x["op"] for x in acts] == ["beat", "smoke_rec", "drop", "smoke_end"] and not stuck, acts
    assert acts[2]["cleanup"] == [f"results/smoke/{sid}", f"smoke/{sid}"], acts[2]
    ok("那边做完 → 记结果、删那边的（连同名字里有 {smoke} 的文件夹），不拉回、不归档")
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
    ok("smoke_end：从总队列里去掉（领取、记录一起删）；记成 ok")
    more, why, stuck, _ = fill()
    assert [a["key"] for a in kinds_of(more, "take")] == [T] and not kinds_of(more, "smoke_add"), more
    ok("冒烟过了 → 真任务送过去")
    return T


def test_fail_retry():
    T = add("D2", "0", variant="V_fail")
    more, *_ = fill()
    run(more)
    K = kinds_of(more, "smoke_add")[0]["key"]
    st = {"keys": {NAME: {K: dict(outs=[], views=[], inputs=[], cleanup=[])}}}
    relay.smoke_tail = lambda name, rk: "Traceback: boom"
    acts, _ = relay.plan(S0(), NAME, RS(dict(key=K, state="failed", worker=None, nice=-1000, note="失败 3 次")), st)
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
    assert r["state"] == "failed" and "boom" in r["why"] and "失败 3 次" in r["why"], r
    more, why, stuck, _ = fill()
    assert T not in [a["key"] for a in more] and not kinds_of(more, "smoke_add"), more
    assert list(stuck) == [f"smoke:{NAME}:{m['kid']}"], stuck
    import plain
    assert "冒烟没过" in plain.problem(list(stuck)[0], dict(msg=proto.alert_msg(list(stuck.values())[0]),
                                                           data=proto.alert_data(list(stuck.values())[0]))), stuck
    ok("冒烟没过 → 记 failed（带那边日志的尾巴）、真任务挡着、报 smoke:<执行机>:<种类 id>（白话说清哪种没过）")
    m0 = M.RELAY[NAME]["envs"]["base"]
    M.RELAY[NAME]["envs"]["base"] = ("conda", "pt-new")
    try:
        more, *_ = fill()
        assert len(kinds_of(more, "smoke_add")) == 1, more
    finally:
        M.RELAY[NAME]["envs"]["base"] = m0
    ok("环境变了 → 重新冒烟")
    SG.SMOKE["toy"] = dict(DEF, params=dict(DEF["params"], cfg="lite"))
    try:
        more, *_ = fill()
        assert len(kinds_of(more, "smoke_add")) == 1 and kinds_of(more, "smoke_add")[0]["params"]["cfg"] == "lite", more
    finally:
        SG.SMOKE["toy"] = DEF
    ok("冒烟定义变了 → 重新冒烟")
    more, *_ = fill(rs=RS(bundle="newbundle"))
    assert len(kinds_of(more, "smoke_add")) == 1, more
    ok("代码包换了 → 没过的重新冒烟")
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
    ok("taskq smoke retry → 去掉没过的记录、重排；每次冒烟一个新名字（新的文件夹）")


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
    ok("真任务在这台训完 → 这种记成过了（src=real）")
    T2 = add("D4", "0", variant="V_adopt")
    central("take", "--worker", NAME, "--assign", T2)
    new = SG.adopt(dry=True)
    assert (NAME, SG.mark(NAME, next(t for t in mtq()["tasks"] if t["key"] == T2)["spec"])["kind"]) in new, new
    assert not any(r.get("src") == "adopted" for r in SG.load(NAME).values())
    new = SG.adopt()
    assert new and SG.adopt() == [], new
    assert any(r.get("src") == "adopted" for r in SG.load(NAME).values())
    ok("adopt：分给各台的任务的种类记成过了（--dry-run 不写），再跑一次不多记")
    central("release", "--worker", NAME, T2)
    central("release", "--worker", NAME, T)


def test_env_variant():
    """11/6：环境编号只按描述加这台的变体算；冒烟过了以后真任务训完不改写记录（以前 lab 的 .ok 是变体的哈希、读不到时退回默认变体，
    两边对不上，又排一次冒烟）。"""
    T = add("D8", "0", variant="V_env")
    sp = next(t for t in mtq()["tasks"] if t["key"] == T)["spec"]
    e0, h0, v0 = M.RELAY[NAME]["envs"]["base"], M.spec_hash, M.spec_variant
    M.RELAY[NAME]["envs"]["base"] = ("spec", "gpu2")
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
        M.RELAY[NAME]["envs"]["base"], M.spec_hash, M.spec_variant = e0, h0, v0
    ok("环境编号按描述加这台的变体算，不看那边的 .ok 读没读到；冒烟过了 → 真任务训完不改写、不再排冒烟")


def test_canary():
    for t in mtq()["tasks"]:  # 前面领着的都交还（canary 的种类只按做法和环境算）
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
        ok("没有冒烟定义的做法（canary）：一次只送一个，在做的那个没完时别的挡着")
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
        ok("canary 训完一个以后不再限")
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
    assert K not in out and "冒烟 1 个" in out, out
    keys = lambda *a: {t["key"] for t in S.json_in(central("status", "--json", *a))["tasks"]}  # noqa: E731
    assert K not in keys() and K in keys("--internal") and keys() | {K} == keys("--internal")
    out = central("status", "--internal")
    assert K in out and "冒烟" not in out.splitlines()[-1], out
    import flow.compile as FC
    fl = FC.compile_legacy(queue_tasks=q, data="/d", python="py")
    assert fl.instances and K not in str(fl.instances), fl
    import advance
    s = S0()
    todo, _ = advance.plan(s, root=os.environ["TASKQ_DATA"])
    assert not any(K in str(x) for x in todo), todo
    import overview
    o = overview.build(dict(s, now=None, ok={}, src={}), relay={})
    assert K not in {t["key"] for t in o["tasks"]}
    ok("冒烟任务：taskq status 的文字和 --json、all_tasks（登记表、看板）、overview、advance、flow 都看不见（status --internal 才列）")
    rs = RS(dict(key=K, state="claimed", worker="w", nice=-1000, beat=9e18))
    acts, _ = relay.plan(S0(), NAME, rs, {"keys": {NAME: {K: dict(outs=[])}}})
    assert [a["op"] for a in acts if a["key"] == K] == ["beat"], acts
    ok("冒烟在训 → 只续约")
    return K


def test_shift():
    T = add("D7", "0", variant="V_shift")
    assert shift.smoke_passed(NAME, T) is False
    sp = next(t for t in mtq()["tasks"] if t["key"] == T)["spec"]
    SG.real_done(NAME, SG.mark(NAME, sp))
    assert shift.smoke_passed(NAME, T) is True
    assert shift.smoke_passed(NAME, "_smoke/x/y") is False
    relay.P.AUTO["smoke"] = False
    try:
        assert shift.smoke_passed(NAME, add("D7", "1", variant="V_shift2")) is True
    finally:
        relay.P.AUTO["smoke"] = True
    ok("交班：接手的那台没过这种 → 不交给它（交还总队列）；过了 → 交；开关关着 → 照旧")


def test_off():
    relay.P.AUTO["smoke"] = False
    relay.P.READY[NAME] = 50
    try:
        T = add("D8", "0", variant="V_off")
        more, why, stuck, notes = fill()
        assert T in [a["key"] for a in kinds_of(more, "take")] and not kinds_of(more, "smoke_add") and not stuck, more
        assert notes and "会排冒烟" in notes[0] and "V_off" in notes[0], notes
        before = os.path.getsize(os.environ["RELAY_LOG"]) if os.path.exists(os.environ["RELAY_LOG"]) else 0
        relay.log(notes)
        assert os.path.getsize(os.environ["RELAY_LOG"]) > before
    finally:
        relay.P.AUTO["smoke"] = True
        relay.P.READY[NAME] = 2
    ok("开关关着：照常送，只把会排的冒烟、会挡下的任务记进 relay.log")


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
    ok("插件表 SMOKE：只认 by、params、vars；by 是参数名的列表")


def test_callable():
    seen = []

    def params(tp, data):
        seen.append(data)
        return dict(DEF["params"], ds="SMOKE_X" if tp["ds"].endswith("X") else "SMOKE")
    SG.SMOKE["toy"] = dict(DEF, params=params, vars=lambda tp, data: dict(DEMO_RESULTS="{data}/smoke/{smoke}/" + tp["ds"]))
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
        assert SG.Gate(NAME, RS(), mtq(), live=True).verdict("D9X/V_call/0", sp), "函数的结果变了要重新冒烟"
        ok("params、vars 可以是函数 f(真任务的参数, hub 的数据目录)：换成的数据不同是两种、各记各的；定义的 id 按算出来的结果，结果变了重新冒烟")

        got = {}
        SG.SMOKE["toy"] = dict(DEF, params=lambda tp, data: got.update(tp) or dict(DEF["params"]))
        SG.mark(NAME, sp)
        assert got.get("data") == list(sp.get("data") or []) and "data" not in (sp.get("names") or {}), got
        ok("冒烟定义的函数拿到真任务的 data（冒烟要带上的输入从这里取）")

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
    ok("函数出错 → 这种记成没过（why = 异常）、报 smoke、这一轮照常；修好（结果变了）后重新冒烟")


def test_race():
    """两次检查同时跑（11/5）：后一次的总队列是前一次加冒烟之前读的，不能再排一个。"""
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
    ok("后一次检查读的总队列里还没有这个冒烟：照样看成已经排了，不再排、真任务照样挡着")
    k2 = a[0]["key"] + "x"
    dup = dict(a[0], key=k2, params=dict(a[0]["params"], key=k2))
    got, msg = SG.enqueue(NAME, dup)
    assert got is None and "已经排了" in msg and smokes() == [a[0]["key"]], (got, msg, smokes())
    msgs = run([dup])
    assert smokes() == [a[0]["key"]] and SG.load(NAME)[a[0]["mark"]["kind"]]["key"] == a[0]["key"], (msgs, smokes())
    ok("两次检查都决定要排：后加的那次发现已经排了，不加、不领、不报警，记录还指着先加的那个")


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
        test_shift()
        test_off()
        test_callable()
        test_race()
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    print("全部通过")


if __name__ == "__main__":
    main()
