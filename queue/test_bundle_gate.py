#!/usr/bin/env python3
"""bundle_gate.py 的测试（11/5）：从工作区和旧提交建代码包，拿总队列现在会送的说明（testdata 的实验表、command 做法）过闸门：
跑得了的放行，跑不了的挡下；hub 上没有那个包、不知道包时放行；超时放行、不记着；relay.plan / fill 挡下、记 held、报 bundlegate，
包换了再送。临时目录跑完删掉。浅克隆里没有旧提交时跳过旧包那几项。
    python3 queue/test_bundle_gate.py
"""
import json
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

NAME = next(n for n in M.RELAY if M.RELAY[n]["scheduler"] == "slurm")
PNAME = next((n for n in M.RELAY if M.RELAY[n]["scheduler"] == "pueue"), None)
OUT = os.path.expanduser(RB.OUT)
OLD = "c5dace4"  # 11/4 有 --spec、还会展开 params 里的 {…}（11/5 出事的那种包）
CMD = S.sendable(S.resolve("command", dict(key="job/a", cmd="echo hi > {data}/out/a/x", env="pt", out="out/a", done="x", relay="path")))
TMPL = dict(CMD, params=dict(CMD["params"], note="{other}"))  # params 里留着给做法自己用的 {…}：新包不展开，旧包报不认识


def ok(msg):
    print(f"通过：{msg}")


def have(commit):
    return subprocess.run(["git", "-C", CORE, "cat-file", "-e", f"{commit}^{{commit}}"], capture_output=True).returncode == 0


def slurm_site(commit):
    """那个提交的机器表里第一台 Slurm 的 relay 执行机（测试用的机器名 11/5 改过，旧提交里叫别的）。"""
    ns = {"__name__": "relay_machines"}
    exec(compile(RB.read_machines(commit), "machines.py", "exec"), ns)
    return next(n for n, v in (ns.get("RELAY") or {}).items() if v["scheduler"] == "slurm")


def build(commit=None):
    name = slurm_site(commit) if commit else NAME
    bid, path = RB.build(name, commit, root=os.path.join(TMP, "root"), core=commit or RB.CORE_REF)
    if commit and not os.path.exists(os.path.join(path, "envs")):  # 11/4 的核心只认 <仓库>/envs（没有 .taskq.toml 的配置文件夹）
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
    ok(f"现在的包：总队列会送的 {len(specs)} 个说明和 command 做法都放行；说明里有它不认识的资源 → 挡下")
    if old is None:
        print(f"跳过：没有提交 {OLD}（浅克隆）")
        return
    assert BG.check(CMD, old) is None, BG.check(CMD, old)
    err = BG.check(TMPL, old)
    assert err and "{other}" in err, err
    bad = [k for k, sp in specs.items() if BG.check(sp, old)]
    assert bad, "旧包应该跑不了带模板的说明"
    ok(f"{OLD} 的包：普通说明放行；params 里带 {{…}} 的挡下（{len(bad)}/{len(specs)} 个实验表的说明）")
    assert BG.check_all(TMPL, [new, old]) == (old, err) and BG.check_all(TMPL, [new]) is None
    ok("check_all：报第一个跑不了的包")


def test_pass_through(new):
    for bid in (None, "", "0" * 16, "../x", ".tmp"):
        assert BG.check(TMPL, bid) is None, bid
    assert BG.check({}, new) is None
    assert BG.in_use(NAME, None, None) == [] and BG.in_use(NAME, "a", ["b", "a", ""]) == ["a", "b"]
    if PNAME:
        assert BG.in_use(PNAME, "a", ["b"]) == ["a"]
    ok("不知道包、hub 上没有那个包、没有说明 → 放行；Slurm 的看 current 和在跑的 worker 的包，pueue 的只看 current")


def test_cache_timeout(new):
    calls, real = [], BG.run
    BG.run = lambda d, sp: calls.append(d) or real(d, sp)
    try:
        sp = dict(CMD, key="job/cache")
        assert BG.check(sp, new) is None and BG.check(sp, new) is None and len(calls) == 1
    finally:
        BG.run = real
    ok("同一个包、同一个说明只跑一次")
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
    assert err and "退出 3" in err, err
    assert BG.main(["0" * 16]) == 0
    ok("跑超时 → 放行、不记着（下次再跑）；退出码不对 → 挡下；命令行：hub 上没有的包放行")


def test_current():
    """那边的 current 和 STALE_BEAT 内动过的包（假的 ssh 在本机跑）；还没有包 → ""；连不上 → None。"""
    import relay_remote as RM
    fake, root = os.path.join(TMP, "fake_ssh"), os.path.join(TMP, "remote")
    with open(fake, "w") as f:
        f.write('#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\n[[ $1 == -O ]] && exit 0\nshift\nexec bash -c "$*"\n')
    os.chmod(fake, 0o755)
    conf0, env0 = M.RELAY[NAME], os.environ.get("RELAY_SSH")
    M.RELAY[NAME] = dict(conf0, root=root)
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
        M.RELAY[NAME] = conf0
        if env0 is None:
            os.environ.pop("RELAY_SSH", None)
        else:
            os.environ["RELAY_SSH"] = env0
    ok("那边在用的包：current 加上 STALE_BEAT 内动过的（不算临时目录）；还没有包 → \"\"；连不上 → None")


def S0(mt):
    return dict(taskq={relay.taskq.queue_host(): dict(enabled=True, tasks=mt)}, pueue={M.hub(): dict(tasks=[])}, records=[],
                ok={M.hub(): True})


def mtask(key, sp, state="claimed", worker=NAME, nice=5):
    return dict(key=key, state=state, worker=worker, nice=nice, order=1, since=1000.0, note="", spec=dict(sp, key=key), skipped_by=[])


def RS(bid, live=(), *ts):
    return dict(taskq=dict(tasks=list(ts), paused=False), tasks={}, free=500e9, bundle=bid, live=list(live))


def ops(acts):
    return sorted((a["op"], a["key"]) for a in acts)


def test_plan(new, old):
    K = "job/t"
    acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, RS(new), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, RS(None), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    ok("plan：包跑得了、不知道那边的包 → 照常传")
    if old is None:
        return
    for rs in (RS(old), RS(new, [old])):
        acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, rs, {})
        assert ops(acts) == [("beat", K), ("hold", K)] and acts[-1]["bundle"] == old, acts
        assert list(stuck) == [f"bundlegate:{NAME}:{K}"] and proto.alert_data(stuck[f"bundlegate:{NAME}:{K}"])["key"] == K, stuck
    ok("plan：current 或 Slurm 里在跑的 worker 的包跑不了 → 不传、记 held、报 bundlegate:<名字>:<key>，领取照续")
    msgs = relay.execute(NAME, [a for a in acts if a["op"] == "hold"], S0([]))
    st = ST.load_state()
    h = BG.held(st, NAME)[K]
    assert h["why"] == proto.HELD["bundle"] == "bundle-incompatible" and h["bundle"] == old and msgs, (h, msgs)
    acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, RS(old), st)
    assert ops(acts) == [("beat", K)] and stuck, acts
    ok("记下以后：同一个包不再记一次，仍然报")
    acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, RS(new), st)
    assert ops(acts) == [("beat", K), ("stage", K), ("unhold", K)] and not stuck, (acts, stuck)
    relay.execute(NAME, [a for a in acts if a["op"] == "unhold"], S0([]))
    assert BG.held(ST.load_state(), NAME) == {}
    ok("那边换了包 → 传、去掉 held")
    BG.hold(NAME, K, new, "stage 挡下的")
    acts, stuck = relay.plan(S0([mtask(K, CMD)]), NAME, RS(new), ST.load_state())
    assert ops(acts) == [("beat", K)] and f"bundlegate:{NAME}:{K}" in stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K, CMD)]), NAME, RS(old), ST.load_state())
    assert ops(acts) == [("beat", K), ("stage", K), ("unhold", K)] and not stuck, (acts, stuck)
    BG.unhold(NAME, K)
    ok("stage 按要送的说明挡下的：那个包还在用就不再排传数据、照报；换了包再排")
    if PNAME:
        acts, _ = relay.plan(S0([mtask(K, TMPL, worker=PNAME)]), PNAME, RS(new, [old]), {})
        assert ops(acts) == [("beat", K), ("stage", K)], acts
        ok("pueue 的执行机：只看 current（worker 代码变了就不再领）")


def test_fill(new, old):
    if old is None:
        return
    K2, K3 = "job/f2", "job/f3"
    cands = [mtask(K2, TMPL, "free", None, nice=3), mtask(K3, CMD, "free", None, nice=4)]
    saved = relay.refill.candidates, relay.relayable, relay.refill.data_gb, relay.P.READY.get(NAME)
    relay.refill.candidates = lambda s, name, mtq, ready, envs_ok, kinds=None, held=None: [t for t in mtq["tasks"] if not t["worker"]]
    relay.relayable = lambda i: True
    relay.refill.data_gb = lambda paths: 1.0
    relay.P.READY[NAME] = 2
    try:
        more, why = relay.fill(S0(cands), NAME, RS(new), {}, [])
        assert [a["key"] for a in more] == [K2, K3], (more, why)
        relay.PLANNED.clear()
        more, why = relay.fill(S0(cands), NAME, RS(old), {}, [])
        assert [a["key"] for a in more] == [K3], (more, why)
        relay.PLANNED.clear()
        more, why = relay.fill(S0(cands[:1]), NAME, RS(old), {}, [])
        assert more == [] and "代码包" in why, (more, why)
    finally:
        relay.refill.candidates, relay.relayable, relay.refill.data_gb = saved[:3]
        relay.P.READY[NAME] = saved[3]
        relay.PLANNED.clear()
    ok("fill：那边在用的包跑不了的不领，接着挑下一个")


def test_stage(new, old):
    import relay_transfer as RT
    if old is None:
        return
    k = next(k for k, t in E.tasks().items() if BG.check(S.sendable(t["spec"]), old))
    saved, said, export = BG.current, [], RT.N.export
    BG.current = lambda name: (new, [old])
    RT.N.export = lambda real, D: dict(params=real["params"])  # 闸门在用到数据之前
    try:
        assert RT.stage(NAME, k, nice=5, say=said.append) is None
        assert BG.held(ST.load_state(), NAME)[k]["bundle"] == old and "先不送" in said[-1], said
        assert ST.keys_of(ST.load_state(), NAME).get(k) is None
        sp = S.sendable(S.resolve(E.tasks()[k]["recipe"], E.tasks()[k]["params"]))
        BG.current = lambda name: (new, [])
        assert BG.check_all(sp, RT.in_use(NAME)) is None
        BG.current = lambda name: None
        assert RT.in_use(NAME) == []
    finally:
        BG.current, RT.N.export = saved, export
    ok("stage：送之前按要送的说明再核对一次，跑不了 → 不传、不加、记 held，退出码同排不上（下次检查再来）")


def main():
    try:
        new = build()
        old = build(OLD) if have(OLD) else None
        test_check(new, old)
        test_pass_through(new)
        test_current()
        test_cache_timeout(new)
        test_plan(new, old)
        test_fill(new, old)
        test_stage(new, old)
        print("全部通过")
    finally:
        shutil.rmtree(TMP, ignore_errors=True)


if __name__ == "__main__":
    main()
