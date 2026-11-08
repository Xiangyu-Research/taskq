#!/usr/bin/env python3
"""bundle_gate.py 的测试：从工作区和旧提交建代码包，拿总队列现在会送的说明（testdata 的实验表、command 做法）过闸门：
跑得了的放行，跑不了的挡下；hub 上没有那个包、不知道包时放行；超时放行、不记着；relay.plan 挡下、记 held、报 bundlegate，
包换了再送；relay.assign 不派那边在用的包跑不了的；stage 送之前再核对一次。临时目录跑完删掉。浅克隆里没有旧提交时跳过旧包那几项。
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
OLD = "c5dace4"  # 旧核心：说明的格式和现在的不一样（输入叫 data）
CMD = S.sendable(S.resolve("command", dict(key="job/a", cmd="echo hi > {data}/out/a/x", env="pt", out="out/a", done="x", relay="path")))
TMPL = dict(CMD, params=dict(CMD["params"], note="{other}"))  # params 里留着给做法自己用的 {…}：现在的包不展开，假包 picky() 挡下


def ok(msg):
    print(f"PASS:{msg}")


def have(commit):
    return subprocess.run(["git", "-C", CORE, "cat-file", "-e", f"{commit}^{{commit}}"], capture_output=True).returncode == 0


@contextlib.contextmanager
def patched(obj, **kw):
    """暂时换掉 obj 的属性，出来时换回。"""
    old = {k: getattr(obj, k) for k in kw}
    for k, v in kw.items():
        setattr(obj, k, v)
    try:
        yield
    finally:
        for k, v in old.items():
            setattr(obj, k, v)


def build(commit=None):
    """commit=None：工作区的使用者文件和核心；否则两边都取自 commit（核心和测试的项目在同一个仓库）。"""
    bid, path = RB.build(commit, core=commit or RB.CORE_REF)
    if commit and not os.path.exists(os.path.join(path, "envs")):  # 旧核心只认 <仓库>/envs（没有 .taskq.toml 的配置文件夹）
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
        print(f"SKIP:没有提交 {OLD}（浅克隆）")
        return
    err = BG.check(CMD, old)
    assert err and "spec.py shell" in err, err
    bad = [k for k, sp in specs.items() if BG.check(sp, old)]
    assert len(bad) == len(specs), bad
    ok(f"{OLD} 的包（说明的格式是旧的）：现在送的说明都挡下（{len(bad)}/{len(specs)} 个）")
    assert BG.check_all(CMD, [new, old]) == (old, err) and BG.check_all(CMD, [new]) is None
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
    ok("那边在用的包：current 加上 STALE_BEAT 内动过的（不算临时目录）；还没有包 → \"\"；连不上 → None")


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
    """old：只挡带 {other} 的说明的假包（picky）。"""
    K = "job/t"
    acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, RS(new), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K, TMPL)]), NAME, RS(None), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    ok("plan：包跑得了、不知道那边的包 → 照常传")
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


def test_assign(new, old):
    """relay.assign（refill.choose 的 block）：那边在用的包跑不了的不派，接着挑下一个；都跑不了时 why 里写代码包。old 是假包 picky。"""
    K2, K3 = "job/f2", "job/f3"
    env = sorted(M.ready_envs(NAME))[0]  # 这台不用另建就有的环境
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
        assert took == [] and f"在用的代码包 {old} 跑不了" in why[NAME] and "{other}" in why[NAME], (took, why)
    ok("assign：那边在用的包（current、Slurm 在跑的 worker 的）跑不了的不派，接着挑下一个；都跑不了 → why 写代码包")


def test_stage(new):
    """RT.stage：送之前按 export 给的参数解析出要送的说明，用那边在用的包再核对一次。跑不了的包用一个假包（spec.py 退出 3）。"""
    import relay_transfer as RT
    k = next(iter(E.tasks()))
    bad = "e" * 16
    os.makedirs(os.path.dirname(os.path.join(OUT, bad, BG.SPEC_PY)), exist_ok=True)
    with open(os.path.join(OUT, bad, BG.SPEC_PY), "w") as f:
        f.write("import sys\nsys.exit(3)\n")
    said, asked = [], []

    def export(what, data, **kw):  # 闸门在用到数据之前：只要 params
        asked.append(what)
        return dict(params=data["spec"]["params"], files=[], views=[], outs=[], texts={}, cleanup=[])
    with patched(BG, current=lambda name: (new, [bad])), patched(RT.RM, store_op=export):
        assert RT.stage(NAME, k, nice=5, say=said.append) is None and asked == ["export"], asked
        h = BG.held(ST.load_state(), NAME)[k]
        assert h["bundle"] == bad and "退出 3" in h["err"] and "先不送" in said[-1], (h, said)
        assert ST.keys_of(ST.load_state(), NAME).get(k) is None
        sp = S.sendable(S.resolve(E.tasks()[k]["recipe"], E.tasks()[k]["params"]))
        BG.current = lambda name: (new, [])
        assert BG.check_all(sp, RT.in_use(NAME)) is None
        BG.current = lambda name: None
        assert RT.in_use(NAME) == []
    BG.unhold(NAME, k)
    ok("stage：送之前按要送的说明再核对一次，跑不了 → 不传、不加、记 held（下次检查再来）；连不上 → 不核对")


def picky():
    """假包：说明里带 {other} 的跑不了（spec.py 退出 2），别的都跑得了。plan、assign 的测试要一个只挡一部分说明的包。"""
    bid = "f" * 16
    p = os.path.join(OUT, bid, BG.SPEC_PY)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        f.write("import sys\nif '{other}' in sys.argv[-1]:\n    sys.exit('不认识 {other}')\n"
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
        print("全部通过")
    finally:
        shutil.rmtree(TMP, ignore_errors=True)


if __name__ == "__main__":
    main()
