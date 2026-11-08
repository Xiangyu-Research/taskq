#!/usr/bin/env python3
"""relay.py 的测试：
  1. 每次检查的决策（plan：每台领着的任务；assign：总队列里能派的任务一次分给各台；topup：开 worker、交作业）：
     假的总队列、那边的状态、hub 的任务；
  2. 拉回时挪进数据目录的顺序（site_ops.place）：中途出错时做完的标志不在（不会被当成做完）；
  3. 真跑一遍：本机模拟 store（hub 的根目录）和"那边"（site-slurm2，假的 ssh、srun 在本地跑，隧道开在 127.0.0.1），
     推代码包、传数据（stage）、那边的队列、拉回（fetch），和 store 一样的相对路径；
  4. site_ops 的单个操作（视图、列结果、收进 blobs、删任务）；删任务时按引用计数只删别的任务不用的路径。
配置用 testdata/ 的拷贝（hub 和 site-slurm2 的根目录、数据目录换成临时目录，代码包里带的也是这份）；
包里的 Python 文件用本机 bundle_check 那个环境核对，没有时用当前的 Python。
跑法：python3 queue/test_relay.py"""
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
import cfg as _cfg  # noqa: E402  调度代码的根、使用者的仓库根
QR = _cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）
BQ = _cfg.bundle_rel("queue")  # 代码包里调度代码的 queue/（总是 scripts/taskq/queue）
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
STATE_DIR = fixture.tmpdir(prefix="test_relay_state_")
os.environ["TASKQ_STATE"] = STATE_DIR  # 本机的状态（队列、记录…）放临时目录
import test_relay_plugins as TN  # noqa: E402  （建了临时目录 TN.TMP、TN.ROOT 是 store 的数据目录）

T = TN.TMP
HROOT = os.path.join(T, "hub_root")  # store（hub）的根目录：store 的操作跑这里的代码包
RROOT = os.path.join(T, "remote")  # site-slurm2 的根目录
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

assert M.machine("hub")["root"] == HROOT and M.data() == TN.ROOT, "配置的拷贝没有用上"
NAME = "site-slurm2"
UNSET = " ".join(f"-u {v}" for v in proto.HUB_ONLY_ENV)
with open(FAKE, "w") as f:  # 假的 ssh：跳过 -o，-O check 看 master_down，其余在本机跑（去掉 hub 才有的变量，和登录过去一样）
    f.write(f'#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\n[[ $1 == -O ]] && {{ [[ -e {T}/master_down ]] && exit 255; exit 0; }}\n'
            f'shift\ncd / && exec env {UNSET} bash -c "$*"\n')
os.chmod(FAKE, 0o755)
with open(os.path.join(BIN, "srun"), "w") as f:  # 假的 srun：跳过选项，从 python3 / bash 那一项起照跑
    f.write(f"#!{sys.executable}\nimport os,sys\na=sys.argv[1:]\n"
            f"open({os.path.join(T, 'srun_args')!r},'a').write(' '.join(a)+chr(10))\n"
            f"if os.path.exists({os.path.join(T, 'no_slot')!r}):\n    sys.stderr.write('srun: error: Unable to allocate resources: Immediate')\n    sys.exit(1)\n"
            "i=next(i for i,x in enumerate(a) if x.endswith('python3') or x=='bash')\nos.execvp(a[i],a[i:])\n")
os.chmod(os.path.join(BIN, "srun"), 0o755)
os.makedirs(os.path.join(CONDA, "bin"))
os.symlink(relay_bundle.check_python() or sys.executable, os.path.join(CONDA, "bin", "python3"))
Q = "hub"  # 总队列所在的机器（taskq.queue_host()）
assert relay.taskq.queue_host() == Q


def ok(msg):
    print("PASS:" + msg)


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


@contextlib.contextmanager
def mpatch(name, **kw):
    """暂时改机器表里 name 的条目（kw 的值是 None 时去掉那个字段）。"""
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
    """暂时改 policy 里的一张表。"""
    d0 = dict(d)
    d.update(kw)
    try:
        yield
    finally:
        d.clear()
        d.update(d0)


# ---------------- 1. 决策 ----------------
def test_pueue_worker():
    g1, g0, cw = M.workers("site-pueue")
    assert (g1["id"], g0["id"], cw["id"], cw["kind"]) == ("site-pueue-gpu1", "site-pueue-gpu0", "site-pueue-cpu", "cpu"), (g1, g0, cw)
    lab = lambda w: proto.WORKER_LABEL + w["id"]  # noqa: E731
    ids = lambda subs: [w["id"] for _, w in subs]  # noqa: E731
    ready = dict(tasks=[dict(key="a/x/0", state="free"), dict(key="a/x/1", state="claimed")])
    none = dict(tasks=[dict(key="a/x/1", state="claimed")])
    nocpu = f"那边没有备好、没人领的 CPU 任务给 {cw['id']}"
    assert relay.topup("site-pueue", dict(jobs=None, taskq=ready)) == ([], "读不到 pueue")
    subs, why = relay.topup("site-pueue", dict(jobs=[], taskq=ready))
    assert ids(subs) == [g1["id"], g0["id"]] and why == nocpu, (subs, why)
    subs, why = relay.topup("site-pueue", dict(jobs=[[3, lab(g1), "Running", None]], taskq=ready))
    assert ids(subs) == [g0["id"]] and why == f"{g1['id']} 在；{nocpu}", (subs, why)
    subs, why = relay.topup("site-pueue", dict(jobs=[[3, lab(g1), "Done", "Killed"], [4, "other", "Running", None]], taskq=ready))
    assert ids(subs) == [g1["id"], g0["id"]], (subs, why)
    subs, why = relay.topup("site-pueue", dict(jobs=[[3, lab(g1), "Done", "{'Failed': 1}"]], taskq=ready))
    assert ids(subs) == [g0["id"]] and why.startswith(f"上一个 {g1['id']}（pueue 任务 3）失败了（{{'Failed': 1}}）"), (subs, why)
    ok("pueue 执行机（site-pueue）：机器表的每个 worker 各自判断，没有活着的才开，上一个失败了不重开")
    gpu_none = "；".join(f"那边没有备好、没人领的任务给 {w['id']}" for w in (g1, g0))
    assert relay.topup("site-pueue", dict(jobs=[], taskq=none)) == ([], f"{gpu_none}；{nocpu}")
    assert relay.topup("site-pueue", dict(jobs=[[3, lab(g1), "Done", "Success"]], taskq=none)) == ([], f"{gpu_none}；{nocpu}")
    ok("pueue 执行机：没有备好的这种任务不开 worker（空闲退出的那个不算失败），有了再开")
    cpu = dict(tasks=[dict(key="e/x/0", state="free", spec=dict(device="cpu")), dict(key="a/x/1", state="claimed")])
    subs, why = relay.topup("site-pueue", dict(jobs=[], taskq=cpu))
    assert ids(subs) == [cw["id"]] and why == gpu_none, (subs, why)
    subs, why = relay.topup("site-pueue", dict(jobs=[[5, lab(cw), "Done", "{'Failed': 1}"]], taskq=dict(tasks=cpu["tasks"] + ready["tasks"][:1])))
    assert ids(subs) == [g1["id"], g0["id"]] and why == f"上一个 {cw['id']}（pueue 任务 5）失败了（{{'Failed': 1}}）", (subs, why)
    with patched(relay.M, working=lambda n, w, t=None: w["id"] != g0["id"]):
        subs, why = relay.topup("site-pueue", dict(jobs=[], taskq=ready))
    assert ids(subs) == [g1["id"]] and f"{g0['id']} 不在时段里" in why, (subs, why)
    ok("pueue 执行机：CPU worker 只看纯 CPU 任务；时段外的 worker 不开（machines.py 的 hours）")


def test_slurm_topup_ready():
    two = dict(enabled=True, tasks=[dict(key="a/x/0", state="free"), dict(key="a/x/1", state="free"), dict(key="a/x/2", state="claimed")])
    probe = lambda out: (lambda name, cmd: (0, out, ""))  # noqa: E731
    q = next(iter(relay.P.SLURM[NAME]["gpu"]))
    keep = relay.P.SLURM[NAME]["gpu"][q][1]
    subs, _ = relay.topup(NAME, dict(taskq=two), run=probe(f"QOS|{q}|10|0|0\n"))
    assert [n for _, _, n, _ in subs] == [10 - keep] and subs[0][3] == "gpu", subs
    subs, why = relay.topup(NAME, dict(taskq=two), run=probe(f"QOS|{q}|10|{10 - keep}|2\n"))
    assert subs == [] and "排满" in why
    subs, why = relay.topup(NAME, dict(taskq=dict(enabled=True, tasks=[])), run=probe(f"QOS|{q}|10|0|0\n"))
    assert subs == [] and "没有要卡的任务" in why
    assert relay.topup(NAME, dict(taskq=two), run=lambda n, c: (1, "", "ssh 断了")) == ([], "读不到作业数")
    ok("Slurm 执行机（site-slurm2）：有任务就按名额排满（攒排队时间），满了不交，没有任务、读不到作业数不交")
    cpu = dict(enabled=True, tasks=[dict(key="e/x/0", state="free", spec=dict(device="cpu"))])
    subs, why = relay.topup(NAME, dict(taskq=cpu), run=probe(f"QOS|{q}|10|0|0\n"))
    assert subs == [] and "CPU" not in why, (subs, why)
    slurm0 = relay.P.SLURM
    with pdict(relay.P.SLURM, **{"site-slurm": dict(gpu={"long": ("3-00:00:00", 0)}, cpu=("normal", "08:00:00", 2)),
                                 NAME: dict(slurm0[NAME], cpu=("normal", "08:00:00", 2))}):
        subs, why = relay.topup("site-slurm", dict(taskq=cpu), run=probe("QOS|long|10|0|0\nCPU|0\nALL|10|3\n"))
        assert subs == [("normal", "08:00:00", 1, "cpu")], (subs, why)
        subs, why = relay.topup("site-slurm", dict(taskq=dict(two, tasks=two["tasks"] + cpu["tasks"])), run=probe("QOS|long|10|0|0\nCPU|1\nALL|10|3\n"))
        assert subs == [("long", "3-00:00:00", 10, "gpu")] and "CPU：已有 1 个" in why, (subs, why)
        subs, why = relay.topup(NAME, dict(taskq=cpu), run=probe(f"QOS|{q}|10|0|0\nCPU|0\nALL|10|3\n"))
        assert subs == [] and "CPU" not in why, (subs, why)
    with pdict(relay.P.SLURM):
        relay.P.SLURM.pop(NAME)
        assert relay.topup(NAME, dict(taskq=two), run=probe("")) == ([], "policy.py 的 SLURM 里没有它")
    ok("Slurm 执行机：jobs 里有 cpu 的（site-slurm）有纯 CPU 任务时交 CPU worker；没有的（site-slurm2）SLURM 里写了 cpu 也不交；"
       "SLURM 里没有这台不交")


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
    """那边的状态（remote_status 的结果）：ts 是那边队列里的任务。"""
    return dict(dict(taskq=dict(tasks=list(ts), paused=kw.pop("paused", False)), tasks=kw.pop("tasks", {}), free=500e9, out_used=0,
                     envs_ok={}, bundle=None), **kw)


def test_plan():
    K = A = "D1_x/tr/0"
    st = keymap(K)

    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "claimed", "123")), st)
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "free")), st)
    assert ops(acts) == [("beat", K)], acts
    ok("那边排着、在训 → 只续")
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
    assert calls == [f"set-nice --nice -101 {A}"] and "档位 -101" in msgs[0], (calls, msgs)
    ok("总队列里改了档位 → 那边跟着改（set-nice）；那边训完的不改")

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
    ok("还没放过去 → 排传数据；在传 → 等；出错 → 重排，满 3 次或手动停的 → 报、领取保留；领取之前的旧任务不算")
    ns = [dict(id=7, label=rec["label"], status=f"Failed ({relay.NOSLOT_RC})", code=relay.NOSLOT_RC)]
    acts, stuck = relay.plan(S0([mtask(K, since=time.time() - 600)], ns, [rec]), NAME, RS(), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K, since=time.time() - 4 * 3600)], ns, [dict(rec, added=time.time() - 3600)]), NAME, RS(), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and f"site:{NAME}:stage" in stuck, (acts, stuck)
    ok("传数据排不上 CPU 作业（退出码 75）→ 下次检查再传、不报；领了 3 小时以上还排不上才报")

    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K), ("fetch", K)], acts
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(dict(rtask(A, "done", "123"), beat=time.time() - 60)), st)
    assert ops(acts) == [("beat", K)], acts
    ok("那边有了做完的标志、worker 还领着（训练命令还在写结果）→ 只续约，不拉回")
    for kw in (dict(beat=time.time() - 3600), dict(beat=time.time() - 3600, task_log_age=3600), dict()):
        acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(dict(rtask(A, "done", "123"), **kw)), st)
        assert ops(acts) == [("beat", K), ("fetch", K)], (kw, acts)
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(dict(rtask(A, "done", "123"), beat=time.time() - 3600, task_log_age=30)), st)
    assert ops(acts) == [("beat", K)], acts
    ok("做完的标志有了、领取已过期（worker 没交还就没了）→ 照常拉回；心跳旧但日志还在写 → 仍算领着")
    frec = dict(machine=M.hub(), id=8, label="relay_fetch:" + K, step="relay_fetch", key=K, added=2000.0)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=8, label=frec["label"], status="Running")], [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=8, label=frec["label"], status="Failed")], [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K), ("fetch", K)] and not stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=i, label=frec["label"], status="Failed") for i in (8, 9, 10)],
                                [dict(frec, id=i) for i in (8, 9, 10)]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
    ok("那边训完 → 排一次拉回；在拉 → 等；出错 → 重排，满 3 次才报")

    acts, _ = relay.plan(S0([mtask(K, "done")]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("drop", K), ("release", K)], acts
    fdone = [dict(id=8, label=frec["label"], status="Success")]
    acts, _ = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("drop", K), ("release", K)], acts
    acts, stuck = relay.plan(S0([mtask(K, "done")], [dict(fdone[0], status="Running")], [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, stuck = relay.plan(S0([mtask(K, "done")], [dict(fdone[0], status="Failed")], [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
    ok("总队列显示训完（store 上有了）→ 交还、删那边的任务和结果；拉回、归档还在跑 → 等；出错 → 报、不删")

    import common as C
    old_lookup, old_down = RT.lookup, C.backup_down
    RT.lookup = lambda k: dict(archive=True)
    arec = dict(machine=M.hub(), id=9, label="relay_archive:" + K, step="relay_archive", key=K, added=2000.0)
    plain_fail = [dict(fdone[0], status="Failed")]
    try:
        C.backup_down = lambda ok=None: "site-slurm 暂停检查（停机）"
        acts, stuck = relay.plan(S0([mtask(K, "done")], plain_fail, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
        acts, stuck = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("drop", K), ("release", K)] and not stuck, (acts, stuck)
        assert "没有归档" in [a for a in acts if a["op"] == "release"][0]["why"], acts
        C.backup_down = lambda ok=None: "site-slurm 这次连不上" if ok is not None else None
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
        assert "归档了" in [a for a in acts if a["op"] == "release"][0]["why"], acts
        acts, stuck = relay.plan(S0([mtask(K, "done")], [fdone[0], dict(id=9, label=arec["label"], status="Failed")], [frec, arec]), NAME,
                                 RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K), ("drop", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
    finally:
        RT.lookup, C.backup_down = old_lookup, old_down
    ok("要归档的任务：store 上有了就删那边的；备份机能用时单独排归档（relay_archive），归档成功才交还，归档出错 → 报、领取保留；"
       "备份机停用时直接交还（没有归档）；只是这次连不上 → 等；单独的拉回出错 → 报")

    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "failed")), st)
    assert ops(acts) == [("drop", K), ("release", K)] and [a for a in acts if a["op"] == "release"][0].get("skip"), acts
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "failed"), tasks={A: dict(final=False, runs=[dict(epochs_done=3)])}), st)
    assert ops(acts) == [("beat", K)] and f"site:{NAME}:fail:{K}" in stuck, acts
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "bad")), st)
    assert ops(acts) == [("beat", K)] and f"site:{NAME}:fail:{K}" in stuck, stuck
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "claimed", "123"), paused=True), st)
    assert f"site:{NAME}:fail" in stuck, stuck
    ok("那边FAIL:没有进度 → 交还（--skip）并删掉；有进度、bad → 保留、报 site:<名字>:fail:<key>；那边暂停 → site:<名字>:fail")

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
    assert args.startswith(proto.IO_GROUP + " ") and "--no-add" in args and "--nice" not in args and "补传" in msgs[0], (added, msgs)
    ok("那边缺输入 → 在 hub 的核心组排补传（stage --no-add：只补缺的、撤掉标记）；在补 → 等；补传出错 → 报；标记之前的旧任务不算")

    def fail_add(*a, **k):
        raise RuntimeError("hub 上加任务FAIL:git fetch 出错")
    st_ = {}
    msgs = relay.execute(NAME, [dict(op="restage", key=K), dict(op="fetch", key=K)], S0([mtask(K)]), run_add=fail_add, stuck=st_)
    assert set(st_) == {f"relayadd:{NAME}:relay_stage:{K}", f"relayadd:{NAME}:relay_fetch:{K}"} and len(msgs) == 2 and all("出错" in m for m in msgs), (st_, msgs)
    import plain
    assert "没排上" in plain.problem(next(iter(st_)), dict(msg=st_[next(iter(st_))]["msg"], data=st_[next(iter(st_))]["data"])), st_
    ok("在 hub 上排传数据、拉回出错 → 报 relayadd（下次检查重试），后面的动作照做")

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
    ok("总队列里停了 → 那边停，那边不再领着后交还；再领到 → 那边撤销停止")

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
    ok("那边有、总队列里不归它的：不让它训；总队列训完了、或恢复了没分给谁且那边没进度就删；认不出的、在训的报 site:<名字>:orphan")

    for state in ("free", "stopped"):
        acts, stuck = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(rtask(A, state)), st)
        assert ops(acts) == [("drop", K)] and acts[0]["rkey"] == A and not acts[0].get("gone") and not stuck, (state, acts)
    acts, _ = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(), st)
    assert ops(acts) == [], acts
    dst = dict(keys={NAME: {K: dict(st["keys"][NAME][K], dropped=1.0)}})
    for mt in ([mtask(K, "claimed", "other")], [mtask(K, "done", "other")], []):
        acts, _ = relay.plan(S0(mt), NAME, RS(), dst)
        assert ops(acts) == [("drop", K)] and acts[0].get("gone") and acts[0]["rkey"] == K, (mt, acts)
    ok("总队列里改分给了别的执行机（那边停着的也算）→ 删那边的任务和结果；那边的任务删了、路径没删掉的（dropped）→ 只再删路径")

    acts, stuck = relay.plan(S0([mtask(K)]), NAME, None, dict(st, last_ok={NAME: time.time() - 3 * 3600}))
    assert ops(acts) == [("beat", K)] and f"site:{NAME}:down" in stuck, stuck
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, None, dict(st, last_ok={NAME: time.time() - 600}))
    assert ops(acts) == [("beat", K)] and not stuck
    ok("连不上：只续总队列的领取，2 小时以上报")




def test_assign():
    """总队列里能派的任务一次分给各台（relay.assign → refill.choose）：每台的余量按那边的状态算（site_for），输入多大在 store 上量。"""
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
        assert probed[-1] == ["prepared/D2_y", "prepared/D3_z"], probed  # 已经在那边的（D1_x）不量
        berr = S0([mtask(K)] + cands)
        berr["pueue"] = {M.hub(): {"error": "pueue 没有响应"}}
        out, why, _ = run(None, {NAME: (RS(rtask(K, "claimed", "1")), st)}, s=berr)
        assert out == {} and "pueue" in why[NAME], (out, why)
        ok("派任务：按档位领到 READY 个，只领能翻译的做法；hub 的 pueue 读不到时不派（传数据要在 hub 的 pueue 排）")
        out, why, _ = run([mtask(K), cands[0], dict(cands[1], nice=6)], {NAME: (RS(rtask(K, "free")), st)})
        assert out == {NAME: [("take", K2)]}, (out, why)
        out, why, _ = run([mtask(K), mtask(K5)] + [dict(t, nice=t["nice"] + 3) for t in cands], {NAME: (RS(rtask(K, "free")), st)})
        assert out == {NAME: []} and "排满了" in why[NAME], (out, why)
        tight = dict(RS(rtask(K, "claimed", "1")), free=(30 + 10 + 10 + 10 - 5) * 1e9)  # 已有一个任务留 10 GB，再领一个要 10+10，剩不到 30
        out, why, _ = run([mtask(K)] + cands, {NAME: (tight, st)})
        assert out == {NAME: []} and "放不下" in why[NAME], (out, why)
        out, why, _ = run([mtask(K), mtask(K2, "free", None, nice=3, inputs=["prepared/nowhere"])], {NAME: (RS(rtask(K, "claimed", "1")), st)})
        assert out == {NAME: []} and "store 上没有输入 prepared/nowhere" in why[NAME], (out, why)
        ok("派任务：那边备着的（含还没传过去的）不够 READY 个才领；排满了、放不下、store 上没有输入的不派，原因写进 why")
        full = [mtask(K), mtask(K5), mtask(K0, "free", None, nice=-100, order=5)] + [dict(t, nice=t["nice"] + 3) for t in cands]
        out, _, _ = run(full, {NAME: (RS(rtask(K, "free")), st)})
        assert out == {NAME: [("take", K0)]}, out
        out, _, _ = run([mtask(K), mtask(K0, "free", None, nice=-100, order=5)] + cands[:1], {NAME: (RS(rtask(K, "free")), st)})
        assert out == {NAME: [("take", K0), ("take", K2)]}, out
        ok("第 ⓪ 档（nice < 0，训完后的推理）不占 READY 的名额：排满了也领，先领它")

        late = [mtask(K2, "free", None, nice=6), mtask(K3, "free", None, nice=7)]
        out, why, _ = run([mtask(K), mtask(K5, nice=6)] + late, {NAME: (RS(rtask(K, "stopped"), rtask(K5, "free")), st2)})
        assert out == {NAME: []} and "排满了" in why[NAME], (out, why)
        ok("那边停着、这次要撤销停止的也算备着")
        one = [mtask(K2, "free", None, nice=3)]
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + one, {NAME: (RS(rtask(K, "free"), rtask(K5, "free")), st2)})
        assert out == {NAME: [("drop", K5), ("release", K5), ("take", K2)]}, out
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + one, {NAME: (RS(rtask(K, "claimed", "1"), rtask(K5, "free")), st2)})
        assert out == {NAME: [("take", K2)]}, out
        for f in (dict(runs=[dict(epochs_done=300)]), dict(resumable=True, runs=[])):  # 作业到时限、worker 交还的做到一半的
            out, _, _ = run([mtask(K), mtask(K5, nice=6)] + one, {NAME: (RS(rtask(K, "free"), rtask(K5, "free"), tasks={K5: f}), st2)})
            assert out == {NAME: [("drop", K), ("release", K), ("take", K2)]}, (f, out)  # 换没有进度的那个
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + cands, {NAME: (RS(rtask(K, "free"), rtask(K5, "free")), st2)})
        assert out == {NAME: [("drop", K5), ("release", K5), ("take", K2)]}, out  # 每种每轮只换一个
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + [mtask(K2, "free", None, nice=6)], {NAME: (RS(rtask(K, "free"), rtask(K5, "free")), st2)})
        assert out == {NAME: []}, out
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + one, {NAME: (RS(rtask(K, "free", "9"), rtask(K5, "free", "9")), st2)})
        assert out == {NAME: []}, out
        out, _, _ = run([mtask(K), mtask(K5, nice=6)] + one, {NAME: (dict(RS(rtask(K, "free"), rtask(K5, "free")), free=45e9), st2)})
        assert out == {NAME: []}, out
        ok("备满了、总队列里有更优先的：交还一个备好没人领的（先删那边、再交还）换它进来；在训的、有人领的、同档的、"
           "有进度的（作业到时限交还的）不换，放不下不换，每种每轮只换一个")

        E1, E2 = "D7_f/ev/0", "D8_g/ev/0"
        ctask = lambda key, nice=3: dict(mtask(key, "free", None, nice=nice), spec=dict(mtask(key)["spec"], device="cpu"))  # noqa: E731
        out, why, _ = run([mtask(K), ctask(E1), ctask(E2, 4)], {NAME: (RS(rtask(K, "free")), st)})
        assert out == {NAME: []} and "队列里没有能派给它的任务" in why[NAME], (out, why)
        out, why, _ = run([mtask(K), ctask(E1), ctask(E2, 4)] + cands, {NAME: (RS(rtask(K, "free")), st), "site-slurm": (RS(), {})})
        assert out == {NAME: [], "site-slurm": [("take", E1), ("take", K2), ("take", E2), ("take", K3)]}, out
        ok("纯 CPU 任务只派给有 CPU worker 的（site-slurm），GPU、CPU 各备 READY 个，互不占名额")

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
        ok("几台都能去：按 PLACE 的偏好（没写 = 机器表的顺序）；放不下的换下一台；输入已经在那边的那台先；"
           "跳过过它的（skipped_by）不去，能去的都跳过了 → 报 tqblocked、不派")

        with mpatch(NAME, cap=None, ssh="fake-shared"), mpatch("site-slurm", cap=None, ssh="fake-shared"):
            small = RS(free=(30 + 10 + 10 + 5) * 1e9, dev=7)  # 一个任务（输入 10 + 产出 10）放得下，两个放不下
            out, why, _ = run(cands[:2], {NAME: (small, {}), "site-slurm": (dict(small), {})})
            assert out == {NAME: [], "site-slurm": [("take", K2)]} and K3 not in str(out), (out, why)
            out, _, _ = run(cands[:2], {NAME: (small, {}), "site-slurm": (dict(small, dev=8), {})})
            assert out == {NAME: [("take", K3)], "site-slurm": [("take", K2)]}, out
        ok("同一个盘上的几台（同一个 ssh、没有 cap、盘号相同）：这一轮已经计划占的合起来扣；盘不同各算各的")

        on = (rtask(K, "claimed", "1"), rtask("D9_h/tr/0", "done"), rtask("D9_h/tr/1", "claimed", "2"))
        OUT = proto.OUT_GB
        sep = RS(*on, free=(3 * OUT + 35) * 1e9, out_used=12e9, dev=1, blob_dev=2, blob_free=500e9)
        out, _, _ = run([mtask(K)] + cands, {NAME: (sep, st)})
        assert out == {NAME: [("take", K2)]}, out  # 剩 3×OUT+35+12，那边 3 个任务各留 OUT 后剩 47：领一个（留 OUT）剩 37，第二个剩 27 < 30
        out, why, _ = run([mtask(K)] + cands, {NAME: (dict(sep, out_used=0), st)})
        assert out == {NAME: []} and "放不下" in why[NAME], (out, why)
        out, why, _ = run([mtask(K)] + cands, {NAME: (dict(sep, free=500e9, blob_free=45e9), st)})
        assert out == {NAME: [("take", K2)]}, (out, why)  # 输入按放 blobs 的盘查：45 − 10 剩 35，第二个剩 25
        same = RS(*on, free=(3 * OUT + 55) * 1e9, dev=1, blob_dev=1, blob_free=500e9)
        out, _, _ = run([mtask(K)] + cands, {NAME: (same, st)})
        assert out == {NAME: [("take", K2)]}, out  # 同一个盘：输入也从这里扣，55 − 20 剩 35，第二个剩 15
        out, _, _ = run([mtask(K)] + cands, {NAME: (dict(same, blob_dev=2), st)})
        assert out == {NAME: [("take", K2), ("take", K3)]}, out
        ok("那边每个任务（含训完没拉回的、这一轮领的）的产出各留 out_gb（默认 proto.OUT_GB），加回已经写出的；"
           "blobs 在另一个盘时输入按那个盘查，同一个盘时一起扣")

        with patched(relay.M, working=lambda n, w, t=None: False):
            out, why, _ = run([mtask(K)] + cands, {NAME: (RS(rtask(K, "free")), st)})
        assert out == {} and why[NAME] == "所有 worker 都不在时段里（machines.py 的 hours）", (out, why)
        out, why, _ = run([mtask(K)] + cands, {NAME: (None, st)})
        assert out == {} and why[NAME] == "连不上", (out, why)
        ok("所有 worker 都在时段外、连不上的执行机：不派")
    if status0 is not None:
        open(relay.STATUS, "w").write(status0)


def test_refcount():
    """删一个任务时只删别的任务不用的路径：同一个数据集的几折共用视图，推理的输入在训练的产出下面。"""
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
    ok("引用计数：共用的视图、别的任务输入所在的产出不删；只是名字前缀相同的不算")

    seen = []
    with patched(RM, op=lambda name, what, data=None, **kw: (seen.append((what, data)), 1)[1], rtq=lambda name, cmd: (0, "")), \
            patched(ST, load_state=lambda: dict(keys={NAME: keys}), update_state=lambda fn: None):
        msgs = relay.execute(NAME, [dict(op="drop", key="inf", rkey="inf")], S0([]))
    assert seen == [("drop", dict(paths=["raw/A/img/a.dat", "res/X/tr/dataset.json"], logs="inf"))], (seen, msgs)
    ok("drop 只删算出来能删的路径和自己的日志")


def test_drop_refused():
    """那边拒绝删任务（有人在训）时不删结果、总队列里也不交还；上次删了任务没删完结果的照常删。"""
    K = "D1_x/tr/0"
    seen, central = [], []
    acts = [dict(op="drop", key=K, rkey=K), dict(op="release", key=K, why="测试")]
    rop = lambda name, what, data=None, **kw: (seen.append((what, data)), 1)[1]  # noqa: E731
    with patched(RM, op=rop), patched(relay.taskq, central=lambda args, timeout=300: (central.append(args), (0, "", ""))[1]):
        try:
            for r in [(relay.taskq.REFUSED, f"不删：{K}：claimed，有人在做"), (1, f"队列里没有：{K}"), (1, "Traceback"), None]:
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
                raise RuntimeError("ssh 断了")
            with patched(RM, rtq=lambda name, cmd: (0, ""), op=broken):
                msgs = relay.execute(NAME, acts, S0([]))
            assert central and any("下次检查再删" in m for m in msgs), (central, msgs)
            st = ST.load_state()
            assert st["keys"][NAME][K].get("dropped"), st
            acts2, _ = relay.plan(S0([]), NAME, RS(), st)
            assert [(a["op"], a["key"], a.get("gone")) for a in acts2] == [("drop", K, True)], acts2
            seen.clear()
            relay.execute(NAME, acts2, S0([]))
            assert [w for w, _ in seen] == ["drop"] and K not in ST.load_state()["keys"].get(NAME, {}), (seen, ST.load_state())
        finally:
            ST.update_state(lambda st: st.get("keys", {}).get(NAME, {}).pop(K, None))
    ok("那边拒绝删任务（退出码 3）、别的错或连不上时不删结果、不交还；任务已经删掉的（退出码 4）照常删结果；"
       "任务删了、路径没删掉：照常交还，记 dropped，下一轮再删路径")


def test_beat_lost():
    """总队列里的领取已经不是它的（beat --strict 退出码 13）：这些 key 这一轮不再动（不传、不拉回、不删、不交还），别的照常。"""
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
    assert any("已经不是它的" in m and K in m for m in msgs), msgs
    ok("relay 续约用 beat --strict：领取已经不是它的 key 这一轮不再动，别的照常")


def test_fresh_site():
    """新机器第一次：连得上、根目录里还没有代码包（读状态失败）→ 先部署、再读状态；部署失败时这次当连不上。
    已有旧代码包、换新包失败（推的时候出错、超时，或打包出错）→ 这次也当连不上，只续约。"""
    W = "site-pueue"
    s = {"ok": {M.hub(): True}, "taskq": {Q: {"tasks": []}}}
    # cur：读不到状态时那边的 code/current（"" = 没有；"b0" = 旧包跑不了新的状态程序；"b1" = 已是新包；None = 连不上）
    for fail, old, cur in ((None, False, ""), ("rt", False, ""), ("rt", True, ""), ("timeout", True, ""), ("build", True, ""),
                           (None, False, "b0"), ("rt", False, "b0"), ("same", False, "b1"), ("down", False, None)):
        calls = []
        seq = [{"bundle": "b0", "jobs": [], "squeue": None}] if old else [None, {"bundle": "b1", "jobs": [], "squeue": None}]

        def wanted():
            if fail == "build":
                raise RuntimeError("打包出错")
            return "b1", "/nonexistent"

        def dep(n, b, p, claims=None):
            if fail == "rt":
                raise RuntimeError("推不过去")
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
    ok("新机器还没有代码包时先部署再读状态；部署失败（新机器或换新包）这次当连不上")
    ok("读不到状态、那边是旧包（旧包跑不了新的状态程序）→ 试一次换包再读；已是新包、连不上 → 不部署，这次当连不上")


def test_paused():
    """policy.py 的 PAUSE 里暂停着的执行机：这一轮不读、不派、不交作业；它做了一半的任务放开给别的执行机。"""
    seen = []
    s = {"ok": {M.hub(): True}, "taskq": {Q: {"enabled": True, "tasks": []}}}
    ctx = lambda n: dict(rs={}, st={}, acts=[], stuck={}, notes=[])  # noqa: E731
    with pdict(relay.P.PAUSE, **{NAME: (None, "测试")}), \
            patched(relay, update_code=lambda stuck: [], prepare=lambda s, n, dry: (seen.append(n), ctx(n))[1],
                    assign=lambda s, ctxs, stuck, notes: (seen.append(("assign", sorted(ctxs))), ({}, {}))[1],
                    finish=lambda s, n, c, more, why, sn, dry: (seen.append(("finish", n)), ([], {}))[1]):
        relay.run_all(s, dry=True)
        assert NAME not in seen and ("assign", sorted(n for n in M.executors() if n != NAME)) in seen, seen
        assert ("finish", NAME) not in seen and ("finish", "site-pueue") in seen, seen
    ok("暂停着的执行机（PAUSE）：不读、不派、不交作业")


def test_held_by():
    """做了一半的任务认回原来那台：这一轮读到的用读到的，别的用 status.json；那台暂停着、不是执行机时放开。"""
    other = next(n for n in M.executors() if n != NAME)
    old = open(relay.STATUS).read() if os.path.exists(relay.STATUS) else None
    try:
        os.makedirs(os.path.dirname(relay.STATUS), exist_ok=True)
        json.dump({other: {"tasks": {"h/x": {"resumable": True}, "h/y": {}}}, NAME: {"tasks": {"h/old": {"ran": True}}},
                   "gone-site": {"tasks": {"h/z": {"ran": True}}}}, open(relay.STATUS, "w"))
        with patched(relay, site_view=lambda n, rs, st: {"h/me": {"ran": True}}):
            h = relay.held_all({NAME: dict(rs={}, st={})})
            assert h == {"h/x": [other], "h/me": [NAME]}, h
            with pdict(relay.P.PAUSE, **{other: (None, "测试")}):
                assert relay.held_all({NAME: dict(rs={}, st={})}) == {"h/me": [NAME]}
            assert relay.held_all({NAME: dict(rs=None, st={})}) == {"h/x": [other], "h/old": [NAME]}
    finally:
        if old is None:
            os.remove(relay.STATUS)
        else:
            open(relay.STATUS, "w").write(old)
    ok("做了一半的任务认回原来那台：这台用刚读到的（读不到时用 status.json），别的执行机暂停着、不在机器表里时放开")


def test_tilde_root():
    """根目录写成 ~/…：开头的 ~ 留给那边展开，其余加引号。"""
    with mpatch(NAME, root="~/st/t 123"), mpatch("site-pueue", root="~/pq"):
        p = RM.prefix(NAME)
        assert p == f"cd ~/'st/t 123'/code/current && TASKQ_ROOT=~/'st/t 123' TASKQ_MACHINE={NAME}", p
        c = relay.SL.submit_cmd(NAME, "long", "1:00:00", "gpu")
        assert c.startswith("R=~/'st/t 123'; ") and '"~' not in c and '--chdir="$R"' in c, c
        w = relay.PQ.worker_cmd("site-pueue", M.workers("site-pueue")[0])
        assert "R=~/pq;" in w and '"~' not in w, w
        assert RM.root(NAME) == "~/st/t 123"
    ok("根目录 ~/…：命令里 ~ 不加引号（那边展开），交作业、开 worker 的命令也是")


def test_adapters_in_relay():
    """调度器的读写都经 sites/pueue.py、slurm.py。"""
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
    ok("relay：一次 ssh 读回原始输出、在 hub 上用适配器解析（这台各个 worker 的组）；开 worker / 交作业用适配器，"
       "报警的编号：pueue 按 worker，Slurm 的 GPU 照旧、CPU 加 :cpu")


# ---------------- 2. 拉回时挪进数据目录的顺序 ----------------
@contextlib.contextmanager
def spy_replace(D, fail_at=None):
    """记下挪到 D 下的路径（os.replace 的目标；软链接也是建在临时名上再换过去）；第 fail_at 个出错。"""
    seen, real = [], os.replace

    def spy(a, b):
        if os.path.commonpath([os.path.abspath(b), D]) == D:
            seen.append(os.path.relpath(b, D))
            if len(seen) == fail_at:
                raise OSError("盘满了")
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
    with spy_replace(D, fail_at=3) as seen:  # 普通文件 2 个之后的第一个标志
        try:
            SO.place(src, D, written, sp)
            raise AssertionError("应出错")
        except OSError:
            pass
    assert not os.path.exists(os.path.join(D, sp["out"], "check/summary.json"))
    assert not os.path.exists(os.path.join(D, sp["out"], "final.ckpt"))
    SO.place(src, D, [w for w in written if os.path.exists(os.path.join(src, w))], sp)
    assert open(os.path.join(D, "results/X/tr/plans.json"), "rb").read() == b"old"
    assert seen[:2] == ["results/X/tr/frag_0/check/a.dat", "logs/tasks/X__tr__0__1_part1.log"], seen
    assert os.path.exists(os.path.join(D, sp["out"], "check/summary.json"))
    ok("拉回：做完的标志最后挪（中途出错时不算做完），上一级已有的 JSON 不覆盖")

    src, D = os.path.join(T, "place2_src"), os.path.join(T, "place2_dst")
    sp = dict(key="m/0", out="preds/m/frag_0", done=["DONE"], fetch=dict(extra=[]))
    written = ["preds/m/frag_0/DONE", "preds/m/frag_0/MANIFEST.json", "preds/m/frag_0/c1.dat", "preds/m/note.txt"]
    for w in written:
        TN.wb(os.path.join(src, w), b"n")
    with spy_replace(D) as seen:
        SO.place(src, D, written, sp)
    assert seen == ["preds/m/frag_0/c1.dat", "preds/m/note.txt", "preds/m/frag_0/MANIFEST.json", "preds/m/frag_0/DONE"], seen
    ok("拉回：MANIFEST.json 在做完的标志前、普通文件后挪")

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
    ok("拉回：done 的几项倒着放，第一项最后；产出里的软链接写成相对链接，在普通文件后、标志前放")


# ---------------- 3. 真跑 ----------------
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
    ok(f"代码包 {bid} 推到 store（hub 的根目录）和执行机、切换 current，那边的队列开着、是空的")

    assert M.machine(NAME).get("ssh_master")
    open(os.path.join(T, "master_down"), "w").close()
    assert relay.remote_status(NAME) is None
    os.remove(os.path.join(T, "master_down"))
    assert relay.remote_status(NAME)["bundle"] == bid
    ok("ssh_master：长连接断着 → 当连不上，不另起连接；通着照常读")

    if os.path.exists(os.path.join(T, "srun_args")):
        os.remove(os.path.join(T, "srun_args"))
    assert RM.op(NAME, "missing", ["ab" * 32], job="-p cpu -t 10") == ["ab" * 32]
    sa = open(os.path.join(T, "srun_args")).read()
    assert f"-I{RM.JOB_WAIT} -p cpu -t 10 -J " in sa and " bash -c " in sa, sa
    open(os.path.join(T, "no_slot"), "w").close()
    try:
        RM.op(NAME, "missing", [], job="-p cpu")
        raise AssertionError("应出 NoSlot")
    except RM.NoSlot:
        pass
    finally:
        os.remove(os.path.join(T, "no_slot"))
    ok("transfer_job：收进 blobs 在 srun 的 CPU 作业里跑（-I 等排队）；排不上 → NoSlot（这次不传）")

    said = []
    os.remove(os.path.join(T, "srun_args"))
    assert RT.stage(NAME, sp["key"], nice=5, say=said.append) == sp["key"]
    assert "-p test -J " in open(os.path.join(T, "srun_args")).read()  # 收进 blobs 用机器表的 transfer_job
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
    ok(f"传数据：{blobs} 个文件按内容存、软链接放在和 store 一样的路径，key 和参数原样，加进那边的队列；sha256 缓存在 store 的 state/hashes.db")

    rq = os.path.join(RROOT, "queue", "tasks.jsonl")
    recs = [json.loads(x) for x in open(rq) if x.strip()]
    want = S.sendable(S.resolve(sp["recipe"], sp["params"]))
    assert len(recs) == 1 and recs[0]["spec"] == want and recs[0]["recipe"] == sp["recipe"] and recs[0]["params"] == sp["params"], recs
    assert not {"machines", "after", "names", "exp"} & set(recs[0]["spec"]), recs[0]["spec"]
    ok("那边队列的记录带 hub 解析好的说明（去掉 machines/after/names/exp），做法和参数也留着")

    with open(rq + ".new", "w") as f:
        f.write(json.dumps(dict(recs[0], spec=dict(recs[0]["spec"], env="old")), ensure_ascii=False) + "\n")
    os.replace(rq + ".new", rq)
    RT.resend(NAME, say=lambda m: None)
    recs = [json.loads(x) for x in open(rq) if x.strip()]
    assert len(recs) == 1 and recs[0]["spec"] == want, recs
    ok("resend：那边队列里的说明换成现在解析的，不加新任务")

    said.clear()
    assert RT.stage(NAME, sp["key"], nice=5, say=said.append) == sp["key"] and nblobs() == blobs
    assert any("那边缺 0 个" in m for m in said), said
    recs = [json.loads(x) for x in open(rq) if x.strip()]
    assert len(recs) == 1, recs
    ok("再传一次：不重传、不重复加")

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
    os.symlink(os.path.realpath(g0), os.path.join(rout, "refs", "blob.dat"))  # 解开过的输入（直接指向 blob）
    os.symlink(os.path.relpath(g0, os.path.join(rout, "refs")), os.path.join(rout, "refs", "view.dat"))  # 指向视图里的输入
    os.symlink("../final.ckpt", os.path.join(rout, "refs", "best.ckpt"))  # 产出里的
    log = S.safe(sp["key"]) + "__12345_part1.log"
    TN.wb(os.path.join(D, "logs", "tasks", log), b"log")
    out = os.path.join(TN.ROOT, sp["out"])
    inbox = os.path.join(TN.ROOT, proto.RELAY_INBOX, S.safe(sp["key"]))
    os.environ["TUNNEL_TEST_BADMD5"] = sp["out"] + "/final.ckpt"
    try:
        RT.fetch(NAME, sp["key"], say=lambda m: None)
        raise AssertionError("有文件没拉到应报错")
    except RT.RN.RelayError as e:  # store 上拉出错也是 RelayError；site_ops 报的那一行留着（隧道的进度行可能排在它后面）
        assert "pull: " in str(e) and ("没拉到" in str(e) or "对不上" in str(e)), e
    finally:
        del os.environ["TUNNEL_TEST_BADMD5"]
    assert not os.path.exists(out) and os.path.isfile(os.path.join(inbox, sp["out"], "check", "summary.json"))
    sj = os.path.join(inbox, sp["out"], "check", "summary.json")
    TN.wb(sj, b"x" * os.path.getsize(sj))  # 大小一样、修改时间不同
    TN.wb(os.path.join(inbox, sp["out"], "check", "old.dat"), b"old")
    TN.wb(os.path.join(inbox, sp["out"], "debug.json.tunnelpart"), b"{")
    said.clear()
    RT.fetch(NAME, sp["key"], say=said.append)
    assert any("拉 2 个文件" in m for m in said), said
    assert not os.path.exists(os.path.join(out, "check", "old.dat")) and not os.path.exists(os.path.join(out, "debug.json.tunnelpart"))
    assert json.load(open(os.path.join(out, "check", "summary.json"))) == dict(metric_per_case=[{}, {}])
    ok("拉回到一半（上次缺文件报错，store 上什么都没放）：再拉只拉缺的和大小、修改时间对不上的，旧的、半份的不混进结果")
    assert sorted(f for f in os.listdir(os.path.join(out, "check")) if f.endswith(".dat")) == sorted(c + ".dat" for c in TN.CASES[:2])
    assert not os.path.exists(os.path.join(out, "latest.ckpt"))
    assert open(os.path.join(out, "final.ckpt"), "rb").read() == b"ck"
    assert os.path.exists(os.path.join(os.path.dirname(out), "plans.json"))
    assert not os.listdir(os.path.join(TN.ROOT, proto.RELAY_INBOX))
    logs = os.listdir(os.path.join(TN.ROOT, "logs", "tasks"))
    assert logs == [log], logs
    ok("拉回：同样的相对路径、不拉 latest.ckpt、上一级的 json 和分段日志、临时目录删掉")
    gin = os.path.join(TN.ROOT, "prepared", TN.DS, "gt", TN.CASES[0] + ".dat")
    for f in ("blob.dat", "view.dat"):
        p = os.path.join(out, "refs", f)
        assert os.readlink(p) == os.path.relpath(gin, os.path.dirname(p)) and os.path.samefile(p, gin), (f, os.readlink(p))
    assert os.readlink(os.path.join(out, "refs", "best.ckpt")) == "../final.ckpt"
    ok("拉回带软链接：指向输入的（直接指向 blob 的、指向视图的）在 store 上换成指向那个输入的相对链接；产出里的链接照原样")

    relay.write_status(NAME, relay.remote_status(NAME), ST.load_state())
    st = json.load(open(relay.STATUS))[NAME]
    assert sp["key"] in st["tasks"] and st["tasks"][sp["key"]]["final"], st
    ok("status.json 里有这个任务")
    return pre


def test_e2e_path(pre):
    """通用换名的任务：和上面训练的视图同时在那边，传数据只动自己的视图；拉回一半不算做完。"""
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
    assert walk(pre) == before, "训练的视图被动了"
    TN.wb(os.path.join(D, "proj/inputs/cohortA", "stray.txt"), b"?")
    blob = next(os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(RROOT, "blobs")) for f in fs if not f.endswith(proto.BLOB_STAT))
    os.symlink(blob, os.path.join(D, "proj/inputs/cohortA", "stale"))
    RT.stage(NAME, sp["key"], add=False, say=lambda m: None)
    assert not os.path.lexists(os.path.join(D, "proj/inputs/cohortA", "stale"))
    assert os.path.exists(os.path.join(D, "proj/inputs/cohortA", "stray.txt"))  # 不是指向 blobs 的链接：不碰
    os.remove(os.path.join(D, "proj/inputs/cohortA", "stray.txt"))
    ok("通用插件传数据：文件夹是视图、文件和通配符展开的文件放在同样的路径；视图里只删多出来的指向 blobs 的链接；训练的视图不动")

    TN.wb(os.path.join(D, sp["out"], "a.csv"), b"1")
    TN.wb(os.path.join(D, os.path.dirname(sp["out"]), "plans.json"), b"{}")
    log = S.safe(sp["key"]) + "__9_part1.log"
    TN.wb(os.path.join(D, "logs", "tasks", log), b"log")
    try:
        RT.fetch(NAME, sp["key"], say=lambda m: None)
        raise AssertionError("少 .done 应报没做完")
    except relay.RN.RelayError as e:
        assert "还没" in str(e), e
    assert not os.path.exists(os.path.join(TN.ROOT, sp["out"]))
    os.symlink("/etc/hostname", os.path.join(D, sp["out"], "outside"))
    TN.wb(os.path.join(D, sp["out"], ".done"), b"")
    try:
        RT.fetch(NAME, sp["key"], say=lambda m: None)
        raise AssertionError("指到数据目录外的链接应报错")
    except relay.RN.RelayError as e:
        assert "指到别处的链接" in str(e) and "outside" in str(e), e
    assert not os.path.exists(os.path.join(TN.ROOT, sp["out"]))
    os.remove(os.path.join(D, sp["out"], "outside"))
    RT.fetch(NAME, sp["key"], say=lambda m: None)
    out = os.path.join(TN.ROOT, sp["out"])
    assert sorted(os.listdir(out)) == [".done", "a.csv"], os.listdir(out)
    assert os.path.exists(os.path.join(TN.ROOT, "proj/analysis/thing/plans.json"))
    assert os.path.exists(os.path.join(TN.ROOT, "logs", "tasks", log))
    ok("通用插件拉回：那边没做完不拉、产出里有指到数据目录外的链接不拉；做完后结果、上一级的文件、分段日志放进 store")

    keys = ST.keys_of(ST.load_state(), NAME)
    tk = next(r for r in keys if r != sp["key"])
    assert ST.droppable(keys, sp["key"], ST.paths_of(keys[sp["key"]])) == sorted(ST.paths_of(keys[sp["key"]]))
    RT.drop_remote(NAME, sp["key"], sp["key"], gone=True)
    assert not os.path.lexists(os.path.join(D, "raw/Set/c1/c1-seg.dat")) and not os.path.exists(os.path.join(D, "proj/inputs"))
    assert not os.path.exists(os.path.join(D, sp["out"])) and os.listdir(pre) and sp["key"] not in ST.keys_of(ST.load_state(), NAME)
    assert not [f for f in os.listdir(os.path.join(D, "logs", "tasks")) if f.startswith(S.safe(sp["key"]))]
    ok(f"删任务：它独占的输入、产出、日志删掉，空了的上级目录删掉；别的任务（{tk}）的视图不动")

    for bad in ("/etc", "a/../../x", "in"):
        try:
            ST.safe_rel(bad)
            raise AssertionError(bad)
        except relay.RN.RelayError:
            pass
    ok("要删的、要建的路径在数据目录里、至少两层")


def other_disk(R):
    """和 R 不在同一个盘的临时目录（/dev/shm）；没有 → None。"""
    if not os.path.isdir("/dev/shm"):
        return None
    d = tempfile.mkdtemp(prefix="test_relay_", dir="/dev/shm")
    if os.stat(d).st_dev == os.stat(R).st_dev:
        shutil.rmtree(d)
        return None
    return d


def test_status_blob_cap():
    """读状态：blobs 指到另一个盘、写了 blob_cap → blob_free = min(那个盘剩的, blob_cap − blobs 已占的)；没写 → 那个盘剩多少。"""
    B0 = os.path.join(RROOT, "blobs")
    shm = other_disk(RROOT)
    if not shm:
        print("SKIP:没有另一个盘可用（/dev/shm）")
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
    ok("读状态：blobs 在另一个盘、写了 blob_cap → blob_free 按 blob_cap 减去 blobs 已占的算；没写 → 那个盘剩多少")


def test_env_remote():
    """真跑（假 ssh）：状态读回环境的标志文件；建之前查空间写 .building / .failed（按 cap）；ensure_blobs 把 store 的文件放进 blobs。"""
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
        assert st == {"base": "空间不够"}, st
        os.remove(os.path.join(sd, "base.failed"))
    ok("那边：按要的空间（有 cap 时按 cap 减已占的）写 .building 或 .failed nospace；状态读回来")
    src = os.path.join(T, "envblob.bin")
    open(src, "wb").write(b"wheel-bytes")
    hh = SO.hashes(HROOT, [src])[src]
    assert hh == relay.C.sha256(src) and SO.hashes(HROOT, [src]) == {src: hh}
    said = []
    got = RT.ensure_blobs(NAME, {hh: [None, os.path.getsize(src), src]}, say=said.append)
    assert got == {hh: f"{RM.root(NAME)}/{proto.blob(hh)}"} and open(os.path.join(RROOT, proto.blob(hh)), "rb").read() == b"wheel-bytes"
    said.clear()
    assert RT.ensure_blobs(NAME, {hh: [None, os.path.getsize(src), src]}, say=said.append) == got
    assert any("那边缺 0 个" in m for m in said), said
    ok("ensure_blobs：store 的文件放进那边的 blobs，已经有的不再传，返回那边的路径")


def test_env_auto():
    """按描述建的环境自动建：没建的排一个 hub 任务（基础环境先建、一台一次一个），hub 上已经排着、那边在建的不重排；
    开关关着只记日志；空间不够、建坏了报 site:<机器>:env:<环境>；长连接断着不连。"""
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
        ok(f"没建：hub 的 {proto.IO_GROUP} 组排 taskq site env（基础环境先建，变体按条目），一次一个")
        adds.clear()
        relay.env_builds({"pueue": {hub: {"tasks": [dict(id=3, label=lab("base"), status="Running")]}}}, NAME, {}, now, False, {})
        hb = EV.vhash("base", "cu128")
        relay.env_builds(s, NAME, {"base": dict(building=f"{hb} {now - 60}")}, now, False, {})
        relay.env_builds({"pueue": {hub: {"error": "x"}}}, NAME, {}, now, False, {})
        assert adds == [], adds
        ok("hub 上已经排着、那边 .building 是新的、读不到 hub 的 pueue：不重排")
        relay.env_builds(s, NAME, {"base": dict(ok=hb, active=True)}, now, False, {})
        assert len(adds) == 1 and adds[0][1] == lab("alt") and "--variant default" in adds[0][4], adds
        adds.clear()
        ok("基础环境好了才建建在它上面的 venv")
        st = {}
        relay.env_builds(s, NAME, {"base": dict(failed=f"{hb} {now - 60} nospace 5000000000 1000000000")}, now, False, st)
        a = st.get(f"site:{NAME}:env:base")
        assert adds == [] and a and proto.alert_data(a)["state"] == "空间不够" and "5.0 GB" in proto.alert_msg(a), st
        st = {}
        relay.env_builds(s, NAME, {"base": dict(failed=f"{hb} {now - 60} rc 1", log="...\npip 坏了\n")}, now, False, st)
        assert adds == [] and "pip 坏了" in proto.alert_msg(st[f"site:{NAME}:env:base"]), st
        ok("空间不够、建坏了：不重排，报 site:<机器>:env:<环境>（带要多少剩多少、日志最后几行）")
        relay.P.AUTO["envs"] = False
        st = {}
        relay.env_builds(s, NAME, {}, now, False, st)
        relay.env_builds(s, NAME, {}, now, False, st)
        assert adds == [] and not st and len([x for x in logs if "开关没开" in x]) == 1, logs
        assert relay.env_builds(s, NAME, {}, now, True, {}) and adds == []
        ok("AUTO['envs'] 关着：不排、不报，只记一次 ~/relay.log；dry 只说会建")
        relay.P.AUTO["envs"] = {NAME: ["alt"]}
        M.MACHINES[NAME]["envs"] = {"base": ("conda", "x")}
        relay.env_builds(s, NAME, {}, now, False, {})
        assert [x[1] for x in adds] == [lab("base")] and "--variant default" in adds[0][4], adds
        adds.clear()
        ok("AUTO['envs'] = {机器: [环境]}：条目还不是 spec 的也预建（预建的 venv 用按描述建的基础环境，基础环境先建）")
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
        ok("每次检查读状态时顺带建环境（没有它的任务也建）；ssh 长连接断着：读不到、不建、不另起连接")


def test_run_lines():
    """边跑边交出标准错误：每一行都交给 say、都在返回的标准错误里（不和读标准输出的抢行），标准输入送到、标准输出完整；超时 → 124。"""
    prog = ("import sys\nd = sys.stdin.read()\nfor i in range(3000):\n    print(f'进度 {i}', file=sys.stderr, flush=True)\n"
            "print('pull: 最后一行', file=sys.stderr)\nprint(d * 2)\nsys.exit(3)\n")
    for _ in range(5):
        said = []
        rc, out, err = RM.run_lines([sys.executable, "-c", prog], "ab", 60, said.append)
        lines = err.splitlines()
        assert rc == 3 and out == "abab\n" and len(lines) == 3001 and lines[-1] == "pull: 最后一行" and said == lines, (rc, out, len(lines))
    rc, out, err = RM.run_lines([sys.executable, "-c", "import time; time.sleep(30)"], "", 1, lambda m: None)
    assert rc == 124 and "超过 1 秒" in err, (rc, err)
    ok("run_lines：标准错误一行不丢（交给 say 的和返回的一样）、标准输入送到、标准输出完整；超时 → 124")


def test_err_tail():
    """那边出错时的输出：错误行被后面的进度挤出最后 300 字时放在前面；在尾部里时只给尾部。"""
    prog = "\n".join(f"tunnel: {i} MB/s" for i in range(100))
    tb = "Traceback (most recent call last):\n  File \"x.py\", line 1\nFileNotFoundError: [Errno 2] 没有 a.dat"
    for text, want in ((tb + "\n" + prog, "FileNotFoundError: [Errno 2] 没有 a.dat"), ("pull: 3 个文件没拉到\n" + prog, "pull: 3 个文件没拉到"),
                       ("错误：盘满了\n" + prog, "错误：盘满了"), ("srun: error: 节点没了\n" + prog, "srun: error: 节点没了")):
        got = RM.err_tail(text, own=("pull: ",))
        assert got.startswith(want + " …… ") and got.endswith(prog[-100:]) and len(got) <= len(want) + 306, got
    assert RM.err_tail(prog) == prog[-300:] and RM.err_tail("x\n" + tb) == ("x\n" + tb)[-300:]
    assert not RM.err_tail("pull: 没拉到\n" + prog).startswith("pull:")  # 不是这个操作的前缀：不认
    ok("那边出错的输出：最后一条错误行（Traceback 的最后一行、错误、<程序>: error、site_ops 的 <操作>: ）被进度挤掉时放在前面，再加尾部")

    def op_fail(*a, **kw):
        raise RuntimeError("那台上 site_ops 出错（1）：x")
    with patched(RT, lookup=lambda k: dict(key=k, out="out/k", done=[], recipe="toy", params={})), \
            patched(ST, keys_of=lambda st, n: {"k": dict(outs=["out/k"])}), \
            patched(RM, op=lambda *a, **kw: dict(done=True, bad=[], items=[], links=[]), store_op=op_fail):
        try:
            RT.fetch(NAME, "k", say=lambda m: None)
            raise AssertionError("store 上拉出错应报 RelayError")
        except RT.RN.RelayError as e:
            assert "拉回 store 出错" in str(e) and "site_ops 出错" in str(e), e
    with patched(RT, lookup=lambda k: dict(key=k, out="out/k", done=[], recipe="toy", params={})), \
            patched(ST, keys_of=lambda st, n: {"k": dict(outs=["out/k"])}), patched(RM, op=op_fail):
        try:
            RT.fetch(NAME, "k", say=lambda m: None)
            raise AssertionError("那边列产出出错应报 RelayError")
        except RT.RN.RelayError as e:
            assert "列 k 的产出出错" in str(e), e
    ok("fetch：那边列不出、store 上拉出错都报 RelayError（和没做完、没记录一样），调用方按一种错处理")


# ---------------- 4. site_ops 的单个操作 ----------------
def run_op(what, R, data, wait=True):
    """在本机跑 site_ops.py 的一个操作（根目录 R）→ (退出码, 结果, 标准错误)；wait=False → 进程。"""
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
    """R 的 blobs 里放一个 blob（记下大小和修改时间，和收进来时一样）→ sha256。"""
    import hashlib
    h = hashlib.sha256(body).hexdigest()
    p = os.path.join(R, proto.blob(h))
    TN.wb(p, body)
    SO.record(R, h)
    return h


def blocked_on_lock(pid):
    """/proc/locks 里有这个进程在等的锁（"->" 开头的行）。"""
    with open("/proc/locks") as f:
        return any(ln.split()[1] == "->" and ln.split()[5] == str(pid) for ln in f if len(ln.split()) > 5)


def test_view_lock():
    """建视图在那边一次只跑一个（同一个数据集的两折同时建视图会互相删 .incomplete）。"""
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
    ok("建视图：别的在建时等锁，建完删掉 .incomplete")


def test_view_no_overwrite():
    """视图从不盖掉真文件：位置上已有别的真文件（那边写出的产出）→ 报错、文件不动、.incomplete 留着（不算建好）；
    已是那个 blob 本身（同一个 inode，同盘收进来的前一步产出）→ 不动、照常建好。缺 blob → 什么都不放就退出。"""
    R = fixture.tmpdir(dir=T, prefix="vo_")
    D = os.path.join(R, "data")
    h, h2 = put_blob(R, b"input-a"), put_blob(R, b"input-b")
    links = {"prepared/D1/a.dat": h}
    assert run_op("view", R, dict(views=["prepared/D1"], links=links, texts={}))[:2] == (0, 1)
    TN.wb(os.path.join(D, "prepared/D1/out.dat"), b"real output")
    rc, _, err = run_op("view", R, dict(views=["prepared/D1", "prepared/D2"], links=dict(links, **{"prepared/D1/out.dat": h2, "prepared/D2/b.dat": h2}),
                                        texts={}))
    assert rc != 0 and "不盖" in err and "out.dat" in err, (rc, err)
    assert open(os.path.join(D, "prepared/D1/out.dat"), "rb").read() == b"real output" and not os.path.islink(os.path.join(D, "prepared/D1/out.dat"))
    assert os.path.exists(os.path.join(D, "prepared/D1", proto.INCOMPLETE))
    assert sorted(os.listdir(os.path.join(D, "prepared/D2"))) == ["b.dat"], os.listdir(os.path.join(D, "prepared/D2"))
    shutil.rmtree(os.path.join(D, "prepared/D2"))
    os.remove(os.path.join(D, "prepared/D1/out.dat"))
    os.link(os.path.join(R, proto.blob(h2)), os.path.join(D, "prepared/D1/out.dat"))
    assert run_op("view", R, dict(views=["prepared/D1"], links=dict(links, **{"prepared/D1/out.dat": h2}), texts={}))[0] == 0
    assert not os.path.islink(os.path.join(D, "prepared/D1/out.dat")) and not os.path.exists(os.path.join(D, "prepared/D1", proto.INCOMPLETE))
    ok("视图不盖真文件：位置上是别的真文件 → 报错、不动它、那个视图不算建好（留着 .incomplete），同一次的别的视图照常建好、"
       "不留 .incomplete；就是那个 blob 本身（硬链接）→ 不动")

    before = walk(os.path.join(D, "prepared/D1"))
    rc, _, err = run_op("view", R, dict(views=["prepared/D1"], links=dict(links, **{"prepared/D1/b.dat": "cd" + "0" * 62}),
                                        texts={"prepared/D1/x.json": "e30="}))
    assert rc != 0 and "缺 blob" in err, (rc, err)
    assert walk(os.path.join(D, "prepared/D1")) == before, walk(os.path.join(D, "prepared/D1"))
    ok("建视图缺 blob：不放 .incomplete、不改原来的视图就退出（下次传数据重建）")


def test_export_plugin_error():
    """store 上 export 时插件出了别的错（读不到文件）：报一行原因（插件名、函数、错误），不带 traceback。"""
    R, D = fixture.tmpdir(dir=T, prefix="ex_"), fixture.tmpdir(dir=T, prefix="exd_")
    rc, res, err = run_op("export", R, dict(spec=S.resolve("toy", dict(ds="D404_none", frag="0")), data=D))
    assert rc != 0 and res is None and "Traceback" not in err and len(err.strip().splitlines()) == 1, (rc, err)
    assert "插件 toy 的 export_toy 出错：FileNotFoundError" in err and "dataset.json" in err, err
    ok("export 时插件读不到文件：报一行（插件名、函数、错误），不带 traceback")


def test_list():
    """拉回前列结果：做完的判断（字符串、a|b、缺一项、空 done）；软链接换成逻辑路径；读不了的子目录 → 出错。"""
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
    ok("拉回前列结果：做完的判断和 spec.is_done 一样（字符串、a|b、缺一项、空 done）；不存在的结果文件夹跳过")

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
    os.symlink("/etc/hostname", os.path.join(out, "l_skip.tmp"))  # 不拉的名字（exclude、写到一半的）：链接也不拉、不算 bad
    os.symlink("part", os.path.join(out, "x_dir"))
    rc, res, err = run(["out/k"], dict(spec, fetch=dict(exclude=["x_*"])), links={"prepared/D1/a.dat": h})
    assert rc == 0 and sorted(res["links"]) == [["out/k/l_blob", "prepared/D1/a.dat"], ["out/k/l_dir", "out/k/part"],
                                                 ["out/k/l_own", "out/k/check/DONE"], ["out/k/l_view", "prepared/D1/a.dat"]], (res, err)
    assert sorted(res["bad"]) == ["out/k/l_out", "out/k/l_stray"] and not any(i[0].startswith("out/k/l_") for i in res["items"]), res
    for f in ("l_blob", "l_view", "l_own", "l_dir", "l_stray", "l_out", "l_skip.tmp", "x_dir"):
        os.remove(os.path.join(out, f))
    ok("拉回前列结果：产出里的软链接换成逻辑路径（指向 blob 的按这个任务的视图换回输入的路径，指向数据目录里的按路径），"
       "别的 blob、数据目录外的 → bad；exclude、写到一半的名字先滤掉，这样的链接不拉")

    os.chmod(os.path.join(out, "part"), 0)
    try:
        rc, res, err = run(["out/k"])
    finally:
        os.chmod(os.path.join(out, "part"), 0o755)
    assert rc != 0 and res is None, (rc, res, err)
    ok("拉回前列结果：读不了的子目录 → 出错（不拉回、不算做完）")


def test_logs_exact():
    """拉回、删任务时只认这个任务自己的分段日志：key a/b 不带上 a/b/c 的。"""
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
    ok("分段日志按 <key>__<作业>_part<段>.log 认：前缀相同、多一段的 key（a/b 和 a/b/c）的日志不拉、不删")


def test_ingest():
    """收进 blobs（ingest）：同一个盘、大小和 sha256 对得上 → 硬链接进来、记下；blobs 在别的盘时来源不读、不算 sha256，直接跳过。
    硬链接的源头被原地改过 → blob 跟着变了：missing 认出来、丢掉记录和 blob（源头不动），再收时按新的内容核对。"""
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
    ok("收进 blobs：同一个盘、大小和 sha256 对得上的硬链接进来、记下大小和修改时间；大小、sha256 对不上、没有相对路径的不收")

    with open(src, "r+b") as f:  # 源头原地改（同样大小）：blob 是同一个 inode，跟着变
        f.write(b"Z" * 10)
    rc, miss, err = run_op("missing", R, [h])
    assert rc == 0 and miss == [h] and "丢掉重收" in err, (rc, miss, err)
    assert not os.path.lexists(b) and not os.path.lexists(b + proto.BLOB_STAT) and open(src, "rb").read(10) == b"Z" * 10
    assert run_op("ingest", R, dict(srcs=srcs, files={h: ["res/ck.pth", 5000]}))[:2] == (0, [])  # 内容不是 h 了：不收（由 store 重送）
    TN.wb(src, body)
    assert run_op("ingest", R, dict(srcs=srcs, files={h: ["res/ck.pth", 5000]}))[:2] == (0, [h])
    assert run_op("missing", R, [h])[:2] == (0, []) and open(b, "rb").read() == body
    ok("硬链接的源头被原地改过：missing 认出来、丢掉 blob 和记录（源头不动）；内容对不上不收，对得上再收进来")

    R2 = fixture.tmpdir(dir=T, prefix="ing2_")
    shm = other_disk(R2)
    if not shm:
        print("SKIP:没有另一个盘可用（/dev/shm）")
        return
    try:
        os.symlink(shm, os.path.join(R2, "blobs"))
        src2 = os.path.join(R2, "data", "res", "ck.pth")
        TN.wb(src2, body)
        os.chmod(src2, 0)  # 读它就出错
        try:
            rc, got, err = run_op("ingest", R2, dict(srcs=[os.path.join(R2, "data")], files={h: ["res/ck.pth", 5000]}))
        finally:
            os.chmod(src2, 0o644)
        assert rc == 0 and got == [] and not os.listdir(shm), (rc, got, err)
    finally:
        shutil.rmtree(shm)
    ok("收进 blobs：blobs 在别的盘时来源不读、不算 sha256，直接跳过（由 store 送）")


def test_pin_refresh():
    """传数据期间定时重写钉子（那边的清理只认 12 小时内的），传完撤掉。"""
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
    assert len(pins) == n, "传完以后不再写"
    ok("传数据期间钉子定时重写，传完撤掉、不再写")


# ---------------- 5. 推代码包 ----------------
def test_deploy_cleanup():
    """清理旧包：留下 current、最近的 3 个、4 天内碰过的（在跑的 worker 每圈碰自己的包），和还领着的任务里最早的领取前一小时起碰过的
    （worker 换包后接着跑的任务还在用领取时的包）；不知道那边领着什么（claims=None）时不删旧包；被打断的部署留下的临时文件 1 天后删。"""
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
    ok("清理旧包：留下 current、最近的 3 个、4 天内碰过的、最早的领取前一小时起碰过的（换包后接着跑的任务用的旧包）；"
       "不知道那边领着什么时不删旧包；被打断的部署留下的临时文件 1 天后删")


def test_deploy_safety():
    """推代码包：传了一半不装、current 不动；重推一个已有的旧包时不当场删掉它；同时推两次时 current 只指向完整的包。"""
    import tarfile
    import threading
    src = os.path.join(T, "ds_src")
    n_files = 200
    for i in range(n_files):
        TN.wb(os.path.join(src, "d", f"f{i}"), os.urandom(4096))
    full = lambda r, b: sum(len(fs) for _, _, fs in os.walk(os.path.join(r, "code", b)))  # noqa: E731
    tb = subprocess.run(["tar", "-C", src, "-cf", "-", "."], capture_output=True, check=True).stdout
    at = tarfile.open(fileobj=io.BytesIO(tb)).getmembers()[100].offset  # 断在两个文件之间：tar 不报错
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
            raise AssertionError("传了一半应出错")
        except RuntimeError:
            pass
        finally:
            os.environ["RELAY_SSH"] = FAKE
        assert os.readlink(os.path.join(r, "code", "current")) == "good" and not os.path.exists(os.path.join(r, "code", "half")), \
            os.listdir(os.path.join(r, "code"))
        ok("推代码包：传了一半 → 出错，不装、current 不动")

        old = time.time() - 10 * 86400
        for i, d in enumerate("abcd"):
            os.makedirs(os.path.join(r, "code", d))
            t = old if d == "a" else time.time() - 600 * (4 - i)
            os.utime(os.path.join(r, "code", d), (t, t))
        relay.deploy(NAME, "a", src)
        assert os.readlink(os.path.join(r, "code", "current")) == "a" and os.path.isdir(os.path.join(r, "code", "a")), \
            os.listdir(os.path.join(r, "code"))
        ok("重推一个那边已有的旧包（回退）：切过去，不被同一次的清理删掉")

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
    ok("同时推几次（同一个包、不同的包）：都完整，current 指向完整的包")


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
    print("全部通过")
