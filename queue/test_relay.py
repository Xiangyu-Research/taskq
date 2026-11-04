#!/usr/bin/env python3
"""relay.py 的测试：
  1. 每次检查的决策（plan / fill）：假的总队列、那边的状态、box 的任务；
  2. 拉回时挪进数据目录的顺序：中途出错时训完的标志不在（不会被当成训完）；
  3. 真跑一遍：本机模拟"那边"（假的 ssh、srun 在本地跑，服务开在 127.0.0.1），推代码包、传数据（stage）、
     那边的队列、拉回（fetch），和 box 一样的真名路径；
  4. 删任务时按引用计数只删别的任务不用的路径；redo（重新传）。
跑法：python3 scripts/taskq/queue/test_relay.py（配置用测试用的 testdata/；包里的 Python 文件用本机 bundle_check 那个环境核对，没有时用当前的 Python）"""
import contextlib
import io
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import cfg as _cfg  # noqa: E402  调度代码的根、使用者的仓库根
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
STATE_DIR = tempfile.mkdtemp(prefix="test_relay_state_")
os.environ["TASKQ_STATE"] = STATE_DIR  # 本机的状态（时段、队列…）放临时目录
sys.path.insert(0, HERE)
import test_relay_plugins as TN  # noqa: E402  （设了 RELAY_DIR、建了临时目录 TN.TMP）

T = TN.TMP
FAKE = os.path.join(T, "fake_ssh")
with open(FAKE, "w") as f:
    f.write(f'#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\n[[ $1 == -O ]] && {{ [[ -e {T}/master_down ]] && exit 255; exit 0; }}\n'
            f'shift\ncd / && exec env -u TASKQ_CONFIG -u TASKQ_STATE TASKQ_PLUGINS={T}/remote/code/current/scripts/taskq/queue/testdata/plugins bash -c "$*"\n')
os.chmod(FAKE, 0o755)
BIN = os.path.join(T, "bin")
os.makedirs(BIN)
with open(os.path.join(BIN, "srun"), "w") as f:  # 假的 srun：跳过选项，从 python3 那一项起照跑
    f.write(f"#!{sys.executable}\nimport os,sys\na=sys.argv[1:]\n"
            f"open({os.path.join(T, 'srun_args')!r},'a').write(' '.join(a)+chr(10))\n"
            f"if os.path.exists({os.path.join(T, 'no_slot')!r}):\n    sys.stderr.write('srun: error: Unable to allocate resources: Immediate')\n    sys.exit(1)\n"
            "i=next(i for i,x in enumerate(a) if x.endswith('python3') or x=='bash')\nos.execvp(a[i],a[i:])\n")
os.chmod(os.path.join(BIN, "srun"), 0o755)
s = socket.socket()
s.bind(("127.0.0.1", 0))
PORT = s.getsockname()[1]
s.close()
os.environ.update(RELAY_SSH=FAKE, TUNNEL_SSH=FAKE, TUNNEL_URL=f"http://127.0.0.1:{PORT}", TUNNEL_BIND=f"127.0.0.1:{PORT}",
                  TUNNEL_POLL="1", TUNNEL_LOCK=os.path.join(T, "lock"), PHD_TASKS=os.path.join(T, "tasks.json"),
                  PHD_LAST=os.path.join(T, "last"), RELAY_LOG=os.path.join(T, "relay.log"),
                  PATH=BIN + ":" + os.environ["PATH"])

import cfg  # noqa: E402

cfg.use_config()
import machines as M  # noqa: E402
import spec as S  # noqa: E402
import relay  # noqa: E402
import relay_bundle  # noqa: E402

NAME = next(iter(M.RELAY))
RROOT = os.path.join(T, "remote")
CONDA = os.path.join(T, "conda")
os.makedirs(os.path.join(CONDA, "bin"))
os.symlink(relay_bundle.check_python() or sys.executable, os.path.join(CONDA, "bin", "python3"))
M.RELAY[NAME] = dict(M.RELAY[NAME], root=RROOT, conda=(CONDA, "pt"), transfer_job="-p test")
Q = "box"  # 总队列所在的机器（taskq.queue_host()，10/31）
assert relay.taskq.queue_host() == Q


def test_pueue_worker():
    w = relay.conf("lab1")["worker"]
    lab = f"worker:{w['id']}"
    ready = dict(tasks=[dict(key="a/x/0", state="free"), dict(key="a/x/1", state="claimed")])
    none = dict(tasks=[dict(key="a/x/1", state="claimed")])
    assert relay.topup("lab1", dict(jobs=None, taskq=ready)) == ([], "读不到 pueue")
    assert relay.topup("lab1", dict(jobs=[], taskq=ready)) == ([("pueue", "gpu", 1)], "那边没有备好、没人领的 CPU 任务")
    assert relay.topup("lab1", dict(jobs=[[3, lab, "Running", None]], taskq=ready)) == ([], "worker 在；那边没有备好、没人领的 CPU 任务")
    assert relay.topup("lab1", dict(jobs=[[3, lab, "Done", "Killed"], [4, "other", "Running", None]], taskq=ready)) == ([("pueue", "gpu", 1)], "那边没有备好、没人领的 CPU 任务")
    subs, why = relay.topup("lab1", dict(jobs=[[3, lab, "Done", "{'Failed': 1}"]], taskq=ready))
    assert not subs and why.startswith("上一个 worker")
    ok("pueue 类（lab1）：组里没有活着的 worker 才开，上一个失败了不重开")
    assert relay.topup("lab1", dict(jobs=[], taskq=none)) == ([], "那边没有备好、没人领的任务；那边没有备好、没人领的 CPU 任务")
    assert relay.topup("lab1", dict(jobs=[[3, lab, "Done", "Success"]], taskq=none)) == ([], "那边没有备好、没人领的任务；那边没有备好、没人领的 CPU 任务")
    assert relay.topup("lab1", dict(jobs=[[3, lab, "Done", "Success"]], taskq=ready)) == ([("pueue", "gpu", 1)], "那边没有备好、没人领的 CPU 任务")
    ok("pueue 类：没有备好的任务不开 worker（空闲退出的那个不算失败），有了再开")

    cw = relay.conf("lab1")["cpu_worker"]
    clab = f"worker:{cw['id']}"
    cpu = dict(tasks=[dict(key="e/x/0", state="free", spec=dict(device="cpu")), dict(key="a/x/1", state="claimed")])
    both = dict(tasks=cpu["tasks"] + ready["tasks"][:1])
    assert relay.topup("lab1", dict(jobs=[], taskq=cpu)) == ([("pueue", "cpu", 1)], "那边没有备好、没人领的任务")
    assert relay.topup("lab1", dict(jobs=[], taskq=both)) == ([("pueue", "gpu", 1), ("pueue", "cpu", 1)], "")
    assert relay.topup("lab1", dict(jobs=[[3, lab, "Running", None]], taskq=both)) == ([("pueue", "cpu", 1)], "worker 在")
    assert relay.topup("lab1", dict(jobs=[[5, clab, "Running", None]], taskq=both)) == ([("pueue", "gpu", 1)], "CPU worker 在")
    subs, why = relay.topup("lab1", dict(jobs=[[5, clab, "Done", "{'Failed': 1}"]], taskq=both))
    assert subs == [("pueue", "gpu", 1)] and why == "上一个 CPU worker（pueue 任务 5）失败了（{'Failed': 1}）", (subs, why)
    allowed0, relay.shift.allowed = relay.shift.allowed, lambda n, now=None: True  # 不看 box 上真实的时段记录（~/.cache/phd/shift.json）
    try:
        assert relay.topup("lab0", dict(jobs=[], taskq=cpu)) == ([], "那边没有备好、没人领的任务")
    finally:
        relay.shift.allowed = allowed0
    ok("pueue 类：GPU、CPU 两种 worker 各自判断（在不在、上一个失败没有、有没有这种任务）；没有 cpu_worker 的（lab0）不开 CPU worker")


def test_slurm_topup_ready():
    two = dict(enabled=True, tasks=[dict(key="a/x/0", state="free"), dict(key="a/x/1", state="free"), dict(key="a/x/2", state="claimed")])
    probe = lambda out: (lambda name, cmd: (0, out, ""))  # noqa: E731
    q = next(iter(relay.P.SLURM[NAME]["gpu"]))
    keep = relay.P.SLURM[NAME]["gpu"][q][1]
    subs, _ = relay.topup(NAME, dict(taskq=two), run=probe(f"QOS|{q}|10|0|0\n"))
    assert [n for _, _, n, _ in subs] == [10 - keep] and subs[0][3] == "worker.sbatch", subs
    subs, why = relay.topup(NAME, dict(taskq=two), run=probe(f"QOS|{q}|10|{10 - keep}|2\n"))
    assert subs == [] and "排满" in why
    subs, why = relay.topup(NAME, dict(taskq=dict(enabled=True, tasks=[])), run=probe(f"QOS|{q}|10|0|0\n"))
    assert subs == [] and "没有要卡的任务" in why
    ok("Slurm 类（DICC2）：有任务就按名额排满（攒排队时间，11/2），满了不交，没有任务不交")
    cpu = dict(enabled=True, tasks=[dict(key="e/x/0", state="free", spec=dict(device="cpu"))])
    subs, why = relay.topup(NAME, dict(taskq=cpu), run=probe(f"QOS|{q}|10|0|0\n"))
    assert subs == [] and "CPU" not in why, (subs, why)
    slurm0 = dict(relay.P.SLURM)
    try:
        relay.P.SLURM["dicc1"] = dict(gpu={"long": ("3-00:00:00", 0)}, cpu=("normal", "08:00:00", 2))
        subs, why = relay.topup("dicc1", dict(taskq=cpu), run=probe("QOS|long|10|0|0\nCPU|0\nALL|10|3\n"))
        assert subs == [("normal", "08:00:00", 1, "cpu_worker.sbatch")], (subs, why)
        subs, why = relay.topup("dicc1", dict(taskq=dict(two, tasks=two["tasks"] + cpu["tasks"])), run=probe("QOS|long|10|0|0\nCPU|1\nALL|10|3\n"))
        assert subs == [("long", "3-00:00:00", 10, "worker.sbatch")] and "CPU：已有 1 个" in why, (subs, why)
        relay.P.SLURM["dicc2"] = dict(slurm0["dicc2"], cpu=("normal", "08:00:00", 2))
        subs, why = relay.topup(NAME, dict(taskq=cpu), run=probe(f"QOS|{q}|10|0|0\nCPU|0\nALL|10|3\n"))
        assert subs == [] and "CPU" not in why, (subs, why)
    finally:
        relay.P.SLURM.clear()
        relay.P.SLURM.update(slurm0)
    ok("Slurm 类：jobs 里有 cpu 的（dicc1）有纯 CPU 任务时交 cpu_worker.sbatch；没有的（DICC2）SLURM 里写了 cpu 也不交")


def ok(msg):
    print("通过：" + msg)


# ---------------- 1. 决策 ----------------
def mtask(key, state="claimed", worker=NAME, nice=5, since=1000.0, relay_="toy"):
    return dict(key=key, state=state, worker=worker, nice=nice, order=1, since=since, note="",
                spec=dict(env="base", data=[f"prepared/{key.split('/')[0]}"], relay=relay_, device="gpu", machines=None),
                skipped_by=[])


def rtask(key, state, worker=None):
    return dict(key=key, state=state, worker=worker, nice=5, order=1, note="", failures=0, spec=dict(out=f"out/{key}"))


def record(k, **kw):
    return dict(dict(recipe="toy", params={}, data=[f"prepared/{k.split('/')[0]}"], views=[f"prepared/{k.split('/')[0]}"],
                     outs=[f"out/{k}"]), **kw)


def S0(mt, box=(), recs=()):
    return dict(taskq={Q: dict(enabled=True, tasks=mt)}, pueue={M.hub(): dict(tasks=list(box))}, records=list(recs),
                ok={M.hub(): True}, src={n: dict(tasks={}) for n in M.names("train")})


def keymap(*pairs):
    """(key, 那边的 key)：两个一样 = 11/1 起的真名记录；不一样 = 之前换名的老记录（alias）。"""
    return dict(keys={NAME: {k: record(k, **(dict(alias=a) if a != k else {})) for k, a in pairs}})


def ops(acts):
    return sorted((a["op"], a["key"]) for a in acts)


def test_adopt_out():
    """老数据目录、别的执行机里训到一半的存档（最新的那份）拷进结果目录；这边已有更新的存档或训完了时不动。"""
    import subprocess
    old, sib, R = os.path.join(T, "ao_old"), os.path.join(T, "ao_sib"), os.path.join(T, "ao_R")
    src, ssrc = os.path.join(old, "res", "frag_3"), os.path.join(sib, "data", "a", "frag_3")
    os.makedirs(src)
    for f, b in (("latest.ckpt", b"L"), ("best.ckpt", b"B"), ("train_log.txt", b"t")):
        open(os.path.join(src, f), "wb").write(b)
    dirs = [ssrc, src]
    run = lambda: S.json_in(subprocess.run(
        [sys.executable, "-c", relay.ADOPT_OUT_PY, R], input=json.dumps(dict(srcs=dirs, out="a/frag_3", resume=["latest.ckpt", "final.ckpt"],
                                                                         done=["check/summary.json", "final.ckpt|ARCHIVED"])),
        capture_output=True, text=True, cwd=_cfg.REPO, check=True).stdout)
    r = run()
    assert r["n"] == 3 and r["src"] == src, r
    new = os.path.join(R, "data", "a", "frag_3")
    assert open(os.path.join(new, "latest.ckpt"), "rb").read() == b"L"
    assert not [f for f in os.listdir(new) if f.endswith(".relaytmp")]
    open(os.path.join(new, "latest.ckpt"), "wb").write(b"N")
    assert run()["n"] == 0 and open(os.path.join(new, "latest.ckpt"), "rb").read() == b"N"
    ok("老目录的存档拷进结果目录；已有更新的存档时不覆盖")
    os.makedirs(ssrc)
    open(os.path.join(ssrc, "latest.ckpt"), "wb").write(b"S")
    t = time.time() + 60
    os.utime(os.path.join(ssrc, "latest.ckpt"), (t, t))
    r = run()
    assert r["src"] == ssrc and open(os.path.join(new, "latest.ckpt"), "rb").read() == b"S", r
    ok("另一台执行机（同一个盘、换卡）的存档更新 → 拷它的，接着训")
    open(os.path.join(new, "final.ckpt"), "wb").write(b"F")
    wb_ = os.path.join(new, "check", "summary.json")
    os.makedirs(os.path.dirname(wb_))
    open(wb_, "w").write("{}")
    os.utime(os.path.join(ssrc, "latest.ckpt"), (t + 60, t + 60))
    assert run()["n"] == 0 and open(os.path.join(new, "latest.ckpt"), "rb").read() == b"S"
    ok("这边已经训完（任务说明的 done 都在）→ 不动")


def test_plan():
    K = A = "D1_x/tr/0"
    st = keymap((K, A))
    RS = lambda *ts, **kw: dict(taskq=dict(tasks=list(ts), paused=kw.get("paused", False)), tasks=kw.get("tasks", {}), free=500e9)  # noqa: E731

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
    old_rtq = relay.rtq
    relay.rtq = lambda name, args, timeout=180: calls.append(args) or (0, "")
    try:
        msgs = relay.execute(NAME, [dict(op="rnice", key=K, rkey=A, nice=-101)], S0([mtask(K)]))
    finally:
        relay.rtq = old_rtq
    assert calls == [f"set-nice --nice -101 {A}"] and "档位 -101" in msgs[0], (calls, msgs)
    ok("总队列里改了档位 → 那边跟着改（set-nice）；那边训完的不改")

    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(), {})
    assert ops(acts) == [("beat", K), ("stage", K)], acts
    rec = dict(machine=M.hub(), id=7, label="relay_stage:" + K, step="relay_stage", key=K, added=2000.0)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=7, label=rec["label"], status="Running")], [rec]), NAME, RS(), {})
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=7, label=rec["label"], status="Failed")], [rec]), NAME, RS(), {})
    assert ops(acts) == [("beat", K)] and f"relaystage:{K}" in stuck, (acts, stuck)
    old = dict(rec, added=10.0)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=7, label=rec["label"], status="Failed")], [old]), NAME, RS(), {})
    assert ops(acts) == [("beat", K), ("stage", K)] and not stuck, (acts, stuck)
    ok("还没放过去 → 排传数据；在传 → 等；出错 → 报、领取保留；领取之前的旧任务不算")
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
    assert ops(acts) == [("beat", K)] and f"relayfetch:{K}" in stuck, stuck
    ok("那边训完 → 排一次拉回；在拉 → 等；出错 → 报")

    acts, _ = relay.plan(S0([mtask(K, "done")]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("drop", K), ("release", K)], acts
    fdone = [dict(id=8, label=frec["label"], status="Success")]
    acts, _ = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("drop", K), ("release", K)], acts
    acts, stuck = relay.plan(S0([mtask(K, "done")], [dict(fdone[0], status="Running")], [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, stuck = relay.plan(S0([mtask(K, "done")], [dict(fdone[0], status="Failed")], [frec]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
    ok("总队列显示训完（box 上有了）→ 交还、删那边的任务和结果；拉回、归档还在跑 → 等；出错 → 报、不删")

    import common as C
    old_lookup, old_down = relay.lookup, C.backup_down
    relay.lookup = lambda k: dict(archive=True)
    arec = dict(machine=M.hub(), id=9, label="relay_archive:" + K, step="relay_archive", key=K, added=2000.0)
    fcmd = dict(fdone[0], command="python3 scripts/taskq/queue/relay.py fetch x k && bash scripts/taskq/queue/archive_task.sh box k")
    plain_fail = [dict(fdone[0], status="Failed")]
    try:
        C.backup_down = lambda ok=None: "dicc1 暂停检查（停机）"
        acts, stuck = relay.plan(S0([mtask(K, "done")], plain_fail, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
        acts, stuck = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("drop", K), ("release", K)] and not stuck, (acts, stuck)
        assert "没有归档" in [a for a in acts if a["op"] == "release"][0]["why"], acts
        C.backup_down = lambda ok=None: "dicc1 这次连不上" if ok is not None else None
        acts, stuck = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K), ("drop", K)] and not stuck, (acts, stuck)
        C.backup_down = lambda ok=None: None
        acts, stuck = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("archive", K), ("beat", K), ("drop", K)] and not stuck, (acts, stuck)
        acts, _ = relay.plan(S0([mtask(K, "done")], [fcmd], [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("drop", K), ("release", K)], acts
        assert "归档了" in [a for a in acts if a["op"] == "release"][0]["why"], acts
        acts, stuck = relay.plan(S0([mtask(K, "done")], [dict(fcmd, status="Failed")], [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("archive", K), ("beat", K), ("drop", K)] and not stuck, (acts, stuck)
        two = [dict(fcmd, status="Failed"), dict(id=9, label=arec["label"], status="Running")]
        acts, stuck = relay.plan(S0([mtask(K, "done")], two, [frec, arec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K)] and not stuck, (acts, stuck)
        two[1]["status"] = "Success"
        acts, _ = relay.plan(S0([mtask(K, "done")], two, [frec, arec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("drop", K), ("release", K)], acts
        two[1]["status"] = "Failed"
        acts, stuck = relay.plan(S0([mtask(K, "done")], two, [frec, arec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K), ("drop", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
        acts, stuck = relay.plan(S0([mtask(K, "done")], [fdone[0], dict(id=9, label=arec["label"], status="Failed")], [frec, arec]), NAME,
                                 RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K), ("drop", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
    finally:
        relay.lookup, C.backup_down = old_lookup, old_down
    ok("要归档的折：box 上有了就删那边的；备份机能用时单独排归档（relay_archive），归档成功才交还，归档出错 → 报、领取保留；"
       "备份机停用时直接交还（没有归档）；只是这次连不上 → 等；单独的拉回出错 → 报")

    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "failed")), st)
    assert ops(acts) == [("drop", K), ("release", K)] and [a for a in acts if a["op"] == "release"][0].get("skip"), acts
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "failed"), tasks={A: dict(final=False, runs=[dict(epochs_done=3)])}), st)
    assert ops(acts) == [("beat", K)] and f"site:{NAME}:fail:{K}" in stuck, acts
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "bad")), st)
    assert ops(acts) == [("beat", K)] and f"site:{NAME}:fail:{K}" in stuck, stuck
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "claimed", "123"), paused=True), st)
    assert f"site:{NAME}:fail" in stuck, stuck
    ok("那边失败：没有进度 → 交还（--skip）并删掉；有进度、bad → 保留、报 site:<名字>:fail:<key>；那边暂停 → site:<名字>:fail")

    ri = dict(rtask(A, "input"), input_since=5000.0)
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(ri), st)
    assert ops(acts) == [("beat", K), ("restage", K)] and not stuck, acts
    srec = dict(machine=M.hub(), id=9, label="relay_stage:" + K, step="relay_stage", key=K, added=6000.0)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=9, label=srec["label"], status="Running")], [srec]), NAME, RS(ri), st)
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=9, label=srec["label"], status="Failed")], [srec]), NAME, RS(ri), st)
    assert ops(acts) == [("beat", K)] and f"relaystage:{K}" in stuck, (acts, stuck)
    acts, stuck = relay.plan(S0([mtask(K)], [dict(id=9, label=srec["label"], status="Failed")], [dict(srec, added=1500.0)]), NAME, RS(ri), st)
    assert ops(acts) == [("beat", K), ("restage", K)] and not stuck, (acts, stuck)
    added = []
    msgs = relay.execute(NAME, [dict(op="restage", key=K)], S0([mtask(K)]), run_add=lambda *a, **k: added.append(a) or 77)
    assert added and "--no-add" in added[0][4] and "--nice" not in added[0][4] and "补传" in msgs[0], (added, msgs)
    ok("那边缺输入 → 排补传（stage --no-add：只补缺的、撤掉标记）；在补 → 等；补传出错 → 报；标记之前的旧任务不算")

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
    acts, stuck = relay.plan(S0([]), NAME, RS(rtask("D999_zz/tr/0", "free")), st)
    assert ops(acts) == [] and list(stuck) == [f"site:{NAME}:orphan:D999_zz/tr/0"], stuck
    acts, stuck = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(rtask(A, "claimed", "123")), st)
    assert ops(acts) == [] and list(stuck) == [f"site:{NAME}:orphan:{K}"], stuck
    ok("那边有、总队列里不归它的：不让它训；总队列训完了就删；认不出的、在训的报 site:<名字>:orphan")

    acts, stuck = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(rtask(A, "free")), st)
    assert ops(acts) == [("unq", K)] and acts[0]["to"] == "other" and not stuck, acts
    acts, stuck = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(rtask(A, "stopped")), st)
    assert ops(acts) == [("unq", K)] and not stuck, acts
    ust = dict(keys={NAME: {K: dict(st["keys"][NAME][K], recipe="toy", params=dict(ds="D901_ab", frag="0"), unq=1.0)}})
    acts, _ = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(), ust)
    assert ops(acts) == [], acts
    acts, _ = relay.plan(S0([mtask(K, "done", "other")]), NAME, RS(), ust)
    assert ops(acts) == [("drop", K)] and acts[0].get("gone") and acts[0]["rkey"] == A, acts
    acts, _ = relay.plan(S0([]), NAME, RS(), ust)
    assert ops(acts) == [("drop", K)] and acts[0].get("gone"), acts
    ok("总队列里改分给了别的执行机（换卡；那边停着的也算）→ 那边队列里删掉（unq）、结果留着给它收编；总队列训完了再删结果（不再删队列）")

    old = keymap((K, "D901_ab/tr/0"))
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask("D901_ab/tr/0", "claimed", "123")), old)
    assert ops(acts) == [("beat", K)] and not stuck, (acts, stuck)
    acts, _ = relay.plan(S0([mtask(K, "done")]), NAME, RS(rtask("D901_ab/tr/0", "done")), old)
    assert ops(acts) == [("drop", K), ("release", K)] and [a for a in acts if a["op"] == "drop"][0]["rkey"] == "D901_ab/tr/0", acts
    ok("11/1 前换名的老记录：那边的 key 是别名，照样续、删")

    red = dict(keys={NAME: {K: record(K, alias="D901_ab/tr/0", redo=1.0)}})
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask("D901_ab/tr/0", "free")), red)
    assert ops(acts) == [("beat", K), ("redo", K)] and not acts[1]["busy"] and acts[1]["rkey"] == "D901_ab/tr/0", acts
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask("D901_ab/tr/0", "claimed", "123")), red)
    assert ops(acts) == [("beat", K), ("redo", K), ("rstop", K)] and acts[1]["busy"], acts
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(), red)
    assert ops(acts) == [("beat", K), ("redo", K)], acts
    acts, _ = relay.plan(S0([mtask(K, "free", None)]), NAME, RS(rtask("D901_ab/tr/0", "free")), red)
    assert ops(acts) == [("drop", K)] and not acts[0]["gone"], acts
    acts, _ = relay.plan(S0([]), NAME, RS(), red)
    assert ops(acts) == [("drop", K)] and acts[0]["gone"], acts
    acts, stuck = relay.plan(S0([mtask(K, "free", None)]), NAME, RS(rtask("D901_ab/tr/0", "claimed", "1")), red)
    assert ops(acts) == [("rstop", K)], acts
    ok("redo：领着的 → 那边没人领着时删掉、下次重新传，在训的先停；不归它的 → 删掉（那边在训的先停）")

    acts, stuck = relay.plan(S0([mtask(K)]), NAME, None, dict(st, last_ok={NAME: time.time() - 3 * 3600}))
    assert ops(acts) == [("beat", K)] and f"site:{NAME}:down" in stuck, stuck
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, None, dict(st, last_ok={NAME: time.time() - 600}))
    assert ops(acts) == [("beat", K)] and not stuck
    ok("连不上：只续总队列的领取，2 小时以上报")

    K2, K3 = "D2_y/tr/0", "D3_z/tr/0"
    cands = [mtask(K2, "free", None, nice=3), mtask(K3, "free", None, nice=4), mtask("D4_c/tr/0", "free", None, relay_=None)]
    relay.refill.data_gb = lambda paths: 10.0
    relay.P.READY[NAME] = 2
    HARD0 = (relay.conf(NAME).get("budget") or (None, relay.HARD_GB))[1]
    more, why = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "claimed", "1")), st, [])
    assert [a["key"] for a in more] == [K2, K3], (more, why)
    berr = S0([mtask(K)] + cands)
    berr["pueue"] = {M.hub(): {"error": "pueue 没有响应"}}
    more, why = relay.fill(berr, NAME, RS(rtask(A, "claimed", "1")), st, [])
    assert more == [] and "pueue" in why, (more, why)
    ok("box 的 pueue 读不到：不领新任务、不换位（传数据要在 box 的 pueue 排）")
    more, _ = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "free")), st, [])
    assert [a["key"] for a in more] == [K2], more
    more, why = relay.fill(S0([mtask(K), mtask("D5_d/tr/0")] + [dict(t, nice=t["nice"] + 3) for t in cands]), NAME, RS(rtask(A, "free")), st, [])
    assert more == [] and "排着 2" in why, (more, why)
    more, why = relay.fill(S0([mtask(K)] + cands), NAME, dict(RS(rtask(A, "free")), free=(HARD0 + 5) * 1e9), st, [])
    assert more == [] and "放不下" in why, why
    ok("补任务：那边排着的（含在传数据的）不够 READY 个才领，按档位，只领能翻译的做法，放不下不领")
    K0 = "D6_e/tr/0"
    more, why = relay.fill(S0([mtask(K), mtask("D5_d/tr/0"), mtask(K0, "free", None, nice=-100)] + [dict(t, nice=t["nice"] + 3) for t in cands]), NAME,
                           RS(rtask(A, "free")), st, [])
    assert [a["key"] for a in more] == [K0], (more, why)
    more, why = relay.fill(S0([mtask(K), mtask(K0, "free", None, nice=-100)] + cands), NAME, RS(rtask(A, "free")), st, [])
    assert [a["key"] for a in more] == [K0, K2], (more, why)
    ok("第 ⓪ 档（nice < 0，训完后的推理）不受 READY 限制：排满了也领，先领它")

    K5 = "D5_d/tr/0"
    st2 = keymap((K, A), (K5, K5))
    late = [mtask(K2, "free", None, nice=6), mtask(K3, "free", None, nice=7)]
    more, why = relay.fill(S0([mtask(K), mtask(K5, nice=6)] + late), NAME, RS(rtask(A, "stopped"), rtask(K5, "free")), st2, [])
    assert more == [] and "排着 2" in why, (more, why)
    ok("那边停着、这次要撤销停止的也算排着（11/2：lab1 多领了 2 个）")
    more, why = relay.fill(S0([mtask(K), mtask(K5, nice=6)] + cands), NAME, RS(rtask(A, "free"), rtask(K5, "free")), st2, [])
    assert [(a["op"], a["key"]) for a in more] == [("drop", K5), ("release", K5), ("take", K2)] and more[0]["rkey"] == K5, more
    more, why = relay.fill(S0([mtask(K), mtask(K5, nice=6)] + cands), NAME, RS(rtask(A, "claimed", "1"), rtask(K5, "free")), st2, [])
    assert [(a["op"], a["key"]) for a in more] == [("take", K2)], more
    relay.P.READY[NAME] = 1
    more, why = relay.fill(S0([mtask(K), mtask(K5, nice=6)] + cands), NAME, RS(rtask(A, "claimed", "1"), rtask(K5, "free")), st2, [])
    assert [(a["op"], a["key"]) for a in more] == [("drop", K5), ("release", K5), ("take", K2)], more
    relay.P.READY[NAME] = 2
    more, why = relay.fill(S0([mtask(K), mtask(K5, nice=6)] + late), NAME, RS(rtask(A, "free"), rtask(K5, "free")), st2, [])
    assert more == [] and "排着 2" in why, (more, why)
    more, why = relay.fill(S0([mtask(K), mtask(K5, nice=6)] + [mtask(K2, "free", None, nice=6)]), NAME, RS(rtask(A, "free"), rtask(K5, "free")), st2, [])
    assert more == [], more
    more, why = relay.fill(S0([mtask(K), mtask(K5, nice=6)] + cands), NAME, dict(RS(rtask(A, "free"), rtask(K5, "free")), free=(HARD0 + 5) * 1e9), st2, [])
    assert more == [], more
    more, why = relay.fill(S0([mtask(K), mtask(K5, nice=6)] + cands), NAME, RS(rtask(A, "waiting"), rtask(K5, "free", "9")), st2, [])
    assert more == [], more
    relay.PLANNED.clear()
    ok("备满了、总队列里有更优先的：交还一个备好没人领的（先删那边、再交还）换它进来；在训的、有人领的、同档的不换，放不下不换")

    def ctask(key, state="free", worker=None, nice=3):
        t = mtask(key, state, worker, nice=nice)
        return dict(t, spec=dict(t["spec"], device="cpu"))
    hi = [dict(t, nice=t["nice"] + 3) for t in cands]
    E1, E2 = "D7_f/ev/0", "D8_g/ev/0"
    jobs0 = M.RELAY[NAME]["jobs"]
    relay.P.READY[NAME] = 1
    relay.PLANNED.clear()
    try:
        more, why = relay.fill(S0([mtask(K)] + hi + [ctask(E1), ctask(E2, nice=4)]), NAME, RS(rtask(A, "free")), st, [])
        assert more == [] and "排着 1" in why, (more, why)
        M.RELAY[NAME]["jobs"] = tuple(jobs0) + ("cpu",)
        more, why = relay.fill(S0([mtask(K)] + hi + [ctask(E1), ctask(E2, nice=4)]), NAME, RS(rtask(A, "free")), st, [])
        assert [a["key"] for a in more] == [E1], (more, why)
        more, why = relay.fill(S0([mtask(K), ctask(E1, "claimed", NAME)] + hi + [ctask(E2, nice=4)]), NAME, RS(rtask(A, "free")), st, [])
        assert more == [] and "排着 2" in why, (more, why)
        more, why = relay.fill(S0([ctask(E1, "claimed", NAME)] + hi + [ctask(E2, nice=4)]), NAME, RS(), st, [])
        assert [a["key"] for a in more] == [K2], (more, why)
    finally:
        M.RELAY[NAME]["jobs"] = jobs0
        relay.P.READY[NAME] = 2
        relay.PLANNED.clear()
    ok("能跑 CPU 任务的执行机（jobs 里有 cpu / 有 cpu_worker）：GPU、CPU 各备 READY 个，互不占名额；不能跑的不领 CPU 任务")

    relay.PLANNED.clear()
    a1, _ = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "free")), st, [])
    disk = next(iter(relay.PLANNED))
    relay.PLANNED[disk] = 500.0 - HARD0 - 5
    more, why = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "free")), st, [])
    assert a1 and more == [] and "放不下" in why, (a1, more, why)
    relay.PLANNED.clear()
    ok("同一个盘上的几台（lab0、lab1）：这次检查里已经计划传的算进去")

    relay.PLANNED.clear()
    OUT = relay.refill.TRAIN_OUT_GB
    cap0 = relay.conf(NAME).get("cap")
    relay.conf(NAME)["cap"] = 90
    try:
        three = RS(rtask(A, "claimed", "1"), rtask("Dataset9_h/tr/0", "done"), rtask("Dataset9_h/tr/1", "claimed", "2"))
        sep = dict(three, free=(3 * OUT + 5) * 1e9, out_used=12e9, dev=1, blob_dev=2, blob_free=500e9)
        more, why = relay.fill(S0([mtask(K)] + cands), NAME, sep, st, [])
        assert [a["key"] for a in more] == [K2], (more, why)  # 剩 3×OUT+5+12，3 个任务各留 OUT 后够 1 个
        more, why = relay.fill(S0([mtask(K)] + cands), NAME, dict(sep, out_used=0), st, [])
        assert more == [] and "各留" in why, (more, why)
        relay.PLANNED.clear()
        more, why = relay.fill(S0([mtask(K)] + cands), NAME, dict(sep, blob_free=(HARD0 + 5) * 1e9), st, [])
        assert more == [] and "放不下" in why and "各留" not in why, (more, why)  # 输入按放 blobs 的盘查
        relay.PLANNED.clear()
        same = dict(three, free=(3 * OUT + 25) * 1e9, out_used=0, dev=1, blob_dev=1, blob_free=500e9)
        more, why = relay.fill(S0([mtask(K)] + cands), NAME, same, st, [])
        assert [a["key"] for a in more] == [K2], (more, why)  # 同一个盘：输入 10 GB 也从 cap 里扣
        relay.PLANNED.clear()
        more, why = relay.fill(S0([mtask(K)] + cands), NAME, dict(same, free=(3 * OUT + 15) * 1e9), st, [])
        assert more == [], (more, why)
    finally:
        relay.conf(NAME)["cap"] = cap0
        relay.PLANNED.clear()
    ok("有 cap 的执行机：那边每个任务（含训完没拉回的、这次领的）的输出各留 TRAIN_OUT_GB；输入按放 blobs 的盘查（11/4）")

    rule0, allowed0 = relay.shift.rule, relay.shift.allowed
    relay.shift.allowed = lambda n, now=None: False
    try:
        more, why = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "free")), st, [])
        assert more == [] and why, (more, why)
        subs, why = relay.topup(NAME, dict(taskq=dict(enabled=True, tasks=[dict(key="a/x/0", state="free")])), run=lambda n, c: (0, "", ""))
        assert subs == [] and why, (subs, why)
    finally:
        relay.shift.rule, relay.shift.allowed = rule0, allowed0
    ok("按时段的执行机（lab0）时段外：不领任务、不开 worker")


# ---------------- 2. 挪进数据目录的顺序 ----------------
def test_place():
    src, D = os.path.join(T, "place_src"), os.path.join(T, "place_dst")
    real = dict(out="results/X/tr/frag_0", done=["check/summary.json", "final.ckpt|ARCHIVED"],
                fetch=dict(extra=["*.json"]))
    written = ["results/X/tr/frag_0/check/a.dat", "results/X/tr/plans.json",
               "logs/tasks/k__1_seg1.log", "results/X/tr/frag_0/final.ckpt",
               "results/X/tr/frag_0/check/summary.json"]
    for w in written:
        TN.wb(os.path.join(src, w), b"new")
    TN.wb(os.path.join(D, "results/X/tr/plans.json"), b"old")
    seen, real_replace = [], os.replace

    def spy(a, b):
        seen.append(os.path.relpath(b, D))
        if len(seen) == 3:  # 普通文件 2 个之后的第一个标志
            raise OSError("盘满了")
        real_replace(a, b)
    relay.os.replace = spy
    try:
        relay.place(src, D, written, real)
        raise AssertionError("应出错")
    except OSError:
        pass
    finally:
        relay.os.replace = real_replace
    assert not os.path.exists(os.path.join(D, real["out"], "check/summary.json"))
    assert not os.path.exists(os.path.join(D, real["out"], "final.ckpt"))
    relay.place(src, D, [w for w in written if os.path.exists(os.path.join(src, w))], real)
    assert open(os.path.join(D, "results/X/tr/plans.json"), "rb").read() == b"old"
    assert seen[:2] == ["results/X/tr/frag_0/check/a.dat", "logs/tasks/k__1_seg1.log"], seen
    assert os.path.exists(os.path.join(D, real["out"], "check/summary.json"))
    ok("拉回：训完的标志最后挪（中途出错时不算训完），上一级已有的 JSON 不覆盖")

    src, D = os.path.join(T, "place2_src"), os.path.join(T, "place2_dst")
    real = dict(out="preds/m/frag_0", done=[".infer_done"], fetch=dict(extra=[]))
    written = ["preds/m/frag_0/.infer_done", "preds/m/frag_0/MANIFEST.json", "preds/m/frag_0/c1.dat",
               "preds/m/note.txt"]
    for w in written:
        TN.wb(os.path.join(src, w), b"n")
    seen = []

    def spy2(a, b):
        seen.append(os.path.relpath(b, D))
        real_replace(a, b)
    relay.os.replace = spy2
    try:
        relay.place(src, D, written, real)
    finally:
        relay.os.replace = real_replace
    assert seen == ["preds/m/frag_0/c1.dat", "preds/m/note.txt", "preds/m/frag_0/MANIFEST.json",
                    "preds/m/frag_0/.infer_done"], seen
    ok("拉回：MANIFEST.json 在训完的标志前、普通文件后挪")

    # 变异审查（11/3）：done 有几项时倒着放，第一项（只看第一项的读者认的标志）最后放
    src, D = os.path.join(T, "place3_src"), os.path.join(T, "place3_dst")
    real = dict(out="o/f0", done=["first.flag", "second.flag|alt.flag", "third.flag"], fetch=dict(extra=[]))
    written = ["o/f0/first.flag", "o/f0/second.flag", "o/f0/third.flag", "o/f0/x.dat"]
    for w in written:
        TN.wb(os.path.join(src, w), b"n")
    seen = []
    relay.os.replace = spy2
    try:
        relay.place(src, D, written, real)
    finally:
        relay.os.replace = real_replace
    assert seen == ["o/f0/x.dat", "o/f0/third.flag", "o/f0/second.flag", "o/f0/first.flag"], seen
    ok("拉回：done 的几项倒着放，第一项最后")


# ---------------- 3. 真跑 ----------------
def test_e2e():
    TN.setup()
    sp = S.resolve("toy", dict(ds=TN.DS, frag="0"))
    relay.lookup = lambda k: sp
    relay.box_data = lambda: TN.ROOT
    bid, path = relay_bundle.build(NAME, None, os.path.join(T, "bundles"), root=RROOT)
    relay.deploy(NAME, bid, path)
    rs = relay.remote_status(NAME)
    assert rs and rs["bundle"] == bid and rs["taskq"]["enabled"] and rs["taskq"]["tasks"] == [], rs
    ok(f"代码包 {bid} 推过去、切换 current，那边的队列开着、是空的")

    c0 = M.RELAY[NAME]
    M.RELAY[NAME] = dict(c0, ssh_master=True)
    open(os.path.join(T, "master_down"), "w").close()
    assert relay.remote_status(NAME) is None
    os.remove(os.path.join(T, "master_down"))
    assert relay.remote_status(NAME)["bundle"] == bid
    M.RELAY[NAME] = c0
    ok("ssh_master：长连接断着 → 当连不上，不另起连接；通着照常读")

    if os.path.exists(os.path.join(T, "srun_args")):
        os.remove(os.path.join(T, "srun_args"))
    assert relay.rpy(NAME, relay.MISSING_PY, ["ab" * 32], job="-p cpu -t 10") == ["ab" * 32]
    sa = open(os.path.join(T, "srun_args")).read()
    assert f"-I{relay.JOB_WAIT} -p cpu -t 10 -J " in sa and " bash -c " in sa, sa
    open(os.path.join(T, "no_slot"), "w").close()
    try:
        relay.rpy(NAME, relay.MISSING_PY, [], job="-p cpu")
        raise AssertionError("应出 NoSlot")
    except relay.NoSlot:
        pass
    finally:
        os.remove(os.path.join(T, "no_slot"))
    ok("adopt_job：收编在 srun 的 CPU 作业里跑（-I 等排队）；排不上 → NoSlot（这次不传）")

    rkey_ = relay.stage(NAME, sp["key"], nice=5, say=lambda m: None)
    assert rkey_ == sp["key"]
    pre = os.path.join(RROOT, "data", "prepared", TN.DS)
    links = [os.path.join(dp, f) for dp, _, fs in os.walk(pre) for f in fs]
    assert links and all(os.path.isfile(p) for p in links), links
    assert not os.path.exists(os.path.join(pre, ".incomplete"))
    gt = sorted(os.listdir(os.path.join(pre, "gt")))
    assert gt == sorted(c + ".dat" for c in TN.CASES), gt
    assert open(os.path.join(pre, "gt", TN.CASES[0] + ".dat"), "rb").read() == b"gt" + TN.CASES[0].encode()
    blobs = sum(len(fs) for _, _, fs in os.walk(os.path.join(RROOT, "blobs")))
    rs = relay.remote_status(NAME)
    assert [(t["key"], t["state"]) for t in rs["taskq"]["tasks"]] == [(sp["key"], "free")], rs["taskq"]["tasks"]
    k = relay.keys_of(relay.load_state(), NAME)[sp["key"]]
    assert k["views"] == [f"prepared/{TN.DS}"] and k["outs"] == [sp["out"]] and k["params"] == sp["params"], k
    ok(f"传数据：{blobs} 个文件按内容存、软链接放在和 box 一样的路径，key 和参数原样，加进那边的队列")

    rkey2 = relay.stage(NAME, sp["key"], nice=5, say=lambda m: None)
    assert rkey2 == sp["key"] and sum(len(fs) for _, _, fs in os.walk(os.path.join(RROOT, "blobs"))) == blobs
    ok("再传一次：不重传、不重复加")

    rout = os.path.join(RROOT, "data", sp["out"])
    for c in TN.CASES[:2]:
        TN.wb(os.path.join(rout, "check", c + ".dat"), b"p")
    TN.wj(os.path.join(rout, "check", "summary.json"), dict(metric_per_case=[{}, {}]))
    TN.wb(os.path.join(rout, "final.ckpt"), b"ck")
    TN.wb(os.path.join(rout, "latest.ckpt"), b"latest")
    TN.wb(os.path.join(rout, "debug.json"), b"{}")
    TN.wj(os.path.join(os.path.dirname(rout), "plans.json"), {})
    TN.wb(os.path.join(RROOT, "data", "logs", "tasks", S.safe(sp["key"]) + "__12345_seg1.log"), b"log")
    out = os.path.join(TN.ROOT, sp["out"])
    inbox = os.path.join(TN.ROOT, ".relay_in", S.safe(sp["key"]))
    os.environ["TUNNEL_TEST_BADMD5"] = sp["out"] + "/final.ckpt"
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            relay.fetch(NAME, sp["key"], say=lambda m: None)
        raise AssertionError("有文件没拉到应报错")
    except relay.RN.RelayError as e:
        assert "1 个文件没拉到" in str(e), e
    finally:
        del os.environ["TUNNEL_TEST_BADMD5"]
    assert not os.path.exists(out) and os.path.isfile(os.path.join(inbox, sp["out"], "check", "summary.json"))
    sj = os.path.join(inbox, sp["out"], "check", "summary.json")
    TN.wb(sj, b"x" * os.path.getsize(sj))  # 大小一样、修改时间不同
    TN.wb(os.path.join(inbox, sp["out"], "check", "old.dat"), b"old")
    TN.wb(os.path.join(inbox, sp["out"], "debug.json.tunnelpart"), b"{")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        relay.fetch(NAME, sp["key"], say=lambda m: None)
    assert "拉 2 个文件" in buf.getvalue(), buf.getvalue()
    assert not os.path.exists(os.path.join(out, "check", "old.dat")) and not os.path.exists(os.path.join(out, "debug.json.tunnelpart"))
    assert json.load(open(os.path.join(out, "check", "summary.json"))) == dict(metric_per_case=[{}, {}])
    ok("拉回到一半（上次缺文件报错，box 上什么都没放）：再拉只拉缺的和大小、修改时间对不上的，旧的、半份的不混进结果")
    assert os.path.exists(os.path.join(out, "check", "summary.json"))
    assert sorted(f for f in os.listdir(os.path.join(out, "check")) if f.endswith(".dat")) == sorted(c + ".dat" for c in TN.CASES[:2])
    assert not os.path.exists(os.path.join(out, "latest.ckpt"))
    assert os.path.exists(os.path.join(out, "final.ckpt"))
    assert os.path.exists(os.path.join(os.path.dirname(out), "plans.json"))
    assert not os.listdir(os.path.join(TN.ROOT, ".relay_in"))
    logs = os.listdir(os.path.join(TN.ROOT, "logs", "tasks"))
    assert logs == [S.safe(sp["key"]) + "__12345_seg1.log"], logs
    ok("拉回：同样的相对路径、不拉 latest.ckpt、上一级的 json 和分段日志、临时目录删掉")

    relay.write_status(NAME, relay.remote_status(NAME), relay.load_state())
    st = json.load(open(relay.STATUS))[NAME]
    assert sp["key"] in st["tasks"] and st["tasks"][sp["key"]]["final"], st
    ok("status.json 里有这一折（真 key）")
    return pre


def test_e2e_path(pre):
    """通用换名的任务：和上面训练的视图同时在那边，传数据只动自己的视图；拉回一半不算做完。"""
    before = sorted((os.path.relpath(os.path.join(dp, f), pre), os.readlink(os.path.join(dp, f)) if os.path.islink(os.path.join(dp, f)) else None)
                    for dp, _, fs in os.walk(pre) for f in fs)
    sp = TN.path_setup()
    relay.lookup = lambda k: sp
    rk = relay.stage(NAME, sp["key"], add=False, say=lambda m: None)
    k = relay.keys_of(relay.load_state(), NAME)[sp["key"]]
    D = os.path.join(RROOT, "data")
    assert rk == sp["key"] and k["views"] == ["proj/inputs/cohortA"], k
    assert set(k["outs"]) == {sp["out"], "proj/analysis/thing/shared"}, k
    assert "proj/lists/cases.txt" in k["inputs"] and "raw/Set/c1/c1-seg.dat" in k["inputs"] and not any("t1" in x for x in k["inputs"]), k
    assert open(os.path.join(D, "proj/inputs/cohortA", "y_0000.dat"), "rb").read() == b"yy"
    assert open(os.path.join(D, "proj/inputs/cohortA", "a", "x_0000.dat"), "rb").read() == b"xa"
    assert open(os.path.join(D, "raw/Set/c2/c2-seg.dat"), "rb").read() == b"sc2"
    assert not os.path.exists(os.path.join(D, "proj/inputs/cohortA", ".incomplete"))
    after = sorted((os.path.relpath(os.path.join(dp, f), pre), os.readlink(os.path.join(dp, f)) if os.path.islink(os.path.join(dp, f)) else None)
                   for dp, _, fs in os.walk(pre) for f in fs)
    assert after == before, "训练的视图被动了"
    TN.wb(os.path.join(D, "proj/inputs/cohortA", "stray.txt"), b"?")
    blob = next(os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(RROOT, "blobs")) for f in fs)
    os.symlink(blob, os.path.join(D, "proj/inputs/cohortA", "stale"))
    relay.stage(NAME, sp["key"], add=False, say=lambda m: None)
    assert not os.path.lexists(os.path.join(D, "proj/inputs/cohortA", "stale"))
    assert os.path.exists(os.path.join(D, "proj/inputs/cohortA", "stray.txt"))  # 不是指向 blobs 的链接：不碰
    os.remove(os.path.join(D, "proj/inputs/cohortA", "stray.txt"))
    ok("通用插件传数据：文件夹是视图、文件和通配符展开的文件放在同样的路径；视图里只删多出来的指向 blobs 的链接；训练的视图不动")

    TN.wb(os.path.join(D, sp["out"], "a.csv"), b"1")
    TN.wb(os.path.join(D, os.path.dirname(sp["out"]), "plans.json"), b"{}")
    TN.wb(os.path.join(D, "logs", "tasks", S.safe(sp["key"]) + "__9_seg1.log"), b"log")
    try:
        relay.fetch(NAME, sp["key"], say=lambda m: None)
        raise AssertionError("少 .done 应报没做完")
    except relay.RN.RelayError as e:
        assert "还没" in str(e), e
    assert not os.path.exists(os.path.join(TN.ROOT, sp["out"]))
    TN.wb(os.path.join(D, sp["out"], ".done"), b"")
    relay.fetch(NAME, sp["key"], say=lambda m: None)
    out = os.path.join(TN.ROOT, sp["out"])
    assert sorted(os.listdir(out)) == [".done", "a.csv"], os.listdir(out)
    assert os.path.exists(os.path.join(TN.ROOT, "proj/analysis/thing/plans.json"))
    assert os.path.exists(os.path.join(TN.ROOT, "logs", "tasks", S.safe(sp["key"]) + "__9_seg1.log"))
    ok("通用插件拉回：那边没做完不拉；做完后结果、上一级的文件、分段日志放进 box")

    keys = relay.keys_of(relay.load_state(), NAME)
    tk = next(r for r in keys if r != sp["key"])
    assert relay.droppable(keys, sp["key"], relay.paths_of(keys[sp["key"]])) == sorted(relay.paths_of(keys[sp["key"]]))
    relay.drop_remote(NAME, sp["key"], sp["key"], gone=True)
    assert not os.path.lexists(os.path.join(D, "raw/Set/c1/c1-seg.dat")) and not os.path.exists(os.path.join(D, "proj/inputs"))
    assert not os.path.exists(os.path.join(D, sp["out"])) and os.listdir(pre) and sp["key"] not in relay.keys_of(relay.load_state(), NAME)
    assert not [f for f in os.listdir(os.path.join(D, "logs", "tasks")) if f.startswith(S.safe(sp["key"]))]
    ok(f"删任务：它独占的输入、产出、日志删掉，空了的上级目录删掉；别的任务（{tk}）的视图不动")

    for bad in ("/etc", "a/../../x", "in"):
        try:
            relay.safe_rel(bad)
            raise AssertionError(bad)
        except relay.RN.RelayError:
            pass
    assert relay.views_of(dict(view="prepared/D1")) == ["prepared/D1"]
    ok("要删的、要建的路径在数据目录里、至少两层；旧记录的 view 也认")


def test_tilde_root():
    """根目录写成 ~/…（dicc1 的 ~/st/…）：开头的 ~ 留给那边展开，其余加引号（10/31）。"""
    c0 = M.RELAY[NAME]
    M.RELAY[NAME] = dict(c0, root="~/st/t123")
    try:
        assert relay.prefix(NAME).startswith("cd ~/st/t123/code/current && TASKQ_DIR=~/st/t123/queue "), relay.prefix(NAME)
        seen = []
        relay.rsh, rsh0 = (lambda name, cmd, input=None, timeout=300: (seen.append(cmd), (0, "{}", ""))[1]), relay.rsh
        try:
            relay.rpy(NAME, "print(1)")
        except RuntimeError:
            pass
        relay.rsh = rsh0
        assert seen and seen[0].endswith(" ~/st/t123"), seen
    finally:
        M.RELAY[NAME] = c0
    files = relay_bundle.contents(NAME, commit="HEAD", root="~/st/t123")
    sb = next(t for p, (t, _) in files.items() if p.endswith("worker.sbatch"))
    assert "#SBATCH --output=../../../../../data/logs/workers/%x_%j.out" in sb and "\nR=~/st/t123\n" in sb, sb
    assert "worker_start.sh" in sb  # 显存填充那条路（worker.sbatch → worker_start.sh）不绕开
    txt = "\n".join(t for t, _ in files.values())
    assert "~/st/t123/queue" in txt or "$HOME/st/t123/queue" in txt
    sh = "\n".join(t for p, (t, _) in files.items() if p.endswith((".sh", ".sbatch")))
    assert '"~/st/t123' not in sh, "shell 双引号里的 ~ 不展开（Python 的 expanduser 里可以）"
    assert relay_bundle.home_paths('a="$HOME/.cache/phd/taskq" b=~/.cache/phd/x', "~/st/t1") == 'a="$HOME/st/t1/queue" b=~/st/t1/state/x'
    assert relay_bundle.home_paths('a="$HOME/.cache/phd/taskq"', "/scr/u/r") == 'a="/scr/u/r/queue"'
    ok("根目录 ~/…：命令里 ~ 不加引号，sbatch 的日志用相对路径，包里 $HOME/~ 的写法照旧")


def test_adapters_in_relay():
    """调度器的读写都经 scripts/taskq/sites/pueue.py、slurm.py（10/31）。"""
    raw = json.dumps(dict(tasks={"3": dict(group="gpu1", label="worker:w1", status=dict(Done=dict(result="Success"))),
                                 "4": dict(group="other", label="x", status="Running")}))
    rs = relay.parse_sched("lab1", dict(sched_raw=raw, taskq={}))
    assert rs["squeue"] is None and [j[0] for j in rs["jobs"]] == [3] and rs["jobs"][0][3] == "Success", rs
    assert relay.parse_sched("lab1", dict(sched_raw=None))["jobs"] is None
    rs = relay.parse_sched(NAME, dict(sched_raw="101|w|RUNNING|1:00|long|n1\n"))
    assert rs["jobs"] is None and rs["squeue"] == [["101", "w", "RUNNING", "1:00", "long", "n1"]]
    seen = []
    relay.rsh, rsh0 = (lambda name, cmd, input=None, timeout=300: (seen.append((name, cmd)), (0, "4870\n", ""))[1]), relay.rsh
    try:
        msgs, stuck = relay.submit(NAME, [("long", "1:00:00", 2, "worker.sbatch")])
        assert not stuck and len(seen) == 2 and all(re.search(r"sbatch --parsable -J [a-z]+-[a-z]+ --qos=long --time=1:00:00 worker.sbatch", c) for _, c in seen)
        assert seen[0][1].startswith(f"cd {relay.R.q(relay.root(NAME))}/code/current/scripts/taskq/queue && ")
        seen.clear()
        msgs, stuck = relay.submit("dicc1", [("normal", "08:00:00", 1, "cpu_worker.sbatch")])
        assert not stuck and len(seen) == 1 and seen[0][1].endswith("--qos=normal --time=08:00:00 cpu_worker.sbatch") and "CPU worker" in msgs[0], (seen, msgs)
        seen.clear()
        msgs, stuck = relay.submit("lab1", [("pueue", "gpu", 1)])
        assert not stuck and len(seen) == 1 and " add -g " in seen[0][1] and "worker:" in seen[0][1], seen
        seen.clear()
        msgs, stuck = relay.submit("lab1", [("pueue", "cpu", 1)])
        cw = relay.conf("lab1")["cpu_worker"]
        assert not stuck and len(seen) == 1 and f"add -g {cw['group']} -l worker:{cw['id']}" in seen[0][1] and "WORKER_DEVICE=cpu" in seen[0][1], seen
        relay.rsh = lambda name, cmd, input=None, timeout=300: (1, "boom", "")
        _, stuck = relay.submit("lab1", [("pueue", "gpu", 1), ("pueue", "cpu", 1)])
        assert list(stuck) == ["site:lab1:worker", "site:lab1:worker:cpu"], stuck
        _, stuck = relay.submit(NAME, [("long", "1:00:00", 1, "worker.sbatch")])
        assert list(stuck) == [f"topup:{NAME}:long"], stuck
        _, stuck = relay.submit("dicc1", [("normal", "08:00:00", 1, "cpu_worker.sbatch")])
        assert list(stuck) == ["topup:dicc1:normal:cpu"], stuck
    finally:
        relay.rsh = rsh0
    assert not hasattr(relay, "worker_jobs")
    raw = json.dumps(dict(tasks={"3": dict(group="gpu1", label="worker:w1", status="Running"),
                                 "5": dict(group="cpu", label="worker:w2", status="Queued"),
                                 "6": dict(group="gpu0", label="x", status="Running")}))
    assert [j[0] for j in relay.parse_sched("lab1", dict(sched_raw=raw))["jobs"]] == [3, 5]
    assert [j[0] for j in relay.parse_sched("lab0", dict(sched_raw=raw))["jobs"]] == [6]
    ok("relay：一次 ssh 读回原始输出、在 box 上用适配器解析（这台各种 worker 的组）；开 worker / 交作业用适配器，"
       "报警的编号 GPU 照旧、CPU 加 :cpu")


def test_fresh_site():
    """新机器第一次：连得上、根目录里还没有代码包（读状态失败）→ 先部署、再读状态；部署失败时这次当连不上。
    已有旧代码包、换新包失败（11/3：切换当天新路径对着旧包；推的时候出错、超时，或打包出错）→ 这次也当连不上，只续约。"""
    keep = {k: getattr(relay, k) for k in ("remote_status", "undeployed", "deploy", "write_status", "update_state", "load_state", "plan", "fill", "topup", "log")}
    build, auto = relay.RB.build, dict(relay.P.AUTO)
    try:
        for fail, old in ((None, False), ("rt", False), ("rt", True), ("timeout", True), ("build", True)):
            calls = []
            seq = [{"bundle": "b0", "jobs": [], "squeue": None}] if old else [None, {"bundle": "b1", "jobs": [], "squeue": None}]
            relay.remote_status = lambda n: seq.pop(0) if seq else None
            relay.undeployed = lambda n: not old
            def bld(n, c):
                if fail == "build":
                    raise RuntimeError("打包出错")
                return "b1", "/nonexistent"
            relay.RB.build = bld
            def dep(n, b, p):
                if fail == "rt":
                    raise RuntimeError("推不过去")
                if fail == "timeout":
                    raise subprocess.TimeoutExpired("ssh", 600)
                calls.append(("deploy", n, b))
            relay.deploy = dep
            relay.write_status = lambda n, rs, st: calls.append(("status", rs.get("bundle")))
            relay.update_state = lambda fn: None
            relay.load_state = lambda: {}
            relay.plan = lambda s, n, rs, st: calls.append(("plan", rs and rs.get("bundle"))) or ([], {})
            relay.fill = lambda *a: ([], "")
            relay.topup = lambda *a: ([], "")
            relay.log = lambda lines: None
            relay.P.AUTO["lab0"] = True
            s = {"ok": {relay.M.hub(): True}, "taskq": {relay.taskq.queue_host(): {"tasks": []}}}
            try:
                relay.run_one(s, "lab0", False)
            except Exception:  # noqa: BLE001  后面的步骤不在这里测
                pass
            if fail:
                assert ("plan", None) in calls and not any(c[0] == "status" for c in calls), (old, calls)
            else:
                assert calls[:3] == [("deploy", "lab0", "b1"), ("status", "b1"), ("plan", "b1")], calls
    finally:
        for k, v in keep.items():
            setattr(relay, k, v)
        relay.RB.build = build
        relay.P.AUTO.clear(); relay.P.AUTO.update(auto)
    print("通过：新机器还没有代码包时先部署再读状态；部署失败（新机器或换新包）这次当连不上")


def test_auto_off():
    """AUTO 关着（11/2）：不传、不重传、不让那边接着训、不领新的、不交作业；训完的照常拉回。"""
    keep = {k: getattr(relay, k) for k in ("remote_status", "undeployed", "write_status", "update_state", "load_state", "plan", "fill",
                                           "topup", "log", "execute", "submit")}
    auto = dict(relay.P.AUTO)
    try:
        ran = []
        relay.remote_status = lambda n: {"bundle": None, "free": 1e12}
        relay.undeployed = lambda n: False
        relay.write_status = lambda n, rs, st: None
        relay.update_state = lambda fn: None
        relay.load_state = lambda: {}
        relay.plan = lambda s, n, rs, st: ([dict(op=o, key=o) for o in ("beat", "stage", "restage", "redo", "rresume", "fetch")], {})
        relay.fill = lambda *a: ([dict(op="take", key="t")], "")
        relay.topup = lambda *a: ([("long", "x", 1, "worker.sbatch")], "")
        relay.log = lambda lines: None
        relay.execute = lambda n, acts, s: ran.extend(a["op"] for a in acts) or []
        relay.submit = lambda n, subs: ran.append("submit") or ([], {})
        relay.P.AUTO[NAME] = False
        s = {"ok": {relay.M.hub(): True}, "taskq": {relay.taskq.queue_host(): {"tasks": [dict(key="x", worker=NAME, state="claimed")]}}}
        relay.run_one(s, NAME, False)
        assert ran == ["beat", "fetch"], ran
    finally:
        for k, v in keep.items():
            setattr(relay, k, v)
        relay.P.AUTO.clear(); relay.P.AUTO.update(auto)
    print("通过：AUTO 关着 → 只续领取、拉回训完的；不传、不接着训、不领新的、不交作业")


def test_refcount():
    """删一个任务时只删别的任务不用的路径：同一个数据集的几折共用视图，推理的输入在训练的产出下面（11/1）。"""
    keys = {"t0": dict(views=["pre/X"], outs=["res/X/tr/frag_0"]),
            "t1": dict(views=["pre/X"], outs=["res/X/tr/frag_1"]),
            "inf": dict(inputs=["res/X/tr/frag_0/final.ckpt", "res/X/tr/dataset.json", "raw/A/img/a.dat"],
                        outs=["pred/X/tr/frag_0"]),
            "ev": dict(inputs=["pred/X/tr/frag_0/a.dat", "raw/A/lab/a.dat"], outs=["an/X/frag_0/eval"])}
    P = lambda k: relay.paths_of(keys[k])  # noqa: E731
    assert relay.droppable(keys, "t0", P("t0")) == []
    assert relay.droppable(keys, "t1", P("t1")) == ["res/X/tr/frag_1"]
    assert relay.droppable(keys, "inf", P("inf")) == ["raw/A/img/a.dat", "res/X/tr/dataset.json"]
    assert relay.droppable(keys, "ev", P("ev")) == ["an/X/frag_0/eval", "raw/A/lab/a.dat"]
    k2 = {k: v for k, v in keys.items() if k not in ("t1", "inf")}
    assert relay.droppable(k2, "t0", P("t0")) == ["pre/X", "res/X/tr/frag_0"]
    assert relay.overlaps("a/b", {"a/b/c"}, relay.ancestors({"a/b/c"})) and relay.overlaps("a/b/c/d", {"a/b"}, set())
    assert not relay.overlaps("a/bc", {"a/b"}, relay.ancestors({"a/b"}))
    ok("引用计数：共用的视图、别的任务输入所在的产出不删；只是名字前缀相同的不算")

    seen, rpy0, rtq0, st0 = [], relay.rpy, relay.rtq, relay.load_state
    relay.rpy = lambda name, code, data=None, **kw: (seen.append((code, data)), 1)[1]
    relay.rtq = lambda name, cmd: (0, "")
    relay.load_state = lambda: dict(keys={NAME: keys})
    up0 = relay.update_state
    relay.update_state = lambda fn: None
    try:
        msgs = relay.execute(NAME, [dict(op="drop", key="inf", rkey="inf")], S0([]))
        assert seen and seen[-1][0] == relay.DROP_PY and seen[-1][1] == dict(paths=["raw/A/img/a.dat", "res/X/tr/dataset.json"], logs="inf"), (seen, msgs)
        seen.clear()
        msgs = relay.execute(NAME, [dict(op="redo", key="ev", rkey="ev", busy=True)], S0([]))
        assert not seen and "等它停下" in msgs[0], msgs
        msgs = relay.execute(NAME, [dict(op="redo", key="ev", rkey="ev", busy=False)], S0([]))
        assert seen[-1][1]["paths"] == ["an/X/frag_0/eval", "raw/A/lab/a.dat"] and "重新传" in msgs[0], msgs
    finally:
        relay.rpy, relay.rtq, relay.load_state, relay.update_state = rpy0, rtq0, st0, up0
    ok("drop / redo 只删算出来能删的路径和自己的日志；redo 时那边还领着就等")


def test_drop_refused():
    """那边拒绝删任务（有人在训）时不删结果、总队列里也不交还；上次删了任务没删完结果的照常删（审查 11/1）。"""
    K = "D1_x/tr/0"
    seen, central = [], []
    rpy0, rtq0, cen0 = relay.rpy, relay.rtq, relay.taskq.central
    relay.rpy = lambda name, code, data=None, **kw: (seen.append(data), 1)[1]
    relay.taskq.central = lambda args, timeout=300: (central.append(args), (0, "", ""))[1]
    acts = [dict(op="drop", key=K, rkey=K), dict(op="release", key=K, why="测试")]
    try:
        for r in [(1, f"不删：{K}：claimed，有人在做"), None]:
            seen.clear(); central.clear()
            relay.rtq = lambda name, cmd, r=r: r
            msgs = relay.execute(NAME, acts, S0([]))
            assert not seen, (r, seen)
            assert not central, (r, central, msgs)
        seen.clear()
        relay.rtq = lambda name, cmd: (1, f"队列里没有：{K}")
        msgs = relay.execute(NAME, acts, S0([]))
        assert seen and central, (seen, central, msgs)
        central.clear()
        relay.rtq = lambda name, cmd: (0, "")
        relay.rpy = lambda name, code, data=None, **kw: (_ for _ in ()).throw(OSError("ssh 断了"))
        msgs = relay.execute(NAME, acts, S0([]))
        assert not central and any("先不交还" in m for m in msgs), (central, msgs)
    finally:
        relay.rpy, relay.rtq, relay.taskq.central = rpy0, rtq0, cen0
    ok("那边拒绝删任务或连不上时不删结果、不交还；任务已经删掉的照常删结果；删路径时出错也不交还")


def test_view_lock():
    """建视图在那边一次只跑一个（11/2：同一个数据集的两折同时建视图，互相删了 .incomplete）。"""
    import fcntl
    root = tempfile.mkdtemp()
    os.makedirs(os.path.join(root, "state"))
    lk = open(os.path.join(root, "state", "view.lock"), "w")
    fcntl.flock(lk, fcntl.LOCK_EX)
    data = json.dumps(dict(views=["prepared/D1"], links={}, texts={"prepared/D1/a.json": "e30="}))
    repo = _cfg.REPO
    p = subprocess.Popen([sys.executable, "-c", relay.VIEW_PY, root], cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    p.stdin.write(data)
    p.stdin.close()
    time.sleep(1.5)
    waited = p.poll() is None
    fcntl.flock(lk, fcntl.LOCK_UN)
    lk.close()
    p.wait(timeout=30)
    d = os.path.join(root, "data", "prepared/D1")
    assert waited and p.returncode == 0 and os.path.isfile(os.path.join(d, "a.json")) and not os.path.exists(os.path.join(d, ".incomplete"))
    shutil.rmtree(root)
    ok("建视图：别的在建时等锁，建完删掉 .incomplete")


def test_list_unreadable():
    """拉回前列结果：读不了的子目录不能静默跳过（11/3：只拉回一部分、box 上算训完，下一轮删掉那边的整份）。"""
    root = tempfile.mkdtemp()
    out = os.path.join(root, "data", "out", "k")
    os.makedirs(os.path.join(out, "check"))
    os.makedirs(os.path.join(out, "part"))
    open(os.path.join(out, "check", "DONE"), "w").close()
    open(os.path.join(out, "part", "a"), "w").close()
    spec = dict(key="k", out="out/k", done=["check/DONE"])
    repo = _cfg.REPO
    run = lambda outs: subprocess.run([sys.executable, "-c", relay.LIST_PY, root], input=json.dumps(dict(spec=spec, outs=outs, key="k")),
                                      cwd=repo, capture_output=True, text=True)
    r = run(["out/k", "out/none"])
    res = S.json_in(r.stdout)
    assert r.returncode == 0 and res["done"] and sorted(i[0] for i in res["items"]) == ["out/k/check/DONE", "out/k/part/a"], (r, res)
    os.chmod(os.path.join(out, "part"), 0)
    try:
        r = run(["out/k"])
    finally:
        os.chmod(os.path.join(out, "part"), 0o755)
    shutil.rmtree(root)
    assert r.returncode != 0 and S.json_in(r.stdout) is None, (r.returncode, r.stdout)
    ok("拉回前列结果：读不了的子目录 → 出错（不拉回、不算做完）；不存在的结果文件夹照样跳过")


def test_deploy_keeps_busy():
    """清理旧包：Slurm 排着、在跑的作业的工作目录（交作业时在那个包里）不删；读不到 squeue 时一个都不删；pueue 照旧按新旧删。"""
    c0, sq = M.RELAY[NAME], os.path.join(BIN, "squeue")
    src = os.path.join(T, "dk_src")
    os.makedirs(src, exist_ok=True)
    TN.wb(os.path.join(src, "f"), b"x")
    old = time.time() - 10 * 86400
    try:
        for sched, squeue, want in (("slurm", "busy", {"a", "n", "c", "d"}), ("slurm", "fail", {"a", "b", "n", "c", "d"}),
                                    ("pueue", "busy", {"n", "c", "d"})):
            r = tempfile.mkdtemp(dir=T, prefix="dk_")
            M.RELAY[NAME] = dict(c0, root=r, scheduler=sched)
            for i, d in enumerate("abcd"):
                os.makedirs(os.path.join(r, "code", d, "scripts", "taskq", "queue"))
                t = old if d in "ab" else time.time() - 3600 * (4 - i)
                os.utime(os.path.join(r, "code", d), (t, t))
            with open(sq, "w") as f:
                f.write(f"#!/bin/bash\necho {r}/code/a/scripts/taskq/queue\n" if squeue == "busy" else "#!/bin/bash\nexit 1\n")
            os.chmod(sq, 0o755)
            relay.deploy(NAME, "n", src)
            got = set(os.listdir(os.path.join(r, "code"))) - {"current"}
            assert got == want and os.readlink(os.path.join(r, "code", "current")) == "n", (sched, squeue, got)
    finally:
        M.RELAY[NAME] = c0
        os.path.exists(sq) and os.remove(sq)
    ok("清理旧包：排着的作业的工作目录所在的包留着；squeue 读不到时不删；pueue 照旧")


def test_deploy_safety():
    """推代码包：传了一半不装、current 不动；重推一个已有的旧包时不当场删掉它；同时推两次时 current 只指向完整的包。"""
    import threading
    c0 = M.RELAY[NAME]
    src = os.path.join(T, "ds_src")
    for i in range(200):
        TN.wb(os.path.join(src, "d", f"f{i}"), os.urandom(4096))
    n_files = 200
    full = lambda r, b: sum(len(fs) for _, _, fs in os.walk(os.path.join(r, "code", b)))  # noqa: E731
    import tarfile
    tb = subprocess.run(["tar", "-C", src, "-cf", "-", "."], capture_output=True, check=True).stdout
    at = tarfile.open(fileobj=io.BytesIO(tb)).getmembers()[100].offset  # 断在两个文件之间：tar 不报错
    cut = os.path.join(T, "cut_ssh")
    with open(cut, "w") as f:
        f.write(f"#!/bin/bash\nhead -c {at} | {FAKE} \"$@\"\n")
    os.chmod(cut, 0o755)
    try:
        r = tempfile.mkdtemp(dir=T, prefix="ds_")
        M.RELAY[NAME] = dict(c0, root=r, scheduler="pueue")
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

        r = tempfile.mkdtemp(dir=T, prefix="ds_")
        M.RELAY[NAME] = dict(c0, root=r, scheduler="pueue")
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
    finally:
        M.RELAY[NAME] = c0
        os.environ["RELAY_SSH"] = FAKE


if __name__ == "__main__":
    test_drop_refused()
    test_refcount()
    test_fresh_site()
    test_auto_off()
    test_plan()
    test_pueue_worker()
    test_slurm_topup_ready()
    test_tilde_root()
    test_adapters_in_relay()
    test_adopt_out()
    test_place()
    test_e2e_path(test_e2e())
    test_view_lock()
    test_list_unreadable()
    test_deploy_keeps_busy()
    test_deploy_safety()
    shutil.rmtree(T)
    shutil.rmtree(STATE_DIR)
    print("全部通过")
