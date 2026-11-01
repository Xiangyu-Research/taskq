#!/usr/bin/env python3
"""relay.py 的测试：
  1. 每次检查的决策（plan / fill）：假的总队列、那边的状态、box 的任务；
  2. 拉回时挪进数据目录的顺序：中途出错时训完的标志不在（不会被当成训完）；
  3. 真跑一遍：本机模拟"那边"（假的 ssh、srun 在本地跑，服务开在 127.0.0.1），推代码包、传数据（stage）、
     那边的队列、拉回（fetch）、换回真名。
跑法：python3 scripts/queue/test_relay.py（在 box 上；checkpoint 用本机的 nnunet 环境）"""
import json
import os
import shutil
import socket
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import test_relay_names as TN  # noqa: E402  （设了 RELAY_DIR、建了临时目录 TN.TMP）

T = TN.TMP
FAKE = os.path.join(T, "fake_ssh")
with open(FAKE, "w") as f:
    f.write(f'#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\n[[ $1 == -O ]] && {{ [[ -e {T}/master_down ]] && exit 255; exit 0; }}\n'
            'shift\ncd / && exec bash -c "$*"\n')
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

import machines as M  # noqa: E402
import spec as S  # noqa: E402
import relay  # noqa: E402
import relay_bundle  # noqa: E402

NAME = next(iter(M.RELAY))
RROOT = os.path.join(T, "remote")
CONDA = os.path.join(T, "conda")
os.makedirs(os.path.join(CONDA, "bin"))
os.symlink(relay_bundle.nnunet_python(), os.path.join(CONDA, "bin", "python3"))
M.RELAY[NAME] = dict(M.RELAY[NAME], root=RROOT, conda=(CONDA, "pt"), transfer_job="-p test")
Q = "box"  # 总队列所在的机器（taskq.queue_host()，10/31）
assert relay.taskq.queue_host() == Q


def test_pueue_worker():
    w = relay.conf("lab1")["worker"]
    lab = f"worker:{w['id']}"
    ready = dict(tasks=[dict(key="a/x/0", state="free"), dict(key="a/x/1", state="claimed")])
    none = dict(tasks=[dict(key="a/x/1", state="claimed")])
    assert relay.topup("lab1", dict(jobs=None, taskq=ready)) == ([], "读不到 pueue")
    assert relay.topup("lab1", dict(jobs=[], taskq=ready)) == ([("pueue", None, 1)], "")
    assert relay.topup("lab1", dict(jobs=[[3, lab, "Running", None]], taskq=ready)) == ([], "worker 在")
    assert relay.topup("lab1", dict(jobs=[[3, lab, "Done", "Killed"], [4, "other", "Running", None]], taskq=ready)) == ([("pueue", None, 1)], "")
    subs, why = relay.topup("lab1", dict(jobs=[[3, lab, "Done", "{'Failed': 1}"]], taskq=ready))
    assert not subs and why.startswith("上一个 worker")
    ok("pueue 类（lab1）：组里没有活着的 worker 才开，上一个失败了不重开")
    assert relay.topup("lab1", dict(jobs=[], taskq=none)) == ([], "那边没有备好、没人领的任务")
    assert relay.topup("lab1", dict(jobs=[[3, lab, "Done", "Success"]], taskq=none)) == ([], "那边没有备好、没人领的任务")
    assert relay.topup("lab1", dict(jobs=[[3, lab, "Done", "Success"]], taskq=ready)) == ([("pueue", None, 1)], "")
    ok("pueue 类：没有备好的任务不开 worker（空闲退出的那个不算失败），有了再开")


def test_slurm_topup_ready():
    two = dict(enabled=True, tasks=[dict(key="a/x/0", state="free"), dict(key="a/x/1", state="free"), dict(key="a/x/2", state="claimed")])
    probe = lambda out: (lambda name, cmd: (0, out, ""))  # noqa: E731
    q = next(iter(relay.P.SLURM[NAME]["gpu"]))
    subs, _ = relay.topup(NAME, dict(taskq=two), run=probe(f"QOS|{q}|10|0|0\n"))
    assert [n for _, _, n in subs] == [2], subs
    subs, why = relay.topup(NAME, dict(taskq=two), run=probe(f"QOS|{q}|10|3|2\n"))
    assert subs == [] and "排着的作业已有 2 个" in why
    subs, why = relay.topup(NAME, dict(taskq=dict(enabled=True, tasks=[])), run=probe(f"QOS|{q}|10|0|0\n"))
    assert subs == [] and "备好" in why
    ok("Slurm 类（DICC2）：备好 2 个最多交 2 个，排着的够了不交，没备好不交")


def ok(msg):
    print("通过：" + msg)


# ---------------- 1. 决策 ----------------
def mtask(key, state="claimed", worker=NAME, nice=5, since=1000.0, relay_="nnunet"):
    return dict(key=key, state=state, worker=worker, nice=nice, order=1, since=since, note="",
                spec=dict(env="nnunet", data=[f"nnUNet/preprocessed/{key.split('/')[0]}"], relay=relay_, device="gpu", machines=None),
                skipped_by=[])


def rtask(key, state, worker=None):
    return dict(key=key, state=state, worker=worker, nice=5, order=1, note="", failures=0, spec=dict(out=f"out/{key}"))


def S0(mt, box=(), recs=()):
    return dict(taskq={Q: dict(enabled=True, tasks=mt)}, pueue={M.hub(): dict(tasks=list(box))}, records=list(recs),
                ok={M.hub(): True}, src={n: dict(tasks={}) for n in M.names("train")})


def keymap(*pairs):
    return dict(keys={NAME: {k: dict(alias=a, recipe="nnunet", params={}, data=[f"nnUNet/preprocessed/{k.split('/')[0]}"])
                             for k, a in pairs}})


def ops(acts):
    return sorted((a["op"], a["key"]) for a in acts)


def test_adopt_out():
    """老数据目录、别的执行机里训到一半的存档（最新的那份）拷进别名结果目录；这边已有更新的存档或训完了时不动。"""
    import subprocess
    old, sib, R = os.path.join(T, "ao_old"), os.path.join(T, "ao_sib"), os.path.join(T, "ao_R")
    src, ssrc = os.path.join(old, "res", "fold_3"), os.path.join(sib, "data", "a", "fold_3")
    os.makedirs(src)
    for f, b in (("checkpoint_latest.pth", b"L"), ("checkpoint_best.pth", b"B"), ("training_log.txt", b"t")):
        open(os.path.join(src, f), "wb").write(b)
    dirs = [ssrc, src]
    run = lambda: S.json_in(subprocess.run(
        [sys.executable, "-c", relay.ADOPT_OUT_PY, R], input=json.dumps(dict(srcs=dirs, alias="a/fold_3", resume=["checkpoint_latest.pth", "checkpoint_final.pth"],
                                                                         done=["validation/summary.json", "checkpoint_final.pth|ARCHIVED"])),
        capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(HERE)), check=True).stdout)
    r = run()
    assert r["n"] == 3 and r["src"] == src, r
    new = os.path.join(R, "data", "a", "fold_3")
    assert open(os.path.join(new, "checkpoint_latest.pth"), "rb").read() == b"L"
    assert not [f for f in os.listdir(new) if f.endswith(".relaytmp")]
    open(os.path.join(new, "checkpoint_latest.pth"), "wb").write(b"N")
    assert run()["n"] == 0 and open(os.path.join(new, "checkpoint_latest.pth"), "rb").read() == b"N"
    ok("老目录的存档拷进别名结果目录；已有更新的存档时不覆盖")
    os.makedirs(ssrc)
    open(os.path.join(ssrc, "checkpoint_latest.pth"), "wb").write(b"S")
    t = time.time() + 60
    os.utime(os.path.join(ssrc, "checkpoint_latest.pth"), (t, t))
    r = run()
    assert r["src"] == ssrc and open(os.path.join(new, "checkpoint_latest.pth"), "rb").read() == b"S", r
    ok("另一台执行机（同一个盘、换卡）的存档更新 → 拷它的，接着训")
    open(os.path.join(new, "checkpoint_final.pth"), "wb").write(b"F")
    wb_ = os.path.join(new, "validation", "summary.json")
    os.makedirs(os.path.dirname(wb_))
    open(wb_, "w").write("{}")
    os.utime(os.path.join(ssrc, "checkpoint_latest.pth"), (t + 60, t + 60))
    assert run()["n"] == 0 and open(os.path.join(new, "checkpoint_latest.pth"), "rb").read() == b"S"
    ok("这边已经训完（任务说明的 done 都在）→ 不动")


def test_plan():
    K, A = "Dataset1_x/tr/0", "Dataset901_ab/tr/0"
    st = keymap((K, A))
    RS = lambda *ts, **kw: dict(taskq=dict(tasks=list(ts), paused=kw.get("paused", False)), tasks=kw.get("tasks", {}), free=500e9)  # noqa: E731

    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "claimed", "123")), st)
    assert ops(acts) == [("beat", K)] and not stuck, acts
    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "free")), st)
    assert ops(acts) == [("beat", K)], acts
    ok("那边排着、在训 → 只续")

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

    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "done")), st)
    assert ops(acts) == [("beat", K), ("fetch", K)], acts
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
    fcmd = dict(fdone[0], command="python3 scripts/queue/relay.py fetch x k && bash scripts/queue/archive_task.sh box k")
    try:
        C.backup_down = lambda ok=None: "dicc 暂停检查（停机）"
        acts, stuck = relay.plan(S0([mtask(K, "done")], [dict(fdone[0], status="Failed")], [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K)] and not stuck, (acts, stuck)
        acts, stuck = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K)] and not stuck, (acts, stuck)
        C.backup_down = lambda ok=None: None
        acts, stuck = relay.plan(S0([mtask(K, "done")], [dict(fdone[0], status="Failed")], [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("archive", K), ("beat", K)] and not stuck, (acts, stuck)
        acts, _ = relay.plan(S0([mtask(K, "done")], fdone, [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("archive", K), ("beat", K)], acts
        acts, _ = relay.plan(S0([mtask(K, "done")], [fcmd], [frec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("drop", K), ("release", K)], acts
        two = [dict(fdone[0], status="Failed"), dict(id=9, label=arec["label"], status="Running")]
        acts, stuck = relay.plan(S0([mtask(K, "done")], two, [frec, arec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K)] and not stuck, (acts, stuck)
        two[1]["status"] = "Success"
        acts, _ = relay.plan(S0([mtask(K, "done")], two, [frec, arec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("drop", K), ("release", K)], acts
        two[1]["status"] = "Failed"
        acts, stuck = relay.plan(S0([mtask(K, "done")], two, [frec, arec]), NAME, RS(rtask(A, "done")), st)
        assert ops(acts) == [("beat", K)] and f"relayfetch:{K}" in stuck, (acts, stuck)
    finally:
        relay.lookup, C.backup_down = old_lookup, old_down
    ok("要归档的折：备份机暂停检查时拉回后先不归档、领取保留、不报；备份机回来后单独排归档（relay_archive），归档成功才交还、删那边的；归档出错 → 报")

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
    acts, stuck = relay.plan(S0([]), NAME, RS(rtask("Dataset999_zz/tr/0", "free")), st)
    assert ops(acts) == [] and list(stuck) == [f"site:{NAME}:orphan:Dataset999_zz/tr/0"], stuck
    acts, stuck = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(rtask(A, "claimed", "123")), st)
    assert ops(acts) == [] and list(stuck) == [f"site:{NAME}:orphan:{K}"], stuck
    ok("那边有、总队列里不归它的：不让它训；总队列训完了就删；认不出的、在训的报 site:<名字>:orphan")

    acts, stuck = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(rtask(A, "free")), st)
    assert ops(acts) == [("unq", K)] and acts[0]["to"] == "other" and not stuck, acts
    ust = dict(keys={NAME: {K: dict(st["keys"][NAME][K], recipe="nnunet", params=dict(dataset="Dataset901_ab", fold="0"), unq=1.0)}})
    acts, _ = relay.plan(S0([mtask(K, "claimed", "other")]), NAME, RS(), ust)
    assert ops(acts) == [], acts
    acts, _ = relay.plan(S0([mtask(K, "done", "other")]), NAME, RS(), ust)
    assert ops(acts) == [("drop", K)] and acts[0].get("gone") and acts[0]["out"], acts
    acts, _ = relay.plan(S0([]), NAME, RS(), ust)
    assert ops(acts) == [("drop", K)] and acts[0].get("gone"), acts
    ok("总队列里改分给了别的执行机（换卡）→ 那边队列里删掉（unq）、结果留着给它收编；总队列训完了再删结果（不再删队列）")

    acts, stuck = relay.plan(S0([mtask(K)]), NAME, None, dict(st, last_ok={NAME: time.time() - 3 * 3600}))
    assert ops(acts) == [("beat", K)] and f"site:{NAME}:down" in stuck, stuck
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, None, dict(st, last_ok={NAME: time.time() - 600}))
    assert ops(acts) == [("beat", K)] and not stuck
    ok("连不上：只续总队列的领取，2 小时以上报")

    K2, K3 = "Dataset2_y/tr/0", "Dataset3_z/tr/0"
    cands = [mtask(K2, "free", None, nice=3), mtask(K3, "free", None, nice=4), mtask("Dataset4_c/tr/0", "free", None, relay_=None)]
    relay.refill.data_gb = lambda paths: 10.0
    relay.P.READY[NAME] = 2
    more, why = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "claimed", "1")), st, [])
    assert [a["key"] for a in more] == [K2, K3], (more, why)
    more, _ = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "free")), st, [])
    assert [a["key"] for a in more] == [K2], more
    more, why = relay.fill(S0([mtask(K), mtask("Dataset5_d/tr/0")] + cands), NAME, RS(rtask(A, "free")), st, [])
    assert more == [] and "排着 2" in why, (more, why)
    more, why = relay.fill(S0([mtask(K)] + cands), NAME, dict(RS(rtask(A, "free")), free=105e9), st, [])
    assert more == [] and "放不下" in why, why
    ok("补任务：那边排着的（含在传数据的）不够 READY 个才领，按档位，只领能翻译的做法，放不下不领")
    K0 = "Dataset6_e/tr/0"
    more, why = relay.fill(S0([mtask(K), mtask("Dataset5_d/tr/0"), mtask(K0, "free", None, nice=-100)] + cands), NAME,
                           RS(rtask(A, "free")), st, [])
    assert [a["key"] for a in more] == [K0], (more, why)
    more, why = relay.fill(S0([mtask(K), mtask(K0, "free", None, nice=-100)] + cands), NAME, RS(rtask(A, "free")), st, [])
    assert [a["key"] for a in more] == [K0, K2], (more, why)
    ok("第 ⓪ 档（nice < 0，训完后的推理）不受 READY 限制：排满了也领，先领它")

    relay.PLANNED.clear()
    a1, _ = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "free")), st, [])
    disk = next(iter(relay.PLANNED))
    relay.PLANNED[disk] = 395.0
    more, why = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "free")), st, [])
    assert a1 and more == [] and "放不下" in why, (a1, more, why)
    relay.PLANNED.clear()
    ok("同一个盘上的几台（lab0、lab1）：这次检查里已经计划传的算进去")

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
    real = dict(out="nnUNet/results/X/tr/fold_0", done=["validation/summary.json", "checkpoint_final.pth|ARCHIVED"],
                fetch=dict(extra=["*.json"]))
    written = ["nnUNet/results/X/tr/fold_0/validation/a.nii.gz", "nnUNet/results/X/tr/plans.json",
               "logs/tasks/k__1_seg1.log", "nnUNet/results/X/tr/fold_0/checkpoint_final.pth",
               "nnUNet/results/X/tr/fold_0/validation/summary.json"]
    for w in written:
        TN.wb(os.path.join(src, w), b"new")
    TN.wb(os.path.join(D, "nnUNet/results/X/tr/plans.json"), b"old")
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
    assert not os.path.exists(os.path.join(D, real["out"], "validation/summary.json"))
    assert not os.path.exists(os.path.join(D, real["out"], "checkpoint_final.pth"))
    relay.place(src, D, [w for w in written if os.path.exists(os.path.join(src, w))], real)
    assert open(os.path.join(D, "nnUNet/results/X/tr/plans.json"), "rb").read() == b"old"
    assert seen[:2] == ["nnUNet/results/X/tr/fold_0/validation/a.nii.gz", "logs/tasks/k__1_seg1.log"], seen
    assert os.path.exists(os.path.join(D, real["out"], "validation/summary.json"))
    ok("拉回：训完的标志最后挪（中途出错时不算训完），上一级已有的 JSON 不覆盖")

    src, D = os.path.join(T, "place2_src"), os.path.join(T, "place2_dst")
    real = dict(out="predictions/m/fold_0", done=[".infer_done"], fetch=dict(extra=[]))
    written = ["predictions/m/fold_0/.infer_done", "predictions/m/fold_0/MANIFEST.json", "predictions/m/fold_0/c1.nii.gz",
               "predictions/m/note.txt"]
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
    assert seen == ["predictions/m/fold_0/c1.nii.gz", "predictions/m/note.txt", "predictions/m/fold_0/MANIFEST.json",
                    "predictions/m/fold_0/.infer_done"], seen
    ok("拉回：MANIFEST.json 在训完的标志前、普通文件后挪")


# ---------------- 3. 真跑 ----------------
def test_e2e():
    TN.setup()
    sp = S.resolve("nnunet", dict(dataset=TN.REAL, fold="0"))
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

    akey = relay.stage(NAME, sp["key"], nice=5, say=lambda m: None)
    alias = akey.split("/")[0]
    pre = os.path.join(RROOT, "data", "nnUNet", "preprocessed", alias)
    links = [os.path.join(dp, f) for dp, _, fs in os.walk(pre) for f in fs]
    assert links and all(os.path.isfile(p) for p in links), links
    assert not os.path.exists(os.path.join(pre, ".incomplete"))
    gt = sorted(os.listdir(os.path.join(pre, "gt_segmentations")))
    assert gt == ["c0001.nii.gz", "c0002.nii.gz", "c0003.nii.gz", "c0004.nii.gz"], gt
    assert open(os.path.join(pre, "gt_segmentations", "c0001.nii.gz"), "rb").read() == b"gt100101A"
    assert TN.REAL not in open(os.path.join(pre, "dataset.json")).read()
    blobs = sum(len(fs) for _, _, fs in os.walk(os.path.join(RROOT, "blobs")))
    rs = relay.remote_status(NAME)
    assert [(t["key"], t["state"]) for t in rs["taskq"]["tasks"]] == [(akey, "free")], rs["taskq"]["tasks"]
    ok(f"传数据：{blobs} 个文件按内容存、软链接视图、别名，加进那边的队列")

    akey2 = relay.stage(NAME, sp["key"], nice=5, say=lambda m: None)
    assert akey2 == akey and sum(len(fs) for _, _, fs in os.walk(os.path.join(RROOT, "blobs"))) == blobs
    ok("再传一次：不重传、不重复加")

    asp = S.resolve("nnunet", relay.keys_of(relay.load_state(), NAME)[sp["key"]]["params"])
    TN.fake_output(os.path.join(RROOT, "data"), asp, alias)
    TN.wb(os.path.join(RROOT, "data", asp["out"], "checkpoint_latest.pth"), b"latest")
    relay.fetch(NAME, sp["key"], say=lambda m: None)
    out = os.path.join(TN.ROOT, sp["out"])
    assert os.path.exists(os.path.join(out, "validation", "summary.json"))
    assert sorted(f for f in os.listdir(os.path.join(out, "validation")) if f.endswith(".nii.gz")) == ["100101A.nii.gz", "100101A_ig.nii.gz"]
    assert not os.path.exists(os.path.join(out, "checkpoint_latest.pth"))
    assert os.path.exists(os.path.join(out, "checkpoint_final.pth"))
    assert alias not in open(os.path.join(out, "debug.json")).read()
    assert not os.listdir(os.path.join(TN.ROOT, ".relay_in"))
    logs = os.listdir(os.path.join(TN.ROOT, "logs", "tasks"))
    assert logs == [S.safe(sp["key"]) + "__12345_seg1.log"], logs
    ok("拉回：真名、不拉 checkpoint_latest、分段日志改名、临时目录删掉")

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
    akey = relay.stage(NAME, sp["key"], add=False, say=lambda m: None)
    k = relay.keys_of(relay.load_state(), NAME)[sp["key"]]
    assert akey.startswith("job/") and k["views"] == k["params"]["data"] and len(k["views"]) == 3, k
    assert k["cleanup"] == [os.path.dirname(k["params"]["out"])], k
    D = os.path.join(RROOT, "data")
    a_in = k["views"][0]
    assert open(os.path.join(D, a_in, "y_0000.nii.gz"), "rb").read() == b"yy"
    assert open(os.path.join(D, a_in, "a", "x_0000.nii.gz"), "rb").read() == b"xa"
    assert not any(os.path.exists(os.path.join(D, v, ".incomplete")) for v in k["views"])
    after = sorted((os.path.relpath(os.path.join(dp, f), pre), os.readlink(os.path.join(dp, f)) if os.path.islink(os.path.join(dp, f)) else None)
                   for dp, _, fs in os.walk(pre) for f in fs)
    assert after == before, "训练的视图被动了"
    reg = relay.load_state()["views"][NAME]
    assert set(k["views"]) <= set(reg) and any(v.startswith("nnUNet/preprocessed/") for v in reg), reg
    TN.wb(os.path.join(D, a_in, "stray.txt"), b"?")
    relay.stage(NAME, sp["key"], add=False, say=lambda m: None)
    assert not os.path.exists(os.path.join(D, a_in, "stray.txt")) and after == sorted(
        (os.path.relpath(os.path.join(dp, f), pre), os.readlink(os.path.join(dp, f)) if os.path.islink(os.path.join(dp, f)) else None)
        for dp, _, fs in os.walk(pre) for f in fs)
    ok("通用换名传数据：每个输入一个视图（登记了），只在自己的视图里删多出来的文件，同时在那边的训练视图不动")

    asp = S.resolve("command", k["params"])
    TN.wb(os.path.join(D, asp["out"], "a.csv"), b"1")
    TN.wb(os.path.join(D, os.path.dirname(asp["out"]), "plans.json"), b"{}")
    TN.wb(os.path.join(D, "logs", "tasks", S.safe(asp["key"]) + "__9_seg1.log"), b"log")
    try:
        relay.fetch(NAME, sp["key"], say=lambda m: None)
        raise AssertionError("少 .done 应报没做完")
    except relay.RN.RelayError as e:
        assert "还没" in str(e), e
    assert not os.path.exists(os.path.join(TN.ROOT, sp["out"]))
    TN.wb(os.path.join(D, asp["out"], ".done"), b"")
    relay.fetch(NAME, sp["key"], say=lambda m: None)
    out = os.path.join(TN.ROOT, sp["out"])
    assert sorted(os.listdir(out)) == [".done", "a.csv"], os.listdir(out)
    assert os.path.exists(os.path.join(TN.ROOT, "RQ1/analysis/thing/plans.json"))
    assert os.path.exists(os.path.join(TN.ROOT, "logs", "tasks", S.safe(sp["key"]) + "__9_seg1.log"))
    ok("通用换名拉回：那边没做完不拉；做完后结果、上一级的文件、分段日志换回真名放进 box")

    for bad in ("/etc", "a/../../x", "in"):
        try:
            relay.safe_rel(bad)
            raise AssertionError(bad)
        except relay.RN.RelayError:
            pass
    assert relay.views_of(dict(view="nnUNet/preprocessed/D1")) == ["nnUNet/preprocessed/D1"]
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
    assert "#SBATCH --output=../../../../data/logs/workers/%x_%j.out" in sb and "\nR=~/st/t123\n" in sb, sb
    assert relay_bundle.rename("worker_start.sh") in sb  # 显存填充那条路（worker.sbatch → worker_start.sh）不绕开
    txt = "\n".join(t for t, _ in files.values())
    assert "~/st/t123/queue" in txt or "$HOME/st/t123/queue" in txt
    sh = "\n".join(t for p, (t, _) in files.items() if p.endswith((".sh", ".sbatch")))
    assert '"~/st/t123' not in sh, "shell 双引号里的 ~ 不展开（Python 的 expanduser 里可以）"
    assert relay_bundle.home_paths('a="$HOME/.cache/phd/taskq" b=~/.cache/phd/x', "~/st/t1") == 'a="$HOME/st/t1/queue" b=~/st/t1/state/x'
    assert relay_bundle.home_paths('a="$HOME/.cache/phd/taskq"', "/scr/u/r") == 'a="/scr/u/r/queue"'
    ok("根目录 ~/…：命令里 ~ 不加引号，sbatch 的日志用相对路径，包里 $HOME/~ 的写法照旧")


def test_adapters_in_relay():
    """调度器的读写都经 scripts/sites/pueue.py、slurm.py（10/31）。"""
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
        msgs, stuck = relay.submit(NAME, [("long", "1:00:00", 2)])
        assert not stuck and len(seen) == 2 and all("sbatch --parsable --qos=long --time=1:00:00 worker.sbatch" in c for _, c in seen)
        assert seen[0][1].startswith(f"cd {relay.R.q(relay.root(NAME))}/code/current/scripts/queue && ")
        seen.clear()
        msgs, stuck = relay.submit("lab1", [("pueue", None, 1)])
        assert not stuck and len(seen) == 1 and " add -g " in seen[0][1] and "worker:" in seen[0][1], seen
        relay.rsh = lambda name, cmd, input=None, timeout=300: (1, "boom", "")
        _, stuck = relay.submit("lab1", [("pueue", None, 1)])
        assert list(stuck) == ["site:lab1:worker"], stuck
        _, stuck = relay.submit(NAME, [("long", "1:00:00", 1)])
        assert list(stuck) == [f"topup:{NAME}:long"], stuck
    finally:
        relay.rsh = rsh0
    assert not hasattr(relay, "worker_jobs")
    ok("relay：一次 ssh 读回原始输出、在 box 上用适配器解析；开 worker / 交作业用适配器，报警的编号照旧")


def test_fresh_site():
    """新机器第一次：连得上、根目录里还没有代码包（读状态失败）→ 先部署、再读状态；部署失败时这次当连不上。"""
    keep = {k: getattr(relay, k) for k in ("remote_status", "undeployed", "deploy", "write_status", "update_state", "load_state", "plan", "fill", "topup", "log")}
    build, auto = relay.RB.build, dict(relay.P.AUTO)
    try:
        for fail in (False, True):
            calls, seq = [], [None, {"bundle": "b1", "jobs": [], "squeue": None}]
            relay.remote_status = lambda n: seq.pop(0) if seq else None
            relay.undeployed = lambda n: True
            relay.RB.build = lambda n, c: ("b1", "/nonexistent")
            def dep(n, b, p):
                if fail:
                    raise RuntimeError("推不过去")
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
                assert ("plan", None) in calls and not any(c[0] == "status" for c in calls), calls
            else:
                assert calls[:3] == [("deploy", "lab0", "b1"), ("status", "b1"), ("plan", "b1")], calls
    finally:
        for k, v in keep.items():
            setattr(relay, k, v)
        relay.RB.build = build
        relay.P.AUTO.clear(); relay.P.AUTO.update(auto)
    print("通过：新机器还没有代码包时先部署再读状态；部署失败这次当连不上")


def test_forget():
    """推理任务在那边删掉后，box 上为它做的别名 checkpoint 也删掉；训练任务、别的任务的不动（11/1）。"""
    K, A = "Dataset1_x/tr/0", "Dataset901_ab/tr/0"
    top = os.path.join(relay.RN.RELAY_DIR, "ckpt")
    mine = os.path.join(top, "Dataset901_ab", "tr", "fold_0")
    other = os.path.join(top, "Dataset901_ab", "tr", "fold_1")
    for d in (mine, other):
        os.makedirs(d, exist_ok=True)
        for f in ("checkpoint_final.pth", "checkpoint_final.pth.src"):
            open(os.path.join(d, f), "w").close()
    seen = []
    rsh0, rtq0 = relay.rsh, relay.rtq
    relay.rsh = lambda name, cmd, input=None, timeout=300: (seen.append(cmd), (0, "", ""))[1]
    relay.rtq = lambda name, cmd: (0, "")
    try:
        msgs = relay.execute(NAME, [dict(op="drop", key=K, alias=A, out="predictions/Dataset901_ab/tr/fold_0")], S0([]))
        assert not os.path.exists(mine) and os.path.isfile(os.path.join(other, "checkpoint_final.pth")), msgs
        relay.rsh = lambda name, cmd, input=None, timeout=300: (1, "", "boom")
        msgs = relay.execute(NAME, [dict(op="drop", key=K, alias=A, out="predictions/Dataset901_ab/tr/fold_1")], S0([]))
        assert os.path.isfile(os.path.join(other, "checkpoint_final.pth")), msgs
        assert relay.RN.forget("nnUNet/results/Dataset901_ab/tr/fold_0") == []
    finally:
        relay.rsh, relay.rtq = rsh0, rtq0
    shutil.rmtree(os.path.join(top, "Dataset901_ab"))
    ok("推理任务那边删掉后删 box 上的别名 checkpoint；那边没删掉时留着；训练任务的结果路径不碰")


if __name__ == "__main__":
    test_forget()
    test_fresh_site()
    test_plan()
    test_pueue_worker()
    test_slurm_topup_ready()
    test_tilde_root()
    test_adapters_in_relay()
    test_adopt_out()
    test_place()
    test_e2e_path(test_e2e())
    shutil.rmtree(T)
    print("全部通过")
