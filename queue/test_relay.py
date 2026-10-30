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
    f.write('#!/bin/bash\nwhile [[ $1 == -o ]]; do shift 2; done\nshift\ncd / && exec bash -c "$*"\n')
os.chmod(FAKE, 0o755)
BIN = os.path.join(T, "bin")
os.makedirs(BIN)
with open(os.path.join(BIN, "srun"), "w") as f:  # 假的 srun：跳过选项，从 python3 那一项起照跑
    f.write(f"#!{sys.executable}\nimport os,sys\na=sys.argv[1:]\ni=next(i for i,x in enumerate(a) if x.endswith('python3'))\n"
            "os.execv(a[i],a[i:])\n")
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
Q = "dicc"  # 总队列所在的机器（refill.queue_host()）
assert relay.refill.queue_host() == Q


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
                ok={M.hub(): True}, src={n: dict(folds={}) for n in M.names("train")})


def keymap(*pairs):
    return dict(keys={NAME: {k: dict(alias=a, recipe="nnunet", params={}, data=[f"nnUNet/preprocessed/{k.split('/')[0]}"])
                             for k, a in pairs}})


def ops(acts):
    return sorted((a["op"], a["key"]) for a in acts)


def test_plan():
    K, A = "Dataset1_x/tr/0", "Dataset901_ab/tr/0"
    st = keymap((K, A))
    RS = lambda *ts, **kw: dict(taskq=dict(tasks=list(ts), paused=kw.get("paused", False)), folds=kw.get("folds", {}), free=500e9)  # noqa: E731

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
    ok("总队列显示训完 → 交还、删那边的任务和结果")

    acts, _ = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "failed")), st)
    assert ops(acts) == [("drop", K), ("release", K)] and [a for a in acts if a["op"] == "release"][0].get("skip"), acts
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, RS(rtask(A, "failed"), folds={A: dict(final=False, runs=[dict(epochs_done=3)])}), st)
    assert ops(acts) == [("beat", K)] and f"relayfail:{K}" in stuck, acts
    ok("那边失败：没有进度 → 交还（--skip）并删掉；有进度 → 保留、报")

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
    assert ops(acts) == [] and stuck, stuck
    ok("那边有、总队列里不归它的：不让它训；总队列训完了就删；认不出的报")

    acts, stuck = relay.plan(S0([mtask(K)]), NAME, None, dict(st, last_ok={NAME: time.time() - 3 * 3600}))
    assert ops(acts) == [("beat", K)] and f"relaydown:{NAME}" in stuck, stuck
    acts, stuck = relay.plan(S0([mtask(K)]), NAME, None, dict(st, last_ok={NAME: time.time() - 600}))
    assert ops(acts) == [("beat", K)] and not stuck
    ok("连不上：只续总队列的领取，2 小时以上报")

    K2, K3 = "Dataset2_y/tr/0", "Dataset3_z/tr/0"
    cands = [mtask(K2, "free", None, nice=3), mtask(K3, "free", None, nice=4), mtask("Dataset4_c/tr/0", "free", None, relay_=None)]
    relay.refill.data_gb = lambda paths: 10.0
    relay.P.SLURM[NAME] = dict(relay.P.SLURM.get(NAME) or {}, ready=2)
    more, why = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "claimed", "1")), st, [])
    assert [a["key"] for a in more] == [K2, K3], (more, why)
    more, _ = relay.fill(S0([mtask(K)] + cands), NAME, RS(rtask(A, "free")), st, [])
    assert [a["key"] for a in more] == [K2], more
    more, why = relay.fill(S0([mtask(K)] + cands), NAME, dict(RS(rtask(A, "free")), free=105e9), st, [])
    assert more == [] and "放不下" in why, why
    ok("补任务：那边排着的不够 ready 个才领，按档位，只领能翻译的做法，放不下不领")


# ---------------- 2. 挪进数据目录的顺序 ----------------
def test_place():
    src, D = os.path.join(T, "place_src"), os.path.join(T, "place_dst")
    real = dict(out="nnUNet/results/X/tr/fold_0", done=["validation/summary.json", "checkpoint_final.pth|ARCHIVED"])
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
    assert sp["key"] in st["folds"] and st["folds"][sp["key"]]["final"], st
    ok("status.json 里有这一折（真 key）")


if __name__ == "__main__":
    test_plan()
    test_place()
    test_e2e()
    shutil.rmtree(T)
    print("全部通过")
