#!/usr/bin/env python3
"""feed.py 的测试（2026-10-31）：plan() 用构造的状态，execute() 用假的总队列和缓冲，最后在临时目录里真跑一次缓冲上的 worker
（box 不在线时）。不连任何机器。  python3 scripts/queue/test_feed.py"""
import json
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
T = tempfile.mkdtemp()
os.environ.update(FEED_STATE=os.path.join(T, "feed.json"), FEED_LOG=os.path.join(T, "feed.log"),
                  PHD_TASKS=os.path.join(T, "tasks.json"), PHD_LAST=os.path.join(T, "last"))
sys.path.insert(0, HERE)
import feed as F  # noqa: E402
import machines as M  # noqa: E402

P = F.P
NAME, HUB = "dicc", M.hub()
P.READY[NAME] = 2
fails = 0


def ok(msg, cond):
    global fails
    print(("通过：" if cond else "失败：") + msg)
    fails += not cond


M.MACHINES["lab"].update(worker=dict(group="gpu1", id="lab-gpu1"), slots=1)  # pueue 类的缓冲执行机（10/31 起 lab 本身走 RELAY 的 lab1）

D1, D2, D3 = (f"nnUNet/preprocessed/Dataset10{i}_X" for i in (1, 2, 3))


def ct(key, state="free", nice=50, order=1, assigned=None, data=(D1,), machines=None, after=None, device="gpu", skipped=(), **kw):
    """总队列 / 缓冲里的一行（taskq.py local_status 的样子）。"""
    sp = dict(key=key, env="nnunet", data=list(data), machines=machines, device=device, after=list(after or []), out=f"out/{key}")
    return dict(dict(key=key, nice=nice, order=order, exp=None, recipe="nnunet", params={}, state=state, spec=sp, assigned=assigned,
                     worker=assigned, skipped_by=list(skipped), held=False, failures=0, note=""), **kw)


def bt(key, state="free", worker=None, **kw):
    return dict(ct(key, state, **kw), worker=worker, assigned=None)


def snap(c, b=(), box=(), recs=(), src=None, head="a", paused=False, benabled=True):
    return dict(taskq={HUB: dict(enabled=True, tasks=list(c)),
                       NAME: None if b is None else dict(enabled=benabled, buffer=True, tasks=list(b), paused=paused,
                                                         recent_failures=3 if paused else 0)},
                pueue={HUB: dict(tasks=list(box))}, records=list(recs), src=src or {},
                code=dict(main="a", heads={NAME: head}), ok={HUB: True, NAME: True})


def fold(final=False, epochs=0):
    return dict(final=final, runs=[dict(start="", end="2026-10-31 10:00:00", epochs_done=epochs, tl=[])])


READY = ({D1: (10, 1000), D2: (5, 500), D3: (7, 700)},) * 2


def ops(acts):
    return [(a["op"], a["key"]) for a in acts]


def plan(s, data=READY, st=None, now=None):
    return F.plan(s, NAME, {} if st is None else st, data, now)


K = [f"Dataset10{i}_X/tr/{f}" for i in (1, 2, 3) for f in (0, 1)]

# ---------------- 补训练 ----------------
c = [ct(K[3], nice=50, order=4, data=(D2,)), ct(K[0], nice=50, order=1), ct(K[1], nice=50, order=2), ct(K[2], nice=0, order=3, data=(D2,))]
acts, stuck, _ = plan(snap(c))
ok("按（档位，先后）补到 READY 个", ops(acts) == [("send", K[2]), ("send", K[0])] and not stuck)
acts, _, _ = plan(snap(c + [ct(K[4], "claimed", assigned=NAME)], [bt(K[4])]))
ok("缓冲里已有没人领的：只补差的", [a["key"] for a in acts if a["op"] == "send"] == [K[2]])
acts, _, _ = plan(snap(c + [ct(k, "claimed", assigned=NAME) for k in K[4:]], [bt(K[4]), bt(K[5])]))
ok("缓冲里没人领的够了：不送", not [a for a in acts if a["op"] == "send"])
acts, _, _ = plan(snap(c, [bt(K[4], "claimed", "123"), bt(K[5], "claimed", "124")]))
ok("缓冲里的都在训：照样补满（在训的不算备着的）", [a["key"] for a in acts if a["op"] == "send"] == [K[2], K[0]])
c2 = [ct(K[0], skipped=[NAME]), ct(K[1]), ct(K[2], data=(D2,))]
ok("这台跳过的不送", [a["key"] for a in plan(snap(c2))[0] if a["op"] == "send"] == [K[1], K[2]])
acts, _, _ = plan(snap(c, src={"lab": {"folds": {K[2]: fold(epochs=30)}}}))
ok("别的机器上有进度的不送", [a["key"] for a in acts if a["op"] == "send"] == [K[0], K[1]])
ok("代码不是最新的：不送新任务", not [a for a in plan(snap(c, head="b"))[0] if a["op"] == "send"])
ok("缓冲总开关没开：不送", not [a for a in plan(snap(c, benabled=False))[0] if a["op"] == "send"])
s0 = snap([ct(K[0], "claimed", assigned=NAME)] + c, [])
s0["taskq"][NAME].pop("buffer")
ok("那边还不是缓冲（没有 BUFFER）：什么都不做（不 put 分给它的）", plan(s0)[0] == [])
cpu = [ct("eval/x", device="cpu", machines=None), ct(K[0])]
ok("纯 CPU、没限定机器的任务不当训练送", [a["key"] for a in plan(snap(cpu))[0] if a["op"] == "send"] == [K[0]])

# ---------------- 数据到位没有（拷到一半） ----------------
box_d = {D1: (10, 1000), D2: (5, 500)}
c3 = [ct(K[0]), ct(K[2], data=(D2,), order=2)]
acts, _, _ = plan(snap(c3), (box_d, {D1: (10, 1000)}))
ok("那边没有 D2：送 K0，排一个传 D2 的任务", ops(acts) == [("send", K[0]), ("sync", K[2])] and acts[1]["paths"] == [D2])
acts, _, _ = plan(snap(c3), (box_d, {D1: "inc", D2: (5, 500)}))
ok("拷到一半 1：那边的数据目录有 .incomplete → 不送", ("send", K[2]) in ops(acts) and ("send", K[0]) not in ops(acts))
acts, _, _ = plan(snap(c3), (box_d, {D1: (10, 999), D2: (5, 500)}))
ok("拷到一半 2：文件数对、字节数少（有文件被截断）→ 不送，排传输", ("send", K[0]) not in ops(acts) and ("sync", K[0]) in ops(acts))
acts, _, _ = plan(snap(c3), (dict(box_d, **{D1: "inc"}), {D1: (10, 1000), D2: (5, 500)}))
ok("box 上的数据自己还在生成（.incomplete）：不送、不传", ("send", K[0]) not in ops(acts) and ("sync", K[0]) not in ops(acts))
acts, _, _ = plan(snap(c3), (dict(box_d, **{D1: (0, 0)}), {D2: (5, 500)}))
ok("box 上也没有这份数据（还没生成）：不送、不传", ("send", K[0]) not in ops(acts) and ("sync", K[0]) not in ops(acts))
SY = dict(id=40, label="sync_dicc_Dataset101_X", status="Running", group="wait", command="x")
SR = dict(machine=HUB, id=40, label=SY["label"], step="sync", key=D1, to=NAME, group="wait", created_at=0)
acts, _, _ = plan(snap(c3, box=[SY], recs=[SR]), (box_d, {D1: (10, 1000), D2: (5, 500)}))
ok("那边看着齐了、但传输还在跑：不送这个，别的照送", ("send", K[0]) not in ops(acts) and ("send", K[2]) in ops(acts))
acts, _, _ = plan(snap(c3, box=[SY], recs=[SR]), (box_d, {}))
ok("在传一份时不另排传输（同时只传一份）", not [a for a in acts if a["op"] == "sync"])
ok("数据查不了：不送训练", not plan(snap(c3), (None, {}))[0])
ok("want_paths：只查前 READY+2 个候选的数据", F.want_paths(snap(c3), NAME, {}) == [D1, D2])

# ---------------- 低于 READY 持续 60 分钟才报 ----------------
st = {}
now = time.time()
_, stuck, _ = plan(snap(c3, box=[SY], recs=[SR]), (box_d, {}), st=st, now=now)
ok("补不满：先记下开始时间，不报", "site:dicc:low" not in stuck and st["low_since"][NAME] == now)
_, stuck, _ = plan(snap(c3, box=[SY], recs=[SR]), (box_d, {}), st=st, now=now + 61 * 60)
ok("补不满 61 分钟：报 site:dicc:low", "site:dicc:low" in stuck)
plan(snap(c3), st=st, now=now + 62 * 60)
ok("补满了：清掉", NAME not in st["low_since"])

# ---------------- 分给它的：训完、停止、同步 ----------------
mine = lambda k, state="claimed", **kw: ct(k, state, assigned=NAME, **kw)  # noqa: E731
acts, _, _ = plan(snap([mine(K[0], "done")], [bt(K[0], "done")]))
ok("box 上训完、缓冲里没人领着：先删缓冲记录，再交还", ops(acts)[:2] == [("bremove", K[0]), ("release", K[0])])
FE = dict(id=50, label=f"fetch:{K[0]}", status="Running", group="wait", command="x")
FR = dict(machine=HUB, id=50, label=FE["label"], step="fetch", key=K[0], src=NAME, created_at=0)
acts, _, _ = plan(snap([mine(K[0], "done")], [bt(K[0], "done")], box=[FE], recs=[FR]))
ok("拷到一半 3：box 上看着训完了，但拉回还在跑 → 不交还、不删", not [a for a in acts if a["key"] == K[0] and a["op"] in ("bremove", "release")])
acts, _, _ = plan(snap([mine(K[0], "claimed")], [bt(K[0], "done")]))
ok("拷到一半 4：那边训完、box 上还不算训完（缺 summary.json）→ 不交还", not [a for a in acts if a["op"] in ("bremove", "release")])
INF = "infer/x/fold_0"
acts, _, _ = plan(snap([mine(K[0], "done"), mine(INF, "waiting", machines=[NAME], after=[K[0]])],
                       [bt(K[0], "done"), bt(INF, "free", machines=[NAME], after=[K[0]])]))
ok("训完的折还被缓冲里的推理引用：只交还，不删记录", ("release", K[0]) in ops(acts) and ("bremove", K[0]) not in ops(acts))
acts, _, _ = plan(snap([mine(K[0], "done")], [bt(K[0], "claimed", "123")]))
ok("缓冲里还有作业领着（在收尾）：等", not [a for a in acts if a["op"] in ("bremove", "release")])
acts, _, _ = plan(snap([mine(K[0], "stopped")], [bt(K[0], "claimed", "123")]))
ok("总队列停了：缓冲里也停", ops(acts) == [("bstop", K[0])])
acts, _, _ = plan(snap([mine(K[0], "stopped")], [bt(K[0], "stopped")]))
ok("缓冲里停了、没人领着：删记录、交还", ops(acts) == [("bremove", K[0]), ("release", K[0])])
acts, _, _ = plan(snap([mine(K[0])], []))
ok("分给它的、缓冲里没有（上次送的时候出错）：再送一次", ops(acts)[0] == ("put", K[0]))
acts, _, _ = plan(snap([mine(K[0], nice=-5)], [bt(K[0])]))
ok("档位改了（set-nice）：照总队列改", ops(acts)[0] == ("put", K[0]) and acts[0]["rec"]["nice"] == -5)
acts, _, _ = plan(snap([mine(K[0])], [bt(K[0], "stopped")]))
ok("总队列撤销停止：缓冲里也撤销", ops(acts)[0] == ("bresume", K[0]))
acts, _, _ = plan(snap([mine(K[0])], [bt(K[0], "stopped", held=True)]))
ok("依赖记录后来真分给它了：put hold=false 撤掉停止", ops(acts)[0] == ("put", K[0]) and acts[0]["rec"]["hold"] is False)

# ---------------- 失败 ----------------
acts, stuck, _ = plan(snap([mine(K[0])], [bt(K[0], "failed", failures=2)]))
rel = [a for a in acts if a["op"] == "release"]
ok("缓冲里失败、这台没有进度：删记录、交还（--skip）", ("bremove", K[0]) in ops(acts) and rel and rel[0].get("skip"))
acts, stuck, _ = plan(snap([mine(K[0])], [bt(K[0], "failed", failures=2)], src={NAME: {"folds": {K[0]: fold(epochs=50)}}}))
ok("失败、有进度：留着、报出来", not [a for a in acts if a["op"] == "release"] and f"site:dicc:fail:{K[0]}" in stuck)
acts, stuck, _ = plan(snap([mine(K[0])], [bt(K[0], "failed", failures=2)], paused=True))
ok("缓冲整个暂停：不交还，只报", not [a for a in acts if a["op"] == "release"] and "site:dicc:fail" in stuck)

# ---------------- 缓冲里有、总队列没分给它的 ----------------
acts, stuck, _ = plan(snap([ct(K[0])], [bt(K[0], "claimed", "123")]))
ok("在训、但总队列没分给它：报 orphan，不动", f"site:dicc:orphan:{K[0]}" in stuck and ("bremove", K[0]) not in ops(acts))
acts, _, _ = plan(snap([ct(K[0])], [bt(K[0], "free")]))
ok("没人领、总队列没分给它：删掉", ("bremove", K[0]) in ops(acts))

# ---------------- 只有这台能跑的 ----------------
c4 = [mine(K[0]), ct(INF, "waiting", machines=[NAME], after=[K[0]], nice=-100)]
acts, _, _ = plan(snap(c4 + [ct(k, "claimed", assigned=NAME) for k in K[4:]] + [ct(K[2])], [bt(K[0], "claimed", "123"), bt(K[4]), bt(K[5])]))
ok("推理（machines=[dicc]）、训练在缓冲里：马上送，不受 READY 限制", ("send", INF) in ops(acts) and not [a for a in acts if a["op"] == "send" and a["key"] != INF])
c5 = [ct(K[0], "done"), ct(INF, "free", machines=[NAME], after=[K[0]], nice=-100)]
acts, _, _ = plan(snap(c5, [], src={NAME: {"folds": {K[0]: fold(final=True)}}}))
s5 = [a for a in acts if a["op"] == "send"]
ok("训练在这台训完、已交还：推理带上依赖记录（hold）一起送", s5 and s5[0]["key"] == INF and [d["key"] for d in s5[0]["deps"]] == [K[0]]
   and s5[0]["deps"][0]["hold"] is True)
acts, _, _ = plan(snap(c5, []))
ok("训练不是在这台训的：推理不送", ("send", INF) not in ops(acts))
AR = dict(id=51, label=f"archive:{K[0]}", status="Running", group="wait", command="x")
ARR = dict(machine=HUB, id=51, label=AR["label"], step="archive", key=K[0], created_at=0)
acts, _, _ = plan(snap(c5, [], box=[AR], recs=[ARR], src={NAME: {"folds": {K[0]: fold(final=True)}}}))
ok("拷到一半 5：依赖还在归档（拷）：下游不送", ("send", INF) not in ops(acts))
ok("推理要的环境这台没有：不送", ("send", "infer/y") not in ops(plan(snap([dict(ct("infer/y", machines=[NAME]), spec=dict(ct("infer/y")["spec"], env="nope", machines=[NAME]))], []))[0]))

# ---------------- 连不上 ----------------
acts, stuck, why = plan(snap([mine(K[0])], None), st={"last_ok": {NAME: time.time() - 600}})
ok("缓冲读不到：什么都不动、不交还", acts == [] and not stuck and why == "连不上")
acts, stuck, _ = plan(snap([mine(K[0])], None), st={"last_ok": {NAME: time.time() - 3 * 3600}})
ok("读不到 3 小时：报 site:dicc:down", acts == [] and "site:dicc:down" in stuck)
s = snap([mine(K[0])], [])
s["taskq"][HUB] = None
ok("总队列读不到：什么都不动", plan(s)[0] == [])

# ---------------- 做（假的总队列、缓冲） ----------------
calls = []


def central(rc=0):
    return lambda args: calls.append(("central", args)) or (rc, "", "")


def buf(rc=0, put_rc=None):
    def f(args, inp):
        calls.append(("buf", args, inp))
        if args == "put" and put_rc is not None:
            return (put_rc, "", "put 出错")
        return None if rc is None else (rc, "", "")
    return f


calls.clear()
F.execute(NAME, [dict(op="send", key=K[0], rec={"key": K[0]}, deps=[], nice=0)], central=central(), remote=buf(put_rc=2))
ok("send：分过去后送不进缓冲 → 马上交还", [c[1] for c in calls if c[0] == "central"] == [["take", "--worker", NAME, "--assign", K[0]], ["release", "--worker", NAME, K[0]]])
calls.clear()
F.execute(NAME, [dict(op="send", key=K[0], rec={"key": K[0]}, deps=[{"key": K[1], "hold": True}], nice=0)], central=central(), remote=buf())
ok("send：依赖记录和任务一次 put，依赖在前", json.loads([c for c in calls if c[0] == "buf"][0][2]) == [{"key": K[1], "hold": True}, {"key": K[0]}])
calls.clear()
F.execute(NAME, [dict(op="send", key=K[0], rec={}, deps=[], nice=0)], central=central(1), remote=buf())
ok("send：总队列分不到（别人先拿了）→ 不往缓冲写", not [c for c in calls if c[0] == "buf"])
calls.clear()
F.execute(NAME, [dict(op="bremove", key=K[0]), dict(op="release", key=K[0], why="x")], central=central(), remote=buf(rc=None))
ok("缓冲里删不成（连不上）：总队列先不交还", not [c for c in calls if c[0] == "central"])
calls.clear()
F.execute(NAME, [dict(op="release", key=K[0], why="x", skip="DICC 上失败 2 次")], central=central(), remote=buf())
ok("交还带 --skip", calls == [("central", ["release", "--worker", NAME, K[0], "--skip", "DICC 上失败 2 次"])])

# ---------------- 开关没开：维护照做，新送的只记日志 ----------------
P.AUTO[NAME] = False
done = []
F.execute, orig = (lambda name, acts, **kw: done.extend(acts) or [f"{a['op']} {a['key']}" for a in acts]), F.execute
s = snap([mine(K[1], "done"), ct(K[0])], [bt(K[1], "done")])
F.run_one(s, NAME, st={}, data_run=lambda script, host, timeout: (0, "".join(f"DATA|{D1}|10|1000\n" for _ in [0]) + F.C.HAVE_END, ""))
ok("AUTO 关着：交还照做，送新任务只写进 feed.log", [a["op"] for a in done] == ["bremove", "release"] and "（开关没开）" in open(F.LOG).read())
done.clear()
ev, _ = F.run_one(s, NAME, dry=True, st={}, data_run=lambda script, host, timeout: (0, f"DATA|{D1}|10|1000\n{F.C.HAVE_END}", ""))
ok("dry：只说会做什么，什么都不做", not done and "会" in ev[0])
F.execute = orig


# ---------------- pueue 类执行机（lab）----------------
LAB = "lab"
W = M.MACHINES[LAB]["worker"]
P.READY[LAB] = 2
ok("sites()：DICC 和 lab", set(F.sites()) >= {"dicc", "lab"} and "box" not in F.sites())


def lsnap(c, b=(), lab_tasks=(), recs=(), box=(), src=None):
    return dict(taskq={HUB: dict(enabled=True, tasks=list(c)), LAB: dict(enabled=True, buffer=True, tasks=list(b), paused=False, recent_failures=0)},
                pueue={HUB: dict(tasks=list(box)), LAB: dict(tasks=list(lab_tasks))}, records=list(recs), src=src or {},
                code=dict(main="a", heads={LAB: "a"}), ok={HUB: True, LAB: True})


def wtask(tid, status):
    t = dict(id=tid, label=f"worker:{W['id']}", status=status, group=W["group"], command="x")
    r = dict(machine=LAB, id=tid, label=t["label"], step="worker", key=W["id"], group=W["group"], created_at=0)
    return t, r


def lplan(s, data=READY):
    return F.plan(s, LAB, {}, data)


acts, stuck, _ = lplan(lsnap([]))
ok("lab：worker 组里没有 worker → 开一个", ("worker", W["id"]) in ops(acts) and not stuck)
t, r = wtask(7, "Running")
ok("lab：worker 在跑 → 不另开", ("worker", W["id"]) not in ops(lplan(lsnap([], lab_tasks=[t], recs=[r]))[0]))
t, r = wtask(7, "Success")
ok("lab：上一个 worker 正常退出（代码更新了）→ 重开", ("worker", W["id"]) in ops(lplan(lsnap([], lab_tasks=[t], recs=[r]))[0]))
t, r = wtask(7, "Failed")
acts, stuck, _ = lplan(lsnap([], lab_tasks=[t], recs=[r]))
ok("lab：上一个 worker 失败了 → 不重开，报 site:lab:worker", ("worker", W["id"]) not in ops(acts) and "site:lab:worker" in stuck)
t2 = dict(t, label="worker:lab-gpu1")
ok("lab：同标签、没有任务记录的手动任务不当 worker", ("worker", W["id"]) in ops(lplan(lsnap([], lab_tasks=[t2]))[0]))
s_ = lsnap([])
s_["pueue"][LAB] = {"error": "x"}
ok("lab：pueue 读不到 → 不开 worker", ("worker", W["id"]) not in ops(lplan(s_)[0]))
G0 = f"{LAB}:gpu0"
acts, _, _ = lplan(lsnap([ct(K[1], "done", assigned=G0)]))
rel = [a for a in acts if a["op"] == "release"]
ok("lab:gpu0 手动训的：box 上训完 → 用 lab:gpu0 交还", rel and rel[0]["key"] == K[1] and rel[0]["worker"] == G0)
FE2 = dict(FE, label=f"fetch:{K[1]}")
FR2 = dict(FR, label=FE2["label"], key=K[1], src=LAB)
acts, _, _ = lplan(lsnap([ct(K[1], "done", assigned=G0)], box=[FE2], recs=[FR2]))
ok("lab:gpu0 手动训的：还在拉回 → 不交还", not [a for a in acts if a["op"] == "release"])
acts, _, _ = lplan(lsnap([ct(K[1], "claimed", assigned=G0)]))
ok("lab:gpu0 手动训的：不往缓冲 put", not [a for a in acts if a["key"] == K[1]])
INF2 = "infer/y/fold_0"
acts, _, _ = lplan(lsnap([ct(K[1], "claimed", assigned=G0), ct(INF2, "waiting", machines=[LAB], after=[K[1]], nice=-100)],
                         src={LAB: {"folds": {K[1]: fold(final=True)}}}))
s6 = [a for a in acts if a["op"] == "send"]
ok("0 号卡训完的折的推理：带依赖记录送进 lab 缓冲（1 号卡的 worker 做）", s6 and s6[0]["key"] == INF2 and [d["key"] for d in s6[0]["deps"]] == [K[1]])

added, sent = [], []
F.execute(LAB, [dict(op="worker", key=W["id"]), dict(op="prefetch", key=K[0], paths=[D1], gb=3.0)], central=central(), remote=buf(),
          add=lambda *a, **kw: added.append((a, kw)) or 9, transfer=lambda name, paths: sent.append((name, paths)) or [10])
ok("execute worker：在 worker 组用 q 加 WORKER_ID 的 worker（有任务记录）", added and added[0][0][2] == "worker"
   and f"WORKER_ID={W['id']}" in added[0][0][4] and added[0][0][4].startswith(W["group"] + " ") and added[0][1]["group"] == W["group"])
ok("execute prefetch：refill.transfer（tunnel_push）", sent == [(LAB, [D1])])
calls.clear()
F.execute(LAB, [dict(op="release", key=K[1], worker=G0, why="x")], central=central(), remote=buf())
ok("交还 lab:gpu0 的用它的名字", calls == [("central", ["release", "--worker", G0, K[1]])])

s_ = lsnap([ct(K[0])])
s_["probe"] = {LAB: {"DISK": str(int(45e9))}}
ogb = F.refill.data_gb
F.refill.data_gb = lambda paths: 10.0
st_ = {}
acts, stuck, _ = F.plan(s_, LAB, st_, (box_d, {}))
ok("lab：传完盘剩的不到提醒线 + 10 GB → 不传", not [a for a in acts if a["op"] == "prefetch"])
_, stuck, _ = F.plan(s_, LAB, st_, (box_d, {}), now=time.time() + 61 * 60)
ok("lab：放不下持续 60 分钟 → site:lab:low 里写明盘剩多少", "盘剩 45 GB" in stuck.get("site:lab:low", ""))
s_["probe"][LAB]["DISK"] = str(int(200e9))
ok("lab：放得下 → 传", [a for a in F.plan(s_, LAB, {}, (box_d, {}))[0] if a["op"] == "prefetch"])
F.refill.data_gb = ogb

# ---------------- 送任务只有一个入口（10/31）：feed.run_all 先转给 relay.py，再处理有缓冲的执行机 ----------------
import types  # noqa: E402
rl, rcalls = types.ModuleType("relay"), []
rl.run_all = lambda s, dry=False: (rcalls.append(dry), (["lab1 领了 a/x/0"], {"site:lab1:x": "y"}))[1]
sys.modules["relay"] = rl
P.OFFLINE, off0 = set(M.MACHINES) | set(P.OFFLINE), P.OFFLINE
ev, st = F.run_all(dict(taskq={}, ok={}), dry=False)
ok("relay=True：先跑 relay.run_all，说明和问题并进来，feed.log 里有一行指向 ~/relay.log",
   rcalls == [False] and ev == ["lab1 领了 a/x/0"] and st == {"site:lab1:x": "y"} and "见 ~/relay.log" in open(F.LOG).read())
ev, st = F.run_all(dict(taskq={}, ok={}), dry=False, relay=False)
ok("relay=False（advance 之后的第二遍）：不跑 relay", rcalls == [False] and ev == [] and st == {})
rl.run_all = lambda s, dry=False: 1 / 0
ev, st = F.run_all(dict(taskq={}, ok={}), dry=True)
ok("relay.py 出错：报 relay:error，feed 照常", list(st) == ["relay:error"] and "ZeroDivisionError" in st["relay:error"])
P.OFFLINE = off0
del sys.modules["relay"]
ok("DICC、lab 的环境（conda / venv）没有收集器上报 envs_ok 也算就绪",
   all(F.runnable(n, ct("k", machines=[n])) for n in ("dicc", "lab")) and F.envs_ok(dict(src={}), "dicc") is None)

# ---------------- box 不在线：缓冲上的 worker 照常领、交还 ----------------
BUF, DATA = os.path.join(T, "buffer"), os.path.join(T, "data")
os.makedirs(BUF), os.makedirs(DATA)
for f in ("BUFFER", "ENABLED"):
    open(os.path.join(BUF, f), "w").close()
ENV = dict(os.environ, TASKQ_DIR=BUF, BRAIN_TUMOR=DATA, TASKQ_MACHINE=NAME)
ENV.pop("nnUNet_results", None)


def tqb(*args, feed=False, stdin=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, **({"TASKQ_FEED": "1"} if feed else {})),
                       capture_output=True, text=True, input=stdin)
    return r.returncode, r.stdout.strip()


recs = [dict(order=i, nice=0, key=f"toy/{i}", exp=None, recipe="command",
             params={"key": f"toy/{i}", "cmd": "true", "env": "nnunet", "out": f"toy/{i}", "done": '["DONE"]'}) for i in range(2)]
rc, out = tqb("put", feed=True, stdin=json.dumps(recs))
got = []
for w in ("900", "901", "902"):
    rc, out = tqb("next", "--worker", w, "--envs", "nnunet")
    got.append((rc, out.splitlines()[0] if rc == 0 else ""))
ok("box 不在线（缓冲目录里没有总队列）：worker 按顺序领完，再领返回 10", [g[0] for g in got] == [0, 0, 10] and got[0][1] == "toy/0")
rc, _ = tqb("release", "--worker", "900", "toy/0")
rc2, out2 = tqb("next", "--worker", "903", "--envs", "nnunet")
ok("交还后别的 worker 接着领", rc == 0 and rc2 == 0 and out2.startswith("toy/0"))

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
