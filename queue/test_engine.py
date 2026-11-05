#!/usr/bin/env python3
"""flow/engine.py 的测试（2026-11-05）：用 testdata 的实验表，
  1. 反事实回放：随机假设做完了哪些、box 上在跑什么、总队列分给了谁，advance.plan 和 engine.plan 的结果一样；
  2. ENGINE=shadow 时照 advance 排，不一样的写进 shadow/engine-<日期>.jsonl；ENGINE=dag 时用引擎的；写错了报出来。
    python3 queue/test_engine.py
"""
import json
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
sys.path.insert(0, HERE)
T = tempfile.mkdtemp(prefix="test_engine_")
os.environ["TASKQ_STATE"] = os.environ["ADVANCE_BT"] = T
os.environ["ADVANCE_LOG"] = os.path.join(T, "advance.log")
import advance as A  # noqa: E402
import policy as P  # noqa: E402
import mtab as M  # noqa: E402
from flow import engine as G  # noqa: E402

fails = []


def ok(cond, what):
    if not cond:
        fails.append(what)


try:
    # ---- 1. 反事实回放 ----
    d = G.replay(n=150, seed=1)
    ok(not d, f"回放有 {len(d)} 处不一样，如 {d[:3]}")

    # 回放真的排出了东西（不是全空）
    cnt = []
    orig = A.plan
    A.plan = lambda *a, **k: (lambda r: cnt.append(len(r[0])) or r)(orig(*a, **k))
    G.replay(n=20, seed=2)
    A.plan = orig
    ok(sum(cnt) > 20, f"回放几乎没排出东西：{cnt}")

    # ---- 2. 开关 ----
    hub = M.hub()
    s = dict(pueue={hub: dict(tasks=[])}, taskq={hub: dict(tasks=[])}, records=[])
    old = getattr(P, "ENGINE", None)
    try:
        P.ENGINE = "shadow"
        real = G.plan
        G.plan = lambda s_, **k: ([dict(step="x", key="k", group="cpu", label="x:k", cmd="true", nice=0)], {})
        A.run(s, q=lambda *a: 1, tq=lambda *a: (0, ""), rm=lambda *a: (0, ""))
        G.plan = real
        files = os.listdir(os.path.join(T, "shadow")) if os.path.isdir(os.path.join(T, "shadow")) else []
        ok(any(f.startswith("engine-") for f in files), f"影子不一样时没写 shadow/engine-*.jsonl：{files}")
        if files:
            rows = [json.loads(x) for x in open(os.path.join(T, "shadow", files[0]))]
            ok(any(r["only"] == "engine" for r in rows), f"影子记录不对：{rows}")
        ok(A.SHADOW and A.SHADOW[1] >= 1, f"SHADOW 没记不一样的个数：{A.SHADOW}")
        P.ENGINE = "dag"
        seen = []
        G.plan = lambda s_, **k: (seen.append(1), ([], {}))[1]
        A.run(s, dry=True)
        G.plan = real
        ok(seen, "ENGINE=dag 时没用引擎")
        P.ENGINE = "bogus"
        try:
            G.mode()
            ok(False, "ENGINE 写错了没报")
        except ValueError:
            pass
    finally:
        if old is None:
            P.__dict__.pop("ENGINE", None)
        else:
            P.ENGINE = old
finally:
    shutil.rmtree(T, ignore_errors=True)

if fails:
    print("失败：\n  " + "\n  ".join(fails))
    sys.exit(1)
print("引擎：回放和开关都对")
