#!/usr/bin/env python3
"""flow/ledger.py 的测试（2026-11-05）：testdata 的实验表，临时目录里的假数据目录和台账。
adopt 重复跑结果一样；只有 mtime 变 / 大小变 / 不见了三种分级；accept；影子对比的新做完、重跑、改过（只记一次）、做完又不算了。
    python3 queue/test_ledger.py
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
T = tempfile.mkdtemp(prefix="test_ledger_")
os.environ["TASKQ_STATE"] = os.path.join(T, "state")
os.environ["TASKQ_LEDGER"] = os.path.join(T, "state", "ledger.db")
sys.path.insert(0, HERE)
import spec as S  # noqa: E402
from flow import ledger as LG  # noqa: E402
from flow.compile import compile_legacy  # noqa: E402
from flow.graph import Graph  # noqa: E402

fails = []


def ok(cond, what):
    if not cond:
        fails.append(what)


ROOT = os.path.join(T, "data")
F = compile_legacy()
G = Graph(F)
src = F.instances["D133_alpha_s18b/V_save/0"]
ev = F.instances["eval:t18b_f0"]
fin = F.instances["compare_all"]


def write(rel, text):
    p = os.path.join(ROOT, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        f.write(text)
    return p


def make_source(i):
    o = S.outputs(i.spec)
    for item in o["done"]:
        write(os.path.join(o["outs"][0], item[0]), "x")


def rec(i, status="ok"):
    return write(i.marker, json.dumps(dict(status=status, start="2026-11-05T10:00:00")))


try:
    make_source(src)
    rec(ev)
    rec(fin)
    db = LG.connect()
    n = LG.adopt(F, ROOT, db, dry=True)
    ok(n["adopted"] == 3, f"dry-run 数出 3 个做完的：{n}")
    ok(LG.last_attempt(db, ev.id) is None, "dry-run 不写")
    n = LG.adopt(F, ROOT, db)
    ok(n["adopted"] == 3 and LG.adopt(F, ROOT, db)["adopted"] == 0, "adopt 重复跑结果一样")
    ok(LG.done(db, ev, ROOT) == (True, None), "adopt 以后算做完")
    ok(LG.verify(F, ROOT, db) == [], "刚记下时都对得上")

    p = os.path.join(ROOT, ev.marker)
    st = os.stat(p)
    os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + 10 ** 9))
    v = LG.verify(F, ROOT, db)
    ok([(r["id"], r["level"]) for r in v] == [(ev.id, "info")], f"只有 mtime 变：info（{v}）")
    ok(LG.done(db, ev, ROOT)[0], "只有 mtime 变还算做完")
    with open(p, "a") as f:
        f.write(" ")
    ok([r["level"] for r in LG.verify(F, ROOT, db)] == ["warn"], "大小变：warn")
    ok(LG.done(db, ev, ROOT) == (False, "size"), "大小变了不算做完")
    os.remove(p)
    ok([r["level"] for r in LG.verify(F, ROOT, db)] == ["error"], "不见了：error")
    rec(ev)
    ok(LG.accept(F, G, ROOT, db, p, "手动重算") == [ev.id] and LG.verify(F, ROOT, db) == [], "accept 后对得上")
    ok(db.execute("SELECT count(*) FROM event WHERE kind='accept'").fetchone()[0] == 1, "accept 记一笔")
    ok(LG.accept(F, G, ROOT, db, "nowhere/x", "?") == [], "谁的都不是：不记")

    # ---- 影子 ----
    ev1 = F.instances["eval:t18b_f1"]
    rec(ev1)  # 新做完
    s = dict(pueue={"hub": {"tasks": []}}, records=[])
    ident = lambda h, t, recs: next((r for r in recs if r["id"] == t["id"]), None)  # noqa: E731
    n = LG.shadow(F, G, ROOT, db, s=s, hub="hub", identify=ident)
    ok(n["new"] == 1 and not n["unexplained"], f"新做完的记成 legacy：{n}")
    ok(LG.last_attempt(db, ev1.id)["state"] == "legacy", "legacy")
    with open(os.path.join(ROOT, ev1.marker), "a") as f:
        f.write(" ")  # 大小变了，没有调度跑过它
    n = LG.shadow(F, G, ROOT, db, s=s, hub="hub", identify=ident)
    ok(n["changed"] == 1 and len(n["unexplained"]) == 1 and n["unexplained"][0]["level"] == "warn", f"改过：报一次（{n}）")
    n = LG.shadow(F, G, ROOT, db, s=s, hub="hub", identify=ident)
    ok(n["changed"] == 1 and not n["unexplained"], "同样的改动不重复报")
    shadow_dir = os.path.join(T, "state", "shadow")
    lines = [ln for f in os.listdir(shadow_dir) for ln in open(os.path.join(shadow_dir, f))]
    ok(len(lines) == 1, f"说不清的写进 shadow/ledger-日期.jsonl 一行（{len(lines)}）")
    # 调度这期间跑过它：记成新的一次
    s2 = dict(pueue={"hub": {"tasks": [dict(id=7, status="Success", end="2099-01-01T00:00:00+08:00")]}},
              records=[dict(id=7, step="eval", key=ev1.key)])
    n = LG.shadow(F, G, ROOT, db, s=s2, hub="hub", identify=ident)
    ok(n["rerun"] == 1 and LG.done(db, ev1, ROOT) == (True, None), f"调度重跑过：记成新的一次（{n}）")
    # 做完又不算了：标志不见了
    os.remove(os.path.join(ROOT, ev1.marker))
    n = LG.shadow(F, G, ROOT, db, s=s, hub="hub", identify=ident)
    ok(n["regressed"] == 1 and n["unexplained"] and n["unexplained"][0]["level"] == "error", f"标志不见了：error（{n}）")
    # 在重跑（旧的判断说没做完）不报
    rec(ev, status="running")
    s3 = dict(pueue={"hub": {"tasks": [dict(id=8, status="Running")]}}, records=[dict(id=8, step="eval", key=ev.key)])
    n = LG.shadow(F, G, ROOT, db, s=s3, hub="hub", identify=ident)
    ok(not [u for u in n["unexplained"] if u["id"] == ev.id], "在重跑的不报")
    # advance 算过的直接用
    ev2 = F.instances["eval:t18b_f2"]  # 盘上没有，advance 这一轮说做完了：用 advance 的
    LG.shadow(F, G, ROOT, db, s=s, seen={("eval", ev2.key): True}, hub="hub", identify=ident)
    ok((LG.last_attempt(db, ev2.id) or {"state": None})["state"] == "legacy", "用 advance 这一轮的判断")
    # 时间用完：剩下的这轮不查
    n = LG.shadow(F, G, ROOT, db, s=s, hub="hub", identify=ident, budget=-1)
    ok(n["skipped"] == len(F.instances), "超时的这轮不查")
finally:
    shutil.rmtree(T, ignore_errors=True)

print("\n".join(fails) if fails else "台账：都对")
sys.exit(1 if fails else 0)
