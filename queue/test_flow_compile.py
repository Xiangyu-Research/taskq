#!/usr/bin/env python3
"""flow/compile.py、flow/graph.py 的测试（2026-11-05）：用 testdata 的实验表编译出规则和实例，
  1. 实例 id、队列 key 和 advance.py 用的标签、key 一样；
  2. 随机假设哪些实例做完了，按先后算出"该做的"，和照 advance.plan 的判断写的对照一样；
  3. 产出重叠、输入是别的产出的上级、先后有环、输入的产出者不在前面，这几种都报出来；who 按最长前缀。
    python3 queue/test_flow_compile.py
"""
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
sys.path.insert(0, HERE)
import experiments as E  # noqa: E402
import spec as S  # noqa: E402
from flow.compile import compile_legacy  # noqa: E402
from flow.graph import Graph  # noqa: E402
from flow.model import Flow, Instance  # noqa: E402

fails = []


def ok(cond, what):
    if not cond:
        fails.append(what)


F = compile_legacy()
G = Graph(F)
EXPS, FINAL = E.load()
TASKS = E.tasks()

# ---- 1. 身份 ----
ok(not G.check(), f"testdata 的实验表编译出问题：{G.check()}")
want = set(TASKS)
for key, t in TASKS.items():
    e = E.experiment(t["exp"])
    names = E.task_names(t, "{data}", "{python}")
    for st in e.get("after", []):
        iid = f"{st['name']}:{E.label(e, t)}"
        want.add(iid)
        i = F.instances.get(iid)
        ok(i and i.key == key and i.rule == st["name"], f"{iid} 不在或 key 不对")
        if i and st.get("queue"):
            ok(i.qk == S.fill(st["queue"]["key"], names, keep=()), f"{iid} 的队列 key {i.qk}")
for e in EXPS:
    for chain in e.get("group_after", []):
        for st in chain:
            want.add(f"{st['name']}:{e['id']}")
want |= {f["name"] for f in FINAL}
ok(set(F.instances) == want, f"实例多了 {sorted(set(F.instances) - want)[:5]}，少了 {sorted(want - set(F.instances))[:5]}")
ok(all(F.by_key(i.rule, i.key) is i for i in F.instances.values()), "by_key 对不上")


# ---- 2. 先后：随机的"做完了"集合，和照 advance.plan 写的对照比 ----
def oracle(done):
    """advance.plan 的判断（只看做完没有）：每个任务训完后按顺序做第一个没做完的；实验一级的串要所有任务训完、
    needs 的步骤每个任务都做完，按顺序做第一个没做完的；FINAL 没有 rerun_if_newer 的没做完就做。"""
    out, after_ok = set(), {}
    for key, t in TASKS.items():
        e = E.experiment(t["exp"])
        if key not in done:
            continue
        blocked = False
        for st in e.get("after", []):
            iid = f"{st['name']}:{E.label(e, t)}"
            after_ok[(key, st["name"])] = iid in done
            if iid not in done and not blocked:
                out.add(iid)
                blocked = True
    for e in EXPS:
        mine = [k for k, t in TASKS.items() if t["exp"] == e["id"]]
        for chain in e.get("group_after", []):
            if not all(k in done for k in mine):
                continue
            for st in chain:
                need = st.get("needs") or []
                if not all(after_ok.get((k, x)) for k in mine for x in ([need] if isinstance(need, str) else need)):
                    break
                iid = f"{st['name']}:{e['id']}"
                if iid not in done:
                    out.add(iid)
                    break
    for f in FINAL:
        if not f.get("rerun_if_newer") and f["name"] not in done:
            out.add(f["name"])
    return out


def ready(done):
    return {i.id for i in F.instances.values()
            if i.id not in done and F.rules[i.rule].scope != "source" and not F.rules[i.rule].on_change
            and all(d in done for d in i.deps)}


rng = random.Random(5)
ids = list(F.instances)
for n in range(400):
    p = rng.random()
    done = {x for x in ids if rng.random() < p}
    if n % 2:  # 一半按先后"做到某处为止"，更像真的状态
        done = {x for x in G.order() if all(d in done for d in F.instances[x].deps) and rng.random() < 0.9}
    a, b = oracle(done), ready(done)
    if a != b:
        fails.append(f"第 {n} 组：只有对照有 {sorted(a - b)[:4]}，只有先后有 {sorted(b - a)[:4]}")
        break
fin = [f for f in FINAL if f.get("rerun_if_newer")][0]
watch = [x for x in F.instances.values() if x.rule in fin["rerun_if_newer"] and F.rules[x.rule].scope == "gather"]
ok(set(F.instances[fin["name"]].deps) == {x.id for x in watch} and F.rules[fin["name"]].on_change == "rerun", "FINAL 的先后不对")

# ---- 3. 归属、核对 ----
t0 = next(t for t in TASKS.values() if t["exp"] == "t0" and t["each"]["frag"] == "0")
inf = F.instances[f"infer:{E.label(E.experiment('t0'), t0)}"]
lp = F.instances[f"lprob:{E.label(E.experiment('t0'), t0)}"]
ev = F.instances[f"eval:{E.label(E.experiment('t0'), t0)}"]
ok(G.who(inf.outputs[0] + "/G-A/x.prob")[0] == inf.id, "推理目录下的文件归推理")
ok(G.who(lp.outputs[0] + "/a.csv")[0] == lp.id, "评测目录里的 lprob 目录归 lprob（最长前缀）")
ok(G.who(ev.outputs[0] + "/report/x")[0] == ev.id, "评测目录里别的归评测")
ok(G.who("nowhere/x") == (None, None), "谁的都不是")
src = F.instances[t0["key"]]
ok(isinstance(G.who(os.path.join(os.path.dirname(src.outputs[0]), "plans.json"))[0], list), "源头任务上一级的 json 是共用的")
pr = F.instances["prune:t18b"]
ok(pr.mutates and not pr.outputs, f"prune 看整个预测目录：算改别人的产出，不归它（{pr.outputs} {pr.mutates}）")
ok(G.dag("t0") and "infer:t0_f0" in G.dag("t0") and G.dag("nothing") is None, "dag")
ok(G.dag("prune:t18b", dot=True).startswith("digraph"), "dag --dot")


def bad(insts):
    return Graph(Flow(rules={}, instances={i.id: i for i in insts})).check()


ok(any("同时是" in p for p in bad([Instance("a", "r", "a", outputs=["x/y"]), Instance("b", "r", "b", outputs=["x/y"])])), "重叠")
ok(any("上级" in p for p in bad([Instance("a", "r", "a", outputs=["x/y"]), Instance("b", "r", "b", inputs=["x"], deps=["a"])])), "上级")
ok(not any("上级" in p for p in Graph(Flow({}, {i.id: i for i in [Instance("a", "r", "a", outputs=["x/y"]),
                                                                   Instance("b", "w", "b", inputs=["x"], deps=["a"])]})).check(whole={"w"})),
   "input_dir: whole 不报")
ok(any("有环" in p for p in bad([Instance("a", "r", "a", deps=["b"]), Instance("b", "r", "b", deps=["a"])])), "环")
ok(any("不在它前面" in p for p in bad([Instance("a", "r", "a", outputs=["x"]), Instance("b", "r", "b", inputs=["x/f"])])), "输入的产出者不在前面")

print("\n".join(fails) if fails else f"flow 编译：{len(F.instances)} 个实例，都对")
sys.exit(1 if fails else 0)
