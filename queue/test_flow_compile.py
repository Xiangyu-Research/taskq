#!/usr/bin/env python3
"""flow/compile.py、flow/graph.py 的测试：用 testdata 的实验表编译出实例和先后，
  1. 实例 id：源头 = 队列 key，每个任务的步骤 <步骤>/<实验>_<简称>（only 筛过），实验一级的 <步骤>/<实验>，FINAL 的 <步骤>；
  2. 先后：同一个任务里按顺序；输入落在别的实例的产出里就接上（跨实验、通配、实验一级按 each 展开都算）；
     不在任何产出里的是原始数据，训练共用的 fetch.extra 不算；
  3. 拓扑序、上下游、who（最长前缀）、dag；总队列里不属于实验表的任务也算源头；
  4. 核对：模板里不认识的名字、产出带通配、产出重叠、输入落在自己的产出里、有环。
    python3 queue/test_flow_compile.py
"""
import copy
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
from flow import compile as FC  # noqa: E402
from flow.graph import Graph  # noqa: E402
from flow.model import EACH, GATHER, GLOBAL, SOURCE, Instance  # noqa: E402

fails = []


def ok(cond, what):
    if not cond:
        fails.append(what)
    else:
        print(f"通过：{what}")


F = FC.build()
I = F.instances
G = Graph(F)
EXPS, FINAL = E.load()
TASKS = E.tasks()

# ---- 1. 实例 id ----
ok(not F.errors, f"testdata 的实验表编译出问题：{F.errors}")
want = {}
for key, t in TASKS.items():
    e = E.experiment(t["exp"])
    want[key] = SOURCE
    for st in E.steps_of(t, e):
        want[f"{st['name']}/{E.label(e, t)}"] = EACH
for e in EXPS:
    for st in e["group_after"]:
        want[f"{st['name']}/{e['id']}"] = GATHER
for st in FINAL:
    want[st["name"]] = GLOBAL
ok({k: i.scope for k, i in I.items()} == want, f"实例多了 {sorted(set(I) - set(want))[:5]}，少了 {sorted(set(want) - set(I))[:5]}")
ok(all(i.key == i.id for i in I.values()), "实例 id 就是进总队列的 key")
eds = sorted(i.id for i in I.values() if i.rule == "edit")
ok(eds == ["edit/arch_s_f0"], f"only={{frag: [0]}} 的步骤只该有 frag 0 的实例：{eds}")
try:
    E.check_fields("x", type("M", (), dict(EXPERIMENTS=[dict(id="x", recipe="toy", each=dict(frag=["0"]),
                                                              after=[dict(EXPS[0]["after"][0], only=dict(seed=["1"]))])])))
    ok(False, "only 写了 each 里没有的参数应该报错")
except S.SpecError:
    pass
t0 = next(t for t in TASKS.values() if t["exp"] == "t0" and t["each"]["frag"] == "0")
src = I[t0["key"]]
inf, ev, lp = I["infer/t0_f0"], I["eval/t0_f0"], I["lprob/t0_f0"]
ok(src.outputs == [os.path.normpath(o) for o in S.outputs(t0["spec"])["outs"]] and src.spec is t0["spec"], "源头的产出是 spec.outputs 的")
ok(inf.outputs == ["preds/" + t0["spec"]["names"]["model"] + "/frag_0"], f"步骤的产出填好模板：{inf.outputs}")
ok(inf.task == t0["key"] and inf.lab == "t0_f0", "每个任务的步骤记下所属的任务")

# ---- 2. 先后 ----
ok(inf.deps == [t0["key"]], f"推理接在训练后面：{inf.deps}")
ok(ev.deps == [t0["key"], "infer/t0_f0"], f"评测接在训练、推理后面（同一个任务里按顺序）：{ev.deps}")
ok(set(lp.deps) == {t0["key"], "infer/t0_f0", "eval/t0_f0"}, f"lprob：顺序 + 输入（评测的 report）：{lp.deps}")
ens = I["ens/t0"]
ok(ens.deps == [f"infer/t0_f{k}" for k in range(5)], f"实验一级的输入按 each 的每个取值展开，接在每份的推理后面：{ens.deps}")
ok(len([p for p in ens.inputs if p.startswith("preds/")]) == 5, "ens 的输入展开成 5 份")
ok(set(I["paired/t3b"].deps) == {"ens/t0", "oof/t3b"}, f"跨实验：paired/t3b 接在 ens/t0 后面：{I['paired/t3b'].deps}")
ok(set(I["t18cmp/t18b"].deps) == {"ens/t18", "ens/t18b"}, f"跨实验：t18cmp：{I['t18cmp/t18b'].deps}")
ca = I["compare_all"]
want_ca = {i.id for i in I.values() if i.rule in ("paired", "ens")}
ok(set(ca.deps) == want_ca, f"FINAL 的通配输入接上所有匹配的产出：多了 {set(ca.deps) - want_ca}，少了 {want_ca - set(ca.deps)}")
ok(set(I["p2cmp/t3b"].deps) == {"p2/t3b"}, f"p2cmp 接在同一实验的 p2 后面：{I['p2cmp/t3b'].deps}")
ok(set(I["oof/t3b"].deps) >= {f"eval/t3b_f{k}" for k in range(5)}, "oof 接在每份的评测后面")
raw = sorted({p for i in I.values() for p in i.raw})
ok("raw/holdout/A/inputs" in inf.raw and "raw/labels/*/*-lab.dat" in ev.raw, f"原始数据：{inf.raw} {ev.raw}")
ok(not any(p.startswith(("results/", "preds/", "demo/analysis/")) for p in raw), f"产出、训练共用的 json 不算原始数据：{raw}")
ok(src.shared and all(p.endswith("*.json") for p in src.shared), f"训练的 fetch.extra 记为共用：{src.shared}")

# 拓扑序：每个实例都在它的先后后面；上下游互为反向
pos = {n: k for k, n in enumerate(G.order())}
ok(len(pos) == len(I) and all(pos[d] < pos[n] for n, i in I.items() for d in i.deps), "拓扑序")
rng = random.Random(5)
for n in rng.sample(list(I), 40):
    ok(all(n in G.descendants(a) for a in G.ancestors(n)), f"{n} 的上游的下游里没有它")
ok(G.ancestors("compare_all") >= {t0["key"], "infer/t0_f0", "ens/t0", "paired/t3b", "oof/t3b"}, "compare_all 的上游")
ok("eval/t0_f0" in G.descendants("infer/t0_f0") and "compare_all" in G.descendants(t0["key"]), "下游")
ok(G.direct("eval/t0_f0") == ["infer/t0_f0"], f"direct 去掉能推出来的：{G.direct('eval/t0_f0')}")

# ---- 3. 归属、dag、总队列里的任务 ----
ok(G.who(inf.outputs[0] + "/G-A/x.prob")[0] == inf.id, "推理目录下的文件归推理")
ok(G.who(lp.outputs[0] + "/a.csv")[0] == lp.id, "lprob 的目录归 lprob")
ok(G.who(ev.outputs[0] + "/report/x")[0] == ev.id, "评测目录下的归评测")
ok(G.who("nowhere/x") == (None, None), "谁的都不是")
ok(G.who(src.outputs[0] + "/final.ckpt")[0] == src.id, "训练的产出归源头")
sh = G.who(os.path.join(os.path.dirname(src.outputs[0]), "*.json"))
ok(isinstance(sh[0], list) and t0["key"] in sh[0], f"源头任务上一级的 json 是共用的：{sh}")
ok(G.dag("t0") and "infer/t0_f0 → eval/t0_f0" in G.dag("t0") and G.dag("nothing") is None, "dag")
ok(G.dag("paired/t3b", dot=True).startswith("digraph") and '"ens/t0" -> "paired/t3b"' in G.dag("paired/t3b", dot=True), "dag --dot")
q = [dict(key="adhoc/x", recipe="toy", params=dict(ds="D101_alpha", variant="V_q", frag="0")),
     dict(key="infer/t0_f0", recipe="command", spec=dict(step="infer")),  # 步骤进总队列的任务：不是源头
     dict(key="eval/t0_f0", recipe="command", params=dict(key="eval/t0_f0", step="eval", cmd="c", env="base", out="o", done="d")),  # 没带说明的也按解析出的说明认
     dict(key=t0["key"], recipe="toy")]                                     # 实验表里有的：照实验表
F2 = FC.build(queue_tasks=q)
ok(F2.instances["adhoc/x"].scope == SOURCE and F2.instances["infer/t0_f0"].scope == EACH and F2.instances["eval/t0_f0"].scope == EACH
   and not F2.errors,
   f"总队列里不属于实验表的训练任务也算源头：{F2.errors}")

def step_of(exps, eid, name):
    e = next(x for x in exps if x["id"] == eid)
    return next(st for st in e["after"] + e["group_after"] if st["name"] == name)


ex3 = copy.deepcopy(EXPS)
step_of(ex3, "t0", "infer").update(env=["e_{frag}"], done=["d_{frag}.json"])
i3 = FC.build(ex3, copy.deepcopy(FINAL)).instances["infer/t0_f0"]
ok(i3.step["env"] == ["e_0"] and i3.step["done"] == ["d_0.json"] and step_of(ex3, "t0", "infer")["env"] == ["e_{frag}"],
   f"步骤的 env、done 和 cmd、out 一样按实例的参数展开，实验表里的不动：{i3.step['env']} {i3.step['done']}")

# ---- 4. 核对 ----
def compiled(edit):
    exps, final = copy.deepcopy(EXPS), copy.deepcopy(FINAL)
    edit(exps, final)
    return FC.build(exps, final).errors



def tpl(exps, final):
    step_of(exps, "t0", "eval")["out"] = ["demo/{nosuch}/x"]


ok(any("nosuch" in p for p in compiled(tpl)), "模板里不认识的名字")


def magic(exps, final):
    step_of(exps, "t0", "eval")["out"] = ["demo/analysis/*/x"]


ok(any("带通配" in p for p in compiled(magic)), "产出带通配")


def same(exps, final):
    step_of(exps, "t0", "lprob")["out"] = [step_of(exps, "t0", "eval")["out"][0]]


ok(any("同时是" in p for p in compiled(same)), "两个实例的产出相同")


def nested(exps, final):
    step_of(exps, "t0", "lprob")["out"] = [step_of(exps, "t0", "eval")["out"][0] + "/sub"]


ok(any("在 eval/t0_f0 的产出" in p for p in compiled(nested)), "产出在别的产出里面")


def own(exps, final):
    st = step_of(exps, "t0", "eval")
    st["inputs"] = st["inputs"] + [st["out"][0] + "/report"]


ok(any("落在自己的产出" in p for p in compiled(own)), "输入落在自己的产出里")


def loop(exps, final):
    step_of(exps, "t3b", "oof")["inputs"] += [step_of(exps, "t3b", "paired")["out"][0]]


ok(any("先后有环" in p for p in compiled(loop)), "有环")
ok(FC.cycles({"a": Instance("a", EACH, "r", deps=["b"]), "b": Instance("b", EACH, "r", deps=["a"])}) == ["先后有环：a → b → a"],
   "cycles：每个环报一次")

print("\n".join(fails) if fails else f"flow 编译：{len(I)} 个实例，都对")
sys.exit(1 if fails else 0)
