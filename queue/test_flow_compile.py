#!/usr/bin/env python3
"""Tests for flow/compile.py and flow/graph.py: compile instances and order from testdata's experiment table.
   1. instance ids: source = queue key; per-task steps <step>/<experiment>_<short> (filtered by only); experiment-level
      <step>/<experiment>; FINAL ones <step>;
   2. order: within a task in order; an input in another instance's output is ordered after it (across experiments, wildcards and
      experiment-level each expansion all count); inputs in no output are raw data, training's shared fetch.extra is not;
   3. topological order, upstream/downstream, who (longest prefix), dag; tasks in the hub queue outside the experiment table are sources too;
   4. checks: unknown template names, wildcard outputs, overlapping outputs, an input inside its own outputs, cycles.
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
        print(f"PASS:{what}")


F = FC.build()
inst = F.instances
G = Graph(F)
EXPS, FINAL = E.load()
TASKS = E.tasks()

# ---- 1. instance ids ----
ok(not F.errors, f"testdata's experiment table has compile errors: {F.errors}")
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
ok({k: i.scope for k, i in inst.items()} == want, f"extra instances {sorted(set(inst) - set(want))[:5]}, missing {sorted(set(want) - set(inst))[:5]}")
ok(all(i.key == i.id for i in inst.values()), "the instance id is the hub queue key")
eds = sorted(i.id for i in inst.values() if i.rule == "edit")
ok(eds == ["edit/arch_s_f0"], f"a step with only={{frag: [0]}} must have only frag 0 instances: {eds}")
try:
    E.check_fields("x", type("M", (), dict(EXPERIMENTS=[dict(id="x", recipe="toy", each=dict(frag=["0"]),
                                                              after=[dict(EXPS[0]["after"][0], only=dict(seed=["1"]))])])))
    ok(False, "only naming a param absent from each should be an error")
except S.SpecError:
    pass
t0 = next(t for t in TASKS.values() if t["exp"] == "t0" and t["each"]["frag"] == "0")
src = inst[t0["key"]]
inf, ev, lp = inst["infer/t0_f0"], inst["eval/t0_f0"], inst["lprob/t0_f0"]
ok(src.outputs == [os.path.normpath(o) for o in S.outputs(t0["spec"])["outs"]] and src.spec is t0["spec"], "source outputs come from spec.outputs")
ok(inf.outputs == ["preds/" + t0["spec"]["names"]["model"] + "/frag_0"], f"step outputs have templates filled: {inf.outputs}")
ok(inf.task == t0["key"] and inf.lab == "t0_f0", "per-task steps record their task")

# ---- 2. order ----
ok(inf.deps == [t0["key"]], f"inference follows training: {inf.deps}")
ok(ev.deps == [t0["key"], "infer/t0_f0"], f"eval follows training and inference (in order within the task): {ev.deps}")
ok(set(lp.deps) == {t0["key"], "infer/t0_f0", "eval/t0_f0"}, f"lprob: order + inputs (eval's report): {lp.deps}")
ens = inst["ens/t0"]
ok(ens.deps == [f"infer/t0_f{k}" for k in range(5)], f"experiment-level inputs expand over each value and follow each part's inference: {ens.deps}")
ok(len([p for p in ens.inputs if p.startswith("preds/")]) == 5, "ens inputs expand to 5 parts")
ok(set(inst["paired/t3b"].deps) == {"ens/t0", "oof/t3b"}, f"across experiments: paired/t3b follows ens/t0: {inst['paired/t3b'].deps}")
ok(set(inst["t18cmp/t18b"].deps) == {"ens/t18", "ens/t18b"}, f"across experiments: t18cmp: {inst['t18cmp/t18b'].deps}")
ca = inst["compare_all"]
want_ca = {i.id for i in inst.values() if i.rule in ("paired", "ens")}
ok(set(ca.deps) == want_ca, f"FINAL's wildcard input follows every matching output: extra {set(ca.deps) - want_ca}, missing {want_ca - set(ca.deps)}")
ok(set(inst["p2cmp/t3b"].deps) == {"p2/t3b"}, f"p2cmp follows p2 in the same experiment: {inst['p2cmp/t3b'].deps}")
ok(set(inst["oof/t3b"].deps) >= {f"eval/t3b_f{k}" for k in range(5)}, "oof follows each part's eval")
raw = sorted({p for i in inst.values() for p in i.raw})
ok("raw/holdout/A/inputs" in inf.raw and "raw/labels/*/*-lab.dat" in ev.raw, f"raw data: {inf.raw} {ev.raw}")
ok(not any(p.startswith(("results/", "preds/", "demo/analysis/")) for p in raw), f"outputs and training-shared json are not raw data: {raw}")
ok(src.shared and all(p.endswith("*.json") for p in src.shared), f"training's fetch.extra is shared: {src.shared}")

# topological order: every instance comes after its deps; upstream and downstream are inverses
pos = {n: k for k, n in enumerate(G.order())}
ok(len(pos) == len(inst) and all(pos[d] < pos[n] for n, i in inst.items() for d in i.deps), "topological order")
rng = random.Random(5)
for n in rng.sample(list(inst), 40):
    ok(all(n in G.descendants(a) for a in G.ancestors(n)), f"{n} is missing from its ancestors' descendants")
ok(G.ancestors("compare_all") >= {t0["key"], "infer/t0_f0", "ens/t0", "paired/t3b", "oof/t3b"}, "compare_all's upstream")
ok("eval/t0_f0" in G.descendants("infer/t0_f0") and "compare_all" in G.descendants(t0["key"]), "downstream")
ok(G.direct("eval/t0_f0") == ["infer/t0_f0"], f"direct drops implied edges: {G.direct('eval/t0_f0')}")

# ---- 3. ownership, dag, tasks in the hub queue ----
ok(G.who(inf.outputs[0] + "/G-A/x.prob")[0] == inf.id, "files under the inference dir belong to inference")
ok(G.who(lp.outputs[0] + "/a.csv")[0] == lp.id, "lprob's dir belongs to lprob")
ok(G.who(ev.outputs[0] + "/report/x")[0] == ev.id, "files under the eval dir belong to eval")
ok(G.who("nowhere/x") == (None, None), "nobody's")
ok(G.who(src.outputs[0] + "/final.ckpt")[0] == src.id, "training outputs belong to the source")
sh = G.who(os.path.join(os.path.dirname(src.outputs[0]), "*.json"))
ok(isinstance(sh[0], list) and t0["key"] in sh[0], f"json in the source task's parent is shared: {sh}")
ok(G.dag("t0") and "infer/t0_f0 -> eval/t0_f0" in G.dag("t0") and G.dag("nothing") is None, "dag")
ok(G.dag("paired/t3b", dot=True).startswith("digraph") and '"ens/t0" -> "paired/t3b"' in G.dag("paired/t3b", dot=True), "dag --dot")
q = [dict(key="adhoc/x", recipe="toy", params=dict(ds="D101_alpha", variant="V_q", frag="0")),
     dict(key="infer/t0_f0", recipe="command", spec=dict(step="infer")),  # a step queued in the hub queue: not a source
     dict(key="eval/t0_f0", recipe="command", params=dict(key="eval/t0_f0", step="eval", cmd="c", env="base", out="o", done="d")),  # without a spec: resolved from the recipe
     dict(key=t0["key"], recipe="toy")]                                     # present in the experiment table: follow the table
F2 = FC.build(queue_tasks=q)
ok(F2.instances["adhoc/x"].scope == SOURCE and F2.instances["infer/t0_f0"].scope == EACH and F2.instances["eval/t0_f0"].scope == EACH
   and not F2.errors,
   f"training tasks in the hub queue outside the experiment table are sources too: {F2.errors}")

def step_of(exps, eid, name):
    e = next(x for x in exps if x["id"] == eid)
    return next(st for st in e["after"] + e["group_after"] if st["name"] == name)


ex3 = copy.deepcopy(EXPS)
step_of(ex3, "t0", "infer").update(env=["e_{frag}"], done=["d_{frag}.json"])
i3 = FC.build(ex3, copy.deepcopy(FINAL)).instances["infer/t0_f0"]
ok(i3.step["env"] == ["e_0"] and i3.step["done"] == ["d_0.json"] and step_of(ex3, "t0", "infer")["env"] == ["e_{frag}"],
   f"step env and done expand per instance like cmd and out; the experiment table is untouched: {i3.step['env']} {i3.step['done']}")

# ---- 4. checks ----
def compiled(edit):
    exps, final = copy.deepcopy(EXPS), copy.deepcopy(FINAL)
    edit(exps, final)
    return FC.build(exps, final).errors



def tpl(exps, final):
    step_of(exps, "t0", "eval")["out"] = ["demo/{nosuch}/x"]


ok(any("nosuch" in p for p in compiled(tpl)), "unknown template names")


def magic(exps, final):
    step_of(exps, "t0", "eval")["out"] = ["demo/analysis/*/x"]


ok(any("has a wildcard" in p for p in compiled(magic)), "wildcard outputs")


def same(exps, final):
    step_of(exps, "t0", "lprob")["out"] = [step_of(exps, "t0", "eval")["out"][0]]


ok(any("is an output of both" in p for p in compiled(same)), "two instances with the same output")


def nested(exps, final):
    step_of(exps, "t0", "lprob")["out"] = [step_of(exps, "t0", "eval")["out"][0] + "/sub"]


ok(any("inside output" in p and "eval/t0_f0" in p for p in compiled(nested)), "an output inside another output")


def own(exps, final):
    st = step_of(exps, "t0", "eval")
    st["inputs"] = st["inputs"] + [st["out"][0] + "/report"]


ok(any("falls in its own output" in p for p in compiled(own)), "an input inside its own output")


def loop(exps, final):
    step_of(exps, "t3b", "oof")["inputs"] += [step_of(exps, "t3b", "paired")["out"][0]]


ok(any("cycle in the order" in p for p in compiled(loop)), "a cycle")
ok(FC.cycles({"a": Instance("a", EACH, "r", deps=["b"]), "b": Instance("b", EACH, "r", deps=["a"])}) == ["cycle in the order: a -> b -> a"],
   "cycles: each cycle once")

print("\n".join(fails) if fails else f"flow compile: {len(inst)} instances, all fine")
sys.exit(1 if fails else 0)
