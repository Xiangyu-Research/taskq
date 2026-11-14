"""Experiment table (EXPERIMENTS and FINAL read by experiments.py) -> instances and order.
There are only two ordering rules: within a task in order (source -> after's first step -> second ...); an input (wildcards
included) that falls in another instance's output (equal, inside it, or containing it) runs after that instance, whichever
experiment it belongs to. An input in no output is raw data (raw).
Checks (errors): unknown template names, wildcard outputs, overlapping outputs of two instances, an input inside its own outputs, cycles."""
import fnmatch
import glob as G
import itertools
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common as C  # noqa: E402
import experiments as E  # noqa: E402
import spec as S  # noqa: E402
from flow.model import EACH, GATHER, GLOBAL, SOURCE, Flow, Instance  # noqa: E402


def norm(p):
    return os.path.normpath(str(p).strip().strip("/"))


def expand_inputs(paths, names, each=()):
    """Input templates -> paths relative to the data dir; at experiment level, a {<param>} (an each param missing from names) expands over every value in <param>_list."""
    out = []
    for p in paths:
        ks = [k for k in each if "{" + k + "}" in p and k not in names]
        if ks:
            out += [S.fill(p, dict(names, **dict(zip(ks, vs)))) for vs in itertools.product(*(names.get(f"{k}_list", "").split() for k in ks))]
        else:
            out.append(S.fill(p, names))
    return [norm(p) for p in out]


class Outs:
    """All instances' outputs in a tree by path component: match(input) -> instances whose outputs fall in this input or contain it."""
    END = "\0"

    def __init__(self):
        self.root = {}

    def add(self, path, iid):
        n = self.root
        for c in path.split("/"):
            n = n.setdefault(c, {})
        n.setdefault(self.END, []).append(iid)

    def match(self, pat):
        out, parts = set(), pat.split("/")

        def everything(n):
            for k, v in n.items():
                if k == self.END:
                    out.update(v)
                else:
                    everything(v)

        def walk(n, i):
            if self.END in n:  # output ends here: the input is inside it
                out.update(n[self.END])
            if i == len(parts):  # input ends here: outputs are inside it
                everything(n)
                return
            c = parts[i]
            for k, v in n.items():  # a wildcard input component matches tree components; a wildcard tree component (fetch.extra's *.json) matches the other way round
                if k != self.END and (fnmatch.fnmatchcase(k, c) if G.has_magic(c) else k == c or G.has_magic(k) and fnmatch.fnmatchcase(c, k)):
                    walk(v, i + 1)
        walk(self.root, 0)
        return out


def build(exps=None, final=None, queue_tasks=()):
    """-> Flow. queue_tasks: tasks in the hub queue (taskq status --json tasks); those with a recipe and not in the experiment table are sources too."""
    if exps is None:
        exps, final = E.load()
    inst, errors, adhoc = {}, [], set()

    def add(i):
        if i.id in inst:
            errors.append(f"duplicate instance id: {i.id}")
        inst[i.id] = i
        return i

    def source(key, exp, recipe, sp, lab):
        o = S.outputs(sp)
        shared = [norm(os.path.join(os.path.dirname(o["outs"][0]), x)) for x in o["extra"]]
        return add(Instance(id=key, scope=SOURCE, rule=recipe, exp=exp or "", names=dict(sp.get("names") or {}),
                            outputs=[norm(x) for x in o["outs"]], shared=shared, spec=sp, lab=lab))

    def step(scope, st, iid, exp, names, deps=(), each=(), **kw):
        try:
            outs = [norm(S.fill(x, names)) for x in st["out"]]
            ins = expand_inputs(st["inputs"], names, each)
            st = dict(st, **{k: [S.fill(x, names) for x in st[k]] for k in ("env", "done")})  # templates like cmd, out and inputs
        except S.SpecError as e:
            errors.append(f"{iid}: {e}")
            return None
        for o in outs:
            if G.has_magic(o):
                errors.append(f"output {o} of {iid} has a wildcard (outputs must be concrete paths)")
        return add(Instance(id=iid, scope=scope, rule=st["name"], exp=exp, names=names, deps=list(deps), outputs=outs,
                            inputs=ins, step=st, **kw))

    ids = {e["id"] for e in exps}
    table = {k: t for k, t in E.tasks().items() if t["exp"] in ids}
    for key, t in table.items():
        source(key, t["exp"], t["recipe"], t["spec"], E.label(E.experiment(t["exp"]), t))
    for q in queue_tasks:
        if q["key"] in table or q["key"] in inst or not q.get("recipe") or S.P.is_smoke(q["key"]):
            continue
        try:
            sp = q.get("spec") or E.resolve_task(q["recipe"], q.get("params") or {}, q.get("exp"))
        except S.SpecError:
            continue
        if not sp.get("step"):  # step instances are derived from the experiment table below
            adhoc.add(source(q["key"], None, q["recipe"], sp, q["key"]).id)

    for e in exps:
        for t in (t for t in table.values() if t["exp"] == e["id"]):
            names, lab, prev = E.task_names(t), E.label(e, t), [t["key"]]
            for st in E.steps_of(t, e):
                i = step(EACH, st, f"{st['name']}/{lab}", e["id"], names, deps=prev, task=t["key"], lab=lab)
                if i:
                    prev = prev + [i.id]
        if e.get("group_after"):
            names = E.group_names(e)
            for st in e["group_after"]:
                step(GATHER, st, f"{st['name']}/{e['id']}", e["id"], names, each=e.get("each") or {}, lab=e["id"])
    for st in final or []:
        step(GLOBAL, st, st["name"], "", {}, lab=st["name"])

    outs, shared = Outs(), Outs()
    for i in inst.values():
        for o in i.outputs:
            outs.add(o, i.id)
        for o in i.shared:
            shared.add(o, i.id)
    errors += overlaps(inst, adhoc)
    for i in inst.values():
        for p in i.inputs:
            own = [o for o in i.outputs if C.under(p, o) or C.under(o, p)]
            if own:
                errors.append(f"input {p} of {i.id} falls in its own output {own[0]} (inputs and outputs must not overlap)")
            src = outs.match(p) - {i.id}
            if src:
                i.deps += [d for d in inst if d in src and d not in i.deps]  # in experiment table order
            elif not shared.match(p):
                i.raw.append(p)
    return Flow(instances=inst, errors=errors + cycles(inst))


def overlaps(inst, adhoc=()):
    """Two instances have the same output, or one is inside the other -> [problems]. Two tasks that are both in adhoc (added to
    the hub queue by hand, outside the experiment table) may share an output: that is history the experiment table cannot fix."""
    owner, probs = {}, []

    def both_adhoc(a, b):
        return a in adhoc and b in adhoc

    for i in inst.values():
        for o in i.outputs:
            if o in owner and owner[o] != i.id and not both_adhoc(owner[o], i.id):
                probs.append(f"{o} is an output of both {owner[o]} and {i.id}")
            owner.setdefault(o, i.id)
    for o, iid in owner.items():
        parts = o.split("/")
        for n in range(1, len(parts)):
            up = "/".join(parts[:n])
            if up in owner and owner[up] != iid and not both_adhoc(owner[up], iid):
                probs.append(f"output {o} of {iid} is inside output {up} of {owner[up]} (a path belongs to one step only)")
    return probs


def cycles(inst):
    """The order has cycles -> [problems] (each cycle once)."""
    state, probs = {}, []

    def visit(n, path):
        if state.get(n) == 1:
            probs.append("cycle in the order: " + " -> ".join(path[path.index(n):] + [n]))
            return
        if state.get(n) == 2 or n not in inst:
            return
        state[n] = 1
        for d in inst[n].deps:
            visit(d, path + [n])
        state[n] = 2
    for n in inst:
        visit(n, [])
    return probs
