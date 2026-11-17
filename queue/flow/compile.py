"""Experiment table (EXPERIMENTS and FINAL read by experiments.py) -> instances and order.
There are only two ordering rules: within a task in order (source -> after's first step -> second ...); an input (wildcards
included) that falls in another instance's output (equal, inside it, or containing it) runs after that instance, whichever
experiment it belongs to. An input in no output is raw data (raw).
Checks (errors): unknown template names (cmd and inputs_cmd included, filled here), wildcard outputs, overlapping outputs of two
instances, an input inside its own outputs, temp patterns that leave the output or hit a done marker, cycles, and entries left out of
the experiment tables (experiments.errors). An error belongs to the instances it names: they and everything after them are blocked
(not queued), the rest are not. A step whose outputs cannot be filled stays in the order through its outputs with the unknown names as
wildcards, so what reads them waits too."""
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


def glob_hits(pat, path):
    """Does a glob (a ** component stands for any number of dirs, as in glob.glob(recursive=True)) match this relative path?"""
    def go(ps, xs):
        if not ps:
            return not xs
        if ps[0] == "**":
            return any(go(ps[1:], xs[k:]) for k in range(len(xs) + 1))
        return bool(xs) and fnmatch.fnmatchcase(xs[0], ps[0]) and go(ps[1:], xs[1:])
    return go(pat.split("/"), path.split("/"))


def bad_temp(t, done):
    """Why a step's temp pattern (relative to its first output) cannot be used, or None; done: the step's done field."""
    if t.startswith("/") or ".." in t.split("/") or norm(t) == ".":
        return "must be a path under the output, without .. (not the output itself)"
    hit = [a for d in S.P.done_items(done) for a in d if glob_hits(norm(t), norm(a))]
    return f"matches the done marker {hit[0]} (the step would count as not done after the cleanup)" if hit else None


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
                if k == "**":  # an unknown name in a step's out (_Wild): one or more components
                    for j in range(i + 1, len(parts)):
                        walk(v, j)
                    if self.END in v:  # the input ends where that output does
                        out.update(v[self.END])
                elif k != self.END and (fnmatch.fnmatchcase(k, c) if G.has_magic(c) else k == c or G.has_magic(k) and fnmatch.fnmatchcase(c, k)):
                    walk(v, i + 1)
        walk(self.root, 0)
        return out


class _Wild(dict):
    """Template names in which every unknown name is ** (the outputs of a step whose out cannot be filled, as patterns: a name
    may stand for several path components)."""

    def __contains__(self, k):
        return True

    def __missing__(self, k):
        return "**"


def build(exps=None, final=None, queue_tasks=()):
    """-> Flow. queue_tasks: tasks in the hub queue (taskq status --json tasks); those with a recipe and not in the experiment table are sources too."""
    if exps is None:
        exps, final = E.load()
    left_out = E.errors()
    inst, errors, adhoc = {}, list(left_out), set()
    ghosts = {}  # id -> outputs of a step not in inst (out cannot be filled: patterns; a duplicate id: its outputs), so readers wait for it
    bad = {}  # id -> its own problems
    flags = dict(partial=bool(left_out))  # entries left out: their outputs and inputs are not in the graph

    def add(i):
        if i.id in inst:
            errors.append(f"duplicate instance id: {i.id}")
            bad.setdefault(i.id, []).append(f"duplicate instance id: {i.id}")
            ghosts.setdefault(i.id, []).extend(i.outputs)
            flags["partial"] = True
            return None
        inst[i.id] = i
        return i

    def source(key, exp, recipe, sp, lab):
        o = S.outputs(sp)
        shared = [norm(os.path.join(os.path.dirname(o["outs"][0]), x)) for x in o["extra"]]
        return add(Instance(id=key, scope=SOURCE, rule=recipe, exp=exp or "", names=dict(sp.get("names") or {}),
                            outputs=[norm(x) for x in o["outs"]], shared=shared, spec=sp, lab=lab))

    def step(scope, st, iid, exp, names, deps=(), each=(), **kw):
        """-> the instance; None when it has no place (out cannot be filled, or a duplicate id): then it is blocked by id."""
        probs, got = [], {}

        def fill(k, f):
            try:
                got[k] = f()
            except S.SpecError as e:
                probs.append(f"{iid}: {k}: {e}")
        fill("out", lambda: [norm(S.fill(x, names)) for x in st["out"]])
        fill("inputs", lambda: expand_inputs(st["inputs"], names, each))
        for k in ("env", "done", "temp"):
            fill(k, lambda k=k: [S.fill(x, names) for x in st[k]])
        for k in ("cmd", "inputs_cmd"):  # filled now, so a bad name is reported at once and not on the day the step is due
            if st.get(k) is not None:
                fill(k, lambda k=k: S.fill(st[k], names))
        if "done" in got and "temp" in got:
            probs += [f"temp {t} of {iid} {why}" for t in got["temp"] for why in [bad_temp(t, got["done"])] if why]
        probs += [f"output {o} of {iid} has a wildcard (outputs must be concrete paths)" for o in got.get("out", []) if G.has_magic(o)]
        errors.extend(probs)
        if "inputs" not in got:
            flags["partial"] = True  # what it reads is unknown: an instance may have this user without the graph knowing
        if "out" not in got:
            bad.setdefault(iid, []).extend(probs)
            ghosts.setdefault(iid, []).extend(norm(S.fill(x, _Wild(names))) for x in st["out"])
            flags["partial"] = True
            return None
        filled = {k: v for k, v in got.items() if k not in ("out", "inputs")}  # the step holds env, done, temp, cmd, inputs_cmd filled
        return add(Instance(id=iid, scope=scope, rule=st["name"], exp=exp, names=names, deps=list(deps), outputs=got["out"],
                            inputs=got.get("inputs", []), step=dict(st, **filled), errors=probs, **kw))

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
            source(q["key"], None, q["recipe"], sp, q["key"])
            adhoc.add(q["key"])

    for e in exps:
        for t in (t for t in table.values() if t["exp"] == e["id"]):
            names, lab, prev = E.task_names(t), E.label(e, t), [t["key"]]
            for st in E.steps_of(t, e):
                iid = f"{st['name']}/{lab}"
                step(EACH, st, iid, e["id"], names, deps=prev, task=t["key"], lab=lab)
                prev = prev + [iid]  # with no place of its own (blocked), the later steps of the task still wait for it
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
    for iid, pats in ghosts.items():
        for o in pats:
            outs.add(o, iid)

    def problem(msg, ids):
        errors.append(msg)
        for x in ids:
            (inst[x].errors if x in inst else bad.setdefault(x, [])).append(msg)
    for msg, ids in overlaps(inst, adhoc):
        problem(msg, ids)
    for i in inst.values():
        for p in i.inputs:
            own = [o for o in i.outputs if C.under(p, o) or C.under(o, p)]
            if own:
                problem(f"input {p} of {i.id} falls in its own output {own[0]} (inputs and outputs must not overlap)", [i.id])
            src = outs.match(p) - {i.id}
            if src:
                i.deps += [d for d in list(inst) + list(ghosts) if d in src and d not in i.deps]  # in experiment table order
            elif not shared.match(p):
                i.raw.append(p)
    for msg, ids in _cycles(inst):
        problem(msg, ids)
    for i in inst.values():
        if i.errors:
            bad.setdefault(i.id, []).extend(i.errors)
    return Flow(instances=inst, errors=errors, blocked=blocked(inst, bad), partial=flags["partial"])


def blocked(inst, bad):
    """Instances not to queue -> {id: why}: those with problems of their own (bad: {id: [problems]}, ids with no instance included)
    and every instance after one of them."""
    users = {}
    for i in inst.values():
        for d in i.deps:
            users.setdefault(d, []).append(i.id)
    out = {k: v[0] for k, v in bad.items() if v}
    todo = [(k, k) for k in out]
    while todo:
        n, root = todo.pop(0)
        for u in users.get(n, []):
            if u not in out:
                out[u] = f"after {root}, which has an error"
                todo.append((u, root))
    return out


def overlaps(inst, adhoc=()):
    """Two instances have the same output, or one is inside the other -> [(problem, (instance id, instance id))]. Two tasks that are
    both in adhoc (added to the hub queue by hand, outside the experiment table) may share an output: that is history the experiment
    table cannot fix."""
    owner, probs = {}, []

    def both_adhoc(a, b):
        return a in adhoc and b in adhoc

    for i in inst.values():
        for o in i.outputs:
            if o in owner and owner[o] != i.id and not both_adhoc(owner[o], i.id):
                probs.append((f"{o} is an output of both {owner[o]} and {i.id}", (owner[o], i.id)))
            owner.setdefault(o, i.id)
    for o, iid in owner.items():
        parts = o.split("/")
        for n in range(1, len(parts)):
            up = "/".join(parts[:n])
            if up in owner and owner[up] != iid and not both_adhoc(owner[up], iid):
                probs.append((f"output {o} of {iid} is inside output {up} of {owner[up]} (a path belongs to one step only)", (iid, owner[up])))
    return probs


def cycles(inst):
    """The order has cycles -> [problems] (each cycle once)."""
    return [msg for msg, _ in _cycles(inst)]


def _cycles(inst):
    """-> [(problem, [instance ids in the cycle])]."""
    state, probs = {}, []

    def visit(n, path):
        if state.get(n) == 1:
            ring = path[path.index(n):]
            probs.append(("cycle in the order: " + " -> ".join(ring + [n]), ring))
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
