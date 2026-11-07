"""实验表（experiments.py 读的 EXPERIMENTS、FINAL）→ 实例和先后。
先后只有两条：同一个任务里按顺序（源头 → after 的第一步 → 第二步 …）；输入（含通配）落在别的实例的产出里（相同、在里面、或包着它）
就接在那个实例后面，不管属于哪个实验。不在任何产出里的输入是原始数据（raw）。
核对（errors）：模板里不认识的名字、产出带通配、两个实例的产出重叠、输入落在自己的产出里、有环。"""
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
    """输入的模板 → 相对数据目录的路径；实验一级的里带 {<参数>}（each 的参数，names 里没有它）的按 <参数>_list 的每个取值展开。"""
    out = []
    for p in paths:
        ks = [k for k in each if "{" + k + "}" in p and k not in names]
        if ks:
            out += [S.fill(p, dict(names, **dict(zip(ks, vs)))) for vs in itertools.product(*(names.get(f"{k}_list", "").split() for k in ks))]
        else:
            out.append(S.fill(p, names))
    return [norm(p) for p in out]


class Outs:
    """所有实例的产出，按路径的段建的树：match(输入) → 产出落在这个输入里或包着它的实例。"""
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
            if self.END in n:  # 产出到这里为止：输入在它里面
                out.update(n[self.END])
            if i == len(parts):  # 输入到这里为止：产出在它里面
                everything(n)
                return
            c = parts[i]
            for k, v in n.items():  # 输入的这一段带通配时按它匹配树里的；树里的这一段带通配（fetch.extra 的 *.json）时反过来
                if k != self.END and (fnmatch.fnmatchcase(k, c) if G.has_magic(c) else k == c or G.has_magic(k) and fnmatch.fnmatchcase(c, k)):
                    walk(v, i + 1)
        walk(self.root, 0)
        return out


def build(exps=None, final=None, queue_tasks=()):
    """→ Flow。queue_tasks：总队列里的任务（taskq status --json 的 tasks），不属于实验表、有做法的也算源头。"""
    if exps is None:
        exps, final = E.load()
    inst, errors = {}, []

    def add(i):
        if i.id in inst:
            errors.append(f"实例 id 重复：{i.id}")
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
            st = dict(st, **{k: [S.fill(x, names) for x in st[k]] for k in ("env", "done")})  # 和 cmd、out、inputs 一样是模板
        except S.SpecError as e:
            errors.append(f"{iid}：{e}")
            return None
        for o in outs:
            if G.has_magic(o):
                errors.append(f"{iid} 的产出 {o} 带通配（产出要写确定的路径）")
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
        if not sp.get("step"):  # 步骤实例由下面按实验表推出来
            source(q["key"], None, q["recipe"], sp, q["key"])

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
    errors += overlaps(inst)
    for i in inst.values():
        for p in i.inputs:
            own = [o for o in i.outputs if C.under(p, o) or C.under(o, p)]
            if own:
                errors.append(f"{i.id} 的输入 {p} 落在自己的产出 {own[0]} 里（输入和产出不能重叠）")
            src = outs.match(p) - {i.id}
            if src:
                i.deps += [d for d in inst if d in src and d not in i.deps]  # 按实验表的顺序
            elif not shared.match(p):
                i.raw.append(p)
    return Flow(instances=inst, errors=errors + cycles(inst))


def overlaps(inst):
    """两个实例的产出相同、或一个在另一个里面 → [问题]。"""
    owner, probs = {}, []
    for i in inst.values():
        for o in i.outputs:
            if o in owner and owner[o] != i.id:
                probs.append(f"{o} 同时是 {owner[o]} 和 {i.id} 的产出")
            owner.setdefault(o, i.id)
    for o, iid in owner.items():
        parts = o.split("/")
        for n in range(1, len(parts)):
            up = "/".join(parts[:n])
            if up in owner and owner[up] != iid:
                probs.append(f"{iid} 的产出 {o} 在 {owner[up]} 的产出 {up} 里（一个路径只归一个步骤）")
    return probs


def cycles(inst):
    """先后有环 → [问题]（每个环报一次）。"""
    state, probs = {}, []

    def visit(n, path):
        if state.get(n) == 1:
            probs.append("先后有环：" + " → ".join(path[path.index(n):] + [n]))
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
