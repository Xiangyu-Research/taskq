"""把现在的实验表（experiments.py 读的 EXPERIMENTS、FINAL）编译成规则和实例（2026-11-05）。
先后照 advance.py 暗含的：
  每个任务的步骤（after）：按顺序，每一步要源头任务和前面的每一步都做完；
  实验一级的每一串（group_after）：每一步要实验的所有源头任务、串里前面的每一步、到这一步为止 needs 列的步骤（每个任务的）都做完；
  FINAL：有 rerun_if_newer 的看列出的规则的所有实例（on_change=rerun），没有的不等谁。"""
import itertools
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import experiments as E  # noqa: E402
import spec as S  # noqa: E402
from flow.model import EACH, GATHER, GLOBAL, SOURCE, Flow, Instance, Rule  # noqa: E402


def fill(v, names):
    return S.fill(v, names, keep=())


def norm(p):
    return os.path.normpath(p.strip("/"))


def expand_inputs(paths, names, each=()):
    """queue 的 inputs → 相对数据目录的路径；实验一级的里带 {<参数>}（each 的参数）的按 <参数>_list 的每个取值展开。"""
    out = []
    for p in paths:
        ks = [k for k in each if "{" + k + "}" in p and k not in names]
        if ks:
            out += [fill(p, dict(names, **dict(zip(ks, vs))))
                    for vs in itertools.product(*(names.get(f"{k}_list", "").split() for k in ks))]
        else:
            out.append(fill(p, names))
    return out


def rule_of(st, scope, rq=""):
    need = st.get("needs") or ()
    watch = tuple(st.get("rerun_if_newer") or ())
    return Rule(name=st["name"], scope=scope, group=st.get("group", ""), cmd=st.get("cmd", ""), done=st.get("done"),
                queue=st.get("queue"), needs=(need,) if isinstance(need, str) else tuple(need),
                on_change="rerun" if watch else "", watch=watch, rq=st.get("rq", rq))


def step_paths(st, names, each=()):
    """步骤 → (产出, 标志, 输入)，都相对数据目录。产出 = queue 的 out、fetch.also 和 done 判断的路径。"""
    outs, marker, ins = [], "", []
    qd = st.get("queue") or {}
    if qd.get("out"):
        o = S.outputs(fill({k: qd.get(k) for k in ("out", "done", "fetch")}, names))
        outs += o["outs"]
    d = st.get("done")
    if getattr(d, "path", None):
        marker = norm(fill(d.path, names))
        if not any(marker == o or marker.startswith(o + "/") for o in outs):
            outs.append(marker)
    if qd.get("inputs"):
        ins = [norm(p) for p in expand_inputs(qd["inputs"], names, each)]
    return outs, marker, ins


def compile_legacy(exps=None, final=None, queue_tasks=(), data="{data}", python="{python}"):
    """→ Flow。queue_tasks：总队列里的任务（taskq status --json 的 tasks），不属于实验表的有做法的也算源头任务。"""
    if exps is None:
        exps, final = E.load()
    rules, inst, errors = {}, {}, []

    def add_rule(r):
        old = rules.get(r.name)
        if old and (old.scope != r.scope or old.cmd != r.cmd):
            # 同名规则在不同实验里写法不同（如 prune 的 needs 多了 lprob）：规则按名字只留一条，差别记在实例的先后里
            if old.scope != r.scope:
                errors.append(f"规则 {r.name} 既是 {old.scope} 又是 {r.scope}")
        rules.setdefault(r.name, r)

    def add(i):
        if i.id in inst:
            errors.append(f"实例 id 重复：{i.id}")
        inst[i.id] = i

    def source(key, exp, recipe, sp):
        add_rule(Rule(name=recipe, scope=SOURCE))
        o = S.outputs(sp)
        outs = o["outs"]
        shared = [norm(os.path.join(os.path.dirname(outs[0]), x)) for x in o["extra"]]
        marker = norm(os.path.join(outs[0], o["done"][0][0])) if o["done"] else ""
        i = Instance(id=key, rule=recipe, key=key, exp=exp or "", names=dict(sp.get("names") or {}), outputs=outs,
                     shared=shared, marker=marker)
        add(i)
        return i

    ids = {e["id"] for e in exps}
    table = {k: t for k, t in E.tasks().items() if t["exp"] in ids}
    for key, t in table.items():
        source(key, t["exp"], t["recipe"], t["spec"])
    for q in queue_tasks:
        if q["key"] in table or q["key"] in inst or not q.get("recipe"):
            continue
        try:
            sp = q.get("spec") or E.resolve_task(q["recipe"], q.get("params") or {}, q.get("exp"))
        except S.SpecError:
            continue
        source(q["key"], None, q["recipe"], sp)

    for e in exps:
        mine = [t for t in table.values() if t["exp"] == e["id"]]
        by_rule = {}
        for t in mine:
            names = E.task_names(t, data, python)
            prev = [t["key"]]
            lab = E.label(e, t)
            for st in e.get("after", []):
                add_rule(rule_of(st, EACH, e.get("rq", "")))
                outs, marker, ins = step_paths(st, names)
                qk = norm(fill(st["queue"]["key"], names)) if st.get("queue") else ""
                i = Instance(id=f"{st['name']}:{lab}", rule=st["name"], key=t["key"], exp=e["id"], names=names,
                             deps=list(prev),
                             qk=qk, outputs=outs, inputs=ins, marker=marker, task=t["key"])
                add(i)
                by_rule.setdefault(st["name"], []).append(i.id)
                prev.append(i.id)
        if not e.get("group_after"):
            continue
        gnames = E.group_names(e, data, python)
        heads = [t["key"] for t in mine]
        for chain in e["group_after"]:
            deps = list(heads)
            for st in chain:
                add_rule(rule_of(st, GATHER, e.get("rq", "")))
                r = rule_of(st, GATHER)
                for x in r.needs:
                    if x not in by_rule:
                        errors.append(f"{st['name']}:{e['id']} 要 {x} 都做完，但 {e['id']} 的每个任务没有 {x} 这一步")
                    deps += by_rule.get(x, [])
                outs, marker, ins = step_paths(st, gnames, e.get("each") or ())
                qk = norm(fill(st["queue"]["key"], gnames)) if st.get("queue") else ""
                i = Instance(id=f"{st['name']}:{e['id']}", rule=st["name"], key=e["id"], exp=e["id"], names=gnames,
                             deps=list(dict.fromkeys(deps)), qk=qk, outputs=outs, inputs=ins, marker=marker)
                add(i)
                deps = i.deps + [i.id]

    gathered = {}
    for i in inst.values():
        gathered.setdefault(i.rule, []).append(i.id)
    for f in final or []:
        r = rule_of(f, GLOBAL)
        add_rule(r)
        names = dict(data=data, python=python)
        outs, marker, ins = step_paths(f, names)
        deps = [x for w in r.watch for x in gathered.get(w, []) if rules[inst[x].rule].scope == GATHER]
        add(Instance(id=f["name"], rule=f["name"], key="all", names=names, deps=deps, outputs=outs, inputs=ins, marker=marker))

    # done 判断的路径是别的实例产出的上级（如删单折概率图的那一步看整个预测目录）：算它改别人的产出，不归它
    for i in inst.values():
        if i.marker and i.marker in i.outputs:
            pre = i.marker + "/"
            if any(o.startswith(pre) for j in inst.values() if j is not i for o in j.outputs):
                i.outputs.remove(i.marker)
                i.mutates.append(i.marker)
    return Flow(rules=rules, instances=inst, errors=errors)
