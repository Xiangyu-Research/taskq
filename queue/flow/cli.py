"""只读的命令（2026-11-05）：taskq who <路径>、taskq dag [目标] [--dot]。按实验表编译出的规则和实例回答，不看台账。"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from flow.compile import compile_legacy  # noqa: E402
from flow.graph import Graph  # noqa: E402

NOT_FOUND = 4


def data_root():
    import mtab as M
    return os.environ.get("ADVANCE_BT") or M.expand(M.MACHINES[M.hub()]["data"])


def graph():
    return Graph(compile_legacy())


def who(argv):
    ap = argparse.ArgumentParser(prog="taskq who", description="这个文件（或目录）是哪个实例的产出（按实验表声明的产出，最长前缀）")
    ap.add_argument("path", help="绝对路径（在数据目录下）或相对数据目录的路径")
    ap.add_argument("--json", action="store_true", help="stdout 只输出 JSON")
    a = ap.parse_args(argv)
    root = data_root()
    p = a.path
    if os.path.isabs(p):
        rp = os.path.relpath(os.path.realpath(p), os.path.realpath(root))
        if rp.startswith(".."):
            print(f"taskq who：{p} 不在数据目录 {root} 下", file=sys.stderr)
            return NOT_FOUND
        p = rp
    g = graph()
    iid, out = g.who(p)
    if iid is None:
        if a.json:
            print(json.dumps(dict(path=p, owner=None)))
        else:
            print(f"{p}：不是哪个实例声明的产出（人的文件，或者实验表没声明）", file=sys.stderr)
        return NOT_FOUND
    ids = iid if isinstance(iid, list) else [iid]
    rows = [dict(id=x, rule=g.inst[x].rule, exp=g.inst[x].exp, key=g.inst[x].key, queue_key=g.inst[x].qk or None) for x in ids]
    if a.json:
        print(json.dumps(dict(path=p, declared=out, shared=isinstance(iid, list), owners=rows), ensure_ascii=False))
        return 0
    head = f"{p}：几个实例共用（{out}）" if isinstance(iid, list) else f"{p}：属于 {out}"
    print(head)
    for r in rows:
        print(f"  {r['id']}  规则 {r['rule']}" + (f"  实验 {r['exp']}" if r["exp"] else "") + (f"  队列 {r['queue_key']}" if r["queue_key"] else ""))
    return 0


def dag(argv):
    ap = argparse.ArgumentParser(prog="taskq dag", description="实例之间的先后（去掉能推出来的）；给目标时只看它的上下游")
    ap.add_argument("target", nargs="?", help="实例 id、规则名、实验 id 或 key")
    ap.add_argument("--dot", action="store_true", help="输出 Graphviz（dot -Tsvg）")
    ap.add_argument("--check", action="store_true", help="核对：产出重叠、有环、输入是别的产出的上级、输入的产出者不在前面；有问题退出码 1")
    ap.add_argument("--json", action="store_true", help="全部实例（id、规则、key、实验、依赖、产出、队列 key），--check 时是问题列表")
    a = ap.parse_args(argv)
    g = graph()
    if a.json:
        if a.check:
            probs = g.check()
            print(json.dumps({"problems": probs}, ensure_ascii=False))
            return 1 if probs else 0
        print(json.dumps({"instances": [dict(id=i.id, rule=i.rule, key=i.key, exp=i.exp, deps=list(i.deps), qk=i.qk,
                                              outputs=list(i.outputs), shared=list(i.shared))
                                         for i in g.flow.instances.values()]}, ensure_ascii=False))
        return 0
    if a.check:
        probs = g.check()
        print("\n".join(probs) if probs else f"{len(g.inst)} 个实例，先后和产出都没有问题", file=sys.stderr if probs else sys.stdout)
        return 1 if probs else 0
    out = g.dag(a.target, dot=a.dot)
    if out is None:
        print(f"taskq dag：没有 {a.target}（实例 id、规则名、实验 id 或 key）", file=sys.stderr)
        return NOT_FOUND
    print(out)
    return 0


def flow_for_ledger():
    import advance
    from flow import ledger as LG
    flow = compile_legacy(python=advance.python())
    return flow, Graph(flow), data_root(), LG


def adopt(argv):
    ap = argparse.ArgumentParser(prog="taskq adopt", description="用现在判断做完的那些检查扫一遍，做完、台账里还没有的记成 adopted"
                                 "（只 stat、不重跑，重复跑结果一样）")
    ap.add_argument("-n", "--dry-run", action="store_true", help="只数，不写台账")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    flow, _, root, LG = flow_for_ledger()
    n = LG.adopt(flow, root, LG.connect(), dry=a.dry_run)
    print(json.dumps(n) if a.json else f"{'会记' if a.dry_run else '记了'} {n['adopted']} 个，已经有 {n['already']} 个，没做完 {n['not_done']} 个")
    return 0


def verify(argv):
    ap = argparse.ArgumentParser(prog="taskq verify", description="台账说做完的，产出现在还对不对（不见了 error、大小变了 warn、只有 mtime 变了 info）")
    ap.add_argument("target", nargs="?", help="实例 id、规则名、实验 id 或 key；不给 = 全部")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    flow, g, root, LG = flow_for_ledger()
    ids = None
    if a.target:
        ids = {i.id for i in flow.instances.values() if a.target in (i.id, i.rule, i.exp, i.key)}
        if not ids:
            print(f"taskq verify：没有 {a.target}", file=sys.stderr)
            return NOT_FOUND
    out = LG.verify(flow, root, LG.connect(), ids)
    if a.json:
        print(json.dumps(out, ensure_ascii=False))
    else:
        for r in out:
            print(f"[{r['level']}] {r['id']}  {r['path']}  {r['what']}")
        print(f"{len(out)} 处不一样" if out else "台账里做完的产出都对得上", file=sys.stderr)
    return 1 if any(r["level"] != "info" for r in out) else 0


def accept(argv):
    ap = argparse.ArgumentParser(prog="taskq accept", description="有意改过的产出：按现在的样子重新记进台账，写明原因")
    ap.add_argument("target", help="路径（绝对，或相对数据目录）或实例 id")
    ap.add_argument("--reason", required=True)
    a = ap.parse_args(argv)
    flow, g, root, LG = flow_for_ledger()
    ids = LG.accept(flow, g, root, LG.connect(), a.target, a.reason)
    if not ids:
        print(f"taskq accept：{a.target} 不是哪个实例的产出", file=sys.stderr)
        return NOT_FOUND
    print("重新记下：" + "、".join(ids))
    return 0


CMDS = dict(who=who, dag=dag, adopt=adopt, verify=verify, accept=accept)


def main(argv):
    cmd, rest = (argv[0], argv[1:]) if argv else ("", [])
    if cmd in CMDS:
        return CMDS[cmd](rest)
    print("用法：taskq who|dag|adopt|verify|accept …", file=sys.stderr)
    return 2
