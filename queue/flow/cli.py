"""只读的命令：taskq who <路径>、taskq dag [目标] [--dot|--check|--json]。按实验表编译出的实例回答。"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from flow.compile import build  # noqa: E402
from flow.graph import Graph  # noqa: E402

NOT_FOUND = 4


def graph():
    return Graph(build())


def who(argv):
    import cfg
    M = cfg.mtab()
    ap = argparse.ArgumentParser(prog="taskq who", description="这个文件（或目录）是哪个实例的产出（按实验表声明的产出，最长前缀）")
    ap.add_argument("path", help="相对数据目录的路径，或 store 上的绝对路径（数据目录开头）")
    ap.add_argument("--json", action="store_true", help="stdout 只输出 JSON")
    a = ap.parse_args(argv)
    p = a.path
    if os.path.isabs(p) or p.startswith("~"):
        root = M.data().rstrip("/")
        if not (p == root or p.startswith(root + "/")):
            print(f"taskq who：{p} 不在 store（{M.store()}）的数据目录 {root} 下", file=sys.stderr)
            return NOT_FOUND
        p = os.path.normpath(p[len(root) + 1:] or ".")
    g = graph()
    iid, out = g.who(p)
    if iid is None:
        print(json.dumps(dict(path=p, owner=None)) if a.json else f"{p}：不是哪个实例声明的产出（人的文件，或者实验表没声明）",
              file=sys.stdout if a.json else sys.stderr)
        return NOT_FOUND
    ids = iid if isinstance(iid, list) else [iid]
    rows = [dict(id=x, rule=g.inst[x].rule, exp=g.inst[x].exp) for x in ids]
    if a.json:
        print(json.dumps(dict(path=p, declared=out, shared=isinstance(iid, list), owners=rows), ensure_ascii=False))
        return 0
    print(f"{p}：几个实例共用（{out}）" if isinstance(iid, list) else f"{p}：属于 {out}")
    for r in rows:
        print(f"  {r['id']}  步骤 {r['rule']}" + (f"  实验 {r['exp']}" if r["exp"] else ""))
    return 0


def dag(argv):
    ap = argparse.ArgumentParser(prog="taskq dag", description="实例之间的先后（去掉能推出来的）；给目标时只看它的上下游")
    ap.add_argument("target", nargs="?", help="实例 id、步骤名或实验 id")
    ap.add_argument("--dot", action="store_true", help="输出 Graphviz（dot -Tsvg）")
    ap.add_argument("--check", action="store_true", help="核对：产出重叠、输入落在自己的产出里、有环；有问题退出码 1")
    ap.add_argument("--json", action="store_true", help="全部实例（id、步骤、实验、依赖、产出、输入、原始输入），--check 时是问题列表")
    a = ap.parse_args(argv)
    g = graph()
    probs = g.flow.errors
    if a.check:
        if a.json:
            print(json.dumps({"problems": probs}, ensure_ascii=False))
        else:
            print("\n".join(probs) if probs else f"{len(g.inst)} 个实例，先后和产出都没有问题", file=sys.stderr if probs else sys.stdout)
        return 1 if probs else 0
    if a.json:
        print(json.dumps({"instances": [dict(id=i.id, scope=i.scope, rule=i.rule, exp=i.exp, deps=list(i.deps), outputs=list(i.outputs),
                                              shared=list(i.shared), inputs=list(i.inputs), raw=list(i.raw))
                                         for i in g.inst.values()]}, ensure_ascii=False))
        return 0
    out = g.dag(a.target, dot=a.dot)
    if out is None:
        print(f"taskq dag：没有 {a.target}（实例 id、步骤名或实验 id）", file=sys.stderr)
        return NOT_FOUND
    print(out)
    return 0


CMDS = dict(who=who, dag=dag)


def main(argv):
    cmd, rest = (argv[0], argv[1:]) if argv else ("", [])
    if cmd in CMDS:
        return CMDS[cmd](rest)
    print("用法：taskq who|dag …", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
