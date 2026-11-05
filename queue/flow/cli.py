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
    a = ap.parse_args(argv)
    g = graph()
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


def main(argv):
    cmd, rest = (argv[0], argv[1:]) if argv else ("", [])
    if cmd == "who":
        return who(rest)
    if cmd == "dag":
        return dag(rest)
    print("用法：taskq who <路径> | taskq dag [目标] [--dot|--check]", file=sys.stderr)
    return 2
