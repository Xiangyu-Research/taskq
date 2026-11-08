"""Read-only commands: taskq who <path>, taskq dag [target] [--dot|--check|--json]. They answer from instances compiled from the experiment table."""
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
    ap = argparse.ArgumentParser(prog="taskq who", description="whose output this file (or dir) is (from declared outputs, longest prefix)")
    ap.add_argument("path", help="path relative to the data dir, or an absolute path on the store (starting with the data dir)")
    ap.add_argument("--json", action="store_true", help="print JSON only on stdout")
    a = ap.parse_args(argv)
    p = a.path
    if os.path.isabs(p) or p.startswith("~"):
        root = M.data().rstrip("/")
        if not (p == root or p.startswith(root + "/")):
            print(f"taskq who: {p} is not under the data dir {root} of the store ({M.store()})", file=sys.stderr)
            return NOT_FOUND
        p = os.path.normpath(p[len(root) + 1:] or ".")
    g = graph()
    iid, out = g.who(p)
    if iid is None:
        print(json.dumps(dict(path=p, owner=None)) if a.json else f"{p}: not a declared output of any instance (a human file, or not declared in the experiment table)",
              file=sys.stdout if a.json else sys.stderr)
        return NOT_FOUND
    ids = iid if isinstance(iid, list) else [iid]
    rows = [dict(id=x, rule=g.inst[x].rule, exp=g.inst[x].exp) for x in ids]
    if a.json:
        print(json.dumps(dict(path=p, declared=out, shared=isinstance(iid, list), owners=rows), ensure_ascii=False))
        return 0
    print(f"{p}: shared by several instances ({out})" if isinstance(iid, list) else f"{p}: belongs to {out}")
    for r in rows:
        print(f"  {r['id']}  step {r['rule']}" + (f"  experiment {r['exp']}" if r["exp"] else ""))
    return 0


def dag(argv):
    ap = argparse.ArgumentParser(prog="taskq dag", description="order between instances (implied edges removed); with a target, only its upstream and downstream")
    ap.add_argument("target", nargs="?", help="instance id, step name or experiment id")
    ap.add_argument("--dot", action="store_true", help="print Graphviz (dot -Tsvg)")
    ap.add_argument("--check", action="store_true", help="check overlapping outputs, inputs inside own outputs, cycles; exit 1 on problems")
    ap.add_argument("--json", action="store_true", help="all instances (id, step, experiment, deps, outputs, inputs, raw inputs); with --check, the problem list")
    a = ap.parse_args(argv)
    g = graph()
    probs = g.flow.errors
    if a.check:
        if a.json:
            print(json.dumps({"problems": probs}, ensure_ascii=False))
        else:
            print("\n".join(probs) if probs else f"{len(g.inst)} instances, order and outputs have no problems", file=sys.stderr if probs else sys.stdout)
        return 1 if probs else 0
    if a.json:
        print(json.dumps({"instances": [dict(id=i.id, scope=i.scope, rule=i.rule, exp=i.exp, deps=list(i.deps), outputs=list(i.outputs),
                                              shared=list(i.shared), inputs=list(i.inputs), raw=list(i.raw))
                                         for i in g.inst.values()]}, ensure_ascii=False))
        return 0
    out = g.dag(a.target, dot=a.dot)
    if out is None:
        print(f"taskq dag: no {a.target} (instance id, step name or experiment id)", file=sys.stderr)
        return NOT_FOUND
    print(out)
    return 0


CMDS = dict(who=who, dag=dag)


def main(argv):
    cmd, rest = (argv[0], argv[1:]) if argv else ("", [])
    if cmd in CMDS:
        return CMDS[cmd](rest)
    print("usage: taskq who|dag ...", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
