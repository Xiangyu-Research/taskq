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
    """台账、who、run、publish 用的流程：实验表加总队列里实验表以外的任务（总队列在本机时；和健康检查编的一样）。"""
    import advance
    import taskq
    from flow import ledger as LG
    flow = compile_legacy(queue_tasks=taskq.read_tasks(), python=advance.python())
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


def publish(argv):
    ap = argparse.ArgumentParser(prog="taskq publish", description="处理被冲突挡住的发布（工作目录里的结果搬进数据目录），"
                                 "或接着做中途断了的发布")
    ap.add_argument("inst", nargs="?", help="实例 id（不给 = 列出被挡住的）")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--overwrite", action="store_true", help="冲突的文件也归档后换上")
    g.add_argument("--accept-theirs", action="store_true", help="冲突的文件留着现在的，其余照放")
    g.add_argument("--resume", action="store_true", help="照 journal 把中途断了的发布做完")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    from flow import publish as PB
    flow, _, root, LG = flow_for_ledger()
    db = LG.connect()
    if a.resume:
        done = PB.resume(flow, db)
        print(json.dumps(dict(resumed=done)) if a.json else ("接着做完：" + "、".join(done) if done else "没有断了的发布"))
        return 0
    bl = PB.blocked(db)
    if not a.inst:
        if a.json:
            print(json.dumps(dict(blocked=bl), ensure_ascii=False))
        for k, v in sorted(bl.items()):
            if not a.json:
                print(f"{k}  {len(v.get('conflicts') or [])} 处冲突，工作目录 {v.get('work')}")
        if not bl and not a.json:
            print("没有被挡住的发布", file=sys.stderr)
        return 0
    i = flow.instances.get(a.inst)
    if i is None or a.inst not in bl:
        print(f"taskq publish：{a.inst} " + ("不在流程里" if i is None else "没有被挡住的发布"), file=sys.stderr)
        return NOT_FOUND
    v = bl[a.inst]
    if not (a.overwrite or a.accept_theirs):
        for c in v.get("conflicts") or []:
            print(f"冲突：{c['path']}  {c['why']}")
        print("用 --overwrite（冲突的归档后换上）或 --accept-theirs（留着现在的）", file=sys.stderr)
        return 3
    try:
        r = PB.publish(i, v["work"], root, db, aid=v.get("attempt"), dest=v.get("dest"),
                       overwrite=a.overwrite, accept_theirs=a.accept_theirs)
    except PB.PublishError as e:
        print(f"taskq publish：{e}", file=sys.stderr)
        return 3
    n = sum(op == "put" for op, _, _ in r["ops"])
    print(json.dumps(dict(state=r["state"], placed=n)) if a.json else f"{a.inst}：放了 {n} 个文件")
    return 0


def replay(argv):
    ap = argparse.ArgumentParser(prog="taskq internal engine-replay", description="反事实回放：随机假设做完了哪些，advance.plan 和 "
                                 "flow/engine.py 各算一遍（不碰盘、不排任务）；有不一样的退出码 1")
    ap.add_argument("-n", type=int, default=100, help="几种假设（默认 100）")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    from flow import engine
    d = engine.replay(n=a.n, seed=a.seed)
    if a.json:
        print(json.dumps(dict(cases=a.n, diffs=d), ensure_ascii=False))
    else:
        for x in d[:50]:
            print(f"假设 {x['case']}：只有 {x['only']} 有 {x['item']}")
        print(f"{a.n} 种假设，{len(d)} 处不一样", file=sys.stderr)
    return 1 if d else 0


CMDS = dict(who=who, dag=dag, adopt=adopt, verify=verify, accept=accept, publish=publish, replay=replay,
            run=lambda argv: __import__("flow.run").run.main(argv))


def main(argv):
    cmd, rest = (argv[0], argv[1:]) if argv else ("", [])
    if cmd in CMDS:
        return CMDS[cmd](rest)
    print("用法：taskq who|dag|adopt|verify|accept …", file=sys.stderr)
    return 2
