"""实例之间的先后和文件归属：taskq who（这个文件是谁的产出）、taskq dag（先后）、check 的核对。"""
from common import under
from flow.model import Flow


class Graph:
    def __init__(self, flow: Flow):
        self.flow = flow
        self.inst = flow.instances
        self.users = {}
        for i in self.inst.values():
            for d in i.deps:
                self.users.setdefault(d, []).append(i.id)
        self._anc = {}

    def who(self, path):
        """路径（相对数据目录）→ (实例 id, 归属的那个产出)；几个源头共用的文件 → ([实例 id], 那个文件)；谁的都不是 → (None, None)。"""
        p = path.strip("/")
        sh = [i.id for i in self.inst.values() if p in i.shared]
        if sh:
            return sh, p
        best = None
        for i in self.inst.values():
            for o in i.outputs:
                if under(p, o) and (best is None or len(o) > len(best[1])):
                    best = (i.id, o)
        return best or (None, None)

    def order(self):
        """拓扑序（实例 id 的列表；有环的部分按实验表的顺序）。"""
        seen, out = set(), []

        def visit(n, stack):
            if n in seen or n in stack or n not in self.inst:
                return
            for d in self.inst[n].deps:
                visit(d, stack | {n})
            seen.add(n)
            out.append(n)
        for n in self.inst:
            visit(n, frozenset())
        return out

    def ancestors(self, iid):
        if iid not in self._anc:
            out, todo = set(), list(self.inst[iid].deps)
            while todo:
                n = todo.pop()
                if n not in out and n in self.inst:
                    out.add(n)
                    todo += self.inst[n].deps
            self._anc[iid] = out
        return self._anc[iid]

    def descendants(self, iid):
        out, todo = set(), list(self.users.get(iid, []))
        while todo:
            n = todo.pop()
            if n not in out:
                out.add(n)
                todo += self.users.get(n, [])
        return out

    def direct(self, iid):
        """去掉能经别的先后推出来的（显示用）。"""
        deps = self.inst[iid].deps
        via = set()
        for d in deps:
            if d in self.inst:
                via |= self.ancestors(d)
        return [d for d in deps if d not in via]

    def dag(self, target=None, dot=False):
        """target（实例 id、步骤名、实验 id；空 = 全部）上下游的先后 → 文字（每行 "前 → 后"）或 Graphviz。"""
        if target:
            hit = [i.id for i in self.inst.values() if target in (i.id, i.rule, i.exp)]
            if not hit:
                return None
            keep = set(hit)
            for h in hit:
                keep |= self.ancestors(h) | self.descendants(h)
        else:
            keep = set(self.inst)
        edges = [(d, n) for n in self.order() if n in keep for d in self.direct(n) if d in keep]
        if dot:
            lines = ["digraph flow {", "  rankdir=LR;"]
            lines += [f'  "{n}";' for n in self.order() if n in keep and not self.inst[n].deps and n not in self.users]
            lines += [f'  "{a}" -> "{b}";' for a, b in edges]
            return "\n".join(lines + ["}"])
        lone = [n for n in self.order() if n in keep and not any(n in e for e in edges)]
        return "\n".join([f"{a} → {b}" for a, b in edges] + lone)
