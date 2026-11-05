"""实例之间的先后和文件归属（2026-11-05）：产出按文件归属，最长的前缀为准；who、dag、核对。"""
import fnmatch
import os

from flow.model import Flow


def under(p, d):
    """p 等于 d 或在 d 下面（按路径的段，不按字符串前缀）。"""
    return p == d or p.startswith(d.rstrip("/") + "/")


class Graph:
    def __init__(self, flow: Flow):
        self.flow = flow
        self._anc = {}
        self.inst = flow.instances
        self.owner = {}   # 产出 → 实例 id
        self.shared = {}  # 共用的文件 → [实例 id]
        self.problems = list(flow.errors)
        for i in self.inst.values():
            for o in i.outputs:
                if o in self.owner and self.owner[o] != i.id:
                    self.problems.append(f"{o} 同时是 {self.owner[o]} 和 {i.id} 的产出（几个实例都会写的同一个文件用 shared）")
                self.owner.setdefault(o, i.id)
            for o in i.shared:
                self.shared.setdefault(o, []).append(i.id)
        self.users = {}
        for i in self.inst.values():
            for d in i.deps:
                if d not in self.inst:
                    self.problems.append(f"{i.id} 要等的 {d} 不存在")
                self.users.setdefault(d, []).append(i.id)

    # ---------------- 归属 ----------------
    def who(self, path):
        """路径（相对数据目录）→ (实例 id, 归属的那个产出)；共用的文件 → ([实例 id], 写法)；谁的都不是 → (None, None)。"""
        p = os.path.normpath(path.strip("/"))
        for pat, ids in self.shared.items():
            if fnmatch.fnmatchcase(p, pat):
                return list(ids), pat
        best = None
        for o, iid in self.owner.items():
            if under(p, o) and (best is None or len(o) > len(best[1])):
                best = (iid, o)
        return best or (None, None)

    # ---------------- 先后 ----------------
    def order(self):
        """拓扑序（实例 id 的列表）；有环时 ValueError。"""
        seen, out, onstack = set(), [], set()

        def visit(n, path):
            if n in onstack:
                raise ValueError("先后有环：" + " → ".join(path + [n]))
            if n in seen or n not in self.inst:
                return
            onstack.add(n)
            for d in self.inst[n].deps:
                visit(d, path + [n])
            onstack.discard(n)
            seen.add(n)
            out.append(n)
        for n in self.inst:
            visit(n, [])
        return out

    def ancestors(self, iid):
        if iid in self._anc:
            return self._anc[iid]
        out, todo = set(), list(self.inst[iid].deps)
        while todo:
            n = todo.pop()
            if n not in out and n in self.inst:
                out.add(n)
                todo += self.inst[n].deps
        self._anc[iid] = out
        return out

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

    # ---------------- 核对 ----------------
    def check(self, whole=()):
        """→ [问题]：产出重叠、有环、输入是别的产出的上级（whole 里的规则除外）、输入的产出者不在先后里排在前面。"""
        probs = list(self.problems)
        try:
            self.order()
        except ValueError as e:
            probs.append(str(e))
        outs = sorted(self.owner)
        for i in self.inst.values():
            anc = None
            for p in i.inputs:
                if "*" in p:
                    continue
                inside = [o for o in outs if under(o, p) and o != p and self.owner[o] != i.id]
                if inside and i.rule not in whole:
                    probs.append(f"{i.id} 的输入 {p} 是 {', '.join(sorted({self.owner[o] for o in inside}))} 的产出的上级"
                                 "（要读整个目录的规则写 input_dir: whole）")
                w, _ = self.who(p)
                for src in ([w] if isinstance(w, str) else []):
                    if src == i.id:
                        continue
                    anc = self.ancestors(i.id) if anc is None else anc
                    if src not in anc:
                        probs.append(f"{i.id} 的输入 {p} 是 {src} 的产出，但先后里 {src} 不在它前面")
        return probs

    # ---------------- 显示 ----------------
    def dag(self, target=None, dot=False):
        """target（实例 id、规则名、实验 id；空 = 全部）上下游的先后 → 文字（每行 "前 → 后"）或 Graphviz。"""
        if target:
            hit = [i.id for i in self.inst.values() if target in (i.id, i.rule, i.exp, i.key)]
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
