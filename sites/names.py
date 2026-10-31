"""执行机上的化名（2026-10-31）：送去执行机的任务、数据、结果换成别名，拉回的结果换回真名。分两层：
  path（通用，任何任务、任何文件格式）：只换路径，不动文件内容。每个输入（数据目录下的文件夹或文件，或 box 上的绝对路径）
       换成 in/<别名>/，结果文件夹换成 out/<别名>/o（上一级 out/<别名>/ 只属于这个任务，fetch.extra 放在这里），任务名换成
       job/<别名>；命令、环境变量等参数里出现的这些路径按同一张表替换。别名记在 names.json 的 paths 里，一旦分配就不变。
  内容插件（scripts/queue/relay_names.py 的 nnunet、nnunet_infer）：nnU-Net 的数据集名、病例名写在文件内容里，插件改写内容。
任务用哪种：说明的 relay 字段（scripts/queue/recipes.py；spec 的 names 是参数表，不是插件名）。

每种都有 export(spec, root, names) → dict(params, files, texts, views, cleanup)：
  params   那边加任务用的参数（别名）
  files    [(那边的相对路径, box 上的真实文件)]，按 sha256 放进 blobs、视图里是软链接
  texts    {那边的相对路径: 内容}（改写过的小文件）
  views    这次建的视图文件夹（相对那边的数据目录）：只在这些文件夹里删多出来的文件
  cleanup  任务交还后那边可以删的文件夹（相对那边的数据目录）
和 import_(alias_spec, real_spec, src, dst, root, names) → 写到 dst 下的文件（相对路径，真名）；结果不全时报 RelayError。"""
import os
import re
import secrets
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "scripts", "queue"))
import relay_names as RN  # noqa: E402
import spec as S  # noqa: E402

RelayError = RN.RelayError
Names = RN.Names
SKIP = ("machines", "after")  # 只在 box 的总队列里有意义的参数，不送过去


def plugin_of(spec):
    return spec.get("relay")


def alias_path(names, real, kind):
    """真的路径或 key → <kind>/<12 位十六进制>（names.json 的 paths，分配后不变）。"""
    def fn(d):
        m = d.setdefault("paths", {})
        k = f"{kind}:{real}"
        if k in m:
            return m[k], False
        used = set(m.values())
        while True:
            a = f"{kind}/{secrets.token_hex(6)}"
            if a not in used:
                break
        m[k] = a
        return a, True
    return names._change(fn)


def _sub(v, table):
    """字符串里的真路径按 table（[(真, 别名)]，长的先换）换成别名；路径两边不能紧挨着字母、数字、. - _。"""
    if isinstance(v, str):
        for a, b in table:
            v = re.sub(r"(?<![\w.-])" + re.escape(a) + r"(?![\w.-])", lambda m, b=b: b, v)
        return v
    if isinstance(v, list):
        return [_sub(x, table) for x in v]
    if isinstance(v, dict):
        return {k: _sub(x, table) for k, x in v.items()}
    return v


def _walk(p):
    out = []
    for dp, dns, fns in os.walk(p, followlinks=True):
        dns.sort()
        for f in sorted(fns):
            out.append(os.path.relpath(os.path.join(dp, f), p))
    return out


def _inside(a, b):
    return a == b or a.startswith(b.rstrip("/") + "/")


def export_path(spec, root, names):
    ins = list(spec.get("inputs") or spec.get("data") or [])
    out, key = spec["out"].strip("/"), spec["key"]
    files, table, views = [], [], []
    for i in ins:
        absp = os.path.isabs(i)
        rel = os.path.normpath(i if absp else i.strip("/"))
        p = rel if absp else os.path.join(root, rel)
        if not os.path.exists(p):
            raise RelayError(f"box 上没有输入 {i}")
        a = alias_path(names, rel, "in")
        if os.path.isdir(p):
            fs = _walk(p)
            if not fs:
                raise RelayError(f"输入 {i} 是空的")
            files += [(f"{a}/{f}", os.path.realpath(os.path.join(p, f))) for f in fs]
            target = a
        else:
            target = f"{a}/{os.path.basename(rel)}"
            files.append((target, os.path.realpath(p)))
        views.append(a)
        table.append((rel, "{data}/" + target if absp else target))
    aout = alias_path(names, out, "out") + "/o"
    table.append((out, aout))
    parent = os.path.dirname(out)
    if parent and not any(_inside(r, parent) or _inside(parent, r) for r, _ in table[:-1]):
        table.append((parent, os.path.dirname(aout)))
    table.sort(key=lambda x: -len(x[0]))
    params = {k: v for k, v in spec["params"].items() if k not in SKIP}
    params = {k: (v if k == "key" else _sub(v, table)) for k, v in params.items()}
    if "key" in params:
        params["key"] = alias_path(names, key, "job")
    params["out"] = aout
    for k in ("inputs", "data"):
        if k in params:
            params[k] = list(views)
    blob = repr(params)
    left = [r for r, _ in table if re.search(r"(?<![\w.-])" + re.escape(r) + r"(?![\w.-])", blob)]
    if left:
        raise RelayError(f"{key}：参数里还留着真的路径 {left[:3]}")
    return dict(params=params, files=files, texts={}, views=views, cleanup=[os.path.dirname(aout)])


def _put(dst, written, rel, srcp):
    p = os.path.join(dst, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".relaytmp"
    if os.path.lexists(tmp):
        os.remove(tmp)
    try:
        os.link(srcp, tmp)
    except OSError:
        shutil.copy2(srcp, tmp)
    os.replace(tmp, p)
    written.append(rel)


def import_path(alias_spec, real_spec, src, dst, root, names):
    aout, rout = alias_spec["out"].strip("/"), real_spec["out"].strip("/")
    apar, rpar = os.path.dirname(aout), os.path.dirname(rout)
    la, lr = f"logs/tasks/{S.safe(alias_spec['key'])}__", f"logs/tasks/{S.safe(real_spec['key'])}__"
    written, odd = [], []
    for rel in _walk(src):
        sp = os.path.join(src, rel)
        if _inside(rel, aout):
            _put(dst, written, rout + rel[len(aout):], sp)
        elif os.path.dirname(rel) == apar and rpar:
            _put(dst, written, f"{rpar}/{os.path.basename(rel)}", sp)
        elif rel.startswith(la):
            _put(dst, written, lr + rel[len(la):], sp)
        else:
            odd.append(rel)
    if odd:
        raise RelayError(f"{real_spec['key']} 拉回的文件里有认不出的：{odd[:5]}")
    got = set(written)
    miss = [d for d in real_spec["done"] if not any(f"{rout}/{alt}" in got for alt in d.split("|"))]
    if miss:
        raise RelayError(f"{real_spec['key']} 拉回的结果不全：没有 {miss}")
    return written


PLUGINS = {"path": (export_path, import_path), **RN.TRANSLATORS}
ACCEPTS = dict(RN.ACCEPTS)  # 插件 → 能不能送这个任务（参数 → bool）；没列的插件都能送
VIEW_ROOTS = list(RN.VIEW_ROOTS)  # 插件建视图的文件夹（相对数据目录，下一级是一个视图）


def accepts(plugin, params):
    return plugin in PLUGINS and ACCEPTS.get(plugin, lambda p: True)(params or {})


def _pair(spec):
    t = PLUGINS.get(plugin_of(spec))
    if t is None:
        raise RelayError(f"做法 {spec.get('recipe')} 不能送到执行机（relay={plugin_of(spec)}）")
    return t


def export(spec, root, names):
    ex = _pair(spec)[0](spec, root, names)
    views = [os.path.normpath(v) for v in ex.get("views") or []]
    bad = [v for v in views if v.startswith(("/", "..")) or len(v.split("/")) < 2]
    if not views or bad:
        raise RelayError(f"{spec['key']} 的视图文件夹不对：{bad or '没有'}（要在数据目录里、至少两层）")
    for rel in [a for a, _ in ex["files"]] + list(ex["texts"]):
        if os.path.normpath(rel).startswith(("/", "..")):
            raise RelayError(f"{spec['key']}：{rel} 不在数据目录里")
    return dict(ex, views=views, cleanup=[os.path.normpath(c) for c in ex.get("cleanup") or []])


def import_(alias_spec, real_spec, src, dst, root, names):
    return _pair(real_spec)[1](alias_spec, real_spec, src, dst, root, names)
