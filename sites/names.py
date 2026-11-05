"""送到执行机的任务（2026-11-01 起都用真名）：执行机的数据目录 R/data 和 hub 的数据目录同样布局，任务要的文件以软链接放在
同样的相对路径，参数原样送过去，结果在同样的相对路径拉回。唯一的换法：hub 上数据目录外的绝对路径输入放到 abs/<路径>，
参数里换成 {data}/abs/<路径>。
  path（通用，任何任务）：输入（spec 的 inputs 或 data）是数据目录下的文件夹（视图）、文件或 glob（展开成文件），或 hub 上的绝对路径。
  项目自己的种类（插件文件夹的 relay_plugins.py，经 queue/relay_plugins.py 加载）：按参数列出要的文件，拉回后核对。
任务用哪种：说明的 relay 字段（queue/recipes.py）。

export(spec, root) → dict(params, files, texts, views, outs, cleanup)：
  params   那边加任务用的参数
  files    [(相对路径, hub 上的真实文件)]，按 sha256 放进 blobs、在那边是软链接
  texts    {相对路径: 内容}（现在的插件都不用）
  views    视图文件夹：那边按这次列出的文件同步，多出来的删掉
  outs     要拉回的文件夹（outs(spec)：out 和 fetch.also）
  cleanup  任务交还后那边可以删的文件夹（默认 = outs）
import_(spec, src, dst, root) → 放进 dst 的文件（相对路径）：src 下是拉回的文件（真的相对路径），认不出的、
done 不全的、插件核对不过的报 RelayError，这时 dst 不动。"""
import fnmatch
import glob
import os
import re
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(CORE, "queue"))
import relay_plugins as RN  # noqa: E402
import spec as S  # noqa: E402

RelayError = RN.RelayError
SKIP = ("machines", "after")  # 只在 hub 的总队列里有意义的参数，不送过去
ABS = "abs"  # hub 上数据目录外的输入在那边的位置


def plugin_of(spec):
    return spec.get("relay")


def outs(spec):
    """要拉回的文件夹（相对数据目录）：spec.outputs 的 outs（out，加上 fetch.also）。"""
    return S.outputs(spec)["outs"]


def _inside(a, b):
    return a == b or a.startswith(b.rstrip("/") + "/")


def _sub(v, table):
    """字符串里的绝对路径按 table（[(真, 换成)]，长的先换）换掉；路径两边不能紧挨着字母、数字、. - _。"""
    if isinstance(v, str):
        for a, b in table:
            v = re.sub(r"(?<![\w.-])" + re.escape(a) + r"(?![\w.-])", lambda m, b=b: b, v)
        return v
    if isinstance(v, list):
        return [_sub(x, table) for x in v]
    if isinstance(v, dict):
        return {k: _sub(x, table) for k, x in v.items()}
    return v


def export_path(spec, root):
    files, views, table, rels = [], [], [], {}
    roots = (os.path.normpath(root), os.path.realpath(root))
    for i in spec.get("inputs") or spec.get("data") or []:
        if os.path.isabs(i):
            a = os.path.normpath(i)
            inner = next((os.path.relpath(x, r) for x, r in zip((a, os.path.realpath(a)), roots) if x != r and _inside(x, r)), None)
            base, sub, pre = (root, inner, "") if inner else ("/", a.lstrip("/"), ABS + "/")
            rels[i] = pre + sub
            table.append((a, "{data}/" + rels[i]))
        else:
            base, sub, pre = root, os.path.normpath(i.strip("/")), ""
        if any(ch in sub for ch in "*?["):
            hits = sorted(p for p in glob.glob(os.path.join(base, sub)) if os.path.isfile(p))
            if not hits:
                raise RelayError(f"hub 上没有和 {i} 对得上的文件")
            files += [(pre + os.path.relpath(p, base), os.path.realpath(p)) for p in hits]
            continue
        fs, v = RN.expand(base, sub)
        files += [(pre + f, p) for f, p in fs]
        views += [pre + v] if v else []
    table.sort(key=lambda x: -len(x[0]))
    params = {k: _sub(v, table) for k, v in spec["params"].items() if k not in SKIP}
    for k in ("inputs", "data"):
        if isinstance(params.get(k), list):
            params[k] = [rels.get(x, x) for x in spec["params"][k]]
    return dict(params=params, files=files, views=views)


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


PLUGINS = {"path": (export_path, None), **RN.PLUGINS}
ACCEPTS = dict(RN.ACCEPTS)  # 插件 → 能不能送这个任务（参数 → bool）；没列的插件都能送
VIEW_ROOTS = list(RN.VIEW_ROOTS)  # 插件建视图的文件夹（相对数据目录，下一级是一个视图）


def accepts(plugin, params):
    return plugin in PLUGINS and ACCEPTS.get(plugin, lambda p: True)(params or {})


def _pair(spec):
    t = PLUGINS.get(plugin_of(spec))
    if t is None:
        raise RelayError(f"做法 {spec.get('recipe')} 不能送到执行机（relay={plugin_of(spec)}）")
    return t


def _rel(spec, what, r, depth=1):
    r = os.path.normpath(r)
    if r.startswith("/") or ".." in r.split("/") or len(r.split("/")) < depth:
        raise RelayError(f"{spec['key']} 的{what} {r} 不对（要在数据目录里{'、至少两层' if depth > 1 else ''}）")
    return r


def export(spec, root):
    ex = _pair(spec)[0](spec, root)
    files, seen = [], {}
    for a, p in ex["files"]:
        a = _rel(spec, "文件", a)
        if seen.setdefault(a, p) != p:
            raise RelayError(f"{spec['key']}：{a} 对应 hub 上两个不同的文件（{seen[a]}、{p}）")
        if len(files) < len(seen):
            files.append((a, p))
    views = list(dict.fromkeys(_rel(spec, "视图", v, 2) for v in ex.get("views") or []))
    os_ = [_rel(spec, "结果文件夹", o, 2) for o in outs(spec)]
    bad = [(v, o) for v in views for o in os_ if _inside(v, o) or _inside(o, v)]
    if bad:
        raise RelayError(f"{spec['key']}：视图 {bad[0][0]} 和结果 {bad[0][1]} 重叠（同步视图时会删掉结果）")
    params = ex.get("params")
    if params is None:
        params = {k: v for k, v in spec["params"].items() if k not in SKIP}
    cleanup = [_rel(spec, "要删的文件夹", c, 2) for c in (ex["cleanup"] if "cleanup" in ex else os_)]
    return dict(params=params, files=files, texts=dict(ex.get("texts") or {}), views=views, outs=os_, cleanup=cleanup)


def import_(spec, src, dst, root):
    o = S.outputs(spec)
    os_, out, extra, logs = o["outs"], o["outs"][0], o["extra"], o["logs"]
    par = os.path.dirname(out)
    got, odd = [], []
    for rel in RN.walk(src):
        if any(_inside(rel, o) for o in os_) or (rel.startswith(logs) and "__" not in rel[len(logs):]) or (
                par and os.path.dirname(rel) == par and any(fnmatch.fnmatch(os.path.basename(rel), x) for x in extra)):
            got.append(rel)
        else:
            odd.append(rel)
    if odd:
        raise RelayError(f"{spec['key']} 拉回的文件里有认不出的：{odd[:5]}")
    have = set(got)
    miss = S.done_missing(o, lambda a: f"{out}/{a}" in have)
    if miss:
        raise RelayError(f"{spec['key']} 拉回的结果不全：没有 {miss}")
    check = _pair(spec)[1]
    if check:
        check(spec, src, root)
    written = []
    for rel in got:
        _put(dst, written, rel, os.path.join(src, rel))
    return written
