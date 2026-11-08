"""Relay plugin "path": the files a task needs are placed at the same relative paths on the executor
(symlinks to blobs), params are sent as is and results come back at the same paths. The one exception: an
absolute input outside the store data dir goes under abs/<path> over there, and the param becomes {data}/abs/<path>.
Other plugins (relay_plugins.py in the project) list their files per params and check them after the fetch."""
import fnmatch
import glob
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of this scheduler code
sys.path.insert(0, os.path.join(CORE, "queue"))
import common as C  # noqa: E402
import relay_plugins as RN  # noqa: E402
import spec as S  # noqa: E402

RelayError = RN.RelayError
ABS = "abs"  # where inputs outside the store data dir go over there


def plugin_of(spec):
    return spec.get("relay")


def outs(spec):
    """Output dirs to fetch back (relative to the data dir): spec.outputs' outs (out plus fetch.also)."""
    return S.outputs(spec)["outs"]


def _sub(v, table):
    """Replace absolute paths in a string by table ([(real, replacement)], longest first); a path must not touch a letter, digit, . - _."""
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
    for i in spec.get("inputs") or []:
        if os.path.isabs(i):
            a = os.path.normpath(i)
            inner = next((os.path.relpath(x, r) for x, r in zip((a, os.path.realpath(a)), roots) if x != r and C.under(x, r)), None)
            base, sub, pre = (root, inner, "") if inner else ("/", a.lstrip("/"), ABS + "/")
            rels[i] = pre + sub
            table.append((a, "{data}/" + rels[i]))
        else:
            base, sub, pre = root, os.path.normpath(i.strip("/")), ""
        if any(ch in sub for ch in "*?["):
            hits = sorted(p for p in glob.glob(os.path.join(base, sub)) if os.path.isfile(p))
            if not hits:
                raise RelayError(f"no file on the store matches {i}")
            files += [(pre + os.path.relpath(p, base), os.path.realpath(p)) for p in hits]
            continue
        fs, v = RN.expand(base, sub)
        files += [(pre + f, p) for f, p in fs]
        views += [pre + v] if v else []
    table.sort(key=lambda x: -len(x[0]))
    params = {k: _sub(v, table) for k, v in spec["params"].items()}
    if isinstance(params.get("inputs"), list):
        params["inputs"] = [rels.get(x, x) for x in spec["params"]["inputs"]]
    return dict(params=params, files=files, views=views)


def _put(dst, written, rel, srcp):
    C.atomic_copy(srcp, os.path.join(dst, rel), link=True)
    written.append(rel)


PLUGINS = {"path": (export_path, None), **RN.PLUGINS}
ACCEPTS = dict(RN.ACCEPTS)  # plugin -> can it take this task (params -> bool); plugins not listed accept any
VIEW_ROOTS = list(RN.VIEW_ROOTS)  # dirs where a plugin builds views (relative to the data dir, one view per child)


def accepts(plugin, params):
    return plugin in PLUGINS and ACCEPTS.get(plugin, lambda p: True)(params or {})


def _pair(spec):
    t = PLUGINS.get(plugin_of(spec))
    if t is None:
        raise RelayError(f"recipe {spec.get('recipe')} cannot be sent to an executor (relay={plugin_of(spec)})")
    return t


def _call(spec, i, *a):
    """Call the plugin's export (i=0) or check (i=1): its RelayError is re-raised; any other error (unreadable file,
    etc.) becomes a one-line RelayError (plugin, function, error), so the hub gets no traceback."""
    f = _pair(spec)[i]
    try:
        return f(*a)
    except RelayError:
        raise
    except Exception as e:  # noqa: BLE001
        raise RelayError(f"{spec.get('key')}: plugin {plugin_of(spec)} {f.__name__} failed: {type(e).__name__}: {e}") from None


def _rel(spec, what, r, depth=1):
    r = os.path.normpath(r)
    if r.startswith("/") or ".." in r.split("/") or len(r.split("/")) < depth:
        raise RelayError(f"{spec['key']}: bad {what} {r} (must be inside the data dir{', at least two levels' if depth > 1 else ''})")
    return r


def export(spec, root):
    ex = _call(spec, 0, spec, root)
    files, seen = [], {}
    for a, p in ex["files"]:
        a = _rel(spec, "file", a)
        if seen.setdefault(a, p) != p:
            raise RelayError(f"{spec['key']}: {a} maps to two different files on the store ({seen[a]}, {p})")
        if len(files) < len(seen):
            files.append((a, p))
    views = list(dict.fromkeys(_rel(spec, "view", v, 2) for v in ex.get("views") or []))
    os_ = [_rel(spec, "output dir", o, 2) for o in outs(spec)]
    bad = [(v, o) for v in views for o in os_ if C.under(v, o) or C.under(o, v)]
    if bad:
        raise RelayError(f"{spec['key']}: view {bad[0][0]} overlaps output {bad[0][1]} (syncing the view would delete results)")
    params = ex.get("params")
    if params is None:
        params = dict(spec["params"])
    cleanup = [_rel(spec, "dir to delete", c, 2) for c in (ex["cleanup"] if "cleanup" in ex else os_)]
    return dict(params=params, files=files, texts=dict(ex.get("texts") or {}), views=views, outs=os_, cleanup=cleanup)


def import_(spec, src, dst, root, links=()):
    o = S.outputs(spec)
    os_, out, extra, logs = o["outs"], o["outs"][0], o["extra"], o["logs"]
    par = os.path.dirname(out)
    got, odd = [], []
    for rel in RN.walk(src):
        if any(C.under(rel, o) for o in os_) or (rel.startswith(logs) and "__" not in rel[len(logs):]) or (
                par and os.path.dirname(rel) == par and any(fnmatch.fnmatch(os.path.basename(rel), x) for x in extra)):
            got.append(rel)
        else:
            odd.append(rel)
    if odd:
        raise RelayError(f"{spec['key']}: fetched files that do not belong here: {odd[:5]}")
    have = set(got) | set(links)
    miss = S.done_missing(o, lambda a: f"{out}/{a}" in have)
    if miss:
        raise RelayError(f"{spec['key']}: fetched results incomplete: missing {miss}")
    if _pair(spec)[1]:
        _call(spec, 1, spec, src, root)
    written = []
    for rel in got:
        _put(dst, written, rel, os.path.join(src, rel))
    return written
