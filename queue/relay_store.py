"""relay 在 hub 上的记账（状态文件夹的 relay/state.json，cfg.state：每台各任务放过去的路径、视图、代码包）和执行机根目录的清理。

清理：剩余低于软线时，先删总队列不再要的视图（state 里登记过的各任务的视图，加上插件建视图的文件夹（sites/names.py 的
VIEW_ROOTS）下的目录；不在 keep 里、没有 .incomplete（正在建，或位置上有别的真文件、没建成）的），再按最久没用先删没有软链接指向的 blob（只删 nlink == 1 的，
别处还有硬链接的不碰；正在传、视图还没建好的任务钉住的（R/state/pins/，relay.py stage 写，PIN_TTL 内的）和传到一半的临时文件
不碰），直到剩余回到软线以上。只碰根目录 R 里面的东西；R/blobs 是指到别的盘的软链接时（如 Slurm 执行机的 scratch 盘）也清理它，
只删 blobs/<前两位>/ 下的文件。删视图时拿建视图的锁（R/state/view.lock）。
根目录经软链接到达时，视图里的软链接指的是软链接的路径，比较时都换成真实路径。"""
import fcntl
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import relay_plugins as RN  # noqa: E402
from common import write_json  # noqa: E402

DIR = cfg.state("relay")
STATE = os.path.join(DIR, "state.json")


# ---------------- 状态文件 ----------------
def update_state(fn):
    os.makedirs(DIR, exist_ok=True)
    with open(STATE + ".lock", "a") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        st = load_state()
        out = fn(st)
        write_json(STATE, st)
    return out


def load_state():
    try:
        with open(STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def keys_of(st, name):
    """key → dict(recipe, params, inputs, staged, views, placed, outs, cleanup)。inputs：说明里的输入；placed：放过去的文件、视图里的除外（相对 R/data）；
    outs：产出的文件夹。"""
    return (st.get("keys") or {}).get(name) or {}


def paths_of(k):
    """一个任务在那边占的路径（相对 R/data）：视图、输入、产出、要清理的。"""
    return set(views_of(k)) | set(k.get("placed") or []) | set(k.get("outs") or []) | set(k.get("cleanup") or [])


def overlaps(p, used, anc):
    """p 和 used 里的某个路径相同、是它的上级或下级 → True。anc：used 里各路径的所有上级。"""
    if p in used or p in anc:
        return True
    parts = p.split("/")
    return any("/".join(parts[:i]) in used for i in range(1, len(parts)))


def ancestors(paths):
    return {"/".join(q.split("/")[:i]) for q in paths for i in range(1, len(q.split("/")))}


def droppable(keys, key, paths):
    """删 key 时 paths 里能删的：别的任务（keys 里的其余记录）还用着的（相同、上级或下级）不删。"""
    used = set().union(*[paths_of(k) for r, k in keys.items() if r != key]) if len(keys) > 1 else set()
    anc = ancestors(used)
    return sorted(p for p in paths if not overlaps(p, used, anc))


def views_of(k):
    """一个任务的视图文件夹。"""
    return list(k.get("views") or [])


def safe_rel(rel):
    """相对 R/data 的路径：不能是绝对路径、不能有 ..、至少两层（不删到 data 下的第一层）。"""
    r = os.path.normpath(rel)
    if r.startswith("/") or ".." in r.split("/") or len(r.split("/")) < 2:
        raise RN.RelayError(f"路径不对：{rel}")
    return r


# ---------------- 清理 ----------------
def budget(name):
    return M.machine(name).get("budget")


def gc(name, keep_dirs, op, soft_gb=None, free=None, views=(), blob_free=None):
    """name 这台的清理 → dict(freed, removed_views, removed_blobs, free)；没有 budget → None。
    keep_dirs：分给这台的任务要用的、刚建的视图（相对 R/data）。views：登记过的视图（state.json 的 views）。op：relay_remote.op。
    free：剩余的字节数（有 cap 的执行机由 relay.py 按 cap 算好传进来；不给时看盘本身）。
    blob_free：blobs 在另一个盘、有 blob_cap 时 blobs 那边的剩余（relay.py 按 blob_cap 算好传进来）：删 blob 只看它，
    删视图看两边哪边剩不到软线；不给时 blob 和视图都看 free。removed_views 里也有登记了但那边已经没有的视图（登记表跟着去掉）。"""
    if soft_gb is None:
        b = budget(name)
        if not b:
            return None
        soft_gb = b[0]
    sys.path.insert(0, os.path.join(CORE, "sites"))
    import names as N
    return op(name, "gc", dict(soft_gb=soft_gb, keep=sorted(keep_dirs), free=free, views=sorted(views), scan=N.VIEW_ROOTS, blob_free=blob_free))
