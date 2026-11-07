"""relay：store 上的数据传到执行机（stage）、做完的结果从执行机拉回 store（fetch）、备份到备份机（archive）、删那边的任务和结果
（drop_remote）。hub 只统筹：读写数据的都在 store、执行机上跑（relay_remote.op / store_op，queue/site_ops.py），数据在 store 和
执行机之间直接传，不经过 hub 的盘。那边的路径和 store 的数据目录一样；数据按 sha256 存成 blob（relay_remote.py 的说明）。"""
import contextlib
import json
import os
import shlex
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import proto  # noqa: E402
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import spec as S  # noqa: E402
import relay_plugins as RN  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_store as ST  # noqa: E402
import bundle_gate as BG  # noqa: E402

PIN_EVERY = 3600  # 传数据期间每隔这么久重写一次钉子（那边的清理只认 12 小时内的）
MOVE_TIMEOUT = 3 * 86400  # 一次送、拉、归档最多跑多久（隧道自己连续 STALL 秒没有进展就停）


class Gone(RN.RelayError):
    """实验表和总队列里都没有这个 key。"""


# ---------------- 传数据（hub 的 wait 组里跑） ----------------
def lookup(key):
    s = S.lookup(key)
    if s is None:
        raise Gone(f"实验表和总队列里都没有 {key}")
    return s if s.get("exp") else S.resolve(s["recipe"], s["params"])


def stage(name, key, nice=None, add=True, say=print):
    """store 上的数据 → 那边（和 store 的数据目录一样的相对路径）；add 时再加进那边的队列。→ key。"""
    real = lookup(key)
    ex = RM.store_op("export", dict(spec=real))
    ids = in_use(name) if add else []
    g = ids and BG.check_all(S.sendable(S.resolve(real["recipe"], ex["params"])), ids)
    if g:  # 那边在用的代码包跑不了要送的说明：不传、不加，领取保留（queue/bundle_gate.py）
        BG.hold(name, key, *g)
        say(f"{RM.conf(name)['label']} 在用的代码包 {g[0]} 跑不了 {key} 的说明，先不送：{g[1]}")
        return None
    links = {rel: h for rel, h, *_ in ex["files"]}
    with pinned(name, key, links.values(), say):  # 视图建好前那边的清理不删这些块
        return _stage(name, key, nice, add, say, real, ex, links)


@contextlib.contextmanager
def pinned(name, key, hashes, say=print):
    """这段时间里那边的清理不删这些 blob（R/state/pins/<key>.json）；时间长时定时重写，钉子不过期，出来时撤掉。"""
    pin = proto.safe(key)
    want = dict(key=pin, hashes=sorted(set(hashes)))
    RM.op(name, "pin", want)
    stop = threading.Event()

    def refresh():
        while not stop.wait(PIN_EVERY):
            try:
                RM.op(name, "pin", want)
            except Exception:  # noqa: BLE001
                pass
    th = threading.Thread(target=refresh, daemon=True)
    th.start()
    try:
        yield
    finally:
        stop.set()
        th.join()
        try:
            RM.op(name, "pin", dict(key=pin, hashes=None))
        except RuntimeError as e:
            say(f"撤掉 {key} 的钉住记录没成（{e}），清理 12 小时后不再认它")


def ensure_blobs(name, files, say=print, src=None, what=None, n=None):
    """src（默认 store）上的文件 {sha256: [相对 src 数据目录的路径或 None, 字节数, src 上的路径]} → 那边的 blobs 里都有了
    → {sha256: 那边的路径（R/blobs/<前两位>/<sha256>，~ 不展开）}。先问那边缺哪些；缺的先从那边本机已有的数据目录里找
    （relay_remote.same_disk_srcs：按相对路径找，大小、sha256 对得上、同一个盘的硬链接）；还缺的由 src 送过去（site_ops push）。
    要不被清理删掉，调用方先 pinned。CPU 作业排不上 → RM.NoSlot；没传到 → RelayError。"""
    miss = RM.op(name, "missing", sorted(files))
    say(f"{what or name}：{len(files) if n is None else n} 个文件，那边缺 {len(miss)} 个")
    if miss:
        got = set(RM.op(name, "ingest", dict(srcs=RM.same_disk_srcs(name), files={h: files[h][:2] for h in miss}),
                        timeout=3 * 3600, job=RM.conf(name).get("transfer_job")))
        miss = [h for h in miss if h not in got]
        say(f"那边已有的数据里收进 {len(got)} 个，还缺 {len(miss)} 个")
    if miss:
        src = src or M.store()
        bad = RM.op(src, "push", dict(to=name, files={h: files[h][2] for h in miss}), timeout=MOVE_TIMEOUT, say=say)
        if bad:
            raise RN.RelayError(f"{len(bad)} 个文件没传到或对不上（下次重传）")
    return {h: f"{RM.root(name)}/{proto.blob(h)}" for h in files}


def in_use(name):
    """那边在用的代码包（queue/bundle_gate.py）；连不上 → []。"""
    u = BG.current(name)
    return BG.in_use(name, *u) if u else []


def check_overlap(name, key, paths):
    """这个任务要放的路径和同一台上别的没做完的任务的产出重叠（相同、上级或下级）→ RelayError，不传。"""
    keys = ST.keys_of(ST.load_state(), name)
    hit = []
    for r, k in keys.items():
        if r == key:
            continue
        outs = set(k.get("outs") or [])
        anc = ST.ancestors(outs)
        if any(ST.overlaps(p, outs, anc) for p in paths):
            hit.append(r)
    if not hit:
        return
    states = RM.op(name, "states", None)
    busy = [r for r in hit if states.get(r) not in (None, "done")]
    if busy:
        raise RN.RelayError(f"{key} 要放的输入和 {RM.conf(name)['label']} 上还没做完的 {', '.join(busy)} 的产出重叠，先不传")


def _stage(name, key, nice, add, say, real, ex, links):
    views = [ST.safe_rel(v) for v in ex["views"]]
    placed = sorted({ST.safe_rel(r) for r in list(links) + list(ex["texts"])} - {r for r in links if any(r.startswith(v + "/") for v in views)})
    outs = [ST.safe_rel(o) for o in ex["outs"]]
    cleanup = [ST.safe_rel(c) for c in ex["cleanup"]]
    check_overlap(name, key, views + placed)
    try:
        ensure_blobs(name, {h: [inner, size, path] for _, h, size, inner, path in ex["files"]}, say, what=key, n=len(links))
    except RM.NoSlot as e:
        say(f"{e}；这次不传，下次检查再来")
        return None
    now = time.time()
    ST.update_state(lambda st: st.setdefault("views", {}).setdefault(name, {}).update({v: now for v in views}))
    others = [k for r, k in ST.keys_of(ST.load_state(), name).items() if r != key]
    used = set().union(*[ST.paths_of(k) for k in others]) if others else set()
    clean = [v for v in views if not ST.overlaps(v, used, ST.ancestors(used))]  # 视图里多出来的旧链接只在别的任务不用这个视图时删
    n = RM.op(name, "view", dict(views=views, clean=clean, links=links, texts=ex["texts"]))
    say(f"那边的输入放好了（{n} 个文件，视图 {'、'.join(views) or '无'}）")

    def rec(st):
        st.setdefault("keys", {}).setdefault(name, {})[key] = dict(
            recipe=real["recipe"], params=ex["params"], inputs=real.get("inputs") or [], staged=time.time(), views=views,
            placed=placed, outs=outs, cleanup=cleanup, links=links)
    ST.update_state(rec)
    if add:
        r = RM.rtq(name, f"add --nice {int(nice)} --spec-stdin", input=json.dumps([S.sendable(S.resolve(real["recipe"], ex["params"]))]))
        if r is None or r[0]:
            raise RuntimeError(f"加进 {RM.conf(name)['label']} 的队列出错：{RM.err_tail((r or [0, 'ssh 不通'])[1])}")
        say(r[1])
    else:  # 补传（那边查到缺输入）：撤掉标记，那边接着领
        r = RM.rtq(name, f"input-ok {shlex.quote(key)}")
        if r is None or r[0]:
            raise RuntimeError(f"{RM.conf(name)['label']} 上撤掉缺输入的标记出错：{RM.err_tail((r or [0, 'ssh 不通'])[1])}")
        say(r[1] or "那边没有缺输入的标记")
    return key


def resend(name, say=print):
    """把传给这台的任务按现在的做法重新解析，换掉那边队列里的说明（只换那边已有的，不加新的）。"""
    keys = ST.keys_of(ST.load_state(), name)
    specs, ids = [], in_use(name) if keys else []
    for real, k in keys.items():
        sp = S.sendable(S.resolve(k["recipe"], k["params"]))
        g = BG.check_all(sp, ids)
        if g:  # 那边在用的包跑不了新的说明：留着那边原来的
            say(f"{real} 不换：在用的代码包 {g[0]} 跑不了新的说明（{g[1]}）")
        else:
            specs.append(sp)
    if not specs:
        say("没有传过去的任务")
        return
    r = RM.rtq(name, "add --nice 0 --spec-stdin --update-only", input=json.dumps(specs))
    if r is None or r[0]:
        raise RuntimeError(f"{RM.conf(name)['label']} 换说明出错：{RM.err_tail((r or [0, 'ssh 不通'])[1])}")
    say(r[1] or "没有要换的")


# ---------------- 拉回、归档（hub 的 IO 组里排，数据在 store 和执行机之间走） ----------------
def fetch(name, key, say=print):
    """那边做完的结果 → store 的数据目录（site_ops 的 list 在那边列、pull 在 store 上拉、核对、放好）。
    没拉成（没做完、那边列不出、store 上拉、核对出错）都报 RelayError。"""
    real = lookup(key)
    k = ST.keys_of(ST.load_state(), name).get(key)
    if not k:
        raise RN.RelayError(f"{key} 没有传给 {RM.conf(name)['label']} 的记录")
    o = S.outputs(real)
    try:
        res = RM.op(name, "list", dict(out=real["out"], done=o["done"], exclude=o["exclude"], extra=o["extra"],
                                       outs=k.get("outs") or o["outs"], key=key, links=k.get("links") or {}))
    except RuntimeError as e:
        raise RN.RelayError(f"列 {key} 的产出出错：{e}") from None
    if not res["done"]:
        raise RN.RelayError(f"{RM.conf(name)['label']} 上 {key} 还没做完")
    if res["bad"]:
        raise RN.RelayError(f"{key} 的产出里有指到别处的链接（不是数据目录里的、也不是这个任务的输入）：{res['bad'][:3]}")
    try:
        r = RM.store_op("pull", dict(frm=name, spec=real, items=res["items"], links=res["links"]), timeout=MOVE_TIMEOUT, say=say)
    except RuntimeError as e:
        raise RN.RelayError(f"{key} 拉回 store 出错：{e}") from None
    say(f"{key} 拉回了 {r['files']} 个文件、{r['links']} 个链接")


def archive(key, say=print):
    """store 上这个任务的结果备份到备份机、核对（site_ops archive → archive_task.sh）。"""
    RM.store_op("archive", dict(spec=S.sendable(lookup(key))), timeout=MOVE_TIMEOUT, say=say)
    say(f"{key} 归档了")


# ---------------- 删那边的任务 ----------------
class PathsLeft(Exception):
    """那边的任务删了、路径没删掉：记录里标了 dropped，relay.plan 下一轮再删。"""


def drop_remote(name, key, rk, gone=False):
    """删那边的任务（taskq.py remove 在锁里查过没人领着），再删这个任务占的、别的任务不用的路径和日志，最后删记录 → 出错说明或 None。"""
    if not gone:
        r = RM.rtq(name, f"remove {shlex.quote(rk)}")
        if not r or r[0] not in (0, proto.EXIT["gone"]):  # gone：上次删了任务、没删完路径
            return f"任务没删掉，路径留着：{RM.err_tail((r or [0, 'ssh 不通'])[1], 150)}"
        ST.update_state(lambda st: ((st.get("keys") or {}).get(name, {}).get(key) or {}).update(dropped=time.time()))
    keys = ST.keys_of(ST.load_state(), name)
    paths = ST.droppable(keys, key, ST.paths_of(keys.get(key) or {}))
    try:
        RM.op(name, "drop", dict(paths=paths, logs=proto.safe(rk)))
    except (RuntimeError, OSError) as e:
        if not gone:
            raise PathsLeft(str(e)[-150:]) from e
        raise
    ST.update_state(lambda st: (st.get("keys") or {}).get(name, {}).pop(key, None))
    return None
