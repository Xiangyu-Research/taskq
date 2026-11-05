"""relay：box 上的数据传到执行机（stage）、做完的结果拉回 box（fetch）、删那边的任务和结果（drop_remote）。
那边的路径和 box 的数据目录一样；数据按 sha256 存成 blob（relay_remote.py 的说明）。"""
import base64
import contextlib
import json
import os
import shlex
import shutil
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import proto  # noqa: E402
from proto import ROOT  # noqa: E402
sys.path.insert(0, os.path.join(CORE, "sync"))
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import spec as S  # noqa: E402
import relay_plugins as RN  # noqa: E402
import names as N  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_store as ST  # noqa: E402
import bundle_gate as BG  # noqa: E402

PIN_EVERY = 3600  # 传数据期间每隔这么久重写一次钉子（那边的清理只认 12 小时内的）


def box_data():
    return M.expand(M.MACHINES[M.hub()]["data"])


class Gone(RN.RelayError):
    """实验表和总队列里都没有这个 key。"""


# ---------------- 传数据（box 的 wait 组里跑） ----------------
def lookup(key):
    s = S.lookup(key)
    if s is None:
        raise Gone(f"实验表和总队列里都没有 {key}")
    return s if s.get("exp") else S.resolve(s["recipe"], s["params"])


def stage(name, key, nice=None, add=True, say=print):
    """box 上的数据 → 那边（和 box 一样的相对路径）；add 时再加进那边的队列。→ key。"""
    real, D = lookup(key), box_data()
    ex = N.export(real, D)
    ids = in_use(name) if add else []
    g = ids and BG.check_all(S.sendable(S.resolve(real["recipe"], ex["params"])), ids)
    if g:  # 那边在用的代码包跑不了要送的说明：不传、不加，领取保留（queue/bundle_gate.py，11/5）
        BG.hold(name, key, *g)
        say(f"{RM.conf(name)['label']} 在用的代码包 {g[0]} 跑不了 {key} 的说明，先不送：{g[1]}")
        return None
    hs = ST.sha256s(sorted({p for _, p in ex["files"]}))
    links = {rel: hs[p] for rel, p in ex["files"]}
    with pinned(name, key, links.values(), say):  # 视图建好前那边的清理不删这些块（11/1）
        return _stage(name, key, nice, add, say, real, D, ex, hs, links)


@contextlib.contextmanager
def pinned(name, key, hashes, say=print):
    """这段时间里那边的清理不删这些 blob（R/state/pins/<key>.json）；时间长时定时重写，钉子不过期（11/4），出来时撤掉。"""
    pin = proto.safe(key)
    want = dict(key=pin, hashes=sorted(set(hashes)))
    RM.rpy(name, RM.PIN_PY, want)
    stop = threading.Event()

    def refresh():
        while not stop.wait(PIN_EVERY):
            try:
                RM.rpy(name, RM.PIN_PY, want)
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
            RM.rpy(name, RM.PIN_PY, dict(key=pin, hashes=None))
        except RuntimeError as e:
            say(f"撤掉 {key} 的钉住记录没成（{e}），清理 12 小时后不再认它")


def ensure_blobs(name, files, say=print, data_dir=None, what=None, n=None):
    """box 上的文件 {sha256: 本机路径} → 那边的 blobs 里都有了 → {sha256: 那边的路径（R/blobs/<前两位>/<sha256>，~ 不展开）}。
    先问那边缺哪些，缺的先从那边已有的数据里收编（硬链接，老数据目录、同一个盘上别的执行机、它自己的数据目录：本机路径在
    data_dir（默认 box 的数据目录）下的按相对路径找），还缺的经隧道传。要不被清理删掉，调用方先 pinned。
    CPU 作业排不上 → RM.NoSlot；没传到 → RelayError。"""
    import tunnel
    D = data_dir or box_data()
    miss = RM.rpy(name, RM.MISSING_PY, sorted(files))
    say(f"{what or name}：{len(files) if n is None else n} 个文件，那边缺 {len(miss)} 个")
    if miss:
        srcs, job = RM.adopt_srcs(name), RM.conf(name).get("adopt_job")
        meta = {h: (os.path.relpath(files[h], D) if files[h].startswith(D.rstrip("/") + "/") else None, os.path.getsize(files[h]))
                for h in miss}
        got = set(RM.rpy(name, RM.ADOPT_PY, dict(srcs=srcs + [dict(path=f"{RM.root(name)}/{ROOT['data']}", site=False)], files=meta),
                         timeout=3 * 3600, job=job))
        miss = [h for h in miss if h not in got]
        say(f"那边已有的数据里收编了 {len(got)} 个，还缺 {len(miss)} 个")
    if miss:
        os.makedirs(ST.DIR, exist_ok=True)
        tmp = tempfile.mkdtemp(prefix="push_", dir=ST.DIR)
        try:
            rels = {h: os.path.relpath(proto.blob(h), ROOT["blobs"]) for h in miss}  # 相对 blobs
            for h in miss:
                os.makedirs(os.path.dirname(os.path.join(tmp, rels[h])), exist_ok=True)
                os.symlink(files[h], os.path.join(tmp, rels[h]))
            bad = tunnel.push(RM.conf(name)["ssh"], tmp, f"{RM.root(name)}/{ROOT['blobs']}", [rels[h] for h in miss], site=name)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
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
    states = RM.rpy(name, RM.STATES_PY, None)
    busy = [r for r in hit if states.get(r) not in (None, "done")]
    if busy:
        raise RN.RelayError(f"{key} 要放的输入和 {RM.conf(name)['label']} 上还没做完的 {', '.join(busy)} 的产出重叠，先不传")


def _stage(name, key, nice, add, say, real, D, ex, hs, links):
    import tunnel
    views = [ST.safe_rel(v) for v in ex["views"]]
    inputs = sorted({ST.safe_rel(r) for r in list(links) + list(ex["texts"])} - {r for r in links if any(r.startswith(v + "/") for v in views)})
    outs = [ST.safe_rel(o) for o in ex.get("outs") or N.outs(real)]
    cleanup = [ST.safe_rel(c) for c in ex["cleanup"]]
    check_overlap(name, key, views + inputs)
    srcs = RM.adopt_srcs(name)
    job = RM.conf(name).get("adopt_job")
    try:
        ensure_blobs(name, {h: p for p, h in hs.items()}, say, D, what=key, n=len(links))
    except RM.NoSlot as e:
        say(f"{e}；这次不传，下次检查再来")
        return None
    now = time.time()
    ST.update_state(lambda st: st.setdefault("views", {}).setdefault(name, {}).update({v: now for v in views}))
    texts = {k: (v if isinstance(v, bytes) else v.encode()) for k, v in ex["texts"].items()}
    others = [k for r, k in ST.keys_of(ST.load_state(), name).items() if r != key]
    used = set().union(*[ST.paths_of(k) for k in others]) if others else set()
    clean = [v for v in views if not ST.overlaps(v, used, ST.ancestors(used))]  # 视图里多出来的旧链接只在别的任务不用这个视图时删
    n = RM.rpy(name, RM.VIEW_PY, dict(views=views, clean=clean, links=links,
                                      texts={k: base64.b64encode(v).decode() for k, v in texts.items()}))
    say(f"那边的输入放好了（{n} 个文件，视图 {'、'.join(views) or '无'}）")
    if add and srcs and real.get("out") and real.get("resume"):  # 老数据目录、别的执行机里训到一半的存档（任务说明的 resume，最新的那份）拷过来，接着训
        dirs = [f"{x['path']}/{ROOT['data']}/{real['out']}" if x["site"] else f"{x['path']}/{real['out']}" for x in srcs]
        # 那台还领着（proto.claim_alive）：存档可能写到一半
        claims = [dict(claim=f"{x['path']}/{ROOT['queue']}/{proto.QUEUE['claims']}/{proto.safe(key)}",
                       out=f"{x['path']}/{ROOT['data']}/{real['out']}") for x in srcs if x["site"]]
        try:
            r = RM.rpy(name, RM.ADOPT_OUT_PY, dict(srcs=dirs, out=real["out"], resume=real["resume"], done=S.outputs(real)["done"],
                                                   claims=claims, log=real.get("log")), job=job)
        except RM.NoSlot as e:
            say(f"{e}；这次不加进队列，下次检查再来")
            return None
        if r.get("busy"):
            say(f"{r['busy']}：那边还领着 {key}，存档可能还在写；这次不加进队列，下次检查再来")
            return None
        if r["n"]:
            say(f"{r['src']} 的存档拷过来了（{r['n']} 个文件），接着训")

    def rec(st):
        st.setdefault("keys", {}).setdefault(name, {})[key] = dict(
            recipe=real["recipe"], params=ex["params"], data=real.get("data") or [], staged=time.time(), views=views,
            inputs=inputs, outs=outs, cleanup=cleanup)
    ST.update_state(rec)
    if add:
        r = RM.rtq(name, f"add --nice {int(nice)} --spec-stdin", input=json.dumps([S.sendable(S.resolve(real["recipe"], ex["params"]))]))
        if r is None or r[0]:
            raise RuntimeError(f"加进 {RM.conf(name)['label']} 的队列出错：{(r or [0, 'ssh 不通'])[1][-300:]}")
        say(r[1])
    else:  # 补传（那边查到缺输入）：撤掉标记，那边接着领
        r = RM.rtq(name, f"input-ok {shlex.quote(key)}")
        if r is None or r[0]:
            raise RuntimeError(f"{RM.conf(name)['label']} 上撤掉缺输入的标记出错：{(r or [0, 'ssh 不通'])[1][-300:]}")
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
        raise RuntimeError(f"{RM.conf(name)['label']} 换说明出错：{(r or [0, 'ssh 不通'])[1][-300:]}")
    say(r[1] or "没有要换的")


# ---------------- 拉回（box 的 wait 组里跑） ----------------
def place(src, D, written, real):
    """核对过的结果（src 下，和数据目录一样的相对路径）挪进数据目录：普通文件 → out 上一级里 fetch.extra 的文件（已有的不动）→
    MANIFEST.json → 做完的标志（按 done 倒着放，第一项最后放）。"""
    import fnmatch
    o = S.outputs(real)
    rout = o["outs"][0]
    par = os.path.dirname(rout)
    extra = o["extra"]
    rank = {}
    for n, item in enumerate(o["done"]):
        for alt in item:
            rank[f"{rout}/{alt}"] = n
    done = sorted((w for w in written if w in rank), key=lambda w: -rank[w])
    parent = [w for w in written if w not in rank and os.path.dirname(w) == par
              and any(fnmatch.fnmatch(os.path.basename(w), x) for x in extra)]
    man = [w for w in written if w == f"{rout}/{proto.MANIFEST}" and w not in rank]
    first = [w for w in written if w not in rank and w not in parent and w not in man]

    def mv(w):
        p = os.path.join(D, w)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        os.replace(os.path.join(src, w), p)
    for w in first:
        mv(w)
    for w in parent:
        if not os.path.exists(os.path.join(D, w)):
            mv(w)
    for w in man + done:
        mv(w)


def fetch(name, key, say=print):
    import stat
    import tunnel
    real, D = lookup(key), box_data()
    k = ST.keys_of(ST.load_state(), name).get(key)
    if not k:
        raise RN.RelayError(f"{key} 没有传给 {RM.conf(name)['label']} 的记录")
    o = S.outputs(real)
    res = RM.rpy(name, RM.LIST_PY, dict(out=real["out"], done=o["done"], exclude=o["exclude"], extra=o["extra"],
                                        outs=k.get("outs") or o["outs"], key=key))
    if not res["done"]:
        raise RN.RelayError(f"{RM.conf(name)['label']} 上 {key} 还没做完")
    inbox = os.path.join(D, proto.RELAY_INBOX, proto.safe(key))
    out = inbox + ".real"
    shutil.rmtree(out, ignore_errors=True)
    # 上次拉到一半的（11/2）：清单里有、大小和修改时间都对上的留着（隧道核对过 MD5 才放进来），只拉其余的；别的删掉
    want = {i[0]: list(i[1:]) for i in res["items"]}
    for dp, _, fs in os.walk(inbox):
        for f in fs:
            p = os.path.join(dp, f)
            st = os.lstat(p)
            rel = os.path.relpath(p, inbox)
            if rel in want and stat.S_ISREG(st.st_mode) and [st.st_size, st.st_mtime_ns][:len(want[rel])] == want[rel]:
                want.pop(rel)
            else:
                os.remove(p)
    todo = [(r, v[0]) for r, v in want.items()]
    bad = tunnel.pull(RM.conf(name)["ssh"], f"{RM.root(name)}/{ROOT['data']}", inbox, todo, site=name) if todo else []
    if bad:
        raise RN.RelayError(f"{len(bad)} 个文件没拉到（{bad[:3]}）")
    written = N.import_(real, inbox, out, D)
    place(out, D, written, real)
    for d in (inbox, out):
        shutil.rmtree(d, ignore_errors=True)
    say(f"{key} 拉回了 {len(written)} 个文件")


# ---------------- 删那边的任务 ----------------
class PathsLeft(Exception):
    """那边的任务删了、路径没删掉：记录里标了 dropped，relay.plan 下一轮再删（11/5）。"""


def drop_remote(name, key, rk, gone=False):
    """删那边的任务（taskq.py remove 在锁里查过没人领着），再删这个任务占的、别的任务不用的路径和日志，最后删记录 → 出错说明或 None。"""
    if not gone:
        r = RM.rtq(name, f"remove {shlex.quote(rk)}")
        if not r or r[0] not in (0, proto.EXIT["gone"]):  # gone：上次删了任务、没删完路径
            return f"任务没删掉，路径留着：{(r or [0, 'ssh 不通'])[1][-150:]}"
        ST.update_state(lambda st: ((st.get("keys") or {}).get(name, {}).get(key) or {}).update(dropped=time.time()))
    keys = ST.keys_of(ST.load_state(), name)
    paths = ST.droppable(keys, key, ST.paths_of(keys.get(key) or {}))
    try:
        RM.rpy(name, RM.DROP_PY, dict(paths=paths, logs=proto.safe(rk)))
    except (RuntimeError, OSError) as e:
        if not gone:
            raise PathsLeft(str(e)[-150:]) from e
        raise
    ST.update_state(lambda st: (st.get("keys") or {}).get(name, {}).pop(key, None))
    return None
