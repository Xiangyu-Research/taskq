#!/usr/bin/env python3
"""在执行机、store 上跑的操作：hub 经 relay_remote.op 调用，跑那台根目录里的代码包（本机的也一样）。

    python3 site_ops.py <操作>      参数是标准输入上的一个 JSON，结果是以 proto.JSON_MARK 开头的一行；出错 → 退出码非 0、错误在 stderr

根目录 R 是环境变量 TASKQ_ROOT（布局见 proto.ROOT）；store 的操作另给数据目录（参数里的 data）。
执行机的操作只 import proto、common（都只用标准库）；送、拉、按说明解析的操作用到时再 import names、spec、mtab、tunnel（mods）。
操作里打印的进度在 stderr。

执行机的根目录：
  status   队列、各任务的情况、剩余空间、环境、代码包
  states   队列里各任务的状态
  missing  blobs 里没有（或核对不上）的哈希
  pin      钉住 / 放开一个任务要用的 blob（清理不删）
  ingest   把同一个盘上已有的文件硬链接进 blobs（sha256 对得上才收）
  view     建一个任务的视图：输入是指向 blob 的软链接，从不盖掉真文件
  list     列一个任务要拉回的产出（和"做完没有"用同一条规则）
  drop     删一个任务的视图、产出、分段日志
  gc       空间不够时删不用的视图、blob
  env_mark 建环境前查空间、写标志
store 的数据目录：
  done     各实例的 done 文件缺哪些
  probe    各通配匹配几个文件、最新的修改时间、占多大（硬链接只算一次）
  rm       删中间产物（temp 的通配）
  export   按说明列出送一个任务要的文件、算 sha256（缓存在 R/state/hashes.db）
  pull     从执行机拉回一个任务的产出、核对、放进数据目录（标志最后放）
  collect  各任务在数据目录里的情况（spec.py collect）
  facts    总队列各任务的结果和日志（spec.facts；hub 按它判断做完、在训、做了一半）
  smoke    冒烟定义里的函数（要读数据目录的）算出来
  archive  一个任务的结果备份到备份机（archive_task.sh）
有文件要送的机器（store；有锁的环境时 hub 的 wheelhouse）：
  push     把文件放进执行机的 blobs（同一台本机拷、硬链接，别的经隧道）
"""
import base64
import fcntl
import fnmatch
import glob
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import proto  # noqa: E402
import common as C  # noqa: E402

HASH_RE = re.compile(r"[0-9a-f]{64}")


class Bad(Exception):
    """参数不对、状态不对：报给 hub，不当作连不上。"""


def root():
    r = os.environ.get("TASKQ_ROOT")
    if not r:
        raise Bad("没有 TASKQ_ROOT")
    return os.path.realpath(os.path.expanduser(r))


def sub(R, k):
    return os.path.join(R, proto.ROOT[k])


def rel_ok(r, depth=1):
    """数据目录下的相对路径：不能是绝对路径、不能有 ..、至少 depth 级。"""
    r = os.path.normpath(r)
    if r.startswith("/") or ".." in r.split("/") or len(r.split("/")) < depth:
        raise Bad(f"路径不对：{r}")
    return r


def exists(p):
    """路径在（软链接解开以后也在）。"""
    try:
        os.stat(p)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False


def done_missing(D, out, done):
    """out（相对数据目录）下 done 的每一项（[备选, …]）都至少有一个在 → []；否则缺的那几项（每项写成 a|b）。
    "做完没有"只有这一条：执行机上 list、store 上 done 都用它。"""
    return ["|".join(alts) for alts in done if not any(exists(os.path.join(D, out, a)) for a in alts)]


def lock(R):
    p = os.path.join(R, proto.ROOT["view_lock"])
    os.makedirs(os.path.dirname(p), exist_ok=True)
    f = open(p, "w")
    fcntl.flock(f, fcntl.LOCK_EX)
    return f


# ---------------- blob ----------------
def blob_path(R, h):
    return os.path.join(R, proto.blob(h))


def stamp(p):
    x = os.stat(p)
    return f"{x.st_size} {x.st_mtime_ns}"


def record(R, h):
    """记下 blob 现在的大小和修改时间（刚核对过 sha256）。"""
    C.atomic_write(blob_path(R, h) + proto.BLOB_STAT, stamp(blob_path(R, h)))


def blob_ok(R, h):
    """blob 在、内容还是 h：大小、修改时间和记下的一样；没有记录的（经隧道传来的）算一次 sha256 再记。
    对不上（硬链接的源头被原地改过、传坏了）→ 删掉 blob 和记录（源头不动），返回 False。"""
    p = blob_path(R, h)
    if not os.path.isfile(p):
        return False
    try:
        rec = open(p + proto.BLOB_STAT).read().strip()
    except OSError:
        rec = None
    if rec is not None and rec == stamp(p):
        return True
    if rec is None and C.sha256(p) == h:
        record(R, h)
        return True
    for x in (p, p + proto.BLOB_STAT):
        if os.path.lexists(x):
            os.remove(x)
    print(f"blob {h[:12]} 和收进来时不一样，丢掉重收", file=sys.stderr)
    return False


def op_missing(A):
    R = root()
    return [h for h in A if not blob_ok(R, h)]


def op_pin(A):
    p = os.path.join(sub(root(), "pins"), A["key"] + ".json")
    if A.get("hashes") is None:
        if os.path.exists(p):
            os.remove(p)
    else:
        C.atomic_write(p, json.dumps(A["hashes"]))
    return 1


def op_ingest(A):
    """files {h: [相对路径, 大小]}，srcs：找的数据目录。同一个盘（st_dev）、大小和 sha256 都对得上 → 硬链接进 blobs、记下。"""
    R = root()
    B = sub(R, "blobs")
    bdev = os.stat(B if os.path.isdir(B) else R).st_dev
    got = []
    for h, (rel, size) in A["files"].items():
        if not rel:
            continue
        t = blob_path(R, h)
        for base in A["srcs"]:
            p = os.path.join(os.path.expanduser(base), rel)
            try:
                x = os.stat(p)
            except OSError:
                continue
            if x.st_size != size or x.st_dev != bdev or C.sha256(p) != h:
                continue
            os.makedirs(os.path.dirname(t), exist_ok=True)
            if not os.path.exists(t):
                try:
                    os.link(os.path.realpath(p), t)
                except OSError:
                    continue
            record(R, h)
            got.append(h)
            break
    return got


# ---------------- 视图 ----------------


def op_view(A):
    """views：这个任务的视图文件夹；links {相对路径: h}；texts {相对路径: base64}；clean：要清掉旧链接的文件夹（默认 views）。
    输入的位置已经是真文件时：就是那个 blob 本身（同一个 inode，同盘收进来的前一步产出）→ 不动；否则报错，不盖：
    那个位置所在的视图留着 .incomplete（不算建好，清理也不删它下面的真文件），别的视图照常建好、去掉 .incomplete。"""
    R = root()
    D = sub(R, "data")
    L = lock(R)
    views = [rel_ok(v, 2) for v in A["views"]]
    for h in A["links"].values():
        if not blob_ok(R, h):
            raise Bad(f"缺 blob {h}")
    incs = []
    for v in views:
        os.makedirs(os.path.join(D, v), exist_ok=True)
        inc = os.path.join(D, v, proto.INCOMPLETE)
        open(inc, "w").close()
        incs.append(inc)
    want, clash = set(incs), []
    for rel, h in A["links"].items():
        p = os.path.join(D, rel_ok(rel))
        t = blob_path(R, h)
        want.add(p)
        if os.path.islink(p):
            if os.readlink(p) != t:
                C.atomic_link(t, p)
        elif os.path.lexists(p):
            if not os.path.samefile(p, t):
                clash.append(rel)
        else:
            C.atomic_link(t, p)
    for rel, b in A["texts"].items():
        p = os.path.join(D, rel_ok(rel))
        want.add(p)

        def w(x, b=b):
            with open(x, "wb") as f:
                f.write(base64.b64decode(b))
        C.atomic_put(p, w)
    B = os.path.realpath(sub(R, "blobs")) + "/"
    for v in A.get("clean", views):
        for dp, _, fs in os.walk(os.path.join(D, rel_ok(v))):
            for f in fs:
                p = os.path.join(dp, f)
                if p not in want and os.path.islink(p) and os.path.realpath(p).startswith(B):
                    os.remove(p)
    for v, inc in zip(views, incs):
        if not any(C.under(os.path.normpath(c), v) for c in clash):
            os.remove(inc)
    if clash:
        L.close()
        raise Bad("视图的位置上已有别的真文件，不盖：" + ", ".join(clash[:5]))
    return len(want) - len(incs)


# ---------------- 拉回 ----------------
def raise_(e):
    raise e


def op_list(A):
    """out：主文件夹；outs：要拉回的文件夹（out 和其余的）；done；exclude：不拉的文件名；extra：out 上一级里还要拉的；key：分段日志；
    links：这个任务的视图 {相对路径: sha256}（stage 时记下的）。
    → dict(done, missing, items=[[相对路径, 大小, 修改时间 ns]], links=[[相对路径, 指向的相对数据目录的路径]], bad=[相对路径])。
    产出里的软链接都换成它指的逻辑路径（相对数据目录）：指向数据目录里的（outs 里的、视图里的输入）按路径；指向 blob（解开过的输入）
    按这个任务的视图换回那个输入的路径；指到别处、换不回的 → bad。"""
    R = root()
    D = sub(R, "data")
    Bdir = os.path.realpath(sub(R, "blobs"))
    os.stat(D)
    outs = [rel_ok(o) for o in A["outs"]]
    by_hash = {h: r for r, h in sorted((A.get("links") or {}).items(), reverse=True)}
    items, links, bad = [], [], []

    def add(p):
        x = os.lstat(p)
        items.append([os.path.relpath(p, D), x.st_size, x.st_mtime_ns])

    def link(p):
        t = os.readlink(p)
        a = os.path.normpath(t if t.startswith("/") else os.path.join(os.path.dirname(p), t))
        rel = os.path.relpath(p, D)
        if a.startswith(Bdir + "/") and HASH_RE.fullmatch(os.path.basename(a)):
            to = by_hash.get(os.path.basename(a))
            (links.append([rel, to]) if to else bad.append(rel))
        elif a == D or a.startswith(D + "/"):
            links.append([rel, os.path.relpath(a, D)])
        else:
            bad.append(rel)

    for o in outs:
        top = os.path.join(D, o)
        if not os.path.isdir(top):
            continue
        for dp, dns, fs in os.walk(top, onerror=raise_):  # 读不了的子目录不能静默跳过（只拉回一部分却算做完）
            for f in fs + [d for d in dns if os.path.islink(os.path.join(dp, d))]:
                p = os.path.join(dp, f)
                if any(fnmatch.fnmatch(f, x) for x in A["exclude"]) or f.endswith(proto.TMP):  # 不拉的名字：文件、链接都不拉
                    continue
                if os.path.islink(p):
                    link(p)
                elif os.path.isfile(p):
                    add(p)
    for x in A["extra"]:
        for p in glob.glob(os.path.join(os.path.dirname(os.path.join(D, outs[0])), x)):
            if os.path.isfile(p) and not os.path.islink(p):
                add(p)
    pre = proto.safe(A["key"]) + proto.SAFE
    for p in glob.glob(os.path.join(D, proto.TASK_LOGS, glob.escape(pre) + "*")):
        if proto.SAFE not in os.path.basename(p)[len(pre):]:
            add(p)
    miss = done_missing(D, outs[0], A["done"])
    return dict(done=not miss, missing=miss, items=items, links=links, bad=bad)


def op_drop(A):
    """paths：要删的（相对数据目录，至少两级），删完把空的上级也删掉；logs：分段日志名的前缀（safe key）。"""
    D = sub(root(), "data")
    n = 0
    for rel in A["paths"]:
        try:
            r = rel_ok(rel, 2)
        except Bad:
            continue
        p = os.path.join(D, r)
        if os.path.islink(p) or os.path.isfile(p):
            os.remove(p)
        elif os.path.isdir(p):
            shutil.rmtree(p)
        else:
            continue
        n += 1
        d = os.path.dirname(p)
        while len(os.path.relpath(d, D).split("/")) >= 2:
            try:
                os.rmdir(d)
            except OSError:
                break
            d = os.path.dirname(d)
    pre = A["logs"] + proto.SAFE
    for p in glob.glob(os.path.join(D, proto.TASK_LOGS, glob.escape(pre) + "*")):
        if proto.SAFE not in os.path.basename(p)[len(pre):]:
            os.remove(p)
    return n


# ---------------- 状态 ----------------
def versioned(args, inp=None):
    """同一个代码包里的带版本号的命令（taskq.py status --json、spec.py collect --json）→ 去掉 version 的结果。"""
    p = subprocess.run([sys.executable, os.path.join(HERE, args[0])] + args[1:], input=inp, capture_output=True, text=True)
    line = next((x[len(proto.JSON_MARK):] for x in p.stdout.splitlines() if x.startswith(proto.JSON_MARK)), None)
    try:
        r = json.loads(line) if p.returncode == 0 and line is not None else None
    except ValueError:
        r = None
    if not isinstance(r, dict) or not isinstance(r.get("version"), int):
        raise Bad("%s: %s %s" % (" ".join(args), p.returncode, (p.stderr or p.stdout).strip()[-300:]))
    r.pop("version")
    return r


def local_status():
    return versioned(["taskq.py", "status", "--json", "--internal"])


def op_states(A):
    return {t["key"]: t["state"] for t in local_status()["tasks"]}


def capped_free(path, cap, top=None):
    """path 所在盘剩多少字节；有 cap（GB）时不超过 cap 减去 top（默认 path）下已占的。"""
    free = shutil.disk_usage(path).free
    if not cap:
        return free
    return min(free, max(0, int(cap * 1e9) - C.du([top or path])[0]))


def env_states(R, A):
    ed = sub(R, "envs_ok")
    oks = {f[:-3]: open(os.path.join(ed, f)).read().strip() for f in (os.listdir(ed) if os.path.isdir(ed) else []) if f.endswith(".ok")}
    st = {}
    er = os.path.expanduser(A.get("env_root") or sub(R, "envs"))
    for n in A.get("envs") or []:
        d = st[n] = dict(active=os.path.exists(os.path.join(er, n, ".activate")), log="")
        for x in ("ok", "failed", "building"):
            try:
                d[x] = open(os.path.join(ed, n + "." + x)).readline().strip()
            except OSError:
                d[x] = ""
        if d["failed"]:
            try:
                d["log"] = open(os.path.join(ed, n + ".log"), "rb").read()[-2000:].decode("utf-8", "replace")
            except OSError:
                pass
    return oks, st


def op_status(A):
    """A：sched（pueue / slurm）、pueue、cap、blob_cap、env_root、envs。"""
    R = root()
    st = local_status()
    sp = [{k: t["spec"].get(k) for k in proto.COLLECT_FIELDS} for t in st["tasks"] if t.get("spec")]
    col = versioned(["spec.py", "collect", "--json"], json.dumps(dict(specs=sp, root=sub(R, "data"), wait=None)))
    try:
        if A.get("sched") == "pueue":
            q = subprocess.run([os.path.expanduser(A["pueue"]), "status", "--json"], capture_output=True, text=True, timeout=60)
        else:
            q = subprocess.run(["squeue", "-u", os.environ.get("USER", ""), "-h", "-o", "%i|%j|%T|%M|%q|%R"],
                               capture_output=True, text=True, timeout=60)
        raw = q.stdout if q.returncode == 0 else None
    except Exception:
        raw = None
    oks, envs_st = env_states(R, A)
    free = capped_free(R, A.get("cap"))
    out_used = C.du([sub(R, "data")])[0]  # 各任务已经写出的产出（视图是链接，不算）：挑执行机时和留给它们的空间对冲
    cdir = sub(R, "code")
    live = [d for d in (os.listdir(cdir) if os.path.isdir(cdir) else []) if ".tmp" not in d and os.path.isdir(os.path.join(cdir, d))
            and time.time() - os.stat(os.path.join(cdir, d)).st_mtime < proto.STALE_BEAT]
    B = os.path.realpath(sub(R, "blobs"))
    dev = os.stat(R).st_dev
    bdev = os.stat(B).st_dev if os.path.isdir(B) else dev
    bfree = free
    if bdev != dev:
        bfree = capped_free(B, A.get("blob_cap"))
    return dict(taskq=st, tasks=col.get("tasks"), sched_raw=raw, free=free, dev=dev, envs_ok=oks, envs_st=envs_st, now=int(time.time()),
                out_used=out_used, blob_dev=bdev, blob_free=bfree,
                bundle=os.path.basename(os.path.realpath(sub(R, "current"))), live=live)


# ---------------- 清理 ----------------
_PINS = [None, set()]  # (pins 文件夹, inode, 修改时间 ns) 和读到的：没变时不重新读（几百个块、几十个钉子时清理很慢）


def pins(R):
    """钉住的 blob（PIN_TTL 内写的 pins/*.json）。文件夹 2 秒内改过时总是重读（同一个修改时间里可能还在写）。"""
    d, s = sub(R, "pins"), set()
    try:
        x = os.stat(d)
    except OSError:
        return s
    k = (d, x.st_ino, x.st_mtime_ns)
    if _PINS[0] == k and time.time() - x.st_mtime > 2:
        return _PINS[1]
    for f in os.listdir(d):
        p = os.path.join(d, f)
        try:
            if f.endswith(".json") and time.time() - os.path.getmtime(p) < proto.PIN_TTL:
                s |= set(json.load(open(p)))
        except (OSError, ValueError):
            continue
    _PINS[:] = [k, s]
    return s


def op_gc(A):
    """剩余空间（free / blob_free，没给就现量）低于 soft_gb 时：先删 views、scan 下不在 keep 里的视图，再按最久没用的删 blob
    （没有视图指着、没被钉住、只有一个链接）。→ dict(freed, removed_views, removed_blobs, free[, blob_free])。"""
    R = root()
    soft = int(A["soft_gb"] * 1e9)
    D, B = sub(R, "data"), sub(R, "blobs")
    blobs_ok = os.path.isdir(B) and (C.inside(B, R) or os.path.islink(B) and not C.inside(R, B))
    free0 = A["free"] if A.get("free") is not None else shutil.disk_usage(R).free
    bfree0 = A.get("blob_free")
    out = dict(freed=0, removed_views=[], removed_blobs=0, free=free0)
    bf = [0]

    def free():
        return free0 + out["freed"] - (bf[0] if bfree0 is not None else 0)

    def bfree():
        return free() if bfree0 is None else bfree0 + bf[0]

    def rmtree(d):
        if os.path.islink(d):
            os.remove(d)
            return
        if not C.inside(d, D):
            return
        for dp, dns, fns in os.walk(d, topdown=False):
            for f in fns:
                p = os.path.join(dp, f)
                x = os.lstat(p)
                os.remove(p)
                if stat.S_ISREG(x.st_mode) and x.st_nlink == 1:
                    out["freed"] += x.st_size
            for n in dns:
                p = os.path.join(dp, n)
                os.remove(p) if os.path.islink(p) else os.rmdir(p)
        os.rmdir(d)

    if free() < soft or bfree() < soft:
        L = lock(R)
        keep = {os.path.normpath(k) for k in A.get("keep") or []}
        cand = set()
        for v in A.get("views") or []:
            try:
                cand.add(rel_ok(v, 2))
            except Bad:
                pass
        for sc in A.get("scan") or []:
            P = os.path.join(D, sc)
            if os.path.isdir(P) and C.inside(P, D):
                cand |= {os.path.relpath(os.path.join(P, n), D) for n in os.listdir(P)}
        for rel in sorted(cand):
            d = os.path.join(D, rel)
            if rel in keep or any(k.startswith(rel + "/") or rel.startswith(k + "/") for k in keep):
                continue
            if not os.path.lexists(d):
                out["removed_views"].append(rel)
                continue
            if os.path.exists(os.path.join(d, proto.INCOMPLETE)) or not C.inside(d, D):
                continue
            rmtree(d)
            out["removed_views"].append(rel)
        L.close()
    if bfree() < soft and blobs_ok:
        used = set()
        for dp, dns, fns in os.walk(D):
            for f in fns + dns:
                p = os.path.join(dp, f)
                if os.path.islink(p):
                    used.add(os.path.realpath(p))
        pinned = pins(R)
        cand = []
        for dp, _, fns in os.walk(B):
            for f in fns:
                p = os.path.normpath(os.path.join(dp, f))
                if f.endswith(proto.BLOB_STAT) and not os.path.exists(p[:-len(proto.BLOB_STAT)]):
                    os.remove(p)  # blob 已经没了的记录
                    continue
                if not HASH_RE.fullmatch(f) or os.path.dirname(p) != os.path.dirname(blob_path(R, f)):
                    h = f.split(".", 1)[0]
                    if "." in f and HASH_RE.fullmatch(h) and os.path.dirname(p) == os.path.dirname(blob_path(R, h)) \
                            and time.time() - os.lstat(p).st_mtime > proto.TMP_KEEP and not f.endswith(proto.BLOB_STAT):
                        os.remove(p)  # 传到一半丢下的（<h2>/<h>.<后缀>）；块目录外的、不按哈希起名的不碰
                    continue
                if os.path.realpath(p) in used or os.path.islink(p) or f in pinned:
                    continue
                try:
                    x = os.lstat(p)
                except OSError:
                    continue
                if x.st_nlink != 1:
                    continue
                cand.append((max(x.st_atime, x.st_mtime), p, x.st_size, f))
        for _, p, sz, h in sorted(cand):
            if bfree() >= soft:
                break
            if not C.inside(p, B) or h in pins(R):
                continue
            try:
                os.remove(p)
            except OSError:
                continue
            if os.path.exists(p + proto.BLOB_STAT):
                os.remove(p + proto.BLOB_STAT)
            out["freed"] += sz
            bf[0] += sz
            out["removed_blobs"] += 1
    out["free"] = free()
    if bfree0 is not None:
        out["blob_free"] = bfree()
    return out


# ---------------- 环境 ----------------
def op_env_mark(A):
    """建环境前：mode=reserve 按 need（字节）查 env_root 所在盘剩多少（cap：按根目录已占的算），够写 <环境>.building、不够写 .failed；
    mode=clear 撤掉这次写的 .building。sd：标志文件夹。"""
    E, SD, n = os.path.expanduser(A["env_root"]), os.path.expanduser(A["sd"]), A["env"]
    os.makedirs(SD, exist_ok=True)
    if A["mode"] == "clear":
        p = os.path.join(SD, n + ".building")
        try:
            if open(p).read().split()[0] == A["hash"]:
                os.remove(p)
        except (OSError, IndexError):
            pass
        return dict(ok=True)
    p = E
    while not os.path.isdir(p):
        p = os.path.dirname(p)
    free = capped_free(p, A.get("cap"), root() if A.get("cap") else None)
    now = int(time.time())
    ok = free >= A["need"]
    with open(os.path.join(SD, n + (".building" if ok else ".failed")), "w") as f:
        f.write("%s %d\n" % (A["hash"], now) if ok else "%s %d nospace %d %d\n" % (A["hash"], now, A["need"], free))
    return dict(ok=ok, free=free, need=A["need"])


# ---------------- store 的数据目录 ----------------
def store_dir(A):
    return os.path.realpath(os.path.expanduser(A["data"]))


def op_done(A):
    """items {名: dict(out, done)} → {名: 缺的 done 项}（[] = 做完了）。"""
    D = store_dir(A)
    return {k: done_missing(D, rel_ok(v["out"]), v["done"]) for k, v in A["items"].items()}


def matches(D, pat):
    """相对数据目录的通配 → 匹配到的路径（文件夹算它下面的全部）。"""
    return glob.glob(os.path.join(D, rel_ok(pat)), recursive=True)


def newest(paths):
    t = None
    for top in paths:
        for dp, _, fs in (os.walk(top) if os.path.isdir(top) else [(os.path.dirname(top), [], [os.path.basename(top)])]):
            for f in fs:
                try:
                    m = os.stat(os.path.join(dp, f)).st_mtime
                except OSError:
                    continue
                t = m if t is None or m > t else t
    return t


def op_probe(A):
    """pats：通配（相对数据目录）；size：要不要算大小（要传多少：软链接按指到的算，硬链接一份）
    → {通配: dict(n=匹配几个, newest=最新的修改时间或 None, size=字节或 None)}。"""
    D = store_dir(A)
    res = {}
    for pat in A["pats"]:
        got = matches(D, pat)
        res[pat] = dict(n=len(got), newest=newest(got), size=C.du(got, follow=True)[0] if A.get("size") else None)
    return res


def op_rm(A):
    """pats：中间产物的通配（相对数据目录）→ dict(n=删了几个, bytes=腾出多少)；只删文件，不删数据目录以外的。"""
    D = store_dir(A)
    n, freed = 0, 0
    for pat in A["pats"]:
        for p in matches(D, pat):
            if not C.inside(p, D) or os.path.realpath(p) == D:
                continue
            if os.path.isdir(p) and not os.path.islink(p):
                freed += C.du([p])[0]
                shutil.rmtree(p)
            else:
                x = os.lstat(p)
                freed += x.st_blocks * 512 if stat.S_ISREG(x.st_mode) and x.st_nlink == 1 else 0
                os.remove(p)
            n += 1
    return dict(n=n, bytes=freed)


# ---------------- 有文件要送的机器（store 的数据、hub 的 wheelhouse）：送给执行机、拉回、归档 ----------------
def core_path(*parts):
    return os.path.join(os.path.dirname(HERE), *parts)


def mods():
    """传输、按说明解析才用的调度模块（names、spec、mtab、tunnel，要读配置和插件）：用到时才 import，执行机的操作不用。"""
    for d in ("sites", "sync"):
        if core_path(d) not in sys.path:
            sys.path.insert(0, core_path(d))
    import mtab
    import names
    import spec
    import tunnel
    return mtab, names, spec, tunnel


def task_call(f, *a, **kw):
    """说明、插件出的错（RelayError、SpecError）→ Bad：报给 hub 的是原因。"""
    _, N, S, _ = mods()
    try:
        return f(*a, **kw)
    except (N.RelayError, S.SpecError) as e:
        raise Bad(str(e)) from None


def reach(name):
    """从本机到执行机 name：→ (ssh 名，本机是 None；那边的根目录)。ssh 是 None 的机器都在 hub 的机器上，彼此是本机。"""
    M = mods()[0]
    m, me = M.machine(name), M.whoami()
    if name == me or m["ssh"] is None:
        if name != me and (me is None or M.machine(me)["ssh"] is not None):
            raise Bad(f"{name} 在 hub 的机器上（ssh 是 None），{me} 连不到它")
        return None, m["root"].rstrip("/")
    return m["ssh"], m["root"].rstrip("/")


def hashes(R, paths):
    """真实路径 → sha256；按 (路径, 大小, 修改时间) 缓存在根目录的 state/hashes.db：没改过的文件不再读一遍。"""
    import sqlite3
    p = sub(R, "hashes")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    db = sqlite3.connect(p, timeout=600)
    try:
        db.execute("CREATE TABLE IF NOT EXISTS hash(k TEXT PRIMARY KEY, sha TEXT NOT NULL)")
        out = {}
        for x in paths:
            st = os.stat(x)
            k = f"{x}|{st.st_size}|{st.st_mtime_ns}"
            r = db.execute("SELECT sha FROM hash WHERE k=?", (k,)).fetchone()
            out[x] = r[0] if r else C.sha256(x)
            if not r:
                with db:
                    db.execute("INSERT OR REPLACE INTO hash(k, sha) VALUES(?, ?)", (k, out[x]))
        return out
    finally:
        db.close()


def op_export(A):
    """spec：解析好的说明 → 送这个任务要的东西（sites/names.py 的 export，在 store 的数据目录上）：
    dict(params, views, outs, cleanup, texts={相对路径: base64},
         files=[[相对路径, sha256, 字节数, 相对数据目录的路径（不在里面的 None）, store 上的真实路径]])。"""
    N = mods()[1]
    D = store_dir(A)
    ex = task_call(N.export, A["spec"], D)
    hs = hashes(root(), sorted({p for _, p in ex["files"]}))
    files = [[rel, hs[p], os.path.getsize(p), os.path.relpath(p, D) if C.inside(p, D) else None, p] for rel, p in ex["files"]]
    texts = {k: base64.b64encode(v if isinstance(v, bytes) else v.encode()).decode() for k, v in ex["texts"].items()}
    return dict(params=ex["params"], views=ex["views"], outs=ex["outs"], cleanup=ex["cleanup"], texts=texts, files=files)


def op_push(A):
    """to：执行机；files {sha256: 本机路径} → 放进那边的 blobs（同一台机器时在本机拷、能硬链接就硬链接，否则经隧道）
    → 没传到、对不上的 sha256（已删掉）。"""
    T = mods()[3]
    R = root()
    host, troot = reach(A["to"])
    rels = {h: os.path.relpath(proto.blob(h), proto.ROOT["blobs"]) for h in A["files"]}
    os.makedirs(sub(R, "tmp"), exist_ok=True)
    tmp = tempfile.mkdtemp(prefix="push_", dir=sub(R, "tmp"))
    try:
        for h, p in A["files"].items():
            q = os.path.join(tmp, rels[h])
            os.makedirs(os.path.dirname(q), exist_ok=True)
            os.symlink(os.path.realpath(p), q)
        bad = T.push(host, tmp, f"{troot}/{proto.ROOT['blobs']}", sorted(rels.values()), site=A["to"])
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    back = {v: k for k, v in rels.items()}
    return [back[r] for r in bad]


def place(src, D, written, sp, links=()):
    """核对过的结果（src 下，和数据目录一样的相对路径）挪进数据目录 D，标志最后放：普通文件 → out 上一级里 fetch.extra 的文件
    （已有的不动）→ 产出里的软链接（links [(相对路径, 指向的相对数据目录的路径)]，写成相对链接）→ MANIFEST.json → 做完的标志
    （按 done 倒着放，第一项最后放）。"""
    S = mods()[2]
    o = S.outputs(sp)
    rout = o["outs"][0]
    par = os.path.dirname(rout)
    rank = {f"{rout}/{alt}": n for n, item in enumerate(o["done"]) for alt in item}
    done = sorted((w for w in written if w in rank), key=lambda w: -rank[w])
    parent = [w for w in written if w not in rank and os.path.dirname(w) == par
              and any(fnmatch.fnmatch(os.path.basename(w), x) for x in o["extra"])]
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
    for rel, to in links:
        p = os.path.join(D, rel)
        C.atomic_link(os.path.relpath(os.path.join(D, to), os.path.dirname(p)), p)
    for w in man + done:
        mv(w)


def op_pull(A):
    """frm：执行机；spec；items [[相对路径, 字节数, 修改时间 ns]]、links [[相对路径, 指向的相对数据目录的路径]]：那边 list 列出的产出
    → 拉到 store 数据目录下的 .relay_in/<safe key>（上次拉到一半、大小和修改时间对得上的不再拉），按说明核对（names.import_），
    再放进数据目录（place，标志最后放）→ dict(files, links)。"""
    _, N, S, T = mods()
    D = store_dir(A)
    sp = A["spec"]
    outs = S.outputs(sp)["outs"]
    links = [(rel_ok(r), rel_ok(t)) for r, t in A["links"]]
    off = [r for r, _ in links if not any(C.under(r, o) for o in outs)]
    if off:
        raise Bad(f"{sp['key']} 的链接不在结果文件夹里：{off[:3]}")
    host, troot = reach(A["frm"])
    inbox = os.path.join(D, proto.RELAY_INBOX, proto.safe(sp["key"]))
    out = inbox + ".real"
    shutil.rmtree(out, ignore_errors=True)
    want = {i[0]: list(i[1:]) for i in A["items"]}
    for dp, _, fs in os.walk(inbox):
        for f in fs:
            p = os.path.join(dp, f)
            x = os.lstat(p)
            rel = os.path.relpath(p, inbox)
            if rel in want and stat.S_ISREG(x.st_mode) and [x.st_size, x.st_mtime_ns][:len(want[rel])] == want[rel]:
                want.pop(rel)
            else:
                os.remove(p)
    todo = [(r, v[0]) for r, v in want.items()]
    bad = T.pull(host, f"{troot}/{proto.ROOT['data']}", inbox, todo, site=A["frm"]) if todo else []
    if bad:
        raise Bad(f"{len(bad)} 个文件没拉到（{bad[:3]}）")
    os.makedirs(inbox, exist_ok=True)
    written = task_call(N.import_, sp, inbox, out, D, links=[r for r, _ in links])
    place(out, D, written, sp, links)
    for d in (inbox, out):
        shutil.rmtree(d, ignore_errors=True)
    return dict(files=len(written), links=len(links))


def op_collect(A):
    """specs → 这些任务在 store 数据目录里的情况（spec.py collect，和执行机 status 里的同一个）；wait：最多等几秒。"""
    return versioned(["spec.py", "collect", "--json"], json.dumps(dict(specs=A["specs"], root=store_dir(A), wait=A.get("wait"))))


def op_facts(A):
    """items [dict(spec, since, tags)] → 各任务在 store 数据目录里的结果和日志（spec.facts：执行机的队列在本机算的同一个），
    hub 的总队列按它判断状态。"""
    S = mods()[2]
    return S.facts(A["items"], store_dir(A))


def op_smoke(A):
    """recipe、names：冒烟定义（插件 smoke.py 的 SMOKE）里是函数的 params、vars，在 store 的数据目录上算 →
    {字段: 值}；出错的字段 → {"error": {字段: 原因}}。"""
    mods()
    import cfg
    ns = dict(SMOKE={})
    cfg.extend(ns, "smoke")
    d, out, err = ns["SMOKE"].get(A["recipe"]) or {}, {}, {}
    for f in ("params", "vars"):
        if callable(d.get(f)):
            try:
                out[f] = d[f](dict(A.get("names") or {}), store_dir(A))
            except Exception as e:  # noqa: BLE001
                err[f] = f"{type(e).__name__}: {e}"
    return dict(out, error=err) if err else out


def op_archive(A):
    """spec、dry：这个任务的结果从 store 备份到备份机、核对（archive_task.sh）→ True；没成 → Bad。输出在 stderr。"""
    p = subprocess.run(["bash", os.path.join(HERE, "archive_task.sh"), "--spec", json.dumps(A["spec"])] + (["--dry-run"] if A.get("dry") else []),
                       env=dict(os.environ, TASKQ_DATA=store_dir(A)), stdout=sys.stderr)
    if p.returncode:
        raise Bad(f"归档没成（archive_task.sh 退出码 {p.returncode}）")
    return True


OPS = {k[3:]: v for k, v in globals().items() if k.startswith("op_")}


def main(argv):
    if len(argv) != 1 or argv[0] not in OPS:
        sys.exit("用法：site_ops.py <" + "|".join(sorted(OPS)) + ">（参数是标准输入上的 JSON）")
    out, sys.stdout = sys.stdout, sys.stderr  # 操作里打印的进度（隧道、归档）都到 stderr，标准输出只有结果那一行
    try:
        res = OPS[argv[0]](json.load(sys.stdin))
    except Bad as e:
        sys.exit(f"{argv[0]}: {e}")
    finally:
        sys.stdout = out
    print(proto.JSON_MARK + json.dumps(res), flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
