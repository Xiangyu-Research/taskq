#!/usr/bin/env python3
"""Operations run on an executor or the store: the hub calls them through relay_remote.op, running the bundle in that
machine's root (local machines too). Root R is TASKQ_ROOT; store operations also get the data dir (data). Operations import
only what they need, and progress goes to stderr.

    python3 site_ops.py <op>    args are one JSON object on stdin; the result is one line starting with proto.JSON_MARK;
                                on failure: non-zero exit, error on stderr. Operations: status, states, missing, pin, ingest,
                                view, list, drop, gc, env_mark, done, probe, rm, export, pull, collect, facts, smoke, archive, push
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
    """Bad arguments or state: report to the hub instead of treating it as unreachable."""


def root():
    r = os.environ.get("TASKQ_ROOT")
    if not r:
        raise Bad("TASKQ_ROOT is not set")
    return os.path.realpath(os.path.expanduser(r))


def sub(R, k):
    return os.path.join(R, proto.ROOT[k])


def rel_ok(r, depth=1):
    """A path relative to the data dir: not absolute, no .., at least depth levels."""
    if ".." in str(r).split("/"):  # before normalising: a/b/.. would become a, which is not what was written
        raise Bad(f"bad path: {r}")
    r = os.path.normpath(r)
    if r.startswith("/") or len(r.split("/")) < depth:
        raise Bad(f"bad path: {r}")
    return r


def exists(p):
    """The path exists (also after following symlinks)."""
    try:
        os.stat(p)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False


def done_missing(D, out, done):
    """proto.done_missing for files under data dir D / out (done: proto.done_items form)."""
    return proto.done_missing(done, lambda a: exists(os.path.join(D, out, a)))


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
    return proto.BLOB_STAMP.format(size=x.st_size, mtime_ns=x.st_mtime_ns)


def record(R, h):
    """Record the blob's current size and mtime (its sha256 was just verified)."""
    C.atomic_write(blob_path(R, h) + proto.BLOB_STAT, stamp(blob_path(R, h)))


def blob_ok(R, h, verify=True):
    """The blob exists and still matches h: size and mtime equal the recorded values (ingest and push record them once the
    sha256 is checked, push while receiving); without a record compute sha256 once and record it (verify=False: report it
    as not ok and leave it, so the hashing happens in ingest with its long timeout). On a mismatch (a hard-link source was
    modified in place, or a bad transfer) remove the blob and its record (the source is left alone) and return False."""
    p = blob_path(R, h)
    if not os.path.isfile(p):
        return False
    try:
        rec = open(p + proto.BLOB_STAT).read().strip()
    except OSError:
        rec = None
    if rec is not None and rec == stamp(p):
        return True
    if rec is None and not verify:
        return False
    if rec is None and C.sha256(p) == h:
        record(R, h)
        return True
    for x in (p, p + proto.BLOB_STAT):
        if os.path.lexists(x):
            os.remove(x)
    print(f"blob {h[:12]} changed since ingest, dropping it to re-ingest", file=sys.stderr)
    return False


def op_missing(A):
    """Blobs not present with a matching record. No hashing here (short timeout); unrecorded blobs are checked by ingest."""
    R = root()
    return [h for h in A if not blob_ok(R, h, verify=False)]


def op_pin(A):
    p = os.path.join(sub(root(), "pins"), A["key"] + ".json")
    if A.get("hashes") is None:
        if os.path.exists(p):
            os.remove(p)
    else:
        C.atomic_write(p, json.dumps(A["hashes"]))
    return 1


def op_ingest(A):
    """files {h: [relative path, size]}, srcs: data dirs to look in. A blob already there without a record is sha256-checked
    and recorded (dropped on a mismatch). Otherwise same disk (st_dev), matching size and sha256 → hard link into blobs and
    record it."""
    R = root()
    B = sub(R, "blobs")
    bdev = os.stat(B if os.path.isdir(B) else R).st_dev
    got = []
    for h, (rel, size) in A["files"].items():
        if blob_ok(R, h):
            got.append(h)
            continue
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


# ---------------- views ----------------


def op_view(A):
    """views: this task's view dirs; links {relative path: h}; texts {relative path: base64}; clean: dirs whose stale links
    should be removed (default views). When the input path already holds a real file: if it is the blob itself (same inode,
    output of a previous step ingested on the same disk) or has the same content (a file another task wrote over there, such
    as a training's copy of a shared input) leave it; otherwise fail without overwriting: that view keeps
    .incomplete (not built; cleanup will not remove real files under it), while other views build as usual and lose .incomplete."""
    R = root()
    D = sub(R, "data")
    L = lock(R)
    views = [rel_ok(v, 2) for v in A["views"]]
    for h in A["links"].values():
        if not blob_ok(R, h):
            raise Bad(f"missing blob {h}")
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
            if not os.path.samefile(p, t) and not (os.path.isfile(p) and os.path.getsize(p) == os.path.getsize(t) and C.sha256(p) == h):
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
        raise Bad("a view path already holds another real file, not overwriting: " + ", ".join(clash[:5]))
    return len(want) - len(incs)


# ---------------- fetch ----------------
def raise_(e):
    raise e


def op_list(A):
    """out: the main dir; outs: dirs to fetch (out and the rest); done; exclude: file names not to fetch; extra: files in out's
    parent to fetch too; key: part logs; links: this task's views {relative path: sha256} (recorded at stage time).
    → dict(done, missing, items=[[relative path, size, mtime ns]], links=[[relative path, target relative to the data dir]], bad=[relative path]).
    Every symlink in the outputs becomes the logical path it points at (relative to the data dir): paths into the data dir
    (outputs, inputs in views) by path; links to a blob (an unpacked input) map back through this task's views to that input's
    path; links elsewhere or impossible to map → bad."""
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
        for dp, dns, fs in os.walk(top, onerror=raise_):  # never silently skip unreadable subdirs (would count a partial fetch as done)
            for f in fs + [d for d in dns if os.path.islink(os.path.join(dp, d))]:
                p = os.path.join(dp, f)
                if any(fnmatch.fnmatch(f, x) for x in A["exclude"]) or f.endswith(proto.TMP):  # excluded names: skip files and links alike
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
    """paths: paths to remove (relative to the data dir, at least two levels); remove empty parents too. logs: prefix of part
    log names (safe key)."""
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


# ---------------- status ----------------
def versioned(args, inp=None):
    """A versioned command in the same bundle (taskq.py status --json, spec.py collect --json) → its result without version."""
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
    """Bytes free on path's disk; with cap (GB), at most cap minus what top (default path) already uses."""
    free = shutil.disk_usage(path).free
    if not cap:
        return free
    return min(free, max(0, int(cap * 1e9) - C.du([top or path])[0]))


def env_states(R, A):
    ed = sub(R, "envs_ok")
    sfx = proto.ENV_MARK["ok"]
    oks = {f[:-len(sfx)]: open(os.path.join(ed, f)).read().strip() for f in (os.listdir(ed) if os.path.isdir(ed) else []) if f.endswith(sfx)}
    st = {}
    er = os.path.expanduser(A.get("env_root") or sub(R, "envs"))
    for n in A.get("envs") or []:
        d = st[n] = dict(active=os.path.exists(os.path.join(er, n, ".activate")), log="")
        for x in ("ok", "failed", "building"):
            try:
                d[x] = open(os.path.join(ed, n + proto.ENV_MARK[x])).readline().strip()
            except OSError:
                d[x] = ""
        if d["failed"]:
            try:
                d["log"] = open(os.path.join(ed, n + proto.ENV_MARK["log"]), "rb").read()[-2000:].decode("utf-8", "replace")
            except OSError:
                pass
    return oks, st


def executor_type(t):
    """Executor type t's module (sites/executors.py; it reads config and plugins, so only status loads it)."""
    if core_path("sites") not in sys.path:
        sys.path.insert(0, core_path("sites"))
    import executors
    return executors.get(t)


def op_status(A):
    """A: sched (the executor type), its status_args, cap, blob_cap, env_root, envs."""
    R = root()
    st = local_status()
    sp = [{k: t["spec"].get(k) for k in proto.COLLECT_FIELDS} for t in st["tasks"] if t.get("spec")]
    col = versioned(["spec.py", "collect", "--json"], json.dumps(dict(specs=sp, root=sub(R, "data"), wait=None)))
    try:
        raw = executor_type(A.get("sched")).read_status(A)
    except Exception:
        raw = None
    oks, envs_st = env_states(R, A)
    free = capped_free(R, A.get("cap"))
    out_used = C.du([sub(R, "data")])[0]  # outputs tasks have already written (views are links, not counted): offsets space reserved for them when choosing an executor
    cdir = sub(R, "code")
    live = [d for d in (os.listdir(cdir) if os.path.isdir(cdir) else []) if ".tmp" not in d and os.path.isdir(os.path.join(cdir, d))
            and time.time() - os.stat(os.path.join(cdir, d)).st_mtime < proto.STALE_BEAT]
    B = os.path.realpath(sub(R, "blobs"))
    dev = os.stat(R).st_dev
    bdev = os.stat(B).st_dev if os.path.isdir(B) else dev
    bfree = free
    if bdev != dev:
        bfree = capped_free(B, A.get("blob_cap"))
    return dict(taskq=st, tasks=col.get("tasks"), seglogs=col.get("seglogs") or [], sched_raw=raw, free=free, dev=dev, envs_ok=oks, envs_st=envs_st, now=int(time.time()),
                out_used=out_used, blob_dev=bdev, blob_free=bfree,
                bundle=os.path.basename(os.path.realpath(sub(R, "current"))), live=live)


# ---------------- cleanup ----------------
_PINS = [None, set()]  # (pins dir, inode, mtime ns) and what was read: do not reread when unchanged (cleanup is slow with hundreds of blobs and dozens of pins)


def pins(R):
    """Pinned blobs (pins/*.json written within PIN_TTL). Always reread when the dir changed in the last 2 seconds (writes may
    still be landing in the same mtime tick)."""
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
    """When free space (free / blob_free, measured if absent) is below soft_gb: first remove views in views and under scan that
    are not in keep, then remove blobs least recently used first (not referenced by a view, not pinned, only one link).
    orphans (seconds, the store only, whose blobs are hard links of its data dir): whatever the free space, also remove such
    blobs whose ctime (when their other link went away) is at least this old.
    → dict(freed, removed_views, removed_blobs, free[, blob_free])."""
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
    orphans = A.get("orphans")
    if (bfree() < soft or orphans is not None) and blobs_ok:
        now = time.time()
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
                    os.remove(p)  # record for a blob that is already gone
                    continue
                if not HASH_RE.fullmatch(f) or os.path.dirname(p) != os.path.dirname(blob_path(R, f)):
                    h = f.split(".", 1)[0]
                    if "." in f and HASH_RE.fullmatch(h) and os.path.dirname(p) == os.path.dirname(blob_path(R, h)) \
                            and time.time() - os.lstat(p).st_mtime > proto.TMP_KEEP and not f.endswith(proto.BLOB_STAT):
                        os.remove(p)  # left behind by a half-done transfer (<h2>/<h>.<suffix>); touch nothing outside the blob dirs or not named by hash
                    continue
                if os.path.realpath(p) in used or os.path.islink(p) or f in pinned:
                    continue
                try:
                    x = os.lstat(p)
                except OSError:
                    continue
                if x.st_nlink != 1:
                    continue
                cand.append((max(x.st_atime, x.st_mtime), p, x.st_size, f, x.st_ctime))
        for _, p, sz, h, ct in sorted(cand):
            if bfree() >= soft and (orphans is None or now - ct < orphans):
                continue
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


# ---------------- env ----------------
def op_env_mark(A):
    """Before building an env: mode=reserve checks free space on env_root's disk against need (bytes) (cap: based on what the
    root already uses); enough → write <env>.building, not enough → .failed. mode=clear removes the .building written this
    time. sd: marker dir."""
    E, SD, n = os.path.expanduser(A["env_root"]), os.path.expanduser(A["sd"]), A["env"]
    os.makedirs(SD, exist_ok=True)
    if A["mode"] == "clear":
        p = os.path.join(SD, n + proto.ENV_MARK["building"])
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
    with open(os.path.join(SD, n + proto.ENV_MARK["building" if ok else "failed"]), "w") as f:
        f.write((proto.ENV_BUILDING_FMT % (A["hash"], now) if ok else proto.ENV_NOSPACE_FMT % (A["hash"], now, A["need"], free)) + "\n")
    return dict(ok=ok, free=free, need=A["need"])


# ---------------- store data dir ----------------
def store_dir(A):
    return os.path.realpath(os.path.expanduser(A["data"]))


def op_done(A):
    """items {name: dict(out, done)} → {name: missing done items} ([] = done)."""
    D = store_dir(A)
    return {k: done_missing(D, rel_ok(v["out"]), v["done"]) for k, v in A["items"].items()}


def matches(D, pat):
    """A glob relative to the data dir → matching paths (a dir counts as everything under it)."""
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
    """pats: globs (relative to the data dir); size: whether to measure size (how much to transfer: symlinks count their
    target, hard links once) → {glob: dict(n=matches, newest=latest mtime or None, size=bytes or None)}."""
    D = store_dir(A)
    res = {}
    for pat in A["pats"]:
        got = matches(D, pat)
        res[pat] = dict(n=len(got), newest=newest(got), size=C.du(got, follow=True)[0] if A.get("size") else None)
    return res


def op_rm(A):
    """pats: globs of files to delete (relative to the data dir; write dir/** for everything under a dir) → dict(n=files removed,
    bytes=freed, dirs=matched directories left because they still hold something). Removes files and links only, never a directory
    with something in it and nothing outside the data dir; a matched directory that is empty afterwards goes too."""
    D = store_dir(A)
    n, freed, dirs = 0, 0, set()
    for pat in A["pats"]:
        for p in matches(D, pat):
            if not C.inside(p, D) or os.path.realpath(p) == D:
                continue
            if os.path.isdir(p) and not os.path.islink(p):
                dirs.add(os.path.normpath(p))
                continue
            x = os.lstat(p)
            freed += x.st_blocks * 512 if stat.S_ISREG(x.st_mode) and x.st_nlink == 1 else 0
            os.remove(p)
            n += 1
    left = 0
    for d in sorted(dirs, key=len, reverse=True):  # deepest first, so a dir whose subdirs all went goes too
        try:
            os.rmdir(d)
        except OSError:
            left += 1
    return dict(n=n, bytes=freed, dirs=left)


# ---------------- machines with files to send (store data, hub wheelhouse): push to executors, fetch, archive ----------------
def core_path(*parts):
    return os.path.join(os.path.dirname(HERE), *parts)


def mods():
    """Scheduler modules needed only for transfers and spec resolution (names, spec, mtab, tunnel; they read config and
    plugins): import lazily so executor operations do not need them."""
    for d in ("sites", "sync"):
        if core_path(d) not in sys.path:
            sys.path.insert(0, core_path(d))
    import mtab
    import names
    import spec
    import tunnel
    return mtab, names, spec, tunnel


def task_call(f, *a, **kw):
    """Errors from specs and plugins (RelayError, SpecError) → Bad: the hub gets the reason."""
    _, N, S, _ = mods()
    try:
        return f(*a, **kw)
    except (N.RelayError, S.SpecError) as e:
        raise Bad(str(e)) from None


def reach(name):
    """From this machine to executor name: → (ssh name, None for local; its root). Machines with ssh=None all live on the hub
    machine and see each other as local."""
    M = mods()[0]
    m, me = M.machine(name), M.whoami()
    if name == me or m["ssh"] is None:
        if name != me and (me is None or M.machine(me)["ssh"] is not None):
            raise Bad(f"{name} is on the hub machine (ssh is None); {me} cannot reach it")
        return None, m["root"].rstrip("/")
    return m["ssh"], m["root"].rstrip("/")


def hashes(R, paths):
    """Real paths → sha256; cached by (path, size, mtime) in state/hashes.db under the root: unchanged files are not read again."""
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
    """spec: a resolved spec → what sending this task needs (sites/names.py export, on the store data dir):
    dict(params, views, outs, cleanup, texts={relative path: base64},
         files=[[relative path, sha256, bytes, path relative to the data dir (None if outside), real path on the store]]))."""
    N = mods()[1]
    D = store_dir(A)
    ex = task_call(N.export, A["spec"], D)
    hs = hashes(root(), sorted({p for _, p in ex["files"]}))
    files = [[rel, hs[p], os.path.getsize(p), os.path.relpath(p, D) if C.inside(p, D) else None, p] for rel, p in ex["files"]]
    texts = {k: base64.b64encode(v if isinstance(v, bytes) else v.encode()).decode() for k, v in ex["texts"].items()}
    return dict(params=ex["params"], views=ex["views"], outs=ex["outs"], cleanup=ex["cleanup"], texts=texts, files=files)


def op_push(A):
    """to: executor; files {sha256: local path} → put them in the remote blobs (copy locally on the same machine, hard link when
    possible, otherwise over the tunnel) → sha256s not transferred or mismatched (already removed). The receiving side checks
    each file's sha256 in the read that checks its MD5 and records it (proto.BLOB_STAT), so later operations there (missing,
    view) compare size and mtime instead of reading hundreds of GB again within their timeout."""
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
        bad = T.push(host, tmp, f"{troot}/{proto.ROOT['blobs']}", sorted(rels.values()), site=A["to"],
                     sums={rels[h]: h for h in A["files"]}, stat=(proto.BLOB_STAT, proto.BLOB_STAMP))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    back = {v: k for k, v in rels.items()}
    return [back[r] for r in bad]


def place(src, D, written, sp, links=()):
    """Move verified results (under src, same relative paths as the data dir) into data dir D, markers last: regular files →
    files matching fetch.extra in out's parent (keep existing ones) → output symlinks (links [(relative path, target relative
    to the data dir)], written as relative links) → flush them and their dirs to disk → MANIFEST.json → done markers (in
    reverse done order, first item last) → flush the markers' dirs. After a power cut a marker never points at missing data."""
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

    def dirs(rels):  # every dir from D down to each path's dir: new dirs' entries live in their parents
        out = {D}
        for r in rels:
            d = os.path.dirname(r)
            while d:
                out.add(os.path.join(D, d))
                d = os.path.dirname(d)
        return sorted(out)
    moved = list(first)
    for w in first:
        mv(w)
    for w in parent:
        if not os.path.exists(os.path.join(D, w)):
            mv(w)
            moved.append(w)
    for rel, to in links:
        p = os.path.join(D, rel)
        C.atomic_link(os.path.relpath(os.path.join(D, to), os.path.dirname(p)), p)
    C.fsync_paths([os.path.join(D, w) for w in moved] + dirs(moved + [r for r, _ in links] + man + done))
    for w in man + done:
        mv(w)
    C.fsync_paths(dirs(man + done))


def op_pull(A):
    """frm: executor; spec; items [[relative path, bytes, mtime ns]], links [[relative path, target relative to the data dir]]:
    outputs listed by list over there → fetch into .relay_in/<safe key> under the store data dir (skip files from a previous
    partial fetch with matching size and mtime), verify against the spec (names.import_), then place them in the data dir
    (place, markers last) → dict(files, links). The listing comes from the executor: a path that is not one of this task's
    results (spec.owns; links only inside the output dirs) stops the fetch before anything is written."""
    _, N, S, T = mods()
    D = store_dir(A)
    sp = A["spec"]
    o = S.outputs(sp)
    links = [(rel_ok(r), rel_ok(t)) for r, t in A["links"]]
    off = [r for r, _ in links if not any(C.under(r, x) for x in o["outs"])]
    if off:
        raise Bad(f"links for {sp['key']} are not inside the output dirs: {off[:3]}")
    off = [i[0] for i in A["items"] if not S.owns(o, rel_ok(i[0]))]
    if off:
        raise Bad(f"files listed for {sp['key']} are not its results: {off[:3]}")
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
        raise Bad(f"{len(bad)} files were not fetched ({bad[:3]})")
    os.makedirs(inbox, exist_ok=True)
    written = task_call(N.import_, sp, inbox, out, D, links=[r for r, _ in links])
    place(out, D, written, sp, links)
    for d in (inbox, out):
        shutil.rmtree(d, ignore_errors=True)
    return dict(files=len(written), links=len(links))


def op_collect(A):
    """specs → how these tasks look in the store data dir (spec.py collect, the same as in executor status); wait: max seconds
    to wait."""
    return versioned(["spec.py", "collect", "--json"], json.dumps(dict(specs=A["specs"], root=store_dir(A), wait=A.get("wait"))))


def op_facts(A):
    """items [dict(spec, since, tags)] → results and logs for each task in the store data dir (spec.facts: the same as the
    executor queue computes locally); the hub queue uses it to decide state."""
    S = mods()[2]
    return S.facts(A["items"], store_dir(A))


def op_smoke(A):
    """recipe, names: params and vars that are functions in the smoke definition (SMOKE in the plugin's smoke.py), evaluated on
    the store data dir → {field: value}; failing fields → {"error": {field: reason}}."""
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
    """spec, dry: back up this task's store results to the backup machine and verify (archive_task.sh) → True; failure → Bad.
    Output goes to stderr."""
    p = subprocess.run(["bash", os.path.join(HERE, "archive_task.sh"), "--spec", "-"] + (["--dry-run"] if A.get("dry") else []),
                       input=json.dumps(A["spec"]), text=True,  # on stdin: a spec (tokens in vars) never goes on a command line
                       env=dict(os.environ, TASKQ_DATA=store_dir(A)), stdout=sys.stderr)
    if p.returncode:
        raise Bad(f"archive failed (archive_task.sh exit code {p.returncode})")
    return True


OPS = {k[3:]: v for k, v in globals().items() if k.startswith("op_")}


def main(argv):
    if len(argv) != 1 or argv[0] not in OPS:
        sys.exit("usage: site_ops.py <" + "|".join(sorted(OPS)) + "> (args are JSON on stdin)")
    out, sys.stdout = sys.stdout, sys.stderr  # progress (tunnel, archive) goes to stderr; stdout carries only the result line
    try:
        res = OPS[argv[0]](json.load(sys.stdin))
    except Bad as e:
        sys.exit(f"{argv[0]}: {e}")
    finally:
        sys.stdout = out
    print(proto.JSON_MARK + json.dumps(res), flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
