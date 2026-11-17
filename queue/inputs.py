#!/usr/bin/env python3
"""Task inputs: check they are all present before starting; when the executor allows it, build a task-private view on the
node's local disk and copy blobs there in the background.

    inputs.py prepare <data dir> <task name> <local root or empty> <input>...
        Check every input (file or dir; .incomplete at any level = half-built, unreadable counts as missing) and print the data
        dir, or build a links-only private view under <root>/views/<task name>/ plus <root>/views/<task name>.todo.json and print
        the view root. Missing inputs → exit 4 with a count and examples on stderr.
    inputs.py copy <local root> <task name>
        Copy the listed blobs under <root>/blobs/, verify sha256, and repoint the view at the local copies, keeping at least
        STAGE_KEEP_GB (default 300) GB free (hard links from other jobs on this node do not count). Copies on one node are
        serialized by a lock; a mismatch or failure keeps reading the original.
Only blobs are copied; other files are read in place. Exit codes: 0 ok; 4 missing inputs; anything else = error (run_task.sh
reads in place in that case).
The local root and other jobs' dirs sit in a shared temp dir with predictable names: the root must be a private dir of this
account (created 0700; one made by another account or a planted link means reading in place), only this account's dirs are
reused or removed, and a local blob is used only while its sha256 is known to match: its record (<blob>.st, size and mtime,
written right after the check) still holds, or its sha256 is computed again (in the background copy, never before the start).
"""
import fcntl
import glob
import json
import os
import shutil
import stat
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import proto  # noqa: E402
import common as C  # noqa: E402
import site_ops as SO  # noqa: E402

MISSING = proto.EXIT["run_missing"]
PAR = int(os.environ.get("STAGE_PAR", 16))
KEEP_GB = float(os.environ.get("STAGE_KEEP_GB", 300))


def entries(data, inp):
    """One input → ([(path relative to the data dir, absolute path)], [missing descriptions]). A glob expands to matching files;
    no matches counts as missing."""
    rel = os.path.relpath(inp, data)
    if rel.startswith(".."):
        return [], [f"{inp} is not inside the data dir"]
    if glob.has_magic(rel):
        hits = sorted(glob.glob(inp))
        if not hits:
            return [], [f"{rel} (no matches)"]
        out, miss = [], []
        for h in hits:
            e, m = entries(data, h)
            out += e
            miss += m
        return out, miss
    if os.path.isdir(inp):
        out, bad = [], []
        for d, ds, fs in os.walk(inp, onerror=lambda e: bad.append(f"{os.path.relpath(e.filename, data)} (unreadable)"), followlinks=True):
            if proto.INCOMPLETE in fs or proto.INCOMPLETE in ds:  # half-built at some level
                return [], [f"{os.path.relpath(d, data)} (half-built)"]
            for f in fs:
                p = os.path.join(d, f)
                out.append((os.path.relpath(p, data), p))
        return ([], bad) if bad else (out, [])
    if os.path.lexists(inp):
        return [(rel, inp)], []
    return [], [f"{rel} (missing)"]


def look(p):
    """→ (size, real path, sha256 or None); does not resolve to a file → None. sha256: h when the real path is a blob
    (<h2>/<h>)."""
    try:
        st = os.stat(p)
    except OSError:
        return None
    if not os.path.isfile(p):
        return None
    real = os.path.realpath(p)
    h = os.path.basename(real)
    is_blob = SO.HASH_RE.fullmatch(h) and os.path.basename(os.path.dirname(real)) == h[:2]
    return st.st_size, real, h if is_blob else None


def live_jobs():
    """Job ids this account still has in Slurm; cannot check → None (then do not remove other jobs' dirs).
    Array subjobs show %i as <array id>_<index> while SLURM_JOB_ID (the local dir name) is %A; accept both."""
    if not shutil.which("squeue"):
        return None
    try:
        r = subprocess.run(["squeue", "-h", "-u", os.environ.get("USER", ""), "-o", "%A %i"], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None
    if r.returncode:
        return None
    return {x.split("_")[0] for x in r.stdout.split()}


def jobs_dirs(root):
    """Local copies of this account's jobs on this node (in the same parent dir; real dirs owned by this account, another
    account's dirs and links are never used or removed) → {job id: dir}."""
    out = {}
    for d in glob.glob(os.path.join(os.path.dirname(root), proto.STAGE_DIR_PREFIX + "*")):
        j = os.path.basename(d)[len(proto.STAGE_DIR_PREFIX):]
        try:
            st = os.lstat(d)
        except OSError:
            continue
        if j.isdigit() and stat.S_ISDIR(st.st_mode) and st.st_uid == os.getuid():
            out[j] = d
    return out


def clean_old(root):
    live = live_jobs()
    if live is None:
        return
    for j, d in jobs_dirs(root).items():
        if d != root and j not in live:
            print(f"removing local copy {d} of ended job {j}", file=sys.stderr)
            shutil.rmtree(d, ignore_errors=True)


def _record(p):
    """The record next to blob p (proto.BLOB_STAT: size and mtime when its sha256 was checked), when it is a file of this account."""
    try:
        fd = os.open(p + proto.BLOB_STAT, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        return None
    try:
        return os.read(fd, 256).decode(errors="replace").strip() if os.fstat(fd).st_uid == os.getuid() else None
    finally:
        os.close(fd)


def usable(p, h, size, check):
    """Blob p can stand for content h: a regular file of this account (not a link) of size bytes, writable by nobody else, whose
    sha256 is known to match: its record holds its current size and mtime, or (check) its sha256 is h now → its stamp (size and
    mtime, proto.BLOB_STAMP), else None. A file of ours that others could write is closed first when checking (a record from
    before does not count)."""
    try:
        st = os.lstat(p)
    except OSError:
        return None
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_size != size:
        return None
    stamp = proto.BLOB_STAMP.format(size=st.st_size, mtime_ns=st.st_mtime_ns)
    if st.st_mode & 0o022:
        if not check:
            return None
        os.chmod(p, stat.S_IMODE(st.st_mode) & ~0o022)
    elif _record(p) == stamp:
        return stamp
    return stamp if check and C.sha256(p) == h else None


def private_root(root):
    """The local root is a private dir of this account → True; else say so (inputs are read in place) → False."""
    if C.private_dir(root):
        return True
    print(f"local copy dir {root} is not a private dir of this account (another account made it, or it is a link); reading inputs in place",
          file=sys.stderr)
    return False


def prepare(data, key, root, inputs):
    data = os.path.abspath(data)
    found, miss = [], []
    for inp in inputs:
        e, m = entries(data, os.path.abspath(inp))
        found += e
        miss += m
    with ThreadPoolExecutor(PAR) as ex:
        info = list(ex.map(lambda e: look(e[1]), found))
    miss += [f"{r} (does not resolve to a file)" for (r, _), x in zip(found, info) if x is None]
    if miss:
        print(f"missing inputs: {len(miss)} ({'; '.join(miss[:5])}{' …' if len(miss) > 5 else ''})", file=sys.stderr)
        return MISSING
    root = os.path.abspath(root) if root else ""
    if not root or not private_root(root):
        print(data)
        return 0
    clean_old(root)
    view = os.path.join(root, "views", key)
    shutil.rmtree(view, ignore_errors=True)
    todo = []
    for (rel, _), (size, real, h) in zip(found, info):
        p = os.path.join(view, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        local = os.path.join(root, proto.blob(h)) if h else None
        if local and usable(local, h, size, check=False):  # checked earlier (record holds); otherwise the copy checks it
            os.symlink(local, p)
        else:
            os.symlink(real, p)
            if h:
                todo.append([rel, real, size, h])
    C.atomic_write(view + ".todo.json", json.dumps(todo))
    print(view)
    return 0


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def copy(root, key):
    t0 = time.time()
    root = os.path.abspath(root)
    if not private_root(root):
        return 0
    view = os.path.join(root, "views", key)
    todo = json.load(open(view + ".todo.json"))
    for p in glob.glob(os.path.join(root, "blobs", "*", "*" + proto.TMP)):  # left by a stopped copy (<h>.<pid>.tmp)
        try:
            if not alive(int(p.rsplit(".", 2)[1])):
                os.remove(p)
        except (ValueError, IndexError, OSError):
            pass
    by = {}
    for rel, real, size, h in todo:
        by.setdefault(h, [real, size, []])[2].append(rel)
    others = [d for d in jobs_dirs(root).values() if d != root]
    lock = os.path.join(os.path.dirname(root), f".{proto.STAGE_DIR_PREFIX}{os.getuid()}.lock")
    try:
        lk = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError as e:
        print(f"cannot open the copy lock {lock} ({e}); not copying, reading in place")
        return 0
    try:
        if os.fstat(lk).st_uid != os.getuid():
            print(f"the copy lock {lock} belongs to another account; not copying, reading in place")
            return 0
        fcntl.flock(lk, fcntl.LOCK_EX)  # one copy at a time on this node: no other copy between the space check and the copy
        need = sum(size for h, (_, size, _) in by.items()
                   if not usable(os.path.join(root, proto.blob(h)), h, size, check=False)
                   and not any(usable(os.path.join(o, proto.blob(h)), h, size, check=False) for o in others))
        free = shutil.disk_usage(root).free
        if free - need < KEEP_GB * 1024 ** 3:
            print(f"local disk has {free / 1024 ** 3:.0f} GB; copying {need / 1024 ** 3:.0f} GB would leave less than {KEEP_GB:.0f} GB, not copying, reading in place")
            return 0
        return _copy(root, view, by, others, t0)
    finally:
        os.close(lk)


def _copy(root, view, by, others, t0):
    """Each blob: already here and known to match (record, else sha256 now), else hard-linked from another job of ours whose copy
    is known to match (the linked file must still have the size and mtime checked), else copied from the original with its sha256
    checked while copying; then closed to others, recorded, and the view repointed. A blob whose content does not match is not used."""
    n = dict(copied=0, linked=0, had=0, bytes=0)
    bad = []

    def one(item):
        h, (real, size, rels) = item
        dst = os.path.join(root, proto.blob(h))
        try:
            if usable(dst, h, size, check=True):
                n["had"] += 1
            else:
                src, stamp = next(((q, s) for q in (os.path.join(o, proto.blob(h)) for o in others)
                                   if (s := usable(q, h, size, check=True))), (None, None))
                if src and C.atomic_copy(src, dst, link=True, size=size) and SO.stamp(dst) == stamp:
                    n["linked"] += 1
                else:
                    C.atomic_copy(real, dst, size=size, sha=h)
                    n["copied"] += 1
                    n["bytes"] += size
                    mode = stat.S_IMODE(os.lstat(dst).st_mode)
                    if mode & 0o022:  # copied with the original's mode: nobody else may write a blob we rely on
                        os.chmod(dst, mode & ~0o022)
            if _record(dst) != SO.stamp(dst):
                SO.record(root, h)
        except OSError as e:
            bad.append(f"{os.path.basename(rels[0])}: {e}")
            return
        for rel in rels:
            C.atomic_link(dst, os.path.join(view, rel))
    with ThreadPoolExecutor(min(PAR, 8)) as ex:
        list(ex.map(one, sorted(by.items(), key=lambda x: x[1][2][0])))
    print(f"{time.strftime('%F %T')} inputs copied to local {view}: {len(by) - len(bad)}/{len(by)} files (new {n['copied']} "
          f"{n['bytes'] / 1024 ** 3:.1f} GB, hard-linked {n['linked']}, already present {n['had']}), {time.time() - t0:.0f} s"
          + (f"; {len(bad)} were not copied, reading in place ({'; '.join(bad[:3])})" if bad else ""))
    return 0


def main(argv):
    if argv[:1] == ["prepare"] and len(argv) >= 4:
        return prepare(argv[1], argv[2], argv[3], argv[4:])
    if argv[:1] == ["copy"] and len(argv) == 3:
        return copy(argv[1], argv[2])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
