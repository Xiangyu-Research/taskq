"""Small helpers shared by scheduler scripts, standard library only (executors use the system python3)."""
import json
import os


def _proto():
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import proto
    return proto


def _cfg():
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import cfg
    return cfg


def tmp_of(path):
    """Name of a half-written path (same dir, with pid, ending in proto.TMP): rename over it when done."""
    return f"{path}.{os.getpid()}{_proto().TMP}"


def atomic_put(path, make):
    """make(tmp) builds a file or link at a temp name in the same dir, then moves it to path: path is either the old or the new one. If make raises, the exception propagates and path is untouched."""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = tmp_of(path)
    try:
        if os.path.lexists(tmp):
            os.remove(tmp)
        make(tmp)
        os.replace(tmp, path)
    finally:
        if os.path.lexists(tmp):
            os.remove(tmp)


def atomic_write(path, data):
    """Write the whole payload to a temp name, fsync, then move it over: after a power cut or kill the file is either old or new, never half-written. data: str or bytes."""
    def make(tmp):
        with open(tmp, "wb") as f:
            f.write(data.encode() if isinstance(data, str) else data)
            f.flush()
            os.fsync(f.fileno())
    atomic_put(path, make)


def atomic_link(target, path):
    """Replace path with a symlink to target."""
    atomic_put(path, lambda tmp: os.symlink(target, tmp))


def atomic_copy(src, dst, link=False, size=None, sha=None):
    """Put src at dst: with link=True try a hardlink first (same disk only), else copy (with mtime); write to a temp name,
    check size (size) and sha256 (sha, computed while copying), then move it over. Mismatch -> OSError, dst untouched.
    Returns whether it is a hardlink."""
    import hashlib
    import shutil
    linked = []

    def make(tmp):
        if link:
            try:
                os.link(src, tmp)
                linked.append(1)
            except OSError:
                pass
        if not linked:
            h = hashlib.sha256() if sha else None
            with open(src, "rb") as fi, open(tmp, "wb") as fo:
                for b in iter(lambda: fi.read(1 << 22), b""):
                    fo.write(b)
                    if h:
                        h.update(b)
            shutil.copystat(src, tmp)
            if h and h.hexdigest() != sha:
                raise OSError(f"{src}: content does not match sha256 {sha[:12]}...")
        elif sha and sha256(tmp) != sha:
            raise OSError(f"{src}: content does not match sha256 {sha[:12]}...")
        got = os.path.getsize(tmp)
        if size is not None and got != size:
            raise OSError(f"{src}: size {got}, expected {size}")
    atomic_put(dst, make)
    return bool(linked)


def write_json(path, obj, indent=1):
    atomic_write(path, json.dumps(obj, ensure_ascii=False, indent=indent))


def sha256(path):
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 22), b""):
            h.update(b)
    return h.hexdigest()


def inside(path, base):
    """path (after resolving symlinks) is base itself or under it."""
    p, b = os.path.realpath(path), os.path.realpath(base)
    return p == b or p.startswith(b.rstrip("/") + "/")


def under(p, d):
    """Relative path p equals d or is under d (by path components, symlinks not resolved)."""
    return p == d or p.startswith(d.rstrip("/") + "/")


def du(paths, skip_top=(), follow=False):
    """Disk used (bytes) by paths (files or dirs): hardlinks count once -> (total, the part outside skip_top).
    skip_top: top-level names under the first path (the root's blobs, code) are not counted in the second number.
    follow: symlinks count as their target's content (how much to transfer: the content is transferred); otherwise not."""
    used, rest, seen = 0, 0, set()
    for top in paths:
        if os.path.isfile(top) and (follow or not os.path.islink(top)):
            walk = [(os.path.dirname(top), [], [os.path.basename(top)])]
        else:
            walk = os.walk(top, followlinks=follow)
        for dp, _, fns in walk:
            first = os.path.relpath(dp, top).split(os.sep)[0]
            for f in fns:
                p = os.path.join(dp, f)
                try:
                    x = os.stat(p) if follow else os.lstat(p)
                except OSError:
                    continue
                if not os.path.isfile(p) or os.path.islink(p) and not follow:
                    continue
                if (x.st_dev, x.st_ino) in seen:
                    continue
                seen.add((x.st_dev, x.st_ino))
                used += x.st_blocks * 512
                if first not in skip_top:
                    rest += x.st_blocks * 512
    return used, rest


# ---------------- Run commands (locally or over ssh) ----------------
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]


def remote_timeout(cmd, timeout):
    """Wrap a command run over ssh in that side's timeout: after this side times out and disconnects, a process stuck in storage there also ends instead of lingering on the login node."""
    return f"timeout -k 10 {max(int(timeout) - 15, 10)} {cmd}"


def run(cmd, host=None, timeout=120, input=None, ssh=None):
    """Run a command -> (exit code, stdout, stderr). A list runs locally; a string runs via bash -c locally, or on host (ssh name)
    when given. Timeout or failed to start -> (124, "", reason). A broken ssh returns 255. Remote commands are also limited there
    (remote_timeout), so no stuck process is left behind."""
    import shlex
    import subprocess
    if isinstance(cmd, str):
        cmd = ["bash", "-c", cmd] if host is None else (ssh or SSH) + [host, remote_timeout(f"bash -c {shlex.quote(cmd)}", timeout)]
    elif host is not None:
        raise ValueError("list commands run locally only")
    try:
        r = subprocess.run(cmd, input=input, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except Exception as e:  # noqa: BLE001
        return _proto().EXIT["timeout"], "", str(e)


def pueue_time(x):
    """pueue time (with timezone; the fractional part may have 0, 3, 6 or 9 digits) -> timestamp; missing -> None."""
    import re
    from datetime import datetime
    if not x:
        return None
    m = re.fullmatch(r"(.+T\d\d:\d\d:\d\d)(\.\d+)?(.*)", x)
    return datetime.fromisoformat(m.group(1) + (m.group(2) or "")[:7] + m.group(3)).timestamp() if m else None


def local_time(x):
    """pueue time -> a naive datetime in local time; missing -> None."""
    from datetime import datetime
    ts = pueue_time(x)
    return None if ts is None else datetime.fromtimestamp(ts)


# ---------------- Skip the log when nothing changed ----------------
def changed(name, value):
    """value differs from the recorded one -> record it and return True; same -> False. Stored at last/<name>.json in the state dir (such as ~/.local/state/taskq/last/)."""
    import re
    d = os.path.expanduser(os.environ.get("TASKQ_LAST") or _cfg().state("last", expand=False))  # env var for tests only
    p = os.path.join(d, re.sub(r"[^\w.-]", "_", name) + ".json")
    try:
        if json.load(open(p)) == value:
            return False
    except (OSError, ValueError):
        pass
    write_json(p, value)
    return True


# ---------------- task records: the only way to know who a pueue task the core added (transfer, fetch, archive, env build in the hub core group) is ----------------
def tasks_path():
    return os.path.expanduser(os.environ.get("TASKQ_TASKS") or _cfg().state("tasks.json", expand=False))  # env var for tests only


def load_records():
    try:
        d = json.load(open(tasks_path()))
        return d if isinstance(d, list) else []
    except (OSError, ValueError):
        return []


def update_records(fn):
    """fn(records) -> new list, under a lock read-modify-write -> new list."""
    import fcntl
    p = tasks_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p + ".lock", "a") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        recs = fn(load_records())
        write_json(p, recs)
    return recs


def new_record(machine, label, step, key, **kw):
    """Record it before adding the task (id still None) -> the record; fill in the task id with set_record_id after adding."""
    import time
    rec = dict(machine=machine, id=None, label=label, added=time.time(), step=step, key=key,
               **{k: v for k, v in kw.items() if v is not None})
    update_records(lambda recs: recs + [rec])
    return rec


def set_record_id(rec, tid):
    def f(recs):
        for r in recs:
            if r["machine"] == rec["machine"] and r["label"] == rec["label"] and r["added"] == rec["added"]:
                r["id"] = tid
        return recs
    rec["id"] = tid
    update_records(f)


def drop_records(pred):
    update_records(lambda recs: [r for r in recs if not pred(r)])


SKEW = 300  # clock skew between machines
LOST = 3600  # how long to keep records of tasks whose add failed or that cannot be found


def sync_records(pueue, recs, now=None):
    """Each machine's pueue (qs --json) + records -> new records (not written): a missing task id is recovered by "same machine,
    same label, created no earlier than the record"; records for tasks that are gone are dropped. Records of unreadable machines
    are left alone."""
    import time
    now = now or time.time()
    taken = {(r["machine"], r["id"]) for r in recs if r.get("id") is not None}
    out = []
    for r in recs:
        tasks = (pueue.get(r["machine"]) or {}).get("tasks")
        if tasks is None:
            out.append(r)
            continue
        if r.get("id") is None:
            hit = sorted((t for t in tasks if (t.get("label") or "") == r["label"] and (r["machine"], t["id"]) not in taken
                          and (pueue_time(t.get("created_at")) or 0) >= r["added"] - SKEW), key=lambda t: t["id"])
            if hit:
                r = dict(r, id=hit[0]["id"])
                taken.add((r["machine"], r["id"]))
            elif now - r["added"] > LOST:
                continue
            out.append(r)
            continue
        if any(t["id"] == r["id"] and (t.get("label") or "") == r["label"] for t in tasks):
            out.append(r)
    return out


def identify(machine, task, recs):
    """Who a pueue task on this machine is -> its record dict(step, key, ...) (id and label both match); no record (added by hand) -> None."""
    label = task.get("label") or ""
    return next((r for r in recs if r["machine"] == machine and r.get("id") == task["id"] and r["label"] == label), None)


def backup_down(ok=None):
    """A backup machine (role backup in machines.py) is paused (policy.py's PAUSE) or unreachable this check (ok: the snapshot's
    s["ok"]) -> the reason; usable -> None. When unreachable, do not queue archives so queued ones do not all fail and stick."""
    M = _cfg().mtab()
    for b in M.backups():
        if b == M.hub():
            continue
        if M.paused(b):
            return f"{b} is paused ({M.paused(b)})"
        if ok is not None and ok.get(b) is False:
            return f"{b} is unreachable this time"
    return None
