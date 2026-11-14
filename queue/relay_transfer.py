"""Relay: stage data from the store to an executor, fetch finished results back to the store, archive them to the backup
machine, and drop a remote task and its results. The hub only coordinates: everything that reads or writes data runs on the
store or the executor (relay_remote.op / store_op, queue/site_ops.py), and data moves directly between them, never through the
hub's disk. Remote paths match the store's data dir; data is stored as blobs by sha256 (see relay_remote.py)."""
import contextlib
import fcntl
import json
import os
import shlex
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduler code
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

PIN_EVERY = 3600  # rewrite the pin this often while staging (remote cleanup only honours pins younger than 12 hours)
MOVE_TIMEOUT = 3 * 86400  # max run time for one stage, fetch or archive (the tunnel stops after STALL seconds without progress)


class Gone(RN.RelayError):
    """Neither the experiment table nor the hub queue has this key."""


# ---------------- staging (runs in the hub's wait group) ----------------
def lookup(key):
    s = S.lookup(key)
    if s is None:
        raise Gone(f"no {key} in the hub queue or the experiment table")
    return s if s.get("exp") else S.resolve(s["recipe"], s["params"])


def stage(name, key, nice=None, add=True, say=print):
    """Store data → remote (same relative paths as the store data dir); when add, also add it to the remote queue. → key."""
    real = lookup(key)
    ex = RM.store_op("export", dict(spec=real))
    ids = in_use(name) if add else []
    g = ids and BG.check_all(S.sendable(S.resolve(real["recipe"], ex["params"])), ids)
    if g:  # the bundle in use over there cannot run the spec to send: do not stage or add, keep the claim (queue/bundle_gate.py)
        BG.hold(name, key, *g)
        say(f"the bundle {g[0]} in use on {RM.conf(name)['label']} cannot run the spec for {key}, not sending yet: {g[1]}")
        return None
    links = layout(ex)["links"]
    with pinned(name, key, links.values(), say):  # remote cleanup will not remove these blobs before the view is built
        return _stage(name, key, nice, add, say, real, ex, links)


@contextlib.contextmanager
def pinned(name, key, hashes, say=print):
    """Remote cleanup will not remove these blobs while this runs (R/state/pins/<key>.json); refresh it periodically so it does
    not expire, and clear it on exit."""
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
            say(f"could not clear the pin for {key} ({e}); cleanup stops honouring it after 12 hours")


_tl = threading.local()  # .held: executors whose target_lock this thread holds


@contextlib.contextmanager
def target_lock(name, say=print):
    """Blob work for one executor (missing → ingest → push → view) runs one at a time across hub processes: two stagings
    sharing files would otherwise both send the same blob and one replaces it under the other's check or view. Nested use in
    one thread does not lock again."""
    held = _tl.__dict__.setdefault("held", set())
    if name in held:
        yield
        return
    os.makedirs(ST.DIR, exist_ok=True)
    with open(os.path.join(ST.DIR, proto.STAGE_LOCK.format(proto.safe(name))), "a") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            say(f"another transfer to {RM.conf(name)['label']} is running, waiting for it")
            fcntl.flock(f, fcntl.LOCK_EX)
        held.add(name)
        try:
            yield
        finally:
            held.discard(name)


def ensure_blobs(name, files, say=print, src=None, what=None, n=None):
    """Files on src (default the store) as {sha256: [path relative to src's data dir or None, bytes, path on src]} → they all
    exist in the remote blobs → {sha256: remote path (R/blobs/<first two>/<sha256>, ~ not expanded)}. First ask what is
    missing; for those, try data dirs already on that machine (relay_remote.same_disk_srcs: by relative path, matching size and
    sha256, hard link on the same disk); the rest is sent by src (site_ops push). Caller pins first so cleanup leaves them alone.
    No CPU job slot → RM.NoSlot; not transferred → RelayError. Holds target_lock."""
    with target_lock(name, say):
        return _ensure_blobs(name, files, say, src, what, n)


def _ensure_blobs(name, files, say, src, what, n):
    miss = RM.op(name, "missing", sorted(files))
    say(f"{what or name}: {len(files) if n is None else n} files, {len(miss)} missing over there")
    if miss:
        got = set(RM.op(name, "ingest", dict(srcs=RM.same_disk_srcs(name), files={h: files[h][:2] for h in miss}),
                        timeout=3 * 3600, job=RM.conf(name).get("transfer_job")))
        miss = [h for h in miss if h not in got]
        say(f"ingested {len(got)} from data already over there, {len(miss)} still missing")
    if miss:
        src = src or M.store()
        bad = RM.op(src, "push", dict(to=name, files={h: files[h][2] for h in miss}), timeout=MOVE_TIMEOUT, say=say)
        if bad:
            raise RN.RelayError(f"{len(bad)} files were not transferred or do not match (will retry)")
    return {h: f"{RM.root(name)}/{proto.blob(h)}" for h in files}


def in_use(name):
    """The bundle in use over there (queue/bundle_gate.py); unreachable → []."""
    u = BG.current(name)
    return BG.in_use(name, *u) if u else []


def check_overlap(name, key, paths):
    """The paths this task will place overlap (same, parent or child) with outputs of another unfinished task on the same
    machine → RelayError, do not stage."""
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
        raise RN.RelayError(f"inputs for {key} overlap outputs of unfinished {', '.join(busy)} on {RM.conf(name)['label']}, not staging")


def layout(ex):
    """export result → where its inputs sit over there: dict(links={relative path: sha256}, views, placed (files outside views),
    outs, cleanup), all relative to R/data."""
    links = {rel: h for rel, h, *_ in ex["files"]}
    views = [ST.safe_rel(v) for v in ex["views"]]
    placed = sorted({ST.safe_rel(r) for r in list(links) + list(ex["texts"])} - {r for r in links if any(r.startswith(v + "/") for v in views)})
    return dict(links=links, views=views, placed=placed, outs=[ST.safe_rel(o) for o in ex["outs"]],
                cleanup=[ST.safe_rel(c) for c in ex["cleanup"]])


def _stage(name, key, nice, add, say, real, ex, links):
    lay = layout(ex)
    views, placed, outs, cleanup = lay["views"], lay["placed"], lay["outs"], lay["cleanup"]
    check_overlap(name, key, views + placed)
    with target_lock(name, say):  # the blobs stay as checked until the view links them
        try:
            ensure_blobs(name, {h: [inner, size, path] for _, h, size, inner, path in ex["files"]}, say, what=key, n=len(links))
        except RM.NoSlot as e:
            say(f"{e}; not staging this time, will retry on the next check")
            return None
        now = time.time()
        ST.update_state(lambda st: st.setdefault("views", {}).setdefault(name, {}).update({v: now for v in views}))
        others = [k for r, k in ST.keys_of(ST.load_state(), name).items() if r != key]
        used = set().union(*[ST.paths_of(k) for k in others]) if others else set()
        clean = [v for v in views if not ST.overlaps(v, used, ST.ancestors(used))]  # extra old links in a view go only when no other task uses that view
        n = RM.op(name, "view", dict(views=views, clean=clean, links=links, texts=ex["texts"]))
    say(f"inputs placed over there ({n} files, views {', '.join(views) or 'none'})")

    def rec(st):
        st.setdefault("keys", {}).setdefault(name, {})[key] = dict(
            recipe=real["recipe"], params=ex["params"], inputs=real.get("inputs") or [], staged=time.time(), views=views,
            placed=placed, outs=outs, cleanup=cleanup, links=links)
    ST.update_state(rec)
    if add:
        r = RM.rtq(name, f"add --nice {int(nice)} --spec-stdin", input=json.dumps([S.sendable(S.resolve(real["recipe"], ex["params"]))]))
        if r is None or r[0]:
            raise RuntimeError(f"could not add to the {RM.conf(name)['label']} queue: {RM.err_tail((r or [0, 'ssh unreachable'])[1])}")
        say(r[1])
    else:  # restage (the remote side reported missing inputs): clear the marker so it can claim again
        r = RM.rtq(name, f"input-ok {shlex.quote(key)}")
        if r is None or r[0]:
            raise RuntimeError(f"could not clear the missing-input marker on {RM.conf(name)['label']}: {RM.err_tail((r or [0, 'ssh unreachable'])[1])}")
        say(r[1] or "no missing-input marker over there")
    return key


def resend(name, say=print):
    """Replace the specs of tasks staged to this machine in the remote queue (only existing ones, add nothing) with what staging
    would send now: the hub's current recipe and params (same lookup as stage, so taskq set on the hub reaches the executor);
    the params recorded at staging only when the key is gone from the hub. Not replaced: tasks whose inputs changed since staging
    (the view over there is old; stage them again), and specs the bundle in use cannot run. The recorded params follow what was
    sent."""
    keys = ST.keys_of(ST.load_state(), name)
    specs, sent, ids = [], {}, in_use(name) if keys else []
    for key, k in keys.items():
        try:
            real = lookup(key)
            ex = RM.store_op("export", dict(spec=real))
        except Gone:
            rec, params = k["recipe"], k["params"]
        except (RN.RelayError, RuntimeError, S.SpecError) as e:
            say(f"{key} not updated: {e}")
            continue
        else:
            lay = layout(ex)
            moved = [f for f in ("links", "views", "placed") if f in k and lay[f] != k[f]]
            if moved:
                say(f"{key} not updated: its inputs changed since staging ({', '.join(moved)}); stage it again")
                continue
            rec, params = real["recipe"], ex["params"]
        sp = S.sendable(S.resolve(rec, params))
        g = BG.check_all(sp, ids)
        if g:  # the bundle in use cannot run the new spec: keep the remote one
            say(f"{key} not updated: the bundle {g[0]} in use cannot run the new spec ({g[1]})")
        else:
            specs.append(sp)
            sent[key] = dict(recipe=rec, params=params)
    if not specs:
        say("no staged tasks to update")
        return
    r = RM.rtq(name, "add --nice 0 --spec-stdin --update-only", input=json.dumps(specs))
    if r is None or r[0]:
        raise RuntimeError(f"could not replace specs on {RM.conf(name)['label']}: {RM.err_tail((r or [0, 'ssh unreachable'])[1])}")

    def rec_sent(st):
        have = ST.keys_of(st, name)
        for key, v in sent.items():
            if key in have:
                have[key].update(v)
    ST.update_state(rec_sent)
    say(r[1] or "nothing to replace")


# ---------------- fetch and archive (queued in the hub's IO group; data moves between the store and the executor) ----------------
def fetch(name, key, say=print):
    """Results finished over there → the store data dir (site_ops list on the remote side, pull on the store, verify, place).
    Any failure (not done, cannot list, pull or verify) raises RelayError."""
    real = lookup(key)
    k = ST.keys_of(ST.load_state(), name).get(key)
    if not k:
        raise RN.RelayError(f"no record of {key} staged to {RM.conf(name)['label']}")
    o = S.outputs(real)
    try:
        res = RM.op(name, "list", dict(out=real["out"], done=o["done"], exclude=o["exclude"], extra=o["extra"],
                                       outs=k.get("outs") or o["outs"], key=key, links=k.get("links") or {}))
    except RuntimeError as e:
        raise RN.RelayError(f"could not list outputs of {key}: {e}") from None
    if not res["done"]:
        raise RN.RelayError(f"{key} is not done on {RM.conf(name)['label']} yet")
    if res["bad"]:
        raise RN.RelayError(f"outputs of {key} contain links pointing elsewhere (not into the data dir or this task's inputs): {res['bad'][:3]}")
    try:
        r = RM.store_op("pull", dict(frm=name, spec=real, items=res["items"], links=res["links"]), timeout=MOVE_TIMEOUT, say=say)
    except RuntimeError as e:
        raise RN.RelayError(f"could not fetch {key} to the store: {e}") from None
    say(f"{key} fetched {r['files']} files and {r['links']} links")


def archive(key, say=print):
    """Back up this task's results on the store to the backup machine and verify (site_ops archive → archive_task.sh)."""
    RM.store_op("archive", dict(spec=S.sendable(lookup(key))), timeout=MOVE_TIMEOUT, say=say)
    say(f"{key} archived")


# ---------------- drop the remote task ----------------
class PathsLeft(Exception):
    """The remote task was dropped but its paths were not: mark it dropped in the record and relay.plan retries next round."""


def drop_remote(name, key, rk, gone=False):
    """Drop the remote task (taskq.py remove checks under the lock that nobody claimed it), then remove paths and logs this
    task occupied that no other task uses, and finally its record → error text or None."""
    if not gone:
        r = RM.rtq(name, f"remove {shlex.quote(rk)}")
        if not r or r[0] not in (0, proto.EXIT["gone"]):  # gone: the task was removed last time but paths were left
            return f"task not removed, paths left: {RM.err_tail((r or [0, 'ssh unreachable'])[1], 150)}"
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
