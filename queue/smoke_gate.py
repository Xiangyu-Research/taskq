#!/usr/bin/env python3
"""Smoke gate: before a kind of task (recipe + the parameters listed in the plugin's smoke.py by + env) is first sent to
an executor, run a small smoke task there and send the real one only after it passes. See "smoke" in queue/README.md.

    taskq smoke status [executor] [--json]   records per executor
    taskq smoke retry <executor> [kind]      queue failed kinds again (kind: full kind name or 12-char id)
    taskq smoke adopt [--dry-run]            mark kinds now assigned to executors and finished there as passed
"""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import secrets
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import proto  # noqa: E402
from proto import alert, alert_id, is_smoke  # noqa: E402
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import relay_remote as RM  # noqa: E402
import spec as S  # noqa: E402
import taskq  # noqa: E402
from common import write_json  # noqa: E402

SMOKE = {}
cfg.extend(globals(), "smoke")
FIELDS = ("by", "params", "vars")
QUEUED_ALERT_S = 6 * 3600
OK, FAILED, QUEUED = proto.SMOKE
TAG = "{smoke}"


def on():
    return bool(P.AUTO.get("smoke"))


class DefError(Exception):
    """A function in a smoke definition failed (recorded as failed)."""


class StoreError(DefError):
    """The store failed while evaluating a definition (unreachable, wrong bundle); unrelated to any task."""


class StoreOldBundle(StoreError):
    """The store's bundle is older than this core (no site_ops script or operation)."""

    def __init__(self, bundle):
        self.bundle = bundle
        super().__init__(f"the store's bundle {bundle or 'in use'} is older than this core; store-side smoke evaluation skipped")


def real_params(sp):
    """Real task parameters passed to functions in a smoke definition: the spec's names plus its expanded inputs (the smoke task takes only some of them)."""
    return dict(sp.get("names") or {}, inputs=list(sp.get("inputs") or []))


def definition(recipe, names=None):
    """A recipe's smoke definition -> dict(by, params, vars); params and vars are evaluated on the store when they are a
    function f(real task parameters, store data dir) (error -> DefError). No definition -> None; bad shape -> ValueError."""
    d = SMOKE.get(recipe)
    if d is None:
        return None
    if not isinstance(d, dict) or set(d) - set(FIELDS):
        raise ValueError(f"smoke.py {recipe} may only have {', '.join(FIELDS)}: {d!r}")
    by = d.get("by") or []
    if not isinstance(by, list) or not all(isinstance(x, str) for x in by):
        raise ValueError(f"smoke.py {recipe} by must be a list of parameter names")
    out = dict(by=by)
    got = {}
    if any(callable(d.get(f)) for f in ("params", "vars")):  # functions read the data dir: evaluate on the store (site_ops smoke)
        try:
            got = RM.store_op("smoke", dict(recipe=recipe, names=dict(names or {})))
        except RM.OldBundle as e:
            raise StoreOldBundle(e.bundle) from None
        except RuntimeError as e:
            raise StoreError(f"evaluating the smoke definition on the store failed: {str(e)[-300:]}") from None
        for f, why in (got.pop("error", None) or {}).items():
            raise DefError(f"{f}(): {why}")
    for f in ("params", "vars"):
        v = got[f] if callable(d.get(f)) else d.get(f) or {}
        if not isinstance(v, dict):
            raise ValueError(f"smoke.py {recipe} {f} must be a dict or a function returning one: {v!r}")
        out[f] = dict(v)
    return out


def h12(x):
    return hashlib.sha256((x if isinstance(x, str) else json.dumps(x, sort_keys=True, ensure_ascii=False)).encode()).hexdigest()[:12]


def env_id(site, sp):
    """The spec's envs on this executor -> id: spec entries hash the variant this executor uses (the same hash the
    built .ok has there, whether or not that side has read it); conda / venv entries hash the entry itself."""
    envs = (M.machine(site).get("envs") or {})
    out = []
    for e in S.envs_of(sp):
        x = envs.get(e)
        v = M.spec_hash(e, M.spec_variant(site, e)) if M.is_spec(x) else repr(tuple(x) if isinstance(x, list) else x)
        out.append(f"{e}:{h12(str(v))}")
    return ",".join(out)


def mark(site, sp):
    """Spec -> dict(kind, kid, env, defn, canary). kind = recipe + the values of by parameters + parameters the smoke task
    swaps in (without {smoke}) + env name: tasks with the same by but different swapped data (different channel, say) are
    two kinds, tested and recorded separately (as one kind they would overwrite each other's records and requeue back and
    forth). A recipe with no smoke definition is a canary."""
    rec = sp.get("recipe")
    names = sp.get("names") or {}
    err = None
    try:
        d = definition(rec, real_params(sp))
    except DefError as e:
        err, d = str(e), None
    by = (d or {}).get("by") or (SMOKE.get(rec) or {}).get("by") or []
    swap = [f"{p}={v}" for p, v in sorted(((d or {}).get("params") or {}).items())
            if p not in by and TAG not in json.dumps(v, ensure_ascii=False)]
    kind = "|".join([str(rec)] + [f"{p}={names.get(p)}" for p in by] + swap + [f"env={','.join(S.envs_of(sp))}"])
    return dict(kind=kind, kid=h12(kind), env=env_id(site, sp), defn=h12({"error": err}) if err else h12(d) if d else "",
                canary=d is None and err is None, recipe=rec, defs=d, error=err)


# ---------------- records ----------------
def path(site):
    return os.path.join(taskq.TQ, proto.SMOKE_DIR, f"{site}.json")


def load(site):
    try:
        with open(path(site)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


_HELD = set()  # executors this process already holds the lock for (update inside enqueue must not flock again: a second flock in the same process waits for itself)


@contextlib.contextmanager
def locked(site):
    if site in _HELD:
        yield
        return
    os.makedirs(os.path.dirname(path(site)), exist_ok=True)
    with open(path(site) + ".lock", "a") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        _HELD.add(site)
        try:
            yield
        finally:
            _HELD.discard(site)


def update(site, fn):
    with locked(site):
        recs = load(site)
        out = fn(recs)
        write_json(path(site), recs)
    return out


def queue_keys(mtq=None):
    """Keys in the queue now: those read at the start of the round (mtq) plus those on disk (smoke tests another check
    added after this round started also count)."""
    keys = master_keys(mtq)
    if taskq.have_queue():
        keys |= {t["key"] for t in taskq.read_tasks()}
    return keys


def pending(r, m, keys):
    """This kind already has a smoke test queued and still in the queue -> True."""
    return fresh(r, m) and r.get("state") == QUEUED and r.get("key") in keys


def fresh(r, m):
    return bool(r) and r.get("env") == m["env"] and r.get("defn") == m["defn"]


def master_keys(mtq):
    return {t["key"] for t in (mtq or {}).get("tasks", [])}


class Gate:
    """Used by relay.fill: verdict(key, sp) -> None (send) or why not (do not send this time). Smoke tests to queue are in
    ops(), alerts in alerts, the shadow when the switch is off in notes."""

    def __init__(self, site, rs, mtq, live=None, now=None):
        self.site, self.mtq, self.now = site, mtq or {}, now or time.time()
        self.live = on() if live is None else live
        self.bundle = (rs or {}).get("bundle")
        self.recs = load(site)
        self.want, self.alerts, self.would, self.taken, self.fails = {}, {}, {}, {}, {}
        self._fly = self._keys = None

    def queue_keys(self):
        if self._keys is None:
            self._keys = queue_keys(self.mtq)
        return self._keys

    def inflight(self, kind):
        if self._fly is None:
            self._fly = {}
            for t in self.mtq.get("tasks", []):
                if t.get("worker") == self.site and t.get("state") != proto.S_DONE and not is_smoke(t["key"]) and t.get("spec"):
                    try:
                        k = mark(self.site, t["spec"])["kind"]
                    except ValueError:
                        continue
                    self._fly[k] = self._fly.get(k, 0) + 1
        return self._fly.get(kind, 0) + self.taken.get(kind, 0)

    def took(self, key, sp):
        if is_smoke(key) or not sp:
            return
        try:
            k = mark(self.site, sp)["kind"]
        except ValueError:
            return
        self.taken[k] = self.taken.get(k, 0) + 1

    def _hold(self, key, why):
        if self.live:
            return why
        self.would[key] = why
        return None

    @property
    def notes(self):
        """The shadow when the switch is off: which smoke tests would be queued and how many tasks would be held back."""
        if self.live or not (self.want or self.would):
            return []
        out = [f"(smoke switch off) {self.site} would queue smoke tests: {', '.join(sorted(self.want)) or 'none'}; would hold back {len(self.would)} tasks"
               + (f" ({', '.join(sorted(self.would)[:3])}{' ...' if len(self.would) > 3 else ''})" if self.would else "")]
        return out

    def verdict(self, key, sp):
        if is_smoke(key) or not sp:
            return None
        try:
            m = mark(self.site, sp)
        except ValueError as e:
            if self.live:
                self.alerts[alert_id("smoke", self.site, h12(str(sp.get("recipe"))))] = alert(
                    f"bad smoke definition for {self.site}: {e}", machine=self.site, kind=str(sp.get("recipe")), why=str(e), key=key)
            return self._hold(key, f"bad smoke definition: {e}")
        r = self.recs.get(m["kind"])
        if fresh(r, m) and r.get("state") == OK:
            return None
        if m["error"]:
            why = f"smoke definition function failed: {m['error']}"
            if self.live:
                self.alerts[alert_id("smoke", self.site, m["kid"])] = alert(
                    f"{self.site}: {m['kind']} smoke test failed: {why}", machine=self.site, kind=m["kind"], why=why, key=key)
                if not (fresh(r, m) and r.get("state") == FAILED):
                    self.fails[m["kind"]] = dict(m=m, why=why)
            return self._hold(key, why)
        if m["canary"]:
            n = self.inflight(m["kind"])
            return self._hold(key, f"{m['kind']} has never succeeded on this executor, sending one first ({n} in flight)") if n else None
        if pending(r, m, self.queue_keys()):
            if self.live and self.now - (r.get("t") or self.now) > QUEUED_ALERT_S:
                self.alerts[alert_id("smoke", self.site, m["kid"])] = alert(
                    f"{self.site}: smoke task {r['key']} for {m['kind']} has been queued for {(self.now - r['t']) / 3600:.0f} h and is not done",
                    machine=self.site, kind=m["kind"], why="not done yet", key=r["key"])
            return self._hold(key, f"waiting for smoke {r.get('key')}")
        if fresh(r, m) and r.get("state") == FAILED and (not self.bundle or r.get("bundle") == self.bundle):
            if self.live:
                self.alerts[alert_id("smoke", self.site, m["kid"])] = alert(
                    f"{self.site}: {m['kind']} smoke test failed: {r.get('why')} (taskq smoke retry {self.site} {m['kid']})",
                    machine=self.site, kind=m["kind"], why=r.get("why"), key=r.get("key"))
            return self._hold(key, f"smoke test failed: {r.get('why')}")
        self.want.setdefault(m["kind"], dict(sp=sp, m=m))
        return self._hold(key, f"queueing the {m['kind']} smoke test first")

    def ops(self):
        """-> [dict(op="smoke_add", ...)]: one smoke test per kind, each with a new name ({smoke})."""
        if not self.live:
            return []
        out = [dict(op="smoke_fail", key=f"{proto.SMOKE_PREFIX}{self.site}/{f['m']['kid']}", mark=f["m"], why=f["why"])
               for f in self.fails.values()]
        for kind, w in self.want.items():
            m, sp, d = w["m"], w["sp"], w["m"]["defs"]
            stamp = f"{int(self.now)}-{secrets.token_hex(2)}"
            sid, key = f"{self.site}-{m['kid']}-{stamp}", f"{proto.SMOKE_PREFIX}{self.site}/{m['kid']}-{stamp}"
            names = sp.get("names") or {}
            params = {p: names[p] for p in d["by"] if p in names}
            params.update(subst(d["params"], sid))
            if d["vars"]:
                params["vars"] = subst(d["vars"], sid)
            params.update(machines=[self.site], key=key)
            out.append(dict(op="smoke_add", key=key, recipe=m["recipe"], params=params, mark=m, envs=S.envs_of(sp),
                            bundle=self.bundle, sid=sid))
        return out


def subst(v, sid):
    if isinstance(v, str):
        return v.replace(TAG, sid)
    if isinstance(v, list):
        return [subst(x, sid) for x in v]
    if isinstance(v, dict):
        return {k: subst(x, sid) for k, x in v.items()}
    return v


def record(site, m, **kw):
    def fn(recs):
        recs[m["kind"]] = dict(kind=m["kind"], env=m["env"], defn=m["defn"], t=time.time(), **kw)
    update(site, fn)


def enqueue(site, op):
    """smoke_add -> (whether it was added, explanation). Unparseable, env differs from the real task, or cannot be added
    to the queue: recorded as failed. This kind already has a queued smoke test (another check added it at the same
    time) -> (None, explanation), do not add another (two checks running at once do not add twice)."""
    m = op["mark"]
    with locked(site):  # another check cannot slip in between the check and the add
        r = load(site).get(m["kind"])
        if pending(r, m, queue_keys()):
            return None, f"{site}: {m['kind']} already has smoke task {r['key']} queued, not adding another"
        return _enqueue(site, op, m)


def _enqueue(site, op, m):
    try:
        sp = S.resolve(op["recipe"], op["params"])
        if S.envs_of(sp) != op["envs"]:
            raise S.SpecError(f"smoke task env {S.envs_of(sp)} differs from the real task's {op['envs']} (add the env parameter to by)")
    except S.SpecError as e:
        record(site, m, state=FAILED, src="smoke", key=op["key"], bundle=op.get("bundle"), why=f"cannot resolve: {e}"[:300])
        return False, f"{site}: cannot resolve the {m['kind']} smoke task: {e}"
    rc, out, err = taskq.central(["add", f"--nice={proto.SMOKE_NICE}", "--recipe"] + S.kv_args(op["recipe"], op["params"]))
    if rc:
        record(site, m, state=FAILED, src="smoke", key=op["key"], bundle=op.get("bundle"), why=f"cannot add: {(out + err).strip()[-200:]}")
        return False, f"{site}: cannot add the {m['kind']} smoke task to the queue: {(out + err).strip()[-150:]}"
    record(site, m, state=QUEUED, src="smoke", key=op["key"], bundle=op.get("bundle"), why="")
    return True, f"{site}: queued the {m['kind']} smoke task {op['key']}"


def finished(site, key):
    """This smoke task's record already has a result (ok / failed) -> True."""
    return any(r.get("key") == key and r.get("state") in (OK, FAILED) for r in load(site).values())


def finish(site, key, ok, why=""):
    def fn(recs):
        for r in recs.values():
            if r.get("key") == key and r.get("state") == QUEUED:
                r.update(state=OK if ok else FAILED, why=(why or "")[-600:], t=time.time())
                return r["kind"]
    return update(site, fn)


def smoke_dirs(sp, sid):
    """Top-level dirs the smoke task occupies on that side whose name contains sid (relative to the data dir, at least
    two levels): paths in out and in vars values starting with {data}/."""
    cands = [sp.get("out") or ""] + [v[len("{data}/"):] for v in (sp.get("vars") or {}).values()
                                    if isinstance(v, str) and v.startswith("{data}/")]
    out = set()
    for p in cands:
        parts = p.strip("/").split("/")
        i = next((i for i, x in enumerate(parts) if sid in x), None)
        if i is not None and i >= 1:
            out.add("/".join(parts[:i + 1]))
    return sorted(out)


def sid_of(key):
    """_smoke/<executor>/<id>-<time> -> <executor>-<id>-<time> ({smoke})."""
    return key[len(proto.SMOKE_PREFIX):].replace("/", "-", 1)


def real_done(site, m):
    """The real task finished on this executor: mark this kind passed (src=real)."""
    def fn(recs):
        r = recs.get(m["kind"])
        if fresh(r, m) and r.get("state") == OK:
            return False
        recs[m["kind"]] = dict(kind=m["kind"], env=m["env"], defn=m["defn"], t=time.time(), state=OK, src="real", key=None, why="")
        return True
    return update(site, fn)


def passed(site, key):
    """Whether this kind of task passed the smoke gate on site (switch off -> True)."""
    if not on():
        return True
    if is_smoke(key):
        return False
    t = next((t for t in taskq.read_tasks() if t["key"] == key), None)
    sp = taskq.spec_of(dict(t)) if t else None
    if not sp:
        return False
    try:
        m = mark(site, sp)
    except ValueError:
        return False
    r = load(site).get(m["kind"])
    return fresh(r, m) and r.get("state") == OK


def retry(site, kind=None):
    def fn(recs):
        gone = [k for k, r in recs.items() if r.get("state") == FAILED and (kind is None or kind in (k, h12(k)))]
        for k in gone:
            recs.pop(k)
        return gone
    return update(site, fn)


def adopt(dry=False):
    """Tasks now assigned to executors and finished there -> mark that executor's kind passed (src=adopted). -> [(executor, kind)] (newly recorded)."""
    st = taskq.local_status()
    got = {}
    for t in st["tasks"]:
        if is_smoke(t["key"]) or not t.get("spec"):
            continue
        site = t.get("assigned") or t.get("worker")
        if site not in M.executors() and t["state"] == proto.S_DONE:
            site = (taskq.released_info(t["key"]) or (None, None))[1]
        if site in M.executors():
            m = mark(site, t["spec"])
            got.setdefault(site, {})[m["kind"]] = m
    new = []
    for site, ms in got.items():
        def fn(recs, ms=ms):
            out = []
            for k, m in ms.items():
                if fresh(recs.get(k), m) and recs[k].get("state") == OK:
                    continue
                out.append(k)
                if not dry:
                    recs[k] = dict(kind=k, env=m["env"], defn=m["defn"], t=time.time(), state=OK, src="adopted", key=None, why="")
            return out
        new += [(site, k) for k in (update(site, fn) if not dry else fn(dict(load(site))))]
    return new


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq smoke", description="Smoke gate records")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("status", help="records per executor")
    p.add_argument("site", nargs="?")
    p.add_argument("--json", action="store_true")
    p = sub.add_parser("retry", help="queue failed kinds again")
    p.add_argument("site")
    p.add_argument("kind", nargs="?")
    p = sub.add_parser("adopt", help="mark kinds assigned to executors and finished there as passed")
    p.add_argument("-n", "--dry-run", action="store_true")
    a = ap.parse_args(argv)
    if getattr(a, "site", None) and a.site not in M.executors():
        print(f"no executor {a.site} (have: {', '.join(M.executors())})", file=sys.stderr)
        return proto.EXIT["gone"]
    if a.cmd == "status":
        allr = {s: load(s) for s in ([a.site] if a.site else M.executors())}
        if a.json:
            S.json_out(dict(version=proto.VERSION, on=on(), sites=allr))
            return 0
        print(f"smoke gate {'on' if on() else 'off (only relay.log)'}")
        for s, recs in allr.items():
            print(f"{s}: {len(recs)} kinds")
            for k, r in sorted(recs.items()):
                when = time.strftime("%m-%d %H:%M", time.localtime(r.get("t") or 0))
                print(f"  {h12(k)} {r.get('state'):<7} {r.get('src'):<8} {when}  {k}" + (f"  {r['why']}" if r.get("why") else ""))
        return 0
    if a.cmd == "retry":
        gone = retry(a.site, a.kind)
        print(f"{a.site}: dropped records for {len(gone)} failed kinds, smoke will be queued again next check" + (f" ({', '.join(gone)})" if gone else ""))
        return 0
    new = adopt(a.dry_run)
    print(("would mark" if a.dry_run else "marked") + f" {len(new)} kinds" + "".join(f"\n  {s}: {k}" for s, k in new))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
