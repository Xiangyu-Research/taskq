#!/usr/bin/env python3
"""Relay for executors (machines with a root and scheduler in machines.py, pueue or Slurm): the hub claims tasks for an
executor from the hub queue, stages inputs at the same relative paths as the store data dir, adds them to the executor's own
queue, fetches results back to the store and archives them. The hub only coordinates: data moves directly between the store
and the executor (relay_transfer.py), and executors never talk to each other. Remote commands and the root layout are in
relay_remote.py, hub-side bookkeeping and remote cleanup in relay_store.py.

Every health check (monitor/health.py, every 30 minutes) calls run_all through queue/feed.py's run_all (the only entry point
for sending tasks): read remote state with one ssh; plan stage/fetch/archive/release per task; then assign unclaimed tasks to
executors; finally execute the plan and top up worker jobs. See queue/README.md for the full flow.

    taskq plan                                     what this round would do (the health check's dry run), sending included
    taskq internal stage <name> <key> --nice N [--no-add]   stage inputs and add to the remote queue (--no-add: only stage, clear the missing-input marker)
    taskq internal fetch <name> <key>              fetch a finished task, verify it and put it in the store data dir
    taskq internal archive <key>                   back up this task's store results to the backup machine and verify
    taskq site bundle <name> [--commit C]          build a bundle, push it and switch current
    taskq site status <name>                       remote queue (the format of taskq status --queue)
    taskq site resend <name>                       replace staged tasks' specs in the remote queue with the hub's current ones
"""
import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduler code
sys.path.insert(0, HERE)
import cfg  # noqa: E402
REPO = cfg.REPO  # project repo root
cfg.use_config()
import proto  # noqa: E402
from proto import ROOT, alert, alert_id  # noqa: E402
sys.path.insert(0, os.path.join(CORE, "sync"))
sys.path.insert(0, os.path.join(CORE, "monitor"))
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import machine_name  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402
import taskq  # noqa: E402
import refill  # noqa: E402
import remote as R  # noqa: E402
import executors as EX  # noqa: E402
import relay_plugins as RN  # noqa: E402
import relay_bundle as RB  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_store as ST  # noqa: E402
import hubstate as HS  # noqa: E402
import relay_transfer as RT  # noqa: E402
import bundle_gate as BG  # noqa: E402
import smoke_gate as SG  # noqa: E402
import switches  # noqa: E402
import events  # noqa: E402
import names as N  # noqa: E402
import envs as EV  # noqa: E402

LIVE = proto.PUEUE_LIVE
NOSLOT_RC = proto.EXIT["stage_later"]  # relay.py stage could not get a CPU job this time (or the checkpoint source is still claimed): retry next check without alerting


def relayable(i):
    """refill.info result → whether it can be claimed for this executor: the recipe resolves (using the local recipes.py; the
    hub queue's code may be older) and the plugin accepts the task (accepts in sites/names.py, e.g. a plugin that refuses
    tasks reading files in the repo)."""
    sp = i["spec"]
    rec, params = i["recipe"] or sp.get("recipe"), i["params"] or sp.get("params") or {}
    try:
        relay = N.plugin_of(S.resolve(rec, params))
    except S.SpecError:
        relay = N.plugin_of(sp)
    return N.accepts(relay, params)


def gate(name, rs, mtq, key):
    """The bundle in use cannot run key's spec → (bundle, reason); fine or unknown → None (queue/bundle_gate.py; checked again
    when stage sends it)."""
    ids = BG.in_use(name, rs.get("bundle"), rs.get("live"))
    return BG.check_all(S.sendable(refill.info(mtq, key)["spec"]), ids) if ids else None


# ---------------- remote state ----------------
def remote_status(name):
    """→ remote status dict; unreachable or failed → None."""
    c = RM.conf(name)
    if RM.master_down(c):
        return None
    try:
        rs = RM.op(name, "status", dict(sched=c["scheduler"], **EX.of(name).status_args(name), cap=c.get("cap"), blob_cap=c.get("blob_cap"), env_root=M.env_root(name),
                                             envs=sorted(EV.wanted(name, envs_switch(name)[1]))), timeout=proto.SITE_STATUS_TIMEOUT)
    except RuntimeError:
        return None
    return parse_sched(name, rs)


def current_bundle(name):
    """When status cannot be read, look at remote code/current → bundle name; no bundle yet (first time on a new machine) → "";
    unreachable → None."""
    c = RM.conf(name)
    if RM.master_down(c):
        return None
    cur = f"{R.q(RM.root(name))}/{ROOT['current']}"
    rc, out, _ = RM.rsh(name, f'if [ -e {cur} ]; then basename "$(readlink -f {cur})"; fi', timeout=30)
    return out.strip() if rc == 0 else None


def parse_sched(name, rs):
    """Raw scheduler output read back by the status script → fields its executor type parses it into (sites/executors.py:
    pueue rs["jobs"], the tasks in each worker's group; Slurm rs["squeue"]); cannot read → None. One ssh reads everything, and
    the hub parses it: each machine is contacted once per check, so the state is from one moment."""
    raw = rs.pop("sched_raw", None)
    rs["jobs"] = rs["squeue"] = None
    if raw is not None:
        rs.update(EX.of(name).parse_status(name, raw))
    return rs


# ---------------- bundles ----------------
def claim_times(rs):
    """Remote status → claim time of each task still claimed ([] = none); no queue in the status (cannot read) → None."""
    tasks = ((rs or {}).get("taskq") or {}).get("tasks")
    return None if tasks is None else [t["since"] for t in tasks if t.get("since")]


def deploy(name, bid, path, claims=None, switch=True):
    """Push the bundle and switch current (atomic mv -T; switch=False: only unpack it under code/, self migrate switches it);
    the first time, create dirs, enable the queue and stamp the root with this code's on-disk format (proto.ROOT format: a new
    root is in it; an existing one is stamped by taskq self migrate); record the machine name in the root (proto.ROOT machine).
    Only unpack after receiving everything and matching sha256 (tar does not complain when ssh drops between two files); push
    one at a time per machine (flock), each with its own temp dir. Prune old bundles (claims: claim times of tasks still claimed
    over there, claim_times): keep current, the newest proto.CODE_KEEP, anything touched within CODE_KEEP_S (a running worker
    touches its bundle every loop), and anything touched since CODE_CLAIM_SLACK before the earliest claim (after a worker
    switches bundles, tasks it handed to the new bundle still run from the bundle at claim time: that bundle was last touched
    after the claim). claims None (unknown what is running over there) keeps all old bundles. Queued Slurm jobs do not hold an
    old bundle: they submit paths under code/current, resolved when the job starts (sites/slurm.py). Temp files left by an
    interrupted deploy are removed after a day."""
    r = R.q(RM.root(name))
    b = shlex.quote(bid)
    tar = subprocess.run(["tar", "-C", path, "-cf", "-", "."], capture_output=True, check=True).stdout
    sha = hashlib.sha256(tar).hexdigest()
    cur = os.path.relpath(ROOT["current"], ROOT["code"])  # name under code/
    tq, en = f"$R/{ROOT['queue']}/{proto.QUEUE['tasks']}", f"$R/{ROOT['queue']}/{proto.QUEUE['enabled']}"
    mf, fm = f"$R/{ROOT['machine']}", f"$R/{ROOT['format']}"
    cmd = (f"set -e; R={r}; mkdir -p {' '.join('$R/' + d for d in proto.MKDIRS)}; "
           f"[ -e {tq} ] || {{ : > {tq}; touch {en}; echo {proto.FORMAT} > {fm}.tmp.$$; mv -f {fm}.tmp.$$ {fm}; }}; echo {shlex.quote(name)} > {mf}.tmp.$$; mv -f {mf}.tmp.$$ {mf}; cd $R/{ROOT['code']}; "
           f"exec 9> $R/{ROOT['deploy_lock']}; flock -w 300 9; "
           "find . -maxdepth 1 -name '*.tmp.*' -mtime +0 -exec rm -rf {} + || true; "  # left by earlier interrupted deploys (nobody else pushes under the lock)
           f"t={b}.tmp.$$; rm -rf $t $t.tar; cat > $t.tar; "
           f"if [ \"$(sha256sum < $t.tar | cut -d' ' -f1)\" != {sha} ]; then rm -f $t.tar; echo 'bundle transfer incomplete' >&2; exit 3; fi; "
           f"if [ ! -d {b} ]; then mkdir $t; tar -xf $t.tar -C $t; mv -T $t {b}; fi; rm -f $t.tar; "
           f"touch {b}" + (f"; ln -sfn {b} {cur}.tmp.$$; mv -T {cur}.tmp.$$ {cur}" if switch else ""))
    if claims is not None:
        floor = int(min([time.time() - proto.CODE_KEEP_S] + [c - proto.CODE_CLAIM_SLACK for c in claims]))
        cmd += (f"; ls -1t | grep -vx {cur} | grep -v '\\.tmp' | tail -n +{proto.CODE_KEEP + 1} | "
                f"while read -r d; do if [ \"$d\" != {b} ] && [ \"$(stat -c %Y \"$d\")\" -lt {floor} ]; then rm -rf \"$d\"; fi; done")
    r2 = subprocess.run(RM.site_argv(name, cmd, 600), input=tar, capture_output=True, timeout=600)
    if r2.returncode:
        raise RuntimeError(f"could not push bundle {bid} to {RM.conf(name)['label']}: {RM.err_tail(r2.stderr.decode(errors='replace'))}")


BUNDLE_REF = "origin/main"  # commit to build bundles from (pinned in tests; pushing main elsewhere does not change it)


def bundle_key(commit, core):
    """Cache key of the bundle built from commit and core: also the bundle rules (dirs / skips), so a bundle built under other
    rules is never reused."""
    rules = hashlib.sha256(repr(RB.rules(commit or BUNDLE_REF)).encode()).hexdigest()[:12]
    return f"{commit}:{core}:{rules}"


def wanted_bundle():
    """The bundle every executor should use (the same everywhere) → (name, path on the hub): built from BUNDLE_REF in the
    project repo and RB.CORE_REF in the core repo. When the key (bundle_key) is unchanged and the bundle still exists, reuse
    the recorded one (bundle_for in relay's bookkeeping) instead of rebuilding."""
    commit = subprocess.run(["git", "-C", REPO, "rev-parse", BUNDLE_REF], capture_output=True, text=True).stdout.strip()
    core = subprocess.run(["git", "-C", cfg.CORE, "rev-parse", RB.CORE_REF], capture_output=True, text=True).stdout.strip()
    key = bundle_key(commit, core)
    had = ST.load_state().get("bundle_for") or {}
    bid = had.get(key)
    path = os.path.join(os.path.expanduser(RB.OUT), bid) if bid else None
    if not path or not os.path.isdir(path):
        bid, path = RB.build(commit or BUNDLE_REF, core=core or RB.CORE_REF)
        new = {} if bid in had.values() else dict(bundle_since=time.time())  # the code changed: failed transfers get fresh tries (again)
        ST.update_state(lambda x: x.update(bundle_for={key: bid}, **new))
    return bid, path


def bundle(name, have=None, claims=None):
    """Push the wanted bundle when it differs from remote current (claims as in deploy) → message (None when unchanged)."""
    bid, path = wanted_bundle()
    if bid == have:
        return None
    deploy(name, bid, path, claims)
    return f"{RM.conf(name)['label']} switched to code {bid}"


def code_only():
    """Machines with a root that are not executors (the store; the hub when envs are locked): they also run site_ops
    operations, with the same bundle as executors."""
    return [n for n in M.MACHINES if M.MACHINES[n].get("root") and not M.is_exec(n)]


def update_code(stuck):
    """Push the wanted bundle to code_only machines whose current differs → messages."""
    out = []
    for name in code_only():
        if M.paused(name):
            continue
        try:
            cur = current_bundle(name)
            if cur is None:
                stuck[alert_id("code", name)] = f"cannot reach {RM.conf(name)['label']} to read its bundle"
                continue
            msg = bundle(name, cur, claims=[])  # not an executor: no workers and no claimed tasks
            out += [msg] if msg else []
        except Exception as e:  # noqa: BLE001
            stuck[alert_id("code", name)] = f"building or pushing the bundle to {RM.conf(name)['label']} failed: {e}"[:300]
    return out


def prune_bundles(bid):
    """Keep on the hub only the wanted bundle, the ones executors use (bundle and live of each executor's hubstate.sites
    row), ones held by the send gate, and the newest few (relay_bundle.prune)."""
    keep = {bid}
    for d in HS.sites().values():
        keep |= {d.get("bundle")} | set(d.get("live") or ()) | {h.get("bundle") for h in (d.get("held") or {}).values()}
    return RB.prune(keep - {None})


# ---------------- each check ----------------
def io_tasks(s, step, key, since=0):
    """Tasks for step and key in the hub's IO group (proto.IO_GROUP) (records added after since), oldest first → list of pueue tasks."""
    hub, out = M.hub(), []
    for t in sorted((s["pueue"].get(hub) or {}).get("tasks", []), key=lambda t: t["id"]):
        r = C.identify(hub, t, s.get("records") or [])
        if r and r["step"] == step and r["key"] == key and r.get("added", 0) >= since - 60:
            out.append(t)
    return out


def io_task(s, step, key, since=0):
    """The newest of io_tasks, or None."""
    return (io_tasks(s, step, key, since) or [None])[-1]


def again(s, step, key, since=0, code=0):
    """The newest run of this step failed (not stopped by hand) and fewer than proto.XFER_TRIES have failed since the code last
    changed (code: state bundle_since; a new bundle may fix what failed) -> True: queue it again, do not report."""
    def failed(t):
        return (t["status"] not in LIVE and t["status"] != "Success" and t.get("code") != NOSLOT_RC
                and not (t.get("result") == "Killed" or t.get("code") == 143))
    ts = io_tasks(s, step, key, since)
    return bool(ts) and failed(ts[-1]) and sum(map(failed, io_tasks(s, step, key, max(since, code)))) < proto.XFER_TRIES


def smoke_step(name, mtq, t, a, k, now):
    """Smoke task (proto.SMOKE_PREFIX): done or failed over there, or a result was recorded → record it, drop the remote task and
    remove it from the hub queue (no fetch or archive); otherwise → None (stage and beat as usual)."""
    key = t["key"]
    over = SG.finished(name, key)
    res = None
    if a is not None:
        if a["state"] == "done" and not proto.worker_holds(a, now):
            res = (True, "")
        elif a["state"] in (proto.S_FAILED, proto.S_BAD):
            res = (False, f"{a['state']}: {a.get('note') or ''}")
    if res is None and t["state"] == "done":
        res = (True, "")
    if res is None and not over:
        return None
    ops = [dict(op="beat", key=key)]
    if res is not None and not over:
        ops.append(dict(op="smoke_rec", key=key, rkey=key, ok=res[0], why=res[1]))
    if k:
        sp = refill.info(mtq, key)["spec"]
        ops.append(dict(op="drop", key=key, rkey=key, cleanup=SG.smoke_dirs(sp, SG.sid_of(key)) if sp else []))
    ops.append(dict(op="smoke_end", key=key))
    return ops


def plan(s, name, rs, st, now=None):
    """→ (actions, stuck {problem id: text}). rs: remote state (None = unreachable); st: load_state().
    An action is dict(op=beat|release|stage|restage|fetch|drop|rstop|rresume|rnice|forget|archive|hold|unhold|failtail, key, ...); rkey: the key
    in the remote queue; failtail copies the end of the log of a failure over there into the hub queue (taskq show)."""
    now = now or time.time()
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    L, keys = RM.conf(name)["label"], ST.keys_of(st, name)
    code = st.get("bundle_since") or 0
    mine = [t for t in (mtq or {}).get("tasks", []) if t.get("worker") == name]
    acts, stuck = [], {}
    if not mtq or not mtq.get("enabled"):
        return [], {}
    if rs is not None and "error" in (s.get("pueue", {}).get(M.hub()) or {"error": 1}):
        rs = None  # hub pueue unreadable: running fetches and archives are invisible, so treat it as unknown and only beat
    if rs is None:
        acts = [dict(op="beat", key=t["key"]) for t in mine if t["state"] != "done"]
        last = (st.get("last_ok") or {}).get(name, 0)
        if acts and now - last > proto.SITE_FRESH:
            stuck[alert_id("site", name, "down")] = alert(
                f"cannot read {L}'s status{f' for {(now - last) / 3600:.0f} h' if last else ''} (ssh down, or its storage hangs);"
                f" it has {len(acts)} claimed tasks and their progress is invisible", hours=round((now - last) / 3600, 1) if last else None, tasks=len(acts))
        return acts, stuck
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}
    view = site_view(name, rs, st)  # half-done is judged on these entries (they carry ran), never on raw rs["tasks"]
    owned, holds, gated = set(), BG.held(st, name), set()

    def fetched(key, since, kept):
        """A task finished over there whose results the store does not have: queue its fetch, wait while it runs, report a failed
        one (kept: what stays as it is meanwhile) → the fetch task once it has succeeded, else None."""
        ft = io_task(s, "relay_fetch", key, since)
        if ft is None or again(s, "relay_fetch", key, since, code):
            acts.append(dict(op="fetch", key=key))
        elif ft["status"] == "Success":
            return ft
        elif ft["status"] not in LIVE:
            stuck[alert_id("relayfetch", key)] = alert(f"fetching {key} from {L} failed on {M.hub()} task {ft['id']} ({ft['status']}); {kept}."
                                                       f" See pueue log {ft['id']}, then pueue remove {ft['id']} to retry next check",
                                                       machine=name, step="fetch", task=ft["id"], status=ft["status"])
        return None

    def late(key, ft):
        """Fetched, but the hub queue still does not show it as done after a while: report it."""
        end = C.local_time(ft.get("end"))
        if end is not None and now - end.timestamp() > proto.FETCH_LATE:
            stuck[alert_id("relayfetch", key)] = alert(f"{key} on {L} was fetched and archived successfully ({M.hub()} task {ft['id']}), but {M.hub()} still does not see it as done;"
                                                       f" check the result dir on {M.hub()}", machine=name, step="late", task=ft["id"], status=ft["status"])
    for t in mine:
        n0 = len(acts)
        try:
            key = t["key"]
            k = keys.get(key)
            a = rt.get(key) if k else None
            if a:
                owned.add(a["key"])
                if newer_tail(a.get("fail_tail"), t.get("fail_tail")):
                    acts.append(dict(op="failtail", key=key, tail=dict(a["fail_tail"], machine=name)))
            if proto.is_smoke(key):
                sm = smoke_step(name, mtq, t, a, k, now)
                if sm is not None:
                    acts += sm
                    continue
            if t["state"] == "done":
                ft = io_task(s, "relay_fetch", key, t.get("since") or 0)
                at = io_task(s, "relay_archive", key, t.get("since") or 0)
                if any(x is not None and x["status"] in LIVE for x in (ft, at)):  # it is on the store, but fetch/archive is still running: wait
                    acts.append(dict(op="beat", key=key))
                    continue
                # the hub queue says done: the store has it (the result itself, not the fetch record). Report fetch failures
                if ft is not None and ft["status"] != "Success":
                    acts.append(dict(op="beat", key=key))
                    stuck[alert_id("relayfetch", key)] = alert(f"fetching {key} from {L} failed on {M.hub()} task {ft['id']} ({ft['status']}); claim kept."
                                                               f" See pueue log {ft['id']}, then pueue remove {ft['id']} to retry next check",
                                                               machine=name, step="fetch", task=ft["id"], status=ft["status"])
                    continue
                if k:  # once the store has it, drop the remote task and results without waiting for archive (executor disks are only staging)
                    acts.append(dict(op="drop", key=key, rkey=key))
                try:
                    want = ft is not None and bool(RT.lookup(key).get("archive"))
                except (RN.RelayError, S.SpecError):
                    want = False
                archived = at is not None and at["status"] == "Success"
                if want and not archived:
                    if at is not None:
                        acts.append(dict(op="beat", key=key))
                        stuck[alert_id("relayfetch", key)] = alert(f"archiving {key} to the backup machine failed on {M.hub()} task {at['id']} ({at['status']}); it is on {M.hub()} and the claim is kept."
                                                                   f" See pueue log {at['id']}, then pueue remove {at['id']} to retry next check",
                                                                   machine=name, step="archive", task=at["id"], status=at["status"])
                        continue
                    if not C.backup_down():  # backup machine usable: the hub queues archive separately and releases only after success; if it is just unreachable this time, retry next check
                        acts.append(dict(op="beat", key=key))
                        if not C.backup_down(s.get("ok")):
                            acts.append(dict(op="archive", key=key))
                        continue
                acts.append(dict(op="release", key=key, why=f"done, it is on {M.hub()}" + (", archived" if archived else " (not archived)")))
                sp = refill.info(mtq, key)["spec"]
                try:
                    if sp and sp.get("recipe") and SG.on():
                        acts.append(dict(op="smoke_real", key=key, mark=SG.mark(name, sp)))
                except ValueError:
                    pass
                continue
            acts.append(dict(op="beat", key=key))
            if t["state"] == "stopped" and not (a and a["state"] == "done"):
                sg = None if a else io_task(s, "relay_stage", key, t.get("since") or 0)
                if a and a["state"] != "stopped":
                    acts.append(dict(op="rstop", key=key, rkey=a["key"]))
                elif sg is not None and sg["status"] in LIVE:
                    pass  # its staging has not ended: keep the claim, so a task it still adds over there is stopped from here next check
                elif not (a and a.get("worker")):  # no longer claimed over there (or not staged yet)
                    acts[-1] = dict(op="release", key=key, why="stopped (taskq.py stop)" + ("; remote results kept" if a else ""))
                continue
            if a and a["state"] not in ("done", "failed", "bad") and a.get("nice") is not None and a["nice"] != t["nice"]:
                acts.append(dict(op="rnice", key=key, rkey=a["key"], nice=t["nice"]))  # nice changed in the hub queue (set-nice): follow over there
            since = t.get("since") or 0
            if a is None:
                sg = io_task(s, "relay_stage", key, since)
                noslot = sg is not None and sg["status"] not in LIVE and sg.get("code") == NOSLOT_RC
                if sg is None or sg["status"] == "Success" or noslot or again(s, "relay_stage", key, since, code):
                    g, h = gate(name, rs, mtq, key), holds.get(key) or {}
                    if not g and h.get("bundle") in BG.in_use(name, rs.get("bundle"), rs.get("live")):
                        g = (h["bundle"], h.get("err"))  # held by stage for this spec: do not queue again while the same bundle is still in use
                    if g:  # the bundle in use cannot run this spec: do not send yet, keep the claim, recheck after it switches bundles
                        gated.add(key)
                        if h.get("bundle") != g[0]:
                            acts.append(dict(op="hold", key=key, bundle=g[0], err=g[1]))
                        older = bool(rs.get("bundle")) and g[0] != rs["bundle"] and now - (h.get("since") or now) < proto.HELD_PATIENCE
                        stuck[alert_id("bundlegate", name, key)] = alert(  # only an older bundle still used by running workers: it clears when they switch, so only record it for a while
                            f"the bundle {g[0]} in use on {L} cannot run the spec for {key} ({g[1]}); not sending yet and keeping the claim until it switches bundles",
                            level="info" if older else None, machine=name, key=key, bundle=g[0], err=g[1])
                        continue
                    acts.append(dict(op="stage", key=key, nice=t["nice"]))
                    if noslot and now - since > proto.NOSLOT_ALERT:
                        stuck[alert_id("site", name, "stage")] = alert(
                            f"{key} for {L} has not been staged for {(now - since) / 3600:.0f} h (no CPU job slot, or the checkpoint source is still claimed)"
                            f" ({M.hub()} task {sg['id']}, retried every check)", key=key, hours=round((now - since) / 3600, 1), task=sg["id"])
                elif sg["status"] not in LIVE:
                    stuck[alert_id("relaystage", key)] = alert(f"staging {key} to {L} failed on {M.hub()} task {sg['id']} ({sg['status']}); claim kept."
                                                               f" See pueue log {sg['id']}, then pueue remove {sg['id']} to retry next check",
                                                               machine=name, step="stage", task=sg["id"], status=sg["status"])
                continue
            ast = a["state"]
            if ast == "done" and proto.worker_holds(a, now):
                pass  # done marker exists but the worker still claims it: the command is still writing results, so only beat and fetch after release; an expired claim (worker died without releasing) fetches as usual
            elif ast == "done":
                ft = fetched(key, since, "claim kept")
                if ft:
                    late(key, ft)
            elif ast == "stopped":
                acts.append(dict(op="rresume", key=key, rkey=a["key"]))
            elif ast == "input":  # remote side found missing inputs before starting (taskq.py missing): restage only the missing ones, rebuild views, then clear the marker
                sg = io_task(s, "relay_stage", key, a.get("input_since") or since)
                if (sg is None or sg["status"] == "Success" or (sg["status"] not in LIVE and sg.get("code") == NOSLOT_RC)
                        or again(s, "relay_stage", key, a.get("input_since") or since, code)):
                    acts.append(dict(op="restage", key=key))
                elif sg["status"] not in LIVE:
                    stuck[alert_id("relaystage", key)] = alert(f"restaging missing inputs for {key} to {L} failed on {M.hub()} task {sg['id']} ({sg['status']}); claim kept."
                                                               f" See pueue log {sg['id']}, then pueue remove {sg['id']} to retry next check",
                                                               machine=name, step="restage", task=sg["id"], status=sg["status"])
            elif ast == "failed":
                ft = a.get("fail_tail") or {}
                why, cls = taskq.tail_reason(ft), proto.fail_class(ft.get("lines"))
                if refill.site_progress(view.get(key)):
                    tries, done = proto.RETRY["progress"][3], events.retries(key, name, now)
                    try:
                        at = time.mktime(time.strptime(ft.get("time") or "", "%Y-%m-%d %H:%M:%S"))
                    except ValueError:
                        at = None
                    at = max([at] + done[-1:]) + proto.retry_wait("progress", len(done) + 1) if at else None
                    if len(done) < tries and at and now >= at:
                        acts.append(dict(op="forget", key=key, rkey=a["key"], why=f"failed on {L} ({cls}) with progress; retry {len(done) + 1} of {tries}"))
                    elif len(done) >= tries or not at:
                        stuck[alert_id("site", name, "fail", key)] = alert(
                            f"{key} ({a['key']}) failed on {L} with progress ({cls})" + (f" after {len(done)} automatic retries" if done else "") + "; claim kept."
                            + (f" Last log line: {why}." if why else "")
                            + f" taskq show {key} has the end of its log; once fixed: taskq retry {key}; to give up: taskq.py release --worker {name} -- {key}",
                            state="failed", progress=True, failures=a.get("failures"), why=why or None, cls=cls, retries=len(done))
                else:
                    acts[-1] = dict(op="release", key=key, skip=f"{proto.FAIL_SKIP}{L}" + (f": {why}" if why else ""), failed=True, reason=why, cls=cls,
                                    why=f"failed on {L} with no progress ({cls}); released, {L} takes it again only after a wait (taskq show {key})")
                    acts.append(dict(op="drop", key=key, rkey=a["key"]))
            elif ast == proto.S_BAD:
                stuck[alert_id("site", name, "fail", key)] = alert(f"{a['key']} ({key}) in the {L} queue is {ast}: {a.get('note')}",
                                                                   state=ast, progress=False, failures=a.get("failures"))
        except Exception as e:  # noqa: BLE001  one task's error stays with it: its claim is kept (beat), the other tasks go on
            del acts[n0:]
            acts.append(dict(op="beat", key=t["key"]))
            stuck[alert_id("site", name, "error", t["key"])] = alert(f"{t['key']} on {L}: planning it failed ({type(e).__name__}: {e}"[:300] + "); its claim is kept, the other tasks go on",
                                                                   key=t["key"], error=f"{type(e).__name__}: {e}"[:300])
    mt = {t["key"]: t for t in (mtq or {}).get("tasks", [])}
    mk = {t["key"] for t in mine}
    for a in rs["taskq"]["tasks"]:
        n0 = len(acts)
        try:
            if a["key"] in owned:
                continue
            real = a["key"] if a["key"] in keys else None
            m = mt.get(real) if real else None
            if real is None:
                stuck[alert_id("site", name, "orphan", a["key"])] = alert(f"cannot map {a['key']} in the {L} queue to a task (no placement record on the hub)",
                                                                          unknown=True, state=a["state"])
            elif a["state"] in ("claimed", "busy"):
                stuck[alert_id("site", name, "orphan", real)] = alert(f"{L} is running {real}, but the hub queue does not have it claimed there; compare the hub queue and the {L} queue",
                                                                      unknown=False, state=a["state"], assigned=(m or {}).get("worker"))
            elif a["state"] == "done" and not (m and m["state"] == "done") and not proto.is_smoke(real):
                # finished here, but the hub queue no longer has it on this machine (released, assigned elsewhere or removed) and does
                # not show it as done: its results exist only here, so fetch them first; they are dropped once the store has them
                if not proto.worker_holds(a, now):  # a worker still holding it is still writing results
                    ft = fetched(real, keys[real].get("staged") or 0, f"its results stay on {L}")
                    if ft and m is None:  # not in the hub queue (nothing will show it as done): the fetch verified what it placed
                        acts.append(dict(op="drop", key=real, rkey=a["key"]))
                    elif ft:
                        late(real, ft)
            elif m is None or m["state"] == "done" or m.get("worker") and m["worker"] != name:  # done, or assigned to another executor: drop
                acts.append(dict(op="drop", key=real, rkey=a["key"]))
            elif a["state"] != "stopped":
                acts.append(dict(op="rstop", key=real, rkey=a["key"]))
            elif m["state"] != proto.S_STOPPED and not m.get("worker") and not refill.site_progress(view.get(real)):
                acts.append(dict(op="drop", key=real, rkey=a["key"]))  # resumed in the hub queue unassigned, stopped over there with no progress → drop
        except Exception as e:  # noqa: BLE001  one remote task's error stays with it
            del acts[n0:]
            stuck[alert_id("site", name, "error", a.get("key"))] = alert(f"{a.get('key')} in the {L} queue: planning it failed ({type(e).__name__}: {e}"[:300] + "); the other tasks go on",
                                                                       key=a.get("key"), error=f"{type(e).__name__}: {e}"[:300])
    for real, k in keys.items():  # remote task dropped but paths left (drop_remote recorded dropped): remove them again
        if real not in mk and real not in rt and k.get("dropped"):
            acts.append(dict(op="drop", key=real, rkey=real, gone=True))
    acts += [dict(op="unhold", key=k) for k in holds if k not in gated]
    if rs["taskq"].get("paused"):
        stuck[alert_id("site", name, "fail")] = alert(f"the {L} task queue failed {rs['taskq'].get('recent_failures')} times in {proto.hours(proto.PAUSE_WINDOW)}; claiming is paused",
                                                      paused=True, failures=rs["taskq"].get("recent_failures"))
    return acts, stuck


def site_for(name, rs, st, mtq):
    """This machine's room this round → (refill.Site, smoke gate) or (None, reason not to assign).
    Reserved tasks (claimed but not started: staging, queued over there, or stopped and about to resume) count READY per worker
    kind; reserve out_gb for the outputs of every remote task (queued, running, done but not fetched, plus claimed but not yet
    staged) and add back what they have already written (out_used)."""
    if not any(M.working(name, w) for w in M.workers(name)):
        return None, "no worker is inside its hours window (machines.py hours)"
    c, kinds, keys = RM.conf(name), M.kinds(name), ST.keys_of(st, name)
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}
    spec = {t["key"]: t.get("spec") or {} for t in rs["taskq"]["tasks"]}
    spec.update({t["key"]: t.get("spec") or {} for t in mtq.get("tasks", [])})

    def remote(t):
        return rt.get(t["key"]) if t["key"] in keys else None

    def waiting(t):
        a = remote(t)
        return a is None or a["state"] in (proto.S_FREE, proto.S_STALE, proto.S_STOPPED, proto.S_INPUT)

    view = site_view(name, rs, st)

    def idle(t):  # staged over there, unclaimed and no progress: may yield to a higher-priority task (never one with progress)
        a = remote(t)
        return a is not None and a["state"] == proto.S_FREE and not a.get("worker") and not refill.site_progress(view.get(t["key"]))
    have, mine = {k: 0 for k in kinds}, []
    for t in mtq.get("tasks", []):
        if t.get("worker") == name and t["state"] not in ("done", "stopped") and waiting(t):
            k = refill.kind_of(spec[t["key"]])
            have[k] = have.get(k, 0) + 1
            mine.append(t)
    on = set(rt) | {t["key"] for t in mine if remote(t) is None}
    root_gb = (rs.get("free", 0) + (rs.get("out_used") or 0)) / 1e9 - sum(refill.out_gb(spec.get(k) or {}) for k in on)
    dev, bdev = rs.get("dev"), rs.get("blob_dev", rs.get("dev"))
    rootk = (name, "cap") if c.get("cap") else (c["ssh"], dev)  # with cap it is this machine's own quota; otherwise share one deduction across machines on the same disk
    blobk = rootk if bdev == dev else (name, "blob_cap") if c.get("blob_cap") else (c["ssh"], bdev)
    idle_by = {k: [dict(key=t["key"], nice=t["nice"], out=refill.out_gb(spec[t["key"]]))
                   for t in sorted((t for t in mine if refill.kind_of(spec[t["key"]]) == k and idle(t)), key=lambda t: (-t["nice"], -t["order"]))]
               for k in kinds}
    sg = SG.Gate(name, rs, mtq)

    def block(key, sp):
        g = gate(name, rs, mtq, key)
        return f"the bundle {g[0]} in use cannot run this spec ({g[1]})" if g else sg.verdict(key, sp)
    site = refill.Site(name, {k: R.ready(name) - have[k] for k in kinds}, {p for k in keys.values() for p in k.get("inputs") or []},
                       M.ready_envs(name, rs.get("envs_ok")), (rootk, root_gb), (blobk, rs.get("blob_free", rs.get("free", 0)) / 1e9),
                       c["budget"][1], block=block, took=sg.took, idle=idle_by, busy=bool(on))
    return site, sg


def held_all(ctxs):
    """Half-done tasks on each executor → {key: [executors]} (refill.holders): use what was read this round, and what was
    read at the last check for the rest (hubstate.sites); paused (policy.py PAUSE) or non-executor machines do not count
    (their tasks are opened up to others to start over)."""
    views = {n: (d or {}).get("tasks") or {} for n, d in HS.sites().items()}
    views.update({n: site_view(n, c["rs"], c["st"]) for n, c in ctxs.items() if c.get("rs") is not None})
    return refill.holders(views, lambda n: M.is_exec(n) and not M.paused(n))


def assign(s, ctxs, stuck=None, notes=None):
    """Assign every unclaimed task in the hub queue to executors in one pass (refill.choose). ctxs {executor: dict(rs=remote
    state or None, st=load_state())}. → ({executor: [actions]} (take, drop + release when yielding to make room, smoke_add
    from the smoke gate, ...), {executor: why it got nothing}). Tasks skipped by every possible executor alert tqblocked; smoke
    gate alerts go to stuck and its shadow-mode notes to notes."""
    stuck = {} if stuck is None else stuck
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    if not mtq or not mtq.get("enabled") or mtq.get("paused"):
        off = switches.queue_off()
        return {}, {n: f"hub queue is off ({switches.why(off)})" if off else "hub queue is off or paused" for n in ctxs}
    if "error" in (s.get("pueue", {}).get(M.hub()) or {"error": 1}):  # staging is queued in hub pueue; running tasks are invisible
        return {}, {n: f"cannot read pueue on {M.hub()}" for n in ctxs}
    tasks, bad = [], {}  # bad {key: error}: one task's bad spec or plugin error stays with that task, the others are assigned
    for t in mtq.get("tasks", []):
        if t["state"] != proto.S_FREE or t.get("bad") or not t.get("spec"):
            continue
        try:
            if relayable(refill.info(mtq, t["key"])) and not refill.progress(s, t["key"]):
                tasks.append(t)
        except Exception as e:  # noqa: BLE001
            bad[t["key"]] = f"{type(e).__name__}: {e}"[:300]
    specs = {t["key"]: (t.get("recipe"), t["spec"]) for t in tasks}
    sites, gates, why = {}, {}, {}
    for n, c in ctxs.items():
        if c.get("rs") is None:
            why[n] = "unreachable"
            continue
        site, sg = site_for(n, c["rs"], c["st"], mtq)
        if site is None:
            why[n] = sg
            continue
        sites[n], gates[n] = site, sg
        for t in tasks:  # smoke gate: for tasks this machine can take with a matching kind and envs, queue a smoke and alert if not past it (whether or not there is room this round)
            rec, sp = specs[t["key"]]
            try:
                if n in refill.places(rec, sp) and n not in (t.get("skipped_by") or []) and refill.kind_of(sp) in site.room \
                        and set(S.envs_of(sp)) <= site.envs:
                    sg.verdict(t["key"], sp)
            except Exception as e:  # noqa: BLE001
                bad[t["key"]] = f"{type(e).__name__}: {e}"[:300]
    need = {p for t in tasks for p in specs[t["key"]][1].get("inputs") or [] if any(p not in x.ready for x in sites.values())}
    try:
        sizes = refill.data_sizes(need, lambda pats: RM.store_op("probe", dict(pats=pats, size=True)))
    except RuntimeError as e:
        stuck[alert_id("relay", "store")] = f"cannot measure inputs to transfer on the store ({e}); assigning nothing this round"
        return {}, {n: "cannot measure inputs on the store" for n in ctxs}
    res = refill.choose(tasks, sites, specs, held_all(ctxs), sizes)
    bad.update(res["errors"])
    for k, e in bad.items():
        stuck[alert_id("relaytask", k)] = alert(f"{k} cannot be sent to an executor ({e}); the other tasks are assigned as usual", key=k, error=e)
    for k in res["nowhere"]:
        stuck[alert_id("tqblocked", k)] = alert(f"{k}: every executor it can go to ({', '.join(refill.places(*specs[k]))}) released and skipped it, so it will not start;"
                                                f" check why each released it (taskq show {k}), then taskq retry {k}", key=k, places=refill.places(*specs[k]))
    for k in res["noroom"]:
        stuck[alert_id("tqroom", k)] = alert(f"{k}: no executor it can go to has room ({res['why'][k]}); free space or lower this step's out_gb", after=60, key=k,
                                             places=refill.places(*specs[k]))
    out = {}
    for n, site in sites.items():
        ops = []
        for w, t in site.swaps:
            ops += [dict(op="drop", key=w["key"], rkey=w["key"]),
                    dict(op="release", key=w["key"], why=f"yielded to higher-priority {t['key']} (nice {t['nice']} < {w['nice']})")]
        ops += [dict(op="take", key=t["key"], nice=t["nice"]) for t in site.take]
        sg = gates[n]
        stuck.update(sg.alerts)
        if notes is not None:
            notes.setdefault(n, []).extend(sg.notes)
        out[n] = ops + sg.ops()
        if not out[n]:
            mine = sorted({r for pairs in res["by_site"].values() for site, r in pairs if site == n})
            why[n] = ", ".join(mine[:3]) or "no task in the queue can go to it"
    return out, why


def topup(name, rs, run=None):
    """→ (workers to start, why none, {problem id: alert} for previous workers that failed), decided by the machine's executor
    type (sites/executors.py plan; pueue: each worker in the table, Slurm: refill.slurm_plan); adapters just read state and
    start them; none on a drained machine (taskq drain). run: fn(name, command) → (exit code, output, error), replaced in tests."""
    d = switches.machine_drained(name, [w["name"] for w in M.workers(name)])
    if d:
        return [], f"drained ({switches.why(d)})", {}
    return EX.of(name).plan(name, rs, lambda c: (lambda r: (r[0], r[1] + r[2]))((run or RM.rsh)(name, c)))


def newer_tail(remote, hub):
    """Is the executor's failure tail one the hub queue does not have yet (another time or job)?"""
    if not isinstance(remote, dict) or not isinstance(remote.get("lines"), list):
        return False
    return not isinstance(hub, dict) or (remote.get("time"), remote.get("job")) != (hub.get("time"), hub.get("job"))


def label(op, key):
    return f"{op}:{key}"


def smoke_tail(name, rkey, n=20):
    """Why a smoke task failed: the last lines of the remote failure record and the last n lines of the newest part log;
    cannot read → ""."""
    r, sk = R.q(RM.root(name)), shlex.quote(proto.safe(rkey))
    cmd = (f"tail -n 3 {r}/{ROOT['queue']}/{proto.QUEUE['failures']}/{sk} 2>/dev/null; "
           f"f=$(ls -t {r}/{ROOT['data']}/{proto.TASK_LOGS}/{sk}{proto.SAFE}* 2>/dev/null | head -1); "
           f'[ -n "$f" ] && tail -n {n} "$f"')
    try:
        rc, out, _ = RM.rsh(name, cmd, timeout=60)
    except Exception:  # noqa: BLE001
        return ""
    return out.strip()


def hub_nice(key):
    """key's nice in the hub queue as it is now (a check's snapshot can be minutes old: taskq nice may have changed it and set it
    over there since); not there or unreadable -> None."""
    try:
        return next((t["nice"] for t in taskq.read_tasks() if t["key"] == key), None)
    except (OSError, SystemExit, ValueError):
        return None


def execute(name, acts, s, run_add=None, stuck=None):
    """Run actions → [messages]. A failing step is reported in the messages and later ones still run. Failing to add a hub task
    (stage, fetch, archive) goes to stuck; the next check retries and the alert disappears once it succeeds."""
    L, msgs = RM.conf(name)["label"], []
    stuck = {} if stuck is None else stuck
    raw_add = run_add or refill.add
    what = dict(relay_stage="staging", relay_fetch="fetch", relay_archive="archive")

    def add(host, lab, step, k, args, **kw):
        try:
            return raw_add(host, lab, step, k, args, **kw)
        except Exception as e:  # noqa: BLE001
            stuck[alert_id("relayadd", name, step, k)] = alert(f"{k} for {L}: could not queue the {what[step]} task on {M.hub()} ({RM.err_tail(str(e), 200)}); will retry next check",
                                                                machine=name, step=step, key=k)
            raise

    def master(args):
        r = taskq.run(args)
        return r[0], r[1] + r[2]
    beats = [a["key"] for a in acts if a["op"] == "beat"]
    lost = set()  # no longer assigned to it in the hub queue (released or reassigned elsewhere): do not touch these this round
    if beats:
        rc, out, err = taskq.run(["beat", "--strict", "--worker", name, "--"] + beats)
        if rc == proto.EXIT["lost"]:
            lost = set(out.split()) & set(beats)
            msgs.append(f"{L} no longer holds the claim on: {', '.join(sorted(lost))}; leaving them alone this round")
        elif rc:
            msgs.append(f"could not renew the claim for {L} ({RM.err_tail(out + err, 150)})")
    kept = set()  # not removed over there: do not release in the hub queue (another executor would claim it and run it in two places)
    for a in acts:
        k = a["key"]
        if k in lost:
            continue
        try:
            if a["op"] == "smoke_add":  # add to the hub queue, then claim it (take → stage)
                ok, m = SG.enqueue(name, a)
                msgs.append(m)
                if ok is None:  # one is already queued
                    continue
                if not ok:
                    stuck[alert_id("smoke", name, a["mark"]["kid"])] = alert(m, machine=name, kind=a["mark"]["kind"], why=m, key=k)
                    continue
                a = dict(op="take", key=k, nice=proto.SMOKE_NICE)
            if a["op"] in ("release", "smoke_end") and k in kept:
                msgs.append(f"{k} was not removed from {L}; not releasing yet")
            elif a["op"] == "smoke_fail":
                SG.record(name, a["mark"], state="failed", src="smoke", key=None, bundle=None, why=a["why"][-600:])
                msgs.append(f"smoke for {a['mark']['kind']} on {L} did not pass: {a['why'][-200:]}")
            elif a["op"] == "smoke_rec":
                why = a["why"] if a["ok"] else (a["why"] + "\n" + smoke_tail(name, a["rkey"])).strip()
                SG.finish(name, k, a["ok"], why)
                msgs.append(f"smoke {k} on {L} {'passed' if a['ok'] else 'did not pass: ' + why[-200:]}")
            elif a["op"] == "smoke_end":
                master(f"release --worker {name} -- {shlex.quote(k)}")
                r = master(f"remove -- {shlex.quote(k)}")
                msgs.append(f"smoke {k} removed from the hub queue for {L}" if r[0] in (0, proto.EXIT["gone"]) else f"could not remove smoke {k} for {L}: {RM.err_tail(r[1], 150)}")
            elif a["op"] == "smoke_real":
                if SG.real_done(name, a["mark"]):
                    msgs.append(f"the real {a['mark']['kind']} task {k} on {L} is done; recorded as passing smoke")
            elif a["op"] == "release":
                r = master(f"release --worker {name}" + (f" --skip {shlex.quote(a['skip'])}" if a.get("skip") else "") + f" -- {shlex.quote(k)}")
                msgs.append(f"{L} released {k}: {a['why']}" if r and r[0] == 0 else f"{L} failed to release {k}: {RM.err_tail((r or [0, 'ssh unreachable'])[1], 150)}")
                if r and r[0] == 0 and a.get("failed"):  # counted as a failure (taskq status: last 24 hours)
                    events.add("relay", f"{k} failed on {L} ({a.get('cls') or 'error'})" + (f": {a['reason']}" if a.get("reason") else ""), kind=proto.EV_FAILED,
                               key=k, site=name, data=dict(why=a.get("reason"), cls=a.get("cls")))
            elif a["op"] == "failtail":
                if not taskq.write_fail_tail(k, a["tail"]):
                    msgs.append(f"could not record the end of {k}'s log from {L} in the hub queue")
            elif a["op"] == "take":
                r = master(f"take --worker {name} --assign -- {shlex.quote(k)}")
                if r is None or r[0]:
                    msgs.append(f"{L} did not claim {k}: {RM.err_tail((r or [0, 'ssh unreachable'])[1], 150)}")
                    continue
                msgs.append(f"{L} claimed {k} (nice {a['nice']})")
                for t in (s.get("taskq", {}).get(taskq.queue_host()) or {}).get("tasks", []):  # stop later executors in this check from picking it
                    if t["key"] == k:
                        t.update(worker=name, assigned=name, state="claimed")
                a = dict(a, op="stage")
            if a["op"] in ("stage", "restage"):
                lab = label("relay_stage", k)
                cmd = f"python3 {shlex.quote(cfg.local('queue', 'relay.py'))} stage {name} {shlex.quote(k)} " + (
                    "--no-add" if a["op"] == "restage" else f"--nice {int(a['nice'])}")
                tid = add(M.hub(), lab, "relay_stage", k, f"{proto.IO_GROUP} -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group=proto.IO_GROUP)
                msgs.append(f"queued {'restage of missing inputs' if a['op'] == 'restage' else 'staging'} for {k} to {L} ({M.hub()} task {tid})")
            elif a["op"] == "fetch":
                lab = label("relay_fetch", k)
                cmd = f"python3 {shlex.quote(cfg.local('queue', 'relay.py'))} fetch {name} {shlex.quote(k)}"
                tid = add(M.hub(), lab, "relay_fetch", k, f"{proto.IO_GROUP} -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group=proto.IO_GROUP)
                msgs.append(f"{k} is done on {L}; queued fetch ({M.hub()} task {tid})")
            elif a["op"] == "archive":
                lab = label("relay_archive", k)
                cmd = f"python3 {shlex.quote(cfg.local('queue', 'relay.py'))} archive {shlex.quote(k)}"
                tid = add(M.hub(), lab, "relay_archive", k, f"{proto.IO_GROUP} -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group=proto.IO_GROUP)
                msgs.append(f"queued archive of {k} from {L} ({M.hub()} task {tid})")
            elif a["op"] == "drop":
                if a.get("cleanup"):
                    ST.update_state(lambda st: ((st.get("keys") or {}).get(name, {}).get(k) or {}).update(
                        cleanup=sorted(set(((st.get("keys") or {}).get(name, {}).get(k) or {}).get("cleanup") or []) | set(a["cleanup"]))))
                try:
                    err = RT.drop_remote(name, k, a["rkey"], gone=a.get("gone"))
                except RT.PathsLeft as e:  # task already dropped and cannot run again: release as usual, remove paths next round
                    msgs.append(f"dropped the task for {k} on {L} but not its results ({e}); will retry next check")
                    continue
                if err:
                    msgs.append(f"failed to drop {k} on {L} ({err})")
                    kept.add(k)
                else:
                    msgs.append(f"dropped the task and results for {k} on {L}")
            elif a["op"] == "hold":
                BG.hold(name, k, a["bundle"], a["err"])
                msgs.append(f"the bundle {a['bundle']} in use on {L} cannot run the spec for {k}; not sending yet: {a['err']}")
            elif a["op"] == "unhold":
                BG.unhold(name, k)
                msgs.append(f"{k} is no longer held on {L} (the bundle changed or the task no longer waits to be sent)")
            elif a["op"] == "rnice":
                now_nice = hub_nice(k)
                if now_nice not in (None, a["nice"]):
                    msgs.append(f"{k}: nice changed to {now_nice} in the hub queue after this check read it; left as is on {L}, the next check follows")
                    continue
                r = RM.set_nice(name, [a["rkey"]], a["nice"])
                msgs.append(f"{k} on {L} set to nice {a['nice']}" if r and r[0] == 0 else f"could not change nice for {k} on {L}: {RM.err_tail((r or [0, 'ssh unreachable'])[1], 150)}")
            elif a["op"] == "forget":
                r = RM.rtq(name, f"forget -- {shlex.quote(a['rkey'])}")
                if r and r[0] == 0:
                    msgs.append(f"cleared the failures of {k} on {L} so it resumes there ({a['why']})")
                    events.add("relay", f"{k} retried on {L}: {a['why']}", kind=proto.EV_RETRY, key=k, site=name)
                else:
                    msgs.append(f"could not clear the failures of {k} on {L}: {RM.err_tail((r or [0, 'ssh unreachable'])[1], 150)}")
            elif a["op"] in ("rstop", "rresume"):
                r = RM.rtq(name, ("stop --why stopped-from-hub-queue -- " if a["op"] == "rstop" else "resume -- ") + shlex.quote(a["rkey"]))
                what = "stopped" if a["op"] == "rstop" else "resumed"
                msgs.append(f"{what} {k} on {L}" if r and r[0] == 0 else f"failed to set {k} {what} on {L}: {RM.err_tail((r or [0, 'ssh unreachable'])[1], 150)}")
        except Exception as e:  # noqa: BLE001
            msgs.append(f"{a['op']} {k} on {L} failed: {e}")
            if a["op"] == "drop":
                kept.add(k)
    return msgs


def submit(name, subs):
    """Start workers as topup decided (the executor type's start) → (messages, failures {problem id: text})."""
    def run(c):
        r = RM.rsh(name, c)
        return r[0], r[1] + r[2]
    return EX.of(name).start(name, subs, run)


def site_view(name, rs, st):
    """Remote status → {real key: dict(state, worker, runs, final, resumable, debug, since)} (tasks of hubstate.sites; since: when a worker there claimed it)."""
    rt = {t["key"]: t for t in (rs.get("taskq") or {}).get("tasks") or []}
    out = {}
    for real in ST.keys_of(st, name):
        a = rt.get(real)
        if a:
            f = (rs.get("tasks") or {}).get(real) or {}
            out[real] = dict(state=a["state"], worker=a.get("worker"), runs=f.get("runs") or [], final=f.get("final"),
                             resumable=f.get("resumable"), ran=a.get("progress"), debug=f.get("debug") or {}, since=a.get("since"))
    return out


def write_status(name, rs, st):
    out = site_view(name, rs, st)
    mine = tuple(proto.safe(k) + proto.SAFE for k in out)  # part logs of the tasks recorded here (taskq log, taskq status --all --json parts)
    segs = [x for x in rs.get("seglogs") or [] if mine and x.get("name", "").startswith(mine)]
    HS.put_site(name, dict(time=time.time(), label=RM.conf(name)["label"], tasks=out, seglogs=segs, squeue=rs.get("squeue"),
                           free=rs.get("free"), bundle=rs.get("bundle"), live=rs.get("live"), held=BG.held(st, name),
                           envs_ok=rs.get("envs_ok")))


def log(lines, name):
    """Lines about one machine in the event log, with its key in the site field (taskq events --site)."""
    events.lines("relay", lines, site=name)


def envs_switch(name):
    """policy.py AUTO["envs"] (taskq switch auto envs on|off overrides whether) → (enabled, extra envs to prebuild for this
    machine, each an env or (env, variant))."""
    a = P.AUTO.get("envs")
    extra = list(a.get(name) or []) if isinstance(a, dict) else []
    return switches.auto("envs", bool(a)), extra


def env_builds(s, name, rows, now, dry, stuck):
    """Envs built from their spec (sites/envs.py): not built, spec changed, build stopped, or out of space past RETRY_S → queue
    one taskq site env in the hub's IO group (proto.IO_GROUP) (one at a time per machine; skip when one is already queued);
    broken builds, no space or missing wheelhouse packages alert site:<machine>:env:<env>. With AUTO["envs"] off, only log to
    relay.log. rows, now: remote marker files and time (envs.read_rows or envs_st from the status script). → messages"""
    on, extra = envs_switch(name)
    want = EV.wanted(name, extra)
    if not want:
        return []
    st = EV.classify(rows, want, now)
    probs = EV.problems(name, want, st, rows)
    if on:
        for env, msg in probs:
            stuck[alert_id("site", name, "env", env)] = alert(msg[:1500], machine=name, sub="env", key=env, state=st.get(env))
    nb = EV.next_build(want, st, rows, now, skip={k for k, _ in probs if st.get(k) not in (proto.ENV_STATE["failed"], proto.ENV_STATE["nospace"])})
    if nb is None:
        return []
    env, v = nb
    hub, L = M.hub(), M.machine(name)["label"]
    d = (s.get("pueue") or {}).get(hub) or {}
    if "tasks" not in d:  # hub pueue unreadable: cannot tell whether one is queued, so skip this time
        return []
    lab = f"{proto.ENV_BUILD}:{name}:{env}"
    if any((t.get("label") or "").startswith(f"{proto.ENV_BUILD}:{name}:") and t.get("status") in LIVE for t in d["tasks"]):
        return []
    what = f"env {env} on {L} ({v}, {st[env]})"
    if dry:
        return [f"would build {what}"]
    if not on:
        if C.changed(f"envs_{name}", lab):
            log([f"(switch off) would build {what}"], name)
        return []
    cmd = f"python3 {shlex.quote(cfg.local('sites', 'envs_cli.py'))} env {name} {shlex.quote(env)} --variant {shlex.quote(v)}"
    try:
        tid = refill.add(hub, lab, proto.ENV_BUILD, f"{name}:{env}",
                         f"{proto.IO_GROUP} -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)), to=name, group=proto.IO_GROUP)
    except Exception as e:  # noqa: BLE001
        stuck[alert_id("relayadd", name, proto.ENV_BUILD, env)] = alert(f"env {env} on {L}: could not queue the env build on {hub} ({RM.err_tail(str(e), 200)}); will retry next check",
                                                                         machine=name, step=proto.ENV_BUILD, key=env)
        return []
    return [f"queued build of {what} ({M.hub()} task {tid})"]


def run_all(s, dry=False, times=None):
    """Called by health.py → (messages, stuck {problem id: text}). Read and prepare each executor (prepare), assign unclaimed
    hub tasks to them in one pass (assign), act per machine (finish), then the store's daily orphan sweep (sweep_orphans).
    times: a dict that gets each part's seconds (code, prepare:<machine>, assign, finish:<machine>, orphans)."""
    events, stuck, ctxs = [], {}, {}
    times = {} if times is None else times
    t0 = [time.time()]

    def lap(part):
        t = time.time()
        times[part] = round(t - t0[0], 1)
        t0[0] = t
    if not dry:  # switch bundles first: store and executor operations (hub queue status, collect) use them, even when the hub queue is unreadable (a fresh install or format change recovers on its own)
        events += update_code(stuck)
        lap("code")
    if not s["ok"].get(M.hub()) or not s.get("taskq", {}).get(taskq.queue_host()):
        return events, stuck
    for name in M.executors():
        if M.paused(name):
            continue
        try:
            ctxs[name] = prepare(s, name, dry)
        except Exception as e:  # noqa: BLE001
            stuck[alert_id("relay", name)] = f"queue/relay.py ({name}) failed: {type(e).__name__}: {e}"[:300]
        lap(f"prepare:{name}")
    snotes = {}
    try:
        more, why = assign(s, ctxs, stuck, snotes)
    except Exception as e:  # noqa: BLE001
        more, why = {}, {n: "assignment failed" for n in ctxs}
        stuck[alert_id("relay", "assign")] = f"queue/relay.py assignment failed: {type(e).__name__}: {e}"[:300]
    lap("assign")
    for name, c in ctxs.items():
        try:
            ev, st_ = finish(s, name, c, more.get(name, []), why.get(name, ""), snotes.get(name) or [], dry)
        except Exception as e:  # noqa: BLE001
            ev, st_ = [], {alert_id("relay", name): f"queue/relay.py ({name}) failed: {type(e).__name__}: {e}"[:300]}
        events += ev
        stuck.update(st_)
        lap(f"finish:{name}")
    if not dry:
        try:
            events += sweep_orphans(ctxs)
        except Exception as e:  # noqa: BLE001
            stuck[alert_id("relay", "orphans")] = f"queue/relay.py orphan sweep failed: {type(e).__name__}: {e}"[:300]
        try:
            prune_bundles(wanted_bundle()[0])
        except Exception as e:  # noqa: BLE001
            events.append(f"failed to clean up old bundles on the hub: {e}")
        lap("orphans")
    return events, stuck


def prepare(s, name, dry):
    """Read remote state (deploy first when unreadable or the bundle differs, then read again), clean up and plan this
    machine's actions → dict(rs, st, acts, stuck, notes)."""
    st, L = ST.load_state(), RM.conf(name)["label"]
    rs = remote_status(name)
    notes = []
    # status unreadable but reachable: no bundle yet (new machine) or the old remote bundle cannot run the new status script → deploy first, then read again
    cur = current_bundle(name) if rs is None and not dry else None
    ph = {"bundle": cur or None} if cur is not None else None
    if ph is not None:
        rs = ph
    if not dry and rs is not None:
        try:
            bid, path = wanted_bundle()
            if rs.get("bundle") != bid:
                try:
                    deploy(name, bid, path, claim_times(rs))
                    notes.append(f"{L} switched to code {bid}")
                    rs = remote_status(name)
                except RuntimeError as e:
                    notes.append(str(e))
                    rs = None  # still the old bundle over there, whose commands and paths may not match: treat as unreachable, only beat, push next time
        except Exception as e:  # noqa: BLE001
            notes.append(f"building or pushing the bundle to {L} failed: {e}")
            rs = None  # same: on timeout or build failure the remote side still has the old bundle
    if ph is not None and rs is ph:  # no deploy (same bundle) or still unreadable after deploying
        rs = None
    if rs is not None and not dry:
        ST.update_state(lambda x: x.setdefault("last_ok", {}).update({name: time.time()}))
        write_status(name, rs, ST.load_state())
    if rs is not None and not dry:
        msg, freed = cleanup(name, rs)
        notes += msg
        if freed:
            rs = remote_status(name) or rs
    st = ST.load_state()
    acts, stuck = plan(s, name, rs, st)
    if rs is not None and "envs_st" in rs:
        notes += env_builds(s, name, rs["envs_st"], rs["now"], dry, stuck)
    return dict(rs=rs, st=st, acts=acts, stuck=stuck, notes=notes)


def cleanup(name, rs, orphans=False):
    """Remove data views and blobs nothing needs on name (relay_store.gc; orphans: also the store's orphan sweep, which records
    orphans_at when it ran) → (notes, whether anything was removed)."""
    L = RM.conf(name)["label"]
    try:
        st = ST.load_state()
        reg = (st.get("views") or {}).get(name) or {}
        keep = {v for k in ST.keys_of(st, name).values() for v in ST.paths_of(k)}
        keep |= {v for v, t in reg.items() if time.time() - t < proto.VIEW_KEEP}
        g = ST.gc(name, keep, RM.op, views=reg, free=rs.get("free") if RM.conf(name).get("cap") else None,
                  blob_free=rs.get("blob_free") if RM.conf(name).get("blob_cap") else None, orphans=orphans)
        if orphans and g is not None:
            ST.update_state(lambda x: x.setdefault("orphans_at", {}).update({name: time.time()}))
        if g and g["removed_views"]:
            ST.update_state(lambda x: [((x.get("views") or {}).get(name) or {}).pop(v, None) for v in g["removed_views"]])
        if g and (g["removed_views"] or g["removed_blobs"]):
            return [f"{L} cleanup: removed {len(g['removed_views'])} data views and {g['removed_blobs']} blobs,"
                    f" freed {g['freed'] / 1e9:.0f} GB, {g['free'] / 1e9:.0f} GB left"], True
    except Exception as e:  # noqa: BLE001
        return [f"{L} cleanup failed: {e}"], False
    return [], False


def sweep_orphans(ctxs):
    """The store's orphan sweep, once per proto.ORPHAN_EVERY, after every executor has been sent its work: it walks the store's
    whole data and blob dirs (minutes on cold metadata), and the sites are handled one after another → messages."""
    name = M.store()
    c = ctxs.get(name)
    if not c or c["rs"] is None or time.time() - (ST.load_state().get("orphans_at") or {}).get(name, 0) < proto.ORPHAN_EVERY:
        return []
    msgs = cleanup(name, c["rs"], orphans=True)[0]
    log(msgs, name)
    return msgs


def push_switches(name, rs, dry, now=False, run=None):
    """Bring machine name's copy of its switches up to date: switches.site_doc → ROOT["overrides"] (read by consumers/drain.py),
    taskq switch gpu_pad → the GPU pad file (removed only when the hub made it), and SIGTERM the pueue workers a drain "on" covers
    that still run (they release their tasks and exit 0; a Slurm worker stops on reading the file). Writes only what differs
    from what was last written there (meta switches_pushed:<machine>). rs: this round's remote state (its pueue jobs), or None
    (now=True: taskq drain right after the change; a paused machine or a down ssh master is left to the next round).
    → (lines, {problem id: alert}); dry: the lines only."""
    m = RM.conf(name)
    L = m["label"]
    ws = M.workers(name)
    doc = switches.site_doc(name, {w["name"]: w["id"] for w in ws})
    pad = switches.gpu_pad_off(name)
    was = switches.pushed(name) or {}
    jobs = (rs or {}).get("jobs")
    kill = [w for w in ws if (switches.drained(name, w["name"]) or {}).get("value") == "on" and m.get("scheduler") == "pueue"
            and (jobs is None or any(j[2] != "Done" for j in EX.of(name).tasks_of(w, jobs)))]
    write = doc != was.get("doc", {}) or pad != bool(was.get("pad"))
    if not write and not kill:
        return [], {}
    what = (["switches" if doc else "no switches"] if doc != was.get("doc", {}) else []) \
        + ([f"GPU pad {'off' if pad else 'back on'}"] if pad != bool(was.get("pad")) else []) + [f"stop {w['id']}" for w in kill]
    if dry:
        return [f"{L}: would push {', '.join(what)}"], {}
    if now and (M.paused(name) or RM.master_down(m)):
        return [f"{L}: not contacted now (paused, or its ssh master connection is down); the next health check pushes it"], {}
    run = run or (lambda c, i=None: RM.rsh(name, c, input=i))
    msgs, stuck, aid = [], {}, alert_id("site", name, "switches")
    if write:
        r, f = R.q(RM.root(name)), proto.ROOT["overrides"]
        pf = f"{r}/{proto.ROOT['queue']}/{proto.QUEUE['gpu_pad_off']}"
        cmd = (f"mkdir -p {r}/{os.path.dirname(f)} && cat > {r}/{f}.tmp && mv {r}/{f}.tmp {r}/{f}"
               + (f" && touch {pf}" if pad else f" && rm -f {pf}" if was.get("pad") else ""))
        res = run(cmd, json.dumps(doc, ensure_ascii=False))
        if res[0] != 0:
            stuck[aid] = alert(f"cannot write the switches on {L} ({RM.err_tail(res[1] + res[2], 150)}); a drain or GPU pad change"
                               " there waits until it can", after=60)
            return [], stuck
        switches.mark_pushed(name, dict(doc=doc, pad=pad))
        msgs.append(f"{L}: pushed {', '.join(x for x in what if not x.startswith('stop '))}")
    for w in kill:
        ok, out = EX.of(name).stop_worker(name, lambda c: (lambda r: (r[0], r[1] + r[2]))(run(c)), w)
        msgs.append(f"{L}: {'stopped' if ok else 'could not stop'} {w['id']} (drained){'' if ok else ': ' + out}")
    return msgs, stuck


def finish(s, name, c, more, why, snotes, dry):
    """Carry out prepare's actions and assign's tasks, push switches, then top up workers → (messages, stuck)."""
    L, rs, acts, stuck, notes = RM.conf(name)["label"], c["rs"], c["acts"], c["stuck"], c["notes"]
    if snotes and not dry and C.changed(f"smoke_{name}", snotes):
        log(snotes, name)
    sw, sst = push_switches(name, rs, dry) if rs is not None else ([], {})
    stuck.update(sst)
    notes = notes + sw
    subs, swhy, failed = (topup(name, rs) if rs is not None else ([], "unreachable", {}))
    stuck.update(failed)
    todo = [a for a in acts if a["op"] != "beat"]
    desc = ", ".join(f"{a['op']} {a['key']}" for a in todo + more)
    sdesc = ", ".join(EX.of(name).describe(x) for x in subs)
    if dry:
        return [f"{L}: would {desc}" if desc else f"{L}: nothing to do ({why})",
                f"{L}: would {sdesc}" if sdesc else f"{L}: submitting no jobs ({swhy})"] + notes, stuck
    msgs = notes + execute(name, acts, s, stuck=stuck) + execute(name, more, s, stuck=stuck)
    m2, st2 = submit(name, subs)
    msgs += m2
    stuck.update(st2)
    log(msgs, name)
    return msgs, stuck


def main(argv):
    ap = argparse.ArgumentParser(description="Relay for executors (taskq plan shows what each executor would do now)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("stage", help="stage inputs and add to the remote queue")
    p.add_argument("name")
    p.add_argument("key")
    p.add_argument("--nice", type=int)
    p.add_argument("--no-add", action="store_true")
    p = sub.add_parser("fetch", help="fetch a finished task, verify it and put it in the store data dir")
    p.add_argument("name")
    p.add_argument("key")
    p = sub.add_parser("archive", help="back up this task's store results to the backup machine and verify")
    p.add_argument("key")
    p = sub.add_parser("bundle", help="push the wanted bundle and switch current (an executor, or a store/hub with a root)")
    p.add_argument("name")
    p = sub.add_parser("status", help="remote queue")
    p.add_argument("name")
    p = sub.add_parser("resend", help="replace staged tasks' specs in the remote queue with the hub's current ones")
    p.add_argument("name")
    a = ap.parse_args(argv)
    if getattr(a, "name", None):
        a.name = machine_name.key(a.name)
    why = C.format_problem() if a.cmd in ("stage", "fetch", "archive") else None
    if why:  # steps of a hub round: not on state in another format (taskq self migrate first)
        print(f"{ap.prog} {a.cmd}: not run: {why}", file=sys.stderr)
        return 1
    if a.cmd == "archive":
        try:
            RT.archive(a.key)
        except (RN.RelayError, S.SpecError, RuntimeError) as e:
            print(f"{ap.prog} archive {a.key}: {e}", file=sys.stderr)
            return proto.EXIT["gone"] if isinstance(e, RT.Gone) else 1
        return 0
    if a.cmd == "bundle" and a.name in code_only():
        print(bundle(a.name, current_bundle(a.name), claims=[]) or "unchanged")
        return 0
    if not M.is_exec(a.name):
        print(f"{ap.prog} {a.cmd}: no executor {a.name} (have: {', '.join(M.executors()) or 'none'})", file=sys.stderr)
        return 2 if a.cmd == "stage" else proto.EXIT["gone"]
    if a.cmd in ("stage", "fetch"):
        if a.cmd == "stage" and a.nice is None and not a.no_add:
            ap.error("--nice is required (or use --no-add)")
        try:
            if a.cmd == "fetch":
                RT.fetch(a.name, a.key)
            elif RT.stage(a.name, a.key, a.nice, add=not a.no_add) is None:
                return NOSLOT_RC
        except (RN.RelayError, S.SpecError, RuntimeError, OSError, subprocess.SubprocessError) as e:
            if a.cmd == "stage":
                print(f"relay.py {a.cmd} {a.name} {a.key}: {type(e).__name__}: {e}", file=sys.stderr)
                return 1
            print(f"{ap.prog} {a.cmd} {a.name}: {e}", file=sys.stderr)
            return proto.EXIT["gone"] if isinstance(e, RT.Gone) else 1
    elif a.cmd == "bundle":
        rs = remote_status(a.name)
        print(bundle(a.name, (rs or {}).get("bundle"), claim_times(rs)) or "unchanged")
    elif a.cmd == "resend":
        RT.resend(a.name)
    elif a.cmd == "status":
        rs = remote_status(a.name)
        if rs is None:
            print(f"cannot read {a.name}", file=sys.stderr)
            return 1
        print(taskq.queue_text(rs["taskq"]))
        jobs = rs.get("squeue")
        if jobs is not None:  # Slurm rows: id, name, state, time, partition, reason (node when running)
            why = {}
            for j in jobs:
                if j[2] != "RUNNING":
                    why[j[-1].strip("()")] = why.get(j[-1].strip("()"), 0) + 1
            n = sum(j[2] == "RUNNING" for j in jobs)
            jobs = f"; jobs {n} running, {len(jobs) - n} queued" + (f" ({', '.join(f'{k} {v}' for k, v in sorted(why.items()))})" if why else "")
        print(f"{rs['free'] / 1e9:.0f} GB free; code {rs['bundle']}{jobs or ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
