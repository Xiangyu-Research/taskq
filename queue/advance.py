#!/usr/bin/env python3
"""Downstream steps (a step's after, group_after, FINAL) on the hub: each health check calls run.

The order comes from flow/compile.py. Each round: queue instances whose dependencies are done; rerun a step when an input
is newer than its done file; delete temp files once every user is done and fetched. HOLD is skipped; with policy.py
AUTO["advance"] false only what would be queued is written.

    python3 queue/advance.py      show what would be queued (changes nothing; taskq plan also shows sending)
"""
import fnmatch
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduling code
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
from proto import alert_id, alert_msg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "monitor"))
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402
import relay_remote as RM  # noqa: E402
import refill  # noqa: E402
from flow import compile as FC  # noqa: E402
from flow.graph import Graph  # noqa: E402
from flow.model import SOURCE  # noqa: E402

NOTES = []  # why nothing was queued this round (written to ~/advance.log, not reported as a problem)
TQ_TIMEOUT = 420  # longer than the longest queue-lock wait (300 s): a lock timeout returns an exit code, not a timeout


def done_items(inst):
    """Instance -> one store done item: dict(out=main dir, done=[[alternative, ...], ...])."""
    if inst.scope == SOURCE:
        o = S.outputs(inst.spec)
        return dict(out=o["outs"][0], done=o["done"])
    return dict(out=inst.outputs[0], done=[d.split("|") for d in inst.step["done"]])


def fetching(s):
    """Keys of tasks whose results are being fetched into the store (relay_fetch running in the hub pueue): not done until fetched."""
    hub = M.hub()
    out = set()
    for t in (s.get("pueue", {}).get(hub) or {}).get("tasks") or []:
        r = C.identify(hub, t, s.get("records") or [])
        if r and r["step"] == "relay_fetch" and t["status"] in refill.LIVE:
            out.add(r["key"])
    return out


def done_files(i):
    """Every done alternative of a step instance (relative to the data dir)."""
    return [f"{i.outputs[0]}/{a}" for d in i.step["done"] for a in d.split("|")]


def queue_params(i):
    """Step instance -> parameters for the queue (recipe command). {data} is left for the executor to expand."""
    st = i.step
    p = dict(key=i.id, step=st["name"], cmd=S.fill(st["cmd"], i.names), env=st["env"] if len(st["env"]) > 1 else st["env"][0],
             out=i.outputs[0], done=list(st["done"]), inputs=list(i.inputs), relay=st["relay"], device=st["device"],
             fetch=dict(exclude=[], extra=[], also=i.outputs[1:]))
    if st.get("res"):
        p["res"] = dict(st["res"])
    if st.get("out_gb") is not None:
        p["out_gb"] = st["out_gb"]
    if st.get("inputs_cmd"):
        p["inputs_cmd"] = S.fill(st["inputs_cmd"], i.names)
    return p


def plan(s):
    """-> (todo [dict(op=add|rerun|temp, id, ...)], stuck {problem id: explanation})."""
    NOTES.clear()
    hub = M.hub()
    mq = (s.get("taskq") or {}).get(hub)
    if not mq:
        return [], {}
    flow = FC.build(queue_tasks=mq["tasks"])
    if flow.errors:
        more = f" (and {len(flow.errors) - 5} more)" if len(flow.errors) > 5 else ""
        return [], {alert_id("advance", "flow"): "The experiment table has bad steps, so nothing was queued this time: " + "; ".join(flow.errors[:5]) + more}
    g = Graph(flow)
    inst = flow.instances
    queued = {t["key"]: t for t in mq["tasks"]}
    moving = fetching(s)
    try:
        miss = RM.store_op("done", dict(items={k: done_items(i) for k, i in inst.items()}))
    except RuntimeError as e:
        return [], {alert_id("advance", "store"): f"cannot check results on the store ({e}), not queueing this round"}
    done = {k for k, m in miss.items() if not m and k not in moving}
    reruns = {}  # instance id -> done files to delete
    cand = [i for i in inst.values() if i.step and i.step["rerun"] and i.id in done
            and (queued.get(i.id) or {}).get("state") in (None, proto.S_DONE)]
    if cand:
        files = {i.id: done_files(i) for i in cand}
        try:
            pr = RM.store_op("probe", dict(pats=sorted({p for i in cand for p in i.inputs + files[i.id]})))
        except RuntimeError as e:
            pr = {}
            NOTES.append(f"cannot check modification times on the store ({e}), not checking for reruns this round")
        newest = lambda ps: max((pr[p]["newest"] or 0 for p in ps if p in pr), default=0)  # noqa: E731
        for i in cand:
            if pr and newest(i.inputs) > newest(files[i.id]) and g.ancestors(i.id) <= done:
                done.discard(i.id)
                reruns[i.id] = files[i.id]
    todo, stuck = [], {}
    for k in g.order():
        i = inst[k]
        if i.scope == SOURCE or k in done:
            continue
        wait = [d for d in i.deps if d not in done]
        if wait or k in moving:  # results still being fetched: not done, do not queue again
            continue
        if k in queued and k not in reruns:
            continue
        if any(fnmatch.fnmatch(k, pat) for pat in P.HOLD):
            NOTES.append(f"{k}: HOLD, not queueing")
            continue
        if k in reruns:
            todo.append(dict(op="rerun", id=k, files=reruns[k]))
        todo.append(dict(op="add", id=k, nice=i.step["nice"], params=queue_params(i), queued=k in queued))
    for i in inst.values():
        if not (i.step and i.step.get("temp")) or i.id not in done:
            continue
        users = g.users.get(i.id, [])
        if users and all(u in done for u in users) and not g.descendants(i.id) & set(reruns):
            pats = [S.fill(f"{i.outputs[0]}/{t}", i.names) for t in i.step["temp"]]
            todo.append(dict(op="temp", id=i.id, pats=pats))
    return todo, stuck


def taskq_add(nice, params):
    """Add to the queue (local taskq.py on the hub) -> (exit code, output)."""
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "add", "--nice", str(nice), "--recipe",
                        *S.kv_args("command", params)], cwd=cfg.REPO, capture_output=True, text=True, timeout=TQ_TIMEOUT)
    return r.returncode, (r.stdout + r.stderr).strip()


def run(s, dry=False, tq=None, store=None):
    """Called by health.py -> (list of what was done ("would ..." when only looking), stuck {problem id: explanation})."""
    tq = tq or taskq_add
    store = store or RM.store_op
    todo, stuck = plan(s)
    live = bool(P.AUTO.get("advance")) and not dry
    did, lines = [], []
    for t in todo:
        if t["op"] == "rerun":
            what = f"{t['id']}: delete the done files on the store and redo ({', '.join(t['files'])})"
            if live:
                try:
                    store("rm", dict(pats=t["files"]))
                    lines.append(f"redid {what}")
                except RuntimeError as e:
                    stuck[alert_id("advance", "rerun", t["id"])] = f"{t['id']}: deleting done files failed ({e})"
            else:
                lines.append(f"would redo {what}")
            continue
        if t["op"] == "temp":
            what = f"temp files of {t['id']}: {', '.join(t['pats'])}"
            if live:
                try:
                    r = store("rm", dict(pats=t["pats"]))
                    if r["n"]:
                        lines.append(f"deleted {what}: {r['n']} files, {r['bytes'] / 1e9:.1f} GB")
                except RuntimeError as e:
                    stuck[alert_id("advance", "temp", t["id"])] = f"deleting {what} failed ({e})"
            else:
                lines.append(f"would delete {what}")
            continue
        if t["queued"]:
            continue  # a redo already in the queue can be claimed again once the done files are gone
        what = f"{t['id']} (nice {t['nice']}): {t['params']['cmd']}"
        if live:
            rc, out = tq(t["nice"], t["params"])
            if rc == 0:
                did.append(t["id"])
                lines.append(f"queued {what}")
            else:
                lines.append(f"error {t['id']}: {out[-200:]}")
                stuck[alert_id("advance", "add", t["id"])] = f"{t['id']}: adding to the queue failed ({out[-200:]})"
        else:
            did.append(f"would queue {t['id']}")
            lines.append(f"would queue {what}")
    lines += [f"not queued: {x}" for x in NOTES]
    if not dry:
        log(lines, stuck, live)
    return did, stuck


def log(lines, stuck, live):
    """Write ~/advance.log: skip a repeat of the last lines (same "would queue", "not queued", stuck)."""
    acted = ("queued ", "redid ", "deleted ")
    cur = dict(lines=[ln for ln in lines if not ln.startswith(acted)], stuck=sorted(stuck))
    out = [ln for ln in lines if ln.startswith(acted)]
    if C.changed("advance", cur):
        out += cur["lines"] + [f"stuck {stuck[k]}" for k in cur["stuck"]]
        if not out:
            out = ["nothing to queue"]
    if out:
        import events
        events.lines("advance", out, kind=None if live else "dry")


def describe(todo, stuck):
    """plan() result -> lines for people."""
    out = []
    for t in todo:
        if t["op"] == "add" and not t["queued"]:
            out.append(f"would queue {t['id']:36} nice {t['nice']:<5} {t['params']['cmd']}")
        elif t["op"] == "rerun":
            out.append(f"would redo {t['id']:34} delete {', '.join(t['files'])}")
        elif t["op"] == "temp":
            out.append(f"would delete temp files of {t['id']:36}: {', '.join(t['pats'])}")
    out += [f"not queued: {x}" for x in NOTES]
    out += [f"stuck: {alert_msg(v)}" for v in stuck.values()]
    if not out:
        out.append("nothing to queue")
    return out


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="Downstream steps: show what would be queued (the health check calls run to queue for real)")
    ap.add_argument("-n", "--dry-run", "--dry", action="store_true", help="only look (the default)")
    ap.parse_args(argv)
    import health as H  # noqa: E402
    print("\n".join(describe(*plan(H.gather()))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
