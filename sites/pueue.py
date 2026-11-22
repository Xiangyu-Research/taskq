"""The pueue executor type (machines with scheduler="pueue" in the table; interface in executors.py): resident workers, one
per workers entry, each in its own pueue group. pueue's raw output is read once by the remote status script (site_ops.py status,
read_status); the hub parses it, refill.pueue_plan decides which workers to start and start_worker starts them."""
import json
import os
import shlex
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import remote as R  # noqa: E402

sys.path.insert(0, os.path.join(R.CORE, "queue"))
import cfg  # noqa: E402
cfg.use_config()
import mtab as M  # noqa: E402
import proto  # noqa: E402
import schema  # noqa: E402
from proto import ROOT, alert, alert_id  # noqa: E402

RESIDENT = True


def check(name, m):
    return [] if m.get("workers") else ["a pueue executor needs workers"]


def workers(name, m):
    """One per workers entry, named gpu<card> or cpu (cpu<n> when there are several CPU workers), with a pueue group of the
    same name."""
    ws = m["workers"] or ()
    ncpu = sum(1 for w in ws if w.get("gpu") is None)
    icpu, out = 0, []
    for w in ws:
        w = schema.defaults(schema.WORKER_FIELDS, w)
        if w["gpu"] is not None:
            wn = "gpu" + str(w["gpu"]).replace(",", "")
        else:
            icpu += 1
            wn = "cpu" if ncpu == 1 else f"cpu{icpu}"
        out.append(dict(w, name=wn, id=f"{name}-{wn}", kind="gpu" if w["gpu"] is not None else "cpu", group=wn))
    return out


def groups(name, ws):
    """One group per worker (one at a time), plus the core's own group (proto.IO_GROUP)."""
    out = {w["group"]: (1, w["gpu"], {}) for w in ws if w["group"]}
    out[proto.IO_GROUP] = (0, None, {})
    return out


def status_args(name):
    return dict(pueue=M.machine(name).get("pueue"))


def read_status(A):
    q = subprocess.run([os.path.expanduser(A["pueue"]), "status", "--json"], capture_output=True, text=True, timeout=60)
    return q.stdout if q.returncode == 0 else None


def parse_status(name, raw):
    """→ jobs: the tasks in this machine's worker groups (parse)."""
    return dict(jobs=parse(raw, [w["group"] for w in M.workers(name)]))


def parse(raw, group):
    """The output of `pueue status --json` (text or already parsed) → [[task id, label, status, result or None], ...]
    (only this group; group may be a list of groups); unreadable → None."""
    groups = [group] if isinstance(group, str) or group is None else list(group)
    try:
        tasks = (json.loads(raw) if isinstance(raw, str) else raw)["tasks"]
    except (TypeError, ValueError, KeyError):
        return None
    jobs = []
    for i, t in sorted(tasks.items(), key=lambda x: int(x[0])):
        s = t["status"]
        st = s if isinstance(s, str) else list(s)[0]
        done = s.get("Done") if isinstance(s, dict) else None
        res = done.get("result") if isinstance(done, dict) else None
        if t.get("group") in groups:
            jobs.append([int(i), t.get("label") or "", st, str(res) if res is not None else None])
    return jobs


def tasks_of(w, jobs):
    """Worker w's (an mtab.workers item) pueue tasks (label worker:<id>), by task id."""
    return [j for j in jobs or [] if j[1] == f"{proto.WORKER_LABEL}{w['id']}"]


def plan(name, rs, run):
    """refill.pueue_plan: subs are mtab.workers items."""
    import refill
    return refill.pueue_plan(name, rs)


def describe(w):
    return f"start {w['id']}"


def bundles(cur, live):
    """A pueue worker stops claiming new tasks when its code changes: only current."""
    return [cur]


def env_launch(name, m, minutes):
    return None


def worker_cmd(name, w):
    """The pueue add command that starts worker w (an mtab.workers item). The whole bash -c is one argument: pueue
    joins the args after -- with spaces and hands them to sh. It starts with exec so pueue's child is the worker
    itself; without it sh gets pueue kill's SIGTERM first, pueue marks the task done at once while the worker is still
    stopping the task and releasing it. pueue add carries the environment of the side that adds: strip the hub's own
    variables first (proto.HUB_ONLY_ENV), so a worker on the hub's machine does not take the hub's config and queue as
    its own."""
    c = M.machine(name)
    env = {"TASKQ_MACHINE": name, **M.worker_env(w)}
    env = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env.items())
    run = (f'unset {" ".join(proto.HUB_ONLY_ENV)}; R={R.q(c["root"].rstrip("/"))}; REPO=$(readlink -f "$R/{ROOT["current"]}"); '
           f'export TASKQ_ROOT="$R" {env}; exec bash "$REPO/{cfg.bundle_rel("queue", "worker_start.sh")}"')
    return f"{c['pueue']} add -g {w['group']} -l {proto.WORKER_LABEL}{w['id']} -- {shlex.quote('exec bash -c ' + shlex.quote(run))}"


def start_worker(name, run, w):
    """Start worker w → (ok, output). run: fn(command) → (exit code, output)."""
    rc, out = run(worker_cmd(name, w))
    return rc == 0, out.strip()[-200:]


def start(name, subs, run):
    L, msgs, stuck = M.machine(name)["label"], [], {}
    for w in subs:
        ok, out = start_worker(name, run, w)
        if ok:
            msgs.append(f"{L} started {w['id']} ({out[-80:]})")
        else:
            stuck[alert_id("site", name, "worker", w["id"])] = alert(f"failed to start {w['id']} on {L}: {out}", worker=w["id"], start=True)
    return msgs, stuck
