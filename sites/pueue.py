"""The pueue adapter (machines with scheduler="pueue" in the table): read the tasks in a worker's group and start
resident workers. Counterpart of slurm.py: read state, the caller decides whether to start one (relay.py topup),
then start_worker. pueue's raw output is read once by the remote status script (site_ops.py status); this only parses."""
import json
import os
import shlex
import sys

import remote as R

sys.path.insert(0, os.path.join(R.CORE, "queue"))
import cfg  # noqa: E402
cfg.use_config()
import mtab as M  # noqa: E402
import proto  # noqa: E402
from proto import ROOT  # noqa: E402


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


def workers(w, jobs):
    """Worker w's (an mtab.workers item) pueue tasks (label worker:<id>), by task id."""
    return [j for j in jobs or [] if j[1] == f"{proto.WORKER_LABEL}{w['id']}"]



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
