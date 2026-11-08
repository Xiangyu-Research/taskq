"""Plain wording for phone pushes: task key or pueue label -> experiment name; health problem -> what happened and what it affects.
Used by health.py (problem pushes) and overview.py; the detailed descriptions stay in health.py's problem list.
"""
import os
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduling code
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg  # noqa: E402
import proto  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import experiments as E  # noqa: E402

# Display names of core steps; plugin labels.py adds more, steps without one use their name
STEPS = {"train": "training", "archive": "archive", "relay_fetch": "fetch results", "relay_stage": "stage data", "relay_archive": "archive",
         proto.ENV_BUILD: "build env"}
QUIET = ["train", "archive"]  # intermediate steps: not listed in progress reports (plugin ones are appended)
PLAN_TITLES = None  # plugin function: () -> {analysis id: title}
cfg.extend(globals(), "labels")
_tasks = _plan = _steps = _model = None


def table_steps():
    """Steps in the experiment table (per task, per experiment, final) -> {name: definition}."""
    global _steps
    if _steps is None:
        exps, final = E.load()
        _steps = {}
        for st in [x for e in exps for x in e["after"] + e["group_after"]] + final:
            _steps.setdefault(st["name"], st)
    return _steps


def step_name(step):
    """Step name -> display name (plugin labels.py STEPS; the step name itself when missing)."""
    return STEPS.get(step, step)


def known_step(step):
    try:
        return step in STEPS or step in table_steps()
    except Exception:  # noqa: BLE001
        return step in STEPS


def final_step(step):
    try:
        return any(st["name"] == step for st in E.load()[1])
    except Exception:  # noqa: BLE001
        return False


def usual_model():
    """Most common model in the experiment table (left out of experiment names); None when the table is unreadable or has no model."""
    global _model
    if _model is None:
        try:
            n = Counter(e["model"] for e in E.load()[0] if e.get("model"))
        except Exception:  # noqa: BLE001
            n = Counter()
        _model = [n.most_common(1)[0][0] if n else None]
    return _model[0]


def exp_name(e):
    m = e.get("model")
    return f"{e['exp']} {e['label']}" + (f" {m}" if m and m != usual_model() else "")


def key_name(key):
    """Task key -> experiment name + short name (e.g. "exp1 s0"); keys not in the experiment table are returned as is."""
    global _tasks
    try:
        if _tasks is None:
            _tasks = E.tasks()
        t = _tasks[key]
        return " ".join(x for x in (exp_name(E.experiment(t["exp"])), E.short(t)) if x)
    except Exception:  # noqa: BLE001
        return key


def plan_items():
    """Analysis id -> title (plugin labels.py PLAN_TITLES; no plugin -> {})."""
    return dict(PLAN_TITLES() or {}) if PLAN_TITLES else {}


def plan_of(who):
    """The part after the label is <id> or <id>_... and the id has a title -> "id title"; otherwise None."""
    global _plan
    if _plan is None:
        _plan = plan_items()
    k = next((k for k in _plan if who == k or who.startswith(k + "_")), None)
    return f"{k} {_plan[k]}" if k else None


def label(lab):
    """pueue label -> (experiment name, what was done, task short name (E.short, e.g. "f1") or None); intermediate step -> None.
    A final step of the experiment table (no second half) -> ("All experiments", step, None); an analysis with a title
    -> ("Analysis", "id title", None); other one-off tasks -> ("Other", label, None)."""
    step, _, who = lab.partition(":")
    if not who:
        return ("All experiments", step_name(step), None) if final_step(step) else ("Other", lab, None)
    if step in QUIET:
        return None
    if not known_step(step):
        what = plan_of(who)
        return ("Analysis", what, None) if what else ("Other", lab, None)
    e, t = E.parse_label(who)
    if not e:
        return ("Other", lab, None)
    return exp_name(e), step_name(step), (E.short(t) or None) if t else None


def task(lab):
    """pueue label -> one phrase (for problem pushes; intermediate steps included)."""
    step, _, who = lab.partition(":")
    if step == "train":
        return f"{key_name(who)} training"
    if step in QUIET:
        e, t = E.parse_label(who)
        if not e:
            return f"{step_name(step)} task {who}"
        return " ".join(x for x in (exp_name(e), E.short(t) if t else "") if x) + " " + step_name(step)
    r = label(lab)
    if r and r[0] == "Analysis":
        return f"Analysis {r[1]}"
    if r and r[0] == "All experiments":
        return f"{r[1]} (all experiments)"
    if r and r[0] != "Other":
        return " ".join(x for x in (r[0], r[2], r[1]) if x)
    if lab.startswith(proto.WORKER_LABEL):
        return f"worker {who}"
    if who and known_step(step):
        return f"{key_name(who)} {step_name(step)}" if key_name(who) != who else f"{step_name(step)} task {who}"
    return f"one-off task {lab}"


def _cat(*parts):
    """Join sentence parts, adding a space between ASCII words/numbers and neighbours."""
    out = ""
    for x in parts:
        if out and x and (out[-1].isascii() and out[-1].isalnum() or x[0].isascii() and x[0].isalnum()) and out[-1] != " " and x[0] != " ":
            out += " "
        out += x
    return out


def _who(key):
    if "/" in key:
        return key_name(key)
    e = E.experiment(key)
    return exp_name(e) if e else ("all experiments" if key == "all" else key)


def problem(pid, p):
    """Health problem id -> push wording (what happened + effect). Structured facts (counts, progress, ...) come from p["data"]
    (set by health.py and relay.py); when data is missing or unmatched, fall back to p["msg"]."""
    msg, part, d = p["msg"], pid.split(":"), p.get("data") or {}
    kind = part[0]
    L = lambda n: M.MACHINES[n]["label"] if n in M.MACHINES else n  # noqa: E731
    try:
        if kind == "down":
            return f"Cannot read the status of {L(part[1])}: the network is down or its storage is stuck, so it is unknown whether its jobs are still running"
        if kind == "unread" and "n" in d:
            return f"{d['n']} tasks on {L(part[1])} have unreadable files (its storage is probably stuck); they are not sent out for now, the rest continue"
        if kind == "tqblocked":
            key = d.get("key") or pid[len("tqblocked:"):]
            return f"{key_name(key)}: every executor it can go to was tried and could not run it (skipped), so it will not start on its own"
        if kind == "tqroom":
            key = d.get("key") or pid[len("tqroom:"):]
            return f"{key_name(key)}: no executor it can go to has enough disk space, so it will not start until space is freed"
        if kind == "taskq":
            return f"Cannot read the task queue on {L(part[1])}, so its jobs are unknown"
        if kind == "collect":
            return f"Collecting job status on {L(part[1])} failed; results are unknown this round and no task is released"
        if kind == "pueued":
            return f"The task queue on {L(part[1])} is not responding; jobs and analysis there may have stopped"
        if kind == "pueue" and "label" in d:
            return f"{_cat(L(part[1]), task(d['label']), 'failed and stopped')}"
        if kind == "disk":
            what = f"{L(part[1])} {d['path']} has only {d['free_gb']:.0f} GB left"
            return f"{what}; if the disk fills, jobs and archiving fail"
        if kind == "gpulog":
            return f"GPU usage logging on {L(part[1])} stopped (does not affect jobs)"
        if kind == "boot":
            return f"{L(part[1])} rebooted at {(d.get('time') or pid.split(':', 2)[2])[5:16]}; interrupted tasks were put back in the queue, check that they resumed"
        if kind == "advance" and part[1] == "policy":
            return f"Downstream steps are stopped, nothing is queued: {msg.split('. Downstream steps')[0]}. Fix it, commit and push, and the next check recovers automatically"
        if kind == "advance" and part[1] == "error":
            return "The automatic scheduler failed: finished tasks will not queue their downstream steps"
        if kind == "site":  # executor (relay.py): site:<machine>:<sub>[:<key>]
            site, sub, key = L(part[1]), part[2], ":".join(part[3:])
            if sub == "down":
                return f"Cannot read {site}, so it is unknown whether the jobs assigned to it are still running"
            if sub == "env" and d.get("state") == proto.ENV_STATE["nospace"]:
                return f"{site} is out of space; env {key} was not built, so tasks using it are not sent there; it retries after space is freed"
            if sub == "env" and key:
                return f"Cannot build env {key} on {site}; tasks using it are not sent there, check the env build log"
            if sub == "env":
                return f"Building envs on {site} failed"
            if sub == "worker" and key:
                return f"The {key.upper()} worker on {site} failed and stopped; no one runs {key.upper()} tasks there, check the log and restart it"
            if sub == "worker":
                return f"A worker on {site} failed and stopped; its GPUs will idle, check the log and restart it"
            if sub == "fail" and not key:
                return f"{site} failed too many jobs in the last 3 hours; claiming new tasks is paused"
            if sub == "fail" and "progress" in d:
                if d["progress"]:
                    return f"{key_name(key)} failed on {site} with progress; decide whether to resume or drop it"
                return f"No training started for {key_name(key)} on {site}"
            if sub == "orphan" and "unknown" in d:
                if d["unknown"]:
                    return f"There is an unrecognized task in the {site} queue"
                return f"{site} is running {key_name(key)}, but the hub queue has not assigned it there; it may be training twice"
        if kind == "relayadd":
            what = dict(relay_stage="stage data", relay_fetch="fetch", relay_archive="archive", env_build="build env").get(d.get("step"), "next step")
            return f"Could not queue the {what} for {key_name(d.get('key', ''))} on {L(M.hub())}; {L(part[1])} will wait, the next check retries"
        if kind == "bundlegate":
            return f"The bundle in use on {L(part[1])} cannot run the task spec for {key_name(d.get('key', ''))}; not sending it there until a new bundle arrives"
        if kind == "smoke":
            k = (d.get("kind") or "").split("|")
            what = " ".join([k[0]] + [x.split("=", 1)[1] for x in k[1:] if "=" in x and not x.startswith("env=")])
            why = (d.get("why") or "").splitlines()[0][:120] if d.get("why") else "see taskq smoke status"
            return f"The {what} smoke test on {L(part[1])} did not pass: {why}; such tasks are not sent there"
        if kind == "topup":
            return f"Submitting more jobs to {L(part[1])} failed; its queue will slowly drain"
        if kind == "advance":
            return f"{_who(':'.join(part[2:]))} {step_name(part[1])} failed and stopped; this result will be delayed"
    except Exception:  # noqa: BLE001
        pass
    return msg
