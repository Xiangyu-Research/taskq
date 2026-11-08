"""Config field tables: machine table (MACHINES in machines.py), each machine's workers, policy.py, experiment table (experiments and steps).
Each entry is (type, default, one-line description). taskq check verifies against it (unknown fields are errors), taskq machines fields
prints it, and a test checks the README config tables match it. Type names are in check.py's TYPES.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import proto  # noqa: E402

REQUIRED = object()  # writing it as the default = required

# ---------------- machine table: one entry per physical machine ----------------
MACHINE_FIELDS = {
    # connection
    "label": ("str", REQUIRED, "name shown in alerts and pages"),
    "hostname": ("regex", REQUIRED, "regex matching this machine's hostname (hostname -s)"),
    "ssh": ("str?", None, "ssh name the hub connects to; None = the machine the hub runs on"),
    "ssh_master": ("bool", False, "ssh only over a persistent connection (ControlMaster); do not connect while it is down"),
    "tz": ("str?", None, "this machine's time zone (IANA name, e.g. Asia/Kuala_Lumpur); worker hours use it; None = local time zone"),
    "conda": ("pair", None, "(conda location, default env name)"),
    "pueue": ("str", "~/.local/bin/pueue", "pueue location"),
    "disk": ("disk?", None, "(dir to check, alert below this many GB free, urgent below this many GB or None)"),
    "gpu_log": ("bool", False, "record GPU use at boot (taskq gpu usage)"),
    "tunnel": ("tunnel", (), "file transfer direction between this machine and the store: push (store -> here), pull (here -> store)"),
    "quick_tunnel": ("bool", False, "transfer files over a temporary trycloudflare address"),
    "transfer_job": ("str?", None, "slurm: srun args for large transfers on a compute node"),
    "env_job": ("str?", None, "slurm: srun args for building envs on a compute node; None = use transfer_job"),
    # roles
    "roles": ("roles", (), "hub (hub queue, health check), store (home of results, data dir in data), backup (archive backups go to backup)"),
    "repo": ("str?", None, "hub: the project clone cron uses"),
    "data": ("str?", None, "store: data dir (home of results)"),
    "backup": ("str?", None, "backup: where archive backups go (data dir layout)"),
    "transfer": ("pair?", None, "store: file service (public address, listen address:port)"),
    "quick": ("pair?", None, "store: temporary tunnel (cloudflared image, regex matching temporary addresses)"),
    # execution
    "root": ("str?", None, "root (blobs, data, code, queue, state, tmp); executors and the store need it"),
    "scheduler": ("sched?", None, "executor: pueue (resident worker) or slurm (submit worker jobs)"),
    "workers": ("workers", (), "pueue executor: this machine's workers, each see WORKER_FIELDS"),
    "jobs": ("jobs", {}, "slurm executor: {gpu|cpu: sbatch options dict(partition, gres, cpus, mem)}; QOS, time limits and count in policy.py's SLURM"),
    "gpu_pad": ("triple?", None, "slurm: (floor, target, high) MiB for the worker job's own GPU pad"),
    "job_names": ("str?", None, "slurm: job name style (STYLES in sites/slurm.py); None = pair"),
    "envs": ("envs", {}, "{env name: ('conda', name) | ('venv', dir, base env) | 'spec' | ('spec', variant)}"),
    "cap": ("num?", None, "most GB the root may use (home dirs with a quota)"),
    "blob_cap": ("num?", None, "most GB blobs may use when they point at another disk"),
    "budget": ("pair", (100, 50), "(start cleaning when the disk has less than this many GB free, keep at least this many GB free after a transfer)"),
    "task_vars": ("strmap", {}, "extra env vars for tasks run on this machine"),
    "task_unset": ("strs", (), "env vars to drop from tasks run on this machine"),
    "manual_groups": ("groups", {}, "manual pueue groups {group: (how many at once, GPU or None, {env vars})}; the core never queues into them"),
}

# ---------------- one worker of a pueue executor ----------------
WORKER_FIELDS = {
    "gpu": ("str?", None, "which GPU (CUDA_VISIBLE_DEVICES); None = a CPU-only worker"),
    "cpus": ("int?", None, "most cores to use; None = all"),
    "slots": ("int", 1, "how many run at once"),
    "hours": ("hours", (), "claim and run only in these hours: [(start HH:MM, end HH:MM, first day or None, last day or None)]; empty = all day"),
    "idle": ("int?", None, "minutes the GPU must be idle after someone else used it before starting"),
    "wait": ("int?", None, "most minutes to wait while the GPU is busy"),
}

# ---------------- policy.py ----------------
AUTO_SWITCHES = proto.AUTO_SWITCHES

POLICY_FIELDS = {
    "AUTO": ("auto", REQUIRED, "which automatic parts are on: " + ", ".join(AUTO_SWITCHES)),
    "PAUSE": ("pause", {}, "{machine: (until 'YYYY-MM-DD HH:MM' or None, reason)}: while paused no connections or sends; resumes automatically"),
    "HOLD": ("strs", (), "held steps (wildcard over step instance names, e.g. 'infer/*')"),
    "PLACE": ("place", {}, "{step or training recipe: [executor, ...] | 'any'}: allowed executors in preference order; absent = 'any' (any executor with such a worker)"),
    "READY": ("intmap", {}, "{machine: how many unclaimed tasks to keep ready}; absent = 1"),
    "SLURM": ("slurm", {}, "{machine: dict(gpu={QOS: (time limit per part, jobs left for others)}, cpu=(QOS, time limit, max jobs))}"),
    "OUTPUTS": ("outputs", REQUIRED, "the project's outputs and entry points, see OUTPUT_FIELDS"),
}

OUTPUT_FIELDS = {
    "state": ("str", "~/.local/state/taskq/", "state dir"),
    "entry": ("str?", None, "main loop entry (relative to the hub's repo)"),
    "cron": ("cron", (), "((time, subcommand), ...): taskq site setup writes it into the hub's crontab"),
    "origin": ("str?", None, "where the hub's repo is cloned from"),
    "bundle": ("strs", (), "extra dirs or files carried in the executor bundle"),
    "bundle_skip": ("strs", (), "left out of the bundle (fnmatch wildcards)"),
    "bundle_check": ("str?", None, "check that the bundle compiles, using this machine's env python"),
    "experiments": ("str?", None, "wildcard path of the experiment tables"),
    "code_dirs": ("strs", (), "dirs watched for '+changes' after the code version in logs"),
    "env_script": ("str?", None, "script sourced first by tasks added with taskq pueue-add"),
    "name": ("str", "taskq", "prefix of push titles"),
    "topic": ("str", "~/.config/taskq/ntfy_topic", "file holding the push channel name (the channel name does not go in the repo)"),
    "logs": ("str", "~", "dir for plain text logs (relay.log, advance.log, ...)"),
}


# ---------------- experiment table (each module in OUTPUTS["experiments"]: EXPERIMENTS, FINAL) ----------------
EXP_FIELDS = {
    "id": ("str", REQUIRED, "unique short name (instance id, taskq add --exp)"),
    "exp": ("str", "", "experiment number (ledger, board)"),
    "label": ("str", "", "one-line description"),
    "model": ("str", "", "model name (ledger grouping)"),
    "hours": ("num?", None, "hours each task counts as (ledger total progress)"),
    "recipe": ("str", REQUIRED, "training recipe (recipes.py); PLACE places training tasks by it"),
    "params": ("dict", {}, "recipe params"),
    "fields": ("dict", {}, "extra spec fields to override (e.g. metrics, vars, inputs)"),
    "each": ("dict", {}, "{param: [values...]}: expand into one task per combination; absent = one task"),
    "after": ("steps", (), "steps after each task is done (STEP_FIELDS), done in order"),
    "group_after": ("steps", (), "experiment-wide steps (STEP_FIELDS); order follows inputs and outputs"),
    "names": ("dict", {}, "extra names available in step templates (analysis scripts also read name-of)"),
    "nice": ("int", 0, "nice when taskq add --exp has no --nice"),
    "board": ("dict", {}, "board data (each table decides)"),
    "design": ("str", "", "neutral note on what was done (for the facts file)"),
}

STEP_FIELDS = {
    "name": ("str", REQUIRED, "step name (policy.py PLACE and HOLD use it)"),
    "cmd": ("str", REQUIRED, "command to run on the executor (template; working dir is the project in the bundle, {data} is the data dir there)"),
    "out": ("strs", REQUIRED, "outputs (relative to the data dir; several allowed, the first is the main dir). A path belongs to one step only"),
    "done": ("strs", REQUIRED, "done markers: files under the first output, every item required; 'a|b' needs one; may use {param}"),
    "inputs": ("strs", (), "inputs (relative to the data dir, wildcards allowed); an input inside another step's output runs after it, "
                           "across experiments too; in experiment-wide steps {param} (of each) expands over every value"),
    "inputs_cmd": ("str?", None, "command that prints inputs too (one path per line relative to the data dir; used by the relay plugin)"),
    "relay": ("str", "path", "which plugin prepares inputs and checks results when sending to an executor (sites/names.py)"),
    "env": ("strs", REQUIRED, "env name (one or several; may use {param} like cmd)"),
    "device": ("device", "gpu", "gpu (needs a GPU) or cpu (CPU only)"),
    "res": ("dict", {}, "resources (recipes.py res)"),
    "out_gb": ("num?", None, "roughly how many GB of output (leaves room when choosing a machine; None = proto.OUT_GB)"),
    "temp": ("strs", (), "temp files under the first output (wildcards): after every step using them is done and they are fetched, "
                         "the store deletes them and records it in the event log"),
    "rerun": ("bool", False, "redo when an input is newer than the output"),
    "nice": ("int", 0, "nice in the hub queue"),
    "only": ("dict?", None, "per-task steps: only for tasks where each takes these values ({param: [values...]})"),
}


# ---------------- a recipe (each RECIPES entry in recipes.py) and a task spec ----------------
# Fields of a recipe other than params and open; all may use {param}, plus {data} (the executor's data dir) and {repo} (the project
# in the bundle). A task param with the same name overrides the field (dicts merge, everything else is replaced). A recipe that
# omits a field takes the default here.
RECIPE_FIELDS = {
    "key": ("str", REQUIRED, "task name (queue, claims, ledger all use it; must be unique)"),
    "env": ("strs", REQUIRED, "env name (each machine's envs; several comma-separated or a list)"),
    "cmd": ("str", REQUIRED, "command run with bash in the project root in the bundle; a rerun continues from the last run (after release another worker continues from the checkpoint)"),
    "out": ("str", REQUIRED, "output dir (relative to the data dir): fetch, archive and ledger all look here"),
    "done": ("strs", REQUIRED, "done markers: files under out, every item required; 'a|b' needs one"),
    "vars": ("dict", {}, "env vars set before running"),
    "log": ("str?", "*.log", "logs under out (glob): how long since the last update judges whether someone is working"),
    "progress": ("str?", None, "progress reader (spec.py PROGRESS, readers/ plugins; progress_json = out/PROGRESS.json)"),
    "resume": ("strs?", None, "checkpoints under out (glob): any one means progress, kept there on release for the same machine to continue; None = none"),
    "res": ("dict", {}, "resources: gpus, cpus, mem, gpu_mem, wait_gpu (minutes), local_inputs (1 = copy to node-local disk after start)"),
    "owner": ("str?", None, "whose task (None = own)"),
    "info": ("str?", None, "training settings reader (ledger params column)"),
    "metrics": ("dict", {}, "ledger validation metrics: {column: (JSON file under out, key path)}"),
    "checkpoint": ("str?", None, "file under out whose checksum the ledger records"),
    "inputs": ("strs", [], "inputs that must exist on the executor (relative to the data dir, wildcards allowed): transferred before sending, checked again before start; an experiment step's inputs are these"),
    "inputs_cmd": ("str?", None, "command that prints inputs too (one path per line relative to the data dir), to list on the store what to transfer"),
    "fetch": ("dict", {"exclude": [], "extra": []}, "fetch to the store: exclude (not fetched), extra (next to out, fetched too, e.g. *.json), "
                                                    "also (other dirs to fetch, relative to the data dir)"),
    "archive": ("bool", False, "back up to the backup machine after fetching, and verify"),
    "relay": ("str", "path", "plugin used to list inputs and check results: path = inputs are those in inputs (sites/names.py); others = relay_plugins.py"),
    "machines": ("strs?", None, "only run on these executors (intersected with PLACE); None = any"),
    "device": ("device", "gpu", "gpu (needs a GPU) or cpu"),
    "step": ("str?", None, "experiment step name (steps added by advance have it; training is None): PLACE, HOLD and the board use it"),
    "out_gb": ("num?", None, "roughly how many GB of output (leaves room when choosing a machine); None = proto.OUT_GB"),
}


def defaults(table, entry):
    """Fill entry with the defaults of fields it omits (REQUIRED is not filled)."""
    return {**{k: v[1] for k, v in table.items() if v[1] is not REQUIRED}, **entry}


TABLES = ("MACHINE_FIELDS", "WORKER_FIELDS", "POLICY_FIELDS", "OUTPUT_FIELDS", "EXP_FIELDS", "STEP_FIELDS", "RECIPE_FIELDS")


def markdown(name):
    """A field table (a name in TABLES) -> Markdown table (field, type, default, description). The READMEs paste its output
    (taskq machines fields --markdown; test_readme.py checks it)."""
    def cell(s):
        return str(s).replace("|", "\\|").replace("\n", " ")

    rows = ["| field | type | default | description |", "|---|---|---|---|"]
    for k, (typ, dflt, doc) in globals()[name].items():
        d = "required" if dflt is REQUIRED else f"`{cell(repr(dflt))}`"
        rows.append(f"| `{k}` | `{cell(typ)}` | {d} | {cell(doc)} |")
    return "\n".join(rows)
