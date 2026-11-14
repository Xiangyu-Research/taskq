#!/usr/bin/env python3
"""Scheduler protocol: names, directory layout, formats, constants, exit codes and fields shared across scripts and machines live only here.
When an on-disk format changes, add the rewrite of the old format to taskq self migrate in the same cutover; every executor switches bundles together.

Standard library only, no imports of other scheduler files (site_ops.py on executors and bash scripts read it via proto.py sh).
  python3 proto.py sh     assignments for bash (eval "$(python3 proto.py sh)"), names are TQ_<UPPER>
  python3 proto.py json   the whole protocol (JSON)
"""
import json
import sys

VERSION = 1  # included in cross-version command output (status --json, collect --json); fields are add-only, bump only when changed

# ---------------- executor root R (the root in the machine table machines.py; relay.py creates it, the bundle's worker uses it) ----------------
ROOT = dict(
    code="code",                 # bundle code/<bundle> (relay_bundle.py, the name is a content hash)
    current="code/current",      # bundle in use (symlink; resolved when a queued job starts)
    queue="queue",               # the executor's task queue (TASKQ_DIR)
    blobs="blobs",               # data files, stored by sha256
    data="data",                 # views of each task (TASKQ_DATA)
    state="state",               # relay bookkeeping
    pins="state/pins",           # blobs pinned by tasks being transferred or whose view is not ready (relay.py stage writes, relay_store.py gc keeps)
    envs_ok="state/envs",        # marker that an env is built <env>.ok (sites/envs.py)
    hashes="state/hashes.db",    # sha256 cache of data files on the store (by path, size, mtime; site_ops.hashes)
    view_lock="state/view.lock",  # lock for creating and removing views
    deploy_lock="state/deploy.lock",  # lock for pushing bundles (relay.py deploy)
    tmp="tmp",                   # temp files for half-transferred files
    envs="envs",                 # envs installed on the executor (conda, venv)
    machine="machine",           # this root's machine name, one line (relay.py deploy and self migrate write it): processes started without TASKQ_MACHINE read it (worker_start.sh)
)
# where the scheduler core sits inside a bundle (relative to the bundle root): queued Slurm jobs and worker commands find the launcher there
BUNDLE_CORE = "scripts/taskq"
# launcher path in bundles from before the core moved to BUNDLE_CORE: workers started from such a bundle switch to this path under
# code/current; every bundle has a generated forwarder there (relay_bundle.contents) until no such worker runs
OLD_LAUNCHER = "scripts/queue/worker_start.sh"
IO_GROUP = "taskq"  # the core's own group on pueue machines, the hub and the store (transfers, fetches, archives, unbounded concurrency), sites/mtab.groups creates it
WORKER_LABEL = "worker:"  # label prefix for worker tasks on pueue executors: worker:<id> (sites/pueue.py)
MKDIRS = ("code", "queue", "blobs", "tmp", "state", "data/logs/tasks", "data/logs/workers")  # created when relay.py first makes the root


def blob(h):
    """sha256 -> relative path under the root (one blob layout: executor blobs and node-local caches alike)."""
    return f"blobs/{h[:2]}/{h}"


BLOB_STAT = ".st"       # blobs/<h2>/<h>.st: "size mtime(ns)" of a blob when ingested and sha256-checked; mismatch (hardlink source changed in place) -> drop and ingest again
PIN_TTL = 12 * 3600     # a pinned blob (state/pins/<key>.json) no longer counts as pinned after this long without an update
TMP_KEEP = 24 * 3600    # temp files with a dot in blobs (half-transferred) are kept this long
TMP = ".tmp"            # suffix of half-written files (common.tmp_of: <name>.<pid>.tmp), renamed when done
OUT_GB = 10             # GB reserved for a task's outputs (archive, results) when choosing an executor and the spec has no out_gb (refill.choose)


# ---------------- queue dirs (the hub queue and executors use the same layout, taskq.py) ----------------
QUEUE = dict(
    tasks="tasks.jsonl",      # one task per line
    claims="claims",          # claims/<safe key>/info
    failures="failures",
    skips="skips",
    inputs="inputs",          # missing inputs
    released="released",      # last release: "<time> <worker> <why>" (see RELEASE_WHY)
    stopped="stopped",        # stopped/<safe key>: stopped tasks; running workers check every 5 seconds
    enabled="ENABLED",        # master switch: a queued worker exits at start when it is absent (worker_start.sh)
    lock="lock",
    gpu_pad_off="gpu_pad_DISABLED",  # executor's GPU pad master switch (cfg.state("gpu_pad/DISABLED"), STATE_IN_ROOT)
    held="tasks.held.jsonl",  # unclaimed tasks set aside while self migrate switches the bundle (nobody can claim them meanwhile)
)
CLAIM_INFO = "info"
# fields of claims/<key>/info: claimed by a worker (executor queue) / assigned by the hub queue to an executor
CLAIM_FIELDS = ("worker", "host", "since", "beat", "token", "log")  # log: job name in the part log (next --log-tag; same as worker when absent)
ASSIGN_FIELDS = ("worker", "host", "since", "beat", "assigned")

# ---------------- state of processes on an executor (TASKQ_ROOT) inside the root (cfg.state; the rest is under state/) ----------------
STATE_IN_ROOT = {"taskq": ROOT["queue"], "gpu_pad/DISABLED": ROOT["queue"] + "/" + QUEUE["gpu_pad_off"]}  # everything else -> state/

# ---------------- marker files ----------------
INCOMPLETE = ".incomplete"     # a dir half created (tunnel_push.sh, relay making a view); any level means missing
MANIFEST = "MANIFEST.json"     # fetched result: written last, only present when the fetch is complete (relay_transfer.py)
RELAY_INBOX = ".relay_in"      # half-fetched results in the store data dir .relay_in/<safe key> (site_ops.py pull)
STAGE_DIR_PREFIX = "taskq_stage_"  # input copy on node-local disk $TMPDIR/taskq_stage_<job> (inputs.py, run_task.sh, consumers/slurm.py)
GPU_PAD_BASE = "/tmp/gpu_pad_"  # GPU pad and worker handshake /tmp/gpu_pad_<job>{.mib,.ready,.start}
GPU_PAD_FILES = (".mib", ".ready", ".start")

# ---------------- logs ----------------
TASK_LOGS = "logs/tasks"       # under the data dir: part logs <safe key>__<job>_part<part>.log
WORKER_LOGS = "logs/workers"   # workers, GPU pad, Slurm's %x_%j.out
LOG_PART = "part"
DONE_LINE = "DONE"             # a part log line starting with it = done (run_task.sh writes, spec.py reads)
STALL_LINE = "STALLED"         # a part log line starting with it = stopped by the worker as stalled (consume.py writes and counts)

# ---------------- GPU usage records (monitor/gpu_log.sh writes; run_task.sh's TASK_GPU_IDLE and the health check read) ----------------
GPU_LOG_DIR = "~/gpu_usage"      # override with GPU_LOG_DIR
GPU_LOG_FILE = "gpu_%Y-%m.csv"   # one per month (strftime)
GPU_LOG_PID = "gpu_log.pid"      # in GPU_LOG_DIR: pid of the running gpu_log.sh, locked (flock) while it runs; on_boot.sh checks the lock

# ---------------- key -> filename ----------------
SAFE = "__"


def safe(key):
    """key -> filename (claim records, stops, part logs)."""
    return key.replace("/", SAFE)


def task_log(key, job, part=1, kind="part"):
    """Path of a part log relative to the data dir."""
    return f"{TASK_LOGS}/{safe(key)}{SAFE}{job}_{kind}{part}.log"


# ---------------- JSON read back over ssh ----------------
JSON_MARK = "@@JSON "  # only the line starting with it is read (spec.json_out / json_in)

# ---------------- time (seconds) ----------------
STALE_BEAT = 20 * 60   # a claim whose heartbeat has not been renewed for this long is stale
FRESH_LOG = 15 * 60    # a part log updated within this long counts as still running (not claimed)
HALF_CLAIM = 5 * 60    # a claim with only a dir and no info is stale after this long
BEAT = 5 * 60          # worker heartbeat interval
FAIL_RETRY = 60 * 60   # after a task fails or stalls and is released, the same worker waits this long before claiming it again
                       # (after MAX_FAIL failures the queue marks it failed and nobody claims it)
XFER_TRIES = 3         # a failed transfer (stage / fetch) is queued again at the next check; reported only after this many failures within one claim
# old bundle cleanup (relay.deploy cleans the root's code/, relay_bundle.prune cleans bundles built on the hub)
CODE_KEEP = 3                # keep the most recent bundles besides the one in use
SWITCH_HOLD_S = 90           # self migrate: after switching an executor's bundle, keep unclaimed tasks aside this long (a worker of the old bundle that checked the bundle just before can still claim once)
CODE_KEEP_S = 4 * 86400      # bundles under the root's code/ touched within this long are kept (running workers touch theirs every round)
CODE_CLAIM_SLACK = 3600      # also keep bundles touched since this long before the earliest claim among tasks still claimed: a task that keeps running after a switch still uses its claim's bundle


def claim_alive(info, made, now, log_age=None, stale=None, fresh=None, half=None):
    """Is a claim in an executor queue still alive (shared by taskq.py and the hub reading it through relay_remote.py)?
    info: contents of claims/<key>/info (None when unreadable); made: dir mtime; log_age: seconds since the part log last changed.
    stale, fresh, half: override STALE_BEAT, FRESH_LOG, HALF_CLAIM (taskq.py tests change them through env vars)."""
    if info is None:
        return now - made <= (HALF_CLAIM if half is None else half)
    if info.get("assigned"):
        return True
    if now - (info.get("beat") or 0) < (STALE_BEAT if stale is None else stale):
        return True
    return log_age is not None and log_age < (FRESH_LOG if fresh is None else fresh)


# ---------------- task states (taskq.py state(), local_status state) ----------------
STATES = dict(
    free="claimable",
    claimed="claimed (or assigned to an executor)",
    busy="unclaimed, but its training log changed within 15 minutes",
    stale="claim expired",
    failed="failed twice",
    input="missing inputs",
    stopped="stopped",
    bad="spec cannot be parsed",
    done="done",
)
S_FREE, S_CLAIMED, S_BUSY, S_STALE = "free", "claimed", "busy", "stale"
S_FAILED, S_INPUT, S_STOPPED, S_BAD, S_DONE = "failed", "input", "stopped", "bad", "done"
assert {S_FREE, S_CLAIMED, S_BUSY, S_STALE, S_FAILED, S_INPUT, S_STOPPED, S_BAD, S_DONE} == set(STATES)
UNCLAIMED = (S_FREE, S_STALE)  # unclaimed and claimable
# why the hub holds a task assigned to an executor (the relay state's held why; queue/bundle_gate.py)
HELD = dict(bundle="bundle-incompatible")  # the bundle in use there cannot run this spec

# ---------------- exit codes ----------------
EXIT = dict(
    # taskq.py
    next_ok=0, nothing=10, wait=11, paused=12, lost=13,  # next / beat --strict; 12 also means the data dir is unreadable
    refused=3, gone=4,                                   # remove
    no_queue=2,                                          # not on the hub and no local queue
    # run_task.sh
    run_done=0, run_failed=1, run_bad=2, run_not_done=3, run_missing=4, run_unreadable=5, run_term=143,
    # spec.py done
    done_yes=0, done_no=1, done_bad=2, done_unreadable=5,
    # relay.py stage
    stage_later=75,
    # relay_remote.op: the bundle in use on that machine has no such script or operation (older bundle)
    old_bundle=66,
    # common.run: timeout, failed to start
    timeout=124,
)

# ---------------- field tables ----------------
# taskq.local_status() per task (beyond a line of tasks.jsonl)
STATUS_FIELDS = ("spec", "state", "note", "worker", "host", "since", "assigned", "input_since", "beat", "log_age",
                 "task_log_age", "out_age", "failures", "skipped_by", "progress")  # progress: half-done on this machine (taskq.has_progress)
STATUS_TOP = ("enabled", "tasks", "recent_failures", "paused", "recent_skips")
TASK_FIELDS = ("order", "nice", "key", "exp", "recipe", "params", "spec", "added")  # a line of tasks.jsonl; added: when it entered this queue
# third item of released/<key>: why it was released (taskq.py records it from release's arguments; worker calls do not change)
RELEASE_WHY = dict(back="back",        # normal release: stopped, time limit, stalled, not training this round
                   failed="failed",    # release --failed
                   skip="skip",        # release --skip: this machine cannot run it
                   missing="missing")  # missing: missing inputs
COLLECT_FIELDS = ("key", "out", "done", "log", "progress", "info", "metrics", "resume")  # spec fields spec.collect wants
COLLECT_TOP = ("tasks", "seglogs", "stuck", "errors")  # result of spec.collect
COLLECT_TASK = ("runs", "final", "metrics", "debug", "resumable")  # per task
SEGLOG_FIELDS = ("name", "first", "trained")
LOG_HEAD_CODE = r"code (\S+?),"  # how the code commit appears in the first part log line (run_task.sh writes; regex)

# experiments.py list --json / tasks --json: one JSON line, callable objects by name, other objects as {"type": class name, attributes…}
EXP_LIST_TOP = ("version", "experiments", "final")      # experiments: experiment table entries (experiments.EXP_FIELDS); final: FINAL steps
EXP_TASKS_TOP = ("version", "tasks")
EXP_TASK_FIELDS = ("key", "exp", "recipe", "params", "each", "short", "label", "names", "spec")  # label: task name in the hub pueue label
# cli_json.py (taskq exp steps): each task's steps in flow/compile.py instances (task), experiment level (group), FINAL (final), command filled from names
EXP_STEPS_TOP = ("version", "data", "steps", "errors")  # errors: problems pushing the experiment table to instances (flow.errors)
# id: instance id; out: outputs (the first is the main dir); inputs: declared inputs (expanded); done: with --done, whether done files are present under the first output;
# needs: with --needs, inputs printed by inputs_cmd; null when not asked or absent
EXP_STEP_FIELDS = ("level", "exp", "key", "id", "name", "cmd", "names", "out", "inputs", "done", "needs", "error")

# cli_json.py (taskq machines show): values the core computes from the machine table
MSHOW_TOP = ("version", "hub", "machines", "specs")
MSHOW_MACHINE = ("name", "label", "scheduler", "roles", "executor", "paused", "kinds", "workers", "worker_cmds", "groups", "root",
                 "root_q", "ready_envs", "activate", "task_sh")
# workers: mtab.workers list; worker_cmds: {worker id: pueue add command to start it} (pueue executors); groups: {group: [slots, GPUs, env]};
# paused: current pause reason in PAUSE or null; ready_envs: envs usable without a built marker; task_sh: statements spec shell adds on this machine
MSHOW_SPEC = ("hash", "base", "torch", "error")  # torch: {variant: index_url}; error: why the description is unreadable, null when fine

# cli_json.py (taskq spec export / import): sent by relay plugins, checked on fetch
RELAY_EXPORT_TOP = ("version", "plugin", "params", "files", "texts", "views", "outs", "cleanup", "logs")  # logs: part log filename prefixes
RELAY_IMPORT_TOP = ("version", "written")

# overview.py --json: all state a summary like the ledger needs, one JSON line
OVERVIEW_TOP = ("version", "now", "hub", "data", "queue", "sources", "machines", "tasks", "health", "code")
OVERVIEW_QUEUE_TOP = ("enabled", "readable")               # hub queue enabled, readable
OVERVIEW_SOURCE = ("name", "label", "kind", "time")        # where training state was collected; kind: machine (gather) / relay (relay.py status.json)
OVERVIEW_MACHINE = ("name", "label", "scheduler", "ok", "why", "gpus", "pueue")  # gpus: [{gpu, uuid, util, mem, apps: [{pid, mem, args}]}]
OVERVIEW_PUEUE = ("groups", "tasks", "error")              # each task: qs --json fields plus start_ts, end_ts (seconds), about (plain.label: [experiment, what, short] or null)
OVERVIEW_TASK = ("key", "exp", "recipe", "params", "names", "owner", "out", "checkpoint", "queue", "copy", "total", "progress",
                 "eta", "parts", "commits")
OVERVIEW_TASK_QUEUE = ("rank", "state", "nice", "assigned", "note", "worker", "host")  # rank: position among unfinished tasks in the hub queue (1-based)
OVERVIEW_COPY = ("where", "source", "final", "epochs", "start", "last", "runs", "metrics", "debug", "resumable")
OVERVIEW_PART = ("name", "job", "part", "machine", "trained", "commit")  # part log: job, part number, machine, at least one round done, code commit
OVERVIEW_PROBLEM = ("id", "msg", "level", "kind", "first", "active", "notified", "plain")  # plain: push text in plain words (plain.problem)
SUMMARY_WINDOW = 24 * 3600  # taskq status (monitor/summary.py): "due in the next" / "last" window
SUMMARY_OVERDUE = 3600  # a running task this far past its estimated finish is listed under "Needs you"
TOTAL_INFO = "num_epochs"  # key in INFO for "total rounds" (used when runs have no total; overview total, progress, eta)

# ---------------- alerts (problem ids in health.py; plain.py explains them by kind) ----------------
# id = kind:<args>…, args joined with ":" in the order below; a problem with the same id is pushed once
ALERTS = dict(
    down=("machine",), unread=("machine",), readerr=("machine",), collect=("machine",), code=("machine",), taskq=("machine",),
    tqoff=("machine",), tqblocked=("key",), tqroom=("key",), pueued=("machine",), pueue=("machine", "id", "end"),
    disk=("machine",), gpulog=("machine",), boot=("machine", "time"),
    site=("machine", "sub", "key"), topup=("machine", "qos", "kind"), advance=("step", "who"),
    relaystage=("key",), relayfetch=("key",), relay=("machine",), feed=("sub", "machine"),
    relayadd=("machine", "op", "key"),
    bundlegate=("machine", "key"),
)


def alert_id(kind, *args):
    return ":".join([kind] + [str(a) for a in args if a not in (None, "")])


# a problem's value: message text (str), or dict(msg=text, data={…}, after=minutes before reporting) (plain.py reads data, never parses text)
def alert(msg, after=0, **data):
    v = dict(msg=msg, data=data) if data else {"msg": msg}
    if after:
        v["after"] = after
    return v if len(v) > 1 else msg


def alert_msg(v):
    return v["msg"] if isinstance(v, dict) else v


def alert_data(v):
    return (v.get("data") or {}) if isinstance(v, dict) else {}


# ---------------- env vars (TASKQ_*): who sets them, what they mean; kind = run (set in production) / tune (threshold, default in production) / test (tests only) ----------------
ENV = dict(
    TASKQ_DATA=("run", "this process's data dir (run_task.sh, worker_start.sh set it from cfg.data_dir for the task's command); when unset cfg.data_dir derives it from TASKQ_ROOT or the store's data"),
    TASKQ_ROOT=("run", "process on an executor: this machine's root (set by pueue worker commands, worker.sbatch, relay_remote.prefix); queue, state and data dir derive from it (cfg.state, cfg.data_dir)"),
    TASKQ_DIR=("test", "queue dir (tests only); when unset cfg.state('taskq')"),
    TASKQ_CONFIG=("run", "config dir (machines.py, policy.py); when unset the user repo's .taskq.toml config, or <repo>/config without .taskq.toml (cfg.py)"),
    TASKQ_PLUGINS=("run", "plugins dir; when unset the user repo's .taskq.toml plugins (cfg.py)"),
    TASKQ_STATE=("run", "root of the state dir; when unset the one from config (cfg.py)"),
    TASKQ_REPO=("run", "the user's repo root; when unset: <repo> if the core is embedded in <repo>/scripts/taskq, otherwise project.py finds the project, else the core itself"),
    TASKQ_PROJECT=("run", "the user's project root (with .taskq.toml); when unset searches upward from the current dir (project.py)"),
    TASKQ_HOME=("run", "where install.sh clones taskq (default ~/.local/share/taskq)"),
    TASKQ_ORIGIN=("run", "where install.sh clones from (default the GitHub repo)"),
    TASKQ_CPUS=("run", "max cores for a worker outside Slurm (pueue worker commands set it from machines.py's cpus)"),
    TASKQ_GPU_PAD=("run", "0 = no GPU pad for this job (set by inference and analysis scripts)"),
    TASKQ_GPU_PAD_STARTED=("run", "GPU pad already started (set by gpu_pad_start.sh to avoid starting twice)"),
    TASKQ_LIB=("run", "the scheduler core's queue/ dir (run_task.sh sets it for the task's command and puts the core's bin/ first on PATH)"),
    TASKQ_ENVS=("run", "envs a worker may claim (comma separated); when unset machines.py's envs"),
    TASKQ_PROG=("run", "command name bin/taskq passes to bash scripts (such as taskq boot), used in usage and errors; scripts unset it after reading"),
    TASKQ_TASK_DONE=("run", "set to its label when a task in the hub core group (IO_GROUP: transfer, fetch, archive, env build) finishes, to kick a health check (refill.then_check)"),
    TASKQ_SLOTS=("tune", "max concurrent training tasks per worker; in Slurm by cores and memory (2 or 1), outside Slurm from machines.py slots"),
    TASKQ_MACHINE=("run", "which machine in the machine table this is (set by pueue worker commands, worker.sbatch, relay_remote.prefix); when unset the unique hostname match"),
    TASKQ_HEAD_CMD=("test", "replaces the command that reads the code version"),
    TASKQ_GPU_PAD_SCRIPT=("test", "replaces gpu_pad.py"),
    TASKQ_GPU_PAD_BASE=("test", "replaces /tmp/gpu_pad_<job>"),
    TASKQ_TEST_FAKEBIN=("test", "dir holding fake commands"),
    TASKQ_LAST=("test", "replaces the state dir's last"),
    TASKQ_TASKS=("test", "replaces the state dir's tasks.json (task records)"),
    TASKQ_LOCK_WAIT=("test", "retry interval while waiting for a lock (seconds, default 0.5)"),
    TASKQ_STALE_BEAT=("test", "replaces STALE_BEAT"),
    TASKQ_FRESH_LOG=("test", "replaces FRESH_LOG"),
)
# consumer (consume.py) thresholds (tune), defaults in consume.py's Worker.__init__
for _k in ("ADD_AFTER", "UTIL_MAX", "MIN_LEFT", "PATIENCE", "ERR_MAX", "IDLE_MAX", "IDLE_POLL", "IDLE_EXIT", "EARLY_FAIL",
           "FAIL_WINDOW", "FAIL_RETRY", "STALL_MAX", "GIVEUP", "BEAT", "POLL", "STOP_POLL", "STOP_WAIT", "TERM_WAIT"):
    ENV["TASKQ_" + _k] = ("tune", "consume.py threshold, default in Worker.__init__")
# consumer bundle switch: the old process writes its tasks as JSON into TASKQ_HANDOFF and execs the new bundle's worker_start.sh, which takes over
HANDOFF = 1                # handoff format version; a new process accepts only the same version, otherwise the claim expires and someone else can take it
HANDOFF_TOP = ("v", "t0", "changed", "excl", "later", "fail_at", "tasks", "idle_since")  # idle_since: when the worker went idle,
# so IDLE_EXIT keeps counting across a bundle switch; missing → counted from the switch
HANDOFF_TASK = ("pid", "key", "token", "started", "outdir", "safe", "jtag", "stalled", "why", "sid", "zomb", "ended")
ENV["TASKQ_HANDOFF"] = ("run", "tasks handed by the old consumer to the new one on a bundle switch (JSON, fields in HANDOFF_TOP / HANDOFF_TASK); worker_start.sh skips the master switch when set")
ENV["TASKQ_ADMITTED"] = ("run", "1 = the consumer checked the GPU before claiming (gpu_idle plugin); run_task.sh no longer waits for it")
ENV["TASKQ_STAGE_ROOT"] = ("run", "node-local staging root (set by the consumer's slurm plugin); run_task.sh copies inputs there only when set")
ENV["TASKQ_DB"] = ("run", "hub database file (db.py); default <state dir>/taskq.db")
# local executor (a machine in machines.py with ssh=None that is also the hub): the hub process's own variables are not passed to it (as when
# sshing elsewhere), otherwise its worker and taskq.py would use the hub's config, data dir and queue. relay_remote.site_argv and sites/pueue.py worker commands strip them
HUB_ONLY_ENV = ("TASKQ_CONFIG", "TASKQ_PLUGINS", "TASKQ_REPO", "TASKQ_PROJECT", "TASKQ_DATA", "TASKQ_DIR", "TASKQ_STATE",
                "TASKQ_LIB", "TASKQ_ENVS", "TASKQ_MACHINE", "TASKQ_TASKS", "TASKQ_LAST")
ENV["TASKQ_TEST_NOW"] = ("test", "a file holding a time (epoch seconds): the window plugin treats it as now")
ENV["TASKQ_SWITCH_HOLD"] = ("test", "seconds self migrate keeps unclaimed tasks aside after switching a bundle (default SWITCH_HOLD_S)")
# gpu_pad.py arguments (tune), defaults on its command line
for _k in ("FLOOR_MIB", "TARGET_MIB", "HIGH_MIB", "CHUNK_MIB", "MIN_FREE_MIB", "MIN_TRAIN_MIB", "MIN_TOTAL_MIB", "INTERVAL",
           "SETTLE", "START_HOLD", "RETRY_DELAY", "MAX_FAILS", "RESET_AFTER"):
    ENV["TASKQ_GPU_PAD_" + _k] = ("tune", "gpu_pad.py argument, default on its command line")


# project notices (taskq notice, monitor/notice.py): in the hub database; each reports one problem notice:<name>
NOTICE_NAME = r"[A-Za-z0-9_-]+"
NOTICE_TOP = ("version", "notices")  # taskq notice list --json
NOTICE_FIELDS = ("name", "text", "since", "at")  # since: when it was last posted (%Y-%m-%d %H:%M:%S)
ALERTS["notice"] = ("name",)
AUTO_SWITCHES = {  # policy.AUTO switches
    "advance": "add downstream steps of finished training to the hub queue from the experiment table",
    "envs": "build and switch executors' spec envs from lock files (True, or {machine: [env or (env, variant)]} for extra prebuilt ones, sites/envs.py)",
    "revive": "health check restores pueued when it is gone",
    "smoke": "run a smoke task the first time an executor trains a kind, and after an env switch",
}
PUEUE_SEEN = "pueue_seen.json"  # in the health check's state dir: {pueue machine: {t: last time pueued was seen, running: [groups running then]}} (for revive)

# ---------------- smoke (smoke_gate.py) ----------------
SMOKE_PREFIX = "_smoke/"   # smoke task key: _smoke/<executor>/<kind id>-<time>-<random> ({smoke} = <executor>-<kind id>-<time>-<random>)
SMOKE_DIR = "smoke"        # in the hub queue: smoke/<executor>.json, whether each kind passed on that machine
SMOKE = ("ok", "failed", "queued")  # recorded state
SMOKE_SRC = ("smoke", "real", "adopted")  # recorded src: smoke task, real task done, taskq smoke adopt
SMOKE_NICE = -1000         # smoke task nice (runs first)
ALERTS["smoke"] = ("machine", "kind")  # smoke failed (kind = kind id)


def is_smoke(key):
    return str(key or "").startswith(SMOKE_PREFIX)
ENV_BUILD = "env_build"    # step name of a task that builds an env for an executor (task record, pueue label env_build:<machine>:<env>): queued in the hub core group (IO_GROUP), env built on that executor

# ---------------- env build states (sites/envs.py classify writes them; relay.py and monitor/plain.py compare; they reach status --json) ----------------
ENV_STATE = dict(
    ok="ok",
    failed="check failed",
    nospace="no space",
    building="building",
    stopped="building (may have stopped)",
    changed="spec changed",
    missing="not built",
    not_spec="not spec-built",
)

# ---------------- env locks (sites/envlock.py) ----------------
WHEELHOUSE = "wheelhouse"      # under the state dir: each locked file wheelhouse/<first 2 of sha256>/<sha256>
LOCK_SUFFIX = ".lock.json"     # lock next to an env description <name>.<variant>.lock.json
LOCK_FORMAT = 1
LOCK_TOP = ("format", "name", "variant", "spec_sha256", "platform", "base", "conda", "pip", "tools", "created")
LOCK_CONDA = ("name", "version", "build", "channel", "subdir", "url", "fn", "size", "md5", "sha256")
LOCK_PIP = ("name", "version", "url", "fn", "size", "sha256", "no_deps")  # pure-Python wheel built locally: url is null, built_from is the source archive URL
LOCK_UNHASHED = ("created",)   # keys removed when hashing (the description's hash, base's lock_sha256)


def wheel(h):
    """sha256 -> relative path under the state dir."""
    return f"{WHEELHOUSE}/{h[:2]}/{h}"


# ---------------- one task's history (taskq log, queue/tasklog.py) ----------------
# first line of each part in a part log (run_task.sh writes): job_part @ host, task=key, part N, env E, code C, start time
LOG_HEAD_FMT = "job %s @ %s, task=%s, part %s, env %s, code %s, %s"  # printf format: job, host, key, part, env, code, start
LOG_HEAD = r"job (\S+) @ ([^,]+), task=([^,]*), part (\d+), env ([^,]*), code ([^,]*), (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
# lines after the header (tasklog.py reads):
LOG_ENDED_FMT = "command ended (exit %s), %s"  # command, exit code, rest
LOG_ENDED_RE = r"command ended \(exit (-?\d+)\), (.*)"
LOG_NOT_STARTED_FMT = "not started: %s"
LOG_NOT_STARTED_RE = r"not started: (.*)"
LOG_GPU_BUSY_FMT = "GPU %s busy%s, not running"
LOG_GPU_BUSY_RE = r"GPU \S+ busy.*not running"
LOG_UNREADABLE = "disk unreadable"           # prefix of the ENDED rest when the data dir cannot be read
# full part-log line regexes: each writer format above plus the timestamp run_task.sh puts in front; readers use these
LOG_TS = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
LOG_LINE_TS = r"^" + LOG_TS + r"(?:\.\d+)?:? ?(.*)$"
LOG_ENDED = r"^" + LOG_TS + r" " + LOG_ENDED_RE + r"$"
LOG_NOT_STARTED = r"^" + LOG_TS + r" " + LOG_NOT_STARTED_RE + r"$"
LOG_GPU_BUSY = r"^" + LOG_TS + r" " + LOG_GPU_BUSY_RE + r"$"
LOG_STALLED = r"^" + STALL_LINE + r" " + LOG_TS + r":? ?(.*)$"
TASKLOG_TOP = ("version", "key", "data", "attempts", "events", "records")
TASKLOG_ATTEMPT = ("file", "path", "job", "machine", "host", "part", "env", "code", "start", "end", "last", "state", "exit", "note")
TASKLOG_EVENT = ("log", "time", "text")  # records: this key's entry in the hub's task records (tasks.json), as is
TASKLOG_STATES = dict(
    done="done",
    not_done="command exited normally but training did not finish",
    failed="command failed (exit is the exit code)",
    unreadable="disk unreadable, cannot tell whether training finished",
    not_started="did not start (note is the reason)",
    gpu_busy="GPU was busy, gave up waiting",
    stalled="stopped by the worker as stalled",
    running="log updated within FRESH_LOG, still running",
    ended="no end record (stopped, released or the job ended)",
)

# failure record line failures/<key> (taskq.py writes and reads): time, job, reason, bundle that ran it
FAIL_LINE_FMT = "%s\tjob %s\t%s\t%s"

# ---------------- GPU usage summary (taskq gpu usage, monitor/gpu_usage.py) ----------------
GPU_USAGE_TOP = ("version", "dir", "me", "days", "from", "to", "hours", "spans", "totals")  # hours: {weekday|weekend: [GPU_USAGE_HOUR]}
GPU_USAGE_HOUR = ("gpu", "hour", "n", "other", "me", "idle")  # n: records at this hour; other, me, idle: how many had someone else, only me, idle
GPU_USAGE_SPAN = ("gpu", "user", "start", "end", "hours", "max_mem_mib", "programs")  # each use by someone else
GPU_USAGE_TOTAL = ("gpu", "user", "hours")


# ---------------- hub database and event log (queue/db.py, queue/events.py) ----------------
DB = "taskq.db"  # under the state dir
STAGE_LOCK = "stage_{}.lock"  # under the hub's relay state dir, per executor: one blob transfer (missing → ingest → push → view) at a time
# names at the top level of the state dir (cfg.state): taskq self migrate deletes those not listed (left by older versions)
STATE_NAMES = ("taskq", "relay", "health", "last", "tasks.json", "tunnel", "tunnel_srv", "tunnel_push.lock", "gpu_pad",
               WHEELHOUSE, "envlock", DB)
EVENT_FIELDS = ("id", "time", "t", "source", "kind", "level", "key", "site", "msg", "data")  # taskq events --json, one per line
# source -> plain text log name (cfg.log; "health" is health.log): events from these sources also write a line there for tailing
TEXT_LOGS = {"relay": "relay", "feed": "feed", "advance": "advance", "health": "health"}
PROGRESS_TOP = ("version", "rows")  # taskq progress --json
PROGRESS_FIELDS = ("time", "t", "key", "state", "progress", "site", "data")  # a row whenever state, progress or machine changes
PROBLEMS_TOP = ("version", "checked", "problems")  # taskq problems --json; each problem's fields in OVERVIEW_PROBLEM


# ---------------- for bash ----------------
def sh_vars():
    out = {"TQ_SAFE": SAFE, "TQ_STAGE_DIR_PREFIX": STAGE_DIR_PREFIX, "TQ_GPU_PAD_BASE": GPU_PAD_BASE, "TQ_TASK_LOGS": TASK_LOGS,
           "TQ_WORKER_LOGS": WORKER_LOGS, "TQ_STALL_LINE": STALL_LINE, "TQ_DONE_LINE": DONE_LINE, "TQ_JSON_MARK": JSON_MARK,
           "TQ_INCOMPLETE": INCOMPLETE, "TQ_STALE_BEAT": STALE_BEAT, "TQ_FRESH_LOG": FRESH_LOG, "TQ_BEAT": BEAT,
           "TQ_GPU_LOG_DIR": GPU_LOG_DIR, "TQ_GPU_LOG_FILE": GPU_LOG_FILE, "TQ_GPU_LOG_PID": GPU_LOG_PID,
           "TQ_BUNDLE_CORE": BUNDLE_CORE,
           "TQ_LOG_HEAD_FMT": LOG_HEAD_FMT, "TQ_LOG_ENDED_FMT": LOG_ENDED_FMT, "TQ_LOG_NOT_STARTED_FMT": LOG_NOT_STARTED_FMT,
           "TQ_LOG_GPU_BUSY_FMT": LOG_GPU_BUSY_FMT, "TQ_LOG_UNREADABLE": LOG_UNREADABLE}
    out.update({f"TQ_GPU_PAD_{x[1:].upper()}": x for x in GPU_PAD_FILES})  # TQ_GPU_PAD_MIB, _READY, _START
    out["TQ_LOG_PART"] = LOG_PART
    out.update({f"TQ_ROOT_{k.upper()}": v for k, v in ROOT.items()})
    out.update({f"TQ_Q_{k.upper()}": v for k, v in QUEUE.items()})
    out.update({f"TQ_EXIT_{k.upper()}": v for k, v in EXIT.items()})
    return out


def everything():
    g = globals()
    return {k: g[k] for k in sorted(g) if k.isupper() and not k.startswith("_")}


def main(argv):
    import shlex
    if argv[:1] == ["sh"]:
        print("\n".join(f"{k}={shlex.quote(str(v))}" for k, v in sh_vars().items()))
        return 0
    if argv[:1] == ["json"]:
        print(json.dumps(everything(), ensure_ascii=False, indent=1))
        return 0
    print(__doc__.strip(), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
