# taskq

A general task scheduler: a hub queue on one machine (the hub), executors (workstations with resident pueue workers, Slurm clusters)
joined through a machine table that claim tasks from the queue, train, and release them. Data has one home (the store): inputs are
sent from the store to executors, results are fetched back to the store and archived to a backup machine when needed, and executor
disk is only a staging area. Downstream steps (inference, evaluation, summaries, ...) are queued automatically from the experiment table.

On the hub, cron runs a health check every 30 minutes (`taskq tick`): check every machine, push problems, queue downstream steps,
send tasks to executors, top up workers, and fetch results. The scheduler knows no specific model or training code: how a kind of task runs,
which env and data it needs, and how it counts as done all live in the project's recipes (plugins) and experiment table; how many
tasks share a GPU and when to yield memory follows live state (GPU utilisation, memory, failure records).

A project uses it through only three things: a config dir (`machines.py`, `policy.py`), a plugins dir, and the command line.
Mechanisms, config fields and how to write plugins are in [`queue/README.md`](queue/README.md).

## Install

```bash
bash install.sh      # in a clone of taskq; to curl install.sh without cloning first, see the top of install.sh
```

It clones to `${TASKQ_HOME:-~/.local/share/taskq}` and links `~/.local/bin/taskq` to its `bin/taskq`; `TASKQ_ORIGIN` changes the clone
source. Repeating is fine: an existing install is updated (`taskq self update`, pulls `origin/main`).

Needs: Python 3 (the core uses only the standard library), ssh; [pueue](https://github.com/Nukesor/pueue) on pueue machines, the hub
and the store (`queue/install_pueue.sh` installs it); conda on executors (the machine table's `conda`).
`taskq site setup [machine]` creates each machine's pueue daemon, groups and the hub's crontab.

## Project (`.taskq.toml`)

Put a `.taskq.toml` in the project root naming the config and plugins dirs (relative to the project root):

```toml
config = "config"        # the dir holding machines.py and policy.py
plugins = ["plugins"]    # plugins dir (recipes.py, relay_plugins.py, consumers.py, smoke.py, labels.py, readers/)
```

Project lookup order: `taskq --project <dir>` (passed to children through `TASKQ_PROJECT`) > search upward from the current dir for
`.taskq.toml`. The config dir can also be set with `TASKQ_CONFIG`.

| file | what it holds |
|---|---|
| `<config>/machines.py` | machine table `MACHINES`: one entry per physical machine, roles (hub, store, backup), executor roots and scheduling (pueue workers, Slurm jobs) |
| `<config>/policy.py` | how to use them: automatic switches `AUTO`, pauses `PAUSE`, holds `HOLD`, allowed destinations `PLACE`, how many to keep ready `READY`, how many Slurm jobs `SLURM`, project outputs `OUTPUTS` |
| `<config>/specs/<env>.yml` | envs built on executors from a description (`sites/envs.py`) |
| `<plugins>/recipes.py` | recipes: how a kind of task runs, which env and data it needs, and how it counts as done |
| the file `OUTPUTS["experiments"]` points to | experiment table `EXPERIMENTS`, `FINAL`: what each experiment trains and which steps run after it |

Field tables are in [`queue/README.md`](queue/README.md)'s "config", or `taskq machines fields`. After changing config run `taskq check`.

## Commands

`taskq --help` lists everything; `taskq <command> --help` (or `taskq help <command>`) shows one command.
Read-only commands only read; commands that change the queue run on the hub. Commands with `--json` print one JSON line (fields in `queue/proto.py`).

<!-- commands -->

**Queue**

| command | what it does |
|---|---|
| `taskq add` | add a task: experiment table (--exp), recipe (--recipe) or task file (--file) |
| `taskq set` | change a task's params |
| `taskq nice` | change nice (order) |
| `taskq show` | one task's full spec and state |
| `taskq log` | one task's training parts (part logs), event log rows about it, hub task records |
| `taskq status` | on the hub: a one-screen summary (--all: every task by state; <machine>: one machine; <words>: matching tasks) |
| `taskq stop` | stop a task (running one stops and releases within 30 seconds; not claimed again afterwards) |
| `taskq resume` | undo a stop |
| `taskq rm` | remove an unclaimed task |

**Scheduling**

| command | what it does |
|---|---|
| `taskq tick` | one health check (really does it: push, queue downstream steps, send tasks) |
| `taskq plan` | dry run: what this round would queue downstream and which tasks it would send |
| `taskq boot` | boot (@reboot): start pueued, put interrupted tasks back |
| `taskq problems` | problems found by the last health check ([--all] [--json]) |
| `taskq notice set` | post or edit text (<name> [text...]; reads stdin without text) |
| `taskq notice clear` | remove (<name>...) |
| `taskq notice list` | posted notices ([--json]) |
| `taskq notice ack` | acknowledge "things that happened" reported by the health check (such as a reboot; <problem id or prefix>...), not listed again |
| `taskq events` | event log: what happened (--since --key --site --source --kind --grep, -f, --json) |
| `taskq note` | record a line in the event log (what happened, what was done) |
| `taskq progress` | task progress over time ([--key K] [--since T] [--json]) |
| `taskq notify` | push a message to a phone (ntfy) and record it in the event log |

**Executors and experiments**

| command | what it does |
|---|---|
| `taskq site status` | that machine's queue |
| `taskq site bundle` | build, push and switch current |
| `taskq site resend` | replace the specs of sent tasks there with the hub's current recipe and params |
| `taskq site env` | build envs on that machine and run checks |
| `taskq site envs` | dry run: each env's state on that machine |
| `taskq site setup` | pueue daemon, groups, crontab (no name: this machine) |
| `taskq env lock` | resolve an env description into a lock <name>.<variant>.lock.json and download the files into the wheelhouse (not installed anywhere) |
| `taskq smoke status` | each machine's records ([executor] [--json]) |
| `taskq smoke retry` | queue failed kinds again (<executor> [kind]) |
| `taskq smoke adopt` | mark kinds of assigned tasks that finished and were released as passed ([--dry-run]) |
| `taskq exp list` | list experiments |
| `taskq exp tasks` | expanded tasks (--json) |
| `taskq exp env-of` | model dir -> the env used to train it |
| `taskq exp name-of` | model dir -> the entries of its experiment's names (one per line) |
| `taskq exp steps` | steps with commands filled from names (--done checks done files, --needs lists inputs; JSON) |

**Dependencies**

| command | what it does |
|---|---|
| `taskq who` | which instance produces this file |
| `taskq dag` | order between instances (--dot draws it, --check verifies) |

**Other**

| command | what it does |
|---|---|
| `taskq tunnel rsync` | rsync stand-in: transfer through the tunnel first, then run rsync unchanged |
| `taskq tunnel push` | this machine -> the other side |
| `taskq tunnel pull` | the other side -> this machine |
| `taskq tunnel push-dirs` | this machine -> the other side, whole subdirs, verified afterwards |
| `taskq gpu usage` | who used each GPU by hour, and each use by someone else ([dir] [--me account] [--days N] [--json]) |
| `taskq pad room` | let the pad grow to floor and wait for it to say ready ([--wait seconds]; does nothing without a pad) |
| `taskq pueue-add` | add a one-off task to this machine's pueue manual group (manual_groups; working dir is this machine's repo) |
| `taskq pueue-status` | each pueue machine's queue (read-only) |
| `taskq check` | check the whole project (config, plugins, experiment table, all tasks) |
| `taskq db migrate` | create the database or run pending migrations (hub only) |
| `taskq db path` | where it is |
| `taskq db version` | on-disk and code schema version |
| `taskq path` | core locations: lib / api (queue/), core (root) |
| `taskq machines` | machine table (get, names, activate, show, ...) |
| `taskq self update` | pull origin/main (5 retries; stays on the old version when it cannot) |
| `taskq self version` | version |
| `taskq self test` | run every core test (own temp HOME, several at once, result table) |
| `taskq self migrate` | rewrite old on-disk formats to current ones (run once at a cutover; lists by default, --apply changes) |

**Low-level commands (workers, programs on executors, the scheduler itself; arguments and exit codes do not change)**

| command | what it does |
|---|---|
| `taskq worker next` | claim one task |
| `taskq worker beat` | heartbeat |
| `taskq worker release` | release |
| `taskq worker missing` | missing inputs, release |
| `taskq spec shell` | shell statements for run_task.sh (expanded for this machine) |
| `taskq spec done` | done exits 0, not done 1, bad spec 2, data dir unreadable 5 |
| `taskq spec resolve` | resolved spec (one JSON line) |
| `taskq spec lookup` | find a task by key (the queue first, then the experiment table) and print its spec (JSON) |
| `taskq spec outputs` | this task's outputs (JSON); --sh: shell statements for archive_task.sh |
| `taskq spec collect` | training state on this machine (for cross-version reads): one @@JSON line |
| `taskq spec export` | what a relay plugin sends: files, views, fetched dirs (JSON) |
| `taskq spec import` | relay plugin fetch check, written under --dst (JSON) |
| `taskq internal stage` | transfer data and add to the executor queue (exit 75 when it cannot fit this round) |
| `taskq internal fetch` | fetch finished tasks (into the store) |
| `taskq internal archive` | archive results from the store to the backup machine |

<!-- /commands -->

## Tests

```bash
taskq self test              # everything: test_*.py, test_*.sh under queue/, sites/, sync/, monitor/, several at once (-j N, default 4)
taskq self test -l           # only list what would run
taskq self test relay spec   # only names or paths containing these (relay -> test_relay*.py; * and ? work)
taskq self test --gpu        # also run tests that need a real GPU (files starting with "# needs-gpu")
```

Each file runs at the core root with its own temp HOME, an environment holding only `PATH`, `LANG`, `HOME` and `TMPDIR`, and the fixed
config in `queue/testdata/`; it does not read the user's project. One result line per file (result, seconds, count of "PASS:" lines,
skip reason) is written to `~/.cache/taskq-test/report.tsv`, and each file's output to `log_<file>.txt` in the same dir. Tests missing
numpy, pueue etc. exit 77 and count as skipped. Exit codes: 0 all pass (or skip), 1 some failed, 2 no matching tests.

Which tests to run after changing which file (run the full suite once at the end of a change; shared files such as `queue/proto.py`
and `bin/taskq` mean the full suite):

<!-- tests -->
| changed | run |
|---|---|
| `bin/taskq` | `test_cli.py`, `test_cli_json.py`, `test_readme.py` |
| `README.md`, `queue/README.md`, `queue/schema.py` | `test_readme.py`, `test_check.py` |
| `queue/proto.py` | `test_protocol.py`, `test_core_clean.py`, then the full suite |
| any core file (no specific project, machine or path) | `test_core_clean.py` |
| `queue/cfg.py`, `queue/project.py`, `sites/mtab.py` | `test_cfg.py`, `test_common.py` |
| `queue/common.py` | `test_common.py` |
| `queue/check.py` | `test_check.py`, `test_example.py` |
| `example/` | `test_example.py` |
| `queue/taskq.py` | `test_taskq.py`, `test_races.py`, `test_worker.sh` |
| `queue/consume.py`, `queue/consumers/`, `queue/worker_start.sh` | `test_consume.sh`, `test_worker.sh`, `test_worker_code.sh`, `test_worker_stall.sh`, `test_worker_stop.sh` |
| `queue/run_task.sh`, `queue/inputs.py` | `test_run_task.sh`, `test_inputs.py` |
| `queue/gpu_pad.py`, `queue/gpu_pad_start.sh` | `test_gpu_pad.py`, `test_pad_room.py`, `test_gpu_pad_launcher.sh`; with a GPU also `test_gpu_pad_integration.sh` (`--gpu`) |
| `queue/spec.py`, `queue/recipes.py` | `test_spec.py` |
| `queue/experiments.py` | `test_experiments_cli.py`, `test_example.py` |
| `queue/flow/` | `test_flow_compile.py`, `test_advance.py` |
| `queue/advance.py` | `test_advance.py`, `test_smoke.py` |
| `queue/feed.py` | `test_feed.py` |
| `queue/refill.py` | `test_refill.py`, `test_relay.py` |
| `queue/relay.py`, `queue/relay_remote.py`, `queue/relay_transfer.py` | `test_relay.py`, `test_races.py`, `test_bundle_paths.py` |
| `queue/relay_store.py`, `queue/site_ops.py` | `test_relay_store.py`, `test_relay.py` |
| `queue/relay_bundle.py` | `test_relay_bundle.py`, `test_bundle_paths.py` |
| `queue/bundle_gate.py` | `test_bundle_gate.py` |
| `queue/relay_plugins.py`, `sites/names.py` | `test_relay_plugins.py` |
| `queue/smoke_gate.py` | `test_smoke.py` |
| `queue/archive_task.sh` | `test_archive_task.sh` |
| `queue/db.py`, `queue/events.py`, `queue/tasklog.py` | `test_db.py`, `test_tasklog.py` |
| `queue/cli_json.py` | `test_cli_json.py` |
| `queue/migrate.py` | `test_migrate.py` |
| `sites/pueue.py`, `sites/slurm.py`, `sites/remote.py`, `queue/setup.sh`, `queue/worker.sbatch`, `queue/cpu_worker.sbatch` | `test_adapters.py`, `test_bundle_paths.py`, `test_e2e.py` (the Slurm chain) |
| `sites/envs.py`, `sites/envs_cli.py` | `test_envs.py` |
| `sites/envlock.py` | `test_envlock.py` |
| `sync/tunnel.py`, `sync/tunnel_push.sh` | `test_tunnel.py` |
| `monitor/health.py`, `monitor/notice.py`, `monitor/plain.py`, `monitor/push.py` | `test_health.py`, `test_probe.py`, `test_db.py` |
| `monitor/overview.py` | `test_overview.py` |
| `monitor/summary.py` | `test_summary.py` |
| `monitor/gpu_usage.py`, `monitor/gpu_log.sh` | `test_gpu_usage.py` |
| anything crossing modules (send, transfer, finish, fetch in one round) | `test_e2e.py` (pueue, separate store and fake Slurm layouts) |
<!-- /tests -->

## Example project

[`example/`](example/) is a minimal project: one hub (this machine, also the store), one pueue executor, one Slurm executor (fake hostname,
unreachable), one recipe, two experiments, and one step queued after each task finishes. Read-only commands need no other machine:

```bash
taskq --project example check
taskq --project example exp tasks
taskq --project example plan
```

The fixed test config [`queue/testdata/`](queue/testdata/) shows every field in use.

## Conventions

- One version only: pushing to `main` is going live; the hub's cron runs `taskq self update` next round, executor bundles are rebuilt and
  pushed next round, and running workers switch bundles when they go idle.
- On-disk names, time constants, task states, exit codes, field tables, alert formats and `TASKQ_*` env vars are defined only in
  `queue/proto.py`; when a format changes, the rewrite of old state goes into `taskq self migrate` and happens in one cutover (see
  [`queue/README.md`](queue/README.md)'s "command line and conventions").
- Commands with `--json` print one JSON line with a `version`; fields are add-only. Commands the hub reads back over ssh
  (`taskq status --json`, `taskq spec collect --json`) put that line behind `@@JSON `.
- Changes that go live without a cutover do not change the arguments and exit codes of `taskq worker next/beat/release` (running workers
  and queued Slurm jobs use them).

Exit codes (`proto.EXIT`):

| command | exit codes |
|---|---|
| `taskq` itself | 0 fine; 1 error; 2 usage error, no project found, not on the hub and no local queue; 5 data dir unreadable (not created, disk not mounted) |
| `taskq show`, `set`, `stop`, `nice`, `add --exp` | 4 not in the queue (or experiment table) (several keys: 4 when any is missing) |
| `taskq rm` | 0 removed; 3 not removed (claimed, assigned, another task waits for it); 4 none of them in the queue |
| `taskq status` | 1 queue unreadable |
| `taskq worker next` | 0 claimed; 10 nothing to claim; 11 wait first (leave room for queued, recently stopped tasks); 12 paused (3 failures in 3 hours across the queue, or data dir unreadable) |
| `taskq worker beat --strict` | 13 some claims are no longer ours |
| `taskq worker missing` | 1 not claimed by us |
| `queue/run_task.sh` | 0 done; 1 training failed; 2 bad spec or env; 3 command exited normally but training did not finish; 4 missing inputs (did not start); 5 disk unreadable, cannot tell whether training finished; 143 got SIGTERM |
| `taskq spec done` | 0 done; 1 not done; 2 bad spec; 5 data dir unreadable |
| `taskq internal stage` | 75 cannot fit this round, transfer next health check (not reported) |
| external commands the scheduler runs | 124 timeout or failed to start |
| `taskq check` | 0 no problems; 1 problems |
