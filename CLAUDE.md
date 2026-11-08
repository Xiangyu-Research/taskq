# Notes for Claude Code: changing the taskq core

taskq is a general task scheduler (the hub queue lives on the hub; executors join through the machine table). A project uses it only
through a config dir (`machines.py`, `policy.py`), a plugins dir and the command line; structure, commands and conventions are in
`README.md` and `queue/README.md`.

## Sync and release

- Run `git pull --rebase` before changing anything; there is only one version, `main`, and pushing to it is going live (the hub's cron
  runs `taskq self update` next round). Never force-push; resolve conflicts before committing.
- When the user's repo must change at the same time, push taskq first and the user's repo after (the core must arrive first when the
  user needs something new from it).
- If you changed files executors use, the bundle is rebuilt automatically at the next health check and running workers switch when they
  go idle; batch such changes (comments included) into one push.
- Never edit a running shell script in place (Write / Edit and `sed -i` both count): write a temp file and `mv` it over.
- Before releasing a new scheduler script, run it once in the cron environment: `env -i HOME=$HOME PATH=/usr/bin:/bin bin/taskq --project <user repo> plan`
  (the dry-run form), not only the unit tests.

## Tests

- `bin/taskq self test` (everything, several files at once; `-l` lists them, `taskq self test <pattern>` runs only related ones). Run the
  full suite once per change; when it fails, find the cause and then rerun that one file.
- Core tests use only the fixed config and small experiment table in `queue/testdata/`; they never read the user's project config,
  experiment table or plugins. Checks about a real project belong in that project's own tests.
- Every external command a test replaces must also run for real at least once: project scripts the scheduler calls (such as a step's
  `inputs_cmd`) run for real against each experiment in the test experiment table, with state "just finished, files still on another machine".
- Tests kill only PIDs they recorded themselves (`$!`, `pgrep` limited to the test dir); never use broad matches such as `pkill -f`:
  real tasks run the same commands.

## Changing formats (on-disk layout, queue, marker files, JSON fields, bundles)

- When a format changes, put the rewrite of old state into `taskq self migrate` (`queue/migrate.py`) and do it in the same cutover: stop
  cron, hand back running tasks, push the code, switch every executor to the new bundle, run migrate, start cron. Code keeps no branch
  reading the old format and no old entry point "to delete after the transition".
- Changes that go live without a cutover do not change:
  - the arguments and exit codes of `taskq worker next/beat/release`; the tests in `self test` guarding this interface (worker calls,
    stops, bundles) must pass;
  - the versioned `taskq status --json` and `spec collect --json` the hub reads from executors (one line starting with `@@JSON`), fields add-only;
  - the launcher path inside a bundle (queued Slurm jobs find it at the same path under `code/current` when they start).

## Style

- **The protocol lives only in `queue/proto.py`**: on-disk names (root layout, queue dirs, marker files), time constants, task states,
  exit codes, field tables, alert formats and `TASKQ_*` env vars are defined only there; the rest of the core writes no literals
  (`test_protocol.py` checks); to change a value see "Changing formats" above.
- **The core is general**: no specific project, model, training code, dataset, machine name, hostname or path (`test_core_clean.py` checks);
  project-specific things go through config and plugins (`cfg.KINDS`), and plugins import only `taskq_api`. When an expression is missing,
  add a general mechanism to the core (place, condition, plugin kind) with tests, not a special path per step or machine.
- **Do not classify by model**: how many tasks share a GPU, when to refill the GPU pad, whether training can start all follow live state
  (GPU utilisation, memory, failure records); do not add "training code -> kind / threshold" tables.
- **One path per job**: sending tasks is one step inside the health check; reading Slurm / pueue state, submitting jobs and starting workers
  go only through adapters in `sites/`; "how many to start" is computed once by the caller, adapters do not repeat that rule; a leading
  `~` in a remote command is left for the other side to expand.
- **"Done" means the outputs are there**: count actual files on disk, not just marker files or manifests; when copying, write marker files
  (`MANIFEST.json`, top-level files) last; while a copy, fetch or transfer is running, it is not done and the source is not cleaned up.
  Follow these three rules when adding a new result path, and add a "copied halfway" test.
- A command's `--json` output is one JSON line, its field table in `proto.py`, with a `version`; only frozen commands read back over ssh
  start with `@@JSON `.
- Config fields are defined only in `queue/schema.py` (type, default, description), and `check` reports unknown fields against it; the
  README's field tables are generated from it, and the command table matches `bin/taskq` (`test_readme.py` checks); README describes only
  current general mechanisms, never history or dates, and project-specific notes stay in the project.
