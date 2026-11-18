---
name: taskq-ship
description: Safely release changes to the taskq core or to a project's scheduler side (machine table, policy, plugins, experiment table, cron and monitor scripts): run the matching tests, avoid running scripts, run once in the cron environment, push, then verify every machine has the new code. Use after such changes, or when the user says 上线、部署、推上去、确保和在跑的作业兼容.
---

# Ship scheduler changes

Light testing: unit tests plus at most one real run, then release. No repeated benchmarking. The core's rules are in its `CLAUDE.md`; read that before changing the core.

## Project notes

The project root is `$TASKQ_PROJECT`, or the nearest directory above the current one holding `.taskq.toml`. Outside any project, the hub's crontab names it (`crontab -l | grep -- --project`). If `<project root>/.claude/taskq/ship.md` exists, read it first and follow it: it lists the project's tests, how its code reaches the machines, and how it records changes.

## Two sides, released separately

- **The core** is its own repo with one version, `main`. Pushing to `main` is going live: the hub's cron picks it up at its next `taskq self update`. Develop in a separate checkout; the installed copy (`taskq path core`) is the one cron updates, so never edit it.
- **The project** releases through its own repo, as its notes describe.
- When both change: push the core first, then the project, so the core arrives before anything needs it.

## Steps

1. **Before changing**: pull both repos. For a shell script, check whether it is running (`pgrep -af <script>`, on each machine involved). Never edit a running script in place (Write, Edit, `sed -i` and writing from Python all count): write a temp file and `mv` it over.
2. **Test**: for any core change, `bin/taskq self test` from the checkout (`-l` lists the tests, `taskq self test <pattern>` runs related ones). Run the full suite once per change; when it fails, find the cause and rerun that one file. If the change touches what plugins or project scripts rely on, also run the project's plugin tests and `taskq check`. Kill only PIDs you recorded, never with broad matches such as `pkill -f`.
3. **New scheduler scripts**: run once in the cron environment, in the dry-run form where there is one: `env -i HOME=$HOME PATH=/usr/bin:/bin <checkout>/bin/taskq --project <project> plan`.
4. **Docs with code**: keep the README's command tables and conventions, and the project's rules, in step with the change.
5. **Commit and push**: say what changed and why, `git pull --rebase`, `git push`. Never force-push. Batch changes to files executors use (comments included) into one push, because each one rebuilds the bundle.
6. **Verify it took effect**:
   - Core: after the next health check `taskq self version` is the commit you pushed. If it is urgent, run `taskq self update`.
   - Executors have only bundles. Check the code line of `taskq site status <machine>`; the bundle is rebuilt at the next check when files executors use changed, and running workers switch when idle. If it is urgent, `taskq site bundle <machine>`.
   - Queued Slurm jobs read the new bundle when they start. If a worker interface changed, confirm old and new jobs both run.
   - On-disk names, constants, exit codes or fields changed: follow "Changing formats" in the core's `CLAUDE.md`.
7. **Record**: the commit message carries the change. Record a scheduler incident's fix with `taskq note --kind fix "<cause, fix, commit>"`.

## Report

First sentence: what went live and whether running jobs are affected. Then one line each on which tests ran (all passed, or which failed and why), what was run for real, and when each machine gets the new code.
