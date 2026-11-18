---
name: taskq-triage
description: Handle a taskq health alert or an entry in `taskq problems`: decide false alarm or real fault, recover by the project's rules, record it with `taskq note`. Use when the user says ntfy 又报了、手机收到报警、这个报错是什么、怎么又失败了、problems 里那条是什么意思, pastes alert text, or asks about a failed or stuck task.
---

# Handle a taskq alert

Alerts come from the hub's scheduled health check, `taskq tick`. Goal: separate false alarms from real faults, fix real ones so work resumes by itself, and tell the user the conclusion in a sentence. The core's `queue/README.md` has the full "Where to look when something is wrong" table.

## Project notes

The project root is `$TASKQ_PROJECT`, or the nearest directory above the current one holding `.taskq.toml`. Outside any project, the hub's crontab names it (`crontab -l | grep -- --project`). If `<project root>/.claude/taskq/triage.md` exists, read it first and follow it: it holds the project's rules on what to fix alone and what to ask about, and wins over this file.

## Order of investigation

1. `git pull --rebase --autostash` in the project repo and read recent commits: another session may already be on it.
2. The alert and how long it has lasted: `taskq problems --all` (`--json` for every field), `taskq events --source health --since 1d`.
3. Is it still there: `taskq tick -n`. One that has gone was usually transient (a fetch just finished, a job just changed part); explain why and stop.
4. Follow the problem id (`<kind>:<machine or key>...`, kinds in `proto.ALERTS`):
   - `site:<machine>:...`, an executor (claiming, transfers, job submission, workers not starting, fetches, disk cleanup): `taskq site status <machine>`, `taskq plan`, and `taskq events --source relay --site <machine>`. `--site` and `site` commands take a machine name or label. Relay events recorded before `site` was filled in have it empty: for those use `--grep <machine label>`.
   - `taskq:<hub>`, `tqoff:<hub>`, `tqblocked:` (the hub queue): `taskq status --all`, `taskq show <key>`.
   - `advance:` or a stalled step after a task finished: `taskq events --source advance`, `taskq log <key>`, `taskq plan`, then `pueue log <id>` for that step on its machine.
   - A failed or stuck task: `taskq status`, `taskq show <key>`, the part log, and the executor's failure record shown by `taskq site status <machine>`. A failure without progress goes back to the hub queue and is recorded in `skips/<key>`, so that executor is not given the task again.
   - The same task running in two places is urgent: confirm both logs are really advancing, then stop the one that started later.
5. Judge "finished" and "stuck" by the actual files on disk (count them, read modification times), not only by marker files, manifests or log lines. While a copy or fetch is running, nothing is finished.

## Handling

- Fix without asking, unless the project notes say otherwise: remove the cause, then delete `failures/<key>` in the executor's queue so the task runs again (`skips/<key>` in the hub queue if it was handed back), when the cause is fixed or had nothing to do with the task; stop a hung process; `pueue remove` a stalled step so the next check queues it again.
- Ask first, each time, naming the object: cancelling a scheduler job, submitting extra jobs by hand, any deletion that loses data, removing a queue's `ENABLED` master switch. Before `taskq rm` of a task sent to an executor, `taskq stop` it and wait for the release.
- Add no new watchdog, patrol or switch file. If the root cause is in code, fix the code with a test, and ship it with `taskq-ship`.

## Report and record

- First sentence: false alarm or real fault, fixed or not, anything the user must do.
- One or two sentences on cause and impact: was training progress lost, was any data affected.
- Record what you did with `taskq note --kind fix` or as the project notes say.
