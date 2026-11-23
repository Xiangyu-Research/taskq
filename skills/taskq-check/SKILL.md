---
name: taskq-check
description: Check a taskq project across the hub and every executor, and handle what is wrong: a one-page plain-language report with expected finish times, then for a health alert or an entry in `taskq problems`, decide false alarm or real fault, recover by the project's rules and record it with `taskq note`. Use when the user asks 检查一下各台机器、一切正常吗、进展如何、现在训到哪了、什么时候训完、看看有没有问题, says ntfy 又报了、手机收到报警、这个报错是什么、怎么又失败了、problems 里那条是什么意思, pastes alert text, or asks about a failed or stuck task.
---

# Check a taskq project and handle alerts

Goal: answer "how is it going, does anything need me, when will it finish" within minutes, and when something is wrong, separate false alarms from real faults and fix real ones so work resumes by itself. The check itself is read only; only the handling section changes anything. Alerts come from the hub's scheduled health check, `taskq tick`. The core's `queue/README.md` has the full "Where to look when something is wrong" table.

## Project notes

The project root is `$TASKQ_PROJECT`, or the nearest directory above the current one holding `.taskq.toml`. Outside any project, the hub's agent clock names it (`grep -h -- --project ~/.config/taskq/agent-*.clock.json`). If `<project root>/.claude/taskq/check.md` exists, read it first and follow it: it adds the project's own checks, report format and rules on what to fix alone and what to ask about, and wins over this file on project matters.

## Check (send what can run in parallel together)

1. In the project repo: `git pull --rebase --autostash`, then `git log --oneline -15` to see what other sessions just changed (another session may already be on a fault).
2. Everything at once: `taskq tick -n`. It uses the same checks as the scheduled health round, prints each problem (`[level/kind] id + text`) and what this round would do, pushes nothing and changes no file. `taskq plan` shows only the "what it would do" part.
3. Details, only as needed:
   - All tasks with state, machine, progress and expected finish: `taskq status --all`. The hub queue: `taskq status`.
   - One executor's queue, disk, bundle and jobs: `taskq site status <machine>`. Use a name from `taskq machines names` or the label shown in event text.
   - pueue tasks, including those finished in the last 24 hours: `taskq pueue-status`.
   - What the scheduler did recently: `taskq events --since <time>`, filtered with `--source relay|feed|advance|health|notify|note`.
   - Problems and how long they have lasted: `taskq problems --all` (`--json` for every field), `taskq events --source health --since 1d`.
   - One task: `taskq show <key>`, `taskq log <key>`, `taskq progress --key <key>`.

## When something is wrong

1. Is it still there: `taskq tick -n`. One that has gone was usually transient (a fetch just finished, a job just changed part); explain why and stop.
2. Follow the problem id (`<kind>:<machine or key>...`, kinds in `proto.ALERTS`):
   - `site:<machine>:...`, an executor (claiming, transfers, job submission, workers not starting, fetches, disk cleanup): `taskq site status <machine>`, `taskq plan`, and `taskq events --source relay --site <machine>`. `--site` and `site` commands take a machine name or label. Relay events recorded before `site` was filled in have it empty: for those use `--grep <machine label>`.
   - `taskq:<hub>`, `tqoff:<hub>`, `tqblocked:` (the hub queue): `taskq status --all`, `taskq show <key>`.
   - `advance:` or a stalled step after a task finished: `taskq events --source advance`, `taskq log <key>`, `taskq plan`, then `pueue log <id>` for that step on its machine.
   - A failed or stuck task: `taskq status`, `taskq show <key>`, the part log, and the executor's failure record shown by `taskq site status <machine>`. A failure without progress goes back to the hub queue and is recorded in `skips/<key>`, so that executor is not given the task again.
   - The same task running in two places is urgent: confirm both logs are really advancing, then stop the one that started later.
3. Judge "finished" and "stuck" by the actual files on disk (count them, read modification times), not only by marker files, manifests or log lines. While a copy or fetch is running, nothing is finished.
4. Fix without asking, unless the project notes say otherwise: remove the cause, then delete `failures/<key>` in the executor's queue so the task runs again (`skips/<key>` in the hub queue if it was handed back), when the cause is fixed or had nothing to do with the task; stop a hung process; `pueue remove` a stalled step so the next check queues it again.
5. Ask first, each time, naming the object: cancelling a scheduler job, submitting extra jobs by hand, any deletion that loses data, removing a queue's `ENABLED` master switch. Before `taskq rm` of a task sent to an executor, `taskq stop` it and wait for the release.
6. Add no new watchdog, patrol or switch file. If the root cause is in code, fix the code with a test, and ship it with `taskq-ship`.

## Report

- First sentence is the conclusion: all fine, or how many things need attention; for an alert, false alarm or real fault, fixed or not.
- One or two lines per machine: what is running, how the GPUs are used, expected finish (to the hour, in the user's time zone).
- For a fault, one or two sentences on cause and impact: was progress lost, was any data affected.
- Then what needs handling: what is already recovering by itself, what a session should do, what the user must decide. Omit this section when empty.
- Plain language, no raw output, no invented abbreviations. Do not promise to keep watching and do not start polling.
- A check writes no log. Record what you fixed with `taskq note --kind fix`, or as the project notes say.
