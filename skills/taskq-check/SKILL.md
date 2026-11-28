---
name: taskq-check
description: Check a taskq project across the hub and every executor, and handle what is wrong: a one-page plain-language report with expected finish times, then for a health alert or an entry in `taskq problems`, decide false alarm or real fault, recover by the project's rules and record it with `taskq note`. Use when the user asks 检查一下各台机器、一切正常吗、进展如何、现在训到哪了、什么时候训完、看看有没有问题, says ntfy 又报了、手机收到报警、这个报错是什么、怎么又失败了、problems 里那条是什么意思, pastes alert text, or asks about a failed or stuck task.
---

# Check a taskq project and handle alerts

Goal: answer "how is it going, does anything need me, when will it finish" within minutes, and when something is wrong, separate false alarms from real faults and fix real ones so work resumes by itself. The check itself is read only; only the handling section changes anything. Alerts come from the hub's scheduled health check, `taskq tick`. The core's `queue/README.md` has the full "Where to look when something is wrong" table.

## Project notes

The project root is `$TASKQ_PROJECT`, or the nearest directory above the current one holding `.taskq.toml`. On the hub, read-only commands (`status`, `problems`, `events`, `show`, `log`, `progress`, `wait`) run outside any project or in another checkout of the same repo read the repo the health check runs, and say so on stderr: no `cd` is needed (`--here` reads this checkout instead). If `<project root>/.claude/taskq/check.md` exists, read it first and follow it: it adds the project's own checks, report format and rules on what to fix alone and what to ask about, and wins over this file on project matters.

## Check (send what can run in parallel together)

1. In the project repo: `git pull --rebase --autostash`, then `git log --oneline -15` to see what other sessions just changed (another session may already be on a fault).
2. Everything at once: `taskq tick -n`. It uses the same checks as the scheduled health round, prints each problem (`[level/kind] id + text`) and what this round would do, pushes nothing and changes no file. `taskq plan` shows only the "what it would do" part.
3. Details, only as needed:
   - All tasks with state, machine, progress and expected finish: `taskq status --all`. The hub queue: `taskq status`.
   - One executor's queue, disk, bundle and jobs: `taskq site status <machine>`. Use a name from `taskq machines names` or the label shown in event text.
   - pueue tasks, including those finished in the last 24 hours: `taskq pueue-status`.
   - What the scheduler did recently: `taskq events --since <time>`, filtered with `--source relay|feed|advance|health|notify|note`.
   - Problems and how long they have lasted: `taskq problems --all` (`--json` for every field), `taskq events --source health --since 1d`. Problems with the same cause (such as one disk short for many tasks) are one line in the push and one item in `taskq status`, "+N more with the same cause"; `taskq problems --json` gives each one's `cause` and `members`.
   - One task: `taskq why <key>` first (state, claim, marks, failures and retry times, why the last round sent it nowhere, switches, intents, events, a suggested next command), then `taskq show <key>`, `taskq log <key>`, `taskq progress --key <key>` as needed.
   - Switches in force (queue off, drained workers or machines, pauses, with reason and end time): `taskq switch list`; who changed them: `taskq events --source switch`.

## When something is wrong

1. Is it still there: `taskq tick -n`. One that has gone was usually transient (a fetch just finished, a job just changed part); explain why and stop.
2. Follow the problem id (`<kind>:<machine or key>...`, kinds in `proto.ALERTS`):
   - `site:<machine>:...`, an executor (claiming, transfers, job submission, workers not starting, fetches, disk cleanup): `taskq site status <machine>`, `taskq plan`, and `taskq events --source relay --site <machine>`. `--site` and `site` commands take a machine name or label. Relay events recorded before `site` was filled in have it empty: for those use `--grep <machine label>`.
   - `taskq:<hub>`, `tqoff:<hub>`, `tqblocked:` (the hub queue): `taskq status --all`, `taskq show <key>`, `taskq switch list`.
   - `advance:` or a stalled step after a task finished: `taskq events --source advance`, `taskq log <key>`, `taskq plan`, then `pueue log <id>` for that step on its machine.
   - A failed or stuck task: `taskq why <key>`, `taskq show <key>`, the part log, and the executor's failure record shown by `taskq site status <machine>`. A failure without progress goes back to the hub queue with a skip for that executor that expires after a wait (1 h, 3 h, 9 h; after the fourth it stays until `taskq retry`); a failure with progress is cleared and resumed on the same executor after a wait, three times; a failed pueue worker is restarted the same way. Alerts come only once those retries are used up, and name the failure class (oom, missing, killed, error).
   - The same task running in two places is urgent: confirm both logs are really advancing, then stop the one that started later.
   - `intent:<id>`: a `taskq move`, `now`, `after` or `rm` that failed or expired; `taskq intents --all` says why. Operations still under way are in `taskq intents` (and in `taskq tick -n`).
3. Judge "finished" and "stuck" by the actual files on disk (count them, read modification times), not only by marker files, manifests or log lines. While a copy or fetch is running, nothing is finished.
4. Fix without asking, unless the project notes say otherwise. To send a task elsewhere: `taskq move <key> --to <machine>` (stops it, resumes it once released, sends it only there; `--restart` when it is half-done elsewhere); never stop, release and edit by hand. Otherwise: remove the cause, then `taskq retry <key>` so the task runs again at once (it clears the skips, failures and failure tail on the hub and on the executor holding it; `taskq retry worker <machine>/<worker>` for a pueue worker whose restarts are used up), when the cause is fixed or had nothing to do with the task. Failures retry by themselves after a wait (`taskq show <key>` says when), so a single failure with an obvious transient cause needs nothing. Stop a hung process; `pueue remove` a stalled step so the next check queues it again.
5. Ask first, each time, naming the object: cancelling a scheduler job, submitting extra jobs by hand, any deletion that loses data, turning off a queue's master switch (an executor's `ENABLED`, `taskq queue off`). To take a machine or one worker out for a while, use `taskq drain <machine>[/<worker>] --for <length> --why <reason>` (`--soft` lets running tasks finish), and `taskq pause` to stop contacting a machine; both end by themselves, never edit config or kill workers for this. `taskq rm` of a task in use stops it and removes it once released by itself; `taskq cancel --exp <id> [--only k=v] [--hold] -n` shows what removing a whole experiment would do.
6. Add no new watchdog, patrol or switch file (temporary on/off is `taskq switch`, `drain`, `pause`, `queue`). If the root cause is in code, fix the code with a test, and ship it with `taskq-ship`.

## Report

- First sentence is the conclusion: all fine, or how many things need attention; for an alert, false alarm or real fault, fixed or not.
- One or two lines per machine: what is running, how the GPUs are used, expected finish (to the hour, in the user's time zone).
- For a fault, one or two sentences on cause and impact: was progress lost, was any data affected.
- Then what needs handling: what is already recovering by itself, what a session should do, what the user must decide. Omit this section when empty.
- Plain language, no raw output, no invented abbreviations. Do not promise to keep watching and do not start polling. When a specific result is needed (a task running again after a fix), one background `taskq wait <key> --until running|done|failed|settled --timeout <time>` waits and ends by itself; its exit code says how it ended.
- A check writes no log. Record what you fixed with `taskq note --kind fix`, or as the project notes say.
