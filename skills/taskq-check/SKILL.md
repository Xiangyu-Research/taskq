---
name: taskq-check
description: Read-only health and progress check of a taskq project across the hub and every executor, reported as one page of plain language with expected finish times. Use when the user asks 检查一下各台机器、一切正常吗、进展如何、现在训到哪了、什么时候训完、看看有没有问题, or "is everything OK", "how far along are the jobs". Changes nothing; hand real faults to taskq-triage.
---

# Check a taskq project

Goal: answer "how is it going, does anything need me, when will it finish" within minutes. Read only: add nothing, stop nothing, delete nothing. If you find a fault, continue with `taskq-triage`.

## Project notes

The project root is `$TASKQ_PROJECT`, or the nearest directory above the current one holding `.taskq.toml`. Outside any project, the hub's agent clock names it (`grep -h -- --project ~/.config/taskq/agent-*.clock.json`). If `<project root>/.claude/taskq/check.md` exists, read it first and follow it: it adds the project's own checks and report format, and wins over this file on project matters.

## Steps (send what can run in parallel together)

1. In the project repo: `git pull --rebase --autostash`, then `git log --oneline -15` to see what other sessions just changed.
2. Everything at once: `taskq tick -n`. It uses the same checks as the scheduled health round, prints each problem (`[level/kind] id + text`) and what this round would do, pushes nothing and changes no file. `taskq plan` shows only the "what it would do" part.
3. Details, only as needed:
   - All tasks with state, machine, progress and expected finish: `taskq status --all`. The hub queue: `taskq status`.
   - One executor's queue, disk, bundle and jobs: `taskq site status <machine>`. Use a name from `taskq machines names` or the label shown in event text.
   - pueue tasks, including those finished in the last 24 hours: `taskq pueue-status`.
   - What the scheduler did recently: `taskq events --since <time>`, filtered with `--source relay|feed|advance|health|notify|note`.
   - Problems not yet due for a push: `taskq problems --all`.
   - One task: `taskq show <key>`, `taskq log <key>`, `taskq progress --key <key>`.

## Report

- First sentence is the conclusion: all fine, or how many things need attention.
- One or two lines per machine: what is running, how the GPUs are used, expected finish (to the hour, in the user's time zone).
- Then what needs handling: what is already recovering by itself, what a session should do, what the user must decide. Omit this section when empty.
- Plain language, no raw output, no invented abbreviations. Do not promise to keep watching and do not start polling.
- A check writes no log. If you did fix something, record it with `taskq note --kind fix`.
