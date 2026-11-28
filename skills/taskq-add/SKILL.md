---
name: taskq-add
description: Add new work to a taskq project's hub queue end to end: entry in the experiment table, the project's own preparation, `taskq check`, a real smoke run, commit and push, `taskq add`, a sanity check of the first task once it runs, and a report with GPU hours and queue position. Use when the user says 加一个实验、把 X 排上、新开一组训练、这个也训一下、换种子再训, or "queue this experiment", "add these runs".
---

# Add work to the queue

Goal: new work in the hub queue that runs and continues through its later steps without anyone watching.

## Project notes

The project root is `$TASKQ_PROJECT`, or the nearest directory above the current one holding `.taskq.toml`. If `<project root>/.claude/taskq/add.md` exists, read it first and follow it: it says where the experiment table is, what each entry needs, how inputs and environments are prepared, which project tests to run and where the decision is recorded. It wins over this file on project matters.

## Steps

1. `git pull --rebase --autostash` in the project repo; read recent commits for changes to the experiment table, machine table and policy.
2. Add the entry to the experiment table (`OUTPUTS["experiments"]` in the project config; fields in the core's `queue/schema.py`). Reuse an existing entry's shape; keep ids stable, they are the task keys.
3. Prepare what the entry needs, per the project notes (inputs on the hub, env spec, step scripts).
4. Check: `taskq check` (table, recipes, plugins, policy), the project's own tests from the notes, and `taskq add --exp <id> -n` to see the tasks it would add.
5. Smoke: run the project's smoke check for the new kind of task when it is new (a new script, env or input format), so a mistake shows in minutes, not after hours in the queue.
6. Commit and push (pull first); the hub reads the pushed config at its next round.
7. On the hub: `taskq add --exp <id>`; set priority with `taskq nice` if the project rules say so (later changes to the entry's `nice` follow into the queue by themselves). `add` refuses a spec that would only fail on an executor (a `{name}` left unfilled, a repo file the bundle leaves out or does not have, with the reason): fix the entry or commit and push the file, then add again. To jump the line: `taskq now <key> [--on <machine>]`; to queue something once a task finishes: `taskq after <key> done -- <command>`.
8. Where it stands: `taskq status --all` for its position and the machines that can take it; estimate GPU hours from the project's measured speeds and the number of tasks.
9. When the first task starts (later, not by waiting in the session; one background `taskq wait <key> --until running --timeout <time>` can tell when): `taskq progress --key <key>` shows progress moving and the log shows no errors. If the user is still there, check once; otherwise leave it to the health check.
10. Record the decision where the project notes say.

## Report

- First sentence: what was queued (how many tasks), where it sits in the queue and when it should start and finish.
- GPU hours in total, and which later steps the scheduler will run by itself.
- Anything the user must decide, each with a recommendation. Do not promise to keep watching.
