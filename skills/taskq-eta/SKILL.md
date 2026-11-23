---
name: taskq-eta
description: Predict when a project's Slurm executors get their next GPU, with `taskq site eta` (one snapshot of the whole partition, local simulation of Slurm's scheduling), and improve the prediction when it misses. Use when the user asks 下一张卡什么时候拿到、什么时候能拿到卡、还要排多久、排队要等多久、集群什么时候有卡, or "when will my job start", "how long until a GPU".
---

# When the next GPU comes

`taskq site eta` reads, through one ssh call per cluster, every user's jobs in the GPU partition, their priority parts (`sprio`), the partition, the per-user QOS limits and the age and backfill settings, then simulates the queue forward. Each run keeps the raw snapshot, the predictions and the starts and ends it saw in the state dir, and compares the predictions made 1-48 hours before each start with the actual start. A periodic job (`taskq site eta --record`) builds that history.

## Project notes

The project root is `$TASKQ_PROJECT`, or the nearest directory above the current one holding `.taskq.toml`. If `<project root>/.claude/taskq/eta.md` exists, read it first: it holds the cluster's own facts and the improvement record of the method, and wins over this file.

## Use

1. `taskq site eta` (all Slurm executors with a GPU partition; name machines to narrow). It runs in seconds.
2. Read the report and answer with the time, not the command output.
3. If the user also asks why a job is slow to start or whether it risks being cancelled, continue with `taskq-slurm`.

## Report

- First sentence: when each of the project's accounts gets its next GPU (date and hour, and in how many hours), and which job.
- Then who holds the GPUs released before that, and whether new jobs from other users would jump ahead (the "Jumping the queue" list: users whose base priority is above ours; a job they submit now starts first).
- One line on how good the method has been lately: the "Looking back" line for our jobs.
- Say plainly that it assumes nobody submits more and that running jobs end as their users' earlier jobs did.

## Improving the method

When "Looking back" shows our jobs missed by more than about two hours, or a run is clearly wrong:

1. Find the miss: the kept snapshot from before the start is in the state dir under `slurm_eta/<partition>/raw/`. `taskq site eta --replay <file>` reruns it and keeps nothing.
2. Work out which rule was wrong (end-time estimate, priority growth, per-user limit, backfill reservation, dependency, a hold reason) by comparing the simulated timeline with the starts recorded later.
3. Change `sites/slurm_eta.py` in the taskq dev checkout, raise `METHOD` by one, and run `taskq site eta --backtest` before and after: keep the change only if both "everyone" and "ours" do not get worse. Add a case to `sites/test_slurm_eta.py`.
4. Release through `taskq-ship`. Write the change and the backtest numbers before and after into the project's `.claude/taskq/eta.md` (improvement record).

## Assumptions of the method

- Running jobs end at the median share of their time limit that the same user's earlier jobs (those that ran longer than this one has so far) used; with too few records, at their limit.
- Queued jobs run to their time limit when deciding backfill.
- Priority grows only through age, up to the partition's maximum; fairshare and other parts stay as in the snapshot.
- A job that cannot start now reserves the earliest time it fits (once it has waited the configured minimum); later jobs only start before it if they end by then.
- The per-user GPU limit is the smaller of the job's QOS and the partition's QOS.
- Held jobs and jobs waiting on a dependency that has not resolved never start in the simulation.
- Each node's CPUs, memory and GPUs are the partition totals divided by the node count (reading nodes often needs rights a user lacks).
- The per-user limit counts only this partition's jobs, though a QOS limit may span partitions.
- Dependencies are read only in the `after*:<job id>` form.
