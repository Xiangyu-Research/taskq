---
name: taskq-slurm
description: A project's Slurm GPU jobs: when each executor gets its next GPU (`taskq site eta`, a simulation from one snapshot of the whole partition, improved when it misses), GPU utilisation and memory, why utilisation is low, whether the cluster may cancel a job, and why it is still queued (priority, fairshare, limits). Use when the user asks 下一张卡什么时候拿到、什么时候能拿到卡、还要排多久、集群什么时候有卡, says sssh <job id> 怎么一直 0%、利用率怎么这么低、会不会被杀、fairshare / 优先级 / 排队、为什么还在排队, or "when will my job start", "why is my job pending", "is my job idle".
---

# Slurm GPU jobs

Read only, except the fixes the project's rules allow without asking. Cancelling a job (`scancel`) always needs the user's approval, with the job id.

## Project notes

The project root is `$TASKQ_PROJECT`, or the nearest directory above the current one holding `.taskq.toml`. If `<project root>/.claude/taskq/slurm.md` exists, read it first: it holds the cluster's limits (cancellation rules, memory floor, per-user GPUs, submit slots, QOS time limits), the project's own rules and the improvement record of the start prediction, and wins over this file. Machine names and partitions come from `taskq machines names` and the machine table (`jobs.gpu`).

## Utilisation

1. Which jobs run where: `ssh <ssh name> 'squeue --me -o "%.10i %.9P %.20j %.2t %.10M %.10L %R"'`, and `taskq site status <machine>` for what taskq put in each job.
2. The GPU of a running job, from the login node (a light call only): `ssh <ssh name> 'srun --jobid=<id> --overlap -n1 nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader'`. Use the real job id (`squeue -o %A`); `srun` does not accept array notation. Ask through the account that owns the job.
3. When utilisation is low, check in this order:
   - Startup: the first minutes of a worker job copy data and build the env; low use then is normal. `taskq site status` and the worker log show the phase.
   - I/O: data read from shared storage instead of node-local scratch; look at the worker log and the job's working dir.
   - The GPU pad: Slurm workers keep a pad on the GPU (`gpu_pad` in the machine table: floor, target, high in MiB) where the cluster requires a minimum use; check it is running and in range. It is a cluster requirement, not waste. To turn it off on one machine for a while: `taskq switch set gpu_pad <machine> off --for <length> --why <reason>`.
   - CPU: too few CPUs for the data loading (`sstat -j <id>`, `top` through `srun --overlap`).
   - The task itself: between tasks, or a task that does little GPU work.
4. Compare with the cluster's cancellation rule in the project notes and say how close the job is to it.

## Queued jobs: priority and fairshare

- Why it waits: `squeue -j <id> -o "%R"` (Priority, Resources, a QOS or association limit, a dependency, a hold).
- Priority parts: `sprio -j <id> -l`; the account's fairshare: `sshare -l -u <user>` (and `sshare -l -a` for the whole tree). Usage decays with the cluster's half-life (`scontrol show config | grep -i PriorityDecayHalfLife`), so heavy use lowers priority for a while.
- Limits: `sacctmgr -nP show qos format=name,maxtrespu,maxwall`, `sacctmgr show assoc user=<user> format=user,account,qos,maxjobs,maxsubmit`.
- When it will start: see "When the next GPU comes" below.

## When the next GPU comes

`taskq site eta` reads, through one ssh call per cluster, every user's jobs in the GPU partition, their priority parts (`sprio`), the partition, the per-user QOS limits and the age and backfill settings, then simulates the queue forward. Each run keeps the raw snapshot, the predictions and the starts and ends it saw in the state dir, and compares the predictions made 1-48 hours before each start with the actual start. A periodic job (`taskq site eta --record`) builds that history.

### Use

1. `taskq site eta` (all Slurm executors with a GPU partition; name machines to narrow). It runs in seconds.
2. Read the report and answer with the time, not the command output.

### Report

- First sentence: when each of the project's accounts gets its next GPU (date and hour, and in how many hours), and which job.
- Then who holds the GPUs released before that, and whether new jobs from other users would jump ahead (the "Jumping the queue" list: users whose base priority is above ours; a job they submit now starts first).
- One line on how good the method has been lately: the "Looking back" line for our jobs.
- Say plainly that it assumes nobody submits more and that running jobs end as their users' earlier jobs did.

### Improving the method

When "Looking back" shows our jobs missed by more than about two hours, or a run is clearly wrong:

1. Find the miss: the kept snapshot from before the start is in the state dir under `slurm_eta/<partition>/raw/`. `taskq site eta --replay <file>` reruns it and keeps nothing.
2. Work out which rule was wrong (end-time estimate, priority growth, per-user limit, backfill reservation, dependency, a hold reason) by comparing the simulated timeline with the starts recorded later.
3. Change `sites/slurm_eta.py` in the taskq dev checkout, raise `METHOD` by one, and run `taskq site eta --backtest` before and after: keep the change only if both "everyone" and "ours" do not get worse. Add a case to `sites/test_slurm_eta.py`.
4. Release through `taskq-ship`. Write the change and the backtest numbers before and after into the project's `.claude/taskq/slurm.md` (improvement record).

### Assumptions of the method

- Running jobs end at the median share of their time limit that the same user's earlier jobs (those that ran longer than this one has so far) used; with too few records, at their limit.
- Queued jobs run to their time limit when deciding backfill.
- Priority grows only through age, up to the partition's maximum; fairshare and other parts stay as in the snapshot.
- A job that cannot start now reserves the earliest time it fits (once it has waited the configured minimum); later jobs only start before it if they end by then.
- The per-user GPU limit is the smaller of the job's QOS and the partition's QOS.
- Held jobs and jobs waiting on a dependency that has not resolved never start in the simulation.
- Each node's CPUs, memory and GPUs are the partition totals divided by the node count (reading nodes often needs rights a user lacks).
- The per-user limit counts only this partition's jobs, though a QOS limit may span partitions.
- Dependencies are read only in the `after*:<job id>` form.

## Report (utilisation and queue)

- First sentence: the conclusion (the job is fine, at risk of cancellation, or waiting for a stated reason).
- Per job one line: utilisation, memory, phase, and the cause if low.
- What to do: fixes already made (record them with `taskq note --kind fix`), and decisions for the user (cancelling, extra jobs, QOS or time limit changes), each with a recommendation.
