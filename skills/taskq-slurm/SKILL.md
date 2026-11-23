---
name: taskq-slurm
description: Diagnose a project's Slurm GPU jobs: GPU utilisation and memory, why utilisation is low, whether the cluster may cancel the job, and why it is still queued (priority, fairshare, limits). Use when the user says sssh <job id> 怎么一直 0%、利用率怎么这么低、会不会被杀、fairshare / 优先级 / 排队、为什么还在排队, or "why is my job pending", "is my job idle". For when the next GPU comes, use taskq-eta.
---

# Slurm job diagnosis

Read only, except the fixes the project's rules allow without asking. Cancelling a job (`scancel`) always needs the user's approval, with the job id.

## Project notes

The project root is `$TASKQ_PROJECT`, or the nearest directory above the current one holding `.taskq.toml`. If `<project root>/.claude/taskq/slurm.md` exists, read it first: it holds the cluster's limits (cancellation rules, memory floor, per-user GPUs, submit slots, QOS time limits) and the project's own rules, and wins over this file. Machine names and partitions come from `taskq machines names` and the machine table (`jobs.gpu`).

## Utilisation

1. Which jobs run where: `ssh <ssh name> 'squeue --me -o "%.10i %.9P %.20j %.2t %.10M %.10L %R"'`, and `taskq site status <machine>` for what taskq put in each job.
2. The GPU of a running job, from the login node (a light call only): `ssh <ssh name> 'srun --jobid=<id> --overlap -n1 nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader'`. Use the real job id (`squeue -o %A`); `srun` does not accept array notation. Ask through the account that owns the job.
3. When utilisation is low, check in this order:
   - Startup: the first minutes of a worker job copy data and build the env; low use then is normal. `taskq site status` and the worker log show the phase.
   - I/O: data read from shared storage instead of node-local scratch; look at the worker log and the job's working dir.
   - The GPU pad: Slurm workers keep a pad on the GPU (`gpu_pad` in the machine table: floor, target, high in MiB) where the cluster requires a minimum use; check it is running and in range. It is a cluster requirement, not waste.
   - CPU: too few CPUs for the data loading (`sstat -j <id>`, `top` through `srun --overlap`).
   - The task itself: between tasks, or a task that does little GPU work.
4. Compare with the cluster's cancellation rule in the project notes and say how close the job is to it.

## Queued jobs: priority and fairshare

- Why it waits: `squeue -j <id> -o "%R"` (Priority, Resources, a QOS or association limit, a dependency, a hold).
- Priority parts: `sprio -j <id> -l`; the account's fairshare: `sshare -l -u <user>` (and `sshare -l -a` for the whole tree). Usage decays with the cluster's half-life (`scontrol show config | grep -i PriorityDecayHalfLife`), so heavy use lowers priority for a while.
- Limits: `sacctmgr -nP show qos format=name,maxtrespu,maxwall`, `sacctmgr show assoc user=<user> format=user,account,qos,maxjobs,maxsubmit`.
- When it will start: `taskq site eta` (skill `taskq-eta`).

## Report

- First sentence: the conclusion (the job is fine, at risk of cancellation, or waiting for a stated reason).
- Per job one line: utilisation, memory, phase, and the cause if low.
- What to do: fixes already made (record them with `taskq note --kind fix`), and decisions for the user (cancelling, extra jobs, QOS or time limit changes), each with a recommendation.
