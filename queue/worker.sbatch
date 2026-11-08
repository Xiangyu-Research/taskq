#!/bin/bash
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
# GPU worker job: submitted by sites/slurm.py; partition, GPU, cores, memory, QOS, time limit, log, TASKQ_ROOT and TASKQ_MACHINE are all on the sbatch command line;
# $1 = worker_start.sh under the root's code/current (resolved when the job starts, so it uses the bundle of that moment)
exec bash "$1"
