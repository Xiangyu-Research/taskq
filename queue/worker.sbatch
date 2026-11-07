#!/bin/bash
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
# GPU worker 作业：sites/slurm.py 交，分区、卡、核、内存、QOS、时限、日志、TASKQ_ROOT、TASKQ_MACHINE 都在 sbatch 的命令行上；
# $1 = 根目录 code/current 下的 worker_start.sh（作业开始时才解析，用的是那时的代码包）
exec bash "$1"
