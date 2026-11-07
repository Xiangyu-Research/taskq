#!/bin/bash
#SBATCH --nodes=1
# CPU worker 作业：同 worker.sbatch（squeue 按文件名认出是 CPU worker）
WORKER_DEVICE=cpu exec bash "$1"
