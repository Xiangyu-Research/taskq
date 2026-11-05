#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=%x_%j.out
# 模板（11/4）：同 worker.sbatch
WORKER_DEVICE=cpu exec bash "${SLURM_SUBMIT_DIR:-.}/worker_start.sh"
