#!/bin/bash
#SBATCH --partition=cpu
#SBATCH --qos=normal
#SBATCH --time=08:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --output=%x_%j.out
# 模板（11/4）：同 worker.sbatch
WORKER_DEVICE=cpu exec bash "${SLURM_SUBMIT_DIR:-.}/worker_start.sh"
