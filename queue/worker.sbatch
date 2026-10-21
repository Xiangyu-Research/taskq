#!/bin/bash
#SBATCH --job-name=train_worker
#SBATCH --partition=gpu-a100
#SBATCH --qos=long
#SBATCH --time=3-00:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:a100:1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --output=%x_%j.out
REPO=$(git -C "${SLURM_SUBMIT_DIR:-$(dirname "${BASH_SOURCE[0]}")}" rev-parse --show-toplevel)
exec bash "$REPO/scripts/queue/worker_start.sh"
