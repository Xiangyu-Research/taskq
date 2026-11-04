#!/bin/bash
#SBATCH --partition=gpu-a100
#SBATCH --qos=long
#SBATCH --time=3-00:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:a100:1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --output=%x_%j.out
REPO=$(cd "${SLURM_SUBMIT_DIR:-$(dirname "${BASH_SOURCE[0]}")}" && git rev-parse --show-toplevel)
exec bash "$REPO/scripts/taskq/queue/worker_start.sh"
