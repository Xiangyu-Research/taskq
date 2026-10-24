#!/bin/bash
#SBATCH --job-name=cpu_worker
#SBATCH --partition=cpu-epyc,cpu-epyc-genoa
#SBATCH --qos=normal
#SBATCH --time=08:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --output=%x_%j.out
REPO=$(cd "${SLURM_SUBMIT_DIR:-$(dirname "${BASH_SOURCE[0]}")}" && git rev-parse --show-toplevel)
WORKER_DEVICE=cpu exec bash "$REPO/scripts/queue/worker_start.sh"
