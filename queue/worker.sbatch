#!/bin/bash
#SBATCH --job-name=train_worker
#SBATCH --partition=gpu-a100
#SBATCH --qos=long
#SBATCH --time=3-00:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:a100:1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --output=%x_%A_%a.out
set -uo pipefail
REPO=$(git -C "${SLURM_SUBMIT_DIR:-$(dirname "${BASH_SOURCE[0]}")}" rev-parse --show-toplevel)
[[ -f ${TASKQ_DIR:-$HOME/.cache/phd/taskq}/ENABLED ]] || { echo "$(date '+%F %T') 任务队列的总开关不在，结束"; exit 0; }
read -r CONDA_BASE CONDA_ENV < <(python3 "$REPO/envs/machines.py" get . conda) || true
[[ -f ${CONDA_BASE:-}/etc/profile.d/conda.sh ]] || { echo "envs/machines.py 里没有这台机器的 conda（${CONDA_BASE:-没有}）" >&2; exit 1; }
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"
export BRAIN_TUMOR=$(python3 "$REPO/envs/machines.py" get . data)
source "$REPO/scripts/queue/gpu_pad_start.sh"
exec bash "$REPO/scripts/queue/dicc_worker.sh"
