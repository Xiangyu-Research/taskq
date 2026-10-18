#!/bin/bash
#SBATCH --job-name=train_worker
#SBATCH --partition=gpu-a100
#SBATCH --qos=long
#SBATCH --time=1-12:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:a100:1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --output=%x_%A_%a.out
# DICC 上的训练作业（2026-10-18）：作业只负责领到一张卡，开始后从任务队列（scripts/queue/taskq.py）里按顺序领任务，
# 什么任务都领（scripts/queue/dicc_worker.sh：先训 1 个，GPU 不忙、显存够时再加 1 个；显存填充等训练稳定后再补）。
# 到时限时没训完的任务交还队列，下一个作业从存档接着训。数组不限并发：每一段都自己去队列领任务，同时跑几个由 QOS 的 GPU 上限管。
# 平时由 box 上的 scripts/queue/refill.py 自动补交（每个 QOS 排着的不到上限就补满，QOS 和每段时限在 envs/machines.py）；手动交：
#   cd scripts/queue && sbatch --array=1-10 worker.sbatch                                   （默认 long、每段 36 小时，10/18 定）
#   cd scripts/queue && sbatch --qos=normal --time=08:00:00 --array=1-10 worker.sbatch
# 队列的总开关 ~/.cache/phd/taskq/ENABLED 不在时什么都不做。
set -uo pipefail
REPO=$(git -C "${SLURM_SUBMIT_DIR:-$(dirname "${BASH_SOURCE[0]}")}" rev-parse --show-toplevel)
[[ -f ${TASKQ_DIR:-$HOME/.cache/phd/taskq}/ENABLED ]] || { echo "$(date '+%F %T') 任务队列的总开关不在，结束"; exit 0; }
# worker 自己只用 python3 的标准库；每个任务的环境由 run_task.sh 按任务说明换
read -r CONDA_BASE CONDA_ENV < <(python3 "$REPO/envs/machines.py" get . conda) || true
[[ -f ${CONDA_BASE:-}/etc/profile.d/conda.sh ]] || { echo "envs/machines.py 里没有这台机器的 conda（${CONDA_BASE:-没有}）" >&2; exit 1; }
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"
BRAIN_TUMOR=$(python3 "$REPO/envs/machines.py" get . data)
export BRAIN_TUMOR
source "$REPO/scripts/queue/gpu_pad_start.sh"
exec bash "$REPO/scripts/queue/dicc_worker.sh"
