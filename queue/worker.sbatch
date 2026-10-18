#!/bin/bash
#SBATCH --job-name=rq1_worker
#SBATCH --partition=gpu-a100
#SBATCH --qos=long
#SBATCH --time=1-12:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:a100:1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --output=%x_%A_%a.out
# DICC 上不带折的训练作业（2026-10-18）：作业只负责领到一张卡，开始后从任务队列（scripts/queue/taskq.py）里按顺序领任务，
# 什么模型都领（scripts/queue/dicc_worker.sh：先训 1 个，GPU 不忙、显存够时再加 1 个；显存填充等训练稳定后再补）。
# 到时限时没训完的任务交还队列，下一个作业从存档接着训。数组的每个任务是一段，%1 让一次只跑一段：
#   cd research/RQ1/nnunet && sbatch --array=1-10%1 worker.sbatch
# 默认 long QOS、每段 36 小时（10/18 定）；交到 normal QOS 时：sbatch --qos=normal --time=08:00:00 --array=1-20%1 worker.sbatch
# 队列的总开关 ~/.cache/phd/taskq/ENABLED 不在时什么都不做（这种作业没有自己那一折可以训）。
set -uo pipefail
REPO=$(git -C "${SLURM_SUBMIT_DIR:-$(dirname "${BASH_SOURCE[0]}")}" rev-parse --show-toplevel)
[[ -f ${TASKQ_DIR:-$HOME/.cache/phd/taskq}/ENABLED ]] || { echo "$(date '+%F %T') 任务队列的总开关不在，结束"; exit 0; }
read -r CONDA_BASE CONDA_ENV < <(python3 "$REPO/envs/machines.py" get . conda) || true
[[ -f ${CONDA_BASE:-}/etc/profile.d/conda.sh ]] || { echo "envs/machines.py 里没有这台机器的 conda（${CONDA_BASE:-没有}）" >&2; exit 1; }
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"
source "$REPO/envs/nnunet_paths.sh"  # 数据目录；显存填充（gpu_pad.py）也在这里启动
exec bash "$REPO/scripts/queue/dicc_worker.sh"
