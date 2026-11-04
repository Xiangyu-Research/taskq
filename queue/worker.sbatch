#!/bin/bash
#SBATCH --partition=gpu
#SBATCH --qos=long
#SBATCH --time=3-00:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --output=%x_%j.out
# 模板（11/4）：relay_bundle.py 只取 #SBATCH 行，按执行机条目的 sbatch 覆盖（partition、gres、cpus、mem…），生成各执行机的
exec bash "${SLURM_SUBMIT_DIR:-.}/worker_start.sh"
