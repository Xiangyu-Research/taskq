#!/bin/bash
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --output=%x_%j.out
# 模板（11/4）：relay_bundle.py 只取 #SBATCH 行，分区、qos、时限、CPU、内存都写在执行机条目的 sbatch 里（这里只放通用的），生成各执行机的
exec bash "${SLURM_SUBMIT_DIR:-.}/worker_start.sh"
