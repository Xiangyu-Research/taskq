#!/bin/bash
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(dirname "$(dirname "$HERE")")
[[ -f ${TASKQ_DIR:-$HOME/.cache/phd/taskq}/ENABLED ]] || { echo "$(date '+%F %T') 任务队列的总开关不在，结束"; exit 0; }
read -r CONDA_BASE CONDA_ENV < <(python3 "$REPO/envs/machines.py" get . conda) || true
[[ -f ${CONDA_BASE:-}/etc/profile.d/conda.sh ]] || { echo "envs/machines.py 里没有这台机器的 conda（${CONDA_BASE:-没有}）" >&2; exit 1; }
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"
export BRAIN_TUMOR=$(python3 "$REPO/envs/machines.py" get . data)
_tmp_root=${TASKQ_DIR:-$HOME/.cache/phd/taskq}/TEMP_ROOT
if [[ -n ${SLURM_JOB_ID:-} && -s $_tmp_root ]]; then
    _t=$(grep -v '^[[:space:]]*\(#\|$\)' "$_tmp_root" | head -n 1 | tr -d '[:space:]')
    _t=${_t/#\~/$HOME}
    if [[ -d $_t ]]; then
        echo "$(date '+%F %T') 临时数据目录 $_t（$_tmp_root），只领那里列的任务"
        export BRAIN_TUMOR=$_t
    fi
fi
source "$HERE/gpu_pad_start.sh"
exec bash "$HERE/dicc_worker.sh"
