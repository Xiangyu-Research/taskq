#!/bin/bash
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(dirname "$(dirname "$HERE")")
if [[ ${WORKER_DEVICE:-gpu} == gpu && -z ${TASKQ_DIR:-} ]]; then
    SITE=$(python3 "$REPO/envs/machines.py" site-root . 2> /dev/null) || SITE=
    if [[ -n $SITE && -f $SITE/code/current/scripts/queue/worker.sbatch ]]; then
        echo "$(date '+%F %T') 这台已经换成 $SITE 的代码包，转过去"
        exec bash "$SITE/code/current/scripts/queue/worker.sbatch"
    fi
fi
[[ -f ${TASKQ_DIR:-$HOME/.cache/phd/taskq}/ENABLED ]] || { echo "$(date '+%F %T') 任务队列的总开关不在，结束"; exit 0; }
read -r CONDA_BASE CONDA_ENV < <(python3 "$REPO/envs/machines.py" get . conda) || true
[[ -f ${CONDA_BASE:-}/etc/profile.d/conda.sh ]] || { echo "envs/machines.py 里没有这台机器的 conda（${CONDA_BASE:-没有}）" >&2; exit 1; }
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"
TASKQ_DATA=$(python3 "$REPO/envs/machines.py" get . data)
export TASKQ_DATA BRAIN_TUMOR=$TASKQ_DATA
source "$HERE/gpu_pad_start.sh"
exec bash "$HERE/dicc_worker.sh"
