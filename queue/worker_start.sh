#!/bin/bash
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(dirname "$(dirname "$HERE")")
if [[ -z ${TASKQ_DIR:-} ]]; then
    KIND=${WORKER_DEVICE:-gpu}
    [[ $KIND == gpu ]] && SB=worker.sbatch || SB=${KIND}_worker.sbatch
    SITE=$(python3 "${TASKQ_CONFIG:-$REPO/envs}/machines.py" site-root . 2> /dev/null) || SITE=
    if [[ -n $SITE && -f $SITE/code/current/scripts/queue/$SB ]]; then
        echo "$(date '+%F %T') 这台已经换成 $SITE 的代码包，转过去（$SB）"
        exec bash "$SITE/code/current/scripts/queue/$SB"
    fi
fi
QD=${TASKQ_DIR:-${TASKQ_STATE:+$TASKQ_STATE/taskq}}
[[ -f ${QD:-$HOME/.cache/phd/taskq}/ENABLED ]] || { echo "$(date '+%F %T') 任务队列的总开关不在，结束"; exit 0; }
read -r CONDA_BASE CONDA_ENV < <(python3 "${TASKQ_CONFIG:-$REPO/envs}/machines.py" get . conda) || true
[[ -f ${CONDA_BASE:-}/etc/profile.d/conda.sh ]] || { echo "envs/machines.py 里没有这台机器的 conda（${CONDA_BASE:-没有}）" >&2; exit 1; }
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"
TASKQ_DATA=$(python3 "${TASKQ_CONFIG:-$REPO/envs}/machines.py" get . data)
export TASKQ_DATA
source "$HERE/legacy_names.sh" export
source "$HERE/gpu_pad_start.sh"
exec bash "$HERE/dicc_worker.sh"
