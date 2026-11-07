#!/bin/bash
set -uo pipefail
HERE=$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)  # 真实路径：经 code/current 进来时钉在开始时的代码包上
eval "$(python3 "$HERE/proto.py" sh)"; [[ -n ${TQ_SAFE:-} ]] || { echo "读不了 $HERE/proto.py" >&2; exit 1; }
CORE=$(cd "$HERE/.." && pwd)  # 调度代码的根；使用者的仓库根 REPO（TASKQ_REPO，没设时同 cfg.py）
if [[ -n ${TASKQ_REPO:-} ]]; then REPO=$TASKQ_REPO; elif [[ $CORE == */scripts/taskq ]]; then REPO=${CORE%/scripts/taskq}; else eval "$(python3 "$CORE/queue/project.py" sh 2> /dev/null)"; REPO=${TASKQ_REPO:-$CORE}; fi
QD=${TASKQ_DIR:-$(python3 "$CORE/queue/cfg.py" state taskq)}
[[ -n ${TASKQ_HANDOFF:-} || -f $QD/$TQ_Q_ENABLED ]] || { echo "$(date '+%F %T') 任务队列的总开关不在，结束"; exit 0; }
read -r CONDA_BASE CONDA_ENV < <(python3 "$CORE/sites/mtab.py" get . conda) || true
if [[ ! -f ${CONDA_BASE:-}/etc/profile.d/conda.sh ]]; then
    echo "机器表（machines.py）里没有这台机器的 conda（${CONDA_BASE:-没有}）" >&2
    [[ -n ${TASKQ_HANDOFF:-} ]] || exit 1  # 换包途中：手上有在跑的任务，不能丢下不管，照样接着跑
else
    source "$CONDA_BASE/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV" || echo "$(date '+%F %T') !! conda activate $CONDA_ENV 失败（$CONDA_BASE）：任务照常各自激活环境，显存填充多半起不来"
fi
TASKQ_DATA=$(python3 "$CORE/queue/cfg.py" data)  # 执行机的数据目录：根目录下的 data（视图、结果、日志）
export TASKQ_DATA
source "$HERE/gpu_pad_start.sh"
exec python3 "$HERE/consume.py"
