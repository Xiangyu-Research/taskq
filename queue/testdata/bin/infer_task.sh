#!/bin/bash
# 测试用的推理任务（形状同真的）：跑一份推理，写 <输出文件夹>/MANIFEST.json（做完的标志）。
#   bash infer_task.sh <模型文件夹> <第几份>
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MODEL=$(cd "${1:?用法：infer_task.sh <模型文件夹> <第几份>}" && pwd) FRAG=${2:?缺第几份}
bash "$HERE/step.sh" --stage infer "$MODEL" "$FRAG"
PRED=${TASKQ_DATA:?}/preds/$(basename "$(dirname "$MODEL")")/$(basename "$MODEL")/frag_$FRAG
mkdir -p "$PRED"
[[ -f $PRED/MANIFEST.json ]] || echo '{"cohorts": {}, "probabilities_zeroed_below": {}}' > "$PRED/MANIFEST.json"
