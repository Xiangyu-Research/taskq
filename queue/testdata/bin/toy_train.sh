#!/bin/bash
# 测试用的训练（toy 做法的命令）：在结果文件夹里写一份日志、存档和验证结果；DEMO_EPOCHS（默认 2）轮，每轮 DEMO_SLEEP 秒（默认 0）。
#   bash toy_train.sh <ds> <cfg> <frag> <plan> <variant>
set -euo pipefail
DS=$1 CFG=$2 FRAG=$3 PLAN=$4 VAR=$5
OUT=${DEMO_RESULTS:-${TASKQ_DATA:?}/results}/$DS/${VAR}__${PLAN}__$CFG/frag_$FRAG
mkdir -p "$OUT/check"
LOG=$OUT/train_log_$(date +%Y_%m_%d_%H_%M_%S).txt
for ((e = 0; e < ${DEMO_EPOCHS:-2}; e++)); do
    echo "$(date '+%F %T'): Epoch $e" >> "$LOG"
    sleep "${DEMO_SLEEP:-0}"
    echo "$(date '+%F %T'): Epoch time: ${DEMO_SLEEP:-0} s" >> "$LOG"
    touch "$OUT/latest.ckpt"
done
echo '{"metric_per_case": []}' > "$OUT/check/summary.json"
touch "$OUT/final.ckpt"
