#!/bin/bash
# 示例的"训练"：数到 N，每 10 步存档（ckpt.json）、写进度（PROGRESS.json），重跑从存档接着数，数完写 DONE。
#   bash bin/count.sh <N> <结果文件夹>
set -euo pipefail
N=$1 OUT=$2
mkdir -p "$OUT"
i=0
[[ -f $OUT/ckpt.json ]] && i=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["i"])' "$OUT/ckpt.json")
while (( i < N )); do
    i=$((i + 1))
    if (( i % 10 == 0 || i == N )); then
        echo "{\"i\": $i}" > "$OUT/ckpt.json"
        echo "{\"done\": $i, \"total\": $N}" > "$OUT/PROGRESS.json"
        echo "$(date '+%F %T') $i/$N" >> "$OUT/count.log"
    fi
done
touch "$OUT/DONE"
