#!/bin/bash
# Example "training": count to N, checkpoint every 10 steps (ckpt.json), write progress (PROGRESS.json), resume from the checkpoint,
# write DONE when finished.
#   bash bin/count.sh <N> <result dir>
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
