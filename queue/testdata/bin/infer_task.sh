#!/bin/bash
# Test inference task (same shape as real): run one part of inference and write <output dir>/MANIFEST.json (the done marker).
#   bash infer_task.sh <model dir> <part>
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MODEL=$(cd "${1:?usage: infer_task.sh <model dir> <part>}" && pwd) FRAG=${2:?missing part}
bash "$HERE/step.sh" --stage infer "$MODEL" "$FRAG"
PRED=${TASKQ_DATA:?}/preds/$(basename "$(dirname "$MODEL")")/$(basename "$MODEL")/frag_$FRAG
mkdir -p "$PRED"
[[ -f $PRED/MANIFEST.json ]] || echo '{"cohorts": {}, "probabilities_zeroed_below": {}}' > "$PRED/MANIFEST.json"
