#!/bin/bash
# Test step script (same shape as real inference / eval scripts): --stage needs:<infer|eval|all> only prints the inputs that
# stage reads (relative to the data dir, one per line, * for wildcards); the model may not be on this machine yet. Other stages log one line.
#   bash step.sh [--stage infer|eval|all|needs[:<stage>]] <model dir> <part, such as 0 or "0 1 2 3 4">
set -euo pipefail
STAGE=all
if [[ ${1:-} == --stage ]]; then STAGE=${2:?--stage needs infer or eval}; shift 2; fi
NEEDS=
[[ $STAGE == needs ]] && STAGE=needs:infer
if [[ $STAGE == needs:* ]]; then NEEDS=${STAGE#needs:}; STAGE=needs; fi
case $STAGE in all | infer | eval | needs) ;; *) echo "--stage must be infer, eval, all or needs:<stage>: $STAGE" >&2; exit 2 ;; esac
case $NEEDS in '' | infer | eval | all) ;; *) echo "after needs: use infer, eval or all: $NEEDS" >&2; exit 2 ;; esac
MODEL=${1:?usage: step.sh [--stage ...] <model dir> <part>}
FRAGS=${2:?missing part}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DATA=${TASKQ_DATA:?}
if [[ $STAGE == needs ]]; then MODEL=$(realpath -m "$MODEL"); else MODEL=$(cd "$MODEL" && pwd); fi
DS=$(basename "$(dirname "$MODEL")") VAR=$(basename "$MODEL")
if [[ $(echo $FRAGS | wc -w) == 1 ]]; then TAG=frag_$FRAGS; else TAG=frags_$(echo $FRAGS | tr ' ' '-'); fi
EXT=(B G-A G-B G-C G-D X)
if [[ -z ${SKIP_EXT+x} && -z ${ONLY_EXT+x} ]]; then
    X=$(python3 "$HERE/../../experiments.py" name-of "$MODEL" skip_ext only_ext) || { echo "or set SKIP_EXT / ONLY_EXT" >&2; exit 1; }
    { IFS= read -r SKIP_EXT || true; IFS= read -r ONLY_EXT || true; } <<< "$X"
fi
KEEP=()
for c in "${EXT[@]}"; do
    if [[ -n ${ONLY_EXT:-} ]]; then [[ " $ONLY_EXT " == *" $c "* ]]; else [[ " ${SKIP_EXT:-} " != *" $c "* ]]; fi && KEEP+=("$c") || true
done
if [[ $STAGE == needs ]]; then
    BT=$(mkdir -p "$DATA" && cd "$DATA" && pwd -P)/
    M=$MODEL/
    [[ $M == "$BT"* ]] || { echo "the model is not under $DATA" >&2; exit 1; }
    M=${M#"$BT"}
    P=preds/$DS/$VAR
    {
    echo "${M}dataset.json"
    if [[ $NEEDS != eval ]]; then
        echo "${M}plans.json"
        for f in $FRAGS; do echo "${M}frag_$f/final.ckpt"; done
        echo "raw/holdout/A/inputs"
        for c in "${KEEP[@]}"; do echo "raw/ext/$c/inputs"; done
    fi
    if [[ $NEEDS != infer ]]; then
        if [[ $NEEDS == all && $TAG == frags_* ]]; then for f in $FRAGS; do echo "$P/frag_$f"; done; fi
        if [[ $NEEDS == eval ]]; then echo "$P/$TAG"; fi
        echo "raw/labels/*/*-lab.dat"
        for c in "${KEEP[@]}"; do echo "raw/ext/$c/labels"; done
        if [[ $TAG == frag_* && $DS == D101_* ]]; then echo "${M}$TAG/check"; fi
    fi
    } | awk '!seen[$0]++'
    exit 0
fi
echo "$(date '+%F %T') step.sh $STAGE $MODEL $FRAGS（${KEEP[*]}）"
