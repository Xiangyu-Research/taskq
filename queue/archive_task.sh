#!/bin/bash
# Back up a task fetched to the store onto the backup machine (role backup in the table; to its backup dir when set, else its data
# dir) and verify it. Runs on the store (queue/site_ops.py's archive; the hub's relay queues relay_archive in the IO group); the
# data dir is TASKQ_DATA.
#   archive_task.sh --spec <resolved spec JSON> [--dry-run]
# Result dirs and files to back up come from the task's outputs (queue/spec.py outputs --spec): outs, extra files in the parent,
# and part logs.
# Steps:
#   1. store -> backup machine: outs, extra files in the parent, part logs (<data dir>/logs/tasks/<key filename>__*).
#      Files on the backup that differ are overwritten from the store (such as results compressed on the store); extra files on the
#      backup are not deleted.
#   2. verify: every file on the store exists on the backup with the same size and mtime (just transferred by rsync, which checked
#      checksums; do not read a lot from the backup's login node).
# Exit non-zero on mismatch (the health check reports it).
set -euo pipefail
USAGE="usage: archive_task.sh --spec <spec JSON> [--dry-run]"
[[ ${1:-} == --spec && -n ${2:-} ]] || { echo "$USAGE" >&2; exit 2; }
SPEC=$2 DRY=${3:-}
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # scheduler core root
M="python3 $CORE/sites/mtab.py"
HOST=$($M names store | head -1)
LD=${TASKQ_DATA:?run on the store: TASKQ_DATA is the data dir of the store}
BK=$($M backups | grep -vx "$HOST" | head -1)
[[ -n $BK ]] || { echo "the machine table (machines.py) has no backup machine (role backup) other than $HOST" >&2; exit 2; }
BKH=$($M get "$BK" ssh) BKD=$($M get "$BK" backup 2> /dev/null || $M get "$BK" data)
SH=$(python3 "$CORE/queue/spec.py" outputs --spec "$SPEC" --sh) || { echo "bad spec: ${SPEC:0:200}" >&2; exit 2; }
eval "$SH"
(( ARCHIVE )) || { echo "the recipe for $KEY does not archive (no archive in the spec)" >&2; exit 2; }
OUT=${OUTS[0]}
PARENT=$(dirname "$OUT") SRC=$LD/$OUT
log() { echo "$(date '+%F %T') $*"; }
[[ -d $SRC ]] && (cd "$SRC" && eval "$TEST") || { log "this task is not fully fetched on $HOST: $SRC"; exit 1; }
RB=(python3 "$CORE/sync/tunnel.py" rsync -rlt --partial)
[[ $DRY == --dry-run ]] && RB+=(-n --itemize-changes)

log "1. backup: ${OUTS[*]} ($HOST -> $BK)"
[[ $DRY == --dry-run ]] || ssh "$BKH" "mkdir -p ${OUTS[*]/#/$BKD/} $BKD/logs/tasks"
for x in ${EXTRA[@]+"${EXTRA[@]}"}; do
    compgen -G "$LD/$PARENT/$x" > /dev/null && "${RB[@]}" "$LD/$PARENT/"$x "$BKH:$BKD/$PARENT/"
done
for o in "${OUTS[@]}"; do
    [[ -d $LD/$o ]] && "${RB[@]}" "$LD/$o/" "$BKH:$BKD/$o/"
done
SEGS=()
for f in "$LD/$LOGS"*; do  # only this task's: a prefix followed by another __ is a different, longer key
    [[ -e $f && ${f#"$LD/$LOGS"} != *__* ]] && SEGS+=("$f")
done
(( ${#SEGS[@]} )) && "${RB[@]}" "${SEGS[@]}" "$BKH:$BKD/logs/tasks/"

log "2. verify"
diffs() {  # list mismatches (files to transfer: pull >, push <; dirs or links to create: c); an rsync error (such as unreachable) exits the script
    local out
    out=$(rsync "$@") || { log "rsync failed during verify (exit $?)" >&2; return 1; }
    grep -E '^(<|>|c)' <<< "$out" || true
}
bad_bk=
for o in "${OUTS[@]}"; do
    [[ -d $LD/$o ]] && bad_bk+=$(diffs -rltn --itemize-changes "$LD/$o/" "$BKH:$BKD/$o/")
done
if [[ $DRY == --dry-run ]]; then
    log "(dry-run: nothing backed up on $BK yet, skipping its verify)"
elif [[ -n $bad_bk ]]; then
    log "the copy on $BK differs from $HOST:"; echo "$bad_bk" | head -20; exit 1
fi
log "the copy on $HOST is backed up to $BK and verified"
