#!/bin/bash
# Send large data from this machine to another machine (for when one ssh stream is slow, or ssh over VPN competes with
# other transfers): use sync/tunnel.py push over this machine's Cloudflare tunnel (the other side downloads; with
# transfer_job in the machine table it downloads on a CPU node). Data goes through Cloudflare.
#
# Run on the machine that has the source data (it takes tens of minutes, so use tmux or the background):
#   taskq tunnel push-dirs <machine> <source dir here> <target dir there (relative to home)> <subdir>...
# Example (inputs for a few datasets):
#   taskq tunnel push-dirs gpubox ~/data/raw data/raw setA/images setB/images
#
# How it works and safety (the transfer itself: sync/tunnel.py):
# - Symlinks are resolved here and stored as real files there. Files that exist there with the same size and mtime are
#   skipped, so rerunning after an interruption continues.
# - In the end both sides compute MD5 per file (following symlinks) and list the mismatches.
# - Before the transfer it puts .incomplete in each subdir there and removes it only after the check matches: refill.py
#   does not take a partial dir left by an interrupted run as data being present.
set -euo pipefail

PROG=${TASKQ_PROG:-tunnel_push.sh}
unset TASKQ_PROG
USAGE="usage: $PROG <machine> <source dir here> <target dir there (relative to home)> <subdir>..."
[[ ${1:-} == -h || ${1:-} == --help ]] && { echo "$USAGE"; exit 0; }
(( $# >= 4 )) || { echo "$USAGE" >&2; exit 2; }
HOST=$1 SRC=$2 DEST=$3
shift 3
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # scheduler code root; REPO is the user's repo root (TASKQ_REPO, as cfg.py when unset)
if [[ -n ${TASKQ_REPO:-} ]]; then REPO=$TASKQ_REPO; elif [[ $CORE == */scripts/taskq ]]; then REPO=${CORE%/scripts/taskq}; else eval "$(python3 "$CORE/queue/project.py" sh 2> /dev/null)"; REPO=${TASKQ_REPO:-$CORE}; fi
eval "$(python3 "$CORE/queue/proto.py" sh)"  # TQ_INCOMPLETE
rc=0
H=$(python3 "$CORE/sites/mtab.py" get "$HOST" ssh 2> /dev/null) || rc=$?
(( rc != TQ_EXIT_GONE )) || { echo "$PROG: no $HOST in the machine table" >&2; exit "$rc"; }
[[ -n $H ]] || { echo "$PROG: $HOST has no ssh, cannot transfer" >&2; exit 2; }
SUBS=("$@")
SRC=$(cd "$SRC" && pwd)
W=$(mktemp -d)
trap 'rm -rf "$W"' EXIT

(cd "$SRC" && find -L "${SUBS[@]}" -type f | sort) > "$W/files.txt"
N=$(wc -l < "$W/files.txt")
(( N > 0 )) || { echo "no files" >&2; exit 1; }
MARKS=$(printf " ~/$DEST/%q/$TQ_INCOMPLETE" "${SUBS[@]}")
ssh "$H" "for f in $MARKS; do mkdir -p \"\$(dirname \$f)\" && touch \$f; done"
python3 "$CORE/sync/tunnel.py" push "$HOST" "$SRC" "$DEST" "$W/files.txt"

# check: compute MD5 on both sides (following symlinks, so a symlink there also matches), list files missing or different there
echo "$(date '+%F %T') checking checksums (mismatched files are listed below; nothing listed = match)"
BAD=$(diff <(cd "$SRC" && xargs -d '\n' -a "$W/files.txt" md5sum) \
           <(ssh "$H" "cd $(printf %q "$DEST") && xargs -d '\n' md5sum 2> /dev/null" < "$W/files.txt") \
      | sed -n 's/^< [0-9a-f]*  //p' || true)
if [[ -n $BAD ]]; then
    echo "$BAD" | head -20
    echo "mismatch: $(echo "$BAD" | wc -l) files (rerun this script to retry files with the wrong size; for files with the right size but wrong content, delete them on $HOST and rerun)"
    exit 1
fi
ssh "$H" "rm -f $MARKS"
echo "$(date '+%F %T') all $N files match"
