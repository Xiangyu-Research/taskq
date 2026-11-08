#!/bin/bash
# Install taskq: clone to ${TASKQ_HOME:-~/.local/share/taskq} and link ~/.local/bin/taskq to its bin/taskq. Repeating is fine:
# an existing install is updated (taskq self update, pulls origin/main).
#   bash install.sh
#   curl -fsSL https://raw.githubusercontent.com/Xiangyu-Research/taskq/main/install.sh | bash
# TASKQ_ORIGIN: clone source (default the GitHub repo).
set -euo pipefail
DEST=${TASKQ_HOME:-$HOME/.local/share/taskq}
ORIGIN=${TASKQ_ORIGIN:-git@github.com:Xiangyu-Research/taskq.git}
BIN=$HOME/.local/bin
if [[ -d $DEST/.git ]]; then
    python3 "$DEST/bin/taskq" self update || echo "install.sh: update failed, staying on the old version" >&2
else
    [[ ! -e $DEST ]] || { echo "install.sh: $DEST exists and is not a git checkout; move it away first" >&2; exit 1; }
    mkdir -p "$(dirname "$DEST")"
    for i in 1 2 3 4 5; do
        git clone --quiet "$ORIGIN" "$DEST" && break
        rm -rf "$DEST"
        [[ $i == 5 ]] && { echo "install.sh: clone of $ORIGIN failed (5 tries)" >&2; exit 1; }
        sleep $((i * 5))
    done
fi
mkdir -p "$BIN"
ln -sfn "$DEST/bin/taskq" "$BIN/taskq"
echo "taskq $(python3 "$BIN/taskq" self version) -> $BIN/taskq"
case ":$PATH:" in *":$BIN:"*) ;; *) echo "($BIN is not on PATH)" ;; esac
