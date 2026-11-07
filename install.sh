#!/bin/bash
# 装 taskq：克隆到 ${TASKQ_HOME:-~/.local/share/taskq}，~/.local/bin/taskq 链到它的 bin/taskq。重复运行没关系：
# 已经装了就更新（taskq self update，拉 origin/main）。
#   bash install.sh
#   curl -fsSL https://raw.githubusercontent.com/Xiangyu-Research/taskq/main/install.sh | bash
# TASKQ_ORIGIN：克隆的来源（默认 GitHub 上的仓库）。
set -euo pipefail
DEST=${TASKQ_HOME:-$HOME/.local/share/taskq}
ORIGIN=${TASKQ_ORIGIN:-git@github.com:Xiangyu-Research/taskq.git}
BIN=$HOME/.local/bin
if [[ -d $DEST/.git ]]; then
    python3 "$DEST/bin/taskq" self update || echo "install.sh：更新失败，留在原来的版本" >&2
else
    [[ ! -e $DEST ]] || { echo "install.sh：$DEST 已经有了、又不是 git 检出，先挪走" >&2; exit 1; }
    mkdir -p "$(dirname "$DEST")"
    for i in 1 2 3 4 5; do
        git clone --quiet "$ORIGIN" "$DEST" && break
        rm -rf "$DEST"
        [[ $i == 5 ]] && { echo "install.sh：克隆 $ORIGIN 失败（试了 5 次）" >&2; exit 1; }
        sleep $((i * 5))
    done
fi
mkdir -p "$BIN"
ln -sfn "$DEST/bin/taskq" "$BIN/taskq"
echo "taskq $(python3 "$BIN/taskq" self version) → $BIN/taskq"
case ":$PATH:" in *":$BIN:"*) ;; *) echo "（$BIN 不在 PATH 里）" ;; esac
