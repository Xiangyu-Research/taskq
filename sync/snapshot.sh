#!/bin/bash
# 按提交号的代码副本（2026-10-25）：任务从这里跑，代码更新不会改到正在跑的任务。
#   snapshot.sh <克隆>
# 1. 克隆 fetch 一次，取 origin/main；~/.cache/phd/code/<提交号> 没有就用 git worktree 建一份（git rev-parse、runlog 照常能用）
# 2. ~/.cache/phd/code/current 指到它（一步换过去）
# 3. 删旧副本：不是 current、没有 pueue 任务的工作目录在里面（排着、失败的也算：restart --in-place 要用）、建了 7 天以上
# fetch 失败（GitHub 连不上）时照样用克隆里已有的 origin/main，退出码 1。
set -u
SRC=${1:?用法：snapshot.sh <克隆>}
C=${TASKQ_STATE:+$TASKQ_STATE/code}
C=${C:-~/.cache/phd/code}
mkdir -p "$C"
exec 8> "$C/.lock"
flock -w 300 8 || { echo "snapshot.sh：等锁超时"; exit 1; }
rc=0
git -C "$SRC" fetch -q origin || { echo "snapshot.sh：拉取失败，用克隆里已有的 origin/main"; rc=1; }
sha=$(git -C "$SRC" rev-parse origin/main) || exit 1
if [[ ! -d $C/$sha ]]; then
    git -C "$SRC" worktree remove --force "$C/$sha.tmp" 2> /dev/null
    rm -rf "$C/$sha.tmp"
    git -C "$SRC" worktree prune
    git -C "$SRC" worktree add -q --detach "$C/$sha.tmp" "$sha" && git -C "$SRC" worktree move "$C/$sha.tmp" "$C/$sha" || {
        echo "snapshot.sh：建副本 $sha 失败"; exit 1; }
fi
if [[ $(readlink "$C/current" 2> /dev/null) != "$sha" ]]; then
    ln -sfn "$sha" "$C/current.tmp" && mv -Tf "$C/current.tmp" "$C/current" || exit 1
fi
used=$(~/.local/bin/pueue status --json 2> /dev/null | python3 -c '
import json, sys
print("\n".join(t["path"] for t in json.load(sys.stdin)["tasks"].values()))') || exit $rc
for d in "$C"/*/; do
    d=${d%/} n=${d##*/}
    [[ $n == "$sha" || $n == *.tmp ]] && continue
    grep -q "^$d\(/\|$\)" <<< "$used" && continue
    [[ -n $(find "$d" -maxdepth 0 -mtime -7) ]] && continue
    git -C "$SRC" worktree remove --force "$d" && echo "snapshot.sh：删旧副本 ${n:0:7}"
done
exit $rc
