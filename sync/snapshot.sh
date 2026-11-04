#!/bin/bash
# 按提交号的代码副本（2026-10-25）：任务从这里跑，代码更新不会改到正在跑的任务。
#   snapshot.sh <克隆>
# 1. 克隆拉取一次（fetch.sh：连同子模块钉的提交），取 origin/main；状态文件夹的 code/<提交号>（cfg.py state code）没有就用 git worktree
#    建一份（git rev-parse、runlog 照常能用）；子模块在副本里用克隆里那个子模块的 worktree（不连网）
# 2. code/current 指到它（一步换过去）
# 3. 删旧副本：不是 current、没有 pueue 任务的工作目录在里面（排着、失败的也算：restart --in-place 要用）、建了 7 天以上
# fetch 失败（GitHub 连不上）时照样用克隆里已有的 origin/main，退出码 1。
set -u
SRC=${1:?用法：snapshot.sh <克隆>}
HERE=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
C=$(python3 "$HERE/../queue/cfg.py" state code) || exit 1
mkdir -p "$C"
exec 8> "$C/.lock"
flock -w 300 8 || { echo "snapshot.sh：等锁超时"; exit 1; }
rc=0
bash "$HERE/fetch.sh" "$SRC" || { echo "snapshot.sh：拉取失败，用克隆里已有的 origin/main"; rc=1; }
sha=$(git -C "$SRC" rev-parse origin/main) || exit 1
links=$(git -C "$SRC" ls-tree -r "$sha" | awk '$1 == "160000" {print $3, $4}')  # 子模块：提交号 路径
each() { local s p; while read -r s p; do [[ -z $p ]] || "$@" "$s" "$p" || return 1; done <<< "$links"; }
has() { [[ -e $C/$sha/$2/.git ]]; }
sub_add() { git -C "$SRC/$2" worktree add -q --detach "$C/$sha/$2" "$1"; }
sub_prune() { git -C "$SRC/$2" worktree prune; }
if [[ ! -d $C/$sha ]] || ! each has; then
    for d in "$C/$sha.tmp" "$C/$sha"; do git -C "$SRC" worktree remove --force "$d" 2> /dev/null; rm -rf "$d"; done
    git -C "$SRC" worktree prune
    each sub_prune
    git -C "$SRC" worktree add -q --detach "$C/$sha.tmp" "$sha" && git -C "$SRC" worktree move "$C/$sha.tmp" "$C/$sha" && each sub_add || {
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
each sub_prune
exit $rc
