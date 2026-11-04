#!/bin/bash
# 拉取 <仓库> 的 origin/<分支>，连同它钉的子模块提交（11/4）。两样都拿到才让 origin/<分支> 前进，
# 否则退回原来的，退出码 1：打包、建副本都按 origin/<分支> 取文件，不能指到拿不到的子模块提交。
# 每次连接试 5 次（GitHub 时好时坏）。还没初始化的子模块不在这里拿，由之后的 submodule update --init 拿。
#   fetch.sh <仓库> [分支，默认 main]
set -u
R=${1:?用法：fetch.sh <仓库> [分支]} B=${2:-main}
try() { local i; for i in 1 2 3 4 5; do "$@" && return 0; sleep $((i * 5)); done; return 1; }
ref=refs/remotes/origin/$B
old=$(git -C "$R" rev-parse -q --verify "$ref")
try git -C "$R" fetch -q --no-recurse-submodules origin || { echo "fetch.sh：拉取失败"; exit 1; }
git -C "$R" ls-tree -r "$ref" | while read -r mode _ sha path; do
    [[ $mode == 160000 && -e $R/$path/.git ]] || continue
    git -C "$R/$path" cat-file -e "$sha^{commit}" 2> /dev/null && continue
    try git -C "$R/$path" fetch -q origin
    git -C "$R/$path" cat-file -e "$sha^{commit}" 2> /dev/null || { echo "fetch.sh：子模块 $path 拿不到 ${sha:0:7}"; exit 1; }
done || { [[ -n $old ]] && git -C "$R" update-ref "$ref" "$old"; exit 1; }
