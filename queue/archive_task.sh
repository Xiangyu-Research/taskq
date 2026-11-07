#!/bin/bash
# store 上拉回的一个任务备份到备份机器（表里角色 backup 的，有 backup 字段时备份到那个目录，没有时到它的数据目录），再核对。
# 在 store 上跑（queue/site_ops.py 的 archive；hub 的 relay 在 IO 组里排 relay_archive），数据目录是 TASKQ_DATA。
#   archive_task.sh --spec <解析好的说明 JSON> [--dry-run]
# 结果文件夹、一起备份的文件按任务的输出（queue/spec.py outputs --spec）：outs、上一级里的 extra、分段日志。
# 步骤：
#   1. store → 备份机器：outs、上一级里的 extra、分段日志（<数据目录>/logs/tasks/<key 的文件名>__*）。
#      备份机器上和 store 不同的文件按 store 的覆盖（如 store 上压缩过的结果），不删备份机器上多出的文件。
#   2. 核对：store 上的每个文件备份机器上都有、大小和修改时间一致（刚用 rsync 传过，传输时已按校验和核对；不在备份机器的登录节点上大量读盘）。
# 不一致时以非 0 退出（健康检查会提醒）。
set -euo pipefail
USAGE="用法：archive_task.sh --spec <说明 JSON> [--dry-run]"
[[ ${1:-} == --spec && -n ${2:-} ]] || { echo "$USAGE" >&2; exit 2; }
SPEC=$2 DRY=${3:-}
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # 调度代码的根
M="python3 $CORE/sites/mtab.py"
HOST=$($M names store | head -1)
LD=${TASKQ_DATA:?在 store 上跑：TASKQ_DATA 是 store 的数据目录}
BK=$($M backups | grep -vx "$HOST" | head -1)
[[ -n $BK ]] || { echo "机器表（machines.py）里没有 $HOST 以外的备份机器（角色 backup）" >&2; exit 2; }
BKH=$($M get "$BK" ssh) BKD=$($M get "$BK" backup 2> /dev/null || $M get "$BK" data)
SH=$(python3 "$CORE/queue/spec.py" outputs --spec "$SPEC" --sh) || { echo "说明不对：${SPEC:0:200}" >&2; exit 2; }
eval "$SH"
(( ARCHIVE )) || { echo "$KEY 的做法不归档（说明里没有 archive）" >&2; exit 2; }
OUT=${OUTS[0]}
PARENT=$(dirname "$OUT") SRC=$LD/$OUT
log() { echo "$(date '+%F %T') $*"; }
[[ -d $SRC ]] && (cd "$SRC" && eval "$TEST") || { log "$HOST 上这个任务还没拉回完整：$SRC"; exit 1; }
RB=(python3 "$CORE/sync/tunnel.py" rsync -rlt --partial)
[[ $DRY == --dry-run ]] && RB+=(-n --itemize-changes)

log "1. 备份：${OUTS[*]}（$HOST → $BK）"
[[ $DRY == --dry-run ]] || ssh "$BKH" "mkdir -p ${OUTS[*]/#/$BKD/} $BKD/logs/tasks"
for x in ${EXTRA[@]+"${EXTRA[@]}"}; do
    compgen -G "$LD/$PARENT/$x" > /dev/null && "${RB[@]}" "$LD/$PARENT/"$x "$BKH:$BKD/$PARENT/"
done
for o in "${OUTS[@]}"; do
    [[ -d $LD/$o ]] && "${RB[@]}" "$LD/$o/" "$BKH:$BKD/$o/"
done
SEGS=()
for f in "$LD/$LOGS"*; do  # 只要这个任务的：前缀后面再有 __ 的是 key 更长的别的任务
    [[ -e $f && ${f#"$LD/$LOGS"} != *__* ]] && SEGS+=("$f")
done
(( ${#SEGS[@]} )) && "${RB[@]}" "${SEGS[@]}" "$BKH:$BKD/logs/tasks/"

log "2. 核对"
diffs() {  # 列出不一致的（要传的文件：拉 >、推 <；要建的目录或链接 c）；rsync 本身出错（如连不上）时整个脚本退出
    local out
    out=$(rsync "$@") || { log "核对时 rsync 出错（退出码 $?）" >&2; return 1; }
    grep -E '^(<|>|c)' <<< "$out" || true
}
bad_bk=
for o in "${OUTS[@]}"; do
    [[ -d $LD/$o ]] && bad_bk+=$(diffs -rltn --itemize-changes "$LD/$o/" "$BKH:$BKD/$o/")
done
if [[ $DRY == --dry-run ]]; then
    log "（dry-run：$BK 还没备份，跳过 $BK 的核对）"
elif [[ -n $bad_bk ]]; then
    log "$BK 上的副本和 $HOST 不一致："; echo "$bad_bk" | head -20; exit 1
fi
log "$HOST 上的副本已备份到 $BK、核对一致"
