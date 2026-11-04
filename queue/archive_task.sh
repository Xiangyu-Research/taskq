#!/bin/bash
# box 上训完拉回的一个任务（queue/relay.py 拉回、改名后排的 relay_archive）备份到备份机器（表里角色 backup 的，
# 11/3 起是 dicc1 的 backup 目录，在 DICC 的 /scr 上），再核对（2026-10-18）。
#   archive_task.sh <hub 的名字> <key> [--dry-run]
# 结果文件夹、一起备份的文件按任务的输出（queue/spec.py outputs <key>）：outs、上一级里的 extra、分段日志。
# 步骤：
#   1. box → 备份机器：outs、上一级里的 extra、分段日志（<数据目录>/logs/tasks/<key 的文件名>__*；以前放在别处的按说明的 oldlogs）。
#      备份机器上和 box 不同的文件按 box 的覆盖（11/3：如 11/2 稀疏化过的概率图），不删备份机器上多出的文件。
#   2. 核对：box 上的每个文件备份机器上都有、大小和修改时间一致（刚用 rsync 传过，传输时已按校验和核对；DICC 登录节点上不做大量读盘）。
# 不一致时以非 0 退出（健康检查会提醒）。
set -euo pipefail
HOST=${1:?用法：archive_task.sh <机器> <key> [--dry-run]} KEY=${2:?用法：archive_task.sh <机器> <key> [--dry-run]}
DRY=${3:-}
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # 调度代码的根；使用者的仓库根 REPO（TASKQ_REPO，没设时同 cfg.py）
if [[ -n ${TASKQ_REPO:-} ]]; then REPO=$TASKQ_REPO; elif [[ $CORE == */scripts/taskq ]]; then REPO=${CORE%/scripts/taskq}; else eval "$(python3 "$CORE/queue/project.py" sh 2> /dev/null)"; REPO=${TASKQ_REPO:-$CORE}; fi
M="python3 $CORE/sites/mtab.py"
[[ $HOST == "$($M names hub | head -1)" ]] || { echo "只归档 box 上的任务（$HOST 不是 hub）" >&2; exit 2; }
BK=$($M backups | grep -vx "$HOST" | head -1)
[[ -n $BK ]] || { echo "envs/machines.py 里没有 $HOST 以外的备份机器（角色 backup）" >&2; exit 2; }
BKH=$($M get "$BK" ssh) BKD=$($M get "$BK" backup 2> /dev/null || $M get "$BK" data) LD=${TASKQ_DATA:-$($M get . data)}
SH=$(python3 "$CORE/queue/spec.py" outputs "$KEY" --sh) || { echo "找不到任务 $KEY（实验表、训练队列里都没有）" >&2; exit 2; }
eval "$SH"
(( ARCHIVE )) || { echo "$KEY 的做法不归档（说明里没有 archive）" >&2; exit 2; }
OUT=${OUTS[0]}
PARENT=$(dirname "$OUT") BOX=$LD/$OUT
log() { echo "$(date '+%F %T') $*"; }
[[ -d $BOX ]] && (cd "$BOX" && eval "$TEST") || { log "box 上这个任务还没拉回完整：$BOX"; exit 1; }
RB=(python3 "$CORE/sync/tunnel.py" rsync -rlt --partial)
[[ $DRY == --dry-run ]] && RB+=(-n --itemize-changes)

log "1. 备份：${OUTS[*]}（box → $BK）"
[[ $DRY == --dry-run ]] || ssh "$BKH" "mkdir -p ${OUTS[*]/#/$BKD/} $BKD/logs/tasks${OLDSEG:+ $BKD/$OLDDIR}"
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
if [[ -n $OLDSEG && -d $LD/$OLDDIR ]]; then
    "${RB[@]}" --include="${OLDSEG}*" --exclude='*' "$LD/$OLDDIR/" "$BKH:$BKD/$OLDDIR/"
fi

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
    log "$BK 上的副本和 box 不一致："; echo "$bad_bk" | head -20; exit 1
fi
log "box 上的副本已备份到 $BK、核对一致"
