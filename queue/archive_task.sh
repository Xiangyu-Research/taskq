#!/bin/bash
# box 上训完拉回的一个任务（scripts/taskq/queue/relay.py 拉回、改名后排的 relay_archive）备份到备份机器（表里角色 backup 的，
# 11/3 起是 dicc1 的 backup 目录，在 DICC 的 /scr 上），再核对（2026-10-18）。
#   archive_task.sh <hub 的名字> <key> [--dry-run]
# 结果文件夹、一起备份的文件按任务说明（scripts/taskq/queue/spec.py lookup <key>）：out、fetch 的 extra（上一级里一起备份的文件）。
# 步骤：
#   1. box → 备份机器：out、上一级里的 extra、分段日志（<数据目录>/logs/tasks/<key 的文件名>_*；以前放在别处的按说明的 oldlogs）。
#      备份机器上和 box 不同的文件按 box 的覆盖（11/3：如 11/2 稀疏化过的概率图），不删备份机器上多出的文件。
#   2. 核对：box 上的每个文件备份机器上都有、大小和修改时间一致（刚用 rsync 传过，传输时已按校验和核对；DICC 登录节点上不做大量读盘）。
# 不一致时以非 0 退出（健康检查会提醒）。
set -euo pipefail
HOST=${1:?用法：archive_task.sh <机器> <key> [--dry-run]} KEY=${2:?用法：archive_task.sh <机器> <key> [--dry-run]}
DRY=${3:-}
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # 调度代码的根；使用者的仓库根 REPO（TASKQ_REPO，没设时同 cfg.py）
REPO=${TASKQ_REPO:-$CORE}; [[ -z ${TASKQ_REPO:-} && $CORE == */scripts/taskq ]] && REPO=${CORE%/scripts/taskq}
M="python3 ${TASKQ_CONFIG:-$REPO/envs}/machines.py"
source "$CORE/queue/legacy_names.sh" map
[[ $HOST == "$($M names hub | head -1)" ]] || { echo "只归档 box 上的任务（$HOST 不是 hub）" >&2; exit 2; }
BK=$($M backups | grep -vx "$HOST" | head -1)
[[ -n $BK ]] || { echo "envs/machines.py 里没有 $HOST 以外的备份机器（角色 backup）" >&2; exit 2; }
BKH=$($M get "$BK" ssh) BKD=$($M get "$BK" backup 2> /dev/null || $M get "$BK" data) LD=${TASKQ_DATA:-$($M get . data)}
SPEC=$(python3 "$CORE/queue/spec.py" lookup "$KEY") || { echo "找不到任务 $KEY（实验表、训练队列里都没有）" >&2; exit 2; }
sp() { python3 -c '
import json, re, shlex, sys
sys.path.insert(0, sys.argv[2])
import spec as S
s = json.loads(sys.stdin.read()); f = sys.argv[1]
if f == "out": print(s["out"])
elif f == "test": print(" && ".join("{ " + " || ".join("test -e " + shlex.quote(a) for a in d.split("|")) + "; }" for d in s["done"]))
elif f == "extra": print("\n".join((s.get("fetch") or {}).get(f, [])))
elif f == "safe": print(S.safe(s["key"]))
elif f == "archive": print("1" if s.get("archive") else "")
elif f in ("olddir", "oldprefix"):  # 以前放在别处的分段日志（make_registry.py seg_summary 也认）
    o = s.get("oldlogs") or {}
    if o.get("dir") and o.get("prefix"):
        print(o["dir"] if f == "olddir" else o["prefix"])
' "$1" "$CORE/queue" <<< "$SPEC"; }
OUT=$(sp out) TEST=$(sp test) SAFE=$(sp safe) OLDDIR=$(sp olddir) OLDSEG=$(sp oldprefix)
mapfile -t EXTRA < <(sp extra | grep . || true)
[[ -n $(sp archive) ]] || { echo "$KEY 的做法不归档（说明里没有 archive）" >&2; exit 2; }
PARENT=$(dirname "$OUT") BOX=$LD/$OUT
log() { echo "$(date '+%F %T') $*"; }
[[ -d $BOX ]] && (cd "$BOX" && eval "$TEST") || { log "box 上这个任务还没拉回完整：$BOX"; exit 1; }
RB=(python3 "$CORE/sync/tunnel.py" rsync -rlt --partial)
[[ $DRY == --dry-run ]] && RB+=(-n --itemize-changes)

log "1. 备份：$OUT（box → $BK）"
[[ $DRY == --dry-run ]] || ssh "$BKH" "mkdir -p $BKD/$OUT $BKD/logs/tasks${OLDSEG:+ $BKD/$OLDDIR}"
for x in ${EXTRA[@]+"${EXTRA[@]}"}; do
    compgen -G "$LD/$PARENT/$x" > /dev/null && "${RB[@]}" "$LD/$PARENT/"$x "$BKH:$BKD/$PARENT/"
done
"${RB[@]}" "$BOX/" "$BKH:$BKD/$OUT/"
compgen -G "$LD/logs/tasks/${SAFE}_*" > /dev/null && "${RB[@]}" "$LD/logs/tasks/${SAFE}_"* "$BKH:$BKD/logs/tasks/"
if [[ -n $OLDSEG && -d $LD/$OLDDIR ]]; then
    "${RB[@]}" --include="${OLDSEG}*" --exclude='*' "$LD/$OLDDIR/" "$BKH:$BKD/$OLDDIR/"
fi

log "2. 核对"
diffs() {  # 列出不一致的（要传的文件：拉 >、推 <；要建的目录或链接 c）；rsync 本身出错（如连不上）时整个脚本退出
    local out
    out=$(rsync "$@") || { log "核对时 rsync 出错（退出码 $?）" >&2; return 1; }
    grep -E '^(<|>|c)' <<< "$out" || true
}
bad_bk=$(diffs -rltn --itemize-changes "$BOX/" "$BKH:$BKD/$OUT/")
if [[ $DRY == --dry-run ]]; then
    log "（dry-run：$BK 还没备份，跳过 $BK 的核对）"
elif [[ -n $bad_bk ]]; then
    log "$BK 上的副本和 box 不一致："; echo "$bad_bk" | head -20; exit 1
fi
log "box 上的副本已备份到 $BK、核对一致"
