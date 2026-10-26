#!/bin/bash
# 某台训练机（envs/machines.py 里的名字，如 lab）上训完、已经拉回 box 的一个任务：先备份到备份机器（表里角色 backup 的，现在是 DICC），
# 三份核对一致后删掉训练机上的大文件（2026-10-18，取代 archive_fold.sh；规则同 10/16 定的，lab 的盘几乎满了）。
#   archive_task.sh <机器> <key> [--dry-run]
# 拉回以后由 box 上的 scripts/queue/advance.py 放进 wait 组（和推理并行）：
#   q wait --label archive:t15men_f1 -- 'bash scripts/queue/archive_task.sh lab Dataset111_MENrq1/nnUNetTrainer_save10_n470/1'
# 结果文件夹、要删的文件按任务说明（scripts/queue/spec.py lookup <key>）：out、fetch（extra：上一级里一起备份的文件；
# exclude：没拉回 box 的文件，核对时跳过）、archive（drop 匹配、keep 不匹配的才删）。
# 步骤：
#   1. 训练机上这个任务的分段日志拉回 box；box → 备份机器：out、上一级里的 extra、分段日志。只新增，不覆盖备份机器上已有的文件。
#      分段日志：<数据目录>/logs/tasks/<key 的文件名>_*；10/18 前 nnU-Net 的在 nnUNet/results/logs/（Dataset101 的
#      <训练器>_fold<k>_*，其他的 <数据集>_<训练器>_fold<k>_*）。
#   2. 核对：训练机上 out 里的每个文件（exclude 的除外）box 上都有、内容一致（按校验和）；box 上的每个文件备份机器上都有、大小和
#      修改时间一致（刚用 rsync 传过，传输时已按校验和核对；DICC 登录节点上不做大量读盘）。
#   3. 都一致才删训练机上要删的文件，另写一个 ARCHIVED（训完的判断、登记表都认它）。没有要删的文件时只做第 1 步。
# 任何一步不一致都不删、以非 0 退出（健康检查会提醒）。删除记到 box 的 ~/archive_prune.log。
set -euo pipefail
HOST=${1:?用法：archive_task.sh <机器> <key> [--dry-run]} KEY=${2:?用法：archive_task.sh <机器> <key> [--dry-run]}
DRY=${3:-}
REPO=$(git -C "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" rev-parse --show-toplevel)
M="python3 $REPO/envs/machines.py"
SRCH=$($M get "$HOST" ssh) SRCD=$($M get "$HOST" data)
[[ -n $SRCH ]] || { echo "envs/machines.py 里没有可以归档的机器 $HOST" >&2; exit 2; }
BK=$($M names backup | grep -vx "$HOST" | head -1)
[[ -n $BK ]] || { echo "envs/machines.py 里没有 $HOST 以外的备份机器（角色 backup）" >&2; exit 2; }
BKH=$($M get "$BK" ssh) BKD=$($M get "$BK" data) LD=${BRAIN_TUMOR:-$($M get . data)}
SPEC=$(python3 "$REPO/scripts/queue/spec.py" lookup "$KEY") || { echo "找不到任务 $KEY（实验表、训练队列里都没有）" >&2; exit 2; }
sp() { python3 -c '
import json, re, shlex, sys
sys.path.insert(0, sys.argv[2])
import spec as S
s = json.loads(sys.stdin.read()); f = sys.argv[1]
if f == "out": print(s["out"])
elif f == "test": print(" && ".join("{ " + " || ".join("test -e " + shlex.quote(a) for a in d.split("|")) + "; }" for d in s["done"]))
elif f in ("exclude", "extra"): print("\n".join((s.get("fetch") or {}).get(f, [])))
elif f == "safe": print(S.safe(s["key"]))
elif f == "archive": print("1" if s.get("archive") else "")
elif f == "oldseg":  # 10/18 前 nnU-Net 分段日志的文件名前缀（make_registry.py seg_summary）
    n = s.get("names") or {}
    if s.get("recipe") == "nnunet":
        ds, tr, k = n["dataset"], n["trainer"], n["fold"]
        print(f"{tr}_fold{k}_" if ds == "Dataset101_GLIrq1" else f"{ds}_{tr}_fold{k}_")
elif f == "drop": print("\n".join(S.drop_list(s, [ln for ln in open(sys.argv[3]).read().splitlines() if ln])))
' "$1" "$REPO/scripts/queue" "${2:-}" <<< "$SPEC"; }
OUT=$(sp out) TEST=$(sp test) SAFE=$(sp safe) OLDSEG=$(sp oldseg)
mapfile -t EXCL < <(sp exclude | grep . || true)
mapfile -t EXTRA < <(sp extra | grep . || true)
[[ -n $(sp archive) ]] || { echo "$KEY 的做法不归档（说明里没有 archive）" >&2; exit 2; }
PARENT=$(dirname "$OUT") BOX=$LD/$OUT
log() { echo "$(date '+%F %T') $*"; }
[[ -d $BOX ]] && (cd "$BOX" && eval "$TEST") || { log "box 上这个任务还没拉回完整：$BOX"; exit 1; }
R=(python3 "$REPO/scripts/sync/tunnel.py" rsync -rlt --partial --ignore-existing)
[[ $DRY == --dry-run ]] && R+=(-n --itemize-changes)
EX=()
for x in ${EXCL[@]+"${EXCL[@]}"}; do EX+=(--exclude="$x"); done

log "1. 备份：$OUT（$HOST → box 的分段日志；box → $BK）"
mkdir -p "$LD/logs/tasks"
if ssh "$SRCH" "test -d $SRCD/logs/tasks"; then  # 那边可能还没有这个目录
    "${R[@]}" --include="${SAFE}_*" --exclude='*' "$SRCH:$SRCD/logs/tasks/" "$LD/logs/tasks/"
fi
if [[ -n $OLDSEG ]] && ssh "$SRCH" "test -d $SRCD/nnUNet/results/logs"; then
    mkdir -p "$LD/nnUNet/results/logs"
    "${R[@]}" --include="${OLDSEG}*" --exclude='*' "$SRCH:$SRCD/nnUNet/results/logs/" "$LD/nnUNet/results/logs/"
fi
[[ $DRY == --dry-run ]] || ssh "$BKH" "mkdir -p $BKD/$OUT $BKD/logs/tasks $BKD/nnUNet/results/logs"
for x in ${EXTRA[@]+"${EXTRA[@]}"}; do
    compgen -G "$LD/$PARENT/$x" > /dev/null && "${R[@]}" "$LD/$PARENT/"$x "$BKH:$BKD/$PARENT/"
done
"${R[@]}" "$BOX/" "$BKH:$BKD/$OUT/"
compgen -G "$LD/logs/tasks/${SAFE}_*" > /dev/null && "${R[@]}" "$LD/logs/tasks/${SAFE}_"* "$BKH:$BKD/logs/tasks/"
if [[ -n $OLDSEG && -d $LD/nnUNet/results/logs ]]; then
    "${R[@]}" --include="${OLDSEG}*" --exclude='*' "$LD/nnUNet/results/logs/" "$BKH:$BKD/nnUNet/results/logs/"
fi

log "2. 核对"
diffs() {  # 列出不一致的（要传的文件 >、要建的目录或链接 c）；rsync 本身出错（如连不上）时整个脚本退出，不删任何东西
    local out
    out=$(rsync "$@") || { log "核对时 rsync 出错（退出码 $?），不删 $HOST 上的文件" >&2; return 1; }
    grep -E '^(>|c)' <<< "$out" || true
}
bad_bk=$(diffs -rltn --itemize-changes "$BOX/" "$BKH:$BKD/$OUT/")
if [[ $DRY == --dry-run ]]; then
    log "（dry-run：$BK 还没备份，跳过 $BK 的核对）"
elif [[ -n $bad_bk ]]; then
    log "$BK 上的副本和 box 不一致，不删 $HOST 上的文件："; echo "$bad_bk" | head -20; exit 1
fi
LIST=$(mktemp); trap 'rm -f "$LIST"' EXIT
ssh "$SRCH" "cd $SRCD/$OUT && find . -type f -printf '%P\n'" > "$LIST"
drop=$(sp drop "$LIST")
if [[ -z $drop ]]; then
    log "$HOST 上没有要删的文件（已经删过或不在 $HOST 上），只做备份"; exit 0
fi
bad_src=$(diffs -rlcn --itemize-changes ${EX[@]+"${EX[@]}"} "$SRCH:$SRCD/$OUT/" "$BOX/")
[[ -z $bad_src ]] || { log "box 上的副本和 $HOST 不一致（按校验和），不删 $HOST 上的文件："; echo "$bad_src" | head -20; exit 1; }
if [[ $DRY == --dry-run ]]; then log "box 的副本和 $HOST 一致"; else log "box、$BK 的副本都和 $HOST 一致"; fi

log "3. 删 $HOST 上的大文件"
n=$(grep -c . <<< "$drop" || true)
gb=$(ssh "$SRCH" "cd $SRCD/$OUT && xargs -d '\n' du -cb 2> /dev/null | tail -1 | cut -f1" <<< "$drop" | awk '{printf "%.1f", $1 / 1e9}')
if [[ $DRY == --dry-run ]]; then
    log "（dry-run）会删 $n 个文件，$gb GB：$(head -3 <<< "$drop" | tr '\n' ' ')…"; exit 0
fi
ssh "$SRCH" "cd $SRCD/$OUT && xargs -d '\n' rm -f -- &&
    echo '$(date '+%F %T') 存档和验证集预测已删（box、$BK 有副本，scripts/queue/archive_task.sh）' > ARCHIVED" <<< "$drop"
log "已删 $n 个文件，$gb GB"
echo "$(date '+%F %T') 删 $HOST $OUT：$n 个文件 $gb GB（box、$BK 已核对）" >> ~/archive_prune.log
