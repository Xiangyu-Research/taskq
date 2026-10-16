#!/bin/bash
# pueue 里的训练任务（2026-10-16）：训一折（research/RQ1/nnunet/train_folds.sbatch，--c 从存档接着训），
# 卡被占时等着；给了 --epochs N 时训满 N 个 epoch 后，在下一次存盘（每 10 个 epoch 一次）后停。
#   train_task.sh <数据集编号> <fold> [训练器，默认 nnUNetTrainer_save10] [--epochs N]
# 用哪张卡由 CUDA_VISIBLE_DEVICES 决定（q 按组设好）。
# 退出码（scripts/queue/README.md 的"约定"）：0 训完（有 checkpoint_final.pth 和验证结果）；75 训满 N 个 epoch 停了、还没训完；
#   3 = 别的机器在训这一折，或 Slurm 上排着（同一折不在两处训，要在这里训先停掉那边的）；
#   其他 = 出错（训练连续出错 3 次才算，每次隔 5 分钟）。
# 别的机器在训的折由 scripts/monitor/health.py 每 30 分钟写到 ~/.cache/phd/claimed_elsewhere.txt；文件超过 2 小时没更新时照常训，只在日志里警告。
# 停这个任务用 pueue kill -s SIGTERM <任务号>：会先停训练（最多丢 10 个 epoch）再退出。
set -u
DS=${1:?用法：train_task.sh <数据集编号> <fold> [训练器] [--epochs N]} FOLD=${2:?} ; shift 2
TRAINER=nnUNetTrainer_save10
[[ ${1:-} && $1 != --* ]] && { TRAINER=$1; shift; }
N=
while (( $# )); do
    case $1 in
        --epochs) N=$2; shift 2 ;;
        *) echo "不认识的参数 $1" >&2; exit 2 ;;
    esac
done
REPO=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
source "$REPO/envs/nnunet_paths.sh"
# 只给 test_train_task.sh 用：假的 nvidia-smi 和训练脚本，结果写到临时目录
if [[ -n ${QUEUE_TEST_DIR:-} ]]; then export PATH=$QUEUE_TEST_DIR/bin:$PATH nnUNet_results=$QUEUE_TEST_DIR nnUNet_preprocessed=$QUEUE_TEST_DIR; fi
TRAIN=${QUEUE_TEST_TRAIN:-$REPO/research/RQ1/nnunet/train_folds.sbatch}
POLL=${QUEUE_TEST_POLL:-10}
GPU=${CUDA_VISIBLE_DEVICES:?要先设 CUDA_VISIBLE_DEVICES（用 q 加任务会自动设）}
export CUDA_DEVICE_ORDER=PCI_BUS_ID

log() { echo "$(date '+%F %T') $*"; }
gpu_free() {
    [[ -z $(nvidia-smi -i "$GPU" --query-compute-apps=pid --format=csv,noheader 2> /dev/null | tr -d ' ') ]] &&
        (( $(nvidia-smi -i "$GPU" --query-gpu=memory.used --format=csv,noheader,nounits | tr -d ' ') <= 500 ))
}
DIR=$(ls -d "$nnUNet_preprocessed/Dataset${DS}_"* 2> /dev/null | head -1)
[[ -n $DIR ]] || { echo "没有预处理好的数据集 Dataset${DS}_*" >&2; exit 1; }
OUT=$nnUNet_results/$(basename "$DIR")/${TRAINER}__nnUNetResEncUNetMPlans__3d_fullres/fold_$FOLD
CK=$OUT/checkpoint_latest.pth
done_() { [[ ( -f $OUT/checkpoint_final.pth || -f $OUT/ARCHIVED ) && -f $OUT/validation/summary.json ]]; }  # ARCHIVED：训完、大文件已归档（archive_fold.sh）
alive() { local st; st=$(ps -o stat= -p "$1" 2> /dev/null | tr -d ' '); [[ -n $st && $st != Z* ]]; }  # 僵尸不算
tree() { local c; for c in $(pgrep -P "$1"); do tree "$c"; echo "$c"; done; }  # 某进程的所有子孙
stop_train() {  # 停训练（train_folds.sbatch → python → 数据增强子进程），不停本脚本
    local p; p=$(tree "$1"; echo "$1")
    kill -TERM $p 2> /dev/null
    for _ in $(seq 30); do alive "$1" || break; sleep 1; done
    p=$(tree "$1"; echo "$1"); kill -KILL $p 2> /dev/null
    wait "$1" 2> /dev/null
}
last_epoch() {  # 本次启动后新写的训练日志里最后一个 epoch 编号
    local L
    L=$(find "$OUT" -maxdepth 1 -name 'training_log_*.txt' -newer "$MARK" -printf '%T@ %p\n' 2> /dev/null | sort -n | tail -1 | cut -d' ' -f2-)
    [[ -n $L ]] && grep -oE ": Epoch [0-9]+ *$" "$L" | tail -1 | grep -oE "[0-9]+"
}

CLAIMS=${QUEUE_CLAIMS:-$HOME/.cache/phd/claimed_elsewhere.txt}
KEY=$(basename "$DIR")/$TRAINER/$FOLD
elsewhere() {  # 别的机器在训这一折（或 Slurm 上排着）：输出那台机器的名字
    [[ -f $CLAIMS ]] || return 1
    if [[ -z $(find "$CLAIMS" -mmin -120) ]]; then log "警告：$CLAIMS 超过 2 小时没更新，不能确认别的机器上有没有这一折" >&2; return 1; fi
    awk -F'\t' -v k="$KEY" '$1 == k {print $2; f = 1} END {exit !f}' "$CLAIMS"
}
JOB="数据集 $DS fold $FOLD $TRAINER（卡 $GPU${N:+，训 $N 个 epoch}）"
done_ && { log "已经训完：$JOB"; exit 0; }
MARK=$(mktemp); trap 'rm -f "$MARK"' EXIT
pid=
trap '[[ -n $pid ]] && { log "收到停止信号，停训练"; stop_train $pid; }; exit 143' TERM INT
errors=0
while true; do
    if where=$(elsewhere); then log "$where 上在训或排着这一折（$KEY，见 $CLAIMS），不在这里训；要在这里训，先停掉那边的"; exit 3; fi
    if ! gpu_free; then
        log "卡 $GPU 被占，5 分钟后再看"; sleep 300 & wait $!; continue
    fi
    RQ1_DATASET=$DS bash "$TRAIN" "$FOLD" "$TRAINER" < /dev/null &
    pid=$!
    log "开始 / 接着训：$JOB（进程 $pid）"
    e0= t0=$(stat -c %Y "$CK" 2> /dev/null || echo 0)
    while alive $pid; do
        [[ -z $e0 ]] && e0=$(last_epoch)
        t1=$(stat -c %Y "$CK" 2> /dev/null || echo 0)
        if [[ -n $N && $t1 != "$t0" ]]; then  # 刚存完盘
            t0=$t1 e=$(last_epoch)
            if [[ -n $e0 && -n $e ]] && (( e - e0 >= N )); then
                sleep "${QUEUE_TEST_SAVE_WAIT:-20}"; stop_train $pid; log "存盘后停（训满 $N 个 epoch，第 $e0 → $e 个 epoch）"; exit 75
            fi
        fi
        sleep "$POLL" & wait $!
    done
    wait $pid; rc=$?; pid=
    done_ && { log "训完：$JOB"; exit 0; }
    errors=$((errors + 1))
    log "训练结束但没训完（退出码 $rc，第 $errors 次）"
    (( errors >= 3 )) && exit "$(( rc ? rc : 1 ))"
    sleep 300 & wait $!
done
