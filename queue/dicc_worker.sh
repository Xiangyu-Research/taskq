#!/bin/bash
# DICC 训练作业的任务队列 worker（2026-10-17）：作业开始后，envs/nnunet_paths.sh 在 ~/.cache/phd/taskq/ENABLED 存在时
# exec 到这里，不再训作业名对应的那一折，而是从 scripts/queue/taskq.py 的队列里按顺序领任务：同一张卡同时训
# 2 个（nnU-Net 类，并且作业有 ≥ 24 个 CPU、≥ 56G 内存；SwinUNETR、U-Mamba、STU-Net 1 个），训完一个再领下一个；
# 离时限不到 1 小时不再领新的；没有在训的、也领不到新的就退出。作业到时限被停掉时，没训完的任务交还队列，
# 下一个领到的从存档接着训（最多丢 10 个 epoch）。
#   bash dicc_worker.sh <本作业的训练器> <本作业的 fold> <本作业的数据集编号>     （由 nnunet_paths.sh 调用）
# 出问题时不让排队的作业空跑掉（10/17）：
#   - 本作业训练失败 2 次（或开训前就出错）就不再领新的，也不重领自己失败过的；
#   - 整个队列 3 小时内失败 3 次，taskq.py 暂停领任务：作业先空着等（健康检查提醒），免得后面排队的作业一个个
#     领不到任务就结束；
#   - 队列本身出错（taskq.py 读写不了）连续 10 分钟、手上又没有在训的：改回原来的做法，训本作业自己那一折
#     （已训完、或日志 15 分钟内更新过、或在队列里被领了的就不训，直接结束）；
#   - 查不到作业的结束时间时当作时间还多，之后每轮再查。
# 不管什么原因，手上没有在训的连续 40 分钟就结束作业、把卡还回去：DICC gpu-a100 分区 GPU 使用率连续 1 小时低于 40%
# 会被自动取消（10/17 的新规定）。正常训练没有这个问题（以前的记录里连续低于 40% 最长 19 分钟）。
# 每个任务的日志和 train_folds.sbatch 的分段日志同一格式（作业号 = 本作业的实际作业号，第 1 段），登记表照常认。
# WORKER_TEST_CMD 只给测试用：代替 python 训练命令（会在后面加上 "数据集编号 fold"）；TASKQ_* 环境变量也只给测试用。
set -uo pipefail
OWN_TRAINER=${1:?本作业的训练器} OWN_FOLD=${2:-} OWN_DS=${3:-101}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(dirname "$(dirname "$HERE")")
TQPY="python3 $HERE/taskq.py"
TQDIR=${TASKQ_DIR:-$HOME/.cache/phd/taskq}
W=${SLURM_JOB_ID:?只在 Slurm 作业里用}
case ${OWN_TRAINER,,} in
    *swin*) KIND=swin ;; *umamba*) KIND=umamba ;; *stunet*) KIND=stunet ;; *) KIND=nnunet ;;
esac
NCPU=$(python3 -c "import os; print(len(os.sched_getaffinity(0)))" 2> /dev/null || nproc)
MEMMB=${SLURM_MEM_PER_NODE:-$(( ${SLURM_MEM_PER_CPU:-0} * NCPU ))}
if [[ $KIND == nnunet ]] && (( NCPU >= 24 )) && (( MEMMB == 0 || MEMMB >= 56 * 1024 )); then
    SLOTS=${TASKQ_SLOTS:-2}
else
    SLOTS=${TASKQ_SLOTS:-1}
fi
MIN_LEFT=${TASKQ_MIN_LEFT:-3600}
PATIENCE=${TASKQ_PATIENCE:-1200}        # 开始后最多等这么久，给排在前面、刚停下的折留位置
ERR_FALLBACK=${TASKQ_ERR_FALLBACK:-600} # 队列连续出错这么久就改回训自己那一折
IDLE_MAX=${TASKQ_IDLE_MAX:-2400}        # 手上没有在训的连续这么久就结束作业（DICC：使用率连续 1 小时低于 40% 会被取消）
MAX_OWN_FAIL=2
DA=$(( NCPU / SLOTS - 2 )); (( DA > 30 )) && DA=30; (( DA < 1 )) && DA=1
PLANS=nnUNetResEncUNetMPlans
LOGDIR=$nnUNet_results/logs
mkdir -p "$LOGDIR"
COMMIT=$(git -C "$REPO" rev-parse --short HEAD 2> /dev/null || echo 未知)
git -C "$REPO" diff --quiet HEAD -- research/RQ1/nnunet scripts/queue 2> /dev/null || COMMIT="$COMMIT+改动"
ts() { date '+%F %T'; }
T0=$(date +%s)

# 作业的结束时间：查不到（Slurm 偶尔超时）就记 0，当作时间还多，之后每轮再查
END=0
get_end() {
    local id e s
    for id in "$W" ${SLURM_ARRAY_JOB_ID:+"${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID:-}"}; do
        e=$(squeue -j "$id" -h -o %e 2> /dev/null | head -1)
        [[ $e =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$ ]] || continue
        s=$(date -d "$e" +%s 2> /dev/null) || continue
        (( s > T0 )) && { END=$s; return 0; }
    done
    return 1
}
get_end
left() { (( END == 0 )) && echo 999999 || echo $(( END - $(date +%s) )); }
echo "$(ts) 任务队列 worker：作业 $W @ $(hostname -s)，种类 $KIND，同时 $SLOTS 个，CPU $NCPU，内存 ${MEMMB}M（每个训练 $DA 个数据增强进程），" \
     "时限 $( (( END )) && date -d @"$END" '+%F %T' || echo 查不到，之后再查)，代码 $COMMIT"
nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader 2> /dev/null

GPUCSV=$LOGDIR/worker_${W}_gpu.csv
( while true; do nvidia-smi --query-gpu=timestamp,index,memory.used,memory.total,utilization.gpu --format=csv,noheader >> "$GPUCSV" 2> /dev/null; sleep 60; done ) &
GPUMON=$!

declare -A KEY=() FD=()   # 训练的 pid → key、这一折的结果目录
EXCL=()                   # 本作业失败过的 key，不再领
OWN_FAILS=0
FB_PID=                   # 改回训自己那一折时的训练 pid
run_one() {  # 数据集编号 训练器 fold 日志
    local dsid=$1 tr=$2 fold=$3 log=$4 rc
    export nnUNet_n_proc_DA=$DA PYTHONUNBUFFERED=1
    if [[ -n ${WORKER_TEST_CMD:-} ]]; then
        $WORKER_TEST_CMD "$dsid" "$fold" >> "$log" 2>&1
        return
    fi
    train() { python -c "from nnunetv2.run.run_training import run_training_entry; run_training_entry()" \
                  "$dsid" 3d_fullres "$fold" -p $PLANS -tr "$tr" -num_gpus 1 --npz --c >> "$log" 2>&1; }
    train; rc=$?
    # torch.compile 出错时关掉编译重跑一次（同 train_folds.sbatch）
    if (( rc != 0 )) && [[ ${nnUNet_compile:-} != f ]] && tail -n 200 "$log" | grep -E "_dynamo|_inductor|BackendCompilerFailed|CompilationError" > /dev/null; then
        echo "===== 关掉 torch.compile 重跑 =====" >> "$log"
        nnUNet_compile=f train; rc=$?
    fi
    return $rc
}
fail() {  # key 原因：交还并记一次失败；本作业不再领这个 key
    $TQPY release --worker "$W" "$1" --failed "$2"
    EXCL+=("$1")
    OWN_FAILS=$(( OWN_FAILS + 1 ))
    (( OWN_FAILS >= MAX_OWN_FAIL )) && echo "$(ts) 本作业已失败 $OWN_FAILS 次，不再领新任务（多半是这个节点或环境有问题）"
}
start() {  # key 数据集编号 训练器 fold
    local key=$1 dsid=$2 tr=$3 fold=$4 dsdir dsname log
    dsdir=$(ls -d "$nnUNet_preprocessed/Dataset${dsid}_"* 2> /dev/null | head -1)
    if [[ -z $dsdir ]]; then
        echo "$(ts) $key：找不到预处理好的 Dataset${dsid}（$nnUNet_preprocessed），交还"
        fail "$key" "没有预处理好的数据集"
        return 1
    fi
    dsname=$(basename "$dsdir")
    log=$LOGDIR/${tr}_fold${fold}_${W}_seg1.log
    [[ $dsid == 101 ]] || log=$LOGDIR/${dsname}_${tr}_fold${fold}_${W}_seg1.log
    echo "作业 ${W}_1 @ $(hostname)，数据集=$dsname，训练器=$tr，fold=$fold，第 1 段（任务队列），代码 $COMMIT，$(ts)" >> "$log"
    run_one "$dsid" "$tr" "$fold" "$log" &
    KEY[$!]=$key
    FD[$!]=$nnUNet_results/$dsname/${tr}__${PLANS}__3d_fullres/fold_$fold
    echo "$(ts) 开训 $key（pid $!，日志 $(basename "$log")）"
}
# 队列出错时改回原来的做法：训本作业自己那一折（和 train_folds.sbatch 一样的日志名）
fallback_own() {
    local dsdir dsname fd log jid=${SLURM_ARRAY_JOB_ID:-$W} seg=${SLURM_ARRAY_TASK_ID:-1}
    [[ -n $OWN_FOLD ]] || { echo "$(ts) 不知道本作业自己的 fold，结束"; return 1; }
    dsdir=$(ls -d "$nnUNet_preprocessed/Dataset${OWN_DS}_"* 2> /dev/null | head -1)
    [[ -n $dsdir ]] || { echo "$(ts) 找不到预处理好的 Dataset${OWN_DS}，结束"; return 1; }
    dsname=$(basename "$dsdir")
    fd=$nnUNet_results/$dsname/${OWN_TRAINER}__${PLANS}__3d_fullres/fold_$OWN_FOLD
    if [[ ( -f $fd/checkpoint_final.pth || -f $fd/ARCHIVED ) && -f $fd/validation/summary.json ]]; then
        echo "$(ts) 自己那一折已训完，结束"; return 200
    fi
    if [[ -n $(find "$fd" -maxdepth 1 -name 'training_log_*.txt' -mmin -15 2> /dev/null) ]] || [[ -e $TQDIR/claims/${dsname}__${OWN_TRAINER}__${OWN_FOLD} ]]; then
        echo "$(ts) 自己那一折正在别处训（日志 15 分钟内更新过，或在队列里被领了），结束"; return 200
    fi
    log=$LOGDIR/${OWN_TRAINER}_fold${OWN_FOLD}_${jid}_seg${seg}.log
    [[ $OWN_DS == 101 ]] || log=$LOGDIR/${dsname}_${OWN_TRAINER}_fold${OWN_FOLD}_${jid}_seg${seg}.log
    echo "作业 ${jid}_${seg} @ $(hostname)，数据集=$dsname，训练器=$OWN_TRAINER，fold=$OWN_FOLD，第 $seg 段（队列出错，训自己这一折），代码 $COMMIT，$(ts)" >> "$log"
    DA=$(( NCPU - 2 )); (( DA > 30 )) && DA=30; (( DA < 1 )) && DA=1
    echo "$(ts) 队列连续出错，改回原来的做法：训自己那一折 $dsname/$OWN_TRAINER/$OWN_FOLD（日志 $(basename "$log")）"
    run_one "$OWN_DS" "$OWN_TRAINER" "$OWN_FOLD" "$log" &
    FB_PID=$!
    wait "$FB_PID"
}
kill_tree() { local c; for c in $(pgrep -P "$1"); do kill_tree "$c"; done; kill -TERM "$1" 2> /dev/null; }
on_term() {
    echo "$(ts) 收到停止信号（多半是到时限），停训练、交还任务"
    local p
    for p in "${!KEY[@]}" $FB_PID; do kill_tree "$p"; done
    for _ in $(seq 15); do
        local alive=0
        for p in "${!KEY[@]}" $FB_PID; do kill -0 "$p" 2> /dev/null && alive=1; done
        (( alive )) || break
        sleep 1
    done
    (( ${#KEY[@]} )) && $TQPY release --worker "$W" "${KEY[@]}"
    kill_tree "$GPUMON"
    exit 143
}
trap on_term TERM INT

last_beat=$(date +%s)
ERRF=$(mktemp)
err_since=0   # 队列从什么时候开始连续出错
idle_since=$T0  # 从什么时候开始手上没有在训的（0 = 有在训的）
while true; do
    # 收训完的
    for p in "${!KEY[@]}"; do
        if ! kill -0 "$p" 2> /dev/null; then
            wait "$p"; rc=$?
            key=${KEY[$p]} fd=${FD[$p]}; unset "KEY[$p]" "FD[$p]"
            if (( rc == 0 )) && ! [[ ( -f $fd/checkpoint_final.pth || -f $fd/ARCHIVED ) && -f $fd/validation/summary.json ]]; then
                echo "$(ts) $key 训练正常退出，但没有训完的文件，交还并记一次失败"
                fail "$key" "退出码 0 但没训完"
            elif (( rc == 0 )); then
                echo "$(ts) 训完 $key"
                $TQPY release --worker "$W" "$key"
            else
                echo "$(ts) $key 训练失败（退出码 $rc），交还并记一次失败"
                fail "$key" "退出码 $rc"
            fi
        fi
    done
    now=$(date +%s)
    (( END )) || get_end
    if (( now - last_beat >= 300 && ${#KEY[@]} > 0 )); then
        $TQPY beat --worker "$W" "${KEY[@]}"
        last_beat=$now
    fi
    # 领新的
    why=
    while (( ${#KEY[@]} < SLOTS && $(left) > MIN_LEFT && OWN_FAILS < MAX_OWN_FAIL )); do
        t=$($TQPY claim --worker "$W" --kind "$KIND" --free-slots $(( SLOTS - ${#KEY[@]} )) \
                        --patience-until $(( T0 + PATIENCE )) --exclude ${EXCL[@]+"${EXCL[@]}"} 2> "$ERRF")
        rc=$?
        case $rc in
            0)  err_since=0; start $t ;;
            10) err_since=0; why=nothing; break ;;
            11) err_since=0; why=wait; break ;;
            12) err_since=0; why=paused; break ;;
            *)  (( err_since )) || { err_since=$now; echo "$(ts) 队列出错（taskq.py 退出码 $rc：$(tail -n 1 "$ERRF")），之后每轮再试"; }
                why=error; break ;;
        esac
    done
    if (( ${#KEY[@]} > 0 )); then
        idle_since=0
    else
        (( idle_since )) || idle_since=$now
        if (( OWN_FAILS >= MAX_OWN_FAIL )); then
            echo "$(ts) 本作业失败太多次，作业结束"; break
        elif (( $(left) <= MIN_LEFT )); then
            echo "$(ts) 离时限不到 $(( MIN_LEFT / 60 )) 分钟，作业结束"; break
        elif [[ $why == nothing ]]; then
            echo "$(ts) 没有在训的，也没有能领的任务，作业结束"; break
        elif [[ $why == error ]] && (( now - err_since >= ERR_FALLBACK )); then
            fallback_own; rc=$?
            (( rc == 200 )) && rc=0  # 自己那一折不用训，和 train_folds.sbatch 一样正常结束
            kill_tree "$GPUMON"
            echo "$(ts) 作业结束（退出码 $rc）"
            exit $rc
        elif (( now - idle_since >= IDLE_MAX )); then
            echo "$(ts) 手上没有在训的已经 $(( (now - idle_since) / 60 )) 分钟（$why），结束作业、把卡还回去（DICC：GPU 使用率连续 1 小时低于 40% 会被取消）"
            break
        fi
        # wait（等前面刚停下的折）、paused（队列暂停）、error（还没到改回的时候）：先空着等，最多 IDLE_MAX
        [[ $why == paused && ${said_paused:-} != 1 ]] && { echo "$(ts) 队列暂停领任务（3 小时内失败太多次），空着等，最多 $(( IDLE_MAX / 60 )) 分钟"; said_paused=1; }
        [[ $why == wait && ${said_wait:-} != 1 ]] && { echo "$(ts) 排在前面的折刚停下，等它（最多到开始后 $(( PATIENCE / 60 )) 分钟）"; said_wait=1; }
    fi
    sleep "${TASKQ_POLL:-30}" & wait $!
done
kill_tree "$GPUMON"
exit 0
