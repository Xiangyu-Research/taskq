#!/bin/bash
# DICC 训练作业的任务队列 worker（2026-10-17）：作业开始后，envs/nnunet_paths.sh 在 ~/.cache/phd/taskq/ENABLED 存在时
# exec 到这里，不再训作业名对应的那一折，而是从 scripts/queue/taskq.py 的队列里按顺序领任务，什么模型都领（10/18 起，
# 不分种类）。一张卡训几个看实际情况：先训 1 个；最近一个训练开始 15 分钟以后，如果近 10 分钟 GPU 平均使用率低于 80%、
# 除去显存填充还空着一半显存（当时用 nvidia-smi 读），再领 1 个（最多 2 个，并且作业要有 ≥ 24 个 CPU、≥ 56G 内存）。nnU-Net 这类
# CPU 跟不上的会变成 2 个，SwinUNETR、U-Mamba 这类 GPU 跑满的自然只训 1 个。训完一个再领下一个；
# 离时限不到 1 小时不再领新的；没有在训的、也领不到新的就退出。作业到时限被停掉时，没训完的任务交还队列，
# 下一个领到的从存档接着训（最多丢 10 个 epoch）。
# 开始每个训练前先通知显存填充（gpu_pad.py）全部放掉（touch <base>.start，等 <base>.mib 变成 0，最多 30 秒），
# 新训练启动时整张卡都能用。每个任务按训练器换环境（~/envs/swinunetr、~/envs/umamba，同 train_folds.sbatch）。
#   bash dicc_worker.sh <本作业的训练器> <本作业的 fold> <本作业的数据集编号>     （由 nnunet_paths.sh 调用，10/17 前交的作业）
#   bash dicc_worker.sh                                                             （不带折的作业 research/RQ1/nnunet/worker.sbatch）
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
OWN_TRAINER=${1:-} OWN_FOLD=${2:-} OWN_DS=${3:-101}  # 只在队列出错、改回训自己那一折时用
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(dirname "$(dirname "$HERE")")
TQPY="python3 $HERE/taskq.py"
TQDIR=${TASKQ_DIR:-$HOME/.cache/phd/taskq}
W=${SLURM_JOB_ID:?只在 Slurm 作业里用}
NCPU=$(python3 -c "import os; print(len(os.sched_getaffinity(0)))" 2> /dev/null || nproc)
MEMMB=${SLURM_MEM_PER_NODE:-$(( ${SLURM_MEM_PER_CPU:-0} * NCPU ))}
if (( NCPU >= 24 )) && (( MEMMB == 0 || MEMMB >= 56 * 1024 )); then
    SLOTS=${TASKQ_SLOTS:-2}   # 最多同时训几个
else
    SLOTS=${TASKQ_SLOTS:-1}
fi
ADD_AFTER=${TASKQ_ADD_AFTER:-900}       # 最近一个训练开始以后，过这么久才判断要不要再加一个
UTIL_MAX=${TASKQ_UTIL_MAX:-80}          # 近 10 分钟 GPU 平均使用率低于这个才加
PADBASE=${RQ1_GPU_PAD_BASE:-/tmp/gpu_pad_$W}  # 和 gpu_pad.py 交换信号的文件前缀
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
echo "$(ts) 任务队列 worker：作业 $W @ $(hostname -s)，最多同时 $SLOTS 个，CPU $NCPU，内存 ${MEMMB}M（每个训练 $DA 个数据增强进程），" \
     "时限 $( (( END )) && date -d @"$END" '+%F %T' || echo 查不到，之后再查)，代码 $COMMIT"
nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader 2> /dev/null

GPUCSV=$LOGDIR/worker_${W}_gpu.csv
( while true; do nvidia-smi --query-gpu=timestamp,index,memory.used,memory.total,utilization.gpu --format=csv,noheader >> "$GPUCSV" 2> /dev/null; sleep 60; done ) &
GPUMON=$!

declare -A KEY=() FD=()   # 训练的 pid → key、这一折的结果目录
CHANGED=$T0               # 最近一个训练开始的时刻
EXCL=()                   # 本作业失败过的 key，不再领
OWN_FAILS=0
FB_PID=                   # 改回训自己那一折时的训练 pid
pad_make_room() {  # 通知显存填充全部放掉，等它放完（没有填充时立刻返回）
    [[ -f $PADBASE.mib ]] || return 0
    touch "$PADBASE.start"
    local i
    for i in $(seq 30); do
        [[ $(cat "$PADBASE.mib" 2> /dev/null) == 0 ]] && return 0
        sleep 1
    done
    echo "$(ts) 显存填充 30 秒内没放掉（$PADBASE.mib = $(cat "$PADBASE.mib" 2> /dev/null)），照样开始"
}
gpu_stats() {  # 近 10 分钟的 GPU 平均使用率、样本数；现在的显存已用、总量（MiB）
    if [[ -n ${WORKER_TEST_GPU:-} ]]; then echo "$WORKER_TEST_GPU"; return; fi
    local ut mem
    ut=$(tail -n 10 "$GPUCSV" 2> /dev/null | awk -F', *' '{gsub(/[^0-9]/, "", $5); if ($5 != "") { u += $5; n++ } }
        END { if (n) printf "%d %d\n", u / n, n; else print "0 0" }')
    mem=$(nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader,nounits 2> /dev/null | head -1 | tr -d ' ' | tr ',' ' ')
    echo "$ut ${mem:-0 0}"
}
pad_now() {  # 显存填充现在占着多少 MiB；<base>.mib 超过 60 秒没更新（填充已经不在了）算 0
    local v
    v=$(cat "$PADBASE.mib" 2> /dev/null)
    [[ $v =~ ^[0-9]+$ ]] && (( $(date +%s) - $(stat -c %Y "$PADBASE.mib") <= 60 )) && echo "$v" || echo 0
}
can_add() {  # 手上有训练时，还能不能再加一个；不能时 why_not 里写原因
    local n=${#KEY[@]} util cnt used total pad
    why_not=
    (( n == 0 )) && return 0
    (( n >= SLOTS )) && { why_not="已经 $n 个"; return 1; }
    (( $(date +%s) - CHANGED >= ADD_AFTER )) || { why_not="刚开始，先看看"; return 1; }
    read -r util cnt used total <<< "$(gpu_stats)"
    pad=$(pad_now)
    (( cnt >= 5 && total > 0 )) || { why_not="GPU 记录不够"; return 1; }
    (( util < UTIL_MAX )) || { why_not="GPU 使用率 $util%"; return 1; }
    (( total - used + pad >= total / 2 )) || { why_not="显存不够（训练占 $(( used - pad ))MiB / ${total}MiB）"; return 1; }
    echo "$(ts) GPU 使用率 $util%、训练占 $(( used - pad ))MiB / ${total}MiB，再领一个"
    return 0
}
run_one() {  # 数据集编号 训练器 fold 日志
    local dsid=$1 tr=$2 fold=$3 log=$4 rc
    export nnUNet_n_proc_DA=$DA PYTHONUNBUFFERED=1
    pad_make_room
    # 按训练器换环境（同 train_folds.sbatch；在后台的子 shell 里，不影响别的任务）。先退出作业启动时带进来的 venv
    # （旧的 SwinUNETR / U-Mamba 作业是在 venv 里开始的），基于 nnU-Net 的训练器用 conda 的默认环境。
    if [[ -n ${VIRTUAL_ENV:-} ]]; then PATH=${PATH//"$VIRTUAL_ENV/bin:"/}; unset VIRTUAL_ENV; fi
    case $tr in
        *SwinUNETR*) source ~/envs/swinunetr/bin/activate || return 1 ;;
        *UMamba*) source ~/envs/umamba/bin/activate || return 1 ;;
    esac
    if [[ -n ${WORKER_TEST_CMD:-} ]]; then
        WORKER_TEST_TR=$tr $WORKER_TEST_CMD "$dsid" "$fold" >> "$log" 2>&1
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
    CHANGED=$(date +%s)
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
    rm -f "$ERRF"
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
    why= tried=0
    while (( $(left) > MIN_LEFT && OWN_FAILS < MAX_OWN_FAIL )) && can_add; do
        tried=1
        # 一次只开始 1 个（第二个要等 15 分钟、看 GPU），所以只按 1 个空位给排在前面刚停下的折留位置
        t=$($TQPY claim --worker "$W" --kind any --free-slots 1 \
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
    (( tried )) || err_since=0  # 这一轮没去领（手上满了）：之前的出错不接着算
    if [[ -n $why_not && $why_not != "${last_why:-}" ]]; then echo "$(ts) 先不再加训练：$why_not"; fi
    last_why=$why_not
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
            kill_tree "$GPUMON"; rm -f "$ERRF"
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
rm -f "$ERRF"
exit 0
