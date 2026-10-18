#!/bin/bash
# DICC 训练作业的任务队列 worker（2026-10-17；10/18 起不认识具体模型）：从 scripts/queue/taskq.py 的队列里按顺序领任务，
# 每个任务交给 scripts/queue/run_task.sh 跑（环境、命令、怎么算训完都在任务说明里，见 scripts/queue/recipes.py）。
# 只领本机有环境的任务（envs/machines.py 的 envs）。
# 一张卡训几个看实际情况：先训 1 个；最近一个训练开始 15 分钟以后，如果近 10 分钟 GPU 平均使用率低于 90%（10/18 前 80%）、
# 还空着一半显存，再领 1 个（最多 2 个，并且作业要有 ≥ 24 个 CPU、≥ 56G 内存）。
# CPU 跟不上的训练（nnU-Net）会变成 2 个，GPU 跑满的（SwinUNETR、U-Mamba）自然只训 1 个。训完一个再领下一个；
# 离时限不到 1 小时不再领新的；没有在训的、也领不到新的就退出。作业到时限被停掉时，没训完的任务交还队列，
# 下一个领到的从存档接着训。
#   bash dicc_worker.sh                     （不带折的作业 scripts/queue/worker.sbatch）
#   bash dicc_worker.sh <训练器> <fold> <数据集编号>   （10/17 前交的按折作业，由 envs/nnunet_paths.sh 转过来；参数只记日志）
# 出问题时不让排队的作业空跑掉：
#   - 本作业训练失败 2 次（或开训前就出错）就不再领新的，也不重领自己失败过的；
#   - 整个队列 3 小时内失败 3 次，taskq.py 暂停领任务：作业先空着等（健康检查提醒）；
#   - 队列本身出错（taskq.py 读写不了）连续 10 分钟、手上又没有在训的：结束作业、把卡还回去；
#   - 查不到作业的结束时间时当作时间还多，之后每轮再查。
# 不管什么原因，手上没有在训的连续 40 分钟就结束作业、把卡还回去：DICC gpu-a100 分区 GPU 使用率连续 1 小时低于 40%
# 会被自动取消（10/17 的新规定）。
# TASKQ_* 环境变量只给测试用（scripts/queue/test_dicc_worker.sh）。
set -uo pipefail
unset _envs
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(dirname "$(dirname "$HERE")")
TQPY="python3 $HERE/taskq.py"
W=${SLURM_JOB_ID:?只在 Slurm 作业里用}
DATA=${BRAIN_TUMOR:-$(python3 "$REPO/envs/machines.py" get . data)}
export BRAIN_TUMOR=$DATA
ENVS=${TASKQ_ENVS:-$(python3 "$REPO/envs/machines.py" get . envs | tr ' ' ,)}
NCPU=$(python3 -c "import os; print(len(os.sched_getaffinity(0)))" 2> /dev/null || nproc)
MEMMB=${SLURM_MEM_PER_NODE:-$(( ${SLURM_MEM_PER_CPU:-0} * NCPU ))}
if (( NCPU >= 24 )) && (( MEMMB == 0 || MEMMB >= 56 * 1024 )); then
    SLOTS=${TASKQ_SLOTS:-2}   # 最多同时训几个
else
    SLOTS=${TASKQ_SLOTS:-1}
fi
ADD_AFTER=${TASKQ_ADD_AFTER:-900}       # 最近一个训练开始以后，过这么久才判断要不要再加一个
UTIL_MAX=${TASKQ_UTIL_MAX:-90}          # 近 10 分钟 GPU 平均使用率低于这个才加
PADBASE=${RQ1_GPU_PAD_BASE:-/tmp/gpu_pad_$W}
MIN_LEFT=${TASKQ_MIN_LEFT:-3600}
PATIENCE=${TASKQ_PATIENCE:-1200}        # 开始后最多等这么久，给排在前面、刚停下的任务留位置
ERR_MAX=${TASKQ_ERR_MAX:-600}           # 队列连续出错这么久（手上又没有在训的）就结束作业
IDLE_MAX=${TASKQ_IDLE_MAX:-2400}        # 手上没有在训的连续这么久就结束作业（DICC：使用率连续 1 小时低于 40% 会被取消）
MAX_OWN_FAIL=2
TASK_CPUS=$(( NCPU / SLOTS ))           # 每个训练分到的 CPU（run_task.sh → 训练命令，如 nnU-Net 的数据增强进程数）
unset nnUNet_n_proc_DA                  # 10/17 前交的作业里 nnunet_paths.sh 按整个作业设的，不管一张卡训几个；改由训练命令按 TASK_CPUS 算
LOGDIR=$DATA/logs/workers
mkdir -p "$LOGDIR"
COMMIT=$(git -C "$REPO" rev-parse --short HEAD 2> /dev/null || echo 未知)
git -C "$REPO" diff --quiet HEAD -- scripts/queue research/common 2> /dev/null || COMMIT="$COMMIT+改动"
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
echo "$(ts) 任务队列 worker：作业 $W @ $(hostname -s)，最多同时 $SLOTS 个，CPU $NCPU，内存 ${MEMMB}M（每个训练 $TASK_CPUS 个 CPU），" \
     "环境 $ENVS，时限 $( (( END )) && date -d @"$END" '+%F %T' || echo 查不到，之后再查)，代码 $COMMIT${1:+（按折作业 $*，参数不再用）}"
nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader 2> /dev/null

GPUCSV=$LOGDIR/worker_${W}_gpu.csv
( while true; do nvidia-smi --query-gpu=timestamp,index,memory.used,memory.total,utilization.gpu --format=csv,noheader >> "$GPUCSV" 2> /dev/null; sleep 60; done ) &
GPUMON=$!

declare -A KEY=()         # 训练的 pid → key
CHANGED=$T0               # 最近一个训练开始的时刻
EXCL=()                   # 本作业失败过的 key，不再领
OWN_FAILS=0
pad_make_room() {
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
pad_now() {
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
fail() {  # key 原因：交还并记一次失败；本作业不再领这个 key
    $TQPY release --worker "$W" "$1" --failed "$2"
    EXCL+=("$1")
    OWN_FAILS=$(( OWN_FAILS + 1 ))
    (( OWN_FAILS >= MAX_OWN_FAIL )) && echo "$(ts) 本作业已失败 $OWN_FAILS 次，不再领新任务（多半是这个节点或环境有问题）"
}
start() {  # taskq.py next 的输出：第一行 key，第二行 run_task.sh 的参数（shell 引号）
    local key args
    { read -r key; read -r args; } <<< "$1"
    eval "local targs=($args)"
    pad_make_room
    TASK_JOBTAG=$W TASK_SEG=1 TASK_CPUS=$TASK_CPUS bash "$HERE/run_task.sh" "${targs[@]}" &
    KEY[$!]=$key
    CHANGED=$(date +%s)
    echo "$(ts) 开训 $key（pid $!：run_task.sh $args）"
}
kill_tree() { local c; for c in $(pgrep -P "$1"); do kill_tree "$c"; done; kill -TERM "$1" 2> /dev/null; }
on_term() {
    echo "$(ts) 收到停止信号（多半是到时限），停训练、交还任务"
    local p
    for p in "${!KEY[@]}"; do kill_tree "$p"; done
    for _ in $(seq 15); do
        local alive=0
        for p in "${!KEY[@]}"; do kill -0 "$p" 2> /dev/null && alive=1; done
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
    # 收训完的（run_task.sh 的退出码：0 训完，3 命令正常结束但没训完，其他 = 出错）
    for p in "${!KEY[@]}"; do
        if ! kill -0 "$p" 2> /dev/null; then
            wait "$p"; rc=$?
            key=${KEY[$p]}; unset "KEY[$p]"
            if (( rc == 0 )); then
                echo "$(ts) 训完 $key"
                $TQPY release --worker "$W" "$key"
            elif (( rc == 3 )); then
                echo "$(ts) $key 命令正常结束，但没有训完的文件，交还并记一次失败"
                fail "$key" "退出码 0 但没训完"
            else
                echo "$(ts) $key 训练失败（run_task.sh 退出码 $rc），交还并记一次失败"
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
        # 一次只开始 1 个（第二个要等 15 分钟、看 GPU），所以只按 1 个空位给排在前面刚停下的任务留位置
        t=$($TQPY next --worker "$W" --envs "$ENVS" --free-slots 1 \
                       --patience-until $(( T0 + PATIENCE )) --exclude ${EXCL[@]+"${EXCL[@]}"} 2> "$ERRF")
        rc=$?
        case $rc in
            0)  err_since=0; start "$t" ;;
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
        elif [[ $why == error ]] && (( now - err_since >= ERR_MAX )); then
            echo "$(ts) 队列连续出错 $(( (now - err_since) / 60 )) 分钟（$(tail -n 1 "$ERRF")），结束作业、把卡还回去"; break
        elif (( now - idle_since >= IDLE_MAX )); then
            echo "$(ts) 手上没有在训的已经 $(( (now - idle_since) / 60 )) 分钟（$why），结束作业、把卡还回去（DICC：GPU 使用率连续 1 小时低于 40% 会被取消）"
            break
        fi
        # wait（等前面刚停下的任务）、paused（队列暂停）、error（还没到结束的时候）：先空着等，最多 IDLE_MAX
        [[ $why == paused && ${said_paused:-} != 1 ]] && { echo "$(ts) 队列暂停领任务（3 小时内失败太多次），空着等，最多 $(( IDLE_MAX / 60 )) 分钟"; said_paused=1; }
        [[ $why == wait && ${said_wait:-} != 1 ]] && { echo "$(ts) 排在前面的任务刚停下，等它（最多到开始后 $(( PATIENCE / 60 )) 分钟）"; said_wait=1; }
    fi
    sleep "${TASKQ_POLL:-30}" & wait $!
done
kill_tree "$GPUMON"
rm -f "$ERRF"
exit 0
