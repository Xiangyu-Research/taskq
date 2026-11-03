#!/bin/bash
# DICC 训练作业的任务队列 worker（2026-10-17；10/18 起不认识具体模型）：从 scripts/queue/taskq.py 的队列里按顺序领任务，
# 每个任务交给 scripts/queue/run_task.sh 跑（环境、命令、怎么算训完都在任务说明里，见 scripts/queue/recipes.py）。
# 只领本机有环境的任务（envs/machines.py 的 envs）。
# 一张卡训几个看实际情况：先训 1 个；最近一个训练开始 15 分钟以后，如果近 10 分钟 GPU 平均使用率低于 90%（10/18 前 80%）、
# 还空着一半显存，再领 1 个（最多 2 个，并且作业要有 ≥ 24 个 CPU、≥ 56G 内存）。
# CPU 跟不上的训练（nnU-Net）会变成 2 个，GPU 跑满的（SwinUNETR、U-Mamba）自然只训 1 个。训完一个再领下一个；
# 离时限不到 1 小时不再领新的；没有在训的、也领不到新的，空着等（有了就领），等满 40 分钟还没有就退出
# （11/2 起：排了几天的作业开始时缓冲刚好空着、数据还在传，不再几秒就结束）。作业到时限被停掉时，没训完的任务交还队列，
# 下一个领到的从存档接着训。
#   bash dicc_worker.sh                     （scripts/queue/worker.sbatch → worker_start.sh）
# WORKER_DEVICE=cpu（scripts/queue/cpu_worker.sbatch，2026-10-24）：CPU 分区的作业，只领 device=cpu 的任务（如评测），一次 1 个，
# 不等排在前面的、不看 GPU；没有能领的就结束（由 box 上的 refill.py 在有这类任务时才交）。
# 出问题时不让排队的作业空跑掉：
#   - 任务卡住（10/23 起）：开始 30 分钟以后，任务的日志和结果文件夹都 30 分钟没有变化，就停掉它、交还（不算失败），
#     马上领下一个；本作业不再领它。停不下来的（卡在读写上）3 分钟后不再等它。读不了日志或结果文件夹时不判断；
#   - 本作业有 2 个任务开始 10 分钟内就失败（多半是节点或环境的问题），就不再领新的；跑了一段时间才失败的
#     只记在任务上（10/23 起），作业照常领下一个。本作业不重领自己失败过的；
#   - 整个队列 3 小时内失败 3 次，taskq.py 暂停领任务：作业先空着等（健康检查提醒）；
#   - 队列本身出错（taskq.py 读写不了）连续 10 分钟、手上又没有在训的：结束作业、把卡还回去；
#   - 查不到作业的结束时间时当作时间还多，之后每轮再查。
# 停止的任务（taskq.py stop，10/30）：每 5 秒看一次，发停止信号，20 秒还没停就强制停，交还（不算失败），30 秒内做完；
# 存档是原子写的，下次从存档接着训。
# 不管什么原因，手上没有在训的连续 40 分钟就结束作业、把卡还回去：DICC gpu-a100 分区 GPU 使用率连续 1 小时低于 40%
# 会被自动取消（10/17 的新规定）。
# 不在 Slurm 里（lab 的 pueue 组里常驻，10/31）：WORKER_ID 当 worker 名（envs/machines.py 的 worker），同时训几个看本机的 slots；
# 没有时限、不查 squeue、没有显存填充；没有在训的、领不到的、队列出错、本 worker 失败太多次都不退出，每 60 秒再看；
# 代码变了（仓库的提交号，或代码包的 current 换了）就不再领新的，手上的做完后退出（退出码 0），由 box 用新代码重开（11/1）。
# GPU 记录只看 CUDA_VISIBLE_DEVICES 那张卡（lab 另一张卡别人在用）。
# TASKQ_* 环境变量只给测试用（scripts/queue/test_dicc_worker.sh）。
set -uo pipefail
unset _envs
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(dirname "$(dirname "$HERE")")
TQPY="python3 $HERE/taskq.py"
if [[ -n ${SLURM_JOB_ID:-} ]]; then INSLURM=1; else INSLURM=0; fi
W=${SLURM_JOB_ID:-${WORKER_ID:?不在 Slurm 作业里时要设 WORKER_ID（envs/machines.py 的 worker）}}
DEVICE=${WORKER_DEVICE:-gpu}
source "$HERE/legacy_names.sh" map
DATA=${TASKQ_DATA:-$(python3 "${TASKQ_CONFIG:-$REPO/envs}/machines.py" get . data)}
export TASKQ_DATA=$DATA
source "$HERE/legacy_names.sh" export
ENVS=${TASKQ_ENVS:-$(python3 "${TASKQ_CONFIG:-$REPO/envs}/machines.py" get . envs | tr ' ' ,)}
NCPU=$(python3 -c "import os; print(len(os.sched_getaffinity(0)))" 2> /dev/null || nproc)
# Slurm 外（pueue 的常驻 worker）：最多用 TASKQ_CPUS 个核（envs/machines.py 的 cpu_worker 的 cpus），和别人共用这台机器
if (( ! INSLURM )) && [[ ${TASKQ_CPUS:-} =~ ^[0-9]+$ ]] && (( TASKQ_CPUS > 0 && TASKQ_CPUS < NCPU )); then NCPU=$TASKQ_CPUS; fi
MEMMB=${SLURM_MEM_PER_NODE:-$(( ${SLURM_MEM_PER_CPU:-0} * NCPU ))}
if [[ $DEVICE == cpu ]]; then
    SLOTS=1
elif (( ! INSLURM )); then
    SLOTS=${TASKQ_SLOTS:-$(python3 "${TASKQ_CONFIG:-$REPO/envs}/machines.py" get . slots 2> /dev/null || true)}
    [[ $SLOTS =~ ^[0-9]+$ ]] || SLOTS=1
elif (( NCPU >= 24 )) && (( MEMMB == 0 || MEMMB >= 56 * 1024 )); then
    SLOTS=${TASKQ_SLOTS:-2}   # 最多同时训几个
else
    SLOTS=${TASKQ_SLOTS:-1}
fi
ADD_AFTER=${TASKQ_ADD_AFTER:-900}       # 最近一个训练开始以后，过这么久才判断要不要再加一个
UTIL_MAX=${TASKQ_UTIL_MAX:-90}          # 近 10 分钟 GPU 平均使用率低于这个才加
PADBASE=${TASKQ_GPU_PAD_BASE:-$( (( INSLURM )) && echo "/tmp/gpu_pad_$W" || echo /nonexistent/gpu_pad)}
MIN_LEFT=${TASKQ_MIN_LEFT:-3600}
PATIENCE=${TASKQ_PATIENCE:-$([[ $DEVICE == cpu ]] && echo 0 || echo 1200)}        # 开始后最多等这么久，给排在前面、刚停下的任务留位置
ERR_MAX=${TASKQ_ERR_MAX:-600}           # 队列连续出错这么久（手上又没有在训的）就结束作业
IDLE_MAX=${TASKQ_IDLE_MAX:-2400}        # 手上没有在训的连续这么久就结束作业（DICC：使用率连续 1 小时低于 40% 会被取消）
IDLE_POLL=${TASKQ_IDLE_POLL:-60}        # 不在 Slurm 里、手上没有在训的时候隔多久再看
IDLE_EXIT=${TASKQ_IDLE_EXIT:-1800}      # 不在 Slurm 里：手上没有在训的连续这么久就退出（退出码 0），有备好的任务时下次检查再开（10/31）
MAX_OWN_FAIL=2
EARLY_FAIL=${TASKQ_EARLY_FAIL:-600}     # 开始后这么久内就失败的才算本作业（节点、环境）的问题
FAIL_WINDOW=${TASKQ_FAIL_WINDOW:-3600}  # 只数这么久以内的（10/24：隔很久的零星失败不让作业提前结束）
STALL_MAX=${TASKQ_STALL_MAX:-1800}      # 任务的日志和结果文件夹都这么久没变化，算卡住
TASK_CPUS=$(( NCPU / SLOTS ))           # 每个训练分到的 CPU（run_task.sh → 训练命令，如 nnU-Net 的数据增强进程数）
unset nnUNet_n_proc_DA                  # nnunet_paths.sh 在 Slurm 里按整个作业设的，不管一张卡训几个；改由训练命令按 TASK_CPUS 算
LOGDIR=$DATA/logs/workers
mkdir -p "$LOGDIR"
if [[ $(git -C "$REPO" rev-parse --show-toplevel 2> /dev/null) == "$(readlink -f "$REPO")" ]]; then
    COMMIT=$(git -C "$REPO" rev-parse --short HEAD 2> /dev/null || echo 未知)
    CODE_DIRS=$(python3 "$HERE/cfg.py" get code_dirs 2> /dev/null)
    git -C "$REPO" diff --quiet HEAD -- ${CODE_DIRS:-scripts/queue} 2> /dev/null || COMMIT="$COMMIT+改动"
else
    COMMIT=$(basename "$(readlink -f "$REPO")")  # 代码包（relay_bundle.py）：包名 <提交号>-<内容哈希>
fi
head_now() {
    if [[ -n ${TASKQ_HEAD_CMD:-} ]]; then eval "$TASKQ_HEAD_CMD"
    elif [[ -L $(dirname "$REPO")/current ]]; then readlink -f "$(dirname "$REPO")/current"
    else git -C "$REPO" rev-parse HEAD 2> /dev/null; fi
}
code_changed() { (( ! INSLURM )) && [[ -n $HEAD0 && $(head_now) != "$HEAD0" ]]; }
HEAD0=$(head_now)
RESTART=
[[ -L $(dirname "$REPO")/current ]] && touch "$REPO" 2> /dev/null  # 在用的代码包：开始时和每次心跳更新时间戳
NVSEL=()  # 不在 Slurm 里：只看分给本 worker 的卡（Slurm 作业里只看得到自己的卡）
(( ! INSLURM )) && [[ ${CUDA_VISIBLE_DEVICES:-} =~ ^[0-9]+$ ]] && NVSEL=(-i "$CUDA_VISIBLE_DEVICES")
ts() { date '+%F %T'; }
T0=$(date +%s)

# 作业的结束时间：查不到（Slurm 偶尔超时）就记 0，当作时间还多，之后每轮再查
END=0
get_end() {
    (( INSLURM )) || return 1
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
echo "$(ts) 任务队列 worker（$DEVICE）：作业 $W @ $(hostname -s)，最多同时 $SLOTS 个，CPU $NCPU，内存 ${MEMMB}M（每个训练 $TASK_CPUS 个 CPU），" \
     "环境 $ENVS，时限 $( (( END )) && date -d @"$END" '+%F %T' || { (( INSLURM )) && echo 查不到，之后再查 || echo 无; })，代码 $COMMIT"
GPUCSV=$LOGDIR/worker_${W}_gpu.csv
if [[ $DEVICE == gpu ]]; then
    nvidia-smi ${NVSEL[@]+"${NVSEL[@]}"} --query-gpu=index,name,memory.total --format=csv,noheader 2> /dev/null
    ( while true; do nvidia-smi ${NVSEL[@]+"${NVSEL[@]}"} --query-gpu=timestamp,index,memory.used,memory.total,utilization.gpu --format=csv,noheader >> "$GPUCSV" 2> /dev/null; sleep 60; done ) &
    GPUMON=$!
else
    GPUMON=
fi
# 任务的 res 超过这里给得起的不领（11/1）：每个任务的 CPU、内存，卡的显存（GB）
RESARGS=(--cpus "$TASK_CPUS")
(( MEMMB > 0 )) && RESARGS+=(--mem-mb $(( MEMMB / SLOTS )))
if [[ $DEVICE == gpu ]]; then
    g=$(nvidia-smi ${NVSEL[@]+"${NVSEL[@]}"} --query-gpu=memory.total --format=csv,noheader,nounits 2> /dev/null | head -1 | tr -d ' ')
    [[ $g =~ ^[0-9]+$ ]] && RESARGS+=(--gpu-mem $(( (g + 512) / 1024 )))
fi

declare -A KEY=()         # 训练的 pid → key
declare -A STARTED=()     # pid → 开始的时刻
declare -A OUTDIR=()      # pid → 结果文件夹
declare -A SAFE=()        # pid → key 转成的文件名（spec.py 的 TASK_SAFE）
declare -A JTAG=()        # pid → 分段日志名里的作业名（run_task.sh 的 TASK_JOBTAG）
declare -A STALLED=()     # pid → 当作卡住、开始停它的时刻
declare -A TOK=()         # key → 这次领取的 token（taskq.py next 的第三行；交还、心跳时带上，11/1）
tq() {  # tq <release|beat|missing> <key> [参数…]：带上这个 key 的 token
    local c=$1 k=$2; shift 2
    $TQPY "$c" --worker "$W" ${TOK[$k]:+--token "${TOK[$k]}"} "$k" "$@"
}
dead() { ! kill -0 "$1" 2> /dev/null; }
CHANGED=$T0               # 最近一个训练开始的时刻
EXCL=()                   # 本作业失败过的 key，不再领
OWN_FAILS=0
FAIL_AT=()                # 本作业开始不久就失败的时刻
pad_make_room() {
    [[ -f $PADBASE.mib ]] || return 0
    rm -f "$PADBASE.ready"
    touch "$PADBASE.start"
    local i
    for i in $(seq 30); do
        [[ -e $PADBASE.ready || $(cat "$PADBASE.mib" 2> /dev/null) == 0 ]] && return 0
        sleep 1
    done
    echo "$(ts) 显存填充 30 秒内没回应（$PADBASE.mib = $(cat "$PADBASE.mib" 2> /dev/null)），照样开始"
}
gpu_stats() {  # 近 10 分钟的 GPU 平均使用率、样本数；现在的显存已用、总量（MiB）
    if [[ -n ${WORKER_TEST_GPU:-} ]]; then echo "$WORKER_TEST_GPU"; return; fi
    local ut mem
    ut=$(tail -n 10 "$GPUCSV" 2> /dev/null | awk -F', *' '{gsub(/[^0-9]/, "", $5); if ($5 != "") { u += $5; n++ } }
        END { if (n) printf "%d %d\n", u / n, n; else print "0 0" }')
    mem=$(nvidia-smi ${NVSEL[@]+"${NVSEL[@]}"} --query-gpu=memory.used,memory.total --format=csv,noheader,nounits 2> /dev/null | head -1 | tr -d ' ' | tr ',' ' ')
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
fail() {  # key 原因 跑了多少秒：交还并记一次失败；本作业不再领这个 key；开始不久就失败的才算本作业的
    tq release "$1" --failed "$2"
    EXCL+=("$1")
    (( $3 < EARLY_FAIL )) || return 0
    FAIL_AT+=("$(date +%s)")
    own_fails
    (( OWN_FAILS >= MAX_OWN_FAIL )) && echo "$(ts) 本作业 $(( FAIL_WINDOW / 60 )) 分钟内已有 $OWN_FAILS 个任务开始不久就失败，不再领新任务（多半是这个节点或环境有问题）"
    return 0
}
own_fails() {  # OWN_FAILS = FAIL_WINDOW 以内的次数
    local now t keep=()
    now=$(date +%s)
    for t in ${FAIL_AT[@]+"${FAIL_AT[@]}"}; do (( now - t < FAIL_WINDOW )) && keep+=("$t"); done
    FAIL_AT=(${keep[@]+"${keep[@]}"})
    OWN_FAILS=${#FAIL_AT[@]}
}
quiet_since() {  # pid → 任务的日志和结果文件夹多少秒没有变化；读不了时输出空
    local p=$1 now log m
    now=$(date +%s)
    log=$DATA/logs/tasks/${SAFE[$p]}__${JTAG[$p]:-$W}_part1.log
    m=$(timeout 20 stat -c %Y "$log" 2> /dev/null) || return 0
    (( now - m < STALL_MAX )) && { echo $(( now - m )); return 0; }
    [[ -n ${OUTDIR[$p]:-} && -d ${OUTDIR[$p]} ]] || { echo $(( now - m )); return 0; }
    local hit
    hit=$(timeout 60 find "${OUTDIR[$p]}" -newermt "@$(( now - STALL_MAX ))" -print -quit 2> /dev/null) || return 0
    [[ -n $hit ]] && echo 0 || echo $(( now - m ))
}
start() {  # taskq.py next 的输出：第一行 key，第二行 run_task.sh 的参数（shell 引号）
    local key args tok
    { read -r key; read -r args; read -r tok; } <<< "$1"
    TOK[$key]=$tok
    eval "local targs=($args)"
    pad_make_room
    local jt
    jt=$( (( INSLURM )) && echo "$W" || echo "$(hostname -s)-$(date +%Y%m%d%H%M)")
    TASK_JOBTAG=$jt TASK_PART=1 TASK_CPUS=$TASK_CPUS bash "$HERE/run_task.sh" "${targs[@]}" &
    local pid=$! sh TASK_OUT= TASK_SAFE=
    KEY[$pid]=$key
    JTAG[$pid]=$jt
    STARTED[$pid]=$(date +%s)
    sh=$(python3 "$HERE/spec.py" shell "${targs[@]}" 2> /dev/null | grep -E '^TASK_(OUT|SAFE)=')
    eval "$sh"
    OUTDIR[$pid]=$TASK_OUT
    SAFE[$pid]=${TASK_SAFE:-${key//\//__}}
    CHANGED=$(date +%s)
    echo "$(ts) 开训 $key（pid $pid：run_task.sh $args）"
}
kill_tree() { [[ -n $1 ]] || return 0; local c s=${2:-TERM}; for c in $(pgrep -P "$1"); do kill_tree "$c" "$s"; done; kill -"$s" "$1" 2> /dev/null; }
any_alive() { local p; for p in "${!KEY[@]}"; do dead "$p" || return 0; done; return 1; }
on_term() {  # run_task.sh 自己 8 秒后强制停训练；15 秒还没退出的整棵树 SIGKILL。确认停下的才交还（11/1），30 秒内做完
    echo "$(ts) 收到停止信号（$( (( INSLURM )) && echo 多半是到时限 || echo pueue kill)），停训练、交还任务"
    local p
    for p in "${!KEY[@]}"; do kill_tree "$p"; done
    for _ in $(seq "$TERM_WAIT"); do any_alive || break; sleep 1; done
    if any_alive; then
        echo "$(ts) $TERM_WAIT 秒没停下，强制停"
        for p in "${!KEY[@]}"; do dead "$p" || kill_tree "$p" KILL; done
        for _ in 1 2 3; do any_alive || break; sleep 1; done
    fi
    for p in "${!KEY[@]}"; do
        if dead "$p"; then tq release "${KEY[$p]}"
        else echo "$(ts) ${KEY[$p]} 强制停之后还在，不交还（领取过期后才能被别人领）"; fi
    done
    kill_tree "$GPUMON"
    rm -f "$ERRF"
    rm -rf "${TMPDIR:-/tmp}/phd_stage_${SLURM_JOB_ID:-none}"
    (( INSLURM )) && exit 143
    exit 0  # 不在 Slurm 里：交还好了算正常结束，feed.py 照常重开（失败的 worker 它不重开）
}
trap on_term TERM INT

QDIR=${TASKQ_DIR:-${TASKQ_STATE:+$TASKQ_STATE/taskq}}
QDIR=${QDIR:-$HOME/.cache/phd/taskq}
SAFECH=$(python3 -c "import sys; sys.path.insert(0, '$HERE'); import spec; print(spec.SAFE)" 2> /dev/null || echo __)
STOP_POLL=${TASKQ_STOP_POLL:-5}
STOP_WAIT=${TASKQ_STOP_WAIT:-20}
TERM_WAIT=${TASKQ_TERM_WAIT:-15}
check_stops() {
    local p k i n=0
    for p in "${!KEY[@]}"; do
        k=${KEY[$p]}
        [[ -n ${STALLED[$p]:-} ]] && continue  # 已经在按卡住停
        [[ -e $QDIR/stopped/${k//\//$SAFECH} ]] || continue
        echo "$(ts) $k 被停止（taskq.py stop），停训练、交还"
        kill_tree "$p"
        for (( i = 0; i < STOP_WAIT; i++ )); do kill -0 "$p" 2> /dev/null || break; sleep 1; done
        if kill -0 "$p" 2> /dev/null; then
            echo "$(ts) $k $STOP_WAIT 秒没停下，强制停"
            kill_tree "$p" KILL
            sleep 1
        fi
        if dead "$p"; then
            wait "$p" 2> /dev/null
            tq release "$k"
            unset "KEY[$p]" "STARTED[$p]" "OUTDIR[$p]" "STALLED[$p]" "SAFE[$p]" "JTAG[$p]"
            n=1
        else  # 交给卡住的那一路：停不下就不交还
            echo "$(ts) $k 强制停之后还在，按卡住处理"
            STALLED[$p]=${STALLED[$p]:-$(date +%s)}
        fi
    done
    (( n ))
}
nap() {  # [秒数]（默认 TASKQ_POLL）
    local end=$(( $(date +%s) + ${1:-${TASKQ_POLL:-30}} )) d
    while (( (d = end - $(date +%s)) > 0 )); do
        check_stops && return 0
        (( d > STOP_POLL )) && d=$STOP_POLL
        sleep "$d" & wait $!
    done
    check_stops
    return 0
}

why_not=
last_beat=$(date +%s)
ERRF=$(mktemp)
err_since=0   # 队列从什么时候开始连续出错
idle_since=$T0  # 从什么时候开始手上没有在训的（0 = 有在训的）
while true; do
    # 收训完的（run_task.sh 的退出码：0 训完，3 命令正常结束但没训完，4 缺输入，其他 = 出错）
    for p in "${!KEY[@]}"; do
        if ! kill -0 "$p" 2> /dev/null; then
            wait "$p"; rc=$?
            key=${KEY[$p]}; unset "KEY[$p]"
            ran=$(( $(date +%s) - ${STARTED[$p]:-0} ))
            stalled=${STALLED[$p]:-}
            unset "STARTED[$p]" "OUTDIR[$p]" "STALLED[$p]" "SAFE[$p]" "JTAG[$p]"
            if [[ -n $stalled ]]; then
                echo "$(ts) 卡住的 $key 已停下，交还（不算失败），本作业不再领它"
                tq release "$key"
                EXCL+=("$key")
            elif (( rc == 0 )); then
                echo "$(ts) 训完 $key"
                tq release "$key"
            elif (( rc == 4 )); then
                echo "$(ts) $key 缺输入，没开训，交还，记为缺输入（不算失败，补齐前不再领）"
                tq missing "$key" --why "作业 $W 开训前查到" || tq release "$key"
                EXCL+=("$key")
            elif (( rc == 3 )); then
                echo "$(ts) $key 命令正常结束，但没有训完的文件，交还并记一次失败"
                fail "$key" "退出码 0 但没训完" "$ran"
            else
                echo "$(ts) $key 训练失败（run_task.sh 退出码 $rc），交还并记一次失败"
                fail "$key" "退出码 $rc" "$ran"
            fi
        fi
    done
    now=$(date +%s)
    # 卡住的任务：停掉、交还，空出位置领下一个
    for p in "${!KEY[@]}"; do
        if [[ -n ${STALLED[$p]:-} ]]; then
            if (( now - STALLED[$p] >= 180 )); then
                # 还活着就不交还（11/1）：不再心跳，领取过期后别人才能领；空出位置领下一个
                echo "$(ts) ${KEY[$p]} 停了 3 分钟还没停下（多半卡在读写上），不再等它，不交还（领取过期后别人才能领）"
                EXCL+=("${KEY[$p]}")
                unset "KEY[$p]" "STARTED[$p]" "OUTDIR[$p]" "STALLED[$p]" "SAFE[$p]" "JTAG[$p]" "TOK[${KEY[$p]}]"
            elif (( now - STALLED[$p] >= 60 )); then
                kill_tree "$p" KILL
            fi
            continue
        fi
        (( now - STARTED[$p] >= STALL_MAX )) || continue
        q=$(quiet_since "$p")
        [[ -n $q ]] && (( q >= STALL_MAX )) || continue
        echo "$(ts) ${KEY[$p]} 的日志和结果文件夹 $(( q / 60 )) 分钟没有变化，当作卡住：停掉、交还，领下一个"
        STALLED[$p]=$now
        kill_tree "$p"
    done
    (( END )) || get_end
    if (( now - last_beat >= 300 )); then
        for p in "${!KEY[@]}"; do tq beat "${KEY[$p]}"; done
        # 代码包：更新时间戳，清理旧包时不删还有 worker 在用的（只删 4 天没动过的，11/1）
        [[ -L $(dirname "$REPO")/current ]] && touch "$REPO" 2> /dev/null
        last_beat=$now
    fi
    # 领新的
    why= tried=0
    own_fails
    while (( $(left) > MIN_LEFT && OWN_FAILS < MAX_OWN_FAIL )) && ! code_changed && can_add; do
        tried=1
        # 一次只开始 1 个（第二个要等 15 分钟、看 GPU），所以只按 1 个空位给排在前面刚停下的任务留位置
        t=$($TQPY next --worker "$W" --envs "$ENVS" --device "$DEVICE" "${RESARGS[@]}" --free-slots 1 \
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
        idle_since=0 said_idle= said_nothing=
    else
        (( idle_since )) || idle_since=$now
        if (( ! INSLURM )); then
            if code_changed; then
                echo "$(ts) 代码更新了"; RESTART=1; break
            fi
            msg=$why
            (( OWN_FAILS >= MAX_OWN_FAIL )) && msg=own
            if [[ $msg != own ]] && (( now - idle_since >= IDLE_EXIT )); then
                echo "$(ts) 手上没有在训的已经 $(( (now - idle_since) / 60 )) 分钟，退出（有备好的任务时下次检查再开）"; break
            fi
            if [[ $msg != "${said_idle:-}" ]]; then
                case $msg in
                    own) echo "$(ts) 本 worker $(( FAIL_WINDOW / 60 )) 分钟内有 $OWN_FAILS 个任务开始不久就失败，先不领，过了时间窗再领" ;;
                    nothing|error) echo "$(ts) 没有能领的任务，每 $IDLE_POLL 秒再看" ;;
                    paused) echo "$(ts) 队列暂停领任务（3 小时内失败太多次），空着等" ;;
                    wait) echo "$(ts) 排在前面的任务刚停下，等它（最多到开始后 $(( PATIENCE / 60 )) 分钟）" ;;
                esac
                said_idle=$msg
            fi
            nap "$IDLE_POLL"
            continue
        fi
        if (( OWN_FAILS >= MAX_OWN_FAIL )); then
            echo "$(ts) 本作业失败太多次，作业结束"; break
        elif (( $(left) <= MIN_LEFT )); then
            echo "$(ts) 离时限不到 $(( MIN_LEFT / 60 )) 分钟，作业结束"; break
        elif [[ $why == nothing && $DEVICE == cpu ]]; then
            echo "$(ts) 没有在训的，也没有能领的任务，作业结束"; break
        elif [[ $why == nothing ]] && (( now - idle_since >= IDLE_MAX )); then
            echo "$(ts) 空等 $(( (now - idle_since) / 60 )) 分钟没有能领的任务，作业结束、把卡还回去"; break
        elif [[ $why == error ]] && (( now - err_since >= ERR_MAX )); then
            echo "$(ts) 队列连续出错 $(( (now - err_since) / 60 )) 分钟（$(tail -n 1 "$ERRF")），结束作业、把卡还回去"; break
        elif (( now - idle_since >= IDLE_MAX )); then
            echo "$(ts) 手上没有在训的已经 $(( (now - idle_since) / 60 )) 分钟（$why），结束作业、把卡还回去（DICC：GPU 使用率连续 1 小时低于 40% 会被取消）"
            break
        fi
        # nothing（没有能领的）、wait（等前面刚停下的任务）、paused（队列暂停）、error（还没到结束的时候）：先空着等，最多 IDLE_MAX
        [[ $why == nothing && ${said_nothing:-} != 1 ]] && { echo "$(ts) 没有能领的任务，空着等，每 ${TASKQ_POLL:-30} 秒再看，最多 $(( IDLE_MAX / 60 )) 分钟"; said_nothing=1; }
        [[ $why == paused && ${said_paused:-} != 1 ]] && { echo "$(ts) 队列暂停领任务（3 小时内失败太多次），空着等，最多 $(( IDLE_MAX / 60 )) 分钟"; said_paused=1; }
        [[ $why == wait && ${said_wait:-} != 1 ]] && { echo "$(ts) 排在前面的任务刚停下，等它（最多到开始后 $(( PATIENCE / 60 )) 分钟）"; said_wait=1; }
    fi
    nap
done
kill_tree "$GPUMON"
rm -f "$ERRF"
rm -rf "${TMPDIR:-/tmp}/phd_stage_${SLURM_JOB_ID:-none}"  # run_task.sh 建的本地副本（inputs.py）
# 代码包换了：直接用新包接着跑（同一个 pueue 任务，不等下次检查重开，11/1）；不是代码包的退出，由 feed.py 重开
NEW=$(readlink -f "$(dirname "$REPO")/current" 2> /dev/null)
if [[ -n $RESTART && -L $(dirname "$REPO")/current && -f $NEW/scripts/queue/worker_start.sh ]]; then
    echo "$(ts) 换成新代码 $(basename "$NEW") 接着跑"
    exec bash "$NEW/scripts/queue/worker_start.sh"
fi
[[ -n $RESTART ]] && echo "$(ts) 退出，由 feed.py 用新代码重开"
exit 0
