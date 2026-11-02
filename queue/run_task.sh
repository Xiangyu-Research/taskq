#!/bin/bash
# 跑一个训练任务（2026-10-18）：所有机器共用（DICC 的 dicc_worker.sh、lab 的 pueue 任务、手动跑）。
#   run_task.sh <做法> k=v…          （如 run_task.sh command key=demo/a 'cmd=python3 train.py' env=<环境> out=demo/a done=DONE）
# 按任务说明（scripts/queue/spec.py，做法在 scripts/queue/recipes.py）：激活环境（envs/machines.py 的 envs）、设环境变量、
# 在仓库根目录跑命令。命令的输出写进分段日志 <数据目录>/logs/tasks/<key>__<作业>_part<段>.log（段号 TASK_PART；10/31 前的 worker
# 传 TASK_SEG，日志照旧叫 _seg<段>.log，它按这个名字看日志），第一行记作业、机器、
# 任务、代码版本（登记表 make_registry.py 读）。作业 = TASK_JOBTAG（dicc_worker.sh 给：Slurm 里是作业号，lab 上是"主机名-时间"），否则"主机名-时间"。
# 不在 Slurm 里、并且设了 CUDA_VISIBLE_DEVICES（pueue 的 GPU 组）时，卡被别人占着就等，每分钟看一次；任务的 res.wait_gpu
# 给了分钟数时最多等这么久，等不到算出错（退出码 1），没给就一直等（10/31）；
# 训练出错隔 5 分钟重来，最多 3 次（dicc_worker.sh 跑的只跑 1 次，由它交还队列）。
# 退出码（scripts/queue/README.md 的"约定"）：0 训完；3 命令正常结束但没训完；2 任务说明或环境有问题；1 训练出错（命令的退出码记在分段日志）；
# 4 缺输入（data 里的文件不全，没开训，11/1）；
# ≥128 被信号停掉（自己收到 SIGTERM 是 143，训练命令被杀原样传出）。
# 停这个任务用 SIGTERM（pueue kill -s SIGTERM）：先停训练（8 秒没停下就强制停）再退出，下次从存档接着训。
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(dirname "$(dirname "$HERE")")
(( $# >= 1 )) || { echo "用法：run_task.sh <做法> k=v…" >&2; exit 2; }
source "$HERE/legacy_names.sh" map
DATA=${TASKQ_DATA:-$(python3 "${TASKQ_CONFIG:-$REPO/envs}/machines.py" get . data)}
[[ -n $DATA ]] || { echo "envs/machines.py 里没有这台机器，先 export TASKQ_DATA=<数据目录>" >&2; exit 2; }
export TASKQ_DATA=$DATA
source "$HERE/legacy_names.sh" export
SH=$(python3 "$HERE/spec.py" shell "$@") || exit 2
eval "$SH"
ts() { date '+%F %T'; }
done_() { python3 "$HERE/spec.py" done "$@"; }
if done_ "$@"; then echo "$(ts) 已经训完：$TASK_KEY"; exit 0; fi
ACT=$(python3 "${TASKQ_CONFIG:-$REPO/envs}/machines.py" activate "$TASK_ENV") || { echo "这台机器没有环境 $TASK_ENV（envs/machines.py）" >&2; exit 2; }
JOBTAG=${TASK_JOBTAG:-$(hostname -s)-$(date +%Y%m%d%H%M)}
if [[ -n ${TASK_PART:-} ]]; then PART=$TASK_PART PSUF=part
elif [[ -n ${TASK_SEG:-} ]]; then PART=$TASK_SEG PSUF=seg
else PART=1 PSUF=part; fi
mkdir -p "$DATA/logs/tasks" "$TASK_OUT"
LOG=$DATA/logs/tasks/${TASK_SAFE}__${JOBTAG}_${PSUF}${PART}.log
if [[ $(git -C "$REPO" rev-parse --show-toplevel 2> /dev/null) == "$(readlink -f "$REPO")" ]]; then
    COMMIT=$(git -C "$REPO" rev-parse --short HEAD 2> /dev/null || echo 未知)
    CODE_DIRS=$(python3 "$HERE/cfg.py" get code_dirs 2> /dev/null)
    git -C "$REPO" diff --quiet HEAD -- ${CODE_DIRS:-scripts/queue} 2> /dev/null || COMMIT="$COMMIT+改动"
else
    COMMIT=$(basename "$(readlink -f "$REPO")")  # 代码包（relay_bundle.py）：包名 <提交号>-<内容哈希>
fi
TRIES=3
[[ -n ${SLURM_JOB_ID:-}${TASK_JOBTAG:-} ]] && TRIES=1  # Slurm 里、dicc_worker.sh 跑的（设了 TASK_JOBTAG）只试一次，失败交给 worker
TRIES=${RUN_TASK_TRIES:-$TRIES}   # 只给测试用
POLL=${RUN_TASK_POLL:-300}        # 只给测试用
GPU_POLL=${RUN_TASK_GPU_POLL:-60} # 只给测试用
STOP_WAIT=${RUN_TASK_STOP_WAIT:-8} # 只给测试用

gpu_free() {  # 卡上没有别人的进程、别人占的显存 ≤ 500 MiB（自己账号的进程不算，10/19 起）
    [[ -z ${CUDA_VISIBLE_DEVICES:-} || -n ${SLURM_JOB_ID:-} ]] && return 0
    local used mine=0 pid pm
    used=$(nvidia-smi -i "$CUDA_VISIBLE_DEVICES" --query-gpu=memory.used --format=csv,noheader,nounits 2> /dev/null | tr -d ' ')
    while IFS=', ' read -r pid pm; do
        [[ -z $pid ]] && continue
        [[ $(ps -o user= -p "$pid" 2> /dev/null | tr -d ' ') == "$(id -un)" ]] || return 1
        mine=$(( mine + ${pm:-0} ))
    done < <(nvidia-smi -i "$CUDA_VISIBLE_DEVICES" --query-compute-apps=pid,used_memory --format=csv,noheader,nounits 2> /dev/null)
    (( ${used:-0} - mine <= 500 ))
}
wait_gpu() {  # 卡空出来 → 0；等满 TASK_WAIT_GPU 分钟还被占 → 1。每次看都往任务日志写一行（dicc_worker.sh 按日志判断卡没卡住）
    local waited=0
    while ! gpu_free; do
        if [[ -n ${TASK_WAIT_GPU:-} ]] && (( waited >= TASK_WAIT_GPU * 60 )); then
            echo "$(ts) 卡 $CUDA_VISIBLE_DEVICES 被别人占着，等了 $TASK_WAIT_GPU 分钟还没空出来，不跑" | tee -a "$LOG"
            return 1
        fi
        echo "$(ts) 卡 $CUDA_VISIBLE_DEVICES 被别人占着，等它空出来（已等 $(( waited / 60 )) 分钟${TASK_WAIT_GPU:+，最多 $TASK_WAIT_GPU 分钟}）" | tee -a "$LOG"
        sleep "$GPU_POLL" & wait $!
        waited=$(( waited + GPU_POLL ))
    done
}
kill_tree() { local c s=${2:-TERM}; for c in $(pgrep -P "$1"); do kill_tree "$c" "$s"; done; kill -"$s" "$1" 2> /dev/null; }
gone() { ! kill -0 -- -"$1" 2> /dev/null && ! kill -0 "$1" 2> /dev/null; }  # 训练的进程组和它本身都不在了
pid=
on_term() {  # 先 SIGTERM 整个进程组，8 秒没停下就 SIGKILL（存档是原子写的），确认停下了才退出（11/1）
    if [[ -n $pid ]]; then
        echo "$(ts) 收到停止信号，停训练"
        kill -TERM -- -"$pid" 2> /dev/null; kill_tree "$pid"
        for _ in $(seq "$STOP_WAIT"); do gone "$pid" && break; sleep 1; done
        if ! gone "$pid"; then
            echo "$(ts) 训练 $STOP_WAIT 秒没停下，强制停" | tee -a "$LOG"
            kill -KILL -- -"$pid" 2> /dev/null; kill_tree "$pid" KILL
            for _ in 1 2 3; do gone "$pid" && break; sleep 1; done
        fi
        gone "$pid" || echo "$(ts) 训练进程强制停之后还在（多半卡在读写上）" | tee -a "$LOG"
    fi
    exit 143
}
trap on_term TERM INT
export CUDA_DEVICE_ORDER=PCI_BUS_ID

echo "作业 ${JOBTAG}_${PART} @ $(hostname -s)，任务=$TASK_KEY，第 $PART 段，环境 $TASK_ENV，代码 $COMMIT，$(ts)" >> "$LOG"
echo "$(ts) 开始：$TASK_KEY（环境 $TASK_ENV，日志 $LOG）"
# 输入（任务说明的 data）：开训前查全没有，不全退出码 4；STAGE_TMP 开着、任务的 res.local_inputs 时在节点本地盘建私有视图，
# 后台拷过去（scripts/queue/inputs.py，11/1）。命令从 TASK_INPUTS_ROOT 读输入（和数据目录一样的相对路径）
TASK_INPUTS_ROOT=$DATA CPID=
if (( ${#TASK_INPUTS[@]} )); then
    LROOT=
    if [[ ${TASK_LOCAL:-0} == 1 && -n ${SLURM_JOB_ID:-} ]]; then
        ST=${TASKQ_STAGE_TMP:-$(python3 -c "import sys; sys.path.insert(0, '$REPO/envs'); import machines, policy; print(int(bool(policy.STAGE_TMP.get(machines.whoami()))))" 2> /dev/null || echo 0)}
        [[ $ST == 1 ]] && LROOT=${TMPDIR:-/tmp}/phd_stage_$SLURM_JOB_ID
    fi
    V=$(python3 "$HERE/inputs.py" prepare "$DATA" "$TASK_SAFE" "$LROOT" "${TASK_INPUTS[@]}" 2>> "$LOG"); rc=$?
    if (( rc == 4 )); then
        echo "$(ts) 不开训：$(tail -n 1 "$LOG")"
        exit 4
    elif (( rc == 0 )) && [[ -n $V ]]; then
        TASK_INPUTS_ROOT=$V
        if [[ $V != "$DATA" ]]; then
            python3 "$HERE/inputs.py" copy "$LROOT" "$TASK_SAFE" >> "$LOG" 2>&1 &
            CPID=$!
            echo "$(ts) 输入读 $V（后台拷到本地盘）" | tee -a "$LOG"
        fi
    else
        echo "$(ts) 输入没查成（退出码 $rc），照旧读 $DATA" | tee -a "$LOG"
    fi
fi
export TASK_INPUTS_ROOT
trap '[[ -n $CPID ]] && kill "$CPID" 2> /dev/null' EXIT
for (( try = 1; ; try++ )); do
    wait_gpu || exit 1
    ( eval "$ACT" && cd "$REPO" && exec setsid -w bash -c "$TASK_CMD" ) >> "$LOG" 2>&1 < /dev/null &  # 自己一个进程组，停的时候整组停
    pid=$!
    wait "$pid"; rc=$?; pid=
    if done_ "$@"; then
        echo "训完 $(ts)" >> "$LOG"; echo "$(ts) 训完：$TASK_KEY"; exit 0
    fi
    echo "$(ts) 命令结束（退出码 $rc），没训完（第 $try 次）" | tee -a "$LOG"
    (( rc == 0 )) && exit 3
    (( try >= TRIES )) && exit $(( rc >= 128 ? rc : 1 ))
    sleep "$POLL" & wait $!
done
