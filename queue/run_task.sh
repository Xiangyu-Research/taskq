#!/bin/bash
# 跑一个训练任务（2026-10-18）：所有机器共用（DICC 的 dicc_worker.sh、lab 的 pueue 任务、手动跑）。
#   run_task.sh <做法> k=v…          （如 run_task.sh nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=1）
# 按任务说明（scripts/queue/spec.py，做法在 scripts/queue/recipes.py）：激活环境（envs/machines.py 的 envs）、设环境变量、
# 在仓库根目录跑命令。命令的输出写进分段日志 <数据目录>/logs/tasks/<key>__<作业>_seg<段>.log，第一行记作业、机器、
# 任务、代码版本（登记表 make_registry.py 读）。作业 = TASK_JOBTAG（dicc_worker.sh 给 Slurm 作业号），否则"主机名-时间"。
# 不在 Slurm 里、并且设了 CUDA_VISIBLE_DEVICES（pueue 的 GPU 组）时，卡被别人占着就等，每 5 分钟看一次；
# 训练出错隔 5 分钟重来，最多 3 次（Slurm 里只跑 1 次，由 dicc_worker.sh 交还队列）。
# 退出码（scripts/queue/README.md 的"约定"）：0 训完；3 命令正常结束但没训完；2 任务说明或环境有问题；143 被停掉；其他 = 训练出错。
# 停这个任务用 SIGTERM（pueue kill -s SIGTERM）：先停训练再退出，下次从存档接着训。
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(dirname "$(dirname "$HERE")")
(( $# >= 1 )) || { echo "用法：run_task.sh <做法> k=v…" >&2; exit 2; }
DATA=${BRAIN_TUMOR:-$(python3 "$REPO/envs/machines.py" get . data)}
[[ -n $DATA ]] || { echo "envs/machines.py 里没有这台机器，先 export BRAIN_TUMOR=<数据目录>" >&2; exit 2; }
export BRAIN_TUMOR=$DATA
SH=$(python3 "$HERE/spec.py" shell "$@") || exit 2
eval "$SH"
ts() { date '+%F %T'; }
done_() { python3 "$HERE/spec.py" done "$@"; }
if done_ "$@"; then echo "$(ts) 已经训完：$TASK_KEY"; exit 0; fi
ACT=$(python3 "$REPO/envs/machines.py" activate "$TASK_ENV") || { echo "这台机器没有环境 $TASK_ENV（envs/machines.py）" >&2; exit 2; }
JOBTAG=${TASK_JOBTAG:-$(hostname -s)-$(date +%Y%m%d%H%M)}
SEG=${TASK_SEG:-1}
mkdir -p "$DATA/logs/tasks" "$TASK_OUT"
LOG=$DATA/logs/tasks/${TASK_SAFE}__${JOBTAG}_seg${SEG}.log
COMMIT=$(cd "$REPO" && git rev-parse --short HEAD 2> /dev/null || echo 未知)
(cd "$REPO" && git diff --quiet HEAD -- scripts/queue research/common) 2> /dev/null || COMMIT="$COMMIT+改动"
TRIES=3
[[ -n ${SLURM_JOB_ID:-} ]] && TRIES=1
TRIES=${RUN_TASK_TRIES:-$TRIES}   # 只给测试用
POLL=${RUN_TASK_POLL:-300}        # 只给测试用

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
kill_tree() { local c; for c in $(pgrep -P "$1"); do kill_tree "$c"; done; kill -TERM "$1" 2> /dev/null; }
pid=
on_term() {
    [[ -n $pid ]] && { echo "$(ts) 收到停止信号，停训练"; kill_tree "$pid"; for _ in $(seq 30); do kill -0 "$pid" 2> /dev/null || break; sleep 1; done; }
    exit 143
}
trap on_term TERM INT
export CUDA_DEVICE_ORDER=PCI_BUS_ID

echo "作业 ${JOBTAG}_${SEG} @ $(hostname -s)，任务=$TASK_KEY，第 $SEG 段，环境 $TASK_ENV，代码 $COMMIT，$(ts)" >> "$LOG"
echo "$(ts) 开始：$TASK_KEY（环境 $TASK_ENV，日志 $LOG）"
for (( try = 1; ; try++ )); do
    while ! gpu_free; do echo "$(ts) 卡 $CUDA_VISIBLE_DEVICES 被占，5 分钟后再看"; sleep "$POLL" & wait $!; done
    ( eval "$ACT" && cd "$REPO" && exec bash -c "$TASK_CMD" ) >> "$LOG" 2>&1 < /dev/null &
    pid=$!
    wait "$pid"; rc=$?; pid=
    if done_ "$@"; then
        echo "训完 $(ts)" >> "$LOG"; echo "$(ts) 训完：$TASK_KEY"; exit 0
    fi
    echo "$(ts) 命令结束（退出码 $rc），没训完（第 $try 次）"
    (( rc == 0 )) && exit 3
    (( try >= TRIES )) && exit "$rc"
    sleep "$POLL" & wait $!
done
