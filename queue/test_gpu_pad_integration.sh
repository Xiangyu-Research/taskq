#!/bin/bash
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
CORE=$(dirname "$HERE")
REPO=${TASKQ_REPO:-$CORE}; [[ -z ${TASKQ_REPO:-} && $CORE == */scripts/taskq ]] && REPO=${CORE%/scripts/taskq}
export REPO
T=$(mktemp -d)
export T
fails=0
check() { if "${@:2}"; then echo "通过：$1"; else echo "失败：$1"; fails=$((fails + 1)); fi; }
cleanup() {
    [[ -n ${WATCH:-} ]] && kill "$WATCH" 2> /dev/null
    [[ -n ${TRAINPID:-} ]] && kill "$TRAINPID" 2> /dev/null
    stop_pads
    [[ -n ${SUP:-} ]] && kill "$SUP" 2> /dev/null
    [[ -n ${PARENT:-} ]] && kill "$PARENT" 2> /dev/null
    [[ -n ${PARENT_SUP:-} ]] && kill "$PARENT_SUP" 2> /dev/null
    rm -rf "$T"
}
mkdir -p "$T/pad"
cp "$HERE/gpu_pad.py" "$HERE/proto.py" "$T/pad/"
cat > "$T/pad/fake_train.py" << 'EOF'
import sys, time, torch
x = torch.zeros(int(sys.argv[1]) * 1024 * 1024, dtype=torch.uint8, device=0)
time.sleep(600)
EOF
export TASKQ_GPU_PAD_SCRIPT=$T/pad/gpu_pad.py
our_pids() { pgrep -f "$T/pad/" 2> /dev/null; }
stop_pads() { local p; for p in $(pgrep -f "$T/pad/gpu_pad.py" 2> /dev/null); do kill "$p" 2> /dev/null; done; }
stop_train() { [[ -n ${TRAINPID:-} ]] && kill "$TRAINPID" 2> /dev/null; TRAINPID=; sleep 2; }
start_train() { python3 "$T/pad/fake_train.py" 2000 & TRAINPID=$!; sleep 6; }
trap cleanup EXIT

command -v nvidia-smi > /dev/null || { echo "这台机器没有 nvidia-smi，跳过"; exit 0; }
foreign() {
    local ours p
    ours=" $(our_pids | tr '\n' ' ') "
    for p in $(nvidia-smi -i 0 --query-compute-apps=pid --format=csv,noheader 2> /dev/null | tr -d ' '); do
        [[ $ours == *" $p "* ]] || echo "$p"
    done
}
for _ in $(seq 24); do F=$(foreign); [[ -z $F ]] && break; sleep 5; done
if [[ -n $F ]]; then
    echo "GPU 上有别的 CUDA 进程（PID $(echo $F)，可能在推理）；跳过集成测试"
    exit 0
fi
read -r CONDA_BASE CONDA_ENV < <(python3 "$REPO/envs/machines.py" get . conda 2> /dev/null) || true
if [[ -f ${CONDA_BASE:-}/etc/profile.d/conda.sh ]]; then
    source "$CONDA_BASE/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV" || { echo "conda 环境 $CONDA_ENV 激活失败，跳过"; exit 0; }
fi
python3 -c "import torch" 2> /dev/null || { echo "python3 没有 torch（envs/machines.py 的 conda：${CONDA_BASE:-没有} ${CONDA_ENV:-}），跳过"; exit 0; }
used() { nvidia-smi -i 0 --query-gpu=memory.used --format=csv,noheader,nounits | tr -d ' '; }
pad_gone() { ! pgrep -f "$T/pad/gpu_pad.py" > /dev/null; }
filled() { test "$(used)" -ge 7000; }
parent_done() { pad_gone && ! kill -0 "$(cat "$T/parent_sup")" 2> /dev/null; }
wait_for() { local n=$1; shift; while (( n-- > 0 )); do "$@" && return 0; sleep 1; done; "$@"; }
BASE=$(used)

(
    while sleep 0.5; do
        F=$(foreign)
        if [[ -n $F ]]; then
            echo "$F" > "$T/abort"
            for p in $(our_pids); do kill "$p" 2> /dev/null; done
            exit 0
        fi
    done
) &
WATCH=$!
aborted() {
    if [[ -f $T/abort ]]; then
        echo "中止：测试中途 GPU 上出现了别的 CUDA 进程（PID $(cat "$T/abort" | tr '\n' ' ')），已停掉 pad；结果不算，之后再跑"
        exit 0
    fi
}

export TASKQ_DATA=$T/bt
export CUDA_VISIBLE_DEVICES=0
export SLURM_JOB_ID=999001
export nnUNet_n_proc_DA=4
export TASKQ_GPU_PAD_SWITCH=$T/switch
export TASKQ_GPU_PAD_BASE=$T/padsig
export TASKQ_GPU_PAD_FLOOR_MIB=5000
export TASKQ_GPU_PAD_TARGET_MIB=8000
export TASKQ_GPU_PAD_HIGH_MIB=9000
export TASKQ_GPU_PAD_MIN_FREE_MIB=2000
export TASKQ_GPU_PAD_MIN_TOTAL_MIB=2000
export TASKQ_GPU_PAD_MIN_TRAIN_MIB=1000
export TASKQ_GPU_PAD_SETTLE=4
export TASKQ_GPU_PAD_START_HOLD=6
export TASKQ_GPU_PAD_INTERVAL=1
export TASKQ_GPU_PAD_CHUNK_MIB=256
PADLOG=$TASKQ_DATA/logs/workers/gpu_pad_999001.log

source "$REPO/envs/nnunet_paths.sh"
SUP=$!
sleep 4
check "pad 启动且只启动一个" test -f "$PADLOG"
check "日志里有 target=8000、settle=4s" bash -c "grep -q 'target=8000' '$PADLOG' && grep -q 'settle=4s' '$PADLOG'"
aborted
check "还没有训练时先填到 floor 5000（现在 $(used)MiB）" test "$(used)" -ge 5000 -a "$(used)" -le 5600
start_train
sleep 4
U=$(used)
aborted
check "训练稳定后补到约 8GB（现在 ${U}MiB）" test "$U" -ge 7000
check "没有把卡占满" test "$U" -le 10000
check "<base>.mib 写了 pad 的大小" bash -c "test \$(cat '$T/padsig.mib') -ge 4000"

touch "$T/padsig.start"
sleep 2
U=$(used)
aborted
check "收到新训练信号后只放到 floor 5000（现在 ${U}MiB）" test "$U" -ge 5000 -a "$U" -le 5600
check "放好后写 <base>.ready" test -f "$T/padsig.ready"
sleep 2
U=$(used)
check "新训练没起来、start_hold 没到：不补到 target（现在 ${U}MiB）" test "$U" -ge 5000 -a "$U" -le 5600
sleep 6
aborted
check "等满 start_hold 后又补上（现在 $(used)MiB）" test "$(used)" -ge 7000

stop_train
sleep 2
aborted
U=$(used)
check "训练结束后不放掉，显存不低于 floor（现在 ${U}MiB）" test "$U" -ge 5000 -a "$U" -le 8300

start_train; sleep 4
touch "$T/switch"
sleep 3
stop_train
U=$(used)
aborted
check "DISABLED 后释放、缓存也还了（现在 ${U}MiB，开始时 ${BASE}MiB）" test "$U" -le $(( BASE + 300 ))
wait_for 15 pad_gone
check "DISABLED 后 pad 退出" pad_gone
kill "$SUP" 2> /dev/null; SUP=

rm -f "$T/switch"
unset TASKQ_GPU_PAD_STARTED
export SLURM_JOB_ID=999002 TASKQ_GPU_PAD=0
source "$REPO/envs/nnunet_paths.sh"
sleep 2
check "TASKQ_GPU_PAD=0 不启动" test ! -e "$TASKQ_DATA/logs/workers/gpu_pad_999002.log"
unset TASKQ_GPU_PAD TASKQ_GPU_PAD_STARTED

touch "$T/switch"
export SLURM_JOB_ID=999003
source "$REPO/envs/nnunet_paths.sh"
sleep 2
check "DISABLED 开关在启动前生效" test ! -e "$TASKQ_DATA/logs/workers/gpu_pad_999003.log"
rm -f "$T/switch"
unset TASKQ_GPU_PAD_STARTED

export SLURM_JOB_ID=999005 TASKQ_GPU_PAD_BASE=$T/padsig5
start_train
bash -c '
    source "$REPO/envs/nnunet_paths.sh"
    echo $! > "$T/parent_sup"
    sleep 60
' &
PARENT=$!
wait_for 30 filled
PL=$TASKQ_DATA/logs/workers/gpu_pad_999005.log
U=$(used)
aborted
check "父进程测试：pad 先补到约 8GB（现在 ${U}MiB）" test "$U" -ge 7000
kill "$PARENT" 2> /dev/null
sleep 3
stop_train
U=$(used)
aborted
check "父进程退出后释放（现在 ${U}MiB，开始时 ${BASE}MiB）" test "$U" -le $(( BASE + 300 ))
wait_for 15 parent_done
check "父进程退出后日志有说明" grep -q '父进程' "$PL"
check "父进程退出后 pad 和 supervisor 都不在了" parent_done

aborted
if (( fails == 0 )); then echo "全部通过"; else echo "$fails 项失败"; exit 1; fi
