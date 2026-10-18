#!/bin/bash
# box 上的真卡集成测试（2026-10-17；10/18 改成不分模型的版本，用一个占 2GB 的假训练）：用真实 envs/nnunet_paths.sh、真实 GPU、真实 torch/nvidia-smi 跑 gpu_pad.py。
# 不连 DICC，不提交任何作业。为了不把 12GB 的卡占满，目标降到 8GB。
#   bash scripts/queue/test_gpu_pad_integration.sh      （最好排进 box 的 gpu 组：q gpu --label test_gpu_pad -- 'bash …'）
# 卡上有别的 CUDA 进程时跳过；测试中途出现别的进程（比如推理开始了）就立刻停掉 pad、中止测试，不让推理 OOM。
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(git -C "$HERE" rev-parse --show-toplevel)
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
# pad 用测试目录里的副本启动，命令行里带 $T，才能和别的 python 进程分开（只杀自己的）。
mkdir -p "$T/pad"
cp "$HERE/gpu_pad.py" "$T/pad/"
# 假训练：占 2000MiB 显存后睡着（命令行里带 $T，算本测试自己的进程）
cat > "$T/pad/fake_train.py" << 'EOF'
import sys, time, torch
x = torch.zeros(int(sys.argv[1]) * 1024 * 1024, dtype=torch.uint8, device=0)
time.sleep(600)
EOF
export RQ1_GPU_PAD_SCRIPT=$T/pad/gpu_pad.py
our_pids() { pgrep -f "$T/pad/" 2> /dev/null; }
stop_pads() { local p; for p in $(pgrep -f "$T/pad/gpu_pad.py" 2> /dev/null); do kill "$p" 2> /dev/null; done; }
stop_train() { [[ -n ${TRAINPID:-} ]] && kill "$TRAINPID" 2> /dev/null; TRAINPID=; sleep 2; }
start_train() { python3 "$T/pad/fake_train.py" 2000 & TRAINPID=$!; sleep 6; }
trap cleanup EXIT

command -v nvidia-smi > /dev/null || { echo "这台机器没有 nvidia-smi，跳过"; exit 0; }
# 卡上不是本测试 pad 的 CUDA 进程
foreign() {
    local ours p
    ours=" $(our_pids | tr '\n' ' ') "
    for p in $(nvidia-smi -i 0 --query-compute-apps=pid --format=csv,noheader 2> /dev/null | tr -d ' '); do
        [[ $ours == *" $p "* ]] || echo "$p"
    done
}
F=$(foreign)
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
BASE=$(used)

# 全程看着：一出现别的 CUDA 进程就停掉 pad，测试中止
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

export BRAIN_TUMOR=$T/bt
export CUDA_VISIBLE_DEVICES=0
export SLURM_JOB_ID=999001
export nnUNet_n_proc_DA=4
export RQ1_GPU_PAD_SWITCH=$T/switch
export RQ1_GPU_PAD_BASE=$T/padsig
export RQ1_GPU_PAD_TARGET_MIB=8000
export RQ1_GPU_PAD_HIGH_MIB=9000
export RQ1_GPU_PAD_MIN_FREE_MIB=2000
export RQ1_GPU_PAD_MIN_TOTAL_MIB=2000
export RQ1_GPU_PAD_MIN_TRAIN_MIB=1000
export RQ1_GPU_PAD_SETTLE=4
export RQ1_GPU_PAD_START_HOLD=6
export RQ1_GPU_PAD_INTERVAL=1
export RQ1_GPU_PAD_CHUNK_MIB=256
PADLOG=$BRAIN_TUMOR/nnUNet/results/logs/gpu_pad_999001.log

# 1. 没有训练时不填；训练来了、稳定 4 秒后补到约 8GB
source "$REPO/envs/nnunet_paths.sh"
SUP=$!
sleep 4
check "pad 启动且只启动一个" test -f "$PADLOG"
check "日志里有 target=8000、settle=4s" bash -c "grep -q 'target=8000' '$PADLOG' && grep -q 'settle=4s' '$PADLOG'"
aborted
check "没有训练时不占显存（现在 $(used)MiB，开始时 ${BASE}MiB）" test "$(used)" -le $(( BASE + 800 ))
start_train
sleep 4
U=$(used)
aborted
check "训练稳定后补到约 8GB（现在 ${U}MiB）" test "$U" -ge 7000
check "没有把卡占满" test "$U" -le 10000
check "<base>.mib 写了 pad 的大小" bash -c "test \$(cat '$T/padsig.mib') -ge 4000"

# 2. 要开始新训练（touch <base>.start）：全部放掉，<base>.mib 变成 0；假训练的显存没涨（像新训练还没起来），
#    等满 start_hold（这里 6 秒）后照常补
touch "$T/padsig.start"
sleep 2
U=$(used)
aborted
check "收到新训练信号后放掉（现在 ${U}MiB）" test "$U" -le $(( BASE + 3300 ))
check "<base>.mib 变成 0" test "$(cat "$T/padsig.mib")" = 0
sleep 2
check "新训练没起来、start_hold 没到：不补（现在 $(used)MiB）" test "$(used)" -le $(( BASE + 3300 ))
sleep 6
aborted
check "等满 start_hold 后又补上（现在 $(used)MiB）" test "$(used)" -ge 7000

# 3. 训练没了：释放
stop_train
sleep 2
aborted
check "训练结束后释放（现在 $(used)MiB，开始时 ${BASE}MiB）" test "$(used)" -le $(( BASE + 800 ))

# 4. DISABLED：释放并退出
start_train; sleep 4
touch "$T/switch"
sleep 3
stop_train
U=$(used)
aborted
check "DISABLED 后释放、缓存也还了（现在 ${U}MiB，开始时 ${BASE}MiB）" test "$U" -le $(( BASE + 300 ))
check "DISABLED 后 pad 退出" bash -c "! pgrep -f '$T/pad/gpu_pad.py' > /dev/null"
kill "$SUP" 2> /dev/null; SUP=

# 5. RQ1_GPU_PAD=0：不启动
rm -f "$T/switch"
unset RQ1_GPU_PAD_STARTED
export SLURM_JOB_ID=999002 RQ1_GPU_PAD=0
source "$REPO/envs/nnunet_paths.sh"
sleep 2
check "RQ1_GPU_PAD=0 不启动" test ! -e "$BRAIN_TUMOR/nnUNet/results/logs/gpu_pad_999002.log"
unset RQ1_GPU_PAD RQ1_GPU_PAD_STARTED

# 6. DISABLED 开关在启动前就不启动
touch "$T/switch"
export SLURM_JOB_ID=999003
source "$REPO/envs/nnunet_paths.sh"
sleep 2
check "DISABLED 开关在启动前生效" test ! -e "$BRAIN_TUMOR/nnUNet/results/logs/gpu_pad_999003.log"
rm -f "$T/switch"
unset RQ1_GPU_PAD_STARTED

# 7. 父进程退出：pad 释放并退出（用 bash -c，让 $$ 就是它自己的 PID，和真实批处理一样）
export SLURM_JOB_ID=999005 RQ1_GPU_PAD_BASE=$T/padsig5
start_train
bash -c '
    source "$REPO/envs/nnunet_paths.sh"
    echo $! > "$T/parent_sup"
    sleep 60
' &
PARENT=$!
sleep 8
PL=$BRAIN_TUMOR/nnUNet/results/logs/gpu_pad_999005.log
U=$(used)
aborted
check "父进程测试：pad 先补到约 8GB（现在 ${U}MiB）" test "$U" -ge 7000
kill "$PARENT" 2> /dev/null
sleep 3
stop_train
U=$(used)
aborted
check "父进程退出后释放（现在 ${U}MiB，开始时 ${BASE}MiB）" test "$U" -le $(( BASE + 300 ))
check "父进程退出后日志有说明" grep -q '父进程' "$PL"
check "父进程退出后 pad 和 supervisor 都不在了" bash -c "! pgrep -f '$T/pad/gpu_pad.py' > /dev/null && ! kill -0 \$(cat '$T/parent_sup') 2> /dev/null"

aborted
if (( fails == 0 )); then echo "全部通过"; else echo "$fails 项失败"; exit 1; fi
