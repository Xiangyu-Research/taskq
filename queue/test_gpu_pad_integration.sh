#!/bin/bash
# needs-gpu: real GPU pad behavior on GPU 0 of this machine (runs only with taskq self test --gpu; skips itself without nvidia-smi or torch)
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
CORE=$(dirname "$HERE")
export HERE
T=$(mktemp -d)
export T
fails=0
check() { if "${@:2}"; then echo "PASS:$1"; else echo "FAIL:$1"; fails=$((fails + 1)); fi; }
cleanup() {
    [[ -n ${WATCH:-} ]] && kill "$WATCH" 2> /dev/null
    [[ -n ${TRAINPID:-} ]] && kill "$TRAINPID" 2> /dev/null
    stop_pads
    [[ -n ${SUP:-} ]] && kill "$SUP" 2> /dev/null
    [[ -n ${PARENT:-} ]] && kill "$PARENT" 2> /dev/null
    [[ -n ${PARENT_SUP:-} ]] && kill "$PARENT_SUP" 2> /dev/null
    rm -rf "${T:?}"
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

command -v nvidia-smi > /dev/null || { echo "no nvidia-smi on this machine; skipping"; exit 0; }
foreign() {
    local ours p
    ours=" $(our_pids | tr '\n' ' ') "
    for p in $(nvidia-smi -i 0 --query-compute-apps=pid --format=csv,noheader 2> /dev/null | tr -d ' '); do
        [[ $ours == *" $p "* ]] || echo "$p"
    done
}
for _ in $(seq 24); do F=$(foreign); [[ -z $F ]] && break; sleep 5; done
if [[ -n $F ]]; then
    echo "another CUDA process is on the GPU (PID $(echo $F), maybe inference); skipping the integration test"
    exit 0
fi
read -r CONDA_BASE CONDA_ENV < <(python3 "$CORE/sites/mtab.py" get . conda 2> /dev/null) || true
if [[ -f ${CONDA_BASE:-}/etc/profile.d/conda.sh ]]; then
    source "$CONDA_BASE/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV" || { echo "failed to activate conda env $CONDA_ENV; skipping"; exit 0; }
fi
python3 -c "import torch" 2> /dev/null || { echo "python3 has no torch (machine table conda: ${CONDA_BASE:-none} ${CONDA_ENV:-}); skipping"; exit 0; }
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
        echo "aborted: another CUDA process appeared on the GPU during the test (PID $(cat "$T/abort" | tr '\n' ' ')); the pad was stopped; results do not count, run again later"
        exit 0
    fi
}

export TASKQ_DATA=$T/bt
export CUDA_VISIBLE_DEVICES=0
export SLURM_JOB_ID=999001
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

source "$HERE/gpu_pad_start.sh"
SUP=$!
sleep 4
check "pad starts, only one" test -f "$PADLOG"
check "log has target=8000 and settle=4s" bash -c "grep -q 'target=8000' '$PADLOG' && grep -q 'settle=4s' '$PADLOG'"
aborted
check "pads to floor 5000 before training starts (now $(used)MiB)" test "$(used)" -ge 5000 -a "$(used)" -le 5600
start_train
sleep 4
U=$(used)
aborted
check "padded to about 8GB after training settles (now ${U}MiB)" test "$U" -ge 7000
check "does not fill the GPU" test "$U" -le 10000
check "<base>.mib holds the pad size" bash -c "test \$(cat '$T/padsig.mib') -ge 4000"

touch "$T/padsig.start"
sleep 2
U=$(used)
aborted
check "new-training signal drops it to floor 5000 only (now ${U}MiB)" test "$U" -ge 5000 -a "$U" -le 5600
check "writes <base>.ready after making room" test -f "$T/padsig.ready"
sleep 2
U=$(used)
check "new training did not start and start_hold has not passed: not padded to target (now ${U}MiB)" test "$U" -ge 5000 -a "$U" -le 5600
sleep 6
aborted
check "padded again after start_hold passes (now $(used)MiB)" test "$(used)" -ge 7000

stop_train
sleep 2
aborted
U=$(used)
check "training ended: does not release, memory stays at or above floor (now ${U}MiB)" test "$U" -ge 5000 -a "$U" -le 8300

start_train; sleep 4
touch "$T/switch"
sleep 3
stop_train
U=$(used)
aborted
check "DISABLED releases and returns the cache (now ${U}MiB, at start ${BASE}MiB)" test "$U" -le $(( BASE + 300 ))
wait_for 15 pad_gone
check "pad exits after DISABLED" pad_gone
kill "$SUP" 2> /dev/null; SUP=

rm -f "$T/switch"
unset TASKQ_GPU_PAD_STARTED
export SLURM_JOB_ID=999002 TASKQ_GPU_PAD=0
source "$HERE/gpu_pad_start.sh"
sleep 2
check "TASKQ_GPU_PAD=0 does not start" test ! -e "$TASKQ_DATA/logs/workers/gpu_pad_999002.log"
unset TASKQ_GPU_PAD TASKQ_GPU_PAD_STARTED

touch "$T/switch"
export SLURM_JOB_ID=999003
source "$HERE/gpu_pad_start.sh"
sleep 2
check "DISABLED switch takes effect before startup" test ! -e "$TASKQ_DATA/logs/workers/gpu_pad_999003.log"
rm -f "$T/switch"
unset TASKQ_GPU_PAD_STARTED

export SLURM_JOB_ID=999005 TASKQ_GPU_PAD_BASE=$T/padsig5
start_train
bash -c '
    source "$HERE/gpu_pad_start.sh"
    echo $! > "$T/parent_sup"
    sleep 60
' &
PARENT=$!
wait_for 30 filled
PL=$TASKQ_DATA/logs/workers/gpu_pad_999005.log
U=$(used)
aborted
check "parent test: pad first pads to about 8GB (now ${U}MiB)" test "$U" -ge 7000
kill "$PARENT" 2> /dev/null
sleep 3
stop_train
U=$(used)
aborted
check "released after the parent exits (now ${U}MiB, at start ${BASE}MiB)" test "$U" -le $(( BASE + 300 ))
wait_for 15 parent_done
check "log explains the parent exit" grep -q 'parent or supervisor' "$PL"
check "pad and supervisor are both gone after the parent exits" parent_done

aborted
if (( fails == 0 )); then echo "all passed"; else echo "$fails checks failed"; exit 1; fi
