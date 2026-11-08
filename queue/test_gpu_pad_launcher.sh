#!/bin/bash
# GPU pad launcher (gpu_pad_start.sh): when sourced through an env script (policy.py's OUTPUTS["env_script"], testdata/env.sh here),
# when it starts, that it starts only once, and that it restarts after a failure.  bash queue/test_gpu_pad_launcher.sh
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
CORE=$(dirname "$HERE")
REPO=${TASKQ_REPO:-$CORE}; [[ -z ${TASKQ_REPO:-} && $CORE == */scripts/taskq ]] && REPO=${CORE%/scripts/taskq}
source "$HERE/testdata/fixture.sh"
ENV_SCRIPT=$REPO/$(python3 "$HERE/cfg.py" get env_script)
T=$(mktemp -d)
trap 'stop_pads; rm -rf "${T:?}"' EXIT
SWITCH=$(TASKQ_ROOT=$T/root python3 "$HERE/cfg.py" state gpu_pad/DISABLED)  # executor master switch: under queue/ in the root
fails=0
check() {
    if "${@:2}"; then echo "PASS:$1"; else echo "FAIL:$1"; fails=$((fails + 1)); fi
}

mkdir -p "$T/bin" "$T/nobin" "$T/bt"
for c in bash sh python3 dirname mkdir date nohup nice sleep cat tr env wc; do
    src=$(command -v "$c" 2> /dev/null) || continue
    ln -s "$src" "$T/bin/$c"
    ln -s "$src" "$T/nobin/$c"
done
printf '#!/bin/bash\nexit 0\n' > "$T/bin/nvidia-smi"
chmod +x "$T/bin/nvidia-smi"
cat > "$T/fake_pad.py" <<'EOF'
import os
import sys
import time
with open(os.environ["FAKE_PAD_ARGS"], "a") as f:
    f.write(" ".join(sys.argv[1:]) + f" pid={os.getpid()} ppid={os.getppid()}\n")
if os.environ.get("FAKE_PAD_FAIL_ONCE") == "1" and not os.path.exists(os.environ["FAKE_PAD_FAILED"]):
    open(os.environ["FAKE_PAD_FAILED"], "w").close()
    sys.exit(1)
if os.environ.get("FAKE_PAD_FAIL") == "1":
    sys.exit(1)
time.sleep(60)
EOF

source_it() {
    local path=$1; shift
    (
        export PATH=$path TASKQ_DATA=$T/bt TASKQ_GPU_PAD_SCRIPT=$T/fake_pad.py \
               TASKQ_ROOT=$T/root TASKQ_GPU_PAD_RETRY_DELAY=0.1 \
               FAKE_PAD_ARGS=$T/args FAKE_PAD_FAILED=$T/failed \
               TASKQ_GPU_PAD_FLOOR_MIB=1 TASKQ_GPU_PAD_TARGET_MIB=2 TASKQ_GPU_PAD_HIGH_MIB=3
        local kv
        for kv in "$@"; do export "$kv"; done
        source "$ENV_SCRIPT"
    )
}
source_it_twice() {
    (
        export PATH=$T/bin TASKQ_DATA=$T/bt TASKQ_GPU_PAD_SCRIPT=$T/fake_pad.py \
               TASKQ_ROOT=$T/root TASKQ_GPU_PAD_RETRY_DELAY=0.1 \
               FAKE_PAD_ARGS=$T/args FAKE_PAD_FAILED=$T/failed \
               SLURM_JOB_ID=2001 CUDA_VISIBLE_DEVICES=0 \
               TASKQ_GPU_PAD_FLOOR_MIB=1 TASKQ_GPU_PAD_TARGET_MIB=2 TASKQ_GPU_PAD_HIGH_MIB=3
        source "$ENV_SCRIPT"
        source "$ENV_SCRIPT"
    )
}
stop_pads() {
    local p
    for p in $(pgrep -f "$T/fake_pad.py" 2> /dev/null); do kill "$p" 2> /dev/null; done
    for p in $(pgrep -f "bash -c.*$T/fake_pad.py" 2> /dev/null); do kill "$p" 2> /dev/null; done
    sleep 0.1
}
lines() { if [[ -f $T/args ]]; then wc -l < "$T/args"; else echo 0; fi; }
waitfor() { local i; for (( i = 0; i < $1 * 10; i++ )); do eval "$2" && return 0; sleep 0.1; done; return 1; }  # seconds condition
reset() { stop_pads; rm -f "$T/args" "$T/failed" "$SWITCH"; }

reset
source_it "$T/bin" SLURM_JOB_ID=1001 CUDA_VISIBLE_DEVICES=0
waitfor 20 '(( $(lines) >= 1 ))'; sleep 0.3
check "starts one pad" test "$(lines)" -eq 1
check "passes only parent-pid and supervisor-pid (the pad does not know models)" grep -Eq '^--parent-pid [0-9]+ --supervisor-pid [0-9]+ pid=' "$T/args"

reset
mkdir -p "$T/badbin"
printf '#!/bin/bash\necho bad >> "%s/badpy"\nexit 1\n' "$T" > "$T/badbin/python3"
chmod +x "$T/badbin/python3"
printf 'PATH=%s/badbin:$PATH\n' "$T" > "$T/bashenv"
source_it "$T/bin" SLURM_JOB_ID=1003 CUDA_VISIBLE_DEVICES=0 BASH_ENV="$T/bashenv"
waitfor 20 '(( $(lines) >= 1 ))'
check "a child bash changing PATH still uses the python3 from startup" bash -c "test \$(wc -l < '$T/args') -eq 1 && test ! -e '$T/badpy'"
unset BASH_ENV

reset; source_it "$T/bin" CUDA_VISIBLE_DEVICES=0
sleep 0.3; check "no SLURM_JOB_ID: does not start" test "$(lines)" -eq 0
reset; source_it "$T/bin" SLURM_JOB_ID=1003
sleep 0.3; check "no CUDA_VISIBLE_DEVICES: does not start" test "$(lines)" -eq 0
reset; source_it "$T/bin" SLURM_JOB_ID=1004 CUDA_VISIBLE_DEVICES=0 SLURM_STEP_ID=1
sleep 0.3; check "srun step: does not start" test "$(lines)" -eq 0
reset; source_it "$T/bin" SLURM_JOB_ID=1005 CUDA_VISIBLE_DEVICES=0 TASKQ_TEST_FAKEBIN=1
sleep 0.3; check "test env TASKQ_TEST_FAKEBIN: does not start" test "$(lines)" -eq 0
reset; source_it "$T/bin" SLURM_JOB_ID=1006 CUDA_VISIBLE_DEVICES=0 QUEUE_TEST_DIR=$T
sleep 0.3; check "test env QUEUE_TEST_DIR: does not start" test "$(lines)" -eq 0
printf 'MACHINES = {"m": dict(label="m", hostname=r".*", ssh=None, scheduler="slurm", gpu_pad=(1, 2, 3))}\n' > "$T/machines_pad.py"
mkdir -p "$T/conf_pad" "$T/conf_nopad"; mv "$T/machines_pad.py" "$T/conf_pad/machines.py"
sed 's/, gpu_pad=(1, 2, 3)//' "$T/conf_pad/machines.py" > "$T/conf_nopad/machines.py"
reset; source_it "$T/bin" SLURM_JOB_ID=1014 CUDA_VISIBLE_DEVICES=0 TASKQ_GPU_PAD_FLOOR_MIB= TASKQ_GPU_PAD_TARGET_MIB= TASKQ_GPU_PAD_HIGH_MIB= TASKQ_CONFIG=$T/conf_nopad
sleep 0.3; check "no gpu_pad in the machine table and no thresholds in the environment: does not start" test "$(lines)" -eq 0
reset; source_it "$T/bin" SLURM_JOB_ID=1015 CUDA_VISIBLE_DEVICES=0 TASKQ_GPU_PAD_FLOOR_MIB= TASKQ_GPU_PAD_TARGET_MIB= TASKQ_GPU_PAD_HIGH_MIB= TASKQ_CONFIG=$T/conf_pad
waitfor 20 '(( $(lines) >= 1 ))'; sleep 0.3
check "thresholds from the machine table gpu_pad: starts" test "$(lines)" -eq 1
reset; source_it "$T/bin" SLURM_JOB_ID=1007 CUDA_VISIBLE_DEVICES=0 TASKQ_GPU_PAD=0
sleep 0.3; check "TASKQ_GPU_PAD=0: does not start" test "$(lines)" -eq 0
reset; mkdir -p "$(dirname "$SWITCH")"; touch "$SWITCH"
source_it "$T/bin" SLURM_JOB_ID=1008 CUDA_VISIBLE_DEVICES=0
sleep 0.3; check "DISABLED switch: does not start" test "$(lines)" -eq 0
reset; source_it "$T/nobin" SLURM_JOB_ID=1009 CUDA_VISIBLE_DEVICES=0
sleep 0.3; check "no nvidia-smi: does not start" test "$(lines)" -eq 0
reset; out=$(source_it "$T/bin" SLURM_JOB_ID=1013 CUDA_VISIBLE_DEVICES=0 TASKQ_GPU_PAD_SCRIPT=$T/nope.py 2>&1)
sleep 0.3; check "pad script missing: does not start, writes one line to the job log" bash -c "test $(lines) -eq 0 && grep -q 'GPU pad not started: cannot find' <<< '$out'"

reset
source_it_twice
waitfor 20 '(( $(lines) >= 1 ))'; sleep 0.3
check "sourcing twice starts it only once" test "$(lines)" -eq 1

reset
source_it "$T/bin" SLURM_JOB_ID=1010 CUDA_VISIBLE_DEVICES=0 FAKE_PAD_FAIL_ONCE=1 > "$T/wlog"
waitfor 20 '(( $(lines) >= 2 ))'; sleep 0.3
check "supervisor restarts the pad after one failure" test "$(lines)" -eq 2
check "restart written to the job log (when it is a file)" grep -q 'GPU pad exited (exit code 1); restarting in 1 x 0.1 seconds' "$T/wlog"

reset
source_it "$T/bin" SLURM_JOB_ID=1011 CUDA_VISIBLE_DEVICES=0 FAKE_PAD_FAIL=1 TASKQ_GPU_PAD_MAX_FAILS=3 > "$T/wlog"
waitfor 20 'grep -q "not restarting" "$T/wlog"'; sleep 0.5
check "never starts: gives up after 3 in a row, the job log says there is no GPU pad" \
      bash -c "test \$(wc -l < '$T/args') -eq 3 && grep -q '!! GPU pad failed to start 3 times in a row (exit code 1); not restarting, this job has no GPU pad' '$T/wlog'"
check "restart delay doubles" grep -q 'restarting in 2 x 0.1 seconds' "$T/wlog"

reset
source_it "$T/bin" SLURM_JOB_ID=1012 CUDA_VISIBLE_DEVICES=0 FAKE_PAD_FAIL=1 TASKQ_GPU_PAD_MAX_FAILS=2 TASKQ_GPU_PAD_RESET_AFTER=0 > "$T/wlog"
waitfor 20 '(( $(lines) >= 5 ))'
check "runs that lasted past RESET_AFTER: keep restarting, never give up" bash -c "test \$(wc -l < '$T/args') -ge 5 && ! grep -q \"not restarting\" '$T/wlog'"

reset
if (( fails == 0 )); then echo "all passed"; else echo "$fails checks failed"; exit 1; fi
