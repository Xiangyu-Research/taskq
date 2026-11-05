#!/bin/bash
# 显存填充的启动器（gpu_pad_start.sh）：经环境脚本（policy.py 的 OUTPUTS["env_script"]，这里是测试用的 testdata/env.sh）source 时
# 什么情况下启动、只启动一次、失败重启。  bash queue/test_gpu_pad_launcher.sh
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
CORE=$(dirname "$HERE")
REPO=${TASKQ_REPO:-$CORE}; [[ -z ${TASKQ_REPO:-} && $CORE == */scripts/taskq ]] && REPO=${CORE%/scripts/taskq}
source "$HERE/testdata/fixture.sh"
ENV_SCRIPT=$REPO/$(python3 "$HERE/cfg.py" get env_script)
T=$(mktemp -d)
trap 'stop_pads; rm -rf "${T:?}"' EXIT
fails=0
check() {
    if "${@:2}"; then echo "通过：$1"; else echo "失败：$1"; fails=$((fails + 1)); fi
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
               TASKQ_GPU_PAD_SWITCH=$T/switch TASKQ_GPU_PAD_RETRY_DELAY=0.1 \
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
               TASKQ_GPU_PAD_SWITCH=$T/switch TASKQ_GPU_PAD_RETRY_DELAY=0.1 \
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
waitfor() { local i; for (( i = 0; i < $1 * 10; i++ )); do eval "$2" && return 0; sleep 0.1; done; return 1; }  # 秒数 条件
reset() { stop_pads; rm -f "$T/args" "$T/failed" "$T/switch"; }

reset
source_it "$T/bin" SLURM_JOB_ID=1001 CUDA_VISIBLE_DEVICES=0
waitfor 20 '(( $(lines) >= 1 ))'; sleep 0.3
check "正常启动一个 pad" test "$(lines)" -eq 1
check "只传 parent-pid 和 supervisor-pid（pad 不分模型）" grep -Eq '^--parent-pid [0-9]+ --supervisor-pid [0-9]+ pid=' "$T/args"

reset
mkdir -p "$T/badbin"
printf '#!/bin/bash\necho bad >> "%s/badpy"\nexit 1\n' "$T" > "$T/badbin/python3"
chmod +x "$T/badbin/python3"
printf 'PATH=%s/badbin:$PATH\n' "$T" > "$T/bashenv"
source_it "$T/bin" SLURM_JOB_ID=1003 CUDA_VISIBLE_DEVICES=0 BASH_ENV="$T/bashenv"
waitfor 20 '(( $(lines) >= 1 ))'
check "子 bash 换了 PATH 也用启动时的 python3" bash -c "test \$(wc -l < '$T/args') -eq 1 && test ! -e '$T/badpy'"
unset BASH_ENV

reset; source_it "$T/bin" CUDA_VISIBLE_DEVICES=0
sleep 0.3; check "没有 SLURM_JOB_ID：不启动" test "$(lines)" -eq 0
reset; source_it "$T/bin" SLURM_JOB_ID=1003
sleep 0.3; check "没有 CUDA_VISIBLE_DEVICES：不启动" test "$(lines)" -eq 0
reset; source_it "$T/bin" SLURM_JOB_ID=1004 CUDA_VISIBLE_DEVICES=0 SLURM_STEP_ID=1
sleep 0.3; check "srun step：不启动" test "$(lines)" -eq 0
reset; source_it "$T/bin" SLURM_JOB_ID=1005 CUDA_VISIBLE_DEVICES=0 TASKQ_TEST_FAKEBIN=1
sleep 0.3; check "测试环境 TASKQ_TEST_FAKEBIN：不启动" test "$(lines)" -eq 0
reset; source_it "$T/bin" SLURM_JOB_ID=1006 CUDA_VISIBLE_DEVICES=0 QUEUE_TEST_DIR=$T
sleep 0.3; check "测试环境 QUEUE_TEST_DIR：不启动" test "$(lines)" -eq 0
printf 'MACHINES = {"m": dict(label="m", hostname=r".*", ssh=None, scheduler="slurm", gpu_pad=(1, 2, 3))}\nSITES = {}\nRELAY = {}\n' > "$T/machines_pad.py"
mkdir -p "$T/conf_pad" "$T/conf_nopad"; mv "$T/machines_pad.py" "$T/conf_pad/machines.py"
sed 's/, gpu_pad=(1, 2, 3)//' "$T/conf_pad/machines.py" > "$T/conf_nopad/machines.py"
reset; source_it "$T/bin" SLURM_JOB_ID=1014 CUDA_VISIBLE_DEVICES=0 TASKQ_GPU_PAD_FLOOR_MIB= TASKQ_GPU_PAD_TARGET_MIB= TASKQ_GPU_PAD_HIGH_MIB= TASKQ_CONFIG=$T/conf_nopad
sleep 0.3; check "机器表没写 gpu_pad、环境里也没给阈值：不启动" test "$(lines)" -eq 0
reset; source_it "$T/bin" SLURM_JOB_ID=1015 CUDA_VISIBLE_DEVICES=0 TASKQ_GPU_PAD_FLOOR_MIB= TASKQ_GPU_PAD_TARGET_MIB= TASKQ_GPU_PAD_HIGH_MIB= TASKQ_CONFIG=$T/conf_pad
waitfor 20 '(( $(lines) >= 1 ))'; sleep 0.3
check "阈值取机器表的 gpu_pad：启动" test "$(lines)" -eq 1
reset; source_it "$T/bin" SLURM_JOB_ID=1007 CUDA_VISIBLE_DEVICES=0 TASKQ_GPU_PAD=0
sleep 0.3; check "TASKQ_GPU_PAD=0：不启动" test "$(lines)" -eq 0
reset; touch "$T/switch"
source_it "$T/bin" SLURM_JOB_ID=1008 CUDA_VISIBLE_DEVICES=0
sleep 0.3; check "DISABLED 开关：不启动" test "$(lines)" -eq 0
reset; source_it "$T/nobin" SLURM_JOB_ID=1009 CUDA_VISIBLE_DEVICES=0
sleep 0.3; check "没有 nvidia-smi：不启动" test "$(lines)" -eq 0
reset; out=$(source_it "$T/bin" SLURM_JOB_ID=1013 CUDA_VISIBLE_DEVICES=0 TASKQ_GPU_PAD_SCRIPT=$T/nope.py 2>&1)
sleep 0.3; check "找不到 pad 脚本：不启动，作业日志里写一行" bash -c "test $(lines) -eq 0 && grep -q '显存填充没开：找不到' <<< '$out'"

reset
source_it_twice
waitfor 20 '(( $(lines) >= 1 ))'; sleep 0.3
check "重复 source 只启动一次" test "$(lines)" -eq 1

reset
source_it "$T/bin" SLURM_JOB_ID=1010 CUDA_VISIBLE_DEVICES=0 FAKE_PAD_FAIL_ONCE=1 > "$T/wlog"
waitfor 20 '(( $(lines) >= 2 ))'; sleep 0.3
check "pad 失败一次后 supervisor 重启" test "$(lines)" -eq 2
check "重启写进作业日志（作业日志是文件时）" grep -q '显存填充退出（退出码 1），1 倍 0.1 秒后重启' "$T/wlog"

reset
source_it "$T/bin" SLURM_JOB_ID=1011 CUDA_VISIBLE_DEVICES=0 FAKE_PAD_FAIL=1 TASKQ_GPU_PAD_MAX_FAILS=3 > "$T/wlog"
waitfor 20 'grep -q 不再重启 "$T/wlog"'; sleep 0.5
check "一直起不来：连续 3 次后放弃，作业日志里写明没有显存填充" \
      bash -c "test \$(wc -l < '$T/args') -eq 3 && grep -q '!! 显存填充连续 3 次起不来（退出码 1），不再重启，这个作业没有显存填充' '$T/wlog'"
check "重启间隔加倍" grep -q '2 倍 0.1 秒后重启' "$T/wlog"

reset
source_it "$T/bin" SLURM_JOB_ID=1012 CUDA_VISIBLE_DEVICES=0 FAKE_PAD_FAIL=1 TASKQ_GPU_PAD_MAX_FAILS=2 TASKQ_GPU_PAD_RESET_AFTER=0 > "$T/wlog"
waitfor 20 '(( $(lines) >= 5 ))'
check "跑过一阵（RESET_AFTER 以上）才退出的：一直重启、不放弃" bash -c "test \$(wc -l < '$T/args') -ge 5 && ! grep -q 不再重启 '$T/wlog'"

reset
if (( fails == 0 )); then echo "全部通过"; else echo "$fails 项失败"; exit 1; fi
