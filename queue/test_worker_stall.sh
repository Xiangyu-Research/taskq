#!/bin/bash
# 不在 Slurm 里（pueue 常驻 worker）的卡住检测：日志名是主机名-时间，不是 WORKER_ID（11/3）
# 假训练卡住不动：worker 停掉它、交还，接着训下一个；不用 Slurm、不用 GPU。
#   bash scripts/taskq/queue/test_worker_stall.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=dicc   # 本机当队列所在的机器（不转发到真的 DICC）
export TASKQ_POLL=1 TASKQ_IDLE_MAX=4 TASKQ_PATIENCE=0 TASKQ_MIN_LEFT=0 TASKQ_SLOTS=2 TASKQ_ADD_AFTER=0 TASKQ_ERR_MAX=3
mkdir -p "$TASKQ_DIR" "$TASKQ_DATA"
touch "$TASKQ_DIR/ENABLED"
cat > "$T/fake_train.sh" << 'EOF'
#!/bin/bash
# 假训练：参数 任务名；记下开始、结束时刻、环境，睡 ${FAKE_SECS:-3} 秒后写训完的文件
out=$TASKQ_DATA/runs/$1
mkdir -p "$out"
echo "start $(date +%s.%N) $1 ${DEMO_ACTIVE_VENV:-无} ${CONDA_DEFAULT_ENV:-无} cpus=${TASK_CPUS:-}" >> "$FAKE_LOG"
trap 'echo "term $1" >> "$FAKE_LOG"; exit 143' TERM
if [[ ${FAKE_IGNORE:-} == "$1" ]]; then  # 不理停止信号
    trap 'echo "term $1" >> "$FAKE_LOG"' TERM
    while :; do sleep 1; done
fi
if [[ ${FAKE_BUSY:-} == "$1" ]]; then  # 一直在写结果文件夹、不写日志（像概率图稀疏化）
    for _ in $(seq 10); do touch "$out/progress"; sleep 1; done
elif [[ ${FAKE_HANG:-} == "$1" ]]; then
    sleep 120 & wait $!
else
    sleep "${FAKE_SECS:-3}" & wait $!
fi
[[ ${FAKE_FAIL:-} == "$1" ]] && { echo "fail $1" >> "$FAKE_LOG"; exit 1; }
touch "$out/DONE"
echo "end $(date +%s.%N) $1" >> "$FAKE_LOG"
EOF
chmod +x "$T/fake_train.sh"
export FAKE_LOG=$T/train.log
fails=0
check() { if eval "$2"; then echo "通过：$1"; else echo "失败：$1"; fails=$((fails + 1)); fi; }
waitfor() { local i; for i in $(seq "$1"); do eval "$2" && return 0; sleep 1; done; return 1; }
table() {  # 每行：任务名 环境 [device]
    : > "$TASKQ_DIR/tasks.jsonl"
    local i=0 name env dev
    while read -r name env dev; do
        i=$((i + 1))
        python3 - "$TASKQ_DIR/tasks.jsonl" "$i" "$name" "$env" "$T/fake_train.sh" "${dev:-gpu}" << 'EOF'
import json, sys
p, i, name, env, fake, dev = sys.argv[1:]
t = dict(order=int(i), nice=0, key=name, exp=None, recipe="command",
         params=dict(key=name, cmd=f"bash {fake} {name}", env=env, out=f"runs/{name}", done=["DONE"], device=dev))
open(p, "a").write(json.dumps(t) + "\n")
EOF
    done
}
reset() { : > "$T/train.log"; rm -rf "$TASKQ_DATA/runs" "$TASKQ_DIR/claims" "$TASKQ_DIR/failures" "$TASKQ_DIR/released" "$TASKQ_DIR/stopped"; }
export WORKER_TEST_GPU="50 10 20000 81920"
reset
table << EOT
t/a base
t/b base
EOT
(WORKER_ID=lab-test TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=6 TASKQ_STALL_MAX=4 FAKE_HANG=t/a timeout 40 bash "$HERE/dicc_worker.sh") > "$T/w.out" 2>&1
check "pueue worker：卡住的 t/a 被停掉、交还" 'grep -q "t/a 的日志和结果文件夹 .*当作卡住" $T/w.out && grep -q "^term t/a" $T/train.log'
check "pueue worker：卡住后接着训 t/b" 'grep -q "end .* t/b" $T/train.log'
rm -rf "$T"
(( fails == 0 )) && echo 全部通过 || { echo "失败 $fails 项"; exit 1; }
