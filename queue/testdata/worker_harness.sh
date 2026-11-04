# dicc_worker.sh 的测试共用的假训练和小工具：先建 $T、设 TASKQ_DIR、TASKQ_DATA、TASKQ_MACHINE，再 source
# WORKER_TEST_ONLY="<情况…>"：只跑这几个情况（want），没设时都跑
export TASKQ_POLL=1 TASKQ_IDLE_MAX=2 TASKQ_PATIENCE=0 TASKQ_MIN_LEFT=0 TASKQ_SLOTS=2 TASKQ_ADD_AFTER=0 TASKQ_ERR_MAX=2
export WORKER_TEST_GPU="50 10 20000 81920"   # 近 10 分钟使用率 50%、训练占 20GB
export TASKQ_GPU_PAD_BASE=$T/nopad TMPDIR=$T/tmp   # 显存填充、本地副本不用 /tmp 下按作业号的路径
mkdir -p "$TASKQ_DIR" "$TASKQ_DATA" "$TMPDIR"
touch "$TASKQ_DIR/ENABLED"
cat > "$T/fake_train.sh" << 'EOT'
#!/bin/bash
# 假训练：参数 任务名；记下开始、结束时刻、环境，睡 ${FAKE_SECS:-1} 秒后写训完的文件
out=$TASKQ_DATA/runs/$1
mkdir -p "$out"
echo "start $(date +%s.%N) $1 ${DEMO_ACTIVE_VENV:-无} ${CONDA_DEFAULT_ENV:-无} cpus=${TASK_CPUS:-}" >> "$FAKE_LOG"
trap 'echo "term $1" >> "$FAKE_LOG"; exit 143' TERM
if [[ ${FAKE_IGNORE:-} == "$1" ]]; then  # 不理停止信号
    trap 'echo "term $1" >> "$FAKE_LOG"' TERM
    while :; do sleep 1; done
fi
if [[ ${FAKE_BUSY:-} == "$1" ]]; then  # 一直在写结果文件夹、不写日志（像概率图稀疏化）
    for _ in $(seq "${FAKE_BUSY_SECS:-10}"); do touch "$out/progress"; sleep 1; done
elif [[ ${FAKE_HANG:-} == "$1" ]]; then
    sleep 120 & wait $!
else
    sleep "${FAKE_SECS:-1}" & wait $!
fi
[[ " ${FAKE_FAIL:-} " == *" $1 "* ]] && { echo "fail $1" >> "$FAKE_LOG"; exit 1; }  # FAKE_FAIL：空格分开的任务名
touch "$out/DONE"
echo "end $(date +%s.%N) $1" >> "$FAKE_LOG"
EOT
chmod +x "$T/fake_train.sh"
export FAKE_LOG=$T/train.log
fails=0
check() { if eval "$2"; then echo "通过：$1"; else echo "失败：$1"; fails=$((fails + 1)); fi; }
waitfor() { local i; for i in $(seq "$1"); do eval "$2" && return 0; sleep 1; done; return 1; }
want() { [[ -z ${WORKER_TEST_ONLY:-} || " $WORKER_TEST_ONLY " == *" $1 "* ]]; }
table() {  # 每行：任务名 环境 [device]
    : > "$TASKQ_DIR/tasks.jsonl"
    local i=0 name env dev
    while read -r name env dev; do
        i=$((i + 1))
        python3 - "$TASKQ_DIR/tasks.jsonl" "$i" "$name" "$env" "$T/fake_train.sh" "${dev:-gpu}" << 'EOT'
import json, sys
p, i, name, env, fake, dev = sys.argv[1:]
t = dict(order=int(i), nice=0, key=name, exp=None, recipe="command",
         params=dict(key=name, cmd=f"bash {fake} {name}", env=env, out=f"runs/{name}", done=["DONE"], device=dev))
open(p, "a").write(json.dumps(t) + "\n")
EOT
    done
}
reset() { : > "$T/train.log"; rm -rf "$TASKQ_DATA/runs" "$TASKQ_DIR/claims" "$TASKQ_DIR/failures" "$TASKQ_DIR/released" "$TASKQ_DIR/stopped"; }
