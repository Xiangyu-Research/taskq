# worker（worker_start.sh → consume.py）的测试共用的假训练和小工具：先建 $T、设 TASKQ_DIR、TASKQ_DATA、TASKQ_MACHINE，再 source
# WORKER_TEST_ONLY="<情况…>"：只跑这几个情况（want），没设时都跑
export TASKQ_POLL=1 TASKQ_IDLE_MAX=2 TASKQ_PATIENCE=0 TASKQ_MIN_LEFT=0 TASKQ_SLOTS=2 TASKQ_ADD_AFTER=0 TASKQ_ERR_MAX=2
export WORKER_TEST_GPU="50 10 20000 81920"   # 近 10 分钟使用率 50%、训练占 20GB
export TASKQ_GPU_PAD_BASE=$T/nopad TMPDIR=$T/tmp   # 显存填充、本地副本不用 /tmp 下按作业号的路径
export TASKQ_GPU_PAD=0   # worker_start.sh 不开真的显存填充（和填充的握手由 TASKQ_GPU_PAD_BASE 下的假文件模拟）
mkdir -p "$TASKQ_DIR" "$TASKQ_DATA" "$TMPDIR"
touch "$TASKQ_DIR/ENABLED"
cat > "$T/fake_train.sh" << 'EOT'
#!/bin/bash
# 假训练：参数 任务名；记下开始、结束时刻、环境，睡 ${FAKE_SECS:-1} 秒后写训完的文件
out=$TASKQ_DATA/runs/$1
mkdir -p "$out"
# 先装好收停止信号的处理，再记开始（测试看到 start 就可能马上发停止）
if [[ ${FAKE_IGNORE:-} == "$1" ]]; then  # 不理停止信号
    trap 'echo "term $1" >> "$FAKE_LOG"' TERM
else
    trap 'echo "term $1" >> "$FAKE_LOG"; exit 143' TERM
fi
echo "start $(date +%s.%N) $1 ${DEMO_ACTIVE_VENV:-无} ${CONDA_DEFAULT_ENV:-无} cpus=${TASK_CPUS:-}" >> "$FAKE_LOG"
[[ ${FAKE_IGNORE:-} == "$1" ]] && while :; do sleep 1; done
if [[ ${FAKE_ESCAPE:-} == "$1" ]]; then  # 留下一个停不下来的进程：同一个会话、另一个进程组、不理 SIGTERM，FAKE_ESCAPE_SECS 秒后自己退出
    python3 -c 'import os, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
if os.fork():
    os._exit(0)
os.setpgid(0, 0)
open(sys.argv[1], "w").write(str(os.getpid()))
time.sleep(float(sys.argv[2]))' "$FAKE_LOG.escape" "${FAKE_ESCAPE_SECS:-20}"
    sleep 120 & wait $!
elif [[ ${FAKE_BUSY:-} == "$1" ]]; then  # 一直在写结果文件夹、不写日志（像概率图稀疏化）
    for _ in $(seq "${FAKE_BUSY_SECS:-10}"); do touch "$out/progress"; sleep 1; done
elif [[ ${FAKE_HANG:-} == "$1" ]]; then
    sleep 120 & wait $!
elif [[ ${FAKE_EAGER:-} == "$1" ]]; then  # 子进程（sleep）一结束就写训完的文件，不管它怎么结束的；旁边一条 30 层深的进程链：
    # 从叶子停进程树时会先停到 sleep、隔一会儿才停到训练本身（要整组一起停才不写训完的文件）
    sleep 60 & c=$!
    bash "$FAKE_CHAIN" 30 &
    wait $c
    : > "$out/DONE"
    echo "end $(date +%s.%N) $1" >> "$FAKE_LOG"
    exit 0
else
    sleep "${FAKE_SECS:-1}" & wait $!
fi || { echo "term $1" >> "$FAKE_LOG"; exit 143; }  # 子进程先被停（停进程树时从叶子停起）：当作收到停止，不写训完的文件
[[ " ${FAKE_FAIL:-} " == *" $1 "* ]] && { echo "fail $1" >> "$FAKE_LOG"; exit 1; }  # FAKE_FAIL：空格分开的任务名
touch "$out/DONE"
echo "end $(date +%s.%N) $1" >> "$FAKE_LOG"
EOT
chmod +x "$T/fake_train.sh"
cat > "$T/chain.sh" << 'EOT'
#!/bin/bash
# $1 层深的进程链，最底下写 chain.ready 后睡 60 秒（FAKE_EAGER 用）
if (( $1 > 0 )); then bash "$0" $(( $1 - 1 )); else touch "$(dirname "$0")/chain.ready"; sleep 60; fi
EOT
export FAKE_CHAIN=$T/chain.sh
export FAKE_LOG=$T/train.log
fails=0
check() { if eval "$2"; then echo "PASS:$1"; else echo "FAIL:$1"; fails=$((fails + 1)); fi; }
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
reset() { : > "$T/train.log"; rm -f "$T/chain.ready"; rm -rf "$TASKQ_DATA/runs" "$TASKQ_DIR/claims" "$TASKQ_DIR/failures" "$TASKQ_DIR/released" "$TASKQ_DIR/stopped"; }
