#!/bin/bash
# dicc_worker.sh 的模拟测试（同 test_dicc_worker.sh）：不在 Slurm 里（pueue 常驻）、代码更新、代码包，gpu_stats / pad_now
#   bash scripts/taskq/queue/test_dicc_worker_code.sh        WORKER_TEST_ONLY="6 6e" 只跑这几个情况
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CORE=$(cd "$HERE/.." && pwd)  # 调度代码的根；使用者的仓库根 REPO（TASKQ_REPO，没设时同 cfg.py）
REPO=${TASKQ_REPO:-$CORE}; [[ -z ${TASKQ_REPO:-} && $CORE == */scripts/taskq ]] && REPO=${CORE%/scripts/taskq}
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=dicc   # 本机当队列所在的机器（不转发到真的 DICC）
source "$HERE/testdata/worker_harness.sh"
HEADCMD="cat $T/head"

# 6. 不在 Slurm 里（lab 的 pueue 组里常驻，10/31）：WORKER_ID 当名字，按本机 slots（box 没写 → 1），只看 CUDA_VISIBLE_DEVICES 那张卡；
#    空闲不到 IDLE_EXIT（默认 30 分钟）、队列出错、失败太多都不退出（RUN_TASK_TRIES=1：不在 Slurm 里 run_task.sh 自己会重试 3 次）；手上没有在训的时候提交号变了才退出（退出码 0），在训时变了等训完
if want 6; then
mkdir -p "$T/bin6"; printf '#!/bin/bash\necho "$*" >> %q\n' "$T/nvsmi.args" > "$T/bin6/nvidia-smi"; chmod +x "$T/bin6/nvidia-smi"
echo a > "$T/head"
reset
table << EOF
t/a base
t/b base
EOF
(unset TASKQ_SLOTS; WORKER_ID=lab-test CUDA_VISIBLE_DEVICES=1 TASKQ_IDLE_POLL=1 TASKQ_HEAD_CMD=$HEADCMD PATH=$T/bin6:$PATH exec bash "$HERE/dicc_worker.sh") > "$T/w6.out" 2>&1 &
wpid=$!
waitfor 60 '[[ $(grep -c ^end $T/train.log) == 2 ]]'
check "不在 Slurm 里：按 slots 一次 1 个（GPU 不忙也不加第二个），两个先后训完" '[[ $(sed -n 2p $T/train.log) == end* && $(grep -c ^end $T/train.log) == 2 ]] && grep -q "作业 lab-test @ .*最多同时 1 个.*时限 无" $T/w6.out'
check "不在 Slurm 里：nvidia-smi 只看 CUDA_VISIBLE_DEVICES 那张卡" '[[ -s $T/nvsmi.args ]] && ! grep -qv "^-i 1 " $T/nvsmi.args'
waitfor 20 'grep -q "没有能领的任务，每 1 秒再看" $T/w6.out'
sleep $(( TASKQ_IDLE_MAX + 1 ))
check "不在 Slurm 里：空闲超过 IDLE_MAX 也不退出" 'kill -0 $wpid 2> /dev/null && grep -q "没有能领的任务，每 1 秒再看" $T/w6.out && ! grep -q "结束作业" $T/w6.out'
mv "$TASKQ_DIR/tasks.jsonl" "$TASKQ_DIR/tasks.jsonl.bak"
waitfor 20 'grep -q "队列出错" $T/w6.out'
sleep $(( TASKQ_ERR_MAX + 1 ))
check "不在 Slurm 里：队列连续出错超过 ERR_MAX 也不退出" 'kill -0 $wpid 2> /dev/null && grep -q "队列出错" $T/w6.out && ! grep -q "队列连续出错" $T/w6.out'
mv "$TASKQ_DIR/tasks.jsonl.bak" "$TASKQ_DIR/tasks.jsonl"
echo b > "$T/head"
waitfor 15 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "不在 Slurm 里：空闲时提交号变了就退出（退出码 0）" '[[ $rc == 0 ]] && grep -q "代码更新了" $T/w6.out && grep -q "退出，由 relay.py 用新代码重开" $T/w6.out'
fi

if want 6d; then
reset
: > "$TASKQ_DIR/tasks.jsonl"
echo b > "$T/head"
(WORKER_ID=lab-test TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_HEAD_CMD=$HEADCMD exec bash "$HERE/dicc_worker.sh") > "$T/w6d.out" 2>&1 &
wpid=$!
waitfor 20 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "不在 Slurm 里：空闲超过 IDLE_EXIT 就退出（退出码 0）" '[[ $rc == 0 ]] && grep -q "退出（有备好的任务时下次检查再开）" $T/w6d.out'
fi

if want 6b; then
reset
table << EOF
t/c base
t/d base
EOF
echo a > "$T/head"
(WORKER_ID=lab-test TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 FAKE_SECS=3 TASKQ_HEAD_CMD=$HEADCMD exec bash "$HERE/dicc_worker.sh") > "$T/w6b.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^start .* t/c" $T/train.log'
echo b > "$T/head"
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "不在 Slurm 里：在训时代码变了，训完不再领下一个、退出" '[[ $rc == 0 ]] && grep -q "^end .* t/c" $T/train.log && ! grep -q "t/d" $T/train.log && grep -q "代码更新了" $T/w6b.out && [[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'
fi

if want 6c; then
reset
table << EOF
t/a base
t/b base
t/c base
EOF
echo a > "$T/head"
(WORKER_ID=lab-test TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 RUN_TASK_TRIES=1 FAKE_FAIL="t/a t/b" TASKQ_HEAD_CMD=$HEADCMD exec bash "$HERE/dicc_worker.sh") > "$T/w6c.out" 2>&1 &
wpid=$!
waitfor 30 '[[ $(grep -c "^fail" $T/train.log) == 2 ]]'
waitfor 20 'grep -q "先不领" $T/w6c.out'
sleep 2
check "不在 Slurm 里：两个开始不久就失败，不再领、也不退出" 'kill -0 $wpid 2> /dev/null && ! grep -q "start .* t/c" $T/train.log && grep -q "先不领" $T/w6c.out'
kill -TERM $wpid; wait $wpid 2> /dev/null
fi

# 不在 Slurm 里的 CPU worker（pueue 的 cpu_worker）：最多用 TASKQ_CPUS 个核
if want 6f; then
reset
table << EOF
t/c1 base cpu
EOF
(WORKER_ID=lab-cpu WORKER_DEVICE=cpu TASKQ_CPUS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 exec bash "$HERE/dicc_worker.sh") > "$T/w6f.out" 2>&1 &
wpid=$!
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid
check "不在 Slurm 里的 CPU worker：最多用 TASKQ_CPUS 个核" 'grep -q "worker（cpu）.*CPU 1，" $T/w6f.out && grep -q "^start .* t/c1 .*cpus=1$" $T/train.log'
fi

# 6e. 代码包（<根>/code/<包>、current 指向在用的）：current 换了，空闲时直接 exec 新包的 worker_start.sh，同一进程接着跑（11/1）
if want 6e; then
reset
table << EOF
t/k base
EOF
mkdir -p "$T/code/a" "$T/code/b/scripts/taskq/queue"
cp -r "$REPO/scripts" "$T/code/a/"   # 配置、插件经 TASKQ_CONFIG、TASKQ_PLUGINS 找（testdata）
printf '#!/bin/bash
echo "新包的 start $WORKER_ID"
' > "$T/code/b/scripts/taskq/queue/worker_start.sh"
ln -sfn a "$T/code/current"
touch -d "5 days ago" "$T/code/a"
(WORKER_ID=lab-test TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 exec bash "$T/code/a/scripts/taskq/queue/dicc_worker.sh") > "$T/w6e.out" 2>&1 &
wpid=$!
waitfor 20 'grep -q "没有能领的任务" $T/w6e.out'
ln -sfn b "$T/code/current"
waitfor 20 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "代码包换了：空闲时 exec 新包的 worker_start.sh 接着跑，不退出等重开" '[[ $rc == 0 ]] && grep -q "换成新代码 b 接着跑" $T/w6e.out && grep -q "新包的 start lab-test" $T/w6e.out'
check "代码包：worker、任务日志记的代码版本是包名（不是"未知+改动"）" 'grep -q "，代码 a$" $T/w6e.out && grep -q "^end .* t/k" $T/train.log && head -1 $TASKQ_DATA/logs/tasks/t__k__*_part1.log | grep -q "，代码 a，"'
check "代码包：worker 在用的包更新了时间戳（清理只删 4 天没动过的）" '[[ -n $(find "$T/code/a" -maxdepth 0 -mtime -1) ]]'
fi

# gpu_stats / pad_now 读真的文件（假 nvidia-smi）：只取 dicc_worker.sh 里这两个函数
if want gpu; then
sed -n '/^gpu_stats() {/,/^can_add() {/p' "$HERE/dicc_worker.sh" | sed '$d' > "$T/fns.sh"
mkdir -p "$T/bin"; printf '#!/bin/bash\necho "30000, 81920"\n' > "$T/bin/nvidia-smi"; chmod +x "$T/bin/nvidia-smi"
GPUCSV=$T/gpu.csv PADBASE=$T/pad
for i in $(seq 12); do echo "2026/10/18 10:$i, 0, 30000 MiB, 81920 MiB, $(( i <= 2 ? 0 : 60 )) %"; done > "$GPUCSV"
source "$T/fns.sh"; unset WORKER_TEST_GPU
check "gpu_stats：只看最近 10 条使用率（前面的 0% 不算），显存读 nvidia-smi" '[[ $(PATH=$T/bin:$PATH gpu_stats) == "60 10 30000 81920" ]]'
check "gpu_stats：没有记录时样本数 0" '[[ $(GPUCSV=$T/none PATH=$T/bin:$PATH gpu_stats) == "0 0 30000 81920" ]]'
printf '#!/bin/bash\n[[ "$*" == "-i 1 "* ]] && echo "5000, 24576" || echo "30000, 81920"\n' > "$T/bin/nvidia-smi"
check "gpu_stats：NVSEL=(-i 1) 时只读那张卡的显存" '[[ $(NVSEL=(-i 1); PATH=$T/bin:$PATH gpu_stats) == "60 10 5000 24576" ]]'
echo 12000 > "$PADBASE.mib"
check "pad_now：<base>.mib 刚更新过，按它算" '[[ $(pad_now) == 12000 ]]'
touch -d "-2 minutes" "$PADBASE.mib"
check "pad_now：<base>.mib 超过 60 秒没更新（填充不在了），算 0" '[[ $(pad_now) == 0 ]]'
fi

(( fails == 0 )) && { echo "全部通过"; rm -rf "$T"; } || { echo "$fails 项失败（文件在 $T）"; exit 1; }
