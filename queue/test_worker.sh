#!/bin/bash
# Slurm 作业里的 worker（worker.sbatch → worker_start.sh → consume.py）的模拟测试：临时的假队列（TASKQ_DIR）、临时数据目录、假作业号，
# 任务用 command 做法跑假训练（睡几秒后写训完的文件）；本机当成测试用的机器表（testdata/config）里的 site-slurm，
# 环境按它激活：假的 conda、venv（base、alt）。不用 Slurm、不用 GPU。
# 这里是领、训、交还的基本情况；停止和失败在 test_worker_stop.sh，不在 Slurm 里的和代码包在 test_worker_code.sh。
#   bash queue/test_worker.sh        WORKER_TEST_ONLY="1 4" 只跑这几个情况
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'rm -rf "${T:?}"' EXIT
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=site-slurm   # 本机当 Slurm 执行机（作业里跑的 worker）
source "$HERE/testdata/worker_harness.sh"

# 1. Slurm 作业的调用方式（worker.sbatch <code/current 下的 worker_start.sh>）：按顺序领；GPU 不忙、显存够时一张卡 2 个；按任务换环境
if want 1; then
reset
table << EOF
t/a base
t/b alt
t/c base
t/d base
EOF
FAKE_SECS=3 SLURM_JOB_ID=900101 timeout 90 bash "$HERE/worker.sbatch" "$HERE/worker_start.sh" > "$T/w1.out" 2>&1
check "Slurm 作业：四个都训完" '[[ $(grep -c ^end $T/train.log) == 4 ]]'
check "Slurm 作业：GPU 不忙时前两个同时开始" '[[ $(head -2 $T/train.log | grep -c ^start) == 2 ]]'
check "按任务说明换环境：alt 用 venv（基于 conda 的 base0），base 用 conda 环境（不带进 venv）" \
      'grep -q "^start .* t/b alt base0 " $T/train.log && grep -q "^start .* t/c 无 base0 " $T/train.log'
check "每个训练分到的 CPU 传给任务（TASK_CPUS）" 'grep -q "cpus=[0-9]" $T/train.log'
check "领取记录都交还了" '[[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'
check "分段日志按任务名、作业号写在 logs/tasks/" '[[ -f $TASKQ_DATA/logs/tasks/t__a__900101_part1.log ]] && head -1 $TASKQ_DATA/logs/tasks/t__a__900101_part1.log | grep -q "^job 900101_1 @ .*task=t/a"'
fi

# 1b. 本机没有的环境（x2 只在 hub 上）不领
if want 1b; then
reset
table << EOF
t/x x2
t/y base
EOF
SLURM_JOB_ID=900102 timeout 60 bash "$HERE/worker_start.sh" > "$T/w1b.out" 2>&1
check "本机没有的环境不领，别的照常领" '[[ $(grep -c ^end $T/train.log) == 1 ]] && grep -q "end .* t/y" $T/train.log && grep -q "没有能领的任务" $T/w1b.out'
fi

# 1c. Slurm 里开始时没有能领的：空着等，来了任务就领；一直没有，空等 IDLE_MAX 后结束
if want 1c; then
reset
: > "$TASKQ_DIR/tasks.jsonl"
SLURM_JOB_ID=900103 TASKQ_IDLE_MAX=8 timeout 60 bash "$HERE/worker_start.sh" > "$T/w1c.out" 2>&1 &
wpid=$!
waitfor 20 'grep -q "没有能领的任务，空着等" $T/w1c.out'
sleep 1
check "开始时没有能领的：不马上结束" 'kill -0 $wpid 2> /dev/null && ! grep -q "作业结束" $T/w1c.out'
table << EOF
t/a base
EOF
waitfor 30 'grep -q "^end .* t/a" $T/train.log'
check "空等时来了任务：领了、训完" 'grep -q "^end .* t/a" $T/train.log && grep -q "开训 t/a" $T/w1c.out'
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "训完后又没有能领的：空等 IDLE_MAX 后结束（退出码 0）" '[[ $rc == 0 ]] && grep -q "空等 0 分钟没有能领的任务，作业结束" $T/w1c.out && [[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'
fi

if want 1d; then
reset
: > "$TASKQ_DIR/tasks.jsonl"
t0=$(date +%s)
SLURM_JOB_ID=900104 timeout 60 bash "$HERE/worker_start.sh" > "$T/w1d.out" 2>&1; rc=$?
dt=$(( $(date +%s) - t0 ))
check "一直没有能领的：空等 IDLE_MAX（$TASKQ_IDLE_MAX 秒）后结束，用了 $dt 秒" '[[ $rc == 0 ]] && (( dt >= TASKQ_IDLE_MAX )) && grep -q "空等 0 分钟没有能领的任务，作业结束" $T/w1d.out && ! grep -q "没有在训的，也没有能领的任务" $T/w1d.out'
fi

# 2. GPU 跑满（使用率 95%）时一次只训 1 个
if want 2; then
reset
table << EOF
t/a base
t/b base
EOF
SLURM_JOB_ID=900105 WORKER_TEST_GPU="95 10 30000 81920" timeout 60 bash "$HERE/worker_start.sh" > "$T/w2.out" 2>&1
check "GPU 跑满：一个训完才开始下一个" '[[ $(sed -n 2p $T/train.log) == end* && $(grep -c ^end $T/train.log) == 2 ]]'
fi

# 2b. 显存不够一半（训练占 50GB）时也只训 1 个
if want 2b; then
reset
table << EOF
t/a base
t/b base
EOF
SLURM_JOB_ID=900106 WORKER_TEST_GPU="50 10 50000 81920" timeout 60 bash "$HERE/worker_start.sh" > "$T/w2b.out" 2>&1
check "显存不够：一次 1 个" '[[ $(sed -n 2p $T/train.log) == end* && $(grep -c ^end $T/train.log) == 2 ]]'
fi

# 2c. 显存填充：放掉（或写 ready）之后才开始训练
if want 2c; then
reset
table << EOF
t/a base
EOF
rm -f "$T/pad.start" "$T/pad.released" "$T/pad.ready"
echo 33000 > "$T/pad.mib"
( while [[ ! -e $T/pad.start ]]; do sleep 0.2; done; sleep 1; echo 0 > "$T/pad.mib"; date +%s.%N > "$T/pad.released" ) &
padpid=$!
SLURM_JOB_ID=900107 TASKQ_GPU_PAD_BASE=$T/pad timeout 60 bash "$HERE/worker_start.sh" > "$T/w2c.out" 2>&1
kill $padpid 2> /dev/null
check "显存填充放掉之后才开始训练" '[[ -f $T/pad.released ]] && awk -v r=$(cat $T/pad.released) "/^start/ {exit !(\$2 >= r)}" $T/train.log'
fi

if want 2c2; then
reset
table << EOF
t/a base
EOF
rm -f "$T/pad.start" "$T/pad.released" "$T/pad.ready"
echo 33000 > "$T/pad.mib"
echo stale > "$T/pad.ready"
( while [[ ! -e $T/pad.start ]]; do sleep 0.2; done; sleep 1; echo 1 > "$T/pad.ready"; date +%s.%N > "$T/pad.released" ) &
padpid=$!
SLURM_JOB_ID=900108 TASKQ_GPU_PAD_BASE=$T/pad timeout 60 bash "$HERE/worker_start.sh" > "$T/w2c2.out" 2>&1
kill $padpid 2> /dev/null
check "显存填充只放到 floor（不到 0）：等它写 ready 后才开始训练，旧的 ready 不算" '[[ -f $T/pad.released ]] && awk -v r=$(cat $T/pad.released) "/^start/ {exit !(\$2 >= r)}" $T/train.log && ! grep -q "没回应" $T/w2c2.out'
fi

# 2d. 训练FAIL:交还、记一次失败，本作业不再领它
if want 2d; then
reset
table << EOF
t/a base
t/b base
EOF
SLURM_JOB_ID=900109 FAKE_FAIL=t/a TASKQ_SLOTS=1 timeout 60 bash "$HERE/worker_start.sh" > "$T/w2d.out" 2>&1
check "训练FAIL:记一次失败、接着领下一个" '[[ $(wc -l < $TASKQ_DIR/failures/t__a) == 1 ]] && grep -q "end .* t/b" $T/train.log && [[ $(grep -c "start .* t/a" $T/train.log) == 1 ]]'
fi

# 2d2. 缺输入（run_task.sh 退出码 4）：交还、记为缺输入，不算失败，接着领下一个
if want 2d2; then
reset
rm -rf "$TASKQ_DIR/inputs"
table << EOF
t/a base
t/b base
EOF
python3 - "$TASKQ_DIR/tasks.jsonl" << 'PY'
import json, sys
p = sys.argv[1]
ts = [json.loads(l) for l in open(p)]
ts[0]["params"]["inputs"] = ["in/nothere"]
open(p, "w").write("".join(json.dumps(t) + "\n" for t in ts))
PY
SLURM_JOB_ID=900110 TASKQ_SLOTS=1 timeout 60 bash "$HERE/worker_start.sh" > "$T/w2m.out" 2>&1
check "缺输入：没开训、不记失败、记为缺输入，接着训下一个" '! grep -q "start .* t/a" $T/train.log && [[ ! -e $TASKQ_DIR/failures/t__a ]] && [[ -e $TASKQ_DIR/inputs/t__a ]] && grep -q "end .* t/b" $T/train.log'
check "缺输入：领取交还了" '[[ ! -e $TASKQ_DIR/claims/t__a ]]'
rm -rf "$TASKQ_DIR/inputs"
fi

# 2f. 卡住的任务（日志、结果文件夹都不动）：停掉、交还、不记失败，本作业领下一个；一直在写结果文件夹的不算卡住
if want 2f; then
reset
table << EOF
t/a base
t/b base
t/c base
EOF
SLURM_JOB_ID=900111 TASKQ_SLOTS=1 TASKQ_STALL_MAX=3 FAKE_HANG=t/a FAKE_BUSY=t/b FAKE_BUSY_SECS=7 timeout 90 bash "$HERE/worker_start.sh" > "$T/w2f.out" 2>&1
check "卡住：停掉、交还、不记失败" 'grep -q "^term t/a" $T/train.log && grep -q "t/a 的日志和结果文件夹 .*当作卡住" $T/w2f.out && grep -q "卡住的 t/a 已停下" $T/w2f.out && [[ ! -e $TASKQ_DIR/failures/t__a ]]'
check "卡住的停下后，本作业接着领后面的" 'grep -q "end .* t/b" $T/train.log && grep -q "end .* t/c" $T/train.log && [[ $(grep -c "start .* t/a" $T/train.log) == 1 ]]'
check "一直在写结果文件夹（不写日志）的不算卡住" '! grep -q "t/b 的日志和结果文件夹" $T/w2f.out'
check "领取记录都交还了" '[[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'
fi

# 4. 分出去的领取（take --assign，不过期；beat 30 分钟前）不被 Slurm 作业领走
if want 4; then
reset
table << EOF
t/a base
EOF
python3 "$HERE/taskq.py" take --worker site-pueue-gpu1 --assign t/a > /dev/null
python3 - "$TASKQ_DIR" << 'EOF'
import json, os, sys, time
p = os.path.join(sys.argv[1], "claims", os.listdir(os.path.join(sys.argv[1], "claims"))[0], "info")
i = json.load(open(p)); i["beat"] = time.time() - 1800; json.dump(i, open(p, "w"))
EOF
SLURM_JOB_ID=900112 timeout 60 bash "$HERE/worker_start.sh" > "$T/w4.out" 2>&1
check "分给 site-pueue-gpu1 的（30 分钟没续也不过期）：Slurm 作业不领" '[[ ! -s $T/train.log ]] && grep -q "没有能领的任务" $T/w4.out'
fi

# 5. 队列出错（任务表读不了）连续一段时间、手上没有在训的：结束作业
if want 5; then
reset
rm -f "$TASKQ_DIR/tasks.jsonl"
SLURM_JOB_ID=900113 TASKQ_IDLE_MAX=60 timeout 60 bash "$HERE/worker_start.sh" > "$T/w5.out" 2>&1; rc=$?
check "队列出错：报出来、过一会儿结束作业" '[[ $rc == 0 ]] && grep -q "队列出错" $T/w5.out && grep -q "队列连续出错" $T/w5.out'
fi

(( fails == 0 )) && echo "全部通过" || { echo "$fails 项失败"; exit 1; }
