#!/bin/bash
# dicc_worker.sh 的模拟测试（同 test_dicc_worker.sh）：停止信号、停止某个任务、CPU worker、开始不久就失败
#   bash queue/test_dicc_worker_stop.sh        WORKER_TEST_ONLY="3 4b" 只跑这几个情况
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=dicc   # 本机当队列所在的机器（不转发到真的 DICC）
source "$HERE/testdata/worker_harness.sh"

# 2e. 开始不久就失败的 2 个任务：作业不再领；跑了一段时间才失败的、两次隔得比时间窗久的不算作业的问题，照常领下一个
if want 2e; then
reset
table << EOF
t/a base
t/b base
t/c base
EOF
export FAKE_FAIL="t/a t/b"
SLURM_JOB_ID=900201 TASKQ_SLOTS=1 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2e1.out" 2>&1
check "两个任务开始不久就失败：作业不再领第三个" '[[ $(grep -c "^fail" $T/train.log) == 2 ]] && ! grep -q "start .* t/c" $T/train.log && grep -q "开始不久就失败" $T/w2e1.out'
reset
SLURM_JOB_ID=900202 TASKQ_SLOTS=1 TASKQ_EARLY_FAIL=0 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2e2.out" 2>&1
check "跑了一段时间才失败的：记在任务上，作业照常领下一个" '[[ $(grep -c "^fail" $T/train.log) == 2 ]] && grep -q "end .* t/c" $T/train.log && [[ -f $TASKQ_DIR/failures/t__a && -f $TASKQ_DIR/failures/t__b ]]'
reset
SLURM_JOB_ID=900203 TASKQ_SLOTS=1 TASKQ_FAIL_WINDOW=0 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2e3.out" 2>&1
check "开始不久就失败、但不在时间窗内的两次：作业照常领下一个" '[[ $(grep -c "^fail" $T/train.log) == 2 ]] && grep -q "end .* t/c" $T/train.log && ! grep -q "开始不久就失败" $T/w2e3.out'
unset FAKE_FAIL
fi

# 3a. 假训练本身：停进程树时先停到子进程（sleep）、过一会儿才停到它，也不写训完的文件（CI 上 3b 因此偶尔看到 t/a 训完）
if want 3a; then
reset
FAKE_SECS=60 bash "$T/fake_train.sh" t/z & fp=$!
waitfor 30 'pgrep -P $fp sleep > /dev/null'
kill -TERM $(pgrep -P $fp sleep); sleep 0.5; kill -TERM $fp 2> /dev/null; wait $fp; rc=$?
check "假训练：子进程先被停 → 退出 143、不算训完" '[[ $rc == 143 && ! -e $TASKQ_DATA/runs/t/z/DONE ]] && grep -q "^term t/z" $T/train.log'
fi

# 3. 收到停止信号：停训练、交还领取（下一个作业能接着领）
if want 3; then
reset
table << EOF
t/a base
EOF
FAKE_SECS=60 SLURM_JOB_ID=900204 bash "$HERE/dicc_worker.sh" V_save 3 101 > "$T/w3.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q ^start $T/train.log 2> /dev/null'
kill -TERM $wpid; wait $wpid; rc=$?
check "停止信号：退出 143、训练收到停止、领取交还" '[[ $rc == 143 ]] && grep -q ^term $T/train.log && [[ -z $(ls $TASKQ_DIR/claims) ]]'
fi

# 3b. 停止某个任务（taskq.py stop）：30 秒内停下、交还、不记失败，不再领它；另一个照常训；不理停止信号的强制停
if want 3b; then
reset
table << EOF
t/a base
t/b base
t/c base
EOF
FAKE_SECS=40 FAKE_IGNORE=t/b SLURM_JOB_ID=900205 TASKQ_IDLE_MAX=60 TASKQ_STOP_WAIT=3 TASKQ_POLL=30 TASKQ_STOP_POLL=1 bash "$HERE/dicc_worker.sh" > "$T/w3b.out" 2>&1 &
wpid=$!
waitfor 30 '[[ $(grep -c ^start $T/train.log 2> /dev/null) == 2 ]]'
t0=$(date +%s)
python3 "$HERE/taskq.py" stop --why 测试 t/a > "$T/stop.out"
waitfor 40 '[[ ! -e $TASKQ_DIR/claims/t__a ]]'
dt=$(( $(date +%s) - t0 ))
check "停止：$dt 秒内停下、交还（≤ 30 秒）、不记失败" '(( dt <= 30 )) && grep -q "^term t/a" $T/train.log && [[ ! -e $TASKQ_DIR/claims/t__a && ! -e $TASKQ_DIR/failures/t__a ]] && grep -q "30 秒内停下" $T/stop.out'
python3 "$HERE/taskq.py" status > "$T/st.out"
check "状态里是 stopped" 'grep -q "t/a .*stopped 停止" $T/st.out'
t0=$(date +%s)
python3 "$HERE/taskq.py" stop t/b > /dev/null
waitfor 40 '[[ ! -e $TASKQ_DIR/claims/t__b ]]'
dt=$(( $(date +%s) - t0 ))
check "不理停止信号的：$dt 秒内强制停、交还（≤ 30 秒）" '(( dt <= 30 )) && grep -q "t/b 3 秒没停下，强制停" $T/w3b.out && [[ ! -e $TASKQ_DIR/claims/t__b ]]'
waitfor 20 'grep -q "start .* t/c" $T/train.log'
check "停止的不再领，空出的位置领下一个" '[[ $(grep -c "start .* t/a" $T/train.log) == 1 ]] && grep -q "start .* t/c" $T/train.log'
python3 "$HERE/taskq.py" resume t/a > /dev/null
# 撤销后有空位时 worker 可能马上又领走 t/a：只看不再是 stopped（free 或已经领了）
waitfor 10 'python3 "$HERE/taskq.py" status | grep -q "t/a " && ! python3 "$HERE/taskq.py" status | grep -q "t/a .*stopped"'
check "撤销停止后照常排" '[[ ! -e $TASKQ_DIR/stopped/t__a ]] && python3 "$HERE/taskq.py" status | grep "t/a " | grep -qv stopped'
kill -TERM $wpid; wait $wpid 2> /dev/null
fi

# 3c. 作业收到停止信号时训练不理 SIGTERM（11/1）：强制停，确认停下才交还；作业退出时训练进程都不在了
if want 3c; then
reset
table << EOF
t/d base
EOF
FAKE_SECS=60 FAKE_IGNORE=t/d SLURM_JOB_ID=900206 TASKQ_IDLE_MAX=60 RUN_TASK_STOP_WAIT=2 bash "$HERE/dicc_worker.sh" > "$T/w3c.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^start .* t/d" $T/train.log 2> /dev/null'
kill -TERM $wpid; wait $wpid; rc=$?
waitfor 5 '! pgrep -f "$T/fake_train.sh t/d" > /dev/null'  # 强制停之后进程表里消失要一点时间
check "停止信号、训练不理：强制停、交还，作业退出时假训练已经不在" '[[ $rc == 143 ]] && [[ -z $(ls $TASKQ_DIR/claims) ]] && ! pgrep -f "$T/fake_train.sh t/d" > /dev/null'
fi

# 4b. CPU worker（WORKER_DEVICE=cpu，10/24）：只领 device=cpu 的，一次 1 个，不记 GPU；GPU worker 不领 device=cpu 的
if want 4b; then
reset
table << EOF
t/g base
t/c1 base cpu
t/c2 base cpu
EOF
SLURM_JOB_ID=900207 WORKER_DEVICE=cpu TASKQ_SLOTS= timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w4b.out" 2>&1
check "CPU worker：两个纯 CPU 任务先后训完，不碰要卡的" '[[ $(grep -c "^end .* t/c" $T/train.log) == 2 ]] && ! grep -q "t/g" $T/train.log'
check "CPU worker：一次 1 个、不记 GPU" '[[ $(head -2 $T/train.log | grep -c ^start) == 1 && ! -e $TASKQ_DATA/logs/workers/worker_900207_gpu.csv ]] && grep -q "worker（cpu）.*最多同时 1 个" $T/w4b.out'
check "CPU worker：没有能领的就结束，不空等" 'grep -q "没有在训的，也没有能领的任务，作业结束" $T/w4b.out && ! grep -q "空着等" $T/w4b.out'
SLURM_JOB_ID=900208 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w4c.out" 2>&1
check "GPU worker：只训要卡的" 'grep -q "^end .* t/g" $T/train.log && [[ $(grep -c "^start .* t/c" $T/train.log) == 2 ]]'
fi

# 5 / 2r. run_task.sh 退出码 5（读不了盘）：交还、不记失败、本作业不马上再领；退出码 2：失败原因带上分段日志的最后一行（11/4）
#   代码副本里的 spec.py：FAKE_DONE5 这个文件在时 done 退出 5，FAKE_SHELL2 在时 shell 报错退出 2
if want 5; then
mkdir -p "$T/core"; rm -rf "$T/core/queue" "$T/core/sites"; cp -r "$HERE" "$T/core/queue"; cp -r "$HERE/../sites" "$T/core/sites"
sed -i '1a import os, sys\nif __name__ == "__main__" and sys.argv[1:2] == ["done"] and os.path.exists(os.environ.get("FAKE_DONE5", "/nonexistent")): sys.exit(5)\nif __name__ == "__main__" and sys.argv[1:2] == ["shell"] and os.path.exists(os.environ.get("FAKE_SHELL2", "/nonexistent")): sys.exit("spec.py：不认识的 {trainer}")' "$T/core/queue/spec.py"
reset
table << EOF
t/u base
EOF
touch "$T/unreadable"
FAKE_DONE5=$T/unreadable SLURM_JOB_ID=900209 TASKQ_SLOTS=1 timeout 60 bash "$T/core/queue/dicc_worker.sh" > "$T/w5.out" 2>&1
check "退出码 5：交还、不记失败，本作业不马上再领" '[[ $(grep -c "读不了盘、判断不了训完没有" $T/w5.out) == 1 && -z $(ls $TASKQ_DIR/claims) && ! -e $TASKQ_DIR/failures/t__u ]] && ! grep -q t/u $T/train.log'
rm -f "$T/unreadable"
reset
touch "$T/badspec"
FAKE_SHELL2=$T/badspec SLURM_JOB_ID=900210 TASKQ_SLOTS=1 timeout 60 bash "$T/core/queue/dicc_worker.sh" > "$T/w2r.out" 2>&1
check "退出码 2：失败原因带上分段日志里的原因" 'grep -q "退出码 2：没开训：任务说明有问题：spec.py：不认识的 {trainer}" $TASKQ_DIR/failures/t__u'
rm -f "$T/badspec"
fi

(( fails == 0 )) && { echo "全部通过"; rm -rf "$T"; } || { echo "$fails 项失败（文件在 $T）"; exit 1; }
