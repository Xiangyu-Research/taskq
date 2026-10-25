#!/bin/bash
# run_task.sh 的测试（2026-10-18）：command 做法、临时数据目录，真的激活本机的 nnunet 环境，不用 GPU。  bash scripts/queue/test_run_task.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
T=$(mktemp -d)
export BRAIN_TUMOR=$T RUN_TASK_POLL=1 TASK_JOBTAG=test-1
unset CUDA_VISIBLE_DEVICES SLURM_JOB_ID
ok=1
check() { if eval "$2"; then echo "通过：$1"; else echo "失败：$1"; ok=0; fi; }
R="bash $HERE/run_task.sh"

$R command key=toy/a 'cmd=python -c "import sys; print(sys.executable)"; touch $BRAIN_TUMOR/toy/a/DONE' env=nnunet out=toy/a 'done=["DONE"]' > "$T/o1"; rc=$?
L=$T/logs/tasks/toy__a__test-1_seg1.log
check "训完：退出码 0" "(( rc == 0 ))"
check "分段日志第一行记作业、任务、环境、代码" "head -1 '$L' | grep -q '^作业 test-1_1 @ .*任务=toy/a，第 1 段，环境 nnunet，代码 '"
check "命令在 nnunet 环境里跑（envs/machines.py 查表）" "grep -q 'envs/nnunetv2/bin/python' '$L'"
check "日志最后记训完" "tail -1 '$L' | grep -q '^训完'"
$R command key=toy/a 'cmd=false' env=nnunet out=toy/a 'done=["DONE"]' > "$T/o2"; rc=$?
check "已经训完的不再跑：退出码 0" "(( rc == 0 )) && grep -q 已经训完 '$T/o2'"
$R command key=toy/b 'cmd=true' env=nnunet out=toy/b 'done=["DONE"]' > /dev/null; rc=$?
check "命令正常结束但没训完：退出码 3" "(( rc == 3 ))"
RUN_TASK_TRIES=2 $R command key=toy/c 'cmd=echo run >> $BRAIN_TUMOR/toy/c/n; exit 7' env=nnunet out=toy/c 'done=["DONE"]' > /dev/null; rc=$?
check "训练出错：重来到次数用完，退出码 1，日志里有命令的退出码" "(( rc == 1 )) && (( \$(wc -l < '$T/toy/c/n') == 2 )) && grep -q '退出码 7' $T/logs/tasks/toy__c__test-1_seg1.log"
RUN_TASK_TRIES=1 $R command key=toy/f 'cmd=exit 2' env=nnunet out=toy/f 'done=["DONE"]' > /dev/null; rc=$?
check "训练命令退出 2：run_task 退出 1（2 只留给任务说明、环境）" "(( rc == 1 )) && grep -q '退出码 2' $T/logs/tasks/toy__f__test-1_seg1.log"
RUN_TASK_TRIES=1 $R command key=toy/g 'cmd=kill -KILL $$' env=nnunet out=toy/g 'done=["DONE"]' > /dev/null; rc=$?
check "训练命令被信号杀掉：退出码原样（137）" "(( rc == 137 ))"
$R command key=toy/d 'cmd=true' env=nosuchenv out=toy/d 'done=["DONE"]' > /dev/null 2>&1; rc=$?
check "这台机器没有的环境：退出码 2" "(( rc == 2 ))"
$R nnunet dataset=Dataset101_GLIrq1 fold=0 trainr=x > /dev/null 2>&1; rc=$?
check "参数写错：退出码 2" "(( rc == 2 ))"
$R command key=toy/e 'cmd=sleep 300' env=nnunet out=toy/e 'done=["DONE"]' > /dev/null & rp=$!
sleep 3
child=$(pgrep -f "sleep 300" -P "$(pgrep -P $rp | head -1)" 2> /dev/null; pgrep -P "$(pgrep -P "$(pgrep -P $rp | head -1)" | head -1)" 2> /dev/null)
kill -TERM $rp; wait $rp; rc=$?
sleep 1
check "SIGTERM：退出码 143、训练进程一起停" "(( rc == 143 )) && ! pgrep -P $rp > /dev/null && ! ps -o args= -p ${child:-0} 2> /dev/null | grep -q 'sleep 300'"
rm -rf "$T"
(( ok )) && echo "全部通过" || { echo "有失败"; exit 1; }
