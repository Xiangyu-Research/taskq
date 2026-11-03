#!/bin/bash
# run_task.sh 的测试（2026-10-18）：command 做法、临时数据目录，按测试用的机器表（testdata/config）激活假的环境，不用 GPU。
#   bash scripts/queue/test_run_task.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
export TASKQ_DATA=$T RUN_TASK_POLL=1 TASK_JOBTAG=test-1
unset BRAIN_TUMOR TASK_PART TASK_SEG
unset CUDA_VISIBLE_DEVICES SLURM_JOB_ID
ok=1
check() { if eval "$2"; then echo "通过：$1"; else echo "失败：$1"; ok=0; fi; }
R="bash $HERE/run_task.sh"

$R command key=toy/a 'cmd=echo "conda=${CONDA_DEFAULT_ENV:-无} venv=${DEMO_ACTIVE_VENV:-无}"; touch $TASKQ_DATA/toy/a/DONE' env=base out=toy/a 'done=["DONE"]' > "$T/o1"; rc=$?
L=$T/logs/tasks/toy__a__test-1_part1.log
check "训完：退出码 0" "(( rc == 0 ))"
check "分段日志第一行记作业、任务、环境、代码" "head -1 '$L' | grep -q '^作业 test-1_1 @ .*任务=toy/a，第 1 段，环境 base，代码 '"
check "命令在 base 环境里跑（机器表查表：本机 base 是 conda 环境 base0）" "grep -q '^conda=base0 venv=无$' '$L'"
check "日志最后记训完" "tail -1 '$L' | grep -q '^训完'"
$R command key=toy/a 'cmd=false' env=base out=toy/a 'done=["DONE"]' > "$T/o2"; rc=$?
check "已经训完的不再跑：退出码 0" "(( rc == 0 )) && grep -q 已经训完 '$T/o2'"
$R command key=toy/b 'cmd=true' env=base out=toy/b 'done=["DONE"]' > /dev/null; rc=$?
check "命令正常结束但没训完：退出码 3" "(( rc == 3 ))"
RUN_TASK_TRIES=2 $R command key=toy/c 'cmd=echo run >> $TASKQ_DATA/toy/c/n; exit 7' env=base out=toy/c 'done=["DONE"]' > /dev/null; rc=$?
check "训练出错：重来到次数用完，退出码 1，日志里有命令的退出码" "(( rc == 1 )) && (( \$(wc -l < '$T/toy/c/n') == 2 )) && grep -q '退出码 7' $T/logs/tasks/toy__c__test-1_part1.log"
RUN_TASK_TRIES=1 $R command key=toy/f 'cmd=exit 2' env=base out=toy/f 'done=["DONE"]' > /dev/null; rc=$?
check "训练命令退出 2：run_task 退出 1（2 只留给任务说明、环境）" "(( rc == 1 )) && grep -q '退出码 2' $T/logs/tasks/toy__f__test-1_part1.log"
RUN_TASK_TRIES=1 $R command key=toy/g 'cmd=kill -KILL $$' env=base out=toy/g 'done=["DONE"]' > /dev/null; rc=$?
check "训练命令被信号杀掉：退出码原样（137）" "(( rc == 137 ))"
$R command key=toy/d 'cmd=true' env=nosuchenv out=toy/d 'done=["DONE"]' > /dev/null 2>&1; rc=$?
check "这台机器没有的环境：退出码 2" "(( rc == 2 ))"
$R toy ds=D101_alpha frag=0 variantt=x > /dev/null 2>&1; rc=$?
check "参数写错：退出码 2" "(( rc == 2 ))"
$R command key=toy/e 'cmd=sleep 300' env=base out=toy/e 'done=["DONE"]' > /dev/null & rp=$!
sleep 3
child=$(pgrep -f "sleep 300" -P "$(pgrep -P $rp | head -1)" 2> /dev/null; pgrep -P "$(pgrep -P "$(pgrep -P $rp | head -1)" | head -1)" 2> /dev/null)
kill -TERM $rp; wait $rp; rc=$?
sleep 1
check "SIGTERM：退出码 143、训练进程一起停" "(( rc == 143 )) && ! pgrep -P $rp > /dev/null && ! ps -o args= -p ${child:-0} 2> /dev/null | grep -q 'sleep 300'"
# 训练不理 SIGTERM（11/1）：run_task.sh 等 RUN_TASK_STOP_WAIT 秒后 SIGKILL 整个进程组，退出时训练已经不在
RUN_TASK_STOP_WAIT=2 $R command key=toy/i "cmd=trap '' TERM; sleep 307 & wait; sleep 307" env=base out=toy/i 'done=["DONE"]' > /dev/null & rp=$!
sleep 3
kids=$(pgrep -f "sleep 307" | tr '\n' ' ')
kill -TERM $rp; wait $rp; rc=$?
check "训练不理 SIGTERM：强制停，run_task.sh 退出时训练进程都不在了" "(( rc == 143 )) && [[ -n '$kids' ]] && ! ps -o pid= -p ${kids// /,}0 2> /dev/null | grep -q . && grep -q 强制停 $T/logs/tasks/toy__i__test-1_part1.log"
# 等卡（res.wait_gpu，10/31）：假 nvidia-smi，前 $T/gpu/busy 次查进程时报 $T/gpu/apps 里的进程，之后报空；OTHER 是别的用户的一个进程
OTHER=$(ps -eo pid=,user= | awk -v me="$(id -un)" '$2 != me {print $1; exit}')
mkdir -p "$T/bin" "$T/gpu"
cat > "$T/bin/nvidia-smi" << 'FAKE'
#!/bin/bash
G=$TASKQ_DATA/gpu
case "$*" in
    *query-compute-apps*) n=$(cat "$G/n" 2> /dev/null || echo 0); echo $(( n + 1 )) > "$G/n"
                          (( n < $(cat "$G/busy") )) && cat "$G/apps" ;;
    *query-gpu*) cat "$G/used" ;;
esac
exit 0
FAKE
chmod +x "$T/bin/nvidia-smi"
gpu() { echo "$1" > "$T/gpu/busy"; echo "$2" > "$T/gpu/apps"; echo "$3" > "$T/gpu/used"; rm -f "$T/gpu/n"; }
G="env PATH=$T/bin:$PATH CUDA_VISIBLE_DEVICES=0 RUN_TASK_GPU_POLL=1"
if [[ -n $OTHER ]]; then
gpu 2 "$OTHER, 1000" 0
$G $R command key=toy/w1 'cmd=touch $TASKQ_DATA/toy/w1/DONE' env=base out=toy/w1 'done=["DONE"]' > /dev/null; rc=$?
check "卡上有别人的进程：等到空出来再跑（没给 wait_gpu = 一直等），每次看都记进日志" "(( rc == 0 )) && (( \$(grep -c 等它空出来 $T/logs/tasks/toy__w1__test-1_part1.log) == 2 ))"
gpu 2 "$OTHER, 1000" 0
$G $R command key=toy/w2 'cmd=touch $TASKQ_DATA/toy/w2/DONE' env=base out=toy/w2 'done=["DONE"]' 'res={"wait_gpu": 5}' > /dev/null; rc=$?
check "wait_gpu 以内空出来：照常跑" "(( rc == 0 ))"
gpu 99 "$OTHER, 1000" 0
$G $R command key=toy/w3 'cmd=touch $TASKQ_DATA/toy/w3/DONE' env=base out=toy/w3 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "等满 wait_gpu 还被占：退出码 1、命令没跑、日志写原因" "(( rc == 1 )) && [[ ! -e $T/toy/w3/DONE ]] && grep -q '还没空出来，不跑' $T/logs/tasks/toy__w3__test-1_part1.log"
else
echo "跳过：本机看不到别的用户的进程，等卡的三项不测"
fi
gpu 99 "$$, 3000" 3000
$G $R command key=toy/w4 'cmd=touch $TASKQ_DATA/toy/w4/DONE' env=base out=toy/w4 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "卡上只有自己的进程（worker、显存填充）：不等" "(( rc == 0 )) && ! grep -q 等它空出来 $T/logs/tasks/toy__w4__test-1_part1.log"
gpu 99 "1, 1000" 1000
$G SLURM_JOB_ID=1 $R command key=toy/w5 'cmd=touch $TASKQ_DATA/toy/w5/DONE' env=base out=toy/w5 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "Slurm 里不查卡" "(( rc == 0 )) && [[ ! -e $T/gpu/n ]]"
# 空满 TASK_GPU_IDLE 分钟才跑（11/2）：GPU 使用记录（gpu_log.sh 的格式）里最近有别人 → 不跑
mkdir -p "$T/gl"; GL="$T/gl/gpu_$(date +%Y-%m).csv"
glrow() { echo "time,gpu,util_pct,mem_used_mib,mem_total_mib,user,pid,proc_mem_mib,program" > "$GL"; echo "$(date -d "-$1 min" '+%F %T'),$2,90,16000,32000,$3,123,16000,train.py" >> "$GL"; echo "$(date '+%F %T'),0,0,100,32000,,,," >> "$GL"; }
I="$G GPU_LOG_DIR=$T/gl TASK_GPU_IDLE=60"
gpu 0 "" 0; glrow 10 0 someoneelse
$I $R command key=toy/q1 'cmd=touch $TASKQ_DATA/toy/q1/DONE' env=base out=toy/q1 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "别人 10 分钟前还用这张卡：不跑，日志写还要空多久" "(( rc == 1 )) && [[ ! -e $T/toy/q1/DONE ]] && grep -q '要空满 60 分钟' $T/logs/tasks/toy__q1__test-1_part1.log"
gpu 0 "" 0; glrow 90 0 someoneelse
$I $R command key=toy/q2 'cmd=touch $TASKQ_DATA/toy/q2/DONE' env=base out=toy/q2 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "别人 90 分钟前用过：照常跑" "(( rc == 0 ))"
gpu 0 "" 0; glrow 10 1 someoneelse
$I $R command key=toy/q3 'cmd=touch $TASKQ_DATA/toy/q3/DONE' env=base out=toy/q3 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "别人用的是另一张卡：照常跑" "(( rc == 0 ))"
gpu 0 "" 0; glrow 10 0 "$(id -un)"
$I $R command key=toy/q4 'cmd=touch $TASKQ_DATA/toy/q4/DONE' env=base out=toy/q4 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "记录里只有自己：照常跑" "(( rc == 0 ))"
gpu 0 "" 0; glrow 10 0 someoneelse
$G GPU_LOG_DIR=$T/gl $R command key=toy/q5 'cmd=touch $TASKQ_DATA/toy/q5/DONE' env=base out=toy/q5 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "没设 TASK_GPU_IDLE：不看记录" "(( rc == 0 ))"

# 机器表的 task_vars（11/2）：只在定义它的机器上导出给任务，已经设了（非空）的不改。as_machine：本机当成 <名字> 的测试用机器表副本
as_machine() {
    mkdir -p "$T/cfg_$1"; cp -r "$TASKQ_CONFIG/." "$T/cfg_$1/"
    python3 - "$T/cfg_$1/machines.py" "$1" "$(hostname -s)" "$(dirname "$TASKQ_CONFIG")" << 'PY'
import re, sys
p, name, host, fx = sys.argv[1:]
s = open(p).read()
s = re.sub(r"^FAKE = .*$", "FAKE = " + repr(fx), s, count=1, flags=re.M)
mark = "# ---------------- LOGIC ----------------"
s = s.replace(mark, f"for _n, _m in MACHINES.items():\n    _m['hostname'] = {re.escape(host)!r} if _n == {name!r} else r'(?!)'\n" + mark, 1)
open(p, "w").write(s)
PY
    [[ $(TASKQ_CONFIG=$T/cfg_$1 python3 "$T/cfg_$1/machines.py" whoami) == "$1" ]]
}
VC='cmd=echo "${DEMO_FAST:-无}/${DEMO_FAST_LOADER:-无}" > $TASKQ_DATA/toy/v.vars; touch $TASKQ_DATA/toy/v/DONE'
tvrun() { rm -rf "$T/toy/v"; env -u DEMO_FAST -u DEMO_FAST_LOADER TASKQ_CONFIG=$T/cfg_$1 "${@:2}" $R command key=toy/v "$VC" env=base out=toy/v 'done=["DONE"]' > /dev/null; }
check "测试用的机器表副本：本机认成 lab / box / dicc" "as_machine lab && as_machine box && as_machine dicc"
tvrun lab; rc=$?
check "lab（有 task_vars）：导出给任务" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == 1/1 ]]"
tvrun lab DEMO_FAST=0; rc=$?
check "lab：已经设了的不改，没设的照样导出" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == 0/1 ]]"
tvrun box; rc=$?
check "box（没有 task_vars）：不导出" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == 无/无 ]]"
tvrun dicc; rc=$?
check "dicc（没有 task_vars）：不导出" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == 无/无 ]]"

# 旧名（10/31 前）：只设 BRAIN_TUMOR 的调用方、传 TASK_SEG 的旧 worker（它按 _seg<段>.log 找日志）
env -u TASKQ_DATA BRAIN_TUMOR=$T TASK_SEG=1 $R command key=toy/o1 'cmd=touch $TASKQ_DATA/toy/o1/DONE; touch $BRAIN_TUMOR/toy/o1/B' env=base out=toy/o1 'done=["DONE"]' > /dev/null; rc=$?
check "旧调用方：只给 BRAIN_TUMOR、TASK_SEG=1 → 照常跑，两个变量都设，日志叫 _seg1.log" "(( rc == 0 )) && [[ -e $T/toy/o1/B && -f $T/logs/tasks/toy__o1__test-1_seg1.log ]]"
TASK_PART=2 $R command key=toy/o2 'cmd=touch $TASKQ_DATA/toy/o2/DONE' env=base out=toy/o2 'done=["DONE"]' > /dev/null; rc=$?
check "新 worker：TASK_PART=2 → 日志 _part2.log，第一行记第 2 段" "(( rc == 0 )) && head -1 $T/logs/tasks/toy__o2__test-1_part2.log | grep -q '第 2 段'"
rm -rf "$T"
(( ok )) && echo "全部通过" || { echo "有失败"; exit 1; }
