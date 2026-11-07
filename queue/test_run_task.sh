#!/bin/bash
# run_task.sh 的测试：command 做法、临时数据目录，按测试用的机器表（testdata/config）激活假的环境，不用 GPU。
#   bash queue/test_run_task.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'rm -rf "${T:?}"' EXIT
export TASKQ_DATA=$T RUN_TASK_POLL=1 TASK_JOBTAG=test-1
unset TASK_PART
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
SP=$(python3 "$HERE/spec.py" resolve command key=toy/s 'cmd=touch {data}/toy/s/DONE' env=base out=toy/s 'done=["DONE"]')
mkdir -p "$T/noplug"; TASKQ_PLUGINS=$T/noplug $R --spec "$SP" > /dev/null; rc=$?
check "--spec：按 hub 解析好的说明跑，执行机上没有做法插件也行" "(( rc == 0 )) && [[ -e $T/toy/s/DONE ]] && head -1 $T/logs/tasks/toy__s__test-1_part1.log | grep -q '任务=toy/s'"
$R --spec '{"key": "x"}' > /dev/null 2>&1; rc=$?
check "--spec 说明缺字段：退出码 2" "(( rc == 2 ))"
check "说明有问题：原因写进这个任务的分段日志（第一行照常）" "head -1 $T/logs/tasks/x__test-1_part1.log | grep -q '任务=x，' && grep -q '没开训：任务说明有问题：.' $T/logs/tasks/x__test-1_part1.log"
check "环境没有：原因写进分段日志" "grep -q '没开训：这台机器没有环境 nosuchenv' $T/logs/tasks/toy__d__test-1_part1.log"
# 读不了盘（spec.py done 退出 5）：代码副本里的 spec.py 在 FAKE_DONE5 这个文件在时 done 退出 5
mkdir -p "$T/core"; cp -r "$HERE" "$T/core/queue"; cp -r "$HERE/../sites" "$T/core/sites"
sed -i '1a import os, sys\nif __name__ == "__main__" and sys.argv[1:2] == ["done"] and os.path.exists(os.environ.get("FAKE_DONE5", "/nonexistent")): sys.exit(5)' "$T/core/queue/spec.py"
R5="env FAKE_DONE5=$T/unreadable bash $T/core/queue/run_task.sh"
touch "$T/unreadable"
$R5 command key=toy/u1 'cmd=touch $TASKQ_DATA/toy/u1/ran' env=base out=toy/u1 'done=["DONE"]' > /dev/null 2>&1; rc=$?
check "开训前读不了盘：退出码 5、不跑命令、日志写原因" "(( rc == 5 )) && [[ ! -e $T/toy/u1/ran ]] && grep -q '没开训：读不了盘' $T/logs/tasks/toy__u1__test-1_part1.log"
rm -f "$T/unreadable"
RUN_TASK_TRIES=3 $R5 command key=toy/u2 "cmd=touch $T/unreadable; exit 1" env=base out=toy/u2 'done=["DONE"]' > /dev/null 2>&1; rc=$?
check "命令结束后读不了盘：退出码 5（不当成没训完、不重试）" "(( rc == 5 )) && grep -q '读不了盘，判断不了训完没有' $T/logs/tasks/toy__u2__test-1_part1.log && ! grep -q '第 1 次' $T/logs/tasks/toy__u2__test-1_part1.log"
rm -f "$T/unreadable"
# desc：某个进程的所有后代；started：等后代里出现命令行正好是 $2 的进程（最多 60 秒），打印后代里命令行含 $2 的
desc() { local c; for c in $(pgrep -P "$1"); do echo "$c"; desc "$c"; done; }
started() {
    local p n
    for (( n = 0; n < 600; n++ )); do
        for p in $(desc "$1"); do
            if [[ $(ps -o args= -p "$p" 2> /dev/null) == "$2" ]]; then
                for p in $(desc "$1"); do ps -o args= -p "$p" 2> /dev/null | grep -qF "$2" && echo "$p"; done
                return 0
            fi
        done
        sleep 0.1
    done
    return 1
}
$R command key=toy/e 'cmd=sleep 300' env=base out=toy/e 'done=["DONE"]' > /dev/null & rp=$!
child=$(started $rp "sleep 300" | tr '\n' ' ')
kill -TERM $rp; wait $rp; rc=$?
for _ in $(seq 10); do ps -o args= -p ${child// /,}0 2> /dev/null | grep -q 'sleep 300' || break; sleep 0.1; done
check "SIGTERM：退出码 143、训练进程一起停" "(( rc == 143 )) && [[ -n '$child' ]] && ! pgrep -P $rp > /dev/null && ! ps -o args= -p ${child// /,}0 2> /dev/null | grep -q 'sleep 300'"
# 训练不理 SIGTERM：run_task.sh 等 RUN_TASK_STOP_WAIT 秒后 SIGKILL 整个进程组，退出时训练已经不在
RUN_TASK_STOP_WAIT=2 $R command key=toy/i "cmd=trap '' TERM; sleep 307 & wait; sleep 307" env=base out=toy/i 'done=["DONE"]' > /dev/null & rp=$!
kids=$(started $rp "sleep 307" | tr '\n' ' ')
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
gpu 99 "99999999, 1000" 1000
$G SLURM_JOB_ID=1 $R command key=toy/w5 'cmd=touch $TASKQ_DATA/toy/w5/DONE' env=base out=toy/w5 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "Slurm 里不查卡" "(( rc == 0 )) && [[ ! -e $T/gpu/n ]]"
# 空满 TASK_GPU_IDLE 分钟才跑：GPU 使用记录（gpu_log.sh 的格式）里最近有别人 → 不跑
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

# 机器表的 task_vars：只在本机（TASKQ_MACHINE，代码包、worker 设的）这台导出给任务，已经设了（非空）的不改；task_unset：任务的环境里去掉。
# 测试用的机器表里 site-pueue 有 task_vars（WS_TASK_VARS）；副本 cfg_tv 再给它加上 task_unset=("DEMO_UNSET",)
mkdir -p "$T/cfg_tv"; cp -r "$TASKQ_CONFIG/." "$T/cfg_tv/"
python3 - "$T/cfg_tv/machines.py" "$(dirname "$TASKQ_CONFIG")" << 'PY'
import re, sys
p, fx = sys.argv[1:]
s = open(p).read()
s = re.sub(r"^FAKE = .*$", "FAKE = " + repr(fx), s, count=1, flags=re.M)
s += "MACHINES['site-pueue']['task_unset'] = ('DEMO_UNSET',)\n"
open(p, "w").write(s)
PY
is_machine() { [[ $(TASKQ_CONFIG=$T/cfg_tv TASKQ_MACHINE=$1 python3 "$HERE/../sites/mtab.py" whoami) == "$1" ]]; }
VC='cmd=echo "${DEMO_FAST:-无}/${DEMO_FAST_LOADER:-无}" > $TASKQ_DATA/toy/v.vars; echo "${DEMO_UNSET:-无}" > $TASKQ_DATA/toy/v.unset; touch $TASKQ_DATA/toy/v/DONE'
tvrun() { rm -rf "$T/toy/v"; env -u DEMO_FAST -u DEMO_FAST_LOADER DEMO_UNSET=job TASKQ_CONFIG=$T/cfg_tv TASKQ_MACHINE=$1 "${@:2}" $R command key=toy/v "$VC" env=base out=toy/v 'done=["DONE"]' > /dev/null; }
check "测试用的机器表副本：TASKQ_MACHINE 认成 site-pueue / hub / site-slurm" "is_machine site-pueue && is_machine hub && is_machine site-slurm"
tvrun site-pueue; rc=$?
check "site-pueue（机器表里有 task_vars）：导出给任务" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == 1/1 ]]"
check "site-pueue（机器表里有 task_unset）：worker 环境里设的，任务里去掉" "[[ \$(cat $T/toy/v.unset) == 无 ]]"
tvrun site-pueue DEMO_FAST=0; rc=$?
check "site-pueue：已经设了的不改，没设的照样导出" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == 0/1 ]]"
tvrun hub; rc=$?
check "hub（没有 task_vars）：不导出" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == 无/无 ]]"
check "hub（没有 task_unset）：照样传给任务" "[[ \$(cat $T/toy/v.unset) == job ]]"
tvrun site-slurm; rc=$?
check "site-slurm（没有 task_vars）：不导出" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == 无/无 ]]"

# 任务的命令看得到核心：TASKQ_LIB = 核心的 queue/，核心的 bin/ 在 PATH 最前面
$R command key=toy/lib 'cmd=echo "$TASKQ_LIB|${PATH%%:*}" > $TASKQ_DATA/toy/lib.txt; touch $TASKQ_DATA/toy/lib/DONE' env=base out=toy/lib 'done=["DONE"]' > /dev/null; rc=$?
check "任务的环境：TASKQ_LIB、PATH 前面是核心的 bin/" "(( rc == 0 )) && [[ \$(cat $T/toy/lib.txt) == \"$(cd "$HERE" && pwd)|$(cd "$HERE/.." && pwd)/bin\" ]]"

TASK_PART=2 $R command key=toy/o2 'cmd=touch $TASKQ_DATA/toy/o2/DONE' env=base out=toy/o2 'done=["DONE"]' > /dev/null; rc=$?
check "新 worker：TASK_PART=2 → 日志 _part2.log，第一行记第 2 段" "(( rc == 0 )) && head -1 $T/logs/tasks/toy__o2__test-1_part2.log | grep -q '第 2 段'"
(( ok )) && echo "全部通过" || { echo "有失败"; exit 1; }
