#!/bin/bash
# Tests for run_task.sh: the command recipe, a temp data dir, fake envs activated from the test machine table (testdata/config), no GPU.
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
check() { if eval "$2"; then echo "PASS:$1"; else echo "FAIL:$1"; ok=0; fi; }
R="bash $HERE/run_task.sh"

$R command key=toy/a 'cmd=echo "conda=${CONDA_DEFAULT_ENV:-none} venv=${DEMO_ACTIVE_VENV:-none}"; touch $TASKQ_DATA/toy/a/DONE' env=base out=toy/a 'done=["DONE"]' > "$T/o1"; rc=$?
L=$T/logs/tasks/toy__a__test-1_part1.log
check "done: exit 0" "(( rc == 0 ))"
check "the first part log line records job, task, env and code" "head -1 '$L' | grep -q '^job test-1_1 @ .*task=toy/a, part 1, env base, code '"
check "the command runs in the base env (machine table: local base is conda env base0)" "grep -q '^conda=base0 venv=none$' '$L'"
check "the log ends with done" "tail -1 '$L' | grep -q '^DONE'"
$R command key=toy/a 'cmd=false' env=base out=toy/a 'done=["DONE"]' > "$T/o2"; rc=$?
check "already done: not run again, exit 0" "(( rc == 0 )) && grep -q 'already done' '$T/o2'"
$R command key=toy/b 'cmd=true' env=base out=toy/b 'done=["DONE"]' > /dev/null; rc=$?
check "command exited normally but training did not finish: exit 3" "(( rc == 3 ))"
RUN_TASK_TRIES=2 $R command key=toy/c 'cmd=echo run >> $TASKQ_DATA/toy/c/n; exit 7' env=base out=toy/c 'done=["DONE"]' > /dev/null; rc=$?
check "training fails: retried until attempts run out, exit 1, the command exit code is in the log" "(( rc == 1 )) && (( \$(wc -l < '$T/toy/c/n') == 2 )) && grep -q 'exit 7' $T/logs/tasks/toy__c__test-1_part1.log"
RUN_TASK_TRIES=1 $R command key=toy/f 'cmd=exit 2' env=base out=toy/f 'done=["DONE"]' > /dev/null; rc=$?
check "training command exits 2: run_task exits 1 (2 is only for bad spec or env)" "(( rc == 1 )) && grep -q 'exit 2' $T/logs/tasks/toy__f__test-1_part1.log"
RUN_TASK_TRIES=1 $R command key=toy/g 'cmd=kill -KILL $$' env=base out=toy/g 'done=["DONE"]' > /dev/null; rc=$?
check "training command killed by a signal: exit code passed through (137)" "(( rc == 137 ))"
$R command key=toy/d 'cmd=true' env=nosuchenv out=toy/d 'done=["DONE"]' > /dev/null 2>&1; rc=$?
check "env missing on this machine: exit 2" "(( rc == 2 ))"
$R toy ds=D101_alpha frag=0 variantt=x > /dev/null 2>&1; rc=$?
check "bad params: exit 2" "(( rc == 2 ))"
SP=$(python3 "$HERE/spec.py" resolve command key=toy/s 'cmd=touch {data}/toy/s/DONE' env=base out=toy/s 'done=["DONE"]')
mkdir -p "$T/noplug"; TASKQ_PLUGINS=$T/noplug $R --spec "$SP" > /dev/null; rc=$?
check "--spec: run the spec resolved by the hub, without the recipe plugin on the executor" "(( rc == 0 )) && [[ -e $T/toy/s/DONE ]] && head -1 $T/logs/tasks/toy__s__test-1_part1.log | grep -q 'task=toy/s'"
$R --spec '{"key": "x"}' > /dev/null 2>&1; rc=$?
check "--spec with missing fields: exit 2" "(( rc == 2 ))"
check "bad spec: the reason goes into the task's part log (header line as usual)" "head -1 $T/logs/tasks/x__test-1_part1.log | grep -q 'task=x,' && grep -q 'not started: bad task spec: .' $T/logs/tasks/x__test-1_part1.log"
check "missing env: the reason goes into the part log" "grep -q 'not started: this machine has no env nosuchenv' $T/logs/tasks/toy__d__test-1_part1.log"
# unreadable disk (spec.py done exits 5): the copy's spec.py exits 5 from done while the FAKE_DONE5 file exists
mkdir -p "$T/core"; cp -r "$HERE" "$T/core/queue"; cp -r "$HERE/../sites" "$T/core/sites"
sed -i '1a import os, sys\nif __name__ == "__main__" and sys.argv[1:2] == ["done"] and os.path.exists(os.environ.get("FAKE_DONE5", "/nonexistent")): sys.exit(5)' "$T/core/queue/spec.py"
R5="env FAKE_DONE5=$T/unreadable bash $T/core/queue/run_task.sh"
touch "$T/unreadable"
$R5 command key=toy/u1 'cmd=touch $TASKQ_DATA/toy/u1/ran' env=base out=toy/u1 'done=["DONE"]' > /dev/null 2>&1; rc=$?
check "unreadable before start: exit 5, command not run, reason in the log" "(( rc == 5 )) && [[ ! -e $T/toy/u1/ran ]] && grep -q 'not started: disk unreadable' $T/logs/tasks/toy__u1__test-1_part1.log"
rm -f "$T/unreadable"
RUN_TASK_TRIES=3 $R5 command key=toy/u2 "cmd=touch $T/unreadable; exit 1" env=base out=toy/u2 'done=["DONE"]' > /dev/null 2>&1; rc=$?
check "unreadable after the command: exit 5 (not treated as not done, no retry)" "(( rc == 5 )) && grep -q 'disk unreadable, cannot tell whether training finished' $T/logs/tasks/toy__u2__test-1_part1.log && ! grep -q 'attempt 1' $T/logs/tasks/toy__u2__test-1_part1.log"
rm -f "$T/unreadable"
# desc: all descendants of a process; started: wait for a descendant whose command line is exactly $2 (up to 60 seconds) and print descendants whose command line contains $2
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
check "SIGTERM: exit 143, the training process stops too" "(( rc == 143 )) && [[ -n '$child' ]] && ! pgrep -P $rp > /dev/null && ! ps -o args= -p ${child// /,}0 2> /dev/null | grep -q 'sleep 300'"
# training ignores SIGTERM: run_task.sh waits RUN_TASK_STOP_WAIT seconds, SIGKILLs the whole process group, and training is gone when it exits
RUN_TASK_STOP_WAIT=2 $R command key=toy/i "cmd=trap '' TERM; sleep 307 & wait; sleep 307" env=base out=toy/i 'done=["DONE"]' > /dev/null & rp=$!
kids=$(started $rp "sleep 307" | tr '\n' ' ')
kill -TERM $rp; wait $rp; rc=$?
check "training ignores SIGTERM: forced stop, no training processes left when run_task.sh exits" "(( rc == 143 )) && [[ -n '$kids' ]] && ! ps -o pid= -p ${kids// /,}0 2> /dev/null | grep -q . && grep -q forcing $T/logs/tasks/toy__i__test-1_part1.log"
# GPU wait (res.wait_gpu): fake nvidia-smi reports the processes in $T/gpu/apps for the first $T/gpu/busy checks, then none; OTHER is a process of another user
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
check "someone else's process on the GPU: wait for it (no wait_gpu = wait forever), every check logged" "(( rc == 0 )) && (( \$(grep -c 'waiting for it' $T/logs/tasks/toy__w1__test-1_part1.log) == 2 ))"
gpu 2 "$OTHER, 1000" 0
$G $R command key=toy/w2 'cmd=touch $TASKQ_DATA/toy/w2/DONE' env=base out=toy/w2 'done=["DONE"]' 'res={"wait_gpu": 5}' > /dev/null; rc=$?
check "GPU free within wait_gpu: run as usual" "(( rc == 0 ))"
gpu 99 "$OTHER, 1000" 0
$G $R command key=toy/w3 'cmd=touch $TASKQ_DATA/toy/w3/DONE' env=base out=toy/w3 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "still busy after wait_gpu: exit 1, command not run, reason in the log" "(( rc == 1 )) && [[ ! -e $T/toy/w3/DONE ]] && grep -q 'not running' $T/logs/tasks/toy__w3__test-1_part1.log"
else
echo "SKIP:no processes of other users visible here, the three GPU wait checks are not run"
fi
gpu 99 "$$, 3000" 3000
$G $R command key=toy/w4 'cmd=touch $TASKQ_DATA/toy/w4/DONE' env=base out=toy/w4 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "only our own processes on the GPU (worker, GPU pad): do not wait" "(( rc == 0 )) && ! grep -q 'waiting for it' $T/logs/tasks/toy__w4__test-1_part1.log"
gpu 99 "1, 1000" 1000
$G SLURM_JOB_ID=1 $R command key=toy/w5 'cmd=touch $TASKQ_DATA/toy/w5/DONE' env=base out=toy/w5 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "no GPU check in Slurm" "(( rc == 0 )) && [[ ! -e $T/gpu/n ]]"
# require TASK_GPU_IDLE minutes of quiet: someone else in the recent GPU usage records (gpu_log.sh format) -> do not run
mkdir -p "$T/gl"; GL="$T/gl/gpu_$(date +%Y-%m).csv"
glrow() { echo "time,gpu,util_pct,mem_used_mib,mem_total_mib,user,pid,proc_mem_mib,program" > "$GL"; echo "$(date -d "-$1 min" '+%F %T'),$2,90,16000,32000,$3,123,16000,train.py" >> "$GL"; echo "$(date '+%F %T'),0,0,100,32000,,,," >> "$GL"; }
I="$G GPU_LOG_DIR=$T/gl TASK_GPU_IDLE=60"
gpu 0 "" 0; glrow 10 0 someoneelse
$I $R command key=toy/q1 'cmd=touch $TASKQ_DATA/toy/q1/DONE' env=base out=toy/q1 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "someone used this GPU 10 minutes ago: do not run, the log says how long it must stay free" "(( rc == 1 )) && [[ ! -e $T/toy/q1/DONE ]] && grep -q 'needs 60 minutes free' $T/logs/tasks/toy__q1__test-1_part1.log"
gpu 0 "" 0; glrow 90 0 someoneelse
$I $R command key=toy/q2 'cmd=touch $TASKQ_DATA/toy/q2/DONE' env=base out=toy/q2 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "someone used it 90 minutes ago: run as usual" "(( rc == 0 ))"
gpu 0 "" 0; glrow 10 1 someoneelse
$I $R command key=toy/q3 'cmd=touch $TASKQ_DATA/toy/q3/DONE' env=base out=toy/q3 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "someone used another GPU: run as usual" "(( rc == 0 ))"
gpu 0 "" 0; glrow 10 0 "$(id -un)"
$I $R command key=toy/q4 'cmd=touch $TASKQ_DATA/toy/q4/DONE' env=base out=toy/q4 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "only ourselves in the records: run as usual" "(( rc == 0 ))"
gpu 0 "" 0; glrow 10 0 someoneelse
$G GPU_LOG_DIR=$T/gl $R command key=toy/q5 'cmd=touch $TASKQ_DATA/toy/q5/DONE' env=base out=toy/q5 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "TASK_GPU_IDLE unset: records are not consulted" "(( rc == 0 ))"

# machine table task_vars: exported to tasks only on this machine (TASKQ_MACHINE, set by bundles and workers); already set (non-empty) values are kept; task_unset: removed from the task env.
# in the test machine table site-pueue has task_vars (WS_TASK_VARS); the cfg_tv copy also gives it task_unset=("DEMO_UNSET",)
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
VC='cmd=echo "${DEMO_FAST:-none}/${DEMO_FAST_LOADER:-none}" > $TASKQ_DATA/toy/v.vars; echo "${DEMO_UNSET:-none}" > $TASKQ_DATA/toy/v.unset; touch $TASKQ_DATA/toy/v/DONE'
tvrun() { rm -rf "$T/toy/v"; env -u DEMO_FAST -u DEMO_FAST_LOADER DEMO_UNSET=job TASKQ_CONFIG=$T/cfg_tv TASKQ_MACHINE=$1 "${@:2}" $R command key=toy/v "$VC" env=base out=toy/v 'done=["DONE"]' > /dev/null; }
check "test machine table copy: TASKQ_MACHINE resolves to site-pueue / hub / site-slurm" "is_machine site-pueue && is_machine hub && is_machine site-slurm"
tvrun site-pueue; rc=$?
check "site-pueue (task_vars in the machine table): exported to the task" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == 1/1 ]]"
check "site-pueue (task_unset in the machine table): set in the worker env, removed from the task" "[[ \$(cat $T/toy/v.unset) == none ]]"
tvrun site-pueue DEMO_FAST=0; rc=$?
check "site-pueue: already set values kept, unset ones exported" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == 0/1 ]]"
tvrun hub; rc=$?
check "hub (no task_vars): not exported" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == none/none ]]"
check "hub (no task_unset): passed to the task as usual" "[[ \$(cat $T/toy/v.unset) == job ]]"
tvrun site-slurm; rc=$?
check "site-slurm (no task_vars): not exported" "(( rc == 0 )) && [[ \$(cat $T/toy/v.vars) == none/none ]]"

# the task's command sees the core: TASKQ_LIB = the core's queue/, the core's bin/ first on PATH
$R command key=toy/lib 'cmd=echo "$TASKQ_LIB|${PATH%%:*}" > $TASKQ_DATA/toy/lib.txt; touch $TASKQ_DATA/toy/lib/DONE' env=base out=toy/lib 'done=["DONE"]' > /dev/null; rc=$?
check "task env: TASKQ_LIB and the core's bin/ first on PATH" "(( rc == 0 )) && [[ \$(cat $T/toy/lib.txt) == \"$(cd "$HERE" && pwd)|$(cd "$HERE/.." && pwd)/bin\" ]]"

TASK_PART=2 $R command key=toy/o2 'cmd=touch $TASKQ_DATA/toy/o2/DONE' env=base out=toy/o2 'done=["DONE"]' > /dev/null; rc=$?
check "new worker: TASK_PART=2 -> _part2.log, the first line says part 2" "(( rc == 0 )) && head -1 $T/logs/tasks/toy__o2__test-1_part2.log | grep -q 'part 2'"
(( ok )) && echo "all pass" || { echo "failed"; exit 1; }
