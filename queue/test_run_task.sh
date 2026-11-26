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
SPV=$(python3 -c 'import json, sys
s = json.loads(sys.argv[1]); s.update(key="toy/v1", out="toy/v1", vars={"A; touch " + sys.argv[2] + "; B": "1"}); print(json.dumps(s))' "$SP" "$T/pwned")
$R --spec "$SPV" > /dev/null 2>&1; rc=$?
check "a vars name that is not a shell variable name: exit 2, none of it runs" "(( rc == 2 )) && [[ ! -e $T/pwned ]] && grep -q 'not started: bad task spec: .*vars name' $T/logs/tasks/toy__v1__test-1_part1.log"
$R command key=toy/v2 'cmd=printf %s "$V" > $TASKQ_DATA/toy/v2.txt; touch $TASKQ_DATA/toy/v2/DONE' env=base out=toy/v2 'done=["DONE"]' \
  "vars={\"V\": \"\$(touch $T/pwned) \`touch $T/pwned\` ; it's\"}" > /dev/null; rc=$?
check "vars values reach the command literally (run_task.sh evaluates quoted shell lines)" "(( rc == 0 )) && [[ ! -e $T/pwned && \$(cat $T/toy/v2.txt) == \"\\\$(touch $T/pwned) \\\`touch $T/pwned\\\` ; it's\" ]]"
# --stdin (how a worker starts it): the arguments as a JSON list on stdin, handed on to spec.py on stdin; no command line holds the
#   spec. python3 on PATH is a spy that records the command line of every python3 run_task.sh starts
mkdir -p "$T/spy"
printf '#!/bin/bash\nprintf "%%s\\n" "$*" >> %q\nexec %q "$@"\n' "$T/spy.log" "$(command -v python3)" > "$T/spy/python3"; chmod +x "$T/spy/python3"
SPS=$(python3 "$HERE/spec.py" resolve command key=toy/st 'cmd=printf %s "$TOKEN" > {data}/toy/st.txt; touch {data}/toy/st/DONE' env=base out=toy/st \
      'done=["DONE"]' 'vars={"TOKEN": "s3cr3t-st"}')
python3 -c 'import json, sys; print(json.dumps(["--spec", sys.argv[1]]))' "$SPS" > "$T/st.json"
: > "$T/spy.log"; PATH=$T/spy:$PATH $R --stdin < "$T/st.json" > /dev/null; rc=$?
check "--stdin: runs the spec from stdin, the command gets its vars" "(( rc == 0 )) && [[ \$(cat $T/toy/st.txt) == s3cr3t-st && -e $T/toy/st/DONE ]]"
check "--stdin: spec.py shell and done get the spec on stdin, no python3 command line holds it" \
  "grep -q 'spec.py shell --stdin' $T/spy.log && grep -q 'spec.py done --stdin' $T/spy.log && ! grep -q s3cr3t $T/spy.log"
python3 -c 'import json; print(json.dumps(["command", "key=toy/st2", "cmd=printf %s \"$TOKEN\" > $TASKQ_DATA/toy/st2.txt; touch $TASKQ_DATA/toy/st2/DONE",
                                           "env=base", "out=toy/st2", "done=[\"DONE\"]", "vars={\"TOKEN\": \"s3cr3t-st2\"}"]))' > "$T/st2.json"
: > "$T/spy.log"; PATH=$T/spy:$PATH $R --stdin < "$T/st2.json" > /dev/null; rc=$?
check "--stdin with <recipe> k=v... arguments: runs, nothing on a python3 command line" "(( rc == 0 )) && [[ \$(cat $T/toy/st2.txt) == s3cr3t-st2 ]] && ! grep -q s3cr3t $T/spy.log"
SPS3=$(python3 -c 'import json, sys; s = json.loads(sys.argv[1]); s.update(key="toy/st3", out="toy/st3", cmd="touch {data}/toy/st3/DONE"); print(json.dumps(s))' "$SPS")
: > "$T/spy.log"; PATH=$T/spy:$PATH $R --spec "$SPS3" > /dev/null; rc=$?
check "--spec <JSON> by hand: spec.py still gets it on stdin" "(( rc == 0 )) && [[ -e $T/toy/st3/DONE ]] && grep -q 'spec.py shell --stdin' $T/spy.log && ! grep -q s3cr3t $T/spy.log"
echo '["--spec", "{\"key\": \"st4\"}"]' | $R --stdin > /dev/null 2>&1; rc=$?
check "--stdin with a bad spec: exit 2, the reason in the part log of the key it names" "(( rc == 2 )) && grep -q 'not started: bad task spec: .' $T/logs/tasks/st4__test-1_part1.log"
$R --stdin extra < "$T/st.json" > /dev/null 2>&1; rc=$?
check "--stdin with more arguments: usage, exit 2" "(( rc == 2 ))"
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
# training never outlives its task. gone_in: within $1 seconds none of the pids is alive (zombies count as gone)
gone_in() {
    local n p a
    for (( n = 0; n < $1 * 10; n++ )); do
        a=0; for p in "${@:2}"; do [[ $(ps -o stat= -p "$p" 2> /dev/null) == [^Z]* ]] && a=1; done
        (( a )) || return 0
        sleep 0.1
    done
    return 1
}
printf '#!/bin/bash\n"$@" > /dev/null &\nwait\n' > "$T/boss.sh"  # a stand-in worker: starts run_task.sh and waits
RUN_TASK_BOSS_POLL=1 bash "$T/boss.sh" $R command key=toy/k1 'cmd=sleep 317' env=base out=toy/k1 'done=["DONE"]' & boss=$!
kids=$(started $boss "sleep 317" | tr '\n' ' '); rt=$(pgrep -P $boss)
kill -KILL $boss; wait $boss 2> /dev/null
check "the worker is killed (SIGKILL): run_task.sh notices it has another parent and stops the training" "[[ -n '$kids' && -n '$rt' ]] && gone_in 20 $rt $kids && grep -q 'the worker that started this task is gone' $T/logs/tasks/toy__k1__test-1_part1.log"
RUN_TASK_BOSS_POLL=1 $R command key=toy/k2 'cmd=sleep 318' env=base out=toy/k2 'done=["DONE"]' > /dev/null & rp=$!
kids=$(started $rp "sleep 318" | tr '\n' ' ')
kill -KILL $rp; wait $rp 2> /dev/null
check "run_task.sh itself is killed (SIGKILL): its watcher stops the training" "[[ -n '$kids' ]] && gone_in 20 $kids && grep -q 'run_task.sh is gone' $T/logs/tasks/toy__k2__test-1_part1.log"
$R command key=toy/l 'cmd=sleep 319 & echo $! > $TASKQ_DATA/toy/l/pid; touch $TASKQ_DATA/toy/l/DONE' env=base out=toy/l 'done=["DONE"]' > /dev/null; rc=$?
lp=$(cat "$T/toy/l/pid" 2> /dev/null)
check "the command exits leaving a background process: it is stopped, done as usual" "(( rc == 0 )) && [[ -n '$lp' ]] && gone_in 5 $lp && grep -q 'left processes in its session' $T/logs/tasks/toy__l__test-1_part1.log"
# GPU wait (res.wait_gpu): fake nvidia-smi reports the processes in $T/gpu/apps for the first $T/gpu/busy checks, then none; OTHER is a process of another user
OTHER=$(ps -eo pid=,user= | awk -v me="$(id -un)" '$2 != me {print $1; exit}')
mkdir -p "$T/bin" "$T/gpu"
cat > "$T/bin/nvidia-smi" << 'FAKE'
#!/bin/bash
G=$TASKQ_DATA/gpu
case "$*" in
    *query-compute-apps*) n=$(cat "$G/n" 2> /dev/null || echo 0); echo $(( n + 1 )) > "$G/n"
                          (( n < $(cat "$G/busy") )) && cat "$G/apps" ;;
    *query-gpu=index,memory.used*) echo "0, $(cat "$G/used")" ;;
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
gpu 99 "99999999, 1000" 1000
$G SLURM_JOB_ID=1 $R command key=toy/w5 'cmd=touch $TASKQ_DATA/toy/w5/DONE' env=base out=toy/w5 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null; rc=$?
check "no GPU check in Slurm" "(( rc == 0 )) && [[ ! -e $T/gpu/n ]]"
# TERM while checking the GPU (gpu_free): its local pid used to shadow the training pid, so the process listed on the GPU was
#   stopped instead; before training starts nothing is stopped
mkdir -p "$T/binh"
printf '#!/bin/bash\ncase "$*" in *query-compute-apps*) cat %q; echo $$ > %q; exec sleep 30 ;; *query-gpu=index,memory.used*) echo \"0, 10\" ;; esac\nexit 0\n' "$T/gpu/hang_apps" "$T/gpu/hang_pid" > "$T/binh/nvidia-smi"; chmod +x "$T/binh/nvidia-smi"
sleep 313 & victim=$!
echo "$victim, 10" > "$T/gpu/hang_apps"; rm -f "$T/gpu/hang_pid"
env PATH=$T/binh:$PATH CUDA_VISIBLE_DEVICES=0 $R command key=toy/w6 'cmd=touch $TASKQ_DATA/toy/w6/DONE' env=base out=toy/w6 'done=["DONE"]' > /dev/null & rp=$!
for _ in $(seq 100); do [[ -s $T/gpu/hang_pid ]] && break; sleep 0.1; done; sleep 0.5
kill -TERM $rp; wait $rp; rc=$?
alive=0; kill -0 $victim 2> /dev/null && alive=1
kill $victim $(cat "$T/gpu/hang_pid" 2> /dev/null) 2> /dev/null; wait $victim 2> /dev/null
check "TERM while checking the GPU: exit code 143, command not run, the process listed on the GPU not stopped" "(( rc == 143 && alive == 1 )) && [[ -s $T/gpu/hang_pid && ! -e $T/toy/w6/DONE ]]"
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
# every GPU in CUDA_VISIBLE_DEVICES (by index or UUID) must be free, and a GPU nvidia-smi cannot tell about is not free. The fake
#   answers -i <gpu> from $T/gpus/<gpu>/{index,used,apps}; no dir: exit 6 (no such GPU); a fail file: exit 9
mkdir -p "$T/binm"
cat > "$T/binm/nvidia-smi" << 'FAKE'
#!/bin/bash
d=$TASKQ_DATA/gpus/$2
[[ $1 == -i && -d $d ]] || exit 6
[[ -e $d/fail ]] && exit 9
case "$*" in
    *query-compute-apps*) cat "$d/apps" ;;
    *query-gpu=index,memory.used*) echo "$(cat "$d/index"), $(cat "$d/used")" ;;
esac
exit 0
FAKE
chmod +x "$T/binm/nvidia-smi"
mgpu() { rm -rf "$T/gpus/$1"; mkdir -p "$T/gpus/$1"; echo "$2" > "$T/gpus/$1/index"; echo "$3" > "$T/gpus/$1/used"; : > "$T/gpus/$1/apps"; }
M="env PATH=$T/binm:$PATH RUN_TASK_GPU_POLL=1"
mrun() { local k=$1; shift; $M "$@" $R command key=toy/$k "cmd=touch \$TASKQ_DATA/toy/$k/DONE" env=base out=toy/$k 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null 2>&1; }
mlog() { cat "$T/logs/tasks/toy__$1__test-1_part1.log" 2> /dev/null; }
mgpu 0 0 0; mgpu 1 1 2000
mrun m1 CUDA_VISIBLE_DEVICES=0,1; rc=$?
check "CUDA_VISIBLE_DEVICES=0,1, someone else's memory on GPU 1: busy, not run" "(( rc == 1 )) && [[ ! -e $T/toy/m1/DONE ]] && mlog m1 | grep -q 'GPU 0,1 busy, waited 0 minutes, not running'"
mgpu 1 1 300
mrun m2 CUDA_VISIBLE_DEVICES=0,1; rc=$?
check "CUDA_VISIBLE_DEVICES=0,1, both free: run" "(( rc == 0 ))"
touch "$T/gpus/1/fail"
mrun m3 CUDA_VISIBLE_DEVICES=0,1; rc=$?
check "nvidia-smi fails for one GPU: not free (cannot tell), not run, the log says why" "(( rc == 1 )) && [[ ! -e $T/toy/m3/DONE ]] && mlog m3 | grep -q 'GPU 0,1 busy (cannot tell: nvidia-smi failed for GPU 1 (exit 9)), waited 0 minutes, not running'"
mgpu 1 1 '[N/A]'
mrun m4 CUDA_VISIBLE_DEVICES=0,1; rc=$?
check "unreadable nvidia-smi output: not free, not run" "(( rc == 1 )) && [[ ! -e $T/toy/m4/DONE ]] && mlog m4 | grep -q \"cannot tell: nvidia-smi gave '1, \\[N/A\\]' for GPU 1\""
mrun m5 CUDA_VISIBLE_DEVICES=7; rc=$?
check "a GPU nvidia-smi does not know: not free, not run" "(( rc == 1 )) && [[ ! -e $T/toy/m5/DONE ]] && mlog m5 | grep -q 'nvidia-smi failed for GPU 7 (exit 6)'"
# a PATH of links to everything on PATH but nvidia-smi: dropping its dir would also drop bash when they share one (/usr/bin)
NOSMI=$T/nosmi-bin
mkdir "$NOSMI"
IFS=: read -ra _dirs <<< "$PATH"
for _d in "${_dirs[@]}"; do [[ -d $_d ]] && ln -s "$_d"/* "$NOSMI"/ 2> /dev/null; done
rm -f "$NOSMI/nvidia-smi"
env PATH="$NOSMI" CUDA_VISIBLE_DEVICES=0 $R command key=toy/m6 'cmd=touch $TASKQ_DATA/toy/m6/DONE' env=base out=toy/m6 'done=["DONE"]' 'res={"wait_gpu": 0}' > /dev/null 2>&1; rc=$?
check "no nvidia-smi at all: not free, not run" "(( rc == 1 )) && [[ ! -e $T/toy/m6/DONE ]] && mlog m6 | grep -q 'nvidia-smi failed for GPU 0 (exit 127)'"
mgpu 1 1 0; mgpu GPU-abc 3 0
glrow 10 3 someoneelse
mrun m7 CUDA_VISIBLE_DEVICES=GPU-abc GPU_LOG_DIR=$T/gl TASK_GPU_IDLE=60; rc=$?
check "CUDA_VISIBLE_DEVICES by UUID: the usage records of that GPU's index count" "(( rc == 1 )) && [[ ! -e $T/toy/m7/DONE ]] && mlog m7 | grep -q 'needs 60 minutes free'"
glrow 10 1 someoneelse
mrun m8 CUDA_VISIBLE_DEVICES=GPU-abc GPU_LOG_DIR=$T/gl TASK_GPU_IDLE=60; rc=$?
check "CUDA_VISIBLE_DEVICES by UUID: someone on another GPU, run" "(( rc == 0 ))"
mrun m9 CUDA_VISIBLE_DEVICES=0,1 GPU_LOG_DIR=$T/gl TASK_GPU_IDLE=60; rc=$?
check "CUDA_VISIBLE_DEVICES=0,1: someone on GPU 1 in the records, not run" "(( rc == 1 )) && [[ ! -e $T/toy/m9/DONE ]] && mlog m9 | grep -q 'needs 60 minutes free'"

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

# the task's command sees the core: TASKQ_LIB = the core's queue/, the core's bin/ first on PATH; TASKQ_PROGRESS = its progress file
$R command key=toy/lib 'cmd=echo "$TASKQ_LIB|${PATH%%:*}" > $TASKQ_DATA/toy/lib.txt; echo "$TASKQ_PROGRESS" > $TASKQ_DATA/toy/lib.pf; touch $TASKQ_DATA/toy/lib/DONE' env=base out=toy/lib 'done=["DONE"]' > /dev/null; rc=$?
check "task env: TASKQ_LIB and the core's bin/ first on PATH" "(( rc == 0 )) && [[ \$(cat $T/toy/lib.txt) == \"$(cd "$HERE" && pwd)|$(cd "$HERE/.." && pwd)/bin\" ]]"
check "task env: TASKQ_PROGRESS = PROGRESS.json in its out dir" "[[ \$(cat $T/toy/lib.pf) == $T/toy/lib/PROGRESS.json ]]"

TASK_PART=2 $R command key=toy/o2 'cmd=touch $TASKQ_DATA/toy/o2/DONE' env=base out=toy/o2 'done=["DONE"]' > /dev/null; rc=$?
check "new worker: TASK_PART=2 -> _part2.log, the first line says part 2" "(( rc == 0 )) && head -1 $T/logs/tasks/toy__o2__test-1_part2.log | grep -q 'part 2'"
(( ok )) && echo "all pass" || { echo "failed"; exit 1; }
