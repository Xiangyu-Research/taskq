#!/bin/bash
# Simulated tests for the worker in a Slurm job (worker_start.sh → consume.py, like test_worker.sh): stop signal, stopping one task, CPU worker, failures shortly after start
#   bash queue/test_worker_stop.sh        WORKER_TEST_ONLY="3 4b" runs only these cases
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'rm -rf "${T:?}"' EXIT
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=site-slurm   # this machine is a Slurm executor (the worker runs inside the job)
source "$HERE/testdata/worker_harness.sh"

# 2e. two tasks failing shortly after start: the job claims no more; failures after running a while, or two further apart than the window, are not a job problem and it keeps claiming
if want 2e; then
reset
table << EOF
t/a base
t/b base
t/c base
EOF
export FAKE_FAIL="t/a t/b"
SLURM_JOB_ID=900201 TASKQ_SLOTS=1 timeout 60 bash "$HERE/worker_start.sh" > "$T/w2e1.out" 2>&1
check "two tasks fail shortly after start: the job claims no third" '[[ $(grep -c "^fail" $T/train.log) == 2 ]] && ! grep -q "start .* t/c" $T/train.log && grep -q "failed shortly after starting" $T/w2e1.out'
reset
SLURM_JOB_ID=900202 TASKQ_SLOTS=1 TASKQ_EARLY_FAIL=0 timeout 60 bash "$HERE/worker_start.sh" > "$T/w2e2.out" 2>&1
check "failures after running a while: recorded on the task, job keeps claiming" '[[ $(grep -c "^fail" $T/train.log) == 2 ]] && grep -q "end .* t/c" $T/train.log && [[ -f $TASKQ_DIR/failures/t__a && -f $TASKQ_DIR/failures/t__b ]]'
reset
SLURM_JOB_ID=900203 TASKQ_SLOTS=1 TASKQ_FAIL_WINDOW=0 timeout 60 bash "$HERE/worker_start.sh" > "$T/w2e3.out" 2>&1
check "two failures shortly after start but outside the window: job keeps claiming" '[[ $(grep -c "^fail" $T/train.log) == 2 ]] && grep -q "end .* t/c" $T/train.log && ! grep -q "failed shortly after starting" $T/w2e3.out'
unset FAKE_FAIL
fi

# 3a. fake training itself: the process tree is stopped child first (sleep), then it, without writing the done file (so 3b does not see t/a done on CI)
if want 3a; then
reset
FAKE_SECS=60 bash "$T/fake_train.sh" t/z & fp=$!
waitfor 30 'pgrep -P $fp sleep > /dev/null'
kill -TERM $(pgrep -P $fp sleep); sleep 0.5; kill -TERM $fp 2> /dev/null; wait $fp; rc=$?
check "fake training: child stopped first → exit 143, not done" '[[ $rc == 143 && ! -e $TASKQ_DATA/runs/t/z/DONE ]] && grep -q "^term t/z" $T/train.log'
fi

# 3. stop signal: stop training and release the claim (the next job can claim it)
if want 3; then
reset
table << EOF
t/a base
EOF
FAKE_SECS=60 SLURM_JOB_ID=900204 bash "$HERE/worker_start.sh" > "$T/w3.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q ^start $T/train.log 2> /dev/null'
kill -TERM $wpid; wait $wpid; rc=$?
check "stop signal: exit 143, training stopped, claim released" '[[ $rc == 143 ]] && grep -q ^term $T/train.log && [[ -z $(ls $TASKQ_DIR/claims) ]]'
fi

# 3b. stop one task (taskq.py stop): stops within 30 seconds, releases, no failure, never claimed again; another keeps training; one ignoring the signal is forced
if want 3b; then
reset
table << EOF
t/a base
t/b base
t/c base
EOF
FAKE_SECS=40 FAKE_IGNORE=t/b SLURM_JOB_ID=900205 TASKQ_IDLE_MAX=60 TASKQ_STOP_WAIT=3 TASKQ_POLL=30 TASKQ_STOP_POLL=1 bash "$HERE/worker_start.sh" > "$T/w3b.out" 2>&1 &
wpid=$!
waitfor 30 '[[ $(grep -c ^start $T/train.log 2> /dev/null) == 2 ]]'
t0=$(date +%s)
python3 "$HERE/taskq.py" stop --why test t/a > "$T/stop.out"
waitfor 40 '[[ ! -e $TASKQ_DIR/claims/t__a ]]'
dt=$(( $(date +%s) - t0 ))
check "stop: stopped and released within $dt seconds (≤ 30), no failure" '(( dt <= 30 )) && grep -q "^term t/a" $T/train.log && [[ ! -e $TASKQ_DIR/claims/t__a && ! -e $TASKQ_DIR/failures/t__a ]] && grep -q "stops and releases within 30 seconds" $T/stop.out'
python3 "$HERE/taskq.py" status > "$T/st.out"
check "status shows stopped" 'grep -q "t/a .*stopped stopped (" $T/st.out'
t0=$(date +%s)
python3 "$HERE/taskq.py" stop t/b > /dev/null
waitfor 40 '[[ ! -e $TASKQ_DIR/claims/t__b ]]'
dt=$(( $(date +%s) - t0 ))
check "ignores the stop signal: forced and released within $dt seconds (≤ 30)" '(( dt <= 30 )) && grep -q "t/b did not stop within 3 seconds; forcing" $T/w3b.out && [[ ! -e $TASKQ_DIR/claims/t__b ]]'
waitfor 20 'grep -q "start .* t/c" $T/train.log'
check "stopped task is not claimed again; the free slot takes the next" '[[ $(grep -c "start .* t/a" $T/train.log) == 1 ]] && grep -q "start .* t/c" $T/train.log'
python3 "$HERE/taskq.py" resume t/a > /dev/null
# after undo with a free slot the worker may claim t/a at once: only check it is no longer stopped (free or claimed)
waitfor 10 'python3 "$HERE/taskq.py" status | grep -q "t/a " && ! python3 "$HERE/taskq.py" status | grep -q "t/a .*stopped"'
check "after undoing the stop it queues as usual" '[[ ! -e $TASKQ_DIR/stopped/t__a ]] && python3 "$HERE/taskq.py" status | grep "t/a " | grep -qv stopped'
kill -TERM $wpid; wait $wpid 2> /dev/null
fi

# 3c. job gets a stop signal while training ignores SIGTERM: force it, release only once stopped; no training processes remain when the job exits
if want 3c; then
reset
table << EOF
t/d base
EOF
FAKE_SECS=60 FAKE_IGNORE=t/d SLURM_JOB_ID=900206 TASKQ_IDLE_MAX=60 RUN_TASK_STOP_WAIT=2 bash "$HERE/worker_start.sh" > "$T/w3c.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^start .* t/d" $T/train.log 2> /dev/null'
kill -TERM $wpid; wait $wpid; rc=$?
waitfor 5 '! pgrep -f "$T/fake_train.sh t/d" > /dev/null'  # after a force kill it takes a moment to leave the process table
check "stop signal, training ignores it: forced and released, fake training gone when the job exits" '[[ $rc == 143 ]] && [[ -z $(ls $TASKQ_DIR/claims) ]] && ! pgrep -f "$T/fake_train.sh t/d" > /dev/null'
fi

# 3d. stopping one task stops the whole group: training that writes its done file as soon as the child ends is not counted done because the child stopped first (without relying on 3a's fake training handling)
if want 3d; then
reset
table << EOF
t/e base
EOF
FAKE_EAGER=t/e SLURM_JOB_ID=900213 TASKQ_SLOTS=1 TASKQ_IDLE_MAX=60 TASKQ_POLL=30 TASKQ_STOP_POLL=1 bash "$HERE/worker_start.sh" > "$T/w3d.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^start .* t/e" $T/train.log 2> /dev/null && [[ -e $T/chain.ready ]]'
python3 "$HERE/taskq.py" stop t/e > /dev/null
waitfor 40 '[[ ! -e $TASKQ_DIR/claims/t__e ]]'
check "stop: training and child get the stop together, no done file, released, no failure" '[[ ! -e $TASKQ_DATA/runs/t/e/DONE && ! -e $TASKQ_DIR/claims/t__e && ! -e $TASKQ_DIR/failures/t__e ]] && grep -q "^term t/e" $T/train.log && ! grep -q "^end .* t/e" $T/train.log'
kill -TERM $wpid; wait $wpid 2> /dev/null
fi

# 3e. worker killed with SIGKILL: no temp files left in TMPDIR; taskq.py next stderr is read into memory, not a file;
#     files from earlier workers untouched for 7 days are deleted; no background processes outlive the worker
if want 3e; then
reset
: > "$TASKQ_DIR/tasks.jsonl"
L=$TASKQ_DATA/logs/workers
mkdir -p "$L"
touch -d "10 days ago" "$L/worker_1_next.err"
touch "$L/worker_2_next.err"
gone=1
for n in 1 2; do
    SLURM_JOB_ID=900214 TASKQ_IDLE_MAX=600 bash "$HERE/worker_start.sh" > "$T/w3e$n.out" 2>&1 &
    wpid=$!
    waitfor 30 'grep -q "no claimable tasks" $T/w3e$n.out'
    kids=$(pgrep -P $wpid | tr '\n' ' ')
    kill -KILL $wpid; wait $wpid 2> /dev/null
    [[ -n $kids ]] && { waitfor 15 '! kill -0 $kids 2> /dev/null' || gone=0; kill -KILL $kids 2> /dev/null; }
done
check "SIGKILL: no temp files left in TMPDIR, no stderr files either" '[[ -z $(ls -A "$TMPDIR") && ! -e $L/worker_900214_next.err ]]'
check "from other workers: untouched for 7 days deleted, newer kept" '[[ ! -e $L/worker_1_next.err && -e $L/worker_2_next.err ]]'
check "SIGKILL: the worker's background processes exit too" '[[ $gone == 1 ]]'
fi

# 4b. CPU worker (WORKER_DEVICE=cpu): claims only device=cpu, one at a time, no GPU records; a GPU worker does not claim device=cpu
if want 4b; then
reset
table << EOF
t/g base
t/c1 base cpu
t/c2 base cpu
EOF
SLURM_JOB_ID=900207 WORKER_DEVICE=cpu TASKQ_SLOTS= timeout 60 bash "$HERE/worker_start.sh" > "$T/w4b.out" 2>&1
check "CPU worker: two CPU-only tasks finish one after the other, GPU tasks untouched" '[[ $(grep -c "^end .* t/c" $T/train.log) == 2 ]] && ! grep -q "t/g" $T/train.log'
check "CPU worker: one at a time, no GPU records" '[[ $(head -2 $T/train.log | grep -c ^start) == 1 && ! -e $TASKQ_DATA/logs/workers/worker_900207_gpu.csv ]] && grep -q "worker (cpu).*at most 1 at once" $T/w4b.out'
check "CPU worker: ends when nothing is claimable, does not idle" 'grep -q "nothing running and no claimable tasks; ending the job" $T/w4b.out && ! grep -q "waiting, checking every" $T/w4b.out'
SLURM_JOB_ID=900208 timeout 60 bash "$HERE/worker_start.sh" > "$T/w4c.out" 2>&1
check "GPU worker: trains only tasks that need a GPU" 'grep -q "^end .* t/g" $T/train.log && [[ $(grep -c "^start .* t/c" $T/train.log) == 2 ]]'
fi

# 5 / 2r. run_task.sh exit code 5 (disk unreadable): release, no failure, this job does not reclaim at once; exit code 2: the failure reason carries the last part-log line
#   spec.py in the code copy: with file FAKE_DONE5 done exits 5, with FAKE_SHELL2 shell reports an error and exits 2
if want 5; then
mkdir -p "$T/core"; rm -rf "$T/core/queue" "$T/core/sites"; cp -r "$HERE" "$T/core/queue"; cp -r "$HERE/../sites" "$T/core/sites"
sed -i '1a import os, sys\nif __name__ == "__main__" and sys.argv[1:2] == ["done"] and os.path.exists(os.environ.get("FAKE_DONE5", "/nonexistent")): sys.exit(5)\nif __name__ == "__main__" and sys.argv[1:2] == ["shell"] and os.path.exists(os.environ.get("FAKE_SHELL2", "/nonexistent")): sys.exit("spec.py: unknown {nosuch}")' "$T/core/queue/spec.py"
reset
table << EOF
t/u base
EOF
touch "$T/unreadable"
FAKE_DONE5=$T/unreadable SLURM_JOB_ID=900209 TASKQ_SLOTS=1 timeout 60 bash "$T/core/queue/worker_start.sh" > "$T/w5.out" 2>&1
check "exit code 5: release, no failure, this job does not reclaim at once" '[[ $(grep -c "t/u: disk unreadable, cannot tell whether training finished; releasing" $T/w5.out) == 1 && -z $(ls $TASKQ_DIR/claims) && ! -e $TASKQ_DIR/failures/t__u ]] && ! grep -q t/u $T/train.log'
rm -f "$T/unreadable"
reset
touch "$T/badspec"
FAKE_SHELL2=$T/badspec SLURM_JOB_ID=900210 TASKQ_SLOTS=1 timeout 60 bash "$T/core/queue/worker_start.sh" > "$T/w2r.out" 2>&1
check "exit code 2: the failure reason carries the part-log reason" 'grep -q "exit code 2: not started: bad task spec: spec.py: unknown {nosuch}" $TASKQ_DIR/failures/t__u'
rm -f "$T/badspec"
fi

# 6. only run_task.sh is stopped from outside: SIGTERM → training stops, release, no failure; SIGKILL → keep holding the claim while training runs, stop it, release only once it stops, no failure
if want 6; then
reset
table << EOF
t/a base
EOF
FAKE_SECS=60 SLURM_JOB_ID=900211 TASKQ_SLOTS=1 timeout 60 bash "$HERE/worker_start.sh" > "$T/w6a.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q ^start $T/train.log 2> /dev/null'
kill -TERM "$(pgrep -P "$(pgrep -P $wpid)" -f run_task.sh)"
wait $wpid
check "run_task.sh alone gets SIGTERM: training stops, release, no failure, this job does not claim it again" 'grep -q "^term t/a" $T/train.log && [[ -z $(ls $TASKQ_DIR/claims) && ! -e $TASKQ_DIR/failures/t__a ]] && grep -q "stopped from outside" $T/w6a.out && [[ $(grep -c ^start $T/train.log) == 1 ]]'
reset
FAKE_IGNORE=t/a SLURM_JOB_ID=900212 TASKQ_SLOTS=1 TASKQ_IDLE_MAX=60 timeout 60 bash "$HERE/worker_start.sh" > "$T/w6b.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q ^start $T/train.log 2> /dev/null'
sleep 3
kill -KILL "$(pgrep -P "$(pgrep -P $wpid)" -f run_task.sh)"
waitfor 20 'grep -q "training is still running" $T/w6b.out'
sleep 2
held=$([[ -e $TASKQ_DIR/claims/t__a ]] && grep -q "^term t/a" $T/train.log && echo 1)
pkill -KILL -f "$T/fake_train.sh t/a"
waitfor 20 '[[ -z $(ls $TASKQ_DIR/claims) ]]'
kill -TERM "$(pgrep -P $wpid)"
wait $wpid
check "run_task.sh killed with SIGKILL while training runs: stop it, keep the claim; release once training stops, no failure" '[[ $held == 1 && -z $(ls $TASKQ_DIR/claims) && ! -e $TASKQ_DIR/failures/t__a ]] && grep -q "finally stopped; releasing" $T/w6b.out'
fi

(( fails == 0 )) && echo "all passed" || { echo "$fails checks failed"; exit 1; }
