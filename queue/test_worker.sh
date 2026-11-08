#!/bin/bash
# Simulated tests for the worker in a Slurm job (worker.sbatch → worker_start.sh → consume.py): temporary fake queue (TASKQ_DIR), temp data dir, fake job id,
# tasks run fake training through the command recipe (sleep a few seconds, then write the done file); this machine is site-slurm from the test machine table (testdata/config),
# and envs are activated as it says: fake conda, venv (base, alt). No Slurm, no GPU.
# These are the basic claim, train and release cases; stop and failure cases are in test_worker_stop.sh, resident and bundle cases in test_worker_code.sh.
#   bash queue/test_worker.sh        WORKER_TEST_ONLY="1 4" runs only these cases
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'rm -rf "${T:?}"' EXIT
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=site-slurm   # this machine is a Slurm executor (the worker runs inside the job)
source "$HERE/testdata/worker_harness.sh"

# 1. how a Slurm job starts (worker.sbatch <worker_start.sh under code/current>): claim in order; 2 per GPU when it is not busy and memory is free; env per task
if want 1; then
reset
table << EOF
t/a base
t/b alt
t/c base
t/d base
EOF
FAKE_SECS=3 SLURM_JOB_ID=900101 timeout 90 bash "$HERE/worker.sbatch" "$HERE/worker_start.sh" > "$T/w1.out" 2>&1
check "Slurm job: all four finish" '[[ $(grep -c ^end $T/train.log) == 4 ]]'
check "Slurm job: the first two start together while the GPU is not busy" '[[ $(head -2 $T/train.log | grep -c ^start) == 2 ]]'
check "env per task: alt uses the venv (on conda base0), base uses the conda env (not inside the venv)" \
      'grep -q "^start .* t/b alt base0 " $T/train.log && grep -q "^start .* t/c " $T/train.log && ! grep -q "^start .* t/c alt " $T/train.log'
check "CPUs per training passed to the task (TASK_CPUS)" 'grep -q "cpus=[0-9]" $T/train.log'
check "all claims released" '[[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'
check "part logs written to logs/tasks/ by task name and job id" '[[ -f $TASKQ_DATA/logs/tasks/t__a__900101_part1.log ]] && head -1 $TASKQ_DATA/logs/tasks/t__a__900101_part1.log | grep -q "^job 900101_1 @ .*task=t/a"'
fi

# 1b. envs missing on this machine (x2 exists only on the hub) are not claimed
if want 1b; then
reset
table << EOF
t/x x2
t/y base
EOF
SLURM_JOB_ID=900102 timeout 60 bash "$HERE/worker_start.sh" > "$T/w1b.out" 2>&1
check "an env missing here is not claimed, others are" '[[ $(grep -c ^end $T/train.log) == 1 ]] && grep -q "end .* t/y" $T/train.log && grep -q "no claimable tasks" $T/w1b.out'
fi

# 1c. nothing claimable when the Slurm job starts: wait; claim when a task arrives; end after IDLE_MAX idle when none does
if want 1c; then
reset
: > "$TASKQ_DIR/tasks.jsonl"
SLURM_JOB_ID=900103 TASKQ_IDLE_MAX=8 timeout 60 bash "$HERE/worker_start.sh" > "$T/w1c.out" 2>&1 &
wpid=$!
waitfor 20 'grep -q "no claimable tasks; waiting, checking every" $T/w1c.out'
sleep 1
check "nothing claimable at start: does not end at once" 'kill -0 $wpid 2> /dev/null && ! grep -q "ending the job" $T/w1c.out'
table << EOF
t/a base
EOF
waitfor 30 'grep -q "^end .* t/a" $T/train.log'
check "task arrives while waiting: claimed and finished" 'grep -q "^end .* t/a" $T/train.log && grep -q "started t/a" $T/w1c.out'
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "nothing claimable after finishing: ends after IDLE_MAX idle (exit 0)" '[[ $rc == 0 ]] && grep -q "waited 0 minutes with no claimable tasks; ending the job and giving the GPU back" $T/w1c.out && [[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'
fi

if want 1d; then
reset
: > "$TASKQ_DIR/tasks.jsonl"
t0=$(date +%s)
SLURM_JOB_ID=900104 timeout 60 bash "$HERE/worker_start.sh" > "$T/w1d.out" 2>&1; rc=$?
dt=$(( $(date +%s) - t0 ))
check "never anything claimable: ends after IDLE_MAX ($TASKQ_IDLE_MAX seconds) idle, took $dt seconds" '[[ $rc == 0 ]] && (( dt >= TASKQ_IDLE_MAX )) && grep -q "waited 0 minutes with no claimable tasks; ending the job and giving the GPU back" $T/w1d.out && ! grep -q "nothing running and no claimable tasks" $T/w1d.out'
fi

# 2. GPU saturated (95% utilization): one training at a time
if want 2; then
reset
table << EOF
t/a base
t/b base
EOF
SLURM_JOB_ID=900105 WORKER_TEST_GPU="95 10 30000 81920" timeout 60 bash "$HERE/worker_start.sh" > "$T/w2.out" 2>&1
check "GPU saturated: the next starts only after the first finishes" '[[ $(sed -n 2p $T/train.log) == end* && $(grep -c ^end $T/train.log) == 2 ]]'
fi

# 2b. less than half the memory free (training uses 50GB): also one at a time
if want 2b; then
reset
table << EOF
t/a base
t/b base
EOF
SLURM_JOB_ID=900106 WORKER_TEST_GPU="50 10 50000 81920" timeout 60 bash "$HERE/worker_start.sh" > "$T/w2b.out" 2>&1
check "not enough memory: one at a time" '[[ $(sed -n 2p $T/train.log) == end* && $(grep -c ^end $T/train.log) == 2 ]]'
fi

# 2c. GPU pad: training starts only after it makes room (or writes ready)
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
check "training starts only after the pad makes room" '[[ -f $T/pad.released ]] && awk -v r=$(cat $T/pad.released) "/^start/ {exit !(\$2 >= r)}" $T/train.log'
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
check "the pad drops only to floor (not 0): training starts after it writes ready; the old ready does not count" '[[ -f $T/pad.released ]] && awk -v r=$(cat $T/pad.released) "/^start/ {exit !(\$2 >= r)}" $T/train.log && ! grep -q "did not respond" $T/w2c2.out'
fi

# 2d. training fails: release, record one failure, this job does not claim it again
if want 2d; then
reset
table << EOF
t/a base
t/b base
EOF
SLURM_JOB_ID=900109 FAKE_FAIL=t/a TASKQ_SLOTS=1 timeout 60 bash "$HERE/worker_start.sh" > "$T/w2d.out" 2>&1
check "training fails: one failure recorded, next task claimed" '[[ $(wc -l < $TASKQ_DIR/failures/t__a) == 1 ]] && grep -q "end .* t/b" $T/train.log && [[ $(grep -c "start .* t/a" $T/train.log) == 1 ]]'
fi

# 2d2. missing inputs (run_task.sh exit code 4): release, record missing inputs, not a failure, claim the next
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
check "missing inputs: did not start, no failure, recorded as missing inputs, next task trained" '! grep -q "start .* t/a" $T/train.log && [[ ! -e $TASKQ_DIR/failures/t__a ]] && [[ -e $TASKQ_DIR/inputs/t__a ]] && grep -q "end .* t/b" $T/train.log'
check "missing inputs: claim released" '[[ ! -e $TASKQ_DIR/claims/t__a ]]'
rm -rf "$TASKQ_DIR/inputs"
fi

# 2f. stalled task (log and output dir both unchanged): stop, release, no failure, claim the next; one that keeps writing its output dir is not stalled
if want 2f; then
reset
table << EOF
t/a base
t/b base
t/c base
EOF
SLURM_JOB_ID=900111 TASKQ_SLOTS=1 TASKQ_STALL_MAX=3 FAKE_HANG=t/a FAKE_BUSY=t/b FAKE_BUSY_SECS=7 timeout 90 bash "$HERE/worker_start.sh" > "$T/w2f.out" 2>&1
check "stalled: stopped, released, no failure" 'grep -q "^term t/a" $T/train.log && grep -q "t/a: log and output dir unchanged for .*treating it as stalled" $T/w2f.out && grep -q "stalled t/a stopped" $T/w2f.out && [[ ! -e $TASKQ_DIR/failures/t__a ]]'
check "after the stalled task stops, this job claims the next ones" 'grep -q "end .* t/b" $T/train.log && grep -q "end .* t/c" $T/train.log && [[ $(grep -c "start .* t/a" $T/train.log) == 1 ]]'
check "one that keeps writing its output dir (not the log) is not stalled" '! grep -q "t/b: log and output dir unchanged" $T/w2f.out'
check "all claims released" '[[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'
fi

# 4. an assigned claim (take --assign, does not expire; heartbeat 30 minutes ago) is not claimed by a Slurm job
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
check "assigned to site-pueue-gpu1 (does not expire after 30 minutes without a heartbeat): a Slurm job does not claim it" '[[ ! -s $T/train.log ]] && grep -q "no claimable tasks" $T/w4.out'
fi

# 5. queue errors (task list unreadable) for a while with nothing running: end the job
if want 5; then
reset
rm -f "$TASKQ_DIR/tasks.jsonl"
SLURM_JOB_ID=900113 TASKQ_IDLE_MAX=60 timeout 60 bash "$HERE/worker_start.sh" > "$T/w5.out" 2>&1; rc=$?
check "queue error: reported, job ends after a while" '[[ $rc == 0 ]] && grep -q "queue error" $T/w5.out && grep -q "queue kept failing" $T/w5.out'
fi

(( fails == 0 )) && echo "all passed" || { echo "$fails checks failed"; exit 1; }
