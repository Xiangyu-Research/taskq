#!/bin/bash
# Stall detection outside Slurm (resident pueue worker, worker_start.sh → consume.py): the log name is host-time, not WORKER_ID;
# fake training hangs: the worker stops it, releases it and trains the next; no Slurm, no GPU.
#   bash queue/test_worker_stall.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'rm -rf "${T:?}"' EXIT
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=site-pueue WORKER_ID=site-pueue-gpu1   # this machine is a pueue executor, worker on GPU 1
source "$HERE/testdata/worker_harness.sh"
reset
table << EOT
t/a base
t/b base
EOT
(TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_STALL_MAX=4 FAKE_HANG=t/a timeout 40 bash "$HERE/worker_start.sh") > "$T/w.out" 2>&1
check "pueue worker: stalled t/a is stopped and released" 'grep -q "t/a: log and output dir unchanged for .*treating it as stalled" $T/w.out && grep -q "^term t/a" $T/train.log'
check "pueue worker: trains t/b after the stall" 'grep -q "end .* t/b" $T/train.log'
check "first stall: release only, no failure" '[[ ! -e $TASKQ_DIR/failures/t__a ]] && grep -q "^STALLED" $TASKQ_DATA/logs/tasks/t__a__*_part1.log'

# unstoppable (run_task.sh exited, training still running): free the slot for other tasks, keep heartbeating, release only once it really stops; do not exit while it runs
reset
rm -rf "$TASKQ_DATA/logs/tasks"   # the part log from t/b above would make it count as half-done and claim it first
table << EOT
t/z base
t/b base
EOT
mkdir -p "$T/binz"; printf '#!/bin/bash\nexit 0\n' > "$T/binz/pkill"; chmod +x "$T/binz/pkill"   # makes the session-wide force kill a no-op
(TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_STALL_MAX=4 TASKQ_GIVEUP=3 TASKQ_BEAT=1 \
  FAKE_ESCAPE=t/z FAKE_ESCAPE_SECS=25 PATH=$T/binz:$PATH exec bash "$HERE/worker_start.sh") > "$T/wz.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^end .* t/b" $T/train.log'
sleep 3
beat=$(python3 -c 'import json, sys, time; print(int(time.time() - json.load(open(sys.argv[1]))["beat"]))' "$TASKQ_DIR/claims/t__z/info" 2> /dev/null)
check "unstoppable: frees the slot to finish t/b, worker does not exit" 'kill -0 $wpid && grep -q "t/z has not stopped for 3 seconds" $T/wz.out && grep -q "unstoppable training" $T/wz.out'
check "unstoppable: keeps heartbeating, no release (claim heartbeat $beat seconds ago)" '[[ -n $beat ]] && (( beat <= 3 ))'
waitfor 40 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "unstoppable task stops: release, no failure, then the worker exits" '[[ $rc == 0 ]] && grep -q "unstoppable t/z finally stopped; releasing" $T/wz.out && [[ ! -e $TASKQ_DIR/claims/t__z && ! -e $TASKQ_DIR/failures/t__z ]]'
[[ -s $FAKE_LOG.escape ]] && kill "$(cat "$FAKE_LOG.escape")" 2> /dev/null

# second stall within a week: record one failure
reset
table << EOT
t/z base
EOT
(TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_STALL_MAX=4 FAKE_HANG=t/z timeout 40 bash "$HERE/worker_start.sh") > "$T/wz2.out" 2>&1
check "second stall: record one failure" 'grep -q "stalled again this week (2 times); recording a failure" $T/wz2.out && grep -q "stalled (2 times this week)" $TASKQ_DIR/failures/t__z'

# heartbeat finds the claim is no longer ours: stop the training, no release, no failure
reset
table << EOT
t/l base
EOT
(TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_BEAT=1 FAKE_SECS=30 exec bash "$HERE/worker_start.sh") > "$T/wl.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^start .* t/l" $T/train.log'
python3 -c 'import json, sys; p = sys.argv[1]; c = json.load(open(p)); c["worker"] = "other"; json.dump(c, open(p, "w"))' "$TASKQ_DIR/claims/t__l/info"
waitfor 40 '! kill -0 $wpid 2> /dev/null'; wait $wpid
check "claim taken over: stop the training, no release, no failure" 'grep -q "t/l: claim is no longer" $T/wl.out && grep -q "^term t/l" $T/train.log && grep -q "\"other\"" $TASKQ_DIR/claims/t__l/info && [[ ! -e $TASKQ_DIR/failures/t__l ]]'

(( fails == 0 )) && echo "all passed" || { echo "$fails checks failed"; exit 1; }
