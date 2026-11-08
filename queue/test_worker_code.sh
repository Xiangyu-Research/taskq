#!/bin/bash
# Simulated tests for the worker (worker_start.sh → consume.py, like test_worker.sh): resident outside Slurm (pueue), code updates, bundles, gpu_stats / pad_now
#   bash queue/test_worker_code.sh        WORKER_TEST_ONLY="6 6e" runs only these cases
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CORE=$(cd "$HERE/.." && pwd)  # scheduler code root; the user's repo root REPO (TASKQ_REPO, else as in cfg.py)
REPO=${TASKQ_REPO:-$CORE}; [[ -z ${TASKQ_REPO:-} && $CORE == */scripts/taskq ]] && REPO=${CORE%/scripts/taskq}
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'rm -rf "${T:?}"' EXIT
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=site-pueue   # this machine is a pueue executor (the worker is resident in a pueue group)
source "$HERE/testdata/worker_harness.sh"
HEADCMD="cat $T/head"

# 6. outside Slurm (resident in site-pueue's pueue group, worker on GPU 1): WORKER_ID is the name, slots from the worker's slots (TASKQ_SLOTS, unset → 1), only the GPU in CUDA_VISIBLE_DEVICES;
#    idle less than IDLE_EXIT (default 30 minutes), queue errors and too many failures do not exit (RUN_TASK_TRIES=1: outside Slurm run_task.sh retries 3 times itself); exit 0 when the commit changes with nothing running, and wait for training to finish when it changes while running
if want 6; then
mkdir -p "$T/bin6"; printf '#!/bin/bash\necho "$*" >> %q\n' "$T/nvsmi.args" > "$T/bin6/nvidia-smi"; chmod +x "$T/bin6/nvidia-smi"
echo a > "$T/head"
reset
table << EOF
t/a base
t/b base
EOF
(unset TASKQ_SLOTS; WORKER_ID=site-pueue-gpu1 CUDA_VISIBLE_DEVICES=1 TASKQ_IDLE_POLL=1 TASKQ_HEAD_CMD=$HEADCMD PATH=$T/bin6:$PATH exec bash "$HERE/worker_start.sh") > "$T/w6.out" 2>&1 &
wpid=$!
waitfor 60 '[[ $(grep -c ^end $T/train.log) == 2 ]]'
check "outside Slurm: one at a time per slots (no second even when the GPU is idle), both finish one after the other" '[[ $(sed -n 2p $T/train.log) == end* && $(grep -c ^end $T/train.log) == 2 ]] && grep -q "job site-pueue-gpu1 @ .*at most 1 at once.*time limit none" $T/w6.out'
check "outside Slurm: nvidia-smi looks only at the GPU in CUDA_VISIBLE_DEVICES" '[[ -s $T/nvsmi.args ]] && ! grep -qv "^-i 1 " $T/nvsmi.args'
waitfor 20 'grep -q "no claimable tasks; checking again every 1 seconds" $T/w6.out'
sleep $(( TASKQ_IDLE_MAX + 1 ))
check "outside Slurm: does not exit after idling past IDLE_MAX" 'kill -0 $wpid 2> /dev/null && grep -q "no claimable tasks; checking again every 1 seconds" $T/w6.out && ! grep -q "ending the job" $T/w6.out'
mv "$TASKQ_DIR/tasks.jsonl" "$TASKQ_DIR/tasks.jsonl.bak"
waitfor 20 'grep -q "queue error" $T/w6.out'
sleep $(( TASKQ_ERR_MAX + 1 ))
check "outside Slurm: does not exit when the queue keeps failing past ERR_MAX" 'kill -0 $wpid 2> /dev/null && grep -q "queue error" $T/w6.out && ! grep -q "queue kept failing" $T/w6.out'
mv "$TASKQ_DIR/tasks.jsonl.bak" "$TASKQ_DIR/tasks.jsonl"
echo b > "$T/head"
waitfor 15 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "outside Slurm: exits when the commit changes while idle (exit 0)" '[[ $rc == 0 ]] && grep -q "code updated" $T/w6.out && grep -q "exiting; relay.py restarts it with the new code" $T/w6.out'
fi

if want 6d; then
reset
: > "$TASKQ_DIR/tasks.jsonl"
echo b > "$T/head"
(WORKER_ID=site-pueue-gpu1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_HEAD_CMD=$HEADCMD exec bash "$HERE/worker_start.sh") > "$T/w6d.out" 2>&1 &
wpid=$!
waitfor 20 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "outside Slurm: exits after idling past IDLE_EXIT (exit 0)" '[[ $rc == 0 ]] && grep -q "exiting (the next check starts it again when there are tasks ready)" $T/w6d.out'
fi

if want 6b; then
reset
table << EOF
t/c base
t/d base
EOF
echo a > "$T/head"
(WORKER_ID=site-pueue-gpu1 TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 FAKE_SECS=3 TASKQ_HEAD_CMD=$HEADCMD exec bash "$HERE/worker_start.sh") > "$T/w6b.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^start .* t/c" $T/train.log'
echo b > "$T/head"
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "outside Slurm: code changes while training; finishes, claims no next task, exits" '[[ $rc == 0 ]] && grep -q "^end .* t/c" $T/train.log && ! grep -q "t/d" $T/train.log && grep -q "code updated" $T/w6b.out && [[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'
fi

if want 6c; then
reset
table << EOF
t/a base
t/b base
t/c base
EOF
echo a > "$T/head"
(WORKER_ID=site-pueue-gpu1 TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 RUN_TASK_TRIES=1 FAKE_FAIL="t/a t/b" TASKQ_HEAD_CMD=$HEADCMD exec bash "$HERE/worker_start.sh") > "$T/w6c.out" 2>&1 &
wpid=$!
waitfor 30 '[[ $(grep -c "^fail" $T/train.log) == 2 ]]'
waitfor 20 'grep -q "not claiming until the window passes" $T/w6c.out'
sleep 2
check "outside Slurm: two failures shortly after start; claims no more and does not exit" 'kill -0 $wpid 2> /dev/null && ! grep -q "start .* t/c" $T/train.log && grep -q "not claiming until the window passes" $T/w6c.out'
kill -TERM $wpid; wait $wpid 2> /dev/null
fi

# CPU worker outside Slurm (pueue cpu_worker): uses at most TASKQ_CPUS cores
if want 6f; then
reset
table << EOF
t/c1 base cpu
EOF
(WORKER_ID=site-pueue-cpu WORKER_DEVICE=cpu TASKQ_CPUS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 exec bash "$HERE/worker_start.sh") > "$T/w6f.out" 2>&1 &
wpid=$!
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid
check "CPU worker outside Slurm: uses at most TASKQ_CPUS cores" 'grep -q "worker (cpu).*CPU 1," $T/w6f.out && grep -q "^start .* t/c1 .*cpus=1$" $T/train.log'
fi

# 6e. bundles (<root>/code/<bundle>, current points at the one in use): when current changes and the worker is idle, exec the new bundle's worker_start.sh and keep running in the same process
if want 6e; then
reset
table << EOF
t/k base
EOF
mkdir -p "$T/code/a/scripts/taskq" "$T/code/b/scripts/taskq"   # config and plugins found through TASKQ_CONFIG, TASKQ_PLUGINS (testdata)
cp -r "$CORE/." "$T/code/a/scripts/taskq/"; cp -r "$CORE/." "$T/code/b/scripts/taskq/"   # the new bundle must pass the selftest (consume.py --selftest) to switch
printf '#!/bin/bash
echo "new bundle start $WORKER_ID"
' > "$T/code/b/scripts/taskq/queue/worker_start.sh"
ln -sfn a "$T/code/current"
touch -d "5 days ago" "$T/code/a"
(WORKER_ID=site-pueue-gpu1 TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=30 exec bash "$T/code/a/scripts/taskq/queue/worker_start.sh") > "$T/w6e.out" 2>&1 &
wpid=$!
waitfor 20 'grep -q "no claimable tasks" $T/w6e.out'
ln -sfn b "$T/code/current"
waitfor 20 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "bundle changed: execs the new bundle's worker_start.sh while idle and keeps running, no exit and restart" '[[ $rc == 0 ]] && grep -q "switching to new code b and continuing" $T/w6e.out && grep -q "new bundle start site-pueue-gpu1" $T/w6e.out'
check "bundle: worker and task log record the bundle name as the code version (not \"unknown+dirty\")" 'grep -q "code a$" $T/w6e.out && grep -q "^end .* t/k" $T/train.log && head -1 $TASKQ_DATA/logs/tasks/t__k__*_part1.log | grep -q ", code a,"'
check "bundle: the worker touches the bundle it uses (cleanup deletes only ones untouched for 4 days)" '[[ -n $(find "$T/code/a" -maxdepth 0 -mtime -1) ]]'
fi

# 6z. bundle change with unstoppable training still running: hand it to the new bundle on exec, the new code keeps heartbeating, release once it stops
if want 6z; then
reset
table << EOF
t/z base
EOF
rm -rf "$T/code"; mkdir -p "$T/code/a/scripts/taskq" "$T/code/b/scripts/taskq" "$T/binz"
cp -r "$CORE/." "$T/code/a/scripts/taskq/"; cp -r "$CORE/." "$T/code/b/scripts/taskq/"
printf '#!/bin/bash\necho "new bundle start"\nexec python3 "$(dirname "$0")/consume.py"\n' > "$T/code/b/scripts/taskq/queue/worker_start.sh"
printf '#!/bin/bash\nexit 0\n' > "$T/binz/pkill"; chmod +x "$T/binz/pkill"
ln -sfn a "$T/code/current"
(WORKER_ID=site-pueue-gpu1 TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_STALL_MAX=4 TASKQ_GIVEUP=3 TASKQ_BEAT=1 \
  FAKE_ESCAPE=t/z FAKE_ESCAPE_SECS=25 PATH=$T/binz:$PATH exec bash "$T/code/a/scripts/taskq/queue/worker_start.sh") > "$T/w6z.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "unstoppable training" $T/w6z.out'
ln -sfn b "$T/code/current"
waitfor 20 'grep -q "following unstoppable t/z" $T/w6z.out'
check "bundle changed with an unstoppable task: execs the new bundle and hands it over" 'grep -q "switching to new code b and continuing" $T/w6z.out && grep -q "new bundle start" $T/w6z.out && grep -q "following unstoppable t/z" $T/w6z.out && [[ -e $TASKQ_DIR/claims/t__z ]]'
waitfor 40 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "new code follows it: releases only once it stops, then exits" '[[ $rc == 0 ]] && grep -q "unstoppable t/z finally stopped; releasing" $T/w6z.out && [[ ! -e $TASKQ_DIR/claims/t__z ]]'
[[ -s $FAKE_LOG.escape ]] && kill "$(cat "$FAKE_LOG.escape")" 2> /dev/null
fi

# worker_start.sh: a failed conda activate is logged clearly and the worker still starts (consume.py in the code copy is replaced by one that only prints)
if want ws; then
mkdir -p "$T/ws" "$T/wscfg" "$T/wsconda/etc/profile.d"
cp -r "$HERE" "$T/ws/queue"; cp -r "$HERE/../sites" "$T/ws/sites"
printf 'print("worker start")\n' > "$T/ws/queue/consume.py"
printf 'MACHINES = {"ws": dict(hostname=r".*", ssh="fake-ws", scheduler="pueue", root="%s", conda=("%s", "env0"))}\n' "$T/wsroot" "$T/wsconda" > "$T/wscfg/machines.py"
printf 'conda() { return 1; }\n' > "$T/wsconda/etc/profile.d/conda.sh"
TASKQ_CONFIG=$T/wscfg TASKQ_MACHINE=ws bash "$T/ws/queue/worker_start.sh" > "$T/ws.out" 2>&1; rc=$?
check "worker_start: conda activate fails, logs !!, worker still starts" '[[ $rc == 0 ]] && grep -q "!! conda activate env0 failed" $T/ws.out && grep -q "worker start" $T/ws.out'
fi

# gpu_stats / pad_now read real files (fake nvidia-smi): these two functions in consume.py's plugins consumers/gpu_share.py and gpu_pad.py
if want gpu; then
mkdir -p "$T/bin"; printf '#!/bin/bash\necho "30000, 81920"\n' > "$T/bin/nvidia-smi"; chmod +x "$T/bin/nvidia-smi"
GPUCSV=$T/gpu.csv PADBASE=$T/pad
for i in $(seq 12); do echo "2026/10/18 10:$i, 0, 30000 MiB, 81920 MiB, $(( i <= 2 ? 0 : 60 )) %"; done > "$GPUCSV"
unset WORKER_TEST_GPU
gpu_stats() { python3 -c 'import sys; sys.path.insert(0, sys.argv[1]); sys.path.insert(0, sys.argv[1] + "/consumers"); import gpu_share
print(*gpu_share.gpu_stats(sys.argv[2], sys.argv[3:]))' "$HERE" "$GPUCSV" ${NVSEL[@]+"${NVSEL[@]}"}; }
pad_now() { python3 -c 'import sys; sys.path.insert(0, sys.argv[1]); sys.path.insert(0, sys.argv[1] + "/consumers"); import gpu_pad
print(gpu_pad.pad_now(sys.argv[2]))' "$HERE" "$PADBASE"; }
NVSEL=()
check "gpu_stats: only the last 10 utilization records (earlier 0% does not count), memory from nvidia-smi" '[[ $(PATH=$T/bin:$PATH gpu_stats) == "60 10 30000 81920" ]]'
check "gpu_stats: sample count 0 without records" '[[ $(GPUCSV=$T/none PATH=$T/bin:$PATH gpu_stats) == "0 0 30000 81920" ]]'
printf '#!/bin/bash\n[[ "$*" == "-i 1 "* ]] && echo "5000, 24576" || echo "30000, 81920"\n' > "$T/bin/nvidia-smi"
check "gpu_stats: NVSEL=(-i 1) reads only that GPU's memory" '[[ $(NVSEL=(-i 1); PATH=$T/bin:$PATH gpu_stats) == "60 10 5000 24576" ]]'
echo 12000 > "$PADBASE.mib"
check "pad_now: <base>.mib just updated, use it" '[[ $(pad_now) == 12000 ]]'
touch -d "-2 minutes" "$PADBASE.mib"
check "pad_now: <base>.mib not updated for over 60 seconds (pad gone), counts as 0" '[[ $(pad_now) == 0 ]]'
fi

(( fails == 0 )) && echo "all passed" || { echo "$fails checks failed"; exit 1; }
