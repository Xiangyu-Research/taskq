#!/bin/bash
# Simulated tests for consume.py (the consumer) and its plugins: taking over running tasks on a bundle switch, hours window, waiting for a free GPU, Slurm, user plugins
#   bash queue/test_consume.sh        WORKER_TEST_ONLY="h win" runs only these cases
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CORE=$(cd "$HERE/.." && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'rm -rf "${T:?}"' EXIT
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=site-pueue   # this machine is a pueue executor (the Slurm cases set site-slurm instead)
source "$HERE/testdata/worker_harness.sh"
unset TASKQ_GPU_PAD_BASE   # the resident worker has no GPU pad
bundle() {  # bundles a and b (the full core); b's worker_start.sh only prints a line and hands over to consume.py (no conda)
    rm -rf "$T/code"; mkdir -p "$T/code/a/scripts/taskq" "$T/code/b/scripts/taskq"
    cp -r "$CORE/." "$T/code/a/scripts/taskq/"; cp -r "$CORE/." "$T/code/b/scripts/taskq/"
    printf '#!/bin/bash\necho "new bundle start"\nexec python3 "$(dirname "$0")/consume.py"\n' > "$T/code/b/scripts/taskq/queue/worker_start.sh"
    ln -sfn a "$T/code/current"
}
nclaims() { ls "$TASKQ_DIR/claims" 2> /dev/null | grep -vc '\.'; }

# h. bundle switch with training running: exec the new bundle, the new process takes over, and receives and releases the exit codes of done and failed tasks
if want h; then
reset
table << EOF
t/h base
t/f base
EOF
bundle
(WORKER_ID=site-pueue-gpu1 TASKQ_SLOTS=3 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 FAKE_SECS=8 FAKE_FAIL=t/f \
  exec bash "$T/code/a/scripts/taskq/queue/worker_start.sh") > "$T/wh.out" 2>&1 &
wpid=$!
waitfor 30 '[[ $(grep -c "started" $T/wh.out) == 2 ]]'
ln -sfn b "$T/code/current"
waitfor 30 'grep -q "taking over t/f" $T/wh.out'
check "switch: both running tasks handed to the new process" 'grep -q "switching to new code b and continuing (2 running handed to the new code)" $T/wh.out && grep -q "taking over t/h" $T/wh.out && [[ $(nclaims) == 2 ]] && kill -0 $wpid'
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "switch: done and failed exit codes received in the new process, then released" '[[ $rc == 0 ]] && sed -n "/new bundle start/,\$p" $T/wh.out | grep -q "done t/h" && sed -n "/new bundle start/,\$p" $T/wh.out | grep -q "t/f training failed (run_task.sh exit code 1)" && [[ $(nclaims) == 0 && -e $TASKQ_DIR/failures/t__f ]]'
check "switch: the new process records the new bundle as code" 'grep -q ", code b$" $T/wh.out'
fi

# hs. new bundle fails its selftest: stay on the old bundle and keep claiming
if want hs; then
reset
table << EOF
t/s base
t/s2 base
EOF
bundle
printf 'raise RuntimeError("broken")\n' > "$T/code/b/scripts/taskq/queue/consumers/slurm.py"
(WORKER_ID=site-pueue-gpu1 TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 FAKE_SECS=3 \
  exec bash "$T/code/a/scripts/taskq/queue/worker_start.sh") > "$T/whs.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "started t/s (" $T/whs.out'
ln -sfn b "$T/code/current"
waitfor 40 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "selftest fails: one line, stay on the old bundle and keep claiming and training" '[[ $rc == 0 ]] && grep -q "new bundle b failed its selftest (.*broken); staying on a" $T/whs.out && grep -q "done t/s2" $T/whs.out && ! grep -q "new bundle start" $T/whs.out'
fi

# retry. after a failed release the same worker claims it again after FAIL_RETRY; after the second failure the queue marks it
#   failed and nobody claims it (a single resident worker used to skip a task forever after one failure caused by someone
#   else holding the GPU)
if want retry; then
reset
table << EOF
t/r base
EOF
(WORKER_ID=site-pueue-gpu1 TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=6 TASKQ_EARLY_FAIL=0 TASKQ_FAIL_RETRY=2 FAKE_FAIL=t/r \
  exec bash "$HERE/worker_start.sh") > "$T/wr.out" 2>&1 &
wpid=$!
waitfor 60 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "retry: claimed again after a while, not after the second failure" '[[ $rc == 0 && $(grep -c "^fail t/r$" $FAKE_LOG) == 2 && $(nclaims) == 0 ]] && grep -q "t/r: this job claims it again in 0 minutes" $T/wr.out'
fi

# hi. bundle switch while idle: the idle start goes to the new process, which exits once idle for IDLE_EXIT in total instead
#   of counting again from the switch (a CPU worker whose bundle changed every half hour never exited)
if want hi; then
reset
table < /dev/null
bundle
t0=$(date +%s)
(WORKER_ID=site-pueue-gpu1 TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=14 \
  exec bash "$T/code/a/scripts/taskq/queue/worker_start.sh") > "$T/whi.out" 2>&1 &
wpid=$!
waitfor 10 '(( $(date +%s) - t0 >= 7 ))'
ln -sfn b "$T/code/current"
waitfor 20 'grep -q "new bundle start" $T/whi.out'
t1=$(date +%s)
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
t2=$(date +%s)
check "idle switch: idle time carried over, exits once idle for IDLE_EXIT in total" '[[ $rc == 0 ]] && grep -q "idle time carried over: idle for" $T/whi.out && sed -n "/new bundle start/,\$p" $T/whi.out | grep -q "exiting (the next check starts it again" && (( t2 - t1 < 11 && t2 - t0 < 22 ))'
fi

# win. hours window (worker hours in the machine table): no claims outside, exit; claims inside; when it ends, stop what is running, release and exit 0
if want win; then
reset
table << EOF
t/w base
EOF
mkdir -p "$T/wcfg"; cp -r "$TASKQ_CONFIG/." "$T/wcfg/"
# machines.py finds the fake conda by its own location: read it from the original place, then change the worker on site-pueue GPU 0 to the night shift
printf 'import importlib.util as _u
_s = _u.spec_from_file_location("_fx_machines", %s)
_m = _u.module_from_spec(_s)
_s.loader.exec_module(_m)
globals().update({k: v for k, v in vars(_m).items() if not k.startswith("__")})
MACHINES["site-pueue"]["workers"][1]["hours"] = [("21:00", "09:00", None, None)]
' \
  "'$TASKQ_CONFIG/machines.py'" > "$T/wcfg/machines.py"
at() { python3 -c 'import datetime as dt, sys; d = dt.date.today()
print(dt.datetime.combine(d, dt.time(*map(int, sys.argv[1].split(":")))).timestamp() + 86400 * int(sys.argv[2]))' "$@"; }  # local time (site-pueue has no tz)
at 12:00 0 > "$T/now"
(TASKQ_CONFIG=$T/wcfg TASKQ_TEST_NOW=$T/now WORKER_ID=site-pueue-gpu0 TASKQ_IDLE_POLL=1 exec bash "$HERE/worker_start.sh") > "$T/win0.out" 2>&1
rc=$?
check "started outside the hours (site-pueue-gpu0 hours): no claims, exit 0" '[[ $rc == 0 ]] && grep -q "outside the hours (21:00–09:00); exiting" $T/win0.out && [[ ! -s $T/train.log ]]'
at 22:00 0 > "$T/now"
(TASKQ_CONFIG=$T/wcfg TASKQ_TEST_NOW=$T/now WORKER_ID=site-pueue-gpu0 TASKQ_IDLE_POLL=1 FAKE_SECS=60 \
  exec bash "$HERE/worker_start.sh") > "$T/win.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^start .* t/w" $T/train.log'
at 09:01 1 > "$T/now"
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "hours end: stop what is running, release, exit 0" '[[ $rc == 0 ]] && grep -q "hours window ended (21:00–09:00); stopping training and releasing tasks" $T/win.out && grep -q "^term t/w" $T/train.log && [[ $(nclaims) == 0 ]] && ! grep -q "^end .* t/w" $T/train.log'
(TASKQ_CONFIG=$T/wcfg TASKQ_TEST_NOW=$T/now WORKER_ID=site-pueue-gpu1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 exec bash "$HERE/worker_start.sh") > "$T/win1.out" 2>&1
check "worker on the same machine without hours (site-pueue-gpu1): claims and trains outside the window" 'grep -q "done t/w" $T/win1.out && ! grep -q "hours" $T/win1.out'
fi

# idle. waiting for a free GPU (gpu_idle): no claims while someone else just used it or is on it (do not exit), claim once free; run_task.sh does not wait again when TASKQ_ADMITTED=1
if want idle; then
reset
table << EOF
t/i base
EOF
mkdir -p "$T/bini" "$T/gpulog"
printf '#!/bin/bash\n[[ "$*" == *compute-apps* && -e %q ]] && echo "99999999, 900"\nexit 0\n' "$T/busy" > "$T/bini/nvidia-smi"; chmod +x "$T/bini/nvidia-smi"
f=$T/gpulog/$(date +gpu_%Y-%m.csv)
echo "$(date -d '-5 min' '+%F %T'),1,50,9000,24576,someone,4242,9000,python" > "$f"
(WORKER_ID=site-pueue-gpu1 CUDA_VISIBLE_DEVICES=1 TASK_GPU_IDLE=60 GPU_LOG_DIR=$T/gpulog TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 \
  PATH=$T/bini:$PATH exec bash "$HERE/worker_start.sh") > "$T/widle.out" 2>&1 &
wpid=$!
waitfor 20 'grep -q "GPU 1 free, but someone .* still used it; waiting 60 minutes of quiet (waited 0 minutes)" $T/widle.out'
sleep 4
check "someone just used the GPU: no claim, and no idle exit" 'kill -0 $wpid && [[ ! -s $T/train.log && $(nclaims) == 0 ]]'
: > "$f"; touch "$T/busy"
waitfor 20 'grep -q "GPU 1 busy, waiting for it" $T/widle.out'
check "someone else's process on the GPU: no claim" '[[ ! -s $T/train.log && $(nclaims) == 0 ]]'
rm -f "$T/busy"
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "GPU free: claim and finish" '[[ $rc == 0 ]] && grep -q "done t/i" $T/widle.out'
touch "$T/busy"
run() { TASK_JOBTAG=x TASK_KEY=t/r CUDA_VISIBLE_DEVICES=1 RUN_TASK_GPU_POLL=1 PATH=$T/bini:$PATH \
    timeout 8 bash "$HERE/run_task.sh" command key=t/r "cmd=bash $T/fake_train.sh t/r" env=base out=runs/t/r 'done=["DONE"]' device=gpu > "$T/rt.out" 2>&1; }
run; rc=$?
check "run_task.sh run by hand: waits while the GPU is busy" '[[ $rc == 124 ]] && grep -q "GPU 1 busy" $T/rt.out'
TASKQ_ADMITTED=1 run; rc=$?
check "run_task.sh started by the consumer (TASKQ_ADMITTED=1): does not wait again" '[[ $rc == 0 ]] && ! grep -q "GPU 1 busy" $T/rt.out && grep -q "^end .* t/r" $T/train.log'
fi

# slurm. Slurm job: the job id is the name, slots from CPUs and memory, an unknown time limit checked again later, the local staging dir handed to run_task.sh and removed at the end
if want slurm; then
reset
python3 - "$TASKQ_DIR/tasks.jsonl" "$T" << 'EOT'
import json, sys
p, t = sys.argv[1:]
cmd = f'echo "$TASKQ_STAGE_ROOT $TASKQ_ADMITTED" > {t}/stage; mkdir -p "$TASKQ_DATA/runs/t/s"; touch "$TASKQ_DATA/runs/t/s/DONE"'
open(p, "w").write(json.dumps(dict(order=1, nice=0, key="t/s", exp=None, recipe="command",
                                   params=dict(key="t/s", cmd=cmd, env="base", out="runs/t/s", done=["DONE"], device="gpu"))) + "\n")
EOT
eval "$(python3 "$HERE/proto.py" sh)"
mkdir -p "$TMPDIR/${TQ_STAGE_DIR_PREFIX}900300" "$T/bins"; printf '#!/bin/bash\nexit 1\n' > "$T/bins/squeue"; chmod +x "$T/bins/squeue"
(unset TASKQ_SLOTS; TASKQ_MACHINE=site-slurm SLURM_JOB_ID=900300 SLURM_MEM_PER_NODE=65536 TASKQ_IDLE_MAX=2 TASKQ_GPU_PAD_BASE=$T/nopad PATH=$T/bins:$PATH \
  exec bash "$HERE/worker_start.sh") > "$T/wslurm.out" 2>&1
rc=$?
n=$(python3 -c "import os; print(len(os.sched_getaffinity(0)))")
slots=$(( n >= 24 ? 2 : 1 ))
check "Slurm: job id as the name, slots from CPUs and memory, unknown time limit" '[[ $rc == 0 ]] && grep -q "job 900300 @ .*at most $slots at once, CPU $n, memory 65536M.*time limit not known yet, checking later" $T/wslurm.out && grep -q "done t/s" $T/wslurm.out'
check "Slurm: local staging dir (TASKQ_STAGE_ROOT) and TASKQ_ADMITTED handed to run_task.sh, removed at the end" '[[ $(cat $T/stage) == "$TMPDIR/${TQ_STAGE_DIR_PREFIX}900300 1" && ! -e $TMPDIR/${TQ_STAGE_DIR_PREFIX}900300 ]]'
fi

# plug. consumers.py (CONSUMERS) in the user's plugin dir: hooks are still called; no claims while admit blocks
if want plug; then
reset
table << EOF
t/p base
EOF
mkdir -p "$T/plug"; cp -r "$TASKQ_PLUGINS/." "$T/plug/"
cat > "$T/plug/consumers.py" << 'EOT'
import os
CONSUMERS = {"demo": {"setup": lambda w: w.say("user plugin loaded"),
                      "admit": lambda w: None if os.path.exists(os.environ["DEMO_OPEN"]) else "the door is closed"}}
EOT
(TASKQ_PLUGINS=$T/plug DEMO_OPEN=$T/open WORKER_ID=site-pueue-gpu1 TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 exec bash "$HERE/worker_start.sh") > "$T/wplug.out" 2>&1 &
wpid=$!
waitfor 20 'grep -q "user plugin loaded" $T/wplug.out'
sleep 4
check "user plugin: no claim while admit blocks (and no exit)" 'kill -0 $wpid && [[ ! -s $T/train.log ]]'
touch "$T/open"
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "user plugin: claims and finishes once allowed" '[[ $rc == 0 ]] && grep -q "done t/p" $T/wplug.out'
fi

# unit. plugin decisions (no worker started): when gpu_share claims a second, gpu_pad making room, resident's slots, unreadable hours: no new claims and running tasks keep going
if want unit; then
python3 - "$HERE" "$T" << 'EOT' > "$T/unit.out" 2>&1
import importlib.util, os, sys, time, types
here, t = sys.argv[1:]
sys.path.insert(0, here)
def load(n):
    s = importlib.util.spec_from_file_location("c_" + n, f"{here}/consumers/{n}.py"); m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
share, pad, res = load("gpu_share"), load("gpu_pad"), load("resident")
said = []
w = types.SimpleNamespace(busy=lambda: 1, changed=time.time(), ADD_AFTER=900, UTIL_MAX=90, gpucsv="", nvsel=[], pad_now=lambda: 0, say=said.append)
r = [share.admit(w)]
w.changed = 0
for g in ("50 3 20000 81920", "95 10 20000 81920", "50 10 60000 81920", "50 10 20000 81920"):
    os.environ["WORKER_TEST_GPU"] = g; r.append(share.admit(w))
w.pad_now = lambda: 30000; os.environ["WORKER_TEST_GPU"] = "50 10 60000 81920"; r.append(share.admit(w))
w.busy = lambda: 0; r.append(share.admit(w))
assert r == ["just started, waiting a bit", "not enough GPU records", "GPU utilization 95%", "not enough GPU memory (training uses 60000MiB / 81920MiB)", None, None, None], r
assert said == ["GPU utilization 50%, training uses 20000MiB / 81920MiB; claiming another", "GPU utilization 50%, training uses 30000MiB / 81920MiB; claiming another"], said
b = t + "/pad"
open(b + ".mib", "w").write("0"); said.clear(); pad.make_room(b, said.append)
assert os.path.exists(b + ".start") and not said
open(b + ".mib", "w").write("5000"); orig = time.sleep; time.sleep = lambda s: None; pad.make_room(b, said.append); time.sleep = orig
assert said and "did not respond within 30 seconds" in said[0], said
w = types.SimpleNamespace(ncpu=64)
os.environ["TASKQ_SLOTS"] = "3"; os.environ["TASKQ_CPUS"] = "16"; os.environ["CUDA_VISIBLE_DEVICES"] = "1"
res.setup(w)
assert (w.ncpu, w.slots, w.nvsel, w.tag_each) == (16, 3, ["-i", "1"], True), vars(w)
os.environ.pop("TASKQ_SLOTS"); res.setup(w)
assert w.slots == 1, vars(w)
win = load("window")
def broken(w):
    raise ImportError("broken")
win._me = broken; said.clear(); stops = []
w = types.SimpleNamespace(say=said.append, runs={1: 1}, zomb={}, shutdown=lambda *a: stops.append(a))
assert win.active(w) is True and "not claiming until it can be read" in said[0], said
assert win.admit(w) == "cannot read the machine table", win.admit(w)
win.tick(w); assert not stops, stops
w.site, w.wconf = "site-pueue", {"hours": [("9am", "21:00", None, None)]}
assert win.admit(w) and win.admit(w).startswith("cannot read the hours"), win.admit(w)
win.tick(w); assert not stops, stops
print("ok")
EOT
check "plugins: gpu_share decisions, gpu_pad making room, resident CPUs and slots, unreadable machine table or hours" 'grep -qx ok $T/unit.out || { cat $T/unit.out; false; }'
fi

(( fails == 0 )) && echo "all passed" || { echo "$fails checks failed"; exit 1; }
