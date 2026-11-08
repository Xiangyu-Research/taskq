#!/bin/bash
# Run one training task: shared by all machines (an executor's worker consume.py, or manually).
#   run_task.sh <recipe> k=v...   (e.g. run_task.sh command key=demo/a 'cmd=python3 train.py' env=<env> out=demo/a done=DONE)
#   run_task.sh --spec <JSON>     run a resolved spec (sent to an executor queue by the hub, the format spec.py resolve makes)
# From the task spec (queue/spec.py, recipes in queue/recipes.py): activate the env (machine table machines.py's envs), set env vars,
# run the command at the repo root. Command output goes to the part log <data dir>/logs/tasks/<key>__<job>_part<part>.log (part number
# TASK_PART); the first line records job, machine, task and code version (read by taskq log and the user's ledger). Job = TASK_JOBTAG
# (worker sets it: the job id in Slurm, "worker id-time" for a resident worker), else "hostname-time".
# Outside Slurm, with CUDA_VISIBLE_DEVICES set (pueue GPU group), wait while the GPU is busy, checking every minute; with the task's
# res.wait_gpu minutes, wait at most that long and then fail (exit 1), otherwise wait forever; with TASKQ_ADMITTED=1 set by the worker,
# do not wait (it checked the GPU before claiming); with TASK_GPU_IDLE (minutes, a group env var in machines.py), also require that the
# GPU usage records (monitor/gpu_log.sh, location in proto.py's GPU_LOG_DIR) show no one else on this GPU for that long.
# Retry a failed training after 5 minutes, up to 3 times (a worker runs it once and releases the task to the queue).
# Exit codes (README.md's "conventions"): 0 done; 3 command exited normally but training did not finish; 2 bad spec or env; 1 training
# failed (the command's exit code is in the part log); 4 missing inputs (not all files in inputs, did not start); 5 disk unreadable,
# cannot tell whether training finished (spec.py done exits 5); >=128 stopped by a signal (SIGTERM to itself is 143, the training
# command's kill propagates as is).
# A spec or env problem (exit 2) also writes the reason to the part log (key from the worker's TASK_KEY or the arguments).
# Stop the task with SIGTERM (pueue kill -s SIGTERM): stop training first (force after 8 seconds) and then exit; next time it resumes from the checkpoint.
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
eval "$(python3 "$HERE/proto.py" sh)"; [[ -n ${TQ_SAFE:-} ]] || { echo "cannot read $HERE/proto.py" >&2; exit 2; }  # shared names and exit codes TQ_*
CORE=$(cd "$HERE/.." && pwd)  # scheduler core root; the user's repo root REPO (TASKQ_REPO, else like cfg.py)
if [[ -n ${TASKQ_REPO:-} ]]; then REPO=$TASKQ_REPO; elif [[ $CORE == */scripts/taskq ]]; then REPO=${CORE%/scripts/taskq}; else eval "$(python3 "$CORE/queue/project.py" sh 2> /dev/null)"; REPO=${TASKQ_REPO:-$CORE}; fi
(( $# >= 1 )) || { echo "usage: run_task.sh <recipe> k=v... or run_task.sh --spec <JSON>" >&2; exit "$TQ_EXIT_RUN_BAD"; }
DATA=$(python3 "$CORE/queue/cfg.py" data)
[[ -n $DATA ]] || { echo "no data dir here (not the store, and not running under an executor root); export TASKQ_DATA=<data dir>" >&2; exit "$TQ_EXIT_RUN_BAD"; }
export TASKQ_DATA=$DATA
ts() { date '+%F %T'; }
JOBTAG=${TASK_JOBTAG:-$(hostname -s)-$(date +%Y%m%d%H%M)}
PART=${TASK_PART:-1}
if [[ $(git -C "$REPO" rev-parse --show-toplevel 2> /dev/null) == "$(readlink -f "$REPO")" ]]; then
    COMMIT=$(git -C "$REPO" rev-parse --short HEAD 2> /dev/null || echo unknown)
    CODE_DIRS=$(python3 "$HERE/cfg.py" get code_dirs 2> /dev/null)
    [[ -z $CODE_DIRS ]] || git -C "$REPO" diff --quiet HEAD -- $CODE_DIRS 2> /dev/null || COMMIT="$COMMIT+dirty"
else
    COMMIT=$(basename "$(readlink -f "$REPO")")  # bundle (relay_bundle.py): the name is a content hash (16 chars)
fi
ARGS=("$@")
early() {  # stop before training: write the reason to stderr and this task's part log
    echo "$1" >&2
    local k=${TASK_KEY:-}
    [[ -n $k ]] || k=$(python3 -c 'import json, sys
a = sys.argv[1:]
print(json.loads(a[1]).get("key") or "" if a[0] == "--spec" else next((x[4:] for x in a[1:] if x.startswith("key=")), ""))' "${ARGS[@]}" 2> /dev/null)
    [[ -n $k ]] || return 0
    mkdir -p "$DATA/$TQ_TASK_LOGS" 2> /dev/null
    { printf "$TQ_LOG_HEAD_FMT\n" "${JOBTAG}_${PART}" "$(hostname -s)" "$k" "$PART" "${TASK_ENV:-unknown}" "$COMMIT" "$(ts)"
      printf "$(ts) $TQ_LOG_NOT_STARTED_FMT\n" "$1"; } 2> /dev/null >> "$DATA/$TQ_TASK_LOGS/${TASK_SAFE:-${k//\//$TQ_SAFE}}$TQ_SAFE${JOBTAG}_$TQ_LOG_PART${PART}.log"
}
if ! SH=$(python3 "$HERE/spec.py" shell "$@" 2> /dev/null); then
    early "bad task spec: $(python3 "$HERE/spec.py" shell "$@" 2>&1 > /dev/null | tail -n 1)"; exit "$TQ_EXIT_RUN_BAD"
fi
eval "$SH"
done_() { python3 "$HERE/spec.py" done "$@"; }
done_ "$@"; rc=$?
(( rc == TQ_EXIT_DONE_YES )) && { echo "$(ts) already done: $TASK_KEY"; exit "$TQ_EXIT_RUN_DONE"; }
(( rc == TQ_EXIT_DONE_UNREADABLE )) && { early "disk unreadable, cannot tell whether training finished"; exit "$TQ_EXIT_RUN_UNREADABLE"; }
ACT=$(python3 "$CORE/sites/mtab.py" activate "$TASK_ENV") || { early "this machine has no env $TASK_ENV (machine table machines.py)"; exit "$TQ_EXIT_RUN_BAD"; }
mkdir -p "$DATA/$TQ_TASK_LOGS" "$TASK_OUT"
LOG=$DATA/$TQ_TASK_LOGS/${TASK_SAFE}$TQ_SAFE${JOBTAG}_$TQ_LOG_PART${PART}.log
TRIES=3
[[ -n ${SLURM_JOB_ID:-}${TASK_JOBTAG:-} ]] && TRIES=1  # in Slurm and run by a worker (TASK_JOBTAG set) try once, the worker handles failure
TRIES=${RUN_TASK_TRIES:-$TRIES}   # tests only
POLL=${RUN_TASK_POLL:-300}        # tests only
GPU_POLL=${RUN_TASK_GPU_POLL:-60} # tests only
STOP_WAIT=${RUN_TASK_STOP_WAIT:-8} # tests only

gpu_free() {  # no one else's processes on the GPU, others' memory <= 500 MiB (our own account's processes do not count)
    [[ -z ${CUDA_VISIBLE_DEVICES:-} || -n ${SLURM_JOB_ID:-} ]] && return 0
    local used mine=0 pid pm
    used=$(nvidia-smi -i "$CUDA_VISIBLE_DEVICES" --query-gpu=memory.used --format=csv,noheader,nounits 2> /dev/null | tr -d ' ')
    while IFS=', ' read -r pid pm; do
        [[ -z $pid ]] && continue
        [[ $(ps -o user= -p "$pid" 2> /dev/null | tr -d ' ') == "$(id -un)" ]] || return 1
        mine=$(( mine + ${pm:-0} ))
    done < <(nvidia-smi -i "$CUDA_VISIBLE_DEVICES" --query-compute-apps=pid,used_memory --format=csv,noheader,nounits 2> /dev/null)
    (( ${used:-0} - mine <= 500 ))
}
gpu_quiet() {  # no one else on this GPU in the last TASK_GPU_IDLE minutes of GPU usage records; if there is, GPU_SEEN gets the last time. No log files -> only look now
    GPU_SEEN=
    [[ -z ${TASK_GPU_IDLE:-} || ${TASK_GPU_IDLE} == 0 || -z ${CUDA_VISIBLE_DEVICES:-} || -n ${SLURM_JOB_ID:-} ]] && return 0
    local dir=${GPU_LOG_DIR:-${TQ_GPU_LOG_DIR/#\~/$HOME}} since files f
    since=$(date -d "-$TASK_GPU_IDLE min" '+%F %T')
    files=("$dir/$(date -d "-$TASK_GPU_IDLE min" +"$TQ_GPU_LOG_FILE")")
    f="$dir/$(date +"$TQ_GPU_LOG_FILE")"; [[ $f == "${files[0]}" ]] || files+=("$f")
    GPU_SEEN=$(for f in "${files[@]}"; do [[ -f $f ]] && tail -n 20000 "$f"; done |
        awk -F, -v s="$since" -v g="$CUDA_VISIBLE_DEVICES" -v me="$(id -un)" '$1 >= s && $2 == g && $6 != "" && $6 != me {t = $1} END {print t}')
    [[ -z $GPU_SEEN ]]
}
wait_gpu() {  # GPU free -> 0; still busy after TASK_WAIT_GPU minutes -> 1. Every check writes a line to the task log (the worker uses the log to spot stalls)
    local waited=0 extra
    while GPU_SEEN=; ! { gpu_free && gpu_quiet; }; do
        if [[ -n ${TASK_WAIT_GPU:-} ]] && (( waited >= TASK_WAIT_GPU * 60 )); then
            extra=", waited $TASK_WAIT_GPU minutes"
            [[ -z ${GPU_SEEN:-} ]] || extra=" (free, someone $GPU_SEEN still using it, needs $TASK_GPU_IDLE minutes free), waited $TASK_WAIT_GPU minutes"
            printf "$(ts) $TQ_LOG_GPU_BUSY_FMT\n" "$CUDA_VISIBLE_DEVICES" "$extra" | tee -a "$LOG"
            return 1
        fi
        if [[ -n ${GPU_SEEN:-} ]]; then
            echo "$(ts) GPU $CUDA_VISIBLE_DEVICES free, but someone $GPU_SEEN still used it; waiting $TASK_GPU_IDLE minutes of quiet (waited $(( waited / 60 )) minutes)" | tee -a "$LOG"
        else
            echo "$(ts) GPU $CUDA_VISIBLE_DEVICES busy, waiting for it (waited $(( waited / 60 )) minutes${TASK_WAIT_GPU:+, at most $TASK_WAIT_GPU minutes})" | tee -a "$LOG"
        fi
        sleep "$GPU_POLL" & wait $!
        waited=$(( waited + GPU_POLL ))
    done
}
kill_tree() { local c s=${2:-TERM}; for c in $(pgrep -P "$1"); do kill_tree "$c" "$s"; done; kill -"$s" "$1" 2> /dev/null; }
gone() { ! kill -0 -- -"$1" 2> /dev/null && ! kill -0 "$1" 2> /dev/null; }  # neither the training process group nor the process itself remains
pid=
on_term() {  # SIGTERM the whole process group, SIGKILL after 8 seconds (checkpoints are written atomically), exit once it is gone
    if [[ -n $pid ]]; then
        echo "$(ts) got stop signal, stopping training"
        kill -TERM -- -"$pid" 2> /dev/null; kill_tree "$pid"
        for _ in $(seq "$STOP_WAIT"); do gone "$pid" && break; sleep 1; done
        if ! gone "$pid"; then
            echo "$(ts) training did not stop within $STOP_WAIT seconds, forcing" | tee -a "$LOG"
            kill -KILL -- -"$pid" 2> /dev/null; kill_tree "$pid" KILL
            for _ in 1 2 3; do gone "$pid" && break; sleep 1; done
        fi
        gone "$pid" || echo "$(ts) training process still there after the force kill (likely stuck in I/O)" | tee -a "$LOG"
    fi
    exit "$TQ_EXIT_RUN_TERM"
}
trap on_term TERM INT
export CUDA_DEVICE_ORDER=PCI_BUS_ID

printf "$TQ_LOG_HEAD_FMT\n" "${JOBTAG}_${PART}" "$(hostname -s)" "$TASK_KEY" "$PART" "$TASK_ENV" "$COMMIT" "$(ts)" >> "$LOG"
echo "$(ts) start: $TASK_KEY (env $TASK_ENV, log $LOG)"
# Inputs (the spec's inputs): check they are all there before starting, exit 4 when not; with a node-local staging dir (TASKQ_STAGE_ROOT,
# from the consumer's slurm plugin) and the task's res.local_inputs, build a private view there and copy in the background
# (queue/inputs.py). The command reads inputs from TASK_INPUTS_ROOT (same relative paths as the data dir)
TASK_INPUTS_ROOT=$DATA CPID=
if (( ${#TASK_INPUTS[@]} )); then
    LROOT=
    [[ ${TASK_LOCAL:-0} == 1 && -n ${TASKQ_STAGE_ROOT:-} ]] && LROOT=$TASKQ_STAGE_ROOT
    V=$(python3 "$HERE/inputs.py" prepare "$DATA" "$TASK_SAFE" "$LROOT" "${TASK_INPUTS[@]}" 2>> "$LOG"); rc=$?
    if (( rc == TQ_EXIT_RUN_MISSING )); then
        echo "$(ts) not starting: $(tail -n 1 "$LOG")"
        exit "$TQ_EXIT_RUN_MISSING"
    elif (( rc == 0 )) && [[ -n $V ]]; then
        TASK_INPUTS_ROOT=$V
        if [[ $V != "$DATA" ]]; then
            python3 "$HERE/inputs.py" copy "$LROOT" "$TASK_SAFE" >> "$LOG" 2>&1 &
            CPID=$!
            echo "$(ts) inputs read from $V (copying to local disk in the background)" | tee -a "$LOG"
        fi
    else
        echo "$(ts) input check failed (exit $rc), reading $DATA as usual" | tee -a "$LOG"
    fi
fi
export TASK_INPUTS_ROOT
trap '[[ -n $CPID ]] && kill "$CPID" 2> /dev/null' EXIT
for (( try = 1; ; try++ )); do
    [[ ${TASKQ_ADMITTED:-} == 1 ]] || wait_gpu || exit "$TQ_EXIT_RUN_FAILED"  # the consumer checked the GPU before claiming
    # own process group so a stop takes the whole group; the task's command gets the core's library (TASKQ_LIB) and commands (bin/ first on PATH)
    ( eval "$ACT" && cd "$REPO" && export TASKQ_LIB=$HERE PATH="$CORE/bin:$PATH" && exec setsid -w bash -c "$TASK_CMD" ) >> "$LOG" 2>&1 < /dev/null &
    train_pid=$!
    wait "$train_pid"; rc=$?; train_pid=
    done_ "$@"; drc=$?
    if (( drc == TQ_EXIT_DONE_YES )); then
        echo "$TQ_DONE_LINE $(ts)" >> "$LOG"; echo "$(ts) done: $TASK_KEY"; exit "$TQ_EXIT_RUN_DONE"
    elif (( drc == TQ_EXIT_DONE_UNREADABLE )); then
        printf "$(ts) $TQ_LOG_ENDED_FMT\n" "$rc" "$TQ_LOG_UNREADABLE, cannot tell whether training finished" | tee -a "$LOG"; exit "$TQ_EXIT_RUN_UNREADABLE"
    fi
    printf "$(ts) $TQ_LOG_ENDED_FMT\n" "$rc" "not done (attempt $try)" | tee -a "$LOG"
    (( rc == 0 )) && exit "$TQ_EXIT_RUN_NOT_DONE"
    (( try >= TRIES )) && exit $(( rc >= 128 ? rc : TQ_EXIT_RUN_FAILED ))
    sleep "$POLL" & wait $!
done
