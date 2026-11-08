_padhere=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Start only when this machine's table entry has gpu_pad=(floor, target, high) in MiB, or all three thresholds are in the environment
if [[ -n ${SLURM_JOB_ID:-} && -z ${SLURM_STEP_ID:-} && -z ${TASKQ_TEST_FAKEBIN:-} && -z ${QUEUE_TEST_DIR:-} \
      && ${TASKQ_GPU_PAD:-} != 0 && -n ${CUDA_VISIBLE_DEVICES:-} ]] \
   && command -v nvidia-smi > /dev/null 2>&1; then
    if [[ -z ${TASKQ_GPU_PAD_FLOOR_MIB:-} || -z ${TASKQ_GPU_PAD_TARGET_MIB:-} || -z ${TASKQ_GPU_PAD_HIGH_MIB:-} ]]; then
        read -r _pf _pt _ph < <(python3 "$_padhere/../sites/mtab.py" get . gpu_pad 2> /dev/null) || true
        [[ -n ${_pf:-} && -n ${_pt:-} && -n ${_ph:-} ]] && export TASKQ_GPU_PAD_FLOOR_MIB=${TASKQ_GPU_PAD_FLOOR_MIB:-$_pf} \
            TASKQ_GPU_PAD_TARGET_MIB=${TASKQ_GPU_PAD_TARGET_MIB:-$_pt} TASKQ_GPU_PAD_HIGH_MIB=${TASKQ_GPU_PAD_HIGH_MIB:-$_ph}
        unset _pf _pt _ph
    fi
fi
if [[ -n ${SLURM_JOB_ID:-} && -z ${SLURM_STEP_ID:-} && -z ${TASKQ_TEST_FAKEBIN:-} && -z ${QUEUE_TEST_DIR:-} \
      && ${TASKQ_GPU_PAD:-} != 0 && -n ${CUDA_VISIBLE_DEVICES:-} && -n ${TASKQ_GPU_PAD_FLOOR_MIB:-} \
      && -n ${TASKQ_GPU_PAD_TARGET_MIB:-} && -n ${TASKQ_GPU_PAD_HIGH_MIB:-} ]] \
   && command -v nvidia-smi > /dev/null 2>&1; then
    _pad=${TASKQ_GPU_PAD_SCRIPT:-$_padhere/gpu_pad.py}
    _switch=$(python3 "$_padhere/cfg.py" state gpu_pad/DISABLED)  # on an executor it is under queue/ in the root (TASKQ_ROOT)
    if [[ -f $_pad && ! -e $_switch && -z ${TASKQ_GPU_PAD_STARTED:-} ]]; then
        export TASKQ_GPU_PAD_STARTED=1
        while IFS= read -r _l; do
            case $_l in TQ_WORKER_LOGS=* | TQ_GPU_PAD_BASE=*) eval "$_l" ;; esac
        done < <(python3 "$_padhere/proto.py" sh)
        unset _l
        _padlog=${TASKQ_DATA:-$HOME}/$TQ_WORKER_LOGS/gpu_pad_${SLURM_JOB_ID}.log
        mkdir -p "$(dirname "$_padlog")" 2> /dev/null || _padlog=$TQ_GPU_PAD_BASE${SLURM_JOB_ID}.log
        unset TQ_WORKER_LOGS TQ_GPU_PAD_BASE
        echo "$(date '+%F %T') GPU pad started: floor=${TASKQ_GPU_PAD_FLOOR_MIB}MiB target=${TASKQ_GPU_PAD_TARGET_MIB}MiB high=${TASKQ_GPU_PAD_HIGH_MIB}MiB log=$_padlog"
        _py=$(command -v python3)
        # Restart if it exits during the job (delay 1, 2, 4... times RETRY_DELAY, at most 16); give up after MAX_FAILS consecutive runs shorter than RESET_AFTER seconds
        _sup='
            n=0 m=1
            while :; do
                t0=$SECONDS
                "$6" "$1" --parent-pid "$2" --supervisor-pid "$BASHPID" >> "$3" 2>&1
                rc=$?
                (( rc == 0 )) && exit 0
                [[ -e $4 ]] && exit 0
                kill -0 "$2" 2> /dev/null || exit 0
                (( rc == 2 )) && { echo "$(date "+%F %T") !! GPU pad has bad arguments (exit code 2); this job has no GPU pad ($3)"; exit 1; }
                (( SECONDS - t0 >= $8 )) && n=0 m=1
                n=$((n + 1))
                if (( n >= $7 )); then
                    echo "$(date "+%F %T") !! GPU pad failed to start $n times in a row (exit code $rc); not restarting, this job has no GPU pad ($3)"; exit 1
                fi
                echo "$(date "+%F %T") GPU pad exited (exit code $rc); restarting in $m x $5 seconds ($3)"
                for ((i = 0; i < m; i++)); do sleep "$5"; done
                (( m < 16 )) && m=$((m * 2))
            done'
        _args=(_ "$_pad" "$$" "$_padlog" "$_switch" "${TASKQ_GPU_PAD_RETRY_DELAY:-30}" "$_py"
               "${TASKQ_GPU_PAD_MAX_FAILS:-5}" "${TASKQ_GPU_PAD_RESET_AFTER:-600}")
        if [[ -f /dev/stdout ]]; then  # the job log is a file: write the supervisor's messages into it (sharing the same open file)
            nohup nice -n 19 bash -c "$_sup" "${_args[@]}" 2>&1 &
        else
            nohup nice -n 19 bash -c "$_sup" "${_args[@]}" > /dev/null 2>&1 &
        fi
        echo "$(date '+%F %T') GPU pad supervisor pid=$!"
    elif [[ ! -f $_pad ]]; then
        echo "$(date '+%F %T') GPU pad not started: cannot find $_pad"
    fi
    unset _pad _switch _padlog _py _sup _args
fi
unset _padhere
