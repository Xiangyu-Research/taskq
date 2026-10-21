_padhere=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [[ -n ${SLURM_JOB_ID:-} && -z ${SLURM_STEP_ID:-} && -z ${RQ1_TEST_FAKEBIN:-} && -z ${QUEUE_TEST_DIR:-} \
      && ${RQ1_GPU_PAD:-1} != 0 && -n ${CUDA_VISIBLE_DEVICES:-} ]] \
   && command -v nvidia-smi > /dev/null 2>&1; then
    _pad=${RQ1_GPU_PAD_SCRIPT:-$_padhere/gpu_pad.py}
    _switch=${RQ1_GPU_PAD_SWITCH:-$HOME/.cache/phd/gpu_pad/DISABLED}
    if [[ -f $_pad && ! -e $_switch && -z ${RQ1_GPU_PAD_STARTED:-} ]]; then
        export RQ1_GPU_PAD_STARTED=1
        _padlog=${BRAIN_TUMOR:-$HOME}/logs/workers/gpu_pad_${SLURM_JOB_ID}.log
        mkdir -p "$(dirname "$_padlog")" 2> /dev/null || _padlog=/tmp/gpu_pad_${SLURM_JOB_ID}.log
        echo "$(date '+%F %T') 显存填充启动：floor=${RQ1_GPU_PAD_FLOOR_MIB:-42000}MiB target=${RQ1_GPU_PAD_TARGET_MIB:-55000}MiB 日志=$_padlog"
        _retry=${RQ1_GPU_PAD_RETRY_DELAY:-30}
        _py=$(command -v python3)
        nohup nice -n 19 bash -c '
            for _try in 1 2 3; do
                "$6" "$1" --parent-pid "$2" --supervisor-pid "$BASHPID" >> "$3" 2>&1
                _rc=$?
                (( _rc == 0 )) && break
                [[ -e "$4" ]] && break
                sleep "$5"
            done
        ' _ "$_pad" "$$" "$_padlog" "$_switch" "$_retry" "$_py" > /dev/null 2>&1 &
        echo "$(date '+%F %T') 显存填充 supervisor pid=$!"
    fi
    unset _pad _switch _padlog _retry _py
fi
unset _padhere
