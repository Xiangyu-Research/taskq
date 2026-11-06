_padhere=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# 只在机器表这台写了 gpu_pad=(floor, target, high)（MiB）、或环境里给齐了三个阈值时开
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
    _switch=${TASKQ_GPU_PAD_SWITCH:-${TASKQ_STATE:+$TASKQ_STATE/gpu_pad/DISABLED}}
    _switch=${_switch:-$HOME/.local/state/taskq/gpu_pad/DISABLED}
    if [[ -f $_pad && ! -e $_switch && -z ${TASKQ_GPU_PAD_STARTED:-} ]]; then
        export TASKQ_GPU_PAD_STARTED=1
        while IFS= read -r _l; do
            case $_l in TQ_WORKER_LOGS=* | TQ_GPU_PAD_BASE=*) eval "$_l" ;; esac
        done < <(python3 "$_padhere/proto.py" sh)
        unset _l
        _padlog=${TASKQ_DATA:-$HOME}/$TQ_WORKER_LOGS/gpu_pad_${SLURM_JOB_ID}.log
        mkdir -p "$(dirname "$_padlog")" 2> /dev/null || _padlog=$TQ_GPU_PAD_BASE${SLURM_JOB_ID}.log
        unset TQ_WORKER_LOGS TQ_GPU_PAD_BASE
        echo "$(date '+%F %T') 显存填充启动：floor=${TASKQ_GPU_PAD_FLOOR_MIB}MiB target=${TASKQ_GPU_PAD_TARGET_MIB}MiB high=${TASKQ_GPU_PAD_HIGH_MIB}MiB 日志=$_padlog"
        _py=$(command -v python3)
        # 作业期间退出了就重启（间隔 1、2、4… 倍 RETRY_DELAY，最多 16 倍）；连续 MAX_FAILS 次跑不到 RESET_AFTER 秒就放弃
        _sup='
            n=0 m=1
            while :; do
                t0=$SECONDS
                "$6" "$1" --parent-pid "$2" --supervisor-pid "$BASHPID" >> "$3" 2>&1
                rc=$?
                (( rc == 0 )) && exit 0
                [[ -e $4 ]] && exit 0
                kill -0 "$2" 2> /dev/null || exit 0
                (( rc == 2 )) && { echo "$(date "+%F %T") !! 显存填充参数错误（退出码 2），这个作业没有显存填充（$3）"; exit 1; }
                (( SECONDS - t0 >= $8 )) && n=0 m=1
                n=$((n + 1))
                if (( n >= $7 )); then
                    echo "$(date "+%F %T") !! 显存填充连续 $n 次起不来（退出码 $rc），不再重启，这个作业没有显存填充（$3）"; exit 1
                fi
                echo "$(date "+%F %T") 显存填充退出（退出码 $rc），$m 倍 $5 秒后重启（$3）"
                for ((i = 0; i < m; i++)); do sleep "$5"; done
                (( m < 16 )) && m=$((m * 2))
            done'
        _args=(_ "$_pad" "$$" "$_padlog" "$_switch" "${TASKQ_GPU_PAD_RETRY_DELAY:-30}" "$_py"
               "${TASKQ_GPU_PAD_MAX_FAILS:-5}" "${TASKQ_GPU_PAD_RESET_AFTER:-600}")
        if [[ -f /dev/stdout ]]; then  # 作业日志是文件：supervisor 的提示写进去（共用同一个打开的文件）
            nohup nice -n 19 bash -c "$_sup" "${_args[@]}" 2>&1 &
        else
            nohup nice -n 19 bash -c "$_sup" "${_args[@]}" > /dev/null 2>&1 &
        fi
        echo "$(date '+%F %T') 显存填充 supervisor pid=$!"
    elif [[ ! -f $_pad ]]; then
        echo "$(date '+%F %T') 显存填充没开：找不到 $_pad"
    fi
    unset _pad _switch _padlog _py _sup _args
fi
unset _padhere
