# DICC 显存填充的启动（2026-10-17，10/18 从 envs/nnunet_paths.sh 抽出来）：gpu-a100 作业要求显存占用 >= 40GB。
# 只占显存、不训练不推理（scripts/queue/gpu_pad.py）。用 source 调用（要用作业主进程的 $$）：scripts/queue/worker.sbatch，
# 和 10/17 前交的按折作业（经 envs/nnunet_paths.sh）。
# 默认所有 Slurm GPU 作业都开；启动前要求卡是空的、或自己做推理的脚本设 RQ1_GPU_PAD=0（名单见 scripts/queue/README.md）。
# 不分模型：等训练的显存稳定了再补，要开始新训练时 dicc_worker.sh 通知它先放掉（见 gpu_pad.py 开头）。
# 总开关是 ~/.cache/phd/gpu_pad/DISABLED；单个作业也可以设 RQ1_GPU_PAD=0。启动失败只记日志，不影响训练。
# 日志在 <数据目录>/logs/workers/gpu_pad_<作业号>.log（10/18 前在 nnUNet/results/logs/）。
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
        echo "$(date '+%F %T') 显存填充启动：target=${RQ1_GPU_PAD_TARGET_MIB:-42000}MiB 日志=$_padlog"
        _retry=${RQ1_GPU_PAD_RETRY_DELAY:-30}
        nohup nice -n 19 bash -c '
            for _try in 1 2 3; do
                python3 "$1" --parent-pid "$2" --supervisor-pid "$BASHPID" >> "$3" 2>&1
                _rc=$?
                (( _rc == 0 )) && break
                [[ -e "$4" ]] && break
                sleep "$5"
            done
        ' _ "$_pad" "$$" "$_padlog" "$_switch" "$_retry" > /dev/null 2>&1 &
        echo "$(date '+%F %T') 显存填充 supervisor pid=$!"
    fi
    unset _pad _switch _padlog _retry
fi
unset _padhere
