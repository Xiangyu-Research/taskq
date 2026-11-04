#!/usr/bin/env bash
# 每分钟记一次本机各 GPU 的使用情况，用来看共用的卡什么时候被谁占着、安排我们自己的训练（2026-10-14，在 lab 上跑）。
# 每张卡每分钟至少一行；卡上有多个进程时每个进程一行。只记账号名和程序名（不记完整命令行）。
# 开机由 queue/on_boot.sh --gpu-log 启动（2026-10-16 起不放在 tmux 里；11/4 搬进调度核心）；手动启动：
#   setsid -f bash <调度代码>/monitor/gpu_log.sh > /dev/null 2>&1 < /dev/null
# 记录停了（10 分钟没有新的一行）box 上的健康检查会提醒。哪些机器跑它见机器表的 gpu_log。
# 输出 proto.py 的 GPU_LOG_DIR/GPU_LOG_FILE；同一个文件夹里的 GPU_LOG_PID 锁着时（已经有一个在跑）直接退出（~/gpu_usage/gpu_<年-月>.csv，环境变量 GPU_LOG_DIR 可以改）：
#   time,gpu,util_pct,mem_used_mib,mem_total_mib,user,pid,proc_mem_mib,program
set -u
[[ ${1:-} == -h || ${1:-} == --help ]] && { sed -n '2,8p' "$0"; exit 0; }
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)
eval "$(python3 "$CORE/queue/proto.py" sh)" || exit 1
OUT=${GPU_LOG_DIR:-${TQ_GPU_LOG_DIR/#\~/$HOME}}
INTERVAL=${GPU_LOG_INTERVAL:-60}
mkdir -p "$OUT"
# 每个账号只跑一个：跑着时一直锁着 $OUT/gpu_log.pid（proto.py 的 GPU_LOG_PID），已经有一个在跑就退出（11/4）
exec 9>> "$OUT/$TQ_GPU_LOG_PID"
flock -n 9 || exit 0
echo $$ > "$OUT/$TQ_GPU_LOG_PID"
while true; do
    ts=$(date '+%F %T')
    f=$OUT/$(date +"$TQ_GPU_LOG_FILE")
    [[ -s $f ]] || echo "time,gpu,util_pct,mem_used_mib,mem_total_mib,user,pid,proc_mem_mib,program" > "$f"
    declare -A IDX=() UTIL=() MU=() MT=() SEEN=()
    while IFS=', ' read -r i uuid u mu mt; do
        IDX[$uuid]=$i; UTIL[$i]=$u; MU[$i]=$mu; MT[$i]=$mt
    done < <(nvidia-smi --query-gpu=index,uuid,utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits)
    while IFS=', ' read -r uuid pid pm; do
        [[ -z ${uuid:-} ]] && continue
        i=${IDX[$uuid]:-?}
        user=$(ps -o user= -p "$pid" 2> /dev/null | tr -d ' ')
        # 程序名：python 的话取脚本名或模块入口，其余取可执行文件名
        prog=$(ps -o args= -p "$pid" 2> /dev/null | awk '{
            for (k = 1; k <= NF; k++) if ($k ~ /\.py$/ || $k ~ /run_training_entry|nnUNetv2_/) { n = split($k, a, "/"); print a[n]; exit }
            n = split($1, a, "/"); print a[n] }' | tr -d ',;()')
        echo "$ts,$i,${UTIL[$i]:-},${MU[$i]:-},${MT[$i]:-},${user:-?},$pid,$pm,${prog:-?}" >> "$f"
        SEEN[$i]=1
    done < <(nvidia-smi --query-compute-apps=gpu_uuid,pid,used_memory --format=csv,noheader,nounits)
    for i in "${!UTIL[@]}"; do
        [[ -n ${SEEN[$i]:-} ]] || echo "$ts,$i,${UTIL[$i]},${MU[$i]},${MT[$i]},,,," >> "$f"
    done
    unset IDX UTIL MU MT SEEN
    sleep "$INTERVAL" 9>&-
done
