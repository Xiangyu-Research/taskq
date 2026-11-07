#!/usr/bin/env bash
# 每分钟记一次本机各 GPU 的使用情况，用来看共用的卡什么时候被谁占着、安排自己的训练（在和别人共用卡的机器上跑）。
# 每张卡每分钟至少一行；卡上有多个进程时每个进程一行。只记账号名和程序名（不记完整命令行）。
# 开机由 queue/on_boot.sh --gpu-log 启动（不放在 tmux 里）；手动启动：
#   setsid -f bash <调度代码>/monitor/gpu_log.sh > /dev/null 2>&1 < /dev/null
# 记录停了（10 分钟没有新的一行）hub 上的健康检查会提醒。哪些机器跑它见机器表的 gpu_log。
# 输出 proto.py 的 GPU_LOG_DIR/GPU_LOG_FILE；同一个文件夹里的 GPU_LOG_PID 锁着时（已经有一个在跑）直接退出（~/gpu_usage/gpu_<年-月>.csv，环境变量 GPU_LOG_DIR 可以改）：
#   time,gpu,util_pct,mem_used_mib,mem_total_mib,user,pid,proc_mem_mib,program
set -u
[[ ${1:-} == -h || ${1:-} == --help ]] && { sed -n '2,8p' "$0"; exit 0; }
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)
eval "$(python3 "$CORE/queue/proto.py" sh)" || exit 1
OUT=${GPU_LOG_DIR:-${TQ_GPU_LOG_DIR/#\~/$HOME}}
INTERVAL=${GPU_LOG_INTERVAL:-60}
mkdir -p "$OUT"
# 每个账号只跑一个：跑着时一直锁着 $OUT/gpu_log.pid（proto.py 的 GPU_LOG_PID），已经有一个在跑就退出
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
        # 程序名：解释器（python*、sh、bash 等）跑的取它后面第一个不是选项的参数（脚本），-m X 取 X，-c 记成"<解释器> -c"；
        # 其余取可执行文件名。都只取最后一段路径
        prog=$(ps -o args= -p "$pid" 2> /dev/null | awk '{
            n = split($1, a, "/"); b = a[n]
            py = (b ~ /^python[0-9.]*$/); sh = (b ~ /^(ba|da|z|k)?sh$/)
            if (!py && !sh) { print b; exit }
            for (k = 2; k <= NF; k++) {
                if (py && $k == "-m" && k < NF) { n = split($(k + 1), a, "/"); print a[n]; exit }
                if ($k ~ /^-[A-Za-z]*c$/) { print b " -c"; exit }
                if ((py && $k ~ /^-[WX]$/) || (sh && $k ~ /^[-+][A-Za-z]*o$/)) { k++; continue }
                if ($k ~ /^[-+]/) continue
                n = split($k, a, "/"); print a[n]; exit
            }
            print b }' | tr -d ',;()')
        echo "$ts,$i,${UTIL[$i]:-},${MU[$i]:-},${MT[$i]:-},${user:-?},$pid,$pm,${prog:-?}" >> "$f"
        SEEN[$i]=1
    done < <(nvidia-smi --query-compute-apps=gpu_uuid,pid,used_memory --format=csv,noheader,nounits)
    for i in "${!UTIL[@]}"; do
        [[ -n ${SEEN[$i]:-} ]] || echo "$ts,$i,${UTIL[$i]},${MU[$i]},${MT[$i]},,,," >> "$f"
    done
    unset IDX UTIL MU MT SEEN
    sleep "$INTERVAL" 9>&-
done
