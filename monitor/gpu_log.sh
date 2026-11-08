#!/usr/bin/env bash
# Record GPU usage on this machine once a minute: which shared card is busy and who uses it, to plan your own jobs.
# At least one row per card per minute; one row per process on a card. Only account and program name, not the full command line.
# Started at boot by queue/on_boot.sh --gpu-log (not in tmux); run it by hand with:
#   setsid -f bash <core>/monitor/gpu_log.sh > /dev/null 2>&1 < /dev/null
# If records stop (no new row for 10 minutes) the health check on the hub reports it. Which machines run it is gpu_log in the machine table.
# Writes proto.py's GPU_LOG_DIR/GPU_LOG_FILE; exits if another instance holds the GPU_LOG_PID lock in the same dir
# (~/gpu_usage/gpu_<year-month>.csv, override with GPU_LOG_DIR):
#   time,gpu,util_pct,mem_used_mib,mem_total_mib,user,pid,proc_mem_mib,program
set -u
[[ ${1:-} == -h || ${1:-} == --help ]] && { sed -n '2,9p' "$0"; exit 0; }
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)
eval "$(python3 "$CORE/queue/proto.py" sh)" || exit 1
OUT=${GPU_LOG_DIR:-${TQ_GPU_LOG_DIR/#\~/$HOME}}
INTERVAL=${GPU_LOG_INTERVAL:-60}
mkdir -p "$OUT"
# One per account: holds $OUT/gpu_log.pid (proto.py's GPU_LOG_PID) while running; exit if another is running
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
        # Program name: for interpreters (python*, sh, bash, ...) take the first non-option argument after it (the script),
        # -m X -> X, -c -> "<interpreter> -c"; otherwise the executable name. Only the last path part.
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
