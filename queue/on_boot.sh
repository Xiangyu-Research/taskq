#!/bin/bash
# 开机后（crontab 的 @reboot，queue/setup.sh 写进去）：启动 pueue 常驻进程，把重启时被打断的任务原地放回队列；
# 带 --gpu-log 时（机器表里 gpu_log 的机器，setup.sh 按表加上）同时启动 GPU 使用记录（monitor/gpu_log.sh）。
#   on_boot.sh [--gpu-log]   不带时按本机的机器表
#   on_boot.sh --since <时间戳> [--resume <组,组>]   没重启、只是 pueued 没了（健康检查调用）：不等，被打断的任务按
#                            --since（上次查到 pueued 还在的时间）算；--resume 的组（那时开着的）现在暂停着就打开，别的组不动
#   被打断的任务：开机前 15 分钟（或 --since）到现在结束、没成功的（被杀的训练 / 推理等）。
#   训练从存档接着训，推理跳过已经做好的。
# 日志在 crontab 里重定向到 ~/on_boot.log；hub 上的健康检查看到重启会推送一条。
set -u
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # 调度代码的根；使用者的仓库根 REPO（TASKQ_REPO，没设时同 cfg.py）
P=$(python3 "$CORE/sites/mtab.py" get . pueue)
if [[ -n ${TASKQ_REPO:-} ]]; then REPO=$TASKQ_REPO; elif [[ $CORE == */scripts/taskq ]]; then REPO=${CORE%/scripts/taskq}; else eval "$(python3 "$CORE/queue/project.py" sh 2> /dev/null)"; REPO=${TASKQ_REPO:-$CORE}; fi
log() { echo "$(date '+%F %T') $*"; }
PROG=${TASKQ_PROG:-on_boot.sh}
unset TASKQ_PROG
GL= SINCE= RESUME=
while (( $# )); do
    case $1 in
        --gpu-log) GL=1 ;;
        --since) [[ ${2:-} =~ ^[0-9]+$ ]] || { echo "用法：$PROG [--gpu-log] [--since <时间戳> [--resume <组,组>]]" >&2; exit 2; }; SINCE=$2; shift ;;
        --resume) [[ -n ${2:-} ]] || { echo "用法：$PROG [--gpu-log] [--since <时间戳> [--resume <组,组>]]" >&2; exit 2; }; RESUME=$2; shift ;;
        *) echo "用法：$PROG [--gpu-log] [--since <时间戳> [--resume <组,组>]]" >&2; exit 2 ;;
    esac
    shift
done
[[ -n $GL ]] || GL=$(python3 "$CORE/sites/mtab.py" get . gpu_log 2> /dev/null)
[[ -n $SINCE ]] || sleep "${ON_BOOT_WAIT:-30}"  # 开机时等网络和数据盘
eval "$(python3 "$CORE/queue/proto.py" sh)"
GLD=${GPU_LOG_DIR:-${TQ_GPU_LOG_DIR/#\~/$HOME}}
# 在跑的 gpu_log.sh 一直锁着它的 pid 文件；锁得上 = 没有在跑的
if [[ -n $GL ]] && mkdir -p "$GLD" && flock -n "$GLD/$TQ_GPU_LOG_PID" true; then
    setsid -f bash "$CORE/monitor/gpu_log.sh" > /dev/null 2>&1 < /dev/null && log "已启动 GPU 使用记录"
fi
if ! $P status > /dev/null 2>&1; then
    rm -f ~/.local/share/pueue/pueue_"$(whoami)".socket  # 上次留下的 socket
    setsid -f "$(dirname "$P")/pueued" -d > /dev/null 2>&1 < /dev/null
    sleep 5
fi
if [[ -n $SINCE ]]; then from=$SINCE; else from=$(( $(date +%s) - $(cut -d. -f1 /proc/uptime) - 900 )); fi
ids=$($P status --json | python3 -c '
import json, sys
sys.path.insert(0, sys.argv[2])
from common import pueue_time
since = int(sys.argv[1])
for i, k in sorted(json.load(sys.stdin)["tasks"].items(), key=lambda x: int(x[0])):
    d = k["status"].get("Done") if isinstance(k["status"], dict) else None
    if not d or d["result"] == "Success":
        continue
    end = pueue_time(d["end"])
    if end is not None and end >= since:
        print(i)' "$from" "$CORE/queue") || { log "pueue 没有响应"; exit 1; }
what=开机; [[ -z $SINCE ]] || what="pueued 恢复"
if [[ -n $ids ]]; then
    $P restart --in-place $ids > /dev/null && log "$what：放回队列 $(echo $ids | tr '\n' ' ')"
else
    log "$what：没有被打断的任务"
fi
if [[ -n $RESUME ]]; then
    paused=$($P group --json | python3 -c 'import json, sys; print(" ".join(g for g, v in json.load(sys.stdin).items() if v["status"] == "Paused"))')
    for g in ${RESUME//,/ }; do
        [[ " $paused " == *" $g "* ]] && $P start -g "$g" > /dev/null && log "$what：打开组 $g（pueued 没了之前开着）"
    done
fi
