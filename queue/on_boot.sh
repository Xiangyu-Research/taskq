#!/bin/bash
# 开机后（crontab 的 @reboot，queue/setup.sh 写进去）：启动 pueue 常驻进程，把重启时被打断的任务原地放回队列；
# 带 --gpu-log 时（机器表里 gpu_log 的机器，setup.sh 按表加上）同时启动 GPU 使用记录（monitor/gpu_log.sh）。2026-10-16。
#   on_boot.sh [--gpu-log]   不带时按本机的机器表（代码包里的机器表只有执行机那一项，所以 setup.sh 写成带参数的；11/4）
#   被打断的任务：开机前 15 分钟到现在结束、没成功的（被杀的训练 / 推理等）。
#   训练从存档接着训，推理跳过已经做好的。
# 日志在 crontab 里重定向到 ~/on_boot.log；box 上的健康检查看到重启会推送一条。
set -u
P=~/.local/bin/pueue
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # 调度代码的根；使用者的仓库根 REPO（TASKQ_REPO，没设时同 cfg.py）
REPO=${TASKQ_REPO:-$CORE}; [[ -z ${TASKQ_REPO:-} && $CORE == */scripts/taskq ]] && REPO=${CORE%/scripts/taskq}
log() { echo "$(date '+%F %T') $*"; }
case ${1:-} in
    --gpu-log) GL=1 ;;
    "") GL=$(python3 "${TASKQ_CONFIG:-$REPO/envs}/machines.py" get . gpu_log) ;;
    *) echo "用法：on_boot.sh [--gpu-log]" >&2; exit 2 ;;
esac
sleep "${ON_BOOT_WAIT:-30}"  # 等网络和数据盘
if [[ -n $GL ]] && ! pgrep -f "gpu_log\.sh" > /dev/null; then
    setsid -f bash "$CORE/monitor/gpu_log.sh" > /dev/null 2>&1 < /dev/null && log "已启动 GPU 使用记录"
fi
if ! $P status > /dev/null 2>&1; then
    rm -f ~/.local/share/pueue/pueue_"$(whoami)".socket  # 上次留下的 socket（或过渡用的链接）
    setsid -f ~/.local/bin/pueued -d > /dev/null 2>&1 < /dev/null
    sleep 5
fi
boot=$(( $(date +%s) - $(cut -d. -f1 /proc/uptime) ))
ids=$($P status --json | python3 -c '
import json, sys
sys.path.insert(0, sys.argv[2])
from common import pueue_time
boot = int(sys.argv[1]) - 900
for i, k in sorted(json.load(sys.stdin)["tasks"].items(), key=lambda x: int(x[0])):
    d = k["status"].get("Done") if isinstance(k["status"], dict) else None
    if not d or d["result"] == "Success":
        continue
    end = pueue_time(d["end"])
    if end >= boot:
        print(i)' "$boot" "$CORE/queue") || { log "pueue 没有响应"; exit 1; }
if [[ -n $ids ]]; then
    $P restart --in-place $ids > /dev/null && log "开机：放回队列 $(echo $ids | tr '\n' ' ')"
else
    log "开机：没有被打断的任务"
fi
