#!/bin/bash
# 开机后（crontab 的 @reboot，scripts/queue/setup.sh 写进去）：启动 pueue 常驻进程，把重启时被打断的任务原地放回队列；
# lab 上同时启动 GPU 使用记录（scripts/monitor/gpu_log.sh，0 号卡夜班开关靠它）。2026-10-16。
#   被打断的任务：开机前 15 分钟到现在结束、没成功的（被杀的训练 / 推理，以及因此"依赖失败"的下游）。
#   不放回：按规则停的（退出码 75，夜班由 gpu0_gate.sh 晚上放回）、白天加跑（标签以 day 开头，每次单独确认）。
#   nnU-Net 从存档接着训，推理跳过已经做好的病例。
# 日志 ~/on_boot.log；box 上的健康检查（scripts/monitor/health.py）看到重启会推送一条。
set -u
P=~/.local/bin/pueue
REPO=$(git -C "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" rev-parse --show-toplevel)
log() { echo "$(date '+%F %T') $*"; }
sleep 30  # 等网络和数据盘
if [[ $(hostname -s) == lab-host* ]] && ! pgrep -f "scripts/monitor/gpu_log.sh" > /dev/null; then
    setsid -f bash "$REPO/scripts/monitor/gpu_log.sh" > /dev/null 2>&1 < /dev/null && log "已启动 GPU 使用记录"
fi
if ! $P status > /dev/null 2>&1; then
    rm -f ~/.local/share/pueue/pueue_"$(whoami)".socket  # 上次留下的 socket（或过渡用的链接）
    setsid -f ~/.local/bin/pueued -d > /dev/null 2>&1 < /dev/null
    sleep 5
fi
boot=$(( $(date +%s) - $(cut -d. -f1 /proc/uptime) ))
ids=$($P status --json | python3 -c '
import datetime, json, sys
boot = int(sys.argv[1]) - 900
for i, k in sorted(json.load(sys.stdin)["tasks"].items(), key=lambda x: int(x[0])):
    d = k["status"].get("Done") if isinstance(k["status"], dict) else None
    if not d or d["result"] in ("Success", {"Failed": 75}) or (k.get("label") or "").startswith("day"):
        continue
    end = datetime.datetime.fromisoformat(d["end"][:26] + d["end"][-6:]).timestamp()
    if end >= boot:
        print(i)' "$boot") || { log "pueue 没有响应"; exit 1; }
if [[ -n $ids ]]; then
    $P restart --in-place $ids > /dev/null && log "开机：放回队列 $(echo $ids | tr '\n' ' ')"
else
    log "开机：没有被打断的任务"
fi
