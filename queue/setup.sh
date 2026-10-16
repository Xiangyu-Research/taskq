#!/bin/bash
# 在 box / lab 上启动 pueue 的常驻进程并建好组（2026-10-16），重复运行没关系。
#   bash scripts/queue/setup.sh
# 常驻进程开机自启：crontab 里加 "@reboot ~/.local/bin/pueued -d"（box、lab 都用这个，lab 没开 linger 用不了 systemd 用户服务）。
# lab 另加每 10 分钟一次的 0 号卡开关：scripts/queue/gpu0_gate.sh。
set -euo pipefail
P=~/.local/bin/pueue
[[ -x $P ]] || bash "$(dirname "$0")/install_pueue.sh"
$P status > /dev/null 2>&1 || { setsid -f ~/.local/bin/pueued -d > /dev/null 2>&1 < /dev/null; sleep 2; }
case $(hostname -s) in
    hub-host) groups="gpu:1 cpu:2 wait:8" ;;
    lab-host*) groups="gpu1:1 gpu0:1 cpu:1 wait:4" ;;
    *) echo "不认识这台机器：$(hostname -s)" >&2; exit 1 ;;
esac
have=$($P group --json | python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin)))')
for gp in $groups; do
    g=${gp%:*} n=${gp#*:}
    [[ " $have " == *" $g "* ]] || $P group add "$g"
    $P parallel -g "$g" "$n"
done
# lab 的 0 号卡组平时是停的，由 gpu0_gate.sh 晚上打开
[[ $groups == *gpu0* && " $have " != *" gpu0 "* ]] && $P pause -g gpu0
cron() { { crontab -l 2> /dev/null | grep -vF "$1" || true; echo "$2 $1"; } | crontab -; }
cron "$HOME/.local/bin/pueued -d" "@reboot"
[[ $groups == *gpu0* ]] && cron "bash $HOME/PhD/scripts/queue/gpu0_gate.sh >> $HOME/gpu0_gate.log 2>&1" "*/10 * * * *"
$P status
