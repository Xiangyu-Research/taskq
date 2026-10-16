#!/bin/bash
# 在 box / lab 上启动 pueue 的常驻进程并建好组（2026-10-16），重复运行没关系。
#   bash scripts/queue/setup.sh
# crontab（lab 没开 linger 用不了 systemd 用户服务，都用 cron）：
#   两台：@reboot scripts/queue/on_boot.sh（启动 pueued、放回重启时被打断的任务）
#   lab：每 10 分钟 0 号卡开关 gpu0_gate.sh、每 30 分钟从 GitHub 拉代码 scripts/sync/lab_git_pull.sh
#   box：每 30 分钟健康检查、每天 08:30 / 19:30 更新登记表和看板（scripts/monitor/auto_refresh.sh）
set -euo pipefail
P=~/.local/bin/pueue
[[ -x $P ]] || bash "$(dirname "$0")/install_pueue.sh"
# socket 固定放在 ~/.local/share/pueue（默认跟着 XDG_RUNTIME_DIR 走：ssh 登录时是 /run/user/<uid>，cron 里没有这个变量，
# 两边会对不上；lab 没开 linger，/run/user/<uid> 在所有登录都结束后还会被删掉）
D=$HOME/.local/share/pueue CFG=~/.config/pueue/pueue.yml
mkdir -p "$D" "$(dirname "$CFG")"
if [[ -f $CFG ]]; then sed -i "s|^  runtime_directory: .*|  runtime_directory: $D|" "$CFG"; else printf 'shared:\n  runtime_directory: %s\n' "$D" > "$CFG"; fi
if ! $P status > /dev/null 2>&1; then
    rm -f "$D/pueue_$(whoami).socket"  # 过渡用的链接（指向旧位置）或上次留下的
    setsid -f ~/.local/bin/pueued -d > /dev/null 2>&1 < /dev/null; sleep 2
fi
case $(hostname -s) in
    hub-host) groups="gpu:1 cpu:2 wait:0" ;;  # wait 组 0 = 不限（拉回任务大部分时间在睡）
    lab-host*) groups="gpu1:1 gpu0:1 cpu:1 wait:0" ;;
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
# 开机：启动 pueued、放回被打断的任务（lab 另启动 GPU 使用记录）。替换以前的 "@reboot pueued -d"
{ crontab -l 2> /dev/null | grep -vxF "@reboot $HOME/.local/bin/pueued -d" || true; } | crontab -
cron "bash $HOME/PhD/scripts/queue/on_boot.sh >> $HOME/on_boot.log 2>&1" "@reboot"
if [[ $groups == *gpu0* ]]; then  # lab
    cron "bash $HOME/PhD/scripts/queue/gpu0_gate.sh >> $HOME/gpu0_gate.log 2>&1" "*/10 * * * *"
    cron "bash $HOME/PhD/scripts/sync/lab_git_pull.sh >> $HOME/git_pull.log 2>&1" "*/30 * * * *"
else  # box：健康检查；登记表和看板。从只给 cron 用的克隆 ~/auto/PhD 运行（只跑已提交的代码）
    [[ -d ~/auto/PhD/.git ]] || git clone -q git@github.com:xiangyumou/PhD.git ~/auto/PhD
    cron "bash $HOME/auto/PhD/scripts/monitor/auto_refresh.sh health" "5,35 * * * *"
    cron "bash $HOME/auto/PhD/scripts/monitor/auto_refresh.sh registry" "30 8,19 * * *"
fi
$P status
