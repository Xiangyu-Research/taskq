#!/bin/bash
# 在 pueue 机器（envs/machines.py 里 scheduler=pueue 的）上启动 pueue 的常驻进程、建组、写 crontab（2026-10-16），重复运行没关系。
#   bash scripts/queue/setup.sh
# 组按 envs/machines.py 建；表里没有、也没有任务的组删掉。
# crontab（lab 没开 linger 用不了 systemd 用户服务，都用 cron）：
#   所有 pueue 机器：@reboot scripts/queue/on_boot.sh（启动 pueued、放回重启时被打断的任务），从表里的 code（没有就 repo）跑
#   角色 hub（box）：每 30 分钟健康检查；08:30–20:30 每 2 小时更新登记表和看板、推送进度；每天 04:20 刷新备份目录的访问时间
#   （scripts/monitor/auto_refresh.sh）
set -euo pipefail
P=~/.local/bin/pueue
REPO=$(git -C "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" rev-parse --show-toplevel)
M="python3 $REPO/envs/machines.py"
me=$($M whoami) || { echo "envs/machines.py 里没有这台机器（$(hostname -s)）" >&2; exit 1; }
[[ $($M get . scheduler) == pueue ]] || { echo "$me 不用 pueue" >&2; exit 1; }
[[ -x $P ]] || bash "$REPO/scripts/queue/install_pueue.sh"
# socket 固定放在 ~/.local/share/pueue（默认跟着 XDG_RUNTIME_DIR 走：ssh 登录时是 /run/user/<uid>，cron 里没有这个变量，
# 两边会对不上；lab 没开 linger，/run/user/<uid> 在所有登录都结束后还会被删掉）
D=$HOME/.local/share/pueue CFG=~/.config/pueue/pueue.yml
mkdir -p "$D" "$(dirname "$CFG")"
if [[ -f $CFG ]]; then sed -i "s|^  runtime_directory: .*|  runtime_directory: $D|" "$CFG"; else printf 'shared:\n  runtime_directory: %s\n' "$D" > "$CFG"; fi
if ! $P status > /dev/null 2>&1; then
    rm -f "$D/pueue_$(whoami).socket"  # 上次留下的
    setsid -f ~/.local/bin/pueued -d > /dev/null 2>&1 < /dev/null; sleep 2
fi
groups=$($M groups)
have=$($P group --json | python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin)))')
for gp in $groups; do
    g=${gp%:*} n=${gp#*:}
    [[ " $have " == *" $g "* ]] || $P group add "$g"
    $P parallel -g "$g" "$n"
done
for g in $have; do  # 表里没有的组：空的删掉，有任务的留着并提醒
    [[ $g == default || " $(echo $groups | sed 's/:[0-9]*//g') " == *" $g "* ]] && continue
    if $P status --json | python3 -c 'import json,sys; sys.exit(any(t["group"] == sys.argv[1] for t in json.load(sys.stdin)["tasks"].values()))' "$g"; then
        $P group remove "$g" && echo "删掉组 $g（envs/machines.py 里没有）"
    else
        echo "组 $g 不在 envs/machines.py 里，但还有任务，没有删" >&2
    fi
done
cron() { { crontab -l 2> /dev/null | grep -vF "$1" || true; echo "$2 $1"; } | crontab -; }
CODE=$($M get . code); [[ -n $CODE ]] || CODE=$($M get . repo)
crontab -l 2> /dev/null | grep -q "on_boot.sh" && { crontab -l | grep -v "on_boot.sh" | crontab -; }
cron "bash $CODE/scripts/queue/on_boot.sh >> $HOME/on_boot.log 2>&1" "@reboot"
crontab -l 2> /dev/null | grep -q "git_pull_cron.sh" && { crontab -l | grep -v "git_pull_cron.sh" | crontab -; }  # 10/25 起由 box 快进
roles=" $($M get . roles) "
if [[ $roles == *" hub "* ]]; then  # 健康检查；登记表和看板。从只给 cron 用的克隆 ~/auto/PhD 运行（只跑已提交的代码）
    [[ -d ~/auto/PhD/.git ]] || git clone -q git@github.com:Xiangyu-Research/PhD.git ~/auto/PhD
    cron "bash $HOME/auto/PhD/scripts/monitor/auto_refresh.sh health" "5,35 * * * *"
    cron "bash $HOME/auto/PhD/scripts/monitor/auto_refresh.sh registry" "30 8-20/2 * * *"
    cron "bash $HOME/auto/PhD/scripts/monitor/auto_refresh.sh backup" "20 4 * * *"
fi
$P status
