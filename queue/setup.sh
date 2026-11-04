#!/bin/bash
# 在 pueue 机器（envs/machines.py 里 scheduler=pueue 的）上启动 pueue 的常驻进程、建组、写 crontab（2026-10-16），重复运行没关系。
#   bash scripts/taskq/queue/setup.sh           在这台上
#   bash scripts/taskq/queue/setup.sh <名字>    在 hub 上替别的机器做：按完整的表算好组、gpu_log，ssh 过去跑那边代码目录里的 setup.sh（11/4）
# 组按 envs/machines.py 建；表里没有、也没有任务的组删掉（只有执行机自己那份的代码包里不删）。
# crontab（lab 没开 linger 用不了 systemd 用户服务，都用 cron）：
#   所有 pueue 机器：@reboot on_boot.sh（启动 pueued、放回重启时被打断的任务；gpu_log 的加 --gpu-log），从代码包、仓库的克隆或装好的 taskq（taskq boot）跑
#   角色 hub：克隆 policy.py 的 OUTPUTS["origin"] 到这台的 repo，按 OUTPUTS["cron"] 写 cron 调 OUTPUTS["entry"]
set -euo pipefail
P=~/.local/bin/pueue
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # 调度代码的根；使用者的仓库根 REPO（TASKQ_REPO，没设时同 cfg.py）
if [[ -n ${TASKQ_REPO:-} ]]; then REPO=$TASKQ_REPO; elif [[ $CORE == */scripts/taskq ]]; then REPO=${CORE%/scripts/taskq}; else eval "$(python3 "$CORE/queue/project.py" sh 2> /dev/null)"; REPO=${TASKQ_REPO:-$CORE}; fi
M="python3 $CORE/sites/mtab.py"
C="python3 $CORE/queue/cfg.py"
eval "$(python3 "$CORE/queue/proto.py" sh)"
if [[ -n ${1:-} && $1 != "$($M whoami 2> /dev/null)" ]]; then
    # 代码包（表里有 code）：核心在包里的 BUNDLE_CORE；仓库的克隆（只有 repo）：和这边同一布局
    ssh=$($M get "$1" ssh) code=$($M get "$1" code) sub=/$TQ_BUNDLE_CORE
    [[ -n $code ]] || { code=$($M get "$1" repo); sub=${CORE#"$REPO"}; }
    [[ -n $ssh && -n $code ]] || { echo "$1：表里没有 ssh 或代码目录" >&2; exit 1; }
    exec ssh "$ssh" "SETUP_GROUPS='$($M groups "$1" | tr '\n' ' ')' SETUP_GPU_LOG='$($M get "$1" gpu_log)' bash $code$sub/queue/setup.sh"
fi
me=$($M whoami) || { echo "envs/machines.py 里没有这台机器（$(hostname -s)）" >&2; exit 1; }
[[ $($M get . scheduler) == pueue ]] || { echo "$me 不用 pueue" >&2; exit 1; }
[[ -x $P ]] || bash "$CORE/queue/install_pueue.sh"
# socket 固定放在 ~/.local/share/pueue（默认跟着 XDG_RUNTIME_DIR 走：ssh 登录时是 /run/user/<uid>，cron 里没有这个变量，
# 两边会对不上；lab 没开 linger，/run/user/<uid> 在所有登录都结束后还会被删掉）
D=$HOME/.local/share/pueue CFG=~/.config/pueue/pueue.yml
mkdir -p "$D" "$(dirname "$CFG")"
if [[ -f $CFG ]]; then sed -i "s|^  runtime_directory: .*|  runtime_directory: $D|" "$CFG"; else printf 'shared:\n  runtime_directory: %s\n' "$D" > "$CFG"; fi
if ! $P status > /dev/null 2>&1; then
    rm -f "$D/pueue_$(whoami).socket"  # 上次留下的
    setsid -f ~/.local/bin/pueued -d > /dev/null 2>&1 < /dev/null; sleep 2
fi
groups=${SETUP_GROUPS-$($M groups)}
have=$($P group --json | python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin)))')
for gp in $groups; do
    g=${gp%:*} n=${gp#*:}
    [[ " $have " == *" $g "* ]] || $P group add "$g"
    $P parallel -g "$g" "$n"
done
[[ -n ${SETUP_GROUPS+x} || -n $($M names hub) ]] || have=  # 代码包里的表只有一台执行机，别的组不算多余
for g in $have; do  # 表里没有的组：空的删掉，有任务的留着并提醒
    [[ $g == default || " $(echo $groups | sed 's/:[0-9]*//g') " == *" $g "* ]] && continue
    if $P status --json | python3 -c 'import json,sys; sys.exit(any(t["group"] == sys.argv[1] for t in json.load(sys.stdin)["tasks"].values()))' "$g"; then
        $P group remove "$g" && echo "删掉组 $g（envs/machines.py 里没有）"
    else
        echo "组 $g 不在 envs/machines.py 里，但还有任务，没有删" >&2
    fi
done
cron() { { crontab -l 2> /dev/null | grep -vF "$1" || true; echo "$2 $1"; } | crontab -; }
# 这份核心在代码包里：经 code（current 的链接）找 BUNDLE_CORE，换包后照样找得到；在仓库的克隆里：表里的 repo 下同一位置；
# 装在仓库外（hub 上装好的 taskq）：这份核心的 taskq boot，项目是表里的 repo（开机时的当前目录找不到 .taskq.toml）
CODE=$($M get . code)
[[ -n ${SETUP_GPU_LOG-$($M get . gpu_log)} ]] && GL=" --gpu-log" || GL=
if [[ -n $CODE && $CORE == "$(readlink -f "$CODE")"/* ]]; then OB="bash $CODE/$TQ_BUNDLE_CORE/queue/on_boot.sh$GL"
elif [[ $CORE == "$REPO"/* ]]; then OB="bash $($M get . repo)${CORE#"$REPO"}/queue/on_boot.sh$GL"
else OB="python3 $CORE/bin/taskq --project $($M get . repo) boot$GL"; fi
crontab -l 2> /dev/null | grep -qE "on_boot\.sh|taskq.* boot" && { crontab -l | { grep -vE "on_boot\.sh|taskq.* boot" || true; } | crontab -; }
cron "$OB >> $($C log on_boot) 2>&1" "@reboot"
crontab -l 2> /dev/null | grep -q "git_pull_cron.sh" && { crontab -l | { grep -v "git_pull_cron.sh" || true; } | crontab -; }  # 10/25 起由 box 快进
roles=" $($M get . roles) " entry=$($C get entry)
if [[ $roles == *" hub "* && -n $entry ]]; then  # 从只给 cron 用的克隆（这台的 repo）运行，只跑已提交的代码
    H=$($M get . repo)
    [[ -d $H/.git ]] || git clone -q --recurse-submodules "$($C get origin)" "$H"
    while read -r line; do
        [[ -n $line ]] && cron "bash $H/$entry ${line##* }" "${line% *}"
    done <<< "$($C get cron)"
fi
$P status
