#!/bin/bash
# 在有 pueue 组的机器（mtab.py groups：pueue 执行机的 worker、hub 和 store 的核心组、manual_groups）上启动 pueue 的常驻进程、
# 建组、写 crontab，重复运行没关系。
#   taskq site setup           在这台上（即 bash queue/setup.sh）
#   taskq site setup <名字>    在 hub 上替别的机器做：按机器表算好组、gpu_log，ssh 过去跑那边代码包里的 setup.sh（告诉那边 TASKQ_MACHINE）；
#                              ssh=None 的（hub 所在的机器）就在这台上做
# 按整台机器建全部组、设好同时跑几个；别的组（表里没有的、别人手动建的）不动。
# crontab（没开 linger 的机器用不了 systemd 用户服务，都用 cron）：
#   所有 pueue 机器：@reboot on_boot.sh（启动 pueued、放回重启时被打断的任务；gpu_log 的加 --gpu-log），从代码包、仓库的克隆或装好的 taskq（taskq boot）跑
#   角色 hub：克隆 policy.py 的 OUTPUTS["origin"] 到这台的 repo，按 OUTPUTS["cron"] 写 cron 调 OUTPUTS["entry"]
set -euo pipefail
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # 这份调度代码的根（项目由 cfg.py 自己找）
M="python3 $CORE/sites/mtab.py"
C="python3 $CORE/queue/cfg.py"
eval "$(python3 "$CORE/queue/proto.py" sh)"
PROG=${TASKQ_PROG:-setup.sh}
unset TASKQ_PROG
(( $# <= 1 )) && [[ ${1:-} != -* ]] || { echo "用法：$PROG [名字]" >&2; exit 2; }
rc=0
[[ -z ${1:-} ]] || $M get "$1" label > /dev/null 2>&1 || rc=$?
(( rc != TQ_EXIT_GONE )) || { echo "$PROG：机器表里没有 $1" >&2; exit "$rc"; }
# ssh=None 的机器就是 hub 所在的这台：在这台上做
[[ -n ${1:-} && -z $($M get "$1" ssh 2> /dev/null) ]] && set --
if [[ -n ${1:-} && $1 != "$($M whoami 2> /dev/null)" ]]; then
    # 那台上的调度代码（mtab core：代码包里的，或仓库的克隆里的）
    ssh=$($M get "$1" ssh) core=$($M get "$1" core)
    [[ -n $ssh && -n $core ]] || { echo "$1：表里没有 ssh，或没有 root、repo（不知道那台的调度代码在哪）" >&2; exit 1; }
    exec ssh "$ssh" "TASKQ_MACHINE='$1' SETUP_GROUPS='$($M groups "$1" | tr '\n' ' ')' SETUP_GPU_LOG='$($M get "$1" gpu_log)' bash $core/queue/setup.sh"
fi
me=$($M whoami) || { echo "机器表（machines.py）里没有这台机器（$(hostname -s)）" >&2; exit 1; }
P=$($M get . pueue)
groups=${SETUP_GROUPS-$($M groups)}
[[ -n ${groups// /} ]] || { echo "$me 上没有 pueue 组（不是 pueue 执行机、hub、store，也没有 manual_groups）" >&2; exit 1; }
[[ -x $P ]] || bash "$CORE/queue/install_pueue.sh"
# socket 固定放在 ~/.local/share/pueue（默认跟着 XDG_RUNTIME_DIR 走：ssh 登录时是 /run/user/<uid>，cron 里没有这个变量，
# 两边会对不上；没开 linger 时，/run/user/<uid> 在所有登录都结束后还会被删掉）
D=$HOME/.local/share/pueue CFG=~/.config/pueue/pueue.yml
mkdir -p "$D" "$(dirname "$CFG")"
if [[ -f $CFG ]]; then sed -i "s|^  runtime_directory: .*|  runtime_directory: $D|" "$CFG"; else printf 'shared:\n  runtime_directory: %s\n' "$D" > "$CFG"; fi
if ! $P status > /dev/null 2>&1; then
    rm -f "$D/pueue_$(whoami).socket"  # 上次留下的
    setsid -f "$(dirname "$P")/pueued" -d > /dev/null 2>&1 < /dev/null; sleep 2
fi
have=$($P group --json | python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin)))')
for gp in $groups; do
    g=${gp%:*} n=${gp#*:}
    [[ " $have " == *" $g "* ]] || $P group add "$g"
    $P parallel -g "$g" "$n"
done
cron() { { crontab -l 2> /dev/null | grep -vF "$1" || true; echo "$2 $1"; } | crontab -; }
# 这份核心就是这台的调度代码（mtab core：代码包里的经 current 的链接找，换包后照样找得到；或仓库的克隆里的）：跑它的 on_boot.sh；
# 装在别处（hub 上装好的 taskq）：这份核心的 taskq boot，项目是表里的 repo（开机时的当前目录找不到 .taskq.toml）
core=$($M get . core)
[[ -n ${SETUP_GPU_LOG-$($M get . gpu_log)} ]] && GL=" --gpu-log" || GL=
if [[ -n $core && $CORE == "$(readlink -f "$core")" ]]; then OB="bash $core/queue/on_boot.sh$GL"
else OB="python3 $CORE/bin/taskq --project $($M get . repo) boot$GL"; fi
crontab -l 2> /dev/null | grep -qE "on_boot\.sh|taskq.* boot" && { crontab -l | { grep -vE "on_boot\.sh|taskq.* boot" || true; } | crontab -; }
cron "$OB >> $($C log on_boot) 2>&1" "@reboot"
roles=" $($M get . roles) " entry=$($C get entry)
if [[ $roles == *" hub "* && -n $entry ]]; then  # 从只给 cron 用的克隆（这台的 repo）运行，只跑已提交的代码
    H=$($M get . repo)
    [[ -d $H/.git ]] || git clone -q --recurse-submodules "$($C get origin)" "$H"
    while read -r line; do
        [[ -n $line ]] && cron "bash $H/$entry ${line##* }" "${line% *}"
    done <<< "$($C get cron)"
fi
$P status
