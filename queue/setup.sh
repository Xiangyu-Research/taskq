#!/bin/bash
# On a machine with pueue groups (mtab.py groups: pueue executors' workers, the hub and store core groups, manual_groups), start
# the pueue daemon, create groups and write crontab; repeating is fine.
#   taskq site setup           on this machine (that is, bash queue/setup.sh)
#   taskq site setup <name>    on the hub for another machine: compute groups and gpu_log from the machine table, ssh over and run
#                              setup.sh from that bundle (telling it TASKQ_MACHINE); ssh=None (the hub's machine) runs here
# Create all groups for the whole machine and set their concurrency; leave other groups (not in the table, created by hand) alone.
# Timed commands go into this machine's agent clock (sync/agent.py clock), not crontab; crontab keeps only the agent's own line
# (written by taskq site agent install, refreshed here when the agent is installed). Lines earlier versions wrote are removed.
#   every pueue machine: @reboot on_boot.sh (start pueued, put tasks interrupted by the reboot back; add --gpu-log with gpu_log),
#                        run from the bundle, a repo clone or an installed taskq (taskq boot)
#   role hub: clone policy.py's OUTPUTS["origin"] into this machine's repo and add OUTPUTS["cron"] entries calling OUTPUTS["entry"]
set -euo pipefail
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # this scheduler code's root (cfg.py finds the project itself)
M="python3 $CORE/sites/mtab.py"
C="python3 $CORE/queue/cfg.py"
eval "$(python3 "$CORE/queue/proto.py" sh)"
PROG=${TASKQ_PROG:-setup.sh}
unset TASKQ_PROG
(( $# <= 1 )) && [[ ${1:-} != -* ]] || { echo "usage: $PROG [name]" >&2; exit 2; }
rc=0
[[ -z ${1:-} ]] || $M get "$1" label > /dev/null 2>&1 || rc=$?
(( rc != TQ_EXIT_GONE )) || { echo "$PROG: no $1 in the machine table" >&2; exit "$rc"; }
# a machine with ssh=None is the hub's machine: do it here
[[ -n ${1:-} && -z $($M get "$1" ssh 2> /dev/null) ]] && set --
if [[ -n ${1:-} && $1 != "$($M whoami 2> /dev/null)" ]]; then
    # scheduler code on that machine (mtab core: in the bundle or a repo clone)
    ssh=$($M get "$1" ssh) core=$($M get "$1" core)
    [[ -n $ssh && -n $core ]] || { echo "$1: no ssh, or no root/repo in the table (cannot tell where its scheduler code is)" >&2; exit 1; }
    exec "$CORE/sync/xssh" "$ssh" "TASKQ_MACHINE='$1' SETUP_GROUPS='$($M groups "$1" | tr '\n' ' ')' SETUP_GPU_LOG='$($M get "$1" gpu_log)' bash $core/queue/setup.sh"
fi
me=$($M whoami) || { echo "this machine ($(hostname -s)) is not in the machine table (machines.py)" >&2; exit 1; }
P=$($M get . pueue)
groups=${SETUP_GROUPS-$($M groups)}
[[ -n ${groups// /} ]] || { echo "$me has no pueue groups (not a pueue executor, hub or store, and no manual_groups)" >&2; exit 1; }
[[ -x $P ]] || bash "$CORE/queue/install_pueue.sh"
# the socket stays in ~/.local/share/pueue (the default follows XDG_RUNTIME_DIR: /run/user/<uid> on an ssh login, absent in cron,
# so the two would disagree; without linger, /run/user/<uid> is removed after all logins end)
D=$HOME/.local/share/pueue CFG=~/.config/pueue/pueue.yml
mkdir -p "$D" "$(dirname "$CFG")"
if [[ -f $CFG ]]; then sed -i "s|^  runtime_directory: .*|  runtime_directory: $D|" "$CFG"; else printf 'shared:\n  runtime_directory: %s\n' "$D" > "$CFG"; fi
if ! $P status > /dev/null 2>&1; then
    rm -f "$D/pueue_$(whoami).socket"  # left from last time
    setsid -f "$(dirname "$P")/pueued" -d > /dev/null 2>&1 < /dev/null; sleep 2
fi
have=$($P group --json | python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin)))')
for gp in $groups; do
    g=${gp%:*} n=${gp#*:}
    [[ " $have " == *" $g "* ]] || $P group add "$g"
    $P parallel -g "$g" "$n"
done
clock=()  # "when<TAB>command" lines for the agent clock
# This core is this machine's scheduler code (mtab core: found through the current link inside a bundle, still found after a switch;
# or in a repo clone): run its on_boot.sh. Installed elsewhere (taskq installed on the hub): use this core's taskq boot with the
# table's repo as project (the current dir at boot has no .taskq.toml)
core=$($M get . core)
[[ -n ${SETUP_GPU_LOG-$($M get . gpu_log)} ]] && GL=" --gpu-log" || GL=
if [[ -n $core && $CORE == "$(readlink -f "$core")" ]]; then OB="bash $core/queue/on_boot.sh$GL"
else OB="python3 $CORE/bin/taskq --project $($M get . repo) boot$GL"; fi
BL=$($C boot-log)
mkdir -p "$(dirname "$BL")"
clock+=("@reboot"$'\t'"$OB >> $BL 2>&1")
roles=" $($M get . roles) " entry=$($C get entry) old_entry=
if [[ $roles == *" hub "* && -n $entry ]]; then  # run from the hub-only clone (this machine's repo), so only committed code runs
    H=$($M get . repo)
    old_entry="$H/$entry"
    [[ -d $H/.git ]] || git clone -q --recurse-submodules "$($C get origin)" "$H"
    while read -r line; do
        [[ -n $line ]] && clock+=("${line% *}"$'\t'"bash $H/$entry ${line##* }")
    done <<< "$($C get cron)"
fi
printf '%s\n' "${clock[@]}" | python3 -c 'import json, sys; print(json.dumps([l.rstrip("\n").split("\t", 1) for l in sys.stdin if l.strip()]))' \
    | python3 "$CORE/sync/agent.py" clock "$me"
# crontab: drop the lines earlier versions wrote (boot steps, the hub entry); keep the agent's own line, refreshed when installed
AG=${TASKQ_AGENT_HOME:-$HOME}/.local/share/taskq/agent.py
if command -v crontab > /dev/null; then
    cur=$(crontab -l 2> /dev/null || true)
    new=$(printf '%s\n' "$cur" | grep -vE "on_boot\.sh|taskq.* boot" | { if [[ -n $old_entry ]]; then grep -vF "$old_entry"; else cat; fi; } || true)
    [[ $new == "$cur" ]] || printf '%s\n' "$new" | crontab -
    if [[ -f $AG ]]; then
        python3 "$CORE/sync/agent.py" cron "$me" "$(command -v python3)" "$AG"
        python3 "$AG" ensure "$me" || echo "$me: the agent did not start" >&2
    fi
fi
[[ -f $AG ]] || echo "$me: no agent installed, its clock does not run until taskq site agent install $me (on the hub)" >&2
echo "$me clock:"; printf '  %s\n' "${clock[@]}"
$P status
