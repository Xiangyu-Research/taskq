#!/bin/bash
# On a machine with pueue groups (mtab.py groups: pueue executors' workers, the hub and store core groups, manual_groups), start
# the pueue daemon, create groups and write crontab; repeating is fine.
#   taskq site setup           on this machine (that is, bash queue/setup.sh)
#   taskq site setup <name>    on the hub for another machine: compute groups and gpu_log from the machine table, ssh over and run
#                              setup.sh from that bundle (telling it TASKQ_MACHINE); ssh=None (the hub's machine) runs here
# Create all groups for the whole machine and set their concurrency; leave other groups (not in the table, created by hand) alone.
# crontab (machines without linger cannot use systemd user services, so everything is cron):
#   every pueue machine: @reboot on_boot.sh (start pueued, put tasks interrupted by the reboot back; add --gpu-log with gpu_log),
#                        run from the bundle, a repo clone or an installed taskq (taskq boot)
#   role hub: clone policy.py's OUTPUTS["origin"] into this machine's repo and write cron entries from OUTPUTS["cron"] calling OUTPUTS["entry"]
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
    exec ssh "$ssh" "TASKQ_MACHINE='$1' SETUP_GROUPS='$($M groups "$1" | tr '\n' ' ')' SETUP_GPU_LOG='$($M get "$1" gpu_log)' bash $core/queue/setup.sh"
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
cron() { { crontab -l 2> /dev/null | grep -vF "$1" || true; echo "$2 $1"; } | crontab -; }
# This core is this machine's scheduler code (mtab core: found through the current link inside a bundle, still found after a switch;
# or in a repo clone): run its on_boot.sh. Installed elsewhere (taskq installed on the hub): use this core's taskq boot with the
# table's repo as project (the current dir at boot has no .taskq.toml)
core=$($M get . core)
[[ -n ${SETUP_GPU_LOG-$($M get . gpu_log)} ]] && GL=" --gpu-log" || GL=
if [[ -n $core && $CORE == "$(readlink -f "$core")" ]]; then OB="bash $core/queue/on_boot.sh$GL"
else OB="python3 $CORE/bin/taskq --project $($M get . repo) boot$GL"; fi
crontab -l 2> /dev/null | grep -qE "on_boot\.sh|taskq.* boot" && { crontab -l | { grep -vE "on_boot\.sh|taskq.* boot" || true; } | crontab -; }
cron "$OB >> $($C log on_boot) 2>&1" "@reboot"
roles=" $($M get . roles) " entry=$($C get entry)
if [[ $roles == *" hub "* && -n $entry ]]; then  # run from the cron-only clone (this machine's repo), so only committed code runs
    H=$($M get . repo)
    [[ -d $H/.git ]] || git clone -q --recurse-submodules "$($C get origin)" "$H"
    while read -r line; do
        [[ -n $line ]] && cron "bash $H/$entry ${line##* }" "${line% *}"
    done <<< "$($C get cron)"
fi
$P status
