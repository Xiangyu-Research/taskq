#!/bin/bash
# After boot (crontab's @reboot, written by queue/setup.sh): start the pueue daemon and put tasks interrupted by the reboot back in
# place. With --gpu-log (machines with gpu_log in the machine table; setup.sh adds it) also start GPU usage records
# (monitor/gpu_log.sh).
#   on_boot.sh [--gpu-log]   without it, uses this machine's table entry
#   on_boot.sh --since <timestamp> [--resume <group,group>]   no reboot, just pueued gone (called by the health check): do not wait;
#                            interrupted tasks are counted from --since (when pueued was last seen alive); groups in --resume (running
#                            then) are started if paused now, others untouched
#   interrupted tasks: ended between 15 minutes before boot (or --since) and now, without success (killed training / inference etc.).
#   Training resumes from its checkpoint; inference skips work already done.
# The crontab redirects output to ~/on_boot.log; the hub's health check pushes a notice when it sees a reboot.
set -u
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # scheduler core root; the user repo root REPO (TASKQ_REPO, else as in cfg.py)
P=$(python3 "$CORE/sites/mtab.py" get . pueue)
if [[ -n ${TASKQ_REPO:-} ]]; then REPO=$TASKQ_REPO; elif [[ $CORE == */scripts/taskq ]]; then REPO=${CORE%/scripts/taskq}; else eval "$(python3 "$CORE/queue/project.py" sh 2> /dev/null)"; REPO=${TASKQ_REPO:-$CORE}; fi
log() { echo "$(date '+%F %T') $*"; }
PROG=${TASKQ_PROG:-on_boot.sh}
unset TASKQ_PROG
GL= SINCE= RESUME=
while (( $# )); do
    case $1 in
        --gpu-log) GL=1 ;;
        --since) [[ ${2:-} =~ ^[0-9]+$ ]] || { echo "usage: $PROG [--gpu-log] [--since <timestamp> [--resume <group,group>]]" >&2; exit 2; }; SINCE=$2; shift ;;
        --resume) [[ -n ${2:-} ]] || { echo "usage: $PROG [--gpu-log] [--since <timestamp> [--resume <group,group>]]" >&2; exit 2; }; RESUME=$2; shift ;;
        *) echo "usage: $PROG [--gpu-log] [--since <timestamp> [--resume <group,group>]]" >&2; exit 2 ;;
    esac
    shift
done
[[ -n $GL ]] || GL=$(python3 "$CORE/sites/mtab.py" get . gpu_log 2> /dev/null)
[[ -n $SINCE ]] || sleep "${ON_BOOT_WAIT:-30}"  # wait for network and data disks at boot
eval "$(python3 "$CORE/queue/proto.py" sh)"
GLD=${GPU_LOG_DIR:-${TQ_GPU_LOG_DIR/#\~/$HOME}}
# a running gpu_log.sh holds a lock on its pid file; lockable = none running
if [[ -n $GL ]] && mkdir -p "$GLD" && flock -n "$GLD/$TQ_GPU_LOG_PID" true; then
    setsid -f bash "$CORE/monitor/gpu_log.sh" > /dev/null 2>&1 < /dev/null && log "started GPU usage records"
fi
if ! $P status > /dev/null 2>&1; then
    rm -f ~/.local/share/pueue/pueue_"$(whoami)".socket  # socket left from last time
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
        print(i)' "$from" "$CORE/queue") || { log "pueue does not respond"; exit 1; }
what=boot; [[ -z $SINCE ]] || what="pueued recovery"
if [[ -n $ids ]]; then
    $P restart --in-place $ids > /dev/null && log "$what: put back in the queue $(echo $ids | tr '\n' ' ')"
else
    log "$what: no interrupted tasks"
fi
if [[ -n $RESUME ]]; then
    paused=$($P group --json | python3 -c 'import json, sys; print(" ".join(g for g, v in json.load(sys.stdin).items() if v["status"] == "Paused"))')
    for g in ${RESUME//,/ }; do
        [[ " $paused " == *" $g "* ]] && $P start -g "$g" > /dev/null && log "$what: started group $g (running before pueued disappeared)"
    done
fi
