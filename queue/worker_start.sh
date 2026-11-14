#!/bin/bash
set -uo pipefail
HERE=$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)  # real path: entered through code/current, stay pinned to the bundle it started with
eval "$(python3 "$HERE/proto.py" sh)"; [[ -n ${TQ_SAFE:-} ]] || { echo "cannot read $HERE/proto.py" >&2; exit 1; }
CORE=$(cd "$HERE/.." && pwd)  # scheduler code root; the user's repo root REPO (TASKQ_REPO, else as in cfg.py)
if [[ -n ${TASKQ_REPO:-} ]]; then REPO=$TASKQ_REPO; elif [[ $CORE == */scripts/taskq ]]; then REPO=${CORE%/scripts/taskq}; else eval "$(python3 "$CORE/queue/project.py" sh 2> /dev/null)"; REPO=${TASKQ_REPO:-$CORE}; fi
# Started without TASKQ_ROOT / TASKQ_MACHINE (a Slurm job queued by an older bundle's sbatch, a worker of an older bundle switching
# to this one): this bundle sits at <root>/code/<bundle>, and the root records its machine name
_b=$(dirname "${CORE%/"$TQ_BUNDLE_CORE"}")
if [[ -z ${TASKQ_ROOT:-} && $CORE == */$TQ_BUNDLE_CORE && $(basename "$_b") == "$TQ_ROOT_CODE" && -d $(dirname "$_b")/$TQ_ROOT_QUEUE ]]; then
    export TASKQ_ROOT=$(dirname "$_b")
fi
if [[ -z ${TASKQ_MACHINE:-} && -n ${TASKQ_ROOT:-} && -s $TASKQ_ROOT/$TQ_ROOT_MACHINE ]]; then read -r TASKQ_MACHINE < "$TASKQ_ROOT/$TQ_ROOT_MACHINE"; export TASKQ_MACHINE; fi
unset _b
QD=${TASKQ_DIR:-$(python3 "$CORE/queue/cfg.py" state taskq)}
[[ -n ${TASKQ_HANDOFF:-} || -f $QD/$TQ_Q_ENABLED ]] || { echo "$(date '+%F %T') the task queue master switch is off; exiting"; exit 0; }
read -r CONDA_BASE CONDA_ENV < <(python3 "$CORE/sites/mtab.py" get . conda) || true
if [[ ! -f ${CONDA_BASE:-}/etc/profile.d/conda.sh ]]; then
    echo "the machine table (machines.py) has no conda for this machine (${CONDA_BASE:-none})" >&2
    [[ -n ${TASKQ_HANDOFF:-} ]] || exit 1  # during a bundle switch: tasks are running and must not be dropped; keep going
else  # the worker's env: a name in envs (activated as for tasks) or a conda env (mtab.worker_activate)
    _act=$(python3 "$CORE/sites/mtab.py" worker-activate .) && eval "$_act" \
        || echo "$(date '+%F %T') !! activating the worker env $CONDA_ENV failed ($CONDA_BASE): tasks still activate their own envs; the GPU pad most likely will not start"
    unset _act
fi
TASKQ_DATA=$(python3 "$CORE/queue/cfg.py" data)  # executor data dir: data under the root (views, results, logs)
export TASKQ_DATA
source "$HERE/gpu_pad_start.sh"
exec python3 "$HERE/consume.py"
