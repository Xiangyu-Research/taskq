# Fixed config for core tests (same as fixture.py): after sourcing, TASKQ_CONFIG and TASKQ_PLUGINS point here and noremote/ is before the real ssh on PATH
_fx=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export TASKQ_CONFIG=$_fx/config TASKQ_PLUGINS=$_fx/plugins
PATH=$(python3 "$_fx/fixture.py" path "$PATH") || exit 1  # put noremote/ before the real ssh and scp (same as fixture.use())
export PATH
unset _fx
