# 调度核心的测试用的固定配置（同 fixture.py）：source 后 TASKQ_CONFIG、TASKQ_PLUGINS 指到 testdata/，PATH 里真的 ssh 前面放 noremote/
_fx=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export TASKQ_CONFIG=$_fx/config TASKQ_PLUGINS=$_fx/plugins
PATH=$(python3 "$_fx/fixture.py" path "$PATH") || exit 1  # 真的 ssh、scp 前面放 noremote/（同 fixture.use()）
export PATH
unset _fx
