# 调度核心的测试用的固定配置（同 fixture.py）：source 后 TASKQ_CONFIG、TASKQ_PLUGINS 指到 testdata/
_fx=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export TASKQ_CONFIG=$_fx/config TASKQ_PLUGINS=$_fx/plugins
unset _fx
