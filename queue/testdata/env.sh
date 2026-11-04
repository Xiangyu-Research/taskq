# 测试用的环境脚本（policy.py 的 OUTPUTS["env_script"]）：数据目录按机器表找，source 显存填充的启动器
_fx=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TASKQ_DATA=${TASKQ_DATA:-$(TASKQ_CONFIG=${TASKQ_CONFIG:-$_fx/config} python3 "$_fx/../../sites/mtab.py" get . data 2> /dev/null)}
export TASKQ_DATA
export DEMO_PREPARED=$TASKQ_DATA/prepared DEMO_RESULTS=$TASKQ_DATA/results
if [[ -f $_fx/../gpu_pad_start.sh ]]; then
    source "$_fx/../gpu_pad_start.sh"
fi
unset _fx
