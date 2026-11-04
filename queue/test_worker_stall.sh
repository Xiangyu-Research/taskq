#!/bin/bash
# 不在 Slurm 里（pueue 常驻 worker）的卡住检测：日志名是主机名-时间，不是 WORKER_ID（11/3）
# 假训练卡住不动：worker 停掉它、交还，接着训下一个；不用 Slurm、不用 GPU。
#   bash scripts/taskq/queue/test_worker_stall.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=dicc   # 本机当队列所在的机器（不转发到真的 DICC）
source "$HERE/testdata/worker_harness.sh"
reset
table << EOT
t/a base
t/b base
EOT
(WORKER_ID=lab-test TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_STALL_MAX=4 FAKE_HANG=t/a timeout 40 bash "$HERE/dicc_worker.sh") > "$T/w.out" 2>&1
check "pueue worker：卡住的 t/a 被停掉、交还" 'grep -q "t/a 的日志和结果文件夹 .*当作卡住" $T/w.out && grep -q "^term t/a" $T/train.log'
check "pueue worker：卡住后接着训 t/b" 'grep -q "end .* t/b" $T/train.log'
rm -rf "$T"
(( fails == 0 )) && echo 全部通过 || { echo "失败 $fails 项"; exit 1; }
