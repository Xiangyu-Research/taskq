#!/bin/bash
# 不在 Slurm 里（pueue 常驻 worker）的卡住检测：日志名是主机名-时间，不是 WORKER_ID（11/3）
# 假训练卡住不动：worker 停掉它、交还，接着训下一个；不用 Slurm、不用 GPU。
#   bash queue/test_worker_stall.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'rm -rf "${T:?}"' EXIT
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
check "第一次卡住：只交还，不记失败" '[[ ! -e $TASKQ_DIR/failures/t__a ]] && grep -q "^当作卡住" $TASKQ_DATA/logs/tasks/t__a__*_part1.log'

# 停不下来的（run_task.sh 退出了、训练还在，11/4）：空出位置训别的，接着心跳，真停下了才交还；还在时不退出
reset
table << EOT
t/z base
t/b base
EOT
mkdir -p "$T/binz"; printf '#!/bin/bash\nexit 0\n' > "$T/binz/pkill"; chmod +x "$T/binz/pkill"   # 会话级的强制停不起作用
(WORKER_ID=lab-z TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_STALL_MAX=4 TASKQ_GIVEUP=3 TASKQ_BEAT=1 \
  FAKE_ESCAPE=t/z FAKE_ESCAPE_SECS=25 PATH=$T/binz:$PATH exec bash "$HERE/dicc_worker.sh") > "$T/wz.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^end .* t/b" $T/train.log'
sleep 3
beat=$(python3 -c 'import json, sys, time; print(int(time.time() - json.load(open(sys.argv[1]))["beat"]))' "$TASKQ_DIR/claims/t__z/info" 2> /dev/null)
check "停不下来的：空出位置训完 t/b，worker 不退出" 'kill -0 $wpid && grep -q "t/z 停了 3 秒还没停下" $T/wz.out && grep -q "还有停不下来的训练" $T/wz.out'
check "停不下来的：接着心跳，不交还（领取 $beat 秒前心跳）" '[[ -n $beat ]] && (( beat <= 3 ))'
waitfor 40 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "停不下来的停下后：交还、不记失败，worker 再退出" '[[ $rc == 0 ]] && grep -q "停不下来的 t/z 终于停下了，交还" $T/wz.out && [[ ! -e $TASKQ_DIR/claims/t__z && ! -e $TASKQ_DIR/failures/t__z ]]'
[[ -s $FAKE_LOG.escape ]] && kill "$(cat "$FAKE_LOG.escape")" 2> /dev/null

# 一周内第 2 次卡住：记一次失败（11/4）
reset
table << EOT
t/z base
EOT
(WORKER_ID=lab-z2 TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_STALL_MAX=4 FAKE_HANG=t/z timeout 40 bash "$HERE/dicc_worker.sh") > "$T/wz2.out" 2>&1
check "第 2 次卡住：记一次失败" 'grep -q "一周内第 2 次卡住，记一次失败" $T/wz2.out && grep -q "卡住（一周内第 2 次）" $TASKQ_DIR/failures/t__z'

# 心跳时领取已经不是自己的（11/4）：停掉训练，不交还、不记失败
reset
table << EOT
t/l base
EOT
(WORKER_ID=lab-l TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_BEAT=1 FAKE_SECS=30 exec bash "$HERE/dicc_worker.sh") > "$T/wl.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^start .* t/l" $T/train.log'
python3 -c 'import json, sys; p = sys.argv[1]; c = json.load(open(p)); c["worker"] = "other"; json.dump(c, open(p, "w"))' "$TASKQ_DIR/claims/t__l/info"
waitfor 40 '! kill -0 $wpid 2> /dev/null'; wait $wpid
check "领取被接管：停掉训练，不交还、不记失败" 'grep -q "t/l 的领取已经不是本作业的" $T/wl.out && grep -q "^term t/l" $T/train.log && grep -q "\"other\"" $TASKQ_DIR/claims/t__l/info && [[ ! -e $TASKQ_DIR/failures/t__l ]]'

(( fails == 0 )) && echo 全部通过 || { echo "失败 $fails 项"; exit 1; }
