#!/bin/bash
# dicc_worker.sh 的模拟测试（2026-10-18）：临时的假队列（TASKQ_DIR）、临时数据目录、假作业号，任务用 command 做法跑假训练
# （睡几秒后写训完的文件）；环境真的按 envs/machines.py 激活（本机 box 的 nnunet、swinunetr）。不用 Slurm、不用 GPU。
#   bash scripts/queue/test_dicc_worker.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
T=$(mktemp -d)
export TASKQ_DIR=$T/tq BRAIN_TUMOR=$T/data
unset nnUNet_results nnUNet_preprocessed
export TASKQ_POLL=1 TASKQ_IDLE_MAX=4 TASKQ_PATIENCE=0 TASKQ_MIN_LEFT=0 TASKQ_SLOTS=2 TASKQ_ADD_AFTER=0 TASKQ_ERR_MAX=3
mkdir -p "$TASKQ_DIR" "$BRAIN_TUMOR"
touch "$TASKQ_DIR/ENABLED"
cat > "$T/fake_train.sh" << 'EOF'
#!/bin/bash
# 假训练：参数 任务名；记下开始、结束时刻、环境，睡 ${FAKE_SECS:-3} 秒后写训完的文件
out=$BRAIN_TUMOR/runs/$1
mkdir -p "$out"
echo "start $(date +%s.%N) $1 ${VIRTUAL_ENV:-无} $(command -v python) cpus=${TASK_CPUS:-}" >> "$FAKE_LOG"
trap 'echo "term $1" >> "$FAKE_LOG"; exit 143' TERM
sleep "${FAKE_SECS:-3}" & wait $!
[[ ${FAKE_FAIL:-} == "$1" ]] && { echo "fail $1" >> "$FAKE_LOG"; exit 1; }
touch "$out/DONE"
echo "end $(date +%s.%N) $1" >> "$FAKE_LOG"
EOF
chmod +x "$T/fake_train.sh"
export FAKE_LOG=$T/train.log
fails=0
check() { if eval "$2"; then echo "通过：$1"; else echo "失败：$1"; fails=$((fails + 1)); fi; }
table() {  # 每行：任务名 环境
    : > "$TASKQ_DIR/tasks.jsonl"
    local i=0 name env
    while read -r name env; do
        i=$((i + 1))
        python3 - "$TASKQ_DIR/tasks.jsonl" "$i" "$name" "$env" "$T/fake_train.sh" << 'EOF'
import json, sys
p, i, name, env, fake = sys.argv[1:]
t = dict(order=int(i), nice=0, key=name, exp=None, recipe="command",
         params=dict(key=name, cmd=f"bash {fake} {name}", env=env, out=f"runs/{name}", done=["DONE"]))
open(p, "a").write(json.dumps(t) + "\n")
EOF
    done
}
reset() { : > "$T/train.log"; rm -rf "$BRAIN_TUMOR/runs" "$TASKQ_DIR/claims" "$TASKQ_DIR/failures" "$TASKQ_DIR/released"; }

# 1. 旧作业的调用方式（训练器 fold 数据集，参数只记日志）：按顺序领；GPU 不忙、显存够时一张卡 2 个；按任务换环境
export WORKER_TEST_GPU="50 10 20000 81920"   # 近 10 分钟使用率 50%、训练占 20GB
reset
table << EOF
t/a nnunet
t/b swinunetr
t/c nnunet
t/d nnunet
EOF
SLURM_JOB_ID=900001 timeout 90 bash "$HERE/dicc_worker.sh" nnUNetTrainer_save10 0 101 > "$T/w1.out" 2>&1
check "旧作业：四个都训完" '[[ $(grep -c ^end $T/train.log) == 4 ]]'
check "旧作业：GPU 不忙时前两个同时开始" '[[ $(head -2 $T/train.log | grep -c ^start) == 2 ]]'
check "按任务说明换环境：swinunetr 用 ~/envs/swinunetr，nnunet 用 conda 环境（不带进 venv）" \
      'grep -q "^start .* t/b .*envs/swinunetr " $T/train.log && grep "^start .* t/c " $T/train.log | grep -q " 无 .*envs/nnunetv2/bin/python"'
check "每个训练分到的 CPU 传给任务（TASK_CPUS）" 'grep -q "cpus=[0-9]" $T/train.log'
check "领取记录都交还了" '[[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'
check "分段日志按任务名、作业号写在 logs/tasks/" '[[ -f $BRAIN_TUMOR/logs/tasks/t__a__900001_seg1.log ]] && head -1 $BRAIN_TUMOR/logs/tasks/t__a__900001_seg1.log | grep -q "^作业 900001_1 @ .*任务=t/a"'

# 1b. 本机没有的环境（hdbet 只在 DICC 上）不领
reset
table << EOF
t/x hdbet
t/y nnunet
EOF
SLURM_JOB_ID=900007 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w1b.out" 2>&1
check "本机没有的环境不领，别的照常领" '[[ $(grep -c ^end $T/train.log) == 1 ]] && grep -q "end .* t/y" $T/train.log && grep -q "没有能领的任务" $T/w1b.out'

# 2. 不带参数（worker.sbatch）；GPU 跑满（使用率 95%）时一次只训 1 个
reset
table << EOF
t/a nnunet
t/b nnunet
EOF
SLURM_JOB_ID=900002 WORKER_TEST_GPU="95 10 30000 81920" timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2.out" 2>&1
check "GPU 跑满：一个训完才开始下一个" '[[ $(sed -n 2p $T/train.log) == end* && $(grep -c ^end $T/train.log) == 2 ]]'

# 2b. 显存不够一半（训练占 50GB）时也只训 1 个
reset
table << EOF
t/a nnunet
t/b nnunet
EOF
SLURM_JOB_ID=900005 WORKER_TEST_GPU="50 10 50000 81920" timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2b.out" 2>&1
check "显存不够：一次 1 个" '[[ $(sed -n 2p $T/train.log) == end* && $(grep -c ^end $T/train.log) == 2 ]]'

reset
table << EOF
t/a nnunet
EOF
export RQ1_GPU_PAD_BASE=$T/pad
echo 33000 > "$T/pad.mib"
( while [[ ! -e $T/pad.start ]]; do sleep 0.2; done; sleep 2; echo 0 > "$T/pad.mib"; date +%s.%N > "$T/pad.released" ) &
padpid=$!
SLURM_JOB_ID=900006 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2c.out" 2>&1
kill $padpid 2> /dev/null
check "显存填充放掉之后才开始训练" '[[ -f $T/pad.released ]] && awk -v r=$(cat $T/pad.released) "/^start/ {exit !(\$2 >= r)}" $T/train.log'

reset
table << EOF
t/a nnunet
EOF
rm -f "$T/pad.start" "$T/pad.released" "$T/pad.ready"
echo 33000 > "$T/pad.mib"
echo stale > "$T/pad.ready"
( while [[ ! -e $T/pad.start ]]; do sleep 0.2; done; sleep 2; echo 1 > "$T/pad.ready"; date +%s.%N > "$T/pad.released" ) &
padpid=$!
SLURM_JOB_ID=900009 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2c2.out" 2>&1
kill $padpid 2> /dev/null
check "显存填充只放到 floor（不到 0）：等它写 ready 后才开始训练，旧的 ready 不算" '[[ -f $T/pad.released ]] && awk -v r=$(cat $T/pad.released) "/^start/ {exit !(\$2 >= r)}" $T/train.log && ! grep -q "没回应" $T/w2c2.out'
unset RQ1_GPU_PAD_BASE

# 2d. 训练失败：交还、记一次失败，本作业不再领它
reset
table << EOF
t/a nnunet
t/b nnunet
EOF
SLURM_JOB_ID=900008 FAKE_FAIL=t/a TASKQ_SLOTS=1 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2d.out" 2>&1
check "训练失败：记一次失败、接着领下一个" '[[ $(wc -l < $TASKQ_DIR/failures/t__a) == 1 ]] && grep -q "end .* t/b" $T/train.log && [[ $(grep -c "start .* t/a" $T/train.log) == 1 ]]'

# 3. 收到停止信号：停训练、交还领取（下一个作业能接着领）
reset
table << EOF
t/a nnunet
EOF
FAKE_SECS=60 SLURM_JOB_ID=900003 bash "$HERE/dicc_worker.sh" nnUNetTrainer_save10 3 101 > "$T/w3.out" 2>&1 &
wpid=$!
for _ in $(seq 30); do grep -q ^start "$T/train.log" 2> /dev/null && break; sleep 1; done
kill -TERM $wpid; wait $wpid; rc=$?
check "停止信号：退出 143、训练收到停止、领取交还" '[[ $rc == 143 ]] && grep -q ^term $T/train.log && [[ -z $(ls $TASKQ_DIR/claims) ]]'

# 4. box 替 lab 领的（ttl 24 小时、beat 30 分钟前）不被 DICC 作业领走
reset
table << EOF
t/a nnunet
EOF
python3 "$HERE/taskq.py" take --worker lab --ttl 86400 t/a > /dev/null
python3 - "$TASKQ_DIR" << 'EOF'
import json, os, sys, time
p = os.path.join(sys.argv[1], "claims", os.listdir(os.path.join(sys.argv[1], "claims"))[0], "info")
i = json.load(open(p)); i["beat"] = time.time() - 1800; json.dump(i, open(p, "w"))
EOF
SLURM_JOB_ID=900004 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w4.out" 2>&1
check "lab 领着的（30 分钟没续，但在 24 小时内）：DICC 作业不领" '[[ ! -s $T/train.log ]] && grep -q "没有能领的任务" $T/w4.out'

# 5. 队列出错（任务表读不了）连续一段时间、手上没有在训的：结束作业（不再训作业名那一折）
rm -f "$TASKQ_DIR/tasks.jsonl"
SLURM_JOB_ID=900009 TASKQ_IDLE_MAX=60 timeout 60 bash "$HERE/dicc_worker.sh" nnUNetTrainer_save10 0 101 > "$T/w5.out" 2>&1; rc=$?
check "队列出错：报出来、过一会儿结束作业" '[[ $rc == 0 ]] && grep -q "队列出错" $T/w5.out && grep -q "队列连续出错" $T/w5.out'

# gpu_stats / pad_now 读真的文件（假 nvidia-smi）：只取 dicc_worker.sh 里这两个函数
sed -n '/^gpu_stats() {/,/^can_add() {/p' "$HERE/dicc_worker.sh" | sed '$d' > "$T/fns.sh"
mkdir -p "$T/bin"; printf '#!/bin/bash\necho "30000, 81920"\n' > "$T/bin/nvidia-smi"; chmod +x "$T/bin/nvidia-smi"
GPUCSV=$T/gpu.csv PADBASE=$T/pad
for i in $(seq 12); do echo "2026/10/18 10:$i, 0, 30000 MiB, 81920 MiB, $(( i <= 2 ? 0 : 60 )) %"; done > "$GPUCSV"
source "$T/fns.sh"; unset WORKER_TEST_GPU
check "gpu_stats：只看最近 10 条使用率（前面的 0% 不算），显存读 nvidia-smi" '[[ $(PATH=$T/bin:$PATH gpu_stats) == "60 10 30000 81920" ]]'
check "gpu_stats：没有记录时样本数 0" '[[ $(GPUCSV=$T/none PATH=$T/bin:$PATH gpu_stats) == "0 0 30000 81920" ]]'
echo 12000 > "$PADBASE.mib"
check "pad_now：<base>.mib 刚更新过，按它算" '[[ $(pad_now) == 12000 ]]'
touch -d "-2 minutes" "$PADBASE.mib"
check "pad_now：<base>.mib 超过 60 秒没更新（填充不在了），算 0" '[[ $(pad_now) == 0 ]]'

(( fails == 0 )) && { echo "全部通过"; rm -rf "$T"; } || { echo "$fails 项失败（文件在 $T）"; exit 1; }
