#!/bin/bash
# dicc_worker.sh 的模拟测试（2026-10-18）：临时的假队列（TASKQ_DIR）、临时数据目录、假作业号，任务用 command 做法跑假训练
# （睡几秒后写训完的文件）；环境真的按 envs/machines.py 激活（本机 box 的 nnunet、swinunetr）。不用 Slurm、不用 GPU。
#   bash scripts/queue/test_dicc_worker.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
T=$(mktemp -d)
export TASKQ_DIR=$T/tq BRAIN_TUMOR=$T/data TASKQ_MACHINE=dicc   # 本机当队列所在的机器（不转发到真的 DICC）
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
if [[ ${FAKE_IGNORE:-} == "$1" ]]; then  # 不理停止信号
    trap 'echo "term $1" >> "$FAKE_LOG"' TERM
    while :; do sleep 1; done
fi
if [[ ${FAKE_BUSY:-} == "$1" ]]; then  # 一直在写结果文件夹、不写日志（像概率图稀疏化）
    for _ in $(seq 10); do touch "$out/progress"; sleep 1; done
elif [[ ${FAKE_HANG:-} == "$1" ]]; then
    sleep 120 & wait $!
else
    sleep "${FAKE_SECS:-3}" & wait $!
fi
[[ ${FAKE_FAIL:-} == "$1" ]] && { echo "fail $1" >> "$FAKE_LOG"; exit 1; }
touch "$out/DONE"
echo "end $(date +%s.%N) $1" >> "$FAKE_LOG"
EOF
chmod +x "$T/fake_train.sh"
export FAKE_LOG=$T/train.log
fails=0
check() { if eval "$2"; then echo "通过：$1"; else echo "失败：$1"; fails=$((fails + 1)); fi; }
table() {  # 每行：任务名 环境 [device]
    : > "$TASKQ_DIR/tasks.jsonl"
    local i=0 name env dev
    while read -r name env dev; do
        i=$((i + 1))
        python3 - "$TASKQ_DIR/tasks.jsonl" "$i" "$name" "$env" "$T/fake_train.sh" "${dev:-gpu}" << 'EOF'
import json, sys
p, i, name, env, fake, dev = sys.argv[1:]
t = dict(order=int(i), nice=0, key=name, exp=None, recipe="command",
         params=dict(key=name, cmd=f"bash {fake} {name}", env=env, out=f"runs/{name}", done=["DONE"], device=dev))
open(p, "a").write(json.dumps(t) + "\n")
EOF
    done
}
reset() { : > "$T/train.log"; rm -rf "$BRAIN_TUMOR/runs" "$TASKQ_DIR/claims" "$TASKQ_DIR/failures" "$TASKQ_DIR/released" "$TASKQ_DIR/stopped"; }

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

# 2e. 开始不久就失败的 2 个任务：作业不再领；跑了一段时间才失败的、两次隔得比时间窗久的不算作业的问题，照常领下一个
reset
table << EOF
t/a nnunet
t/b nnunet
t/c nnunet
EOF
cat > "$T/fail2.sh" << EOF
[[ \$1 == t/a || \$1 == t/b ]] && FAKE_FAIL=\$1; export FAKE_FAIL
exec bash "$T/fake_train.sh.orig" "\$@"
EOF
cp "$T/fake_train.sh" "$T/fake_train.sh.orig"; cp "$T/fail2.sh" "$T/fake_train.sh"
SLURM_JOB_ID=900011 TASKQ_SLOTS=1 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2e1.out" 2>&1
check "两个任务开始不久就失败：作业不再领第三个" '[[ $(grep -c "^fail" $T/train.log) == 2 ]] && ! grep -q "start .* t/c" $T/train.log && grep -q "开始不久就失败" $T/w2e1.out'
reset
SLURM_JOB_ID=900012 TASKQ_SLOTS=1 TASKQ_EARLY_FAIL=0 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2e2.out" 2>&1
check "跑了一段时间才失败的：记在任务上，作业照常领下一个" '[[ $(grep -c "^fail" $T/train.log) == 2 ]] && grep -q "end .* t/c" $T/train.log && [[ -f $TASKQ_DIR/failures/t__a && -f $TASKQ_DIR/failures/t__b ]]'
reset
SLURM_JOB_ID=900013 TASKQ_SLOTS=1 TASKQ_FAIL_WINDOW=0 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2e3.out" 2>&1
check "开始不久就失败、但不在时间窗内的两次：作业照常领下一个" '[[ $(grep -c "^fail" $T/train.log) == 2 ]] && grep -q "end .* t/c" $T/train.log && ! grep -q "开始不久就失败" $T/w2e3.out'
cp "$T/fake_train.sh.orig" "$T/fake_train.sh"

# 2f. 卡住的任务（日志、结果文件夹都不动）：停掉、交还、不记失败，本作业领下一个；一直在写结果文件夹的不算卡住
reset
table << EOF
t/a nnunet
t/b nnunet
t/c nnunet
EOF
SLURM_JOB_ID=900013 TASKQ_SLOTS=1 TASKQ_STALL_MAX=4 FAKE_HANG=t/a FAKE_BUSY=t/b timeout 90 bash "$HERE/dicc_worker.sh" > "$T/w2f.out" 2>&1
check "卡住：停掉、交还、不记失败" 'grep -q "^term t/a" $T/train.log && grep -q "t/a 的日志和结果文件夹 .*当作卡住" $T/w2f.out && grep -q "卡住的 t/a 已停下" $T/w2f.out && [[ ! -e $TASKQ_DIR/failures/t__a ]]'
check "卡住的停下后，本作业接着领后面的" 'grep -q "end .* t/b" $T/train.log && grep -q "end .* t/c" $T/train.log && [[ $(grep -c "start .* t/a" $T/train.log) == 1 ]]'
check "一直在写结果文件夹（不写日志）的不算卡住" '! grep -q "t/b 的日志和结果文件夹" $T/w2f.out'
check "领取记录都交还了" '[[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'

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

# 3b. 停止某个任务（taskq.py stop）：30 秒内停下、交还、不记失败，不再领它；另一个照常训；不理停止信号的强制停
reset
table << EOF
t/a nnunet
t/b nnunet
t/c nnunet
EOF
FAKE_SECS=40 FAKE_IGNORE=t/b SLURM_JOB_ID=900031 TASKQ_IDLE_MAX=60 TASKQ_STOP_WAIT=3 TASKQ_POLL=30 bash "$HERE/dicc_worker.sh" > "$T/w3b.out" 2>&1 &
wpid=$!
for _ in $(seq 30); do [[ $(grep -c ^start "$T/train.log" 2> /dev/null) == 2 ]] && break; sleep 1; done
t0=$(date +%s)
python3 "$HERE/taskq.py" stop --why 测试 t/a > "$T/stop.out"
for _ in $(seq 40); do [[ ! -e $TASKQ_DIR/claims/t__a ]] && break; sleep 1; done
dt=$(( $(date +%s) - t0 ))
check "停止：$dt 秒内停下、交还（≤ 30 秒）、不记失败" '(( dt <= 30 )) && grep -q "^term t/a" $T/train.log && [[ ! -e $TASKQ_DIR/claims/t__a && ! -e $TASKQ_DIR/failures/t__a ]] && grep -q "30 秒内停下" $T/stop.out'
python3 "$HERE/taskq.py" status > "$T/st.out"
check "状态里是 stopped" 'grep -q "t/a .*stopped 停止" $T/st.out'
t0=$(date +%s)
python3 "$HERE/taskq.py" stop t/b > /dev/null
for _ in $(seq 40); do [[ ! -e $TASKQ_DIR/claims/t__b ]] && break; sleep 1; done
dt=$(( $(date +%s) - t0 ))
check "不理停止信号的：$dt 秒内强制停、交还（≤ 30 秒）" '(( dt <= 30 )) && grep -q "t/b 3 秒没停下，强制停" $T/w3b.out && [[ ! -e $TASKQ_DIR/claims/t__b ]]'
sleep 3
check "停止的不再领，空出的位置领下一个" '[[ $(grep -c "start .* t/a" $T/train.log) == 1 ]] && grep -q "start .* t/c" $T/train.log'
python3 "$HERE/taskq.py" resume t/a > /dev/null
check "撤销停止后照常排" '[[ ! -e $TASKQ_DIR/stopped/t__a ]] && python3 "$HERE/taskq.py" status | grep -q "t/a .* free"'
kill -TERM $wpid; wait $wpid 2> /dev/null

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

# 4b. CPU worker（WORKER_DEVICE=cpu，10/24）：只领 device=cpu 的，一次 1 个，不记 GPU；GPU worker 不领 device=cpu 的
reset
table << EOF
t/g nnunet
t/c1 nnunet cpu
t/c2 nnunet cpu
EOF
SLURM_JOB_ID=900020 WORKER_DEVICE=cpu TASKQ_SLOTS= timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w4b.out" 2>&1
check "CPU worker：两个纯 CPU 任务先后训完，不碰要卡的" '[[ $(grep -c "^end .* t/c" $T/train.log) == 2 ]] && ! grep -q "t/g" $T/train.log'
check "CPU worker：一次 1 个、不记 GPU" '[[ $(head -2 $T/train.log | grep -c ^start) == 1 && ! -e $BRAIN_TUMOR/logs/workers/worker_900020_gpu.csv ]] && grep -q "worker（cpu）.*最多同时 1 个" $T/w4b.out'
SLURM_JOB_ID=900021 timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w4c.out" 2>&1
check "GPU worker：只训要卡的" 'grep -q "^end .* t/g" $T/train.log && [[ $(grep -c "^start .* t/c" $T/train.log) == 2 ]]'

# 5. 队列出错（任务表读不了）连续一段时间、手上没有在训的：结束作业（不再训作业名那一折）
rm -f "$TASKQ_DIR/tasks.jsonl"
SLURM_JOB_ID=900009 TASKQ_IDLE_MAX=60 timeout 60 bash "$HERE/dicc_worker.sh" nnUNetTrainer_save10 0 101 > "$T/w5.out" 2>&1; rc=$?
check "队列出错：报出来、过一会儿结束作业" '[[ $rc == 0 ]] && grep -q "队列出错" $T/w5.out && grep -q "队列连续出错" $T/w5.out'

# 6. 不在 Slurm 里（lab 的 pueue 组里常驻，10/31）：WORKER_ID 当名字，按本机 slots（box 没写 → 1），只看 CUDA_VISIBLE_DEVICES 那张卡；
#    空闲、队列出错、失败太多都不退出（RUN_TASK_TRIES=1：不在 Slurm 里 run_task.sh 自己会重试 3 次）；手上没有在训的时候提交号变了才退出（退出码 0），在训时变了等训完
waitfor() { local i; for i in $(seq "$1"); do eval "$2" && return 0; sleep 1; done; return 1; }
mkdir -p "$T/bin6"; printf '#!/bin/bash\necho "$*" >> %q\n' "$T/nvsmi.args" > "$T/bin6/nvidia-smi"; chmod +x "$T/bin6/nvidia-smi"
echo a > "$T/head"
export TASKQ_HEAD_CMD="cat $T/head"
reset
table << EOF
t/a nnunet
t/b nnunet
EOF
(unset TASKQ_SLOTS; WORKER_ID=lab-test CUDA_VISIBLE_DEVICES=1 TASKQ_IDLE_POLL=1 PATH=$T/bin6:$PATH exec bash "$HERE/dicc_worker.sh") > "$T/w6.out" 2>&1 &
wpid=$!
waitfor 60 '[[ $(grep -c ^end $T/train.log) == 2 ]]'
check "不在 Slurm 里：按 slots 一次 1 个（GPU 不忙也不加第二个），两个先后训完" '[[ $(sed -n 2p $T/train.log) == end* && $(grep -c ^end $T/train.log) == 2 ]] && grep -q "作业 lab-test @ .*最多同时 1 个.*时限 无" $T/w6.out'
check "不在 Slurm 里：nvidia-smi 只看 CUDA_VISIBLE_DEVICES 那张卡" '[[ -s $T/nvsmi.args ]] && ! grep -qv "^-i 1 " $T/nvsmi.args'
sleep 6
check "不在 Slurm 里：空闲超过 IDLE_MAX 也不退出" 'kill -0 $wpid 2> /dev/null && grep -q "没有能领的任务，每 1 秒再看" $T/w6.out && ! grep -q "结束作业" $T/w6.out'
mv "$TASKQ_DIR/tasks.jsonl" "$TASKQ_DIR/tasks.jsonl.bak"
sleep 6
check "不在 Slurm 里：队列连续出错超过 ERR_MAX 也不退出" 'kill -0 $wpid 2> /dev/null && grep -q "队列出错" $T/w6.out && ! grep -q "队列连续出错" $T/w6.out'
mv "$TASKQ_DIR/tasks.jsonl.bak" "$TASKQ_DIR/tasks.jsonl"
echo b > "$T/head"
waitfor 15 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "不在 Slurm 里：空闲时提交号变了就退出（退出码 0）" '[[ $rc == 0 ]] && grep -q "代码更新了，退出让 feed.py 用新代码重开" $T/w6.out'

reset
table << EOF
t/c nnunet
EOF
echo a > "$T/head"
(WORKER_ID=lab-test TASKQ_IDLE_POLL=1 FAKE_SECS=5 exec bash "$HERE/dicc_worker.sh") > "$T/w6b.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^start .* t/c" $T/train.log'
echo b > "$T/head"
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "不在 Slurm 里：在训时提交号变了，等训完再退出" '[[ $rc == 0 ]] && grep -q "^end .* t/c" $T/train.log && grep -q "代码更新了" $T/w6b.out && [[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'

reset
table << EOF
t/a nnunet
t/b nnunet
t/c nnunet
EOF
echo a > "$T/head"
cp "$T/fake_train.sh" "$T/fake_train.sh.orig"; cp "$T/fail2.sh" "$T/fake_train.sh"
(WORKER_ID=lab-test TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 RUN_TASK_TRIES=1 exec bash "$HERE/dicc_worker.sh") > "$T/w6c.out" 2>&1 &
wpid=$!
waitfor 30 '[[ $(grep -c "^fail" $T/train.log) == 2 ]]'
sleep 5
check "不在 Slurm 里：两个开始不久就失败，不再领、也不退出" 'kill -0 $wpid 2> /dev/null && ! grep -q "start .* t/c" $T/train.log && grep -q "先不领" $T/w6c.out'
kill -TERM $wpid; wait $wpid 2> /dev/null
cp "$T/fake_train.sh.orig" "$T/fake_train.sh"
unset TASKQ_HEAD_CMD

# gpu_stats / pad_now 读真的文件（假 nvidia-smi）：只取 dicc_worker.sh 里这两个函数
sed -n '/^gpu_stats() {/,/^can_add() {/p' "$HERE/dicc_worker.sh" | sed '$d' > "$T/fns.sh"
mkdir -p "$T/bin"; printf '#!/bin/bash\necho "30000, 81920"\n' > "$T/bin/nvidia-smi"; chmod +x "$T/bin/nvidia-smi"
GPUCSV=$T/gpu.csv PADBASE=$T/pad
for i in $(seq 12); do echo "2026/10/18 10:$i, 0, 30000 MiB, 81920 MiB, $(( i <= 2 ? 0 : 60 )) %"; done > "$GPUCSV"
source "$T/fns.sh"; unset WORKER_TEST_GPU
check "gpu_stats：只看最近 10 条使用率（前面的 0% 不算），显存读 nvidia-smi" '[[ $(PATH=$T/bin:$PATH gpu_stats) == "60 10 30000 81920" ]]'
check "gpu_stats：没有记录时样本数 0" '[[ $(GPUCSV=$T/none PATH=$T/bin:$PATH gpu_stats) == "0 0 30000 81920" ]]'
printf '#!/bin/bash\n[[ "$*" == "-i 1 "* ]] && echo "5000, 24576" || echo "30000, 81920"\n' > "$T/bin/nvidia-smi"
check "gpu_stats：NVSEL=(-i 1) 时只读那张卡的显存" '[[ $(NVSEL=(-i 1); PATH=$T/bin:$PATH gpu_stats) == "60 10 5000 24576" ]]'
echo 12000 > "$PADBASE.mib"
check "pad_now：<base>.mib 刚更新过，按它算" '[[ $(pad_now) == 12000 ]]'
touch -d "-2 minutes" "$PADBASE.mib"
check "pad_now：<base>.mib 超过 60 秒没更新（填充不在了），算 0" '[[ $(pad_now) == 0 ]]'

(( fails == 0 )) && { echo "全部通过"; rm -rf "$T"; } || { echo "$fails 项失败（文件在 $T）"; exit 1; }
