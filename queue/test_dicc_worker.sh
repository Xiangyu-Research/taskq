#!/bin/bash
# dicc_worker.sh 的模拟测试（2026-10-18）：临时的假队列（TASKQ_DIR）、假作业号、假训练（WORKER_TEST_CMD 睡几秒后写训完的文件），
# 不用 Slurm、不用 GPU，在 box 上跑。
#   bash scripts/queue/test_dicc_worker.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
T=$(mktemp -d)
export TASKQ_DIR=$T/tq nnUNet_results=$T/res nnUNet_preprocessed=$T/pre RQ1_CLAIMS=$T/none
export TASKQ_POLL=1 TASKQ_IDLE_MAX=4 TASKQ_PATIENCE=0 TASKQ_MIN_LEFT=0 TASKQ_SLOTS=2 TASKQ_ADD_AFTER=0
mkdir -p "$TASKQ_DIR" "$nnUNet_preprocessed/Dataset101_GLIrq1" "$nnUNet_preprocessed/Dataset111_MENrq1"
touch "$TASKQ_DIR/ENABLED"
P=nnUNetResEncUNetMPlans__3d_fullres
cat > "$T/fake_train.sh" << 'EOF'
#!/bin/bash
# 假训练：参数 数据集编号 fold；训练器在 WORKER_TEST_TR。记下开始、结束时刻和同时在训的个数，睡 ${FAKE_SECS:-3} 秒后写训完的文件
ds=$(basename "$(ls -d "$nnUNet_preprocessed/Dataset$1_"*)")
out=$nnUNet_results/$ds/${WORKER_TEST_TR}__nnUNetResEncUNetMPlans__3d_fullres/fold_$2
mkdir -p "$out"
echo "start $(date +%s.%N) $ds/$WORKER_TEST_TR/$2 $VIRTUAL_ENV_TAG" >> "$FAKE_LOG"
echo "$WORKER_TEST_TR ${VIRTUAL_ENV:-无} $(command -v python3)" >> "$FAKE_LOG.env"
trap 'echo "term $ds/$WORKER_TEST_TR/$2" >> "$FAKE_LOG"; exit 143' TERM
sleep "${FAKE_SECS:-3}" & wait $!
touch "$out/checkpoint_final.pth"; mkdir -p "$out/validation"; echo {} > "$out/validation/summary.json"
echo "end $(date +%s.%N) $ds/$WORKER_TEST_TR/$2" >> "$FAKE_LOG"
EOF
chmod +x "$T/fake_train.sh"
export WORKER_TEST_CMD=$T/fake_train.sh FAKE_LOG=$T/train.log
fails=0
check() { if eval "$2"; then echo "通过：$1"; else echo "失败：$1"; fails=$((fails + 1)); fi; }
table() {  # 每行 key 种类
    : > "$TASKQ_DIR/tasks.tsv"
    local i=0 key ds tr f
    while read -r key; do
        i=$((i + 1)); IFS=/ read -r ds tr f <<< "$key"
        printf '%s\t0\t%s\t%s\t%s\t%s\t%s\n' "$i" "$key" "${ds:7:3}" "$tr" "$f" "-" >> "$TASKQ_DIR/tasks.tsv"
    done
}
# 假的环境：按训练器 source ~/envs/<名>/bin/activate，这里用临时 HOME 下的假环境，只设一个标记
mkdir -p "$T/home/envs/swinunetr/bin" "$T/home/envs/umamba/bin"
echo "export VIRTUAL_ENV_TAG=swin VIRTUAL_ENV=$T/home/envs/swinunetr PATH=$T/home/envs/swinunetr/bin:\$PATH" > "$T/home/envs/swinunetr/bin/activate"
echo "export VIRTUAL_ENV_TAG=umamba VIRTUAL_ENV=$T/home/envs/umamba PATH=$T/home/envs/umamba/bin:\$PATH" > "$T/home/envs/umamba/bin/activate"
ln -s "$(command -v python3)" "$T/home/envs/swinunetr/bin/python3"

# 1. 旧作业的调用方式（训练器 fold 数据集）：什么模型都领、按顺序；GPU 不忙、显存够时一张卡 2 个；Swin 换 swin 环境
export WORKER_TEST_GPU="50 10 20000 81920"   # 近 10 分钟使用率 50%、训练占 20GB
table << EOF
Dataset101_GLIrq1/nnUNetTrainer_save10/0
Dataset101_GLIrq1/nnUNetTrainer_SwinUNETR/0
Dataset111_MENrq1/nnUNetTrainer_save10/1
Dataset111_MENrq1/nnUNetTrainer_save10/2
EOF
SLURM_JOB_ID=900001 HOME=$T/home timeout 60 bash "$HERE/dicc_worker.sh" nnUNetTrainer_save10 0 101 > "$T/w1.out" 2>&1
check "旧作业：四个都训完（含 Swin）" '[[ $(grep -c ^end $T/train.log) == 4 ]]'
check "旧作业：GPU 不忙时前两个同时开始" '[[ $(head -2 $T/train.log | grep -c ^start) == 2 ]]'
check "旧作业：Swin 用 swin 环境" 'grep -q "^start .*SwinUNETR/0 swin$" $T/train.log'
check "旧作业：领取记录都交还了" '[[ -z $(ls $TASKQ_DIR/claims 2> /dev/null) ]]'

# 1b. 旧的 SwinUNETR 作业（在 swin venv 里开始）领到 nnU-Net：先退出 venv，用默认环境
: > "$T/train.log"; : > "$T/train.log.env"; rm -rf "$nnUNet_results"
table << EOF
Dataset101_GLIrq1/nnUNetTrainer_save10/0
EOF
SLURM_JOB_ID=900007 VIRTUAL_ENV=$T/home/envs/swinunetr PATH=$T/home/envs/swinunetr/bin:$PATH HOME=$T/home \
    timeout 60 bash "$HERE/dicc_worker.sh" nnUNetTrainer_SwinUNETR 0 101 > "$T/w1b.out" 2>&1
check "旧 Swin 作业训 nnU-Net：退出 swin venv" '[[ $(cat $T/train.log.env) == "nnUNetTrainer_save10 无 "* && $(cat $T/train.log.env) != *envs/swinunetr* ]]'

# 2. 不带参数（worker.sbatch）；GPU 跑满（使用率 95%）时一次只训 1 个
: > "$T/train.log"; rm -rf "$nnUNet_results"
table << EOF
Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/0
Dataset101_GLIrq1/nnUNetTrainer_save10/1
EOF
SLURM_JOB_ID=900002 WORKER_TEST_GPU="95 10 30000 81920" HOME=$T/home timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2.out" 2>&1
check "GPU 跑满：一个训完才开始下一个，U-Mamba 用 umamba 环境" \
      '[[ $(sed -n 1p $T/train.log) == *UMambaEnc/0\ umamba && $(sed -n 2p $T/train.log) == end*UMambaEnc* && $(grep -c ^end $T/train.log) == 2 ]]'

# 2b. 显存不够一半（训练占 50GB）时也只训 1 个
: > "$T/train.log"; rm -rf "$nnUNet_results"
table << EOF
Dataset101_GLIrq1/nnUNetTrainer_save10/0
Dataset101_GLIrq1/nnUNetTrainer_save10/1
EOF
SLURM_JOB_ID=900005 WORKER_TEST_GPU="50 10 50000 81920" HOME=$T/home timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2b.out" 2>&1
check "显存不够：一次 1 个" '[[ $(sed -n 2p $T/train.log) == end* && $(grep -c ^end $T/train.log) == 2 ]]'

# 2c. 开始训练前通知显存填充放掉：假 pad 看到 <base>.start 后把 <base>.mib 改成 0
: > "$T/train.log"; rm -rf "$nnUNet_results"
table << EOF
Dataset101_GLIrq1/nnUNetTrainer_save10/3
EOF
export RQ1_GPU_PAD_BASE=$T/pad
echo 33000 > "$T/pad.mib"
( while [[ ! -e $T/pad.start ]]; do sleep 0.2; done; sleep 2; echo 0 > "$T/pad.mib"; date +%s.%N > "$T/pad.released" ) &
padpid=$!
SLURM_JOB_ID=900006 HOME=$T/home timeout 60 bash "$HERE/dicc_worker.sh" > "$T/w2c.out" 2>&1
kill $padpid 2> /dev/null
check "显存填充放掉之后才开始训练" '[[ -f $T/pad.released ]] && awk -v r=$(cat $T/pad.released) "/^start/ {exit !(\$2 >= r)}" $T/train.log'
unset RQ1_GPU_PAD_BASE

# 3. 收到停止信号：停训练、交还领取（下一个作业能接着领）
: > "$T/train.log"; rm -rf "$nnUNet_results"
table << EOF
Dataset101_GLIrq1/nnUNetTrainer_save10/3
EOF
FAKE_SECS=60 SLURM_JOB_ID=900003 HOME=$T/home bash "$HERE/dicc_worker.sh" nnUNetTrainer_save10 3 101 > "$T/w3.out" 2>&1 &
wpid=$!
for _ in $(seq 20); do grep -q ^start "$T/train.log" 2> /dev/null && break; sleep 1; done
kill -TERM $wpid; wait $wpid; rc=$?
check "停止信号：退出 143、训练收到停止、领取交还" '[[ $rc == 143 ]] && grep -q ^term $T/train.log && [[ -z $(ls $TASKQ_DIR/claims) ]]'

# 4. 队列没有 lab 领着的折：box 替 lab 领的（ttl 24 小时、beat 30 分钟前）不被 DICC 作业领走
: > "$T/train.log"
table << EOF
Dataset101_GLIrq1/nnUNetTrainer_save10/4
EOF
python3 "$HERE/taskq.py" take --worker lab --ttl 86400 Dataset101_GLIrq1/nnUNetTrainer_save10/4 > /dev/null
python3 - "$TASKQ_DIR" << 'EOF'
import json, os, sys, time
p = os.path.join(sys.argv[1], "claims", os.listdir(os.path.join(sys.argv[1], "claims"))[0], "info")
i = json.load(open(p)); i["beat"] = time.time() - 1800; json.dump(i, open(p, "w"))
EOF
SLURM_JOB_ID=900004 HOME=$T/home timeout 60 bash "$HERE/dicc_worker.sh" nnUNetTrainer_save10 0 101 > "$T/w4.out" 2>&1
check "lab 领着的折（30 分钟没续，但在 24 小时内）：DICC 作业不领" '[[ ! -s $T/train.log ]] && grep -q "没有能领的任务" $T/w4.out'

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
