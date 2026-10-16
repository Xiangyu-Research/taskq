#!/bin/bash
# 测 train_task.sh（2026-10-16）：训满 N 个 epoch 后存盘停、别的机器在训时不训、训完退出。假的训练（每 0.1 秒一个 epoch，每 10 个 epoch 存一次盘）、假的 nvidia-smi。
#   bash scripts/queue/test_train_task.sh     约 1 分钟，不用 GPU
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/Dataset999_test"
printf '#!/bin/bash\n[[ $* == *compute-apps* ]] && exit 0; echo 0\n' > "$T/bin/nvidia-smi"; chmod +x "$T/bin/nvidia-smi"
OUT=$T/Dataset999_test/nnUNetTrainer_save10__nnUNetResEncUNetMPlans__3d_fullres/fold_0
cat > "$T/train.sh" << EOF2
#!/bin/bash
# 假训练：从上次的 epoch 接着训，到 \${FAKE_EPOCHS:-1000} 个训完
mkdir -p $OUT; L=$OUT/training_log_\$(date +%s%N).txt
echo "\$RQ1_DATASET" > $T/dataset
e=\$(cat $OUT/epoch 2> /dev/null || echo 0)
while (( e < \${FAKE_EPOCHS:-1000} )); do
    echo "x: Epoch \$e " >> \$L; e=\$((e + 1)); sleep 0.1
    (( e % 10 == 0 )) && { echo \$e > $OUT/epoch; touch $OUT/checkpoint_latest.pth; }
done
touch $OUT/checkpoint_final.pth; mkdir -p $OUT/validation; echo {} > $OUT/validation/summary.json
EOF2
export QUEUE_TEST_DIR=$T QUEUE_TEST_TRAIN=$T/train.sh QUEUE_TEST_POLL=0.2 QUEUE_TEST_SAVE_WAIT=0 CUDA_VISIBLE_DEVICES=0
fails=0
check() {  # check <说明> <期望退出码> <实际退出码> [<条件>]
    if [[ $3 == "$2" ]] && { [[ -z ${4:-} ]] || eval "$4"; }; then echo "通过：$1"; else echo "失败：$1（退出码 $3，期望 $2）"; fails=$((fails + 1)); fi
}
run() { bash "$HERE/train_task.sh" 999 0 "$@" > "$T/log" 2>&1; }

run --epochs 25; rc=$?
check "训满 25 个 epoch 后存盘停" 75 $rc '(( $(cat $OUT/epoch) >= 25 && $(cat $OUT/epoch) <= 40 ))'
check "数据集编号传给训练脚本（RQ1_DATASET）" 75 $rc '[[ $(cat $T/dataset) == 999 ]]'
export QUEUE_CLAIMS=$T/claims.txt
printf '# test\nDataset999_test/nnUNetTrainer_save10/0\tDICC\n' > "$QUEUE_CLAIMS"
run --epochs 5; check "别的机器在训或 Slurm 上排着这一折时不训（退出码 3）" 3 $? "grep -q 'DICC 上在训或排着' $T/log"
touch -d '3 hours ago' "$QUEUE_CLAIMS"
run --epochs 5; check "清单过期时照常训，日志里警告" 75 $? "grep -q 警告 $T/log"
rm -f "$QUEUE_CLAIMS"
FAKE_EPOCHS=200 run; check "训完退出 0" 0 $? "[[ -f $OUT/checkpoint_final.pth ]]"
run; check "已经训完的直接退出 0" 0 $? "grep -q 已经训完 $T/log"
(( fails == 0 )) && echo "全部通过" || { echo "$fails 项失败"; cat "$T/log"; exit 1; }
