# 任务队列（box、lab）

box 和 lab 上的训练、推理、分析都用 [pueue](https://github.com/Nukesor/pueue) 排队（2026-10-16 起）。DICC 仍用 Slurm。分工和规则见 [`admin/作业.md`](../../admin/作业.md)。

## 组

| 机器 | 组 | 同时跑 | 放什么 |
|---|---|---|---|
| box | `gpu` | 1 | 4070：推理 + 评测（`run_predict_eval.sh`）、P1 / P2 这类重推理 |
| box | `cpu` | 2 | 分析（`oof_eval.py`、`paired_compare.py`、`compare_all.py` 等） |
| box | `wait` | 不限 | 等 DICC / lab 某一折训完再拉回（`fetch_fold.sh`），推理用 `--after` 接在后面；lab 训的折拉回后归档（`archive_lab_fold.sh`） |
| lab | `gpu1` | 1 | 1 号卡常规训练（`train_task.sh`） |
| lab | `gpu0` | 1 | 0 号卡夜班（`train_task.sh --window night`），平时停着，由 `gpu0_gate.sh` 晚上打开 |
| lab | `cpu`、`wait` | 1、不限 | 备用 |

## 文件

| 文件 | 内容 |
|---|---|
| `q` | 加任务：`q <组> [pueue add 选项] -- '<命令>'`，按组设好 `CUDA_VISIBLE_DEVICES`，工作目录是仓库根目录 |
| `qs` | 查看两台机器的队列，每个任务一行（`qs box`、`qs lab` 只看一台） |
| `train_task.sh` | 训一折：卡被占时等；`--epochs N` 训满 N 个 epoch 后存盘停；`--window night / day` 按夜班 / 白天加跑的时间停。DICC 上排着或在训这一折时不训（退出码 3）。退出码 0 训完、75 按规则停了还没训完 |
| `gpu0_gate.sh` | lab 0 号卡开关（cron 每 10 分钟）：20:00–09:40 之间 0 号卡连续空 30 分钟才打开 `gpu0` 组；09:40 后停掉组 |
| `fetch_fold.sh` | 等远端某一折有 `checkpoint_final.pth` 和验证结果，再拉回 box；每小时记一行远端训到第几个 epoch（`qs` 显示在任务下面） |
| `archive_lab_fold.sh` | lab 训的折拉回后：备份到 DICC，box、DICC 两份都和 lab 核对一致后删 lab 上的存档和验证集预测，留 `ARCHIVED`；`--dry-run` 只列出 |
| `on_boot.sh` | 开机（`@reboot`）：启动 pueued，把重启时被打断的任务（连同因此"依赖失败"的下游）原地放回队列；lab 上启动 GPU 使用记录 |
| `setup.sh`、`install_pueue.sh` | 装 pueue、启动常驻进程、建组、写 crontab（两台 `on_boot.sh`；lab 加 `gpu0_gate.sh`、代码同步 `scripts/sync/lab_git_pull.sh`；box 加健康检查和登记表 / 看板更新 `scripts/monitor/auto_refresh.sh`） |
| `test_train_task.sh` | `train_task.sh` 停止规则的测试（假训练、假时钟，约 1 分钟） |

## 常用命令

```bash
Q=scripts/queue/q
# lab 1 号卡训一折（训练器不是 nnUNetTrainer_save10 时写在 fold 后面）
$Q gpu1 --label t15men_f0 -- 'bash scripts/queue/train_task.sh 111 0 nnUNetTrainer_save10_n470'
# lab 0 号卡夜班（数据增强进程按 6 个）
nnUNet_n_proc_DA=6 $Q gpu0 --label umamba_f0 -- 'bash scripts/queue/train_task.sh 101 0 nnUNetTrainer_UMambaEnc --window night'
# lab 0 号卡白天加跑 50 个 epoch：标签以 day 开头，加完后单独启动
nnUNet_n_proc_DA=6 $Q gpu0 --label day_umamba_f0 -- 'bash scripts/queue/train_task.sh 101 0 nnUNetTrainer_UMambaEnc --epochs 50 --window day'
~/.local/bin/pueue start <任务号>
# box：等 lab 训完 → 拉回 → 推理
f=$($Q wait -p --label fetch_x_f4 -- 'bash scripts/queue/fetch_fold.sh lab <results 下的模型文件夹> 4')
$Q gpu --after $f --label infer_x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh <模型文件夹的完整路径> 4'
# lab 训的折：拉回后归档（和推理并行）
$Q wait --after $f --label archive_x_f4 -- 'bash scripts/queue/archive_lab_fold.sh <results 下的模型文件夹> 4'
```

pueue 本身的命令：`pueue log <号>`（输出）、`pueue follow <号>`（实时看）、`pueue kill -s SIGTERM <号>`（停训练任务，存档每 10 个 epoch 一次）、`pueue restart --in-place <号>`（失败的任务原地重跑，排回原来的位置）、`pueue switch <号> <号>`（调顺序）、`pueue clean`（清掉已结束的）。

机器重启后，`on_boot.sh` 把被打断的任务原地放回队列（nnU-Net 从存档接着训，推理跳过已经做好的病例）；记在 `~/on_boot.log`，健康检查会推送一条。pueued 重启时，正在跑的任务会标成失败（退出码 137 / 143），等着它的任务立刻变成"依赖失败"，所以要连同下游一起放回（10/16 在单独的测试 pueued 里实测）。

出错提醒：box 上每 30 分钟的健康检查（`scripts/monitor/health.py`）看两台的队列，任务失败、因依赖失败没跑、训练卡住、拉回在等的折不在任何地方排队，都会推送到手机，并列在登记表最上面。
