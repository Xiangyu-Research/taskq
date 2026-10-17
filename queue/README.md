# 任务队列（pueue 机器）

box 和 lab 上的训练、推理、分析都用 [pueue](https://github.com/Nukesor/pueue) 排队（2026-10-16 起）。DICC 仍用 Slurm。分工和规则见 [`admin/作业.md`](../../admin/作业.md)。

各台机器的数据目录、conda、pueue 的组、角色、要查的盘都写在 [`envs/machines.py`](../../envs/machines.py)，这里的脚本、`scripts/sync/`、`scripts/monitor/health.py`、登记表都从那里读。加一台机器：在 `MACHINES` 里加一项；pueue 机器上再跑一次 `setup.sh`，box 上配好 ssh。

## 组

| 机器 | 组 | 同时跑 | 放什么 |
|---|---|---|---|
| box | `gpu` | 1 | 4070：推理 + 评测（`run_predict_eval.sh`）、P1 / P2 这类重推理 |
| box | `cpu` | 2 | 分析（`oof_eval.py`、`paired_compare.py`、`compare_all.py` 等） |
| box | `wait` | 不限 | 等别的机器上某一折训完再拉回（`fetch_fold.sh`），推理用 `--after` 接在后面；lab 训的折拉回后归档（`archive_fold.sh`） |
| lab | `gpu1` | 1 | 1 号卡训练（`train_task.sh`） |
| lab | `cpu`、`wait` | 1、不限 | 备用 |

## 文件

| 文件 | 内容 |
|---|---|
| `q` | 加任务：`q <组> [pueue add 选项] -- '<命令>'`，按 `envs/machines.py` 设好 `CUDA_VISIBLE_DEVICES` 和组的环境变量，工作目录是仓库根目录 |
| `qs` | 查看各 pueue 机器的队列，每个任务一行（`qs lab` 只看一台） |
| `train_task.sh` | 训一折：卡被占时等；`--epochs N` 训满 N 个 epoch 后存盘停；别的机器在训或 Slurm 上排着这一折时不训 |
| `fetch_fold.sh <机器> <模型文件夹> <fold>` | 等那台机器上这一折有 `checkpoint_final.pth` 和验证结果，再拉回 box；每小时记一行远端训到第几个 epoch（`qs` 显示在任务下面） |
| `archive_fold.sh <机器> <模型文件夹> <fold>` | 训练机上训完的折拉回后：备份到备份机器（DICC），box、DICC 两份都和训练机核对一致后删训练机上的存档和验证集预测，留 `ARCHIVED`；`--dry-run` 只列出 |
| `refill.py` | 训练机自动补任务：卡快空了就按档位从待办（DICC 上排着、从没开始过的折 + 这台机器上暂存的训练任务）取同一个实验的下一批，需要时先传数据集，box 上排好拉回、推理、归档；由健康检查每 30 分钟调用，`python3 scripts/queue/refill.py` 只看会补什么。规则见 `admin/作业.md` 的优先级一节 |
| `on_boot.sh` | 开机（`@reboot`）：启动 pueued，把重启时被打断的任务（连同因此"依赖失败"的下游）原地放回队列；表里 `gpu_log` 的机器启动 GPU 使用记录 |
| `setup.sh`、`install_pueue.sh` | 装 pueue、启动常驻进程、按表建组（表里没有的空组删掉）、写 crontab（所有 pueue 机器 `on_boot.sh`；角色 autopull 的代码同步 `scripts/sync/git_pull_cron.sh`；hub 的健康检查和登记表 / 看板更新 `scripts/monitor/auto_refresh.sh`） |
| `companion.py` | DICC 同卡附加训练：每个在训 nnU-Net 的作业同一张卡上再训一折待办里的 nnU-Net 折（最多 2 个训练，离时限不到 3 小时不附加），box 上排好拉回、推理；包装脚本意外死掉时删掉它的标记。由健康检查每 30 分钟调用，`python3 scripts/queue/companion.py` 只看会附加什么。规则见 `admin/作业.md` 的优先级一节 |
| `dicc_companion.sh` | 在 DICC 登录节点上跑的包装脚本：确认附加折自己的作业在排队 → `srun --overlap -J rq1c_<数组作业号>` 进宿主作业训（附加折自己的作业开始时由 `envs/nnunet_paths.sh` 停掉这个步骤） |
| `test_train_task.sh`、`test_refill.py` | `train_task.sh`、`refill.py` 的测试（假训练、假队列，不连任何机器） |

## 约定

几个脚本之间靠这些约定配合：

- **退出码**（`train_task.sh`）：0 训完；75 训满 `--epochs` 停了、还没训完（`on_boot.sh` 不放回，健康检查不报）；3 别的机器在训或 Slurm 上排着这一折；143 / Killed 手动停的（健康检查只记不推）；其他是出错。`train_folds.sbatch` 被清单拦下时也退出 3。
- **同一折不在两处训**：box 上的健康检查每 30 分钟给每台训练机（角色 train）写 `~/.cache/phd/claimed_elsewhere.txt`，每行 `key<TAB>机器`，内容是别的机器在训的折，加上别的 Slurm 集群上排着的折（DICC 的队列是整批训练的待办，要在 lab 训某一折，先取消 DICC 上那一折的作业）。`train_task.sh`、`train_folds.sbatch` 开训前读；清单超过 2 小时没更新时照常训，只警告。有训练机连不上时这一次不写。DICC 上 10/16 之前提交的作业读的是旧文件 `lab_active.txt`，健康检查同时写，这批作业跑完后去掉。
- **档位**：DICC 上是作业的 Nice，pueue 里是任务的优先级，等于 −Nice（`q gpu1 --priority=-200 …`）。暂存（stashed）的训练任务是这台机器上的待办，`refill.py` 按档位放行。
- **key** = `<数据集文件夹>/<训练器>/<fold>`，如 `Dataset111_MENrq1/nnUNetTrainer_save10_n470/0`；Slurm 作业名 `rq1_<标签>_f<fold>`，按 `make_registry.py` 的 `SPEC` 对上 key。
- **训完** = 有 `checkpoint_final.pth`（或归档后的 `ARCHIVED`）和 `validation/summary.json`。

## 常用命令

```bash
Q=scripts/queue/q
# lab 1 号卡训一折（训练器不是 nnUNetTrainer_save10 时写在 fold 后面）
$Q gpu1 --label t15men_f0 -- 'bash scripts/queue/train_task.sh 111 0 nnUNetTrainer_save10_n470'
~/.local/bin/pueue start <任务号>
# box：等 lab 训完 → 拉回 → 推理
f=$($Q wait -p --label fetch_x_f4 -- 'bash scripts/queue/fetch_fold.sh lab <results 下的模型文件夹> 4')
$Q gpu --after $f --label infer_x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh <模型文件夹的完整路径> 4'
# lab 训的折：拉回后归档（和推理并行）
$Q wait --after $f --label archive_x_f4 -- 'bash scripts/queue/archive_fold.sh lab <results 下的模型文件夹> 4'
```

pueue 本身的命令：`pueue log <号>`（输出）、`pueue follow <号>`（实时看）、`pueue kill -s SIGTERM <号>`（停训练任务，存档每 10 个 epoch 一次）、`pueue restart --in-place <号>`（失败的任务原地重跑，排回原来的位置）、`pueue switch <号> <号>`（调顺序）、`pueue clean`（清掉已结束的）。

机器重启后，`on_boot.sh` 把被打断的任务原地放回队列（nnU-Net 从存档接着训，推理跳过已经做好的病例）；记在 `~/on_boot.log`，健康检查会推送一条。pueued 重启时，正在跑的任务会标成失败（退出码 137 / 143），等着它的任务立刻变成"依赖失败"，所以要连同下游一起放回（10/16 在单独的测试 pueued 里实测）。

出错提醒：box 上每 30 分钟的健康检查（`scripts/monitor/health.py`）看各台机器的队列，任务失败、因依赖失败没跑、训练卡住、拉回在等的折不在那台机器上排队，都会推送到手机，并列在登记表最上面。
