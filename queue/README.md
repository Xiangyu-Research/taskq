# 任务队列（pueue 机器）

训练、拉回、推理、分析的调度（2026-10-18 的结构）。分工和规则见 [`admin/作业.md`](../../admin/作业.md)。

## 结构

```
 tasks.tsv（唯一的训练待办，档位 = nice 列）+ claims/（谁在训哪一折）        ← 都在 DICC 的 ~/.cache/phd/taskq/
     ↑ 领 / 续 / 交还（taskq.py）                     ↑ 领 / 续 / 交还（box 上的 refill.py 替 lab 做）
 DICC 作业（dicc_worker.sh）                          lab（领到后在 pueue gpu1 组里跑 train_task.sh）
                                ↓ 训完（看文件）
 box 按状态推进（advance.py，跟着健康检查每 30 分钟）：拉回 → 推理（gpu）→ 评测（cpu）→ 五折分析（cpu）
```

- **pueue** 在 box、lab 上只当运行器：按组限并发、留日志、断开 ssh 不中断。不用暂存、优先级、`--after`（10/18 起）。
- **开关**（都是文件，删掉 / 不建就是关）：DICC `~/.cache/phd/taskq/ENABLED`（DICC 作业按队列领任务；没有时照旧训作业名那一折）；box `~/.cache/phd/taskq/LAB`（替 lab 领任务；没有时只续 lab 已有的领取，其余只记"会做什么"）；box `~/.cache/phd/advance/LIVE`（按状态推进真的排任务；没有时只记"会排什么"）；box `~/.cache/phd/taskq/DICC`（DICC 排着的作业不够时自动补交 `worker.sbatch`；没有时只记"会交什么"）；DICC `~/.cache/phd/gpu_pad/DISABLED`（有这个文件时不填显存）。

## 出问题时看哪里

| 问题 | 看 |
|---|---|
| 训练：哪一折没人训、谁领了、失败了几次 | `python3 scripts/queue/taskq.py status`（box 上跑，经 ssh 读 DICC 的队列）；DICC 作业的 `.out`；失败记录在 DICC `~/.cache/phd/taskq/failures/`，lab 训不了而跳过的在 `skips/` |
| lab 领了什么、为什么没领；DICC 补交了哪些 worker 作业 | box `~/refill.log`；`python3 scripts/queue/refill.py`（只看会做什么） |
| 训完之后卡在哪一步 | box `~/advance.log`（每次有变化记一行：排了什么、会排什么、哪一步失败停住）；`python3 scripts/queue/advance.py`（只看会排什么）；再看 `pueue log <号>` |
| 总览、报警 | `research/RQ1/实验登记表.md` 最上面的"需要处理"（`scripts/monitor/health.py`） |

各台机器的数据目录、conda、pueue 的组、角色、要查的盘都写在 [`envs/machines.py`](../../envs/machines.py)，这里的脚本、`scripts/sync/`、`scripts/monitor/health.py`、登记表都从那里读。加一台机器：在 `MACHINES` 里加一项；pueue 机器上再跑一次 `setup.sh`，box 上配好 ssh。

## 组

| 机器 | 组 | 同时跑 | 放什么 |
|---|---|---|---|
| box | `gpu` | 1 | 4070：推理（`run_predict_eval.sh --stage infer`）、P1 / P2 这类重推理 |
| box | `cpu` | 2 | 评测（`run_predict_eval.sh --stage eval`）、五折集成评测、分析（`oof_eval.py`、`paired_compare.py`、`compare_all.py` 等） |
| box | `wait` | 不限 | 拉回训完的折（`fetch_fold.sh`）、lab 训的折归档（`archive_fold.sh`）、给 lab 传数据集（`tunnel_push.sh`） |
| lab | `gpu1` | 1 | 1 号卡训练（`train_task.sh`） |
| lab | `cpu`、`wait` | 1、不限 | 备用 |

## 文件

| 文件 | 内容 |
|---|---|
| `q` | 加任务：`q <组> [pueue add 选项] -- '<命令>'`，按 `envs/machines.py` 设好 `CUDA_VISIBLE_DEVICES` 和组的环境变量，工作目录是仓库根目录 |
| `qs` | 查看各 pueue 机器的队列，每个任务一行（`qs lab` 只看一台） |
| `train_task.sh` | 训一折：卡被占时等；`--epochs N` 训满 N 个 epoch 后存盘停。自动加的都是先在队列里领到才加（`refill.py`） |
| `fetch_fold.sh <机器> <模型文件夹> <fold>` | 等那台机器上这一折有 `checkpoint_final.pth` 和验证结果，再拉回 box；每小时记一行远端训到第几个 epoch（`qs` 显示在任务下面） |
| `archive_fold.sh <机器> <模型文件夹> <fold>` | 训练机上训完的折拉回后：备份到备份机器（DICC），box、DICC 两份都和训练机核对一致后删训练机上的存档和验证集预测，留 `ARCHIVED`；`--dry-run` 只列出 |
| `refill.py` | 替 lab 从任务队列领任务（`taskq.py take --worker lab --ttl 86400`）、续、交还（lab 训完的折等备份到 DICC 后才交还；没开始就失败的 `release --skip`）；卡空了领一折，数据集不在 lab 上先传。DICC 每个 QOS 排着和在跑的作业（数组按段算）不到上限、队列里还有没人领的任务时，补交 `worker.sbatch` 补满（QOS 和每段时限在 `envs/machines.py` 的 `qos`）。由健康检查每 30 分钟调用，`python3 scripts/queue/refill.py` 只看会做什么。规则见 `admin/作业.md` 的优先级一节 |
| `advance.py` | box 按状态推进：每一折缺哪一步、那一步没在跑就排（拉回、归档、推理、评测、五折分析）；失败过的不重排、报出来；由健康检查调用，`python3 scripts/queue/advance.py` 只看会排什么 |
| `on_boot.sh` | 开机（`@reboot`）：启动 pueued，把重启时被打断的任务原地放回队列；表里 `gpu_log` 的机器启动 GPU 使用记录 |
| `setup.sh`、`install_pueue.sh` | 装 pueue、启动常驻进程、按表建组（表里没有的空组删掉）、写 crontab（所有 pueue 机器 `on_boot.sh`；角色 autopull 的代码同步 `scripts/sync/git_pull_cron.sh`；hub 的健康检查和登记表 / 看板更新 `scripts/monitor/auto_refresh.sh`） |
| `taskq.py` | 训练的任务队列（在 DICC 的 `~/.cache/phd/taskq/`）：DICC 作业领任务（`claim`）、心跳（`beat`）、交还（`release`）；box 替 lab 领指定的一折（`take`）；加新实验（`add --nice <档> <key>…`）、改先后（`set-nice`）；`status` 看队列。规则见 `admin/作业.md` |
| `dicc_worker.sh` | DICC 训练作业开始后由 `envs/nnunet_paths.sh` 交给它（总开关在时）：什么任务都领；先训 1 个，最近一个开始 15 分钟后、近 10 分钟 GPU 使用率低于 80%、除去填充还空着一半显存时再加 1 个（最多 2 个），训完再领，领不到就退出。`envs/nnunet_paths.sh` 里的这段转接只是给 10/17 前交的作业用的兼容层，这批作业用完（约 2027-03）后删；新作业是 `research/RQ1/nnunet/worker.sbatch`（只负责领卡，同样什么都领；不限并发的数组，由 `refill.py` 自动补交） |
| `gpu_pad.py` | DICC GPU 作业显存填充：只占显存、不训练不推理，按 nvidia-smi 的 `memory.used` 维持 42GB、封顶 62GB；由 `envs/nnunet_paths.sh` 在 Slurm GPU 作业里默认启动，见下面"显存填充" |
| `test_train_task.sh`、`test_refill.py`、`test_taskq.py`、`test_advance.py`、`test_dicc_worker.sh` | 各脚本的测试（假训练、假队列、假快照，不连任何机器、不用 GPU）；`test_taskq.py` 里有"旧 worker 的调用方式"一组，改 `taskq.py` 后都要跑过 |
| `test_gpu_pad.py`、`test_gpu_pad_launcher.sh`、`test_gpu_pad_integration.sh` | `gpu_pad.py` 的假 backend、启动逻辑、box 真卡测试 |

## 显存填充

DICC 的 `gpu-a100` 要求作业显存占用 >= 40GB（10/17 收到）。`gpu_pad.py` 只占显存，不做训练/推理：按 nvidia-smi 的 `memory.used` 把每张卡维持在 **42000 MiB**、封顶 **62000 MiB**，给训练留至少 8GB；总显存 <60GB 时跳过（不是 80GB A100）。不分模型（10/18 改）：训练占的显存（`memory.used` 减去 pad 自己保留的）不到 2000 MiB 算没有训练，不填、已填的放掉；训练占的显存 120 秒内变化不超过 1 GiB 才补。要开始新训练时 `dicc_worker.sh` touch `/tmp/gpu_pad_<作业号>.start`，pad 下一轮（10 秒）全部放掉，worker 等 `/tmp/gpu_pad_<作业号>.mib`（pad 现在占多少）变成 0 再开训（最多等 30 秒）；新训练占上显存（比放掉前多出 2000 MiB；等的时候旧训练结束了，按降下来的值算）并稳定后再补，最多等 900 秒（`--start-hold`）。超过 62000 MiB 时一次算好要放几块、一起释放。

- **默认开启**：`envs/nnunet_paths.sh` 在所有 Slurm GPU 作业里启动它（不管作业名；不是 `SLURM_STEP_ID`、不是测试环境、有 `CUDA_VISIBLE_DEVICES`、有 `nvidia-smi`）。排队中的作业开训时会读这个文件，所以不用重新提交。
- **日志**：`$nnUNet_results/logs/gpu_pad_<作业号>.log`；文件名不含 `_seg`，不会被登记表当成训练段。
- **总开关**：DICC 上 `~/.cache/phd/gpu_pad/DISABLED`。存在时运行中的 pad 10 秒内释放退出，新作业不启动。
- **单个作业/脚本**：`RQ1_GPU_PAD=0` 关闭。设了 0 的脚本：启动前要求卡是空的 `research/RQ1/nnunet/smoke/run_gpu_smoke.sh`、`research/RQ1/nnunet/predict.sh`、`research/RQ1/analysis/run_predict_eval.sh`；自己做推理或分析的 `research/RQ1/probe/p1_predict.sh`、`research/RQ1/probe/run_p2.sh`、`research/RQ2/quickcheck/run_p4.sh`、`research/RQ1/analysis/run_sensitivity.sh`。CPU 分区的作业没有 `CUDA_VISIBLE_DEVICES`，本来就不启动。
- **supervisor**：pad 异常退出时最多重启 3 次（30 秒间隔）；父进程或 supervisor 退出、收到 SIGTERM、开关打开时释放显存退出。
- **测试**：`python3 scripts/queue/test_gpu_pad.py`（假 backend）、`bash scripts/queue/test_gpu_pad_launcher.sh`（假 Slurm）、`bash scripts/queue/test_gpu_pad_integration.sh`（box 真卡，用一个占 2GB 的假训练，目标降到 8GB；卡上有别的 CUDA 进程时跳过，测试中途出现别的进程就立刻停掉 pad、中止测试）。
- **退出码**（`gpu_pad.py`）：0 正常/开关/父进程退出；1 没有 GPU 或初始化失败；2 参数错误。启动失败只记日志，不影响训练。


## 约定

几个脚本之间靠这些约定配合：

- **退出码**（`train_task.sh`）：0 训完；75 训满 `--epochs` 停了、还没训完（`on_boot.sh` 不放回，健康检查不报）；143 / Killed 手动停的（健康检查只记不推）；其他是出错。
- **退出码**（`taskq.py claim`）：0 领到（输出 `key 数据集编号 训练器 fold`）；10 没有能领的；11 等排在前面刚停下的折；12 队列暂停（3 小时内失败 3 次）；其他（如没有 `tasks.tsv` 时的 2）是出错，worker 照出错处理。`take`：0 领到，10 领不到（队列里没有、已训完、别人领着）；box 上经 ssh 调用不通时是 ssh 的退出码。旧作业的调用方式（参数、退出码）不改，只加新命令。
- **同一折不在两处训**：先领到（`claims/<key>/info`，记 worker、host、since、beat、ttl）才训。`skips/<key>`：哪台 pueue 训练机跳过了这一折（每行 时刻、机器、原因），那台以后不领，删掉文件即撤销；`released/`：刚交还的折。领取在 `ttl` 秒内没有续（beat）就过期，别人可以领：DICC 作业 1200 秒、lab 86400 秒。10/16–10/18 用过的 `claimed_elsewhere.txt` 清单不再写（`taskq.py`、`train_folds.sbatch` 里读它的代码在文件超过 2 小时没更新时不起作用）。
- **任务表** `tasks.tsv` 固定 7 列 `order nice key dsid trainer fold kind`（列数不对的行会被跳过，所以不加列）；档位 = `nice`，同一档按 `order`。
- **pueue 任务名** = `<步骤>:<实验标签>_f<fold>`（如 `infer:t14met_f3`、`train:umamba_f1`）；`advance.py` 按命令认步骤，10/18 前手动排的任务也认得。
- **key** = `<数据集文件夹>/<训练器>/<fold>`，如 `Dataset111_MENrq1/nnUNetTrainer_save10_n470/0`；按折的 Slurm 作业名 `rq1_<标签>_f<fold>`，按 `make_registry.py` 的 `SPEC` 对上 key；不带折的 `rq1_worker` 没有 key。
- **训完** = 有 `checkpoint_final.pth`（或归档后的 `ARCHIVED`）和 `validation/summary.json`。

## 常用命令

```bash
Q=scripts/queue/q
TQ="python3 scripts/queue/taskq.py"
# 加新实验（第 ③ 档）、改先后
$TQ add --nice 100 Dataset113_PEDsrq1/nnUNetTrainer_save10/{0,1,2,3,4}
$TQ set-nice --nice 0 Dataset113_PEDsrq1/nnUNetTrainer_save10/2
# 手动在 lab 1 号卡训一折：先领（box 上，经 ssh 在 DICC 上跑），再加任务（训练器不是 nnUNetTrainer_save10 时写在 fold 后面）
ssh dicc 'cd ~/Projects/PhD && source envs/nnunet_paths.sh && python3 scripts/queue/taskq.py take --worker lab --ttl 86400 Dataset111_MENrq1/nnUNetTrainer_save10_n470/1'
$Q gpu1 --label train:t15men_f1 -- 'bash scripts/queue/train_task.sh 111 1 nnUNetTrainer_save10_n470'
# box 上单独推理 / 评测一折（平时由 advance.py 排）
$Q gpu --label infer:x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh --stage infer <模型文件夹的完整路径> 4'
$Q cpu --label eval:x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh --stage eval <模型文件夹的完整路径> 4'
```

pueue 本身的命令：`pueue log <号>`（输出）、`pueue follow <号>`（实时看）、`pueue kill -s SIGTERM <号>`（停训练任务，存档每 10 个 epoch 一次）、`pueue restart --in-place <号>`（失败的任务原地重跑，排回原来的位置）、`pueue switch <号> <号>`（调顺序）、`pueue clean`（清掉已结束的）。

机器重启后，`on_boot.sh` 把被打断的任务原地放回队列（nnU-Net 从存档接着训，推理跳过已经做好的病例）；记在 `~/on_boot.log`，健康检查会推送一条。pueued 重启时，正在跑的任务会标成失败（退出码 137 / 143）。

出错提醒：box 上每 30 分钟的健康检查（`scripts/monitor/health.py`）看各台机器的队列和任务队列，任务失败、训练卡住、lab 领的折停住、box 下游某一步失败停住，都会推送到手机，并列在登记表最上面。
