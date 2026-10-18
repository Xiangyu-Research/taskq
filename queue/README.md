# 任务队列（pueue 机器）

训练、拉回、推理、分析的调度（2026-10-18 的结构；同一天起任务不限于 nnU-Net）。分工和规则见 [`admin/作业.md`](../../admin/作业.md)。

调度只做"领任务 → 换环境 → 跑命令 → 看训完没 → 拉回 → 后续步骤"，不认识具体模型。每个任务要什么都写在三张表里（都是 Python 字典文件）：

| 表 | 内容 |
|---|---|
| [`envs/machines.py`](../../envs/machines.py) 每台机器的 `envs` | 环境名 → 怎么激活（`conda <名>`、`venv <路径>`）。`machines.py activate <环境>` 输出激活语句，这台机器没有就退出 3；没有某个环境的机器不领用它的任务 |
| [`recipes.py`](recipes.py) 做法 | 一种训练怎么跑：参数和默认值、`key`、`env`、`cmd`、`vars`、`out`、`done`、`data`、`fetch`、`archive` 等（字段说明在文件开头）。现在有 `nnunet`（命令在 `research/common/nnunet/train.sh`）和 `command`（全部由任务自己给） |
| `research/<RQ>/experiments.py` 实验表 | 每个实验用哪种做法、什么参数、几折、每折训完后做什么（`after`）、全部训完后做什么（`group_after`）、最后一起做的（`FINAL`）。`experiments.py`（这里的）读所有 RQ 的表并检查任务名不重复 |

`spec.py` 是唯一的解析器：做法 + 参数 → 任务说明（`python3 scripts/queue/spec.py resolve <做法> k=v…` 看完整说明，`spec.py lookup <key>` 按任务名查）。

## 结构

```
 tasks.jsonl（唯一的训练待办：做法 + 参数，档位 = nice）+ claims/（谁在训哪个任务）   ← 都在 DICC 的 ~/.cache/phd/taskq/
     ↑ 领 / 续 / 交还（taskq.py next）                ↑ 领 / 续 / 交还（box 上的 refill.py 替 lab 做）
 DICC 作业（worker.sbatch → dicc_worker.sh）          lab（领到后在 pueue gpu1 组里跑）
                 └──────── 都用 run_task.sh <做法> k=v… 跑：激活环境、设变量、跑命令、写分段日志 ────────┘
                                ↓ 训完（任务说明的 done）
 box 按状态推进（advance.py，跟着健康检查每 30 分钟）：拉回 → 归档 → 实验表里的步骤（RQ1：推理 → 评测 → 五折分析）
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
| box | `wait` | 不限 | 拉回训完的任务（`fetch_task.sh`）、lab 训的任务归档（`archive_task.sh`）、给 lab 传数据（`tunnel_push.sh`） |
| lab | `gpu1` | 1 | 1 号卡训练（`run_task.sh`） |
| lab | `cpu`、`wait` | 1、不限 | 备用 |

## 文件

| 文件 | 内容 |
|---|---|
| `q` | 加任务：`q <组> [pueue add 选项] -- '<命令>'`，按 `envs/machines.py` 设好 `CUDA_VISIBLE_DEVICES` 和组的环境变量，工作目录是仓库根目录 |
| `qs` | 查看各 pueue 机器的队列，每个任务一行（`qs lab` 只看一台） |
| `recipes.py`、`spec.py`、`experiments.py`、`steps.py` | 做法、解析器、读实验表、实验表里步骤的"做完没"判断（`runlog`、`check`） |
| `run_task.sh <做法> k=v…` | 所有机器共用的运行器：按任务说明激活环境、设 `vars`、在仓库根目录跑 `cmd`，输出写进分段日志；已经训完就直接退出；不在 Slurm 里、设了 `CUDA_VISIBLE_DEVICES` 时卡被占就等；Slurm 外出错隔 5 分钟重来，最多 3 次。SIGTERM 先停训练再退出 |
| `fetch_task.sh <机器> <key>` | 等那台机器上这个任务训完（`done`），再把 `out`（去掉 `fetch.exclude`，加上上一级的 `fetch.extra`）拉回 box；每小时记一行最新训练日志 |
| `archive_task.sh <机器> <key> [--dry-run]` | 训练机上训完、拉回以后：备份到备份机器（DICC），三份核对一致后删训练机上 `archive.drop` 匹配的文件，留 `ARCHIVED` |
| `fetch_fold.sh`、`archive_fold.sh` | 10/18 前的拉回、归档（按模型文件夹和 fold），box 上已排的旧任务用完后删 |
| `refill.py` | 替 lab 从任务队列领任务（`taskq.py take --worker lab --ttl 86400`）、续、交还（lab 训完的等备份到 DICC 后才交还；没开始就失败的 `release --skip`）；卡空了领一个 lab 有环境、盘放得下的任务，任务说明里的 `data` 不在 lab 上先传。DICC 每个 QOS 排着和在跑的作业（数组按段算）不到上限、队列里还有没人领的任务时，补交 `worker.sbatch` 补满（QOS 和每段时限在 `envs/machines.py` 的 `qos`）。由健康检查每 30 分钟调用，`python3 scripts/queue/refill.py` 只看会做什么。规则见 `admin/作业.md` 的优先级一节 |
| `advance.py` | box 按状态推进：每一折缺哪一步、那一步没在跑就排（拉回、归档、推理、评测、五折分析）；失败过的不重排、报出来；由健康检查调用，`python3 scripts/queue/advance.py` 只看会排什么 |
| `on_boot.sh` | 开机（`@reboot`）：启动 pueued，把重启时被打断的任务原地放回队列；表里 `gpu_log` 的机器启动 GPU 使用记录 |
| `setup.sh`、`install_pueue.sh` | 装 pueue、启动常驻进程、按表建组（表里没有的空组删掉）、写 crontab（所有 pueue 机器 `on_boot.sh`；角色 autopull 的代码同步 `scripts/sync/git_pull_cron.sh`；hub 的健康检查和登记表 / 看板更新 `scripts/monitor/auto_refresh.sh`） |
| `taskq.py` | 训练的任务队列（在 DICC 的 `~/.cache/phd/taskq/`）：DICC 作业领任务（`next`；10/19 前开始的作业用旧的 `claim`）、心跳（`beat`）、交还（`release`）；box 替 lab 领指定的一个（`take`）；加任务（`add --exp`、`add --recipe`、`add <key>…`）、改参数（`set`）、改先后（`set-nice`）、看说明（`show`）；`status` 看队列。命令的完整说明在文件开头，规则见 `admin/作业.md` |
| `worker.sbatch` | DICC 的训练作业（作业名 `train_worker`）：只负责领到一张卡，开始后交给 `dicc_worker.sh`；不限并发的数组，由 `refill.py` 自动补交 |
| `dicc_worker.sh` | 从队列领本机有环境的任务，用 `run_task.sh` 在后台跑；先训 1 个，最近一个开始 15 分钟后、近 10 分钟 GPU 使用率低于 80%、除去填充还空着一半显存时再加 1 个（最多 2 个），训完再领，领不到就退出；快到时限时把没训完的交还。10/17 前交的按折作业由 `envs/nnunet_paths.sh` 转过来（兼容层，这批作业用完约 2027-03 后删） |
| `gpu_pad.py`、`gpu_pad_start.sh` | DICC GPU 作业显存填充：只占显存、不训练不推理，按 nvidia-smi 的 `memory.used` 维持 42GB、封顶 62GB；`gpu_pad_start.sh` 在 Slurm GPU 作业里启动它（`worker.sbatch` 和 `envs/nnunet_paths.sh` 都调用），见下面"显存填充" |
| `test_spec.py`、`test_run_task.sh`、`test_refill.py`、`test_taskq.py`、`test_advance.py`、`test_dicc_worker.sh` | 各脚本的测试（假训练、假队列、假快照，不连任何机器、不用 GPU）；`test_taskq.py` 里有"旧 worker 的调用方式"一组，改 `taskq.py` 后都要跑过 |
| `test_gpu_pad.py`、`test_gpu_pad_launcher.sh`、`test_gpu_pad_integration.sh` | `gpu_pad.py` 的假 backend、启动逻辑、box 真卡测试 |

## 显存填充

DICC 的 `gpu-a100` 要求作业显存占用 >= 40GB（10/17 收到）。`gpu_pad.py` 只占显存，不做训练/推理：按 nvidia-smi 的 `memory.used` 把每张卡维持在 **42000 MiB**、封顶 **62000 MiB**，给训练留至少 8GB；总显存 <60GB 时跳过（不是 80GB A100）。不分模型（10/18 改）：训练占的显存（`memory.used` 减去 pad 自己保留的）不到 2000 MiB 算没有训练，不填、已填的放掉；训练占的显存 120 秒内变化不超过 1 GiB 才补。要开始新训练时 `dicc_worker.sh` touch `/tmp/gpu_pad_<作业号>.start`，pad 下一轮（10 秒）全部放掉，worker 等 `/tmp/gpu_pad_<作业号>.mib`（pad 现在占多少）变成 0 再开训（最多等 30 秒）；新训练占上显存（比放掉前多出 2000 MiB；等的时候旧训练结束了，按降下来的值算）并稳定后再补，最多等 900 秒（`--start-hold`）。超过 62000 MiB 时一次算好要放几块、一起释放。

- **默认开启**：`gpu_pad_start.sh`（`worker.sbatch` 直接调用，其他作业经 `envs/nnunet_paths.sh`）在所有 Slurm GPU 作业里启动它（不管作业名；不是 `SLURM_STEP_ID`、不是测试环境、有 `CUDA_VISIBLE_DEVICES`、有 `nvidia-smi`）。排队中的作业开训时会读这个文件，所以不用重新提交。
- **日志**：`<数据目录>/logs/workers/gpu_pad_<作业号>.log`（10/18 前在 `nnUNet/results/logs/`）。
- **总开关**：DICC 上 `~/.cache/phd/gpu_pad/DISABLED`。存在时运行中的 pad 10 秒内释放退出，新作业不启动。
- **单个作业/脚本**：`RQ1_GPU_PAD=0` 关闭。设了 0 的脚本：启动前要求卡是空的 `research/RQ1/nnunet/smoke/run_gpu_smoke.sh`、`research/RQ1/nnunet/predict.sh`、`research/RQ1/analysis/run_predict_eval.sh`；自己做推理或分析的 `research/RQ1/probe/p1_predict.sh`、`research/RQ1/probe/run_p2.sh`、`research/RQ2/quickcheck/run_p4.sh`、`research/RQ1/analysis/run_sensitivity.sh`。CPU 分区的作业没有 `CUDA_VISIBLE_DEVICES`，本来就不启动。
- **supervisor**：pad 异常退出时最多重启 3 次（30 秒间隔）；父进程或 supervisor 退出、收到 SIGTERM、开关打开时释放显存退出。
- **测试**：`python3 scripts/queue/test_gpu_pad.py`（假 backend）、`bash scripts/queue/test_gpu_pad_launcher.sh`（假 Slurm）、`bash scripts/queue/test_gpu_pad_integration.sh`（box 真卡，用一个占 2GB 的假训练，目标降到 8GB；卡上有别的 CUDA 进程时跳过，测试中途出现别的进程就立刻停掉 pad、中止测试）。
- **退出码**（`gpu_pad.py`）：0 正常/开关/父进程退出；1 没有 GPU 或初始化失败；2 参数错误。启动失败只记日志，不影响训练。


## 约定

几个脚本之间靠这些约定配合：

- **退出码**（`run_task.sh`）：0 训完；3 命令正常结束但没训完（worker 记成失败）；2 任务说明或环境有问题（参数写错、这台机器没有这个环境）；143 被停掉（健康检查只记不推）；其他是训练出错。10/18 前的 `train_task.sh` 用过 75（训满 `--epochs` 停了），`on_boot.sh`、健康检查仍认。
- **退出码**（`taskq.py next` / `claim`）：0 领到（`next` 输出两行：key、`run_task.sh` 的参数；旧的 `claim` 输出 `key 数据集编号 训练器 fold`）；10 没有能领的；11 等排在前面刚停下的；12 队列暂停（3 小时内失败 3 次）；其他（如没有任务表时的 2）是出错，worker 照出错处理。`take`：0 领到，10 领不到（队列里没有、已训完、别人领着）；box 上经 ssh 调用不通时是 ssh 的退出码。旧作业的调用方式（参数、退出码）不改，只加新命令。
- **同一个任务不在两处训**：先领到（`claims/<key>/info`，记 worker、host、since、beat、ttl）才训。`skips/<key>`：哪台 pueue 训练机跳过了这一折（每行 时刻、机器、原因），那台以后不领，删掉文件即撤销；`released/`：刚交还的折。领取在 `ttl` 秒内没有续（beat）就过期，别人可以领：DICC 作业 1200 秒、lab 86400 秒。10/16–10/18 用过的 `claimed_elsewhere.txt` 清单不再写（`taskq.py`、`train_folds.sbatch` 里读它的代码在文件超过 2 小时没更新时不起作用）。
- **任务表** `tasks.jsonl` 每行一个任务 `{order, nice, key, exp, recipe, params}`；档位 = `nice`，同一档按 `order`。10/18 前的 `tasks.tsv`（7 列）第一次读时自动转过来，原文件留成 `tasks.tsv.bak`。
- **pueue 任务名**：训练 `train:<key>`；`advance.py` 排的每折 `<步骤>:<实验 id>_f<fold>`（如 `infer:t14met_f3`），实验一级 `<步骤>:<实验 id>`；`advance.py` 按命令或任务名认步骤，10/18 前排的任务也认得。
- **key** 由做法给，nnU-Net 是 `<数据集文件夹>/<训练器>/<fold>`（如 `Dataset111_MENrq1/nnUNetTrainer_save10_n470/0`）。10/17 前按折的 Slurm 作业名 `rq1_<实验 id>_f<fold>`；不带折的 `train_worker`（10/18 前 `rq1_worker`）没有 key。
- **训完** = 任务说明的 `done` 都在（nnU-Net：`validation/summary.json`，和 `checkpoint_final.pth` 或归档后的 `ARCHIVED`）。
- **分段日志**：`<数据目录>/logs/tasks/<key 的文件名>__<作业>_seg<段>.log`，第一行 `作业 <作业>_<段> @ <机器>，任务=<key>，第 <段> 段，环境 <环境>，代码 <版本>，<时间>`；作业 = Slurm 作业号或"主机名-时间"。10/18 前 nnU-Net 的在 `nnUNet/results/logs/`，登记表两种都认。worker 自己的日志（GPU 记录、显存填充）在 `<数据目录>/logs/workers/`。

## 常用命令

```bash
Q=scripts/queue/q
TQ="python3 scripts/queue/taskq.py"
# 加实验表里的实验（第 ③ 档）、只加某几折、改先后、看一个任务会怎么跑
$TQ add --nice 100 --exp t15met
$TQ add --nice 0 --exp t15men --folds 1 2 3 4
$TQ set-nice --nice 0 Dataset116_METrq1/nnUNetTrainer_save10_n470/2
$TQ show Dataset116_METrq1/nnUNetTrainer_save10_n470/2
# 改一个任务（换环境、换训练器以外的参数；会改变 key 的参数不能改，删掉重加）
$TQ set Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/1 env=umamba
# 手动在 lab 1 号卡训一个：先领（box 上跑，自动经 ssh 转到 DICC），再加任务
$TQ take --worker lab --ttl 86400 Dataset111_MENrq1/nnUNetTrainer_save10_n470/1
$Q gpu1 --label train:Dataset111_MENrq1/nnUNetTrainer_save10_n470/1 -- \
    'bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=1'
# box 上单独推理 / 评测一折（平时由 advance.py 排）
$Q gpu --label infer:x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh --stage infer <模型文件夹的完整路径> 4'
$Q cpu --label eval:x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh --stage eval <模型文件夹的完整路径> 4'
```

## 加新东西

- **新的 nnU-Net 变体**：在 `research/RQ1/nnunet/trainers/`（约 10/21 起 `research/common/nnunet/trainers/`）写训练器 → 实验表加一行（`recipe="nnunet"`，`params` 里写 `dataset`、`trainer`，要别的环境时写 `env`）→ `taskq.py add --exp <id>`。
- **单个任务换环境、换参数**：`taskq.py set <key> k=v`。
- **新环境**：各台机器装好 → `envs/machines.py` 里那几台的 `envs` 加一项。没装的机器自然不领用它的任务。
- **不是 nnU-Net 的训练**：
  - 实验表里用 `recipe="command"`，`params` 里直接写 `key`、`cmd`、`env`、`out`、`done`（还可以写 `data`、`log`、`vars` 等，见 `recipes.py`）；
  - 同一类训练多了，在 `recipes.py` 加一种做法，参数只写不同的部分。
  - 命令要能"重跑就接着训"：到时限、被停掉以后，下一个领到的作业用同一条命令接着训。
- **实验表以外的临时任务**：`taskq.py add --nice <档> --recipe <做法> k=v…`（只拉回、归档，没有后续步骤）。

pueue 本身的命令：`pueue log <号>`（输出）、`pueue follow <号>`（实时看）、`pueue kill -s SIGTERM <号>`（停训练任务，存档每 10 个 epoch 一次）、`pueue restart --in-place <号>`（失败的任务原地重跑，排回原来的位置）、`pueue switch <号> <号>`（调顺序）、`pueue clean`（清掉已结束的）。

机器重启后，`on_boot.sh` 把被打断的任务原地放回队列（nnU-Net 从存档接着训，推理跳过已经做好的病例）；记在 `~/on_boot.log`，健康检查会推送一条。pueued 重启时，正在跑的任务会标成失败（退出码 137 / 143）。

出错提醒：box 上每 30 分钟的健康检查（`scripts/monitor/health.py`）看各台机器的队列和任务队列，任务失败、训练卡住、lab 领的折停住、box 下游某一步失败停住，都会推送到手机，并列在登记表最上面。
