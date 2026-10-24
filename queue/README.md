# 任务队列（pueue 机器）

训练、拉回、推理、分析的调度（2026-10-18 的结构；同一天起任务不限于 nnU-Net）。分工和规则见 [`admin/作业.md`](../../admin/作业.md)。

调度只做"领任务 → 换环境 → 跑命令 → 看训完没 → 拉回 → 后续步骤"，不认识具体模型。每个任务要什么都写在三张表里（都是 Python 字典文件）：

| 表 | 内容 |
|---|---|
| [`envs/machines.py`](../../envs/machines.py) 每台机器的 `envs` | 环境名 → 怎么激活（`conda <名>`、`venv <路径>`）。`machines.py activate <环境>` 输出激活语句，这台机器没有就退出 3；没有某个环境的机器不领用它的任务 |
| [`recipes.py`](recipes.py) 做法 | 一种训练怎么跑：参数和默认值、`key`、`env`、`cmd`、`vars`、`out`、`done`、`data`、`fetch`、`archive` 等（字段说明在文件开头）。现在有 `nnunet`（命令在 `research/common/nnunet/train.sh`）和 `command`（全部由任务自己给） |
| `research/<RQ>/experiments.py` 实验表 | 每个实验的一切：用哪种做法、什么参数、几折、每折训完后做什么（`after`）、全部训完后做什么（`group_after`）、最后一起做的（`FINAL`）、默认档位（`nice`）、分析脚本要的声明（`names`，如 RQ1 的 `skip_ext`、`splits`）、看板名称（`board`）。`experiments.py`（这里的）读所有 RQ 的表并检查任务名不重复；分析脚本按模型文件夹查所属实验的声明（`experiments.py name-of <模型文件夹> <名字>…`，表里没有的模型报错），不按数据集名字或实验编号判断（10/24 起） |

`spec.py` 是唯一的解析器：做法 + 参数 → 任务说明（`python3 scripts/queue/spec.py resolve <做法> k=v…` 看完整说明，`spec.py lookup <key>` 按任务名查）。

## 结构

```
 tasks.jsonl（唯一的训练待办：做法 + 参数，档位 = nice）+ claims/（谁在训哪个任务）   ← 都在 DICC 的 ~/.cache/phd/taskq/
     ↑ 领 / 续 / 交还（taskq.py next）                ↑ 领 / 续 / 交还（box 上的 refill.py 替 lab 做）
 DICC 作业（worker.sbatch → worker_start.sh → dicc_worker.sh） lab（领到后在 pueue gpu1 组里跑）
                 └──────── 都用 run_task.sh <做法> k=v… 跑：激活环境、设变量、跑命令、写分段日志 ────────┘
                                ↓ 训完（任务说明的 done）
 box 按状态推进（advance.py，跟着健康检查每 30 分钟）：拉回 → 归档 → 实验表里的步骤（RQ1：推理 → 评测 → 五折分析）
     └─ OFFLOAD 对上的推理（10/22 起暂不启用）：传输入（offload_io.sh push）→ lab gpu0 推理 → 拉回、核对（offload_io.sh pull）→ box 核对后删 lab 那份
```

- **pueue** 在 box、lab 上只当运行器：按组限并发、留日志、断开 ssh 不中断。不用暂存、优先级、`--after`（10/18 起）。
- **开关**（都是文件，删掉 / 不建就是关）：DICC `~/.cache/phd/taskq/ENABLED`（DICC 作业按队列领任务；没有时照旧训作业名那一折）；box `~/.cache/phd/taskq/LAB`（替 lab 领任务；没有时只续 lab 已有的领取，其余只记"会做什么"）；box `~/.cache/phd/advance/LIVE`（按状态推进真的排任务；没有时只记"会排什么"）；box `~/.cache/phd/advance/HOLD`（每行一个标签的通配式，如 `infer:umamba_f*`：对上的步骤照常排，但加成暂停的，要跑时 `pueue enqueue <任务号>`）；box `~/.cache/phd/advance/OFFLOAD`（每行一个标签的通配式：对上的每折步骤分到 `envs/machines.py` 里有 `offload` 组的机器（lab `gpu0`）跑；行尾加 `idle` 的只在那个组空着时分过去，box 上排着没开始的一次挪一个；功能有，10/22 起暂不启用：lab 0 号卡有别人在用，规则都注释掉了）；box `~/.cache/phd/taskq/DICC`（DICC 排着的作业不够时自动补交 `worker.sbatch`；没有时只记"会交什么"）；DICC `~/.cache/phd/gpu_pad/DISABLED`（有这个文件时不填显存）。

## 出问题时看哪里

| 问题 | 看 |
|---|---|
| 训练：哪一折没人训、谁领了、失败了几次 | `python3 scripts/queue/taskq.py status`（box 上跑，经 ssh 读 DICC 的队列）；DICC 作业的 `.out`；失败记录在 DICC `~/.cache/phd/taskq/failures/`，lab 训不了而跳过的在 `skips/` |
| lab 领了什么、为什么没领；DICC 补交了哪些 worker 作业 | box `~/refill.log`；`python3 scripts/queue/refill.py`（只看会做什么） |
| 训完之后卡在哪一步 | box `~/advance.log`（每次有变化记一行：排了什么、会排什么、哪一步失败停住）；`python3 scripts/queue/advance.py`（只看会排什么）；再看 `pueue log <号>` |
| 分到 lab 的推理走到哪 | box `~/advance.log`；box 上 `push:<标签>`、`pull:<标签>` 任务的 `pueue log`；lab 上同标签的任务（`ssh lab pueue log <号>`）；分出去还没清理的记在 box `~/.cache/phd/advance/offload.json`（label → 机器、结果文件夹、要用的输入、传过去的模型文件） |
| 总览、报警 | `research/RQ1/实验登记表.md` 最上面的"需要处理"（`scripts/monitor/health.py`） |

各台机器的数据目录、conda、pueue 的组、角色、要查的盘都写在 [`envs/machines.py`](../../envs/machines.py)，这里的脚本、`scripts/sync/`、`scripts/monitor/health.py`、登记表都从那里读。加一台机器：在 `MACHINES` 里加一项；pueue 机器上再跑一次 `setup.sh`，box 上配好 ssh。

## 组

| 机器 | 组 | 同时跑 | 放什么 |
|---|---|---|---|
| box | `gpu` | 1 | 4070：推理（`run_predict_eval.sh --stage infer`）、P1 / P2 这类重推理 |
| box | `cpu` | 2 | 评测（`run_predict_eval.sh --stage eval`）、五折集成评测、分析（`oof_eval.py`、`paired_compare.py`、`compare_all.py` 等） |
| box | `wait` | 不限 | 拉回训完的任务（`fetch_task.sh`）、lab 训的任务归档（`archive_task.sh`）、给 lab 传数据（`tunnel_push.sh`） |
| lab | `gpu1` | 1 | 1 号卡训练（`run_task.sh`） |
| lab | `gpu0` | 1 | 0 号卡：手动加的临时任务（10/22 起，`refill.py` 不往里领训练）；box 分推理过来的功能有，10/22 起暂不启用（卡上有别人时推理先等，最多 24 小时） |
| lab | `cpu`、`wait` | 1、不限 | 备用 |

## 文件

| 文件 | 内容 |
|---|---|
| `q` | 加任务：`q <组> [pueue add 选项] -- '<命令>'`，按 `envs/machines.py` 设好 `CUDA_VISIBLE_DEVICES` 和组的环境变量，工作目录是仓库根目录 |
| `qs` | 查看各 pueue 机器的队列，每个任务一行（`qs lab` 只看一台） |
| `recipes.py`、`spec.py`、`experiments.py`、`steps.py` | 做法、解析器、读实验表、实验表里步骤的"做完没"判断（`runlog`、`check`） |
| `run_analysis.sh <命令…>` | 在 pueue 的 `cpu` 组跑一个分析：进仓库根目录、source `envs/nnunet_paths.sh`、exec 给定命令；stdout/stderr 由 pueue 收（2026-10-20 加，M3/R2/A4 用） |
| `run_task.sh <做法> k=v…` | 所有机器共用的运行器：按任务说明激活环境、设 `vars`、在仓库根目录跑 `cmd`，输出写进分段日志；已经训完就直接退出；不在 Slurm 里、设了 `CUDA_VISIBLE_DEVICES` 时卡被占就等；Slurm 外出错隔 5 分钟重来，最多 3 次。SIGTERM 先停训练再退出 |
| `fetch_task.sh <机器> <key>` | 等那台机器上这个任务训完（`done`），再把 `out`（去掉 `fetch.exclude`，加上上一级的 `fetch.extra`）拉回 box；每小时记一行最新训练日志 |
| `archive_task.sh <机器> <key> [--dry-run]` | 训练机上训完、拉回以后：备份到备份机器（DICC），三份核对一致后删训练机上 `archive.drop` 匹配的文件，留 `ARCHIVED` |
| `refill.py` | 替 lab 从任务队列领任务（`taskq.py take --worker lab --ttl 86400`）、续、交还（lab 训完的等备份到 DICC 后才交还；没开始就失败的 `release --skip`）；lab 每个有卡、开着的组（`envs/machines.py` 的 `manual` 组除外，即 lab 的 `gpu0`）空了各领一个 lab 有环境、盘放得下的任务（10/19 起），任务说明里的 `data` 不在 lab 上先传。DICC 每个 QOS 排着和在跑的作业（数组按段算）不到上限减去留给别的作业的名额（`envs/machines.py` 的 `qos`，DICC 的 long 留 2 个）、队列里还有没人领的任务时，补交 `worker.sbatch` 补满，每个空位一个普通作业、不用数组（10/21 起；QOS 和每段时限在 `envs/machines.py` 的 `qos`）。由健康检查每 30 分钟调用，`python3 scripts/queue/refill.py` 只看会做什么。规则见 `admin/作业.md` 的优先级一节 |
| `advance.py` | box 按状态推进：每一折缺哪一步、那一步没在跑就排（拉回、归档、推理、评测、五折分析）；带 `queue` 的步骤（每折的推理、五折训完后的 P1 / P2 重跑）放进 DICC 的任务队列，那边缺的文件先传，拉回后接着做下一步；失败过的不重排、报出来；`OFFLOAD` 对上的步骤分到 lab 跑（缺的输入先传，跑完拉回，box 核对后删 lab 那份）；由健康检查调用，`python3 scripts/queue/advance.py` 只看会排什么 |
| `offload_io.sh` | 分到别的机器的步骤的传输：`push <机器> <路径>…`（文件夹经隧道 `tunnel_push.sh`，文件用 rsync over ssh）、`pull <机器> <文件夹>`（rsync 拉回 box，再按校验和核对）；由 `advance.py` 放进 box 的 `wait` 组 |
| `on_boot.sh` | 开机（`@reboot`）：启动 pueued，把重启时被打断的任务原地放回队列；表里 `gpu_log` 的机器启动 GPU 使用记录 |
| `setup.sh`、`install_pueue.sh` | 装 pueue、启动常驻进程、按表建组（表里没有的空组删掉）、写 crontab（所有 pueue 机器 `on_boot.sh`；角色 autopull 的代码同步 `scripts/sync/git_pull_cron.sh`；hub 的健康检查和登记表 / 看板更新 `scripts/monitor/auto_refresh.sh`） |
| `taskq.py` | 训练的任务队列（在 DICC 的 `~/.cache/phd/taskq/`）：DICC 作业领任务（`next`；10/19 前开始的作业用旧的 `claim`）、心跳（`beat`）、交还（`release`）；box 替 lab 领指定的一个（`take`）；加任务（`add --exp`、`add --recipe`、`add <key>…`）、改参数（`set`）、改先后（`set-nice`）、看说明（`show`）；`status` 看队列。命令的完整说明在文件开头，规则见 `admin/作业.md` |
| `worker.sbatch`、`worker_start.sh` | DICC 的训练作业（作业名 `train_worker`）：只负责领到一张卡。`worker.sbatch` 提交时被 Slurm 复制，只有资源申请和一行转交（10/19 起）；开始后从仓库读 `worker_start.sh`：查总开关 `ENABLED`、激活 conda、启动显存填充，交给 `dicc_worker.sh`；普通作业（10/21 起不用数组），由 `refill.py` 自动补交，输出 `train_worker_<作业号>.out`。手动交：`cd scripts/queue && sbatch worker.sbatch`，要几个交几次（long、每个 3 天，long 的上限） |
| `dicc_worker.sh` | 从队列领本机有环境的任务，用 `run_task.sh` 在后台跑；先训 1 个，最近一个开始 15 分钟后、近 10 分钟 GPU 使用率低于 90%、除去填充还空着一半显存时再加 1 个（最多 2 个），训完再领，领不到就退出；快到时限时把没训完的交还 |
| `gpu_pad.py`、`gpu_pad_start.sh` | DICC GPU 作业显存填充：只占显存、不训练不推理，按 nvidia-smi 的 `memory.used` 保底 42GB、训练稳定后补到 55GB、封顶 62GB；`gpu_pad_start.sh` 在 Slurm GPU 作业里启动它（`worker_start.sh` 和 `envs/nnunet_paths.sh` 都调用），见下面"显存填充" |
| `test_spec.py`、`test_run_task.sh`、`test_refill.py`、`test_taskq.py`、`test_advance.py`、`test_dicc_worker.sh` | 各脚本的测试（假训练、假队列、假快照，不连任何机器、不用 GPU）；`test_taskq.py` 里有"worker 的调用方式"一组，改 `taskq.py` 后都要跑过 |
| `test_gpu_pad.py`、`test_gpu_pad_launcher.sh`、`test_gpu_pad_integration.sh` | `gpu_pad.py` 的假 backend、启动逻辑、box 真卡测试 |

## 显存填充

DICC 的 `gpu-a100` 要求作业显存占用 >= 40GB（10/17 收到）。`gpu_pad.py` 只占显存，不做训练/推理，按 nvidia-smi 的 `memory.used` 分三档（10/21 起）：**floor 42000 MiB**（第一次看到训练以后，任何时候低于它都马上补，不等稳定）、**target 55000 MiB**（训练显存稳定后补到这里）、**high 62000 MiB**（超过就放掉多出的部分）。每次补、放都是整块 1GB（补完会多出不到 1GB），给训练留至少 8GB；总显存 <60GB 时跳过（不是 80GB A100）。不分模型（10/18 改）：训练占的显存（`memory.used` 减去 pad 自己保留的）不到 2000 MiB 算没有训练：作业开始后还没训练过时不填；训练过、现在没有在训的（一折训完、下一折还没起来）保持在 floor。训练占的显存 120 秒内变化不超过 1 GiB 才从 floor 补到 target。要开始新训练时 `dicc_worker.sh` 删掉 `/tmp/gpu_pad_<作业号>.ready`、touch `/tmp/gpu_pad_<作业号>.start`，pad 下一轮（10 秒）只放到 floor（不低于它，给新训练留约 39GB），写 `.ready`，worker 看到 `.ready`（或旧版 pad 的 `.mib` 变成 0）再开训（最多等 30 秒）；新训练占上显存（比放之前多出 2000 MiB；等的时候旧训练结束了，按降下来的值算）并稳定后再补到 target，最多等 900 秒（`--start-hold`）。10/21 前是只有 target 42000、开始新训练时全部放掉，换折时会有几分钟低于 40GB。

- **默认开启**：`gpu_pad_start.sh`（`worker_start.sh` 直接调用，其他作业经 `envs/nnunet_paths.sh`）在所有 Slurm GPU 作业里启动它（不管作业名；不是 `SLURM_STEP_ID`、不是测试环境、有 `CUDA_VISIBLE_DEVICES`、有 `nvidia-smi`）。排队中的作业开训时会读这个文件，所以不用重新提交。
- **日志**：`<数据目录>/logs/workers/gpu_pad_<作业号>.log`（10/18 前在 `nnUNet/results/logs/`）。
- **总开关**：DICC 上 `~/.cache/phd/gpu_pad/DISABLED`。存在时运行中的 pad 10 秒内释放退出，新作业不启动。
- **单个作业/脚本**：`RQ1_GPU_PAD=0` 关闭。设了 0 的脚本：启动前要求卡是空的 `research/common/nnunet/smoke/run_gpu_smoke.sh`、`research/RQ1/nnunet/predict.sh`、`research/RQ1/analysis/run_predict_eval.sh`；自己做推理或分析的 `research/RQ1/probe/p1_predict.sh`、`research/RQ1/probe/run_p2.sh`、`research/RQ2/quickcheck/run_p4.sh`、`research/RQ1/analysis/run_sensitivity.sh`、`research/RQ1/external/run_t19.sh`、`research/RQ1/external/t19_eval.sh`、`research/RQ1/probe/p1_predict_model.sh`、`research/RQ1/probe/p2_predict_model.sh`。CPU 分区的作业没有 `CUDA_VISIBLE_DEVICES`，本来就不启动。
- **supervisor**：pad 异常退出时最多重启 3 次（30 秒间隔）；父进程或 supervisor 退出、收到 SIGTERM、开关打开时释放显存退出。
- **测试**：`python3 scripts/queue/test_gpu_pad.py`（假 backend）、`bash scripts/queue/test_gpu_pad_launcher.sh`（假 Slurm）、`bash scripts/queue/test_gpu_pad_integration.sh`（box 真卡，用一个占 2GB 的假训练，目标降到 8GB；卡上有别的 CUDA 进程时跳过，测试中途出现别的进程就立刻停掉 pad、中止测试）。
- **退出码**（`gpu_pad.py`）：0 正常/开关/父进程退出；1 没有 GPU 或初始化失败；2 参数错误。启动失败只记日志，不影响训练。


## 约定

几个脚本之间靠这些约定配合：

- **DICC 作业提交时定下的**只有 `worker.sbatch`（资源申请和转交那一行，10/19 起）；其余脚本在作业开始时（`worker_start.sh`、`dicc_worker.sh`、显存填充）或每次调用时（`taskq.py`、`run_task.sh`、做法表、实验表、`envs/machines.py`）从仓库读。排着的作业依赖的只有 `~/.cache/phd/taskq/ENABLED` 这个文件和 `scripts/queue/worker_start.sh` 这个路径；在跑的作业（最长 3 天）一直用开始时读到的 `dicc_worker.sh`，它和 `taskq.py`、`run_task.sh` 之间的参数、退出码在这段时间内要兼容。
- **退出码**（`run_task.sh`）：0 训完；3 命令正常结束但没训完（worker 记成失败）；2 任务说明或环境有问题（参数写错、这台机器没有这个环境）；143 被停掉（健康检查只记不推）；其他是训练出错。10/18 前的 `train_task.sh` 用过 75（训满 `--epochs` 停了），`on_boot.sh`、健康检查仍认。
- **退出码**（`taskq.py next`）：0 领到（输出两行：key、`run_task.sh` 的参数）；10 没有能领的；11 等排在前面刚停下的；12 队列暂停（3 小时内失败 3 次）；其他（如没有任务表时的 2）是出错，worker 照出错处理。`take`：0 领到，10 领不到（队列里没有、已训完、别人领着）；box 上经 ssh 调用不通时是 ssh 的退出码。在跑的 worker 每次都调用仓库里的 `taskq.py`，所以 `next`、`beat`、`release` 的参数和退出码不改，只加新命令。
- **同一个任务不在两处训**：先领到（`claims/<key>/info`，记 worker、host、since、beat、ttl）才训。`skips/<key>`：哪台 pueue 训练机跳过了这一折（每行 时刻、机器、原因），那台以后不领，删掉文件即撤销；`released/`：刚交还的折。领取在 `ttl` 秒内没有续（beat）就过期，别人可以领：DICC 作业 1200 秒、lab 86400 秒。10/16–10/18 用过的 `claimed_elsewhere.txt` 清单不再写（`taskq.py`、`train_folds.sbatch` 里读它的代码在文件超过 2 小时没更新时不起作用）。
- **任务表** `tasks.jsonl` 每行一个任务 `{order, nice, key, exp, recipe, params}`；档位 = `nice`，同一档按 `order`。10/18 前的 `tasks.tsv`（7 列）第一次读时自动转过来，原文件留成 `tasks.tsv.bak`。
- **pueue 任务名**：训练 `train:<key>`；`advance.py` 排的每折 `<步骤>:<实验 id>_f<fold>`（如 `infer:t14met_f3`），实验一级 `<步骤>:<实验 id>`；`advance.py` 按命令或任务名认步骤，10/18 前排的任务也认得。分到 lab 的步骤在 lab 上用同样的名字，box 上的传输是 `push:<名字>`、`pull:<名字>`。
- **能分到别的机器的步骤**（10/22 起）：命令里有 `--stage <步骤>`，把它换成 `--stage needs` 时只打印要用的输入（相对数据目录，每行一个）、不做别的（`run_predict_eval.sh --stage needs`）；结果在 `done` 判断的文件所在的文件夹里。传过去的文件（模型）用完删，文件夹（测试集输入）留着。
- **只在某几台机器上跑**（10/22 起）：任务说明的 `machines`（如 `["dicc"]`；默认不限）。`taskq.py next` 按本机的名字（`envs/machines.py` 的 `whoami`）筛，`refill.py` 不替不在名单里的机器领。推理任务都写 `machines=["dicc"]`。
- **key** 由做法给，nnU-Net 是 `<数据集文件夹>/<训练器>/<fold>`（如 `Dataset111_MENrq1/nnUNetTrainer_save10_n470/0`）。10/17 前按折的 Slurm 作业名 `rq1_<实验 id>_f<fold>`；不带折的 `train_worker`（10/18 前 `rq1_worker`）没有 key。
- **训完** = 任务说明的 `done` 都在（nnU-Net：`validation/summary.json`，和 `checkpoint_final.pth` 或归档后的 `ARCHIVED`）。
- **分段日志**：`<数据目录>/logs/tasks/<key 的文件名>__<作业>_seg<段>.log`，第一行 `作业 <作业>_<段> @ <机器>，任务=<key>，第 <段> 段，环境 <环境>，代码 <版本>，<时间>`；作业 = Slurm 作业号或"主机名-时间"。10/18 前 nnU-Net 的在 `nnUNet/results/logs/`，登记表两种都认。worker 自己的日志（GPU 记录、显存填充）在 `<数据目录>/logs/workers/`。

## 常用命令

```bash
Q=scripts/queue/q
TQ="python3 scripts/queue/taskq.py"
# 加实验表里的实验（档位用表里的 nice；也可以 --nice 指定）、只加某几折、改先后、看一个任务会怎么跑
$TQ add --exp t15met
$TQ add --nice 0 --exp t15men --folds 1 2 3 4
$TQ set-nice --nice 0 Dataset116_METrq1/nnUNetTrainer_save10_n470/2
$TQ show Dataset116_METrq1/nnUNetTrainer_save10_n470/2
# 改一个任务（换环境、换训练器以外的参数；会改变 key 的参数不能改，删掉重加）
$TQ set Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/1 env=umamba
# 手动在 lab 1 号卡训一个：先领（box 上跑，自动经 ssh 转到 DICC），再加任务
$TQ take --worker lab --ttl 86400 Dataset111_MENrq1/nnUNetTrainer_save10_n470/1
$Q gpu1 --label train:Dataset111_MENrq1/nnUNetTrainer_save10_n470/1 -- \
    'bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=1'
# 推理一折放进 DICC 队列（第 ⓪ 档，只在 DICC 领；跑完由 advance.py 拉回 box）
$TQ add --nice -100 --recipe command key=infer/<数据集>/<训练器文件夹>/fold_4 env=nnunet machines='["dicc"]' \
    cmd='bash research/RQ1/analysis/infer_task.sh {data}/nnUNet/results/<数据集>/<训练器文件夹> 4' \
    out=predictions/<数据集>/<训练器文件夹>/fold_4 done='[".infer_done"]'
# box 上单独推理 / 评测一折
$Q gpu --label infer:x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh --stage infer <模型文件夹的完整路径> 4'
$Q cpu --label eval:x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh --stage eval <模型文件夹的完整路径> 4'
```

## 加新东西

- **新实验**：实验表加一个条目（RQ1 用 `_x(...)`：数据集、训练器、折、训完后的步骤 `steps`、档位、看板名称，训练用过外部队列时写 `skip_ext`，做折外评测时写 `splits`；字段说明在 `research/RQ1/experiments.py` 开头）→ `taskq.py add --exp <id>`。别的脚本不用改。
- **新的 nnU-Net 变体**：在 `research/common/nnunet/trainers/` 写训练器（网络代码放 `nets/`），实验条目的 `trainer` 写它，要别的环境时写 `env`。
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
