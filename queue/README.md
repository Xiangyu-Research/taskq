# 任务队列和调度

训练、拉回、推理、评测、分析怎么排、怎么跑（2026-10-18 的结构，10/25 起每一步在哪里做集中写在 [`envs/policy.py`](../../envs/policy.py)）。分工、优先级和理由见 [`admin/作业.md`](../../admin/作业.md)，当前状态见 [`research/RQ1/实验登记表.md`](../../research/RQ1/实验登记表.md)。

调度只做"领任务 → 换环境 → 跑命令 → 看训完没 → 拉回 → 后续步骤"，不认识具体模型。要知道的都在四个 Python 字典文件里：

| 文件 | 内容 |
|---|---|
| [`envs/machines.py`](../../envs/machines.py) | 每台机器：数据目录、conda、环境名 → 怎么激活（`envs`）、pueue 的组、角色、要查的盘。没有某个环境的机器不领用它的任务 |
| [`envs/policy.py`](../../envs/policy.py) | 怎么跑：每个步骤在哪里做（`PLACE`）、自动的部分开不开（`AUTO`）、暂缓哪些步骤（`HOLD`）、只跑手动任务的组（`MANUAL`）、DICC 补交几个作业（`SLURM`） |
| [`recipes.py`](recipes.py) | 做法：一种训练怎么跑（参数和默认值、`key`、`env`、`cmd`、`out`、`done`、`data`、`fetch`、`archive` 等，字段说明在文件开头）。现在有 `nnunet`（`research/common/nnunet/train.sh`）和 `command`（全部由任务自己给） |
| `research/<RQ>/experiments.py` | 实验表：做法和参数、几折、每折训完后的步骤（`after`）、五折都训完后的（`group_after`）、最后一起做的（`FINAL`）、默认档位（`nice`）、分析脚本要的声明（`names`）、看板名称（`board`）。`scripts/queue/experiments.py` 读所有 RQ 的表；分析脚本按模型文件夹查（`experiments.py name-of <模型文件夹> <名字>…`），查不到就报错 |

`spec.py` 是唯一的解析器：做法 + 参数 → 任务说明（`spec.py lookup <key>` 打印某个任务的说明）。

## 结构

```
 DICC ~/.cache/phd/taskq/：tasks.jsonl（唯一的待办，档位 = nice）+ claims/（谁在做哪个任务）
     ↑ 领 / 续 / 交还（taskq.py next）                    ↑ box 上的 refill.py 替 lab 领（take）、续、交还
 DICC 作业：worker.sbatch（GPU）、cpu_worker.sbatch     lab：领到的放进 pueue gpu1 组
     └──────────── 都用 run_task.sh <做法> k=v… 跑：激活环境、设变量、跑命令、写分段日志 ────────────┘
                                   ↓ 训完（任务说明的 done）
 box 的 advance.py（跟着健康检查每 30 分钟）：拉回 → 归档（lab 训的）→ 实验表里的步骤（RQ1：推理 → 评测 → 五折分析）
     每一步在哪里做：envs/policy.py 的 PLACE，按顺序用第一个现在能用的
       box:<组>      box 的 pueue
       trainer       训这一折的 pueue 机器、训练的同一个组，排在训练后面（输入要都在那边）
       queue:<机器>  任务队列，那台机器的 worker 领；缺的文件先传过去，做完按任务拉回
```

pueue 在 box、lab 上只当运行器：按组限并发、留日志、断开 ssh 不中断，不用暂存（`HOLD` 除外）、优先级、`--after`。

## 开关和配置

| 在哪 | 是什么 | 作用 |
|---|---|---|
| `envs/policy.py` `AUTO["advance"]` | True / False | 训完以后的步骤真的排；False 时只把"会排什么"写进 box `~/advance.log` |
| `envs/policy.py` `AUTO["lab"]` | True / False | 替 lab 领训练、预传数据；False 时已领的照常续，其余只记"会做什么" |
| `envs/policy.py` `AUTO["dicc"]` | True / False | 补交 DICC 的 worker 作业；False 时只记"会交什么" |
| `envs/policy.py` `HOLD` | 任务标签的通配式列表（如 `"infer:*"`） | 对上的步骤在 box 上加成暂停的（`pueue enqueue <号>` 才跑），进任务队列的先不加 |
| `envs/policy.py` `PLACE` | 步骤 → 地方的列表 | 每个步骤在哪里做（写法见文件开头；条件只有 `have`）。没写的步骤在 box 上，用实验表里写的组。已经在某处做着的不挪 |
| `envs/policy.py` `MANUAL` | 机器 → 组 | 只跑手动加的任务（lab `gpu0`）：不替它领训练，健康检查不报它空着 |
| `envs/policy.py` `GPU_AUG` | 机器 → True | 这台机器上训练集的数据增强在 GPU 上做（`train.sh` 设 `RQ1_GPU_AUG=1`）；新开始或续训的段生效。单个任务可以预先设 `RQ1_GPU_AUG=0 / 1` |
| `envs/policy.py` `GPU_AUG_LOADER` | 机器 → True | GPU 增强的加载改成训练进程里的线程直接写锁页内存、只传交集（`gpu_aug_loader.py`，`train.sh` 设 `RQ1_GPU_AUG_LOADER=1`，训练前补齐解压缓存，`refill.py` 的 `cache_cleanup()` 删不再用的）；单个任务可以预先设 |
| `envs/policy.py` `SLURM` | 每台 Slurm 机器的 QOS、时限、个数 | `refill.py` 补交多少 GPU / CPU worker 作业 |
| DICC `~/.cache/phd/taskq/ENABLED` | 文件 | 总开关：没有时排着的 worker 作业开始后直接结束（`worker_start.sh`） |
| DICC `~/.cache/phd/gpu_pad/DISABLED` | 文件 | 有时不填显存，在跑的填充 10 秒内放掉退出 |
| `RQ1_GPU_PAD=0` | 环境变量 | 单个作业 / 脚本不填显存（名单见下面"显存填充"） |
| `TASKQ_*` | 环境变量 | `dicc_worker.sh` 的阈值，默认值写在脚本开头：`SLOTS`（一个作业最多同时几个，GPU 2、CPU 1）、`ADD_AFTER`（900 秒）、`UTIL_MAX`（90%）、`MIN_LEFT`（3600 秒）、`PATIENCE`（1200 秒）、`ERR_MAX`（600 秒）、`IDLE_MAX`（2400 秒）、`EARLY_FAIL`（600 秒）、`FAIL_WINDOW`（3600 秒）、`STALL_MAX`（1800 秒）；`TASKQ_DIR`、`TASKQ_SSH`、`TASKQ_MACHINE` 只给测试用 |

`envs/policy.py` 由 box 的健康检查每次读，在 `~/auto/PhD`（只给 cron 用的克隆，跟着 origin/main）里生效，所以改了要提交推送。写错了（不认识的地方、条件、步骤）时 `advance.py` 这次什么都不排，报到健康检查里。

## 出问题时看哪里

| 问题 | 看 |
|---|---|
| 哪个任务没人做、谁领了、失败了几次 | `python3 scripts/queue/taskq.py status`（box 上跑，经 ssh 读 DICC 的队列）；DICC 作业的 `.out`；失败记录 DICC `~/.cache/phd/taskq/failures/`，lab 训不了跳过的 `skips/` |
| lab 领了什么、为什么没领；DICC 补交了哪些作业 | box `~/refill.log`；`python3 scripts/queue/refill.py`（只看会做什么） |
| 训完之后卡在哪一步、为什么没排 | box `~/advance.log`（排了什么、会排什么、"没排"的原因、哪一步失败停住）；`python3 scripts/queue/advance.py`（只看会排什么）；`pueue log <号>` |
| 放到别的 pueue 机器上做的步骤（lab 训的折的推理）走到哪 | box `~/advance.log`；box 上 `pull:<标签>` 任务的 `pueue log`；那边同标签的任务（`ssh lab pueue log <号>`）；还没清理的在 box 的任务记录 `~/.cache/phd/tasks.json` 里（有 `out` 的：机器、标签、组、任务号、训练的任务号、结果文件夹、要用的输入） |
| 某个 pueue 任务被认成哪一步 | box `~/.cache/phd/tasks.json`（见"约定"的任务记录） |
| 总览、报警 | `research/RQ1/实验登记表.md` 最上面的"需要处理"（`scripts/monitor/health.py`） |

## 组

| 机器 | 组 | 同时跑 | 放什么 |
|---|---|---|---|
| box | `gpu` | 1 | 4070：PLACE 写了 `box:gpu` 的步骤、手动的推理 |
| box | `cpu` | 2 | 评测、五折集成评测、删单折概率图、分析（`oof_eval.py`、`paired_compare.py`、`compare_all.py` 等）。同时 2 个不看内存：两个大任务加上 box 上别的服务可能用光 61 GB 内存（10/25 出过一次） |
| box | `wait` | 不限 | 拉回（`fetch_task.sh`）、归档（`archive_task.sh`）、传输（`offload_io.sh`、`tunnel_push.sh`） |
| lab | `gpu1` | 1 | 1 号卡：训练（`run_task.sh`），lab 训的折训完接着推理（PLACE 的 `trainer`） |
| lab | `gpu0` | 1 | 0 号卡：只跑手动加的（`MANUAL`） |
| lab | `cpu`、`wait` | 1、不限 | 备用 |

## 怎么运转

### DICC 作业（`dicc_worker.sh`）

作业只负责申到卡，开始后从任务队列按档位、先后领本机有环境、`machines` 允许、`device` 对得上的任务，用 `run_task.sh` 在后台跑：

- 先跑 1 个；最近一个开始 15 分钟后，近 10 分钟 GPU 平均使用率低于 90%、除去显存填充还空着一半显存，再领 1 个（最多 2 个）。nnU-Net 这类 CPU 跟不上的自然变成 2 个，GPU 跑满的只跑 1 个。CPU 少于 24 个或内存少于 56G 的作业只跑 1 个。
- 跑完一个领下一个；离时限不到 1 小时不再领；到时限时把没做完的交还，下一个领到的从存档接着训。
- 不领：已做完的、别处在做的（领取在有效期内续过：DICC 作业 20 分钟，lab 7 天；或训练日志 15 分钟内更新过）、失败过 2 次的。作业开始时排在前面的任务刚停下，先等它最多 20 分钟。
- 出问题时不让排队的作业空跑掉：1 小时内有 2 个任务开始 10 分钟内就失败，这个作业不再领；任务开始满 30 分钟后日志和结果文件夹都 30 分钟没变化，当作卡住，停掉、交还（不算失败）；整个队列 3 小时内失败 3 次就暂停领任务；手上没有在跑的连续 40 分钟就结束（gpu-a100 使用率连续 1 小时低于 40% 会被取消）；队列读写出错 10 分钟就结束。
- 每个训练 14 个数据增强进程（作业 32 个 CPU）。
- CPU worker（`cpu_worker.sbatch`，`WORKER_DEVICE=cpu`）只领 `device=cpu` 的任务，一次 1 个，领不到就结束。

### 补交 DICC 作业、替 lab 领（`refill.py`，健康检查每 30 分钟调用）

- **DICC**：每个 QOS 排着和在跑的不到 `SLURM` 写的上限、队列里还有没人领的任务时，补交 `worker.sbatch`，空一个补一个，每个空位一个普通作业（不用数组）。队列里有没人领的纯 CPU 任务时才交 `cpu_worker.sbatch`，按剩下的提交名额交。
- **lab**：lab 连不上 DICC，由 box 替它领（`taskq.py take --worker lab --ttl 604800`）、续、交还。每个有卡、开着、不在 `MANUAL` 里的组没有训练时领一个：队列里没人领、任何机器上都没有进度、lab 有环境、盘放得下的，按档位、先后取第一个。
  - 数据集 lab 上没有就先传（传完盘上剩不到 50 GB 的跳过），传完、核对一致再开训（没传完的数据集里有 `.incomplete`）。传数据的任务传完马上跑一次健康检查。
  - 换折不空等：1 号卡在训时，预测的下一折缺数据就提前传（同时只传一份）；在训的这一折按最近 3 小时的速度估计 60 分钟内训完、下一折数据都在时，提前领下一折，排在同一个组后面。
  - 训练没开始就失败（手动停的不算）、又没有进度的（多半是 32 GB 放不下）交还并记为 lab 跳过（`skips/<key>`，DICC 照常领）；24 小时内跳过 2 折就停止领、报出来。
  - lab 训完的折继续领着，等拉回、备份到 DICC 后再交还；任务没了而 lab 上没有进度的交还；没训完、有进度的保留领取、报出来。
  - lab 上自动传过去的数据（box `~/.cache/phd/refill.json`）：队列里没有 lab 能领的任务要用了就删；盘剩的低于软线（硬线的 2 倍，100 GB）时从最晚用到的删起。领着的、在训的、正在传的、拉回和归档在用的、下一折要用的、Dataset101、以前手动放的不删。
  - lab、box 或任何一台训练机连不上时什么都不动。

### 训完以后（`advance.py`，健康检查每 30 分钟调用）

每次对每个任务看盘上的文件，算出它走到哪一步，下一步没在做就排上：

1. 别的机器上训完、box 上没有 → `fetch_task.sh <机器> <key>`（`wait` 组）。
2. 实验表里每折的步骤（RQ1：推理 → 评测），按顺序做第一个没做完的。
3. pueue 训练机（lab）上训完、拉回以后 → `archive_task.sh lab <key>`：备份到 DICC，三份核对一致后删 lab 上的存档和验证集预测。那边还在用这一折做别的步骤时等；要进任务队列的步骤等归档做完再加。
4. 一个实验所有折都在 box 上训完 → `group_after` 的每一串步骤（RQ1：oof、配对比较、五折集成、删单折概率图、P1 / P2 重跑）；最后 `FINAL` 的（`compare_all.py`，有比它上次开始时更新的结果就重跑）。

每一步放在哪：已经在某处做着（box pueue 里有、任务队列里有、任务记录里有那边的）就接着那边的；否则按 `PLACE[步骤]` 的顺序用第一个现在能用的，都用不了时把原因写进 `~/advance.log`（"没排 …"），不报成问题。第一个地方是 `trainer` 的步骤（推理），在这一折还在 lab 训着时就排进训练的组，训完马上做。

- 放到别的 pueue 机器上的：缺的输入先从 box 传过去（文件夹经隧道、文件用 rsync；传完那边的盘要留够，另给结果留 15 GB），齐了在那边的组里排同样的标签（命令里的 `{data}`、`{python}` 换成那边的），成功后 box 拉回、按校验和核对，box 判断做完后删掉那边的结果和自动传过去的文件（别的步骤还要用的留着，文件夹留着）。
- 进任务队列的：那边缺的文件先 `offload_io.sh push` 过去，缺文件夹就报出来；那边做完后按任务拉回（`fetch_task.sh`），拉回后照常按 `done` 判断。`have` 条件的地方只在那边已经有全部输入时用。
- 失败过的不自动重排：最后一次成功之后有失败的任务，或者成功跑完了结果还是不对，就报到健康检查，看过、处理好以后 `pueue remove` 那个任务，下一次就会重排。别的机器上的任务、传输、拉回一样。lab 上训练没训完就停了、排在它后面的推理跟着失败的不算，接着训时重排。
- 提前排（10/25）：DICC 作业训着的折，进任务队列的每折步骤（推理、评测）现在就加，`after` 接上前一步，训完的那个作业接着领推理，推理完马上有 CPU 作业评测。DICC 的代码不是最新的（`code:dicc`）时不提前排。lab 领着、没人领的折照旧等训完。
- 五折集成（`ens`）各折都在 DICC 推理的在 DICC 的 CPU 作业做，拉回时连同 `predictions/<模型>/folds_0-1-2-3-4` 一起（`fetch.also`）；有一折不在 DICC 就在 box 做。
- box 上排的任务做完后马上跑一次健康检查（`auto_refresh.sh kick`，和定时的共用一把锁，最多等 5 分钟），下一步几分钟内排上；往任务队列加了任务的那次检查里接着补交 DICC 作业。
- 判断做完看盘上的文件：推理要每个测试集的分割、概率图、输入病例数都对得上（盘上的文件数和 `MANIFEST.json` 记的一样）；结果还在往 box 上拷（拉回、传输在跑）时不算做完、不清理那边。

### 显存填充（DICC，`gpu_pad.py`）

`gpu-a100` 要求作业显存占用 >= 40 GB（10/17 收到）。`gpu_pad.py` 只占显存、不计算，按 nvidia-smi 的 `memory.used`：作业开始后第一次有训练起，任何时候不低于 **42000 MiB**（floor）；训练占的显存 120 秒内变化不超过 1 GiB 后补到 **55000 MiB**（target）；超过 **62000 MiB** 放掉多出的。每次补、放 1 GB，给训练留至少 8 GB；总显存不到 60 GB 时不填。训练占的显存不到 2000 MiB 算没有训练。开始新训练时 `dicc_worker.sh` 删掉 `/tmp/gpu_pad_<作业号>.ready`、touch `.start`，pad 放到 floor 后写 `.ready`，worker 看到再开训（最多等 30 秒），新训练稳定后再补到 target（最多等 900 秒）。

- 默认开启：`gpu_pad_start.sh`（`worker_start.sh` 直接调用，其他作业经 `envs/nnunet_paths.sh`）在所有 Slurm GPU 作业里启动它。日志 `<数据目录>/logs/workers/gpu_pad_<作业号>.log`。
- `RQ1_GPU_PAD=0` 的脚本：启动前要求卡是空的 `research/common/nnunet/smoke/run_gpu_smoke.sh`、`research/RQ1/nnunet/predict.sh`、`research/RQ1/analysis/run_predict_eval.sh`；自己做推理或分析的 `research/RQ1/probe/p1_predict.sh`、`research/RQ1/probe/run_p2.sh`、`research/RQ2/quickcheck/run_p4.sh`、`research/RQ1/analysis/run_sensitivity.sh`、`research/RQ1/external/run_t19.sh`、`research/RQ1/external/t19_eval.sh`、`research/RQ1/probe/p1_predict_model.sh`、`research/RQ1/probe/p2_predict_model.sh`、`research/RQ1/analysis/validation_predict_eval.sh`、`research/RQ1/analysis/validation_prep.sh`。CPU 分区的作业本来就不启动。
- pad 异常退出时最多重启 3 次；父进程退出、收到 SIGTERM、`DISABLED` 出现时放掉显存退出。退出码：0 正常 / 开关 / 父进程退出，1 没有 GPU 或初始化失败，2 参数错误；启动失败只记日志，不影响训练。

## 文件

| 文件 | 内容 |
|---|---|
| `q`、`qs` | 加任务：`q <组> [pueue add 选项] -- '<命令>'`（按 `envs/machines.py` 设好卡和组的环境变量，工作目录是仓库根目录）；`qs [机器]` 看各 pueue 机器的队列 |
| `taskq.py` | 任务队列：worker 领（`next`）、心跳（`beat`）、交还（`release`）；替 lab 领指定的一个（`take`）；加（`add --exp`、`add --recipe`）、改参数（`set`）、改先后（`set-nice`）、删掉没人在做的（`remove`，依赖它的要一起删）、看说明（`show`）、看队列（`status`）。命令说明在文件开头 |
| `recipes.py`、`spec.py`、`experiments.py`、`steps.py` | 做法、解析器、读实验表、步骤的"做完没"判断 |
| `run_task.sh <做法> k=v…` | 所有机器共用的运行器：激活环境、设变量、跑命令、写分段日志；已经训完就直接退出；Slurm 外设了 `CUDA_VISIBLE_DEVICES` 时卡被占就等，出错隔 5 分钟重来、最多 3 次；SIGTERM 先停训练再退出 |
| `worker.sbatch`、`worker_start.sh`、`cpu_worker.sbatch`、`dicc_worker.sh` | DICC 作业：`worker.sbatch`（作业名见 `envs/machines.py` 的 `jobs`，long、3 天）只有资源申请和转交到 `worker_start.sh` 的一行；`worker_start.sh` 查 `ENABLED`、激活 conda、启动显存填充，交给 `dicc_worker.sh`。`cpu_worker.sbatch`：`cpu-epyc-genoa`、normal、32 核、64G、8 小时（`cpu-epyc` 的节点系统太旧） |
| `refill.py` | 补交 DICC 作业、替 lab 领任务（见上面）；`python3 scripts/queue/refill.py` 只看会做什么 |
| `advance.py` | 训完以后的步骤（见上面）；`python3 scripts/queue/advance.py` 只看会排什么 |
| `fetch_task.sh <机器> <key>` | 等那台机器上的任务做完，把 `fetch.also` 的文件夹和 `out`（去掉 `fetch.exclude`，加上 `fetch.extra`）拉回 box，`out` 最后拉 |
| `archive_task.sh <机器> <key> [--dry-run]` | 训练机上训完、拉回以后：备份到 DICC，三份核对一致后删训练机上 `archive.drop` 匹配的文件，留 `ARCHIVED` |
| `offload_io.sh` | box 和别的机器之间传一步的输入、结果：`push <机器> <路径>…`（文件夹经 `scripts/sync/tunnel_push.sh`，文件经 `tunnel.py push`）、`pull <机器> <文件夹>`（rsync 拉回，再按校验和核对）。这里和 `fetch_task.sh`、`archive_task.sh` 的 rsync 都经 `scripts/sync/tunnel.py rsync`：`envs/machines.py` 的 `tunnel` 里有的方向文件都经隧道传（`scripts/sync/README.md`） |
| `on_boot.sh` | 开机（`@reboot`）：启动 pueued，把被打断的任务原地放回队列（nnU-Net 从存档接着训，推理跳过已做好的病例），记在 `~/on_boot.log`；`gpu_log` 的机器启动 GPU 使用记录 |
| `setup.sh`、`install_pueue.sh` | 装 pueue、启动常驻进程、按表建组（表里没有的空组删掉）、写 crontab（`on_boot.sh`，从表里的 `code` 跑；hub 的 `scripts/monitor/auto_refresh.sh`，它也负责更新 box 的代码副本、快进 lab 和 DICC 的克隆） |
| `gpu_pad.py`、`gpu_pad_start.sh` | 显存填充（见上面） |
| `test_*.py`、`test_*.sh` | 测试（假训练、假队列、假快照，不连机器、不用 GPU；`test_gpu_pad_integration.sh` 用 box 的真卡）。`test_taskq.py` 的"worker 的调用方式"一组和 `test_dicc_worker.sh` 守着在跑的 worker 用的接口 |

## 约定

- **DICC 作业提交时定下的**只有 `worker.sbatch`；其余脚本在作业开始时或每次调用时从仓库读。排着的作业依赖的只有 `ENABLED` 和 `worker_start.sh` 这个路径；在跑的作业（最长 3 天）一直用开始时读到的 `dicc_worker.sh`，它调用的 `taskq.py next / beat / release` 和 `run_task.sh` 的参数、退出码不改，只加新命令。
- **退出码**：`run_task.sh` 0 训完，3 命令正常结束但没训完，2 任务说明或环境有问题，1 训练命令出错（命令自己的退出码 1–127 记在分段日志），128 以上是被信号停掉（自己收到 SIGTERM 是 143，训练命令被杀如 137 原样传出；`refill.py` 据此区分"lab 训不了"和"被杀"）（10/25 定）。`qs --json` 每个任务的 `status` 给人看（如 `Failed (1)`），程序读 `result`（`Success` / `Failed` / `Killed` / …，没结束是空）和 `code`（退出码）。`taskq.py next` 0 领到（两行：key、`run_task.sh` 的参数），10 没有能领的，11 等排在前面刚停下的，12 队列暂停；`take` 0 领到，10 领不到。
- **经 ssh 读回的 JSON**：`taskq.py --json-status`、`taskq.py show --json`、`spec.py` 的 `remote_collect` 把 JSON 写在单独一行、以 `@@JSON ` 开头，读的一方只认这一行（`spec.py` 的 `json_out` / `json_in`），前后别的输出不影响。`taskq.remote` 分开返回标准输出和标准错误，ssh 不通（退出码 255）返回空（10/25 定）。
- **各机器的状态只取一次**：`scripts/monitor/health.py` 的 `gather()`：pueue（`qs --json`）、每台 pueue 机器一次 ssh 的探查（开机、盘、GPU 使用记录、各卡和卡上的进程、训练要用的数据在不在）、Slurm 的 `squeue`（作业名、号、状态、已跑、原因、Nice、分区）和 `sacct`、任务队列、各任务的训练状态（`spec.collect`）。健康检查、`refill.py`、`advance.py` 和登记表 `make_registry.py` 都用它的结果，不另外去取（10/25 定）。
- **队列所在的机器**：`envs/machines.py` 里用 Slurm、能训练的那台（`taskq.queue_host()`）；在别的机器上跑 `taskq.py add / set / set-nice / take / show / remove / status` 时经 ssh 到那边跑。worker 用的 `next / beat / release` 不转发，没有 `tasks.jsonl` 就退出 2（10/25 定）。
- **Slurm 作业名**：`envs/machines.py` 的 `jobs`（`gpu` = `worker.sbatch`、`cpu` = `cpu_worker.sbatch` 的 `--job-name`，`test_refill.py` 核对两边一致）；健康检查只把 GPU worker 当成"还有训练作业"，`refill.py` 按 CPU worker 的名字数已有几个；查提交名额用的 QOS 来自 `envs/policy.py` 的 `SLURM`（10/25 定）。
- **代码版本**：box 的 pueue 任务的工作目录是 `~/.cache/phd/code/<提交号>`（`envs/machines.py` 的 `code`，`q` 加任务时先跑 `scripts/sync/snapshot.sh` 更新到 origin/main），任务开始后代码再更新也不影响它；旧副本没有任务用、建了 7 天以上才删。lab、DICC 跑各自克隆里的代码（`repo`），box 每 30 分钟快进。
- **跑命令、查文件**：调度脚本在本机或经 ssh 跑命令都用 `common.run`（返回退出码、标准输出、标准错误；超时、起不来是 124）；查别的机器上有哪些文件用 `common.have_script` / `parse_have`（输出以 `@@END` 结束，没有结束标记算没查到）（10/25 定）。
- **日志不重复**：`advance.py`、`refill.py` 的"会做什么"和上次一样就不再写日志，上次的内容在 `~/.cache/phd/last/<名字>.json`（`common.changed`）。
- **状态文件整份换**：`refill.json`、`tasks.json`、`last/` 下的文件、领取的 `info` 都先写临时文件再换过去（`scripts/queue/common.py` 的 `write_json`），断电、被杀时只有旧的或新的。领取只建了目录、没有 `info` 的，5 分钟后算过期。
- **同一个任务不在两处做**：先领到（`claims/<key>/info`：worker、host、since、beat、ttl）才做；`ttl` 秒内没续就过期（DICC 作业 1200 秒、lab 604800 秒：box 离线时不过期）。`skips/<key>`：哪台 pueue 训练机跳过了这一折（删掉即撤销）；`released/`：刚交还的。
- **任务表** `tasks.jsonl` 每行 `{order, nice, key, exp, recipe, params}`；档位 = `nice`，同一档按 `order`。任务说明的 `device`（默认 `gpu`）、`machines`（默认不限，如 `["dicc"]`）决定谁能领。`after`（默认 `[]`）：一组任务 key，都做完前这个任务是 `waiting`（不领、不算补交作业的数）；其中有失败 2 次的或不在队列里的是 `blocked`，健康检查报 `tqblocked:<key>`。
- **pueue 任务标签**：训练 `train:<key>`；`advance.py` 排的每折 `<步骤>:<实验 id>_f<fold>`（如 `infer:t14met_f3`），实验一级 `<步骤>:<实验 id>`。放到别的机器上的步骤在那边用同样的标签，box 上的传输是 `push:<标签>`（任务队列缺的文件）、`pull:<标签>`（训练机上做完的拉回），传数据集给 lab 是 `sync_<机器>_<数据集>`。标签只给人看，程序不靠它认任务（见下一条）。
- **任务记录**（10/25）：box、lab 的 pueue 任务是哪一步、哪个 key 只看 box 的 `~/.cache/phd/tasks.json`（`scripts/queue/common.py`）。每条 `{machine, id, label, added, step, key}`，按步骤另有 `group`、`src`（从哪拉回）、`to`、`paths`（传给谁、传什么）、`of`（传输为哪一步）、`out`、`needs`、`train_id`（训练机上做的步骤）。`refill.py`、`advance.py` 加任务前先写记录（`id` 为空）、加上后补任务号；没拿到任务号的，健康检查按"同一台机器、同标签、创建时间不早于记录"找回，一小时找不到就作废。任务号和标签都对上才算同一个任务（任务号会被重用）；任务删掉后记录跟着删，有 `out` 的留到那边的结果清理完；读不到的机器的记录不动。没有记录的（手动加的）只按命令认训练（`run_task.sh`）、拉回（`fetch_task.sh <机器> <key>`）、归档（`archive_task.sh <机器> <key>`）。
- **能放到别处的步骤**：命令里有 `--stage <步骤>`，换成 `--stage needs` 时只打印要用的输入（相对数据目录，每行一个），或者步骤的 `queue` 里写了 `inputs`；结果在 `done` 判断的文件所在的文件夹里。
- **key** 由做法给，nnU-Net 是 `<数据集>/<训练器>/<fold>`（如 `Dataset111_MENrq1/nnUNetTrainer_save10_n470/0`）。**训完** = 任务说明的 `done` 都在（nnU-Net：`validation/summary.json`，和 `checkpoint_final.pth` 或归档后的 `ARCHIVED`）。
- **分段日志**：`<数据目录>/logs/tasks/<key 的文件名>__<作业>_seg<段>.log`，第一行 `作业 <作业>_<段> @ <机器>，任务=<key>，第 <段> 段，环境 <环境>，代码 <版本>，<时间>`（10/18 前 nnU-Net 的在 `nnUNet/results/logs/`，登记表两种都认）。worker 自己的日志在 `<数据目录>/logs/workers/`。

## 常用命令

```bash
Q=scripts/queue/q
TQ="python3 scripts/queue/taskq.py"
# 加实验表里的实验（档位用表里的 nice，也可以 --nice）、只加某几折、改先后、看一个任务会怎么跑
$TQ add --exp t15met
$TQ add --nice 0 --exp t15men --folds 1 2 3 4
$TQ set-nice --nice 0 Dataset116_METrq1/nnUNetTrainer_save10_n470/2
$TQ show Dataset116_METrq1/nnUNetTrainer_save10_n470/2
# 改一个任务（会改变 key 的参数不能改，删掉重加）
$TQ set Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/1 env=umamba
# 手动在 lab 训一个：先领（box 上跑，自动经 ssh 转到 DICC），再加任务
$TQ take --worker lab --ttl 604800 Dataset111_MENrq1/nnUNetTrainer_save10_n470/1
$Q gpu1 --label train:Dataset111_MENrq1/nnUNetTrainer_save10_n470/1 -- \
    'bash scripts/queue/run_task.sh nnunet dataset=Dataset111_MENrq1 trainer=nnUNetTrainer_save10_n470 fold=1'
# box 上单独推理 / 评测一折
$Q gpu --label infer:x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh --stage infer <模型文件夹的完整路径> 4'
$Q cpu --label eval:x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh --stage eval <模型文件夹的完整路径> 4'
```

pueue 本身：`pueue log <号>`、`pueue follow <号>`、`pueue kill -s SIGTERM <号>`（停训练，存档每 10 个 epoch 一次）、`pueue restart --in-place <号>`（失败的原地重跑）、`pueue switch <号> <号>`、`pueue clean -s`（清掉成功的）。pueued 重启时正在跑的任务会标成失败（137 / 143）。

## 加新东西

- **新实验**：实验表加一个条目（RQ1 用 `_x(...)`，字段说明在 `research/RQ1/experiments.py` 开头）→ `taskq.py add --exp <id>`。
- **新的 nnU-Net 变体**：`research/common/nnunet/trainers/` 写训练器（网络代码放 `nets/`），实验条目的 `trainer` 写它，要别的环境时写 `env`。
- **新环境**：各台机器装好 → `envs/machines.py` 里那几台的 `envs` 加一项。
- **新机器**：`envs/machines.py` 的 `MACHINES` 加一项；pueue 机器上跑一次 `setup.sh`，box 上配好 ssh。要让某些步骤放过去，`PLACE` 现有的写法（`trainer`、`queue:<机器>`、`box:<组>`）表达不了时，给 `advance.py` 加一种地方。
- **不是 nnU-Net 的训练**：实验表里 `recipe="command"`，`params` 写 `key`、`cmd`、`env`、`out`、`done`（还可以写 `data`、`log`、`vars`）；同一类多了在 `recipes.py` 加一种做法。命令要能"重跑就接着训"。
- **实验表以外的临时任务**：`taskq.py add --nice <档> --recipe <做法> k=v…`（只拉回、归档，没有后续步骤）。
