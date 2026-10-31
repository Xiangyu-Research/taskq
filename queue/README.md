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
 box ~/.cache/phd/taskq/：总队列 tasks.jsonl（唯一的待办，档位 = nice）+ claims/（分给哪台执行机：assigned）
     ↓ feed.py 分给执行机（take --assign）、送进它的缓冲（put）；lab 0 号卡手动训的分给 lab:gpu0，不进缓冲
 DICC、lab ~/.cache/phd/taskq/：缓冲（BUFFER，总队列一部分的原样副本）
     ↑ 领 / 续 / 交还（taskq.py next）：同一个 dicc_worker.sh
 DICC：worker.sbatch（GPU）、cpu_worker.sbatch 作业     lab：pueue gpu1 组里常驻一个 worker（machines.py 的 worker）
     └──────────── 都用 run_task.sh <做法> k=v… 跑：激活环境、设变量、跑命令、写分段日志 ────────────┘
                                   ↓ 训完（任务说明的 done；总队列看 box 上的结果，10/31）
 box 的 advance.py（跟着健康检查每 30 分钟）：拉回 → 归档（lab 训的）→ 实验表里的步骤（RQ1：推理 → 评测 → 五折分析）
     每一步在哪里做：envs/policy.py 的 PLACE，按顺序用第一个现在能用的
       box:<组>      box 的 pueue
       trainer       训这一折的执行机的缓冲（machines=[那台]，after 接训练，nice -100）；那台没有缓冲时用下一项
       queue:<机器>  任务队列，那台机器的 worker 领；缺的文件先传过去，做完按任务拉回
```

pueue 在 box、lab 上只当运行器：按组限并发、留日志、断开 ssh 不中断，不用暂存（`HOLD` 除外）、优先级、`--after`。

## 开关和配置

| 在哪 | 是什么 | 作用 |
|---|---|---|
| `envs/policy.py` `AUTO["advance"]` | True / False | 训完以后的步骤真的排；False 时只把"会排什么"写进 box `~/advance.log` |
| `envs/policy.py` `AUTO["lab"]` | True / False | 替 lab 领训练、预传数据；False 时已领的照常续，其余只记"会做什么" |
| `envs/policy.py` `AUTO["dicc"]` | True / False | 往 DICC 的缓冲送新任务（`feed.py`）、补交 DICC 的 worker 作业；False 时只记"会送 / 会交什么"（`~/feed.log`、`~/refill.log`），已分过去的照常交还、同步 |
| `envs/policy.py` `READY` | 执行机 → 个数 | 缓冲里备几个没人领的训练（`feed.py`、`relay.py`） |
| `envs/policy.py` `AUTO["dicc2"]` | True / False | 中转执行机（DICC2）：领新的折、补交作业；False 时只记进 box `~/relay.log`，已领的折照常推进（见下面"执行机"） |
| `envs/policy.py` `HOLD` | 任务标签的通配式列表（如 `"infer:*"`） | 对上的步骤在 box 上加成暂停的（`pueue enqueue <号>` 才跑），进任务队列的先不加 |
| `envs/policy.py` `PLACE` | 步骤 → 地方的列表 | 每个步骤在哪里做（写法见文件开头；条件只有 `have`）。没写的步骤在 box 上，用实验表里写的组。已经在某处做着的不挪 |
| `envs/policy.py` `MANUAL` | 机器 → 组 | 只跑手动加的任务（lab `gpu0`）：不替它领训练，健康检查不报它空着 |
| `envs/policy.py` `GPU_AUG` | 机器 → True | 这台机器上训练集的数据增强在 GPU 上做（`train.sh` 设 `RQ1_GPU_AUG=1`）；新开始或续训的段生效。单个任务可以预先设 `RQ1_GPU_AUG=0 / 1` |
| `envs/policy.py` `GPU_AUG_LOADER` | 机器 → True | GPU 增强的加载改成训练进程里的线程直接写锁页内存、只传交集（`gpu_aug_loader.py`，`train.sh` 设 `RQ1_GPU_AUG_LOADER=1`，训练前补齐解压缓存，`refill.py` 的 `cache_cleanup()` 删不再用的）；单个任务可以预先设 |
| `envs/policy.py` `SLURM` | 每台 Slurm 机器的 QOS、时限、个数 | `refill.py` 补交多少 GPU / CPU worker 作业 |
| `envs/policy.py` `SHIFT` | 机器 → {组: (开始, 结束)} | `shift.py` 跟着健康检查在时段开始时恢复组（卡上有别人的进程就等）、结束时暂停，在训没训完的训练交给这台的缓冲（总队列里 `<机器>:<组>` 改分给 `<机器>`、pueue 里删掉，worker 从存档接着训），别的任务放回组的队列；每个时段只动这两次，记录在 box `~/.cache/phd/shift.json`，日志 `~/refill.log` |
| box `~/.cache/phd/taskq/ENABLED` | 文件 | 总队列的开关：没有时 `feed.py`、`refill.py`、`relay.py` 什么都不分 |
| DICC `~/.cache/phd/taskq/ENABLED` | 文件 | 缓冲的开关：没有时排着的 worker 作业开始后直接结束（`worker_start.sh`），`feed.py` 也不往里送 |
| DICC `~/.cache/phd/gpu_pad/DISABLED` | 文件 | 有时不填显存，在跑的填充 10 秒内放掉退出 |
| `RQ1_GPU_PAD=0` | 环境变量 | 单个作业 / 脚本不填显存（名单见下面"显存填充"） |
| `TASKQ_*` | 环境变量 | `dicc_worker.sh` 的阈值，默认值写在脚本开头：`SLOTS`（一个作业最多同时几个，GPU 2、CPU 1）、`ADD_AFTER`（900 秒）、`UTIL_MAX`（90%）、`MIN_LEFT`（3600 秒）、`PATIENCE`（1200 秒）、`ERR_MAX`（600 秒）、`IDLE_MAX`（2400 秒）、`EARLY_FAIL`（600 秒）、`FAIL_WINDOW`（3600 秒）、`STALL_MAX`（1800 秒）；`TASKQ_DIR`、`TASKQ_SSH`、`TASKQ_MACHINE` 只给测试用 |

`envs/policy.py` 由 box 的健康检查每次读，在 `~/auto/PhD`（只给 cron 用的克隆，跟着 origin/main）里生效，所以改了要提交推送。写错了（不认识的地方、条件、步骤）时 `advance.py` 这次什么都不排，报到健康检查里。

## 出问题时看哪里

| 问题 | 看 |
|---|---|
| 哪个任务没人做、分给了哪台、失败了几次 | `python3 scripts/queue/taskq.py status`（box 上跑，读总队列）；执行机缓冲里谁在训：`ssh dicc 'cd ~/Projects/PhD && python3 scripts/queue/taskq.py status'`（lab：`ssh lab 'cd ~/PhD && …'`）；DICC 作业的 `.out`，lab worker 的日志在 lab 数据目录的 `logs/workers/` 和 `pueue log <worker 的任务号>`；失败记录在执行机的缓冲 `~/.cache/phd/taskq/failures/`，训不了跳过的在总队列的 `skips/` |
| 往 DICC、lab 送了什么、为什么没送、传了哪些数据、删了 lab 上哪些数据、开了 worker | box `~/feed.log`；`python3 scripts/queue/feed.py`（只看会做什么） |
| DICC 补交了哪些作业 | box `~/refill.log`；`python3 scripts/queue/refill.py`（只看会做什么） |
| 训完之后卡在哪一步、为什么没排 | box `~/advance.log`（排了什么、会排什么、"没排"的原因、哪一步失败停住）；`python3 scripts/queue/advance.py`（只看会排什么）；`pueue log <号>` |
| lab 训的折的推理走到哪 | 总队列和 lab 缓冲的 `taskq.py show infer/<key>`（lab 上 `ssh lab 'cd ~/PhD && python3 scripts/queue/taskq.py show …'`）；拉回看 box 上 `fetch:infer/…` 的 `pueue log`；box `~/advance.log` |
| 某个 pueue 任务被认成哪一步 | box `~/.cache/phd/tasks.json`（见"约定"的任务记录） |
| 总览、报警 | `research/RQ1/实验登记表.md` 最上面的"需要处理"（`scripts/monitor/health.py`） |

## 组

| 机器 | 组 | 同时跑 | 放什么 |
|---|---|---|---|
| box | `gpu` | 1 | 4070：PLACE 写了 `box:gpu` 的步骤、手动的推理 |
| box | `cpu` | 2 | 评测、五折集成评测、删单折概率图、分析（`oof_eval.py`、`paired_compare.py`、`compare_all.py` 等）。同时 2 个不看内存：两个大任务加上 box 上别的服务可能用光 61 GB 内存（10/25 出过一次） |
| box | `wait` | 不限 | 拉回（`fetch_task.sh`）、归档（`archive_task.sh`）、传输（`offload_io.sh`、`tunnel_push.sh`） |
| lab | `gpu1` | 1 | 1 号卡：常驻的 worker（`dicc_worker.sh`，`WORKER_ID=lab-gpu1`），从 lab 的缓冲领训练和推理（lab 训的折，含 0 号卡训的） |
| lab | `gpu0` | 1 | 0 号卡：只跑手动加的训练（`MANUAL`，总队列里分给 `lab:gpu0`）；晚班 18:00–9:00 由 `shift.py` 开关（`SHIFT`） |
| lab | `cpu`、`wait` | 1、不限 | 备用 |

## 怎么运转

### DICC 作业（`dicc_worker.sh`）

作业只负责申到卡，开始后从任务队列按档位、先后领本机有环境、`machines` 允许、`device` 对得上的任务，用 `run_task.sh` 在后台跑：

- 先跑 1 个；最近一个开始 15 分钟后，近 10 分钟 GPU 平均使用率低于 90%、除去显存填充还空着一半显存，再领 1 个（最多 2 个）。nnU-Net 这类 CPU 跟不上的自然变成 2 个，GPU 跑满的只跑 1 个。CPU 少于 24 个或内存少于 56G 的作业只跑 1 个。
- 跑完一个领下一个；离时限不到 1 小时不再领；到时限时把没做完的交还，下一个领到的从存档接着训。
- 只领本机缓冲里的（`feed.py` 送来的）。不领：已做完的、别处在做的（领取在有效期内续过：20 分钟；或训练日志 15 分钟内更新过）、失败过 2 次的、依赖记录（只给 `after` 看的）。作业开始时排在前面的任务刚停下，先等它最多 20 分钟。
- 出问题时不让排队的作业空跑掉：1 小时内有 2 个任务开始 10 分钟内就失败，这个作业不再领；任务开始满 30 分钟后日志和结果文件夹都 30 分钟没变化，当作卡住，停掉、交还（不算失败）；整个队列 3 小时内失败 3 次就暂停领任务；停止的任务（`taskq.py stop`）每 5 秒看一次，先发 SIGTERM，20 秒没停就 SIGKILL，交还（不算失败），30 秒内做完（存档是原子写的）；手上没有在跑的连续 40 分钟就结束（gpu-a100 使用率连续 1 小时低于 40% 会被取消）；队列读写出错 10 分钟就结束。
- 每个训练 14 个数据增强进程（作业 32 个 CPU）。
- CPU worker（`cpu_worker.sbatch`，`WORKER_DEVICE=cpu`）只领 `device=cpu` 的任务，一次 1 个，领不到就结束。

### 往执行机送任务（`feed.py`，健康检查每 30 分钟调用，10/31）

送任务只有一个入口：健康检查调用 `feed.run_all`，它先把 `envs/machines.py` 的 `SITES` 里 `relay=True` 的执行机（lab1、DICC2）交给 `relay.py`（见下面"执行机"，细节记 box `~/relay.log`，`~/feed.log` 里记一行指过去），再给有缓冲的执行机（我的 DICC、lab）送任务；`advance.py` 往总队列加了任务后的第二遍只送缓冲的（10/31）。`python3 scripts/queue/feed.py --dry` 只看会做什么（`--no-relay` 不看 relay 的那几台）。

总队列在 box，执行机维护、连不上时总队列照常可读可改；执行机只从本机的缓冲领，box 不在线时照常训。DICC、lab 用同一套规则（写在 `feed.py` 开头），要点：

- 训练：缓冲里没人领的不到 `READY[机器]` 个时，按总队列的先后补上，只送数据已到位的（每个数据路径在那边的文件数、字节数都不少于 box 上的，没有 `.incomplete`，也没有在传）；缺的由 box `wait` 组传（DICC `sync_data.sh dicc push <路径>`，lab 经隧道 `tunnel_push.sh`），同时只传一份。补不满持续 60 分钟报 `site:<机器>:low`。
- 只有这台能跑的（`machines=[那台]`：推理、评测、P1 / P2）马上送，不受个数限制；`after` 里在这台训完、已交还的折作为依赖记录一起送。
- box 上训完（拉回、归档都做完）→ 删缓冲记录、总队列交还；总队列 `stop` → 缓冲里停，停下后删、交还；`set`、`set-nice` 同步过去。
- 缓冲里失败、这台上没有进度的交还（`--skip <机器>`）；有进度的留着报出来；缓冲整个暂停时只报。连不上时什么都不交还，2 小时后报 `site:<机器>:down`。那边的代码不是最新时不送新任务。那边没有 `BUFFER` 时什么都不做。
- lab 另外：`gpu1` 组里没有活着的 worker 就开一个（worker 手上没活、代码更新了时自己退出，下次检查重开；上一个失败了报 `site:lab:worker`，`pueue remove` 后重开）；lab 上自动传过去的数据（box `~/.cache/phd/refill.json`）按空间预算管（`refill.cleanup()`）：总队列里没有 lab 能用的任务要用了就删，盘剩的低于软线（硬线的 2 倍，100 GB）时从最晚用到的删起，放不下就不传；缓冲里的、分给 lab / `lab:*` 的、正在传的、拉回和归档在用的、实验表 `KEEP_DATA` 里的、以前手动放的不删。分给 `lab:gpu0` 的（0 号卡手动训）box 上训完、没在拷时交还。

### 补交 DICC 作业、管 lab 的盘（`refill.py`，健康检查每 30 分钟调用）

- **DICC**：每个 QOS 排着和在跑的不到 `SLURM` 写的上限、缓冲里还有没人领的任务时，补交 `worker.sbatch`，空一个补一个，每个空位一个普通作业（不用数组）；排着没开始的不超过缓冲里备好、没人领的任务数（先备好再开卡，10/31 定）。缓冲里有没人领的纯 CPU 任务时才交 `cpu_worker.sbatch`，按剩下的提交名额交。在 `feed.py` 送完之后补。读作业数、交作业用 `scripts/sites/slurm.py`，交几个在 `refill.topup_plan` 算。
- **lab 的盘**（`cleanup()`）：按上一节的空间预算删 lab 上自动传过去的数据；lab 或 box 连不上时不动。传数据（`transfer()`）由 `feed.py` 调用。

### 训完以后（`advance.py`，健康检查每 30 分钟调用）

每次对每个任务看盘上的文件，算出它走到哪一步，下一步没在做就排上：

1. 别的机器上训完、box 上没有 → `fetch_task.sh <机器> <key>`（`wait` 组）。
2. 实验表里每折的步骤（RQ1：推理 → 评测），按顺序做第一个没做完的。
3. pueue 训练机（lab）上训完、拉回以后 → `archive_task.sh lab <key>`：备份到 DICC，三份核对一致后删 lab 上的存档和验证集预测。那边还在用这一折做别的步骤时等；要进任务队列的步骤等归档做完再加。
4. 一个实验所有折都在 box 上训完 → `group_after` 的每一串步骤（RQ1：oof、配对比较、五折集成、删单折概率图、P1 / P2 重跑）；最后 `FINAL` 的（`compare_all.py`，有比它上次开始时更新的结果就重跑）。

每一步放在哪：已经在某处做着（box pueue 里有、任务队列里有、任务记录里有那边的）就接着那边的；否则按 `PLACE[步骤]` 的顺序用第一个现在能用的，都用不了时把原因写进 `~/advance.log`（"没排 …"），不报成问题。推理的第一个地方是 `trainer`：训这一折的执行机（DICC、lab；`lab:gpu0` 算 lab；分配结束后看哪台有没归档的模型）的缓冲，那台在归档、或是 DICC2 时用下一项。lab 训的折，推理做完、拉回 box 以后才归档。

- 放到别的 pueue 机器上的：缺的输入先从 box 传过去（文件夹经隧道、文件用 rsync；传完那边的盘要留够，另给结果留 15 GB），齐了在那边的组里排同样的标签（命令里的 `{data}`、`{python}` 换成那边的），成功后 box 拉回、按校验和核对，box 判断做完后删掉那边的结果和自动传过去的文件（别的步骤还要用的留着，文件夹留着）。
- 进任务队列的：那边缺的文件先 `offload_io.sh push` 过去，缺文件夹就报出来；那边做完后按任务拉回（`fetch_task.sh`），拉回后照常按 `done` 判断。`have` 条件的地方只在那边已经有全部输入时用。
- 失败过的不自动重排：最后一次成功之后有失败的任务，或者成功跑完了结果还是不对，就报到健康检查，看过、处理好以后 `pueue remove` 那个任务，下一次就会重排。别的机器上的任务、传输、拉回一样。lab 上训练没训完就停了、排在它后面的推理跟着失败的不算，接着训时重排。
- 提前排（10/25，10/31 改看总队列的分配）：总队列里分给 DICC、lab（含 `lab:gpu0`）的折，进任务队列的每折步骤（推理；DICC 的还有评测）现在就加进总队列，`after` 接上前一步，`feed.py` 跟着送进那台的缓冲，worker 训完一折先领它的推理（nice -100），DICC 推理完马上有 CPU 作业评测。那台的代码不是最新的（`code:<机器>`）时不提前排。分给 DICC2 的和没分出去的折等训完。
- 五折集成（`ens`）各折都在 DICC 推理的在 DICC 的 CPU 作业做，拉回时连同 `predictions/<模型>/folds_0-1-2-3-4` 一起（`fetch.also`）；有一折不在 DICC 就在 box 做。
- box 上排的任务做完后马上跑一次健康检查（`auto_refresh.sh kick`，和定时的共用一把锁，最多等 5 分钟），下一步几分钟内排上；往总队列加了任务的那次检查里接着送到 DICC、补交 DICC 作业。
- 判断做完看盘上的文件：推理要每个测试集的分割、概率图、输入病例数都对得上（盘上的文件数和 `MANIFEST.json` 记的一样）；结果还在往 box 上拷（拉回、传输在跑）时不算做完、不清理那边。

### 显存填充（DICC，`gpu_pad.py`）

`gpu-a100` 要求作业显存占用 >= 40 GB（10/17 收到）。`gpu_pad.py` 只占显存、不计算，按 nvidia-smi 的 `memory.used`：作业开始后第一次有训练起，任何时候不低于 **42000 MiB**（floor）；训练占的显存 120 秒内变化不超过 1 GiB 后补到 **55000 MiB**（target）；超过 **62000 MiB** 放掉多出的。每次补、放 1 GB，给训练留至少 8 GB；总显存不到 60 GB 时不填。训练占的显存不到 2000 MiB 算没有训练。开始新训练时 `dicc_worker.sh` 删掉 `/tmp/gpu_pad_<作业号>.ready`、touch `.start`，pad 放到 floor 后写 `.ready`，worker 看到再开训（最多等 30 秒），新训练稳定后再补到 target（最多等 900 秒）。

- 默认开启：`gpu_pad_start.sh`（`worker_start.sh` 直接调用，其他作业经 `envs/nnunet_paths.sh`）在所有 Slurm GPU 作业里启动它。日志 `<数据目录>/logs/workers/gpu_pad_<作业号>.log`。
- `RQ1_GPU_PAD=0` 的脚本：启动前要求卡是空的 `research/common/nnunet/smoke/run_gpu_smoke.sh`、`research/RQ1/nnunet/predict.sh`、`research/RQ1/analysis/run_predict_eval.sh`；自己做推理或分析的 `research/RQ1/probe/p1_predict.sh`、`research/RQ1/probe/run_p2.sh`、`research/RQ2/quickcheck/run_p4.sh`、`research/RQ1/analysis/run_sensitivity.sh`、`research/RQ1/external/run_t19.sh`、`research/RQ1/external/t19_eval.sh`、`research/RQ1/probe/p1_predict_model.sh`、`research/RQ1/probe/p2_predict_model.sh`、`research/RQ1/analysis/validation_predict_eval.sh`、`research/RQ1/analysis/validation_prep.sh`、`research/RQ1/analysis/edema_intervention.sh`、`research/RQ1/analysis/edema_ring_probe.sh`。CPU 分区的作业本来就不启动。
- pad 异常退出时最多重启 3 次；父进程退出、收到 SIGTERM、`DISABLED` 出现时放掉显存退出。退出码：0 正常 / 开关 / 父进程退出，1 没有 GPU 或初始化失败，2 参数错误；启动失败只记日志，不影响训练。

### 执行机（DICC2、lab1，`relay.py`，10/30；10/31 起由 `feed.py` 调用）

`envs/machines.py` 的 `SITES` 里 `relay=True` 的执行机（`RELAY`：中转执行机 DICC2，ssh `dicc2`；lab 的 1 号卡 lab1）训练同一个总队列里的折。它们和别的机器不直接通信，一切经 box；那边只有按提交打的代码包，没有 git 克隆、钥匙，数据集和病例用别名。流程细节写在 `relay.py` 开头。

- **调度器**只分 Slurm、pueue 两种，读写都经 `scripts/sites/slurm.py`、`scripts/sites/pueue.py`（10/31）：那边的状态（队列、进度、`pueue status --json` 或 `squeue` 的原始输出、剩余空间、建好的环境）每次检查一次 ssh 读回，在 box 上用适配器解析；要开几个 worker 只在 `relay.topup` 算（Slurm 用 `refill.topup_plan`；pueue 组里没有活着的 worker、上一个没失败、那边有备好没人领的任务时开一个），适配器照着开。
- **根目录**写成 `~/…`（如 dicc1 的 `~/st/…`）时，命令里开头的 `~` 留给那边展开（`scripts/sites/remote.py` 的 `q()`）；代码包里 `worker.sbatch` 的日志用相对路径（Slurm 不展开 `~`）。

```
 box 的 feed.py → relay.py（健康检查每 30 分钟，refill.py 之后）
   总队列 take --worker dicc2 --assign → box wait 组 relay.py stage：换成别名（relay_names.py）、补传缺的数据块、建软链接、加进那边的队列
   那边：R/code/current 的 worker.sbatch → loop.sh（= dicc_worker.sh）→ job_run.sh（= run_task.sh），做法和 DICC 一样（显存填充、加第二个、卡死、停止、到时限交还）
   那边训完 → box wait 组 relay.py fetch（拉回、换回真名、核对验证集病例）&& archive_task.sh box（备份到 DICC）
   总队列显示训完（box 上有了）、拉回归档的任务成功了 → 交还，删掉那边的任务和结果
```

- **代码包**（`relay_bundle.py`）：从 origin/main 取那边要用的脚本，去掉注释，生成只有这一台机器的 `envs/machines.py`、`envs/policy.py` 和 `worker.sbatch`；所有 `.py` 编译、`.sh` 过 `bash -n`，环境变量名 `BRAIN_TUMOR`、`RQ1_*` 换成和领域无关的 `TASKQ_DATA`、`P1_*`（`RENAME`，10/31 起）；作业名（`tq_fit`，传输作业 `tq_xfer`）、资源（30 核、60G，`envs/machines.py` 的 `sbatch`）、常驻脚本的文件名（`FILES`）和 DICC 的分开（10/30 DICC 管理员要求）；包里出现本机路径、账号、钥匙、仓库地址就不发。内容变了才重打。要读仓库里病例子集的训练器（`SPLITS_DIR`：`n470`、`frac`）不领给它。
- **数据**：那边 `R/blobs/` 按 sha256 存，同样的内容只传一次；数据集是软链接视图，JSON 改写过（`relay_names.py`，别名表在 box `~/.cache/phd/relay/names.json`，分配后不变）。拉回时 `validation/` 的病例要和这一折的划分一致，否则不收、报出来。
- **传输**：都经隧道（`scripts/sync/tunnel.py`），在哪里收发和 DICC 一样（10/30 定）：往那边传 1 GB 以上的先交 CPU 作业（`tq_xfer`），10 分钟排不上就在登录节点下载；拉回在登录节点上传。地址用每次新开的临时 trycloudflare 地址（box 上的 cloudflared 容器，`envs/machines.py` hub 的 `quick`、`RELAY` 的 `quick_tunnel`），不用 box 的固定地址（10/30 定）。
- **个数**：`envs/policy.py` 的 `SLURM["dicc2"]`（`gpu`：QOS 和时限；`ready`：那边备好、等 worker 领的折数）。
- **状态**：box `~/.cache/phd/relay/state.json`（领着的折和别名）、`status.json`（那边各折进度，登记表显示成 DICC2）、日志 `~/relay.log`。
- **手动**：`python3 scripts/queue/relay.py stage dicc2 <key> --nice N --no-add`（只传数据，不领）；`relay.py status dicc2`；`relay.py bundle dicc2`。

## 文件

| 文件 | 内容 |
|---|---|
| `q`、`qs` | 加任务：`q <组> [pueue add 选项] -- '<命令>'`（按 `envs/machines.py` 设好卡和组的环境变量，工作目录是仓库根目录）；`qs [机器]` 看各 pueue 机器的队列 |
| `taskq.py` | 总队列（box）和执行机的缓冲：worker 领（`next`）、心跳（`beat`）、交还（`release`）；分给一台执行机（`take --assign`）；往缓冲写原样的记录（`put`，只给 `feed.py` 用）；加（`add --exp`、`add --recipe`）、改参数（`set`）、改先后（`set-nice`）、删掉没人在做的（`remove`，依赖它的要一起删）、看说明（`show`）、看队列（`status`）。命令说明在文件开头 |
| `recipes.py`、`spec.py`、`experiments.py`、`steps.py` | 做法、解析器、读实验表、步骤的"做完没"判断 |
| `run_task.sh <做法> k=v…` | 所有机器共用的运行器：激活环境、设变量、跑命令、写分段日志；已经训完就直接退出；Slurm 外设了 `CUDA_VISIBLE_DEVICES` 时卡被别人占着就等（每分钟看一次，最多 `res.wait_gpu` 分钟），出错隔 5 分钟重来、最多 3 次；SIGTERM 先停训练再退出 |
| `worker.sbatch`、`worker_start.sh`、`cpu_worker.sbatch`、`dicc_worker.sh` | DICC 作业：`worker.sbatch`（作业名见 `envs/machines.py` 的 `jobs`，long、3 天）只有资源申请和转交到 `worker_start.sh` 的一行；`worker_start.sh` 查 `ENABLED`、激活 conda、启动显存填充，交给 `dicc_worker.sh`。`cpu_worker.sbatch`：`cpu-epyc-genoa`、normal、32 核、64G、8 小时（`cpu-epyc` 的节点系统太旧） |
| `feed.py` | 送任务的唯一入口：relay 的执行机转给 `relay.py`，往 DICC、lab 的缓冲送任务（见上面）；`python3 scripts/queue/feed.py --dry` 只看会做什么 |
| `refill.py` | 补交 DICC 作业、清理 lab 的盘（见上面）；`python3 scripts/queue/refill.py` 只看会做什么 |
| `advance.py` | 训完以后的步骤（见上面）；`python3 scripts/queue/advance.py` 只看会排什么 |
| `fetch_task.sh <机器> <key>` | 等那台机器上的任务做完，把 `fetch.also` 的文件夹和 `out`（去掉 `fetch.exclude`，加上 `fetch.extra`）拉回 box，`out` 最后拉 |
| `archive_task.sh <机器> <key> [--dry-run]` | 训练机上训完、拉回以后：备份到 DICC，三份核对一致后删训练机上 `archive.drop` 匹配的文件，留 `ARCHIVED` |
| `offload_io.sh` | box 和别的机器之间传一步的输入、结果：`push <机器> <路径>…`（文件夹经 `scripts/sync/tunnel_push.sh`，文件经 `tunnel.py push`）、`pull <机器> <文件夹>`（rsync 拉回，再按校验和核对）。这里和 `fetch_task.sh`、`archive_task.sh` 的 rsync 都经 `scripts/sync/tunnel.py rsync`：`envs/machines.py` 的 `tunnel` 里有的方向文件都经隧道传（`scripts/sync/README.md`） |
| `on_boot.sh` | 开机（`@reboot`）：启动 pueued，把被打断的任务原地放回队列（nnU-Net 从存档接着训，推理跳过已做好的病例），记在 `~/on_boot.log`；`gpu_log` 的机器启动 GPU 使用记录 |
| `setup.sh`、`install_pueue.sh` | 装 pueue、启动常驻进程、按表建组（表里没有的空组删掉）、写 crontab（`on_boot.sh`，从表里的 `code` 跑；hub 的 `scripts/monitor/auto_refresh.sh`，它也负责更新 box 的代码副本、快进 lab 和 DICC 的克隆） |
| `gpu_pad.py`、`gpu_pad_start.sh` | 显存填充（见上面） |
| `relay.py`、`relay_bundle.py`、`relay_names.py` | 执行机（见上面）：接力、代码包、改名 |
| `scripts/sites/slurm.py`、`pueue.py`、`remote.py` | 调度器适配器：读作业数 / worker、交 worker 作业 / 开 worker；根目录的 shell 写法（`refill.py`、`relay.py` 共用） |
| `test_*.py`、`test_*.sh` | 测试（假训练、假队列、假快照，不连机器、不用 GPU；`test_gpu_pad_integration.sh` 用 box 的真卡）。`test_taskq.py` 的"worker 的调用方式"一组和 `test_dicc_worker.sh` 守着在跑的 worker 用的接口 |

## 约定

- **DICC 作业提交时定下的**只有 `worker.sbatch`；其余脚本在作业开始时或每次调用时从仓库读。排着的作业依赖的只有 `ENABLED` 和 `worker_start.sh` 这个路径；在跑的作业（最长 3 天）一直用开始时读到的 `dicc_worker.sh`，它调用的 `taskq.py next / beat / release` 和 `run_task.sh` 的参数、退出码不改，只加新命令。
- **退出码**：`run_task.sh` 0 训完，3 命令正常结束但没训完，2 任务说明或环境有问题，1 训练命令出错（命令自己的退出码 1–127 记在分段日志），128 以上是被信号停掉（自己收到 SIGTERM 是 143，训练命令被杀如 137 原样传出；worker 据此区分"训不了"和"被杀"）（10/25 定）。`qs --json` 每个任务的 `status` 给人看（如 `Failed (1)`），程序读 `result`（`Success` / `Failed` / `Killed` / …，没结束是空）和 `code`（退出码）。`taskq.py next` 0 领到（两行：key、`run_task.sh` 的参数），10 没有能领的，11 等排在前面刚停下的，12 队列暂停；`take` 0 领到，10 领不到。不在 box、本机又没有队列时，改队列的命令退出码 2（"总队列在 box"）；缓冲（有 `BUFFER`）上 `add`、`set`、`set-nice`、`remove`、`stop`、`resume`、`put` 要带 `TASKQ_FEED=1`（`feed.py` 经 ssh 调用时带），否则退出码 2（10/31）。
- **经 ssh 读回的 JSON**：`taskq.py --json-status`、`taskq.py show --json`、`spec.py` 的 `remote_collect` 把 JSON 写在单独一行、以 `@@JSON ` 开头，读的一方只认这一行（`spec.py` 的 `json_out` / `json_in`），前后别的输出不影响。`taskq.remote` 分开返回标准输出和标准错误，ssh 不通（退出码 255）返回空（10/25 定）。
- **各机器的状态只取一次**：`scripts/monitor/health.py` 的 `gather()`：pueue（`qs --json`）、每台 pueue 机器一次 ssh 的探查（开机、盘、GPU 使用记录、各卡和卡上的进程、训练要用的数据在不在）、Slurm 的 `squeue`（作业名、号、状态、已跑、原因、Nice、分区）和 `sacct`、任务队列、各任务的训练状态（`spec.collect`）。健康检查、`refill.py`、`advance.py` 和登记表 `make_registry.py` 都用它的结果，不另外去取（10/25 定）。
- **队列所在的机器**：`envs/machines.py` 里用 Slurm、能训练的那台（`taskq.queue_host()`）；在别的机器上跑 `taskq.py add / set / set-nice / take / show / remove / status` 时经 ssh 到那边跑。worker 用的 `next / beat / release` 不转发，没有 `tasks.jsonl` 就退出 2（10/25 定）。
- **Slurm 作业名**：`envs/machines.py` 的 `jobs`（`gpu` = `worker.sbatch`、`cpu` = `cpu_worker.sbatch` 的 `--job-name`，`test_refill.py` 核对两边一致）；健康检查只把 GPU worker 当成"还有训练作业"，`refill.py` 按 CPU worker 的名字数已有几个；查提交名额用的 QOS 来自 `envs/policy.py` 的 `SLURM`（10/25 定）。
- **代码版本**：box 的 pueue 任务的工作目录是 `~/.cache/phd/code/<提交号>`（`envs/machines.py` 的 `code`，`q` 加任务时先跑 `scripts/sync/snapshot.sh` 更新到 origin/main），任务开始后代码再更新也不影响它；旧副本没有任务用、建了 7 天以上才删。lab、DICC 跑各自克隆里的代码（`repo`），box 每 30 分钟快进。
- **跑命令、查文件**：调度脚本在本机或经 ssh 跑命令都用 `common.run`（返回退出码、标准输出、标准错误；超时、起不来是 124）；查别的机器上有哪些文件用 `common.have_script` / `parse_have`（输出以 `@@END` 结束，没有结束标记算没查到）（10/25 定）。
- **日志不重复**：`advance.py`、`refill.py` 的"会做什么"和上次一样就不再写日志，上次的内容在 `~/.cache/phd/last/<名字>.json`（`common.changed`）。
- **状态文件整份换**：`refill.json`、`tasks.json`、`last/` 下的文件、领取的 `info` 都先写临时文件再换过去（`scripts/queue/common.py` 的 `write_json`），断电、被杀时只有旧的或新的。领取只建了目录、没有 `info` 的，5 分钟后算过期。
- **同一个任务不在两处做**：总队列里先分给一台执行机（`claims/<key>/info` 的 `assigned`：不过期，只靠交还结束），缓冲里再由那边的作业领（`info`：worker、host、since、beat、ttl；20 分钟没续就过期）才做。`put` 写的记录和总队列的一样（order、nice、exp、recipe、params），`hold` 为真时是依赖记录（停止着，原因"依赖记录"，只给 `after` 看）。`skips/<key>`：哪台执行机跳过了这一折（删掉即撤销）；`released/`：刚交还的（10/31）。
- **任务表** `tasks.jsonl` 每行 `{order, nice, key, exp, recipe, params}`；档位 = `nice`，同一档按 `order`。任务说明的 `device`（默认 `gpu`）、`machines`（默认不限，如 `["dicc"]`）决定谁能领。`after`（默认 `[]`）：一组任务 key，都做完前这个任务是 `waiting`（不领、不算补交作业的数）；其中有失败 2 次的或不在队列里的是 `blocked`，健康检查报 `tqblocked:<key>`。
- **pueue 任务标签**：训练 `train:<key>`；`advance.py` 排的每折 `<步骤>:<实验 id>_f<fold>`（如 `infer:t14met_f3`），实验一级 `<步骤>:<实验 id>`。放到别的机器上的步骤在那边用同样的标签，box 上的传输是 `push:<标签>`（任务队列缺的文件）、`pull:<标签>`（10/31 以前在 lab pueue 里排的推理的拉回），传数据集给 lab 是 `sync_<机器>_<数据集>`。标签只给人看，程序不靠它认任务（见下一条）。
- **任务记录**（10/25）：box、lab 的 pueue 任务是哪一步、哪个 key 只看 box 的 `~/.cache/phd/tasks.json`（`scripts/queue/common.py`）。每条 `{machine, id, label, added, step, key}`，按步骤另有 `group`、`src`（从哪拉回）、`to`、`paths`（传给谁、传什么）、`of`（传输为哪一步）、`out`、`needs`、`train_id`（10/31 以前在 lab pueue 里排的推理）。`refill.py`、`advance.py` 加任务前先写记录（`id` 为空）、加上后补任务号；没拿到任务号的，健康检查按"同一台机器、同标签、创建时间不早于记录"找回，一小时找不到就作废。任务号和标签都对上才算同一个任务（任务号会被重用）；任务删掉后记录跟着删，有 `out` 的留到那边的结果清理完；读不到的机器的记录不动。没有记录的（手动加的）只按命令认训练（`run_task.sh`）、拉回（`fetch_task.sh <机器> <key>`）、归档（`archive_task.sh <机器> <key>`）。
- **能放到别处的步骤**：命令里有 `--stage <步骤>`，换成 `--stage needs` 时只打印要用的输入（相对数据目录，每行一个），或者步骤的 `queue` 里写了 `inputs`；结果在 `done` 判断的文件所在的文件夹里。
- **key** 由做法给，nnU-Net 是 `<数据集>/<训练器>/<fold>`（如 `Dataset111_MENrq1/nnUNetTrainer_save10_n470/0`）。**训完** = 任务说明的 `done` 都在（nnU-Net：`validation/summary.json`，和 `checkpoint_final.pth` 或归档后的 `ARCHIVED`）。
- **分段日志**：`<数据目录>/logs/tasks/<key 的文件名>__<作业>_seg<段>.log`，第一行 `作业 <作业>_<段> @ <机器>，任务=<key>，第 <段> 段，环境 <环境>，代码 <版本>，<时间>`（10/18 前的按任务说明的 `oldlogs`：`dir` 下以 `prefix` 开头的，登记表、归档两种都认）。worker 自己的日志在 `<数据目录>/logs/workers/`。

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
# 停止一个任务（10/30）：分给执行机的由 feed.py 下次检查时在缓冲里停（在训的 30 秒内停下）、交还；分给 lab:gpu0 的手动停那边的 pueue 任务；之后不再分，resume 撤销
$TQ stop --why 原因 Dataset156_METrq1_erand/nnUNetTrainer_save10/0
$TQ resume Dataset156_METrq1_erand/nnUNetTrainer_save10/0
# 手动在 lab 0 号卡训一个：先在总队列里分给 lab:gpu0（box 上跑），再在 lab 上加任务（box 上训完后 feed.py 交还）
$TQ take --worker lab:gpu0 --assign Dataset111_MENrq1/nnUNetTrainer_save10_n470/1
$Q gpu0 --label train:Dataset111_MENrq1/nnUNetTrainer_save10_n470/1 -- \
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
- **别人的任务**：写一张任务单，`taskq.py add --file job.json`，见下一节。

## 别人的任务（任务单，10/31）

帮别人训练、测试东西（不是 nnU-Net、不进实验表），写一张 JSON 任务单，在 box 上 `taskq.py add --file job.json`。一张单子就是 `command` 做法的参数（`recipes.py`），一个文件里可以放一张，也可以放一列。

```json
{
  "key": "guest/alice/resnet-a",
  "owner": "alice",
  "env": "nnunet",
  "cmd": "python3 {data}/guest/alice/code/train.py --data {data}/guest/alice/cifar --out {data}/guest/alice/resnet-a --epochs 50",
  "inputs": ["guest/alice/code", "guest/alice/cifar"],
  "out": "guest/alice/resnet-a",
  "done": ["DONE"],
  "progress": "progress_json",
  "resume": ["ckpt_*.pt"],
  "log": "*.log",
  "res": {"gpus": 1, "cpus": 8, "mem": "32G", "gpu_mem": 20},
  "nice": 120
}
```

| 字段 | 意思 |
|---|---|
| `key` | 任务名，队列里唯一；别人的任务用 `guest/<名字>/…` |
| `owner` | 谁的任务 |
| `env` | 环境名（`envs/machines.py` 的 `envs`；没有这个环境的机器不领） |
| `cmd` | 在仓库根目录用 bash 跑的命令；`{data}` 是那台机器的数据目录。重跑要能从存档接着做 |
| `inputs` | 要用的文件或文件夹（相对数据目录，box 上先放好）；送到执行机之前传完、核对过 |
| `out` | 结果文件夹（相对数据目录），做完拉回 box 的同一位置 |
| `done` | 做完的标志：`out` 下的文件，每一项都要有（一项里 `a|b` 有一个就行） |
| `progress` | `progress_json`：命令每做完一轮写 `out/PROGRESS.json`，内容 `{"done": 已做完的轮数, "total": 总轮数}`；不写 = 只看日志多久没更新；某种模型专用的读法（如 nnU-Net 的 `nnunet_log`）在 `readers/<名>.py` 的 `PROGRESS` 表里（`info` 同理，`INFO` 表），只用标准库，收集进度时随 `spec.py` 一起送到别的机器 |
| `resume` | `out` 下的存档（glob）：有一个就算有进度，失败时留在那台机器上从存档接着做，不换机器重来 |
| `log` | `out` 下的日志（glob），15 分钟没更新算没人在做 |
| `res` | 资源：卡数、CPU 数、内存、显存（GB）；`wait_gpu`（分钟）：不在 Slurm 里时卡上有别人的进程（自己账号的 worker、显存填充不算）就每分钟看一次，最多等这么久，等不到算失败（0 = 不等，不写 = 一直等） |
| `nice` | 档位（越小越先；命令行的 `--nice` 优先） |
| `relay` | 化名方式，任务单加的默认 `path`：执行机上只换路径、不动文件内容；`null` = 只在 box、lab 这类直接用仓库的机器上跑 |
| `device` | 默认 `gpu`；纯 CPU 的写 `cpu` |

命令的写法：读 `inputs` 里的文件，每一轮存一次档（先写临时文件再改名）、写一次 `PROGRESS.json`，开始时有存档就从存档接着做，最后写 `done` 里的文件。一个 PyTorch 例子：

```python
import argparse, glob, json, os, torch
p = argparse.ArgumentParser(); p.add_argument("--data"); p.add_argument("--out"); p.add_argument("--epochs", type=int)
a = p.parse_args(); os.makedirs(a.out, exist_ok=True)
model = torch.nn.Linear(10, 2).cuda(); opt = torch.optim.Adam(model.parameters())
start = 0
ckpts = sorted(glob.glob(f"{a.out}/ckpt_*.pt"))
if ckpts:
    st = torch.load(ckpts[-1]); model.load_state_dict(st["model"]); opt.load_state_dict(st["opt"]); start = st["epoch"] + 1
for ep in range(start, a.epochs):
    ...  # 读 a.data 训一轮
    torch.save(dict(model=model.state_dict(), opt=opt.state_dict(), epoch=ep), f"{a.out}/ckpt.tmp")
    os.replace(f"{a.out}/ckpt.tmp", f"{a.out}/ckpt_{ep:04d}.pt")
    for old in sorted(glob.glob(f"{a.out}/ckpt_*.pt"))[:-1]:
        os.remove(old)
    json.dump({"done": ep + 1, "total": a.epochs}, open(f"{a.out}/PROGRESS.json", "w"))
open(f"{a.out}/DONE", "w").close()
```
