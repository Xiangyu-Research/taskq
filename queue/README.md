# 任务队列和调度

训练、拉回、推理、评测、分析怎么排、怎么跑（2026-10-18 的结构，10/25 起每一步在哪里做集中写在 [`envs/policy.py`](../../../envs/policy.py)）。分工、优先级和理由见 [`admin/作业.md`](../../../admin/作业.md)，当前状态见 [`research/RQ1/实验登记表.md`](../../../research/RQ1/实验登记表.md)。

调度只做"领任务 → 换环境 → 跑命令 → 看训完没 → 拉回 → 后续步骤"，不认识具体模型。要知道的都在四个 Python 字典文件里：

| 文件 | 内容 |
|---|---|
| [`envs/machines.py`](../../../envs/machines.py) | 每台机器：数据目录、conda、环境名 → 怎么激活（`envs`）、pueue 的组、角色、要查的盘。没有某个环境的机器不领用它的任务。只放数据（`MACHINES`、`SITES`，可以有 `RELAY`）；怎么用这些数据（`whoami`、`activate`、`kinds` 等）和命令行（`whoami`、`get`、`names`、`backups`、`q-env`、`groups`、`activate`）在核心的 [`sites/mtab.py`](../sites/mtab.py)（11/4），核心里 `import mtab as M`，bash 调 `python3 $CORE/sites/mtab.py <命令>` |
| [`envs/policy.py`](../../../envs/policy.py) | 怎么跑：每个步骤在哪里做（`PLACE`）、自动的部分开不开（`AUTO`）、暂缓哪些步骤（`HOLD`）、按时段开的执行机（`SHIFT`）、执行机备几个任务（`READY`）、Slurm 补交几个作业（`SLURM`） |
| [`recipes.py`](recipes.py) | 做法：一种训练怎么跑（参数和默认值、`key`、`env`、`cmd`、`out`、`done`、`data`、`fetch`、`archive` 等，字段说明在文件开头）。核心只有 `command`（全部由任务自己给）；`nnunet`（`research/common/nnunet/train.sh`）在 `research/common/taskq_plugins/recipes.py`，按 `TASKQ_PLUGINS` 加载时合进来 |
| `research/<RQ>/experiments.py` | 实验表：做法和参数、按哪些参数展开成一组任务（`each`，如 `{"fold": ["0", "1"]}`；box pueue 标签 `<id>_f0`）、每个任务训完后的步骤（`after`）、一个实验的任务都训完后的（`group_after`）、最后一起做的（`FINAL`）、默认档位（`nice`）、分析脚本要的声明（`names`）、看板名称（`board`）。`queue/experiments.py` 读所有 RQ 的表；分析脚本按模型文件夹查（`experiments.py name-of <模型文件夹> <名字>…`），查不到就报错 |

`spec.py` 是唯一的解析器：做法 + 参数 → 任务说明（`spec.py lookup <key>` 打印某个任务的说明）。任务的输出（`out`、`done`、`fetch`、`archive`、分段日志）只由 `spec.outputs` 解释，拉回、放好、判断做完、归档都用它（`spec.py outputs <key>` 打印，11/4）。

## 结构

```
 box ~/.cache/phd/taskq/：总队列 tasks.jsonl（唯一的待办，档位 = nice）+ claims/（分给哪台执行机：assigned）
     ↓ feed.py → relay.py 替执行机（dicc1、DICC2、lab1、lab0）领（take --assign）、传数据块、加进那边根目录里的队列
 执行机：R/queue/
     ↑ 领 / 续 / 交还（taskq.py next）：同一个消费者 consume.py（插件 consumers/，dicc_worker.sh 转交）
 dicc1：worker.sbatch（GPU）、cpu_worker.sbatch 作业；DICC2：worker.sbatch     lab1、lab0：pueue gpu1 / gpu0 组里常驻一个 worker（machines.py 的 worker）
     └──────────── 都用 run_task.sh <做法> k=v… 跑：激活环境、设变量、跑命令、写分段日志 ────────────┘
                                   ↓ 训完（任务说明的 done；总队列看 box 上的结果，10/31）
 box 的 advance.py（跟着健康检查每 30 分钟）：实验表里的步骤（RQ1：推理 → 评测 → 五折分析）
     每一步在哪里做：envs/policy.py 的 PLACE，按顺序用第一个现在能用的
       box:<组>      box 的 pueue
       trainer       训这个任务的执行机（machines=[那台]，after 接训练，nice -100；lab0 训的给 lab1）；中转执行机、找不到时用下一项
       queue:<执行机>  任务队列，那台执行机的 worker 领；relay.py 连同输入送过去，做完拉回
```

pueue 在 box、lab 上只当运行器：按组限并发、留日志、断开 ssh 不中断，不用暂存（`HOLD` 除外）、优先级、`--after`。

## 开关和配置

| 在哪 | 是什么 | 作用 |
|---|---|---|
| `envs/policy.py` `AUTO["advance"]` | True / False | 训完以后的步骤真的排；False 时只把"会排什么"写进 box `~/advance.log` |
| `envs/policy.py` `READY` | 执行机 → 个数 | 备几个没人领的训练（`relay.py`）；relay 执行机备满时，总队列里有更优先的就把最靠后、备好没人领、没有进度的一个交还换它（每次检查一个，11/2；有存档的不换，11/4） |
| `envs/policy.py` `AUTO["dicc2"]`、`AUTO["lab1"]`、`AUTO["lab0"]`、`AUTO["dicc1"]` | True / False | relay 执行机（DICC2、lab 的两张卡、我的 DICC 账号）：领新的任务、补交作业 / 开 worker；False 时只记进 box `~/relay.log`，已领的任务照常推进（见下面"执行机"） |
| `envs/policy.py` `HOLD` | 任务标签的通配式列表（如 `"infer:*"`） | 对上的步骤在 box 上加成暂停的（`pueue enqueue <号>` 才跑），进任务队列的先不加 |
| `envs/policy.py` `PLACE` | 步骤 → 地方的列表 | 每个步骤在哪里做（写法见文件开头）。没写的步骤在 box 上，用实验表里写的组。已经在某处做着的不挪 |
| `envs/machines.py` `task_vars` | 机器 → {变量: 值} | 这台机器上 `run_task.sh` 跑的每个任务都带的环境变量（`spec.py shell` 设，任务自己已经设了的不改；执行机的代码包里生成的 `machines.py` 照样带）；新开始或续训的段生效。现在 lab1、lab0 是 `RQ1_GPU_AUG=1`（训练集的数据增强在 GPU 上做）、`RQ1_GPU_AUG_LOADER=1`（GPU 增强的加载改成训练进程里的线程直接写锁页内存、只传交集，`gpu_aug_loader.py`），没设时 `train.sh` 当 0；11/2 前是 `envs/policy.py` 的 `GPU_AUG`、`GPU_AUG_LOADER` |
| `envs/machines.py` `task_unset` | 机器 → (变量, …) | 这台机器上 `run_task.sh` 跑的每个任务的环境里去掉的变量（`spec.py shell` 在 `task_vars` 之前 `unset`；worker 的环境里按整个作业设、任务要自己算的，如 Slurm 作业里按整个作业设的数据增强进程数；11/4 起代替 `dicc_worker.sh` 里写死的 `unset`）；代码包里生成的 `machines.py` 照样带 |
| `envs/policy.py` `SLURM` | 每台 Slurm 执行机的 QOS、时限、个数 | `relay.py` 补交多少 GPU / CPU worker 作业（`refill.slurm_plan` 算） |
| `envs/policy.py` `SHIFT` | 执行机 → (开始, 结束, 第一个, 最后一个) | `shift.py` 跟着健康检查在时段开始时恢复执行机的 worker 组（卡上有别人的进程就等），`allowed()` 放行后 `relay.py` 才替它领任务、开 worker；结束时停 worker（等它交还），有进度的任务在总队列里改分给同一台上不按时段的执行机（`adopt` 收编存档、接着训），没开始的交还总队列，这台的队列里删掉（`unq`），那台训完后删这边的结果。每个时段只动这两次，记录在 box `~/.cache/phd/shift.json`，日志 `~/refill.log` |
| `envs/policy.py` `OFFLINE_UNTIL` | 机器 → (到期时间, 原因) | 到期前健康检查、送任务、补交作业、拉回前的查看、代码同步都不连它，`advance.py` 不往它排（`OFFLINE` 由它算出）；到期后自动恢复 |
| `envs/policy.py` `OUTPUTS` | 键 → 路径 | 调度在仓库里、本机上读写的地方（11/2）：代码包带的顶层文件夹（`relay_bundle.py`）、健康检查的状态文件夹和 ntfy 频道文件、推送标题的前缀（`health.py`、`report.py`）、各机器代码提交号的记录（`auto_refresh.sh` 写、`health.py` 读）、登记表的生成脚本、文件夹和生成文件（`auto_refresh.sh`、`report.py`）、实验表 `experiments`（`experiments.py`，glob；代码包里生成的 `policy.py` 也带）、分析计划表 `plans`（`plain.py`）、`code_dirs`（`run_task.sh`、`consume.py` 的代码版本后面标"+改动"时看的目录）、`bundle_check`（`relay_bundle.py` 另用本机哪个环境的 python 核对能编译）、`env_script`（`q` 加的任务先 source 的环境脚本）、`state`（本机的状态文件夹，11/4；没写时核心的默认 `~/.cache/taskq/`）、`logs`（`feed`、`relay`、`advance`、`refill` 的 `.log` 所在的文件夹，没写时家目录；`cfg.py log <名字>`）、`entry`（每 30 分钟的入口脚本，`refill.py` 的 KICK、`setup.sh` 写 cron 用）、`cron`（hub 的定时：(时间, `entry` 的参数)）、`origin`（`setup.sh` 在 hub 上克隆的仓库地址）。没写的键：没有实验表 / 分析计划、只看核心的 `queue/`、只用当前的 python 核对、不 source。bash 读：`python3 queue/cfg.py get <键>` |
| `envs/machines.py` `backup` | 备份机（`roles` 有 `backup`）→ 备份目录 | `archive_task.sh` 把训完的任务备份到这里（`machines.py backups`；11/3 起是 dicc1 的 `~/st/BRAIN_TUMOR`，在 /scr 上） |
| box `~/.cache/phd/taskq/ENABLED` | 文件 | 总队列的开关：没有时 `relay.py` 什么都不分 |
| 执行机 `R/queue/ENABLED` | 文件 | 那边队列的开关：没有时排着的 worker 作业开始后直接结束（`worker_start.sh`，只在 worker 开始时查；在跑的 worker 照样领）；`relay.py` 建队列时放上 |
| 执行机 `R/queue/gpu_pad_DISABLED`（box 等非执行机：`~/.cache/phd/gpu_pad/DISABLED`） | 文件 | 有时不填显存，在跑的填充 10 秒内放掉退出 |
| `TASKQ_GPU_PAD=0` | 环境变量 | 单个作业 / 脚本不填显存（名单见下面"显存填充"）；`gpu_pad*` 的参数 `TASKQ_GPU_PAD_*` |
| `TASKQ_DATA` | 环境变量 | 本机的数据目录（`run_task.sh`、`consume.py`、`taskq.py` 先读它，没有再查 `envs/machines.py`；`envs/nnunet_paths.sh` 也是这样设） |
| `TASKQ_STATE` | 环境变量 | 本机的状态文件夹（11/2）：设了时任务队列、`relay/`、`last/`、`tasks.json`、`envs/`、隧道、显存填充的开关、`code/`（`snapshot.sh`）都在它下面（`<它>/taskq`、`<它>/gpu_pad/DISABLED`…）；没设时在 `OUTPUTS["state"]`，再没有就是核心的默认 `~/.cache/taskq/`（`cfg.py` 的 `STATE_HOME`、`STATE_PLACES`，Python 经 `cfg.state()`，bash 经 `cfg.py state <名字>`）。`TASKQ_DIR`、`TASKQ_GPU_PAD_SWITCH` 等单项的变量优先。执行机的代码包把这些默认写法换成根目录下的 `queue/`、`state/` |
| `TASK_WAIT_GPU` | 环境变量 | 不在 Slurm 里时卡被占最多等几分钟：任务单的 `res.wait_gpu` 经 `spec.py` 给 `run_task.sh`；pueue 组的默认值在 `envs/machines.py` 的 `groups`（box `gpu` 组 600），手动加的推理（`predict.sh`）也读它 |
| `TASKQ_*` | 环境变量 | 全部登记在 `proto.py` 的 `ENV`：谁设、什么意思、`run`（生产里会设，如 `TASKQ_DIR`、`TASKQ_DATA`、`TASKQ_CPUS`、`TASKQ_GPU_PAD_SWITCH`、`TASKQ_ZOMBIES`）/ `tune`（阈值，默认值在 `consume.py` 的 `Worker.__init__`或 `gpu_pad.py` 的命令行）/ `test`（只给测试）；`test_protocol.py` 核对代码里出现的都登记了 |
| `.taskq.toml` | 项目根目录的文件 | 找项目（11/4，`project.py`）：`--project` > `TASKQ_PROJECT` > 从当前目录往上找 `.taskq.toml`；里面写 `config = "envs"`、`plugins = ["research/common/taskq_plugins"]`（相对项目根）。核心的仓库根 `REPO`：`TASKQ_REPO` > 核心在 `<仓库>/scripts/taskq` 里（代码包）时那个仓库 > 找到的项目 > 核心本身（`cfg.py`；bash 脚本不嵌着、没设 `TASKQ_REPO` 时 `eval "$(python3 $CORE/queue/project.py sh)"`）。仓库根有 `.taskq.toml` 时配置和插件按它，没有时用下面两行的默认 |
| `TASKQ_CONFIG` | 环境变量 | `machines.py`、`policy.py` 所在的文件夹，默认 `<仓库>/envs`（11/2）。Python 脚本经 `queue/cfg.py` 的 `use_config()` 找，机器表经 `sites/mtab.py`；`relay_bundle.py` 在它在仓库里时取提交里的 `machines.py`，在仓库外时读文件；执行机的代码包里生成的两个文件照旧在 `envs/` |
| `TASKQ_PLUGINS` | 环境变量 | 领域专用的插件文件夹（`:` 隔开几个，空串 = 没有），默认 `<仓库>/research/common/taskq_plugins`（11/2）。每个文件夹里可以有 `recipes.py`（`RECIPES`、`HOOKS`）、`relay_plugins.py`（`PLUGINS`、`ACCEPTS`、`VIEW_ROOTS`）、`readers/<名>.py`（`PROGRESS`、`INFO`、`TRAINED`，只用标准库）。每类只取这些名字（`cfg.py` 的 `KINDS`，11/4），合进核心的同名表（键重复报错），插件的别的名字不进核心；插件要用核心的东西只 import `taskq_api.py`（`REQUIRED`、`RelayError`…见它开头），不 import 核心的其他模块。钩子：`HOOKS["model_of"]`（模型文件夹 → 训它的任务的 `model` 参数，`experiments.py` 的 `name-of`、`env-of` 用它，核心不认 nnU-Net 的文件夹布局） |

`envs/policy.py` 由 box 的健康检查每次读，在 `~/auto/PhD`（只给 cron 用的克隆，跟着 origin/main）里生效，所以改了要提交推送。写错了（不认识的地方、条件、步骤）时 `advance.py` 这次什么都不排，报到健康检查里。

## 出问题时看哪里

| 问题 | 看 |
|---|---|
| 哪个任务没人做、分给了哪台、失败了几次 | `taskq status`（box 上跑，读总队列；`taskq status --all` 各机器汇总）；执行机（dicc1、DICC2、lab1、lab0）：`taskq site status <名字>`；DICC 作业的 `.out`，lab1、lab0 worker 的日志在各自根目录的 `data/logs/workers/` 和 lab 上的 `pueue log <worker 的任务号>`；失败记录在执行机根目录的 `queue/failures/`，训不了跳过的在总队列的 `skips/` |
| 执行机（dicc1、DICC2、lab1、lab0）领了什么、传了哪些数据、清理了哪些、开了 worker、补交了哪些 DICC 作业、拉回 | box `~/relay.log`（`~/feed.log` 只有指过去的一行）；`taskq plan`（只看会做什么）、`taskq site status <名字>` |
| 按时段的执行机什么时候开、停 | box `~/refill.log`（`shift.py`） |
| 训完之后卡在哪一步、为什么没排 | box `~/advance.log`（排了什么、会排什么、"没排"的原因、哪一步失败停住）；`taskq plan`（只看会排什么）；`pueue log <号>` |
| lab 训的任务的推理走到哪 | 总队列的 `taskq.py show infer/<key>`、`relay.py status lab1`；拉回看 box 上 `relay_fetch:…` 的 `pueue log`；box `~/advance.log` |
| 某个 pueue 任务被认成哪一步 | box `~/.cache/phd/tasks.json`（见"约定"的任务记录） |
| 总览、报警 | `research/RQ1/实验登记表.md` 最上面的"需要处理"（`monitor/health.py`） |

## 组

| 机器 | 组 | 同时跑 | 放什么 |
|---|---|---|---|
| box | `gpu` | 1 | 4070：PLACE 写了 `box:gpu` 的步骤、手动的推理 |
| box | `cpu` | 1 | 评测、五折集成评测、删单折概率图、分析（`oof_eval.py`、`paired_compare.py`、`compare_all.py` 等）。一次一个（10/26 定）：10/25 两个大任务加上 box 上别的服务用光了 61 GB 内存 |
| box | `wait` | 不限 | 拉回（`relay.py fetch`）、归档（`archive_task.sh`）、传输（`relay.py stage`、`tunnel_push.sh`） |
| lab | `gpu1` | 1 | 1 号卡：执行机 lab1 常驻的 worker（代码包里的 `consume.py`，`envs/machines.py` 的 `worker`），领 `relay.py` 替它领来的训练和推理（lab 训的任务，含 0 号卡训的） |
| lab | `gpu0` | 1 | 0 号卡：执行机 lab0 的 worker（10/31 晚起，`SHIFT` 的时段里由 `shift.py` 开组、`relay.py` 替它领任务） |
| lab | `cpu`、`wait` | 1、不限 | 备用 |

## 怎么运转

### 消费者（worker，`consume.py`，11/5）

领任务（`relay.py` 替执行机在总队列领、放进那边的队列）和消费任务分开：执行机上只有一个消费者核心 `consume.py`，在本机队列里领（`taskq.py next`）、开 `run_task.sh`、收退出码、心跳、停、交还；各机器不同的地方写成插件（`consumers/`）。`dicc_worker.sh` 只转交给它（文件名留着：`worker_start.sh`、代码包里的老入口、测试和手动跑都用）。

- **钩子**（插件是模块或 dict，有哪个写哪个）：`active(w)` 这台用不用；`setup(w)` 开始时定 W、slots、时限等；`admit(w)` 领之前问一句，返回不领的原因（`None` 放行）；`env(w, env)` 改任务的环境；`before_start(w)` 开训前（如给显存填充让位）；`tick(w)` 每一轮；`idle(w, why, now, idle_since, err_since)` 领不到时（`why`：`nothing`、`wait`、`paused`、`error`、`admit`、`own`、`zomb`）睡几秒，`None` = 退出，第一个定义了的插件说了算；`on_exit(w)` 退出前。
- **内置插件**（按顺序）：
  - `slurm`（有 `SLURM_JOB_ID`）：W = 作业号；按分到的 CPU 和内存算 slots（≥24 核且 ≥56G 时 2 个）；离时限不到 1 小时不领；空闲 40 分钟退出；节点本地暂存目录交给 `run_task.sh`（`TASKQ_STAGE_ROOT`，结束时删）；被 SIGTERM 退出码 143（多半是到时限）。
  - `resident`（pueue 常驻，不在 Slurm 里）：W = `WORKER_ID`；`TASKQ_CPUS` 限 CPU；slots 取 `TASKQ_SLOTS` 或机器表的 `slots`；`IDLE_POLL` 隔多久再领、`IDLE_EXIT` 空等多久退出（领不到不马上退出）；每次开训另起日志名、`--log-tag`；代码换了、手上没有任务时退出，由 `relay.py` 用新代码重开。
  - `gpu_pad`（有显存填充时）：开训前让填充让出位置，算显存时扣掉填充占的。
  - `window`（站点在 `envs/policy.py` 的 `SHIFT` 里）：站点名取 `TASKQ_SITE`（`sites/pueue.py` 开 worker 时加上），没有就按 `WORKER_ID` 在机器表里反查；时段外不领；时段结束时停下手上的、交还，退出 0（box 的 `shift.py` 照旧 pause 组、交班，作为兜底）。
  - `gpu_idle`（不在 Slurm 里、设了 `CUDA_VISIBLE_DEVICES`）：领之前看卡：卡上有别人的进程、或组的 `TASK_GPU_IDLE` 分钟内 GPU 使用记录里有别人，就不领，空等时在 worker 日志里记一句（状态变了或每 30 分钟）。`consume.py` 开 `run_task.sh` 时设 `TASKQ_ADMITTED=1`，`run_task.sh` 见到就不再自己等卡；手动跑的照旧等。
  - `gpu_share`（GPU 任务）：第二个任务什么时候领：最近一个开训 15 分钟后、近 10 分钟使用率低于 90%、除去填充还空着一半显存。
- **使用者的插件**：插件文件夹里的 `consumers.py` 导出 `CONSUMERS`（列表，`cfg.KINDS`），排在内置插件后面。
- **换代码包**：代码包换了（`code/current` 变了；Slurm 里也换）、并且这一轮要去领新任务（有空位、`admit` 放行）时，先跑新包的 `consume.py --selftest`（加载插件、读 policy、`bash -n` 几个脚本、交接 JSON 来回一次），过了就 `exec` 新包的 `worker_start.sh`，进程号不变，在跑的训练还是它的子进程。交接放在 `TASKQ_HANDOFF`（JSON，格式 `proto.HANDOFF_TOP` / `HANDOFF_TASK`：W、在跑的任务的 pid、会话、key、token、开始时刻、日志、停不下来的、本作业不再领的、失败计数、LATER…），新进程接着心跳、用 `os.waitpid` 收退出码。自检没过就留在旧包、日志里记一句，这个包不再试。新包里没有 `consume.py` 时（往回换）只在手上没有任务时换，停不下来的用老格式 `TASKQ_ZOMBIES` 交过去；11/5 前的 bash worker 带 `TASKQ_ZOMBIES` exec 过来时 `consume.py` 照样认。`worker_start.sh` 带着交接再跑一遍时不查 `ENABLED`，显存填充不开第二个（`TASKQ_GPU_PAD_STARTED`）。
- `next` 的 stderr 直接读回内存（不再写 `worker_<W>_next.err`）；停一个任务先停它的整个会话 / 进程组。

下面是 Slurm 作业里的做法（`slurm`、`gpu_pad`、`gpu_share` 三个插件 + 核心）。

#### DICC 作业

作业只负责申到卡，开始后从任务队列按档位、先后领本机有环境、`machines` 允许、`device` 对得上的任务，用 `run_task.sh` 在后台跑：

- 先跑 1 个；最近一个开始 15 分钟后，近 10 分钟 GPU 平均使用率低于 90%、除去显存填充还空着一半显存，再领 1 个（最多 2 个）。nnU-Net 这类 CPU 跟不上的自然变成 2 个，GPU 跑满的只跑 1 个。CPU 少于 24 个或内存少于 56G 的作业只跑 1 个。
- 跑完一个领下一个；离时限不到 1 小时不再领；到时限时把没做完的交还，下一个领到的从存档接着训。
- 只领本机队列里的（`relay.py` 送来的）。不领：已做完的、别处在做的（领取在有效期内续过：20 分钟；或训练日志 15 分钟内更新过）、失败过 2 次的、依赖记录（只给 `after` 看的）。作业开始时排在前面的任务刚停下，先等它最多 20 分钟。
- 出问题时不让排队的作业空跑掉：1 小时内有 2 个任务开始 10 分钟内就失败，这个作业不再领；任务开始满 30 分钟后日志和结果文件夹都 30 分钟没变化，当作卡住，停掉、交还（不算失败），并在它的分段日志里记一行"当作卡住"，一周内第 2 次起每次记一次失败（11/4 定：CPU 任务这类长时间不出声的也会被误判，按次数累计成失败，健康检查才报得出来，不会被一个个作业反复领、反复停）；整个队列 3 小时内失败 3 次就暂停领任务；停止的任务（`taskq.py stop`）每 5 秒看一次，先发 SIGTERM，20 秒没停就 SIGKILL，交还（不算失败），30 秒内做完（存档是原子写的）；手上没有在跑的连续 40 分钟就结束（gpu-a100 使用率连续 1 小时低于 40% 会被取消）；队列读写出错 10 分钟就结束。
- 每个训练 14 个数据增强进程（作业 32 个 CPU）。每个训练分到的 CPU 在作业开始时按最多同时几个定下（`TASK_CPUS` = CPU 数 / 最多几个），只训 1 个时也只用一半：数据增强的进程数在训练开始时定、之后改不了，按全部 CPU 开的话第二个加进来时就超卖（11/4 定，不改）。
- 停不下来的（11/4）：`run_task.sh` 用 `setsid` 让训练自成一个会话，停的时候 `run_task.sh` 等 8 秒强制停后自己退出，训练卡在读写上时可能还在。worker 按会话认（`pgrep -s`）：停了 3 分钟还没停下的空出位置领别的，但接着给它的领取心跳，真停下了才交还；不在 Slurm 里的 worker 有这种训练时不因空闲退出；代码包换了时连同在跑的一起交给新代码（`TASKQ_HANDOFF`）接着跟。
- 心跳用 `taskq.py beat --strict`（11/4）：领取已经不是自己的（过期后被别人接管、已经没了）时退出码 13，worker 停掉这个训练、不交还，本作业不再领它，免得两处训同一折。作业被停时交还用 `release --lock-wait 5`，锁拿不到就算了（领取 20 分钟后过期），不耽误 Slurm 的 KillWait。box 的 `relay.py` 续约也用 `beat --strict`（11/5）：丢了的 key 这一轮不再替它传、拉回、删、交还。
- 只有 `run_task.sh` 被停掉、worker 还在（11/5）：退出码 143（`run_task.sh` 收到 SIGTERM，训练已经停下）→ 交还，不记失败，本作业不再领它；128 以上、训练所在的会话还在（`run_task.sh` 被 SIGKILL）→ SIGTERM 那个会话，接着心跳，真停下了才交还，不记失败。训练命令自己被杀（如 OOM 的 137）、会话已经不在的照旧记失败。
- `next` 领了、还没打印就收到 SIGTERM / SIGINT / SIGHUP（11/5）：撤掉这次领取再按信号退出，不留到过期。SIGKILL 照旧要等领取过期。
- `run_task.sh` 退出码 5（读不了盘，判断不了训完没有）：交还、不算失败，本作业 10 分钟后再领；退出码 2（任务说明、环境有问题）记失败时带上分段日志里的原因。
- 开训前 `run_task.sh` 查任务的输入（`inputs.py prepare`：文件都在、链接指得到文件、没有建到一半的 `.incomplete`）；缺了退出码 4，worker 用 `taskq.py missing` 交还、记进 `inputs/<key>`（状态 `input`，不算失败，补齐前没人领），`relay.py` 补传缺的数据块后 `input-ok` 放回（11/1）。
- 输入拷到节点本地盘（`envs/policy.py` 的 `STAGE_TMP` 开着、任务 `res.local_inputs` 时，11/1 起代替 `stage_tmp.sh` 开训前的整份拷贝）：在 `$TMPDIR/phd_stage_<作业号>/views/<任务>/` 建这个任务私有的链接树，先都指向共享盘，马上开训；后台 `inputs.py copy` 一个个拷到 `blobs/`、核对（sha256 名的按内容）后原子换掉链接，同一作业、本节点别的作业已有的直接用或硬链接；拷完要剩 `STAGE_KEEP_GB`（默认 300）GB，不够就不拷。命令从 `$TASK_INPUTS_ROOT` 读；作业结束时删自己的目录，下一个作业开始时清掉本节点上已结束作业留下的。
- CPU worker（`cpu_worker.sbatch`，`WORKER_DEVICE=cpu`）只领 `device=cpu` 的任务，一次 1 个，领不到就结束。

### 往执行机送任务（`feed.py`，健康检查每 30 分钟调用，10/31）

送任务只有一个入口：健康检查调用 `feed.run_all`，它把 `envs/machines.py` 的 `SITES` 里 `relay=True` 的执行机（dicc1、DICC2、lab1、lab0）交给 `relay.py`（见下面"执行机"，细节记 box `~/relay.log`，`~/feed.log` 里记一行指过去），并报总队列里分给了不是执行机的名字的任务（`feed:nobody:<名字>`）。`advance.py` 在它之后跑，往总队列加的任务下次检查送过去。`taskq plan` 只看会做什么。

### 训完以后（`advance.py`，健康检查每 30 分钟调用）

每次对每个任务看盘上的文件，算出它走到哪一步，下一步没在做就排上：

执行机上训完的任务由 `relay.py fetch` 拉回 box（见下面"执行机"）。

1. 实验表里每个任务的步骤（RQ1：推理 → 评测），按顺序做第一个没做完的。
2. 一个实验所有任务都在 box 上训完 → `group_after` 的每一串步骤（RQ1：oof、配对比较、五折集成、删单折概率图、P1 / P2 重跑）；最后 `FINAL` 的（`compare_all.py`，有比它上次开始时更新的结果就重跑）。

每一步放在哪：已经在某处做着（box pueue 里有、任务队列里有、任务记录里有那边的）就接着那边的；否则按 `PLACE[步骤]` 的顺序用第一个现在能用的，都用不了时把原因写进 `~/advance.log`（"没排 …"），不报成问题。推理的第一个地方是 `trainer`：训这个任务的执行机（dicc1、lab1；lab0 训的算 lab1，`advance.py` 的 `site_of`），是 DICC2 时用下一项。`queue:<执行机>` 只能写 hub 或 `RELAY` 里的执行机。

- 进任务队列的：`relay.py` 领过去时连同输入传过去；那边做完后 `relay.py fetch` 拉回，拉回后照常按 `done` 判断，拉回在跑时不算做完。
- 失败过的不自动重排：最后一次成功之后有失败的任务，或者成功跑完了结果还是不对，就报到健康检查，看过、处理好以后 `pueue remove` 那个任务，下一次就会重排。拉回一样。
- 提前排（10/25，10/31 改看总队列的分配）：总队列里分给 dicc1、lab1、lab0 的任务，进任务队列的每个任务步骤（推理；dicc1 的还有评测）现在就加进总队列，`after` 接上前一步，`relay.py` 跟着送到那台（lab0 训的推理给 lab1），worker 训完一个任务先领它的推理（nice -100），dicc1 推理完马上有 CPU 作业评测。lab1 的 AUTO 关着时 lab0 训的任务不提前排。分给 DICC2 的和没分出去的任务等训完。
- box 上排的任务做完后马上跑一次健康检查（`auto_refresh.sh kick`，和定时的共用一把锁，最多等 5 分钟），下一步几分钟内排上。
- 判断做完看盘上的文件：推理要每个测试集的分割、概率图、输入病例数都对得上（盘上的文件数和 `MANIFEST.json` 记的一样）；结果还在往 box 上拷（拉回、传输在跑）时不算做完、不清理那边。

### 显存填充（DICC，`gpu_pad.py`）

`gpu-a100` 要求作业显存占用 >= 40 GB（10/17 收到）。`gpu_pad.py` 只占显存、不计算，按 nvidia-smi 的 `memory.used`：作业开始起（11/4 前是第一次有训练起），任何时候不低于 **42000 MiB**（floor），第一个训练开训前也一样先放到 floor；训练占的显存 120 秒内变化不超过 1 GiB 后补到 **55000 MiB**（target）；超过 **62000 MiB** 放掉多出的。每次补、放 1 GB，给训练留至少 8 GB；总显存不到 60 GB 时不填。训练占的显存不到 2000 MiB 算没有训练。开始新训练时 `consume.py`（插件 `gpu_pad`）删掉 `/tmp/gpu_pad_<作业号>.ready`、touch `.start`，pad 放到 floor 后写 `.ready`，worker 看到再开训（最多等 30 秒），新训练稳定后再补到 target（最多等 900 秒）。

- 默认开启：`gpu_pad_start.sh`（`worker_start.sh` 直接调用，其他作业经 `envs/nnunet_paths.sh`）在所有 Slurm GPU 作业里启动它。日志 `<数据目录>/logs/workers/gpu_pad_<作业号>.log`。
- `TASKQ_GPU_PAD=0` 的脚本：启动前要求卡是空的 `research/common/nnunet/smoke/run_gpu_smoke.sh`、`research/common/nnunet/smoke/epoch_bench.sh`、`research/RQ1/nnunet/predict.sh`、`research/RQ1/analysis/run_predict_eval.sh`；自己做推理或分析的 `research/RQ1/probe/p1_predict.sh`、`research/RQ1/probe/run_p2.sh`、`research/RQ2/quickcheck/run_p4.sh`、`research/RQ1/analysis/run_sensitivity.sh`、`research/RQ1/external/run_t19.sh`、`research/RQ1/external/t19_eval.sh`、`research/RQ1/probe/p1_predict_model.sh`、`research/RQ1/probe/p2_predict_model.sh`、`research/RQ1/analysis/validation_predict_eval.sh`、`research/RQ1/analysis/validation_prep.sh`、`research/RQ1/analysis/edema_intervention.sh`、`research/RQ1/analysis/edema_ring_probe.sh`。CPU 分区的作业本来就不启动。
- pad 异常退出时重启，整个作业期间都会（间隔 30 秒的 1、2、4… 倍，最多 16 倍）；连续 5 次跑不到 10 分钟就退出的不再重启，`!!` 开头的一行写进 worker 日志（11/4）；父进程退出、收到 SIGTERM、`DISABLED` 出现时放掉显存退出。退出码：0 正常 / 开关 / 父进程退出，1 没有 GPU 或初始化失败，2 参数错误；启动失败只记日志，不影响训练。

### 执行机（dicc1、DICC2、lab1、lab0，`relay.py`，10/30；10/31 起由 `feed.py` 调用）

`envs/machines.py` 的 `SITES` 里 `relay=True` 的执行机（`RELAY`：DICC 的 dicc1、DICC2，ssh `dicc2`；lab 的 1 号卡 lab1、0 号卡 lab0）领同一个总队列里的任务：GPU 任务，以及有 CPU worker 的执行机领纯 CPU 任务（11/1，`envs/machines.py` 的 `kinds()`：Slurm 类看 `jobs` 里有没有 `cpu`，pueue 类看 `cpu_worker`；lab1 的 `cpu` 组）。它们和别的机器不直接通信，一切经 box；那边只有按提交打的代码包，没有 git 克隆、钥匙；数据和结果在那边的数据目录里用和 box 一样的相对路径。流程细节写在 `relay.py` 开头。

- **调度器**只分 Slurm、pueue 两种，读写都经 `sites/slurm.py`、`sites/pueue.py`（10/31）：那边的状态（队列、进度、`pueue status --json` 或 `squeue` 的原始输出、剩余空间、建好的环境）每次检查一次 ssh 读回，在 box 上用适配器解析；要开几个 worker 只在 `relay.topup` 算，每种（gpu、cpu）各算各的（Slurm 用 `refill.slurm_plan`；pueue 这种 worker 没有活着的、上一个没失败、那边有备好没人领的这种任务时开一个），适配器照着开。
- **根目录**写成 `~/…`（如 dicc1、DICC2 的 `~/q`）时，命令里开头的 `~` 留给那边展开（`sites/remote.py` 的 `q()`）；代码包里 `worker.sbatch` 的日志用相对路径（Slurm 不展开 `~`）。
- **DICC 的两个执行机根目录在 home**（11/2 定）：`/scr` 11/2 卡住过（读不了队列、hash 文件报 "Communication error on send"），home 每个账号 100 GB。`cap`（`envs/machines.py`）是根目录最多占多少 GB，剩余按 cap 减去根目录已占的算（`relay_remote.py` 的 `STATUS_PY`，硬链接只算一次、软链接不算），清理（`relay_store.py` 的 `gc`）、领任务（`fill`）都用它。数据都从 box 传，不收编 `/scr` 上的老数据（跨盘不能硬链接）。实验表 `queue` 的 `disk`（这一步大约要占多少 GB）加上 `budget` 的第二项超过 cap 的步骤，`advance.py` 不放这台，用 `PLACE` 的下一项：推理（输入约 60 GB、输出约 14 GB）放不下，DICC 训的折拉回 box 后由 lab1 推理；评测（约 15 GB）照旧在 dicc1。
- **AUTO 关着 = 这台整个停下**（11/2）：不领新的、不传、不重传、不让那边接着训、不交作业，只续已有的领取、把训完的拉回、交还；`advance.py` 也不往它排步骤。
- **传数据排不上 CPU 作业**（收编在 CPU 节点上做的执行机）：`relay.py stage` 退出码 75，box 上显示 Failed，下次定时检查再传、不报；领了 3 小时以上还排不上才报 `site:<名>:stage`。
- **Slurm worker 开始时没有任务**：和等着的一样空等到 `IDLE_MAX`（40 分钟），这期间有任务进来就领；一直没有才结束（11/2；之前马上退出，排了几天的作业白白退出）。纯 CPU 的 worker 照旧领不到就结束。

```
 box 的 feed.py → relay.py（健康检查每 30 分钟）
   总队列 take --worker dicc2 --assign → box wait 组 relay.py stage：插件列出输入（sites/names.py）、补传缺的数据块、建软链接、加进那边的队列
   那边：R/code/current 的 worker.sbatch → worker_start.sh → consume.py → run_task.sh，做法和 DICC 一样（显存填充、加第二个、卡死、停止、到时限交还）
   那边训完 → box wait 组 relay.py fetch（拉回、插件核对，如验证集病例）
   总队列显示训完（box 上有了）→ 删掉那边的任务和结果；要归档的另排 box wait 组 archive_task.sh box（标签 relay_archive，备份到备份机），
   归档成功后交还（11/3 定：执行机的盘只用来中转，不等归档）；备份机暂停检查（OFFLINE）时不归档、直接交还，只是这次连不上时等
```

- **路径的引用计数**（11/1）：`state.json` 每个任务记下它放的视图、输入和产出（`outs`）。删一个任务时只删别的记录不用的路径（`relay.droppable`），如 lab1 训完的模型正被推理引用就留着；一个任务的输入和同一台上没做完的任务的产出重叠时不传（`check_overlap`）。记录带 `redo` 的，下次检查先删掉那边的任务和路径（那边在跑就先停），再按现在的做法重新传。

- **代码包**（`relay_bundle.py`，11/4 起只装执行机要用的、只按内容命名）：origin/main 里执行机要用的调度文件（`relay_bundle.py` 的 `RUNTIME`：worker、`run_task.sh`、`taskq.py`、`spec.py`、做法和实验表的读法等）加 `envs/policy.py` 的 `OUTPUTS["bundle"]`（如 `envs/`、`research/`）下的代码，减去 `OUTPUTS["bundle_skip"]`（测试、`testdata/`、box 才用的 `make_registry.py`、`relay_plugins.py`）（`.py .sh .R .sbatch`，1 MB 以内的 `.json .csv .txt .yaml .yml`）。11/4 起使用者的文件取自使用者仓库的 origin/main，调度核心的文件取自装好的核心自己的仓库的 HEAD（使用者仓库里不带核心），在包里总放在 `scripts/taskq/`（`proto.BUNDLE_CORE`，和 box 上核心装在哪无关）；切换前交的 Slurm 作业还在用的旧入口（转交 `scripts/queue/worker_start.sh`、`dicc_worker.sh`，`relay_bundle.py` 的 `COMPAT`；软链接 `start.sh`、`loop.sh`，`LINKS`）由核心生成、每个包都有，`scripts/taskq/VERSION` 记核心的提交号（不算进包名），使用者仓库根目录有 `.taskq.toml` 时带上；`bundle_for` 的键是 `<名字>:<提交>:<核心的提交>`。生成只有这一台机器的 `envs/machines.py`、`envs/policy.py`，Slurm 类按 `jobs` 生成 `worker.sbatch`（gpu）、`cpu_worker.sbatch`（cpu）；所有 `.py` 用两个 Python 编译、`.sh` 过 `bash -n`，有像钥匙的内容（私钥、常见 token）就不发。资源（30 核、60G，`envs/machines.py` 的 `sbatch`）和 DICC 的分开（10/30 DICC 管理员要求）；sbatch 里不写作业名。包名 = 内容哈希前 16 位（不带提交号；哪个提交建的记在 box 上 relay 状态的 `bundle_for`），其余文件内容原样（11/1 起不改名），只有生成的 `machines.py` 去掉开头的说明和整行注释；只改 box 的脚本、说明、测试时四台不换包。读不到状态、但那边的 `code/current` 和这边要推的包不同时也推一次再读（改了状态程序后旧包可能跑不了它，11/4）；部署被打断留下的 `<包名>.tmp.*` 1 天后由下次部署删。要读仓库里病例子集的训练器（`SPLITS_DIR`：`n470`、`frac`）不领给它。
- **任务说明在 box 上解析好**（11/4）：`relay_transfer._stage` 把 `spec.resolve` 的结果（去掉 `machines`、`after`、`names`、`exp`，`{data}`、`{repo}` 留给那边展开）经 `taskq.py add --spec-stdin` 放进那边的队列；记录里 `recipe`、`params` 也留着（还在跑旧包的 Slurm worker 照旧能领）。那边 `next` 对带说明的记录给 `run_task.sh --spec <JSON>`，`spec.py` 不再查做法和插件。做法改了以后 `relay.py resend <名字>` 把传过去的任务按现在的做法重新解析、换掉那边的说明（`--update-only`，不加新任务）。拉回、收编存档用的输出（`done`、`exclude`、`extra`）也由 box 按 `spec.outputs` 算好发过去。
- **数据**：那边 `R/blobs/` 按 sha256 存，同样的内容只传一次；数据目录里是指向 blob 的软链接，路径和 box 一样，文件内容不改；要哪些文件由做法的 `relay` 插件列（`sites/names.py`、`relay_plugins.py`；nnU-Net 的在 `research/common/taskq_plugins/relay_plugins.py`）。拉回时插件核对（训练：`validation/` 的病例和这个任务的划分一致；推理：各测试集病例齐、checkpoint 的 sha256 和 box 上的一样），对不上不收、报出来。
- **传输**：都经隧道（`sync/tunnel.py`），在哪里收发和 DICC 一样（10/30 定）：往那边传 1 GB 以上的先交 CPU 作业，10 分钟排不上就在登录节点下载；拉回在登录节点上传。地址用每次新开的临时 trycloudflare 地址（box 上的 cloudflared 容器，`envs/machines.py` hub 的 `quick`、`RELAY` 的 `quick_tunnel`），不用 box 的固定地址（10/30 定）。同一台机器（按 ssh 名字，lab1、lab0 算一台）的传输一个接一个，不同机器的同时传（11/1 定）；box 上共用一个文件服务进程（`tunnel.py _serve`，第一个要用的传输起它，挂了下一个再起，没有会话 5 分钟后退出），每次传输是它的一个会话，会话、`serve.pid`、`serve.log` 在状态文件夹的 `tunnel_srv/`（11/5）。
- **个数**：`envs/policy.py` 的 `READY[名字]`（那边每种 worker 备好、等领的任务数）、`SLURM["dicc2"]`（`gpu`：QOS 和时限）。
- **盘**：有 `budget` 的剩不到软线时由 `relay_store.py` 的 `gc` 清理：先删总队列不再要的视图（和建视图拿同一把锁 `R/state/view.lock`），再按最久没用删没人指向的 blob；领任务时传完至少要剩硬线。`R/blobs` 是指到别的盘的软链接时（DICC 的 `/scr`，11/4）照样清理，只删 `blobs/<前两位>/` 下的块文件，不跟随别的链接。传数据期间钉住要传的块（`R/state/pins/`），每小时重写一次，传完撤掉；清理只认 12 小时内写过的。收编只认和块目录同一个盘的文件（跨盘不能硬链接，不读、不算 sha256）。别的执行机里训到一半的存档，那台的队列里还领着这个任务（心跳没过期）时不拷，下次检查再来（存档可能写到一半，11/4）。
- **状态**：box `~/.cache/phd/relay/state.json`（领着的任务）、`status.json`（那边各任务进度，登记表显示成 DICC2）、日志 `~/relay.log`。
- **手动**：`taskq internal stage dicc2 <key> --nice N --no-add`（只传数据，不领）；`taskq site status dicc2`；`taskq site bundle dicc2`。

### 环境的锁（`sites/envlock.py`，11/5）

`taskq env lock <名> [--variant V]`（`--all`：每个描述 × 机器表里用到的变体，加 default；基础环境先锁）把 `<配置文件夹>/specs/<名>.yml` 解成 `<名>.<变体>.lock.json`（放在描述旁边；不叫 `.yml`，描述的 glob 不会当成描述），每个文件下到状态文件夹的 `wheelhouse/<sha256 前 2 位>/<sha256>`（`proto.WHEELHOUSE`、`proto.wheel`，同 relay 的 blob），下完核对 sha256。只解不装：

- **conda**：`conda create --dry-run --json`，`CONDA_OVERRIDE_GLIBC` = glibc 上限（`envlock.GLIBC_FLOOR`，2.34 = DICC 的 Rocky 9.5；项目的 `.taskq.toml` 写 `glibc_floor = "…"` 可以改），描述写了 channels 时总带 `--override-channels`；`CONDA_PKGS_DIRS` 是只放 repodata 的 `<状态文件夹>/envlock/conda-pkgs`，所以每个包都在 `FETCH` 里、带 url、md5、sha256。顺序照 `LINK`。
- **pip**：按建环境的顺序分步（torch 变体只用它的 `index_url`，再 `pip`，再 `pip_no_deps` 带 `--no-deps`），每步 `pip install --dry-run --report --only-binary :all: --python-version X.Y --implementation cp --abi cpXY`，`--platform` 是从 `manylinux_2_<上限>` 一个个往下到 `manylinux_2_17`、再加 `manylinux2014`、老名字和 `linux_x86_64` 的整个梯子。pip 用一个和环境里同版本的帮手 Python（`<状态文件夹>/envlock/python-<版本>`，conda 建一次）、`--python` 指到一个只有假 dist-info 的 venv：conda 装的 Python 包、基础环境的锁、前几步装的都写成"已经装着"，pip 只报这一步要装的（版本不同的照样报，后一步的替掉前一步的同名包）。占位 `{torch_mm}`、`{abi}`、`{cp}` 按已经解出来的 torch（`{abi}` 读 torch wheel 里 `TorchConfig.cmake` 的 `_GLIBCXX_USE_CXX11_ABI`）和 Python 填。描述的 pip 里可以直接写 wheel 的网址（如 GitHub release 上按 torch、CUDA、ABI 编好的）。
- **只有源码包的**：pip 报 "No matching distribution" 时用 `pip wheel --no-deps`（`SOURCE_DATE_EPOCH` 固定）建，是 `none-any` 的纯 Python wheel 放进 wheelhouse、记 `wheelhouse/built/<源码包 sha256>.json`（以后的锁直接用同一个 wheel），条目的 `url` 为 null、`built_from` 是源码包的网址；建出来要编译的报错，说出是哪个包。
- **venv（有 `base`）**：先要有基础环境同一变体的锁（没有就报错），只记基础环境里没有的包（同名同版本的不记），`base` 记 `{name, variant, lock_sha256}`。`lock_sha256` 和描述的哈希一样去掉 `created` 算（`proto.LOCK_UNHASHED`）。

锁的格式（`format` 1；键排序、缩进 1，同样的输入、同样的索引再锁一次，除 `created` 外一字不差）：

```
{"format", "name", "variant", "spec_sha256"（描述的字节的 sha256）, "channels",
 "platform": {"glibc_floor", "pip_platforms", "python"（X.Y）, "python_full", "implementation", "abi", "conda_subdir"},
 "base": null 或 {"name", "variant", "lock_sha256"},
 "conda": [{"name", "version", "build", "channel", "subdir", "url", "fn", "size", "md5", "sha256", "noarch"}],   （LINK 的顺序）
 "pip":   [{"name", "version", "url"（本地建的为 null）, "fn", "size", "sha256", "no_deps", "step", "built_from", ["built_from_sha256"]}],
 "tools": {"conda", "pip"}, "created"}
```

**描述的哈希**（`mtab.spec_hash(名, 变体)`，`.ok` 里写的、就绪看的）= sha256(描述的字节 + 锁去掉 `created` 后的规范 JSON 字节（键排序、紧凑、UTF-8）)；还没有这个变体的锁时只算描述。一台机器上用哪个变体：`mtab.spec_variant`，条目写 `("spec", 变体)` 就是它，venv 跟着这台基础环境的变体（有那个变体的锁时），否则 default。`taskq check`：机器条目里的 spec 环境没有锁时提示（不算问题），锁的 `spec_sha256` 和现在的描述对不上（描述改了没重新锁）算问题。

## 文件

| 文件 | 内容 |
|---|---|
| `bin/taskq`、`install.sh`（仓库根目录） | 命令行（见下面的"命令行"）、安装和更新 |
| `q`、`qs` | 加任务：`taskq pueue-add <组> [pueue add 选项] -- '<命令>'`（`q`：按 `envs/machines.py` 设好卡和组的环境变量，工作目录是代码副本或仓库根目录，核心的 `bin/` 放在 `PATH` 最前面）；`taskq pueue-status [机器]`（`qs`）看各 pueue 机器的队列 |
| `taskq.py` | 总队列（box）和执行机的队列：worker 领（`next`）、心跳（`beat`）、交还（`release`）、缺输入交还（`missing`，补齐后 `input-ok`）；分给一台执行机（`take --assign`）；加（`add --exp`、`add --recipe`；执行机的队列 `add --spec-stdin`）、改参数（`set`；执行机的队列里 box 送来的说明不改，改总队列后 `relay.py resend`）、改先后（`set-nice`）、删掉没人领着的（`remove`，依赖它的要一起删；失败、停止、缺输入、交还记录一起删，执行机的队列连跳过也删，11/4）、看说明（`show`）、看队列（`status`）。命令说明在文件开头 |
| `recipes.py`、`spec.py`、`experiments.py`、`steps.py` | 做法、解析器、读实验表、步骤的"做完没"判断 |
| `inputs.py` | 开训前查输入（缺了退出码 4）、节点本地盘上的私有视图和后台拷贝（见上面） |
| `run_task.sh <做法> k=v…` | 所有机器共用的运行器：激活环境、设变量、跑命令、写分段日志；已经训完就直接退出；Slurm 外设了 `CUDA_VISIBLE_DEVICES` 时卡被别人占着就等（每分钟看一次，最多 `res.wait_gpu` 分钟；组的环境变量设了 `TASK_GPU_IDLE` 时，还要 GPU 使用记录里这张卡最近这么多分钟没有别人，11/2），出错隔 5 分钟重来、最多 3 次；SIGTERM 先停训练再退出；任务的命令里 `TASKQ_LIB` 是核心的 `queue/`、核心的 `bin/` 在 `PATH` 最前面（11/4） |
| `consume.py`、`consumers/` | 执行机上的消费者核心和内置插件（见"消费者"）；`consume.py --selftest` 换包前在新包里自检 |
| `worker.sbatch`、`worker_start.sh`、`cpu_worker.sbatch`、`dicc_worker.sh` | DICC 作业：`worker.sbatch`（long、3 天）只有资源申请和转交到 `worker_start.sh` 的一行；`worker_start.sh` 查 `ENABLED`、激活 conda（失败时日志里写一行 `!!`，照样往下走：任务各自激活环境，11/4）、启动显存填充，交给 `dicc_worker.sh`（转交 `consume.py`）。`cpu_worker.sbatch`：`cpu-epyc-genoa`、normal、32 核、64G、8 小时（`cpu-epyc` 的节点系统太旧） |
| `feed.py` | 送任务的唯一入口：转给 `relay.py`（见上面）；`taskq plan` 只看会做什么 |
| `refill.py` | `relay.py` 用的挑任务（`candidates`、`next_pick`）和补交 Slurm 作业（`slurm_plan`）的规则 |
| `advance.py` | 训完以后的步骤（见上面）；`taskq plan` 只看会排什么 |
| `archive_task.sh box <key> [--dry-run]` | 执行机上训完、拉回 box 以后：备份 `spec.py outputs` 的 `outs`、上一级的 `extra` 和分段日志到备份机（`envs/machines.py` 的 `backup`），核对一致；rsync 经 `taskq tunnel rsync`（`sync/tunnel.py`；`envs/machines.py` 的 `tunnel` 里有的方向经隧道传） |
| `on_boot.sh` | 开机（`@reboot`）：启动 pueued，把被打断的任务原地放回队列（nnU-Net 从存档接着训，推理跳过已做好的病例），记在 `~/on_boot.log`；`gpu_log` 的机器启动 GPU 使用记录 |
| `setup.sh`、`install_pueue.sh` | 装 pueue、启动常驻进程、按表建组（表里没有的空组删掉）、写 crontab（`on_boot.sh --gpu-log`，从表里的 `code` 跑：执行机是代码包的 `R/code/current`；hub 另按 `OUTPUTS` 的 `cron` 调 `entry`，PhD 是 `scripts/monitor/auto_refresh.sh`，它也负责更新 box 的代码副本）。`setup.sh <名字>`（11/4）：在 box 上替一台没有仓库克隆的机器做，经 ssh 跑那边代码包里的 `setup.sh` |
| `proto.py` | 协议的唯一定义（见"约定"第一条）；`python3 proto.py sh` 打印 bash 用的 `TQ_*` 变量，`python3 proto.py json` 打印全部 |
| `taskq_api.py` | 插件用的核心接口（`REQUIRED`、`RelayError`、`expand`、`sha256`、`check`…） |
| `monitor/gpu_log.sh` | GPU 使用记录（每分钟一行，位置见 `proto.py` 的 `GPU_LOG_DIR`、`GPU_LOG_FILE`；11/4 从使用者仓库搬进核心）：`run_task.sh` 的 `TASK_GPU_IDLE` 和健康检查读，`on_boot.sh --gpu-log` 在机器表 `gpu_log` 的机器上启动 |
| `gpu_pad.py`、`gpu_pad_start.sh` | 显存填充（见上面） |
| `relay.py`、`relay_remote.py`、`relay_store.py`、`relay_transfer.py`、`relay_bundle.py`、`relay_plugins.py`、`shift.py` | 执行机（见上面）：每次检查的计划和命令行（`relay.py`）、经 ssh 在那边跑的命令和发过去的小程序（`relay_remote.py`）、box 上的记账和清理根目录（`relay_store.py`）、传数据和拉回（`relay_transfer.py`）、代码包、中继插件的加载、按时段开停 |
| `cfg.py` | 找配置文件夹（`TASKQ_CONFIG`）、插件文件夹（`TASKQ_PLUGINS`）、状态文件夹（`TASKQ_STATE` / `OUTPUTS["state"]`），把插件合进核心的表（`KINDS`）；bash 用：`get <键>`（`OUTPUTS`）、`state [名字…]`、`log <名字>` |
| `sites/slurm.py`、`pueue.py`、`remote.py` | 调度器适配器：读作业数 / worker、交 worker 作业 / 开 worker；根目录的 shell 写法（`relay.py` 用） |
| `test_*.py`、`test_*.sh` | 测试（假训练、假队列、假快照，不连机器、不用 GPU；`test_gpu_pad_integration.sh` 用 box 的真卡）。`test_taskq.py` 的"worker 的调用方式"一组和 `test_dicc_worker.sh` 守着在跑的 worker 用的接口（停止、失败在 `test_dicc_worker_stop.sh`，不在 Slurm 里的和代码包在 `test_dicc_worker_code.sh`，插件、换包时交接在跑的、时段、等卡在 `test_consume.sh`，共用 `testdata/worker_harness.sh`；`WORKER_TEST_ONLY` 只跑几个情况）。核心的测试不读 `envs/`、`research/`，用 `testdata/` 的固定配置（11/2）；关于真的实验表、nnU-Net 插件、机器表和环境描述的检查在 `research/common/taskq_plugins/tests/` |
| `run_tests.py` | 跑全部测试（`taskq self test`，见根目录 `README.md` 的"测试"）：每个文件自己的临时 HOME、短 TMPDIR，`# 要真卡` 的只在 `--gpu` 时跑，退出码 77 算跳过；`test_example.py` 跑示例项目 `example/` 的 README 里的命令 |
| `testdata/` | 测试用的固定配置：`config/`（机器表、`policy.py`、环境描述，机器名同真的，ssh 名字是假的 `fake-…`）、`plugins/`（`toy` 做法、中继插件、进度读法）、`exps/demo/experiments.py`（小实验表，步骤的写法同真的）、`bin/`（假的训练和步骤脚本）、假的 conda 和 venv；`fixture.py` / `fixture.sh` 设 `TASKQ_CONFIG`、`TASKQ_PLUGINS`，并在 PATH 里真的 ssh 前面放 `noremote/`（`ssh`、`scp` 马上失败，测试连不到真的机器） |

## 命令行（`bin/taskq`，11/4）

命令表在仓库根目录的 `README.md`。`bin/taskq` 先找项目、导出 `TASKQ_REPO` / `TASKQ_CONFIG` / `TASKQ_PLUGINS`，再在进程内调用各模块的 `main(argv)`（bash 脚本用 exec），原来的脚本照旧能直接跑。

- **`--help` 从不做事**：每个命令和老脚本（`shift.py`、`advance.py`、`feed.py`、`health.py`、`relay.py`、`envs_cli.py`、`qs`、`tunnel.py`、`tunnel_push.sh`）都先解析参数，`-h` / `--help` 打印说明、退出码 0。
- **只看的命令统一认 `-n` / `--dry-run`**（老写法 `--dry` 照旧）：`plan`、`advance.py`、`feed.py`、`relay.py` 本来就只看，写不写一样；`shift.py`、`tick` 不带时真做。
- **数据走标准输出，消息和出错走标准错误**：`taskq.py` 的"总队列在 …"、`status` 的"读不到队列"、`relay.py status` 的"读不到"在标准错误；`relay.py stage|fetch` 出错时打一行原因、退出码 1（不是 Python 的出错堆栈；排不上照旧 75）。`taskq.py add / set / show / remove` 的原因、`add --spec-stdin --update-only` 逐个的"换不了"在标准错误（`relay_remote.rtq` 合并两路，读法不变）。转给 bash 脚本时 `bin/taskq` 设 `TASKQ_PROG`，脚本的用法和出错里写 `taskq <命令>`。
- **`--json`**：一行 JSON。冻结的底层命令（`status --json`、`show --json`、`spec collect --json`）照旧以 `@@JSON ` 开头。
- **退出码**：0 成功，1 出错，2 用法错，领域码沿用 `proto.EXIT`（表在根目录的 `README.md`）。冻结的底层命令（`worker …`、`spec …`、`internal stage`）的参数、输出、退出码不变。
- `taskq.py` 另认 `nice`、`rm`（`set-nice`、`remove` 的别名），老名字照旧。

## 约定

- **协议只在 `proto.py` 定义**（11/4）：执行机根目录和队列目录的布局、标志文件（`.incomplete`、`MANIFEST.json`、`ENABLED`、显存填充的握手文件…）、key 的文件名（`safe`）、`@@JSON` 行、时间常数（心跳、过期）、任务状态的取值、各脚本的退出码、`local_status` / `spec.collect` 的字段、报警编号、`TASKQ_*` 环境变量。Python `import proto`，bash `eval "$(python3 proto.py sh)"`（`TQ_*`；`worker_start.sh`、`run_task.sh` 读不到 proto 就退出，所以 `proto.py` 在代码包的 `RUNTIME` 里）。执行机根目录 `R/` 的布局是 `proto.ROOT`（建哪些文件夹 `MKDIRS`），领取是否还活着只有 `proto.claim_alive` 一条规则（心跳 20 分钟、没有心跳时分段日志 15 分钟内写过、只建了目录的 5 分钟内）。盘上的名字和取值只加不改（在跑的旧 worker 和新代码共用队列目录）；`test_protocol.py` 核对核心里别处不再手写这些名字。
- **DICC 作业提交时定下的**只有 `worker.sbatch`；其余脚本在作业开始时或每次调用时从仓库读。排着的作业依赖的只有 `ENABLED` 和 `worker_start.sh` 这个路径；在跑的作业（最长 3 天）用开始时读到的 worker，换代码包时把在跑的交给新包的 `consume.py`（11/5）；worker 调用的 `taskq.py next / beat / release` 和 `run_task.sh` 的参数、退出码不改，只加新命令。
- **退出码**：`run_task.sh` 0 训完，3 命令正常结束但没训完，4 缺输入（开训前查到，没跑命令；11/1），5 读不了盘、判断不了训完没有（`spec.py done` 退出 5；11/4），2 任务说明或环境有问题（原因也写进分段日志），1 训练命令出错（命令自己的退出码 1–127 记在分段日志），128 以上是被信号停掉（自己收到 SIGTERM 是 143，训练命令被杀如 137 原样传出；worker 据此区分"训不了"和"被杀"）（10/25 定）。`qs --json` 每个任务：`status` 没结束时是 pueue 的状态（`Running`、`Queued`… 在跑的集合是 `refill.LIVE`），结束了是结果（`Success`，失败的如 `Failed (1)`），`relay.py` 按它判断在不在跑、成没成；`result`（`Success` / `Failed` / `Killed` / …，没结束是空）和 `code`（退出码）给要区分失败原因的（健康检查认被杀，`relay.py` 认 `relay.py stage` 的退出码 75 = 这次排不上、下次再传）。`taskq.py next` 0 领到（三行：key、`run_task.sh` 的参数、token），10 没有能领的，11 等排在前面刚停下的，12 队列暂停或读不了数据目录（稍后再领）；`beat --strict` 13 有的领取已经不是自己的（打印这些 key；11/4）；`take` 0 领到，10 领不到（`--from <执行机>`：分给那台的在同一把锁里直接改分，`shift.py` 交班用，11/5）；`remove` 0 删了，3 不删（有活着的领取、分出去了、别的任务的 `after` 等它），4 队列里没有（什么都没删；`relay_transfer.drop_remote` 当作上次已经删了），按退出码判断，不看输出的文字（11/4 定）。`spec.py done` 0 训完，1 没训完，2 任务说明有问题，5 读不了数据目录（盘断开等，稍后再查；11/4 定）；`spec.py shell` 说明有问题时退出码 2，只打一句话。不在 box、本机又没有队列时，改队列的命令退出码 2（"总队列在 box"）。
- **跨版本读执行机**（11/4）：box 上的新代码读执行机（那边可能还是旧包）只用两个带版本号的命令，像 `next` / `beat` / `release` 一样冻结，字段只加不改：`taskq.py status --json` → `@@JSON {"version", "enabled", "tasks": [每个任务 proto.STATUS_FIELDS], "recent_failures", "paused", "recent_skips"}`；`spec.py collect --json`（标准输入 `{"specs": [按 proto.COLLECT_FIELDS], "root", "wait"}`）→ `@@JSON {"version", "tasks", "seglogs"[, "stuck", "errors"]}`。没有这两个命令的旧包退出 2、没有 JSON 行，`relay_remote.py` 这时退回 import 旧包内部函数的老写法（换完包以后不再走）。`spec.remote_collect` 把 `proto.py` 的源码拼在 `spec.py` 前面一起送过去（`spec.remote_program`），不用那边的 proto。
- **给使用者的汇总**（11/4）：使用者的脚本（登记表、分析、看板）只读命令的一行 JSON，不 import 核心的内部函数；字段表在 `proto.py`，只加不改，带 `version`。`experiments.py list --json` → `{version, experiments, final}`（实验表原样，函数换成名字）；`experiments.py tasks [--exp ID…] --json` → `{version, tasks: [proto.EXP_TASK_FIELDS]}`；`monitor/overview.py --json [--no-remote]` → `proto.OVERVIEW_TOP`：总队列、各机器（`gather()`）、每个任务（`proto.OVERVIEW_TASK`：队列里的情况、进度最新的那份、轮数、预计训完、分段日志和代码提交号）、健康检查记下的问题。`taskq machines show --json` → `proto.MSHOW_TOP`（每台 `proto.MSHOW_MACHINE`，每个环境描述 `proto.MSHOW_SPEC`）；`taskq exp steps --json` → `proto.EXP_STEPS_TOP`（每步 `proto.EXP_STEP_FIELDS`，`--done` 判断做完没有，`--needs` 真跑 `--stage needs`）；`taskq spec export|import --json` → `proto.RELAY_EXPORT_TOP` / `proto.RELAY_IMPORT_TOP`（中继插件的送出和拉回核对；在 `queue/cli_json.py`）。`queue/check.py [--quick]` 核对机器表（含环境描述 `specs/*.yml` 读得了、`.check.py` 能编译、pip 的占位、每个 torch 变体生成得出建环境的脚本，`quick` 的正则）、policy、插件、实验表、每个任务和步骤能变成命令、`--stage needs`、执行机的代码包；退出码 0 没问题、1 有问题（问题在 stderr）。
- **报警是结构化的**（11/4）：`health.py`、`relay.py`、`feed.py` 的每条报警是 `proto.alert(句子, 字段…)`（编号 `proto.alert_id`，格式在 `proto.ALERTS`），手机推送 `monitor/plain.py` 读字段（`data`），不从中文句子里抠数字；改句子的措辞不影响推送。
- **经 ssh 读回的 JSON**：`taskq.py --json-status`、`taskq.py show --json`、`spec.py` 的 `remote_collect` 把 JSON 写在单独一行、以 `@@JSON ` 开头，读的一方只认这一行（`spec.py` 的 `json_out` / `json_in`），前后别的输出不影响（10/25 定）。
- **各机器的状态只取一次**：`monitor/health.py` 的 `gather()`：pueue（`qs --json`）、每台 pueue 机器一次 ssh 的探查（开机、盘、GPU 使用记录、各卡和卡上的进程）、总队列、box 上各任务的训练状态（`spec.collect`）；执行机的状态由 `relay.py` 每次检查读一次（`relay.py status <名字>`）。健康检查、`relay.py`、`advance.py` 和登记表 `make_registry.py` 都用它的结果，不另外去取（10/25 定）。
- **队列所在的机器**：`envs/machines.py` 的 hub（box，`taskq.queue_host()`，10/31 起）；不在 box、本机又没有队列时 `taskq.py add / set / set-nice / take / show / remove / stop / resume / status` 退出码 2（不转发）；执行机的队列由 box 的 `relay.py` 经 ssh 调用那边代码包里的 `taskq.py`（`relay_remote.rtq`）。worker 用的 `next / beat / release` 不转发，没有 `tasks.jsonl` 就退出 2（10/25 定）。
- **Slurm 作业名**（11/2 定）：每个作业交的时候随机取一个 `<词>-<词>`（`sites/slurm.py` 的 `jobname`：worker 作业、传输的 CPU 作业都是），各账号、各作业都不一样；sbatch 文件里不写 `--job-name`。认 worker 按 squeue 的 `%o`（交的脚本：`worker.sbatch` = GPU，`cpu_worker.sbatch` = CPU，`slurm.kind_of`），不按名字。`relay.py` 按交的脚本数已有几个 GPU / CPU worker；`envs/machines.py` 的 `jobs` 只写这台交哪几种 worker；查提交名额用的 QOS 来自 `envs/policy.py` 的 `SLURM`（10/25 定）。
- **代码版本**：box 的 pueue 任务的工作目录是 `~/.cache/phd/code/<提交号>`（`envs/machines.py` 的 `code`，`taskq pueue-add` 加任务时先跑 `sync/snapshot.sh`（`taskq snapshot`）更新到 origin/main），任务开始后代码再更新也不影响它；旧副本没有任务用、建了 7 天以上才删。拉取都经 `sync/fetch.sh`：仓库钉的子模块提交（核心作为子模块时）也拉到了，`origin/main` 才前进，拉不到就退回原来的（打包、建副本都按 `origin/main` 取文件）；副本里的子模块是克隆里那个子模块的 worktree，不连网。执行机（dicc1、DICC2、lab1、lab0）只有 `relay.py` 推过去的代码包（`R/code/current`），worker 和机器级的脚本（开机、GPU 使用记录、装 pueue）都从它跑（11/4 起 lab 不用仓库克隆，机器表里 `repo=None`、`code` 指代码包）；健康检查的"代码落后"只查有 `repo` 的机器。
- **跑命令**：调度脚本在本机或经 ssh 跑命令都用 `common.run`（返回退出码、标准输出、标准错误；超时、起不来是 124）（10/25 定）。
- **到别的机器跑的命令那边也限时**（11/1）：`common.run`、`spec.remote_collect`、`relay.py` 推代码包、`tunnel.py` 的短命令在那边套 `timeout -k 10 <本机超时 - 15 秒>`（`common.remote_timeout`；健康检查的探查 `run_on` 本来就有），本机超时断开后那边卡在存储里的进程也结束，不留在登录节点上。`remote_collect` 各任务同时读、一共等 90 秒，没读完的记进结果的 `stuck`：`refill.progress` 当它们有进度（不当新任务分出去），健康检查报 `unread:<机器>`。
- **日志不重复**：`advance.py`、`relay.py` 的"会做什么"和上次一样就不再写日志，上次的内容在 `~/.cache/phd/last/<名字>.json`（`common.changed`）。
- **状态文件整份换**：`refill.json`、`tasks.json`、`last/` 下的文件、领取的 `info` 都先写临时文件再换过去（`queue/common.py` 的 `write_json`），断电、被杀时只有旧的或新的。领取只建了目录、没有 `info` 的，5 分钟后算过期。
- **同一个任务不在两处做**：总队列里先分给一台执行机（`claims/<key>/info` 的 `assigned`：不过期，只靠交还结束），那边的队列里再由那边的作业领（`info`：`proto.CLAIM_FIELDS` = worker、host、since、beat、token，不在 Slurm 里的 worker 另记 `log`（分段日志名里的作业名，`next --log-tag`，11/4；没有时同 worker）；过期按 `proto.claim_alive`）才做。`skips/<key>`：哪台执行机跳过了这个任务（删掉即撤销）；`released/`：刚交还的（10/31）。
- **任务表** `tasks.jsonl` 每行 `{order, nice, key, exp, recipe, params}`；档位 = `nice`，同一档按 `order`。任务说明的 `device`（默认 `gpu`）、`machines`（默认不限，如 `["dicc1"]`）决定谁能领。`after`（默认 `[]`）：一组任务 key，都做完前这个任务是 `waiting`（不领、不算补交作业的数）；其中有失败 2 次的或不在队列里的是 `blocked`，健康检查报 `tqblocked:<key>`。
- **pueue 任务标签**：训练 `train:<key>`；`advance.py` 排的每个任务 `<步骤>:<实验 id>_f<fold>`（如 `infer:t14met_f3`），实验一级 `<步骤>:<实验 id>`。执行机的是 `relay_stage:<key>`、`relay_fetch:<key>`、`relay_archive:<key>`。标签也是认任务的一半：任务记录要任务号和标签都对上（见下一条），`HOLD` 按标签的通配式匹配，执行机的 worker 按标签 `worker:<id>` 找（`sites/pueue.py`），健康检查按 `train:` 标签认训练。改标签的写法要连同这些一起改。
- **任务记录**（10/25）：box、lab 的 pueue 任务是哪一步、哪个 key 只看 box 的 `~/.cache/phd/tasks.json`（`queue/common.py`）。每条 `{machine, id, label, added, step, key}`，按步骤另有 `group`、`needs`。`relay.py`、`advance.py` 加任务前先写记录（`id` 为空）、加上后补任务号；没拿到任务号的，健康检查按"同一台机器、同标签、创建时间不早于记录"找回，一小时找不到就作废。任务号和标签都对上才算同一个任务（任务号会被重用）；任务删掉后记录跟着删；读不到的机器的记录不动。没有记录的（手动加的）只按命令认训练（`run_task.sh`）。
- **能放到别处的步骤**：步骤的 `queue` 里写了 `inputs`（固定的输入）或 `inputs_cmd`（打印要用的输入的命令，相对数据目录，每行一个；11/4），没写这两项的老任务照旧按命令里的 `--stage <步骤>` 换成 `--stage needs` 来问；结果在 `done` 判断的文件所在的文件夹里。
- **key** 由做法给，nnU-Net 是 `<数据集>/<训练器>/<fold>`（如 `Dataset111_MENrq1/nnUNetTrainer_save10_n470/0`）。实验表条目、步骤、步骤的 `queue` 的字段在 `experiments.py` 的 `EXP_FIELDS`、`STEP_FIELDS`、`QUEUE_FIELDS`（11/4 起加载时核对，拼错报错）；放到执行机做的步骤用 `queue` 的 `inputs` 或 `inputs_cmd`（打印要用的输入的命令）说清输入，不再从命令行按位置猜。**训完** = 任务说明的 `done` 都在（nnU-Net：`validation/summary.json`，和 `checkpoint_final.pth` 或 `ARCHIVED`；`ARCHIVED` 是 10/31 前归档脚本写的标志，现在不再写，只认）。
- **分段日志**：`<数据目录>/logs/tasks/<key 的文件名>__<作业>_part<段>.log`（`<作业>` 在 Slurm 里是作业号，不在 Slurm 里是"主机名-时间"，记在领取的 `log`；段号 `TASK_PART`；10/31 前的叫 `_seg<段>.log`，读日志的地方两种都认；执行机拉回、删任务时只认 `<key 的文件名>__` 后面不再有 `__` 的，key `a/b` 不带上 `a/b/c` 的，11/4），第一行 `作业 <作业>_<段> @ <机器>，任务=<key>，第 <段> 段，环境 <环境>，代码 <版本>，<时间>`（10/18 前的按任务说明的 `oldlogs`：`dir` 下以 `prefix` 开头的，登记表、归档两种都认）。worker 自己的日志在 `<数据目录>/logs/workers/`。

## 常用命令

```bash
# 加实验表里的实验（档位用表里的 nice，也可以 --nice）、只加某几个任务、改先后、看一个任务会怎么跑
taskq add --exp t15met
taskq add --nice 0 --exp t15men --only fold=1,2,3,4
taskq nice --nice 0 Dataset116_METrq1/nnUNetTrainer_save10_n470/2
taskq show Dataset116_METrq1/nnUNetTrainer_save10_n470/2
# 改一个任务（会改变 key 的参数不能改，删掉重加）
taskq set Dataset101_GLIrq1/nnUNetTrainer_UMambaEnc/1 env=umamba
# 停止一个任务（10/30）：分给执行机的由 relay.py 下次检查时在那边停（在训的 30 秒内停下）、交还；之后不再分，resume 撤销
taskq stop --why 原因 Dataset156_METrq1_erand/nnUNetTrainer_save10/0
taskq resume Dataset156_METrq1_erand/nnUNetTrainer_save10/0
# box 上单独推理 / 评测一个任务；看各 pueue 机器的队列
taskq pueue-add gpu --label infer:x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh --stage infer <模型文件夹的完整路径> 4'
taskq pueue-add cpu --label eval:x_f4 -- 'bash research/RQ1/analysis/run_predict_eval.sh --stage eval <模型文件夹的完整路径> 4'
taskq pueue-status
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
| `inputs` | 要用的文件或文件夹（相对数据目录，box 上先放好）；送到执行机之前传完、核对过；开训前再查一次，缺了不跑、标"缺输入" |
| `out` | 结果文件夹（相对数据目录），做完拉回 box 的同一位置 |
| `done` | 做完的标志：`out` 下的文件，每一项都要有（一项里 `a|b` 有一个就行） |
| `progress` | `progress_json`：命令每做完一轮写 `out/PROGRESS.json`，内容 `{"done": 已做完的轮数, "total": 总轮数}`；不写 = 只看日志多久没更新；某种模型专用的读法（如 nnU-Net 的 `nnunet_log`）在 `research/common/taskq_plugins/readers/<名>.py` 的 `PROGRESS` 表里（`info` 同理，`INFO` 表），只用标准库，收集进度时随 `spec.py` 一起送到别的机器 |
| `resume` | `out` 下的存档（glob）：有一个就算有进度，失败时留在那台机器上从存档接着做，不换机器重来 |
| `log` | `out` 下的日志（glob），15 分钟没更新算没人在做 |
| `res` | 资源：卡数、CPU 数、内存、显存（GB）；`wait_gpu`（分钟）：不在 Slurm 里时卡上有别人的进程（自己账号的 worker、显存填充不算）就每分钟看一次，最多等这么久，等不到算失败（0 = 不等，不写 = 一直等） |
| `nice` | 档位（越小越先；命令行的 `--nice` 优先） |
| `relay` | 送到执行机时用的插件，任务单加的默认 `path`：输入是 `inputs` 里的文件、文件夹、glob，原样送过去；`null` = 只在 box、lab 这类直接用仓库的机器上跑 |
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
