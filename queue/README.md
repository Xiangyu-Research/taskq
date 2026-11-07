# taskq 的机制

总的介绍、安装、命令表、测试、退出码见 [`../README.md`](../README.md)。这里写配置的字段、各部分怎么运转、文件在哪、怎么加新东西。

## 结构

```
                         hub（machines.py 里角色 hub）
   cron 每 30 分钟 ─→ taskq tick（monitor/health.py）
                        ├─ 查各台机器、推送问题（monitor/plain.py、push.py），记进 hub 的数据库（queue/db.py、events.py）
                        ├─ queue/advance.py   训完以后的步骤：做完的看 store，下一步加进总队列
                        └─ queue/feed.py → queue/relay.py   派任务、传数据、补 worker、拉回、归档
                               │   refill.choose：每个没人领的任务挑一台执行机
                               │   relay_remote.op / store_op：到那台跑 queue/site_ops.py 的操作
          总队列（queue/taskq.py，hub 的状态文件夹下）
                               │
        ┌──────────────────────┼─────────────────────────┐
   store（角色 store）      执行机（pueue）              执行机（Slurm）
   数据目录 = 结果的家       根目录 R：code/ queue/       根目录 R：同左
   site_ops：probe、export、 blobs/ data/ state/          sites/slurm.py 交 worker 作业
   pull、collect、archive    worker（consume.py）常驻     作业里跑 worker（consume.py）
        │                    在 pueue 的组里                    │
        └── 数据在 store 和执行机之间直接传（sync/tunnel.py，或同一台本机拷），不经过 hub 的盘 ──┘
   备份机（角色 backup）：要归档的结果从 store 备份过去（queue/archive_task.sh）
```

- hub 替每台执行机在总队列里领任务（总队列里记着分给了哪台），把输入送过去、放进那台自己的任务队列
  （`R/queue`，key 和总队列相同）；那台的 worker 从自己的队列领、训、交还；hub 看到做完就拉回 store。执行机之间不通信。
- 一台机器可以兼几个角色；`ssh=None` 的那台是 hub 所在的机器，它也可以是执行机（命令在本机跑，传输在本机拷）。
- 执行机用的代码是 hub 打的代码包（`queue/relay_bundle.py`：核心里执行机要用的文件放在包里的 `scripts/taskq/`，加上项目的配置文件夹和
  `OUTPUTS["bundle"]` 列的文件夹），包名是内容哈希；推到 `R/code/<包名>`，`R/code/current` 指向在用的那个。

## 开关和配置

配置文件夹里两个文件：`machines.py`（机器表）、`policy.py`（怎么用）。字段、类型、默认值都在 `queue/schema.py`，`taskq check` 按它核对
（不认识的字段报错），`taskq machines fields` 打印它。下面的表由 `taskq machines fields --markdown` 生成，`queue/test_readme.py` 核对一致。

### 机器表

`MACHINES = {名字: dict(…)}`，一台物理机器一项。角色 `roles`：`hub`（总队列、健康检查）、`store`（结果的家）、`backup`（归档）。
有 `root` 和 `scheduler` 的是执行机：

- `scheduler="pueue"`：写 `workers=[dict(…), …]`，每项一个常驻 worker（字段见下一张表）。worker 的名字由核心按顺序算：有卡的是
  `gpu<卡号>`，没卡的是 `cpu`（几个时 `cpu<序号>`）；id 是 `<机器>-<名字>`，pueue 的组同名字。
- `scheduler="slurm"`：写 `jobs=dict(gpu=…, cpu=…)`（sbatch 的选项），交几个、QOS、时限在 `policy.py` 的 `SLURM`。

pueue 执行机、hub、store 上都有核心自己的 pueue 组 `taskq`（`proto.IO_GROUP`：传数据、拉回、归档、建环境，同时跑不限）。
手动用的组写 `manual_groups`，核心不往里排（`taskq pueue-add` 往里加）。本机的 `conda`、`root`、`data`、`repo` 等路径里的 `~` 在本机展开，
远程命令里根目录开头的 `~` 留给那边展开。

<!-- fields:MACHINE_FIELDS -->
| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `label` | `str` | 必写 | 提醒、页面上显示的名字 |
| `hostname` | `regex` | 必写 | 认出本机用的主机名（hostname -s）正则 |
| `ssh` | `str?` | `None` | hub 连过去的 ssh 名字；None = hub 所在的这台 |
| `ssh_master` | `bool` | `False` | ssh 只经长连接（ControlMaster）；长连接断着时不连 |
| `tz` | `str?` | `None` | 这台的时区（IANA 名，如 Asia/Kuala_Lumpur）；worker 的 hours 按它算；None = 本机时区 |
| `conda` | `pair` | `None` | (conda 的位置, 默认环境名) |
| `pueue` | `str` | `'~/.local/bin/pueue'` | pueue 的位置 |
| `disk` | `disk?` | `None` | (要查的目录, 剩不到多少 GB 提醒, 剩不到多少 GB 算紧急或 None) |
| `gpu_log` | `bool` | `False` | 开机启动 GPU 使用记录（taskq gpu usage） |
| `tunnel` | `tunnel` | `()` | 和 store 之间经隧道传文件的方向：push（store → 这里）、pull（这里 → store） |
| `quick_tunnel` | `bool` | `False` | 传文件用临时的 trycloudflare 地址 |
| `transfer_job` | `str?` | `None` | slurm：大文件传输在计算节点上跑的 srun 参数 |
| `env_job` | `str?` | `None` | slurm：建环境在计算节点上跑的 srun 参数；None = 用 transfer_job |
| `roles` | `roles` | `()` | hub（总队列、健康检查）、store（结果的家，数据目录在 data）、backup（归档的备份放在 backup） |
| `repo` | `str?` | `None` | hub：cron 用的项目克隆 |
| `data` | `str?` | `None` | store：数据目录（结果的家） |
| `backup` | `str?` | `None` | backup：归档的备份放在哪（数据目录的布局） |
| `transfer` | `pair?` | `None` | store：文件服务的 (对外地址, 监听的地址:端口) |
| `quick` | `pair?` | `None` | store：临时隧道 (cloudflared 镜像, 认临时地址的正则) |
| `root` | `str?` | `None` | 根目录（blobs、data、code、queue、state、tmp）；执行机和 store 要有 |
| `scheduler` | `sched?` | `None` | 执行机：pueue（常驻 worker）或 slurm（交 worker 作业） |
| `workers` | `workers` | `()` | pueue 执行机：这台的 worker，每项见 WORKER_FIELDS |
| `jobs` | `jobs` | `{}` | slurm 执行机：{gpu\|cpu: sbatch 选项 dict(partition, gres, cpus, mem)}；QOS、时限、个数在 policy.py 的 SLURM |
| `gpu_pad` | `triple?` | `None` | slurm：worker 作业自己开显存填充的 (floor, target, high) MiB |
| `job_names` | `str?` | `None` | slurm：作业名的样式（sites/slurm.py 的 STYLES）；None = pair |
| `envs` | `envs` | `{}` | {环境名: ('conda', 名) \| ('venv', 目录, 基础环境) \| 'spec' \| ('spec', 变体)} |
| `cap` | `num?` | `None` | 根目录最多占多少 GB（有配额的家目录） |
| `blob_cap` | `num?` | `None` | blobs 指到另一个盘时，blobs 最多占多少 GB |
| `budget` | `pair` | `(100, 50)` | (盘剩不到多少 GB 开始清理, 传完至少还剩多少 GB) |
| `task_vars` | `strmap` | `{}` | 在这台跑的任务额外设的环境变量 |
| `task_unset` | `strs` | `()` | 在这台跑的任务里去掉的环境变量 |
| `manual_groups` | `groups` | `{}` | 手动用的 pueue 组 {组名: (同时跑几个, 卡号或 None, {环境变量})}；核心不往里排 |
<!-- /fields -->

pueue 执行机的每个 worker（`workers` 的每项）：

<!-- fields:WORKER_FIELDS -->
| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `gpu` | `str?` | `None` | 用哪张卡（CUDA_VISIBLE_DEVICES）；None = 纯 CPU 的 worker |
| `cpus` | `int?` | `None` | 最多用几个核；None = 全部 |
| `slots` | `int` | `1` | 同时训几个 |
| `hours` | `hours` | `()` | 只在这些时段里领、训：[(开始 HH:MM, 结束 HH:MM, 第一天 或 None, 最后一天 或 None)]；空 = 全天 |
| `idle` | `int?` | `None` | 别人用过这张卡后，空满这么多分钟才开训 |
| `wait` | `int?` | `None` | 卡被别人占着时最多等多少分钟 |
<!-- /fields -->

### policy.py

<!-- fields:POLICY_FIELDS -->
| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `AUTO` | `auto` | 必写 | 自动的部分开不开：advance、envs、revive、smoke |
| `PAUSE` | `pause` | `{}` | {机器: (到什么时候 'YYYY-MM-DD HH:MM' 或 None, 原因)}：暂停期间不连、不派任务，到期自动恢复 |
| `HOLD` | `strs` | `()` | 暂缓的步骤（步骤实例名的通配式，如 'infer/*'） |
| `PLACE` | `place` | `{}` | {步骤或训练的做法: [执行机, …] \| 'any'}：能去的执行机，顺序是偏好；没写的 = 'any'（有这种 worker 的执行机都行） |
| `READY` | `intmap` | `{}` | {机器: 备几个没人领的任务}；没写的 = 1 |
| `SLURM` | `slurm` | `{}` | {机器: dict(gpu={QOS: (每段时限, 给别的作业留几个)}, cpu=(QOS, 时限, 最多几个))} |
| `OUTPUTS` | `outputs` | 必写 | 项目的输出和入口，见 OUTPUT_FIELDS |
<!-- /fields -->

- `AUTO`：`advance`（训完以后自动排下一步）、`envs`（自动建环境；也可以写 `{机器: [另外预建的环境…]}`）、`revive`（pueued 没响应时自动恢复）、
  `smoke`（冒烟闸门）。关着的部分只记"会做什么"，不动。
- `PAUSE`：暂停的机器不连、不派任务；到期（hub 的本地时间）自动恢复，`None` = 一直停到删掉这一项。备份机暂停时要归档的结果直接交还。
- `HOLD`：对上的步骤实例不排（`advance` 跳过）。
- `PLACE`：键是步骤名或训练的做法名，值是能去的执行机（顺序是偏好）或 `'any'`。和任务说明的 `machines` 取交集。

`OUTPUTS`：

<!-- fields:OUTPUT_FIELDS -->
| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `state` | `str` | `'~/.local/state/taskq/'` | 状态文件夹 |
| `entry` | `str?` | `None` | 主循环入口（相对 hub 的 repo） |
| `cron` | `cron` | `()` | ((时间, 子命令), …)：taskq site setup 写进 hub 的 crontab |
| `origin` | `str?` | `None` | hub 的 repo 从哪里克隆 |
| `bundle` | `strs` | `()` | 执行机的代码包另带的文件夹或文件 |
| `bundle_skip` | `strs` | `()` | 不进代码包的（fnmatch 通配） |
| `bundle_check` | `str?` | `None` | 代码包另用本机这个环境的 python 核对能编译 |
| `experiments` | `str?` | `None` | 实验表的通配路径 |
| `code_dirs` | `strs` | `()` | 日志里代码版本后面标'+改动'时看的目录 |
| `env_script` | `str?` | `None` | taskq pueue-add 加的任务先 source 的脚本 |
| `name` | `str` | `'taskq'` | 推送标题的前缀 |
| `topic` | `str` | `'~/.config/taskq/ntfy_topic'` | 放推送频道名的文件（频道名不进仓库） |
| `logs` | `str` | `'~'` | 纯文字日志（relay.log、advance.log…）的文件夹 |
<!-- /fields -->

状态文件夹的先后：`TASKQ_STATE` > `TASKQ_ROOT`（执行机上的进程：队列在 `R/queue`，其余在 `R/state`）> `OUTPUTS["state"]` > `~/.local/state/taskq/`。
数据目录的先后：`TASKQ_DATA` > `TASKQ_ROOT/data` > 本机是 store 时机器表的 `data`。

### 实验表

`OUTPUTS["experiments"]` 指的文件里写 `EXPERIMENTS = [dict(…), …]`（每个实验一项），可以另写 `FINAL = [步骤…]`（所有实验之后的步骤）。

<!-- fields:EXP_FIELDS -->
| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `id` | `str` | 必写 | 唯一的短名（实例 id、taskq add --exp） |
| `exp` | `str` | `''` | 实验编号（登记表、看板） |
| `label` | `str` | `''` | 一句话说明 |
| `model` | `str` | `''` | 模型的名字（登记表分组） |
| `hours` | `num?` | `None` | 每个任务折算的小时数（登记表的总进度） |
| `recipe` | `str` | 必写 | 训练的做法（recipes.py）；PLACE 按它放训练任务 |
| `params` | `dict` | `{}` | 做法的参数 |
| `fields` | `dict` | `{}` | 任务说明里另外覆盖的字段（如 metrics、vars、inputs） |
| `each` | `dict` | `{}` | {参数: [取值…]}：按每种组合展开成一组任务；没有就是一个任务 |
| `after` | `steps` | `()` | 每个任务训完以后的步骤（STEP_FIELDS），按顺序做 |
| `group_after` | `steps` | `()` | 实验一级的步骤（STEP_FIELDS）；先后按输入和产出推 |
| `names` | `dict` | `{}` | 步骤模板里另外可用的名字（分析脚本也按 name-of 读） |
| `nice` | `int` | `0` | taskq add --exp 不给 --nice 时的档位 |
| `board` | `dict` | `{}` | 看板用（各表自己定） |
| `design` | `str` | `''` | 做了什么的中性说明（事实档案用） |
<!-- /fields -->

步骤（`after`、`group_after`、`FINAL` 的每项）：

<!-- fields:STEP_FIELDS -->
| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `name` | `str` | 必写 | 步骤名（policy.py 的 PLACE、HOLD 按它） |
| `cmd` | `str` | 必写 | 在执行机上跑的命令（模板；工作目录是代码包里的项目，{data} 是那边的数据目录） |
| `out` | `strs` | 必写 | 产出（相对数据目录；可以几个，第一个是主目录）。一个路径只归一个步骤 |
| `done` | `strs` | 必写 | 做完的标志：第一个产出下的文件，每一项都要有；'a\|b' 有一个就行；可以写 {参数} |
| `inputs` | `strs` | `()` | 输入（相对数据目录，可以用通配）；落在别的步骤的产出里就接在它后面，跨实验也算；实验一级的步骤里 {参数}（each 的）按每个取值展开 |
| `inputs_cmd` | `str?` | `None` | 另外打印输入的命令（每行一个相对数据目录的路径；relay 插件用） |
| `relay` | `str` | `'path'` | 送到执行机时按哪个插件准备输入、核对结果（sites/names.py） |
| `env` | `strs` | 必写 | 环境名（一个或几个；和 cmd 一样可以写 {参数}） |
| `device` | `device` | `'gpu'` | gpu（要卡）或 cpu（纯 CPU） |
| `res` | `dict` | `{}` | 资源（recipes.py 的 res） |
| `out_gb` | `num?` | `None` | 产出大约多少 GB（挑机器时留空间；None = proto.OUT_GB） |
| `temp` | `strs` | `()` | 第一个产出下的中间产物（通配）：用到它的步骤都做完、拉回以后由 store 删掉，记进事件日志 |
| `rerun` | `bool` | `False` | 输入里有比产出新的就重做 |
| `nice` | `int` | `0` | 进总队列的档位 |
| `only` | `dict?` | `None` | 每个任务的步骤：只对 each 取这些值的任务做（{参数: [取值…]}） |
<!-- /fields -->

模板里能用的名字：做法的参数、`each` 的取值、`names`、`{data}`（执行机上的数据目录）。实例 id：每个任务的步骤是 `<步骤>/<实验 id>_<简称>`，
实验一级的是 `<步骤>/<实验 id>`，`FINAL` 的是 `<步骤>`。`taskq exp steps` 列出展开后的步骤，`taskq dag` 看先后，`taskq who <文件>` 查一个文件是谁的产出。

### 做法

做法（`queue/recipes.py` 的 `command`，加上插件 `recipes.py` 的 `RECIPES`）是一种任务的模板：`params` 是参数和默认值（`REQUIRED` = 必须给，
默认值里可以用 `{别的参数}`），`open=True` 时任务可以带这里没列的参数；其余字段是任务说明（模板，`{参数}` 换成参数的值）。
任务的参数名是下表的字段、做法的 `params` 又没有它时，直接覆盖说明的这个字段（如 `res`、`machines`、`out_gb`）。

<!-- fields:RECIPE_FIELDS -->
| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `key` | `str` | 必写 | 任务名（队列、领取记录、登记表都用它，要唯一） |
| `env` | `strs` | 必写 | 环境名（机器表每台的 envs；几个用逗号或列表） |
| `cmd` | `str` | 必写 | 在代码包里的项目根目录用 bash 跑的命令；重跑要接着上次做（交还以后别的 worker 从存档接着做） |
| `out` | `str` | 必写 | 结果文件夹（相对数据目录）：拉回、归档、登记表都看这里 |
| `done` | `strs` | 必写 | 做完的标志：out 下的文件，每一项都要有；'a\|b' 有一个就行 |
| `vars` | `dict` | `{}` | 跑之前设的环境变量 |
| `log` | `str?` | `'*.log'` | out 下的日志（glob）：多久没更新用来判断有没有人在做 |
| `progress` | `str?` | `None` | 进度的解析方法（spec.py 的 PROGRESS、插件 readers/ 里的；progress_json = out/PROGRESS.json） |
| `resume` | `strs?` | `None` | out 下的存档（glob）：有一个就算有进度，交还时留在那台接着做；None = 没有 |
| `res` | `dict` | `{}` | 资源：gpus、cpus、mem、gpu_mem、wait_gpu（分钟）、local_inputs（1 = 开训后拷到节点本地盘） |
| `owner` | `str?` | `None` | 谁的任务（None = 自己的） |
| `info` | `str?` | `None` | 训练设置的解析方法（登记表的参数列） |
| `metrics` | `dict` | `{}` | 登记表的验证指标：{列名: (out 下的 JSON 文件, 键的路径)} |
| `checkpoint` | `str?` | `None` | 登记表算校验和的文件（out 下） |
| `inputs` | `strs` | `[]` | 执行机上要有的输入（相对数据目录，可以通配）：送过去前传好，开训前再查一遍；实验表步骤的 inputs 就是它 |
| `inputs_cmd` | `str?` | `None` | 另外打印输入的命令（每行一个相对数据目录的路径），在 store 上列要传的输入 |
| `fetch` | `dict` | `{'exclude': [], 'extra': []}` | 拉回 store：exclude（不拉的）、extra（out 上一级还要拉的，如 *.json）、also（另外要拉的文件夹，相对数据目录） |
| `archive` | `bool` | `False` | 拉回以后备份到 backup 那台、核对 |
| `relay` | `str` | `'path'` | 列输入、核对结果用哪个插件：path = 输入就是 inputs 里的（sites/names.py）；别的 = relay_plugins.py 的 |
| `machines` | `strs?` | `None` | 只在这几台执行机上做（和 PLACE 取交集）；None = 不限 |
| `device` | `device` | `'gpu'` | gpu（要卡）或 cpu |
| `step` | `str?` | `None` | 实验表的步骤名（advance 加的任务有；训练是 None）：PLACE、HOLD、看板按它 |
| `out_gb` | `num?` | `None` | 结果大约多少 GB（挑机器时留空间）；None = proto.OUT_GB |
<!-- /fields -->

### 插件

插件文件夹（`.taskq.toml` 的 `plugins`）里按文件名找，每类只取这些名字，只 import `taskq_api`（`queue/taskq_api.py`）：

| 文件 | 导出 | 用途 |
|---|---|---|
| `recipes.py` | `RECIPES`、`HOOKS` | 做法；钩子 `model_of`（结果文件夹 → 训它的任务的 model 参数） |
| `relay_plugins.py` | `PLUGINS`、`ACCEPTS`、`VIEW_ROOTS` | 送到执行机时怎么列输入、建视图、核对结果（`queue/relay_plugins.py`、`sites/names.py`） |
| `consumers.py` | `CONSUMERS` | 消费者插件（见"消费者"） |
| `smoke.py` | `SMOKE` | 冒烟的定义（见"冒烟"） |
| `labels.py` | `STEPS`、`QUIET`、`PLAN_TITLES` | 推送和进度报告里的白话（`monitor/plain.py`） |
| `readers/*.py` | `PROGRESS`、`INFO`、`TRAINED` | 进度、训练设置、做完一轮没有的读法（`queue/spec.py` 读，只用标准库） |

各表合并时键重复报错。

## 出问题看哪里

| 想知道 | 看 |
|---|---|
| 现在有什么问题 | `taskq problems`（最近一次健康检查查到的；`--all` 另列只提醒的和还没到推送时候的） |
| 发生过什么 | `taskq events`（`--key`、`--site`、`--source`、`-f`）；`relay`、`feed`、`advance`、`health` 另写一份纯文字日志（`OUTPUTS` 的 `logs` 文件夹，默认家目录：`relay.log` 等）|
| 一个任务 | `taskq show <key>`（说明和状态）、`taskq log <key>`（每段训练、事件、hub 的任务记录）、`taskq progress --key <key>` |
| 这一轮会做什么 | `taskq plan`（只看，不改） |
| 一台执行机 | `taskq site status <机器>`（那边的队列）、`taskq site envs <机器>`（环境）；hub 状态文件夹的 `relay/status.json` |
| worker 自己 | 执行机根目录的 `data/logs/workers/`（worker、显存填充、Slurm 作业的输出）；pueue 执行机上 `pueue log` |
| 训练的输出 | 分段日志 `data/logs/tasks/<key>__<作业>_part<段>.log`（执行机上；拉回时一起到 store 的同一位置） |
| 配置写对没有 | `taskq check`（`--quick` 不连 store） |

推送的问题编号是 `<种类>:<机器或 key>…`（种类见 `proto.ALERTS`），同一个问题只推一次；处理好了下一轮自己消失。
重启这类"发生过的事"用 `taskq notice ack` 确认。

## 怎么运转

### 任务队列

总队列和每台执行机的队列是同一种队列（`queue/taskq.py`）：`tasks.jsonl` 每行一个任务（档位 `nice`、进队列的先后 `order`、key、做法、参数），
`claims/<key>/` 是领取（心跳 5 分钟一次，20 分钟没续算过期），另有失败、跳过、缺输入、交还、停止的记录。任务状态见 `proto.STATES`。

领的先后：档位小的先；同档里做了一半的先（有存档，或这台有分段日志、没有失败记录）；再按进队列的先后。
整个队列 3 小时内失败 3 次就暂停领任务。`taskq stop` 的任务在训的 worker 30 秒内停下、交还，之后不再领；`taskq resume` 撤销。

### worker（consume.py）和消费者

worker 是 `queue/consume.py`（`queue/worker_start.sh` 激活机器表的 conda 后开它）：从这台的队列领任务、开 `queue/run_task.sh`、
收退出码、心跳、停、交还。`run_task.sh` 按任务说明激活环境、设环境变量、在代码包里的项目根目录跑命令，输出写进分段日志；
训练命令要能接着上次的存档做（交还以后别的 worker 从存档接着训）。

#### 消费者

各种执行机的差别都在消费者插件里：核心的 `queue/consumers/`（按这个顺序），再接项目的 `consumers.py` 的 `CONSUMERS`。
插件是模块或 dict，钩子（`consume.HOOKS`）都可省：

| 钩子 | 什么时候 |
|---|---|
| `active(w)` | 开始时：这个插件用不用（不写 = 用） |
| `setup(w)` | 开始时：设 worker 的名字、同时训几个、时限等 |
| `admit(w)` | 每次要再领一个前：返回原因 = 先不领 |
| `before_start(w)` | 开训前 |
| `env(w, env)` | 开训前：改给 `run_task.sh` 的环境变量 |
| `tick(w)` | 每轮 |
| `idle(w, why, now, since, err_since)` | 手上没有在训的：返回睡几秒，`None` = 退出 |
| `on_exit(w)` | 退出前 |

| 插件 | 做什么 |
|---|---|
| `slurm` | Slurm 作业里：作业号当 worker 名，按分到的 CPU 和内存定同时训几个，作业时限，节点本地的暂存目录，空闲一段时间退出 |
| `resident` | pueue 组里的常驻 worker：worker id 当名字，核数、同时训几个按机器表的 worker，空闲满一段时间退出（退出码 0，下一轮 hub 再开） |
| `gpu_pad` | 有显存填充时：开训前让它放到 floor，算还空多少显存时扣掉它占的 |
| `window` | 机器表 worker 的 `hours`：时段外不领；时段结束时停下手上的、交还、退出 |
| `gpu_idle` | 卡上没有别人的进程，GPU 使用记录里最近 `idle` 分钟也没有别人，才领 |
| `gpu_share` | 一张卡训第二个：上一个开训一段时间后，GPU 平均使用率低、还空着一半显存，再领一个 |

一张卡训几个、什么时候补，只看这些运行时的实况，不按模型分类。

#### Slurm 作业里的 worker

hub 每轮按 `refill.slurm_plan` 给 Slurm 执行机补作业（`sites/slurm.py` 交 `queue/worker.sbatch` 或 `queue/cpu_worker.sbatch`）：
这台有要卡的任务（备好没人领的，或在训的）时按 `SLURM[机器]["gpu"]` 每个 QOS 的名额排满，让排着的作业一直攒排队时间；
纯 CPU 的任务按 `SLURM[机器]["cpu"]` 交，最多几个。作业的参数是 `R/code/current/…/worker_start.sh`，作业开始时才解析
`current`，用的是那时的代码包；worker 要领新任务时才换到新的 `current`。
有输入要拷到节点本地盘（`res.local_inputs`）时，开训后在后台拷（`queue/inputs.py`）。

### 送任务：feed → relay → refill.choose

送任务只有一个入口：健康检查调 `queue/feed.py` 的 `run_all`，它交给 `queue/relay.py`。每轮：

1. **读**（`prepare`）：每台没暂停的执行机 ssh 一次读那边的队列、各任务进度、作业、剩余空间、环境、代码包
   （`site_ops status`，`@@JSON` 一行）；代码包和 hub 要的不同时先推新包再读。空间低于 `budget` 第一项时清理（见"数据"）。
2. **每台的动作**（`plan`）：总队列里分给这台的每个任务——
   - 还没放进那边的队列 → `stage`（在核心组排 `taskq internal stage`：store 补传缺的数据、建视图、加进那边的队列；退出码 75 = 这次排不上，下次再传）；
   - 那边排着、在训 → `beat`（续上总队列里的领取）；那边做完 → `fetch`（拉回 store、核对）；
   - store 上有了 → `drop`（删那边的任务和结果）；要归档的先 `archive`（store 备份到备份机，成功才交还）；
   - 那边失败：没有进度 → `release --skip`（这台以后不再领它）；有进度 → 领取保留、报出来；
   - 总队列里停了、撤销停止、改了档位 → `rstop`、`rresume`、`rnice`；那边缺输入 → `restage`；
   - 代码包或冒烟闸门挡着 → `hold`，不挡了 → `unhold`。
3. **派**（`assign`、`refill.choose`）：总队列里没人领的任务一次分给各台。先后：档位 → 同档里做了一半的先 → 进队列的先后。
   每个任务能去的执行机 = `PLACE[步骤或做法]` 和说明的 `machines` 的交集，再限有这种 worker（gpu / cpu）的执行机；去掉跳过过它的
   （全被跳过 → 报 `tqblocked`）；做了一半的只给有它进度的那台；输入已经在那边的排前面。一台要依次通过（`Site.fit`）：
   有这种 worker、环境就绪、有空位（备着没人领的不到 `READY[机器]` 个，档位 < 0 的不占；备满时可以换掉一个没进度的）、
   输入在 store 上、放得下（输入 + `out_gb` 之后盘上至少还剩 `budget` 第二项；`blob_cap` 时 blobs 的盘另算）、插件接受、
   代码包闸门和冒烟闸门不挡。输入大小在 store 上量（`site_ops probe`）。
4. **做**（`finish`）：照着做，再补 worker：pueue 机器上每个 worker 在时段里、没有活着的、上一个没失败、那边有备好没人领的这种任务时开；
   Slurm 机器按上面的规则交作业。要开几个只在这里算，`sites/` 的适配器只读状态、照着开。

连不上超过 2 小时报警；所有 worker 都不在时段里时不派任务。总队列里分给了不是执行机的名字的任务由 `feed.py` 报。

#### 代码包闸门

送说明之前，用那台在用的代码包在 hub 上跑一遍执行机对说明做的事（`queue/bundle_gate.py`），跑不过就不送（`hold`，报 `bundlegate`），
那台换了包以后重新核对。

### 训完以后：advance 和 flow

`queue/flow/compile.py` 从实验表推出实例和先后：同一个任务里按顺序（训练 → `after` 的第一步 → 第二步 …）；输入（含通配）落在别的实例的产出里
就接在那个实例后面，不管属于哪个实验；不在任何产出里的输入是原始数据。核对：模板里不认识的名字、产出带通配、产出重叠、输入落在自己的产出里、有环。

`queue/advance.py` 每轮：

1. 在 store 上看每个实例做完没有（`done` 的文件都在；还在拉回的不算）；
2. 先要做完的都做完了、自己没做完、不在总队列里的，加进总队列（做法 `command`，key = 实例 id，`step` = 步骤名）；放到哪台由派任务时的 `PLACE` 定；
3. `rerun=True` 的步骤：输入比 done 文件新就在 store 上删掉 done、重排；
4. `temp`：用到它的实例都做完、结果都在 store 上之后，在 store 上删掉这些中间产物，记进事件。

`HOLD` 对上的不排；`AUTO["advance"]` 关着时只写"会排什么"。

### store 和 site_ops

所有读写数据的操作都在数据所在的机器上跑：hub 经 `relay_remote.op`（执行机）、`store_op`（store）在那台的代码包里跑
`queue/site_ops.py <操作>`（参数是标准输入上的 JSON，结果是 `@@JSON` 一行）。操作的列表在 `site_ops.py` 开头：执行机根目录的
`status`、`states`、`missing`、`pin`、`ingest`、`view`、`list`、`drop`、`gc`、`env_mark`；store 的 `done`、`probe`、`rm`、`export`、`pull`、
`collect`、`smoke`、`archive`；送文件的 `push`。

### 数据：blob、视图、拉回

- **blob**：执行机上的数据文件按 sha256 存在 `R/blobs/<前两位>/<sha256>`（`proto.blob`），旁边的 `.st` 记收进来时的大小和修改时间，
  对不上就丢掉重收。store `export` 列出一个任务要的文件和 sha256（缓存在 `R/state/hashes.db`），`push` 只送那边没有的；同一个盘上已有的文件硬链接进来（`ingest`）。
- **视图**：每个任务的输入在 `R/data` 下照 store 数据目录的相对路径建成指向 blob 的软链接（`view`，从不盖掉真文件；建到一半的文件夹有 `.incomplete`），
  命令照常读 `{data}` 下的路径。正在传、视图还没建好的任务钉住它的 blob（`R/state/pins/`）。
- **清理**（`gc`，`queue/relay_store.py`）：剩余低于 `budget` 第一项时先删总队列不再要的视图，再按最久没用删没有链接指向的 blob；
  钉着的、传到一半的不碰。`cap`、`blob_cap` 限根目录和 blobs 各占多少。
- **拉回**（`fetch` → store 的 `pull`）：那边列出产出（`list`，和"做完没有"同一条规则），拉到 store 数据目录下的 `.relay_in/<key>`，
  核对后放进数据目录，标志文件（`MANIFEST.json`、最上层的文件）最后放。做完没有看盘上实际的文件；有拷贝、拉回、传输在跑时不算做完、不清理源头。
- **归档**：`archive=True` 的任务拉回后由 `queue/archive_task.sh` 从 store 备份到备份机、核对。

#### 传输

store 和执行机之间的大批文件经 `sync/tunnel.py`：store 开一个文件服务，地址是机器表 store 的 `transfer`（临时地址用 `quick`），
连接都由执行机发起。机器表的 `tunnel` 写这台用哪个方向：`push`（store → 这里，那边下载；Slurm 上可以在 `transfer_job` 的节点上跑）、
`pull`（这里 → store，分块上传）。同一台机器上的（如 store 兼执行机、`ssh=None` 的执行机）直接本机拷，blob 硬链接，
写到 `.tunnelpart` 再改名。`taskq tunnel rsync` 是 rsync 的替身：先经隧道传，再照原样跑 rsync。

### 显存填充

Slurm 作业里、机器表写了 `gpu_pad=(floor, target, high)`（MiB）时，`queue/gpu_pad_start.sh` 在作业里开 `queue/gpu_pad.py`：
没有在训的时候占着 floor；训练的显存稳定后补到 target；总占用超过 high 就让出。worker 开训前让它放到 floor（`gpu_pad` 消费者，
`taskq pad room` 也行），算还空多少显存时扣掉它占的。总开关：执行机根目录下有 `R/queue/gpu_pad_DISABLED` 时不开。阈值另可由
`TASKQ_GPU_PAD_*` 环境变量给。

### 环境

机器表每台的 `envs` 写环境名怎么激活：`('conda', 名)`、`('venv', 目录, 基础环境)`，或 `'spec'` / `('spec', 变体)` = 按描述建。

#### 建环境

描述是 `<配置>/specs/<环境>.yml`（`python`、`channels`、`conda`、`base`、`torch: {变体: {index_url, packages}}`、`pip`、`pip_no_deps`），
检查是同名的 `.check.py`。`AUTO["envs"]` 开着时，每轮看每台要的环境（机器表 `envs` 里写 `'spec'` 的、它们的基础环境，加 `AUTO["envs"][机器]` 列的）：没建、描述变了、
在建的停了的，在 hub 的核心组排一个 `taskq site env <机器> <环境>`（一台一次一个）；它生成一段 bash 交给那台在 CPU 上跑
（Slurm 用 `env_job` 的 srun）。新版建在 `<名>-<哈希>/` 里，检查过了才换 `<名>` 这个软链接、写 `.ok`，在跑的任务不受影响。
建坏的、空间不够的报 `site:<机器>:env:<环境>`。派任务时环境没就绪的执行机不选。

#### 环境的锁

`taskq env lock <名> [--variant V]`（或 `--all`）把描述解成 `<名>.<变体>.lock.json`：每个 conda 包、pip 包的确切版本、网址、sha256，
文件下到 hub 状态文件夹的 `wheelhouse/`。有锁时执行机不联网建：hub 把锁里的文件当 blob 送进那边的根目录，
那边 `conda create --offline`、`pip install --no-index --require-hashes`。

### 冒烟

`AUTO["smoke"]` 开着时，一种任务（做法 + 插件 `smoke.py` 里 `by` 列的参数 + 环境）第一次送到一台执行机之前，先在那台上跑一个小的冒烟任务
（key `_smoke/<执行机>/…`，档位 -1000），过了才送真的。定义 `SMOKE = {做法: dict(by=[参数名], params=…, vars=…)}`，`params`、`vars`
可以是函数（真任务的参数, store 的数据目录），在 store 上算。记录在总队列的 `smoke/<执行机>.json`（过了、没过、排着）；真任务在那台训完也算过了。
`taskq smoke status`、`retry`、`adopt`。

### 别人的任务（任务单）

`taskq add --file <单子.json>`：一张 `{字段: 值}` 或一列。`recipe` 不写 = `command`；`nice` 写在单子里（几张单子要一样）；
其余字段都是做法的参数（`command` 要 `key`、`cmd`、`env`、`out`、`done`）。例：

```json
{"key": "alice/tune-01", "cmd": "python3 train.py --out {data}/alice/tune-01", "env": "torch",
 "out": "alice/tune-01", "done": "DONE", "resume": "ckpt_*.pt", "res": {"gpu_mem": 20000}, "owner": "alice", "nice": 5}
```

命令要能接着做：交还以后别的 worker 会在别的机器上从 `resume` 的存档接着跑，所以存档写在 `out` 下。

### 切换格式

盘上的格式改了时，在一次切换里（停 cron、交还在训的任务、推代码、各执行机换新包）跑一次 `taskq self migrate`（默认只列出，`--apply` 才改；`--rename 旧名=新名`、`--merge 旧名=旧根目录`）：
hub 上的总队列、relay 的记账、冒烟记录，和各执行机、store 根目录里的队列、分段日志。

## 文件布局

执行机、store 的根目录 R（机器表的 `root`，`proto.ROOT`）：

| 路径 | 内容 |
|---|---|
| `code/<包名>/`、`code/current` | 代码包；`current` 指向在用的那个 |
| `queue/` | 这台的任务队列（`ENABLED` 是总开关：没有它时排着的 worker 开始就结束） |
| `blobs/<前两位>/<sha256>` | 数据文件（可以是指到别的盘的软链接） |
| `data/` | 数据目录：各任务的视图和产出、`logs/tasks/`（分段日志）、`logs/workers/` |
| `state/` | `pins/`、`envs/`（环境的 `.ok`）、`hashes.db`、`view.lock`、`deploy.lock` |
| `tmp/` | 传到一半的文件 |
| `envs/` | 这台建的环境 |

hub 的状态文件夹（`proto.STATE_NAMES`）：`taskq/`（总队列）、`relay/`（relay 的记账、`status.json`）、`health/`、`taskq.db`（事件、问题、进度）、
`wheelhouse/`、`tunnel*` 等。

核心的源码：

| 目录 | 内容 |
|---|---|
| `bin/taskq` | 命令行入口（`TREE`） |
| `queue/` | 队列、worker、任务说明、调度（advance、feed、relay、refill）、site_ops、协议 `proto.py`、字段表 `schema.py`、核对 `check.py` |
| `queue/flow/` | 实验表 → 实例和先后 |
| `queue/consumers/` | 消费者插件 |
| `sites/` | 机器表（`mtab.py`）、pueue / Slurm / ssh 的适配器（`pueue.py`、`slurm.py`、`remote.py`）、建环境（`envs.py`、`envlock.py`） |
| `sync/` | 隧道传输 |
| `monitor/` | 健康检查、推送、GPU 使用记录、状态汇总 |

## 命令行和约定

- 协议只在 `queue/proto.py`：盘上的名字、时间常数、任务状态、退出码（表见 [`../README.md`](../README.md) 的"约定"）、字段表、报警格式、
  `TASKQ_*` 环境变量（`proto.ENV`）。核心别处不手写这些字面量（`queue/test_protocol.py` 核对）。
- 改格式（盘上的布局、队列、标志文件、JSON 字段、代码包）时，旧状态的改写写进 `taskq self migrate`，在一次切换里做完，代码里不留读旧格式的分支。
- `--json` 输出一行 JSON，字段表在 `proto.py`、带 `version`；经 ssh 读回的冻结命令（`taskq status --json`、`taskq spec collect --json`）
  那一行以 `@@JSON ` 开头，hub 只认这一行。
- `taskq worker`、`taskq spec`、`taskq internal` 是 worker、执行机上的程序和调度自己用的。不经切换就上线的改动不改 `worker next/beat/release` 的参数和退出码、
  带版本号的 `status --json`、`spec collect --json`（字段只加），也不改启动脚本在代码包里的路径（排着的 Slurm 作业开始时按 `code/current` 下同一个路径找）。
- 写到一半的文件以 `.tmp` 结尾，写完再改名；文件夹建到一半时里面有 `.incomplete`。

## 加新东西

- **新字段**：加进 `queue/schema.py` 的表（`taskq check` 自动核对），在用到的地方读；再跑 `taskq machines fields --markdown`，把输出贴到这里对应的标记之间。
- **新命令**：加进 `bin/taskq` 的 `TREE` 和 `SECTIONS`，再在 `../README.md` 的命令表里加一行（`queue/test_readme.py` 核对）。
- **新的盘上名字、常数、退出码、环境变量**：只加在 `queue/proto.py`。
- **新的执行机差别**：写成消费者插件（核心的 `queue/consumers/`，或项目的 `consumers.py`），不在 `consume.py` 里按机器另写一条路。
- **新的调度方式**：加 `sites/` 的适配器（读状态、交作业、开 worker），要开几个仍在 `relay.topup` 里算。
- **新的传结果的路**：数盘上实际的文件；标志文件最后拷；传输在跑时不算做完、不清理源头；加"拷到一半"的测试。
- **项目专有的东西**（模型、数据集、机器名、路径）：写在项目的配置和插件里；核心缺一种表达时加一种通用的写法和测试（`queue/test_core_clean.py` 核对）。
