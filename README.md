# taskq

通用的任务调度：总队列在一台机器（hub）上，几台执行机（pueue 常驻 worker 的工作站、Slurm 集群）按机器表接进来，
从总队列领任务、训练、交还。数据有一个家（store）：输入从 store 送到执行机，做完的结果拉回 store、需要时再备份到备份机，
执行机的盘只用来中转。训完以后的步骤（推理、评估、汇总……）按实验表自动往下排。

hub 上 cron 每 30 分钟跑一轮健康检查（`taskq tick`）：查各台机器、推送问题、排下一步、给各执行机派任务、补 worker、拉回结果。
调度不认识具体的模型和训练器：一种任务怎么跑、要什么环境和数据、怎么算做完，都写在项目的做法（插件）和实验表里；
一张卡训几个、什么时候让出显存，看运行时的实况（GPU 使用率、显存、失败记录）。

一个项目只经三样东西用它：配置文件夹（`machines.py`、`policy.py`）、插件文件夹、命令行。
机制、配置的字段和插件的写法见 [`queue/README.md`](queue/README.md)。

## 安装

```bash
bash install.sh      # 在 taskq 的克隆里；不先克隆时 curl 下载 install.sh 交给 bash 的写法见 install.sh 开头
```

克隆到 `${TASKQ_HOME:-~/.local/share/taskq}`，`~/.local/bin/taskq` 链到它的 `bin/taskq`；`TASKQ_ORIGIN` 改克隆的来源。
重复运行没关系：已经装了就更新（`taskq self update`，拉 `origin/main`）。

要的东西：Python 3（核心只用标准库）、ssh；pueue 机器、hub、store 上要有 [pueue](https://github.com/Nukesor/pueue)
（`queue/install_pueue.sh` 装）；执行机上要有 conda（机器表的 `conda`）。
各台的 pueue 常驻进程、组和 hub 的 crontab 由 `taskq site setup [机器]` 建。

## 项目（`.taskq.toml`）

项目根目录里放一个 `.taskq.toml`，写明配置和插件文件夹（相对项目根目录）：

```toml
config = "config"        # machines.py、policy.py 所在的文件夹
plugins = ["plugins"]    # 插件文件夹（recipes.py、relay_plugins.py、consumers.py、smoke.py、labels.py、readers/）
```

找项目的顺序：`taskq --project <文件夹>`（经 `TASKQ_PROJECT` 传给子进程）> 从当前目录往上找 `.taskq.toml`。
配置文件夹也可以由 `TASKQ_CONFIG` 指定。

| 文件 | 写什么 |
|---|---|
| `<配置>/machines.py` | 机器表 `MACHINES`：一台物理机器一项，角色（hub、store、backup）、执行机的根目录和调度方式（pueue 的 worker、Slurm 的作业） |
| `<配置>/policy.py` | 怎么用：自动的开关 `AUTO`、暂停 `PAUSE`、暂缓 `HOLD`、能去哪 `PLACE`、备几个 `READY`、Slurm 交几个 `SLURM`、项目的输出 `OUTPUTS` |
| `<配置>/specs/<环境>.yml` | 执行机上按描述建的环境（`sites/envs.py`） |
| `<插件>/recipes.py` | 做法：一种任务怎么跑、要什么环境和数据、怎么算做完 |
| `OUTPUTS["experiments"]` 指的文件 | 实验表 `EXPERIMENTS`、`FINAL`：每个实验训什么、训完以后做哪些步骤 |

字段表见 [`queue/README.md`](queue/README.md) 的"配置"，或 `taskq machines fields`。改完配置跑 `taskq check`。

## 命令

`taskq --help` 列出全部，`taskq <命令> --help`（或 `taskq help <命令>`）看一个命令。
查看的命令只读；改队列的命令在 hub 上跑。带 `--json` 的命令输出一行 JSON（字段见 `queue/proto.py`）。

<!-- commands -->

**队列**

| 命令 | 做什么 |
|---|---|
| `taskq add` | 加任务：实验表（--exp）、做法（--recipe）或任务单（--file） |
| `taskq set` | 改一个任务的参数 |
| `taskq nice` | 改档位（先后） |
| `taskq show` | 一个任务的完整说明和状态 |
| `taskq log` | 一个任务的每段训练（分段日志）、事件日志里关于它的、hub 的任务记录 |
| `taskq status` | 队列（--all：所有任务的状态、在哪台、进度、预计完成） |
| `taskq stop` | 停止任务（在训的 30 秒内停下、交还，之后不再领） |
| `taskq resume` | 撤销停止 |
| `taskq rm` | 删掉没人领着的任务 |

**调度**

| 命令 | 做什么 |
|---|---|
| `taskq tick` | 健康检查一轮（真做：推送、排下一步、送任务） |
| `taskq plan` | 只看：这一轮训完以后排什么、送什么任务 |
| `taskq boot` | 开机（@reboot）：启动 pueued、放回被打断的任务 |
| `taskq problems` | 最近一次健康检查查到的问题（[--all] [--json]） |
| `taskq notice set` | 挂上或改文字（<名字> [文字…]；不给文字时读标准输入） |
| `taskq notice clear` | 撤掉（<名字>…） |
| `taskq notice list` | 挂着的（[--json]） |
| `taskq notice ack` | 确认健康检查报的"发生过的事"（如重启；<问题编号或前缀>…），之后不再列出 |
| `taskq events` | 事件日志：发生过什么（--since --key --site --source --kind --grep、-f、--json） |
| `taskq note` | 往事件日志里记一条（出了什么事、做了什么） |
| `taskq progress` | 任务进度随时间的变化（[--key K] [--since T] [--json]） |
| `taskq notify` | 推送一条消息到手机（ntfy），并记进事件日志 |

**执行机和实验**

| 命令 | 做什么 |
|---|---|
| `taskq site status` | 那边的队列 |
| `taskq site bundle` | 打包、推过去、切换 current |
| `taskq site resend` | 传过去的任务按现在的做法重新解析、换掉那边的说明 |
| `taskq site env` | 在那台上建环境、跑检查 |
| `taskq site envs` | 只看：那台每个环境的状态 |
| `taskq site setup` | pueue 常驻进程、组、crontab（不给名字：本机） |
| `taskq env lock` | 把环境描述解成锁 <名>.<变体>.lock.json，文件下到 wheelhouse（不装进任何环境） |
| `taskq smoke status` | 各台的记录（[执行机] [--json]） |
| `taskq smoke retry` | 没过的重新排（<执行机> [种类]） |
| `taskq smoke adopt` | 分给各台的、训完交还的任务的种类记成过了（[--dry-run]） |
| `taskq exp list` | 列出实验 |
| `taskq exp tasks` | 展开后的任务（--json） |
| `taskq exp env-of` | 模型文件夹 → 训它用的环境 |
| `taskq exp name-of` | 模型文件夹 → 它所属实验的 names 里这几项（每行一个） |
| `taskq exp steps` | 步骤，命令按 names 填好（--done 判断做完没有，--needs 列输入；JSON） |

**依赖**

| 命令 | 做什么 |
|---|---|
| `taskq who` | 这个文件是哪个实例的产出 |
| `taskq dag` | 实例之间的先后（--dot 画图，--check 核对） |

**其他**

| 命令 | 做什么 |
|---|---|
| `taskq tunnel rsync` | rsync 的替身：先经隧道传，再照原样跑 rsync |
| `taskq tunnel push` | 本机 → 那边 |
| `taskq tunnel pull` | 那边 → 本机 |
| `taskq tunnel push-dirs` | 本机 → 那边，整个子目录，传完核对 |
| `taskq gpu usage` | 各卡按钟点被谁占着、别人的每段使用（[目录] [--me 账号] [--days N] [--json]） |
| `taskq pad room` | 让填充放到 floor、等它说好了（[--wait 秒]；没有填充时什么都不做） |
| `taskq pueue-add` | 往本机 pueue 的手动组（manual_groups）加一次性任务（工作目录是这台的 repo） |
| `taskq pueue-status` | 各 pueue 机器的队列（只看） |
| `taskq check` | 核对整个项目（配置、插件、实验表、所有任务） |
| `taskq db migrate` | 建库或做还没做的迁移（只在 hub 上） |
| `taskq db path` | 在哪 |
| `taskq db version` | 盘上和这份代码的表结构版本 |
| `taskq path` | 核心的位置：lib / api（queue/）、core（根） |
| `taskq machines` | 机器表（get、names、activate、show…） |
| `taskq self update` | 拉 origin/main（失败重试 5 次，拉不到就留在原来的版本） |
| `taskq self version` | 版本 |
| `taskq self test` | 跑核心的全部测试（各自的临时 HOME，同时跑几个，结果表） |
| `taskq self migrate` | 盘上的旧格式改成现在的（切换时跑一次；默认只列出，--apply 才改） |

**底层命令（worker、执行机上的程序、调度自己用；参数和退出码不改）**

| 命令 | 做什么 |
|---|---|
| `taskq worker next` | 领一个任务 |
| `taskq worker beat` | 心跳 |
| `taskq worker release` | 交还 |
| `taskq worker missing` | 缺输入，交还 |
| `taskq spec shell` | 给 run_task.sh 的 shell 语句（按本机展开） |
| `taskq spec done` | 训完了退出码 0，没训完 1，说明有问题 2，读不了数据目录 5 |
| `taskq spec resolve` | 解析好的说明（JSON，一行） |
| `taskq spec lookup` | 按 key 找任务（先查实验表，再查队列），打印说明（JSON） |
| `taskq spec outputs` | 这个任务的输出（JSON）；--sh：给 archive_task.sh 的 shell 语句 |
| `taskq spec collect` | 本机的训练状态（跨版本读用）：输出 @@JSON 一行 |
| `taskq spec export` | 中继插件送出：要传的文件、视图、拉回的文件夹（JSON） |
| `taskq spec import` | 中继插件拉回核对，放进 --dst（JSON） |
| `taskq internal stage` | 传数据、加进执行机的队列（排不上退出码 75） |
| `taskq internal fetch` | 拉回做完的任务（落到 store） |
| `taskq internal archive` | store 上做完的结果备份到备份机 |

<!-- /commands -->

## 测试

```bash
taskq self test              # 全部：queue/、sites/、sync/、monitor/ 下的 test_*.py、test_*.sh，同时跑几个（-j N，默认 4）
taskq self test -l           # 只列出会跑哪些
taskq self test relay spec   # 只跑名字或路径里带这些的（relay → test_relay*.py；可以用 * ?）
taskq self test --gpu        # 也跑要真卡的（文件开头写着"# 要真卡"）
```

每个文件在核心的根目录下跑，有自己的临时 HOME，环境只有 `PATH`、`LANG`、`HOME`、`TMPDIR`，只用固定配置 `queue/testdata/`，
不读使用者项目。每个文件一行结果（结果、秒数、"通过："的项数、跳过的原因），同时写进 `~/.cache/taskq-test/report.tsv`，
每个文件的输出在同一目录的 `log_<文件>.txt`。缺 numpy、pueue 等的测试退出 77，算跳过。退出码：0 都通过（或跳过），1 有没通过的，2 没有匹配的测试。

改了哪个文件跑哪些（同一个改动最后再跑一次全套；`queue/proto.py`、`bin/taskq` 这类大家都用的，直接跑全套）：

<!-- tests -->
| 改了 | 跑 |
|---|---|
| `bin/taskq` | `test_cli.py`、`test_cli_json.py`、`test_readme.py` |
| `README.md`、`queue/README.md`、`queue/schema.py` | `test_readme.py`、`test_check.py` |
| `queue/proto.py` | `test_protocol.py`、`test_core_clean.py`，再全套 |
| 核心的任何文件（不写具体项目、机器、路径） | `test_core_clean.py` |
| `queue/cfg.py`、`queue/project.py`、`sites/mtab.py` | `test_cfg.py`、`test_common.py` |
| `queue/common.py` | `test_common.py` |
| `queue/check.py` | `test_check.py`、`test_example.py` |
| `example/` | `test_example.py` |
| `queue/taskq.py` | `test_taskq.py`、`test_races.py`、`test_worker.sh` |
| `queue/consume.py`、`queue/consumers/`、`queue/worker_start.sh` | `test_consume.sh`、`test_worker.sh`、`test_worker_code.sh`、`test_worker_stall.sh`、`test_worker_stop.sh` |
| `queue/run_task.sh`、`queue/inputs.py` | `test_run_task.sh`、`test_inputs.py` |
| `queue/gpu_pad.py`、`queue/gpu_pad_start.sh` | `test_gpu_pad.py`、`test_pad_room.py`、`test_gpu_pad_launcher.sh`；有卡时 `test_gpu_pad_integration.sh`（`--gpu`） |
| `queue/spec.py`、`queue/recipes.py` | `test_spec.py` |
| `queue/experiments.py` | `test_experiments_cli.py`、`test_example.py` |
| `queue/flow/` | `test_flow_compile.py`、`test_advance.py` |
| `queue/advance.py` | `test_advance.py`、`test_smoke.py` |
| `queue/feed.py` | `test_feed.py` |
| `queue/refill.py` | `test_refill.py`、`test_relay.py` |
| `queue/relay.py`、`queue/relay_remote.py`、`queue/relay_transfer.py` | `test_relay.py`、`test_races.py`、`test_bundle_paths.py` |
| `queue/relay_store.py`、`queue/site_ops.py` | `test_relay_store.py`、`test_relay.py` |
| `queue/relay_bundle.py` | `test_relay_bundle.py`、`test_bundle_paths.py` |
| `queue/bundle_gate.py` | `test_bundle_gate.py` |
| `queue/relay_plugins.py`、`sites/names.py` | `test_relay_plugins.py` |
| `queue/smoke_gate.py` | `test_smoke.py` |
| `queue/archive_task.sh` | `test_archive_task.sh` |
| `queue/db.py`、`queue/events.py`、`queue/tasklog.py` | `test_db.py`、`test_tasklog.py` |
| `queue/cli_json.py` | `test_cli_json.py` |
| `queue/migrate.py` | `test_migrate.py` |
| `sites/pueue.py`、`sites/slurm.py`、`sites/remote.py`、`queue/setup.sh`、`queue/worker.sbatch`、`queue/cpu_worker.sbatch` | `test_adapters.py`、`test_bundle_paths.py` |
| `sites/envs.py`、`sites/envs_cli.py` | `test_envs.py` |
| `sites/envlock.py` | `test_envlock.py` |
| `sync/tunnel.py`、`sync/tunnel_push.sh` | `test_tunnel.py` |
| `monitor/health.py`、`monitor/notice.py`、`monitor/plain.py`、`monitor/push.py` | `test_health.py`、`test_probe.py`、`test_db.py` |
| `monitor/overview.py` | `test_overview.py` |
| `monitor/gpu_usage.py`、`monitor/gpu_log.sh` | `test_gpu_usage.py` |
| 跨几个模块的（派任务、送数据、训完、拉回一整轮） | `test_e2e.py` |
<!-- /tests -->

## 示例项目

[`example/`](example/) 是一个最小的项目：一台 hub（本机，兼 store）、一台 pueue 执行机、一台 Slurm 执行机（主机名是假的，连不上），
一种做法、两个实验，每个任务做完后跑一个步骤。只看的命令都不用连别的机器：

```bash
taskq --project example check
taskq --project example exp tasks
taskq --project example plan
```

每个字段都用到的写法见测试用的固定配置 [`queue/testdata/`](queue/testdata/)。

## 约定

- 只有 `main` 一个版本：推到 `main` 就是上线，hub 的 cron 下一轮 `taskq self update`；执行机的代码包下一轮自动重打、推过去，
  在跑的 worker 空下来时换包。
- 盘上的名字、时间常数、任务状态、退出码、字段表、报警格式、`TASKQ_*` 环境变量都只在 `queue/proto.py` 定义；改格式时旧状态的改写写进
  `taskq self migrate`，在一次切换里做完（见 [`queue/README.md`](queue/README.md) 的"命令行和约定"）。
- 带 `--json` 的命令输出一行 JSON，带 `version`，字段只加不改；hub 经 ssh 读回的命令（`taskq status --json`、`taskq spec collect --json`）
  那一行以 `@@JSON ` 开头。
- 不经切换就上线的改动不改 `taskq worker next/beat/release` 的参数和退出码（在跑的 worker、排着的 Slurm 作业在用）。

退出码（`proto.EXIT`）：

| 命令 | 退出码 |
|---|---|
| `taskq` 本身 | 0 好了；1 出错；2 用法错、找不到项目、不在 hub 上本机又没有队列；5 读不了数据目录（没建、盘没挂上） |
| `taskq show`、`set`、`stop`、`nice`、`add --exp` | 4 队列里（或实验表里）没有这个（有几个 key 的：有一个不在就是 4） |
| `taskq rm` | 0 删了；3 不删（有人领着、分出去了、别的任务等它）；4 一个都不在队列里 |
| `taskq status` | 1 读不到队列 |
| `taskq worker next` | 0 领到了；10 没有能领的；11 先等（给排在前面、刚停下的留位置）；12 暂停（整个队列 3 小时内失败 3 次，或读不了数据目录） |
| `taskq worker beat --strict` | 13 有的领取已经不是自己的 |
| `taskq worker missing` | 1 不是自己领着的 |
| `queue/run_task.sh` | 0 训完；1 训练出错；2 说明或环境有问题；3 命令正常结束但没训完；4 缺输入（没开训）；5 读不了盘、判断不了训完没有；143 收到 SIGTERM |
| `taskq spec done` | 0 做完了；1 没做完；2 说明有问题；5 读不了数据目录 |
| `taskq internal stage` | 75 这次排不上，下次检查再传（不报） |
| 调度跑的外部命令 | 124 超时或起不来 |
| `taskq check` | 0 没有问题；1 有问题 |
