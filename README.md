# taskq

任务队列和调度：总队列在 hub（角色 `hub` 的那台机器）上，执行机（Slurm 作业、pueue worker）领任务、训练，训完以后拉回、按实验表排下一步。核心不认识具体模型，项目通过配置（机器表、`policy.py`、实验表）和插件接入。结构和各模块见 [`queue/README.md`](queue/README.md)。

## 安装

```bash
bash install.sh          # 克隆到 ~/.local/share/taskq（TASKQ_HOME），~/.local/bin/taskq 链到 bin/taskq；重复运行 = 更新
taskq self update        # 拉 origin/main，失败重试 5 次，拉不到就留在原来的版本
```

只维护一个版本（GitHub 上的 main）。执行机上的作业用各自的代码包，不受更新影响。

## 项目

`taskq` 先找项目：`--project 目录` > `TASKQ_PROJECT` > 从当前目录往上找 `.taskq.toml` > 本机的默认项目 `~/.config/taskq/project`（一行，项目根目录；只在前面都找不到时用，不会像环境变量那样传给子进程）：

```toml
config = "config"      # machines.py、policy.py 所在的文件夹（相对这个文件）
plugins = ["plugins"]  # 插件文件夹，可以有几个
```

找到后设 `TASKQ_REPO`、`TASKQ_CONFIG`、`TASKQ_PLUGINS`，再调用核心（bash 脚本也认这几个变量）。完整的例子见 [`example/`](example/)，各个文件的写法见 [`queue/README.md`](queue/README.md) 的"开关和配置"。

## 命令

| 命令 | 做什么 | 原来的入口 |
|---|---|---|
| **队列** | | |
| `taskq add --exp <id> [--nice N] [--only k=v,…]`、`add --recipe <做法> k=v…`、`add --file <任务单>` | 加任务 | `taskq.py add` |
| `taskq set <key> k=v…` | 改一个任务的参数 | `taskq.py set` |
| `taskq nice --nice N <key>…` | 改档位 | `taskq.py set-nice` |
| `taskq show [--json] <key>` | 一个任务的说明和状态 | `taskq.py show` |
| `taskq log <key> [--data 目录] [--json]` | 一个任务的每段训练（分段日志的开头行：作业、机器、主机、第几段、环境、代码、开始；结束、退出码）、hub 上调度日志里提到它的行、hub 的任务记录（`queue/tasklog.py` 开头；JSON 字段 `proto.TASKLOG_*`） | — |
| `taskq status [--json] [--internal]` | 本机的队列（冒烟任务只在 `--internal` 时列） | `taskq.py status` |
| `taskq status --all [--json]` | 所有任务的状态、在哪台、进度、预计完成 | — |
| `taskq stop [--why 原因] <key>…`、`taskq resume <key>…` | 停止、撤销停止 | `taskq.py stop`、`resume` |
| `taskq rm <key>…` | 删掉没人领着的任务 | `taskq.py remove` |
| **调度** | | |
| `taskq tick [-n] [--test]` | 健康检查一轮（真做）；`-n` 只打印问题 | `monitor/health.py` |
| `taskq plan` | 只看：按时段开关、送任务、训完以后这一轮会做什么 | `shift.py --dry-run`、`feed.py`、`relay.py`、`advance.py` |
| `taskq boot [--gpu-log]` | 开机：启动 pueued、放回被打断的任务 | `queue/on_boot.sh` |
| `taskq notice set <名字> [文字…]`、`notice clear <名字>…`、`notice list [--json]` | 挂在健康检查上的提醒：每条报一个问题 `notice:<名字>`（进"需要处理"、推送一次），撤掉后消失 | `monitor/notice.py` |
| `taskq notice ack <问题编号或前缀>…` | 确认健康检查报的"发生过的事"（如 `boot:<机器>` 重启），之后不再列出；一直存在的问题确认不掉，问题消失后确认记录跟着删 | `monitor/notice.py` |
| **执行机** | | |
| `taskq site status\|bundle\|resend <名字>` | 那边的队列；打包推过去；重新解析传过去的任务 | `relay.py status\|bundle\|resend` |
| `taskq site env <机器> <环境> [-n]`、`taskq site envs <机器>` | 建环境；看环境的状态 | `sites/envs_cli.py`（11/4 前叫 `sites/site.py`） |
| `taskq env lock <环境> [--variant V]`、`taskq env lock --all` | 把环境描述解成锁 `<环境>.<变体>.lock.json`，文件下到 wheelhouse（不装；`queue/README.md` 的"环境的锁"） | `sites/envlock.py` |
| `taskq site setup [名字]` | pueue 常驻进程、组、crontab | `queue/setup.sh` |
| **实验** | | |
| `taskq exp list`、`taskq exp tasks [--json]` | 实验、展开后的任务 | — |
| `taskq exp steps [--exp ID…] [--key KEY…] [--level …] [--data 目录] [--done] [--needs]` | 实验表的步骤，命令按 `names` 填好；做完没有、`--stage needs` 列的输入（一行 JSON） | — |
| `taskq exp env-of <模型文件夹>`、`taskq exp name-of <模型文件夹> <名字>…` | 训它用的环境；实验的 `names` | `experiments.py env-of\|name-of` |
| **其他** | | |
| `taskq tunnel rsync\|push\|pull …` | 经隧道传大批文件 | `sync/tunnel.py` |
| `taskq tunnel push-dirs <机器> <源目录> <目标目录> <子目录>…` | 几个子目录整个传过去，带未完成标记，最后逐个文件核对 MD5 | `sync/tunnel_push.sh` |
| `taskq gpu usage [目录] [--me 账号] [--days N] [--json]` | GPU 使用记录（`monitor/gpu_log.sh`）的汇总：各卡按钟点别人 / 自己 / 空闲、别人的每段使用、总时长（JSON 字段 `proto.GPU_USAGE_*`） | — |
| `taskq pad room [--wait 秒]` | 作业里的命令自己要显存时：让显存填充放到 floor、等它说好了（同 worker 开训前的握手）；没有填充时什么都不做，没等到退出码 1 | — |
| `taskq pueue-add <组> [选项] -- '<命令>'` | 往本机 pueue 加一次性任务 | `queue/q` |
| `taskq pueue-status [机器…] [--json]` | 各 pueue 机器的队列（只看） | `queue/qs` |
| `taskq snapshot <克隆>` | 按提交号的代码副本 | `sync/snapshot.sh` |
| `taskq check` | 核对整个项目 | — |
| `taskq path lib\|api\|core` | 核心的 `queue/`（插件 `import taskq_api` 用）、核心的根 | — |
| `taskq machines …` | 机器表 | 配置文件夹里 `machines.py` 的命令行 |
| `taskq machines show [机器…]` | 核心按机器表算出来的值：种类、worker、开 worker 的命令、根目录的写法、能用的环境、激活语句、收编来源、给任务加的语句，环境描述的哈希和变体（一行 JSON） | — |
| `taskq spec export\|import --spec <JSON\|-> --root 目录 [--src 目录 --dst 目录]` | 中继插件送出的文件和视图；拉回核对、放进 `--dst`（一行 JSON） | `names.export\|import_` |
| `taskq self update\|version`、`taskq --version` | 更新、版本（`VERSION` 文件，没有时 `git describe`） | — |
| `taskq self test [-j N] [--out 目录] [--gpu] [文件或模式…]` | 跑核心的全部测试（见下面的"测试"） | — |
| **底层**（不在 `taskq --help` 的首页，参数和退出码冻结） | | |
| `taskq worker next\|beat\|release\|missing …` | worker 领任务、心跳、交还、缺输入 | `taskq.py next\|beat\|release\|missing` |
| `taskq spec shell\|done\|resolve\|lookup\|outputs\|collect …` | 任务说明 | `spec.py` |
| `taskq internal stage\|fetch <名字> <key>` | 调度自己排的传数据、拉回 | `relay.py stage\|fetch` |

`taskq help <命令>` 同 `taskq <命令> --help`。原来的脚本路径和参数照旧能用（在跑的作业、旧代码包还在调用）。

## 测试

```bash
taskq self test -j 4              # 全部：每个文件有自己的临时 HOME 和 TMPDIR，同时跑 4 个，上次慢的先跑
taskq self test relay e2e         # 只跑路径里有这些字的（也可以写文件名、通配）
taskq self test -l                # 只列出会跑哪些
```

跑的是 `queue/`、`sites/`、`sync/`、`monitor/` 下的 `test_*.py`、`test_*.sh`（`queue/run_tests.py`），用测试自己的固定配置（`queue/testdata/`），不读项目。结果表打在标准输出，同时写 `~/.cache/taskq/test/report.tsv`（`--out` 改位置），每个文件的输出在同一个文件夹的 `log_*.txt`；都通过（或跳过）退出码 0，否则 1。开头写了 `# 要真卡` 的（`test_gpu_pad_integration.sh`）只在 `--gpu` 时跑；测试缺东西时自己退出 77、打一行"跳过：原因"（`sites/test_e2e.py` 要 pueue 和带 numpy 的 Python），算跳过。每个文件最多跑 900 秒（`--timeout`）。

改了哪个文件跑哪些测试（只改了几个文件时先跑这些，`taskq self test <模式…>`；同一个改动全套只跑一次）：

| 改了 | 跑 |
|---|---|
| `queue/taskq.py` | `test_taskq.py`（"worker 的调用方式"一组必须过：`next` / `beat` / `release` 的参数和退出码只加不改）、`test_dicc_worker*.sh`、`test_races.py` |
| `consume.py`、`consumers/`、`dicc_worker.sh`、`worker_start.sh`、`worker.sbatch` | `test_consume.sh`、`test_dicc_worker.sh`、`test_dicc_worker_stop.sh`、`test_dicc_worker_code.sh`、`test_worker_stall.sh` |
| `run_task.sh`、`recipes.py`、`spec.py`、`inputs.py` | `test_run_task.sh`、`test_spec.py`、`test_inputs.py` |
| `archive_task.sh` | `test_archive_task.sh` |
| `advance.py`、`steps.py`、`flow/`、`ledger.py`、`run.py` | `test_advance.py`、`test_engine.py`、`test_flow_compile.py`、`test_ledger.py`、`test_run.py`、`test_publish.py` |
| `refill.py`、`feed.py`、`shift.py`、`smoke_gate.py` | `test_refill.py`、`test_feed.py`、`test_shift.py`、`test_smoke.py` |
| `relay.py`、`relay_bundle.py`、`bundle_gate.py`、`relay_remote.py`、`relay_transfer.py` | `test_relay.py`、`test_relay_bundle.py`、`test_bundle_paths.py`、`test_bundle_gate.py` |
| `relay_store.py` | `test_relay_store.py` |
| `relay_plugins.py`、`sites/names.py` | `test_relay_plugins.py` |
| `common.py`、`cfg.py`、`tasklog.py` | `test_common.py`、`test_cfg.py`、`test_tasklog.py` |
| `sites/` | `sites/test_envs.py`、`sites/test_adapters.py`、`sites/test_envlock.py`、`sites/test_e2e.py` |
| `monitor/` | `monitor/test_health.py`、`test_overview.py`、`test_probe.py`、`test_gpu_usage.py` |
| `bin/taskq`、`project.py`、`check.py`、`experiments.py`、`cli_json.py` | `test_cli.py`、`test_check.py`、`test_experiments_cli.py`、`test_cli_json.py`、`test_example.py` |
| `sync/` | `sync/test_tunnel.py` |
| `gpu_pad*`、`consumers/gpu_pad.py` | `test_gpu_pad.py`、`test_gpu_pad_launcher.sh`、`test_pad_room.py`（有真卡时加 `--gpu` 跑 `test_gpu_pad_integration.sh`） |
| `proto.py` | `test_protocol.py` |
| 任何非测试文件的文字 | `test_core_clean.py` |

新写传结果的路（新步骤、新执行机）时加"拷到一半"的测试；测试里替换掉的外部命令至少有一条真跑。测试只杀自己记下的 PID（`$!`、限定到测试目录的 `pgrep`），不用 `pkill -f` 通用匹配。

GitHub 上每次推送跑 `.github/workflows/test.yml`：Python 3.12、装 numpy、pyyaml 和 pueue（`queue/install_pueue.sh`），跑 `taskq self test`，再核对示例项目。

## 示例项目

[`example/`](example/)：一个最小的项目（机器表、`policy.py`、一个插件、实验表），执行机的主机名是假的。`taskq --project example check`、`status`、`plan` 等不用连别的机器，说明见 [`example/README.md`](example/README.md)。

## 约定

见 [`queue/README.md`](queue/README.md) 的"命令行"。退出码：

| 退出码 | 意思 |
|---|---|
| 0 | 成功 |
| 1 | 出错（`spec done`：没训完；`run_task.sh`：训练命令出错） |
| 2 | 用法错（参数不对、找不到项目；队列命令在不是 hub、本机又没有队列的机器上；`spec`、`run_task.sh`：任务说明或环境有问题） |
| 3 | `rm`：不删（有人领着、分出去了、别的任务等它）；`run_task.sh`：命令正常结束但没训完 |
| 4 | 找不到：执行机、机器、环境、任务 key、实验、模型文件夹不在表里或队列里（`rm`、`show`、`set`、`add --exp`、`exp …`、`machines …`、`site …`、`tunnel …`、`internal fetch`、`pueue-status`、`site setup`；`stop`、`nice` 冻结，照旧 1）；`run_task.sh`：缺输入 |
| 5 | 读不了数据目录（`spec done`、`run_task.sh`） |
| 10 / 11 / 12 / 13 | `worker next`：没有能领的 / 等排在前面刚停下的 / 队列暂停或读不了数据目录；`worker beat --strict`：领取已经不是自己的 |
| 75 | `internal stage`：这次排不上，下次再传 |
| 124 | 超时、起不来 |
| 128 以上 | 被信号停掉（`run_task.sh` 收到 SIGTERM 是 143） |

领域码的定义在 `queue/proto.py` 的 `EXIT`。
