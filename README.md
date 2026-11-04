# taskq

任务队列和调度：总队列在 hub（box）上，执行机（Slurm 作业、pueue worker）领任务、训练，训完以后拉回、按实验表排下一步。核心不认识具体模型，项目通过配置（机器表、`policy.py`、实验表）和插件接入。结构和各模块见 [`queue/README.md`](queue/README.md)。

## 安装

```bash
bash install.sh          # 克隆到 ~/.local/share/taskq（TASKQ_HOME），~/.local/bin/taskq 链到 bin/taskq；重复运行 = 更新
taskq self update        # 拉 origin/main，失败重试 5 次，拉不到就留在原来的版本
```

只维护一个版本（GitHub 上的 main）。执行机上的作业用各自的代码包，不受更新影响。

## 项目

`taskq` 先找项目：`--project 目录` > `TASKQ_PROJECT` > 从当前目录往上找 `.taskq.toml`：

```toml
config = "envs"                              # machines.py、policy.py 所在的文件夹
plugins = ["research/common/taskq_plugins"]  # 插件文件夹
```

找到后设 `TASKQ_REPO`、`TASKQ_CONFIG`、`TASKQ_PLUGINS`，再调用核心（bash 脚本也认这几个变量）。

## 命令

| 命令 | 做什么 | 原来的入口 |
|---|---|---|
| **队列** | | |
| `taskq add --exp <id> [--nice N] [--only k=v,…]`、`add --recipe <做法> k=v…`、`add --file <任务单>` | 加任务 | `taskq.py add` |
| `taskq set <key> k=v…` | 改一个任务的参数 | `taskq.py set` |
| `taskq nice --nice N <key>…` | 改档位 | `taskq.py set-nice` |
| `taskq show [--json] <key>` | 一个任务的说明和状态 | `taskq.py show` |
| `taskq status [--json]` | 本机的队列 | `taskq.py status` |
| `taskq status --all [--json]` | 所有任务的状态、在哪台、进度、预计完成 | — |
| `taskq stop [--why 原因] <key>…`、`taskq resume <key>…` | 停止、撤销停止 | `taskq.py stop`、`resume` |
| `taskq rm <key>…` | 删掉没人领着的任务 | `taskq.py remove` |
| **调度** | | |
| `taskq tick [-n] [--test]` | 健康检查一轮（真做）；`-n` 只打印问题 | `monitor/health.py` |
| `taskq plan` | 只看：按时段开关、送任务、训完以后这一轮会做什么 | `shift.py --dry-run`、`feed.py`、`relay.py`、`advance.py` |
| `taskq boot [--gpu-log]` | 开机：启动 pueued、放回被打断的任务 | `queue/on_boot.sh` |
| **执行机** | | |
| `taskq site status\|bundle\|resend <名字>` | 那边的队列；打包推过去；重新解析传过去的任务 | `relay.py status\|bundle\|resend` |
| `taskq site env <机器> <环境> [-n]`、`taskq site envs <机器>` | 建环境；看环境的状态 | `sites/site.py` |
| `taskq site setup [名字]` | pueue 常驻进程、组、crontab | `queue/setup.sh` |
| **实验** | | |
| `taskq exp list`、`taskq exp tasks [--json]` | 实验、展开后的任务 | — |
| `taskq exp env-of <模型文件夹>`、`taskq exp name-of <模型文件夹> <名字>…` | 训它用的环境；实验的 `names` | `experiments.py env-of\|name-of` |
| **其他** | | |
| `taskq tunnel rsync\|push\|pull …` | 经隧道传大批文件 | `sync/tunnel.py` |
| `taskq pueue-add <组> [选项] -- '<命令>'` | 往本机 pueue 加一次性任务 | `queue/q` |
| `taskq snapshot <克隆>` | 按提交号的代码副本 | `sync/snapshot.sh` |
| `taskq check` | 核对整个项目 | — |
| `taskq path lib\|api\|core` | 核心的 `queue/`（插件 `import taskq_api` 用）、核心的根 | — |
| `taskq machines …` | 机器表 | `envs/machines.py` 的命令行 |
| `taskq self update\|version`、`taskq --version` | 更新、版本（`VERSION` 文件，没有时 `git describe`） | — |
| **底层**（不在 `taskq --help` 的首页，参数和退出码冻结） | | |
| `taskq worker next\|beat\|release\|missing …` | worker 领任务、心跳、交还、缺输入 | `taskq.py next\|beat\|release\|missing` |
| `taskq spec shell\|done\|resolve\|lookup\|outputs\|collect …` | 任务说明 | `spec.py` |
| `taskq internal stage\|fetch <名字> <key>` | 调度自己排的传数据、拉回 | `relay.py stage\|fetch` |

`taskq help <命令>` 同 `taskq <命令> --help`。原来的脚本路径和参数照旧能用（在跑的作业、旧代码包还在调用）。

## 约定

见 [`queue/README.md`](queue/README.md) 的"命令行"。退出码：

| 退出码 | 意思 |
|---|---|
| 0 | 成功 |
| 1 | 出错（`spec done`：没训完；`run_task.sh`：训练命令出错） |
| 2 | 用法错（参数不对、找不到项目；队列命令在不是 hub、本机又没有队列的机器上；`spec`、`run_task.sh`：任务说明或环境有问题） |
| 3 | `rm`：不删（有人领着、分出去了、别的任务等它）；`run_task.sh`：命令正常结束但没训完 |
| 4 | `rm`：队列里没有；`run_task.sh`：缺输入 |
| 5 | 读不了数据目录（`spec done`、`run_task.sh`） |
| 10 / 11 / 12 / 13 | `worker next`：没有能领的 / 等排在前面刚停下的 / 队列暂停或读不了数据目录；`worker beat --strict`：领取已经不是自己的 |
| 75 | `internal stage`：这次排不上，下次再传 |
| 124 | 超时、起不来 |
| 128 以上 | 被信号停掉（`run_task.sh` 收到 SIGTERM 是 143） |

领域码的定义在 `queue/proto.py` 的 `EXIT`。
