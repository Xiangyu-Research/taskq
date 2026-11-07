# 示例项目

一个最小的使用者项目：怎么写 `.taskq.toml`、机器表、`policy.py`、插件和实验表。

| 文件 | 内容 |
|---|---|
| `.taskq.toml` | 配置和插件的位置 |
| `config/machines.py` | 三台机器：`hub`（本机，兼 store）、pueue 执行机 `gpubox`（一个 GPU worker、一个 CPU worker）、Slurm 执行机 `cluster`（两台执行机的主机名是假的，连不上） |
| `config/policy.py` | 自动的部分都关着；`summary` 步骤只放 `gpubox`；状态放在 `~/.local/state/taskq-example/`，不碰别的项目 |
| `plugins/recipes.py` | 做法 `count`：数到 `n`，存档、进度、训完的标志（`bin/count.sh`） |
| `exps/count/experiments.py` | 两个实验，按 `seed` 展开成 5 个任务，每个任务做完后跑 `summary` 步骤（`bin/summary.sh`，纯 CPU） |

每个字段都用到的写法见测试用的固定配置 [`../queue/testdata/`](../queue/testdata/)，字段表见 [`../queue/README.md`](../queue/README.md) 的"开关和配置"。

## 试一试

在仓库根目录跑（在 `example/` 里跑时可以不写 `--project example`），都不用连别的机器：

```bash
mkdir -p ~/taskq-example/data                     # store 的数据目录（config/machines.py 里 hub 的 data）
taskq --project example check                     # 核对配置、插件、实验表、每个任务和步骤、执行机的代码包
taskq --project example exp list                  # 实验
taskq --project example exp tasks                 # 展开后的任务
taskq --project example add --exp small           # 加进总队列（~/.local/state/taskq-example/taskq）
taskq --project example status                    # 总队列
taskq --project example show count/small/0        # 一个任务会怎么跑
taskq --project example site bundle hub           # 给 hub 的根目录（~/taskq-example/root）推代码包：store 的操作从它跑
taskq --project example plan                      # 这一轮会做什么：两台执行机连不上，什么都不送
taskq --project example rm count/small/0 count/small/1
```
