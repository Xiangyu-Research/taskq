# 示例项目

一个最小的使用者项目：怎么写 `.taskq.toml`、机器表、`policy.py`、插件和实验表。

| 文件 | 内容 |
|---|---|
| `.taskq.toml` | 配置和插件的位置 |
| `config/machines.py` | 一台 hub（本机）、两台执行机：pueue 的 `gpubox`、Slurm 的 `cluster`（主机名是假的，连不上） |
| `config/policy.py` | 自动的部分都关着；状态放在 `~/.cache/taskq-example/`，不碰别的项目 |
| `plugins/recipes.py` | 做法 `count`：数到 `n`，存档、进度、训完的标志（`bin/count.sh`） |
| `exps/count/experiments.py` | 两个实验，按 `seed` 展开成 5 个任务，每个任务做完后跑 `summary`（`bin/summary.sh`） |

完整的写法（每个字段都用到）见测试用的固定配置 [`../queue/testdata/`](../queue/testdata/)，字段说明见 [`../queue/README.md`](../queue/README.md) 的"开关和配置"。

## 试一试

下面的命令都不用连别的机器（在仓库根目录跑；在 `example/` 里跑时可以不写 `--project example`）：

```bash
mkdir -p ~/taskq-example/data                     # hub 的数据目录（config/machines.py 的 data）
taskq --project example check                     # 核对配置、插件、实验表、每个任务和步骤、执行机的代码包
taskq --project example exp list                  # 实验
taskq --project example exp tasks                 # 展开后的任务
taskq --project example add --exp small           # 加进总队列（~/.cache/taskq-example/taskq）
taskq --project example status                    # 总队列
taskq --project example show count/small/0        # 一个任务会怎么跑
taskq --project example plan                      # 这一轮会做什么：两台执行机连不上，什么都不动
taskq --project example rm count/small/0 count/small/1
```

hub 就是本机，`plan`、`status --all` 会读本机的 pueue（只读）。
