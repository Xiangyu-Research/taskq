"""示例的"怎么跑"：字段说明见核心的 queue/README.md 的"开关和配置"，完整的写法见 queue/testdata/config/policy.py。
自动的部分都关着（AUTO），plan 只打印会做什么。
"""
AUTO = dict(advance=False, gpubox=False, cluster=False)

OFFLINE = {}

HOLD = []

# 步骤在哪里做：summary 在 hub 的 cpu 组（没写的步骤都在 hub 上，用实验表里写的组）
PLACE = {
    "summary": ["hub:cpu"],
}

SHIFT = {}

SLURM = {
    "cluster": dict(gpu={"normal": ("1-00:00:00", 2)}),
}

READY = {"gpubox": 1, "cluster": 2}

OUTPUTS = dict(
    state="~/.cache/taskq-example/",    # 总队列、记录、日志都在这下面，不碰别的项目
    logs="~/.cache/taskq-example/logs",
    experiments="exps/*/experiments.py",
    bundle=("bin", "exps", "plugins"),   # 执行机的代码包里带的项目文件夹
    name="Example",
)
