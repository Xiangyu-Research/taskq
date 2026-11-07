"""示例的"怎么用"（字段见 queue/schema.py，taskq machines fields）：自动的部分都关着，plan 只打印会做什么。
完整的写法见 queue/testdata/config/policy.py。
"""
AUTO = dict(advance=False, envs=False, revive=False, smoke=False)

PAUSE = {}

HOLD = []

PLACE = {  # 没写的步骤和做法 = "any"（有这种 worker 的执行机都行）
    "summary": ["gpubox"],
}

SLURM = {
    "cluster": dict(gpu={"normal": ("1-00:00:00", 2)}),
}

READY = {"gpubox": 1, "cluster": 2}

OUTPUTS = dict(
    state="~/.local/state/taskq-example/",  # 总队列、记录都在这下面，不碰别的项目
    experiments="exps/*/experiments.py",
    bundle=("bin", "exps", "plugins"),     # 执行机的代码包里带的项目文件夹
    name="Example",
)
