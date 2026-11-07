#!/usr/bin/env python3
"""测试用的"怎么用"：形状同真的 policy.py（字段见 queue/schema.py，taskq machines fields）。"""
import os

import cfg  # 读配置的是核心的模块，核心的 queue/ 已经在 sys.path 上

QR = cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）

AUTO = dict(advance=True, envs=True, revive=True, smoke=True)

PAUSE = {
}

HOLD = [
]

PLACE = {  # 没写的步骤 = "any"
    "infer": ["site-slurm", "site-pueue"],
    "p1": ["site-slurm"],
    "p2": ["site-slurm"],
}

SLURM = {
    "site-slurm2": dict(gpu={"long": ("3-00:00:00", 1)}),
    "site-slurm": dict(gpu={"long": ("3-00:00:00", 4)}, cpu=("normal", "08:00:00", 2)),
}

READY = {"site-slurm2": 2, "site-pueue": 2, "site-slurm": 2}

OUTPUTS = dict(
    state="~/.local/state/taskq/",
    entry="bin/loop.sh",
    cron=(("5,35 * * * *", "health"), ("30 8-20/2 * * *", "registry")),
    origin="git@example.org:demo/demo.git",
    bundle=(os.path.dirname(cfg.CORE_REL) or ".",),  # 调度代码所在的文件夹（单独的检出时是整个仓库）
    experiments=QR + "/testdata/exps/*/experiments.py",
    code_dirs=(QR, QR + "/testdata/code"),
    bundle_check="base",
    env_script=QR + "/testdata/env.sh",
    name="Demo",
)
