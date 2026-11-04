#!/usr/bin/env python3
"""测试用的"怎么跑"（2026-11-03）：形状同真的 policy.py（字段说明见 scripts/taskq/queue/README.md 的"开关和配置"），值取自
11/3 的真表，去掉了和日期有关的部分（OFFLINE_UNTIL 为空，SHIFT 不限日期）。
"""
import os

import cfg  # 读配置的是核心的模块，核心的 queue/ 已经在 sys.path 上

QR = cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）

AUTO = dict(advance=True, lab1=True, dicc2=True, lab0=True, dicc1=True)

OFFLINE_UNTIL = {
}


def _offline():
    import datetime as dt
    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
    return {n: why for n, (until, why) in OFFLINE_UNTIL.items() if until is None or now < until}


OFFLINE = _offline()

HOLD = [
]

PLACE = {
    "infer": ["trainer", "queue:dicc1", "queue:lab1"],
    "eval": ["box:cpu"],
    "ens": ["box:cpu"],
    "lprob": ["box:cpu"],
    "p1": ["queue:dicc1"],
    "p2": ["queue:dicc1"],
}

STAGE_TMP = {}

# 全天（每个时段 24 小时、首尾相接），不限日期：测试结果和现在几点无关
SHIFT = {
    "lab0": ("09:00", "09:00", None, None),
}

SLURM = {
    "dicc2": dict(gpu={"long": ("3-00:00:00", 1)}),
    "dicc1": dict(gpu={"long": ("3-00:00:00", 4)}, cpu=("normal", "08:00:00", 2)),
}

READY = {"dicc2": 2, "lab1": 2, "lab0": 1, "dicc1": 2}

OUTPUTS = dict(
    state="~/.cache/phd/",
    entry="bin/loop.sh",
    cron=(("5,35 * * * *", "health"), ("30 8-20/2 * * *", "registry")),
    origin="git@example.org:demo/demo.git",
    bundle=(os.path.dirname(cfg.CORE_REL) or ".",),  # 调度代码所在的文件夹（单独的检出时是整个仓库）
    experiments=QR + "/testdata/exps/*/experiments.py",
    plans=QR + "/testdata/exps/*/plans.md",
    code_dirs=(QR, QR + "/testdata/code"),
    bundle_check="base",
    env_script=QR + "/testdata/env.sh",
    health="~/.cache/phd-health",
    topic="~/.config/phd-health/ntfy_topic",
    name="Demo",
    heads="~/.cache/phd/code/heads.tsv",
    registry=dict(cmd=QR + "/testdata/bin/registry.py", dir=QR + "/testdata/exps/demo",
                  files=("registry.csv", "registry.md")),
)
