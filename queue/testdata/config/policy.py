#!/usr/bin/env python3
"""测试用的"怎么跑"（2026-11-03）：形状同真的 policy.py（字段说明见 scripts/queue/README.md 的"开关和配置"），值取自
11/3 的真表，去掉了和日期有关的部分（OFFLINE_UNTIL 为空，SHIFT 不限日期）；dicc 是有缓冲的假执行机，真表里 11/3 起没有这类。
"""

AUTO = dict(advance=True, lab=False, dicc=True, lab1=True, dicc2=True, lab0=True, dicc1=True)

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

STAGE_TMP = {"dicc": True}

# 全天（每个时段 24 小时、首尾相接），不限日期：测试结果和现在几点无关
SHIFT = {
    "lab0": ("09:00", "09:00", None, None),
}

SLURM = {
    "dicc": dict(gpu={"long": ("3-00:00:00", 4)}, cpu=("normal", "08:00:00", 2)),
    "dicc2": dict(gpu={"long": ("3-00:00:00", 1)}),
    "dicc1": dict(gpu={"long": ("3-00:00:00", 4)}, cpu=("normal", "08:00:00", 2)),
}

READY = {"dicc": 10, "dicc2": 2, "lab1": 2, "lab0": 1, "dicc1": 2}

MAX_NICE = {}

OUTPUTS = dict(
    bundle=("scripts",),
    experiments="scripts/queue/testdata/exps/*/experiments.py",
    plans="scripts/queue/testdata/exps/*/plans.md",
    code_dirs=("scripts/queue", "scripts/queue/testdata/code"),
    bundle_check="base",
    env_script="scripts/queue/testdata/env.sh",
    health="~/.cache/phd-health",
    topic="~/.config/phd-health/ntfy_topic",
    name="Demo",
    heads="~/.cache/phd/code/heads.tsv",
    registry=dict(cmd="scripts/queue/testdata/bin/registry.py", dir="scripts/queue/testdata/exps/demo",
                  files=("registry.csv", "registry.md")),
)
