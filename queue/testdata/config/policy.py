#!/usr/bin/env python3
"""Test "how to use it": same shape as a real policy.py (fields in queue/schema.py, taskq machines fields)."""
import os

import cfg  # the core module reads config; the core's queue/ is already on sys.path

QR = cfg.rel("queue")  # scheduler code's queue/ under the repo root (scripts/taskq/queue when embedded, queue in a standalone checkout)

AUTO = dict(advance=True, envs=True, revive=True, smoke=True)

PAUSE = {
}

HOLD = [
]

PLACE = {  # steps not listed = "any"
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
    bundle=(os.path.dirname(cfg.CORE_REL) or ".",),  # the dir holding the scheduler code (the whole repo in a standalone checkout)
    experiments=QR + "/testdata/exps/*/experiments.py",
    code_dirs=(QR, QR + "/testdata/code"),
    bundle_check="base",
    env_script=QR + "/testdata/env.sh",
    name="Demo",
)
