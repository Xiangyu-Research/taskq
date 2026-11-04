"""示例的做法 count：数到 n，每 10 步存一次档、写进度，数完写 DONE（bin/count.sh）。字段说明见核心的 queue/recipes.py 开头。"""
from taskq_api import REQUIRED

RECIPES = {
    "count": dict(
        params=dict(name=REQUIRED, seed=REQUIRED, n="100", env="base"),
        key="count/{name}/{seed}",
        env="{env}",
        cmd="bash bin/count.sh {n} {data}/results/{name}/seed_{seed}",
        vars={},
        out="results/{name}/seed_{seed}",
        done=["DONE"],
        log="*.log",
        progress="progress_json",
        resume=["ckpt.json"],
        res=dict(gpus=1),
        owner=None,
        info=None,
        oldlogs=None,
        metrics={},
        checkpoint=None,
        data=[],
        fetch=dict(exclude=[], extra=[]),
        archive=None,
        machines=None,
        device="gpu",
        after=[],
        relay="path",
    ),
}
