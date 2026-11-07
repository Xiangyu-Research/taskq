"""示例的做法 count：数到 n，每 10 步存一次档、写进度，数完写 DONE（bin/count.sh）。字段见 queue/schema.py 的 RECIPE_FIELDS
（没写的取默认值）。"""
from taskq_api import REQUIRED

RECIPES = {
    "count": dict(
        params=dict(name=REQUIRED, seed=REQUIRED, n="100", env="base"),
        key="count/{name}/{seed}",
        env="{env}",
        cmd="bash bin/count.sh {n} {data}/results/{name}/seed_{seed}",
        out="results/{name}/seed_{seed}",
        done=["DONE"],
        progress="progress_json",
        resume=["ckpt.json"],
        res=dict(gpus=1),
    ),
}
