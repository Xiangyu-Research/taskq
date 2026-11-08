"""Example recipe count: count to n, checkpoint and write progress every 10 steps, write DONE when finished (bin/count.sh). Fields
in queue/schema.py's RECIPE_FIELDS (unset ones take defaults)."""
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
