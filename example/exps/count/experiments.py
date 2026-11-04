"""示例的实验表：做法 count（plugins/recipes.py），每个实验按 seed 展开成几个任务，每个任务数完后写摘要（summary 步骤）。
字段说明见核心的 queue/experiments.py 开头。"""
from taskq_api import exists

SUMMARY = dict(name="summary", group="cpu", cmd="bash bin/summary.sh {data}/results/{name}/seed_{seed}",
               done=exists("results/{name}/seed_{seed}/summary.txt"))


def _x(id, label, n, seeds, nice):
    return dict(id=id, exp=id.upper(), label=label, model="count", hours=0.1, recipe="count",
                params=dict(name=id, n=str(n)), each=dict(seed=[str(s) for s in seeds]), after=[SUMMARY], nice=nice)


EXPERIMENTS = [
    _x("small", "数到 100", 100, [0, 1], nice=0),
    _x("large", "数到 10000", 10000, [0, 1, 2], nice=10),
]
