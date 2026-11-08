"""Example experiment table: recipe count (plugins/recipes.py); each experiment expands by seed into several tasks, and each task
writes a summary after counting (the summary step). Fields in queue/schema.py's EXP_FIELDS, STEP_FIELDS."""

SUMMARY = dict(name="summary", env="base", device="cpu",
               cmd="bash bin/summary.sh {data}/results/{name}/seed_{seed} {data}/summaries/{name}/seed_{seed}",
               inputs=["results/{name}/seed_{seed}"], out="summaries/{name}/seed_{seed}", done="summary.txt")


def _x(id, label, n, seeds, nice):
    return dict(id=id, exp=id.upper(), label=label, model="count", hours=0.1, recipe="count",
                params=dict(name=id, n=str(n)), each=dict(seed=[str(s) for s in seeds]), after=[SUMMARY], nice=nice)


EXPERIMENTS = [
    _x("small", "count to 100", 100, [0, 1], nice=0),
    _x("large", "count to 10000", 10000, [0, 1, 2], nice=10),
]
