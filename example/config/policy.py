"""Example "how to use it" (fields in queue/schema.py, taskq machines fields): all automatic parts off, plan only prints what it
would do. The full form is in queue/testdata/config/policy.py.
"""
AUTO = dict(advance=False, envs=False, revive=False, smoke=False)

PAUSE = {}

HOLD = []

PLACE = {  # steps and recipes not listed = "any" (any executor with that worker kind)
    "summary": ["gpubox"],
}

SLURM = {
    "cluster": dict(gpu={"normal": ("1-00:00:00", 2)}),
}

READY = {"gpubox": 1, "cluster": 2}

OUTPUTS = dict(
    state="~/.local/state/taskq-example/",  # hub queue and records live here, other projects untouched
    experiments="exps/*/experiments.py",
    bundle=("bin", "exps", "plugins"),     # project dirs carried in executor bundles
    name="Example",
)
