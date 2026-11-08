"""Test recipe toy: same shape as a training recipe in a real domain plugin (params, result dir, done marker, checkpoint,
progress, fetch, archive, relay); the command is testdata/bin/toy_train.sh. Fields in queue/schema.py's RECIPE_FIELDS."""
import os

from taskq_api import REQUIRED, rel
QR = rel("queue")  # scheduler code's queue/ under the repo root (scripts/taskq/queue when embedded, queue in a standalone checkout)

RECIPES = {
    "toy": dict(
        params=dict(ds=REQUIRED, frag=REQUIRED, variant="V_save", plan="PlanM", cfg="full", env="base",
                    code=QR + "/testdata/code", model="{ds}/{variant}__{plan}__{cfg}"),
        key="{ds}/{variant}/{frag}",
        env="{env}",
        cmd="bash " + QR + "/testdata/bin/toy_train.sh {ds} {cfg} {frag} {plan} {variant}",
        vars=dict(DEMO_RAW="{data}/raw", DEMO_PREPARED="{data}/prepared", DEMO_RESULTS="{data}/results", DEMO_CODE="{repo}/{code}"),
        out="results/{model}/frag_{frag}",
        done=["check/summary.json", "final.ckpt"],
        log="train_log_*.txt",
        progress="toy_log",
        resume=["latest.ckpt", "final.ckpt"],
        res=dict(gpus=1, local_inputs=1),
        info="toy_debug",
        metrics={},
        checkpoint="final.ckpt",
        inputs=["prepared/{ds}"],
        fetch=dict(exclude=["latest.ckpt"], extra=["*.json"]),
        archive=True,
        relay="toy",
    ),
}


def model_of(model_dir):
    """Result dir .../<ds>/<variant>__<plan>__<cfg> -> the model param (last two levels)."""
    return "/".join(os.path.normpath(model_dir).split(os.sep)[-2:])


HOOKS = {"model_of": model_of}
