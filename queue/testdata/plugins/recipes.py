"""测试用的做法 toy（2026-11-02）：形状同真的领域插件里的训练做法（参数、结果文件夹、训完的标志、存档、进度、拉回、归档、中继），
命令是 testdata/bin/toy_train.sh。字段说明见 scripts/queue/recipes.py 开头。"""
from recipes import REQUIRED

INPUT_ROOTS = ["prepared", "raw"]

RECIPES = {
    "toy": dict(
        params=dict(ds=REQUIRED, frag=REQUIRED, variant="V_save", plan="PlanM", cfg="full", env="base",
                    code="scripts/queue/testdata/code", model="{ds}/{variant}__{plan}__{cfg}"),
        key="{ds}/{variant}/{frag}",
        env="{env}",
        cmd="bash scripts/queue/testdata/bin/toy_train.sh {ds} {cfg} {frag} {plan} {variant}",
        vars=dict(DEMO_RAW="{data}/raw", DEMO_PREPARED="{data}/prepared", DEMO_RESULTS="{data}/results", DEMO_CODE="{repo}/{code}"),
        out="results/{model}/frag_{frag}",
        done=["check/summary.json", "final.ckpt|ARCHIVED"],
        log="train_log_*.txt",
        progress="toy_log",
        resume=["latest.ckpt", "final.ckpt"],
        res=dict(gpus=1, local_inputs=1),
        owner=None,
        info="toy_debug",
        oldlogs=dict(dir="results/logs", prefix="{ds}_{variant}_frag{frag}_"),
        metrics={},
        checkpoint="final.ckpt",
        data=["prepared/{ds}"],
        fetch=dict(exclude=["latest.ckpt"], extra=["*.json"]),
        archive=dict(drop=["*.ckpt", "check/**"], keep=["check/summary.json"]),
        machines=None,
        device="gpu",
        after=[],
        relay="toy",
    ),
}
