"""训练的做法：一种训练（或别的任务）怎么跑、要什么环境和数据、结果在哪、怎么算做完、怎么拉回和归档。
调度只按做法的字段办事，不认识具体模型。字段、默认值和说明见 queue/schema.py 的 RECIPE_FIELDS（做法没写的取默认值）；
解析见 queue/spec.py 的 resolve。每种做法另有：
  params     参数和默认值（REQUIRED = 必须给）。默认值里可以用 {别的参数}（如 model）。
  open       True = 任务可以带这里没列的参数（command 做法）；否则写错参数名会报错。

领域专用的做法在 TASKQ_PLUGINS 各文件夹的 recipes.py 里（导出 RECIPES、HOOKS，见 cfg.py 开头），加载时合进来。

钩子（HOOKS，插件给）：
  model_of   结果文件夹（放在哪里都行）→ 训它的任务的 model 参数（experiments.py 的 env-of、name-of 按它查实验表）
"""

from taskq_api import REQUIRED  # noqa: E402

RECIPES = {
    # 什么都由任务自己给（可以直接用，或者照着写一种新的做法）
    "command": dict(
        params=dict(key=REQUIRED, cmd=REQUIRED, env=REQUIRED, out=REQUIRED, done=REQUIRED),
        open=True,
        key="{key}", env="{env}", cmd="{cmd}", out="{out}", done="{done}",
    ),
}

import cfg  # noqa: E402

HOOKS = {}

cfg.extend(globals(), "recipes")
