"""Training recipes: how one training run (or another task) runs, which env and data it needs, where results go, what counts as
done, and how to fetch and archive them.
Scheduling follows only recipe fields and knows no specific model. Fields, defaults and descriptions are in queue/schema.py's
RECIPE_FIELDS (a recipe that omits a field takes the default); resolving is in queue/spec.py's resolve. Each recipe also has:
  params     params and defaults (REQUIRED = must be given). A default may use {another param} (e.g. model).
  open       True = a task may carry params not listed here (the command recipe); otherwise a wrong param name is an error.

Domain-specific recipes live in each TASKQ_PLUGINS dir's recipes.py (exporting RECIPES and HOOKS; see the top of cfg.py) and are
merged on load.

Hooks (HOOKS, supplied by a plugin):
  model_of   result dir (wherever it sits) -> the model param of the task that trained it (experiments.py env-of and name-of look
             up the experiment table by it)
"""

from taskq_api import REQUIRED  # noqa: E402

RECIPES = {
    # everything comes from the task itself (use it directly, or copy it to write a new recipe)
    "command": dict(
        params=dict(key=REQUIRED, cmd=REQUIRED, env=REQUIRED, out=REQUIRED, done=REQUIRED),
        open=True,
        key="{key}", env="{env}", cmd="{cmd}", out="{out}", done="{done}",
    ),
}

import cfg  # noqa: E402

HOOKS = {}

cfg.extend(globals(), "recipes")
