"""Training recipes: how one training run (or another task) runs, which env and data it needs, where results go, what counts as
done, and how to fetch and archive them.
Scheduling follows only recipe fields and knows no specific model. Fields, defaults and descriptions are in queue/schema.py's
RECIPE_FIELDS (a recipe that omits a field takes the default); resolving is in queue/spec.py's resolve. Each recipe also has:
  params     params and defaults (REQUIRED = must be given). A default may use {another param} (e.g. model). A field puts a
             value in as is with {param}, shell-quoted (one word) with {param:q}; on a closed recipe a value a task gives that cmd
             or inputs_cmd takes as is may not hold shell syntax.
  open       True = a task may carry params not listed here and set any field (the command recipe); otherwise a wrong param name
             is an error.
  override   fields (beyond schema.TASK_SETS: key, machines, res, device, out_gb, owner) a task's params may set on this recipe,
             e.g. ["vars", "inputs"]; an experiment table's fields and a smoke definition's vars count as params too. cmd and
             inputs_cmd (schema.OPEN_ONLY) cannot be listed: a recipe whose tasks bring their own command is open.

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
