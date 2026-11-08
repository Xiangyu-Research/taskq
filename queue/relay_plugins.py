"""Plugins for tasks sent to executors, chosen by recipe (the relay field in recipes.py): a plugin lists the files a task needs
from the store and verifies fetched results. Paths are the same on both sides (relative to the data dir) and contents never
change; the generic path plugin and shared entry point are in sites/names.py.
Plugins load from relay_plugins.py in each TASKQ_PLUGINS dir (exports at the top of cfg.py) and merge into PLUGINS, ACCEPTS and
VIEW_ROOTS; RelayError, expand and sha256 come from taskq_api.py."""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduler code
sys.path.insert(0, HERE)
from taskq_api import RelayError, expand, sha256, walk  # noqa: E402,F401  (used through here by names.py and others)

PLUGINS = {}
ACCEPTS = {}  # whether a plugin can send this task (by params; plugins not listed can send anything)
VIEW_ROOTS = []  # dirs where training plugins build views: relay_store.gc also removes unused ones under here, besides registered views

import cfg  # noqa: E402

cfg.extend(globals(), "relay_plugins")
