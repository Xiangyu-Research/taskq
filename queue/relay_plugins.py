"""送到执行机的任务按做法（recipes.py 的 relay 字段）挑插件（2026-11-01 起都用真名）：插件列出任务要用的 hub 上的文件，
拉回后核对结果。路径在两边一样（相对数据目录），文件内容不改。通用的 path 插件和统一的入口见 sites/names.py。

插件在 TASKQ_PLUGINS 各文件夹的 relay_plugins.py 里（导出什么见 cfg.py 开头），加载时合进 PLUGINS、ACCEPTS、VIEW_ROOTS；
插件要用的 RelayError、expand、sha256 从 taskq_api.py 取。
每个插件一对函数：
  export(spec, root) → dict(files=[(相对路径, hub 上的真实文件)], views=[视图文件夹], cleanup=[…]（可省，默认 = 结果文件夹）)
  check(spec, src, root)：src 下是拉回的文件（真的相对路径），对不上时报 RelayError"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
from taskq_api import RelayError, expand, sha256, walk  # noqa: E402,F401  （names.py 等经这里用）

PLUGINS = {}
ACCEPTS = {}  # 插件能不能送这个任务（按参数；不在这里的都能送）
VIEW_ROOTS = []  # 训练插件建视图的文件夹：relay_store.gc 除了登记过的视图，还清理这下面没人用的

import cfg  # noqa: E402

RELAY_DIR = os.path.expanduser(os.environ.get("RELAY_DIR", cfg.state("relay", expand=False)))

cfg.extend(globals(), "relay_plugins")
