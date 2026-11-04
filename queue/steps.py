"""训练以后的步骤怎么判断做完了（2026-10-18）：实验表（research/*/experiments.py）的 after、group_after、FINAL 里的 done 用这些写，
scripts/taskq/queue/advance.py 调用。路径都相对数据目录，可以用 {参数}（任务说明的参数，如 {model}、{fold}）。"""
import json
import os
from datetime import datetime


def _fill(path, names):
    import spec
    return spec.fill(path, names, keep=())


class runlog:
    """research/common/runlog.py 的记录（<脚本>.run.json）是 ok；10/16 用 runlog 之前的结果没有记录，有 legacy 这个文件就算。"""

    def __init__(self, path, legacy=None):
        self.path, self.legacy = path, legacy

    def where(self, root, names):
        return os.path.join(root, _fill(self.path, names))

    def ok(self, root, names):
        p = self.where(root, names)
        if os.path.exists(p):
            try:
                return json.load(open(p)).get("status") == "ok"
            except (OSError, ValueError):
                return False
        return bool(self.legacy) and os.path.exists(os.path.join(os.path.dirname(p), self.legacy))

    def mtime(self, root, names):
        """结果的修改时间（没有记录时用 legacy 文件的）。"""
        p = self.where(root, names)
        return _mtime(p) or (_mtime(os.path.join(os.path.dirname(p), self.legacy)) if self.legacy else 0)

    def started(self, root, names):
        """这次运行开始的时刻（读的是开始时的输入；跑的过程中新出的结果不算它用过）；没有就用修改时间。"""
        p = self.where(root, names)
        try:
            return datetime.fromisoformat(json.load(open(p))["start"]).timestamp()
        except (OSError, ValueError, KeyError, TypeError):
            return _mtime(p)


class check:
    """fn(路径) 为真算做完（如推理的 MANIFEST 核对）。"""

    def __init__(self, path, fn):
        self.path, self.fn = path, fn

    def where(self, root, names):
        return os.path.join(root, _fill(self.path, names))

    def ok(self, root, names):
        return bool(self.fn(self.where(root, names)))

    def mtime(self, root, names):
        return _mtime(self.where(root, names))

    started = mtime


class exists(check):
    """这个文件在就算做完。"""

    def __init__(self, path):
        super().__init__(path, os.path.exists)


def _mtime(p):
    try:
        return os.path.getmtime(p)
    except OSError:
        return 0
