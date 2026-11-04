"""测试用的步骤判断（2026-11-04）：形状同真的使用者那边的运行记录（<脚本>.rec.json 里 status 是 ok、start 是开始时刻；
有记录以前的结果有 legacy 这个文件就算）。和 steps.py 的 check 一样提供 where、ok、mtime、started。"""
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from steps import fill_path, file_mtime  # noqa: E402


class record:
    def __init__(self, path, legacy=None):
        self.path, self.legacy = path, legacy

    def where(self, root, names):
        return os.path.join(root, fill_path(self.path, names))

    def ok(self, root, names):
        p = self.where(root, names)
        if os.path.exists(p):
            try:
                return json.load(open(p)).get("status") == "ok"
            except (OSError, ValueError):
                return False
        return bool(self.legacy) and os.path.exists(os.path.join(os.path.dirname(p), self.legacy))

    def mtime(self, root, names):
        p = self.where(root, names)
        return file_mtime(p) or (file_mtime(os.path.join(os.path.dirname(p), self.legacy)) if self.legacy else 0)

    def started(self, root, names):
        p = self.where(root, names)
        try:
            return datetime.fromisoformat(json.load(open(p))["start"]).timestamp()
        except (OSError, ValueError, KeyError, TypeError):
            return file_mtime(p)
