"""训练以后的步骤怎么判断做完了（2026-10-18）：实验表（OUTPUTS["experiments"]）的 after、group_after、FINAL 里的 done 用这些写，
scripts/taskq/queue/advance.py 调用。路径都相对数据目录，可以用 {参数}（任务说明的参数，如 {model}）。
done 是有 where(root, names)、ok(root, names)、mtime(root, names)、started(root, names) 的对象；这里的 check、exists 通用，
读某种运行记录的放在使用者那边（11/4），照着 check 写。"""
import os


def fill_path(path, names):
    import spec
    return spec.fill(path, names, keep=())


class check:
    """fn(路径) 为真算做完（如推理的 MANIFEST 核对）。"""

    def __init__(self, path, fn):
        self.path, self.fn = path, fn

    def where(self, root, names):
        return os.path.join(root, fill_path(self.path, names))

    def ok(self, root, names):
        return bool(self.fn(self.where(root, names)))

    def mtime(self, root, names):
        return file_mtime(self.where(root, names))

    started = mtime


class exists(check):
    """这个文件在就算做完。"""

    def __init__(self, path):
        super().__init__(path, os.path.exists)


def file_mtime(p):
    try:
        return os.path.getmtime(p)
    except OSError:
        return 0
