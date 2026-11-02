"""送到执行机的任务按做法（recipes.py 的 relay 字段）挑插件（2026-11-01 起都用真名）：插件列出任务要用的 box 上的文件，
拉回后核对结果。路径在两边一样（相对数据目录），文件内容不改。通用的 path 插件和统一的入口见 scripts/sites/names.py。

插件在 TASKQ_PLUGINS 各文件夹的 relay_plugins.py 里（scripts/queue/cfg.py），加载时合进 PLUGINS、ACCEPTS、VIEW_ROOTS。
每个插件一对函数：
  export(spec, root) → dict(files=[(相对路径, box 上的真实文件)], views=[视图文件夹], cleanup=[…]（可省，默认 = 结果文件夹）)
  check(spec, src, root)：src 下是拉回的文件（真的相对路径），对不上时报 RelayError"""
import hashlib
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))

RELAY_DIR = os.path.expanduser(os.environ.get("RELAY_DIR", "~/.cache/phd/relay"))


class RelayError(Exception):
    pass


def walk(p):
    """文件夹 p 下的文件（相对 p，跟着软链接走），排好序。"""
    out = []
    for dp, dns, fns in os.walk(p, followlinks=True):
        dns.sort()
        out += [os.path.relpath(os.path.join(dp, f), p) for f in sorted(fns)]
    return out


def expand(root, rel):
    """数据目录下的一个输入 → (files, 视图文件夹或 None)：文件夹里的文件都放进去、整个文件夹当视图；单个文件不当视图。"""
    p = os.path.join(root, rel)
    if os.path.isdir(p):
        fs = walk(p)
        if not fs:
            raise RelayError(f"输入 {rel} 是空的")
        return [(f"{rel}/{f}", os.path.realpath(os.path.join(p, f))) for f in fs], rel
    if os.path.isfile(p):
        return [(rel, os.path.realpath(p))], None
    raise RelayError(f"box 上没有输入 {rel}")


def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for blk in iter(lambda: f.read(1 << 22), b""):
            h.update(blk)
    return h.hexdigest()


PLUGINS = {}
ACCEPTS = {}  # 插件能不能送这个任务（按参数；不在这里的都能送）
VIEW_ROOTS = []  # 训练插件建视图的文件夹：relay_gc.py 除了登记过的视图，还清理这下面没人用的

import cfg  # noqa: E402

cfg.extend(globals(), "relay_plugins", ("PLUGINS", "ACCEPTS", "VIEW_ROOTS"))
