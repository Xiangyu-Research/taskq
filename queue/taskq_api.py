"""插件用的核心接口（2026-11-04）：做法、中继插件只 import 这个模块，不 import 核心的其他模块（recipes、relay_plugins、spec…）。
插件文件要导出哪些名字见 cfg.py 开头。

  REQUIRED              做法的参数默认值写它 = 任务必须给
  RelayError            中继插件准备输入、核对结果出错时报它
  walk(p)               文件夹 p 下的文件（相对 p，跟着软链接走），排好序
  expand(root, rel)     数据目录下的一个输入 → (files, 视图文件夹或 None)
  sha256(p)             文件的 sha256
  REPO                  使用者的仓库根（任务命令里的相对路径相对它）
  rel(*parts)           调度代码里的文件相对 REPO 的路径
  check、exists         实验表步骤的 done（见 steps.py）；fill_path、file_mtime 给插件写别的 done 用
只用标准库和 cfg.py、steps.py。"""
import hashlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cfg as _cfg  # noqa: E402
from steps import check, exists, fill_path, file_mtime  # noqa: E402,F401

REPO = _cfg.REPO
rel = _cfg.rel


REQUIRED = object()


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
