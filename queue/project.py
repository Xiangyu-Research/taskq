#!/usr/bin/env python3
"""找使用者的项目（2026-11-04）：项目根目录里有 .taskq.toml，写明配置和插件文件夹（相对项目根目录）：

    config = "envs"
    plugins = ["research/common/taskq_plugins"]

找的顺序：明确给的文件夹（--project）> 环境变量 TASKQ_PROJECT > 从当前目录往上找 .taskq.toml。只用标准库，不 import 核心的模块。

    python3 project.py sh [--project 文件夹]   给 bash 的 export 语句（TASKQ_REPO、TASKQ_CONFIG、TASKQ_PLUGINS，已经设了的不输出）；
                                               找不到项目时什么也不输出、退出码 1
"""
import os
import re
import shlex
import sys

NAME = ".taskq.toml"


def find(explicit=None, start=None):
    """项目根目录（绝对路径）；找不到 → None。"""
    for v in (explicit, os.environ.get("TASKQ_PROJECT")):
        if v:
            return os.path.abspath(os.path.expanduser(v))
    d = os.path.abspath(start or os.getcwd())
    while True:
        if os.path.isfile(os.path.join(d, NAME)):
            return d
        up = os.path.dirname(d)
        if up == d:
            return None
        d = up


_STR = r'"((?:[^"\\]|\\.)*)"'
_LINE = re.compile(r'([A-Za-z0-9_-]+)\s*=\s*(.*?)\s*(?:#[^"]*)?$')


def _unq(s):
    return s.encode("utf-8").decode("unicode_escape") if "\\" in s else s


def parse(text):
    """tomllib 没有时（Python < 3.11）的小解析器：只认 key = "字符串" 和 key = ["a", "b"]（一行写完），注释、空行跳过。"""
    out = {}
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _LINE.fullmatch(line)
        v = m and m.group(2)
        if v and re.fullmatch(_STR, v):
            out[m.group(1)] = _unq(re.fullmatch(_STR, v).group(1))
        elif v and re.fullmatch(r"\[\s*(?:%s\s*(?:,\s*%s\s*)*,?\s*)?\]" % (_STR, _STR), v):
            out[m.group(1)] = [_unq(x) for x in re.findall(_STR, v)]
        else:
            raise ValueError(f"{NAME} 第 {n} 行看不懂：{raw}")
    return out


def load(root):
    """项目根目录的 .taskq.toml → dict；没有这个文件 → {}。"""
    p = os.path.join(root, NAME)
    if not os.path.isfile(p):
        return {}
    with open(p, "rb") as f:
        data = f.read()
    try:
        import tomllib
    except ImportError:
        return parse(data.decode("utf-8"))
    return tomllib.loads(data.decode("utf-8"))


def _plugins(v):
    return [v] if isinstance(v, str) else list(v or [])


def env_of(root):
    """项目对应的环境变量 {名: 值}：TASKQ_REPO 总有；TASKQ_CONFIG、TASKQ_PLUGINS 只在 .taskq.toml 写了时有。"""
    conf = load(root)
    out = {"TASKQ_REPO": root}
    if "config" in conf:
        out["TASKQ_CONFIG"] = os.path.join(root, conf["config"])
    if "plugins" in conf:
        out["TASKQ_PLUGINS"] = os.pathsep.join(os.path.join(root, p) for p in _plugins(conf["plugins"]))
    return out


def export(root):
    """把项目的环境变量设进 os.environ（已经设了的不改）→ 新设的 {名: 值}。"""
    new = {k: v for k, v in env_of(root).items() if k not in os.environ}
    os.environ.update(new)
    return new


def main(argv):
    if not argv or argv[0] != "sh" or len(argv) not in (1, 3) or (len(argv) == 3 and argv[1] != "--project"):
        print(__doc__.split("\n\n")[3], file=sys.stderr)
        return 2
    root = find(argv[2] if len(argv) == 3 else None)
    if not root:
        print(f"project.py：从 {os.getcwd()} 往上没有 {NAME}，也没设 TASKQ_PROJECT", file=sys.stderr)
        return 1
    for k, v in env_of(root).items():
        if k not in os.environ:
            print(f"export {k}={shlex.quote(v)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
