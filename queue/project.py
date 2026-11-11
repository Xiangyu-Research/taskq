#!/usr/bin/env python3
"""找使用者的项目（2026-11-04）：项目根目录里有 .taskq.toml，写明配置和插件文件夹（相对项目根目录）：

    config = "config"
    plugins = ["plugins"]

找的顺序：明确给的文件夹（--project）> 环境变量 TASKQ_PROJECT > 从当前目录往上找 .taskq.toml > 本机的默认项目
（~/.config/taskq/project 里写的文件夹，11/5；只在前面都找不到时用，不像环境变量那样会传给子进程）。只用标准库，不 import 核心的模块。

    python3 project.py sh [--project 文件夹]   给 bash 的 export 语句（TASKQ_REPO、TASKQ_CONFIG、TASKQ_PLUGINS，已经设了的不输出）；
                                               找不到项目时什么也不输出、退出码 1
    python3 project.py rebase <旧根> <新根>    TASKQ_REPO、TASKQ_PROJECT、TASKQ_CONFIG、TASKQ_PLUGINS 里在旧根下的路径换到新根下，
                                               打印改了的 export 语句（pueue-add 用：任务按代码副本找配置，不带调用处的路径）
"""
import os
import re
import shlex
import sys

NAME = ".taskq.toml"
DEFAULT = "~/.config/taskq/project"  # 本机的默认项目：一行，项目根目录


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
            return default()
        d = up


def default():
    """~/.config/taskq/project 里写的项目根目录（要有 .taskq.toml）；没有 → None。"""
    try:
        v = open(os.path.expanduser(DEFAULT), encoding="utf-8").read().strip()
    except OSError:
        return None
    d = os.path.abspath(os.path.expanduser(v)) if v else None
    return d if d and os.path.isfile(os.path.join(d, NAME)) else None


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


def export(root, force=False):
    """把项目的环境变量设进 os.environ（已经设了的不改；force：明确给了 --project 时都改）→ 新设的 {名: 值}。"""
    new = {k: v for k, v in env_of(root).items() if force or k not in os.environ}
    os.environ.update(new)
    return new


REBASE = ("TASKQ_REPO", "TASKQ_PROJECT", "TASKQ_CONFIG", "TASKQ_PLUGINS")


def rebase(old, new, env=None):
    """env 里 REBASE 这几个变量中在 old 下的路径（: 隔开的每一段）换到 new 下 → 改了的 {名: 值}。old 按真实路径比较。"""
    env = os.environ if env is None else env
    o = os.path.realpath(old)
    out = {}
    for k in REBASE:
        v = env.get(k)
        if not v:
            continue
        parts = []
        for p in v.split(os.pathsep):
            r = os.path.realpath(p)
            rel = os.path.relpath(r, o) if r == o or r.startswith(o + os.sep) else None
            parts.append(p if rel is None else os.path.normpath(os.path.join(new, rel)))
        nv = os.pathsep.join(parts)
        if nv != v:
            out[k] = nv
    return out


def main(argv):
    if argv and argv[0] == "rebase" and len(argv) == 3:
        for k, v in rebase(argv[1], argv[2]).items():
            print(f"export {k}={shlex.quote(v)}")
        return 0
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
