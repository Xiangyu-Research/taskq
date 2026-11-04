"""找使用者的项目（占位：项目发现另有实现，合并时以那份为准）。

    find(explicit=None, start=None)  → 项目根目录（有 .taskq.toml 的文件夹）或 None：explicit（--project）> TASKQ_PROJECT >
                                       从 start（默认当前目录）往上找。explicit / TASKQ_PROJECT 给了却没有 .taskq.toml 时报 ValueError
    export(root)                     读 .taskq.toml，设 TASKQ_REPO、TASKQ_CONFIG、TASKQ_PLUGINS
"""
import os

NAME = ".taskq.toml"


def _read(path):
    try:
        import tomllib
        with open(path, "rb") as f:
            return tomllib.load(f)
    except ImportError:
        import ast
        out = {}
        for ln in open(path):
            ln = ln.split("#", 1)[0].strip()
            if "=" in ln:
                k, v = ln.split("=", 1)
                out[k.strip()] = ast.literal_eval(v.strip())
        return out


def find(explicit=None, start=None):
    given = explicit or os.environ.get("TASKQ_PROJECT")
    if given:
        root = os.path.abspath(os.path.expanduser(given))
        if not os.path.isfile(os.path.join(root, NAME)):
            raise ValueError(f"{root} 里没有 {NAME}")
        return root
    d = os.path.abspath(start or os.getcwd())
    while True:
        if os.path.isfile(os.path.join(d, NAME)):
            return d
        up = os.path.dirname(d)
        if up == d:
            return None
        d = up


def export(root):
    c = _read(os.path.join(root, NAME))
    plugins = c.get("plugins") or []
    if isinstance(plugins, str):
        plugins = [plugins]
    os.environ["TASKQ_REPO"] = root
    os.environ["TASKQ_CONFIG"] = os.path.join(root, c.get("config", "envs"))
    os.environ["TASKQ_PLUGINS"] = os.pathsep.join(os.path.join(root, p) for p in plugins)
    return c
