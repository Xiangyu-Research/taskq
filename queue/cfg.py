"""配置和插件在哪（2026-11-02）：TASKQ_CONFIG（machines.py、policy.py 所在的文件夹）、TASKQ_PLUGINS（做法、中继插件、
进度读法所在的文件夹，可以用 : 隔开几个）。说明见 scripts/queue/README.md 的"开关和配置"。

    import cfg; cfg.use_config()     把配置文件夹加进 sys.path，之后 import machines、policy
"""
import importlib.util
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def config_dir():
    return os.path.expanduser(os.environ.get("TASKQ_CONFIG") or os.path.join(REPO, "envs"))


def use_config():
    d = config_dir()
    sys.path.insert(0, d)
    return d


def plugin_dirs():
    v = os.environ.get("TASKQ_PLUGINS")
    if v is None:
        return [os.path.join(REPO, "research", "common", "taskq_plugins")]
    return [os.path.expanduser(d) for d in v.split(os.pathsep) if d]


def plugins(name):
    mods = []
    for i, d in enumerate(plugin_dirs()):
        p = os.path.join(d, name + ".py")
        if not os.path.isfile(p):
            continue
        mn = f"taskq_plugin{i}_{name}"
        s = importlib.util.spec_from_file_location(mn, p)
        m = importlib.util.module_from_spec(s)
        sys.modules[mn] = m
        s.loader.exec_module(m)
        mods.append(m)
    return mods


def extend(ns, name, tables):
    """插件的 tables 合进 ns（字典：插件的在前，键重复报错；列表：接在后面），其余核心没有的名字原样放进 ns。"""
    add = {t: ({} if isinstance(ns[t], dict) else []) for t in tables}
    for m in plugins(name):
        for k, v in vars(m).items():
            if k.startswith("__"):
                continue
            if k in add:
                if isinstance(add[k], dict):
                    dup = sorted(set(add[k]) & set(v) | set(ns[k]) & set(v))
                    if dup:
                        raise ValueError(f"插件 {m.__file__} 的 {k} 里 {', '.join(map(str, dup))} 重名")
                    add[k].update(v)
                else:
                    add[k] += [x for x in v if x not in add[k] and x not in ns[k]]
            elif k not in ns:
                ns[k] = v
    for t, v in add.items():
        ns[t] = {**v, **ns[t]} if isinstance(v, dict) else ns[t] + v
