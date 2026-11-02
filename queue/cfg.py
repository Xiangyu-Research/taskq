"""配置、插件、状态在哪（2026-11-02）：TASKQ_CONFIG（machines.py、policy.py 所在的文件夹）、TASKQ_PLUGINS（做法、中继插件、
进度读法所在的文件夹，可以用 : 隔开几个）、TASKQ_STATE（本机的状态文件夹）。说明见 scripts/queue/README.md 的"开关和配置"。

    import cfg; cfg.use_config()     把配置文件夹加进 sys.path，之后 import machines、policy
    cfg.py get <键>                  policy.py 的 OUTPUTS 里的一项（给 bash 用；元组用空格连起来，没有时为空）
"""
import importlib.util
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATE_HOME = "~/.cache/phd/"
STATE_PLACES = {"taskq": "~/.cache/phd/taskq", "gpu_pad/DISABLED": "~/.cache/phd/gpu_pad/DISABLED"}
LEGACY = {"TASKQ_DATA": "BRAIN_TUMOR", "TASKQ_GPU_PAD": "RQ1_GPU_PAD", "TASKQ_TEST_FAKEBIN": "RQ1_TEST_FAKEBIN"}


def config_dir():
    return os.path.expanduser(os.environ.get("TASKQ_CONFIG") or os.path.join(REPO, "envs"))


def use_config():
    d = config_dir()
    sys.path.insert(0, d)
    return d


def outputs(key, default=None):
    d = config_dir()
    if d not in sys.path:
        sys.path.insert(0, d)
    try:
        import policy
    except ImportError:
        return default
    return getattr(policy, "OUTPUTS", {}).get(key, default)


def state(*parts, expand=True):
    rel = "/".join(parts)
    base = os.environ.get("TASKQ_STATE")
    if base:
        p = base.rstrip("/") + ("/" + rel if rel else "")
    else:
        p = next((v + rel[len(k):] for k, v in STATE_PLACES.items() if rel == k or rel.startswith(k + "/")), STATE_HOME + rel)
    return os.path.expanduser(p) if expand else p


def env(name, default=None):
    v = os.environ.get(name)
    if not v:
        old = next((o + name[len(n):] for n, o in LEGACY.items() if name == n or name.startswith(n + "_")), None)
        v = os.environ.get(old) if old else None
    return v if v else default


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


if __name__ == "__main__":
    if sys.argv[1:2] != ["get"] or len(sys.argv) != 3:
        sys.exit(__doc__.split("\n\n")[1])
    v = outputs(sys.argv[2])
    print("" if v is None else " ".join(map(str, v)) if isinstance(v, (tuple, list)) else v)
