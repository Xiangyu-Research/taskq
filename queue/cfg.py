"""配置、插件、状态在哪（2026-11-02）：TASKQ_CONFIG（machines.py、policy.py 所在的文件夹）、TASKQ_PLUGINS（做法、中继插件、
进度读法所在的文件夹，可以用 : 隔开几个）、TASKQ_STATE（本机的状态文件夹）。说明见 scripts/taskq/queue/README.md 的"开关和配置"。

    import cfg; cfg.use_config()     把配置文件夹加进 sys.path，之后 import machines、policy
    cfg.py get <键>                  policy.py 的 OUTPUTS 里的一项（给 bash 用；元组用空格连起来，没有时为空）
"""
import importlib.util
import os
import sys

CORE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 调度代码的根（嵌在使用者的仓库里时是 <仓库>/scripts/taskq）
_EMBED = os.path.join("scripts", "taskq")


def _user_repo():
    """使用者的仓库根：TASKQ_REPO；没设时，嵌在 <仓库>/scripts/taskq 里就是那个仓库，否则（单独的检出）是 CORE。"""
    v = os.environ.get("TASKQ_REPO")
    if v:
        return os.path.abspath(os.path.expanduser(v))
    if CORE.endswith(os.sep + _EMBED):
        return CORE[:-len(_EMBED) - 1]
    return CORE


REPO = _user_repo()
CORE_REL = os.path.relpath(CORE, REPO)  # 使用者的仓库里调度代码的位置（给在仓库根目录下跑的命令用）


def rel(*parts):
    """调度代码里的文件相对使用者仓库根（代码包的根同样布局）的路径：嵌着时 scripts/taskq/…，单独的检出时 …"""
    return os.path.normpath(os.path.join(CORE_REL, *parts))


STATE_HOME = "~/.cache/phd/"
STATE_PLACES = {"taskq": "~/.cache/phd/taskq", "gpu_pad/DISABLED": "~/.cache/phd/gpu_pad/DISABLED"}


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
    """插件的 tables 合进 ns（字典：插件的在前，键重复报错；列表：接在后面），其余核心没有的名字原样放进 ns；
    不以 _ 开头的函数、类和核心或先加载的插件里的同名（不是同一个）时报错（不然后加载的不生效）。"""
    add = {t: ({} if isinstance(ns[t], dict) else []) for t in tables}
    owner = {}
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
                owner[k] = m
            elif ns[k] is not v and callable(v) and not k.startswith("_") and not isinstance(v, type(sys)):
                who = f"插件 {owner[k].__file__}" if k in owner else "核心"
                raise ValueError(f"插件 {m.__file__} 的 {k} 和{who}的同名，后加载的不会生效")
    for t, v in add.items():
        ns[t] = {**v, **ns[t]} if isinstance(v, dict) else ns[t] + v


if __name__ == "__main__":
    if sys.argv[1:2] != ["get"] or len(sys.argv) != 3:
        sys.exit(__doc__.split("\n\n")[1])
    v = outputs(sys.argv[2])
    print("" if v is None else " ".join(map(str, v)) if isinstance(v, (tuple, list)) else v)
