"""配置、插件、状态在哪：TASKQ_CONFIG（machines.py、policy.py 所在的文件夹）、TASKQ_PLUGINS（做法、中继插件、
进度读法所在的文件夹，可以用 : 隔开几个）、TASKQ_STATE（本机的状态文件夹）、TASKQ_ROOT（执行机上的进程：这台的根目录）。说明见 queue/README.md 的"开关和配置"。

    import cfg; cfg.use_config()     把配置文件夹加进 sys.path，之后 import policy；机器表用 cfg.mtab()（sites/mtab.py）
    cfg.py get <键>                  policy.py 的 OUTPUTS 里的一项（给 bash 用；元组用空格连，元组的元组每项一行；没有时为空）
    cfg.py state [名字…]             本机的状态文件夹（或其中的路径，~ 展开）
    cfg.py data                      这个进程的数据目录（data_dir；没有时为空）
    cfg.py log <名字>                日志文件 <名字>.log 的位置（~ 展开）

插件：TASKQ_PLUGINS 的每个文件夹里按文件名找，每类插件只取下面这些名字（KINDS），别的名字（辅助函数、import 进来的）
不进核心；一个也没导出时报错。插件里要用核心的东西只 import taskq_api（taskq_api.py），不 import 核心的其他模块。
  recipes.py         RECIPES  {做法名: 做法}（字段见 recipes.py 开头）
                     HOOKS    {钩子名: 函数}（可省；有哪些钩子见 recipes.py 的 HOOKS）
  relay_plugins.py   PLUGINS  {插件名: (export, check)}（见 relay_plugins.py 开头）
                     ACCEPTS  {插件名: 函数(参数) → 能不能送}（可省）
                     VIEW_ROOTS  [建视图的文件夹]（可省）
  consumers.py       CONSUMERS  {插件名: 消费者插件}（钩子见 consume.py、queue/README.md 的"消费者"）
  smoke.py           SMOKE  {做法名: dict(by=[参数名], params={…}, vars={…})}（冒烟闸门，见 smoke_gate.py）
  labels.py          STEPS  {步骤名: 中文名}（推送、进度报告的白话，monitor/plain.py）
                     QUIET  [进度报告里不列的步骤]（可省）
                     PLAN_TITLES  函数() → {分析编号: 标题}（可省；pueue 标签 <编号> 或 <编号>_… 说成"分析 <编号> <标题>"）
  readers/*.py       PROGRESS、INFO、TRAINED（spec.py 读；只用标准库，经 ssh 送到别的机器上跑）
各表合并时键重复报错；函数（核心里默认是 None 的名字）只能有一个插件给。
"""
import importlib.util
import os
import sys

CORE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 调度代码的根（嵌在使用者的仓库里时是 <仓库>/scripts/taskq）
_EMBED = os.path.join("scripts", "taskq")


def _project():
    """同一文件夹里的 project.py（按文件加载，不靠 sys.path，名字不和使用者的模块撞）。"""
    m = sys.modules.get("taskq_project")
    if m is None:
        s = importlib.util.spec_from_file_location("taskq_project", os.path.join(os.path.dirname(os.path.abspath(__file__)), "project.py"))
        m = importlib.util.module_from_spec(s)
        s.loader.exec_module(m)
        sys.modules["taskq_project"] = m
    return m


def _user_repo():
    """使用者的仓库根：TASKQ_REPO；没设时，嵌在 <仓库>/scripts/taskq 里就是那个仓库；否则 TASKQ_PROJECT 或从当前目录往上找到的
    .taskq.toml 所在的文件夹（project.py）；都没有时是 CORE。"""
    v = os.environ.get("TASKQ_REPO")
    if v:
        return os.path.abspath(os.path.expanduser(v))
    if CORE.endswith(os.sep + _EMBED):
        return CORE[:-len(_EMBED) - 1]
    return _project().find() or CORE


REPO = _user_repo()
CORE_REL = os.path.relpath(CORE, REPO)  # 使用者的仓库里调度代码的位置（给在仓库根目录下跑的命令用）


def rel(*parts):
    """调度代码里的文件相对使用者仓库根的路径：嵌着时 scripts/taskq/…，单独的检出时 …（核心在仓库外时带 ..）。
    只给在本机仓库根目录下跑的命令；执行机、代码包里的用 bundle_rel，hub 上排进 pueue 的用 local。"""
    return os.path.normpath(os.path.join(CORE_REL, *parts))


def bundle_rel(*parts):
    """调度代码里的文件在代码包里（相对包的根，即那边的 code/current）的路径：总是 proto.BUNDLE_CORE（scripts/taskq）/…"""
    if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from proto import BUNDLE_CORE
    return "/".join((BUNDLE_CORE,) + parts)


def local(*parts):
    """调度代码里的文件在本机的绝对路径（hub 上生成、在 hub 上跑的命令用）。"""
    return os.path.join(CORE, *parts)


# 状态文件夹：TASKQ_STATE > TASKQ_ROOT（执行机上的进程：根目录下，任务队列在 queue/，其余在 state/，proto.STATE_IN_ROOT）
# > policy.py 的 OUTPUTS["state"]（没写时取 schema.OUTPUT_FIELDS 的默认）


_PROJECT = []


def project_conf():
    """使用者仓库根的 .taskq.toml（project.py）→ dict；没有这个文件 → None。"""
    if not _PROJECT:
        pj = _project()
        _PROJECT.append(pj.load(REPO) if os.path.isfile(os.path.join(REPO, pj.NAME)) else None)
    return _PROJECT[0]


def config_dir():
    """配置文件夹：TASKQ_CONFIG > 仓库根 .taskq.toml 的 config > 没有 .taskq.toml 时 <仓库>/config（同 example/）。"""
    v = os.environ.get("TASKQ_CONFIG")
    if v:
        return os.path.expanduser(v)
    conf = project_conf()
    if conf is None:
        return os.path.join(REPO, "config")
    return os.path.normpath(os.path.join(REPO, conf.get("config", ".")))


def mtab():
    """机器表（核心的 sites/mtab.py，数据取配置文件夹里的 machines.py）。"""
    d = os.path.join(CORE, "sites")
    if d not in sys.path:
        sys.path.insert(0, d)
    import mtab as m
    return m


def use_config():
    d = config_dir()
    sys.path.insert(0, d)
    return d


def outputs(key):
    """policy.py 的 OUTPUTS[key]；没写、配置文件夹里没有 policy.py → schema.OUTPUT_FIELDS 的默认（不认识的 key → KeyError）。
    写明的配置文件夹不在、policy.py 读不了 → 报错（不悄悄用默认的状态文件夹）。"""
    from schema import OUTPUT_FIELDS
    if key not in OUTPUT_FIELDS:
        raise KeyError(f"OUTPUTS 没有字段 {key}（有：{'、'.join(OUTPUT_FIELDS)}）")
    default = OUTPUT_FIELDS[key][1]
    d = config_dir()
    if d not in sys.path:
        sys.path.insert(0, d)
    try:
        import policy
    except ModuleNotFoundError as e:
        if e.name != "policy":
            raise
        if (os.environ.get("TASKQ_CONFIG") or project_conf() is not None) and not os.path.isdir(d):
            raise ValueError(f"配置文件夹 {d} 不存在（TASKQ_CONFIG 或 .taskq.toml 的 config）") from None
        return default
    return getattr(policy, "OUTPUTS", {}).get(key, default)


def state_home():
    """配置的状态文件夹（不展开 ~，以 / 结尾）：OUTPUTS["state"]。"""
    return outputs("state").rstrip("/") + "/"


def state(*parts, expand=True):
    """状态文件夹里的路径（见上面状态文件夹的说明）。"""
    rel = "/".join(parts)
    base, root = os.environ.get("TASKQ_STATE"), os.environ.get("TASKQ_ROOT")
    if root and not base:
        if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from proto import ROOT, STATE_IN_ROOT
        r = root.rstrip("/")
        p = next((f"{r}/{v}{rel[len(k):]}" for k, v in STATE_IN_ROOT.items() if rel == k or rel.startswith(k + "/")),
                 f"{r}/{ROOT['state']}/{rel}")
    else:
        p = (base or state_home()).rstrip("/") + "/" + rel
    return os.path.expanduser(p) if expand else p


def data_dir():
    """这个进程的数据目录（展开 ~）：TASKQ_DATA（run_task.sh 给任务的命令设）> TASKQ_ROOT 下的 data（执行机上的进程）
    > 本机是 store 时 store 的数据目录；都没有 → ""。"""
    v = os.environ.get("TASKQ_DATA")
    if v:
        return os.path.expanduser(v)
    root = os.environ.get("TASKQ_ROOT")
    if root:
        if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from proto import ROOT
        return os.path.expanduser(f"{root.rstrip('/')}/{ROOT['data']}")
    M = mtab()
    me = M.whoami()
    return M.expand(M.machine(me)["data"]) if me and M.machine(me).get("data") else ""


def log(name):
    """日志文件 <name>.log：环境变量 <NAME>_LOG（给测试用）> OUTPUTS["logs"] 文件夹 > 家目录。"""
    v = os.environ.get(name.upper() + "_LOG")
    return os.path.expanduser(v or os.path.join(outputs("logs"), name + ".log"))


def env(name, default=None):
    v = os.environ.get(name)
    return v if v else default


def plugin_dirs():
    """插件文件夹：TASKQ_PLUGINS（设了就算空的也用）> 仓库根 .taskq.toml 的 plugins > 没有 .taskq.toml 时没有插件。"""
    v = os.environ.get("TASKQ_PLUGINS")
    if v is None:
        conf = project_conf()
        if conf is None:
            return []
        p = conf.get("plugins") or []
        return [os.path.join(REPO, d) for d in ([p] if isinstance(p, str) else p)]
    return [os.path.expanduser(d) for d in v.split(os.pathsep) if d]


KINDS = {
    "recipes": ("RECIPES", "HOOKS"),
    "relay_plugins": ("PLUGINS", "ACCEPTS", "VIEW_ROOTS"),
    "consumers": ("CONSUMERS",),
    "smoke": ("SMOKE",),
    "labels": ("STEPS", "QUIET", "PLAN_TITLES"),
}


def plugins(name):
    """各插件文件夹里的 <name>.py → [模块]。插件文件夹要是绝对路径；写明的（TASKQ_PLUGINS、.taskq.toml）不在 → 报错。"""
    mods = []
    named = os.environ.get("TASKQ_PLUGINS") is not None or project_conf() is not None
    for i, d in enumerate(plugin_dirs()):
        if not os.path.isabs(d):
            raise ValueError(f"插件文件夹要写绝对路径：{d}")
        if named and not os.path.isdir(d):
            raise ValueError(f"插件文件夹 {d} 不存在（TASKQ_PLUGINS 或 .taskq.toml 的 plugins）")
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


def extend(ns, name, tables=None):
    """插件导出的表合进 ns 里的同名表（tables 默认 KINDS[name]）：字典插件的在前，键重复报错；列表接在后面、不重复。
    ns 里是 None 的名字是函数：插件给的要能调用，只能有一个插件给。
    只合这些表，插件的其他名字不进 ns；插件一张表也没导出、或表的类型不对时报错。"""
    tables = tuple(tables or KINDS[name])
    add = {t: ({} if isinstance(ns[t], dict) else [] if isinstance(ns[t], list) else None) for t in tables}
    for m in plugins(name):
        got = [t for t in tables if t in vars(m)]
        if not got:
            raise ValueError(f"插件 {m.__file__} 没有导出 {'、'.join(tables)} 里的任何一个")
        for k in got:
            v = vars(m)[k]
            if ns[k] is None:
                if not callable(v):
                    raise ValueError(f"插件 {m.__file__} 的 {k} 要是函数")
                if add[k] is not None:
                    raise ValueError(f"插件 {m.__file__} 的 {k} 重名（另一个插件已经给了）")
                add[k] = v
                continue
            if not isinstance(v, type(add[k])):
                raise ValueError(f"插件 {m.__file__} 的 {k} 要是{'字典' if isinstance(add[k], dict) else '列表'}")
            if isinstance(add[k], dict):
                dup = sorted(set(add[k]) & set(v) | set(ns[k]) & set(v))
                if dup:
                    raise ValueError(f"插件 {m.__file__} 的 {k} 里 {', '.join(map(str, dup))} 重名")
                add[k].update(v)
            else:
                add[k] += [x for x in v if x not in add[k] and x not in ns[k]]
    for t, v in add.items():
        if ns[t] is None:
            ns[t] = v
        else:
            ns[t] = {**v, **ns[t]} if isinstance(v, dict) else ns[t] + v


if __name__ == "__main__":
    a = sys.argv[1:]
    if a[:1] == ["state"]:
        print(state(*a[1:]))
    elif a == ["data"]:
        print(data_dir())
    elif a[:1] == ["log"] and len(a) == 2:
        print(log(a[1]))
    elif a[:1] == ["get"] and len(a) == 2:
        try:
            v = outputs(a[1])
        except KeyError as e:
            sys.exit(f"cfg.py get：{e.args[0]}")
        if isinstance(v, (tuple, list)):  # 元组用空格连；元组的元组每项一行
            seq = (tuple, list)
            print(("\n" if any(isinstance(x, seq) for x in v) else " ").join(" ".join(map(str, x)) if isinstance(x, seq) else str(x) for x in v))
        else:
            print("" if v is None else v)
    else:
        sys.exit(__doc__.split("\n\n")[1])
