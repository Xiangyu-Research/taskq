#!/usr/bin/env python3
"""cfg.py 的测试（2026-11-02）：TASKQ_CONFIG / TASKQ_PLUGINS / TASKQ_STATE 没设、空、不存在、相对路径；state()、outputs()、env() 和旧名、
cfg.py get / state / log；插件合并（extend）的重名。临时文件，不连机器。
    python3 queue/test_cfg.py
"""
import os
import subprocess
import sys
import textwrap

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import cfg as _cfg  # noqa: E402  调度代码的根、使用者的仓库根
QR = _cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
import cfg  # noqa: E402

T = fixture.tmpdir()
HOME = os.path.expanduser("~")
fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def setenv(**kw):
    for k, v in kw.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(textwrap.dedent(text))
    return path


# 配置文件夹
setenv(TASKQ_CONFIG=None)
ok("TASKQ_CONFIG 没设 → 仓库的 config/", cfg.config_dir() == os.path.join(cfg.REPO, "config"))
setenv(TASKQ_CONFIG="")
ok("TASKQ_CONFIG 空 → 仓库的 config/", cfg.config_dir() == os.path.join(cfg.REPO, "config"))
setenv(TASKQ_CONFIG="~/cfgx")
ok("TASKQ_CONFIG 的 ~ 展开", cfg.config_dir() == os.path.join(HOME, "cfgx"))
setenv(TASKQ_CONFIG="rel/cfg")
ok("TASKQ_CONFIG 相对路径原样（相对当前目录）", cfg.config_dir() == "rel/cfg")
setenv(TASKQ_CONFIG=os.path.join(T, "nonexistent"))
ok("TASKQ_CONFIG 不存在：照样返回，不报错", cfg.config_dir() == os.path.join(T, "nonexistent"))

# 插件文件夹
setenv(TASKQ_PLUGINS=None)
ok("TASKQ_PLUGINS 没设、没有 .taskq.toml → 没有插件", cfg.plugin_dirs() == [])
setenv(TASKQ_PLUGINS="")
ok("TASKQ_PLUGINS 空 → 没有插件", cfg.plugin_dirs() == [] and cfg.plugins("recipes") == [])
setenv(TASKQ_PLUGINS="~/p1::/abs/p2:rel/p3")
ok("TASKQ_PLUGINS 用 : 隔开，~ 展开，空的跳过，相对路径原样",
   cfg.plugin_dirs() == [os.path.join(HOME, "p1"), "/abs/p2", "rel/p3"])


def plugin_error():
    try:
        cfg.plugins("recipes")
    except ValueError as e:
        return str(e)
    return None


setenv(TASKQ_PLUGINS=os.path.join(T, "nonexistent"))
ok("TASKQ_PLUGINS 不存在的文件夹：报错（11/5）", "不存在" in (plugin_error() or ""))
setenv(TASKQ_PLUGINS="rel/p3")
ok("TASKQ_PLUGINS 相对路径：报错（11/5）", "绝对路径" in (plugin_error() or ""))
os.makedirs(os.path.join(T, "noplug"))
setenv(TASKQ_PLUGINS=os.path.join(T, "noplug"))
ok("TASKQ_PLUGINS 空文件夹：没有插件，不报错", cfg.plugins("recipes") == [])

# 调度代码在使用者仓库里的位置：嵌着（<仓库>/scripts/taskq）和单独的检出（核心就是仓库）
lay = {}
for name, sub in (("embed", "scripts/taskq/queue"), ("alone", "queue")):
    d = os.path.join(T, "layout", name, sub)
    os.makedirs(d)
    for n in ("cfg.py", "project.py"):
        with open(os.path.join(HERE, n)) as src, open(os.path.join(d, n), "w") as dst:
            dst.write(src.read())
    lay[name] = subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, sys.argv[1]); import cfg; "
                                "print(cfg.CORE_REL, cfg.rel('queue', 'taskq.py'), cfg.rel('queue'))", d],
                               capture_output=True, text=True, env={k: v for k, v in os.environ.items() if k != "TASKQ_REPO"}).stdout.split()
ok("cfg.rel：嵌着时 scripts/taskq/…，单独的检出时没有前缀（不带 ./）",
   lay["embed"] == ["scripts/taskq", "scripts/taskq/queue/taskq.py", "scripts/taskq/queue"]
   and lay["alone"] == [".", "queue/taskq.py", "queue"])

# 状态文件夹
os.makedirs(os.path.join(T, "c_none"))
setenv(TASKQ_CONFIG=os.path.join(T, "c_none"), TASKQ_STATE=None)  # 没有 policy.py 的配置文件夹：默认的状态文件夹
ok("TASKQ_STATE 没设：taskq → ~/.cache/taskq/taskq", cfg.state("taskq") == os.path.join(HOME, ".cache/taskq/taskq"))
ok("TASKQ_STATE 没设：taskq 下的子路径", cfg.state("taskq", "claims", "k") == os.path.join(HOME, ".cache/taskq/taskq/claims/k"))
ok("TASKQ_STATE 没设：gpu_pad/DISABLED", cfg.state("gpu_pad/DISABLED") == os.path.join(HOME, ".cache/taskq/gpu_pad/DISABLED"))
ok("TASKQ_STATE 没设：别的名字在 ~/.cache/taskq/ 下", cfg.state("tasks.json") == os.path.join(HOME, ".cache/taskq/tasks.json"))
ok("TASKQ_STATE 没设：taskqx 不算 taskq 下", cfg.state("taskqx") == os.path.join(HOME, ".cache/taskq/taskqx"))
ok("expand=False 保留 ~（给远程命令拼）", cfg.state("taskq", expand=False) == "~/.cache/taskq/taskq")
setenv(TASKQ_STATE="")
ok("TASKQ_STATE 空 → 同没设", cfg.state("taskq") == os.path.join(HOME, ".cache/taskq/taskq"))
setenv(TASKQ_STATE=os.path.join(T, "st") + "/")
ok("TASKQ_STATE 设了：都在它下面（末尾 / 去掉）",
   cfg.state("taskq") == os.path.join(T, "st/taskq") and cfg.state("gpu_pad/DISABLED") == os.path.join(T, "st/gpu_pad/DISABLED"))
ok("TASKQ_STATE 设了：不带名字 → 它本身", cfg.state() == os.path.join(T, "st"))
setenv(TASKQ_STATE="~/stx")
ok("TASKQ_STATE 的 ~：expand 时展开，不 expand 时留着",
   cfg.state("taskq") == os.path.join(HOME, "stx/taskq") and cfg.state("taskq", expand=False) == "~/stx/taskq")
setenv(TASKQ_STATE=None)

# env()
setenv(TASKQ_DATA="/new")
ok("env：设了 → 它的值", cfg.env("TASKQ_DATA") == "/new")
setenv(TASKQ_DATA="")
ok("env：空的 → 默认值", cfg.env("TASKQ_DATA", "d") == "d")
setenv(TASKQ_DATA=None)
ok("env：没设 → 默认值", cfg.env("TASKQ_DATA", "d") == "d" and cfg.env("TASKQ_DATA") is None)


# outputs()：import policy 会缓存，每种情况在单独的进程里跑
def outputs_in(config, key, default="DEF"):
    code = f"import sys; sys.path.insert(0, {HERE!r}); import cfg; print(repr(cfg.outputs({key!r}, {default!r})))"
    env = dict(os.environ, TASKQ_CONFIG=config)
    r = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else "ERR " + r.stderr


ok("outputs：测试配置的 name", outputs_in(fixture.CONFIG, "name") == "'Demo'")
ok("outputs：没有的键 → 默认值", outputs_in(fixture.CONFIG, "no_such_key") == "'DEF'")
c1 = os.path.join(T, "c_noout")
write(os.path.join(c1, "policy.py"), "AUTO = {}\n")
ok("outputs：policy.py 里没有 OUTPUTS → 默认值", outputs_in(c1, "name") == "'DEF'")
os.makedirs(os.path.join(T, "c_empty"))
ok("outputs：配置文件夹里没有 policy.py → 默认值", outputs_in(os.path.join(T, "c_empty"), "name") == "'DEF'")
ok("outputs：写明的配置文件夹不在 → 报错（11/5）", "不存在" in outputs_in(os.path.join(T, "c_missing"), "name"))
c2 = os.path.join(T, "c_badimp")
write(os.path.join(c2, "policy.py"), "import no_such_module_x\nOUTPUTS = {}\n")
ok("outputs：policy.py 里 import 失败 → 报错，不用默认值（11/5）", "no_such_module_x" in outputs_in(c2, "name"))


def cfg_get(config, *args):
    env = dict(os.environ, TASKQ_CONFIG=config)
    return subprocess.run([sys.executable, os.path.join(HERE, "cfg.py"), *args], env=env, capture_output=True, text=True)


r = cfg_get(fixture.CONFIG, "get", "name")
ok("cfg.py get：字符串原样", r.returncode == 0 and r.stdout == "Demo\n")
r = cfg_get(fixture.CONFIG, "get", "code_dirs")
ok("cfg.py get：元组用空格连起来", r.returncode == 0 and r.stdout == f"{QR} {QR}/testdata/code\n")
r = cfg_get(fixture.CONFIG, "get", "no_such_key")
ok("cfg.py get：没有的键 → 空行，退出码 0", r.returncode == 0 and r.stdout == "\n")
r = cfg_get(c1, "get", "name")
ok("cfg.py get：没有 OUTPUTS → 空行", r.returncode == 0 and r.stdout == "\n")
r = cfg_get(fixture.CONFIG, "get")
r2 = cfg_get(fixture.CONFIG, "set", "name")
ok("cfg.py：参数不对 → 打印用法、退出码非 0", r.returncode != 0 and r2.returncode != 0 and "cfg.py get" in r.stderr)

# OUTPUTS["state"]、OUTPUTS["logs"]（11/4）：每种情况单独的进程
cs = os.path.join(T, "c_state")
write(os.path.join(cs, "policy.py"), 'OUTPUTS = dict(state="~/st_o", logs="~/lg")\n')
r = cfg_get(cs, "state", "taskq")
ok("cfg.py state：OUTPUTS[\"state\"] 代替默认（STATE_PLACES 也不用）", r.returncode == 0 and r.stdout == os.path.join(HOME, "st_o/taskq") + "\n")
r = cfg_get(cs, "state")
ok("cfg.py state：不带名字 → 文件夹本身，末尾带 /", r.returncode == 0 and r.stdout == os.path.join(HOME, "st_o") + "/\n")
r = subprocess.run([sys.executable, os.path.join(HERE, "cfg.py"), "state", "taskq"], capture_output=True, text=True,
                   env=dict(os.environ, TASKQ_CONFIG=cs, TASKQ_STATE=os.path.join(T, "st2")))
ok("cfg.py state：TASKQ_STATE 比 OUTPUTS[\"state\"] 优先", r.stdout == os.path.join(T, "st2/taskq") + "\n")
r = cfg_get(c1, "state", "taskq")
ok("cfg.py state：没有 OUTPUTS[\"state\"] → 核心的默认", r.stdout == os.path.join(HOME, ".cache/taskq/taskq") + "\n")
r = cfg_get(cs, "log", "feed")
ok("cfg.py log：OUTPUTS[\"logs\"] 下的 <名字>.log", r.returncode == 0 and r.stdout == os.path.join(HOME, "lg/feed.log") + "\n")
r = cfg_get(c1, "log", "feed")
ok("cfg.py log：没有 OUTPUTS[\"logs\"] → ~/<名字>.log", r.stdout == os.path.join(HOME, "feed.log") + "\n")
r = subprocess.run([sys.executable, os.path.join(HERE, "cfg.py"), "log", "feed"], capture_output=True, text=True,
                   env=dict(os.environ, TASKQ_CONFIG=cs, FEED_LOG="~/x.log"))
ok("cfg.py log：<名字大写>_LOG 优先", r.stdout == os.path.join(HOME, "x.log") + "\n")
write(os.path.join(T, "c_cron", "policy.py"), 'OUTPUTS = dict(cron=(("5 * * * *", "a"), ("@daily", "b")), dirs=("x", "y"))\n')
r, r2 = cfg_get(os.path.join(T, "c_cron"), "get", "cron"), cfg_get(os.path.join(T, "c_cron"), "get", "dirs")
ok("cfg.py get：元组的元组每项一行，平的元组仍用空格连", r.stdout == "5 * * * * a\n@daily b\n" and r2.stdout == "x y\n")


# 插件合并
NAME = "extmod"


def plugdir(tag, text):
    d = os.path.join(T, "plug_" + tag)
    write(os.path.join(d, NAME + ".py"), text)
    return d


def core_fn():
    return "core"


def merged(*dirs):
    setenv(TASKQ_PLUGINS=os.pathsep.join(dirs))
    ns = dict(TABLE={"core": 1}, LIST=["a"], core_fn=core_fn, CONST=1)
    try:
        cfg.extend(ns, NAME, ("TABLE", "LIST"))
        return ns
    except ValueError as e:
        return e


pa = plugdir("a", """
    import json
    from os.path import join
    TABLE = {"pa": 2}
    LIST = ["a", "b"]
    CONST = 2
    def helper():
        return "a"
    def _private():
        return "a"
    class Thing:
        pass
""")
pb = plugdir("b", """
    import json
    from os.path import join
    TABLE = {"pb": 3}
    LIST = ["b", "c"]
    def _private():
        return "b"
    def other():
        return "b"
""")
ns = merged(pa, pb)
ok("extend：两个插件文件夹合进来不报错", isinstance(ns, dict))
if isinstance(ns, dict):
    ok("extend：字典插件的在前，核心的在后，都在", list(ns["TABLE"]) == ["pa", "pb", "core"])
    ok("extend：列表接在后面、不重复", ns["LIST"] == ["a", "b", "c"])
    ok("extend：只合约定的表，插件的其他名字（函数、类、import 的模块、常量）不进来（11/4）",
       not {"helper", "other", "Thing", "_private", "json", "join"} & set(ns))
    ok("extend：核心已有的常量、函数不被插件改", ns["CONST"] == 1 and ns["core_fn"] is core_fn)

pc = plugdir("c", """
    TABLE = {"pa": 9}
""")
e = merged(pa, pc)
ok("extend：两个插件的表里同一个键 → 报错", isinstance(e, ValueError) and "pa" in str(e))
pd = plugdir("d", """
    TABLE = {"core": 9}
""")
e = merged(pd)
ok("extend：插件的表和核心的表同一个键 → 报错", isinstance(e, ValueError) and "core" in str(e))
pe = plugdir("e", """
    def helper():
        return "e"
""")
e = merged(pe)
ok("extend：插件一张约定的表也没导出 → 报错（多半是文件放错或表名拼错）", isinstance(e, ValueError) and "没有导出" in str(e))
pf = plugdir("f", """
    TABLE = [1]
""")
e = merged(pf)
ok("extend：表的类型不对 → 报错", isinstance(e, ValueError) and "TABLE" in str(e) and "字典" in str(e))


def merged_fn(*dirs):
    setenv(TASKQ_PLUGINS=os.pathsep.join(dirs))
    ns = dict(TABLE={}, FN=None)
    try:
        cfg.extend(ns, NAME, ("TABLE", "FN"))
        return ns
    except ValueError as e:
        return e


pg = plugdir("g", """
    def FN():
        return "g"
""")
ph = plugdir("h", """
    TABLE = {"h": 1}
""")
ns = merged_fn(ph)
ok("extend：核心里是 None 的名字（函数）插件没给 → 照旧 None", isinstance(ns, dict) and ns["FN"] is None and ns["TABLE"] == {"h": 1})
ns = merged_fn(pg, ph)
ok("extend：核心里是 None 的名字 → 用插件的函数", isinstance(ns, dict) and ns["FN"]() == "g")
e = merged_fn(pg, plugdir("i", """
    def FN():
        return "i"
"""))
ok("extend：两个插件都给了同一个函数 → 报错", isinstance(e, ValueError) and "FN" in str(e))
e = merged_fn(plugdir("j", """
    FN = {"x": 1}
"""))
ok("extend：函数给成了别的 → 报错", isinstance(e, ValueError) and "FN" in str(e) and "函数" in str(e))
# 每类插件导出的名字：cfg.KINDS，核心的模块和测试插件对得上
ok("KINDS：recipes 导出 RECIPES、HOOKS，relay_plugins 导出 PLUGINS、ACCEPTS、VIEW_ROOTS，consumers 导出 CONSUMERS，"
   "smoke 导出 SMOKE，labels 导出 STEPS、QUIET、PLAN_TITLES",
   cfg.KINDS == {"recipes": ("RECIPES", "HOOKS"), "relay_plugins": ("PLUGINS", "ACCEPTS", "VIEW_ROOTS"),
                 "consumers": ("CONSUMERS",), "smoke": ("SMOKE",), "labels": ("STEPS", "QUIET", "PLAN_TITLES")})
setenv(TASKQ_PLUGINS=fixture.PLUGINS)
r = subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, sys.argv[1]); import recipes, relay_plugins; "
                    "print(sorted(recipes.RECIPES), sorted(recipes.HOOKS), sorted(relay_plugins.PLUGINS), "
                    "hasattr(recipes, 'model_of'), hasattr(relay_plugins, 'infer_inputs'), "
                    "relay_plugins.RelayError is __import__('taskq_api').RelayError)", HERE],
                   capture_output=True, text=True, env=dict(os.environ))
ok("测试插件：做法、钩子、中继插件合进来，插件里的函数不进核心模块，RelayError 是 taskq_api 的同一个",
   r.stdout.split() == ["['command',", "'toy']", "['model_of']", "['toy',", "'toy_infer']", "False", "False", "True"])

# 找项目（project.py，11/4）：--project > TASKQ_PROJECT > 从当前目录往上找 .taskq.toml > ~/.config/taskq/project（11/5）；cfg 的仓库根：TASKQ_REPO > 嵌着 > 找到的项目 > CORE
import project  # noqa: E402
PJ = os.path.join(T, "proj")
write(os.path.join(PJ, ".taskq.toml"), """
    # 测试项目
    config = "conf"
    plugins = ["p1", "p/two"]  # 两个
""")
os.makedirs(os.path.join(PJ, "a", "b"))
setenv(TASKQ_PROJECT=None, TASKQ_REPO=None, TASKQ_CONFIG=None, TASKQ_PLUGINS=None)
ok("project.find：从子文件夹往上找到 .taskq.toml", project.find(start=os.path.join(PJ, "a", "b")) == PJ)
ok("project.find：往上没有 → None", project.find(start=os.path.join(T, "layout")) is None)
ok("project.find：明确给的优先（不要求有 .taskq.toml）", project.find(os.path.join(T, "other"), start=PJ) == os.path.join(T, "other"))
setenv(TASKQ_PROJECT="~/pj")
ok("project.find：TASKQ_PROJECT 比往上找优先，~ 展开", project.find(start=PJ) == os.path.join(HOME, "pj"))
ok("project.find：明确给的比 TASKQ_PROJECT 优先", project.find(PJ) == PJ)
setenv(TASKQ_PROJECT=None)
project.DEFAULT = os.path.join(T, "default_project")  # never the real ~/.config/taskq/project
_dp = project.DEFAULT
write(_dp, PJ + "\n")
ok("project.find：往上没有时用 ~/.config/taskq/project 写的（11/5）", project.find(start=os.path.join(T, "layout")) == PJ)
PJ2 = os.path.join(T, "proj2")
write(os.path.join(PJ2, ".taskq.toml"), 'config = "c"\n')
ok("project.find：往上找到的比默认项目优先", project.find(start=PJ2) == PJ2)
write(_dp, os.path.join(T, "layout") + "\n")
ok("project.find：默认项目里没有 .taskq.toml → None", project.find(start=os.path.join(T, "layout")) is None)
os.remove(_dp)
ok("project.load：读 config、plugins", project.load(PJ) == {"config": "conf", "plugins": ["p1", "p/two"]})
ok("project.load：没有 .taskq.toml → {}", project.load(T) == {})
ok("project.parse（没有 tomllib 时）：字符串、列表、空列表、注释、空行",
   project.parse('# c\n\nconfig = "envs"  # 注释\nplugins = ["a", "b/c"]\nx = []\ny = ["#不是注释"]\n')
   == {"config": "envs", "plugins": ["a", "b/c"], "x": [], "y": ["#不是注释"]})
try:
    project.parse("[table]\n")
    ok("project.parse：看不懂的行报错", False)
except ValueError as e:
    ok("project.parse：看不懂的行报错（说明第几行）", "第 1 行" in str(e))
ok("project.env_of：TASKQ_REPO、TASKQ_CONFIG、TASKQ_PLUGINS（: 隔开）",
   project.env_of(PJ) == {"TASKQ_REPO": PJ, "TASKQ_CONFIG": os.path.join(PJ, "conf"),
                          "TASKQ_PLUGINS": os.path.join(PJ, "p1") + ":" + os.path.join(PJ, "p/two")})
write(os.path.join(T, "proj_s", ".taskq.toml"), 'plugins = "one"\n')
ok("project.env_of：plugins 写成一个字符串也行，没写 config 就不设 TASKQ_CONFIG",
   project.env_of(os.path.join(T, "proj_s")) == {"TASKQ_REPO": os.path.join(T, "proj_s"), "TASKQ_PLUGINS": os.path.join(T, "proj_s", "one")})
setenv(TASKQ_CONFIG="/keep")
new = project.export(PJ)
ok("project.export：已经设了的不改，其余设上", os.environ["TASKQ_CONFIG"] == "/keep" and os.environ["TASKQ_REPO"] == PJ
   and "TASKQ_CONFIG" not in new and "TASKQ_PLUGINS" in new)
setenv(TASKQ_CONFIG=None, TASKQ_REPO=None, TASKQ_PLUGINS=None)


def proj_sh(cwd, *args, **env):
    e = {k: v for k, v in os.environ.items() if not k.startswith("TASKQ_")}
    e.update(env)
    return subprocess.run([sys.executable, os.path.join(HERE, "project.py"), "sh", *args], cwd=cwd, env=e, capture_output=True, text=True)


r = proj_sh(os.path.join(PJ, "a"))
ok("project.py sh：往上找到项目，打印 export", r.returncode == 0 and f"export TASKQ_REPO={PJ}\n" in r.stdout
   and f"export TASKQ_CONFIG={PJ}/conf\n" in r.stdout and r.stdout.count("export ") == 3)
r = proj_sh(os.path.join(PJ, "a"), TASKQ_CONFIG="/x")
ok("project.py sh：已经设了的不输出", r.returncode == 0 and "TASKQ_CONFIG" not in r.stdout and "TASKQ_REPO" in r.stdout)
r = proj_sh(T)
ok("project.py sh：找不到 → 什么也不输出，退出码 1", r.returncode == 1 and r.stdout == "" and ".taskq.toml" in r.stderr)
r = proj_sh(T, "--project", PJ)
ok("project.py sh --project", r.returncode == 0 and f"export TASKQ_REPO={PJ}\n" in r.stdout)
r = subprocess.run([sys.executable, os.path.join(HERE, "project.py")], capture_output=True, text=True)
ok("project.py：参数不对 → 用法，退出码 2", r.returncode == 2 and "project.py sh" in r.stderr)


def cfg_in(cwd, code, layout="alone", **env):
    """在复制的核心（alone：单独的检出；embed：<仓库>/scripts/taskq）里 import cfg，跑 code。"""
    d = os.path.join(T, "disc_" + layout, "scripts/taskq/queue" if layout == "embed" else "queue")
    if not os.path.isdir(d):
        os.makedirs(d)
        for n in ("cfg.py", "project.py"):
            with open(os.path.join(HERE, n)) as src, open(os.path.join(d, n), "w") as dst:
                dst.write(src.read())
    e = {k: v for k, v in os.environ.items() if not k.startswith("TASKQ_")}
    e.update(env)
    r = subprocess.run([sys.executable, "-c", f"import sys; sys.path.insert(0, {d!r}); import cfg; print(repr({code}))"],
                       cwd=cwd, env=e, capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else "ERR " + r.stderr


ALONE = os.path.join(T, "disc_alone")
ok("cfg：单独的检出、当前目录往上有 .taskq.toml → 那个项目，配置和插件按它",
   cfg_in(os.path.join(PJ, "a"), "(cfg.REPO, cfg.config_dir(), cfg.plugin_dirs())")
   == repr((PJ, os.path.join(PJ, "conf"), [os.path.join(PJ, "p1"), os.path.join(PJ, "p/two")])))
ok("cfg：TASKQ_PROJECT 也认", cfg_in(T, "cfg.REPO", TASKQ_PROJECT=PJ) == repr(PJ))
ok("cfg：TASKQ_REPO 比往上找优先", cfg_in(os.path.join(PJ, "a"), "cfg.REPO", TASKQ_REPO=T) == repr(T))
ok("cfg：TASKQ_CONFIG、TASKQ_PLUGINS 比 .taskq.toml 优先",
   cfg_in(PJ, "(cfg.config_dir(), cfg.plugin_dirs())", TASKQ_CONFIG="/c", TASKQ_PLUGINS="/p") == repr(("/c", ["/p"])))
ok("cfg：找不到项目 → 核心自己，没有 .taskq.toml 时配置在 config/、没有插件",
   cfg_in(T, "(cfg.REPO, cfg.config_dir(), cfg.plugin_dirs())")
   == repr((ALONE, os.path.join(ALONE, "config"), [])))
write(os.path.join(T, "proj_e", ".taskq.toml"), "")
ok("cfg：.taskq.toml 没写 config、plugins → 项目根、没有插件",
   cfg_in(os.path.join(T, "proj_e"), "(cfg.config_dir(), cfg.plugin_dirs())") == repr((os.path.join(T, "proj_e"), [])))
EMB = os.path.join(T, "disc_embed")
write(os.path.join(EMB, ".taskq.toml"), 'config = "cfgdir"\n')
ok("cfg：嵌在 <仓库>/scripts/taskq 里时就是那个仓库（不往上找），仓库根的 .taskq.toml 照样读",
   cfg_in(os.path.join(PJ, "a"), "(cfg.REPO, cfg.config_dir())", layout="embed") == repr((EMB, os.path.join(EMB, "cfgdir"))))
os.remove(os.path.join(EMB, ".taskq.toml"))
ok("cfg：嵌着、仓库根没有 .taskq.toml → <仓库>/config（11/5）",
   cfg_in(os.path.join(PJ, "a"), "(cfg.REPO, cfg.config_dir())", layout="embed") == repr((EMB, os.path.join(EMB, "config"))))

setenv(TASKQ_PLUGINS=None, TASKQ_CONFIG=None)
print(f"\n{'全部通过' if not fails else f'{fails} 项失败'}")
sys.exit(1 if fails else 0)
