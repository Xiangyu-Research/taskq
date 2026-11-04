#!/usr/bin/env python3
"""cfg.py 的测试（2026-11-02）：TASKQ_CONFIG / TASKQ_PLUGINS / TASKQ_STATE 没设、空、不存在、相对路径；state()、outputs()、env() 和旧名、
cfg.py get / state / log；插件合并（extend）的重名。临时文件，不连机器。
    python3 scripts/taskq/queue/test_cfg.py
"""
import os
import subprocess
import sys
import tempfile
import textwrap

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import cfg as _cfg  # noqa: E402  调度代码的根、使用者的仓库根
QR = _cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
import cfg  # noqa: E402

T = tempfile.mkdtemp()
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
ok("TASKQ_CONFIG 没设 → 仓库的 envs/", cfg.config_dir() == os.path.join(cfg.REPO, "envs"))
setenv(TASKQ_CONFIG="")
ok("TASKQ_CONFIG 空 → 仓库的 envs/", cfg.config_dir() == os.path.join(cfg.REPO, "envs"))
setenv(TASKQ_CONFIG="~/cfgx")
ok("TASKQ_CONFIG 的 ~ 展开", cfg.config_dir() == os.path.join(HOME, "cfgx"))
setenv(TASKQ_CONFIG="rel/cfg")
ok("TASKQ_CONFIG 相对路径原样（相对当前目录）", cfg.config_dir() == "rel/cfg")
setenv(TASKQ_CONFIG=os.path.join(T, "nonexistent"))
ok("TASKQ_CONFIG 不存在：照样返回，不报错", cfg.config_dir() == os.path.join(T, "nonexistent"))

# 插件文件夹
setenv(TASKQ_PLUGINS=None)
ok("TASKQ_PLUGINS 没设 → 仓库里默认的插件文件夹",
   cfg.plugin_dirs() == [os.path.join(cfg.REPO, "research", "common", "taskq_plugins")])
setenv(TASKQ_PLUGINS="")
ok("TASKQ_PLUGINS 空 → 没有插件", cfg.plugin_dirs() == [] and cfg.plugins("recipes") == [])
setenv(TASKQ_PLUGINS="~/p1::/abs/p2:rel/p3")
ok("TASKQ_PLUGINS 用 : 隔开，~ 展开，空的跳过，相对路径原样",
   cfg.plugin_dirs() == [os.path.join(HOME, "p1"), "/abs/p2", "rel/p3"])
setenv(TASKQ_PLUGINS=os.path.join(T, "nonexistent"))
ok("TASKQ_PLUGINS 不存在的文件夹：没有插件，不报错", cfg.plugins("recipes") == [])

# 调度代码在使用者仓库里的位置：嵌着（<仓库>/scripts/taskq）和单独的检出（核心就是仓库）
lay = {}
for name, sub in (("embed", "scripts/taskq/queue"), ("alone", "queue")):
    d = os.path.join(T, "layout", name, sub)
    os.makedirs(d)
    with open(os.path.join(HERE, "cfg.py")) as src, open(os.path.join(d, "cfg.py"), "w") as dst:
        dst.write(src.read())
    lay[name] = subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, sys.argv[1]); import cfg; "
                                "print(cfg.CORE_REL, cfg.rel('queue', 'taskq.py'), cfg.rel('queue'))", d],
                               capture_output=True, text=True, env={k: v for k, v in os.environ.items() if k != "TASKQ_REPO"}).stdout.split()
ok("cfg.rel：嵌着时 scripts/taskq/…，单独的检出时没有前缀（不带 ./）",
   lay["embed"] == ["scripts/taskq", "scripts/taskq/queue/taskq.py", "scripts/taskq/queue"]
   and lay["alone"] == [".", "queue/taskq.py", "queue"])

# 状态文件夹
setenv(TASKQ_STATE=None)
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
ok("outputs：配置文件夹里没有 policy.py → 默认值", outputs_in(os.path.join(T, "c_empty"), "name") == "'DEF'")


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
    ok("extend：核心没有的名字放进来", ns["helper"]() == "a" and ns["other"]() == "b" and isinstance(ns["Thing"], type))
    ok("extend：_ 开头的同名不报错，先加载的生效", ns["_private"]() == "a")
    ok("extend：同一个对象（都 from os.path import join）、同名模块（都 import json）不报错",
       ns["join"] is os.path.join and ns["json"].__name__ == "json")
    ok("extend：核心已有的常量不被插件改", ns["CONST"] == 1 and ns["core_fn"] is core_fn)

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
e = merged(pa, pe)
ok("extend：两个插件定义同名的函数 → 报错（不然后加载的不生效）", isinstance(e, ValueError) and "helper" in str(e) and "plug_a" in str(e))
pf = plugdir("f", """
    def core_fn():
        return "f"
""")
e = merged(pf)
ok("extend：插件的函数和核心的同名 → 报错", isinstance(e, ValueError) and "core_fn" in str(e) and "核心" in str(e))
pg = plugdir("g", """
    class Thing:
        pass
""")
e = merged(pa, pg)
ok("extend：两个插件定义同名的类 → 报错", isinstance(e, ValueError) and "Thing" in str(e))

setenv(TASKQ_PLUGINS=None, TASKQ_CONFIG=None)
print(f"\n{'全部通过' if not fails else f'{fails} 项失败'}")
sys.exit(1 if fails else 0)
