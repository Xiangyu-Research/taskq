#!/usr/bin/env python3
"""Tests for cfg.py: TASKQ_CONFIG / TASKQ_PLUGINS / TASKQ_STATE / TASKQ_ROOT unset, empty, missing, relative; state(), data_dir(), outputs(),
env(); cfg.py get / state / log; plugin merging (extend) duplicates. Temp files only, no machine connection.
    python3 queue/test_cfg.py
"""
import os
import subprocess
import sys
import textwrap

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import cfg as _cfg  # noqa: E402  the scheduler code root, the user's repo root
import schema  # noqa: E402
QR = _cfg.rel("queue")  # queue/ under the repo root (scripts/taskq/queue when embedded, queue in a standalone checkout)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
import cfg  # noqa: E402

T = fixture.tmpdir()
HOME = os.path.expanduser("~")
fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
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


# config dir
setenv(TASKQ_CONFIG=None)
ok("TASKQ_CONFIG unset -> the repo's config/", cfg.config_dir() == os.path.join(cfg.REPO, "config"))
setenv(TASKQ_CONFIG="")
ok("TASKQ_CONFIG empty -> the repo's config/", cfg.config_dir() == os.path.join(cfg.REPO, "config"))
setenv(TASKQ_CONFIG="~/cfgx")
ok("TASKQ_CONFIG expands ~", cfg.config_dir() == os.path.join(HOME, "cfgx"))
setenv(TASKQ_CONFIG="rel/cfg")
ok("TASKQ_CONFIG keeps a relative path as-is (relative to the current dir)", cfg.config_dir() == "rel/cfg")
setenv(TASKQ_CONFIG=os.path.join(T, "nonexistent"))
ok("TASKQ_CONFIG missing: still returned, no error", cfg.config_dir() == os.path.join(T, "nonexistent"))

# plugin dirs
setenv(TASKQ_PLUGINS=None)
ok("TASKQ_PLUGINS unset and no .taskq.toml -> no plugins", cfg.plugin_dirs() == [])
setenv(TASKQ_PLUGINS="")
ok("TASKQ_PLUGINS empty -> no plugins", cfg.plugin_dirs() == [] and cfg.plugins("recipes") == [])
setenv(TASKQ_PLUGINS="~/p1::/abs/p2:rel/p3")
ok("TASKQ_PLUGINS splits on :, expands ~, skips empty entries, keeps relative paths as-is",
   cfg.plugin_dirs() == [os.path.join(HOME, "p1"), "/abs/p2", "rel/p3"])


def plugin_error():
    try:
        cfg.plugins("recipes")
    except ValueError as e:
        return str(e)
    return None


setenv(TASKQ_PLUGINS=os.path.join(T, "nonexistent"))
ok("TASKQ_PLUGINS missing dir: error", "does not exist" in (plugin_error() or ""))
setenv(TASKQ_PLUGINS="rel/p3")
ok("TASKQ_PLUGINS relative path: error", "absolute path" in (plugin_error() or ""))
os.makedirs(os.path.join(T, "noplug"))
setenv(TASKQ_PLUGINS=os.path.join(T, "noplug"))
ok("TASKQ_PLUGINS empty dir: no plugins, no error", cfg.plugins("recipes") == [])

# where the scheduler code sits in the user's repo: embedded (<repo>/scripts/taskq) and standalone (the core is the repo)
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
ok("cfg.rel: scripts/taskq/... when embedded, no prefix standalone (no ./)",
   lay["embed"] == ["scripts/taskq", "scripts/taskq/queue/taskq.py", "scripts/taskq/queue"]
   and lay["alone"] == [".", "queue/taskq.py", "queue"])

# state dir
os.makedirs(os.path.join(T, "c_none"))
setenv(TASKQ_CONFIG=os.path.join(T, "c_none"), TASKQ_STATE=None)  # config dir without policy.py: the default state dir
ok("TASKQ_STATE unset: taskq -> ~/.local/state/taskq/taskq", cfg.state("taskq") == os.path.join(HOME, ".local/state/taskq/taskq"))
ok("TASKQ_STATE unset: a path under taskq", cfg.state("taskq", "claims", "k") == os.path.join(HOME, ".local/state/taskq/taskq/claims/k"))
ok("TASKQ_STATE unset: gpu_pad/DISABLED", cfg.state("gpu_pad/DISABLED") == os.path.join(HOME, ".local/state/taskq/gpu_pad/DISABLED"))
ok("TASKQ_STATE unset: other names under ~/.local/state/taskq/", cfg.state("tasks.json") == os.path.join(HOME, ".local/state/taskq/tasks.json"))
ok("TASKQ_STATE unset: taskqx is not under taskq", cfg.state("taskqx") == os.path.join(HOME, ".local/state/taskq/taskqx"))
ok("expand=False keeps ~ (for building remote commands)", cfg.state("taskq", expand=False) == "~/.local/state/taskq/taskq")
setenv(TASKQ_STATE="")
ok("TASKQ_STATE empty -> same as unset", cfg.state("taskq") == os.path.join(HOME, ".local/state/taskq/taskq"))
setenv(TASKQ_STATE=os.path.join(T, "st") + "/")
ok("TASKQ_STATE set: everything under it (trailing / dropped)",
   cfg.state("taskq") == os.path.join(T, "st/taskq") and cfg.state("gpu_pad/DISABLED") == os.path.join(T, "st/gpu_pad/DISABLED"))
ok("TASKQ_STATE set: no name -> itself, trailing /", cfg.state() == os.path.join(T, "st") + "/")
setenv(TASKQ_STATE="~/stx")
ok("TASKQ_STATE ~: expanded with expand, kept without",
   cfg.state("taskq") == os.path.join(HOME, "stx/taskq") and cfg.state("taskq", expand=False) == "~/stx/taskq")
setenv(TASKQ_STATE=None, TASKQ_ROOT="~/rt/")
ok("TASKQ_ROOT set (processes on an executor): taskq -> the root's queue, gpu_pad/DISABLED -> the switch under queue",
   cfg.state("taskq", "claims") == os.path.join(HOME, "rt/queue/claims")
   and cfg.state("gpu_pad/DISABLED") == os.path.join(HOME, "rt/queue/gpu_pad_DISABLED"))
ok("TASKQ_ROOT set: other names under the root's state/, taskqx is not under taskq",
   cfg.state("tasks.json") == os.path.join(HOME, "rt/state/tasks.json") and cfg.state("taskqx") == os.path.join(HOME, "rt/state/taskqx"))
setenv(TASKQ_STATE=os.path.join(T, "st"))
ok("TASKQ_STATE beats TASKQ_ROOT", cfg.state("taskq") == os.path.join(T, "st/taskq"))
setenv(TASKQ_STATE=None)

# data_dir()
setenv(TASKQ_DATA="~/dd")
ok("data_dir: TASKQ_DATA first, ~ expanded", cfg.data_dir() == os.path.join(HOME, "dd"))
setenv(TASKQ_DATA=None)
ok("data_dir: TASKQ_ROOT set -> data under the root", cfg.data_dir() == os.path.join(HOME, "rt/data"))
setenv(TASKQ_ROOT=None)

# env()
setenv(TASKQ_DATA="/new")
ok("env: set -> its value", cfg.env("TASKQ_DATA") == "/new")
setenv(TASKQ_DATA="")
ok("env: empty -> the default", cfg.env("TASKQ_DATA", "d") == "d")
setenv(TASKQ_DATA=None)
ok("env: unset -> the default", cfg.env("TASKQ_DATA", "d") == "d" and cfg.env("TASKQ_DATA") is None)


# outputs(): importing policy is cached, so run each case in its own process
def outputs_in(config, key):
    code = f"import sys; sys.path.insert(0, {HERE!r}); import cfg; print(repr(cfg.outputs({key!r})))"
    env = dict(os.environ, TASKQ_CONFIG=config)
    r = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else "ERR " + r.stderr


ok("outputs: name from the test config", outputs_in(fixture.CONFIG, "name") == "'Demo'")
ok("outputs: key not in schema -> error (lists the keys)", "ERR" in (o := outputs_in(fixture.CONFIG, "no_such_key")) and "no_such_key" in o and "state" in o)
c1 = os.path.join(T, "c_noout")
write(os.path.join(c1, "policy.py"), "AUTO = {}\n")
ok("outputs: policy.py without OUTPUTS -> schema default", outputs_in(c1, "name") == repr(schema.OUTPUT_FIELDS["name"][1]))
os.makedirs(os.path.join(T, "c_empty"))
ok("outputs: config dir without policy.py -> schema default", outputs_in(os.path.join(T, "c_empty"), "name") == repr(schema.OUTPUT_FIELDS["name"][1]))
ok("outputs: stated config dir missing -> error", "does not exist" in outputs_in(os.path.join(T, "c_missing"), "name"))
c2 = os.path.join(T, "c_badimp")
write(os.path.join(c2, "policy.py"), "import no_such_module_x\nOUTPUTS = {}\n")
ok("outputs: import in policy.py fails -> error, no default", "no_such_module_x" in outputs_in(c2, "name"))


def cfg_get(config, *args):
    env = dict(os.environ, TASKQ_CONFIG=config)
    return subprocess.run([sys.executable, os.path.join(HERE, "cfg.py"), *args], env=env, capture_output=True, text=True)


r = cfg_get(fixture.CONFIG, "get", "name")
ok("cfg.py get: a string as-is", r.returncode == 0 and r.stdout == "Demo\n")
r = cfg_get(fixture.CONFIG, "get", "code_dirs")
ok("cfg.py get: a tuple joins with spaces", r.returncode == 0 and r.stdout == f"{QR} {QR}/testdata/code\n")
r = cfg_get(fixture.CONFIG, "get", "no_such_key")
ok("cfg.py get: key not in schema -> error, non-zero exit", r.returncode != 0 and "no_such_key" in r.stderr)
r = cfg_get(c1, "get", "name")
ok("cfg.py get: no OUTPUTS -> schema default", r.returncode == 0 and r.stdout == schema.OUTPUT_FIELDS["name"][1] + "\n")
r = cfg_get(fixture.CONFIG, "get")
r2 = cfg_get(fixture.CONFIG, "set", "name")
ok("cfg.py: bad args -> usage, non-zero exit", r.returncode != 0 and r2.returncode != 0 and "cfg.py get" in r.stderr)

# OUTPUTS["state"], OUTPUTS["logs"]: each case in its own process
cs = os.path.join(T, "c_state")
write(os.path.join(cs, "policy.py"), 'OUTPUTS = dict(state="~/st_o", logs="~/lg")\n')
r = cfg_get(cs, "state", "taskq")
ok("cfg.py state: OUTPUTS[\"state\"] replaces the default", r.returncode == 0 and r.stdout == os.path.join(HOME, "st_o/taskq") + "\n")
r = cfg_get(cs, "state")
ok("cfg.py state: no name -> the dir itself, trailing /", r.returncode == 0 and r.stdout == os.path.join(HOME, "st_o") + "/\n")
r = subprocess.run([sys.executable, os.path.join(HERE, "cfg.py"), "state", "taskq"], capture_output=True, text=True,
                   env=dict(os.environ, TASKQ_CONFIG=cs, TASKQ_STATE=os.path.join(T, "st2")))
ok("cfg.py state: TASKQ_STATE beats OUTPUTS[\"state\"]", r.stdout == os.path.join(T, "st2/taskq") + "\n")
r = cfg_get(c1, "state", "taskq")
ok("cfg.py state: no OUTPUTS[\"state\"] -> the core default", r.stdout == os.path.join(HOME, ".local/state/taskq/taskq") + "\n")
r = cfg_get(cs, "log", "feed")
ok("cfg.py log: <name>.log under OUTPUTS[\"logs\"]", r.returncode == 0 and r.stdout == os.path.join(HOME, "lg/feed.log") + "\n")
r = cfg_get(c1, "log", "feed")
ok("cfg.py log: no OUTPUTS[\"logs\"] -> ~/<name>.log", r.stdout == os.path.join(HOME, "feed.log") + "\n")
r = subprocess.run([sys.executable, os.path.join(HERE, "cfg.py"), "log", "feed"], capture_output=True, text=True,
                   env=dict(os.environ, TASKQ_CONFIG=cs, FEED_LOG="~/x.log"))
ok("cfg.py log: <NAME>_LOG wins", r.stdout == os.path.join(HOME, "x.log") + "\n")
write(os.path.join(T, "c_cron", "policy.py"), 'OUTPUTS = dict(cron=(("5 * * * *", "a"), ("@daily", "b")), code_dirs=("x", "y"))\n')
r, r2 = cfg_get(os.path.join(T, "c_cron"), "get", "cron"), cfg_get(os.path.join(T, "c_cron"), "get", "code_dirs")
ok("cfg.py get: a tuple of tuples one per line, a flat tuple still joins with spaces", r.stdout == "5 * * * * a\n@daily b\n" and r2.stdout == "x y\n")


# plugin merging
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
ok("extend: merging two plugin dirs does not error", isinstance(ns, dict))
if isinstance(ns, dict):
    ok("extend: plugin dicts first, core last, all present", list(ns["TABLE"]) == ["pa", "pb", "core"])
    ok("extend: lists append without duplicates", ns["LIST"] == ["a", "b", "c"])
    ok("extend: only agreed tables merge; other plugin names (functions, classes, imported modules, constants) do not",
       not {"helper", "other", "Thing", "_private", "json", "join"} & set(ns))
    ok("extend: existing core constants and functions are not changed by plugins", ns["CONST"] == 1 and ns["core_fn"] is core_fn)

pc = plugdir("c", """
    TABLE = {"pa": 9}
""")
e = merged(pa, pc)
ok("extend: the same key in two plugins' tables -> error", isinstance(e, ValueError) and "pa" in str(e))
pd = plugdir("d", """
    TABLE = {"core": 9}
""")
e = merged(pd)
ok("extend: the same key in a plugin and the core table -> error", isinstance(e, ValueError) and "core" in str(e))
pe = plugdir("e", """
    def helper():
        return "e"
""")
e = merged(pe)
ok("extend: a plugin exporting none of the agreed tables -> error (usually a wrong dir or table name)", isinstance(e, ValueError) and "exports none" in str(e))
pf = plugdir("f", """
    TABLE = [1]
""")
e = merged(pf)
ok("extend: a table of the wrong type -> error", isinstance(e, ValueError) and "TABLE" in str(e) and "dict" in str(e))


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
ok("extend: a name that is None in the core (a function) with no plugin -> stays None", isinstance(ns, dict) and ns["FN"] is None and ns["TABLE"] == {"h": 1})
ns = merged_fn(pg, ph)
ok("extend: a name that is None in the core -> uses the plugin's function", isinstance(ns, dict) and ns["FN"]() == "g")
e = merged_fn(pg, plugdir("i", """
    def FN():
        return "i"
"""))
ok("extend: two plugins both supply the same function -> error", isinstance(e, ValueError) and "FN" in str(e))
e = merged_fn(plugdir("j", """
    FN = {"x": 1}
"""))
ok("extend: a function given as something else -> error", isinstance(e, ValueError) and "FN" in str(e) and "function" in str(e))
# names exported by each plugin kind: cfg.KINDS matches the core modules and test plugins
ok("KINDS: recipes exports RECIPES, HOOKS; relay_plugins exports PLUGINS, ACCEPTS, VIEW_ROOTS; consumers exports CONSUMERS; "
   "smoke exports SMOKE; labels exports STEPS, QUIET, PLAN_TITLES",
   cfg.KINDS == {"recipes": ("RECIPES", "HOOKS"), "relay_plugins": ("PLUGINS", "ACCEPTS", "VIEW_ROOTS"),
                 "consumers": ("CONSUMERS",), "smoke": ("SMOKE",), "labels": ("STEPS", "QUIET", "PLAN_TITLES")})
setenv(TASKQ_PLUGINS=fixture.PLUGINS)
r = subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, sys.argv[1]); import recipes, relay_plugins; "
                    "print(sorted(recipes.RECIPES), sorted(recipes.HOOKS), sorted(relay_plugins.PLUGINS), "
                    "hasattr(recipes, 'model_of'), hasattr(relay_plugins, 'infer_inputs'), "
                    "relay_plugins.RelayError is __import__('taskq_api').RelayError)", HERE],
                   capture_output=True, text=True, env=dict(os.environ))
ok("test plugins: recipes, hooks and relay plugins merge; plugin functions do not enter the core modules; RelayError is taskq_api's",
   r.stdout.split() == ["['command',", "'toy']", "['model_of']", "['toy',", "'toy_infer']", "False", "False", "True"])

# project lookup (project.py): --project (passed to child processes via TASKQ_PROJECT) > find .taskq.toml upwards from the current dir; cfg's repo root: TASKQ_REPO > embedded > found project > CORE
import project  # noqa: E402
PJ = os.path.join(T, "proj")
write(os.path.join(PJ, ".taskq.toml"), """
    # test project
    config = "conf"
    plugins = ["p1", "p/two"]  # two
""")
os.makedirs(os.path.join(PJ, "a", "b"))
setenv(TASKQ_PROJECT=None, TASKQ_REPO=None, TASKQ_CONFIG=None, TASKQ_PLUGINS=None)
ok("project.find: finds .taskq.toml upwards from a subdir", project.find(start=os.path.join(PJ, "a", "b")) == PJ)
ok("project.find: nothing above -> None", project.find(start=os.path.join(T, "layout")) is None)
ok("project.find: an explicit dir wins (need not hold .taskq.toml)", project.find(os.path.join(T, "other"), start=PJ) == os.path.join(T, "other"))
setenv(TASKQ_PROJECT="~/pj")
ok("project.find: TASKQ_PROJECT beats searching upwards, ~ expanded", project.find(start=PJ) == os.path.join(HOME, "pj"))
ok("project.find: an explicit dir beats TASKQ_PROJECT", project.find(PJ) == PJ)
setenv(TASKQ_PROJECT=None)
ok("project.load: reads config, plugins", project.load(PJ) == {"config": "conf", "plugins": ["p1", "p/two"]})
ok("project.load: no .taskq.toml -> {}", project.load(T) == {})
ok("project.parse (without tomllib): strings, lists, empty lists, comments, blank lines",
   project.parse('# c\n\nconfig = "envs"  # comment\nplugins = ["a", "b/c"]\nx = []\ny = ["#not a comment"]\n')
   == {"config": "envs", "plugins": ["a", "b/c"], "x": [], "y": ["#not a comment"]})
try:
    project.parse("[table]\n")
    ok("project.parse: an unparsable line is an error", False)
except ValueError as e:
    ok("project.parse: an unparsable line is an error (names the line)", "line 1" in str(e))
ok("project.env_of: TASKQ_REPO, TASKQ_CONFIG, TASKQ_PLUGINS (: separated)",
   project.env_of(PJ) == {"TASKQ_REPO": PJ, "TASKQ_CONFIG": os.path.join(PJ, "conf"),
                          "TASKQ_PLUGINS": os.path.join(PJ, "p1") + ":" + os.path.join(PJ, "p/two")})
write(os.path.join(T, "proj_s", ".taskq.toml"), 'plugins = "one"\n')
ok("project.env_of: plugins may be one string; no config means no TASKQ_CONFIG",
   project.env_of(os.path.join(T, "proj_s")) == {"TASKQ_REPO": os.path.join(T, "proj_s"), "TASKQ_PLUGINS": os.path.join(T, "proj_s", "one")})
setenv(TASKQ_CONFIG="/keep")
new = project.export(PJ)
ok("project.export: already set vars are unchanged, the rest are set", os.environ["TASKQ_CONFIG"] == "/keep" and os.environ["TASKQ_REPO"] == PJ
   and "TASKQ_CONFIG" not in new and "TASKQ_PLUGINS" in new)
setenv(TASKQ_CONFIG=None, TASKQ_REPO=None, TASKQ_PLUGINS=None)


def proj_sh(cwd, *args, **env):
    e = {k: v for k, v in os.environ.items() if not k.startswith("TASKQ_")}
    e.update(env)
    return subprocess.run([sys.executable, os.path.join(HERE, "project.py"), "sh", *args], cwd=cwd, env=e, capture_output=True, text=True)


r = proj_sh(os.path.join(PJ, "a"))
ok("project.py sh: finds the project upwards, prints exports", r.returncode == 0 and f"export TASKQ_REPO={PJ}\n" in r.stdout
   and f"export TASKQ_CONFIG={PJ}/conf\n" in r.stdout and r.stdout.count("export ") == 3)
r = proj_sh(os.path.join(PJ, "a"), TASKQ_CONFIG="/x")
ok("project.py sh: does not print ones already set", r.returncode == 0 and "TASKQ_CONFIG" not in r.stdout and "TASKQ_REPO" in r.stdout)
r = proj_sh(T)
ok("project.py sh: not found -> prints nothing, exit 1", r.returncode == 1 and r.stdout == "" and ".taskq.toml" in r.stderr)
r = proj_sh(T, "--project", PJ)
ok("project.py sh --project", r.returncode == 0 and f"export TASKQ_REPO={PJ}\n" in r.stdout)
r = subprocess.run([sys.executable, os.path.join(HERE, "project.py")], capture_output=True, text=True)
ok("project.py: bad args -> usage, exit 2", r.returncode == 2 and "project.py sh" in r.stderr)


def cfg_in(cwd, code, layout="alone", **env):
    """Import cfg in a copied core (alone: standalone checkout; embed: <repo>/scripts/taskq) and run code."""
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
ok("cfg: standalone checkout, .taskq.toml above the current dir -> that project's config and plugins",
   cfg_in(os.path.join(PJ, "a"), "(cfg.REPO, cfg.config_dir(), cfg.plugin_dirs())")
   == repr((PJ, os.path.join(PJ, "conf"), [os.path.join(PJ, "p1"), os.path.join(PJ, "p/two")])))
ok("cfg: TASKQ_PROJECT is honoured too", cfg_in(T, "cfg.REPO", TASKQ_PROJECT=PJ) == repr(PJ))
ok("cfg: TASKQ_REPO beats searching upwards", cfg_in(os.path.join(PJ, "a"), "cfg.REPO", TASKQ_REPO=T) == repr(T))
ok("cfg: TASKQ_CONFIG and TASKQ_PLUGINS beat .taskq.toml",
   cfg_in(PJ, "(cfg.config_dir(), cfg.plugin_dirs())", TASKQ_CONFIG="/c", TASKQ_PLUGINS="/p") == repr(("/c", ["/p"])))
ok("cfg: no project found -> the core itself; without .taskq.toml config is in config/ and there are no plugins",
   cfg_in(T, "(cfg.REPO, cfg.config_dir(), cfg.plugin_dirs())")
   == repr((ALONE, os.path.join(ALONE, "config"), [])))
write(os.path.join(T, "proj_e", ".taskq.toml"), "")
ok("cfg: .taskq.toml without config, plugins -> the project root, no plugins",
   cfg_in(os.path.join(T, "proj_e"), "(cfg.config_dir(), cfg.plugin_dirs())") == repr((os.path.join(T, "proj_e"), [])))
EMB = os.path.join(T, "disc_embed")
write(os.path.join(EMB, ".taskq.toml"), 'config = "cfgdir"\n')
ok("cfg: embedded in <repo>/scripts/taskq means that repo (no searching upwards); the repo root .taskq.toml is still read",
   cfg_in(os.path.join(PJ, "a"), "(cfg.REPO, cfg.config_dir())", layout="embed") == repr((EMB, os.path.join(EMB, "cfgdir"))))
os.remove(os.path.join(EMB, ".taskq.toml"))
ok("cfg: embedded, no .taskq.toml at the repo root -> <repo>/config",
   cfg_in(os.path.join(PJ, "a"), "(cfg.REPO, cfg.config_dir())", layout="embed") == repr((EMB, os.path.join(EMB, "config"))))

# pueue-add: when the caller is a temporary worktree, the task's paths move under the working dir; paths outside the repo stay
_wt, _snap = os.path.join(T, "wt"), os.path.join(T, "snap")
os.makedirs(os.path.join(_wt, "envs"), exist_ok=True)
os.symlink(_wt, os.path.join(T, "wt_link"))
_env = {"TASKQ_REPO": _wt, "TASKQ_CONFIG": os.path.join(_wt, "envs"), "TASKQ_DATA": os.path.join(_wt, "data"),
        "TASKQ_PLUGINS": os.pathsep.join([os.path.join(_wt, "p1"), "/elsewhere/p2"])}
ok("project.rebase: under the old root -> under the new one, each :-separated part; outside the repo and other variables unchanged",
   project.rebase(os.path.join(T, "wt_link"), _snap, _env) == {
       "TASKQ_REPO": _snap, "TASKQ_CONFIG": os.path.join(_snap, "envs"),
       "TASKQ_PLUGINS": os.pathsep.join([os.path.join(_snap, "p1"), "/elsewhere/p2"])})
ok("project.rebase: same root -> nothing changed", project.rebase(_wt, _wt, _env) == {})
ok("project.rebase: a sibling dir with the same prefix is not under the old root", project.rebase(_wt, _snap, {"TASKQ_CONFIG": _wt + "2/envs"}) == {})
r = subprocess.run([sys.executable, os.path.join(HERE, "project.py"), "rebase", _wt, _snap],
                   env=dict({k: v for k, v in os.environ.items() if not k.startswith("TASKQ_")}, **_env), capture_output=True, text=True)
ok("project.py rebase: prints export for the changed ones", r.returncode == 0 and f"export TASKQ_CONFIG={_snap}/envs\n" in r.stdout and "TASKQ_DATA" not in r.stdout)

setenv(TASKQ_PLUGINS=None, TASKQ_CONFIG=None)
print(f"\n{'all passed' if not fails else f'{fails} failed'}")
sys.exit(1 if fails else 0)
