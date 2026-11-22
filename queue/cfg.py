"""Where config, plugins and state live: TASKQ_CONFIG (the dir holding machines.py and policy.py), TASKQ_PLUGINS (dirs holding
recipes, relay plugins and progress readers, several separated by :), TASKQ_STATE (this machine's state dir), TASKQ_ROOT (processes on
an executor: that machine's root). See "Switches and config" in queue/README.md.

    import cfg; cfg.use_config()     add the config dir to sys.path, then import policy; machine table via cfg.mtab() (sites/mtab.py)
    cfg.py get <key>                 one entry of policy.py's OUTPUTS (for bash; a tuple joins with spaces, a tuple of tuples one per line; empty when absent)
    cfg.py state [name...]           this machine's state dir (or a path in it, ~ expanded)
    cfg.py data                      this process's data dir (data_dir; empty when absent)
    cfg.py log <name>                where the log file <name>.log goes (~ expanded)
    cfg.py boot-log                  where this machine's @reboot output goes (boot_log)
    cfg.py gpu-log-dir               this machine's GPU usage records dir (gpu_log_dir)

Plugins: each TASKQ_PLUGINS dir is searched by filename and only the names below (KINDS) are taken for each kind; other names
(helpers, imports) do not enter the core; a plugin exporting none is an error. A plugin imports only taskq_api (taskq_api.py) from
the core, never other core modules.
  recipes.py         RECIPES  {recipe name: recipe} (fields at the top of recipes.py)
                     HOOKS    {hook name: function} (optional; hooks are listed in recipes.py's HOOKS)
  relay_plugins.py   PLUGINS  {plugin name: (export, check)} (see the top of relay_plugins.py)
                     ACCEPTS  {plugin name: function(params) -> can it be sent} (optional)
                     VIEW_ROOTS  [dirs to build views from] (optional)
  consumers.py       CONSUMERS  {plugin name: consumer plugin} (hooks in consume.py, "Consumers" in queue/README.md)
  smoke.py           SMOKE  {recipe name: dict(by=[param names], params={...}, vars={...})} (smoke gate, see smoke_gate.py)
  labels.py          STEPS  {step name: plain name} (pushes, progress report wording, monitor/plain.py)
                     QUIET  [steps not listed in the progress report] (optional)
                     PLAN_TITLES  function() -> {analysis number: title} (optional; a pueue label <number> or <number>_... becomes "analysis <number> <title>")
  readers/*.py       PROGRESS, INFO, TRAINED (read by spec.py; stdlib only, sent to other machines over ssh)
Merging duplicate keys is an error; a function (a name that is None in the core) may come from one plugin only.
"""
import importlib.util
import os
import sys

CORE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # the scheduler code root (when embedded in the user's repo it is <repo>/scripts/taskq)
_EMBED = os.path.join("scripts", "taskq")


def _project():
    """project.py in the same dir (loaded by file, not via sys.path, so its name cannot collide with a user module)."""
    m = sys.modules.get("taskq_project")
    if m is None:
        s = importlib.util.spec_from_file_location("taskq_project", os.path.join(os.path.dirname(os.path.abspath(__file__)), "project.py"))
        m = importlib.util.module_from_spec(s)
        s.loader.exec_module(m)
        sys.modules["taskq_project"] = m
    return m


def _user_repo():
    """The user's repo root: TASKQ_REPO; when unset, embedded as <repo>/scripts/taskq means that repo; otherwise TASKQ_PROJECT or
    the dir holding the .taskq.toml found upwards from the current dir (project.py); when none, CORE."""
    v = os.environ.get("TASKQ_REPO")
    if v:
        return os.path.abspath(os.path.expanduser(v))
    if CORE.endswith(os.sep + _EMBED):
        return CORE[:-len(_EMBED) - 1]
    return _project().find() or CORE


REPO = _user_repo()
CORE_REL = os.path.relpath(CORE, REPO)  # where the scheduler code sits in the user's repo (for commands run at the repo root)


def rel(*parts):
    """Path of a scheduler file relative to the user's repo root: scripts/taskq/... when embedded, ... when a standalone checkout
    (with .. when the core is outside the repo). Only for commands run at this machine's repo root; use bundle_rel for executors
    and bundles, and local for commands queued in pueue on the hub."""
    return os.path.normpath(os.path.join(CORE_REL, *parts))


def bundle_rel(*parts):
    """Path of a scheduler file inside a bundle (relative to the bundle root, i.e. code/current there): always proto.BUNDLE_CORE (scripts/taskq)/..."""
    if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from proto import BUNDLE_CORE
    return "/".join((BUNDLE_CORE,) + parts)


def local(*parts):
    """Absolute path of a scheduler file on this machine (for commands generated and run on the hub)."""
    return os.path.join(CORE, *parts)


# State dir: TASKQ_STATE > TASKQ_ROOT (processes on an executor: under the root, the task queue in queue/ and the rest in state/,
# proto.STATE_IN_ROOT) > policy.py's OUTPUTS["state"] (default from schema.OUTPUT_FIELDS when absent)


_PROJECT = []


def project_conf():
    """The user's repo root .taskq.toml (project.py) -> dict; no such file -> None."""
    if not _PROJECT:
        pj = _project()
        _PROJECT.append(pj.load(REPO) if os.path.isfile(os.path.join(REPO, pj.NAME)) else None)
    return _PROJECT[0]


def config_dir():
    """Config dir: TASKQ_CONFIG > repo root .taskq.toml's config > <repo>/config when there is no .taskq.toml (like example/)."""
    v = os.environ.get("TASKQ_CONFIG")
    if v:
        return os.path.expanduser(v)
    conf = project_conf()
    if conf is None:
        return os.path.join(REPO, "config")
    return os.path.normpath(os.path.join(REPO, conf.get("config", ".")))


def mtab():
    """The machine table (core sites/mtab.py, data from machines.py in the config dir)."""
    d = os.path.join(CORE, "sites")
    if d not in sys.path:
        sys.path.insert(0, d)
    import mtab as m
    return m


def executors():
    """The executor types (core sites/executors.py)."""
    d = os.path.join(CORE, "sites")
    if d not in sys.path:
        sys.path.insert(0, d)
    import executors as x
    return x


def use_config():
    d = config_dir()
    sys.path.insert(0, d)
    return d


def outputs(key):
    """policy.py's OUTPUTS[key]; when absent or config dir has no policy.py -> schema.OUTPUT_FIELDS default (unknown key -> KeyError).
    A stated config dir that is missing or a policy.py that cannot be read -> error (never silently fall back to the default state dir)."""
    from schema import OUTPUT_FIELDS
    if key not in OUTPUT_FIELDS:
        raise KeyError(f"OUTPUTS has no field {key} (have: {', '.join(OUTPUT_FIELDS)})")
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
            raise ValueError(f"config dir {d} does not exist (TASKQ_CONFIG or .taskq.toml config)") from None
        return default
    return getattr(policy, "OUTPUTS", {}).get(key, default)


def state_home():
    """The configured state dir (without expanding ~, ending in /): OUTPUTS["state"]."""
    return outputs("state").rstrip("/") + "/"


def state(*parts, expand=True):
    """A path in the state dir (see the state dir note above)."""
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
    """This process's data dir (expands ~): TASKQ_DATA (set by run_task.sh for a task's command) > data under TASKQ_ROOT (processes
    on an executor) > the store's data dir when this machine is the store; none -> ""."""
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


def _proto():
    if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import proto
    return proto


def log(name):
    """The log file <name>.log: env var <NAME>_LOG (for tests) > OUTPUTS["logs"] dir > logs/ in the state dir."""
    v = os.environ.get(name.upper() + "_LOG")
    return os.path.expanduser(v or os.path.join(outputs("logs") or state(_proto().LOGS), name + ".log"))


def bundle_root():
    """The root this core runs from a bundle of (<root>/code/<bundle>/BUNDLE_CORE, with the root's machine file, as relay
    deploy leaves it) -> its path; not run from a root's bundle -> None."""
    P = _proto()
    if not CORE.endswith("/" + P.BUNDLE_CORE):
        return None
    code = os.path.dirname(CORE[:-len(P.BUNDLE_CORE) - 1])
    r = os.path.dirname(code)
    ok = os.path.basename(code) == P.ROOT["code"] and os.path.isfile(os.path.join(r, P.ROOT["machine"]))
    return r if ok else None


def machine_state(*parts):
    """A path in this machine's own state dir, for files of the machine rather than of a process (boot log, GPU usage
    records): TASKQ_STATE / TASKQ_ROOT when set (as state()), else the state/ of the root whose bundle this core runs from
    (cron and boot start these without TASKQ_ROOT; the hostname need not match the table), else of the root the machine
    table gives this machine, else the state dir."""
    if not os.environ.get("TASKQ_STATE") and not os.environ.get("TASKQ_ROOT"):
        r = bundle_root()
        try:
            if not r:
                M = mtab()
                me = M.whoami()
                r = M.machine(me).get("root") if me else None
        except Exception:  # noqa: BLE001  (no machine table here: the state dir)
            r = None
        if r:
            return os.path.expanduser("/".join([r.rstrip("/"), _proto().ROOT["state"], *parts]))
    return state(*parts)


def boot_log():
    """This machine's @reboot output (setup.sh's crontab line; the health check reads its last line): OUTPUTS["logs"] dir >
    logs/ in this machine's own state dir (machine_state)."""
    P = _proto()
    d = outputs("logs")
    return os.path.expanduser(os.path.join(d, P.BOOT_LOG + ".log")) if d else machine_state(P.LOGS, P.BOOT_LOG + ".log")


def gpu_log_dir():
    """This machine's GPU usage records (monitor/gpu_log.sh writes, TASK_GPU_IDLE reads): GPU_LOG_DIR > GPU_LOG_SUB in
    this machine's own state dir (machine_state)."""
    return os.path.expanduser(os.environ.get("GPU_LOG_DIR") or "") or machine_state(_proto().GPU_LOG_SUB)


def env(name, default=None):
    v = os.environ.get(name)
    return v if v else default


def plugin_dirs():
    """Plugin dirs: TASKQ_PLUGINS (even empty when set) > repo root .taskq.toml's plugins > no plugins without a .taskq.toml."""
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
    "executors": ("EXECUTORS",),
}


def plugins(name):
    """<name>.py in each plugin dir -> [module]. Plugin dirs must be absolute; a stated one (TASKQ_PLUGINS, .taskq.toml) that is
    missing -> error."""
    mods = []
    named = os.environ.get("TASKQ_PLUGINS") is not None or project_conf() is not None
    for i, d in enumerate(plugin_dirs()):
        if not os.path.isabs(d):
            raise ValueError(f"a plugin dir must be an absolute path: {d}")
        if named and not os.path.isdir(d):
            raise ValueError(f"plugin dir {d} does not exist (TASKQ_PLUGINS or .taskq.toml plugins)")
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
    """Merge the tables a plugin exports into the same-named tables in ns (tables defaults to KINDS[name]): plugin dicts go first
    and duplicate keys are an error; lists are appended without duplicates. A name that is None in ns is a function: a plugin's
    value must be callable and only one plugin may supply it. Only these tables merge; other plugin names do not enter ns. A
    plugin exporting none of the tables, or a table of the wrong type, is an error."""
    tables = tuple(tables or KINDS[name])
    add = {t: ({} if isinstance(ns[t], dict) else [] if isinstance(ns[t], list) else None) for t in tables}
    for m in plugins(name):
        got = [t for t in tables if t in vars(m)]
        if not got:
            raise ValueError(f"plugin {m.__file__} exports none of {', '.join(tables)}")
        for k in got:
            v = vars(m)[k]
            if ns[k] is None:
                if not callable(v):
                    raise ValueError(f"{k} in plugin {m.__file__} must be a function")
                if add[k] is not None:
                    raise ValueError(f"{k} in plugin {m.__file__} is a duplicate (another plugin already supplies it)")
                add[k] = v
                continue
            if not isinstance(v, type(add[k])):
                raise ValueError(f"{k} in plugin {m.__file__} must be a {'dict' if isinstance(add[k], dict) else 'list'}")
            if isinstance(add[k], dict):
                dup = sorted(set(add[k]) & set(v) | set(ns[k]) & set(v))
                if dup:
                    raise ValueError(f"duplicate {', '.join(map(str, dup))} in {k} of plugin {m.__file__}")
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
    elif a == ["boot-log"]:
        print(boot_log())
    elif a == ["gpu-log-dir"]:
        print(gpu_log_dir())
    elif a[:1] == ["get"] and len(a) == 2:
        try:
            v = outputs(a[1])
        except KeyError as e:
            sys.exit(f"cfg.py get: {e.args[0]}")
        if isinstance(v, (tuple, list)):  # a tuple joins with spaces; a tuple of tuples one per line
            seq = (tuple, list)
            print(("\n" if any(isinstance(x, seq) for x in v) else " ").join(" ".join(map(str, x)) if isinstance(x, seq) else str(x) for x in v))
        else:
            print("" if v is None else v)
    else:
        sys.exit(__doc__.split("\n\n")[1])
