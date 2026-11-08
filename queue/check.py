#!/usr/bin/env python3
"""Check a user's config: machine table, policy, plugins, experiment table (fields against schema.py's tables), order (dependencies),
that every task and step turns into a command to run, that a step's inputs_cmd lists its inputs, that raw data is on the store and
that an executor bundle can be built. Changes nothing; only the raw group connects to the store (read-only).

    python3 queue/check.py [--quick] [--only group...]

  --quick  skip inputs, raw and bundle (the slowest groups, which need the store)
  --only   only these groups (machines policy plugins experiments flow tasks steps inputs raw bundle)
Exit code 0 = no problems, 1 = problems. Problems (and notes) go to stderr, each group's result to stdout.
"""
import argparse
import ast
import datetime as dt
import fnmatch
import glob
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import traceback
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
for d in ("sites", "monitor", "sync"):
    sys.path.insert(0, os.path.join(CORE, d))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto as PR  # noqa: E402
cfg.use_config()
# Import core modules first: a project's experiment table may add plugin dirs to sys.path with same-named recipes.py or relay_plugins.py
try:  # wrong or duplicate plugin tables raise on import; report them as a plugins problem
    import recipes  # noqa: E402
    import relay_plugins  # noqa: E402
    assert relay_plugins
    import smoke_gate as SG  # noqa: E402
    PLUGIN_ERR = None
except Exception as e:  # noqa: BLE001
    PLUGIN_ERR = e
import schema  # noqa: E402

GROUPS = ("machines", "policy", "plugins", "experiments", "flow", "tasks", "steps", "inputs", "raw", "bundle")
SLOW = ("inputs", "raw", "bundle")
NEED_EXPS = ("flow", "tasks", "steps", "inputs", "raw")


class Report:
    def __init__(self):
        self.problems, self.notes, self.counts = [], [], {}

    def bad(self, group, msg):
        self.problems.append((group, msg))

    def note(self, group, msg):
        self.notes.append((group, msg))

    def checked(self, group, n=1):
        self.counts[group] = self.counts.get(group, 0) + n


# ---------------- field types (the type names in schema.py's tables) ----------------
def _tup(v, n=None):
    return isinstance(v, (tuple, list)) and (n is None or len(v) == n)


def _num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _hhmm(x):
    return isinstance(x, str) and re.fullmatch(r"\d\d:\d\d", x)


def _day(x):
    return x is None or isinstance(x, str) and re.fullmatch(r"\d{4}-\d\d-\d\d", x)


def _regex(v):
    try:
        re.compile(v)
        return isinstance(v, str)
    except (re.error, TypeError):
        return False


def _strs(v):
    return isinstance(v, str) or _tup(v) and all(isinstance(x, str) for x in v)


def _when(x):
    try:
        dt.datetime.strptime(x, "%Y-%m-%d %H:%M")
        return True
    except (TypeError, ValueError):
        return False


TYPES = {  # type name -> (check, how to write it)
    "str": (lambda v: isinstance(v, str), "a string"),
    "str?": (lambda v: v is None or isinstance(v, str), "a string or None"),
    "strs": (_strs, "a string or a list of strings"),
    "strs?": (lambda v: v is None or _strs(v), "a string, a list of strings or None"),
    "int": (lambda v: isinstance(v, int) and not isinstance(v, bool), "an integer"),
    "int?": (lambda v: v is None or isinstance(v, int) and not isinstance(v, bool), "an integer or None"),
    "num?": (lambda v: v is None or _num(v), "a number or None"),
    "bool": (lambda v: isinstance(v, bool), "True / False"),
    "dict": (lambda v: isinstance(v, dict), "a dict"),
    "dict?": (lambda v: v is None or isinstance(v, dict), "a dict or None"),
    "regex": (_regex, "a regex"),
    "pair": (lambda v: v is None or _tup(v, 2), "a 2-tuple"),
    "pair?": (lambda v: v is None or _tup(v, 2), "a 2-tuple or None"),
    "triple?": (lambda v: v is None or _tup(v, 3) and all(_num(x) for x in v), "a 3-number tuple or None"),
    "disk?": (lambda v: v is None or _tup(v, 3) and isinstance(v[0], str) and _num(v[1]) and (v[2] is None or _num(v[2])),
              "(dir, GB, GB or None) or None"),
    "tunnel": (lambda v: _tup(v) and set(v) <= {"push", "pull"}, "a tuple of push, pull"),
    "roles": (lambda v: _tup(v) and set(v) <= {"hub", "store", "backup"}, "a tuple of hub, store, backup"),
    "sched?": (lambda v: v in (None, "pueue", "slurm"), "pueue, slurm or None"),
    "device": (lambda v: v in ("gpu", "cpu"), "gpu or cpu"),
    "strmap": (lambda v: isinstance(v, dict) and all(isinstance(k, str) and isinstance(x, str) for k, x in v.items()), "{string: string}"),
    "intmap": (lambda v: isinstance(v, dict) and all(isinstance(x, int) and x >= 0 for x in v.values()), "{machine: integer >= 0}"),
    "groups": (lambda v: isinstance(v, dict) and all(_tup(g, 3) and isinstance(g[0], int) and (g[1] is None or isinstance(g[1], str))
                                                     and isinstance(g[2], dict) for g in v.values()),
               "{group: (how many at once, GPU or None, {env vars})}"),
    "envs": (lambda v: isinstance(v, dict), "{env name: definition}"),
    "jobs": (lambda v: isinstance(v, dict) and set(v) <= {"gpu", "cpu"} and all(isinstance(o, dict) for o in v.values()),
             "{gpu|cpu: dict(partition, gres, cpus, mem)}"),
    "workers": (lambda v: _tup(v) and all(isinstance(w, dict) for w in v), "list of workers (each a dict, see WORKER_FIELDS)"),
    "hours": (lambda v: _tup(v) and all(_tup(p, 4) and _hhmm(p[0]) and _hhmm(p[1]) and _day(p[2]) and _day(p[3]) for p in v),
              "[(\"HH:MM\", \"HH:MM\", first day \"YYYY-MM-DD\" or None, last day or None)]"),
    "steps": (lambda v: _tup(v) and all(isinstance(s, dict) for s in v), "list of steps (dicts)"),
    "auto": (lambda v: isinstance(v, dict), "a dict"),
    "pause": (lambda v: isinstance(v, dict) and all(_tup(x, 2) and (x[0] is None or _when(x[0])) and isinstance(x[1], str) for x in v.values()),
              "{machine: (\"YYYY-MM-DD HH:MM\" or None, reason)}"),
    "place": (lambda v: isinstance(v, dict) and all(x == "any" or _tup(x) and x and all(isinstance(n, str) for n in x) for x in v.values()),
              "{step or recipe: [executor, ...] or \"any\"}"),
    "slurm": (lambda v: isinstance(v, dict), "{machine: dict(gpu=..., cpu=...)}"),
    "cron": (lambda v: _tup(v) and all(_tup(x, 2) and all(isinstance(y, str) for y in x) for x in v), "((time, subcommand), ...)"),
    "outputs": (lambda v: isinstance(v, dict), "a dict (see OUTPUT_FIELDS)"),
}
assert {t for tab in (schema.MACHINE_FIELDS, schema.WORKER_FIELDS, schema.POLICY_FIELDS, schema.OUTPUT_FIELDS,
                      schema.EXP_FIELDS, schema.STEP_FIELDS, schema.RECIPE_FIELDS) for t, _, _ in tab.values()} <= set(TYPES)


def fields(r, g, where, d, table):
    """The fields of d: unknown, missing required and wrongly typed ones are all reported."""
    r.checked(g)
    for k in sorted(set(d) - set(table)):
        r.bad(g, f"{where}: unknown field {k} (taskq machines fields shows what may be written)")
    for k, (t, df, _) in table.items():
        if k not in d:
            if df is schema.REQUIRED:
                r.bad(g, f"{where}: missing required field {k}")
            continue
        ok, how = TYPES[t]
        if not ok(d[k]):
            r.bad(g, f"{where}: {k} must be {how}, got {d[k]!r}")


# ---------------- machine table ----------------
def check_machines(r):
    M = cfg.mtab()
    import slurm as SL
    g = "machines"
    for n, m in M.MACHINES.items():
        where = f"MACHINES[{n!r}]"
        fields(r, g, where, m, schema.MACHINE_FIELDS)
        for i, w in enumerate(m.get("workers") or ()):
            if isinstance(w, dict):
                fields(r, g, f"workers[{i}] of {where}", w, schema.WORKER_FIELDS)
    for role in ("hub", "store"):
        got = [n for n in M.MACHINES if role in (M.MACHINES[n].get("roles") or ())]
        if len(got) != 1:
            r.bad(g, f"exactly one machine must have role {role}, now {got or 'none'}")
    for n in M.names("backup"):
        if not M.MACHINES[n].get("backup"):
            r.bad(g, f"MACHINES[{n!r}] is backup but has no backup set (where backups go)")
    try:
        st, hub = M.store(), M.hub()
    except LookupError:
        st = hub = None
    tunneled = [n for n in M.executors() if M.MACHINES[n].get("tunnel")]
    locked = [n for n in M.executors() if any(M.is_spec(ev) for ev in (M.MACHINES[n].get("envs") or {}).values())]
    if hub and M.MACHINES[hub].get("ssh") is not None:
        r.bad(g, f"hub ({hub}) has ssh None: the hub is the machine that runs the scheduler")
    if st:
        sm = M.MACHINES[st]
        for k, why in (("data", "data dir (home of results)"), ("root", "where site_ops bundles, the hashes cache and temp files go")):
            if not sm.get(k):
                r.bad(g, f"store ({st}) needs {k}: {why}")
        if tunneled and not sm.get("transfer"):
            r.bad(g, f"store ({st}) needs transfer: {', '.join(tunneled)} transfer files through the tunnel and the file service runs on the store")
        local = [n for n in M.executors() if M.MACHINES[n].get("ssh") is None]
        if local and sm.get("ssh") is not None:
            r.bad(g, f"{', '.join(local)} share the hub's machine (ssh is None); store ({st}) on another machine cannot reach it, so the store must also be here")
    if hub and locked:
        hm = M.MACHINES[hub]
        for k in ("root", "transfer") if any(M.MACHINES[n].get("tunnel") for n in locked) else ("root",):
            if not hm.get(k):
                r.bad(g, f"hub ({hub}) needs {k}: {', '.join(locked)} use locked envs, bundles are in the hub's wheelhouse and sent by the hub")
    for n, m in M.MACHINES.items():
        where = f"MACHINES[{n!r}]"
        if m.get("scheduler") and not m.get("root"):
            r.bad(g, f"{where}: an executor (scheduler is set) needs root")
        if m.get("root") and not m.get("scheduler") and n not in (st, hub):
            r.bad(g, f"{where}: root is set but scheduler is not (only the store and hub may have just a root)")
        if m.get("scheduler") == "pueue" and not m.get("workers"):
            r.bad(g, f"{where}: a pueue executor needs workers")
        if m.get("scheduler") == "slurm":
            if not m.get("jobs"):
                r.bad(g, f"{where}: a slurm executor needs jobs")
            if m.get("workers"):
                r.bad(g, f"{where}: a slurm executor does not write workers (it submits jobs from jobs)")
            if m.get("job_names") is not None and m["job_names"] not in SL.STYLES:
                r.bad(g, f"{where}: job_names {m['job_names']!r} is not in slurm.STYLES ({sorted(SL.STYLES)})")
        if m.get("scheduler") != "pueue" and m.get("workers"):
            r.bad(g, f"{where}: only a pueue executor writes workers")
        if M.is_exec(n):
            ws = M.workers(n)
            ids = [w["name"] for w in ws]
            if len(set(ids)) != len(ids):
                r.bad(g, f"{where}: duplicate worker names {ids} (one GPU written twice)")
            clash = sorted(set(m.get("manual_groups") or {}) & set(ids))
            if clash:
                r.bad(g, f"{where}: manual_groups and worker groups have the same names: {clash}")
            b = (m.get("budget") or (100, 50))[1]
            for cap in ("cap", "blob_cap"):
                if m.get(cap) and b >= m[cap]:
                    r.bad(g, f"{where}: budget's second item {b} GB is not below {cap} {m[cap]} GB, nothing fits")
        for en, ev in (m.get("envs") or {}).items():
            if M.is_spec(ev):
                if M.spec_hash(en) is None:
                    r.bad(g, f"env {en} in {where} is a spec but has no description {en}.yml")
                elif not os.path.isfile(M.lock_path(en, M.spec_variant(n, en))):
                    r.note(g, f"env {en} in {where} has no lock {os.path.basename(M.lock_path(en, M.spec_variant(n, en)))} yet"
                              f" (taskq env lock {en} --variant {M.spec_variant(n, en)})")
            elif not (_tup(ev) and ev and ev[0] in ("conda", "venv")):
                r.bad(g, f"env {en} in {where} must be (\"conda\", name), (\"venv\", dir, base) or \"spec\", got {ev!r}")
            elif ev[0] == "venv" and (len(ev) != 3 or ev[2] not in m["envs"]):
                r.bad(g, f"env {en} in {where}: venv base {ev[2] if len(ev) > 2 else None!r} is not among this machine's envs")
            elif ev[0] == "conda" and not m.get("conda"):
                r.bad(g, f"env {en} in {where} is conda but this machine has no conda=(dir, env)")
        if m.get("quick") and _tup(m["quick"], 2) and not _regex(m["quick"][1]):
            r.bad(g, f"{where}: quick's second item is not a regex")
    local = [n for n in M.executors() if M.MACHINES[n].get("ssh") is None]
    if len(local) > 1:
        r.bad(g, f"at most one executor may have ssh=None (same machine as the hub), now {local}")
    if st and M.MACHINES[st].get("data"):
        real = lambda p: os.path.realpath(os.path.expanduser(p))  # noqa: E731
        for n in M.executors():
            if n != st and M.MACHINES[n].get("ssh") is None and M.MACHINES[st].get("ssh") is None \
                    and (real(M.MACHINES[n]["root"]) + "/").startswith(real(M.MACHINES[st]["data"]).rstrip("/") + "/"):
                r.bad(g, f"root of MACHINES[{n!r}] is inside the store's data dir: an executor's data must be separate from the home of results")
    check_specs(r, M)


PLACEHOLDERS = {"torch_mm", "abi", "cp"}  # placeholders allowed in pip packages of an env description (sites/envs.py pip_lines)


def check_specs(r, M):
    """<config dir>/specs/*.yml: readable, with a compilable <name>.check.py, only known placeholders in pip, and every torch variant
    can generate an env build script on a fake executor. Counted in the machines group."""
    g = "machines"
    try:
        import envs as EV
    except ImportError as e:
        r.note(g, f"cannot read env descriptions ({e}), not checking specs/")
        return
    names = sorted(os.path.basename(p)[:-4] for p in glob.glob(os.path.join(M.SPECS, "*.yml")))
    check_locks(r, M, names)
    fake = "_check_specs"
    for n in names:
        r.checked(g)
        try:
            sp = EV.load(n)
            p = EV.check_file(n)
            compile(open(p, encoding="utf-8").read(), p, "exec")
        except ImportError as e:
            r.note(g, f"cannot read env descriptions ({e}), not checking specs/")
            return
        except Exception as e:  # noqa: BLE001  EnvError, SyntaxError, yaml errors
            r.bad(g, f"env description {n}: {type(e).__name__}: {e}")
            continue
        odd = sorted({x for pkg in (sp.get("pip") or []) + (sp.get("pip_no_deps") or []) for x in re.findall(r"\{(\w+)\}", str(pkg))}
                     - PLACEHOLDERS)
        if odd:
            r.bad(g, f"env description {n}.yml has unknown pip placeholders {odd} (allowed: {sorted(PLACEHOLDERS)})")
        for v in list(sp.get("torch") or {}) or ["default"]:
            envs = {x: "spec" for x in names}
            envs[n] = ("spec", v)
            M.MACHINES[fake] = dict(label=fake, hostname=r"\A\Z", ssh="-", scheduler="pueue", root="/check/root",
                                    conda=("/check/conda", "base"), envs=envs, workers=[dict(cpus=1)])
            try:
                EV.build_script(fake, n, store="/check/root/blobs")
            except Exception as e:  # noqa: BLE001
                r.bad(g, f"env description {n} (torch variant {v}) cannot generate an env build script: {type(e).__name__}: {e}")
            finally:
                M.MACHINES.pop(fake, None)


def check_locks(r, M, names):
    """<name>.<variant>.lock.json: the description exists and the recorded spec_sha256 is the current one (relock after a change)."""
    g = "machines"
    for p in sorted(glob.glob(os.path.join(M.SPECS, "*" + PR.LOCK_SUFFIX))):
        r.checked(g)
        f = os.path.basename(p)
        try:
            d = json.load(open(p, encoding="utf-8"))
        except (OSError, ValueError) as e:
            r.bad(g, f"lock {f} is unreadable: {e}")
            continue
        n, v = d.get("name"), d.get("variant")
        if f != f"{n}.{v}{PR.LOCK_SUFFIX}" or n not in names:
            r.bad(g, f"lock {f} has name / variant ({n} / {v}) that do not match the filename, or has no description {n}.yml")
            continue
        if d.get("spec_sha256") != hashlib.sha256(open(os.path.join(M.SPECS, f"{n}.yml"), "rb").read()).hexdigest():
            r.bad(g, f"lock {f} is from an older description ({n}.yml changed after locking); relock: taskq env lock {n} --variant {v}")


# ---------------- policy ----------------
def check_policy(r):
    M = cfg.mtab()
    import policy as P
    g = "policy"
    mine = {k: getattr(P, k) for k in dir(P) if k.isupper() and not k.startswith("_") and not callable(getattr(P, k))
            and not isinstance(getattr(P, k), type(sys))}
    extra = sorted(k for k in set(mine) - set(schema.POLICY_FIELDS) if not isinstance(mine[k], (str, int, float, tuple)))
    for k in extra:  # only report uppercase names shaped like fields: constants such as strings or numbers (QR and the like) are config-local
        r.bad(g, f"policy.py {k} is not a policy field (may write: {', '.join(schema.POLICY_FIELDS)})")
    fields(r, g, "policy.py", {k: v for k, v in mine.items() if k in schema.POLICY_FIELDS}, schema.POLICY_FIELDS)
    every, ex = set(M.MACHINES), set(M.executors())
    auto = getattr(P, "AUTO", {}) or {}
    for k, v in auto.items():
        if k not in PR.AUTO_SWITCHES:
            r.bad(g, f"AUTO[{k!r}] is not a switch (may write: {', '.join(PR.AUTO_SWITCHES)}; pause a machine with PAUSE)")
        elif not isinstance(v, bool) and not (k == "envs" and isinstance(v, dict)):
            r.bad(g, f"AUTO[{k!r}] must be True / False (envs may also be {{machine: [env or (env, variant)]}})")
    for mn, lst in (auto.get("envs") if isinstance(auto.get("envs"), dict) else {}).items():
        if mn not in ex:
            r.bad(g, f"AUTO['envs'] has {mn!r}, not an executor")
        for x in lst if isinstance(lst, (list, tuple)) else [None]:
            e = x if isinstance(x, str) else x[0] if isinstance(x, (list, tuple)) and 1 <= len(x) <= 2 else None
            if e is None or M.spec_hash(e) is None:
                r.bad(g, f"AUTO['envs'][{mn!r}] entry {x!r} must be an env name with a description or (env, variant)")
    for name, ok in (("PAUSE", every), ("READY", ex), ("SLURM", set(M.names("slurm")))):
        bad = sorted(set(getattr(P, name, {}) or {}) - ok)
        if bad:
            r.bad(g, f"{name} has {bad}, not {'machines' if name == 'PAUSE' else 'executors' if name == 'READY' else 'slurm executors'}")
    for k, v in (getattr(P, "PLACE", {}) or {}).items():
        bad = [] if v == "any" else [n for n in v if n not in ex]
        if bad:
            r.bad(g, f"PLACE[{k!r}] has {bad}, not executors (write executor names, or \"any\")")
    out = getattr(P, "OUTPUTS", None)
    if isinstance(out, dict):
        fields(r, g, "policy.py OUTPUTS", out, schema.OUTPUT_FIELDS)


# ---------------- plugins ----------------
def core_modules():
    return {os.path.splitext(os.path.basename(p))[0] for d in ("queue", "sites", "monitor", "sync")
            for p in glob.glob(os.path.join(CORE, d, "*.py"))}


def check_labels(r):
    """The labels plugin (push and progress report wording): table types, PLAN_TITLES() runs and returns {number: title}."""
    g = "plugins"
    ns = {"STEPS": {}, "QUIET": [], "PLAN_TITLES": None}
    try:
        cfg.extend(ns, "labels")
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"merging the labels plugin into the core failed: {e}")
        return
    r.checked(g)
    bad = [k for k, v in ns["STEPS"].items() if not (isinstance(k, str) and isinstance(v, str) and v.strip())]
    if bad:
        r.bad(g, f"STEPS in the labels plugin has {bad} with a plain name that is not a non-empty string")
    bad = [x for x in ns["QUIET"] if not isinstance(x, str)]
    if bad:
        r.bad(g, f"QUIET in the labels plugin has {bad}, which are not step names (strings)")
    if ns["PLAN_TITLES"]:
        try:
            got = ns["PLAN_TITLES"]()
        except Exception as e:  # noqa: BLE001
            r.bad(g, f"PLAN_TITLES() in the labels plugin failed: {type(e).__name__}: {e}")
            return
        if not isinstance(got, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in got.items()):
            r.bad(g, f"PLAN_TITLES() in the labels plugin must return {{number: title}} (all strings), got {type(got).__name__}")


def check_plugins(r):
    g = "plugins"
    core = core_modules() - {"taskq_api"}
    r.checked(g, 2)
    if PLUGIN_ERR is not None:
        r.bad(g, f"merging plugins into the core failed: {PLUGIN_ERR}")
    else:
        for k, v in recipes.HOOKS.items():
            if not callable(v):
                r.bad(g, f"recipes plugin HOOKS[{k!r}] is not a function")
    check_labels(r)
    try:
        cfg.extend({"CONSUMERS": {}}, "consumers")
        cfg.extend({"SMOKE": {}}, "smoke")
        r.checked(g, 2)
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"merging plugins into the core failed: {e}")
    for d in cfg.plugin_dirs():
        for p in sorted(glob.glob(os.path.join(d, "*.py")) + glob.glob(os.path.join(d, "*", "*.py"))):
            if "/tests/" in p or os.path.basename(p).startswith("test_"):
                continue
            r.checked(g)
            try:
                tree = ast.parse(open(p, encoding="utf-8").read(), p)
            except SyntaxError as e:
                r.bad(g, f"{p} has a syntax error: {e}")
                continue
            used = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    used |= {a.name.split(".")[0] for a in node.names}
                elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
                    used.add(node.module.split(".")[0])
            local = {os.path.splitext(os.path.basename(x))[0] for x in glob.glob(os.path.join(os.path.dirname(p), "*.py"))}
            bad = sorted((used & core) - local)
            if bad:
                r.bad(g, f"{os.path.relpath(p, cfg.REPO)} imports core {bad} directly: a plugin imports taskq_api only")


# ---------------- experiment table ----------------
def check_experiments(r):
    import experiments as E
    g = "experiments"
    try:
        exps, final = E.load()  # fields (EXP_FIELDS, STEP_FIELDS) are checked on load
        ts = E.tasks()
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"cannot read the experiment tables: {e}")
        return False
    if not exps:
        r.note(g, f"no experiments (OUTPUTS[\"experiments\"] = {cfg.outputs('experiments')!r})")
    for e in exps:
        r.checked(g)
        for k, v in (e.get("each") or {}).items():
            if not isinstance(v, (list, tuple)) or not v:
                r.bad(g, f"experiment {e['id']}: each[{k!r}] must be a non-empty list")
    for st in [st for e in exps for st in e["after"] + e["group_after"]] + final:
        r.checked(g)
        for k, (t, _, _) in schema.STEP_FIELDS.items():
            if k in st and not TYPES[t][0](st[k]):
                r.bad(g, f"step {st['name']}: {k} must be {TYPES[t][1]}, got {st[k]!r}")
        if len(st["out"]) < 1 or not st["done"]:
            r.bad(g, f"step {st['name']}: out and done each need at least one entry")
    labels = {}
    for k, t in ts.items():
        labels.setdefault(E.label(E.experiment(t["exp"]), t), []).append(k)
    dup = {lb: ks for lb, ks in labels.items() if len(ks) > 1}
    if dup:
        r.bad(g, f"duplicate task short names (step instance ids would collide): {dict(list(dup.items())[:5])}")
    return True


# ---------------- order ----------------
_FLOW = {}


def flow():
    if "f" not in _FLOW:
        from flow import compile as FC
        _FLOW["f"] = FC.build()
    return _FLOW["f"]


def check_flow(r):
    """Instances and order: overlapping outputs, an input inside its own output, cycles, names a template cannot fill (errors from
    flow/compile.py); a step or recipe named in PLACE exists and its executors have such a worker."""
    M = cfg.mtab()
    import policy as P
    import experiments as E
    g = "flow"
    f = flow()
    r.checked(g, len(f.instances))
    for e in f.errors:
        r.bad(g, e)
    exps, final = E.load()
    steps = {st["name"]: st for e in exps for st in e["after"] + e["group_after"]} | {st["name"]: st for st in final}
    trains = {e["recipe"] for e in exps}
    for k, v in (getattr(P, "PLACE", {}) or {}).items():
        if k not in steps and k not in trains:
            r.bad(g, f"PLACE[{k!r}] is not any step or training recipe (steps: {', '.join(sorted(steps)) or 'none'})")
            continue
        dev = steps[k]["device"] if k in steps else "gpu"
        cand = M.executors() if v == "any" else [n for n in v if n in M.MACHINES]
        if cand and not any(M.workers(n, dev) for n in cand):
            r.bad(g, f"none of PLACE[{k!r}]'s executors {cand} has a {dev} worker")
    for k, st in steps.items():
        if k not in (getattr(P, "PLACE", {}) or {}) and not any(M.workers(n, st["device"]) for n in M.executors()):
            r.bad(g, f"step {k} needs a {st['device']} worker but no executor has one")
    for i in f.instances.values():
        if i.step and not i.inputs and not i.step.get("inputs_cmd"):
            r.note(g, f"{i.id} has no inputs: it only follows the previous step of the same task and gets no other inputs on the executor")
            break


# ---------------- each task ----------------
def check_tasks(r):
    import experiments as E
    import spec as S
    g = "tasks"
    seen = {}
    for k, t in E.tasks().items():
        r.checked(g)
        sp = t["spec"]
        for name, fn in (("spec", lambda: sp),
                         ("the executor's k=v params", lambda: S.resolve(sp["recipe"], S.parse_kv(S.kv_args(sp["recipe"], sp["params"])[1:]))),
                         ("the spec sent to an executor (--spec)", lambda: S.from_sent(json.dumps(S.sendable(sp))))):
            try:
                S.shell_lines(fn(), "/d")
            except Exception as e:  # noqa: BLE001
                r.bad(g, f"{k}: cannot turn {name} into shell variables: {type(e).__name__}: {e}")
        try:
            S.outputs(sp)
        except Exception as e:  # noqa: BLE001
            r.bad(g, f"{k}: outputs failed: {e}")
        check_smoke(r, k, sp, seen)


def check_smoke(r, k, sp, seen):
    """The task's smoke task (smoke.py definition) resolves, uses the same env as the real task and has a smoke dir; the same smoke
    is checked once. A store-side error (unrelated to the task) is reported once per recipe and later tasks of that recipe skip the
    store."""
    import spec as S
    if ("store", sp["recipe"]) in seen:
        return
    try:
        d = SG.definition(sp["recipe"], SG.real_params(sp))
    except SG.StoreError as e:
        seen["store", sp["recipe"]] = True
        r.bad("tasks", f"smoke definition for recipe {sp['recipe']} was not checked to the end (from {k}): {e}")
        return
    except Exception as e:  # noqa: BLE001
        r.bad("tasks", f"{k}: smoke definition failed: {e}")
        return
    if d is None:
        return
    names = sp.get("names") or {}
    params = {p: names[p] for p in d["by"] if p in names}
    params.update(SG.subst(d["params"], "SID"))
    if d["vars"]:
        params["vars"] = SG.subst(d["vars"], "SID")
    sig = json.dumps([sp["recipe"], params], sort_keys=True, default=str)
    if sig in seen:
        return
    seen[sig] = True
    try:
        sm = S.resolve(sp["recipe"], dict(params, key="_smoke/x/SID"))
        S.shell_lines(sm, "/d")
        if S.envs_of(sm) != S.envs_of(sp):
            r.bad("tasks", f"{k}: smoke task env {S.envs_of(sm)} differs from the real task's {S.envs_of(sp)} (write env params into by)")
        if not SG.smoke_dirs(sm, "SID"):
            r.bad("tasks", f"{k}: smoke task outputs are not under a dir named {{smoke}} (out or vars' {{data}}/...)")
    except Exception as e:  # noqa: BLE001
        r.bad("tasks", f"{k}: cannot resolve the smoke task: {type(e).__name__}: {e}")


# ---------------- each step ----------------
def one_per_step():
    """One instance of each step of each experiment (one of each FINAL step)."""
    seen = {}
    for i in flow().instances.values():
        if i.step:
            seen.setdefault((i.exp, i.rule), i)
    return list(seen.values())


def check_steps(r):
    """Every step instance turns into a hub queue task (recipe command params -> spec -> shell variables) and its env exists on an
    executor with such a worker."""
    import advance as A
    import spec as S
    M = cfg.mtab()
    g = "steps"
    for i in one_per_step():
        r.checked(g)
        try:
            p = A.queue_params(i)
            s = S.resolve("command", S.parse_kv(S.kv_args("command", p)[1:]))
            S.shell_lines(s, "/d")
        except Exception as e:  # noqa: BLE001
            r.bad(g, f"{i.id} cannot become a hub queue task: {type(e).__name__}: {e}")
            continue
        envs = set(S.envs_of(s))
        if not any(envs <= set(M.machine(n)["envs"]) for n in M.executors() if M.workers(n, i.step["device"])):
            r.bad(g, f"{i.id} needs envs {sorted(envs)} and no executor with a {i.step['device']} worker has all of them")


def check_inputs(r):
    """Run each step's inputs_cmd once per experiment (in an empty temp data dir, listing inputs only, reading and writing no data);
    it must list inputs."""
    import spec as S
    g = "inputs"
    root = tempfile.mkdtemp(prefix="check_data.")
    todo = [i for i in one_per_step() if i.step.get("inputs_cmd")]

    def run(i):
        try:
            return S.listed_inputs(S.fill(i.step["inputs_cmd"], i.names), root, i.step.get("vars"), timeout=120), None
        except Exception as e:  # noqa: BLE001
            return None, str(e)
    try:
        with ThreadPoolExecutor(8) as ex:
            res = list(ex.map(run, todo))
    finally:
        shutil.rmtree(root, ignore_errors=True)
    for i, (got, err) in zip(todo, res):
        r.checked(g)
        if err:
            r.bad(g, f"{i.id}: {err}")
        elif not got:
            r.note(g, f"{i.id}: inputs_cmd listed no inputs")


def check_raw(r):
    """Inputs in no output (raw data) all exist on the store (via site_ops probe, read-only)."""
    import relay_remote as RM
    g = "raw"
    raw = sorted({p for i in flow().instances.values() for p in i.raw})
    r.checked(g, len(raw))
    if not raw:
        return
    try:
        got = RM.store_op("probe", dict(pats=raw))
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"cannot connect to the store to check raw data: {e}")
        return
    for p in raw:
        if not (got.get(p) or {}).get("n"):
            who = [i.id for i in flow().instances.values() if p in i.raw]
            r.bad(g, f"raw data {p} is not on the store ({', '.join(who[:3])}{' and others' if len(who) > 3 else ''} need it; it is no step's output)")


# ---------------- bundle ----------------
def check_bundle(r):
    import relay_bundle as B
    g = "bundle"
    skip = ("*/test_*", "*/tests/*", "*/testdata/*")
    r.checked(g)
    out = tempfile.mkdtemp(prefix="check_bundle.")
    try:
        tests = sorted(f for f in B.contents(None) if any(fnmatch.fnmatch(f, p) for p in skip))
        B.build(None, out=out)
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"cannot build a bundle: {e}")
        return
    finally:
        shutil.rmtree(out, ignore_errors=True)
    if tests:
        r.note(g, f"the bundle has test files ({len(tests)}, e.g. {tests[0]}); add unwanted ones to OUTPUTS bundle_skip")


CHECKS = dict(machines=check_machines, policy=check_policy, plugins=check_plugins, experiments=check_experiments, flow=check_flow,
              tasks=check_tasks, steps=check_steps, inputs=check_inputs, raw=check_raw, bundle=check_bundle)


def run(only=None, quick=False):
    """-> Report."""
    r = Report()
    groups = [x for x in GROUPS if (not only or x in only) and not (quick and x in SLOW and not only)]
    ok_exps = None
    for x in groups:
        if x in NEED_EXPS and (ok_exps is False or PLUGIN_ERR is not None):
            r.note(x, "cannot read the experiment tables, skipping")
            continue
        try:
            got = CHECKS[x](r)
            if x == "experiments":
                ok_exps = got
        except Exception as e:  # noqa: BLE001
            r.bad(x, f"the check of this group failed: {type(e).__name__}: {e}\n{traceback.format_exc(limit=3)}")
        r.counts.setdefault(x, 0)
    return r


def main(argv):
    ap = argparse.ArgumentParser(description="check machine table, policy, plugins, experiment table, order, tasks, steps, raw data and bundle (read-only)")
    ap.add_argument("--quick", action="store_true", help="do not run inputs, raw, bundle")
    ap.add_argument("--only", nargs="+", choices=GROUPS, metavar="group", help=" ".join(GROUPS))
    a = ap.parse_args(argv)
    r = run(a.only, a.quick)
    for grp, msg in r.problems:
        print(f"ERROR [{grp}] {msg}", file=sys.stderr)
    for grp, msg in r.notes:
        print(f"NOTE  [{grp}] {msg}", file=sys.stderr)
    per = {}
    for grp, _ in r.problems:
        per[grp] = per.get(grp, 0) + 1
    for grp, n in r.counts.items():
        print(f"{grp:<12} checked {n:>4}  " + (f"{per[grp]} problems" if per.get(grp) else "pass"))
    print(f"{cfg.config_dir()}: " + (f"{len(r.problems)} problems" if r.problems else "no problems"))
    return 1 if r.problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
