"""Read every experiment table (policy.py's OUTPUTS["experiments"] in the config dir, a glob relative to the repo root) and expand
it into tasks. taskq.py (add --exp, resolving queued tasks), flow/compile.py (step instances and order) and the ledger all read
from here; analysis scripts look up the owning experiment's declarations by model dir (name-of, names_of).

Fields are in queue/schema.py: EXP_FIELDS for experiments and STEP_FIELDS for steps (in after, group_after, FINAL); checked on
load, unknown fields and missing required ones are errors. Names usable in a step template: a per-task step gets the task's params
+ exp, id (task_names); an experiment-wide step gets the params all tasks share + exp, id + each param <param>_list (values
space-separated), <param>_list_q (quoted) + the experiment's names (group_names); FINAL gets data only. {data} is left for the
executor to expand to its data dir. A table module may also hold FINAL (steps belonging to no experiment)."""
import glob
import importlib.util
import itertools
import json
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # the scheduler code root
sys.path.insert(0, HERE)
import cfg  # noqa: E402
REPO = cfg.REPO  # the user's repo root
import spec  # noqa: E402
import schema  # noqa: E402
import recipes as R  # noqa: E402  the core recipes table; load it before an experiment table adds plugin dirs to sys.path, so a same-named plugin cannot replace it

_cache = {}


def _check_fields(where, d, table):
    if not isinstance(d, dict):
        raise spec.SpecError(f"{where}: must be a dict")
    bad = sorted(set(d) - set(table))
    if bad:
        raise spec.SpecError(f"{where}: unknown fields {', '.join(bad)} (allowed: {', '.join(table)}; see taskq machines fields)")
    miss = [k for k, v in table.items() if v[1] is schema.REQUIRED and k not in d]
    if miss:
        raise spec.SpecError(f"{where}: missing required fields {', '.join(miss)}")


def step(st):
    """Fill a step with defaults; out, done, inputs, env and temp become lists."""
    st = schema.defaults(schema.STEP_FIELDS, st)
    for k in ("out", "done", "inputs", "env", "temp"):
        v = st.get(k)
        st[k] = [v] if isinstance(v, str) else list(v or [])
    return st


def check_fields(path, mod):
    """Every experiment and step field in an experiment table module is known and required ones are present; otherwise SpecError."""
    steps = [(f"step {st.get('name')} in {path} FINAL", st) for st in getattr(mod, "FINAL", [])]
    for e in mod.EXPERIMENTS:
        where = f"experiment {e.get('id')} in {path}"
        _check_fields(where, e, schema.EXP_FIELDS)
        each = e.get("each") or {}
        for st in e.get("after") or []:
            only = st.get("only")
            if only is None:
                continue
            bad = [k for k in (only if isinstance(only, dict) else ["?"]) if k not in each]
            if bad or not all(isinstance(v, (list, tuple)) for v in only.values()):
                raise spec.SpecError(f"step {st.get('name')} in {where}: only must be {{each param: [values...]}} (each has: {', '.join(each) or 'none'})")
        for st in e.get("group_after") or []:
            if not isinstance(st, dict):
                raise spec.SpecError(f"group_after in {where}: each entry is a step (dict); order follows inputs, no separate lists")
            if "only" in st:
                raise spec.SpecError(f"step {st.get('name')} in {where}: only applies to per-task steps (after)")
        steps += [(f"step {st.get('name')} in {where}", st) for st in list(e.get("after") or []) + list(e.get("group_after") or [])]
    seen = set()
    for where, st in steps:
        if id(st) in seen:
            continue
        seen.add(id(st))
        _check_fields(where, st, schema.STEP_FIELDS)
        if st.get("device", "gpu") not in ("gpu", "cpu"):
            raise spec.SpecError(f"{where}: device is gpu or cpu")


def load():
    """-> (experiment list, FINAL step list), with defaults filled; each experiment gains rq (the dir name of its table). Duplicate
    ids and task names are errors."""
    if "exps" in _cache:
        return _cache["exps"], _cache["final"]
    exps, final = [], []
    pat = cfg.outputs("experiments")
    for path in sorted(glob.glob(os.path.join(REPO, pat))) if pat else []:
        rq = os.path.basename(os.path.dirname(path))
        sp = importlib.util.spec_from_file_location(f"experiments_{rq}", path)
        mod = importlib.util.module_from_spec(sp)
        sp.loader.exec_module(mod)
        check_fields(os.path.relpath(path, REPO), mod)
        for e in mod.EXPERIMENTS:
            e = schema.defaults(schema.EXP_FIELDS, e)
            exps.append(dict(e, rq=rq, after=[step(x) for x in e["after"]], group_after=[step(x) for x in e["group_after"]]))
        final += [step(f) for f in getattr(mod, "FINAL", [])]
    ids = [e["id"] for e in exps]
    dup = sorted({i for i in ids if ids.count(i) > 1})
    if dup:
        raise spec.SpecError(f"duplicate experiment ids in the tables: {', '.join(dup)}")
    _cache["exps"], _cache["final"] = exps, final
    return exps, final


def experiment(eid):
    return next((e for e in load()[0] if e["id"] == eid), None)


def resolve_task(recipe, params, exp=None):
    """A queued task (recipe, params, owning experiment) -> spec. When it belongs to an experiment, the experiment's fields (e.g.
    metrics) are added."""
    e = experiment(exp) if exp else None
    return spec.resolve(recipe, {**((e or {}).get("fields") or {}), **params}, exp=exp)


def expand(e, only=None):
    """One experiment -> [dict(key, exp, recipe, params, spec, each)] (one per combination of each; only={param: [values]} expands
    just those values)."""
    each = {k: [v for v in vs if not only or k not in only or v in only[k]] for k, vs in (e.get("each") or {}).items()}
    bad = sorted(set(only or {}) - set(each))
    if bad:
        raise spec.SpecError(f"{e['id']} does not expand over {', '.join(bad)} (each has: {', '.join(each) or 'none'})")
    out = []
    for vals in itertools.product(*each.values()):
        pick = dict(zip(each, vals))
        params = dict(e["params"], **pick)
        s = resolve_task(e["recipe"], params, e["id"])
        out.append(dict(key=s["key"], exp=e["id"], recipe=e["recipe"], params=params, spec=s, each=pick))
    return out


def applies(st, t):
    """Whether step st applies to task t: without only it applies to all; with only, every each param of t must be among the values
    listed for it."""
    only = st.get("only") or {}
    each = t.get("each") or {}
    return all(str(each.get(k)) in [str(v) for v in vs] for k, vs in only.items())


def steps_of(t, e=None):
    """Steps to do after task t is done (the experiment's after entries that apply to it, in order); a task outside an experiment -> []."""
    e = e if e is not None else experiment(t.get("exp")) if t.get("exp") else None
    return [st for st in (e or {}).get("after") or [] if applies(st, t)]


def short(t):
    """A task's short name in its experiment: each param becomes <first letter of param><value>, space-separated (e.g. "f1", "s2 f0"); no each -> ""."""
    return " ".join(f"{k[0]}{v}" for k, v in (t.get("each") or {}).items())


def label(e, t):
    """The task name in a hub pueue job label: experiment id + every item of the short name prefixed with "_" (e.g. small_s1)."""
    return e["id"] + "".join("_" + x for x in short(t).split())


def parse_label(who):
    """A task name from a label -> (experiment, task); an experiment-wide one (id only) -> (experiment, None); unrecognised -> (None, None)."""
    if "by_label" not in _cache:
        _cache["by_label"] = {label(experiment(t["exp"]), t): t for t in tasks().values()}
    t = _cache["by_label"].get(who)
    if t:
        return experiment(t["exp"]), t
    return experiment(who), None


def tasks():
    """Every task of every experiment: {key: task}, in experiment table order. Duplicate task names are errors."""
    if "tasks" in _cache:
        return _cache["tasks"]
    out = {}
    for e in load()[0]:
        for t in expand(e):
            if t["key"] in out:
                raise spec.SpecError(f"duplicate task name in the experiment tables: {t['key']} ({out[t['key']]['exp']} and {e['id']})")
            out[t["key"]] = t
    _cache["tasks"] = out
    return out


def group_names(e):
    """Template names for experiment-wide steps (group_after): the params all tasks share + exp, id + each param <param>_list
    (values space-separated), <param>_list_q (quoted) + the experiment's names."""
    ts = expand(e)
    common = {k: v for k, v in ts[0]["spec"]["names"].items() if all(t["spec"]["names"].get(k) == v for t in ts)}
    lists = {}
    for k, vs in (e.get("each") or {}).items():
        lists[f"{k}_list"] = " ".join(vs)
        lists[f"{k}_list_q"] = shlex.quote(" ".join(vs))
    return dict(common, exp=e["exp"], id=e["id"], **lists, **(e.get("names") or {}))


def task_names(t):
    """Template names for a task's steps (after): the task's params + exp, id."""
    e = experiment(t["exp"]) or {}
    return dict(t["spec"]["names"], exp=e.get("exp", ""), id=t["exp"])


class NotFound(spec.SpecError):
    """Not in the experiment table (command-line exit code proto.EXIT["gone"])."""


def _task_of(model_dir):
    """Model dir (wherever it sits) -> the first task in the experiment tables that trained it (the recipe plugin's model_of hook
    supplies the model param); not in the tables -> None."""
    if "model_of" not in R.HOOKS:
        raise spec.SpecError("the recipes plugin has no model_of hook (recipes.py HOOKS), cannot look up the experiment table by model dir")
    m = R.HOOKS["model_of"](model_dir)
    return next((t for t in tasks().values() if t["spec"].get("names", {}).get("model") == m), None)


def env_of(model_dir):
    """Model dir -> the env used to train it in the experiment tables; not in the tables -> SpecError (the caller sets MODEL_ENV for
    models outside the tables)."""
    t = _task_of(model_dir)
    if t is None:
        raise NotFound(f"no experiment in the tables trains this model: {model_dir} (set MODEL_ENV for models outside the tables)")
    return spec.envs_of(t["spec"])[0]


def experiment_of(model_dir):
    """Model dir -> the experiment that trained it; not in the tables -> None."""
    t = _task_of(model_dir)
    return experiment(t["exp"]) if t else None


def names_of(model_dir, *names):
    """Model dir -> these entries of its experiment's names (empty strings when absent); analysis scripts read the experiment's
    declarations this way (e.g. skip_ext, splits). A model not in the tables -> SpecError: never guess by name."""
    e = experiment_of(model_dir)
    if e is None:
        raise NotFound(f"no experiment in the tables trains this model: {model_dir}")
    return [str((e.get("names") or {}).get(n, "")) for n in names]


# ---------------- command line ----------------
def jsonable(v):
    """A value from an experiment table -> JSON-ready: functions and classes become names, other objects (e.g. a step's done)
    become {"type": class name, attributes...} (functions among the attributes become names too)."""
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, dict):
        return {str(k): jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [jsonable(x) for x in (sorted(v, key=str) if isinstance(v, (set, frozenset)) else v)]
    if callable(v) and hasattr(v, "__name__"):
        return getattr(v, "__qualname__", v.__name__)
    attrs = {k: jsonable(x) for k, x in vars(v).items() if not k.startswith("_")} if hasattr(v, "__dict__") else {}
    return dict(type=type(v).__name__, **attrs)


def task_json(t):
    """One expanded task -> proto.EXP_TASK_FIELDS."""
    e = experiment(t["exp"]) or {}
    return dict(key=t["key"], exp=t["exp"], recipe=t["recipe"], params=jsonable(t["params"]), each=jsonable(t.get("each") or {}),
                short=short(t), label=label(e, t) if e else t["key"], names=jsonable(t["spec"].get("names") or {}),
                spec=jsonable(t["spec"]))


def _line(obj):
    print(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))


def main(argv):
    """env-of, name-of (used by scripts on executors, output unchanged); list, tasks (--json prints one JSON line, fields in
    proto.EXP_*). Exit codes: 0, 1 error, 2 usage error, proto.EXIT["gone"] when not in the experiment table."""
    import argparse
    import proto
    prog = os.path.basename(sys.argv[0])  # experiments.py, or taskq exp

    def fail(err):
        print(err, file=sys.stderr)
        return proto.EXIT["gone"] if isinstance(err, NotFound) else 1
    if argv[:1] == ["env-of"] or argv[:1] == ["name-of"]:
        try:
            if argv[0] == "env-of" and len(argv) == 2:
                print(env_of(argv[1]))
                return 0
            if argv[0] == "name-of" and len(argv) >= 3:
                print("\n".join(names_of(argv[1], *argv[2:])))
                return 0
        except spec.SpecError as err:
            return fail(err)
        print(USAGE.replace("experiments.py", prog), file=sys.stderr)
        return 2
    ap = argparse.ArgumentParser(prog=prog, description="experiment table (OUTPUTS[\"experiments\"])",
                                 epilog=USAGE.replace("experiments.py", prog), formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("list", help="entries of the experiment tables")
    p.add_argument("--json", action="store_true", help="one JSON line (proto.EXP_LIST_TOP)")
    p = sub.add_parser("tasks", help="expanded tasks")
    p.add_argument("--exp", nargs="+", metavar="ID", help="only these experiments")
    p.add_argument("--json", action="store_true", help="one JSON line (proto.EXP_TASKS_TOP, each task proto.EXP_TASK_FIELDS)")
    a = ap.parse_args(argv)
    try:
        exps, final = load()
        if a.cmd == "list":
            if a.json:
                _line(dict(version=proto.VERSION, experiments=jsonable(exps), final=jsonable(final)))
            else:
                n = {}
                for t in tasks().values():
                    n[t["exp"]] = n.get(t["exp"], 0) + 1
                for e in exps:
                    print(f"{e['id']}\t{e.get('exp', '')}\t{e.get('recipe', '')}\t{n.get(e['id'], 0)} tasks\t{e.get('label', '')}")
            return 0
        want = a.exp or []
        bad = [x for x in want if experiment(x) is None]
        if bad:
            raise NotFound(f"not in the experiment tables: {', '.join(bad)}")
        ts = [t for t in tasks().values() if not want or t["exp"] in want]
        if a.json:
            _line(dict(version=proto.VERSION, tasks=[task_json(t) for t in ts]))
        else:
            for t in ts:
                print(f"{t['key']}\t{t['exp']}\t{label(experiment(t['exp']), t)}")
        return 0
    except spec.SpecError as err:
        return fail(err)


USAGE = """usage: experiments.py env-of <model dir>
       experiments.py name-of <model dir> <name>... (one per line)
       experiments.py list [--json]
       experiments.py tasks [--exp ID...] [--json]"""


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
