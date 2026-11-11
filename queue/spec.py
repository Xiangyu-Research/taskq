#!/usr/bin/env python3
"""A task spec: recipe (queue/recipes.py) + params -> the full spec (key, env, command, output dir, what counts as done, ...).
Every scheduling script parses and judges through here and builds no paths itself. collect() runs on the machine that holds the data
(status on an executor, collect on the store; queue/site_ops.py).

Commands:
  spec.py shell <recipe> k=v...       shell statements for run_task.sh (TASK_KEY, TASK_ENV, TASK_CMD, TASK_OUT and vars, expanded for
                                      this machine; then drops this machine's machines.py task_unset and adds task_vars not yet set)
  spec.py done <recipe> k=v...        exit 0 when done, 1 when not
  spec.py shell|done --spec <JSON>    same, with a spec already resolved (the hub resolves it when sending; executors do not look up recipes)
  spec.py resolve <recipe> k=v...     the resolved spec (JSON, one line)
  spec.py lookup <key>                find a task by key (experiment table (experiments.py) first, then the task queue), print its spec (JSON)
  spec.py outputs <key>|--spec <JSON> [--sh]  this task's outputs (outputs(), JSON); --sh: shell statements for archive_task.sh
  spec.py collect --json < input.json machine-local collect() (cross-version reads use it): input {"specs": [spec...], "root": data dir
                                     (optional, as done), "wait": seconds (optional, default COLLECT_WAIT)}; output one @@JSON line
                                     {"version": ..., "tasks", "seglogs"[, "stuck", "errors"]}
A k=v value starting with [ or { is read as JSON (e.g. done='["a","b|c"]'); everything else is a string.
"""
import glob
import json
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # the scheduler code root
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg as _cfg  # noqa: E402
import proto as P  # noqa: E402
REPO = _cfg.REPO  # the user's repo root (cfg.py)
BUILTIN = ("data", "repo")  # names expanded for this machine at run time
FRESH_LOG = P.FRESH_LOG
SAFE = P.SAFE
safe = P.safe  # key -> filename (claims, part logs)
JSON_MARK = P.JSON_MARK  # JSON read by another machine over ssh goes only on a line starting with it (queue/README.md)


COLLECT_WAIT = 90  # seconds collect waits in all (tasks are read concurrently)


def json_out(obj):
    print(JSON_MARK + json.dumps(obj), flush=True)


def json_in(out):
    """The @@JSON line in out -> object; no such line or cannot parse -> None."""
    line = next((ln[len(JSON_MARK):] for ln in out.splitlines() if ln.startswith(JSON_MARK)), None)
    try:
        return json.loads(line) if line is not None else None
    except ValueError:
        return None


# ---------------- resolving ----------------
class SpecError(ValueError):
    pass


_REF = re.compile(r"(?<!\$)\{(\w+)\}")


def fill(v, names, keep=BUILTIN):
    """Replace {name} with its value (lists and dicts recursively). ${X} belongs to the shell and is left alone; names in keep
    stay as they are. If the whole value is one {name} and that value is not a string (e.g. done is a list), return the value itself."""
    if isinstance(v, str):
        m = _REF.fullmatch(v)
        if m and m.group(1) in names and not isinstance(names[m.group(1)], str):
            return names[m.group(1)]

        def sub(m):
            k = m.group(1)
            if k in names:
                return str(names[k])
            if k in keep:
                return m.group(0)
            raise SpecError(f"unknown {{{k}}} (have: {', '.join(sorted(names))})")
        return _REF.sub(sub, v)
    if isinstance(v, list):
        return [fill(x, names, keep) for x in v]
    if isinstance(v, dict):
        return {k: fill(x, names, keep) for k, x in v.items()}
    return v


def resolve(recipe, params, exp=None):
    """recipe + params -> spec (dict). exp: the experiment id from the experiment table (recorded only)."""
    import copy
    import recipes as R  # only used here (collect() runs on other machines where this file is absent)
    import schema
    if recipe not in R.RECIPES:
        raise SpecError(f"unknown recipe {recipe} (have: {', '.join(R.RECIPES)})")
    r = R.RECIPES[recipe]
    odd = sorted(set(r) - set(schema.RECIPE_FIELDS) - {"params", "open"})
    if odd:
        raise SpecError(f"unknown field {', '.join(odd)} in recipe {recipe} (see RECIPE_FIELDS in queue/schema.py)")
    fields = list(schema.RECIPE_FIELDS)
    over = {k: v for k, v in params.items() if k in fields and k not in r["params"]}
    p = {k: v for k, v in r["params"].items()}
    extra = [k for k in params if k not in p and k not in over]
    taken = sorted(set(params) & set(BUILTIN))
    if taken:
        raise SpecError(f"a param cannot be named {', '.join(taken)} ({{{taken[0]}}} is expanded per executor; write inputs in inputs)")
    if extra and not r.get("open"):
        raise SpecError(f"recipe {recipe} has no params {', '.join(extra)} (have: {', '.join(list(p) + fields)})")
    p.update({k: v for k, v in params.items() if k not in over})
    missing = [k for k, v in p.items() if v is R.REQUIRED]
    if missing:
        raise SpecError(f"recipe {recipe} is missing params {', '.join(missing)}")
    p = {k: (str(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v) for k, v in p.items()}
    for _ in range(5):  # a default referring to another param (e.g. model)
        q = {k: fill(v, p) for k, v in p.items()}
        if q == p:
            break
        p = q
    s = {}
    for k in fields:
        if k not in r and schema.RECIPE_FIELDS[k][1] is schema.REQUIRED:
            raise SpecError(f"recipe {recipe} is missing field {k}")
        v = copy.deepcopy(r[k] if k in r else schema.RECIPE_FIELDS[k][1])
        if k in over:
            v = {**v, **over[k]} if isinstance(v, dict) and isinstance(over[k], dict) else over[k]
        s[k] = fill(v, p)
    if isinstance(s["done"], str):
        s["done"] = [s["done"]]
    _check(s)
    s.update(recipe=recipe, params={k: v for k, v in params.items()}, exp=exp, names=p)
    return s


RES = ("gpus", "cpus", "mem", "gpu_mem", "wait_gpu", "local_inputs")


def _check(s, progress=True):
    """Common spec fields: normalise and check inputs, resume, res and progress.
    progress=False: do not check the progress reader (a spec from the hub was checked already; running a task does not need it and
    whoever reads progress reports itself). key, cmd, out, done, vars, log and fetch are type-checked only (add rejects bad types
    instead of failing later when reading the queue)."""
    for k in ("key", "cmd", "out"):
        if k in s and not (isinstance(s[k], str) and s[k]):
            raise SpecError(f"{k} must be a non-empty string: {s[k]!r}")
    if "done" in s and not (isinstance(s["done"], list) and all(isinstance(x, str) and x for x in s["done"])):
        raise SpecError(f"done must be a list of filenames: {s['done']!r}")
    for k, typ in (("vars", dict), ("fetch", dict), ("res", dict), ("log", str)):
        if s.get(k) is not None and not isinstance(s[k], typ):
            raise SpecError(f"{k} must be a {'dict' if typ is dict else 'string'}: {s[k]!r}")
    for k in ("inputs", "resume"):
        v = s.get(k)
        if isinstance(v, str):
            v = [v]
        if v is not None and (not isinstance(v, list) or not all(isinstance(x, str) and x for x in v)):
            raise SpecError(f"{k} must be a list of paths: {v!r}")
        if k == "inputs" and any(".." in x.split("/") for x in v or []):
            raise SpecError(f"inputs must not contain ..: {v!r}")
        s[k] = v if k == "resume" else (v or [])
    res = dict(s.get("res") or {})
    if set(res) - set(RES):
        raise SpecError(f"res may only hold {', '.join(RES)}: {res!r}")
    for k, v in res.items():
        if v is None:
            continue
        if k == "mem":
            if not re.fullmatch(r"\d+[KMGT]?", str(v)):
                raise SpecError(f"res.mem must look like 32G: {v!r}")
            res[k] = str(v)
            continue
        try:
            res[k] = float(v) if k == "gpu_mem" else int(v)
        except (TypeError, ValueError):
            raise SpecError(f"res.{k} must be a number: {v!r}") from None
        if res[k] < 0:
            raise SpecError(f"res.{k} must not be negative: {v!r}")
    if (res.get("gpus") or 0) > 1:
        raise SpecError(f"res.gpus is at most 1 (a worker gives each task one GPU): {res['gpus']!r}")
    s["res"] = res
    if progress and s.get("progress") not in PROGRESS:
        raise SpecError(f"unknown progress reader {s.get('progress')} (have: {', '.join(k for k in PROGRESS if k)})")


def envs_of(s):
    """The spec's env -> list of env names (env may be one name or a list of names; absent -> [])."""
    v = (s or {}).get("env")
    return [v] if isinstance(v, str) and v else [x for x in v if x] if isinstance(v, list) else []


def mem_mb(v):
    """"32G" -> MB; None -> None."""
    if v is None:
        return None
    m = re.fullmatch(r"(\d+)([KMGT]?)", str(v))
    return int(m.group(1)) * {"K": 1 / 1024, "": 1, "M": 1, "G": 1024, "T": 1024 ** 2}[m.group(2)]


def fits(res, cpus=None, mem_mb_=None, gpu_mem=None):
    """The task's res fits what the worker offers (an item the worker does not state is not checked) -> (fits, shortfall)."""
    res = res or {}
    need = [("cpus", res.get("cpus"), cpus, " CPUs"), ("mem", mem_mb(res.get("mem")), mem_mb_, " MB memory"),
            ("gpu_mem", res.get("gpu_mem"), gpu_mem, " GB GPU memory")]
    short = [f"{k} needs {n}{u}, only {h}" for k, n, h, u in need if n is not None and h is not None and n > h]
    return not short, "; ".join(short)


def parse_kv(args):
    out = {}
    for a in args:
        if "=" not in a:
            raise SpecError(f"a param must be written k=v: {a}")
        k, v = a.split("=", 1)
        out[k] = json.loads(v) if v[:1] and v[:1] in "[{" else v
    return out


def kv_args(recipe, params):
    """recipe + params -> command-line args (run_task.sh <recipe> k=v...)."""
    return [recipe] + [f"{k}={v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)}" for k, v in params.items()]


def expand(v, data, repo=REPO):
    """Expand {data} and {repo} to this machine's paths at run time."""
    return fill(v, dict(data=data, repo=repo), keep=())


def listed_inputs(cmd, data, vars=None, timeout=300):
    """Run the step's inputs_cmd (task names already filled in) once at the repo root -> the inputs it prints (relative to the
    data dir, one per line). {data} and {repo} expand to data and the repo root; vars (the task's env vars, expanded the same) and
    TASKQ_DATA=data go to it, and it gets the core lib and commands like a task's command from run_task.sh (TASKQ_LIB, the core's
    bin/ first on PATH). Error or timeout -> SpecError."""
    cmd = expand(cmd, data)
    env = dict(os.environ, **{k: str(expand(v, data)) for k, v in (vars or {}).items()}, TASKQ_DATA=data, TASKQ_LIB=HERE,
               PATH=os.path.join(CORE, "bin") + os.pathsep + os.environ.get("PATH", ""))
    try:
        p = subprocess.run(["bash", "-c", cmd], cwd=REPO, capture_output=True, text=True, timeout=timeout, env=env)
    except subprocess.TimeoutExpired:
        raise SpecError(f"inputs_cmd timed out ({timeout}s): {cmd}")
    if p.returncode:
        raise SpecError(f"inputs_cmd failed ({p.returncode}): {(p.stderr or p.stdout).strip()[-300:]}")
    return [ln.strip().rstrip("/") for ln in p.stdout.splitlines() if ln.strip()]


# ---------------- reading files (stdlib only; collect is sent to other machines) ----------------
def out_dir(s, root):
    return os.path.join(root, s["out"])


def outputs(s):
    """A task's outputs; out, done, fetch, archive and the part log are interpreted only here (fetch, place, done checks and
    archiving all use it):
      outs     dirs to fetch and back up (relative to the data dir): out, plus fetch.also
      done     done markers (relative to out): every item is required; a | separated item is an alternative, one suffices
      exclude  files not to fetch (filenames)
      extra    files next to out to fetch and back up too (wildcards, e.g. *.json)
      archive  whether to back up to the backup machine after fetching to the store (archive_task.sh)
      logs     part log filename prefix (relative to the data dir)"""
    f = s.get("fetch") or {}
    done = s.get("done") or []
    if isinstance(done, str):
        done = [done]
    return dict(outs=[os.path.normpath(o.strip("/")) for o in [s["out"]] + list(f.get("also") or [])],
                done=[d.split("|") for d in done], exclude=list(f.get("exclude") or []), extra=list(f.get("extra") or []),
                archive=bool(s.get("archive")), logs=f"{P.TASK_LOGS}/{safe(s['key'])}{SAFE}" if s.get("key") else None)


def done_missing(o, exists):
    """Items still missing from outputs' done (the "a|b" form); exists(path relative to out) is supplied by the caller (local
    files, fetched files, ...)."""
    return ["|".join(item) for item in o["done"] if not any(exists(a) for a in item)]


def done_with(o, exists, empty=True):
    """Done: every done item exists. An empty done -> empty (true when judging a finished run, false when adopting a checkpoint
    from elsewhere)."""
    return empty if not o["done"] else not done_missing(o, exists)


def is_done(s, root):
    """Done: every done item exists under out (one of a | separated item suffices).
    A missing file means not done; other read failures (disk gone) raise OSError and are not treated as not done."""
    fd = out_dir(s, root)
    os.stat(root)
    return done_with(outputs(s), lambda a: _exists(os.path.join(fd, a)))


def _exists(p):
    try:
        os.stat(p)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False


def log_age(s, root):
    """Seconds since the training log last changed; no log -> None."""
    logs = glob.glob(os.path.join(out_dir(s, root), s["log"])) if s.get("log") else []
    return time.time() - max(os.path.getmtime(p) for p in logs) if logs else None


# ---------------- facts on disk (taskq.py uses them to judge queue states) ----------------
def dir_age(d):
    """Seconds since the newest mtime of d and its immediate entries (when inferring a test set the log may not move while the
    prediction dir changes); none -> None."""
    try:
        ms = [os.path.getmtime(d)] + [e.stat().st_mtime for e in os.scandir(d)]
    except OSError:
        return None
    return time.time() - max(ms)


def seg_age(root, key, tag):
    """Seconds since the part log that job tag (a log name or worker name in the claim) wrote for this task last changed
    (run_task.sh); none -> None."""
    pre = os.path.join(root, P.TASK_LOGS, f"{glob.escape(safe(key))}{SAFE}{glob.escape(str(tag))}_")
    try:
        logs = glob.glob(f"{pre}{P.LOG_PART}*.log")
        return time.time() - max(os.path.getmtime(p) for p in logs) if logs else None
    except OSError:
        return None


def ran(root, key, since=None):
    """Whether the data dir has a part log for this task (any job, any part; key a/b does not count for a/b/c);
    since: only logs written at or after it (mtime; the task's added); None = any."""
    pre = os.path.join(root, P.TASK_LOGS, f"{safe(key)}{SAFE}")
    for p in glob.glob(f"{glob.escape(pre)}*_{P.LOG_PART}*.log"):
        if SAFE in os.path.basename(p)[len(os.path.basename(pre)):]:
            continue
        try:
            if since is None or os.path.getmtime(p) >= float(since):
                return True
        except (OSError, TypeError, ValueError):
            continue
    return False


def facts(items, root):
    """[dict(spec, since, tags)] -> {task key: dict(done, log_age, out_age, resumable, ran, tag_age={tag: seconds})}: each task's
    results and logs in data dir root (since: the task's added, older part logs do not count; tags: job names in the claim). An
    executor computes its own queue; the hub computes the hub queue on the store (site_ops facts). root unreadable or done cannot
    be judged -> OSError (not treated as not done)."""
    os.listdir(root)
    out = {}
    for it in items:
        s = it["spec"]
        fd = out_dir(s, root)
        try:
            res = bool(s.get("resume")) and resumable(s, fd)
        except (OSError, KeyError, TypeError, ValueError):
            res = False
        out[s["key"]] = dict(done=is_done(s, root), log_age=log_age(s, root), out_age=dir_age(fd), resumable=res,
                             ran=ran(root, s["key"], it.get("since")), tag_age={t: seg_age(root, s["key"], t) for t in it.get("tags") or [] if t})
    return out


def eta_at(runs, nep, hours=3):
    """A running task: speed over the last hours in the latest run's training log -> when it reaches nep epochs; cannot tell
    (fewer than two points, rate not positive, already there) -> None."""
    tl = max(runs, key=lambda r: r["end"] or "").get("tl") or [] if runs else []
    pts = [(datetime.strptime(t, "%Y-%m-%d %H:%M:%S"), e) for t, e in tl]
    if len(pts) < 2 or not nep:
        return None
    recent = [q for q in pts if q[0] >= pts[-1][0] - timedelta(hours=hours)]
    a, b = (recent if len(recent) >= 2 else pts[-2:])[0], pts[-1]
    rate = (b[1] - a[1]) / max((b[0] - a[0]).total_seconds(), 1)  # epochs / second
    if rate <= 0 or b[1] >= nep:
        return None
    return b[0] + timedelta(seconds=(nep - b[1]) / rate)


def progress_json(fd):
    """The task's own out/PROGRESS.json ({"done": n, "total": m}) -> one progress entry; absent or unreadable -> None."""
    p = os.path.join(fd, "PROGRESS.json")
    try:
        d = json.load(open(p))
        done = int(d.get("done") or 0)
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(p)))
    except (OSError, ValueError, TypeError, AttributeError):
        return None
    total = d.get("total")
    return dict(start=None, end=t, epochs_done=done, total=int(total) if isinstance(total, (int, float)) else None, tl=[[t, done]])


def resumable(s, fd):
    """Any of the resume checkpoints (glob) exists under out."""
    return any(glob.glob(os.path.join(fd, g)) for g in s.get("resume") or [])


def plain_log(path):
    """Without a progress reader: record only the log's mtime."""
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(path)))
    return dict(start=None, end=t, epochs_done=0, tl=[])


# How progress and training settings are read: generic ones here; model-specific ones in readers/<name>.py under each
# TASKQ_PLUGINS dir (PROGRESS, INFO, TRAINED tables, stdlib only), merged on load.
PROGRESS = {"progress_json": plain_log, None: plain_log}  # progress_json: the log supplies the time, progress is read separately
INFO = {None: lambda fd: {}}
TRAINED = []  # one of these in a part log = at least one epoch done (each reader's TRAINED; lines starting with run_task.sh's DONE_LINE count too)


def reader_sources():
    """readers/*.py (here and in each TASKQ_PLUGINS dir) -> {name: source} (empty when the dirs are absent)."""
    if HERE not in sys.path:
        sys.path.append(HERE)
    import cfg
    out, where = {}, {}
    for d in [os.path.join(x, "readers") for x in [HERE] + cfg.plugin_dirs()]:
        for f in sorted(os.listdir(d) if os.path.isdir(d) else []):
            if not f.endswith(".py") or f.startswith("test_"):
                continue
            p = os.path.join(d, f)
            if f[:-3] in where and os.path.realpath(where[f[:-3]]) != os.path.realpath(p):
                raise SpecError(f"{p} and {where[f[:-3]]} have the same name; the later one would replace the earlier")
            where[f[:-3]] = p
            out[f[:-3]] = open(p, encoding="utf-8").read()
    return out


def load_readers(sources):
    """Merge each reader's PROGRESS, INFO and TRAINED (duplicate names are an error, not an override)."""
    import types
    for name, src in sources.items():
        m = types.ModuleType(f"spec_reader_{name}")
        exec(compile(src, f"readers/{name}.py", "exec"), m.__dict__)
        for table, own in ((PROGRESS, getattr(m, "PROGRESS", {})), (INFO, getattr(m, "INFO", {}))):
            dup = sorted(set(own) & set(table))
            if dup:
                raise SpecError(f"readers/{name}.py has duplicate names {', '.join(map(str, dup))}")
            table.update(own)
        TRAINED.extend(x for x in getattr(m, "TRAINED", []) if x not in TRAINED)


load_readers(reader_sources())


def metrics(s, fd):
    out = {}
    for col, (fn, path) in (s.get("metrics") or {}).items():
        try:
            v = json.load(open(os.path.join(fd, fn)))
            for k in path:
                v = v[k]
            out[col] = v
        except Exception:  # noqa: BLE001
            pass
    return out


COLLECT_FIELDS = P.COLLECT_FIELDS


def all_tasks(queue=()):
    """Tasks to register: those in the experiment table (in table order) plus those in the task queue but not in the table.
    -> [dict(key, spec, e)] where e is the owning experiment (None for an ad-hoc task). queue: the task list from taskq state."""
    import experiments as E
    out = [dict(key=k, spec=t["spec"], e=E.experiment(t["exp"])) for k, t in E.tasks().items()]
    have = {t["key"] for t in out}
    for t in queue:
        if t["key"] not in have and t.get("spec") and not P.is_smoke(t["key"]):
            out.append(dict(key=t["key"], spec=t["spec"], e=None))
            have.add(t["key"])
    return out


def task_specs(queue=()):
    """Specs for collect() on each machine (only the fields it needs, to keep the amount sent over ssh small)."""
    return [{k: t["spec"].get(k) for k in COLLECT_FIELDS} for t in all_tasks(queue)]


def collect(specs, root, wait=None):
    """Each task on this machine (health check, ledger): {"tasks": {key: ...}, "seglogs": [...]}, plus "stuck": [key...] when some
    reads did not finish, and "errors": {key: reason} when reading failed (not stuck; other tasks carry on).
    specs: [spec] (at least key, out, done, log, progress, info, metrics); root: this machine's data dir.
    Part logs are under logs/tasks/.
    wait: with it, read every task in its own thread and after wait seconds record unfinished ones in stuck (files stuck in
    storage) without waiting longer; the stuck threads stay and end with the process (collect --json exits with os._exit)."""
    errors = {}

    def run(k, fn):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            errors[k] = f"{type(e).__name__}: {e}"[:300]
            return None

    jobs = [(s["key"], lambda s=s: one_task(s, root)) for s in specs] + [("@seglogs", lambda: seg_logs(root))]
    if wait is None:
        res = {k: run(k, fn) for k, fn in jobs}
    else:
        import threading
        res, ths = {}, []
        for k, fn in jobs:
            th = threading.Thread(target=lambda k=k, fn=fn: res.__setitem__(k, run(k, fn)), daemon=True)
            th.start()
            ths.append(th)
        end = time.time() + wait
        for th in ths:
            th.join(max(0, end - time.time()))
        res, errors = dict(res), dict(errors)
    out = {"tasks": {s["key"]: res[s["key"]] for s in specs if res.get(s["key"]) is not None}, "seglogs": res.get("@seglogs") or []}
    stuck = [k for k, _ in jobs if k not in res and k not in errors]
    if stuck:
        out["stuck"] = stuck
    if errors:
        out["errors"] = errors
    return out


def one_task(s, root):
    """One task for collect; no output dir yet -> None."""
    fd = out_dir(s, root)
    if not os.path.isdir(fd):
        return None
    logs = sorted(glob.glob(os.path.join(fd, s["log"]))) if s.get("log") else []
    parse = PROGRESS.get(s.get("progress"), plain_log)
    runs = []
    for p in logs:
        try:
            runs.append(parse(p))
        except OSError:
            pass
    if s.get("progress") == "progress_json":
        pj = progress_json(fd)
        if pj:
            runs.append(pj)
    return dict(runs=runs, final=is_done(s, root),
                metrics=metrics(s, fd), debug=INFO.get(s.get("info"), INFO[None])(fd), resumable=resumable(s, fd))


def seg_logs(root):
    """Part logs for collect: [{name, first, trained}]."""
    out = {"seglogs": []}
    for p in glob.glob(os.path.join(root, P.TASK_LOGS, f"*_{P.LOG_PART}*.log")):
        first, trained = "", False
        try:
            with open(p, errors="replace") as f:
                first = f.readline().strip()
                trained = any(line.startswith(P.DONE_LINE) or any(x in line for x in TRAINED) for line in f)  # at least one epoch
        except Exception:  # noqa: BLE001
            pass
        out["seglogs"].append(dict(name=os.path.basename(p), first=first, trained=trained))
    return out["seglogs"]


# ---------------- looking up tasks ----------------
SENT_DROP = ("machines", "names", "exp")  # fields only meaningful on the hub: allowed executors, names used for expansion, experiment id


def sendable(s):
    """A resolved spec -> the version sent to an executor (drops SENT_DROP; {data} and {repo} are left for that side to expand)."""
    return {k: v for k, v in s.items() if k not in SENT_DROP}


def from_sent(text):
    """A sent spec (JSON) -> spec; bad format -> SpecError."""
    try:
        s = json.loads(text) if isinstance(text, str) else dict(text)
    except (ValueError, TypeError) as e:
        raise SpecError(f"the spec is not a JSON object: {e}")
    if not isinstance(s, dict):
        raise SpecError(f"the spec is not a JSON object: {s!r:.80}")
    miss = [k for k in ("key", "cmd", "out", "done", "inputs", "res", "vars") if k not in s]
    if miss:
        raise SpecError(f"the spec is missing fields {', '.join(miss)}")
    if isinstance(s["done"], str):
        s["done"] = [s["done"]]
    _check(s, progress=False)
    return s


def lookup(key):
    """key -> spec: this machine's task queue first (taskq.py show: its recipe and params, including taskq set changes), then the
    experiment table (a task not queued here). Not found -> None."""
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "show", "--json", key], capture_output=True, text=True)
    s = json_in(r.stdout) if r.returncode == 0 else None
    if s is not None:
        return s
    import experiments as E
    t = E.tasks().get(key)
    return t["spec"] if t else None


SHELL_FIELDS = ("key", "env", "cmd", "out", "inputs", "res", "vars")


def shell_lines(s, data, repo=REPO):
    """For run_task.sh: turn the spec into shell variables (expanded for this machine's data dir and repo). Only the used fields
    are expanded: params hold the original params and may carry other {names}."""
    e = expand({k: s[k] for k in SHELL_FIELDS if k in s}, data, repo)
    lines = [f"TASK_KEY={shlex.quote(e['key'])}", f"TASK_ENV={shlex.quote((envs_of(e) or [''])[0])}", f"TASK_CMD={shlex.quote(e['cmd'])}",
             f"TASK_OUT={shlex.quote(os.path.join(data, e['out']))}", f"TASK_SAFE={shlex.quote(safe(e['key']))}",
             f"TASK_WAIT_GPU={'' if e['res'].get('wait_gpu') is None else int(e['res']['wait_gpu'])}",
             "TASK_INPUTS=(" + " ".join(shlex.quote(os.path.join(data, x)) for x in e["inputs"]) + ")",
             f"TASK_LOCAL={int(bool(e['res'].get('local_inputs')))}"]
    lines += [f"export {k}={shlex.quote(str(v))}" for k, v in e["vars"].items()]
    return "\n".join(lines)


def machine_vars(m=None):
    """This machine's (m: the given machine table entry) machines.py task_unset and task_vars -> shell statements: first unset
    what task_unset lists (set for the whole job in the worker's env, but the task computes its own), then set task_vars entries
    not set (non-empty) yet."""
    if m is None:
        import cfg
        M = cfg.mtab()
        me = M.whoami()
        m = (M.MACHINES.get(me) or {}) if me else {}
    tv = m.get("task_vars") or {}
    return ([f"unset {k}" for k in (m.get("task_unset") or ()) if re.fullmatch(r"[A-Za-z_]\w*", k)]
            + [f"[[ -n ${{{k}:-}} ]] || export {k}={shlex.quote(str(v))}" for k, v in tv.items()])


def main(argv):
    sys.path.insert(0, HERE)
    import cfg
    cfg.use_config()
    if not argv:
        print(__doc__.split("Commands:")[1], file=sys.stderr)
        return 2
    cmd, args = argv[0], argv[1:]
    try:
        return _main(cmd, args)
    except SpecError as e:
        print(f"spec.py: {e}", file=sys.stderr)
        return 2
    except OSError as e:
        if cmd != "done":
            raise
        print(f"spec.py: cannot read the data dir: {e}", file=sys.stderr)
        return DISK_RC


DISK_RC = P.EXIT["done_unreadable"]  # spec.py done cannot read the disk (all exit codes live in proto.EXIT)


def _main(cmd, args):
    if cmd in ("shell", "done", "resolve", "lookup", "outputs") and not args:
        raise SpecError(f"{cmd} needs arguments")
    if cmd == "collect":
        return collect_cmd(args)
    if cmd in ("shell", "done", "outputs") and args[:1] == ["--spec"] and len(args) >= 2:
        s = from_sent(args[1])
        args = args[1:]
    elif cmd in ("shell", "done", "resolve"):
        s = resolve(args[0], parse_kv(args[1:]))
    elif cmd in ("lookup", "outputs"):
        s = lookup(args[0])
        if s is None:
            print(f"no {args[0]} in the task queue or the experiment table", file=sys.stderr)
            return 1
    else:
        print(f"spec.py: unknown command {cmd}", file=sys.stderr)
        return 2
    data = local_data()
    if cmd == "done" and not data:
        raise SpecError("cannot tell this machine's data dir (TASKQ_DATA or machines.py data)")
    if cmd == "lookup":
        print(json.dumps(s, ensure_ascii=False, indent=1))
    elif cmd == "outputs":
        o = outputs(s)
        if "--sh" not in args[1:]:
            print(json.dumps(o, ensure_ascii=False, indent=1))
            return 0
        test = " && ".join("{ " + " || ".join("test -e " + shlex.quote(a) for a in item) + "; }" for item in o["done"])
        print("\n".join([f"KEY={shlex.quote(s['key'])}", f"OUTS=({' '.join(shlex.quote(x) for x in o['outs'])})", f"EXTRA=({' '.join(shlex.quote(x) for x in o['extra'])})",
                         f"TEST={shlex.quote(test or 'true')}", f"ARCHIVE={int(o['archive'])}", f"LOGS={shlex.quote(o['logs'] or '')}"]))
    elif cmd == "resolve":
        print(json.dumps(s, ensure_ascii=False, separators=(",", ":")))
    elif cmd == "shell":
        print("\n".join([shell_lines(s, data)] + machine_vars()))
    elif cmd == "done":
        return P.EXIT["done_yes"] if is_done(s, data) else P.EXIT["done_no"]
    return 0


def local_data():
    """This machine's data dir (cfg.data_dir); absent -> ""."""
    import cfg
    return cfg.data_dir()


def collect_cmd(args):
    """spec.py collect --json: stdin {"specs": [...], "root": ..., "wait": ...} -> one @@JSON line (with version)."""
    if args != ["--json"]:
        raise SpecError("usage: spec.py collect --json < input.json")
    try:
        d = json.loads(sys.stdin.read())
        specs, root, wait = d["specs"], d.get("root") or local_data(), d.get("wait", COLLECT_WAIT)
    except (ValueError, KeyError, TypeError, AttributeError) as e:
        raise SpecError(f"input must be {{\"specs\": [...], \"root\": ...}}: {type(e).__name__}: {e}") from None
    if not root:
        raise SpecError("cannot tell this machine's data dir (input root, TASKQ_DATA or machines.py data)")
    json_out({"version": P.VERSION, **collect(specs, os.path.expanduser(root), wait=wait)})
    sys.stdout.flush()
    os._exit(0)  # do not wait for stuck reader threads


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
