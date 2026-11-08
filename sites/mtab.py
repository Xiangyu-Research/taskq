#!/usr/bin/env python3
"""The machine table: the project's machines.py holds data only (MACHINES, one entry per physical machine; fields in
queue/schema.py), and this module says how to use it. Core code imports mtab as M, never machines directly.

Roles: hub (hub queue, health checks, no tasks), store (data dir, operated through site_ops.py), backup (archives),
executor (has root and scheduler: pueue workers or Slurm jobs). Worker names (gpu<card>, cpu), ids
(<machine>-<name>) and pueue groups are computed here.

Commands:
  mtab.py whoami                 this machine's name (exit 1 when unrecognised)
  mtab.py get <name|.> <field>   field value (defaults filled in); tuples and lists joined by spaces; on this machine
                                 the path fields expand ~ (EXPAND); also code (<root>/code/current) and core (the
                                 scheduler code's root); exit 4 when the machine is unknown, 1 when the field is unknown
  mtab.py names [role|scheduler] machine names, one per line (names pueue, names exec, names hub)
  mtab.py backups                machines with the backup role, one per line
  mtab.py q-env <group>          this machine's env vars for the group (export lines, used by queue/q); exit 2 when
                                 there is no such group
  mtab.py groups [name|.]        pueue groups of that machine (default this one), one "group:parallel" per line
                                 (used by queue/setup.sh); exit 4 when the machine is unknown
  mtab.py activate <env> [name|.] shell statements that activate the env (eval "$(...)"); exit 4 when the machine or
                                 env is unknown
  mtab.py fields                 field tables for the machine table, workers and policy
  mtab.py fields --markdown [table...] the field tables as Markdown (schema.TABLES names, default all), with README markers
  (taskq machines show [name...]: values computed from the table by the core, one JSON line, in queue/cli_json.py)
"""
import datetime as dt
import os
import re
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "queue"))
import cfg  # noqa: E402
cfg.use_config()
import machines as _data  # noqa: E402
import proto  # noqa: E402
import schema  # noqa: E402

MACHINES = _data.MACHINES
KINDS = ("gpu", "cpu")
SPECS = os.path.join(os.path.dirname(os.path.abspath(_data.__file__)), "specs")  # spec-built envs: <config dir>/specs/<env>.yml


class FieldError(KeyError):
    """The machine table has no such field."""

    def __str__(self):
        return self.args[0]


def machine(name):
    """One machine's entry, with defaults filled in."""
    return schema.defaults(schema.MACHINE_FIELDS, MACHINES[name])


def expand(path):
    return os.path.expanduser(path)


# ---------------- roles ----------------

def roles(name):
    return tuple(MACHINES[name].get("roles") or ())


def _role(r):
    got = [n for n in MACHINES if r in roles(n)]
    if len(got) != 1:
        raise LookupError(f"role {r} must match exactly one machine (now: {got or 'none'})")
    return got[0]


def hub():
    return _role("hub")


def store():
    return _role("store")


def data():
    """The store's data dir (where results live; ~ not expanded)."""
    return MACHINES[store()]["data"]


def is_exec(name):
    """Executor: has root and scheduler. A name that is not in the table → False."""
    m = MACHINES.get(name) or {}
    return bool(m.get("root") and m.get("scheduler"))


def executors():
    return [n for n in MACHINES if is_exec(n)]


def names(kind=None):
    """Machine names: kind is a role (hub, store, backup), exec (executors) or a scheduler (pueue, slurm); None = all."""
    if kind == "exec":
        return executors()
    return [n for n in MACHINES if kind is None or kind in roles(n) or kind == MACHINES[n].get("scheduler")]


def backups():
    return names("backup")


def pueue_hosts():
    """Machines with pueue groups (groups non-empty): pueue executors, the hub, the store, and machines with manual_groups.
    pueued runs on these (installed by setup.sh); health checks probe them, read pueue and restart pueued."""
    return [n for n in MACHINES if groups(n)]


def local(name):
    """This machine is the hub's machine (ssh is None)."""
    return MACHINES[name].get("ssh") is None


# ---------------- workers ----------------

def workers(name, kind=None):
    """This machine's workers (ordered): [dict(name, id, kind, group, gpu, cpus, slots, hours, idle, wait)].
    pueue: one per workers entry, named gpu<card> or cpu (cpu<n> when there are several CPU workers), with a pueue
    group of the same name; slurm: one per jobs entry (name and kind are the jobs keys, no pueue group)."""
    m = machine(name)
    out = []
    if m.get("scheduler") == "slurm":
        for k, o in (m["jobs"] or {}).items():
            out.append(dict(name=k, id=f"{name}-{k}", kind=k, group=None, gpu=None, cpus=o.get("cpus"),
                            slots=None, hours=(), idle=None, wait=None))
    elif m.get("scheduler") == "pueue":
        ws = m["workers"] or ()
        ncpu = sum(1 for w in ws if w.get("gpu") is None)
        icpu = 0
        for w in ws:
            w = schema.defaults(schema.WORKER_FIELDS, w)
            if w["gpu"] is not None:
                wn = "gpu" + str(w["gpu"]).replace(",", "")
            else:
                icpu += 1
                wn = "cpu" if ncpu == 1 else f"cpu{icpu}"
            out.append(dict(w, name=wn, id=f"{name}-{wn}", kind="gpu" if w["gpu"] is not None else "cpu", group=wn))
    return [w for w in out if kind is None or w["kind"] == kind]


def worker(name, wid):
    """The worker with id wid on this machine; none → None."""
    return next((w for w in workers(name) if w["id"] == wid), None)


def kinds(name):
    """Task kinds this machine can run (ordered)."""
    have = {w["kind"] for w in workers(name)}
    return tuple(k for k in KINDS if k in have)


def worker_env(w):
    """pueue worker env vars {name: value} (cards, cpus, parallel slots, GPU wait rules)."""
    env = {"WORKER_ID": w["id"], "WORKER_DEVICE": w["kind"], "CUDA_VISIBLE_DEVICES": w["gpu"] or ""}
    if w.get("cpus"):
        env["TASKQ_CPUS"] = str(w["cpus"])
    if w.get("slots"):
        env["TASKQ_SLOTS"] = str(w["slots"])
    if w.get("idle"):
        env["TASK_GPU_IDLE"] = str(w["idle"])
    if w.get("wait"):
        env["TASK_WAIT_GPU"] = str(w["wait"])
    return env


def groups(name):
    """This machine's pueue groups {group: (parallel, gpu, {env})}: one per pueue worker (one worker at a time), plus
    the core's own group on pueue machines, the hub and the store (proto.IO_GROUP: transfers, fetches, archives), plus
    manual_groups."""
    m = machine(name)
    out = {}
    for w in workers(name):
        if w["group"]:
            out[w["group"]] = (1, w["gpu"], {})
    if m.get("scheduler") == "pueue" or {"hub", "store"} & set(roles(name)):
        out[proto.IO_GROUP] = (0, None, {})
    out.update(m["manual_groups"] or {})
    return out


def _tz(name):
    t = machine(name).get("tz")
    if not t:
        return None
    from zoneinfo import ZoneInfo
    return ZoneInfo(t)


def now_at(name, t=None):
    """Local time at this machine (by tz), with timezone. t: seconds (None = now)."""
    import time
    t = time.time() if t is None else t
    tz = _tz(name)
    return dt.datetime.fromtimestamp(t, tz) if tz else dt.datetime.fromtimestamp(t).astimezone()


def in_hours(hours, now):
    """Is now (an aware datetime) inside hours → (inside, this window "start–end" or ""). Empty hours = all day.
    Each window is (start, end, first day, last day): end not later than start crosses midnight (start == end is a
    full 24 hours); dates are the day the window starts, None = no limit."""
    if not hours:
        return True, ""
    for start, end, first, last in hours:
        for d in (now.date(), now.date() - dt.timedelta(days=1)):
            h, mi = map(int, start.split(":"))
            s = now.replace(year=d.year, month=d.month, day=d.day, hour=h, minute=mi, second=0, microsecond=0)
            h, mi = map(int, end.split(":"))
            ed = d + dt.timedelta(days=1) if end <= start else d
            e = now.replace(year=ed.year, month=ed.month, day=ed.day, hour=h, minute=mi, second=0, microsecond=0)
            ds = d.isoformat()
            if s <= now < e and (first is None or ds >= first) and (last is None or ds <= last):
                return True, f"{start}–{end}"
    return False, " / ".join(f"{a}–{b}" for a, b, _, _ in hours)


def working(name, w, t=None):
    """Is this worker inside its hours window right now."""
    return in_hours(w.get("hours") or (), now_at(name, t))[0]


# ---------------- pause ----------------

def paused(name, t=None):
    """Reason if this machine is paused in policy.py's PAUSE right now; None when absent or expired. The expiry time is
    the hub's local time "YYYY-MM-DD HH:MM"."""
    import policy as P
    x = (getattr(P, "PAUSE", None) or {}).get(name)
    if not x:
        return None
    until, why = x
    import time
    now = dt.datetime.fromtimestamp(time.time() if t is None else t).strftime("%Y-%m-%d %H:%M")
    return why if until is None or now < until else None


# ---------------- this machine, paths ----------------

def whoami():
    """This machine's name: env var TASKQ_MACHINE (set by bundles and workers); else the hostname if it matches exactly
    one machine; else None."""
    e = os.environ.get("TASKQ_MACHINE")
    if e:
        return e if e in MACHINES else None
    h = socket.gethostname().split(".")[0]
    hit = [n for n, m in MACHINES.items() if re.fullmatch(m["hostname"], h)]
    return hit[0] if len(hit) == 1 else None


def code(name):
    """This machine's bundle (~ not expanded): <root>/code/current; machines without a root (hub only) → None (run from the repo)."""
    r = MACHINES[name].get("root")
    return r.rstrip("/") + "/" + proto.ROOT["current"] if r else None


def core(name):
    """The scheduler code's root on this machine (~ not expanded): with a root → <code>/scripts/taskq inside the bundle
    (cfg.bundle_rel); without → the repo clone's same place as here; neither → None."""
    c = code(name)
    if c:
        return c + "/" + cfg.bundle_rel()
    r = machine(name)["repo"]
    return r.rstrip("/") + "/" + cfg.CORE_REL if r else None


def env_root(name):
    """"spec" envs are built under this dir (~ not expanded): <root>/envs."""
    return MACHINES[name]["root"].rstrip("/") + "/envs"


def machine_of_jobtag(tag):
    """A job name from the task logs → the machine's label: all digits is a Slurm job id; resident workers write
    "worker id-time" (the id starts with the machine name); run_task.sh started by hand writes "hostname-time"."""
    if re.fullmatch(r"\d+", tag):
        labels = sorted({MACHINES[n]["label"] for n in names("slurm")})
        pre = os.path.commonprefix(labels).strip()
        return labels[0] if len(labels) == 1 else (pre or "Slurm")
    for n in MACHINES:
        if any(tag == w["id"] or tag.startswith(w["id"] + "-") for w in workers(n)):
            return MACHINES[n]["label"]
    for n in names("pueue"):
        if re.match(MACHINES[n]["hostname"], tag):
            return MACHINES[n]["label"]
    return tag


# ---------------- envs ----------------

def is_spec(e):
    return e == "spec" or (isinstance(e, (tuple, list)) and len(e) > 0 and e[0] == "spec")


def lock_path(env, variant="default"):
    """The lock next to the spec: <config dir>/specs/<env>.<variant>.lock.json (written by sites/envlock.py)."""
    return os.path.join(SPECS, f"{env}.{variant}{proto.LOCK_SUFFIX}")


def lock_bytes(path):
    """Canonical bytes of the lock without the LOCK_UNHASHED keys (keys sorted, compact); no such file → None."""
    import json
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except OSError:
        return None
    d = {k: v for k, v in d.items() if k not in proto.LOCK_UNHASHED}
    return json.dumps(d, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def spec_hash(env, variant="default"):
    """sha256(the bytes of <env>.yml + the canonical bytes of this variant's lock); without a lock it is the spec
    alone; no such spec → None."""
    import hashlib
    try:
        with open(os.path.join(SPECS, f"{env}.yml"), "rb") as f:
            h = hashlib.sha256(f.read())
    except OSError:
        return None
    lk = lock_bytes(lock_path(env, variant))
    if lk is not None:
        h.update(lk)
    return h.hexdigest()


def spec_base(env):
    """The spec's base (which env a venv builds on); none → None."""
    try:
        with open(os.path.join(SPECS, f"{env}.yml"), encoding="utf-8") as f:
            m = re.search(r"""^base:[ \t]*["']?([^\s"'#]+)""", f.read(), re.M)
    except OSError:
        return None
    return m.group(1) if m else None


def spec_variant(name, env):
    """Which variant of env this machine uses: ("spec", variant) in the entry is it; a venv (has base) follows the
    base env's variant here when a lock for that variant exists; otherwise default."""
    envs = machine(name)["envs"] or {}
    e = envs.get(env)
    if is_spec(e) and isinstance(e, (tuple, list)) and len(e) > 1:
        return e[1]
    b = spec_base(env)
    if b and is_spec(envs.get(b)):
        v = spec_variant(name, b)
        if v != "default" and os.path.isfile(lock_path(env, v)):
            return v
    return "default"


def ready_envs(name, oks=None):
    """Env names usable on this machine: conda / venv entries always count; a "spec" entry needs the remote <name>.ok
    (oks: {name: hash}) to match the current spec (plus the lock of the variant used here)."""
    out = set()
    for env, e in (machine(name)["envs"] or {}).items():
        if not is_spec(e) or ((oks or {}).get(env) and oks[env] == spec_hash(env, spec_variant(name, env))):
            out.add(env)
    return out


def activate(name, env):
    """Shell statements that activate env on this machine (leaving any active venv first); no such env → None."""
    m = machine(name)
    e = (m["envs"] or {}).get(env)
    if e is None:
        return None
    lines = ['if [[ -n ${VIRTUAL_ENV:-} ]]; then PATH=${PATH//"$VIRTUAL_ENV/bin:"/}; unset VIRTUAL_ENV; fi']
    if is_spec(e):  # activation written when the env was built (sites/envs.py)
        return "\n".join(lines + [f'source {env_root(name)}/{env}/.activate'])
    if e[0] == "venv":
        base = activate(name, e[2])
        if base is None:
            return None
        return base + "\n" + f'source {e[1]}/bin/activate'
    lines += [f'source {m["conda"][0]}/etc/profile.d/conda.sh', f'conda activate {e[1]}']
    return "\n".join(lines)


# ---------------- command line ----------------

EXPAND = ("conda", "repo", "root", "data", "code", "core", "backup", "pueue")  # fields mtab get expands ~ for on this machine


def _value(name, field):
    if field in ("code", "core"):  # computed: the bundle's current, this machine's scheduler code root (core())
        v = code(name) if field == "code" else core(name)
    else:
        if field not in schema.MACHINE_FIELDS:
            raise FieldError(f"no field {field} in the machine table (have: {', '.join(schema.MACHINE_FIELDS)})")
        v = machine(name)[field]
    if field in EXPAND and name == whoami() and v:
        v = expand(v) if isinstance(v, str) else (expand(v[0]),) + tuple(v[1:])
    if v is None or v is False:
        return ""
    if v is True:
        return "1"
    if isinstance(v, (tuple, list)):
        return " ".join(str(x) for x in v if x is not None)
    if isinstance(v, dict):
        return " ".join(v)
    return str(v)


def fields_text():
    """The field tables as text (taskq machines fields)."""
    def show(d):
        return "\n".join(f"  {k:<14} {t:<10} {'required' if df is schema.REQUIRED else repr(df):<22} {doc}" for k, (t, df, doc) in d.items())
    return "\n\n".join([f"machine table (MACHINES in machines.py, one entry per machine):\n{show(schema.MACHINE_FIELDS)}",
                        f"pueue executor workers (each workers entry):\n{show(schema.WORKER_FIELDS)}",
                        f"policy.py:\n{show(schema.POLICY_FIELDS)}",
                        f"policy.py's OUTPUTS:\n{show(schema.OUTPUT_FIELDS)}"])


def main(argv):
    from proto import EXIT
    prog = os.path.basename(sys.argv[0])  # mtab.py, or taskq machines
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__.split("Commands:\n")[1].replace("mtab.py", prog), file=sys.stderr if not argv else sys.stdout)
        return 2 if not argv else 0
    cmd, args = argv[0], argv[1:]
    me = whoami()

    def err(msg, rc):
        print(f"{prog} {cmd}: {msg}", file=sys.stderr)
        return rc

    def gone(name):
        if name in (".", None):
            return err("cannot tell which machine this is", 1)
        return err(f"no {name} in the machine table (have: {', '.join(MACHINES)}; this machine: {me or 'unrecognised'})", EXIT["gone"])
    usage = {"get": "<name|.> <field>", "q-env": "<group>", "activate": "<env> [name|.]", "groups": "[name|.]", "names": "[role|scheduler]"}
    nargs = {"whoami": (0, 0), "get": (2, 2), "names": (0, 1), "backups": (0, 0), "q-env": (1, 1), "groups": (0, 1),
             "activate": (1, 2), "fields": (0, len(schema.TABLES) + 1)}
    if cmd not in nargs:
        return err(f"unknown command ({prog} --help)", 2)
    lo, hi = nargs[cmd]
    if not lo <= len(args) <= hi:
        print(f"usage: {prog} {cmd} {usage.get(cmd, '')}".rstrip(), file=sys.stderr)
        return 2
    if cmd == "whoami":
        if not me:
            return 1
        print(me)
    elif cmd == "fields":
        if not args:
            print(fields_text())
            return 0
        tabs = args[1:] or list(schema.TABLES)
        bad = [n for n in tabs if n not in schema.TABLES]
        if args[0] != "--markdown" or bad:
            return err(f"usage: {prog} fields [--markdown [{' '.join(schema.TABLES)}]]", 2)
        print("\n\n".join(f"<!-- fields:{n} -->\n{schema.markdown(n)}\n<!-- /fields -->" for n in tabs))
    elif cmd == "get":
        name = me if args[0] == "." else args[0]
        if name not in MACHINES:
            return gone(name)
        try:
            print(_value(name, args[1]))
        except FieldError as e:
            return err(str(e), 1)
    elif cmd == "names":
        print("\n".join(names(args[0] if args else None)))
    elif cmd == "backups":
        print("\n".join(backups()))
    elif cmd == "q-env":
        gs = groups(me) if me else {}
        if args[0] not in gs:
            return err(f"no group {args[0]} on {me or 'this machine'} (have: {', '.join(gs) or 'none'})", 2)
        _, gpu, env = gs[args[0]]
        w = next((w for w in workers(me) if w["group"] == args[0]), None)
        env = {**(worker_env(w) if w else {"CUDA_VISIBLE_DEVICES": gpu or ""}), **env}
        for k, v in env.items():
            print(f"export {k}=${{{k}:-{v}}}" if k != "CUDA_VISIBLE_DEVICES" else f"export {k}={v}")
    elif cmd == "activate":
        name = me if len(args) < 2 or args[1] == "." else args[1]
        if name not in MACHINES:
            return gone(name)
        a = activate(name, args[0])
        if a is None:
            return err(f"no env {args[0]} on {name} (have: {', '.join(machine(name)['envs'] or {}) or 'none'})", EXIT["gone"])
        print(a)
    elif cmd == "groups":
        name = me if not args or args[0] == "." else args[0]
        if name not in MACHINES:
            return gone(name)
        for g, (n, _, _) in groups(name).items():
            print(f"{g}:{n}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
