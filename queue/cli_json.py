#!/usr/bin/env python3
"""Read-only JSON commands: tests and scripts read config-derived values without importing core modules.
One JSON line with version; fields are proto MSHOW_*, EXP_STEPS_*, RELAY_* (add-only); --json is optional.
Errors: bad spec or plugin check exit 1 on stderr, bad args 2, not in a table 4 (proto.EXIT["gone"]).
"""
import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
for d in ("sites", "monitor"):
    sys.path.insert(0, os.path.join(CORE, d))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import proto  # noqa: E402


def _line(obj):
    print(json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str))


def _fail(msg, rc=1):
    print(msg, file=sys.stderr)
    return rc


# ---------------- taskq machines show ----------------
def machine_json(n):
    import pueue as PQ
    import remote as R
    import spec as S
    M = cfg.mtab()
    m = M.machine(n)
    ws = M.workers(n)
    cmds = {w["id"]: PQ.worker_cmd(n, w) for w in ws if w["group"]} if m["scheduler"] == "pueue" and m["root"] else {}
    return dict(name=n, label=m["label"], scheduler=m["scheduler"], roles=list(M.roles(n)), executor=M.is_exec(n),
                paused=M.paused(n), kinds=list(M.kinds(n)), workers=ws, worker_cmds=cmds, groups=M.groups(n),
                root=m["root"], root_q=R.q(m["root"]) if m["root"] else None, ready_envs=sorted(M.ready_envs(n)),
                activate={e: M.activate(n, e) for e in m["envs"] or {}}, task_sh=S.machine_vars(m))


def spec_json(env):
    import envs as EV
    M = cfg.mtab()
    try:
        sp = EV.load(env)
    except Exception as e:  # noqa: BLE001  cannot read the description (missing field, not YAML, yaml not installed)
        return dict(hash=M.spec_hash(env), base=None, torch={}, error=f"{type(e).__name__}: {e}")
    torch = {k: (v or {}).get("index_url") for k, v in (sp.get("torch") or {}).items()}
    return dict(hash=M.spec_hash(env), base=sp.get("base"), torch=torch, error=None)


def machines_show(argv):
    ap = argparse.ArgumentParser(prog="taskq machines show", description="values the core computes from the machine table (one JSON line, proto.MSHOW_*)")
    ap.add_argument("names", nargs="*", metavar="NAME", help="only these machines (default: all in the machine table)")
    ap.add_argument("--json", action="store_true", help="one JSON line (this is the default)")
    a = ap.parse_args(argv)
    import glob
    M = cfg.mtab()
    every = list(M.MACHINES)
    bad = [n for n in a.names if n not in every]
    if bad:
        return _fail(f"not in the machine table: {', '.join(bad)} (have: {', '.join(every)})", proto.EXIT["gone"])
    specs = sorted(os.path.basename(p)[:-4] for p in glob.glob(os.path.join(M.SPECS, "*.yml")))
    _line(dict(version=proto.VERSION, hub=M.hub(), machines={n: machine_json(n) for n in a.names or every},
               specs={s: spec_json(s) for s in specs}))
    return 0


# ---------------- taskq exp steps ----------------
LEVELS = ("task", "group", "final")


def exp_steps(argv):
    ap = argparse.ArgumentParser(prog="taskq exp steps", description="experiment table steps with commands filled from names (one JSON line, proto.EXP_STEPS_*)")
    ap.add_argument("--exp", nargs="+", metavar="ID", help="only these experiments (FINAL is not listed)")
    ap.add_argument("--key", nargs="+", metavar="KEY", help="for per-task steps, only these tasks (experiment-level steps and FINAL are not listed)")
    ap.add_argument("--level", nargs="+", choices=LEVELS, help="only these levels: task (per task), group (experiment level), final")
    ap.add_argument("--data", help="{data} in commands (default: this machine's data dir; left as {data} when absent)")
    ap.add_argument("--done", action="store_true", help="check under --data whether each step is done (all done files under the first output exist)")
    ap.add_argument("--needs", action="store_true", help="really run steps with inputs_cmd and list the inputs they print")
    ap.add_argument("--json", action="store_true", help="one JSON line (this is the default)")
    a = ap.parse_args(argv)
    import experiments as E
    import spec as S
    from flow import compile as FC
    from flow.model import EACH, GATHER, GLOBAL
    data = a.data or S.local_data()
    if (a.done or a.needs) and not data:
        return _fail("--done and --needs need the data dir: pass --data, or set TASKQ_DATA")
    data = os.path.abspath(os.path.expanduser(data)) if data else "{data}"
    levels = set(a.level or LEVELS)
    if a.key:
        levels &= {"task"}
    level = {EACH: "task", GATHER: "group", GLOBAL: "final"}
    try:
        bad = [x for x in a.exp or [] if E.experiment(x) is None]
        bad += [k for k in a.key or [] if k not in E.tasks()]
        if bad:
            return _fail(f"not in the experiment table: {', '.join(bad)}", proto.EXIT["gone"])
        flow = FC.build()
    except S.SpecError as err:
        return _fail(str(err))
    out = []
    for i in flow.instances.values():
        lv = level.get(i.scope)
        if lv not in levels or (a.exp and i.exp not in a.exp) or (a.key and i.task not in a.key):
            continue
        out.append(step_row(i, lv, a.done, data))
    if a.needs:
        need_rows(out, [flow.instances[r["id"]] for r in out], data)
    _line(dict(version=proto.VERSION, data=data, steps=out, errors=flow.errors))
    return 0


def _fill_data(text, names, data):
    import spec as S
    return S.fill(S.fill(text, names), dict(data=data, repo=cfg.REPO), keep=())


def step_row(i, lv, done, data):
    import experiments as E
    import site_ops as SO
    st = i.step
    row = dict(level=lv, exp=i.exp or None, key=i.task or None, id=i.id, name=st["name"], cmd=None, names=E.jsonable(i.names),
               out=list(i.outputs), inputs=list(i.inputs), done=None, needs=None, error=None)
    try:
        row["cmd"] = _fill_data(st["cmd"], i.names, data)
    except Exception as e:  # noqa: BLE001
        row["error"] = f"cannot fill cmd: {e}"
    if done:
        row["done"] = not SO.done_missing(data, i.outputs[0], [d.split("|") for d in st["done"]])
    return row


def need_rows(rows, insts, data):
    """Steps with inputs_cmd: really run once (spec.listed_inputs, {data} = data); needs = the inputs it prints."""
    import spec as S

    def one(x):
        r, i = x
        try:
            r["needs"] = S.listed_inputs(S.fill(i.step["inputs_cmd"], i.names), data, i.step.get("vars"))
        except Exception as e:  # noqa: BLE001
            r["error"] = str(e)
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(one, [(r, i) for r, i in zip(rows, insts) if i.step.get("inputs_cmd")]))


# ---------------- taskq spec export / import ----------------
def _spec_arg(text):
    s = json.loads(sys.stdin.read() if text == "-" else text)
    if not isinstance(s, dict) or "key" not in s:
        raise ValueError("--spec must be a resolved spec (the output of taskq spec resolve)")
    return s


def spec_relay(argv, which):
    ap = argparse.ArgumentParser(prog=f"taskq spec {which}",
                                 description="relay plugin " + ("export (read-only)" if which == "export" else "import check, places files in --dst") + " (one JSON line)")
    ap.add_argument("--spec", required=True, help="resolved spec (JSON; - = stdin)")
    ap.add_argument("--root", required=True, help="hub data dir")
    if which == "import":
        ap.add_argument("--src", required=True, help="fetched files (real relative paths)")
        ap.add_argument("--dst", required=True, help="checked files go here")
    ap.add_argument("--json", action="store_true", help="one JSON line (this is the default)")
    a = ap.parse_args(argv)
    import names as N
    import spec as S
    try:
        sp = _spec_arg(a.spec)
    except ValueError as e:  # includes JSONDecodeError
        ap.error(f"--spec: {e}")
    try:
        if which == "export":
            ex = N.export(sp, a.root)
            _line(dict(version=proto.VERSION, plugin=N.plugin_of(sp), params=ex["params"], files=[list(f) for f in ex["files"]],
                       texts=ex["texts"], views=ex["views"], outs=ex["outs"], cleanup=ex["cleanup"], logs=S.outputs(sp)["logs"]))
        else:
            _line(dict(version=proto.VERSION, written=N.import_(sp, a.src, a.dst, a.root)))
    except (N.RelayError, S.SpecError, ValueError, KeyError) as e:
        return _fail(f"{type(e).__name__}: {e}")
    return 0


CMDS = {"machines-show": machines_show, "exp-steps": exp_steps,
        "spec-export": lambda argv: spec_relay(argv, "export"), "spec-import": lambda argv: spec_relay(argv, "import")}


def main(argv):
    if not argv or argv[0] not in CMDS:
        print(__doc__, file=sys.stderr)
        return 2
    return CMDS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
