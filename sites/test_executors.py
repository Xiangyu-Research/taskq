#!/usr/bin/env python3
"""Tests for sites/executors.py: the registry of executor types, the built-in pueue and slurm types giving what the core computed
before types were pluggable (workers, groups, status read and parse, bundles in use, env build launch, table checks), and a
project plugin adding a type through TASKQ_PLUGINS (picked up by the registry, mtab and taskq check)."""
import json
import os
import shlex
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(CORE, "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
import executors as EX  # noqa: E402
import pueue as PQ  # noqa: E402
import slurm as SL  # noqa: E402
import mtab as M  # noqa: E402
import envs as EV  # noqa: E402
import proto  # noqa: E402
import schema  # noqa: E402


def ok(msg):
    print(f"PASS:{msg}")


# the core's per-type branches before executor types (references for the built-ins)
def old_workers(name):
    m = M.machine(name)
    out = []
    if m.get("scheduler") == "slurm":
        for k, o in (m["jobs"] or {}).items():
            out.append(dict(name=k, id=f"{name}-{k}", kind=k, group=None, gpu=None, cpus=o.get("cpus"),
                            slots=o.get("slots") or 1, hours=(), idle=None, wait=None))
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
    return out


def old_groups(name):
    m = M.machine(name)
    out = {}
    for w in old_workers(name):
        if w["group"]:
            out[w["group"]] = (1, w["gpu"], {})
    if m.get("scheduler") == "pueue" or {"hub", "store"} & set(M.roles(name)):
        out[proto.IO_GROUP] = (0, None, {})
    out.update(m["manual_groups"] or {})
    return out


def old_launch(name):
    m = M.machine(name)
    if m["scheduler"] == "slurm":
        job = m.get("env_job") or m.get("transfer_job")
        args = shlex.split(job)
        timed = any(a in ("-t", "--time") or a.startswith("--time=") for a in args)
        return f"srun {job}" + ("" if timed else f" -t {EV.BUILD_MIN}")
    return f"timeout -k 60 {EV.BUILD_MIN}m nice -n 10 env CUDA_VISIBLE_DEVICES="


def registry():
    assert list(EX.BUILTIN) == ["pueue", "slurm"] and set(EX.types()) >= {"pueue", "slurm"}
    assert EX.get("pueue") is PQ and EX.get("slurm") is SL and EX.get(None) is None and EX.get("nope") is None
    assert EX.of("site-pueue") is PQ and EX.of("site-slurm") is SL and EX.of("site-slurm2") is SL and EX.of("hub") is None
    assert EX.missing(PQ) == [] and EX.missing(SL) == [] and EX.resident() == ["pueue"]
    old = M.MACHINES["site-pueue"]["scheduler"]
    M.MACHINES["site-pueue"]["scheduler"] = "nope"
    try:
        EX.of("site-pueue")
        raise AssertionError("an unregistered type must not pass")
    except LookupError as e:
        assert "'nope'" in str(e) and "pueue, slurm" in str(e), e
    finally:
        M.MACHINES["site-pueue"]["scheduler"] = old
    ok("registry: built-in pueue and slurm, of(name) by the table's scheduler (None without one, LookupError for an unregistered one), "
       "both provide the whole interface")


def same_as_before():
    for n in M.MACHINES:
        assert M.workers(n) == old_workers(n), n
        assert M.groups(n) == old_groups(n) and list(M.groups(n)) == list(old_groups(n)), n
        if M.is_exec(n):
            assert f"{{ {old_launch(n)} bash " in EV.runner(n)[0] and EV.runner(n)[1] == M.machine(n).get("ssh"), n
    ok("mtab workers and groups, and the env build launch, are what the core computed before for every test machine")
    assert PQ.status_args("site-pueue") == dict(pueue=M.machine("site-pueue")["pueue"]) and SL.status_args("site-slurm") == {}
    lab = proto.WORKER_LABEL + "site-pueue-gpu1"
    raw = json.dumps(dict(tasks={"3": dict(group="gpu1", label=lab, status=dict(Done=dict(result="Success"))),
                                 "4": dict(group="other", label="x", status="Running")}))
    assert PQ.parse_status("site-pueue", raw) == dict(jobs=PQ.parse(raw, [w["group"] for w in M.workers("site-pueue")])) \
        and PQ.parse_status("site-pueue", raw)["jobs"] == [[3, lab, "Done", "Success"]]
    assert SL.parse_status("site-slurm", "101|w|RUNNING|1:00|long|n1\n") == dict(squeue=[["101", "w", "RUNNING", "1:00", "long", "n1"]])
    ok("status: pueue sends its pueue path and parses jobs of its worker groups; Slurm sends nothing more and parses squeue rows")
    assert PQ.bundles("b1", ["b0"]) == ["b1"] and SL.bundles("b1", ["b0", "b2"]) == ["b1", "b0", "b2"] and SL.bundles("b1", None) == ["b1"]
    assert PQ.describe(M.workers("site-pueue")[0]) == "start site-pueue-gpu1"
    assert SL.describe(("long", "1:00:00", 3, "gpu")) == "submit 3 worker to long" and SL.describe(("normal", "1", 1, "cpu")) == "submit 1 CPU worker to normal"
    assert PQ.worker_cmd("site-pueue", M.workers("site-pueue")[0]).startswith(M.machine("site-pueue")["pueue"] + " add -g gpu1 ")
    assert SL.worker_cmd("site-slurm", M.workers("site-slurm")[0]) is None
    ok("bundles in use (pueue: current; Slurm: current plus live), plan descriptions, worker start commands")
    with_gpu = dict(M.MACHINES["site-slurm"], env_job="-p gpu-part --gres=gpu:1")
    for m, want in ((dict(M.MACHINES["site-slurm"], env_job=None, transfer_job=None), "site-slurm's entry has no env_job (the CPU partition's srun options)"),
                    (with_gpu, "site-slurm's env_job asks for a GPU: -p gpu-part --gres=gpu:1")):
        old = M.MACHINES["site-slurm"]
        M.MACHINES["site-slurm"] = m
        try:
            EV.runner("site-slurm")
            raise AssertionError("must fail")
        except EV.EnvError as e:
            assert str(e) == want, e
        finally:
            M.MACHINES["site-slurm"] = old
    assert PQ.env_launch("site-pueue", M.machine("site-pueue"), 5) is None
    ok("env build: a Slurm entry without env_job or asking for a GPU is the same EnvError as before; pueue uses the local form")
    assert PQ.check("x", dict(scheduler="pueue")) == ["a pueue executor needs workers"] and PQ.check("x", dict(workers=[{}])) == []
    assert SL.check("x", dict(workers=[{}], job_names="zz")) == [
        "a slurm executor needs jobs", "a slurm executor does not write workers (it submits jobs from jobs)",
        f"job_names 'zz' is not in slurm.STYLES ({sorted(SL.STYLES)})"]
    assert SL.check("x", dict(jobs=dict(gpu={}), job_names="star")) == []
    ok("table checks: pueue needs workers; Slurm needs jobs, writes no workers, job_names in STYLES")


def read_status():
    """read_status runs on the executor: pueue status --json from the given path (~ expanded), squeue -u $USER with the fixed format."""
    d = fixture.tmpdir()
    fb = os.path.join(d, "bin")
    os.makedirs(fb)
    for n, out in (("pueue", '{"tasks": {}}'), ("squeue", "101|w|RUNNING|1:00|long|n1")):
        with open(os.path.join(fb, n), "w") as f:
            f.write(f'#!/bin/bash\nprintf "%s\\n" "$@" > "$HOME/{n}.argv"\necho \'{out}\'\n')
        os.chmod(os.path.join(fb, n), 0o755)
    old = {k: os.environ.get(k) for k in ("HOME", "PATH", "USER")}
    os.environ.update(HOME=d, PATH=fb + os.pathsep + os.environ["PATH"], USER="someone")
    try:
        assert PQ.read_status(dict(pueue="~/bin/pueue")) == '{"tasks": {}}\n'
        assert open(os.path.join(d, "pueue.argv")).read().split() == ["status", "--json"]
        assert SL.read_status({}) == "101|w|RUNNING|1:00|long|n1\n"
        assert open(os.path.join(d, "squeue.argv")).read().split("\n")[:4] == ["-u", "someone", "-h", "-o"]
        assert open(os.path.join(d, "squeue.argv")).read().split("\n")[4] == "%i|%j|%T|%M|%q|%R"
        assert PQ.read_status(dict(pueue="/bin/false")) is None
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    ok("read_status: pueue status --json (a leading ~ in the path expanded there), squeue -u $USER -h -o '%i|%j|%T|%M|%q|%R'; failing → None")


TOY = '''import sys

RESIDENT = True


def check(name, m):
    return [] if m.get("workers") else ["a toy executor needs workers"]


def workers(name, m):
    return [dict(name=f"w{i}", id=f"{name}-w{i}", kind="cpu", group=None, gpu=None, cpus=1, slots=1, hours=(), idle=None, wait=None)
            for i, _ in enumerate(m["workers"] or ())]


def groups(name, ws):
    return {"toyq": (2, None, {})}


def status_args(name):
    return {}


def read_status(A):
    return "toy"


def parse_status(name, raw):
    return dict(jobs=[])


def plan(name, rs, run):
    return [], "toy", {}


def describe(sub):
    return "toy"


def start(name, subs, run):
    return [], {}


def bundles(cur, live):
    return [cur]


def env_launch(name, m, minutes):
    return None


def worker_cmd(name, w):
    return None


EXECUTORS = {"toy": sys.modules[__name__]}
'''


def plugin_type():
    """A project plugin executors.py adds a type: the registry, mtab and taskq check see it; a type lacking interface names, a
    built-in name, and a scheduler that is not a registered type are reported."""
    base = fixture.tmpdir()
    plug, conf = os.path.join(base, "plugins"), os.path.join(base, "config")
    os.makedirs(plug)
    shutil.copytree(fixture.CONFIG, conf, ignore=shutil.ignore_patterns("__pycache__"))
    with open(os.path.join(conf, "machines.py"), "a") as f:
        f.write('\nMACHINES["site-toy"] = dict(label="toy", hostname=r"toy-host", ssh="fake-toy", scheduler="toy", root="~/toy", workers=[dict(cpus=1)])\n')

    def write(text):
        with open(os.path.join(plug, "executors.py"), "w") as f:
            f.write(text)

    env = dict(os.environ, TASKQ_CONFIG=conf, TASKQ_PLUGINS=fixture.PLUGINS + os.pathsep + plug)

    def py(code):
        r = subprocess.run([sys.executable, "-c", f"import sys; sys.path.insert(0, {HERE!r}); {code}"], capture_output=True, text=True, env=env)
        assert r.returncode == 0, r.stderr
        return r.stdout.strip()

    def check():
        r = subprocess.run([sys.executable, os.path.join(CORE, "queue", "check.py"), "--only", "machines", "plugins"],
                           capture_output=True, text=True, env=env)
        return r.returncode, r.stdout + r.stderr

    write(TOY)
    got = py("import executors as X, mtab as M; print(sorted(X.types()), X.of('site-toy').describe(None), M.workers('site-toy')[0]['id'], "
             "sorted(M.groups('site-toy')), X.missing(X.of('site-toy')), X.resident())")
    assert got == "['pueue', 'slurm', 'toy'] toy site-toy-w0 ['toyq'] [] ['pueue', 'toy']", got
    rc, out = check()
    assert "site-toy" not in out and "executors" not in out, out
    ok("plugin type: EXECUTORS in the project's executors.py joins the registry; mtab takes its workers and groups; taskq check accepts the machine")

    write(TOY.replace("def worker_cmd(name, w):\n    return None\n", ""))
    rc, out = check()
    assert rc == 1 and "executor type toy from the executors plugin lacks worker_cmd" in out, out
    write(TOY.replace('EXECUTORS = {"toy"', 'EXECUTORS = {"pueue"'))
    rc, out = check()
    assert rc == 1 and "merging the executors plugin into the core failed" in out and "duplicate pueue" in out, out
    write(TOY.replace('EXECUTORS = {"toy"', 'EXECUTORS = {"toy2"'))
    rc, out = check()
    assert rc == 1 and "MACHINES['site-toy']: scheduler must be pueue, slurm, toy2 or None, got 'toy'" in out, out
    ok("plugin type: a type lacking interface names, one reusing a built-in name, and a scheduler no type registers are reported by taskq check")


def main():
    registry()
    same_as_before()
    read_status()
    plugin_type()
    print("all passed")


if __name__ == "__main__":
    main()
