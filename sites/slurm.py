"""The Slurm adapter (machines with scheduler="slurm"): read job counts per QOS, share slots, submit worker jobs.
Like pueue.py, the caller decides how many to submit (refill.py topup_plan) and start_workers submits them; it submits
the core's queue/worker.sbatch (GPU) and cpu_worker.sbatch (CPU), with partition, cards, cpus, memory (the table's jobs),
QOS, time limit (policy.py SLURM), logs, TASKQ_ROOT and TASKQ_MACHINE on the command line; worker_start.sh is found
through code/current when the job starts, so it uses that bundle. run: fn(command) → (exit code, output)."""
import os
import secrets
import shlex
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "queue"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cfg  # noqa: E402
cfg.use_config()
import policy as P  # noqa: E402
import proto  # noqa: E402
import mtab as M  # noqa: E402
import remote as R  # noqa: E402

SUBMIT_CAP = 10  # used when a QOS's submit limit is unreadable
FILES = {"gpu": "worker.sbatch", "cpu": "cpu_worker.sbatch"}  # worker kind → sbatch file; workers are told apart by squeue's %o (the submitted script)

# Job names are random each time (different accounts and jobs may share a name); never identify jobs by name. Each
# machine uses its own set from the table's job_names
_W1 = ("amber azure bold brisk calm clear coral crisp dawn deep dry eager early fair fast fern firm fresh gentle glad gold "
       "grand green grey hazy keen kind late light lime lively lone loyal lucky mellow merry mild misty modest neat noble "
       "pale plain polar proud quick quiet rapid rare ready red rosy round royal ruby rustic sandy sharp shy silent silver "
       "simple sleek slow smooth snowy soft solar solid steady still stout sunny swift tall tidy topaz true vast warm wild wise").split()
_W2 = ("acorn alder anchor apple arch aspen badger basin beacon birch bison bloom brook cabin canyon cedar cliff clover comet "
       "coral crane creek delta dune eagle ember falcon fern finch fjord forest fox garnet glade grove harbor hawk heron hill "
       "island ivy jade lagoon lake lark laurel lynx maple marsh meadow mesa moose moss oak orchid otter owl pebble pine plover "
       "pond prairie quail raven reef ridge river robin sage shore sparrow spruce stone summit swan thicket tide trail tulip "
       "valley willow wren yew").split()


_STARS = ("acrux adhara algol alnair altair antares arcturus atlas bellatrix betelgeuse canopus capella castor deneb diphda "
          "electra enif fomalhaut gacrux hadar hamal izar kochab lyra merak mimosa mira mizar nunki orion pollux procyon rigel "
          "sabik sadr saiph schedar shaula sirius spica suhail thuban vega wezen zosma").split()
STYLES = {
    "pair": lambda: f"{secrets.choice(_W1)}-{secrets.choice(_W2)}",  # <adjective>-<noun>, e.g. late-plover
    "star": lambda: f"{secrets.choice(_STARS)}{secrets.randbelow(90) + 10}",  # <star name><two digits>, e.g. vega27
}


def jobname(style=None):
    """A random job name, by style (a STYLES key; none → pair)."""
    return STYLES[style or "pair"]()


def kind_of(command):
    """squeue's %o (the submitted script's path) → worker kind (FILES); not a worker (transfer jobs, manual jobs) → None."""
    base = (command or "").strip().rsplit("/", 1)[-1]
    return next((k for k, f in FILES.items() if base == f), None)


def probe(name):
    """One line per GPU QOS "QOS|name|submit limit|our queued and running jobs|of which still queued (not started)";
    when submitting CPU workers also "CPU|our CPU worker jobs" and "ALL|that QOS's submit limit|all our jobs" (some
    clusters put every partition on the same QOS, so every job takes a slot there); any command failing fails the whole
    probe (not counted as 0). QOS come from policy.py's SLURM; CPU workers are told apart by the submitted script
    (FILES["cpu"])."""
    conf = P.SLURM[name]
    out = "set -e; for q in " + " ".join(conf["gpu"]) + "; do lim=$(sacctmgr -nP show qos $q format=maxsubmitpu); " \
        "ids=$(squeue -u $USER -h -r -q $q -o %i); pd=$(squeue -u $USER -h -r -q $q -t PD -o %i); " \
        "echo \"QOS|$q|$lim|$(printf '%s' \"$ids\" | grep -c . || true)|$(printf '%s' \"$pd\" | grep -c . || true)\"; done"
    if conf.get("cpu"):
        out += "; cmds=$(squeue -u $USER -h -r -o %o); " \
            f"echo \"CPU|$(printf '%s\\n' \"$cmds\" | grep -cE '(^|/){FILES['cpu'].replace('.', '[.]')}$' || true)\"; " \
            f"lim=$(sacctmgr -nP show qos {conf['cpu'][0]} format=maxsubmitpu); " \
            "ids=$(squeue -u $USER -h -r -o %i); echo \"ALL|$lim|$(printf '%s' \"$ids\" | grep -c . || true)\""
    return out




def parse(out):
    """probe's output → dict(counts={QOS: (submit limit or None, our jobs, still queued)}, cpu=CPU worker jobs or None,
    free=submit slots left or None); a QOS line without numbers is dropped (that QOS counts as unreadable)."""
    counts, cpu, free = {}, None, None
    for ln in out.splitlines():
        p = ln.split("|")
        if len(p) == 5 and p[0] == "QOS" and p[3].isdigit() and p[4].isdigit():
            counts[p[1]] = (int(p[2]) if p[2].isdigit() else None, int(p[3]), int(p[4]))
        elif len(p) == 2 and p[0] == "CPU" and p[1].isdigit():
            cpu = int(p[1])
        elif len(p) == 3 and p[0] == "ALL" and p[2].isdigit():
            free = max(0, (int(p[1]) if p[1].isdigit() else SUBMIT_CAP) - int(p[2]))
    return dict(counts=counts, cpu=cpu, free=free)


def status(name, run):
    """parse's result; unreadable → None."""
    rc, out = run(probe(name))
    return None if rc else parse(out)


def squeue(text):
    """The output of `squeue -h -o '%i|%j|%T|%M|%q|%R'` → [[id, name, state, elapsed, QOS, reason], ...]."""
    return [ln.split("|") for ln in text.splitlines() if ln.strip()]


def distribute(qos, counts, room, kind):
    """Submit room more jobs, one QOS at a time in order, each up to its own limit (minus the slots kept for its owner)
    → [(QOS, time per job, how many, worker kind)]."""
    out = []
    for q, v in qos.items():
        t, keep = (v, 0) if isinstance(v, str) else v
        lim, n = counts[q][0], counts[q][1]
        k = min((lim or SUBMIT_CAP) - keep - n, room)
        if k > 0:
            out.append((q, t, k, kind))
            room -= k
    return out


def submit_cmd(name, qos, time_, kind):
    """The command that submits one kind (gpu / cpu) worker job (run over there). A leading ~ in the root is left for
    the other side to expand (assigned to R first)."""
    m = M.machine(name)
    opts = " ".join(f"--{'cpus-per-task' if k == 'cpus' else k}={shlex.quote(str(v))}" for k, v in (m["jobs"][kind] or {}).items() if v is not None)
    cur = f'"$R"/{proto.ROOT["current"]}'
    logs = f'"$R"/{proto.ROOT["data"]}/{proto.WORKER_LOGS}'
    return (f"R={R.q(m['root'].rstrip('/'))}; mkdir -p {logs} && sbatch --parsable -J {jobname(m.get('job_names'))} --qos={shlex.quote(qos)} "
            f"--time={shlex.quote(time_)} --chdir=\"$R\" --output={logs}/%x_%j.out --export=ALL,TASKQ_ROOT=\"$R\",TASKQ_MACHINE={shlex.quote(name)} "
            f"{opts} {cur}/{cfg.bundle_rel('queue', FILES[kind])} {cur}/{cfg.bundle_rel('queue', 'worker_start.sh')}")


def start_workers(name, subs, run):
    """Submit subs (refill.slurm_plan's [(QOS, time limit, how many, kind)], one random name per job from the table's
    job_names) → [dict(qos, time, n=how many, kind, ids, err)]; an error stops that QOS."""
    res = []
    for q, t, n, k in subs:
        ids, err = [], None
        for _ in range(n):
            rc, out = run(submit_cmd(name, q, t, k))
            if rc:
                err = out[-200:]
                break
            ids.append(out.strip().splitlines()[-1])
        res.append(dict(qos=q, time=t, n=n, kind=k, ids=ids, err=err))
    return res
