#!/usr/bin/env python3
"""Tests for refill.py: sending tasks (choose: pick an executor by capacity; places, holders, progress, data_sizes) and the
Slurm top-up rules (topup_plan, cpu_plan, slurm_plan), using fake queue state and contacting no machine.

    python3 queue/test_refill.py
"""
import copy
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, HERE)
os.environ["TASKQ_TASKS"] = os.path.join(fixture.tmpdir(), "tasks.json")  # do not touch the real task records
os.environ["TASKQ_LAST"] = os.path.join(fixture.tmpdir(), "last")
import refill as R  # noqa: E402

fails = 0
D = fixture.tmpdir()
R.C.run = lambda *a, **k: (_ for _ in ()).throw(AssertionError("tests must not run commands on real machines"))
NN, UM = "V_save", "V_m"
DS = {101: "D101_alpha", 103: "D103_alpha_one", 111: "D111_beta"}
PP = "prepared/"


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def tq(key, nice, order, state="free", worker=None, skipped_by=()):
    return dict(key=key, nice=nice, order=order, state=state, worker=worker, kind="-", host=worker, skipped_by=list(skipped_by))


K_T14 = f"D121_alphagamma/{NN}/0"   # ① but 62 GB of data does not fit
K_T1C = [f"D103_alpha_one/{NN}/{f}" for f in range(3)]
K_UM = f"D101_alpha/{UM}/1"
QUEUE = [tq(K_T14, 0, 1), tq(K_UM, 50, 2), tq(K_T1C[0], 50, 3), tq(K_T1C[1], 50, 4), tq(K_T1C[2], 100, 5)]


def snap(queue=QUEUE, enabled=True):
    return dict(ok={"hub": True, "site-slurm": True}, src={"hub": {"tasks": {}}, "site-slurm": {"tasks": {}}},
                taskq={"hub": dict(enabled=enabled, tasks=copy.deepcopy(list(queue)), recent_skips={})}, pueue={"hub": {"tasks": []}})


# ---------------- Slurm top-up ----------------
QOS = {"long": "1-12:00:00", "normal": "08:00:00"}
p, why = R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 216, 0), "normal": (10, 138, 0)}, QOS)
ok("now: old jobs far over the cap, add none", p == [] and "full" in why)
p, _ = R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 9, 0), "normal": (10, 3, 0)}, QOS)
ok("long has 1 free, normal 7: fill the quotas with long 1, normal 7", p == [("long", "1-12:00:00", 1, "gpu"), ("normal", "08:00:00", 7, "gpu")])
p, _ = R.topup_plan(snap()["taskq"]["hub"], {"long": (None, 4, 0), "normal": (10, 10, 0)}, QOS)
ok("QOS has no queue cap: use SUBMIT_CAP", p == [("long", "1-12:00:00", R.SL.SUBMIT_CAP - 4, "gpu")])
p, _ = R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 6, 0)}, {"long": ("3-00:00:00", 2)})
ok("reserve 2 slots: cap 10, 6 present, add 2", p == [("long", "3-00:00:00", 2, "gpu")])
p, why = R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 8, 0)}, {"long": ("3-00:00:00", 2)})
ok("reserve 2 slots: 8 present, add none", p == [] and "full" in why)
done_q = [tq(K_T14, 0, 1, "done"), tq(K_UM, 50, 2, "claimed", "486191")]
ok("nothing unclaimed but some running: still fill (accumulate queue time)", R.topup_plan(snap(queue=done_q)["taskq"]["hub"], {"long": (10, 2, 0), "normal": (10, 0, 0)}, QOS)[0]
   == [("long", "1-12:00:00", 8, "gpu"), ("normal", "08:00:00", 10, "gpu")])
ok("no unclaimed and none running: add none", R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "done")])["taskq"]["hub"], {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
ok("stale claims count as unclaimed", R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "stale")])["taskq"]["hub"], {"long": (10, 0, 0), "normal": (10, 10, 0)}, QOS)[0] != [])
ok("main switch off: add none", R.topup_plan(snap(enabled=False)["taskq"]["hub"], {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
paused = snap()["taskq"]["hub"] | {"paused": True}
ok("queue paused: add none", R.topup_plan(paused, {"long": (10, 0, 0), "normal": (10, 0, 0)}, QOS)[0] == [])
ok("cannot read one QOS's job count: add none", R.topup_plan(snap()["taskq"]["hub"], {"long": (10, 0, 0)}, QOS)[0] == [])
# fill the quota
two = [tq(K_T14, 0, 1), tq(K_UM, 50, 2), tq(K_T1C[0], 50, 3, "claimed", "486191")]
p, _ = R.topup_plan(snap(queue=two)["taskq"]["hub"], {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 0)})
ok("2 prepared, 10 free slots: add 10", p == [("long", "3-00:00:00", 10, "gpu")])
p, why = R.topup_plan(snap(queue=two)["taskq"]["hub"], {"long": (10, 4, 2)}, {"long": ("3-00:00:00", 1)})
ok("reserve 1, 4 present (2 queued): add 5", p == [("long", "3-00:00:00", 5, "gpu")])
p, why = R.topup_plan(snap(queue=[tq(K_T14, 0, 1, "done")])["taskq"]["hub"], {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 0)})
ok("no GPU tasks: add none", p == [] and "no GPU tasks" in why)
ok("slurm.parse: needs 5 fields, one short counts as unreadable", R.SL.parse("QOS|long|10|3|1\nQOS|normal|10|2\n")["counts"] == {"long": (10, 3, 1)})
ok("duplicates of sites/slurm.py removed from refill", not any(hasattr(R, f) for f in ("topup_probe", "parse_qos")))

# CPU-only tasks (device=cpu): GPU workers are not topped up for them; CPU workers only when such tasks exist
K_EV = "eval/D101_alpha/x/frag_0"
ev_spec = dict(key=K_EV, env="base", machines=["site-slurm"], device="cpu")
cpu_q = [tq(K_T14, 0, 1, "done"), dict(tq(K_EV, -100, 2), spec=ev_spec), dict(tq(K_EV + "b", -100, 3), spec=dict(ev_spec, key=K_EV + "b"))]
cq = snap(queue=cpu_q)["taskq"]["hub"]
p, why = R.topup_plan(cq, {"long": (10, 0, 0)}, {"long": ("3-00:00:00", 2)})
ok("only unclaimed CPU-only tasks: no GPU worker top-up", p == [] and "GPU tasks" in why)
ok("2 CPU-only tasks, max 1, 0 present: add 1 CPU worker", R.cpu_plan(cq, 0, ("normal", "08:00:00", 1), 5) == ([("normal", "08:00:00", 1, "cpu")], ""))
ok("max 3: add 2, one per task", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 5)[0] == [("normal", "08:00:00", 2, "cpu")])
ok("1 CPU worker already: add none", R.cpu_plan(cq, 1, ("normal", "08:00:00", 1), 5)[0] == [])
ok("cannot read CPU worker job count: add none", R.cpu_plan(cq, None, ("normal", "08:00:00", 1), 5)[0] == [])
ok("no CPU-only tasks: add none", R.cpu_plan(snap()["taskq"]["hub"], 0, ("normal", "08:00:00", 1), 5)[0] == [])
ok("queue paused: add none", R.cpu_plan(cq | {"paused": True}, 0, ("normal", "08:00:00", 1), 5)[0] == [])
ok("max 3, only 1 submit slot left: add 1", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 1)[0] == [("normal", "08:00:00", 1, "cpu")])
ok("no submit slots left: add none", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), 0) == ([], "no submit slots left"))
ok("cannot read slots left: add none", R.cpu_plan(cq, 0, ("normal", "08:00:00", 3), None)[0] == [])

CONF = dict(gpu={"long": ("3-00:00:00", 2)}, cpu=("normal", "08:00:00", 1))
ST0 = dict(counts={"long": (10, 0, 0)}, cpu=0, free=5)
for q_ in (cq, snap()["taskq"]["hub"]):
    p, why = R.slurm_plan(q_, ST0, CONF)
    g, gwhy = R.topup_plan(q_, ST0["counts"], CONF["gpu"])
    c, cwhy = R.cpu_plan(q_, 0, CONF["cpu"], 5)
    ok(f"slurm_plan = topup_plan + cpu_plan ({p})", p == g + c and why == (gwhy if c else "; ".join(x for x in (gwhy, f"CPU: {cwhy}") if x)))
p, why = R.slurm_plan(cq, ST0, dict(cpu=CONF["cpu"]))
ok("slurm_plan: a machine with only cpu quota adds CPU workers only", p == [("normal", "08:00:00", 1, "cpu")] and why == "")
p, why = R.slurm_plan(snap()["taskq"]["hub"], ST0, dict(cpu=CONF["cpu"]))
ok("slurm_plan: no CPU-only tasks, say why", p == [] and why == "CPU: no unclaimed CPU-only tasks")


# ---------------- sending tasks: choose ----------------
def site(name, room, **kw):
    kw.setdefault("envs", ("base", "stats"))
    kw.setdefault("root", ((name, "cap"), 1000.0))
    return R.Site(name, room, **kw)


def ctq(key, nice=50, order=1, **sp):
    return dict(tq(key, nice, order, skipped_by=sp.pop("skipped_by", ())), spec=dict(dict(key=key, env="base"), **sp))


def pick(tasks, sites, **kw):
    specs = {t["key"]: ("demo", t["spec"]) for t in tasks}
    res = R.choose(tasks, {s_.name: s_ for s_ in sites}, specs, **kw)
    got = {t["key"]: s_.name for s_ in sites for t in s_.take}
    return got, res


ok("places: a step with no entry = executors with this worker kind, in machine-table order", R.places("demo", {}) == ["site-pueue", "site-slurm", "site-slurm2"])
ok("places: CPU-only goes only to executors with CPU workers", R.places("demo", dict(device="cpu")) == ["site-pueue", "site-slurm"])
ok("places: PLACE entries are followed (infer: slurm first)", R.places("x", dict(step="infer")) == ["site-slurm", "site-pueue"])
ok("places: then intersected with the spec's machines", R.places("x", dict(step="infer", machines=["site-pueue"])) == ["site-pueue"])
four = [ctq(f"c/{i}", order=i) for i in range(4)]
got, res = pick(four, [site("site-pueue", dict(gpu=2)), site("site-slurm", dict(gpu=1)), site("site-slurm2", dict(gpu=1))])
ok("two or three can take it: split by free slots (fill the preferred one first, then the next)",
   got == {"c/0": "site-pueue", "c/1": "site-pueue", "c/2": "site-slurm", "c/3": "site-slurm2"} and res == dict(why={}, nowhere=[], noroom=[]))
got, res = pick(four, [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=1))])
ok("not enough slots: the rest are not sent this round, say why for each", len(got) == 2 and set(res["why"]) == {"c/2", "c/3"}
   and "site-pueue: full" in res["why"]["c/2"] and "site-slurm: full" in res["why"]["c/2"] and res["noroom"] == [])
got, res = pick([ctq("s/1", skipped_by=["site-pueue"])], [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=1))])
ok("skipped by one: send to another that can take it", got == {"s/1": "site-slurm"})
got, res = pick([ctq("s/2", skipped_by=["site-pueue", "site-slurm", "site-slurm2"])], [site("site-pueue", dict(gpu=1))])
ok("every candidate skipped it: not sent, recorded in nowhere (relay alerts)", got == {} and res["nowhere"] == ["s/2"])
got, res = pick([ctq("s/3", step="p1", skipped_by=["site-slurm"])], [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=1))])
ok("PLACE allows one only and it skipped: also nowhere, do not go elsewhere", got == {} and res["nowhere"] == ["s/3"])
ts = [ctq("h/1", order=2), ctq("h/2", order=1)]
got, res = pick(ts, [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=1))], held={"h/1": ["site-slurm"]})
ok("half-done: only to the executor with progress, first within its nice", got == {"h/1": "site-slurm", "h/2": "site-pueue"})
got, res = pick([ctq("h/3")], [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=0))], held={"h/3": ["site-slurm"]})
ok("half-done, that executor full: wait for it, do not start over elsewhere", got == {} and "site-slurm: full" in res["why"]["h/3"])
got, res = pick([ctq("h/4")], [site("site-pueue", dict(gpu=1))], held={"h/4": ["site-slurm"]})
ok("half-done, that executor unreachable this round: not sent, name it", got == {} and "site-slurm" in res["why"]["h/4"])
got, _ = pick([ctq("i/1", inputs=["in/a"])], [site("site-pueue", dict(gpu=1)), site("site-slurm2", dict(gpu=1), ready=["in/a"])],
              sizes={"in/a": 5.0})
ok("inputs already there first (whatever the preference)", got == {"i/1": "site-slurm2"})
got, res = pick([ctq("i/2", inputs=["in/b"])], [site("site-pueue", dict(gpu=1))], sizes={"in/b": None})
ok("input not on the store: not sent, say which is missing", got == {} and "input not on the store: in/b" in res["why"]["i/2"])
# space: after inputs + outputs (out_gb, default proto.OUT_GB) at least keep remains; executors on one disk share the budget
disk = (("fake", "dev1"), 50.0)
got, res = pick([ctq("g/1", inputs=["in/c"]), ctq("g/2", order=2, inputs=["in/d"])],
                [site("site-pueue", dict(gpu=5), root=disk, keep=15), site("site-slurm", dict(gpu=5), root=disk, keep=15)],
                sizes={"in/c": 20.0, "in/d": 20.0})
ok("space: the first fits (50-20-10>=15), the second on the same disk fits nowhere (recorded in noroom: relay reports tqroom; merely full is not recorded)",
   got == {"g/1": "site-pueue"} and R.NOROOM in res["why"]["g/2"]
   and f"site-slurm: {R.NOROOM}" in res["why"]["g/2"] and res["noroom"] == ["g/2"])
got, _ = pick([ctq("g/3", inputs=["in/c"], out_gb=1)], [site("site-pueue", dict(gpu=1), root=(("a", 1), 30.0), keep=5)],
              sizes={"in/c": 20.0})
ok("space: the spec's out_gb is used when set", got == {"g/3": "site-pueue"})
got, res = pick([ctq("g/4", inputs=["in/c"], out_gb=1)],
                [site("site-pueue", dict(gpu=1), root=(("a", 1), 100.0), blob=(("a", 2), 10.0), keep=5)], sizes={"in/c": 20.0})
ok("space: when blobs are on another disk, inputs are charged to blobs", got == {} and R.NOROOM in res["why"]["g/4"])
idle = {"gpu": [dict(key="old/1", nice=100, out=10.0), dict(key="old/2", nice=100, out=10.0)]}
sp_ = site("site-pueue", dict(gpu=0), idle=idle)
got, res = pick([ctq("w/1", nice=50), ctq("w/2", nice=50, order=2)], [sp_])
ok("when full: a more important task swaps in for a prepared unclaimed one (one per kind per round)", got == {"w/1": "site-pueue"}
   and [(a["key"], b["key"]) for a, b in sp_.swaps] == [("old/1", "w/1")] and "full" in res["why"]["w/2"])
got, _ = pick([ctq("n/1", nice=-1), ctq("n/2", nice=-1, order=2)], [site("site-pueue", dict(gpu=0))])
ok("nice < 0 does not take a slot", got == {"n/1": "site-pueue", "n/2": "site-pueue"})
got, res = pick([ctq("e/1", env="stats")], [site("site-pueue", dict(gpu=1), envs=("base",)), site("site-slurm", dict(gpu=1))])
ok("executor without the env is not used, go to the next", got == {"e/1": "site-slurm"})
got, _ = pick([ctq("k/c", device="cpu")], [site("site-pueue", dict(gpu=1)), site("site-slurm", dict(gpu=1, cpu=1))])
ok("executor without this worker kind is not used", got == {"k/c": "site-slurm"})
told = []
got, res = pick([ctq("b/1")], [site("site-pueue", dict(gpu=1), block=lambda k, sp: "bundle cannot run it"),
                               site("site-slurm", dict(gpu=1), took=lambda k, sp: told.append(k))])
ok("blocked executor is not used; taking tells took", got == {"b/1": "site-slurm"} and told == ["b/1"])
ok("nice beats half-done", list(pick([ctq("o/1", nice=60), ctq("o/2", nice=50, order=2)], [site("site-pueue", dict(gpu=1))],
                                     held={"o/1": ["site-pueue"]})[0]) == ["o/2"])
ok("data_sizes: bytes measured on the store -> GB; missing or no match -> None",
   R.data_sizes(["a", "b", "c", "a"], lambda ps: {"a": dict(n=2, size=3e9), "b": dict(n=0, size=0)}) == {"a": 3.0, "b": None, "c": None})
held = R.holders({"site-pueue": {"h/c": {"ran": True}}, "site-slurm": {"h/b": {"resumable": True}, "h/d": {"ran": False}},
                  "site-slurm2": {"h/a": {"runs": [{"epochs_done": 3}]}}}, lambda n: n != "site-slurm2")
ok("holders: only executors with progress that are usable now", held == {"h/c": ["site-pueue"], "h/b": ["site-slurm"]})
ok("site_progress: ran counts as progress, empty does not", R.site_progress({"ran": True}) and not R.site_progress({}) and not R.site_progress(None))
R.SL.P.SLURM = {"site-slurm": dict(gpu={"long": ("3-00:00:00", 2)}, cpu=("normal", "08:00:00", 1))}
probe = R.SL.probe("site-slurm")
ok("job-count probe uses policy.py QOS and recognises the CPU worker by the script submitted",
   "show qos normal " in probe and "cpu_worker[.]sbatch" in probe and "for q in long;" in probe)


for f in R.SL.FILES.values():
    ok(f"{f} does not write --job-name (the job name is random at submit time)",
       not any(ln.startswith("#SBATCH --job-name") for ln in open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f))))

# files on that side unreadable (spec.collect stuck): count as progress, do not hand out as new
st = {"src": {"site-slurm": {"tasks": {}, "stuck": ["k/1"]}, "hub": {"tasks": {}}}}
ok("progress: an unreadable task counts as progress (all machines; that machine); other machines and tasks as usual",
   R.progress(st, "k/1") and R.progress(st, "k/1", "site-slurm") and not R.progress(st, "k/1", "hub") and not R.progress(st, "k/2"))
st = {"src": {"site-slurm": {"tasks": {}, "errors": {"k/1": "OSError: x"}}, "hub": {"tasks": {}}}}
ok("progress: a task that failed to read (spec.collect errors) also counts as progress", R.progress(st, "k/1", "site-slurm") and not R.progress(st, "k/1", "hub"))

print("all passed" if fails == 0 else f"{fails} failures")
sys.exit(1 if fails else 0)
