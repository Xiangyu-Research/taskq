#!/usr/bin/env python3
"""Test summary.py (taskq status on the hub): fixed config, a fake hub queue, relay status.json and database; no machines."""
import os
import sys
import time
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue", "testdata"))
import fixture  # noqa: E402
fixture.use()
os.environ["HOME"] = fixture.tmpdir()  # hub database and relay status.json (cfg.state) live under the temp HOME
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import summary as SU  # noqa: E402
import proto  # noqa: E402
import spec as S  # noqa: E402
import events  # noqa: E402
import db as D  # noqa: E402

NOW = datetime(2026, 10, 16, 12, 0)
fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def train(epochs, total=1000, last=NOW, per_10=600, final=False):
    """A training record as relay status.json keeps it: one run with 10-epoch points every per_10 seconds up to last."""
    tl = [[ts(last - timedelta(seconds=per_10 * i)), epochs - 10 * i] for i in range(10, -1, -1)]
    return dict(state="claimed", worker="w", runs=[dict(start=tl[0][0], end=ts(last), epochs_done=epochs, total=total, tl=tl)],
                final=final, resumable=True, ran=True, debug={}, since=(last - timedelta(hours=3)).timestamp())


keys = [t["key"] for t in S.all_tasks()]
RUN, LATE, WAIT, SEND, HELD, BACK, FAIL, FREE, DONE = keys[:9]


def q(key, state, assigned=None):
    return dict(key=key, state=state, assigned=assigned, nice=0, note="", worker=None, host=None)


queue = dict(enabled=True, tasks=[q(RUN, "claimed", "site-pueue"), q(LATE, "claimed", "site-slurm"), q(WAIT, "claimed", "site-pueue"),
                                  q(SEND, "claimed", "site-pueue"), q(HELD, "claimed", "site-slurm"), q(BACK, "claimed", "site-slurm"),
                                  q(FAIL, "failed"), q(FREE, "free"), q(DONE, "done"), q("_smoke/site-pueue/x", "claimed", "site-pueue")])
relay = {"site-pueue": dict(time=NOW.timestamp() - 600, label="pueue", tasks={RUN: train(500), WAIT: dict(state="free", runs=[])}, squeue=None, held={}),
         "site-slurm": dict(time=NOW.timestamp() - 600, label="Slurm",
                            tasks={LATE: train(500, last=NOW - timedelta(hours=10)), BACK: dict(state="done", runs=[])},
                            squeue=[["1", "a", "RUNNING", "1:00", "q", "n1"], ["2", "b", "PENDING", "0:00", "q", "(Priority)"]],
                            bundle="b2", held={HELD: dict(why="bundle-incompatible", bundle="b1", since=(NOW - timedelta(hours=3)).timestamp())}),
         "site-slurm2": dict(time=NOW.timestamp() - 5 * 3600, label="Slurm2", tasks={}, squeue=[], held={})}
probs = [dict(id="site:x", msg="m", plain="something broke", level="normal", kind="site", first=1, active=True, notified=None),
         dict(id="notice:board", msg="n", plain="a note", level="normal", kind="notice", first=1, active=True, notified=None)]
GATE = dict(id=proto.alert_id("bundlegate", "site-slurm", HELD), msg="g", plain="held by the bundle", level="normal", kind="state",
            first=1, active=True, notified=None)  # relay records it as info (not here) while only an older bundle in use holds it
d = dict(queue=queue, relay=relay, problems=probs, checked="2026-10-16 11:35", notices=["board"],
         changes=[dict(t=0, key=DONE, state="done"), dict(t=0, key="_smoke/a/b", state="done")],
         failed=[dict(t=0, key=FAIL, site=None), dict(t=1, key=FAIL, site="site-slurm"), dict(t=0, key="_smoke/a/b", site="site-pueue")],
         stored={keys[21]})
o = SU.build(d, NOW)
T = o["tasks"]
ok("words: running / being sent / held / waiting there / done, coming back / failed / not sent yet / done",
   [T[k]["word"] for k in (RUN, SEND, HELD, WAIT, BACK, FAIL, FREE, DONE)]
   == [SU.RUNNING, SU.SENDING, SU.HELD, SU.WAITING, SU.BACK, SU.FAILED, SU.NOT_SENT, SU.DONE])
ok("a task in the experiment table but not in the hub queue: not queued yet; done when its results are on the store",
   T[keys[20]]["word"] == SU.NOT_QUEUED and T[keys[21]]["word"] == SU.DONE)
_q2 = dict(queue, tasks=queue["tasks"] + [dict(q(keys[22], "claimed"), host="hubhost"), q(keys[23], "busy")])
_T2 = SU.build(dict(d, queue=_q2), NOW)["tasks"]
ok("claimed by a worker on the hub itself, or unclaimed with its log still changing (busy): running",
   (_T2[keys[22]]["word"], _T2[keys[22]]["where"], _T2[keys[23]]["word"]) == (SU.RUNNING, "hubhost", SU.RUNNING))
ok("smoke tasks are left out", not any(proto.is_smoke(k) for k in T))
ok("a running training has progress and an estimated finish", abs(T[RUN]["frac"] - 0.5) < 1e-9 and T[RUN]["eta"] > NOW)
ok("a training past its estimated finish by over an hour is overdue and listed under Needs you",
   T[LATE]["word"] == SU.OVERDUE and any(LATE in a and "past its estimated finish" in a for a in o["attention"]))
ok("Needs you: active problems but not notices; an executor with no news for over 2 hours",
   "something broke" in o["attention"] and "a note" not in o["attention"] and any("no news from Slurm2" in a for a in o["attention"]))
Mx = {m["name"]: m for m in o["machines"]}
ok("pueue machine: free slots = worker slots - running", Mx["site-pueue"]["slots"] == 3 and Mx["site-pueue"]["count"][SU.RUNNING] == 1
   and Mx["site-pueue"]["later"] == 0)
ok("Slurm machine: jobs running / queued from squeue, no slots", Mx["site-slurm"]["jobs"] == (1, 1) and Mx["site-slurm"]["slots"] is None)
ok("due in the next 24 hours: only running tasks with a future estimate", [t["key"] for t in o["soon"]] == [RUN])
ok("last 24 hours: done changes and failed tasks (hub queue or released by an executor, once per task), smoke left out",
   (o["done"], o["failed"]) == (1, 1))
steps = o["steps"]["toy"]
ok("progress per step (recipe when there is no step): totals add up", steps["total"] == sum(steps[w] for w in SU.ORDER) and steps[SU.DONE] == 2)
txt = SU.text(o)
ok("summary text: head counts what needs you, machines, due list, progress, notes, pointers",
   txt.splitlines()[0].endswith("3 things need you") and "Machines" in txt and "Due in the next 24 hours (1)" in txt
   and "Progress (done / all)" in txt and "Notes (1): board" in txt and "taskq status --all" in txt)
ok("machine heads: free slots on pueue, jobs on Slurm, held tasks listed",
   "free 2/3" in txt and "more slot" not in txt and "jobs 1 running, 1 queued" in txt and f"held: {HELD} (bundle-incompatible: waits for" in txt)
cur = dict(relay, **{"site-slurm": dict(relay["site-slurm"], held={HELD: dict(why="bundle-incompatible", bundle="b2", since=NOW.timestamp())})})
o3 = SU.build(dict(d, relay=cur, problems=probs + [GATE]), NOW)
ok("a hold by the current bundle needs a new bundle: under Needs you at once, said on the held line",
   "held by the bundle" in o3["attention"] and "the current bundle b2 cannot run it; needs a new bundle" in SU.text(o3))
_workers = SU.M.workers
SU.M.workers = lambda n, kind=None: [dict(w, hours=[("21:00", "09:00", None, None)]) if w.get("gpu") == "0" else w for w in _workers(n, kind)]
try:
    day, night = SU.build(d, NOW), SU.build(d, NOW.replace(hour=22))
finally:
    SU.M.workers = _workers
dm = {m["name"]: m for m in day["machines"]}["site-pueue"]
ok("a worker outside its hours is not a free slot now; the head names how many come later and when",
   (dm["slots"], dm["later"]) == (2, 1) and "free 1/2, 1 more slot 21:00–09:00" in SU.text(day)
   and {m["name"]: m for m in night["machines"]}["site-pueue"]["slots"] == 3 and "more slot" not in SU.text(night))
lst = SU.listing(o, list(T.values()), "every task")
ok("listing: grouped by state in screen order, overdue first", lst.index("overdue (1)") < lst.index("running (1)") < lst.index("done (2)"))
ok("listing of nothing says so", SU.listing(o, [], "x").endswith("no such task"))
o2 = SU.build(dict(d, problems=[], relay={n: dict(r, time=NOW.timestamp()) for n, r in relay.items()},
                   queue=dict(queue, tasks=[t for t in queue["tasks"] if t["key"] != LATE])), NOW)
ok("nothing wrong -> running normally", SU.text(o2).splitlines()[0].endswith("running normally"))
ok("unreadable hub queue is said at the top", SU.text(SU.build(dict(d, queue=None), NOW)).splitlines()[0].endswith("hub queue unreadable"))

# events.transitions: changes into done / failed within the window, first records are not changes
db = D.connect(create=True)
t0 = time.time()
events.snapshot(db, [dict(key="a", state="claimed"), dict(key="b", state="done"), dict(key="c", state="free")], t=t0 - 3 * 86400)
events.snapshot(db, [dict(key="a", state="done"), dict(key="b", state="done", progress=1.0), dict(key="c", state="failed"),
                     dict(key="d", state="done")], t=t0 - 60)
got = sorted((c["key"], c["state"]) for c in events.transitions(db, t0 - 86400, ("done", "failed")))
ok("transitions: a claimed->done, c free->failed; b stays done, d's first record is not a change", got == [("a", "done"), ("c", "failed")])

print("All passed" if not fails else f"{fails} failed")
sys.exit(1 if fails else 0)
