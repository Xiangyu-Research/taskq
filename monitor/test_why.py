#!/usr/bin/env python3
"""Tests for why.py (taskq why) on a temp hub database with the fixture config; no machine contacted.
  1. unknown key: exit 4; a task in the queue with no assignment recorded yet says so;
  2. the last round's reasons per executor; all full: suggests taskq now; --json carries the same;
  3. stopped: the stop and taskq resume; claimed: who holds it; an intent on it and a drain of a machine it can go to are listed;
  4. not queued and matching a hold switch: held, and the command that clears it.
    python3 monitor/test_why.py
"""
import contextlib
import io
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
Q = os.path.join(os.path.dirname(HERE), "queue")
sys.path.insert(0, os.path.join(Q, "testdata"))
import fixture  # noqa: E402

fixture.use()
os.environ.update(TASKQ_QSTORE="db", TASKQ_MACHINE="hub", HOME=fixture.tmpdir())
sys.path.insert(0, Q)
sys.path.insert(0, HERE)
import db as D  # noqa: E402

D.connect(create=True)
import hubstate as HS  # noqa: E402
import intents as I  # noqa: E402
import proto  # noqa: E402
import switches  # noqa: E402
import taskq  # noqa: E402
import why as W  # noqa: E402

sys.modules["relay_remote"] = types.SimpleNamespace(push_stop=lambda name, keys, why: [], push_nice=lambda name, keys, nice, dry=False: [])
fails = 0


def ok(what, cond, extra=""):
    global fails
    print(("PASS:" if cond else "FAIL:") + what + ("" if cond else f" ({extra})"))
    fails += 0 if cond else 1


def why(*a):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = W.main(list(a))
    return rc, out.getvalue() + err.getvalue()


taskq.run(["add", "--nice", "50", "--exp", "arch_m"])
K = [t["key"] for t in taskq.read_tasks()]
k0, k1, k2 = K[0], K[1], K[2]

# ---- 1 ----
rc, out = why("nope/x/0")
ok("unknown key: exit 4", rc == taskq.GONE and "neither in the hub queue" in out, out)
rc, out = why(k0)
ok("in the queue, no round recorded yet: not sent, where it can go, says nothing is recorded",
   rc == 0 and "not sent yet" in out and "can go to: " in out and "not recorded yet" in out, out)

# ---- 2 ----
HS.put_placement(dict(t=1.8e9, keys={k0: "site-pueue: full; site-slurm: full"}, by_site={k0: [["site-pueue", "full"], ["site-slurm", "full"]]},
                      sites={"site-pueue": "full", "site-slurm": "full", "site-slurm2": "unreachable"}))
rc, out = why(k0)
ok("the last round's reasons, one line per executor, and executors that got nothing that round",
   "site-pueue: full" in out and "    site-slurm: full" in out and "site-slurm2 got nothing that round: unreachable" in out, out)
ok("every executor full: suggests taskq now", f"next: taskq now {k0}" in out, out)
rc, out = why(k0, "--json")
d = json.loads(out.split(proto.JSON_MARK, 1)[1])["why"]
ok("--json: word, placement and next", d["word"] == "not_sent" and d["placement"]["by_site"][0] == ["site-pueue", "full"]
   and d["next"].startswith("taskq now"), d)

# ---- 3 ----
taskq.run(["stop", "--why", "testing", "--", k1])
rc, out = why(k1)
ok("stopped: the stop and taskq resume", "stopped" in out and "testing" in out and f"next: taskq resume {k1}" in out, out)
taskq.run(["take", "--worker", "site-pueue", "--assign", k2])
I.main(["move", k2, "--to", "site-slurm"])
switches.put("drain", "site-slurm", "on", why_="maintenance")
rc, out = why(k2)
ok("claimed: who holds it; the move under way; the drain of a machine it can go to",
   "claimed by site-pueue" in out and "under way: move" in out and "switch drain site-slurm" in out and "maintenance" in out, out)
ok("its events are listed", "recent events:" in out and "move" in out, out)

# ---- 4 ----
I.main(["intents", "--drop", str(I.rows(key=k2)[0]["id"])])
taskq.run(["release", "--worker", "site-pueue", k2])
taskq.run(["rm", k2])
switches.put("hold", "*/" + k2.rsplit("/", 1)[0].rsplit("/", 1)[-1] + "*", "on", why_="later")
pat = next(r["target"] for r in switches.rows() if r["name"] == "hold")
rc, out = why(k2)
ok("not queued, matching a hold switch: held, and how to clear it",
   rc == 0 and "not queued yet" in out and f"held by {pat}" in out and f"taskq switch clear hold {pat}" in out, out)

print("all passed" if not fails else f"{fails} failed")
sys.exit(1 if fails else 0)
