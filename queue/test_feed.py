#!/usr/bin/env python3
"""Tests for feed.py: run_all hands off to a stand-in relay.py; nobody uses a constructed hub queue. No machines.
  python3 queue/test_feed.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata"))
import fixture  # noqa: E402
T = fixture.tmpdir()
os.environ.update(FEED_LOG=os.path.join(T, "feed.log"))
sys.path.insert(0, HERE)

fixture.use()
import feed as F  # noqa: E402

fails = 0


def ok(msg, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + msg)
    fails += not cond


def ct(key, state="free", assigned=None):
    return dict(key=key, state=state, assigned=assigned)


# ---------------- assigned to a name that is not an executor (left after a rename or removal) ----------------
q_ = dict(tasks=[ct("a/x/0", "claimed", assigned="pueue-host"), ct("a/x/1", "claimed", assigned="site-pueue"),
                 ct("a/x/2", "claimed", assigned="site-slurm"), ct("a/x/3", "done", assigned="old")])
nb = F.nobody(q_)
ok("assigned to pueue-host (not an executor) reports feed:nobody:pueue-host with the release command; relay executors and done tasks are not reported",
   list(nb) == ["feed:nobody:pueue-host"] and "release --worker pueue-host" in F.alert_msg(nb["feed:nobody:pueue-host"])
   and "a/x/0" in F.alert_msg(nb["feed:nobody:pueue-host"]))
ok("feed:nobody has structured data: worker, n, keys", nb["feed:nobody:pueue-host"]["data"] == dict(worker="pueue-host", n=1, keys=["a/x/0"]))
ok("hub queue unreadable: report nothing", F.nobody(None) == {})

# ---------------- one entry point for sending tasks: feed.run_all hands off to relay.py ----------------
rl, rcalls = types.ModuleType("relay"), []
rl.run_all = lambda s, dry=False: (rcalls.append(dry), (["site-pueue claimed a/x/0"], {"site:site-pueue:x": "y"}))[1]
sys.modules["relay"] = rl
hub = F.M.hub()
ev, st = F.run_all(dict(taskq={hub: q_}, ok={}), dry=False)
ok("hands off to relay.run_all, merges messages and problems, adds feed:nobody, writes one line in feed.log pointing at ~/relay.log",
   rcalls == [False] and ev == ["site-pueue claimed a/x/0"] and set(st) == {"site:site-pueue:x", "feed:nobody:pueue-host"}
   and "see ~/relay.log" in open(os.environ["FEED_LOG"]).read())
n0 = len(open(os.environ["FEED_LOG"]).read())
ev, st = F.run_all(dict(taskq={}, ok={}), dry=True)
ok("dry: asks relay as usual (with dry), writes no feed.log", rcalls == [False, True] and len(open(os.environ["FEED_LOG"]).read()) == n0)
rl.run_all = lambda s, dry=False: 1 / 0
ev, st = F.run_all(dict(taskq={}, ok={}), dry=True)
ok("relay.py fails: reports relay:error", list(st) == ["relay:error"] and "ZeroDivisionError" in st["relay:error"])
del sys.modules["relay"]

print("all pass" if fails == 0 else f"{fails} failed")
sys.exit(1 if fails else 0)
