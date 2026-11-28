#!/usr/bin/env python3
"""Tests for intents.py: taskq rm of a task in use, move, now, after, cancel and taskq intents, advanced round by round, on a temp
hub database with the fixture config; pushes to executors and the command an after intent runs are faked (no machine contacted).
  1. rm: unclaimed removed at once; in use stopped (pushed to its executor), removed once released; resumed meanwhile: dropped;
  2. move: stop, release, resume, sent only to the target (only()), done once assigned there; refusals; drop undoes the stop;
  3. now: first in line; after NOW_WAIT waiting, one victim (no progress first) stopped, resumed once the task runs;
  4. after: runs the command once the state is reached, rechecked against the queue; settled otherwise or past the deadline: fails;
  5. cancel: dry run lists, --hold holds the downstream steps; dry rounds change nothing.
    python3 queue/test_intents.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
os.environ.update(TASKQ_QSTORE="db", TASKQ_MACHINE="hub", HOME=fixture.tmpdir())
sys.path.insert(0, HERE)
import db as D  # noqa: E402

D.connect(create=True)
import intents as I  # noqa: E402
import proto  # noqa: E402
import switches  # noqa: E402
import taskq  # noqa: E402

fails = 0
pushed, ran, WORDS = [], [], {}
sys.modules["relay_remote"] = types.SimpleNamespace(push_stop=lambda name, keys, why: pushed.append((name, list(keys), why)) or [],
                                                    push_nice=lambda name, keys, nice, dry=False: [])
I.run_cmd = lambda cmd, it: ran.append(list(cmd)) or (0, "ok")
I.Round.words = lambda self: WORDS


def ok(what, cond, extra=""):
    global fails
    print(("PASS:" if cond else "FAIL:") + what + ("" if cond else f" ({extra})"))
    fails += 0 if cond else 1


def tq(*a):
    rc, out, err = taskq.run(list(a))
    return rc, out + err


def cli(*a):
    return I.main(list(a))


def keys():
    return [t["key"] for t in taskq.read_tasks()]


def it_of(key, kind):
    rs = I.rows(everything=True, key=key, kind=kind)
    return rs[-1] if rs else None


def backdate(it, sec):
    db = D.connect(create=True)
    with D.tx(db):
        db.execute("UPDATE intent SET t = t - ?, deadline = deadline - ? WHERE id = ?", (sec, sec, it["id"]))


tq("add", "--nice", "50", "--exp", "arch_m")
K = keys()
ok("five tasks in the hub queue", len(K) == 5, K)

# ---- 1. rm ----
tq("take", "--worker", "site-pueue", "--assign", K[0])
rc = cli("rm", K[1], K[0])
ok("rm: the unclaimed one is gone at once", K[1] not in keys() and rc == 0)
ok("rm: the one in use is stopped, the stop pushed to its executor, an rm intent left",
   taskq.stopped_info(K[0]) and pushed[-1] == ("site-pueue", [K[0]], "taskq rm") and it_of(K[0], "rm")["state"] == I.ACTIVE)
ok("rm again: refused politely (already being removed)", cli("rm", K[0]) == 0 and len(I.rows(key=K[0])) == 1)
I.run()
ok("round: still claimed, nothing removed", K[0] in keys() and it_of(K[0], "rm")["state"] == I.ACTIVE)
tq("release", "--worker", "site-pueue", K[0])
lines, edited, stuck = I.run()
ok("round after the release: removed, intent done", K[0] not in keys() and it_of(K[0], "rm")["state"] == "done" and edited and not stuck)
tq("take", "--worker", "site-pueue", "--assign", K[2])
cli("rm", K[2])
tq("resume", K[2])
I.run()
ok("rm, resumed before the release: dropped, the task stays", K[2] in keys() and it_of(K[2], "rm")["state"] == "dropped")
tq("release", "--worker", "site-pueue", K[2])

# ---- 2. move ----
k = K[2]
tq("take", "--worker", "site-pueue", "--assign", k)
ok("move to a machine that is not an executor: refused", cli("move", k, "--to", "hub") == taskq.REFUSED and not I.rows(key=k))
pushed.clear()
ok("move: stopped where it is, the stop pushed", cli("move", k, "--to", "site-slurm") == 0 and taskq.stopped_info(k)
   and pushed == [("site-pueue", [k], "taskq move to site-slurm")])
ok("move: only() sends it only to the target", I.only() == {k: dict(to="site-slurm", restart=False)})
ok("move while one is under way: refused", cli("move", k, "--to", "site-slurm2") == taskq.REFUSED)
ok("now while a move is under way: refused", cli("now", k) == taskq.REFUSED)
I.run()
ok("round while still claimed: still stopped, waiting", taskq.stopped_info(k) and it_of(k, "move")["state"] == I.ACTIVE)
tq("release", "--worker", "site-pueue", k)
I.run()
ok("round after the release: our stop resumed, still active", not taskq.stopped_info(k) and it_of(k, "move")["state"] == I.ACTIVE)
tq("take", "--worker", "site-slurm", "--assign", k)
I.run()
ok("assigned to the target: done, only() empty", it_of(k, "move")["state"] == "done" and I.only() == {})
tq("release", "--worker", "site-slurm", k)
k3 = K[3]
ok("move of an unclaimed task: nothing stopped, sent only there", cli("move", k3, "--to", "site-slurm2") == 0
   and not taskq.stopped_info(k3) and I.only() == {k3: dict(to="site-slurm2", restart=False)})
cli("intents", "--drop", str(it_of(k3, "move")["id"]))
ok("drop: the intent ends, only() empty", it_of(k3, "move")["state"] == "dropped" and I.only() == {})
tq("take", "--worker", "site-pueue", "--assign", k3)
cli("move", k3, "--to", "site-slurm")
cli("intents", "--drop", str(it_of(k3, "move")["id"]))
ok("drop of a move that stopped it: the stop is undone", not taskq.stopped_info(k3))
tq("release", "--worker", "site-pueue", k3)

# ---- 3. now ----
kn, v1, v2 = K[4], K[2], K[3]
tq("take", "--worker", "site-pueue", "--assign", v1)
tq("take", "--worker", "site-pueue", "--assign", v2)
tq("take", "--worker", "site-pueue", "--assign", kn)
ok("now: nice set to NOW_NICE", cli("now", kn) == 0 and next(t for t in taskq.read_tasks() if t["key"] == kn)["nice"] == proto.NOW_NICE)
WORDS.update({kn: dict(word="waiting", where="site-pueue"), v1: dict(word="running", where="site-pueue", frac=0.4),
              v2: dict(word="running", where="site-pueue", frac=0.0)})
I.run()
ok("now, waiting under NOW_WAIT: nothing stopped", not taskq.stopped_info(v1) and not taskq.stopped_info(v2))
backdate(it_of(kn, "now"), proto.NOW_WAIT + 60)
pushed.clear()
I.run()
ok("now, waiting past NOW_WAIT: the victim without progress is stopped (not pushed: the round's relay does that)",
   taskq.stopped_info(v2) and not taskq.stopped_info(v1) and it_of(kn, "now")["args"]["victim"] == v2 and pushed == [])
I.run()
ok("next round: no second victim", not taskq.stopped_info(v1))
WORDS[kn] = dict(word="running", where="site-pueue")
I.run()
ok("now runs: the victim resumed, intent done", not taskq.stopped_info(v2) and it_of(kn, "now")["state"] == "done")
WORDS.clear()

# ---- 4. after ----
ka, kb = K[2], K[3]
ok("after: a command outside the list is refused", cli("after", ka, "running", "--", "drain", "site-pueue") == 2)
ok("after: naming a key not in the queue is refused", cli("after", ka, "running", "--", "resume", "nope/x/0") == taskq.REFUSED)
ok("after: --within after the positionals and the command after --",
   cli("after", ka, "done", "--within", "2h", "--", "nice", "--nice", "7", "--", kb) == 0
   and it_of(ka, "after")["args"]["cmd"] == ["nice", "--nice", "7", "--", kb])
WORDS[ka] = dict(word="running", where="site-pueue")
I.run()
ok("not done yet: not run", ran == [] and it_of(ka, "after")["state"] == I.ACTIVE)
WORDS[ka] = dict(word="done", where="site-pueue")
I.run()
ok("done: the command runs once, intent done", ran == [["nice", "--nice", "7", "--", kb]] and it_of(ka, "after")["state"] == "done")
I.run()
ok("not run twice", len(ran) == 1)
cli("after", kb, "done", "--", "resume", ka)
WORDS[kb] = dict(word="failed", where="site-pueue")
_, _, stuck = I.run()
it = it_of(kb, "after")
ok("settled otherwise (failed, waiting for done): failed, alerted, not run",
   it["state"] == "failed" and len(ran) == 1 and proto.alert_id("intent", it["id"]) in stuck)
cli("after", kb, "running", "--", "resume", ka)
backdate(it_of(kb, "after"), 8 * 86400)
WORDS[kb] = dict(word="waiting", where="site-pueue")
_, _, stuck = I.run()
ok("past its deadline: expired and alerted", it_of(kb, "after")["state"] == "expired"
   and any(x.startswith("intent:") for x in stuck))
cli("after", kb, "running", "--", "resume", ka)
tq("release", "--worker", "site-pueue", ka)
tq("rm", ka)
WORDS[kb] = dict(word="running", where="site-pueue")
I.run()
last = it_of(kb, "after")
ok("rechecked when it fires: a key gone from the queue meanwhile fails it", last["state"] == "failed" and "no longer in the hub queue" in last["note"]
   and len(ran) == 1)
WORDS.clear()

# ---- 5. dry rounds, cancel ----
for k in (v1, v2, kn):
    tq("release", "--worker", "site-pueue", k)
tq("take", "--worker", "site-pueue", "--assign", kn)
cli("rm", kn)
tq("release", "--worker", "site-pueue", kn)
lines, edited, _ = I.run(dry=True)
ok("dry round: says what it would do, changes nothing", any(f"would remove {kn}" in x for x in lines) and not edited and kn in keys(), lines)
I.run(kinds=("move",))
ok("kinds: other kinds are left for later", kn in keys())
I.run(kinds=("rm",))
ok("... and done when their kind's turn comes", kn not in keys())
tq("add", "--nice", "50", "--exp", "arch_m")
before = keys()
f0, f1 = "D101_alpha/V_m/0", "D101_alpha/V_m/1"
ok("cancel -n: nothing removed", cli("cancel", "--exp", "arch_m", "-n") == 0 and keys() == before and f0 in before)
tq("take", "--worker", "site-pueue", "--assign", f0)
rc = cli("cancel", "--exp", "arch_m", "--only", "frag=0,1", "--hold")
rest = keys()
ok("cancel --only: free ones of those params removed, the one in use left for an rm intent, the others stay",
   f0 in rest and f1 not in rest and sorted(rest) == sorted(k for k in before if k != f1) and it_of(f0, "rm")["state"] == I.ACTIVE, rest)
ok("cancel --hold: the downstream steps of those tasks are held (one HOLD pattern per label)",
   {r["target"] for r in switches.rows() if r["name"] == "hold"} == {"*/arch_m_f0", "*/arch_m_f1"}, switches.rows())
ok("taskq intents --json lists the active ones", I.rows() and all(r["state"] == I.ACTIVE for r in I.rows()))

print("all passed" if not fails else f"{fails} failed")
sys.exit(1 if fails else 0)
