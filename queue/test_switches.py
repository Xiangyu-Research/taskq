#!/usr/bin/env python3
"""Day-to-day switches (queue/switches.py) and what reads them:
  1. no hub database: no switches, policy.py defaults; set / clear / expiry each write one event; bad names, values, machines,
     workers and automatic parts are refused; prune removes what has ended;
  2. readers: auto, ready (sites/remote.py), holds, pause (mtab.paused), queue off (qstore.DbStore.enabled, expiry honoured);
  3. drain: site_doc per worker id, relay.topup starts nothing on a drained machine, refill.pueue_plan skips a drained worker,
     relay.push_switches writes the executor's copy once, SIGTERMs running hard-drained pueue workers, makes and removes the
     GPU pad file only when it made it; consumers/drain.py reads that copy (admit, tick, expiry);
  4. the command line: drain / undrain / pause / queue / switch set|list|clear.
    python3 queue/test_switches.py"""
import io
import json
import os
import sys
import time
import types
from contextlib import redirect_stderr, redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
T = fixture.tmpdir(prefix="tqswitch")
os.environ["HOME"] = os.path.join(T, "home")
os.makedirs(os.environ["HOME"])
os.environ.update(TASKQ_MACHINE="hub", TASKQ_DB=os.path.join(T, "taskq.db"))
for k in ("TASKQ_DIR", "TASKQ_ROOT", "TASKQ_QSTORE"):
    os.environ.pop(k, None)
sys.path.insert(0, HERE)
import proto  # noqa: E402
import db as D  # noqa: E402
import switches as SW  # noqa: E402
import cfg  # noqa: E402
M = cfg.mtab()

fails = 0


def ok(name, cond, extra=""):
    global fails
    print(("PASS:" if cond else "FAIL:") + name + ("" if cond else f" ({extra})"))
    fails += 0 if cond else 1


def evs():
    c = D.connect()
    try:
        return [dict(kind=k, msg=m, site=s) for k, m, s in c.execute("SELECT kind, msg, site FROM log WHERE source='switch' ORDER BY id")]
    finally:
        c.close()


def cli(*a):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        rc = SW.main(list(a))
    return rc, out.getvalue() + err.getvalue()


# 1. no database, then set / clear / expiry
ok("no database: no switches", SW.rows() == [] and SW.auto("smoke", True) is True and SW.ready("site-pueue", 2) == 2)
SW._cache[1] = None
D.connect(create=True).close()
now = time.time()
r = SW.put("auto", "smoke", "off", why_="testing")
ok("set: stored and read back", SW.auto("smoke", True) is False and SW.get("auto", "smoke")["why"] == "testing")
e = evs()
ok("set: one event", len(e) == 1 and e[0]["kind"] == "set" and "auto smoke off" in e[0]["msg"], e)
SW.put("auto", "smoke", "on")
ok("set again: replaced, not added", len([x for x in SW.rows() if x["name"] == "auto"]) == 1 and SW.auto("smoke", False) is True)
ok("clear: removed with an event", SW.drop("auto", "smoke") and SW.get("auto", "smoke") is None and evs()[-1]["kind"] == "cleared")
ok("clear what is not set: None, no event", SW.drop("auto", "smoke") is None and len(evs()) == 3)
for bad, args in (("unknown name", ("nope", "", "on")), ("bad value", ("drain", "site-pueue", "maybe")),
                  ("unknown machine", ("drain", "nowhere", "on")), ("unknown worker", ("drain", "site-pueue/gpu9", "on")),
                  ("worker on a machine-only switch", ("pause", "site-pueue/gpu0", "on")), ("not an executor", ("ready", "hub", "2")),
                  ("not a number", ("ready", "site-pueue", "lots")), ("unknown automatic part", ("auto", "nope", "on")),
                  ("target on the queue switch", ("queue", "hub", "off")), ("no target", ("drain", "", "on"))):
    try:
        SW.put(*args)
        ok(f"refused: {bad}", False, "accepted")
    except ValueError:
        ok(f"refused: {bad}", True)
try:
    SW.put("pause", "site-slurm", "on", until=now - 5)
    ok("refused: an end time in the past", False, "accepted")
except ValueError:
    ok("refused: an end time in the past", True)
SW.put("pause", "site-slurm", "on", until=now + 60, why_="maintenance")
ok("pause: mtab.paused says why and until", "maintenance" in (M.paused("site-slurm") or "") and "until" in M.paused("site-slurm"))
ok("ended switch: not in force", SW.get("pause", "site-slurm", now + 120) is None)
msgs = SW.prune(now + 120)
ok("prune: removes what ended, with an event", len(msgs) == 1 and evs()[-1]["kind"] == "expired" and SW.all_rows() == [],
   (msgs, SW.all_rows()))

# 2. other readers
SW.put("ready", "site-pueue", "5")
import remote as R  # noqa: E402
ok("ready: sites/remote.py takes the switch", R.ready("site-pueue") == 5)
SW.drop("ready", "site-pueue")
ok("ready: back to policy.py", R.ready("site-pueue") == 2)
SW.put("hold", "lprob/*", "on")
ok("hold: listed", SW.holds() == ["lprob/*"])
SW.drop("hold", "lprob/*")
import qstore  # noqa: E402
st = qstore.DbStore()
ok("queue: on by default", st.enabled())
SW.put("queue", "", "off", until=time.time() + 1, why_="pausing for a move")
ok("queue off: DbStore.enabled is False", not st.enabled() and "pausing for a move" in SW.why(SW.queue_off()))
time.sleep(1.2)
ok("queue off ended: on again before any prune", st.enabled() and SW.queue_off() is None)
SW.prune()

# 3. drain
ws = {w["name"]: w for w in M.workers("site-pueue")}
g0 = ws["gpu0"]
SW.put("drain", "site-pueue/gpu0", "on", why_="someone needs the card")
doc = SW.site_doc("site-pueue", {n: w["id"] for n, w in ws.items()})
ok("site_doc: keyed by worker id", list(doc.get("drain", {})) == [g0["id"]] and doc["drain"][g0["id"]]["value"] == "on", doc)
ok("one worker drained: machine not drained", SW.machine_drained("site-pueue", list(ws)) is None)
import refill  # noqa: E402
rs = dict(jobs=[], taskq=dict(tasks=[dict(key="k1", state=proto.S_FREE, spec=dict(kind="gpu"), kind="gpu")]))
orig_unclaimed = refill.unclaimed
refill.unclaimed = lambda tq, kind: True
try:
    subs, why, _ = refill.pueue_plan("site-pueue", rs)
finally:
    refill.unclaimed = orig_unclaimed
ok("pueue_plan: a drained worker is not started", g0["id"] not in [w["id"] for w in subs] and "drained" in why, (subs, why))
import relay  # noqa: E402
SW.put("drain", "site-pueue", "soft")
subs, why, _ = relay.topup("site-pueue", rs)
ok("topup: a drained machine starts nothing", subs == [] and why.startswith("drained"), why)
SW.drop("drain", "site-pueue")

sent = []


def run(c, i=None):
    sent.append((c, i))
    return 0, "", ""


rs = dict(jobs=[[7, f"{proto.WORKER_LABEL}{g0['id']}", "Running", None]])
msgs, stuck = relay.push_switches("site-pueue", rs, dry=True)
ok("push dry: says what, sends nothing", msgs and "would push" in msgs[0] and not sent, msgs)
msgs, stuck = relay.push_switches("site-pueue", rs, dry=False, run=run)
wrote = [x for x in sent if x[1] is not None]
kills = [x for x in sent if "kill -s SIGTERM" in x[0]]
ok("push: writes the executor's copy", len(wrote) == 1 and json.loads(wrote[0][1]) == doc and proto.ROOT["overrides"] in wrote[0][0], sent)
ok("push: SIGTERMs the running drained worker's group", len(kills) == 1 and f"-g {g0['group']}" in kills[0][0], sent)
ok("push: no GPU pad file touched", "gpu_pad" not in wrote[0][0])
sent.clear()
msgs, stuck = relay.push_switches("site-pueue", dict(jobs=[[7, f"{proto.WORKER_LABEL}{g0['id']}", "Done", "Success"]]), dry=False, run=run)
ok("push again, nothing changed and the worker stopped: nothing sent", sent == [] and msgs == [], sent)
SW.put("gpu_pad", "site-slurm", "off")
msgs, stuck = relay.push_switches("site-slurm", dict(), dry=False, run=run)
ok("gpu_pad off: the hub makes the file", any("touch" in c and proto.QUEUE["gpu_pad_off"] in c for c, _ in sent), sent)
sent.clear()
SW.drop("gpu_pad", "site-slurm")
relay.push_switches("site-slurm", dict(), dry=False, run=run)
ok("gpu_pad back on: the hub removes the file it made", any("rm -f" in c and proto.QUEUE["gpu_pad_off"] in c for c, _ in sent), sent)
sent.clear()
relay.push_switches("site-slurm", dict(), dry=False, run=run)
ok("gpu_pad: a file the hub did not make is left alone", sent == [], sent)


def fail(c, i=None):
    return 1, "", "Permission denied"


SW.put("drain", "site-slurm", "on")
msgs, stuck = relay.push_switches("site-slurm", dict(), dry=False, run=fail)
ok("push fails: one alert, not marked as pushed", list(stuck) == [proto.alert_id("site", "site-slurm", "switches")]
   and SW.pushed("site-slurm").get("doc") == {}, (stuck, SW.pushed("site-slurm")))
SW.drop("drain", "site-slurm")

root = os.path.join(T, "root")
os.makedirs(os.path.join(root, "state"))
os.environ["TASKQ_ROOT"] = root
import importlib.util  # noqa: E402
spec_ = importlib.util.spec_from_file_location("drain_plugin", os.path.join(HERE, "consumers", "drain.py"))
DP = importlib.util.module_from_spec(spec_)
spec_.loader.exec_module(DP)
calls = []
w = types.SimpleNamespace(W=g0["id"], qdir=os.path.join(root, "queue"), runs={}, zomb={}, shutdown=lambda m, rc: calls.append((m, rc)))
ok("plugin: no file → claims", DP.admit(w) is None)
with open(os.path.join(root, proto.ROOT["overrides"]), "w") as f:
    json.dump(doc, f)
ok("plugin: drained → no claim, says why", "someone needs the card" in (DP.admit(w) or ""))
w.runs = {1: object()}
DP.tick(w)
ok("plugin on: running → stop, release, exit 0", calls and calls[-1][1] == 0 and "releasing" in calls[-1][0], calls)
calls.clear()
with open(os.path.join(root, proto.ROOT["overrides"]), "w") as f:
    json.dump({"drain": {"*": dict(value="soft", until=None, why="")}}, f)
DP.tick(w)
ok("plugin soft: running → keeps going", calls == [] and DP.admit(w) == "drained")
w.runs = {}
DP.tick(w)
ok("plugin soft: idle → exit 0", calls and calls[-1][1] == 0)
with open(os.path.join(root, proto.ROOT["overrides"]), "w") as f:
    json.dump({"drain": {"*": dict(value="on", until=time.time() - 1, why="")}}, f)
ok("plugin: ended entry no longer counts", DP.admit(w) is None)
w2 = types.SimpleNamespace(W="other-worker", qdir=w.qdir)
with open(os.path.join(root, proto.ROOT["overrides"]), "w") as f:
    json.dump(doc, f)
ok("plugin: another worker's entry does not count", DP.admit(w2) is None)
os.environ.pop("TASKQ_ROOT")
SW.drop("drain", "site-pueue/gpu0")

# 4. command line (the immediate push replaced: no ssh in tests)
pushed = []
relay.push_switches = lambda name, rs, dry, now=False, run=None: (pushed.append(name) or [f"pushed {name}"], {})
rc, out = cli("drain", "site-pueue/gpu1", "--for", "2h", "--why", "card swap")
r = SW.get("drain", "site-pueue/gpu1")
ok("drain: set with an end about 2h away, pushed now", rc == 0 and r and abs(r["until"] - time.time() - 7200) < 60 and pushed == ["site-pueue"], out)
rc, out = cli("list", "--json")
j = json.loads(out.split(proto.JSON_MARK, 1)[1])
ok("list --json: the switch, with the fields", rc == 0 and [x["target"] for x in j["switches"]] == ["site-pueue/gpu1"]
   and set(j) == set(proto.SWITCH_TOP) and set(j["switches"][0]) == set(proto.SWITCH_FIELDS), out)
rc, out = cli("undrain", "site-pueue/gpu1")
ok("undrain: removed and pushed", rc == 0 and SW.get("drain", "site-pueue/gpu1") is None and pushed == ["site-pueue"] * 2, out)
rc, out = cli("undrain", "site-pueue/gpu1")
ok("undrain what is not drained: exit 1", rc == 1, out)
rc, out = cli("drain", "nowhere")
ok("drain an unknown machine: exit 2 with the machines", rc == 2 and "site-pueue" in out, out)
rc, out = cli("pause", "site-slurm2", "--until", "23:59")
ok("pause --until HH:MM: in force, ends within a day", rc == 0 and 0 < SW.get("pause", "site-slurm2")["until"] - time.time() <= 86400, out)
rc, out = cli("unpause", "site-slurm2")
ok("unpause", rc == 0 and SW.get("pause", "site-slurm2") is None, out)
rc, out = cli("queue", "off", "--why", "moving disks")
ok("queue off", rc == 0 and not qstore.DbStore().enabled(), out)
rc, out = cli("queue", "on")
ok("queue on", rc == 0 and qstore.DbStore().enabled(), out)
rc, out = cli("set", "auto", "advance", "off", "--for", "30m")
ok("switch set auto", rc == 0 and SW.auto("advance", True) is False, out)
rc, out = cli("clear", "auto", "advance")
ok("switch clear", rc == 0 and SW.auto("advance", False) is False and SW.get("auto", "advance") is None, out)
rc, out = cli("set", "auto", "advance", "maybe")
ok("switch set with a bad value: exit 2", rc == 2 and "on, off" in out, out)
ok("every change wrote one event", all(x["msg"] for x in evs()) and len([x for x in evs() if x["kind"] == "set"]) >= 10)

print(f"\n{'all passed' if not fails else f'{fails} failed'}")
sys.exit(1 if fails else 0)
