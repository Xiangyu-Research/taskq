#!/usr/bin/env python3
"""Tests for sites/slurm_eta.py (taskq site eta): Slurm executors sharing a partition and hostname pattern are one cluster; a
snapshot parses; the simulation starts a queued job when a running one frees its GPUs, keeps the per-user QOS limit, backfills a
short job that ends before the reservation and leaves a held job alone; recorded history yields starts, ends and the comparison
with earlier predictions; main reads through a fake remote call, keeps everything in the state dir and prints the report."""
import contextlib
import datetime as dt
import io
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(CORE, "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
TMP = tempfile.mkdtemp(prefix="taskq_eta_")
os.environ["TASKQ_STATE"] = TMP
import slurm_eta as E  # noqa: E402
import proto  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


NOW = 1_800_000_000
H = 3600


def ts(t):
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def run_line(jid, user, node, gpu, start, end, limit="10:00:00"):
    return (f"JobId={jid} UserId={user}(1) Priority=1 QOS=normal JobState=RUNNING Reason=None Dependency=(null) "
            f"TimeLimit={limit} SubmitTime={ts(start - 60)} EligibleTime={ts(start - 60)} StartTime={ts(start)} EndTime={ts(end)} "
            f"Partition=gpu-a NodeList={node} AllocTRES=cpu=8,mem=64G,gres/gpu={gpu}")


def pend_line(jid, user, gpu, limit, reason="Priority", dep="(null)"):
    return (f"JobId={jid} UserId={user}(1) Priority=1 QOS=normal JobState=PENDING Reason={reason} Dependency={dep} "
            f"TimeLimit={limit} SubmitTime={ts(NOW - H)} EligibleTime={ts(NOW - H)} StartTime=Unknown EndTime=Unknown "
            f"Partition=gpu-a NodeList= ReqTRES=cpu=8,mem=64G,gres/gpu={gpu}")


def snapshot(now, running, pending, sprio):
    return "\n".join([f"{now} +0000", "@@JOBS", *running, *pending, "@@SPRIO", *sprio, "@@PART",
                      "PartitionName=gpu-a Nodes=gpu[01-02] TotalNodes=2 QoS=part TRES=cpu=128,mem=1T,node=2,gres/gpu=4",
                      "@@QOS", "normal|gres/gpu=2", "part|", "@@CONF", "PriorityWeightAge = 1000",
                      "PriorityMaxAge = 7-00:00:00", "SchedulerParameters = bf_min_age_reserve=0"]) + "\n"


RUNNING = [run_line("100", "alice", "gpu01", 2, NOW - H, NOW + 2 * H), run_line("101", "bob", "gpu02", 1, NOW - H, NOW + 5 * H)]
PENDING = [pend_line("200", "me", 2, "10:00:00"), pend_line("201", "carol", 1, "1:00:00"), pend_line("202", "dave", 1, "10:00:00"),
           pend_line("203", "me", 2, "10:00:00"), pend_line("204", "erin", 1, "1:00:00", reason="JobHeldUser")]
SPRIO = ["200|me|900|100", "201|carol|500|100", "202|dave|400|100", "203|me|300|100", "204|erin|990|100"]
RAW = snapshot(NOW, RUNNING, PENDING, SPRIO)

# clusters
cs = E.clusters()
ok("Slurm executors sharing partition and hostname pattern are one cluster",
   list(cs) == ["gpu-a"] and set(cs["gpu-a"]["sites"]) == {"site-slurm", "site-slurm2"})
ok("naming machines limits the clusters", set(E.clusters(["site-slurm2"])["gpu-a"]["sites"]) == {"site-slurm2"})
ok("the remote command reads that partition", "scontrol show partition gpu-a" in E.fetch_cmd("gpu-a"))

# parse
snap = E.parse(RAW)
ok("parse: nodes, per-node capacity, QOS limits", snap["nodes"] == ["gpu01", "gpu02"] and snap["cap"]["gpu"] == 2
   and snap["qos"] == {"normal": 2, "part": None} and snap["max_age"] == 7 * 86400)
j200 = next(j for j in snap["jobs"] if j["id"] == "200")
ok("parse: a queued job's base priority is sprio minus its age part", j200["base"] == 800 and j200["age"] == 100 and j200["gpu"] == 2)
ok("parse: durations", E.dur("1-02:00:00") == 26 * H and E.dur("30:00") == 1800 and E.dur("UNLIMITED") is None)

# simulate
pending, timeline = E.simulate(E.parse(RAW))
by = {j["id"]: j for j in pending}
ok("a queued 2-GPU job starts when the running 2-GPU job ends, on that node",
   by["200"].get("sim_start") == NOW + 2 * H and by["200"]["sim_node"] == "gpu01")
ok("a short job that ends before the reservation is backfilled now", by["201"].get("sim_start") == NOW)
ok("a held job never starts", "sim_start" not in by["204"])
ok("the per-user limit holds the user's second 2-GPU job until the first ends",
   by["203"].get("sim_start") is None or by["203"]["sim_start"] >= NOW + 12 * H)
ok("the timeline lists starts in time order", [e["t"] for e in timeline] == sorted(e["t"] for e in timeline))

# history and comparison
d = E.home("gpu-a")
gone = E.record(d, RAW, E.parse(RAW), pending)
ok("first record: the snapshot is kept, no ended jobs yet", gone == [] and os.listdir(os.path.join(d, proto.ETA_FILES["raw"])))
later = NOW + 2 * H + 600
raw2 = snapshot(later, [run_line("101", "bob", "gpu02", 1, NOW - H, NOW + 5 * H),
                        run_line("200", "me", "gpu01", 2, NOW + 2 * H + 300, NOW + 12 * H)], [], [])
snap2 = E.parse(raw2)
pend2, _ = E.simulate(snap2)
gone = E.record(d, raw2, snap2, pend2)
ok("second record: the ended jobs are seen between the two snapshots",
   {g["user"] for g in gone} == {"alice"} and E.load_json(d, "ends", {})["100"]["end_before"] == later)
rows = E.evaluate(E.load_json(d, "starts", {}), E.predictions(d))
ok("the comparison pairs a start with the prediction made 1-48 h before", [(r[0], r[3] - r[2]) for r in rows] == [("200", 300)])
ends = {str(i): dict(user="bob", start=H, limit_end=11 * H, end_after=10 * H, end_before=10 * H) for i in range(3)}
s3 = E.parse(RAW)
E.est_ends(s3, ends)
ok("running jobs end at the share of the limit the user's longer earlier jobs used",
   next(j for j in s3["jobs"] if j["id"] == "101")["est_end"] == NOW - H + 0.9 * 6 * H)

# main, through a fake remote call
shutil.rmtree(os.path.join(TMP, proto.ETA))
calls = []


def fake_rsh(name, cmd):
    calls.append(name)
    return (0, "me\n", "") if cmd == "echo $USER" else (0, RAW, "")


E.rsh, E.reachable = fake_rsh, (lambda name: name == "site-slurm2")
out = io.StringIO()
with contextlib.redirect_stdout(out):
    rc = E.main([])
text = out.getvalue()
ok("main reads once through the reachable machine and caches user names", rc == 0 and calls.count("site-slurm2") == 2
   and "site-slurm" not in calls and E.load_json(E.home(), "users", {}) == {"site-slurm2": "me"})
ok("main reports our next GPU and keeps the snapshot", "Slurm2 (me)" in text and "next GPU:" in text
   and len(E.predictions(E.home("gpu-a"))) == 1)
with contextlib.redirect_stdout(io.StringIO()) as out:
    rc = E.main(["--backtest"])
ok("--backtest reruns the kept snapshots", rc == 0 and "1 snapshots" in out.getvalue())
E.reachable = lambda name: False
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as err:
    rc = E.main(["--record"])
ok("no machine reachable: exit 1 with the reason", rc == 1 and "no machine reachable" in err.getvalue())

shutil.rmtree(TMP, ignore_errors=True)
sys.exit(1 if fails else 0)
