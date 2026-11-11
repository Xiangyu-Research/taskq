#!/usr/bin/env python3
"""Tests for taskq.py: fake queues and fake training results in temp dirs; connects to no machine.  python3 queue/test_taskq.py"""
import glob
import json
import os
import shutil
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata"))
import fixture  # noqa: E402
T = fixture.tmpdir()
TQ, DATA = os.path.join(T, "taskq"), os.path.join(T, "data")
RES = os.path.join(DATA, "results")
ENV = dict(os.environ, TASKQ_DIR=TQ, TASKQ_DATA=DATA, TASKQ_MACHINE="slurm-login")  # in tests this machine holds the queue
ENV.pop("DEMO_RESULTS", None)
sys.path.insert(0, HERE)
fixture.use()
ENV.update(TASKQ_CONFIG=fixture.CONFIG, TASKQ_PLUGINS=fixture.PLUGINS)
os.environ.update(TASKQ_DIR=TQ, TASKQ_LOCK_WAIT="0.05")  # lock retry interval in this process (try_lock cases that must not get it)
import taskq  # noqa: E402
import spec as S  # noqa: E402
import experiments as E  # noqa: E402

ok = True


def check(name, cond):
    global ok
    print(("PASS:" if cond else "FAIL:") + name)
    ok &= bool(cond)


def tq(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=ENV, capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()  # messages go to stderr


def part(ds, tr, f):
    d = os.path.join(RES, ds, f"{tr}__PlanM__full", f"frag_{f}")
    os.makedirs(d, exist_ok=True)
    return d


ALL = "base,alt2,alt"


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for order, nice, key, *env in rows:
            ds, tr, fo = key.split("/")
            p = dict(ds=ds, variant=tr, frag=fo, **(dict(env=env[0]) if env else {}))
            f.write(json.dumps(dict(order=order, nice=nice, key=key, exp=None, recipe="toy", params=p)) + "\n")


tasks = [(10, 0, "D121_x/V_save/0"), (11, 0, "D121_x/V_save/1"),
         (12, 0, "D121_x/V_save/2"), (5, 50, "D103_y/V_save/0"),
         (1, 2000, "D101_z/V_s/0", "alt")]
write_jsonl(os.path.join(TQ, "tasks.jsonl"), tasks)
for t in tasks:
    part(*t[2].split("/"))

# part 0 is done
d0 = part("D121_x", "V_save", "0")
open(os.path.join(d0, "final.ckpt"), "w").close()
os.makedirs(os.path.join(d0, "check"))
open(os.path.join(d0, "check", "summary.json"), "w").write("{}")
# part 1's training log was just updated (someone else is training, e.g. extra training)
open(os.path.join(part("D121_x", "V_save", "1"), "train_log_x.txt"), "w").write("x")

rc, out = tq("next", "--worker", "100", "--envs", ALL)
check("claim by nice first, skip done and freshly logged: claims part 2", rc == 0 and out.startswith("D121_x/V_save/2\n"))
rc, out = tq("next", "--worker", "101", "--envs", ALL)
check("an already claimed task is not claimed again: next is nice 50", rc == 0 and out.startswith("D103_y/"))
rc, out = tq("next", "--worker", "103", "--envs", ALL)
check("claim V_s in order (env alt)", rc == 0 and out.startswith("D101_z/V_s/0\n") and "env=alt" in out)
rc, out = tq("next", "--worker", "102", "--envs", ALL)
check("nothing to claim: exit code 10", rc == 10 and out == "")

# claim expired (heartbeat 30 minutes ago, log not updated either) -> can be claimed again
p = os.path.join(TQ, "claims", taskq.safe("D103_y/V_save/0"), "info")
ci = json.load(open(p))
ci["beat"] = time.time() - 1800
json.dump(ci, open(p, "w"))
rc, out = tq("next", "--worker", "104", "--envs", ALL)
check("expired heartbeat and no log update: claimed again", rc == 0 and out.startswith("D103_y/"))
check("the new claim records the new worker", json.load(open(p))["worker"] == "104")

# next's third line is the token for this claim; after a worker with the same name restarts (a pueue-host pueue worker name does not change), releasing or beating with the old round's token does not touch the new claim
lines = out.splitlines()
check("next's third line is the token, same as in the claim record", len(lines) == 3 and lines[2] == json.load(open(p))["token"])
K103 = "D103_y/V_save/0"
ci = json.load(open(p))
ci["beat"] = time.time() - 600
json.dump(ci, open(p, "w"))
tq("beat", "--worker", "104", "--token", "0ld0ld", K103)
check("old round's token: beat does not change the record", time.time() - json.load(open(p))["beat"] > 500)
tq("release", "--worker", "104", "--token", "0ld0ld", K103)
check("old round's token: cannot release", os.path.exists(p))
rc, _ = tq("missing", "--worker", "104", "--token", "0ld0ld", K103)
check("old round's token: cannot record missing inputs", rc == 1 and os.path.exists(p) and taskq.input_info(K103) is None)
tq("beat", "--worker", "104", "--token", lines[2], K103)
check("this round's token: beat updates", time.time() - json.load(open(p))["beat"] < 10)

# heartbeat expired but the training log was just updated -> still counts as training
key2 = "D121_x/V_save/2"
p2 = os.path.join(TQ, "claims", taskq.safe(key2), "info")
ci = json.load(open(p2))
ci["beat"] = time.time() - 1800
json.dump(ci, open(p2, "w"))
open(os.path.join(part("D121_x", "V_save", "2"), "train_log_y.txt"), "w").write("y")
rc, out = tq("next", "--worker", "105", "--envs", ALL)
check("expired heartbeat but a fresh log: not claimed", rc == 10)

# heartbeat
rc, _ = tq("beat", "--worker", "100", key2)
check("beat updates", rc == 0 and time.time() - json.load(open(p2))["beat"] < 10)
rc, _ = tq("beat", "--worker", "999", key2)
check("another worker's beat does not change the record", json.load(open(p2))["worker"] == "100")

# release: only its own claim can be released; a failure is recorded once
rc, _ = tq("release", "--worker", "999", key2)
check("another worker cannot release", os.path.exists(p2))
rc, _ = tq("release", "--worker", "100", key2, "--failed", "exit code 1")
check("release deletes the claim and records one failure", not os.path.exists(p2) and taskq.failures(key2) == 1)
tq("release", "--worker", "104", "D103_y/V_save/0", "--failed", "exit code 1")
tq("release", "--worker", "104", "D103_y/V_save/0", "--failed", "exit code 1")
rc, _ = tq("next", "--worker", "110", "--envs", ALL)
check("3 failures in 3 hours: the whole queue stops claiming (exit code 12)", rc == 12)
for fp in glob.glob(os.path.join(TQ, "failures", "*")):  # move the failure records to 4 hours ago
    old = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 4 * 3600))
    lines = open(fp).readlines()
    open(fp, "w").write("".join(old + ln[19:] for ln in lines))
for f in os.listdir(part("D121_x", "V_save", "2")):
    os.utime(os.path.join(part("D121_x", "V_save", "2"), f), (time.time() - 3600,) * 2)
rc, out = tq("next", "--worker", "106", "--envs", ALL)
check("1 failure can still be claimed, 2 failures cannot", rc == 0 and out.startswith(key2))
tq("release", "--worker", "106", key2)
rc, out = tq("next", "--worker", "106", "--envs", ALL, "--exclude", key2)
check("a task this job failed on (--exclude) is not claimed again", rc == 10)

# failures count only on the bundle in use: after switching bundles the old bundle's failures no longer count (pausing and the 2-failure limit follow this); the record notes the bundle that ran it
TQC, RC = os.path.join(T, "taskq_code"), os.path.join(T, "root_code")
os.makedirs(os.path.join(TQC, "failures"))
os.makedirs(os.path.join(RC, "code", "B", "scripts", "taskq", "queue"))
os.symlink("B", os.path.join(RC, "code", "current"))
now_c = time.strftime("%F %T")
open(os.path.join(TQC, "failures", "k__a"), "w").write(f"{now_c}\tjob 1\tx\tA\n{now_c}\tjob 2\tx\tA\n")
open(os.path.join(TQC, "failures", "k__b"), "w").write(f"{now_c}\tjob 3\tx\tB\n{now_c}\tjob 4\tx\t\n")
keep = taskq.TQ, taskq.HERE, os.environ.get("TASKQ_ROOT")
taskq.TQ = TQC
check("not in an executor root: all failures count", taskq.recent_failures() == 4 and taskq.failures("k/a") == 2)
os.environ["TASKQ_ROOT"] = RC
check("B is in use: old bundle A's failures do not count, B's and ones not run from a bundle do",
      taskq.code_now() == "B" and taskq.recent_failures() == 2 and taskq.failures("k/a") == 0 and taskq.failures("k/b") == 2)
taskq.HERE = os.path.join(RC, "code", "B", "scripts", "taskq", "queue")
check("the bundle this process runs: from the file being under code/<bundle>", taskq.code_mine() == "B")
taskq.HERE = HERE
check("not running in a bundle: the bundle field is empty", taskq.code_mine() == "")
taskq.TQ, taskq.HERE = keep[0], keep[1]
if keep[2] is None:
    del os.environ["TASKQ_ROOT"]
else:
    os.environ["TASKQ_ROOT"] = keep[2]

# the log did not move after the release: claim again at once; the log moved after the release: someone outside the queue is training
open(os.path.join(part("D121_x", "V_save", "2"), "train_log_y.txt"), "w").write("y")
rc, out = tq("next", "--worker", "120", "--envs", ALL)
check("freshly claimed (fresh log) is claimed as usual", rc == 0 and out.startswith(key2))
tq("release", "--worker", "120", key2)
rc, out = tq("next", "--worker", "121", "--envs", ALL)
check("just released with a log update in the last 15 minutes: also claimable at once", rc == 0 and out.startswith(key2))
tq("release", "--worker", "121", key2)
rel = os.path.join(TQ, "released", taskq.safe(key2))
check("release records the time (released/<key>)", taskq.released_at(key2) is not None)
open(rel, "w").write(f"{time.time() - 10} 121\n")  # release recorded 10 seconds ago; the log updated after that
open(os.path.join(part("D121_x", "V_save", "2"), "train_log_y.txt"), "w").write("y2")
rc, out = tq("next", "--worker", "122", "--envs", ALL)
check("log moved after the release (someone outside the queue is training): not claimed", rc == 10)

# an earlier part just stopped (fresh log, unclaimed) and there is a claimable one behind it: wait for it within the patience window
open(os.path.join(part("D121_x", "V_save", "1"), "train_log_x.txt"), "w").write("x")  # part 1 just stopped
os.remove(os.path.join(TQ, "failures", taskq.safe("D103_y/V_save/0")))  # leave one claimable behind it
rc, out = tq("next", "--worker", "130", "--envs", ALL, "--free-slots", "1", "--patience-until", str(time.time() + 600))
check("1 free slot and 1 just stopped ahead: wait (exit code 11)", rc == 11)
rc, out = tq("next", "--worker", "130", "--envs", ALL, "--free-slots", "3", "--patience-until", str(time.time() + 600))
check("more free slots than just-stopped tasks: claim the one behind as usual", rc == 0 and out.startswith("D103_y/"))
tq("release", "--worker", "130", "D103_y/V_save/0")
rc, out = tq("next", "--worker", "131", "--envs", ALL, "--free-slots", "1", "--patience-until", str(time.time() - 1))
check("patience window passed: claim the one behind", rc == 0 and out.startswith("D103_y/"))
tq("release", "--worker", "131", "D103_y/V_save/0")
os.remove(os.path.join(part("D121_x", "V_save", "1"), "train_log_x.txt"))

for f in os.listdir(part("D121_x", "V_save", "2")):
    os.utime(os.path.join(part("D121_x", "V_save", "2"), f), (time.time() - 3600,) * 2)
rc, out = tq("next", "--worker", "107", "--envs", ALL)
check("claim part 1 in order", rc == 0 and out.startswith("D121_x/V_save/1\n"))
rc, out = tq("next", "--worker", "108", "--envs", ALL)
check("next is part 2 (log updated 1 hour ago, unclaimed)", rc == 0 and out.startswith(key2))
# concurrent claims: start 6 at once, only 1 gets it
procs = [subprocess.Popen([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", f"2{i}", "--envs", ALL],
                          env=ENV, stdout=subprocess.PIPE, text=True) for i in range(6)]
outs = [p.communicate()[0].strip() for p in procs]
check("claiming at the same time, only one gets it", sum(1 for o in outs if o) == 1)

# ---------------- take --assign, add, set-nice, kind any ----------------
TQ2 = os.path.join(T, "taskq2")
ENV2 = dict(ENV, TASKQ_DIR=TQ2)


def tq2(*args, env=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=env or ENV2, capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()  # messages go to stderr


os.makedirs(TQ2)
write_jsonl(os.path.join(TQ2, "tasks.jsonl"), [(486191, 0, "D116_gamma/V_save/4"),
                                               (486300, 2000, "D101_alpha/V_s/0", "alt")])
os.environ["TASKQ_DIR"] = TQ2
rc, out = tq2("add", "--nice", "50", "--exp", "arch_m", "--only", "frag=0")
rc2, out2 = tq2("add", "--nice", "50", "--exp", "arch_m", "--only", "frag=0")
ts = taskq.read_tasks(os.path.join(TQ2, "tasks.jsonl"))
um = ts[1]
check("add --exp: follows the experiment table (arch_m: env alt2, experiment arch_m), ordered by nice, existing ones skipped",
      rc == 0 and [t["key"].split("/")[1] for t in ts] == ["V_save", "V_m", "V_s"]
      and um["exp"] == "arch_m" and um["params"]["env"] == "alt2" and um["order"] == 486301 and "exists, skipped" in out2)
rc, out = tq2("add", "--nice", "0", "--exp", "nosuch")
check("add: an experiment not in the table is an error", rc != 0)
rc, out = tq2("add", "--nice", "0", "D101_alpha/V_m/0")
check("add: no --exp / --recipe is an error", rc != 0)
rc, out = tq2("set-nice", "--nice", "100", "D101_alpha/V_s/0")
ts = taskq.read_tasks(os.path.join(TQ2, "tasks.jsonl"))
check("set-nice: reorders after the nice change", rc == 0 and "V_s" in ts[2]["key"] and ts[2]["nice"] == 100)
rc, out = tq2("set-nice", "--nice", "1", "D101_alpha/V_save/9")
check(f"set-nice: absent key exits {taskq.GONE}", rc == taskq.GONE)

UM = "D101_alpha/V_m/0"
SWK = "D101_alpha/V_s/0"
rc, out = tq2("take", "--worker", "pueue-host", "--assign", UM)
ci = json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))
check("take --assign: assign one given task, record worker and assigned", rc == 0 and out.startswith(UM) and ci["worker"] == "pueue-host" and ci["assigned"] == "pueue-host")
rc, out = tq2("next", "--worker", "500", "--envs", ALL, "--exclude", "D101_alpha/V_s/0", "D116_gamma/V_save/4")
check("assigned to pueue-host: Slurm jobs do not claim it", rc == 10)
ci["beat"] = time.time() - 3 * 86400
json.dump(ci, open(os.path.join(TQ2, "claims", taskq.safe(UM), "info"), "w"))
rc, out = tq2("next", "--worker", "500", "--envs", ALL, "--exclude", "D101_alpha/V_s/0", "D116_gamma/V_save/4")
check("assigned tasks do not expire: heartbeat 3 days ago still counts as claimed", rc == 10)
rc, out = tq2("take", "--worker", "pueue-host", "--assign", UM)
ci2 = json.load(open(os.path.join(TQ2, "claims", taskq.safe(UM), "info")))
check("take on one already assigned to it: continues, claim time unchanged", rc == 0 and ci2["since"] == ci["since"] and time.time() - ci2["beat"] < 10)
rc, out = tq2("take", "--worker", "other", "--assign", UM)
check("take on one assigned to someone else: cannot assign (exit code 10)", rc == 10)
rc, _ = tq2("take", "--worker", "pueue-host", UM)
check("take without --assign: argument error", rc == 2)
rc, _ = tq2("take", "--worker", "other", "--assign", "--from", "pueue-host", UM)
check("take has no --from (an assigned task cannot be reassigned in the queue): argument error", rc == 2)
rc, out = tq2("set", UM, "env=alt")
check("set on a task assigned to an executor: says how to send the new spec there", rc == 0 and "taskq site resend pueue-host" in out)
tq2("set", UM, "env=")
tq2("release", "--worker", "pueue-host", UM)
rc, out = tq2("next", "--worker", "501", "--envs", ALL, "--exclude", "D101_alpha/V_s/0", "D116_gamma/V_save/4")
check("after release a Slurm job can claim it again", rc == 0 and out.startswith(UM))
rc, out = tq2("take", "--worker", "pueue-host", "--assign", "D116_gamma/V_save/9")
check("take on a key not in the queue: exit code 10", rc == 10)
d = part("D116_gamma", "V_save", "4")
open(os.path.join(d, "final.ckpt"), "w").close()
os.makedirs(os.path.join(d, "check"), exist_ok=True)
open(os.path.join(d, "check", "summary.json"), "w").write("{}")
rc, out = tq2("take", "--worker", "pueue-host", "--assign", "D116_gamma/V_save/4")
check("take on a done task: cannot claim", rc == 10 and "done" in out)
rc, out = tq2("next", "--worker", "502", "--envs", ALL)
check("kind any: claims any kind (gets V_s)", rc == 0 and out.startswith("D101_alpha/V_s/0"))
s2 = taskq.S.json_in(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json", "--internal"], env=ENV2, capture_output=True, text=True).stdout)
row = next(t for t in s2["tasks"] if t["key"] == UM)
rc, _ = tq2("release", "--worker", "pueue-host", SWK, "--skip", "training job 5 Failed (1)")
s3 = taskq.S.json_in(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json", "--internal"], env=ENV2, capture_output=True, text=True).stdout)
row3 = next(t for t in s3["tasks"] if t["key"] == SWK)
check("release --skip: recorded in skips/, not a failure; status --json has skipped_by and the 24-hour count",
      rc == 0 and row3["skipped_by"] == ["pueue-host"] and row3["failures"] == 0
      and s3["recent_skips"] == {"pueue-host": 1} and not os.path.exists(os.path.join(TQ2, "failures", taskq.safe(SWK))))
check("status --json has worker, host, beat", row["worker"] == "501" and "host" in row and row["beat"])

# the way workers call it (the arguments consume.py passes) does not change: a running worker calls the repo's taskq.py every time
rc, out = tq2("next", "--worker", "503", "--envs", ALL, "--free-slots", "2", "--patience-until", str(time.time() + 1200))
check("worker: next with --free-slots / --patience-until, exit code 10 when nothing can be claimed", rc == 10)
rc, _ = tq2("beat", "--worker", "502", "D101_alpha/V_s/0")
check("worker: beat exits 0", rc == 0)
rc, _ = tq2("release", "--worker", "502", "D101_alpha/V_s/0")
check("worker: release exits 0 and deletes the record", rc == 0 and not os.path.exists(os.path.join(TQ2, "claims", taskq.safe("D101_alpha/V_s/0"))))
rc, _ = tq2("release", "--worker", "501", UM, "--failed", "exit code 1")
check("worker: release --failed records a failure", rc == 0 and len(open(os.path.join(TQ2, "failures", taskq.safe(UM))).readlines()) == 1)
rc, out = tq2("next", "--worker", "504", "--envs", ALL, "--exclude", UM)
check("a task skipped by pueue-host (skips/) is still claimed by Slurm jobs", rc == 0 and out.startswith(SWK))
tq2("release", "--worker", "other-host", SWK, "--skip", "does not fit")


def skipped_by(k):
    s = taskq.S.json_in(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json", "--internal"], env=ENV2,
                                       capture_output=True, text=True).stdout)
    return next(t for t in s["tasks"] if t["key"] == k)["skipped_by"]


sk0 = skipped_by(SWK)
rc1, _ = tq2("unskip", "--worker", "other-host", SWK)
sk1 = skipped_by(SWK)
rc2, _ = tq2("unskip", SWK)
check("unskip --worker: removes only that machine's skip record; without --worker: removes all and deletes the record file",
      sorted(sk0) == ["other-host", "pueue-host"] and rc1 == 0 and sk1 == ["pueue-host"] and rc2 == 0 and skipped_by(SWK) == []
      and not os.path.exists(os.path.join(TQ2, "skips", taskq.safe(SWK))))
rc, out = tq2("beat", "--worker", "504", "--strict", SWK)
check("beat --strict: the claim is its own, exit code 0", rc == 0 and out == "")
rc, out = tq2("beat", "--worker", "999", "--strict", SWK, "gone/x/0")
check("beat --strict: claims are someone else's or gone -> exit code 13, prints those keys", rc == 13 and out.split("\n") == [SWK, "gone/x/0"])
os.makedirs(os.path.join(TQ2, "claims", taskq.safe("gone/x/0")))
rc, out = tq2("beat", "--worker", "999", "--strict", "gone/x/0")
check("beat --strict: claim dir exists but the record is unreadable, not counted as lost", rc == 0)
os.rmdir(os.path.join(TQ2, "claims", taskq.safe("gone/x/0")))
os.makedirs(os.path.join(TQ2, "lock"))
open(os.path.join(TQ2, "lock", "owner"), "w").write(f"{socket.gethostname()}\t{os.getpid()}\tx\n")
t0 = time.time()
rc, _ = tq2("release", "--worker", "504", "--lock-wait", "1", SWK)
check("release --lock-wait 1: the lock is held, gives up after about 1 second (non-zero exit code) and does not release",
      rc != 0 and time.time() - t0 < 5 and os.path.exists(os.path.join(TQ2, "claims", taskq.safe(SWK), "info")))
shutil.rmtree(os.path.join(TQ2, "lock"))
info_p = os.path.join(TQ2, "claims", taskq.safe(SWK), "info")
ci3 = json.load(open(info_p))
ci3.pop("ttl", None)
ci3["beat"] = time.time() - 600
json.dump(ci3, open(info_p, "w"))
rc, _ = tq2("next", "--worker", "505", "--envs", ALL, "--exclude", UM)
check("worker: a claim record without ttl, heartbeat 10 minutes ago still counts as training", rc == 10)
ci3["beat"] = time.time() - 1300
json.dump(ci3, open(info_p, "w"))
rc, out = tq2("next", "--worker", "505", "--envs", ALL, "--exclude", UM)
check("worker: a claim record without ttl, heartbeat over 20 minutes ago expires and someone else can claim it", rc == 0 and out.startswith(SWK))
NOQ = os.path.join(T, "no_tasks")
os.makedirs(NOQ)
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "506", "--envs", ALL],
                   env=dict(ENV, TASKQ_DIR=NOQ), capture_output=True, text=True)
check("no task table: next exits 2 (an error, not 10 'no tasks', so jobs do not end early because of it)", r.returncode == 2)

EMPTY = os.path.join(T, "hub_tq")  # on the hub: an empty queue dir without a task table
os.makedirs(EMPTY)
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "add", "--nice", "100", "--recipe", "toy",
                    "ds=D113_delta", "variant=V_save", "frag=0"],
                   env=dict(os.environ, TASKQ_DIR=EMPTY, TASKQ_MACHINE="hub"), capture_output=True, text=True)
check("add on the hub (the machine with the hub queue): creates the hub queue locally (no forwarding)", r.returncode == 0 and "added:" in r.stdout
      and "D113_delta" in open(os.path.join(EMPTY, "tasks.jsonl")).read())
NOWHERE = os.path.join(T, "no_queue_here")
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "add", "--nice", "100", "--recipe", "toy",
                    "ds=D113_delta", "variant=V_save", "frag=0"],
                   env=dict(os.environ, TASKQ_DIR=NOWHERE, TASKQ_MACHINE="pueue-host"), capture_output=True, text=True)
check("not on the hub and no local queue: add exits 2, says (on stderr) that the hub queue is on the hub, creates no dir", r.returncode == 2 and "the hub queue is on hub" in r.stderr
      and not os.path.exists(NOWHERE))
J = '{"tasks": [], "enabled": true}'
check("other output around the @@JSON line: only that line is read", taskq.S.json_in(f"warning: x {{1}}\n@@JSON {J}\n{{other}}\n") == json.loads(J))
check("no @@JSON line: not read (does not look for the first {)", taskq.S.json_in(f"x\n{J}\n") is None)


# ---------------- task specs (next, set, show, add --exp / --recipe) ----------------
TQ3 = os.path.join(T, "taskq3")
ENV3 = dict(ENV, TASKQ_DIR=TQ3)


def tq3(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=ENV3, capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


os.makedirs(TQ3)
open(os.path.join(TQ3, "tasks.jsonl"), "w").close()
rc, out, _ = tq3("add", "--nice", "0", "--exp", "t15b", "--only", "frag=1,2")
ts = taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))
check("add --exp --only: expands from the experiment table, records the experiment id", rc == 0 and [t["key"] for t in ts] ==
      ["D111_beta/V_save_n470/1", "D111_beta/V_save_n470/2"] and ts[0]["exp"] == "t15b")
rc, out, _ = tq3("add", "--nice", "5", "--recipe", "command", "key=toy/a", "cmd=python3 -c 1", "env=stats", "out=toy/a", 'done=["DONE"]')
check("add --recipe: a task outside the experiment table", rc == 0 and "toy/a" in out and "env stats" in out)
rc, out, err = tq3("add", "--nice", "5", "--recipe", "toy", "ds=D101_alpha", "frag=0", "variantt=x")
check("add --recipe: a wrong param name is an error and adds nothing", rc == 2 and "variantt" in err and len(taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))) == 3)
rc, out, err = tq3("add", "--nice", "5", "--recipe", "command", "key=toy__a", "cmd=python3 -c 1", "env=stats", "out=toy/a2", 'done=["DONE"]')
check("add: the file name clashes with an existing task (toy/a and toy__a) -> error, not added", rc == 2 and "file name" in err
      and len(taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))) == 3)
rc, out, _ = tq3("add", "--nice", "5", "--recipe", "command", "key=toy/a", "cmd=python3 -c 1", "env=stats", "out=toy/a", 'done=["DONE"]')
rc2, _, out2 = tq3("add", "--nice", "5", "--recipe", "command", "key=toy/a", "cmd=python3 -c 2", "env=stats", "out=toy/a", 'done=["DONE"]')
check("add --recipe with the same name: same spec -> skipped; different spec -> reported, exit code 2, original unchanged",
      rc == 0 and "exists, skipped" in out and rc2 == 2 and "different spec" in out2
      and next(x for x in taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl")) if x["key"] == "toy/a")["params"]["cmd"] == "python3 -c 1")
check("same_file: two keys in one batch mapping to the same file name are caught", taskq.same_file(["x/y", "x__y"], set()) and not taskq.same_file(["x/y"], {"x/y"}))
rc, out, _ = tq3("next", "--worker", "700", "--envs", "base,alt")
lines = out.splitlines()
check("next: prints the key and run_task.sh arguments", rc == 0 and lines[0] == "D111_beta/V_save_n470/1"
      and lines[1] == "toy ds=D111_beta variant=V_save_n470 frag=1")
rc, out, _ = tq3("next", "--worker", "701", "--envs", "base,alt")
rc2, out2, _ = tq3("next", "--worker", "702", "--envs", "base")
check("next: an env this machine does not have (stats) is not claimed", rc == 0 and rc2 == 10)
rc, out, _ = tq3("next", "--worker", "703", "--envs", "stats")
check("next: a machine with this env can claim it; JSON values are passed as is", rc == 0 and out.splitlines()[1] == "command key=toy/a 'cmd=python3 -c 1' env=stats out=toy/a 'done=[\"DONE\"]'")
tq3("release", "--worker", "700", "D111_beta/V_save_n470/1")
rc, out, _ = tq3("set", "D111_beta/V_save_n470/1", "env=alt2")
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQ3, "tasks.jsonl"))}
check("set: changes one task's params", rc == 0 and ts["D111_beta/V_save_n470/1"]["params"]["env"] == "alt2")
os.environ["TASKQ_DIR"] = TQ3
check("spec lookup: the queue (with taskq set changes) before the experiment table",
      taskq.S.lookup("D111_beta/V_save_n470/1")["env"] == "alt2" != E.tasks()["D111_beta/V_save_n470/1"]["spec"]["env"])
os.environ["TASKQ_DIR"] = TQ2
rc, out, _ = tq3("next", "--worker", "705", "--envs", "alt2")
check("next: after the env change it is claimed with the new env", rc == 0 and out.startswith("D111_beta/V_save_n470/1"))
tq3("release", "--worker", "705", "D111_beta/V_save_n470/1")
rc, out, _ = tq3("set", "D111_beta/V_save_n470/1", "env=")
rc, out, _ = tq3("next", "--worker", "706", "--envs", "base")
check("set k= (empty value) deletes the param and returns to the default env", rc == 0 and out.startswith("D111_beta/V_save_n470/1\n"))
rc, out, _ = tq3("set", "D111_beta/V_save_n470/1", "frag=3")
check("set: a change that would rename the task is refused", rc == 2)
rc, out, _ = tq3("show", "--json", "toy/a")
sj = taskq.S.json_in(out)
check("show --json: full spec on the line starting with @@JSON", rc == 0 and out.startswith("@@JSON ") and sj["out"] == "toy/a" and sj["env"] == "stats")
rc, out, _ = tq3("show", "D111_beta/V_save_n470/2")
check("show: spec, state, run_task.sh form", rc == 0 and "claimed" in out and "run_task.sh toy" in out and "train.sh" in out)
d = os.path.join(DATA, "toy", "a")
os.makedirs(d)
open(os.path.join(d, "DONE"), "w").close()
s3 = taskq.S.json_in(subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json", "--internal"], env=ENV3, capture_output=True, text=True).stdout)
st3 = {t["key"]: t for t in s3["tasks"]}
check("status --json: includes the spec; a command task is done by its done files", st3["toy/a"]["state"] == "done" and st3["toy/a"]["spec"]["recipe"] == "command"
      and st3["D111_beta/V_save_n470/2"]["spec"]["out"].startswith("results/D111_beta/"))
with open(os.path.join(TQ3, "tasks.jsonl"), "a") as f:
    f.write(json.dumps(dict(order=99, nice=1, key="bad/x", exp=None, recipe="toy", params=dict(frag="0"))) + "\n")
rc, out, _ = tq3("status")
check("a task that cannot be resolved shows bad and does not affect others", rc == 0 and "bad" in out)

# machines restricts which machines may claim it (inference only on Slurm)
tq3("add", "--nice", "-100", "--recipe", "command", "key=toy/m", "cmd=true", "env=base", "out=toy/m", 'done=["DONE"]', 'machines=["slurm-login"]')
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "710", "--envs", "base"],
                   env=dict(ENV3, TASKQ_MACHINE="pueue-host"), capture_output=True, text=True)
check("next: not claimed when machines does not list this machine", r.returncode == 10)
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "711", "--envs", "base"],
                   env=dict(ENV3, TASKQ_MACHINE="slurm-login"), capture_output=True, text=True)
check("next: claimed by nice order when machines lists this machine", r.returncode == 0 and r.stdout.startswith("toy/m"))
# device=cpu tasks go only to CPU workers (--device cpu); without --device (GPU workers, older running workers) they are not claimed
tq3("add", "--nice", "-200", "--recipe", "command", "key=toy/c", "cmd=true", "env=base", "out=toy/c", 'done=["DONE"]', "device=cpu")
rc, out, _ = tq3("next", "--worker", "712", "--envs", "base")
check("next: without --device (GPU worker), device=cpu tasks are not claimed even when first", rc != 0 or not out.startswith("toy/c"))
if rc == 0:
    tq3("release", "--worker", "712", out.splitlines()[0])
rc, out, _ = tq3("next", "--worker", "713", "--envs", "base", "--device", "cpu")
check("next --device cpu: claims only device=cpu", rc == 0 and out.startswith("toy/c\n") and "device=cpu" in out)
rc, out, _ = tq3("next", "--worker", "714", "--envs", "base", "--device", "cpu")
check("next --device cpu: no other CPU-only task -> 10", rc == 10)
rc, out, _ = tq3("add", "--nice", "-200", "--recipe", "command", "key=toy/e", "cmd=true", 'env=["base","stats"]', "out=toy/e",
                 'done=["DONE"]', "device=cpu")
check("add: env can be a list of several envs", rc == 0 and "env base,stats" in out)
rc, out, _ = tq3("next", "--worker", "715", "--envs", "base", "--device", "cpu")
check("next: an env in the list that is not ready -> not claimed", rc == 10)
rc, out, _ = tq3("next", "--worker", "715", "--envs", "base,stats", "--device", "cpu")
check("next: all envs in the list are ready -> claimed", rc == 0 and out.startswith("toy/e\n"))
rc, out, _ = tq3("status")
check("status: an env list shows as a,b", rc == 0 and "base,stats" in out)

# do not claim when the main data dir cannot be read
# add without --nice: --exp uses the table's nice, anything else is an error (a separate queue, does not affect the above)
TQ3B = os.path.join(T, "taskq3b")
os.makedirs(TQ3B)
open(os.path.join(TQ3B, "tasks.jsonl"), "w").close()


def tq3b(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ3B), capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


rc, out, _ = tq3b("add", "--exp", "t15g", "--only", "frag=0")
ts = taskq.read_tasks(os.path.join(TQ3B, "tasks.jsonl"))
check("add --exp without --nice: uses the nice from the experiment table", rc == 0 and [t["key"] for t in ts] == ["D116_gamma/V_save_n470/0"]
      and ts[0]["nice"] == E.experiment("t15g")["nice"])
rc, out, err = tq3b("add", "--recipe", "command", "key=toy/n", "cmd=true", "env=base", "out=toy/n", 'done=["DONE"]')
check("add --recipe without --nice: error, not added", rc != 0 and "--nice" in err and len(taskq.read_tasks(os.path.join(TQ3B, "tasks.jsonl"))) == 1)
want = [t["key"] for t in E.expand(E.experiment("t16"))]
rc, out, _ = tq3b("add", "--exp", "t16")
rc2, out2, _ = tq3b("add", "--exp", "t16")
ts = [t for t in taskq.read_tasks(os.path.join(TQ3B, "tasks.jsonl")) if t["exp"] == "t16"]
check("add --exp (whole experiment): all expanded tasks added with the table's nice; a second add skips them all",
      rc == 0 and rc2 == 0 and len(want) == 5 and sorted(t["key"] for t in ts) == sorted(want)
      and {t["nice"] for t in ts} == {E.experiment("t16")["nice"]} and out2.count("exists, skipped") == len(want)
      and len(taskq.read_tasks(os.path.join(TQ3B, "tasks.jsonl"))) == 1 + len(want))

TQ4, D4 = os.path.join(T, "taskq4"), os.path.join(T, "data4")
os.makedirs(D4)
ks4 = ["D121_x/V_save/0", "D121_x/V_save/1", "D121_x/V_save/2"]
write_jsonl(os.path.join(TQ4, "tasks.jsonl"), [(i, 0, k) for i, k in enumerate(ks4)])
os.makedirs(os.path.join(TQ4, "failures"))
for k in ks4[:2]:
    with open(os.path.join(TQ4, "failures", taskq.safe(k)), "w") as f:
        f.write(f"{time.strftime('%F %T')}\tjob 1\tx\t\n" * 2)


def tq4(*args, root=D4):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ4, TASKQ_DATA=root),
                       capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


rc, out, err = tq4("next", "--worker", "800", "--envs", ALL, root=os.path.join(T, "nosuch"))
check("next: does not claim when the data dir cannot be read (exit code 12, says why)", rc == 12 and "cannot read" in err)
rc, out, err = tq4("take", "--worker", "pueue-host", "--assign", ks4[2], root=os.path.join(T, "nosuch"))
check("take: does not claim when the data dir cannot be read", rc == 10 and "cannot read" in out and not os.path.exists(os.path.join(TQ4, "claims", taskq.safe(ks4[2]))))
rc, out, err = tq4("next", "--worker", "801", "--envs", ALL)
check("4 recent failures: paused", rc == 12)

# ---------------- claim made only the dir, info was never written (the claiming process was killed) ----------------
TQ5, D5 = os.path.join(T, "taskq5"), os.path.join(T, "data5")
k5 = "D121_x/V_save/3"
write_jsonl(os.path.join(TQ5, "tasks.jsonl"), [(1, 0, k5)])
half = os.path.join(TQ5, "claims", taskq.safe(k5))
os.makedirs(half)
os.makedirs(D5, exist_ok=True)


def tq5(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ5, TASKQ_DATA=D5),
                       capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()  # messages go to stderr


rc, out = tq5("next", "--worker", "900", "--envs", ALL)
check("half claim: one just made (within 5 minutes) counts as being claimed, not taken", rc == 10)
rc, out = tq5("take", "--worker", "pueue-host", "--assign", k5)
check("half claim: take does not take a fresh one either", rc == 10)
os.utime(half, (time.time() - 600,) * 2)
rc, out = tq5("status")
check("half claim: one made 10 minutes ago counts as expired", "stale" in out or "expired" in out or "incomplete" in out)
rc, out = tq5("next", "--worker", "901", "--envs", ALL)
ci5 = json.load(open(os.path.join(half, "info")))
check("half claim: claimed as usual after expiry, info written in full", rc == 0 and out.startswith(k5 + "\n") and ci5["worker"] == "901")
shutil.rmtree(half)
os.makedirs(half)
os.utime(half, (time.time() - 600,) * 2)
rc, out = tq5("take", "--worker", "pueue-host", "--assign", k5)
ci5 = json.load(open(os.path.join(half, "info")))
check("half claim: take can also take an expired one", rc == 0 and ci5["worker"] == "pueue-host" and ci5["assigned"] == "pueue-host")
check("writing info leaves no temp file", os.listdir(half) == ["info"])

# ---------------- remove (delete steps queued on a stopped Slurm machine and do them elsewhere) ----------------
TQ6, D6 = os.path.join(T, "taskq6"), os.path.join(T, "data6")
kt = "D121_x/V_save/4"
os.makedirs(os.path.join(TQ6), exist_ok=True)
part(*kt.split("/"))


def cmdtask(order, key, device="gpu"):
    return dict(order=order, nice=-100, key=key, exp=None, recipe="command",
                params=dict(key=key, cmd="true", env="base", out=f"out/{key}", done=[".done"], device=device))


rows = [dict(order=1, nice=0, key=kt, exp=None, recipe="toy", params=dict(ds="D121_x", variant="V_save", frag="4")),
        cmdtask(2, "infer/x/frag_4"), cmdtask(3, "eval/x/frag_4", "cpu")]
with open(os.path.join(TQ6, "tasks.jsonl"), "w") as f:
    f.writelines(json.dumps(r) + "\n" for r in rows)


def tq6(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ6, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()


def st6():
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json", "--internal"], env=dict(ENV, TASKQ_DIR=TQ6, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return {t["key"]: (t["state"], t["note"]) for t in taskq.S.json_in(r.stdout)["tasks"]}


rc, out = tq6("remove", "nosuch/key")
check("remove: an absent key exits 4", rc == taskq.GONE and "not in queue" in out)
rows2 = rows + [dict(order=5, nice=-100, key="free/z", exp=None, recipe="command",
                     params=dict(key="free/z", cmd="true", env="base", out="out/free/z", done=[".done"], device="cpu"))]
with open(os.path.join(TQ6, "tasks.jsonl"), "a") as f:
    f.write(json.dumps(rows2[-1]) + "\n")
rc, out = tq6("next", "--worker", "970", "--envs", ALL, "--device", "cpu", "--exclude", "eval/x/frag_4")
rc2, out2 = tq6("remove", "free/z")
check("remove: a claimed task is not removed (exit code 3)", rc == 0 and out.startswith("free/z\n") and rc2 == taskq.REFUSED and "claimed" in out2
      and "free/z" in st6())
rc, out = tq6("remove", "infer/x/frag_4", "eval/x/frag_4", "nosuch/key")
s6 = st6()
check("remove: several in one batch -> all removed, others untouched; ones already absent are skipped, exit code 0", rc == 0 and "infer/x/frag_4" not in s6
      and "eval/x/frag_4" not in s6 and kt in s6 and "free/z" in s6 and "not in queue: nosuch/key" in out)

# ---------------- hub queue on the hub, assigning to an executor (take --assign) ----------------
TQ7 = os.path.join(T, "taskq7")
os.makedirs(TQ7)
kt7 = "D121_x/V_save/3"
r7 = [dict(order=1, nice=0, key=kt7, exp=None, recipe="toy", params=dict(ds="D121_x", variant="V_save", frag="3")),
      cmdtask(2, "infer/x/frag_3")]
with open(os.path.join(TQ7, "tasks.jsonl"), "w") as f:
    f.writelines(json.dumps(r) + "\n" for r in r7)


def tq7(*args, env=None, stdin=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQ7, TASKQ_DATA=DATA, **(env or {})),
                       capture_output=True, text=True, input=stdin)
    return r.returncode, (r.stdout + r.stderr).strip()


def st7(d=TQ7):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json", "--internal"], env=dict(ENV, TASKQ_DIR=d, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return {t["key"]: t for t in taskq.S.json_in(r.stdout)["tasks"]}


rc, out = tq7("take", "--worker", "slurm-login", "--assign", "infer/x/frag_3")
s7 = st7()
check("take --assign: assigned -> claimed, records assigned", rc == 0 and s7["infer/x/frag_3"]["assigned"] == "slurm-login"
      and s7["infer/x/frag_3"]["state"] == "claimed")
rc, out = tq7("take", "--worker", "slurm-login", "--assign", kt7)
ci7 = json.load(open(os.path.join(TQ7, "claims", taskq.safe(kt7), "info")))
ci7["beat"] = time.time() - 30 * 86400
json.dump(ci7, open(os.path.join(TQ7, "claims", taskq.safe(kt7), "info"), "w"))
s7 = st7()
check("take --assign: assigned tasks do not expire (heartbeat 30 days ago still claimed)", rc == 0 and s7[kt7]["state"] == "claimed" and "ttl" not in ci7
      and s7[kt7]["assigned"] == "slurm-login")
rc, out = tq7("take", "--worker", "slurm-login", "--assign", kt7)
check("take --assign: assigning one already its own succeeds again", rc == 0)
rc, out = tq7("take", "--worker", "pueue-host", "--assign", kt7)
check("take --assign: cannot assign one assigned to someone else", rc == 10)
rc, out = tq7("next", "--worker", "960", "--envs", ALL)
check("workers do not claim tasks assigned in the hub queue", rc == 10)
rc, out = tq7("remove", kt7, "infer/x/frag_3")
check("remove: an assigned task is not removed, says stop first", rc == taskq.REFUSED and "stop it first" in out)
rc, out = tq7("stop", kt7)
check("stop: an assigned task says the hub stops it on its next check", rc == 0 and "assigned to slurm-login" in out)
rc, out = tq7("take", "--worker", "slurm-login", "--assign", kt7)  # stopped: even its current holder cannot claim it
check("take: a stopped task (even one it holds) is not assigned", rc == 10 and "stopped" in out)
tq7("resume", kt7)

# missing inputs: a worker records missing -> input, not claimed, not a failure; input-ok clears it (executor queue, after relay.py re-sends)
EQ = os.path.join(T, "exec7")
os.makedirs(EQ)
open(os.path.join(EQ, "ENABLED"), "w").close()
with open(os.path.join(EQ, "tasks.jsonl"), "w") as f:
    f.write(json.dumps(r7[0]) + "\n")


def tqe(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=EQ, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()


rc, out = tqe("next", "--worker", "963", "--envs", ALL)
rc1, _ = tqe("missing", "--worker", "999", "--why", "x", kt7)
rc2, _ = tqe("missing", "--worker", "963", "--why", "job 963 checked before starting", kt7)
sb = st7(EQ)
check("missing inputs: cannot record for a task it does not hold; its own -> input, released, not a failure", rc == 0 and out.startswith(kt7 + "\n") and rc1 == 1 and rc2 == 0
      and sb[kt7]["state"] == "input" and "missing inputs" in sb[kt7]["note"] and not sb[kt7]["worker"] and sb[kt7]["failures"] == 0
      and sb[kt7]["input_since"])
rc, out = tqe("next", "--worker", "964", "--envs", ALL)
check("missing inputs: not claimed again", rc == 10 or not out.startswith(kt7 + "\n"))
rc, out = tqe("input-ok", kt7)
check("missing inputs: input-ok clears the marker -> claimable as usual", rc == 0 and st7(EQ)[kt7]["state"] == "free")

# ---------------- add --file (task sheet) ----------------
TQF = os.path.join(T, "taskqf")
os.makedirs(TQF)
open(os.path.join(TQF, "tasks.jsonl"), "w").close()


def tqf(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQF),
                       capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


sheet = dict(key="guest/alice/toy", owner="alice", env="base", cmd="python3 train.py --data {data}/guest/alice/in --out {data}/guest/alice/toy",
             inputs=["guest/alice/in"], out="guest/alice/toy", done=["DONE"], progress="progress_json", resume=["ckpt_*.pt"],
             res=dict(gpus=1, cpus=8, mem="32G"), nice=40)
fn = os.path.join(T, "job.json")
json.dump(sheet, open(fn, "w"))
rc, out, _ = tqf("add", "--file", fn)
tf = taskq.read_tasks(os.path.join(TQF, "tasks.jsonl"))
check("add --file: one task sheet -> a command task, nice from the sheet", rc == 0 and len(tf) == 1 and tf[0]["key"] == "guest/alice/toy"
      and tf[0]["recipe"] == "command" and tf[0]["nice"] == 40 and "nice" not in tf[0]["params"])
sf = S.resolve(tf[0]["recipe"], tf[0]["params"])
check("add --file: inputs, res, resume, owner go into the spec", sf["inputs"] == ["guest/alice/in"]
      and sf["res"] == dict(gpus=1, cpus=8, mem="32G") and sf["resume"] == ["ckpt_*.pt"] and sf["owner"] == "alice")
check("add --file: without relay in the sheet, inputs use the default path (not added to params)", sf["relay"] == "path" and "relay" not in tf[0]["params"])
json.dump([dict(sheet, key="guest/alice/b", out="guest/alice/b"), dict(sheet, key="guest/alice/c", out="guest/alice/c")], open(fn, "w"))
rc, out, _ = tqf("add", "--nice", "7", "--file", fn)
tf = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQF, "tasks.jsonl"))}
check("add --file: a list of task sheets, --nice wins", rc == 0 and tf["guest/alice/b"]["nice"] == 7 and tf["guest/alice/c"]["nice"] == 7)
json.dump(dict(sheet, key="guest/alice/d", res=dict(gpu=1)), open(fn, "w"))
rc, _, err = tqf("add", "--file", fn)
check("add --file: a wrong res field is an error and adds nothing", rc == 2 and "res" in err and len(taskq.read_tasks(os.path.join(TQF, "tasks.jsonl"))) == 3)
json.dump(dict(sheet, key="guest/alice/e", nice=None), open(fn, "w"))
rc, out, err = tqf("add", "--file", fn)
check("add --file: no nice in the sheet or on the command line -> error, not added", rc != 0 and len(taskq.read_tasks(os.path.join(TQF, "tasks.jsonl"))) == 3)
open(fn, "w").write("{oops")
rc, _, err = tqf("add", "--nice", "1", "--file", fn)
check("add --file: not JSON -> error", rc == 2 and "task sheet" in err)

# ---------------- locks, take --assign, old-token release ----------------
TQR = os.path.join(T, "taskqr")
os.makedirs(TQR)


def tqr(*args):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQR, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()


def strl():
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json", "--internal"], env=dict(ENV, TASKQ_DIR=TQR, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return {t["key"]: (t["state"], t["note"]) for t in taskq.S.json_in(r.stdout)["tasks"]}


def try_lock(tries=4):
    try:
        lk = taskq.Lock(tries=tries).__enter__()
    except SystemExit:
        return None
    return lk


tq_orig, taskq.TQ = taskq.TQ, TQR
LK = os.path.join(TQR, "lock")
holder = subprocess.Popen([sys.executable, "-c", "import sys, taskq\nwith taskq.Lock():\n    print('held', flush=True)\n    sys.stdin.read()"],
                          env=dict(ENV, TASKQ_DIR=TQR, PYTHONPATH=HERE), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
check("lock: another process gets the lock", holder.stdout.readline().strip() == "held")
os.utime(LK, (time.time() - 121,) * 2)
lk = try_lock()
check("lock: holder is on this host and alive, lock dir untouched for 2 minutes is not taken", lk is None and os.path.isdir(LK))
if lk:
    lk.__exit__()
holder.stdin.close()
holder.wait(timeout=30)
check("lock: the holder removes its own lock on exit", not os.path.exists(LK))
dead = subprocess.Popen([sys.executable, "-c", "pass"])
dead.wait()
os.mkdir(LK)
open(os.path.join(LK, "owner"), "w").write(f"{socket.gethostname()}\t{dead.pid}\tx\n")
os.utime(LK, (time.time() - 121,) * 2)
lk = try_lock()
check("lock: the holder's process is gone, taken as usual after 2 minutes", lk is not None)
if lk:
    lk.__exit__()
os.mkdir(LK)
open(os.path.join(LK, "owner"), "w").write(f"elsewhere\t{os.getpid()}\tx\n")
lk = try_lock()
check("lock: held on another host, lock dir fresh -> wait", lk is None)
os.utime(LK, (time.time() - 121,) * 2)
lk = try_lock()
check("lock: held on another host, untouched for 2 minutes -> still wait (cannot see whether it is alive)", lk is None)
os.utime(LK, (time.time() - taskq.LOCK_REMOTE - 1,) * 2)
lk = try_lock()
check("lock: held on another host, untouched for LOCK_REMOTE -> take", lk is not None)
if lk:
    lk.__exit__()
os.mkdir(LK)
os.utime(LK, (time.time() - 121,) * 2)
lk = try_lock()
check("lock: an empty lock dir left by an old version is taken as usual after 2 minutes", lk is not None)
if lk:
    lk.__exit__()
lk = taskq.Lock().__enter__()
shutil.rmtree(LK, ignore_errors=True)
os.mkdir(LK)
lk.__exit__()
check("lock: taken over by an old-version process (empty dir), the original holder does not remove it on exit", os.path.isdir(LK))
shutil.rmtree(LK, ignore_errors=True)
lk = taskq.Lock().__enter__()
shutil.rmtree(LK, ignore_errors=True)
os.mkdir(LK)
open(os.path.join(LK, "owner"), "w").write("other\t1\tzz\n")
lk.__exit__()
check("lock: taken over by someone else, the original holder does not remove it on exit", os.path.exists(os.path.join(LK, "owner")))
shutil.rmtree(LK, ignore_errors=True)
taskq.TQ = tq_orig

kp, kc, kr = "r/prereq", "r/child", "r/rel"
rows_r = [cmdtask(1, kp), cmdtask(2, kc), cmdtask(3, kr)]
with open(os.path.join(TQR, "tasks.jsonl"), "w") as f:
    f.writelines(json.dumps(r) + "\n" for r in rows_r)
rc, _ = tqr("take", "--worker", "A", "--assign", kc)
rc2, out2 = tqr("take", "--worker", "B", "--assign", kc)
ci = json.load(open(os.path.join(TQR, "claims", taskq.safe(kc), "info")))
check("take --assign: already assigned to A, B cannot take it (exit code 10), still A", rc == 0 and rc2 == 10 and ci["assigned"] == "A")
rc, _ = tqr("take", "--worker", "A", "--assign", kc)
check("take --assign: A assigning its own again succeeds", rc == 0 and json.load(open(os.path.join(TQR, "claims", taskq.safe(kc), "info")))["assigned"] == "A")

rc, out = tqr("next", "--worker", "W", "--envs", ALL, "--exclude", kp)
tok1 = out.splitlines()[-1] if rc == 0 else None
pr = os.path.join(TQR, "claims", taskq.safe(kr), "info")
ci = json.load(open(pr))
ci["beat"] = time.time() - 1800
json.dump(ci, open(pr, "w"))
rc2, out2 = tqr("next", "--worker", "W", "--envs", ALL, "--exclude", kp)
tok2 = out2.splitlines()[-1] if rc2 == 0 else None
check("old token: a same-name worker re-claims an expired task and the token changes", rc == 0 and rc2 == 0 and out.startswith(kr) and tok1 != tok2)
for _ in range(3):
    tqr("release", "--worker", "W", "--token", tok1, kr, "--failed", "old round failed")
    tqr("release", "--worker", "W", "--token", tok1, kr, "--skip", "old round did not fit")
check("old token: release --failed / --skip record no failure or skip, the claim stays", os.path.exists(pr) and json.load(open(pr))["token"] == tok2
      and not os.path.exists(os.path.join(TQR, "failures", taskq.safe(kr))) and not os.path.exists(os.path.join(TQR, "skips", taskq.safe(kr)))
      and not os.path.exists(os.path.join(TQR, "released", taskq.safe(kr))) and strl()[kr][0] == "claimed")
rc, _ = tqr("release", "--worker", "W", "--token", tok2, kr, "--failed", "this round failed")
check("this round's token: release --failed records once and releases", rc == 0 and not os.path.exists(pr)
      and len(open(os.path.join(TQR, "failures", taskq.safe(kr))).readlines()) == 1)

# a missing-input marker but someone holds it (re-claimed after the marker) -> claimed, not input (input tasks can be removed and restaged)
tq_orig, taskq.TQ = taskq.TQ, TQR
try:
    taskq.write_tasks([dict(cmdtask(1, "r/inp"))])
    t_i = taskq.read_tasks()[0]
    os.makedirs(os.path.join(TQR, "inputs"), exist_ok=True)
    open(os.path.join(TQR, "inputs", taskq.safe("r/inp")), "w").write(f"{time.time()}\tW\tx\n")
    os.makedirs(os.path.join(TQR, "claims", taskq.safe("r/inp")), exist_ok=True)
    taskq.write_json(os.path.join(TQR, "claims", taskq.safe("r/inp"), "info"), dict(worker="W2", host="h", since=time.time(), beat=time.time()), indent=None)
    st_i = taskq.state(t_i, taskq.S.facts([dict(spec=taskq.spec_of(t_i), since=t_i.get("added"), tags=["W2"])], DATA)["r/inp"])
    check("missing-input marker + someone holds it -> claimed", st_i[0] == "claimed")
finally:
    taskq.TQ = tq_orig

# ---------------- executor queues store specs resolved by the hub (add --spec-stdin, next gives --spec) ----------------
TQS = os.path.join(T, "taskq_spec")
ENVS_ = dict(ENV, TASKQ_DIR=TQS)
os.makedirs(TQS)
open(os.path.join(TQS, "tasks.jsonl"), "w").close()


def tqs(*args, stdin=None, env=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=env or ENVS_, capture_output=True, text=True, input=stdin)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


sp_a = S.sendable(S.resolve("command", S.parse_kv(["key=sp/a", "cmd=python3 -c 1", "env=stats", "out=sp/a", 'done=["DONE|ALT"]'])))
sp_b = S.sendable(S.resolve("toy", dict(ds="D101_alpha", frag="2")))
rc, out, _ = tqs("add", "--nice", "3", "--spec-stdin", stdin=json.dumps([sp_a, sp_b]))
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("add --spec-stdin: records the spec, keeps recipe and params too (workers still on an old bundle can claim it)",
      rc == 0 and set(ts) == {"sp/a", sp_b["key"]} and ts["sp/a"]["spec"]["cmd"] == "python3 -c 1"
      and ts["sp/a"]["recipe"] == "command" and ts["sp/a"]["params"]["key"] == "sp/a")
check("sendable: drops fields that only mean something on the hub", not set(S.SENT_DROP) & set(sp_a))
nop = dict(ENVS_, TASKQ_PLUGINS=os.path.join(T, "no_plugins"))  # no recipe or experiment table plugins there
os.makedirs(nop["TASKQ_PLUGINS"], exist_ok=True)
rc, out, err = tqs("next", "--worker", "s1", "--envs", "stats", env=nop)
lines = out.splitlines()
check("next: a task with a spec gives run_task.sh --spec <JSON>, without looking up the recipe", rc == 0 and lines[0] == "sp/a" and lines[1].startswith("--spec ")
      and json.loads(__import__("shlex").split(lines[1])[1])["key"] == "sp/a")
rc, sh, err = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "shell", "--spec", json.dumps(sp_a)], env=nop,
                             capture_output=True, text=True).returncode, None, None
r1 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "shell", "--spec", json.dumps(sp_b)], env=nop, capture_output=True, text=True)
r2 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "shell", *S.kv_args("toy", dict(ds="D101_alpha", frag="2"))], env=ENVS_,
                    capture_output=True, text=True)
check("spec.py shell --spec and <recipe> k=v give the same shell line (works without plugins)", rc == 0 and r1.returncode == 0 and r1.stdout == r2.stdout)
os.makedirs(os.path.join(DATA, "sp/a"), exist_ok=True)
r3 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "done", "--spec", json.dumps(sp_a)], env=nop)
open(os.path.join(DATA, "sp/a", "ALT"), "w").close()
r4 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "done", "--spec", json.dumps(sp_a)], env=nop)
check("spec.py done --spec: done follows the spec (a|b, one is enough)", r3.returncode == 1 and r4.returncode == 0)
r5 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "shell", "--spec", '{"key": "x"}'], env=nop, capture_output=True, text=True)
check("spec.py shell --spec: missing fields exit 2", r5.returncode == 2 and "missing fields" in r5.stderr)
tqs("set-nice", "--nice", "9", "sp/a")
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("the spec is still there after set-nice", ts["sp/a"]["nice"] == 9 and ts["sp/a"]["spec"] == sp_a)
sp_a2 = dict(sp_a, cmd="python3 -c 2")
rc, out, _ = tqs("add", "--nice", "3", "--spec-stdin", stdin=json.dumps(sp_a2))
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("add --spec-stdin for an existing task: replaces the spec, nice and order unchanged", rc == 0 and "spec replaced" in out and ts["sp/a"]["spec"]["cmd"] == "python3 -c 2"
      and ts["sp/a"]["nice"] == 9)
open(os.path.join(TQS, "tasks.jsonl"), "a").write(json.dumps(dict(order=99, nice=1, key="old/c", exp=None, recipe="command",
                                                                    params=dict(key="old/c", cmd="true", env="stats", out="old/c", done="D"))) + "\n")
rc, out, _ = tqs("add", "--nice", "3", "--spec-stdin", stdin=json.dumps(S.sendable(S.resolve("command", dict(key="old/c", cmd="true", env="stats", out="old/c", done="D")))))
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("add --spec-stdin: an existing old record (no spec) gets a spec", rc == 0 and ts["old/c"].get("spec", {}).get("key") == "old/c" and ts["old/c"]["nice"] == 1)
rc, out, _ = tqs("add", "--nice", "3", "--spec-stdin", "--update-only",
                stdin=json.dumps([dict(sp_a, cmd="python3 -c 3"), S.sendable(S.resolve("command", dict(key="gone/x", cmd="true", env="stats", out="gone/x", done="D")))]))
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("add --update-only: only replace existing specs, do not add ones not in the queue", rc == 0 and "gone/x" not in ts and ts["sp/a"]["spec"]["cmd"] == "python3 -c 3")
rc, _, err = tqs("set", sp_b["key"], "env=alt2")
ts = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQS, "tasks.jsonl"))}
check("set: does not change a spec sent by the hub (change the hub queue, then resend)", rc == 2 and "resend" in err and ts[sp_b["key"]]["spec"] == sp_b
      and "env" not in ts[sp_b["key"]]["params"])
rc, out, _ = tqs("show", "sp/a")
check("show: a task with a spec shows run_task.sh --spec", rc == 0 and "run_task.sh --spec" in out)

# ---------------- remove checks claims and deletes records too; bad spec; --update-only; late release ----------------
TQX = os.path.join(T, "taskq_x")
os.makedirs(TQX)
open(os.path.join(TQX, "tasks.jsonl"), "w").close()


def tqx(*args, stdin=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQX, TASKQ_DATA=DATA),
                       capture_output=True, text=True, input=stdin)
    return r.returncode, (r.stdout + r.stderr).strip()


def sx():
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json", "--internal"], env=dict(ENV, TASKQ_DIR=TQX, TASKQ_DATA=DATA),
                       capture_output=True, text=True)
    return {t["key"]: t for t in taskq.S.json_in(r.stdout)["tasks"]}


def cspec(key, **kw):
    return S.sendable(S.resolve("command", dict(key=key, cmd="true", env="stats", out=f"x/{key}", done="DONE", **kw)))


def xp(sub, key):
    return os.path.join(TQX, sub, taskq.safe(key))


xa, xb = cspec("x/a"), cspec("x/b")
tqx("add", "--nice", "0", "--spec-stdin", stdin=json.dumps([xa, xb]))
rc, out = tqx("next", "--worker", "w1", "--envs", "stats")
tok = out.splitlines()[-1]
tqx("stop", "x/a")
rc2, out2 = tqx("remove", "x/a")
check("remove: stopped but a worker still holds it -> not removed (exit code 3), claim stays", rc == 0 and out.startswith("x/a\n") and rc2 == taskq.REFUSED
      and os.path.isdir(xp("claims", "x/a")) and sx()["x/a"]["state"] == "stopped")
os.makedirs(os.path.join(DATA, "x/x/a"), exist_ok=True)
open(os.path.join(DATA, "x/x/a", "DONE"), "w").close()
rc, out = tqx("remove", "x/a")
check("remove: done files complete but a worker still holds it (still writing results) -> not removed", rc == taskq.REFUSED and sx()["x/a"]["state"] == "done")
shutil.rmtree(xp("claims", "x/a"))  # claim expired, claimed elsewhere and released
rc, out = tqx("release", "--worker", "w1", "--token", tok, "x/a", "--failed", "late failure")
check("late release --failed (the claim is gone): records no failure, says so", rc == 0 and not os.path.exists(xp("failures", "x/a")) and "not recording failure" in out)

now_s = time.strftime("%F %T")
for sub, line in (("failures", f"{now_s}\tjob 1\tx\t\n"), ("inputs", f"{time.time()}\tW\tx\n"), ("released", f"{time.time()} W\n"),
                  ("skips", f"{now_s}\tw\tdoes not fit\n")):
    os.makedirs(os.path.join(TQX, sub), exist_ok=True)
    open(xp(sub, "x/b"), "w").write(line)
tqx("stop", "x/b")
os.makedirs(xp("claims", "x/b"))
taskq.write_json(os.path.join(xp("claims", "x/b"), "info"), dict(worker="old", host="h", since=time.time() - 7200, beat=time.time() - 7200), indent=None)
rc, out = tqx("remove", "x/b")
left = [sub for sub in ("failures", "stopped", "inputs", "released", "skips", "claims") if os.path.exists(xp(sub, "x/b"))]
tqx("add", "--nice", "0", "--spec-stdin", stdin=json.dumps(xb))
check("remove: deletes failures, stop, missing-input, release, skip records and the stale claim; adding it back is a new task (free)", rc == 0 and not left
      and sx()["x/b"]["state"] == "free" and sx()["x/b"]["failures"] == 0)
tqx("add", "--nice", "0", "--recipe", "command", "key=x/c", "cmd=true", "env=stats", "out=x/x/c", "done=DONE")
for sub in ("failures", "skips"):
    open(xp(sub, "x/c"), "w").write(f"{now_s}\tw\tx\n")
rc, out = tqx("remove", "x/c")
check("remove: a hub queue record (no spec) deletes the failure record, skips stay", rc == 0 and not os.path.exists(xp("failures", "x/c"))
      and os.path.exists(xp("skips", "x/c")))

rc, out = tqx("add", "--nice", "0", "--spec-stdin", stdin=json.dumps(dict(cspec("x/d"), done=5)))
rc2, out2 = tqx("add", "--nice", "0", "--spec-stdin", stdin=json.dumps(dict(cspec("x/e"), res="abc")))
rc3, out3 = tqx("add", "--nice", "0", "--spec-stdin", stdin="{oops")
rc4, out4 = tqx("add", "--nice", "0", "--spec-stdin", stdin=json.dumps(dict(cspec("x/e"), vars=["A=1"])))
check("add --spec-stdin: wrong done, res, vars types or not JSON -> cannot add (exit code 2)", (rc, rc2, rc3, rc4) == (2, 2, 2, 2)
      and "done" in out and "res" in out2 and "JSON" in out3 and "vars" in out4 and not {"x/d", "x/e"} & set(sx()))
with open(os.path.join(TQX, "tasks.jsonl"), "a") as f:
    f.write(json.dumps(dict(order=99, nice=-5, key="x/f", exp=None, recipe="command", params={}, spec=dict(cspec("x/f"), done=5))) + "\n")
rc, out = tqx("status")
rc2, out2 = tqx("next", "--worker", "w2", "--envs", "stats")
check("one bad spec in the queue: status works and shows bad; next skips it and claims another", rc == 0 and "x/f" in out and "bad" in out
      and rc2 == 0 and out2.startswith("x/b\n"))

rc, out = tqx("add", "--nice", "0", "--spec-stdin", "--update-only",
              stdin=json.dumps([dict(xb, out="x/new"), dict(cspec("x/h"), done=5), cspec("x/zz"), dict(xa, cmd="python3 -c 9")]))
tx = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQX, "tasks.jsonl"))}
check("--update-only: reports bad ones one by one, replaces the rest; prints ones not in the queue; a running task with changed out is not replaced", rc == 2 and "cannot replace: x/h" in out
      and "not in queue, not replaced: x/zz" in out and "spec replaced: x/a" in out and "running" in out and tx["x/a"]["spec"]["cmd"] == "python3 -c 9"
      and tx["x/b"]["spec"]["out"] == xb["out"] and "x/h" not in tx and "x/zz" not in tx)
rc, out = tqx("add", "--nice", "0", "--spec-stdin", "--update-only", stdin=json.dumps([dict(xb, cmd="python3 -c 8")]))
tx = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQX, "tasks.jsonl"))}
check("--update-only: a running task with only a command change (out, done unchanged) is replaced", rc == 0 and tx["x/b"]["spec"]["cmd"] == "python3 -c 8")

# ---------------- next --log-tag (pueue-host's part log name is not the worker name), status --json, proto.claim_alive ----------------
import proto  # noqa: E402
TQL = os.path.join(T, "taskq_l")
os.makedirs(TQL)
open(os.path.join(TQL, "tasks.jsonl"), "w").close()


def tql(*args, stdin=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=dict(ENV, TASKQ_DIR=TQL, TASKQ_DATA=DATA),
                       capture_output=True, text=True, input=stdin)
    return r.returncode, (r.stdout + r.stderr).strip()


tql("add", "--nice", "0", "--spec-stdin", stdin=json.dumps([cspec("l/a"), cspec("l/b")]))
rc, out = tql("next", "--worker", "site-pueue", "--envs", "stats", "--log-tag", "gpuhost-202611041200")
rc2, out2 = tql("next", "--worker", "site-pueue", "--envs", "stats")
ia = json.load(open(os.path.join(TQL, "claims", taskq.safe("l/a"), "info")))
ib = json.load(open(os.path.join(TQL, "claims", taskq.safe("l/b"), "info")))
check("next --log-tag: the claim record has an extra log field; without it the field is absent",
      rc == 0 and rc2 == 0 and ia.get("log") == "gpuhost-202611041200" and ia["worker"] == "site-pueue" and "log" not in ib
      and set(ia) <= set(proto.CLAIM_FIELDS) and set(ib) <= set(proto.CLAIM_FIELDS))
lg = os.path.join(DATA, proto.TASK_LOGS)
os.makedirs(lg, exist_ok=True)
open(os.path.join(lg, f"{taskq.safe('l/a')}__gpuhost-202611041200_part1.log"), "w").write("x\n")
open(os.path.join(lg, f"{taskq.safe('l/b')}__site-pueue_part1.log"), "w").write("x\n")
old_t = time.time() - 4000
os.utime(os.path.join(lg, f"{taskq.safe('l/b')}__site-pueue_part1.log"), (old_t, old_t))
check("seg_age: finds the part log by the job name in the claim record (log or worker); no such name -> None",
      taskq.S.seg_age(DATA, "l/a", "gpuhost-202611041200") < 60
      and taskq.S.seg_age(DATA, "l/a", "site-pueue") is None
      and 3900 < taskq.S.seg_age(DATA, "l/b", "site-pueue") < 4100
      and taskq.S.seg_age(DATA, "l/b", None) is None)
rc, out = tql("status", "--json")
lines = out.splitlines()
js = taskq.S.json_in(out)
rows = {t["key"]: t for t in js["tasks"]}
rc_old, _ = tql("--json-status")
check("status --json: one @@JSON line with version and local_status's top-level fields; task_log_age found by the log field; no --json-status",
      rc == 0 and len(lines) == 1 and lines[0].startswith(proto.JSON_MARK) and js["version"] == proto.VERSION
      and set(proto.STATUS_TOP) <= set(js) and rows["l/a"]["task_log_age"] < 60 and rows["l/b"]["task_log_age"] > 3900
      and rc_old == 2)


def ref_alive(ci, made, now, age, stale, fresh, half):  # reference: whether a claim is alive, spelled out case by case
    if ci is None:
        return not now - made > half
    if ci.get("assigned"):
        return True
    return now - ci.get("beat", 0) < stale or (age is not None and age < fresh)


now = time.time()
grid = [(ci, made, age) for ci in (None, dict(assigned="site-pueue"), dict(beat=now - 10), dict(beat=now - 1199), dict(beat=now - 1201),
                                   dict(beat=now - 9000), dict(worker="w"))
        for made in (now - 10, now - 299, now - 301) for age in (None, 10, 899, 901)]
check("proto.claim_alive matches the reference (default and changed thresholds)",
      all(proto.claim_alive(ci, made, now, age) == ref_alive(ci, made, now, age, 1200, 900, 300) for ci, made, age in grid)
      and all(proto.claim_alive(ci, made, now, age, stale=100, fresh=50, half=20) == ref_alive(ci, made, now, age, 100, 50, 20)
              for ci, made, age in grid))

# next claimed but was stopped before printing: drop the claim instead of leaving it to expire; still exits from the signal
TQK = os.path.join(T, "taskq_kill")
write_jsonl(os.path.join(TQK, "tasks.jsonl"), [(1, 0, "D103_y/V_save/0")])
KILL_NEXT = ("import os,signal,sys\nsys.path.insert(0, sys.argv.pop(1))\nimport taskq\n_w = taskq.write_json\n"
             "def w(p, *a, **k):\n    _w(p, *a, **k)\n    os.kill(os.getpid(), signal.SIGTERM)\n"
             "taskq.write_json = w\nsys.exit(taskq.main(sys.argv[1:]))")
r = subprocess.run([sys.executable, "-c", KILL_NEXT, HERE, "next", "--worker", "600", "--envs", ALL], env=dict(ENV, TASKQ_DIR=TQK),
                   capture_output=True, text=True)
left = os.listdir(os.path.join(TQK, "claims")) if os.path.isdir(os.path.join(TQK, "claims")) else []
r2 = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "next", "--worker", "601", "--envs", ALL], env=dict(ENV, TASKQ_DIR=TQK),
                    capture_output=True, text=True)
check("next claimed and got SIGTERM before printing: drops the claim (another worker can claim at once), exit code still from the signal",
      r.returncode == -15 and not r.stdout and not left and r2.returncode == 0 and r2.stdout.startswith("D103_y/V_save/0"))

# ---------------- within the same nice, a task with a checkpoint (half-done) is claimed first; nice still wins ----------------
TQP = os.path.join(T, "taskq_progress")
os.makedirs(TQP)
open(os.path.join(TQP, "tasks.jsonl"), "w").close()
EP = dict(ENV, TASKQ_DIR=TQP, TASKQ_DATA=DATA)


def tqp(*args, stdin=None):
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), *args], env=EP, capture_output=True, text=True, input=stdin)
    return r.returncode, r.stdout.strip()


def pspec(key):
    return dict(S.sendable(S.resolve("command", dict(key=key, cmd="true", env="stats", out=f"p/{key}", done="DONE"))), resume=["ckpt_*.pt"])


tqp("add", "--nice", "70", "--spec-stdin", stdin=json.dumps([pspec("p/new1"), pspec("p/half"), pspec("p/new2")]))
os.makedirs(os.path.join(DATA, "p/p/half"), exist_ok=True)
open(os.path.join(DATA, "p/p/half", "ckpt_latest.pt"), "w").close()
rc, out = tqp("next", "--worker", "p1", "--envs", "stats")
rc2, out2 = tqp("next", "--worker", "p2", "--envs", "stats")
check("next: within the same nice, a checkpointed task is claimed first (ahead of earlier unstarted ones), the rest by add order",
      rc == 0 and out.startswith("p/half\n") and rc2 == 0 and out2.startswith("p/new1\n"))
tqp("add", "--nice", "-100", "--spec-stdin", stdin=json.dumps(pspec("p/urgent")))
os.makedirs(os.path.join(DATA, "p/p/new2"), exist_ok=True)
open(os.path.join(DATA, "p/p/new2", "ckpt_latest.pt"), "w").close()
rc, out = tqp("next", "--worker", "p3", "--envs", "stats")
rc2, out2 = tqp("next", "--worker", "p4", "--envs", "stats")
check("next: an earlier nice (a new task without a checkpoint) is still claimed first; a checkpoint only moves a task up within its nice",
      rc == 0 and out.startswith("p/urgent\n") and rc2 == 0 and out2.startswith("p/new2\n"))

# steps without resume (inference and the like): one that ran here (has a part log) and was released is claimed first within its nice; ones that failed are not moved up; a/b's log does not count for a/b/c
def sspec(key):
    return S.sendable(S.resolve("command", dict(key=key, cmd="true", env="stats", out=f"s/{key}", done="DONE")))


tqp("add", "--nice", "80", "--spec-stdin", stdin=json.dumps([sspec("s/new"), sspec("s/half"), sspec("s/bad"), sspec("s/half/sub")]))
LOGS = os.path.join(DATA, "logs/tasks")
os.makedirs(LOGS, exist_ok=True)
for k in ("s/half", "s/bad"):
    open(os.path.join(LOGS, k.replace("/", "__") + "__host-1107_part1.log"), "w").close()
os.makedirs(os.path.join(TQP, "failures"), exist_ok=True)
with open(os.path.join(TQP, "failures", "s__bad"), "w") as f:
    f.write("2026-11-07 10:00:00\tjob p0\texit code 1\t\n")
got = [tqp("next", "--worker", f"s{i}", "--envs", "stats")[1].split("\n")[0] for i in range(4)]
check("next: steps without resume that ran and were released are claimed first within their nice; failed ones and ones with only a sub-key log follow add order",
      got == ["s/half", "s/new", "s/bad", "s/half/sub"])

# a redone task: logs from before it was added do not count as half-done; ones written after it was added do. Release records the reason; a missing-input release is not moved up
def slog(k, age=0):
    f = os.path.join(LOGS, k.replace("/", "__") + "__host-1107_part1.log")
    open(f, "a").close()
    os.utime(f, (time.time() - age, time.time() - age))


tqp("add", "--nice", "70", "--spec-stdin", stdin=json.dumps([sspec("r/old"), sspec("r/new")]))
rec = {t["key"]: t for t in taskq.read_tasks(os.path.join(TQP, "tasks.jsonl"))}
slog("r/old", age=3600)
slog("r/new")
check("add: a new task records when it was added (added)", all(abs(rec[k].get("added", 0) - time.time()) < 60 for k in ("r/old", "r/new")))
got = [tqp("next", "--worker", f"r{i}", "--envs", "stats")[1].split("\n")[0] for i in range(2)]
check("next: a redone task (log older than added) is not moved up; one run after it was added is", got == ["r/new", "r/old"])
who = {k: f"r{i}" for i, k in enumerate(got)}
tqp("release", "--worker", who.get("r/new", "r0"), "r/new")
tqp("release", "--worker", who.get("r/old", "r1"), "--failed", "exit code 1", "r/old")


def rel(k):
    try:
        return open(os.path.join(TQP, "released", taskq.safe(k))).read().split()
    except OSError:
        return []


check("release: released/<key> records the reason (normal release back, failure failed)",
      rel("r/new")[2:] == ["back"] and rel("r/old")[2:] == ["failed"])
tqp("add", "--nice", "60", "--spec-stdin", stdin=json.dumps([sspec("m/plain"), sspec("m/miss")]))
rc, out = tqp("next", "--worker", "m0", "--envs", "stats")
rc2, out2 = tqp("next", "--worker", "m1", "--envs", "stats")
slog("m/miss")
tqp("missing", "--worker", "m1", "--why", "missing x", "m/miss")
tqp("release", "--worker", "m0", "m/plain")
tqp("input-ok", "m/miss")
got = [tqp("next", "--worker", f"m{i}", "--envs", "stats")[1].split("\n")[0] for i in (2, 3)]
check("missing: records reason missing; a missing-input release (with a log) is not half-done, so add order holds",
      out.startswith("m/plain\n") and out2.startswith("m/miss\n") and rel("m/miss")[2:] == ["missing"] and got == ["m/plain", "m/miss"])
tq_orig, taskq.TQ = taskq.TQ, TQP
ri = taskq.released_info("r/new")
check("released_info: (time, worker, reason); released_at is the time in it",
      ri is not None and ri[1] == who.get("r/new", "r0") and ri[2] == "back" and taskq.released_at("r/new") == ri[0]
      and taskq.released_info("nosuch/key") is None)
taskq.TQ = tq_orig

print("\nall passed" if ok else "\nfailed")
sys.exit(0 if ok else 1)
