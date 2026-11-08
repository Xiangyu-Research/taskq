#!/usr/bin/env python3
"""Test GPU usage records: program names written by gpu_log.sh (fake nvidia-smi, ps) and gpu_usage.py summaries (text and --json).

    python3 monitor/test_gpu_usage.py
"""
import json
import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(CORE, "queue", "testdata"))
import fixture  # noqa: E402
sys.path.insert(0, os.path.join(CORE, "queue"))
import proto  # noqa: E402

T = fixture.tmpdir()
TQ = os.path.join(CORE, "bin", "taskq")
fails = 0


def ok(what, cond, got=None):
    global fails
    print(("PASS:" if cond else "FAIL:") + what + ("" if cond or got is None else f" (got {got!r})"))
    fails += 0 if cond else 1


# ---------- gpu_log.sh: program names ----------
ARGS = {
    101: "/opt/envs/a/bin/python -c from pkg.run import entry; entry() 1 2",
    102: "/opt/envs/a/bin/python /opt/envs/a/bin/pkg_train 1 3d 0",
    103: "python3 -u train.py --lr 1e-3",
    104: "python3 -m torch.distributed.run --nproc 2 train.py",
    105: "python3.11 -W ignore -X faulthandler /x/infer.py",
    106: "bash -euo pipefail /x/step.sh a b",
    107: "bash -c sleep 100",
    108: "/usr/lib/xorg/Xorg vt1 -displayfd 3",
    109: "/bin/sh -ec echo hi",
}
WANT = {101: "python -c", 102: "pkg_train", 103: "train.py", 104: "torch.distributed.run", 105: "infer.py", 106: "step.sh",
        107: "bash -c", 108: "Xorg", 109: "sh -c"}
BIN = os.path.join(T, "bin")
os.makedirs(os.path.join(T, "args"))
os.makedirs(BIN)
for pid, a in ARGS.items():
    with open(os.path.join(T, "args", str(pid)), "w") as f:
        f.write(a + "\n")
with open(os.path.join(BIN, "nvidia-smi"), "w") as f:
    f.write("#!/bin/bash\ncase \"$*\" in\n"
            "  *--query-gpu=*) echo '0, GPU-aaa, 50, 1000, 81920'; echo '1, GPU-bbb, 0, 10, 81920' ;;\n"
            "  *--query-compute-apps=*) for p in " + " ".join(map(str, ARGS)) + "; do echo \"GPU-aaa, $p, 500\"; done ;;\n"
            "esac\n")
with open(os.path.join(BIN, "ps"), "w") as f:
    f.write(f"#!/bin/bash\n# ps -o user=|args= -p PID\ncase $2 in\n  user=) echo alice ;;\n  args=) cat {T}/args/$4 ;;\nesac\n")
for x in ("nvidia-smi", "ps"):
    os.chmod(os.path.join(BIN, x), 0o755)
GL = os.path.join(T, "gl")
env = dict(os.environ, PATH=BIN + os.pathsep + os.environ["PATH"], GPU_LOG_DIR=GL, GPU_LOG_INTERVAL="0.2")
p = subprocess.Popen(["bash", os.path.join(HERE, "gpu_log.sh")], env=env, start_new_session=True,
                     stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
f = os.path.join(GL, time.strftime(proto.GPU_LOG_FILE))
rows = []
try:
    t0 = time.time()
    while time.time() - t0 < 20:
        try:
            rows = [ln.rstrip("\n").split(",") for ln in open(f)]
        except OSError:
            rows = []
        if len(rows) >= 1 + len(ARGS) + 1:
            break
        time.sleep(0.1)
finally:
    os.killpg(p.pid, signal.SIGTERM)
    p.wait(timeout=10)
head = rows[0] if rows else []
ok("header unchanged (readers use column positions for time, gpu, user)", head == ["time", "gpu", "util_pct", "mem_used_mib", "mem_total_mib", "user", "pid",
                                              "proc_mem_mib", "program"], head)
got = {int(r[6]): r[8] for r in rows[1:1 + len(ARGS) + 1] if len(r) == 9 and r[6]}
ok("program name (first non-option argument after an interpreter, -m X -> X, -c -> <interpreter> -c, otherwise the executable)", got == WANT, got)
ok("every row has 9 columns and the account is column 6", all(len(r) == 9 for r in rows[1:]) and all(r[5] == "alice" for r in rows[1:] if r[6]), rows[1:3])
ok("an idle card still gets a row (empty account)", any(r[1] == "1" and r[5] == "" for r in rows[1:]), rows[-2:])

# ---------- gpu_usage.py ----------
D = os.path.join(T, "usage")
os.makedirs(D)
lines = ["time,gpu,util_pct,mem_used_mib,mem_total_mib,user,pid,proc_mem_mib,program"]
# bob uses GPU 0 on 11/3 (Mon) 10:00-10:20 with a 5 minute gap, still one span; another span at 11:00 (gap > 10 min); me on GPU 1; idle on 11/8 (Sat)
for m in list(range(0, 10)) + list(range(15, 21)):
    lines.append(f"2026-11-03 10:{m:02d}:00,0,90,20000,81920,bob,11,{18000 + m},a.py")
    lines.append(f"2026-11-03 10:{m:02d}:00,1,50,9000,81920,me,12,9000,train.py")
for m in range(0, 31):
    lines.append(f"2026-11-03 11:{m:02d}:00,0,90,20000,81920,bob,13,4000,b.py")
    lines.append(f"2026-11-03 11:{m:02d}:00,0,90,20000,81920,me,14,4000,c.py")
for m in range(0, 10):
    lines.append(f"2026-11-08 03:{m:02d}:00,0,0,0,81920,,,,")
with open(os.path.join(D, "gpu_2026-11.csv"), "w") as fh:
    fh.write("\n".join(lines) + "\n")
p = subprocess.run([sys.executable, TQ, "gpu", "usage", D, "--me", "me", "--days", "0", "--json"], capture_output=True, text=True)
j = json.loads(p.stdout) if p.returncode == 0 else {}
ok("--json top-level fields", tuple(j) == proto.GPU_USAGE_TOP, p.stderr[-300:] or list(j))
seg = [(s["gpu"], s["user"], s["start"], s["end"], s["max_mem_mib"], s["programs"]) for s in j.get("spans", [])]
ok("each other user's spans (a gap of 10 minutes or less stays one span)", seg == [("0", "bob", "2026-11-03 10:00:00", "2026-11-03 10:20:00", 18020, ["a.py"]),
                                      ("0", "bob", "2026-11-03 11:00:00", "2026-11-03 11:30:00", 4000, ["b.py"])], seg)
ok("totals", j.get("totals") == [dict(gpu="0", user="bob", hours=round(50 / 60, 3))], j.get("totals"))
wd = {(h["gpu"], h["hour"]): (h["n"], h["other"], h["me"], h["idle"]) for h in j.get("hours", {}).get("weekday", [])}
ok("weekday by hour: at one moment others > me > idle", wd == {("0", 10): (16, 16, 0, 0), ("1", 10): (16, 0, 16, 0), ("0", 11): (31, 31, 0, 0)}, wd)
we = j.get("hours", {}).get("weekend", [])
ok("weekend", [(h["gpu"], h["hour"], h["idle"]) for h in we] == [("0", 3, 10)], we)
ok("from, to", (j.get("from"), j.get("to")) == ("2026-11-03 10:00:00", "2026-11-08 03:09:00"), (j.get("from"), j.get("to")))
p = subprocess.run([sys.executable, TQ, "gpu", "usage", D, "--me", "me", "--days", "0"], capture_output=True, text=True)
ok("three text sections", p.returncode == 0 and all(s in p.stdout for s in ("## 1.", "### Weekday", "### Weekend", "## 2.", "## 3.", "GPU0 bob: 0.8",
                                                                   "0 | bob | 11-03 Tue 10:00 | 11-03 10:20 | 0.3 h | 17.6 GB | a.py",
                                                                   "10 | 100 /   0 /   0 |   0 / 100 /   0")), p.stdout + p.stderr)
p = subprocess.run([sys.executable, TQ, "gpu", "usage", D, "--me", "me", "--days", "1", "--json"], capture_output=True, text=True)
ok("--days: only the last day", json.loads(p.stdout)["spans"] == [], p.stdout[:200])
p = subprocess.run([sys.executable, TQ, "gpu", "usage", "--json"], capture_output=True, text=True,
                   env=dict(os.environ, GPU_LOG_DIR=os.path.join(T, "empty")))
ok("default dir is GPU_LOG_DIR; no records", p.returncode == 0 and json.loads(p.stdout)["dir"] == os.path.join(T, "empty")
   and json.loads(p.stdout)["spans"] == [], p.stdout + p.stderr)

print(f"\n{'all passed' if not fails else f'{fails} failed'}")
sys.exit(1 if fails else 0)
