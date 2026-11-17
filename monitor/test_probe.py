#!/usr/bin/env python3
"""health.probe_script / probe_ok: run the probe script with bash on this machine (no network): disk, GPU log, truncated output, ssh failure."""
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "queue", "testdata"))
import fixture  # noqa: E402
T = fixture.tmpdir(prefix="probe_")
os.environ.update(HOME=T, TASKQ_STATE=os.path.join(T, "state"))
HERE = os.path.dirname(os.path.abspath(__file__))

fixture.use()
sys.path.insert(0, HERE)
import health  # noqa: E402

health.GPU_PROBE = "true"
script = health.probe_script(dict(disk=[T, 50, 10], gpu_log=True))
r = subprocess.run(["bash", "-s"], input=script, capture_output=True, text=True, timeout=30)
assert health.probe_ok(r.returncode, r.stdout), (r.returncode, r.stdout)
lines = dict(ln.split("|", 1) for ln in r.stdout.splitlines() if "|" in ln)
assert lines["DISK"].strip().isdigit() and "GPULOG" in lines, r.stdout
print("PASS:real run: reachable, disk free and GPU log lines present")
# boot log and GPU records: in the machine's own state dir (its root's state/; ~ expanded over there), nothing in the home dir
import time  # noqa: E402
st = os.path.join(T, "mroot", "state")
os.makedirs(os.path.join(st, "logs"))
os.makedirs(os.path.join(st, "gpu_usage"))
with open(os.path.join(st, "logs", "on_boot.log"), "w") as f:
    f.write("old\n2026-01-01 00:00:00 boot: no interrupted tasks\n")
with open(os.path.join(st, "gpu_usage", time.strftime("gpu_%Y-%m.csv")), "w") as f:
    f.write("time,gpu\n2026-01-02 03:04:05,0\n")
script = health.probe_script(dict(disk=None, gpu_log=True, root="~/mroot"))
assert "~/mroot/state/" in script and "'~" not in script, script
r = subprocess.run(["bash", "-s"], input=script, capture_output=True, text=True, timeout=30)
lines = dict(ln.split("|", 1) for ln in r.stdout.splitlines() if "|" in ln)
assert lines["BOOT"].endswith("|2026-01-01 00:00:00 boot: no interrupted tasks") and lines["GPULOG"] == "2026-01-02 03:04:05", r.stdout
script = health.probe_script(dict(disk=None, gpu_log=True, root=None))
sh = health.cfg.state_home()  # the configured one, as the other machine has it (not this process's TASKQ_STATE)
assert f"{sh}logs/on_boot.log" in script and f"{sh}gpu_usage/" in script, script
print("PASS:boot log and GPU records read from the machine's own state dir (its root's state/, else the state dir)")
assert not health.probe_ok(0, "") and not health.probe_ok(255, r.stdout) and not health.probe_ok(0, "DISK|1\n")
print("PASS:truncated output (no BOOT line) or ssh FAIL: counts as unreachable")
print("All passed")
