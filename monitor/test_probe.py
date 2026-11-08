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
assert not health.probe_ok(0, "") and not health.probe_ok(255, r.stdout) and not health.probe_ok(0, "DISK|1\n")
print("PASS:truncated output (no BOOT line) or ssh FAIL: counts as unreachable")
print("All passed")
