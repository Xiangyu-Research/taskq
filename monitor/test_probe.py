#!/usr/bin/env python3
"""health.probe_script / probe_ok：在本机用 bash 真跑探测脚本（不连网络），盘、GPU 记录、输出被截断、ssh 失败。"""
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
print("PASS:实跑：连得上，盘剩多少、GPU 记录那几行都在")
assert not health.probe_ok(0, "") and not health.probe_ok(255, r.stdout) and not health.probe_ok(0, "DISK|1\n")
print("PASS:输出被截断（没有 BOOT 那行）、ssh FAIL:算没读到")
print("全部通过")
