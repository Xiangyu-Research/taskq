#!/usr/bin/env python3
"""health.probe_script / probe_ok：在本机用 bash 真跑探测脚本（不连网络），数据目录在、不在、进不去、输出被截断。"""
import os
import subprocess
import sys
import tempfile

T = tempfile.mkdtemp(prefix="probe_")
os.environ.update(HOME=T, TASKQ_STATE=os.path.join(T, "state"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import health  # noqa: E402

health.GPU_PROBE = "true"
data = os.path.join(T, "data")
os.makedirs(os.path.join(data, "a"))
os.makedirs(os.path.join(data, "b"))
open(os.path.join(data, "b", ".incomplete"), "w").close()


def probe(d):
    script = health.probe_script(dict(disk=[], gpu_log=False, roles=("train",), data=d), ["a", "b", "c"])
    r = subprocess.run(["bash", "-s"], input=script, capture_output=True, text=True, timeout=30)
    return script, r.returncode, r.stdout


s, rc, out = probe(data)
assert health.probe_ok(s, rc, out) == (True, {"a"}), (rc, out)
print("ok  数据目录在：连得上，没传完的（.incomplete）不算")
for what, d in (("不在", os.path.join(T, "gone")), ("进不去", "/root")):
    s, rc, out = probe(d)
    assert rc == 3 and health.probe_ok(s, rc, out) == (True, set()), (rc, out)
    print(f"ok  数据目录{what}：连得上，数据都不在（不当成连不上）")
s, rc, out = probe(data)
cut = out.split(health.C.HAVE_END)[0]
assert health.probe_ok(s, rc, cut)[0] is False and health.probe_ok(s, 255, "")[0] is False
assert health.probe_ok(s, 3, "BOOT|x\n")[0] is False
print("ok  输出被截断、ssh 失败、退出码 3 但没有 @@NODATA：算没读到")
print("全部通过")
