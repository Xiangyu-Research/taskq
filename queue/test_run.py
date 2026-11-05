#!/usr/bin/env python3
"""flow/run.py（taskq run）和 flow/stall.py 的测试（2026-11-05）：testdata 的实验表、临时的数据目录和台账，
  1. 包一层跑：退出码照旧，台账记开始和结束、这次写了的文件，写到声明以外的记成事件；[[ 在 bash -euo pipefail 下照常；
  2. 不在流程里的实例照常跑、不记台账；
  3. 卡住：sleep（CPU、产出都不长）在几秒内报出来（T24vol 那种）；包装进程没了的心跳文件删掉。
    python3 queue/test_run.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
sys.path.insert(0, HERE)
T = tempfile.mkdtemp(prefix="test_run_")
os.environ.update(ADVANCE_BT=T, TASKQ_STATE=os.path.join(T, "state"), TASKQ_LEDGER=os.path.join(T, "ledger.db"))
import proto as PR  # noqa: E402
from flow import cli as FC  # noqa: E402
from flow import ledger as LG  # noqa: E402
from flow import run as R  # noqa: E402
from flow import stall  # noqa: E402

fails = []


def ok(cond, what):
    if not cond:
        fails.append(what)


try:
    flow, g, root, _ = FC.flow_for_ledger()
    ok(root == T, f"数据目录 {root}")
    i = next(x for x in flow.instances.values() if x.task and x.outputs and x.step and x.step.get("done") is not None)
    out = os.path.join(T, i.outputs[0])
    par = os.path.dirname(out)
    cmd = (f"[[ 1 == 1 ]] && mkdir -p {out} && echo x > {out}/a.txt && echo y > {par}/stray_{os.getpid()}.txt")
    rc = R.main([i.id, "--attempt", "1", "--", cmd])
    ok(rc == 0, f"退出码 {rc}")
    db = LG.connect()
    a = LG.last_attempt(db, i.id)
    ok(a and a["state"] in (PR.A_OK, PR.A_FAILED) and a["end"], f"台账的尝试 {dict(a) if a else None}")
    ev = db.execute("SELECT path, detail FROM event WHERE inst=? AND kind='undeclared'", (i.id,)).fetchall()
    ok(ev and "stray_" in ev[0][0], f"没声明的写没记下：{ev}")
    ok(not os.path.exists(R.beat_path(i.id)), "心跳文件没删")
    if a and a["state"] == PR.A_OK:
        rows = {r[0] for r in db.execute("SELECT path FROM output WHERE inst=? AND state='live'", (i.id,))}
        ok(os.path.relpath(f"{out}/a.txt", T) in rows or i.outputs[0] in rows, f"产出没记下：{rows}")

    rc = R.main([i.id, "--", "exit 3"])
    ok(rc == 3, f"失败的命令退出码 {rc}")
    ok(LG.last_attempt(db, i.id)["state"] == PR.A_FAILED, "失败没记成 failed")
    rc = R.main(["没有这个:实例", "--", "true"])
    ok(rc == 0, "不在流程里的实例没照常跑")
    rc = R.main([i.id, "--", "echo $NOT_SET_VAR_XYZ"])
    ok(rc != 0, "bash -u 没挡住没设的变量")

    # ---- 卡住 ----
    env = dict(os.environ, TASKQ_RUN_BEAT="0.3", TASKQ_STALL_IDLE="1")
    p = subprocess.Popen([sys.executable, os.path.join(HERE, "flow", "run.py"), i.id, "--", "sleep 30"], env=env,
                         stderr=subprocess.DEVNULL)
    hit, t0 = {}, time.time()
    os.environ["TASKQ_STALL_IDLE"] = "1"
    while time.time() - t0 < 15 and not hit:
        time.sleep(0.5)
        hit = stall.check()
    ok(any(k.startswith("stall:") for k in hit), f"sleep 没报卡住：{hit}")
    p.terminate()
    p.wait(10)
    os.environ.pop("TASKQ_STALL_IDLE")
    ok(not os.path.exists(R.beat_path(i.id)), "被停后心跳文件没删")

    # 包装进程没了、心跳停了的心跳文件删掉
    os.makedirs(os.path.dirname(R.beat_path("x:y")), exist_ok=True)
    dead = subprocess.Popen(["true"]); dead.wait()
    json.dump(dict(inst="x:y", pid=dead.pid, wrapper=dead.pid, beat=time.time() - 3600, cpu_changed=0, out_changed=0),
              open(R.beat_path("x:y"), "w"))
    stall.check()
    ok(not os.path.exists(R.beat_path("x:y")), "死掉的心跳文件没删")
finally:
    shutil.rmtree(T, ignore_errors=True)

if fails:
    print("失败：\n  " + "\n  ".join(fails))
    sys.exit(1)
print("taskq run、卡住检测：都对")
