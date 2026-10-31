#!/usr/bin/env python3
"""scripts/sites/pueue.py、slurm.py 的测试：假的 ssh 输出，核对读状态和开 / 补 worker 的命令。"""
import json
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import remote as R  # noqa: E402
import pueue as PQ  # noqa: E402
import slurm as SL  # noqa: E402
import machines as M  # noqa: E402
import policy as P  # noqa: E402


def ok(msg):
    print("  ok ", msg)


class Fake:
    """按命令里的关键字回放输出，记下每条命令。"""

    def __init__(self, replies):
        self.replies, self.calls = replies, []

    def __call__(self, host, cmd, input=None, timeout=None):
        self.calls.append((host, cmd))
        for key, rep in self.replies:
            if key in cmd:
                return rep
        return 0, "", ""


def pueue_json(*tasks):
    out = {}
    for i, group, label, status in tasks:
        out[str(i)] = dict(group=group, label=label, status=status)
    return json.dumps(dict(tasks=out))


def main():
    lab1 = M.SITES["lab1"]
    w = lab1["worker"]
    wl = f"worker:{w['id']}"

    # ---- pueue ----
    R.RUN = Fake([("status --json", (0, pueue_json((3, w["group"], wl, {"Done": {"result": "Success"}}),
                                                    (5, "gpu0", "train:x", "Running"),
                                                    (7, w["group"], wl, "Running")), ""))])
    st = PQ.status("lab1")
    assert [j[0] for j in st["jobs"]] == [3, 7] and st["jobs"][0][3] == "Success" and st["jobs"][1][2] == "Running"
    ok("pueue：只读 worker 所在的组，Done 的结果读出来")
    assert PQ.ensure_workers("lab1", 1, st=st) == []
    ok("pueue：有活着的 worker 不开")

    st = dict(sched="pueue", jobs=[[3, wl, "Done", "Success"]])
    fake = Fake([("add -g", (0, "New task added (id 9).", ""))])
    R.RUN = fake
    acts = PQ.ensure_workers("lab1", 2, st=st)
    assert len(acts) == 1 and acts[0]["op"] == "add" and acts[0]["ok"]
    host, cmd = fake.calls[-1]
    assert host == "lab"
    argv = shlex.split(cmd)
    i = argv.index("--")
    assert argv[i + 1:] == argv[i + 1:i + 2], "pueue add 的 -- 后面只能有一个参数"
    inner = shlex.split(argv[i + 1])
    assert inner[:2] == ["bash", "-c"] and len(inner) == 3
    assert f"WORKER_ID={w['id']}" in inner[2] and "CUDA_VISIBLE_DEVICES=1" in inner[2] and "$REPO/scripts/queue/" in inner[2]
    assert argv[argv.index("-g") + 1] == w["group"] and argv[argv.index("-l") + 1] == wl
    ok("pueue：没有活着的就开一个；want=2 也只开一个；整条 bash -c 作为一个参数（ccb0ba6）")

    acts = PQ.ensure_workers("lab1", 1, st=dict(jobs=[[3, wl, "Done", "Failed(1)"]]))
    assert acts[0]["op"] == "blocked" and "3" in acts[0]["why"]
    ok("pueue：上一个 worker 失败了不重开")
    assert PQ.ensure_workers("lab1", 0, st=dict(jobs=[])) == []
    assert PQ.ensure_workers("lab1", 1, st=dict(jobs=None))[0]["op"] == "skip"
    ok("pueue：want=0 不开；读不到 pueue 不开")
    fake = Fake([])
    R.RUN = fake
    acts = PQ.ensure_workers("lab1", 1, dry=True, st=dict(jobs=[]))
    assert acts[0]["ok"] is None and not fake.calls
    ok("pueue：dry 只给命令不跑")

    R.RUN = Fake([("status --json", (255, "", "ssh: connect"))])
    st = PQ.status("lab1")
    assert st["jobs"] is None and st["unreachable"]
    ok("pueue：连不上 → jobs=None、unreachable")

    # lab0（未启用的条目）的命令也拼得出来
    c0 = PQ.worker_cmd("lab0")
    assert "CUDA_VISIBLE_DEVICES=0" in c0 and M.SITES["lab0"]["worker"]["id"] in c0
    ok("pueue：lab0 条目拼得出命令")

    # ---- slurm ----
    old = dict(P.SLURM)
    try:
        P.SLURM["dicc2"] = dict(gpu={"long": ("3-00:00:00", 0), "normal": "1-00:00:00"})
        jn = M.SITES["dicc2"]["jobs"]["gpu"]
        out = "\n".join(["QOS|long|10|8", "QOS|normal||1",
                         f"JOB|101|{jn}|RUNNING|1:00|long|gpu06", f"JOB|102|{jn}|PENDING|0:00|long|Priority",
                         "JOB|103|tq_xfer|RUNNING|0:01|normal|cpu01"])
        R.RUN = Fake([("sacctmgr", (0, out, ""))])
        st = SL.status("dicc2")
        assert st["counts"] == {"long": (10, 8), "normal": (None, 1)} and len(st["squeue"]) == 3
        assert [j[0] for j in SL.jobs("dicc2", st)] == ["101", "102"]
        ok("slurm：各 QOS 的上限和作业数、只把 worker 作业名的算进 jobs")

        assert SL.plan("dicc2", 2, st) == []
        assert SL.plan("dicc2", 5, st) == [("long", "3-00:00:00", 2), ("normal", "1-00:00:00", 1)]
        ok("slurm：want 减去已有的；按 QOS 顺序、各自的上限分（读不到上限按 10）")

        fake = Fake([("sbatch", (0, "4711\n", ""))])
        R.RUN = fake
        acts = SL.ensure_workers("dicc2", 4, st=st)
        assert [a["qos"] for a in acts] == ["long", "long"] and all(a["ok"] for a in acts) and acts[0]["job"] == "4711"
        cmd = fake.calls[0][1]
        assert cmd.startswith(f"cd {shlex.quote(M.SITES['dicc2']['root'])}/code/current/scripts/queue && sbatch")
        assert "--qos=long" in cmd and "--time=3-00:00:00" in cmd and cmd.endswith(" worker.sbatch")
        ok("slurm：补交走代码包里的 worker.sbatch（再转交 worker_start.sh、开显存填充）")

        fake = Fake([("sbatch", (1, "", "QOSMaxSubmitJobPerUserLimit"))])
        R.RUN = fake
        acts = SL.ensure_workers("dicc2", 5, st=st)
        assert [a["ok"] for a in acts] == [False, False] and len(fake.calls) == 2
        ok("slurm：一个 QOS 交出错就停这个 QOS，换下一个")

        assert SL.ensure_workers("dicc2", 3, st=dict(squeue=None, counts=None))[0]["op"] == "skip"
        assert SL.ensure_workers("dicc1", 3, st=dict(squeue=[], counts={}))[0]["op"] == "skip"
        ok("slurm：读不到作业数、policy 里没有它 → 不交")

        acts = SL.ensure_workers("dicc2", 3, dry=True, st=st)
        assert len(acts) == 1 and acts[0]["ok"] is None
        ok("slurm：dry 只给命令")

        assert SL.sbatch_cmd("dicc1", "long", "1:00:00").startswith("cd ~/") and "user-x" not in SL.sbatch_cmd("dicc1", "long", "1")
        ok("slurm：根目录开头的 ~ 留给那边展开，命令里没有用户名")
    finally:
        P.SLURM.clear()
        P.SLURM.update(old)
        R.RUN = None

    # ---- SITES / RELAY ----
    assert set(M.RELAY) == {k for k, v in M.SITES.items() if v.get("relay")} and "lab0" not in M.RELAY and "dicc1" not in M.RELAY
    for n, c in M.SITES.items():
        assert c["scheduler"] in ("slurm", "pueue") and c.get("root")
        assert c["scheduler"] != "pueue" or (c.get("worker") and c["worker"]["group"] in c["groups"])
    ok("SITES：RELAY = relay=True 的条目；每条都有调度器、根目录，pueue 的 worker 组在 groups 里")
    print("全部通过")


if __name__ == "__main__":
    main()
