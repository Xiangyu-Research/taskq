#!/usr/bin/env python3
"""scripts/taskq/sites/pueue.py、slurm.py、remote.py 的测试：假的输出，核对解析和开 / 补 worker 的命令。"""
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
import remote as R  # noqa: E402
import pueue as PQ  # noqa: E402
import slurm as SL  # noqa: E402
import machines as M  # noqa: E402
import policy as P  # noqa: E402
import cfg  # noqa: E402
QR = cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）


def ok(msg):
    print("  ok ", msg)


class Fake:
    """按命令里的关键字回放 (退出码, 输出)，记下每条命令。"""

    def __init__(self, replies):
        self.replies, self.calls = replies, []

    def __call__(self, cmd):
        self.calls.append(cmd)
        for key, rep in self.replies:
            if key in cmd:
                return rep
        return 0, ""


def pueue_json(*tasks):
    return json.dumps(dict(tasks={str(i): dict(group=g, label=lab, status=st) for i, g, lab, st in tasks}))


def main():
    lab1 = M.SITES["lab1"]
    w = lab1["worker"]
    wl = f"worker:{w['id']}"

    # ---- pueue ----
    raw = pueue_json((3, w["group"], wl, {"Done": {"result": "Success"}}), (5, "gpu0", "train:x", "Running"),
                     (7, w["group"], wl, "Running"), (8, w["group"], "other", {"Done": {"result": {"Failed": 1}}}))
    jobs = PQ.parse(raw, w["group"])
    assert [j[0] for j in jobs] == [3, 7, 8] and jobs[0][3] == "Success" and jobs[1][2] == "Running" and jobs[2][3] == "{'Failed': 1}"
    assert PQ.parse(json.loads(raw), w["group"]) == jobs
    assert PQ.parse("not json", w["group"]) is None and PQ.parse(None, w["group"]) is None
    ok("pueue.parse：只要 worker 所在的组，Done 的结果读出来；读不出 → None")
    assert [j[0] for j in PQ.workers("lab1", jobs)] == [3, 7]
    ok("pueue.workers：只认标签 worker:<id>")

    fake = Fake([("add -g", (0, "New task added (id 9).\n"))])
    good, out = PQ.start_worker("lab1", fake)
    assert good and out == "New task added (id 9)." and len(fake.calls) == 1
    argv = shlex.split(fake.calls[0])
    i = argv.index("--")
    assert len(argv) == i + 2, "pueue add 的 -- 后面只能有一个参数"
    inner = shlex.split(argv[i + 1])[1:]
    assert shlex.split(argv[i + 1])[0] == "exec" and inner[:2] == ["bash", "-c"] and len(inner) == 3
    assert f"WORKER_ID={w['id']}" in inner[2] and f"CUDA_VISIBLE_DEVICES={lab1['groups'][w['group']][1]}" in inner[2]
    assert argv[argv.index("-g") + 1] == w["group"] and argv[argv.index("-l") + 1] == wl
    ok("pueue.start_worker：整条 bash -c 作为一个参数（ccb0ba6），组、标签、卡号来自 SITES")
    assert PQ.start_worker("lab1", Fake([("add -g", (1, "boom"))])) == (False, "boom")
    ok("pueue.start_worker：出错 → (False, 输出)")
    c0 = PQ.worker_cmd("lab0")
    assert "CUDA_VISIBLE_DEVICES=0" in c0 and M.SITES["lab0"]["worker"]["id"] in c0
    assert all(f" {k}={v}" in c0 for k, v in M.SITES["lab0"]["groups"]["gpu0"][2].items())  # 组的环境变量带给 worker
    ok("pueue：lab0 条目拼得出命令")
    cw = lab1["cpu_worker"]
    fake = Fake([("add -g", (0, "New task added (id 10).\n"))])
    assert PQ.start_worker("lab1", fake, "cpu") == (True, "New task added (id 10).")
    argv = shlex.split(fake.calls[0])
    run = shlex.split(argv[argv.index("--") + 1])[3]
    assert argv[argv.index("-g") + 1] == cw["group"] and argv[argv.index("-l") + 1] == f"worker:{cw['id']}"
    assert f"WORKER_ID={cw['id']} WORKER_DEVICE=cpu CUDA_VISIBLE_DEVICES= TASKQ_CPUS={cw['cpus']};" in run, run
    assert run.endswith(f'exec bash "$REPO/{QR}/worker_start.sh"') and "WORKER_DEVICE=gpu" in PQ.worker_cmd("lab1")
    assert "TASKQ_CPUS" not in PQ.worker_cmd("lab1")
    ok("pueue：CPU worker 在 cpu_worker 的组、WORKER_DEVICE=cpu、看不到卡、最多用 cpus 个核，入口是 worker_start.sh")
    raw2 = pueue_json((3, w["group"], wl, "Running"), (4, cw["group"], f"worker:{cw['id']}", "Queued"), (5, "gpu0", "x", "Running"))
    jobs2 = PQ.parse(raw2, [w["group"], cw["group"]])
    assert [j[0] for j in jobs2] == [3, 4] and [j[0] for j in PQ.workers("lab1", jobs2, "cpu")] == [4]
    assert [j[0] for j in PQ.workers("lab1", jobs2)] == [3]
    ok("pueue.parse 读几个组；workers 按种类认标签")
    assert M.kinds("lab1") == ("gpu", "cpu") and M.kinds("lab0") == ("gpu",) and M.kinds("dicc1") == ("gpu", "cpu") \
        and M.kinds("dicc2") == ("gpu",) and M.worker_of("dicc1", "cpu") == dict(kind="cpu") and M.worker_of("lab0", "cpu") is None
    ok("machines.kinds / worker_of：pueue 看 worker、cpu_worker，Slurm 看 jobs")

    home = tempfile.mkdtemp()
    for root, shown in ((os.path.join(home, "r o'ot"), None), (os.path.join(home, "a b"), "~/a b")):
        code = os.path.join(root, "code", "v1", QR)
        os.makedirs(code)
        os.symlink(os.path.join(root, "code", "v1"), os.path.join(root, "code", "current"))
        with open(os.path.join(code, "worker_start.sh"), "w") as f:
            f.write('printf "%s\\n" "$0" "$TASKQ_DIR" "$WORKER_ID" "$$" "$@" > "$HOME/argv"\n')
        old_conf = PQ.R.conf
        PQ.R.conf = lambda name: dict(old_conf(name), root=(shown or root) + "/")
        try:
            argv = shlex.split(PQ.worker_cmd("lab1"))
        finally:
            PQ.R.conf = old_conf
        p = subprocess.Popen(["sh", "-c", argv[argv.index("--") + 1]], env=dict(PATH=os.environ["PATH"], HOME=home))  # pueue 的跑法
        assert p.wait(timeout=30) == 0
        got = open(os.path.join(home, "argv")).read().splitlines()
        assert got == [os.path.join(code, "worker_start.sh"), os.path.join(root, "queue"), w["id"], str(p.pid)], got
    ok("pueue.worker_cmd：根目录带空格、单引号、~/ 开头时 worker 照样起来（路径都加了引号）；"
       "sh 一路 exec 到 worker_start.sh，pueue 停任务时等 worker 自己收尾")

    # ---- slurm ----
    old = dict(P.SLURM)
    try:
        P.SLURM["dicc2"] = dict(gpu={"long": ("3-00:00:00", 0), "normal": "1-00:00:00"})
        pr = SL.probe("dicc2")
        assert "for q in long normal;" in pr and "CPU|" not in pr
        st = SL.status("dicc2", Fake([("sacctmgr", (0, "QOS|long|10|8|1\nQOS|normal||1|0\n"))]))
        assert st == dict(counts={"long": (10, 8, 1), "normal": (None, 1, 0)}, cpu=None, free=None), st
        assert SL.status("dicc2", Fake([("sacctmgr", (255, "ssh"))])) is None
        assert SL.parse("QOS|long|10||0\nCPU|2\nALL|10|7\n") == dict(counts={}, cpu=2, free=3)
        ok("slurm.status / parse：各 QOS 的上限、段数、在排的段数；CPU、ALL 两行；读不到 → None")
        assert SL.squeue("101|w|RUNNING|1:00|long|n1\n\n") == [["101", "w", "RUNNING", "1:00", "long", "n1"]]
        ok("slurm.squeue：每行按 | 分，空行不要")

        qos = P.SLURM["dicc2"]["gpu"]
        assert SL.distribute(qos, st["counts"], 5, "worker.sbatch") == [("long", "3-00:00:00", 2, "worker.sbatch"),
                                                                        ("normal", "1-00:00:00", 3, "worker.sbatch")]
        assert SL.distribute(qos, st["counts"], 1, "worker.sbatch") == [("long", "3-00:00:00", 1, "worker.sbatch")]
        ok("slurm.distribute：按 QOS 的顺序、各自的上限分（读不到上限按 SUBMIT_CAP），不超过要交的数")

        fake = Fake([("sbatch", (0, "4711\n"))])
        res = SL.start_workers(f"~/'st/x'/code/current/{QR}", [("long", "3-00:00:00", 2, "worker.sbatch")], fake)
        assert res == [dict(qos="long", time="3-00:00:00", n=2, file="worker.sbatch", ids=["4711", "4711"], err=None)]
        c0 = re.fullmatch(rf"cd ~/'st/x'/code/current/{QR} && sbatch --parsable -J ([a-z]+-[a-z]+) --qos=long --time=3-00:00:00 worker.sbatch", fake.calls[0])
        c1 = re.fullmatch(r".* -J ([a-z]+-[a-z]+) .*", fake.calls[1])
        assert c0 and c1, fake.calls
        names = {SL.jobname() for _ in range(50)}
        assert len(names) > 40 and all(re.fullmatch(r"[a-z]+-[a-z]+", x) for x in names)
        fake = Fake([("sbatch", (0, "4712\n"))])
        SL.start_workers("d", [("long", "1", 1, "worker.sbatch")], fake, "star")
        stars = {SL.jobname("star") for _ in range(50)}
        assert re.fullmatch(r".* -J [a-z]+[1-9][0-9] .*", fake.calls[0]) and len(stars) > 40 \
            and all(re.fullmatch(r"[a-z]+[1-9][0-9]", x) for x in stars) and not names & stars
        ok("slurm.start_workers：交的是那边的 worker.sbatch（再转交 worker_start.sh、开显存填充），每个作业一个随机名字，"
           "按机器表的 job_names 用各自的一套（pair：<词>-<词>，star：<星名><两位数>）")
        assert SL.kind_of(f"/home/user/a/q/code/current/{QR}/worker.sbatch") == "gpu" and SL.kind_of("cpu_worker.sbatch") == "cpu" \
            and SL.kind_of("/bin/sh") is None and SL.kind_of("/x/worker.sbatch.bak") is None and SL.kind_of(None) is None
        pr = SL.probe("dicc1")
        assert "-n " not in pr and "%o" in pr and "cpu_worker[.]sbatch" in pr
        ok("slurm.kind_of / probe：按交的脚本认 worker，不按作业名")
        fake = Fake([("sbatch", (1, "QOSMaxSubmitJobPerUserLimit"))])
        res = SL.start_workers("d", [("long", "1", 3, "worker.sbatch"), ("normal", "1", 1, "worker.sbatch")], fake)
        assert [r["ids"] for r in res] == [[], []] and res[0]["err"] == "QOSMaxSubmitJobPerUserLimit" and len(fake.calls) == 2
        ok("slurm.start_workers：一个 QOS 交出错就停这个 QOS，换下一个")
        assert not any(hasattr(SL, f) for f in ("plan", "ensure_workers", "jobs", "sbatch_cmd"))
        assert not any(hasattr(PQ, f) for f in ("ensure_workers", "status"))
        ok("要几个只在调用方算（relay.topup、refill.topup_plan），适配器里没有")
    finally:
        P.SLURM.clear()
        P.SLURM.update(old)

    # ---- remote ----
    assert R.q("~") == "~" and R.q("~/st/a b") == "~/'st/a b'" and R.q("/scr/x") == "/scr/x"
    assert R.q(M.SITES["dicc1"]["root"]).startswith("~/") and "user-x" not in R.q(M.SITES["dicc1"]["root"])
    ok("remote.q：开头的 ~ 留给那边展开，命令里没有用户名")
    assert R.conf("dicc") is M.MACHINES["dicc"] and R.conf("lab1") is M.RELAY["lab1"]
    ok("remote.conf：执行机和我的 DICC（MACHINES）都查得到")

    # ---- SITES / RELAY ----
    assert set(M.RELAY) == {k for k, v in M.SITES.items() if v.get("relay")} and {"lab0", "dicc1", "lab1", "dicc2"} <= set(M.RELAY)
    for n, c in M.SITES.items():
        assert c["scheduler"] in ("slurm", "pueue") and c.get("root")
        assert c["scheduler"] != "pueue" or (c.get("worker") and all(M.worker_of(n, k)["group"] in c["groups"] for k in M.kinds(n)))
    ok("SITES：RELAY = relay=True 的条目；每条都有调度器、根目录，pueue 的各种 worker 的组在 groups 里")
    print("全部通过")


if __name__ == "__main__":
    main()
