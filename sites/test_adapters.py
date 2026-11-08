#!/usr/bin/env python3
"""sites/pueue.py、slurm.py、remote.py、mtab.py 的 worker 和组、queue/setup.sh（taskq site setup）的测试：假的输出，
核对解析和开 / 补 worker 的命令；setup.sh 用假的 pueue、crontab、ssh 真跑。"""
import json
import os
import re
import shlex
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
import remote as R  # noqa: E402
import pueue as PQ  # noqa: E402
import slurm as SL  # noqa: E402
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import cfg  # noqa: E402
import proto  # noqa: E402
QR = cfg.bundle_rel("queue")  # 代码包里调度代码的 queue/（总是 scripts/taskq/queue）
CORE = os.path.dirname(HERE)


def ok(msg):
    print(f"PASS:{msg}")


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
    return json.dumps(dict(tasks={str(i): dict(group=g, label=lbl, status=st) for i, g, lbl, st in tasks}))

FAKE_PUEUE = """#!/bin/bash
echo "$*" >> "$HOME/pueue.log"
case "$1 ${2:-}" in
    "group --json") cat "$HOME/groups.json" ;;
    "status --json") echo '{"tasks": {}}' ;;
esac
"""
FAKE_CRONTAB = """#!/bin/bash
f=$HOME/crontab.txt
if [[ $1 == -l ]]; then [[ -f $f ]] && cat "$f"; elif [[ $1 == - ]]; then cat > "$f.new" && mv "$f.new" "$f"; fi
"""
# 假的 ssh：记下参数，那边的命令在本机跑（代码包里的核心换成这份）
FAKE_SSH = """#!/bin/bash
printf "%s\\n" "$@" > "$HOME/ssh.argv"
exec bash -c "${2//$SETUP_CODE/$SETUP_CORE}"
"""


def setup():
    """taskq site setup（queue/setup.sh）：按整台机器建全部组、设好同时跑几个，别的组不删；hub 也建（核心组、manual_groups）；
    给名字时 ssh 过去跑那边代码包里的 setup.sh，带上 TASKQ_MACHINE 和算好的组。"""
    home = fixture.tmpdir()
    fb = os.path.join(home, "fakebin")
    os.makedirs(os.path.join(home, ".local", "bin"))
    os.makedirs(fb)
    os.makedirs(os.path.join(home, "auto", "demo", ".git"))  # hub 的 repo：已经克隆过
    for path, text in ((os.path.join(home, ".local", "bin", "pueue"), FAKE_PUEUE), (os.path.join(fb, "crontab"), FAKE_CRONTAB),
                       (os.path.join(fb, "ssh"), FAKE_SSH)):
        with open(path, "w") as f:
            f.write(text)
        os.chmod(path, 0o755)

    def run(me, *args):
        for x in ("pueue.log", "crontab.txt", "ssh.argv"):
            if os.path.exists(os.path.join(home, x)):
                os.remove(os.path.join(home, x))
        with open(os.path.join(home, "groups.json"), "w") as f:
            json.dump({"default": {}, "gpu1": {}, "other": {}}, f)  # gpu1 已经有了；other 是别人手动建的
        env = dict(os.environ, HOME=home, PATH=fb + os.pathsep + os.environ["PATH"], TASKQ_MACHINE=me, TASKQ_REPO=CORE,
                   SETUP_CODE=M.code("site-pueue") + "/" + proto.BUNDLE_CORE, SETUP_CORE=CORE)
        r = subprocess.run(["bash", os.path.join(CORE, "queue", "setup.sh"), *args], env=env, capture_output=True, text=True, timeout=120)
        log = open(os.path.join(home, "pueue.log")).read().splitlines() if os.path.exists(os.path.join(home, "pueue.log")) else []
        cron = open(os.path.join(home, "crontab.txt")).read() if os.path.exists(os.path.join(home, "crontab.txt")) else ""
        return r, log, cron

    r, log, cron = run("site-pueue")
    assert r.returncode == 0, r.stdout + r.stderr
    par = [x for x in log if x.startswith("parallel ")]
    assert par == ["parallel -g gpu1 1", "parallel -g gpu0 1", "parallel -g cpu 1", f"parallel -g {proto.IO_GROUP} 0"], log
    assert [x for x in log if x.startswith("group add")] == ["group add gpu0", "group add cpu", f"group add {proto.IO_GROUP}"], log
    assert not any("remove" in x for x in log), log
    assert "@reboot" in cron and "--gpu-log" in cron, cron
    ok("site setup（这台）：一台机器的全部 worker 组 + 核心组，已经有的只设同时跑几个；别的组（other）不删；@reboot 带 --gpu-log")

    r, log, cron = run("hub")
    assert r.returncode == 0, r.stdout + r.stderr
    assert [x for x in log if x.startswith("parallel ")] == [f"parallel -g {proto.IO_GROUP} 0", "parallel -g gpu 1"], log
    assert not any("remove" in x for x in log) and "--gpu-log" not in cron, (log, cron)
    assert all(f"bash {home}/auto/demo/bin/loop.sh {c}" in cron for _, c in cfg.outputs("cron")), cron
    ok("site setup（hub，没有 scheduler）：建核心组和 manual_groups，写健康检查的 cron")

    r, log, cron = run("hub", "site-pueue")
    assert r.returncode == 0, r.stdout + r.stderr
    a = open(os.path.join(home, "ssh.argv")).read().splitlines()
    assert a[0] == "fake-pueue-host" and "TASKQ_MACHINE='site-pueue'" in a[1] \
        and f"SETUP_GROUPS='gpu1:1 gpu0:1 cpu:1 {proto.IO_GROUP}:0 '" in a[1] and f"{M.code('site-pueue')}/{QR}/setup.sh" in a[1], a
    assert [x for x in log if x.startswith("parallel ")][-1] == f"parallel -g {proto.IO_GROUP} 0" and not any("remove" in x for x in log), log
    ok("site setup <名字>：ssh 过去跑那边代码包里的 setup.sh，带上 TASKQ_MACHINE 和 hub 算好的组；那边也不删组")

    r, log, _ = run("site-slurm")
    assert r.returncode == 1 and "没有 pueue 组" in r.stderr and not log, (r.returncode, r.stderr, log)
    r, _, _ = run("hub", "NOPE")
    assert r.returncode == proto.EXIT["gone"] and "机器表里没有 NOPE" in r.stderr, r
    ok("site setup：没有 pueue 组的机器（Slurm）→ 退出码 1，不动 pueue；表里没有的名字 → 退出码 EXIT['gone']")


def main():
    ws = {w["name"]: w for w in M.workers("site-pueue")}
    w, w0, cw = ws["gpu1"], ws["gpu0"], ws["cpu"]
    wl = f"worker:{w['id']}"

    # ---- 机器表 → worker、组 ----
    assert list(ws) == ["gpu1", "gpu0", "cpu"] and [x["id"] for x in ws.values()] == ["site-pueue-gpu1", "site-pueue-gpu0", "site-pueue-cpu"]
    assert [x["group"] for x in ws.values()] == ["gpu1", "gpu0", "cpu"] and [x["kind"] for x in ws.values()] == ["gpu", "gpu", "cpu"]
    assert w0["idle"] == 60 and w0["hours"] and not w["hours"] and cw["cpus"] == 8
    ok("mtab.workers：pueue 机器每个 workers 项一个 worker，名字 gpu<卡号> / cpu，id = <机器>-<名字>，组同名字（核心算，不手写）")
    lab = M.MACHINES["site-pueue"]["label"]
    assert M.machine_of_jobtag(f"{w['id']}-20261107120001") == lab and M.machine_of_jobtag(cw["id"]) == lab
    ok("mtab.machine_of_jobtag：常驻 worker 的作业名（worker id-时间）、worker id 本身 → 那台")
    assert M.groups("site-pueue") == {"gpu1": (1, "1", {}), "gpu0": (1, "0", {}), "cpu": (1, None, {}), proto.IO_GROUP: (0, None, {})}
    assert M.groups("hub") == {proto.IO_GROUP: (0, None, {}), "gpu": (1, "0", {"TASK_WAIT_GPU": "600"})}
    assert M.groups("site-slurm") == {} and M.groups("site-slurm2") == {}
    ok("mtab.groups：一台的全部组 = 每个 worker 一个组、pueue 机器和 hub / store 的核心组、manual_groups；Slurm 机器没有")
    sl = M.workers("site-slurm")
    assert [(x["id"], x["kind"], x["group"]) for x in sl] == [("site-slurm-gpu", "gpu", None), ("site-slurm-cpu", "cpu", None)]
    assert M.kinds("site-pueue") == ("gpu", "cpu") and M.kinds("site-slurm") == ("gpu", "cpu") and M.kinds("site-slurm2") == ("gpu",) \
        and M.kinds("hub") == () and M.workers("site-pueue", "cpu") == [cw]
    assert M.executors() == ["site-pueue", "site-slurm", "site-slurm2"] and not M.is_exec("hub")
    ok("mtab.kinds / executors：pueue 看 workers，Slurm 看 jobs；执行机 = 有 root 和 scheduler 的")

    # ---- pueue ----
    raw = pueue_json((3, w["group"], wl, {"Done": {"result": "Success"}}), (5, "gpu0", "train:x", "Running"),
                     (7, w["group"], wl, "Running"), (8, w["group"], "other", {"Done": {"result": {"Failed": 1}}}))
    jobs = PQ.parse(raw, w["group"])
    assert [j[0] for j in jobs] == [3, 7, 8] and jobs[0][3] == "Success" and jobs[1][2] == "Running" and jobs[2][3] == "{'Failed': 1}"
    assert PQ.parse(json.loads(raw), w["group"]) == jobs
    assert PQ.parse("not json", w["group"]) is None and PQ.parse(None, w["group"]) is None
    ok("pueue.parse：只要 worker 所在的组，Done 的结果读出来；读不出 → None")
    assert [j[0] for j in PQ.workers(w, jobs)] == [3, 7]
    ok("pueue.workers：只认标签 worker:<id>")

    fake = Fake([("add -g", (0, "New task added (id 9).\n"))])
    good, out = PQ.start_worker("site-pueue", fake, w)
    assert good and out == "New task added (id 9)." and len(fake.calls) == 1
    argv = shlex.split(fake.calls[0])
    i = argv.index("--")
    assert len(argv) == i + 2, "pueue add 的 -- 后面只能有一个参数"
    inner = shlex.split(argv[i + 1])[1:]
    assert shlex.split(argv[i + 1])[0] == "exec" and inner[:2] == ["bash", "-c"] and len(inner) == 3
    assert f"WORKER_ID={w['id']}" in inner[2] and "CUDA_VISIBLE_DEVICES=1" in inner[2] and "TASKQ_MACHINE=site-pueue" in inner[2]
    assert argv[argv.index("-g") + 1] == w["group"] and argv[argv.index("-l") + 1] == wl
    ok("pueue.start_worker：整条 bash -c 作为一个参数，组、标签、卡号、TASKQ_MACHINE 来自 mtab.workers")
    assert PQ.start_worker("site-pueue", Fake([("add -g", (1, "boom"))]), w) == (False, "boom")
    ok("pueue.start_worker：出错 → (False, 输出)")
    c0 = PQ.worker_cmd("site-pueue", w0)
    assert "CUDA_VISIBLE_DEVICES=0" in c0 and w0["id"] in c0 and " TASK_GPU_IDLE=60" in c0 and "-g gpu0 " in c0
    ok("pueue：0 号卡的 worker 带上自己的规矩（别人用过要空满 idle 分钟）")
    fake = Fake([("add -g", (0, "New task added (id 10).\n"))])
    assert PQ.start_worker("site-pueue", fake, cw) == (True, "New task added (id 10).")
    argv = shlex.split(fake.calls[0])
    run = shlex.split(argv[argv.index("--") + 1])[3]
    assert argv[argv.index("-g") + 1] == cw["group"] and argv[argv.index("-l") + 1] == f"worker:{cw['id']}"
    assert f"WORKER_ID={cw['id']} WORKER_DEVICE=cpu CUDA_VISIBLE_DEVICES='' TASKQ_CPUS={cw['cpus']} TASKQ_SLOTS=1;" in run, run
    assert run.endswith(f'exec bash "$REPO/{QR}/worker_start.sh"') and "WORKER_DEVICE=gpu" in PQ.worker_cmd("site-pueue", w)
    assert "TASKQ_CPUS" not in PQ.worker_cmd("site-pueue", w)
    ok("pueue：CPU worker 在自己的组、WORKER_DEVICE=cpu、看不到卡、最多用 cpus 个核，入口是 worker_start.sh")
    raw2 = pueue_json((3, w["group"], wl, "Running"), (4, cw["group"], f"worker:{cw['id']}", "Queued"), (5, "gpu0", "x", "Running"))
    jobs2 = PQ.parse(raw2, [w["group"], cw["group"]])
    assert [j[0] for j in jobs2] == [3, 4] and [j[0] for j in PQ.workers(cw, jobs2)] == [4]
    assert [j[0] for j in PQ.workers(w, jobs2)] == [3]
    ok("pueue.parse 读几个组；workers 按 worker 认标签")

    home = fixture.tmpdir()
    for root, shown in ((os.path.join(home, "r o'ot"), None), (os.path.join(home, "a b"), "~/a b")):
        code = os.path.join(root, "code", "v1", QR)
        os.makedirs(code)
        os.symlink(os.path.join(root, "code", "v1"), os.path.join(root, "code", "current"))
        with open(os.path.join(code, "worker_start.sh"), "w") as f:
            f.write('printf "%s\\n" "$0" "$TASKQ_ROOT" "$TASKQ_MACHINE" "$WORKER_ID" "$$" "$@" > "$HOME/argv"\n')
        old_machine = PQ.M.machine
        PQ.M.machine = lambda name: dict(old_machine(name), root=(shown or root) + "/")
        try:
            argv = shlex.split(PQ.worker_cmd("site-pueue", w))
        finally:
            PQ.M.machine = old_machine
        p = subprocess.Popen(["sh", "-c", argv[argv.index("--") + 1]], env=dict(PATH=os.environ["PATH"], HOME=home))  # pueue 的跑法
        assert p.wait(timeout=30) == 0
        got = open(os.path.join(home, "argv")).read().splitlines()
        assert got == [os.path.join(code, "worker_start.sh"), root, "site-pueue", w["id"], str(p.pid)], got
    ok("pueue.worker_cmd：根目录带空格、单引号、~/ 开头时 worker 照样起来（路径都加了引号），拿到 TASKQ_ROOT、TASKQ_MACHINE；"
       "sh 一路 exec 到 worker_start.sh，pueue 停任务时等 worker 自己收尾")

    setup()

    # ---- slurm ----
    old = dict(P.SLURM)
    try:
        P.SLURM["site-slurm2"] = dict(gpu={"long": ("3-00:00:00", 0), "normal": "1-00:00:00"})
        pr = SL.probe("site-slurm2")
        assert "for q in long normal;" in pr and "CPU|" not in pr
        st = SL.status("site-slurm2", Fake([("sacctmgr", (0, "QOS|long|10|8|1\nQOS|normal||1|0\n"))]))
        assert st == dict(counts={"long": (10, 8, 1), "normal": (None, 1, 0)}, cpu=None, free=None), st
        assert SL.status("site-slurm2", Fake([("sacctmgr", (255, "ssh"))])) is None
        assert SL.parse("QOS|long|10||0\nCPU|2\nALL|10|7\n") == dict(counts={}, cpu=2, free=3)
        ok("slurm.status / parse：各 QOS 的上限、段数、在排的段数；CPU、ALL 两行；读不到 → None")
        assert SL.squeue("101|w|RUNNING|1:00|long|n1\n\n") == [["101", "w", "RUNNING", "1:00", "long", "n1"]]
        ok("slurm.squeue：每行按 | 分，空行不要")

        qos = P.SLURM["site-slurm2"]["gpu"]
        assert SL.distribute(qos, st["counts"], 5, "gpu") == [("long", "3-00:00:00", 2, "gpu"), ("normal", "1-00:00:00", 3, "gpu")]
        assert SL.distribute(qos, st["counts"], 1, "gpu") == [("long", "3-00:00:00", 1, "gpu")]
        ok("slurm.distribute：按 QOS 的顺序、各自的上限分（读不到上限按 SUBMIT_CAP），不超过要交的数")

        fake = Fake([("sbatch", (0, "4711\n"))])
        res = SL.start_workers("site-slurm2", [("long", "3-00:00:00", 2, "gpu")], fake)
        assert res == [dict(qos="long", time="3-00:00:00", n=2, kind="gpu", ids=["4711", "4711"], err=None)]
        jn = [re.search(r" -J (\S+) ", c) for c in fake.calls]
        assert all(jn) and all(re.fullmatch(r"[a-z]+-[a-z]+", x.group(1)) for x in jn), fake.calls
        names = {SL.jobname() for _ in range(50)}
        assert len(names) > 40 and all(re.fullmatch(r"[a-z]+-[a-z]+", x) for x in names)
        old_jn = M.MACHINES["site-slurm2"].get("job_names")
        M.MACHINES["site-slurm2"]["job_names"] = "star"
        try:
            fake = Fake([("sbatch", (0, "4712\n"))])
            SL.start_workers("site-slurm2", [("long", "1", 1, "gpu")], fake)
        finally:
            M.MACHINES["site-slurm2"].pop("job_names")
            if old_jn is not None:
                M.MACHINES["site-slurm2"]["job_names"] = old_jn
        stars = {SL.jobname("star") for _ in range(50)}
        assert re.fullmatch(r".* -J [a-z]+[1-9][0-9] .*", fake.calls[0]) and len(stars) > 40 \
            and all(re.fullmatch(r"[a-z]+[1-9][0-9]", x) for x in stars) and not names & stars
        ok("slurm.start_workers：每个作业一个随机名字，按机器表的 job_names 用各自的一套（pair：<词>-<词>，star：<星名><两位数>）")

        # 交作业的命令在 bash 里真跑一遍（假的 sbatch 记下参数）：根目录的 ~ 在那边展开，参数都在 sbatch 的命令行上
        sh = fixture.tmpdir()
        fb = os.path.join(sh, "bin")
        os.makedirs(fb)
        with open(os.path.join(fb, "sbatch"), "w") as f:
            f.write('#!/bin/bash\nprintf "%s\\n" "$@" > "$HOME/sbatch.argv"\necho 4713\n')
        os.chmod(os.path.join(fb, "sbatch"), 0o755)
        env = dict(PATH=fb + ":/usr/bin:/bin", HOME=sh)
        for name, kind, want in (("site-slurm", "gpu", ["--partition=gpu-a", "--gres=gpu:a:1"]),
                                 ("site-slurm", "cpu", ["--partition=cpu-b"]),
                                 ("site-slurm2", "gpu", ["--partition=gpu-a", "--gres=gpu:a:1", "--cpus-per-task=30", "--mem=60G"])):
            r = subprocess.run(["bash", "-c", SL.submit_cmd(name, "long", "3-00:00:00", kind)], env=env, capture_output=True, text=True)
            a = open(os.path.join(sh, "sbatch.argv")).read().splitlines()
            R_ = os.path.join(sh, "q")
            cur = f"{R_}/{proto.ROOT['current']}"
            logs = f"{R_}/{proto.ROOT['data']}/{proto.WORKER_LOGS}"
            assert r.returncode == 0 and r.stdout.strip() == "4713", r
            assert a[:2] == ["--parsable", "-J"] and "--qos=long" in a and "--time=3-00:00:00" in a and f"--chdir={R_}" in a, a
            assert f"--output={logs}/%x_%j.out" in a and os.path.isdir(logs), a
            assert f"--export=ALL,TASKQ_ROOT={R_},TASKQ_MACHINE={name}" in a, a
            assert all(x in a for x in want) and a[-2:] == [f"{cur}/{QR}/{SL.FILES[kind]}", f"{cur}/{QR}/worker_start.sh"], a
        ok("slurm.submit_cmd：交核心固定的 worker.sbatch / cpu_worker.sbatch，分区、卡、核、内存（jobs）、QOS、时限、日志、"
           "TASKQ_ROOT、TASKQ_MACHINE 都在命令行上，作业开始时才经 code/current 找 worker_start.sh；先建好日志目录")
        assert SL.kind_of(f"/home/user/a/q/code/current/{QR}/worker.sbatch") == "gpu" and SL.kind_of("cpu_worker.sbatch") == "cpu" \
            and SL.kind_of("/bin/sh") is None and SL.kind_of("/x/worker.sbatch.bak") is None and SL.kind_of(None) is None
        pr = SL.probe("site-slurm")
        assert "-n " not in pr and "%o" in pr and "cpu_worker[.]sbatch" in pr
        ok("slurm.kind_of / probe：按交的脚本认 worker，不按作业名")
        fake = Fake([("sbatch", (1, "QOSMaxSubmitJobPerUserLimit"))])
        res = SL.start_workers("site-slurm2", [("long", "1", 3, "gpu"), ("normal", "1", 1, "gpu")], fake)
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
    r = M.MACHINES["site-slurm"]["root"]
    assert R.q(r).startswith("~/") and os.path.basename(os.path.expanduser("~")) not in R.q(r)
    ok("remote.q：开头的 ~ 留给那边展开，命令里没有用户名")
    assert all(R.conf(n) == M.machine(n) and R.conf(n)["pueue"] for n in M.MACHINES)
    ok("remote.conf：机器表里每台都查得到，没写的字段补上默认值")
    print("全部通过")


if __name__ == "__main__":
    main()
