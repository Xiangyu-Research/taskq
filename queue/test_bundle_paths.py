#!/usr/bin/env python3
"""核心装在哪和命令里的路径：送到执行机、store 的命令（那边的 taskq.py、site_ops.py 的操作、交 Slurm 作业的 sbatch 和
worker_start.sh、pueue 的 worker）只用代码包里的固定位置 proto.BUNDLE_CORE，不带 ..；hub 上排的命令（relay.py stage / fetch / archive、
本机的 q）用核心的绝对路径，文件名不变。三种装法各跑一次：
  嵌着（<仓库>/scripts/taskq，用指过来的链接摆出这个布局）、单独的检出（仓库就是核心）、核心在仓库外（TASKQ_REPO 指别处）。
    python3 queue/test_bundle_paths.py
"""
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))


def child():
    sys.path.insert(0, os.path.join(HERE, "testdata"))
    import fixture
    fixture.use()
    sys.path.insert(0, HERE)
    import cfg
    cfg.use_config()
    import proto
    import refill
    import relay
    import relay_remote as RM
    sys.path.insert(0, os.path.join(cfg.CORE, "sites"))
    import pueue as PQ
    import mtab as M
    assert os.path.dirname(cfg.__file__) == HERE, (cfg.__file__, HERE)
    BC = proto.BUNDLE_CORE
    remote = {}

    got = []
    keep = RM.rsh
    RM.rsh = lambda name, cmd, **kw: got.append(cmd) or (0, "", "")
    try:
        RM.rtq("site-pueue", "status --json")
    finally:
        RM.rsh = keep
    remote["rtq"] = got[0]
    assert f"{BC}/queue/taskq.py" in got[0], got

    got = []
    keep = RM.site_argv, RM.C.run
    RM.site_argv = lambda name, cmd, timeout=300: got.append(cmd) or ["true"]
    RM.C.run = lambda *a, **kw: (0, proto.JSON_MARK + "1", "")
    try:
        assert RM.op("site-slurm", "states") == 1
        RM.store_op("done", dict(items={}))
    finally:
        RM.site_argv, RM.C.run = keep
    remote["site_ops"], remote["store 的 site_ops"] = got
    assert all(f"{BC}/queue/site_ops.py" in c and f"/{proto.ROOT['current']} &&" in c for c in got), got

    got = []
    keep = RM.rsh
    RM.rsh = lambda name, cmd, **kw: got.append(cmd) or (0, "123\n", "")
    try:
        relay.submit("site-slurm", [("normal", "1:00:00", 1, "gpu"), ("normal", "1:00:00", 1, "cpu")])
    finally:
        RM.rsh = keep
    remote["交 GPU 作业"], remote["交 CPU 作业"] = got
    cur = f'"$R"/{proto.ROOT["current"]}'
    assert f"{cur}/{BC}/queue/worker.sbatch {cur}/{BC}/queue/worker_start.sh" in got[0], got
    assert f"{cur}/{BC}/queue/cpu_worker.sbatch {cur}/{BC}/queue/worker_start.sh" in got[1], got
    remote["pueue worker"] = PQ.worker_cmd("site-pueue", M.workers("site-pueue")[0])
    assert f"{BC}/queue/worker_start.sh" in remote["pueue worker"]
    for what, text in remote.items():
        assert ".." not in text, (what, text)

    hubq = []
    acts = [dict(op="stage", key="k/1", nice=0), dict(op="fetch", key="k/2"), dict(op="archive", key="k/3")]
    relay.execute("site-pueue", acts, {}, run_add=lambda *a, **kw: hubq.append(a[4]) or 1)
    rp = cfg.local("queue", "relay.py")
    assert len(hubq) == 3 and f"python3 {rp} stage site-pueue" in hubq[0] and f"python3 {rp} fetch site-pueue" in hubq[1] \
        and f"python3 {rp} archive " in hubq[2], hubq
    assert os.path.isabs(rp) and os.path.isfile(rp) and ".." not in rp, rp
    got = []
    refill.q("hub", f"-g {proto.IO_GROUP} -- true", run=lambda c, ssh: got.append(c) or (0, "1"))
    qp = cfg.local("queue", "q")
    assert f" && {qp} -g {proto.IO_GROUP}" in got[0] and os.path.isfile(qp), got
    print(f"PASS:{cfg.CORE_REL}：送到执行机、store 的命令（taskq.py、site_ops.py、交作业、pueue worker）只用 {BC}/…、不带 ..；"
          "hub 上的命令用核心的绝对路径")


def main():
    if sys.argv[1:] == ["--child"]:
        return child()
    core = os.path.dirname(HERE)
    with tempfile.TemporaryDirectory(prefix="test_bundle_paths.") as T:
        os.makedirs(os.path.join(T, "proj", "scripts"))
        os.symlink(core, os.path.join(T, "proj", "scripts", "taskq"))
        os.makedirs(os.path.join(T, "other"))
        e = {k: v for k, v in os.environ.items() if k != "TASKQ_REPO"}
        ways = (("嵌着", os.path.join(T, "proj", "scripts", "taskq", "queue"), e),
                ("单独的检出", HERE, e) if not core.endswith("/scripts/taskq") else ("嵌着（本来的）", HERE, e),
                ("核心在仓库外", HERE, dict(e, TASKQ_REPO=os.path.join(T, "other"))))
        for what, here, env in ways:
            r = subprocess.run([sys.executable, os.path.join(here, "test_bundle_paths.py"), "--child"], env=env, capture_output=True,
                               text=True, cwd=T)
            assert r.returncode == 0, f"{what}：\n{r.stdout}\n{r.stderr[-3000:]}"
            print(r.stdout.strip().replace("PASS:", f"PASS:（{what}）"))  # run_tests 数"PASS:"开头的行
    print("全部通过")


if __name__ == "__main__":
    main()
