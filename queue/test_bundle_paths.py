#!/usr/bin/env python3
"""核心装在哪和命令里的路径（2026-11-04）：送到执行机的命令（那边的 taskq.py、小程序的 CODEQ、交 Slurm 作业的目录、pueue 的 worker、
生成的 sbatch）只用代码包里的固定位置 proto.BUNDLE_CORE，不带 ..；box 上排的命令（relay.py stage / fetch、archive_task.sh、
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
    import relay_bundle as B
    import relay_remote as RM
    sys.path.insert(0, os.path.join(cfg.CORE, "sites"))
    import pueue as PQ
    import slurm as SL
    assert os.path.dirname(cfg.__file__) == HERE, (cfg.__file__, HERE)
    BC = proto.BUNDLE_CORE
    remote = {}

    got = []
    keep = RM.rsh
    RM.rsh = lambda name, cmd, **kw: got.append(cmd) or (0, "", "")
    try:
        RM.rtq("lab1", "status --json")
    finally:
        RM.rsh = keep
    remote["rtq"] = got[0]
    assert f"{BC}/queue/taskq.py" in got[0], got
    assert RM.CONST["CODEQ"] == f"{BC}/queue", RM.CONST
    remote["小程序"] = RM.PRE

    got = []
    keep = SL.start_workers
    SL.start_workers = lambda cwd, subs, run, names=None: got.append(cwd) or []
    try:
        relay.submit("dicc1", [("normal", "gpu", 1)])
    finally:
        SL.start_workers = keep
    remote["交作业的目录"] = got[0]
    assert got[0].endswith(f"/{proto.ROOT['current']}/{BC}/queue"), got
    remote["pueue worker"] = PQ.worker_cmd("lab1")
    assert f"{BC}/queue/worker_start.sh" in remote["pueue worker"]

    e = B.relay_entry(None, "dicc1")
    m = B.machine_entry("dicc1", e, "/abs/root")
    tpl = open(os.path.join(cfg.CORE, B.SBATCH["gpu"])).read()
    sb = B.gen_sbatch(tpl, m, "/abs/root")
    remote["sbatch"] = sb
    assert sb.split("\n")[-2] == f'exec bash "$REPO/{BC}/queue/worker_start.sh"', sb
    tilde = B.gen_sbatch(tpl, m, "~/q")  # 根目录以 ~ 开头：日志写相对交作业的目录（<根>/code/current/BC/queue）的路径
    up = next(ln for ln in tilde.split("\n") if ln.startswith("#SBATCH --output=")).split("=", 1)[1].split("/data/")[0]
    assert up == "/".join([".."] * (2 + len(f"{BC}/queue".split("/")))), up
    for what, text in remote.items():
        assert ".." not in text, (what, text)
    import ast
    stub = B.gen_machines('MACHINES = {}\n', "lab1", B.machine_entry("lab1", B.relay_entry(None, "lab1"), "/abs/root"))
    mt = [n.value for n in ast.walk(ast.parse(stub)) if isinstance(n, ast.Constant) and str(n.value).endswith("mtab.py")]
    assert mt == [f"../{BC}/sites/mtab.py"], mt  # 生成的 envs/machines.py 的命令行转交：包里的核心（envs/ 的上一级）

    box = []
    acts = [dict(op="stage", key="k/1", nice=0), dict(op="fetch", key="k/2"), dict(op="archive", key="k/3")]
    relay.execute("lab1", acts, {}, run_add=lambda *a, **kw: box.append(a[4]) or 1)
    rp, ap = cfg.local("queue", "relay.py"), cfg.local("queue", "archive_task.sh")
    assert len(box) == 3 and f"python3 {rp} stage lab1" in box[0] and f"python3 {rp} fetch lab1" in box[1] \
        and f"bash {ap} " in box[2], box
    for p in (rp, ap):
        assert os.path.isabs(p) and os.path.isfile(p) and ".." not in p, p
    got = []
    refill.q("box", "-g cpu -- true", run=lambda c, ssh: got.append(c) or (0, "1"))
    qp = cfg.local("queue", "q")
    assert f" && {qp} -g cpu" in got[0] and os.path.isfile(qp), got
    print(f"通过：{cfg.CORE_REL}：送到执行机的命令和 sbatch 只用 {BC}/…、不带 ..；box 上的命令用核心的绝对路径")


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
            print(r.stdout.strip().replace("通过：", f"通过（{what}）："))
    print("全部通过")


if __name__ == "__main__":
    main()
