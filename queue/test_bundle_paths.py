#!/usr/bin/env python3
"""Where the core sits and paths in commands: commands sent to executors and the store (taskq.py there, site_ops.py ops, the
sbatch and worker_start.sh for Slurm jobs, the pueue worker) use only the bundle's fixed location proto.BUNDLE_CORE and no ..;
commands queued on the hub (relay.py stage / fetch / archive, the local q) use the core's absolute path with the same filenames.
Run once per install layout:
  embedded (<repo>/scripts/taskq, this layout built from a symlink), a standalone checkout (the repo is the core), and the core
  outside the repo (TASKQ_REPO points elsewhere).
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
    remote["site_ops"], remote["store site_ops"] = got
    assert all(f"{BC}/queue/site_ops.py" in c and f"/{proto.ROOT['current']} &&" in c for c in got), got

    got = []
    keep = RM.rsh
    RM.rsh = lambda name, cmd, **kw: got.append(cmd) or (0, "123\n", "")
    try:
        relay.submit("site-slurm", [("normal", "1:00:00", 1, "gpu"), ("normal", "1:00:00", 1, "cpu")])
    finally:
        RM.rsh = keep
    remote["submit GPU job"], remote["submit CPU job"] = got
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
    print(f"PASS:{cfg.CORE_REL}: commands sent to executors and the store (taskq.py, site_ops.py, job submission, pueue worker) use only {BC}/... and no ..; "
          "hub commands use the core's absolute path")


def main():
    if sys.argv[1:] == ["--child"]:
        return child()
    core = os.path.dirname(HERE)
    with tempfile.TemporaryDirectory(prefix="test_bundle_paths.") as T:
        os.makedirs(os.path.join(T, "proj", "scripts"))
        os.symlink(core, os.path.join(T, "proj", "scripts", "taskq"))
        os.makedirs(os.path.join(T, "other"))
        e = {k: v for k, v in os.environ.items() if k != "TASKQ_REPO"}
        ways = (("embedded", os.path.join(T, "proj", "scripts", "taskq", "queue"), e),
                ("standalone checkout", HERE, e) if not core.endswith("/scripts/taskq") else ("embedded (native)", HERE, e),
                ("core outside the repo", HERE, dict(e, TASKQ_REPO=os.path.join(T, "other"))))
        for what, here, env in ways:
            r = subprocess.run([sys.executable, os.path.join(here, "test_bundle_paths.py"), "--child"], env=env, capture_output=True,
                               text=True, cwd=T)
            assert r.returncode == 0, f"{what}:\n{r.stdout}\n{r.stderr[-3000:]}"
            print(r.stdout.strip().replace("PASS:", f"PASS: ({what})"))  # run_tests counts lines starting with "PASS:"
    print("all pass")


if __name__ == "__main__":
    main()
