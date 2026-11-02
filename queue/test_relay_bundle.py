#!/usr/bin/env python3
"""relay_bundle.py 的测试（2026-10-30；11/1 起不改名）：取文件、钥匙、生成的文件、包里的 taskq.py 完整走一遍、包里的 dicc_worker.sh 跑原来的测试。
包从工作区建（commit=None），根目录换成临时目录。
    python3 scripts/queue/test_relay_bundle.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "envs"))
import machines as M  # noqa: E402
import relay_bundle as B  # noqa: E402

NAME = "dicc2"


def ok(msg):
    print(f"通过：{msg}")


def run(cmd, env=None, **kw):
    e = {k: v for k, v in os.environ.items() if k not in ("BRAIN_TUMOR", "TASKQ_DIR", "TASKQ_MACHINE")}
    e.update(env or {})
    return subprocess.run(cmd, capture_output=True, text=True, env=e, **kw)


def test_source_files_from_commit():
    files = B.source_files("HEAD")
    for f in ("scripts/queue/taskq.py", "scripts/queue/dicc_worker.sh", "research/common/nnunet/trainers/nnUNetTrainer_save10.py",
              "research/RQ1/analysis/run_predict_eval.sh", "research/RQ1/experiments.py", "envs/machines.py"):
        assert f in files, f
    assert any(f.endswith(".R") for f in files) and not any(f.startswith(("papers/", "admin/", "talks/")) for f in files)
    assert not any("__pycache__" in f for f in files) and all(len(files[f][0]) <= B.SMALL_MAX for f in files if f.endswith(B.SMALL))
    head = subprocess.run(["git", "-C", REPO, "show", "HEAD:research/RQ1/analysis/run_predict_eval.sh"], capture_output=True).stdout
    assert files["research/RQ1/analysis/run_predict_eval.sh"] == (head, False)
    ok("从提交取文件：scripts/、envs/、research/ 下的代码和小数据文件，内容和可执行位同提交（.sh 在包里另设可执行），没有 papers/ 等")


def test_secrets():
    sk = "-----BEGIN OPENSSH " + "PRIVATE KEY-----"
    tok = "ghp_" + "a1" * 18
    assert B.secret_hits({"a.sh": ("x\n" + sk + "\n", False), "b.py": (f"T = '{tok}'", False), "c.py": ("ok = 'ghp_short'", False)}) \
        == [("a.sh", 2, sk[:12]), ("b.py", 1, tok[:12])]
    saved = B.source_files
    B.source_files = lambda c: dict(saved(c), **{"scripts/x.sh": (f"echo {tok}".encode(), False)})
    try:
        B.contents(NAME, None)
        raise AssertionError("有钥匙应报错")
    except B.BundleError as e:
        assert "scripts/x.sh:1" in str(e) and tok not in str(e), e
    finally:
        B.source_files = saved
    ok("钥匙：私钥、GitHub token 报错（报错里不带整个钥匙），短的不算")


def test_bundle(T):
    R = os.path.join(T, "R")
    bid, path = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R)
    assert path == os.path.join(R, "code", bid) and bid.startswith("worktree-") and len(bid) == len("worktree-") + 8, bid
    bid2, path2 = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R)
    assert (bid2, path2) == (bid, path)
    assert not [n for n in os.listdir(os.path.join(R, "code")) if n.startswith(".")]
    ok("建包：包名 worktree-<内容哈希>，同样的内容再建一次用原来的包、不留临时目录")

    files = sorted(os.path.relpath(os.path.join(d, f), path) for d, _, fs in os.walk(path) for f in fs)
    src = B.source_files(None)
    for f in ("envs/machines.py", "envs/policy.py", "scripts/queue/worker.sbatch", "scripts/queue/taskq.py", "scripts/queue/dicc_worker.sh",
              "research/common/nnunet/train.sh", "research/RQ1/analysis/run_predict_eval.sh", "research/RQ1/experiments.py"):
        assert f in files, f
    assert "scripts/queue/cpu_worker.sbatch" not in files, "这台没有 jobs.cpu"
    for f in ("research/RQ1/analysis/run_predict_eval.sh", "research/RQ1/nnunet/predict.sh", "scripts/queue/taskq.py"):
        assert open(os.path.join(path, f), encoding="utf-8").read() == B.home_paths(src[f][0].decode(), R), f"{f} 和提交的不一样"
        assert os.access(os.path.join(path, f), os.X_OK) == (src[f][1] or f.endswith(".sh")), f
    assert open(os.path.join(path, "research/RQ1/analysis/run_predict_eval.sh"), "rb").read() == src["research/RQ1/analysis/run_predict_eval.sh"][0]
    for ln, tgt in B.LINKS.items():
        assert os.readlink(os.path.join(path, ln)) == tgt and os.path.isfile(os.path.join(path, ln)), ln
    np_ = open(os.path.join(path, "envs/nnunet_paths.sh"), encoding="utf-8").read()
    assert ".cache/phd" not in np_, np_[:300]
    ok("包里的文件：提交里的代码原样（run_predict_eval.sh、predict.sh 等不改），~/.cache/phd 换成根目录下的，start.sh、loop.sh 指向新名字")

    os.symlink(bid, os.path.join(R, "code", "current"))
    for d in ("data", "queue"):
        os.makedirs(os.path.join(R, d))
    cur = os.path.join(R, "code", "current")
    mp = os.path.join(cur, "envs", "machines.py")
    assert run(["python3", mp, "whoami"]).stdout.strip() == NAME
    assert run(["python3", mp, "names", "slurm"]).stdout.split() == [NAME]
    assert run(["python3", mp, "get", ".", "data"]).stdout.strip() == f"{R}/data"
    e = M.RELAY[NAME]
    assert run(["python3", mp, "get", ".", "conda"]).stdout.split() == list(e["conda"])
    act = run(["python3", mp, "activate", "nnunet"]).stdout
    assert f"conda activate {e['envs']['nnunet'][1]}" in act and e["conda"][0] in act
    pol = run(["python3", "-c", "import sys; sys.path.insert(0, sys.argv[1]); import machines, policy; "
               "print(int(bool(policy.STAGE_TMP.get(machines.whoami()))), int(bool(policy.GPU_AUG.get(machines.whoami()))))",
               os.path.join(cur, "envs")]).stdout.split()
    assert pol == ["1", "0"], pol
    ok("生成的 machines.py、policy.py：只认得这个账号（按数据目录认出本机），环境、数据目录对；STAGE_TMP 开、GPU_AUG 关")

    sb = open(os.path.join(cur, "scripts/queue/worker.sbatch")).read().split("\n")
    real = open(os.path.join(REPO, "scripts/queue/worker.sbatch")).read()
    for ln in real.split("\n"):
        if ln.startswith("#SBATCH") and not any(o in ln for o in ("--output", "--job-name", "--cpus-per-task", "--mem")):
            assert ln in sb, ln
    assert "#SBATCH --cpus-per-task=30" in sb and "#SBATCH --mem=60G" in sb, sb
    assert not any(x.startswith("#SBATCH --job-name") for x in sb), sb
    assert f"#SBATCH --output={R}/data/logs/workers/%x_%j.out" in sb
    assert f"R={R}" in sb and 'REPO=$(readlink -f "$R/code/current")' in sb
    assert "export TASKQ_DIR=$R/queue TASKQ_GPU_PAD_SWITCH=$R/queue/gpu_pad_DISABLED" in sb
    assert sb[-2] == 'exec bash "$REPO/scripts/queue/worker_start.sh"' and "git" not in "\n".join(sb)
    assert run(["bash", "-n", os.path.join(cur, "scripts/queue/worker.sbatch")]).returncode == 0
    fake = os.path.join(T, "fakeR")
    os.makedirs(os.path.join(fake, "code", "b1", "scripts", "queue"))
    with open(os.path.join(fake, "code", "b1", "scripts", "queue", "worker_start.sh"), "w") as f:
        f.write('echo "$TASKQ_DIR|$TASKQ_GPU_PAD_SWITCH|$(dirname "$(dirname "$(dirname "${BASH_SOURCE[0]}")")")"\n')
    os.symlink("b1", os.path.join(fake, "code", "current"))
    body = "\n".join(sb).replace(f"R={R}", f"R={fake}")
    out = run(["bash", "-c", body]).stdout.strip()
    assert out == f"{fake}/queue|{fake}/queue/gpu_pad_DISABLED|{fake}/code/b1", out
    ok("worker.sbatch：CPU、内存、作业名、输出位置按账号，其余资源和原来一样；开始时按 current 的实际包跑，队列和显存开关在根目录下")

    env = {"BRAIN_TUMOR": f"{R}/data", "TASKQ_DIR": f"{R}/queue"}
    tq = ["python3", os.path.join(cur, "scripts/queue/taskq.py")]
    key = "Dataset999_abcdef/nnUNetTrainer_save10/0"
    r = run(tq + ["add", "--nice", "0", "--recipe", "nnunet", "dataset=Dataset999_abcdef", "fold=0"], env)
    assert r.returncode == 0 and key in r.stdout, r
    st = json.loads(next(ln for ln in run(tq + ["--json-status"], env).stdout.splitlines() if ln.startswith("@@JSON"))[6:])
    assert [t["key"] for t in st["tasks"]] == [key] and st["tasks"][0]["state"] == "free", st
    assert st["tasks"][0]["spec"]["relay"] == "nnunet" and st["tasks"][0]["exp"] is None
    r = run(tq + ["next", "--worker", "123", "--envs", "nnunet", "--device", "gpu"], env)
    assert r.returncode == 0, r
    lines = r.stdout.strip().split("\n")
    assert lines[0] == key and lines[1].startswith("nnunet ") and "dataset=Dataset999_abcdef" in lines[1], lines
    assert run(tq + ["next", "--worker", "124", "--envs", "nnunet", "--device", "gpu"], env).returncode == 10
    assert run(tq + ["beat", "--worker", "123", key], env).returncode == 0
    assert run(tq + ["stop", key], env).returncode == 0
    assert os.path.exists(f"{R}/queue/stopped/{key.replace('/', '__')}")
    st = json.loads(next(ln for ln in run(tq + ["--json-status"], env).stdout.splitlines() if ln.startswith("@@JSON"))[6:])
    assert st["tasks"][0]["state"] == "stopped", st["tasks"][0]["state"]
    assert run(tq + ["release", "--worker", "123", key], env).returncode == 0
    assert run(tq + ["resume", key], env).returncode == 0
    assert run(tq + ["remove", key], env).returncode == 0
    assert not open(f"{R}/queue/tasks.jsonl").read().strip()
    r = run(tq + ["--json-status"], {"BRAIN_TUMOR": f"{R}/data"})
    assert r.returncode == 0 and '"enabled": false' in r.stdout
    ok("包里的 taskq.py：add → --json-status → next（0；再领 10）→ beat → stop → release → resume → remove；"
       "不设 TASKQ_DIR 也用根目录下的 queue/")
    return path


def test_cpu_sbatch(T):
    """jobs 里有 cpu 的 Slurm 执行机：另外生成 cpu_worker.sbatch（作业名、资源按 sbatch.cpu，WORKER_DEVICE=cpu）。"""
    R = "~/st/abc"
    entry = dict(jobs=("gpu", "cpu"), sbatch=dict(gpu=dict(cpus=30, mem="60G"), cpu=dict(cpus=8, mem="16G")))
    files = B.contents(NAME, None, root=R, entry=entry)
    sb = files["scripts/queue/cpu_worker.sbatch"][0].split("\n")
    real = open(os.path.join(REPO, "scripts/queue/cpu_worker.sbatch")).read()
    for ln in real.split("\n"):
        if ln.startswith("#SBATCH") and not any(o in ln for o in ("--output", "--job-name", "--cpus-per-task", "--mem")):
            assert ln in sb, ln
    assert not any(x.startswith("#SBATCH --job-name") for x in sb) and "#SBATCH --cpus-per-task=8" in sb and "#SBATCH --mem=16G" in sb, sb
    assert "#SBATCH --output=../../../../data/logs/workers/%x_%j.out" in sb and "R=~/st/abc" in sb, sb
    assert "export TASKQ_DIR=$R/queue TASKQ_GPU_PAD_SWITCH=$R/queue/gpu_pad_DISABLED WORKER_DEVICE=cpu" in sb
    assert sb[-2] == 'exec bash "$REPO/scripts/queue/worker_start.sh"' and "git" not in "\n".join(sb)
    gsb = files["scripts/queue/worker.sbatch"][0]
    assert "#SBATCH --cpus-per-task=30" in gsb and "WORKER_DEVICE" not in gsb
    old = B.contents(NAME, None, root=R, entry=dict(jobs=("gpu", "cpu"), sbatch=dict(cpus=30, mem="60G")))
    assert "#SBATCH --cpus-per-task=30" in old["scripts/queue/worker.sbatch"][0]
    assert "#SBATCH --cpus-per-task=30" not in old["scripts/queue/cpu_worker.sbatch"][0]
    ok("cpu_worker.sbatch：jobs.cpu 有时生成，不带作业名，资源按 sbatch.cpu（老写法的 sbatch 只算 gpu），WORKER_DEVICE=cpu，入口 worker_start.sh")


def test_pueue_site(T):
    """pueue 的执行机（lab 那样，10/31）：不生成 sbatch；slots、worker、组照抄。"""
    R = os.path.join(T, "pueue_site")
    entry = dict(scheduler="pueue", groups={"g1": (1, "1", {})}, slots=1, worker=dict(group="g1", id="w1"))
    files = B.contents(NAME, None, root=R, entry=entry)
    assert not [f for f in files if f.startswith("scripts/queue/") and f.endswith(".sbatch")], sorted(files)
    assert "scripts/queue/worker_start.sh" in files
    _, path = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R, entry=entry)
    os.makedirs(os.path.join(R, "data"))
    mp = os.path.join(path, "envs", "machines.py")
    assert run(["python3", mp, "get", ".", "slots"]).stdout.strip() == "1"
    assert run(["python3", mp, "names", "pueue"]).stdout.split() == [NAME]
    ok("pueue 的执行机：没有 worker.sbatch、cpu_worker.sbatch，slots、组照抄")


def test_dicc_worker_in_bundle(T):
    """包里的 dicc_worker.sh、run_task.sh、taskq.py 跑原来的 test_dicc_worker.sh（环境换成本机的，好真激活）。"""
    R = os.path.join(T, "R2")
    me = M.MACHINES[M.whoami()]
    _, path = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R, entry=dict(conda=me["conda"], envs=me["envs"]))
    os.makedirs(os.path.join(R, "data"))
    test = open(os.path.join(HERE, "test_dicc_worker.sh")).read()
    assert "TASKQ_MACHINE=dicc " in test
    dst = os.path.join(path, "scripts", "queue", "test_in_bundle.sh")
    with open(dst, "w") as f:
        f.write(test.replace("TASKQ_MACHINE=dicc ", f"TASKQ_MACHINE={NAME} "))
    r = run(["bash", dst], timeout=1800)
    assert r.returncode == 0 and "全部通过" in r.stdout and not [ln for ln in r.stdout.splitlines() if ln.startswith("失败：")], r.stdout[-3000:] + r.stderr[-2000:]
    ok(f"包里的 dicc_worker.sh 跑原来的 test_dicc_worker.sh：{r.stdout.count('通过：')} 项全部通过")


def main():
    T = tempfile.mkdtemp(prefix="test_relay_bundle.")
    try:
        test_source_files_from_commit()
        test_secrets()
        test_bundle(T)
        test_cpu_sbatch(T)
        test_pueue_site(T)
        test_dicc_worker_in_bundle(T)
    except BaseException:
        print(f"失败（文件在 {T}）")
        raise
    shutil.rmtree(T)
    print("全部通过")


if __name__ == "__main__":
    main()
