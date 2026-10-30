#!/usr/bin/env python3
"""relay_bundle.py 的测试（2026-10-30）：去注释、生成的文件、包里的 taskq.py 完整走一遍、包里的 dicc_worker.sh 跑原来的测试。
包从工作区建（commit=None），根目录换成临时目录。
    python3 scripts/queue/test_relay_bundle.py
"""
import ast
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import tokenize

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


def comments(src):
    return [t.string for t in tokenize.generate_tokens(io.StringIO(src).readline)
            if t.type == tokenize.COMMENT and not (t.start[0] == 1 and t.string.startswith("#!"))]


def test_strip_py():
    src = '''#!/usr/bin/env python3
"""模块说明
多行"""
import os  # 注释
X = "# 不是注释"  # 是注释
Y = f"{X}#{os.sep}"


# 整行注释
def f():
    """只有说明"""


class A:
    """说明"""
    s = """多行字符串
# 里面的井号留着
"""

    def g(self):  # 注释
        """说明"""
        return 1  # 注释
'''
    out = B.strip_py(src)
    assert out.startswith("#!/usr/bin/env python3\n"), out
    assert not comments(out), comments(out)
    assert "模块说明" not in out and "只有说明" not in out and "说明" not in out.replace("多行字符串", ""), out
    assert '"# 不是注释"' in out and "# 里面的井号留着" in out and 'f"{X}#{os.sep}"' in out, out
    ns = {}
    exec(compile(out, "t", "exec"), ns)
    assert ns["f"]() is None and ns["A"]().g() == 1 and ns["A"].s.startswith("多行字符串\n# 里")
    py311 = B.nnunet_python()
    if py311:
        r = subprocess.run([py311, "-c", "import sys; compile(sys.stdin.read(), 't', 'exec')"], input=out, text=True)
        assert r.returncode == 0
    ok("strip_py：去掉注释和文档字符串，字符串里的 # 留着，函数体只剩说明时换成 pass，两个 Python 版本都能编译")


def test_strip_sh():
    src = r'''#!/bin/bash
#SBATCH --job-name=x
# 整行注释
set -u  # 行尾注释
n=${#ARR[@]} m=$# s='# 引号里' d="a # b"
read -r a b <<< "$(echo 1 2)"  # 注释
x=$(echo a |  # 子命令里的注释留着
    tr a b)
cat << 'EOF'
# heredoc 里的留着
EOF
echo a \
# 续行（是参数还是注释由 bash 定，不动）
y=$(awk '{ if ($1 != "") print "#" }' <<< "1")  # 注释
echo done
'''
    out = B.strip_sh(src)
    lines = out.split("\n")
    assert lines[:2] == ["#!/bin/bash", "#SBATCH --job-name=x"], lines
    assert "整行注释" not in out and "行尾注释" not in out and out.count("# 注释") == 0, out
    assert "# 引号里" in out and '"a # b"' in out and "${#ARR[@]}" in out and "m=$#" in out
    assert "# 子命令里的注释留着" in out and "# heredoc 里的留着" in out and "# 续行" in out, out
    assert "set -u\n" in out and 'read -r a b <<< "$(echo 1 2)"\n' in out, out
    ok("strip_sh：去掉整行和行尾注释，引号、heredoc、子命令、续行里的不动，<<< 不当 heredoc；declare -f 前后一样")


def test_source_files_from_commit():
    files = B.source_files("HEAD")
    assert set(B.KEEP) <= set(files)
    assert "research/common/nnunet/trainers/nnUNetTrainer_save10.py" in files
    assert not any("__pycache__" in f or os.path.basename(f).startswith("test_") for f in files)
    head = subprocess.run(["git", "-C", REPO, "show", "HEAD:scripts/queue/taskq.py"], capture_output=True).stdout
    assert files["scripts/queue/taskq.py"][0] == head
    ok("从提交取文件：git show 的内容，不带测试和 __pycache__")


def test_bundle(T):
    R = os.path.join(T, "R")
    bid, path = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R)
    assert path == os.path.join(R, "code", bid) and len(bid) == 12
    bid2, path2 = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R)
    assert (bid2, path2) == (bid, path)
    assert not [n for n in os.listdir(os.path.join(R, "code")) if n.startswith(".")]
    ok("建包：包名 12 位，同样的内容再建一次用原来的包、不留临时目录")

    files = sorted(os.path.relpath(os.path.join(d, f), path) for d, _, fs in os.walk(path) for f in fs)
    for f in ("envs/machines.py", "envs/policy.py", "scripts/queue/worker.sbatch", "scripts/queue/taskq.py",
              "research/common/nnunet/train.sh", "research/common/nnunet/trainers/gpu_aug_loader.py"):
        assert f in files, f
    for f in ("scripts/queue/refill.py", "scripts/queue/relay.py", "scripts/queue/relay_names.py", "scripts/queue/steps.py",
              "envs/nnunet_paths.sh"):
        assert f not in files, f
    assert not any(f.startswith("research/RQ") for f in files)
    assert not B.forbidden_hits(path, files)
    for f in files:
        text = open(os.path.join(path, f), encoding="utf-8").read()
        if f.endswith(".py"):
            assert not comments(text), (f, comments(text)[:3])
            assert not [n for n in ast.walk(ast.parse(text)) if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef))
                        and ast.get_docstring(n)], f
    ok("包里的文件：要用的都在，没有实验表、relay*.py、refill.py；Python 没有注释；没有 FORBIDDEN 的字样")

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
        if ln.startswith("#SBATCH") and "--output" not in ln and "--job-name" not in ln:
            assert ln in sb, ln
    assert f"#SBATCH --job-name={e['jobs']['gpu']}" in sb
    assert f"#SBATCH --output={R}/data/logs/workers/%x_%j.out" in sb
    assert f"R={R}" in sb and 'REPO=$(readlink -f "$R/code/current")' in sb
    assert "export TASKQ_DIR=$R/queue RQ1_GPU_PAD_SWITCH=$R/queue/gpu_pad_DISABLED" in sb
    assert sb[-2] == 'exec bash "$REPO/scripts/queue/worker_start.sh"' and "git" not in "\n".join(sb)
    assert run(["bash", "-n", os.path.join(cur, "scripts/queue/worker.sbatch")]).returncode == 0
    fake = os.path.join(T, "fakeR")
    os.makedirs(os.path.join(fake, "code", "b1", "scripts", "queue"))
    with open(os.path.join(fake, "code", "b1", "scripts", "queue", "worker_start.sh"), "w") as f:
        f.write('echo "$TASKQ_DIR|$RQ1_GPU_PAD_SWITCH|$(dirname "$(dirname "$(dirname "${BASH_SOURCE[0]}")")")"\n')
    os.symlink("b1", os.path.join(fake, "code", "current"))
    body = "\n".join(sb).replace(f"R={R}", f"R={fake}")
    out = run(["bash", "-c", body]).stdout.strip()
    assert out == f"{fake}/queue|{fake}/queue/gpu_pad_DISABLED|{fake}/code/b1", out
    ok("worker.sbatch：资源和原来一样，作业名、输出位置按账号；开始时按 current 的实际包跑，队列和显存开关在根目录下")

    env = dict(BRAIN_TUMOR=f"{R}/data", TASKQ_DIR=f"{R}/queue")
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


def test_dicc_worker_in_bundle(T):
    """包里的 dicc_worker.sh、run_task.sh、taskq.py 跑原来的 test_dicc_worker.sh（环境换成本机的，好真激活）。"""
    R = os.path.join(T, "R2")
    me = M.MACHINES[M.whoami()]
    saved = B.FORBIDDEN
    B.FORBIDDEN = [w for w in saved if w != "miniforge"]  # 本机的 conda 位置
    try:
        _, path = B.build(NAME, commit=None, out=os.path.join(R, "code"), root=R,
                          entry=dict(conda=me["conda"], envs=me["envs"]))
    finally:
        B.FORBIDDEN = saved
    os.makedirs(os.path.join(R, "data"))
    test = open(os.path.join(HERE, "test_dicc_worker.sh")).read()
    assert "TASKQ_MACHINE=dicc " in test
    dst = os.path.join(path, "scripts", "queue", "test_dicc_worker.sh")
    with open(dst, "w") as f:
        f.write(test.replace("TASKQ_MACHINE=dicc ", f"TASKQ_MACHINE={NAME} "))
    r = run(["bash", dst], timeout=1800)
    assert r.returncode == 0 and "全部通过" in r.stdout and not [ln for ln in r.stdout.splitlines() if ln.startswith("失败：")], r.stdout[-3000:] + r.stderr[-2000:]
    ok(f"包里的 dicc_worker.sh 跑原来的 test_dicc_worker.sh：{r.stdout.count('通过：')} 项全部通过")


def main():
    T = tempfile.mkdtemp(prefix="test_relay_bundle.")
    try:
        test_strip_py()
        test_strip_sh()
        test_source_files_from_commit()
        test_bundle(T)
        test_dicc_worker_in_bundle(T)
    except BaseException:
        print(f"失败（文件在 {T}）")
        raise
    shutil.rmtree(T)
    print("全部通过")


if __name__ == "__main__":
    main()
