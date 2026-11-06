#!/usr/bin/env python3
"""跑核心的全部测试（2026-11-04）：queue/、sites/、sync/、monitor/ 下的 test_*.py、test_*.sh，同时跑 -j 个文件，上次慢的先跑。

    taskq self test [-j N] [--out 目录] [--timeout 秒] [--gpu] [-l] [文件或模式…]
    python3 queue/run_tests.py …（同上）

每个文件在核心的根目录下跑，有自己的临时 HOME 和短的 TMPDIR（unix socket 的路径有长度限制），环境只有 PATH、LANG、HOME、TMPDIR
（不继承 TASKQ_*，测试用自己的固定配置 queue/testdata/）。临时 HOME 里链着本机的 conda（miniforge3 等）、~/envs/* 和 .gitconfig。
模式：文件的路径或文件名，可以用 * ?，也可以是路径的一部分（relay → test_relay*.py）。

  跳过：文件开头几行有"# 要真卡"的（--gpu 才跑）；测试自己退出码 77（缺 numpy、pueue 等，原因是输出里"跳过："那一行）。
  结果：每个文件一行（测试、结果、秒、"通过："的项数、备注：跳过的原因、写进 HOME 的东西）打在标准输出，同时写 <目录>/report.tsv，
        每个文件的输出在 <目录>/log_<文件>.txt。默认目录 ~/.cache/taskq-test。
  退出码：0 都通过（或跳过），1 有没通过的，2 用法错（没有匹配的测试）。
"""
import argparse
import fnmatch
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

CORE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIRS = ("queue", "sites", "sync", "monitor")
SKIP = 77  # 测试自己说跳过（automake 的写法）
GPU_MARK = re.compile(r"^#\s*要真卡")
OUT = "~/.cache/taskq-test"
PASS, SKIPPED = "通过", "跳过"


def discover():
    out = []
    for d in DIRS:
        p = os.path.join(CORE, d)
        if os.path.isdir(p):
            out += [f"{d}/{f}" for f in os.listdir(p) if re.fullmatch(r"test_.+\.(py|sh)", f)]
    return sorted(out)


def pick(tests, pats):
    if not pats:
        return tests
    return [t for t in tests if any(fnmatch.fnmatch(t, p) or fnmatch.fnmatch(os.path.basename(t), p) or p in t for p in pats)]


def needs_gpu(t):
    with open(os.path.join(CORE, t), encoding="utf-8", errors="replace") as f:
        return any(GPU_MARK.match(ln) for ln in f.readlines()[:10])


def make_home(home):
    """临时 HOME：拷 .gitconfig，链本机的 conda 和 ~/envs/* 下的每个环境（测试新建的留在临时 HOME）→ 建好时有的名字。"""
    os.makedirs(home)
    real = os.path.expanduser("~")
    if os.path.isfile(os.path.join(real, ".gitconfig")):
        shutil.copy(os.path.join(real, ".gitconfig"), home)
    for d in ("miniforge3", "miniconda3", "anaconda3"):
        if os.path.isdir(os.path.join(real, d)):
            os.symlink(os.path.join(real, d), os.path.join(home, d))
    if os.path.isdir(os.path.join(real, "envs")):
        os.makedirs(os.path.join(home, "envs"))
        for e in os.listdir(os.path.join(real, "envs")):
            os.symlink(os.path.join(real, "envs", e), os.path.join(home, "envs", e))
    return set(os.listdir(home))


def short_base():
    """放临时 HOME、TMPDIR 的文件夹：路径要短（unix socket 的路径最长 107 字节），/tmp 能写就放 /tmp。"""
    for d in ("/tmp", tempfile.gettempdir()):
        if os.path.isdir(d) and os.access(d, os.W_OK):
            return tempfile.mkdtemp(prefix="tqt", dir=d)
    return tempfile.mkdtemp(prefix="tqt")


def env_path():
    """本 Python 所在的文件夹在前（子进程的 python3 和跑测试的是同一个），再是系统的，再是 pueue 所在的。"""
    parts = [os.path.dirname(sys.executable), "/usr/local/bin", "/usr/bin", "/bin"]
    pq = shutil.which("pueue") or (os.path.expanduser("~/.local/bin/pueue") if os.access(os.path.expanduser("~/.local/bin/pueue"), os.X_OK) else None)
    if pq:
        parts.append(os.path.dirname(pq))
    seen = []
    for p in parts:
        if p not in seen:
            seen.append(p)
    return ":".join(seen)


def skip_reason(log):
    lines = [ln.strip() for ln in open(log, encoding="utf-8", errors="replace").read().splitlines() if ln.strip()]
    hit = [ln for ln in lines if ln.startswith("跳过：")]
    return (hit[-1][3:] if hit else lines[-1] if lines else "没说原因")


def stop_group(p):
    try:
        os.killpg(p.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return
    try:
        p.wait(10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


def run_one(t, i, a, out, base, env):
    """→ (测试, 结果, 秒, 项, 备注)"""
    if not a.gpu and needs_gpu(t):
        return t, SKIPPED, 0, 0, "要真卡（--gpu 才跑）"
    name = t.replace("/", "__")
    home, tmp = os.path.join(base, str(i), "h"), os.path.join(base, str(i), "t")
    made = make_home(home)
    os.makedirs(tmp)
    cmd = [sys.executable, t] if t.endswith(".py") else ["bash", t]
    log = os.path.join(out, f"log_{name}.txt")
    s = time.time()
    with open(log, "w") as lf:
        p = subprocess.Popen(cmd, cwd=CORE, env=dict(env, HOME=home, TMPDIR=tmp), stdout=lf, stderr=subprocess.STDOUT,
                             stdin=subprocess.DEVNULL, start_new_session=True)
        try:
            rc = p.wait(a.timeout)
        except subprocess.TimeoutExpired:
            rc = None
        stop_group(p)  # 超时的整组停掉；正常结束的，测试留下的后台进程也停掉
    sec = int(time.time() - s)
    with open(log, encoding="utf-8", errors="replace") as lf:
        n = sum(ln.lstrip().startswith("通过：") for ln in lf)
    wrote = sorted(set(os.listdir(home)) - made)
    note = ("写了 HOME：" + " ".join(wrote)) if wrote else ""
    if rc == SKIP:
        return t, SKIPPED, sec, n, skip_reason(log)
    res = PASS if rc == 0 else f"超时（{a.timeout} 秒）" if rc is None else f"失败（{rc}）"
    return t, res, sec, n, note


def last_seconds(rep):
    last = {}
    if os.path.isfile(rep):
        for ln in open(rep, encoding="utf-8").read().splitlines()[1:]:
            f = ln.split("\t")
            if len(f) >= 3 and f[2].isdigit():
                last[f[0]] = int(f[2])
    return last


def table(rows):
    head = ("测试", "结果", "秒", "项", "备注")
    cells = [head] + [tuple(map(str, r)) for r in rows]

    def width(s):  # 中文字符占两格
        return sum(2 if ord(c) > 0x2e7f else 1 for c in s)
    w = [max(width(r[k]) for r in cells) for k in range(4)]
    return "\n".join(("  ".join(r[k] + " " * (w[k] - width(r[k])) for k in range(4)) + "  " + r[4]).rstrip() for r in cells)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog="\n\n".join(__doc__.split("\n\n")[2:]))
    ap.add_argument("tests", nargs="*", metavar="文件或模式", help="只跑这些（默认全部）")
    ap.add_argument("-j", "--jobs", type=int, default=min(4, os.cpu_count() or 1), help="同时跑几个文件（默认 4，不超过核数）")
    ap.add_argument("--out", default=OUT, help=f"结果和日志放哪（默认 {OUT}）")
    ap.add_argument("--timeout", type=int, default=900, help="每个文件最多跑几秒（默认 900）")
    ap.add_argument("--gpu", action="store_true", help="也跑要真卡的（# 要真卡）")
    ap.add_argument("-l", "--list", action="store_true", help="只列出会跑哪些，不跑")
    ap.add_argument("--keep", action="store_true", help="留下临时 HOME 和 TMPDIR（默认跑完删掉）")
    a = ap.parse_args(argv)
    tests = pick(discover(), a.tests)
    if not tests:
        print(f"run_tests：没有匹配的测试：{' '.join(a.tests)}", file=sys.stderr)
        return 2
    if a.list:
        print("\n".join(t + ("\t要真卡" if needs_gpu(t) else "") for t in tests))
        return 0
    out = os.path.abspath(os.path.expanduser(a.out))
    os.makedirs(out, exist_ok=True)
    rep = os.path.join(out, "report.tsv")
    last = last_seconds(rep)
    base = short_base()
    env = {"PATH": env_path(), "LANG": "C.UTF-8"}
    order = sorted(tests, key=lambda t: (-last.get(t, 0), t))
    rows, t0 = {}, time.time()
    print(f"run_tests：{len(tests)} 个文件，同时跑 {max(1, a.jobs)} 个；日志在 {out}", file=sys.stderr)
    try:
        with ThreadPoolExecutor(max(1, a.jobs)) as ex:
            futs = [ex.submit(run_one, t, i, a, out, base, env) for i, t in enumerate(order)]
            for fut in as_completed(futs):
                r = fut.result()
                rows[r[0]] = r
                print(f"  {r[1]}  {r[0]}（{r[2]} 秒）" + (f"：{r[4]}" if r[1] == SKIPPED else ""), file=sys.stderr, flush=True)
    finally:
        if not a.keep:
            shutil.rmtree(base, ignore_errors=True)
    done = [rows[t] for t in tests]
    with open(rep, "w", encoding="utf-8") as f:
        f.write("测试\t结果\t秒\t项\t备注\n")
        f.writelines("\t".join(map(str, r)) + "\n" for r in done)
    bad = [r for r in done if r[1] not in (PASS, SKIPPED)]
    skipped = sum(r[1] == SKIPPED for r in done)
    print(table(done))
    print(f"\n{len(done)} 个文件：{len(done) - len(bad) - skipped} 个通过，{skipped} 个跳过，{len(bad)} 个没通过；"
          f"用时 {int(time.time() - t0)} 秒；结果表 {rep}" + (f"；临时文件 {base}" if a.keep else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
