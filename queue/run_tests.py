#!/usr/bin/env python3
"""Run every core test: test_*.py and test_*.sh under queue/, sites/, sync/ and monitor/, -j files at a time, slowest first.

    taskq self test [-j N] [--out DIR] [--timeout SECONDS] [--gpu] [-l] [FILE or PATTERN...]
    python3 queue/run_tests.py ... (same)

Each file runs at the core root with its own temp HOME and a short TMPDIR (unix socket paths have a length limit); the
environment holds only PATH, LANG, HOME and TMPDIR (no TASKQ_*; tests use their own fixed config in queue/testdata/).
The temp HOME links this machine's conda (miniforge3 etc.), ~/envs/* and .gitconfig.
Patterns: a file path or name, with * and ?, or part of a path (relay -> test_relay*.py).

  Skip: files whose first lines say "# needs-gpu" (only with --gpu); a test exiting 77 (missing numpy, pueue etc.;
        the reason is its "SKIP:" line).
  Result: one line per file (test, result, seconds, count: lines starting "PASS:" plus unittest's Ran N tests, note:
        skip reason, things written to HOME) on stdout and in <dir>/report.tsv; each file's output in
        <dir>/log_<file>.txt. Default dir ~/.cache/taskq-test.
  Exit code: 0 all pass (or skip), 1 some failed, 2 usage error (no matching tests).
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
SKIP = 77  # the test says it skips (automake style)
GPU_MARK = re.compile(r"^#\s*needs-gpu")
OUT = "~/.cache/taskq-test"
PASS, SKIPPED = "PASS", "SKIP"


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
    """Temp HOME: copy .gitconfig, link the machine's conda and each env under ~/envs/* (envs a test creates stay in the temp HOME)."""
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
    """Dir for temp HOMEs and TMPDIRs: keep the path short (unix socket paths max out at 107 bytes); /tmp if writable."""
    for d in ("/tmp", tempfile.gettempdir()):
        if os.path.isdir(d) and os.access(d, os.W_OK):
            return tempfile.mkdtemp(prefix="tqt", dir=d)
    return tempfile.mkdtemp(prefix="tqt")


def env_path():
    """This Python's dir first (child python3 is the one running tests), then the system dirs, then pueue's."""
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
    hit = [ln for ln in lines if ln.startswith("SKIP:")]
    return (hit[-1][5:] if hit else lines[-1] if lines else "no reason given")


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


def log_name(t):
    return f"log_{t.replace('/', '__')}.txt"


def run_one(t, i, a, out, base, env):
    """-> (test, result, seconds, count, note)"""
    if not a.gpu and needs_gpu(t):
        return t, SKIPPED, 0, 0, "needs a real GPU (only with --gpu)"
    home, tmp = os.path.join(base, str(i), "h"), os.path.join(base, str(i), "t")
    made = make_home(home)
    os.makedirs(tmp)
    cmd = [sys.executable, t] if t.endswith(".py") else ["bash", t]
    log = os.path.join(out, log_name(t))
    s = time.time()
    with open(log, "w") as lf:
        p = subprocess.Popen(cmd, cwd=CORE, env=dict(env, HOME=home, TMPDIR=tmp), stdout=lf, stderr=subprocess.STDOUT,
                             stdin=subprocess.DEVNULL, start_new_session=True)
        try:
            rc = p.wait(a.timeout)
        except subprocess.TimeoutExpired:
            rc = None
        stop_group(p)  # stop the whole group on timeout, and any background processes a finished test left behind
    sec = int(time.time() - s)
    with open(log, encoding="utf-8", errors="replace") as lf:
        text = lf.read()
    ran = re.search(r"^Ran (\d+) tests?\b", text, re.M)  # unittest tests
    n = sum(ln.lstrip().startswith("PASS:") for ln in text.splitlines()) + (int(ran.group(1)) if ran else 0)
    wrote = sorted(set(os.listdir(home)) - made)
    note = ("wrote HOME: " + " ".join(wrote)) if wrote else ""
    if rc == SKIP:
        return t, SKIPPED, sec, n, skip_reason(log)
    res = PASS if rc == 0 else f"timeout ({a.timeout}s)" if rc is None else f"failed ({rc})"
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
    head = ("test", "result", "sec", "n", "note")
    cells = [head] + [tuple(map(str, r)) for r in rows]

    def width(s):  # wide characters take two cells
        return sum(2 if ord(c) > 0x2e7f else 1 for c in s)
    w = [max(width(r[k]) for r in cells) for k in range(4)]
    return "\n".join(("  ".join(r[k] + " " * (w[k] - width(r[k])) for k in range(4)) + "  " + r[4]).rstrip() for r in cells)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog="\n\n".join(__doc__.split("\n\n")[2:]))
    ap.add_argument("tests", nargs="*", metavar="FILE or PATTERN", help="only run these (default: all)")
    ap.add_argument("-j", "--jobs", type=int, default=min(4, os.cpu_count() or 1), help="files at a time (default 4, at most the CPU count)")
    ap.add_argument("--out", default=OUT, help=f"where results and logs go (default {OUT})")
    ap.add_argument("--timeout", type=int, default=900, help="max seconds per file (default 900)")
    ap.add_argument("--gpu", action="store_true", help="also run tests that need a real GPU (# needs-gpu)")
    ap.add_argument("-l", "--list", action="store_true", help="only list what would run")
    ap.add_argument("--keep", action="store_true", help="keep the temp HOMEs and TMPDIRs (default: remove)")
    a = ap.parse_args(argv)
    tests = pick(discover(), a.tests)
    if not tests:
        print(f"run_tests: no matching tests: {' '.join(a.tests)}", file=sys.stderr)
        return 2
    if a.list:
        print("\n".join(t + ("\tneeds-gpu" if needs_gpu(t) else "") for t in tests))
        return 0
    out = os.path.abspath(os.path.expanduser(a.out))
    os.makedirs(out, exist_ok=True)
    have = {log_name(t) for t in discover()}
    for f in os.listdir(out):  # logs left by tests that no longer exist
        if f.startswith("log_") and f.endswith(".txt") and f not in have:
            os.remove(os.path.join(out, f))
    rep = os.path.join(out, "report.tsv")
    last = last_seconds(rep)
    base = short_base()
    env = {"PATH": env_path(), "LANG": "C.UTF-8"}
    order = sorted(tests, key=lambda t: (-last.get(t, 0), t))
    rows, t0 = {}, time.time()
    print(f"run_tests: {len(tests)} files, {max(1, a.jobs)} at a time; logs in {out}", file=sys.stderr)
    try:
        with ThreadPoolExecutor(max(1, a.jobs)) as ex:
            futs = [ex.submit(run_one, t, i, a, out, base, env) for i, t in enumerate(order)]
            for fut in as_completed(futs):
                r = fut.result()
                rows[r[0]] = r
                print(f"  {r[1]}  {r[0]} ({r[2]}s)" + (f": {r[4]}" if r[1] == SKIPPED else ""), file=sys.stderr, flush=True)
    finally:
        if not a.keep:
            shutil.rmtree(base, ignore_errors=True)
    done = [rows[t] for t in tests]
    with open(rep, "w", encoding="utf-8") as f:
        f.write("test\tresult\tsec\tn\tnote\n")
        f.writelines("\t".join(map(str, r)) + "\n" for r in done)
    bad = [r for r in done if r[1] not in (PASS, SKIPPED)]
    skipped = sum(r[1] == SKIPPED for r in done)
    print(table(done))
    print(f"\n{len(done)} files: {len(done) - len(bad) - skipped} pass, {skipped} skip, {len(bad)} fail; "
          f"{int(time.time() - t0)}s; report {rep}" + (f"; temp files {base}" if a.keep else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
