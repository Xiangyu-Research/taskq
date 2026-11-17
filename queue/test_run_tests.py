#!/usr/bin/env python3
"""Tests for run_tests.py (taskq self test): small test files in a temp dir run through run_one: pass with PASS lines and
unittest's count, exit 77 = skip with its SKIP: reason, a passing file's SKIP: lines in its note, failure, timeout (the file's
process group is stopped, its background child too), needs-gpu, files written to HOME; the environment holds only PATH, LANG,
HOME, TMPDIR and TQTEST_* settings; discover and patterns.
    python3 queue/test_run_tests.py
"""
import argparse
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

sys.path.insert(0, HERE)
import run_tests as RT  # noqa: E402

T = fixture.tmpdir(prefix="test_run_tests_")
OUT, BASE = os.path.join(T, "out"), os.path.join(T, "base")
os.makedirs(OUT)
os.makedirs(BASE)
fails = 0
n = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def run(name, text, gpu=False, timeout=60, env=None):
    """Write a test file and run it through run_tests.run_one -> (result, count, note, log text)."""
    global n
    n += 1
    p = os.path.join(T, name)
    with open(p, "w") as f:
        f.write(text)
    a = argparse.Namespace(gpu=gpu, timeout=timeout)
    t, res, _sec, cnt, note = RT.run_one(p, n, a, OUT, BASE, env or RT.child_env())
    log = os.path.join(OUT, RT.log_name(p))
    with open(log, encoding="utf-8") if os.path.exists(log) else open(os.devnull) as f:  # a file skipped without running has no log
        return res, cnt, note, f.read()


res, cnt, note, _ = run("test_a.py", 'print("PASS:one")\nprint("  PASS:two")\nprint("Ran 3 tests in 0.1s")\n')
ok("a passing file: PASS, count = PASS: lines + unittest's Ran N tests, no note", (res, cnt, note) == (RT.PASS, 5, ""))
res, cnt, note, _ = run("test_b.sh", 'echo "PASS:x"\necho "SKIP:no frob here"\necho "SKIP: no rsync on PATH"\necho "SKIP:no frob here"\n')
ok("a passing file that left out some checks: PASS, its SKIP: reasons (once each) in the note",
   (res, cnt) == (RT.PASS, 1) and note == "skipped part: no frob here | no rsync on PATH")
res, cnt, note, _ = run("test_c.sh", 'echo "PASS:x"\necho "SKIP:no rsync on PATH (copies with it)"\nexit 77\n')
ok("exit 77: SKIP with the reason from its SKIP: line", (res, note) == (RT.SKIPPED, "no rsync on PATH (copies with it)"))
res, cnt, note, _ = run("test_d.py", "import sys\nprint('FAIL:x')\nsys.exit(3)\n")
ok("a failing file: failed (exit code)", res == "failed (3)")
res, _, note, _ = run("test_e.sh", "# needs-gpu\necho PASS:x\n")
ok("# needs-gpu without --gpu: skipped without running", res == RT.SKIPPED and "GPU" in note)
res, _, _, _ = run("test_e2.sh", "# needs-gpu\necho PASS:x\n", gpu=True)
ok("# needs-gpu with --gpu: runs", res == RT.PASS)
pidf = os.path.join(T, "child.pid")
t0 = time.time()
res, _, _, _ = run("test_f.sh", f"sleep 300 &\necho $! > {pidf}\nwait\n", timeout=2)
took = time.time() - t0
pid = int(open(pidf).read())
time.sleep(0.5)
try:
    os.kill(pid, 0)
    alive = not open(f"/proc/{pid}/stat").read().split()[2] == "Z"
except (ProcessLookupError, FileNotFoundError):
    alive = False
ok("timeout: result says so, the file's background child is stopped with it", res == "timeout (2s)" and took < 20 and not alive)
res, _, note, _ = run("test_g.sh", 'touch "$HOME/left_here"\necho PASS:x\n')
ok("a file that writes to HOME: noted", res == RT.PASS and "wrote HOME: left_here" in note)

os.environ.update(TQTEST_SOMETHING="v 1", TASKQ_CONFIG="/should/not/pass", SOME_OTHER="x")
env = RT.child_env()
_, _, _, log = run("test_h.py", "import json, os\nprint(json.dumps(sorted(os.environ)))\nprint(os.environ.get('TQTEST_SOMETHING'))\n", env=env)
seen = json.loads(log.splitlines()[0])
ok("environment: only PATH, LANG, HOME, TMPDIR and TQTEST_* settings (no TASKQ_*, nothing else from the caller)",
   set(seen) - {"PWD", "SHLVL", "_"} == {"PATH", "LANG", "HOME", "TMPDIR", "TQTEST_SOMETHING"} and log.splitlines()[1] == "v 1")

found = RT.discover()
ok("discover: test files under queue/, sites/, sync/ and monitor/, this one included",
   "queue/test_run_tests.py" in found and all(f.split("/")[0] in RT.DIRS for f in found))
ok("patterns: part of a path, a glob, a file name", RT.pick(found, ["run_tests"]) == ["queue/test_run_tests.py"]
   and RT.pick(found, ["queue/test_run_t*.py"]) == ["queue/test_run_tests.py"] and RT.pick(found, ["test_run_tests.py"]) == ["queue/test_run_tests.py"])

sys.exit(1 if fails else 0)
