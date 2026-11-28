#!/usr/bin/env python3
"""The protocol is defined only in proto.py: no other core file hard-codes proto names; every TASKQ_* is registered in proto.ENV and
every worker / task env var (WORKER_*, TASK_*, RUN_TASK_*) in proto.RUN_ENV; proto uses the standard library only and runs standalone."""
import ast
import os
import re
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import proto  # noqa: E402

SKIP_DIRS = {"testdata", "tests", "__pycache__"}


def read(p):
    with open(p, encoding="utf-8") as f:
        return f.read()


def core_files():
    for d, ds, fs in os.walk(CORE):
        ds[:] = [x for x in ds if x not in SKIP_DIRS and not x.startswith(".")]
        for f in fs:
            if f.startswith("test_") or f.endswith((".md", ".pyc")) or f == "proto.py":
                continue
            p = os.path.join(d, f)
            if f.endswith((".py", ".sh", ".sbatch")) or f in ("q", "qs"):
                yield p


def code_lines(p):
    """Lines after removing comments and docstrings: (line number, text)."""
    src = read(p)
    if not p.endswith(".py"):
        for i, ln in enumerate(src.splitlines(), 1):
            s = ln.lstrip()
            if not s.startswith("#"):
                yield i, re.sub(r"\s#\s.*$", "", ln)
        return
    doc = set()
    try:
        tree = ast.parse(src)
    except SyntaxError:
        tree = None
    for n in ast.walk(tree) if tree else ():
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)) and n.body and \
                isinstance(n.body[0], ast.Expr) and isinstance(getattr(n.body[0], "value", None), ast.Constant):
            doc.update(range(n.body[0].lineno, n.body[0].end_lineno + 1))
    for i, ln in enumerate(src.splitlines(), 1):
        if i in doc or ln.lstrip().startswith("#"):
            continue
        yield i, re.sub(r"\s#\s.*$", "", ln)


# proto names -> patterns that are violations when written elsewhere
LITERALS = {
    "taskq_stage_": r"taskq_stage_",
    "/tmp/gpu_pad_": r"/tmp/gpu_pad_",
    "taskq_gpu_pad_": r"taskq_gpu_pad_",
    "@@JSON": r"@@JSON",
    "gpu_pad_DISABLED": r"gpu_pad_DISABLED",
    "view.lock": r"view\.lock",
    "state/pins": r"""state/pins|['"]state['"],\s*['"]pins['"]""",  # reading it as a proto.ROOT key (sub(R, "pins")) does not count
    ".incomplete": r"\.incomplete",
    "MANIFEST.json": r"MANIFEST\.json",
    "code/current": r"code/current",
    ".relay_in": r"\.relay_in",
    "gpu_log.pid": r"gpu_log\.pid",
    # dir names written by hand when joining paths (lookups such as Q["claims"] and task state values such as "stopped" do not count)
    "stopped/": r"""stopped/|/stopped\b|join\([^)]*(?<!\[)['"]stopped['"]""",
    "claims/": r"""claims/|/claims\b|join\([^)]*(?<!\[)['"]claims['"]""",
    # env build states (proto.ENV_STATE; "ok" is too common and "building" is also a marker suffix)
    "check failed": r"""['"]check failed['"]""",
    "no space": r"""['"]no space['"]""",
    "building (may have stopped)": r"""['"]building \(may have stopped\)['"]""",
    "spec changed": r"""['"]spec changed['"]""",
    "not built": r"""['"]not built['"]""",
    "not spec-built": r"""['"]not spec-built['"]""",
    # env build marker files and their lines (proto.ENV_MARK, ENV_*_FMT, ENV_NOSPACE); the queue lock's owner file (proto.LOCK_OWNER)
    "env marker suffix": r"""\$\w+\.(ok|failed|building)\b|['"]\.(ok|failed|building)['"]""",
    "nospace": r"""(?<![\[\w])['"]nospace['"]|\bnospace %""",
    "lock owner": r"""join\([^)]*['"]owner['"]""",
    # time windows and status sets with one definition (proto.PAUSE_WINDOW / proto.hours, proto.PUEUE_LIVE)
    "3 hours": r"\b3 hours\b",
    "pueue live statuses": r"""['"]Running['"],\s*['"]Queued['"],\s*['"]Stashed['"]""",
}
# places the migration has not reached yet: (file relative to CORE, name). Only shrinks
KNOWN = set()


class ProtoAlone(unittest.TestCase):
    def test_stdlib_only(self):
        tree = ast.parse(read(os.path.join(HERE, "proto.py")))
        mods = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        mods |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        self.assertLessEqual(mods, set(sys.stdlib_module_names))

    def test_sh_matches_python(self):
        out = subprocess.run([sys.executable, os.path.join(HERE, "proto.py"), "sh"], capture_output=True, text=True, check=True).stdout
        names = list(proto.sh_vars())
        script = out + "\n" + "\n".join(f'printf "%s\\0" "${{{n}}}"' for n in names)
        got = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True).stdout.split("\0")[:-1]
        self.assertEqual(got, [str(v) for v in proto.sh_vars().values()])

    def test_safe(self):
        self.assertEqual(proto.safe("D/T/0"), "D__T__0")
        self.assertEqual(proto.blob("abcdef"), "blobs/ab/abcdef")

    def test_claim_alive(self):
        now = 10_000.0
        self.assertTrue(proto.claim_alive(None, now - 60, now))
        self.assertFalse(proto.claim_alive(None, now - proto.HALF_CLAIM - 1, now))
        self.assertTrue(proto.claim_alive({"assigned": "site-pueue", "beat": 0}, 0, now))
        self.assertTrue(proto.claim_alive({"beat": now - 60}, 0, now))
        self.assertFalse(proto.claim_alive({"beat": now - proto.STALE_BEAT - 1}, 0, now))
        self.assertTrue(proto.claim_alive({"beat": 0}, 0, now, log_age=60))
        self.assertFalse(proto.claim_alive({"beat": 0}, 0, now, log_age=proto.FRESH_LOG + 1))

    def test_env_marker_lines(self):
        """classify tells the two .failed forms apart by the third word; .building and .failed start with hash and time."""
        h = "ab" * 32
        self.assertEqual((proto.ENV_NOSPACE_FMT % (h, 7, 900, 100)).split()[:3], [h, "7", proto.ENV_NOSPACE])
        self.assertNotEqual((proto.ENV_FAILED_FMT % (h, 7, 1)).split()[2], proto.ENV_NOSPACE)
        self.assertEqual((proto.ENV_BUILDING_FMT % (h, 7)).split(), [h, "7"])
        sfx = list(proto.ENV_MARK.values())
        self.assertTrue(len(set(sfx)) == len(sfx) and all(s.startswith(".") for s in sfx), sfx)
        self.assertLess(proto.ENV_BUILD_MIN * 60, proto.ENV_BUILD_TIMEOUT)

    def test_lock_times(self):
        """A holder that is working touches its lock long before anyone may take it over."""
        self.assertLess(proto.LOCK_KEEP * 2, proto.LOCK_DEAD)
        self.assertLessEqual(proto.LOCK_DEAD, proto.LOCK_REMOTE)
        self.assertLessEqual(proto.LOCK_REMOTE, proto.LOCK_MAX)

    def test_part_log_lines(self):
        ts = "2026-11-01 10:00:00"
        self.assertEqual(re.fullmatch(proto.LOG_ENDED, f"{ts} " + proto.LOG_ENDED_FMT % (3, "rest")).groups(), (ts, "3", "rest"))
        self.assertEqual(re.fullmatch(proto.LOG_NOT_STARTED, f"{ts} " + proto.LOG_NOT_STARTED_FMT % "why").groups(), (ts, "why"))
        self.assertIsNotNone(re.fullmatch(proto.LOG_GPU_BUSY, f"{ts} " + proto.LOG_GPU_BUSY_FMT % (0, "")))
        self.assertEqual(re.fullmatch(proto.LOG_STALLED, f"{proto.STALL_LINE} {ts}: log").groups(), (ts, "log"))
        self.assertEqual(re.fullmatch(proto.LOG_LINE_TS, f"{ts} text").groups(), (ts, "text"))


class NoCopies(unittest.TestCase):
    def test_env_registered(self):
        seen = {}
        for p in core_files():
            for v in re.findall(r"TASKQ_[A-Z][A-Z_]*[A-Z]", read(p)):
                seen.setdefault(v, os.path.relpath(p, CORE))
        prefixes = {"TASKQ_GPU_PAD"}  # gpu_pad.py builds parameter names from the prefix
        missing = {v: f for v, f in seen.items() if v not in proto.ENV and v not in prefixes}
        self.assertEqual(missing, {}, "TASKQ_* must be registered in proto.ENV")

    def test_alert_types_registered(self):
        """Every problem id written as alert_id("<type>", ...) has its type in proto.ALERTS."""
        seen = {}
        for p in core_files():
            for i, ln in code_lines(p):
                for k in re.findall(r"""alert_id\(\s*['"]([A-Za-z_]+)['"]""", ln):
                    seen.setdefault(k, f"{os.path.relpath(p, CORE)}:{i}")
        self.assertIn("revive", seen)  # the scan finds them
        self.assertEqual({k: f for k, f in seen.items() if k not in proto.ALERTS}, {}, "alert types must be in proto.ALERTS")

    def test_status_doc(self):
        """taskq status --help describes every --json field."""
        doc = {k for _, d in proto.STATUS_DOC for k in d}
        want = set(proto.STATUS_TOP + proto.TASK_FIELDS + proto.STATUS_FIELDS + proto.OVERVIEW_TOP + proto.OVERVIEW_TASK + proto.OVERVIEW_TASK_QUEUE)
        self.assertEqual(want - doc, set())
        self.assertEqual(doc - want, set())

    def test_alert_rules(self):
        """ALERT_RULES only names alert types; a cause is filled from the id's parts and the data, else it is the id."""
        self.assertEqual(set(proto.ALERT_RULES) - set(proto.ALERTS), set())
        self.assertEqual(proto.alert_parts("pueue:box:12:2026-11-14T17:24:27+08:00"), dict(machine="box", id="12", end="2026-11-14T17:24:27+08:00"))
        self.assertEqual(proto.alert_parts("site:lab:fail"), dict(machine="lab", sub="fail"))
        self.assertEqual(proto.alert_cause("tqroom:a/b", dict(key="a/b", places=["lab", "dicc1"])), "tqroom:dicc1,lab")
        self.assertEqual(proto.alert_cause("site:lab:orphan:a/b"), "site:lab:orphan")
        self.assertEqual(proto.alert_cause("tqroom:a/b", dict(key="a/b")), "tqroom:a/b")
        self.assertEqual(proto.alert_cause("disk:box"), "disk:box")
        self.assertFalse(proto.alert_push("notice:x"))
        self.assertTrue(proto.alert_push("disk:box"))

    def test_run_env_registered(self):
        """Env vars of workers and tasks: every WORKER_* / TASK_* / RUN_TASK_* name in the core is in proto.RUN_ENV, or a shell
        variable spec.py shell prints (proto.SHELL_VARS), or the name of a proto / schema constant (TASK_LOGS, WORKER_FIELDS, ...);
        queue/README.md lists every proto.RUN_ENV name."""
        import schema
        known = set(proto.RUN_VARS.values()) | set(proto.SHELL_VARS.values())
        seen = {}
        for p in core_files():
            for v in re.findall(r"\b(?:TASK|WORKER|RUN_TASK)_[A-Z][A-Z0-9_]*\b", read(p)):
                seen.setdefault(v, os.path.relpath(p, CORE))
        missing = {v: f for v, f in seen.items() if v not in known and not hasattr(proto, v) and not hasattr(schema, v)}
        self.assertEqual(missing, {}, "worker and task env vars must be registered in proto.RUN_ENV (spec.py shell's in proto.SHELL_VARS)")
        self.assertLessEqual({"WORKER_ID", "TASK_KEY", "TASK_JOBTAG", "TASK_INPUTS_ROOT", "TASK_OUT"}, set(seen), "the scan finds them")
        self.assertTrue(all(len(v) == 3 and v[1] in ("run", "test") and v[2] for v in proto.RUN_ENV.values()))
        readme = read(os.path.join(HERE, "README.md"))
        self.assertEqual([n for n in proto.RUN_VARS.values() if f"`{n}`" not in readme], [], "queue/README.md lists every proto.RUN_ENV name")

    def test_no_handwritten_literals(self):
        bad = []
        for p in core_files():
            rel = os.path.relpath(p, CORE)
            for i, ln in code_lines(p):
                for name, pat in LITERALS.items():
                    if (rel, name) not in KNOWN and re.search(pat, ln):
                        bad.append(f"{rel}:{i} {name}: {ln.strip()[:100]}")
        self.assertEqual(bad, [], "these names are defined only in proto.py")

    def test_known_still_needed(self):
        left = set()
        for p in core_files():
            rel = os.path.relpath(p, CORE)
            for _, ln in code_lines(p):
                for name, pat in LITERALS.items():
                    if (rel, name) in KNOWN and re.search(pat, ln):
                        left.add((rel, name))
        self.assertEqual(KNOWN - left, set(), "remove fixed entries from KNOWN")


if __name__ == "__main__":
    unittest.main()
