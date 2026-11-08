"""The READMEs match the code: README.md's command table matches bin/taskq's TREE; the field tables between markers match what
queue/schema.py generates; the files in the "which files to test after a change" table exist and every test file is listed; core
paths mentioned in the READMEs exist. Reads only the core's own files; runs sites/mtab.py fields --markdown with the fixed config in
queue/testdata/.
"""
import glob
import importlib.machinery
import importlib.util
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
import schema  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


def read(rel):
    with open(os.path.join(CORE, rel), encoding="utf-8") as f:
        return f.read()


def between(text, start, end):
    """The text between start ... end (one per pair)."""
    return re.findall(re.escape(start) + r"\n(.*?)\n" + re.escape(end), text, re.S)


def cli():
    """Load bin/taskq as a module (the file has no .py)."""
    loader = importlib.machinery.SourceFileLoader("taskq_cli", os.path.join(CORE, "bin", "taskq"))
    spec = importlib.util.spec_from_loader("taskq_cli", loader)
    m = importlib.util.module_from_spec(spec)
    loader.exec_module(m)
    return m


READMES = ("README.md", "queue/README.md")
TEXT = {r: read(r) for r in READMES}
REGEN = "bin/taskq --project example machines fields --markdown {}"

# ---------------- command table ----------------
m = cli()
want = set()
for name, node in m.TREE.items():
    if isinstance(node, m.Group):
        want |= {f"{name} {sub}" for sub in node.cmds}
    else:
        want.add(name)
blocks = between(TEXT["README.md"], "<!-- commands -->", "<!-- /commands -->")
ok("README.md has one command table (<!-- commands --> ... <!-- /commands -->)", len(blocks) == 1)
rows = re.findall(r"^\| `taskq ([^`]+)` \|", blocks[0] if blocks else "", re.M)
have = set(rows)
ok(f"the command table has every command and subcommand in TREE (hidden groups too), missing: {sorted(want - have)}", not want - have)
ok(f"the command table has no command TREE does not, extra: {sorted(have - want)}", not have - want)
ok("every command has one row", len(rows) == len(have))
hidden = [n for n, c in m.TREE.items() if isinstance(c, m.Group) and c.hidden]
ok(f"hidden groups ({', '.join(hidden)}) are listed in the command table too", hidden and all(any(r.startswith(h + " ") for r in rows) for h in hidden))

# ---------------- field tables ----------------
seen = set()
for r in READMES:
    for name, body in re.findall(r"<!-- fields:(\w+) -->\n(.*?)\n<!-- /fields -->", TEXT[r], re.S):
        seen.add(name)
        if name not in schema.TABLES:
            ok(f"field table {name} in {r} is in schema.TABLES", False)
            continue
        ok(f"field table {name} in {r} matches what schema.py generates" + ("" if body == schema.markdown(name) else
           f" (differs: regenerate with {REGEN.format(name)} and paste between the markers)"), body == schema.markdown(name))
ok(f"all seven schema.py tables are pasted in the READMEs, missing: {sorted(set(schema.TABLES) - seen)}", set(schema.TABLES) <= seen)
ok("schema.TABLES is seven field tables", len(schema.TABLES) == 7 and all(isinstance(getattr(schema, t), dict) for t in schema.TABLES))
ok("markdown: | in descriptions escaped, required written as 'required'",
   "\\|" in schema.markdown("POLICY_FIELDS") and "| `AUTO` | `auto` | required |" in schema.markdown("POLICY_FIELDS"))

env = dict(os.environ, TASKQ_CONFIG=fixture.CONFIG, TASKQ_PLUGINS=fixture.PLUGINS)
mtab = os.path.join(CORE, "sites", "mtab.py")
r = subprocess.run([sys.executable, mtab, "fields", "--markdown", "POLICY_FIELDS"], capture_output=True, text=True, env=env, timeout=60)
ok("mtab.py fields --markdown <table>: schema.markdown output with markers",
   r.returncode == 0 and r.stdout.strip() == f"<!-- fields:POLICY_FIELDS -->\n{schema.markdown('POLICY_FIELDS')}\n<!-- /fields -->")
r = subprocess.run([sys.executable, mtab, "fields", "--markdown"], capture_output=True, text=True, env=env, timeout=60)
ok("mtab.py fields --markdown: all seven tables", r.returncode == 0 and all(f"<!-- fields:{t} -->" in r.stdout for t in schema.TABLES))
r = subprocess.run([sys.executable, mtab, "fields", "--markdown", "NO_SUCH"], capture_output=True, text=True, env=env, timeout=60)
ok("mtab.py fields --markdown with an unknown table -> exit 2", r.returncode == 2)

# ---------------- "which files to test after a change" ----------------
blocks = between(TEXT["README.md"], "<!-- tests -->", "<!-- /tests -->")
ok("README.md has one test table (<!-- tests --> ... <!-- /tests -->)", len(blocks) == 1)
table = blocks[0] if blocks else ""
tests = {os.path.basename(p) for d in ("queue", "sites", "sync", "monitor") for p in glob.glob(os.path.join(CORE, d, "test_*"))
         if p.endswith((".py", ".sh"))}
named = set(re.findall(r"`(test_\w+\.(?:py|sh))`", table))
ok(f"every test file in the test table exists, not found: {sorted(named - tests)}", not named - tests)
ok(f"every test file appears in the test table at least once, unlisted: {sorted(tests - named)}", not tests - named)
srcs = [p for p in re.findall(r"`([\w./-]+)`", table) if "/" in p]
ok(f"every source file and dir in the test table exists, not found: {[p for p in srcs if not os.path.exists(os.path.join(CORE, p))]}",
   srcs and all(os.path.exists(os.path.join(CORE, p)) for p in srcs))

# ---------------- paths mentioned in the READMEs ----------------
PATH_RE = re.compile(r"`((?:bin|queue|sites|sync|monitor|example)/[\w./-]*)`")
for r in READMES:
    paths = sorted(set(PATH_RE.findall(TEXT[r])))
    gone = [p for p in paths if not os.path.exists(os.path.join(CORE, p))]
    ok(f"all {len(paths)} core paths mentioned in {r} exist, not found: {gone}", paths and not gone)

# ---------------- no history or dates ----------------
for r in READMES:
    dates = re.findall(r"\b\d{1,2}/\d{1,2}\b|\b20\d\d-\d\d-\d\d\b", TEXT[r])
    ok(f"{r} has no dates, found: {dates[:5]}", not dates)
    old = [w for w in ("used to be", "old way", "still works") if w in TEXT[r]]
    ok(f"{r} has no history (used to be, old way, still works), found: {old}", not old)

sys.exit(1 if fails else 0)
