"""No core file mentions a domain word (a specific model, trainer, dataset), and no core file outside tests uses the user's paths or
repo; non-test files carry no dates (comments say what is, not when it changed)."""
import os
import re
import subprocess
import sys

CORE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # scheduler code root
ROOT = CORE  # paths in the list are relative to it

# Check the whole core (files tracked by git and new text files not committed yet); only this test itself contains these words
CLEAN = sorted(f for f in subprocess.run(["git", "-C", CORE, "ls-files", "-co", "--exclude-standard"], capture_output=True, text=True,
                                         check=True).stdout.split()
               if f != "queue/test_core_clean.py" and os.path.isfile(os.path.join(CORE, f)) and not f.endswith((".png", ".pyc")))
# the last two groups: the user's run record format (runlog's .run.json) and the layout of nnU-Net result dirs (<trainer>__<plans>__<config>)
WORDS = re.compile(r"nnunet|nnU-?Net|BRAIN_TUMOR|\bfold|RQ[0-9]|Dataset[0-9]|trainer|research/|segment|lesion|tumou?r|\bMRI\b"
                   r"|\brunlog\b|\.run\.json|__<?\{?plans\}?>?__", re.I)

FIXTURE = "queue/testdata"
ALLOW = {"install.sh": "Xiangyu-Research/taskq", "README.md": "Xiangyu-Research/taskq"}  # the core's own repo URL

bad = []
for f in CLEAN:
    for i, line in enumerate(open(os.path.join(ROOT, f), encoding="utf-8", errors="replace"), 1):
        line = line.replace(ALLOW[f], "") if f in ALLOW else line
        m = WORDS.search(line)
        if m:
            bad.append(f"{f}:{i}: {m.group(0)}")
print("\n".join(bad) if bad else f"PASS:{len(CLEAN)} files have no domain words")

# the core (outside testdata) does not use the user's paths or repo directly
HOST = re.compile(r"scripts/monitor|Xiangyu-Research/PhD|\.cache/phd|/work/PhD")
seen = {}
for f in CLEAN:
    if f.startswith(FIXTURE + "/"):
        continue
    n = sum(len(HOST.findall(line)) for line in open(os.path.join(ROOT, f), encoding="utf-8", errors="replace"))
    if n:
        seen[f] = n
print("\n".join(f"uses the user's path: {p}: {n} places" for p, n in sorted(seen.items())) or "PASS:the core has no user paths")

# non-test core files (outside test_* and testdata) carry no dates: marks such as "(11/4)", "since 10/31" or "2026-11-04" only say
# when something changed; comments and docs say what is and why. Month/day only matches 1-12 / 1-31 not adjacent to letters, digits or / . : -
# (paths, versions and ratios do not count)
DATE = re.compile(r"(?<![\w/.:-])(?:1[0-2]|[1-9])/(?:3[01]|[12][0-9]|[1-9])(?![\w/.])|(?<![0-9])20[0-9]{2}-[01][0-9]-[0-3][0-9](?![0-9])"
                  r"|[0-9]{1,2}\u6708[0-9]{1,2}\u65e5|20[0-9]{2} ?\u5e74")
DATE_OK = {}  # dates a feature needs (format docs, config example values): file -> allowed text, like ALLOW above
dated = []
for f in CLEAN:
    if os.path.basename(f).startswith("test_") or f.startswith(FIXTURE + "/"):
        continue
    for i, line in enumerate(open(os.path.join(ROOT, f), encoding="utf-8", errors="replace"), 1):
        line = line.replace(DATE_OK[f], "") if f in DATE_OK else line
        m = DATE.search(line)
        if m:
            dated.append(f"{f}:{i}: {m.group(0)}")
print("\n".join(f"has a date: {h}" for h in dated) or "PASS:non-test core files have no dates")
sys.exit(1 if bad or seen or dated else 0)
