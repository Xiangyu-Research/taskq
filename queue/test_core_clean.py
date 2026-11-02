"""调度核心里已经没有领域词的文件，保持没有（剥离的准备，11/2）。清单和其余文件的情况见 SPLIT.md。"""
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CLEAN = [
    "scripts/queue/common.py", "scripts/queue/inputs.py", "scripts/queue/feed.py", "scripts/queue/refill.py",
    "scripts/queue/shift.py", "scripts/queue/relay_gc.py",
    "scripts/sites/envs.py", "scripts/sites/pueue.py", "scripts/sites/remote.py", "scripts/sites/site.py",
    "scripts/sites/slurm.py",
    "scripts/monitor/health.py", "scripts/sync/tunnel.py",
    "scripts/queue/run_task.sh", "scripts/queue/worker_start.sh", "scripts/queue/fetch_task.sh",
    "scripts/queue/archive_task.sh", "scripts/queue/offload_io.sh", "scripts/queue/gpu_pad.py",
    "scripts/queue/gpu_pad_start.sh", "scripts/queue/relay_plugins.py", "scripts/queue/relay_bundle.py",
    "scripts/queue/experiments.py", "scripts/queue/spec.py", "scripts/queue/q", "scripts/sync/snapshot.sh",
]
WORDS = re.compile(r"nnunet|nnU-?Net|BRAIN_TUMOR|\bfold|RQ[0-9]|Dataset[0-9]|trainer|research/|segment|lesion|tumou?r|\bMRI\b",
                   re.I)

bad = []
for f in CLEAN:
    for i, line in enumerate(open(os.path.join(REPO, f), encoding="utf-8"), 1):
        m = WORDS.search(line)
        if m:
            bad.append(f"{f}:{i}: {m.group(0)}")
print("\n".join(bad) if bad else f"{len(CLEAN)} 个文件都没有领域词")
sys.exit(1 if bad else 0)
