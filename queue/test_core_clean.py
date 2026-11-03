"""调度核心里已经没有领域词的文件，保持没有（剥离的准备，11/2）。清单和其余文件的情况见 SPLIT.md。"""
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROOT = REPO  # 清单里的路径相对它

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
    "scripts/queue/test_feed.py", "scripts/queue/test_common.py", "scripts/queue/test_shift.py", "scripts/sites/test_adapters.py",
    "scripts/sites/test_e2e.py", "scripts/sites/test_envs.py", "scripts/sync/test_tunnel.py", "scripts/queue/test_archive_task.sh",
    "scripts/queue/test_races.py", "scripts/monitor/test_probe.py",
]
WORDS = re.compile(r"nnunet|nnU-?Net|BRAIN_TUMOR|\bfold|RQ[0-9]|Dataset[0-9]|trainer|research/|segment|lesion|tumou?r|\bMRI\b",
                   re.I)

# 测试用的固定配置（testdata/）整个查；PLACE 的地方 "trainer"（训练所在的执行机）是核心的写法
FIXTURE = "scripts/queue/testdata"
FIXED = sorted(os.path.relpath(os.path.join(d, f), ROOT) for d, ds, fs in os.walk(os.path.join(ROOT, FIXTURE))
               if "__pycache__" not in d for f in fs if not f.endswith(".pyc"))
if not FIXED:
    sys.exit(f"{FIXTURE} 下没有文件")
CLEAN += FIXED
ALLOW = {f"{FIXTURE}/config/policy.py": '"trainer"'}

bad = []
for f in CLEAN:
    for i, line in enumerate(open(os.path.join(ROOT, f), encoding="utf-8", errors="replace"), 1):
        line = line.replace(ALLOW[f], "") if f in ALLOW else line
        m = WORDS.search(line)
        if m:
            bad.append(f"{f}:{i}: {m.group(0)}")
print("\n".join(bad) if bad else f"{len(CLEAN)} 个文件都没有领域词")
sys.exit(1 if bad else 0)
