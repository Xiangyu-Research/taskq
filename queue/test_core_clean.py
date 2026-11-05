"""调度核心里已经没有领域词的文件，保持没有（剥离的准备，11/2）；核心里使用者的路径、仓库只减不增（11/4）。"""
import os
import re
import sys

CORE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 调度代码的根
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg as _cfg  # noqa: E402
REPO = _cfg.REPO  # 使用者的仓库根（cfg.py）
ROOT = CORE  # 清单里的路径相对它

CLEAN = [
    "queue/common.py", "queue/inputs.py", "queue/feed.py", "queue/refill.py",
    "queue/shift.py", "queue/relay_store.py", "queue/relay_remote.py", "queue/relay_transfer.py",
    "sites/envs.py", "sites/pueue.py", "sites/remote.py", "sites/envs_cli.py", "sites/envlock.py", "sites/test_envlock.py",
    "sites/slurm.py",
    "monitor/health.py", "monitor/notice.py", "sync/tunnel.py",
    "queue/run_task.sh", "queue/worker_start.sh",
    "queue/archive_task.sh", "queue/gpu_pad.py",
    "queue/gpu_pad_start.sh", "queue/relay_plugins.py", "queue/relay_bundle.py",
    "queue/experiments.py", "queue/spec.py", "queue/q", "sync/snapshot.sh",
    "queue/steps.py", "queue/recipes.py", "queue/taskq_api.py", "queue/bundle_gate.py", "queue/test_bundle_gate.py",
    "queue/test_feed.py", "queue/test_common.py", "queue/test_shift.py", "sites/test_adapters.py",
    "sites/test_e2e.py", "sites/test_envs.py", "sync/test_tunnel.py", "queue/test_archive_task.sh",
    "queue/test_races.py", "queue/test_worker_stall.sh", "monitor/test_probe.py", "queue/test_relay_store.py",
    "monitor/overview.py", "monitor/test_overview.py", "queue/test_experiments_cli.py", "queue/check.py", "queue/test_check.py",
    "queue/run_tests.py", "queue/test_example.py", "queue/consume.py", "queue/dicc_worker.sh", "queue/test_consume.sh",
    "queue/consumers/__init__.py", "queue/consumers/slurm.py", "queue/consumers/resident.py", "queue/consumers/gpu_pad.py",
    "queue/consumers/gpu_share.py", "queue/consumers/gpu_idle.py", "queue/consumers/window.py",
    "queue/tasklog.py", "queue/test_tasklog.py", "queue/test_pad_room.py", "monitor/gpu_log.sh", "monitor/gpu_usage.py",
    "monitor/test_gpu_usage.py",
    "queue/flow/__init__.py", "queue/flow/model.py", "queue/flow/compile.py", "queue/flow/graph.py", "queue/flow/cli.py",
    "README.md", "CLAUDE.md", "bin/taskq", "monitor/plain.py", "queue/cfg.py", "queue/cli_json.py", "queue/project.py", "queue/proto.py",
    "queue/smoke_gate.py", "queue/taskq.py", "queue/relay.py", "queue/qs", "queue/setup.sh", "queue/on_boot.sh",
    "queue/install_pueue.sh", "queue/worker.sbatch", "queue/cpu_worker.sbatch", "sites/mtab.py", "sites/names.py",
    "sync/fetch.sh", "sync/tunnel_push.sh",
    "queue/flow/ledger.py", "queue/flow/engine.py", "queue/flow/run.py", "queue/flow/stall.py", "queue/flow/publish.py", "queue/test_publish.py", "queue/test_run.py", "queue/test_flow_compile.py", "queue/test_ledger.py", "queue/test_engine.py",
]
# 后两组（11/4）：使用者的运行记录格式（runlog 的 .run.json）、nnU-Net 结果文件夹的布局（<训练器>__<plans>__<配置>）
WORDS = re.compile(r"nnunet|nnU-?Net|BRAIN_TUMOR|\bfold|RQ[0-9]|Dataset[0-9]|trainer|research/|segment|lesion|tumou?r|\bMRI\b"
                   r"|\brunlog\b|\.run\.json|__<?\{?plans\}?>?__", re.I)

# 测试用的固定配置（testdata/）整个查；PLACE 的地方 "trainer"（训练所在的执行机）是核心的写法
FIXTURE = "queue/testdata"
FIXED = sorted(os.path.relpath(os.path.join(d, f), ROOT) for d, ds, fs in os.walk(os.path.join(ROOT, FIXTURE))
               if "__pycache__" not in d for f in fs if not f.endswith(".pyc"))
if not FIXED:
    sys.exit(f"{FIXTURE} 下没有文件")
CLEAN += FIXED
# 示例项目（example/）整个查（11/4）
CLEAN += sorted(os.path.relpath(os.path.join(d, f), ROOT) for d, ds, fs in os.walk(os.path.join(ROOT, "example"))
                if "__pycache__" not in d for f in fs if not f.endswith(".pyc"))
ALLOW = {f"{FIXTURE}/config/policy.py": '"trainer"', "sync/snapshot.sh": "、runlog 照常能用"}  # 后者只是说明 git 副本的用处

bad = []
for f in CLEAN:
    for i, line in enumerate(open(os.path.join(ROOT, f), encoding="utf-8", errors="replace"), 1):
        line = line.replace(ALLOW[f], "") if f in ALLOW else line
        m = WORDS.search(line)
        if m:
            bad.append(f"{f}:{i}: {m.group(0)}")
print("\n".join(bad) if bad else f"{len(CLEAN)} 个文件都没有领域词")

# 核心（testdata 外）不直接用使用者的路径、仓库（11/4）；KNOWN 是还没改的，个数只能减少
HOST = re.compile(r"scripts/monitor|Xiangyu-Research/PhD|\.cache/phd|/work/PhD")
KNOWN = {}
seen = {}
for d, ds, fs in os.walk(ROOT):
    ds[:] = [x for x in ds if x not in ("__pycache__", ".git") and os.path.relpath(os.path.join(d, x), ROOT) != FIXTURE]
    for f in fs:
        p = os.path.relpath(os.path.join(d, f), ROOT)
        if f.endswith(".pyc") or p == "queue/test_core_clean.py":
            continue
        n = sum(len(HOST.findall(line)) for line in open(os.path.join(ROOT, p), encoding="utf-8", errors="replace"))
        if n:
            seen[p] = n
more = [f"{p}：{n} 处（已知 {KNOWN.get(p, 0)}）" for p, n in sorted(seen.items()) if n > KNOWN.get(p, 0)]
fewer = [p for p in KNOWN if seen.get(p, 0) < KNOWN[p]]
print("\n".join("用了使用者的路径：" + x for x in more) if more else f"核心里使用者的路径没有多出来（已知 {len(seen)} 个文件）")
if fewer:
    print("已经改掉的，从 KNOWN 里减掉：" + "、".join(fewer))
sys.exit(1 if bad or more else 0)
