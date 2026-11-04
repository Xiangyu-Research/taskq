"""pueue 类执行机（SITES 里 scheduler="pueue"，如 lab1、lab0）的适配器（2026-10-31）：读 worker 所在组的任务、开常驻 worker。
和 slurm.py 对应：读状态 → 调用方定开不开（scripts/taskq/queue/relay.py 的 topup）→ start_worker 开。
pueue 的原始输出由那边的状态脚本一次读回（relay_remote.py 的 STATUS_PY），这里只解析。"""
import json
import os
import shlex
import sys

import remote as R

sys.path.insert(0, os.path.join(R.CORE, "queue"))
import cfg  # noqa: E402
cfg.use_config()
import machines as M  # noqa: E402
from proto import ROOT  # noqa: E402


def parse(raw, group):
    """`pueue status --json` 的输出（文本或已解析的 dict）→ [[任务号, 标签, 状态, 结果或 None], …]（只要这个组的，group 可以是几个组的列表）；
    读不出 → None。"""
    groups = [group] if isinstance(group, str) or group is None else list(group)
    try:
        tasks = (json.loads(raw) if isinstance(raw, str) else raw)["tasks"]
    except (TypeError, ValueError, KeyError):
        return None
    jobs = []
    for i, t in sorted(tasks.items(), key=lambda x: int(x[0])):
        s = t["status"]
        st = s if isinstance(s, str) else list(s)[0]
        done = s.get("Done") if isinstance(s, dict) else None
        res = done.get("result") if isinstance(done, dict) else None
        if t.get("group") in groups:
            jobs.append([int(i), t.get("label") or "", st, str(res) if res is not None else None])
    return jobs


def workers(name, jobs, kind="gpu"):
    """跑 kind 任务的 worker（标签 worker:<id>，envs/machines.py 的 worker_of），按任务号排。"""
    w = M.worker_of(name, kind)
    return [j for j in jobs or [] if j[1] == f"worker:{w['id']}"]



def busy_cmd():
    """排着的 worker 开始时才解析 code/current，不占着旧包：没有要留的。"""
    return "true"

def worker_cmd(name, kind="gpu"):
    """开 worker 的 pueue add 命令。整条 bash -c 作为一个参数：pueue 把 -- 后面的参数用空格拼起来交给 sh（ccb0ba6）。
    前面加 exec：pueue 的子进程就是 worker 本身。不加时 sh 收到 pueue kill 的 SIGTERM 先退出，pueue 马上记为结束，
    worker 还在停训练、交还任务（11/3）。"""
    c, w = R.conf(name), M.worker_of(name, kind)
    gpu = c["groups"][w["group"]][1]
    env = f"WORKER_ID={w['id']} WORKER_DEVICE={kind} CUDA_VISIBLE_DEVICES={gpu or ''}" + (f" TASKQ_CPUS={w['cpus']}" if w.get("cpus") else "")
    env += "".join(f" {k}={shlex.quote(str(v))}" for k, v in (c["groups"][w["group"]][2] or {}).items())  # 组的环境变量（如 TASK_GPU_IDLE）
    run = (f'R={R.q(c["root"].rstrip("/"))}; REPO=$(readlink -f "$R/{ROOT["current"]}"); export TASKQ_DIR="$R/{ROOT["queue"]}" {env}; '
           f'exec bash "$REPO/{cfg.rel("queue", "worker_start.sh")}"')
    return f"{c['pueue']} add -g {w['group']} -l worker:{w['id']} -- {shlex.quote('exec bash -c ' + shlex.quote(run))}"


def start_worker(name, run, kind="gpu"):
    """开一个跑 kind 任务的 worker → (成功与否, 输出)。run：fn(命令) → (退出码, 输出)。"""
    rc, out = run(worker_cmd(name, kind))
    return rc == 0, out.strip()[-200:]
