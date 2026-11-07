"""pueue 执行机（机器表里 scheduler="pueue" 的）的适配器：读 worker 所在组的任务、开常驻 worker。
和 slurm.py 对应：读状态 → 调用方定开不开（queue/relay.py 的 topup）→ start_worker 开。
pueue 的原始输出由那边的状态脚本一次读回（site_ops.py 的 status），这里只解析。"""
import json
import os
import shlex
import sys

import remote as R

sys.path.insert(0, os.path.join(R.CORE, "queue"))
import cfg  # noqa: E402
cfg.use_config()
import mtab as M  # noqa: E402
import proto  # noqa: E402
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


def workers(w, jobs):
    """worker w（mtab.workers 的一项）的 pueue 任务（标签 worker:<id>），按任务号排。"""
    return [j for j in jobs or [] if j[1] == f"{proto.WORKER_LABEL}{w['id']}"]



def worker_cmd(name, w):
    """开 worker w（mtab.workers 的一项）的 pueue add 命令。整条 bash -c 作为一个参数：pueue 把 -- 后面的参数用空格拼起来交给 sh。
    前面加 exec：pueue 的子进程就是 worker 本身（不加时 sh 收到 pueue kill 的 SIGTERM 先退出，pueue 马上记为结束，worker 还在停训练、交还任务）。
    pueue add 带上的是加任务那边的环境：先去掉 hub 自己的变量（proto.HUB_ONLY_ENV），和 hub 在同一台上的执行机的 worker
    不会把 hub 的配置、队列当成自己的。"""
    c = M.machine(name)
    env = {"TASKQ_MACHINE": name, **M.worker_env(w)}
    env = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env.items())
    run = (f'unset {" ".join(proto.HUB_ONLY_ENV)}; R={R.q(c["root"].rstrip("/"))}; REPO=$(readlink -f "$R/{ROOT["current"]}"); '
           f'export TASKQ_ROOT="$R" {env}; exec bash "$REPO/{cfg.bundle_rel("queue", "worker_start.sh")}"')
    return f"{c['pueue']} add -g {w['group']} -l {proto.WORKER_LABEL}{w['id']} -- {shlex.quote('exec bash -c ' + shlex.quote(run))}"


def start_worker(name, run, w):
    """开 worker w → (成功与否, 输出)。run：fn(命令) → (退出码, 输出)。"""
    rc, out = run(worker_cmd(name, w))
    return rc == 0, out.strip()[-200:]
