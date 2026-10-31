"""pueue 类执行机（SITES 里 scheduler="pueue"，如 lab1、lab0）的适配器（2026-10-31）：读 worker 所在组的任务、开常驻 worker。
和 slurm.py 对应：读状态 → 调用方按"先备好再开卡"定开不开（scripts/queue/relay.py 的 topup）→ start_worker 开。
pueue 的原始输出由那边的状态脚本一次读回（relay.py 的 STATUS_PY），这里只解析。"""
import json
import shlex

import remote as R


def _rename(s):
    import relay_bundle as RB
    return RB.rename(s)


def parse(raw, group):
    """`pueue status --json` 的输出（文本或已解析的 dict）→ [[任务号, 标签, 状态, 结果或 None], …]（只要这个组的）；读不出 → None。"""
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
        if t.get("group") == group:
            jobs.append([int(i), t.get("label") or "", st, str(res) if res is not None else None])
    return jobs


def workers(name, jobs):
    """组里的 worker 任务（标签 worker:<id>），按任务号排。"""
    w = R.conf(name)["worker"]
    return [j for j in jobs or [] if j[1] == f"worker:{w['id']}"]


def worker_cmd(name):
    """开 worker 的 pueue add 命令。整条 bash -c 作为一个参数：pueue 把 -- 后面的参数用空格拼起来交给 sh（ccb0ba6）。"""
    c = R.conf(name)
    w, gpu = c["worker"], c["groups"][c["worker"]["group"]][1]
    run = (f"R={R.q(c['root'].rstrip('/'))}; REPO=$(readlink -f $R/code/current); export TASKQ_DIR=$R/queue WORKER_ID={w['id']} "
           f"CUDA_VISIBLE_DEVICES={gpu}; exec bash $REPO/scripts/queue/{_rename('worker_start.sh')}")
    return f"{c['pueue']} add -g {w['group']} -l worker:{w['id']} -- {shlex.quote('bash -c ' + shlex.quote(run))}"


def start_worker(name, run):
    """开一个 worker → (成功与否, 输出)。run：fn(命令) → (退出码, 输出)。"""
    rc, out = run(worker_cmd(name))
    return rc == 0, out.strip()[-200:]
