"""pueue 类执行机（SITES 里 scheduler="pueue"，如 lab1、lab0）的适配器（2026-10-31）：读组里的任务、开常驻 worker。
和 slurm.py 同一组函数：status(name) → dict，jobs(name, st) → worker 任务，ensure_workers(name, want, dry) → 动作列表。
逻辑照搬 scripts/queue/relay.py 的 STATUS_PY（pueue 那一支）、worker_jobs()、topup() 和 submit()（pueue 那一支）；
"要几个"由调用方算（want）。"""
import json
import shlex

import remote as R


def _rename(s):
    import relay_bundle as RB
    return RB.rename(s)


def status(name):
    """→ dict(sched="pueue", jobs=[[任务号, 标签, 状态, 结果或 None], …]（只要 worker 所在的组）)；读不到 → jobs=None。"""
    c = R.conf(name)
    group = (c.get("worker") or {}).get("group")
    rc, out, _ = R.rsh(name, f"{c['pueue']} status --json", timeout=60)
    if rc:
        return dict(sched="pueue", jobs=None, unreachable=R.unreachable(rc))
    try:
        tasks = json.loads(out)["tasks"]
    except (ValueError, KeyError):
        return dict(sched="pueue", jobs=None, unreachable=False)
    jobs = []
    for i, t in sorted(tasks.items(), key=lambda x: int(x[0])):
        s = t["status"]
        st = s if isinstance(s, str) else list(s)[0]
        done = s.get("Done") if isinstance(s, dict) else None
        res = done.get("result") if isinstance(done, dict) else None
        if t.get("group") == group:
            jobs.append([int(i), t.get("label") or "", st, str(res) if res is not None else None])
    return dict(sched="pueue", jobs=jobs, unreachable=False)


def jobs(name, st=None):
    """组里的 worker 任务（标签 worker:<id>），按任务号排。"""
    st = st if st is not None else status(name)
    w = R.conf(name)["worker"]
    return [j for j in st.get("jobs") or [] if j[1] == f"worker:{w['id']}"]


def worker_cmd(name):
    """开 worker 的 pueue add 命令。整条 bash -c 作为一个参数：pueue 把 -- 后面的参数用空格拼起来交给 sh（ccb0ba6）。"""
    c = R.conf(name)
    w, gpu = c["worker"], c["groups"][c["worker"]["group"]][1]
    run = (f"R={R.q(c['root'])}; REPO=$(readlink -f $R/code/current); export TASKQ_DIR=$R/queue WORKER_ID={w['id']} "
           f"CUDA_VISIBLE_DEVICES={gpu}; exec bash $REPO/scripts/queue/{_rename('worker_start.sh')}")
    return f"{c['pueue']} add -g {w['group']} -l worker:{w['id']} -- {shlex.quote('bash -c ' + shlex.quote(run))}"


def ensure_workers(name, want, dry=False, st=None):
    """组里活着的 worker 补到 want 个（一台 pueue 执行机一个 worker id，want 超过 1 按 1 算）。
    上一个 worker 失败了（不是 Success / Killed）不重开，返回 blocked。→ [dict(op=add|blocked|skip, …, ok)]。"""
    st = st if st is not None else status(name)
    if st.get("jobs") is None:
        return [dict(op="skip", why="读不到 pueue", ok=False)]
    mine = jobs(name, st)
    alive = sum(1 for j in mine if j[2] != "Done")
    if min(want, 1) - alive <= 0:
        return []
    if mine and mine[-1][3] not in ("Success", "Killed"):
        return [dict(op="blocked", why=f"上一个 worker（pueue 任务 {mine[-1][0]}）失败了（{mine[-1][3]}）", ok=False)]
    cmd = worker_cmd(name)
    if dry:
        return [dict(op="add", cmd=cmd, ok=None)]
    rc, out, err = R.rsh(name, cmd)
    return [dict(op="add", cmd=cmd, ok=rc == 0, out=(out + err).strip()[-200:])]
