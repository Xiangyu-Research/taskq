"""Slurm 类执行机（SITES 里 scheduler="slurm"，如 dicc2、dicc1）的适配器（2026-10-31）：读作业、补交 worker 作业。
和 pueue.py 同一组函数：status(name) → dict，jobs(name, st) → worker 作业，ensure_workers(name, want, dry) → 动作列表。
逻辑照搬 scripts/queue/refill.py 的 topup_probe()、topup_plan()（各 QOS 的上限、留给本人的名额）和 scripts/queue/relay.py 的
submit()（slurm 那一支）；"要几个"由调用方算（want = 排着和在跑的 worker 作业一共要几个）。作业照旧交根目录代码包里的
worker.sbatch，它转交 worker_start.sh（开显存填充 gpu_pad，gpu-a100 要求 >= 40GB；中转执行机的开关是 R/queue/gpu_pad_DISABLED）。"""
import remote as R

SUBMIT_CAP = 10  # 同 refill.SUBMIT_CAP：读不到 QOS 的排队上限时按这个算


def _policy(name):
    import policy as P
    return (P.SLURM.get(name) or {}).get("gpu") or {}


def probe(name):
    """每个 GPU QOS 一行"QOS|名字|排队上限|这个 QOS 下我们的作业数"，再加我们的每个作业"JOB|号|名字|状态|已跑|QOS|原因"。"""
    qs = " ".join(_policy(name))
    return ("set -e; for q in " + qs + "; do lim=$(sacctmgr -nP show qos $q format=maxsubmitpu); "
            "ids=$(squeue -u $USER -h -r -q $q -o %i); echo \"QOS|$q|$lim|$(printf '%s' \"$ids\" | grep -c . || true)\"; done; "
            "squeue -u $USER -h -o 'JOB|%i|%j|%T|%M|%q|%R'")


def status(name):
    """→ dict(sched="slurm", counts={QOS: (上限或 None, 作业数)}, squeue=[[号, 名字, 状态, 已跑, QOS, 原因], …])；读不到 → None。"""
    rc, out, _ = R.rsh(name, probe(name), timeout=120)
    if rc:
        return dict(sched="slurm", counts=None, squeue=None, unreachable=R.unreachable(rc))
    counts, sq = {}, []
    for ln in out.splitlines():
        p = ln.split("|")
        if len(p) == 4 and p[0] == "QOS" and p[3].isdigit():
            counts[p[1]] = (int(p[2]) if p[2].isdigit() else None, int(p[3]))
        elif len(p) == 7 and p[0] == "JOB":
            sq.append(p[1:])
    return dict(sched="slurm", counts=counts, squeue=sq, unreachable=False)


def jobs(name, st=None):
    """我们的 GPU worker 作业（作业名 = 条目的 jobs.gpu），排着和在跑的。"""
    st = st if st is not None else status(name)
    jn = (R.conf(name).get("jobs") or {}).get("gpu")
    return [j for j in st.get("squeue") or [] if j[1] == jn]


def sbatch_cmd(name, qos, limit):
    return (f"cd {R.q(R.conf(name)['root'])}/code/current/scripts/queue && "
            f"sbatch --parsable --qos={qos} --time={limit} worker.sbatch")


def plan(name, want, st):
    """→ [(QOS, 时限, 交几个)]：want 减去已有的 worker 作业，按 envs/policy.py SLURM 里 QOS 的顺序、各 QOS 的上限（减去留给本人的）分。"""
    need = want - len(jobs(name, st))
    out = []
    for qos, v in _policy(name).items():
        if need <= 0:
            break
        limit, keep = (v, 0) if isinstance(v, str) else v
        if qos not in st["counts"]:
            continue
        lim, n = st["counts"][qos]
        k = min(max((lim or SUBMIT_CAP) - keep - n, 0), need)
        if k:
            out.append((qos, limit, k))
            need -= k
    return out


def ensure_workers(name, want, dry=False, st=None):
    """排着和在跑的 worker 作业补到 want 个。→ [dict(op=sbatch|skip, qos, cmd, ok, job/out)]。"""
    st = st if st is not None else status(name)
    if st.get("squeue") is None or st.get("counts") is None:
        return [dict(op="skip", why="读不到作业数", ok=False)]
    if not _policy(name):
        return [dict(op="skip", why="envs/policy.py 的 SLURM 里没有它", ok=False)]
    acts = []
    for qos, limit, k in plan(name, want, st):
        cmd = sbatch_cmd(name, qos, limit)
        for _ in range(k):
            if dry:
                acts.append(dict(op="sbatch", qos=qos, cmd=cmd, ok=None))
                continue
            rc, out, err = R.rsh(name, cmd)
            if rc:
                acts.append(dict(op="sbatch", qos=qos, cmd=cmd, ok=False, out=(out + err).strip()[-200:]))
                break
            acts.append(dict(op="sbatch", qos=qos, cmd=cmd, ok=True, job=out.strip().splitlines()[-1]))
    return acts
