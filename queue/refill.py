#!/usr/bin/env python3
"""relay 用的挑任务和补作业的规则（2026-10-18）：queue/relay.py 替执行机领任务时调用。
  - 挑任务：candidates（能领的、按先后排）、next_pick（下一个领哪个、要传哪些数据）、progress（哪台机器上有进度）、data_gb；
  - Slurm 补作业：slurm_plan（GPU worker 按 policy.py 的 SLURM[机器]["gpu"]：QOS → (每段时限, 留几个)，
    排着和在跑的不到上限（sacctmgr 的 MaxSubmitPU，没设时按 sites/slurm.py 的 SUBMIT_CAP）减去留给别的作业的名额时，
    每个空位交一个 queue/worker.sbatch；纯 CPU 任务（device=cpu）有 SLURM[机器]["cpu"] 时交 cpu_worker.sbatch，
    不超过 min(这类任务数, 最多几个)，作业领不到任务就结束）。读作业数、交作业经 slurm.py；
  - hub 上的任务：add（先记任务记录再用 q 加）、then_check（做完马上跑一次健康检查）。
"""
import glob
import os
import re
import shlex
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
from proto import S_FREE, S_CLAIMED, UNCLAIMED  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "monitor"))  # health.py（这里的函数里才 import，避免互相 import）
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import slurm as SL  # noqa: E402
import experiments as E  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402

TRAIN_OUT_GB = 10  # 领之前除了数据集和盘的提醒线，再给训练输出（存档、验证结果）留的空间
_ENTRY = cfg.outputs("entry")  # 主循环的入口（相对 hub 的 repo；policy.py 的 OUTPUTS，11/4）
KICK = _ENTRY and (_ENTRY if _ENTRY.startswith(("/", "~")) else M.MACHINES[M.hub()]["repo"] + "/" + _ENTRY)  # kick 和定时的共用一把锁，最多等 5 分钟
LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")


def too_big(m, q):
    """实验表一步（queue = 它的 QUEUE_FIELDS）在执行机 m 上放不下 → 原因；放得下或 m 没有上限 → None。
    cap 管根目录；blobs 在另一个盘、m 写了 blob_cap 且这一步写了 out_disk 时，产出 out_disk 对 cap、输入（disk − out_disk）
    对 blob_cap（11/6）；否则 disk 整个对 cap。两边都要留出 budget 的第二项。"""
    c = M.RELAY.get(m) or {}
    q = q or {}
    gb, out, cap, bcap = q.get("disk"), q.get("out_disk"), c.get("cap"), c.get("blob_cap")
    hard = (c.get("budget") or (0, 0))[1]
    if not gb:
        return None
    if bcap and out is not None:
        if cap and out + hard > cap:
            return f"产出要 {out} GB，{m} 的根目录最多放 {cap} GB"
        return f"输入要 {gb - out} GB，{m} 的 blobs 最多放 {bcap} GB" if gb - out + hard > bcap else None
    return f"要 {gb} GB，{m} 最多放 {cap} GB" if cap and gb + hard > cap else None


def info(tq, key):
    """队列里的任务 → dict(recipe, params, spec)；队列状态里没有说明时从实验表找。"""
    t = next((t for t in (tq or {}).get("tasks", []) if t["key"] == key), {})
    if t.get("spec"):
        return dict(recipe=t.get("recipe"), params=t.get("params") or {}, spec=t["spec"])
    e = E.tasks().get(key)
    return dict(recipe=e["recipe"], params=e["params"], spec=e["spec"]) if e else dict(recipe=None, params={}, spec={})


def tag(path):
    """数据路径 → 传数据任务的标签后缀（最后一级目录名）。"""
    return os.path.basename(path.rstrip("/"))


def progress(s, key, host=None):
    """这个任务有没有训练进度（训完、有 epoch，或者有存档：任务说明的 resume）：host=None 时看所有机器。
    那边这个任务的文件读不动（收集时记进 stuck，存储挂住）或读出错（errors）的当成有进度：不当新任务分出去、不当没进度交还（11/1）。"""
    from health import task_info
    if any(key in (d.get("stuck") or []) or key in (d.get("errors") or {}) for h, d in (s.get("src") or {}).items() if host in (None, h)):
        return True
    if any(v[0] or v[1] > 0 for h, v in task_info(s, key).items() if host in (None, h)):
        return True
    return any(((d.get("tasks") or {}).get(key) or {}).get("resumable") for h, d in (s.get("src") or {}).items()
               if host in (None, h))


def site_progress(f):
    """执行机上一个任务的情况（relay.site_view / status.json 的 tasks[key]）→ 有没有进度：训完、有做完的轮数、有存档（说明的 resume），
    或者那边说做了一半（ran：跑过、交还、没失败，taskq.has_progress；11/7，旧代码包不报）。"""
    return bool(f) and bool(f.get("final") or f.get("resumable") or f.get("ran")
                            or any((r.get("epochs_done") or 0) > 0 for r in f.get("runs") or []))


def holders(views, usable):
    """各执行机的任务情况 {执行机: {key: site_view 的一项}} → {key: [有进度、现在能用的执行机]}（usable(名字)：在 RELAY、没暂停检查、AUTO 开着）。"""
    out = {}
    for name, tasks in views.items():
        if usable(name):
            for key, f in (tasks or {}).items():
                if site_progress(f):
                    out.setdefault(key, []).append(name)
    return out


def data_of(tq, key):
    return info(tq, key)["spec"].get("data") or []


def rank(tq, t, ready, held=None):
    """领的先后：（档位，做没做过一半，缺不缺数据，先后）。同一档里在某台执行机上做了一半的先领（11/7），再是数据已经在这台机器上的（马上能训）。"""
    return (t["nice"], t["key"] not in (held or {}), any(p not in ready for p in data_of(tq, t["key"])), t["order"])


def candidates(s, name, tq, ready, oks=None, kinds=("gpu",), held=None):
    """这台机器能领的任务，按领的先后排：free、没标成有问题、环境都就绪、没限定在别的机器、种类（device）在 kinds 里、
    hub 看得到的机器（MACHINES）上都没有进度、这台机器没跳过。oks：那边建好、检查过的按描述建的环境 {名: 描述的哈希}（machines.py 的 ready_envs）。
    held（holders 的结果）：在别的执行机上做了一半的只给那台（或能收编它的，如同一个盘上的两台），那台停用时才放开（11/7）。"""
    envs = M.ready_envs(name, oks)
    mine = {name} | {x["site"] for x in M.adopt_sources(name) if x["site"]}

    def theirs(key):
        hs = (held or {}).get(key)
        return bool(hs) and not mine & set(hs)

    def env_ok(sp):
        return bool(S.envs_of(sp)) and set(S.envs_of(sp)) <= envs
    cands = [t for t in tq["tasks"] if t["state"] == S_FREE and not t.get("bad") and env_ok(info(tq, t["key"])["spec"])
             and M.serves(name, info(tq, t["key"])["spec"].get("machines"))
             and info(tq, t["key"])["spec"].get("device", "gpu") in kinds
             and not progress(s, t["key"]) and name not in t.get("skipped_by", []) and not theirs(t["key"])]
    return sorted(cands, key=lambda t: rank(tq, t, ready, held))


def next_pick(tq, cands, ready, free, hard, exclude=(), moving=()):
    """下一个领哪个任务 → (任务, 要传的数据, 要传多少 GB, 有数据正在传)；没有 → (None, [], 0, False)。moving：正在传的数据集（tag）。
    要传数据的：传完后盘剩的不低于硬线才领。正在传的不再另传，领了等传完再开始（relay.py 用）。"""
    for t in cands:
        if t["key"] in exclude:
            continue
        need = [p for p in data_of(tq, t["key"]) if p not in ready]
        send = [p for p in need if tag(p) not in moving]
        if not send:
            return t, [], 0, len(need) > 0
        gb = data_gb(send)
        if gb is not None and free is not None and free - gb >= hard:
            return t, send, gb, len(need) > len(send)
    return None, [], 0, False


def data_gb(paths):
    """hub 上这些数据（相对数据目录，可以带通配符）一共多大（几个路径指到同一个文件时按几份算，和传过去后一样）；有一个不在
    （带通配符的一个也对不上）→ None。"""
    ps = []
    for p in paths:
        p = os.path.join(M.expand(M.MACHINES[M.hub()]["data"]), p)
        got = sorted(glob.glob(p)) if glob.has_magic(p) else [p] if os.path.exists(p) else []
        if not got:
            return None
        ps += got
    r = subprocess.run(["du", "-sbLlc"] + ps, capture_output=True, text=True)
    return int(r.stdout.strip().splitlines()[-1].split()[0]) / 1e9 if r.returncode == 0 else None


# ---------------- 执行 ----------------
def sh(cmd, host=None, timeout=300):
    """在 hub（host=None）或别的机器上跑一条 shell 命令 → (退出码, 输出)。"""
    rc, out, err = C.run(cmd, host, timeout)
    return rc, (out + err).strip()


def q(name, args, run=sh):
    """在某台 pueue 机器上用 queue/q 加任务 → 任务号。"""
    m = M.MACHINES[name]
    qp = shlex.quote(cfg.local("queue", "q")) if m["ssh"] is None else cfg.rel("queue", "q")  # 本机：核心的绝对路径（11/4）
    rc, out = run(f"cd {m['repo']} && {qp} {args}", m["ssh"])
    # 带 -p 时只输出任务号，不带时是"New task added (id 118)."（10/17 改成两种都认）
    hit = re.fullmatch(r"(?:New task added \(id )?(\d+)\)?\.?", out.strip().splitlines()[-1].strip()) if out.strip() else None
    if rc or not hit:
        raise RuntimeError(f"{name} 上加任务失败：{out[-200:]}")
    return int(hit.group(1))


def add(name, label, step, key, args, run=sh, **rec):
    """先记下（queue/common.py 的任务记录）、再在这台机器上加任务 → 任务号。"""
    r = C.new_record(name, label, step, key, **rec)
    tid = q(name, args, run)
    C.set_record_id(r, tid)
    return tid


def then_check(cmd, label):
    """hub 任务的命令 → 做完（成功）后马上跑一次健康检查（不等 30 分钟，和定时的共用一把锁）；这一次里这个任务算做完
    （health.py 看环境变量 TASKQ_TASK_DONE；11/6 前排的命令里叫 PHD_TASK_DONE，也认）。检查出错不算这个任务失败。没有入口（OUTPUTS["entry"]）时原样。"""
    return f"{cmd}{KICK_SEP}{shlex.quote(label)} bash {KICK} kick || true; }}" if KICK else cmd


KICK_SEP = " && { TASKQ_TASK_DONE="


# ---------------- Slurm 补作业 ----------------
GPU_FILE, CPU_FILE = SL.FILES["gpu"], SL.FILES["cpu"]


def unclaimed(tq, device):
    """队列里没人领（free / stale）、device 是这个的任务数。"""
    return sum(1 for t in tq["tasks"] if t["state"] in UNCLAIMED and info(tq, t["key"])["spec"].get("device", "gpu") == device)


def training(tq):
    """队列里有人领着（在训）、要卡的任务数。"""
    return sum(1 for t in tq["tasks"] if t["state"] == S_CLAIMED and info(tq, t["key"])["spec"].get("device", "gpu") == "gpu")


def topup_plan(tq, counts, qos):
    """→ ([(QOS, 每段时限, 补几段)], 不补的原因)。counts：{QOS: (排队上限或 None, 现有段数, 其中还在排的段数)}。
    这台有要卡的任务（备好没人领的，或在训的）时按各 QOS 的名额排满，让排着的作业一直攒排队时间（11/2 定；10/31–11/2 排着的
    不超过备好的任务数）。每人同时最多 2 张卡，多排的作业等在跑的结束才开始，那时那边已备到 READY 个；开始时没有任务的 worker
    空等 IDLE_MAX 再结束（consume.py 的 slurm 插件）。两样都没有就不交。"""
    if not tq:
        return [], "读不到任务队列"
    if not tq.get("enabled"):
        return [], "任务队列的总开关不在"
    if tq.get("paused"):
        return [], "任务队列失败太多、暂停中"
    if not unclaimed(tq, "gpu") and not training(tq):
        return [], "队列里没有要卡的任务（备好没人领的、在训的都没有）"
    if any(q not in counts for q in qos):
        return [], f"读不到 {next(q for q in qos if q not in counts)} 的作业数"
    out = SL.distribute(qos, counts, sum((counts[q][0] or SL.SUBMIT_CAP) for q in qos), GPU_FILE)
    return out, "" if out else "各 QOS 都排满了"


def cpu_plan(tq, have, conf, free=None):
    """纯 CPU 任务的 worker 作业 → ([(QOS, 时限, 交几个, CPU_FILE)], 不交的原因)。have：我们排着和在跑的 CPU worker 作业数；
    free：还剩几个提交名额（None = 读不到，不交）。"""
    q, t, most = conf
    if not tq or not tq.get("enabled") or tq.get("paused"):
        return [], "任务队列关着或暂停中"
    if have is None or free is None:
        return [], "读不到作业数"
    if not unclaimed(tq, "cpu"):
        return [], "没有没人领的纯 CPU 任务"
    n = min(min(unclaimed(tq, "cpu"), most) - have, free)
    return ([(q, t, n, CPU_FILE)], "") if n > 0 else ([], f"已有 {have} 个" if free > 0 else "提交名额用完了")


def slurm_plan(tq, st, conf):
    """Slurm 类机器要补交的 worker 作业 → ([(QOS, 时限, 交几个, sbatch 文件)], 不交的原因)。st：sites/slurm.py 的 status；
    conf：policy.py 的 SLURM[机器]（有 gpu 补 GPU worker，有 cpu 补 CPU worker）。"""
    subs, why = topup_plan(tq, st["counts"], conf["gpu"]) if conf.get("gpu") else ([], "")
    if conf.get("cpu"):
        csub, cwhy = cpu_plan(tq, st["cpu"], conf["cpu"], st["free"])
        subs += csub
        why = why if csub else "；".join(x for x in (why, f"CPU：{cwhy}") if x)
    return subs, why
