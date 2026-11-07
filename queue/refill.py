#!/usr/bin/env python3
"""派任务和补作业的规则：queue/relay.py 每轮替所有执行机派任务时调用。
  - 派任务：choose（总队列里没人领的任务按先后，一个个挑一台执行机）。能去的执行机是 places（policy.py 的 PLACE 和说明的
    machines 取交集，顺序是偏好）；在里面先挑有进度的那台（holders），再挑输入已经在那边的，都没有就按偏好；要那台
    有这种 worker 的空位、环境就绪、放得下（Site.fit：输入 + 产出 out_gb 后盘至少还剩 budget 的第二项）、没跳过过它、
    代码包和冒烟闸门不挡。每台执行机这一轮的余量是一个 Site（relay.py 按那边的状态建）；
  - Slurm 补作业：slurm_plan（GPU worker 按 policy.py 的 SLURM[机器]["gpu"]：QOS → (每段时限, 留几个)，
    排着和在跑的不到上限（sacctmgr 的 MaxSubmitPU，没设时按 sites/slurm.py 的 SUBMIT_CAP）减去留给别的作业的名额时，
    每个空位交一个 GPU worker 作业；纯 CPU 任务（device=cpu）有 SLURM[机器]["cpu"] 时交 CPU worker 作业，
    不超过 min(这类任务数, 最多几个)，作业领不到任务就结束）。读作业数、交作业经 slurm.py；
  - hub 上的任务：add（先记任务记录再用 q 加）、then_check（做完马上跑一次健康检查）。
"""
import os
import re
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
from proto import S_CLAIMED, UNCLAIMED  # noqa: E402
cfg.use_config()
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import slurm as SL  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402

NOROOM = "放不下"  # Site.fit 空间不够时的原因以它开头（choose 据此分出 noroom）

_ENTRY = cfg.outputs("entry")  # 主循环的入口（相对 hub 的 repo；policy.py 的 OUTPUTS）
KICK = _ENTRY and (_ENTRY if _ENTRY.startswith(("/", "~")) else M.MACHINES[M.hub()]["repo"] + "/" + _ENTRY)  # kick 和定时的共用一把锁，最多等 5 分钟
LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")


def info(tq, key):
    """队列里的任务 → dict(recipe, params, spec)（spec 是 taskq.py 解析好的；解析不了的 spec = {}）。"""
    t = next((t for t in (tq or {}).get("tasks", []) if t["key"] == key), {})
    return dict(recipe=t.get("recipe"), params=t.get("params") or {}, spec=t.get("spec") or {})


def kind_of(sp):
    return sp.get("device") or "gpu"


def out_gb(sp):
    """一个任务的产出要留的 GB：说明的 out_gb，没写 = proto.OUT_GB。"""
    v = sp.get("out_gb")
    return float(v) if v is not None else float(proto.OUT_GB)


def places(recipe, sp):
    """能去的执行机，按偏好排：PLACE[步骤名或训练的做法]（没写、"any" = 有这种 worker 的执行机，按机器表的顺序），
    再和说明的 machines 取交集（不限 = None）。"""
    have = [n for n in M.executors() if kind_of(sp) in M.kinds(n)]
    p = (P.PLACE or {}).get(sp.get("step") or recipe, "any")
    out = have if p == "any" else [n for n in p if n in have]
    lim = sp.get("machines")
    return [n for n in out if lim is None or n in lim]


def progress(s, key, host=None):
    """store 上收集到的这个任务有没有训练进度（训完、有 epoch，或者有存档：任务说明的 resume）：host=None 时看所有收集到的。
    这个任务的文件读不动（收集时记进 stuck，存储挂住）或读出错（errors）的当成有进度：不当新任务分出去。"""
    for h, d in (s.get("src") or {}).items():
        if host not in (None, h):
            continue
        if key in (d.get("stuck") or []) or key in (d.get("errors") or {}):
            return True
        f = (d.get("tasks") or {}).get(key)
        if f and (f.get("final") or f.get("resumable") or any((r.get("epochs_done") or 0) > 0 for r in f.get("runs") or [])):
            return True
    return False


def site_progress(f):
    """执行机上一个任务的情况（relay.site_view / status.json 的 tasks[key]）→ 有没有进度：训完、有做完的轮数、有存档（说明的 resume），
    或者那边说做了一半（ran：跑过、交还、没失败，taskq.has_progress）。"""
    return bool(f) and bool(f.get("final") or f.get("resumable") or f.get("ran")
                            or any((r.get("epochs_done") or 0) > 0 for r in f.get("runs") or []))


def holders(views, usable):
    """各执行机的任务情况 {执行机: {key: site_view 的一项}} → {key: [有进度、现在能用的执行机]}（usable(名字)：是执行机、没暂停）。"""
    out = {}
    for name, tasks in views.items():
        if usable(name):
            for key, f in (tasks or {}).items():
                if site_progress(f):
                    out.setdefault(key, []).append(name)
    return out


class Site:
    """一台执行机这一轮还能领多少（relay.py 按那边的状态建）。
      room {种类: 还能备几个}（档位 < 0 的不占）；ready：已经在那边的输入（相对数据目录）；envs：建好的环境；
      root、blob：(盘的编号, 剩多少 GB) —— 根目录（产出）和 blobs（输入）所在的盘，同一个盘时编号相同；同一个盘上的几台
        在 choose 里合起来扣；root 的剩余已经扣掉那边每个任务的 out_gb、加回它们已经写出的；
      keep：输入传完、产出留完后盘上至少还剩多少 GB（machines.py 的 budget 第二项）；
      block(key, 说明) → 这台这次不领它的原因或 None（代码包跑不了、冒烟闸门）；took(key, 说明)：领了以后告诉闸门；
      idle {种类: [dict(key, nice, out=它的 out_gb)：备好没人领、没进度的任务，最该让的在前]}：备满时每种交还一个，换更优先的进来。"""

    def __init__(self, name, room, ready=(), envs=(), root=(None, 0.0), blob=None, keep=0, block=None, took=None, idle=None):
        self.name, self.room, self.ready, self.envs = name, dict(room), set(ready), set(envs)
        self.root, self.blob, self.keep = root, blob or root, keep
        self.block, self._took, self.idle = block, took, {k: list(v) for k, v in (idle or {}).items()}
        self.take, self.swaps = [], []  # 这一轮领的任务；[(交还的, 换进来的)]

    def fit(self, t, sp, sizes, planned):
        """t 能不能领到这台 → None（能，并记下）或不能的原因。sizes {输入: GB 或 None}；planned {盘: 这一轮已经计划占的 GB}；
        交还的任务留的产出空间加回。"""
        k = kind_of(sp)
        if k not in self.room:
            return f"没有 {k} 的 worker"
        envs = S.envs_of(sp)
        if not envs or not set(envs) <= self.envs:
            return f"环境 {'、'.join(sorted(set(envs) - self.envs)) or '（没写）'} 没就绪"
        swap = None
        if t["nice"] >= 0 and self.room[k] <= 0:
            swap = next((x for x in self.idle.get(k, []) if x["nice"] > t["nice"]), None)
            if swap is None:
                return "排满了"
        send = [p for p in sp.get("inputs") or [] if p not in self.ready]
        if any(sizes.get(p) is None for p in send):
            return f"store 上没有输入 {next(p for p in send if sizes.get(p) is None)}"
        gb, out = sum(sizes[p] for p in send), out_gb(sp)
        back = swap["out"] if swap else 0.0
        same = self.blob[0] == self.root[0]
        left = self.root[1] - planned.get(self.root[0], 0.0) + back - out - (gb if same else 0.0)
        bleft = left if same else self.blob[1] - planned.get(self.blob[0], 0.0) - gb
        if min(left, bleft) < self.keep:
            return (f"{NOROOM}（输入 {gb:.0f} GB、产出留 {out:.0f} GB 后剩 {min(left, bleft) + self.keep:.0f} GB，"
                    f"要留 {self.keep:.0f} GB）")
        why = self.block(t["key"], sp) if self.block else None
        if why:
            return why
        planned[self.root[0]] = planned.get(self.root[0], 0.0) - back + out + (gb if same else 0.0)
        if not same:
            planned[self.blob[0]] = planned.get(self.blob[0], 0.0) + gb
        if swap:
            self.idle[k] = []  # 每种每轮只换一个
            self.swaps.append((swap, t))
        elif t["nice"] >= 0:
            self.room[k] -= 1
        self.ready |= set(send)
        self.take.append(t)
        if self._took:
            self._took(t["key"], sp)
        return None


def choose(tasks, sites, specs, held=None, sizes=None):
    """总队列里能派的任务（没人领、解析得了、插件接受）→ 每个挑一台执行机，记进 sites[名].take / swaps。
    specs {key: (做法, 说明)}；held：holders 的结果；sizes：{输入: GB 或 None}（store 上量的，data_sizes）。
    先后：档位，同档里做了一半的先，再按进队列的先后。→ dict(why={key: 这轮没派出去的原因}, nowhere=[能去的执行机都跳过了它的 key],
    noroom=[这一轮能派的执行机上都放不下的 key]（不是排满、等一等就能派，要腾地方或改 out_gb）)。"""
    held, sizes, planned, why, nowhere, noroom = held or {}, sizes or {}, {}, {}, [], []
    for t in sorted(tasks, key=lambda t: (t["nice"], t["key"] not in held, t["order"])):
        rec, sp = specs[t["key"]]
        opts = places(rec, sp)
        if not opts:
            why[t["key"]] = f"没有能去的执行机（PLACE、machines 和 {kind_of(sp)} worker 的交集是空的）"
            continue
        skipped = set(t.get("skipped_by") or [])
        if set(opts) <= skipped:
            nowhere.append(t["key"])
            continue
        opts = [n for n in opts if n not in skipped]
        hs = held.get(t["key"])
        if hs:  # 做了一半的只给有进度的那台（它暂停、不再是执行机时 holders 里就没有它，放开给别的从头做）
            opts = [n for n in opts if n in hs]
        live = [n for n in opts if n in sites]
        if not live:
            why[t["key"]] = f"能去的 {'、'.join(opts) or '、'.join(hs or [])} 这一轮都不能派（连不上或没开）"
            continue
        data = sp.get("inputs") or []
        live.sort(key=lambda n: not all(p in sites[n].ready for p in data))  # 输入已经在那边的先；其余照偏好
        fails = []
        for n in live:
            r = sites[n].fit(t, sp, sizes, planned)
            if r is None:
                break
            fails.append(f"{n}：{r}")
        else:
            why[t["key"]] = "；".join(fails)
            if all(f.split("：", 1)[1].startswith(NOROOM) for f in fails):
                noroom.append(t["key"])
    return dict(why=why, nowhere=nowhere, noroom=noroom)


def data_sizes(paths, probe):
    """输入（相对数据目录，可以带通配）在 store 上多大 → {路径: GB 或 None（不在、通配一个都对不上）}。
    probe(pats) → store 上 site_ops 的 probe 结果（带 size；软链接按内容算、硬链接一份，和传过去后一样）。"""
    paths = sorted(set(paths))
    if not paths:
        return {}
    got = probe(paths)
    return {p: (got[p]["size"] / 1e9 if got.get(p) and got[p]["n"] and got[p]["size"] is not None else None) for p in paths}


# ---------------- 执行 ----------------
def sh(cmd, host=None, timeout=300):
    """在 hub（host=None）或别的机器上跑一条 shell 命令 → (退出码, 输出)。"""
    rc, out, err = C.run(cmd, host, timeout)
    return rc, (out + err).strip()


def q(name, args, run=sh):
    """在某台 pueue 机器上用 queue/q 加任务 → 任务号。"""
    m = M.MACHINES[name]
    qp = shlex.quote(cfg.local("queue", "q")) if m["ssh"] is None else cfg.rel("queue", "q")  # 本机：核心的绝对路径
    rc, out = run(f"cd {m['repo']} && {qp} {args}", m["ssh"])
    # 带 -p 时只输出任务号，不带时是"New task added (id 118)."（两种都认）
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
    """核心自己的 pueue 任务（传数据、拉回、归档）的命令 → 做完（成功）后马上跑一次健康检查（不等下一轮，和定时的共用一把锁）；
    这一次里这个任务算做完（health.py 看环境变量 TASKQ_TASK_DONE）。检查出错不算这个任务失败。没有入口（OUTPUTS["entry"]）时原样。"""
    return f"{cmd}{KICK_SEP}{shlex.quote(label)} bash {KICK} kick || true; }}" if KICK else cmd


KICK_SEP = " && { TASKQ_TASK_DONE="


# ---------------- Slurm 补作业 ----------------
def unclaimed(tq, device):
    """队列里没人领（free / stale）、device 是这个的任务数。"""
    return sum(1 for t in tq["tasks"] if t["state"] in UNCLAIMED and info(tq, t["key"])["spec"].get("device", "gpu") == device)


def training(tq):
    """队列里有人领着（在训）、要卡的任务数。"""
    return sum(1 for t in tq["tasks"] if t["state"] == S_CLAIMED and info(tq, t["key"])["spec"].get("device", "gpu") == "gpu")


def topup_plan(tq, counts, qos):
    """→ ([(QOS, 每段时限, 补几段)], 不补的原因)。counts：{QOS: (排队上限或 None, 现有段数, 其中还在排的段数)}。
    这台有要卡的任务（备好没人领的，或在训的）时按各 QOS 的名额排满，让排着的作业一直攒排队时间。
    同时能跑几个由集群的 QOS 管，多排的作业等在跑的结束才开始，那时那边已备到 READY 个；开始时没有任务的 worker
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
    out = SL.distribute(qos, counts, sum((counts[q][0] or SL.SUBMIT_CAP) for q in qos), "gpu")
    return out, "" if out else "各 QOS 都排满了"


def cpu_plan(tq, have, conf, free=None):
    """纯 CPU 任务的 worker 作业 → ([(QOS, 时限, 交几个, "cpu")], 不交的原因)。have：我们排着和在跑的 CPU worker 作业数；
    free：还剩几个提交名额（None = 读不到，不交）。"""
    q, t, most = conf
    if not tq or not tq.get("enabled") or tq.get("paused"):
        return [], "任务队列关着或暂停中"
    if have is None or free is None:
        return [], "读不到作业数"
    if not unclaimed(tq, "cpu"):
        return [], "没有没人领的纯 CPU 任务"
    n = min(min(unclaimed(tq, "cpu"), most) - have, free)
    return ([(q, t, n, "cpu")], "") if n > 0 else ([], f"已有 {have} 个" if free > 0 else "提交名额用完了")


def slurm_plan(tq, st, conf):
    """Slurm 机器要补交的 worker 作业 → ([(QOS, 时限, 交几个, worker 的种类)], 不交的原因)。st：sites/slurm.py 的 status；
    conf：policy.py 的 SLURM[机器]（有 gpu 补 GPU worker，有 cpu 补 CPU worker）。"""
    subs, why = topup_plan(tq, st["counts"], conf["gpu"]) if conf.get("gpu") else ([], "")
    if conf.get("cpu"):
        csub, cwhy = cpu_plan(tq, st["cpu"], conf["cpu"], st["free"])
        subs += csub
        why = why if csub else "；".join(x for x in (why, f"CPU：{cwhy}") if x)
    return subs, why
