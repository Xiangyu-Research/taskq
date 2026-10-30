#!/usr/bin/env python3
"""给训练机补活（2026-10-18）：pueue 训练机（envs/machines.py 里 scheduler=pueue、角色 train 的，现在是 lab）从 DICC 的
任务队列领任务；Slurm 训练机（DICC）排着的作业不够时补交 worker 作业（最后一节）。
由 box 上的健康检查（scripts/monitor/health.py，每 30 分钟）调用。lab 连不上 DICC，所以由 box 替它领、续、交还。

待办只有一份：DICC 上的 ~/.cache/phd/taskq/tasks.jsonl（scripts/queue/taskq.py）；谁在训哪一折只看 claims/。
每次检查，对每台这样的机器：
  1. 维护它已经领的折（领取记录 worker = 机器名；只看领取之后开始的训练任务）：
     - 训练任务还在（pueue 里 Running / Queued / Stashed / Paused / Locked），或者数据还在传 → 续上（beat）；
     - 这台机器上训完了 → 继续续着，等 box 拉回、archive_task.sh 备份到 DICC、队列显示"训完"后再交还
       （提前交还的话 DICC 作业会把它当成没训过、从头训）；这样续着的不影响领下一折；
     - 任务没了、没训完：这台机器上一点进度都没有 → 交还，回到队列；有进度 → 领取保留、报出来（存档在这台，只让它接着训）；
     - 传数据出错 → 领取保留、报出来（数据可能只传了一半，不开始训）。
     这台机器、box 或任何一台训练机连不上时什么都不动（领取 7 天内有效，过期后 DICC 才能领：box 在时停了的折马上交还，
     有效期只管 box 关机、断电的时候，设长一些免得 box 离线一天多时 DICC 把 lab 在训的折再训一遍；10/25）。
  2. 卡空了（有卡的组里没有活着的训练任务、也没有领着没训完的折）就给这个组领下一折（10/19 起每个有卡的组各领各的；
     组停着、或者是 envs/policy.py 的 MANUAL 组时不领）：队列里 free、任何机器上都没有进度、这台机器没跳过的，按（档位，先后）
     取第一个。训练没开始就失败、又没有进度（多半是这台机器放不下这个模型）时用 release --skip 交还：记在 skips/，
     这台机器以后不再领它，不算队列的失败，不用事先按模型分类。24 小时内这样跳过 2 个就先不领、报出来（多半是环境坏了）。
     taskq.py take 成功后：任务说明里的 data（scripts/queue/spec.py）这台机器上都有，就直接在 pueue 的训练组里加
     run_task.sh；缺的先用 scripts/sync/tunnel_push.sh 传（box 的 wait 组），下次检查时数据到了再加训练任务。
     传完后盘上剩不到"表里 disk 的提醒线 + 10 GB（训练输出）"（硬线）的任务跳过；这台机器没有任务要的环境（machines.py 的 envs）不领。
     只领要卡的任务（device=gpu）。数据正在传（box 上的 sync 任务活着）的文件夹不算"在"：领了等传完再开始，不另传一份。
  3. 卡都在训时为下一折做准备（2026-10-24，换折不空等）：预测下一折（和第 2 步同一个挑法）缺数据就马上提前传（每台机器同时只传一份）；
     组里在训的这一折预计 PRETAKE_S 秒内训完、下一折数据都在，就提前领下一折、加进同一个组排在它后面，训完马上开始。
     训完时间按训练日志最近 3 小时的速度算（spec.py 的 eta_at），算不出来就不提前领。
  拉回、推理、评测不在这里排：box 上的 scripts/queue/advance.py 按状态推进。
  自动传过去的数据（相对数据目录的路径）记在 ~/.cache/phd/refill.json，按"还要不要用"和空间预算管（cleanup()，2026-10-24）：
  队列里没有这台机器能领的任务要用了就删；还有任务要用的留着，盘剩的低于软线（硬线 × SOFT）时从最晚用到的删起；
  提前传之前先这样腾地方，腾不出来就不传。领着的、在训的、正在传的、拉回归档在用的、预测下一折要用的、Dataset101 不删。
  训练机自己生成的缓存（做法的 cache：nnU-Net 的解压缓存，train.sh 训练前导出）只留在训、领着的折用的（cache_cleanup()，10/26）。
开关：envs/policy.py 的 AUTO["lab"] 是 False 时只续已有的领取，其余只算、不动（"会做什么"有变化时记进 ~/refill.log，不推送）；
要停掉 lab 的训练用这个开关：只在 lab 上停掉训练任务的话，下次检查会把那一折交还队列、再领下一折。
跳过记录（skips/<key>）要撤销，删掉那个文件。

DICC 补作业：每个 QOS（envs/policy.py 的 SLURM[机器]["gpu"]：QOS → (每段时限, 留几个)）排着和在跑的作业（数组按段算）不到上限（sacctmgr 的 MaxSubmitPU，
没设时按 SUBMIT_CAP）减去留给别的作业的名额、任务队列里还有没人领的任务时，交 scripts/queue/worker.sbatch 补满（每个空位交一个
普通作业，不用数组：10/21 起 DICC 的使用率检查会在数组作业开始约 65 分钟后把它取消；同时跑几个由 QOS 的 GPU 上限管）。
空出一个补一个，作业的排队时间分从交上去开始累积。只看要卡的任务（任务说明的 device=gpu）。
纯 CPU 的任务（device=cpu，如评测，2026-10-24）：SLURM[机器] 里有 cpu 的机器，队列里有没人领的这类任务、我们的 CPU worker 作业
（排着和在跑的）不到 min(这类任务数, 最多几个) 时交 scripts/queue/cpu_worker.sbatch；作业领不到任务就结束，不在节点上空等。
开关：envs/policy.py 的 AUTO["dicc"]，False 时只把"会交什么"记进 ~/refill.log。交上去的作业记进 ~/refill.log，不推送。

    python3 scripts/queue/refill.py            看现在会做什么（不改任何东西）
"""
import json
import os
import re
import shlex
import subprocess
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "envs"))
sys.path.insert(0, os.path.join(REPO, "scripts", "monitor"))  # health.py（这里的函数里才 import，避免互相 import）
sys.path.insert(0, HERE)
import machines as M  # noqa: E402
import policy as P  # noqa: E402
import experiments as E  # noqa: E402
import spec as S  # noqa: E402
import taskq  # noqa: E402
import common as C  # noqa: E402
from common import write_json  # noqa: E402

TTL = 7 * 86400  # lab 的领取多久不续就过期（秒）
TRAIN_OUT_GB = 10  # 领之前除了数据集和盘的提醒线，再给训练输出（checkpoint、验证集预测）留的空间
SOFT = 2  # 自动传过去、留着以后用的数据和预传：盘至少剩"硬线（提醒线 + TRAIN_OUT_GB）× SOFT"
KICK = "~/auto/PhD/scripts/monitor/auto_refresh.sh"  # 健康检查（在 cron 用的克隆里跑，和定时的共用一把锁；kick 最多等 5 分钟）
PRETAKE_S = 3600  # 组里在训的这一折预计这么多秒内训完时，提前领下一折、排在它后面（两次检查的间隔，保证训完前会检查一次）
SKIP_BREAK = 2  # 24 小时内这么多个领到的任务没开始就失败，就先停止替这台机器领（多半是环境坏了）
STATE = os.path.expanduser("~/.cache/phd/refill.json")
SUBMIT_CAP = 10  # QOS 没设排队上限时，每个 QOS 最多排这么多段
LOG = os.path.expanduser("~/refill.log")
LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")


def queue_host():
    """任务队列在哪台 Slurm 机器上（角色 train 的 Slurm 机器，现在是 dicc）。"""
    return next(n for n in M.names("slurm") if "train" in M.MACHINES[n]["roles"])


def gpu_groups(name):
    """这台机器上有卡、不是只跑手动任务的 pueue 组。"""
    return [g for g, (_, gpu, _) in M.MACHINES[name]["groups"].items() if gpu is not None and g not in M.manual_groups(name)]


def gpu_group(name):
    return gpu_groups(name)[0]


def open_group(s, name, g):
    """这个组现在能不能开始训练：pueue 里组是 Running；读不到组的状态时算开着。"""
    st = ((s["pueue"].get(name) or {}).get("groups") or {}).get(g)
    return st == "Running" if st else True


def kind_key(s, name, t):
    """这台机器上的 pueue 任务 → (步骤, key)（任务记录，scripts/queue/common.py 的 identify）；认不出 → (None, None)。"""
    r = C.identify(name, t, s.get("records") or [])
    return (r["step"], r["key"]) if r else (None, None)


def task_groups(s, name):
    """这台机器 pueue 里每一折的训练任务在哪个组 → {key: 组}：有活着的任务看活着的，否则看最新的。"""
    out = {}
    for t in sorted((s["pueue"].get(name) or {}).get("tasks", []), key=lambda t: t["id"]):
        kind, key = kind_key(s, name, t)
        if kind == "train" and t.get("group") and (key not in out or t["status"] in LIVE or out[key][1] not in LIVE):
            out[key] = (t["group"], t["status"])
    return {k: g for k, (g, _) in out.items()}


def slots(name):
    """领的时候给每一折定的组（数据还要先传的折，开始训时按这个放）→ {key: 组}。"""
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    return st.get("slot", {}).get(name, {})


def set_slot(name, key, group):
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    d = st.setdefault("slot", {}).setdefault(name, {})
    if group is None:
        d.pop(key, None)
    else:
        d[key] = group
    write_json(STATE, st)


def info(tq, key):
    """队列里的任务 → dict(recipe, params, spec)；队列状态里没有说明时从实验表找。"""
    t = next((t for t in (tq or {}).get("tasks", []) if t["key"] == key), {})
    if t.get("spec"):
        return dict(recipe=t.get("recipe"), params=t.get("params") or {}, spec=t["spec"])
    e = E.tasks().get(key)
    return dict(recipe=e["recipe"], params=e["params"], spec=e["spec"]) if e else dict(recipe=None, params={}, spec={})


def run_cmd(f):
    return "bash scripts/queue/run_task.sh " + " ".join(shlex.quote(a) for a in S.kv_args(f["recipe"], f["params"]))


def tag(path):
    """数据路径 → 传数据任务的标签后缀（最后一级目录名）。"""
    return os.path.basename(path.rstrip("/"))


def progress(s, key, host=None):
    """这一折有没有训练进度（训完或有 epoch）：host=None 时看所有机器。"""
    from health import fold_info
    return any(v[0] or v[1] > 0 for h, v in fold_info(s, key).items() if host in (None, h))


def train_tasks(s, name):
    """这台机器 pueue 里的训练任务 → {key: [(任务号, 状态, 开始时间或 None)]}，按任务号排。"""
    out = {}
    for t in sorted((s["pueue"].get(name) or {}).get("tasks", []), key=lambda t: t["id"]):
        kind, key = kind_key(s, name, t)
        if kind == "train":
            out.setdefault(key, []).append((t["id"], t["status"], C.local_time(t.get("start"))))
    return out


def syncing(s, name):
    """box 上给这台机器传数据的任务，每个数据集只看最新的一个 → ({数据集: 还在传的任务}, {数据集: 出错停了的任务})。"""
    box = s["pueue"].get(M.hub()) or {}
    last = {}
    for t in sorted(box.get("tasks", []), key=lambda t: t["id"]):
        r = C.identify(M.hub(), t, s.get("records") or [])
        if r and r["step"] == "sync" and r.get("to") == name:
            last[tag(r["key"])] = t
    return ({d: t for d, t in last.items() if t["status"] in LIVE},
            {d: t for d, t in last.items() if t["status"] not in LIVE and t["status"] != "Success"})


def failed_here(s, name, tid):
    """这台机器的 pueue 任务是训练命令自己出错退出的（退出码 1–127）；被信号停掉的（≥128，如 137、143）、pueue 停的不算。"""
    t = next((t for t in (s["pueue"].get(name) or {}).get("tasks", []) if t["id"] == tid), {})
    return t.get("result") == "Failed" and 0 < (t.get("code") or 0) < 128


def plan(s, name):
    """→ (动作列表, 卡住的 {问题编号: 说明}, 不领的原因)。动作：dict(op=beat|release|start|transfer|take, key, …)。"""
    m, host = M.MACHINES[name], queue_host()
    tq = s.get("taskq", {}).get(host)
    if not s["ok"].get(name) or not s["ok"].get(M.hub()) or "error" in (s["pueue"].get(name) or {"error": 1}) \
            or any(n not in s["src"] for n in M.names("train")):
        return [], {}, "连不上"  # 有一台训练机的进度读不到时也不动：不然看不出哪一折在别处训了一半
    if not tq or not tq.get("enabled"):
        return [], {}, "读不到任务队列，或者队列没开"
    tt_all, (sync, sync_bad) = train_tasks(s, name), syncing(s, name)
    tgroup, slot = task_groups(s, name), slots(name)
    held_g = set()  # 领着没训完的折占着的组
    have = set(s["probe"].get(name, {}).get("HAVE", "").split())
    ready = {p for p in have if tag(p) not in sync and tag(p) not in sync_bad}  # 还在传、传出错的文件夹不算有
    acts, stuck = [], {}
    mine = [t for t in tq["tasks"] if t.get("worker") == name]
    for t in mine:
        key, f = t["key"], info(tq, t["key"])
        data = f["spec"].get("data") or []
        need = [p for p in data if p not in have]
        insync = [p for p in data if tag(p) in sync]
        bad = [p for p in data if tag(p) in sync_bad]  # 传了一半出错时文件夹可能已经在了，所以看全部 data
        # 只看这次领取之后开始的训练任务（以前训过、停掉的旧任务不算）；还没开始的（排着）都算
        since = datetime.fromtimestamp(t["since"] - 60) if t.get("since") else datetime.min
        tt = {key: [(i, st) for i, st, beg in tt_all.get(key, []) if beg is None or beg >= since]}
        live = [st for _, st, _ in tt_all.get(key, []) if st in LIVE]  # 活着的任务不看开始时间（恢复的旧任务也算）
        g = tgroup.get(key) or slot.get(key) or gpu_group(name)
        n_act = len(acts)
        if t["state"] == "stopped":  # taskq.py stop（10/30）：停掉这台机器上的训练（SIGTERM，从存档接着训）、交还
            ids = [i for i, st, _ in tt_all.get(key, []) if st in LIVE]
            acts.append(dict(op="release", key=key, kill=ids,
                             why="停止（taskq.py stop）" + (f"，停了训练任务 {' '.join(map(str, ids))}" if ids else "")))
            continue
        if t["state"] == "done":
            acts.append(dict(op="release", key=key, why="训完了，DICC 上有了存档"))
        elif s["src"].get(name, {}).get("folds", {}).get(key, {}).get("final"):
            # lab 训完了，但要等 box 拉回、archive_task.sh 备份到 DICC 后队列才看得到"训完"；这之前交还的话 DICC 作业会从头重训
            acts.append(dict(op="keep", key=key))
        elif live:
            acts.append(dict(op="beat", key=key))
        elif bad:  # 传出错了：数据可能只传了一半，不能当成"数据在"
            acts.append(dict(op="beat", key=key))
            b = sync_bad[tag(bad[0])]
            stuck[f"labsync:{tag(bad[0])}"] = (f"给 {M.MACHINES[name]['label']} 传 {bad[0]} 的 box 任务 {b['id']} 出错了（{b['status']}），"
                                               f"领的 {key} 在等；看 pueue log {b['id']}，处理后 pueue remove {b['id']}（下次检查会重传）")
        elif insync:
            acts.append(dict(op="beat", key=key))
        elif need and not tt[key]:
            acts.append(dict(op="beat", key=key))  # 刚领、数据还没开始传
            acts.append(dict(op="transfer", key=key, paths=need))
        elif not need and not tt[key]:
            acts.append(dict(op="beat", key=key))
            acts.append(dict(op="start", key=key, group=g))  # 数据传完了，开始训
        elif progress(s, key, name):
            last = tt[key][-1]
            acts.append(dict(op="beat", key=key))
            stuck[f"labclaim:{key}"] = (f"{M.MACHINES[name]['label']} 领的 {key} 训练任务 {last[0]} 停了（{last[1]}），没训完、有进度，"
                                        f"领取保留；接着训：q {g} --label train:{key} -- {shlex.quote(run_cmd(f))}；"
                                        f"放弃：taskq.py release --worker {name} {key}")
        elif failed_here(s, name, tt[key][-1][0]):
            # 没开始就失败（多半是这台机器放不下这个模型）：交还，记在 skips/（这台机器以后不再领它；不算队列的失败）
            acts.append(dict(op="release", key=key, skip=f"训练任务 {tt[key][-1][0]} {tt[key][-1][1]}",
                             why=f"训练任务 {tt[key][-1][0]} 失败了（{tt[key][-1][1]}），{name} 上没有进度，交还，{name} 不再领它"))
        else:
            acts.append(dict(op="release", key=key, why=f"训练任务 {tt[key][-1][0]} 停了（{tt[key][-1][1]}），{name} 上没有进度，交还队列"))
        if any(a["op"] == "beat" for a in acts[n_act:]):  # 领着、没训完的折（在训、在传数据、刚开始、停了等人处理）；keep 的不算
            held_g.add(g)
    busy = [k for k, ts in tt_all.items() if any(st in LIVE for _, st, _ in ts)]
    busy_g = {tgroup.get(k) for k in busy}
    free_g = [g for g in gpu_groups(name) if g not in busy_g and g not in held_g and open_group(s, name, g)]
    pr = s["probe"].get(name, {})
    free = int(pr["DISK"]) / 1e9 if pr.get("DISK", "").isdigit() else None
    hard = (m["disk"][1] if m["disk"] else 0) + TRAIN_OUT_GB
    cands = candidates(s, name, tq, ready)
    if not free_g:
        if (tq.get("recent_skips") or {}).get(name, 0) < SKIP_BREAK:
            acts += ahead(s, name, tq, cands, ready, free, hard, tt_all, tgroup, sync, sync_bad, acts, stuck)
        return acts, stuck, "卡上有训练任务" if busy else "还领着没训完的折" if held_g else "有卡的组都停着"
    nskip = (tq.get("recent_skips") or {}).get(name, 0)
    if nskip >= SKIP_BREAK:
        stuck[f"labskip:{name}"] = (f"{M.MACHINES[name]['label']} 24 小时内有 {nskip} 个领到的任务没开始就失败了，先不再领；"
                                    f"看 {name} 上 pueue log 里失败的训练任务（多半是环境或数据坏了，不是放不下）")
        return acts, stuck, f"24 小时内 {nskip} 个任务没开始就失败"
    took = set()
    for g in free_g:  # 每个空着的组领一折
        t, need, gb, wait = next_pick(tq, cands, ready, free, hard, took, sync)
        if t is None:
            continue
        if need:
            free -= gb
        acts.append(dict(op="take", key=t["key"], nice=t["nice"], paths=need, group=g, wait=wait))
        took.add(t["key"])
    if took:
        return acts, stuck, ""
    return acts, stuck, "队列里没有能领的折" if not cands else f"队列里的任务要的数据都放不下（{name} 盘剩 {free:.0f} GB）" if free is not None else "不知道盘还剩多少"


def data_of(tq, key):
    return info(tq, key)["spec"].get("data") or []


def rank(tq, t, ready):
    """领的先后：（档位，缺不缺数据，先后）。同一档里数据已经在这台机器上的先领（马上能训）。"""
    return (t["nice"], any(p not in ready for p in data_of(tq, t["key"])), t["order"])


def candidates(s, name, tq, ready):
    """这台机器能领的任务，按领的先后排：free、没标成有问题、有环境、没限定在别的机器、要卡、任何机器上都没有进度、这台机器没跳过。"""
    envs = set(M.machine(name).get("envs") or {})
    cands = [t for t in tq["tasks"] if t["state"] == "free" and not t.get("bad") and info(tq, t["key"])["spec"].get("env") in envs
             and name in (info(tq, t["key"])["spec"].get("machines") or [name])
             and info(tq, t["key"])["spec"].get("device", "gpu") == "gpu"
             and not progress(s, t["key"]) and name not in t.get("skipped_by", [])]
    return sorted(cands, key=lambda t: rank(tq, t, ready))


def next_pick(tq, cands, ready, free, hard, exclude=(), moving=()):
    """下一折领哪个 → (任务, 要传的数据, 要传多少 GB, 有数据正在传)；没有 → (None, [], 0, False)。moving：正在传的数据集（tag）。
    要传数据的：传完后盘剩的不低于硬线才领。正在传的不再另传，领了等传完再开始。
    领（take）、提前传（prefetch）、提前领（pretake）都用它，三者挑的是同一折。"""
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


def ahead(s, name, tq, cands, ready, free, hard, tt_all, tgroup, sync, sync_bad, acts, stuck):
    """组里都在训时为下一折做准备（2026-10-24）→ 动作：
      - 预测的下一折缺数据：马上提前传（prefetch；每台机器同时只传一份，空间预算由 cleanup() 管，放不下就不传）；
      - 组里在训的这一折预计 PRETAKE_S 秒内训完、下一折数据都在：提前领，排在它后面（pueue 一个组一次跑一个，训完马上开始）。"""
    out, took = [], {a["key"] for a in acts if a["op"] == "take"}
    moving = bool(sync) or any(a["op"] == "transfer" for a in acts)
    m = M.MACHINES[name]
    now = s.get("now") or datetime.now()
    for g in gpu_groups(name):
        if not open_group(s, name, g):
            continue
        live = [(t, kind_key(s, name, t)[1]) for t in (s["pueue"].get(name) or {}).get("tasks", [])
                if t["status"] in LIVE and t.get("group") == g and kind_key(s, name, t)[0] == "train"]
        if len(live) != 1 or live[0][0]["status"] != "Running":
            continue  # 组空着（由领的那一步管）、或者已经排了下一折
        t, need, gb, wait = next_pick(tq, cands, ready, free, hard, took, sync)
        if t is None or wait:
            continue  # 下一折的数据正在传：等传完
        if need:
            bad = [p for p in need if tag(p) in sync_bad]
            if bad:
                b = sync_bad[tag(bad[0])]
                stuck[f"labsync:{tag(bad[0])}"] = (f"给 {m['label']} 提前传 {bad[0]} 的 box 任务 {b['id']} 出错了（{b['status']}）；"
                                                   f"看 pueue log {b['id']}，处理后 pueue remove {b['id']}（下次检查会重传）")
            elif not moving:
                out.append(dict(op="prefetch", key=t["key"], paths=need, gb=gb, group=g))
                moving = True
            continue
        f = (s["src"].get(name, {}).get("folds") or {}).get(live[0][1]) or {}
        eta = S.eta_at(f.get("runs") or [], (f.get("debug") or {}).get("num_epochs"))
        if eta is not None and (eta - now).total_seconds() <= PRETAKE_S:
            out.append(dict(op="take", key=t["key"], nice=t["nice"], paths=[], group=g, after=live[0][1]))
            took.add(t["key"])
    return out


def data_gb(paths):
    """box 上这些数据（相对数据目录）一共多大（几个路径指到同一个文件时按几份算，和传过去后一样）；有一个不在 → None。"""
    ps = [os.path.join(M.expand(M.MACHINES[M.hub()]["data"]), p) for p in paths]
    if not all(os.path.exists(p) for p in ps):
        return None
    r = subprocess.run(["du", "-sbLlc"] + ps, capture_output=True, text=True)
    return int(r.stdout.strip().splitlines()[-1].split()[0]) / 1e9 if r.returncode == 0 else None


# ---------------- 执行 ----------------
def sh(cmd, host=None, timeout=300):
    """在 box（host=None）或别的机器上跑一条 shell 命令 → (退出码, 输出)。"""
    rc, out, err = C.run(cmd, host, timeout)
    return rc, (out + err).strip()


def q(name, args, run=sh):
    """在某台 pueue 机器上用 scripts/queue/q 加任务 → 任务号。"""
    m = M.MACHINES[name]
    rc, out = run(f"cd {m['repo']} && scripts/queue/q {args}", m["ssh"])
    # 带 -p 时只输出任务号，不带时是"New task added (id 118)."（10/17 改成两种都认）
    hit = re.fullmatch(r"(?:New task added \(id )?(\d+)\)?\.?", out.strip().splitlines()[-1].strip()) if out.strip() else None
    if rc or not hit:
        raise RuntimeError(f"{name} 上加任务失败：{out[-200:]}")
    return int(hit.group(1))


def add(name, label, step, key, args, run=sh, **rec):
    """先记下（scripts/queue/common.py 的任务记录）、再在这台机器上加任务 → 任务号。"""
    r = C.new_record(name, label, step, key, **rec)
    tid = q(name, args, run)
    C.set_record_id(r, tid)
    return tid


def start(name, key, run=sh, tq=None, group=None):
    g = group or gpu_group(name)
    return add(name, f"train:{key}", "train", key, f"{g} -p --label train:{key} -- {shlex.quote(run_cmd(info(tq, key)))}", run, group=g)


def then_check(cmd, label):
    """box 任务的命令 → 做完（成功）后马上跑一次健康检查（不等 30 分钟，和定时的共用一把锁）；这一次里这个任务算做完
    （health.py 看 PHD_TASK_DONE）。检查出错不算这个任务失败。"""
    return f"{cmd}{KICK_SEP}{shlex.quote(label)} bash {KICK} kick || true; }}"


KICK_SEP = " && { PHD_TASK_DONE="


def transfer(name, paths, run=sh):
    """把 box 上的这些数据（相对数据目录）传到这台机器 → [box 任务号]。"""
    tids = []
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    lst = st.setdefault("transferred", {}).setdefault(name, [])
    for p in paths:
        parent = os.path.dirname(p.rstrip("/"))
        rel = M.MACHINES[name]["data"].replace("~/", "", 1) + (f"/{parent}" if parent else "")
        src = M.expand(M.MACHINES[M.hub()]["data"]) + (f"/{parent}" if parent else "")
        lab = f"sync_{name}_{tag(p)}"
        tids.append(add(M.hub(), lab, "sync", p, f"wait -p --label {lab} -- "
                        + shlex.quote(then_check(f"bash scripts/sync/tunnel_push.sh {name} {src} {rel} {tag(p)}", lab)), run,
                        to=name, group="wait"))
        if p not in lst:
            lst.append(p)
    write_json(STATE, st)
    return tids


def execute(acts, name, remote=None, run=sh, ok=None, tq=None):
    """按动作做 → [说明]。出错的那一步写进说明，后面的照做。ok（set）：记下领到或开始训的 key。"""
    ok = set() if ok is None else ok
    host = queue_host()

    def dicc(a):
        r = taskq.remote(host, a)
        return r and (r[0], r[1] + r[2])  # 出错的说明在标准错误里

    remote = remote or dicc
    tq = tq or {}
    L, msgs = M.MACHINES[name]["label"], []
    beats = [a["key"] for a in acts if a["op"] in ("beat", "keep")]
    if beats:
        r = remote(f"beat --worker {name} " + " ".join(beats))
        if r is None or r[0]:
            msgs.append(f"{L} 的领取续不上（{(r or ['', 'ssh 不通'])[1][-150:]}）")
    for a in acts:
        k = a["key"]
        try:
            if a["op"] == "release":
                if a.get("kill"):
                    ids = " ".join(map(str, a["kill"]))
                    rc, out = run(f"~/.local/bin/pueue kill -s SIGTERM {ids}; ~/.local/bin/pueue remove {ids}; true", M.MACHINES[name]["ssh"])
                r = remote(f"release --worker {name} {k}" + (f" --skip {shlex.quote(a['skip'])}" if a.get("skip") else ""))
                if r and r[0] == 0:
                    set_slot(name, k, None)
                msgs.append(f"{L} 交还 {k}：{a['why']}" if r and r[0] == 0 else f"{L} 交还 {k} 出错：{(r or ['', 'ssh 不通'])[1][-150:]}")
            elif a["op"] == "start":
                msgs.append(f"{L} 开始训 {k}（{a.get('group') or gpu_group(name)} 组，任务 {start(name, k, run, tq, a.get('group'))}）")
                ok.add(k)
            elif a["op"] == "transfer":
                msgs.append(f"{L} 领的 {k} 要先传 {' '.join(a['paths'])}（box 任务 {transfer(name, a['paths'], run)}）")
            elif a["op"] == "prefetch":
                msgs.append(f"{L} 卡在训，下一折多半是 {k}，提前传 {' '.join(a['paths'])}（{a['gb']:.0f} GB，box 任务 "
                            f"{transfer(name, a['paths'], run)}）")
            elif a["op"] == "take":
                r = remote(f"take --worker {name} --ttl {TTL} {k}")
                if r is None or r[0]:
                    msgs.append(f"{L} 没领到 {k}：{(r or ['', 'ssh 不通'])[1].strip()[-150:]}")
                    continue
                ok.add(k)
                if a.get("group"):
                    set_slot(name, k, a["group"])
                if a["paths"]:
                    msgs.append(f"{L} 领了 {k}（档位 {a['nice']}），先传 {' '.join(a['paths'])}（box 任务 {transfer(name, a['paths'], run)}）")
                elif a.get("wait"):
                    msgs.append(f"{L} 领了 {k}（档位 {a['nice']}），数据正在传，传完开始训")
                elif a.get("after"):
                    msgs.append(f"{L} 提前领了 {k}（档位 {a['nice']}），排在快训完的 {a['after']} 后面（{a['group']} 组，"
                                f"任务 {start(name, k, run, tq, a['group'])}）")
                else:
                    msgs.append(f"{L} 领了 {k}（档位 {a['nice']}），开始训（{a.get('group') or gpu_group(name)} 组，"
                                f"任务 {start(name, k, run, tq, a.get('group'))}）")
        except Exception as e:  # noqa: BLE001
            msgs.append(f"{L} {a['op']} {k} 出错：{e}")
    return msgs


def moves(s, t, name, p):
    """box 任务 t 在给这台机器传 p、或在从这台机器拉回 / 归档 p 那个数据集的折。没有记录的（手动加的）按命令里有没有
    这台机器和 p 的名字算，宁可多留。"""
    r = C.identify(M.hub(), t, s.get("records") or [])
    if r and (r.get("to") == name or r.get("src") == name):
        return r["key"] == p or tag(p) in r["key"].split("/")
    return not r and tag(p) in t["command"] and f" {name} " in f" {t['command']} "


def cleanup(s, name, run=sh, acts=None, dry=False):
    """自动传过去的数据（~/.cache/phd/refill.json 的 transferred）按"还要不要用"和空间预算管（2026-10-24）→ [说明]。
    不删：Dataset101；这台机器领着的、这次要领 / 开始 / 传的、预测下一折的任务要用的；这台机器上训练任务在用的；box 上正在传、
    正在从这台机器拉回 / 归档的；训完还没归档的折的。其余的：
      - 队列里没有这台机器能领的任务要用了 → 删；
      - 还有任务要用的留着，但盘剩的要不低于软线（硬线 × SOFT）：低了就从"下次用到"最晚的删起；
      - acts 里有提前传（prefetch）的：先删比它晚用到的、腾到"剩的 − 要传的 ≥ 软线"，腾不出来就从 acts 里去掉、这次不传。
    dry：只算、不删（说明照样给）。"""
    acts = [] if acts is None else acts
    m = M.MACHINES[name]
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    lst = st.get("transferred", {}).get(name, [])
    pf = next((a for a in acts if a["op"] == "prefetch"), None)
    if not s["ok"].get(name) or "error" in (s["pueue"].get(name) or {"error": 1}):
        if pf:
            acts.remove(pf)
        return []
    box = s["pueue"].get(M.hub()) or {"error": 1}
    tq = s.get("taskq", {}).get(queue_host()) or {}
    if "error" in box or not tq.get("tasks"):
        if pf:
            acts.remove(pf)
        return []
    L = m["label"]
    have = set(s["probe"].get(name, {}).get("HAVE", "").split())
    sync, sync_bad = syncing(s, name)
    ready = {p for p in have if tag(p) not in sync and tag(p) not in sync_bad}
    cands = candidates(s, name, tq, ready)
    hard = (m["disk"][1] if m["disk"] else 0) + TRAIN_OUT_GB
    soft = hard * SOFT
    pr = s["probe"].get(name, {})
    free = int(pr["DISK"]) / 1e9 if pr.get("DISK", "").isdigit() else None
    nxt = next_pick(tq, cands, ready, free if free is not None else 1e9, hard, moving=sync)[0]
    keep = set().union(*[set(data_of(tq, t["key"])) for t in tq["tasks"] if t.get("worker") == name and t["state"] != "done"],
                       *[set(data_of(tq, a["key"])) | set(a.get("paths") or []) for a in acts if a["op"] != "release"],
                       data_of(tq, nxt["key"]) if nxt else [])
    when = {}  # 数据 → 要用它的任务里最早领到的那个的先后
    for t in cands:
        for d in data_of(tq, t["key"]):
            when[d] = min(when.get(d, (9e9,)), rank(tq, t, ready))
    msgs, gone, held, later = [], [], [], []
    for p in list(lst):
        parts = p.split("/")
        if (p.startswith("/") or ".." in parts or "" in parts or len(parts) < 3 or parts[:2] not in (["nnUNet", "preprocessed"], ["nnUNet", "raw"])
                or not m.get("data")):
            msgs.append(f"{L} 上自动传过去的记录 {p!r} 不是 nnUNet/preprocessed|raw/<数据集> 下的路径，不删")
            continue
        if tag(p).startswith("Dataset101_") or p in keep or tag(p) in sync:
            continue
        busy = any(t["status"] in LIVE and p in data_of(tq, kind_key(s, name, t)[1] or "") for t in s["pueue"][name]["tasks"])
        busy |= any(t["status"] in LIVE and moves(s, t, name, p) for t in box["tasks"])
        busy |= any(p in data_of(tq, k) and not f.get("archived") for k, f in s["src"].get(name, {}).get("folds", {}).items() if f.get("final"))
        if busy:
            continue
        if p not in when:
            gone.append((p, "队列里没有任务要用了"))
        else:
            later.append(p)
    later.sort(key=lambda p: when[p], reverse=True)  # 最晚用到的在前
    room = None if free is None else free - sum(data_gb([d]) or 0 for d in lst if tag(d) in sync)  # 正在传的按传完算
    if pf:
        if room is None:
            acts.remove(pf)
            msgs.append(f"不知道 {L} 盘还剩多少，不提前传 {' '.join(pf['paths'])}")
        else:
            mark = rank(tq, next(t for t in tq["tasks"] if t["key"] == pf["key"]), ready)
            take, r = [], room
            for p in [q for q in later if when[q] > mark]:
                if r - pf["gb"] >= soft:
                    break
                take.append(p)
                r += data_gb([p]) or 0
            if r - pf["gb"] >= soft:
                gone += [(p, f"给提前传 {tag(pf['paths'][0])} 腾地方") for p in take]
                later = [p for p in later if p not in take]
                room = r - pf["gb"]
            else:
                acts.remove(pf)
                msgs.append(f"{L} 盘剩 {room:.0f} GB，提前传 {' '.join(pf['paths'])}（{pf['gb']:.0f} GB）后会低于 {soft:.0f} GB，"
                            f"删掉比它晚用到的数据也不够，这次不传")
    while room is not None and room < soft and later:  # 别人把盘用多了：我们留着的先缩小
        p = later.pop(0)
        gone.append((p, f"盘剩 {room:.0f} GB，低于 {soft:.0f} GB"))
        room += data_gb([p]) or 0
    for p, why in gone:
        if dry:
            msgs.append(f"{L} 上自动传过去的 {p} 会删（{why}）")
            continue
        rc, out = run(f"rm -rf {m['data']}/{shlex.quote(p)}", m["ssh"])
        if rc == 0:
            lst[:] = [x for x in lst if x not in (p, tag(p))]
            msgs.append(f"{L} 上自动传过去的 {p} 删掉了（{why}；box 上有完整的一份）")
        else:
            msgs.append(f"{L} 上删 {p} 出错：{out[-150:]}")
    if not dry:
        write_json(STATE, st)
    return msgs


CACHE_DIR = re.compile(r"Dataset\d+_[A-Za-z0-9_-]+")


def cache_cleanup(s, name, run=sh, acts=(), dry=False):
    """训练机自己从数据生成的缓存（做法的 cache，如 nnU-Net 的解压缓存，10/26）→ [说明]。要用时训练开始前重新生成
    （几分钟），所以只留这台机器上活着的训练任务、领着没训完的折、这次要领 / 开始的折的数据集用过的（缓存自己的数据集，
    加上缓存里记的用过它的数据集；软链接到别的数据集时几个数据集共用一份），其余删掉。
    缓存列表没读全（探查没有结束标记）、队列或 pueue 读不到时什么都不删。"""
    import recipes
    m = M.MACHINES[name]
    raw = (s["probe"].get(name) or {}).get("CACHE")
    if raw is None or not raw.endswith(C.CACHE_END) or not s["ok"].get(name) or "error" in (s["pueue"].get(name) or {"error": 1}):
        return []
    tq = s.get("taskq", {}).get(queue_host()) or {}
    have = [x.split(":", 1) for x in raw[:-len(C.CACHE_END)].split()]
    if not have or not tq.get("tasks") or any(len(x) != 2 for x in have):
        return []
    keys = {kind_key(s, name, t)[1] for t in s["pueue"][name]["tasks"] if t["status"] in LIVE and kind_key(s, name, t)[0] == "train"}
    keys |= {t["key"] for t in tq["tasks"] if t.get("worker") == name and t["state"] != "done"}
    keys |= {a["key"] for a in acts if a["op"] in ("take", "start")}
    used = {tag(p) for k in keys - {None} for p in data_of(tq, k)}
    roots = {c["root"]: c["depth"] for c in (r.get("cache") for r in recipes.RECIPES.values()) if c}
    msgs = []
    for p, users in have:
        root = next((r for r in roots if p.startswith(r + "/")), None)
        rest = p[len(root) + 1:].split("/") if root else []
        if (not rest or len(rest) != roots[root] or not CACHE_DIR.fullmatch(rest[0])
                or not all(re.fullmatch(r"[A-Za-z0-9_.-]+", x) and x not in (".", "..") for x in rest)):
            msgs.append(f"{m['label']} 上的缓存 {p!r} 路径不对，不删")
            continue
        if ({rest[0]} | set(filter(None, users.split(",")))) & used:
            continue
        if dry:
            msgs.append(f"{m['label']} 上的缓存 {p} 会删（没有在训、领着的折用它）")
            continue
        rc, out = run(f"rm -rf {m['data']}/{shlex.quote(p)}", m["ssh"])
        msgs.append(f"{m['label']} 上的缓存 {p} 删掉了（没有在训、领着的折用它；要用时训练开始前重新生成）" if rc == 0
                    else f"{m['label']} 上删缓存 {p} 出错：{out[-150:]}")
    return msgs


def log(lines):
    if lines:
        with open(LOG, "a") as fo:
            fo.writelines(f"{datetime.now():%F %T} {x}\n" for x in lines)


def shadow_log(name, line):
    """影子模式的"会做什么"：和上次一样就不再写（每 30 分钟一条一样的没用）。"""
    if C.changed(f"refill_{name}", line):
        log([line])


def run_all(s, dry=False):
    """health.py 调用：→ (事件说明列表, 卡住的 {问题编号: 说明}, 这次领了任务的机器)。"""
    events, stuck, filled = topup_all(s, dry) + (set(),)
    live = bool(P.AUTO.get("lab")) and not dry
    for name in M.names("pueue"):
        if "train" not in M.MACHINES[name]["roles"]:
            continue
        acts, st, why = plan(s, name)
        stuck.update(st)
        todo = [a for a in acts if a["op"] not in ("beat", "keep")]
        if not live:  # 开关没开：只续已有的领取（lab 上手动开始的训练不会过期被别处重训），其余只记日志、不推送
            desc = "；".join(f"{a['op']} {a['key']}" for a in todo) or f"不领（{why}）"
            if dry:
                notes = cleanup(s, name, acts=acts, dry=True) + cache_cleanup(s, name, acts=acts, dry=True)
                todo = [a for a in acts if a["op"] not in ("beat", "keep")]
                desc = "；".join(f"{a['op']} {a['key']}" for a in todo) or f"不领（{why}）"
                events.append(f"{name}：会 {desc}" if todo else f"{name}：{desc}")
                events += notes
                continue
            log(execute([a for a in acts if a["op"] in ("beat", "keep")], name))
            if todo:
                shadow_log(name, f"（开关没开）{name} 会：{desc}")
            continue
        ok = set()
        msgs = cleanup(s, name, acts=acts)  # 先腾地方；提前传放不下时从 acts 里去掉
        msgs += cache_cleanup(s, name, acts=acts)
        msgs += execute(acts, name, ok=ok, tq=s.get("taskq", {}).get(queue_host()))
        log(msgs)
        events += msgs
        if ok:
            filled.add(name)
    return events, stuck, filled


# ---------------- Slurm 训练机补作业 ----------------
def topup_all(s, dry=False):
    """每台 Slurm 训练机补交 worker 作业 → (dry 时的说明, 出错的 {问题编号: 说明})。"""
    events, stuck = [], {}
    for name in M.names("slurm"):
        if "train" in M.MACHINES[name]["roles"] and P.SLURM.get(name):
            ev, st = topup(s, name, dry=dry)
            events += ev
            stuck.update(st)
    return events, stuck


GPU_FILE, CPU_FILE = "worker.sbatch", "cpu_worker.sbatch"


def topup_probe(name):
    """每个 GPU QOS 一行"QOS|名字|排队上限|我们排着和在跑的段数"；交 CPU worker 时再加"CPU|我们的 CPU worker 作业数"、
    "ALL|CPU worker 那个 QOS 的提交上限|我们所有的作业数"（DICC 的分区都挂 normal，所有作业都占它的名额）；
    有一条命令出错就整个出错（不按 0 算）。QOS 来自 envs/policy.py 的 SLURM，作业名来自 envs/machines.py 的 jobs。"""
    conf = P.SLURM[name]
    out = "set -e; for q in " + " ".join(conf["gpu"]) + "; do lim=$(sacctmgr -nP show qos $q format=maxsubmitpu); " \
        "ids=$(squeue -u $USER -h -r -q $q -o %i); echo \"QOS|$q|$lim|$(printf '%s' \"$ids\" | grep -c . || true)\"; done"
    if conf.get("cpu"):
        out += f"; ids=$(squeue -u $USER -h -r -n {M.MACHINES[name]['jobs']['cpu']} -o %i); " \
            "echo \"CPU|$(printf '%s' \"$ids\" | grep -c . || true)\"; " \
            f"lim=$(sacctmgr -nP show qos {conf['cpu'][0]} format=maxsubmitpu); " \
            "ids=$(squeue -u $USER -h -r -o %i); echo \"ALL|$lim|$(printf '%s' \"$ids\" | grep -c . || true)\""
    return out


def unclaimed(tq, device):
    """队列里没人领（free / stale）、device 是这个的任务数。"""
    return sum(1 for t in tq["tasks"] if t["state"] in ("free", "stale") and info(tq, t["key"])["spec"].get("device", "gpu") == device)


def topup_plan(tq, counts, qos):
    """→ ([(QOS, 每段时限, 补几段)], 不补的原因)。counts：{QOS: (排队上限或 None, 现有段数)}。"""
    if not tq:
        return [], "读不到任务队列"
    if not tq.get("enabled"):
        return [], "任务队列的总开关不在"
    if tq.get("paused"):
        return [], "任务队列失败太多、暂停中"
    if not unclaimed(tq, "gpu"):
        return [], "队列里没有没人领的要卡的任务"
    out = []
    for q, v in qos.items():
        t, keep = (v, 0) if isinstance(v, str) else v
        if q not in counts:
            return [], f"读不到 {q} 的作业数"
        lim, n = counts[q]
        cap = (lim or SUBMIT_CAP) - keep
        if cap > n:
            out.append((q, t, cap - n, GPU_FILE))
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


def topup(s, name, dry=False, run=None):
    """→ (dry 时的说明, 出错的 {问题编号: 说明})。交上去的作业只记日志、不推送。"""
    run = run or sh
    m, tq, conf = M.MACHINES[name], s.get("taskq", {}).get(name), P.SLURM[name]
    if not s["ok"].get(name) or not tq:
        return [], {}
    rc, out = run(topup_probe(name), m["ssh"])
    if rc:
        return [], {}  # 一时连不上：下次再看（ssh 不通健康检查另外报）
    counts, ncpu, free = {}, None, None
    for ln in out.splitlines():
        p = ln.split("|")
        if len(p) == 4 and p[0] == "QOS" and p[3].isdigit():
            counts[p[1]] = (int(p[2]) if p[2].isdigit() else None, int(p[3]))
        elif len(p) == 2 and p[0] == "CPU" and p[1].isdigit():
            ncpu = int(p[1])
        elif len(p) == 3 and p[0] == "ALL" and p[2].isdigit():
            free = max(0, (int(p[1]) if p[1].isdigit() else SUBMIT_CAP) - int(p[2]))
    subs, why = topup_plan(tq, counts, conf["gpu"])
    if conf.get("cpu"):
        csub, cwhy = cpu_plan(tq, ncpu, conf["cpu"], free)
        subs += csub
        why = why if csub else f"{why}；CPU：{cwhy}"
    desc = "；".join(f"交 {'CPU worker ' if f == CPU_FILE else ''}{q} {n} 段" for q, _, n, f in subs) or f"不交（{why}）"
    if dry:
        return [f"{name}：会 {desc}" if subs else f"{name}：{desc}"], {}
    if not P.AUTO.get("dicc"):
        if subs:
            shadow_log(f"{name}:topup", f"（开关没开）{m['label']} 会：{desc}")
        return [], {}
    msgs, stuck = [], {}
    for q, t, n, f in subs:
        ids = []
        for _ in range(n):
            rc, out = run(f"cd {m['repo']}/scripts/queue && sbatch --parsable --qos={q} --time={t} {f}", m["ssh"])
            if rc:
                stuck[f"topup:{name}:{q}{':cpu' if f == CPU_FILE else ''}"] = f"{m['label']} {q} 补交 {f} 出错：{out[-200:]}"
                break
            ids.append(out.strip().splitlines()[-1])
        if ids and f == CPU_FILE:
            msgs.append(f"{m['label']} 有纯 CPU 任务，交了 {len(ids)} 个 CPU worker 作业 {'、'.join(ids)}（{q}，每个 {t}）")
        elif ids:
            msgs.append(f"{m['label']} {q} 空出 {n} 个位置，交了 {len(ids)} 个 worker 作业 {'、'.join(ids)}（每个 {t}）")
    log(msgs)
    return [], stuck


if __name__ == "__main__":
    import health as H  # noqa: E402
    ev, st, _ = run_all(H.gather(), dry=True)
    print("\n".join(ev + [f"卡住：{v}" for v in st.values()]))
