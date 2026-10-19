#!/usr/bin/env python3
"""给训练机补活（2026-10-18）：pueue 训练机（envs/machines.py 里 scheduler=pueue、角色 train 的，现在是 lab）从 DICC 的
任务队列领任务；Slurm 训练机（DICC）排着的作业不够时补交 worker 作业（最后一节）。
由 box 上的健康检查（scripts/monitor/health.py，每 30 分钟）调用。lab 连不上 DICC，所以由 box 替它领、续、交还。

待办只有一份：DICC 上的 ~/.cache/phd/taskq/tasks.tsv（scripts/queue/taskq.py）；谁在训哪一折只看 claims/。
每次检查，对每台这样的机器：
  1. 维护它已经领的折（领取记录 worker = 机器名；只看领取之后开始的训练任务）：
     - 训练任务还在（pueue 里 Running / Queued / Stashed / Paused / Locked），或者数据还在传 → 续上（beat）；
     - 这台机器上训完了 → 继续续着，等 box 拉回、archive_fold.sh 备份到 DICC、队列显示"训完"后再交还
       （提前交还的话 DICC 作业会把它当成没训过、从头训）；这样续着的不影响领下一折；
     - 任务没了、没训完：这台机器上一点进度都没有 → 交还，回到队列；有进度 → 领取保留、报出来（存档在这台，只让它接着训）；
     - 传数据出错 → 领取保留、报出来（数据可能只传了一半，不开始训）。
     这台机器、box 或任何一台训练机连不上时什么都不动（领取 24 小时内有效，过期后 DICC 才能领）。
  2. 卡空了（训练组里没有活着的训练任务）就领下一折：队列里 free、任何机器上都没有进度、这台机器没跳过的，按（档位，先后）
     取第一个。训练没开始就失败、又没有进度（多半是这台机器放不下这个模型）时用 release --skip 交还：记在 skips/，
     这台机器以后不再领它，不算队列的失败，不用事先按模型分类。24 小时内这样跳过 2 个就先不领、报出来（多半是环境坏了）。
     taskq.py take 成功后：任务说明里的 data（scripts/queue/spec.py）这台机器上都有，就直接在 pueue 的训练组里加
     run_task.sh；缺的先用 scripts/sync/tunnel_push.sh 传（box 的 wait 组），下次检查时数据到了再加训练任务。
     传完后盘上剩不到"表里 disk 的提醒线 + 10 GB（训练输出）"的任务跳过；这台机器没有任务要的环境（machines.py 的 envs）不领。
  拉回、推理、评测不在这里排：box 上的 scripts/queue/advance.py 按状态推进。
  自动传过去的数据（相对数据目录的路径）记在 ~/.cache/phd/refill.json，这台机器上用它的训练和 box 上的拉回、归档都做完后删掉（Dataset101 不删）。
开关：box 上 ~/.cache/phd/taskq/LAB 不存在时只续已有的领取，其余只算、不动（"会做什么"有变化时记进 ~/refill.log，不推送）；
要停掉 lab 的训练用这个开关：只在 lab 上停掉训练任务的话，下次检查会把那一折交还队列、再领下一折。
DICC 上的 taskq.py 是 10/18 之前的旧版时什么都不做。跳过记录（skips/<key>）要撤销，删掉那个文件。

DICC 补作业：每个 QOS（表里 qos 字段：QOS → (每段时限, 留几个)）排着和在跑的作业（数组按段算）不到上限（sacctmgr 的 MaxSubmitPU，
没设时按 SUBMIT_CAP）减去留给别的作业的名额、任务队列里还有没人领的任务时，交 scripts/queue/worker.sbatch 补满（一个不限并发的数组，
同时跑几个由 QOS 的 GPU 上限管）。空出一个补一个，作业的排队时间分从交上去开始累积。
开关：box 上 ~/.cache/phd/taskq/DICC，不存在时只把"会交什么"记进 ~/refill.log。交上去的作业记进 ~/refill.log，不推送。

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
import experiments as E  # noqa: E402
import spec as S  # noqa: E402
import taskq  # noqa: E402

TTL = 86400  # lab 的领取多久不续就过期（秒）
TRAIN_OUT_GB = 10  # 领之前除了数据集和盘的提醒线，再给训练输出（checkpoint、验证集预测）留的空间
SKIP_BREAK = 2  # 24 小时内这么多个领到的任务没开始就失败，就先停止替这台机器领（多半是环境坏了）
STATE = os.path.expanduser("~/.cache/phd/refill.json")
SWITCH = os.path.expanduser("~/.cache/phd/taskq/LAB")
DICC_SWITCH = os.path.expanduser("~/.cache/phd/taskq/DICC")
SUBMIT_CAP = 10  # QOS 没设排队上限时，每个 QOS 最多排这么多段
LOG = os.path.expanduser("~/refill.log")
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]
LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")


def queue_host():
    """任务队列在哪台 Slurm 机器上（角色 train 的 Slurm 机器，现在是 dicc）。"""
    return next(n for n in M.names("slurm") if "train" in M.MACHINES[n]["roles"])


def gpu_group(m):
    return next(g for g, (_, gpu, _) in m["groups"].items() if gpu is not None)


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
    from health import task_key, mr_time
    out = {}
    for t in sorted((s["pueue"].get(name) or {}).get("tasks", []), key=lambda t: t["id"]):
        kind, key = task_key(t["command"])
        if kind == "train":
            out.setdefault(key, []).append((t["id"], t["status"], mr_time(t.get("start"))))
    return out


def syncing(s, name):
    """box 上给这台机器传数据的任务，每个数据集只看最新的一个 → ({数据集: 还在传的任务}, {数据集: 出错停了的任务})。"""
    box = s["pueue"].get(M.hub()) or {}
    last = {}
    for t in sorted(box.get("tasks", []), key=lambda t: t["id"]):
        if (t["label"] or "").startswith(f"sync_{name}_"):
            last[t["label"][len(f"sync_{name}_"):]] = t
    return ({d: t for d, t in last.items() if t["status"] in LIVE},
            {d: t for d, t in last.items() if t["status"] not in LIVE and t["status"] != "Success"})


def plan(s, name):
    """→ (动作列表, 卡住的 {问题编号: 说明}, 不领的原因)。动作：dict(op=beat|release|start|transfer|take, key, …)。"""
    m, host = M.MACHINES[name], queue_host()
    tq = s.get("taskq", {}).get(host)
    if not s["ok"].get(name) or not s["ok"].get(M.hub()) or "error" in (s["pueue"].get(name) or {"error": 1}) \
            or any(n not in s["src"] for n in M.names("train")):
        return [], {}, "连不上"  # 有一台训练机的进度读不到时也不动：不然看不出哪一折在别处训了一半
    if not tq or not tq.get("enabled"):
        return [], {}, "读不到任务队列，或者队列没开"
    if "recent_skips" not in tq:  # 10/18 前的 taskq.py 不认 take、ttl、--skip
        return [], {}, f"{host} 上的 taskq.py 还是旧版（先在 {host} 上 git pull）"
    tt_all, (sync, sync_bad) = train_tasks(s, name), syncing(s, name)
    have = set(s["probe"].get(name, {}).get("HAVE", "").split())
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
        if t["state"] == "done":
            acts.append(dict(op="release", key=key, why="训完了，DICC 上有了存档"))
        elif s["src"].get(name, {}).get("folds", {}).get(key, {}).get("final"):
            # lab 训完了，但要等 box 拉回、archive_fold.sh 备份到 DICC 后队列才看得到"训完"；这之前交还的话 DICC 作业会从头重训
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
            acts.append(dict(op="start", key=key))  # 数据传完了，开始训
        elif progress(s, key, name):
            last = tt[key][-1]
            acts.append(dict(op="beat", key=key))
            stuck[f"labclaim:{key}"] = (f"{M.MACHINES[name]['label']} 领的 {key} 训练任务 {last[0]} 停了（{last[1]}），没训完、有进度，"
                                        f"领取保留；接着训：q {gpu_group(m)} --label train:{key} -- {shlex.quote(run_cmd(f))}；"
                                        f"放弃：taskq.py release --worker {name} {key}")
        elif tt[key][-1][1].startswith("Failed") and tt[key][-1][1] not in ("Failed (75)", "Failed (137)", "Failed (143)"):
            # 没开始就失败（多半是这台机器放不下这个模型）：交还，记在 skips/（这台机器以后不再领它；不算队列的失败）
            acts.append(dict(op="release", key=key, skip=f"训练任务 {tt[key][-1][0]} {tt[key][-1][1]}",
                             why=f"训练任务 {tt[key][-1][0]} 失败了（{tt[key][-1][1]}），{name} 上没有进度，交还，{name} 不再领它"))
        else:
            acts.append(dict(op="release", key=key, why=f"训练任务 {tt[key][-1][0]} 停了（{tt[key][-1][1]}），{name} 上没有进度，交还队列"))
    g = gpu_group(m)
    busy = [k for k, ts in tt_all.items() if any(st in LIVE for _, st, _ in ts)]
    held = {a["key"] for a in acts if a["op"] == "beat"}  # 领着、没训完的折（在训、在传数据、刚开始、停了等人处理）；keep 的不算
    if busy or held:
        return acts, stuck, "卡上有训练任务" if busy else "还领着没训完的折"
    nskip = (tq.get("recent_skips") or {}).get(name, 0)
    if nskip >= SKIP_BREAK:
        stuck[f"labskip:{name}"] = (f"{M.MACHINES[name]['label']} 24 小时内有 {nskip} 个领到的任务没开始就失败了，先不再领；"
                                    f"看 {name} 上 pueue log 里失败的训练任务（多半是环境或数据坏了，不是放不下）")
        return acts, stuck, f"24 小时内 {nskip} 个任务没开始就失败"
    pr = s["probe"].get(name, {})
    free = int(pr["DISK"]) / 1e9 if pr.get("DISK", "").isdigit() else None
    warn = (m["disk"][1] if m["disk"] else 0) + TRAIN_OUT_GB
    envs = set(m.get("envs") or {})
    cands = [t for t in tq["tasks"] if t["state"] == "free" and not t.get("bad") and info(tq, t["key"])["spec"].get("env") in envs
             and not progress(s, t["key"]) and name not in t.get("skipped_by", [])]
    cands.sort(key=lambda t: (t["nice"], t["order"]))
    for t in cands:
        need = [p for p in info(tq, t["key"])["spec"].get("data") or [] if p not in have]
        if need:
            gb = data_gb(need)
            if gb is None or free is None or free - gb < warn:
                continue
        acts.append(dict(op="take", key=t["key"], nice=t["nice"], paths=need))
        return acts, stuck, ""
    return acts, stuck, "队列里没有能领的折" if not cands else f"队列里的任务要的数据都放不下（{name} 盘剩 {free:.0f} GB）" if free is not None else "不知道盘还剩多少"


def data_gb(paths):
    """box 上这些数据（相对数据目录）一共多大；有一个不在 → None。"""
    ps = [os.path.join(M.expand(M.MACHINES[M.hub()]["data"]), p) for p in paths]
    if not all(os.path.exists(p) for p in ps):
        return None
    r = subprocess.run(["du", "-sbLc"] + ps, capture_output=True, text=True)
    return int(r.stdout.strip().splitlines()[-1].split()[0]) / 1e9 if r.returncode == 0 else None


# ---------------- 执行 ----------------
def sh(cmd, host=None, timeout=300):
    """在 box（host=None）或别的机器上跑一条 shell 命令 → (退出码, 输出)。"""
    full = ["bash", "-c", cmd] if host is None else SSH + [host, cmd]
    try:
        r = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout + r.stderr).strip()
    except Exception as e:  # noqa: BLE001
        return 1, str(e)


def q(name, args, run=sh):
    """在某台 pueue 机器上用 scripts/queue/q 加任务 → 任务号。"""
    m = M.MACHINES[name]
    rc, out = run(f"cd {m['repo']} && scripts/queue/q {args}", m["ssh"])
    # 带 -p 时只输出任务号，不带时是"New task added (id 118)."（10/17 改成两种都认）
    hit = re.fullmatch(r"(?:New task added \(id )?(\d+)\)?\.?", out.strip().splitlines()[-1].strip()) if out.strip() else None
    if rc or not hit:
        raise RuntimeError(f"{name} 上加任务失败：{out[-200:]}")
    return int(hit.group(1))


def start(name, key, run=sh, tq=None):
    return q(name, f"{gpu_group(M.MACHINES[name])} -p --label train:{key} -- {shlex.quote(run_cmd(info(tq, key)))}", run)


def transfer(name, paths, run=sh):
    """把 box 上的这些数据（相对数据目录）传到这台机器 → [box 任务号]。"""
    tids = []
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    lst = st.setdefault("transferred", {}).setdefault(name, [])
    for p in paths:
        parent = os.path.dirname(p.rstrip("/"))
        rel = M.MACHINES[name]["data"].replace("~/", "", 1) + (f"/{parent}" if parent else "")
        src = M.expand(M.MACHINES[M.hub()]["data"]) + (f"/{parent}" if parent else "")
        tids.append(q(M.hub(), f"wait -p --label sync_{name}_{tag(p)} -- "
                      + shlex.quote(f"bash scripts/sync/tunnel_push.sh {name} {src} {rel} {tag(p)}"), run))
        if p not in lst:
            lst.append(p)
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    json.dump(st, open(STATE, "w"), indent=1)
    return tids


def execute(acts, name, remote=None, run=sh, ok=None, tq=None):
    """按动作做 → [说明]。出错的那一步写进说明，后面的照做。ok（set）：记下领到或开始训的 key。"""
    ok = set() if ok is None else ok
    host = queue_host()
    remote = remote or (lambda a: taskq.remote(host, a))
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
                r = remote(f"release --worker {name} {k}" + (f" --skip {shlex.quote(a['skip'])}" if a.get("skip") else ""))
                msgs.append(f"{L} 交还 {k}：{a['why']}" if r and r[0] == 0 else f"{L} 交还 {k} 出错：{(r or ['', 'ssh 不通'])[1][-150:]}")
            elif a["op"] == "start":
                msgs.append(f"{L} 开始训 {k}（任务 {start(name, k, run, tq)}）")
                ok.add(k)
            elif a["op"] == "transfer":
                msgs.append(f"{L} 领的 {k} 要先传 {' '.join(a['paths'])}（box 任务 {transfer(name, a['paths'], run)}）")
            elif a["op"] == "take":
                r = remote(f"take --worker {name} --ttl {TTL} {k}")
                if r is None or r[0]:
                    msgs.append(f"{L} 没领到 {k}：{(r or ['', 'ssh 不通'])[1].strip()[-150:]}")
                    continue
                ok.add(k)
                if a["paths"]:
                    msgs.append(f"{L} 领了 {k}（档位 {a['nice']}），先传 {' '.join(a['paths'])}（box 任务 {transfer(name, a['paths'], run)}）")
                else:
                    msgs.append(f"{L} 领了 {k}（档位 {a['nice']}），开始训（任务 {start(name, k, run, tq)}）")
        except Exception as e:  # noqa: BLE001
            msgs.append(f"{L} {a['op']} {k} 出错：{e}")
    return msgs


def cleanup(s, name, run=sh, fresh=()):
    """自动传过去的数据集：这台机器上没有用它的训练任务、box 上没有从这台机器拉回 / 归档它的任务了，就删掉 → [说明]。"""
    from health import task_key
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    lst = st.get("transferred", {}).get(name, [])
    if not lst or not s["ok"].get(name) or "error" in (s["pueue"].get(name) or {"error": 1}):
        return []
    box = s["pueue"].get(M.hub()) or {"error": 1}
    if "error" in box:
        return []
    tq = s.get("taskq", {}).get(queue_host()) or {}
    uses = lambda key: set(info(tq, key)["spec"].get("data") or [])  # noqa: E731
    claimed = set().union(*[uses(t["key"]) for t in tq.get("tasks", []) if t.get("worker") == name], *[uses(k) for k in fresh])
    msgs = []
    for p in list(lst):
        p = p if "/" in p else f"nnUNet/preprocessed/{p}"  # 10/18 前只记数据集名
        if ".." in p.split("/") or tag(p).startswith("Dataset101_") or p in claimed:
            continue
        busy = any(t["status"] in LIVE and p in uses(task_key(t["command"])[1] or "") for t in s["pueue"][name]["tasks"])
        busy |= any(t["status"] in LIVE and tag(p) in t["command"] and f" {name} " in f" {t['command']} " for t in box["tasks"])
        busy |= any(p in uses(k) and not f.get("archived") for k, f in s["src"].get(name, {}).get("folds", {}).items() if f.get("final"))
        if busy:
            continue
        rc, out = run(f"rm -rf {M.MACHINES[name]['data']}/{shlex.quote(p)}", M.MACHINES[name]["ssh"])
        if rc == 0:
            lst[:] = [x for x in lst if x not in (p, tag(p))]
            msgs.append(f"{M.MACHINES[name]['label']} 上自动传过去的 {p} 用完了，已删掉（box 上有完整的一份）")
    json.dump(st, open(STATE, "w"), indent=1)
    return msgs


def log(lines):
    if lines:
        with open(LOG, "a") as fo:
            fo.writelines(f"{datetime.now():%F %T} {x}\n" for x in lines)


def shadow_log(name, line):
    """影子模式的"会做什么"：和上次一样就不再写（每 30 分钟一条一样的没用）。"""
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    if st.setdefault("shadow", {}).get(name) == line:
        return
    st["shadow"][name] = line
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    json.dump(st, open(STATE, "w"), indent=1)
    log([line])


def run_all(s, dry=False):
    """health.py 调用：→ (事件说明列表, 卡住的 {问题编号: 说明}, 这次领了任务的机器)。"""
    events, stuck, filled = [], {}, set()
    for name in M.names("slurm"):
        if "train" in M.MACHINES[name]["roles"] and M.MACHINES[name].get("qos"):
            ev, st = topup(s, name, dry=dry)
            events += ev
            stuck.update(st)
    live = os.path.exists(SWITCH) and not dry
    for name in M.names("pueue"):
        if "train" not in M.MACHINES[name]["roles"]:
            continue
        acts, st, why = plan(s, name)
        stuck.update(st)
        todo = [a for a in acts if a["op"] not in ("beat", "keep")]
        if not live:  # 开关没开：只续已有的领取（lab 上手动开始的训练不会过期被别处重训），其余只记日志、不推送
            desc = "；".join(f"{a['op']} {a['key']}" for a in todo) or f"不领（{why}）"
            if dry:
                events.append(f"{name}：会 {desc}" if todo else f"{name}：{desc}")
                continue
            log(execute([a for a in acts if a["op"] in ("beat", "keep")], name))
            if todo:
                shadow_log(name, f"（开关没开）{name} 会：{desc}")
            continue
        ok = set()
        msgs = execute(acts, name, ok=ok, tq=s.get("taskq", {}).get(queue_host()))
        msgs += cleanup(s, name, fresh=ok)
        log(msgs)
        events += msgs
        if ok:
            filled.add(name)
    return events, stuck, filled


# ---------------- Slurm 训练机补作业 ----------------
def topup_probe(qos):
    """每个 QOS 一行"QOS|名字|排队上限|我们排着和在跑的段数"；有一条命令出错就整个出错（不按 0 算）。"""
    return "set -e; for q in " + " ".join(qos) + "; do lim=$(sacctmgr -nP show qos $q format=maxsubmitpu); " \
        "ids=$(squeue -u $USER -h -r -q $q -o %i); echo \"QOS|$q|$lim|$(printf '%s' \"$ids\" | grep -c . || true)\"; done"


def topup_plan(tq, counts, qos):
    """→ ([(QOS, 每段时限, 补几段)], 不补的原因)。counts：{QOS: (排队上限或 None, 现有段数)}。"""
    if not tq:
        return [], "读不到任务队列"
    if not tq.get("enabled"):
        return [], "任务队列的总开关不在"
    if tq.get("paused"):
        return [], "任务队列失败太多、暂停中"
    if not any(t["state"] in ("free", "stale") for t in tq["tasks"]):
        return [], "队列里没有没人领的任务"
    out = []
    for q, v in qos.items():
        t, keep = (v, 0) if isinstance(v, str) else v
        if q not in counts:
            return [], f"读不到 {q} 的作业数"
        lim, n = counts[q]
        cap = (lim or SUBMIT_CAP) - keep
        if cap > n:
            out.append((q, t, cap - n))
    return out, "" if out else "各 QOS 都排满了"


def topup(s, name, dry=False, run=None):
    """→ (dry 时的说明, 出错的 {问题编号: 说明})。交上去的作业只记日志、不推送。"""
    run = run or sh
    m, tq = M.MACHINES[name], s.get("taskq", {}).get(name)
    if not s["ok"].get(name) or not tq:
        return [], {}
    rc, out = run(topup_probe(m["qos"]), m["ssh"])
    if rc:
        return [], {}  # 一时连不上：下次再看（ssh 不通健康检查另外报）
    counts = {}
    for ln in out.splitlines():
        p = ln.split("|")
        if len(p) == 4 and p[0] == "QOS" and p[3].isdigit():
            counts[p[1]] = (int(p[2]) if p[2].isdigit() else None, int(p[3]))
    subs, why = topup_plan(tq, counts, m["qos"])
    desc = "；".join(f"交 {q} {n} 段" for q, _, n in subs) or f"不交（{why}）"
    if dry:
        return [f"{name}：会 {desc}" if subs else f"{name}：{desc}"], {}
    if not os.path.exists(DICC_SWITCH):
        if subs:
            shadow_log(f"{name}:topup", f"（开关没开）{m['label']} 会：{desc}")
        return [], {}
    msgs, stuck = [], {}
    for q, t, n in subs:
        rc, out = run(f"cd {m['repo']}/scripts/queue && sbatch --parsable --qos={q} --time={t} --array=1-{n} worker.sbatch", m["ssh"])
        if rc == 0:
            msgs.append(f"{m['label']} {q} 空出 {n} 个位置，交了 worker 作业 {out.strip().splitlines()[-1]}（{n} 段，每段 {t}）")
        else:
            stuck[f"topup:{name}:{q}"] = f"{m['label']} {q} 补交 worker.sbatch 出错：{out[-200:]}"
    log(msgs)
    return [], stuck


if __name__ == "__main__":
    import health as H  # noqa: E402
    ev, st, _ = run_all(H.gather(), dry=True)
    print("\n".join(ev + [f"卡住：{v}" for v in st.values()]))
