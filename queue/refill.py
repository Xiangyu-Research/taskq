#!/usr/bin/env python3
"""训练机的数据清理和 DICC 补作业（2026-10-18；10/31 起 lab 不在这里领训练）。由 box 上的健康检查
（scripts/monitor/health.py，每 30 分钟）调用：run_all 管 pueue 训练机（envs/machines.py 里 scheduler=pueue、角色 train 的，
现在是 lab）上的数据，topup_all 给 Slurm 训练机（DICC）补交 worker 作业。

lab 的训练（10/31 起）：box 上的 scripts/queue/feed.py 把总队列里的任务分给 lab（assigned = lab）、送进 lab 的缓冲，
lab 上常驻的 worker（scripts/queue/dicc_worker.sh）领着训；0 号卡手动训的折在总队列里分给 lab:gpu0。
传数据（transfer）、box 任务（add、then_check）、挑下一折（candidates、next_pick）这些共用的函数在这里，feed.py、relay.py、
advance.py 调用。

lab 上的数据：
  自动传过去的数据（相对数据目录的路径）记在 ~/.cache/phd/refill.json 的 transferred，按"还要不要用"和空间预算管（cleanup()，2026-10-24）：
  - 不删：lab 缓冲里所有任务的、总队列里分给 lab 的（assigned 是 lab 或 lab:<组>）任务的、lab 上活着的训练任务的、box 上正在传、
    正在从 lab 拉回 / 归档的、lab 上训完还没归档的折的，以及实验表的 KEEP_DATA；
  - 其余的：总队列里没有 lab 能训的任务（free、有环境、要卡、没有进度、lab 没跳过）要用了就删；还有任务要用的留着，
    盘剩的低于软线（硬线 × SOFT，硬线 = 盘的提醒线 + TRAIN_OUT_GB）时从最晚用到的删起。
  lab、box 连不上，或者总队列、lab 的缓冲读不到时什么都不删。
开关：envs/policy.py 的 AUTO["lab"] 是 False 时只算、不删（"会删什么"有变化时记进 ~/refill.log）。

DICC 补作业：每个 QOS（envs/policy.py 的 SLURM[机器]["gpu"]：QOS → (每段时限, 留几个)）排着和在跑的作业（数组按段算）不到上限（sacctmgr 的 MaxSubmitPU，
没设时按 SUBMIT_CAP；读作业数、交作业用 scripts/sites/slurm.py）减去留给别的作业的名额、DICC 的缓冲里还有没人领的任务时，交 scripts/queue/worker.sbatch 补满（每个空位交一个
普通作业，不用数组：10/21 起 DICC 的使用率检查会在数组作业开始约 65 分钟后把它取消；同时跑几个由 QOS 的 GPU 上限管）。
空出一个补一个，作业的排队时间分从交上去开始累积。只看要卡的任务（任务说明的 device=gpu）。
纯 CPU 的任务（device=cpu，如评测，2026-10-24）：SLURM[机器] 里有 cpu 的机器，缓冲里有没人领的这类任务、我们的 CPU worker 作业
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
sys.path.insert(0, os.path.join(REPO, "scripts", "sites"))
import machines as M  # noqa: E402
import slurm as SL  # noqa: E402
import policy as P  # noqa: E402
import experiments as E  # noqa: E402
import taskq  # noqa: E402
import common as C  # noqa: E402
from common import write_json  # noqa: E402

TRAIN_OUT_GB = 10  # 领之前除了数据集和盘的提醒线，再给训练输出（存档、验证结果）留的空间
SOFT = 2  # 自动传过去、留着以后用的数据：盘至少剩"硬线（提醒线 + TRAIN_OUT_GB）× SOFT"
KICK = "~/auto/PhD/scripts/monitor/auto_refresh.sh"  # 健康检查（在 cron 用的克隆里跑，和定时的共用一把锁；kick 最多等 5 分钟）
STATE = os.path.expanduser("~/.cache/phd/refill.json")
SUBMIT_CAP = SL.SUBMIT_CAP  # QOS 没设排队上限时，每个 QOS 最多排这么多段（scripts/sites/slurm.py）
LOG = os.path.expanduser("~/refill.log")
LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")


def kind_key(s, name, t):
    """这台机器上的 pueue 任务 → (步骤, key)（任务记录，scripts/queue/common.py 的 identify）；认不出 → (None, None)。"""
    r = C.identify(name, t, s.get("records") or [])
    return (r["step"], r["key"]) if r else (None, None)


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
    """这一折有没有训练进度（训完、有 epoch，或者有存档：任务说明的 resume）：host=None 时看所有机器。"""
    from health import fold_info
    if any(v[0] or v[1] > 0 for h, v in fold_info(s, key).items() if host in (None, h)):
        return True
    return any(((d.get("folds") or {}).get(key) or {}).get("resumable") for h, d in (s.get("src") or {}).items()
               if host in (None, h))


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


def data_of(tq, key):
    return info(tq, key)["spec"].get("data") or []


def rank(tq, t, ready):
    """领的先后：（档位，缺不缺数据，先后）。同一档里数据已经在这台机器上的先领（马上能训）。"""
    return (t["nice"], any(p not in ready for p in data_of(tq, t["key"])), t["order"])


def candidates(s, name, tq, ready, oks=None):
    """这台机器能领的任务，按领的先后排：free、没标成有问题、环境就绪、没限定在别的机器、要卡、任何机器上都没有进度、这台机器没跳过。
    oks：那边建好、检查过的按描述建的环境 {名: 描述的哈希}（envs/machines.py 的 ready_envs）。"""
    envs = M.ready_envs(name, oks)
    cands = [t for t in tq["tasks"] if t["state"] == "free" and not t.get("bad") and info(tq, t["key"])["spec"].get("env") in envs
             and M.serves(name, info(tq, t["key"])["spec"].get("machines"))
             and info(tq, t["key"])["spec"].get("device", "gpu") == "gpu"
             and not progress(s, t["key"]) and name not in t.get("skipped_by", [])]
    return sorted(cands, key=lambda t: rank(tq, t, ready))


def next_pick(tq, cands, ready, free, hard, exclude=(), moving=()):
    """下一折领哪个 → (任务, 要传的数据, 要传多少 GB, 有数据正在传)；没有 → (None, [], 0, False)。moving：正在传的数据集（tag）。
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


def moves(s, t, name, p):
    """box 任务 t 在给这台机器传 p、或在从这台机器拉回 / 归档 p 那个数据集的折。没有记录的（手动加的）按命令里有没有
    这台机器和 p 的名字算，宁可多留。"""
    r = C.identify(M.hub(), t, s.get("records") or [])
    if r and (r.get("to") == name or r.get("src") == name):
        return r["key"] == p or tag(p) in r["key"].split("/")
    return not r and tag(p) in t["command"] and f" {name} " in f" {t['command']} "


def kept(s, name):
    """这台机器还要用的任务 → (key 的集合, 它们要的数据的集合)；总队列或这台的缓冲读不到 → None（10/31）。
    算上缓冲里所有的任务，和总队列里分给它的（assigned 冒号前那段是它：lab、lab:gpu0）。"""
    tqs = s.get("taskq") or {}
    ctq, btq = tqs.get(taskq.queue_host()), tqs.get(name)
    if not ctq or not ctq.get("tasks") or not isinstance(btq, dict) or btq.get("tasks") is None:
        return None
    keys, data = set(), set()
    for tq, ts in ((btq, btq["tasks"]), (ctq, [t for t in ctq["tasks"] if (t.get("assigned") or "").split(":")[0] == name])):
        for t in ts:
            keys.add(t["key"])
            data |= set(data_of(tq, t["key"]))
    return keys, data


def cleanup(s, name, run=None, dry=False):
    """自动传过去的数据（~/.cache/phd/refill.json 的 transferred）按"还要不要用"和空间预算管（2026-10-24，10/31 改）→ [说明]。
    不删：实验表的 KEEP_DATA；只删做法的 INPUT_ROOTS 下面的；这台机器缓冲里的、总队列里分给它的任务要用的（kept）；这台机器上训练任务在用的；box 上正在传、
    正在从这台机器拉回 / 归档的；训完还没归档的折的。其余的：
      - 总队列里没有这台机器能训的任务（candidates）要用了 → 删；
      - 还有任务要用的留着，但盘剩的要不低于软线（硬线 × SOFT）：低了就从"下次用到"最晚的删起。
    这台机器、box 连不上，总队列、缓冲读不到时什么都不删。dry：只算、不删（说明照样给）。"""
    run, m = run or sh, M.MACHINES[name]
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    lst = st.get("transferred", {}).get(name, [])
    if not s["ok"].get(name) or "error" in (s["pueue"].get(name) or {"error": 1}):
        return []
    box = s["pueue"].get(M.hub()) or {"error": 1}
    k = kept(s, name)
    if "error" in box or k is None:
        return []
    keep, tq = k[1], s["taskq"][taskq.queue_host()]
    L = m["label"]
    have = set(s["probe"].get(name, {}).get("HAVE", "").split())
    sync, sync_bad = syncing(s, name)
    ready = {p for p in have if tag(p) not in sync and tag(p) not in sync_bad}
    hard = (m["disk"][1] if m["disk"] else 0) + TRAIN_OUT_GB
    soft = hard * SOFT
    pr = s["probe"].get(name, {})
    free = int(pr["DISK"]) / 1e9 if pr.get("DISK", "").isdigit() else None
    when = {}  # 数据 → 要用它的任务里最先训的那个的先后
    for t in candidates(s, name, tq, ready):
        for d in data_of(tq, t["key"]):
            when[d] = min(when.get(d, (9e9,)), rank(tq, t, ready))
    import fnmatch
    import recipes
    roots = [r.strip("/").split("/") for r in recipes.INPUT_ROOTS]
    pinned = E.keep_data()
    msgs, gone, later = [], [], []
    for p in list(lst):
        parts = p.split("/")
        if (p.startswith("/") or ".." in parts or "" in parts or not any(len(parts) > len(r) and parts[:len(r)] == r for r in roots)
                or not m.get("data")):
            msgs.append(f"{L} 上自动传过去的记录 {p!r} 不是 {'|'.join(recipes.INPUT_ROOTS)} 的下一级路径，不删")
            continue
        if any(fnmatch.fnmatch(tag(p), k) for k in pinned) or p in keep or tag(p) in sync:
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


def log(lines):
    if lines:
        with open(LOG, "a") as fo:
            fo.writelines(f"{datetime.now():%F %T} {x}\n" for x in lines)


def shadow_log(name, line):
    """影子模式的"会做什么"：和上次一样就不再写（每 30 分钟一条一样的没用）。"""
    if C.changed(f"refill_{name}", line):
        log([line])


def run_all(s, dry=False):
    """health.py 调用：清理 pueue 训练机上的数据和缓存 → (事件说明列表, 卡住的 {问题编号: 说明}, 这次领了任务的机器)。
    10/31 起这里不领任务，后两项总是空的。"""
    events = []
    for name in M.names("pueue"):
        if "train" not in M.MACHINES[name]["roles"]:
            continue
        live = bool(P.AUTO.get(name)) and not dry
        msgs = cleanup(s, name, dry=not live)
        if live or dry:
            if live:
                log(msgs)
            events += msgs
        elif msgs:
            shadow_log(name, f"（开关没开）{name} 会：" + "；".join(msgs))
    return events, {}, set()


# ---------------- Slurm 训练机补作业 ----------------
def topup_all(s, dry=False):
    """每台 Slurm 训练机补交 worker 作业 → (dry 时的说明, 出错的 {问题编号: 说明})。"""
    events, stuck = [], {}
    for name in M.names("slurm"):
        if "train" in M.MACHINES[name]["roles"] and P.SLURM.get(name) and name not in P.OFFLINE:
            ev, st = topup(s, name, dry=dry)
            events += ev
            stuck.update(st)
    return events, stuck


GPU_FILE, CPU_FILE = "worker.sbatch", "cpu_worker.sbatch"


def unclaimed(tq, device):
    """队列里没人领（free / stale）、device 是这个的任务数。"""
    return sum(1 for t in tq["tasks"] if t["state"] in ("free", "stale") and info(tq, t["key"])["spec"].get("device", "gpu") == device)


def topup_plan(tq, counts, qos):
    """→ ([(QOS, 每段时限, 补几段)], 不补的原因)。counts：{QOS: (排队上限或 None, 现有段数, 其中还在排的段数)}。
    先备好再开卡（10/31 定）：排着没开始的作业数不超过缓冲里备好、没人领的 GPU 任务数（缓冲里的训练是数据到位后才送的，
    推理只在训练所在的执行机跑、输入是那边自己训出来的），一个都没备好就不交。"""
    if not tq:
        return [], "读不到任务队列"
    if not tq.get("enabled"):
        return [], "任务队列的总开关不在"
    if tq.get("paused"):
        return [], "任务队列失败太多、暂停中"
    ready = unclaimed(tq, "gpu")
    if not ready:
        return [], "队列里没有备好、没人领的要卡的任务"
    if any(q not in counts for q in qos):
        return [], f"读不到 {next(q for q in qos if q not in counts)} 的作业数"
    room = ready - sum(counts[q][2] for q in qos)
    if room <= 0:
        return [], f"备好的任务 {ready} 个，排着的作业已有 {ready - room} 个"
    out = SL.distribute(qos, counts, room, GPU_FILE)
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
    st = SL.status(name, lambda c: run(c, m["ssh"]))
    if st is None:
        return [], {}  # 一时连不上：下次再看（ssh 不通健康检查另外报）
    counts, ncpu, free = st["counts"], st["cpu"], st["free"]
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
    for r in SL.start_workers(f"{m['repo']}/scripts/queue", subs, lambda c: run(c, m["ssh"])):
        q, t, n, ids, f = r["qos"], r["time"], r["n"], r["ids"], r["file"]
        if r["err"] is not None:
            stuck[f"topup:{name}:{q}{':cpu' if f == CPU_FILE else ''}"] = f"{m['label']} {q} 补交 {f} 出错：{r['err']}"
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
