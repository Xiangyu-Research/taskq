#!/usr/bin/env python3
"""各台机器（envs/machines.py）的健康检查（2026-10-16）：box 上 cron 每 30 分钟跑一次，发现问题推送到手机（ntfy），
同时记进状态文件，make_registry.py 把当前的问题放在实验登记表最上面（"需要处理"）。各机器的状态只在 gather() 里取，
登记表 make_registry.py 也用它。

    python3 scripts/monitor/health.py              检查、推送、更新状态
    python3 scripts/monitor/health.py --dry-run    只打印这次查到的问题，不推送、不改任何文件
    python3 scripts/monitor/health.py --test       推送一条测试消息

查什么（每项一个问题编号，同一个问题只推送一次）：
  - 连不上：ssh 不通（pueue 机器 30 分钟、Slurm 集群 1 小时后才报）。
  - pueue：常驻进程连不上；任务失败（手动停的 143 / Killed 只记进登记表，不推送）。
  - Slurm：过去 24 小时 FAILED / OUT_OF_MEMORY / NODE_FAIL 的作业（到时限停的段不算）。
  - 训练卡住：Slurm 在跑的作业、pueue 在跑的训练任务，开始超过 1 小时、训练日志 90 分钟没有新 epoch。
  - 拉回：box 上在等的任务既没训完、也不在那台机器上排队 / 训练；或者训完 90 分钟了还没拉回。
  - 同一个任务两处：同一个任务在两台以上机器上排队或在训。两边都已经开训（或有段间的进度）算紧急。
  - 训练机的卡空了：pueue 训练机（角色 train）上有卡的组里没有排着的训练任务（只剩在训的一个，或者全空），这次也没领到新的。
  - 盘：表里 disk 一项（lab /mnt/disk1 剩不到 40 GB 提醒、15 GB 紧急；box 数据盘剩不到 100 GB）。
  - GPU 使用记录（表里 gpu_log 的机器，scripts/monitor/gpu_log.sh）10 分钟没有新记录；别的机器上的代码 2 小时没快进到 main（auto_refresh.sh 写的 heads.tsv）。
  - pueue 机器 24 小时内重启过（scripts/queue/on_boot.sh 放回了哪些任务）；看板有结果等确认后发布（publish.py --auto 留下的）。
提醒（10/21 起）：每个问题推送一次，用 scripts/monitor/plain.py 的白话（出了什么事、影响什么），同一次检查查到几件并成一条；
  恢复了不推送。22:00–08:00 只即时推送紧急的，其余在 08:00 后的第一次检查时推。检查本身在不在跑，看每 2 小时的进度推送（report.py）。
文件（envs/policy.py 的 OUTPUTS）：topic（ntfy 频道名，不进仓库）、health 下的 state.json（问题和推送记录，
  make_registry.py 读）、health.log（每次有变化记一行）。
每次检查后还做两件事（10/18 起）：
  - scripts/queue/refill.py：清理 lab 上用不着的数据；送完任务后给 DICC 补交 worker 作业。做了什么记在 ~/refill.log。
  - scripts/queue/feed.py：送任务的唯一入口（10/31）。SITES 里 relay=True 的执行机交给 scripts/queue/relay.py（领任务、传数据、
    拉回，记在 ~/relay.log；停住等人处理的报成问题），其余往缓冲送任务、交还，记在 ~/feed.log。
  - scripts/queue/advance.py：box 上按每个任务的状态排下一步（拉回 → 每个任务的步骤 → 实验一级的步骤）；失败停住的报成问题。
"""
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "scripts", "queue"))
import cfg  # noqa: E402
cfg.use_config()
TQS = cfg.state("taskq", expand=False)
sys.path.insert(0, os.path.join(REPO, "scripts", "sites"))
import machines as M  # noqa: E402
import policy  # noqa: E402
import experiments as E  # noqa: E402  scripts/queue/experiments.py：实验表
import spec  # noqa: E402  scripts/queue/spec.py：任务说明、各机器上的收集
import taskq  # noqa: E402  scripts/queue/taskq.py：总队列（box）和执行机的缓冲（10/31）
import plain as PL  # noqa: E402  scripts/monitor/plain.py：推送的白话说法
import common as C  # noqa: E402  scripts/queue/common.py
import slurm as SL  # noqa: E402  scripts/sites/slurm.py
from common import write_json  # noqa: E402

OUT = getattr(policy, "OUTPUTS", {})
HDIR = os.path.expanduser(OUT.get("health", "~/.cache/taskq-health"))
NAME = OUT.get("name", "taskq")
STATE = os.path.join(HDIR, "state.json")
CODE_HEADS = os.path.expanduser(OUT.get("heads", "~/.cache/taskq/code/heads.tsv"))  # scripts/monitor/auto_refresh.sh 写
LOG = os.path.join(HDIR, "health.log")
TOPIC = os.path.expanduser(OUT.get("topic", "~/.config/taskq-health/ntfy_topic"))
H_PENDING = os.path.join(HDIR, "board_pending.txt")  # scripts/dashboard/publish.py --auto 写
NIGHT = (22, 8)  # 22:00–08:00 只推紧急的


def stopped(t):
    """手动停的（pueue kill，或 SIGTERM 让 run_task.sh 退出 143）：只记进登记表，不推送。"""
    return t.get("result") == "Killed" or t.get("code") == 143


LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")

SLURM_PROBE = ("squeue -u $USER -h -r -o '%j|%i|%T|%M|%r|%y|%P|%o'; echo '=====';"
               " sacct -u $USER -X -n -P -S now-24hours -s F,OOM,NF,BF,DL -o JobID,JobName,State,End,ExitCode")


def is_worker_job(row):
    return SL.kind_of(row[7]) == "gpu"  # squeue 的一行：交的脚本是 worker.sbatch（CPU worker 不算；作业名每次随机，不按名字认）


def data_candidates(queue=()):
    """训练要用的数据（相对数据目录）：实验表里所有任务、加上队列里的任务（queue：taskq 状态里的任务列表）的 data。"""
    out = {p for t in E.tasks().values() for p in t["spec"].get("data") or []}
    out |= {p for t in queue for p in (t.get("spec") or {}).get("data") or []}
    return sorted(out)


GPU_PROBE = r"""timeout 20 nvidia-smi --query-gpu=index,uuid,utilization.gpu,memory.used --format=csv,noheader,nounits 2>/dev/null | sed 's/^/GPU|/'
timeout 20 nvidia-smi --query-compute-apps=gpu_uuid,pid,used_memory --format=csv,noheader,nounits 2>/dev/null | while IFS=, read u p m; do
  echo "APP|$u|$p|$m|$(ps -o args= -p $p 2>/dev/null | cut -c1-400)"; done"""


def gpus_of(lines):
    """探查里的 GPU|、APP| 行 → [dict(gpu=编号, uuid, util, mem, apps=[dict(pid, mem, args)])]。"""
    gpus = {}
    for ln in lines:
        p = [x.strip() for x in ln.split("|", 4)]
        if p[0] == "GPU" and len(p) >= 2 and p[1].count(",") >= 3:
            idx, uuid, util, mem = [x.strip() for x in p[1].split(",")][:4]
            gpus[uuid] = dict(gpu=idx, uuid=uuid, util=util, mem=mem, apps=[])
        elif p[0] == "APP" and len(p) >= 5 and p[1] in gpus:
            gpus[p[1]]["apps"].append(dict(pid=p[2], mem=p[3], args=p[4]))
    return list(gpus.values())


def probe_script(m, paths=()):
    """pueue 机器上要查的东西（按表里的字段），每行"项|值"。paths：训练机上要看在不在的数据（相对数据目录）。"""
    lines = ['echo "BOOT|$(uptime -s)|$(tail -1 ~/on_boot.log 2>/dev/null)"', GPU_PROBE]
    if m["disk"]:
        lines.append(f'echo "DISK|$(df -B1 --output=avail {m["disk"][0]} | tail -1 | tr -d \' \')"')
    if m["gpu_log"]:
        lines.append('echo "GPULOG|$(tail -1 ~/gpu_usage/gpu_$(date +%Y-%m).csv 2>/dev/null | cut -d, -f1)"')
    if "train" in m["roles"] and paths:  # 训练要用的数据哪些已经在了（scripts/queue/refill.py 判断要不要先传）
        lines.append(C.have_script(paths, m["data"]).rstrip("\n"))
    return "\n".join(lines) + "\n"


def sh(cmd, inp=None, timeout=120):
    """本机跑命令（列表）→ (退出码, 标准输出；出错时没有输出就给标准错误)。"""
    rc, out, err = C.run(cmd, timeout=timeout, input=inp)
    return rc, out if rc == 0 or out else err


def run_on(m, script, timeout=120):
    remote = f"timeout -k 10 {max(timeout - 15, 10)} bash -s"
    return sh(["bash", "-s"] if m["ssh"] is None else C.SSH + [m["ssh"], remote], inp=script, timeout=timeout)


# ---------------- 收集 ----------------
def minutes(elapsed):
    """squeue 的 %M（[D-]HH:MM:SS 或 MM:SS）→ 分钟。"""
    d, _, hms = elapsed.rpartition("-")
    p = [int(x) for x in hms.split(":")]
    while len(p) < 3:
        p.insert(0, 0)
    return int(d or 0) * 1440 + p[0] * 60 + p[1] + p[2] / 60


def code_heads():
    """各台机器的代码在哪个提交（auto_refresh.sh 快进后写的 heads.tsv）和 GitHub 上的 main → dict(main, heads={机器: 提交号})。"""
    rc, main = sh(["git", "-C", REPO, "rev-parse", "HEAD"])
    heads = {}
    try:
        for line in open(CODE_HEADS):
            name, _, head = (line.rstrip("\n").split("\t") + ["", ""])[:3]
            heads[name] = head
    except OSError:
        pass
    return dict(main=main.strip() if rc == 0 else "", heads=heads)


def gather(remote=True):
    """各机器的状态（只读）。remote=False：只看本机（不连别的机器）。"""
    s = dict(now=datetime.now(), ok={}, why={}, probe={}, gpus={}, slurm={}, src={}, taskq={}, code=code_heads())
    here = [n for n in M.MACHINES if (remote or M.MACHINES[n]["ssh"] is None) and n not in policy.OFFLINE]
    for n in set(M.MACHINES) & set(policy.OFFLINE):
        s["ok"][n], s["why"][n] = False, f"暂停检查：{policy.OFFLINE[n]}"
    pq = [n for n in M.names("pueue") if n in here]
    rc, out = sh([os.path.join(REPO, "scripts", "queue", "qs"), "--json"] + ([] if remote else pq), timeout=180)
    s["pueue"] = json.loads(out) if rc == 0 else {n: {"error": out} for n in pq}
    s["records"] = C.sync_records(s["pueue"], C.load_records())  # 任务记录（scripts/queue/common.py），main() 里写回
    just = os.environ.get("PHD_TASK_DONE")  # box 的任务做完后马上调用检查时设上（它的标签）：这个任务这一次算做完（2026-10-24）
    for t in (s["pueue"].get(M.hub()) or {}).get("tasks", []) if just else []:
        if t["label"] == just and t["status"] == "Running":
            t.update(status="Success", result="Success", code=0)
    # 总队列在 box（10/31）：收集训练状态、查训练机上的数据都要用队列里的任务；执行机的缓冲是其中一部分的副本
    hub = M.hub()
    s["taskq"][hub] = taskq.local_status()
    queue = list((s["taskq"][hub] or {}).get("tasks", []))
    order = sorted(here, key=lambda n: M.MACHINES[n]["scheduler"] != "slurm")
    for name in order:
        m = M.MACHINES[name]
        if m["scheduler"] == "slurm":
            rc, out = run_on(m, SLURM_PROBE)
            ok = rc == 0 and "=====" in out
            if ok:
                q, a = out.split("=====", 1)
                s["slurm"][name] = dict(squeue=[ln.split("|", 7) for ln in q.splitlines() if ln.count("|") >= 7],  # 作业名、号、状态、已跑、原因、Nice、分区、脚本
                                        sacct=[ln.split("|") for ln in a.splitlines() if ln.count("|") == 4])
                if "train" in m["roles"]:
                    s["taskq"][name] = taskq.remote_status(name)
                    seen = {t["key"] for t in queue}
                    queue += [t for t in (s["taskq"][name] or {}).get("tasks", []) if t["key"] not in seen]
        else:
            script = probe_script(m, data_candidates(queue))
            rc, out = run_on(m, script)
            have = C.parse_have(out) if C.HAVE_END in script else set()  # 输出被截断：这次算没读到，不当成数据不在
            ok = rc == 0 and "BOOT|" in out and have is not None
            if ok:
                lines = out.splitlines()
                s["probe"][name] = dict(ln.split("|", 1) for ln in lines if "|" in ln and not ln.startswith(("GPU|", "APP|", "HAVE|")))
                if C.HAVE_END in script:
                    s["probe"][name]["HAVE"] = " ".join(sorted(have))
                s["gpus"][name] = gpus_of(lines)
        s["ok"][name] = ok
    specs = spec.task_specs(queue)
    for name in order:  # 各机器上每个任务的训练状态（spec.collect）
        m = M.MACHINES[name]
        if s["ok"][name]:
            r = spec.collect(specs, M.expand(m["data"])) if m["ssh"] is None else spec.remote_collect(m["ssh"], specs, m["data"])
            if r:
                s["src"][name] = r
            elif m["ssh"] is not None:  # 收集超时（多半是那边的存储挂住，10/31）：这次检查后面的步骤都当它连不上，不再去连
                s["ok"][name] = False
                s["why"][name] = "收集训练状态超时或出错（多半是那边的存储挂住）"
                if name != hub:
                    s["taskq"].pop(name, None)
    s["board_pending"] = open(H_PENDING).read().split() if os.path.exists(H_PENDING) else []
    return s


def task_info(s, key):
    """各机器上这个任务：(是否训完, 训完的 epoch 数, 最后一个 epoch 的时间)。"""
    out = {}
    for host, data in s["src"].items():
        f = data["tasks"].get(key)
        if f:
            ends = [r["end"] for r in f["runs"] if r["end"]]
            out[host] = (f["final"], max([r["epochs_done"] for r in f["runs"]] or [0]),
                         datetime.strptime(max(ends), "%Y-%m-%d %H:%M:%S") if ends else None)
    return out


# ---------------- 检查 ----------------
def check(s):
    """→ {问题编号: dict(msg, level=urgent|normal|info, kind=event|state, after=持续多少分钟才算)}。"""
    now, P = s["now"], {}
    L = lambda n: M.MACHINES[n]["label"]  # noqa: E731

    def add(pid, msg, level="normal", kind="state", after=0):
        P[pid] = dict(msg=msg, level=level, kind=kind, after=after)

    claims = {}  # key → {机器（pueue 的是"机器:组"，和同一台的缓冲分开）: [描述]}（排队或在训）
    busy = {}  # key → {机器（同上）}：在训，或段间等待（有进度、下一段在排队）

    def claim(key, name, desc):
        claims.setdefault(key, {}).setdefault(name, []).append(desc)

    for name, m in M.MACHINES.items():
        if not s["ok"].get(name) and name not in policy.OFFLINE:
            add(f"down:{name}", f"{L(name)} 连不上（{(s.get('why') or {}).get(name) or 'ssh ' + str(m['ssh'])}）",
                after=60 if m["scheduler"] == "slurm" else 30)
    for name, d in (s.get("src") or {}).items():  # 存储挂住、读不动的任务（spec.collect 的 stuck，11/1）
        if d.get("stuck"):
            add(f"unread:{name}", f"{L(name)} 上 {len(d['stuck'])} 个任务的文件读不动（多半是存储挂住），先不分、不交还："
                                 f"{'、'.join(d['stuck'][:5])}{' …' if len(d['stuck']) > 5 else ''}", after=60)
    code = s.get("code") or {}
    for name, head in (code.get("heads") or {}).items():  # 连不上的（提交号空着）上面已经报了
        if head and code.get("main") and head != code["main"] and name in M.MACHINES and name not in policy.OFFLINE:
            add(f"code:{name}", f"{L(name)} 的代码停在 {head[:7]}，GitHub 上是 {code['main'][:7]}"
                f"（{M.MACHINES[name].get('repo')} 快进不了？auto_refresh.sh 每 30 分钟快进一次）", after=120)
    # --- 总队列（box，10/31）---
    hub = M.hub()
    ctq = s.get("taskq", {}).get(hub)
    if hub in s.get("taskq", {}) and ctq is None:
        add(f"taskq:{hub}", f"{L(hub)} 读不到总队列（{TQS}/）")
    elif ctq and not ctq.get("enabled"):
        add(f"tqoff:{hub}", f"{L(hub)} 的总队列关着（{TQS}/ENABLED 不在），不会往执行机送任务", after=60)
    elif ctq:
        for t in ctq["tasks"]:
            if t["state"] == "blocked" and not t.get("assigned"):
                add(f"tqblocked:{t['key']}", f"总队列任务 {t['key']} {t['note']}，不会开始")
    # --- 执行机的缓冲（DICC、lab；10/31）：排队、在训都看缓冲（作业名、pueue 任务不对应训哪个任务）---
    for name in [n for n in s.get("taskq", {}) if n != hub]:
        tq = s["taskq"][name]
        if name in s["slurm"] and tq is None:
            add(f"taskq:{name}", f"{L(name)} 读不到任务队列（{TQS}/）")
        if tq and tq["enabled"]:
            for t in tq["tasks"]:
                key, st = t["key"], t["state"]
                if st in ("free", "stale", "claimed", "busy"):
                    claim(key, name, "队列" if st in ("free", "stale") else f"在训（{t['note']}）")
                if st in ("claimed", "busy"):
                    busy.setdefault(key, set()).add(name)
                ages = [a for a in (t["log_age"], t.get("task_log_age"), t.get("out_age")) if a is not None]
                if st == "claimed" and t["since"] and time.time() - t["since"] > 3600 and (not ages or min(ages) > 5400):
                    add(f"stuck:{name}:{key}", f"{L(name)} {'作业' if name in s['slurm'] else 'worker'} {t['worker']} 领了 {key} {(time.time() - t['since']) / 3600:.1f} 小时，"
                        f"{'日志和结果文件夹 90 分钟没有变化' if ages else '还没有日志'}")
                if st == "blocked":  # 提前排的推理、评测：前一步失败了或不在队列里（2026-10-25）
                    add(f"tqblocked:{key}", f"{L(name)} 队列任务 {key} {t['note']}，不会开始")
                if t["failures"] and st != "done":  # 后来训完了就不再报
                    add(f"tqfail:{key}", f"{L(name)} 队列任务 {key} 失败 {t['failures']} 次"
                        + (f"，不再领（看 {TQS}/failures/）" if st == "failed" else ""))
    # --- Slurm ---
    for name, d in s["slurm"].items():
        tq = s.get("taskq", {}).get(name)
        if tq and tq["enabled"]:
            # 在跑的作业比领了任务的作业多，且跑了 30 分钟以上：有作业空着卡
            run = [r for r in d["squeue"] if is_worker_job(r) and r[2] == "RUNNING" and minutes(r[3]) >= 30]
            workers = {t["worker"] for t in tq["tasks"] if t["state"] == "claimed"}
            if len(run) > len(workers):
                add(f"tqidle:{name}", f"{L(name)} {len(run)} 个作业在跑超过 30 分钟，只有 {len(workers)} 个领到了任务，有卡空着"
                    "（看作业的 .out：可能在等刚停下的任务、队列暂停或出错）")
            # 队列里还有没人训的任务，但 Slurm 上已经没有排着或在跑的作业了：refill.py 应该已经补交了 worker.sbatch
            left = sum(t["state"] in ("free", "stale") for t in tq["tasks"])
            if left and not any(is_worker_job(r) for r in d["squeue"]):
                add(f"tqorphan:{name}", f"{L(name)} 任务队列里还有 {left} 个任务没人训，但已经没有排着或在跑的作业"
                    f"（envs/policy.py 的 AUTO[\"{name}\"] 关着？看 ~/refill.log）", after=60)
            for jid, jname, st, end, code in d["sacct"]:
                add(f"{name}:{jid}", f"{L(name)} 作业 {jid} {jname} {st}（{end}，退出码 {code}）", kind="event")
            continue
        for jid, jname, st, end, code in d["sacct"]:
            add(f"{name}:{jid}", f"{L(name)} 作业 {jid} {jname} {st}（{end}，退出码 {code}）", kind="event")

    # --- pueue ---
    waits = []
    for name in M.names("pueue"):
        if not s["ok"].get(name):
            continue
        d = s["pueue"].get(name) or {"error": "没有结果"}
        if "error" in d:
            add(f"pueued:{name}", f"{L(name)} 的 pueue 没有响应：{d['error'][:120]}")
            continue
        for t in d["tasks"]:
            r = C.identify(name, t, s.get("records") or [])
            kind, key = (r["step"], r["key"]) if r else (None, None)
            tname = t["label"] or f"#{t['id']}"
            st = t["status"]
            if kind == "train":
                if st in LIVE:
                    claim(key, f"{name}:{t['group']}", f"任务 {t['id']} {tname}（{st}）")
                if st == "Running":
                    busy.setdefault(key, set()).add(f"{name}:{t['group']}")
                    info = task_info(s, key).get(name)
                    start = C.local_time(t["start"])
                    if start and now - start > timedelta(minutes=60) and \
                            (not info or not info[2] or now - max(info[2], start) > timedelta(minutes=90)):
                        add(f"stuck:{name}:{t['id']}", f"{L(name)} 任务 {t['id']} {tname}：90 分钟没有新 epoch（在等卡，或训练卡住）")
            if kind == "fetch" and st == "Running":
                waits.append((name, t, key, r.get("src")))
            if st in LIVE or st == "Success":
                continue
            end = C.local_time(t["end"])
            if end and now - end > timedelta(hours=24):
                continue
            add(f"pueue:{name}:{t['id']}:{t['end']}", f"{L(name)} 任务 {t['id']} {tname} {st}",
                level="info" if stopped(t) else "normal", kind="event")

    # --- 拉回 ---
    for host, t, key, src in waits:
        start = C.local_time(t["start"])
        if not start or now - start < timedelta(minutes=60) or src not in M.MACHINES:
            continue
        fin = task_info(s, key).get(src)
        if fin and fin[0]:
            if fin[2] and now - fin[2] > timedelta(minutes=90):
                add(f"fetch:{key}", f"{key} 在 {L(src)} 上训完了，{L(host)} 还没拉回（任务 {t['id']}）")
        elif s["ok"].get(src) and src not in {n.split(":")[0] for n in claims.get(key, {})}:
            add(f"orphan:{key}", f"{L(host)} 在等 {key} 在 {L(src)} 上训完再拉回（任务 {t['id']}），但它不在 {L(src)} 上排队、训练")

    # --- 同一个任务两处 ---
    for key in sorted(k for k, v in claims.items() if len(v) >= 2):
        both = len(busy.get(key, ())) >= 2
        add(f"dup:{key}", f"同一个任务在两处：{key}，" + "；".join(f"{L(n.split(':')[0])}{' ' + n.split(':')[1] if ':' in n else ''} {'、'.join(v)}" for n, v in claims[key].items())
            + ("（两边都在训或有进度，同步时会互相覆盖存档）" if both else "（还没同时开训，取消一边前要确认作业号）"),
            level="urgent" if both else "normal")

    # --- 盘、GPU 记录、代码同步、重启 ---
    for name, pr in s["probe"].items():
        m = M.MACHINES[name]
        if m["disk"] and pr.get("DISK", "").strip().isdigit():
            free, warn, urgent = int(pr["DISK"]) / 1e9, m["disk"][1], m["disk"][2]
            if free < warn:
                add(f"disk:{name}", f"{L(name)} {m['disk'][0]} 只剩 {free:.0f} GB", level="urgent" if urgent and free < urgent else "normal")
        if m["gpu_log"]:
            last = pr.get("GPULOG", "")
            try:
                age = now - datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                age = None
            if age is None or age > timedelta(minutes=10):
                add(f"gpulog:{name}", f"{L(name)} GPU 使用记录停了（最后一条 {last or '没有'}）")
        boot, _, line = pr.get("BOOT", "").partition("|")
        try:
            t = datetime.strptime(boot, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if now - t < timedelta(hours=24):
            add(f"boot:{name}:{boot}", f"{L(name)} {t:%m-%d %H:%M} 重启过：{line[20:] or '没有开机记录（on_boot.sh 没跑？）'}", kind="event")
    if s.get("board_pending"):
        add("board", f"看板有新结果等确认后发布（{len(s['board_pending'])} 个文件：{'、'.join(s['board_pending'][:4])}）")

    return P


# ---------------- 推送和状态 ----------------
def push(msg, title=f"{NAME} 作业", priority="default", tags=""):
    topic = open(TOPIC).read().strip()
    q = urllib.parse.urlencode({"title": title, "priority": priority, "tags": tags})
    req = urllib.request.Request(f"https://ntfy.sh/{topic}?{q}", data=msg.encode(), method="POST")
    urllib.request.urlopen(req, timeout=30).read()


def night(now):
    return now.hour >= NIGHT[0] or now.hour < NIGHT[1]


def update(P, now, dry=False):
    """和上次的状态比：新出现、持续够久的推送（白话，plain.py；同一次查到几件并成一条）；夜里只推紧急的，其余早上第一次检查时推。"""
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    old = st.get("problems", {})
    msgs, log = [], []
    cur = {}
    for pid, p in P.items():
        o = old.get(pid, {})
        p["first"] = o.get("first", now.strftime("%Y-%m-%d %H:%M"))
        p["notified"] = o.get("notified", False)
        age = (now - datetime.strptime(p["first"], "%Y-%m-%d %H:%M")).total_seconds() / 60
        p["active"] = age >= p["after"]
        if p["active"] and not p["notified"] and p["level"] != "info" and (p["level"] == "urgent" or not night(now)):
            msgs.append((pid, p))
        if pid not in old:
            log.append(f"新问题 {pid}：{p['msg']}")
        cur[pid] = p
    for pid in old:
        if pid not in cur:
            log.append(f"消失 {pid}")
    sent_ok = True
    if not dry:
        try:
            if msgs:
                urgent = any(p["level"] == "urgent" for _, p in msgs)
                texts = [PL.problem(pid, p) for pid, p in msgs]
                push(texts[0] if len(texts) == 1 else "\n".join("- " + t for t in texts),
                     title=(f"{NAME}：紧急" if urgent else f"{NAME}：需要处理") + (f"（{len(texts)} 件）" if len(texts) > 1 else ""),
                     priority="high" if urgent else "default", tags="rotating_light" if urgent else "warning")
                for _, p in msgs:
                    p["notified"] = True
        except Exception as e:  # noqa: BLE001  推送失败：下次再推
            sent_ok = False
            log.append(f"推送失败：{e}")
        for k in ("digest", "digest_prune_pos", "resolved_overnight"):
            st.pop(k, None)
        st.update(problems=cur, checked=now.strftime("%Y-%m-%d %H:%M"))
        write_json(STATE, st)
        if log:
            with open(LOG, "a") as f:
                f.writelines(f"{now:%F %T} {x}\n" for x in log)
    return [p for _, p in msgs], sent_ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--test", action="store_true")
    a = ap.parse_args()
    if a.test:
        push("测试：健康检查的推送能收到", tags="tada")
        print("已推送测试消息")
        return
    s = gather()
    if not a.dry_run:
        s["records"] = C.update_records(lambda recs: C.sync_records(s["pueue"], recs))
    P = check(s)
    # 这两步出错（包括 import 出错）不能让健康检查本身停掉（报警、登记表都靠它）：出错时报出来，其余照常
    try:
        import shift  # scripts/queue/shift.py：按时段开关 pueue 组（envs/policy.py 的 SHIFT）
        shift_msgs = shift.run(s, dry=a.dry_run)
    except Exception as e:  # noqa: BLE001
        shift_msgs, shift_err = [], f"scripts/queue/shift.py 出错：{type(e).__name__}: {e}"[:300]
    else:
        shift_err = None
    try:
        import refill  # scripts/queue/refill.py：清理 lab 上用不着的数据
        events, stuck, _ = refill.run_all(s, dry=a.dry_run)
    except Exception as e:  # noqa: BLE001
        events, stuck = [], {"refill:error": f"scripts/queue/refill.py 出错：{type(e).__name__}: {e}"[:300]}
    events = shift_msgs + events
    if shift_err:
        stuck["shift:error"] = shift_err
    try:
        import feed  # scripts/queue/feed.py：往执行机的缓冲送任务（10/31）
    except Exception as e:  # noqa: BLE001
        feed, stuck["feed:error"] = None, f"scripts/queue/feed.py 出错：{type(e).__name__}: {e}"[:300]

    def run_feed(relay):
        if feed is None:
            return
        try:
            ev, st = feed.run_all(s, dry=a.dry_run, relay=relay)
            events.extend(ev)
            stuck.update(st)
        except Exception as e:  # noqa: BLE001
            stuck["feed:error"] = f"scripts/queue/feed.py 出错：{type(e).__name__}: {e}"[:300]

    run_feed(relay=True)  # 送任务只从 feed.py 进：SITES 里 relay=True 的由它转给 relay.py
    try:
        import advance  # scripts/queue/advance.py：box 上按状态排下一步
        done, stuck2 = advance.run(s, dry=a.dry_run)
    except Exception as e:  # noqa: BLE001
        done, stuck2 = [], {"advance:error": f"scripts/queue/advance.py 出错：{type(e).__name__}: {e}"[:300]}
    if not a.dry_run and any(d.startswith("任务队列") for d in done):
        # 刚加进总队列的（如评测）这一次就送到执行机，不等下一次检查（10/25、10/31）
        # 第一遍送过的任务、排过的传输都要重读，不然会再送一遍、再传一遍（10/31 同一份数据传了两次）
        s["taskq"][M.hub()] = taskq.local_status() or s["taskq"].get(M.hub())
        rc, out = sh([os.path.join(REPO, "scripts", "queue", "qs"), "--json"], timeout=180)
        if rc == 0:
            s["pueue"] = json.loads(out)
        for name in feed.sites() if feed else []:
            if s["ok"].get(name) and name in s["taskq"]:
                s["taskq"][name] = taskq.remote_status(name)
        run_feed(relay=False)
    try:
        # 缓冲有了任务、而那边没有排着的作业时补交 worker（送任务之后再补，10/31）
        for name in s.get("taskq", {}):
            if name != M.hub():
                s["taskq"][name] = taskq.remote_status(name) or s["taskq"][name]
        ev2, st3 = refill.topup_all(s, dry=a.dry_run)
        events.extend(ev2)
        stuck.update(st3)
    except Exception as e:  # noqa: BLE001
        stuck["refill:error"] = f"scripts/queue/refill.py 出错：{type(e).__name__}: {e}"[:300]
    for pid, msg in {**stuck, **stuck2}.items():
        # 拉回任务刚拷完、还没退出时会被算成"在等已经在 box 上的任务"：持续到下一次检查才报
        P[pid] = dict(msg=msg, level="normal", kind="state", after=0)
    update(P, s["now"], dry=a.dry_run)
    if a.dry_run:
        for pid, p in sorted(P.items()):
            wait = f"/持续 {p['after']} 分钟才报" if p["after"] else ""
            print(f"[{p['level']}/{p['kind']}{wait}] {pid}\n    {p['msg']}")
        print("\n送任务、清理、补作业、按时段开关：" + "；".join(events))
        print("box 按状态推进：" + ("；".join(done) or "没有要排的（影子模式时看 ~/advance.log）"))


if __name__ == "__main__":
    main()
