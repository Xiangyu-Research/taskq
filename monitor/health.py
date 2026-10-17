#!/usr/bin/env python3
"""各台机器（envs/machines.py）的健康检查（2026-10-16）：box 上 cron 每 30 分钟跑一次，发现问题推送到手机（ntfy），
同时记进状态文件，make_registry.py 把当前的问题放在实验登记表最上面（"需要处理"）。

    python3 scripts/monitor/health.py              检查、推送、更新状态
    python3 scripts/monitor/health.py --dry-run    只打印这次查到的问题，不推送、不改任何文件
    python3 scripts/monitor/health.py --test       推送一条测试消息

查什么（每项一个问题编号，同一个问题只推送一次）：
  - 连不上：ssh 不通（pueue 机器 30 分钟、Slurm 集群 1 小时后才报）。
  - pueue：常驻进程连不上；任务失败或因依赖失败没跑（训满 N 个 epoch 停的退出码 75、手动停的 143 / Killed 不算）。
  - Slurm：过去 24 小时 FAILED / OUT_OF_MEMORY / NODE_FAIL 的作业（到时限停的段不算）。
  - 训练卡住：Slurm 在跑的作业、pueue 在跑的训练任务，开始超过 1 小时、训练日志 90 分钟没有新 epoch。
  - 拉回：box 上在等的折既没训完、也不在那台机器上排队 / 训练；或者训完 90 分钟了还没拉回。
  - 同一折两处：同一折在两台以上机器上排队或在训。两边都已经开训（或有段间的进度）算紧急。
  - 训练机的卡快空了：pueue 训练机（角色 train）上有卡的组里，排着的训练任务没有了（只剩在训的一个，或者全空），该补任务了。
  - 盘：表里 disk 一项（lab /mnt/disk1 剩不到 40 GB 提醒、15 GB 紧急；box 数据盘剩不到 100 GB）。
  - GPU 使用记录（表里 gpu_log 的机器，scripts/monitor/gpu_log.sh）10 分钟没有新记录；代码自动同步（角色 autopull）2 小时没成功。
  - pueue 机器 24 小时内重启过（scripts/queue/on_boot.sh 放回了哪些任务）；看板有结果等确认后发布（publish.py --auto 留下的）。
提醒：22:00–08:00 只即时推送紧急的，其余在 08:00 后的第一次检查时和每日汇总一起发（每天一条，没有问题也发，说明检查本身在跑）。
  事件类（任务失败）推送一次；状态类（盘、卡住、连不上……）恢复后再推送一次"已恢复"。
文件：~/.config/phd-health/ntfy_topic（ntfy 频道名，不进仓库）、~/.cache/phd-health/state.json（问题和推送记录，
  make_registry.py 读）、~/.cache/phd-health/health.log（每次有变化记一行）。
训练机的卡快空了时自动从待办里补任务（scripts/queue/refill.py），补了什么作为事件推送。
另外给每台训练机（角色 train）写 ~/.cache/phd/claimed_elsewhere.txt：别的机器在训的折、别的 Slurm 集群上排着的折，
  每行"key<TAB>机器"，开训前读（scripts/queue/train_task.sh、research/RQ1/nnunet/train_folds.sbatch），防止同一折两处训练。
"""
import argparse
import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "research", "RQ1", "nnunet"))
sys.path.insert(0, os.path.join(REPO, "envs"))
sys.path.insert(0, os.path.join(REPO, "scripts", "queue"))
import machines as M  # noqa: E402
import make_registry as mr  # noqa: E402  收集各机器训练状态的函数、SPEC、DS

STATE = os.path.expanduser("~/.cache/phd-health/state.json")
LOG = os.path.expanduser("~/.cache/phd-health/health.log")
TOPIC = os.path.expanduser("~/.config/phd-health/ntfy_topic")
PRUNE_LOG = os.path.expanduser("~/archive_prune.log")
H_PENDING = os.path.expanduser("~/.cache/phd-health/board_pending.txt")  # scripts/dashboard/publish.py --auto 写
CLAIMS = "~/.cache/phd/claimed_elsewhere.txt"
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]
NIGHT = (22, 8)  # 22:00–08:00 只推紧急的
PAUSED_OK = ("Failed (75)",)  # train_task.sh 训满 N 个 epoch 停了、还没训完
STOPPED = ("Failed (143)", "Killed")  # 手动停的：只记进登记表，不推送
LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")

SLURM_PROBE = ("squeue -u $USER -h -r -o '%j|%i|%T|%M|%r|%y'; echo '=====';"
               " sacct -u $USER -X -n -P -S now-24hours -s F,OOM,NF,BF,DL -o JobID,JobName,State,End,ExitCode")


def probe_script(m):
    """pueue 机器上要查的东西（按表里的字段），每行"项|值"。"""
    lines = ['echo "BOOT|$(uptime -s)|$(tail -1 ~/on_boot.log 2>/dev/null)"']
    if m["disk"]:
        lines.append(f'echo "DISK|$(df -B1 --output=avail {m["disk"][0]} | tail -1 | tr -d \' \')"')
    if m["gpu_log"]:
        lines.append('echo "GPULOG|$(tail -1 ~/gpu_usage/gpu_$(date +%Y-%m).csv 2>/dev/null | cut -d, -f1)"')
    if "train" in m["roles"]:  # 已经有哪些预处理好的数据集（scripts/queue/refill.py 判断要不要先传数据）
        lines.append(f'echo "PREP|$(ls {m["data"]}/nnUNet/preprocessed 2>/dev/null | tr \'\\n\' \' \')"')
    if "autopull" in m["roles"]:
        lines.append('echo "GITOK|$(cat ~/.cache/phd/git_pull_ok 2>/dev/null)"')
        lines.append('echo "GITLOG|$(tail -1 ~/git_pull.log 2>/dev/null)"')
    return "\n".join(lines) + "\n"


def sh(cmd, inp=None, timeout=120):
    try:
        r = subprocess.run(cmd, input=inp, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout
    except Exception as e:  # noqa: BLE001
        return 1, str(e)


def run_on(m, script, timeout=120):
    return sh(["bash", "-s"] if m["ssh"] is None else SSH + [m["ssh"], "bash -s"], inp=script, timeout=timeout)


# ---------------- 收集 ----------------
def job_keys():
    """Slurm 作业名 → key（数据集/训练器/fold），按 make_registry.SPEC。"""
    out = {}
    for _, _, _, dsid, tr, folds, tag in mr.SPEC:
        for f in folds:
            if tag:
                out[f"rq1_{tag}_f{f}" if f != "all" else f"rq1_{tag}"] = f"{mr.DS[dsid]}/{tr}/{f}"
    return out


def minutes(elapsed):
    """squeue 的 %M（[D-]HH:MM:SS 或 MM:SS）→ 分钟。"""
    d, _, hms = elapsed.rpartition("-")
    p = [int(x) for x in hms.split(":")]
    while len(p) < 3:
        p.insert(0, 0)
    return int(d or 0) * 1440 + p[0] * 60 + p[1] + p[2] / 60


def task_key(cmd):
    """pueue 任务命令 → (种类, key)：train_task.sh <数据集> <fold> [训练器]、fetch_fold.sh <机器> <模型文件夹> <fold>。"""
    cmd = cmd.replace("'", " ").replace('"', " ")
    m = re.search(r"train_task\.sh\s+(\d+)\s+(\S+)(?:\s+(?!--)(\S+))?", cmd)
    if m and int(m.group(1)) in mr.DS:
        return "train", f"{mr.DS[int(m.group(1))]}/{m.group(3) or 'nnUNetTrainer_save10'}/{m.group(2)}"
    m = re.search(r"fetch_fold\.sh\s+(\S+)\s+(\S+)/(\S+?)__\S+\s+(\S+)", cmd)
    if m:
        return "fetch:" + m.group(1), f"{m.group(2)}/{m.group(3)}/{m.group(4)}"
    return None, None


def gather():
    s = dict(now=datetime.now(), ok={}, probe={}, slurm={}, src={})
    rc, out = sh([os.path.join(REPO, "scripts", "queue", "qs"), "--json"], timeout=180)
    s["pueue"] = json.loads(out) if rc == 0 else {n: {"error": out} for n in M.names("pueue")}
    for name, m in M.MACHINES.items():
        if m["scheduler"] == "slurm":
            rc, out = run_on(m, SLURM_PROBE)
            ok = rc == 0 and "=====" in out
            if ok:
                q, a = out.split("=====", 1)
                s["slurm"][name] = dict(squeue=[ln.split("|") for ln in q.splitlines() if ln.count("|") == 5],  # 作业名、号、状态、已跑、原因、Nice
                                        sacct=[ln.split("|") for ln in a.splitlines() if ln.count("|") == 4])
        else:
            rc, out = run_on(m, probe_script(m))
            ok = rc == 0 and "BOOT|" in out
            if ok:
                s["probe"][name] = dict(ln.split("|", 1) for ln in out.splitlines() if "|" in ln)
        s["ok"][name] = ok
        if ok:
            r = mr.collect(M.expand(M.results(name))) if m["ssh"] is None else mr.remote_collect(m["ssh"], M.results(name))
            if r:
                s["src"][name] = r
    s["board_pending"] = open(H_PENDING).read().split() if os.path.exists(H_PENDING) else []
    return s


def fold_info(s, key):
    """各机器上这一折：(是否训完, 训完的 epoch 数, 最后一个 epoch 的时间)。"""
    out = {}
    for host, data in s["src"].items():
        f = data["folds"].get(key)
        if f:
            ends = [r["end"] for r in f["runs"] if r["end"]]
            out[host] = (f["final"], max([r["epochs_done"] for r in f["runs"]] or [0]),
                         datetime.strptime(max(ends), "%Y-%m-%d %H:%M:%S") if ends else None)
    return out


# ---------------- 检查 ----------------
def check(s):
    """→ ({问题编号: dict(msg, level=urgent|normal|info, kind=event|state, after=持续多少分钟才算)},
    {训练机: [(key, 在哪台机器)]，写给它的清单；这次没收集全（有训练机连不上）时不在里面})。"""
    now, P = s["now"], {}
    L = lambda n: M.MACHINES[n]["label"]  # noqa: E731

    def add(pid, msg, level="normal", kind="state", after=0):
        P[pid] = dict(msg=msg, level=level, kind=kind, after=after)

    claims = {}  # key → {机器: [描述]}（排队或在训）
    busy = {}  # key → {机器}：在训，或段间等待（有进度、下一段在排队）
    queued_slurm = {}  # key → {Slurm 机器}：排着（还没开始也算，要在别处训先取消）

    def claim(key, name, desc):
        claims.setdefault(key, {}).setdefault(name, []).append(desc)

    for name, m in M.MACHINES.items():
        if not s["ok"].get(name):
            add(f"down:{name}", f"{L(name)} 连不上（ssh {m['ssh']}）", after=60 if m["scheduler"] == "slurm" else 30)
    # --- Slurm ---
    jk = job_keys()
    for name, d in s["slurm"].items():
        for jname, jid, st, el, why, *_ in d["squeue"]:
            key = jk.get(jname)
            if not key:
                continue
            claim(key, name, f"{jid}（{st}）")
            queued_slurm.setdefault(key, set()).add(name)
            info = fold_info(s, key).get(name)
            if st == "RUNNING":
                busy.setdefault(key, set()).add(name)
                last = info[2] if info else None
                if minutes(el) > 60 and (not last or now - last > timedelta(minutes=90)):
                    add(f"stuck:{name}:{jid}", f"{L(name)} {jid}（{key}）开始 {minutes(el) / 60:.1f} 小时，"
                        f"{'训练日志 90 分钟没有新 epoch' if last else '还没有训练日志'}")
            elif st == "PENDING" and info and info[1] > 0 and not info[0]:
                busy.setdefault(key, set()).add(name)
        for jid, jname, st, end, code in d["sacct"]:
            add(f"{name}:{jid}", f"{L(name)} 作业 {jid} {jname} {st}（{end}，退出码 {code}）", kind="event")

    # --- pueue ---
    waits, train_q = [], {}
    for name in M.names("pueue"):
        if not s["ok"].get(name):
            continue
        d = s["pueue"].get(name) or {"error": "没有结果"}
        if "error" in d:
            add(f"pueued:{name}", f"{L(name)} 的 pueue 没有响应：{d['error'][:120]}")
            continue
        for t in d["tasks"]:
            kind, key = task_key(t["command"])
            tname = t["label"] or f"#{t['id']}"
            st = t["status"]
            if kind == "train":
                if st in LIVE:
                    claim(key, name, f"任务 {t['id']} {tname}（{st}）")
                if st == "Running":
                    busy.setdefault(key, set()).add(name)
                    info = fold_info(s, key).get(name)
                    start = mr_time(t["start"])
                    if start and now - start > timedelta(minutes=60) and \
                            (not info or not info[2] or now - max(info[2], start) > timedelta(minutes=90)):
                        add(f"stuck:{name}:{t['id']}", f"{L(name)} 任务 {t['id']} {tname}：90 分钟没有新 epoch（在等卡，或训练卡住）")
            if kind and kind.startswith("fetch:") and st == "Running":
                waits.append((name, t, key, kind[6:]))
            if kind == "train" and st in ("Running", "Queued"):
                train_q.setdefault((name, t["group"]), []).append((st, tname))
            if st in LIVE or st == "Success" or st in PAUSED_OK:
                continue
            end = mr_time(t["end"])
            if end and now - end > timedelta(hours=24):
                continue
            add(f"pueue:{name}:{t['id']}:{t['end']}", f"{L(name)} 任务 {t['id']} {tname} {st}",
                level="info" if st in STOPPED else "normal", kind="event")

    # --- 训练机的卡快空了 ---
    for name in M.names("pueue"):
        m = M.MACHINES[name]
        if "train" not in m["roles"] or not s["ok"].get(name) or "error" in (s["pueue"].get(name) or {"error": 1}):
            continue
        for g, (_, gpu, _) in m["groups"].items():
            if gpu is None:
                continue
            ts = train_q.get((name, g), [])
            run = [n for st, n in ts if st == "Running"]
            if not ts:
                add(f"idle:{name}:{g}", f"{L(name)} {g} 组没有训练任务，{gpu} 号卡空着，该补任务了")
            elif len(ts) == len(run):
                add(f"idle:{name}:{g}", f"{L(name)} {g} 组只剩在训的 {'、'.join(run)}，后面没有排着的训练，该补任务了")

    # --- 拉回 ---
    for host, t, key, src in waits:
        start = mr_time(t["start"])
        if not start or now - start < timedelta(minutes=60) or src not in M.MACHINES:
            continue
        fin = fold_info(s, key).get(src)
        if fin and fin[0]:
            if fin[2] and now - fin[2] > timedelta(minutes=90):
                add(f"fetch:{key}", f"{key} 在 {L(src)} 上训完了，{L(host)} 还没拉回（任务 {t['id']}）")
        elif s["ok"].get(src) and src not in claims.get(key, {}):
            add(f"orphan:{key}", f"{L(host)} 在等 {key} 在 {L(src)} 上训完再拉回（任务 {t['id']}），但它不在 {L(src)} 上排队、训练")

    # --- 同一折两处 ---
    for key in sorted(k for k, v in claims.items() if len(v) >= 2):
        both = len(busy.get(key, ())) >= 2
        add(f"dup:{key}", f"同一折在两处：{key}，" + "；".join(f"{L(n)} {'、'.join(v)}" for n, v in claims[key].items())
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
        if "autopull" in m["roles"]:
            ok = pr.get("GITOK", "").strip()
            if not ok.isdigit() or now - datetime.fromtimestamp(int(ok)) > timedelta(hours=2):
                add(f"git:{name}", f"{L(name)} 代码 2 小时没有同步成功：{pr.get('GITLOG', '')[:150] or '没有同步记录'}")
        boot, _, line = pr.get("BOOT", "").partition("|")
        try:
            t = datetime.strptime(boot, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if now - t < timedelta(hours=24):
            add(f"boot:{name}:{boot}", f"{L(name)} {t:%m-%d %H:%M} 重启过：{line[20:] or '没有开机记录（on_boot.sh 没跑？）'}", kind="event")
    if s.get("board_pending"):
        add("board", f"看板有新结果等确认后发布（{len(s['board_pending'])} 个文件：{'、'.join(s['board_pending'][:4])}）")

    # --- 写给训练机的清单：别的机器在训的折 + 别的 Slurm 集群上排着的折 ---
    out = {}
    train = M.names("train")
    if all(s["ok"].get(n) for n in train):
        for n in train:
            out[n] = sorted({(k, L(o)) for k, v in busy.items() for o in v if o != n} |
                            {(k, L(o)) for k, v in queued_slurm.items() for o in v if o != n})
    return P, out


def mr_time(x):
    """pueue 的时间（带时区、纳秒）→ 本地 naive datetime。"""
    if not x:
        return None
    return datetime.fromisoformat(x[:26] + x[-6:]).astimezone().replace(tzinfo=None)


# ---------------- 推送和状态 ----------------
def push(msg, title="PhD 作业", priority="default", tags=""):
    topic = open(TOPIC).read().strip()
    q = urllib.parse.urlencode({"title": title, "priority": priority, "tags": tags})
    req = urllib.request.Request(f"https://ntfy.sh/{topic}?{q}", data=msg.encode(), method="POST")
    urllib.request.urlopen(req, timeout=30).read()


def night(now):
    return now.hour >= NIGHT[0] or now.hour < NIGHT[1]


def update(P, now, dry=False):
    """和上次的状态比：新出现、持续够久的推送；恢复的推送"已恢复"；夜里只推紧急的，其余攒到早上的汇总。"""
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    old = st.get("problems", {})
    pending_res = st.get("resolved_overnight", [])
    msgs, log = [], []
    cur = {}
    for pid, p in P.items():
        o = old.get(pid, {})
        p["first"] = o.get("first", now.strftime("%Y-%m-%d %H:%M"))
        p["notified"] = o.get("notified", False)
        age = (now - datetime.strptime(p["first"], "%Y-%m-%d %H:%M")).total_seconds() / 60
        p["active"] = age >= p["after"]
        if p["active"] and not p["notified"] and p["level"] != "info" and (p["level"] == "urgent" or not night(now)):
            msgs.append(p)
        if pid not in old:
            log.append(f"新问题 {pid}：{p['msg']}")
        cur[pid] = p
    resolved = [o for pid, o in old.items() if pid not in cur and o.get("kind") == "state" and o.get("notified")]
    for pid, o in old.items():
        if pid not in cur:
            log.append(f"消失 {pid}")
    sent_ok = True
    if not dry:
        try:
            day = now.strftime("%Y-%m-%d")
            pending_res += [{"msg": o["msg"]} for o in resolved]
            if not night(now) and st.get("digest") != day:  # 当天第一次白天的检查：所有消息并进每日汇总
                push(digest(cur, pending_res, st.get("digest_prune_pos", 0), now), title="PhD 作业：每日汇总",
                     priority="high" if any(p["level"] == "urgent" for p in msgs) else "low", tags="clipboard")
                for p in cur.values():
                    if p["active"] and p["level"] != "info":
                        p["notified"] = True
                pending_res = []
                st["digest"] = day
                st["digest_prune_pos"] = os.path.getsize(PRUNE_LOG) if os.path.exists(PRUNE_LOG) else 0
            else:
                for p in msgs:
                    push(p["msg"], title="PhD 作业：紧急" if p["level"] == "urgent" else "PhD 作业",
                         priority="high" if p["level"] == "urgent" else "default", tags="rotating_light" if p["level"] == "urgent" else "warning")
                    p["notified"] = True
                if pending_res and not night(now):
                    push("\n".join("已恢复：" + o["msg"] for o in pending_res), tags="white_check_mark")
                    pending_res = []
        except Exception as e:  # noqa: BLE001  推送失败：下次再推
            sent_ok = False
            log.append(f"推送失败：{e}")
        st.update(problems=cur, resolved_overnight=pending_res, checked=now.strftime("%Y-%m-%d %H:%M"))
        os.makedirs(os.path.dirname(STATE), exist_ok=True)
        tmp = STATE + ".tmp"
        json.dump(st, open(tmp, "w"), ensure_ascii=False, indent=1)
        os.replace(tmp, STATE)
        if log:
            with open(LOG, "a") as f:
                f.writelines(f"{now:%F %T} {x}\n" for x in log)
    return msgs, resolved, sent_ok


def digest(cur, resolved, prune_pos, now):
    act = [p for p in cur.values() if p["active"]]
    lines = [f"{now:%m-%d %H:%M} 健康检查：" + (f"{len(act)} 个问题" if act else "一切正常")]
    lines += ["- " + p["msg"] for p in act]
    lines += ["已恢复：" + o["msg"] for o in resolved]
    if os.path.exists(PRUNE_LOG):
        with open(PRUNE_LOG) as f:
            f.seek(prune_pos if prune_pos <= os.path.getsize(PRUNE_LOG) else 0)
            pr = [ln.strip() for ln in f if ln.strip()]
        if pr:
            lines.append(f"归档清理 {len(pr)} 条：" + "；".join(pr[-5:]))
    return "\n".join(lines)


def write_claims(name, items, now):
    m = M.MACHINES[name]
    body = (f"# health.py {now:%F %T}：别的机器在训、或 Slurm 上排着的折，{m['label']} 上不训（key<TAB>机器）\n"
            + "".join(f"{k}\t{w}\n" for k, w in items))
    cmd = f"mkdir -p ~/.cache/phd && cat > {CLAIMS}.tmp && mv {CLAIMS}.tmp {CLAIMS}"
    if m["scheduler"] == "slurm":
        # 10/16 之前提交的作业复制了旧版 train_folds.sbatch，读 lab_active.txt（每行一个 key）；这批作业跑完后去掉
        cmd += " && cut -f1 " + CLAIMS + " | grep -v '^#' > ~/.cache/phd/lab_active.txt"
    sh(["bash", "-c", cmd] if m["ssh"] is None else SSH + [m["ssh"], cmd], inp=body)


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
    P, claims = check(s)
    import refill  # scripts/queue/refill.py：训练机的卡快空了就自动补任务
    events, filled = refill.run_all(s, dry=a.dry_run)
    import companion  # scripts/queue/companion.py：DICC 上每个在训的作业同卡附加一折（10/17）
    events += companion.run_all(s, dry=a.dry_run)
    if not a.dry_run:
        for i, msg in enumerate(events):
            P[f"refill:{s['now']:%m%d%H%M}:{i}"] = dict(msg=msg, level="normal", kind="event", after=0)
        for pid in [k for k in P if k.startswith("idle:") and k.split(":")[1] in filled]:
            del P[pid]
    update(P, s["now"], dry=a.dry_run)
    if a.dry_run:
        for pid, p in sorted(P.items()):
            wait = f"/持续 {p['after']} 分钟才报" if p["after"] else ""
            print(f"[{p['level']}/{p['kind']}{wait}] {pid}\n    {p['msg']}")
        for n, items in claims.items():
            print(f"\n写给 {n} 的清单（这里不训）：{len(items)} 折")
        print("\n自动补任务、附加训练：" + "；".join(events))
        return
    for n, items in claims.items():
        write_claims(n, items, s["now"])


if __name__ == "__main__":
    main()
