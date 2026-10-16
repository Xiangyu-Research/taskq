#!/usr/bin/env python3
"""box、lab、DICC 的健康检查（2026-10-16）：box 上 cron 每 30 分钟跑一次，发现问题推送到手机（ntfy），
同时记进状态文件，make_registry.py 把当前的问题放在实验登记表最上面（"需要处理"）。

    python3 scripts/monitor/health.py              检查、推送、更新状态
    python3 scripts/monitor/health.py --dry-run    只打印这次查到的问题，不推送、不改任何文件
    python3 scripts/monitor/health.py --test       推送一条测试消息

查什么（每项一个问题编号，同一个问题只推送一次）：
  - pueue：box、lab 的常驻进程连不上；任务失败或因依赖失败没跑（按规则停的退出码 75、手动停的 143 / Killed 不算）。
  - DICC：过去 24 小时 FAILED / OUT_OF_MEMORY / NODE_FAIL 的作业（到时限停的段不算）；连续 1 小时连不上。
  - 训练卡住：DICC 在跑的作业、lab 在跑的训练任务，开始超过 1 小时、训练日志 90 分钟没有新 epoch。
  - 拉回：box 上在等的折既没训完、也不在 DICC 或 lab 上排队 / 训练；或者训完 90 分钟了还没拉回。
  - 同一折两处：同一折在 DICC 上排队 / 在跑、同时在 lab 的队列里。两边都已经开训（或有段间、夜班间的进度）算紧急。
  - 盘：lab /mnt/disk1 剩不到 40 GB（不到 15 GB 紧急），box 数据盘剩不到 100 GB。
  - lab：GPU 使用记录（scripts/monitor/gpu_log.sh，0 号卡夜班开关靠它）10 分钟没有新记录；代码自动同步 2 小时没成功。
  - box、lab 24 小时内重启过（scripts/queue/on_boot.sh 放回了哪些任务）；看板有结果等确认后发布（publish.py --auto 留下的）。
提醒：22:00–08:00 只即时推送紧急的，其余在 08:00 后的第一次检查时和每日汇总一起发（每天一条，没有问题也发，说明检查本身在跑）。
  事件类（任务失败）推送一次；状态类（盘、卡住、连不上……）恢复后再推送一次"已恢复"。
文件：~/.config/phd-health/ntfy_topic（ntfy 频道名，不进仓库）、~/.cache/phd-health/state.json（问题和推送记录，
  make_registry.py 读）、~/.cache/phd-health/health.log（每次有变化记一行）。
另外把"正在训练"的清单写到对方机器上，开训前读（防止同一折两处训练）：
  lab ~/.cache/phd/dicc_active.txt（DICC 上排队或在跑的折：要在 lab 训，先取消 DICC 的作业）← scripts/queue/train_task.sh 读
  DICC ~/.cache/phd/lab_active.txt（lab 在跑、或夜班停着等晚上接着训的折）← research/RQ1/nnunet/train_folds.sbatch 读
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "research", "RQ1", "nnunet"))
import make_registry as mr  # noqa: E402  收集各机器训练状态的函数、SPEC、DS

STATE = os.path.expanduser("~/.cache/phd-health/state.json")
LOG = os.path.expanduser("~/.cache/phd-health/health.log")
TOPIC = os.path.expanduser("~/.config/phd-health/ntfy_topic")
PRUNE_LOG = os.path.expanduser("~/prune_lab.log")
H_PENDING = os.path.expanduser("~/.cache/phd-health/board_pending.txt")  # scripts/dashboard/publish.py --auto 写
BT = os.environ.get("BRAIN_TUMOR", "/data/others/BRAIN_TUMOR")
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]
NIGHT = (22, 8)  # 22:00–08:00 只推紧急的
PAUSED_OK = ("Failed (75)",)  # train_task.sh 按规则停了、还没训完
STOPPED = ("Failed (143)", "Killed")  # 手动停的：只记进登记表，不推送
LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")

LAB_PROBE = r"""
echo "DISK|$(df -B1 --output=avail /mnt/disk1 | tail -1 | tr -d ' ')"
echo "GPULOG|$(tail -1 ~/gpu_usage/gpu_$(date +%Y-%m).csv 2>/dev/null | cut -d, -f1)"
echo "GITOK|$(cat ~/.cache/phd/git_pull_ok 2>/dev/null)"
echo "GITLOG|$(tail -1 ~/git_pull.log 2>/dev/null)"
echo "BOOT|$(uptime -s)|$(tail -1 ~/on_boot.log 2>/dev/null)"
"""
DICC_PROBE = ("squeue -u $USER -h -r -o '%j|%i|%T|%M|%r'; echo '=====';"
              " sacct -u $USER -X -n -P -S now-24hours -s F,OOM,NF,BF,DL -o JobID,JobName,State,End,ExitCode")


def sh(cmd, inp=None, timeout=120):
    try:
        r = subprocess.run(cmd, input=inp, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout
    except Exception as e:  # noqa: BLE001
        return 1, str(e)


# ---------------- 收集 ----------------
def job_keys():
    """DICC 作业名 → key（数据集/训练器/fold），按 make_registry.SPEC。"""
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
    m = re.search(r"fetch_fold\.sh\s+(dicc|lab)\s+(\S+)/(\S+?)__\S+\s+(\S+)", cmd)
    if m:
        return "fetch:" + m.group(1), f"{m.group(2)}/{m.group(3)}/{m.group(4)}"
    return None, None


def gather():
    s = dict(now=datetime.now())
    rc, out = sh([os.path.join(REPO, "scripts", "queue", "qs"), "--json"], timeout=180)
    s["pueue"] = json.loads(out) if rc == 0 else {"box": {"error": out}, "lab": {"error": out}}
    rc, out = sh(SSH + ["dicc", DICC_PROBE])
    s["dicc_ok"] = rc == 0 and "=====" in out
    s["squeue"], s["sacct"] = [], []
    if s["dicc_ok"]:
        q, a = out.split("=====", 1)
        s["squeue"] = [ln.split("|") for ln in q.splitlines() if ln.count("|") == 4]
        s["sacct"] = [ln.split("|") for ln in a.splitlines() if ln.count("|") == 4]
    rc, out = sh(SSH + ["lab", "bash -s"], inp=LAB_PROBE)
    s["lab_ok"] = rc == 0 and "DISK|" in out
    s["lab"] = dict(ln.split("|", 1) for ln in out.splitlines() if "|" in ln) if s["lab_ok"] else {}
    s["src"] = {"box": mr.collect(os.path.join(BT, "nnUNet", "results"))}
    for host, root in [("lab", "~/BRAIN_TUMOR/nnUNet/results"), ("DICC", "~/st/BRAIN_TUMOR/nnUNet/results")]:
        ok = s["lab_ok"] if host == "lab" else s["dicc_ok"]
        r = mr.remote_collect(host.lower(), root) if ok else None
        if r:
            s["src"][host] = r
    s["box_free"] = shutil.disk_usage(BT).free
    s["box_boot"] = subprocess.run(["uptime", "-s"], capture_output=True, text=True).stdout.strip()
    s["box_bootlog"] = open(os.path.expanduser("~/on_boot.log")).read().splitlines()[-1:] if os.path.exists(os.path.expanduser("~/on_boot.log")) else []
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
    """→ {问题编号: dict(msg, level=urgent|normal|info, kind=event|state, after=持续多少分钟才算)}，
    以及写给对方机器的清单 dicc_active、lab_active（None = 这次没收集到，不写）。"""
    now, P = s["now"], {}

    def add(pid, msg, level="normal", kind="state", after=0):
        P[pid] = dict(msg=msg, level=level, kind=kind, after=after)

    jk = job_keys()
    # --- DICC ---
    dicc_claim, dicc_busy = {}, set()  # busy：在跑，或段间等待（有进度、下一段排队）
    for name, jid, st, el, why in s["squeue"]:
        key = jk.get(name)
        if not key:
            continue
        dicc_claim.setdefault(key, []).append(f"{jid}（{st}）")
        info = fold_info(s, key).get("DICC")
        if st == "RUNNING":
            dicc_busy.add(key)
            last = info[2] if info else None
            if minutes(el) > 60 and (not last or now - last > timedelta(minutes=90)):
                add(f"stuck:dicc:{jid}", f"DICC {jid}（{key}）开始 {minutes(el) / 60:.1f} 小时，"
                    f"{'训练日志 90 分钟没有新 epoch' if last else '还没有训练日志'}")
        elif st == "PENDING" and info and info[1] > 0 and not info[0]:
            dicc_busy.add(key)
    if not s["dicc_ok"]:
        add("down:dicc", "DICC 连不上（ssh dicc）", after=60)
    for jid, name, st, end, code in s["sacct"]:
        add(f"dicc:{jid}", f"DICC 作业 {jid} {name} {st}（{end}，退出码 {code}）", kind="event")

    # --- pueue ---
    lab_claim, lab_active, waits = {}, set(), []
    for host in ("box", "lab"):
        d = s["pueue"].get(host) or {"error": "没有结果"}
        if "error" in d:
            if host == "lab" and not s["lab_ok"]:
                add("down:lab", "lab 连不上（ssh lab）", after=30)
            else:
                add(f"pueued:{host}", f"{host} 的 pueue 没有响应：{d['error'][:120]}")
            continue
        for t in d["tasks"]:
            kind, key = task_key(t["command"])
            name = t["label"] or f"#{t['id']}"
            st = t["status"]
            if host == "lab" and kind == "train":
                if st in LIVE:
                    lab_claim.setdefault(key, []).append(f"任务 {t['id']} {name}（{st}）")
                if st == "Running":
                    lab_active.add(key)
                    info = fold_info(s, key).get("lab")
                    start = mr_time(t["start"])
                    if start and now - start > timedelta(minutes=60) and \
                            (not info or not info[2] or now - max(info[2], start) > timedelta(minutes=90)):
                        add(f"stuck:lab:{t['id']}", f"lab 任务 {t['id']} {name}：90 分钟没有新 epoch（在等卡，或训练卡住）")
                elif st in PAUSED_OK and t["group"] == "gpu0" and not name.startswith("day"):
                    lab_active.add(key)  # 夜班停着，晚上由 gpu0_gate.sh 放回队列
                    lab_claim.setdefault(key, []).append(f"任务 {t['id']} {name}（夜班停着）")
            if host == "box" and kind and kind.startswith("fetch") and st == "Running":
                waits.append((t, key, "DICC" if kind == "fetch:dicc" else "lab"))
            if st in LIVE or st == "Success" or st in PAUSED_OK:
                continue
            end = mr_time(t["end"])
            if end and now - end > timedelta(hours=24):
                continue
            add(f"pueue:{host}:{t['id']}:{t['end']}", f"{host} 任务 {t['id']} {name} {st}",
                level="info" if st in STOPPED else "normal", kind="event")

    # --- 拉回 ---
    for t, key, src in waits:
        start = mr_time(t["start"])
        if not start or now - start < timedelta(minutes=60):
            continue
        fin = fold_info(s, key).get(src)
        if fin and fin[0]:
            if fin[2] and now - fin[2] > timedelta(minutes=90):
                add(f"fetch:{key}", f"{key} 在 {src} 上训完了，box 还没拉回（任务 {t['id']}）")
        elif s["dicc_ok"] and s["lab_ok"] and key not in dicc_claim and key not in lab_claim:
            add(f"orphan:{key}", f"box 在等 {key} 训完再拉回（任务 {t['id']}），但它不在 DICC 或 lab 上排队、训练")

    # --- 同一折两处 ---
    if s["dicc_ok"] and s["lab_ok"]:
        for key in sorted(set(dicc_claim) & set(lab_claim)):
            both = key in dicc_busy and key in lab_active
            add(f"dup:{key}", f"同一折在两处：{key}，DICC {'、'.join(dicc_claim[key])}；lab {'、'.join(lab_claim[key])}"
                + ("（两边都在训或有进度，同步时会互相覆盖存档）" if both else "（还没同时开训，取消一边前要确认作业号）"),
                level="urgent" if both else "normal")

    # --- 盘、lab ---
    if s["lab_ok"]:
        free = int(s["lab"].get("DISK") or 0) / 1e9
        if free < 40:
            add("disk:lab", f"lab /mnt/disk1 只剩 {free:.0f} GB", level="urgent" if free < 15 else "normal")
        last = s["lab"].get("GPULOG", "")
        try:
            age = now - datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            age = None
        if age is None or age > timedelta(minutes=10):
            add("gpulog:lab", f"lab GPU 使用记录停了（最后一条 {last or '没有'}），0 号卡夜班开关会一直不开")
        ok = s["lab"].get("GITOK", "").strip()
        if not ok.isdigit() or now - datetime.fromtimestamp(int(ok)) > timedelta(hours=2):
            add("git:lab", f"lab 代码 2 小时没有同步成功：{s['lab'].get('GITLOG', '')[:150] or '没有同步记录'}")
    boots = [("box", s.get("box_boot", ""), " ".join(s.get("box_bootlog", [])))]
    if s["lab_ok"]:
        b = s["lab"].get("BOOT", "").split("|", 1)
        boots.append(("lab", b[0], b[1] if len(b) > 1 else ""))
    for host, boot, line in boots:
        try:
            t = datetime.strptime(boot, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if now - t < timedelta(hours=24):
            add(f"boot:{host}:{boot}", f"{host} {t:%m-%d %H:%M} 重启过：{line[20:] or '没有开机记录（on_boot.sh 没跑？）'}", kind="event")
    if s.get("board_pending"):
        add("board", f"看板有新结果等确认后发布（{len(s['board_pending'])} 个文件：{'、'.join(s['board_pending'][:4])}）")
    if s["box_free"] / 1e9 < 100:
        add("disk:box", f"box 数据盘只剩 {s['box_free'] / 1e9:.0f} GB")

    return P, (sorted(dicc_claim) if s["dicc_ok"] else None), (sorted(lab_active) if s["lab_ok"] else None)


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
            lines.append(f"lab 清理 {len(pr)} 条：" + "；".join(pr[-5:]))
    return "\n".join(lines)


def write_active(host, name, keys, now):
    body = f"# health.py {now:%F %T}（{'DICC' if host == 'lab' else 'lab'} 上排着或正在训练、这里不能同时训的折）\n" + "".join(k + "\n" for k in keys)
    f = f"~/.cache/phd/{name}"
    sh(SSH + [host, f"mkdir -p ~/.cache/phd && cat > {f}.tmp && mv {f}.tmp {f}"], inp=body)


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
    P, dicc_active, lab_active = check(s)
    msgs, resolved, _ = update(P, s["now"], dry=a.dry_run)
    if a.dry_run:
        for pid, p in sorted(P.items()):
            wait = f"/持续 {p['after']} 分钟才报" if p["after"] else ""
            print(f"[{p['level']}/{p['kind']}{wait}] {pid}\n    {p['msg']}")
        print(f"\nDICC 排队 / 在跑（写给 lab）：{dicc_active}\nlab 在训（写给 DICC）：{lab_active}")
        return
    if dicc_active is not None:
        write_active("lab", "dicc_active.txt", dicc_active, s["now"])
    if lab_active is not None:
        write_active("dicc", "lab_active.txt", lab_active, s["now"])


if __name__ == "__main__":
    main()
