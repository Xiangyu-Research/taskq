#!/usr/bin/env python3
"""各台机器（配置文件夹里的 machines.py）的健康检查（2026-10-16）：hub 上 cron 每 30 分钟跑一次，发现问题推送到手机（ntfy），
同时记进状态文件（使用者的登记表脚本可以读它，把当前的问题列出来）。各机器的状态只在 gather() 里取，
使用者的登记表脚本也可以用它。

    taskq tick              检查、推送、更新状态
    taskq tick --dry-run    只打印这次查到的问题，不推送、不改任何文件
    taskq tick --test       推送一条测试消息

查什么（每项一个问题编号，同一个问题只推送一次）：
  - 连不上：pueue 机器 ssh 不通 30 分钟。
  - pueue：常驻进程连不上；任务失败（手动停的 143 / Killed 只记进登记表，不推送；调度停的 worker 不算）。
  - 训练卡住：pueue 在跑的训练任务，开始超过 1 小时、训练日志 90 分钟没有新 epoch。
  - 同一个任务两处：同一个任务在两台以上机器上排队或在训。两边都已经开训（或有段间的进度）算紧急。
  - 执行机（relay.py 管的）的问题由 relay.py 报（经 feed.run_all）。
  - 盘：表里 disk 一项（路径、剩多少 GB 提醒、剩多少 GB 紧急）。
  - GPU 使用记录（表里 gpu_log 的机器，monitor/gpu_log.sh）10 分钟没有新记录。
  - pueue 机器 24 小时内重启过（queue/on_boot.sh 放回了哪些任务）。
  - 项目挂的提醒（taskq notice，monitor/notice.py）：每条一个问题，撤掉后消失。
提醒（10/21 起）：每个问题推送一次，用 monitor/plain.py 的白话（出了什么事、影响什么），同一次检查查到几件并成一条；
  恢复了不推送。22:00–08:00 只即时推送紧急的，其余在 08:00 后的第一次检查时推。检查本身在不在跑，可以看 health.log 的时间。
文件（配置文件夹里 policy.py 的 OUTPUTS）：topic（ntfy 频道名，不进仓库）、health 下的 state.json（问题和推送记录，
  使用者的登记表脚本可以读）、health.log（每次有变化记一行）。
每次检查后还做这几件事（10/18 起）：
  - queue/shift.py：按时段开关 pueue 组（policy.py 的 SHIFT）。
  - queue/feed.py：送任务的唯一入口（10/31），交给 queue/relay.py（领任务、传数据、补 worker、拉回，
    记在 ~/relay.log；停住等人处理的报成问题）。
  - queue/advance.py：hub 上按每个任务的状态排下一步（拉回 → 每个任务的步骤 → 实验一级的步骤）；失败停住的报成问题。
"""
import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg  # noqa: E402
import proto  # noqa: E402
from proto import alert_id  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
cfg.use_config()
TQS = cfg.state("taskq", expand=False)
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import policy  # noqa: E402
sys.path.insert(0, HERE)
import notice as NT  # noqa: E402

OUT = getattr(policy, "OUTPUTS", {})
HDIR = os.path.expanduser(OUT.get("health") or cfg.state("health", expand=False))
NAME = OUT.get("name", "taskq")
STATE = os.path.join(HDIR, "state.json")
LOG = os.path.join(HDIR, "health.log")
TOPIC = os.path.expanduser(OUT.get("topic", "~/.config/taskq-health/ntfy_topic"))
H_NOTICES = os.path.join(HDIR, proto.NOTICES)  # taskq notice（monitor/notice.py）
H_LOADERR = os.path.join(HDIR, "load_error.txt")
NIGHT = (22, 8)  # 22:00–08:00 只推紧急的


def push(msg, title=f"{NAME} 作业", priority="default", tags=""):
    topic = open(TOPIC).read().strip()
    q = urllib.parse.urlencode({"title": title, "priority": priority, "tags": tags})
    req = urllib.request.Request(f"https://ntfy.sh/{topic}?{q}", data=msg.encode(), method="POST")
    urllib.request.urlopen(req, timeout=30).read()


def load_failed(e, dry=False):
    """实验表、插件加载出错：整个检查（送任务、拉回、排下一步）都停了，推送一次（同样的错不重复推；夜里等到早上）。"""
    msg = f"健康检查没法运行：实验表或插件加载出错，送任务、拉回、排下一步都停了。\n{type(e).__name__}: {e}"
    print(msg, file=sys.stderr)
    if dry or not NIGHT[1] <= datetime.now().hour < NIGHT[0]:
        return
    old = open(H_LOADERR).read() if os.path.exists(H_LOADERR) else None
    if old != msg:
        push(msg, priority="high", tags="warning")
        os.makedirs(HDIR, exist_ok=True)
        open(H_LOADERR, "w").write(msg)


try:
    import experiments as E  # noqa: E402  queue/experiments.py：实验表
    import spec  # noqa: E402  queue/spec.py：任务说明、各机器上的收集
    import taskq  # noqa: E402  queue/taskq.py：总队列（hub，10/31）
    import plain as PL  # noqa: E402  monitor/plain.py：推送的白话说法
    import common as C  # noqa: E402  queue/common.py
    from common import write_json  # noqa: E402
except Exception as _e:
    if __name__ != "__main__":
        raise
    LOAD_ERROR = _e  # 当脚本跑时由 main() 先解析参数（--help 不推送）再报
else:
    LOAD_ERROR = None


def stopped(t):
    """手动停的（pueue kill，或 SIGTERM 让 run_task.sh 退出 143）：只记进登记表，不推送。"""
    return t.get("result") == "Killed" or t.get("code") == 143


LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")

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


def probe_ok(rc, out):
    return rc == 0 and "BOOT|" in out


def probe_script(m):
    """pueue 机器上要查的东西（按表里的字段），每行"项|值"。"""
    lines = ['echo "BOOT|$(uptime -s)|$(tail -1 ~/on_boot.log 2>/dev/null)"', GPU_PROBE]
    if m["disk"]:
        lines.append(f'echo "DISK|$(df -B1 --output=avail {m["disk"][0]} | tail -1 | tr -d \' \')"')
    if m["gpu_log"]:
        lines.append(f'echo "GPULOG|$(tail -1 {proto.GPU_LOG_DIR}/$(date +{proto.GPU_LOG_FILE}) 2>/dev/null | cut -d, -f1)"')
    return "\n".join(lines) + "\n"


def sh(cmd, inp=None, timeout=120):
    """本机跑命令（列表）→ (退出码, 标准输出；出错时没有输出就给标准错误)。"""
    rc, out, err = C.run(cmd, timeout=timeout, input=inp)
    return rc, out if rc == 0 or out else err


def run_on(m, script, timeout=120):
    remote = f"timeout -k 10 {max(timeout - 15, 10)} bash -s"
    return sh(["bash", "-s"] if m["ssh"] is None else C.SSH + [m["ssh"], remote], inp=script, timeout=timeout)


# ---------------- 收集 ----------------
def gather(remote=True):
    """各机器的状态（只读）。remote=False：只看本机（不连别的机器）。"""
    s = dict(now=datetime.now(), ok={}, why={}, probe={}, gpus={}, src={}, taskq={})
    here = [n for n in M.names("pueue") if (remote or M.MACHINES[n]["ssh"] is None) and n not in policy.OFFLINE]
    for n in set(M.names("pueue")) & set(policy.OFFLINE):
        s["ok"][n], s["why"][n] = False, f"暂停检查：{policy.OFFLINE[n]}"
    rc, out = sh([os.path.join(CORE, "queue", "qs"), "--json"] + ([] if remote else here), timeout=180)
    s["pueue"] = json.loads(out) if rc == 0 else {n: {"error": out} for n in here}
    s["records"] = C.sync_records(s["pueue"], C.load_records())  # 任务记录（queue/common.py），main() 里写回
    just = os.environ.get("TASKQ_TASK_DONE") or os.environ.get("PHD_TASK_DONE")  # hub 的任务做完后马上调用检查时设上（它的标签；PHD_TASK_DONE 是 11/6 前排的命令里的旧名字，等 hub 的 pueue 里没有这种命令后删）：这个任务这一次算做完（2026-10-24）
    for t in (s["pueue"].get(M.hub()) or {}).get("tasks", []) if just else []:
        if t["label"] == just and t["status"] == "Running":
            t.update(status="Success", result="Success", code=0)
    # 总队列在 hub（10/31）：收集训练状态要用队列里的任务
    hub = M.hub()
    s["taskq"][hub] = taskq.local_status()
    queue = list((s["taskq"][hub] or {}).get("tasks", []))
    for name in here:
        m = M.MACHINES[name]
        rc, out = run_on(m, probe_script(m))
        ok = probe_ok(rc, out)
        if ok:
            lines = out.splitlines()
            s["probe"][name] = dict(ln.split("|", 1) for ln in lines if "|" in ln and not ln.startswith(("GPU|", "APP|")))
            s["gpus"][name] = gpus_of(lines)
        s["ok"][name] = ok
    specs = spec.task_specs(queue)
    for name in here:  # 各机器上每个任务的训练状态（spec.collect）：有数据目录的
        m = M.MACHINES[name]
        if s["ok"][name] and m.get("data"):
            r = spec.collect(specs, M.expand(m["data"])) if m["ssh"] is None else spec.remote_collect(m["ssh"], specs, m["data"])
            if r:
                s["src"][name] = r
            elif m["ssh"] is not None:  # 收集超时（多半是那边的存储挂住，10/31）：这次检查后面的步骤都当它连不上，不再去连
                s["ok"][name] = False
                s["why"][name] = "收集训练状态超时或出错（多半是那边的存储挂住）"
    s["notices"] = {n: v["text"] for n, v in NT.read(H_NOTICES).items()}
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
def problem(msg, level="normal", kind="state", after=0, data=None):
    """一个问题：msg 是给人看的说明（登记表），data 是结构化的事实（plain.py 据此说白话）。"""
    p = dict(msg=msg, level=level, kind=kind, after=after)
    if data:
        p["data"] = data
    return p


def check(s):
    """→ {问题编号: dict(msg, level=urgent|normal|info, kind=event|state, after=持续多少分钟才算, data=结构化的事实（可选）)}。"""
    now, P = s["now"], {}
    L = lambda n: M.MACHINES[n]["label"]  # noqa: E731

    def add(pid, msg, level="normal", kind="state", after=0, data=None):
        P[pid] = problem(msg, level, kind, after, data)

    claims = {}  # key → {"机器:组": [描述]}（排队或在训）
    busy = {}  # key → {机器（同上）}：在训，或段间等待（有进度、下一段在排队）

    def claim(key, name, desc):
        claims.setdefault(key, {}).setdefault(name, []).append(desc)

    for name, ok in s["ok"].items():
        if not ok and name not in policy.OFFLINE:
            add(alert_id("down", name), f"{L(name)} 连不上（{(s.get('why') or {}).get(name) or 'ssh ' + str(M.MACHINES[name]['ssh'])}）",
                after=30)
    for name, d in (s.get("src") or {}).items():  # 存储挂住、读不动的任务（spec.collect 的 stuck，11/1）
        if d.get("stuck"):
            add(alert_id("unread", name), f"{L(name)} 上 {len(d['stuck'])} 个任务的文件读不动（多半是存储挂住），先不分、不交还："
                f"{'、'.join(d['stuck'][:5])}{' …' if len(d['stuck']) > 5 else ''}", after=60, data=dict(n=len(d["stuck"]), keys=d["stuck"]))
        if d.get("errors"):
            er = sorted(d["errors"].items())
            add(alert_id("readerr", name), f"{L(name)} 上 {len(er)} 个任务读状态出错，先不分、不交还："
                f"{'；'.join(f'{k}（{v}）' for k, v in er[:3])}{' …' if len(er) > 3 else ''}", after=60, data=dict(n=len(er)))
    # --- 总队列（hub，10/31）---
    hub = M.hub()
    ctq = s.get("taskq", {}).get(hub)
    if hub in s.get("taskq", {}) and ctq is None:
        add(alert_id("taskq", hub), f"{L(hub)} 读不到总队列（{TQS}/）")
    elif ctq and not ctq.get("enabled"):
        add(alert_id("tqoff", hub), f"{L(hub)} 的总队列关着（{TQS}/{proto.QUEUE['enabled']} 不在），不会往执行机送任务", after=60)
    elif ctq:
        for t in ctq["tasks"]:
            if t["state"] == proto.S_BLOCKED and not t.get("assigned") and not proto.is_smoke(t["key"]):
                add(alert_id("tqblocked", t["key"]), f"总队列任务 {t['key']} {t['note']}，不会开始", data=dict(key=t["key"]))
    # --- pueue ---
    for name in M.names("pueue"):
        if not s["ok"].get(name):
            continue
        d = s["pueue"].get(name) or {"error": "没有结果"}
        if "error" in d:
            add(alert_id("pueued", name), f"{L(name)} 的 pueue 没有响应：{d['error'][:120]}")
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
                        add(alert_id("stuck", name, t["id"]), f"{L(name)} 任务 {t['id']} {tname}：90 分钟没有新 epoch（在等卡，或训练卡住）",
                            data=dict(key=key, label=tname))
            if st in LIVE or st == "Success":
                continue
            if stopped(t) and tname.startswith(proto.WORKER_LABEL):  # 时段结束、让卡时调度停的 worker；worker 出错由 relay 报 site:<机器>:worker
                continue
            end = C.local_time(t["end"])
            if end and now - end > timedelta(hours=24):
                continue
            add(alert_id("pueue", name, t["id"], str(t["end"])), f"{L(name)} 任务 {t['id']} {tname} {st}",
                level="info" if stopped(t) else "normal", kind="event", data=dict(label=tname, status=st))

    # --- 同一个任务两处 ---
    for key in sorted(k for k, v in claims.items() if len(v) >= 2):
        both = len(busy.get(key, ())) >= 2
        add(alert_id("dup", key), f"同一个任务在两处：{key}，" + "；".join(f"{L(n.split(':')[0])}{' ' + n.split(':')[1] if ':' in n else ''} {'、'.join(v)}" for n, v in claims[key].items())
            + ("（两边都在训或有进度，同步时会互相覆盖存档）" if both else "（还没同时开训，取消一边前要确认作业号）"),
            level="urgent" if both else "normal", data=dict(key=key, both=both, where=sorted(claims[key])))

    # --- 盘、GPU 记录、代码同步、重启 ---
    for name, pr in s["probe"].items():
        m = M.MACHINES[name]
        if m["disk"] and pr.get("DISK", "").strip().isdigit():
            free, warn, urgent = int(pr["DISK"]) / 1e9, m["disk"][1], m["disk"][2]
            if free < warn:
                add(alert_id("disk", name), f"{L(name)} {m['disk'][0]} 只剩 {free:.0f} GB", level="urgent" if urgent and free < urgent else "normal",
                    data=dict(path=m["disk"][0], free_gb=free))
        if m["gpu_log"]:
            last = pr.get("GPULOG", "")
            try:
                age = now - datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                age = None
            if age is None or age > timedelta(minutes=10):
                add(alert_id("gpulog", name), f"{L(name)} GPU 使用记录停了（最后一条 {last or '没有'}）")
        boot, _, line = pr.get("BOOT", "").partition("|")
        try:
            t = datetime.strptime(boot, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if now - t < timedelta(hours=24):
            add(alert_id("boot", name, boot), f"{L(name)} {t:%m-%d %H:%M} 重启过：{line[20:] or '没有开机记录（on_boot.sh 没跑？）'}",
                kind="event", data=dict(time=boot))
    for name, text in (s.get("notices") or {}).items():
        add(alert_id("notice", name), text, data=dict(name=name))

    return P


# ---------------- 推送和状态 ----------------
def drop_acked(P, dry=False):
    """taskq notice ack 确认过的"发生过的事"不再列出；确认记录里已经不在 P 里的删掉（11/5）。"""
    d = NT.acked()
    if not d:
        return P
    keep = {k: v for k, v in d.items() if k in P}
    if keep != d and not dry:
        NT.save_acked(keep)
    return {k: p for k, p in P.items() if not (k in keep and p.get("kind") == "event")}


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


def undeclared_writes(days=1):
    """台账里最近 days 天 taskq run 记下的"写到声明的产出以外" → {问题编号: 说明}（过了 days 天自动消失）。"""
    import json as _j
    import time as _t
    from flow import ledger as LG
    out = {}
    for inst, path, detail in LG.connect().execute("SELECT inst, path, detail FROM event WHERE kind='undeclared' AND t>?",
                                                   (_t.time() - days * 86400,)):
        n = (_j.loads(detail or "{}") or {}).get("n", 1)
        out[alert_id("undeclared", inst)] = f"{inst} 写到了声明的产出以外：{path}{f' 等 {n} 个' if n > 1 else ''}（taskq who 看归谁）"
    return out


def publishes(dry=False):
    """接着做中途断了的发布（queue/flow/publish.py 的 journal），报被冲突挡住的 → {问题编号: 说明}。"""
    import os as _os
    from flow import ledger as LG
    from flow import publish as PB
    db = LG.connect()
    if not dry and _os.path.isdir(PB.journal_dir()) and _os.listdir(PB.journal_dir()):
        from flow import cli as FC
        PB.resume(FC.flow_for_ledger()[0], db)
    return {alert_id("publish", k): f"{k} 的发布被挡住：{len(v.get('conflicts') or [])} 个文件不归它或被改过（taskq publish {k} 看）"
            for k, v in PB.blocked(db).items()}


def ledger_shadow(s):
    """台账的影子对比（queue/flow/ledger.py）：和 advance.py 这一轮的判断比，结果一行写进 ~/ledger.log。"""
    import advance
    from flow.compile import compile_legacy
    from flow.graph import Graph
    from flow import ledger as LG
    root = advance.bt()
    flow = compile_legacy(queue_tasks=((s.get("taskq") or {}).get(M.hub()) or {}).get("tasks", []), python=advance.python())
    db = LG.connect()
    try:
        n = LG.shadow(flow, Graph(flow), root, db, s=s, seen=advance.SEEN, hub=M.hub(), identify=C.identify)
    finally:
        db.close()
    line = (f"{datetime.now():%F %T} 一致 {n['agree']}、新做完 {n['new']}、重跑 {n['rerun']}、改过 {n['changed']}、"
            f"做完又不算了 {n['regressed']}、这轮没查 {n['skipped']}；新出的说不清的 {len(n['unexplained'])}（{n['seconds']} 秒）")
    with open(cfg.log("ledger"), "a") as f:
        f.write(line + "\n")
    return n


def from_step(v):
    """shift / feed（relay）/ advance 报的卡住：说明（str），或 dict(msg, data) → 问题。"""
    if isinstance(v, dict):
        return problem(v["msg"], data=v.get("data"))
    return problem(v)


def main(argv=None):
    ap = argparse.ArgumentParser(description="健康检查一轮：查各机器、推送问题、按时段开关、送任务、排下一步")
    ap.add_argument("-n", "--dry-run", action="store_true", help="只打印这次查到的问题，不推送、不改任何文件")
    ap.add_argument("--test", action="store_true", help="推送一条测试消息")
    a = ap.parse_args(argv)
    if LOAD_ERROR is not None:
        load_failed(LOAD_ERROR, dry=a.dry_run)
        return 1
    if not a.dry_run and os.path.exists(H_LOADERR):
        os.remove(H_LOADERR)
    if a.test:
        push("测试：健康检查的推送能收到", tags="tada")
        print("已推送测试消息")
        return 0
    s = gather()
    if not a.dry_run:
        s["records"] = C.update_records(lambda recs: C.sync_records(s["pueue"], recs))
    P = check(s)
    # 这几步出错（包括 import 出错）不能让健康检查本身停掉（报警、登记表都靠它）：出错时报出来，其余照常
    try:
        import shift  # queue/shift.py：按时段开关 pueue 组（policy.py 的 SHIFT）
        shift_msgs = shift.run(s, dry=a.dry_run)
    except Exception as e:  # noqa: BLE001
        shift_msgs, shift_err = [], f"queue/shift.py 出错：{type(e).__name__}: {e}"[:300]
    else:
        shift_err = None
    events, stuck = shift_msgs, {}
    if shift_err:
        stuck["shift:error"] = shift_err
    try:
        import feed  # queue/feed.py：送任务的唯一入口，转给 relay.py（10/31）
        ev, st = feed.run_all(s, dry=a.dry_run)
        events.extend(ev)
        stuck.update(st)
    except Exception as e:  # noqa: BLE001
        stuck["feed:error"] = f"queue/feed.py 出错：{type(e).__name__}: {e}"[:300]
    try:
        import advance  # queue/advance.py：hub 上按状态排下一步
        done, stuck2 = advance.run(s, dry=a.dry_run)
    except Exception as e:  # noqa: BLE001
        done, stuck2 = [], {"advance:error": f"queue/advance.py 出错：{type(e).__name__}: {e}"[:300]}
    if policy.AUTO.get("ledger") and not a.dry_run:
        try:
            ledger_shadow(s)
        except Exception as e:  # noqa: BLE001  影子对比出错不影响别的
            stuck2[alert_id("ledger", "error")] = f"台账的影子对比出错：{type(e).__name__}: {e}"[:300]
    try:  # taskq run 包着的步骤卡住、写到声明以外（queue/flow/stall.py、台账的事件）
        from flow import stall
        stuck2.update(stall.check())
        if policy.AUTO.get("ledger"):
            stuck2.update(undeclared_writes())
            stuck2.update(publishes(a.dry_run))
    except Exception as e:  # noqa: BLE001
        stuck2[alert_id("stall", "error")] = f"卡住检测出错：{type(e).__name__}: {e}"[:300]
    for pid, v in {**stuck, **stuck2}.items():
        P[pid] = from_step(v)
    P = drop_acked(P, dry=a.dry_run)
    update(P, s["now"], dry=a.dry_run)
    if a.dry_run:
        for pid, p in sorted(P.items()):
            wait = f"/持续 {p['after']} 分钟才报" if p["after"] else ""
            print(f"[{p['level']}/{p['kind']}{wait}] {pid}\n    {p['msg']}")
        print("\n送任务、补作业、按时段开关：" + "；".join(events))
        print(f"{M.hub()} 按状态推进：" + ("；".join(done) or "没有要排的（影子模式时看 ~/advance.log）"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
