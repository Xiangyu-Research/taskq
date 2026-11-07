#!/usr/bin/env python3
"""各台机器（配置文件夹里的 machines.py）的健康检查：hub 上 cron 每 30 分钟跑一次，发现问题推送到手机（ntfy），
同时记进 hub 的数据库（使用者的登记表脚本可以读它，把当前的问题列出来）。各机器的状态只在 gather() 里取，
使用者的登记表脚本也可以用它。

    taskq tick              检查、推送、更新状态
    taskq tick --dry-run    只打印这次查到的问题，不推送、不改任何文件
    taskq tick --test       推送一条测试消息

查什么（每项一个问题编号，同一个问题只推送一次）。探查、读 pueue、恢复 pueued 的机器是有 pueue 组的（mtab.pueue_hosts：
pueue 执行机、hub、store、写了 manual_groups 的）：
  - 连不上：ssh 不通 30 分钟；store 上收集训练状态出错。
  - pueue：常驻进程没响应（开关 AUTO["revive"] 开着时先自动恢复）；任务失败（手动停的 143 / Killed 只记进登记表，
    不推送；调度停的 worker 不算）。
  - store 上读不动、读出错的任务（spec.collect 的 stuck、errors）。
  - 执行机的问题（领任务、传数据、worker、能去的执行机都跳过了的任务）由 relay.py 报（经 feed.run_all）。
  - 盘：表里 disk 一项（路径、剩多少 GB 提醒、剩多少 GB 紧急）。
  - GPU 使用记录（表里 gpu_log 的机器，monitor/gpu_log.sh）10 分钟没有新记录。
  - 24 小时内重启过（queue/on_boot.sh 放回了哪些任务）。
  - 项目挂的提醒（taskq notice，monitor/notice.py）：每条一个问题，撤掉后消失；只列在 taskq problems 里，不推送。
提醒：每个问题推送一次，用 monitor/plain.py 的白话（出了什么事、影响什么），同一次检查查到几件并成一条；
  恢复了不推送。22:00–08:00 只即时推送紧急的，其余在 08:00 后的第一次检查时推。检查本身在不在跑，可以看 health.log 的时间。
推送：monitor/push.py（频道名在 OUTPUTS["topic"] 指的文件，默认 ~/.config/taskq/ntfy_topic）。问题、提醒、日志在 hub 的数据库（taskq problems、taskq events）。
每次检查后还做这几件事：
  - queue/advance.py：按每个任务的状态排下一步（拉回 → 每个任务的步骤 → 实验一级的步骤），加进总队列；失败停住的报成问题。
  - queue/feed.py：送任务的唯一入口，交给 queue/relay.py（派任务、传数据、补 worker、拉回，记在 relay.log；停住等人处理的报成问题）。
"""
import argparse
import json
import os
import sys
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
NIGHT = (22, 8)  # 22:00–08:00 只推紧急的


from push import push  # noqa: E402


def load_failed(e, dry=False):
    """实验表、插件加载出错：整个检查（送任务、拉回、排下一步）都停了，推送一次（同样的错不重复推；夜里等到早上）。"""
    msg = f"健康检查没法运行：实验表或插件加载出错，送任务、拉回、排下一步都停了。\n{type(e).__name__}: {e}"
    print(msg, file=sys.stderr)
    if dry or not NIGHT[1] <= datetime.now().hour < NIGHT[0]:
        return
    db = NT.open_db()
    if db is None or not NT.D.writable(db) or NT.D.get_meta(db, "load_error") != msg:
        push(msg, priority="high", tags="warning")
        if db is not None and NT.D.writable(db):
            with NT.D.tx(db):
                NT.D.set_meta(db, "load_error", msg)


try:
    import spec  # noqa: E402  queue/spec.py：任务说明、各机器上的收集
    import relay_remote as RM  # noqa: E402  queue/relay_remote.py：在 store 上跑 site_ops 的操作（import 时读插件）
    import taskq  # noqa: E402  queue/taskq.py：总队列（在 hub）
    import plain as PL  # noqa: E402  monitor/plain.py：推送的白话说法
    import common as C  # noqa: E402  queue/common.py
except Exception as _e:
    if __name__ != "__main__":
        raise
    LOAD_ERROR = _e  # 当脚本跑时由 main() 先解析参数（--help 不推送）再报
else:
    LOAD_ERROR = None


def stopped(t):
    """手动停的（pueue kill，或 SIGTERM 让任务退出 143）：只记进登记表，不推送。"""
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
    """有 pueue 组的机器上要查的东西（按表里的字段），每行"项|值"。"""
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
    hosts = M.pueue_hosts()
    here = [n for n in hosts if (remote or M.local(n)) and not M.paused(n)]
    for n in hosts:
        if M.paused(n):
            s["ok"][n], s["why"][n] = False, f"暂停着：{M.paused(n)}"
    if here:  # qs 不带机器名会问全部：只问这一轮要看的
        rc, out = sh([os.path.join(CORE, "queue", "qs"), "--json"] + here, timeout=180)
        s["pueue"] = json.loads(out) if rc == 0 else {n: {"error": out} for n in here}
    else:
        s["pueue"] = {}
    s["records"] = C.sync_records(s["pueue"], C.load_records())  # 任务记录（queue/common.py），main() 里写回
    just = os.environ.get("TASKQ_TASK_DONE")  # hub 核心组的任务做完后马上调用检查时设上（它的标签）：这个任务这一次算做完
    for t in (s["pueue"].get(M.hub()) or {}).get("tasks", []) if just else []:
        if t["label"] == just and t["status"] == "Running":
            t.update(status="Success", result="Success", code=0)
    # 总队列在 hub：收集训练状态要用队列里的任务
    hub = M.hub()
    try:
        s["taskq"][hub] = taskq.local_status()
    except taskq.DiskError as e:  # store 连不上、上面的代码包还用不了：这一轮不派任务（relay 照样先换包）
        s["taskq"][hub], s["why"]["taskq"] = None, str(e)
    queue = list((s["taskq"][hub] or {}).get("tasks", []))
    for name in here:
        m = M.machine(name)
        rc, out = run_on(m, probe_script(m))
        ok = probe_ok(rc, out)
        if ok:
            lines = out.splitlines()
            s["probe"][name] = dict(ln.split("|", 1) for ln in lines if "|" in ln and not ln.startswith(("GPU|", "APP|")))
            s["gpus"][name] = gpus_of(lines)
        s["ok"][name] = ok
    specs = spec.task_specs(queue)
    st = M.store()  # 每个任务在 store 上的训练状态（spec.collect，在 store 上跑）；执行机上的由 relay 的 status 读
    s["collect"] = {}  # 收集出错的：{store: 原因}（连得上、代码包用不了、存储挂住都在这里，不当成整台连不上）
    if (remote or M.local(st)) and s["ok"].get(st, True):
        try:
            s["src"][st] = RM.store_op("collect", dict(specs=specs, wait=spec.COLLECT_WAIT), timeout=2 * spec.COLLECT_WAIT + 120)
        except RuntimeError as e:
            s["collect"][st] = str(e)[-300:]
    db = NT.open_db(write=False)
    s["notices"] = {n: v["text"] for n, v in NT.notices(db).items()} if db else {}
    return s


def pueue_seen(s, dry=False):
    """这一轮查到 pueued 在的机器：记下时间和开着的组（proto.PUEUE_SEEN）。→ 记录（含以前的）。"""
    p = os.path.join(HDIR, proto.PUEUE_SEEN)
    try:
        seen = json.load(open(p))
    except (OSError, ValueError):
        seen = {}
    t = int(s["now"].timestamp())
    for name, d in (s.get("pueue") or {}).items():
        if isinstance(d, dict) and "error" not in d:
            seen[name] = dict(t=t, running=sorted(g for g, st in (d.get("groups") or {}).items() if st == "Running"))
    if not dry:
        C.write_json(p, seen)
    return seen


def revive(s, seen, dry=False):
    """pueued 没响应、机器连得上（开关 AUTO["revive"]）：在那台跑 on_boot.sh --since（上次查到它在的时间）--resume（那时开着的组），
    启动 pueued、放回被打断的任务、打开原来开着的组。on_boot.sh 用那台上的调度代码（mtab.core：代码包，或仓库的克隆）。
    → (事件, {机器: 恢复后还是不行的原因})。恢复成功的机器这一轮不报 pueued。"""
    if not policy.AUTO.get("revive"):
        return [], {}
    events, still = [], {}
    for name in M.pueue_hosts():
        d = (s.get("pueue") or {}).get(name)
        if M.paused(name) or not s["ok"].get(name) or not (isinstance(d, dict) and "error" in d):
            continue
        m, last, core = M.machine(name), seen.get(name) or {}, M.core(name)
        if core is None:
            still[name] = "不知道那台的调度代码在哪（机器表里没有 root 也没有 repo），没法自动恢复"
            continue
        since = int(last["t"]) - 60 if last.get("t") else int(s["now"].timestamp())  # 没有记录：只启动，不放回任务
        args = ["--since", str(since)] + (["--resume", ",".join(last["running"])] if last.get("running") else [])
        script = " ".join(["bash", core + "/queue/on_boot.sh"] + args
                          + (["--gpu-log"] if m.get("gpu_log") else []))
        if dry:
            events.append(f"{L(name)} 的 pueued 没响应：会自动恢复（{script}）")
            continue
        rc, out = run_on(m, script, timeout=180)
        rc2, out2 = sh([os.path.join(CORE, "queue", "qs"), "--json", name], timeout=90)
        try:
            back = rc2 == 0 and "error" not in json.loads(out2).get(name, {"error": 1})
        except ValueError:
            back = False
        tail = " ".join(ln.split(" ", 2)[-1] for ln in out.strip().splitlines()[-3:])
        if back:
            events.append(f"{L(name)} 的 pueued 没响应，已自动恢复：{tail or '启动了 pueued'}")
            import events as EV
            EV.add("health", events[-1], kind="revive", site=name)
        else:
            still[name] = f"自动恢复没成功（退出码 {rc}）：{(out or out2).strip()[-200:]}"
    return events, still


def L(name):
    return M.MACHINES[name]["label"]


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

    for name, ok in s["ok"].items():
        if not ok and not M.paused(name):
            add(alert_id("down", name), f"{L(name)} 连不上（{(s.get('why') or {}).get(name) or 'ssh ' + str(M.MACHINES[name]['ssh'])}）",
                after=30)
    for name, d in (s.get("src") or {}).items():  # 存储挂住、读不动的任务（spec.collect 的 stuck）
        if d.get("stuck"):
            add(alert_id("unread", name), f"{L(name)} 上 {len(d['stuck'])} 个任务的文件读不动（多半是存储挂住），先不分、不交还："
                f"{'、'.join(d['stuck'][:5])}{' …' if len(d['stuck']) > 5 else ''}", after=60, data=dict(n=len(d["stuck"]), keys=d["stuck"]))
        if d.get("errors"):
            er = sorted(d["errors"].items())
            add(alert_id("readerr", name), f"{L(name)} 上 {len(er)} 个任务读状态出错，先不分、不交还："
                f"{'；'.join(f'{k}（{v}）' for k, v in er[:3])}{' …' if len(er) > 3 else ''}", after=60, data=dict(n=len(er)))
    # --- 总队列（在 hub）---
    hub = M.hub()
    ctq = s.get("taskq", {}).get(hub)
    for name, why in (s.get("collect") or {}).items():
        add(alert_id("collect", name), f"在 {L(name)} 上收集训练状态出错：{why}", after=30)
    if hub in s.get("taskq", {}) and ctq is None:
        add(alert_id("taskq", hub), f"{L(hub)} 读不到总队列（{TQS}/）{('：' + s['why']['taskq']) if s.get('why', {}).get('taskq') else ''}")
    elif ctq and not ctq.get("enabled"):
        add(alert_id("tqoff", hub), f"{L(hub)} 的总队列关着（{TQS}/{proto.QUEUE['enabled']} 不在），不会往执行机送任务", after=60)
    # --- pueue ---
    for name in M.pueue_hosts():
        if not s["ok"].get(name):
            continue
        d = s["pueue"].get(name) or {"error": "没有结果"}
        if "error" in d:
            add(alert_id("pueued", name), f"{L(name)} 的 pueue 没有响应：{d['error'][:120]}")
            continue
        ident = {t["id"]: C.identify(name, t, s.get("records") or []) for t in d["tasks"]}
        newer = {}
        for i, r in ident.items():
            if r:
                newer[r["step"], r["key"]] = max(i, newer.get((r["step"], r["key"]), i))
        for t in d["tasks"]:
            tname = t["label"] or f"#{t['id']}"
            st = t["status"]
            if st in LIVE or st == "Success":
                continue
            if stopped(t) and tname.startswith(proto.WORKER_LABEL):  # 时段结束、让卡时调度停的 worker；worker 出错由 relay 报 site:<机器>:worker
                continue
            end = C.local_time(t["end"])
            if end and now - end > timedelta(hours=24):
                continue
            if r and (kind, key) in newer and newer[kind, key] > t["id"]:  # 同一步、同一个任务后来又排过（出错后重排的）：只看最新的
                continue
            add(alert_id("pueue", name, t["id"], str(t["end"])), f"{L(name)} 任务 {t['id']} {tname} {st}",
                level="info" if stopped(t) else "normal", kind="event", data=dict(label=tname, status=st))

    # --- 盘、GPU 记录、重启 ---
    for name, pr in s["probe"].items():
        m = M.machine(name)
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
def drop_acked(P, db, dry=False):
    """确认过的"发生过的事"（taskq notice ack）不列出；已经不再报的问题的确认记录删掉。"""
    d = NT.acked(db) if db else {}
    if d and not dry and NT.D.writable(db):
        NT.prune_acks(db, set(P))
    return {k: p for k, p in P.items() if not (k in d and p.get("kind") == "event")}


QUIET = ("notice",)  # 只列在 taskq problems 里、不推送的问题种类（手机只收故障，到期的提醒开工时看）


def night(now):
    return now.hour >= NIGHT[0] or now.hour < NIGHT[1]


def update(P, now, db, dry=False):
    """和上次的状态比：新出现、持续够久的推送（白话，plain.py；同一次查到几件并成一条）；夜里只推紧急的，其余早上第一次检查时推。"""
    old = NT.problems(db)[0] if db else {}
    msgs, log = [], []
    cur = {}
    for pid, p in P.items():
        o = old.get(pid, {})
        p["first"] = o.get("first", now.strftime("%Y-%m-%d %H:%M"))
        p["notified"] = o.get("notified", False)
        age = (now - datetime.strptime(p["first"], "%Y-%m-%d %H:%M")).total_seconds() / 60
        p["active"] = age >= p["after"]
        if p["active"] and not p["notified"] and p["level"] != "info" and pid.split(":")[0] not in QUIET \
                and (p["level"] == "urgent" or not night(now)):
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
        if db is not None and NT.D.writable(db):
            NT.save_problems(db, cur, now.strftime("%Y-%m-%d %H:%M"))
        import events
        events.lines("health", log, db=db)
    return [p for _, p in msgs], sent_ok


def progress_snapshot(s, db):
    """Each task's state, progress and where its best copy is → the progress table (only what changed)."""
    import events
    import overview
    tasks = [dict(key=t["key"], state=(t["queue"] or {}).get("state"), progress=t["progress"], site=(t["copy"] or {}).get("where"),
                  data=dict(epochs=(t["copy"] or {}).get("epochs"), total=t["total"])) for t in overview.build(s)["tasks"]]
    return events.snapshot(db, tasks)


def from_step(v):
    """feed（relay）/ advance 报的卡住：说明（str），或 dict(msg, data) → 问题。"""
    if isinstance(v, dict):
        return problem(v["msg"], data=v.get("data"), after=v.get("after", 0))
    return problem(v)


def main(argv=None):
    ap = argparse.ArgumentParser(description="健康检查一轮：查各机器、推送问题、排下一步、送任务")
    ap.add_argument("-n", "--dry-run", action="store_true", help="只打印这次查到的问题，不推送、不改任何文件")
    ap.add_argument("--test", action="store_true", help="推送一条测试消息")
    a = ap.parse_args(argv)
    if LOAD_ERROR is not None:
        load_failed(LOAD_ERROR, dry=a.dry_run)
        return 1
    import events
    db = NT.open_db(write=not a.dry_run)
    if not a.dry_run and db is not None and NT.D.writable(db):
        NT.D.set_meta(db, "load_error", None)
        try:
            events.flush(db)
        except Exception as e:  # noqa: BLE001
            print(f"事件暂存没导进数据库：{e}", file=sys.stderr)
    if a.test:
        push("测试：健康检查的推送能收到", tags="tada")
        print("已推送测试消息")
        return 0
    s = gather()
    if not a.dry_run:
        s["records"] = C.update_records(lambda recs: C.sync_records(s["pueue"], recs))
    P = check(s)
    try:
        revived, still = revive(s, pueue_seen(s, dry=a.dry_run), dry=a.dry_run)
    except Exception as e:  # noqa: BLE001
        revived, still = [], {}
        P[alert_id("revive", "error")] = problem(f"pueued 的自动恢复出错：{type(e).__name__}: {e}"[:300])
    for line in revived:
        name = next((n for n in M.pueue_hosts() if line.startswith(L(n) + " ")), None)
        if name and "已自动恢复" in line:
            P.pop(alert_id("pueued", name), None)
    for name, why in still.items():
        pid = alert_id("pueued", name)
        if pid in P:
            P[pid] = dict(P[pid], msg=f"{P[pid]['msg']}；{why}")
    # 这几步出错（包括 import 出错）不能让健康检查本身停掉（报警、登记表都靠它）：出错时报出来，其余照常
    events, stuck = list(revived), {}
    try:  # 先排训完以后的步骤，当轮排出来的当轮就派出去
        import advance  # queue/advance.py：按实验表推下一步，加进总队列
        done, stuck2 = advance.run(s, dry=a.dry_run)
        if done and not a.dry_run:
            try:
                s["taskq"][M.hub()] = taskq.local_status()
            except taskq.DiskError as e:
                s["taskq"][M.hub()], s["why"]["taskq"] = None, str(e)
    except Exception as e:  # noqa: BLE001
        done, stuck2 = [], {"advance:error": f"queue/advance.py 出错：{type(e).__name__}: {e}"[:300]}
    try:
        import feed  # queue/feed.py：送任务的唯一入口，转给 relay.py
        ev, st = feed.run_all(s, dry=a.dry_run)
        events.extend(ev)
        stuck.update(st)
    except Exception as e:  # noqa: BLE001
        stuck["feed:error"] = f"queue/feed.py 出错：{type(e).__name__}: {e}"[:300]
    for pid, v in {**stuck, **stuck2}.items():
        P[pid] = from_step(v)
    if not a.dry_run and (db is None or not NT.D.writable(db)):
        P[alert_id("db", M.hub())] = problem(f"hub 的数据库 {NT.D.path()} 写不了（在网络盘上，或是新版本的 "
                                             "taskq 建的）：每次检查都会把问题再推一遍", level="urgent")
    P = drop_acked(P, db, dry=a.dry_run)
    update(P, s["now"], db, dry=a.dry_run)
    if not a.dry_run and db is not None and NT.D.writable(db):
        try:
            progress_snapshot(s, db)
        except Exception as e:  # noqa: BLE001
            print(f"进度快照没记上：{type(e).__name__}: {e}", file=sys.stderr)
    if a.dry_run:
        for pid, p in sorted(P.items()):
            wait = f"/持续 {p['after']} 分钟才报" if p["after"] else ""
            print(f"[{p['level']}/{p['kind']}{wait}] {pid}\n    {p['msg']}")
        print("\n送任务、补作业：" + "；".join(events))
        print(f"{M.hub()} 按状态推进：" + ("；".join(done) or "没有要排的"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
