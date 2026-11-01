#!/usr/bin/env python3
"""按时段开关（envs/policy.py 的 SHIFT，2026-10-30）：health.py 每次检查时、送任务之前调用。

执行机（SITES 里的名字，10/31）：只在时段里领任务、开 worker（relay.py 看 allowed()）。
  - 时段开始后：卡上没有别的用户的进程 → 恢复它的 pueue 组、记下这个时段开了（有人在用就下次检查再看，直到时段结束）；
  - 时段结束后：暂停组、停掉 worker（SIGTERM，worker 交还手上的任务）；总队列里分给它、没训完的任务：有进度（在训、训过几轮）的
    改分给同一台上不按时段的执行机（day_site，从存档接着训，relay.py 的收编把存档带过去），其余交还总队列。
做了什么记在 ~/.cache/phd/shift.json（每项最近一次开、停的时段日期），日志 ~/refill.log。
"""
import datetime as dt
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "envs"))
import common as C  # noqa: E402
import machines as M  # noqa: E402
import policy as P  # noqa: E402

STATE = os.path.expanduser("~/.cache/phd/shift.json")
LOG = os.path.expanduser("~/refill.log")
PUEUE = "~/.local/bin/pueue"
TZ = dt.timezone(dt.timedelta(hours=8))


def window(now, start, end):
    """→ (这次时段开始的日期 或 None, 刚结束的时段的开始日期)。start、end 是 "HH:MM"，end 早于 start 时跨夜。"""
    def at(d, hm):
        h, m = map(int, hm.split(":"))
        return dt.datetime.combine(d, dt.time(h, m), TZ)
    today = now.date()
    for d in (today, today - dt.timedelta(days=1)):
        s = at(d, start)
        e = at(d + dt.timedelta(days=1) if end <= start else d, end)
        if s <= now < e:
            return d.isoformat(), None
    for d in (today, today - dt.timedelta(days=1)):
        e = at(d + dt.timedelta(days=1) if end <= start else d, end)
        if e <= now:
            return None, d.isoformat()
    return None, None


def dated(now, start, end, first=None, last=None):
    """window()，只算开始日期在 [first, last] 里的时段（None = 不限）。"""
    cur, ended = window(now, start, end)
    if cur is not None and ended is None:  # 首尾相接的时段（开始 = 结束）：上一个时段已经结束、这一个开始了
        prev = dt.date.fromisoformat(cur) - dt.timedelta(days=1)
        if start == end:
            ended = prev.isoformat()

    def inside(d):
        return d is not None and (first is None or d >= first) and (last is None or d <= last)
    return (cur if inside(cur) else None), (ended if inside(ended) else None)


def rule(name):
    """执行机 name 的时段 (开始, 结束, 第一个, 最后一个)；不按时段 → None。"""
    v = getattr(P, "SHIFT", {}).get(name)
    if not isinstance(v, (tuple, list)):
        return None
    return tuple(v) + (None,) * (4 - len(v))


def limited(name):
    return rule(name) is not None


def allowed(name, now=None):
    """执行机现在能不能领任务、开 worker：不按时段 → True；按时段的要在时段里、这个时段已经由 run() 开了（卡空着）。"""
    r = rule(name)
    if r is None:
        return True
    cur, _ = dated(now or dt.datetime.now(TZ), *r)
    rec = load().get(name) or {}
    return bool(cur) and rec.get("started") == cur and rec.get("paused") != cur


def day_site(host, but=None):
    """同一台主机上开着、不按时段的执行机（白天接手的）；没有 → None。"""
    return next((r for r in sorted(M.RELAY) if r != but and M.RELAY[r].get("ssh") == host and P.AUTO.get(r) and not limited(r)), None)


def load():
    try:
        with open(STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save(st):
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    tmp = STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(st, f, indent=1)
    os.replace(tmp, STATE)


def others_on(host, gpu):
    """这张卡上别的用户的进程 → [用户名]（看不到是谁的记"?"；没有进程、显存却占了 1 GB 以上也算有人）；读不到 → None。"""
    cmd = (f"read u m < <(nvidia-smi -i {gpu} --query-gpu=uuid,memory.used --format=csv,noheader,nounits | tr -d ,) || exit 3; "
           "echo \"MEM|$m\"; echo \"ME|$(whoami)\"; nvidia-smi --query-compute-apps=gpu_uuid,pid --format=csv,noheader | while IFS=', ' read g p; do "
           "[ \"$g\" = \"$u\" ] && echo \"APP|$(ps -o user= -p $p)\"; done; echo END")
    rc, out, _ = C.run(cmd, host, timeout=60)
    if rc or "END" not in out:
        return None
    lines = out.splitlines()
    me = next((ln[3:].strip() for ln in lines if ln.startswith("ME|")), "")
    apps = [ln[4:].strip() or "?" for ln in lines if ln.startswith("APP|")]
    mem = next((int(ln[4:]) for ln in lines if ln.startswith("MEM|") and ln[4:].strip().isdigit()), 0)
    who = [u for u in apps if u != me]
    return who if apps else (["?"] if mem > 1024 else [])


def group_tasks(host, g, pueue=PUEUE):
    rc, out, _ = C.run(f"{pueue} status -g {g} --json", host, timeout=60)
    if rc:
        return None, None
    d = json.loads(out)
    tasks = {}
    for i, t in d["tasks"].items():
        if t.get("group") != g:
            continue
        s = t["status"]
        tasks[int(i)] = dict(state=s if isinstance(s, str) else next(iter(s)), status=s, label=t.get("label") or "",
                             priority=t.get("priority", 0))
    return d["groups"][g]["status"], tasks


def hand_over(key, frm, to):
    """总队列里把分给 frm 的 key 改分给 to（box 上，10/31）→ 是否成功。"""
    import taskq
    taskq.central(["release", "--worker", frm, key])
    rc, _, _ = taskq.central(["take", "--worker", to, "--assign", key])
    return rc == 0


def give_back(key, frm):
    """总队列里交还分给 frm 的 key → 是否成功。"""
    import taskq
    rc, _, _ = taskq.central(["release", "--worker", frm, key])
    return rc == 0


def central_mine(name):
    """总队列里分给 name、没训完、没停的任务 → [key]。"""
    import taskq
    st = taskq.local_status() or {}
    return [t["key"] for t in st.get("tasks", []) if t.get("worker") == name and t["state"] not in ("done", "stopped")]


def site_tasks(name):
    """relay.py 最近一次读到的这台的各任务（~/.cache/phd/relay/status.json）→ {真 key: dict(state, runs, final)}。"""
    import relay_names as RN
    try:
        with open(os.path.join(RN.RELAY_DIR, "status.json")) as f:
            return (json.load(f).get(name) or {}).get("tasks") or {}
    except (OSError, ValueError):
        return {}


def has_progress(f):
    return bool(f) and (f.get("state") in ("claimed", "busy") or bool(f.get("final"))
                        or any((r.get("epochs_done") or 0) > 0 for r in f.get("runs") or []))


def stop_group(host, g, pueue, wait=False):
    """暂停组、SIGTERM 在跑的任务、等它们结束 → (在跑的任务号, 结束后的任务表)。"""
    C.run(f"{pueue} pause -g {g}" + (" --wait" if wait else ""), host, timeout=60)
    _, tasks = group_tasks(host, g, pueue)
    run = [i for i, t in (tasks or {}).items() if t["state"] == "Running"]
    if run:
        C.run(f"{pueue} kill -s SIGTERM {' '.join(map(str, run))}", host, timeout=60)
        for _ in range(60):
            _, tasks = group_tasks(host, g, pueue)
            if tasks and all(tasks[i]["state"] == "Done" for i in run if i in tasks):
                break
            time.sleep(5)
    return run, tasks or {}


def stop_site(name):
    """时段结束：暂停执行机的 worker 组、SIGTERM worker（它交还手上的任务），总队列里分给它的任务：有进度的交给白天的执行机，其余交还 → 说明。"""
    c = M.RELAY[name]
    host, g = c["ssh"], c["worker"]["group"]
    run, _ = stop_group(host, g, c.get("pueue") or PUEUE, wait=True)
    have, to = site_tasks(name), day_site(host, but=name)
    moved, gave = [], []
    for k in central_mine(name):
        if to and has_progress(have.get(k)) and hand_over(k, name, to):
            moved.append(k)
        elif give_back(k, name):
            gave.append(k)
    return (f"停了 worker（pueue 任务 {run or '无'}）；交给 {to or '（没有白天的执行机）'}：{', '.join(moved) or '无'}；"
            f"交还总队列：{', '.join(gave) or '无'}")


def run_site(name, r, now, st, dry):
    """按时段的执行机 → 说明列表。"""
    c, msgs = M.RELAY.get(name), []
    if not c or not P.AUTO.get(name) or name in getattr(P, "OFFLINE", {}):
        return msgs
    host, g = c["ssh"], c["worker"]["group"]
    rec = st.setdefault(name, {})
    cur, ended = dated(now, *r)
    if cur and rec.get("started") != cur:
        gs, _ = group_tasks(host, g, c.get("pueue") or PUEUE)
        if gs is None:
            return [f"{name}：读不到 pueue 组的状态，下次再看"]
        who = others_on(host, c["groups"][g][1])
        if who is None:
            msgs.append(f"{name}：读不到卡上的进程，下次再看")
        elif who:
            if rec.get("busy") != cur:
                msgs.append(f"{name}：时段开始，卡上有别人的进程（{', '.join(sorted(set(who)))}），等下次检查")
                rec["busy"] = cur
        elif dry:
            msgs.append(f"{name}：会开始领任务（{r[0]}–{r[1]}）")
        else:
            if gs != "Running":
                C.run(f"{c.get('pueue') or PUEUE} start -g {g}", host, timeout=60)
            rec["started"] = cur
            msgs.append(f"{name}：{r[0]} 时段开始，卡空着，开始领任务、开 worker")
    elif ended and rec.get("started") == ended and rec.get("paused") != ended:
        if dry:
            return [f"{name}：会停 worker、交出没训完的任务（{r[1]} 时段结束）"]
        rec["paused"] = ended
        msgs.append(f"{name}：{r[1]} 时段结束，" + stop_site(name))
    return msgs


def run(s=None, dry=False, now=None):
    """→ 说明列表。"""
    now = now or dt.datetime.now(TZ)
    st, msgs = load(), []
    for name in getattr(P, "SHIFT", {}):
        if rule(name):
            msgs += run_site(name, rule(name), now, st, dry)
    if not dry:
        save(st)
        if msgs:
            with open(LOG, "a") as f:
                f.writelines(f"{now:%F %T} {m}\n" for m in msgs)
    return msgs


if __name__ == "__main__":
    print("\n".join(run(dry="--dry-run" in sys.argv)) or "不动")
