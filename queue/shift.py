#!/usr/bin/env python3
"""按时段开关 pueue 组（envs/policy.py 的 SHIFT，2026-10-30）：health.py 每次检查时调用。

每个时段只动两次，中间手动的暂停、恢复都不管：
  - 时段开始后：组暂停着、这张卡上没有别的用户的进程 → 恢复（有人在用就下次检查再看，直到时段结束）；
  - 时段结束后：这个时段是这里恢复的、组还开着 → 暂停，在跑的任务 SIGTERM 后放回队列（训练从存档接着训），
    给还没训完的折预排的推理（advance.py 加的带优先级的任务）删掉，下次开训后 advance.py 重排。
做了什么记在 ~/.cache/phd/shift.json（每个组最近一次恢复、暂停的时段日期），日志 ~/refill.log。
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


def group_tasks(host, g):
    rc, out, _ = C.run(f"{PUEUE} status -g {g} --json", host, timeout=60)
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


def pause(host, g):
    """暂停组，停掉在跑的任务并放回队列 → 说明。"""
    C.run(f"{PUEUE} pause -g {g} --wait", host, timeout=60)
    _, tasks = group_tasks(host, g)
    run = [i for i, t in (tasks or {}).items() if t["state"] == "Running"]
    if run:
        C.run(f"{PUEUE} kill -s SIGTERM {' '.join(map(str, run))}", host, timeout=60)
        for _ in range(60):
            _, tasks = group_tasks(host, g)
            if tasks and all(tasks[i]["state"] == "Done" for i in run):
                break
            time.sleep(5)
        back = [i for i in run if tasks and tasks[i]["state"] == "Done"
                and "Success" not in json.dumps(tasks[i]["status"])]
        if back:
            C.run(f"{PUEUE} restart --in-place {' '.join(map(str, back))}", host, timeout=60)
        pre = [i for i, t in (tasks or {}).items() if t["state"] == "Queued" and t["priority"] > 0
               and t["label"].startswith("infer:") and i not in back]
        if pre and any(tasks[i]["label"].startswith("train:") for i in back):
            C.run(f"{PUEUE} remove {' '.join(map(str, pre))}", host, timeout=60)
        names = ", ".join(tasks[i]["label"] for i in run) if tasks else str(run)
        return f"停了 {names}（放回队列：{back or '无'}；删掉预排的推理：{pre or '无'}）"
    return "没有在跑的任务"


def run(s=None, dry=False, now=None):
    """→ 说明列表。"""
    now = now or dt.datetime.now(TZ)
    st, msgs = load(), []
    for name, groups in getattr(P, "SHIFT", {}).items():
        host = M.MACHINES[name]["ssh"]
        for g, (start, end) in groups.items():
            k = f"{name}:{g}"
            rec = st.setdefault(k, {})
            cur, ended = window(now, start, end)
            if cur and rec.get("started") != cur:
                gs, tasks = group_tasks(host, g)
                if gs is None:
                    msgs.append(f"{k}：读不到 pueue 组的状态，下次再看")
                    continue
                if gs == "Running":
                    rec["started"], rec["manual"] = cur, True
                    msgs.append(f"{k}：时段开始，组已经开着（手动开的），不动")
                    continue
                who = others_on(host, M.MACHINES[name]["groups"][g][1])
                if who is None:
                    msgs.append(f"{k}：读不到卡上的进程，下次再看")
                elif who:
                    if rec.get("busy") != cur:
                        msgs.append(f"{k}：时段开始，卡上有别人的进程（{', '.join(sorted(set(who)))}），等下次检查")
                        rec["busy"] = cur
                elif dry:
                    msgs.append(f"{k}：会恢复组（{start}–{end}）")
                else:
                    C.run(f"{PUEUE} start -g {g}", host, timeout=60)
                    rec["started"], rec["manual"] = cur, False
                    n = sum(1 for t in (tasks or {}).values() if t["state"] == "Queued")
                    msgs.append(f"{k}：{start} 时段开始，卡空着，恢复组（排着 {n} 个任务）")
            elif ended and rec.get("started") == ended and rec.get("paused") != ended and not rec.get("manual"):
                if dry:
                    msgs.append(f"{k}：会暂停组（{end} 时段结束）")
                    continue
                gs, _ = group_tasks(host, g)
                if gs is None:
                    msgs.append(f"{k}：读不到 pueue 组的状态，下次再暂停")
                    continue
                rec["paused"] = ended
                msgs.append(f"{k}：{end} 时段结束，" + (pause(host, g) if gs == "Running" else "组已经停着，不动"))
    if not dry:
        save(st)
        if msgs:
            with open(os.path.expanduser("~/refill.log"), "a") as f:
                f.writelines(f"{now:%F %T} {m}\n" for m in msgs)
    return msgs


if __name__ == "__main__":
    print("\n".join(run(dry="--dry-run" in sys.argv)) or "不动")
