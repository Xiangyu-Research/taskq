#!/usr/bin/env python3
"""夜班开关（2026-10-19，取代 10/16 删掉的 gpu0_gate.sh）：管 envs/machines.py 里 shifts 写的组（lab 的 gpu0，0 号卡和别人共用）。
cron 每 30 分钟在那台机器上跑（scripts/queue/setup.sh 写进去）：

  - 时间段里（lab：20:00–次日 12:00），组停着时：卡上过去 N 分钟（lab 45 分钟）只有我们自己账号的进程或者空着
    （GPU 使用记录 ~/gpu_usage/，scripts/monitor/gpu_log.sh，每分钟一行；记录要够 N - 5 条，别人的进程显存都算占用），
    此刻也没有别人的进程，就打开组（pueue start -g）。没等到就等下一次（30 分钟后）再看，直到时间段结束。
    打开后不再看别人用不用（10/13 定：不写"别人用卡就停我们训练"的看守）。组里没有任务时由 box 的 refill.py 替它领。
  - 时间段外，组开着或者组里还有在训的任务：先停住组（pueue pause -g --wait，不再开始新任务），再用 SIGTERM 停掉在训的任务
    （run_task.sh 停训练，下次从存档接着训，存档之后的几个 epoch 要重训），原地放回队列（任务号不变，组停着所以只排着）；
    领取由 refill.py 续着，下一晚打开组时接着训。

日志：~/shift_gate.log（只记状态变化，"别人在用、先不开"每晚只记第一次）。
    python3 scripts/queue/shift_gate.py            按现在的时间做
    python3 scripts/queue/shift_gate.py --dry-run  只说会做什么
"""
import csv
import glob
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(HERE)), "envs"))
import machines as M  # noqa: E402

P = os.path.expanduser("~/.local/bin/pueue")
GPU_LOG = os.path.expanduser(os.environ.get("GPU_LOG_DIR", "~/gpu_usage"))
STATE = os.path.expanduser("~/.cache/phd/shift_gate.json")
LOG = os.path.expanduser("~/shift_gate.log")
FREE_MIB = 500  # 卡上没有进程时，显存在这以下算空着
ME = os.environ.get("USER") or subprocess.run(["id", "-un"], capture_output=True, text=True).stdout.strip()


def hm(s):
    h, m = s.split(":")
    return int(h) * 60 + int(m)


def in_shift(now, beg, end):
    t, b, e = now.hour * 60 + now.minute, hm(beg), hm(end)
    return b <= t < e if b < e else (t >= b or t < e)


def shift_day(now, beg):
    """这一晚的日期（过了午夜还算前一天开始的那一晚）。"""
    return (now - timedelta(days=1) if now.hour * 60 + now.minute < hm(beg) else now).strftime("%F")


def quiet(rows, now, gpu, minutes, me=ME):
    """过去 minutes 分钟这张卡只有我们的进程或空着 → (行不行, 原因)。rows：GPU 使用记录的行（dict）。"""
    since = now - timedelta(minutes=minutes)
    rs = [r for r in rows if r["gpu"] == str(gpu) and since <= datetime.strptime(r["time"], "%Y-%m-%d %H:%M:%S") <= now]
    if len({r["time"] for r in rs}) < minutes - 5:
        return False, f"GPU 使用记录只有 {len({r['time'] for r in rs})} 分钟（要 {minutes - 5}）"
    others = sorted({r["user"] or "?" for r in rs if r["pid"] and r["user"] != me})
    if others:
        return False, f"过去 {minutes} 分钟有别人的进程（{'、'.join(others)}）"
    if any(not r["pid"] and float(r["mem_used_mib"] or 0) > FREE_MIB for r in rs):
        return False, f"过去 {minutes} 分钟有时没有进程但显存超过 {FREE_MIB} MiB（看不到的进程）"
    return True, ""


def read_rows(now):
    rows = []
    for f in sorted(glob.glob(os.path.join(GPU_LOG, "gpu_*.csv")))[-2:]:
        with open(f) as fo:
            rows += list(csv.DictReader(fo))
    return rows


def busy_now(gpu, me=ME):
    """此刻卡上别人的进程 → [账号]（nvidia-smi 读不出来时当成有人：['?']）。"""
    r = subprocess.run(["nvidia-smi", "-i", str(gpu), "--query-compute-apps=pid", "--format=csv,noheader"],
                       capture_output=True, text=True)
    if r.returncode:
        return ["?"]
    users = []
    for pid in r.stdout.split():
        u = subprocess.run(["ps", "-o", "user=", "-p", pid], capture_output=True, text=True).stdout.strip()
        if u != me:
            users.append(u or "?")
    return users


def is_live(t):
    return isinstance(t["status"], dict) and next(iter(t["status"])) in ("Running", "Paused") or t["status"] in ("Running", "Paused")


def decide(now, g, beg, end, minutes, group_status, tasks, rows, busy, me=ME):
    """→ (动作, 说明)。动作：None | "open" | "stop"。tasks：这个组的 pueue 任务（status --json 里的）。"""
    running = [i for i, t in tasks.items() if is_live(t)]
    if in_shift(now, beg, end):
        if group_status == "Running":
            return None, ""
        ok, why = quiet(rows, now, g, minutes, me)
        if ok and busy:
            ok, why = False, f"此刻卡上有别人的进程（{'、'.join(busy)}）"
        return ("open", f"过去 {minutes} 分钟卡上只有我们的进程或空着，打开组") if ok else (None, why)
    if group_status != "Paused" or running:
        return "stop", f"到了 {end}，停住组" + (f"，停掉在训的任务 {' '.join(sorted(running, key=int))} 放回队列" if running else "")
    return None, ""


def pueue(*a):
    r = subprocess.run([P, *a], capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()


def status():
    rc, out = pueue("status", "--json")
    if rc:
        raise RuntimeError(f"pueue 没有响应：{out[-200:]}")
    return json.loads(out)


def log(msg):
    with open(LOG, "a") as fo:
        fo.write(f"{datetime.now():%F %T} {msg}\n")


def stop(group, d):
    """停住组；SIGTERM 停掉在训的任务（最多等 2 分钟，不行再 kill），原地放回队列。"""
    pueue("pause", "-g", group, "--wait")
    ids = [i for i, t in d["tasks"].items() if t["group"] == group and is_live(t)]
    if not ids:
        return []
    pueue("kill", "-s", "SIGTERM", *ids)
    for _ in range(24):
        time.sleep(5)
        if not any(is_live(status()["tasks"][i]) for i in ids):
            break
    else:
        pueue("kill", *ids)
        time.sleep(10)
    rc, out = pueue("restart", "--in-place", *ids)
    return ids if rc == 0 else ids + [f"（放回队列出错：{out[-150:]}）"]


def main(argv):
    dry = "--dry-run" in argv
    me = M.whoami()
    shifts = M.shift_groups(me) if me else {}
    if not shifts:
        return 0
    d = status()
    now = datetime.now()
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    for group, (beg, end, minutes) in shifts.items():
        gpu = M.MACHINES[me]["groups"][group][1]
        gs = (d["groups"].get(group) or {}).get("status")
        if gs is None:
            log(f"pueue 里没有组 {group}（先跑 scripts/queue/setup.sh）")
            continue
        tasks = {i: t for i, t in d["tasks"].items() if t["group"] == group}
        rows = read_rows(now) if in_shift(now, beg, end) and gs != "Running" else []
        busy = busy_now(gpu) if rows else []
        act, why = decide(now, gpu, beg, end, minutes, gs, tasks, rows, busy)
        if dry:
            print(f"{group}（{gpu} 号卡，{beg}–{end}，组 {gs}）：{act or '不动'} {why}")
            continue
        if act == "open":
            rc, out = pueue("start", "-g", group)
            log(f"{group}：{why}" if rc == 0 else f"{group}：打开组出错：{out[-200:]}")
        elif act == "stop":
            ids = stop(group, d)
            log(f"{group}：到了 {end}，停住组" + (f"；停掉任务 {' '.join(ids)}，放回队列，下一晚接着训" if ids else ""))
        elif why:
            k = f"{group}:{shift_day(now, beg)}"
            if st.get("waited") != k:  # 每晚只记第一次
                log(f"{group}：先不开，{why}（每 30 分钟再看，到 {end} 为止）")
                st["waited"] = k
    if not dry:
        os.makedirs(os.path.dirname(STATE), exist_ok=True)
        json.dump(st, open(STATE, "w"))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
