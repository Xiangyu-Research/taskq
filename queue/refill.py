#!/usr/bin/env python3
"""训练机自动补任务（2026-10-16）：pueue 训练机（envs/machines.py 里 scheduler=pueue、角色 train 的，现在是 lab）的卡快空了，
就按档位从待办里取下一批，排好训练和 box 上的下游。由 box 上的健康检查（scripts/monitor/health.py，每 30 分钟）调用。

待办 = 两处的合起来，按档位（Nice，越小越先）、再按先后排：
  - Slurm 上排着、从没开始过的折（任何机器上都没有这一折的训练进度；hold 的不算），档位 = 作业的 Nice；
  - 这台机器 pueue 里暂存（stashed）的训练任务，档位 = −pueue 优先级（DICC 每人每个 QOS 最多排 10 个作业，交不进去的放这里）。
什么时候补：卡上的组里没有排着（queued）的训练，或者待办里最好的那一折档位比排着的都靠前；box 上有给这台机器传数据的任务没做完时不补。
补多少：同一个实验（数据集 + 训练器）接下来的几折，凑够约 20 个折算小时（make_registry.HOURS；nnU-Net 3 折、重模型 1 折）。
数据：这台机器上没有这个数据集的预处理结果时，用 scripts/sync/tunnel_push.sh 传过去，传完再开始这几折（训练任务先暂存）；
  传完后盘上剩不到表里 disk 的提醒线就跳过这个实验，换下一个。传过去的数据集记在 ~/.cache/phd/refill.json，这台机器上用它的
  训练和 box 上的拉回、归档都做完后自动删掉（Dataset101 不删）。
怎么取：Slurm 上的先 scancel --state=PENDING 并确认已经取消（10/16 起从没开始过的折挪过来不用逐个确认）；这台机器上
  q <组> --priority=−Nice 加训练任务（暂存的直接 enqueue）；box 上每折加拉回 → 推理、拉回 → 归档。
  已经在 box 上有拉回任务（别处来的）的折不取。每次补的内容记到 box 的 ~/refill.log，并作为事件推送到手机。

    python3 scripts/queue/refill.py            看现在会不会补、补什么（不改任何东西）
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
sys.path.insert(0, os.path.join(REPO, "research", "RQ1", "nnunet"))
sys.path.insert(0, os.path.join(REPO, "scripts", "monitor"))  # health.py（这里的函数里才 import，避免互相 import）
import machines as M  # noqa: E402
import make_registry as mr  # noqa: E402

PLANS = "nnUNetResEncUNetMPlans__3d_fullres"
TARGET_H = 20  # 一次补约多少个折算小时
STATE = os.path.expanduser("~/.cache/phd/refill.json")
LOG = os.path.expanduser("~/refill.log")
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]
LIVE = ("Running", "Queued", "Stashed", "Paused", "Locked")


def folds():
    """key → dict(tag, ds, dsid, tr, fold, model, exp)：make_registry.SPEC 里有 Slurm 作业名标签的。"""
    out = {}
    for exp, desc, model, dsid, tr, fs, tag in mr.SPEC:
        for f in fs:
            out[f"{mr.DS[dsid]}/{tr}/{f}"] = dict(tag=tag or exp.lower(), ds=mr.DS[dsid], dsid=dsid, tr=tr, fold=f, model=model,
                                                exp=f"{exp} {desc}")
    return out


def gpu_group(m):
    return next(g for g, (_, gpu, _) in m["groups"].items() if gpu is not None)


def started(s, key):
    from health import fold_info
    return any(v[1] > 0 or v[0] for v in fold_info(s, key).values())


def candidates(s, name):
    """待办 → [dict(key, nice, src='slurm'|'stash', ids, host)]，按档位、先后排好。"""
    from health import job_keys, task_key
    F, jk = folds(), job_keys()
    box = s["pueue"].get(M.hub()) or {}
    fetching = {task_key(t["command"])[1] for t in box.get("tasks", []) if t["status"] in LIVE and "fetch_fold.sh" in t["command"]}
    claimed = {task_key(t["command"])[1] for t in (s["pueue"].get(name) or {}).get("tasks", [])
               if t["status"] in LIVE and "train_task.sh" in t["command"]}  # 暂存的那份按暂存任务算，不重复从 Slurm 取
    out = []
    for host, d in s["slurm"].items():
        tq = s.get("taskq", {})
        if host in tq and (tq[host] is None or tq[host].get("enabled")):
            continue  # 这台 Slurm 机器开着自己的任务队列（10/17），或者读不到队列：从队列里取的做法还没写，先不从这里取
        rows = {}
        for jname, jid, st, _, why, *nice in d["squeue"]:
            key = jk.get(jname)
            if key:
                rows.setdefault(key, []).append((jid, st, why, int(nice[0]) if nice and nice[0].lstrip("-").isdigit() else 10 ** 6))
        for key, rs in rows.items():
            if key not in F or key in fetching or key in claimed or started(s, key):
                continue
            if any(st != "PENDING" or "Held" in why for _, st, why, _ in rs):
                continue
            out.append(dict(key=key, nice=min(n for *_, n in rs), src="slurm", host=host,
                            ids=sorted({j.split("_")[0] for j, *_ in rs}), order=min(j for j, *_ in rs)))
    for t in (s["pueue"].get(name) or {}).get("tasks", []):
        kind, key = task_key(t["command"])
        if t["status"] == "Stashed" and kind == "train" and key in F and key not in fetching:
            out.append(dict(key=key, nice=-int(t.get("priority") or 0), src="stash", host=name, ids=[t["id"]], order=f"{t['id']:09d}"))
    return sorted(out, key=lambda c: (c["nice"], c["src"] != "stash", str(c["order"])))


def plan(s, name):
    """→ (计划 dict 或 None, 原因)。计划：keys、nice、exp、ds、transfer（要传的 GB，0 = 不用传）、picks（候选）。"""
    from health import task_key
    m = M.MACHINES[name]
    if not s["ok"].get(name) or not s["ok"].get(M.hub()) or "error" in (s["pueue"].get(name) or {"error": 1}):
        return None, "连不上"
    if any(not s["ok"].get(h) for h in s["slurm"]) or len(s["slurm"]) < len(M.names("slurm")):
        return None, "Slurm 那边没收集到"
    box = s["pueue"].get(M.hub()) or {}
    if any(t["status"] in LIVE and (t["label"] or "").startswith(f"sync_{name}_") for t in box.get("tasks", [])):
        return None, "数据还在传"
    g = gpu_group(m)
    queued = [-int(t.get("priority") or 0) for t in s["pueue"][name]["tasks"]
              if t["group"] == g and t["status"] == "Queued" and task_key(t["command"])[0] == "train"]
    cands = candidates(s, name)
    if not cands:
        return None, "待办里没有能取的折"
    if queued and cands[0]["nice"] >= min(queued):
        return None, "排着的已经够了"
    F = folds()
    pr = s["probe"].get(name, {})
    have = set(pr.get("PREP", "").split())
    free = int(pr["DISK"]) / 1e9 if pr.get("DISK", "").isdigit() else None
    warn = m["disk"][1] if m["disk"] else 0
    tried = set()
    for c in cands:
        f = F[c["key"]]
        exp = (f["ds"], f["tr"])
        if exp in tried:
            continue
        tried.add(exp)
        gb = 0.0
        if f["ds"] not in have:
            gb = dataset_gb(f["ds"])
            if gb is None or free is None or free - gb < warn:
                continue
        picks, hours = [], 0.0
        for c2 in cands:
            f2 = F[c2["key"]]
            if (f2["ds"], f2["tr"]) == exp and c2["nice"] == c["nice"]:
                picks.append(c2)
                hours += mr.HOURS[f2["model"]]
                if hours >= TARGET_H:
                    break
        return dict(keys=[p["key"] for p in picks], picks=picks, nice=c["nice"], exp=f["exp"], ds=f["ds"], transfer=gb, hours=hours), ""
    return None, f"待办里的数据集都放不下（{name} 盘剩 {free:.0f} GB）" if free is not None else "不知道盘还剩多少"


def dataset_gb(ds):
    p = os.path.join(M.expand(M.MACHINES[M.hub()]["data"]), "nnUNet", "preprocessed", ds)
    if not os.path.isdir(p):
        return None
    r = subprocess.run(["du", "-sbL", p], capture_output=True, text=True)
    return int(r.stdout.split()[0]) / 1e9 if r.returncode == 0 else None


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


def execute(p, name, run=sh):
    """按计划取任务 → 一行说明。出错时抛异常（已经做了的步骤写在异常里）。"""
    F, m, hub = folds(), M.MACHINES[name], M.hub()
    done = []
    # 1. Slurm 上的：只取消还在排队的，确认取消了
    for c in p["picks"]:
        if c["src"] != "slurm":
            continue
        h = M.MACHINES[c["host"]]["ssh"]
        ids = " ".join(c["ids"])
        rc, out = run(f"squeue -h -j {','.join(c['ids'])} -o %T 2> /dev/null | sort -u", h)
        if rc or out.split() != ["PENDING"]:
            raise RuntimeError(f"{c['key']} 在 {c['host']} 上已经不是只在排队（{out or rc}），没有取消；已做：{done}")
        run(f"scancel --state=PENDING {ids}", h)
        rc, out = run(f"sleep 3; squeue -h -j {','.join(c['ids'])} -o %T 2> /dev/null | grep -v CANCELLED", h)
        if out.strip():
            raise RuntimeError(f"{c['key']} 的作业 {ids} 没取消掉（{out}）；已做：{done}")
        done.append(f"取消 {M.MACHINES[c['host']]['label']} {ids}")
    # 2. 训练机上的训练任务（要传数据时先暂存）
    g, wait = gpu_group(m), p["transfer"] > 0
    tids = []
    for c in p["picks"]:
        f = F[c["key"]]
        if c["src"] == "stash":
            tids.append(c["ids"][0])
            continue
        tr = "" if f["tr"] == "nnUNetTrainer_save10" else " " + f["tr"]
        cmd = f"bash scripts/queue/train_task.sh {f['dsid']} {f['fold']}{tr}"
        tids.append(q(name, f"{g} -p --priority={-p['nice']}{' --stashed' if wait else ''} --label {f['tag']}_f{f['fold']} -- {shlex.quote(cmd)}", run))
    done.append(f"{name} 训练任务 {tids}")
    if not wait:
        stash = [str(c["ids"][0]) for c in p["picks"] if c["src"] == "stash"]
        if stash:
            run(f"~/.local/bin/pueue enqueue {' '.join(stash)}", m["ssh"])
    # 3. 要传数据：box 上传完再放行这几个训练任务
    if wait:
        rel = M.MACHINES[name]["data"].replace("~/", "", 1) + "/nnUNet/preprocessed"
        src = M.expand(M.MACHINES[hub]["data"]) + "/nnUNet/preprocessed"
        cmd = (f"bash scripts/sync/tunnel_push.sh {name} {src} {rel} {p['ds']} && "
               f"ssh {m['ssh']} ~/.local/bin/pueue enqueue {' '.join(map(str, tids))}")
        q(hub, f"wait -p --label sync_{name}_{p['ds']} -- {shlex.quote(cmd)}", run)
        st = json.load(open(STATE)) if os.path.exists(STATE) else {}
        st.setdefault("transferred", {}).setdefault(name, [])
        if p["ds"] not in st["transferred"][name]:
            st["transferred"][name].append(p["ds"])
        os.makedirs(os.path.dirname(STATE), exist_ok=True)
        json.dump(st, open(STATE, "w"), indent=1)
        done.append(f"传 {p['ds']}（{p['transfer']:.0f} GB）")
    # 4. box 上的下游：拉回 → 推理；拉回 → 归档
    bt = M.expand(M.MACHINES[hub]["data"]) + "/nnUNet/results"
    for c in p["picks"]:
        f = F[c["key"]]
        model, lab, k = f"{f['ds']}/{f['tr']}__{PLANS}", f"{f['tag']}_f{f['fold']}", f["fold"]
        fid = q(hub, f"wait -p --label fetch_{lab} -- " + shlex.quote(f"bash scripts/queue/fetch_fold.sh {name} {model} {k}"), run)
        q(hub, f"gpu --after {fid} --label infer_{lab} -- "
               + shlex.quote(f"bash research/RQ1/analysis/run_predict_eval.sh {bt}/{model} {f['fold']}"), run)
        q(hub, f"wait --after {fid} --label archive_{lab} -- " + shlex.quote(f"bash scripts/queue/archive_fold.sh {name} {model} {f['fold']}"), run)
    fl = "、".join(F[k]["fold"] for k in p["keys"])
    msg = (f"{M.MACHINES[name]['label']} 自动接手 {p['exp']} fold {fl}（档位 Nice {p['nice']}，约 {p['hours']:.0f} 个折算小时）："
           + "；".join(done) + "；box 上排好了拉回、推理、归档")
    with open(LOG, "a") as fo:
        fo.write(f"{datetime.now():%F %T} {msg}\n")
    return msg


def cleanup(s, name, run=sh):
    """自动传过去的数据集：这台机器上没有用它的训练任务、box 上没有从这台机器拉回 / 归档它的任务了，就删掉 → [说明]。"""
    from health import task_key
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    lst = st.get("transferred", {}).get(name, [])
    if not lst or not s["ok"].get(name) or "error" in (s["pueue"].get(name) or {"error": 1}):
        return []
    box = s["pueue"].get(M.hub()) or {"error": 1}
    if "error" in box:
        return []
    msgs = []
    for ds in list(lst):
        if not re.fullmatch(r"Dataset\d+_\w+", ds) or ds.startswith("Dataset101_"):
            continue
        busy = any(t["status"] in LIVE and (task_key(t["command"])[1] or "").startswith(ds + "/") for t in s["pueue"][name]["tasks"])
        busy |= any(t["status"] in LIVE and ds in t["command"] and f" {name} " in f" {t['command']} " for t in box["tasks"])
        if busy:
            continue
        rc, out = run(f"rm -rf {M.MACHINES[name]['data']}/nnUNet/preprocessed/{ds}", M.MACHINES[name]["ssh"])
        if rc == 0:
            lst.remove(ds)
            msgs.append(f"{M.MACHINES[name]['label']} 上自动传过去的 {ds} 用完了，已删掉（box 上有完整的一份）")
            with open(LOG, "a") as fo:
                fo.write(f"{datetime.now():%F %T} {msgs[-1]}\n")
    json.dump(st, open(STATE, "w"), indent=1)
    return msgs


def run_all(s, dry=False):
    """health.py 调用：→ (事件说明列表, 补过的机器)。"""
    events, filled = [], set()
    for name in M.names("pueue"):
        if "train" not in M.MACHINES[name]["roles"]:
            continue
        p, why = plan(s, name)
        if dry:
            events.append(f"{name}：" + (f"会补 {p['exp']} {p['keys']}，Nice {p['nice']}，要传 {p['transfer']:.1f} GB" if p else f"不补（{why}）"))
            continue
        if p:
            try:
                events.append(execute(p, name))
                filled.add(name)
            except Exception as e:  # noqa: BLE001
                events.append(f"{M.MACHINES[name]['label']} 自动补任务出错：{e}")
                with open(LOG, "a") as fo:
                    fo.write(f"{datetime.now():%F %T} 出错：{e}\n")
        events += cleanup(s, name)
    return events, filled


if __name__ == "__main__":
    import health as H  # noqa: E402
    print("\n".join(run_all(H.gather(), dry=True)[0]))
