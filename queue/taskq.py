#!/usr/bin/env python3
"""DICC 自己的训练任务队列（2026-10-17）：Slurm 上排着的作业只负责申卡，开始后由 scripts/queue/dicc_worker.sh 从这里
按顺序领任务（一张卡同时训 2 折），训完一个再领下一个，没有能领的就退出。Slurm 作业名不再决定训哪一折。

队列目录（DICC 上，家目录各节点共用）：~/.cache/phd/taskq/
  tasks.tsv     任务：顺序、档位（Nice，越小越先）、key（数据集/训练器/fold）、数据集编号、训练器、fold、种类
                （nnunet / swin / umamba / stunet，worker 只领和自己作业同一种类的）
  claims/<key>  正在训的任务（mkdir 原子领取）：info 里是 worker 的作业号、主机、领取和最近一次心跳的时间
  failures/<key> 训练失败的记录，失败 2 次的不再领（健康检查提醒）；整个队列 3 小时内失败 3 次就暂停领任务
                （多半是节点或环境坏了，免得把所有任务都标成失败、排队的作业一个个空跑掉）
  released/<key> 最近一次交还的时间：交还之后训练日志没再更新的，马上可以领（不用等 15 分钟）
  ENABLED       总开关：有这个文件时作业才按队列领任务；删掉后作业照旧训自己那一折
  lock          改队列时的锁（mkdir）
不领的任务：已经训完（checkpoint_final 或 ARCHIVED，并且有 validation/summary.json）；别处正在训（claims 里有、心跳
  20 分钟内，或者这一折的训练日志 15 分钟内更新过）；lab 等别的机器在训（~/.cache/phd/claimed_elsewhere.txt）。

DICC 上（worker 调用）：
    taskq.py claim --worker <作业号> --kind nnunet [--free-slots N] [--patience-until 时间] [--exclude key...]
        领一个 → 打印 "key 数据集编号 训练器 fold"，退出码 0。没有能领的退出码 10；排在前面的折刚停下、还在等它
        （--patience-until 之前）退出码 11；队列因为连续失败暂停了退出码 12；其他退出码都是队列本身出错。
    taskq.py beat --worker <作业号> <key>...            心跳
    taskq.py release --worker <作业号> <key>... [--failed 原因]
box 上：
    taskq.py init [--write]       从 Slurm 上排着的 rq1_ 作业生成任务表（不带 --write 只打印）
    taskq.py status               打印队列（DICC 上直接读；box 上 ssh 过去读）
"""
import argparse
import glob
import json
import os
import shutil
import socket
import subprocess
import sys
import time

TQ = os.path.expanduser(os.environ.get("TASKQ_DIR", "~/.cache/phd/taskq"))  # TASKQ_DIR 只给测试用
CLAIMS_ELSEWHERE = os.path.expanduser(os.environ.get("RQ1_CLAIMS", "~/.cache/phd/claimed_elsewhere.txt"))
PLANS = "nnUNetResEncUNetMPlans__3d_fullres"
FIELDS = ["order", "nice", "key", "dsid", "trainer", "fold", "kind"]
STALE_BEAT = int(os.environ.get("TASKQ_STALE_BEAT", 20 * 60))  # 心跳超过这么久算过期（环境变量只给测试用）
FRESH_LOG = int(os.environ.get("TASKQ_FRESH_LOG", 15 * 60))    # 训练日志这么久内更新过算有人在训
MAX_FAIL = 2
PAUSE_N, PAUSE_WINDOW = 3, 3 * 3600  # 整个队列这么久内失败这么多次就暂停领任务
NOTHING, WAIT, PAUSED = 10, 11, 12   # claim 的退出码


def kind_of(trainer):
    t = trainer.lower()
    return "swin" if "swin" in t else "umamba" if "umamba" in t else "stunet" if "stunet" in t else "nnunet"


def safe(key):
    return key.replace("/", "__")


# ---------------- 文件 ----------------
def read_tasks(path=None):
    path = path or os.path.join(TQ, "tasks.tsv")
    out = []
    if not os.path.exists(path):
        return out
    for ln in open(path, encoding="utf-8"):
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.rstrip("\n").split("\t")
        if len(p) == len(FIELDS):
            t = dict(zip(FIELDS, p))
            t["order"], t["nice"] = int(t["order"]), int(t["nice"])
            out.append(t)
    return sorted(out, key=lambda t: (t["nice"], t["order"]))


def write_tasks(tasks, path=None):
    path = path or os.path.join(TQ, "tasks.tsv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("# " + "\t".join(FIELDS) + f"\t（taskq.py，{time.strftime('%F %T')}）\n")
        for t in tasks:
            f.write("\t".join(str(t[k]) for k in FIELDS) + "\n")
    os.replace(tmp, path)


class Lock:
    def __init__(self):
        self.d = os.path.join(TQ, "lock")

    def __enter__(self):
        os.makedirs(TQ, exist_ok=True)
        for _ in range(600):
            try:
                os.mkdir(self.d)
                return self
            except FileExistsError:
                try:
                    if time.time() - os.path.getmtime(self.d) > 120:  # 拿锁的进程死了
                        os.rename(self.d, f"{self.d}.stale{os.getpid()}")
                        shutil.rmtree(f"{self.d}.stale{os.getpid()}", ignore_errors=True)
                        continue
                except OSError:
                    pass
                time.sleep(0.5)
        raise SystemExit("拿不到队列的锁")

    def __exit__(self, *a):
        try:
            os.rmdir(self.d)
        except OSError:
            pass


def claim_info(key):
    p = os.path.join(TQ, "claims", safe(key), "info")
    try:
        return json.load(open(p))
    except Exception:  # noqa: BLE001
        return None


def fold_dir(results, t):
    ds = glob.glob(os.path.join(results, f"Dataset{t['dsid']}_*"))
    return os.path.join(ds[0], f"{t['trainer']}__{PLANS}", f"fold_{t['fold']}") if ds else None


def is_done(fd):
    return bool(fd) and (os.path.exists(os.path.join(fd, "checkpoint_final.pth")) or os.path.exists(os.path.join(fd, "ARCHIVED"))) \
        and os.path.exists(os.path.join(fd, "validation", "summary.json"))


def log_age(fd):
    logs = glob.glob(os.path.join(fd, "training_log_*.txt")) if fd else []
    return time.time() - max(os.path.getmtime(p) for p in logs) if logs else None


def elsewhere():
    """别的机器在训的 key（健康检查写的清单；超过 2 小时没更新时照常领，和 train_folds.sbatch 一样）。"""
    try:
        if time.time() - os.path.getmtime(CLAIMS_ELSEWHERE) > 7200:
            return set()
        return {ln.split("\t")[0] for ln in open(CLAIMS_ELSEWHERE) if ln.strip() and not ln.startswith("#")}
    except OSError:
        return set()


def failures(key):
    p = os.path.join(TQ, "failures", safe(key))
    return sum(1 for _ in open(p)) if os.path.exists(p) else 0


def released_at(key):
    try:
        return float(open(os.path.join(TQ, "released", safe(key))).read().split()[0])
    except (OSError, ValueError, IndexError):
        return None


def recent_failures(window=PAUSE_WINDOW):
    """整个队列最近 window 秒内的失败次数。"""
    n, cut = 0, time.time() - window
    for p in glob.glob(os.path.join(TQ, "failures", "*")):
        for ln in open(p, encoding="utf-8"):
            try:
                n += time.mktime(time.strptime(ln.split("\t")[0], "%Y-%m-%d %H:%M:%S")) >= cut
            except ValueError:
                pass
    return n


def state(t, results, other):
    """→ (状态, 说明)：done / claimed / busy / elsewhere / failed / free / stale（领取过期，可以重新领）。"""
    fd = fold_dir(results, t)
    if is_done(fd):
        return "done", ""
    ci = claim_info(t["key"])
    age = log_age(fd)
    if ci:
        if time.time() - ci.get("beat", 0) < STALE_BEAT or (age is not None and age < FRESH_LOG):
            return "claimed", f"作业 {ci.get('worker')} @ {ci.get('host')}"
        return "stale", f"作业 {ci.get('worker')} 的领取已过期"
    if age is not None and age < FRESH_LOG:
        rel = released_at(t["key"])
        if rel is None or time.time() - age > rel + 5:  # 交还之后日志又更新过（或从没交还过）：队列外有人在训
            return "busy", f"训练日志 {age / 60:.0f} 分钟前更新过"
    if t["key"] in other:
        return "elsewhere", "别的机器在训"
    if failures(t["key"]) >= MAX_FAIL:
        return "failed", f"失败 {failures(t['key'])} 次"
    return "free", ""


# ---------------- DICC 上：领、心跳、交还 ----------------
def cmd_claim(a):
    results = os.environ.get("nnUNet_results") or sys.exit("没有 nnUNet_results（先 source envs/nnunet_paths.sh）")
    other = elsewhere()
    if recent_failures() >= PAUSE_N:
        return PAUSED
    busy_ahead = 0  # 排在前面、刚停下（日志 15 分钟内更新过、没人领）的折
    with Lock():
        for t in read_tasks():
            if t["kind"] != a.kind or t["key"] in a.exclude:
                continue
            st, _ = state(t, results, other)
            if st == "busy":
                busy_ahead += 1
            if st not in ("free", "stale"):
                continue
            if busy_ahead >= a.free_slots and time.time() < a.patience_until:
                return WAIT  # 给排在前面的留着位置，等它的日志过了 15 分钟再领
            d = os.path.join(TQ, "claims", safe(t["key"]))
            if st == "stale":
                gone = f"{d}.old{os.getpid()}"
                try:
                    os.rename(d, gone)
                    shutil.rmtree(gone, ignore_errors=True)
                except OSError:
                    continue
            try:
                os.makedirs(os.path.dirname(d), exist_ok=True)
                os.mkdir(d)
            except FileExistsError:
                continue
            now = time.time()
            json.dump(dict(worker=a.worker, host=socket.gethostname().split(".")[0], since=now, beat=now), open(os.path.join(d, "info"), "w"))
            print(t["key"], t["dsid"], t["trainer"], t["fold"])
            return 0
    return NOTHING


def cmd_beat(a):
    for key in a.keys:
        p = os.path.join(TQ, "claims", safe(key), "info")
        ci = claim_info(key)
        if ci and str(ci.get("worker")) == str(a.worker):
            ci["beat"] = time.time()
            json.dump(ci, open(p + ".tmp", "w"))
            os.replace(p + ".tmp", p)
    return 0


def cmd_release(a):
    for key in a.keys:
        d = os.path.join(TQ, "claims", safe(key))
        ci = claim_info(key)
        if a.failed:
            os.makedirs(os.path.join(TQ, "failures"), exist_ok=True)
            with open(os.path.join(TQ, "failures", safe(key)), "a") as f:
                f.write(f"{time.strftime('%F %T')}\t作业 {a.worker}\t{a.failed}\n")
        if ci and str(ci.get("worker")) == str(a.worker):
            os.makedirs(os.path.join(TQ, "released"), exist_ok=True)
            with open(os.path.join(TQ, "released", safe(key)), "w") as f:
                f.write(f"{time.time()} {a.worker}\n")
            shutil.rmtree(d, ignore_errors=True)
    return 0


# ---------------- 状态（DICC 上直接读，box 上 ssh 过去） ----------------
def local_status():
    results = os.environ.get("nnUNet_results") or os.path.expanduser("~/st/BRAIN_TUMOR/nnUNet/results")
    other = elsewhere()
    out = []
    for t in read_tasks():
        st, note = state(t, results, other)
        ci = claim_info(t["key"]) or {}
        out.append(dict(t, state=st, note=note, worker=ci.get("worker"), since=ci.get("since"),
                        log_age=log_age(fold_dir(results, t)), failures=failures(t["key"])))
    nf = recent_failures()
    return dict(enabled=os.path.exists(os.path.join(TQ, "ENABLED")), tasks=out, recent_failures=nf, paused=nf >= PAUSE_N)


def remote_status(host):
    """box 上：ssh 到 host 跑本脚本的 --json-status → dict 或 None。"""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "envs"))
    import machines as M
    m = M.MACHINES[host]
    try:
        r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", m["ssh"],
                            f"nnUNet_results={M.results(host)} python3 - --json-status"],
                           stdin=open(os.path.abspath(__file__)), capture_output=True, text=True, timeout=180)
        i = r.stdout.find("{")
        return json.loads(r.stdout[i:]) if r.returncode == 0 and i >= 0 else None
    except Exception:  # noqa: BLE001
        return None


def cmd_status(a):
    s = local_status() if os.path.isdir(TQ) and os.path.exists(os.path.join(TQ, "tasks.tsv")) else remote_status(a.host)
    if s is None:
        print("读不到队列")
        return 1
    print(f"总开关：{'开' if s['enabled'] else '关'}；任务 {len(s['tasks'])} 个"
          + (f"；{PAUSE_WINDOW // 3600} 小时内失败 {s.get('recent_failures')} 次，暂停领任务" if s.get("paused") else ""))
    n = {}
    for t in s["tasks"]:
        n[t["state"]] = n.get(t["state"], 0) + 1
        if t["state"] not in ("done",):
            print(f"  {t['nice']:>5} {t['kind']:7} {t['key']:60} {t['state']} {t['note']}")
    print("；".join(f"{k} {v}" for k, v in sorted(n.items())))
    return 0


# ---------------- box 上：从 Slurm 队列生成任务表 ----------------
def cmd_init(a):
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.dirname(os.path.dirname(here))
    sys.path[:0] = [os.path.join(repo, "envs"), os.path.join(repo, "scripts", "monitor"), here]
    import machines as M
    from health import job_keys
    import refill
    F, jk = refill.folds(), job_keys()
    m = M.MACHINES[a.host]
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", m["ssh"], "squeue -u $USER -h -r -o '%j|%i|%T|%y'"],
                       capture_output=True, text=True, timeout=120, check=True)
    best = {}
    for ln in r.stdout.splitlines():
        name, jid, st, nice = ln.split("|")
        key = jk.get(name)
        if not key or key not in F:
            continue
        base = int(jid.split("_")[0])
        cur = best.get(key)
        cand = (int(nice), base)
        if cur is None or cand < cur:
            best[key] = cand
    tasks = []
    for key, (nice, base) in sorted(best.items(), key=lambda kv: (kv[1], kv[0])):
        f = F[key]
        tasks.append(dict(order=base, nice=nice, key=key, dsid=f["dsid"], trainer=f["tr"], fold=f["fold"], kind=kind_of(f["tr"])))
    for t in tasks:
        print(f"{t['nice']:>5} {t['order']} {t['kind']:7} {t['key']}")
    print(f"共 {len(tasks)} 个任务")
    if a.write:
        body = "# " + "\t".join(FIELDS) + f"\t（taskq.py init，{time.strftime('%F %T')}）\n" + \
               "".join("\t".join(str(t[k]) for k in FIELDS) + "\n" for t in tasks)
        cmd = "mkdir -p ~/.cache/phd/taskq && cat > ~/.cache/phd/taskq/tasks.tsv.new && mv ~/.cache/phd/taskq/tasks.tsv.new ~/.cache/phd/taskq/tasks.tsv"
        subprocess.run(["ssh", "-o", "BatchMode=yes", m["ssh"], cmd], input=body, text=True, check=True, timeout=60)
        print("已写到", a.host)
    return 0


def main(argv):
    if argv[:1] == ["--json-status"]:
        print(json.dumps(local_status()))
        return 0
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("claim")
    p.add_argument("--worker", required=True)
    p.add_argument("--kind", required=True)
    p.add_argument("--free-slots", type=int, default=1)
    p.add_argument("--patience-until", type=float, default=0)
    p.add_argument("--exclude", nargs="*", default=[])
    p = sub.add_parser("beat")
    p.add_argument("--worker", required=True)
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("release")
    p.add_argument("--worker", required=True)
    p.add_argument("keys", nargs="+")
    p.add_argument("--failed")
    p = sub.add_parser("status")
    p.add_argument("--host", default="dicc")
    p = sub.add_parser("init")
    p.add_argument("--host", default="dicc")
    p.add_argument("--write", action="store_true")
    a = ap.parse_args(argv)
    return dict(claim=cmd_claim, beat=cmd_beat, release=cmd_release, status=cmd_status, init=cmd_init)[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
