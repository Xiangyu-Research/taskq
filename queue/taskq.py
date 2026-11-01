#!/usr/bin/env python3
"""训练任务队列（2026-10-17 起；10/18 起任务不限于 nnU-Net；10/31 起总队列在 box，各执行机有自己的缓冲）。

总队列（box 的 ~/.cache/phd/taskq/）：所有训练和要进队列的步骤。只有 box 写它（实验表加任务、改档位、停止都在 box 上做）；
  分给某台执行机的任务记为 claims/<key>/info 里的 assigned: <机器>：没有心跳、不过期，只靠交还结束（scripts/queue/feed.py 交还）。
缓冲（每台执行机的 ~/.cache/phd/taskq/，中转执行机是它自己的 R/queue/）：总队列里分给它的任务的原样副本（order、nice、exp、
  recipe、params 不变），加一个 BUFFER 标记。worker（scripts/queue/dicc_worker.sh）在缓冲上领、心跳、交还，box 不在线时照常跑。
  缓冲里改任务只由 feed.py 做（带 TASKQ_FEED=1）。训完以后 box 拉回，box 上有了结果才算训完（总队列的 done）。

每个任务 = 做法 + 参数（scripts/queue/recipes.py，解析在 scripts/queue/spec.py）：环境、命令、结果文件夹、怎么算训完都由
任务说明决定，这里不认识具体模型。属于实验表（research/*/experiments.py）的任务记下实验 id。

队列目录（总队列和缓冲一样）：
  tasks.jsonl   任务，每行一个：{order, nice（档位，越小越先）, key, exp, recipe, params}
  claims/<key>  正在训的任务（mkdir 原子领取）：info 里是 worker（DICC 作业号等）、主机、领取和最近一次心跳的时间，
                可选 ttl（心跳多久不更新算过期，默认 20 分钟）；总队列里是 assigned（分给哪台执行机，不过期）
  failures/<key> 训练失败的记录，失败 2 次的不再领（健康检查提醒）；整个队列 3 小时内失败 3 次就暂停领任务
                （多半是节点或环境坏了，免得把所有任务都标成失败、排队的作业一个个空跑掉）
  skips/<key>   某台机器训不了的任务（release --skip）：这台机器不再领它，别的机器照常领；不算失败
  inputs/<key>  缺输入（11/1）：worker 开训前查到输入不全（run_task.sh 退出码 4），交还并记在这里；不再领它，等补齐输入的一方
                （relay.py 补传，feed.py 过一阵重试）taskq.py input-ok 撤掉。不算失败
  released/<key> 最近一次交还的时间：交还之后训练日志没再更新的，马上可以领（不用等 15 分钟）
  stopped/<key> 停止的任务（taskq.py stop，10/30）：不再领；在训的 worker 每 5 秒看一次，30 秒内停下、交还。
                缓冲里"依赖记录"（put 的 hold）也是停止的：只给别的任务的 after 看，不训
  ENABLED       总开关：没有这个文件时，排队的 worker 作业开始后直接结束（scripts/queue/worker_start.sh）
  BUFFER        这是执行机的缓冲
  lock          改队列时的锁（mkdir）
不领的任务：已经训完（任务说明的 done）；别处正在训（claims 里有、心跳在 ttl 内，或者训练日志 15 分钟内更新过）；
  说明解析不了的（参数写错，status 里显示 bad）。

worker 调用（缓冲上）：
    taskq.py next --worker <作业号> --envs <环境,…> [--device gpu|cpu] [--free-slots N] [--patience-until 时间] [--exclude key...]
        领一个本机有环境的（任务说明写了 machines 的，只在列出的机器上领；只领 device 和 --device 一样的，默认 gpu）→ 打印两行：key、run_task.sh 的参数（shell 引号），退出码 0。没有能领的退出码 10；
        排在前面的刚停下、还在等它（--patience-until 之前）退出码 11；队列因为连续失败暂停了退出码 12；
        其他退出码都是队列本身出错（包括没有任务表：退出码 2）。
    taskq.py beat --worker <作业号> <key>...            心跳
    taskq.py release --worker <作业号> <key>... [--failed 原因] [--skip 原因]
    taskq.py missing --worker <作业号> <key> [--why 说明]   缺输入（11/1）：交还，记 inputs/<key>，补齐前不再领
box 上（总队列；在别的机器上跑这些命令时退出码 2，缓冲上只有带 TASKQ_FEED=1 的 feed.py 能改）：
    taskq.py add [--nice <档位>] --exp <实验 id> [--only <参数>=<取值>[,<取值>…]]…   从实验表加（已有的跳过；不给 --nice 用表里的档位；
                                       --only 只加 each 里这个参数取这些值的任务，如 --only fold=1,2）
    taskq.py add --nice <档位> --recipe <做法> k=v…                 加一个实验表以外的任务
    taskq.py add [--nice <档位>] --file <任务单.json>               按任务单加（别人的训练、测试；不给 --nice 用单子里的）
    taskq.py set <key> k=v…           改一个任务的参数（如 env=umamba；k= 空值 = 删掉这个参数，回到默认）
    taskq.py set-nice --nice <档位> <key>...
    taskq.py show [--json] <key>      任务的完整说明和状态（--json：说明写在 @@JSON 开头的一行里）
    taskq.py remove <key>...         删掉没人领着的任务（after 里有它的任务要一起删；结果文件不动；分出去的先 stop）
    taskq.py stop [--why 原因] <key>...   停止任务（10/30）：在训的 30 秒内停下、交还（存档是原子写的，从存档接着训），
                                      之后不再领它；分给执行机的由 box 下次检查时在那边停（feed.py / refill.py / relay.py）
    taskq.py resume <key>...         撤销停止，照常排
    taskq.py input-ok <key>...       撤掉"缺输入"（输入补齐了，11/1）
    taskq.py take --worker <机器> --assign <key>   分给一台执行机（free / stale / waiting 的）：分到退出码 0，分不到 10
    taskq.py put                      标准输入里的任务记录（JSON，一个或一列）原样写进本机队列（缓冲；feed.py 用）；
                                      hold: true = 依赖记录（停止着，只给 after 看），false = 撤掉依赖记录的停止
    taskq.py status                   打印队列
"""
import argparse
import glob
import json
import os
import shlex
import shutil
import socket
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "envs"))
import spec as S  # noqa: E402
import common as C  # noqa: E402
from common import write_json  # noqa: E402

TQ = os.path.expanduser(os.environ.get("TASKQ_DIR", "~/.cache/phd/taskq"))  # TASKQ_DIR：测试和中转执行机（relay.py）用
TASKS = "tasks.jsonl"
STALE_BEAT = int(os.environ.get("TASKQ_STALE_BEAT", 20 * 60))  # 心跳超过这么久算过期（环境变量只给测试用）
FRESH_LOG = int(os.environ.get("TASKQ_FRESH_LOG", 15 * 60))    # 训练日志这么久内更新过算有人在训
HALF_CLAIM = 5 * 60  # 领取只建了目录、没写 info，过这么久算过期
MAX_FAIL = 2
PAUSE_N, PAUSE_WINDOW = 3, 3 * 3600  # 整个队列这么久内失败这么多次就暂停领任务
NOTHING, WAIT, PAUSED = 10, 11, 12   # 领任务的退出码
FEED_ONLY = ("add", "set", "set-nice", "remove", "stop", "resume", "put", "input-ok")  # 缓冲上只有 feed.py（TASKQ_FEED=1）能做
safe = S.safe


def tasks_path():
    return os.path.join(TQ, TASKS)


def have_queue():
    return os.path.exists(tasks_path())


def is_buffer():
    return os.path.exists(os.path.join(TQ, "BUFFER"))


HOLD_WHY = "依赖记录"


# ---------------- 文件 ----------------
def read_tasks(path=None):
    path = path or tasks_path()
    out = []
    if not os.path.exists(path):
        return out
    for ln in open(path, encoding="utf-8"):
        if ln.startswith("#") or not ln.strip():
            continue
        try:
            t = json.loads(ln)
            t["order"], t["nice"] = int(t["order"]), int(t["nice"])
            t.setdefault("exp", None)
            t.setdefault("params", {})
            out.append(t)
        except (ValueError, KeyError):
            continue
    return sorted(out, key=lambda t: (t["nice"], t["order"]))


def write_tasks(tasks, path=None):
    path = path or tasks_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        for t in sorted(tasks, key=lambda t: (t["nice"], t["order"])):
            f.write(json.dumps({k: t.get(k) for k in ("order", "nice", "key", "exp", "recipe", "params")}, ensure_ascii=False) + "\n")
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


# ---------------- 任务说明 ----------------
_specs = {}


def spec_of(t):
    """任务 → 说明；解析不了时 → None（错误记在 t["bad"]）。"""
    k = json.dumps([t.get("recipe"), t.get("params"), t.get("exp")], sort_keys=True)
    if k not in _specs:
        import experiments as E
        try:
            _specs[k] = E.resolve_task(t["recipe"], t["params"], t.get("exp"))
        except (S.SpecError, KeyError) as e:
            _specs[k] = None
            t["bad"] = str(e)
    s = _specs[k]
    if s is not None and s["key"] != t["key"]:
        t["bad"] = f"参数算出的任务名是 {s['key']}，和记录的 {t['key']} 不一致"
        return None
    return s


def data_root():
    """本机的数据目录：TASKQ_DATA（旧名 BRAIN_TUMOR，envs/nnunet_paths.sh 两个都设）> envs/machines.py。"""
    if os.environ.get("TASKQ_DATA") or os.environ.get("BRAIN_TUMOR"):
        return os.environ.get("TASKQ_DATA") or os.environ["BRAIN_TUMOR"]
    import machines as M
    me = M.whoami()
    if not me:
        sys.exit("认不出本机（envs/machines.py 里没有），不知道数据目录")
    return M.expand(M.MACHINES[me]["data"])


def root_error(root):
    """数据目录读不了时 → 原因（这时训完没、日志新旧都判断不了，不能领任务）；读得了 → None。"""
    try:
        os.listdir(root)
    except OSError as e:
        return f"数据目录 {root} 读不了：{e}"
    return None


# ---------------- 状态 ----------------
def claim_info(key):
    try:
        return json.load(open(os.path.join(TQ, "claims", safe(key), "info")))
    except Exception:  # noqa: BLE001
        return None


def failures(key):
    p = os.path.join(TQ, "failures", safe(key))
    return sum(1 for _ in open(p)) if os.path.exists(p) else 0


def skips(key=None):
    """skips/ 的记录 → [(时间戳, worker, 原因)]：某台机器训不了（多半是放不下）的任务，只是这台机器不再领它；
    不算失败（不影响 MAX_FAIL 和暂停）。key=None 时是所有任务的。"""
    out = []
    for p in glob.glob(os.path.join(TQ, "skips", safe(key) if key else "*")):
        for ln in open(p, encoding="utf-8"):
            f = ln.rstrip("\n").split("\t")
            try:
                out.append((time.mktime(time.strptime(f[0], "%Y-%m-%d %H:%M:%S")), f[1], f[2] if len(f) > 2 else ""))
            except (ValueError, IndexError):
                pass
    return out


def stopped_info(key):
    """停止记录 → "时间 原因"；没有 → None。"""
    try:
        return open(os.path.join(TQ, "stopped", safe(key)), encoding="utf-8").read().strip().replace("\t", " ")
    except OSError:
        return None


def input_info(key):
    """缺输入的记录 → (时间戳, 说明)；没有 → None。"""
    try:
        f = open(os.path.join(TQ, "inputs", safe(key)), encoding="utf-8").read().rstrip("\n").split("\t")
        return float(f[0]), " ".join(f[1:])
    except (OSError, ValueError, IndexError):
        return None


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


_BY_KEY = {}  # (任务表路径, 修改时间) → {key: 任务}


def after_state(s, root):
    """说明里 after 的任务 → None（都做完了）或 (waiting / blocked, 说明)。"""
    if not s.get("after"):
        return None
    p = tasks_path()
    ver = (p, os.path.getmtime(p) if os.path.exists(p) else 0)
    if ver not in _BY_KEY:
        _BY_KEY.clear()
        _BY_KEY[ver] = {x["key"]: x for x in read_tasks()}
    for k in s["after"]:
        x = _BY_KEY[ver].get(k)
        xs = spec_of(x) if x else None
        if xs is None:
            return "blocked", f"等的 {k} 不在队列里"
        if S.is_done(xs, root):
            continue
        if failures(k) >= MAX_FAIL:
            return "blocked", f"等的 {k} 失败 {failures(k)} 次"
        return "waiting", f"等 {k}"
    return None


def state(t, root):
    """→ (状态, 说明)：done / stopped（taskq.py stop）/ input（缺输入，taskq.py missing）/ claimed / busy / failed / free / stale（领取过期，可以重新领）/ bad（说明解析不了）/
    waiting（after 里的任务还没做完）/ blocked（after 里的任务失败了或不在队列里）。"""
    s = spec_of(t)
    if s is None:
        return "bad", t.get("bad", "")
    if S.is_done(s, root):
        return "done", ""
    stop = stopped_info(t["key"])
    if stop is not None:
        ci = claim_info(t["key"])
        return "stopped", f"停止（{stop}）" + (f"，作业 {ci.get('worker')} 还领着、正在停" if ci else "")
    inp = input_info(t["key"])
    if inp is not None and claim_info(t["key"]) is None:
        return "input", f"缺输入（{time.strftime('%m-%d %H:%M', time.localtime(inp[0]))} {inp[1]}）"
    w = after_state(s, root)
    if w:
        return w
    ci = claim_info(t["key"])
    age = S.log_age(s, root)
    if ci is None and os.path.isdir(os.path.join(TQ, "claims", safe(t["key"]))):
        # 只建了目录、info 没写上（领的进程在中间被杀）：几分钟后算过期，下一个照常领走（10/25）
        if time.time() - os.stat(os.path.join(TQ, "claims", safe(t["key"]))).st_mtime > HALF_CLAIM:
            return "stale", "领取记录不完整（只有目录，没有 info）"
        return "claimed", "正在领"
    if ci and ci.get("assigned"):
        return "claimed", f"分给 {ci['assigned']}"
    if ci:
        if time.time() - ci.get("beat", 0) < ci.get("ttl", STALE_BEAT) or (age is not None and age < FRESH_LOG):
            return "claimed", f"作业 {ci.get('worker')} @ {ci.get('host')}"
        return "stale", f"作业 {ci.get('worker')} 的领取已过期"
    if age is not None and age < FRESH_LOG:
        rel = released_at(t["key"])
        if rel is None or time.time() - age > rel + 5:  # 交还之后日志又更新过（或从没交还过）：队列外有人在训
            return "busy", f"训练日志 {age / 60:.0f} 分钟前更新过"
    if failures(t["key"]) >= MAX_FAIL:
        return "failed", f"失败 {failures(t['key'])} 次"
    return "free", ""


# ---------------- DICC 上：领、心跳、交还 ----------------
def _claim(a, pick):
    """按顺序领第一个 pick(t) 不为 None 的 free / stale 任务 → (退出码, pick 的结果)。"""
    if not have_queue():
        # 不能当成"没有任务"（退出码 10）：那样每个开始的作业都会马上结束，排队的作业几分钟内全部用掉
        print(f"没有 {tasks_path()}", file=sys.stderr)
        return 2, None
    root = data_root()
    err = root_error(root)
    if err:
        print(err, file=sys.stderr)
        return PAUSED, None
    if recent_failures() >= PAUSE_N:
        return PAUSED, None
    busy_ahead = 0  # 排在前面、刚停下（日志 15 分钟内更新过、没人领）的
    with Lock():
        for t in read_tasks():
            if t["key"] in a.exclude:
                continue
            st, _ = state(t, root)
            if st == "busy" and pick(t) is not None:
                busy_ahead += 1
            if st not in ("free", "stale"):
                continue
            out = pick(t)
            if out is None:
                continue
            if busy_ahead >= a.free_slots and time.time() < a.patience_until:
                return WAIT, None  # 给排在前面的留着位置，等它的日志过了 15 分钟再领
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
            write_json(os.path.join(d, "info"), dict(worker=a.worker, host=socket.gethostname().split(".")[0], since=now, beat=now), indent=None)
            return 0, out
    return NOTHING, None


def cmd_next(a):
    envs = set(e for e in a.envs.split(",") if e)
    me = here()

    def pick(t):
        s = spec_of(t)
        if s["env"] not in envs or (s.get("machines") and me not in s["machines"]) or s.get("device", "gpu") != a.device:
            return None
        return t["key"] + "\n" + " ".join(shlex.quote(x) for x in S.kv_args(t["recipe"], t["params"]))
    rc, out = _claim(a, pick)
    if rc == 0:
        print(out)
    return rc


def cmd_beat(a):
    for key in a.keys:
        p = os.path.join(TQ, "claims", safe(key), "info")
        ci = claim_info(key)
        if ci and str(ci.get("worker")) == str(a.worker):
            ci["beat"] = time.time()
            write_json(p, ci, indent=None)
    return 0


def cmd_release(a):
    for key in a.keys:
        d = os.path.join(TQ, "claims", safe(key))
        ci = claim_info(key)
        if a.skip:
            os.makedirs(os.path.join(TQ, "skips"), exist_ok=True)
            with open(os.path.join(TQ, "skips", safe(key)), "a") as f:
                f.write(f"{time.strftime('%F %T')}\t{a.worker}\t{a.skip}\n")
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


def cmd_missing(a):
    """缺输入（11/1）：记 inputs/<key>，像 release 一样交还（不算失败）。"""
    ci = claim_info(a.key)
    if not (ci and str(ci.get("worker")) == str(a.worker)):
        print(f"{a.key} 不是 {a.worker} 领着的", file=sys.stderr)
        return 1
    os.makedirs(os.path.join(TQ, "inputs"), exist_ok=True)
    with open(os.path.join(TQ, "inputs", safe(a.key)), "w", encoding="utf-8") as f:
        f.write(f"{time.time()}\t{a.worker}\t{(a.why or '').replace(chr(9), ' ').replace(chr(10), ' ')}\n")
    os.makedirs(os.path.join(TQ, "released"), exist_ok=True)
    with open(os.path.join(TQ, "released", safe(a.key)), "w") as f:
        f.write(f"{time.time()} {a.worker}\n")
    shutil.rmtree(os.path.join(TQ, "claims", safe(a.key)), ignore_errors=True)
    return 0


def cmd_input_ok(a):
    for k in a.keys:
        p = os.path.join(TQ, "inputs", safe(k))
        if os.path.exists(p):
            os.remove(p)
            print(f"输入补齐了：{k}")
    return 0


def cmd_take(a):
    """领指定的一个（box 替 lab 领，2026-10-18）：只领 free / stale 的；已经是自己领的就续上（since 不变）。
    --assign（10/31）：分给执行机 a.worker，不过期；waiting 的也能分（提前送到那边，after 做完后那边接着做）。"""
    root = data_root()
    err = root_error(root)
    if err:
        print(err)
        return NOTHING
    with Lock():
        t = next((t for t in read_tasks() if t["key"] == a.key), None)
        if t is None:
            print(f"队列里没有 {a.key}")
            return NOTHING
        ci = claim_info(a.key)
        mine = bool(ci) and str(ci.get("worker")) == str(a.worker)
        st, note = state(t, root)
        ok = ("free", "stale", "waiting") if a.assign else ("free", "stale")
        if st in ("done", "stopped") or (not mine and st not in ok):
            print(f"{a.key}：{st} {note}")
            return NOTHING
        d = os.path.join(TQ, "claims", safe(a.key))
        if ci or st == "stale":
            shutil.rmtree(d, ignore_errors=True)
        os.makedirs(os.path.dirname(d), exist_ok=True)
        try:
            os.mkdir(d)
        except FileExistsError:
            print(f"{a.key}：刚被别人领走")
            return NOTHING
        now = time.time()
        info = dict(worker=a.worker, host=a.worker, since=ci.get("since", now) if mine else now, beat=now)
        info.update(assigned=a.worker) if a.assign else info.update(ttl=a.ttl)
        write_json(os.path.join(d, "info"), info, indent=None)
        print(t["key"])
        return 0


def cmd_put(a):
    """标准输入的任务记录（一个 dict 或一列）原样写进本机队列（10/31，feed.py 往缓冲里送）：没有的加上，有的改 order、nice、
    exp、recipe、params。参数算出的任务名要和 key 一样。hold 见开头。"""
    import experiments as E
    recs = json.loads(sys.stdin.read() or "[]")
    recs = [recs] if isinstance(recs, dict) else recs
    for r in recs:
        s = E.resolve_task(r["recipe"], r.get("params") or {}, r.get("exp"))
        if s["key"] != r["key"]:
            print(f"{r['key']}：参数算出的任务名是 {s['key']}")
            return 2
    with Lock():
        tasks = read_tasks()
        by = {t["key"]: t for t in tasks}
        for r in recs:
            sp = os.path.join(TQ, "stopped", safe(r["key"]))
            if r.get("hold") and not os.path.exists(sp):  # 先停再写记录：worker 不会在中间领走
                os.makedirs(os.path.dirname(sp), exist_ok=True)
                with open(sp, "w", encoding="utf-8") as f:
                    f.write(f"{time.strftime('%F %T')}\t{HOLD_WHY}\n")
            elif r.get("hold") is False and (stopped_info(r["key"]) or "").endswith(HOLD_WHY):
                os.remove(sp)
            t = by.get(r["key"])
            new = {k: r.get(k) for k in ("order", "nice", "key", "exp", "recipe")}
            new["params"] = r.get("params") or {}
            new["order"], new["nice"] = int(new["order"]), int(new["nice"])
            if t is None:
                tasks.append(new)
                by[r["key"]] = new
                print(f"加入：{r['key']}")
            elif any(t.get(k) != new[k] for k in new):
                t.update(new)
                print(f"改了：{r['key']}")
        write_tasks(tasks)
    return 0


# ---------------- 加、改 ----------------
def _insert(new, nice):
    """把新任务（dict(key, exp, recipe, params)）加进队列，已有的跳过。先解析一遍，写错参数的不加。"""
    import experiments as E
    with Lock():
        tasks = read_tasks()
        have = {t["key"] for t in tasks}
        order = max([t["order"] for t in tasks] or [0])
        for t in new:
            s = E.resolve_task(t["recipe"], t["params"], t.get("exp"))
            if s["key"] in have:
                print(f"已有，跳过：{s['key']}")
                continue
            order += 1
            tasks.append(dict(t, key=s["key"], order=order, nice=nice))
            have.add(s["key"])
            print(f"加入：{nice:>5} {s['key']}（{t['recipe']}，环境 {s['env']}）")
        write_tasks(tasks)
    return 0


def read_sheets(path):
    """任务单文件（JSON：一张 {字段: 值} 或一列）→ ([dict(exp, recipe, params)], 档位)。recipe 不写 = command；
    nice 写在单子里（几张单子的 nice 要一样）；其余字段都是做法的参数（scripts/queue/README.md 的"别人的任务"）。"""
    try:
        d = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise S.SpecError(f"读不了任务单 {path}：{e}") from None
    sheets = d if isinstance(d, list) else [d]
    if not sheets or not all(isinstance(x, dict) for x in sheets):
        raise S.SpecError(f"任务单 {path} 要是一个 JSON 对象或一列对象")
    nices = {x.get("nice") for x in sheets}
    if len(nices) > 1:
        raise S.SpecError(f"任务单 {path} 里的 nice 不一样：{sorted(map(str, nices))}")
    # 任务单加的 command 任务默认能交给执行机（只换路径）；调度自己加的 command 任务（推理、评测）默认不交
    new = [dict(exp=None, recipe=x.get("recipe", "command"),
                params={**({"relay": "path"} if x.get("recipe", "command") == "command" else {}),
                        **{k: v for k, v in x.items() if k not in ("recipe", "nice")}})
           for x in sheets]
    nice = nices.pop()
    return new, (int(nice) if nice is not None else None)


def only_of(items):
    """["fold=1,2", …] → {"fold": ["1", "2"]}；没给 → None。"""
    out = {}
    for x in items:
        k, sep, v = x.partition("=")
        if not sep or not k or not v:
            sys.exit(f"--only 要写成 参数=取值[,取值…]：{x}")
        out.setdefault(k, []).extend(v.split(","))
    return out or None


def cmd_add(a):
    import experiments as E
    try:
        if a.exp:
            e = E.experiment(a.exp) or sys.exit(f"实验表里没有 {a.exp}（有：{', '.join(x['id'] for x in E.load()[0])}）")
            new = [dict(exp=t["exp"], recipe=t["recipe"], params=t["params"]) for t in E.expand(e, only_of(a.only))]
            if a.nice is None:
                a.nice = e.get("nice")
                if a.nice is None:
                    sys.exit(f"实验表里 {a.exp} 没有 nice，要给 --nice")
                print(f"档位用实验表里的 {a.nice}")
        elif a.recipe:
            new = [dict(exp=None, recipe=a.recipe, params=S.parse_kv(a.args))]
        elif a.file:
            new, nice = read_sheets(a.file)
            if a.nice is None:
                a.nice = nice
        else:
            sys.exit("要给 --exp、--recipe 或 --file")
        if a.nice is None:
            sys.exit("要给 --nice")
        return _insert(new, a.nice)
    except S.SpecError as e:
        print(f"加不进去：{e}")
        return 2


def cmd_set(a):
    import experiments as E
    with Lock():
        tasks = read_tasks()
        t = next((t for t in tasks if t["key"] == a.key), None)
        if t is None:
            print(f"队列里没有 {a.key}")
            return 1
        p = dict(t["params"])
        for k, v in S.parse_kv(a.kv).items():
            if v == "":
                p.pop(k, None)
            else:
                p[k] = v
        try:
            s = E.resolve_task(t["recipe"], p, t.get("exp"))
        except S.SpecError as e:
            print(f"改不了：{e}")
            return 2
        if s["key"] != t["key"]:
            print(f"改了以后任务名会变成 {s['key']}（领取记录、结果都按任务名）：改成新任务用 add，再把这个删掉")
            return 2
        print(f"{t['key']}：{json.dumps(t['params'], ensure_ascii=False)} → {json.dumps(p, ensure_ascii=False)}")
        t["params"] = p
        write_tasks(tasks)
    return 0


def cmd_set_nice(a):
    with Lock():
        tasks = read_tasks()
        for t in tasks:
            if t["key"] in a.keys:
                print(f"{t['key']}：{t['nice']} → {a.nice}")
                t["nice"] = a.nice
        write_tasks(tasks)
    missing = set(a.keys) - {t["key"] for t in tasks}
    for k in sorted(missing):
        print(f"队列里没有：{k}")
    return 0 if not missing else 1


def cmd_remove(a):
    """从队列里删掉没人在做的任务（10/28：DICC 停机期间把排在那边的评测挪回 box）。有人领着、在做的不删；
    别的任务的 after 里有它、又不一起删的不删。结果文件不动。"""
    root = data_root()
    with Lock():
        tasks = read_tasks()
        by = {t["key"]: t for t in tasks}
        missing = [k for k in a.keys if k not in by]
        bad = []
        for k in a.keys:
            if k in by:
                st, _ = state(by[k], root)
                ci = claim_info(k) or {}
                if ci.get("assigned"):
                    bad.append(f"{k}：分给了 {ci['assigned']}，先 stop，交还后再删")
                elif st in ("claimed", "busy"):
                    bad.append(f"{k}：{st}，有人在做")
        for t in tasks:
            if t["key"] in a.keys:
                continue
            dep = [k for k in ((spec_of(t) or {}).get("after") or []) if k in a.keys]
            if dep:
                bad.append(f"{t['key']} 的 after 里有 {' '.join(dep)}（一起删，或者先改它）")
        for k in missing:
            print(f"队列里没有：{k}")
        for b in bad:
            print(f"不删：{b}")
        if missing or bad:
            return 1
        write_tasks([t for t in tasks if t["key"] not in a.keys])
    for k in a.keys:
        print(f"删了：{k}")
    return 0


def cmd_stop(a):
    root = data_root()
    with Lock():
        by = {t["key"]: t for t in read_tasks()}
        missing = [k for k in a.keys if k not in by]
        for k in missing:
            print(f"队列里没有：{k}")
        if missing:
            return 1
        os.makedirs(os.path.join(TQ, "stopped"), exist_ok=True)
        for k in a.keys:
            ci = claim_info(k)
            with open(os.path.join(TQ, "stopped", safe(k)), "w", encoding="utf-8") as f:
                f.write(f"{time.strftime('%F %T')}\t{a.why or ''}\n")
            w = str((ci or {}).get("worker", ""))
            if (ci or {}).get("assigned"):
                print(f"停止：{k}（分给了 {ci['assigned']}，由 box 下次检查时在那边停、交还）")
            elif not ci:
                print(f"停止：{k}（没人在做，以后不领）")
            elif w.isdigit():
                print(f"停止：{k}（作业 {w} 30 秒内停下、交还）")
            else:
                print(f"停止：{k}（{w} 领着，由 box 下次检查时停、交还）")
    return 0


def cmd_resume(a):
    for k in a.keys:
        p = os.path.join(TQ, "stopped", safe(k))
        if os.path.exists(p):
            os.remove(p)
            print(f"撤销停止：{k}")
        else:
            print(f"没有停止：{k}")
    return 0


def cmd_show(a):
    t = next((t for t in read_tasks() if t["key"] == a.key), None)
    if t is None:
        print(f"队列里没有 {a.key}")
        return 1
    s = spec_of(t)
    if a.json:
        if s is None:
            print(t.get("bad"), file=sys.stderr)
            return 2
        S.json_out(s)
        return 0
    st, note = state(t, data_root())
    print(f"{t['key']}：档位 {t['nice']}，{st} {note}；实验 {t.get('exp') or '（不在实验表里）'}")
    print(f"  run_task.sh {' '.join(shlex.quote(x) for x in S.kv_args(t['recipe'], t['params']))}")
    if s:
        for k in ("env", "cmd", "out", "done", "data"):
            print(f"  {k:5} {s[k]}")
    return 0


# ---------------- 状态（DICC 上直接读，box 上 ssh 过去） ----------------
def task_log_age(root, key, worker):
    """领着这个任务的作业写的分段日志（run_task.sh）多久没更新（秒）；没有 → None。"""
    if not worker:
        return None
    pre = os.path.join(root, "logs", "tasks", f"{glob.escape(safe(key))}__{glob.escape(str(worker))}_")
    logs = glob.glob(pre + "part*.log") + glob.glob(pre + "seg*.log")  # _seg：10/31 前的名字
    try:
        return time.time() - max(os.path.getmtime(p) for p in logs) if logs else None
    except OSError:
        return None


def out_age(s, root):
    """结果文件夹和它下一层里最新的修改时间离现在多久（秒；推理一个测试集时日志不动、预测文件夹在变，2026-10-24）；没有 → None。"""
    d = S.out_dir(s, root)
    try:
        ms = [os.path.getmtime(d)] + [e.stat().st_mtime for e in os.scandir(d)]
    except OSError:
        return None
    return time.time() - max(ms)


def local_status():
    root = data_root()
    out = []
    for t in read_tasks():
        st, note = state(t, root)
        s = spec_of(t)
        ci = claim_info(t["key"]) or {}
        out.append(dict(t, spec=s, state=st, note=note, worker=ci.get("worker"), host=ci.get("host"), since=ci.get("since"),
                        assigned=ci.get("assigned"), input_since=(input_info(t["key"]) or (None,))[0], held=(stopped_info(t["key"]) or "").endswith(HOLD_WHY),
                        beat=ci.get("beat"), log_age=S.log_age(s, root) if s else None,
                        task_log_age=task_log_age(root, t["key"], ci.get("worker")),
                        out_age=out_age(s, root) if s and st == "claimed" else None, failures=failures(t["key"]),
                        skipped_by=sorted({w for _, w, _ in skips(t["key"])})))
    nf = recent_failures()
    day = [w for ts, w, _ in skips() if ts >= time.time() - 86400]
    return dict(enabled=os.path.exists(os.path.join(TQ, "ENABLED")), buffer=is_buffer(), tasks=out, recent_failures=nf, paused=nf >= PAUSE_N,
                recent_skips={w: day.count(w) for w in set(day)})


def remote_status(host):
    """box 上：ssh 到 host、跑那边仓库里的本脚本 --json-status → dict 或 None。"""
    r = remote(host, "--json-status", timeout=180)
    if r is None:
        return None
    rc, out, _ = r
    return S.json_in(out) if rc == 0 else None


def remote(host, args, timeout=120, env="", input=None):
    """box 上：在 host 的仓库里跑 taskq.py <args>（env：前面加的环境变量，如 TASKQ_FEED=1）→ (退出码, 标准输出, 标准错误)，
    ssh 不通时 None。"""
    import machines as M
    m = M.MACHINES[host]
    ssh = os.environ.get("TASKQ_SSH", "ssh").split() + C.SSH[1:]  # TASKQ_SSH 只给测试用
    cmd = f"cd {m['repo']} && {env + ' ' if env else ''}TASKQ_DATA={m['data']} BRAIN_TUMOR={m['data']} python3 scripts/queue/taskq.py {args}"
    rc, out, err = C.run(cmd, m["ssh"], timeout, ssh=ssh, input=input)
    return None if rc in (124, 255) else (rc, out, err)  # 124：超时、ssh 起不来


def central(args, timeout=300):
    """box 上改总队列：本机跑 taskq.py <args>（参数列表或 shell 写法的字符串）→ (退出码, 标准输出, 标准错误)。"""
    import subprocess
    argv = shlex.split(args) if isinstance(args, str) else list(args)
    r = subprocess.run([sys.executable, os.path.abspath(__file__)] + argv, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


def status(name):
    """一台机器的队列状态：本机直接读，别的机器 ssh 过去 → dict 或 None。"""
    return local_status() if name == here() or (name == queue_host() and have_queue()) else remote_status(name)


def queue_host():
    """总队列所在的机器：envs/machines.py 的 hub（box）；没有 hub 时（中转执行机上的代码包）是本机。"""
    import machines as M
    try:
        return M.hub()
    except StopIteration:
        return here()


def here():
    import machines as M
    return os.environ.get("TASKQ_MACHINE") or M.whoami()  # TASKQ_MACHINE 只给测试用


def cmd_status(a):
    s = local_status() if have_queue() or here() == queue_host() else None
    if s is None:
        print("读不到队列")
        return 1
    print(f"总开关：{'开' if s['enabled'] else '关'}；任务 {len(s['tasks'])} 个"
          + (f"；{PAUSE_WINDOW // 3600} 小时内失败 {s.get('recent_failures')} 次，暂停领任务" if s.get("paused") else ""))
    n = {}
    for t in s["tasks"]:
        n[t["state"]] = n.get(t["state"], 0) + 1
        if t["state"] not in ("done",):
            env = (t.get("spec") or {}).get("env", "?")
            print(f"  {t['nice']:>5} {t['key']:60} {env:10} {t['state']} {t['note']}")
    print("；".join(f"{k} {v}" for k, v in sorted(n.items())))
    return 0


def main(argv):
    if argv[:1] == ["--json-status"]:
        S.json_out(local_status())
        return 0
    cmd = argv[0] if argv else ""
    if cmd in ("add", "set", "set-nice", "take", "show", "remove", "stop", "resume", "put", "status") \
            and not have_queue() and not is_buffer() and here() != queue_host():
        print(f"总队列在 {queue_host()}，在那边跑（本机没有 {tasks_path()}）")
        return 2
    if cmd in FEED_ONLY and is_buffer() and not os.environ.get("TASKQ_FEED"):
        print(f"这里是执行机的缓冲（{TQ}），任务由 box 的 feed.py 送来：在 {queue_host()} 上改总队列")
        return 2
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("next")
    p.add_argument("--worker", required=True)
    p.add_argument("--free-slots", type=int, default=1)
    p.add_argument("--patience-until", type=float, default=0)
    p.add_argument("--exclude", nargs="*", default=[])
    p.add_argument("--envs", required=True, help="本机有的环境，逗号分开（envs/machines.py）")
    p.add_argument("--device", default="gpu", choices=["gpu", "cpu"], help="只领任务说明里 device 是这个的（2026-10-24）")
    p = sub.add_parser("beat")
    p.add_argument("--worker", required=True)
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("release")
    p.add_argument("--worker", required=True)
    p.add_argument("keys", nargs="+")
    p.add_argument("--failed")
    p.add_argument("--skip", help="这台机器训不了、以后不再领它（记在 skips/，不算失败）")
    p = sub.add_parser("missing")
    p.add_argument("--worker", required=True)
    p.add_argument("--why", default="")
    p.add_argument("key")
    p = sub.add_parser("input-ok")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("take")
    p.add_argument("--worker", required=True)
    p.add_argument("--ttl", type=int, default=STALE_BEAT)
    p.add_argument("--assign", action="store_true", help="分给执行机：不过期，waiting 的也能分（10/31）")
    p.add_argument("key")
    sub.add_parser("put")
    p = sub.add_parser("add")
    p.add_argument("--nice", type=int, help="档位；--exp 时不给就用实验表里的 nice")
    p.add_argument("--exp")
    p.add_argument("--only", action="append", default=[], metavar="参数=取值[,取值…]")
    p.add_argument("--recipe")
    p.add_argument("--file", help="任务单（JSON），不写 recipe 时是 command 做法")
    p.add_argument("args", nargs="*", help="--recipe 的 k=v")
    p = sub.add_parser("set")
    p.add_argument("key")
    p.add_argument("kv", nargs="+")
    p = sub.add_parser("set-nice")
    p.add_argument("--nice", type=int, required=True)
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("remove")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("stop")
    p.add_argument("--why", default="")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("resume")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("show")
    p.add_argument("--json", action="store_true")
    p.add_argument("key")
    p = sub.add_parser("status")
    a = ap.parse_args(argv)
    return dict(next=cmd_next, beat=cmd_beat, release=cmd_release, take=cmd_take, add=cmd_add, set=cmd_set,
                set_nice=cmd_set_nice, show=cmd_show, remove=cmd_remove, status=cmd_status,
                stop=cmd_stop, resume=cmd_resume, put=cmd_put,
                missing=cmd_missing, input_ok=cmd_input_ok)[a.cmd.replace("-", "_")](a)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
