#!/usr/bin/env python3
"""训练任务队列（2026-10-17 起；10/18 起任务不限于 nnU-Net；10/31 起总队列在 box）。

总队列（box 的 ~/.cache/phd/taskq/）：所有训练和要进队列的步骤。只有 box 写它（实验表加任务、改档位、停止都在 box 上做）；
  分给某台执行机的任务记为 claims/<key>/info 里的 assigned: <机器>：没有心跳、不过期，只靠交还结束（scripts/taskq/queue/relay.py 交还）。
执行机上的队列（relay.py 在执行机根目录里建的 R/queue/）：relay.py 替它从总队列领来的任务，key 和总队列相同。
  worker（scripts/taskq/queue/dicc_worker.sh）在那边领、心跳、交还，box 不在线时照常跑。训完以后 box 拉回，box 上有了结果才算训完（总队列的 done）。

每个任务 = 做法 + 参数（scripts/taskq/queue/recipes.py，解析在 scripts/taskq/queue/spec.py）：环境、命令、结果文件夹、怎么算训完都由
任务说明决定，这里不认识具体模型。属于实验表（research/*/experiments.py）的任务记下实验 id。

队列目录（总队列和执行机上的一样）：
  tasks.jsonl   任务，每行一个：{order, nice（档位，越小越先）, key, exp, recipe, params}；执行机的队列另有 spec（box 解析好的说明，11/4）
  claims/<key>  正在训的任务（mkdir 原子领取）：info 里是 worker（DICC 作业号等）、主机、领取和最近一次心跳的时间、token
                （心跳 20 分钟不更新算过期）；总队列里是 assigned（分给哪台执行机，不过期）
  failures/<key> 训练失败的记录，失败 2 次的不再领（健康检查提醒）；整个队列 3 小时内失败 3 次就暂停领任务
                （多半是节点或环境坏了，免得把所有任务都标成失败、排队的作业一个个空跑掉）
  skips/<key>   某台机器训不了的任务（release --skip）：这台机器不再领它，别的机器照常领；不算失败
  inputs/<key>  缺输入（11/1）：worker 开训前查到输入不全（run_task.sh 退出码 4），交还并记在这里；不再领它，等补齐输入的一方
                （relay.py 补传）taskq.py input-ok 撤掉。不算失败
  released/<key> 最近一次交还的时间：交还之后训练日志没再更新的，马上可以领（不用等 15 分钟）
  stopped/<key> 停止的任务（taskq.py stop，10/30）：不再领；在训的 worker 每 5 秒看一次，30 秒内停下、交还。
  ENABLED       总开关：没有这个文件时，排队的 worker 作业开始后直接结束（scripts/taskq/queue/worker_start.sh）
  lock          改队列时的锁（mkdir）
不领的任务：已经训完（任务说明的 done）；别处正在训（claims 里有、心跳在 ttl 内，或者训练日志 15 分钟内更新过）；
  说明解析不了的（参数写错，status 里显示 bad）。

worker 调用（执行机上的队列）：
    taskq.py next --worker <作业号> --envs <环境,…> [--device gpu|cpu] [--cpus N] [--mem-mb N] [--gpu-mem GB]
                  [--free-slots N] [--patience-until 时间] [--exclude key...]
        领一个本机有环境的（任务说明写了 machines 的，只在列出的机器上领；只领 device 和 --device 一样的，默认 gpu；res 超出
        --cpus / --mem-mb / --gpu-mem 的不领）→ 打印三行：key、run_task.sh 的参数（shell 引号）、这次领取的 token，退出码 0。没有能领的退出码 10；
        排在前面的刚停下、还在等它（--patience-until 之前）退出码 11；队列因为连续失败暂停了退出码 12；
        其他退出码都是队列本身出错（包括没有任务表：退出码 2）。
    taskq.py beat --worker <作业号> [--token T] <key>...            心跳
    taskq.py release --worker <作业号> [--token T] <key>... [--failed 原因] [--skip 原因]
    taskq.py missing --worker <作业号> [--token T] <key> [--why 说明]   缺输入（11/1）：交还，记 inputs/<key>，补齐前不再领
        --token：只动这一轮的领取（领取已被别人接管或已经没了时，交还什么都不记）
改队列（box 上改总队列；add --spec-stdin、set-nice、remove、stop、resume、input-ok 也由 box 的 relay.py 在执行机的队列上调用；
在不是 box、本机又没有队列的机器上跑时退出码 2）：
    taskq.py add [--nice <档位>] --exp <实验 id> [--only <参数>=<取值>[,<取值>…]]…   从实验表加（已有的跳过；不给 --nice 用表里的档位；
                                       --only 只加 each 里这个参数取这些值的任务，如 --only fold=1,2）
    taskq.py add --nice <档位> --recipe <做法> k=v…                 加一个实验表以外的任务
    taskq.py add [--nice <档位>] --file <任务单.json>               按任务单加（别人的训练、测试；不给 --nice 用单子里的）
    taskq.py add --nice <档位> --spec-stdin [--update-only] < 说明.json   加解析好的说明（执行机的队列，relay.py 送；已有的换成新说明，
                                       --update-only 只换已有的，11/4）。在训的任务 out / done 变了的先不换；--update-only 时
                                       解析不了的逐个报（退出码 2）、别的照换
    taskq.py set <key> k=v…           改一个任务的参数（如 env=umamba；k= 空值 = 删掉这个参数，回到默认）；
                                       带 spec 的（执行机的队列）不改：改总队列，再 relay.py resend
    taskq.py set-nice --nice <档位> <key>...
    taskq.py show [--json] <key>      任务的完整说明和状态（--json：说明写在 @@JSON 开头的一行里）
    taskq.py remove <key>...         删掉没人领着的任务和它的失败、停止、缺输入、交还记录（执行机的队列连跳过也删）；after 里有它的任务
                                     要一起删；结果文件不动；分出去的先 stop。退出码 0 删了，3 不删（有人领着等），4 队列里没有
    taskq.py stop [--why 原因] <key>...   停止任务（10/30）：在训的 30 秒内停下、交还（存档是原子写的，从存档接着训），
                                      之后不再领它；分给执行机的由 box 下次检查时在那边停（relay.py）
    taskq.py resume <key>...         撤销停止，照常排
    taskq.py input-ok <key>...       撤掉"缺输入"（输入补齐了，11/1）
    taskq.py take --worker <机器> --assign <key>   分给一台执行机（free / stale / waiting 的）：分到退出码 0，分不到 10
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
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import spec as S  # noqa: E402
from common import write_json  # noqa: E402

TQ = os.path.expanduser(os.environ.get("TASKQ_DIR", cfg.state("taskq", expand=False)))  # TASKQ_DIR：测试和中转执行机（relay.py）用
TASKS = "tasks.jsonl"
STALE_BEAT = int(os.environ.get("TASKQ_STALE_BEAT", 20 * 60))  # 心跳超过这么久算过期（环境变量只给测试用）
FRESH_LOG = int(os.environ.get("TASKQ_FRESH_LOG", 15 * 60))    # 训练日志这么久内更新过算有人在训
LOCK_WAIT = float(os.environ.get("TASKQ_LOCK_WAIT", 0.5))  # 等锁时每次重试的间隔（秒，环境变量只给测试用）
LOCK_DEAD, LOCK_REMOTE, LOCK_MAX = 120, 600, 900  # 锁目录这么久没动就抢：持有者已死 / 在别的主机（看不到死活）/ 本机还活着（pid 可能被复用）
HALF_CLAIM = 5 * 60  # 领取只建了目录、没写 info，过这么久算过期
MAX_FAIL = 2
PAUSE_N, PAUSE_WINDOW = 3, 3 * 3600  # 整个队列这么久内失败这么多次就暂停领任务
NOTHING, WAIT, PAUSED = 10, 11, 12   # 领任务的退出码
REFUSED, GONE = 3, 4  # remove 的退出码：不删（有人领着、分出去了、别的任务等它）、队列里没有（11/4）
safe = S.safe


def tasks_path():
    return os.path.join(TQ, TASKS)


def have_queue():
    return os.path.exists(tasks_path())


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
            rec = {k: t.get(k) for k in ("order", "nice", "key", "exp", "recipe", "params")}
            if t.get("spec"):
                rec["spec"] = t["spec"]  # 执行机的队列：box 解析好的说明（11/4）
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    os.replace(tmp, path)


class Lock:
    def __init__(self, tries=600):
        self.d = os.path.join(TQ, "lock")
        self.tries = tries
        self.me = None

    def __enter__(self):
        os.makedirs(TQ, exist_ok=True)
        for _ in range(self.tries):
            try:
                os.mkdir(self.d)
            except FileExistsError:
                try:
                    old = self._ident(self.d)
                    age = time.time() - old[1] / 1e9
                    if age > {"alive": LOCK_MAX, "remote": LOCK_REMOTE}.get(self._holder(), LOCK_DEAD):
                        stale = f"{self.d}.stale{os.getpid()}"
                        os.rename(self.d, stale)
                        if self._ident(stale) != old:  # 判断之后别人已经抢过、重新拿到了锁：还给它
                            if not os.path.lexists(self.d):
                                os.rename(stale, self.d)
                        else:
                            shutil.rmtree(stale, ignore_errors=True)
                            continue
                except OSError:
                    pass
                time.sleep(LOCK_WAIT)
                continue
            me = f"{socket.gethostname()}\t{os.getpid()}\t{os.urandom(6).hex()}\n"
            try:
                with open(os.path.join(self.d, "owner"), "w") as f:
                    f.write(me)
                self.me = me
            except OSError:
                pass
            return self
        raise SystemExit("拿不到队列的锁")

    @staticmethod
    def _ident(d):
        """认一把锁：inode 号（ext4 会马上复用）、mtime（纳秒）、owner 的内容。"""
        st = os.stat(d)
        try:
            owner = open(os.path.join(d, "owner")).read()
        except OSError:
            owner = None
        return (st.st_ino, st.st_mtime_ns, owner)

    def _holder(self):
        """持有者 → alive（本机、进程还在）/ remote（别的主机）/ dead（进程不在、没有 owner）。"""
        try:
            host, pid, _ = open(os.path.join(self.d, "owner")).read().split("\t")
            if host != socket.gethostname():
                return "remote"
            os.kill(int(pid), 0)
        except PermissionError:
            return "alive"
        except (OSError, ValueError):
            return "dead"
        return "alive"

    def __exit__(self, *a):
        p = os.path.join(self.d, "owner")
        try:
            if self.me is not None:
                if open(p).read() != self.me:  # 锁已被别人接管
                    return
                os.remove(p)
            os.rmdir(self.d)
        except OSError:
            pass


# ---------------- 任务说明 ----------------
_specs = {}


def spec_of(t):
    """任务 → 说明；解析不了时 → None（错误记在 t["bad"]）。带 spec 的（执行机的队列，box 解析好的）直接用，不查做法。"""
    k = json.dumps([t.get("spec")] if t.get("spec") else [t.get("recipe"), t.get("params"), t.get("exp")], sort_keys=True)
    if k not in _specs:
        try:
            if t.get("spec"):
                _specs[k] = S.from_sent(t["spec"])
            else:
                import experiments as E
                _specs[k] = E.resolve_task(t["recipe"], t["params"], t.get("exp"))
        except (KeyError, TypeError, ValueError, AttributeError) as e:  # 一条坏记录只坏它自己，不拖垮整个队列（11/4）
            _specs[k] = None
            t["bad"] = str(e) if isinstance(e, S.SpecError) else f"{type(e).__name__}: {e}"
    s = _specs[k]
    if s is not None and s["key"] != t["key"]:
        t["bad"] = f"参数算出的任务名是 {s['key']}，和记录的 {t['key']} 不一致"
        return None
    return s


def data_root():
    """本机的数据目录：TASKQ_DATA > envs/machines.py。"""
    if cfg.env("TASKQ_DATA"):
        return cfg.env("TASKQ_DATA")
    import machines as M
    me = M.whoami()
    if not me or not M.MACHINES[me].get("data"):
        sys.exit("认不出本机或本机没有数据目录（envs/machines.py），先设 TASKQ_DATA")
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
    wait = None  # 先看完全部：有 blocked 的报 blocked
    for k in s["after"]:
        x = _BY_KEY[ver].get(k)
        xs = spec_of(x) if x else None
        if xs is None:
            return "blocked", f"等的 {k} 不在队列里"
        if S.is_done(xs, root):
            continue
        if failures(k) >= MAX_FAIL:
            return "blocked", f"等的 {k} 失败 {failures(k)} 次"
        wait = wait or ("waiting", f"等 {k}")
    return wait


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
    age = S.log_age(s, root)
    cs = claim_state(t["key"], age)
    if cs:
        return cs
    if age is not None and age < FRESH_LOG:
        rel = released_at(t["key"])
        if rel is None or time.time() - age > rel + 5:  # 交还之后日志又更新过（或从没交还过）：队列外有人在训
            return "busy", f"训练日志 {age / 60:.0f} 分钟前更新过"
    if failures(t["key"]) >= MAX_FAIL:
        return "failed", f"失败 {failures(t['key'])} 次"
    return "free", ""


def claim_state(key, age):
    """领取记录（age：训练日志多久没更新）→ None（没有）/ ("claimed", 说明)（活着的）/ ("stale", 说明)（过期了，可以重新领）。"""
    d = os.path.join(TQ, "claims", safe(key))
    ci = claim_info(key)
    if ci is None:
        # 只建了目录、info 没写上（领的进程在中间被杀）：几分钟后算过期，下一个照常领走（10/25）
        try:
            made = os.stat(d).st_mtime
        except OSError:
            return None
        return ("stale", "领取记录不完整（只有目录，没有 info）") if time.time() - made > HALF_CLAIM else ("claimed", "正在领")
    if ci.get("assigned"):
        return "claimed", f"分给 {ci['assigned']}"
    if time.time() - ci.get("beat", 0) < STALE_BEAT or (age is not None and age < FRESH_LOG):
        return "claimed", f"作业 {ci.get('worker')} @ {ci.get('host')}"
    return "stale", f"作业 {ci.get('worker')} 的领取已过期"


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
            a.token = os.urandom(6).hex()
            write_json(os.path.join(d, "info"), dict(worker=a.worker, host=socket.gethostname().split(".")[0], since=now, beat=now,
                                                     token=a.token), indent=None)
            return 0, out
    return NOTHING, None


def run_args(t):
    """run_task.sh 的参数：带 spec 的给 --spec <JSON>，否则 <做法> k=v…。"""
    if t.get("spec"):
        return ["--spec", json.dumps(t["spec"], ensure_ascii=False, separators=(",", ":"))]
    return S.kv_args(t["recipe"], t["params"])


def cmd_next(a):
    envs = set(e for e in a.envs.split(",") if e)
    me = here()

    def pick(t):
        s = spec_of(t)
        if not S.envs_of(s) or not set(S.envs_of(s)) <= envs or (s.get("machines") and me not in s["machines"]) or s.get("device", "gpu") != a.device:
            return None
        if not S.fits(s.get("res"), a.cpus, a.mem_mb, a.gpu_mem)[0]:
            return None
        return t["key"] + "\n" + " ".join(shlex.quote(x) for x in run_args(t))
    rc, out = _claim(a, pick)
    if rc == 0:
        print(out + "\n" + a.token)  # 第三行：这次领取的 token（11/1 加，只读两行的老 worker 不受影响）
    return rc


def owns(ci, a):
    """领取记录是不是这个 worker 这一轮的：worker 相同；给了 --token 时 token 也要相同（同名 worker 重开后是新的一轮，11/1）。"""
    if not ci or str(ci.get("worker")) != str(a.worker):
        return False
    return not getattr(a, "token", None) or not ci.get("token") or ci["token"] == a.token


def cmd_beat(a):
    with Lock():  # 和领取用同一把锁：读到自己的记录、写回之间不会被别人接管
        for key in a.keys:
            p = os.path.join(TQ, "claims", safe(key), "info")
            ci = claim_info(key)
            if owns(ci, a):
                ci["beat"] = time.time()
                write_json(p, ci, indent=None)
    return 0


def cmd_release(a):
    with Lock():
        for key in a.keys:
            _release(a, key)
    return 0


def _release(a, key):
    d = os.path.join(TQ, "claims", safe(key))
    ci = claim_info(key)
    if getattr(a, "token", None) and not owns(ci, a):  # 迟到的交还（领取已被新一轮接管，或者已经没了）：什么都不记
        if ci is None and (a.failed or a.skip):
            print(f"{key}：这一轮的领取已经没了（过期后被别人领走、交还过），不记失败 / 跳过")
        return
    if a.skip:
        os.makedirs(os.path.join(TQ, "skips"), exist_ok=True)
        with open(os.path.join(TQ, "skips", safe(key)), "a") as f:
            f.write(f"{time.strftime('%F %T')}\t{a.worker}\t{a.skip}\n")
    if a.failed:
        os.makedirs(os.path.join(TQ, "failures"), exist_ok=True)
        with open(os.path.join(TQ, "failures", safe(key)), "a") as f:
            f.write(f"{time.strftime('%F %T')}\t作业 {a.worker}\t{a.failed}\n")
    if owns(ci, a):
        os.makedirs(os.path.join(TQ, "released"), exist_ok=True)
        with open(os.path.join(TQ, "released", safe(key)), "w") as f:
            f.write(f"{time.time()} {a.worker}\n")
        shutil.rmtree(d, ignore_errors=True)


def cmd_missing(a):
    with Lock():
        return _missing(a)


def _missing(a):
    """缺输入（11/1）：记 inputs/<key>，像 release 一样交还（不算失败）。"""
    ci = claim_info(a.key)
    if not owns(ci, a):
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
    """把指定的一个分给执行机 a.worker（10/31，relay.py、shift.py 调用）：不过期；free / stale / waiting 的能分（waiting 的提前送到那边，
    after 做完后那边接着做）；已经分给它的再分一次照样成功（since 不变）。"""
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
        if ci and ci.get("assigned") and not mine:
            print(f"{a.key}：已分给 {ci['assigned']}")
            return NOTHING
        st, note = state(t, root)
        if st in ("done", "stopped") or (not mine and st not in ("free", "stale", "waiting")):
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
        info = dict(worker=a.worker, host=a.worker, since=ci.get("since", now) if mine else now, beat=now, assigned=a.worker)
        write_json(os.path.join(d, "info"), info, indent=None)
        print(t["key"])
        return 0


# ---------------- 加、改 ----------------
def same_file(keys, have):
    """新 key 转成文件名（safe）后和队列里或这一批里另一个 key 相同 → 说明列表（领取、失败、日志都按文件名，11/1）。"""
    seen, bad = {safe(k): k for k in have}, []
    for k in keys:
        o = seen.setdefault(safe(k), k)
        if o != k:
            bad.append(f"{k} 和 {o} 转成文件名都是 {safe(k)}")
    return bad


def _resolve_new(t):
    if t.get("spec"):
        return S.from_sent(t["spec"])
    import experiments as E
    return E.resolve_task(t["recipe"], t["params"], t.get("exp"))


def _insert(new, nice, update_only=False):
    """把新任务（dict(key, exp, recipe, params[, spec])）加进队列，已有的跳过（带 spec 的换成新的说明）。先解析一遍，写错参数的不加。
    update_only（relay.py resend）：只换已有的说明；解析不了的逐个报（退出码 2），别的照换；队列里没有的打印出来。
    在训的任务 out / done 变了的先不换（worker 手上是旧说明，完成判断会对不上；下次 resend 再换，11/4）。"""
    errs = []
    with Lock():
        tasks = read_tasks()
        have = {t["key"]: t for t in tasks}
        order = max([t["order"] for t in tasks] or [0])
        pairs = []
        for t in new:
            try:
                pairs.append((t, _resolve_new(t)))
            except S.SpecError as e:
                if not update_only:
                    raise
                errs.append(f"{(t.get('spec') or {}).get('key', '?')}：{e}")
        if update_only:
            for _, s in pairs:
                if s["key"] not in have:
                    print(f"队列里没有，不换：{s['key']}")
            pairs = [(t, s) for t, s in pairs if s["key"] in have]
        bad = same_file([s["key"] for t, s in pairs if not (t.get("spec") and s["key"] in have)], have)
        if bad:
            print("加不进去：" + "；".join(bad))
            return 2
        clash = 0
        for t, s in pairs:
            if s["key"] in have:
                old = have[s["key"]]
                if t.get("spec") and old.get("spec") != t["spec"]:
                    was = spec_of(dict(old))
                    if was and (was["out"], was["done"]) != (s["out"], s["done"]) and (claim_state(s["key"], None) or ("",))[0] == "claimed":
                        print(f"在训，out / done 变了，先不换（下次 resend 再换）：{s['key']}")
                        continue
                    old.update(spec=t["spec"], recipe=t["recipe"], params=t["params"])
                    print(f"换了说明：{s['key']}")
                elif not t.get("spec") and not t.get("exp") and _differs(spec_of(old), s):  # --recipe、--file 加的撞名（11/4）
                    print(f"已有同名、说明不一样的任务，没加：{s['key']}（已有 {old.get('recipe')} {json.dumps(old.get('params'), ensure_ascii=False)}）")
                    clash += 1
                else:
                    print(f"已有，跳过：{s['key']}")
                continue
            order += 1
            tasks.append(dict(t, key=s["key"], order=order, nice=nice))
            have[s["key"]] = tasks[-1]
            print(f"加入：{nice:>5} {s['key']}（{t['recipe']}，环境 {','.join(S.envs_of(s))}）")
        write_tasks(tasks)
    for e in errs:
        print(f"换不了：{e}")
    return 2 if errs or clash else 0


def _differs(a, b):
    drop = ("exp", "params", "names", "recipe")
    return a is None or {k: v for k, v in a.items() if k not in drop} != {k: v for k, v in b.items() if k not in drop}


def read_sheets(path):
    """任务单文件（JSON：一张 {字段: 值} 或一列）→ ([dict(exp, recipe, params)], 档位)。recipe 不写 = command；
    nice 写在单子里（几张单子的 nice 要一样）；其余字段都是做法的参数（scripts/taskq/queue/README.md 的"别人的任务"）。"""
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
        elif a.spec_stdin:
            try:
                got = json.loads(sys.stdin.read())
            except ValueError as e:
                raise S.SpecError(f"说明不是 JSON：{e}") from None
            got = got if isinstance(got, list) else [got]
            if not all(isinstance(x, dict) for x in got):
                raise S.SpecError("说明要是 JSON 对象或一列对象")
            new = [dict(exp=None, recipe=x.get("recipe"), params=x.get("params") or {}, spec=x) for x in got]
        elif a.recipe:
            new = [dict(exp=None, recipe=a.recipe, params=S.parse_kv(a.args))]
        elif a.file:
            new, nice = read_sheets(a.file)
            if a.nice is None:
                a.nice = nice
        else:
            sys.exit("要给 --exp、--recipe、--file 或 --spec-stdin")
        if a.nice is None:
            sys.exit("要给 --nice")
        return _insert(new, a.nice, update_only=a.update_only)
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
        if t.get("spec"):  # 执行机的队列：说明是 box 解析好送来的，这边按参数重新解析会丢掉它（11/4）
            print(f"{a.key} 的说明是 box 送来的：改总队列里的参数，再 relay.py resend")
            return 2
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


RECORDS = ("failures", "stopped", "inputs", "released")  # remove 时和任务一起删的记录（执行机的队列另加 skips/）


def cmd_remove(a):
    """从队列里删掉没人在做的任务（10/28：DICC 停机期间把排在那边的评测挪回 box）。有活着的领取（不管状态）、训练日志刚更新的不删；
    别的任务的 after 里有它、又不一起删的不删。删的时候连同它的失败、停止、缺输入、交还记录和过期的领取一起删（同一个 key 以后再加进来
    是新任务，11/4）；跳过（skips/）只在执行机的队列（带 spec 的记录）里删，总队列里留着（哪台执行机训不了，换个时候再加也一样）。
    结果文件不动。退出码 0 删了；REFUSED 不删；GONE 队列里没有（什么都没删）。"""
    root = data_root()
    with Lock():
        tasks = read_tasks()
        by = {t["key"]: t for t in tasks}
        missing = [k for k in a.keys if k not in by]
        bad = []
        for k in a.keys:
            if k in by:
                s = spec_of(by[k])
                cs = claim_state(k, S.log_age(s, root) if s else None)  # 不看状态：停了的、done 齐了的 worker 也可能还领着（11/4）
                ci = claim_info(k) or {}
                if ci.get("assigned"):
                    bad.append(f"{k}：分给了 {ci['assigned']}，先 stop，交还后再删")
                elif cs and cs[0] == "claimed":
                    bad.append(f"{k}：claimed（{cs[1]}），有人领着")
                elif state(by[k], root)[0] == "busy":
                    bad.append(f"{k}：busy，有人在做")
        for t in tasks:
            if t["key"] in a.keys:
                continue
            dep = [k for k in ((spec_of(t) or {}).get("after") or []) if k in a.keys and k in by]
            if dep:
                bad.append(f"{t['key']} 的 after 里有 {' '.join(dep)}（一起删，或者先改它）")
        for k in missing:
            print(f"队列里没有：{k}")
        for b in bad:
            print(f"不删：{b}")
        if bad:
            return REFUSED
        if missing:
            return GONE
        write_tasks([t for t in tasks if t["key"] not in a.keys])
        for k in a.keys:
            for sub in RECORDS + (("skips",) if by[k].get("spec") else ()):
                try:
                    os.remove(os.path.join(TQ, sub, safe(k)))
                except FileNotFoundError:
                    pass
            shutil.rmtree(os.path.join(TQ, "claims", safe(k)), ignore_errors=True)  # 只剩过期的
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
    print(f"  run_task.sh {' '.join(shlex.quote(x) for x in run_args(t))}")
    if s:
        for k in ("env", "cmd", "out", "done", "data"):
            print(f"  {k:5} {s[k]}")
    return 0


# ---------------- 状态 ----------------
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
                        assigned=ci.get("assigned"), input_since=(input_info(t["key"]) or (None,))[0],
                        beat=ci.get("beat"), log_age=S.log_age(s, root) if s else None,
                        task_log_age=task_log_age(root, t["key"], ci.get("worker")),
                        out_age=out_age(s, root) if s and st == "claimed" else None, failures=failures(t["key"]),
                        skipped_by=sorted({w for _, w, _ in skips(t["key"])})))
    nf = recent_failures()
    day = [w for ts, w, _ in skips() if ts >= time.time() - 86400]
    return dict(enabled=os.path.exists(os.path.join(TQ, "ENABLED")), tasks=out, recent_failures=nf, paused=nf >= PAUSE_N,
                recent_skips={w: day.count(w) for w in set(day)})


def central(args, timeout=420):  # 比锁的最长等待（300 秒）长：等不到锁时拿到退出码，不是超时
    """box 上改总队列：本机跑 taskq.py <args>（参数列表或 shell 写法的字符串）→ (退出码, 标准输出, 标准错误)。"""
    import subprocess
    argv = shlex.split(args) if isinstance(args, str) else list(args)
    r = subprocess.run([sys.executable, os.path.abspath(__file__)] + argv, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


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
            env = ",".join(S.envs_of(t.get("spec"))) or "?"
            print(f"  {t['nice']:>5} {t['key']:60} {env:10} {t['state']} {t['note']}")
    print("；".join(f"{k} {v}" for k, v in sorted(n.items())))
    return 0


def main(argv):
    if argv[:1] == ["--json-status"]:
        S.json_out(local_status())
        return 0
    cmd = argv[0] if argv else ""
    if cmd in ("add", "set", "set-nice", "take", "show", "remove", "stop", "resume", "status") \
            and not have_queue() and here() != queue_host():
        print(f"总队列在 {queue_host()}，在那边跑（本机没有 {tasks_path()}）")
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
    p.add_argument("--cpus", type=int, help="这个 worker 每个任务给的 CPU 数；任务的 res.cpus 更多的不领（11/1，不给不查）")
    p.add_argument("--mem-mb", type=int, help="每个任务给的内存（MB）；res.mem 更多的不领")
    p.add_argument("--gpu-mem", type=float, help="卡的显存（GB）；res.gpu_mem 更多的不领")
    tok = "next 打印的第三行；给了就只动这一轮的领取（11/1）"
    p = sub.add_parser("beat")
    p.add_argument("--worker", required=True)
    p.add_argument("--token", help=tok)
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("release")
    p.add_argument("--worker", required=True)
    p.add_argument("--token", help=tok)
    p.add_argument("keys", nargs="+")
    p.add_argument("--failed")
    p.add_argument("--skip", help="这台机器训不了、以后不再领它（记在 skips/，不算失败）")
    p = sub.add_parser("missing")
    p.add_argument("--worker", required=True)
    p.add_argument("--token", help=tok)
    p.add_argument("--why", default="")
    p.add_argument("key")
    p = sub.add_parser("input-ok")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("take")
    p.add_argument("--worker", required=True)
    p.add_argument("--assign", action="store_true", required=True, help="分给执行机：不过期，waiting 的也能分（10/31）")
    p.add_argument("key")
    p = sub.add_parser("add")
    p.add_argument("--nice", type=int, help="档位；--exp 时不给就用实验表里的 nice")
    p.add_argument("--exp")
    p.add_argument("--only", action="append", default=[], metavar="参数=取值[,取值…]")
    p.add_argument("--recipe")
    p.add_argument("--file", help="任务单（JSON），不写 recipe 时是 command 做法")
    p.add_argument("--spec-stdin", action="store_true", help="从标准输入读解析好的说明（一个或列表，spec.py resolve 的格式；box 的 relay 送任务用）")
    p.add_argument("--update-only", action="store_true", help="只换已有任务的说明，不加新的（relay.py resend）")
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
                stop=cmd_stop, resume=cmd_resume,
                missing=cmd_missing, input_ok=cmd_input_ok)[a.cmd.replace("-", "_")](a)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
