#!/usr/bin/env python3
"""调度的协议（2026-11-04）：跨脚本、跨机器、跨版本共用的名字、目录布局、格式、常数、退出码和字段，都只在这里定义。
盘上的名字和取值改了，在跑的 worker（用开始时的代码包，和新代码共用同一个队列目录）就对不上：这里的值只加不改。

只用标准库、不 import 别的调度文件：spec.py 的 remote_collect 把本文件和 spec.py 一起经 ssh 送到别的机器上跑。
  python3 proto.py sh     给 bash 的赋值语句（eval "$(python3 proto.py sh)"），名字是 TQ_<大写>
  python3 proto.py json   全部协议（JSON）
"""
import json
import sys

VERSION = 1  # 跨版本的命令（status --json、collect --json）输出里带上；字段只加不改，改了才加 1

# ---------------- 执行机根目录 R（envs/machines.py 的 root；relay.py 建，代码包里的 worker 用） ----------------
ROOT = dict(
    code="code",                 # 代码包 code/<包名>（relay_bundle.py，名字是内容哈希）
    current="code/current",      # 在用的代码包（软链接；排着的作业开始时才解析）
    queue="queue",               # 执行机的任务队列（TASKQ_DIR）
    blobs="blobs",               # 数据文件，按 sha256 存
    data="data",                 # 各任务的视图（TASKQ_DATA）
    state="state",               # relay 的记账
    pins="state/pins",           # 正在传、视图还没建好的任务钉住的 blob（relay.py stage 写，relay_store.py gc 不删）
    envs_ok="state/envs",        # 环境装好了的标志 <环境>.ok（sites/envs.py）
    view_lock="state/view.lock",  # 建视图、删视图的锁
    deploy_lock="state/deploy.lock",  # 推代码包的锁（relay.py deploy）
    tmp="tmp",                   # 传到一半的临时文件
    envs="envs",                 # 执行机上装的环境（conda、venv）
)
MKDIRS = ("code", "queue", "blobs", "tmp", "state", "data/logs/tasks", "data/logs/workers")  # relay.py 第一次建根目录时建的


def blob(h):
    """sha256 → 根目录下的相对路径。"""
    return f"blobs/{h[:2]}/{h}"


# ---------------- 队列目录（总队列和执行机的一样，taskq.py） ----------------
QUEUE = dict(
    tasks="tasks.jsonl",      # 每行一个任务
    claims="claims",          # claims/<safe key>/info
    failures="failures",
    skips="skips",
    inputs="inputs",          # 缺输入
    released="released",      # 最近一次交还的时间
    stopped="stopped",        # stopped/<safe key>：停止的任务，在训的 worker 每 5 秒看一次
    enabled="ENABLED",        # 总开关：排着的 worker 开始时没有它就结束（worker_start.sh）
    lock="lock",
    gpu_pad_off="gpu_pad_DISABLED",  # 执行机的显存填充总开关（worker.sbatch 设 TASKQ_GPU_PAD_SWITCH）
)
CLAIM_INFO = "info"
# claims/<key>/info 的字段：worker 领的（执行机的队列）/ 总队列分给执行机的
CLAIM_FIELDS = ("worker", "host", "since", "beat", "token")
ASSIGN_FIELDS = ("worker", "host", "since", "beat", "assigned")

# ---------------- 状态目录（cfg.py 的 STATE_PLACES；relay_bundle.py 在代码包里改写到根目录下） ----------------
STATE_HOME = "~/.cache/phd/"
STATE_PLACES = {"taskq": "~/.cache/phd/taskq", "gpu_pad/DISABLED": "~/.cache/phd/gpu_pad/DISABLED"}
STATE_IN_ROOT = {"taskq": ROOT["queue"], "gpu_pad/DISABLED": ROOT["queue"] + "/" + QUEUE["gpu_pad_off"]}  # 其余 → state/

# ---------------- 标志文件 ----------------
INCOMPLETE = ".incomplete"     # 文件夹建到一半（tunnel_push.sh、relay 建视图）；任一层有就算缺
MANIFEST = "MANIFEST.json"     # 拉回的结果：最后拷，有它才算拉完（relay_transfer.py）
INFER_DONE = ".infer_done"
ARCHIVED = "ARCHIVED"          # 10/31 前的归档标志：归档后删了 checkpoint 的模型；现在不再写，只认
STAGE_PREFIX = "phd_stage_"    # 节点本地盘上的输入副本 $TMPDIR/phd_stage_<作业号>（inputs.py、run_task.sh）
GPU_PAD_BASE = "/tmp/gpu_pad_"  # 显存填充和 worker 的握手 /tmp/gpu_pad_<作业号>{.mib,.ready,.start}
GPU_PAD_FILES = (".mib", ".ready", ".start")

# ---------------- 日志 ----------------
TASK_LOGS = "logs/tasks"       # 数据目录下：分段日志 <safe key>__<作业>_part<段>.log（10/31 前 _seg<段>）
WORKER_LOGS = "logs/workers"   # worker、显存填充、Slurm 的 %x_%j.out
LOG_PARTS = ("part", "seg")
DONE_LINE = "训完"             # 分段日志里以它开头的行 = 训完（run_task.sh 写，spec.py 认）
STALL_LINE = "当作卡住"        # 分段日志里以它开头的行 = 被 worker 当作卡住停掉（dicc_worker.sh 写、数）

# ---------------- key → 文件名 ----------------
SAFE = "__"


def safe(key):
    """key → 文件名（领取记录、停止、分段日志）。"""
    return key.replace("/", SAFE)


def task_log(key, job, part=1, kind="part"):
    """分段日志相对数据目录的路径。"""
    return f"{TASK_LOGS}/{safe(key)}{SAFE}{job}_{kind}{part}.log"


# ---------------- 经 ssh 读回的 JSON ----------------
JSON_MARK = "@@JSON "  # 只认以它开头的那一行（spec.json_out / json_in）

# ---------------- 时间（秒） ----------------
STALE_BEAT = 20 * 60   # 领取的心跳这么久没续算过期
FRESH_LOG = 15 * 60    # 分段日志这么久内更新过，算还在训（不领）
HALF_CLAIM = 5 * 60    # 只建了目录、没有 info 的领取，这么久后算过期
BEAT = 5 * 60          # worker 心跳间隔


def claim_alive(info, made, now, log_age=None):
    """一个执行机队列里的领取还活着吗（taskq.py 和 box 上读那边的 relay_remote.py 都用这一条）。
    info：claims/<key>/info 的内容（读不到 = None）；made：目录的修改时间；log_age：分段日志多久没更新（秒）。"""
    if info is None:
        return now - made <= HALF_CLAIM
    if info.get("assigned"):
        return True
    if now - (info.get("beat") or 0) < STALE_BEAT:
        return True
    return log_age is not None and log_age < FRESH_LOG


# ---------------- 任务的状态（taskq.py state()，local_status 的 state） ----------------
STATES = dict(
    free="能领",
    claimed="有人领着（或分给了执行机）",
    busy="没人领着，但训练日志 15 分钟内更新过",
    stale="领取过期",
    waiting="after 里的任务还没做完",
    blocked="after 里的任务失败 2 次或不在队列里",
    failed="失败 2 次",
    input="缺输入",
    stopped="停止",
    bad="说明解析不了",
    done="训完",
)

# ---------------- 退出码 ----------------
EXIT = dict(
    # taskq.py
    next_ok=0, nothing=10, wait=11, paused=12, lost=13,  # next / beat --strict；12 也表示读不了数据目录
    refused=3, gone=4,                                   # remove
    no_queue=2,                                          # 不在 box、本机又没有队列
    # run_task.sh
    run_done=0, run_failed=1, run_bad=2, run_not_done=3, run_missing=4, run_unreadable=5, run_term=143,
    # spec.py done
    done_yes=0, done_no=1, done_bad=2, done_unreadable=5,
    # relay.py stage
    stage_later=75,
    # common.run：超时、起不来
    timeout=124,
)

# ---------------- 字段表 ----------------
# taskq.local_status() 每个任务（tasks.jsonl 的一行之外）
STATUS_FIELDS = ("spec", "state", "note", "worker", "host", "since", "assigned", "input_since", "beat", "log_age",
                 "task_log_age", "out_age", "failures", "skipped_by")
STATUS_TOP = ("enabled", "tasks", "recent_failures", "paused", "recent_skips")
TASK_FIELDS = ("order", "nice", "key", "exp", "recipe", "params", "spec")  # tasks.jsonl 的一行
COLLECT_FIELDS = ("key", "out", "done", "log", "progress", "info", "metrics", "resume", "oldlogs")  # spec.collect 要的说明字段
COLLECT_TOP = ("tasks", "seglogs", "stuck", "errors")  # spec.collect 的结果
COLLECT_TASK = ("runs", "final", "archived", "dice", "debug", "resumable")  # 每个任务
SEGLOG_FIELDS = ("name", "first", "trained")

# ---------------- 报警（health.py 的问题编号；plain.py 按 kind 说白话） ----------------
# 编号 = kind:<参数>…，参数按下面的顺序用 ":" 连起来；同一编号的问题只推一次
ALERTS = dict(
    down=("machine",), unread=("machine",), readerr=("machine",), code=("machine",), taskq=("machine",),
    tqoff=("machine",), tqblocked=("key",), pueued=("machine",), stuck=("machine", "id"), pueue=("machine", "id", "end"),
    idle=("machine", "group"), dup=("key",), disk=("machine",), gpulog=("machine",), boot=("machine", "time"), board=(),
    site=("machine", "sub", "key"), topup=("machine", "qos", "kind"), advance=("step", "who"),
    relaystage=("key",), relayfetch=("key",), relay=("machine",), feed=("sub", "machine"),
)


def alert_id(kind, *args):
    return ":".join([kind] + [str(a) for a in args if a not in (None, "")])


# 问题的值：说明文字（str），或带结构化数据的 dict(msg=说明, data={…})（11/4；plain.py 读 data，不从文字里抠）
def alert(msg, **data):
    return dict(msg=msg, data=data) if data else msg


def alert_msg(v):
    return v["msg"] if isinstance(v, dict) else v


def alert_data(v):
    return (v.get("data") or {}) if isinstance(v, dict) else {}


# ---------------- 给 bash ----------------
def sh_vars():
    out = {"TQ_SAFE": SAFE, "TQ_STAGE_PREFIX": STAGE_PREFIX, "TQ_GPU_PAD_BASE": GPU_PAD_BASE, "TQ_TASK_LOGS": TASK_LOGS,
           "TQ_WORKER_LOGS": WORKER_LOGS, "TQ_STALL_LINE": STALL_LINE, "TQ_DONE_LINE": DONE_LINE, "TQ_JSON_MARK": JSON_MARK,
           "TQ_INCOMPLETE": INCOMPLETE, "TQ_STALE_BEAT": STALE_BEAT, "TQ_FRESH_LOG": FRESH_LOG, "TQ_BEAT": BEAT}
    out.update({f"TQ_ROOT_{k.upper()}": v for k, v in ROOT.items()})
    out.update({f"TQ_Q_{k.upper()}": v for k, v in QUEUE.items()})
    out.update({f"TQ_EXIT_{k.upper()}": v for k, v in EXIT.items()})
    return out


def everything():
    g = globals()
    return {k: g[k] for k in sorted(g) if k.isupper() and not k.startswith("_")}


def main(argv):
    import shlex
    if argv[:1] == ["sh"]:
        print("\n".join(f"{k}={shlex.quote(str(v))}" for k, v in sh_vars().items()))
        return 0
    if argv[:1] == ["json"]:
        print(json.dumps(everything(), ensure_ascii=False, indent=1))
        return 0
    print(__doc__.strip(), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
