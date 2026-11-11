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

# ---------------- 执行机根目录 R（机器表 machines.py 的 root；relay.py 建，代码包里的 worker 用） ----------------
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
# 代码包里调度核心的位置（相对包的根，11/4）：排着的 Slurm 作业、lab 的 worker 命令、旧包的转交都按这个位置找，不改
BUNDLE_CORE = "scripts/taskq"
WORKER_LABEL = "worker:"  # pueue 执行机上 worker 任务的标签前缀：worker:<id>（sites/pueue.py）
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
    released="released",      # 最近一次交还："<时间> <worker> <原因>"（原因见 RELEASE_WHY，11/7 前的只有前两项）
    stopped="stopped",        # stopped/<safe key>：停止的任务，在训的 worker 每 5 秒看一次
    enabled="ENABLED",        # 总开关：排着的 worker 开始时没有它就结束（worker_start.sh）
    lock="lock",
    gpu_pad_off="gpu_pad_DISABLED",  # 执行机的显存填充总开关（worker.sbatch 设 TASKQ_GPU_PAD_SWITCH）
)
CLAIM_INFO = "info"
# claims/<key>/info 的字段：worker 领的（执行机的队列）/ 总队列分给执行机的
CLAIM_FIELDS = ("worker", "host", "since", "beat", "token", "log")  # log：分段日志名里的作业名（next --log-tag，11/4；没有时同 worker）
ASSIGN_FIELDS = ("worker", "host", "since", "beat", "assigned")

# ---------------- 状态目录在执行机根目录里的位置（家目录下的默认位置见 cfg.py 的 STATE_HOME、STATE_PLACES，键相同；
# relay_bundle.py 在代码包里改写到根目录下） ----------------
STATE_IN_ROOT = {"taskq": ROOT["queue"], "gpu_pad/DISABLED": ROOT["queue"] + "/" + QUEUE["gpu_pad_off"]}  # 其余 → state/

# ---------------- 标志文件 ----------------
INCOMPLETE = ".incomplete"     # 文件夹建到一半（tunnel_push.sh、relay 建视图）；任一层有就算缺
MANIFEST = "MANIFEST.json"     # 拉回的结果：最后拷，有它才算拉完（relay_transfer.py）
INFER_DONE = ".infer_done"
RELAY_INBOX = ".relay_in"      # hub 数据目录下拉回到一半的结果 .relay_in/<safe key>（relay_transfer.py）
ARCHIVED = "ARCHIVED"          # 10/31 前的归档标志：归档后删了 checkpoint 的模型；现在不再写，只认
STAGE_PREFIX = "phd_stage_"    # 旧前缀：11/6 前的作业建的本地输入副本，只认不建（11/8 以后删）
STAGE_DIR_PREFIX = "taskq_stage_"  # 节点本地盘上的输入副本 $TMPDIR/taskq_stage_<作业号>（inputs.py、run_task.sh、consumers/slurm.py）
STAGE_PREFIXES = (STAGE_DIR_PREFIX, STAGE_PREFIX)  # 认目录（清理、共用 blob、拷贝锁）时新旧都认（旧的 11/8 以后删）
GPU_PAD_BASE = "/tmp/gpu_pad_"  # 显存填充和 worker 的握手 /tmp/gpu_pad_<作业号>{.mib,.ready,.start}
GPU_PAD_FILES = (".mib", ".ready", ".start")

# ---------------- 日志 ----------------
TASK_LOGS = "logs/tasks"       # 数据目录下：分段日志 <safe key>__<作业>_part<段>.log（10/31 前 _seg<段>）
WORKER_LOGS = "logs/workers"   # worker、显存填充、Slurm 的 %x_%j.out
LOG_PARTS = ("part", "seg")
DONE_LINE = "训完"             # 分段日志里以它开头的行 = 训完（run_task.sh 写，spec.py 认）
STALL_LINE = "当作卡住"        # 分段日志里以它开头的行 = 被 worker 当作卡住停掉（consume.py 写、数）

# ---------------- GPU 使用记录（monitor/gpu_log.sh 写；run_task.sh 的 TASK_GPU_IDLE、健康检查读） ----------------
GPU_LOG_DIR = "~/gpu_usage"      # 环境变量 GPU_LOG_DIR 可以改
GPU_LOG_FILE = "gpu_%Y-%m.csv"   # 每月一个（strftime）
GPU_LOG_PID = "gpu_log.pid"      # GPU_LOG_DIR 里：在跑的那个 gpu_log.sh 的进程号，它跑着时一直锁着（flock）；on_boot.sh 看锁决定开不开

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
FAIL_RETRY = 60 * 60   # 任务失败或卡住交还后，同一个 worker 隔这么久再领（失败满 MAX_FAIL 次后队列记为 failed，谁都不再领）


def claim_alive(info, made, now, log_age=None, stale=None, fresh=None, half=None):
    """一个执行机队列里的领取还活着吗（taskq.py 和 hub 上读那边的 relay_remote.py 都用这一条）。
    info：claims/<key>/info 的内容（读不到 = None）；made：目录的修改时间；log_age：分段日志多久没更新（秒）。
    stale、fresh、half：代替 STALE_BEAT、FRESH_LOG、HALF_CLAIM（taskq.py 的测试用环境变量改）。"""
    if info is None:
        return now - made <= (HALF_CLAIM if half is None else half)
    if info.get("assigned"):
        return True
    if now - (info.get("beat") or 0) < (STALE_BEAT if stale is None else stale):
        return True
    return log_age is not None and log_age < (FRESH_LOG if fresh is None else fresh)


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
S_FREE, S_CLAIMED, S_BUSY, S_STALE, S_WAITING, S_BLOCKED = "free", "claimed", "busy", "stale", "waiting", "blocked"
S_FAILED, S_INPUT, S_STOPPED, S_BAD, S_DONE = "failed", "input", "stopped", "bad", "done"
assert {S_FREE, S_CLAIMED, S_BUSY, S_STALE, S_WAITING, S_BLOCKED, S_FAILED, S_INPUT, S_STOPPED, S_BAD, S_DONE} == set(STATES)
UNCLAIMED = (S_FREE, S_STALE)  # 没人领着、可以领的
# 分给执行机、hub 先不送过去的原因（relay 的状态 held 的 why；queue/bundle_gate.py，11/5）
HELD = dict(bundle="bundle-incompatible")  # 那台在用的代码包跑不了这个说明

# ---------------- 退出码 ----------------
EXIT = dict(
    # taskq.py
    next_ok=0, nothing=10, wait=11, paused=12, lost=13,  # next / beat --strict；12 也表示读不了数据目录
    refused=3, gone=4,                                   # remove
    no_queue=2,                                          # 不在 hub、本机又没有队列
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
                 "task_log_age", "out_age", "failures", "skipped_by", "progress")  # progress：这台上做了一半（taskq.has_progress，11/7）
STATUS_TOP = ("enabled", "tasks", "recent_failures", "paused", "recent_skips")
TASK_FIELDS = ("order", "nice", "key", "exp", "recipe", "params", "spec", "added")  # tasks.jsonl 的一行；added：加进这个队列的时间（11/7 起，旧的没有）
# released/<key> 的第三项：为什么交还（taskq.py 按 release 的参数记，worker 的调用方式不变）
RELEASE_WHY = dict(back="back",        # 普通交还：被停、到时限、卡住、这一轮不训了
                   failed="failed",    # release --failed
                   skip="skip",        # release --skip：这台训不了
                   missing="missing")  # missing：缺输入
COLLECT_FIELDS = ("key", "out", "done", "log", "progress", "info", "metrics", "resume", "oldlogs")  # spec.collect 要的说明字段
COLLECT_TOP = ("tasks", "seglogs", "stuck", "errors")  # spec.collect 的结果
COLLECT_TASK = ("runs", "final", "archived", "dice", "debug", "resumable")  # 每个任务
SEGLOG_FIELDS = ("name", "first", "trained")
LOG_HEAD_CODE = r"代码 (\S+?)，"  # 分段日志第一行（run_task.sh 写）里代码提交号的写法（正则）

# experiments.py list --json / tasks --json（11/4）：一行 JSON，可调用的对象写成名字，别的对象写成 {"type": 类名, 属性…}
EXP_LIST_TOP = ("version", "experiments", "final")      # experiments：实验表的条目（experiments.EXP_FIELDS 的字段）；final：FINAL 的步骤
EXP_TASKS_TOP = ("version", "tasks")
EXP_TASK_FIELDS = ("key", "exp", "recipe", "params", "each", "short", "label", "names", "spec")  # label：hub pueue 标签里的任务名
# cli_json.py（taskq exp steps，11/4）：每个任务的步骤（task）、实验一级的（group）、FINAL（final），命令按 names 填好
EXP_STEPS_TOP = ("version", "data", "python", "steps")
EXP_STEP_FIELDS = ("level", "exp", "key", "name", "group", "cmd", "names", "done", "needs", "error")  # done、needs：没要时 null

# cli_json.py（taskq machines show，11/4）：核心按机器表算出来的值
MSHOW_TOP = ("version", "hub", "machines", "specs")
MSHOW_MACHINE = ("name", "table", "label", "scheduler", "roles", "relay", "kinds", "workers", "worker_cmds", "root", "root_q",
                 "ready_envs", "activate", "adopt", "task_sh")  # ready_envs：不看建好的标志就能用的环境；task_sh：spec shell 在这台加的语句
MSHOW_SPEC = ("hash", "base", "torch", "error")  # torch：{变体: index_url}；error：描述读不了时的原因，没问题时 null

# cli_json.py（taskq spec export / import，11/4）：中继插件送出、拉回核对
RELAY_EXPORT_TOP = ("version", "plugin", "params", "files", "texts", "views", "outs", "cleanup", "logs")  # logs：分段日志的文件名前缀
RELAY_IMPORT_TOP = ("version", "written")

# overview.py --json（11/4）：登记表这类汇总要的全部状态，一行 JSON
OVERVIEW_TOP = ("version", "now", "hub", "data", "queue", "sources", "machines", "tasks", "health", "code")
OVERVIEW_QUEUE_TOP = ("enabled", "readable")               # 总队列开着没、读到没
OVERVIEW_SOURCE = ("name", "label", "kind", "time")        # 收集到训练状态的地方；kind：machine（gather 收集的）/ relay（relay.py 的 status.json）
OVERVIEW_MACHINE = ("name", "label", "scheduler", "ok", "why", "gpus", "pueue")  # gpus：[{gpu, uuid, util, mem, apps: [{pid, mem, args}]}]
OVERVIEW_PUEUE = ("groups", "tasks", "error")              # tasks 每个：qs --json 的字段，加 start_ts、end_ts（秒）、about（plain.label：[实验, 做了什么, 简称] 或 null，11/4）
OVERVIEW_TASK = ("key", "exp", "recipe", "params", "names", "owner", "out", "checkpoint", "queue", "copy", "total", "progress",
                 "eta", "parts", "commits")
OVERVIEW_TASK_QUEUE = ("rank", "state", "nice", "assigned", "note", "worker", "host")  # rank：总队列里没做完的任务中排第几（1 起）
OVERVIEW_COPY = ("where", "source", "final", "archived", "epochs", "start", "last", "runs", "dice", "debug", "resumable")
OVERVIEW_PART = ("name", "job", "part", "machine", "trained", "commit")  # 分段日志：作业、第几段、哪台机器、至少做完一轮没、代码提交号
OVERVIEW_PROBLEM = ("id", "msg", "level", "kind", "first", "active", "notified", "plain")  # plain：推送用的白话（plain.problem，11/4）
TOTAL_INFO = "num_epochs"  # 进度读法 INFO 里"一共几轮"的键（runs 里没有 total 时用；overview 的 total、progress、eta）

# ---------------- 报警（health.py 的问题编号；plain.py 按 kind 说白话） ----------------
# 编号 = kind:<参数>…，参数按下面的顺序用 ":" 连起来；同一编号的问题只推一次
ALERTS = dict(
    down=("machine",), unread=("machine",), readerr=("machine",), code=("machine",), taskq=("machine",),
    tqoff=("machine",), tqblocked=("key",), pueued=("machine",), stuck=("machine", "id"), pueue=("machine", "id", "end"),
    idle=("machine", "group"), dup=("key",), disk=("machine",), gpulog=("machine",), boot=("machine", "time"), board=(),
    site=("machine", "sub", "key"), topup=("machine", "qos", "kind"), advance=("step", "who"),
    relaystage=("key",), relayfetch=("key",), relay=("machine",), feed=("sub", "machine"),
    relayadd=("machine", "op", "key"),
    bundlegate=("machine", "key"),
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


# ---------------- 环境变量（TASKQ_*）：谁设、什么意思；kind = run（生产里会设）/ tune（阈值，生产不设、用默认）/ test（只给测试） ----------------
ENV = dict(
    TASKQ_DATA=("run", "数据目录；消费者（consume.py）按 machines.py 的 data 设，执行机是 R/data（relay_remote.prefix）"),
    TASKQ_DIR=("run", "队列目录；执行机是 R/queue（worker.sbatch、pueue 的 worker 命令、relay_remote.prefix 设），没设时用状态目录的 taskq"),
    TASKQ_CONFIG=("run", "配置文件夹（machines.py、policy.py）；没设时用使用者仓库 .taskq.toml 的 config，没有 .taskq.toml 时用 <仓库>/config（cfg.py）"),
    TASKQ_PLUGINS=("run", "插件文件夹；没设时用使用者仓库 .taskq.toml 的 plugins（cfg.py）"),
    TASKQ_STATE=("run", "状态文件夹的根；没设时用配置里的（cfg.py）"),
    TASKQ_REPO=("run", "使用者的仓库根；没设时：核心嵌在 <仓库>/scripts/taskq 里就是 <仓库>，否则按 project.py 找项目，找不到是核心本身"),
    TASKQ_PROJECT=("run", "使用者的项目根（有 .taskq.toml）；没设时从当前目录往上找（project.py）"),
    TASKQ_HOME=("run", "install.sh 把 taskq 克隆到哪里（默认 ~/.local/share/taskq）"),
    TASKQ_ORIGIN=("run", "install.sh 从哪里克隆（默认 GitHub 上的仓库）"),
    TASKQ_CPUS=("run", "Slurm 外的 worker 最多用几个核（pueue 的 worker 命令按 machines.py 的 cpus 设）"),
    TASKQ_GPU_PAD_SWITCH=("run", "显存填充的总开关文件（worker.sbatch 设成 R/queue/gpu_pad_DISABLED）"),
    TASKQ_GPU_PAD=("run", "0 = 这个作业不开显存填充（自己推理、分析的脚本设）"),
    TASKQ_GPU_PAD_STARTED=("run", "显存填充已经开了（gpu_pad_start.sh 设，免得重复开）"),
    TASKQ_ZOMBIES=("run", "旧 worker 换新代码包时交给新进程的停不下来的训练，每行 pid sid key token why safe（11/5 前的 dicc_worker.sh 写，consume.py 照样认）"),
    TASKQ_STAGE_TMP=("run", "1/0：开训后把输入拷到节点本地盘；没设时看 machines.py 的 stage_tmp"),
    TASKQ_LIB=("run", "调度核心的 queue/ 文件夹（run_task.sh 给任务的命令设，同时把核心的 bin/ 加在 PATH 前面）"),
    TASKQ_ENVS=("run", "worker 能领的环境（逗号分隔）；没设时看 machines.py 的 envs"),
    TASKQ_PROG=("run", "bin/taskq 转给 bash 脚本时的命令名（如 taskq boot），脚本的用法、出错里用；脚本读完就 unset"),
    TASKQ_TASK_DONE=("run", "hub 的 pueue 任务做完后 kick 健康检查时设成它的标签（refill.py 包的命令；11/6 前的命令里叫 PHD_TASK_DONE）"),
    TASKQ_SLOTS=("tune", "一个 worker 最多同时训几个；Slurm 里按核数和内存（2 或 1），Slurm 外看 machines.py 的 slots"),
    TASKQ_MACHINE=("test", "当作哪台机器"),
    TASKQ_HEAD_CMD=("test", "代替读代码版本的命令"),
    TASKQ_GPU_PAD_SCRIPT=("test", "代替 gpu_pad.py"),
    TASKQ_GPU_PAD_BASE=("test", "代替 /tmp/gpu_pad_<作业号>"),
    TASKQ_TEST_FAKEBIN=("test", "假命令所在的文件夹"),
    TASKQ_LAST=("test", "代替状态目录的 last"),
    TASKQ_TASKS=("test", "代替状态目录的 tasks.json（任务记录）"),
    TASKQ_LOCK_WAIT=("test", "等锁时每次重试的间隔（秒，默认 0.5）"),
    TASKQ_STALE_BEAT=("test", "代替 STALE_BEAT"),
    TASKQ_FRESH_LOG=("test", "代替 FRESH_LOG"),
)
# 消费者（consume.py）的阈值（tune），默认值在 consume.py 的 Worker.__init__
for _k in ("ADD_AFTER", "UTIL_MAX", "MIN_LEFT", "PATIENCE", "ERR_MAX", "IDLE_MAX", "IDLE_POLL", "IDLE_EXIT", "EARLY_FAIL",
           "FAIL_WINDOW", "FAIL_RETRY", "STALL_MAX", "GIVEUP", "BEAT", "POLL", "STOP_POLL", "STOP_WAIT", "TERM_WAIT"):
    ENV["TASKQ_" + _k] = ("tune", "consume.py 的阈值，默认值在 Worker.__init__")
# 消费者换代码包（11/5）：旧进程把手上的任务写成 JSON 放进 TASKQ_HANDOFF，exec 新包的 worker_start.sh，新进程接着管
HANDOFF = 1                # 交接格式的版本；新进程只认同一版本，认不了的不接（领取过期后别人能领）
HANDOFF_TOP = ("v", "t0", "changed", "excl", "later", "fail_at", "tasks")
HANDOFF_TASK = ("pid", "key", "token", "started", "outdir", "safe", "jtag", "stalled", "why", "sid", "zomb", "ended")
ENV["TASKQ_HANDOFF"] = ("run", "换代码包时旧的消费者交给新的：手上的任务（JSON，字段见 HANDOFF_TOP / HANDOFF_TASK）；worker_start.sh 见到它不再查总开关")
ENV["TASKQ_SITE"] = ("run", "执行机的名字（sites/pueue.py 的 worker 命令设）；消费者的 window 插件按它查时段，没设时按 WORKER_ID 在机器表里反查")
ENV["TASKQ_ADMITTED"] = ("run", "1 = 消费者领之前已经查过卡空不空（gpu_idle 插件），run_task.sh 不再等卡")
ENV["TASKQ_STAGE_ROOT"] = ("run", "节点本地的暂存目录（消费者的 slurm 插件设）；run_task.sh 有它才把输入拷到本地，没有时看 SLURM_JOB_ID")
ENV["TASKQ_DB"] = ("run", "hub database file (db.py); default <state>/taskq.db")
# 本机执行机（SITES 里 ssh=None，和 hub 在同一台机器上，11/7）：hub 的进程带着的这些变量不传给它（同 ssh 到别处时那边本来没有），
# 否则那边的 worker、taskq.py 会用 hub 的配置、数据目录和队列。relay_remote.site_argv、sites/pueue.py 的 worker 命令去掉它们
HUB_ONLY_ENV = ("TASKQ_CONFIG", "TASKQ_PLUGINS", "TASKQ_REPO", "TASKQ_PROJECT", "TASKQ_DATA", "TASKQ_DIR", "TASKQ_STATE",
                "TASKQ_LIB", "TASKQ_ENVS", "TASKQ_MACHINE", "TASKQ_TASKS", "TASKQ_LAST", "RELAY_DIR")
ENV["TASKQ_TEST_NOW"] = ("test", "放着一个时刻（epoch 秒）的文件：window 插件当作现在")
# gpu_pad.py 的参数（tune），默认值在 gpu_pad.py 的命令行
for _k in ("FLOOR_MIB", "TARGET_MIB", "HIGH_MIB", "CHUNK_MIB", "MIN_FREE_MIB", "MIN_TRAIN_MIB", "MIN_TOTAL_MIB", "INTERVAL",
           "SETTLE", "START_HOLD", "RETRY_DELAY", "MAX_FAILS", "RESET_AFTER"):
    ENV["TASKQ_GPU_PAD_" + _k] = ("tune", "gpu_pad.py 的参数，默认值在它的命令行")


# ---------------- 台账（flow/ledger.py，11/5） ----------------
LEDGER_DB = "ledger.db"            # 在状态文件夹下
LEDGER_SHADOW = "shadow"           # 影子对比的记录：<状态文件夹>/shadow/ledger-<日期>.jsonl
A_QUEUED, A_RUNNING, A_OK, A_FAILED, A_STALLED, A_BLOCKED, A_LEGACY, A_ADOPTED = (
    "queued", "running", "ok", "failed", "stalled", "publish_blocked", "legacy", "adopted")
A_DONE = (A_OK, A_LEGACY, A_ADOPTED)  # 算做完的尝试
O_LIVE, O_RETIRED, O_SUPERSEDED = "live", "retired", "superseded"
ALERTS["ledger"] = ("sub",)  # 台账的影子对比出错
ALERTS["engine"] = ("sub",)  # 引擎（flow/engine.py）的影子对比出错
RUNS = "runs"              # 状态文件夹下：taskq run 在跑的尝试的心跳（<实例>.json）
RUN_BEAT = 60              # taskq run 多久写一次心跳
STALL_IDLE = 30 * 60       # CPU 时间和产出都这么久没长 → 卡住
PUBLISH_JOURNAL = "publish"         # 发布的 journal：<状态文件夹>/publish/<实例>.json
PUBLISH_OLD = ".taskq/old"          # 发布时归档的旧文件：<数据目录>/.taskq/old/<实例>/<时间>/
ALERTS["stall"] = ("inst",)  # hub 上 taskq run 包着的步骤卡住
ALERTS["undeclared"] = ("inst",)  # 步骤写到了声明的产出以外
ALERTS["publish"] = ("inst",)  # 发布被冲突挡住（taskq publish 处理）
# 项目挂的提醒（taskq notice，11/5）：健康检查的状态文件夹下 notices/<名字>.txt，内容是提醒的文字；每个报一个问题 notice:<名字>
NOTICES = "notices"
NOTICE_NAME = r"[A-Za-z0-9_-]+"
NOTICE_TOP = ("version", "notices")  # taskq notice list --json
NOTICE_FIELDS = ("name", "text", "since", "at")  # since：最后一次挂上的时间（%Y-%m-%d %H:%M:%S）
ALERTS["notice"] = ("name",)
ACKED = "acked.json"  # taskq notice ack（11/5）：健康检查的状态文件夹下，{问题编号: 确认的时间}；只认 kind=event 的问题，问题消失后这一项跟着删
AUTO_SWITCHES = ("advance", "ledger", "run", "smoke", "envs", "revive")  # policy.AUTO 里机器名以外的开关：advance 排下游、台账的影子对比、taskq run 包 hub 的步骤、冒烟闸门、
# envs 自动建环境（True，或 {机器: [环境 或 (环境, 变体)]} 另外预建的，sites/envs.py）、revive pueued 没了时健康检查自动恢复（11/6）
PUEUE_SEEN = "pueue_seen.json"  # 健康检查的状态文件夹下，{pueue 机器: {t: 最后一次查到 pueued 在的时间戳, running: [那时开着的组]}}（11/6，revive 用）

# ---------------- 冒烟（smoke_gate.py，11/5） ----------------
SMOKE_PREFIX = "_smoke/"   # 冒烟任务的 key：_smoke/<执行机>/<种类 id>-<时间>-<随机>（{smoke} = <执行机>-<种类 id>-<时间>-<随机>）
SMOKE_DIR = "smoke"        # 总队列里：smoke/<执行机>.json，每种任务在那台机器上过没过
SMOKE = ("ok", "failed", "queued")  # 记录的 state
SMOKE_SRC = ("smoke", "real", "adopted")  # 记录的 src：冒烟任务、真任务训完、taskq smoke adopt
SMOKE_NICE = -1000         # 冒烟任务的档位（排在最前）
ALERTS["smoke"] = ("machine", "kind")  # 冒烟没过（kind = 种类 id）


def is_smoke(key):
    return str(key or "").startswith(SMOKE_PREFIX)
ENV["TASKQ_STALL_IDLE"] = ("test", "代替 STALL_IDLE")
ENV_BUILD = "env_build"    # hub 上建环境的任务的步骤名（任务记录、pueue 标签 env_build:<机器>:<环境>）
ENV["TASKQ_RUN_BEAT"] = ("test", "代替 RUN_BEAT")
ENV["TASKQ_LEDGER"] = ("test", "台账文件的位置（代替 <状态文件夹>/ledger.db）")

# ---------------- 环境的锁（sites/envlock.py，11/5） ----------------
WHEELHOUSE = "wheelhouse"      # 状态文件夹下：锁里的每个文件 wheelhouse/<sha256 前 2 位>/<sha256>
LOCK_SUFFIX = ".lock.json"     # 环境描述旁边的锁 <名>.<变体>.lock.json
LOCK_FORMAT = 1
LOCK_TOP = ("format", "name", "variant", "spec_sha256", "platform", "base", "conda", "pip", "tools", "created")
LOCK_CONDA = ("name", "version", "build", "channel", "subdir", "url", "fn", "size", "md5", "sha256")
LOCK_PIP = ("name", "version", "url", "fn", "size", "sha256", "no_deps")  # 本地建的纯 Python wheel：url 为 null，built_from 是源码包的网址
LOCK_UNHASHED = ("created",)   # 算哈希时去掉的键（描述的哈希、base 的 lock_sha256）


def wheel(h):
    """sha256 → 状态文件夹下的相对路径。"""
    return f"{WHEELHOUSE}/{h[:2]}/{h}"


# ---------------- 一个任务的经过（taskq log，queue/tasklog.py，11/5） ----------------
# 分段日志里每段开头那一行（run_task.sh 写）：作业_段 @ 主机，任务=key，第 N 段，环境 E，代码 C，开始时刻
LOG_HEAD = r"作业 (\S+) @ ([^，]+)，任务=([^，]*)，第 (\d+) 段，环境 ([^，]*)，代码 ([^，]*)，(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
SCHED_LOGS = ("refill", "relay", "feed", "advance")  # hub 上调度写的日志（cfg.log 的名字）
TASKLOG_TOP = ("version", "key", "data", "attempts", "events", "records")
TASKLOG_ATTEMPT = ("file", "path", "job", "machine", "host", "part", "env", "code", "start", "end", "last", "state", "exit", "note")
TASKLOG_EVENT = ("log", "time", "text")  # records：hub 的任务记录（tasks.json）里这个 key 的条目，原样
TASKLOG_STATES = dict(
    done="训完",
    not_done="命令正常结束，没训完",
    failed="命令出错（exit 是退出码）",
    unreadable="读不了盘，判断不了训完没有",
    not_started="没开训（note 是原因）",
    gpu_busy="卡被别人占着，等不到",
    stalled="被 worker 当作卡住停掉",
    running="日志 FRESH_LOG 内更新过，还在跑",
    ended="没有结束的记录（被停掉、交还或作业结束）",
)

# ---------------- GPU 使用记录的汇总（taskq gpu usage，monitor/gpu_usage.py，11/5） ----------------
GPU_USAGE_TOP = ("version", "dir", "me", "days", "from", "to", "hours", "spans", "totals")  # hours：{weekday|weekend: [GPU_USAGE_HOUR]}
GPU_USAGE_HOUR = ("gpu", "hour", "n", "other", "me", "idle")  # n：这个钟点的记录数；other、me、idle：其中别人在用、只有自己、空着的个数
GPU_USAGE_SPAN = ("gpu", "user", "start", "end", "hours", "max_mem_mib", "programs")  # 别人的每段使用
GPU_USAGE_TOTAL = ("gpu", "user", "hours")


# ---------------- hub database and event log (queue/db.py, queue/events.py) ----------------
DB = "taskq.db"  # under the state dir
EVENT_FIELDS = ("id", "time", "t", "source", "kind", "level", "key", "site", "msg", "data")  # taskq events --json, one object per line
# source → old text log name (cfg.log, or "health" = health.log); these still get their lines appended until retired
LEGACY_LOGS = {"relay": "relay", "feed": "feed", "advance": "advance", "shift": "refill", "ledger": "ledger", "health": "health"}
PROGRESS_TOP = ("version", "rows")  # taskq progress --json
PROGRESS_FIELDS = ("time", "t", "key", "state", "progress", "site", "data")  # one row per change of state, progress or site
PROBLEMS_TOP = ("version", "checked", "problems")  # taskq problems --json; each problem: OVERVIEW_PROBLEM


# ---------------- 给 bash ----------------
def sh_vars():
    out = {"TQ_SAFE": SAFE, "TQ_STAGE_PREFIX": STAGE_PREFIX, "TQ_STAGE_DIR_PREFIX": STAGE_DIR_PREFIX, "TQ_GPU_PAD_BASE": GPU_PAD_BASE, "TQ_TASK_LOGS": TASK_LOGS,
           "TQ_WORKER_LOGS": WORKER_LOGS, "TQ_STALL_LINE": STALL_LINE, "TQ_DONE_LINE": DONE_LINE, "TQ_JSON_MARK": JSON_MARK,
           "TQ_INCOMPLETE": INCOMPLETE, "TQ_STALE_BEAT": STALE_BEAT, "TQ_FRESH_LOG": FRESH_LOG, "TQ_BEAT": BEAT,
           "TQ_GPU_LOG_DIR": GPU_LOG_DIR, "TQ_GPU_LOG_FILE": GPU_LOG_FILE, "TQ_GPU_LOG_PID": GPU_LOG_PID,
           "TQ_BUNDLE_CORE": BUNDLE_CORE, "TQ_RELAY_INBOX": RELAY_INBOX}
    out.update({f"TQ_GPU_PAD_{x[1:].upper()}": x for x in GPU_PAD_FILES})  # TQ_GPU_PAD_MIB、_READY、_START
    out["TQ_LOG_PART"] = LOG_PARTS[0]
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
