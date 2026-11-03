"""Slurm 类机器（我的 DICC、SITES 里 scheduler="slurm" 的执行机）的适配器（2026-10-31）：读各 QOS 的作业数、按 QOS 分配、交 worker 作业。
和 pueue.py 对应：读状态 → 调用方算交几个（scripts/queue/refill.py 的 topup_plan）→ start_workers 交。
交的是那台的 worker.sbatch，它转交 worker_start.sh（开显存填充 gpu_pad，gpu-a100 要求 >= 40GB）。
run：fn(命令) → (退出码, 输出)，由调用方绑好机器（测试替换）。"""
import os
import secrets
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "scripts", "queue"))
import cfg  # noqa: E402
cfg.use_config()
import policy as P  # noqa: E402

SUBMIT_CAP = 10  # 读不到 QOS 的排队上限时按这个算
FILES = {"gpu": "worker.sbatch", "cpu": "cpu_worker.sbatch"}  # worker 的种类 → sbatch 文件；认 worker 按 squeue 的 %o（交的脚本）

# 作业名每次随机取（11/2 定：各账号、各作业的名字都不一样），不靠名字认作业；每台按机器表的 job_names 用自己的一套（11/3 定）
_W1 = ("amber azure bold brisk calm clear coral crisp dawn deep dry eager early fair fast fern firm fresh gentle glad gold "
       "grand green grey hazy keen kind late light lime lively lone loyal lucky mellow merry mild misty modest neat noble "
       "pale plain polar proud quick quiet rapid rare ready red rosy round royal ruby rustic sandy sharp shy silent silver "
       "simple sleek slow smooth snowy soft solar solid steady still stout sunny swift tall tidy topaz true vast warm wild wise").split()
_W2 = ("acorn alder anchor apple arch aspen badger basin beacon birch bison bloom brook cabin canyon cedar cliff clover comet "
       "coral crane creek delta dune eagle ember falcon fern finch fjord forest fox garnet glade grove harbor hawk heron hill "
       "island ivy jade lagoon lake lark laurel lynx maple marsh meadow mesa moose moss oak orchid otter owl pebble pine plover "
       "pond prairie quail raven reef ridge river robin sage shore sparrow spruce stone summit swan thicket tide trail tulip "
       "valley willow wren yew").split()


_STARS = ("acrux adhara algol alnair altair antares arcturus atlas bellatrix betelgeuse canopus capella castor deneb diphda "
          "electra enif fomalhaut gacrux hadar hamal izar kochab lyra merak mimosa mira mizar nunki orion pollux procyon rigel "
          "sabik sadr saiph schedar shaula sirius spica suhail thuban vega wezen zosma").split()
STYLES = {
    "pair": lambda: f"{secrets.choice(_W1)}-{secrets.choice(_W2)}",  # <形容词>-<名词>，如 late-plover
    "star": lambda: f"{secrets.choice(_STARS)}{secrets.randbelow(90) + 10}",  # <星名><两位数>，如 vega27
}


def jobname(style=None):
    """一个随机的作业名，按 style（STYLES 的键；没有 → pair）。"""
    return STYLES[style or "pair"]()


def kind_of(command):
    """squeue 的 %o（交的脚本的路径）→ worker 的种类（FILES）；不是 worker（传输作业、手动的作业）→ None。"""
    base = (command or "").strip().rsplit("/", 1)[-1]
    return next((k for k, f in FILES.items() if base == f), None)


def probe(name):
    """每个 GPU QOS 一行"QOS|名字|排队上限|我们排着和在跑的段数|其中还在排（没开始）的段数"；交 CPU worker 时再加"CPU|我们的 CPU worker 作业数"、
    "ALL|CPU worker 那个 QOS 的提交上限|我们所有的作业数"（DICC 的分区都挂 normal，所有作业都占它的名额）；
    有一条命令出错就整个出错（不按 0 算）。QOS 来自 envs/policy.py 的 SLURM；CPU worker 按交的脚本（FILES["cpu"]）认。"""
    conf = P.SLURM[name]
    out = "set -e; for q in " + " ".join(conf["gpu"]) + "; do lim=$(sacctmgr -nP show qos $q format=maxsubmitpu); " \
        "ids=$(squeue -u $USER -h -r -q $q -o %i); pd=$(squeue -u $USER -h -r -q $q -t PD -o %i); " \
        "echo \"QOS|$q|$lim|$(printf '%s' \"$ids\" | grep -c . || true)|$(printf '%s' \"$pd\" | grep -c . || true)\"; done"
    if conf.get("cpu"):
        out += "; cmds=$(squeue -u $USER -h -r -o %o); " \
            f"echo \"CPU|$(printf '%s\\n' \"$cmds\" | grep -cE '(^|/){FILES['cpu'].replace('.', '[.]')}$' || true)\"; " \
            f"lim=$(sacctmgr -nP show qos {conf['cpu'][0]} format=maxsubmitpu); " \
            "ids=$(squeue -u $USER -h -r -o %i); echo \"ALL|$lim|$(printf '%s' \"$ids\" | grep -c . || true)\""
    return out



def busy_cmd():
    """打印我们排着和在跑的作业的工作目录（squeue 的 %Z），一行一个；读不到时出错。
    Slurm 在交作业时记下工作目录（交的那个代码包里），作业开始时要进去开日志，所以开始前不能删。"""
    return "squeue -u $USER -h -r -o %Z"

def parse(out):
    """probe 的输出 → dict(counts={QOS: (排队上限或 None, 现有段数, 其中还在排的段数)}, cpu=CPU worker 作业数或 None,
    free=还剩几个提交名额或 None)；读不出数的 QOS 行不算（那个 QOS 当读不到）。"""
    counts, cpu, free = {}, None, None
    for ln in out.splitlines():
        p = ln.split("|")
        if len(p) == 5 and p[0] == "QOS" and p[3].isdigit() and p[4].isdigit():
            counts[p[1]] = (int(p[2]) if p[2].isdigit() else None, int(p[3]), int(p[4]))
        elif len(p) == 2 and p[0] == "CPU" and p[1].isdigit():
            cpu = int(p[1])
        elif len(p) == 3 and p[0] == "ALL" and p[2].isdigit():
            free = max(0, (int(p[1]) if p[1].isdigit() else SUBMIT_CAP) - int(p[2]))
    return dict(counts=counts, cpu=cpu, free=free)


def status(name, run):
    """→ parse 的结果；读不到 → None。"""
    rc, out = run(probe(name))
    return None if rc else parse(out)


def squeue(text):
    """`squeue -h -o '%i|%j|%T|%M|%q|%R'` 的输出 → [[号, 名字, 状态, 已跑, QOS, 原因], …]。"""
    return [ln.split("|") for ln in text.splitlines() if ln.strip()]


def distribute(qos, counts, room, file):
    """要再交 room 个时按 QOS 的顺序、各自的上限（减去留给本人的）分 → [(QOS, 每段时限, 交几个, sbatch 文件)]。"""
    out = []
    for q, v in qos.items():
        t, keep = (v, 0) if isinstance(v, str) else v
        lim, n = counts[q][0], counts[q][1]
        k = min((lim or SUBMIT_CAP) - keep - n, room)
        if k > 0:
            out.append((q, t, k, file))
            room -= k
    return out


def start_workers(cwd, subs, run, style=None):
    """在 cwd（那边放 sbatch 文件的目录，shell 写法）交 subs（每个作业一个随机名字，按 style）→ [dict(qos, time, n=要交几个, file, ids, err)]；一个 QOS 出错就停这个 QOS。"""
    res = []
    for q, t, n, f in subs:
        ids, err = [], None
        for _ in range(n):
            rc, out = run(f"cd {cwd} && sbatch --parsable -J {jobname(style)} --qos={q} --time={t} {f}")
            if rc:
                err = out[-200:]
                break
            ids.append(out.strip().splitlines()[-1])
        res.append(dict(qos=q, time=t, n=n, file=f, ids=ids, err=err))
    return res
