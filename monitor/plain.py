"""手机推送的白话说法（2026-10-21）：任务 key、pueue 标签 → 实验名；健康检查的问题 → 出了什么事、影响什么。
health.py（问题推送）和 report.py（进度推送）用；登记表"需要处理"里仍是 health.py 原来的详细说明。
"""
import glob
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg  # noqa: E402
import proto  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import experiments as E  # noqa: E402

# 步骤的中文名：实验表的步骤定义里有 zh 时用它（step_name），没有时查这张表，再没有用步骤名本身
STEP = {"eval": "评测", "oof": "五折汇总", "paired": "和 T0 的配对比较", "ens": "五折集成评测", "infer": "推理",
        "lprob": "病灶概率汇总", "prune": "删单折概率图", "p1": "P1 水肿反事实", "p2": "P2 合成病灶插入", "p2cmp": "P2 和 T0 的比较",
        "t18cmp": "T18 的后续比较", "j1b3": "J1B3 覆盖梯度回归",
        "archive": "存档", "train": "训练",
        "compare_all": "对比表", "relay_fetch": "结果拉回", "relay_stage": "数据传送", "relay_archive": "存档", "env_build": "建环境"}
QUIET = {"infer", "archive", "train", "prune"}  # 中间步骤：进度报告里不列
_tasks = _plan = _steps = None


def table_steps():
    """实验表里的步骤（每个任务的、实验一级的、最后的）→ {名字: 定义}。"""
    global _steps
    if _steps is None:
        exps, final = E.load()
        _steps = {}
        for st in [x for e in exps for x in e.get("after", []) + [y for c in e.get("group_after", []) for y in c]] + final:
            _steps.setdefault(st["name"], st)
    return _steps


def step_name(step):
    """步骤名 → 中文名。"""
    try:
        zh = (table_steps().get(step) or {}).get("zh")
    except Exception:  # noqa: BLE001  实验表读不了时照样说白话
        zh = None
    return zh or STEP.get(step, step)


def known_step(step):
    try:
        return step in STEP or step in table_steps()
    except Exception:  # noqa: BLE001
        return step in STEP


def exp_name(e):
    return f"{e['exp']} {e['label']}" + (f" {e['model']}" if e.get("model") not in (None, "nnU-Net") else "")


def fold(key):
    """任务 key → "T15 MEN 对齐 470 例 f3"；不在实验表里的原样返回。"""
    global _tasks
    try:
        if _tasks is None:
            _tasks = E.tasks()
        t = _tasks[key]
        return " ".join(x for x in (exp_name(E.experiment(t["exp"])), E.short(t)) if x)
    except Exception:  # noqa: BLE001
        return key


def plan_items():
    """各 RQ 分析计划表里的编号 → 问题（"C3" → "混合训练是否改变对水肿 / 合成病灶的依赖？"）。"""
    out = {}
    pat = cfg.outputs("plans")
    for f in sorted(glob.glob(os.path.join(REPO, pat))) if pat else []:
        for ln in open(f, encoding="utf-8"):
            m = re.match(r"\| ([A-Z]\d+) \| ([^|]+?) \|", ln)
            if m:
                out.setdefault(m.group(1), m.group(2))
    return out


def label(lab):
    """pueue 标签 → (实验名, 做了什么, 任务的简称（E.short，如 "f1"）或 None)；中间步骤 → None。分析计划里的分析 → ("分析", "C3 问题", None)，
    别的一次性任务 → ("其他", 标签, None)。"""
    global _plan
    step, _, who = lab.partition(":")
    if not who:
        return ("所有实验", "对比表", None) if step == "compare_all" else ("其他", lab, None)
    if step in QUIET:
        return None
    if not known_step(step):
        m = re.match(r"([A-Z]\d+)(_|$)", who)
        if _plan is None:
            _plan = plan_items()
        return ("分析", f"{m.group(1)} {_plan[m.group(1)]}", None) if m and m.group(1) in _plan else ("其他", lab, None)
    e, t = E.parse_label(who)
    if not e:
        return ("其他", lab, None)
    return exp_name(e), step_name(step), (E.short(t) or None) if t else None


def task(lab):
    """pueue 标签 → 一句话（问题推送用，中间步骤也说）。"""
    step, _, who = lab.partition(":")
    if step == "train":
        return f"{fold(who)} 的训练"
    if step in QUIET:
        e, t = E.parse_label(who)
        if not e:
            return f"{step_name(step)}任务 {who}"
        return " ".join(x for x in (exp_name(e), E.short(t) if t else "") if x) + " 的" + step_name(step)
    r = label(lab)
    if r and r[0] == "分析":
        return f"分析 {r[1]}"
    if r and r[0] == "所有实验":
        return f"{r[1]}（所有实验）"
    if r and r[0] != "其他":
        return f"{r[0]}{' ' + r[2] if r[2] else ''} 的{r[1]}"
    if lab.startswith(proto.WORKER_LABEL):
        return f"worker {who}"
    if who and known_step(step):
        return f"{fold(who)} 的{step_name(step)}" if fold(who) != who else f"{step_name(step)}任务 {who}"
    return f"一次性任务 {lab}"


def _cat(*parts):
    """拼中文句子：中文和英文、数字相邻处加一个空格。"""
    out = ""
    for x in parts:
        if out and x and (out[-1].isascii() and out[-1].isalnum() or x[0].isascii() and x[0].isalnum()) and out[-1] != " " and x[0] != " ":
            out += " "
        out += x
    return out


def _who(key):
    if "/" in key:
        return fold(key)
    e = E.experiment(key)
    return exp_name(e) if e else ("所有实验" if key == "all" else key)


def problem(pid, p):
    """健康检查的问题编号 → 推送的白话（出了什么事 + 影响）。要用到的事实（个数、有无进度…）从 p["data"] 取（health.py、relay.py
    报问题时给的）；该有 data 却没有的（旧状态文件里的）、没对上的，用原来的说明。"""
    msg, part, d = p["msg"], pid.split(":"), p.get("data") or {}
    kind = part[0]
    L = lambda n: M.machine(n)["label"] if n in M.MACHINES or n in M.RELAY else n  # noqa: E731
    try:
        if kind == "down":
            return f"{L(part[1])} 读不到状态了（网络断了，或那边的存储挂住），看不到上面的训练还在不在跑。"
        if kind == "unread" and "n" in d:
            return f"{L(part[1])} 上 {d['n']} 个任务的文件读不动（多半是那边的存储挂住），这些任务先不分出去，别的照常。"
        if kind == "stuck" and d:
            what = fold(d["key"]) if d.get("key") else "一个训练"
            return f"{L(part[1])} 上 {what} 90 分钟没有进展，可能卡住了，GPU 白占着。"
        if kind == "tqblocked":
            key = d.get("key") or pid[len("tqblocked:"):]
            step = {"infer": "推理", "eval": "评测"}.get(key.split("/")[0], "任务")
            return f"{fold(key)} 的{step}在等的前一步失败了（或不在队列里），不会自己开始。"
        if kind == "taskq":
            return f"读不到 {L(part[1])} 的任务队列，看不到那边的训练情况。"
        if kind == "pueued":
            return f"{L(part[1])} 的任务队列程序没有响应，上面的训练和分析可能都停了。"
        if kind == "pueue" and "label" in d:
            return f"{_cat(L(part[1]), '上的', task(d['label']), '出错停了。')}"
        if kind == "idle":
            gpu = M.MACHINES[part[1]]["groups"][part[2]][1]
            return f"{L(part[1])} {gpu} 号卡空着，没有训练。"
        if kind == "dup":
            key = d.get("key") or pid[len("dup:"):]
            if p.get("level") == "urgent":
                return f"{fold(key)} 同时在两处训练，存档会互相覆盖，要尽快停掉一边。"
            return f"{fold(key)} 在两处排着，要取消一边，不然会重复训练。"
        if kind == "disk":
            what = f"{L(part[1])} {d['path']} 只剩 {d['free_gb']:.0f} GB" if "free_gb" in d else msg
            return f"{what}，盘满了训练和存档会失败。"
        if kind == "gpulog":
            return f"{L(part[1])} 的 GPU 使用记录停了（不影响训练）。"
        if kind == "boot":
            return f"{L(part[1])} {(d.get('time') or pid.split(':', 2)[2])[5:16]} 重启过，被打断的任务已自动放回队列，要确认训练接上了。"
        if kind == "board" and "n" in d:
            return f"看板有新结果，要确认后才发布（{d['n']} 个文件）。"
        if kind == "advance" and part[1] == "policy":
            return f"训完以后的步骤停了，一个都不排：{msg.split('。训完以后')[0]}。改好、提交推送后下一次检查自动恢复。"
        if kind == "advance" and part[1] == "error":
            return "自动调度脚本出错了：训完的任务不会自动推理、评测。"
        if kind == "site":  # 执行机（relay.py，10/31）：site:<机器>:<类>[:<key>]
            site, sub, key = L(part[1]), part[2], ":".join(part[3:])
            if sub == "down":
                return f"{site} 读不到了，看不到分给它的训练还在不在跑。"
            if sub == "env" and d.get("state") == "空间不够":
                return f"{site} 空间不够，环境 {key} 没建，用它的任务不会送过去；空出地方后自动再建。"
            if sub == "env" and key:
                return f"{site} 上的环境 {key} 建不了，用它的任务不会送过去，要看建环境的日志。"
            if sub == "env":
                return f"{site} 的环境自动建出错了。"
            if sub == "worker" and key:
                return f"{site} 上的 {key.upper()} worker 出错停了，那边的 {key.upper()} 任务没人做，要看日志后重开。"
            if sub == "worker":
                return f"{site} 上的 worker 出错停了，那边的卡会空着，要看日志后重开。"
            if sub == "fail" and not key:
                return f"{site} 3 小时内训练失败太多次，暂停领新任务。"
            if sub == "fail" and "progress" in d:
                if d["progress"]:
                    return f"{site} 上 {fold(key)} 训练失败了，有进度，要决定接着训还是放弃。"
                return f"{site} 上 {fold(key)} 的任务出了问题，不会开始训练。"
            if sub == "orphan" and "unknown" in d:
                if d["unknown"]:
                    return f"{site} 的队列里有一个认不出来的任务。"
                return f"{site} 在训 {fold(key)}，但总队列里没分给它，可能重复训练。"
        if kind == "relayadd":
            return f"box 上没排上 {fold(d.get('key', ''))} 的{dict(relay_stage='传数据', relay_fetch='拉回', relay_archive='归档', env_build='建环境').get(d.get('step'), '下一步')}，{L(part[1])} 那边会等着，下次检查重试。"
        if kind == "bundlegate":
            return f"{L(part[1])} 在用的代码包跑不了 {fold(d.get('key', ''))} 的任务说明，先不送过去，那边换了新包后自动再送。"
        if kind == "topup":
            return f"给 {L(part[1])} 补交作业出错，那边排队的作业会越来越少。"
        if kind == "advance":
            return f"{_who(':'.join(part[2:]))} 的{step_name(part[1])}出错停了，这个结果会推迟。"
    except Exception:  # noqa: BLE001
        pass
    return msg
