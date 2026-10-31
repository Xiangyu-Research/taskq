"""手机推送的白话说法（2026-10-21）：任务 key、pueue 标签 → 实验名；健康检查的问题 → 出了什么事、影响什么。
health.py（问题推送）和 report.py（进度推送）用；登记表"需要处理"里仍是 health.py 原来的详细说明。
"""
import glob
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "envs"))
sys.path.insert(0, os.path.join(REPO, "scripts", "queue"))
import machines as M  # noqa: E402
import experiments as E  # noqa: E402

STEP = {"eval": "评测", "oof": "五折汇总", "paired": "和 T0 的配对比较", "ens": "五折集成评测", "infer": "推理",
        "lprob": "病灶概率汇总",
        "fetch": "拉回 box", "archive": "存档", "train": "训练",
        "compare_all": "对比表"}
QUIET = {"infer", "fetch", "archive", "train", "wait"}  # 中间步骤：进度报告里不列
SLURM = {"FAILED": "失败", "OUT_OF_MEMORY": "内存不够", "NODE_FAIL": "节点故障", "BOOT_FAIL": "启动失败", "DEADLINE": "超过截止时间"}
_tasks = _plan = None


def exp_name(e):
    return f"{e['exp']} {e['label']}" + (f" {e['model']}" if e.get("model") not in (None, "nnU-Net") else "")


def fold(key):
    """任务 key → "T15 MEN 对齐 470 例 f3"；不在实验表里的原样返回。"""
    global _tasks
    try:
        if _tasks is None:
            _tasks = E.tasks()
        t = _tasks[key]
        return f"{exp_name(E.experiment(t['exp']))} f{t['fold']}"
    except Exception:  # noqa: BLE001
        return key


def plan_items():
    """各 RQ 分析计划表里的编号 → 问题（"C3" → "混合训练是否改变对水肿 / 合成病灶的依赖？"）。"""
    out = {}
    for f in sorted(glob.glob(os.path.join(REPO, "research", "*", "分析计划.md"))):
        for ln in open(f, encoding="utf-8"):
            m = re.match(r"\| ([A-Z]\d+) \| ([^|]+?) \|", ln)
            if m:
                out.setdefault(m.group(1), m.group(2))
    return out


def label(lab):
    """pueue 标签 → (实验名, 做了什么, 第几折或 None)；中间步骤 → None。分析计划里的分析 → ("分析", "C3 问题", None)，
    别的一次性任务 → ("其他", 标签, None)。"""
    global _plan
    step, _, who = lab.partition(":")
    if not who:
        return ("所有实验", "对比表", None) if step == "compare_all" else ("其他", lab, None)
    if step in QUIET:
        return None
    if step not in STEP:
        m = re.match(r"([A-Z]\d+)(_|$)", who)
        if _plan is None:
            _plan = plan_items()
        return ("分析", f"{m.group(1)} {_plan[m.group(1)]}", None) if m and m.group(1) in _plan else ("其他", lab, None)
    m = re.fullmatch(r"(.+)_f(\d+)", who)
    eid, f = (m.group(1), m.group(2)) if m and E.experiment(m.group(1)) else (who, None)
    e = E.experiment(eid)
    if not e:
        return ("其他", lab, None)
    return exp_name(e), STEP[step], f


def task(lab):
    """pueue 标签 → 一句话（问题推送用，中间步骤也说）。"""
    step, _, who = lab.partition(":")
    if step == "train":
        return f"{fold(who)} 的训练"
    if step in STEP and step in QUIET:
        m = re.fullmatch(r"(.+)_f(\d+)", who)
        e = E.experiment(m.group(1)) if m else None
        return (f"{exp_name(e)} f{m.group(2)}" if e else who) + " 的" + STEP[step]
    r = label(lab)
    return f"{r[0]}{' f' + r[2] if r[2] else ''} 的{r[1]}" if r else lab


def _who(key):
    if "/" in key:
        return fold(key)
    e = E.experiment(key)
    return exp_name(e) if e else ("所有实验" if key == "all" else key)


def problem(pid, p):
    """健康检查的问题编号 → 推送的白话（出了什么事 + 影响）；没对上的用原来的说明。"""
    msg, part = p["msg"], pid.split(":")
    kind = part[0]
    L = lambda n: M.machine(n)["label"] if n in M.MACHINES or n in M.RELAY else n  # noqa: E731
    num = lambda pat: (re.search(pat, msg) or [None, "?"])[1]  # noqa: E731
    try:
        if kind == "down":
            return f"{L(part[1])} 读不到状态了（网络断了，或那边的存储挂住），看不到上面的训练还在不在跑。"
        if kind == "stuck":
            m = re.search(r"train:(\S+?)：", msg) or re.search(r"（(\S+/\S+)）", msg)
            what = fold(m.group(1)) if m else (fold(part[2]) if "/" in part[2] else "一个训练")
            return f"{L(part[1])} 上 {what} 90 分钟没有进展，可能卡住了，GPU 白占着。"
        if kind == "tqfail":
            again = "，不会再自动重试" if "不再领" in msg else "，会自动重试"
            step = {"infer": "推理", "eval": "评测"}.get(pid[7:].split("/")[0], "训练")
            return f"{fold(pid[7:])} {step}失败了 {num(r'失败 (\d+) 次')} 次{again}。"
        if kind == "tqblocked":
            step = {"infer": "推理", "eval": "评测"}.get(pid[10:].split("/")[0], "任务")
            return f"{fold(pid[10:])} 的{step}在等的前一步失败了（或不在队列里），不会自己开始。"
        if kind == "tqpause":
            return f"{L(part[1])} 3 小时内训练失败太多次，暂停领新任务；排着的作业会白白用掉。"
        if kind == "tqidle":
            return f"{L(part[1])} 有作业占着 GPU 却没在训练。"
        if kind == "tqorphan":
            return f"{L(part[1])} 还有 {num(r'还有 (\d+) 个')} 折没训，但已经没有作业在排队，训练会停下来。"
        if kind == "taskq":
            return f"读不到 {L(part[1])} 的任务队列，看不到那边的训练情况。"
        if kind == "pueued":
            return f"{L(part[1])} 的任务队列程序没有响应，上面的训练和分析可能都停了。"
        if kind == "pueue":
            m = re.search(r"任务 \d+ (\S+) ", msg)
            return f"{L(part[1])} 上 {task(m.group(1)) if m else '一个任务'}出错停了。"
        if kind == "idle":
            gpu = M.MACHINES[part[1]]["groups"][part[2]][1]
            return f"{L(part[1])} {gpu} 号卡空着，没有训练。"
        if kind == "fetch":
            return f"{fold(pid[6:])} 训完 1.5 小时了还没拉回 box，后面的评测会推迟。"
        if kind == "orphan":
            return f"box 在等 {fold(pid[7:])} 训完，但已经没有地方在训它，会一直等下去。"
        if kind == "dup":
            if p.get("level") == "urgent":
                return f"{fold(pid[4:])} 同时在两处训练，存档会互相覆盖，要尽快停掉一边。"
            return f"{fold(pid[4:])} 在两处排着，要取消一边，不然会重复训练。"
        if kind == "disk":
            return f"{msg}，盘满了训练和存档会失败。"
        if kind == "gpulog":
            return f"{L(part[1])} 的 GPU 使用记录停了（不影响训练）。"
        if kind == "code":
            return f"{L(part[1])} 2 小时没拉到最新代码，那里跑的是旧版本的脚本。"
        if kind == "boot":
            return f"{L(part[1])} {pid.split(':', 2)[2][5:16]} 重启过，被打断的任务已自动放回队列，要确认训练接上了。"
        if kind == "board":
            return f"看板有新结果，要确认后才发布（{num(r'（(\d+) 个文件')} 个文件）。"
        if kind in ("refill", "advance") and part[1] == "error":
            return "自动调度脚本出错了：" + ("DICC 不会补交作业，lab 上用完的数据不会清理。" if kind == "refill" else "训完的折不会自动推理、评测。")
        if kind == "site":  # 执行机（feed.py、relay.py，10/31）：site:<机器>:<类>[:<key 或数据集>]
            site, sub, key = L(part[1]), part[2], ":".join(part[3:])
            if sub == "down":
                return f"{site} 读不到了，看不到分给它的训练还在不在跑。"
            if sub == "low":
                return f"{site} 排着的训练不够（数据还没传到），卡可能会空着。"
            if sub == "sync":
                return f"给 {site} 传训练数据出错了，要这份数据的训练开不了。"
            if sub == "worker":
                return f"{site} 上的 worker 出错停了，那边的卡会空着，要看日志后重开。"
            if sub == "fail" and not key:
                return f"{site} 3 小时内训练失败太多次，暂停领新任务。"
            if sub == "fail":
                if "有进度" in msg:
                    return f"{site} 上 {fold(key)} 训练失败了，有进度，要决定接着训还是放弃。"
                return f"{site} 上 {fold(key)} 的任务出了问题，不会开始训练。"
            if sub == "orphan":
                if "认不出" in msg:
                    return f"{site} 的队列里有一个认不出是哪一折的任务。"
                return f"{site} 在训 {fold(key)}，但总队列里没分给它，可能重复训练。"
        if kind == "topup":
            return f"给 {L(part[1])} 补交作业出错，那边排队的作业会越来越少。"
        if kind == "advance":
            return f"{_who(':'.join(part[2:]))} 的{STEP.get(part[1], part[1])}出错停了，这个结果会推迟。"
        if len(part) == 2 and part[0] in M.MACHINES:  # Slurm 作业异常结束
            st = num(r" (\S+)（")
            return f"{L(part[0])} 作业 {part[1]} 异常结束（{SLURM.get(st.split()[0], st)}）。"
    except Exception:  # noqa: BLE001
        pass
    return msg
