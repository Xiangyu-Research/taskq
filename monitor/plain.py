"""手机推送的白话说法：任务 key、pueue 标签 → 实验名；健康检查的问题 → 出了什么事、影响什么。
health.py（问题推送）和 overview.py 用；问题清单里是 health.py 的详细说明。
"""
import os
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg  # noqa: E402
import proto  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import experiments as E  # noqa: E402

# 步骤的中文名：实验表的步骤定义里有 zh 时用它（step_name），没有时查 STEPS（核心自己排的步骤 + 插件 labels.py 的），再没有用步骤名本身
STEPS = {"train": "训练", "archive": "存档", "relay_fetch": "结果拉回", "relay_stage": "数据传送", "relay_archive": "存档",
         proto.ENV_BUILD: "建环境"}
QUIET = ["train", "archive"]  # 中间步骤：进度报告里不列（插件的接在后面）
PLAN_TITLES = None  # 插件的函数：() → {分析编号: 标题}
cfg.extend(globals(), "labels")
_tasks = _plan = _steps = _model = None


def table_steps():
    """实验表里的步骤（每个任务的、实验一级的、最后的）→ {名字: 定义}。"""
    global _steps
    if _steps is None:
        exps, final = E.load()
        _steps = {}
        for st in [x for e in exps for x in e["after"] + e["group_after"]] + final:
            _steps.setdefault(st["name"], st)
    return _steps


def step_name(step):
    """步骤名 → 中文名（labels 插件的 STEPS；没写的用步骤名本身）。"""
    return STEPS.get(step, step)


def known_step(step):
    try:
        return step in STEPS or step in table_steps()
    except Exception:  # noqa: BLE001
        return step in STEPS


def final_step(step):
    try:
        return any(st["name"] == step for st in E.load()[1])
    except Exception:  # noqa: BLE001
        return False


def usual_model():
    """实验表里最多的模型（说实验名时不提它）；读不了实验表、没有模型 → None。"""
    global _model
    if _model is None:
        try:
            n = Counter(e["model"] for e in E.load()[0] if e.get("model"))
        except Exception:  # noqa: BLE001
            n = Counter()
        _model = [n.most_common(1)[0][0] if n else None]
    return _model[0]


def exp_name(e):
    m = e.get("model")
    return f"{e['exp']} {e['label']}" + (f" {m}" if m and m != usual_model() else "")


def key_name(key):
    """任务 key → 实验名 + 简称（如 "exp1 小数据 s0"）；不在实验表里的原样返回。"""
    global _tasks
    try:
        if _tasks is None:
            _tasks = E.tasks()
        t = _tasks[key]
        return " ".join(x for x in (exp_name(E.experiment(t["exp"])), E.short(t)) if x)
    except Exception:  # noqa: BLE001
        return key


def plan_items():
    """分析编号 → 标题（插件 labels.py 的 PLAN_TITLES；没有插件 → {}）。"""
    return dict(PLAN_TITLES() or {}) if PLAN_TITLES else {}


def plan_of(who):
    """标签的后半是 <编号> 或 <编号>_… 且编号有标题 → "编号 标题"；否则 None。"""
    global _plan
    if _plan is None:
        _plan = plan_items()
    k = next((k for k in _plan if who == k or who.startswith(k + "_")), None)
    return f"{k} {_plan[k]}" if k else None


def label(lab):
    """pueue 标签 → (实验名, 做了什么, 任务的简称（E.short，如 "f1"）或 None)；中间步骤 → None。实验表最后一起做的步骤（没有后半）
    → ("所有实验", 步骤, None)；有标题的分析 → ("分析", "编号 标题", None)，别的一次性任务 → ("其他", 标签, None)。"""
    step, _, who = lab.partition(":")
    if not who:
        return ("所有实验", step_name(step), None) if final_step(step) else ("其他", lab, None)
    if step in QUIET:
        return None
    if not known_step(step):
        what = plan_of(who)
        return ("分析", what, None) if what else ("其他", lab, None)
    e, t = E.parse_label(who)
    if not e:
        return ("其他", lab, None)
    return exp_name(e), step_name(step), (E.short(t) or None) if t else None


def task(lab):
    """pueue 标签 → 一句话（问题推送用，中间步骤也说）。"""
    step, _, who = lab.partition(":")
    if step == "train":
        return f"{key_name(who)} 的训练"
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
        return f"{key_name(who)} 的{step_name(step)}" if key_name(who) != who else f"{step_name(step)}任务 {who}"
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
        return key_name(key)
    e = E.experiment(key)
    return exp_name(e) if e else ("所有实验" if key == "all" else key)


def problem(pid, p):
    """健康检查的问题编号 → 推送的白话（出了什么事 + 影响）。要用到的事实（个数、有无进度…）从 p["data"] 取（health.py、relay.py
    报问题时给的）；该有 data 却没有的、没对上的，用 p["msg"] 的说明。"""
    msg, part, d = p["msg"], pid.split(":"), p.get("data") or {}
    kind = part[0]
    L = lambda n: M.MACHINES[n]["label"] if n in M.MACHINES else n  # noqa: E731
    try:
        if kind == "down":
            return f"{L(part[1])} 读不到状态了（网络断了，或那边的存储挂住），看不到上面的训练还在不在跑。"
        if kind == "unread" and "n" in d:
            return f"{L(part[1])} 上 {d['n']} 个任务的文件读不动（多半是那边的存储挂住），这些任务先不分出去，别的照常。"
        if kind == "tqblocked":
            key = d.get("key") or pid[len("tqblocked:"):]
            return f"{key_name(key)} 能去的执行机都试过、都训不了（跳过了），不会自己开始。"
        if kind == "tqroom":
            key = d.get("key") or pid[len("tqroom:"):]
            return f"{key_name(key)} 能去的执行机上盘都不够放，不会开始，要腾出地方。"
        if kind == "taskq":
            return f"读不到 {L(part[1])} 的任务队列，看不到那边的训练情况。"
        if kind == "collect":
            return f"在 {L(part[1])} 上收集训练状态出错，这一轮看不到结果的情况、不交还任务。"
        if kind == "pueued":
            return f"{L(part[1])} 的任务队列程序没有响应，上面的训练和分析可能都停了。"
        if kind == "pueue" and "label" in d:
            return f"{_cat(L(part[1]), '上的', task(d['label']), '出错停了。')}"
        if kind == "disk":
            what = f"{L(part[1])} {d['path']} 只剩 {d['free_gb']:.0f} GB"
            return f"{what}，盘满了训练和存档会失败。"
        if kind == "gpulog":
            return f"{L(part[1])} 的 GPU 使用记录停了（不影响训练）。"
        if kind == "boot":
            return f"{L(part[1])} {(d.get('time') or pid.split(':', 2)[2])[5:16]} 重启过，被打断的任务已自动放回队列，要确认训练接上了。"
        if kind == "advance" and part[1] == "policy":
            return f"训完以后的步骤停了，一个都不排：{msg.split('。训完以后')[0]}。改好、提交推送后下一次检查自动恢复。"
        if kind == "advance" and part[1] == "error":
            return "自动调度脚本出错了：训完的任务不会自动排后续步骤。"
        if kind == "site":  # 执行机（relay.py）：site:<机器>:<类>[:<key>]
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
                    return f"{site} 上 {key_name(key)} 训练失败了，有进度，要决定接着训还是放弃。"
                return f"{site} 上 {key_name(key)} 的任务出了问题，不会开始训练。"
            if sub == "orphan" and "unknown" in d:
                if d["unknown"]:
                    return f"{site} 的队列里有一个认不出来的任务。"
                return f"{site} 在训 {key_name(key)}，但总队列里没分给它，可能重复训练。"
        if kind == "relayadd":
            return f"{L(M.hub())} 上没排上 {key_name(d.get('key', ''))} 的{dict(relay_stage='传数据', relay_fetch='拉回', relay_archive='归档', env_build='建环境').get(d.get('step'), '下一步')}，{L(part[1])} 那边会等着，下次检查重试。"
        if kind == "bundlegate":
            return f"{L(part[1])} 在用的代码包跑不了 {key_name(d.get('key', ''))} 的任务说明，先不送过去，那边换了新包后自动再送。"
        if kind == "smoke":
            k = (d.get("kind") or "").split("|")
            what = " ".join([k[0]] + [x.split("=", 1)[1] for x in k[1:] if "=" in x and not x.startswith("env=")])
            return f"{L(part[1])} 上 {what} 冒烟没过：{(d.get('why') or '').splitlines()[0][:120] if d.get('why') else '原因看 taskq smoke status'}，这种任务先不送过去。"
        if kind == "topup":
            return f"给 {L(part[1])} 补交作业出错，那边排队的作业会越来越少。"
        if kind == "advance":
            return f"{_who(':'.join(part[2:]))} 的{step_name(part[1])}出错停了，这个结果会推迟。"
    except Exception:  # noqa: BLE001
        pass
    return msg
