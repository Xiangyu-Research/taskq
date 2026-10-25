#!/usr/bin/env python3
"""按状态推进训练完以后的事（2026-10-18；10/25 起每一步在哪里做由 envs/policy.py 的 PLACE 定）：box 上的健康检查
（scripts/monitor/health.py，每 30 分钟）调用。每次对每个任务看文件，算出它走到了哪一步，下一步没在做就排上；不提前排链。
步骤和判断都从实验表（research/*/experiments.py，经 scripts/queue/experiments.py 读）和任务说明（scripts/queue/spec.py）来，
这里不认识具体模型。

每个任务（实验表里的，加上队列里不属于任何实验的）：
  fetch    别的机器上训完了（说明里的 done），box 上还没有 → scripts/queue/fetch_task.sh <机器> <key>（wait 组）
  after    实验表里每折的步骤（RQ1：推理 → 评测），按顺序做第一个没做完的
  archive  pueue 训练机（lab）上训完、拉回 box 以后，那边还没归档 → scripts/queue/archive_task.sh <机器> <key>（wait 组：
           备份、核对后删那边的大文件）。那边还在用这一折做别的步骤时等；要进任务队列的步骤等归档做完再加
一个实验所有任务都在 box 上训完以后：group_after 的每一串步骤，按顺序做第一个没做完的（needs = 要每折都做完的步骤）。
最后：FINAL 的步骤（RQ1：compare_all，在 box 上）。有 rerun_if_newer 的，列出的步骤有比它开始时更新的结果、这些步骤（和同一串
里排在它们前面的）都没有在排 / 在跑时重跑。队列里不属于任何实验的任务只拉回、归档。

在哪里做（每折的步骤、group_after 的步骤）：已经在某处做着（box pueue 里有、任务队列里有、remote.json 里记着）就接着那边的；
否则按 PLACE[步骤] 的顺序用第一个现在能用的（写法见 envs/policy.py），都用不了时写进 ~/advance.log。没写的步骤在 box 上：
  box:<组>       box pueue 里排（标签 <步骤>:<实验 id>_f<fold>，实验一级的 <步骤>:<实验 id>）
  queue:<机器>   加进任务队列（taskq.py add --recipe command，参数是步骤 queue 里的，machines = 这台，nice = queue 的 nice）。
                 那边缺的输入是文件就先 offload_io.sh push 过去（wait 组 push:<标签>），缺文件夹就报出来；那边做完后按"队列里
                 不属于任何实验的任务"拉回（fetch_task.sh），拉回后照常按 done 判断
  <机器>:<组>    别的 pueue 机器：缺输入 → box wait 组 push:<标签>（offload_io.sh push：文件夹经隧道、文件用 rsync；那边的盘要留够）
                 → 那边的组里排同样的标签（命令里的 {data}、{python} 换成那边的）→ 那边成功 → box wait 组 pull:<标签>
                 （offload_io.sh pull，按校验和核对）→ box 上判断做完 → 删那边的结果和自动传过去的文件（别的步骤还要用的留着）
  trainer        同上，机器和组是训这一折的：这一折还在那边训（或刚训完、那个组里没有下一折）时排，pueue 按任务号先后跑，训完马上
                 接着做；输入要都在那边（不传），否则用下一个地方。训练没训完就停了、这一步跟着失败的不算，接着训时重排
  那边的步骤记在 ~/.cache/phd/advance/remote.json。要知道每折的输入：命令支持 --stage needs（把 --stage <步骤> 换成 --stage
  needs，打印相对数据目录的输入），或者步骤的 queue 里写了 inputs。

失败过的不自动重排：box pueue 里这一步最后一次成功之后有失败的任务，或者成功跑完了但结果还是不对（rerun_if_newer 的除外），
  就只报出来（健康检查"需要处理"），看过 pueue log、处理好以后 pueue remove 那个任务，下一次就会重排；那边的任务、传输、拉回一样。
在不在跑：box pueue 里的任务，拉回、归档按命令（带源机器）认，其他步骤按命令或标签认。拉回按源机器认：以前预先排的、在等
  另一台机器的拉回任务报出来。这一步的结果还在往 box 上拷（拉回、传输在跑）时不算做完、不清理那边。
envs/policy.py 的 AUTO["advance"] 是 False 时只把"会排什么"写进 ~/advance.log，不真排；HOLD 对上的步骤在 box 上加成暂停的
  （pueue add --stashed），进任务队列的先不加。

    python3 scripts/queue/advance.py           看现在会排什么（不改任何东西）
"""
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime
from fnmatch import fnmatch

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "envs"))
sys.path.insert(0, os.path.join(REPO, "scripts", "monitor"))
sys.path.insert(0, HERE)
import machines as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import experiments as E  # noqa: E402
import refill  # noqa: E402

STATE_DIR = os.path.expanduser("~/.cache/phd/advance")
LOG = os.path.expanduser(os.environ.get("ADVANCE_LOG", "~/advance.log"))  # 环境变量只给测试用
LIVE_ST = refill.LIVE
TRAINERS = [n for n in M.names("pueue") if "train" in M.MACHINES[n]["roles"]]  # 训完要备份、删大文件的机器（lab）
NO_NICE = 10 ** 6
REMOTE_OUT_GB = 15  # 别的机器上做的一步最多写多少（一折的预测 7–12 GB）
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]
NOTES = []  # 这次检查里"为什么没排"的说明（写进 ~/advance.log，不报成问题）


def bt():
    return os.environ.get("ADVANCE_BT") or M.expand(M.MACHINES[M.hub()]["data"])  # 环境变量只给测试用


def python(name=None):
    base, env = M.MACHINES[name or M.hub()]["conda"]
    return f"{M.expand(base) if not name else base}/envs/{env}/bin/python"


def state_path():
    return os.path.join(STATE_DIR, "remote.json")


# ---------------- 在哪里做（envs/policy.py 的 PLACE） ----------------
def targets(step, group):
    """PLACE[步骤] → [dict(text, kind=box|pueue|trainer|queue, machine, group, idle, hours, have)]；没写 → box 的这个组。
    写错了 → ValueError。"""
    out = []
    for text in P.PLACE.get(step) or [f"{M.hub()}:{group}"]:
        w = text.split()
        bad = [x for x in w[1:] if x not in ("idle", "have") and not x.startswith("hours=")]
        if not w or bad:
            raise ValueError(f"envs/policy.py PLACE[{step!r}] 的 {text!r}：不认识的条件 {bad}")
        d = dict(text=text, idle="idle" in w, have="have" in w, hours=next((x[6:] for x in w if x.startswith("hours=")), None),
                 machine=None, group=None)
        a, _, b = w[0].partition(":")
        if w[0] == "trainer":
            d["kind"] = "trainer"
        elif a == "queue" and b in M.MACHINES:
            d.update(kind="queue", machine=b)
        elif a in M.MACHINES and M.MACHINES[a]["scheduler"] == "pueue" and b in M.MACHINES[a]["groups"]:
            d.update(kind="box" if a == M.hub() else "pueue", machine=a, group=b)
        else:
            raise ValueError(f"envs/policy.py PLACE[{step!r}] 的 {text!r}：不认识的地方（box:<组>、<机器>:<组>、trainer、queue:<机器>）")
        if d["hours"]:
            in_hours(d["hours"])
        out.append(d)
    return out


def check_policy(steps):
    """PLACE 里的步骤都认得、每个地方都写对了；不对 → ValueError。"""
    unknown = sorted(set(P.PLACE) - set(steps))
    if unknown:
        raise ValueError(f"envs/policy.py PLACE 里有实验表里没有的步骤 {unknown}")
    for st in P.PLACE:
        targets(st, None)


def in_hours(spec, now=None):
    """'20:00-08:00[,12:00-13:00]' → 现在在不在里面（box 的本地时间，可以跨午夜）。写错了 → ValueError。"""
    now = now or datetime.now()
    m = now.hour * 60 + now.minute
    hit = False
    for x in spec.split(","):
        try:
            a, b = [int(h) * 60 + int(mm) for h, mm in (p.split(":") for p in x.split("-"))]
        except ValueError:
            raise ValueError(f"envs/policy.py 的 hours={spec}：时段写错了（{x}）")
        hit |= (a <= m < b) if a < b else (m >= a or m < b)
    return hit


# ---------------- 认命令 ----------------
def step_of(cmd):
    """pueue 任务的命令 → [(步骤, key)]：拉回（fetch@<源机器>）、归档。其他步骤按命令或标签认（plan() 里）。认不出 → []。"""
    try:
        tok = shlex.split(cmd)
    except ValueError:
        return []
    for i, t in enumerate(tok):
        b, rest = os.path.basename(t), tok[i + 1:]
        if b in ("fetch_task.sh", "archive_task.sh") and len(rest) >= 2:
            return [(f"fetch@{rest[0]}" if b == "fetch_task.sh" else "archive", rest[1])]
    return []


# ---------------- 候选步骤 ----------------
def candidates(root, py, queue_tasks):
    """所有任务、所有步骤（不管现在该不该做）→ (任务列表, 实验列表, 最后的步骤, {命令: (步骤, key)}, {标签: (步骤, key)})。
    任务：dict(key, exp, e, spec, names, lab)；不属于实验的 e = None。"""
    exps, final = E.load()
    table = E.tasks()
    tasks = []
    for key, t in table.items():
        e = E.experiment(t["exp"])
        lab = f"{e['id']}_f{t['fold']}" if t["fold"] is not None else e["id"]
        tasks.append(dict(key=key, exp=t["exp"], e=e, spec=t["spec"], names=E.task_names(t, root, py), lab=lab))
    for q in queue_tasks:
        if q["key"] in table or not q.get("recipe"):
            continue
        try:
            s = q.get("spec") or E.resolve_task(q["recipe"], q.get("params") or {}, q.get("exp"))
        except S.SpecError:
            continue
        tasks.append(dict(key=q["key"], exp=None, e=None, spec=s, names=dict(s["names"], data=root, python=py), lab=q["key"]))
    by_cmd, by_label = {}, {}
    for t in tasks:
        for st in (t["e"] or {}).get("after", []):
            by_cmd[S.fill(st["cmd"], t["names"], keep=())] = (st["name"], t["key"])
            by_label[f"{st['name']}:{t['lab']}"] = (st["name"], t["key"])
    groups = {}
    for e in exps:
        if not e.get("group_after"):
            continue
        names = E.group_names(e, root, py)
        groups[e["id"]] = names
        for chain in e["group_after"]:
            for st in chain:
                by_cmd[S.fill(st["cmd"], names, keep=())] = (st["name"], e["id"])
                by_label[f"{st['name']}:{e['id']}"] = (st["name"], e["id"])
    for f in final:
        by_cmd[S.fill(f["cmd"], dict(data=root, python=py), keep=())] = (f["name"], "all")
        by_label[f["name"]] = (f["name"], "all")
    return tasks, groups, final, by_cmd, by_label


# ---------------- 算下一步 ----------------
def plan(s, root=None):
    """→ (要排的 [dict(step, key, group, label, cmd, nice, …)], 卡住的 {问题编号: 说明})。"""
    root = root or bt()
    hub = M.hub()
    ptasks = (s["pueue"].get(hub) or {}).get("tasks")
    NOTES.clear()
    if ptasks is None:
        return [], {}  # box 的 pueue 读不到：这次什么都不做
    queue_tasks = [t for tq in (s.get("taskq") or {}).values() if tq for t in tq.get("tasks", [])]
    nice = {t["key"]: t["nice"] for t in queue_tasks}
    py = python()
    tasks, groups, final, by_cmd, by_label = candidates(root, py, queue_tasks)
    try:
        check_policy({st["name"] for e in E.load()[0] for st in e.get("after", []) + [x for c in e.get("group_after", []) for x in c]})
    except ValueError as e:
        return [], {"advance:policy": f"{e}，这次什么都不排"}

    live, done = set(), {}
    live_fetch = {}  # 任务 → [(源机器, 任务号)]：活着的拉回任务
    for t in ptasks:
        hit = by_cmd.get(t["command"].strip()) or by_label.get(t.get("label") or "")
        for sk in step_of(t["command"]) or ([hit] if hit else []):
            if t["status"] in LIVE_ST:
                live.add(sk)
                if sk[0].startswith("fetch@"):
                    live_fetch.setdefault(sk[1], []).append((sk[0][6:], t["id"]))
            else:
                done.setdefault(sk, []).append(t)
    src = {h: d["folds"] for h, d in s.get("src", {}).items() if h != hub}
    rerun = {f["name"] for f in final if f.get("rerun_if_newer")}
    todo, stuck = [], {}

    def want(step, key, label, cmd, group, n=NO_NICE, src_=None):
        """box pueue 里排（已经在排 / 在跑的不排；失败过的、成功了结果还不对的报出来）。"""
        sk = (f"{step}@{src_}" if src_ else step, key)
        if sk in live:
            return
        prev = sorted(done.get(sk, []), key=lambda t: t["id"])
        last_ok = max((i for i, t in enumerate(prev) if t["status"] == "Success"), default=-1)
        # 只看最后一次成功之后的；依赖失败的没跑过，原因在前一步
        bad = [t for t in prev[last_ok + 1:] if t["status"] not in ("Success", "DependencyFailed")]
        ok_empty = last_ok >= 0 and step not in rerun
        if bad or ok_empty:
            t = bad[-1] if bad else prev[last_ok]
            why = f"任务 {t['id']} {t['status']}" if bad else f"任务 {t['id']} 成功跑完，但结果还是不全"
            stuck[f"advance:{step}:{key}"] = (f"{step} {key}：{why}，不再自动重排（看 pueue log {t['id']}；处理好以后 "
                                              f"pueue remove {t['id']}，下一次检查会重排）")
            return
        todo.append(dict(step=step, key=key, group=group, label=label, cmd=cmd, nice=n))

    def stale_fetch(key, from_):
        """活着、但等的不是 from_ 的拉回任务（以前预先排的链在等另一台机器）：报出来。"""
        for h, i in live_fetch.get(key, []):
            if h != from_:
                stuck[f"advance:stale_fetch:{key}:{i}"] = (f"box 任务 {i} 在等 {h} 上的 {key} 训完，但这一折"
                                                           f"{'在 ' + from_ + ' 训完了' if from_ else '已经在 box 上了'}；"
                                                           f"不用了：pueue kill {i} 后 pueue remove {i}")

    rem = Remote(s, root, ptasks, todo, stuck)
    qkeys = {t["key"] for t in queue_tasks}
    moving = {x.get("label") for x in ptasks if x["status"] in LIVE_ST and (x.get("label") or "").startswith(("pull:", "push:"))}

    def fetching(st, names, label):
        """这一步的结果还在往 box 上拷（任务队列的拉回、pull:<标签>、push:<标签>）：box 上的判断（如 MANIFEST）可能先对上，
        拷完再算做完，也不清理那边（10/24）。"""
        qd = st.get("queue")
        return (bool(qd) and S.fill(qd["key"], names, keep=()) in live_fetch) or f"pull:{label}" in moving or f"push:{label}" in moving

    def to_queue(t, st, label, n, wait, tg):
        """放进任务队列（见开头 queue:<机器>）→ True（排了、在等、报了），或者用不了的原因（have 的那边缺输入、连不上）。"""
        qd = st.get("queue")
        if not qd:
            stuck[f"advance:{st['name']}:{t['key']}"] = f"{label}：envs/policy.py 写了 {tg['text']}，但实验表里这一步没有 queue"
            return True
        if wait:
            return True
        mach = tg["machine"]
        q = {}
        for k, v in qd.items():
            if k not in ("nice", "inputs"):
                q[k] = S.fill(v, dict(t["names"], data="{data}"), keep=())
            if k == "env":
                q["machines"] = [mach]
        q.setdefault("machines", [mach])
        if mach != hub:
            cmd = S.fill(st["cmd"], t["names"], keep=())
            needs = [S.fill(p, t["names"], keep=()) for p in qd["inputs"]] if "inputs" in qd else step_needs(cmd)
            if needs is None:
                if tg["have"]:
                    return no_needs(cmd)
                stuck[f"advance:{st['name']}:{t['key']}"] = f"{label}：{no_needs(cmd)}，查不了 {mach} 上缺什么"
                return True
            have = remote_have(mach, needs)
            if tg["have"] and (have is None or any(p not in have for p in needs)):
                return f"连不上 {mach}" if have is None else f"{mach} 上没有全部输入"
            if have is None:
                stuck[f"advance:{st['name']}:{t['key']}"] = f"{label}：连不上 {mach}，这次不处理"
                return True
            missing = [p for p in needs if p not in have]
            if missing:
                dirs = [p for p in missing if not os.path.isfile(os.path.join(root, p))]
                pcmd = "bash scripts/queue/offload_io.sh push " + " ".join([mach] + [shlex.quote(p) for p in missing])
                # 同样的传输（同一个实验的另一步要的同一批文件）也算
                pushes = sorted((x for x in ptasks if x.get("label") == f"push:{label}" or x["command"].strip() == pcmd),
                                key=lambda x: x["id"])
                if dirs or (pushes and pushes[-1]["status"] not in LIVE_ST):
                    why = (f"{mach} 上没有 {' '.join(dirs)}，要先传过去" if dirs else
                           f"传输任务 {pushes[-1]['id']} {pushes[-1]['status']}，{mach} 上还缺 {' '.join(missing)}（看 pueue log "
                           f"{pushes[-1]['id']}；处理好以后 pueue remove {pushes[-1]['id']}）")
                    stuck[f"advance:{st['name']}:{t['key']}"] = f"{label}：{why}"
                elif not pushes and not any(x.get("cmd") == pcmd for x in todo):
                    todo.append(dict(step="push", key=t["key"], group="wait", label=f"push:{label}", nice=n, paths=missing, cmd=pcmd))
                return True
        todo.append(dict(op="taskq", step=st["name"], key=t["key"], group="taskq", label=label, nice=n, qnice=qd.get("nice", 0),
                         args=S.kv_args("command", q)[1:], cmd=q["cmd"]))
        return True

    def place(t, st, label, n, archiving, archive_due, fold):
        """这一步没做完、没在往 box 上拷：接着已经在做的地方，否则按 PLACE 选一个排上。"""
        key, names = t["key"], t["names"]
        sk = (st["name"], key)
        cmd = S.fill(st["cmd"], names, keep=())
        if sk in live:
            return
        qd = st.get("queue")
        qk = S.fill(qd["key"], names, keep=()) if qd else None
        if qk in qkeys:  # 已经在任务队列里：那边做完由拉回接着
            out = os.path.join(root, S.fill(qd["out"], names, keep=()))
            if all(os.path.exists(os.path.join(out, d)) for d in qd["done"]):
                stuck[f"advance:{st['name']}:{key}"] = f"{label}：任务队列的 {qk} 做完、拉回了，但 box 上核对还是没做完（{out}）"
            return
        if label in rem.state and rem.resume(t, st, label, n, archiving):
            return
        if sk in done:  # box 上做过：失败的、成功了结果还不对的报出来
            want(st["name"], key, label, cmd, st["group"], n)
            return
        why = []
        for tg in targets(st["name"], st["group"]):
            if tg["hours"] and not in_hours(tg["hours"]):
                why.append(f"{tg['text']}：不在时段里")
                continue
            if tg["kind"] == "box":
                want(st["name"], key, label, cmd, tg["group"], n)
                return
            if tg["kind"] == "queue":
                r = to_queue(t, st, label, n, archiving or archive_due, tg)
            elif tg["kind"] == "trainer":
                r = rem.trainer(t, st, label, n, archiving, src) if fold else "不是每折的步骤"
            else:
                r = rem.start(t, st, label, n, archiving, tg["machine"], tg["group"], idle=tg["idle"], have_only=tg["have"])
            if r is True:
                return
            why.append(f"{tg['text']}：{r}")
        NOTES.append(f"{label}：现在没有能用的地方（{'；'.join(why)}）")

    trained, after_ok = {}, {}  # 任务 → box 上训完没；(任务, 步骤) → 做完没
    for t in tasks:
        key, sp, n = t["key"], t["spec"], nice.get(t["key"], NO_NICE)
        after = (t["e"] or {}).get("after", [])
        trained[key] = S.is_done(sp, root)
        archiving = ("archive", key) in live
        if not trained[key]:
            frm = next((h for h in sorted(src) if (src[h].get(key) or {}).get("final")), None)
            if frm:
                want("fetch", key, f"fetch:{t['lab']}", f"bash scripts/queue/fetch_task.sh {frm} {key}", "wait", n, src_=frm)
                stale_fetch(key, frm)
            # 第一步写了 trainer 的：这一折还在那边训时就排在训练后面
            st = after[0] if after else None
            tg = next((x for x in targets(st["name"], st["group"]) if x["kind"] == "trainer"), None) if st else None
            label = f"{st['name']}:{t['lab']}" if st else None
            if tg and (st["name"], key) not in live and not st["done"].ok(root, t["names"]) and (not tg["hours"] or in_hours(tg["hours"])):
                if not (label in rem.state and rem.resume(t, st, label, n, archiving)):
                    r = rem.trainer(t, st, label, n, archiving, src)
                    if r is not True and any(key in (src.get(h) or {}) for h in TRAINERS):
                        NOTES.append(f"{label}：训练期间不能接着在 {tg['text']} 做（{r}），训完再选地方")
            continue
        stale_fetch(key, None)
        archive_due = [h for h in TRAINERS if sp.get("archive") and (t["e"] or {}).get("archive", True) is not False
                       and (src.get(h) or {}).get(key, {}).get("final") and (src.get(h) or {}).get(key, {}).get("archived") is False]
        blocked = False
        for st in after:
            label = f"{st['name']}:{t['lab']}"
            busy = fetching(st, t["names"], label)
            ok = not busy and st["done"].ok(root, t["names"])
            after_ok[(key, st["name"])] = ok
            if ok:
                rem.finished(label)
            elif not blocked:
                if not busy:
                    place(t, st, label, n, archiving, bool(archive_due), True)
                blocked = True
        if key not in rem.busy:  # 那边在用这一折（归档会删那边的 checkpoint）时先不归档
            for h in archive_due:
                want("archive", key, f"archive:{t['lab']}", f"bash scripts/queue/archive_task.sh {h} {key}", "wait", n)

    newest = 0  # rerun_if_newer 列出的步骤里最新的结果（决定要不要重跑最后的步骤）
    watch = {name for f in final for name in f.get("rerun_if_newer", [])}
    group_steps = set()
    for e in E.load()[0]:
        if e["id"] not in groups:
            continue
        mine = [t for t in tasks if t["exp"] == e["id"]]
        names = groups[e["id"]]
        n = min([nice.get(t["key"], NO_NICE) for t in mine] or [NO_NICE])
        for chain in e["group_after"]:
            cn = [st["name"] for st in chain]  # 串里最后一个 rerun_if_newer 列出的步骤和它前面的（后面的如 prune 不影响比较）
            group_steps.update(cn[:max((i for i, x in enumerate(cn) if x in watch), default=-1) + 1])
            if not all(trained.get(t["key"]) for t in mine):
                continue
            for st in chain:
                need = st.get("needs") or []
                if not all(after_ok.get((t["key"], x)) for t in mine for x in ([need] if isinstance(need, str) else need)):
                    break
                label = f"{st['name']}:{e['id']}"
                busy = fetching(st, names, label)
                if busy or not st["done"].ok(root, names):
                    if not busy:
                        place(dict(key=e["id"], names=names), st, label, n, False, False, False)
                    break
                rem.finished(label)
                if st["name"] in watch:
                    newest = max(newest, st["done"].mtime(root, names))
    pending = any(t["step"] in group_steps for t in todo) or any(sk[0] in group_steps for sk in live)
    for f in final:
        names = dict(data=root, python=py)
        cmd = S.fill(f["cmd"], names, keep=())
        if f.get("rerun_if_newer"):
            if newest > f["done"].started(root, names) and not pending:
                want(f["name"], "all", f["name"], cmd, f["group"])
        elif not f["done"].ok(root, names):
            want(f["name"], "all", f["name"], cmd, f["group"])
    rem.close()
    todo.sort(key=lambda t: (t["nice"], t["key"]))
    return todo, stuck


# ---------------- 别的机器 ----------------
NEEDS_ERR = {}


def step_needs(cmd):
    """步骤的命令 → 要用的输入（相对数据目录）；不支持 --stage needs 或运行出错 → None（原因见 no_needs）。"""
    tok = shlex.split(cmd)
    if "--stage" not in tok or tok.index("--stage") + 1 >= len(tok):
        return None
    tok[tok.index("--stage") + 1] = "needs"
    r = subprocess.run(tok, cwd=REPO, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        err = (r.stderr.strip().splitlines() or [f"退出码 {r.returncode}"])[-1]
        NEEDS_ERR[cmd] = f"--stage needs 出错（{err}）"
        return None
    return [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]


def no_needs(cmd):
    """step_needs 返回 None 的原因。"""
    return NEEDS_ERR.get(cmd, "命令不支持 --stage needs")


def remote_have(name, paths):
    """这台机器上已经有的（文件夹里有 .incomplete 的是没传完的，不算）→ set；连不上 → None。"""
    m = M.MACHINES[name]
    script = (f'cd {m["data"]} && for p in {" ".join(shlex.quote(p) for p in paths)}; do '
              '[[ -e $p && ! -e $p/.incomplete ]] && echo "$p"; done; echo END')
    try:
        r = subprocess.run(SSH + [m["ssh"], "bash -s"], input=script, capture_output=True, text=True, timeout=120)
    except Exception:  # noqa: BLE001
        return None
    lines = r.stdout.split()
    return set(lines[:-1]) if r.returncode == 0 and lines[-1:] == ["END"] else None


def remote_gpu_free(name, group):
    """这个组用的卡上没有别人（显存 ≤ 500 MiB，和 predict.sh 的判断一样）→ True；有人或查不到 → False。"""
    gpu = M.MACHINES[name]["groups"][group][1]
    try:
        r = subprocess.run(SSH + [M.MACHINES[name]["ssh"], f"nvidia-smi -i {gpu} --query-gpu=memory.used --format=csv,noheader,nounits"],
                           capture_output=True, text=True, timeout=60)
        return r.returncode == 0 and int(r.stdout.strip()) <= 500
    except Exception:  # noqa: BLE001
        return False


def local_gb(paths, root):
    r = subprocess.run(["du", "-sbLc"] + [os.path.join(root, p) for p in paths], capture_output=True, text=True)
    return int(r.stdout.strip().splitlines()[-1].split()[0]) / 1e9 if r.stdout.strip() else 0


def load_state():
    try:
        return json.load(open(state_path()))
    except (OSError, ValueError):
        return {}


class Remote:
    """plan() 里别的 pueue 机器上做的步骤（<机器>:<组>、trainer，见开头）。往 todo 里加的除了 box 任务，还有 host=<机器> 的任务、
    op="clean" 的清理；record 是排上以后要记进 remote.json 的。"""

    def __init__(self, s, root, ptasks, todo, stuck):
        from health import task_key
        self.task_key = task_key
        self.state = load_state()
        self.s, self.root, self.ptasks, self.todo, self.stuck = s, root, ptasks, todo, stuck
        self.moved = set()  # 这次已经按 idle 分过的 (机器, 组)
        self.reserved = {}  # 机器 → 这次要传过去的 GB
        self.gpu_free = {}
        self.cleaning, self.have = [], {}
        # 那边在做的步骤（任务活着、或读不到那边的 pueue）的任务：先不归档；这次新排的也加进来
        self.busy = set()
        for lb, r in self.state.items():
            rts = self.rtasks(r["machine"])
            if rts is None or any(x["status"] in LIVE_ST for x in self.by_label(rts, lb)):
                self.busy.add(r["key"])

    def rtasks(self, name):
        return (self.s["pueue"].get(name) or {}).get("tasks")

    def by_label(self, tasks, label):
        return sorted((t for t in tasks or [] if t.get("label") == label), key=lambda t: t["id"])

    def trainings(self, rts, key):
        return sorted((x for x in rts or [] if self.task_key(x["command"]) == ("train", key)), key=lambda x: x["id"])

    def idle(self, name, group):
        """那个组空着（没有排着、在跑的任务，box 上也没有在传的），卡上也没有别人；这次还没按 idle 分过。"""
        busy = any(t["status"] in LIVE_ST and t.get("group") == group for t in self.rtasks(name) or [])
        pushing = any(t["status"] in LIVE_ST and (t.get("label") or "").startswith("push:") for t in self.ptasks)
        if busy or pushing or (name, group) in self.moved:
            return False
        if (name, group) not in self.gpu_free:
            self.gpu_free[(name, group)] = M.MACHINES[name]["groups"][group][1] is None or remote_gpu_free(name, group)
        return self.gpu_free[(name, group)]

    def trainer(self, t, st, label, n, archiving, src):
        """trainer：训这一折的 pueue 机器、训练的那个组 → True（排了或在等），或者用不了的原因。"""
        key = t["key"]
        for h in TRAINERS:
            cp = (src.get(h) or {}).get(key)
            if cp is None:
                continue
            rts = self.rtasks(h)
            if rts is None:
                return f"读不到 {h} 的 pueue"
            trs = self.trainings(rts, key)
            tr = trs[-1] if trs else None
            if not tr or cp.get("archived") or (tr["status"] != "Running" and not cp.get("final")):
                return f"{h} 上没有在训这一折"
            if cp.get("final") and (archiving or any(y["status"] in LIVE_ST and y.get("group") == tr.get("group")
                                                     and self.task_key(y["command"])[0] == "train" for y in rts)):
                return f"{h} 上训完了，在归档或那个组在训下一折"
            return self.start(t, st, label, n, archiving, h, tr["group"], push=False,
                              exclude=t["spec"]["out"].rstrip("/") + "/", train_id=tr["id"])
        return "这一折不在 pueue 机器上训"

    def start(self, t, st, label, n, archiving, name, group, idle=False, have_only=False, push=True, exclude=None, train_id=None):
        """在 name 的 group 组里做这一步：缺输入先传，齐了排上 → True（排了、在传、报了），或者用不了的原因。"""
        rts = self.rtasks(name)
        if rts is None:
            return f"读不到 {name} 的 pueue"
        if any(x["status"] in LIVE_ST for x in self.by_label(rts, label)):  # 那边已经有（remote.json 没记上）
            return True
        if idle and not self.idle(name, group):
            return f"{name} {group} 组不空"
        if archiving:  # 训练机上的同一折还在归档（会删那边的存档），归档完再说
            return True
        names = dict(t["names"], data=self.root)
        out = os.path.dirname(S.fill(st["done"].path, names, keep=()))
        qd = st.get("queue") or {}
        cmd = S.fill(st["cmd"], names, keep=())
        needs = [S.fill(p, t["names"], keep=()) for p in qd["inputs"]] if "inputs" in qd else step_needs(cmd)
        if needs is None:
            self.stuck[f"advance:remote:{label}"] = f"{label}：{no_needs(cmd)}，不能在 {name} 上做（envs/policy.py 的 PLACE）"
            return no_needs(cmd)
        needs = [p for p in needs if not (exclude and p.startswith(exclude))]
        have = remote_have(name, needs)
        if have is None:
            return f"连不上 {name}"
        self.have[label] = needs
        old = self.state.get(label) or {}
        rec = dict(machine=name, group=group, key=t["key"], out=out, needs=needs, pushed=old.get("pushed", []),
                   **({"train_id": train_id} if train_id is not None else {}))
        missing = [p for p in needs if p not in have]
        if missing:
            if not push or have_only:
                return f"{name} 上缺 {' '.join(missing[:3])}{' 等' if len(missing) > 3 else ''}"
            pushes = self.by_label(self.ptasks, f"push:{label}")
            if any(x["status"] in LIVE_ST for x in pushes):
                return True
            if pushes and pushes[-1]["status"] != "Success":
                x = pushes[-1]
                self.stuck[f"advance:remote:{label}"] = (f"{label}：传输任务 {x['id']} {x['status']}，不再自动重排（看 pueue log "
                                                         f"{x['id']}；处理好以后 pueue remove {x['id']}）")
                return True
            flying = " ".join(x["command"] for x in self.ptasks if x["status"] in LIVE_ST and (x.get("label") or "").startswith("push:"))
            planned = {p for x in self.todo if x.get("label", "").startswith("push:") for p in x.get("paths", [])}
            rest = [p for p in missing if p not in planned and p not in flying.split()]
            if not rest:
                return True
            need_gb = local_gb(rest, self.root) + REMOTE_OUT_GB
            free = (self.s.get("probe", {}).get(name) or {}).get("DISK", "").strip()
            line = (M.MACHINES[name]["disk"] or (None, 0))[1] + 10
            if not free.isdigit() or int(free) / 1e9 - self.reserved.get(name, 0) - need_gb < line:
                self.stuck[f"advance:remote:{label}"] = (f"{label}：要先传 {need_gb - REMOTE_OUT_GB:.1f} GB，{name} 盘"
                                                         f"{'剩 %.0f GB' % (int(free) / 1e9) if free.isdigit() else '还剩多少不知道'}，"
                                                         f"传完、加上结果要留 {line} GB 以上，先不传")
                return True
            self.reserved[name] = self.reserved.get(name, 0) + need_gb
            files = [p for p in rest if os.path.isfile(os.path.join(self.root, p))]
            rec["pushed"] = sorted(set(rec["pushed"]) | set(files))
            self.todo.append(dict(step="push", key=t["key"], group="wait", label=f"push:{label}", nice=n, paths=rest,
                                  cmd=refill.then_check("bash scripts/queue/offload_io.sh push " + " ".join([name] + [shlex.quote(p) for p in rest]),
                                                        f"push:{label}"),
                                  record=(label, rec)))
        else:
            rnames = dict(names, data=M.MACHINES[name]["data"], python=python(name))
            self.todo.append(dict(step=st["name"], key=t["key"], group=group, label=label, nice=n, host=name,
                                  cmd=S.fill(st["cmd"], rnames, keep=()), record=(label, rec)))
        if idle:
            self.moved.add((name, group))
        self.busy.add(t["key"])
        return True

    def resume(self, t, st, label, n, archiving):
        """remote.json 里记着的：接着做 → True；记录作废（那边的任务删掉了、这一折又重训了、训练没训完就停了）→ False，重新选地方。"""
        r = self.state[label]
        name = r["machine"]
        rts = self.rtasks(name)
        if rts is None:
            self.stuck[f"advance:remote:{label}"] = f"{label}：读不到 {name} 的 pueue，这次不处理"
            return True
        rt = self.by_label(rts, label)
        x = rt[-1] if rt else None
        trs = self.trainings(rts, r["key"]) if "train_id" in r else []
        if x and trs and trs[-1]["id"] > x["id"]:
            x = None  # 以前那次训练后面排的（这一折后来又在这台接着训了）
        if x and x["status"] in LIVE_ST:
            return True
        if x and x["status"] == "Success":
            self.pull_back(t, label)
            return True
        if x:
            cp = ((self.s.get("src", {}).get(name) or {}).get("folds") or {}).get(r["key"]) or {}
            if "train_id" in r and not cp.get("final"):
                return False  # 训练没训完就停了，这一步跟着开始、失败了：不算
            self.stuck[f"advance:remote:{label}"] = (f"{label}：{name} 任务 {x['id']} {x['status']}，不再自动重排（ssh {name} "
                                                     f"pueue log {x['id']}；处理好以后在 {name} 上 pueue remove {x['id']}）")
            return True
        if r.get("id") is not None or "train_id" in r:
            return False  # 那边的任务删掉了（处理好以后）
        why = self.start(t, st, label, n, archiving, name, r["group"])  # 还在传输入
        if why is not True:
            NOTES.append(f"{label}：在 {name} 上做，{why}")
        return True

    def pull_back(self, t, label):
        """那边做完了 → 拉回（box wait 组 pull:<标签>）。"""
        r = self.state[label]
        pulls = self.by_label(self.ptasks, f"pull:{label}")
        if any(x["status"] in LIVE_ST for x in pulls):
            return
        if pulls:
            x = pulls[-1]
            why = (f"box 任务 {x['id']} 拉回了，但 box 上核对还是没做完" if x["status"] == "Success" else f"拉回任务 {x['id']} {x['status']}")
            self.stuck[f"advance:remote:{label}"] = (f"{label}（{r['machine']} 任务 {r.get('id')}）：{why}，不再自动重排"
                                                     f"（看 pueue log {x['id']}；处理好以后 pueue remove {x['id']}）")
            return
        self.todo.append(dict(step="pull", key=t["key"], group="wait", label=f"pull:{label}", nice=NO_NICE,
                              cmd=f"bash scripts/queue/offload_io.sh pull {r['machine']} {shlex.quote(r['out'])}"))

    def finished(self, label):
        """box 上判断做完了：在别的机器上做的要清理那边。"""
        if label in self.state:
            self.cleaning.append(label)

    def close(self):
        """清理做完的：结果文件夹、别的步骤不再用的自动传过去的文件。"""
        busy = {p for lb, r in self.state.items() if lb not in self.cleaning for p in r.get("needs", [])}
        busy |= {p for ns in self.have.values() for p in ns}
        for lb in self.cleaning:
            r = self.state[lb]
            drop = [p for p in [r["out"]] + [p for p in r.get("pushed", []) if p not in busy]
                    if ".." not in p.split("/") and len(p.strip("/").split("/")) >= 2]
            self.todo.append(dict(op="clean", step="clean", key=r["key"], label=lb, nice=NO_NICE, host=r["machine"], paths=drop))


# ---------------- 执行 ----------------
def taskq_add(nice, args):
    """加进训练的任务队列（box 上的 taskq.py 自动经 ssh 转到 DICC）→ (退出码, 输出)。"""
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "add", "--nice", str(nice), "--recipe", "command", *args],
                       cwd=REPO, capture_output=True, text=True, timeout=300)
    return r.returncode, (r.stdout + r.stderr).strip()


def run(s, dry=False, q=None, tq=None):
    """health.py 调用：→ (这次排了的说明列表（只算不排时是"会排 …"）, 卡住的 {问题编号: 说明})。"""
    q = q or refill.q
    tq = tq or taskq_add
    todo, stuck = plan(s)
    live_mode = bool(P.AUTO.get("advance")) and not dry
    done, lines = [], []
    state = load_state()
    for t in todo:
        host = t.get("host") or M.hub()
        where = f"{host} " if host != M.hub() else ""
        if t.get("op") == "clean":
            what = f"{t['label']} 在 box 上做完了，删 {host} 上的 {' '.join(t['paths'])}"
            if live_mode:
                rc, out = refill.sh("rm -rf " + " ".join(f"{M.MACHINES[host]['data']}/{shlex.quote(p)}" for p in t["paths"]),
                                    M.MACHINES[host]["ssh"])
                if rc == 0:
                    state.pop(t["label"], None)
                    lines.append(f"清理 {what}")
                else:
                    stuck[f"advance:remote:{t['label']}"] = f"{t['label']}：删 {host} 上的结果出错（{out[-150:]}）"
            else:
                lines.append(f"会清理 {what}")
            continue
        h = any(fnmatch(t["label"], pat) for pat in P.HOLD)
        if t.get("op") == "taskq":
            what = f"{t['label']}（任务队列，档位 {t['qnice']}）：{t['cmd']}"
            if h:
                lines.append(f"暂缓 {what}（HOLD）")
            elif live_mode:
                rc, out = tq(t["qnice"], t["args"])
                if rc == 0:
                    done.append(f"任务队列 {t['label']}")
                    lines.append(f"排了 {what}")
                else:
                    lines.append(f"出错 {t['label']}：{out[-200:]}")
                    stuck[f"advance:{t['step']}:{t['key']}"] = f"{t['step']} {t['key']}：加进任务队列出错（{out[-200:]}）"
            else:
                done.append(f"会排 任务队列 {t['label']}")
                lines.append(f"会排 {what}")
            continue
        note = "，暂停（HOLD）" if h else ""
        if live_mode:
            try:
                tid = q(host, f"{t['group']} -p{' --stashed' if h else ''} --label {shlex.quote(t['label'])} -- {shlex.quote(t['cmd'])}")
                if t.get("record"):
                    label, rec = t["record"]
                    state[label] = dict(rec, **({"id": tid} if t.get("host") else {}))
                done.append(f"{where}{t['label']}（任务 {tid}{note}）")
                lines.append(f"排了 {t['label']}（{where}{t['group']} 组，任务 {tid}{note}）：{t['cmd']}")
            except Exception as e:  # noqa: BLE001
                lines.append(f"出错 {t['label']}：{e}")
                stuck[f"advance:{t['step']}:{t['key']}"] = f"{t['step']} {t['key']}：加任务出错（{e}）"
        else:
            done.append(f"会排 {where}{t['label']}{note}")
            lines.append(f"会排 {t['label']}（{where}{t['group']} 组{note}）：{t['cmd']}")
    lines += [f"没排 {x}" for x in NOTES]
    if live_mode:
        os.makedirs(STATE_DIR, exist_ok=True)
        with open(state_path() + ".tmp", "w") as f:
            json.dump(state, f, indent=1, ensure_ascii=False)
        os.replace(state_path() + ".tmp", state_path())
    if not dry:
        log(lines, stuck, live_mode)
    return done, stuck


def log(lines, stuck, live_mode):
    """写 ~/advance.log：和上次一样（同样的"会排"、"没排"、卡住）就不重复写。"""
    os.makedirs(STATE_DIR, exist_ok=True)
    last_p = os.path.join(STATE_DIR, "last.json")
    cur = dict(lines=[ln for ln in lines if not ln.startswith("排了")], stuck=sorted(stuck))
    try:
        last = json.load(open(last_p))
    except (OSError, ValueError):
        last = {}
    out = [ln for ln in lines if ln.startswith("排了")]
    if cur != last:
        out += [ln for ln in cur["lines"]] + [f"卡住 {stuck[k]}" for k in cur["stuck"]]
        if not out:
            out = ["没有要排的"]
    if out:
        with open(LOG, "a") as f:
            mode = "" if live_mode else "（只算不排）"
            f.writelines(f"{datetime.now():%F %T}{mode} {ln}\n" for ln in out)
    json.dump(cur, open(last_p, "w"), ensure_ascii=False)


if __name__ == "__main__":
    import health as H  # noqa: E402
    todo, stuck = plan(H.gather())
    for t in todo:
        if t.get("op") == "clean":
            print(f"会清理 {t['label']:26} {t['host']} {' '.join(t['paths'])}")
        elif t.get("op") == "taskq":
            print(f"会排 {t['label']:28} 任务队列（档位 {t['qnice']}） {' '.join(shlex.quote(a) for a in t['args'])}")
        else:
            print(f"会排 {t['label']:28} {(t.get('host', '') + ' ' + t['group']).strip():9} {t['cmd']}")
    for x in NOTES:
        print(f"没排 {x}")
    for k, v in stuck.items():
        print(f"卡住 {v}")
    if not todo and not stuck:
        print("没有要排的")
