#!/usr/bin/env python3
"""按状态推进训练完以后的事（2026-10-18；10/25 起每一步在哪里做由 envs/policy.py 的 PLACE 定）：box 上的健康检查
（scripts/monitor/health.py，每 30 分钟）调用。每次对每个任务看文件，算出它走到了哪一步，下一步没在做就排上；只有分给执行机的折提前排（见下）。
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

在哪里做（每折的步骤、group_after 的步骤）：已经在某处做着（box pueue 里有、任务队列里有、任务记录里有）就接着那边的；
否则按 PLACE[步骤] 的顺序用第一个现在能用的（写法见 envs/policy.py），都用不了时写进 ~/advance.log。没写的步骤在 box 上：
  box:<组>       box pueue 里排（标签 <步骤>:<实验 id>_f<fold>，实验一级的 <步骤>:<实验 id>）
  queue:<机器>   加进任务队列（taskq.py add --recipe command，参数是步骤 queue 里的，machines = 这台，nice = queue 的 nice）。
                 那边缺的输入是文件就先 offload_io.sh push 过去（wait 组 push:<标签>），缺文件夹就报出来；写了 have 的，那边
                 缺输入就用下一个地方。那边做完后按"队列里不属于任何实验的任务"拉回（fetch_task.sh），拉回后照常按 done 判断
  trainer        训这一折的执行机（feed.sites()）：总队列里这一折分给的那台（"lab:gpu0" 算 lab），分配已经结束的看哪台有这一折
                 没归档的进度、模型；就当成 queue:<那台>（10/31）。训的不是执行机（中转执行机、box）、找不到时用下一个地方。
                 lab 上做完的照常拉回（fetch_task.sh lab），box 上判断做完后删 lab 上的结果（训完要归档的机器，TRAINERS）；
                 这一折在 lab 上排着、没做完的步骤先做完再归档（归档会删 lab 上的模型）
  要知道每折的输入：命令支持 --stage needs（把 --stage <步骤> 换成 --stage needs，打印相对数据目录的输入），或者步骤的
  queue 里写了 inputs。10/31 以前 trainer 排在 lab pueue 里的步骤（任务记录里有 out 的）照常认、拉回、清理，不再新排（Leftover）。

提前排（10/25、10/31）：一折在总队列里分给了执行机（"lab:gpu0" 算 lab）、那边的代码是最新的（health 的 code 检查）时，每折的
  步骤里 PLACE 第一个用得上的地方是这台的任务队列的（trainer 也算），现在就加进队列，任务说明的 after 接上前一步（推理 after
  训练，评测 after 推理）；after 里的没做完时那个任务是 waiting，worker 不领，做完后训完的那个 worker 接着就能领到。
  分给中转执行机、没人领的折不提前排。
box 上排的任务（不连别的机器的）做完后马上跑一次健康检查（refill.then_check → auto_refresh.sh kick），下一步不用等 30 分钟。
失败过的不自动重排：box pueue 里这一步最后一次成功之后有失败的任务，或者成功跑完了但结果还是不对（rerun_if_newer 的除外），
  就只报出来（健康检查"需要处理"），看过 pueue log、处理好以后 pueue remove 那个任务，下一次就会重排；那边的任务、传输、拉回一样。
在不在跑：box、lab 的 pueue 任务按任务记录认（~/.cache/phd/tasks.json，scripts/queue/common.py 的 identify；这里加的任务都先记下），
  没有记录的（手动加的）只认训练、拉回、归档的命令。这一步的结果还在往 box 上拷（拉回、传输在跑）时不算做完、不清理那边。
envs/policy.py 的 AUTO["advance"] 是 False 时只把"会排什么"写进 ~/advance.log，不真排；HOLD 对上的步骤在 box 上加成暂停的
  （pueue add --stashed），进任务队列的先不加。

    python3 scripts/queue/advance.py           看现在会排什么（不改任何东西）
"""
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
import feed  # noqa: E402
import common as C  # noqa: E402

LOG = os.path.expanduser(os.environ.get("ADVANCE_LOG", "~/advance.log"))  # 环境变量只给测试用
LIVE_ST = refill.LIVE
TRAINERS = [n for n in M.names("pueue") if "train" in M.MACHINES[n]["roles"]]  # 训完要备份、删大文件的机器（lab）
NO_NICE = 10 ** 6
NOTES = []  # 这次检查里"为什么没排"的说明（写进 ~/advance.log，不报成问题）


def bt():
    return os.environ.get("ADVANCE_BT") or M.expand(M.MACHINES[M.hub()]["data"])  # 环境变量只给测试用


def python(name=None):
    base, env = M.MACHINES[name or M.hub()]["conda"]
    return f"{M.expand(base) if not name else base}/envs/{env}/bin/python"


# ---------------- 在哪里做（envs/policy.py 的 PLACE） ----------------
def targets(step, group):
    """PLACE[步骤] → [dict(text, kind=box|trainer|queue, machine, group, have)]；没写 → box 的这个组。写错了 → ValueError。"""
    out = []
    for text in P.PLACE.get(step) or [f"{M.hub()}:{group}"]:
        w = text.split()
        bad = [x for x in w[1:] if x != "have"]
        if not w or bad:
            raise ValueError(f"envs/policy.py PLACE[{step!r}] 的 {text!r}：不认识的条件 {bad}")
        d = dict(text=text, have="have" in w, machine=None, group=None)
        a, _, b = w[0].partition(":")
        if w[0] == "trainer":
            d["kind"] = "trainer"
        elif a == "queue" and b in M.MACHINES:
            d.update(kind="queue", machine=b)
        elif a == M.hub() and b in M.MACHINES[a]["groups"]:
            d.update(kind="box", machine=a, group=b)
        else:
            raise ValueError(f"envs/policy.py PLACE[{step!r}] 的 {text!r}：不认识的地方（{M.hub()}:<组>、trainer、queue:<机器>）")
        out.append(d)
    return out


def check_policy(steps):
    """PLACE 里的步骤都认得、每个地方都写对了；不对 → ValueError。"""
    unknown = sorted(set(P.PLACE) - set(steps))
    if unknown:
        raise ValueError(f"envs/policy.py PLACE 里有实验表里没有的步骤 {unknown}")
    for st in P.PLACE:
        targets(st, None)


# ---------------- 候选步骤 ----------------
def candidates(root, py, queue_tasks):
    """所有任务、所有步骤（不管现在该不该做）→ (任务列表, {实验: 实验一级的 names}, 最后的步骤)。
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
    groups = {e["id"]: E.group_names(e, root, py) for e in exps if e.get("group_after")}
    return tasks, groups, final


# ---------------- 算下一步 ----------------
def plan(s, root=None):
    """→ (要排的 [dict(step, key, group, label, cmd, nice, …)], 卡住的 {问题编号: 说明})。"""
    root = root or bt()
    hub = M.hub()
    ptasks = (s["pueue"].get(hub) or {}).get("tasks")
    NOTES.clear()
    if ptasks is None:
        return [], {}  # box 的 pueue 读不到：这次什么都不做
    # 总队列（box）在前：执行机的缓冲是其中一部分的副本，同一个 key 以总队列为准（10/31）
    tqs = s.get("taskq") or {}
    queue_tasks = list({t["key"]: t for h in sorted(tqs, key=lambda h: h == hub) if tqs[h] for t in tqs[h].get("tasks", [])}.values())
    nice = {t["key"]: t["nice"] for t in queue_tasks}
    py = python()
    tasks, groups, final = candidates(root, py, queue_tasks)
    recs = s.get("records") or []
    try:
        check_policy({st["name"] for e in E.load()[0] for st in e.get("after", []) + [x for c in e.get("group_after", []) for x in c]})
    except ValueError as e:
        return [], {"advance:policy": f"{e}，这次什么都不排"}

    live, done = set(), {}
    live_fetch = set()  # 有活着的拉回任务的任务
    moving = set()  # (步骤, key)：结果还在往 box 上拷、输入还在往那边传（pull、push）
    for t in ptasks:
        r = C.identify(hub, t, recs)
        if not r or r["step"] in ("train", "sync"):
            continue
        if r["step"] in ("pull", "push"):
            if t["status"] in LIVE_ST:
                moving.add((r.get("of"), r["key"]))
            continue
        sk = (f"fetch@{r.get('src')}" if r["step"] == "fetch" else r["step"], r["key"])
        if t["status"] in LIVE_ST:
            live.add(sk)
            if r["step"] == "fetch":
                live_fetch.add(r["key"])
        else:
            done.setdefault(sk, []).append(t)
    src = {h: d["folds"] for h, d in s.get("src", {}).items() if h != hub}
    rerun = {f["name"] for f in final if f.get("rerun_if_newer")}
    todo, stuck = [], {}

    def want(step, key, label, cmd, group, n=NO_NICE, src=None):
        """box pueue 里排（已经在排 / 在跑的不排；失败过的、成功了结果还不对的报出来）。src：拉回、归档的那台机器。"""
        sk = (f"fetch@{src}" if step == "fetch" else step, key)
        if sk in live:
            return
        prev = sorted(done.get(sk, []), key=lambda t: t["id"])
        last_ok = max((i for i, t in enumerate(prev) if t.get("result") == "Success"), default=-1)
        bad = [t for t in prev[last_ok + 1:] if t.get("result") != "Success"]  # 只看最后一次成功之后的
        ok_empty = last_ok >= 0 and step not in rerun
        if bad or ok_empty:
            t = bad[-1] if bad else prev[last_ok]
            why = f"任务 {t['id']} {t['status']}" if bad else f"任务 {t['id']} 成功跑完，但结果还是不全"
            stuck[f"advance:{step}:{key}"] = (f"{step} {key}：{why}，不再自动重排（看 pueue log {t['id']}；处理好以后 "
                                              f"pueue remove {t['id']}，下一次检查会重排）")
            return
        todo.append(dict(step=step, key=key, group=group, label=label, cmd=cmd, nice=n, src=src))

    old = Leftover(s, ptasks, todo, stuck, recs)
    remote_live = {}  # 别的 pueue 机器上活着的步骤 → (机器, 任务号)：都算在做、在用这一折
    for h, d in s["pueue"].items():
        if h == hub:
            continue
        for x in (d or {}).get("tasks") or []:
            r = C.identify(h, x, recs)
            if r and r["step"] not in ("train", "fetch", "archive") and x["status"] in LIVE_ST:
                remote_live[(r["step"], r["key"])] = (h, x["id"])
    remote_busy = {k for _, k in remote_live}
    qkeys = {t["key"] for t in queue_tasks}

    def fetching(st, names, key):
        """这一步的结果还在往 box 上拷（任务队列的拉回、pull、push）：box 上的判断（如 MANIFEST）可能先对上，
        拷完再算做完，也不清理那边（10/24）。"""
        qd = st.get("queue")
        return (bool(qd) and S.fill(qd["key"], names, keep=()) in live_fetch) or (st["name"], key) in moving

    def is_push(x, st, key, mach, paths):
        """box 任务 x 是这一步的传输，或者往同一台机器传同一批文件的传输（同一个实验的另一步要的）。"""
        r = C.identify(hub, x, recs)
        return bool(r) and r["step"] == "push" and ((r.get("of"), r["key"]) == (st["name"], key)
                                                    or (r.get("to") == mach and sorted(r.get("paths") or []) == sorted(paths)))

    def queue_item(t, st, label, n, mach, after=None):
        """任务队列里的一项（taskq.py add --recipe command 的参数，machines = mach；after = 先要做完的任务）。"""
        qd = st["queue"]
        q = {}
        for k, v in qd.items():
            if k not in ("nice", "inputs"):
                q[k] = S.fill(v, dict(t["names"], data="{data}"), keep=())
            if k == "env":
                q["machines"] = [mach]
        q.setdefault("machines", [mach])
        if after:
            q["after"] = after
        return dict(op="taskq", step=st["name"], key=t["key"], group="taskq", label=label, nice=n, qnice=qd.get("nice", 0),
                    args=S.kv_args("command", q)[1:], cmd=q["cmd"], qk=q["key"], machine=mach)

    def to_queue(t, st, label, n, wait, tg):
        """放进任务队列（见开头 queue:<机器>）→ True（排了、在等、报了），或者用不了的原因（have 的那边缺输入、连不上；
        实验表里这一步没写 queue）。"""
        qd = st.get("queue")
        if not qd:
            return "实验表里这一步没有 queue"
        if wait:
            return True
        mach = tg["machine"]
        if mach != hub:
            cmd = S.fill(st["cmd"], t["names"], keep=())
            needs = inputs_of(qd, t["names"]) if "inputs" in qd else step_needs(cmd)
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
                pushes = sorted((x for x in ptasks if is_push(x, st, t["key"], mach, missing)), key=lambda x: x["id"])
                if dirs or (pushes and pushes[-1]["status"] not in LIVE_ST):
                    why = (f"{mach} 上没有 {' '.join(dirs)}，要先传过去" if dirs else
                           f"传输任务 {pushes[-1]['id']} {pushes[-1]['status']}，{mach} 上还缺 {' '.join(missing)}（看 pueue log "
                           f"{pushes[-1]['id']}；处理好以后 pueue remove {pushes[-1]['id']}）")
                    stuck[f"advance:{st['name']}:{t['key']}"] = f"{label}：{why}"
                elif not pushes and not any(x.get("cmd") == pcmd for x in todo):
                    todo.append(dict(step="push", key=t["key"], group="wait", label=f"push:{label}", nice=n, paths=missing, cmd=pcmd,
                                     of=st["name"], to=mach))
                return True
        todo.append(queue_item(t, st, label, n, mach))
        return True

    tq_tasks = {h: {x["key"]: x for x in (tq or {}).get("tasks", [])} for h, tq in (s.get("taskq") or {}).items()}
    feed_sites = set(feed.sites())
    code = s.get("code") or {}

    def assigned_to(key):
        """总队列里这一折分给的机器（"lab:gpu0" → lab）；没分出去 → None。"""
        x = tq_tasks.get(hub, {}).get(key) or {}
        if x.get("state") != "claimed":
            return None
        return (x.get("assigned") or "").partition(":")[0] or None

    def trainer_of(key):
        """训这一折的机器：总队列里分给的那台；分配已经结束的看哪台有这一折没归档的进度、模型（训完的优先）。"""
        h = assigned_to(key)
        if h:
            return h
        have = [h for h in sorted(src) if key in src[h] and not src[h][key].get("archived")]
        return next((h for h in have if src[h][key].get("final")), have[0] if have else None)

    def where(t, tg, fold):
        """PLACE 里的一项 → 地方（trainer 换成训这一折的执行机的 queue，带 trainer=True），或者用不了的原因。"""
        if tg["kind"] != "trainer":
            return tg
        if not fold:
            return "不是每折的步骤"
        h = trainer_of(t["key"])
        if h not in feed_sites:
            return f"训这一折的 {h} 不是执行机" if h else "找不到训这一折的机器"
        return dict(tg, kind="queue", machine=h, trainer=True)

    def qmach(qk):
        """总队列里这个任务（加上这次要加的）给了哪几台机器。"""
        x = tq_tasks.get(hub, {}).get(qk) or {}
        ms = (x.get("spec") or x.get("params") or {}).get("machines") or []
        return set(ms) | {y["machine"] for y in todo if y.get("op") == "taskq" and y.get("qk") == qk}

    cleans = {}  # (机器, 路径) → op="clean" 的一项：执行机上做完、box 上判断做完的步骤，删那边的结果

    def finished(t, st, label):
        """box 上判断这一步做完了：在训练机（TRAINERS，lab）的缓冲里做的，删那边的结果（那边有做完的标志时）。"""
        old.finished(label)
        qd = st.get("queue")
        if not qd:
            return
        qk, out = (S.fill(qd[k], t["names"], keep=()) for k in ("key", "out"))
        for h in TRAINERS:
            if (src.get(h) or {}).get(qk, {}).get("final"):
                cleans.setdefault((h, out), dict(op="clean", step="clean", key=t["key"], label=label, nice=NO_NICE, host=h, paths=[out]))

    def prequeue(t, after):
        """这一折在总队列里分给了一台执行机（feed.py 的 sites）：每折的步骤里，PLACE 第一个用得上的地方是这台的，现在就加进
        总队列，after 接上前一步；feed.py 跟着训练送进那边的缓冲，训完的那个 worker 接着就能领到推理（10/25、10/31）。"""
        h = assigned_to(t["key"])
        if h not in feed_sites:
            return
        prev, items = t["key"], []
        for st in after:
            label = f"{st['name']}:{t['lab']}"
            qd = st.get("queue")
            tg = next((w for w in (where(t, y, True) for y in targets(st["name"], st["group"])) if not isinstance(w, str)), None)
            if not qd or not tg or tg["kind"] != "queue" or tg["machine"] != h or any(fnmatch(label, pat) for pat in P.HOLD):
                break
            sk = (st["name"], t["key"])
            if sk in live or sk in done or sk in remote_live or (label in old.state and old.resume(label)) or st["done"].ok(root, t["names"]):
                break
            qk = S.fill(qd["key"], t["names"], keep=())
            if qk not in qkeys:
                items.append(queue_item(t, st, label, nice.get(t["key"], NO_NICE), h, after=[prev]))
            prev = qk
        if not items:
            return
        if not code.get("main") or (code.get("heads") or {}).get(h) != code["main"]:
            NOTES.append(f"{t['lab']}：{h} 的代码不是最新的（不认 after），先不提前排")
            return
        todo.extend(items)

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
        if label in old.state and old.resume(label):
            return
        if sk in remote_live:
            h, i = remote_live[sk]
            stuck[f"advance:remote:{label}"] = (f"{label}：{h} 任务 {i} 在做，但不是这一折现在的记录里的那个任务，做完不会自动拉回"
                                                f"（等它做完、在 {h} 上 pueue remove {i} 后重排）")
            return
        if sk in done:  # box 上做过：失败的、成功了结果还不对的报出来
            want(st["name"], key, label, cmd, st["group"], n)
            return
        why = []
        for tg in targets(st["name"], st["group"]):
            w = where(t, tg, fold)
            if isinstance(w, str):
                r = w
            elif w["kind"] == "box":
                want(st["name"], key, label, cmd, w["group"], n)
                return
            elif w.get("trainer"):  # 模型在那边：不等归档（归档等这一步做完），在归档时不用那边
                r = f"{w['machine']} 上在归档这一折" if archiving else to_queue(t, st, label, n, False, w)
            else:
                r = to_queue(t, st, label, n, archiving or archive_due, w)
            if r is True:
                return
            why.append(f"{tg['text']}：{r}")
        NOTES.append(f"{label}：现在没有能用的地方（{'；'.join(why)}）")

    trained, after_ok = {}, {}  # 任务 → box 上训完没；(任务, 步骤) → 做完没
    for t in tasks:
        key, sp, n = t["key"], t["spec"], nice.get(t["key"], NO_NICE)
        after = (t["e"] or {}).get("after", [])
        trained[key] = S.is_done(sp, root)
        archiving = ("archive", key) in live or ("relay_fetch", key) in live
        if not trained[key]:
            frm = next((h for h in sorted(src) if (src[h].get(key) or {}).get("final")), None)
            if frm:
                want("fetch", key, f"fetch:{t['lab']}", f"bash scripts/queue/fetch_task.sh {frm} {key}", "wait", n, src=frm)
            prequeue(t, after)
            continue
        archive_due = [h for h in TRAINERS if sp.get("archive") and (t["e"] or {}).get("archive", True) is not False
                       and (src.get(h) or {}).get(key, {}).get("final") and (src.get(h) or {}).get(key, {}).get("archived") is False]
        blocked = False
        for st in after:
            label = f"{st['name']}:{t['lab']}"
            busy = fetching(st, t["names"], key)
            ok = not busy and st["done"].ok(root, t["names"])
            after_ok[(key, st["name"])] = ok
            if ok:
                finished(t, st, label)
            elif not blocked:
                if not busy:
                    place(t, st, label, n, archiving, bool(archive_due), True)
                blocked = True
        # 那边在用这一折（归档会删那边的 checkpoint）时先不归档：旧的 lab pueue 步骤在做，或者有步骤排给了那台、box 上还没做完
        pend = {m for st in after if st.get("queue") and not after_ok[(key, st["name"])]
                for m in qmach(S.fill(st["queue"]["key"], t["names"], keep=()))}
        if key not in old.busy and key not in remote_busy:
            for h in archive_due:
                if h in pend:
                    NOTES.append(f"archive:{t['lab']}：{h} 上还有这一折的步骤没做完，做完再归档")
                    continue
                want("archive", key, f"archive:{t['lab']}", f"bash scripts/queue/archive_task.sh {h} {key}", "wait", n, src=h)

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
                busy = fetching(st, names, e["id"])
                if busy or not st["done"].ok(root, names):
                    if not busy:
                        place(dict(key=e["id"], names=names), st, label, n, False, False, False)
                    break
                finished(dict(key=e["id"], names=names), st, label)
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
    for c in old.close():
        cleans.setdefault((c["host"], c["paths"][0]), c)
    todo += [c for c in cleans.values() if ".." not in c["paths"][0].split("/") and len(c["paths"][0].strip("/").split("/")) >= 2]
    todo.sort(key=lambda t: (t["nice"], t["key"]))
    return todo, stuck


# ---------------- 别的机器 ----------------
NEEDS_ERR = {}


def inputs_of(qd, names):
    """步骤 queue 里的 inputs → 相对数据目录的路径；实验一级的步骤（没有 fold）里带 {fold} 的按实验的每一折展开。"""
    out = []
    for p in qd["inputs"]:
        if "{fold}" in p and "fold" not in names:
            out += [S.fill(p, dict(names, fold=f), keep=()) for f in names.get("folds", "").split()]
        else:
            out.append(S.fill(p, names, keep=()))
    return out


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
    rc, out, _ = C.run("bash -s", m["ssh"], input=C.have_script(paths, m["data"]))
    return C.parse_have(out) if rc == 0 else None


class Leftover:
    """10/31 以前 trainer 排在训练机 pueue 里的步骤（任务记录里有 out、train_id）：在排、在跑的照常认，做完拉回（box wait 组
    pull:<标签>，offload_io.sh pull，按校验和核对），box 上判断做完后删那边的结果；不再新排。state：标签 → 这个标签最新的这种记录。"""

    def __init__(self, s, ptasks, todo, stuck, recs):
        self.recs, self.s, self.ptasks, self.todo, self.stuck = recs, s, ptasks, todo, stuck
        self.state = {r["label"]: r for r in sorted((r for r in recs if r.get("out")), key=lambda r: r["added"])}
        self.cleaning = []
        self.busy = set()  # 那边在做的步骤（任务活着、或读不到那边的 pueue）的任务：先不归档
        for r in self.state.values():
            rts = self.rtasks(r["machine"])
            x = self.task_of(r, rts)
            if rts is None or (x and x["status"] in LIVE_ST):
                self.busy.add(r["key"])

    def rtasks(self, name):
        return (self.s["pueue"].get(name) or {}).get("tasks")

    def task_of(self, r, rts):
        return next((x for x in rts or [] if x["id"] == r.get("id") and (x.get("label") or "") == r["label"]), None)

    def resume(self, label):
        """在排、在跑、做完了（拉回）、失败了（报出来）→ True；那边的任务删掉了、训练没训完就停了 → False，按 PLACE 重新选地方。"""
        r = self.state[label]
        name = r["machine"]
        rts = self.rtasks(name)
        if rts is None:
            self.stuck[f"advance:remote:{label}"] = f"{label}：读不到 {name} 的 pueue，这次不处理"
            return True
        x = self.task_of(r, rts)
        if not x:
            return False
        if x["status"] in LIVE_ST:
            return True
        if x.get("result") == "Success":
            self.pull_back(label)
            return True
        cp = ((self.s.get("src", {}).get(name) or {}).get("folds") or {}).get(r["key"]) or {}
        if "train_id" in r and not cp.get("final"):
            return False  # 训练没训完就停了，这一步跟着开始、失败了：不算
        self.stuck[f"advance:remote:{label}"] = (f"{label}：{name} 任务 {x['id']} {x['status']}，不再自动重排（ssh {name} "
                                                 f"pueue log {x['id']}；处理好以后在 {name} 上 pueue remove {x['id']}）")
        return True

    def pull_back(self, label):
        """那边做完了 → 拉回（box wait 组 pull:<标签>）。"""
        r = self.state[label]
        pulls = sorted((x for x in self.ptasks if (lambda p: bool(p) and p["step"] == "pull" and (p.get("of"), p["key"]) == (r["step"], r["key"]))
                        (C.identify(M.hub(), x, self.recs))), key=lambda x: x["id"])
        if any(x["status"] in LIVE_ST for x in pulls):
            return
        if pulls:
            x = pulls[-1]
            why = (f"box 任务 {x['id']} 拉回了，但 box 上核对还是没做完" if x.get("result") == "Success" else f"拉回任务 {x['id']} {x['status']}")
            self.stuck[f"advance:remote:{label}"] = (f"{label}（{r['machine']} 任务 {r.get('id')}）：{why}，不再自动重排"
                                                     f"（看 pueue log {x['id']}；处理好以后 pueue remove {x['id']}）")
            return
        self.todo.append(dict(step="pull", key=r["key"], group="wait", label=f"pull:{label}", nice=NO_NICE, of=r["step"], src=r["machine"],
                              cmd=f"bash scripts/queue/offload_io.sh pull {r['machine']} {shlex.quote(r['out'])}"))

    def finished(self, label):
        if label in self.state:
            self.cleaning.append(label)

    def close(self):
        """box 上判断做完了的 → op="clean"：删那边的结果文件夹（run 里删完再删记录）。"""
        return [dict(op="clean", step="clean", key=self.state[lb]["key"], label=lb, nice=NO_NICE, host=self.state[lb]["machine"],
                     paths=[self.state[lb]["out"]]) for lb in self.cleaning]


# ---------------- 执行 ----------------
def taskq_add(nice, args):
    """加进总队列（box 上本地的 taskq.py，10/31）→ (退出码, 输出)；feed.py 再送到执行机的缓冲。"""
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
    for t in todo:
        host = t.get("host") or M.hub()
        where = f"{host} " if host != M.hub() else ""
        if t.get("op") == "clean":
            what = f"{t['label']} 在 box 上做完了，删 {host} 上的 {' '.join(t['paths'])}"
            if live_mode:
                rc, out = refill.sh("rm -rf " + " ".join(f"{M.MACHINES[host]['data']}/{shlex.quote(p)}" for p in t["paths"]),
                                    M.MACHINES[host]["ssh"])
                if rc == 0:
                    C.drop_records(lambda r: bool(r.get("out")) and r["machine"] == host and r["label"] == t["label"])
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
                # 先记下来再加任务：加上了、没拿到任务号（断电、ssh 超时）时，下次按标签和创建时间找回
                rec = C.new_record(host, t["label"], t["step"], t["key"], group=t["group"],
                                   **{k: t.get(k) for k in ("src", "of", "to", "paths")})
                cmd = refill.then_check(t["cmd"], t["label"])
                tid = q(host, f"{t['group']} -p{' --stashed' if h else ''} --label {shlex.quote(t['label'])} -- {shlex.quote(cmd)}")
                C.set_record_id(rec, tid)
                done.append(f"{where}{t['label']}（任务 {tid}{note}）")
                lines.append(f"排了 {t['label']}（{where}{t['group']} 组，任务 {tid}{note}）：{t['cmd']}")
            except Exception as e:  # noqa: BLE001
                lines.append(f"出错 {t['label']}：{e}")
                stuck[f"advance:{t['step']}:{t['key']}"] = f"{t['step']} {t['key']}：加任务出错（{e}）"
        else:
            done.append(f"会排 {where}{t['label']}{note}")
            lines.append(f"会排 {t['label']}（{where}{t['group']} 组{note}）：{t['cmd']}")
    lines += [f"没排 {x}" for x in NOTES]
    if not dry:
        log(lines, stuck, live_mode)
    return done, stuck


def log(lines, stuck, live_mode):
    """写 ~/advance.log：和上次一样（同样的"会排"、"没排"、卡住）就不重复写。"""
    cur = dict(lines=[ln for ln in lines if not ln.startswith("排了")], stuck=sorted(stuck))
    out = [ln for ln in lines if ln.startswith("排了")]
    if C.changed("advance", cur):
        out += [ln for ln in cur["lines"]] + [f"卡住 {stuck[k]}" for k in cur["stuck"]]
        if not out:
            out = ["没有要排的"]
    if out:
        with open(LOG, "a") as f:
            mode = "" if live_mode else "（只算不排）"
            f.writelines(f"{datetime.now():%F %T}{mode} {ln}\n" for ln in out)


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
