#!/usr/bin/env python3
"""按状态推进训练完以后的事（2026-10-18；10/25 起每一步在哪里做由 envs/policy.py 的 PLACE 定）：box 上的健康检查
（scripts/taskq/monitor/health.py，每 30 分钟）调用。每次对每个任务看文件，算出它走到了哪一步，下一步没在做就排上；只有分给执行机的任务提前排（见下）。
步骤和判断都从实验表（research/*/experiments.py，经 scripts/taskq/queue/experiments.py 读）和任务说明（scripts/taskq/queue/spec.py）来，
这里不认识具体模型。

每个任务（实验表里的，加上队列里不属于任何实验的）：box 上训完（说明里的 done）以后，实验表里每个任务的步骤（如推理 → 评测），
  按顺序做第一个没做完的。执行机训完的由 relay.py 拉回、归档。
一个实验所有任务都在 box 上训完以后：group_after 的每一串步骤，按顺序做第一个没做完的（needs = 要每个任务都做完的步骤）。
最后：FINAL 的步骤（如所有实验的汇总比较，在 box 上）。有 rerun_if_newer 的，列出的步骤有比它开始时更新的结果、这些步骤（和同一串
里排在它们前面的）都没有在排 / 在跑时重跑。

在哪里做（每个任务的步骤、group_after 的步骤）：已经在某处做着（box pueue 里有、任务队列里有、任务记录里有）就接着那边的；
否则按 PLACE[步骤] 的顺序用第一个现在能用的（写法见 envs/policy.py），都用不了时写进 ~/advance.log。没写的步骤在 box 上：
  box:<组>       hub（总队列所在的机器，不管它在机器表里叫什么；也可以写 hub:<组>）的 pueue 里排（标签 <步骤>:<实验 id>_f<fold>，
                 实验一级的 <步骤>:<实验 id>）
  queue:<机器>   加进总队列（taskq.py add --recipe command，参数是步骤 queue 里的，machines = 这台，nice = queue 的 nice）。
                 执行机（envs/machines.py 的 RELAY）由 relay.py 领到后连同输入送过去，做完由 relay.py 拉回
  trainer        训这个任务的执行机：总队列里这个任务分给的那台（训完还没交还的也算），就当成 queue:<那台>（10/31）。
                 按时段的执行机（lab0）训的任务给同一台主机上不按时段的执行机（lab1）。找不到时用下一个地方。
  要知道每个任务的输入：命令支持 --stage needs（把 --stage <步骤> 换成 --stage needs，打印相对数据目录的输入），或者步骤的
  queue 里写了 inputs。

提前排（10/25、10/31）：一个任务在总队列里分给了执行机时，每个任务的步骤里 PLACE 第一个用得上的地方是这台的任务队列的
  （trainer 也算），现在就加进总队列，任务说明的 after 接上前一步（推理 after 训练，评测 after 推理）；after 里的没做完时那个任务
  是 waiting，worker 不领，做完后训完的那个 worker 接着就能领到。没人领的任务不提前排。
box 上排的任务做完后马上跑一次健康检查（refill.then_check → auto_refresh.sh kick），下一步不用等 30 分钟。
失败过的不自动重排：box pueue 里这一步最后一次成功之后有失败的任务，或者成功跑完了但结果还是不对（rerun_if_newer 的除外），
  就只报出来（健康检查"需要处理"），看过 pueue log、处理好以后 pueue remove 那个任务，下一次就会重排。
在不在跑：box 的 pueue 任务按任务记录认（~/.cache/phd/tasks.json，scripts/taskq/queue/common.py 的 identify；这里加的任务都先记下），
  没有记录的（手动加的）只认训练的命令。这一步的结果还在往 box 上拷（relay.py 的拉回在跑）时不算做完。
envs/policy.py 的 AUTO["advance"] 是 False 时只把"会排什么"写进 ~/advance.log，不真排；HOLD 对上的步骤在 box 上加成暂停的
  （pueue add --stashed），进任务队列的先不加。

    python3 scripts/taskq/queue/advance.py           看现在会排什么（不改任何东西）
"""
import itertools
import os
import shlex
import subprocess
import sys
from datetime import datetime
from fnmatch import fnmatch

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
from proto import alert_id, alert_msg, S_CLAIMED, S_DONE, S_WAITING, S_FREE  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "monitor"))
sys.path.insert(0, HERE)
import machines as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import experiments as E  # noqa: E402
import refill  # noqa: E402
import shift  # noqa: E402
import common as C  # noqa: E402
import taskq  # noqa: E402

LOG = cfg.log("advance")
LIVE_ST = refill.LIVE
NO_NICE = 10 ** 6
NOTES = []  # 这次检查里"为什么没排"的说明（写进 ~/advance.log，不报成问题）


def bt():
    return os.environ.get("ADVANCE_BT") or M.expand(M.MACHINES[M.hub()]["data"])  # 环境变量只给测试用


def python(name=None):
    base, env = M.MACHINES[name or M.hub()]["conda"]
    return f"{M.expand(base) if not name else base}/envs/{env}/bin/python"


# ---------------- 在哪里做（envs/policy.py 的 PLACE） ----------------
HUB_PLACES = ("box", "hub")  # PLACE 里 box:<组>（或 hub:<组>）= 总队列所在的机器（M.hub()，不管它叫什么）的 pueue 组（11/4）


def targets(step, group):
    """PLACE[步骤] → [dict(text, kind=box|trainer|queue, machine, group)]；没写 → hub 的这个组。写错了 → ValueError。"""
    out = []
    for text in P.PLACE.get(step) or [f"box:{group}"]:
        w = text.split()
        if len(w) != 1:
            raise ValueError(f"envs/policy.py PLACE[{step!r}] 的 {text!r}：不认识的条件 {w[1:]}")
        d = dict(text=text, machine=None, group=None)
        a, _, b = w[0].partition(":")
        if w[0] == "trainer":
            d["kind"] = "trainer"
        elif a == "queue" and (b == M.hub() or b in M.RELAY):
            d.update(kind="queue", machine=b)
        elif (a in HUB_PLACES or a == M.hub()) and b in M.MACHINES[M.hub()]["groups"]:
            d.update(kind="box", machine=M.hub(), group=b)
        else:
            raise ValueError(f"envs/policy.py PLACE[{step!r}] 的 {text!r}：不认识的地方（box:<组>、trainer、queue:<机器>）")
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
        lab = E.label(e, t)
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
    queue_tasks = list(((s.get("taskq") or {}).get(hub) or {}).get("tasks", []))
    nice = {t["key"]: t["nice"] for t in queue_tasks}
    py = python()
    tasks, groups, final = candidates(root, py, queue_tasks)
    recs = s.get("records") or []
    try:
        check_policy({st["name"] for e in E.load()[0] for st in e.get("after", []) + [x for c in e.get("group_after", []) for x in c]})
    except ValueError as e:
        return [], {alert_id("advance", "policy"): f"{e}，这次什么都不排"}

    live, done = set(), {}
    for t in ptasks:
        r = C.identify(hub, t, recs)
        if not r or r["step"] in ("train", "sync"):
            continue
        sk = (r["step"], r["key"])
        if t["status"] in LIVE_ST:
            live.add(sk)
        else:
            done.setdefault(sk, []).append(t)
    rerun = {f["name"] for f in final if f.get("rerun_if_newer")}
    todo, stuck = [], {}

    def want(step, key, label, cmd, group, n=NO_NICE):
        """box pueue 里排（已经在排 / 在跑的不排；失败过的、成功了结果还不对的报出来）。"""
        sk = (step, key)
        if sk in live:
            return
        prev = sorted(done.get(sk, []), key=lambda t: t["id"])
        last_ok = max((i for i, t in enumerate(prev) if t.get("result") == "Success"), default=-1)
        bad = [t for t in prev[last_ok + 1:] if t.get("result") != "Success"]  # 只看最后一次成功之后的
        ok_empty = last_ok >= 0 and step not in rerun
        if bad or ok_empty:
            t = bad[-1] if bad else prev[last_ok]
            why = f"任务 {t['id']} {t['status']}" if bad else f"任务 {t['id']} 成功跑完，但结果还是不全"
            stuck[alert_id("advance", step, key)] = (f"{step} {key}：{why}，不再自动重排（看 pueue log {t['id']}；处理好以后 "
                                              f"pueue remove {t['id']}，下一次检查会重排）")
            return
        todo.append(dict(step=step, key=key, group=group, label=label, cmd=cmd, nice=n))

    qkeys = {t["key"] for t in queue_tasks}

    def fetching(st, names):
        """这一步的结果还在往 box 上拷（relay.py 的拉回）：box 上的判断（如 MANIFEST）可能先对上，拷完再算做完（10/24）。"""
        qd = st.get("queue")
        return bool(qd) and ("relay_fetch", S.fill(qd["key"], names, keep=())) in live

    def queue_item(t, st, label, n, mach, after=None, inputs=None):
        """任务队列里的一项（taskq.py add --recipe command 的参数，machines = mach；after = 先要做完的任务；inputs = 执行机要准备的输入）。"""
        qd = st["queue"]
        q = {}
        for k, v in qd.items():
            if k not in ("nice", "inputs", "disk"):
                q[k] = S.fill(v, dict(t["names"], data="{data}"), keep=())
            if k == "env":
                q["machines"] = [mach]
        q.setdefault("machines", [mach])
        if after:
            q["after"] = after
        if inputs:
            q["inputs"] = inputs
        return dict(op="taskq", step=st["name"], key=t["key"], group="taskq", label=label, nice=n, qnice=qd.get("nice", 0),
                    args=S.kv_args("command", q)[1:], cmd=q["cmd"], qk=q["key"], machine=mach)

    def to_queue(t, st, label, n, wait, tg):
        """放进任务队列（见开头 queue:<机器>）→ True（排了、在等），或者用不了的原因（实验表里这一步没写 queue）。"""
        qd = st.get("queue")
        if not qd:
            return "实验表里这一步没有 queue"
        if wait:
            return True
        mach = tg["machine"]
        if mach in M.RELAY:  # relay.py 领到后连同输入一起送过去：输入按命令的 --stage needs:<阶段> 列出，不支持的用 queue 的 inputs
            cmd = S.fill(st["cmd"], t["names"], keep=())
            needs = step_needs(cmd)
            if needs is None and "inputs" in qd:
                needs = inputs_of(qd, t["names"], t.get("each") or ())
            todo.append(queue_item(t, st, label, n, mach, inputs=needs))
            return True
        todo.append(queue_item(t, st, label, n, mach))
        return True

    tq_tasks = {x["key"]: x for x in queue_tasks}
    relay_sites = set(M.RELAY)  # relay.py 替它领任务的执行机：RELAY 全部（READY 缺项按 1，和 relay.py 同一规则，11/4）

    def site_of(h):
        """训这个任务的机器 → 放后续步骤的执行机：不按时段的执行机就是它；按时段的（lab0，envs/policy.py 的 SHIFT）→ 同一台主机上
        开着、不按时段的执行机（lab1）；没有 → None。"""
        if h in relay_sites and not shift.limited(h):
            return h
        ssh = (M.RELAY.get(h) or {}).get("ssh")
        return shift.day_site(ssh) if ssh else None

    def assigned_to(key):
        """总队列里这个任务分给的机器；没分出去 → None。"""
        x = tq_tasks.get(key) or {}
        if x.get("state") != S_CLAIMED:
            return None
        return x.get("assigned") or None

    def trainer_of(key):
        """训这个任务的执行机：总队列里分给的那台（训完、box 上有了、还没交还的也算）。"""
        h = assigned_to(key)
        if h:
            return h
        r = (tq_tasks.get(key) or {}).get("assigned")
        return r if r in relay_sites else None

    def too_big(m, st):
        """这一步在执行机 m 上放不下（m 有 cap，实验表 queue 的 disk 加上 budget 的第二项超过 cap）→ 原因；放得下 → None。"""
        c, gb = (M.RELAY.get(m) or {}).get("cap"), ((st or {}).get("queue") or {}).get("disk")
        hard = ((M.RELAY.get(m) or {}).get("budget") or (0, 0))[1]
        return f"要 {gb} GB，{m} 最多放 {c} GB" if c and gb and gb + hard > c else None

    def where(t, tg, per_task, st=None):
        """PLACE 里的一项 → 地方（trainer 换成训这个任务的执行机的 queue，带 trainer=True），或者用不了的原因。
        queue:<机器>：那台暂停检查（OFFLINE）时用不了；执行机的 AUTO 关着时用不了（输入由 relay.py 传过去）。执行机放不下这一步（too_big）时用不了。"""
        if tg["kind"] == "queue":
            m = tg["machine"]
            if m in P.OFFLINE:
                return f"{m} 暂停检查（{P.OFFLINE[m]}）"
            if m in M.RELAY and m not in M.MACHINES and not P.AUTO.get(m):
                return f"{m} 的 AUTO 关着"
            return too_big(m, st) or tg
        if tg["kind"] != "trainer":
            return tg
        if not per_task:
            return "不是每个任务的步骤"
        h = trainer_of(t["key"])
        site = site_of(h)
        if not site:
            return f"训这个任务的 {h} 不是执行机" if h else "找不到训这个任务的机器"
        return too_big(site, st) or dict(tg, kind="queue", machine=site, trainer=True)

    def prequeue(t, after):
        """这个任务在总队列里分给了一台执行机：每个任务的步骤里，PLACE 第一个用得上的地方是这台的，现在就加进总队列，
        after 接上前一步；relay.py 跟着训练送过去，训完的那个 worker 接着就能领到推理（10/25、10/31）。"""
        h = site_of(assigned_to(t["key"]))
        # 训练不再分给提前排时的那台（交还、停下、改分给别处），或那台现在放不下这一步：那时排的、还没人领的后续步骤连同后面的
        # 一起撤掉，分出去时按新的那台重排（11/2）。训练做完了（done）不撤：后续步骤正等着那边的 worker 领
        tx = tq_tasks.get(t["key"]) or {}
        gone, cut = [], False
        for st in after if tx.get("state") != S_DONE else ():
            qk = S.fill(st["queue"]["key"], t["names"], keep=()) if st.get("queue") else None
            x = tq_tasks.get(qk) if qk else None
            if not cut and h:
                tg = next((w for w in (where(t, y, True, st) for y in targets(st["name"], st["group"])) if not isinstance(w, str)), None)
                cut = not tg or tg["kind"] != "queue" or tg["machine"] != h
            ms = ((x or {}).get("spec") or {}).get("machines") or []
            if x and x.get("state") in (S_WAITING, S_FREE) and not x.get("worker") and ms and (cut or not h or ms != [h]):
                gone.append(qk)
        if gone:
            ms_ = " ".join(sorted({m for k in gone for m in ((tq_tasks[k].get("spec") or {}).get("machines") or [])}))
            todo.append(dict(op="unqueue", step="unqueue", key=t["key"], label=f"unqueue:{t['lab']}", nice=NO_NICE, qks=gone,
                             why=f"训练{f'分给了 {h}' if h else '没分给执行机'}，撤掉先前排给 {ms_} 的"))
            qkeys.difference_update(gone)
        if not h:
            return
        prev, items = t["key"], []
        for st in after:
            label = f"{st['name']}:{t['lab']}"
            qd = st.get("queue")
            tg = next((w for w in (where(t, y, True, st) for y in targets(st["name"], st["group"])) if not isinstance(w, str)), None)
            if not qd or not tg or tg["kind"] != "queue" or tg["machine"] != h or any(fnmatch(label, pat) for pat in P.HOLD):
                break
            sk = (st["name"], t["key"])
            if sk in live or sk in done or st["done"].ok(root, t["names"]):
                break
            qk = S.fill(qd["key"], t["names"], keep=())
            if qk not in qkeys:
                items.append(queue_item(t, st, label, nice.get(t["key"], NO_NICE), h, after=[prev]))
            prev = qk
        todo.extend(items)

    def place(t, st, label, n, archiving, per_task):
        """这一步没做完、没在往 box 上拷：接着已经在做的地方，否则按 PLACE 选一个排上。"""
        key, names = t["key"], t["names"]
        sk = (st["name"], key)
        cmd = S.fill(st["cmd"], names, keep=())
        if sk in live:
            return
        qd = st.get("queue")
        qk = S.fill(qd["key"], names, keep=()) if qd else None
        if qk in qkeys:  # 已经在任务队列里：那边做完由拉回接着
            o = S.outputs(S.fill({k: qd.get(k) for k in ("out", "done", "fetch")}, names, keep=()))
            out = os.path.join(root, o["outs"][0])
            if S.done_with(o, lambda a: os.path.exists(os.path.join(out, a)), empty=False):
                stuck[alert_id("advance", st["name"], key)] = f"{label}：任务队列的 {qk} 做完、拉回了，但 box 上核对还是没做完（{out}）"
            return
        if sk in done:  # box 上做过：失败的、成功了结果还不对的报出来
            want(st["name"], key, label, cmd, st["group"], n)
            return
        why = []
        for tg in targets(st["name"], st["group"]):
            w = where(t, tg, per_task, st)
            if isinstance(w, str):
                r = w
            elif w["kind"] == "box":
                want(st["name"], key, label, cmd, w["group"], n)
                return
            elif w.get("trainer"):  # 模型在那边：在往 box 拉回（拉回后删那边的）时不用那边
                r = f"{w['machine']} 上在拉回这个任务" if archiving else to_queue(t, st, label, n, False, w)
            else:
                r = to_queue(t, st, label, n, archiving, w)
            if r is True:
                return
            why.append(f"{tg['text']}：{r}")
        NOTES.append(f"{label}：现在没有能用的地方（{'；'.join(why)}）")

    trained, after_ok = {}, {}  # 任务 → box 上训完没；(任务, 步骤) → 做完没
    for t in tasks:
        key, sp, n = t["key"], t["spec"], nice.get(t["key"], NO_NICE)
        after = (t["e"] or {}).get("after", [])
        here = S.is_done(sp, root)
        trained[key] = here
        archiving = ("relay_fetch", key) in live
        if not here:
            prequeue(t, after)
            continue
        blocked = False
        for st in after:
            label = f"{st['name']}:{t['lab']}"
            busy = fetching(st, t["names"])
            ok = not busy and st["done"].ok(root, t["names"])
            after_ok[(key, st["name"])] = ok
            if not ok and not blocked:
                if not busy:
                    place(t, st, label, n, archiving, True)
                blocked = True

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
                busy = fetching(st, names)
                if busy or not st["done"].ok(root, names):
                    if not busy:
                        place(dict(key=e["id"], names=names, each=e.get("each") or {}), st, label, n, False, False)
                    break
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
    todo.sort(key=lambda t: (t["nice"], t["key"]))
    return todo, stuck


# ---------------- 输入 ----------------
NEEDS_ERR = {}


def inputs_of(qd, names, each=()):
    """步骤 queue 里的 inputs → 相对数据目录的路径；实验一级的步骤里带 {<参数>}（each 的参数，names 里没有它）的按 <参数>_list
    的每个取值展开。"""
    out = []
    for p in qd["inputs"]:
        ks = [k for k in each if "{" + k + "}" in p and k not in names]
        if ks:
            out += [S.fill(p, dict(names, **dict(zip(ks, vs))), keep=())
                    for vs in itertools.product(*(names.get(f"{k}_list", "").split() for k in ks))]
        else:
            out.append(S.fill(p, names, keep=()))
    return out


def step_needs(cmd):
    """步骤的命令 → 要用的输入（相对数据目录，带 * 的是通配）；不支持 --stage needs 或运行出错 → None（原因见 no_needs）。
    --stage <阶段> 换成 --stage needs:<阶段>；命令开头的 X=… 当环境变量。"""
    tok = shlex.split(cmd)
    if "--stage" not in tok or tok.index("--stage") + 1 >= len(tok):
        return None
    i = tok.index("--stage") + 1
    tok[i] = "needs:" + tok[i]
    env = dict(os.environ)
    while tok and "=" in tok[0] and tok[0].split("=", 1)[0].isidentifier():
        k, v = tok.pop(0).split("=", 1)
        env[k] = v
    r = subprocess.run(tok, cwd=REPO, capture_output=True, text=True, timeout=120, env=env)
    if r.returncode != 0:
        err = (r.stderr.strip().splitlines() or [f"退出码 {r.returncode}"])[-1]
        NEEDS_ERR[cmd] = f"--stage needs 出错（{err}）"
        return None
    return [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]


def no_needs(cmd):
    """step_needs 返回 None 的原因。"""
    return NEEDS_ERR.get(cmd, "命令不支持 --stage needs")


# ---------------- 执行 ----------------
TQ_TIMEOUT = 420  # 比队列锁的最长等待（300 秒）长：等不到锁时拿到退出码，不是超时


def taskq_add(nice, args):
    """加进总队列（box 上本地的 taskq.py，10/31）→ (退出码, 输出)；relay.py 再替执行机领过去。"""
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "add", "--nice", str(nice), "--recipe", "command", *args],
                       cwd=REPO, capture_output=True, text=True, timeout=TQ_TIMEOUT)
    return r.returncode, (r.stdout + r.stderr).strip()


def taskq_remove(keys):
    """从总队列删掉这些（没人领着的）任务 → (退出码, 输出)。"""
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "remove", *keys], cwd=REPO, capture_output=True, text=True,
                       timeout=TQ_TIMEOUT)
    return r.returncode, (r.stdout + r.stderr).strip()


def run(s, dry=False, q=None, tq=None, rm=None):
    """health.py 调用：→ (这次排了的说明列表（只算不排时是"会排 …"）, 卡住的 {问题编号: 说明})。"""
    q = q or refill.q
    tq = tq or taskq_add
    todo, stuck = plan(s)
    live_mode = bool(P.AUTO.get("advance")) and not dry
    done, lines = [], []
    host = M.hub()
    for t in todo:
        if t.get("op") == "unqueue":
            what = f"{t['label']}：{t['why']}：{' '.join(t['qks'])}"
            if live_mode:
                rc, out = (rm or taskq_remove)(t["qks"])
                if rc in (0, proto.EXIT["gone"]):  # 都已经不在队列里
                    lines.append(f"撤了 {what}")
                else:
                    stuck[alert_id("advance", "unqueue", t["key"])] = f"{t['label']}：从任务队列撤掉出错（{out[-200:]}）"
            else:
                lines.append(f"会撤 {what}")
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
                    stuck[alert_id("advance", t["step"], t["key"])] = f"{t['step']} {t['key']}：加进任务队列出错（{out[-200:]}）"
            else:
                done.append(f"会排 任务队列 {t['label']}")
                lines.append(f"会排 {what}")
            continue
        note = "，暂停（HOLD）" if h else ""
        if live_mode:
            try:
                # 先记下来再加任务：加上了、没拿到任务号（断电、ssh 超时）时，下次按标签和创建时间找回
                rec = C.new_record(host, t["label"], t["step"], t["key"], group=t["group"])
                cmd = refill.then_check(t["cmd"], t["label"])
                tid = q(host, f"{t['group']} -p{' --stashed' if h else ''} --label {shlex.quote(t['label'])} -- {shlex.quote(cmd)}")
                C.set_record_id(rec, tid)
                done.append(f"{t['label']}（任务 {tid}{note}）")
                lines.append(f"排了 {t['label']}（{t['group']} 组，任务 {tid}{note}）：{t['cmd']}")
            except Exception as e:  # noqa: BLE001
                lines.append(f"出错 {t['label']}：{e}")
                stuck[alert_id("advance", t["step"], t["key"])] = f"{t['step']} {t['key']}：加任务出错（{e}）"
        else:
            done.append(f"会排 {t['label']}{note}")
            lines.append(f"会排 {t['label']}（{t['group']} 组{note}）：{t['cmd']}")
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


def describe(todo, stuck):
    """plan() 的结果 → 给人看的行。"""
    out = []
    for t in todo:
        if t.get("op") == "taskq":
            out.append(f"会排 {t['label']:28} 任务队列（档位 {t['qnice']}） {' '.join(shlex.quote(a) for a in t['args'])}")
        elif t.get("op") == "unqueue":
            out.append(f"会撤 {t['label']:28} {t['why']}：{' '.join(t['qks'])}")
        else:
            out.append(f"会排 {t['label']:28} {t['group']:9} {t['cmd']}")
    out += [f"没排 {x}" for x in NOTES]
    out += [f"卡住 {alert_msg(v)}" for v in stuck.values()]
    if not todo and not stuck:
        out.append("没有要排的")
    return out


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="训完以后的步骤：只看现在会排什么（真排由健康检查调用 run）")
    ap.add_argument("-n", "--dry-run", "--dry", action="store_true", help="只看（默认就是）")
    ap.parse_args(argv)
    import health as H  # noqa: E402
    print("\n".join(describe(*plan(H.gather()))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
