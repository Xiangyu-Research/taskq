"""按流程（flow/compile.py 编出的实例和先后）算下一步（2026-11-05，P2）：和 advance.plan 的输出一样（op、step、key、label、cmd、
nice、排队参数、after），由配置文件夹里 policy.py 的 ENGINE 决定用哪个：
  advance（默认）只用 advance.plan；shadow 两个都算、照 advance 的排，差别写进事件日志（source flow，kind engine_diff）；dag 用这里的。
放在哪里（PLACE、源头任务所在的执行机、queue:<机器>、放不下）、提前排、撤掉提前排的，照 advance.py 原样搬过来；不同的只有"该做哪个"：
  实例的先后都做完了、自己没做完、没在拷回 → 排它；源头没做完 → 提前排它后面的；on_change=rerun 的看上游有没有更新的产出。"""
import json
import os
import sys
from datetime import datetime
from fnmatch import fnmatch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import advance as A  # noqa: E402  先 import：它设好 sys.path、读配置
import cfg  # noqa: E402
import proto  # noqa: E402
from proto import alert_id, S_CLAIMED, S_DONE, S_WAITING, S_FREE  # noqa: E402
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import experiments as E  # noqa: E402
import refill  # noqa: E402
import shift  # noqa: E402
import common as C  # noqa: E402
from flow.compile import compile_legacy  # noqa: E402
from flow.model import EACH, GATHER, GLOBAL, SOURCE  # noqa: E402

NO_NICE = A.NO_NICE
NOTES = []


def mode():
    m = getattr(P, "ENGINE", "advance")
    if m not in ("advance", "shadow", "dag"):
        raise ValueError(f"policy.py ENGINE = {m!r}：只能是 advance、shadow、dag")
    return m


def plan(s, root=None, flow=None):
    """→ (要排的, 卡住的)，和 advance.plan 一样的格式。"""
    root = root or A.bt()
    hub = M.hub()
    ptasks = (s["pueue"].get(hub) or {}).get("tasks")
    NOTES.clear()
    if ptasks is None:
        return [], {}
    queue_tasks = [t for t in ((s.get("taskq") or {}).get(hub) or {}).get("tasks", []) if not proto.is_smoke(t["key"])]
    nice = {t["key"]: t["nice"] for t in queue_tasks}
    py = A.python()
    flow = flow or compile_legacy(queue_tasks=queue_tasks, data=root, python=py)
    recs = s.get("records") or []
    try:
        A.check_policy({r.name for r in flow.rules.values() if r.scope in (EACH, GATHER)})
    except ValueError as e:
        return [], {alert_id("advance", "policy"): f"{e}。训完以后的步骤这次一个都没排；改好、提交推送后下一次检查自动恢复"}

    live, done = set(), {}
    for t in ptasks:
        r = C.identify(hub, t, recs)
        if not r or r["step"] in ("train", "sync"):
            continue
        sk = (r["step"], r["key"])
        if t["status"] in A.LIVE_ST:
            live.add(sk)
        else:
            done.setdefault(sk, []).append(t)
    inst = flow.instances
    rerun = {r.name for r in flow.rules.values() if r.on_change == "rerun"}
    todo, stuck = [], {}
    qkeys = {t["key"] for t in queue_tasks}
    tq_tasks = {x["key"]: x for x in queue_tasks}
    relay_sites = set(M.RELAY)

    # ---------------- 下面到 place 为止照 advance.plan 原样 ----------------
    def want(step, key, label, cmd, group, n=NO_NICE):
        sk = (step, key)
        if sk in live:
            return
        prev = sorted(done.get(sk, []), key=lambda t: t["id"])
        last_ok = max((i for i, t in enumerate(prev) if t.get("result") == "Success"), default=-1)
        bad = [t for t in prev[last_ok + 1:] if t.get("result") != "Success"]
        ok_empty = last_ok >= 0 and step not in rerun
        if bad or ok_empty:
            t = bad[-1] if bad else prev[last_ok]
            why = f"任务 {t['id']} {t['status']}" if bad else f"任务 {t['id']} 成功跑完，但结果还是不全"
            stuck[alert_id("advance", step, key)] = (f"{step} {key}：{why}，不再自动重排（看 pueue log {t['id']}；处理好以后 "
                                              f"pueue remove {t['id']}，下一次检查会重排）")
            return
        todo.append(dict(step=step, key=key, group=group, label=label, cmd=cmd, nice=n))

    def fetching(st, names):
        qd = st.get("queue")
        return bool(qd) and ("relay_fetch", S.fill(qd["key"], names, keep=())) in live

    def queue_item(t, st, label, n, mach, after=None, inputs=None):
        qd = st["queue"]
        q = {}
        for k, v in qd.items():
            if k not in ("nice", "inputs", "disk", "out_disk"):
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
        qd = st.get("queue")
        if not qd:
            return "实验表里这一步没有 queue"
        if wait:
            return True
        mach = tg["machine"]
        if mach in M.RELAY:
            cmd = S.fill(st["cmd"], t["names"], keep=())
            needs = A.step_needs(cmd)
            if needs is None and "inputs" in qd:
                needs = A.inputs_of(qd, t["names"], t.get("each") or ())
            todo.append(queue_item(t, st, label, n, mach, inputs=needs))
            return True
        todo.append(queue_item(t, st, label, n, mach))
        return True

    def site_of(h):
        if h in relay_sites and not shift.limited(h):
            return h
        ssh = (M.RELAY.get(h) or {}).get("ssh")
        return shift.day_site(ssh) if ssh else None

    def assigned_to(key):
        x = tq_tasks.get(key) or {}
        if x.get("state") != S_CLAIMED:
            return None
        return x.get("assigned") or None

    def runner_of(key):
        h = assigned_to(key)
        if h:
            return h
        r = (tq_tasks.get(key) or {}).get("assigned")
        return r if r in relay_sites else None

    def too_big(m, st):
        return refill.too_big(m, (st or {}).get("queue"))

    def where(t, tg, per_task, st=None):
        if tg["kind"] == "queue":
            m = tg["machine"]
            if m in P.OFFLINE:
                return f"{m} 暂停检查（{P.OFFLINE[m]}）"
            if m in M.RELAY and m not in M.MACHINES and not P.AUTO.get(m):
                return f"{m} 的 AUTO 关着"
            return too_big(m, st) or tg
        if tg["kind"] != A.NEAR:
            return tg
        if not per_task:
            return "不是每个任务的步骤"
        h = runner_of(t["key"])
        site = site_of(h)
        if not site:
            return f"训这个任务的 {h} 不是执行机" if h else "找不到训这个任务的机器"
        return too_big(site, st) or dict(tg, kind="queue", machine=site, near=True)

    def prequeue(t, after):
        h = site_of(assigned_to(t["key"]))
        tx = tq_tasks.get(t["key"]) or {}
        gone, cut = [], False
        for st in after if tx.get("state") != S_DONE else ():
            qk = S.fill(st["queue"]["key"], t["names"], keep=()) if st.get("queue") else None
            x = tq_tasks.get(qk) if qk else None
            if not cut and h:
                tg = next((w for w in (where(t, y, True, st) for y in A.targets(st["name"], st["group"])) if not isinstance(w, str)), None)
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
            tg = next((w for w in (where(t, y, True, st) for y in A.targets(st["name"], st["group"])) if not isinstance(w, str)), None)
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
        key, names = t["key"], t["names"]
        sk = (st["name"], key)
        cmd = S.fill(st["cmd"], names, keep=())
        if sk in live:
            return
        qd = st.get("queue")
        qk = S.fill(qd["key"], names, keep=()) if qd else None
        if qk in qkeys:
            o = S.outputs(S.fill({k: qd.get(k) for k in ("out", "done", "fetch")}, names, keep=()))
            out = os.path.join(root, o["outs"][0])
            if S.done_with(o, lambda a: os.path.exists(os.path.join(out, a)), empty=False):
                stuck[alert_id("advance", st["name"], key)] = f"{label}：任务队列的 {qk} 做完、拉回了，但 hub 上核对还是没做完（{out}）"
            return
        if sk in done:
            want(st["name"], key, label, cmd, st["group"], n)
            return
        why = []
        for tg in A.targets(st["name"], st["group"]):
            w = where(t, tg, per_task, st)
            if isinstance(w, str):
                r = w
            elif w["kind"] == "hub":
                want(st["name"], key, label, cmd, w["group"], n)
                return
            elif w.get("near"):
                r = f"{w['machine']} 上在拉回这个任务" if archiving else to_queue(t, st, label, n, False, w)
            else:
                r = to_queue(t, st, label, n, archiving, w)
            if r is True:
                return
            why.append(f"{tg['text']}：{r}")
        NOTES.append(f"{label}：现在没有能用的地方（{'；'.join(why)}）")

    # ---------------- 该做哪个：按实例的先后 ----------------
    ok = {}  # 实例 id → 做完没有（只算到用得上的）
    each_of = {}
    for i in inst.values():
        if i.task:
            each_of.setdefault(i.task, []).append(i)

    def task_dict(i):
        names = each_of[i.id][0].names if i.id in each_of else dict(i.spec.get("names") or {}, data=root, python=py)
        return dict(key=i.key, names=names, lab=i.lab)

    for i in inst.values():  # 源头和它的每个任务的步骤
        if i.rule not in flow.rules or flow.rules[i.rule].scope != SOURCE:
            continue
        mine = each_of.get(i.id, [])
        t = task_dict(i)
        after = [x.step for x in mine]
        n = nice.get(i.key, NO_NICE)
        ok[i.id] = S.is_done(i.spec, root)
        if not ok[i.id]:
            prequeue(t, after)
            continue
        archiving = ("relay_fetch", i.key) in live
        for x in mine:  # 自己的判断都算（实验一级的 needs 要看），先后都做完的才排
            st = x.step
            busy = fetching(st, x.names)
            ok[x.id] = not busy and st["done"].ok(root, x.names)
            if not ok[x.id] and not busy and all(ok.get(d) for d in x.deps):
                place(dict(t, names=x.names), st, x.id, n, archiving, True)

    newest, todo_gather = 0, []
    watch = {w for r in flow.rules.values() for w in r.watch}
    for i in inst.values():  # 实验一级：先后都做完了才看自己
        if flow.rules[i.rule].scope != GATHER or not all(ok.get(d) for d in i.deps):
            continue
        st = i.step
        busy = fetching(st, i.names)
        ok[i.id] = not busy and st["done"].ok(root, i.names)
        if not ok[i.id]:
            if not busy:
                n = min([nice.get(d, NO_NICE) for d in i.deps if d in inst and flow.rules[inst[d].rule].scope == SOURCE] or [NO_NICE])
                place(dict(key=i.key, names=i.names, each=i.each or {}), st, i.id, n, False, False)
        elif i.rule in watch:
            newest = max(newest, st["done"].mtime(root, i.names))

    finals = [i for i in inst.values() if flow.rules[i.rule].scope == GLOBAL]
    up = set()  # on_change 的上游里实验一级的规则：它们还有在排、在跑的就先不重跑
    for f in finals:
        for d in f.deps:
            up.add(inst[d].rule)
            up.update(inst[x].rule for x in inst[d].deps if x in inst and flow.rules[inst[x].rule].scope == GATHER)
    pending = any(t["step"] in up for t in todo) or any(sk[0] in up for sk in live)
    for f in finals:
        st, names = f.step, f.names
        cmd = S.fill(st["cmd"], names, keep=())
        if flow.rules[f.rule].on_change == "rerun":
            if newest > st["done"].started(root, names) and not pending:
                want(f.rule, "all", f.id, cmd, st["group"])
        elif not st["done"].ok(root, names):
            want(f.rule, "all", f.id, cmd, st["group"])
    todo.sort(key=lambda t: (t["nice"], t["key"]))
    return todo, stuck


# ---------------- 影子对比 ----------------
def norm(todo, stuck, notes=()):
    """→ 可比较的集合：(op, step, key, label, group, cmd, args, after, qks) + 卡住的编号 + 没排的说明。"""
    out = set()
    for t in todo:
        out.add(("todo", t.get("op", "hub"), t["step"], t["key"], t["label"], t.get("group"), t.get("cmd", ""),
                 json.dumps(t.get("args"), ensure_ascii=False), t.get("nice"), json.dumps(sorted(t.get("qks") or []))))
    out |= {("stuck", k) for k in stuck}
    out |= {("note", x) for x in notes}
    return out


def shadow(s, a_todo, a_stuck, budget=60):
    """ENGINE=shadow：同一份状态再用这里算一遍，和 advance 的比；differences go into the event log (source flow, kind engine_diff) → (same, different)。"""
    t0 = datetime.now()
    a = norm(a_todo, a_stuck, A.NOTES)
    b = norm(*plan(s), NOTES)
    diff = sorted(a ^ b)
    if diff:
        import events
        events.add_many(events._row("flow", f"only {'advance' if x in a else 'engine'}: {x}", kind="engine_diff",
                                    data=dict(only="advance" if x in a else "engine", item=x))
                        for x in diff)
    if (datetime.now() - t0).total_seconds() > budget:
        NOTES.append(f"引擎的影子对比用了 {(datetime.now() - t0).total_seconds():.0f} 秒")
    return len(a & b), len(diff)


# ---------------- 反事实回放 ----------------
def replay(n=200, seed=0, root="/replay", s=None, flow=None):
    """随机假设哪些任务训完、哪些步骤做完（不碰盘：做完的判断、--stage needs 都换成假的）、hub 上哪些步骤在跑或失败过、
    总队列里哪些任务分给了哪台执行机，每一种都用 advance.plan 和这里的 plan 各算一遍 → [不一样的 dict(case, only, item)]。
    s：状态（默认空的 hub，总队列 = 实验表展开的任务）。"""
    import random
    import copy
    import steps as STEPS
    rnd = random.Random(seed)
    hub = M.hub()
    if s is None:
        s = dict(pueue={hub: dict(tasks=[])}, taskq={hub: dict(tasks=[])}, records=[])
        s["taskq"][hub]["tasks"] = [dict(key=k, nice=rnd.choice([-100, 0, 50]), state="free", spec=t["spec"], recipe=t["recipe"],
                                         params=t.get("params"), exp=t["exp"]) for k, t in E.tasks().items()]
    flow0 = flow or compile_legacy(queue_tasks=s["taskq"][hub]["tasks"], data=root, python=A.python())
    inst = list(flow0.instances.values())
    classes = {type(i.step["done"]) for i in inst if i.step and i.step.get("done") is not None}
    sites = sorted(M.RELAY) or [hub]
    DONE, MT, ST, TRAINED = set(), {}, {}, set()
    saved = [(c, c.ok, c.mtime, c.started) for c in classes]
    saved_is_done, saved_needs = S.is_done, A.step_needs
    out = []
    try:
        for c in classes:
            c.ok = lambda self, r, names: self.where(r, names) in DONE
            c.mtime = lambda self, r, names: MT.get(self.where(r, names), 0)
            c.started = lambda self, r, names: ST.get(self.where(r, names), 0)
        S.is_done = lambda sp, r: sp["key"] in TRAINED
        A.step_needs = lambda cmd: ["needs/" + str(abs(hash(cmd)) % 1000)]
        for case in range(n):
            DONE.clear(); MT.clear(); ST.clear(); TRAINED.clear()
            p_tr, p_ok = rnd.random(), rnd.random()
            sc = copy.deepcopy(s)
            ptasks, recs = [], []
            for i in inst:
                if i.spec is not None:
                    if rnd.random() < p_tr:
                        TRAINED.add(i.key)
                    continue
                d = i.step.get("done") if i.step else None
                if d is None:
                    continue
                w = d.where(root, i.names)
                if rnd.random() < p_ok:
                    DONE.add(w)
                MT[w] = rnd.randint(1, 100)
                ST[w] = rnd.randint(1, 100)
                x = rnd.random()
                if x < 0.08:  # hub 上在跑 / 做过
                    tid = len(ptasks)
                    st = rnd.choice(["Running", "Queued", "Done", "Done"])
                    ptasks.append(dict(id=tid, label=i.id, status=st, result=rnd.choice(["Success", "Failed"]) if st == "Done" else None))
                    recs.append(dict(machine=hub, id=tid, label=i.id, step=i.rule, key=i.key))
                elif x < 0.10 and i.qk:
                    tid = len(ptasks)
                    ptasks.append(dict(id=tid, label=f"relay_fetch:{i.qk}", status="Running"))
                    recs.append(dict(machine=hub, id=tid, label=f"relay_fetch:{i.qk}", step="relay_fetch", key=i.qk))
            for q in sc["taskq"][hub]["tasks"]:
                x = rnd.random()
                if x < 0.3:
                    q.update(state=S_CLAIMED, assigned=rnd.choice(sites))
                elif x < 0.4:
                    q.update(state=S_DONE, assigned=rnd.choice(sites))
            for i in inst:  # 先前提前排过的后续步骤
                if i.qk and i.task and rnd.random() < 0.1:
                    sc["taskq"][hub]["tasks"].append(dict(key=i.qk, nice=0, state=rnd.choice([S_WAITING, S_FREE]),
                                                          spec=dict(machines=[rnd.choice(sites)])))
            sc["pueue"][hub]["tasks"] = ptasks
            sc["records"] = recs
            a = norm(*A.plan(sc, root=root), A.NOTES)
            b = norm(*plan(sc, root=root), NOTES)
            for x in sorted(a ^ b):
                out.append(dict(case=case, only="advance" if x in a else "engine", item=x))
    finally:
        for c, o, m, st in saved:
            c.ok, c.mtime, c.started = o, m, st
        S.is_done, A.step_needs = saved_is_done, saved_needs
    return out
