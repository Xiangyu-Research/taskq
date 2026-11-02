#!/usr/bin/env python3
"""往执行机的缓冲送任务（2026-10-31）：box 是中心，总队列在 box（scripts/queue/taskq.py），有缓冲的执行机（Slurm 的，DICC）
有一份缓冲，是总队列里分给它的任务的原样副本，由那边 refill.py 补交的 Slurm 作业里的 worker（scripts/queue/dicc_worker.sh）领。
RELAY 的执行机（envs/machines.py 的 RELAY，lab1、lab0 等）由 relay.py 管，run_all 转过去。box 的健康检查
（scripts/monitor/health.py，每 30 分钟）调用 run_all。那边没有 BUFFER 标记时（还没改成缓冲）什么都不做。

每次检查，对每台执行机：
  1. 总队列里分给它的（assigned）：
     - box 上训完了（总队列的 done），而且没有在往 box 拷（拉回、归档）→ 删掉缓冲里的记录（别的记录的 after 还引用它时留着），
       再在总队列交还；
     - 总队列里停了 → 缓冲里也停；那边没人领着以后删掉记录、交还；撤销停止后缓冲里也撤销；
     - 缓冲里没有（送的时候出错）→ 再送一次；档位、参数和总队列不一样（set、set-nice）→ 照总队列改；
     - 缓冲里失败了：这台机器上没有进度 → 删掉记录、交还（--skip，这台以后不再分它）；有进度 → 留着、报出来；
       缓冲整个暂停时（3 小时失败 3 次，多半是节点或环境坏了）不交还，只报出来。
  2. 缓冲里有、总队列里没分给它的记录：别的记录的 after 引用的留着（依赖记录）；没人领着的删掉；有人在训的报出来。
  3. 只有这台能跑的（任务说明的 machines 只有它，如 DICC 的推理、评测）：分过去、马上送，不受个数限制。after 里的任务要么已经在
     缓冲里，要么在这台机器上训完了、没有在往那边拷；不在缓冲里的作为依赖记录（停止着，只给 after 看）一起送。
  4. 训练：缓冲里没人领的训练不到 envs/policy.py 的 READY[机器] 个时，按总队列的先后（档位，同档里数据已在那边的先）补上。
     只送数据已到位的：任务说明的 data 每个路径在那边的文件数、字节数都不少于 box 上的，没有 .incomplete，也没有在传；
     缺的排一个传输（box 的 wait 组，scripts/sync/sync_data.sh <机器> push <路径>），同时只传一份。
     别的机器上有进度的、这台跳过的（skips）不送。补不满、而总队列里还有能给它的任务时，持续 60 分钟后报出来。
  这台已经换成接替它的执行机（envs/machines.py 的 replaces，接替的那台 AUTO 开着、这台关着）：分给这台、没人在做的从缓冲里删掉，
  有进度的在总队列里改分给接替的那台（它从老数据目录收编存档，接着训），其余交还。
  执行机连不上：什么都不动，分给它的也不交还；连不上 2 小时以上报出来。那边的代码不是最新的（health 的 code 检查）时不送新任务。
  先在缓冲里删成功，再在总队列交还；分过去后送不进缓冲时马上交还。
总队列里分给了一个不是执行机的名字（执行机换了名字、撤掉了以后留下的）：没有谁会做它，报出来（feed:nobody），交还后照常重新分。
开关 envs/policy.py 的 AUTO[机器]：False 时第 3、4 步只把"会送什么"记进 ~/feed.log。

    python3 scripts/queue/feed.py            看现在会做什么（不改任何东西）
"""
import json
import os
import shlex
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(REPO, "scripts", "monitor"))
sys.path.insert(0, HERE)
import machines as M  # noqa: E402
import policy as P  # noqa: E402
import common as C  # noqa: E402
import taskq  # noqa: E402
import refill  # noqa: E402
import spec as S  # noqa: E402
import recipes  # noqa: E402
from common import write_json  # noqa: E402

STATE = os.path.expanduser(os.environ.get("FEED_STATE", "~/.cache/phd/feed.json"))  # 环境变量只给测试用
LOG = os.path.expanduser(os.environ.get("FEED_LOG", "~/feed.log"))
LOW_MIN = 60   # 补不满持续这么多分钟才报
DOWN_S = 2 * 3600  # 连不上这么久才报
INPUT_RETRY_S = 3600  # 缺输入的任务过这么久撤掉标记重试
COPYING = ("fetch", "archive", "relay_fetch")  # box 上这些步骤在跑时，这个任务的结果还在拷
# 比较两边的数据时不算的文件：临时文件，加上各做法拉回时不拉的（fetch.exclude，box 上本来就没有）
EXCLUDE = ("*.tmp", ".DS_Store", ".incomplete") + tuple(sorted({x for r in recipes.RECIPES.values()
                                                             for x in ((r.get("fetch") or {}).get("exclude") or []) if "{" not in x}))
LIVE = refill.LIVE


def sites():
    """有缓冲的执行机：Slurm 的训练机（DICC，worker 是补交的作业）。"""
    return [n for n, m in M.MACHINES.items() if "train" in m["roles"] and n in P.READY and m["scheduler"] == "slurm"]


def load_state():
    try:
        return json.load(open(STATE))
    except (OSError, ValueError):
        return {}


def save_state(st):
    write_json(STATE, st)


def log(lines):
    if lines:
        with open(LOG, "a") as f:
            f.writelines(f"{time.strftime('%F %T')} {x}\n" for x in lines)


# ---------------- 数据到位没有 ----------------
def data_script(paths, data):
    """bash：在数据目录 data 下，每个路径打印一行"DATA|路径|文件数|字节数"（有 .incomplete 的打印"DATA|路径|inc"），最后 @@END。"""
    ex = " ".join(f"! -name {shlex.quote(x)}" for x in EXCLUDE)
    ps = " ".join(shlex.quote(p) for p in paths)
    return (f"cd {data} 2>/dev/null || exit 3; for p in {ps}; do "
            f'if [[ -e $p/.incomplete ]]; then echo "DATA|$p|inc"; '
            f'elif [[ -e $p ]]; then find -L "$p" -type f {ex} -printf "%s\\n" | awk -v p="$p" '
            "'{n++; b+=$1} END {printf \"DATA|%s|%d|%.0f\\n\", p, n, b}'; "
            f'else echo "DATA|$p|0|0"; fi; done; echo {C.HAVE_END}\n')


def parse_data(out):
    """data_script 的输出 → {路径: (文件数, 字节数) 或 "inc"}；没到 @@END（截断、出错）→ None。"""
    if C.HAVE_END not in (out or ""):
        return None
    d = {}
    for ln in out.splitlines():
        f = ln.split("|")
        if f[0] == "DATA" and len(f) == 3 and f[2] == "inc":
            d[f[1]] = "inc"
        elif f[0] == "DATA" and len(f) == 4 and f[2].isdigit() and f[3].isdigit():
            d[f[1]] = (int(f[2]), int(f[3]))
    return d


def probe(name, paths, run=C.run):
    """box 和这台机器上这些数据的 (文件数, 字节数) → (box, 那边)；读不到的一边是 None。"""
    if not paths:
        return {}, {}
    m = M.MACHINES[name]
    rc, out, _ = run(data_script(paths, M.expand(M.MACHINES[M.hub()]["data"])), None, 600)
    box = parse_data(out) if rc == 0 else None
    rc, out, _ = run(data_script(paths, m["data"]), m["ssh"], 600)
    return box, (parse_data(out) if rc == 0 else None)


def ready_path(p, box, there):
    """这个数据路径在那边到位了没有：文件数、字节数都不少于 box 上的（box 上没有时那边有就算）。"""
    b, t = (box or {}).get(p), (there or {}).get(p)
    if t is None or t == "inc" or b == "inc" or t[0] == 0:
        return False
    return b is None or (t[0] >= b[0] and t[1] >= b[1])


# ---------------- 算 ----------------
def spec(t):
    return t.get("spec") or {}


def box_live(s):
    """box pueue 里活着的步骤 → {(步骤, key)}。"""
    hub, out = M.hub(), set()
    for t in (s["pueue"].get(hub) or {}).get("tasks", []):
        r = C.identify(hub, t, s.get("records") or [])
        if r and t["status"] in LIVE:
            out.add((r["step"], r["key"]))
    return out


def record(t, hold=None):
    r = {k: t.get(k) for k in ("order", "nice", "key", "exp", "recipe")}
    r["params"] = t.get("params") or {}
    if hold is not None:
        r["hold"] = hold
    return r


def differs(c, b):
    return any(c.get(k) != b.get(k) for k in ("order", "nice", "exp", "recipe")) or (c.get("params") or {}) != (b.get("params") or {})


def trained_there(s, name, key):
    return bool(((s.get("src", {}).get(name) or {}).get("tasks", {}).get(key) or {}).get("final"))


def progress_elsewhere(s, key, name):
    return any(refill.progress(s, key, h) for h in s.get("src", {}) if h != name)


def runnable(name, t, oks=None):
    """这台机器能跑这个任务：环境就绪（envs/machines.py 的 ready_envs）、machines 允许、说明没坏。"""
    sp = spec(t)
    envs = M.ready_envs(name, oks)
    return bool(sp) and not t.get("bad") and t["state"] != "bad" and bool(S.envs_of(sp)) and set(S.envs_of(sp)) <= envs and name in (sp.get("machines") or [name])


def candidates(s, name, ctq):
    """可以送去训练的：总队列里 free / stale、没分出去、这台能跑、要卡、这台没跳过、别的机器上没有进度；按（档位，先后）排。"""
    out = [t for t in ctq["tasks"] if t["state"] in ("free", "stale") and not t.get("assigned") and runnable(name, t, envs_ok(s, name))
           and spec(t).get("device", "gpu") == "gpu" and spec(t).get("machines") != [name]
           and name not in (t.get("skipped_by") or []) and not progress_elsewhere(s, t["key"], name)]
    return sorted(out, key=lambda t: (t["nice"], t["order"]))


def only_here(name, ctq, oks=None):
    """只有这台能跑、还没分出去的（free / stale / waiting）。"""
    return [t for t in ctq["tasks"] if t["state"] in ("free", "stale", "waiting") and not t.get("assigned")
            and spec(t).get("machines") == [name] and runnable(name, t, oks) and name not in (t.get("skipped_by") or [])]


def envs_ok(s, name):
    """那台机器上按描述建好、检查过的环境 {名: 哈希}（收集的状态里的 envs_ok；没有 → None）。"""
    return ((s.get("src") or {}).get(name) or {}).get("envs_ok")


def plan(s, name, st, data=None, now=None):
    """→ (动作, 卡住的 {问题编号: 说明}, 说明)。动作 dict(op=release|bremove|bstop|bresume|put|send|sync, key, …)。
    data：(box, 那边) 数据探查结果（probe），None = 这次不送训练（只算要查哪些路径，见 want_paths）。"""
    now = now or time.time()
    hub, L = M.hub(), M.MACHINES[name]["label"]
    ctq = (s.get("taskq") or {}).get(hub)
    btq = (s.get("taskq") or {}).get(name)
    acts, stuck = [], {}
    if not ctq or not ctq.get("enabled"):
        return [], {}, "总队列读不到或没开"
    mine = [t for t in ctq["tasks"] if t.get("assigned") == name]
    if btq is None:
        last = (st.get("last_ok") or {}).get(name, 0)
        if mine and now - last > DOWN_S:
            stuck[f"site:{name}:down"] = (f"{L} 的缓冲读不到{f' {(now - last) / 3600:.0f} 小时' if last else ''}，分给它的 {len(mine)} 个任务"
                                          "留着不动（那边的 worker 照常训）")
        return [], stuck, "连不上"
    if not btq.get("buffer"):
        return [], {}, f"{L} 还没有缓冲（{L} 的 ~/.cache/phd/taskq 里没有 BUFFER）"
    ct = {t["key"]: t for t in ctq["tasks"]}
    bt = {t["key"]: t for t in btq["tasks"]}
    live = box_live(s)
    copying = {k for stp, k in live if stp in COPYING}
    refs = {}  # key → 引用它的缓冲记录
    for t in btq["tasks"]:
        for k in spec(t).get("after") or []:
            refs.setdefault(k, set()).add(t["key"])
    gone = set()  # 这次删掉的缓冲记录

    def removable(k):
        return not (refs.get(k, set()) - gone - {k})

    heir = M.successor(name)  # 接替这台的执行机（machines.py 的 replaces）开着、这台关着：分给这台的都交出去
    heir = heir if heir and P.AUTO.get(heir) and not P.AUTO.get(name) and heir not in P.OFFLINE else None
    for t in mine:
        k, b = t["key"], bt.get(t["key"])
        if t["state"] == "done":
            if k in copying or (b and b.get("worker")):
                continue
            if b and removable(k):
                acts.append(dict(op="bremove", key=k))
                gone.add(k)
            acts.append(dict(op="release", key=k, why="box 上有了"))
            continue
        if heir and t["state"] != "stopped":
            if b and b.get("worker"):
                continue  # 还有人在做：做完或交还以后再交出去
            if b:
                acts.append(dict(op="bremove", key=k))
                gone.add(k)
            if refill.progress(s, k, name):
                acts.append(dict(op="handover", key=k, to=heir, why=f"换成了 {heir}，这个任务在 {L} 上有进度，从存档接着训"))
            else:
                acts.append(dict(op="release", key=k, why=f"换成了 {heir}"))
            continue
        if t["state"] == "stopped":
            if b is None or (b["state"] in ("stopped", "done") and not b.get("worker")):
                if b and removable(k):
                    acts.append(dict(op="bremove", key=k))
                    gone.add(k)
                acts.append(dict(op="release", key=k, why="总队列里停了"))
            elif b["state"] != "stopped" or b.get("held"):
                acts.append(dict(op="bstop", key=k))
            continue
        if b is None:
            acts.append(dict(op="put", key=k, rec=record(t, hold=False), why="缓冲里没有"))
            continue
        if b.get("held") or differs(t, b):
            acts.append(dict(op="put", key=k, rec=record(t, hold=False), why="照总队列改"))
        elif b["state"] == "stopped":
            acts.append(dict(op="bresume", key=k))
        if b["state"] == "failed":
            if btq.get("paused"):
                pass  # 下面整体报
            elif refill.progress(s, k, name):
                stuck[f"site:{name}:fail:{k}"] = (f"{L} 上 {k} 失败 {b.get('failures')} 次，有进度，留在那边；看那边的 "
                                                  f"~/.cache/phd/taskq/failures/ 和 logs/tasks/")
            else:
                acts.append(dict(op="bremove", key=k))
                gone.add(k)
                acts.append(dict(op="release", key=k, skip=f"{L} 上失败 {b.get('failures')} 次", why=f"{L} 上失败、没有进度，{L} 不再分它"))
        elif b["state"] in ("bad", "blocked"):
            stuck[f"site:{name}:fail:{k}"] = f"{L} 缓冲里的 {k} {b['state']}：{b.get('note')}"
        elif b["state"] == "input":  # 那边开训前查到输入不全（taskq.py missing，11/1）：报，过一阵撤掉标记重试（盘一时读不了也会这样）
            stuck[f"site:{name}:input:{k}"] = f"{L} 上 {k} {b.get('note')}；看那边数据目录里的输入"
            if time.time() - (b.get("input_since") or 0) >= INPUT_RETRY_S:
                acts.append(dict(op="binputok", key=k))
    assigned = {t["key"] for t in mine}
    for k, b in bt.items():
        if k in assigned or not removable(k) or k in gone:
            continue
        if b["state"] in ("claimed", "busy"):
            stuck[f"site:{name}:orphan:{k}"] = f"{L} 在训 {k}，但总队列里没分给它；看总队列（taskq.py show {k}）和 {L} 的缓冲"
        else:
            acts.append(dict(op="bremove", key=k, why="总队列里没分给它"))
            gone.add(k)
    if btq.get("paused"):
        stuck[f"site:{name}:fail"] = (f"{L} 的缓冲 3 小时内失败 {btq.get('recent_failures')} 次，暂停领任务（多半是节点或环境坏了；"
                                      "看那边的 ~/.cache/phd/taskq/failures/ 和作业的 .out）")

    # ---- 送新任务 ----
    code = s.get("code") or {}
    if not btq.get("enabled"):
        return acts, stuck, "缓冲的总开关没开"
    if not code.get("main") or (code.get("heads") or {}).get(name) != code["main"]:
        return acts, stuck, f"{L} 的代码不是最新的，先不送新任务"
    inbuf = set(bt) - gone
    for t in only_here(name, ctq, envs_ok(s, name)):
        deps = spec(t).get("after") or []
        extra = []
        for d in deps:
            if d in inbuf:
                continue
            if trained_there(s, name, d) and d not in copying and d in ct:
                extra.append(record(ct[d], hold=True))
            else:
                break
        else:
            acts.append(dict(op="send", key=t["key"], rec=record(t, hold=False), deps=extra, nice=t["nice"]))
            inbuf |= {t["key"]} | {r["key"] for r in extra}
    unclaimed = [t for t in btq["tasks"] if t["key"] not in gone and t["state"] in ("free", "stale") and not t.get("held")
                 and spec(t).get("device", "gpu") == "gpu"]
    need = P.READY.get(name, 0) - len(unclaimed)
    cands = candidates(s, name, ctq)
    low = st.setdefault("low_since", {})
    if need <= 0 or not cands:
        low.pop(name, None)
        return acts, stuck, "" if need <= 0 else "总队列里没有能给它的训练"
    if data is None:
        return acts, stuck, "没查数据"
    box, there = data
    if box is None or there is None:
        return acts, stuck, "数据查不了"
    sync, sync_bad = refill.syncing(s, name)
    moving = bool(sync)
    sent = 0
    for t in cands[:need + 2]:
        paths = spec(t).get("data") or []
        busy = [p for p in paths if refill.tag(p) in sync]
        bad = [p for p in paths if refill.tag(p) in sync_bad]
        miss = [p for p in paths if not ready_path(p, box, there)]
        if not busy and not bad and not miss:
            acts.append(dict(op="send", key=t["key"], rec=record(t, hold=False), deps=[], nice=t["nice"]))
            sent += 1
            if sent >= need:
                break
            continue
        if bad:
            b = sync_bad[refill.tag(bad[0])]
            stuck[f"site:{name}:sync:{refill.tag(bad[0])}"] = (f"给 {L} 传 {bad[0]} 的 box 任务 {b['id']} 出错了（{b['status']}）；"
                                                               f"看 pueue log {b['id']}，处理后 pueue remove {b['id']}（下次检查重传）")
        elif miss and not busy and not moving and all(isinstance((box or {}).get(p), tuple) and box[p][0] > 0 for p in miss):
            acts.append(dict(op="sync", key=t["key"], paths=miss))
            moving = True
    if sent < need:
        low.setdefault(name, now)
        if now - low[name] >= LOW_MIN * 60:
            stuck[f"site:{name}:low"] = (f"{L} 的缓冲只备了 {len(unclaimed) + sent} 个训练（要 {P.READY[name]} 个），总队列里还有 {len(cands)} 个"
                                         f"能给它的，数据没到位（{(now - low[name]) / 60:.0f} 分钟；看 ~/feed.log 和 box 的 wait 组）")
    else:
        low.pop(name, None)
    return acts, stuck, ""


def want_paths(s, name, st):
    """这次要查哪些数据路径（补训练时看的前 READY+2 个候选的 data）。"""
    ctq = (s.get("taskq") or {}).get(M.hub())
    btq = (s.get("taskq") or {}).get(name)
    if not ctq or not btq:
        return []
    out = []
    for t in candidates(s, name, ctq)[:P.READY.get(name, 0) + 2]:
        out += [p for p in spec(t).get("data") or [] if p not in out]
    return out


# ---------------- 做 ----------------
def execute(name, acts, central=None, remote=None, add=None):
    """做动作 → [说明]。出错的那一步写进说明；交还只在缓冲里删成功（或不用删）后做。"""
    central = central or taskq.central
    add = add or refill.add
    L, msgs = M.MACHINES[name]["label"], []

    def buf(args, input=None):
        r = (remote or (lambda a, i: taskq.remote(name, a, env="TASKQ_FEED=1", input=i)))(args, input)
        return (r[0], (r[1] + r[2]).strip()) if r else None
    failed = set()  # 缓冲里没删成的：总队列里先不交还
    for a in acts:
        k = a["key"]
        try:
            if a["op"] == "bremove":
                r = buf(f"remove {shlex.quote(k)}")
                if r is None or r[0]:
                    failed.add(k)
                    msgs.append(f"{L} 缓冲里删 {k} 出错：{(r or [0, '连不上'])[1][-150:]}")
                else:
                    msgs.append(f"{L} 缓冲里删了 {k}" + (f"（{a['why']}）" if a.get("why") else ""))
            elif a["op"] == "release":
                if k in failed:
                    continue
                rc, out, err = central(["release", "--worker", a.get("worker", name), k] + (["--skip", a["skip"]] if a.get("skip") else []))
                msgs.append(f"总队列交还 {k}（{L}，{a['why']}）" if rc == 0 else f"总队列交还 {k} 出错：{(out + err)[-150:]}")
            elif a["op"] == "handover":
                if k in failed:
                    continue
                central(["release", "--worker", name, k])
                rc, out, err = central(["take", "--worker", a["to"], "--assign", k])
                msgs.append(f"总队列里 {k} 改分给 {a['to']}（{a['why']}）" if rc == 0 else f"{k} 改分给 {a['to']} 出错：{(out + err)[-150:]}")
            elif a["op"] == "binputok":
                r = buf("input-ok " + shlex.quote(k))
                msgs.append(f"{L} 缓冲里 {k} 撤掉缺输入的标记，重试" if r and r[0] == 0 else f"{L} 缓冲里 {k} 撤掉缺输入的标记出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
            elif a["op"] in ("bstop", "bresume"):
                r = buf(("stop --why 总队列里停了 " if a["op"] == "bstop" else "resume ") + shlex.quote(k))
                what = "停" if a["op"] == "bstop" else "撤销停止"
                msgs.append(f"{L} 缓冲里{what} {k}" if r and r[0] == 0 else f"{L} 缓冲里{what} {k} 出错：{(r or [0, '连不上'])[1][-150:]}")
            elif a["op"] == "put":
                r = buf("put", json.dumps([a["rec"]]))
                msgs.append(f"{L} 缓冲里更新 {k}（{a['why']}）" if r and r[0] == 0 else f"{L} 缓冲里更新 {k} 出错：{(r or [0, '连不上'])[1][-150:]}")
            elif a["op"] == "send":
                rc, out, err = central(["take", "--worker", name, "--assign", k])
                if rc:
                    msgs.append(f"没分到 {k}：{(out + err).strip()[-150:]}")
                    continue
                r = buf("put", json.dumps(a["deps"] + [a["rec"]]))
                if r is None or r[0]:
                    rc2, out2, err2 = central(["release", "--worker", name, k])
                    msgs.append(f"{k} 送不进 {L} 的缓冲（{(r or [0, '连不上'])[1][-150:]}），交还" + ("" if rc2 == 0 else f"出错：{(out2 + err2)[-100:]}"))
                    continue
                dep = f"，带上依赖记录 {' '.join(d['key'] for d in a['deps'])}" if a["deps"] else ""
                msgs.append(f"送到 {L}：{k}（档位 {a['nice']}{dep}）")
            elif a["op"] == "sync":
                tids = []
                for p in a["paths"]:
                    lab = f"sync_{name}_{refill.tag(p)}"
                    cmd = refill.then_check(f"bash scripts/sync/sync_data.sh {name} push {shlex.quote(p)}", lab)
                    tids.append(add(M.hub(), lab, "sync", p, f"wait -p --label {lab} -- " + shlex.quote(cmd), to=name, group="wait"))
                msgs.append(f"{k} 的数据还没到 {L}，传 {' '.join(a['paths'])}（box 任务 {' '.join(map(str, tids))}）")
        except Exception as e:  # noqa: BLE001
            msgs.append(f"{L} {a['op']} {k} 出错：{e}")
    return msgs


def describe(acts):
    return "；".join(f"{a['op']} {a['key']}" for a in acts)


def run_one(s, name, dry=False, st=None, data_run=None):
    st = load_state() if st is None else st
    btq = (s.get("taskq") or {}).get(name)
    if btq is not None:
        st.setdefault("last_ok", {})[name] = time.time()
    paths = want_paths(s, name, st)
    data = probe(name, paths, **({"run": data_run} if data_run else {})) if paths else ({}, {})
    acts, stuck, why = plan(s, name, st, data)
    fresh = {"send", "sync"}
    upkeep = [a for a in acts if a["op"] not in fresh]
    new = [a for a in acts if a["op"] in fresh]
    L = M.MACHINES[name]["label"]
    if dry:
        return ([f"{L}：会 {describe(acts)}" if acts else f"{L}：不动" + (f"（{why}）" if why else "")], stuck)
    msgs = execute(name, upkeep)
    if P.AUTO.get(name):
        msgs += execute(name, new)
    elif new and C.changed(f"feed_{name}", describe(new)):
        msgs.append(f"（开关没开）{L} 会：{describe(new)}")
    if not msgs and why and C.changed(f"feed_{name}_why", why):
        msgs.append(f"{L} 不送新任务：{why}")
    log(msgs)
    return msgs, stuck


def nobody(ctq):
    """总队列里分给了不是执行机的名字的（执行机改名、撤掉以后留下的）→ {问题编号: 说明}。"""
    own = set(sites()) | set(M.RELAY)
    by = {}
    for t in (ctq or {}).get("tasks") or []:
        if t.get("assigned") and t["assigned"] not in own and t["state"] != "done":
            by.setdefault(t["assigned"], []).append(t["key"])
    return {f"feed:nobody:{w}": (f"总队列里 {len(ks)} 个任务分给了 {w}，但 {w} 不是执行机，没有谁会做（{ks[0]}…）；"
                                 f"交还：python3 scripts/queue/taskq.py release --worker {w} <key>…") for w, ks in by.items()}


def run_all(s, dry=False, relay=True):
    """health.py 调用 → (说明, 卡住的 {问题编号: 说明})。送任务的唯一入口（10/31）：relay=True 时先处理 SITES 里 relay=True 的执行机
    （scripts/queue/relay.py，细节记 ~/relay.log，这里记一行指过去），再给有缓冲的执行机（sites()）送任务。"""
    events, stuck = [], {}
    if relay:
        try:
            import relay as RL  # scripts/queue/relay.py
            ev, st_ = RL.run_all(s, dry=dry)
        except Exception as e:  # noqa: BLE001
            ev, st_ = [], {"relay:error": f"scripts/queue/relay.py 出错：{type(e).__name__}: {e}"[:300]}
        events += ev
        stuck.update(st_)
        if ev and not dry:
            log([f"执行机 {'、'.join(sorted(M.RELAY))}（relay.py）：{len(ev)} 条，见 ~/relay.log"])
    st = load_state()
    for name in sites():
        if name in P.OFFLINE:
            continue
        try:
            ev, st_ = run_one(s, name, dry, st)
        except Exception as e:  # noqa: BLE001
            ev, st_ = [], {f"feed:{name}": f"scripts/queue/feed.py（{name}）出错：{type(e).__name__}: {e}"[:300]}
        events += ev
        stuck.update(st_)
    stuck.update(nobody((s.get("taskq") or {}).get(M.hub())))
    if not dry:
        save_state(st)
    return events, stuck


if __name__ == "__main__":
    import argparse
    import health as H  # noqa: E402
    ap = argparse.ArgumentParser(description="往执行机送任务（health.py 每次检查调用）；不带参数时也只看、不改")
    ap.add_argument("--dry", action="store_true", help="只打印会做什么（默认就是）")
    ap.add_argument("--no-relay", action="store_true", help="不处理 relay.py 管的执行机")
    a = ap.parse_args()
    ev, st = run_all(H.gather(), dry=True, relay=not a.no_relay)
    print("\n".join(ev + [f"卡住：{v}" for v in st.values()]))
