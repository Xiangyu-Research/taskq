#!/usr/bin/env python3
"""训完以后的步骤（实验表的 after、group_after、FINAL）怎么往前走：hub 上的健康检查每轮调用 run。

先后由 flow/compile.py 从实验表推出（同一个任务里按顺序，加上"输入落在别的实例的产出里"）。每轮：
  1. 在 store 上看每个实例做完没有（done 的文件都在；结果还在拉回的不算）；
  2. 先要做完的都做完了、自己还没做完、不在总队列里的，加进总队列（做法 command，key = 实例 id，step = 步骤名）；
     放到哪台执行机在派任务时由 refill.choose 定（policy.py 的 PLACE），这里不定；
  3. rerun=True 的步骤：输入里有比它的 done 文件新的，删掉 done 文件（在 store 上）、重新排；
  4. temp：用到它的实例都做完了（结果都在 store 上），在 store 上删掉这些中间产物，记进事件。
HOLD 对上的实例不排；policy.py 的 AUTO["advance"] 是 False 时只写"会排什么"，不改任何东西。

    python3 queue/advance.py      看现在会排什么（不改任何东西；taskq plan 连同送任务一起看）
"""
import fnmatch
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto  # noqa: E402
from proto import alert_id, alert_msg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "monitor"))
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402
import relay_remote as RM  # noqa: E402
import refill  # noqa: E402
from flow import compile as FC  # noqa: E402
from flow.graph import Graph  # noqa: E402
from flow.model import SOURCE  # noqa: E402

NOTES = []  # 这次"为什么没排"的说明（写进 ~/advance.log，不报成问题）
TQ_TIMEOUT = 420  # 比队列锁的最长等待（300 秒）长：等不到锁时拿到退出码，不是超时


def done_items(inst):
    """实例 → store 上 done 操作的一项：dict(out=主目录, done=[[备选, …], …])。"""
    if inst.scope == SOURCE:
        o = S.outputs(inst.spec)
        return dict(out=o["outs"][0], done=o["done"])
    return dict(out=inst.outputs[0], done=[d.split("|") for d in inst.step["done"]])


def fetching(s):
    """结果正在拉回 store 的任务 key（hub 的 pueue 里在跑的 relay_fetch）：拉完以前不算做完。"""
    hub = M.hub()
    out = set()
    for t in (s.get("pueue", {}).get(hub) or {}).get("tasks") or []:
        r = C.identify(hub, t, s.get("records") or [])
        if r and r["step"] == "relay_fetch" and t["status"] in refill.LIVE:
            out.add(r["key"])
    return out


def done_files(i):
    """步骤实例 done 的每个备选（相对数据目录）。"""
    return [f"{i.outputs[0]}/{a}" for d in i.step["done"] for a in d.split("|")]


def queue_params(i):
    """步骤实例 → 加进总队列的参数（做法 command）。{data} 留给执行机展开。"""
    st = i.step
    p = dict(key=i.id, step=st["name"], cmd=S.fill(st["cmd"], i.names), env=st["env"] if len(st["env"]) > 1 else st["env"][0],
             out=i.outputs[0], done=list(st["done"]), inputs=list(i.inputs), relay=st["relay"], device=st["device"],
             fetch=dict(exclude=[], extra=[], also=i.outputs[1:]))
    if st.get("res"):
        p["res"] = dict(st["res"])
    if st.get("out_gb") is not None:
        p["out_gb"] = st["out_gb"]
    if st.get("inputs_cmd"):
        p["inputs_cmd"] = S.fill(st["inputs_cmd"], i.names)
    return p


def plan(s):
    """→ (要做的 [dict(op=add|rerun|temp, id, …)], 卡住的 {问题编号: 说明})。"""
    NOTES.clear()
    hub = M.hub()
    mq = (s.get("taskq") or {}).get(hub)
    if not mq:
        return [], {}
    flow = FC.build(queue_tasks=mq["tasks"])
    if flow.errors:
        more = f"（另有 {len(flow.errors) - 5} 处）" if len(flow.errors) > 5 else ""
        return [], {alert_id("advance", "flow"): "实验表的步骤有问题，这次一个都没排：" + "；".join(flow.errors[:5]) + more}
    g = Graph(flow)
    inst = flow.instances
    queued = {t["key"]: t for t in mq["tasks"]}
    moving = fetching(s)
    try:
        miss = RM.store_op("done", dict(items={k: done_items(i) for k, i in inst.items()}))
    except RuntimeError as e:
        return [], {alert_id("advance", "store"): f"store 上查不了结果（{e}），这次不排"}
    done = {k for k, m in miss.items() if not m and k not in moving}
    reruns = {}  # 实例 id → 要删的 done 文件
    cand = [i for i in inst.values() if i.step and i.step["rerun"] and i.id in done
            and (queued.get(i.id) or {}).get("state") in (None, proto.S_DONE)]
    if cand:
        files = {i.id: done_files(i) for i in cand}
        try:
            pr = RM.store_op("probe", dict(pats=sorted({p for i in cand for p in i.inputs + files[i.id]})))
        except RuntimeError as e:
            pr = {}
            NOTES.append(f"store 上查不了修改时间（{e}），这次不看要不要重做")
        newest = lambda ps: max((pr[p]["newest"] or 0 for p in ps if p in pr), default=0)  # noqa: E731
        for i in cand:
            if pr and newest(i.inputs) > newest(files[i.id]) and g.ancestors(i.id) <= done:
                done.discard(i.id)
                reruns[i.id] = files[i.id]
    todo, stuck = [], {}
    for k in g.order():
        i = inst[k]
        if i.scope == SOURCE or k in done:
            continue
        wait = [d for d in i.deps if d not in done]
        if wait or k in moving:  # 结果正在拉回的：不算做完，也不重排
            continue
        if k in queued and k not in reruns:
            continue
        if any(fnmatch.fnmatch(k, pat) for pat in P.HOLD):
            NOTES.append(f"{k}：HOLD，先不排")
            continue
        if k in reruns:
            todo.append(dict(op="rerun", id=k, files=reruns[k]))
        todo.append(dict(op="add", id=k, nice=i.step["nice"], params=queue_params(i), queued=k in queued))
    for i in inst.values():
        if not (i.step and i.step.get("temp")) or i.id not in done:
            continue
        users = g.users.get(i.id, [])
        if users and all(u in done for u in users) and not g.descendants(i.id) & set(reruns):
            pats = [S.fill(f"{i.outputs[0]}/{t}", i.names) for t in i.step["temp"]]
            todo.append(dict(op="temp", id=i.id, pats=pats))
    return todo, stuck


def taskq_add(nice, params):
    """加进总队列（hub 上本地的 taskq.py）→ (退出码, 输出)。"""
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "add", "--nice", str(nice), "--recipe",
                        *S.kv_args("command", params)], cwd=cfg.REPO, capture_output=True, text=True, timeout=TQ_TIMEOUT)
    return r.returncode, (r.stdout + r.stderr).strip()


def run(s, dry=False, tq=None, store=None):
    """health.py 调用 → (这次做了的说明列表（只看不做时是"会…"）, 卡住的 {问题编号: 说明})。"""
    tq = tq or taskq_add
    store = store or RM.store_op
    todo, stuck = plan(s)
    live = bool(P.AUTO.get("advance")) and not dry
    did, lines = [], []
    for t in todo:
        if t["op"] == "rerun":
            what = f"{t['id']}：删掉 store 上的 done 文件重做（{', '.join(t['files'])}）"
            if live:
                try:
                    store("rm", dict(pats=t["files"]))
                    lines.append(f"重做 {what}")
                except RuntimeError as e:
                    stuck[alert_id("advance", "rerun", t["id"])] = f"{t['id']}：删 done 文件出错（{e}）"
            else:
                lines.append(f"会重做 {what}")
            continue
        if t["op"] == "temp":
            what = f"{t['id']} 的中间产物 {', '.join(t['pats'])}"
            if live:
                try:
                    r = store("rm", dict(pats=t["pats"]))
                    if r["n"]:
                        lines.append(f"删了 {what}：{r['n']} 个，{r['bytes'] / 1e9:.1f} GB")
                except RuntimeError as e:
                    stuck[alert_id("advance", "temp", t["id"])] = f"删 {what} 出错（{e}）"
            else:
                lines.append(f"会删 {what}")
            continue
        if t["queued"]:
            continue  # 重做的：已经在总队列里，删了 done 文件就又能领
        what = f"{t['id']}（档位 {t['nice']}）：{t['params']['cmd']}"
        if live:
            rc, out = tq(t["nice"], t["params"])
            if rc == 0:
                did.append(t["id"])
                lines.append(f"排了 {what}")
            else:
                lines.append(f"出错 {t['id']}：{out[-200:]}")
                stuck[alert_id("advance", "add", t["id"])] = f"{t['id']}：加进总队列出错（{out[-200:]}）"
        else:
            did.append(f"会排 {t['id']}")
            lines.append(f"会排 {what}")
    lines += [f"没排 {x}" for x in NOTES]
    if not dry:
        log(lines, stuck, live)
    return did, stuck


def log(lines, stuck, live):
    """写 ~/advance.log：和上次一样（同样的"会排"、"没排"、卡住）就不重复写。"""
    acted = ("排了", "重做", "删了")
    cur = dict(lines=[ln for ln in lines if not ln.startswith(acted)], stuck=sorted(stuck))
    out = [ln for ln in lines if ln.startswith(acted)]
    if C.changed("advance", cur):
        out += cur["lines"] + [f"卡住 {stuck[k]}" for k in cur["stuck"]]
        if not out:
            out = ["没有要排的"]
    if out:
        import events
        events.lines("advance", out, kind=None if live else "dry")


def describe(todo, stuck):
    """plan() 的结果 → 给人看的行。"""
    out = []
    for t in todo:
        if t["op"] == "add" and not t["queued"]:
            out.append(f"会排 {t['id']:36} 档位 {t['nice']:<5} {t['params']['cmd']}")
        elif t["op"] == "rerun":
            out.append(f"会重做 {t['id']:34} 删掉 {', '.join(t['files'])}")
        elif t["op"] == "temp":
            out.append(f"会删 {t['id']:36} 的中间产物 {', '.join(t['pats'])}")
    out += [f"没排 {x}" for x in NOTES]
    out += [f"卡住 {alert_msg(v)}" for v in stuck.values()]
    if not out:
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
