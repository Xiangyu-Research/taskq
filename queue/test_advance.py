#!/usr/bin/env python3
"""advance.py 的测试：testdata 的实验表，store 是本机（hub 那一项），数据目录指到临时目录；store 上的操作（done、probe、rm）
真跑 site_ops.py。
  1. 训完一份 → 排这一份的第一步；前一步做完 → 下一步；在总队列里的不重排；结果还在拉回的不算做完；
  2. 跨实验：paired/t3b 等 ens/t0 做完才排；FINAL 等它所有的上游；
  3. rerun：输入比 done 文件新 → 删 done 文件（store 上）重做；上游没做完不重做；
  4. temp：用到它的实例都做完 → 在 store 上删中间产物，别的不动；有下游要重做时不删；
  5. HOLD、AUTO["advance"]=False（只写会做什么）、实验表有问题、store 连不上都报对；
  6. 加进总队列真跑一次 taskq.py add（临时的状态目录），总队列里的任务带 step、key 是实例 id。
    python3 queue/test_advance.py
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
os.environ["HOME"] = fixture.tmpdir()
sys.path.insert(0, HERE)
import advance as A  # noqa: E402
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import proto  # noqa: E402
from flow import compile as FC  # noqa: E402
from flow.model import SOURCE  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


DATA = fixture.tmpdir()
M.MACHINES[M.hub()]["data"] = DATA
HUB = M.hub()
# store（hub 那一项，本机）根目录的 code/current 是手搭的代码包：这份核心放在 proto.BUNDLE_CORE，.taskq.toml 指到测试的配置和插件；
# store 的 conda 里的 python3 是跑测试的这个。数据目录由 store_op 带过去（M.data()）
CUR = os.path.join(os.environ["HOME"], "demo_root", proto.ROOT["current"])
os.makedirs(os.path.dirname(os.path.join(CUR, proto.BUNDLE_CORE)))
os.symlink(os.path.dirname(HERE), os.path.join(CUR, proto.BUNDLE_CORE))
with open(os.path.join(CUR, ".taskq.toml"), "w", encoding="utf-8") as f:
    f.write(f"config = {json.dumps(os.environ['TASKQ_CONFIG'])}\nplugins = {json.dumps([fixture.PLUGINS])}\n")
CONDA = fixture.tmpdir()
os.makedirs(os.path.join(CONDA, "bin"))
os.symlink(sys.executable, os.path.join(CONDA, "bin", "python3"))
M.MACHINES[HUB]["conda"] = (CONDA, "base0")
INST = FC.build().instances


def touch(rel, text="", age=0):
    p = os.path.join(DATA, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        f.write(text)
    if age:
        t = time.time() - age
        os.utime(p, (t, t))
    return p


def finish(iid, age=0):
    """把实例的结果放到 store 上（done 的每一项取第一个备选）。"""
    i = INST[iid]
    it = A.done_items(i)
    for d in it["done"]:
        touch(f"{it['out']}/{d[0] if isinstance(d, list) else d}", age=age)


def snap(queue=(), pueue=(), records=()):
    return {"taskq": {HUB: dict(tasks=list(queue))}, "pueue": {HUB: dict(tasks=list(pueue))}, "records": list(records)}


def adds(todo):
    return sorted(t["id"] for t in todo if t["op"] == "add" and not t["queued"])


def src(exp, frag):
    return next(k for k, i in INST.items() if i.scope == SOURCE and i.exp == exp and i.spec["names"].get("frag") == frag)


t00 = src("t0", "0")

# ---- 1. 每个任务的步骤 ----
todo, stuck = A.plan(snap())
ok("什么都没训完：不排", adds(todo) == [] and not stuck)
finish(t00)
todo, stuck = A.plan(snap())
ok("训完一份：只排这一份的第一步（推理）", adds(todo) == ["infer/t0_f0"] and not stuck)
p = next(t for t in todo if t["id"] == "infer/t0_f0")
ok("参数：做法 command 的 key、step、cmd（{data} 留着）、out、done、inputs、档位",
   p["params"]["key"] == "infer/t0_f0" and p["params"]["step"] == "infer" and "{data}/results/" in p["params"]["cmd"]
   and p["params"]["out"] == INST["infer/t0_f0"].outputs[0] and p["params"]["done"] == ["MANIFEST.json"]
   and p["nice"] == -100 and p["params"]["relay"] == "toy_infer" and "--stage needs:infer" in p["params"]["inputs_cmd"])
todo, _ = A.plan(snap(queue=[dict(key="infer/t0_f0", state=proto.S_FREE)]))
ok("已经在总队列里的不重排", adds(todo) == [])
finish("infer/t0_f0")
todo, _ = A.plan(snap())
ok("推理做完 → 评测（lprob 要评测的 report，还不排）", adds(todo) == ["eval/t0_f0"])
rec = dict(machine=HUB, id=7, label="relay_fetch infer/t0_f0", step="relay_fetch", key="infer/t0_f0")
todo, _ = A.plan(snap(pueue=[dict(id=7, label=rec["label"], status="Running")], records=[rec]))
ok("推理的结果还在拉回 store：不算做完，评测不排，推理也不重排", adds(todo) == [])
todo, _ = A.plan(snap(pueue=[dict(id=7, label=rec["label"], status="Success")], records=[rec]))
ok("拉回结束了：照常排", adds(todo) == ["eval/t0_f0"])
finish("eval/t0_f0")
todo, _ = A.plan(snap())
ok("评测做完 → lprob", adds(todo) == ["lprob/t0_f0"])

# ---- 2. 实验一级、跨实验、FINAL ----
for f in "01234":
    for k in (src("t0", f), f"infer/t0_f{f}", f"eval/t0_f{f}", src("t3b", f), f"infer/t3b_f{f}", f"eval/t3b_f{f}"):
        finish(k)
todo, _ = A.plan(snap())
got = adds(todo)
ok("五份都推理完 → ens；评测完 → oof；paired/t3b 等 ens/t0 还不排",
   {"ens/t0", "oof/t3b", "ens/t3b"} <= set(got) and "paired/t3b" not in got and "compare_all" not in got)
finish("oof/t3b")
got = adds(A.plan(snap())[0])
ok("oof/t3b 做完、ens/t0 没做完：paired/t3b 还不排（跨实验的输入）", "paired/t3b" not in got)
finish("ens/t0")
got = adds(A.plan(snap())[0])
ok("ens/t0 做完：paired/t3b 排上", "paired/t3b" in got and "compare_all" not in got)

# ---- 3. rerun（FINAL 的 compare_all）----
ca = INST["compare_all"]
for k in A.Graph(FC.build()).ancestors("compare_all"):
    finish(k, age=7200)
finish("compare_all", age=3600)
todo, _ = A.plan(snap(queue=[dict(key="compare_all", state=proto.S_DONE)]))
ok("compare_all 的输入都比它旧：不重做", not [t for t in todo if t["id"] == "compare_all"])
touch(INST["paired/t3b"].outputs[0] + "/paired.csv")  # 现在写的：比 compare_all 新
todo, _ = A.plan(snap(queue=[dict(key="compare_all", state=proto.S_DONE)]))
rr = [t for t in todo if t["id"] == "compare_all"]
ok("输入比 done 文件新：删 done 文件重做（已经在总队列里，不重加）",
   [t["op"] for t in rr] == ["rerun", "add"] and rr[0]["files"] == [ca.outputs[0] + "/compare_all.rec.json"] and rr[1]["queued"])
todo, _ = A.plan(snap(queue=[dict(key="compare_all", state=proto.S_CLAIMED)]))
ok("正在做的不重做", not [t for t in todo if t["id"] == "compare_all"])
calls = []
did, stuck = A.run(snap(queue=[dict(key="compare_all", state=proto.S_DONE)]), tq=lambda n, p: (calls.append(p["key"]) or (0, "")))
ok("run：在 store 上删掉 done 文件，不往总队列加 compare_all",
   not os.path.exists(os.path.join(DATA, ca.outputs[0], "compare_all.rec.json")) and "compare_all" not in calls and not stuck)
finish("compare_all")
os.remove(os.path.join(DATA, INST["ens/t18"].outputs[0], "report/report.md"))
touch(INST["paired/t3b"].outputs[0] + "/paired.csv")
todo, _ = A.plan(snap(queue=[dict(key="compare_all", state=proto.S_DONE)]))
ok("上游有没做完的（ens/t18）：不重做", not [t for t in todo if t["id"] == "compare_all" and t["op"] == "rerun"])
finish("ens/t18")
finish("compare_all")  # 重做完了（比输入新）

# ---- 4. temp ----
inf = INST["infer/t0_f0"]
users = sorted(A.Graph(FC.build()).users["infer/t0_f0"])
ok("推理的下游：eval、lprob、ens", users == ["ens/t0", "eval/t0_f0", "lprob/t0_f0"])
prob = touch(inf.outputs[0] + "/G-A/a.prob", "x" * 1000)
keep = touch(inf.outputs[0] + "/G-A/a.pred")
ok("有下游还没做完（lprob）：不删", not [t for t in A.plan(snap())[0] if t["op"] == "temp" and t["id"] == "infer/t0_f0"])
finish("lprob/t0_f0")
todo, _ = A.plan(snap())
tp = [t for t in todo if t["op"] == "temp" and t["id"] == "infer/t0_f0"]
ok("下游都做完了：删推理的中间产物", tp and tp[0]["pats"] == [inf.outputs[0] + "/*/*.prob"])
calls = []
A.run(snap(), tq=lambda n, p: (calls.append(p["key"]) or (0, "")))
ok("run：store 上删掉 *.prob，别的文件（预测、done）不动",
   not os.path.exists(prob) and os.path.exists(keep) and os.path.exists(os.path.join(DATA, inf.outputs[0], "MANIFEST.json")))

# ---- 5. HOLD、AUTO、出错 ----
os.remove(os.path.join(DATA, INST["eval/t0_f1"].outputs[0], "report/report.md"))
P.HOLD[:] = ["eval/t0_*"]
todo, _ = A.plan(snap())
ok("HOLD 对上的不排，写进说明", "eval/t0_f1" not in adds(todo) and any("eval/t0_f1" in x and "HOLD" in x for x in A.NOTES))
P.HOLD[:] = []
P.AUTO["advance"] = False
calls = []
did, _ = A.run(snap(), tq=lambda n, p: (calls.append(p["key"]) or (0, "")))
ok("AUTO['advance']=False：不加任务，只写会排什么", not calls and "会排 eval/t0_f1" in did)
P.AUTO["advance"] = True
did, _ = A.run(snap(), dry=True, tq=lambda n, p: (calls.append(p["key"]) or (0, "")))
ok("dry：同样不加", not calls and "会排 eval/t0_f1" in did)
did, stuck = A.run(snap(), tq=lambda n, p: (1, "锁超时"))
ok("加进总队列出错：报出来", any("eval/t0_f1" in v and "锁超时" in v for v in stuck.values()))
saved = A.FC.build
A.FC.build = lambda **kw: type("F", (), dict(errors=["x 的产出 a 同时是 b 的"], instances={}))()
todo, stuck = A.plan(snap())
A.FC.build = saved
ok("实验表有问题：一个都不排，报 advance:flow", not todo and list(stuck) == [proto.alert_id("advance", "flow")])
M.MACHINES[HUB]["data"] = os.path.join(DATA, "nosuch")
todo, stuck = A.plan(snap())
M.MACHINES[HUB]["data"] = DATA
ok("store 上的数据目录不在：done 全算没做完（不报错、什么都不删）", not [t for t in todo if t["op"] in ("temp", "rerun")])
hub_root = M.MACHINES[HUB]["root"]
M.MACHINES[HUB].update(ssh="fake-nohost", root="~/q")
todo, stuck = A.plan(snap())
M.MACHINES[HUB].update(ssh=None, root=hub_root)
ok("store 连不上：不排，报 advance:store", not todo and list(stuck) == [proto.alert_id("advance", "store")])

# ---- 6. 真跑一次 taskq.py add ----
rc, out = A.taskq_add(-100, A.queue_params(INST["eval/t0_f1"]))
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json"], capture_output=True, text=True)
rows = (S.json_in(r.stdout) or {}).get("tasks") or []
row = next((t for t in rows if t["key"] == "eval/t0_f1"), None)
ok(f"taskq.py add 真跑：总队列里有 eval/t0_f1，做法 command、带 step（{rc} {out[-200:]}）",
   rc == 0 and row is not None and row["recipe"] == "command" and (row.get("spec") or {}).get("step") == "eval"
   and row.get("nice") == -100)
rc, out = A.taskq_add(-100, A.queue_params(INST["eval/t0_f1"]))
r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "status", "--json"], capture_output=True, text=True)
ok("同一个 key 再加：不重复", [t["key"] for t in S.json_in(r.stdout)["tasks"]].count("eval/t0_f1") == 1)
sp = S.resolve("command", S.parse_kv(S.kv_args("command", A.queue_params(INST["infer/t0_f0"]))[1:]))
ok("步骤的说明变得成 shell 变量", bool(S.shell_lines(sp, "/d")))

print("全部通过" if not fails else f"{fails} 项失败")
sys.exit(1 if fails else 0)
