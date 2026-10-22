#!/usr/bin/env python3
"""按状态推进训练完以后的事（2026-10-18）：box 上的健康检查（scripts/monitor/health.py，每 30 分钟）调用。
每次对每个任务看文件，算出它走到了哪一步，下一步没在跑就在 box 的 pueue 里启动它；不提前排链（不用 --after）。
步骤和判断都从实验表（research/*/experiments.py，经 scripts/queue/experiments.py 读）和任务说明（scripts/queue/spec.py）来，
这里不认识具体模型。

每个任务（实验表里的，加上队列里不属于任何实验的）：
  fetch    别的机器上训完了（说明里的 done），box 上还没有 → scripts/queue/fetch_task.sh <机器> <key>（wait 组）。
  archive  pueue 训练机（lab）上训完、拉回 box 以后，那边还没归档（没有 ARCHIVED），做法有 archive、实验没写 archive=False
           → scripts/queue/archive_task.sh <机器> <key>（wait 组：备份、核对后删那边的大文件）
  after    实验表里每折的步骤（RQ1：推理 → 评测），按顺序做第一个没做完的
一个实验所有任务都在 box 上训完以后：group_after 的每一串步骤，按顺序做第一个没做完的（needs=<每折的步骤> 的要每折都做完那一步）。
最后：FINAL 的步骤（RQ1：compare_all）。有 rerun_if_newer 的，在列出的步骤有比它开始时更新的结果、实验一级的步骤都没有在排 / 在跑时重跑。
队列里不属于任何实验的任务只拉回、归档。

在不在跑：box pueue 里的任务，按命令或标签认出是哪一步、哪个任务：拉回、归档按命令（带源机器）；其他步骤的命令和这里会生成的
  一样、或者标签一样就算；10/18 前的写法（fetch_fold.sh、archive_fold.sh、不带 --stage 的 run_predict_eval.sh 等）也认得。
失败过的不自动重排：pueue 里这一步最后一次成功之后有失败 / 被停掉的任务，或者成功跑完了但结果还是不对（rerun_if_newer 的除外），
  就只报出来（健康检查"需要处理"），看过 pueue log、处理好以后 pueue remove 那个任务，下一次就会重排。
  拉回按源机器认：以前预先排的、在等另一台机器的拉回任务不算，报出来（可以删掉）。
影子模式：没有 ~/.cache/phd/advance/LIVE 时只把"会排什么"写进 ~/advance.log，不真排。
暂缓：~/.cache/phd/advance/HOLD 里每行一个标签的通配式（fnmatch，# 开头是注释），对上的步骤照常排，但加成暂停的
  （pueue add --stashed），不开始跑；要跑时 pueue enqueue <任务号>。暂停的任务算在排，不会再排一次。
分到别的机器（2026-10-22）：~/.cache/phd/advance/OFFLOAD 里每行一个标签的通配式，对上的每折步骤不在 box 上跑，分到
  machines.py 里有 offload 组的机器（lab 0 号卡）。行尾加 idle 的只在那个组空着时分过去（box 上排着、还没开始的也挪过去，
  一次挪一个），否则照常在 box 上跑。步骤要支持 --stage needs（把命令里的 --stage <步骤> 换成 --stage needs，打印要用的输入，
  相对数据目录）。每次检查按状态做下一件事：
    缺输入         → box wait 组 push:<标签>：scripts/queue/offload_io.sh push（文件夹经隧道、文件用 rsync；那边的盘要留够）
    输入齐了       → 那边 offload 组排 <标签>（命令里的 {data}、{python} 换成那边的）
    那边成功跑完   → box wait 组 pull:<标签>：offload_io.sh pull（拉回结果文件夹 = done 判断的文件所在的文件夹，按校验和核对）
    box 上判断做完 → 删掉那边的结果文件夹和自动传过去的模型文件（别的分出去的步骤还要用的留着；测试集的输入文件夹留着）
  分出去的步骤记在 ~/.cache/phd/advance/offload.json；那边失败、拉回失败或拉回后还是没做完的不重排，报出来。
任务的标签：每折 <步骤>:<实验 id>_f<fold>（如 infer:t14met_f3），实验一级 <步骤>:<实验 id>，最后的 <步骤>；
  不属于实验的任务 <步骤>:<key>。

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
sys.path.insert(0, HERE)
import machines as M  # noqa: E402
import spec as S  # noqa: E402
import experiments as E  # noqa: E402

STATE_DIR = os.path.expanduser("~/.cache/phd/advance")
LOG = os.path.expanduser(os.environ.get("ADVANCE_LOG", "~/advance.log"))  # 环境变量只给测试用
LIVE_ST = ("Running", "Queued", "Stashed", "Paused", "Locked")
ARCHIVE_FROM = [n for n in M.names("pueue") if "train" in M.MACHINES[n]["roles"]]  # 训完要备份、删大文件的机器（lab）
NO_NICE = 10 ** 6
OFFLOAD_OUT_GB = 15  # 分出去的一步在那边最多写多少（一折的预测 7–12 GB）


def bt():
    return os.environ.get("ADVANCE_BT") or M.expand(M.MACHINES[M.hub()]["data"])  # 环境变量只给测试用


def python(name=None):
    base, env = M.MACHINES[name or M.hub()]["conda"]
    return f"{M.expand(base) if not name else base}/envs/{env}/bin/python"


# ---------------- 认命令 ----------------
def step_of(cmd):
    """pueue 任务的命令 → [(步骤, key)]：拉回、归档（fetch_task.sh、archive_task.sh），和 10/18 前的写法。认不出 → []。
    拉回的步骤带源机器：fetch@<机器>。10/18 前的实验一级步骤（oof、paired、ens）的 key 是 "数据集/训练器"，plan() 再换成实验 id。"""
    try:
        tok = shlex.split(cmd)
    except ValueError:
        return []

    def model_key(path):  # …/<数据集>/<训练器>__<plans>[/…] → (数据集, 训练器)
        parts = path.rstrip("/").split("/")
        for i, p in enumerate(parts):
            if p.startswith("Dataset") and i + 1 < len(parts) and "__" in parts[i + 1]:
                return p, parts[i + 1].split("__")[0]
        return None

    for i, t in enumerate(tok):
        b = os.path.basename(t)
        rest = tok[i + 1:]
        if b in ("fetch_task.sh", "archive_task.sh") and len(rest) >= 2:
            return [(f"fetch@{rest[0]}" if b == "fetch_task.sh" else "archive", rest[1])]
        # 以下是 10/18 前的写法（box pueue 里的旧任务）
        if b in ("fetch_fold.sh", "archive_fold.sh") and len(rest) >= 3:
            mk = model_key(rest[1])
            step = f"fetch@{rest[0]}" if b == "fetch_fold.sh" else "archive"
            return [(step, f"{mk[0]}/{mk[1]}/{rest[2]}")] if mk else []
        if b == "archive_lab_fold.sh" and len(rest) >= 2:  # 10/16 前的写法
            mk = model_key(rest[0])
            return [("archive", f"{mk[0]}/{mk[1]}/{rest[1]}")] if mk else []
        if b == "run_predict_eval.sh":
            stage = None
            if rest[:1] == ["--stage"] and len(rest) >= 2:
                stage, rest = rest[1], rest[2:]
            if len(rest) < 2 or not model_key(rest[0]):
                return []
            ds, tr = model_key(rest[0])
            fs = rest[1].split()
            if len(fs) > 1:
                return [("ens", f"{ds}/{tr}")]
            return [(s, f"{ds}/{tr}/{fs[0]}") for s in ([stage] if stage else ["infer", "eval"])]
        if b == "oof_eval.py" and rest:
            mk = model_key(rest[0])
            return [("oof", f"{mk[0]}/{mk[1]}")] if mk else []
        if b == "paired_compare.py" and "--out" in rest:
            out = rest[rest.index("--out") + 1] if rest.index("--out") + 1 < len(rest) else ""
            mk = model_key(out)
            return [("paired", f"{mk[0]}/{mk[1]}")] if mk and out.rstrip("/").endswith("oof/paired") else []
        if b == "compare_all.py":
            return [("compare_all", "all")]
    return []


# ---------------- 候选步骤 ----------------
def candidates(root, py, queue_tasks):
    """所有任务、所有步骤（不管现在该不该做）→ (任务列表, 实验列表, 最后的步骤, {命令: (步骤, key)}, {标签: (步骤, key)},
    {"数据集/训练器": 实验 id})。任务：dict(key, exp, e, spec, names, lab)；不属于实验的 e = None。"""
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
    by_cmd, by_label, legacy = {}, {}, {}
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
        if "dataset" in names and "trainer" in names:
            legacy[f"{names['dataset']}/{names['trainer']}"] = e["id"]
        for chain in e["group_after"]:
            for st in chain:
                by_cmd[S.fill(st["cmd"], names, keep=())] = (st["name"], e["id"])
                by_label[f"{st['name']}:{e['id']}"] = (st["name"], e["id"])
    for f in final:
        by_cmd[S.fill(f["cmd"], dict(data=root, python=py), keep=())] = (f["name"], "all")
        by_label[f["name"]] = (f["name"], "all")
    return tasks, groups, final, by_cmd, by_label, legacy


# ---------------- 算下一步 ----------------
def plan(s, root=None):
    """→ (要排的 [dict(step, key, group, label, cmd, nice)], 卡住的 {问题编号: 说明})。"""
    root = root or bt()
    hub = M.hub()
    ptasks = (s["pueue"].get(hub) or {}).get("tasks")
    if ptasks is None:
        return [], {}  # box 的 pueue 读不到：这次什么都不做
    queue_tasks = [t for tq in (s.get("taskq") or {}).values() if tq for t in tq.get("tasks", [])]
    nice = {t["key"]: t["nice"] for t in queue_tasks}
    py = python()
    tasks, groups, final, by_cmd, by_label, legacy = candidates(root, py, queue_tasks)

    def recognise(t):
        sks = step_of(t["command"])
        if sks:
            out = []
            for st, k in sks:
                if st in ("oof", "paired", "ens") and k in legacy:  # 10/18 前的实验一级步骤：数据集/训练器 → 实验 id
                    k = legacy[k]
                out.append((st, k))
            return out
        hit = by_cmd.get(t["command"].strip()) or by_label.get(t.get("label") or "")
        return [hit] if hit else []

    live, done = set(), {}
    live_fetch = {}  # 任务 → [(源机器, 任务号)]：活着的拉回任务
    live_tasks = {}  # (步骤, key) → [活着的 box 任务]
    for t in ptasks:
        for sk in recognise(t):
            if t["status"] in LIVE_ST:
                live.add(sk)
                live_tasks.setdefault(sk, []).append(t)
                if sk[0].startswith("fetch@"):
                    live_fetch.setdefault(sk[1], []).append((sk[0][6:], t["id"]))
            else:
                done.setdefault(sk, []).append(t)
    remote = {h: d["folds"] for h, d in s.get("src", {}).items() if h != hub}
    rerun = {f["name"] for f in final if f.get("rerun_if_newer")}
    todo, stuck = [], {}

    def want(step, key, label, cmd, group, n=NO_NICE, src=None):
        sk = (f"{step}@{src}" if src else step, key)
        if sk in live:
            return
        prev = sorted(done.get(sk, []), key=lambda t: t["id"])
        last_ok = max((i for i, t in enumerate(prev) if t["status"] == "Success"), default=-1)
        # 只看最后一次成功之后的；依赖失败的没跑过，原因在前一步
        bad = [t for t in prev[last_ok + 1:] if t["status"] not in ("Success", "DependencyFailed")]
        # 成功跑完却还没有结果：报出来，不每 30 分钟重跑（有新结果就要重跑的不算）
        ok_empty = last_ok >= 0 and step not in rerun
        if bad or ok_empty:
            t = bad[-1] if bad else prev[last_ok]
            why = f"任务 {t['id']} {t['status']}" if bad else f"任务 {t['id']} 成功跑完，但结果还是不全"
            stuck[f"advance:{step}:{key}"] = (f"{step} {key}：{why}，不再自动重排（看 pueue log {t['id']}；处理好以后 "
                                              f"pueue remove {t['id']}，下一次检查会重排）")
            return
        todo.append(dict(step=step, key=key, group=group, label=label, cmd=cmd, nice=n))

    def stale_fetch(key, src):
        """活着、但等的不是 src 的拉回任务（以前预先排的链在等另一台机器）：报出来。"""
        for h, i in live_fetch.get(key, []):
            if h != src:
                stuck[f"advance:stale_fetch:{key}:{i}"] = (f"box 任务 {i} 在等 {h} 上的 {key} 训完，但这一折"
                                                           f"{'在 ' + src + ' 训完了' if src else '已经在 box 上了'}；"
                                                           f"不用了：pueue kill {i} 后 pueue remove {i}")

    off = Offload(s, root, ptasks, todo, stuck, live, live_tasks)
    trained, after_ok = {}, {}  # 任务 → box 上训完没；(任务, 步骤) → 做完没
    for t in tasks:
        key, sp, n = t["key"], t["spec"], nice.get(t["key"], NO_NICE)
        trained[key] = S.is_done(sp, root)
        if not trained[key]:
            src = next((h for h in sorted(remote) if (remote[h].get(key) or {}).get("final")), None)
            if src:
                want("fetch", key, f"fetch:{t['lab']}", f"bash scripts/queue/fetch_task.sh {src} {key}", "wait", n, src=src)
                stale_fetch(key, src)
            continue
        stale_fetch(key, None)
        if sp.get("archive") and (t["e"] or {}).get("archive", True) is not False:
            for h in ARCHIVE_FROM:
                cp = (remote.get(h) or {}).get(key)
                if cp and cp.get("final") and cp.get("archived") is False:
                    want("archive", key, f"archive:{t['lab']}", f"bash scripts/queue/archive_task.sh {h} {key}", "wait", n)
        blocked = False
        archiving = ("archive", key) in live or any(x["step"] == "archive" and x["key"] == key for x in todo)
        for st in (t["e"] or {}).get("after", []):
            ok = st["done"].ok(root, t["names"])
            after_ok[(key, st["name"])] = ok
            label = f"{st['name']}:{t['lab']}"
            if ok:
                off.finished(label)
            elif not blocked:
                if not off.take(t, st, label, n, archiving):
                    want(st["name"], key, label, S.fill(st["cmd"], t["names"], keep=()), st["group"], n)
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
            for st in chain:
                group_steps.add(st["name"])
            if not all(trained.get(t["key"]) for t in mine):
                continue
            for st in chain:
                if st.get("needs") and not all(after_ok.get((t["key"], st["needs"])) for t in mine):
                    break
                if not st["done"].ok(root, names):
                    want(st["name"], e["id"], f"{st['name']}:{e['id']}", S.fill(st["cmd"], names, keep=()), st["group"], n)
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
    off.close()
    todo.sort(key=lambda t: (t["nice"], t["key"]))
    return todo, stuck


# ---------------- 分到别的机器 ----------------
def offload_rules():
    """OFFLOAD 里的每行 → [(通配式, 只在那边空着时分)]。"""
    try:
        lines = open(os.path.join(STATE_DIR, "OFFLOAD")).read().splitlines()
    except OSError:
        return []
    out = []
    for ln in lines:
        w = ln.split("#", 1)[0].split()
        if w:
            out.append((w[0], w[1:2] == ["idle"]))
    return out


def offload_target():
    """→ (机器, 组)：machines.py 里第一台有 offload 组的机器；没有 → (None, None)。"""
    return next(((n, M.MACHINES[n]["offload"]) for n in M.names("pueue") if M.MACHINES[n].get("offload")), (None, None))


def offload_state():
    try:
        return json.load(open(os.path.join(STATE_DIR, "offload.json")))
    except (OSError, ValueError):
        return {}


def step_needs(cmd):
    """步骤的命令 → 要用的输入（相对数据目录）；不支持 --stage needs → None。"""
    tok = shlex.split(cmd)
    if "--stage" not in tok or tok.index("--stage") + 1 >= len(tok):
        return None
    tok[tok.index("--stage") + 1] = "needs"
    r = subprocess.run(tok, cwd=REPO, capture_output=True, text=True, timeout=120)
    return [ln.strip() for ln in r.stdout.splitlines() if ln.strip()] if r.returncode == 0 else None


def remote_have(name, paths):
    """这台机器上已经有的（文件夹里有 .incomplete 的是没传完的，不算）→ set；连不上 → None。"""
    m = M.MACHINES[name]
    script = (f'cd {m["data"]} && for p in {" ".join(shlex.quote(p) for p in paths)}; do '
              '[[ -e $p && ! -e $p/.incomplete ]] && echo "$p"; done; echo END')
    try:
        r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", m["ssh"], "bash -s"], input=script,
                           capture_output=True, text=True, timeout=120)
    except Exception:  # noqa: BLE001
        return None
    lines = r.stdout.split()
    return set(lines[:-1]) if r.returncode == 0 and lines[-1:] == ["END"] else None


def local_gb(paths, root):
    r = subprocess.run(["du", "-sbLc"] + [os.path.join(root, p) for p in paths], capture_output=True, text=True)
    return int(r.stdout.strip().splitlines()[-1].split()[0]) / 1e9 if r.stdout.strip() else 0


class Offload:
    """plan() 里分到别的机器的部分（见开头"分到别的机器"）。往 todo 里加的除了 box 任务，还有 host=<机器> 的任务、
    op="clean" 的清理；record 是排上以后要记进 offload.json 的。"""

    def __init__(self, s, root, ptasks, todo, stuck, live, live_tasks):
        self.rules = offload_rules()
        self.name, self.group = offload_target()
        self.state = offload_state()
        self.s, self.root, self.ptasks, self.todo, self.stuck = s, root, ptasks, todo, stuck
        self.live, self.live_tasks = live, live_tasks
        self.rtasks = (s["pueue"].get(self.name) or {}).get("tasks") if self.name else None
        self.moved = False  # idle 的这次已经分过一个
        self.reserved = 0.0  # 这次要传过去的 GB
        self.cleaning = []
        self.have = {}

    def rule(self, label):
        return next((idle for pat, idle in self.rules if fnmatch(label, pat)), None)

    def by_label(self, tasks, label):
        return sorted((t for t in tasks or [] if t.get("label") == label), key=lambda t: t["id"])

    def idle(self):
        busy = any(t["status"] in LIVE_ST and t.get("group") == self.group for t in self.rtasks or [])
        pushing = any(t["status"] in LIVE_ST and (t.get("label") or "").startswith("push:") for t in self.ptasks)
        return not busy and not pushing and not self.moved

    def take(self, t, st, label, n, archiving):
        """这一步分到别的机器（已经分过、或这次分）→ True；照常在 box 上排 → False。"""
        idle = self.rule(label)
        if idle is None or not self.name:
            return False
        sk = (st["name"], t["key"])
        mine = label in self.state
        if self.rtasks is None:  # 那边的 pueue 读不到：分过的等下次；idle 的照常在 box 上跑
            if mine or not idle:
                self.stuck[f"advance:offload:{label}"] = f"{label}：读不到 {self.name} 的 pueue，这次不处理"
                return True
            return False
        steal = []
        if sk in self.live:
            box = self.live_tasks.get(sk, [])
            movable = ("Queued",) if idle else ("Queued", "Stashed")
            if mine or not all(x["status"] in movable for x in box) or (idle and not self.idle()):
                return False
            steal = [x["id"] for x in box]
        elif idle and not mine and not self.idle():
            return False
        rt = self.by_label(self.rtasks, label)
        if any(x["status"] in LIVE_ST for x in rt):
            return True
        names = dict(t["names"], data=self.root)
        out = os.path.dirname(S.fill(st["done"].path, names, keep=()))
        pulls = self.by_label(self.ptasks, f"pull:{label}")
        if any(x["status"] in LIVE_ST for x in pulls):
            return True
        if rt and rt[-1]["status"] == "Success":
            p = pulls
            if p:
                why = (f"box 任务 {p[-1]['id']} 拉回了，但 box 上核对还是没做完" if p[-1]["status"] == "Success"
                       else f"拉回任务 {p[-1]['id']} {p[-1]['status']}")
                self.stuck[f"advance:offload:{label}"] = (f"{label}（{self.name} 任务 {rt[-1]['id']}）：{why}，不再自动重排"
                                                          f"（看 pueue log {p[-1]['id']}；处理好以后 pueue remove {p[-1]['id']}）")
                return True
            self.todo.append(dict(step="pull", key=t["key"], group="wait", label=f"pull:{label}", nice=n,
                                  cmd=f"bash scripts/queue/offload_io.sh pull {self.name} {shlex.quote(out)}"))
            return True
        if rt:
            x = rt[-1]
            self.stuck[f"advance:offload:{label}"] = (f"{label}：{self.name} 任务 {x['id']} {x['status']}，不再自动重排（ssh "
                                                      f"{self.name} pueue log {x['id']}；处理好以后在 {self.name} 上 pueue remove {x['id']}）")
            return True
        if archiving:  # 训练机上的同一折还在归档（会删那边的存档），归档完再说
            return True
        cmd = S.fill(st["cmd"], names, keep=())
        needs = step_needs(cmd)
        if needs is None:
            self.stuck[f"advance:offload:{label}"] = f"{label}：命令不支持 --stage needs，不能分到 {self.name}（{cmd}）"
            return True
        have = remote_have(self.name, needs)
        if have is None:
            self.stuck[f"advance:offload:{label}"] = f"{label}：连不上 {self.name}，这次不处理"
            return True
        self.have[label] = needs
        rec = dict(machine=self.name, key=t["key"], out=out, needs=needs,
                   pushed=(self.state.get(label) or {}).get("pushed", []))
        missing = [p for p in needs if p not in have]
        if missing:
            pushes = self.by_label(self.ptasks, f"push:{label}")
            if any(x["status"] in LIVE_ST for x in pushes):
                return True
            if pushes and pushes[-1]["status"] != "Success":
                x = pushes[-1]
                self.stuck[f"advance:offload:{label}"] = (f"{label}：传输任务 {x['id']} {x['status']}，不再自动重排（看 pueue log "
                                                          f"{x['id']}；处理好以后 pueue remove {x['id']}）")
                return True
            flying = " ".join(x["command"] for x in self.ptasks if x["status"] in LIVE_ST and (x.get("label") or "").startswith("push:"))
            planned = {p for x in self.todo if x.get("label", "").startswith("push:") for p in x["paths"]}
            rest = [p for p in missing if p not in planned and p not in flying.split()]
            if not rest:
                return True
            need_gb = local_gb(rest, self.root) + OFFLOAD_OUT_GB
            free = (self.s.get("probe", {}).get(self.name) or {}).get("DISK", "").strip()
            line = (M.MACHINES[self.name]["disk"] or (None, 0))[1] + 10
            if not free.isdigit() or int(free) / 1e9 - self.reserved - need_gb < line:
                self.stuck[f"advance:offload:{label}"] = (f"{label}：要先传 {need_gb - OFFLOAD_OUT_GB:.1f} GB，{self.name} 盘"
                                                          f"{'剩 %.0f GB' % (int(free) / 1e9) if free.isdigit() else '还剩多少不知道'}，"
                                                          f"传完、加上结果要留 {line} GB 以上，先不传")
                return True
            self.reserved += need_gb
            files = [p for p in rest if os.path.isfile(os.path.join(self.root, p))]
            rec["pushed"] = sorted(set(rec["pushed"]) | set(files))
            self.todo.append(dict(step="push", key=t["key"], group="wait", label=f"push:{label}", nice=n, paths=rest,
                                  cmd="bash scripts/queue/offload_io.sh push " + " ".join([self.name] + [shlex.quote(p) for p in rest]),
                                  steal=steal, record=(label, rec)))
            self.moved |= bool(idle)
            return True
        rnames = dict(names, data=M.MACHINES[self.name]["data"], python=python(self.name))
        self.todo.append(dict(step=st["name"], key=t["key"], group=self.group, label=label, nice=n, host=self.name,
                              cmd=S.fill(st["cmd"], rnames, keep=()), steal=steal, record=(label, rec)))
        self.moved |= bool(idle)
        return True

    def finished(self, label):
        """box 上判断做完了：分出去的要清理那边。"""
        if label in self.state:
            self.cleaning.append(label)

    def close(self):
        """清理做完的：结果文件夹、别的分出去的步骤不再用的模型文件。"""
        busy = {p for lb, r in self.state.items() if lb not in self.cleaning for p in r.get("needs", [])}
        busy |= {p for ns in self.have.values() for p in ns}
        for lb in self.cleaning:
            r = self.state[lb]
            drop = [r["out"]] + [p for p in r.get("pushed", []) if p not in busy]
            self.todo.append(dict(op="clean", step="clean", key=r["key"], label=lb, nice=NO_NICE, host=r["machine"], paths=drop))


# ---------------- 执行 ----------------
def held():
    """HOLD 里的通配式 → [str]。"""
    try:
        lines = open(os.path.join(STATE_DIR, "HOLD")).read().splitlines()
    except OSError:
        return []
    return [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]


def run(s, dry=False, q=None):
    """health.py 调用：→ (这次排了的说明列表（影子模式下是"会排 …"）, 卡住的 {问题编号: 说明})。没有 LIVE 文件时是影子模式。"""
    import refill  # q()：在 box 上用 scripts/queue/q 加任务
    q = q or refill.q
    todo, stuck = plan(s)
    live_mode = os.path.exists(os.path.join(STATE_DIR, "LIVE")) and not dry
    done, lines = [], []
    hold = held()
    state = offload_state()
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
                    stuck[f"advance:offload:{t['label']}"] = f"{t['label']}：删 {host} 上的结果出错（{out[-150:]}）"
            else:
                lines.append(f"会清理 {what}")
            continue
        h = any(fnmatch(t["label"], pat) for pat in hold)
        note = "，暂停（HOLD）" if h else ""
        if t.get("steal"):
            note += f"，从 box 挪过去（删 box 任务 {' '.join(map(str, t['steal']))}）"
        if live_mode:
            try:
                for i in t.get("steal") or []:
                    rc, out = refill.sh(f"~/.local/bin/pueue remove {i}")
                    if rc:
                        raise RuntimeError(f"删不掉 box 任务 {i}：{out[-150:]}")
                tid = q(host, f"{t['group']} -p{' --stashed' if h else ''} --label {shlex.quote(t['label'])} -- {shlex.quote(t['cmd'])}")
                if t.get("record"):
                    state[t["record"][0]] = t["record"][1]
                done.append(f"{where}{t['label']}（任务 {tid}{note}）")
                lines.append(f"排了 {t['label']}（{where}{t['group']} 组，任务 {tid}{note}）：{t['cmd']}")
            except Exception as e:  # noqa: BLE001
                lines.append(f"出错 {t['label']}：{e}")
                stuck[f"advance:{t['step']}:{t['key']}"] = f"{t['step']} {t['key']}：加任务出错（{e}）"
        else:
            done.append(f"会排 {where}{t['label']}{note}")
            lines.append(f"会排 {t['label']}（{where}{t['group']} 组{note}）：{t['cmd']}")
    if live_mode:
        os.makedirs(STATE_DIR, exist_ok=True)
        with open(os.path.join(STATE_DIR, "offload.json.tmp"), "w") as f:
            json.dump(state, f, indent=1, ensure_ascii=False)
        os.replace(os.path.join(STATE_DIR, "offload.json.tmp"), os.path.join(STATE_DIR, "offload.json"))
    if not dry:
        log(lines, stuck, live_mode)
    return done, stuck


def log(lines, stuck, live_mode):
    """写 ~/advance.log：和上次一样（影子模式下同样的"会排"、同样的卡住）就不重复写。"""
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
            mode = "" if live_mode else "（影子模式）"
            f.writelines(f"{datetime.now():%F %T}{mode} {ln}\n" for ln in out)
    json.dump(cur, open(last_p, "w"), ensure_ascii=False)


if __name__ == "__main__":
    sys.path.insert(0, os.path.join(REPO, "scripts", "monitor"))
    import health as H  # noqa: E402
    todo, stuck = plan(H.gather())
    for t in todo:
        if t.get("op") == "clean":
            print(f"会清理 {t['label']:26} {t['host']} {' '.join(t['paths'])}")
        else:
            print(f"会排 {t['label']:28} {(t.get('host', '') + ' ' + t['group']).strip():9} {t['cmd']}"
                  + (f"（挪走 box 任务 {t['steal']}）" if t.get("steal") else ""))
    for k, v in stuck.items():
        print(f"卡住 {v}")
    if not todo and not stuck:
        print("没有要排的")
