#!/usr/bin/env python3
"""按状态推进训练完以后的事（2026-10-18）：box 上的健康检查（scripts/monitor/health.py，每 30 分钟）调用。
每次对每一折看文件，算出它走到了哪一步，下一步没在跑就在 box 的 pueue 里启动它；不提前排链（不用 --after）。

一折的几步（都在 box 上，看 box 上的文件判断做没做完）：
  fetch    别的机器上训完了（checkpoint_final 或 ARCHIVED + validation/summary.json），box 上还没有
           → scripts/queue/fetch_fold.sh <机器>（wait 组）。训完才排，所以一开始就能拉。
  archive  pueue 训练机（lab）训的折拉回 box 以后，那边还有大文件 → scripts/queue/archive_fold.sh <机器>
           （wait 组：备份到 DICC、核对后删那边的；T0 nnU-Net 五折在 lab 上的副本删不删没定，不排）
  infer    box 上有了，推理没做完（predictions/…/fold_k/MANIFEST.json 里还没有概率图稀疏化的记录）
           → run_predict_eval.sh --stage infer（gpu 组）
  eval     推理做完了，评测没做完（RQ1/analysis/…/fold_k/report/report.run.json 不是 ok）→ --stage eval（cpu 组）
一个实验（数据集 + 训练器）五折都到了以后（cpu 组，每次只排当前缺的那一步）：
  oof      T3 / T14 / T15（数据集在 research/RQ1/splits/t3_splits.json 里）：五折都在 box 上 → oof_eval.py
  paired   oof 做完 → paired_compare.py 和 T0 五折集成配对（oof/paired/）
  ens      T3、T15 以外的五折实验（含 T14）：五折推理都做完 → run_predict_eval.sh <模型> "0 1 2 3 4"（用单折的概率图集成，纯 CPU）
  compare_all  上面任何一项的结果比 compare/all/ 新 → compare_all.py（所有实验一起，同时只排一个）
fold all（T0b）只拉回，不推理。

在不在跑：box pueue 里活着的任务（排着 / 在跑），按命令认出是哪一步、哪一折（不靠标签；以前手动排的也认得）。
失败过的不自动重排：pueue 里这一步最后一次成功之后有失败 / 被停掉的任务，或者成功跑完了但结果还是不对（compare_all 除外），
  就只报出来（健康检查"需要处理"），看过 pueue log、处理好以后 pueue remove 那个任务，下一次就会重排。
  拉回按源机器认：以前预先排的、在等另一台机器的拉回任务不算，报出来（可以删掉）。
影子模式：没有 ~/.cache/phd/advance/LIVE 时只把"会排什么"写进 ~/advance.log，不真排。
任务的标签：<步骤>:<实验标签>_f<fold>，如 infer:t14met_f3；实验一级的是 <步骤>:<实验标签>。

    python3 scripts/queue/advance.py           看现在会排什么（不改任何东西）
"""
import glob
import json
import os
import shlex
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "envs"))
sys.path.insert(0, os.path.join(REPO, "research", "RQ1", "nnunet"))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "research", "RQ1", "analysis"))
import machines as M  # noqa: E402
import write_manifest as wm  # noqa: E402
import make_registry as mr  # noqa: E402

PLANS = "nnUNetResEncUNetMPlans__3d_fullres"
F5 = ["0", "1", "2", "3", "4"]
STATE_DIR = os.path.expanduser("~/.cache/phd/advance")
LOG = os.path.expanduser(os.environ.get("ADVANCE_LOG", "~/advance.log"))  # 环境变量只给测试用
LIVE_ST = ("Running", "Queued", "Stashed", "Paused", "Locked")
OOF_DS = set(json.load(open(os.path.join(REPO, "research", "RQ1", "splits", "t3_splits.json"))))
T0_ENS = f"Dataset101_GLIrq1/nnUNetTrainer_save10__{PLANS}/folds_0-1-2-3-4"
ARCHIVE_FROM = [n for n in M.names("pueue") if "train" in M.MACHINES[n]["roles"]]  # 训完要备份、删大文件的机器（lab）
GROUP = dict(fetch="wait", archive="wait", infer="gpu", eval="cpu", oof="cpu", paired="cpu", ens="cpu", compare_all="cpu")


def bt():
    return os.environ.get("ADVANCE_BT") or M.expand(M.MACHINES[M.hub()]["data"])  # 环境变量只给测试用


def python():
    base, env = M.MACHINES[M.hub()]["conda"]
    return f"{M.expand(base)}/envs/{env}/bin/python"


def experiments():
    """[dict(exp, tag, ds, tr, folds, nice)]：make_registry.SPEC 里的每个实验。"""
    out = []
    for exp, desc, model, dsid, tr, fs, tag in mr.SPEC:
        out.append(dict(exp=exp, desc=desc, tag=tag or exp.lower(), ds=mr.DS[dsid], tr=tr, folds=fs))
    return out


# ---------------- 认命令 ----------------
def step_of(cmd):
    """pueue 任务的命令 → [(步骤, key)]：key 是一折（数据集/训练器/fold）、一个实验（数据集/训练器）或 "all"。认不出 → []。
    拉回的步骤带源机器：fetch@<机器>。"""
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
        if b in ("fetch_fold.sh", "archive_fold.sh") and len(rest) >= 3:
            mk = model_key(rest[1])  # 拉回认到源机器（fetch@dicc）：从别的机器拉同一折的旧任务不算
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


# ---------------- 看文件 ----------------
def run_ok(path, legacy=None):
    """结果做完了：runlog 的记录（<脚本>.run.json）是 ok；10/16 用 runlog 之前的结果没有记录，有 legacy 这个输出文件就算。"""
    if os.path.exists(path):
        try:
            return json.load(open(path)).get("status") == "ok"
        except (OSError, ValueError):
            return False
    return bool(legacy) and os.path.exists(os.path.join(os.path.dirname(path), legacy))


def mtime(path):
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0


def started(path):
    """runlog 记录里这次运行开始的时刻（读的是开始时的输入；跑的过程中新出的结果不算它用过）；没有就用修改时间。"""
    try:
        return datetime.fromisoformat(json.load(open(path))["start"]).timestamp()
    except (OSError, ValueError, KeyError, TypeError):
        return mtime(path)


class Box:
    """box 上的文件：训练结果、推理、分析。"""

    def __init__(self, root):
        self.res = os.path.join(root, "nnUNet", "results")
        self.pred = os.path.join(root, "predictions")
        self.an = os.path.join(root, "RQ1", "analysis")

    def model(self, ds, tr):
        return f"{ds}/{tr}__{PLANS}"

    def trained(self, ds, tr, f):
        fd = os.path.join(self.res, self.model(ds, tr), f"fold_{f}")
        return (os.path.exists(os.path.join(fd, "checkpoint_final.pth")) or os.path.exists(os.path.join(fd, "ARCHIVED"))) \
            and os.path.exists(os.path.join(fd, "validation", "summary.json"))

    def inferred(self, ds, tr, f):
        """推理做完了：MANIFEST 里每个测试集的分割、概率图、输入病例数都对得上，概率图已稀疏化。"""
        return wm.incomplete(os.path.join(self.pred, self.model(ds, tr), f"fold_{f}", "MANIFEST.json")) is None

    def evaluated(self, ds, tr, tag):
        return run_ok(os.path.join(self.an, self.model(ds, tr), tag, "report", "report.run.json"), "report.md")

    def oof(self, ds, tr):
        return os.path.join(self.an, self.model(ds, tr), "oof", "oof_eval.run.json")

    def paired(self, ds, tr):
        return os.path.join(self.an, self.model(ds, tr), "oof", "paired", "paired_compare.run.json")

    def ens(self, ds, tr):
        return os.path.join(self.an, self.model(ds, tr), "folds_0-1-2-3-4", "report", "report.run.json")

    def compare_all(self):
        return os.path.join(self.an, "compare", "all", "compare_all.run.json")


# ---------------- 算下一步 ----------------
def plan(s, root=None):
    """→ (要排的 [dict(step, key, group, label, cmd, nice)], 卡住的 {问题编号: 说明})。"""
    box = Box(root or bt())
    hub = M.hub()
    tasks = (s["pueue"].get(hub) or {}).get("tasks")
    if tasks is None:
        return [], {}  # box 的 pueue 读不到：这次什么都不做
    live, done = set(), {}
    for t in tasks:
        for sk in step_of(t["command"]):
            if t["status"] in LIVE_ST:
                live.add(sk)
            else:
                done.setdefault(sk, []).append(t)
    nice = {t["key"]: t["nice"] for tq in (s.get("taskq") or {}).values() if tq for t in tq["tasks"]}
    remote = {h: d["folds"] for h, d in s.get("src", {}).items() if h != hub}
    py = python()
    todo, stuck = [], {}

    def want(step, key, label, cmd, n=10 ** 6, src=None):
        sk = (f"{step}@{src}" if src else step, key)
        if sk in live:
            return
        prev = sorted(done.get(sk, []), key=lambda t: t["id"])
        last_ok = max((i for i, t in enumerate(prev) if t["status"] == "Success"), default=-1)
        # 只看最后一次成功之后的；依赖失败的没跑过，原因在前一步
        bad = [t for t in prev[last_ok + 1:] if t["status"] not in ("Success", "DependencyFailed")]
        # 成功跑完却还没有结果：报出来，不每 30 分钟重跑（compare_all 有新结果就要重跑，不算）
        ok_empty = last_ok >= 0 and step != "compare_all"
        if bad or ok_empty:
            t = bad[-1] if bad else prev[last_ok]
            why = f"任务 {t['id']} {t['status']}" if bad else f"任务 {t['id']} 成功跑完，但结果还是不全"
            stuck[f"advance:{step}:{key}"] = (f"{step} {key}：{why}，不再自动重排（看 pueue log {t['id']}；处理好以后 "
                                              f"pueue remove {t['id']}，下一次检查会重排）")
            return
        todo.append(dict(step=step, key=key, group=GROUP[step], label=label, cmd=cmd, nice=n))

    live_fetch = {}  # 一折 → [(源机器, 任务号)]：活着的拉回任务
    for t in tasks:
        if t["status"] in LIVE_ST:
            for st, k in step_of(t["command"]):
                if st.startswith("fetch@"):
                    live_fetch.setdefault(k, []).append((st[6:], t["id"]))

    def stale_fetch(key, src):
        """活着、但等的不是 src 的拉回任务（以前预先排的链在等另一台机器）：报出来。"""
        for h, i in live_fetch.get(key, []):
            if h != src:
                stuck[f"advance:stale_fetch:{key}:{i}"] = (f"box 任务 {i} 在等 {h} 上的 {key} 训完，但这一折"
                                                           f"{'在 ' + src + ' 训完了' if src else '已经在 box 上了'}；"
                                                           f"不用了：pueue kill {i} 后 pueue remove {i}")

    newest_exp = 0  # 实验一级结果里最新的修改时间（决定要不要重跑 compare_all）
    for e in experiments():
        ds, tr, model = e["ds"], e["tr"], box.model(e["ds"], e["tr"])
        mpath = os.path.join(box.res, model)
        five = e["folds"] == F5
        for f in e["folds"]:
            key, n = f"{ds}/{tr}/{f}", nice.get(f"{ds}/{tr}/{f}", 10 ** 6)
            lab = f"{e['tag']}_f{f}"
            if not box.trained(ds, tr, f):
                src = next((h for h in sorted(remote) if (remote[h].get(key) or {}).get("final")), None)
                if src:
                    want("fetch", key, f"fetch:{lab}", f"bash scripts/queue/fetch_fold.sh {src} {model} {f}", n, src=src)
                    stale_fetch(key, src)
                continue
            stale_fetch(key, None)
            # pueue 训练机（lab）上训完的折拉回后备份到 DICC、删掉那边的大文件。
            # T0 nnU-Net 五折在 lab 上的副本（验证集概率图是原始精度，和 box 上稀疏化过的不一致）删不删还没定，不自动归档
            t0_nnunet = e["exp"] == "T0" and tr == "nnUNetTrainer_save10"
            for h in ARCHIVE_FROM:
                cp = (remote.get(h) or {}).get(key)
                if cp and cp.get("final") and cp.get("archived") is False and not t0_nnunet:
                    want("archive", key, f"archive:{lab}", f"bash scripts/queue/archive_fold.sh {h} {model} {f}", n)
            if not five:
                continue
            if not box.inferred(ds, tr, f):
                want("infer", key, f"infer:{lab}",
                     f"bash research/RQ1/analysis/run_predict_eval.sh --stage infer {mpath} {f}", n)
            elif not box.evaluated(ds, tr, f"fold_{f}"):
                want("eval", key, f"eval:{lab}", f"bash research/RQ1/analysis/run_predict_eval.sh --stage eval {mpath} {f}", n)
        if not five:  # fold all（T0b）只拉回
            continue
        ekey, n = f"{ds}/{tr}", min([nice.get(f"{ds}/{tr}/{f}", 10 ** 6) for f in F5])
        if ds in OOF_DS and all(box.trained(ds, tr, f) for f in F5):
            if not run_ok(box.oof(ds, tr), "check.txt"):
                want("oof", ekey, f"oof:{e['tag']}", f"{py} research/RQ1/analysis/oof_eval.py {mpath}", n)
            elif not run_ok(box.paired(ds, tr), "paired.csv"):
                want("paired", ekey, f"paired:{e['tag']}",
                     f"{py} research/RQ1/analysis/paired_compare.py --ref {box.an}/{T0_ENS} "
                     f"--cmp {e['exp']}={box.an}/{model}/oof/eval --out {box.an}/{model}/oof/paired", n)
            else:
                newest_exp = max(newest_exp, mtime(box.paired(ds, tr)) or mtime(os.path.join(os.path.dirname(box.paired(ds, tr)), "paired.csv")))
        if e["exp"] not in ("T3", "T15") and all(box.inferred(ds, tr, f) for f in F5):
            if not run_ok(box.ens(ds, tr), "report.md"):
                want("ens", ekey, f"ens:{e['tag']}", f"ENSEMBLE_ONLY=1 bash research/RQ1/analysis/run_predict_eval.sh {mpath} " + shlex.quote(" ".join(F5)), n)
            else:
                newest_exp = max(newest_exp, mtime(box.ens(ds, tr)) or mtime(os.path.join(os.path.dirname(box.ens(ds, tr)), "report.md")))
    if newest_exp > started(box.compare_all()) and not any(t["group"] == "cpu" and t["step"] in ("oof", "paired", "ens") for t in todo) \
            and not any(sk[0] in ("oof", "paired", "ens") for sk in live):
        want("compare_all", "all", "compare_all", f"{py} research/RQ1/analysis/compare_all.py")
    todo.sort(key=lambda t: (t["nice"], t["key"]))
    return todo, stuck


# ---------------- 执行 ----------------
def run(s, dry=False, q=None):
    """health.py 调用：→ (这次排了的说明列表（影子模式下是"会排 …"）, 卡住的 {问题编号: 说明})。没有 LIVE 文件时是影子模式。"""
    import refill  # q()：在 box 上用 scripts/queue/q 加任务
    q = q or refill.q
    todo, stuck = plan(s)
    live_mode = os.path.exists(os.path.join(STATE_DIR, "LIVE")) and not dry
    done, lines = [], []
    for t in todo:
        if live_mode:
            try:
                tid = q(M.hub(), f"{t['group']} -p --label {shlex.quote(t['label'])} -- {shlex.quote(t['cmd'])}")
                done.append(f"{t['label']}（任务 {tid}）")
                lines.append(f"排了 {t['label']}（{t['group']} 组，任务 {tid}）：{t['cmd']}")
            except Exception as e:  # noqa: BLE001
                lines.append(f"出错 {t['label']}：{e}")
                stuck[f"advance:{t['step']}:{t['key']}"] = f"{t['step']} {t['key']}：加任务出错（{e}）"
        else:
            done.append(f"会排 {t['label']}")
            lines.append(f"会排 {t['label']}（{t['group']} 组）：{t['cmd']}")
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
        print(f"会排 {t['label']:28} {t['group']:5} {t['cmd']}")
    for k, v in stuck.items():
        print(f"卡住 {v}")
    if not todo and not stuck:
        print("没有要排的")
