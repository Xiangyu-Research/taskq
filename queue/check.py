#!/usr/bin/env python3
"""核对一个使用者的配置（2026-11-04）：机器表、policy、插件、实验表、每个任务和步骤都能变成要跑的命令、执行机的代码包建得出来。
不连别的机器、不改任何东西（步骤的 --stage needs 在临时的空数据目录里跑，代码包建在临时目录里）。

    python3 queue/check.py [--quick] [--only 组…] [--data 目录]

  --quick  不跑步骤的 --stage needs、不建代码包（这两项最慢）
  --only   只查这几组（machines policy plugins experiments tasks steps needs bundle）
  --data   --stage needs 用的数据目录（只列输入，不读写）；默认 TASKQ_DATA、本机有的 hub 数据目录，都没有时用空的临时目录
退出码 0 = 没有问题，1 = 有问题。问题（和只提醒的"注意"）写在 stderr，每组的结果写在 stdout。
"""
import argparse
import ast
import fnmatch
import glob
import json
import os
import re
import shutil
import sys
import tempfile
import traceback
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
for d in ("sites", "monitor", "sync"):
    sys.path.insert(0, os.path.join(CORE, d))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto as PR  # noqa: E402
cfg.use_config()
# 核心模块先 import：项目的实验表可能把插件文件夹加进 sys.path，里面有同名的 recipes.py、relay_plugins.py
import recipes  # noqa: E402,F401
import relay_plugins  # noqa: E402,F401
import smoke_gate as SG  # noqa: E402

GROUPS = ("machines", "policy", "plugins", "experiments", "tasks", "steps", "needs", "bundle")
SLOW = ("needs", "bundle")


class Report:
    def __init__(self):
        self.problems, self.notes, self.counts = [], [], {}

    def bad(self, group, msg):
        self.problems.append((group, msg))

    def note(self, group, msg):
        self.notes.append((group, msg))

    def checked(self, group, n=1):
        self.counts[group] = self.counts.get(group, 0) + n


def _is_tuple(v, n=None):
    return isinstance(v, (tuple, list)) and (n is None or len(v) in ((n,) if isinstance(n, int) else n))


# ---------------- 机器表 ----------------
def check_machines(r):
    M = cfg.mtab()
    import slurm as SL
    g = "machines"
    hubs = [n for n, m in M.MACHINES.items() if "hub" in (m.get("roles") or ())]
    if len(hubs) != 1:
        r.bad(g, f"MACHINES 里 roles 带 hub 的要正好一台，现在 {hubs or '没有'}")
    sites = getattr(M, "SITES", {})
    both = sorted(set(M.MACHINES) & set(sites))
    if both:
        r.bad(g, f"MACHINES 和 SITES 里重名：{both}")
    relay = {k for k, v in sites.items() if v.get("relay")}
    if set(getattr(M, "RELAY", {})) != relay:
        r.bad(g, f"RELAY 要正好是 SITES 里 relay=True 的条目（{sorted(relay)}），现在 {sorted(getattr(M, 'RELAY', {}))}")
    for n, m in {**M.MACHINES, **sites}.items():
        where = f"{'SITES' if n in sites else 'MACHINES'}[{n!r}]"
        r.checked(g)
        for k in ("label", "hostname", "scheduler"):
            if not m.get(k):
                r.bad(g, f"{where} 没有 {k}")
        try:
            re.compile(m.get("hostname") or "")
        except re.error as e:
            r.bad(g, f"{where} 的 hostname 不是正则：{e}")
        if m.get("scheduler") not in ("pueue", "slurm"):
            r.bad(g, f"{where} 的 scheduler 要是 pueue 或 slurm，现在 {m.get('scheduler')!r}")
        if not _is_tuple(m.get("roles", ())):
            r.bad(g, f"{where} 的 roles 要是元组")
        groups = m.get("groups") or {}
        for gn, gv in groups.items():
            if not (_is_tuple(gv, 3) and isinstance(gv[0], int) and (gv[1] is None or isinstance(gv[1], str)) and isinstance(gv[2], dict)):
                r.bad(g, f"{where} 的组 {gn} 要写成 (并行数, 卡号或 None, {{环境变量}})，现在 {gv!r}")
        envs = m.get("envs") or {}
        for en, ev in envs.items():
            if M.is_spec(ev):
                if M.spec_hash(en) is None:
                    r.bad(g, f"{where} 的环境 {en} 是 spec，但没有它的描述 {en}.yml")
                elif not os.path.isfile(M.lock_path(en, M.spec_variant(n, en))):
                    r.note(g, f"{where} 的环境 {en} 还没有锁 {os.path.basename(M.lock_path(en, M.spec_variant(n, en)))}"
                              f"（taskq env lock {en} --variant {M.spec_variant(n, en)}）")
            elif not (_is_tuple(ev) and ev and ev[0] in ("conda", "venv")):
                r.bad(g, f"{where} 的环境 {en} 要写成 (\"conda\", 名字)、(\"venv\", 目录, 底子) 或 \"spec\"，现在 {ev!r}")
            elif ev[0] == "venv" and (len(ev) != 3 or ev[2] not in envs):
                r.bad(g, f"{where} 的环境 {en}：venv 的底子 {ev[2] if len(ev) > 2 else None!r} 不在这台的 envs 里")
            elif ev[0] == "conda" and not m.get("conda"):
                r.bad(g, f"{where} 的环境 {en} 是 conda，但这台没有 conda=(目录, 环境)")
        if n not in sites:
            continue
        if not m.get("root"):
            r.bad(g, f"{where} 没有 root（执行机的根目录）")
        for kind in M.kinds(n):
            w = M.worker_of(n, kind)
            if m.get("scheduler") == "pueue" and (not isinstance(w, dict) or w.get("group") not in groups or not w.get("id")):
                r.bad(g, f"{where} 的 {kind} worker 要有 group（在 groups 里）和 id，现在 {w!r}")
        if m.get("scheduler") == "slurm":
            bad = sorted(set(m.get("jobs") or ()) - set(M.KINDS))
            if bad:
                r.bad(g, f"{where} 的 jobs 里有不认识的种类 {bad}（能写：{list(M.KINDS)}）")
            if m.get("job_names") is not None and m["job_names"] not in SL.STYLES:
                r.bad(g, f"{where} 的 job_names {m['job_names']!r} 不在 slurm.STYLES 里（{sorted(SL.STYLES)}）")
        if m.get("relay") and m.get("cap") and m.get("budget") and m["budget"][1] >= m["cap"]:
            r.bad(g, f"{where} 的 budget 第二项 {m['budget'][1]} GB 不小于 cap {m['cap']} GB，什么都放不下")
    names = [sites[n].get("job_names") or "pair" for n in sites if sites[n].get("scheduler") == "slurm"]
    dup = sorted({x for x in names if names.count(x) > 1})
    if dup:
        r.note(g, f"几台 Slurm 执行机的作业名用同一套（job_names {dup}），squeue 里分不开是哪台的")
    for n, m in {**M.MACHINES, **sites}.items():
        quick = m.get("quick")
        if quick is None:
            continue
        if not (_is_tuple(quick, 2) and all(isinstance(x, str) and x for x in quick)):
            r.bad(g, f"{n} 的 quick 要写成 (cloudflared 镜像, 认临时地址的正则)，现在 {quick!r}")
            continue
        try:
            re.compile(quick[1])
        except re.error as e:
            r.bad(g, f"{n} 的 quick 第二项不是正则：{e}")
    check_specs(r, M)


PLACEHOLDERS = {"torch_mm", "abi", "cp"}  # 环境描述的 pip 包里能写的占位（sites/envs.py 的 pip_lines）


def check_specs(r, M):
    """<配置文件夹>/specs/*.yml：读得了、有能编译的 <名>.check.py、pip 里只有认得的占位，每个 torch 变体在一台假的执行机上
    都生成得出建环境的脚本（base 也要有描述）。算在 machines 一组。"""
    g = "machines"
    try:
        import envs as EV
    except ImportError as e:
        r.note(g, f"读不了环境描述（{e}），没查 specs/")
        return
    names = sorted(os.path.basename(p)[:-4] for p in glob.glob(os.path.join(M.SPECS, "*.yml")))
    fake = "_check_specs"
    sites = getattr(M, "SITES", None)
    if sites is None or fake in sites:
        return
    check_locks(r, M, names)
    for n in names:
        r.checked(g)
        try:
            sp = EV.load(n)
            p = EV.check_file(n)
            compile(open(p, encoding="utf-8").read(), p, "exec")
        except ImportError as e:
            r.note(g, f"读不了环境描述（{e}），没查 specs/")
            return
        except Exception as e:  # noqa: BLE001  EnvError、SyntaxError、yaml 的错
            r.bad(g, f"环境描述 {n}：{type(e).__name__}: {e}")
            continue
        odd = sorted({x for pkg in (sp.get("pip") or []) + (sp.get("pip_no_deps") or []) for x in re.findall(r"\{(\w+)\}", str(pkg))}
                     - PLACEHOLDERS)
        if odd:
            r.bad(g, f"环境描述 {n}.yml 的 pip 里有不认识的占位 {odd}（能写：{sorted(PLACEHOLDERS)}）")
        for v in list(sp.get("torch") or {}) or ["default"]:
            envs = {x: "spec" for x in names}
            envs[n] = ("spec", v)
            sites[fake] = dict(label=fake, hostname=r"(?!)", scheduler="pueue", root="/check/root", conda=("/check/conda", "base"),
                               envs=envs, roles=())
            try:
                EV.build_script(fake, n, store="/check/root/blobs")
            except Exception as e:  # noqa: BLE001
                r.bad(g, f"环境描述 {n}（torch 变体 {v}）生成不出建环境的脚本：{type(e).__name__}: {e}")
            finally:
                sites.pop(fake, None)


def check_locks(r, M, names):
    """<名>.<变体>.lock.json：有对应的描述，记的 spec_sha256 是现在的描述（描述改了要重新锁）。"""
    import hashlib
    g = "machines"
    for p in sorted(glob.glob(os.path.join(M.SPECS, "*" + PR.LOCK_SUFFIX))):
        r.checked(g)
        f = os.path.basename(p)
        try:
            d = json.load(open(p, encoding="utf-8"))
        except (OSError, ValueError) as e:
            r.bad(g, f"锁 {f} 读不了：{e}")
            continue
        n, v = d.get("name"), d.get("variant")
        if f != f"{n}.{v}{PR.LOCK_SUFFIX}" or n not in names:
            r.bad(g, f"锁 {f} 的 name / variant（{n} / {v}）和文件名对不上，或者没有描述 {n}.yml")
            continue
        if d.get("spec_sha256") != hashlib.sha256(open(os.path.join(M.SPECS, f"{n}.yml"), "rb").read()).hexdigest():
            r.bad(g, f"锁 {f} 是旧描述的（{n}.yml 锁了以后改过），重新锁：taskq env lock {n} --variant {v}")


# ---------------- policy ----------------
def check_policy(r):
    M = cfg.mtab()
    import policy as P
    import advance as A
    import experiments as E
    g = "policy"
    every = set(M.MACHINES) | set(getattr(M, "SITES", {}))
    for name, typ in (("AUTO", dict), ("PLACE", dict), ("HOLD", list), ("SHIFT", dict), ("SLURM", dict), ("READY", dict),
                      ("OFFLINE", dict), ("OUTPUTS", dict)):
        v = getattr(P, name, None)
        r.checked(g)
        if v is None:
            r.bad(g, f"policy.py 没有 {name}")
        elif not isinstance(v, typ):
            r.bad(g, f"policy.py 的 {name} 要是{'字典' if typ is dict else '列表'}")
    auto = getattr(P, "AUTO", {}) or {}
    bad = sorted(set(auto) - every - set(PR.AUTO_SWITCHES))
    if bad:
        r.bad(g, f"AUTO 里有不认识的机器 {bad}（能写：{'、'.join(PR.AUTO_SWITCHES)} 和机器名）")
    nb = sorted(k for k, v in auto.items() if not isinstance(v, bool) and not (k == "envs" and isinstance(v, dict)))
    if nb:
        r.bad(g, f"AUTO 的 {nb} 要写 True / False（envs 也可以是 {{机器: [环境 或 (环境, 变体)]}}）")
    for mn, lst in (auto.get("envs") if isinstance(auto.get("envs"), dict) else {}).items():
        if mn not in every:
            r.bad(g, f"AUTO['envs'] 里有不认识的机器 {mn!r}")
        for x in lst if isinstance(lst, (list, tuple)) else [None]:
            e = x if isinstance(x, str) else x[0] if isinstance(x, (list, tuple)) and 1 <= len(x) <= 2 else None
            if e is None or M.spec_hash(e) is None:
                r.bad(g, f"AUTO['envs'][{mn!r}] 的 {x!r} 要是有描述的环境名或 (环境, 变体)")
    for name in ("READY", "SLURM", "SHIFT", "OFFLINE"):
        bad = sorted(set(getattr(P, name, {}) or {}) - every)
        if bad:
            r.bad(g, f"{name} 里有不认识的机器 {bad}")
    for n, v in (getattr(P, "READY", {}) or {}).items():
        if not isinstance(v, int) or v < 0:
            r.bad(g, f"READY[{n!r}] 要是不小于 0 的整数，现在 {v!r}")
    for n in getattr(P, "SLURM", {}) or {}:
        if n in every and M.machine(n).get("scheduler") != "slurm":
            r.bad(g, f"SLURM 里的 {n} 不是 Slurm 的机器")
    for n, v in (getattr(P, "SHIFT", {}) or {}).items():
        periods = v if v and isinstance(v[0], (tuple, list)) else [v]
        if n not in M.RELAY:
            r.bad(g, f"SHIFT 里的 {n} 不是执行机（relay=True）")
        if not periods or not all(_is_tuple(p, (2, 3, 4)) and all(re.fullmatch(r"\d\d:\d\d", x or "") for x in p[:2]) for p in periods):
            r.bad(g, f"SHIFT[{n!r}] 要写成一段或几段 (\"HH:MM\", \"HH:MM\"[, 第一天, 最后一天])，现在 {v!r}")
    hold = getattr(P, "HOLD", []) or []
    if not all(isinstance(x, str) for x in hold):
        r.bad(g, "HOLD 要是字符串（pueue 标签的通配）的列表")
    try:
        exps, final = E.load()
        steps = {st["name"] for e in exps for st in (e.get("after") or []) + [x for c in e.get("group_after") or [] for x in c]}
        steps |= {f["name"] for f in final}
        A.check_policy(steps)
    except Exception as e:  # noqa: BLE001  实验表读不了的在 experiments 一组报
        if isinstance(e, ValueError):
            r.bad(g, str(e))
    text = ""
    for f in glob.glob(os.path.join(CORE, "*", "*")) + glob.glob(os.path.join(CORE, "bin", "*")):
        if not os.path.isfile(f) or os.path.basename(f).startswith("test_") or f.endswith(".md") or os.path.abspath(f) == os.path.abspath(__file__):
            continue
        try:
            text += open(f, encoding="utf-8").read()
        except (OSError, UnicodeDecodeError):
            pass
    known = {k for k in getattr(P, "OUTPUTS", {}) or {} if re.search(r"\b" + re.escape(k) + r"\b", text)}
    unknown = sorted(set(getattr(P, "OUTPUTS", {}) or {}) - known)
    if unknown:
        r.note(g, f"OUTPUTS 里 {unknown} 核心没有用到（拼错了，或只给使用者自己的脚本用）")


# ---------------- 插件 ----------------
def core_modules():
    return {os.path.splitext(os.path.basename(p))[0] for d in ("queue", "sites", "monitor", "sync")
            for p in glob.glob(os.path.join(CORE, d, "*.py"))}


def check_labels(r):
    """labels 插件（推送、进度报告的白话）：表的类型、PLAN_TITLES() 跑得通、返回 {编号: 标题}。"""
    g = "plugins"
    ns = {"STEPS": {}, "QUIET": [], "PLAN_TITLES": None}
    try:
        cfg.extend(ns, "labels")
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"核心合 labels 插件时出错：{e}")
        return
    r.checked(g)
    bad = [k for k, v in ns["STEPS"].items() if not (isinstance(k, str) and isinstance(v, str) and v.strip())]
    if bad:
        r.bad(g, f"labels 插件的 STEPS 里 {bad} 的中文名不是非空字符串")
    bad = [x for x in ns["QUIET"] if not isinstance(x, str)]
    if bad:
        r.bad(g, f"labels 插件的 QUIET 里 {bad} 不是步骤名（字符串）")
    if ns["PLAN_TITLES"]:
        try:
            got = ns["PLAN_TITLES"]()
        except Exception as e:  # noqa: BLE001
            r.bad(g, f"labels 插件的 PLAN_TITLES() 出错：{type(e).__name__}: {e}")
            return
        if not isinstance(got, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in got.items()):
            r.bad(g, f"labels 插件的 PLAN_TITLES() 要返回 {{编号: 标题}}（都是字符串），得到 {type(got).__name__}")


def check_plugins(r):
    g = "plugins"
    core = core_modules() - {"taskq_api"}
    try:
        import recipes  # 核心的表 + 插件（import 时 cfg.extend 合进来；插件导出的表不对、重名都在这里报）
        import relay_plugins  # noqa: F401
        r.checked(g, 2)
        for k, v in recipes.HOOKS.items():
            if not callable(v):
                r.bad(g, f"recipes 插件的 HOOKS[{k!r}] 不是函数")
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"核心合插件时出错：{e}")
    check_labels(r)
    try:
        cfg.extend({"CONSUMERS": {}}, "consumers")
        cfg.extend({"SMOKE": {}}, "smoke")
        r.checked(g, 2)
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"核心合插件时出错：{e}")
    for d in cfg.plugin_dirs():
        for p in sorted(glob.glob(os.path.join(d, "*.py")) + glob.glob(os.path.join(d, "*", "*.py"))):
            if "/tests/" in p or os.path.basename(p).startswith("test_"):
                continue
            r.checked(g)
            try:
                tree = ast.parse(open(p, encoding="utf-8").read(), p)
            except SyntaxError as e:
                r.bad(g, f"{p} 语法错：{e}")
                continue
            used = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    used |= {a.name.split(".")[0] for a in node.names}
                elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
                    used.add(node.module.split(".")[0])
            local = {os.path.splitext(os.path.basename(x))[0] for x in glob.glob(os.path.join(os.path.dirname(p), "*.py"))}
            bad = sorted((used & core) - local)
            if bad:
                r.bad(g, f"{os.path.relpath(p, cfg.REPO)} 直接 import 核心的 {bad}：插件只 import taskq_api")


# ---------------- 实验表 ----------------
STEP_NEED = ("name", "group", "cmd", "done")


def _steps(exps, final):
    for e in exps:
        for st in e.get("after") or []:
            yield e, "after", st
        for c in e.get("group_after") or []:
            for st in c:
                yield e, "group_after", st
    for f in final:
        yield None, "FINAL", f


def check_experiments(r):
    import experiments as E
    import spec as S
    M = cfg.mtab()
    g = "experiments"
    try:
        exps, final = E.load()
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"实验表读不了：{e}")
        return False
    if not exps:
        r.note(g, f"没有实验（OUTPUTS[\"experiments\"] = {cfg.outputs('experiments')!r}）")
    for e in exps:
        where = f"实验 {e.get('id')}"
        r.checked(g)
        for k in ("id", "exp", "recipe"):
            if not e.get(k):
                r.bad(g, f"{where} 没有 {k}")
        types = dict(nice=int, hours=(int, float), each=dict, after=list, group_after=list, names=dict, params=dict, fields=dict)
        for k, t in types.items():
            if k in e and not isinstance(e[k], t):
                r.bad(g, f"{where} 的 {k} 类型不对（要 {t.__name__ if isinstance(t, type) else '数字'}）")
        for k, v in (e.get("each") or {}).items():
            if not isinstance(v, (list, tuple)) or not v:
                r.bad(g, f"{where} 的 each[{k!r}] 要是非空列表")
        if not all(isinstance(c, list) for c in e.get("group_after") or []):
            r.bad(g, f"{where} 的 group_after 要是一串串步骤（列表的列表）")
    hub_groups = set(M.MACHINES[M.hub()].get("groups") or {})
    for e, kind, st in _steps(exps, final):
        where = f"{'实验 ' + e['id'] if e else 'FINAL'} 的 {kind} 步骤 {st.get('name')}"
        r.checked(g)
        miss = [k for k in STEP_NEED if k not in st]
        if miss:
            r.bad(g, f"{where} 缺 {miss}")
            continue
        if st["group"] not in hub_groups:
            r.bad(g, f"{where} 的 group {st['group']!r} 不是 hub 的 pueue 组（{sorted(hub_groups)}）")
        if not callable(getattr(st["done"], "ok", None)):
            r.bad(g, f"{where} 的 done 没有 ok(root, names)")
        if kind == "FINAL" and st.get("rerun_if_newer") and not callable(getattr(st["done"], "started", None)):
            r.bad(g, f"{where} 有 rerun_if_newer，done 要有 started(root, names)")
        q = st.get("queue")
        if q is not None and (not isinstance(q, dict) or "key" not in q):
            r.bad(g, f"{where} 的 queue 要是带 key 的字典")
    names = {st["name"] for _, k, st in _steps(exps, final) if k == "after"}
    for e in exps:
        for c in e.get("group_after") or []:
            here = [st["name"] for st in c]
            for st in c:
                need = st.get("needs") or []
                for x in [need] if isinstance(need, str) else need:
                    if x not in names and x not in here:
                        r.bad(g, f"实验 {e['id']} 的步骤 {st['name']} 的 needs {x!r} 不是任何实验的每个任务的步骤")
    watch = {x for f in final for x in f.get("rerun_if_newer") or []}
    for e, kind, st in _steps(exps, final):
        if st.get("name") in watch and kind != "FINAL" and not callable(getattr(st.get("done"), "mtime", None)):
            r.bad(g, f"步骤 {st['name']} 被 FINAL 的 rerun_if_newer 看着，done 要有 mtime(root, names)")
    bad = sorted(watch - {st.get("name") for _, _, st in _steps(exps, final)})
    if bad:
        r.bad(g, f"FINAL 的 rerun_if_newer 里 {bad} 不是任何步骤")
    try:
        ts = E.tasks()
    except S.SpecError as e:
        r.bad(g, f"实验表展开成任务时出错：{e}")
        return False
    labels = {}
    for k, t in ts.items():
        labels.setdefault(E.label(E.experiment(t["exp"]), t), []).append(k)
    dup = {lb: ks for lb, ks in labels.items() if len(ks) > 1}
    if dup:
        r.bad(g, f"任务的标签重复（hub 的 pueue 标签、步骤的标签会撞）：{dict(list(dup.items())[:5])}")
    return True


# ---------------- 每个任务 ----------------
def check_tasks(r):
    import experiments as E
    import spec as S
    g = "tasks"
    seen = set()
    for k, t in E.tasks().items():
        r.checked(g)
        sp = t["spec"]
        for name, fn in (("说明", lambda: sp),
                         ("执行机的 k=v 参数", lambda: S.resolve(sp["recipe"], S.parse_kv(S.kv_args(sp["recipe"], sp["params"])[1:]))),
                         ("送到执行机的说明（--spec）", lambda: S.from_sent(json.dumps(S.sendable(sp))))):
            try:
                S.shell_lines(fn(), "/d")
            except Exception as e:  # noqa: BLE001
                r.bad(g, f"{k}：按{name}变不成 shell 变量：{type(e).__name__}: {e}")
        try:
            S.outputs(sp)
        except Exception as e:  # noqa: BLE001
            r.bad(g, f"{k}：outputs 出错：{e}")
        check_smoke(r, k, sp, seen)


def check_smoke(r, k, sp, seen):
    """这个任务的冒烟任务（smoke.py 的定义）解析得了、环境和真任务一样、冒烟文件夹找得到；同样的冒烟只查一次。"""
    import spec as S
    try:
        d = SG.definition(sp["recipe"], sp.get("names"))
    except Exception as e:  # noqa: BLE001
        r.bad("tasks", f"{k}：冒烟定义出错：{e}")
        return
    if d is None:
        return
    names = sp.get("names") or {}
    params = {p: names[p] for p in d["by"] if p in names}
    params.update(SG.subst(d["params"], "SID"))
    if d["vars"]:
        params["vars"] = SG.subst(d["vars"], "SID")
    sig = json.dumps([sp["recipe"], params], sort_keys=True, default=str)
    if sig in seen:
        return
    seen.add(sig)
    try:
        sm = S.resolve(sp["recipe"], dict(params, machines=["x"], key="_smoke/x/SID"))
        S.shell_lines(sm, "/d")
        if S.envs_of(sm) != S.envs_of(sp):
            r.bad("tasks", f"{k}：冒烟任务的环境 {S.envs_of(sm)} 和真任务的 {S.envs_of(sp)} 不一样（环境参数写进 by）")
        if not SG.smoke_dirs(sm, "SID"):
            r.bad("tasks", f"{k}：冒烟任务的产出不在名字带 {{smoke}} 的文件夹下（out 或 vars 的 {{data}}/…）")
    except Exception as e:  # noqa: BLE001
        r.bad("tasks", f"{k}：冒烟任务解析不了：{type(e).__name__}: {e}")


# ---------------- 每个步骤 ----------------
def _step_cases(root, py):
    """每个步骤用得上的 names：每个任务的步骤 → 每个实验里这一步适用的第一个任务；实验一级的 → group_names；FINAL → data、python。"""
    import experiments as E
    exps, final = E.load()
    mine = {}
    for k, t in E.tasks().items():
        mine.setdefault(t["exp"], []).append(t)
    for e in exps:
        for st in e.get("after") or []:
            t = next((x for x in mine.get(e["id"], []) if E.applies(st, x)), None)
            if t:
                yield f"实验 {e['id']} 的步骤 {st['name']}", st, E.task_names(t, root, py), e, True
        if e.get("group_after"):
            names = E.group_names(e, root, py)
            for c in e["group_after"]:
                for st in c:
                    yield f"实验 {e['id']} 的步骤 {st['name']}", st, names, e, False
    for f in final:
        yield f"FINAL 的步骤 {f['name']}", f, dict(data=root, python=py), None, False


def check_steps(r, root):
    import spec as S
    import advance as A
    M = cfg.mtab()
    g = "steps"
    py = sys.executable
    for where, st, names, e, per_task in _step_cases(root, py):
        r.checked(g)
        try:
            S.fill(st["cmd"], names, keep=())
        except Exception as err:  # noqa: BLE001
            r.bad(g, f"{where} 的 cmd 填不上：{err}")
        try:
            st["done"].ok(root, names)
        except Exception as err:  # noqa: BLE001
            r.bad(g, f"{where} 的 done.ok 在空的数据目录里出错：{type(err).__name__}: {err}")
        q = st.get("queue")
        if not q:
            continue
        try:
            mach = next(iter(M.RELAY), M.hub())
            item = {k: S.fill(v, dict(names, data="{data}"), keep=()) for k, v in q.items() if k not in ("nice", "inputs", "disk")}
            item["machines"] = [mach]
            s = S.resolve("command", S.parse_kv(S.kv_args("command", item)[1:]))
            S.shell_lines(s, "/d")
        except Exception as err:  # noqa: BLE001
            r.bad(g, f"{where} 的 queue 变不成总队列里的任务：{type(err).__name__}: {err}")
        places = [t for t in A.targets(st["name"], st["group"]) if t["kind"] != "hub"] if st["name"] else []
        relay_places = [t for t in places if t["kind"] != "queue" or t["machine"] in M.RELAY]
        if relay_places and not ("inputs" in q or "inputs_cmd" in q or "--stage" in st["cmd"]):
            r.bad(g, f"{where} 可能放到执行机上（PLACE），但没法列出输入：queue 里写 inputs 或 inputs_cmd，或命令支持 --stage needs")


def check_needs(r, root):
    """命令带 --stage 的步骤真跑一次 --stage needs:<阶段>（只列输入、不读写数据），要能列出输入。root：hub 的数据目录
    （TASKQ_DATA 或机器表的 data，本机有时），没有时用空的临时目录。"""
    import spec as S
    import advance as A
    g = "needs"
    todo, seen = [], set()
    for where, st, names, e, per_task in _step_cases(root, sys.executable):
        try:
            cmd = S.fill(st["cmd"], names, keep=())
        except Exception:  # noqa: BLE001  在 steps 一组报
            continue
        if "--stage" not in cmd or (e or {}).get("id", "") + st["name"] in seen:
            continue
        seen.add((e or {}).get("id", "") + st["name"])
        todo.append((where, cmd))
    old = os.environ.get("TASKQ_DATA")
    os.environ["TASKQ_DATA"] = root
    try:
        with ThreadPoolExecutor(8) as ex:
            res = list(ex.map(lambda x: A.step_needs(x[1]), todo))
    finally:
        if old is None:
            os.environ.pop("TASKQ_DATA", None)
        else:
            os.environ["TASKQ_DATA"] = old
    for (where, cmd), got in zip(todo, res):
        r.checked(g)
        if got is None:
            r.bad(g, f"{where}：{A.no_needs(cmd)}")
        elif not got:
            r.note(g, f"{where}：--stage needs 没列出任何输入")


# ---------------- 代码包 ----------------
def check_bundle(r):
    M = cfg.mtab()
    import relay_bundle as B
    g = "bundle"
    skip = ("*/test_*", "*/tests/*", "*/testdata/*")
    tests = {}
    for n in M.RELAY:
        r.checked(g)
        out = tempfile.mkdtemp(prefix="check_bundle.")
        try:
            tests[n] = sorted(f for f in B.contents(n, None) if any(fnmatch.fnmatch(f, p) for p in skip))
            B.build(n, None, out=out)
        except Exception as e:  # noqa: BLE001
            r.bad(g, f"{n} 的代码包建不出来：{e}")
        finally:
            shutil.rmtree(out, ignore_errors=True)
    have = {n: x for n, x in tests.items() if x}
    if have:
        x = next(iter(have.values()))
        r.note(g, f"{'、'.join(have)} 的包里有测试文件（{len(x)} 个，如 {x[0]}），不要的写进 OUTPUTS 的 bundle_skip")


CHECKS = dict(machines=check_machines, policy=check_policy, plugins=check_plugins, experiments=check_experiments,
              tasks=check_tasks, steps=check_steps, needs=check_needs, bundle=check_bundle)


def data_root(given, empty):
    """--stage needs 用的数据目录：--data、TASKQ_DATA、本机有的 hub 数据目录，都没有 → empty。"""
    M = cfg.mtab()
    d = given or os.environ.get("TASKQ_DATA") or M.MACHINES[M.hub()].get("data")
    return os.path.expanduser(d) if d and os.path.isdir(os.path.expanduser(d)) else empty


def run(only=None, quick=False, data=None):
    """→ Report。data：--stage needs 用的数据目录（默认见 data_root）。"""
    r = Report()
    groups = [x for x in GROUPS if (not only or x in only) and not (quick and x in SLOW and not only)]
    root = tempfile.mkdtemp(prefix="check_data.")
    ok_exps = True
    try:
        for x in groups:
            if x in ("tasks", "steps", "needs") and not ok_exps:
                r.note(x, "实验表读不了，跳过")
                continue
            try:
                args = (r, root) if x == "steps" else (r, data_root(data, root)) if x == "needs" else (r,)
                got = CHECKS[x](*args)
                if x == "experiments":
                    ok_exps = got
            except Exception as e:  # noqa: BLE001
                r.bad(x, f"这一组的检查自己出错了：{type(e).__name__}: {e}\n{traceback.format_exc(limit=3)}")
            r.counts.setdefault(x, 0)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return r


def main(argv):
    ap = argparse.ArgumentParser(description="核对机器表、policy、插件、实验表、任务、步骤、代码包（只读）")
    ap.add_argument("--quick", action="store_true", help="不跑 --stage needs、不建代码包")
    ap.add_argument("--only", nargs="+", choices=GROUPS, metavar="组", help=" ".join(GROUPS))
    ap.add_argument("--data", help="--stage needs 用的数据目录（默认 TASKQ_DATA、本机的 hub 数据目录，都没有时用空的临时目录）")
    a = ap.parse_args(argv)
    r = run(a.only, a.quick, a.data)
    for grp, msg in r.problems:
        print(f"错  [{grp}] {msg}", file=sys.stderr)
    for grp, msg in r.notes:
        print(f"注意 [{grp}] {msg}", file=sys.stderr)
    per = {}
    for grp, _ in r.problems:
        per[grp] = per.get(grp, 0) + 1
    for grp, n in r.counts.items():
        print(f"{grp:<12} 查了 {n:>4} 项  " + (f"{per[grp]} 个问题" if per.get(grp) else "通过"))
    print(f"{cfg.config_dir()}：" + (f"{len(r.problems)} 个问题" if r.problems else "没有问题"))
    return 1 if r.problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
