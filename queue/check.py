#!/usr/bin/env python3
"""核对一个使用者的配置：机器表、policy、插件、实验表（字段按 schema.py 的字段表）、先后（依赖）、每个任务和步骤都能变成要跑的命令、
步骤的 inputs_cmd 列得出输入、原始数据在 store 上、执行机的代码包建得出来。不改任何东西；只有 raw 一组连 store（只读）。

    python3 queue/check.py [--quick] [--only 组…]

  --quick  不跑 inputs、raw、bundle（最慢、要连 store 的几组）
  --only   只查这几组（machines policy plugins experiments flow tasks steps inputs raw bundle）
退出码 0 = 没有问题，1 = 有问题。问题（和只提醒的"注意"）写在 stderr，每组的结果写在 stdout。
"""
import argparse
import ast
import datetime as dt
import fnmatch
import glob
import hashlib
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
try:  # 插件导出的表不对、重名在 import 时报，算 plugins 一组的问题
    import recipes  # noqa: E402
    import relay_plugins  # noqa: E402
    assert relay_plugins
    import smoke_gate as SG  # noqa: E402
    PLUGIN_ERR = None
except Exception as e:  # noqa: BLE001
    PLUGIN_ERR = e
import schema  # noqa: E402

GROUPS = ("machines", "policy", "plugins", "experiments", "flow", "tasks", "steps", "inputs", "raw", "bundle")
SLOW = ("inputs", "raw", "bundle")
NEED_EXPS = ("flow", "tasks", "steps", "inputs", "raw")


class Report:
    def __init__(self):
        self.problems, self.notes, self.counts = [], [], {}

    def bad(self, group, msg):
        self.problems.append((group, msg))

    def note(self, group, msg):
        self.notes.append((group, msg))

    def checked(self, group, n=1):
        self.counts[group] = self.counts.get(group, 0) + n


# ---------------- 字段的类型（schema.py 的字段表用的类型名） ----------------
def _tup(v, n=None):
    return isinstance(v, (tuple, list)) and (n is None or len(v) == n)


def _num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _hhmm(x):
    return isinstance(x, str) and re.fullmatch(r"\d\d:\d\d", x)


def _day(x):
    return x is None or isinstance(x, str) and re.fullmatch(r"\d{4}-\d\d-\d\d", x)


def _regex(v):
    try:
        re.compile(v)
        return isinstance(v, str)
    except (re.error, TypeError):
        return False


def _strs(v):
    return isinstance(v, str) or _tup(v) and all(isinstance(x, str) for x in v)


def _when(x):
    try:
        dt.datetime.strptime(x, "%Y-%m-%d %H:%M")
        return True
    except (TypeError, ValueError):
        return False


TYPES = {  # 类型名 → (核对, 怎么写)
    "str": (lambda v: isinstance(v, str), "字符串"),
    "str?": (lambda v: v is None or isinstance(v, str), "字符串或 None"),
    "strs": (_strs, "字符串或字符串的列表"),
    "strs?": (lambda v: v is None or _strs(v), "字符串、字符串的列表或 None"),
    "int": (lambda v: isinstance(v, int) and not isinstance(v, bool), "整数"),
    "int?": (lambda v: v is None or isinstance(v, int) and not isinstance(v, bool), "整数或 None"),
    "num?": (lambda v: v is None or _num(v), "数或 None"),
    "bool": (lambda v: isinstance(v, bool), "True / False"),
    "dict": (lambda v: isinstance(v, dict), "dict"),
    "dict?": (lambda v: v is None or isinstance(v, dict), "dict 或 None"),
    "regex": (_regex, "正则"),
    "pair": (lambda v: v is None or _tup(v, 2), "两项的元组"),
    "pair?": (lambda v: v is None or _tup(v, 2), "两项的元组或 None"),
    "triple?": (lambda v: v is None or _tup(v, 3) and all(_num(x) for x in v), "三个数的元组或 None"),
    "disk?": (lambda v: v is None or _tup(v, 3) and isinstance(v[0], str) and _num(v[1]) and (v[2] is None or _num(v[2])),
              "(目录, GB, GB 或 None) 或 None"),
    "tunnel": (lambda v: _tup(v) and set(v) <= {"push", "pull"}, "push、pull 的元组"),
    "roles": (lambda v: _tup(v) and set(v) <= {"hub", "store", "backup"}, "hub、store、backup 的元组"),
    "sched?": (lambda v: v in (None, "pueue", "slurm"), "pueue、slurm 或 None"),
    "device": (lambda v: v in ("gpu", "cpu"), "gpu 或 cpu"),
    "strmap": (lambda v: isinstance(v, dict) and all(isinstance(k, str) and isinstance(x, str) for k, x in v.items()), "{字符串: 字符串}"),
    "intmap": (lambda v: isinstance(v, dict) and all(isinstance(x, int) and x >= 0 for x in v.values()), "{机器: 不小于 0 的整数}"),
    "groups": (lambda v: isinstance(v, dict) and all(_tup(g, 3) and isinstance(g[0], int) and (g[1] is None or isinstance(g[1], str))
                                                     and isinstance(g[2], dict) for g in v.values()),
               "{组名: (同时跑几个, 卡号或 None, {环境变量})}"),
    "envs": (lambda v: isinstance(v, dict), "{环境名: 写法}"),
    "jobs": (lambda v: isinstance(v, dict) and set(v) <= {"gpu", "cpu"} and all(isinstance(o, dict) for o in v.values()),
             "{gpu|cpu: dict(partition, gres, cpus, mem)}"),
    "workers": (lambda v: _tup(v) and all(isinstance(w, dict) for w in v), "worker 的列表（每项 dict，见 WORKER_FIELDS）"),
    "hours": (lambda v: _tup(v) and all(_tup(p, 4) and _hhmm(p[0]) and _hhmm(p[1]) and _day(p[2]) and _day(p[3]) for p in v),
              "[(\"HH:MM\", \"HH:MM\", 第一天 \"YYYY-MM-DD\" 或 None, 最后一天 或 None)]"),
    "steps": (lambda v: _tup(v) and all(isinstance(s, dict) for s in v), "步骤（dict）的列表"),
    "auto": (lambda v: isinstance(v, dict), "dict"),
    "pause": (lambda v: isinstance(v, dict) and all(_tup(x, 2) and (x[0] is None or _when(x[0])) and isinstance(x[1], str) for x in v.values()),
              "{机器: (\"YYYY-MM-DD HH:MM\" 或 None, 原因)}"),
    "place": (lambda v: isinstance(v, dict) and all(x == "any" or _tup(x) and x and all(isinstance(n, str) for n in x) for x in v.values()),
              "{步骤或做法: [执行机, …] 或 \"any\"}"),
    "slurm": (lambda v: isinstance(v, dict), "{机器: dict(gpu=…, cpu=…)}"),
    "cron": (lambda v: _tup(v) and all(_tup(x, 2) and all(isinstance(y, str) for y in x) for x in v), "((时间, 子命令), …)"),
    "outputs": (lambda v: isinstance(v, dict), "dict（见 OUTPUT_FIELDS）"),
}
assert {t for tab in (schema.MACHINE_FIELDS, schema.WORKER_FIELDS, schema.POLICY_FIELDS, schema.OUTPUT_FIELDS,
                      schema.EXP_FIELDS, schema.STEP_FIELDS, schema.RECIPE_FIELDS) for t, _, _ in tab.values()} <= set(TYPES)


def fields(r, g, where, d, table):
    """d 的字段：不认识的、缺必写的、类型不对的都报。"""
    r.checked(g)
    for k in sorted(set(d) - set(table)):
        r.bad(g, f"{where}：不认识的字段 {k}（taskq machines fields 看能写什么）")
    for k, (t, df, _) in table.items():
        if k not in d:
            if df is schema.REQUIRED:
                r.bad(g, f"{where}：缺必写的字段 {k}")
            continue
        ok, how = TYPES[t]
        if not ok(d[k]):
            r.bad(g, f"{where} 的 {k} 要写成{how}，现在 {d[k]!r}")


# ---------------- 机器表 ----------------
def check_machines(r):
    M = cfg.mtab()
    import slurm as SL
    g = "machines"
    for n, m in M.MACHINES.items():
        where = f"MACHINES[{n!r}]"
        fields(r, g, where, m, schema.MACHINE_FIELDS)
        for i, w in enumerate(m.get("workers") or ()):
            if isinstance(w, dict):
                fields(r, g, f"{where} 的 workers[{i}]", w, schema.WORKER_FIELDS)
    for role in ("hub", "store"):
        got = [n for n in M.MACHINES if role in (M.MACHINES[n].get("roles") or ())]
        if len(got) != 1:
            r.bad(g, f"roles 带 {role} 的机器要正好一台，现在 {got or '没有'}")
    for n in M.names("backup"):
        if not M.MACHINES[n].get("backup"):
            r.bad(g, f"MACHINES[{n!r}] 是 backup，要写 backup（备份放在哪）")
    try:
        st, hub = M.store(), M.hub()
    except LookupError:
        st = hub = None
    tunneled = [n for n in M.executors() if M.MACHINES[n].get("tunnel")]
    locked = [n for n in M.executors() if any(M.is_spec(ev) for ev in (M.MACHINES[n].get("envs") or {}).values())]
    if hub and M.MACHINES[hub].get("ssh") is not None:
        r.bad(g, f"hub（{hub}）的 ssh 写 None：hub 就是跑调度的这台")
    if st:
        sm = M.MACHINES[st]
        for k, why in (("data", "数据目录（结果的家）"), ("root", "跑 site_ops 用的代码包、hashes 缓存、临时目录放在这里")):
            if not sm.get(k):
                r.bad(g, f"store（{st}）要写 {k}：{why}")
        if tunneled and not sm.get("transfer"):
            r.bad(g, f"store（{st}）要写 transfer：{'、'.join(tunneled)} 经隧道和它传文件，文件服务开在 store 上")
        local = [n for n in M.executors() if M.MACHINES[n].get("ssh") is None]
        if local and sm.get("ssh") is not None:
            r.bad(g, f"{'、'.join(local)} 和 hub 在同一台上（ssh 是 None），store（{st}）在别的机器上连不到它，store 也要在这台上")
    if hub and locked:
        hm = M.MACHINES[hub]
        for k in ("root", "transfer") if any(M.MACHINES[n].get("tunnel") for n in locked) else ("root",):
            if not hm.get(k):
                r.bad(g, f"hub（{hub}）要写 {k}：{'、'.join(locked)} 用锁好的环境，包在 hub 的 wheelhouse 里，由 hub 送过去")
    for n, m in M.MACHINES.items():
        where = f"MACHINES[{n!r}]"
        if m.get("scheduler") and not m.get("root"):
            r.bad(g, f"{where}：执行机（写了 scheduler）要写 root")
        if m.get("root") and not m.get("scheduler") and n not in (st, hub):
            r.bad(g, f"{where}：写了 root 没写 scheduler（只有 store、hub 可以只有根目录）")
        if m.get("scheduler") == "pueue" and not m.get("workers"):
            r.bad(g, f"{where}：pueue 执行机要写 workers")
        if m.get("scheduler") == "slurm":
            if not m.get("jobs"):
                r.bad(g, f"{where}：slurm 执行机要写 jobs")
            if m.get("workers"):
                r.bad(g, f"{where}：slurm 执行机不写 workers（按 jobs 交作业）")
            if m.get("job_names") is not None and m["job_names"] not in SL.STYLES:
                r.bad(g, f"{where} 的 job_names {m['job_names']!r} 不在 slurm.STYLES 里（{sorted(SL.STYLES)}）")
        if m.get("scheduler") != "pueue" and m.get("workers"):
            r.bad(g, f"{where}：只有 pueue 执行机写 workers")
        if M.is_exec(n):
            ws = M.workers(n)
            ids = [w["name"] for w in ws]
            if len(set(ids)) != len(ids):
                r.bad(g, f"{where} 的 worker 名字重复：{ids}（同一张卡写了两项）")
            clash = sorted(set(m.get("manual_groups") or {}) & set(ids))
            if clash:
                r.bad(g, f"{where} 的 manual_groups 和 worker 的组重名：{clash}")
            b = (m.get("budget") or (100, 50))[1]
            for cap in ("cap", "blob_cap"):
                if m.get(cap) and b >= m[cap]:
                    r.bad(g, f"{where} 的 budget 第二项 {b} GB 不小于 {cap} {m[cap]} GB，什么都放不下")
        for en, ev in (m.get("envs") or {}).items():
            if M.is_spec(ev):
                if M.spec_hash(en) is None:
                    r.bad(g, f"{where} 的环境 {en} 是 spec，但没有它的描述 {en}.yml")
                elif not os.path.isfile(M.lock_path(en, M.spec_variant(n, en))):
                    r.note(g, f"{where} 的环境 {en} 还没有锁 {os.path.basename(M.lock_path(en, M.spec_variant(n, en)))}"
                              f"（taskq env lock {en} --variant {M.spec_variant(n, en)}）")
            elif not (_tup(ev) and ev and ev[0] in ("conda", "venv")):
                r.bad(g, f"{where} 的环境 {en} 要写成 (\"conda\", 名字)、(\"venv\", 目录, 底子) 或 \"spec\"，现在 {ev!r}")
            elif ev[0] == "venv" and (len(ev) != 3 or ev[2] not in m["envs"]):
                r.bad(g, f"{where} 的环境 {en}：venv 的底子 {ev[2] if len(ev) > 2 else None!r} 不在这台的 envs 里")
            elif ev[0] == "conda" and not m.get("conda"):
                r.bad(g, f"{where} 的环境 {en} 是 conda，但这台没有 conda=(目录, 环境)")
        if m.get("quick") and _tup(m["quick"], 2) and not _regex(m["quick"][1]):
            r.bad(g, f"{where} 的 quick 第二项不是正则")
    local = [n for n in M.executors() if M.MACHINES[n].get("ssh") is None]
    if len(local) > 1:
        r.bad(g, f"ssh=None（和 hub 在同一台上）的执行机最多一个，现在 {local}")
    if st and M.MACHINES[st].get("data"):
        real = lambda p: os.path.realpath(os.path.expanduser(p))  # noqa: E731
        for n in M.executors():
            if n != st and M.MACHINES[n].get("ssh") is None and M.MACHINES[st].get("ssh") is None \
                    and (real(M.MACHINES[n]["root"]) + "/").startswith(real(M.MACHINES[st]["data"]).rstrip("/") + "/"):
                r.bad(g, f"MACHINES[{n!r}] 的 root 在 store 的数据目录里面：执行机的数据要和结果的家分开")
    check_specs(r, M)


PLACEHOLDERS = {"torch_mm", "abi", "cp"}  # 环境描述的 pip 包里能写的占位（sites/envs.py 的 pip_lines）


def check_specs(r, M):
    """<配置文件夹>/specs/*.yml：读得了、有能编译的 <名>.check.py、pip 里只有认得的占位，每个 torch 变体在一台假的执行机上
    都生成得出建环境的脚本。算在 machines 一组。"""
    g = "machines"
    try:
        import envs as EV
    except ImportError as e:
        r.note(g, f"读不了环境描述（{e}），没查 specs/")
        return
    names = sorted(os.path.basename(p)[:-4] for p in glob.glob(os.path.join(M.SPECS, "*.yml")))
    check_locks(r, M, names)
    fake = "_check_specs"
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
            M.MACHINES[fake] = dict(label=fake, hostname=r"\A\Z", ssh="-", scheduler="pueue", root="/check/root",
                                    conda=("/check/conda", "base"), envs=envs, workers=[dict(cpus=1)])
            try:
                EV.build_script(fake, n, store="/check/root/blobs")
            except Exception as e:  # noqa: BLE001
                r.bad(g, f"环境描述 {n}（torch 变体 {v}）生成不出建环境的脚本：{type(e).__name__}: {e}")
            finally:
                M.MACHINES.pop(fake, None)


def check_locks(r, M, names):
    """<名>.<变体>.lock.json：有对应的描述，记的 spec_sha256 是现在的描述（描述改了要重新锁）。"""
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
    g = "policy"
    mine = {k: getattr(P, k) for k in dir(P) if k.isupper() and not k.startswith("_") and not callable(getattr(P, k))
            and not isinstance(getattr(P, k), type(sys))}
    extra = sorted(k for k in set(mine) - set(schema.POLICY_FIELDS) if not isinstance(mine[k], (str, int, float, tuple)))
    for k in extra:  # 只报表一样的大写名字：字符串、数这种常量（QR 之类）是写配置时自己用的
        r.bad(g, f"policy.py 的 {k} 不是 policy 的字段（能写：{'、'.join(schema.POLICY_FIELDS)}）")
    fields(r, g, "policy.py", {k: v for k, v in mine.items() if k in schema.POLICY_FIELDS}, schema.POLICY_FIELDS)
    every, ex = set(M.MACHINES), set(M.executors())
    auto = getattr(P, "AUTO", {}) or {}
    for k, v in auto.items():
        if k not in PR.AUTO_SWITCHES:
            r.bad(g, f"AUTO 里的 {k!r} 不是开关（能写：{'、'.join(PR.AUTO_SWITCHES)}；停一台机器用 PAUSE）")
        elif not isinstance(v, bool) and not (k == "envs" and isinstance(v, dict)):
            r.bad(g, f"AUTO[{k!r}] 要写 True / False（envs 也可以是 {{机器: [环境 或 (环境, 变体)]}}）")
    for mn, lst in (auto.get("envs") if isinstance(auto.get("envs"), dict) else {}).items():
        if mn not in ex:
            r.bad(g, f"AUTO['envs'] 里的 {mn!r} 不是执行机")
        for x in lst if isinstance(lst, (list, tuple)) else [None]:
            e = x if isinstance(x, str) else x[0] if isinstance(x, (list, tuple)) and 1 <= len(x) <= 2 else None
            if e is None or M.spec_hash(e) is None:
                r.bad(g, f"AUTO['envs'][{mn!r}] 的 {x!r} 要是有描述的环境名或 (环境, 变体)")
    for name, ok in (("PAUSE", every), ("READY", ex), ("SLURM", set(M.names("slurm")))):
        bad = sorted(set(getattr(P, name, {}) or {}) - ok)
        if bad:
            r.bad(g, f"{name} 里的 {bad} 不是{'机器' if name == 'PAUSE' else '执行机' if name == 'READY' else 'Slurm 执行机'}")
    for k, v in (getattr(P, "PLACE", {}) or {}).items():
        bad = [] if v == "any" else [n for n in v if n not in ex]
        if bad:
            r.bad(g, f"PLACE[{k!r}] 里的 {bad} 不是执行机（写执行机的名字，或 \"any\"）")
    out = getattr(P, "OUTPUTS", None)
    if isinstance(out, dict):
        fields(r, g, "policy.py 的 OUTPUTS", out, schema.OUTPUT_FIELDS)


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
    r.checked(g, 2)
    if PLUGIN_ERR is not None:
        r.bad(g, f"核心合插件时出错：{PLUGIN_ERR}")
    else:
        for k, v in recipes.HOOKS.items():
            if not callable(v):
                r.bad(g, f"recipes 插件的 HOOKS[{k!r}] 不是函数")
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
def check_experiments(r):
    import experiments as E
    g = "experiments"
    try:
        exps, final = E.load()  # 字段（EXP_FIELDS、STEP_FIELDS）在加载时核对
        ts = E.tasks()
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"实验表读不了：{e}")
        return False
    if not exps:
        r.note(g, f"没有实验（OUTPUTS[\"experiments\"] = {cfg.outputs('experiments')!r}）")
    for e in exps:
        r.checked(g)
        for k, v in (e.get("each") or {}).items():
            if not isinstance(v, (list, tuple)) or not v:
                r.bad(g, f"实验 {e['id']} 的 each[{k!r}] 要是非空列表")
    for st in [st for e in exps for st in e["after"] + e["group_after"]] + final:
        r.checked(g)
        for k, (t, _, _) in schema.STEP_FIELDS.items():
            if k in st and not TYPES[t][0](st[k]):
                r.bad(g, f"步骤 {st['name']} 的 {k} 要写成{TYPES[t][1]}，现在 {st[k]!r}")
        if len(st["out"]) < 1 or not st["done"]:
            r.bad(g, f"步骤 {st['name']}：out、done 至少各写一个")
    labels = {}
    for k, t in ts.items():
        labels.setdefault(E.label(E.experiment(t["exp"]), t), []).append(k)
    dup = {lb: ks for lb, ks in labels.items() if len(ks) > 1}
    if dup:
        r.bad(g, f"任务的简称重复（步骤的实例 id 会撞）：{dict(list(dup.items())[:5])}")
    return True


# ---------------- 先后 ----------------
_FLOW = {}


def flow():
    if "f" not in _FLOW:
        from flow import compile as FC
        _FLOW["f"] = FC.build()
    return _FLOW["f"]


def check_flow(r):
    """实例和先后：产出重叠、输入落在自己的产出里、有环、模板填不上（flow/compile.py 的 errors）；PLACE 写的步骤、做法要存在，
    能去的执行机要有这种 worker。"""
    M = cfg.mtab()
    import policy as P
    import experiments as E
    g = "flow"
    f = flow()
    r.checked(g, len(f.instances))
    for e in f.errors:
        r.bad(g, e)
    exps, final = E.load()
    steps = {st["name"]: st for e in exps for st in e["after"] + e["group_after"]} | {st["name"]: st for st in final}
    trains = {e["recipe"] for e in exps}
    for k, v in (getattr(P, "PLACE", {}) or {}).items():
        if k not in steps and k not in trains:
            r.bad(g, f"PLACE[{k!r}] 不是任何步骤或训练的做法（步骤：{'、'.join(sorted(steps)) or '没有'}）")
            continue
        dev = steps[k]["device"] if k in steps else "gpu"
        cand = M.executors() if v == "any" else [n for n in v if n in M.MACHINES]
        if cand and not any(M.workers(n, dev) for n in cand):
            r.bad(g, f"PLACE[{k!r}] 的执行机 {cand} 都没有 {dev} 的 worker")
    for k, st in steps.items():
        if k not in (getattr(P, "PLACE", {}) or {}) and not any(M.workers(n, st["device"]) for n in M.executors()):
            r.bad(g, f"步骤 {k} 要 {st['device']} 的 worker，但没有一台执行机有")
    for i in f.instances.values():
        if i.step and not i.inputs and not i.step.get("inputs_cmd"):
            r.note(g, f"{i.id} 没写 inputs：只跟在同一个任务的前一步后面，执行机上拿不到别的输入")
            break


# ---------------- 每个任务 ----------------
def check_tasks(r):
    import experiments as E
    import spec as S
    g = "tasks"
    seen = {}
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
    """这个任务的冒烟任务（smoke.py 的定义）解析得了、环境和真任务一样、冒烟文件夹找得到；同样的冒烟只查一次。
    store 那边出错（和任务无关）每种做法只报一次，这种做法后面的任务不再去 store 算。"""
    import spec as S
    if ("store", sp["recipe"]) in seen:
        return
    try:
        d = SG.definition(sp["recipe"], SG.real_params(sp))
    except SG.StoreError as e:
        seen["store", sp["recipe"]] = True
        r.bad("tasks", f"做法 {sp['recipe']} 的冒烟定义没查完（从 {k} 起）：{e}")
        return
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
    seen[sig] = True
    try:
        sm = S.resolve(sp["recipe"], dict(params, key="_smoke/x/SID"))
        S.shell_lines(sm, "/d")
        if S.envs_of(sm) != S.envs_of(sp):
            r.bad("tasks", f"{k}：冒烟任务的环境 {S.envs_of(sm)} 和真任务的 {S.envs_of(sp)} 不一样（环境参数写进 by）")
        if not SG.smoke_dirs(sm, "SID"):
            r.bad("tasks", f"{k}：冒烟任务的产出不在名字带 {{smoke}} 的文件夹下（out 或 vars 的 {{data}}/…）")
    except Exception as e:  # noqa: BLE001
        r.bad("tasks", f"{k}：冒烟任务解析不了：{type(e).__name__}: {e}")


# ---------------- 每个步骤 ----------------
def one_per_step():
    """每个实验的每个步骤取一个实例（FINAL 的各一个）。"""
    seen = {}
    for i in flow().instances.values():
        if i.step:
            seen.setdefault((i.exp, i.rule), i)
    return list(seen.values())


def check_steps(r):
    """每个步骤的实例都变得成总队列里的任务（做法 command 的参数 → 说明 → shell 变量），环境在有这种 worker 的执行机上有。"""
    import advance as A
    import spec as S
    M = cfg.mtab()
    g = "steps"
    for i in one_per_step():
        r.checked(g)
        try:
            p = A.queue_params(i)
            s = S.resolve("command", S.parse_kv(S.kv_args("command", p)[1:]))
            S.shell_lines(s, "/d")
        except Exception as e:  # noqa: BLE001
            r.bad(g, f"{i.id} 变不成总队列里的任务：{type(e).__name__}: {e}")
            continue
        envs = set(S.envs_of(s))
        if not any(envs <= set(M.machine(n)["envs"]) for n in M.executors() if M.workers(n, i.step["device"])):
            r.bad(g, f"{i.id} 要的环境 {sorted(envs)} 没有一台有 {i.step['device']} worker 的执行机全有")


def check_inputs(r):
    """写了 inputs_cmd 的步骤，每个实验真跑一次（在空的临时数据目录里，只列输入、不读写数据），要列得出输入。"""
    import spec as S
    g = "inputs"
    root = tempfile.mkdtemp(prefix="check_data.")
    todo = [i for i in one_per_step() if i.step.get("inputs_cmd")]

    def run(i):
        try:
            return S.listed_inputs(S.fill(i.step["inputs_cmd"], i.names), root, i.step.get("vars"), timeout=120), None
        except Exception as e:  # noqa: BLE001
            return None, str(e)
    try:
        with ThreadPoolExecutor(8) as ex:
            res = list(ex.map(run, todo))
    finally:
        shutil.rmtree(root, ignore_errors=True)
    for i, (got, err) in zip(todo, res):
        r.checked(g)
        if err:
            r.bad(g, f"{i.id}：{err}")
        elif not got:
            r.note(g, f"{i.id} 的 inputs_cmd 没列出任何输入")


def check_raw(r):
    """不在任何产出里的输入（原始数据）在 store 上都有（经 site_ops 的 probe，只读）。"""
    import relay_remote as RM
    g = "raw"
    raw = sorted({p for i in flow().instances.values() for p in i.raw})
    r.checked(g, len(raw))
    if not raw:
        return
    try:
        got = RM.store_op("probe", dict(pats=raw))
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"连不上 store 查原始数据：{e}")
        return
    for p in raw:
        if not (got.get(p) or {}).get("n"):
            who = [i.id for i in flow().instances.values() if p in i.raw]
            r.bad(g, f"原始数据 {p} 在 store 上没有（{', '.join(who[:3])}{' 等' if len(who) > 3 else ''} 要用；不是哪个步骤的产出）")


# ---------------- 代码包 ----------------
def check_bundle(r):
    import relay_bundle as B
    g = "bundle"
    skip = ("*/test_*", "*/tests/*", "*/testdata/*")
    r.checked(g)
    out = tempfile.mkdtemp(prefix="check_bundle.")
    try:
        tests = sorted(f for f in B.contents(None) if any(fnmatch.fnmatch(f, p) for p in skip))
        B.build(None, out=out)
    except Exception as e:  # noqa: BLE001
        r.bad(g, f"代码包建不出来：{e}")
        return
    finally:
        shutil.rmtree(out, ignore_errors=True)
    if tests:
        r.note(g, f"包里有测试文件（{len(tests)} 个，如 {tests[0]}），不要的写进 OUTPUTS 的 bundle_skip")


CHECKS = dict(machines=check_machines, policy=check_policy, plugins=check_plugins, experiments=check_experiments, flow=check_flow,
              tasks=check_tasks, steps=check_steps, inputs=check_inputs, raw=check_raw, bundle=check_bundle)


def run(only=None, quick=False):
    """→ Report。"""
    r = Report()
    groups = [x for x in GROUPS if (not only or x in only) and not (quick and x in SLOW and not only)]
    ok_exps = None
    for x in groups:
        if x in NEED_EXPS and (ok_exps is False or PLUGIN_ERR is not None):
            r.note(x, "实验表读不了，跳过")
            continue
        try:
            got = CHECKS[x](r)
            if x == "experiments":
                ok_exps = got
        except Exception as e:  # noqa: BLE001
            r.bad(x, f"这一组的检查自己出错了：{type(e).__name__}: {e}\n{traceback.format_exc(limit=3)}")
        r.counts.setdefault(x, 0)
    return r


def main(argv):
    ap = argparse.ArgumentParser(description="核对机器表、policy、插件、实验表、先后、任务、步骤、原始数据、代码包（只读）")
    ap.add_argument("--quick", action="store_true", help="不跑 inputs、raw、bundle")
    ap.add_argument("--only", nargs="+", choices=GROUPS, metavar="组", help=" ".join(GROUPS))
    a = ap.parse_args(argv)
    r = run(a.only, a.quick)
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
