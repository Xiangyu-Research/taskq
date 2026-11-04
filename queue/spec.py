#!/usr/bin/env python3
"""任务说明（2026-10-18）：做法（scripts/taskq/queue/recipes.py）+ 参数 → 完整的说明（key、环境、命令、结果文件夹、怎么算训完……）。
调度的各个脚本都经这里解析和判断，不自己拼路径。只用标准库：健康检查（scripts/taskq/monitor/health.py 的 gather）把本文件经 ssh 送到别的机器上跑 collect()。

命令行：
  spec.py shell <做法> k=v…          给 run_task.sh 的 shell 语句（TASK_KEY、TASK_ENV、TASK_CMD、TASK_OUT 和 vars，按本机展开；
                                     再加本机 envs/machines.py 的 task_vars 里还没设的）
  spec.py done <做法> k=v…           训完了退出码 0，没训完 1
  spec.py lookup <key>               按 key 找任务（先查实验表（experiments.py），再查训练队列），打印说明（JSON）
  spec.py outputs <key> [--sh]       这个任务的输出（outputs()，JSON）；--sh：给 archive_task.sh 的 shell 语句
k=v 的值以 [ 或 { 开头时按 JSON 读（如 done='["a","b|c"]'），其他都是字符串。
"""
import glob
import json
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(CORE, "queue"))
try:
    import cfg as _cfg  # noqa: E402
    REPO = _cfg.REPO  # 使用者的仓库根（cfg.py）
except ImportError:  # 本文件经 ssh 送到别的机器上单独跑时（remote_collect）
    REPO = CORE
BUILTIN = ("data", "repo")  # 运行时按机器展开的名字
FRESH_LOG = 15 * 60
SAFE = "__"


def safe(key):
    """key → 文件名（领取记录、分段日志）。"""
    return key.replace("/", SAFE)


JSON_MARK = "@@JSON "  # 给别的机器经 ssh 读的 JSON 只写在以它开头的一行里（scripts/taskq/queue/README.md 的"约定"）


COLLECT_WAIT = 90  # remote_collect 一共等几秒（各任务同时读）


def json_out(obj):
    print(JSON_MARK + json.dumps(obj), flush=True)


def json_in(out):
    """输出里 @@JSON 那一行 → 对象；没有这一行或解析不了 → None。"""
    line = next((ln[len(JSON_MARK):] for ln in out.splitlines() if ln.startswith(JSON_MARK)), None)
    try:
        return json.loads(line) if line is not None else None
    except ValueError:
        return None


# ---------------- 解析 ----------------
class SpecError(ValueError):
    pass


_REF = re.compile(r"(?<!\$)\{(\w+)\}")


def fill(v, names, keep=BUILTIN):
    """把 {名字} 换成值（递归处理列表、字典）。${X} 是 shell 的，不换；keep 里的名字原样留着。
    整个值就是一个 {名字}、而那个值不是字符串时（如 done 是列表），直接换成那个值。"""
    if isinstance(v, str):
        m = _REF.fullmatch(v)
        if m and m.group(1) in names and not isinstance(names[m.group(1)], str):
            return names[m.group(1)]

        def sub(m):
            k = m.group(1)
            if k in names:
                return str(names[k])
            if k in keep:
                return m.group(0)
            raise SpecError(f"不认识的 {{{k}}}（有：{', '.join(sorted(names))}）")
        return _REF.sub(sub, v)
    if isinstance(v, list):
        return [fill(x, names, keep) for x in v]
    if isinstance(v, dict):
        return {k: fill(x, names, keep) for k, x in v.items()}
    return v


def resolve(recipe, params, exp=None):
    """做法 + 参数 → 说明（dict）。exp：实验表里的实验 id（只记下来）。"""
    import recipes as R  # 只在解析时用（collect() 在别的机器上跑时没有这个文件）
    if recipe not in R.RECIPES:
        raise SpecError(f"不认识的做法 {recipe}（有：{', '.join(R.RECIPES)}）")
    r = R.RECIPES[recipe]
    if "inputs" in params:  # inputs 是 data 的另一个名字（2026-10-31）
        if "data" in params:
            raise SpecError("inputs 和 data 是同一个字段，只给一个")
        params = {("data" if k == "inputs" else k): v for k, v in params.items()}
    fields = [k for k in r if k not in ("params", "open")]
    over = {k: v for k, v in params.items() if k in fields and k not in r["params"]}
    p = {k: v for k, v in r["params"].items()}
    extra = [k for k in params if k not in p and k not in over]
    if extra and not r.get("open"):
        raise SpecError(f"做法 {recipe} 没有参数 {', '.join(extra)}（有：{', '.join(list(p) + fields)}）")
    p.update({k: v for k, v in params.items() if k not in over})
    missing = [k for k, v in p.items() if v is R.REQUIRED]
    if missing:
        raise SpecError(f"做法 {recipe} 缺参数 {', '.join(missing)}")
    p = {k: (str(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v) for k, v in p.items()}
    for _ in range(5):  # 默认值里引用别的参数（如 model）
        q = {k: fill(v, p) for k, v in p.items()}
        if q == p:
            break
        p = q
    s = {}
    for k in fields:
        v = r[k]
        if k in over:
            v = {**v, **over[k]} if isinstance(v, dict) and isinstance(over[k], dict) else over[k]
        s[k] = fill(v, p)
    if isinstance(s["done"], str):
        s["done"] = [s["done"]]
    _check(s)
    s.update(recipe=recipe, params={k: v for k, v in params.items()}, exp=exp, names=p)
    return s


RES = ("gpus", "cpus", "mem", "gpu_mem", "wait_gpu", "local_inputs")


def _check(s):
    """任务单的通用字段：data（= inputs）、resume、res、progress 统一格式并检查（2026-10-31）。"""
    for k in ("data", "resume"):
        v = s.get(k)
        if isinstance(v, str):
            v = [v]
        if v is not None and (not isinstance(v, list) or not all(isinstance(x, str) and x for x in v)):
            raise SpecError(f"{k} 要是路径的列表：{v!r}")
        if k == "data" and any(".." in x.split("/") for x in v or []):
            raise SpecError(f"data 里不能有 ..：{v!r}")
        s[k] = v if k == "resume" else (v or [])
    s["inputs"] = s["data"]
    res = dict(s.get("res") or {})
    if set(res) - set(RES):
        raise SpecError(f"res 只能有 {', '.join(RES)}：{res!r}")
    for k, v in res.items():
        if v is None:
            continue
        if k == "mem":
            if not re.fullmatch(r"\d+[KMGT]?", str(v)):
                raise SpecError(f"res.mem 要写成 32G 这样：{v!r}")
            res[k] = str(v)
            continue
        try:
            res[k] = float(v) if k == "gpu_mem" else int(v)
        except (TypeError, ValueError):
            raise SpecError(f"res.{k} 要是数：{v!r}") from None
        if res[k] < 0:
            raise SpecError(f"res.{k} 不能是负数：{v!r}")
    if (res.get("gpus") or 0) > 1:
        raise SpecError(f"res.gpus 最多 1（worker 每个任务只给一张卡，11/1）：{res['gpus']!r}")
    s["res"] = res
    if s.get("progress") not in PROGRESS:
        raise SpecError(f"不认识的进度解析方法 {s.get('progress')}（有：{', '.join(k for k in PROGRESS if k)}）")


def envs_of(s):
    """说明的 env → 环境名的列表（env 可以是一个名字或几个名字的列表；没写 → []）。"""
    v = (s or {}).get("env")
    return [v] if isinstance(v, str) and v else [x for x in v if x] if isinstance(v, list) else []


def mem_mb(v):
    """"32G" 这样的 → MB；None → None。"""
    if v is None:
        return None
    m = re.fullmatch(r"(\d+)([KMGT]?)", str(v))
    return int(m.group(1)) * {"K": 1 / 1024, "": 1, "M": 1, "G": 1024, "T": 1024 ** 2}[m.group(2)]


def fits(res, cpus=None, mem_mb_=None, gpu_mem=None):
    """任务的 res 在 worker 给得起的范围里（worker 没说的那项不查，11/1）→ (能不能, 不够的说明)。"""
    res = res or {}
    need = [("cpus", res.get("cpus"), cpus, "个 CPU"), ("mem", mem_mb(res.get("mem")), mem_mb_, " MB 内存"),
            ("gpu_mem", res.get("gpu_mem"), gpu_mem, " GB 显存")]
    short = [f"{k} 要 {n}{u}、只有 {h}" for k, n, h, u in need if n is not None and h is not None and n > h]
    return not short, "；".join(short)


def parse_kv(args):
    out = {}
    for a in args:
        if "=" not in a:
            raise SpecError(f"参数要写成 k=v：{a}")
        k, v = a.split("=", 1)
        out[k] = json.loads(v) if v[:1] and v[:1] in "[{" else v
    return out


def kv_args(recipe, params):
    """做法 + 参数 → 命令行参数（run_task.sh <做法> k=v…）。"""
    return [recipe] + [f"{k}={v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)}" for k, v in params.items()]


def expand(v, data, repo=REPO):
    """运行时把 {data}、{repo} 换成本机的路径。"""
    return fill(v, dict(data=data, repo=repo), keep=())


# ---------------- 看文件（只用标准库；collect 会被送到别的机器上跑） ----------------
def out_dir(s, root):
    return os.path.join(root, s["out"])


def outputs(s):
    """任务的输出，out、done、fetch、archive、分段日志只在这里解释（拉回、放好、判断做完、归档都用它）：
      outs     要拉回、备份的文件夹（相对数据目录）：out，加上 fetch.also
      done     做完的标志（相对 out）：每一项都要有，一项是备选的列表（done 里 | 分开的），有一个就行
      exclude  不拉回的文件（文件名）
      extra    out 的上一级里一起拉回、备份的文件（通配，如 *.json）
      archive  拉回 box 后要不要备份到备份机（archive_task.sh）
      logs     分段日志的文件名前缀（相对数据目录）；oldlogs：以前放在别处的分段日志（dir、prefix），没有时 None"""
    f = s.get("fetch") or {}
    done = s.get("done") or []
    if isinstance(done, str):
        done = [done]
    old = s.get("oldlogs") or {}
    return dict(outs=[os.path.normpath(o.strip("/")) for o in [s["out"]] + list(f.get("also") or [])],
                done=[d.split("|") for d in done], exclude=list(f.get("exclude") or []), extra=list(f.get("extra") or []),
                archive=bool(s.get("archive")), logs=f"logs/tasks/{safe(s['key'])}{SAFE}" if s.get("key") else None,
                oldlogs=old if old.get("dir") and old.get("prefix") else None)


def done_missing(o, exists):
    """outputs 的 done 里还缺的项（"a|b" 的写法）；exists(相对 out 的路径) 由调用方给（本机的文件、拉回的文件……）。"""
    return ["|".join(item) for item in o["done"] if not any(exists(a) for a in item)]


def done_with(o, exists, empty=True):
    """做完了：done 的每一项都有。done 是空的时候 → empty（判断训完时算做完，收编别处的存档时不算）。"""
    return empty if not o["done"] else not done_missing(o, exists)


def is_done(s, root):
    """训完了：done 的每一项在 out 下都有（一项里 | 分开的有一个就行）。
    文件不存在 → 没训完；其他读不了的情况（盘断开等）抛 OSError，不当成没训完。"""
    fd = out_dir(s, root)
    os.stat(root)
    return done_with(outputs(s), lambda a: _exists(os.path.join(fd, a)))


def _exists(p):
    try:
        os.stat(p)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False


def log_age(s, root):
    """训练日志多久没更新（秒）；没有日志 → None。"""
    logs = glob.glob(os.path.join(out_dir(s, root), s["log"])) if s.get("log") else []
    return time.time() - max(os.path.getmtime(p) for p in logs) if logs else None


def eta_at(runs, nep, hours=3):
    """训练中的任务：最近一次启动的训练日志里，最近 hours 小时的速度 → 训到 nep 个 epoch 的时刻；算不出来（不到两个点、
    速度不是正的、已经训到）→ None。"""
    tl = max(runs, key=lambda r: r["end"] or "").get("tl") or [] if runs else []
    pts = [(datetime.strptime(t, "%Y-%m-%d %H:%M:%S"), e) for t, e in tl]
    if len(pts) < 2 or not nep:
        return None
    recent = [q for q in pts if q[0] >= pts[-1][0] - timedelta(hours=hours)]
    a, b = (recent if len(recent) >= 2 else pts[-2:])[0], pts[-1]
    rate = (b[1] - a[1]) / max((b[0] - a[0]).total_seconds(), 1)  # epoch / 秒
    if rate <= 0 or b[1] >= nep:
        return None
    return b[0] + timedelta(seconds=(nep - b[1]) / rate)


def progress_json(fd):
    """任务自己写的 out/PROGRESS.json（{"done": n, "total": m}）→ 一段进度；没有或读不了 → None。"""
    p = os.path.join(fd, "PROGRESS.json")
    try:
        d = json.load(open(p))
        done = int(d.get("done") or 0)
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(p)))
    except (OSError, ValueError, TypeError, AttributeError):
        return None
    total = d.get("total")
    return dict(start=None, end=t, epochs_done=done, total=int(total) if isinstance(total, (int, float)) else None, tl=[[t, done]])


def resumable(s, fd):
    """out 下有任务说明 resume 里的存档（任何一个）。"""
    return any(glob.glob(os.path.join(fd, g)) for g in s.get("resume") or [])


def plain_log(path):
    """没有进度解析方法时：只记日志的修改时间。"""
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(path)))
    return dict(start=None, end=t, epochs_done=0, tl=[])


# 进度、训练设置的读法：通用的在这里，某种模型专用的放在 TASKQ_PLUGINS 各文件夹的 readers/<名>.py（PROGRESS、INFO、TRAINED 三张表，只用标准库），
# 加载时合进来（2026-10-31）。collect() 送到别的机器上跑时 remote_collect 把 readers/ 的内容一起送过去。
PROGRESS = {"progress_json": plain_log, None: plain_log}  # progress_json：日志只看时间，进度另读
INFO = {None: lambda fd: {}}
TRAINED = []  # 分段日志里出现这些字样之一 = 至少做完了一轮（各 reader 的 TRAINED；另外 run_task.sh 写的"训完"开头的行也算）


def reader_sources():
    """readers/*.py（这里的和 TASKQ_PLUGINS 各文件夹里的）→ {名: 源码}（没有这些文件夹时为空）。"""
    if HERE not in sys.path:
        sys.path.append(HERE)
    import cfg
    out, where = {}, {}
    for d in [os.path.join(x, "readers") for x in [HERE] + cfg.plugin_dirs()]:
        for f in sorted(os.listdir(d) if os.path.isdir(d) else []):
            if not f.endswith(".py") or f.startswith("test_"):
                continue
            p = os.path.join(d, f)
            if f[:-3] in where and os.path.realpath(where[f[:-3]]) != os.path.realpath(p):
                raise SpecError(f"{p} 和 {where[f[:-3]]} 同名，后加载的会顶掉前一个")
            where[f[:-3]] = p
            out[f[:-3]] = open(p, encoding="utf-8").read()
    return out


def load_readers(sources):
    """把各 reader 的 PROGRESS、INFO、TRAINED 合进来（同名的报错，不覆盖）。"""
    import types
    for name, src in sources.items():
        m = types.ModuleType(f"spec_reader_{name}")
        exec(compile(src, f"readers/{name}.py", "exec"), m.__dict__)
        for table, own in ((PROGRESS, getattr(m, "PROGRESS", {})), (INFO, getattr(m, "INFO", {}))):
            dup = sorted(set(own) & set(table))
            if dup:
                raise SpecError(f"readers/{name}.py 的 {', '.join(map(str, dup))} 和已有的重名")
            table.update(own)
        TRAINED.extend(x for x in getattr(m, "TRAINED", []) if x not in TRAINED)


if os.path.basename(__file__) == "spec.py":  # 经 ssh 送过去跑时（__file__ 不是 spec.py），由 remote_collect 另外调用
    load_readers(reader_sources())


def metrics(s, fd):
    out = {}
    for col, (fn, path) in (s.get("metrics") or {}).items():
        try:
            v = json.load(open(os.path.join(fd, fn)))
            for k in path:
                v = v[k]
            out[col] = v
        except Exception:  # noqa: BLE001
            pass
    return out


COLLECT_FIELDS = ("key", "out", "done", "log", "progress", "info", "metrics", "resume", "oldlogs")


def all_tasks(queue=()):
    """要登记的任务：实验表里的（按表的顺序）+ 任务队列里有、实验表里没有的。
    → [dict(key, spec, e)]，e 是所属的实验（临时任务为 None）。queue：taskq 状态里的任务列表。"""
    import experiments as E
    out = [dict(key=k, spec=t["spec"], e=E.experiment(t["exp"])) for k, t in E.tasks().items()]
    have = {t["key"] for t in out}
    for t in queue:
        if t["key"] not in have and t.get("spec"):
            out.append(dict(key=t["key"], spec=t["spec"], e=None))
            have.add(t["key"])
    return out


def task_specs(queue=()):
    """各机器上 collect() 用的说明（只留它要的字段，经 ssh 传过去的量小）。"""
    return [{k: t["spec"].get(k) for k in COLLECT_FIELDS} for t in all_tasks(queue)]


def collect(specs, root, wait=None):
    """这台机器上每个任务的情况（健康检查、登记表用）：{"tasks": {key: …}, "seglogs": […]}，有读不动的再加 "stuck": [key…]。
    specs：[说明]（至少有 key、out、done、log、progress、info、metrics）；root：本机的数据目录。
    分段日志在 logs/tasks/，加上说明里 oldlogs 的 dir（以前放在别处的）。
    wait：给了就每个任务一个线程同时读，wait 秒后还没读完的（文件卡在存储里）记进 stuck、不再等（11/1），
    卡住的线程留着，进程结束时一起结束（remote_collect 读完后 os._exit）。"""
    jobs = [(s["key"], lambda s=s: one_task(s, root)) for s in specs] + [("@seglogs", lambda: seg_logs(specs, root))]
    if wait is None:
        res = {k: fn() for k, fn in jobs}
    else:
        import threading
        res, ths = {}, []
        for k, fn in jobs:
            th = threading.Thread(target=lambda k=k, fn=fn: res.__setitem__(k, fn()), daemon=True)
            th.start()
            ths.append(th)
        end = time.time() + wait
        for th in ths:
            th.join(max(0, end - time.time()))
        res = dict(res)
    out = {"tasks": {s["key"]: res[s["key"]] for s in specs if res.get(s["key"]) is not None}, "seglogs": res.get("@seglogs") or []}
    stuck = [k for k, _ in jobs if k not in res]
    if stuck:
        out["stuck"] = stuck
    return out


def one_task(s, root):
    """collect 里一个任务的情况；还没有输出目录 → None。"""
    fd = out_dir(s, root)
    if not os.path.isdir(fd):
        return None
    logs = sorted(glob.glob(os.path.join(fd, s["log"]))) if s.get("log") else []
    parse = PROGRESS.get(s.get("progress"), plain_log)
    runs = []
    for p in logs:
        try:
            runs.append(parse(p))
        except OSError:
            pass
    if s.get("progress") == "progress_json":
        pj = progress_json(fd)
        if pj:
            runs.append(pj)
    return dict(runs=runs, final=is_done(s, root), archived=os.path.exists(os.path.join(fd, "ARCHIVED")),
                dice=metrics(s, fd), debug=INFO.get(s.get("info"), INFO[None])(fd), resumable=resumable(s, fd))


def seg_logs(specs, root):
    """collect 里的分段日志：[{name, first, trained}]。"""
    out = {"seglogs": []}
    dirs = sorted({s["oldlogs"]["dir"] for s in specs if (s.get("oldlogs") or {}).get("dir")})
    pats = [os.path.join(root, d, "*_seg*.log") for d in dirs] + [os.path.join(root, "logs", "tasks", f"*_{w}*.log") for w in ("seg", "part")]
    for p in (q for pat in pats for q in glob.glob(pat)):
        first, trained = "", False
        try:
            with open(p, errors="replace") as f:
                first = f.readline().strip()
                trained = any(line.startswith("训完") or any(x in line for x in TRAINED) for line in f)  # 至少做完 1 轮
        except Exception:  # noqa: BLE001
            pass
        out["seglogs"].append(dict(name=os.path.basename(p), first=first, trained=trained))
    return out["seglogs"]


def remote_collect(ssh, specs, root, timeout=600):
    """ssh 到别的机器上跑 collect()：把本文件、readers/ 和调用一起经 stdin 送过去（那边的仓库可能还是旧版本）。"""
    import common as C
    prog = open(os.path.abspath(__file__), encoding="utf-8").read().replace('\nif __name__ == "__main__":', "\nif False:")
    prog += f"\nload_readers(json.loads({json.dumps(json.dumps(reader_sources()))}))"
    prog += (f"\njson_out(collect(json.loads({json.dumps(json.dumps(specs))}), os.path.expanduser({root!r}), "
             f"wait={COLLECT_WAIT}))\nos._exit(0)\n")  # 卡住的线程不等
    try:
        r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", ssh, C.remote_timeout("python3 -", timeout)], input=prog,
                           capture_output=True, text=True, timeout=timeout)
        out = json_in(r.stdout)
        if out is None:
            print(f"收集 {ssh} 失败（退出码 {r.returncode}）：{r.stderr.strip()[-300:]}", file=sys.stderr)
        return out
    except Exception as e:  # noqa: BLE001
        print(f"收集 {ssh} 失败：{e}", file=sys.stderr)
        return None


# ---------------- 查任务 ----------------
def lookup(key):
    """key → 说明：先查实验表，再查训练队列（taskq.py show，box 上会转到 DICC）。找不到 → None。"""
    import experiments as E
    t = E.tasks().get(key)
    if t:
        return t["spec"]
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "show", "--json", key], capture_output=True, text=True)
    return json_in(r.stdout) if r.returncode == 0 else None


def shell_lines(s, data, repo=REPO):
    """run_task.sh 用：把说明变成 shell 变量（按本机的数据目录、仓库展开）。"""
    e = expand(s, data, repo)
    lines = [f"TASK_KEY={shlex.quote(e['key'])}", f"TASK_ENV={shlex.quote((envs_of(e) or [''])[0])}", f"TASK_CMD={shlex.quote(e['cmd'])}",
             f"TASK_OUT={shlex.quote(os.path.join(data, e['out']))}", f"TASK_SAFE={shlex.quote(safe(e['key']))}",
             f"TASK_WAIT_GPU={'' if e['res'].get('wait_gpu') is None else int(e['res']['wait_gpu'])}",
             "TASK_INPUTS=(" + " ".join(shlex.quote(os.path.join(data, x)) for x in e["data"]) + ")",
             f"TASK_LOCAL={int(bool(e['res'].get('local_inputs')))}"]
    lines += [f"export {k}={shlex.quote(str(v))}" for k, v in e["vars"].items()]
    return "\n".join(lines)


def machine_vars():
    """本机 envs/machines.py 的 task_vars → shell 语句：已经设了（非空）的不改。"""
    import machines as M
    me = M.whoami()
    tv = ((M.MACHINES.get(me) or {}).get("task_vars") or {}) if me else {}
    return [f"[[ -n ${{{k}:-}} ]] || export {k}={shlex.quote(str(v))}" for k, v in tv.items()]


def main(argv):
    sys.path.insert(0, HERE)
    import cfg
    cfg.use_config()
    if not argv:
        print(__doc__.split("命令行：")[1], file=sys.stderr)
        return 2
    cmd, args = argv[0], argv[1:]
    try:
        if cmd in ("shell", "done"):
            s = resolve(args[0], parse_kv(args[1:]))
        elif cmd in ("lookup", "outputs"):
            s = lookup(args[0])
            if s is None:
                print(f"实验表和训练队列里都没有 {args[0]}", file=sys.stderr)
                return 1
        else:
            print(f"spec.py：不认识的命令 {cmd}", file=sys.stderr)
            return 2
    except SpecError as e:
        print(f"spec.py：{e}", file=sys.stderr)
        return 2
    data = cfg.env("TASKQ_DATA")
    if not data:
        import machines as M
        me = M.whoami()
        data = M.expand(M.MACHINES[me].get("data") or "") if me else ""
    if cmd == "lookup":
        print(json.dumps(s, ensure_ascii=False, indent=1))
    elif cmd == "outputs":
        o = outputs(s)
        if "--sh" not in args[1:]:
            print(json.dumps(o, ensure_ascii=False, indent=1))
            return 0
        test = " && ".join("{ " + " || ".join("test -e " + shlex.quote(a) for a in item) + "; }" for item in o["done"])
        old = o["oldlogs"] or {}
        print("\n".join([f"OUTS=({' '.join(shlex.quote(x) for x in o['outs'])})", f"EXTRA=({' '.join(shlex.quote(x) for x in o['extra'])})",
                         f"TEST={shlex.quote(test or 'true')}", f"ARCHIVE={int(o['archive'])}", f"LOGS={shlex.quote(o['logs'] or '')}",
                         f"OLDDIR={shlex.quote(old.get('dir', ''))}", f"OLDSEG={shlex.quote(old.get('prefix', ''))}"]))
    elif cmd == "shell":
        print("\n".join([shell_lines(s, data)] + machine_vars()))
    elif cmd == "done":
        return 0 if is_done(s, data) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
