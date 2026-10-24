#!/usr/bin/env python3
"""任务说明（2026-10-18）：做法（scripts/queue/recipes.py）+ 参数 → 完整的说明（key、环境、命令、结果文件夹、怎么算训完……）。
调度的各个脚本都经这里解析和判断，不自己拼路径。只用标准库：make_registry.py 把本文件经 ssh 送到别的机器上跑 collect()。

命令行：
  spec.py resolve <做法> k=v…        完整的说明（JSON；{data}、{repo} 不展开）
  spec.py shell <做法> k=v…          给 run_task.sh 的 shell 语句（TASK_KEY、TASK_ENV、TASK_CMD、TASK_OUT 和 vars，按本机展开）
  spec.py done <做法> k=v…           训完了退出码 0，没训完 1
  spec.py lookup <key>               按 key 找任务（先查实验表 research/*/experiments.py，再查训练队列），打印说明（JSON）
  spec.py field <key> <字段>         说明里的一个字段（列表每行一个）
k=v 的值以 [ 或 { 开头时按 JSON 读（如 done='["a","b|c"]'），其他都是字符串。
"""
import fnmatch
import glob
import json
import os
import re
import shlex
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
BUILTIN = ("data", "repo")  # 运行时按机器展开的名字
FRESH_LOG = 15 * 60
SAFE = "__"


def safe(key):
    """key → 文件名（领取记录、分段日志）。"""
    return key.replace("/", SAFE)


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
    s.update(recipe=recipe, params={k: v for k, v in params.items()}, exp=exp, names=p)
    return s


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


def is_done(s, root):
    """训完了：done 的每一项在 out 下都有（一项里 | 分开的有一个就行）。
    文件不存在 → 没训完；其他读不了的情况（盘断开等）抛 OSError，不当成没训完。"""
    fd = out_dir(s, root)
    os.stat(root)
    return all(any(_exists(os.path.join(fd, alt)) for alt in item.split("|")) for item in s["done"])


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


TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def nnunet_log(path):
    """nnU-Net 的 training_log_*.txt（一个文件是一次启动）→ 开始、最后更新、训完的 epoch 数；
    tl：每训完 10 个 epoch 记一个 [时间, 已训完 epoch 数]（最后一个 epoch 也记），用来画总进度随时间的曲线。"""
    start = end = None
    done = 0
    cur = None
    tl, last = [], None
    with open(path, errors="replace") as f:
        for line in f:
            m = TS.match(line)
            if m:
                start = start or m.group(1)
                end = m.group(1)
            e = re.search(r": Epoch (\d+)\s*$", line)
            if e:
                cur = int(e.group(1))
            elif cur is not None and ": Epoch time:" in line:
                done = max(done, cur + 1)
                if m and (cur + 1) % 10 == 0:
                    tl.append([m.group(1), cur + 1])
                last = [m.group(1), cur + 1] if m else None
    if done and (not tl or tl[-1][1] != done) and last:
        tl.append(last)
    return dict(start=start, end=end, epochs_done=done, tl=tl)


def plain_log(path):
    """没有进度解析方法时：只记日志的修改时间。"""
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(path)))
    return dict(start=None, end=t, epochs_done=0, tl=[])


def _num(v):
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return v


def nnunet_debug(fd):
    """nnU-Net 训练器写的 debug.json（全部设置，记最后一次启动的）→ 登记表的参数列。"""
    try:
        d = {k: _num(v) for k, v in json.load(open(os.path.join(fd, "debug.json"))).items()}
    except Exception:  # noqa: BLE001
        return {}
    cm = str(d.get("configuration_manager", ""))
    patch = re.search(r"'patch_size': \[([^\]]*)\]", cm) or re.search(r"patch_size[^\[]*\[([^\]]*)\]", cm)
    loss = re.sub(r"\s+", " ", str(d.get("loss", "")))[:120]
    return {"initial_lr": d.get("initial_lr"), "num_epochs": d.get("num_epochs"), "batch_size": d.get("batch_size"),
            "patch_size": patch.group(1).replace(" ", "") if patch else "", "iters_per_epoch": d.get("num_iterations_per_epoch"),
            "oversample_fg": d.get("oversample_foreground_percent"), "weight_decay": d.get("weight_decay"), "loss": loss,
            "DA进程": d.get("dataloader_train.num_processes"), "机器": d.get("hostname"), "GPU": d.get("gpu_name"),
            "torch": d.get("torch_version")}


PROGRESS = {"nnunet_log": nnunet_log, None: plain_log}
INFO = {"nnunet_debug": nnunet_debug, None: lambda fd: {}}


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


def collect(specs, root):
    """这台机器上每个任务的情况（make_registry.py 用）：{"folds": {key: …}, "seglogs": […]}。
    specs：[说明]（至少有 key、out、done、log、progress、info、metrics）；root：本机的数据目录。"""
    out = {"folds": {}, "seglogs": []}
    for s in specs:
        fd = out_dir(s, root)
        if not os.path.isdir(fd):
            continue
        logs = sorted(glob.glob(os.path.join(fd, s["log"]))) if s.get("log") else []
        parse = PROGRESS.get(s.get("progress"), plain_log)
        runs = []
        for p in logs:
            try:
                runs.append(parse(p))
            except OSError:
                pass
        out["folds"][s["key"]] = dict(runs=runs, final=is_done(s, root), archived=os.path.exists(os.path.join(fd, "ARCHIVED")),
                                      dice=metrics(s, fd), debug=INFO.get(s.get("info"), INFO[None])(fd))
    pats = [os.path.join(root, "nnUNet", "results", "logs", "*_seg*.log"), os.path.join(root, "logs", "tasks", "*_seg*.log")]
    for p in (q for pat in pats for q in glob.glob(pat)):
        first, trained = "", False
        try:
            with open(p, errors="replace") as f:
                first = f.readline().strip()
                trained = any(": Epoch time:" in line or line.startswith("训完") for line in f)  # 至少训完 1 个 epoch
        except Exception:  # noqa: BLE001
            pass
        out["seglogs"].append(dict(name=os.path.basename(p), first=first, trained=trained))
    return out


def remote_collect(ssh, specs, root, timeout=600):
    """ssh 到别的机器上跑 collect()：把本文件和调用一起经 stdin 送过去（那边的仓库可能还是旧版本）。"""
    prog = open(os.path.abspath(__file__), encoding="utf-8").read().replace('\nif __name__ == "__main__":', "\nif False:")
    prog += f"\nprint(json.dumps(collect(json.loads({json.dumps(json.dumps(specs))}), os.path.expanduser({root!r}))))\n"
    try:
        r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", ssh, "python3 -"], input=prog,
                           capture_output=True, text=True, timeout=timeout)
        i = r.stdout.find("{")
        return json.loads(r.stdout[i:]) if i >= 0 else None
    except Exception as e:  # noqa: BLE001
        print(f"收集 {ssh} 失败：{e}", file=sys.stderr)
        return None


def drop_list(s, files):
    """归档时要删的文件：out 下的相对路径列表 files 里，匹配 archive.drop、不匹配 archive.keep 的。"""
    a = s.get("archive") or {}
    return sorted(f for f in files if any(fnmatch.fnmatch(f, p) for p in a.get("drop", []))
                  and not any(fnmatch.fnmatch(f, p) for p in a.get("keep", [])))


# ---------------- 查任务 ----------------
def lookup(key):
    """key → 说明：先查实验表，再查训练队列（taskq.py show，box 上会转到 DICC）。找不到 → None。"""
    import experiments as E
    t = E.tasks().get(key)
    if t:
        return t["spec"]
    r = subprocess.run([sys.executable, os.path.join(HERE, "taskq.py"), "show", "--json", key], capture_output=True, text=True)
    if r.returncode == 0:
        i = r.stdout.find("{")
        return json.loads(r.stdout[i:]) if i >= 0 else None
    return None


def shell_lines(s, data, repo=REPO):
    """run_task.sh 用：把说明变成 shell 变量（按本机的数据目录、仓库展开）。"""
    e = expand(s, data, repo)
    lines = [f"TASK_KEY={shlex.quote(e['key'])}", f"TASK_ENV={shlex.quote(e['env'])}", f"TASK_CMD={shlex.quote(e['cmd'])}",
             f"TASK_OUT={shlex.quote(os.path.join(data, e['out']))}", f"TASK_SAFE={shlex.quote(safe(e['key']))}"]
    lines += [f"export {k}={shlex.quote(str(v))}" for k, v in e["vars"].items()]
    return "\n".join(lines)


def main(argv):
    sys.path.insert(0, HERE)
    sys.path.insert(0, os.path.join(REPO, "envs"))
    if not argv:
        print(__doc__.split("命令行：")[1], file=sys.stderr)
        return 2
    cmd, args = argv[0], argv[1:]
    try:
        if cmd in ("resolve", "shell", "done"):
            s = resolve(args[0], parse_kv(args[1:]))
        elif cmd in ("lookup", "field"):
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
    data = os.environ.get("BRAIN_TUMOR")
    if not data:
        import machines as M
        me = M.whoami()
        data = M.expand(M.MACHINES[me]["data"]) if me else ""
    if cmd in ("resolve", "lookup"):
        print(json.dumps(s, ensure_ascii=False, indent=1))
    elif cmd == "field":
        v = s.get(args[1]) if args[1] != "out_abs" else os.path.join(data, s["out"])
        print("\n".join(map(str, v)) if isinstance(v, list) else json.dumps(v, ensure_ascii=False) if isinstance(v, dict) else v if v is not None else "")
    elif cmd == "shell":
        print(shell_lines(s, data))
    elif cmd == "done":
        return 0 if is_done(s, data) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
