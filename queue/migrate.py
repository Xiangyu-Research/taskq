"""taskq self migrate：把盘上的旧格式改成现在的。改格式的版本切换时跑一次（先停 cron 的健康检查、各执行机交还在训的任务、
推好新代码、各台换上新包），重复跑没关系。默认只列出要做的事，--apply 才做。
  taskq self migrate [--apply] [--rename 旧名=新名 …] [--merge 旧名=旧根目录 …]
    --rename  旧的机器名（记账、领取、冒烟记录里的）→ 机器表里的名字；几个旧名可以并成一台
    --merge   这个旧名的执行机根目录并进它新名那一台的根目录（同一台物理机器上原来分开的根目录）：分段日志搬过去，
              blob 同一个盘的硬链接过去；旧根目录的队列里要已经没有任务，旧根目录本身不删（最后列出删它的命令）
hub 上：总队列（按步骤实例的新任务名改名、参数换成现在算出来的、补 added、交还记录补原因、机器改名）、relay 的记账
（机器改名、补视图链接）、冒烟记录、状态目录里不再用的文件、配置里的旧东西。
各执行机和 store 上：经 relay_remote.op 跑那台代码包里的 `migrate.py root`（队列按新任务名改名、说明换成 hub 按现在的做法
解析的（added 不动）；分段日志按新任务名改名，旧的分段日志名 _seg<段> 改成 _part<段>；并根目录；读视图里的链接）。"""
import argparse
import glob
import json
import os
import re
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import proto  # noqa: E402
import common as C  # noqa: E402

safe = proto.safe
OLD_CODE = "before-migrate"  # bundle field of failure records from before the cutover (not a bundle in use, so it no longer counts after the switch, taskq.fail_lines)
PER_KEY = ("claims", "failures", "skips", "inputs", "released", "stopped")  # per-task records in a queue (proto.QUEUE)
OLD_PART = "_seg"  # old part log name <safe key>__<job>_seg<part>.log

# old on-disk text (only this rewrite step reads it; no reader keeps accepting it)
OLD_TS = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
OLD_HEAD = re.compile(r"^作业 (\S+) @ ([^，]+)，任务=([^，]*)，第 (\d+) 段，环境 ([^，]*)，代码 ([^，]*)，" + OLD_TS + r"$")
OLD_ENDED = re.compile(r"^" + OLD_TS + r" 命令结束（退出码 (-?\d+)），(.*)$")
OLD_NOT_STARTED = re.compile(r"^" + OLD_TS + r" 没开训：(.*)$")
OLD_GPU_GIVEUP_SEEN = re.compile(r"^" + OLD_TS + r" 卡 (\S+) 被别人占着（空着，别人 (.+?) 还在用，要空满 (\d+) 分钟），等了 (\d+) 分钟还没空出来，不跑$")
OLD_GPU_GIVEUP = re.compile(r"^" + OLD_TS + r" 卡 (\S+) 被别人占着，等了 (\d+) 分钟还没空出来，不跑$")
OLD_GPU_WAIT_SEEN = re.compile(r"^" + OLD_TS + r" 卡 (\S+) 空着，别人 (.+?) 还在用，空满 (\d+) 分钟再跑（已等 (\d+) 分钟）$")
OLD_GPU_WAIT = re.compile(r"^" + OLD_TS + r" 卡 (\S+) 被别人占着，等它空出来（已等 (\d+) 分钟(?:，最多 (\d+) 分钟)?）$")
OLD_STALL_MSG = re.compile(r"^( \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})：日志和结果文件夹 (\d+) 分钟没有变化，作业 (\S+) 停掉它$")
OLD_FAIL_JOB = "作业 "


class Stop(Exception):
    """A situation that cannot be handled automatically: change nothing, list why."""


# ---------------- 在一台机器上（代码包里，经 relay_remote.op 跑） ----------------
def log_rename(name, keys):
    """分段日志的文件名 → 新名字（不用改 → None）。<safe key>__<作业>_part<段>.log：作业名里没有 "__"。"""
    new = name
    if OLD_PART in new:
        i = new.rfind(OLD_PART)
        rest = new[i + len(OLD_PART):]
        if rest[:-4].isdigit() and rest.endswith(".log"):
            new = new[:i] + "_" + proto.LOG_PART + rest
    for old, nk in keys.items():
        pre = safe(old) + proto.SAFE
        if new.startswith(pre) and proto.SAFE not in new[len(pre):]:
            new = safe(nk) + proto.SAFE + new[len(pre):]
            break
    return None if new == name else new


def move(src, dst, say, dry):
    """src → dst（dst 已有 → 不动，记一条）。"""
    if os.path.lexists(dst):
        say(f"  留着 {src}（{dst} 已有）")
        return False
    if not dry:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        os.rename(src, dst)
    return True


def rename_logs(D, keys, say, dry):
    d = os.path.join(D, proto.TASK_LOGS)
    n = 0
    for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        nf = log_rename(f, keys)
        if nf:
            n += move(os.path.join(d, f), os.path.join(d, nf), say, dry)
    if n:
        say(f"{d}: renamed {n} part logs")


def english_line(ln):
    """One part log line in the old Chinese format -> English; anything else unchanged (line ending kept)."""
    s = ln.rstrip("\r\n")
    eol = ln[len(s):]
    m = OLD_HEAD.match(s)
    if m:
        job, host, key, part, env, code, ts = m.groups()
        return proto.LOG_HEAD_FMT % (job, host, key, part, env, code, ts) + eol
    if s.startswith("训完"):
        return proto.DONE_LINE + s[len("训完"):] + eol
    if s.startswith("当作卡住"):
        rest = s[len("当作卡住"):]
        m = OLD_STALL_MSG.match(rest)
        if m:
            rest = f"{m.group(1)}: log and output dir unchanged for {m.group(2)} minutes, job {m.group(3)} stopped it"
        else:
            rest = rest.replace("：", ": ", 1)
        return proto.STALL_LINE + rest + eol
    m = OLD_ENDED.match(s)
    if m:
        ts, rc, rest = m.groups()
        if rest.startswith("读不了盘"):
            rest = "disk unreadable, cannot tell whether training finished"
        else:
            r = re.match(r"^没训完（第 (\d+) 次）$", rest)
            if r:
                rest = f"not done (attempt {r.group(1)})"
        return f"{ts} " + proto.LOG_ENDED_FMT % (rc, rest) + eol
    m = OLD_NOT_STARTED.match(s)
    if m:
        ts, why = m.groups()
        if why.startswith("读不了盘"):
            why = "disk unreadable, cannot tell whether training finished"
        else:
            r = re.match(r"^这台机器没有环境 (\S+)(?:（机器表 machines.py）)?$", why)
            if r:
                why = f"this machine has no env {r.group(1)} (machine table machines.py)"
            elif why.startswith("任务说明有问题："):
                why = "bad task spec: " + why[len("任务说明有问题："):]
        return f"{ts} " + proto.LOG_NOT_STARTED_FMT % why + eol
    m = OLD_GPU_GIVEUP_SEEN.match(s)
    if m:
        ts, gpu, seen, idle, waited = m.groups()
        extra = f" (free, someone {seen} still using it, needs {idle} minutes free), waited {waited} minutes"
        return f"{ts} " + proto.LOG_GPU_BUSY_FMT % (gpu, extra) + eol
    m = OLD_GPU_GIVEUP.match(s)
    if m:
        ts, gpu, waited = m.groups()
        return f"{ts} " + proto.LOG_GPU_BUSY_FMT % (gpu, f", waited {waited} minutes") + eol
    m = OLD_GPU_WAIT_SEEN.match(s)
    if m:
        ts, gpu, seen, idle, waited = m.groups()
        return f"{ts} GPU {gpu} free, but someone {seen} still used it; waiting {idle} minutes of quiet (waited {waited} minutes)" + eol
    m = OLD_GPU_WAIT.match(s)
    if m:
        ts, gpu, waited, most = m.groups()
        return f"{ts} GPU {gpu} busy, waiting for it (waited {waited} minutes{', at most ' + most + ' minutes' if most else ''})" + eol
    return ln


def english_logs(D, say, dry):
    """Rewrite part logs under <D>/logs/tasks from the old Chinese format to English. Idempotent; only matching lines change."""
    d = os.path.join(D, proto.TASK_LOGS)
    n = 0
    for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        p = os.path.join(d, f)
        if not f.endswith(".log") or not os.path.isfile(p):
            continue
        with open(p, encoding="utf-8", errors="surrogateescape") as fh:
            old = fh.read()
        new = "".join(english_line(ln) for ln in old.splitlines(keepends=True))
        if new != old:
            n += 1
            if not dry:
                C.atomic_write(p, new)
    if n:
        say(f"{d}: {n} part logs rewritten to English")


def english_failures(Q, say, dry):
    """Rewrite failure records <time>\\tjob <worker>\\t<reason>\\t<bundle> from the old job field. Idempotent."""
    d = os.path.join(Q, proto.QUEUE["failures"])
    n = 0
    for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        p = os.path.join(d, f)
        if not os.path.isfile(p):
            continue
        with open(p, encoding="utf-8", errors="surrogateescape") as fh:
            old = fh.read()
        out = []
        for ln in old.splitlines(keepends=True):
            s = ln.rstrip("\r\n")
            eol = ln[len(s):]
            x = s.split("\t")
            if len(x) > 1 and x[1].startswith(OLD_FAIL_JOB):
                x[1] = "job " + x[1][len(OLD_FAIL_JOB):]
                ln = "\t".join(x) + eol
            out.append(ln)
        new = "".join(out)
        if new != old:
            n += 1
            if not dry:
                C.atomic_write(p, new)
    if n:
        say(f"{d}: {n} failure records rewritten to English")


def merge_root(R, O, say, dry):
    """旧根目录 O 并进 R：分段日志、worker 日志搬过来；blob 同一个盘的硬链接过来（不同盘的不管，要用时再传）。"""
    import taskq
    old = taskq.read_tasks(os.path.join(O, proto.ROOT["queue"], proto.QUEUE["tasks"]))
    if old:
        raise Stop(f"{O} 的队列里还有 {len(old)} 个任务（{old[0]['key']} …）：先让它们做完或交还到别处")
    for sub in (proto.TASK_LOGS, proto.WORKER_LOGS):
        src = os.path.join(O, proto.ROOT["data"], sub)
        n = sum(move(os.path.join(src, f), os.path.join(R, proto.ROOT["data"], sub, f), say, dry)
                for f in (sorted(os.listdir(src)) if os.path.isdir(src) else []))
        say(f"{src} → {R}：搬 {n} 个文件")
    ob, rb = os.path.join(O, proto.ROOT["blobs"]), os.path.join(R, proto.ROOT["blobs"])
    if not os.path.isdir(ob):
        return
    if os.stat(ob).st_dev != os.stat(rb if os.path.isdir(rb) else R).st_dev:
        say(f"{ob} 和 {rb} 不在同一个盘：blob 不搬（要用时再传）")
        return
    n = 0
    for p in glob.glob(os.path.join(ob, "??", "*")):
        q = os.path.join(rb, os.path.relpath(p, ob))
        if not os.path.lexists(q) and os.path.isfile(p) and not os.path.islink(p):
            if not dry:
                os.makedirs(os.path.dirname(q), exist_ok=True)
                os.link(p, q)
            n += 1
    say(f"{ob} → {rb}：硬链接 {n} 个 blob（含 {proto.BLOB_STAT}）")


def view_links(R, D, views):
    """视图里指向 blob 的软链接 → {相对数据目录的路径: sha256}（relay 记账里的 links）。"""
    blobs = os.path.realpath(os.path.join(R, proto.ROOT["blobs"]))
    out = {}
    for v in views:
        for dp, _, fs in os.walk(os.path.join(D, v)):
            for f in fs:
                p = os.path.join(dp, f)
                t = os.path.realpath(p) if os.path.islink(p) else None
                if t and os.path.dirname(os.path.dirname(t)) == blobs:
                    out[os.path.relpath(p, D)] = os.path.basename(t)
    return out


def fix_queue(Q, keys, workers, say, dry, specs=None, drop=()):
    """队列目录 Q：任务改名、补 added、换成 specs {新任务名: 新说明} 里的说明；drop 里的旧任务连同记录删掉；按任务名放的记录
    跟着改名；交还记录补原因；skips 只留现在还有的 worker 的。"""
    import taskq
    tp = os.path.join(Q, proto.QUEUE["tasks"])
    busy = os.listdir(os.path.join(Q, proto.QUEUE["claims"])) if os.path.isdir(os.path.join(Q, proto.QUEUE["claims"])) else []
    if busy and workers is not None:
        raise Stop(f"{Q} 还有 {len(busy)} 个领着的任务（{busy[0]} …）：先交还（taskq stop 或停 worker）")
    drop = set(drop)
    tasks = taskq.read_tasks(tp)
    gone = [t["key"] for t in tasks if t["key"] in drop]
    tasks = [t for t in tasks if t["key"] not in drop]
    n = m = 0
    specs = specs or {}
    for t in tasks:
        if keys.get(t["key"], t["key"]) != t["key"]:
            t["key"] = keys[t["key"]]
            if t.get("spec"):
                t["spec"]["key"] = t["key"]
            n += 1
        if t.get("added") is None:
            t["added"] = 0
        if t["key"] in specs:
            t["spec"] = specs[t["key"]]  # 直接写新说明（不经 resend：那会把换了输出的当成重做，重算 added）
            m += 1
    if not dry:
        taskq.write_tasks(tasks, tp)
    say(f"{tp}：{len(tasks)} 个任务，{n} 个改名，{m} 个换成新说明" + "".join(f"\n  删 {k}（总队列里丢掉了）" for k in gone))
    for d in PER_KEY:
        base = os.path.join(Q, proto.QUEUE[d])
        for k in sorted(drop):
            p = os.path.join(base, safe(k))
            if os.path.lexists(p):
                say(f"  删 {p}")
                if not dry:
                    shutil.rmtree(p) if os.path.isdir(p) and not os.path.islink(p) else os.remove(p)
        for old, nk in keys.items():
            if old != nk and os.path.lexists(os.path.join(base, safe(old))):
                move(os.path.join(base, safe(old)), os.path.join(base, safe(nk)), say, dry)
    rd = os.path.join(Q, proto.QUEUE["released"])
    for f in sorted(os.listdir(rd)) if os.path.isdir(rd) else []:
        p = os.path.join(rd, f)
        x = open(p).read().split()
        if len(x) == 2:
            say(f"  {p}：补原因 {proto.RELEASE_WHY['back']}")
            if not dry:
                with open(p, "w") as fh:
                    fh.write(f"{x[0]} {x[1]} {proto.RELEASE_WHY['back']}\n")
    sd = os.path.join(Q, proto.QUEUE["skips"])
    for f in sorted(os.listdir(sd)) if workers is not None and os.path.isdir(sd) else []:
        p = os.path.join(sd, f)
        lines = open(p, encoding="utf-8").readlines()
        keep = [ln for ln in lines if len(ln.split("\t")) > 1 and ln.split("\t")[1] in workers]
        if len(keep) != len(lines):
            say(f"  {p}: drop {len(lines) - len(keep)} skip records of workers that no longer exist")
            if not dry:
                (open(p, "w", encoding="utf-8").writelines(keep) if keep else os.remove(p))
    english_failures(Q, say, dry)


def op_root(A):
    """在这台（TASKQ_ROOT 是它的根目录）：A = dict(dry, keys {旧任务名: 新的}, merge [旧根目录], views {任务名: [视图]},
    data [另外的数据目录], specs {新任务名: 按现在的做法解析的说明}, drop [总队列里丢掉的旧任务名])
    → dict(log [做了什么], links {任务名: {相对路径: sha256}})。"""
    import cfg
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
    import mtab as M
    log, dry, keys = [], A["dry"], A["keys"]
    R = os.path.expanduser(os.environ["TASKQ_ROOT"]).rstrip("/")
    me = M.whoami()
    for O in A["merge"]:
        merge_root(R, os.path.expanduser(O), log.append, dry)
    if M.is_exec(me):
        fix_queue(cfg.state("taskq"), keys, {w["id"] for w in M.workers(me)}, log.append, dry, A["specs"], A["drop"])
    D = os.path.join(R, proto.ROOT["data"])
    for d in [D] + [os.path.expanduser(x) for x in A["data"]]:
        rename_logs(d, keys, log.append, dry)
        english_logs(d, log.append, dry)
    pins = os.path.join(R, proto.ROOT["pins"])
    for old in keys:
        p = os.path.join(pins, safe(old) + ".json")
        if os.path.exists(p):
            log.append(f"删钉住记录 {p}（视图建好前才有，换了名字的任务下次传时重钉）")
            if not dry:
                os.remove(p)
    return dict(log=log, links={k: view_links(R, D, v) for k, v in A["views"].items()})


def root_main(argv):
    if argv != ["root"]:
        sys.exit("用法：migrate.py root（参数是标准输入上的 JSON）")
    out, sys.stdout = sys.stdout, sys.stderr
    try:
        res = op_root(json.load(sys.stdin))
    except Stop as e:
        res = dict(stop=str(e))
    finally:
        sys.stdout = out
    print(proto.JSON_MARK + json.dumps(res, ensure_ascii=False), flush=True)


# ---------------- hub 上 ----------------
def parse_pairs(xs, what):
    out = {}
    for x in xs:
        if "=" not in x:
            raise SystemExit(f"taskq self migrate：{what} 要写成 旧=新：{x}")
        a, b = x.split("=", 1)
        out[a] = b
    return out


def key_map(tasks):
    """总队列里做法 command 的任务 → 现在实验表的步骤实例（按产出 out 对上）：({旧名: 新名}, {新名: 参数}, [(旧名, 为什么丢掉)])。"""
    import advance
    from flow.compile import build
    flow = build(queue_tasks=[t for t in tasks if t["recipe"] != "command"])
    if flow.errors:
        raise Stop("实验表的步骤有问题：" + "；".join(flow.errors[:5]))
    by_out = {}
    for i in flow.instances.values():
        if i.step:
            by_out.setdefault(os.path.normpath(i.outputs[0]), []).append(i)
    keys, params, drop = {}, {}, []
    for t in tasks:
        if t["recipe"] != "command" or proto.is_smoke(t["key"]):
            continue
        hit = by_out.get(os.path.normpath(str(t["params"].get("out", "")).replace("{data}/", "")), [])
        if len(hit) != 1:
            drop.append((t["key"], "实验表里没有产出是它的步骤" if not hit else f"产出对上 {len(hit)} 个步骤实例"))
            continue
        i = hit[0]
        if i.id in params:
            drop.append((t["key"], f"和别的任务对上同一个步骤实例 {i.id}"))
            continue
        keys[t["key"]], params[i.id] = i.id, advance.queue_params(i)
    return keys, params, drop


def renamed_params(params):
    """任务的参数：旧的 data（任务说明的输入，哪种做法都一样）改叫 inputs；现在参数不能叫 data（spec.BUILTIN）。"""
    if "data" not in params:
        return params
    p = dict(params)
    p["inputs"] = p.pop("data")
    return p


def plan_hub(ren):
    """总队列 → (新的任务表, {旧名: 新名}, [丢掉的 (旧名, 为什么)], [要说的])。"""
    import experiments as E
    import taskq
    tasks = taskq.read_tasks()
    keys, params, drop = key_map(tasks)
    gone = {k for k, _ in drop}
    new, say = [], []
    for t in tasks:
        if t["key"] in gone:
            continue
        t = dict(t)
        if t["key"] in keys:
            t["key"], t["params"], t["exp"] = keys[t["key"]], params[keys[t["key"]]], None
        else:
            t["params"] = renamed_params(t["params"])
        if t["params"].get("machines"):
            t["params"]["machines"] = sorted({ren.get(m, m) for m in t["params"]["machines"]})
        if t.get("added") is None:
            t["added"] = 0
        try:
            E.resolve_task(t["recipe"], t["params"], t.get("exp"))
        except Exception as e:  # noqa: BLE001
            raise Stop(f"{t['key']} 按现在的做法解析不了：{e}")
        new.append(t)
    say.append(f"总队列：{len(tasks)} 个任务，{sum(o != n for o, n in keys.items())} 个改成步骤实例的名字，丢掉 {len(drop)} 个")
    say += [f"  丢掉 {k}：{why}" for k, why in drop]
    return new, keys, drop, say


def fix_hub_queue(Q, tasks, keys, drop, ren, say, dry):
    """总队列：写新任务表；按任务名的记录改名，丢掉的任务的记录删掉；领取、交还、失败、跳过里的机器名改名；交还记录补原因。"""
    import taskq
    if not dry:
        taskq.write_tasks(tasks, os.path.join(Q, proto.QUEUE["tasks"]))
    fix_queue(Q, keys, None, say, dry, drop=drop)
    cd = os.path.join(Q, proto.QUEUE["claims"])
    for f in sorted(os.listdir(cd)) if os.path.isdir(cd) else []:
        p = os.path.join(cd, f, proto.CLAIM_INFO)
        try:
            ci = json.load(open(p))
        except (OSError, ValueError):
            continue
        nc = {k: (ren.get(v, v) if k in ("worker", "host", "assigned") else v) for k, v in ci.items()}
        if nc != ci and not dry:
            with open(p, "w") as fh:
                json.dump(nc, fh)
    for d in ("released", "failures", "skips"):
        base = os.path.join(Q, proto.QUEUE[d])
        for f in sorted(os.listdir(base)) if os.path.isdir(base) else []:
            p = os.path.join(base, f)
            sep = " " if d == "released" else "\t"
            lines = open(p, encoding="utf-8").read().splitlines()
            out = []
            for ln in lines:
                x = ln.split(sep)
                if len(x) > 1:  # 第 2 段的最后一个词是机器名（failures 的是"作业 <机器>"）
                    w = x[1].split(" ")
                    w[-1] = ren.get(w[-1], w[-1])
                    x[1] = " ".join(w)
                if d == "failures" and len(x) == 3:  # 加上跑它的代码包：切换前的包都不再是在用的，记成 OLD_CODE
                    x.append(OLD_CODE)
                out.append(sep.join(x))
            if out != lines and not dry:
                with open(p, "w", encoding="utf-8") as fh:
                    fh.write("\n".join(out) + "\n")
    say(f"{Q}：领取、交还、失败、跳过里的机器名按 {ren or '（没有改名）'} 改；失败记录补上代码包一栏")


def fix_smoke(Q, ren, say, dry):
    """冒烟记录 smoke/<机器>.json：改名；几台并成一台时每种取最新的。"""
    d = os.path.join(Q, proto.SMOKE_DIR)
    if not os.path.isdir(d):
        return
    merged = {}
    for f in sorted(glob.glob(os.path.join(d, "*.json"))):
        old = os.path.basename(f)[:-5]
        try:
            recs = json.load(open(f))
        except ValueError:
            recs = {}
        to = merged.setdefault(ren.get(old, old), {})
        for k, r in recs.items():
            if k not in to or (r.get("t") or 0) > (to[k].get("t") or 0):
                to[k] = r
    say(f"冒烟记录：{sorted(os.path.basename(f)[:-5] for f in glob.glob(os.path.join(d, '*.json')))} → {sorted(merged)}")
    if dry:
        return
    for f in glob.glob(os.path.join(d, "*.json")) + glob.glob(os.path.join(d, "*.json.lock")):
        os.remove(f)
    import common
    for n, recs in merged.items():
        common.write_json(os.path.join(d, n + ".json"), recs)


def fix_relay(st, keys, params, ren, links, merged, drop, say):
    """relay 的记账 state.json（就地改 st）：机器改名、任务改名、做法参数换新、去掉 unq、补 links；并掉的旧根目录的记录、
    总队列里丢掉的任务的记录丢掉；代码包闸门挡下的（held，按旧包记的）清掉，换包后重新判断。"""
    out = {}
    for old, recs in (st.get("keys") or {}).items():
        if old in merged:
            say(f"relay 记账：丢掉 {old} 的 {len(recs)} 条（根目录并进了 {ren.get(old, old)}，队列已空）")
            continue
        to = out.setdefault(ren.get(old, old), {})
        for k, r in recs.items():
            if k in drop:
                say(f"relay 记账：丢掉 {old} 的 {k}（总队列里丢掉了）")
                continue
            r = {x: v for x, v in r.items() if x != "unq"}
            if "data" in r:  # 旧记账：inputs 是放过去的文件、data 是说明里的输入
                r["placed"], r["inputs"] = r.pop("inputs", []), r.pop("data")
            nk = keys.get(k, k)
            r["params"] = params[nk] if nk in params else renamed_params(r.get("params") or {})
            r.setdefault("links", (links.get(ren.get(old, old)) or {}).get(nk, {}))
            to[nk] = r
    st["keys"] = out
    for f in ("views", "last_ok"):
        v = {}
        for old, x in (st.get(f) or {}).items():
            if old in merged and f == "views":
                continue
            n = ren.get(old, old)
            if f == "views":
                v.setdefault(n, {}).update(x)
            else:
                v[n] = max(v.get(n, 0), x)
        st[f] = v
    st.pop("bundle_for", None)
    st.pop("held", None)
    say(f"relay 记账：{ {n: len(r) for n, r in out.items()} } 条；代码包按现在的提交重打")


def leftovers(home):
    """状态目录第一层不在 proto.STATE_NAMES 里的 → 删；relay/、health/ 里旧版本留下的备份、迁移前的文件也删。"""
    out = [os.path.join(home, f) for f in sorted(os.listdir(home))
           if not any(f == n or f.startswith((n + ".", n + "-")) for n in proto.STATE_NAMES)]
    for sub in ("relay", "health"):
        d = os.path.join(home, sub)
        for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
            if f.endswith(".migrated") or ".bak" in f or f in ("old", "status.json") or f.startswith("push_"):
                out.append(os.path.join(d, f))
    last = os.path.join(home, "last")
    out += sorted(glob.glob(os.path.join(last, "*")))  # "和上次一样就不写日志"的记录：清空只会多记一次
    return out


def move_health_log(home, say, dry):
    """健康检查的纯文字日志以前在状态目录的 health/ 里，现在和别的纯文字日志一样在 cfg.log 那里：接到那边的后面。"""
    import cfg
    src, dst = os.path.join(home, "health", "health.log"), cfg.log("health")
    if not os.path.isfile(src) or os.path.abspath(src) == os.path.abspath(dst):
        return
    say(f"{src} 接到 {dst} 后面")
    if not dry:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(src, "rb") as f, open(dst, "ab") as g:
            shutil.copyfileobj(f, g)
        os.remove(src)


def copy_hashes(say, dry):
    """hub 数据库里旧的 sha256 缓存 → store 根目录的 state/hashes.db（store 在本机时；路径只在那台有意义）。"""
    import sqlite3
    import db as DB
    import mtab as M
    p = DB.path()
    if not os.path.exists(p):
        return
    src = sqlite3.connect(p)
    try:
        if src.execute("PRAGMA user_version").fetchone()[0] >= 2 or not DB.has(src, "hash"):
            return
        rows = src.execute("SELECT k, sha FROM hash").fetchall()
    finally:
        src.close()
    s = M.store()
    if M.machine(s)["ssh"] is not None:
        say(f"数据库里的 {len(rows)} 条 sha256 缓存不搬（store {s} 不在本机，那边第一次用时重算）")
        return
    to = os.path.join(os.path.expanduser(M.machine(s)["root"]), proto.ROOT["hashes"])
    say(f"数据库里的 {len(rows)} 条 sha256 缓存 → {to}")
    if dry:
        return
    os.makedirs(os.path.dirname(to), exist_ok=True)
    d = sqlite3.connect(to)
    with d:
        d.execute("CREATE TABLE IF NOT EXISTS hash(k TEXT PRIMARY KEY, sha TEXT NOT NULL)")
        d.executemany("INSERT OR IGNORE INTO hash(k, sha) VALUES(?, ?)", rows)
    d.close()
    db = DB.connect(create=True)  # 跑数据库的迁移（去掉 hash 表）
    db.close()


def root_args(name, keys, merge, ren, views, dry, specs, drop):
    import mtab as M
    return dict(dry=dry, keys=keys, merge=[r for o, r in merge.items() if ren.get(o, o) == name], views=views,
                data=[M.data()] if name == M.store() else [], specs=specs.get(name, {}), drop=drop)


def new_specs(st):
    """relay 记账（已改名）→ {执行机: {任务名: 按现在的做法解析、送得出去的说明}}；解析不了 → Stop。"""
    import relay_store as ST
    import spec as S
    out = {}
    for name in st.get("keys") or {}:
        for k, r in ST.keys_of(st, name).items():
            try:
                out.setdefault(name, {})[k] = S.sendable(S.resolve(r["recipe"], r["params"]))
            except S.SpecError as e:
                raise Stop(f"{k}（{name}）按现在的做法解析不了：{e}")
    return out


def main(argv):
    if argv[:1] == ["root"]:
        return root_main(argv)
    ap = argparse.ArgumentParser(prog="taskq self migrate", description=__doc__.split("\n")[0])
    ap.add_argument("--apply", action="store_true", help="真的改（默认只列出要做的事）")
    ap.add_argument("--rename", nargs="*", default=[], metavar="旧名=新名")
    ap.add_argument("--merge", nargs="*", default=[], metavar="旧名=旧根目录")
    a = ap.parse_args(argv)
    import cfg
    cfg.use_config()
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
    import mtab as M
    import relay_remote as RM
    import relay_store as ST
    import taskq
    say, quiet = print, (lambda _: None)
    ren, merge = parse_pairs(a.rename, "--rename"), parse_pairs(a.merge, "--merge")
    st = ST.load_state()
    seen = set(st.get("keys") or {}) | set(st.get("views") or {}) | set(st.get("last_ok") or {})
    seen |= {os.path.basename(f)[:-5] for f in glob.glob(os.path.join(taskq.TQ, proto.SMOKE_DIR, "*.json"))}
    unknown = sorted(n for n in seen if ren.get(n, n) not in M.MACHINES)
    if unknown:
        sys.exit(f"taskq self migrate：记账里的 {'、'.join(unknown)} 不在机器表里，用 --rename 旧名=新名 指明是哪台")
    bad = [o for o in merge if ren.get(o, o) not in M.MACHINES or not M.is_exec(ren.get(o, o))]
    if bad:
        sys.exit(f"taskq self migrate：--merge 的 {'、'.join(bad)} 改名后不是机器表里的执行机")
    roots = [n for n in M.MACHINES if M.MACHINES[n].get("root")]

    def on(name, args, after):
        """在那台的新代码包里跑 migrate.py root；连不上、没换包、出错 → Stop（说清楚改到哪了）。"""
        try:
            return RM.op(name, "root", args, timeout=3600, script="migrate.py")
        except RuntimeError as e:
            raise Stop(f"{e}（先 taskq site bundle {name} 换上新代码包、确认连得上；{after}）")
    try:
        with taskq.Lock():
            tasks, keys, dropped, notes = plan_hub(ren)
            drop = sorted(k for k, _ in dropped)
            for n in notes:
                say(n)
            views = {}
            for old, recs in (st.get("keys") or {}).items():
                if old not in merge:
                    for k, r in recs.items():
                        if not r.get("links"):
                            views.setdefault(ren.get(old, old), {})[keys.get(k, k)] = r.get("views") or []
            links = {}
            for name in roots:  # 先在各台只看不改：有不能自动处理的就什么都不改
                res = on(name, root_args(name, keys, merge, ren, views.get(name, {}), True, {}, drop), "什么都还没改")
                if res.get("stop"):
                    raise Stop(f"{RM.conf(name)['label']}：{res['stop']}")
                links[name] = res["links"]
                say(f"== {RM.conf(name)['label']}" + "".join("\n  " + ln for ln in res["log"]))
            params = {t["key"]: t["params"] for t in tasks if t["key"] in keys.values()}
            fix_relay(st, keys, params, ren, links, set(merge), set(drop), say)
            specs = new_specs(st)
            say(f"执行机队列里的说明按现在的做法换掉：{sum(map(len, specs.values()))} 个")
            fix_hub_queue(taskq.TQ, tasks, keys, drop, ren, say, True)
            fix_smoke(taskq.TQ, ren, say, True)
            home = cfg.state().rstrip("/")
            move_health_log(home, say, True)
            junk = leftovers(home)
            say(f"状态目录 {home} 里不再用的：{len(junk)} 项" + "".join(f"\n  删 {p}" for p in junk))
            copy_hashes(say, True)
            if not a.apply:
                say("（只列出，没改；确认后加 --apply）")
                return 0
            for name in roots:
                res = on(name, root_args(name, keys, merge, ren, {}, False, specs, drop), "这台之前的几台已经改了，处理好后再跑一次")
                if res.get("stop"):
                    raise Stop(f"{RM.conf(name)['label']}：{res['stop']}（这台之前的几台已经改了，处理好后再跑一次）")
            ST.update_state(lambda x: (x.clear(), x.update(st)))
            fix_hub_queue(taskq.TQ, tasks, keys, drop, ren, quiet, False)
            fix_smoke(taskq.TQ, ren, quiet, False)
            copy_hashes(quiet, False)
            move_health_log(home, quiet, False)
            for p in junk:
                shutil.rmtree(p) if os.path.isdir(p) and not os.path.islink(p) else os.remove(p)
    except Stop as e:
        print(f"taskq self migrate：{e}", file=sys.stderr)
        return 1
    for o, r in merge.items():
        say(f"旧根目录 {r}（{o}）没删；确认 {ren.get(o, o)} 一切正常后在那台上删：rm -rf {r}")
    say(f"迁移完成（{time.strftime('%F %T')}）")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
