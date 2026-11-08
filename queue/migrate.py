"""taskq self migrate: rewrite on-disk state from the old format to the current one, once per cutover; safe to re-run.
By default it only lists what it would do, --apply does it.

    taskq self migrate [--apply] [--rename old=new ...] [--merge old=old-root ...]
      --rename  old machine names (ledger, claims, smoke records) -> names in the machine table; several old names may merge
      --merge   merge an old root into its new machine's root (same physical machine: move part logs, hardlink blobs on
                the same disk; the old queue must be empty; the old root itself is kept, the delete command is printed)
The hub side rewrites the queue, the relay ledger, smoke records, unused state-dir files and old config entries. Executors
and the store run `migrate.py root` from their bundle through relay_remote.op: rename tasks and part logs, replace specs,
merge roots, collect view links.
"""
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


# ---------------- on one machine (inside the bundle, run through relay_remote.op) ----------------
def log_rename(name, keys):
    """Part log file name -> new name (None when unchanged). <safe key>__<job>_part<part>.log: a job name never contains "__"."""
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
    """src -> dst (dst exists -> leave it alone and say so)."""
    if os.path.lexists(dst):
        say(f"  kept {src} ({dst} exists)")
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


def merge_root(R, old_root, say, dry):
    """Merge old root into R: move part logs and worker logs; hardlink blobs on the same disk (other disks: leave them, transfer when needed)."""
    import taskq
    old = taskq.read_tasks(os.path.join(old_root, proto.ROOT["queue"], proto.QUEUE["tasks"]))
    if old:
        raise Stop(f"{old_root} still has {len(old)} task(s) in its queue ({old[0]['key']} ...): let them finish or release them elsewhere first")
    for sub in (proto.TASK_LOGS, proto.WORKER_LOGS):
        src = os.path.join(old_root, proto.ROOT["data"], sub)
        n = sum(move(os.path.join(src, f), os.path.join(R, proto.ROOT["data"], sub, f), say, dry)
                for f in (sorted(os.listdir(src)) if os.path.isdir(src) else []))
        say(f"{src} -> {R}: moved {n} files")
    ob, rb = os.path.join(old_root, proto.ROOT["blobs"]), os.path.join(R, proto.ROOT["blobs"])
    if not os.path.isdir(ob):
        return
    if os.stat(ob).st_dev != os.stat(rb if os.path.isdir(rb) else R).st_dev:
        say(f"{ob} and {rb} are on different disks: blobs not moved (transfer them when needed)")
        return
    n = 0
    for p in glob.glob(os.path.join(ob, "??", "*")):
        q = os.path.join(rb, os.path.relpath(p, ob))
        if not os.path.lexists(q) and os.path.isfile(p) and not os.path.islink(p):
            if not dry:
                os.makedirs(os.path.dirname(q), exist_ok=True)
                os.link(p, q)
            n += 1
    say(f"{ob} -> {rb}: hardlinked {n} blobs (including {proto.BLOB_STAT})")


def view_links(R, D, views):
    """Symlinks in views pointing at blobs -> {path relative to the data dir: sha256} (links in the relay ledger)."""
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
    """Queue dir Q: rename tasks, fill in added, replace specs from specs {new task name: new spec}; delete tasks in drop
    with their records; rename per-task-name records; add the reason to release records; keep only skips of workers
    that still exist."""
    import taskq
    tp = os.path.join(Q, proto.QUEUE["tasks"])
    busy = os.listdir(os.path.join(Q, proto.QUEUE["claims"])) if os.path.isdir(os.path.join(Q, proto.QUEUE["claims"])) else []
    if busy and workers is not None:
        raise Stop(f"{Q} still has {len(busy)} claimed tasks ({busy[0]} ...): release them first (taskq stop or stop the worker)")
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
            t["spec"] = specs[t["key"]]  # write the new spec directly (not via resend: that would treat a changed output as a redo and recompute added)
            m += 1
    if not dry:
        taskq.write_tasks(tasks, tp)
    say(f"{tp}: {len(tasks)} tasks, {n} renamed, {m} given a new spec" + "".join(f"\n  drop {k} (gone from the queue)" for k in gone))
    for d in PER_KEY:
        base = os.path.join(Q, proto.QUEUE[d])
        for k in sorted(drop):
            p = os.path.join(base, safe(k))
            if os.path.lexists(p):
                say(f"  drop {p}")
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
            say(f"  {p}: add reason {proto.RELEASE_WHY['back']}")
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
    """On this machine (TASKQ_ROOT is its root): A = dict(dry, keys {old task name: new}, merge [old root], views {task
    name: [views]}, data [extra data dirs], specs {new task name: spec resolved with the current recipes}, drop [old task
    names gone from the queue]) -> dict(log [what was done], links {task name: {relative path: sha256}})."""
    import cfg
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
    import mtab as M
    log, dry, keys = [], A["dry"], A["keys"]
    R = os.path.expanduser(os.environ["TASKQ_ROOT"]).rstrip("/")
    me = M.whoami()
    for old_root in A["merge"]:
        merge_root(R, os.path.expanduser(old_root), log.append, dry)
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
            log.append(f"drop pin record {p} (only exists before the view is built; a renamed task is pinned again on its next transfer)")
            if not dry:
                os.remove(p)
    return dict(log=log, links={k: view_links(R, D, v) for k, v in A["views"].items()})


def root_main(argv):
    if argv != ["root"]:
        sys.exit("usage: migrate.py root (parameters are JSON on stdin)")
    out, sys.stdout = sys.stdout, sys.stderr
    try:
        res = op_root(json.load(sys.stdin))
    except Stop as e:
        res = dict(stop=str(e))
    finally:
        sys.stdout = out
    print(proto.JSON_MARK + json.dumps(res, ensure_ascii=False), flush=True)


# ---------------- on the hub ----------------
def parse_pairs(xs, what):
    out = {}
    for x in xs:
        if "=" not in x:
            raise SystemExit(f"taskq self migrate: {what} must be old=new: {x}")
        a, b = x.split("=", 1)
        out[a] = b
    return out


def key_map(tasks):
    """Queue tasks with recipe command -> current experiment-table step instances (matched by output out): ({old: new},
    {new: params}, [(old, why dropped)])."""
    import advance
    from flow.compile import build
    flow = build(queue_tasks=[t for t in tasks if t["recipe"] != "command"])
    if flow.errors:
        raise Stop("bad steps in the experiment table: " + "; ".join(flow.errors[:5]))
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
            drop.append((t["key"], "no step in the experiment table has this output" if not hit else f"output matches {len(hit)} step instances"))
            continue
        i = hit[0]
        if i.id in params:
            drop.append((t["key"], f"same step instance {i.id} as another task"))
            continue
        keys[t["key"]], params[i.id] = i.id, advance.queue_params(i)
    return keys, params, drop


def renamed_params(params):
    """Task parameters: old data (the spec's inputs, the same in every recipe) becomes inputs; parameters may no longer be called data (spec.BUILTIN)."""
    if "data" not in params:
        return params
    p = dict(params)
    p["inputs"] = p.pop("data")
    return p


def plan_hub(ren):
    """Queue -> (new task list, {old: new}, [(dropped old, why)], [lines to say])."""
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
            raise Stop(f"{t['key']} cannot be resolved with the current recipes: {e}")
        new.append(t)
    say.append(f"Queue: {len(tasks)} tasks, {sum(o != n for o, n in keys.items())} renamed to step instance names, {len(drop)} dropped")
    say += [f"  dropped {k}: {why}" for k, why in drop]
    return new, keys, drop, say


def fix_hub_queue(Q, tasks, keys, drop, ren, say, dry):
    """Hub queue: write the new task list; rename per-task-name records and delete those of dropped tasks; rename machines
    in claims, releases, failures and skips; add the reason to release records."""
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
                if len(x) > 1:  # the last word of field 2 is the machine name (for failures: "job <machine>")
                    w = x[1].split(" ")
                    w[-1] = ren.get(w[-1], w[-1])
                    x[1] = " ".join(w)
                if d == "failures" and len(x) == 3:  # add the bundle that ran it: bundles from before the cutover are no longer in use, record OLD_CODE
                    x.append(OLD_CODE)
                out.append(sep.join(x))
            if out != lines and not dry:
                with open(p, "w", encoding="utf-8") as fh:
                    fh.write("\n".join(out) + "\n")
    say(f"{Q}: machine names in claims, releases, failures and skips updated per {ren or '(no renames)'}; failure records given a bundle column")


def fix_smoke(Q, ren, say, dry):
    """Smoke records smoke/<machine>.json: rename; when several machines merge, keep the newest record per kind."""
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
    say(f"Smoke records: {sorted(os.path.basename(f)[:-5] for f in glob.glob(os.path.join(d, '*.json')))} -> {sorted(merged)}")
    if dry:
        return
    for f in glob.glob(os.path.join(d, "*.json")) + glob.glob(os.path.join(d, "*.json.lock")):
        os.remove(f)
    import common
    for n, recs in merged.items():
        common.write_json(os.path.join(d, n + ".json"), recs)


def fix_relay(st, keys, params, ren, links, merged, drop, say):
    """Relay ledger state.json (edited in place): rename machines and tasks, replace recipe parameters, drop unq, fill in
    links; drop records of merged old roots and of tasks gone from the queue; clear bundle-gate holds (held, recorded
    under the old bundle) so the new bundle is judged afresh."""
    out = {}
    for old, recs in (st.get("keys") or {}).items():
        if old in merged:
            say(f"relay ledger: dropping {len(recs)} records of {old} (root merged into {ren.get(old, old)}, queue empty)")
            continue
        to = out.setdefault(ren.get(old, old), {})
        for k, r in recs.items():
            if k in drop:
                say(f"relay ledger: dropping {old} {k} (gone from the queue)")
                continue
            r = {x: v for x, v in r.items() if x != "unq"}
            if "data" in r:  # old ledger: inputs were the files placed there, data was the spec's inputs
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
    say(f"relay ledger: { {n: len(r) for n, r in out.items()} } records; bundles will be rebuilt from the current commit")


def leftovers(home):
    """First-level entries in the state dir not in proto.STATE_NAMES -> delete; also old backups left by earlier versions
    in relay/, health/ and files from before a migration."""
    out = [os.path.join(home, f) for f in sorted(os.listdir(home))
           if not any(f == n or f.startswith((n + ".", n + "-")) for n in proto.STATE_NAMES)]
    for sub in ("relay", "health"):
        d = os.path.join(home, sub)
        for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
            if f.endswith(".migrated") or ".bak" in f or f in ("old", "status.json") or f.startswith("push_"):
                out.append(os.path.join(d, f))
    last = os.path.join(home, "last")
    out += sorted(glob.glob(os.path.join(last, "*")))  # "skip a repeat" records: clearing them only logs one extra line
    return out


def move_health_log(home, say, dry):
    """Append the health check's plain-text log from its old <state dir>/health/ location to cfg.log, where the other
    plain-text logs live."""
    import cfg
    src, dst = os.path.join(home, "health", "health.log"), cfg.log("health")
    if not os.path.isfile(src) or os.path.abspath(src) == os.path.abspath(dst):
        return
    say(f"appending {src} to {dst}")
    if not dry:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(src, "rb") as f, open(dst, "ab") as g:
            shutil.copyfileobj(f, g)
        os.remove(src)


def copy_hashes(say, dry):
    """Old sha256 cache in the hub database -> <store root>/state/hashes.db (only when the store is local; the path only makes sense there)."""
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
        say(f"not moving {len(rows)} sha256 cache rows from the database (store {s} is not local, it will recompute on first use)")
        return
    to = os.path.join(os.path.expanduser(M.machine(s)["root"]), proto.ROOT["hashes"])
    say(f"{len(rows)} sha256 cache rows from the database -> {to}")
    if dry:
        return
    os.makedirs(os.path.dirname(to), exist_ok=True)
    d = sqlite3.connect(to)
    with d:
        d.execute("CREATE TABLE IF NOT EXISTS hash(k TEXT PRIMARY KEY, sha TEXT NOT NULL)")
        d.executemany("INSERT OR IGNORE INTO hash(k, sha) VALUES(?, ?)", rows)
    d.close()
    db = DB.connect(create=True)  # run the database migration (drops the hash table)
    db.close()


def root_args(name, keys, merge, ren, views, dry, specs, drop):
    import mtab as M
    return dict(dry=dry, keys=keys, merge=[r for o, r in merge.items() if ren.get(o, o) == name], views=views,
                data=[M.data()] if name == M.store() else [], specs=specs.get(name, {}), drop=drop)


def new_specs(st):
    """Relay ledger (already renamed) -> {executor: {task name: sendable spec resolved with the current recipes}}; cannot resolve -> Stop."""
    import relay_store as ST
    import spec as S
    out = {}
    for name in st.get("keys") or {}:
        for k, r in ST.keys_of(st, name).items():
            try:
                out.setdefault(name, {})[k] = S.sendable(S.resolve(r["recipe"], r["params"]))
            except S.SpecError as e:
                raise Stop(f"{k} ({name}) cannot be resolved with the current recipes: {e}")
    return out


def main(argv):
    if argv[:1] == ["root"]:
        return root_main(argv)
    ap = argparse.ArgumentParser(prog="taskq self migrate", description=__doc__.split("\n")[0])
    ap.add_argument("--apply", action="store_true", help="really change things (default: only list what would be done)")
    ap.add_argument("--rename", nargs="*", default=[], metavar="old=new")
    ap.add_argument("--merge", nargs="*", default=[], metavar="old=old-root")
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
        sys.exit(f"taskq self migrate: {', '.join(unknown)} in the ledger is not in the machine table, use --rename old=new to say which machine it is")
    bad = [o for o in merge if ren.get(o, o) not in M.MACHINES or not M.is_exec(ren.get(o, o))]
    if bad:
        sys.exit(f"taskq self migrate: {', '.join(bad)} in --merge is not an executor in the machine table after renaming")
    roots = [n for n in M.MACHINES if M.MACHINES[n].get("root")]

    def on(name, args, after):
        """Run migrate.py root in that machine's new bundle; unreachable, old bundle or error -> Stop (saying how far it got)."""
        try:
            return RM.op(name, "root", args, timeout=3600, script="migrate.py")
        except RuntimeError as e:
            raise Stop(f"{e} (install the new bundle first with taskq site bundle {name}, check it is reachable; {after})")
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
            for name in roots:  # look only, change nothing on any machine until all of them can be handled
                res = on(name, root_args(name, keys, merge, ren, views.get(name, {}), True, {}, drop), "nothing has been changed yet")
                if res.get("stop"):
                    raise Stop(f"{RM.conf(name)['label']}: {res['stop']}")
                links[name] = res["links"]
                say(f"== {RM.conf(name)['label']}" + "".join("\n  " + ln for ln in res["log"]))
            params = {t["key"]: t["params"] for t in tasks if t["key"] in keys.values()}
            fix_relay(st, keys, params, ren, links, set(merge), set(drop), say)
            specs = new_specs(st)
            say(f"Specs in executor queues replaced with the current recipes: {sum(map(len, specs.values()))}")
            fix_hub_queue(taskq.TQ, tasks, keys, drop, ren, say, True)
            fix_smoke(taskq.TQ, ren, say, True)
            home = cfg.state().rstrip("/")
            move_health_log(home, say, True)
            junk = leftovers(home)
            say(f"Unused items in state dir {home}: {len(junk)}" + "".join(f"\n  drop {p}" for p in junk))
            copy_hashes(say, True)
            if not a.apply:
                say("(listed only, nothing changed; add --apply when ready)")
                return 0
            for name in roots:
                res = on(name, root_args(name, keys, merge, ren, {}, False, specs, drop), "machines before this one are already changed, fix this and run again")
                if res.get("stop"):
                    raise Stop(f"{RM.conf(name)['label']}: {res['stop']} (machines before this one are already changed, fix this and run again)")
            ST.update_state(lambda x: (x.clear(), x.update(st)))
            fix_hub_queue(taskq.TQ, tasks, keys, drop, ren, quiet, False)
            fix_smoke(taskq.TQ, ren, quiet, False)
            copy_hashes(quiet, False)
            move_health_log(home, quiet, False)
            for p in junk:
                shutil.rmtree(p) if os.path.isdir(p) and not os.path.islink(p) else os.remove(p)
    except Stop as e:
        print(f"taskq self migrate: {e}", file=sys.stderr)
        return 1
    for o, r in merge.items():
        say(f"old root {r} ({o}) not deleted; once {ren.get(o, o)} looks fine, delete it on that machine: rm -rf {r}")
    say(f"migration complete ({time.strftime('%F %T')})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
