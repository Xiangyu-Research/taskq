"""Operations that take more than one health check round, kept in the hub database's intent table and advanced by every round
(health.py calls run before sending, and again after it for rm): taskq rm of a task in use, move, now, after, and cancel (removes
what it can now and leaves rm intents for the rest). Each change of state writes an event (source intent, kind proto.EV_INTENT).

    taskq rm <key>...                       unclaimed: removed now; in use: stopped now, removed once released
    taskq move <key> --to <executor> [--restart]
    taskq now <key> [--on <executor>] [--no-preempt]
    taskq after <key> running|done|failed [--within 7d] -- <taskq command>
    taskq cancel --exp E [--only PARAM=VALUE...] [--hold] [-n]
    taskq intents [--all] [--json] [--drop ID]
"""
import argparse
import fnmatch
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime

import cfg
import db as D
import events
import proto
import taskq

ACTIVE = "active"
BIN = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bin", "taskq")
NOW_WHY = "making room for {} (taskq now)"
VALUED = {"--to", "--on", "--nice", "--exp", "--only", "--why", "--site", "--worker", "--within", "--recipe", "--file"}


def rows(db=None, everything=False, key=None, kind=None):
    db = db if db is not None else D.connect(ro=True)
    if db is None or not D.has(db, "intent"):
        return []
    w, a = [], []
    for col, v in (("state", None if everything else ACTIVE), ("key", key), ("kind", kind)):
        if v is not None:
            w.append(f"{col} = ?")
            a.append(v)
    out = []
    for r in db.execute(f"SELECT {', '.join(proto.INTENT_FIELDS)} FROM intent{' WHERE ' + ' AND '.join(w) if w else ''} ORDER BY id", a):
        d = dict(zip(proto.INTENT_FIELDS, r))
        d["args"] = json.loads(d["args"] or "{}")
        out.append(d)
    return out


def only(db=None):
    """Active move intents, and now intents with --on -> {key: dict(to=executor, restart=bool)} (refill.choose sends those keys only
    there; restart: ignore progress elsewhere)."""
    out = {}
    for it in rows(db):
        to = it["args"].get("to") if it["kind"] == "move" else it["args"].get("on") if it["kind"] == "now" else None
        if to:
            out[it["key"]] = dict(to=to, restart=bool(it["args"].get("restart")))
    return out


def _log(it, msg, db=None, level=None):
    events.add("intent", f"{it['kind']} {it['key']}: {msg}", db=db, kind=proto.EV_INTENT, key=it["key"], level=level,
               site=it["args"].get("to") or it["args"].get("on") or it["args"].get("site"),
               data=dict(id=it["id"], kind=it["kind"], state=it["state"]))


def new(kind, key, args, note, deadline=None, by=None):
    db = D.connect(create=True)
    now = time.time()
    with D.tx(db):
        cur = db.execute("INSERT INTO intent(kind, key, args, state, t, deadline, note, by) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                         (kind, key, json.dumps(args), ACTIVE, now, deadline or now + proto.INTENT_KEEP, note, by or taskq.here()))
    it = dict(id=cur.lastrowid, kind=kind, key=key, args=args, state=ACTIVE, t=now, note=note)
    _log(it, note, db)
    return it


def save(db, it, state=None, note=None, level=None):
    """Write it back (args, and state / note when given); an event when the state or the note changed."""
    changed = (state and state != it["state"]) or (note and note != it.get("note"))
    it.update(state=state or it["state"], note=note or it.get("note"))
    with D.tx(db):
        db.execute("UPDATE intent SET state = ?, note = ?, args = ? WHERE id = ?", (it["state"], it["note"], json.dumps(it["args"]), it["id"]))
    if changed:
        _log(it, f"{it['note']}" + ("" if it["state"] == ACTIVE else f" ({it['state']})"), db, level)


# ---------------- hub queue ----------------
def _hub():
    return {t["key"]: t for t in taskq.read_tasks()}


def _claim(key):
    return taskq.claim_info(key) or {}


def _where(ci):
    return ci.get("assigned") or (f"{ci.get('host') or taskq.here()} worker {ci.get('worker')}" if ci.get("worker") else "")


def _stop(keys, why, push=True):
    """Stop keys in the hub queue; push: also in the queue of the executor each is assigned to, right away (the health check would
    on its next round) -> lines."""
    rc, out, err = taskq.run(["stop", "--why", why, "--", *keys])
    lines = [ln for ln in (out + err).splitlines() if ln.strip()]
    by = {}
    for k in keys:
        s = _claim(k).get("assigned")
        if push and s and cfg.mtab().is_exec(s):
            by.setdefault(s, []).append(k)
    if by:
        import relay_remote as RM
        for s, ks in sorted(by.items()):
            lines += RM.push_stop(s, ks, why)
    return lines


def _ours(key, why):
    """Whether key's stop record is the one an intent made (why), so undoing it does not undo a stop someone else made."""
    r = taskq.stopped_info(key)
    return bool(r) and r.endswith(why)


def _holders(key):
    import hubstate as HS
    import refill
    M = cfg.mtab()
    views = {n: (d or {}).get("tasks") or {} for n, d in HS.sites().items()}
    return refill.holders(views, lambda n: M.is_exec(n) and not M.paused(n)).get(key) or []


def _target(t, to, restart, verb):
    """Can t be sent to executor to? -> None, or why not (printed, exit REFUSED)."""
    import refill
    M = cfg.mtab()
    if to not in M.MACHINES or not M.is_exec(to):
        return f"{to} is not an executor (taskq machines names)"
    sp = taskq.spec_of(t)
    if sp is None:
        return f"{t['key']}: its spec cannot be read ({t.get('bad')})"
    opts = refill.places(t.get("recipe"), sp)
    if to not in opts:
        return f"{t['key']} cannot go to {to} (PLACE, machines and {refill.kind_of(sp)} workers allow: {', '.join(opts) or 'none'})"
    held = [n for n in _holders(t["key"]) if n != to]
    if held and not restart:
        return (f"{t['key']} is half-done on {', '.join(held)}; {verb} would start it over on {to} "
                + ("(taskq move --restart does that)" if verb != "move" else "(add --restart to do that)"))
    return None


def _clear_skips(key, site):
    with taskq.Lock():
        taskq.ST().keep_lines("skips", key, lambda ln: ln.split("\t")[1:2] != [site])


def _hub_only(cmd):
    if taskq.here() != taskq.queue_host():
        print(f"taskq {cmd} runs on the hub ({taskq.queue_host()}); this is {taskq.here()}", file=sys.stderr)
        return True
    return False


# ---------------- commands ----------------
def cmd_rm(a):
    if taskq.here() != taskq.queue_host():
        return taskq.main(["rm", *a.keys])
    hub = _hub()
    missing = [k for k in a.keys if k not in hub]
    for k in missing:
        print(f"not in queue: {k}", file=sys.stderr)
    free = [k for k in a.keys if k in hub and not _claim(k)]
    used = [k for k in a.keys if k in hub and _claim(k)]
    rc = taskq.main(["rm", *free]) if free else 0
    for k in used:
        if rows(key=k, kind="rm"):
            print(f"{k}: already being removed (taskq intents)")
            continue
        ci = _claim(k)
        lines = _stop([k], "taskq rm")
        new("rm", k, dict(site=ci.get("assigned")), f"stopped on {_where(ci)}; removed once it is released")
        print(f"{k}: stopped on {_where(ci)}; removed once it is released (taskq intents)")
        for ln in lines:
            if not ln.startswith("stopped: "):
                print(f"  {ln}")
    return rc if free or used else taskq.GONE


def cmd_move(a):
    if _hub_only("move"):
        return 2
    t = _hub().get(a.key)
    if t is None:
        print(f"not in queue: {a.key}", file=sys.stderr)
        return taskq.GONE
    ci = _claim(a.key)
    if ci.get("assigned") == a.to:
        print(f"{a.key}: already on {a.to}")
        return 0
    bad = _target(t, a.to, a.restart, "move")
    if bad:
        print(f"not moved: {bad}", file=sys.stderr)
        return taskq.REFUSED
    for it in rows(key=a.key):
        if it["kind"] in ("move", "now", "rm"):
            print(f"not moved: {a.key} already has a {it['kind']} under way (taskq intents --drop {it['id']} first)", file=sys.stderr)
            return taskq.REFUSED
    _clear_skips(a.key, a.to)
    lines = _stop([a.key], f"taskq move to {a.to}") if ci else []
    note = (f"stopped on {_where(ci)}; sent only to {a.to} once released" if ci else f"sent only to {a.to}")
    new("move", a.key, dict(to=a.to, restart=a.restart, stopped=bool(ci), site=ci.get("assigned")), note)
    print(f"{a.key}: {note}" + (" (starting over)" if a.restart else ""))
    for ln in lines:
        if not ln.startswith("stopped: "):
            print(f"  {ln}")
    if not ci and taskq.stopped_info(a.key):
        print(f"  {a.key} is stopped: taskq resume {a.key} to send it")
    return 0


def cmd_now(a):
    if _hub_only("now"):
        return 2
    t = _hub().get(a.key)
    if t is None:
        print(f"not in queue: {a.key}", file=sys.stderr)
        return taskq.GONE
    if a.on:
        bad = _target(t, a.on, False, "now --on")
        if bad:
            print(f"not changed: {bad}", file=sys.stderr)
            return taskq.REFUSED
        _clear_skips(a.key, a.on)
    for it in rows(key=a.key):
        if it["kind"] in ("move", "now", "rm"):
            print(f"not changed: {a.key} already has a {it['kind']} under way (taskq intents --drop {it['id']} first)", file=sys.stderr)
            return taskq.REFUSED
    rc, out, err = taskq.run(["nice", "--nice", str(proto.NOW_NICE), "--", a.key])
    print((out + err).strip())
    if rc:
        return rc
    how = "" if a.no_preempt else f"; after {proto.NOW_WAIT // 60} min waiting on an executor, one lower-priority task there makes room"
    new("now", a.key, dict(on=a.on, no_preempt=a.no_preempt, nice=t["nice"]), f"first in line{f' on {a.on}' if a.on else ''}{how}")
    print(f"{a.key}: first in line{f' (only on {a.on})' if a.on else ''}{how}")
    if taskq.stopped_info(a.key):
        print(f"  {a.key} is stopped: taskq resume {a.key} to send it")
    return 0


def seconds(x):
    import re
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd]?)", x.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"not a duration: {x} (90s, 30m, 6h, 2d)")
    return float(m.group(1)) * dict(s=1, m=60, h=3600, d=86400)[m.group(2) or "s"]


def cmd_after(a):
    if _hub_only("after"):
        return 2
    cmd = list(a.then)
    if not cmd or cmd[0] not in proto.AFTER_CMDS:
        print(f"taskq after: the command must be one of {', '.join(proto.AFTER_CMDS)} (after --)", file=sys.stderr)
        return 2
    hub = _hub()
    if a.key not in hub and a.when != "done":
        print(f"not in queue: {a.key}", file=sys.stderr)
        return taskq.GONE
    bad = recheck(cmd, hub)
    if bad:
        print(f"not set: {bad}", file=sys.stderr)
        return taskq.REFUSED
    it = new("after", a.key, dict(when=a.when, cmd=cmd), f"once {a.when}: taskq {shlex.join(cmd)}", deadline=time.time() + a.within)
    print(f"{a.key}: once {a.when}, taskq {shlex.join(cmd)} (checked every round until {time.strftime('%m-%d %H:%M', time.localtime(time.time() + a.within))};"
          f" taskq intents --drop {it['id']} to cancel)")
    return 0


def cmd_cancel(a):
    if _hub_only("cancel"):
        return 2
    import experiments as E
    e = E.experiment(a.exp)
    want = {}
    for kv in a.only:
        k, _, v = kv.partition("=")
        want[k] = [x for x in v.split(",") if x]
    table = [t for t in E.tasks().values() if t["exp"] == a.exp and all(str(t["each"].get(k)) in vs for k, vs in want.items())]
    pats = [f"*/{E.label(e, t)}" for t in table] if e else []
    if e and not want:
        pats.append(f"*/{a.exp}")
    hub = _hub()

    def mine(t):
        if t.get("exp") == a.exp:
            return all(str((t.get("params") or {}).get(k)) in vs for k, vs in want.items())
        return any(fnmatch.fnmatch(t["key"], p) for p in pats)
    keys = [k for k, t in hub.items() if mine(t)]
    if not keys and not (a.hold and pats):
        print(f"nothing of {a.exp} in the hub queue" + ("" if e else " (and no such experiment in the tables)"), file=sys.stderr)
        return taskq.GONE
    free = [k for k in keys if not _claim(k)]
    used = [k for k in keys if _claim(k)]
    w = "would " if a.dry_run else ""
    for k in free:
        print(f"{w}remove {k}")
    for k in used:
        print(f"{w}stop {k} on {_where(_claim(k))}, remove it once released")
    if a.hold:
        for p in pats:
            print(f"{w}hold downstream steps {p}")
    if a.dry_run:
        return 0
    rc = 0
    if free:
        r, out, err = taskq.run(["rm", *free])
        print((out + err).strip())
        rc = r
    if used:
        rc = max(rc, cmd_rm(argparse.Namespace(keys=used)))
    if a.hold:
        import switches
        for p in pats:
            switches.put("hold", p, "on", why_=f"taskq cancel --exp {a.exp}")
    return rc


def cmd_list(a):
    if a.drop is not None:
        db = D.connect(create=True)
        it = next((r for r in rows(db, everything=True) if r["id"] == a.drop), None)
        if it is None or it["state"] != ACTIVE:
            print(f"no active intent {a.drop}", file=sys.stderr)
            return taskq.GONE
        undo(it)
        save(db, it, "dropped", "dropped (taskq intents --drop)")
        print(f"dropped {it['id']}: {it['kind']} {it['key']}")
        return 0
    rs = rows(everything=a.all)
    if a.json:
        print(proto.JSON_MARK + json.dumps(dict(version=proto.VERSION, intents=rs), ensure_ascii=False))
        return 0
    if not rs:
        print("no intents under way" if not a.all else "no intents")
        return 0
    for r in rs:
        end = f", until {time.strftime('%m-%d %H:%M', time.localtime(r['deadline']))}" if r["state"] == ACTIVE and r["deadline"] else ""
        print(f"{r['id']:>4} {r['kind']:<5} {r['state']:<7} {time.strftime('%m-%d %H:%M', time.localtime(r['t']))}{end}  {r['key']}\n"
              f"      {r['note']}")
    return 0


# ---------------- each round ----------------
def recheck(cmd, hub):
    """A taskq command an after intent will run -> None, or why it no longer fits the experiment table (a task named in it whose
    experiment, or the task itself, left the table; --exp naming an experiment no longer there; a key gone from the hub queue)."""
    import experiments as E
    try:
        wanted = E.tasks()
    except Exception as e:  # noqa: BLE001
        return f"the experiment tables cannot be read ({type(e).__name__}: {e})"
    args, i = cmd[1:], 0
    while i < len(args):
        x = args[i]
        if x in VALUED:
            if x == "--exp" and i + 1 < len(args) and E.experiment(args[i + 1]) is None:
                return f"experiment {args[i + 1]} is no longer in the experiment table"
            i += 2
            continue
        if not x.startswith("-") and not (cmd[0] == "add" and "=" in x):
            t = hub.get(x)
            if t is None and cmd[0] not in ("add",):
                return f"{x} is no longer in the hub queue"
            if t is not None and t.get("exp") and (E.experiment(t["exp"]) is None or x not in wanted):
                return f"{x} (experiment {t['exp']}) is no longer in the experiment table"
        i += 1
    return None


class Round:
    def __init__(self, s, dry):
        self.s, self.dry, self.now, self.lines, self.edits = s, dry, time.time(), [], 0
        self.hub = _hub()
        self._words = None

    def words(self):
        if self._words is None:
            import hubstate as HS
            import taskstate as TS
            q = ((self.s or {}).get("taskq") or {}).get(taskq.queue_host())
            if not q:
                q = taskq.local_status()
            db = D.connect(ro=True)
            w = TS.compute(q.get("tasks") or [], HS.sites(), events.stored(db) if db is not None else set(), datetime.now())
            name = {v: k for k, v in proto.TASK_WORDS.items()}
            self._words = {k: dict(t, word=name.get(t["word"], t["word"])) for k, t in w.items()}
        return self._words

    def word(self, key):
        return (self.words().get(key) or {}).get("word")

    def failed_since(self, key, t):
        db = D.connect(ro=True)
        return bool(db is not None and events.query(db, since=t, key=key, kind=proto.EV_FAILED))

    def act(self, what, fn):
        if self.dry:
            self.lines.append(f"would {what}")
            return None
        self.edits += 1
        return fn()


def undo(it, rd=None):
    """Undo what an intent left behind when it ends without finishing: a stop it made (move), the stop of the task that made room (now)."""
    a = it["args"]
    k = None
    if it["kind"] == "move" and a.get("stopped") and _ours(it["key"], f"taskq move to {a['to']}"):
        k = it["key"]
    elif it["kind"] == "now" and a.get("victim") and _ours(a["victim"], NOW_WHY.format(it["key"])):
        k = a["victim"]
    if k:
        if rd is not None:
            rd.act(f"resume {k}", lambda: taskq.run(["resume", k]))
        else:
            taskq.run(["resume", k])


def step_rm(it, rd):
    k = it["key"]
    if k not in rd.hub:
        return "done", "no longer in the hub queue"
    if _claim(k):
        if not taskq.stopped_info(k):
            return "dropped", "resumed meanwhile; not removed"
        return None, None
    r = rd.act(f"remove {k} (released)", lambda: taskq.run(["rm", k]))
    if r is None:
        return None, None
    rc, out, err = r
    return ("done", "released, removed") if rc == 0 else (None, f"released, not removed yet: {(err or out).strip()[:200]}")


def step_move(it, rd):
    k, a = it["key"], it["args"]
    to = a["to"]
    if k not in rd.hub:
        return "failed", "no longer in the hub queue"
    ci = _claim(k)
    if ci.get("assigned") == to:
        return "done", f"sent to {to}"
    if ci:
        if taskq.stopped_info(k):
            return None, None
        if a.get("stopped"):
            return "dropped", "resumed meanwhile, before it was released"
        rd.act(f"stop {k} on {_where(ci)}", lambda: _stop([k], f"taskq move to {to}", push=False))
        a["stopped"] = True
        return None, f"stopping it on {_where(ci)}; sent only to {to} once released"
    if a.get("stopped") and _ours(k, f"taskq move to {to}"):
        rd.act(f"resume {k}", lambda: taskq.run(["resume", k]))
        a["stopped"] = False
        return None, f"released; sent only to {to}"
    return None, None


def victim(rd, key, site):
    """The task to stop on site so that key can start there: running, same kind, worse nice, not a smoke, not making room or being
    made room for itself; no progress first, then the worst nice, then the least done -> key or None."""
    import refill
    t = rd.hub.get(key)
    sp = taskq.spec_of(t) if t else None
    if sp is None:
        return None
    busy = {x["key"] for x in rows() if x["kind"] == "now"} | {x["args"].get("victim") for x in rows() if x["kind"] == "now"}
    cand = []
    for k, w in rd.words().items():
        h = rd.hub.get(k)
        if w["word"] not in ("running", "overdue") or w.get("where") != site or not h or k in busy or proto.is_smoke(k):
            continue
        hs = taskq.spec_of(h)
        if hs is None or refill.kind_of(hs) != refill.kind_of(sp) or h["nice"] <= proto.NOW_NICE or h["nice"] <= t["nice"]:
            continue
        cand.append(((w.get("frac") or 0) > 0 or (w.get("epochs") or 0) > 0, -h["nice"], w.get("frac") or 0, k))
    return min(cand)[-1] if cand else None


def step_now(it, rd):
    k, a = it["key"], it["args"]
    w = rd.word(k)
    v = a.get("victim")
    if w in proto.WAIT_UNTIL["running"]:
        if v:
            undo(it, rd)
        return "done", "running" + (f"; {v} resumed" if v else "")
    if k not in rd.hub or w in ("failed", "bad"):
        if v:
            undo(it, rd)
        return "failed", (w and proto.TASK_WORDS.get(w, w)) or "no longer in the hub queue"
    if a.get("no_preempt") or v or w != "waiting" or rd.now - it["t"] < proto.NOW_WAIT:
        return None, None
    site = rd.words()[k].get("where") or _claim(k).get("assigned")
    v = victim(rd, k, site) if site else None
    if v is None:
        return None, f"waiting on {site}; no lower-priority task of its kind runs there to make room"
    rd.act(f"stop {v} on {site} to make room for {k}", lambda: _stop([v], NOW_WHY.format(k), push=False))
    a["victim"], a["site"] = v, site
    return None, f"stopped {v} on {site} to make room; it resumes once {k} runs"


def step_after(it, rd):
    k, a = it["key"], it["args"]
    w = rd.word(k)
    hit = w in proto.WAIT_UNTIL[a["when"]] or (a["when"] == "failed" and rd.failed_since(k, it["t"]))
    if not hit:
        if w in proto.WAIT_SETTLED:
            return "failed", f"it is {proto.TASK_WORDS.get(w, w)}, not {a['when']}; taskq {shlex.join(a['cmd'])} not run"
        if k not in rd.hub and w != "done":
            return "failed", f"removed from the hub queue; taskq {shlex.join(a['cmd'])} not run"
        return None, None
    bad = recheck(a["cmd"], rd.hub)
    if bad:
        return "failed", f"{a['when']}, but not run: {bad}"
    r = rd.act(f"run taskq {shlex.join(a['cmd'])}", lambda: run_cmd(a["cmd"], it))
    if r is None:
        return None, None
    rc, out = r
    return ("done" if rc == 0 else "failed"), f"{a['when']}: ran taskq {shlex.join(a['cmd'])} (exit {rc}){': ' + out[-300:] if out else ''}"


def run_cmd(cmd, it):
    try:
        p = subprocess.run([sys.executable, BIN, "--project", cfg.REPO, *cmd], capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        return 124, "timed out after 10 min"
    return p.returncode, " ".join((p.stdout + p.stderr).split())


STEPS = dict(rm=step_rm, move=step_move, now=step_now, after=step_after)


def run(s=None, dry=False, kinds=None):
    """One pass over the active intents (health.py, before sending and, with kinds=("rm",), after it) -> (lines for taskq plan,
    whether the hub queue changed, problems {id: text})."""
    db = D.connect(ro=dry, create=not dry)
    its = [it for it in rows(db) if not kinds or it["kind"] in kinds] if db is not None else []
    if not its:
        return [], False, {}
    rd, stuck = Round(s, dry), {}
    for it in its:
        n0 = len(rd.lines)
        try:
            state, note = STEPS[it["kind"]](it, rd)
            if state is None and rd.now > (it["deadline"] or rd.now + 1):
                undo(it, rd)
                state, note = "expired", f"gave up after {(rd.now - it['t']) / 3600:.0f} h: {it['note']}"
        except Exception as e:  # noqa: BLE001  one intent's error stays with it
            state, note = None, None
            stuck[proto.alert_id("intent", it["id"])] = f"taskq {it['kind']} {it['key']}: its step failed this round ({type(e).__name__}: {e})"[:300]
        if dry:
            rd.lines[n0:] = [f"{it['kind']} {it['key']}: {x}" for x in rd.lines[n0:]]
            if state or note:
                rd.lines.append(f"{it['kind']} {it['key']}: {state or 'active'}: {note or it['note']}")
            continue
        if state or note:
            save(db, it, state, note, level="warn" if state in ("failed", "expired") else None)
        if state in ("failed", "expired"):
            stuck[proto.alert_id("intent", it["id"])] = f"taskq {it['kind']} {it['key']} {state}: {it['note']} (taskq intents --all)"[:400]
    return rd.lines, rd.edits > 0, stuck


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("rm", help="remove tasks; one in use is stopped now and removed once released")
    p.add_argument("keys", nargs="+")
    p = sub.add_parser("move", help="send a task to one executor (stops it where it is first)")
    p.add_argument("key")
    p.add_argument("--to", required=True, metavar="EXECUTOR")
    p.add_argument("--restart", action="store_true", help="start over there even though it is half-done elsewhere")
    p = sub.add_parser("now", help=f"put a task first in line (nice {proto.NOW_NICE}); after {proto.NOW_WAIT // 60} min waiting on an executor, "
                                   "stop one lower-priority task of its kind there and resume that once this one runs")
    p.add_argument("key")
    p.add_argument("--on", metavar="EXECUTOR", help="only on this executor")
    p.add_argument("--no-preempt", action="store_true", help="only change the order; stop nothing")
    p = sub.add_parser("after", help="run one taskq command once a task is running, done or failed")
    p.add_argument("key")
    p.add_argument("when", choices=("running", "done", "failed"))
    p.add_argument("--within", type=seconds, default=proto.AFTER_KEEP, help="give up after this long (default 7d)")
    p.add_argument("then", nargs="*", metavar="cmd", help=f"after --: one of {', '.join(proto.AFTER_CMDS)} with its arguments")
    p = sub.add_parser("cancel", help="remove an experiment's tasks from the hub queue (those in use: stop, remove once released)")
    p.add_argument("--exp", required=True)
    p.add_argument("--only", action="append", default=[], metavar="PARAM=VALUE[,VALUE...]")
    p.add_argument("--hold", action="store_true", help="also hold their downstream steps (taskq switch hold)")
    p.add_argument("-n", "--dry-run", action="store_true")
    p = sub.add_parser("intents", help="operations under way")
    p.add_argument("--all", action="store_true", help="also finished ones")
    p.add_argument("--json", action="store_true")
    p.add_argument("--drop", type=int, metavar="ID", help="give one up (a stop it made is undone)")
    rest = None
    if argv[:1] == ["after"] and "--" in argv:
        i = argv.index("--")
        argv, rest = argv[:i], argv[i + 1:]
    a = ap.parse_args(argv)
    if rest is not None:
        a.then = a.then + rest
    return dict(rm=cmd_rm, move=cmd_move, now=cmd_now, after=cmd_after, cancel=cmd_cancel, intents=cmd_list)[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
