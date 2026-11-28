"""Day-to-day switches in the hub database (table switch): each overrides a policy.py default until it is cleared or expires.

    taskq switch list [--json]
    taskq switch set <name> <target> <value> [--for 4h | --until "YYYY-MM-DD HH:MM"] [--why TEXT]
    taskq switch clear <name> [<target>]
    taskq drain <machine>[/<worker>] [--soft] [--for | --until] [--why]        taskq undrain <machine>[/<worker>]
    taskq pause <machine> [--for | --until] [--why]                             taskq unpause <machine>
    taskq queue on | off [--for | --until] [--why]

Names, targets and values: proto.SWITCHES. Every change is one transaction with its event (source "switch"); an expired switch
stops counting at once and the health check removes it with an event. Reading never raises: no hub database (an executor, a
test without TASKQ_DB) means the policy.py defaults. Executors get their part (drain, GPU pad) from relay.push_switches."""
import argparse
import getpass
import json
import os
import socket
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import proto  # noqa: E402
import db as D  # noqa: E402
import events  # noqa: E402

COLS = proto.SWITCH_FIELDS
PUSHED = "switches_pushed"  # meta key prefix: what relay.push_switches last wrote on each executor
_cache = [0.0, None]
TTL = 2


def _off():
    return bool((os.environ.get("TASKQ_DIR") or os.environ.get("TASKQ_ROOT")) and not os.environ.get("TASKQ_DB"))


def all_rows():
    """Every stored switch, expired ones too; no hub database → []."""
    if _off():
        return []
    now = time.time()
    if _cache[1] is not None and now - _cache[0] < TTL:
        return _cache[1]
    try:
        c = D.connect(ro=True)
    except Exception:  # noqa: BLE001
        return []
    try:
        rs = [dict(zip(COLS, r)) for r in c.execute(f"SELECT {','.join(COLS)} FROM switch ORDER BY name, target")] \
            if c is not None and D.has(c, "switch") else []
    except Exception:  # noqa: BLE001
        rs = []
    finally:
        if c is not None:
            c.close()
    _cache[:] = [now, rs]
    return rs


def live(r, now=None):
    return not r["until"] or r["until"] > (time.time() if now is None else now)


def rows(now=None):
    """Switches in force now."""
    return [r for r in all_rows() if live(r, now)]


def get(name, target="", now=None):
    return next((r for r in rows(now) if r["name"] == name and r["target"] == target), None)


def when(t):
    return time.strftime("%m-%d %H:%M", time.localtime(t))


def why(r):
    """One line for messages: its reason (or the command that set it), and until when."""
    return (r["why"] or f"taskq switch {r['name']}") + (f", until {when(r['until'])}" if r["until"] else "")


# ---------------- what callers ask ----------------
def paused(machine, now=None):
    r = get("pause", machine, now)
    return why(r) if r else None


def drained(machine, worker=None, now=None):
    """The drain switch on machine/worker, else on the whole machine; none → None."""
    return (get("drain", f"{machine}/{worker}", now) if worker else None) or get("drain", machine, now)


def machine_drained(machine, workers=(), now=None):
    """The whole machine is drained, or each of its workers (names) is → the first of those switches; else None."""
    r = get("drain", machine, now)
    if r or not workers:
        return r
    rs = [get("drain", f"{machine}/{w}", now) for w in workers]
    return rs[0] if all(rs) else None


def auto(part, default):
    r = get("auto", part)
    return default if r is None else r["value"] == "on"


def ready(machine, default):
    r = get("ready", machine)
    return default if r is None else int(r["value"])


def holds():
    return [r["target"] for r in rows() if r["name"] == "hold"]


def queue_off():
    return get("queue", "")


def gpu_pad_off(machine):
    return get("gpu_pad", machine) is not None


def site_doc(machine, worker_ids, now=None):
    """What machine's workers read in <root>/ROOT["overrides"]: {"drain": {worker id or "*": {value, until, why}}}. worker_ids:
    {worker name: worker id} (mtab.workers)."""
    d = {}
    for r in rows(now):
        m, _, w = r["target"].partition("/")
        if r["name"] == "drain" and m == machine and (not w or w in worker_ids):
            d.setdefault("drain", {})[worker_ids[w] if w else "*"] = dict(value=r["value"], until=r["until"], why=r["why"])
    return d


# ---------------- changes ----------------
def check(name, target, value):
    """→ the value as stored; a bad name, target or value raises ValueError."""
    if name not in proto.SWITCHES:
        raise ValueError(f"no switch {name!r} (switches: {', '.join(proto.SWITCHES)})")
    tdesc, values, _ = proto.SWITCHES[name]
    if values == "int":
        try:
            value = str(int(value))
        except (TypeError, ValueError):
            raise ValueError(f"{name}: the value is a whole number, not {value!r}") from None
    elif value not in values:
        raise ValueError(f"{name}: the value is one of {', '.join(values)}, not {value!r}")
    if not tdesc and target:
        raise ValueError(f"{name} has no target")
    if tdesc and not target:
        raise ValueError(f"{name} needs a target ({tdesc})")
    import cfg
    M = cfg.mtab()
    m, _, w = target.partition("/")
    if tdesc.startswith("machine") or tdesc == "executor":
        if m not in M.MACHINES:
            raise ValueError(f"no machine {m!r} (machines: {', '.join(M.MACHINES)})")
        if tdesc == "executor" and not M.is_exec(m):
            raise ValueError(f"{m} is not an executor")
        if w and "/" not in tdesc:
            raise ValueError(f"{name} takes a whole machine, not {target!r}")
        if w and w not in [x["name"] for x in M.workers(m)]:
            raise ValueError(f"{m} has no worker {w!r} (workers: {', '.join(x['name'] for x in M.workers(m)) or 'none'})")
    if name == "auto" and target not in proto.AUTO_SWITCHES:
        raise ValueError(f"no automatic part {target!r} (parts: {', '.join(proto.AUTO_SWITCHES)})")
    return value


def by():
    return f"{getpass.getuser()}@{socket.gethostname().split('.')[0]}"


def put(name, target, value, until=None, why_="", who=None, now=None):
    """Set a switch (replacing the same name and target) and log it in the same transaction → the stored row."""
    value = check(name, target, value)
    now = time.time() if now is None else now
    if until is not None and until <= now:
        raise ValueError(f"the end time {when(until)} has already passed")
    r = dict(name=name, target=target, value=value, t=now, until=until, why=why_ or "", by=who or by())
    c = D.connect(create=True)
    if c is None:
        raise OSError(f"cannot open the hub database {D.path()}")
    try:
        with D.tx(c):
            c.execute(f"INSERT INTO switch({','.join(COLS)}) VALUES({','.join('?' * len(COLS))}) ON CONFLICT(name, target) DO UPDATE SET "
                      + ", ".join(f"{k}=excluded.{k}" for k in COLS[2:]), tuple(r[k] for k in COLS))
            events.add_many([events._row("switch", f"{name} {target} {value}".replace("  ", " ") + f" ({why(r)}; by {r['by']})",
                                         kind="set", site=target.partition("/")[0] or None, data=r, t=now)], db=c)
    finally:
        c.close()
    _cache[1] = None
    return r


def drop(name, target="", reason="cleared", who=None, now=None):
    """Remove a switch and log it → the removed row, or None when there was none."""
    c = D.connect(create=True)
    if c is None:
        raise OSError(f"cannot open the hub database {D.path()}")
    try:
        with D.tx(c):
            got = c.execute(f"SELECT {','.join(COLS)} FROM switch WHERE name=? AND target=?", (name, target)).fetchone()
            if got is None:
                return None
            r = dict(zip(COLS, got))
            c.execute("DELETE FROM switch WHERE name=? AND target=?", (name, target))
            events.add_many([events._row("switch", f"{name} {target} {reason}".replace("  ", " ") + f" (was {r['value']}; by {who or by()})",
                                         kind=reason, site=target.partition("/")[0] or None, data=r,
                                         t=time.time() if now is None else now)], db=c)
    finally:
        c.close()
    _cache[1] = None
    return r


def prune(now=None):
    """Remove switches whose time is up, each with an event (health check) → messages."""
    now = time.time() if now is None else now
    out = []
    for r in all_rows():
        if not live(r, now) and drop(r["name"], r["target"], reason="expired", who="health check", now=now):
            out.append(f"switch {r['name']} {r['target']} {r['value']} ended (set until {when(r['until'])})".replace("  ", " "))
    return out


def pushed(machine):
    c = D.connect(ro=True) if not _off() else None
    try:
        return D.get_meta(c, f"{PUSHED}:{machine}") if c is not None else None
    finally:
        if c is not None:
            c.close()


def mark_pushed(machine, doc):
    c = D.connect(create=True)
    try:
        D.set_meta(c, f"{PUSHED}:{machine}", doc)
    finally:
        c.close()


# ---------------- command line ----------------
def end_time(a, now=None):
    """--for 4h / 30m / 2d, or --until "YYYY-MM-DD HH:MM" / "HH:MM" (the next such time) → epoch seconds or None."""
    now = time.time() if now is None else now
    if a.for_:
        t = events.ahead(a.for_, now)
        if t <= now:
            raise ValueError(f"--for {a.for_}: give a length such as 30m, 4h, 2d")
        return t
    if a.until:
        s = a.until.strip()
        if len(s) <= 5 and ":" in s:
            h, m = map(int, s.split(":"))
            lt = time.localtime(now)
            t = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, h, m, 0, 0, 0, -1))
            return t + 86400 if t <= now else t
        return events.ahead(s, now)
    return None


def timing(p):
    g = p.add_mutually_exclusive_group()
    g.add_argument("--for", dest="for_", metavar="LENGTH", help="for this long (30m, 4h, 2d); then it ends by itself")
    g.add_argument("--until", metavar="TIME", help='until this time ("YYYY-MM-DD HH:MM", or "HH:MM": the next one); then it ends by itself')
    p.add_argument("--why", default="", help="reason, shown wherever the switch is reported")


def show(rs, now=None):
    if not rs:
        return ["no switches set (policy.py defaults apply)"]
    return [f"{r['name']:8} {r['target'] or '-':22} {r['value']:5} {why(r)}; set {when(r['t'])} by {r['by']}" for r in rs]


def push_now(name):
    """After a drain or undrain: bring that machine's copy up to date now, as the next health check would → lines."""
    try:
        import relay
        msgs, stuck = relay.push_switches(name, None, False, now=True)
        return msgs + list(stuck.values())
    except Exception as e:  # noqa: BLE001
        return [f"could not reach {name} now ({type(e).__name__}: {e}); the next health check does it"]


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq switch" if argv[:1] in (["list"], ["set"], ["clear"]) else "taskq",
                                 description="day-to-day switches that override policy.py until they end")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("list", help="switches in force")
    p.add_argument("--json", action="store_true")
    p = sub.add_parser("set", help="set a switch: " + "; ".join(f"{k} <{v[0] or '-'}> {v[1] if v[1] == 'int' else '|'.join(v[1])}"
                                                                   for k, v in proto.SWITCHES.items()))
    p.add_argument("name", choices=list(proto.SWITCHES))
    p.add_argument("target", help="what it applies to ('-' for none)")
    p.add_argument("value")
    timing(p)
    p = sub.add_parser("clear", help="remove a switch (back to the policy.py default)")
    p.add_argument("name", choices=list(proto.SWITCHES))
    p.add_argument("target", nargs="?", default="")
    for c, h in (("drain", proto.SWITCHES["drain"][2]), ("pause", proto.SWITCHES["pause"][2])):
        p = sub.add_parser(c, help=h)
        p.add_argument("target", help="machine" + ("[/worker]" if c == "drain" else ""))
        if c == "drain":
            p.add_argument("--soft", action="store_true", help="let running tasks finish; claim nothing new")
        timing(p)
        p = sub.add_parser("un" + c, help=f"end a {c}")
        p.add_argument("target")
    p = sub.add_parser("queue", help="the hub queue's master switch: on | off")
    p.add_argument("state", choices=("on", "off"))
    timing(p)
    a = ap.parse_args(argv)
    try:
        if a.cmd == "list":
            rs = rows()
            if a.json:
                print(proto.JSON_MARK + json.dumps(dict(version=proto.VERSION, switches=rs), ensure_ascii=False))
            else:
                print("\n".join(show(rs)))
            return 0
        if a.cmd == "clear":
            r = drop(a.name, "" if a.target == "-" else a.target)
            print(f"cleared {a.name} {a.target}".strip() if r else f"{a.name} {a.target}: not set".replace("  ", " "))
            if r and a.name in ("drain", "gpu_pad"):
                print("\n".join(push_now(a.target.partition("/")[0])))
            return 0 if r else 1
        if a.cmd in ("unpause", "undrain"):
            r = drop(a.cmd[2:], a.target)
            print(f"{a.cmd[2:]} {a.target} ended" if r else f"{a.target} is not {a.cmd[2:]}ed")
            if r and a.cmd == "undrain":
                print("\n".join(push_now(a.target.partition("/")[0])))
            return 0 if r else 1
        until = end_time(a)
        if a.cmd == "queue":
            if a.state == "on":
                r = drop("queue", "", who=None)
                print("the hub queue is on" + ("" if r else " (it was not off)"))
                return 0
            r = put("queue", "", "off", until, a.why)
        elif a.cmd == "set":
            r = put(a.name, "" if a.target == "-" else a.target, a.value, until, a.why)
        elif a.cmd == "pause":
            r = put("pause", a.target, "on", until, a.why)
        else:
            r = put("drain", a.target, "soft" if a.soft else "on", until, a.why)
        print(show([r])[0])
        if r["name"] in ("drain", "gpu_pad"):
            print("\n".join(push_now(r["target"].partition("/")[0])))
        return 0
    except (ValueError, OSError) as e:
        print(f"taskq {a.cmd}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
