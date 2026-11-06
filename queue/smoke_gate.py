#!/usr/bin/env python3
"""冒烟闸门（11/5）：一种任务（做法 + 插件 smoke.py 里 by 列的参数 + 环境）第一次送到一台执行机之前，先在那台上跑一个小的冒烟任务，
过了才送真的。说明见 queue/README.md 的"冒烟"。

    taskq smoke status [执行机] [--json]   各台的记录
    taskq smoke retry <执行机> [种类]      没过的重新排（种类：完整的种类名或 12 位 id）
    taskq smoke adopt [--dry-run]          现在分给各台的、各台训完交还的任务的种类记成过了（已经在跑的不再冒烟）
"""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import secrets
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import proto  # noqa: E402
from proto import alert, alert_id, is_smoke  # noqa: E402
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import taskq  # noqa: E402
from common import write_json  # noqa: E402

SMOKE = {}
cfg.extend(globals(), "smoke")
FIELDS = ("by", "params", "vars")
QUEUED_ALERT_S = 6 * 3600
OK, FAILED, QUEUED = proto.SMOKE
TAG = "{smoke}"


def on():
    return bool(P.AUTO.get("smoke"))


class DefError(Exception):
    """定义里的函数出错（这种记成没过）。"""


def hub_data():
    """hub 本机的数据目录（relay 从这里读要传的输入）。"""
    return os.environ.get("TASKQ_DATA") or M.expand(M.machine(M.hub())["data"])


def real_params(sp):
    """交给冒烟定义里的函数的真任务参数：说明的 names，加上它的 data（要传的输入，展开后的；冒烟任务按需带上其中的一部分）。"""
    return dict(sp.get("names") or {}, data=list(sp.get("data") or []))


def definition(recipe, names=None):
    """做法的冒烟定义 → dict(by, params, vars)；params、vars 是函数 f(真任务的参数 real_params, hub 的数据目录) 时算出来
    （出错 → DefError）。没有定义 → None；写法不对 → ValueError。"""
    d = SMOKE.get(recipe)
    if d is None:
        return None
    if not isinstance(d, dict) or set(d) - set(FIELDS):
        raise ValueError(f"smoke.py 的 {recipe} 只能有 {'、'.join(FIELDS)}：{d!r}")
    by = d.get("by") or []
    if not isinstance(by, list) or not all(isinstance(x, str) for x in by):
        raise ValueError(f"smoke.py 的 {recipe} 的 by 要是参数名的列表")
    out = dict(by=by)
    for f in ("params", "vars"):
        v = d.get(f) or {}
        if callable(v):
            try:
                v = v(dict(names or {}), hub_data())
            except Exception as e:  # noqa: BLE001
                raise DefError(f"{f}()：{type(e).__name__}: {e}") from None
        if not isinstance(v, dict):
            raise ValueError(f"smoke.py 的 {recipe} 的 {f} 要是字典，或返回字典的函数：{v!r}")
        out[f] = dict(v)
    return out


def h12(x):
    return hashlib.sha256((x if isinstance(x, str) else json.dumps(x, sort_keys=True, ensure_ascii=False)).encode()).hexdigest()[:12]


def env_id(site, sp):
    """这台上这个说明的环境 → id：spec 条目按描述加这台用的变体的锁（和那边建好后 .ok 里写的同一个哈希，不看那边读没读到），
    conda / venv 条目按条目本身。"""
    envs = (M.machine(site).get("envs") or {})
    out = []
    for e in S.envs_of(sp):
        x = envs.get(e)
        v = M.spec_hash(e, M.spec_variant(site, e)) if M.is_spec(x) else repr(tuple(x) if isinstance(x, list) else x)
        out.append(f"{e}:{h12(str(v))}")
    return ",".join(out)


def mark(site, sp):
    """说明 → dict(kind, kid, env, defn, canary)。kind = 做法 + by 的参数值 + 冒烟任务换成的参数（不含带 {smoke} 的）+ 环境名：
    同一 by 的任务冒烟换成的数据不同（如通道不同）时是两种，各测各的、各记各的（11/6：以前算一种，两边互相改写记录、来回重排）。
    没有冒烟定义的做法是 canary。"""
    rec = sp.get("recipe")
    names = sp.get("names") or {}
    err = None
    try:
        d = definition(rec, real_params(sp))
    except DefError as e:
        err, d = str(e), None
    by = (d or {}).get("by") or (SMOKE.get(rec) or {}).get("by") or []
    swap = [f"{p}={v}" for p, v in sorted(((d or {}).get("params") or {}).items())
            if p not in by and TAG not in json.dumps(v, ensure_ascii=False)]
    kind = "|".join([str(rec)] + [f"{p}={names.get(p)}" for p in by] + swap + [f"env={','.join(S.envs_of(sp))}"])
    return dict(kind=kind, kid=h12(kind), env=env_id(site, sp), defn=h12({"error": err}) if err else h12(d) if d else "",
                canary=d is None and err is None, recipe=rec, defs=d, error=err)


# ---------------- 记录 ----------------
def path(site):
    return os.path.join(taskq.TQ, proto.SMOKE_DIR, f"{site}.json")


def load(site):
    try:
        with open(path(site)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


_HELD = set()  # 这个进程已经拿着锁的执行机（enqueue 里再记录时不重复拿：同一进程开两次 flock 会自己等自己）


@contextlib.contextmanager
def locked(site):
    if site in _HELD:
        yield
        return
    os.makedirs(os.path.dirname(path(site)), exist_ok=True)
    with open(path(site) + ".lock", "a") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        _HELD.add(site)
        try:
            yield
        finally:
            _HELD.discard(site)


def update(site, fn):
    with locked(site):
        recs = load(site)
        out = fn(recs)
        write_json(path(site), recs)
    return out


def queue_keys(mtq=None):
    """总队列里现在有的 key：这一轮开始时读的（mtq）加上现在盘上的（别的检查在这一轮开始以后加的冒烟也算，11/5）。"""
    keys = master_keys(mtq)
    if taskq.have_queue():
        keys |= {t["key"] for t in taskq.read_tasks()}
    return keys


def pending(r, m, keys):
    """这个种类已经排了冒烟、还在总队列里 → True。"""
    return fresh(r, m) and r.get("state") == QUEUED and r.get("key") in keys


def fresh(r, m):
    return bool(r) and r.get("env") == m["env"] and r.get("defn") == m["defn"]


def master_keys(mtq):
    return {t["key"] for t in (mtq or {}).get("tasks", [])}


class Gate:
    """relay.fill 用：verdict(key, sp) → None（送）或原因（这次不送）。要排的冒烟在 ops()，报警在 alerts，开关关着时的影子在 notes。"""

    def __init__(self, site, rs, mtq, live=None, now=None):
        self.site, self.mtq, self.now = site, mtq or {}, now or time.time()
        self.live = on() if live is None else live
        self.bundle = (rs or {}).get("bundle")
        self.recs = load(site)
        self.want, self.alerts, self.would, self.taken, self.fails = {}, {}, {}, {}, {}
        self._fly = self._keys = None

    def queue_keys(self):
        if self._keys is None:
            self._keys = queue_keys(self.mtq)
        return self._keys

    def inflight(self, kind):
        if self._fly is None:
            self._fly = {}
            for t in self.mtq.get("tasks", []):
                if t.get("worker") == self.site and t.get("state") != proto.S_DONE and not is_smoke(t["key"]) and t.get("spec"):
                    try:
                        k = mark(self.site, t["spec"])["kind"]
                    except ValueError:
                        continue
                    self._fly[k] = self._fly.get(k, 0) + 1
        return self._fly.get(kind, 0) + self.taken.get(kind, 0)

    def took(self, key, sp):
        if is_smoke(key) or not sp:
            return
        try:
            k = mark(self.site, sp)["kind"]
        except ValueError:
            return
        self.taken[k] = self.taken.get(k, 0) + 1

    def _hold(self, key, why):
        if self.live:
            return why
        self.would[key] = why
        return None

    @property
    def notes(self):
        """开关关着时的影子：会排哪些冒烟、会挡下几个任务。"""
        if self.live or not (self.want or self.would):
            return []
        out = [f"（冒烟开关没开）{self.site} 会排冒烟：{'、'.join(sorted(self.want)) or '无'}；会挡下 {len(self.would)} 个任务"
               + (f"（{'、'.join(sorted(self.would)[:3])}{' …' if len(self.would) > 3 else ''}）" if self.would else "")]
        return out

    def verdict(self, key, sp):
        if is_smoke(key) or not sp:
            return None
        try:
            m = mark(self.site, sp)
        except ValueError as e:
            if self.live:
                self.alerts[alert_id("smoke", self.site, h12(str(sp.get("recipe"))))] = alert(
                    f"{self.site} 的冒烟定义写错了：{e}", machine=self.site, kind=str(sp.get("recipe")), why=str(e), key=key)
            return self._hold(key, f"冒烟定义写错了：{e}")
        r = self.recs.get(m["kind"])
        if fresh(r, m) and r.get("state") == OK:
            return None
        if m["error"]:
            why = f"冒烟定义的函数出错：{m['error']}"
            if self.live:
                self.alerts[alert_id("smoke", self.site, m["kid"])] = alert(
                    f"{self.site} 上 {m['kind']} 冒烟没过：{why}", machine=self.site, kind=m["kind"], why=why, key=key)
                if not (fresh(r, m) and r.get("state") == FAILED):
                    self.fails[m["kind"]] = dict(m=m, why=why)
            return self._hold(key, why)
        if m["canary"]:
            n = self.inflight(m["kind"])
            return self._hold(key, f"{m['kind']} 在这台上还没成功过，先只送一个（已有 {n} 个）") if n else None
        if pending(r, m, self.queue_keys()):
            if self.live and self.now - (r.get("t") or self.now) > QUEUED_ALERT_S:
                self.alerts[alert_id("smoke", self.site, m["kid"])] = alert(
                    f"{self.site} 上 {m['kind']} 的冒烟 {r['key']} 排了 {(self.now - r['t']) / 3600:.0f} 小时还没做完",
                    machine=self.site, kind=m["kind"], why="还没做完", key=r["key"])
            return self._hold(key, f"等冒烟 {r.get('key')}")
        if fresh(r, m) and r.get("state") == FAILED and (not self.bundle or r.get("bundle") == self.bundle):
            if self.live:
                self.alerts[alert_id("smoke", self.site, m["kid"])] = alert(
                    f"{self.site} 上 {m['kind']} 冒烟没过：{r.get('why')}（taskq smoke retry {self.site} {m['kid']}）",
                    machine=self.site, kind=m["kind"], why=r.get("why"), key=r.get("key"))
            return self._hold(key, f"冒烟没过：{r.get('why')}")
        self.want.setdefault(m["kind"], dict(sp=sp, m=m))
        return self._hold(key, f"先排 {m['kind']} 的冒烟")

    def ops(self):
        """→ [dict(op="smoke_add", …)]：每种要冒烟的排一个，每次一个新的名字（{smoke}）。"""
        if not self.live:
            return []
        out = [dict(op="smoke_fail", key=f"{proto.SMOKE_PREFIX}{self.site}/{f['m']['kid']}", mark=f["m"], why=f["why"])
               for f in self.fails.values()]
        for kind, w in self.want.items():
            m, sp, d = w["m"], w["sp"], w["m"]["defs"]
            stamp = f"{int(self.now)}-{secrets.token_hex(2)}"
            sid, key = f"{self.site}-{m['kid']}-{stamp}", f"{proto.SMOKE_PREFIX}{self.site}/{m['kid']}-{stamp}"
            names = sp.get("names") or {}
            params = {p: names[p] for p in d["by"] if p in names}
            params.update(subst(d["params"], sid))
            if d["vars"]:
                params["vars"] = subst(d["vars"], sid)
            params.update(machines=[self.site], key=key)
            out.append(dict(op="smoke_add", key=key, recipe=m["recipe"], params=params, mark=m, envs=S.envs_of(sp),
                            bundle=self.bundle, sid=sid))
        return out


def subst(v, sid):
    if isinstance(v, str):
        return v.replace(TAG, sid)
    if isinstance(v, list):
        return [subst(x, sid) for x in v]
    if isinstance(v, dict):
        return {k: subst(x, sid) for k, x in v.items()}
    return v


def record(site, m, **kw):
    def fn(recs):
        recs[m["kind"]] = dict(kind=m["kind"], env=m["env"], defn=m["defn"], t=time.time(), **kw)
    update(site, fn)


def enqueue(site, op):
    """smoke_add → (加上了没有, 说明)。解析不了、环境和真任务不一样、加不进总队列的记成没过。
    这个种类已经有排着的冒烟（同时跑的另一次检查刚加的）→ (None, 说明)，不再加（11/5：两次检查同时跑，一轮里加了四百多个）。"""
    m = op["mark"]
    with locked(site):  # 查和加之间别的检查插不进来
        r = load(site).get(m["kind"])
        if pending(r, m, queue_keys()):
            return None, f"{site} 上 {m['kind']} 已经排了冒烟 {r['key']}，不再加"
        return _enqueue(site, op, m)


def _enqueue(site, op, m):
    try:
        sp = S.resolve(op["recipe"], op["params"])
        if S.envs_of(sp) != op["envs"]:
            raise S.SpecError(f"冒烟任务的环境 {S.envs_of(sp)} 和真任务的 {op['envs']} 不一样（把环境参数加进 by）")
    except S.SpecError as e:
        record(site, m, state=FAILED, src="smoke", key=op["key"], bundle=op.get("bundle"), why=f"解析不了：{e}"[:300])
        return False, f"{site} 上 {m['kind']} 的冒烟解析不了：{e}"
    rc, out, err = taskq.central(["add", f"--nice={proto.SMOKE_NICE}", "--recipe"] + S.kv_args(op["recipe"], op["params"]))
    if rc:
        record(site, m, state=FAILED, src="smoke", key=op["key"], bundle=op.get("bundle"), why=f"加不进去：{(out + err).strip()[-200:]}")
        return False, f"{site} 上 {m['kind']} 的冒烟加不进总队列：{(out + err).strip()[-150:]}"
    record(site, m, state=QUEUED, src="smoke", key=op["key"], bundle=op.get("bundle"), why="")
    return True, f"{site} 排了 {m['kind']} 的冒烟 {op['key']}"


def finished(site, key):
    """这个冒烟任务的记录已经有结果（ok / failed）→ True。"""
    return any(r.get("key") == key and r.get("state") in (OK, FAILED) for r in load(site).values())


def finish(site, key, ok, why=""):
    def fn(recs):
        for r in recs.values():
            if r.get("key") == key and r.get("state") == QUEUED:
                r.update(state=OK if ok else FAILED, why=(why or "")[-600:], t=time.time())
                return r["kind"]
    return update(site, fn)


def smoke_dirs(sp, sid):
    """冒烟任务在那边占的、名字里有 sid 的最上一级文件夹（相对数据目录，至少两级）：out 和 vars 里 {data}/… 的路径。"""
    cands = [sp.get("out") or ""] + [v[len("{data}/"):] for v in (sp.get("vars") or {}).values()
                                    if isinstance(v, str) and v.startswith("{data}/")]
    out = set()
    for p in cands:
        parts = p.strip("/").split("/")
        i = next((i for i, x in enumerate(parts) if sid in x), None)
        if i is not None and i >= 1:
            out.add("/".join(parts[:i + 1]))
    return sorted(out)


def sid_of(key):
    """_smoke/<执行机>/<id>-<时间> → <执行机>-<id>-<时间>（{smoke}）。"""
    return key[len(proto.SMOKE_PREFIX):].replace("/", "-", 1)


def real_done(site, m):
    """真任务在这台上训完：这种记成过了（src=real）。"""
    def fn(recs):
        r = recs.get(m["kind"])
        if fresh(r, m) and r.get("state") == OK:
            return False
        recs[m["kind"]] = dict(kind=m["kind"], env=m["env"], defn=m["defn"], t=time.time(), state=OK, src="real", key=None, why="")
        return True
    return update(site, fn)


def passed(site, key):
    """shift 交班：key 这种任务在 site 上过了冒烟（开关关着 → True）。"""
    if not on():
        return True
    if is_smoke(key):
        return False
    t = next((t for t in taskq.read_tasks() if t["key"] == key), None)
    sp = taskq.spec_of(dict(t)) if t else None
    if not sp:
        return False
    try:
        m = mark(site, sp)
    except ValueError:
        return False
    r = load(site).get(m["kind"])
    return fresh(r, m) and r.get("state") == OK


def retry(site, kind=None):
    def fn(recs):
        gone = [k for k, r in recs.items() if r.get("state") == FAILED and (kind is None or kind in (k, h12(k)))]
        for k in gone:
            recs.pop(k)
        return gone
    return update(site, fn)


def adopt(dry=False):
    """现在分给各台的、各台训完交还的任务 → 那台的这种记成过了（src=adopted）。→ [(执行机, 种类)]（新记的）。"""
    st = taskq.local_status()
    rel = os.path.join(taskq.TQ, proto.QUEUE["released"])
    got = {}
    for t in st["tasks"]:
        if is_smoke(t["key"]) or not t.get("spec"):
            continue
        site = t.get("assigned") or t.get("worker")
        if site not in M.RELAY and t["state"] == proto.S_DONE:
            try:
                with open(os.path.join(rel, proto.safe(t["key"]))) as f:
                    site = (f.read().split() + ["", ""])[1]
            except OSError:
                site = None
        if site in M.RELAY:
            m = mark(site, t["spec"])
            got.setdefault(site, {})[m["kind"]] = m
    new = []
    for site, ms in got.items():
        def fn(recs, ms=ms):
            out = []
            for k, m in ms.items():
                if fresh(recs.get(k), m) and recs[k].get("state") == OK:
                    continue
                out.append(k)
                if not dry:
                    recs[k] = dict(kind=k, env=m["env"], defn=m["defn"], t=time.time(), state=OK, src="adopted", key=None, why="")
            return out
        new += [(site, k) for k in (update(site, fn) if not dry else fn(dict(load(site))))]
    return new


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq smoke", description="冒烟闸门的记录")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("status", help="各台的记录")
    p.add_argument("site", nargs="?")
    p.add_argument("--json", action="store_true")
    p = sub.add_parser("retry", help="没过的重新排")
    p.add_argument("site")
    p.add_argument("kind", nargs="?")
    p = sub.add_parser("adopt", help="分给各台的、训完交还的任务的种类记成过了")
    p.add_argument("-n", "--dry-run", action="store_true")
    a = ap.parse_args(argv)
    if getattr(a, "site", None) and a.site not in M.RELAY:
        print(f"没有执行机 {a.site}（有：{'、'.join(M.RELAY)}）", file=sys.stderr)
        return proto.EXIT["gone"]
    if a.cmd == "status":
        allr = {s: load(s) for s in ([a.site] if a.site else M.RELAY)}
        if a.json:
            S.json_out(dict(version=proto.VERSION, on=on(), sites=allr))
            return 0
        print(f"冒烟闸门{'开着' if on() else '关着（只记进 relay.log）'}")
        for s, recs in allr.items():
            print(f"{s}：{len(recs)} 种")
            for k, r in sorted(recs.items()):
                when = time.strftime("%m-%d %H:%M", time.localtime(r.get("t") or 0))
                print(f"  {h12(k)} {r.get('state'):<7} {r.get('src'):<8} {when}  {k}" + (f"  {r['why']}" if r.get("why") else ""))
        return 0
    if a.cmd == "retry":
        gone = retry(a.site, a.kind)
        print(f"{a.site}：{len(gone)} 种没过的去掉了记录，下次检查重新排冒烟" + (f"（{'、'.join(gone)}）" if gone else ""))
        return 0
    new = adopt(a.dry_run)
    print(("会记" if a.dry_run else "记了") + f" {len(new)} 种" + "".join(f"\n  {s}：{k}" for s, k in new))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
