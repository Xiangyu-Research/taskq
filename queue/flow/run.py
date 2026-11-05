"""taskq run：hub 上的一步由它包一层再交给 pueue（2026-11-05，P3，inplace 方式）。
  taskq run <实例> [--attempt N] -- <命令>
用 bash -euo pipefail 跑命令；跑前跑后各 stat 一遍声明的产出（目录走到底）和它们的上级（只列一层）：差别就是这次写了的文件，
写到声明以外的记成台账事件 undeclared（健康检查报）；每 RUN_BEAT 秒写心跳（进程树的 CPU 时间、产出总大小）到
<状态文件夹>/runs/<实例>.json，健康检查据此判断卡住（flow/stall.py）；结束后按 done 的判断在台账里记 ok 或 failed。
退出码就是命令的退出码（判断没过时命令成功也照旧是 0：和不包时一样，由 advance 报"成功了结果还不全"）。
实例不在流程里（实验表改了、手动的标签）时照常跑命令，只是不记台账。"""
import argparse
import fnmatch
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import cfg  # noqa: E402
import proto as PR  # noqa: E402

MAX_ENTRIES = 200_000


def safe(iid):
    return iid.replace("/", "%").replace(":", "@")


def beat_path(iid):
    return cfg.state(PR.RUNS, safe(iid) + ".json")


def scan(root, outs, parents):
    """→ {相对路径: (大小, mtime_ns)}：outs 走到底，parents 只列一层。"""
    got = {}

    def put(p):
        try:
            st = os.lstat(p)
        except OSError:
            return
        got[os.path.relpath(p, root)] = (st.st_size, st.st_mtime_ns)

    for rel in outs:
        p = os.path.join(root, rel)
        if os.path.isdir(p):
            for d, _, fs in os.walk(p):
                for f in fs:
                    put(os.path.join(d, f))
                    if len(got) > MAX_ENTRIES:
                        return got
        else:
            put(p)
    for rel in parents:
        p = os.path.join(root, rel)
        try:
            names = os.listdir(p)
        except OSError:
            continue
        for n in names:
            q = os.path.join(p, n)
            if not os.path.isdir(q):
                put(q)
            else:
                got.setdefault(os.path.relpath(q, root), (-1, 0))  # 目录只记在不在
    return got


def under(rel, roots):
    return any(rel == r or rel.startswith(r.rstrip("/") + "/") for r in roots)


def declared(i):
    return list(i.outputs) + list(i.mutates)


def changes(before, after):
    return sorted(p for p, v in after.items() if before.get(p) != v)


def undeclared(i, files, graph=None):
    """写了、但不在声明的产出（含共用、会改的）里的文件；别的实例的产出不算（可能是同时在跑的它写的）。"""
    ok = declared(i)
    out = [f for f in files if not under(f, ok) and not any(fnmatch.fnmatch(f, g) for g in i.shared)]
    return [f for f in out if graph is None or graph.who(f)[0] is None]


def tree_cpu(pid):
    """pid 和它所有子孙进程的 CPU 时间（秒）。"""
    kids, cpu = {}, {}
    tick = os.sysconf("SC_CLK_TCK")
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            f = open(f"/proc/{d}/stat").read()
        except OSError:
            continue
        rest = f[f.rfind(")") + 2:].split()
        kids.setdefault(int(rest[1]), []).append(int(d))
        cpu[int(d)] = (int(rest[11]) + int(rest[12]) + int(rest[13]) + int(rest[14])) / tick
    total, todo = 0.0, [pid]
    while todo:
        p = todo.pop()
        total += cpu.get(p, 0)
        todo += kids.get(p, [])
    return total


def out_bytes(root, outs):
    n = 0
    for rel in outs:
        p = os.path.join(root, rel)
        if os.path.isdir(p):
            for d, _, fs in os.walk(p):
                for f in fs:
                    try:
                        n += os.lstat(os.path.join(d, f)).st_size
                    except OSError:
                        pass
        else:
            try:
                n += os.lstat(p).st_size
            except OSError:
                pass
    return n


class Beat(threading.Thread):
    def __init__(self, path, info, root, outs, every):
        super().__init__(daemon=True)
        self.path, self.info, self.root, self.outs, self.every = path, info, root, outs, every
        self.stop = threading.Event()

    def write(self):
        now = time.time()
        cpu = tree_cpu(self.info["pid"])
        out = out_bytes(self.root, self.outs)
        i = self.info
        if cpu > i.get("cpu", -1) + 1:
            i["cpu"], i["cpu_changed"] = cpu, now
        if out != i.get("out"):
            i["out"], i["out_changed"] = out, now
        i["beat"] = now
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(i, f)
        os.replace(tmp, self.path)

    def run(self):
        while not self.stop.wait(self.every):
            try:
                self.write()
            except Exception:  # noqa: BLE001  心跳出错不影响命令
                pass


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq run", description="包一层跑 hub 上的一步：记台账、查没声明的写、写心跳")
    ap.add_argument("inst", help="实例 id（hub 上的标签）")
    ap.add_argument("--attempt", type=int, help="第几次（只记下）")
    if "--" in argv:
        k = argv.index("--")
        argv, cmd = argv[:k], argv[k + 1:]
    else:
        cmd = []
    a = ap.parse_args(argv)
    if not cmd:
        ap.error("要给命令：taskq run <实例> [--attempt N] -- <命令>")
    cmd = cmd[0] if len(cmd) == 1 else " ".join(cmd)

    i = root = db = aid = g = None
    try:
        from flow import cli as FC
        from flow import ledger as LG
        import advance
        flow, g, root, _ = FC.flow_for_ledger()
        i = flow.instances.get(a.inst)
        if i is None:
            print(f"taskq run：{a.inst} 不在流程里，照常跑、不记台账", file=sys.stderr)
        else:
            db = LG.connect()
            aid = LG.begin(db, i, machine=socket.gethostname(), ref=os.environ.get("PUEUE_TASK_ID"),
                           code=os.getcwd(), note=f"attempt {a.attempt}" if a.attempt else None)
            py = advance.python()
    except Exception as e:  # noqa: BLE001  台账用不了也照常跑
        print(f"taskq run：台账用不了（{type(e).__name__}: {e}），照常跑", file=sys.stderr)
        i = None

    parents = sorted({os.path.dirname(o) for o in declared(i)}) if i else []
    before = scan(root, declared(i), parents) if i else {}
    p = subprocess.Popen(["bash", "-euo", "pipefail", "-c", cmd], start_new_session=True)

    def fwd(sig, _):
        try:
            os.killpg(p.pid, sig)
        except OSError:
            pass
    for sg in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sg, fwd)

    beat = None
    if i:
        os.makedirs(cfg.state(PR.RUNS), exist_ok=True)
        now = time.time()
        info = dict(inst=i.id, attempt=aid, pid=p.pid, wrapper=os.getpid(), host=socket.gethostname(), start=now, beat=now,
                    cpu=0.0, cpu_changed=now, out=out_bytes(root, i.outputs), out_changed=now, cmd=cmd[:500])
        beat = Beat(beat_path(i.id), info, root, i.outputs, float(os.environ.get("TASKQ_RUN_BEAT") or PR.RUN_BEAT))
        beat.write()
        beat.start()
    rc = p.wait()
    if beat:
        beat.stop.set()
    if not i:
        return rc
    try:
        files = changes(before, scan(root, declared(i), parents))
        bad = undeclared(i, files, g)
        if bad:
            LG.event(db, i.id, bad[0], "undeclared", "warn", dict(files=bad[:50], n=len(bad)))
            db.commit()
            print(f"taskq run：{i.id} 写到了声明的产出以外：{bad[:5]}{' …' if len(bad) > 5 else ''}", file=sys.stderr)
        st = i.step or {}
        ok = rc == 0
        if ok and st.get("done") is not None:
            ok = bool(st["done"].ok(root, dict(i.names, data=root, python=py)))
        LG.finish(db, aid, i, PR.A_OK if ok else PR.A_FAILED, root, files=[f for f in files if f not in bad],
                  note=None if ok else (f"退出码 {rc}" if rc else "命令成功，做完的判断没过"))
    except Exception as e:  # noqa: BLE001
        print(f"taskq run：记台账出错（{type(e).__name__}: {e}）", file=sys.stderr)
    finally:
        try:
            os.remove(beat_path(i.id))
        except OSError:
            pass
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
