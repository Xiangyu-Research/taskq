#!/usr/bin/env python3
"""并发和中途失败（11/3）：锁的抢占、box 的 pueue 读不到时 relay 只续约、GC 和 stage 抢 pin。
跑法：python3 scripts/taskq/queue/test_races.py [lock|relay_err_release|relay_err_fetch|gc_pin]（不带参数全跑，各条独立报告）
全部在临时目录里：HOME、TASKQ_STATE、TASKQ_DIR 指到临时目录，fixture 的配置，test_relay 的假 ssh；不连网络。"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import cfg as _cfg  # noqa: E402  调度代码的根、使用者的仓库根
TMP = tempfile.mkdtemp(prefix="test_review_conc_")
os.environ["HOME"] = os.path.join(TMP, "home")
os.makedirs(os.environ["HOME"])
os.environ["TASKQ_DIR"] = os.path.join(TMP, "tq")
os.environ["TASKQ_LOCK_WAIT"] = "0.05"  # 等锁的重试间隔
sys.path.insert(0, HERE)
import test_relay as TR  # noqa: E402  （fixture.use()、临时 TASKQ_STATE、假 ssh；只跑模块级的准备，不跑测试）
import common as C  # noqa: E402
import machines as M  # noqa: E402
import relay  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_store as ST  # noqa: E402
import relay_transfer as RT  # noqa: E402
import taskq  # noqa: E402

assert taskq.TQ == os.environ["TASKQ_DIR"], taskq.TQ


def test_lock():
    """两个等锁的进程都看到同一把死锁过期：A 抢下并拿到新锁后，B 把 A 的新锁当成旧的再抢一次 → 两个同时持锁。
    tmpfs（/tmp）不复用 inode 号，ext4 会把刚删的号马上给新目录：两种盘都跑。"""
    _lock_race(taskq.TQ)
    if os.path.isdir("/var/tmp"):
        disk = tempfile.mkdtemp(prefix="test_races_", dir="/var/tmp")
        try:
            _lock_race(disk)
        finally:
            shutil.rmtree(disk, ignore_errors=True)


def _lock_race(base):
    os.makedirs(base, exist_ok=True)
    d = os.path.join(base, "lock")
    p = subprocess.Popen(["true"])
    p.wait()
    os.mkdir(d)
    with open(os.path.join(d, "owner"), "w") as f:  # 持锁的进程被 SIGKILL：本机、pid 已经不在
        f.write(f"{socket.gethostname()}\t{p.pid}\tdeadbeef0000\n")
    old = time.time() - 200
    os.utime(d, (old, old))
    A, B = taskq.Lock(tries=4), taskq.Lock(tries=4)
    A.d = B.d = d
    orig = taskq.Lock._holder
    state = dict(done=False)

    def hook(self):
        r = orig(self)
        if self is B and not state["done"]:  # B 判断完"持有者死了"、还没 rename 时，A 抢下旧锁并拿到新锁
            state["done"] = True
            A.__enter__()
        return r

    taskq.Lock._holder = hook
    try:
        both = False
        try:
            B.__enter__()
            both = True
        except SystemExit:
            pass
        a_owner_now = open(os.path.join(d, "owner")).read() if os.path.exists(os.path.join(d, "owner")) else None
        assert not both, (f"{base}：A 持锁期间 B 也拿到了锁（A={A.me!r}，B={B.me!r}，锁里现在记的是 {a_owner_now!r}）："
                          "B 把 A 刚建的新锁当成过期的旧锁 rename 掉了")
    finally:
        taskq.Lock._holder = orig
        shutil.rmtree(d, ignore_errors=True)


def _plan_with_box_error(mt, rt):
    K = "D1_x/tr/0"
    S = TR.S0([TR.mtask(K, mt)])
    S["pueue"] = {M.hub(): {"error": "pueue 没有响应"}}  # health.gather：qs --json 失败或 box 的 pueue 读不到时就是这样
    RS = dict(taskq=dict(tasks=[TR.rtask(K, rt)], paused=False), tasks={}, free=500e9)
    old_lookup, old_down = RT.lookup, C.backup_down
    RT.lookup = lambda k: dict(archive=True)
    C.backup_down = lambda ok=None: None
    try:
        return K, relay.plan(S, TR.NAME, RS, TR.keymap(K))
    finally:
        RT.lookup, C.backup_down = old_lookup, old_down


def test_relay_err_release():
    """box 的 pueue 读不到（s["pueue"]["box"] = {"error": …}，s["ok"]["box"] 仍是 True）：要归档的折总队列显示训完 →
    应当只续约、等下次；现在当成"从来没拉回过、不用归档"，删那边的结果并交还，归档永远不排。"""
    K, (acts, stuck) = _plan_with_box_error("done", "done")
    assert "release" not in [a["op"] for a in acts], f"box 的 pueue 读不到时交还了要归档的折：{TR.ops(acts)}，{acts}"


def test_relay_err_fetch():
    """box 的 pueue 读不到：那边训完、box 上已有一个拉回在跑（读不到而已）→ 不应再排一个拉回；现在排 fetch。"""
    K, (acts, stuck) = _plan_with_box_error("claimed", "done")
    assert "fetch" not in [a["op"] for a in acts], f"box 的 pueue 读不到时又排了拉回：{TR.ops(acts)}"


def test_gc_pin():
    """GC 读完 pins 以后 stage 才写 pin（stage 在 box 的 pueue wait 组里跑，和健康检查里的 GC 并行）：
    stage 的 MISSING_PY 已认为这个 blob 在、不再传，GC 照样删掉它 → 建视图时缺 blob。"""
    R = os.path.join(TMP, "gcroot")
    h = "ab" + "0" * 62
    os.makedirs(os.path.join(R, "blobs", h[:2]))
    os.makedirs(os.path.join(R, "data", "views", "v"))
    os.makedirs(os.path.join(R, "state", "pins"))
    blob = os.path.join(R, "blobs", h[:2], h)
    with open(blob, "wb") as f:
        f.write(b"x" * 1000)
    old = time.time() - 3 * 86400
    os.utime(blob, (old, old))
    pin = os.path.join(R, "state", "pins", "stage_k.json")
    hook = (f"import os,json\n_w=os.walk\n_s=dict(n=0)\n"
            f"def _walk(*a,**k):\n"
            f"    if _s['n']==0:\n"
            f"        _s['n']=1\n"
            f"        json.dump([{h!r}],open({pin!r},'w'))\n"
            f"    return _w(*a,**k)\n"
            f"os.walk=_walk\n")
    code = hook + "exec(compile(open(__import__('sys').argv.pop(1)).read(),'GC_PY','exec'))"
    gc_file = os.path.join(TMP, "gc_py.py")
    with open(gc_file, "w") as f:
        f.write(RM.GC_PY)
    repo = _cfg.REPO
    r = subprocess.run([sys.executable, "-c", code, gc_file, R], cwd=repo, capture_output=True, text=True,
                       input=json.dumps(dict(soft_gb=1e6, keep=[], free=0, views=[], scan=["views/v"])))
    assert r.returncode == 0, r.stderr
    assert os.path.exists(blob), (f"stage 在 GC 运行中写了 pin，GC 仍删了这个 blob：{r.stdout.strip()}")


def test_central_timeout():
    import inspect
    wait = inspect.signature(taskq.Lock).parameters["tries"].default * 0.5
    t = inspect.signature(taskq.central).parameters["timeout"].default
    assert t > wait + 30, f"central() 的超时 {t} 秒不比锁的最长等待 {wait} 秒长：锁被占着时调用方拿到 TimeoutExpired，而不是退出码"
    import advance
    for f in (advance.taskq_add, advance.taskq_remove):
        src = inspect.getsource(f)
        t = eval(src.split("timeout=")[1].split(")")[0], vars(advance))
        assert t > wait + 30, f"advance.{f.__name__} 的超时 {t} 秒不比锁的最长等待 {wait} 秒长"


TESTS = dict(lock=test_lock, relay_err_release=test_relay_err_release, relay_err_fetch=test_relay_err_fetch, gc_pin=test_gc_pin,
             central_timeout=test_central_timeout)

if __name__ == "__main__":
    names = sys.argv[1:] or list(TESTS)
    bad = 0
    for n in names:
        try:
            TESTS[n]()
            print(f"通过：{n}")
        except AssertionError as e:
            bad += 1
            print(f"失败：{n}：{e}")
        except Exception:  # noqa: BLE001
            bad += 1
            print(f"出错：{n}")
            traceback.print_exc()
    shutil.rmtree(TMP, ignore_errors=True)
    shutil.rmtree(TR.T, ignore_errors=True)
    shutil.rmtree(TR.STATE_DIR, ignore_errors=True)
    sys.exit(1 if bad else 0)
