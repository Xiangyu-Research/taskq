#!/usr/bin/env python3
import os
import sys
import tempfile
import contextlib
import io
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
os.environ["TASKQ_STATE"] = tempfile.mkdtemp()  # 总开关（DISABLED）的默认位置不指到本机真的状态文件夹
sys.path.insert(0, HERE)
import gpu_pad as G  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


class FakeChunk:

    def __init__(self, backend, dev, mib):
        self.b, self.dev, self.mib = backend, dev, mib
        backend.live[dev] += mib

    def __del__(self):
        self.b.live[self.dev] -= self.mib
        self.b.cache[self.dev] += self.mib


class FakeBackend:
    def __init__(self, total=81920, devices=1, training=9000, fallback=False):
        self.total = {d: total for d in range(devices)}
        self.training = {d: training for d in range(devices)}
        self.live = {d: 0 for d in range(devices)}
        self.cache = {d: 0 for d in range(devices)}
        self.fallback = fallback
        self.fail_alloc = set()
        self.empty_calls = 0
        self.mem_calls = 0

    def device_count(self):
        return len(self.total)

    def device_ids(self):
        return list(self.total)

    def mem(self, dev):
        self.mem_calls += 1
        return self.training[dev] + self.live[dev] + self.cache[dev], self.total[dev], self.fallback

    def pad_mib(self, dev):
        return self.live[dev] + self.cache[dev]

    def alloc(self, dev, mib):
        if dev in self.fail_alloc:
            raise RuntimeError("fake OOM")
        return FakeChunk(self, dev, mib)

    def empty_cache(self, dev):
        self.empty_calls += 1
        self.cache[dev] = 0

    def used(self, dev=0):
        return self.mem(dev)[0]


def cfg(**kw):
    base = dict(floor_mib=42000, target_mib=55000, high_mib=62000, chunk_mib=1024, interval=0,
                settle=0, start_hold=900, min_free_mib=8000, min_total_mib=60000, release_hysteresis_mib=2048,
                min_train_mib=2000, base="", switch="/nonexistent/gpu_pad_switch", parent_pid=None,
                supervisor_pid=None, dry_run=False, iterations=0, device=None)
    base.update(kw)
    return G.Config(**base)


def pad_of(b, **kw):
    logs = []
    return G.Pad(b, cfg(**kw), logger=logs.append), logs


def filled(used, target=55000):
    return target <= used < target + 1024


def at_floor(used, floor=42000):
    return floor <= used < floor + 1024


b = FakeBackend()
p, logs = pad_of(b)
p.step()
ok("补到 55000 MiB 以上、不到一块的余量，都是整块", filled(b.used()) and b.live[0] == 45 * 1024)
ok("日志里有启动和补显存", any("启动" in x for x in logs) and any("补 " in x for x in logs))

b = FakeBackend(training=3000)
p, _ = pad_of(b, floor_mib=5500, target_mib=5500, high_mib=9000, chunk_mib=1024)
p.step()
ok("只补整块（差 2500 补 3 块）", b.live[0] == 3 * 1024)

b = FakeBackend()
p, _ = pad_of(b)
p.step()
b.training[0] = 39000
p.step()
ok("used > high 时释放到 high - hysteresis 以下", b.used() <= 62000 - 2048 and b.live[0] > 0)

b = FakeBackend(training=70000)
p, _ = pad_of(b)
p.step()
ok("训练自己超过 high 时 pad 不动", b.live[0] == 0)

b = FakeBackend(training=0)
p, logs = pad_of(b)
p.step()
ok("作业开始、还没有训练：先填到 floor（11/4）", at_floor(b.used()) and any("没有在训的" in x for x in logs))
b.training[0] = 9000
p.step()
ok("训练来了：补到 55GB", filled(b.used()))
b.training[0] = 500
p.step()
ok("训练没了：不放掉，显存不低于 floor", b.live[0] == 45 * 1024 and b.used() >= 42000)
b.training[0] = 0
b.live[0] -= 0
p._release(0, 0, reason="测试")
p.step()
ok("训练过、现在没有在训的：马上补回 floor（不补到 target）", at_floor(b.used()))

b = FakeBackend(training=0)
p, logs = pad_of(b, settle=100)
t = [1000.0]
G.time.time = lambda: t[0]
p.changed[0] = t[0]
b.training[0] = 5000; p.step()
ok("训练刚开始：马上补到 floor，不到 target", at_floor(b.used()))
lv = b.live[0]
t[0] += 60; b.training[0] = 20000; p.step()
ok("60 秒后显存还在涨：不再补", b.live[0] == lv and any("等稳定" in x for x in [p._decide(0, b.used(), 81920, False, t[0])[0]]))
t[0] += 60; b.training[0] = 20500; p.step()
ok("又 60 秒，变化 < 1GiB 但离上次大变化才 60 秒：不再补", b.live[0] == lv)
t[0] += 50; p.step()
ok("稳定 110 秒：训练涨上来后已经超过 55GB，不再补", b.used() >= 55000 and b.live[0] == lv)
t[0] += 10; b.training[0] = 22500; p.step()
ok("训练又涨 2GB（没超过 high）：不放、也不再补", b.live[0] == lv)
t[0] += 200; b.training[0] = 3000; p.step()
ok("一个训练结束、训练显存掉下来：马上补回 floor，不等稳定", b.used() >= 42000)
t[0] += 10; p.step()
ok("再等稳定后补到 55GB", not filled(b.used()))
t[0] += 110; p.step()
ok("稳定 120 秒：补到 55GB", filled(b.used()))
G.time.time = time.time

with tempfile.TemporaryDirectory() as d:
    base = os.path.join(d, "pad")
    b = FakeBackend()
    p, logs = pad_of(b, base=base, settle=100)
    t = [1000.0]
    G.time.time = lambda: t[0]
    p.changed[0], p.train_ref[0] = 0, 9000
    p.step()
    ok("稳定的训练：补到 55GB", filled(b.used()))
    ok("<base>.mib 写了 pad 的大小", open(base + ".mib").read().strip() == str(45 * 1024))
    ok("还没有新训练信号：没有 <base>.ready", not os.path.exists(base + ".ready"))
    open(base + ".start", "w").close()
    os.utime(base + ".start", (t[0] + 5, t[0] + 5))
    t[0] += 10; p.step()
    ok("收到新训练信号：只放到 floor（42GB），不低于它", at_floor(b.used()) and any("要开始新训练" in x for x in logs))
    ok("放好后写 <base>.ready", open(base + ".ready").read().strip() == str(t[0] - 5))
    ok("给新训练留出约 39GB", 81920 - b.used() >= 38000)
    lv = b.live[0]
    t[0] += 50; b.training[0] = 18000; p.step()
    ok("新训练起来了、还没稳定：不补", b.live[0] == lv)
    t[0] += 110; p.step()
    ok("新训练稳定后：补到 55GB", filled(b.used()))
    os.utime(base + ".start", (t[0] + 5, t[0] + 5))
    t[0] += 10; p.step()
    ok("7b 第二次信号：放到 floor", at_floor(b.used()))
    t[0] += 300; p.step()
    ok("7b 训练显存没涨（新训练还没起来）：settle 过了也不补到 target", at_floor(b.used()) and any("等新训练起来" in x for x in [p._decide(0, b.used(), 81920, False, t[0])[0]]))
    t[0] += 300; b.training[0] = 30000; p.step()
    ok("7b 新训练起来了：先等它稳定", not filled(b.used()) and b.used() >= 42000)
    t[0] += 110; p.step()
    ok("7b 稳定后补上", filled(b.used()))
    os.utime(base + ".start", (t[0] + 5, t[0] + 5))
    t[0] += 10; p.step()
    t[0] += 950; p.step()
    ok("7b 等了 start_hold（900 秒）新训练还没起来：照常补", filled(b.used()))
    os.utime(base + ".start", (t[0] + 5, t[0] + 5))
    t[0] += 10; p.step()
    t[0] += 10; b.training[0] = 1000; p.step()
    ok("7c 旧训练结束、新训练还没起来：显存不低于 floor", b.used() >= 42000)
    t[0] += 60; b.training[0] = 20000; p.step()
    ok("7c 新训练涨上来：放掉多出 high 的部分或保持，不低于 floor", 42000 <= b.used() <= 62000)
    t[0] += 110; p.step()
    ok("7c 旧训练结束后新训练起来：稳定后在 55GB 以上、high 以下", 55000 <= b.used() <= 62000)
    b.training[0] = 50000
    p._release(0, 0, reason="测试")
    os.remove(base + ".ready")
    t[0] += 200; p.step()
    os.utime(base + ".start", (t[0] + 5, t[0] + 5))
    t[0] += 10; p.step()
    ok("7d 训练本身已经超过 floor 时收到信号：没什么可放，照样写 ready", os.path.exists(base + ".ready") and b.live[0] == 0)
    G.time.time = time.time

b = FakeBackend(fallback=True)
p, _ = pad_of(b)
p.step()
ok("退回 mem_get_info 时目标多 1500MiB", filled(b.used(), 56500))

b = FakeBackend(total=32000)
p, logs = pad_of(b)
p.step()
ok("总显存 <60GB 不填", b.live[0] == 0 and any("不填充" in x for x in logs))

b = FakeBackend()
p, logs = pad_of(b)
b.fail_alloc = {0}
p.step()
ok("分配失败不抛异常、不占显存", b.live[0] == 0 and any("失败" in x for x in logs))
b.fail_alloc = set()
p.next_try[0] = 0
p.step()
ok("恢复后能补上", filled(b.used()))

b = FakeBackend(training=70000)
p, _ = pad_of(b, target_mib=76000, high_mib=79000, min_free_mib=8000)
p.step()
ok("给训练留 min_free", b.used() <= 81920 - 8000 and b.live[0] > 0)

b = FakeBackend()
p, logs = pad_of(b, dry_run=True)
p.step()
ok("dry-run 不分配", b.live[0] == 0 and any("dry-run" in x for x in logs))

with tempfile.TemporaryDirectory() as d:
    b = FakeBackend()
    sw = os.path.join(d, "DISABLED")
    open(sw, "w").close()
    p, logs = pad_of(b, switch=sw)
    p.run()
    ok("DISABLED 时不填充并退出", b.live[0] == 0 and any("DISABLED" in x for x in logs))

b = FakeBackend()
p, logs = pad_of(b, parent_pid=999999)
p.run()
ok("父进程不在了就退出、不占显存", any("父进程" in x for x in logs) and b.live[0] == 0)

b = FakeBackend(devices=2)
b.training[0] = 0
p, _ = pad_of(b)
p.step()
ok("多卡分别处理", at_floor(b.used(0)) and filled(b.used(1)))

b = FakeBackend()
p, _ = pad_of(b, iterations=2)
p.run()
ok("iterations=2 跑两步后释放", p._steps == 2 and b.live[0] == 0)

ok("无 CUDA_VISIBLE_DEVICES + 多卡拒绝", not G.visible_devices_ok(None, 2) and not G.visible_devices_ok("", 2))
ok("有 CUDA_VISIBLE_DEVICES 或多卡以下允许", G.visible_devices_ok("0", 2) and G.visible_devices_ok(None, 1))

a = G.parse_args([])
ok("默认：floor 42000、target 55000、high 62000 MiB，settle 120 秒、start_hold 900 秒、10 秒一轮、2000MiB 以下算没有训练",
   (a.floor_mib, a.target_mib, a.high_mib) == (42000, 55000, 62000)
   and a.settle == 120 and a.start_hold == 900 and a.interval == 10 and a.min_train_mib == 2000)
os.environ["SLURM_JOB_ID"] = "4242"
os.environ.pop("TASKQ_GPU_PAD_BASE", None)
ok("默认 base 是 /tmp/gpu_pad_<作业号>", G.parse_args([]).base == "/tmp/gpu_pad_4242")
ok("总开关默认在 TASKQ_STATE/gpu_pad/DISABLED", G.default_switch() == os.path.join(os.environ["TASKQ_STATE"], "gpu_pad", "DISABLED"))
os.environ["TASKQ_GPU_PAD_FLOOR_MIB"] = "43000"
f_new = G.parse_args([]).floor_mib
os.environ.pop("TASKQ_GPU_PAD_FLOOR_MIB")
ok("环境变量 TASKQ_GPU_PAD_FLOOR_MIB 改默认值", f_new == 43000)
try:
    with contextlib.redirect_stderr(io.StringIO()):
        G.parse_args(["--target-mib", "70000", "--high-mib", "60000"])
    bad = False
except SystemExit:
    bad = True
ok("target > high 被拒绝", bad)
try:
    with contextlib.redirect_stderr(io.StringIO()):
        G.parse_args(["--floor-mib", "56000"])
    bad = False
except SystemExit:
    bad = True
ok("floor > target 被拒绝", bad)

b = FakeBackend()
p, _ = pad_of(b)
p.step()
p._release(0, 0, reason="测试")
ok("释放到 0 后只剩训练（缓存也清了）", b.used() == 9000 and b.cache[0] == 0 and not p.chunks[0])
b = FakeBackend()
p, _ = pad_of(b)
p.step()
b.training[0] = 34000
calls, empties = b.mem_calls, b.empty_calls
p.step()
calls, empties = b.mem_calls - calls, b.empty_calls - empties
ok("超过 high：只放够的块数（降到 59952 以下、不多于一块的余量）", 59952 - 1024 < b.used() <= 59952)
ok("超过 high：只读一次显存、只 empty_cache 一次", calls == 1 and empties == 1)

b = FakeBackend(devices=2)
b.fail_alloc = {0}
p, logs = pad_of(b)
p.step()
ok("dev 0 分配失败不影响 dev 1", b.live[0] == 0 and filled(b.used(1)))
ok("两张卡都写心跳", any("心跳 dev 0" in x for x in logs) and any("心跳 dev 1" in x for x in logs))

if fails:
    print(f"{fails} 项失败")
    sys.exit(1)
print("全部通过")
