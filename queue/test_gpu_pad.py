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
os.environ["TASKQ_STATE"] = fixture.tmpdir()  # keep the default DISABLED switch out of this machine's real state dir
sys.path.insert(0, HERE)
import gpu_pad as G  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
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
ok("pads to 55000 MiB or more in whole chunks, less than one chunk over", filled(b.used()) and b.live[0] == 45 * 1024)
ok("log has the start and added memory", any("start:" in x for x in logs) and any("added " in x for x in logs))

b = FakeBackend(training=3000)
p, _ = pad_of(b, floor_mib=5500, target_mib=5500, high_mib=9000, chunk_mib=1024)
p.step()
ok("pads whole chunks only (2500 short, adds 3 chunks)", b.live[0] == 3 * 1024)

b = FakeBackend()
p, _ = pad_of(b)
p.step()
b.training[0] = 39000
p.step()
ok("used > high releases down to high - hysteresis", b.used() <= 62000 - 2048 and b.live[0] > 0)

b = FakeBackend(training=70000)
p, _ = pad_of(b)
p.step()
ok("training alone above high leaves the pad untouched", b.live[0] == 0)

b = FakeBackend(training=0)
p, logs = pad_of(b)
p.step()
ok("job starts, no training yet: pad to floor first", at_floor(b.used()) and any("no training" in x for x in logs))
b.training[0] = 9000
p.step()
ok("training starts: pad to 55GB", filled(b.used()))
b.training[0] = 500
p.step()
ok("training gone: do not release; memory stays at or above floor", b.live[0] == 45 * 1024 and b.used() >= 42000)
b.training[0] = 0
b.live[0] -= 0
p._release(0, 0, reason="test")
p.step()
ok("trained before, nothing now: pad back to floor at once (not target)", at_floor(b.used()))

b = FakeBackend(training=0)
p, logs = pad_of(b, settle=100)
t = [1000.0]
G.time.time = lambda: t[0]
p.changed[0] = t[0]
b.training[0] = 5000; p.step()
ok("training just started: pad to floor at once, not target", at_floor(b.used()))
lv = b.live[0]
t[0] += 60; b.training[0] = 20000; p.step()
ok("memory still growing after 60 seconds: add no more", b.live[0] == lv and any("waiting for it to settle" in x for x in [p._decide(0, b.used(), 81920, False, t[0])[0]]))
t[0] += 60; b.training[0] = 20500; p.step()
ok("another 60 seconds, change < 1GiB but only 60 seconds since the last big change: add no more", b.live[0] == lv)
t[0] += 50; p.step()
ok("stable for 110 seconds: training memory is already above 55GB; add no more", b.used() >= 55000 and b.live[0] == lv)
t[0] += 10; b.training[0] = 22500; p.step()
ok("training grows another 2GB (below high): neither release nor add", b.live[0] == lv)
t[0] += 200; b.training[0] = 3000; p.step()
ok("a training ended and memory dropped: pad back to floor at once, do not wait to settle", b.used() >= 42000)
t[0] += 10; p.step()
ok("after waiting to settle, not yet at 55GB", not filled(b.used()))
t[0] += 110; p.step()
ok("stable for 120 seconds: pad to 55GB", filled(b.used()))
G.time.time = time.time

with tempfile.TemporaryDirectory() as d:
    base = os.path.join(d, "pad")
    b = FakeBackend()
    p, logs = pad_of(b, base=base, settle=100)
    t = [1000.0]
    G.time.time = lambda: t[0]
    p.changed[0], p.train_ref[0] = 0, 9000
    p.step()
    ok("stable training: pad to 55GB", filled(b.used()))
    ok("<base>.mib holds the pad size", open(base + ".mib").read().strip() == str(45 * 1024))
    ok("no new-training signal yet: no <base>.ready", not os.path.exists(base + ".ready"))
    open(base + ".start", "w").close()
    os.utime(base + ".start", (t[0] + 5, t[0] + 5))
    t[0] += 10; p.step()
    ok("new-training signal: drop to floor (42GB) only, not below", at_floor(b.used()) and any("new training starting" in x for x in logs))
    ok("writes <base>.ready after making room", open(base + ".ready").read().strip() == str(t[0] - 5))
    ok("leaves about 39GB for the new training", 81920 - b.used() >= 38000)
    lv = b.live[0]
    t[0] += 50; b.training[0] = 18000; p.step()
    ok("new training started but not stable: add nothing", b.live[0] == lv)
    t[0] += 110; p.step()
    ok("new training stable: pad to 55GB", filled(b.used()))
    os.utime(base + ".start", (t[0] + 5, t[0] + 5))
    t[0] += 10; p.step()
    ok("7b second signal: drop to floor", at_floor(b.used()))
    t[0] += 300; p.step()
    ok("7b training memory did not grow (new training not up): past settle, still not padded to target", at_floor(b.used()) and any("waiting for the new training" in x for x in [p._decide(0, b.used(), 81920, False, t[0])[0]]))
    t[0] += 300; b.training[0] = 30000; p.step()
    ok("7b new training started: wait for it to settle", not filled(b.used()) and b.used() >= 42000)
    t[0] += 110; p.step()
    ok("7b padded after it settles", filled(b.used()))
    os.utime(base + ".start", (t[0] + 5, t[0] + 5))
    t[0] += 10; p.step()
    t[0] += 950; p.step()
    ok("7b waited start_hold (900 seconds) and the new training did not start: pad as usual", filled(b.used()))
    os.utime(base + ".start", (t[0] + 5, t[0] + 5))
    t[0] += 10; p.step()
    t[0] += 10; b.training[0] = 1000; p.step()
    ok("7c old training ended, new one not up: memory stays at or above floor", b.used() >= 42000)
    t[0] += 60; b.training[0] = 20000; p.step()
    ok("7c new training grows: release what is above high or keep it, never below floor", 42000 <= b.used() <= 62000)
    t[0] += 110; p.step()
    ok("7c new training after the old one ended: after settling, above 55GB and below high", 55000 <= b.used() <= 62000)
    b.training[0] = 50000
    p._release(0, 0, reason="test")
    os.remove(base + ".ready")
    t[0] += 200; p.step()
    os.utime(base + ".start", (t[0] + 5, t[0] + 5))
    t[0] += 10; p.step()
    ok("7d signal when training alone is above floor: nothing to release, still writes ready", os.path.exists(base + ".ready") and b.live[0] == 0)
    G.time.time = time.time

b = FakeBackend(fallback=True)
p, _ = pad_of(b)
p.step()
ok("mem_get_info fallback adds 1500MiB to the target", filled(b.used(), 56500))

b = FakeBackend(total=32000)
p, logs = pad_of(b)
p.step()
ok("total memory <60GB: no padding", b.live[0] == 0 and any("not padding" in x for x in logs))

b = FakeBackend()
p, logs = pad_of(b)
b.fail_alloc = {0}
p.step()
ok("allocation failure raises nothing and holds no memory", b.live[0] == 0 and any("failed to allocate" in x for x in logs))
b.fail_alloc = set()
p.next_try[0] = 0
p.step()
ok("pads once allocation works again", filled(b.used()))

b = FakeBackend(training=70000)
p, _ = pad_of(b, target_mib=76000, high_mib=79000, min_free_mib=8000)
p.step()
ok("leaves min_free for training", b.used() <= 81920 - 8000 and b.live[0] > 0)

b = FakeBackend()
p, logs = pad_of(b, dry_run=True)
p.step()
ok("dry-run allocates nothing", b.live[0] == 0 and any("dry-run" in x for x in logs))

with tempfile.TemporaryDirectory() as d:
    b = FakeBackend()
    sw = os.path.join(d, "DISABLED")
    open(sw, "w").close()
    p, logs = pad_of(b, switch=sw)
    p.run()
    ok("DISABLED: no padding, exits", b.live[0] == 0 and any("DISABLED" in x for x in logs))

b = FakeBackend()
p, logs = pad_of(b, parent_pid=fixture.dead_pid())
p.run()
ok("parent gone: exits holding no memory", any("parent or supervisor" in x for x in logs) and b.live[0] == 0)

b = FakeBackend(devices=2)
b.training[0] = 0
p, _ = pad_of(b)
p.step()
ok("handles each GPU separately", at_floor(b.used(0)) and filled(b.used(1)))

b = FakeBackend()
p, _ = pad_of(b, iterations=2)
p.run()
ok("iterations=2 runs two steps then releases", p._steps == 2 and b.live[0] == 0)

ok("no CUDA_VISIBLE_DEVICES with several GPUs is refused", not G.visible_devices_ok(None, 2) and not G.visible_devices_ok("", 2))
ok("CUDA_VISIBLE_DEVICES set, or one GPU, is allowed", G.visible_devices_ok("0", 2) and G.visible_devices_ok(None, 1))

try:
    with contextlib.redirect_stderr(io.StringIO()):
        G.parse_args([])
    no_thr = False
except SystemExit:
    no_thr = True
ok("no three thresholds (machine table gpu_pad or env vars): argument error", no_thr)
os.environ.update(TASKQ_GPU_PAD_FLOOR_MIB="42000", TASKQ_GPU_PAD_TARGET_MIB="55000", TASKQ_GPU_PAD_HIGH_MIB="62000")
a = G.parse_args([])
ok("thresholds from env vars; settle 120 s, start_hold 900 s, 10 s rounds, below 2000MiB counts as no training",
   (a.floor_mib, a.target_mib, a.high_mib) == (42000, 55000, 62000)
   and a.settle == 120 and a.start_hold == 900 and a.interval == 10 and a.min_train_mib == 2000)
os.environ["SLURM_JOB_ID"] = "4242"
os.environ.pop("TASKQ_GPU_PAD_BASE", None)
ok("default base is /tmp/gpu_pad_<job id>", G.parse_args([]).base == "/tmp/gpu_pad_4242")
ok("the master switch defaults to TASKQ_STATE/gpu_pad/DISABLED", G.default_switch() == os.path.join(os.environ["TASKQ_STATE"], "gpu_pad", "DISABLED"))
os.environ["TASKQ_GPU_PAD_FLOOR_MIB"] = "43000"
f_new = G.parse_args([]).floor_mib
os.environ["TASKQ_GPU_PAD_FLOOR_MIB"] = "42000"
ok("TASKQ_GPU_PAD_FLOOR_MIB changes the threshold", f_new == 43000)
try:
    with contextlib.redirect_stderr(io.StringIO()):
        G.parse_args(["--target-mib", "70000", "--high-mib", "60000"])
    bad = False
except SystemExit:
    bad = True
ok("target > high is rejected", bad)
try:
    with contextlib.redirect_stderr(io.StringIO()):
        G.parse_args(["--floor-mib", "56000"])
    bad = False
except SystemExit:
    bad = True
ok("floor > target is rejected", bad)

b = FakeBackend()
p, _ = pad_of(b)
p.step()
p._release(0, 0, reason="test")
ok("after releasing to 0 only training remains (cache cleared too)", b.used() == 9000 and b.cache[0] == 0 and not p.chunks[0])
b = FakeBackend()
p, _ = pad_of(b)
p.step()
b.training[0] = 34000
calls, empties = b.mem_calls, b.empty_calls
p.step()
calls, empties = b.mem_calls - calls, b.empty_calls - empties
ok("above high: release just enough chunks (down to 59952, at most one chunk over)", 59952 - 1024 < b.used() <= 59952)
ok("above high: one memory read and one empty_cache", calls == 1 and empties == 1)

b = FakeBackend(devices=2)
b.fail_alloc = {0}
p, logs = pad_of(b)
p.step()
ok("allocation failure on dev 0 does not affect dev 1", b.live[0] == 0 and filled(b.used(1)))
ok("heartbeats written for both GPUs", any("heartbeat dev 0" in x for x in logs) and any("heartbeat dev 1" in x for x in logs))

if fails:
    print(f"{fails} checks failed")
    sys.exit(1)
print("all passed")
