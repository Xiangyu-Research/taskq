#!/usr/bin/env python3
"""GPU pad: reserve GPU memory for this Slurm GPU job so other jobs see the GPU as occupied; it does not train.

    python3 gpu_pad.py --floor-mib N --target-mib N --high-mib N
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import cfg as tcfg
except ImportError:
    tcfg = None
import proto  # noqa: E402

MIB_F, READY_F, START_F = proto.GPU_PAD_FILES  # handshake files with the worker: <base>.mib, .ready, .start

MIB = 1024 * 1024
STABLE_TOL_MIB = 1024
FALLBACK_TARGET_MARGIN_MIB = 1500
HEARTBEAT = 600


def pad_env(name, default=None):
    if tcfg:
        return tcfg.env("TASKQ_GPU_PAD_" + name, default)
    return os.environ.get("TASKQ_GPU_PAD_" + name) or default


def default_switch():
    return tcfg.state("gpu_pad", "DISABLED") if tcfg else os.path.expanduser("~/.local/state/taskq/gpu_pad/DISABLED")


def env_int(name, default):
    v = pad_env(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def default_base():
    return pad_env("BASE") or f"{proto.GPU_PAD_BASE}{os.environ.get('SLURM_JOB_ID', 'nojob')}"


def env_float(name, default):
    v = pad_env(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except ValueError:
        return default


@dataclass
class Config:
    floor_mib: int
    target_mib: int
    high_mib: int
    chunk_mib: int = 1024
    interval: float = 10.0
    settle: float = 120.0
    start_hold: float = 900.0
    min_free_mib: int = 8000
    min_total_mib: int = 60000
    release_hysteresis_mib: int = 2048
    min_train_mib: int = 2000
    base: str = ""
    switch: str = default_switch()
    parent_pid: int | None = None
    supervisor_pid: int | None = None
    dry_run: bool = False
    iterations: int = 0
    device: int | None = None


class TorchBackend:

    def __init__(self, device=None):
        import torch
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA is not available")
        self.torch = torch
        self.device = device

    def device_count(self):
        return self.torch.cuda.device_count()

    def device_ids(self):
        if self.device is not None:
            return [self.device]
        return list(range(self.device_count()))

    def uuid(self, dev):
        u = str(self.torch.cuda.get_device_properties(dev).uuid)
        return u if u.startswith("GPU-") else "GPU-" + u

    def nvidia(self, dev, *args):
        cmd = ["nvidia-smi", "-i", self.uuid(dev), *args]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if r.returncode != 0:
            raise RuntimeError((r.stderr or r.stdout).strip() or f"nvidia-smi exit code {r.returncode}")
        return r.stdout.strip()

    def mem(self, dev):
        try:
            out = self.nvidia(dev, "--query-gpu=memory.used,memory.total", "--format=csv,noheader,nounits")
            used, total = [int(x.strip()) for x in out.splitlines()[0].split(",")]
            return used, total, False
        except Exception:
            free, total = self.torch.cuda.mem_get_info(dev)
            return int((total - free) // MIB), int(total // MIB), True

    def pad_mib(self, dev):
        return int(self.torch.cuda.memory_reserved(dev) // MIB)

    def alloc(self, dev, mib):
        return self.torch.zeros(int(mib) * MIB, dtype=self.torch.uint8, device=dev)

    def empty_cache(self, dev):
        with self.torch.cuda.device(dev):
            self.torch.cuda.empty_cache()


def log(msg):
    print(f"{time.strftime('%F %T')} {msg}", flush=True)


def visible_devices_ok(cuda_visible, count):
    return bool(cuda_visible) or count <= 1


class Pad:
    def __init__(self, backend, cfg, logger=log):
        self.b = backend
        self.c = cfg
        self.log = logger
        self.chunks = {d: [] for d in backend.device_ids()}
        self.devices = list(self.chunks)
        self.start = time.time()
        self.last_heartbeat = {d: 0.0 for d in self.devices}
        self.next_try = {d: 0.0 for d in self.devices}
        self.skip_total = set()
        self.train_ref = {d: 0 for d in self.devices}
        self.changed = {d: self.start for d in self.devices}
        self.seen_start = self._start_mtime()
        self.wait_new = {}
        self.stopped = False
        self._steps = 0
        self.initial_ppid = os.getppid()
        self.log(f"start: job={os.environ.get('SLURM_JOB_ID', '')} host={socket.gethostname()} pid={os.getpid()} "
                 f"floor={cfg.floor_mib}MiB target={cfg.target_mib}MiB high={cfg.high_mib}MiB settle={cfg.settle:g}s start_hold={cfg.start_hold:g}s "
                 f"min_train={cfg.min_train_mib}MiB base={cfg.base or '(none)'} devices={self.devices} "
                 f"switch={cfg.switch} parent={cfg.parent_pid} supervisor={cfg.supervisor_pid} dry_run={cfg.dry_run}")

    def _start_mtime(self):
        try:
            return os.path.getmtime(self.c.base + START_F) if self.c.base else 0.0
        except OSError:
            return 0.0

    def _write_mib(self):
        if not self.c.base:
            return
        try:
            with open(self.c.base + MIB_F + ".tmp", "w") as f:
                f.write(f"{sum(self.b.pad_mib(d) for d in self.devices)}\n")
            os.replace(self.c.base + MIB_F + ".tmp", self.c.base + MIB_F)
        except OSError:
            pass

    def _write_ready(self):
        if not self.c.base:
            return
        try:
            with open(self.c.base + READY_F + ".tmp", "w") as f:
                f.write(f"{self.seen_start}\n")
            os.replace(self.c.base + READY_F + ".tmp", self.c.base + READY_F)
        except OSError:
            pass

    def stop_now(self, *_):
        self.stopped = True

    def switch_on(self):
        return os.path.exists(self.c.switch)

    def parent_gone(self):
        for pid in (self.c.parent_pid, self.c.supervisor_pid):
            if pid is None:
                continue
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return True
            except PermissionError:
                pass
        if self.initial_ppid != 1 and os.getppid() != self.initial_ppid:
            return True
        return False

    def _step_dev(self, dev, now):
        try:
            used, total, fallback = self.b.mem(dev)
        except Exception as e:
            self.log(f"dev {dev}: cannot read GPU memory ({e})")
            return
        if total < self.c.min_total_mib:
            if dev not in self.skip_total:
                self.log(f"dev {dev}: total={total}MiB < {self.c.min_total_mib}MiB, not an 80GB A100; not padding")
                self.skip_total.add(dev)
            return
        state, used = self._decide(dev, used, total, fallback, now)
        if now - self.last_heartbeat[dev] >= HEARTBEAT:
            self.log(f"heartbeat dev {dev}: used={used}MiB total={total}MiB pad={self.b.pad_mib(dev)}MiB {state}")
            self.last_heartbeat[dev] = now

    def _decide(self, dev, used, total, fallback, now):
        train = max(0, used - self.b.pad_mib(dev))
        if abs(train - self.train_ref[dev]) > STABLE_TOL_MIB:
            self.train_ref[dev], self.changed[dev] = train, now
        if dev in self.wait_new:
            self.wait_new[dev] = (min(self.wait_new[dev][0], train), self.wait_new[dev][1])
        margin = FALLBACK_TARGET_MARGIN_MIB if fallback else 0
        floor, target = self.c.floor_mib + margin, self.c.target_mib + margin
        if train < self.c.min_train_mib:
            if used < floor:
                used = self._allocate(dev, floor, used, total)
            return f"no training (train={train}MiB); holding floor", used
        state = f"train={train}MiB"
        waiting = False
        if dev in self.wait_new:
            before, t0 = self.wait_new[dev]
            if train >= before + self.c.min_train_mib or now - t0 >= self.c.start_hold:
                del self.wait_new[dev]
            else:
                waiting = True
        if used > self.c.high_mib:
            used = self._release(dev, self.c.high_mib - self.c.release_hysteresis_mib,
                                 reason=f"used={used}MiB > high={self.c.high_mib}MiB", used=used)
            return state, used
        if used < floor:
            used = self._allocate(dev, floor, used, total)
        if waiting:
            return f"{state}, waiting for the new training to start (was {self.wait_new[dev][0]}MiB)", used
        if now - self.changed[dev] < self.c.settle:
            return f"{state}, training memory still changing; waiting for it to settle", used
        if used < target:
            used = self._allocate(dev, target, used, total)
        return state, used

    def _allocate(self, dev, target, used, total):
        if self.c.dry_run:
            self.log(f"dry-run dev {dev}: used={used} target={target}, would add {target - used}MiB")
            return used
        while used < target:
            if time.time() < self.next_try[dev]:
                return used
            mib = self.c.chunk_mib
            if total - used - mib < self.c.min_free_mib:
                self.log(f"dev {dev}: adding {mib}MiB would leave less than min_free={self.c.min_free_mib}MiB; stopping")
                return used
            try:
                self.chunks[dev].append((self.b.alloc(dev, mib), mib))
            except Exception as e:
                self.log(f"dev {dev}: failed to allocate {mib}MiB ({e}); retrying in 60 seconds")
                self.next_try[dev] = time.time() + 60
                return used
            used += mib
            self.log(f"dev {dev}: added {mib}MiB, pad={self.b.pad_mib(dev)}MiB")
        return used

    def _release(self, dev, limit, reason="", used=None, keep_limit=False):
        chunks = self.chunks[dev]
        if not chunks:
            return used
        if used is None:
            try:
                used = self.b.mem(dev)[0]
            except Exception:
                used = None
        excess = None if used is None or limit <= 0 else used - limit
        if excess is not None and excess <= 0:
            return used
        n, freed = 0, 0
        if keep_limit and (excess is None or chunks[-1][1] > excess):
            return used
        while chunks and (excess is None or freed < excess):
            if keep_limit and freed + chunks[-1][1] > excess:
                break
            _tensor, mib = chunks.pop()
            del _tensor
            freed += mib
            n += 1
        self.b.empty_cache(dev)
        self.log(f"dev {dev}: {reason}; released {freed}MiB ({n} chunks), pad={self.b.pad_mib(dev)}MiB")
        return None if used is None else used - freed

    def step(self):
        if self.switch_on():
            self.log("DISABLED switch present; releasing and exiting")
            return False
        if self.parent_gone():
            self.log("parent or supervisor exited; releasing and exiting")
            return False
        now = time.time()
        st = self._start_mtime()
        if st > self.seen_start:
            self.seen_start = st
            for dev in self.devices:
                self._release(dev, self.c.floor_mib, reason="new training starting; dropping to floor", keep_limit=True)
                self.changed[dev] = now
                self.wait_new[dev] = (self.train_ref[dev], now)
            self._write_ready()
        for dev in self.devices:
            self._step_dev(dev, now)
        self._write_mib()
        return True

    def run(self):
        try:
            while not self.stopped:
                if not self.step():
                    break
                self._steps += 1
                if self.c.iterations and self._steps >= self.c.iterations:
                    break
                if self.c.interval > 0:
                    time.sleep(self.c.interval)
        except KeyboardInterrupt:
            pass
        finally:
            for dev in self.devices:
                self._release(dev, 0, reason="exit")
            self._write_mib()
            self.log("finished")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="GPU pad for a Slurm GPU job (holds GPU memory only, does not train); the three thresholds come from the machine table's gpu_pad or env vars")
    p.add_argument("--floor-mib", type=int, default=env_int("FLOOR_MIB", None))
    p.add_argument("--target-mib", type=int, default=env_int("TARGET_MIB", None))
    p.add_argument("--high-mib", type=int, default=env_int("HIGH_MIB", None))
    p.add_argument("--chunk-mib", type=int, default=env_int("CHUNK_MIB", 1024))
    p.add_argument("--interval", type=float, default=env_float("INTERVAL", 10.0))
    p.add_argument("--settle", type=float, default=env_float("SETTLE", 120.0))
    p.add_argument("--start-hold", type=float, default=env_float("START_HOLD", 900.0))
    p.add_argument("--min-free-mib", type=int, default=env_int("MIN_FREE_MIB", 8000))
    p.add_argument("--min-total-mib", type=int, default=env_int("MIN_TOTAL_MIB", 60000))
    p.add_argument("--release-hysteresis-mib", type=int, default=env_int("HYSTERESIS_MIB", 2048))
    p.add_argument("--min-train-mib", type=int, default=env_int("MIN_TRAIN_MIB", 2000))
    p.add_argument("--base", default=default_base())
    p.add_argument("--switch", default=pad_env("SWITCH", default_switch()))
    p.add_argument("--parent-pid", type=int, default=None)
    p.add_argument("--supervisor-pid", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--iterations", type=int, default=0)
    p.add_argument("--device", type=int, default=None)
    a = p.parse_args(argv)
    if None in (a.floor_mib, a.target_mib, a.high_mib):
        p.error("set floor-mib, target-mib and high-mib (the machine table's gpu_pad, or TASKQ_GPU_PAD_FLOOR_MIB / TARGET_MIB / HIGH_MIB)")
    if not 0 < a.floor_mib <= a.target_mib <= a.high_mib:
        p.error("require 0 < floor-mib <= target-mib <= high-mib")
    if a.chunk_mib <= 0 or a.interval < 0:
        p.error("chunk-mib must be > 0 and interval >= 0")
    return a


def main(argv=None):
    a = parse_args(argv)
    if a.dry_run:
        log("dry-run: read GPU memory only, do not allocate")
    try:
        backend = TorchBackend(device=a.device)
    except Exception as e:
        log(f"failed to initialize GPU: {e}")
        return 1
    if backend.device_count() <= 0:
        log("no visible GPU")
        return 1
    if not visible_devices_ok(os.environ.get("CUDA_VISIBLE_DEVICES"), backend.device_count()):
        log("no CUDA_VISIBLE_DEVICES but several GPUs visible; refusing to run (might touch someone else's GPU)")
        return 1
    if a.device is not None and a.device >= backend.device_count():
        log(f"--device {a.device} is beyond the {backend.device_count()} visible GPUs")
        return 1
    cfg = Config(floor_mib=a.floor_mib, target_mib=a.target_mib, high_mib=a.high_mib, chunk_mib=a.chunk_mib,
                 interval=a.interval, settle=a.settle, start_hold=a.start_hold, min_free_mib=a.min_free_mib,
                 min_total_mib=a.min_total_mib, release_hysteresis_mib=a.release_hysteresis_mib,
                 min_train_mib=a.min_train_mib, base=a.base, switch=a.switch, parent_pid=a.parent_pid,
                 supervisor_pid=a.supervisor_pid, dry_run=a.dry_run, iterations=a.iterations, device=a.device)
    pad = Pad(backend, cfg)
    signal.signal(signal.SIGTERM, pad.stop_now)
    signal.signal(signal.SIGINT, pad.stop_now)
    pad.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
