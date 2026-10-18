#!/usr/bin/env python3
"""DICC GPU 作业显存填充（2026-10-17）：DICC 的 gpu-a100 作业要求显存占用 >= 40GB。

只占显存，不做训练/推理：每张卡分配一组 CUDA 张量，按 nvidia-smi 的 memory.used 动态调整。
  - 低于 target（默认 42000 MiB，> 40GiB 和 40GB 两种口径）就补；
  - 高于 high（默认 62000 MiB）就一次释放到 high - hysteresis 以下；
  - 给训练留至少 min_free（默认 8000 MiB）。
不分模型（10/18 改）：先让训练把显存用起来，等它稳定了再补。
  - 训练占了多少 = memory.used - 本进程 torch 保留的显存；不到 min_train（默认 2000 MiB）算没有训练，全部释放；
  - 训练占的显存 settle 秒（默认 120）内变化不超过 1 GiB 才算稳定，稳定了才补；
  - 要开始新训练时，dicc_worker.sh 先 touch <base>.start：pad 下一轮（interval 默认 10 秒）全部释放，
    等新训练起来（训练占的显存比放掉前多出 min_train）并稳定后再补；start_hold（默认 900 秒）内一直没起来也照常补。
    这样不管什么模型，启动时都有整张卡可用。
  - 本进程占着多少写在 <base>.mib（dicc_worker.sh 判断能不能再加一个训练时减掉它）。
  <base> 默认 /tmp/gpu_pad_<作业号>，可用 $RQ1_GPU_PAD_BASE 改。

由 scripts/queue/gpu_pad_start.sh 在 Slurm GPU 作业里启动（worker.sbatch，和经 envs/nnunet_paths.sh 的旧按折作业）；
要求空卡的脚本（run_gpu_smoke.sh、predict.sh、run_predict_eval.sh 等）自己设 RQ1_GPU_PAD=0。总开关是 ~/.cache/phd/gpu_pad/DISABLED。
日志由启动脚本重定向到 <数据目录>/logs/workers/gpu_pad_<作业号>.log（10/18 前在 nnUNet/results/logs/）。

测试：python3 scripts/queue/test_gpu_pad.py（假 backend，不需要 GPU）。
手动试：python3 scripts/queue/gpu_pad.py --dry-run --iterations 2
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass

MIB = 1024 * 1024
STABLE_TOL_MIB = 1024  # 训练占的显存变化在这以内算没变
FALLBACK_TARGET_MARGIN_MIB = 1500  # nvidia-smi 失败、退回 mem_get_info 时多留一点
HEARTBEAT = 600


def env_int(name, default):
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def default_base():
    return os.environ.get("RQ1_GPU_PAD_BASE") or f"/tmp/gpu_pad_{os.environ.get('SLURM_JOB_ID', 'nojob')}"


def env_float(name, default):
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except ValueError:
        return default


@dataclass
class Config:
    target_mib: int = 42000
    high_mib: int = 62000
    chunk_mib: int = 1024
    interval: float = 10.0
    settle: float = 120.0
    start_hold: float = 900.0
    min_free_mib: int = 8000
    min_total_mib: int = 60000
    release_hysteresis_mib: int = 2048
    min_train_mib: int = 2000
    base: str = ""
    switch: str = os.path.expanduser("~/.cache/phd/gpu_pad/DISABLED")
    parent_pid: int | None = None
    supervisor_pid: int | None = None
    dry_run: bool = False
    iterations: int = 0
    device: int | None = None


class TorchBackend:
    """真实的 GPU 操作：nvidia-smi 看 used/total，torch 分配/释放。"""

    def __init__(self, device=None):
        import torch  # 延迟导入：测试和 --help 不需要 torch
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA 不可用")
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
            raise RuntimeError((r.stderr or r.stdout).strip() or f"nvidia-smi 退出码 {r.returncode}")
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
        """本进程占着的显存：torch 保留的（含还没还给驱动的缓存）。"""
        return int(self.torch.cuda.memory_reserved(dev) // MIB)

    def alloc(self, dev, mib):
        # zeros 会真正写入显存，确保 nvidia-smi 立刻看到；u8 每 MiB 只占 1 MiB。
        return self.torch.zeros(int(mib) * MIB, dtype=self.torch.uint8, device=dev)

    def empty_cache(self, dev):
        with self.torch.cuda.device(dev):
            self.torch.cuda.empty_cache()


def log(msg):
    print(f"{time.strftime('%F %T')} {msg}", flush=True)


def visible_devices_ok(cuda_visible, count):
    """没有 CUDA_VISIBLE_DEVICES 时只允许单卡，避免 pad 碰到别人的卡。"""
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
        self.train_ref = {d: 0 for d in self.devices}      # 上次变化时训练占的显存
        self.changed = {d: self.start for d in self.devices}  # 训练占的显存上次变化的时刻
        self.seen_start = self._start_mtime()
        self.wait_new = {}  # 收到 <base>.start 后：dev → (放掉前训练占的显存, 时刻)，等新训练起来
        self.stopped = False
        self._steps = 0
        self.initial_ppid = os.getppid()
        self.log(f"启动：job={os.environ.get('SLURM_JOB_ID', '')} host={socket.gethostname()} pid={os.getpid()} "
                 f"target={cfg.target_mib}MiB high={cfg.high_mib}MiB settle={cfg.settle:g}s start_hold={cfg.start_hold:g}s "
                 f"min_train={cfg.min_train_mib}MiB base={cfg.base or '（无）'} devices={self.devices} "
                 f"switch={cfg.switch} parent={cfg.parent_pid} supervisor={cfg.supervisor_pid} dry_run={cfg.dry_run}")

    def _start_mtime(self):
        try:
            return os.path.getmtime(self.c.base + ".start") if self.c.base else 0.0
        except OSError:
            return 0.0

    def _write_mib(self):
        if not self.c.base:
            return
        try:
            with open(self.c.base + ".mib.tmp", "w") as f:
                f.write(f"{sum(self.b.pad_mib(d) for d in self.devices)}\n")
            os.replace(self.c.base + ".mib.tmp", self.c.base + ".mib")
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
        except Exception as e:  # noqa: BLE001  读不到就下一轮再试，不让 pad 退出
            self.log(f"dev {dev}: 读取显存失败（{e}）")
            return
        if total < self.c.min_total_mib:
            if dev not in self.skip_total:
                self.log(f"dev {dev}: total={total}MiB < {self.c.min_total_mib}MiB，不是 80GB A100，不填充")
                self.skip_total.add(dev)
            return
        state, used = self._decide(dev, used, total, fallback, now)
        if now - self.last_heartbeat[dev] >= HEARTBEAT:
            self.log(f"心跳 dev {dev}: used={used}MiB total={total}MiB pad={self.b.pad_mib(dev)}MiB {state}")
            self.last_heartbeat[dev] = now

    def _decide(self, dev, used, total, fallback, now):
        """调整一张卡，返回（状态说明, 调整后的 used）。"""
        train = max(0, used - self.b.pad_mib(dev))
        if abs(train - self.train_ref[dev]) > STABLE_TOL_MIB:
            self.train_ref[dev], self.changed[dev] = train, now
        if dev in self.wait_new:  # 等的时候旧训练结束了：按降下来的值算
            self.wait_new[dev] = (min(self.wait_new[dev][0], train), self.wait_new[dev][1])
        if train < self.c.min_train_mib:
            if self.chunks[dev]:
                used = self._release(dev, 0, reason=f"训练只占 {train}MiB，算没有训练", used=used)
            return f"没有训练（train={train}MiB）", used
        state = f"train={train}MiB"
        if dev in self.wait_new:
            before, t0 = self.wait_new[dev]
            if train >= before + self.c.min_train_mib or now - t0 >= self.c.start_hold:
                del self.wait_new[dev]
            elif used <= self.c.high_mib:
                return f"{state}，等新训练起来（之前 {before}MiB）", used
        target = self.c.target_mib + (FALLBACK_TARGET_MARGIN_MIB if fallback else 0)
        if used > self.c.high_mib:
            used = self._release(dev, self.c.high_mib - self.c.release_hysteresis_mib,
                                 reason=f"used={used}MiB > high={self.c.high_mib}MiB", used=used)
        elif now - self.changed[dev] < self.c.settle:
            return f"{state}，训练显存还在变，等稳定", used
        elif used < target:
            used = self._allocate(dev, target, used, total)
        return state, used

    def _allocate(self, dev, target, used, total):
        if self.c.dry_run:
            self.log(f"dry-run dev {dev}: used={used} target={target}，会补 {target - used}MiB")
            return used
        while used < target:
            if time.time() < self.next_try[dev]:
                return used
            need = target - used
            mib = min(self.c.chunk_mib, need)
            if total - used - mib < self.c.min_free_mib:
                self.log(f"dev {dev}: 再补 {mib}MiB 会少于 min_free={self.c.min_free_mib}MiB，停")
                return used
            try:
                self.chunks[dev].append((self.b.alloc(dev, mib), mib))
            except Exception as e:  # noqa: BLE001  OOM 等：回退，60 秒后再试
                self.log(f"dev {dev}: 分配 {mib}MiB 失败（{e}），60 秒后再试")
                self.next_try[dev] = time.time() + 60
                return used
            used += mib
            self.log(f"dev {dev}: 补 {mib}MiB，pad={self.b.pad_mib(dev)}MiB")
        return used

    def _release(self, dev, limit, reason="", used=None):
        """把 used 降到 limit 以下：算好要放几块，一次放掉再 empty_cache 一次。返回估计的 used。"""
        chunks = self.chunks[dev]
        if not chunks:
            return used
        if used is None:
            try:
                used = self.b.mem(dev)[0]
            except Exception:  # noqa: BLE001  读不到就全部释放
                used = None
        excess = None if used is None or limit <= 0 else used - limit
        if excess is not None and excess <= 0:
            return used
        n, freed = 0, 0
        while chunks and (excess is None or freed < excess):
            _tensor, mib = chunks.pop()
            del _tensor  # 不留引用，empty_cache 前就真正释放
            freed += mib
            n += 1
        self.b.empty_cache(dev)
        self.log(f"dev {dev}: {reason}，释放 {freed}MiB（{n} 块），pad={self.b.pad_mib(dev)}MiB")
        return None if used is None else used - freed

    def step(self):
        if self.switch_on():
            self.log("DISABLED 开关存在，释放并退出")
            return False
        if self.parent_gone():
            self.log("父进程或 supervisor 已退出，释放并退出")
            return False
        now = time.time()
        st = self._start_mtime()
        if st > self.seen_start:  # 要开始新训练：全部放掉，等新训练稳定后再补
            self.seen_start = st
            for dev in self.devices:
                self._release(dev, 0, reason="要开始新训练")
                self.changed[dev] = now
                self.wait_new[dev] = (self.train_ref[dev], now)
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
                self._release(dev, 0, reason="退出")
            self._write_mib()
            self.log("结束")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="DICC GPU 作业显存填充（只占显存，不训练）")
    p.add_argument("--target-mib", type=int, default=env_int("RQ1_GPU_PAD_TARGET_MIB", 42000))
    p.add_argument("--high-mib", type=int, default=env_int("RQ1_GPU_PAD_HIGH_MIB", 62000))
    p.add_argument("--chunk-mib", type=int, default=env_int("RQ1_GPU_PAD_CHUNK_MIB", 1024))
    p.add_argument("--interval", type=float, default=env_float("RQ1_GPU_PAD_INTERVAL", 10.0))
    p.add_argument("--settle", type=float, default=env_float("RQ1_GPU_PAD_SETTLE", 120.0),
                   help="训练占的显存这么多秒没变（±1GiB）才补")
    p.add_argument("--start-hold", type=float, default=env_float("RQ1_GPU_PAD_START_HOLD", 900.0),
                   help="收到 <base>.start 后最多等新训练起来这么多秒")
    p.add_argument("--min-free-mib", type=int, default=env_int("RQ1_GPU_PAD_MIN_FREE_MIB", 8000))
    p.add_argument("--min-total-mib", type=int, default=env_int("RQ1_GPU_PAD_MIN_TOTAL_MIB", 60000))
    p.add_argument("--release-hysteresis-mib", type=int, default=env_int("RQ1_GPU_PAD_HYSTERESIS_MIB", 2048))
    p.add_argument("--min-train-mib", type=int, default=env_int("RQ1_GPU_PAD_MIN_TRAIN_MIB", 2000),
                   help="训练占的显存不到这么多算没有训练，不填")
    p.add_argument("--base", default=default_base(), help="和 dicc_worker.sh 交换信号的文件前缀（<base>.start、<base>.mib）")
    p.add_argument("--switch", default=os.environ.get("RQ1_GPU_PAD_SWITCH",
                                                      os.path.expanduser("~/.cache/phd/gpu_pad/DISABLED")))
    p.add_argument("--parent-pid", type=int, default=None)
    p.add_argument("--supervisor-pid", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--iterations", type=int, default=0)
    p.add_argument("--device", type=int, default=None)
    a = p.parse_args(argv)
    if not 0 < a.target_mib <= a.high_mib:
        p.error("要求 0 < target-mib <= high-mib")
    if a.chunk_mib <= 0 or a.interval < 0:
        p.error("chunk-mib 要 > 0，interval 要 >= 0")
    return a


def main(argv=None):
    a = parse_args(argv)
    if a.dry_run:
        log("dry-run：只读显存，不分配")
    try:
        backend = TorchBackend(device=a.device)
    except Exception as e:  # noqa: BLE001
        log(f"初始化 GPU 失败：{e}")
        return 1
    if backend.device_count() <= 0:
        log("没有可见的 GPU")
        return 1
    if not visible_devices_ok(os.environ.get("CUDA_VISIBLE_DEVICES"), backend.device_count()):
        log("没有 CUDA_VISIBLE_DEVICES 但看到多张卡，拒绝运行（可能碰到别人的卡）")
        return 1
    if a.device is not None and a.device >= backend.device_count():
        log(f"--device {a.device} 超出可见卡数 {backend.device_count()}")
        return 1
    cfg = Config(target_mib=a.target_mib, high_mib=a.high_mib, chunk_mib=a.chunk_mib,
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
