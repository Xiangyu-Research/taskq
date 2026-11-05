"""调度核心的测试用的固定配置（2026-11-02）：机器表、policy（config/）、做法和插件（plugins/）、实验表（exps/demo/）、
假的 conda 和 venv、步骤脚本（bin/）。

    sys.path.insert(0, os.path.join(HERE, "testdata")); import fixture; fixture.use()

use() 在 import 读配置的模块（machines、policy、recipes、relay_plugins、spec、experiments…）之前调用：设 TASKQ_CONFIG、
TASKQ_PLUGINS 指到这里（覆盖已经设了的），并在 PATH 里真的 ssh 前面放 noremote/（ssh、scp 马上失败，测试忘了换假 ssh 时
也连不到真的机器；测试自己的假 ssh 放在更前面照常用），子进程跟着继承。bash 的测试 source fixture.sh。
"""
import atexit
import os
import shutil
import subprocess
import sys
import tempfile

FIX = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(FIX, "config")
PLUGINS = os.path.join(FIX, "plugins")
BIN = os.path.join(FIX, "bin")
EXPS = os.path.join(FIX, "exps")
NOREMOTE = os.path.join(FIX, "noremote")


def _real_ssh(d):
    """文件夹 d 里有真的 ssh（可执行的二进制文件；测试自己写的假 ssh 是脚本）。"""
    p = os.path.join(d or ".", "ssh")
    try:
        with open(p, "rb") as f:
            return os.access(p, os.X_OK) and f.read(4) == b"\x7fELF"
    except OSError:
        return False


def guard_path(path):
    """PATH → 在第一个有真的 ssh 的文件夹前面插上 NOREMOTE（已经有了、或者没有真的 ssh 时原样）。"""
    parts = path.split(os.pathsep) if path else []
    if NOREMOTE in parts:
        return path
    for i, d in enumerate(parts):
        if _real_ssh(d):
            return os.pathsep.join(parts[:i] + [NOREMOTE] + parts[i:])
    return path


def use():
    os.environ["TASKQ_CONFIG"] = CONFIG
    os.environ["TASKQ_PLUGINS"] = PLUGINS
    os.environ["PATH"] = guard_path(os.environ.get("PATH", ""))
    return CONFIG, PLUGINS


def tmpdir(**kw):
    """tempfile.mkdtemp(**kw)，建它的进程退出时（通过、失败、出错都算）删掉；测试改成读不了的先放开权限。"""
    d, pid = tempfile.mkdtemp(**kw), os.getpid()

    def rm():
        if os.getpid() == pid:
            subprocess.run(["chmod", "-R", "u+rwx", d], capture_output=True)
            shutil.rmtree(d, ignore_errors=True)
    atexit.register(rm)
    return d


def dead_pid():
    """一个已经结束、回收过的子进程的 pid（os.kill(pid, 0) 报 ProcessLookupError）。不用 999999 这类"多半不存在"的号：
    有的沙箱对看不到的 pid 报 PermissionError，按 POSIX 那算"进程在"。"""
    p = subprocess.Popen(["true"])
    p.wait()
    return p.pid


if __name__ == "__main__" and sys.argv[1:2] == ["path"]:  # fixture.sh 用
    print(guard_path(sys.argv[2] if len(sys.argv) > 2 else os.environ.get("PATH", "")))
