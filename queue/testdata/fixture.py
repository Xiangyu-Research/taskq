"""调度核心的测试用的固定配置（2026-11-02）：机器表、policy（config/）、做法和插件（plugins/）、实验表（exps/demo/）、
假的 conda 和 venv、步骤脚本（bin/）。

    sys.path.insert(0, os.path.join(HERE, "testdata")); import fixture; fixture.use()

use() 在 import 读配置的模块（machines、policy、recipes、relay_plugins、spec、experiments…）之前调用：设 TASKQ_CONFIG、
TASKQ_PLUGINS 指到这里（覆盖已经设了的），子进程跟着继承。bash 的测试 source fixture.sh。
"""
import atexit
import os
import shutil
import subprocess
import tempfile

FIX = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(FIX, "config")
PLUGINS = os.path.join(FIX, "plugins")
BIN = os.path.join(FIX, "bin")
EXPS = os.path.join(FIX, "exps")


def use():
    os.environ["TASKQ_CONFIG"] = CONFIG
    os.environ["TASKQ_PLUGINS"] = PLUGINS
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
