"""执行机（envs/machines.py 的 SITES）共用的小工具：机器表里的一台、每台备几个任务、根目录的写法（2026-10-31）。"""
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg  # noqa: E402
cfg.use_config()
import mtab as M  # noqa: E402
import policy as P  # noqa: E402


def conf(name):
    """执行机（RELAY、SITES）或 MACHINES 里的一台（envs/machines.py 的 machine）。"""
    return M.machine(name)


def ready(name):
    """执行机那边备几个没人领的任务（每种 worker 各几个）：envs/policy.py 的 READY，没写的 1（11/4）。执行机 = envs/machines.py 的 RELAY。"""
    return P.READY.get(name, 1)


def q(path):
    """路径的 shell 写法：开头的 ~ 留给那边展开（根目录里不写用户名），其余部分加引号。"""
    if path == "~":
        return "~"
    if path.startswith("~/"):
        return "~/" + shlex.quote(path[2:])
    return shlex.quote(path)
