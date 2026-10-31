"""执行机（envs/machines.py 的 SITES）共用的小工具：在那边跑命令、根目录的写法（2026-10-31）。"""
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "envs"))
sys.path.insert(0, os.path.join(REPO, "scripts", "queue"))
import machines as M  # noqa: E402
import common as C  # noqa: E402

RUN = None  # 测试替换：fn(ssh名, 命令, input, timeout) → (退出码, 标准输出, 标准错误)


def conf(name):
    """执行机（RELAY、SITES）或 MACHINES 里的一台（envs/machines.py 的 machine）。"""
    return M.machine(name)


def ssh_argv():
    return shlex.split(os.environ.get("RELAY_SSH", "ssh")) + C.SSH[1:]


def rsh(name, cmd, input=None, timeout=300):
    host = conf(name)["ssh"]
    if RUN is not None:
        return RUN(host, cmd, input, timeout)
    return C.run(cmd, host, timeout, input=input, ssh=ssh_argv())


def q(path):
    """路径的 shell 写法：开头的 ~ 留给那边展开（根目录里不写用户名），其余部分加引号。"""
    if path == "~":
        return "~"
    if path.startswith("~/"):
        return "~/" + shlex.quote(path[2:])
    return shlex.quote(path)


def unreachable(rc):
    return rc in (124, 255)
