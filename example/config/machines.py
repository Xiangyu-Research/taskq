"""示例的机器表：一台 hub（总队列在它上面）、两台执行机（一台 pueue、一台 Slurm）。字段说明见核心的 queue/README.md 的
"开关和配置"，完整的写法见 queue/testdata/config/machines.py。
hub 的 hostname 认本机：在哪台跑示例，哪台就是 hub。执行机的主机名是假的（.invalid），连不上，只看的命令照样能用。
"""
import re
import socket

LOCAL = re.escape(socket.gethostname().split(".")[0])

MACHINES = {
    "hub": dict(label="hub", hostname=LOCAL, ssh=None, scheduler="pueue", repo=None, code=None,
                data="~/taskq-example/data", conda=("~/miniforge3", "base"), envs={"base": ("conda", "base")},
                groups={"gpu": (1, "0", {}), "cpu": (2, None, {})}, roles=("hub",), disk=None, gpu_log=False),
}

SITES = {
    "gpubox": dict(label="GPU 工作站", hostname=r"gpubox", ssh="gpubox.example.invalid", scheduler="pueue",
                   root="/srv/taskq", conda=("~/miniforge3", "base"), envs={"base": ("conda", "base")}, roles=("train",),
                   groups={"gpu0": (1, "0", {})}, worker=dict(group="gpu0", id="w0"), slots=1,
                   pueue="~/.local/bin/pueue", budget=(100, 50), relay=True),
    "cluster": dict(label="集群", hostname=r"(login|gpu)\d+", ssh="login.cluster.example.invalid", scheduler="slurm",
                    root="~/q", cap=90, budget=(15, 30), conda=("~/miniforge3", "base"), envs={"base": ("conda", "base")},
                    roles=("train",), jobs=("gpu",), sbatch=dict(gpu=dict(partition="gpu", gres="gpu:1", cpus=8, mem="32G")),
                    relay=True),
}
RELAY = {k: v for k, v in SITES.items() if v.get("relay")}
