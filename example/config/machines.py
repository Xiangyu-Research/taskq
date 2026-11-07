"""示例的机器表：一台物理机器一项（字段见 queue/schema.py，taskq machines fields）。
  hub      总队列（hub）和结果的家（store）：在哪台跑示例，哪台就是它（hostname 认本机）
  gpubox   pueue 执行机：0 号卡一个 GPU worker，另有一个 CPU worker
  cluster  Slurm 执行机：交 GPU 作业
执行机的主机名是假的（.invalid），连不上，只看的命令照样能用。完整的写法见 queue/testdata/config/machines.py。
"""
import re
import socket

LOCAL = re.escape(socket.gethostname().split(".")[0])
CONDA = ("~/miniforge3", "base")
ENVS = {"base": ("conda", "base")}

MACHINES = {
    "hub": dict(label="hub", hostname=LOCAL, ssh=None, roles=("hub", "store"),
                root="~/taskq-example/root", data="~/taskq-example/data"),
    "gpubox": dict(label="GPU 工作站", hostname=r"gpubox", ssh="gpubox.example.invalid", scheduler="pueue",
                   root="/srv/taskq", conda=CONDA, envs=ENVS, workers=[dict(gpu="0"), dict(cpus=4)]),
    "cluster": dict(label="集群", hostname=r"(login|gpu)\d+", ssh="login.cluster.example.invalid", scheduler="slurm",
                    root="~/q", cap=90, budget=(15, 30), conda=CONDA, envs=ENVS,
                    jobs=dict(gpu=dict(partition="gpu", gres="gpu:1", cpus=8, mem="32G"))),
}
