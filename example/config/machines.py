"""Example machine table: one entry per physical machine (fields in queue/schema.py, taskq machines fields).
  hub      hub queue and home of results (store): whichever machine runs the example is this one (hostname matches)
  gpubox   pueue executor: one GPU worker on GPU 0, plus one CPU worker
  cluster  Slurm executor: submits GPU jobs
Executor hostnames are fake (.invalid) and unreachable; read-only commands still work. The full form is in
queue/testdata/config/machines.py.
"""
import re
import socket

LOCAL = re.escape(socket.gethostname().split(".")[0])
CONDA = ("~/miniforge3", "base")
ENVS = {"base": ("conda", "base")}

MACHINES = {
    "hub": dict(label="hub", hostname=LOCAL, ssh=None, roles=("hub", "store"),
                root="~/taskq-example/root", data="~/taskq-example/data"),
    "gpubox": dict(label="GPU workstation", hostname=r"gpubox", ssh="gpubox.example.invalid", scheduler="pueue",
                   root="/srv/taskq", conda=CONDA, envs=ENVS, workers=[dict(gpu="0"), dict(cpus=4)]),
    "cluster": dict(label="cluster", hostname=r"(login|gpu)\d+", ssh="login.cluster.example.invalid", scheduler="slurm",
                    root="~/q", cap=90, budget=(15, 30), conda=CONDA, envs=ENVS,
                    jobs=dict(gpu=dict(partition="gpu", gres="gpu:1", cpus=8, mem="32G"))),
}
