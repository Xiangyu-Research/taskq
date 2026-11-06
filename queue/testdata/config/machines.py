"""测试用的机器表（2026-11-02）：形状和字段同真的机器表（字段说明见 queue/README.md 的"开关和配置"），
机器名按角色起（11/5）：hub（总队列所在的机器）、pueue-host（pueue 执行机所在的主机）、slurm-login（Slurm 集群的登录机），
relay 执行机 site-pueue、site-pueue2（pueue，各一张卡）、site-slurm、site-slurm2（Slurm）；ssh 名字（fake-…）、环境名、目录、作业参数是虚构的。conda 指向 testdata/conda（假的 conda.sh：activate 只记下环境名）。
只有数据；怎么用（whoami、activate 等）和命令行在核心的 sites/mtab.py（11/4）。
"""
import os
import re
import socket

FAKE = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if "__file__" in globals() else "/nonexistent"
FAKE_CONDA = FAKE + "/conda"
WS_TASK_VARS = {"DEMO_FAST": "1", "DEMO_FAST_LOADER": "1"}
LOCAL = re.escape(socket.gethostname().split(".")[0])  # 测试在哪台跑，哪台就是 hub（11/4）
WS1_ROOT = "/mnt/disk1/datasets/ee09fdd2"

MACHINES = {
    "hub": dict(label="hub", hostname=r"hub-host|" + LOCAL, ssh=None, scheduler="pueue", repo="~/auto/demo",
                code="~/.local/state/taskq/code/current",
                data="/data/others/DEMO_DATA", conda=(FAKE_CONDA, "base0"),
                envs={"base": ("conda", "base0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                      "alt2": ("venv", FAKE + "/venv/alt2", "base"), "stats": ("conda", "stats0"),
                      "x1": ("venv", FAKE + "/venv/x1", "base"), "x2": ("conda", "x2")},
                groups={"gpu": (1, "0", {"TASK_WAIT_GPU": "600"}), "cpu": (1, None, {}), "wait": (0, None, {})},
                roles=("hub",), disk=("/data/others/DEMO_DATA", 100, None), gpu_log=False,
                transfer=("https://transfer.example.org", "172.18.0.1:18090"),
                quick=("cloudflare/cloudflared:2026.10.4", r"https://[a-z0-9-]+\.trycloudflare\.com")),
    "pueue-host": dict(label="pueue-host", hostname=r"gpu-host", ssh="fake-pueue-host", scheduler="pueue", repo=None, code=WS1_ROOT + "/code/current",
                conda=(FAKE_CONDA, "w0"),
                envs={"base": ("conda", "w0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                      "alt2": ("venv", FAKE + "/venv/alt2", "base")},
                groups={"gpu1": (1, "1", {}), "gpu0": (1, "0", {}), "cpu": (1, None, {}),
                        "wait": (0, None, {})},
                roles=(), disk=("/mnt/disk1", 40, 15), gpu_log=True, tunnel=("push",)),
    "slurm-login": dict(label="Slurm", hostname=r"(login|gpu|cpu)\d+", ssh="fake-slurm-login", scheduler="slurm", repo="~/Projects/demo", code=None,
                 data="~/st/DEMO_DATA", conda=(FAKE_CONDA, "base0"),
                 envs={"base": ("conda", "base0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                       "alt2": ("venv", FAKE + "/venv/alt2", "base"), "x3": ("venv", FAKE + "/venv/x3", "base"),
                       "x1": ("venv", FAKE + "/venv/x1", "base"), "stats": ("conda", "stats0")},
                 groups={}, roles=("backup",), disk=None, gpu_log=False,
                 tunnel=("push", "pull"),
                 transfer_job="-p cpu-part --qos=normal -c 2 --mem=2G",
                 env_job="-p cpu-part --qos=normal -c 8 --mem=16G -t 120"),
}

SITES = {
    "site-slurm2": dict(label="Slurm2", hostname=r"(login|gpu|cpu)\d+", ssh="fake-site-slurm2", scheduler="slurm", root="~/q", cap=90, budget=(15, 30),
                   conda=(FAKE_CONDA, "pt"), envs={"base": ("conda", "pt")}, roles=("train",),
                   jobs=("gpu",), ssh_master=True, sbatch=dict(gpu=dict(partition="gpu-a", gres="gpu:a:1", cpus=30, mem="60G")),
                   tunnel=("push", "pull"), quick_tunnel=True,
                   transfer_job="-p cpu-part --qos=normal -c 4 --mem=4G", relay=True),
    "site-pueue": dict(label="GPU 1", hostname=r"(?!)", ssh="fake-pueue-host", scheduler="pueue", root=WS1_ROOT,
                 conda=(FAKE_CONDA, "w0"), envs={"base": ("conda", "w0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                       "alt2": ("venv", FAKE + "/venv/alt2", "base")}, roles=("train",),
                 groups={"gpu1": (1, "1", {}), "cpu": (1, None, {})}, worker=dict(group="gpu1", id="w7e2837"), slots=1,
                 cpu_worker=dict(group="cpu", id="w1dadee", cpus=8),
                 pueue="~/.local/bin/pueue", tunnel=("push", "pull"), adopt=["site-pueue2"], budget=(100, 50),
                 task_vars=WS_TASK_VARS, stage_tmp=False, relay=True),
    "site-slurm": dict(label="Slurm", hostname=r"(login|gpu|cpu)\d+", ssh="fake-slurm-login", scheduler="slurm", root="~/q", cap=85, budget=(15, 30),
                  conda=(FAKE_CONDA, "base0"),
                  envs={"base": ("conda", "base0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                        "alt2": ("venv", FAKE + "/venv/alt2", "base"), "x3": ("venv", FAKE + "/venv/x3", "base"),
                        "x1": ("venv", FAKE + "/venv/x1", "base"), "stats": ("conda", "stats0")}, roles=("train",),
                  jobs=("gpu", "cpu"), sbatch=dict(gpu=dict(partition="gpu-a", gres="gpu:a:1"), cpu=dict(partition="cpu-b")),
                  ssh_master=True, tunnel=("push", "pull"),
                  transfer_job="-p cpu-part --qos=normal -c 2 --mem=2G", relay=True),
    "site-pueue2": dict(label="GPU 0", hostname=r"(?!)", ssh="fake-pueue-host", scheduler="pueue", root="/mnt/disk1/datasets/d0eab39f",
                 conda=(FAKE_CONDA, "w0"), envs={"base": ("conda", "w0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                       "alt2": ("venv", FAKE + "/venv/alt2", "base")}, roles=("train",),
                 groups={"gpu0": (1, "0", {"TASK_GPU_IDLE": "60"})}, worker=dict(group="gpu0", id="wd2c85f"), slots=1,
                 pueue="~/.local/bin/pueue", tunnel=("push", "pull"), adopt=["site-pueue"],
                 budget=(100, 50), task_vars=WS_TASK_VARS, stage_tmp=False, relay=True),
}
RELAY = {k: v for k, v in SITES.items() if v.get("relay")}

