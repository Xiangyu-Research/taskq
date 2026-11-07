"""测试用的机器表：形状和字段同真的机器表（字段见 queue/schema.py，taskq machines fields），一台物理机器一项：
  hub          总队列、store（结果的家）所在的机器（测试在哪台跑，哪台就是它）；根目录放 store 跑操作用的代码包
  site-pueue   pueue 执行机：1 号卡全天、0 号卡按时段（别人用过要空满 60 分钟），另有一个 CPU worker
  site-slurm   Slurm 执行机（GPU 和 CPU 作业），也放归档的备份
  site-slurm2  另一个 Slurm 账号（只有 GPU 作业；和 site-slurm 主机名一样，靠 TASKQ_MACHINE 认）
ssh 名字（fake-…）、环境名、目录、作业参数是虚构的。conda 指向 testdata/conda（假的 conda.sh：activate 只记下环境名）。
"""
import os
import re
import socket

FAKE = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if "__file__" in globals() else "/nonexistent"
FAKE_CONDA = FAKE + "/conda"
WS_TASK_VARS = {"DEMO_FAST": "1", "DEMO_FAST_LOADER": "1"}
LOCAL = re.escape(socket.gethostname().split(".")[0])
WS1_ROOT = "/mnt/disk1/datasets/ee09fdd2"
SLURM_ENVS = {"base": ("conda", "base0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
              "alt2": ("venv", FAKE + "/venv/alt2", "base"), "x3": ("venv", FAKE + "/venv/x3", "base"),
              "x1": ("venv", FAKE + "/venv/x1", "base"), "stats": ("conda", "stats0")}

MACHINES = {
    "hub": dict(label="hub", hostname=r"hub-host|" + LOCAL, ssh=None, roles=("hub", "store"), repo="~/auto/demo",
                root="~/demo_root", data="/data/others/DEMO_DATA", conda=(FAKE_CONDA, "base0"),
                envs={"base": ("conda", "base0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                      "alt2": ("venv", FAKE + "/venv/alt2", "base"), "stats": ("conda", "stats0"),
                      "x1": ("venv", FAKE + "/venv/x1", "base"), "x2": ("conda", "x2")},
                manual_groups={"gpu": (1, "0", {"TASK_WAIT_GPU": "600"})},
                disk=("/data/others/DEMO_DATA", 100, None),
                transfer=("https://transfer.example.org", "172.18.0.1:18090"),
                quick=("cloudflare/cloudflared:2026.10.4", r"https://[a-z0-9-]+\.trycloudflare\.com")),
    "site-pueue": dict(label="pueue", hostname=r"gpu-host", ssh="fake-pueue-host", scheduler="pueue", root=WS1_ROOT,
                       conda=(FAKE_CONDA, "w0"), envs={"base": ("conda", "w0"), "alt": ("venv", FAKE + "/venv/alt", "base"),
                                                       "alt2": ("venv", FAKE + "/venv/alt2", "base")},
                       workers=[dict(gpu="1"),
                                dict(gpu="0", hours=[("09:00", "09:00", None, None)], idle=60),  # 全天、首尾相接：结果和现在几点无关
                                dict(cpus=8)],
                       disk=("/mnt/disk1", 40, 15), gpu_log=True, tunnel=("push", "pull"), budget=(100, 50),
                       task_vars=WS_TASK_VARS),
    "site-slurm": dict(label="Slurm", hostname=r"(login|gpu|cpu)\d+", ssh="fake-slurm-login", scheduler="slurm", root="~/q",
                       cap=85, budget=(15, 30), conda=(FAKE_CONDA, "base0"), envs=SLURM_ENVS,
                       roles=("backup",), backup="~/st/DEMO_DATA",
                       jobs=dict(gpu=dict(partition="gpu-a", gres="gpu:a:1"), cpu=dict(partition="cpu-b")),
                       ssh_master=True, tunnel=("push", "pull"),
                       transfer_job="-p cpu-part --qos=normal -c 2 --mem=2G",
                       env_job="-p cpu-part --qos=normal -c 8 --mem=16G -t 120"),
    "site-slurm2": dict(label="Slurm2", hostname=r"(login|gpu|cpu)\d+", ssh="fake-site-slurm2", scheduler="slurm", root="~/q",
                        cap=90, budget=(15, 30), conda=(FAKE_CONDA, "pt"), envs={"base": ("conda", "pt")},
                        jobs=dict(gpu=dict(partition="gpu-a", gres="gpu:a:1", cpus=30, mem="60G")), ssh_master=True,
                        tunnel=("push", "pull"), quick_tunnel=True,
                        transfer_job="-p cpu-part --qos=normal -c 4 --mem=4G"),
}
