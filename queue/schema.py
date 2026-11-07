"""配置的字段表：机器表（machines.py 的 MACHINES）、每台的 worker、policy.py、实验表（实验和步骤）。
每项 (类型, 默认值, 一句说明)。taskq check 按它核对（不认识的字段报错），taskq machines fields 打印它，
README 的配置表由测试核对和它一致。类型的写法见 check.py 的 TYPES。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import proto  # noqa: E402

REQUIRED = object()  # 默认值写它 = 必须写

# ---------------- 机器表：一台物理机器一项 ----------------
MACHINE_FIELDS = {
    # 连接
    "label": ("str", REQUIRED, "提醒、页面上显示的名字"),
    "hostname": ("regex", REQUIRED, "认出本机用的主机名（hostname -s）正则"),
    "ssh": ("str?", None, "hub 连过去的 ssh 名字；None = hub 所在的这台"),
    "ssh_master": ("bool", False, "ssh 只经长连接（ControlMaster）；长连接断着时不连"),
    "tz": ("str?", None, "这台的时区（IANA 名，如 Asia/Kuala_Lumpur）；worker 的 hours 按它算；None = 本机时区"),
    "conda": ("pair", None, "(conda 的位置, 默认环境名)"),
    "pueue": ("str", "~/.local/bin/pueue", "pueue 的位置"),
    "disk": ("disk?", None, "(要查的目录, 剩不到多少 GB 提醒, 剩不到多少 GB 算紧急或 None)"),
    "gpu_log": ("bool", False, "开机启动 GPU 使用记录（taskq gpu usage）"),
    "tunnel": ("tunnel", (), "和 store 之间经隧道传文件的方向：push（store → 这里）、pull（这里 → store）"),
    "quick_tunnel": ("bool", False, "传文件用临时的 trycloudflare 地址"),
    "transfer_job": ("str?", None, "slurm：大文件传输在计算节点上跑的 srun 参数"),
    "env_job": ("str?", None, "slurm：建环境在计算节点上跑的 srun 参数；None = 用 transfer_job"),
    # 角色
    "roles": ("roles", (), "hub（总队列、健康检查）、store（结果的家，数据目录在 data）、backup（归档的备份放在 backup）"),
    "repo": ("str?", None, "hub：cron 用的项目克隆"),
    "data": ("str?", None, "store：数据目录（结果的家）"),
    "backup": ("str?", None, "backup：归档的备份放在哪（数据目录的布局）"),
    "transfer": ("pair?", None, "store：文件服务的 (对外地址, 监听的地址:端口)"),
    "quick": ("pair?", None, "store：临时隧道 (cloudflared 镜像, 认临时地址的正则)"),
    # 执行
    "root": ("str?", None, "根目录（blobs、data、code、queue、state、tmp）；执行机和 store 要有"),
    "scheduler": ("sched?", None, "执行机：pueue（常驻 worker）或 slurm（交 worker 作业）"),
    "workers": ("workers", (), "pueue 执行机：这台的 worker，每项见 WORKER_FIELDS"),
    "jobs": ("jobs", {}, "slurm 执行机：{gpu|cpu: sbatch 选项 dict(partition, gres, cpus, mem)}；QOS、时限、个数在 policy.py 的 SLURM"),
    "gpu_pad": ("triple?", None, "slurm：worker 作业自己开显存填充的 (floor, target, high) MiB"),
    "job_names": ("str?", None, "slurm：作业名的样式（sites/slurm.py 的 STYLES）；None = pair"),
    "envs": ("envs", {}, "{环境名: ('conda', 名) | ('venv', 目录, 基础环境) | 'spec' | ('spec', 变体)}"),
    "cap": ("num?", None, "根目录最多占多少 GB（有配额的家目录）"),
    "blob_cap": ("num?", None, "blobs 指到另一个盘时，blobs 最多占多少 GB"),
    "budget": ("pair", (100, 50), "(盘剩不到多少 GB 开始清理, 传完至少还剩多少 GB)"),
    "task_vars": ("strmap", {}, "在这台跑的任务额外设的环境变量"),
    "task_unset": ("strs", (), "在这台跑的任务里去掉的环境变量"),
    "manual_groups": ("groups", {}, "手动用的 pueue 组 {组名: (同时跑几个, 卡号或 None, {环境变量})}；核心不往里排"),
}

# ---------------- pueue 执行机的一个 worker ----------------
WORKER_FIELDS = {
    "gpu": ("str?", None, "用哪张卡（CUDA_VISIBLE_DEVICES）；None = 纯 CPU 的 worker"),
    "cpus": ("int?", None, "最多用几个核；None = 全部"),
    "slots": ("int", 1, "同时训几个"),
    "hours": ("hours", (), "只在这些时段里领、训：[(开始 HH:MM, 结束 HH:MM, 第一天 或 None, 最后一天 或 None)]；空 = 全天"),
    "idle": ("int?", None, "别人用过这张卡后，空满这么多分钟才开训"),
    "wait": ("int?", None, "卡被别人占着时最多等多少分钟"),
}

# ---------------- policy.py ----------------
AUTO_SWITCHES = proto.AUTO_SWITCHES

POLICY_FIELDS = {
    "AUTO": ("auto", REQUIRED, "自动的部分开不开：" + "、".join(AUTO_SWITCHES)),
    "PAUSE": ("pause", {}, "{机器: (到什么时候 'YYYY-MM-DD HH:MM' 或 None, 原因)}：暂停期间不连、不派任务，到期自动恢复"),
    "HOLD": ("strs", (), "暂缓的步骤（步骤实例名的通配式，如 'infer/*'）"),
    "PLACE": ("place", {}, "{步骤或训练的做法: [执行机, …] | 'any'}：能去的执行机，顺序是偏好；没写的 = 'any'（有这种 worker 的执行机都行）"),
    "READY": ("intmap", {}, "{机器: 备几个没人领的任务}；没写的 = 1"),
    "SLURM": ("slurm", {}, "{机器: dict(gpu={QOS: (每段时限, 给别的作业留几个)}, cpu=(QOS, 时限, 最多几个))}"),
    "OUTPUTS": ("outputs", REQUIRED, "项目的输出和入口，见 OUTPUT_FIELDS"),
}

OUTPUT_FIELDS = {
    "state": ("str", "~/.local/state/taskq/", "状态文件夹"),
    "entry": ("str?", None, "主循环入口（相对 hub 的 repo）"),
    "cron": ("cron", (), "((时间, 子命令), …)：taskq site setup 写进 hub 的 crontab"),
    "origin": ("str?", None, "hub 的 repo 从哪里克隆"),
    "bundle": ("strs", (), "执行机的代码包另带的文件夹或文件"),
    "bundle_skip": ("strs", (), "不进代码包的（fnmatch 通配）"),
    "bundle_check": ("str?", None, "代码包另用本机这个环境的 python 核对能编译"),
    "experiments": ("str?", None, "实验表的通配路径"),
    "code_dirs": ("strs", (), "日志里代码版本后面标'+改动'时看的目录"),
    "env_script": ("str?", None, "taskq pueue-add 加的任务先 source 的脚本"),
    "name": ("str", "taskq", "推送标题的前缀"),
    "topic": ("str", "~/.config/taskq/ntfy_topic", "放推送频道名的文件（频道名不进仓库）"),
    "logs": ("str", "~", "纯文字日志（relay.log、advance.log…）的文件夹"),
}


# ---------------- 实验表（OUTPUTS["experiments"] 的每个模块：EXPERIMENTS、FINAL） ----------------
EXP_FIELDS = {
    "id": ("str", REQUIRED, "唯一的短名（实例 id、taskq add --exp）"),
    "exp": ("str", "", "实验编号（登记表、看板）"),
    "label": ("str", "", "一句话说明"),
    "model": ("str", "", "模型的名字（登记表分组）"),
    "hours": ("num?", None, "每个任务折算的小时数（登记表的总进度）"),
    "recipe": ("str", REQUIRED, "训练的做法（recipes.py）；PLACE 按它放训练任务"),
    "params": ("dict", {}, "做法的参数"),
    "fields": ("dict", {}, "任务说明里另外覆盖的字段（如 metrics、vars、inputs）"),
    "each": ("dict", {}, "{参数: [取值…]}：按每种组合展开成一组任务；没有就是一个任务"),
    "after": ("steps", (), "每个任务训完以后的步骤（STEP_FIELDS），按顺序做"),
    "group_after": ("steps", (), "实验一级的步骤（STEP_FIELDS）；先后按输入和产出推"),
    "names": ("dict", {}, "步骤模板里另外可用的名字（分析脚本也按 name-of 读）"),
    "nice": ("int", 0, "taskq add --exp 不给 --nice 时的档位"),
    "board": ("dict", {}, "看板用（各表自己定）"),
    "design": ("str", "", "做了什么的中性说明（事实档案用）"),
}

STEP_FIELDS = {
    "name": ("str", REQUIRED, "步骤名（policy.py 的 PLACE、HOLD 按它）"),
    "cmd": ("str", REQUIRED, "在执行机上跑的命令（模板；工作目录是代码包里的项目，{data} 是那边的数据目录）"),
    "out": ("strs", REQUIRED, "产出（相对数据目录；可以几个，第一个是主目录）。一个路径只归一个步骤"),
    "done": ("strs", REQUIRED, "做完的标志：第一个产出下的文件，每一项都要有；'a|b' 有一个就行；可以写 {参数}"),
    "inputs": ("strs", (), "输入（相对数据目录，可以用通配）；落在别的步骤的产出里就接在它后面，跨实验也算；"
                           "实验一级的步骤里 {参数}（each 的）按每个取值展开"),
    "inputs_cmd": ("str?", None, "另外打印输入的命令（每行一个相对数据目录的路径；relay 插件用）"),
    "relay": ("str", "path", "送到执行机时按哪个插件准备输入、核对结果（sites/names.py）"),
    "env": ("strs", REQUIRED, "环境名（一个或几个；和 cmd 一样可以写 {参数}）"),
    "device": ("device", "gpu", "gpu（要卡）或 cpu（纯 CPU）"),
    "res": ("dict", {}, "资源（recipes.py 的 res）"),
    "out_gb": ("num?", None, "产出大约多少 GB（挑机器时留空间；None = proto.OUT_GB）"),
    "temp": ("strs", (), "第一个产出下的中间产物（通配）：用到它的步骤都做完、拉回以后由 store 删掉，记进事件日志"),
    "rerun": ("bool", False, "输入里有比产出新的就重做"),
    "nice": ("int", 0, "进总队列的档位"),
    "only": ("dict?", None, "每个任务的步骤：只对 each 取这些值的任务做（{参数: [取值…]}）"),
}


# ---------------- 做法（recipes.py 的 RECIPES 的每一项）和任务的说明 ----------------
# 做法里 params、open 以外的字段；都可以写 {参数}，还可以用 {data}（执行机的数据目录）、{repo}（代码包里的项目）。
# 任务的参数和字段同名时覆盖这个字段（dict 合并，别的整个换掉）。做法没写的字段取这里的默认值。
RECIPE_FIELDS = {
    "key": ("str", REQUIRED, "任务名（队列、领取记录、登记表都用它，要唯一）"),
    "env": ("strs", REQUIRED, "环境名（机器表每台的 envs；几个用逗号或列表）"),
    "cmd": ("str", REQUIRED, "在代码包里的项目根目录用 bash 跑的命令；重跑要接着上次做（交还以后别的 worker 从存档接着做）"),
    "out": ("str", REQUIRED, "结果文件夹（相对数据目录）：拉回、归档、登记表都看这里"),
    "done": ("strs", REQUIRED, "做完的标志：out 下的文件，每一项都要有；'a|b' 有一个就行"),
    "vars": ("dict", {}, "跑之前设的环境变量"),
    "log": ("str?", "*.log", "out 下的日志（glob）：多久没更新用来判断有没有人在做"),
    "progress": ("str?", None, "进度的解析方法（spec.py 的 PROGRESS、插件 readers/ 里的；progress_json = out/PROGRESS.json）"),
    "resume": ("strs?", None, "out 下的存档（glob）：有一个就算有进度，交还时留在那台接着做；None = 没有"),
    "res": ("dict", {}, "资源：gpus、cpus、mem、gpu_mem、wait_gpu（分钟）、local_inputs（1 = 开训后拷到节点本地盘）"),
    "owner": ("str?", None, "谁的任务（None = 自己的）"),
    "info": ("str?", None, "训练设置的解析方法（登记表的参数列）"),
    "metrics": ("dict", {}, "登记表的验证指标：{列名: (out 下的 JSON 文件, 键的路径)}"),
    "checkpoint": ("str?", None, "登记表算校验和的文件（out 下）"),
    "inputs": ("strs", [], "执行机上要有的输入（相对数据目录，可以通配）：送过去前传好，开训前再查一遍；实验表步骤的 inputs 就是它"),
    "inputs_cmd": ("str?", None, "另外打印输入的命令（每行一个相对数据目录的路径），在 store 上列要传的输入"),
    "fetch": ("dict", {"exclude": [], "extra": []}, "拉回 store：exclude（不拉的）、extra（out 上一级还要拉的，如 *.json）、"
                                                    "also（另外要拉的文件夹，相对数据目录）"),
    "archive": ("bool", False, "拉回以后备份到 backup 那台、核对"),
    "relay": ("str", "path", "列输入、核对结果用哪个插件：path = 输入就是 inputs 里的（sites/names.py）；别的 = relay_plugins.py 的"),
    "machines": ("strs?", None, "只在这几台执行机上做（和 PLACE 取交集）；None = 不限"),
    "device": ("device", "gpu", "gpu（要卡）或 cpu"),
    "step": ("str?", None, "实验表的步骤名（advance 加的任务有；训练是 None）：PLACE、HOLD、看板按它"),
    "out_gb": ("num?", None, "结果大约多少 GB（挑机器时留空间）；None = proto.OUT_GB"),
}


def defaults(table, entry):
    """entry 补上 table 里没写的默认值（REQUIRED 的不补）。"""
    return {**{k: v[1] for k, v in table.items() if v[1] is not REQUIRED}, **entry}


TABLES = ("MACHINE_FIELDS", "WORKER_FIELDS", "POLICY_FIELDS", "OUTPUT_FIELDS", "EXP_FIELDS", "STEP_FIELDS", "RECIPE_FIELDS")


def markdown(name):
    """一张字段表（TABLES 里的名字）→ Markdown 表（字段、类型、默认值、说明）。README 里贴的就是它的输出
    （taskq machines fields --markdown，test_readme.py 核对）。"""
    def cell(s):
        return str(s).replace("|", "\\|").replace("\n", " ")

    rows = ["| 字段 | 类型 | 默认值 | 说明 |", "|---|---|---|---|"]
    for k, (typ, dflt, doc) in globals()[name].items():
        d = "必写" if dflt is REQUIRED else f"`{cell(repr(dflt))}`"
        rows.append(f"| `{k}` | `{cell(typ)}` | {d} | {cell(doc)} |")
    return "\n".join(rows)
