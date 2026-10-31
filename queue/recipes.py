"""训练的做法（2026-10-18）：一种训练怎么跑、要什么环境和数据、结果在哪、怎么算训完、怎么拉回和归档。
调度（taskq.py、dicc_worker.sh、run_task.sh、refill.py、advance.py、make_registry.py）只按这里的字段办事，不认识具体模型。
解析和使用见 scripts/queue/spec.py；实验表（research/*/experiments.py）和队列里的任务给出参数。

每种做法：
  params     参数和默认值（REQUIRED = 必须给）。默认值里可以用 {别的参数}（如 model）。
  open       True = 任务可以带这里没列的参数（command 做法）；否则写错参数名会报错。
  其他字段    都可以写 {参数}；还可以用 {data}（这台机器的数据目录）、{repo}（仓库）。任务的参数和字段同名时，覆盖这个字段
             （字典字段合并，如 vars=…；列表、字符串整个换掉）。
    key        任务名（队列、领取记录、登记表都用它，要唯一）
    env        环境名（envs/machines.py 每台机器的 envs；没有这个环境的机器不领）
    cmd        在仓库根目录用 bash 跑的命令。要求：重跑就接着上次训（被停掉以后下一个领到的从存档接着训）
    vars       跑之前设的环境变量
    out        结果文件夹（相对数据目录）。拉回、归档、登记表都看这里
    done       训完的标志：out 下的文件，每一项都要有；一项里用 | 分开的有一个就行
    log        out 下的训练日志（glob）：多久没更新用来判断有没有人在训
    progress   进度的解析方法（spec.py 的 PROGRESS：nnunet_log = nnU-Net 的训练日志；progress_json = 任务自己写的
               out/PROGRESS.json，{"done": 已做完的轮数, "total": 总轮数}；None = 只看训完没、日志多久没更新）
    resume     out 下的存档（glob 列表）：有一个就算有进度，失败时留在那台机器上接着做，不交还跳过（2026-10-31）；None = 没有
    res        资源：gpus（卡数）、cpus、mem（如 "32G"）、gpu_mem（GB）；None 的项不限（2026-10-31）
    owner      谁的任务（None = 自己的；别人请帮忙训练、测试的写名字）
    info       训练设置的解析方法（spec.py 的 INFO，登记表的参数列；None = 不读）
    metrics    登记表的验证指标：{列名: (out 下的 JSON 文件, 键的路径列表)}（实验表里按数据给）
    checkpoint 登记表算校验和的文件（out 下；None = 不算）
    data       训练机上要有的输入（相对数据目录的文件或文件夹；送到执行机前先传好）。任务里写 inputs=… 和 data=… 一样（2026-10-31）
    fetch      拉回 box：exclude（不拉的文件）、extra（out 的上一级里还要拉的文件，如 nnU-Net 的 plans.json）、
               also（另外要拉的文件夹，相对数据目录，先于 out 拉）
    archive    训练机上训完、拉回、备份核对以后删的文件（drop），删完写 out/ARCHIVED；None = 不归档
    relay      交给执行机（envs/machines.py 的 RELAY）时数据和结果的化名方式：path = 只换路径、不动文件内容（任何任务都能用）；
               nnunet 等 = 另外改写文件内容（scripts/queue/relay_names.py 的 TRANSLATORS）；None = 不交
    machines   只在这几台机器上跑（envs/machines.py 的名字，如 ["dicc"]）；None = 有环境的都行
    device     gpu = 要卡（DICC 的 GPU worker、lab 领）；cpu = 纯 CPU（只由 DICC 的 CPU worker 领，scripts/queue/cpu_worker.sbatch，
               2026-10-24）
    after      先要做完的任务（队列里的 key 列表）：没做完时这个任务是 waiting，谁都不领；前一个失败、不在队列里时是 blocked
               （2026-10-25，DICC 训的折提前排的推理、评测）
    cache      训练机自己从 data 生成的缓存（10/26，nnU-Net 的解压缓存）：root（相对数据目录）、depth（root 下几层是一份缓存，
               第一层是数据集名）、users（每份缓存里记"哪些数据集用过它"的文件夹，里面一个数据集名一个空文件）。用过它的数据集和
               它自己的数据集都没有在训、领着的折时，refill.py 的 cache_cleanup() 删掉；None = 没有
"""

REQUIRED = object()

RECIPES = {
    # nnU-Net v2 的训练（任何训练器、数据集、plans、配置）
    "nnunet": dict(
        params=dict(dataset=REQUIRED, fold=REQUIRED, trainer="nnUNetTrainer_save10", plans="nnUNetResEncUNetMPlans",
                    config="3d_fullres", env="nnunet", code="research/common/nnunet/trainers",
                    model="{dataset}/{trainer}__{plans}__{config}"),
        key="{dataset}/{trainer}/{fold}",
        env="{env}",
        cmd="bash research/common/nnunet/train.sh {dataset} {config} {fold} {plans} {trainer}",
        vars=dict(nnUNet_raw="{data}/nnUNet/raw", nnUNet_preprocessed="{data}/nnUNet/preprocessed",
                  nnUNet_results="{data}/nnUNet/results", nnUNet_extTrainer="{repo}/{code}"),
        out="nnUNet/results/{model}/fold_{fold}",
        done=["validation/summary.json", "checkpoint_final.pth|ARCHIVED"],
        log="training_log_*.txt",
        progress="nnunet_log",
        resume=["checkpoint_latest.pth", "checkpoint_final.pth"],
        res=dict(gpus=1),
        owner=None,
        info="nnunet_debug",
        metrics={},
        checkpoint="checkpoint_final.pth",
        data=["nnUNet/preprocessed/{dataset}"],
        fetch=dict(exclude=["checkpoint_latest.pth"], extra=["*.json"]),
        archive=dict(drop=["checkpoint_*.pth", "validation/**"], keep=["validation/summary.json"]),
        machines=None,
        device="gpu",
        after=[],
        cache=dict(root="nnUNet/npy_cache", depth=2, users=".used_by"),
        relay="nnunet",
    ),
    # 什么都由任务自己给（非 nnU-Net 的训练可以直接用，或者照着写一种新的做法）
    "command": dict(
        params=dict(key=REQUIRED, cmd=REQUIRED, env=REQUIRED, out=REQUIRED, done=REQUIRED),
        open=True,
        key="{key}", env="{env}", cmd="{cmd}", vars={}, out="{out}", done="{done}", log="*.log",
        progress=None, resume=None, res=dict(gpus=1), owner=None, info=None, metrics={}, checkpoint=None, data=[],
        fetch=dict(exclude=[], extra=[]), archive=None, machines=None, device="gpu", after=[], cache=None, relay="path",
    ),
}
