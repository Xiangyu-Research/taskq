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
    progress   进度的解析方法（spec.py 的 PROGRESS，None = 只看训完没、日志多久没更新）
    info       训练设置的解析方法（spec.py 的 INFO，登记表的参数列；None = 不读）
    metrics    登记表的验证指标：{列名: (out 下的 JSON 文件, 键的路径列表)}（实验表里按数据给）
    checkpoint 登记表算校验和的文件（out 下；None = 不算）
    data       训练机上要有的数据（相对数据目录；lab 没有时 refill.py 先传）
    fetch      拉回 box：exclude（不拉的文件）、extra（out 的上一级里还要拉的文件，如 nnU-Net 的 plans.json）
    archive    训练机上训完、拉回、备份核对以后删的文件（drop），删完写 out/ARCHIVED；None = 不归档
    machines   只在这几台机器上跑（envs/machines.py 的名字，如 ["dicc"]）；None = 有环境的都行
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
        info="nnunet_debug",
        metrics={},
        checkpoint="checkpoint_final.pth",
        data=["nnUNet/preprocessed/{dataset}"],
        fetch=dict(exclude=["checkpoint_latest.pth"], extra=["*.json"]),
        archive=dict(drop=["checkpoint_*.pth", "validation/**"], keep=["validation/summary.json"]),
        machines=None,
    ),
    # 什么都由任务自己给（非 nnU-Net 的训练可以直接用，或者照着写一种新的做法）
    "command": dict(
        params=dict(key=REQUIRED, cmd=REQUIRED, env=REQUIRED, out=REQUIRED, done=REQUIRED),
        open=True,
        key="{key}", env="{env}", cmd="{cmd}", vars={}, out="{out}", done="{done}", log="*.log",
        progress=None, info=None, metrics={}, checkpoint=None, data=[], fetch=dict(exclude=[], extra=[]), archive=None,
        machines=None,
    ),
}
