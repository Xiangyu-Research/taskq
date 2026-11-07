"""实例（实验表编译出的一个个要做的事，flow/compile.py）。路径都相对数据目录。
源头（SOURCE）是实验展开出来、进总队列训练的任务，id 是它的队列 key；步骤的实例 id 也是它进总队列的 key：
每个任务的步骤 <步骤>/<实验 id>_<简称>，实验一级的 <步骤>/<实验 id>，FINAL 的 <步骤>。"""
from dataclasses import dataclass, field

SOURCE, EACH, GATHER, GLOBAL = "source", "each", "gather", "global"


@dataclass
class Instance:
    id: str
    scope: str                  # source | each | gather | global
    rule: str                   # 步骤名；源头是训练的做法
    exp: str = ""
    names: dict = field(default_factory=dict)    # 模板名字（{data} 留着）
    deps: list = field(default_factory=list)     # 先要做完的实例 id：同一个任务里前面的，加上输入落在它产出里的
    outputs: list = field(default_factory=list)  # 归它的路径（第一个是主目录，done 相对它）
    shared: list = field(default_factory=list)   # 几个源头都会写的同一个文件（训练的 fetch.extra）：不归谁，也不算原始数据
    inputs: list = field(default_factory=list)   # 声明的输入（可以带通配）
    raw: list = field(default_factory=list)      # 输入里不在任何产出里的：原始数据，要在 store 上已经有
    task: str = ""              # each：所属源头任务的 key
    spec: dict = None           # 源头：任务说明
    step: dict = None           # 步骤：实验表里的那一步（补好默认值）
    lab: str = ""               # 源头任务的简称（<实验>_<简称>）
    each: dict = None           # gather：实验的 each

    @property
    def key(self):
        return self.id


@dataclass
class Flow:
    instances: dict             # id → Instance（按实验表的顺序）
    errors: list = field(default_factory=list)
