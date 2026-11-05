"""规则和实例（2026-11-05）。规则 = 一步怎么做；实例 = 规则代入参数后的一个任务。
路径都相对数据目录。实例 id 就是现在的 box 标签（<规则>:<实验>_<简称>，实验一级的 <规则>:<实验>，最后的 <规则>），
源头任务（实验展开出来、进总队列的）的 id 是它的队列 key。"""
from dataclasses import dataclass, field

EACH, GATHER, GLOBAL, SOURCE = "each", "gather", "global", "source"


@dataclass
class Rule:
    name: str
    scope: str                  # each | gather | global | source
    group: str = ""             # 没写 PLACE 时 hub 的哪个组
    cmd: str = ""               # 模板
    done: object = None         # where/ok/mtime/started 的对象；源头任务是 None（看说明的 done）
    queue: dict = None          # 进总队列时的写法（实验表的 queue）
    needs: tuple = ()           # gather：要各实例都做完的 each 规则
    on_change: str = ""         # "rerun"：输入有更新的就重跑（旧的 rerun_if_newer）
    watch: tuple = ()           # on_change 看哪些规则的产出
    rq: str = ""


@dataclass
class Instance:
    id: str
    rule: str
    key: str                    # 旧的 (步骤, key) 里的 key：each 是任务 key，gather 是实验 id，global 是 "all"，源头是任务 key
    exp: str = ""
    names: dict = field(default_factory=dict)
    deps: list = field(default_factory=list)     # 先要做完的实例 id
    qk: str = ""                # 进总队列时的 key
    outputs: list = field(default_factory=list)  # 归它的路径（文件或目录）
    shared: list = field(default_factory=list)   # 几个实例都会产出的同一个文件
    mutates: list = field(default_factory=list)  # 会改别的实例的产出（不归它）
    inputs: list = field(default_factory=list)   # 声明的输入（只用来核对先后）
    marker: str = ""            # 做完的标志
    task: str = ""              # each：所属源头任务的 key


@dataclass
class Flow:
    rules: dict                 # 名字 → Rule
    instances: dict             # id → Instance（按实验表的顺序）
    errors: list = field(default_factory=list)

    def by_key(self, rule, key):
        """旧的 (步骤, key) → 实例。"""
        return self._idx().get((rule, key))

    def _idx(self):
        if not hasattr(self, "_sk"):
            self._sk = {(i.rule, i.key): i for i in self.instances.values()}
        return self._sk
