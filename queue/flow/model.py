"""Instances (the units of work compiled from the experiment table, flow/compile.py). All paths are relative to the data dir.
A source (SOURCE) is a task expanded from an experiment that trains from the hub queue; its id is its queue key. A step's
instance id is also its hub queue key: per-task steps are <step>/<experiment id>_<short>, experiment-level ones
<step>/<experiment id>, FINAL ones <step>."""
from dataclasses import dataclass, field

SOURCE, EACH, GATHER, GLOBAL = "source", "each", "gather", "global"


@dataclass
class Instance:
    id: str
    scope: str                  # source | each | gather | global
    rule: str                   # step name; for a source, the training recipe
    exp: str = ""
    names: dict = field(default_factory=dict)    # template names ({data} left in place)
    deps: list = field(default_factory=list)     # instance ids that must finish first: earlier ones in the same task, plus those whose outputs contain an input
    outputs: list = field(default_factory=list)  # paths owned by it (the first is the main dir; done is relative to it)
    shared: list = field(default_factory=list)   # one file written by several sources (training fetch.extra): owned by none, not raw data either
    inputs: list = field(default_factory=list)   # declared inputs (wildcards allowed)
    raw: list = field(default_factory=list)      # inputs not in any output: raw data, must already exist on the store
    task: str = ""              # each: key of the source task it belongs to
    spec: dict = None           # source: task spec
    step: dict = None           # step: the step from the experiment table (defaults filled in)
    lab: str = ""               # short name of the source task (<experiment>_<short>)
    each: dict = None           # gather: the experiment's each

    @property
    def key(self):
        return self.id


@dataclass
class Flow:
    instances: dict             # id → Instance（按实验表的顺序）
    errors: list = field(default_factory=list)
