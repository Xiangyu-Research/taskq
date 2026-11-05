"""测试用的白话（2026-11-05）：实验表 demo 里步骤的中文名、进度报告里不列的步骤、分析编号的标题（monitor/plain.py 用）。"""

STEPS = {"eval": "评测", "oof": "分份汇总", "paired": "配对比较", "ens": "集成评测", "infer": "推理", "lprob": "概率汇总",
         "prune": "删单份概率图", "p1": "P1 探针", "p2": "P2 探针", "p2cmp": "P2 的比较", "t18cmp": "后续比较", "edit": "改写",
         "compare_all": "对比表"}
QUIET = ["infer", "prune"]


def PLAN_TITLES():
    return {"X1": "示例问题一", "X12": "示例问题十二"}
