"""Test plain words: display names for the demo experiment table's steps, steps not listed in progress reports, and analysis
titles (used by monitor/plain.py)."""

STEPS = {"eval": "evaluation", "oof": "out-of-sample summary", "paired": "paired comparison", "ens": "ensemble evaluation", "infer": "inference", "lprob": "probability summary",
         "prune": "prune per-part probability maps", "p1": "P1 probe", "p2": "P2 probe", "p2cmp": "P2 comparison", "t18cmp": "follow-up comparison", "edit": "rewrite",
         "compare_all": "comparison table"}
QUIET = ["infer", "prune"]


def PLAN_TITLES():
    return {"X1": "example question one", "X12": "example question twelve"}
