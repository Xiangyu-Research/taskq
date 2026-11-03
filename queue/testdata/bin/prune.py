#!/usr/bin/env python3
"""测试用的删单份概率脚本（2026-11-03，接口同真的）：集成做完以后删各份的 .prob，在各份的 MANIFEST 里记 probabilities_removed。
    prune.py <preds/<模型>> --frags "0 1 2 3 4" [--dry-run]
先核对，不对就一个都不删、退出码 1：集成（frags_<…>）的 MANIFEST 在，每份的 MANIFEST 在。
"""
import argparse
import glob
import json
import os
import sys

ap = argparse.ArgumentParser()
ap.add_argument("pred")
ap.add_argument("--frags", required=True)
ap.add_argument("--dry-run", action="store_true")
a = ap.parse_args()
pred = os.path.abspath(a.pred)
frags = a.frags.split()
ens = os.path.join(pred, "frags_" + "-".join(frags), "MANIFEST.json")
bad = [] if os.path.isfile(ens) else [f"集成没做完：没有 {ens}"]
bad += [f"第 {k} 份：没有 MANIFEST" for k in frags if not os.path.isfile(os.path.join(pred, f"frag_{k}", "MANIFEST.json"))]
if bad:
    print(f"{pred}：核对没通过，一个都不删")
    print("\n".join("  " + b for b in bad))
    sys.exit(1)
for k in frags:
    d = os.path.join(pred, f"frag_{k}")
    probs = glob.glob(os.path.join(d, "*", "*.prob"))
    print(f"第 {k} 份：{'要删' if a.dry_run else '删'} {len(probs)} 个")
    if a.dry_run:
        continue
    mp = os.path.join(d, "MANIFEST.json")
    m = json.load(open(mp))
    cohorts = {}
    for f in probs:
        cohorts[os.path.basename(os.path.dirname(f))] = cohorts.get(os.path.basename(os.path.dirname(f)), 0) + 1
        os.remove(f)
    m["probabilities_removed"] = dict(status="done", cohorts=cohorts)
    json.dump(m, open(mp, "w"))
