#!/usr/bin/env python3
"""Test prune-per-part-probabilities script (same interface as the real one): after the ensemble finishes, delete each part's
.prob and record probabilities_removed in each part's MANIFEST.
    prune.py <preds/<model>> --frags "0 1 2 3 4" [--dry-run]
Check first; on any mismatch delete nothing and exit 1: the ensemble's (frags_<...>) MANIFEST exists and each part's MANIFEST exists.
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
bad = [] if os.path.isfile(ens) else [f"ensemble not finished: no {ens}"]
bad += [f"part {k}: no MANIFEST" for k in frags if not os.path.isfile(os.path.join(pred, f"frag_{k}", "MANIFEST.json"))]
if bad:
    print(f"{pred}: check failed, deleting nothing")
    print("\n".join("  " + b for b in bad))
    sys.exit(1)
for k in frags:
    d = os.path.join(pred, f"frag_{k}")
    probs = glob.glob(os.path.join(d, "*", "*.prob"))
    print(f"part {k}: {'would delete' if a.dry_run else 'deleting'} {len(probs)}")
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
