"""Test relay plugins: same shape as real domain plugins.
  toy        training: files this run needs under prepared/<ds>/, the whole dir as the view; the fetch checks the validation set (check/)
  toy_infer  inference: model files and test inputs listed by the spec's inputs_cmd (testdata/bin/step.sh --stage needs:infer); the fetch checks MANIFEST"""
import json
import os
import re

from taskq_api import RelayError, expand, listed_inputs, sha256

FIX = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _jload(p):
    with open(p) as f:
        return json.load(f)


# ---------------- training ----------------
def _data_identifier(plans, config):
    c, seen = plans["configurations"].get(config), set()
    while c is not None and "data_identifier" not in c:
        parent = c.get("inherits_from")
        if parent is None or parent in seen:
            break
        seen.add(parent)
        c = plans["configurations"].get(parent)
    if c is None or "data_identifier" not in c:
        raise RelayError(f"no data_identifier for {config} in plans")
    return c["data_identifier"]


def _case_of(name, suffixes, known):
    hits = [name[:-len(s)] for s in suffixes if name.endswith(s) and name[:-len(s)] in known]
    if len(hits) != 1:
        raise RelayError(f"cannot tell which case file {name} belongs to")
    return hits[0]


def export_toy(spec, root):
    n = spec["names"]
    base = f"prepared/{n['ds']}"
    pre = os.path.join(root, base)
    dsj = _jload(os.path.join(pre, "dataset.json"))
    plans = _jload(os.path.join(pre, n["plan"] + ".json"))
    splits = _jload(os.path.join(pre, "splits_final.json"))
    ending = dsj.get("file_ending", ".dat")
    in_splits = {c for s in splits for k in ("train", "val") for c in s[k]}
    gts = sorted(os.listdir(os.path.join(pre, "gt")))
    in_gt = {g[:-len(ending)] for g in gts if g.endswith(ending)}
    if not in_splits <= in_gt or len(in_gt) != len(gts):
        raise RelayError(f"{n['ds']}: splits_final.json and gt cases disagree"
                         f" ({len(in_splits - in_gt)} only in splits, {len(gts) - len(in_gt)} other files in gt)")
    di = _data_identifier(plans, n["cfg"])
    rels = [f"gt/{c}{ending}" for c in sorted(in_splits)]
    for f in sorted(os.listdir(os.path.join(pre, di))):
        if _case_of(f, ("_lab.bin", ".bin", ".meta"), in_gt) in in_splits:
            rels.append(f"{di}/{f}")
    rels += ["dataset.json", n["plan"] + ".json", "splits_final.json", "fingerprint.json"]
    files = []
    for r in rels:
        p = os.path.join(pre, r)
        if not os.path.isfile(p):
            raise RelayError(f"no {base}/{r} on the store")
        files.append((f"{base}/{r}", os.path.realpath(p)))
    return dict(files=files, views=[base])


def check_toy(spec, src, root):
    """Validation cases = this part's val in splits, with the same count in summary.json."""
    n = spec["names"]
    pre = os.path.join(root, "prepared", n["ds"])
    want = set(_jload(os.path.join(pre, "splits_final.json"))[int(n["frag"])]["val"])
    ending = _jload(os.path.join(pre, "dataset.json")).get("file_ending", ".dat")
    vdir = os.path.join(src, spec["out"], "check")
    got = {f[:-len(ending)] for f in os.listdir(vdir) if f.endswith(ending)} if os.path.isdir(vdir) else set()
    summ = os.path.join(vdir, "summary.json")
    npc = len(_jload(summ).get("metric_per_case", [])) if os.path.exists(summ) else -1
    if got != want or npc != len(want):
        miss, extra = sorted(want - got), sorted(got - want)
        raise RelayError(f"{spec['key']}'s validation set does not match: missing {len(miss)} cases {miss[:5]}, extra {len(extra)} cases {extra[:5]}, "
                         f"{npc} in summary.json (expected {len(want)})")


# ---------------- inference (testdata/bin/infer_task.sh) ----------------
def infer_inputs(spec, root):
    """Inputs inference needs (relative to the data dir): paths printed by the spec's inputs_cmd (the infer step's inputs_cmd in
    the experiment table, run at the repo root with the task's vars, {data} = root)."""
    if not spec.get("inputs_cmd"):
        raise RelayError(f"{spec['key']}: inference task has no inputs_cmd")
    out = listed_inputs(spec, root)
    if not out:
        raise RelayError(f"{spec['key']}: the input command listed nothing")
    return out


def export_toy_infer(spec, root):
    files, views = [], []
    for rel in infer_inputs(spec, root):
        fs, v = expand(root, rel)
        files += fs
        views += [v] if v else []
    return dict(files=files, views=views)


IN_RE = re.compile(r"^(.+)_\d{4}\.dat$")  # inference input: <case>_<channel>.dat


def _cases(names, ext):
    return {f[:-len(ext)] for f in names if f.endswith(ext)}


def check_toy_infer(spec, src, root):
    """Each test set's outputs, probabilities and .meta cover the same cases, matching one input dir on the store, and counts
    agree with MANIFEST; checkpoint hashes in MANIFEST match those on the store (sha256)."""
    key, sdir = spec["key"], os.path.join(src, spec["out"])
    if not os.path.isfile(os.path.join(sdir, "MANIFEST.json")):
        raise RelayError(f"{key}'s results have no MANIFEST.json")
    ins = infer_inputs(spec, root)
    dirs = [d for d in ins if os.path.isdir(os.path.join(root, d))]
    free = [{m.group(1) for m in map(IN_RE.match, os.listdir(os.path.join(root, d))) if m} for d in dirs]
    man = _jload(os.path.join(sdir, "MANIFEST.json"))
    sets = sorted(d for d in os.listdir(sdir) if os.path.isdir(os.path.join(sdir, d)))
    if sorted(man.get("cohorts") or {}) != sets:
        raise RelayError(f"{key}: MANIFEST test sets {sorted(man.get('cohorts') or {})} differ from dirs {sets}")
    for c in sets:
        fs = os.listdir(os.path.join(sdir, c))
        outs, probs, metas = _cases(fs, ".out"), _cases(fs, ".prob"), _cases(fs, ".meta")
        other = [f for f in fs if not f.endswith((".out", ".prob", ".meta")) and f not in ("dataset.json", "plans.json", "args.json")]
        if other or any(f.endswith(".tmp.prob") for f in fs):
            raise RelayError(f"{key}'s {c} has unrecognised files: {sorted(other or [f for f in fs if f.endswith('.tmp.prob')])[:5]}")
        v = man["cohorts"][c]
        hit = next((i for i, cs in enumerate(free) if cs == outs), None)
        if hit is None or probs != outs or metas != outs or v.get("n_outputs") != len(outs) or v.get("n_probs") != len(probs):
            raise RelayError(f"{key}'s {c} does not match: outputs {len(outs)}, probabilities {len(probs)}, meta {len(metas)}, "
                             f"MANIFEST says {v.get('n_outputs')} / {v.get('n_probs')}, "
                             f"{'matches an input on the store' if hit is not None else 'no input dir on the store has exactly these cases'}")
        free.pop(hit)
    if free:
        raise RelayError(f"{key}: {len(free)} input dirs on the store have no matching output")
    cks = [r for r in ins if os.path.basename(r) == "final.ckpt"]
    want = [sha256(os.path.join(root, r)) for r in cks]
    sent = [x.get("sha256") for x in man.get("checkpoints") or []]
    if sorted(sent) != sorted(want):
        raise RelayError(f"{key}: the checkpoints used for inference there differ from {cks} on the store")


PLUGINS = {"toy": (export_toy, check_toy), "toy_infer": (export_toy_infer, check_toy_infer)}


def repo_files(variant):
    """The variant reads sample subsets from the repo (SPLITS_DIR) -> True: the bundle there has no such subsets, do not claim."""
    d = os.path.join(FIX, "code")
    for f in os.listdir(d) if variant else []:
        if f.endswith(".py"):
            src = open(os.path.join(d, f), encoding="utf-8").read()
            if f"class {variant}(" in src:
                return "SPLITS_DIR" in src
    return False


def _accepts(params):
    return not repo_files(params.get("variant"))


ACCEPTS = {"toy": _accepts, "toy_infer": _accepts}
VIEW_ROOTS = ["prepared"]
