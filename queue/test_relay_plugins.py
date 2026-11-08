"""Tests for relay_plugins.py and sites/names.py: the toy and toy_infer plugins in the test config (testdata/) and the generic
path plugin, with a fake dataset, training and inference results, sent and fetched by real names and verified.
Run: python3 queue/test_relay_plugins.py"""
import hashlib
import json
import os
import shutil
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata"))
import fixture  # noqa: E402
TMP = fixture.tmpdir(prefix="test_relay_plugins_")
os.environ.setdefault("TASKQ_STATE", os.path.join(TMP, "state"))  # relay bookkeeping (cfg.state("relay")) in a temp dir
HERE = os.path.dirname(os.path.abspath(__file__))

fixture.use()
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "sites"))
import relay_plugins as RN  # noqa: E402
import names as N  # noqa: E402
import spec as S  # noqa: E402
import cfg  # noqa: E402
QR = cfg.rel("queue")  # queue/ under the repo root (scripts/taskq/queue when nested, queue in a standalone checkout)

DS = "D777_fake"
CASES = ["s101", "s101_b", "s102", "x-00582-000-syn"]
ROOT = os.path.join(TMP, "root")
PRE = os.path.join(ROOT, "prepared", DS)
DI = "PlanM_full"
PM = sys.modules[RN.PLUGINS["toy_infer"][0].__module__]  # the plugin module (only the agreed tables merge into relay_plugins)
REAL_INFER_INPUTS = PM.infer_inputs


def wj(p, obj):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        json.dump(obj, f)


def wb(p, data):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb") as f:
        f.write(data)


def ok(msg):
    print("PASS:" + msg)


def fails(fn, *a, has=""):
    try:
        fn(*a)
    except RN.RelayError as e:
        assert has in str(e), (has, str(e))
        return
    raise AssertionError(f"expected a failure ({has})")


# ---------------- training ----------------
def setup():
    wj(os.path.join(PRE, "dataset.json"), dict(channel_names={"0000": "c0"}, labels={"background": 0, "a": 1},
                                               numTraining=4, file_ending=".dat", name=DS))
    wj(os.path.join(PRE, "PlanM.json"),
       dict(dataset_name=DS, plans_name="PlanM",
            configurations={"full": dict(data_identifier=DI, patch_size=[1, 2, 3]),
                            "custom": dict(inherits_from="full", batch_size=1)}))
    wj(os.path.join(PRE, "splits_final.json"), [dict(train=CASES[2:], val=CASES[:2]), dict(train=CASES[:2], val=CASES[2:])])
    wj(os.path.join(PRE, "fingerprint.json"), dict(spacings=[[1, 1, 1]] * 4))
    wj(os.path.join(PRE, "splits_final_v1.json"), [])
    for c in CASES:
        wb(os.path.join(PRE, "gt", c + ".dat"), b"gt" + c.encode())
        for suf in (".bin", "_lab.bin", ".meta"):
            wb(os.path.join(PRE, DI, c + suf), b"x" * 10 + suf.encode())
    other = os.path.join(ROOT, "prepared", "D716_base", DI)
    os.makedirs(other)
    p = os.path.join(PRE, DI, "s102.bin")
    os.replace(p, os.path.join(other, "s102.bin"))
    os.symlink(os.path.relpath(os.path.join(other, "s102.bin"), os.path.dirname(p)), p)


def test_train():
    sp = S.resolve("toy", dict(ds=DS, frag="0", cfg="custom"))
    assert N.plugin_of(sp) == "toy"
    ex = N.export(sp, ROOT)
    base = f"prepared/{DS}"
    assert ex["params"] == sp["params"] and ex["views"] == [base] and ex["texts"] == {}, ex
    fm = dict(ex["files"])
    want = {f"{base}/{f}" for f in ("dataset.json", "PlanM.json", "splits_final.json", "fingerprint.json")}
    want |= {f"{base}/gt/{c}.dat" for c in CASES}
    want |= {f"{base}/{DI}/{c}{s}" for c in CASES for s in (".bin", "_lab.bin", ".meta")}
    assert set(fm) == want, sorted(set(fm) ^ want)
    assert fm[f"{base}/{DI}/s102.bin"] == os.path.join(ROOT, "prepared", "D716_base", DI, "s102.bin")
    assert ex["outs"] == [sp["out"]] and ex["cleanup"] == [sp["out"]]
    ok("training export: real paths, only the files needed this time (splits_final_v1.json excluded), symlinks sent as their real files, params unchanged")

    wj(os.path.join(PRE, "splits_final.json"), [dict(train=CASES[2:], val=CASES[:2] + ["nobody"])])
    fails(N.export, sp, ROOT, has="disagree")
    wj(os.path.join(PRE, "splits_final.json"), [dict(train=CASES[2:], val=CASES[:2]), dict(train=CASES[:2], val=CASES[2:])])
    ok("training export: fails when splits contain samples without gt")

    src = os.path.join(TMP, "train_src")
    out = os.path.join(src, sp["out"])
    for c in CASES[:2]:
        wb(os.path.join(out, "check", c + ".dat"), b"p")
    wj(os.path.join(out, "check", "summary.json"), dict(metric_per_case=[{}, {}]))
    wb(os.path.join(out, "final.ckpt"), b"ck")
    wb(os.path.join(out, "train_log_1.txt"), b"log")
    wj(os.path.join(os.path.dirname(out), "plans.json"), {})
    wj(os.path.join(os.path.dirname(out), "dataset.json"), {})
    wb(os.path.join(src, "logs", "tasks", S.safe(sp["key"]) + "__1_part1.log"), b"log")
    dst = os.path.join(TMP, "train_dst")
    written = N.import_(sp, src, dst, ROOT)
    par = os.path.dirname(sp["out"])
    assert f"{par}/plans.json" in written and f"{sp['out']}/final.ckpt" in written, written
    assert len(written) == 8 and all(os.path.isfile(os.path.join(dst, w)) for w in written), written
    ok("training fetch: results, parent json (fetch.extra) and part logs land at the same relative paths")

    os.remove(os.path.join(out, "check", CASES[1] + ".dat"))
    fails(N.import_, sp, src, os.path.join(TMP, "train_dst2"), ROOT, has="validation set does not match")
    assert not os.path.exists(os.path.join(TMP, "train_dst2"))
    wb(os.path.join(out, "check", CASES[1] + ".dat"), b"p")
    wj(os.path.join(out, "check", "summary.json"), dict(metric_per_case=[{}]))
    fails(N.import_, sp, src, os.path.join(TMP, "train_dst2"), ROOT, has="summary.json")
    wj(os.path.join(out, "check", "summary.json"), dict(metric_per_case=[{}, {}]))
    ok("training fetch: missing validation samples or a wrong case count in summary.json both fail, leaving the target alone")

    longer = os.path.join(src, "logs", "tasks", S.safe(sp["key"]) + "__x__1_part1.log")
    wb(longer, b"log")
    fails(N.import_, sp, src, os.path.join(TMP, "train_dst2"), ROOT, has="do not belong here")
    os.remove(longer)
    ok("training fetch: another task's part log with a longer key does not belong to this task (reported as unrecognized)")

    os.remove(os.path.join(out, "final.ckpt"))
    fails(N.import_, sp, src, os.path.join(TMP, "train_dst2"), ROOT, has="incomplete")
    wb(os.path.join(out, "final.ckpt"), b"ck")
    wb(os.path.join(src, "prepared", DS, "x.json"), b"{}")
    fails(N.import_, sp, src, os.path.join(TMP, "train_dst2"), ROOT, has="do not belong here")
    os.remove(os.path.join(src, "prepared", DS, "x.json"))
    ok("training fetch: incomplete done markers or unrecognized files both fail")


# ---------------- inference ----------------
TD = "V_save__PlanM__full"
INS = {"raw/holdout/A/inputs": ["A-001", "A-002"],
       "raw/ext/B/inputs": ["B-004"],
       "raw/ext/C/inputs": ["C-010"],
       "raw/ext/C-x/inputs": ["C-010"]}
COH = {"raw/holdout/A/inputs": "A_holdout", "raw/ext/B/inputs": "B",
       "raw/ext/C/inputs": "C", "raw/ext/C-x/inputs": "C-x"}
INFER_SH = f"{QR}/testdata/bin/infer_task.sh"


def infer_setup():
    model = f"results/{DS}/{TD}"
    wj(os.path.join(ROOT, model, "dataset.json"), dict(name=DS))
    wj(os.path.join(ROOT, model, "plans.json"), dict(dataset_name=DS))
    wb(os.path.join(ROOT, model, "frag_0", "final.ckpt"), b"weights")
    store = os.path.join(ROOT, "raw_store")
    for d, cases in INS.items():
        for c in cases:
            for ch in range(4):
                f = f"{c}_{ch:04d}.dat"
                wb(os.path.join(store, d, f), f"{d}/{f}".encode())
                os.makedirs(os.path.join(ROOT, d), exist_ok=True)
                os.symlink(os.path.join(store, d, f), os.path.join(ROOT, d, f))
    needs = [f"{model}/dataset.json", f"{model}/plans.json", f"{model}/frag_0/final.ckpt", *sorted(INS)]
    PM.infer_inputs = lambda spec, root: needs
    return S.resolve("command", dict(key=f"infer/{DS}/{TD}/frag_0", env="base",
                                     cmd=f"bash {INFER_SH} {{data}}/{model} 0",
                                     out=f"preds/{DS}/{TD}/frag_0", done=["MANIFEST.json"], relay="toy_infer",
                                     machines=["site-pueue"]))


def fake_pred(src, sp):
    out = os.path.join(src, sp["out"])
    cohorts = {}
    for d, cases in INS.items():
        c = COH[d]
        for k in cases:
            for ext in (".out", ".prob", ".meta"):
                wb(os.path.join(out, c, k + ext), b"p")
        wj(os.path.join(out, c, "args.json"), {})
        cohorts[c] = dict(n_outputs=len(cases), n_probs=len(cases))
    sha = hashlib.sha256(b"weights").hexdigest()
    wj(os.path.join(out, "MANIFEST.json"), dict(cohorts=cohorts, checkpoints=[dict(path="/elsewhere/frag_0/final.ckpt", sha256=sha)]))
    return out


def test_infer_needs():
    """Run --stage needs for real (it lists even when the model is not on the store yet)."""
    q = dict(key="infer/x", env="base", cmd=f"bash {INFER_SH} {{data}}/results/D9_X/T__P__c 0",
             out="preds/D9_X/T__P__c/frag_0", done=["MANIFEST.json"], relay="toy_infer", vars=dict(SKIP_EXT="", ONLY_EXT="G-A"))
    sp = S.resolve("command", dict(q, inputs_cmd=f"bash {QR}/testdata/bin/step.sh --stage needs:infer {{data}}/results/D9_X/T__P__c 0"))
    os.makedirs(os.path.join(ROOT, "raw"), exist_ok=True)
    got = REAL_INFER_INPUTS(sp, ROOT)
    m = "results/D9_X/T__P__c"
    assert got[:3] == [f"{m}/dataset.json", f"{m}/plans.json", f"{m}/frag_0/final.ckpt"], got
    assert len(got) == 5 and all(g.startswith("raw/") and g.endswith("/inputs") for g in got[3:]), got
    ok("inference: runs the task's inputs_cmd (step.sh --stage needs:infer) with the task vars and lists model files and input dirs")
    fails(REAL_INFER_INPUTS, S.resolve("command", q), ROOT, has="has no inputs_cmd")
    ok("inference: fails without inputs_cmd instead of guessing from the command")


def test_infer():
    sp = infer_setup()
    ex = N.export(sp, ROOT)
    pa = ex["params"]
    assert pa == sp["params"], pa
    assert sorted(ex["views"]) == sorted(INS), ex["views"]
    fm = dict(ex["files"])
    model = f"results/{DS}/{TD}"
    assert fm[f"{model}/frag_0/final.ckpt"] == os.path.join(ROOT, model, "frag_0", "final.ckpt")
    assert fm["raw/holdout/A/inputs/A-001_0000.dat"] == os.path.join(ROOT, "raw_store", "raw/holdout/A/inputs/A-001_0000.dat")
    assert len(fm) == 3 + 4 * sum(map(len, INS.values())) and ex["cleanup"] == [sp["out"]], ex["cleanup"]
    ok("inference export: model files from needs (not views) and input dirs (views) are at real paths, params unchanged")

    src = os.path.join(TMP, "infer_src")
    out = fake_pred(src, sp)
    written = N.import_(sp, src, os.path.join(TMP, "infer_dst"), ROOT)
    assert f"{sp['out']}/MANIFEST.json" in written and f"{sp['out']}/C-x/C-010.prob" in written, written
    ok("inference fetch: every test-set sample is present and matches the store inputs, checkpoint sha256 matches, placed unchanged")

    def bad(why, has, change, undo):
        change()
        fails(N.import_, sp, src, os.path.join(TMP, "infer_dst2"), ROOT, has=has)
        undo()
        assert not os.path.exists(os.path.join(TMP, "infer_dst2")), why

    p = os.path.join(out, "B", "B-004.prob")
    bad("missing probability", "does not match", lambda: os.remove(p), lambda: wb(p, b"p"))
    t = os.path.join(out, "B", "B-004.tmp.prob")
    bad("half-written probability", "unrecognised files", lambda: wb(t, b"p"), lambda: os.remove(t))
    m = os.path.join(out, "MANIFEST.json")
    keep = open(m).read()
    man = json.loads(keep)

    def put(obj):
        return lambda: wj(m, obj)

    def back():
        open(m, "w").write(keep)

    bad("different checkpoint", "checkpoints used for inference", put(dict(man, checkpoints=[dict(sha256="0" * 64)])), back)
    bad("MANIFEST misses a test set", "MANIFEST", put(dict(man, cohorts={k: v for k, v in man["cohorts"].items() if k != "B"})), back)
    bad("MANIFEST counts are wrong", "does not match", put(dict(man, cohorts=dict(man["cohorts"], B=dict(n_outputs=2, n_probs=2)))), back)
    gone = os.path.join(out, "B")
    bad("a test-set dir is missing", "MANIFEST", lambda: shutil.move(gone, gone + ".x"), lambda: shutil.move(gone + ".x", gone))
    d, keep = os.path.join(out, "MANIFEST.json"), open(os.path.join(out, "MANIFEST.json"), "rb").read()
    bad("no done marker MANIFEST.json", "incomplete", lambda: os.remove(d), lambda: wb(d, keep))
    ok("inference fetch: missing probabilities, half-written files, a different checkpoint, MANIFEST/dir or count mismatches and a missing done marker all fail, leaving the target alone")


# ---------------- generic path ----------------
def path_setup():
    """On the store: an input dir (with a symlink pointing elsewhere), an input file, a glob, and an absolute path outside the
    store data dir. → task spec."""
    base = os.path.join(ROOT, "proj", "inputs", "cohortA")
    wb(os.path.join(base, "a", "x_0000.dat"), b"xa")
    wb(os.path.join(ROOT, "elsewhere", "y.dat"), b"yy")
    os.symlink(os.path.join(ROOT, "elsewhere", "y.dat"), os.path.join(base, "y_0000.dat"))
    wb(os.path.join(ROOT, "proj", "lists", "cases.txt"), b"x\ny\n")
    for c in ("c1", "c2"):
        wb(os.path.join(ROOT, "raw", "Set", c, f"{c}-seg.dat"), b"s" + c.encode())
        wb(os.path.join(ROOT, "raw", "Set", c, f"{c}-t1.dat"), b"t")
    ext = os.path.join(TMP, "boxonly", "ref.json")
    wb(ext, b"{}")
    out = "proj/analysis/thing/run1"
    cmd = (f"python3 tools/x.py --in {{data}}/proj/inputs/cohortA --list {{data}}/proj/lists/cases.txt --ref {ext} "
           f"--ref2 {ext}x --in2 {ROOT}/proj/inputs/cohortA --out {{data}}/{out}")
    return S.resolve("command", dict(key="probe/thing_run1", cmd=cmd, env="base", out=out, done=[".done", "a.csv|b.csv"],
                                     inputs=["proj/inputs/cohortA", "proj/lists/cases.txt", "raw/Set/*/*-seg.dat", ext],
                                     relay="path", machines=["site-pueue"], vars=dict(REF=ext),
                                     fetch=dict(exclude=[], extra=["*.json"], also=["proj/analysis/thing/shared"])))


def test_path():
    sp = path_setup()
    assert N.plugin_of(sp) == "path"
    ex = N.export(sp, ROOT)
    pa = ex["params"]
    a_ext = "abs/" + os.path.join(TMP, "boxonly", "ref.json").lstrip("/")
    assert pa["machines"] == ["site-pueue"] and pa["key"] == sp["key"] and pa["out"] == sp["out"], pa
    assert pa["inputs"] == ["proj/inputs/cohortA", "proj/lists/cases.txt", "raw/Set/*/*-seg.dat", a_ext], pa["inputs"]
    assert f"--ref {{data}}/{a_ext} " in pa["cmd"] and f"--ref2 {os.path.join(TMP, 'boxonly', 'ref.json')}x" in pa["cmd"], pa["cmd"]
    assert f"--in2 {ROOT}/proj/inputs/cohortA" in pa["cmd"], "paths inside the data dir but not inputs are left alone"
    assert pa["vars"] == dict(REF="{data}/" + a_ext), pa["vars"]
    assert ex["views"] == ["proj/inputs/cohortA"], ex["views"]
    fm = dict(ex["files"])
    assert fm["proj/inputs/cohortA/y_0000.dat"] == os.path.join(ROOT, "elsewhere", "y.dat")
    assert "proj/lists/cases.txt" in fm and fm[a_ext] == os.path.join(TMP, "boxonly", "ref.json")
    assert {f for f in fm if f.startswith("raw/")} == {"raw/Set/c1/c1-seg.dat", "raw/Set/c2/c2-seg.dat"}, fm
    assert ex["outs"] == [sp["out"], "proj/analysis/thing/shared"] and ex["cleanup"] == ex["outs"]
    ok("generic export: dirs become views, files and expanded globs keep the same relative paths, absolute paths outside the data dir go to abs/ and change in params; similar names are left alone")

    sp2 = dict(sp, inputs=[f"{ROOT}/proj/lists/cases.txt"], params=dict(sp["params"], inputs=[f"{ROOT}/proj/lists/cases.txt"],
                                                                                    cmd=f"cat {ROOT}/proj/lists/cases.txt"))
    ex2 = N.export(sp2, ROOT)
    assert ex2["params"]["inputs"] == ["proj/lists/cases.txt"] and ex2["params"]["cmd"] == "cat {data}/proj/lists/cases.txt", ex2["params"]
    assert ex2["views"] == [] and dict(ex2["files"]) == {"proj/lists/cases.txt": os.path.join(ROOT, "proj/lists/cases.txt")}
    ok("generic export: absolute paths inside the data dir become relative, params use {data}/..., and there are no views when only files are involved")

    fails(N.export, dict(sp, inputs=["raw/Set/*/*-none.dat"]), ROOT, has="no file on the store matches")
    fails(N.export, dict(sp, inputs=["proj/inputs/nothing"]), ROOT, has="no input")
    wb(os.path.join(ROOT, "proj", "analysis", "thing", "old.txt"), b"o")
    fails(N.export, dict(sp, inputs=["proj/analysis/thing"]), ROOT, has="overlaps output")
    fails(N.export, dict(sp, relay=None), ROOT, has="cannot be sent")
    ok("generic export: an unmatched glob, a missing input, an overlapping view and output, or a missing relay all fail")

    src = os.path.join(TMP, "path_src")
    wb(os.path.join(src, sp["out"], "a.csv"), b"1")
    wb(os.path.join(src, sp["out"], "sub", "p.dat"), b"2")
    wb(os.path.join(src, "proj/analysis/thing/shared", "s.txt"), b"3")
    wb(os.path.join(src, "proj/analysis/thing", "plans.json"), b"{}")
    wb(os.path.join(src, "logs", "tasks", S.safe(sp["key"]) + "__1_part1.log"), b"log")
    fails(N.import_, sp, src, os.path.join(TMP, "path_dst"), ROOT, has=".done")
    assert not os.path.exists(os.path.join(TMP, "path_dst"))
    wb(os.path.join(src, sp["out"], ".done"), b"")
    written = N.import_(sp, src, os.path.join(TMP, "path_dst"), ROOT)
    assert sorted(written) == sorted([f"{sp['out']}/a.csv", f"{sp['out']}/sub/p.dat", f"{sp['out']}/.done",
                                      "proj/analysis/thing/shared/s.txt", "proj/analysis/thing/plans.json",
                                      f"logs/tasks/{S.safe(sp['key'])}__1_part1.log"]), written
    ok("generic fetch: out, fetch.also, fetch.extra files in the parent and part logs land at the same relative paths; incomplete done markers fail")
    wb(os.path.join(src, "proj/analysis/thing", "notes.txt"), b"?")
    fails(N.import_, sp, src, os.path.join(TMP, "path_dst3"), ROOT, has="do not belong here")
    os.remove(os.path.join(src, "proj/analysis/thing", "notes.txt"))
    wb(os.path.join(src, "proj/inputs/cohortA", "stray.txt"), b"?")
    fails(N.import_, sp, src, os.path.join(TMP, "path_dst3"), ROOT, has="do not belong here")
    ok("generic fetch: files in the parent not in extra, and files in views, fail")


if __name__ == "__main__":
    setup()
    test_train()
    test_infer_needs()
    test_infer()
    test_path()
    shutil.rmtree(TMP)
    print("all passed")
