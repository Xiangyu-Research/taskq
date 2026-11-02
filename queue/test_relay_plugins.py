"""relay_plugins.py 和 scripts/sites/names.py 的测试：假的数据集、训练结果、推理结果，按真名送出、拉回、核对。
跑法：python3 scripts/queue/test_relay_plugins.py"""
import hashlib
import json
import os
import shutil
import sys
import tempfile

TMP = tempfile.mkdtemp(prefix="test_relay_plugins_")
os.environ["RELAY_DIR"] = os.path.join(TMP, "relay")
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "sites"))
import relay_plugins as RN  # noqa: E402
import names as N  # noqa: E402
import spec as S  # noqa: E402

DS = "Dataset777_FAKEmet"
CASES = ["100101A", "100101A_ig", "100102A", "BraTS-GLI-00582-000-syn"]
ROOT = os.path.join(TMP, "root")
PRE = os.path.join(ROOT, "nnUNet", "preprocessed", DS)
DI = "nnUNetPlans_3d_fullres"
REAL_INFER_INPUTS = RN.infer_inputs


def wj(p, obj):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        json.dump(obj, f)


def wb(p, data):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb") as f:
        f.write(data)


def ok(msg):
    print("通过：" + msg)


def fails(fn, *a, has=""):
    try:
        fn(*a)
    except RN.RelayError as e:
        assert has in str(e), (has, str(e))
        return
    raise AssertionError(f"应报错（{has}）")


# ---------------- 训练 ----------------
def setup():
    wj(os.path.join(PRE, "dataset.json"), dict(channel_names={"0000": "t1n"}, labels={"background": 0, "a": 1},
                                               numTraining=4, file_ending=".nii.gz", name=DS))
    wj(os.path.join(PRE, "nnUNetResEncUNetMPlans.json"),
       dict(dataset_name=DS, plans_name="nnUNetResEncUNetMPlans",
            configurations={"3d_fullres": dict(data_identifier=DI, patch_size=[1, 2, 3]),
                            "3d_custom": dict(inherits_from="3d_fullres", batch_size=1)}))
    wj(os.path.join(PRE, "splits_final.json"), [dict(train=CASES[2:], val=CASES[:2]), dict(train=CASES[:2], val=CASES[2:])])
    wj(os.path.join(PRE, "dataset_fingerprint.json"), dict(spacings=[[1, 1, 1]] * 4))
    wj(os.path.join(PRE, "splits_final_v1.json"), [])
    for c in CASES:
        wb(os.path.join(PRE, "gt_segmentations", c + ".nii.gz"), b"gt" + c.encode())
        for suf in (".b2nd", "_seg.b2nd", ".pkl"):
            wb(os.path.join(PRE, DI, c + suf), b"x" * 10 + suf.encode())
    other = os.path.join(ROOT, "nnUNet", "preprocessed", "Dataset116_base", DI)
    os.makedirs(other)
    p = os.path.join(PRE, DI, "100102A.b2nd")
    os.replace(p, os.path.join(other, "100102A.b2nd"))
    os.symlink(os.path.relpath(os.path.join(other, "100102A.b2nd"), os.path.dirname(p)), p)


def test_train():
    sp = S.resolve("nnunet", dict(dataset=DS, fold="0", config="3d_custom"))
    assert N.plugin_of(sp) == "nnunet"
    ex = N.export(sp, ROOT)
    base = f"nnUNet/preprocessed/{DS}"
    assert ex["params"] == sp["params"] and ex["views"] == [base] and ex["texts"] == {}, ex
    fm = dict(ex["files"])
    want = {f"{base}/{f}" for f in ("dataset.json", "nnUNetResEncUNetMPlans.json", "splits_final.json", "dataset_fingerprint.json")}
    want |= {f"{base}/gt_segmentations/{c}.nii.gz" for c in CASES}
    want |= {f"{base}/{DI}/{c}{s}" for c in CASES for s in (".b2nd", "_seg.b2nd", ".pkl")}
    assert set(fm) == want, sorted(set(fm) ^ want)
    assert fm[f"{base}/{DI}/100102A.b2nd"] == os.path.join(ROOT, "nnUNet", "preprocessed", "Dataset116_base", DI, "100102A.b2nd")
    assert ex["outs"] == [sp["out"]] and ex["cleanup"] == [sp["out"]]
    ok("训练送出：真路径、只列这次要的文件（不含 splits_final_v1.json），软链接按真实文件送，参数原样")

    wj(os.path.join(PRE, "splits_final.json"), [dict(train=CASES[2:], val=CASES[:2] + ["nobody"])])
    fails(N.export, sp, ROOT, has="不一致")
    wj(os.path.join(PRE, "splits_final.json"), [dict(train=CASES[2:], val=CASES[:2]), dict(train=CASES[:2], val=CASES[2:])])
    ok("训练送出：splits 里有 gt_segmentations 没有的病例时报错")

    src = os.path.join(TMP, "train_src")
    out = os.path.join(src, sp["out"])
    for c in CASES[:2]:
        wb(os.path.join(out, "validation", c + ".nii.gz"), b"p")
    wj(os.path.join(out, "validation", "summary.json"), dict(metric_per_case=[{}, {}]))
    wb(os.path.join(out, "checkpoint_final.pth"), b"ck")
    wb(os.path.join(out, "training_log_1.txt"), b"log")
    wj(os.path.join(os.path.dirname(out), "plans.json"), {})
    wj(os.path.join(os.path.dirname(out), "dataset.json"), {})
    wb(os.path.join(src, "logs", "tasks", S.safe(sp["key"]) + "__1_seg1.log"), b"log")
    dst = os.path.join(TMP, "train_dst")
    written = N.import_(sp, src, dst, ROOT)
    par = os.path.dirname(sp["out"])
    assert f"{par}/plans.json" in written and f"{sp['out']}/checkpoint_final.pth" in written, written
    assert len(written) == 8 and all(os.path.isfile(os.path.join(dst, w)) for w in written), written
    ok("训练拉回：结果、上一级的 json（fetch.extra）、分段日志放到同样的相对路径")

    os.remove(os.path.join(out, "validation", CASES[1] + ".nii.gz"))
    fails(N.import_, sp, src, os.path.join(TMP, "train_dst2"), ROOT, has="验证集对不上")
    assert not os.path.exists(os.path.join(TMP, "train_dst2"))
    wb(os.path.join(out, "validation", CASES[1] + ".nii.gz"), b"p")
    wj(os.path.join(out, "validation", "summary.json"), dict(metric_per_case=[{}]))
    fails(N.import_, sp, src, os.path.join(TMP, "train_dst2"), ROOT, has="summary.json")
    wj(os.path.join(out, "validation", "summary.json"), dict(metric_per_case=[{}, {}]))
    ok("训练拉回：验证集缺病例、summary.json 例数不对都报错，目标不动")

    os.remove(os.path.join(out, "checkpoint_final.pth"))
    fails(N.import_, sp, src, os.path.join(TMP, "train_dst2"), ROOT, has="不全")
    wb(os.path.join(out, "checkpoint_final.pth"), b"ck")
    wb(os.path.join(src, "nnUNet", "preprocessed", DS, "x.json"), b"{}")
    fails(N.import_, sp, src, os.path.join(TMP, "train_dst2"), ROOT, has="认不出")
    os.remove(os.path.join(src, "nnUNet", "preprocessed", DS, "x.json"))
    ok("训练拉回：done 不全、有认不出的文件都报错")


# ---------------- 推理 ----------------
TD = "nnUNetTrainer_save10__nnUNetResEncUNetMPlans__3d_fullres"
INS = {"nnUNet/raw/holdout/A/imagesTs": ["A-001", "A-002"],
       "nnUNet/raw/external/B/imagesTs": ["B-004"],
       "nnUNet/raw/external/C/imagesTs": ["C-010"],
       "nnUNet/raw/external/C-noskull/imagesTs": ["C-010"]}
COH = {"nnUNet/raw/holdout/A/imagesTs": "A_holdout", "nnUNet/raw/external/B/imagesTs": "B",
       "nnUNet/raw/external/C/imagesTs": "C", "nnUNet/raw/external/C-noskull/imagesTs": "C-noskull"}


def infer_setup():
    model = f"nnUNet/results/{DS}/{TD}"
    wj(os.path.join(ROOT, model, "dataset.json"), dict(name=DS))
    wj(os.path.join(ROOT, model, "plans.json"), dict(dataset_name=DS))
    wb(os.path.join(ROOT, model, "fold_0", "checkpoint_final.pth"), b"weights")
    store = os.path.join(ROOT, "raw_store")
    for d, cases in INS.items():
        for c in cases:
            for ch in range(4):
                f = f"{c}_{ch:04d}.nii.gz"
                wb(os.path.join(store, d, f), f"{d}/{f}".encode())
                os.makedirs(os.path.join(ROOT, d), exist_ok=True)
                os.symlink(os.path.join(store, d, f), os.path.join(ROOT, d, f))
    needs = [f"{model}/dataset.json", f"{model}/plans.json", f"{model}/fold_0/checkpoint_final.pth", *sorted(INS)]
    sys.modules[REAL_INFER_INPUTS.__module__].infer_inputs = lambda spec, root: needs
    return S.resolve("command", dict(key=f"infer/{DS}/{TD}/fold_0", env="nnunet",
                                     cmd=f"bash research/RQ1/analysis/infer_task.sh {{data}}/{model} 0",
                                     out=f"predictions/{DS}/{TD}/fold_0", done=[".infer_done"], relay="nnunet_infer",
                                     machines=["lab1"], after=[f"{DS}/nnUNetTrainer_save10/0"]))


def fake_pred(src, sp):
    out = os.path.join(src, sp["out"])
    cohorts = {}
    for d, cases in INS.items():
        c = COH[d]
        for k in cases:
            for ext in (".nii.gz", ".npz", ".pkl"):
                wb(os.path.join(out, c, k + ext), b"p")
        wj(os.path.join(out, c, "predict_from_raw_data_args.json"), {})
        cohorts[c] = dict(n_segmentations=len(cases), n_probabilities=len(cases))
    sha = hashlib.sha256(b"weights").hexdigest()
    wj(os.path.join(out, "MANIFEST.json"), dict(cohorts=cohorts, checkpoints=[dict(path="/elsewhere/fold_0/checkpoint_final.pth", sha256=sha)]))
    wb(os.path.join(out, ".infer_done"), b"")
    return out


def test_infer_needs():
    """真跑一次 --stage needs（模型还不在 box 上也能列）。"""
    sp = S.resolve("command", dict(key="infer/x", env="nnunet", cmd="bash research/RQ1/analysis/infer_task.sh {data}/nnUNet/results/Dataset9_X/T__P__3d 0",
                                   out="predictions/Dataset9_X/T__P__3d/fold_0", done=[".infer_done"], relay="nnunet_infer",
                                   vars=dict(SKIP_EXT="", ONLY_EXT="UCSF")))
    os.makedirs(os.path.join(ROOT, "nnUNet", "raw"), exist_ok=True)
    got = REAL_INFER_INPUTS(sp, ROOT)
    m = "nnUNet/results/Dataset9_X/T__P__3d"
    assert got[:3] == [f"{m}/dataset.json", f"{m}/plans.json", f"{m}/fold_0/checkpoint_final.pth"], got
    assert len(got) == 5 and all(g.startswith("nnUNet/raw/") and g.endswith("/imagesTs") for g in got[3:]), got
    ok("推理：真跑 run_predict_eval.sh --stage needs，带任务的 vars，列出模型文件和输入文件夹")


def test_infer():
    sp = infer_setup()
    ex = N.export(sp, ROOT)
    pa = ex["params"]
    assert "machines" not in pa and "after" not in pa and pa["cmd"] == sp["params"]["cmd"], pa
    assert sorted(ex["views"]) == sorted(INS), ex["views"]
    fm = dict(ex["files"])
    model = f"nnUNet/results/{DS}/{TD}"
    assert fm[f"{model}/fold_0/checkpoint_final.pth"] == os.path.join(ROOT, model, "fold_0", "checkpoint_final.pth")
    assert fm["nnUNet/raw/holdout/A/imagesTs/A-001_0000.nii.gz"] == os.path.join(ROOT, "raw_store", "nnUNet/raw/holdout/A/imagesTs/A-001_0000.nii.gz")
    assert len(fm) == 3 + 4 * sum(map(len, INS.values())) and ex["cleanup"] == [sp["out"]], ex["cleanup"]
    ok("推理送出：needs 列的模型文件（不当视图）和输入文件夹（视图）都在真路径，参数原样")

    src = os.path.join(TMP, "infer_src")
    out = fake_pred(src, sp)
    written = N.import_(sp, src, os.path.join(TMP, "infer_dst"), ROOT)
    assert f"{sp['out']}/MANIFEST.json" in written and f"{sp['out']}/C-noskull/C-010.npz" in written, written
    ok("推理拉回：各测试集病例齐、和 box 上的输入一致，checkpoint 的 sha256 对得上，原样放回")

    def bad(why, has, change, undo):
        change()
        fails(N.import_, sp, src, os.path.join(TMP, "infer_dst2"), ROOT, has=has)
        undo()
        assert not os.path.exists(os.path.join(TMP, "infer_dst2")), why

    p = os.path.join(out, "B", "B-004.npz")
    bad("缺概率图", "对不上", lambda: os.remove(p), lambda: wb(p, b"p"))
    t = os.path.join(out, "B", "B-004.tmp.npz")
    bad("写到一半的概率图", "认不出", lambda: wb(t, b"p"), lambda: os.remove(t))
    m = os.path.join(out, "MANIFEST.json")
    keep = open(m).read()
    man = json.loads(keep)

    def put(obj):
        return lambda: wj(m, obj)

    def back():
        open(m, "w").write(keep)

    bad("checkpoint 不同", "checkpoint", put(dict(man, checkpoints=[dict(sha256="0" * 64)])), back)
    bad("MANIFEST 少一个测试集", "MANIFEST", put(dict(man, cohorts={k: v for k, v in man["cohorts"].items() if k != "B"})), back)
    bad("MANIFEST 的数目不对", "对不上", put(dict(man, cohorts=dict(man["cohorts"], B=dict(n_segmentations=2, n_probabilities=2)))), back)
    gone = os.path.join(out, "B")
    bad("少一个测试集的文件夹", "MANIFEST", lambda: shutil.move(gone, gone + ".x"), lambda: shutil.move(gone + ".x", gone))
    d = os.path.join(out, ".infer_done")
    bad("没有 .infer_done", "不全", lambda: os.remove(d), lambda: wb(d, b""))
    ok("推理拉回：缺概率图、写到一半的文件、checkpoint 不同、MANIFEST 和文件夹或数目不一致、没有完成标志都报错，目标不动")


# ---------------- 通用的 path ----------------
def path_setup():
    """box 上：一个输入文件夹（含指向别处的软链接）、一个输入文件、glob、box 数据目录外的绝对路径。→ 任务说明。"""
    base = os.path.join(ROOT, "proj", "inputs", "cohortA")
    wb(os.path.join(base, "a", "x_0000.nii.gz"), b"xa")
    wb(os.path.join(ROOT, "elsewhere", "y.nii.gz"), b"yy")
    os.symlink(os.path.join(ROOT, "elsewhere", "y.nii.gz"), os.path.join(base, "y_0000.nii.gz"))
    wb(os.path.join(ROOT, "proj", "lists", "cases.txt"), b"x\ny\n")
    for c in ("c1", "c2"):
        wb(os.path.join(ROOT, "raw", "Set", c, f"{c}-seg.nii.gz"), b"s" + c.encode())
        wb(os.path.join(ROOT, "raw", "Set", c, f"{c}-t1.nii.gz"), b"t")
    ext = os.path.join(TMP, "boxonly", "ref.json")
    wb(ext, b"{}")
    out = "proj/analysis/thing/run1"
    cmd = (f"python3 research/x.py --in {{data}}/proj/inputs/cohortA --list {{data}}/proj/lists/cases.txt --ref {ext} "
           f"--ref2 {ext}x --in2 {ROOT}/proj/inputs/cohortA --out {{data}}/{out}")
    return S.resolve("command", dict(key="probe/thing_run1", cmd=cmd, env="nnunet", out=out, done=[".done", "a.csv|b.csv"],
                                     data=["proj/inputs/cohortA", "proj/lists/cases.txt", "raw/Set/*/*-seg.nii.gz", ext],
                                     relay="path", machines=["lab1"], vars=dict(REF=ext),
                                     fetch=dict(exclude=[], extra=["*.json"], also=["proj/analysis/thing/shared"])))


def test_path():
    sp = path_setup()
    assert N.plugin_of(sp) == "path"
    ex = N.export(sp, ROOT)
    pa = ex["params"]
    a_ext = "abs/" + os.path.join(TMP, "boxonly", "ref.json").lstrip("/")
    assert "machines" not in pa and pa["key"] == sp["key"] and pa["out"] == sp["out"], pa
    assert pa["data"] == ["proj/inputs/cohortA", "proj/lists/cases.txt", "raw/Set/*/*-seg.nii.gz", a_ext], pa["data"]
    assert f"--ref {{data}}/{a_ext} " in pa["cmd"] and f"--ref2 {os.path.join(TMP, 'boxonly', 'ref.json')}x" in pa["cmd"], pa["cmd"]
    assert f"--in2 {ROOT}/proj/inputs/cohortA" in pa["cmd"], "不在输入里的数据目录内的路径不动"
    assert pa["vars"] == dict(REF="{data}/" + a_ext), pa["vars"]
    assert ex["views"] == ["proj/inputs/cohortA"], ex["views"]
    fm = dict(ex["files"])
    assert fm["proj/inputs/cohortA/y_0000.nii.gz"] == os.path.join(ROOT, "elsewhere", "y.nii.gz")
    assert "proj/lists/cases.txt" in fm and fm[a_ext] == os.path.join(TMP, "boxonly", "ref.json")
    assert {f for f in fm if f.startswith("raw/")} == {"raw/Set/c1/c1-seg.nii.gz", "raw/Set/c2/c2-seg.nii.gz"}, fm
    assert ex["outs"] == [sp["out"], "proj/analysis/thing/shared"] and ex["cleanup"] == ex["outs"]
    ok("通用送出：文件夹当视图、文件和 glob 展开的文件按同样的相对路径，数据目录外的绝对路径放到 abs/、参数里跟着换，名字相近的不换")

    sp2 = dict(sp, inputs=None, data=[f"{ROOT}/proj/lists/cases.txt"], params=dict(sp["params"], data=[f"{ROOT}/proj/lists/cases.txt"],
                                                                                    cmd=f"cat {ROOT}/proj/lists/cases.txt"))
    ex2 = N.export(sp2, ROOT)
    assert ex2["params"]["data"] == ["proj/lists/cases.txt"] and ex2["params"]["cmd"] == "cat {data}/proj/lists/cases.txt", ex2["params"]
    assert ex2["views"] == [] and dict(ex2["files"]) == {"proj/lists/cases.txt": os.path.join(ROOT, "proj/lists/cases.txt")}
    ok("通用送出：数据目录里的绝对路径当相对路径、参数里换成 {data}/…，只有文件时没有视图")

    fails(N.export, dict(sp, inputs=["raw/Set/*/*-none.nii.gz"]), ROOT, has="对得上")
    fails(N.export, dict(sp, inputs=["proj/inputs/nothing"]), ROOT, has="没有输入")
    wb(os.path.join(ROOT, "proj", "analysis", "thing", "old.txt"), b"o")
    fails(N.export, dict(sp, inputs=["proj/analysis/thing"]), ROOT, has="重叠")
    fails(N.export, dict(sp, relay=None), ROOT, has="不能送")
    ok("通用送出：glob 没对上、缺输入、视图和结果重叠、没写 relay 都报错")

    src = os.path.join(TMP, "path_src")
    wb(os.path.join(src, sp["out"], "a.csv"), b"1")
    wb(os.path.join(src, sp["out"], "sub", "p.nii.gz"), b"2")
    wb(os.path.join(src, "proj/analysis/thing/shared", "s.txt"), b"3")
    wb(os.path.join(src, "proj/analysis/thing", "plans.json"), b"{}")
    wb(os.path.join(src, "logs", "tasks", S.safe(sp["key"]) + "__1_seg1.log"), b"log")
    fails(N.import_, sp, src, os.path.join(TMP, "path_dst"), ROOT, has=".done")
    assert not os.path.exists(os.path.join(TMP, "path_dst"))
    wb(os.path.join(src, sp["out"], ".done"), b"")
    written = N.import_(sp, src, os.path.join(TMP, "path_dst"), ROOT)
    assert sorted(written) == sorted([f"{sp['out']}/a.csv", f"{sp['out']}/sub/p.nii.gz", f"{sp['out']}/.done",
                                      "proj/analysis/thing/shared/s.txt", "proj/analysis/thing/plans.json",
                                      f"logs/tasks/{S.safe(sp['key'])}__1_seg1.log"]), written
    ok("通用拉回：out、fetch.also、上一级里 fetch.extra 的文件、分段日志放到同样的相对路径；done 不全时报错")
    wb(os.path.join(src, "proj/analysis/thing", "notes.txt"), b"?")
    fails(N.import_, sp, src, os.path.join(TMP, "path_dst3"), ROOT, has="认不出")
    os.remove(os.path.join(src, "proj/analysis/thing", "notes.txt"))
    wb(os.path.join(src, "proj/inputs/cohortA", "stray.txt"), b"?")
    fails(N.import_, sp, src, os.path.join(TMP, "path_dst3"), ROOT, has="认不出")
    ok("通用拉回：上一级里不在 extra 的、视图里的文件报错")


if __name__ == "__main__":
    setup()
    test_train()
    test_infer_needs()
    test_infer()
    test_path()
    shutil.rmtree(TMP)
    print("全部通过")
