"""relay_names.py 的测试：假的数据集和训练结果，改名送出、改回拉回。
跑法：python3 scripts/queue/test_relay_names.py（checkpoint 那一段用本机的 nnunet 环境）"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

TMP = tempfile.mkdtemp(prefix="test_relay_names_")
os.environ["RELAY_DIR"] = os.path.join(TMP, "relay")
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "..", "envs"))
import relay_names as RN  # noqa: E402
import relay_bundle as RB  # noqa: E402
import spec as S  # noqa: E402
import machines as M  # noqa: E402

REAL = "Dataset777_FAKEmet"
CASES = ["100101A", "100101A_ig", "100102A", "BraTS-GLI-00582-000-syn"]
ROOT = os.path.join(TMP, "root")
PRE = os.path.join(ROOT, "nnUNet", "preprocessed", REAL)
DI = "nnUNetPlans_3d_fullres"


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


def make_case(c):
    wb(os.path.join(PRE, "gt_segmentations", c + ".nii.gz"), b"gt" + c.encode())
    for suf in (".b2nd", "_seg.b2nd", ".pkl"):
        wb(os.path.join(PRE, DI, c + suf), b"x" * 10 + suf.encode())


def setup():
    for n in (900, 901):
        os.makedirs(os.path.join(ROOT, "nnUNet", "raw", f"Dataset{n}_other"))
    wj(os.path.join(PRE, "dataset.json"), dict(channel_names={"0000": "t1n"}, labels={"background": 0, "a": 1},
                                               numTraining=4, file_ending=".nii.gz", name=REAL, description="说明 " + REAL))
    wj(os.path.join(PRE, "nnUNetResEncUNetMPlans.json"),
       dict(dataset_name=REAL, plans_name="nnUNetResEncUNetMPlans",
            configurations={"3d_fullres": dict(data_identifier=DI, patch_size=[1, 2, 3]),
                            "3d_custom": dict(inherits_from="3d_fullres", batch_size=1)}))
    wj(os.path.join(PRE, "splits_final.json"),
       [dict(train=CASES[2:], val=CASES[:2]), dict(train=CASES[:2], val=CASES[2:])])
    wj(os.path.join(PRE, "dataset_fingerprint.json"), dict(spacings=[[1, 1, 1]] * 4, _note="来自 " + REAL))
    wj(os.path.join(PRE, "splits_final_v1.json"), [])
    for c in CASES:
        make_case(c)
    other = os.path.join(ROOT, "nnUNet", "preprocessed", "Dataset116_base", DI)
    os.makedirs(other)
    p = os.path.join(PRE, DI, "100102A.b2nd")
    os.replace(p, os.path.join(other, "100102A.b2nd"))
    os.symlink(os.path.relpath(os.path.join(other, "100102A.b2nd"), os.path.dirname(p)), p)


def test_export():
    names = RN.Names()
    sp = S.resolve("nnunet", dict(dataset=REAL, fold="0", config="3d_custom"))
    ex = RN.export(sp, ROOT, names)
    alias = ex["params"]["dataset"]
    assert alias.startswith("Dataset902_") and len(alias) == len("Dataset902_") + 6, alias
    assert names.real_dataset(alias) == REAL
    base = f"nnUNet/preprocessed/{alias}"
    assert set(ex["texts"]) == {f"{base}/{f}" for f in ("dataset.json", "nnUNetResEncUNetMPlans.json",
                                                         "splits_final.json", "dataset_fingerprint.json")}
    assert len(ex["files"]) == 4 * 4
    words = [REAL] + CASES
    for rel, data in ex["texts"].items():
        assert not any(w.encode() in data for w in words), rel
    for a, r in ex["files"]:
        assert not any(w in a for w in words), a
        assert os.path.isfile(r) and not os.path.islink(r)
    fm = dict(ex["files"])
    assert "Dataset116_base" in fm[f"{base}/{DI}/c0003.b2nd"]
    assert fm[f"{base}/{DI}/c0002_seg.b2nd"].endswith("100101A_ig_seg.b2nd")
    assert fm[f"{base}/gt_segmentations/c0004.nii.gz"].endswith("BraTS-GLI-00582-000-syn.nii.gz")
    dsj = json.loads(ex["texts"][f"{base}/dataset.json"])
    assert dsj["name"] == alias and "description" not in dsj
    assert "_note" not in json.loads(ex["texts"][f"{base}/dataset_fingerprint.json"])
    spl = json.loads(ex["texts"][f"{base}/splits_final.json"])
    assert spl[0] == dict(train=["c0003", "c0004"], val=["c0001", "c0002"])
    ok("送出：别名、文件、改写的小文件里没有真名，软链接取真实路径，带后缀的病例认得出")

    ex2 = RN.export(sp, ROOT, names)
    assert ex2["params"]["dataset"] == alias and ex2["files"] == ex["files"]
    CASES.append("100100A")
    make_case("100100A")
    s = json.load(open(os.path.join(PRE, "splits_final.json")))
    s[0]["train"].append("100100A")
    wj(os.path.join(PRE, "splits_final.json"), s)
    ex3 = RN.export(sp, ROOT, names)
    m = names.case_map(REAL)
    assert m["100100A"] == "c0005" and m["100101A"] == "c0001" and len(ex3["files"]) == 5 * 4
    assert RN.Names().dataset("Dataset778_other", RN.used_ids(ROOT)).startswith("Dataset903_")
    ok("别名稳定：再送一次不变，新病例接着编号，已用的编号不再分配")

    wb(os.path.join(PRE, DI, "zzz.b2nd"), b"")
    try:
        RN.export(sp, ROOT, names)
        raise AssertionError("认不出的文件应报错")
    except RN.RelayError:
        pass
    os.remove(os.path.join(PRE, DI, "zzz.b2nd"))
    try:
        RN.export(dict(sp, relay=None), ROOT, names)
        raise AssertionError("没有 relay 的做法应报错")
    except RN.RelayError:
        pass
    make_case("100199A")
    ex4 = RN.export(sp, ROOT, names)
    assert len(ex4["files"]) == 5 * 4 and "100199A" not in names.case_map(REAL)
    for f in os.listdir(os.path.join(PRE, DI)):
        if f.startswith("100199A"):
            os.remove(os.path.join(PRE, DI, f))
    os.remove(os.path.join(PRE, "gt_segmentations", "100199A.nii.gz"))
    ok("认不出的文件、不能送的做法报错；不在划分里的病例不送")
    return sp, alias


def torch_run(code, payload):
    act = M.activate(M.whoami(), "nnunet")
    r = subprocess.run(["bash", "-c", act + "\nexec python -c '" + code + "'"], input=json.dumps(payload),
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout


def fake_output(src, asp, alias):
    fold = os.path.join(src, asp["out"])
    tdir = os.path.dirname(fold)
    wj(os.path.join(tdir, "plans.json"), dict(dataset_name=alias))
    wj(os.path.join(tdir, "dataset.json"), dict(name=alias))
    wj(os.path.join(tdir, "dataset_fingerprint.json"), dict(spacings=[]))
    rr = "/remote/data/nnUNet"
    mpc = []
    for a in ("c0001", "c0002"):
        for e in (".nii.gz", ".npz", ".pkl"):
            wb(os.path.join(fold, "validation", a + e), b"pred" + a.encode())
        mpc.append(dict(metrics={"1": {"Dice": 0.5}},
                        prediction_file=f"{rr}/results/{alias}/x/fold_0/validation/{a}.nii.gz",
                        reference_file=f"{rr}/preprocessed/{alias}/gt_segmentations/{a}.nii.gz"))
    wj(os.path.join(fold, "validation", "summary.json"), dict(mean={}, metric_per_case=mpc))
    wj(os.path.join(fold, "debug.json"), dict(output_folder=f"{rr}/results/{alias}/x", dataset_name=alias))
    wb(os.path.join(fold, "training_log_2026_11_02_10_00_00.txt"), f"dataset_name: {alias}\nEpoch 1\n".encode())
    wb(os.path.join(fold, "progress.png"), b"png")
    wb(os.path.join(src, "logs", "tasks", S.safe(asp["key"]) + "__12345_seg1.log"), f"run {alias}\n".encode())
    wb(os.path.join(src, "logs", "tasks", "Other__x__0__1_seg1.log"), b"other")
    dsj = dict(channel_names={"0000": "t1n"}, labels={"background": 0, "a": 1}, numTraining=4,
               file_ending=".nii.gz", name=alias)
    torch_run("import json,sys,torch\n"
              "a=json.load(sys.stdin)\n"
              "for f in a[\"files\"]:\n"
              "    torch.save(dict(network_weights={\"w\": torch.zeros(2)}, init_args=dict(plans=dict(dataset_name=a[\"alias\"]), "
              "dataset_json=a[\"dsj\"], fold=0)), f)\n",
              dict(alias=alias, dsj=dsj, files=[os.path.join(fold, f) for f in ("checkpoint_final.pth", "checkpoint_best.pth")]))
    os.utime(os.path.join(fold, "checkpoint_final.pth"), (1.7e9, 1.7e9))


def snapshot(d):
    out = {}
    for dp, _, fs in os.walk(d):
        for f in fs:
            p = os.path.join(dp, f)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, d)] = (fh.read(), os.stat(p).st_mtime)
    return out


def test_import(sp, alias):
    names = RN.Names()
    sp = S.resolve("nnunet", dict(dataset=REAL, fold="0"))
    asp = S.resolve("nnunet", dict(dataset=alias, fold="0"))
    src = os.path.join(TMP, "src")
    fake_output(src, asp, alias)
    before = snapshot(src)
    dst = os.path.join(TMP, "dst")
    written = RN.import_(asp, sp, src, dst, ROOT, names)
    assert snapshot(src) == before
    out = sp["out"]
    assert written[-1] == f"{out}/validation/summary.json"
    for rel in written:
        assert os.path.exists(os.path.join(dst, rel)), rel
    for c in ("100101A", "100101A_ig"):
        for e in (".nii.gz", ".npz", ".pkl"):
            assert os.path.exists(os.path.join(dst, out, "validation", c + e)), c + e
    tdir = os.path.dirname(out)
    assert json.load(open(os.path.join(dst, tdir, "dataset.json")))["description"] == "说明 " + REAL
    assert "_note" in json.load(open(os.path.join(dst, tdir, "dataset_fingerprint.json")))
    assert json.load(open(os.path.join(dst, tdir, "plans.json")))["dataset_name"] == REAL
    s = json.load(open(os.path.join(dst, out, "validation", "summary.json")))
    assert [os.path.basename(m["reference_file"]) for m in s["metric_per_case"]] == ["100101A.nii.gz", "100101A_ig.nii.gz"]
    assert f"/results/{REAL}/" in s["metric_per_case"][0]["prediction_file"]
    assert os.path.exists(os.path.join(dst, "logs", "tasks", S.safe(sp["key"]) + "__12345_seg1.log"))
    assert not any("Other__" in r for r in written)
    assert os.path.exists(os.path.join(dst, out, "progress.png"))
    for dp, _, fs in os.walk(dst):
        for f in fs:
            p = os.path.join(dp, f)
            assert alias not in p and not f.endswith(".relaytmp"), p
            if not f.endswith(".pth"):
                assert alias.encode() not in open(p, "rb").read(), p
    assert abs(os.stat(os.path.join(dst, out, "checkpoint_final.pth")).st_mtime - 1.7e9) < 1
    got = json.loads(torch_run(
        "import json,sys,torch\n"
        "a=json.load(sys.stdin)\n"
        "r=[]\n"
        "for f in a:\n"
        "    ia=torch.load(f,weights_only=False)[\"init_args\"]\n"
        "    r.append([ia[\"plans\"][\"dataset_name\"], ia[\"dataset_json\"][\"name\"], ia[\"dataset_json\"].get(\"description\")])\n"
        "print(json.dumps(r))\n",
        [os.path.join(dst, out, f) for f in ("checkpoint_final.pth", "checkpoint_best.pth")]))
    assert got == [[REAL, REAL, "说明 " + REAL]] * 2, got
    ok("拉回：病例名、数据集名、checkpoint、分段日志都换回真名，源目录不动，summary.json 最后写")

    os.remove(os.path.join(src, asp["out"], "validation", "c0002.nii.gz"))
    dst2 = os.path.join(TMP, "dst2")
    try:
        RN.import_(asp, sp, src, dst2, ROOT, names)
        raise AssertionError("少一例应报错")
    except RN.RelayError as e:
        assert "100101A_ig" in str(e), e
    assert not os.path.exists(dst2)
    ok("验证集少一例时报错，目标目录不动")


# ---------------- 推理（nnunet_infer） ----------------
REAL2, TD = "Dataset778_FAKEgli", "nnUNetTrainer_save10__nnUNetResEncUNetMPlans__3d_fullres"
INS = {"nnUNet/raw/RQ1_holdout/GLI/imagesTs": ["BraTS-GLI-00001-000", "BraTS-GLI-00002-000"],
       "nnUNet/raw/RQ1_external/UCSF/imagesTs": ["UCSF-PDGM-0004"],
       "nnUNet/raw/RQ1_external/PEDs/imagesTs": ["BraTS-PED-00010-000"],
       "nnUNet/raw/RQ1_external/PEDs-noskullstrip/imagesTs": ["BraTS-PED-00010-000"]}
OUTNAME = {"nnUNet/raw/RQ1_holdout/GLI/imagesTs": "GLI_holdout"}


def infer_setup():
    model = os.path.join(ROOT, "nnUNet", "results", REAL2, TD)
    rdsj = dict(channel_names={"0000": "t1n"}, labels={"background": 0, "a": 1}, numTraining=4, file_ending=".nii.gz",
                name=REAL2, description="说明 " + REAL2)
    wj(os.path.join(model, "dataset.json"), rdsj)
    wj(os.path.join(model, "plans.json"), dict(dataset_name=REAL2, plans_name="nnUNetResEncUNetMPlans", note="用 " + REAL2))
    os.makedirs(os.path.join(model, "fold_0"))
    torch_run("import json,sys,torch\n"
              "a=json.load(sys.stdin)\n"
              "torch.save(dict(network_weights={\"w\": torch.ones(2)}, init_args=dict(plans=dict(dataset_name=a[\"real\"]), "
              "dataset_json=dict(a[\"dsj\"], numTraining=5), fold=0)), a[\"f\"])\n",
              dict(real=REAL2, dsj=rdsj, f=os.path.join(model, "fold_0", "checkpoint_final.pth")))
    store = os.path.join(ROOT, "raw_store")
    for d, cases in INS.items():
        for c in cases:
            for ch in range(4):
                f = f"{c}_{ch:04d}.nii.gz"
                wb(os.path.join(store, d, f), f"{d}/{f}".encode())
                os.makedirs(os.path.join(ROOT, d), exist_ok=True)
                os.symlink(os.path.join(store, d, f), os.path.join(ROOT, d, f))
    RN.infer_inputs = lambda spec, root, model_rel, fold: sorted(INS) if model_rel == f"nnUNet/results/{REAL2}/{TD}" else []
    RN.infer_vars = lambda model_rel: dict(SKIP_EXT="MEN", ONLY_EXT="", MODEL_ENV="nnunet")
    return S.resolve("command", dict(key=f"infer/{REAL2}/{TD}/fold_0", env="nnunet",
                                     cmd=f"bash research/RQ1/analysis/infer_task.sh {{data}}/nnUNet/results/{REAL2}/{TD} 0",
                                     out=f"predictions/{REAL2}/{TD}/fold_0", done=[".infer_done"], relay="nnunet_infer",
                                     machines=["lab1"], after=[f"{REAL2}/nnUNetTrainer_save10/0"]))


def test_infer():
    names = RN.Names()
    sp = infer_setup()
    ex = RN.export(sp, ROOT, names)
    alias = names.dataset(REAL2)
    pa = ex["params"]
    asp = S.resolve("command", pa)
    assert asp["key"] == f"infer/{alias}/{TD}/fold_0" and asp["out"] == f"predictions/{alias}/{TD}/fold_0", asp
    assert asp["cmd"] == f"bash research/RQ1/analysis/infer_task.sh {{data}}/nnUNet/models/{alias}/{TD} 0", asp["cmd"]
    assert "machines" not in pa and "after" not in pa and asp["relay"] == "nnunet_infer"
    assert asp["vars"] == dict(SKIP_EXT="MEN", ONLY_EXT="", MODEL_ENV="nnunet"), asp["vars"]
    am = f"nnUNet/models/{alias}/{TD}"
    assert set(ex["texts"]) == {f"{am}/dataset.json", f"{am}/plans.json"}
    dsj = json.loads(ex["texts"][f"{am}/dataset.json"])
    assert dsj["name"] == alias and "description" not in dsj
    words = [REAL2] + [c for v in INS.values() for c in v]
    for rel, data in ex["texts"].items():
        assert not any(w.encode() in data for w in words), rel
    fm = dict(ex["files"])
    assert len(fm) == 1 + 4 * sum(len(v) for v in INS.values()), len(fm)
    for a, r in ex["files"]:
        assert not any(w in a for w in words) and "RQ1" not in a, a
        assert os.path.isfile(r) and not os.path.islink(r)
    rd = {d: RB.rename(d) for d in INS}
    assert ex["views"] == [f"{am}/fold_0"] + [rd[d] for d in sorted(INS)], ex["views"]
    an = RB.rename(f"RQ1/analysis/{alias}/{TD}/fold_0")
    assert ex["cleanup"] == [f"{am}/fold_0", an] and not an.startswith("RQ1"), ex["cleanup"]
    cm = names.case_map(RN.INFER_CASES)
    assert cm["BraTS-PED-00010-000"] == cm["BraTS-PED-00010-000"] and len(set(cm.values())) == len(cm) == 4
    assert fm[f"{rd['nnUNet/raw/RQ1_external/UCSF/imagesTs']}/{cm['UCSF-PDGM-0004']}_0002.nii.gz"].endswith("UCSF-PDGM-0004_0002.nii.gz")
    ack = fm[f"{am}/fold_0/checkpoint_final.pth"]
    got = json.loads(torch_run("import json,sys,torch\n"
                               "ia=torch.load(json.load(sys.stdin),weights_only=False)[\"init_args\"]\n"
                               "print(json.dumps([ia[\"plans\"][\"dataset_name\"], ia[\"dataset_json\"][\"name\"], "
                               "\"description\" in ia[\"dataset_json\"], ia[\"dataset_json\"][\"numTraining\"]]))\n", ack))
    assert got == [alias, alias, False, 5], got
    m0 = os.stat(ack).st_mtime_ns
    ex2 = RN.export(sp, ROOT, names)
    assert ex2["files"] == ex["files"] and os.stat(ack).st_mtime_ns == m0
    ok("推理送出：模型、checkpoint、输入换成别名（几个测试集同一个病例同一个别名），参数里是别名、去掉 machines 和 after，"
       "跳过的外部集和环境放进 vars；别名版 checkpoint 缓存着")

    src = os.path.join(TMP, "isrc")
    aout = os.path.join(src, asp["out"])
    rr = "/remote/data"
    cohorts = {}
    for d in sorted(INS):
        c = OUTNAME.get(d, d.split("/")[-2])
        acs = sorted({os.path.basename(a)[:-len("_0000.nii.gz")] for a in fm if a.startswith(rd[d] + "/")})
        for a in acs:
            for e in (".nii.gz", ".npz", ".pkl"):
                wb(os.path.join(aout, c, a + e), b"pred" + a.encode())
        wj(os.path.join(aout, c, "dataset.json"), dsj)
        wj(os.path.join(aout, c, "plans.json"), dict(dataset_name=alias))
        args = dict(list_of_lists_or_source_folder=f"{rr}/{rd[d]}", output_folder_or_list_of_truncated_output_files=f"{rr}/{asp['out']}/{c}")
        wj(os.path.join(aout, c, "predict_from_raw_data_args.json"), args)
        cohorts[c] = dict(n_segmentations=len(acs), n_probabilities=len(acs), nnunet_args=args)
    asha = RN._sha256(ack)
    man = dict(model_dir=f"{rr}/{am}", dataset=alias, trainer=TD, folds=[0],
               checkpoints=[dict(fold=0, file=f"{rr}/{am}/fold_0/checkpoint_final.pth", bytes=1, mtime="x", sha256=asha)],
               machine="remote", cohorts=cohorts, probabilities_zeroed_below=dict(threshold=1e-5))
    wj(os.path.join(aout, "MANIFEST.json"), man)
    wb(os.path.join(aout, ".infer_done"), b"")
    wb(os.path.join(src, "logs", "tasks", S.safe(asp["key"]) + "__7_seg1.log"), f"推理 {alias}\n".encode())
    before = snapshot(src)
    dst = os.path.join(TMP, "idst")
    written = RN.import_(asp, sp, src, dst, ROOT, names)
    assert snapshot(src) == before
    out = sp["out"]
    assert written[-2:] == [f"{out}/MANIFEST.json", f"{out}/.infer_done"], written[-2:]
    back = {a: c for c, a in cm.items()}
    for d, cases in INS.items():
        c = OUTNAME.get(d, d.split("/")[-2])
        for case in cases:
            for e in (".nii.gz", ".npz", ".pkl"):
                assert os.path.exists(os.path.join(dst, out, c, case + e)), (c, case, e)
    for dp, _, fs in os.walk(dst):
        for f in fs:
            p = os.path.join(dp, f)
            assert alias not in p and not f.endswith(".relaytmp") and not any(a in f for a in back), p
            assert alias.encode() not in open(p, "rb").read() or f == "MANIFEST.json", p
    m = json.load(open(os.path.join(dst, out, "MANIFEST.json")))
    rck = os.path.join(ROOT, "nnUNet", "results", REAL2, TD, "fold_0", "checkpoint_final.pth")
    assert m["dataset"] == REAL2 and m["model_dir"] == os.path.dirname(os.path.dirname(rck))
    assert m["checkpoints"][0]["file"] == rck and m["checkpoints"][0]["sha256"] == RN._sha256(rck) != asha
    assert m["relayed"] == dict(dataset=alias, checkpoint_sha256=asha)
    assert REAL2 in m["cohorts"]["UCSF"]["nnunet_args"]["output_folder_or_list_of_truncated_output_files"]
    assert json.load(open(os.path.join(dst, out, "GLI_holdout", "dataset.json")))["description"] == "说明 " + REAL2
    assert os.path.exists(os.path.join(dst, "logs", "tasks", S.safe(sp["key"]) + "__7_seg1.log"))
    sys.path.insert(0, os.path.join(HERE, "..", "..", "research", "RQ1", "analysis"))
    import write_manifest as WM
    assert WM.incomplete(os.path.join(dst, out, "MANIFEST.json")) is None
    ok("推理拉回：病例名、数据集名换回真名，MANIFEST 记 box 上的 checkpoint（别名版的 sha256 另记），MANIFEST、.infer_done 最后写")

    def fails(why, fix=None):
        d2 = os.path.join(TMP, "idst_bad")
        shutil.rmtree(d2, ignore_errors=True)
        try:
            RN.import_(asp, sp, src, d2, ROOT, names)
            raise AssertionError(why)
        except RN.RelayError:
            pass
        assert not os.path.exists(d2)
        if fix:
            fix()
    a1 = cm["UCSF-PDGM-0004"]
    p = os.path.join(aout, "UCSF", a1 + ".npz")
    os.rename(p, p + ".x")
    fails("少一个概率图应报错", lambda: os.rename(p + ".x", p))
    wj(os.path.join(aout, "MANIFEST.json"), dict(man, checkpoints=[dict(man["checkpoints"][0], sha256="0" * 64)]))
    fails("那边用的 checkpoint 不是送过去的应报错", lambda: wj(os.path.join(aout, "MANIFEST.json"), man))
    q = os.path.join(aout, "UCSF", "zzz.tmp.npz")
    wb(q, b"")
    fails("认不出的文件应报错", lambda: os.remove(q))
    shutil.rmtree(os.path.join(aout, "PEDs-noskullstrip"))
    c2 = {k: v for k, v in cohorts.items() if k != "PEDs-noskullstrip"}
    wj(os.path.join(aout, "MANIFEST.json"), dict(man, cohorts=c2))
    fails("少一个测试集应报错")
    ok("推理拉回：少文件、checkpoint 对不上、认不出的文件、少测试集都报错，目标目录不动")


# ---------------- 通用的换名（scripts/sites/names.py 的 path） ----------------
def path_setup():
    """box 上：一个输入文件夹（含指向别处的软链接）、一个输入文件、一个 box 上的绝对路径输入。→ 任务说明。"""
    base = os.path.join(ROOT, "RQ1", "inputs", "cohortA")
    wb(os.path.join(base, "a", "x_0000.nii.gz"), b"xa")
    wb(os.path.join(ROOT, "elsewhere", "y.nii.gz"), b"yy")
    os.symlink(os.path.join(ROOT, "elsewhere", "y.nii.gz"), os.path.join(base, "y_0000.nii.gz"))
    wb(os.path.join(ROOT, "RQ1", "lists", "cases.txt"), b"x\ny\n")
    ext = os.path.join(TMP, "boxonly", "ref.json")
    wb(ext, b"{}")
    out = "RQ1/analysis/thing/run1"
    cmd = (f"python3 research/RQ1/x.py --in {{data}}/RQ1/inputs/cohortA --list {{data}}/RQ1/lists/cases.txt --ref {ext} "
           f"--out {{data}}/{out} --tag RQ1/inputs/cohortA2")
    return S.resolve("command", dict(key="probe/RQ1_thing_run1", cmd=cmd, env="nnunet", out=out, done=[".done", "a.csv|b.csv"],
                                     data=["RQ1/inputs/cohortA", "RQ1/lists/cases.txt", ext], relay="path", machines=["lab"],
                                     vars=dict(LIST="RQ1/lists/cases.txt"), fetch=dict(exclude=[], extra=["*.json"])))


def test_path():
    sys.path.insert(0, os.path.join(HERE, "..", "sites"))
    import names as N
    names = RN.Names()
    sp = path_setup()
    assert N.plugin_of(sp) == "path"
    ex = N.export(sp, ROOT, names)
    pa = ex["params"]
    blob = json.dumps(pa)
    assert blob.count("RQ1/inputs") == 1 and "RQ1/lists" not in blob and "thing" not in blob and "boxonly" not in blob, blob
    assert "machines" not in pa and pa["key"].startswith("job/") and pa["out"].startswith("out/") and pa["out"].endswith("/o"), pa
    a_in, a_list, a_ext = pa["data"]
    assert pa["data"] == ex["views"] and all(v.startswith("in/") for v in ex["views"]), ex["views"]
    assert f"--in {{data}}/{a_in} " in pa["cmd"] and f"--list {{data}}/{a_list}/cases.txt" in pa["cmd"], pa["cmd"]
    assert f"--ref {{data}}/{a_ext}/ref.json" in pa["cmd"] and f"--out {{data}}/{pa['out']}" in pa["cmd"], pa["cmd"]
    assert "--tag RQ1/inputs/cohortA2" in pa["cmd"], "名字只是前缀相同的不换"
    assert pa["vars"] == dict(LIST=f"{a_list}/cases.txt"), pa["vars"]
    fm = dict(ex["files"])
    assert fm[f"{a_in}/y_0000.nii.gz"] == os.path.join(ROOT, "elsewhere", "y.nii.gz") and f"{a_in}/a/x_0000.nii.gz" in fm, fm
    assert ex["texts"] == {} and ex["cleanup"] == [os.path.dirname(pa["out"])]
    assert N.export(sp, ROOT, names)["params"] == pa
    ok("通用换名送出：输入文件夹、文件、box 上的绝对路径、结果、任务名都换成别名（再送一次别名不变），名字相近的不换，参数里没有真路径")

    asp = S.resolve("command", pa)
    src, dst = os.path.join(TMP, "path_src"), os.path.join(TMP, "path_dst")
    wb(os.path.join(src, asp["out"], "a.csv"), b"1")
    wb(os.path.join(src, asp["out"], "sub", "p.nii.gz"), b"2")
    wb(os.path.join(src, os.path.dirname(asp["out"]), "plans.json"), b"{}")
    wb(os.path.join(src, "logs", "tasks", S.safe(asp["key"]) + "__1_seg1.log"), b"log")
    try:
        N.import_(asp, sp, src, dst, ROOT, names)
        raise AssertionError("少 .done 应报错")
    except RN.RelayError as e:
        assert ".done" in str(e), e
    ok("通用换名拉回：少一项训完的标志（拉回一半）报错")
    wb(os.path.join(src, asp["out"], ".done"), b"")
    written = N.import_(asp, sp, src, os.path.join(TMP, "path_dst2"), ROOT, names)
    assert sorted(written) == sorted([f"{sp['out']}/a.csv", f"{sp['out']}/sub/p.nii.gz", f"{sp['out']}/.done",
                                      "RQ1/analysis/thing/plans.json", f"logs/tasks/{S.safe(sp['key'])}__1_seg1.log"]), written
    wb(os.path.join(src, "in", "stray.txt"), b"?")
    try:
        N.import_(asp, sp, src, dst, ROOT, names)
        raise AssertionError("认不出的文件应报错")
    except RN.RelayError:
        pass
    ok("通用换名拉回：结果、上一级的文件、分段日志换回真名，认不出的文件报错")
    os.remove(os.path.join(ROOT, "RQ1", "lists", "cases.txt"))
    try:
        N.export(sp, ROOT, names)
        raise AssertionError("缺输入应报错")
    except RN.RelayError:
        pass
    wb(os.path.join(ROOT, "RQ1", "lists", "cases.txt"), b"x\ny\n")
    try:
        N.export(dict(sp, relay=None), ROOT, names)
        raise AssertionError("relay=None 应报错")
    except RN.RelayError:
        pass
    ok("通用换名：box 上缺输入、没写 relay 的任务都报错")


if __name__ == "__main__":
    setup()
    test_import(*test_export())
    test_infer()
    test_path()
    shutil.rmtree(TMP)
    print("全部通过")
