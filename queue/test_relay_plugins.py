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
    ok("认不出的文件、不能送的做法报错")
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


if __name__ == "__main__":
    setup()
    test_import(*test_export())
    shutil.rmtree(TMP)
    print("全部通过")
