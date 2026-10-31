"""中转执行机（dicc2）上数据集和病例改名（2026-10-30）：送去训练的数据集名、病例名换成别名，拉回的结果换回真名。
别名记在 ~/.cache/phd/relay/names.json（数据集 → Dataset9xx_<6 位十六进制>，病例 → c0001…，每个数据集各自编号），
一旦分配就不变。每种做法（recipes.py 的 relay 字段）一对函数：export 列出要送的文件和改写过的小文件，import_ 把拉回的结果换回真名。
export 还可以给 views（那边只放这次列出的文件的文件夹，多出来的删掉）和 cleanup（任务交还后那边可以删的，相对数据目录）。
推理（nnunet_infer，10/31）：模型和测试集的输入换名送过去，推理结果拉回后换回真名，MANIFEST 里的存档换成 box 上的那个。"""
import datetime
import fcntl
import glob
import hashlib
import json
import os
import random
import re
import shlex
import shutil
import subprocess
import sys
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
import spec as S  # noqa: E402
from common import write_json  # noqa: E402

RELAY_DIR = os.path.expanduser(os.environ.get("RELAY_DIR", "~/.cache/phd/relay"))
DS_RE = re.compile(r"^Dataset(\d{3})_")


class RelayError(Exception):
    pass


class Names:
    def __init__(self, path=None):
        self.path = path or os.path.join(RELAY_DIR, "names.json")

    def _load(self):
        try:
            with open(self.path) as f:
                d = json.load(f)
        except FileNotFoundError:
            d = {}
        d.setdefault("datasets", {})
        d.setdefault("cases", {})
        d.setdefault("cohorts", {})
        return d

    def _change(self, fn):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(self.path + ".lock", "w") as lk:
            fcntl.flock(lk, fcntl.LOCK_EX)
            d = self._load()
            out, changed = fn(d)
            if changed:
                write_json(self.path, d)
            return out

    def dataset(self, real, used=()):
        def fn(d):
            if real in d["datasets"]:
                return d["datasets"][real], False
            taken = set(used) | {int(DS_RE.match(a).group(1)) for a in d["datasets"].values()}
            free = [n for n in list(range(900, 1000)) + list(range(500, 900)) if n not in taken]
            if not free:
                raise RelayError("没有空的数据集编号")
            a = f"Dataset{free[0]:03d}_{random.SystemRandom().getrandbits(24):06x}"
            d["datasets"][real] = a
            return a, True
        return self._change(fn)

    def real_dataset(self, alias):
        return next((r for r, a in self._load()["datasets"].items() if a == alias), None)

    def cases(self, real_ds, cases):
        def fn(d):
            m = d["cases"].setdefault(real_ds, {})
            new = sorted(set(cases) - set(m))
            n = max((int(a[1:]) for a in m.values()), default=0)
            for c in new:
                n += 1
                m[c] = f"c{n:04d}"
            return {c: m[c] for c in cases}, bool(new)
        return self._change(fn)

    def case_map(self, real_ds):
        return dict(self._load()["cases"].get(real_ds, {}))

    def cohorts(self, reals):
        """测试集（外部集、留出集）的真名 → 别名（t + 10 位十六进制），稳定、不重复。"""
        def fn(d):
            m, new = d["cohorts"], [c for c in dict.fromkeys(reals) if c not in d["cohorts"]]
            rng, taken = random.SystemRandom(), set(m.values())
            for c in new:
                a = f"t{rng.getrandbits(40):010x}"
                while a in taken:
                    a = f"t{rng.getrandbits(40):010x}"
                m[c] = a
                taken.add(a)
            return {c: m[c] for c in reals}, bool(new)
        return self._change(fn)

    def cohort_map(self):
        return dict(self._load()["cohorts"])


def used_ids(root):
    out = set()
    for sub in ("raw", "preprocessed", "results"):
        for p in glob.glob(os.path.join(root, "nnUNet", sub, "Dataset*")):
            m = DS_RE.match(os.path.basename(p))
            if m:
                out.add(int(m.group(1)))
    return out


def _jdump(obj):
    return (json.dumps(obj, ensure_ascii=False, indent=1) + "\n").encode()


def _jload(p):
    with open(p) as f:
        return json.load(f)


# ---------------- nnU-Net ----------------
def _data_identifier(plans, config):
    c, seen = plans["configurations"].get(config), set()
    while c is not None and "data_identifier" not in c:
        parent = c.get("inherits_from")
        if parent is None or parent in seen:
            break
        seen.add(parent)
        c = plans["configurations"].get(parent)
    if c is None or "data_identifier" not in c:
        raise RelayError(f"plans 里找不到 {config} 的 data_identifier")
    return c["data_identifier"]


def _case_of(name, suffixes, known):
    hits = [name[:-len(s)] for s in suffixes if name.endswith(s) and name[:-len(s)] in known]
    if len(hits) != 1:
        raise RelayError(f"认不出文件 {name} 属于哪个病例")
    return hits[0], name[len(hits[0]):]


def _check_clean(texts, paths, words):
    blob = b"\n".join(texts) + "\n".join(paths).encode()
    bad = sorted(w for w in words if w.encode() in blob)
    if bad:
        raise RelayError(f"改名后还留着真名：{', '.join(bad[:5])}")


def export_nnunet(spec, root, names):
    n = spec["names"]
    real = n["dataset"]
    pre = os.path.join(root, "nnUNet", "preprocessed", real)
    alias = names.dataset(real, used_ids(root))
    dsj = _jload(os.path.join(pre, "dataset.json"))
    plans = _jload(os.path.join(pre, n["plans"] + ".json"))
    splits = _jload(os.path.join(pre, "splits_final.json"))
    fp = _jload(os.path.join(pre, "dataset_fingerprint.json"))
    ending = dsj.get("file_ending", ".nii.gz")
    in_splits = {c for s in splits for k in ("train", "val") for c in s[k]}
    gt_dir = os.path.join(pre, "gt_segmentations")
    gts = sorted(os.listdir(gt_dir))
    in_gt = {g[:-len(ending)] for g in gts if g.endswith(ending)}
    if not in_splits <= in_gt or len(in_gt) != len(gts):
        raise RelayError(f"{real}：splits_final.json 和 gt_segmentations 的病例不一致"
                         f"（只在 splits {len(in_splits - in_gt)} 个，gt 里别的文件 {len(gts) - len(in_gt)} 个）")
    cm = names.cases(real, sorted(in_splits))
    di = _data_identifier(plans, n["config"])
    base = f"nnUNet/preprocessed/{alias}"
    files = [(f"{base}/gt_segmentations/{cm[c]}{ending}", os.path.realpath(os.path.join(gt_dir, c + ending)))
             for c in sorted(in_splits)]
    for name in sorted(os.listdir(os.path.join(pre, di))):
        c, rest = _case_of(name, ("_seg.b2nd", ".b2nd", ".pkl"), in_gt)
        if c not in in_splits:
            continue
        files.append((f"{base}/{di}/{cm[c]}{rest}", os.path.realpath(os.path.join(pre, di, name))))
    dsj = {k: v for k, v in dsj.items() if k != "description"}
    dsj["name"] = alias
    plans["dataset_name"] = alias
    splits = [{k: [cm[c] for c in s[k]] for k in ("train", "val")} for s in splits]
    fp.pop("_note", None)
    texts = {f"{base}/dataset.json": _jdump(dsj), f"{base}/{n['plans']}.json": _jdump(plans),
             f"{base}/splits_final.json": _jdump(splits), f"{base}/dataset_fingerprint.json": _jdump(fp)}
    _check_clean(list(texts.values()), [a for a, _ in files] + list(texts), [real] + sorted(in_splits))
    params = {k: (v.replace(real, alias) if isinstance(v, str) else v) for k, v in spec["params"].items()}
    return dict(params=params, files=files, texts=texts, views=[base])


def _torch_fix(jobs, real_dsj):
    """checkpoint_*.pth 里的数据集名换成 real_dsj 的（拉回时换回真名，推理送出时换成别名，别名的 dataset.json 没有 description）：
    在本机的 nnunet 环境里用 torch 读写。jobs: [(源, 目标)]。"""
    sys.path.insert(0, os.path.join(REPO, "envs"))
    import machines as M
    act = M.activate(M.whoami(), "nnunet")
    if act is None:
        raise RelayError("本机没有 nnunet 环境，改不了 checkpoint")
    code = r"""
import json, os, sys, torch
a = json.load(sys.stdin)
for src, dst in a["jobs"]:
    ck = torch.load(src, map_location="cpu", weights_only=False)
    ia = ck.get("init_args") or {}
    if "plans" in ia:
        ia["plans"]["dataset_name"] = a["real"]
    if "dataset_json" in ia:
        dj = ia["dataset_json"]
        same = {k: v for k, v in dj.items() if k not in ("name", "description")} == {k: v for k, v in a["dsj"].items() if k not in ("name", "description")}
        ia["dataset_json"] = a["dsj"] if same else {**dj, "name": a["real"]}
        if "description" not in a["dsj"]:
            ia["dataset_json"].pop("description", None)
    tmp = dst + ".tmp"
    torch.save(ck, tmp)
    st = os.stat(src)
    os.utime(tmp, (st.st_atime, st.st_mtime))
    os.replace(tmp, dst)
"""
    payload = json.dumps(dict(jobs=jobs, real=real_dsj["name"], dsj=real_dsj))
    r = subprocess.run(["bash", "-c", act + "\nexec python -c " + _shq(code)], input=payload, capture_output=True, text=True)
    if r.returncode != 0:
        raise RelayError(f"改 checkpoint 失败：{r.stderr.strip()[-500:]}")


def _shq(s):
    return "'" + s.replace("'", "'\"'\"'") + "'"


def import_nnunet(alias_spec, real_spec, src, dst, root, names):
    an, rn = alias_spec["names"], real_spec["names"]
    alias, real = an["dataset"], rn["dataset"]
    if names.real_dataset(alias) != real:
        raise RelayError(f"{alias} 不是 {real} 的别名")
    back = {a: c for c, a in names.case_map(real).items()}
    pre = os.path.join(root, "nnUNet", "preprocessed", real)
    want = set(_jload(os.path.join(pre, "splits_final.json"))[int(rn["fold"])]["val"])
    ending = _jload(os.path.join(pre, "dataset.json")).get("file_ending", ".nii.gz")
    aout, rout = alias_spec["out"], real_spec["out"]
    sfold = os.path.join(src, aout)
    vdir = os.path.join(sfold, "validation")
    got_alias = {f[:-len(ending)] for f in os.listdir(vdir) if f.endswith(ending)} if os.path.isdir(vdir) else set()
    unknown = sorted(got_alias - set(back))
    got = {back[a] for a in got_alias if a in back}
    summ = os.path.join(vdir, "summary.json")
    npc = len(_jload(summ).get("metric_per_case", [])) if os.path.exists(summ) else -1
    if unknown or got != want or npc != len(want):
        miss, extra = sorted(want - got), sorted(got - want)
        raise RelayError(f"{real_spec['key']} 的验证集对不上：缺 {len(miss)} 例 {miss[:5]}，多 {len(extra)} 例 {extra[:5]}，"
                         f"认不出 {unknown[:5]}，summary.json 里 {npc} 例（应为 {len(want)}）")

    def rpath(rel):
        return rel.replace(alias, real)

    def text(p):
        with open(p, "rb") as f:
            return f.read().replace(alias.encode(), real.encode())

    def put(rel, data=None, srcp=None):
        p = os.path.join(dst, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = p + ".relaytmp"
        if os.path.lexists(tmp):
            os.remove(tmp)
        if data is None:
            try:
                os.link(srcp, tmp)
            except OSError:
                shutil.copy2(srcp, tmp)
        else:
            with open(tmp, "wb") as f:
                f.write(data)
            if srcp:
                st = os.stat(srcp)
                os.utime(tmp, (st.st_atime, st.st_mtime))
        os.replace(tmp, p)
        written.append(rel)

    written, ckpts, last = [], [], None
    tdir_a, tdir_r = os.path.dirname(aout), os.path.dirname(rout)
    for f in sorted(os.listdir(os.path.join(src, tdir_a))):
        sp = os.path.join(src, tdir_a, f)
        if not os.path.isfile(sp):
            continue
        if f in ("dataset.json", "dataset_fingerprint.json"):
            put(f"{tdir_r}/{f}", srcp=os.path.join(pre, f))
        elif f.endswith(".json"):
            put(f"{tdir_r}/{f}", text(sp), sp)
        else:
            put(f"{tdir_r}/{rpath(f)}", srcp=sp)
    for dp, _, fs in os.walk(sfold):
        for f in sorted(fs):
            sp = os.path.join(dp, f)
            rel = os.path.relpath(sp, sfold)
            dsub = os.path.relpath(dp, sfold)
            if dsub == "validation":
                if f == "summary.json":
                    last = sp
                    continue
                try:
                    a, rest = _case_of(f, (ending, ".npz", ".pkl"), got_alias)
                except RelayError:
                    a = None
                if a is not None:
                    put(f"{rout}/validation/{back[a]}{rest}", srcp=sp)
                    continue
            if dsub == "." and f.startswith("checkpoint_") and f.endswith(".pth"):
                ckpts.append((sp, os.path.join(dst, rout, f)))
                continue
            if dsub == "." and (f == "debug.json" or (f.startswith("training_log_") and f.endswith(".txt"))):
                put(f"{rout}/{f}", text(sp), sp)
                continue
            put(f"{rout}/{rpath(rel)}", srcp=sp)
    ldir = os.path.join(src, "logs", "tasks")
    pa, pr = S.safe(alias_spec["key"]) + "__", S.safe(real_spec["key"]) + "__"
    if os.path.isdir(ldir):
        for f in sorted(os.listdir(ldir)):
            if f.startswith(pa):
                put(f"logs/tasks/{pr}{f[len(pa):]}", text(os.path.join(ldir, f)), os.path.join(ldir, f))
    if ckpts:
        os.makedirs(os.path.join(dst, rout), exist_ok=True)
        _torch_fix(ckpts, _jload(os.path.join(pre, "dataset.json")))
        written += [f"{rout}/{os.path.basename(d)}" for _, d in ckpts]
    s = _jload(last)
    for m in s.get("metric_per_case", []):
        for k in ("prediction_file", "reference_file"):
            if k in m:
                d, b = os.path.split(m[k])
                a = b[:-len(ending)] if b.endswith(ending) else None
                m[k] = os.path.join(d.replace(alias, real), (back[a] + ending) if a in back else b)
    put(f"{rout}/validation/summary.json", _jdump(s).replace(alias.encode(), real.encode()), last)
    return written


# ---------------- nnU-Net 推理（RQ1 的 infer_task.sh，10/31） ----------------
PRED_RE = re.compile(r"^predictions/(Dataset\d{3}_[^/]+)/([^/]+)/fold_(\w+)$")
IMG_RE = re.compile(r"^(.+)_(\d{4})\.nii\.gz$")
INFER_CASES = "raw:infer"  # 推理输入的病例共用一套别名（同一个病例在几个测试集里是同一个别名）
INFER_MODELS = "nnUNet/models"  # 那边放推理用的模型的地方（不和训练结果放在一起：训练的结果交还时会删）


def _sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for blk in iter(lambda: f.read(1 << 22), b""):
            h.update(blk)
    return h.hexdigest()


def _where(spec):
    m = PRED_RE.match(spec["out"])
    if not m:
        raise RelayError(f"认不出推理的结果文件夹 {spec['out']}（应为 predictions/<数据集>/<训练器文件夹>/fold_<k>）")
    return m.groups()


def infer_inputs(spec, root, model_rel, fold):
    """推理要的输入文件夹（相对数据目录）：任务命令所在目录的 run_predict_eval.sh --stage needs。"""
    sh = next((t for t in shlex.split(spec["cmd"]) if t.endswith(".sh")), None)
    if sh is None:
        raise RelayError(f"推理命令里没有脚本：{spec['cmd']}")
    pe = os.path.join(REPO, os.path.dirname(sh), "run_predict_eval.sh")
    r = subprocess.run(["bash", pe, "--stage", "needs", os.path.join(root, model_rel), fold], cwd=REPO, capture_output=True,
                       text=True, timeout=300, env=dict(os.environ, BRAIN_TUMOR=root))
    if r.returncode:
        raise RelayError(f"--stage needs 出错：{(r.stderr.strip().splitlines() or ['?'])[-1]}")
    return [ln.strip().rstrip("/") for ln in r.stdout.splitlines() if ln.strip().startswith("nnUNet/raw/")]


def infer_vars(model_rel):
    """别名下实验表查不到这个模型：跳过、只测哪些外部集和模型的环境在这里查好，作为任务的环境变量送过去。"""
    import experiments as QE
    skip, only = QE.names_of(model_rel, "skip_ext", "only_ext")
    return dict(SKIP_EXT=skip, ONLY_EXT=only, MODEL_ENV=QE.env_of(model_rel))


HOLD_IN = "nnUNet/raw/RQ1_holdout/GLI/imagesTs"
EXT_IN = re.compile(r"nnUNet/raw/RQ1_external/([^/]+)/imagesTs")


def _cohort_of(d):
    """推理的输入目录 → 测试集的名字（和那边预测文件夹的名字一样）。"""
    if d == HOLD_IN:
        return "GLI_holdout"
    m = EXT_IN.fullmatch(d)
    if not m:
        raise RelayError(f"认不出推理的输入目录 {d}")
    return m.group(1)


def _cases_in(d):
    out = {}
    for f in sorted(os.listdir(d)):
        m = IMG_RE.match(f)
        if not m:
            raise RelayError(f"{d} 里认不出的文件 {f}")
        out.setdefault(m.group(1), []).append(m.group(2))
    return out


def _alias_ckpt(src, dst, alias_dsj, real):
    """别名版的 checkpoint（缓存在 dst，旁边的 .src 记着源的大小、修改时间和两边的 sha256）→ (别名版 sha256, 源 sha256)。"""
    st = os.stat(src)
    tag = f"{os.path.realpath(src)}|{st.st_size}|{st.st_mtime_ns}"
    side = dst + ".src"
    try:
        d = _jload(side)
        if d["tag"] == tag and os.path.isfile(dst) and os.path.getsize(dst) == d["bytes"]:
            return d["alias_sha256"], d["sha256"]
    except (OSError, ValueError, KeyError):
        pass
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    _torch_fix([(src, dst)], alias_dsj)
    with zipfile.ZipFile(dst) as z:
        pk = b"".join(z.read(n) for n in z.namelist() if n.endswith("data.pkl"))
    if real.encode() in pk:
        os.remove(dst)
        raise RelayError(f"别名版 checkpoint 里还有 {real}")
    d = dict(tag=tag, bytes=os.path.getsize(dst), alias_sha256=_sha256(dst), sha256=_sha256(src))
    write_json(side, d)
    return d["alias_sha256"], d["sha256"]


def export_nnunet_infer(spec, root, names):
    import relay_bundle as RB
    real, td, fold = _where(spec)
    model_rel = f"nnUNet/results/{real}/{td}"
    model = os.path.join(root, model_rel)
    ck = os.path.join(model, f"fold_{fold}", "checkpoint_final.pth")
    for p in (os.path.join(model, "dataset.json"), os.path.join(model, "plans.json"), ck):
        if not os.path.isfile(p):
            raise RelayError(f"box 上没有 {os.path.relpath(p, root)}")
    alias = names.dataset(real, used_ids(root))
    amodel = f"{INFER_MODELS}/{alias}/{td}"
    dsj = {k: v for k, v in _jload(os.path.join(model, "dataset.json")).items() if k != "description"}
    dsj["name"] = alias
    plans = _jload(os.path.join(model, "plans.json"))
    plans["dataset_name"] = alias
    texts = {f"{amodel}/dataset.json": _jdump(dsj), f"{amodel}/plans.json": _jdump(plans).replace(real.encode(), alias.encode())}
    ack = os.path.join(RELAY_DIR, "ckpt", alias, td, f"fold_{fold}", "checkpoint_final.pth")
    _alias_ckpt(ck, ack, dsj, real)
    files = [(f"{amodel}/fold_{fold}/checkpoint_final.pth", ack)]
    ins = infer_inputs(spec, root, model_rel, fold)
    if not ins:
        raise RelayError(f"{model_rel} 没有要推理的测试集")
    per = {d: _cases_in(os.path.join(root, d)) for d in ins}
    cm = names.cases(INFER_CASES, sorted({c for v in per.values() for c in v}))
    coh = {d: _cohort_of(d) for d in ins}
    ta = names.cohorts(list(coh.values()))
    hold = [ta[c] for c in coh.values() if c == "GLI_holdout"]
    if len(hold) != 1:
        raise RelayError(f"{model_rel} 的推理输入里没有留出集")
    views = [f"{amodel}/fold_{fold}"]
    for d, cases in per.items():
        top = "nnUNet/raw/RQ1_holdout" if coh[d] == "GLI_holdout" else "nnUNet/raw/RQ1_external"
        rd = f"{RB.rename(top)}/{ta[coh[d]]}/imagesTs"
        views.append(rd)
        for c, chs in cases.items():
            files += [(f"{rd}/{cm[c]}_{ch}.nii.gz", os.path.realpath(os.path.join(root, d, f"{c}_{ch}.nii.gz"))) for ch in chs]
    vs = dict(spec.get("vars") or {})
    for k, v in infer_vars(model_rel).items():
        vs.setdefault(k, v)
    # 那边的脚本按 TS_HOLD、TS_EXT（别名）推理；要测哪些外部集已经在 box 上按 SKIP_EXT / ONLY_EXT 挑好（infer_inputs）
    vs.update(TS_HOLD=hold[0], TS_EXT=" ".join(ta[c] for c in coh.values() if c != "GLI_holdout"), SKIP_EXT="", ONLY_EXT="")
    params = {k: v for k, v in spec["params"].items() if k not in ("machines", "after")}
    for k, v in params.items():
        if isinstance(v, str):
            params[k] = v.replace(model_rel, amodel).replace(real, alias)
    params["vars"] = vs
    _check_clean(list(texts.values()) + [json.dumps(params, ensure_ascii=False).encode()],
                 [a for a, _ in files] + list(texts), [real])
    bad = [a for a, _ in files[1:] if not re.fullmatch(r"c\d{4,}_\d{4}\.nii\.gz", os.path.basename(a))]
    if bad:
        raise RelayError(f"改名后的输入文件名不对：{bad[:3]}")
    blob = "\n".join([json.dumps(params, ensure_ascii=False)] + [a for a, _ in files] + list(texts) + views
                      + [t.decode(errors="replace") for t in texts.values()])
    left = RB.cohort_hits(blob, list(coh.values()) + ["GLI"])
    if left:
        raise RelayError(f"改名后还留着测试集的真名：{', '.join(left)}")
    return dict(params=params, files=files, texts=texts, views=views,
                cleanup=[f"{amodel}/fold_{fold}", RB.rename(f"RQ1/analysis/{alias}/{td}/fold_{fold}")])


def _put(dst, written, rel, data=None, srcp=None):
    p = os.path.join(dst, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".relaytmp"
    if os.path.lexists(tmp):
        os.remove(tmp)
    if data is None:
        try:
            os.link(srcp, tmp)
        except OSError:
            shutil.copy2(srcp, tmp)
    else:
        with open(tmp, "wb") as f:
            f.write(data)
        if srcp:
            st = os.stat(srcp)
            os.utime(tmp, (st.st_atime, st.st_mtime))
    os.replace(tmp, p)
    written.append(rel)


def _swap(o, a, b, keys=False):
    if isinstance(o, str):
        return o.replace(a, b)
    if isinstance(o, list):
        return [_swap(x, a, b, keys) for x in o]
    if isinstance(o, dict):
        return {(k.replace(a, b) if keys else k): _swap(v, a, b, keys) for k, v in o.items()}
    return o


def import_nnunet_infer(alias_spec, real_spec, src, dst, root, names):
    real, td, fold = _where(real_spec)
    alias, atd, afold = _where(alias_spec)
    if names.real_dataset(alias) != real or (atd, afold) != (td, fold):
        raise RelayError(f"{alias_spec['out']} 不是 {real_spec['out']} 的别名")
    back = {a: c for c, a in names.case_map(INFER_CASES).items()}
    tback = {a: c for c, a in names.cohort_map().items()}
    model_rel = f"nnUNet/results/{real}/{td}"
    model = os.path.join(root, model_rel)
    sfold, rout = os.path.join(src, alias_spec["out"]), real_spec["out"]
    for f in (".infer_done", "MANIFEST.json"):
        if not os.path.isfile(os.path.join(sfold, f)):
            raise RelayError(f"{alias_spec['key']} 的结果里没有 {f}")
    ins = {d: set(_cases_in(os.path.join(root, d))) for d in infer_inputs(real_spec, root, model_rel, fold)}
    man = _jload(os.path.join(sfold, "MANIFEST.json"))
    cohorts = sorted(d for d in os.listdir(sfold) if os.path.isdir(os.path.join(sfold, d)))
    if sorted(man.get("cohorts") or {}) != cohorts:
        raise RelayError(f"{alias_spec['key']}：MANIFEST 里的测试集 {sorted(man.get('cohorts') or {})} 和文件夹 {cohorts} 不一样")
    odd = [c for c in cohorts if c not in tback]
    if odd:
        raise RelayError(f"{alias_spec['key']}：认不出的测试集文件夹 {odd}")
    jobs, free = [], dict(ins)
    for ac in cohorts:
        c, d = tback[ac], os.path.join(sfold, ac)
        segs, probs, pkls, other = set(), set(), set(), []
        for f in os.listdir(d):
            for ext, bag in ((".nii.gz", segs), (".npz", probs), (".pkl", pkls)):
                if f.endswith(ext) and f[:-len(ext)] in back:
                    bag.add(f[:-len(ext)])
                    break
            else:
                if f not in ("dataset.json", "plans.json", "predict_from_raw_data_args.json"):
                    other.append(f)
        if other:
            raise RelayError(f"{alias_spec['key']} 的 {c} 里有认不出的文件：{sorted(other)[:5]}")
        v = man["cohorts"][ac]
        got = {back[a] for a in segs}
        hit = next((k for k, cs in free.items() if cs == got), None)
        if hit is None or probs != segs or pkls != segs or v.get("n_segmentations") != len(segs) or v.get("n_probabilities") != len(probs):
            raise RelayError(f"{alias_spec['key']} 的 {c} 对不上：分割 {len(segs)}、概率图 {len(probs)}、pkl {len(pkls)}，"
                             f"MANIFEST 记 {v.get('n_segmentations')} / {v.get('n_probabilities')}，"
                             f"{'和 box 上的输入一致' if hit else 'box 上没有病例完全一样的输入文件夹'}")
        free.pop(hit)
        jobs.append((c, d, segs))
    if free:
        raise RelayError(f"{alias_spec['key']}：box 上的输入 {sorted(free)} 没有对应的预测")
    ck = os.path.join(model, f"fold_{fold}", "checkpoint_final.pth")
    ack = os.path.join(RELAY_DIR, "ckpt", alias, td, f"fold_{fold}", "checkpoint_final.pth")
    try:
        side = _jload(ack + ".src")
    except (OSError, ValueError):
        side = {}
    st = os.stat(ck)
    if side.get("tag") != f"{os.path.realpath(ck)}|{st.st_size}|{st.st_mtime_ns}":
        raise RelayError(f"box 上的 {model_rel}/fold_{fold}/checkpoint_final.pth 和送过去的不是同一个（{ack}.src 对不上）")
    sent = [x.get("sha256") for x in man.get("checkpoints") or []]
    if sent != [side["alias_sha256"]]:
        raise RelayError(f"{alias_spec['key']}：那边推理用的 checkpoint 不是送过去的那个")
    rdsj = os.path.join(model, "dataset.json")

    def text(p):
        with open(p, "rb") as f:
            b = f.read().replace(alias.encode(), real.encode())
        for a, c in tback.items():
            b = b.replace(a.encode(), c.encode())
        return b
    written = []
    for c, d, segs in jobs:
        for a in sorted(segs):
            for ext in (".nii.gz", ".npz", ".pkl"):
                _put(dst, written, f"{rout}/{c}/{back[a]}{ext}", srcp=os.path.join(d, a + ext))
        for f in ("plans.json", "predict_from_raw_data_args.json"):
            if os.path.exists(os.path.join(d, f)):
                _put(dst, written, f"{rout}/{c}/{f}", text(os.path.join(d, f)), os.path.join(d, f))
        if os.path.exists(os.path.join(d, "dataset.json")):
            _put(dst, written, f"{rout}/{c}/dataset.json", srcp=rdsj)
    ldir = os.path.join(src, "logs", "tasks")
    pa, pr = S.safe(alias_spec["key"]) + "__", S.safe(real_spec["key"]) + "__"
    if os.path.isdir(ldir):
        for f in sorted(os.listdir(ldir)):
            if f.startswith(pa):
                _put(dst, written, f"logs/tasks/{pr}{f[len(pa):]}", text(os.path.join(ldir, f)), os.path.join(ldir, f))
    man = _swap(man, alias, real)
    for a, c in tback.items():
        man = _swap(man, a, c, keys=True)
    man.update(model_dir=model, dataset=real, trainer=td, relayed=dict(dataset=alias, checkpoint_sha256=side["alias_sha256"]))
    man["checkpoints"] = [dict(fold=int(fold), file=ck, bytes=st.st_size,
                               mtime=datetime.datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"), sha256=side["sha256"])]
    mp = os.path.join(sfold, "MANIFEST.json")
    _put(dst, written, f"{rout}/MANIFEST.json", (json.dumps(man, indent=1, ensure_ascii=False)).encode(), mp)
    _put(dst, written, f"{rout}/.infer_done", text(os.path.join(sfold, ".infer_done")), os.path.join(sfold, ".infer_done"))
    return written


TRANSLATORS = {"nnunet": (export_nnunet, import_nnunet), "nnunet_infer": (export_nnunet_infer, import_nnunet_infer)}


def _pair(spec):
    t = TRANSLATORS.get(spec.get("relay"))
    if t is None:
        raise RelayError(f"做法 {spec.get('recipe')} 不能送到中转执行机（relay={spec.get('relay')}）")
    return t


def export(spec, root, names):
    return _pair(spec)[0](spec, root, names)


def import_(alias_spec, real_spec, src, dst, root, names):
    return _pair(real_spec)[1](alias_spec, real_spec, src, dst, root, names)
