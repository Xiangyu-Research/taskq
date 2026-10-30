"""中转执行机（dicc2）上数据集和病例改名（2026-10-30）：送去训练的数据集名、病例名换成别名，拉回的结果换回真名。
别名记在 ~/.cache/phd/relay/names.json（数据集 → Dataset9xx_<6 位十六进制>，病例 → c0001…，每个数据集各自编号），
一旦分配就不变。每种做法（recipes.py 的 relay 字段）一对函数：export 列出要送的文件和改写过的小文件，import_ 把拉回的结果换回真名。"""
import fcntl
import glob
import json
import os
import random
import re
import shutil
import subprocess
import sys

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
    if in_splits != in_gt or len(in_gt) != len(gts):
        raise RelayError(f"{real}：splits_final.json 和 gt_segmentations 的病例不一致"
                         f"（只在 splits {len(in_splits - in_gt)} 个，只在 gt {len(in_gt - in_splits)} 个，gt 里别的文件 {len(gts) - len(in_gt)} 个）")
    cm = names.cases(real, sorted(in_gt))
    di = _data_identifier(plans, n["config"])
    base = f"nnUNet/preprocessed/{alias}"
    files = [(f"{base}/gt_segmentations/{cm[c]}{ending}", os.path.realpath(os.path.join(gt_dir, c + ending)))
             for c in sorted(in_gt)]
    for name in sorted(os.listdir(os.path.join(pre, di))):
        c, rest = _case_of(name, ("_seg.b2nd", ".b2nd", ".pkl"), in_gt)
        files.append((f"{base}/{di}/{cm[c]}{rest}", os.path.realpath(os.path.join(pre, di, name))))
    dsj = {k: v for k, v in dsj.items() if k != "description"}
    dsj["name"] = alias
    plans["dataset_name"] = alias
    splits = [{k: [cm[c] for c in s[k]] for k in ("train", "val")} for s in splits]
    fp.pop("_note", None)
    texts = {f"{base}/dataset.json": _jdump(dsj), f"{base}/{n['plans']}.json": _jdump(plans),
             f"{base}/splits_final.json": _jdump(splits), f"{base}/dataset_fingerprint.json": _jdump(fp)}
    _check_clean(list(texts.values()), [a for a, _ in files] + list(texts), [real] + sorted(in_gt))
    params = {k: (v.replace(real, alias) if isinstance(v, str) else v) for k, v in spec["params"].items()}
    return dict(params=params, files=files, texts=texts)


def _torch_fix(jobs, real_dsj):
    """checkpoint_*.pth 里的数据集名换回真名：在本机的 nnunet 环境里用 torch 读写。jobs: [(源, 目标)]。"""
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


TRANSLATORS = {"nnunet": (export_nnunet, import_nnunet)}


def _pair(spec):
    t = TRANSLATORS.get(spec.get("relay"))
    if t is None:
        raise RelayError(f"做法 {spec.get('recipe')} 不能送到中转执行机（relay={spec.get('relay')}）")
    return t


def export(spec, root, names):
    return _pair(spec)[0](spec, root, names)


def import_(alias_spec, real_spec, src, dst, root, names):
    return _pair(real_spec)[1](alias_spec, real_spec, src, dst, root, names)
