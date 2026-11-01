"""送到执行机的任务按做法（recipes.py 的 relay 字段）挑插件（2026-11-01 起都用真名）：插件列出任务要用的 box 上的文件，
拉回后核对结果。路径在两边一样（相对数据目录），文件内容不改。通用的 path 插件和统一的入口见 scripts/sites/names.py。

每个插件一对函数：
  export(spec, root) → dict(files=[(相对路径, box 上的真实文件)], views=[视图文件夹], cleanup=[…]（可省，默认 = 结果文件夹）)
  check(spec, src, root)：src 下是拉回的文件（真的相对路径），对不上时报 RelayError
  nnunet        训练：nnUNet/preprocessed/<数据集>/ 里这次要用的文件，视图是整个数据集文件夹；拉回核对验证集
  nnunet_infer  推理：run_predict_eval.sh --stage needs 列出的模型文件和测试集的输入；拉回核对 MANIFEST"""
import hashlib
import json
import os
import re
import shlex
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))

RELAY_DIR = os.path.expanduser(os.environ.get("RELAY_DIR", "~/.cache/phd/relay"))


class RelayError(Exception):
    pass


def _jload(p):
    with open(p) as f:
        return json.load(f)


def walk(p):
    """文件夹 p 下的文件（相对 p，跟着软链接走），排好序。"""
    out = []
    for dp, dns, fns in os.walk(p, followlinks=True):
        dns.sort()
        out += [os.path.relpath(os.path.join(dp, f), p) for f in sorted(fns)]
    return out


def expand(root, rel):
    """数据目录下的一个输入 → (files, 视图文件夹或 None)：文件夹里的文件都放进去、整个文件夹当视图；单个文件不当视图。"""
    p = os.path.join(root, rel)
    if os.path.isdir(p):
        fs = walk(p)
        if not fs:
            raise RelayError(f"输入 {rel} 是空的")
        return [(f"{rel}/{f}", os.path.realpath(os.path.join(p, f))) for f in fs], rel
    if os.path.isfile(p):
        return [(rel, os.path.realpath(p))], None
    raise RelayError(f"box 上没有输入 {rel}")


def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for blk in iter(lambda: f.read(1 << 22), b""):
            h.update(blk)
    return h.hexdigest()


# ---------------- nnU-Net 训练 ----------------
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
    return hits[0]


def export_nnunet(spec, root):
    n = spec["names"]
    base = f"nnUNet/preprocessed/{n['dataset']}"
    pre = os.path.join(root, base)
    dsj = _jload(os.path.join(pre, "dataset.json"))
    plans = _jload(os.path.join(pre, n["plans"] + ".json"))
    splits = _jload(os.path.join(pre, "splits_final.json"))
    ending = dsj.get("file_ending", ".nii.gz")
    in_splits = {c for s in splits for k in ("train", "val") for c in s[k]}
    gts = sorted(os.listdir(os.path.join(pre, "gt_segmentations")))
    in_gt = {g[:-len(ending)] for g in gts if g.endswith(ending)}
    if not in_splits <= in_gt or len(in_gt) != len(gts):
        raise RelayError(f"{n['dataset']}：splits_final.json 和 gt_segmentations 的病例不一致"
                         f"（只在 splits {len(in_splits - in_gt)} 个，gt 里别的文件 {len(gts) - len(in_gt)} 个）")
    di = _data_identifier(plans, n["config"])
    rels = [f"gt_segmentations/{c}{ending}" for c in sorted(in_splits)]
    for f in sorted(os.listdir(os.path.join(pre, di))):
        if _case_of(f, ("_seg.b2nd", ".b2nd", ".pkl"), in_gt) in in_splits:
            rels.append(f"{di}/{f}")
    rels += ["dataset.json", n["plans"] + ".json", "splits_final.json", "dataset_fingerprint.json"]
    files = []
    for r in rels:
        p = os.path.join(pre, r)
        if not os.path.isfile(p):
            raise RelayError(f"box 上没有 {base}/{r}")
        files.append((f"{base}/{r}", os.path.realpath(p)))
    return dict(files=files, views=[base])


def check_nnunet(spec, src, root):
    """验证集的病例 = splits 里这一折的 val，summary.json 的例数一样。"""
    n = spec["names"]
    pre = os.path.join(root, "nnUNet", "preprocessed", n["dataset"])
    want = set(_jload(os.path.join(pre, "splits_final.json"))[int(n["fold"])]["val"])
    ending = _jload(os.path.join(pre, "dataset.json")).get("file_ending", ".nii.gz")
    vdir = os.path.join(src, spec["out"], "validation")
    got = {f[:-len(ending)] for f in os.listdir(vdir) if f.endswith(ending)} if os.path.isdir(vdir) else set()
    summ = os.path.join(vdir, "summary.json")
    npc = len(_jload(summ).get("metric_per_case", [])) if os.path.exists(summ) else -1
    if got != want or npc != len(want):
        miss, extra = sorted(want - got), sorted(got - want)
        raise RelayError(f"{spec['key']} 的验证集对不上：缺 {len(miss)} 例 {miss[:5]}，多 {len(extra)} 例 {extra[:5]}，"
                         f"summary.json 里 {npc} 例（应为 {len(want)}）")


# ---------------- nnU-Net 推理（RQ1 的 infer_task.sh） ----------------
def infer_inputs(spec, root):
    """推理要的输入（相对数据目录，文件或文件夹）：任务命令里的脚本所在目录的 run_predict_eval.sh --stage needs（带任务的 vars）。"""
    toks = shlex.split(spec["cmd"])
    i = next((j for j, t in enumerate(toks) if t.endswith(".sh")), None)
    if i is None or len(toks) < i + 3:
        raise RelayError(f"推理命令里没有脚本、模型和 fold：{spec['cmd']}")
    model, fold = toks[i + 1].replace("{data}", root), toks[i + 2]
    pe = os.path.join(REPO, os.path.dirname(toks[i]), "run_predict_eval.sh")
    env = dict(os.environ, **{k: str(v).replace("{data}", root) for k, v in (spec.get("vars") or {}).items()}, BRAIN_TUMOR=root)
    r = subprocess.run(["bash", pe, "--stage", "needs", model, fold], cwd=REPO, capture_output=True,
                       text=True, timeout=300, env=env)
    if r.returncode:
        raise RelayError(f"--stage needs 出错：{(r.stderr.strip().splitlines() or ['?'])[-1]}")
    out = [ln.strip().rstrip("/") for ln in r.stdout.splitlines() if ln.strip()]
    if not out:
        raise RelayError(f"{spec['key']}：--stage needs 什么都没列")
    return out


def export_nnunet_infer(spec, root):
    files, views = [], []
    for rel in infer_inputs(spec, root):
        fs, v = expand(root, rel)
        files += fs
        views += [v] if v else []
    return dict(files=files, views=views)


IMG_RE = re.compile(r"^(.+)_\d{4}\.nii\.gz$")  # 推理输入：<病例>_<通道>.nii.gz


def _cases(names, ext):
    return {f[:-len(ext)] for f in names if f.endswith(ext)}


def check_nnunet_infer(spec, src, root):
    """每个测试集的分割、概率图、.pkl 病例一样，和 box 上某个输入文件夹的病例一样，数目和 MANIFEST 一致；
    MANIFEST 里的 checkpoint 和 box 上的同一个（sha256）。"""
    key, sfold = spec["key"], os.path.join(src, spec["out"])
    for f in (".infer_done", "MANIFEST.json"):
        if not os.path.isfile(os.path.join(sfold, f)):
            raise RelayError(f"{key} 的结果里没有 {f}")
    ins = infer_inputs(spec, root)
    dirs = [d for d in ins if os.path.isdir(os.path.join(root, d))]
    free = [{m.group(1) for m in map(IMG_RE.match, os.listdir(os.path.join(root, d))) if m} for d in dirs]
    man = _jload(os.path.join(sfold, "MANIFEST.json"))
    cohorts = sorted(d for d in os.listdir(sfold) if os.path.isdir(os.path.join(sfold, d)))
    if sorted(man.get("cohorts") or {}) != cohorts:
        raise RelayError(f"{key}：MANIFEST 里的测试集 {sorted(man.get('cohorts') or {})} 和文件夹 {cohorts} 不一样")
    for c in cohorts:
        fs = os.listdir(os.path.join(sfold, c))
        segs, probs, pkls = _cases(fs, ".nii.gz"), _cases(fs, ".npz"), _cases(fs, ".pkl")
        other = [f for f in fs if not f.endswith((".nii.gz", ".npz", ".pkl"))
                 and f not in ("dataset.json", "plans.json", "predict_from_raw_data_args.json")]
        if other or any(f.endswith(".tmp.npz") for f in fs):
            raise RelayError(f"{key} 的 {c} 里有认不出的文件：{sorted(other or [f for f in fs if f.endswith('.tmp.npz')])[:5]}")
        v = man["cohorts"][c]
        hit = next((i for i, cs in enumerate(free) if cs == segs), None)
        if hit is None or probs != segs or pkls != segs or v.get("n_segmentations") != len(segs) or v.get("n_probabilities") != len(probs):
            raise RelayError(f"{key} 的 {c} 对不上：分割 {len(segs)}、概率图 {len(probs)}、pkl {len(pkls)}，"
                             f"MANIFEST 记 {v.get('n_segmentations')} / {v.get('n_probabilities')}，"
                             f"{'和 box 上的输入一致' if hit is not None else 'box 上没有病例完全一样的输入文件夹'}")
        free.pop(hit)
    if free:
        raise RelayError(f"{key}：box 上有 {len(free)} 个输入文件夹没有对应的预测")
    cks = [r for r in ins if os.path.basename(r) == "checkpoint_final.pth"]
    want = [sha256(os.path.join(root, r)) for r in cks]
    sent = [x.get("sha256") for x in man.get("checkpoints") or []]
    if sorted(sent) != sorted(want):
        raise RelayError(f"{key}：那边推理用的 checkpoint 和 box 上的 {cks} 不是同一个")


PLUGINS = {"nnunet": (export_nnunet, check_nnunet), "nnunet_infer": (export_nnunet_infer, check_nnunet_infer)}


def repo_files(trainer):
    """训练器要读仓库里的病例子集（SPLITS_DIR，如 n470、frac）→ True：那边的代码包里没有这些子集，不领。"""
    d = os.path.join(REPO, "research", "common", "nnunet", "trainers")
    for f in os.listdir(d) if trainer else []:
        if f.endswith(".py"):
            src = open(os.path.join(d, f), encoding="utf-8").read()
            if f"class {trainer}(" in src:
                return "SPLITS_DIR" in src
    return False


def _accepts(params):
    return not repo_files(params.get("trainer"))


ACCEPTS = {"nnunet": _accepts, "nnunet_infer": _accepts}  # 插件能不能送这个任务（按参数；不在这里的都能送）
VIEW_ROOTS = ["nnUNet/preprocessed"]  # 训练插件建视图的文件夹：relay_gc.py 除了登记过的视图，还清理这下面没人用的
