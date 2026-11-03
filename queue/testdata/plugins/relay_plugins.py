"""测试用的中继插件（2026-11-02）：形状同真的领域插件。
  toy        训练：prepared/<ds>/ 里这次要用的文件，视图是整个文件夹；拉回核对验证集（check/）
  toy_infer  推理：testdata/bin/step.sh --stage needs 列出的模型文件和测试集的输入；拉回核对 MANIFEST"""
import json
import os
import re
import shlex
import subprocess

from relay_plugins import RelayError, expand, sha256

FIX = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(FIX)))


def _jload(p):
    with open(p) as f:
        return json.load(f)


# ---------------- 训练 ----------------
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
        raise RelayError(f"认不出文件 {name} 属于哪个样本")
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
        raise RelayError(f"{n['ds']}：splits_final.json 和 gt 的样本不一致"
                         f"（只在 splits {len(in_splits - in_gt)} 个，gt 里别的文件 {len(gts) - len(in_gt)} 个）")
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
            raise RelayError(f"box 上没有 {base}/{r}")
        files.append((f"{base}/{r}", os.path.realpath(p)))
    return dict(files=files, views=[base])


def check_toy(spec, src, root):
    """验证集的样本 = splits 里这一份的 val，summary.json 的例数一样。"""
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
        raise RelayError(f"{spec['key']} 的验证集对不上：缺 {len(miss)} 例 {miss[:5]}，多 {len(extra)} 例 {extra[:5]}，"
                         f"summary.json 里 {npc} 例（应为 {len(want)}）")


# ---------------- 推理（testdata/bin/infer_task.sh） ----------------
def infer_inputs(spec, root):
    """推理要的输入（相对数据目录）：任务命令里的脚本所在目录的 step.sh --stage needs（带任务的 vars）。"""
    toks = shlex.split(spec["cmd"])
    i = next((j for j, t in enumerate(toks) if t.endswith(".sh")), None)
    if i is None or len(toks) < i + 3:
        raise RelayError(f"推理命令里没有脚本、模型和第几份：{spec['cmd']}")
    model, frag = toks[i + 1].replace("{data}", root), toks[i + 2]
    pe = os.path.join(REPO, os.path.dirname(toks[i]), "step.sh")
    env = dict(os.environ, **{k: str(v).replace("{data}", root) for k, v in (spec.get("vars") or {}).items()}, TASKQ_DATA=root)
    r = subprocess.run(["bash", pe, "--stage", "needs", model, frag], cwd=REPO, capture_output=True,
                       text=True, timeout=300, env=env)
    if r.returncode:
        raise RelayError(f"--stage needs 出错：{(r.stderr.strip().splitlines() or ['?'])[-1]}")
    out = [ln.strip().rstrip("/") for ln in r.stdout.splitlines() if ln.strip()]
    if not out:
        raise RelayError(f"{spec['key']}：--stage needs 什么都没列")
    return out


def export_toy_infer(spec, root):
    files, views = [], []
    for rel in infer_inputs(spec, root):
        fs, v = expand(root, rel)
        files += fs
        views += [v] if v else []
    return dict(files=files, views=views)


IN_RE = re.compile(r"^(.+)_\d{4}\.dat$")  # 推理输入：<样本>_<通道>.dat


def _cases(names, ext):
    return {f[:-len(ext)] for f in names if f.endswith(ext)}


def check_toy_infer(spec, src, root):
    """每个测试集的输出、概率、.meta 样本一样，和 box 上某个输入文件夹的样本一样，数目和 MANIFEST 一致；
    MANIFEST 里的存档和 box 上的同一个（sha256）。"""
    key, sdir = spec["key"], os.path.join(src, spec["out"])
    for f in (".infer_done", "MANIFEST.json"):
        if not os.path.isfile(os.path.join(sdir, f)):
            raise RelayError(f"{key} 的结果里没有 {f}")
    ins = infer_inputs(spec, root)
    dirs = [d for d in ins if os.path.isdir(os.path.join(root, d))]
    free = [{m.group(1) for m in map(IN_RE.match, os.listdir(os.path.join(root, d))) if m} for d in dirs]
    man = _jload(os.path.join(sdir, "MANIFEST.json"))
    sets = sorted(d for d in os.listdir(sdir) if os.path.isdir(os.path.join(sdir, d)))
    if sorted(man.get("cohorts") or {}) != sets:
        raise RelayError(f"{key}：MANIFEST 里的测试集 {sorted(man.get('cohorts') or {})} 和文件夹 {sets} 不一样")
    for c in sets:
        fs = os.listdir(os.path.join(sdir, c))
        outs, probs, metas = _cases(fs, ".out"), _cases(fs, ".prob"), _cases(fs, ".meta")
        other = [f for f in fs if not f.endswith((".out", ".prob", ".meta")) and f not in ("dataset.json", "plans.json", "args.json")]
        if other or any(f.endswith(".tmp.prob") for f in fs):
            raise RelayError(f"{key} 的 {c} 里有认不出的文件：{sorted(other or [f for f in fs if f.endswith('.tmp.prob')])[:5]}")
        v = man["cohorts"][c]
        hit = next((i for i, cs in enumerate(free) if cs == outs), None)
        if hit is None or probs != outs or metas != outs or v.get("n_outputs") != len(outs) or v.get("n_probs") != len(probs):
            raise RelayError(f"{key} 的 {c} 对不上：输出 {len(outs)}、概率 {len(probs)}、meta {len(metas)}，"
                             f"MANIFEST 记 {v.get('n_outputs')} / {v.get('n_probs')}，"
                             f"{'和 box 上的输入一致' if hit is not None else 'box 上没有样本完全一样的输入文件夹'}")
        free.pop(hit)
    if free:
        raise RelayError(f"{key}：box 上有 {len(free)} 个输入文件夹没有对应的输出")
    cks = [r for r in ins if os.path.basename(r) == "final.ckpt"]
    want = [sha256(os.path.join(root, r)) for r in cks]
    sent = [x.get("sha256") for x in man.get("checkpoints") or []]
    if sorted(sent) != sorted(want):
        raise RelayError(f"{key}：那边推理用的存档和 box 上的 {cks} 不是同一个")


PLUGINS = {"toy": (export_toy, check_toy), "toy_infer": (export_toy_infer, check_toy_infer)}


def repo_files(variant):
    """变体要读仓库里的样本子集（SPLITS_DIR）→ True：那边的代码包里没有这些子集，不领。"""
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
