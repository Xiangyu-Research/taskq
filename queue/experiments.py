"""读所有实验表（envs/policy.py 的 OUTPUTS["experiments"]，相对仓库根目录的 glob；2026-10-18），展开成一个个任务。
taskq.py（add --exp、解析队列里的任务）、advance.py（训完以后的步骤）、make_registry.py（登记表）都从这里读；
分析脚本按模型文件夹查所属实验的声明（name-of、names_of）。

实验的字段：id、exp、label、model、hours、recipe、params、fields、each、after、group_after、names（见各实验表），
另外 nice（taskq.py add --exp 不给 --nice 时的档位）、board（看板用，各 RQ 自己定）可选。
each：{参数: [取值…]}，一个实验按这些参数的每种组合展开成一组任务（如 {"seed": ["0", "1"]}）；没有就是一个任务。
表的模块里另外可以有 FINAL（所有实验做完后的步骤）、KEEP_DATA（keep_data()）。"""
import glob
import importlib.util
import itertools
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
import spec  # noqa: E402

_cache = {}


def load():
    """→ (实验列表, 结束步骤列表)。每个实验加上 rq（实验表所在的文件夹名）。id、任务名重复时报错。"""
    if "exps" in _cache:
        return _cache["exps"], _cache["final"]
    exps, final, keep = [], [], []
    pat = cfg.outputs("experiments")
    for path in sorted(glob.glob(os.path.join(REPO, pat))) if pat else []:
        rq = os.path.basename(os.path.dirname(path))
        sp = importlib.util.spec_from_file_location(f"experiments_{rq}", path)
        mod = importlib.util.module_from_spec(sp)
        sp.loader.exec_module(mod)
        for e in mod.EXPERIMENTS:
            exps.append(dict(e, rq=rq))
        final += [dict(f, rq=rq) for f in getattr(mod, "FINAL", [])]
        keep += [k for k in getattr(mod, "KEEP_DATA", []) if k not in keep]
    ids = [e["id"] for e in exps]
    dup = sorted({i for i in ids if ids.count(i) > 1})
    if dup:
        raise spec.SpecError(f"实验表里 id 重复：{', '.join(dup)}")
    _cache["exps"], _cache["final"], _cache["keep"] = exps, final, keep
    return exps, final


def keep_data():
    """各实验表的 KEEP_DATA：自动传到训练机后不按空间预算删的数据（最后一级目录名的 fnmatch 模式）。"""
    load()
    return list(_cache.get("keep") or [])


def experiment(eid):
    return next((e for e in load()[0] if e["id"] == eid), None)


def resolve_task(recipe, params, exp=None):
    """队列里的任务（做法、参数、所属实验）→ 说明。属于某个实验时，加上实验的 fields（如 metrics）。"""
    e = experiment(exp) if exp else None
    return spec.resolve(recipe, {**((e or {}).get("fields") or {}), **params}, exp=exp)


def expand(e, only=None):
    """一个实验 → [dict(key, exp, recipe, params, spec, each)]（each 的每种组合一个；only={参数: [取值]} 只展开这些取值）。"""
    each = {k: [v for v in vs if not only or k not in only or v in only[k]] for k, vs in (e.get("each") or {}).items()}
    bad = sorted(set(only or {}) - set(each))
    if bad:
        raise spec.SpecError(f"{e['id']} 不按 {'、'.join(bad)} 展开（each 里有：{'、'.join(each) or '没有'}）")
    out = []
    for vals in itertools.product(*each.values()):
        pick = dict(zip(each, vals))
        params = dict(e["params"], **pick)
        s = resolve_task(e["recipe"], params, e["id"])
        out.append(dict(key=s["key"], exp=e["id"], recipe=e["recipe"], params=params, spec=s, each=pick))
    return out


def short(t):
    """任务在实验里的简称：each 的每个参数写成 <参数首字母><取值>，空格隔开（如 "f1"、"s2 f0"）；没有 each → ""。"""
    return " ".join(f"{k[0]}{v}" for k, v in (t.get("each") or {}).items())


def label(e, t):
    """box pueue 任务标签里的任务名：实验 id + 简称的每一项前面加 "_"（如 t3met_f1）。"""
    return e["id"] + "".join("_" + x for x in short(t).split())


def parse_label(who):
    """标签里的任务名 → (实验, 任务)；实验一级的（只有 id）→ (实验, None)；认不出 → (None, None)。"""
    if "by_label" not in _cache:
        _cache["by_label"] = {label(experiment(t["exp"]), t): t for t in tasks().values()}
    t = _cache["by_label"].get(who)
    if t:
        return experiment(t["exp"]), t
    return experiment(who), None


def tasks():
    """所有实验的所有任务：{key: 任务}，按实验表的顺序。任务名重复时报错。"""
    if "tasks" in _cache:
        return _cache["tasks"]
    out = {}
    for e in load()[0]:
        for t in expand(e):
            if t["key"] in out:
                raise spec.SpecError(f"实验表里任务名重复：{t['key']}（{out[t['key']]['exp']} 和 {e['id']}）")
            out[t["key"]] = t
    _cache["tasks"] = out
    return out


def group_names(e, data, python):
    """实验一级步骤（group_after、FINAL）的模板名字：各任务都一样的参数 + exp、id + each 的每个参数 <参数>_list（取值空格隔开）、
    <参数>_list_q（加了引号的） + 实验的 names + data、python。"""
    ts = expand(e)
    common = {k: v for k, v in ts[0]["spec"]["names"].items() if all(t["spec"]["names"].get(k) == v for t in ts)}
    lists = {}
    for k, vs in (e.get("each") or {}).items():
        lists[f"{k}_list"] = " ".join(vs)
        lists[f"{k}_list_q"] = shlex.quote(" ".join(vs))
    return dict(common, exp=e["exp"], id=e["id"], **lists, **(e.get("names") or {}), data=data, python=python)


def task_names(t, data, python):
    """每个任务的步骤（after）的模板名字：任务的参数 + exp、id + data、python。"""
    e = experiment(t["exp"]) or {}
    return dict(t["spec"]["names"], exp=e.get("exp", ""), id=t["exp"], data=data, python=python)


def _task_of(model_dir):
    """模型文件夹（…/<数据集>/<训练器>__<plans>__<配置>，放在哪里都行）→ 实验表里训它的第一个任务；表里没有 → None。"""
    rel = "/".join(os.path.normpath(model_dir).split(os.sep)[-2:])
    return next((t for t in tasks().values() if t["spec"].get("names", {}).get("model") == rel), None)


def env_of(model_dir):
    """模型文件夹 → 实验表里训它用的环境；表里没有时报 SpecError（实验表以外的模型由调用的一方设 MODEL_ENV）。"""
    t = _task_of(model_dir)
    if t is None:
        raise spec.SpecError(f"实验表里没有训这个模型的实验：{model_dir}（实验表以外的模型设 MODEL_ENV）")
    return spec.envs_of(t["spec"])[0]


def experiment_of(model_dir):
    """模型文件夹 → 训它的实验；表里没有 → None。"""
    t = _task_of(model_dir)
    return experiment(t["exp"]) if t else None


def names_of(model_dir, *names):
    """模型文件夹 → 它所属实验的 names 里这几项（没有的给空串），分析脚本按这个取实验的声明（如 skip_ext、splits）。
    表里没有这个模型时报 SpecError：不按名字猜。"""
    e = experiment_of(model_dir)
    if e is None:
        raise spec.SpecError(f"实验表里没有训这个模型的实验：{model_dir}")
    return [str((e.get("names") or {}).get(n, "")) for n in names]


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "env-of":
        try:
            print(env_of(sys.argv[2]))
        except spec.SpecError as err:
            sys.exit(str(err))
    elif len(sys.argv) >= 4 and sys.argv[1] == "name-of":
        try:
            print("\n".join(names_of(sys.argv[2], *sys.argv[3:])))
        except spec.SpecError as err:
            sys.exit(str(err))
    else:
        sys.exit("用法：experiments.py env-of <模型文件夹>\n      experiments.py name-of <模型文件夹> <名字>…（每行一个）")
