"""读所有研究问题的实验表（research/*/experiments.py，2026-10-18），展开成一个个任务。
taskq.py（add --exp、解析队列里的任务）、advance.py（训完以后的步骤）、make_registry.py（登记表）都从这里读；
分析脚本按模型文件夹查所属实验的声明（name-of、names_of）。

实验的字段：id、exp、label、model、hours、recipe、params、fields、folds、after、group_after、names（见 RQ1 的表），
另外 nice（taskq.py add --exp 不给 --nice 时的档位）、board（看板用，各 RQ 自己定）可选。"""
import glob
import importlib.util
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
import spec  # noqa: E402

_cache = {}


def load():
    """→ (实验列表, 结束步骤列表)。每个实验加上 rq（所在的研究问题文件夹）。id、任务名重复时报错。"""
    if "exps" in _cache:
        return _cache["exps"], _cache["final"]
    exps, final = [], []
    for path in sorted(glob.glob(os.path.join(REPO, "research", "*", "experiments.py"))):
        rq = os.path.basename(os.path.dirname(path))
        sp = importlib.util.spec_from_file_location(f"experiments_{rq}", path)
        mod = importlib.util.module_from_spec(sp)
        sp.loader.exec_module(mod)
        for e in mod.EXPERIMENTS:
            exps.append(dict(e, rq=rq))
        final += [dict(f, rq=rq) for f in getattr(mod, "FINAL", [])]
    ids = [e["id"] for e in exps]
    dup = sorted({i for i in ids if ids.count(i) > 1})
    if dup:
        raise spec.SpecError(f"实验表里 id 重复：{', '.join(dup)}")
    _cache["exps"], _cache["final"] = exps, final
    return exps, final


def experiment(eid):
    return next((e for e in load()[0] if e["id"] == eid), None)


def resolve_task(recipe, params, exp=None):
    """队列里的任务（做法、参数、所属实验）→ 说明。属于某个实验时，加上实验的 fields（如 metrics）。"""
    e = experiment(exp) if exp else None
    return spec.resolve(recipe, {**((e or {}).get("fields") or {}), **params}, exp=exp)


def expand(e, folds=None):
    """一个实验 → [dict(key, exp, recipe, params, spec, fold)]（每折一个；folds 只展开这几折）。"""
    out = []
    for f in (folds or e.get("folds") or [None]):
        params = dict(e["params"], **({"fold": f} if f is not None else {}))
        s = resolve_task(e["recipe"], params, e["id"])
        out.append(dict(key=s["key"], exp=e["id"], recipe=e["recipe"], params=params, spec=s, fold=f))
    return out


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
    """实验一级步骤（group_after、FINAL）的模板名字：各折都一样的参数 + exp、id、folds（空格隔开）、folds_q（加了引号的）
    + 实验的 names + data、python。"""
    ts = expand(e)
    common = {k: v for k, v in ts[0]["spec"]["names"].items() if all(t["spec"]["names"].get(k) == v for t in ts)}
    folds = " ".join(e.get("folds") or [])
    return dict(common, exp=e["exp"], id=e["id"], folds=folds, folds_q=shlex.quote(folds), **(e.get("names") or {}),
                data=data, python=python)


def task_names(t, data, python):
    """每折步骤（after）的模板名字：任务的参数 + exp、id + data、python。"""
    e = experiment(t["exp"]) or {}
    return dict(t["spec"]["names"], exp=e.get("exp", ""), id=t["exp"], data=data, python=python)


def _task_of(model_dir):
    """模型文件夹（…/<数据集>/<训练器>__<plans>__<配置>，放在哪里都行）→ 实验表里训它的第一个任务；表里没有 → None。"""
    rel = "/".join(os.path.normpath(model_dir).split(os.sep)[-2:])
    return next((t for t in tasks().values() if t["spec"].get("names", {}).get("model") == rel), None)


def env_of(model_dir):
    """模型文件夹 → 实验表里训它用的环境；表里没有 → nnunet。"""
    t = _task_of(model_dir)
    return t["spec"]["env"] if t else "nnunet"


def experiment_of(model_dir):
    """模型文件夹 → 训它的实验；表里没有 → None。"""
    t = _task_of(model_dir)
    return experiment(t["exp"]) if t else None


def names_of(model_dir, *names):
    """模型文件夹 → 它所属实验的 names 里这几项（没有的给空串），分析脚本按这个取实验的声明（如 RQ1 的 skip_ext、splits）。
    表里没有这个模型时报 SpecError：不按名字猜。"""
    e = experiment_of(model_dir)
    if e is None:
        raise spec.SpecError(f"实验表里没有训这个模型的实验：{model_dir}")
    return [str((e.get("names") or {}).get(n, "")) for n in names]


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "env-of":
        print(env_of(sys.argv[2]))
    elif len(sys.argv) >= 4 and sys.argv[1] == "name-of":
        try:
            print("\n".join(names_of(sys.argv[2], *sys.argv[3:])))
        except spec.SpecError as err:
            sys.exit(str(err))
    else:
        sys.exit("用法：experiments.py env-of <模型文件夹>\n      experiments.py name-of <模型文件夹> <名字>…（每行一个）")
