"""读所有实验表（配置文件夹里 policy.py 的 OUTPUTS["experiments"]，相对仓库根目录的 glob），展开成一个个任务。
taskq.py（add --exp、解析队列里的任务）、flow/compile.py（步骤的实例和先后）、登记表都从这里读；
分析脚本按模型文件夹查所属实验的声明（name-of、names_of）。

字段见 queue/schema.py：实验 EXP_FIELDS，步骤（after、group_after、FINAL 里的）STEP_FIELDS；加载时核对，不认识的、缺必写的报错。
步骤的模板里能用的名字：每个任务的步骤是任务的参数 + exp、id（task_names）；实验一级的步骤是各任务都一样的参数 + exp、id
+ each 的每个参数 <参数>_list（取值空格隔开）、<参数>_list_q（加了引号的） + 实验的 names（group_names）；FINAL 只有 data。
{data} 留到执行机上换成那边的数据目录。表的模块里另外可以有 FINAL（不属于哪个实验的步骤）。"""
import glob
import importlib.util
import itertools
import json
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
import spec  # noqa: E402
import schema  # noqa: E402
import recipes as R  # noqa: E402  核心的做法表；要在实验表把插件文件夹加进 sys.path 之前加载，免得同名插件顶掉

_cache = {}


def _check_fields(where, d, table):
    if not isinstance(d, dict):
        raise spec.SpecError(f"{where}：要写成 dict")
    bad = sorted(set(d) - set(table))
    if bad:
        raise spec.SpecError(f"{where}：不认识的字段 {'、'.join(bad)}（允许：{'、'.join(table)}；见 taskq machines fields）")
    miss = [k for k, v in table.items() if v[1] is schema.REQUIRED and k not in d]
    if miss:
        raise spec.SpecError(f"{where}：缺必写的字段 {'、'.join(miss)}")


def step(st):
    """步骤补上默认值；out、done、inputs、env、temp 写成列表。"""
    st = schema.defaults(schema.STEP_FIELDS, st)
    for k in ("out", "done", "inputs", "env", "temp"):
        v = st.get(k)
        st[k] = [v] if isinstance(v, str) else list(v or [])
    return st


def check_fields(path, mod):
    """实验表模块里每个实验、步骤的字段都认识、必写的都有；不对的报 SpecError。"""
    steps = [(f"{path} 的 FINAL 的步骤 {st.get('name')}", st) for st in getattr(mod, "FINAL", [])]
    for e in mod.EXPERIMENTS:
        where = f"{path} 的实验 {e.get('id')}"
        _check_fields(where, e, schema.EXP_FIELDS)
        each = e.get("each") or {}
        for st in e.get("after") or []:
            only = st.get("only")
            if only is None:
                continue
            bad = [k for k in (only if isinstance(only, dict) else ["?"]) if k not in each]
            if bad or not all(isinstance(v, (list, tuple)) for v in only.values()):
                raise spec.SpecError(f"{where} 的步骤 {st.get('name')}：only 要写成 {{each 的参数: [取值…]}}（each 里有：{'、'.join(each) or '没有'}）")
        for st in e.get("group_after") or []:
            if not isinstance(st, dict):
                raise spec.SpecError(f"{where} 的 group_after：每项是一个步骤（dict），先后按输入推，不用分串")
            if "only" in st:
                raise spec.SpecError(f"{where} 的步骤 {st.get('name')}：only 只用在每个任务的步骤（after）上")
        steps += [(f"{where} 的步骤 {st.get('name')}", st) for st in list(e.get("after") or []) + list(e.get("group_after") or [])]
    seen = set()
    for where, st in steps:
        if id(st) in seen:
            continue
        seen.add(id(st))
        _check_fields(where, st, schema.STEP_FIELDS)
        if st.get("device", "gpu") not in ("gpu", "cpu"):
            raise spec.SpecError(f"{where}：device 只能是 gpu 或 cpu")


def load():
    """→ (实验列表, FINAL 的步骤列表)，都补上了默认值；每个实验加上 rq（实验表所在的文件夹名）。id、任务名重复时报错。"""
    if "exps" in _cache:
        return _cache["exps"], _cache["final"]
    exps, final = [], []
    pat = cfg.outputs("experiments")
    for path in sorted(glob.glob(os.path.join(REPO, pat))) if pat else []:
        rq = os.path.basename(os.path.dirname(path))
        sp = importlib.util.spec_from_file_location(f"experiments_{rq}", path)
        mod = importlib.util.module_from_spec(sp)
        sp.loader.exec_module(mod)
        check_fields(os.path.relpath(path, REPO), mod)
        for e in mod.EXPERIMENTS:
            e = schema.defaults(schema.EXP_FIELDS, e)
            exps.append(dict(e, rq=rq, after=[step(x) for x in e["after"]], group_after=[step(x) for x in e["group_after"]]))
        final += [step(f) for f in getattr(mod, "FINAL", [])]
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


def applies(st, t):
    """步骤 st 对任务 t 做不做：没有 only 都做；有的话 t 的 each 每个参数都要在 only 列的取值里。"""
    only = st.get("only") or {}
    each = t.get("each") or {}
    return all(str(each.get(k)) in [str(v) for v in vs] for k, vs in only.items())


def steps_of(t, e=None):
    """任务 t 训完以后要做的步骤（实验的 after 里对它适用的，按顺序）；不属于实验的任务 → []。"""
    e = e if e is not None else experiment(t.get("exp")) if t.get("exp") else None
    return [st for st in (e or {}).get("after") or [] if applies(st, t)]


def short(t):
    """任务在实验里的简称：each 的每个参数写成 <参数首字母><取值>，空格隔开（如 "f1"、"s2 f0"）；没有 each → ""。"""
    return " ".join(f"{k[0]}{v}" for k, v in (t.get("each") or {}).items())


def label(e, t):
    """hub 的 pueue 任务标签里的任务名：实验 id + 简称的每一项前面加 "_"（如 small_s1）。"""
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


def group_names(e):
    """实验一级步骤（group_after）的模板名字：各任务都一样的参数 + exp、id + each 的每个参数 <参数>_list（取值空格隔开）、
    <参数>_list_q（加了引号的） + 实验的 names。"""
    ts = expand(e)
    common = {k: v for k, v in ts[0]["spec"]["names"].items() if all(t["spec"]["names"].get(k) == v for t in ts)}
    lists = {}
    for k, vs in (e.get("each") or {}).items():
        lists[f"{k}_list"] = " ".join(vs)
        lists[f"{k}_list_q"] = shlex.quote(" ".join(vs))
    return dict(common, exp=e["exp"], id=e["id"], **lists, **(e.get("names") or {}))


def task_names(t):
    """每个任务的步骤（after）的模板名字：任务的参数 + exp、id。"""
    e = experiment(t["exp"]) or {}
    return dict(t["spec"]["names"], exp=e.get("exp", ""), id=t["exp"])


class NotFound(spec.SpecError):
    """实验表里没有（命令行退出码 proto.EXIT["gone"]）。"""


def _task_of(model_dir):
    """模型文件夹（放在哪里都行）→ 实验表里训它的第一个任务（做法插件的 model_of 钩子给出 model 参数）；表里没有 → None。"""
    if "model_of" not in R.HOOKS:
        raise spec.SpecError("做法插件里没有 model_of 钩子（recipes.py 的 HOOKS），按模型文件夹查不了实验表")
    m = R.HOOKS["model_of"](model_dir)
    return next((t for t in tasks().values() if t["spec"].get("names", {}).get("model") == m), None)


def env_of(model_dir):
    """模型文件夹 → 实验表里训它用的环境；表里没有时报 SpecError（实验表以外的模型由调用的一方设 MODEL_ENV）。"""
    t = _task_of(model_dir)
    if t is None:
        raise NotFound(f"实验表里没有训这个模型的实验：{model_dir}（实验表以外的模型设 MODEL_ENV）")
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
        raise NotFound(f"实验表里没有训这个模型的实验：{model_dir}")
    return [str((e.get("names") or {}).get(n, "")) for n in names]


# ---------------- 命令行 ----------------
def jsonable(v):
    """实验表里的值 → 能写成 JSON 的：函数、类写成名字，别的对象（如步骤的 done）写成 {"type": 类名, 属性…}（属性里的函数也写成名字）。"""
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, dict):
        return {str(k): jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [jsonable(x) for x in (sorted(v, key=str) if isinstance(v, (set, frozenset)) else v)]
    if callable(v) and hasattr(v, "__name__"):
        return getattr(v, "__qualname__", v.__name__)
    attrs = {k: jsonable(x) for k, x in vars(v).items() if not k.startswith("_")} if hasattr(v, "__dict__") else {}
    return dict(type=type(v).__name__, **attrs)


def task_json(t):
    """一个展开的任务 → proto.EXP_TASK_FIELDS。"""
    e = experiment(t["exp"]) or {}
    return dict(key=t["key"], exp=t["exp"], recipe=t["recipe"], params=jsonable(t["params"]), each=jsonable(t.get("each") or {}),
                short=short(t), label=label(e, t) if e else t["key"], names=jsonable(t["spec"].get("names") or {}),
                spec=jsonable(t["spec"]))


def _line(obj):
    print(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))


def main(argv):
    """env-of、name-of（执行机上的脚本用，输出不变）；list、tasks（--json 一行 JSON，字段见 proto.EXP_*）。
    退出码：0，1 出错，2 用法错，实验表里没有 proto.EXIT["gone"]。"""
    import argparse
    import proto
    prog = os.path.basename(sys.argv[0])  # experiments.py，或 taskq exp

    def fail(err):
        print(err, file=sys.stderr)
        return proto.EXIT["gone"] if isinstance(err, NotFound) else 1
    if argv[:1] == ["env-of"] or argv[:1] == ["name-of"]:
        try:
            if argv[0] == "env-of" and len(argv) == 2:
                print(env_of(argv[1]))
                return 0
            if argv[0] == "name-of" and len(argv) >= 3:
                print("\n".join(names_of(argv[1], *argv[2:])))
                return 0
        except spec.SpecError as err:
            return fail(err)
        print(USAGE.replace("experiments.py", prog), file=sys.stderr)
        return 2
    ap = argparse.ArgumentParser(prog=prog, description="实验表（OUTPUTS[\"experiments\"]）",
                                 epilog=USAGE.replace("experiments.py", prog), formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("list", help="实验表的条目")
    p.add_argument("--json", action="store_true", help="一行 JSON（proto.EXP_LIST_TOP）")
    p = sub.add_parser("tasks", help="展开后的任务")
    p.add_argument("--exp", nargs="+", metavar="ID", help="只列这几个实验的")
    p.add_argument("--json", action="store_true", help="一行 JSON（proto.EXP_TASKS_TOP，每个任务 proto.EXP_TASK_FIELDS）")
    a = ap.parse_args(argv)
    try:
        exps, final = load()
        if a.cmd == "list":
            if a.json:
                _line(dict(version=proto.VERSION, experiments=jsonable(exps), final=jsonable(final)))
            else:
                n = {}
                for t in tasks().values():
                    n[t["exp"]] = n.get(t["exp"], 0) + 1
                for e in exps:
                    print(f"{e['id']}\t{e.get('exp', '')}\t{e.get('recipe', '')}\t{n.get(e['id'], 0)} 个任务\t{e.get('label', '')}")
            return 0
        want = a.exp or []
        bad = [x for x in want if experiment(x) is None]
        if bad:
            raise NotFound(f"实验表里没有 {'、'.join(bad)}")
        ts = [t for t in tasks().values() if not want or t["exp"] in want]
        if a.json:
            _line(dict(version=proto.VERSION, tasks=[task_json(t) for t in ts]))
        else:
            for t in ts:
                print(f"{t['key']}\t{t['exp']}\t{label(experiment(t['exp']), t)}")
        return 0
    except spec.SpecError as err:
        return fail(err)


USAGE = """用法：experiments.py env-of <模型文件夹>
      experiments.py name-of <模型文件夹> <名字>…（每行一个）
      experiments.py list [--json]
      experiments.py tasks [--exp ID…] [--json]"""


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
