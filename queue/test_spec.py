#!/usr/bin/env python3
"""测任务说明的解析（2026-10-18；11/2 起用 testdata/ 的配置和实验表）：scripts/taskq/queue/spec.py、recipes.py、experiments.py。不连机器。

    python3 scripts/taskq/queue/test_spec.py
"""
import itertools
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
import experiments as E  # noqa: E402
import spec as S  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def raises(fn):
    try:
        fn()
    except S.SpecError:
        return True
    return False


# ---------------- 做法 toy（插件里的做法） ----------------
s = S.resolve("toy", {"ds": "D111_beta", "variant": "V_save_n470", "frag": "1"})
ok("toy：key、out、env 默认值", s["key"] == "D111_beta/V_save_n470/1"
   and s["out"] == "results/D111_beta/V_save_n470__PlanM__full/frag_1"
   and s["env"] == "base" and s["data"] == ["prepared/D111_beta"])
ok("toy：命令按参数填", s["cmd"] == "bash scripts/taskq/queue/testdata/bin/toy_train.sh D111_beta full 1 PlanM V_save_n470")
ok("toy：{data}、{repo} 留到运行时展开", "{data}" in s["vars"]["DEMO_RESULTS"] and "{repo}" in s["vars"]["DEMO_CODE"])
ok("缺必填参数（frag）：报错", raises(lambda: S.resolve("toy", {"ds": "D101_alpha"})))
ok("写错参数名：报错", raises(lambda: S.resolve("toy", {"ds": "D101_alpha", "frag": "0", "variantt": "x"})))
ok("没有这种做法：报错", raises(lambda: S.resolve("nope", {})))
s2 = S.resolve("toy", {"ds": "D101_alpha", "frag": "0", "env": "alt2", "vars": {"SPLITS_DIR": "x"}})
ok("参数覆盖同名字段：env 换掉、vars 合并", s2["env"] == "alt2" and s2["vars"]["SPLITS_DIR"] == "x" and "DEMO_RESULTS" in s2["vars"])
ok("参数给了 plan：进模型文件夹", S.resolve("toy", {"ds": "D101_alpha", "frag": "0", "plan": "PlanP"})["out"]
   == "results/D101_alpha/V_save__PlanP__full/frag_0")

# ---------------- 做法 command ----------------
c = S.resolve("command", {"key": "misc/a", "cmd": "python x.py --out {data}/misc/a", "env": "base", "out": "misc/a",
                          "done": ["model.pt"], "data": ["other/stuff"], "extra": "1"})
ok("command：任务自己给 key、cmd、out、done，可以带别的参数", c["key"] == "misc/a" and c["done"] == ["model.pt"]
   and c["data"] == ["other/stuff"] and "{data}" in c["cmd"] and c["archive"] is None)
ok("command：缺 cmd 报错", raises(lambda: S.resolve("command", {"key": "a", "env": "x", "out": "a", "done": "x"})))

# ---------------- 10/31：任务单的通用字段 ----------------
G = dict(key="guest/x", cmd="python3 t.py", env="base", out="guest/x", done="DONE")
g = S.resolve("command", dict(G, inputs="guest/in", res={"gpus": "1", "mem": "16G"}, resume="ckpt_*.pt", owner="bob",
                              progress="progress_json"))
ok("任务单：inputs = data（字符串变列表）、res 转成数、resume 变列表、owner", g["data"] == ["guest/in"] and g["inputs"] == ["guest/in"]
   and g["res"] == {"gpus": 1, "mem": "16G"} and g["resume"] == ["ckpt_*.pt"] and g["owner"] == "bob")
ok("任务单：command 默认不交给执行机、一张卡、没有存档", S.resolve("command", G)["relay"] is None
   and S.resolve("command", G)["res"] == {"gpus": 1} and S.resolve("command", G)["resume"] is None)
ok("res.gpus > 1 → 报错（worker 只给一张卡，11/1）", raises(lambda: S.resolve("command", dict(G, res={"gpus": 2}))))
ok("fits：worker 没说的不查；CPU、内存、显存超了不领", S.fits({"cpus": 64, "mem": "200G", "gpu_mem": 90})[0]
   and not S.fits({"cpus": 64}, cpus=16)[0] and not S.fits({"mem": "200G"}, mem_mb_=64 * 1024)[0]
   and not S.fits({"gpu_mem": 40}, gpu_mem=32)[0] and S.fits({"cpus": 8, "mem": "32G", "gpu_mem": 20}, 16, 64 * 1024, 80)[0]
   and S.mem_mb("2T") == 2 * 1024 ** 2)
ok("任务单：inputs 和 data 都给 → 报错", raises(lambda: S.resolve("command", dict(G, inputs=["a"], data=["b"]))))
ok("任务单：res 写错字段、不是数、mem 格式不对 → 报错", raises(lambda: S.resolve("command", dict(G, res={"gpu": 1})))
   and raises(lambda: S.resolve("command", dict(G, res={"cpus": "many"})))
   and raises(lambda: S.resolve("command", dict(G, res={"mem": "lots"}))))
w = S.resolve("command", dict(G, res={"wait_gpu": "120"}))
ok("任务单：res.wait_gpu 转成分钟数、和默认的卡数合在一起，shell 里给 TASK_WAIT_GPU；不写时为空、负数报错",
   w["res"] == {"gpus": 1, "wait_gpu": 120} and "TASK_WAIT_GPU=120" in S.shell_lines(w, "/d").splitlines()
   and "TASK_WAIT_GPU=" in S.shell_lines(S.resolve("command", G), "/d").splitlines()
   and raises(lambda: S.resolve("command", dict(G, res={"wait_gpu": -1}))))
ok("任务单：data 里有 .. → 报错", raises(lambda: S.resolve("command", dict(G, inputs=["../etc"]))))
ok("任务单：不认识的 progress → 报错", raises(lambda: S.resolve("command", dict(G, progress="tqdm"))))
ok("toy：也有 inputs、resume、一张卡、输入拷到本地，进度读插件的日志读法", s["inputs"] == s["data"] and s["resume"]
   and s["res"] == {"gpus": 1, "local_inputs": 1} and s["progress"] == "toy_log" and s["relay"] == "toy")
sl = S.shell_lines(s, "/d").splitlines()
ok("shell：TASK_INPUTS 是输入的绝对路径（数组），TASK_LOCAL 跟 res.local_inputs", f"TASK_INPUTS=(/d/prepared/{s['params']['ds'] if 'params' in s else s['data'][0].split('/')[-1]})" in sl
   and "TASK_LOCAL=1" in sl and "TASK_LOCAL=0" in S.shell_lines(S.resolve("command", G), "/d").splitlines())
raw = dict(S.resolve("command", G), params=dict(G, note="{trainer}_fold{fold}_"))
ok("shell：params 里原样留着的 {名字} 不展开、不报错（10/31 起 oldlogs.prefix 这样写）", "TASK_LOCAL=0" in S.shell_lines(raw, "/d").splitlines())
gr = tempfile.mkdtemp()
gs = {k: g.get(k) for k in S.COLLECT_FIELDS}
gd = os.path.join(gr, "guest/x")
os.makedirs(gd)
open(os.path.join(gd, "train.log"), "w").write("starting\n")
f0 = S.collect([gs], gr)["tasks"]["guest/x"]
ok("collect：还没写 PROGRESS.json、没有存档 → 没进度", not f0["resumable"] and all(r["epochs_done"] == 0 for r in f0["runs"]))
open(os.path.join(gd, "PROGRESS.json"), "w").write('{"done": 3, "total": 10}')
f1 = S.collect([gs], gr)["tasks"]["guest/x"]
ok("collect：PROGRESS.json → 做完 3 轮、共 10 轮", max(r["epochs_done"] for r in f1["runs"]) == 3
   and any(r.get("total") == 10 for r in f1["runs"]))
open(os.path.join(gd, "PROGRESS.json"), "w").write("{broken")
ok("collect：PROGRESS.json 坏了 → 不报错、当没进度", max(r["epochs_done"] for r in S.collect([gs], gr)["tasks"]["guest/x"]["runs"]) == 0)
open(os.path.join(gd, "ckpt_0003.pt"), "w").close()
ok("collect：有 resume 里的存档 → resumable", S.collect([gs], gr)["tasks"]["guest/x"]["resumable"])
# 存储挂住（11/1）：一个任务的文件读不动 → 这个任务记进 stuck、跳过，别的照常读；一共等的时间有上限
import threading  # noqa: E402
hang = threading.Event()
S.PROGRESS["hang"] = lambda p: hang.wait(30) or {}
hs = dict(gs, key="guest/hang", out="guest/hang", progress="hang")
os.makedirs(os.path.join(gr, "guest/hang"), exist_ok=True)
open(os.path.join(gr, "guest/hang", "train.log"), "w").write("x\n")
t0 = time.time()
rh = S.collect([hs, gs], gr, wait=0.5)
ok("collect：一个任务读不动 → 记进 stuck、不等它，别的任务照常", rh.get("stuck") == ["guest/hang"] and "guest/x" in rh["tasks"]
   and "guest/hang" not in rh["tasks"] and time.time() - t0 < 5)
ok("collect：都读得动 → 没有 stuck", "stuck" not in S.collect([gs], gr, wait=5))
hang.set()
del S.PROGRESS["hang"]
import relay as RL  # noqa: E402
import refill as RF  # noqa: E402
ok("relay.progress：只有存档（没有轮数）也算有进度", RL.progress(dict(final=False, runs=[dict(epochs_done=0)], resumable=True))
   and not RL.progress(dict(final=False, runs=[dict(epochs_done=0)], resumable=False)))
src = {"src": {"lab": {"tasks": {"guest/x": dict(final=False, runs=[dict(epochs_done=0, end=None)], resumable=True)}},
               "dicc": {"tasks": {}}}}
ok("refill.progress：只有存档也算有进度，按机器分", RF.progress(src, "guest/x", "lab") and not RF.progress(src, "guest/x", "dicc"))

# ---------------- 训完的判断 ----------------
root = tempfile.mkdtemp()
out = os.path.join(root, s["out"])
os.makedirs(os.path.join(out, "check"))
ok("什么都没有：没训完", not S.is_done(s, root))
open(os.path.join(out, "check", "summary.json"), "w").write("{}")
ok("只有 summary.json：没训完", not S.is_done(s, root))
open(os.path.join(out, "ARCHIVED"), "w").close()
ok("summary.json + ARCHIVED（| 里有一个就行）：训完", S.is_done(s, root))
def oserr(fn):
    try:
        fn()
    except OSError:
        return True
    return False


ok("数据目录不存在（盘没挂上）：报错，不当成没训完", oserr(lambda: S.is_done(s, os.path.join(root, "nope"))))
real_stat = os.stat


def broken_stat(p, *a, **k):
    if str(p).startswith(out):
        raise OSError(70, "Communication error on send")
    return real_stat(p, *a, **k)


os.stat = broken_stat
ok("结果目录读不了（BeeGFS 断开，errno 70）：报错", oserr(lambda: S.is_done(s, root)))
os.stat = real_stat

# ---------------- 命令行参数来回 ----------------
args = S.kv_args("command", {"key": "a", "done": ["x", "y|z"]})
ok("kv_args → parse_kv 来回一致（列表用 JSON）", S.parse_kv(args[1:]) == {"key": "a", "done": ["x", "y|z"]})

# ---------------- 实验表 ----------------
ts = E.tasks()
exps = E.load()[0]
ok(f"实验表展开 {len(ts)} 个任务（= 各实验 each 的组合数之和）、key 不重复",
   len(ts) == sum(len(list(itertools.product(*(e.get("each") or {}).values()))) for e in exps) and len(set(ts)) == len(ts))
_rt = E.resolve_task
E.resolve_task = lambda recipe, params, exp=None: dict(key="/".join(f"{k}={v}" for k, v in sorted(params.items())),  # noqa: E731
                                                     names={k: str(v) for k, v in params.items()})
_ex = dict(id="gx", exp="GX", recipe="command", params=dict(p="1"), each=dict(seed=["1", "2"], frag=["0", "1", "2"]))
_t = E.expand(_ex)
ok("each：按参数的每种组合展开（2 × 3），参数进 params、each 记下取值",
   len(_t) == 6 and _t[0]["params"] == dict(p="1", seed="1", frag="0") and _t[-1]["each"] == dict(seed="2", frag="2"))
ok("each：only 只展开给的取值；only 里的参数不在 each 里 → 报错",
   [t["each"] for t in E.expand(_ex, dict(frag=["2"]))] == [dict(seed="1", frag="2"), dict(seed="2", frag="2")]
   and raises(lambda: E.expand(_ex, dict(lr=["1"]))))
ok("each：标签 = id + 每个参数的 _<首字母><取值>；没有 each → 只有 id",
   E.label(_ex, _t[1]) == "gx_s1_f1" and E.label(dict(id="gy"), E.expand(dict(id="gy", exp="GY", recipe="command", params={}))[0]) == "gy")
_g = E.group_names(_ex, "/d", "py")
ok("each：实验一级的名字有 <参数>_list、<参数>_list_q",
   _g["frag_list"] == "0 1 2" and _g["frag_list_q"] == "'0 1 2'" and _g["seed_list"] == "1 2" and "frag" not in _g and _g["p"] == "1")
E.resolve_task = _rt
_lab = {E.label(E.experiment(t["exp"]), t) for t in ts.values()}
ok("实验表的标签 = <id>_f<份>",
   _lab == {f"{t['exp']}_f{t['params']['frag']}" if "frag" in t["params"] else t["exp"] for t in ts.values()})
ok("每个任务都解析得了、有环境和输出", all(t["spec"].get("env") and t["spec"].get("out") for t in ts.values()))
um = ts["D101_alpha/V_m/1"]["spec"]
sw = ts["D101_alpha/V_s/0"]["spec"]
ok("arch_m、arch_s 写明了自己的环境", um["env"] == "alt2" and sw["env"] == "alt")
ok("toy 的任务 key = ds/variant/frag，out 是 results/<模型>/frag_<份>",
   all(t["spec"]["out"] == "results/{0}/{1}__{p}__full/frag_{2}".format(*k.split("/"), p=t["params"].get("plan", "PlanM"))
       for k, t in ts.items() if t["recipe"] == "toy"))
ok("实验表的 metrics 进了说明（登记表读）", set(um.get("metrics", {})) == {"val_A", "val_B"})
ok("env_of：按实验表找模型的环境",
   E.env_of("/x/D101_alpha/V_s__PlanM__full") == "alt")
try:
    E.env_of("/x/D999_x/foo")
    ok("env_of：表里没有的模型报错（不默认 base）", False)
except S.SpecError as err:
    ok("env_of：表里没有的模型报错（不默认 base），提示设 MODEL_ENV", "MODEL_ENV" in str(err))
r = subprocess.run([sys.executable, os.path.join(HERE, "experiments.py"), "env-of", "/x/D999_x/foo"], capture_output=True, text=True)
ok("experiments.py env-of：表里没有 → 退出码非 0、说明在标准错误（predict.sh 因此停下）", r.returncode != 0 and "MODEL_ENV" in r.stderr and not r.stdout)


# ---------------- 实验的声明（10/24）：别的脚本按模型文件夹查，不按名字猜 ----------------
M = "/x/{}/V_save__PlanM__full"
ok("names_of：t24cov 跳过 G 的四个外部集、用 t24_splits.json",
   E.names_of(M.format("D155_gamma_ecov"), "skip_ext", "splits") == ["G-A G-B G-C G-D", "t24_splits.json"])
ok("names_of：t22a 只测留出的 G-A", E.names_of(M.format("D151_gamma_loA"), "only_ext") == ["G-A"])
ok("names_of：t0 的模型什么都不跳过、没有划分", E.names_of(M.format("D101_alpha"), "skip_ext", "only_ext", "splits") == ["", "", ""])
ok("names_of：表里没有的模型报错（不猜）", raises(lambda: E.names_of("/x/D999_x/foo", "skip_ext")))
ok("每个实验都有档位和看板名称", all(isinstance(e.get("nice"), int) and set(e.get("board") or {}) == {"group", "en", "zh"}
                                for e in exps if e["rq"] == "demo"))
ok("有 oof 步骤的都给了划分文件", all(e["names"]["splits"] for e in exps if e["rq"] == "demo"
                                     and any(s["name"] == "oof" for c in e["group_after"] for s in c)))
import importlib.util  # noqa: E402
spec_ = importlib.util.spec_from_file_location("demo_exps", os.path.join(fixture.EXPS, "demo", "experiments.py"))
R = importlib.util.module_from_spec(spec_)
spec_.loader.exec_module(R)
x = R._x("toy", "T99", "玩具", "base", "D999_x", ("G", "en", "zh"), 0, steps=R.OOF5 + ["ens"] + R.PROBE, splits="s.json", probe="p")
ok("_x：steps 展开成每份的步骤和一串串实验级步骤（paired 接 oof、p2cmp 接 p2）",
   [s["name"] for s in x["after"]] == ["infer", "eval"]
   and [[s["name"] for s in c] for c in x["group_after"]] == [["oof", "paired"], ["ens"], ["p1"], ["p2", "p2cmp"]])


def bad(**kw):
    try:
        R._x("toy", "T99", "玩具", "base", "D999_x", ("G", "en", "zh"), 0, **kw)
    except ValueError:
        return True
    return False


ok("_x：写错步骤名、oof 没给 splits、paired 不跟在 oof 后面、P1 没给 probe 都报错",
   bad(steps=["infer", "evl"]) and bad(steps=["oof"]) and bad(steps=["paired"], splits="s.json") and bad(steps=["p1"]))

# 模型专用的读法放在 readers/（10/31）：本机加载后表里有；remote_collect 送到别处（经 stdin，没有 readers/ 目录）也能读
ok("readers：toy_log、toy_debug 加载进 PROGRESS、INFO，有训完一轮的字样",
   "toy_log" in S.PROGRESS and "toy_debug" in S.INFO and S.TRAINED)
ok("readers：重名的报错", raises(lambda: S.load_readers({"a.py": "PROGRESS={'toy_log': None}"})))
_dup = tempfile.mkdtemp()
os.makedirs(os.path.join(_dup, "readers"))
open(os.path.join(_dup, "readers", "toy.py"), "w").write("PROGRESS = {}\n")
_pl = os.environ.get("TASKQ_PLUGINS")
os.environ["TASKQ_PLUGINS"] = os.pathsep.join(filter(None, [_pl, _dup]))
try:
    ok("readers：两个插件文件夹里有同名的 readers/toy.py 时报错，不让后一个顶掉前一个", raises(S.reader_sources))
finally:
    os.environ.pop("TASKQ_PLUGINS")
    if _pl is not None:
        os.environ["TASKQ_PLUGINS"] = _pl
rr = tempfile.mkdtemp()
ns = S.resolve("toy", {"ds": "D1_x", "variant": "tr", "frag": "0"})
od = os.path.join(rr, ns["out"])
os.makedirs(od)
open(os.path.join(od, "train_log_1.txt"), "w").write(
    "2026-10-01 10:00:00.1: Epoch 0\n2026-10-01 10:05:00.1: Epoch time: 300 s\n2026-10-01 10:05:01.1: Epoch 1\n")
os.makedirs(os.path.join(rr, ns["oldlogs"]["dir"]))
open(os.path.join(rr, ns["oldlogs"]["dir"], ns["oldlogs"]["prefix"] + "123_seg1.log"), "w").write("作业 123_1\nx: Epoch time: 300 s\n")
_run = S.subprocess.run
S.subprocess.run = lambda cmd, **kw: _run([sys.executable, "-"], **kw)
try:
    rc = S.remote_collect("host", [{k: ns.get(k) for k in S.COLLECT_FIELDS}], rr)
finally:
    S.subprocess.run = _run
ok("remote_collect：经 stdin 跑时 readers 一起送过去（toy_log 读出训完 1 轮，oldlogs 目录里的分段日志算训完一轮）",
   rc and rc["tasks"][ns["key"]]["runs"][0]["epochs_done"] == 1
   and [x["trained"] for x in rc["seglogs"] if x["name"].startswith(ns["oldlogs"]["prefix"])] == [True])

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
