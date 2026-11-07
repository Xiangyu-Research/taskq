#!/usr/bin/env python3
"""测任务说明的解析（用 testdata/ 的配置和实验表）：queue/spec.py、recipes.py、experiments.py。不连机器。

    python3 queue/test_spec.py
"""
import itertools
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
fixture.use()
import experiments as E  # noqa: E402
import spec as S  # noqa: E402
import proto  # noqa: E402
import cfg  # noqa: E402
QR = cfg.rel("queue")  # 仓库根下调度代码的 queue/（嵌着时 scripts/taskq/queue，单独的检出时 queue）

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
   and s["env"] == "base" and s["inputs"] == ["prepared/D111_beta"])
ok("toy：命令按参数填", s["cmd"] == f"bash {QR}/testdata/bin/toy_train.sh D111_beta full 1 PlanM V_save_n470")
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
                          "done": ["model.pt"], "inputs": ["other/stuff"], "extra": "1"})
ok("command：任务自己给 key、cmd、out、done，可以带别的参数", c["key"] == "misc/a" and c["done"] == ["model.pt"]
   and c["inputs"] == ["other/stuff"] and "{data}" in c["cmd"] and c["archive"] is False)
ok("command：缺 cmd 报错", raises(lambda: S.resolve("command", {"key": "a", "env": "x", "out": "a", "done": "x"})))

# ---------------- 任务单的通用字段 ----------------
G = dict(key="guest/x", cmd="python3 t.py", env="base", out="guest/x", done="DONE")
g = S.resolve("command", dict(G, inputs="guest/in", res={"gpus": "1", "mem": "16G"}, resume="ckpt_*.pt", owner="bob",
                              progress="progress_json"))
ok("任务单：inputs（字符串变列表）、res 转成数、resume 变列表、owner", g["inputs"] == ["guest/in"]
   and g["res"] == {"gpus": 1, "mem": "16G"} and g["resume"] == ["ckpt_*.pt"] and g["owner"] == "bob")
g0 = S.resolve("command", G)
ok("任务单：command 默认输入就是 inputs 里的（relay=path）、要卡、res 空、没有存档、不归档、不限机器",
   g0["relay"] == "path" and g0["device"] == "gpu" and g0["res"] == {} and g0["resume"] is None and g0["archive"] is False
   and g0["machines"] is None and g0["inputs"] == [])
ok("res.gpus > 1 → 报错（worker 只给一张卡）", raises(lambda: S.resolve("command", dict(G, res={"gpus": 2}))))
ok("fits：worker 没说的不查；CPU、内存、显存超了不领", S.fits({"cpus": 64, "mem": "200G", "gpu_mem": 90})[0]
   and not S.fits({"cpus": 64}, cpus=16)[0] and not S.fits({"mem": "200G"}, mem_mb_=64 * 1024)[0]
   and not S.fits({"gpu_mem": 40}, gpu_mem=32)[0] and S.fits({"cpus": 8, "mem": "32G", "gpu_mem": 20}, 16, 64 * 1024, 80)[0]
   and S.mem_mb("2T") == 2 * 1024 ** 2)
ok("任务单：res 写错字段、不是数、mem 格式不对 → 报错", raises(lambda: S.resolve("command", dict(G, res={"gpu": 1})))
   and raises(lambda: S.resolve("command", dict(G, res={"cpus": "many"})))
   and raises(lambda: S.resolve("command", dict(G, res={"mem": "lots"}))))
w = S.resolve("command", dict(G, res={"wait_gpu": "120"}))
ok("任务单：res.wait_gpu 转成分钟数，shell 里给 TASK_WAIT_GPU；不写时为空、负数报错",
   w["res"] == {"wait_gpu": 120} and "TASK_WAIT_GPU=120" in S.shell_lines(w, "/d").splitlines()
   and "TASK_WAIT_GPU=" in S.shell_lines(S.resolve("command", G), "/d").splitlines()
   and raises(lambda: S.resolve("command", dict(G, res={"wait_gpu": -1}))))
ok("任务单：inputs 里有 .. → 报错", raises(lambda: S.resolve("command", dict(G, inputs=["../etc"]))))
ok("参数叫 data、repo（运行时展开的名字）→ 报错", raises(lambda: S.resolve("command", dict(G, data=["x"])))
   and raises(lambda: S.resolve("command", dict(G, repo="r"))))
ok("任务单：不认识的 progress → 报错", raises(lambda: S.resolve("command", dict(G, progress="tqdm"))))
ok("toy：也有 resume、一张卡、输入拷到本地，进度读插件的日志读法", s["resume"]
   and s["res"] == {"gpus": 1, "local_inputs": 1} and s["progress"] == "toy_log" and s["relay"] == "toy")
sl = S.shell_lines(s, "/d").splitlines()
ok("shell：TASK_INPUTS 是输入的绝对路径（数组），TASK_LOCAL 跟 res.local_inputs", f"TASK_INPUTS=(/d/prepared/{s['params']['ds'] if 'params' in s else s['inputs'][0].split('/')[-1]})" in sl
   and "TASK_LOCAL=1" in sl and "TASK_LOCAL=0" in S.shell_lines(S.resolve("command", G), "/d").splitlines())
raw = dict(S.resolve("command", G), params=dict(G, note="{model}_frag{frag}_"))
ok("shell：params 里原样留着的 {名字} 不展开、不报错", "TASK_LOCAL=0" in S.shell_lines(raw, "/d").splitlines())
gr = fixture.tmpdir()
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
# 存储挂住：一个任务的文件读不动 → 这个任务记进 stuck、跳过，别的照常读；一共等的时间有上限
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
# 读的时候出错：记进 errors（带原因），不算卡住；不带 wait 时也不让整个 collect 失败
S.INFO["boom"] = lambda fd: 1 / 0
bs = dict(gs, key="guest/boom", out="guest/x", info="boom")
for w in (None, 5):
    rb = S.collect([bs, gs], gr, wait=w)
    ok(f"collect（wait={w}）：一个任务读出错 → errors 里带原因，不进 stuck，别的照常",
       "ZeroDivisionError" in rb.get("errors", {}).get("guest/boom", "") and "stuck" not in rb and "guest/x" in rb["tasks"]
       and "guest/boom" not in rb["tasks"])
del S.INFO["boom"]
# spec.py 命令行的错误：说明有问题 → 一句话、退出码 2；done 读不了数据目录 → 退出码 5
SP = [sys.executable, os.path.join(HERE, "spec.py")]
bad = dict(S.sendable(S.resolve("command", G)), cmd="run {nope}")
r = subprocess.run(SP + ["shell", "--spec", __import__("json").dumps(bad)], capture_output=True, text=True)
ok("spec.py shell：说明里有不认识的 {名字} → 一句话、退出码 2", r.returncode == 2 and "nope" in r.stderr and "Traceback" not in r.stderr)
r = subprocess.run(SP + ["shell"], capture_output=True, text=True)
ok("spec.py shell 不带参数 → 退出码 2，不是回溯", r.returncode == 2 and "Traceback" not in r.stderr)
gone = os.path.join(gr, "not_mounted")
r = subprocess.run(SP + ["done", "--spec", __import__("json").dumps(S.sendable(S.resolve("command", G)))], capture_output=True, text=True,
                   env=dict(os.environ, TASKQ_DATA=gone))
ok("spec.py done：数据目录读不了 → 一句话、退出码 5（稍后再查）", r.returncode == 5 and "Traceback" not in r.stderr and r.stderr.strip())
import refill as RF  # noqa: E402
ok("refill.site_progress：只有存档（没有轮数）也算有进度", RF.site_progress(dict(final=False, runs=[dict(epochs_done=0)], resumable=True))
   and not RF.site_progress(dict(final=False, runs=[dict(epochs_done=0)], resumable=False)))
src = {"src": {"pueue-host": {"tasks": {"guest/x": dict(final=False, runs=[dict(epochs_done=0, end=None)], resumable=True)}},
               "slurm-login": {"tasks": {}}}}
ok("refill.progress：只有存档也算有进度，按机器分", RF.progress(src, "guest/x", "pueue-host") and not RF.progress(src, "guest/x", "slurm-login"))

# ---------------- 训完的判断 ----------------
root = fixture.tmpdir()
out = os.path.join(root, s["out"])
os.makedirs(os.path.join(out, "check"))
ok("什么都没有：没训完", not S.is_done(s, root))
open(os.path.join(out, "check", "summary.json"), "w").write("{}")
ok("只有 summary.json：没训完", not S.is_done(s, root))
open(os.path.join(out, "final.ckpt"), "w").close()
ok("summary.json + final.ckpt：训完", S.is_done(s, root))
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

# ---------------- 输出（outputs，只在这一处解释 out、done、fetch、archive） ----------------
o = S.outputs(dict(key="a/b/0", out="/res/a//b/", done="x|y", fetch=dict(also=["pred/a/"], exclude=["big.pth"], extra=["*.json"]),
                   archive=dict(drop=["*"])))
ok("outputs：out 和 also 规整成相对路径、字符串 done 变成一项、| 分开成备选、archive 是开关、分段日志前缀",
   o == dict(outs=["res/a/b", "pred/a"], done=[["x", "y"]], exclude=["big.pth"], extra=["*.json"], archive=True,
             logs="logs/tasks/a__b__0__"))
o2 = S.outputs(dict(out="r", done=[], archive=None))
ok("outputs：没有 fetch、done 空、不归档、没有 key", o2 == dict(outs=["r"], done=[], exclude=[], extra=[], archive=False, logs=None))
o3 = S.outputs(dict(out="r", done=["a", "b|c"]))
ok("done_with：每一项都要有、一项里有一个备选就行", S.done_with(o3, {"a", "c"}.__contains__) and not S.done_with(o3, {"b", "c"}.__contains__))
ok("done_missing：列出缺的项（原来的写法）", S.done_missing(o3, {"a"}.__contains__) == ["b|c"])
ok("done_with：done 是空的 → 按调用方给的 empty", S.done_with(o2, lambda a: False) and not S.done_with(o2, lambda a: False, empty=False))
_os = dict(S.sendable(S.resolve("command", dict(G, archive=True, fetch=dict(extra=["*.json"])))))
_r = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "outputs", "--spec", json.dumps(_os), "--sh"], capture_output=True, text=True)
_sh = _r.stdout.splitlines()
ok("spec.py outputs --spec <JSON> --sh：给 archive_task.sh 的 KEY、OUTS、EXTRA、TEST、ARCHIVE",
   _r.returncode == 0 and "KEY=guest/x" in _sh and "OUTS=(guest/x)" in _sh and "EXTRA=('*.json')" in _sh
   and "ARCHIVE=1" in _sh and any(x.startswith("TEST=") and "DONE" in x for x in _sh))
_r = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "outputs", "--spec", json.dumps(_os)], capture_output=True, text=True)
ok("spec.py outputs --spec <JSON>：JSON 和 outputs() 一样", _r.returncode == 0 and json.loads(_r.stdout) == S.outputs(_os))


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
_g = E.group_names(_ex)
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


# ---------------- 实验的声明：别的脚本按模型文件夹查，不按名字猜 ----------------
M = "/x/{}/V_save__PlanM__full"
ok("names_of：t24cov 跳过 G 的四个外部集、用 t24_splits.json",
   E.names_of(M.format("D155_gamma_ecov"), "skip_ext", "splits") == ["G-A G-B G-C G-D", "t24_splits.json"])
ok("names_of：t22a 只测留出的 G-A", E.names_of(M.format("D151_gamma_loA"), "only_ext") == ["G-A"])
ok("names_of：t0 的模型什么都不跳过、没有划分", E.names_of(M.format("D101_alpha"), "skip_ext", "only_ext", "splits") == ["", "", ""])
ok("names_of：表里没有的模型报错（不猜）", raises(lambda: E.names_of("/x/D999_x/foo", "skip_ext")))
ok("每个实验都有档位和看板名称", all(isinstance(e.get("nice"), int) and set(e.get("board") or {}) == {"group", "en", "zh"}
                                for e in exps if e["rq"] == "demo"))
ok("有 oof 步骤的都给了划分文件", all(e["names"]["splits"] for e in exps if e["rq"] == "demo"
                                     and any(s["name"] == "oof" for s in e["group_after"])))
import importlib.util  # noqa: E402
spec_ = importlib.util.spec_from_file_location("demo_exps", os.path.join(fixture.EXPS, "demo", "experiments.py"))
R = importlib.util.module_from_spec(spec_)
spec_.loader.exec_module(R)
x = R._x("toy", "T99", "玩具", "base", "D999_x", ("G", "en", "zh"), 0, steps=R.OOF5 + ["ens"] + R.PROBE, splits="s.json", probe="p")


# 实验表的字段核对：不认识的字段报错，说清哪个实验、哪个字段、允许哪些
class _Mod:
    def __init__(self, exps, final=()):
        self.EXPERIMENTS, self.FINAL = exps, list(final)


def _field_err(mod):
    try:
        E.check_fields("x/experiments.py", mod)
    except S.SpecError as err:
        return str(err)
    return None


_e = R._x("toy", "T99", "玩具", "base", "D999_x", ("G", "en", "zh"), 0)
ok("字段核对：demo 表的实验都认识", _field_err(_Mod(R.EXPERIMENTS, R.FINAL)) is None and _field_err(_Mod([_e])) is None)
_bad = dict(_e, nicee=1)
_m = _field_err(_Mod([_bad]))
ok("字段核对：实验里拼错的字段 → 报错，写明文件、实验、字段、允许的", bool(_m) and "x/experiments.py" in _m and "toy" in _m
   and "nicee" in _m and "group_after" in _m)
_m = _field_err(_Mod([dict(_e, after=[dict(R.INFER, out_bg=75)])]))
ok("字段核对：步骤里拼错的 out_bg → 报错，写明步骤、允许的（out_gb）",
   bool(_m) and "out_bg" in _m and "infer" in _m and "out_gb" in _m)
_m = _field_err(_Mod([dict(_e, group_after=[[R.OOF, R.PAIRED]])]))
ok("字段核对：group_after 里分串（列表套列表）→ 报错（先后按输入推）", bool(_m) and "group_after" in _m)
_m = _field_err(_Mod([dict(_e, group_after=[dict(R.OOF, only=dict(frag=["0"]))])]))
ok("字段核对：实验一级的步骤写 only → 报错", bool(_m) and "only" in _m)
_m = _field_err(_Mod([dict(_e, after=[dict(R.INFER, only=dict(seed=["0"]))])]))
ok("字段核对：only 里的参数不在 each 里 → 报错", bool(_m) and "only" in _m and "frag" in _m)
_m = _field_err(_Mod([dict(_e, after=[dict(R.EVAL, device="tpu")])]))
ok("字段核对：device 不是 gpu / cpu → 报错", bool(_m) and "device" in _m)
_m = _field_err(_Mod([dict(_e, after=[{k: v for k, v in R.INFER.items() if k != "done"}])]))
ok("字段核对：步骤缺必写的 done → 报错", bool(_m) and "done" in _m)
_m = _field_err(_Mod([_e], [dict(R.FINAL[0], rerun_if_newr=["x"])]))
ok("字段核对：FINAL 步骤里拼错的字段 → 报错", bool(_m) and "rerun_if_newr" in _m)
ok("字段核对：步骤里直接写 res、device、relay、out_gb、temp、rerun",
   _field_err(_Mod([dict(_e, after=[dict(R.INFER, res={}, device="gpu", relay="path", out_gb=1, temp=["x"], rerun=True)])])) is None)

ok("_x：steps 展开成每份的步骤和实验级步骤（一层列表，先后按输入推）",
   [s["name"] for s in x["after"]] == ["infer", "eval"]
   and [s["name"] for s in x["group_after"]] == ["oof", "paired", "ens", "p1", "p2", "p2cmp"])


def bad(**kw):
    try:
        R._x("toy", "T99", "玩具", "base", "D999_x", ("G", "en", "zh"), 0, **kw)
    except ValueError:
        return True
    return False


ok("_x：写错步骤名、oof 没给 splits、P1 没给 probe 都报错",
   bad(steps=["infer", "evl"]) and bad(steps=["oof"]) and bad(steps=["p1"]))

# 模型专用的读法放在插件的 readers/：加载后表里有
ok("readers：toy_log、toy_debug 加载进 PROGRESS、INFO，有训完一轮的字样",
   "toy_log" in S.PROGRESS and "toy_debug" in S.INFO and S.TRAINED)
ok("readers：重名的报错", raises(lambda: S.load_readers({"a.py": "PROGRESS={'toy_log': None}"})))
_dup = fixture.tmpdir()
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
rr = fixture.tmpdir()
ns = S.resolve("toy", {"ds": "D1_x", "variant": "tr", "frag": "0"})
od = os.path.join(rr, ns["out"])
os.makedirs(od)
open(os.path.join(od, "train_log_1.txt"), "w").write(
    "2026-10-01 10:00:00.1: Epoch 0\n2026-10-01 10:05:00.1: Epoch time: 300 s\n2026-10-01 10:05:01.1: Epoch 1\n")
_lg = os.path.join(rr, "logs", "tasks", S.safe(ns["key"]) + proto.SAFE + "123_" + proto.LOG_PART + "1.log")
os.makedirs(os.path.dirname(_lg))
open(_lg, "w").write("作业 123_1\nx: Epoch time: 300 s\n")
rc = S.collect([{k: ns.get(k) for k in S.COLLECT_FIELDS}], rr)
ok("collect：toy_log 读出训完 1 轮，logs/tasks 里的分段日志算训完一轮",
   rc and rc["tasks"][ns["key"]]["runs"][0]["epochs_done"] == 1
   and [x["trained"] for x in rc["seglogs"] if x["name"] == os.path.basename(_lg)] == [True])

# spec.py collect --json：store 和执行机上的 site_ops 经它读进度
_r = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "collect", "--json"], capture_output=True, text=True,
                    input=json.dumps({"specs": [{k: ns.get(k) for k in S.COLLECT_FIELDS}], "root": rr, "wait": 30}))
_cj = S.json_in(_r.stdout)
ok("spec.py collect --json：标准输入给说明和数据目录 → @@JSON 一行，带 version，其余和 collect() 一样",
   _r.returncode == 0 and len(_r.stdout.strip().splitlines()) == 1 and _cj["version"] == S.P.VERSION
   and {k: v for k, v in _cj.items() if k != "version"} == rc)
_r = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "collect"], capture_output=True, text=True, input="{}")
_r2 = subprocess.run([sys.executable, os.path.join(HERE, "spec.py"), "collect", "--json"], capture_output=True, text=True, input="[1]")
ok("spec.py collect：不带 --json、输入不对 → 退出码 2", _r.returncode == 2 and _r2.returncode == 2)

# inputs_cmd 和任务的命令一样能用核心的命令（bin/taskq）和库：cron 的 PATH 里没有 taskq 时也行
_path = os.environ["PATH"]
os.environ["PATH"] = "/usr/bin:/bin"
try:
    _li = S.listed_inputs("command -v taskq; echo $TASKQ_LIB", "/nonexistent")
finally:
    os.environ["PATH"] = _path
ok("inputs_cmd：核心的 bin/ 在 PATH 最前面、TASKQ_LIB 是核心的 queue/（和 run_task.sh 给任务的命令一样）",
   _li == [os.path.join(os.path.dirname(HERE), "bin", "taskq"), HERE])

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
