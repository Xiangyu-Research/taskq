#!/usr/bin/env python3
"""测 cli_json.py 的只读 JSON 命令：taskq machines show、taskq exp steps、taskq spec export / import，经 bin/taskq
跑，固定配置（testdata/）。字段 = proto 的 MSHOW_*、EXP_STEP*、RELAY_*，值和进程里直接算的一样。

    python3 queue/test_cli_json.py
"""
import json
import os
import shlex
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
BIN = os.path.join(CORE, "bin", "taskq")
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

T = fixture.tmpdir(prefix="test_cli_json_")
HOME, PROJ, DATA = (os.path.join(T, x) for x in ("home", "proj", "data"))
for d in (HOME, PROJ, DATA):
    os.makedirs(d)
with open(os.path.join(PROJ, ".taskq.toml"), "w") as f:
    f.write(f'config = "{fixture.CONFIG}"\nplugins = ["{fixture.PLUGINS}"]\n')
os.environ.update(HOME=HOME, TASKQ_DATA=DATA, TASKQ_REPO=PROJ)  # 命令里的路径相对项目，和 bin/taskq 找到的一样
fixture.use()
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(CORE, "sites"))
import cfg  # noqa: E402
import experiments as E  # noqa: E402
import names as N  # noqa: E402
import proto  # noqa: E402
import pueue as PQ  # noqa: E402
import remote as R  # noqa: E402
import spec as S  # noqa: E402
from flow import compile as FC  # noqa: E402
from flow.model import EACH, GATHER, GLOBAL  # noqa: E402

M = cfg.mtab()
ENV = dict({k: v for k, v in os.environ.items() if not k.startswith("TASKQ_")}, HOME=HOME, TASKQ_DATA=DATA,
           PYTHONDONTWRITEBYTECODE="1")
fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def taskq(*args, stdin=None, env=None):
    return subprocess.run([sys.executable, BIN, *args], cwd=PROJ, env=env or ENV, input=stdin, capture_output=True, text=True,
                          timeout=300)


def js(*args, **kw):
    r = taskq(*args, **kw)
    assert r.returncode == 0, (args, r.returncode, r.stderr[-500:])
    assert r.stdout.count("\n") == 1, r.stdout[:300]
    return json.loads(r.stdout)


try:
    # ---------------- taskq machines show ----------------
    d = js("machines", "show", "--json")
    every = set(M.MACHINES)
    ok("machines show：一行、顶层字段 = proto.MSHOW_TOP、每台 = MSHOW_MACHINE、每个描述 = MSHOW_SPEC",
       tuple(d) == proto.MSHOW_TOP and d["version"] == proto.VERSION and set(d["machines"]) == every
       and all(tuple(m) == proto.MSHOW_MACHINE for m in d["machines"].values())
       and all(tuple(s) == proto.MSHOW_SPEC for s in d["specs"].values()))
    ok("machines show：hub、角色、执行机、暂停、种类、worker、组、能用的环境、激活语句和机器表算的一样",
       d["hub"] == M.hub() and all(
           m["roles"] == list(M.roles(n)) and m["executor"] == M.is_exec(n) and m["paused"] == M.paused(n)
           and m["kinds"] == list(M.kinds(n)) and m["workers"] == json.loads(json.dumps(M.workers(n)))
           and m["groups"] == json.loads(json.dumps(M.groups(n))) and m["ready_envs"] == sorted(M.ready_envs(n))
           and m["activate"] == {e: M.activate(n, e) for e in M.machine(n)["envs"] or {}} for n, m in d["machines"].items()))
    ms = d["machines"]
    ok("machines show：执行机是 site-pueue、site-slurm、site-slurm2，hub 不是",
       [n for n, m in ms.items() if m["executor"]] == ["site-pueue", "site-slurm", "site-slurm2"] and ms["hub"]["roles"] == ["hub", "store"])
    ok("machines show：pueue 执行机每个 worker 一条开 worker 的命令（按 worker id），Slurm 的、hub 的没有",
       ms["site-pueue"]["worker_cmds"] == {w["id"]: PQ.worker_cmd("site-pueue", w) for w in M.workers("site-pueue")}
       and list(ms["site-pueue"]["worker_cmds"]) == ["site-pueue-gpu1", "site-pueue-gpu0", "site-pueue-cpu"]
       and ms["site-slurm"]["worker_cmds"] == {} and ms["hub"]["worker_cmds"] == {})
    ok("machines show：根目录的 shell 写法（~ 留给那边展开）", ms["site-slurm"]["root_q"] == R.q("~/q") and ms["hub"]["root_q"] == R.q("~/demo_root"))
    ok("machines show：给任务加的语句（task_vars）", ms["site-pueue"]["task_sh"] == S.machine_vars(M.machine("site-pueue"))
       and any("DEMO_FAST=1" in x for x in ms["site-pueue"]["task_sh"]) and ms["hub"]["task_sh"] == [])
    specs = sorted(f[:-4] for f in os.listdir(os.path.join(fixture.CONFIG, "specs")) if f.endswith(".yml"))
    ok("machines show：环境描述的哈希、base", sorted(d["specs"]) == specs
       and all(s["hash"] == M.spec_hash(n) for n, s in d["specs"].items()) and d["specs"]["alt"]["base"] == "base")
    d2 = js("machines", "show", "site-pueue", "hub")
    ok("machines show 名…：只有这几台", set(d2["machines"]) == {"site-pueue", "hub"} and d2["machines"]["site-pueue"] == ms["site-pueue"])
    r = taskq("machines", "show", "nope")
    ok("machines show 不认识的机器 → 4（proto.EXIT gone），原因在 stderr", r.returncode == proto.EXIT["gone"] and "nope" in r.stderr and not r.stdout)
    r = taskq("machines", "get", "site-slurm", "root")
    ok("machines 的别的命令照旧", r.returncode == 0 and r.stdout.strip() == "~/q")

    # ---------------- taskq exp steps ----------------
    exps, final = E.load()
    tasks = E.tasks()
    flow = FC.build()
    inst = [i for i in flow.instances.values() if i.step]
    LV = {EACH: "task", GATHER: "group", GLOBAL: "final"}

    def filled(text, names):
        return S.fill(S.fill(text, names), dict(data=DATA, repo=cfg.REPO), keep=())

    d = js("exp", "steps", "--data", DATA)
    st = d["steps"]
    ok("exp steps：一行、顶层字段 = proto.EXP_STEPS_TOP、每步 = EXP_STEP_FIELDS、测试的实验表推得出实例",
       tuple(d) == proto.EXP_STEPS_TOP and d["version"] == proto.VERSION and d["data"] == DATA and d["errors"] == flow.errors == []
       and all(tuple(s) == proto.EXP_STEP_FIELDS for s in st))
    n_task = sum(len(E.steps_of(t)) for t in tasks.values())
    n_group = sum(len(e["group_after"]) for e in exps)
    lvs = [s["level"] for s in st]
    ok(f"exp steps：flow 的每个步骤实例都列了、按实验表的顺序（每个任务 {n_task}、实验一级 {n_group}、FINAL {len(final)}）",
       [(s["id"], s["level"]) for s in st] == [(i.id, LV[i.scope]) for i in inst]
       and (lvs.count("task"), lvs.count("group"), lvs.count("final")) == (n_task, n_group, len(final)))
    ok("exp steps：每步的实验、任务、步骤名、names、产出、声明的输入和 flow 的实例一样",
       all((s["exp"], s["key"], s["name"], s["names"], s["out"], s["inputs"])
           == (i.exp or None, i.task or None, i.rule, E.jsonable(i.names), i.outputs, i.inputs) for s, i in zip(st, inst)))
    k0, t0 = next((k, t) for k, t in tasks.items() if E.steps_of(t))
    names0 = E.task_names(t0)
    ok("exp steps：命令按 names 填好、{data} 换成 --data，没有留下 {…}；done、needs 没要时是 null",
       [s["cmd"] for s in st if s["key"] == k0] == [filled(x["cmd"], names0) for x in E.steps_of(t0)]
       and all(s["error"] is None and "{" not in s["cmd"] and s["done"] is None and s["needs"] is None for s in st))
    e0 = next(e for e in exps if any(x["name"] == "ens" for x in e["group_after"]))
    ens = next(s for s in st if s["exp"] == e0["id"] and s["name"] == "ens")
    oof_e = next(e for e in exps if any(x["name"] == "oof" for x in e["group_after"]))
    oof = next(s for s in st if s["exp"] == oof_e["id"] and s["name"] == "oof")
    ok("exp steps：实验一级的命令用 <参数>_list_q，输入里的 {frag} 按每个取值展开",
       shlex.quote(" ".join(e0["each"]["frag"])) in ens["cmd"] and ens["key"] is None
       and all(any(x.endswith(f"/frag_{f}/check") for x in oof["inputs"]) for f in oof_e["each"]["frag"]))
    fin = [s for s in st if s["level"] == "final"]
    ok("exp steps：FINAL 的不属于实验、只有 data", [s["name"] for s in fin] == [f["name"] for f in final]
       and all(s["exp"] is None and s["key"] is None and s["names"] == {} for s in fin))
    dk = js("exp", "steps", "--key", k0, "--data", DATA)["steps"]
    ok("exp steps --key：只有这个任务的步骤", [s["id"] for s in dk] == [s["id"] for s in st if s["key"] == k0])
    de = js("exp", "steps", "--exp", e0["id"], "--level", "group", "--data", DATA)["steps"]
    ok("exp steps --exp --level group：只有这个实验一级的", [s["name"] for s in de] == [x["name"] for x in e0["group_after"]]
       and all(s["exp"] == e0["id"] and s["level"] == "group" for s in de))
    ok("exp steps --exp：FINAL 的不列", not any(s["level"] == "final" for s in js("exp", "steps", "--exp", e0["id"])["steps"]))
    ok("exp steps 不给 --data：用本机的数据目录（TASKQ_DATA）", js("exp", "steps", "--key", k0)["data"] == DATA)

    # --done：数盘上第一个产出下 done 的文件，每一项都要有（只看实际的文件）
    dd = js("exp", "steps", "--key", k0, "--data", DATA, "--done")["steps"]
    ok("exp steps --done：空的数据目录都没做完", dd and all(s["done"] is False for s in dd))
    s0 = dd[0]
    mark = os.path.join(DATA, s0["out"][0], flow.instances[s0["id"]].step["done"][0].split("|")[0])
    os.makedirs(os.path.dirname(mark), exist_ok=True)
    open(mark, "w").close()
    dd = js("exp", "steps", "--key", k0, "--data", DATA, "--done")["steps"]
    ok(f"exp steps --done：{s0['name']} 的标志在了 → 做完，别的没做完", [s["done"] for s in dd] == [True] + [False] * (len(dd) - 1))
    oof_i = flow.instances[oof["id"]]
    lv = ["--exp", oof_e["id"], "--level", "group", "--data", DATA, "--done"]
    first = os.path.join(DATA, oof_i.outputs[0], oof_i.step["done"][0])
    os.makedirs(os.path.dirname(first), exist_ok=True)
    open(first, "w").close()
    half = next(s["done"] for s in js("exp", "steps", *lv)["steps"] if s["name"] == "oof")
    open(os.path.join(DATA, oof_i.outputs[0], oof_i.step["done"][1]), "w").close()
    full = next(s["done"] for s in js("exp", "steps", *lv)["steps"] if s["name"] == "oof")
    ok("exp steps --done：done 有两项时只有一项在 → 没做完，两项都在 → 做完", len(oof_i.step["done"]) == 2 and half is False and full is True)
    shutil.rmtree(DATA)
    os.makedirs(DATA)

    # --needs：写了 inputs_cmd 的步骤真跑（测试实验表的每个实验；刚训完、模型还在别的机器上：数据目录是空的）
    dn = js("exp", "steps", "--data", DATA, "--needs")["steps"]
    has = [s for s in dn if flow.instances[s["id"]].step.get("inputs_cmd")]
    ok(f"exp steps --needs：每个实验写了 inputs_cmd 的步骤（{len(has)} 个）都列出了输入，别的 needs 是 null",
       {s["exp"] for s in has} == {e["id"] for e in exps if any(x.get("inputs_cmd") for x in e["after"] + e["group_after"])}
       and all(s["needs"] and s["error"] is None for s in has) and all(s["needs"] is None for s in dn if s not in has))
    ok("exp steps --needs：列的是相对数据目录的路径，有这一份的 final.ckpt",
       all(not x.startswith("/") for s in has for x in s["needs"])
       and all(f"{s['out'][0].replace('preds/', 'results/', 1)}/final.ckpt" in s["needs"] for s in has if s["name"] == "infer"))
    ok("exp steps --needs：没碰数据目录（只建了空的数据目录本身）", os.listdir(DATA) == [])
    r = taskq("exp", "steps", "--exp", "nope")
    ok("exp steps 不认识的实验 → 4", r.returncode == proto.EXIT["gone"] and "nope" in r.stderr and not r.stdout)
    r = taskq("exp", "steps", "--key", "nope/x/0")
    ok("exp steps 不认识的 key → 4", r.returncode == proto.EXIT["gone"] and "nope/x/0" in r.stderr and not r.stdout)
    r = taskq("exp", "steps", "--level", "nope")
    ok("exp steps 参数不对 → 2", r.returncode == 2)

    # ---------------- taskq spec export / import ----------------
    DS, DI = "D777_fake", "PlanM_full"
    CASES = ["s101", "s101_b", "s102", "x-00582-000-syn"]
    ROOT = os.path.join(T, "root")
    PRE = os.path.join(ROOT, "prepared", DS)

    def wj(p, obj):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            json.dump(obj, f)

    def wb(p, data):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as f:
            f.write(data)

    wj(os.path.join(PRE, "dataset.json"), dict(file_ending=".dat", name=DS))
    wj(os.path.join(PRE, "PlanM.json"), dict(configurations={"full": dict(data_identifier=DI)}))
    wj(os.path.join(PRE, "splits_final.json"), [dict(train=CASES[2:], val=CASES[:2]), dict(train=CASES[:2], val=CASES[2:])])
    wj(os.path.join(PRE, "fingerprint.json"), {})
    for c in CASES:
        wb(os.path.join(PRE, "gt", c + ".dat"), b"gt")
        for suf in (".bin", "_lab.bin", ".meta"):
            wb(os.path.join(PRE, DI, c + suf), b"x")
    sp = S.resolve("toy", dict(ds=DS, frag="0"))
    spj = json.dumps(sp)
    ex = js("spec", "export", "--spec", "-", "--root", ROOT, "--json", stdin=spj)
    want = N.export(sp, ROOT)
    ok("spec export：一行、顶层字段 = proto.RELAY_EXPORT_TOP，和插件送出的一样，logs = 分段日志的前缀",
       tuple(ex) == proto.RELAY_EXPORT_TOP and ex["version"] == proto.VERSION and ex["plugin"] == "toy"
       and ex["files"] == [list(f) for f in want["files"]] and ex["views"] == want["views"] and ex["outs"] == want["outs"]
       and ex["cleanup"] == want["cleanup"] and ex["params"] == want["params"] and ex["logs"] == S.outputs(sp)["logs"])
    ok("spec export --spec 直接给 JSON", js("spec", "export", "--spec", spj, "--root", ROOT) == ex)

    src = os.path.join(T, "src")
    out = os.path.join(src, sp["out"])
    for c in CASES[:2]:
        wb(os.path.join(out, "check", c + ".dat"), b"p")
    wj(os.path.join(out, "check", "summary.json"), dict(metric_per_case=[{}, {}]))
    wb(os.path.join(out, "final.ckpt"), b"ck")
    wb(os.path.join(src, ex["logs"] + "1_part1.log"), b"log")
    im = js("spec", "import", "--spec", "-", "--src", src, "--dst", os.path.join(T, "dst"), "--root", ROOT, stdin=spj)
    w2 = N.import_(sp, src, os.path.join(T, "dst2"), ROOT)
    ok("spec import：顶层字段 = proto.RELAY_IMPORT_TOP，写的和插件拉回的一样、都在 --dst 里",
       tuple(im) == proto.RELAY_IMPORT_TOP and im["written"] == w2 and ex["logs"] + "1_part1.log" in im["written"]
       and all(os.path.isfile(os.path.join(T, "dst", w)) for w in im["written"]))
    os.remove(os.path.join(out, "check", CASES[1] + ".dat"))
    r = taskq("spec", "import", "--spec", "-", "--src", src, "--dst", os.path.join(T, "dst3"), "--root", ROOT, stdin=spj)
    ok("spec import 核对不过 → 1，原因在 stderr，--dst 不动", r.returncode == 1 and "RelayError" in r.stderr and not r.stdout
       and not os.path.exists(os.path.join(T, "dst3")))
    wj(os.path.join(PRE, "splits_final.json"), [dict(train=CASES[2:], val=CASES[:2] + ["nobody"])])
    r = taskq("spec", "export", "--spec", "-", "--root", ROOT, stdin=spj)
    ok("spec export 插件报错 → 1", r.returncode == 1 and "不一致" in r.stderr and not r.stdout)
    r = taskq("spec", "export", "--spec", '{"x": 1}', "--root", ROOT)
    ok("spec export 不是解析好的说明 → 2", r.returncode == 2 and "resolve" in r.stderr)
    r = taskq("spec", "export", "--spec", "/no/such.json", "--root", ROOT)
    ok("spec export --spec 不是 JSON → 2，一行原因、没有出错堆栈", r.returncode == 2 and "--spec" in r.stderr and "Traceback" not in r.stderr)
    r = taskq("spec", "import", "--spec", "-", "--root", ROOT, stdin=spj)
    ok("spec import 缺 --src、--dst → 2", r.returncode == 2)
    ok("--help 退出码 0", all(taskq(*a, "--help").returncode == 0
                              for a in (("machines", "show"), ("exp", "steps"), ("spec", "export"), ("spec", "import"))))
finally:
    shutil.rmtree(T, ignore_errors=True)

print("全部通过" if not fails else f"{fails} 项失败")
sys.exit(1 if fails else 0)
