#!/usr/bin/env python3
"""测 cli_json.py 的只读 JSON 命令（2026-11-04）：taskq machines show、taskq exp steps、taskq spec export / import，经 bin/taskq
跑，固定配置（testdata/）。字段 = proto 的 MSHOW_*、EXP_STEP*、RELAY_*，值和进程里直接算的一样。

    python3 queue/test_cli_json.py
"""
import json
import os
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
    every = set(M.MACHINES) | set(M.SITES)
    ok("machines show：一行、顶层字段 = proto.MSHOW_TOP、每台 = MSHOW_MACHINE、每个描述 = MSHOW_SPEC",
       tuple(d) == proto.MSHOW_TOP and d["version"] == proto.VERSION and set(d["machines"]) == every
       and all(tuple(m) == proto.MSHOW_MACHINE for m in d["machines"].values())
       and all(tuple(s) == proto.MSHOW_SPEC for s in d["specs"].values()))
    ok("machines show：hub、种类、worker、能用的环境、激活语句、收编来源和机器表算的一样",
       d["hub"] == M.hub() and all(
           m["kinds"] == list(M.kinds(n)) and m["workers"] == {k: M.worker_of(n, k) for k in M.KINDS}
           and m["ready_envs"] == sorted(M.ready_envs(n)) and m["adopt"] == M.adopt_sources(n)
           and m["activate"] == {e: M.activate(n, e) for e in M.machine(n).get("envs") or {}} for n, m in d["machines"].items()))
    ms = d["machines"]
    ok("machines show：pueue 执行机有开 worker 的命令，Slurm 的、MACHINES 的没有",
       ms["lab0"]["worker_cmds"] == {k: PQ.worker_cmd("lab0", k) for k in M.kinds("lab0")} and ms["dicc1"]["worker_cmds"] == {}
       and ms["box"]["worker_cmds"] == {})
    ok("machines show：根目录的 shell 写法（~ 留给那边展开）", ms["dicc1"]["root_q"] == R.q("~/q") and ms["box"]["root_q"] is None)
    ok("machines show：给任务加的语句（task_vars）", ms["lab1"]["task_sh"] == S.machine_vars(M.machine("lab1"))
       and any("DEMO_FAST=1" in x for x in ms["lab1"]["task_sh"]) and ms["box"]["task_sh"] == [])
    specs = sorted(f[:-4] for f in os.listdir(os.path.join(fixture.CONFIG, "specs")) if f.endswith(".yml"))
    ok("machines show：环境描述的哈希、base", sorted(d["specs"]) == specs
       and all(s["hash"] == M.spec_hash(n) for n, s in d["specs"].items()) and d["specs"]["alt"]["base"] == "base")
    d2 = js("machines", "show", "lab1", "box")
    ok("machines show 名…：只有这几台", set(d2["machines"]) == {"lab1", "box"} and d2["machines"]["lab1"] == ms["lab1"])
    r = taskq("machines", "show", "nope")
    ok("machines show 不认识的机器 → 4（proto.EXIT gone），原因在 stderr", r.returncode == proto.EXIT["gone"] and "nope" in r.stderr and not r.stdout)
    r = taskq("machines", "get", "dicc1", "root")
    ok("machines 的别的命令照旧", r.returncode == 0 and r.stdout.strip() == "~/q")

    # ---------------- taskq exp steps ----------------
    exps, final = E.load()
    tasks = E.tasks()
    py = sys.executable
    d = js("exp", "steps", "--data", DATA, "--python", py)
    st = d["steps"]
    ok("exp steps：一行、顶层字段 = proto.EXP_STEPS_TOP、每步 = EXP_STEP_FIELDS",
       tuple(d) == proto.EXP_STEPS_TOP and d["version"] == proto.VERSION and d["data"] == DATA and d["python"] == py
       and all(tuple(s) == proto.EXP_STEP_FIELDS for s in st))
    n_task = sum(len(E.steps_of(t)) for t in tasks.values())
    n_group = sum(len(c) for e in exps for c in e.get("group_after") or [])
    ok(f"exp steps：每个任务、实验一级、FINAL 的步骤都列了（{n_task}、{n_group}、{len(final)}）",
       [s["level"] for s in st].count("task") == n_task and [s["level"] for s in st].count("group") == n_group
       and [s["level"] for s in st].count("final") == len(final))
    k0, t0 = next((k, t) for k, t in tasks.items() if (E.experiment(t["exp"]) or {}).get("after"))
    names0 = E.task_names(t0, DATA, py)
    want = [S.fill(x["cmd"], names0, keep=()) for x in E.steps_of(t0)]
    ok("exp steps：命令按 names 填好，没有留下 {data}、{python}",
       [s["cmd"] for s in st if s["key"] == k0] == want and all("{data}" not in (s["cmd"] or "") for s in st)
       and all(s["error"] is None for s in st))
    dk = js("exp", "steps", "--key", k0, "--data", DATA, "--python", py)["steps"]
    ok("exp steps --key：只有这个任务的步骤", dk and all(s["level"] == "task" and s["key"] == k0 for s in dk))
    e0 = next(e for e in exps if e.get("group_after"))
    de = js("exp", "steps", "--exp", e0["id"], "--level", "group", "--data", DATA, "--python", py)["steps"]
    ok("exp steps --exp --level group：只有这个实验一级的", len(de) == sum(map(len, e0["group_after"]))
       and all(s["exp"] == e0["id"] and s["level"] == "group" for s in de))
    ok("exp steps 不给 --data：用本机的数据目录（TASKQ_DATA）", js("exp", "steps", "--key", k0, "--python", py)["data"] == DATA)
    dd = js("exp", "steps", "--key", k0, "--data", DATA, "--python", py, "--done")["steps"]
    exp_done = [bool(x["done"].ok(DATA, names0)) for x in E.steps_of(t0)]
    ok("exp steps --done：和 done.ok 一样（空的数据目录都没做完）", [s["done"] for s in dd] == exp_done and not any(exp_done))
    probe = next((s for s in st if (s["cmd"] or "").endswith("/.probe_done")), None)
    if probe:
        out = probe["cmd"].rsplit("touch ", 1)[1]
        os.makedirs(os.path.dirname(out), exist_ok=True)
        open(out, "w").close()
        lv = ["--key", probe["key"]] if probe["key"] else ["--exp", probe["exp"], "--level", probe["level"]]
        got = [s["done"] for s in js("exp", "steps", *lv, "--data", DATA, "--python", py, "--done")["steps"] if s["name"] == probe["name"]]
        ok(f"exp steps --done：{probe['name']} 的标志在了 → 做完", got == [True])
        os.remove(out)
    dn = js("exp", "steps", "--key", k0, "--data", DATA, "--python", py, "--needs")["steps"]
    staged = [s for s in dn if "--stage" in (s["cmd"] or "")]
    ok("exp steps --needs：带 --stage 的步骤都列出了输入（模型还不在本机），别的 needs 是 null",
       staged and all(s["needs"] and s["error"] is None for s in staged)
       and all(s["needs"] is None for s in dn if s not in staged)
       and any(x.endswith("/final.ckpt") for s in staged for x in s["needs"]))
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
    wb(os.path.join(src, ex["logs"] + "1_seg1.log"), b"log")
    im = js("spec", "import", "--spec", "-", "--src", src, "--dst", os.path.join(T, "dst"), "--root", ROOT, stdin=spj)
    w2 = N.import_(sp, src, os.path.join(T, "dst2"), ROOT)
    ok("spec import：顶层字段 = proto.RELAY_IMPORT_TOP，写的和插件拉回的一样、都在 --dst 里",
       tuple(im) == proto.RELAY_IMPORT_TOP and im["written"] == w2 and ex["logs"] + "1_seg1.log" in im["written"]
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
