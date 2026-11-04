#!/usr/bin/env python3
"""只读的 JSON 命令（2026-11-04）：使用者的测试、脚本经命令行读核心按配置算出来的值，不 import 核心的模块。
输出一行 JSON（带 version，字段见 proto 的 MSHOW_*、EXP_STEPS_*、RELAY_*，只加不改）；--json 可写可不写。

  taskq machines show [名…] [--json]
      每台机器：种类、worker、开 worker 的命令、根目录的 shell 写法、能用的环境、激活语句、收编来源、给任务加的语句；
      还有环境描述（<配置文件夹>/specs/*.yml）的哈希、base、torch 变体
  taskq exp steps [--exp ID…] [--key KEY…] [--level task|group|final…] [--data 目录] [--python P] [--done] [--needs] [--json]
      实验表的步骤，命令按 names 填好（data 默认本机的数据目录，没有时留 {data}；python 默认 hub 的 python）。
      --done：在 --data 下判断做完没有；--needs：命令带 --stage 的真跑一次 --stage needs（TASKQ_DATA = --data）
  taskq spec export --spec <JSON|-> --root <目录> [--json]
      中继插件送出：要传的文件、视图、拉回的文件夹（只读 --root）
  taskq spec import --spec <JSON|-> --src <目录> --dst <目录> --root <目录> [--json]
      中继插件拉回核对：src 下是拉回的文件，核对过的放进 dst（写的只有 dst）
出错（说明不对、插件核对不过、实验表里没有）退出码 1，原因在 stderr；参数不对 2。
"""
import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
for d in ("sites", "monitor"):
    sys.path.insert(0, os.path.join(CORE, d))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import proto  # noqa: E402


def _line(obj):
    print(json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str))


def _fail(msg):
    print(msg, file=sys.stderr)
    return 1


# ---------------- taskq machines show ----------------
def machine_json(n):
    import pueue as PQ
    import remote as R
    import spec as S
    M = cfg.mtab()
    sites = getattr(M, "SITES", {})
    m = M.machine(n)
    workers = {k: M.worker_of(n, k) for k in M.KINDS}
    cmds = {}
    if n in sites and m.get("scheduler") == "pueue" and m.get("root"):
        cmds = {k: PQ.worker_cmd(n, k) for k in M.kinds(n)}
    return dict(name=n, table="SITES" if n in sites else "MACHINES", label=m.get("label"), scheduler=m.get("scheduler"),
                roles=list(m.get("roles") or ()), relay=n in M.RELAY, kinds=list(M.kinds(n)), workers=workers, worker_cmds=cmds,
                root=m.get("root"), root_q=R.q(m["root"]) if m.get("root") else None, ready_envs=sorted(M.ready_envs(n)),
                activate={e: M.activate(n, e) for e in m.get("envs") or {}}, adopt=M.adopt_sources(n), task_sh=S.machine_vars(m))


def spec_json(env):
    import envs as EV
    M = cfg.mtab()
    try:
        sp = EV.load(env)
    except Exception as e:  # noqa: BLE001  描述读不了（缺字段、不是 YAML、没装 yaml）
        return dict(hash=M.spec_hash(env), base=None, torch={}, error=f"{type(e).__name__}: {e}")
    torch = {k: (v or {}).get("index_url") for k, v in (sp.get("torch") or {}).items()}
    return dict(hash=M.spec_hash(env), base=sp.get("base"), torch=torch, error=None)


def machines_show(argv):
    ap = argparse.ArgumentParser(prog="taskq machines show", description="核心按机器表算出来的值（一行 JSON，proto.MSHOW_*）")
    ap.add_argument("names", nargs="*", metavar="名", help="只看这几台（默认 MACHINES 和 SITES 的全部）")
    ap.add_argument("--json", action="store_true", help="一行 JSON（默认就是）")
    a = ap.parse_args(argv)
    import glob
    M = cfg.mtab()
    every = list(M.MACHINES) + [n for n in getattr(M, "SITES", {}) if n not in M.MACHINES]
    bad = [n for n in a.names if n not in every]
    if bad:
        return _fail(f"机器表里没有 {'、'.join(bad)}（有：{'、'.join(every)}）")
    specs = sorted(os.path.basename(p)[:-4] for p in glob.glob(os.path.join(M.SPECS, "*.yml")))
    _line(dict(version=proto.VERSION, hub=M.hub(), machines={n: machine_json(n) for n in a.names or every},
               specs={s: spec_json(s) for s in specs}))
    return 0


# ---------------- taskq exp steps ----------------
LEVELS = ("task", "group", "final")


def _python():
    import advance as A
    try:
        return A.python()
    except (KeyError, TypeError, ValueError):  # hub 没有 conda
        return sys.executable


def exp_steps(argv):
    ap = argparse.ArgumentParser(prog="taskq exp steps", description="实验表的步骤，命令按 names 填好（一行 JSON，proto.EXP_STEPS_*）")
    ap.add_argument("--exp", nargs="+", metavar="ID", help="只列这几个实验的（FINAL 的不列）")
    ap.add_argument("--key", nargs="+", metavar="KEY", help="每个任务的步骤只列这几个任务的（实验一级、FINAL 的不列）")
    ap.add_argument("--level", nargs="+", choices=LEVELS, help="只列这几级：task（每个任务）、group（实验一级）、final")
    ap.add_argument("--data", help="names 的 data（默认本机的数据目录，没有时留 {data}）")
    ap.add_argument("--python", help="names 的 python（默认 hub 的 python）")
    ap.add_argument("--done", action="store_true", help="在 --data 下判断每个步骤做完没有（done.ok）")
    ap.add_argument("--needs", action="store_true", help="命令带 --stage 的真跑一次 --stage needs，列出输入")
    ap.add_argument("--json", action="store_true", help="一行 JSON（默认就是）")
    a = ap.parse_args(argv)
    import experiments as E
    import spec as S
    data = a.data or S.local_data()
    if (a.done or a.needs) and not data:
        return _fail("--done、--needs 要知道数据目录：给 --data，或设 TASKQ_DATA")
    data = os.path.abspath(os.path.expanduser(data)) if data else "{data}"
    py = a.python or _python()
    levels = set(a.level or LEVELS)
    if a.key:
        levels &= {"task"}
    try:
        exps, final = E.load()
        bad = [x for x in a.exp or [] if E.experiment(x) is None]
        bad += [k for k in a.key or [] if k not in E.tasks()]
        if bad:
            return _fail(f"实验表里没有 {'、'.join(bad)}")
        rows = []  # (行, 步骤)
        if "task" in levels:
            for k, t in E.tasks().items():
                if (a.exp and t["exp"] not in a.exp) or (a.key and k not in a.key):
                    continue
                names = E.task_names(t, data, py)
                for st in (E.experiment(t["exp"]) or {}).get("after") or []:
                    rows.append((dict(level="task", exp=t["exp"], key=k, name=st["name"], group=st.get("group")), st, names))
        if "group" in levels:
            for e in exps:
                if (a.exp and e["id"] not in a.exp) or not e.get("group_after"):
                    continue
                names = E.group_names(e, data, py)
                for c in e["group_after"]:
                    for st in c:
                        rows.append((dict(level="group", exp=e["id"], key=None, name=st["name"], group=st.get("group")), st, names))
        if "final" in levels and not a.exp:
            for f in final:
                rows.append((dict(level="final", exp=None, key=None, name=f["name"], group=f.get("group")), f, dict(data=data, python=py)))
    except S.SpecError as err:
        return _fail(str(err))
    out = [step_row(row, st, names, a.done, data) for row, st, names in rows]
    if a.needs:
        need_rows(out, data)
    _line(dict(version=proto.VERSION, data=data, python=py, steps=out))
    return 0


def step_row(row, st, names, done, data):
    import experiments as E
    import spec as S
    row.update(cmd=None, names=E.jsonable(names), done=None, needs=None, error=None)
    try:
        row["cmd"] = S.fill(st["cmd"], names, keep=())
    except Exception as e:  # noqa: BLE001
        row["error"] = f"cmd 填不上：{e}"
    if done:
        try:
            row["done"] = bool(st["done"].ok(data, names))
        except Exception as e:  # noqa: BLE001
            row["error"] = f"done.ok 出错：{type(e).__name__}: {e}"
    return row


def need_rows(rows, data):
    import advance as A
    todo = [r for r in rows if r["cmd"] and "--stage" in r["cmd"]]
    old = os.environ.get("TASKQ_DATA")
    os.environ["TASKQ_DATA"] = data
    try:
        with ThreadPoolExecutor(8) as ex:
            got = list(ex.map(lambda r: A.step_needs(r["cmd"]), todo))
    finally:
        if old is None:
            os.environ.pop("TASKQ_DATA", None)
        else:
            os.environ["TASKQ_DATA"] = old
    for r, g in zip(todo, got):
        r["needs"] = g
        if g is None:
            r["error"] = A.no_needs(r["cmd"])


# ---------------- taskq spec export / import ----------------
def _spec_arg(text):
    s = json.loads(sys.stdin.read() if text == "-" else text)
    if not isinstance(s, dict) or "key" not in s:
        raise ValueError("--spec 要是解析好的说明（taskq spec resolve 的输出）")
    return s


def spec_relay(argv, which):
    ap = argparse.ArgumentParser(prog=f"taskq spec {which}",
                                 description="中继插件" + ("送出（只读）" if which == "export" else "拉回核对，放进 --dst") + "（一行 JSON）")
    ap.add_argument("--spec", required=True, help="解析好的说明（JSON；- = 标准输入）")
    ap.add_argument("--root", required=True, help="box 的数据目录")
    if which == "import":
        ap.add_argument("--src", required=True, help="拉回的文件（真的相对路径）")
        ap.add_argument("--dst", required=True, help="核对过的放到这里")
    ap.add_argument("--json", action="store_true", help="一行 JSON（默认就是）")
    a = ap.parse_args(argv)
    import names as N
    import spec as S
    try:
        sp = _spec_arg(a.spec)
        if which == "export":
            ex = N.export(sp, a.root)
            _line(dict(version=proto.VERSION, plugin=N.plugin_of(sp), params=ex["params"], files=[list(f) for f in ex["files"]],
                       texts=ex["texts"], views=ex["views"], outs=ex["outs"], cleanup=ex["cleanup"], logs=S.outputs(sp)["logs"]))
        else:
            _line(dict(version=proto.VERSION, written=N.import_(sp, a.src, a.dst, a.root)))
    except (N.RelayError, S.SpecError, ValueError, KeyError) as e:
        return _fail(f"{type(e).__name__}: {e}")
    return 0


CMDS = {"machines-show": machines_show, "exp-steps": exp_steps,
        "spec-export": lambda argv: spec_relay(argv, "export"), "spec-import": lambda argv: spec_relay(argv, "import")}


def main(argv):
    if not argv or argv[0] not in CMDS:
        print(__doc__, file=sys.stderr)
        return 2
    return CMDS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
