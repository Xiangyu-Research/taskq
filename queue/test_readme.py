"""README 和代码对得上：README.md 的命令表和 bin/taskq 的 TREE 一样；README 里标记之间的字段表和 queue/schema.py 生成的一样；
"改了哪个文件跑哪些测试"的表里的文件都在、每个测试文件都列着；README 里提到的核心文件都在。
只读核心自己的文件；跑 sites/mtab.py fields --markdown 时用固定配置 queue/testdata/。
"""
import glob
import importlib.machinery
import importlib.util
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402
import schema  # noqa: E402

fails = 0


def ok(what, cond):
    global fails
    print(("通过：" if cond else "失败：") + what)
    fails += 0 if cond else 1


def read(rel):
    with open(os.path.join(CORE, rel), encoding="utf-8") as f:
        return f.read()


def between(text, start, end):
    """标记 start … end 之间的文字（每对一段）。"""
    return re.findall(re.escape(start) + r"\n(.*?)\n" + re.escape(end), text, re.S)


def cli():
    """bin/taskq 当模块载入（文件名没有 .py）。"""
    loader = importlib.machinery.SourceFileLoader("taskq_cli", os.path.join(CORE, "bin", "taskq"))
    spec = importlib.util.spec_from_loader("taskq_cli", loader)
    m = importlib.util.module_from_spec(spec)
    loader.exec_module(m)
    return m


READMES = ("README.md", "queue/README.md")
TEXT = {r: read(r) for r in READMES}
REGEN = "bin/taskq --project example machines fields --markdown {}"

# ---------------- 命令表 ----------------
m = cli()
want = set()
for name, node in m.TREE.items():
    if isinstance(node, m.Group):
        want |= {f"{name} {sub}" for sub in node.cmds}
    else:
        want.add(name)
blocks = between(TEXT["README.md"], "<!-- commands -->", "<!-- /commands -->")
ok("README.md 有一段命令表（<!-- commands --> … <!-- /commands -->）", len(blocks) == 1)
rows = re.findall(r"^\| `taskq ([^`]+)` \|", blocks[0] if blocks else "", re.M)
have = set(rows)
ok(f"命令表包含 TREE 里的每个命令和子命令（含 hidden 组），缺：{sorted(want - have)}", not want - have)
ok(f"命令表里没有 TREE 里没有的命令，多了：{sorted(have - want)}", not have - want)
ok("命令表里每个命令只有一行", len(rows) == len(have))
hidden = [n for n, c in m.TREE.items() if isinstance(c, m.Group) and c.hidden]
ok(f"hidden 组（{'、'.join(hidden)}）也列在命令表里", hidden and all(any(r.startswith(h + " ") for r in rows) for h in hidden))

# ---------------- 字段表 ----------------
seen = set()
for r in READMES:
    for name, body in re.findall(r"<!-- fields:(\w+) -->\n(.*?)\n<!-- /fields -->", TEXT[r], re.S):
        seen.add(name)
        if name not in schema.TABLES:
            ok(f"{r} 的字段表 {name} 是 schema.TABLES 里的", False)
            continue
        ok(f"{r} 的字段表 {name} 和 schema.py 生成的一样" + ("" if body == schema.markdown(name) else
           f"（不一样：用 {REGEN.format(name)} 重新生成，贴到标记之间）"), body == schema.markdown(name))
ok(f"schema.py 的七张表都贴在 README 里，缺：{sorted(set(schema.TABLES) - seen)}", set(schema.TABLES) <= seen)
ok("schema.TABLES 是七张字段表", len(schema.TABLES) == 7 and all(isinstance(getattr(schema, t), dict) for t in schema.TABLES))
ok("markdown：说明里的 | 转义、必写的写'必写'",
   "\\|" in schema.markdown("POLICY_FIELDS") and "| `AUTO` | `auto` | 必写 |" in schema.markdown("POLICY_FIELDS"))

env = dict(os.environ, TASKQ_CONFIG=fixture.CONFIG, TASKQ_PLUGINS=fixture.PLUGINS)
mtab = os.path.join(CORE, "sites", "mtab.py")
r = subprocess.run([sys.executable, mtab, "fields", "--markdown", "POLICY_FIELDS"], capture_output=True, text=True, env=env, timeout=60)
ok("mtab.py fields --markdown <表>：带标记的 schema.markdown 输出",
   r.returncode == 0 and r.stdout.strip() == f"<!-- fields:POLICY_FIELDS -->\n{schema.markdown('POLICY_FIELDS')}\n<!-- /fields -->")
r = subprocess.run([sys.executable, mtab, "fields", "--markdown"], capture_output=True, text=True, env=env, timeout=60)
ok("mtab.py fields --markdown：七张表都有", r.returncode == 0 and all(f"<!-- fields:{t} -->" in r.stdout for t in schema.TABLES))
r = subprocess.run([sys.executable, mtab, "fields", "--markdown", "NO_SUCH"], capture_output=True, text=True, env=env, timeout=60)
ok("mtab.py fields --markdown 不认识的表 → 退出码 2", r.returncode == 2)

# ---------------- "改了哪个文件跑哪些测试" ----------------
blocks = between(TEXT["README.md"], "<!-- tests -->", "<!-- /tests -->")
ok("README.md 有一段测试表（<!-- tests --> … <!-- /tests -->）", len(blocks) == 1)
table = blocks[0] if blocks else ""
tests = {os.path.basename(p) for d in ("queue", "sites", "sync", "monitor") for p in glob.glob(os.path.join(CORE, d, "test_*"))
         if p.endswith((".py", ".sh"))}
named = set(re.findall(r"`(test_\w+\.(?:py|sh))`", table))
ok(f"测试表里的测试文件都在，不在的：{sorted(named - tests)}", not named - tests)
ok(f"每个测试文件在测试表里至少出现一次，没列的：{sorted(tests - named)}", not tests - named)
srcs = [p for p in re.findall(r"`([\w./-]+)`", table) if "/" in p]
ok(f"测试表里的源文件、文件夹都在，不在的：{[p for p in srcs if not os.path.exists(os.path.join(CORE, p))]}",
   srcs and all(os.path.exists(os.path.join(CORE, p)) for p in srcs))

# ---------------- README 里提到的路径 ----------------
PATH_RE = re.compile(r"`((?:bin|queue|sites|sync|monitor|example)/[\w./-]*)`")
for r in READMES:
    paths = sorted(set(PATH_RE.findall(TEXT[r])))
    gone = [p for p in paths if not os.path.exists(os.path.join(CORE, p))]
    ok(f"{r} 里提到的 {len(paths)} 个核心路径都在，不在的：{gone}", paths and not gone)

# ---------------- 不写历史和日期 ----------------
for r in READMES:
    dates = re.findall(r"\b\d{1,2}/\d{1,2}\b|\b20\d\d-\d\d-\d\d\b", TEXT[r])
    ok(f"{r} 里没有日期，有：{dates[:5]}", not dates)
    old = [w for w in ("原来叫", "原来的入口", "旧的写法", "照旧能用") if w in TEXT[r]]
    ok(f"{r} 里不写历史（原来叫、原来的入口、旧的写法…），有：{old}", not old)

sys.exit(1 if fails else 0)
