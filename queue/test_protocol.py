#!/usr/bin/env python3
"""协议只在 proto.py 定义：核心里别处不再手写 proto 里的名字；TASKQ_* 都登记在 proto.ENV；proto 只用标准库、能单独跑。"""
import ast
import os
import re
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import proto  # noqa: E402

SKIP_DIRS = {"testdata", "tests", "__pycache__"}


def read(p):
    with open(p, encoding="utf-8") as f:
        return f.read()


def core_files():
    for d, ds, fs in os.walk(CORE):
        ds[:] = [x for x in ds if x not in SKIP_DIRS and not x.startswith(".")]
        for f in fs:
            if f.startswith("test_") or f.endswith((".md", ".pyc")) or f == "proto.py":
                continue
            p = os.path.join(d, f)
            if f.endswith((".py", ".sh", ".sbatch")) or f in ("q", "qs"):
                yield p


def code_lines(p):
    """去掉注释和文档字符串以后的行：(行号, 文字)。"""
    src = read(p)
    if not p.endswith(".py"):
        for i, ln in enumerate(src.splitlines(), 1):
            s = ln.lstrip()
            if not s.startswith("#"):
                yield i, re.sub(r"\s#\s.*$", "", ln)
        return
    doc = set()
    try:
        tree = ast.parse(src)
    except SyntaxError:
        tree = None
    for n in ast.walk(tree) if tree else ():
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)) and n.body and \
                isinstance(n.body[0], ast.Expr) and isinstance(getattr(n.body[0], "value", None), ast.Constant):
            doc.update(range(n.body[0].lineno, n.body[0].end_lineno + 1))
    for i, ln in enumerate(src.splitlines(), 1):
        if i in doc or ln.lstrip().startswith("#"):
            continue
        yield i, re.sub(r"\s#\s.*$", "", ln)


# proto 里的名字 → 在别处手写算违规的写法
LITERALS = {
    "taskq_stage_": r"taskq_stage_",
    "/tmp/gpu_pad_": r"/tmp/gpu_pad_",
    "@@JSON": r"@@JSON",
    "gpu_pad_DISABLED": r"gpu_pad_DISABLED",
    "view.lock": r"view\.lock",
    "state/pins": r"""state/pins|['"]state['"],\s*['"]pins['"]""",  # 按 proto.ROOT 的键（sub(R, "pins")）取的不算
    ".incomplete": r"\.incomplete",
    "MANIFEST.json": r"MANIFEST\.json",
    "code/current": r"code/current",
    ".relay_in": r"\.relay_in",
    "gpu_log.pid": r"gpu_log\.pid",
    # 目录名：拼路径时手写的（Q["claims"] 这种查表、任务状态的取值 "stopped" 不算）
    "stopped/": r"""stopped/|/stopped\b|join\([^)]*(?<!\[)['"]stopped['"]""",
    "claims/": r"""claims/|/claims\b|join\([^)]*(?<!\[)['"]claims['"]""",
}
# 迁移还没做完的地方：(文件相对 CORE, 名字)。只减不加
KNOWN = set()


class ProtoAlone(unittest.TestCase):
    def test_stdlib_only(self):
        tree = ast.parse(read(os.path.join(HERE, "proto.py")))
        mods = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        mods |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        self.assertLessEqual(mods, set(sys.stdlib_module_names))

    def test_sh_matches_python(self):
        out = subprocess.run([sys.executable, os.path.join(HERE, "proto.py"), "sh"], capture_output=True, text=True, check=True).stdout
        names = list(proto.sh_vars())
        script = out + "\n" + "\n".join(f'printf "%s\\0" "${{{n}}}"' for n in names)
        got = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True).stdout.split("\0")[:-1]
        self.assertEqual(got, [str(v) for v in proto.sh_vars().values()])

    def test_safe(self):
        self.assertEqual(proto.safe("D/T/0"), "D__T__0")
        self.assertEqual(proto.blob("abcdef"), "blobs/ab/abcdef")

    def test_claim_alive(self):
        now = 10_000.0
        self.assertTrue(proto.claim_alive(None, now - 60, now))
        self.assertFalse(proto.claim_alive(None, now - proto.HALF_CLAIM - 1, now))
        self.assertTrue(proto.claim_alive({"assigned": "site-pueue", "beat": 0}, 0, now))
        self.assertTrue(proto.claim_alive({"beat": now - 60}, 0, now))
        self.assertFalse(proto.claim_alive({"beat": now - proto.STALE_BEAT - 1}, 0, now))
        self.assertTrue(proto.claim_alive({"beat": 0}, 0, now, log_age=60))
        self.assertFalse(proto.claim_alive({"beat": 0}, 0, now, log_age=proto.FRESH_LOG + 1))


class NoCopies(unittest.TestCase):
    def test_env_registered(self):
        seen = {}
        for p in core_files():
            for v in re.findall(r"TASKQ_[A-Z][A-Z_]*[A-Z]", read(p)):
                seen.setdefault(v, os.path.relpath(p, CORE))
        prefixes = {"TASKQ_GPU_PAD"}  # gpu_pad.py 按前缀拼参数名
        missing = {v: f for v, f in seen.items() if v not in proto.ENV and v not in prefixes}
        self.assertEqual(missing, {}, "TASKQ_* 要登记在 proto.ENV")

    def test_no_handwritten_literals(self):
        bad = []
        for p in core_files():
            rel = os.path.relpath(p, CORE)
            for i, ln in code_lines(p):
                for name, pat in LITERALS.items():
                    if (rel, name) not in KNOWN and re.search(pat, ln):
                        bad.append(f"{rel}:{i} {name}: {ln.strip()[:100]}")
        self.assertEqual(bad, [], "这些名字只在 proto.py 定义")

    def test_known_still_needed(self):
        left = set()
        for p in core_files():
            rel = os.path.relpath(p, CORE)
            for _, ln in code_lines(p):
                for name, pat in LITERALS.items():
                    if (rel, name) in KNOWN and re.search(pat, ln):
                        left.add((rel, name))
        self.assertEqual(KNOWN - left, set(), "已经改好的从 KNOWN 里删掉")


if __name__ == "__main__":
    unittest.main()
