#!/usr/bin/env python3
"""Find the user's project: the project root holds .taskq.toml, stating the config and plugin dirs (relative to the project root):

    config = "config"
    plugins = ["plugins"]

Lookup order: an explicit dir (--project; taskq passes it to child processes via TASKQ_PROJECT) > find .taskq.toml upwards from the
current dir. Standard library only, no core modules imported.

    python3 project.py sh [--project dir]   export statements for bash (TASKQ_REPO, TASKQ_CONFIG, TASKQ_PLUGINS; ones already set
                                            are not printed); no project found prints nothing and exits 1
"""
import os
import re
import shlex
import sys

NAME = ".taskq.toml"


def find(explicit=None, start=None):
    """The project root (absolute path); not found -> None."""
    for v in (explicit, os.environ.get("TASKQ_PROJECT")):
        if v:
            return os.path.abspath(os.path.expanduser(v))
    d = os.path.abspath(start or os.getcwd())
    while True:
        if os.path.isfile(os.path.join(d, NAME)):
            return d
        up = os.path.dirname(d)
        if up == d:
            return None
        d = up


_STR = r'"((?:[^"\\]|\\.)*)"'
_LINE = re.compile(r'([A-Za-z0-9_-]+)\s*=\s*(.*?)\s*(?:#[^"]*)?$')


def _unq(s):
    return s.encode("utf-8").decode("unicode_escape") if "\\" in s else s


def parse(text):
    """A small parser for when tomllib is absent (Python < 3.11): only key = "string" and key = ["a", "b"] (on one line); comments
    and blank lines are skipped."""
    out = {}
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _LINE.fullmatch(line)
        v = m and m.group(2)
        if v and re.fullmatch(_STR, v):
            out[m.group(1)] = _unq(re.fullmatch(_STR, v).group(1))
        elif v and re.fullmatch(r"\[\s*(?:%s\s*(?:,\s*%s\s*)*,?\s*)?\]" % (_STR, _STR), v):
            out[m.group(1)] = [_unq(x) for x in re.findall(_STR, v)]
        else:
            raise ValueError(f"{NAME} line {n}: cannot parse: {raw}")
    return out


def load(root):
    """The project root's .taskq.toml -> dict; no such file -> {}."""
    p = os.path.join(root, NAME)
    if not os.path.isfile(p):
        return {}
    with open(p, "rb") as f:
        data = f.read()
    try:
        import tomllib
    except ImportError:
        return parse(data.decode("utf-8"))
    return tomllib.loads(data.decode("utf-8"))


def _plugins(v):
    return [v] if isinstance(v, str) else list(v or [])


def env_of(root):
    """The project's env vars {name: value}: always TASKQ_REPO; TASKQ_CONFIG and TASKQ_PLUGINS only when .taskq.toml states them."""
    conf = load(root)
    out = {"TASKQ_REPO": root}
    if "config" in conf:
        out["TASKQ_CONFIG"] = os.path.join(root, conf["config"])
    if "plugins" in conf:
        out["TASKQ_PLUGINS"] = os.pathsep.join(os.path.join(root, p) for p in _plugins(conf["plugins"]))
    return out


def export(root, force=False):
    """Set the project's env vars in os.environ (ones already set are unchanged; force: --project was explicit, overwrite) -> the
    newly set {name: value}."""
    new = {k: v for k, v in env_of(root).items() if force or k not in os.environ}
    os.environ.update(new)
    return new


REBASE = ("TASKQ_REPO", "TASKQ_PROJECT", "TASKQ_CONFIG", "TASKQ_PLUGINS")


def rebase(old, new, env=None):
    """env 里 REBASE 这几个变量中在 old 下的路径（: 隔开的每一段）换到 new 下 → 改了的 {名: 值}。old 按真实路径比较。"""
    env = os.environ if env is None else env
    o = os.path.realpath(old)
    out = {}
    for k in REBASE:
        v = env.get(k)
        if not v:
            continue
        parts = []
        for p in v.split(os.pathsep):
            r = os.path.realpath(p)
            rel = os.path.relpath(r, o) if r == o or r.startswith(o + os.sep) else None
            parts.append(p if rel is None else os.path.normpath(os.path.join(new, rel)))
        nv = os.pathsep.join(parts)
        if nv != v:
            out[k] = nv
    return out


def main(argv):
    if argv and argv[0] == "rebase" and len(argv) == 3:
        for k, v in rebase(argv[1], argv[2]).items():
            print(f"export {k}={shlex.quote(v)}")
        return 0
    if not argv or argv[0] != "sh" or len(argv) not in (1, 3) or (len(argv) == 3 and argv[1] != "--project"):
        print(__doc__.split("\n\n")[3], file=sys.stderr)
        return 2
    root = find(argv[2] if len(argv) == 3 else None)
    if not root:
        print(f"project.py: no {NAME} above {os.getcwd()} and TASKQ_PROJECT is not set", file=sys.stderr)
        return 1
    for k, v in env_of(root).items():
        if k not in os.environ:
            print(f"export {k}={shlex.quote(v)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
