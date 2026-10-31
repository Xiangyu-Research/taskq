#!/usr/bin/env python3
"""执行机的准备（2026-10-31）。在 box 上跑。

    site.py env <机器> <环境> [--dry-run]   按 envs/specs/<环境>.yml 在那台的 CPU 上建环境、跑检查，过了写 <环境>.ok
                                          （--dry-run：只打印要跑的命令和脚本）
    site.py envs <机器>                    只读：每个环境的状态（ok / 描述变了 / 没建 / 检查失败 / 在建）
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import envs as E  # noqa: E402


def main(argv):
    if len(argv) >= 3 and argv[0] == "env":
        try:
            rc, out = E.build(argv[1], argv[2], dry="--dry-run" in argv[3:])
        except (E.EnvError, KeyError) as e:
            print(f"site.py：{e}", file=sys.stderr)
            return 2
        print(out)
        return rc
    if len(argv) == 2 and argv[0] == "envs":
        try:
            st = E.states(argv[1])
        except KeyError as e:
            print(f"site.py：不认识的机器 {e}", file=sys.stderr)
            return 2
        if st is None:
            print(f"连不上 {argv[1]}", file=sys.stderr)
            return 1
        for k, v in st.items():
            print(f"{k}\t{v}")
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
