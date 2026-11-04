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
    import argparse
    ap = argparse.ArgumentParser(description="执行机的环境（在 box 上跑）")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("env", help="按 envs/specs/<环境>.yml 在那台的 CPU 上建环境、跑检查，过了写 <环境>.ok")
    p.add_argument("machine")
    p.add_argument("env")
    p.add_argument("-n", "--dry-run", action="store_true", help="只打印要跑的命令和脚本")
    p = sub.add_parser("envs", help="只读：每个环境的状态（ok / 描述变了 / 没建 / 检查失败 / 在建）")
    p.add_argument("machine")
    a = ap.parse_args(argv)
    if a.cmd == "env":
        try:
            rc, out = E.build(a.machine, a.env, dry=a.dry_run)
        except (E.EnvError, KeyError) as e:
            print(f"site.py：{e}", file=sys.stderr)
            return 2
        print(out)
        return rc
    try:
        st = E.states(a.machine)
    except KeyError as e:
        print(f"site.py：不认识的机器 {e}", file=sys.stderr)
        return 2
    if st is None:
        print(f"连不上 {a.machine}", file=sys.stderr)
        return 1
    for k, v in st.items():
        print(f"{k}\t{v}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
