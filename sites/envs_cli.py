#!/usr/bin/env python3
"""执行机的环境（2026-10-31；11/4 前叫 site.py，和标准库的 site 重名）：envs.py 的命令行。在 box 上跑。

    taskq site env <机器> <环境> [--variant V] [-n]
                                      按 envs/specs/<环境>.yml（有 <环境>.<变体>.lock.json 时按锁、不联网）在那台的 CPU 上建环境、
                                      跑检查，过了写 <环境>.ok。机器条目里还是 conda / venv 的也能建（预建）。变体默认按机器条目，
                                      条目不是 "spec" 时是 default（-n / --dry-run：只打印要跑的命令和脚本）
    taskq site envs <机器>              只读：每个环境的状态（ok / 描述变了 / 没建 / 检查失败 / 空间不够 / 在建）
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
    p.add_argument("--variant", help="变体（默认按机器条目，不是 \"spec\" 时 default）")
    p.add_argument("-n", "--dry-run", action="store_true", help="只打印要跑的命令和脚本")
    p = sub.add_parser("envs", help="只读：每个环境的状态（ok / 描述变了 / 没建 / 检查失败 / 空间不够 / 在建）")
    p.add_argument("machine")
    a = ap.parse_args(argv)
    from proto import EXIT
    say = lambda msg: print(f"{ap.prog} {a.cmd}：{msg}", file=sys.stderr)  # noqa: E731
    try:
        m = E.M.machine(a.machine)
    except KeyError:
        say(f"机器表里没有 {a.machine}")
        return EXIT["gone"]
    if a.cmd == "env":
        if a.env not in (m.get("envs") or {}) and E.M.spec_hash(a.env) is None:
            say(f"{a.machine} 的 envs 里没有 {a.env}，也没有它的描述（有：{'、'.join(m.get('envs') or {}) or '没有'}）")
            return EXIT["gone"]
        try:
            rc, out = E.build(a.machine, a.env, dry=a.dry_run, v=a.variant, say=lambda x: print(x, file=sys.stderr, flush=True))
        except (E.EnvError, KeyError, OSError) as e:
            say(e)
            return 2
        print(out)
        return rc
    st = E.states(a.machine)
    if st is None:
        print(f"连不上 {a.machine}", file=sys.stderr)
        return 1
    for k, v in st.items():
        print(f"{k}\t{v}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
