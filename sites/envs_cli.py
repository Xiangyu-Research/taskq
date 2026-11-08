#!/usr/bin/env python3
"""Executor envs: the command line of envs.py. Runs on the hub.

    taskq site env <machine> <env> [--variant V] [-n]
                                      build <config dir>/specs/<env>.yml on that machine's CPU (from the lock and offline
                                      when <env>.<variant>.lock.json exists), run its check, then write <env>.ok. Also
                                      builds entries still listed as conda / venv in the machine table (prebuild). The
                                      variant comes from the table entry, default when the entry is not "spec"
                                      (-n / --dry-run: print the commands and script only)
    taskq site envs <machine>         read only: each env's state (ok / spec changed / not built / check failed /
                                      no space / building)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import envs as E  # noqa: E402


def main(argv):
    import argparse
    ap = argparse.ArgumentParser(description="executor envs (runs on the hub)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("env", help="build <config dir>/specs/<env>.yml on that machine's CPU, run its check and write <env>.ok")
    p.add_argument("machine")
    p.add_argument("env")
    p.add_argument("--variant", help="variant (default from the table entry, or default when the entry is not \"spec\")")
    p.add_argument("-n", "--dry-run", action="store_true", help="print the commands and script only")
    p = sub.add_parser("envs", help="read only: each env's state (ok / spec changed / not built / check failed / no space / building)")
    p.add_argument("machine")
    a = ap.parse_args(argv)
    from proto import EXIT
    say = lambda msg: print(f"{ap.prog} {a.cmd}: {msg}", file=sys.stderr)  # noqa: E731
    try:
        m = E.M.machine(a.machine)
    except KeyError:
        say(f"no {a.machine} in the machine table")
        return EXIT["gone"]
    if a.cmd == "env":
        if a.env not in (m.get("envs") or {}) and E.M.spec_hash(a.env) is None:
            say(f"{a.machine}'s envs has no {a.env} and there is no spec for it (have: {', '.join(m.get('envs') or {}) or 'none'})")
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
        print(f"cannot reach {a.machine}", file=sys.stderr)
        return 1
    for k, v in st.items():
        print(f"{k}\t{v}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
