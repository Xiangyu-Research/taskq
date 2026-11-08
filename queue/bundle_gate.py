#!/usr/bin/env python3
"""Bundle gate: before sending a task spec to an executor, run what the executor does with the spec (CMDS) on this machine
using the bundle in use there (the same-named one under relay_bundle.OUT on the hub). If it fails, do not send it: the task is
recorded as held (proto.HELD["bundle"]) and bundlegate is reported.
Bundles in use: the one code/current points at there, plus those of running Slurm workers (consume.py switches to current only
when claiming a new task and uses its start bundle until then; a worker touches its bundle on every heartbeat, and one touched
within STALE_BEAT counts as in use). A pueue worker stops claiming new tasks when its code changes, so only current counts.
Unknown bundle, no such bundle on the hub, or timeout -> allow. Results are cached in-process by (bundle name, spec hash).
Recorded in relay state held (<name> -> {key: dict(why, bundle, err, since)}), rechecked after the bundle in use changes.

    python3 queue/bundle_gate.py <bundle name or path> < spec.json     exit 0 when it runs; otherwise print why and exit 1
"""
import hashlib
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import proto  # noqa: E402
import relay_bundle as RB  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_store as ST  # noqa: E402
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "sites"))
import remote as R  # noqa: E402

TIMEOUT = 10  # max seconds per command; timeout allows
# what an executor does with a sent spec: run_task.sh and the worker (consume.py) call spec.py shell / done --spec (taskq.py add --spec-stdin parses like shell)
CMDS = dict(shell=(0,), done=(proto.EXIT["done_yes"], proto.EXIT["done_no"]))  # command -> exit codes that count as runnable
SPEC_PY = cfg.bundle_rel("queue", "spec.py")
WHY = proto.HELD["bundle"]
_CACHE = {}


def spec_hash(spec):
    return hashlib.sha256(json.dumps(spec, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def bundle_dir(bid, out=None):
    """Bundle name -> the path of that bundle on the hub; absent -> None."""
    if not bid or "/" in bid or bid.startswith("."):
        return None
    d = os.path.join(os.path.expanduser(out or RB.OUT), bid)
    return d if os.path.isfile(os.path.join(d, SPEC_PY)) else None


def run(d, spec):
    """Run CMDS in bundle d -> (whether the result may be cached, failure reason or None). The data dir is the bundle itself (read-only, done finds no results = not done)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("TASKQ_") and k not in ("PYTHONPATH", "PYTHONHOME")}
    env.update(TASKQ_DATA=d, PYTHONDONTWRITEBYTECODE="1")  # the bundle is pushed as is: do not write __pycache__ into it
    arg = json.dumps(spec, ensure_ascii=False)
    for c, good in CMDS.items():
        try:
            r = subprocess.run([sys.executable, os.path.join(d, SPEC_PY), c, "--spec", arg], cwd=d, env=env,
                               capture_output=True, text=True, timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            return False, None
        if r.returncode not in good or "Traceback" in r.stderr:
            last = (r.stderr.strip() or r.stdout.strip()).splitlines()[-1:]
            return True, f"spec.py {c} exited {r.returncode}: {(last[0] if last else '')[-300:]}"
    return True, None


def check(spec, bid, out=None):
    """Bundle bid can run the sent spec -> None; cannot -> reason. Unknown bundle, absent on the hub, or no spec -> None (allow)."""
    d = bundle_dir(bid, out)
    if d is None or not spec:
        return None
    k = (bid, spec_hash(spec))
    if k not in _CACHE:
        keep, err = run(d, spec)
        if not keep:
            return None
        _CACHE[k] = err
    return _CACHE[k]


def in_use(name, cur, live=()):
    """Bundles on this machine that must run the spec: current, plus running workers' (live) on Slurm."""
    ids = [cur] + (list(live or ()) if RM.conf(name)["scheduler"] == "slurm" else [])
    return list(dict.fromkeys(x for x in ids if x))


def check_all(spec, ids, out=None):
    """-> (a bundle that cannot run it, reason); all can -> None."""
    for bid in ids:
        err = check(spec, bid, out)
        if err:
            return bid, err
    return None


def current(name):
    """Bundles in use there (in_use arguments: current, live); unreachable -> None."""
    c = RM.conf(name)
    if c.get("ssh_master") and subprocess.run(RM.ssh_argv()[:1] + ["-O", "check", c["ssh"]], capture_output=True, timeout=30).returncode:
        return None  # persistent connection is down: do not start another (too many auth failures lock the relay site's account)
    d = f"{R.q(RM.root(name))}/{proto.ROOT['code']}"
    cur = f"{R.q(RM.root(name))}/{proto.ROOT['current']}"
    rc, out, _ = RM.rsh(name, f'[ -e {cur} ] && basename "$(readlink -f {cur})"; echo; [ -d {d} ] && '
                              f"find {d} -mindepth 1 -maxdepth 1 -type d -mmin -{proto.STALE_BEAT // 60} ! -name '*.tmp*' -printf '%f\\n'",
                        timeout=30)
    if rc not in (0, 1):
        return None
    head, _, rest = out.partition("\n")
    return head.strip(), rest.split()


# ---------------- records (relay state held) ----------------
def held(st, name):
    return ((st or {}).get("held") or {}).get(name) or {}


def hold(name, key, bid, err):
    rec = dict(why=WHY, bundle=bid, err=err, since=time.time())
    ST.update_state(lambda st: st.setdefault("held", {}).setdefault(name, {}).update({key: rec}))


def unhold(name, key):
    ST.update_state(lambda st: ((st.get("held") or {}).get(name) or {}).pop(key, None))


def main(argv):
    if len(argv) != 1:
        print(__doc__.strip().split("\n\n")[-1], file=sys.stderr)
        return 2
    d = argv[0] if os.path.isdir(argv[0]) else bundle_dir(argv[0])
    if d is None:
        print(f"no bundle {argv[0]} on the hub ({RB.OUT}): allowing", file=sys.stderr)
        return 0
    _, err = run(os.path.abspath(d), json.loads(sys.stdin.read()))
    if err:
        print(err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
