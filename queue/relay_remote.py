"""Remote operations for executors and the store: rsh (one shell command), rtq (remote taskq.py), op (one site_ops.py
operation) and store_op. Hub-side bookkeeping is in relay_store.py, staging and fetching in relay_transfer.py, planning in
relay.py.
The remote root R layout, marker files and timing constants are in proto.py (ROOT, MKDIRS, blob, QUEUE, claim_alive); operations
run inside the bundle in use (code/current), local machines too (ssh=None)."""
import json
import os
import re
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduler code
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
import proto  # noqa: E402
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402
import remote as R  # noqa: E402
import slurm as SL  # noqa: E402

JOB_WAIT = 600  # max wait to get scheduled when running in a CPU job (machines.py transfer_job)


class NoSlot(RuntimeError):
    """The CPU job could not be scheduled this time."""


ERR_LINE = re.compile(r"(?:[\w.]*(?:Error|Exception)\b|错误|[\w.-]+: (?:error|错误))")  # start of an error line: exception name (last line of a Traceback), the Chinese word for error, "<program>: error"; the Chinese alternative stays because other programs print it, not our format


def err_tail(text, n=300, own=()):
    """Output from a remote failure → a readable excerpt: the last n chars; when the last error line (matching ERR_LINE, or
    starting with one of own: site_ops reports its own errors as "<op>: reason") is not in there (pushed out by later progress
    output), put it in front."""
    t = (text or "").strip()
    tail = t[-n:]
    hit = next((x for x in (ln.strip() for ln in reversed(t.splitlines())) if ERR_LINE.match(x) or x.startswith(tuple(own))), None)
    return f"{hit[:n]} …… {tail}" if hit and hit not in tail else tail


def conf(name):
    return M.machine(name)


def root(name):
    return conf(name)["root"].rstrip("/")


def py(name):
    """Python to run site_ops with: the machine table's conda one if present, otherwise python3 from the remote PATH
    (site_ops only uses the standard library)."""
    c = conf(name).get("conda")
    return c[0] + "/bin/python3" if c else "python3"


def ssh_argv():
    return shlex.split(os.environ.get("RELAY_SSH", "ssh")) + C.SSH[1:]  # RELAY_SSH is for tests only


def master_down(m):
    """True when machine entry m uses an ssh master connection (ssh_master) and it is down: do not open another connection
    (repeated auth failures on a relay site get it locked)."""
    import subprocess
    return bool(m.get("ssh_master") and m.get("ssh")) and subprocess.run(
        ssh_argv()[:1] + ["-O", "check", m["ssh"]], capture_output=True, timeout=30).returncode != 0


def site_argv(name, cmd, timeout=300):
    """Argument list for running one shell command on an executor: over ssh (with a remote timeout too, common.remote_timeout);
    a local executor (ssh=None) runs it in the home dir on this machine with hub-only variables removed (proto.HUB_ONLY_ENV),
    the same as when logging in over ssh."""
    host = conf(name)["ssh"]
    if host is None:
        return ["env", *[a for v in proto.HUB_ONLY_ENV for a in ("-u", v)], "bash", "-c", f"cd ~ && {cmd}"]
    return ssh_argv() + [host, C.remote_timeout(f"bash -c {shlex.quote(cmd)}", timeout)]


def rsh(name, cmd, input=None, timeout=300):
    return C.run(site_argv(name, cmd, timeout), None, timeout, input=input)


def prefix(name):
    """Prefix for remote commands: enter the bundle and set TASKQ_ROOT and TASKQ_MACHINE (the queue, state and data dir are
    derived from them in cfg.py)."""
    r = R.q(root(name))
    return f"cd {r}/{proto.ROOT['current']} && TASKQ_ROOT={r} TASKQ_MACHINE={shlex.quote(name)}"


def rtq(name, args, timeout=180, input=None):
    """Remote taskq.py <args> (input: stdin) → (exit code, output); None when unreachable."""
    rc, out, err = rsh(name, f"{prefix(name)} {py(name)} {cfg.bundle_rel('queue', 'taskq.py')} {args}", input=input, timeout=timeout)
    return None if rc in (proto.EXIT["timeout"], 255) else (rc, (out + err).strip())


def run_lines(argv, input, timeout, say):
    """Run a command → (exit code, stdout, stderr); pass stderr to say line by line (long transfers and archives show progress
    while running). Timeout → 124. stdin, stdout and stderr each get their own thread (communicate also reads stderr and drops
    lines when competing with the line-by-line reader)."""
    import subprocess
    import threading
    p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    err, out = [], []

    def pump():
        for ln in p.stderr:
            err.append(ln)
            say(ln.rstrip("\n"))

    def feed():
        try:
            p.stdin.write(input or "")
            p.stdin.close()
        except (BrokenPipeError, OSError):
            pass
    ths = [threading.Thread(target=f, daemon=True) for f in (pump, feed, lambda: out.append(p.stdout.read()))]
    for th in ths:
        th.start()
    rc = None
    try:
        rc = p.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    for th in ths:
        th.join(10)
    if rc is None:
        return 124, "".join(out), "".join(err) + f"\ntimed out after {timeout} s"
    return rc, "".join(out), "".join(err)


def op(name, what, data=None, timeout=600, job=None, say=None, script="site_ops.py"):
    """Run one site_ops.py operation in the bundle in use on that machine (data as JSON on stdin) → its result.
    Failure → RuntimeError. Local machines (ssh=None) run the bundle in their own root too, same as remote ones.
    job: Slurm srun args (machines.py transfer_job) to run on a CPU node; no slot → NoSlot.
    say: if given, pass remote progress (stderr) to it line by line while running.
    script: another script under queue/ in the bundle with the same calling convention (migrate.py root)."""
    m = conf(name)
    cmd = f"{prefix(name)} {py(name)} {cfg.bundle_rel('queue', script)} {what}"
    if job:
        cmd = f"srun -I{JOB_WAIT} {job} -J {SL.jobname(m.get('job_names'))} bash -c {shlex.quote(cmd)}"
        timeout += JOB_WAIT
    argv, inp = site_argv(name, cmd, timeout), json.dumps(data)
    rc, out, err = run_lines(argv, inp, timeout, say) if say else C.run(argv, None, timeout, input=inp)
    if job and rc and S.json_in(out) is None and any(x in err for x in ("Unable to allocate", "Immediate", "revoked")):
        raise NoSlot(f"no CPU job slot on {m['label']}: {err_tail(err, 200)}")
    res = S.json_in(out)
    if rc or res is None:
        raise RuntimeError(f"{what} failed on {m['label']} ({rc}): {err_tail(err or out, own=(f'{what}: ',))}")
    return res


def store_op(what, data, timeout=600, say=None):
    """Run one site_ops.py store operation on the store machine, with the store's data dir (machines.py data)."""
    return op(M.store(), what, dict(data, data=M.data()), timeout, say=say)


def same_disk_srcs(name):
    """Data dirs on this machine to look in first when ingesting into blobs (hard links to matching content on the same disk,
    no retransfer): its own data dir, plus the store's data dir when it is also the store."""
    return [f"{root(name)}/{proto.ROOT['data']}"] + ([M.data()] if name == M.store() else [])
