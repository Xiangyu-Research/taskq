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


class OldBundle(RuntimeError):
    """The bundle in use on that machine has no such script or operation (it is older than this core)."""

    def __init__(self, bundle, script, what):
        self.bundle = bundle
        super().__init__(f"the bundle {bundle or 'in use'} on that machine has no {script} {what}")


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
    return shlex.split(os.environ.get("RELAY_SSH", C.XSSH)) + C.SSH[1:]  # RELAY_SSH is for tests only


def master_down(m):
    """True when machine entry m uses an ssh master connection (ssh_master) and it is down: do not open another connection
    (repeated auth failures on a relay site get it locked)."""
    import subprocess
    return bool(m.get("ssh_master") and m.get("ssh")) and subprocess.run(
        ssh_argv()[:1] + ["-O", "check", m["ssh"]], capture_output=True, timeout=30).returncode != 0


def site_argv(name, cmd, timeout=300):
    """Argument list for running one shell command on an executor: over ssh (with a remote timeout too, common.remote_timeout);
    a local executor (ssh=None) runs it in the home dir on this machine with hub-only variables removed (proto.HUB_ONLY_ENV),
    the same as when logging in over ssh. With agent=True both go through the machine's resident agent (xssh, sync/agent_box.py)."""
    host = conf(name)["ssh"]
    if host is None and conf(name).get("agent"):
        return [C.XSSH, "--machine", name, C.remote_timeout(f"bash -c {shlex.quote(cmd)}", timeout)]
    if host is None:
        return ["env", *[a for v in proto.HUB_ONLY_ENV for a in ("-u", v)], "bash", "-c", f"cd ~ && {cmd}"]
    return ssh_argv() + [host, C.remote_timeout(f"bash -c {shlex.quote(cmd)}", timeout)]


def rsh(name, cmd, input=None, timeout=300):
    return C.run(site_argv(name, cmd, timeout), None, timeout, input=input)


def prefix(name, guard=None, bundle=None):
    """Prefix for remote commands: enter the bundle and set TASKQ_ROOT and TASKQ_MACHINE (the queue, state and data dir are
    derived from them in cfg.py). guard: a shell test run before that (fails the command when it fails). bundle: enter
    code/<bundle> instead of the one in use (self migrate, before switching)."""
    r = R.q(root(name))
    d = f"{proto.ROOT['code']}/{shlex.quote(bundle)}" if bundle else proto.ROOT["current"]
    return (f"cd {r}/{d} && " + (f"{guard} && " if guard else "")
            + f"TASKQ_ROOT={r} TASKQ_MACHINE={shlex.quote(name)}")


def rtq(name, args, timeout=180, input=None):
    """Remote taskq.py <args> (input: stdin) → (exit code, output); None when unreachable."""
    rc, out, err = rsh(name, f"{prefix(name)} {py(name)} {cfg.bundle_rel('queue', 'taskq.py')} {args}", input=input, timeout=timeout)
    return None if rc in (proto.EXIT["timeout"], 255) else (rc, (out + err).strip())


def set_nice(name, keys, nice):
    """Change only the stored nice of keys in that executor's queue (taskq.py set-nice there) → (exit code, output); None when
    unreachable."""
    return rtq(name, f"set-nice --nice {int(nice)} -- " + " ".join(shlex.quote(k) for k in keys))


def push_nice(name, keys, nice, dry=False):
    """taskq nice on the hub, for keys assigned to executor name: change their nice in its queue now, as the health check's rnice
    would on its next round (only that field, only those keys) → lines to print. Left as is there: keys not sent yet (staging
    sends the hub's nice), done, failed or bad ones (rnice skips them too); a running one keeps running, only its stored nice
    changes. Paused, ssh master down or queue unreadable: nothing is sent (no new connection), the health check follows later.
    dry: only the lines, with "would set"."""
    m = conf(name)
    L = m["label"]
    later = "the health check sets it there once it can read that queue"
    if M.paused(name) or master_down(m):
        return [f"{L}: not contacted (paused, or its ssh master connection is down); {later}"]
    r = rtq(name, "status --json --internal")
    q = S.json_in(r[1]) if r and r[0] == 0 else None
    if not isinstance(q, dict):
        return [f"{L}: cannot read its queue ({err_tail((r or [0, 'ssh unreachable'])[1], 150)}); {later}"]
    rt = {t["key"]: t for t in q.get("tasks") or []}
    lines, todo = [], []
    for k in keys:
        a = rt.get(k)
        if a is None:
            lines.append(f"{k} on {L}: not in its queue yet, staging sends it with nice {nice}")
        elif a["state"] in (proto.S_DONE, proto.S_FAILED, proto.S_BAD):
            lines.append(f"{k} on {L}: {a['state']} there, left at {a['nice']}")
        elif a["nice"] == nice:
            lines.append(f"{k} on {L}: already {nice}")
        else:
            run = " (running there: it keeps running, only the stored nice changes)" if a["state"] in (proto.S_CLAIMED, proto.S_BUSY) else ""
            lines.append(f"{'would set ' if dry else ''}{k} on {L}: {a['nice']} -> {nice}{run}")
            todo.append(k)
    if todo and not dry:
        r = set_nice(name, todo, nice)
        if not r or r[0]:
            return [ln for ln in lines if " -> " not in ln] + [
                f"could not change nice on {L} for {', '.join(todo)}: {err_tail((r or [0, 'ssh unreachable'])[1], 150)}; {later}"]
    return lines


def push_forget(name, keys):
    """taskq retry on the hub, for keys assigned to executor name: clear their failures in its queue now (taskq.py forget there), so
    a failed one is claimed again and resumes from its progress → lines to print. Paused or ssh master down: nothing is sent."""
    L = conf(name)["label"]
    if M.paused(name) or master_down(conf(name)):
        return [f"{L}: not contacted (paused, or its ssh master connection is down); run taskq retry again once it is reachable"]
    r = rtq(name, "forget -- " + " ".join(shlex.quote(k) for k in keys))
    if not r or r[0]:
        return [f"could not clear failures on {L} for {', '.join(keys)}: {err_tail((r or [0, 'ssh unreachable'])[1], 150)}"]
    return [f"{L}: {ln}" for ln in r[1].splitlines() if ln.strip()]


def push_stop(name, keys, why):
    """taskq rm, move or now on the hub, for keys assigned to executor name: stop them in its queue now (taskq.py stop there), as the
    health check's rstop would on its next round → lines to print. Paused or ssh master down: nothing is sent."""
    L = conf(name)["label"]
    later = "the health check stops it there on its next round"
    if M.paused(name) or master_down(conf(name)):
        return [f"{L}: not contacted (paused, or its ssh master connection is down); {later}"]
    r = rtq(name, f"stop --why {shlex.quote(why)} -- " + " ".join(shlex.quote(k) for k in keys))
    if not r or r[0]:
        return [f"could not stop {', '.join(keys)} on {L} now ({err_tail((r or [0, 'ssh unreachable'])[1], 150)}); {later}"]
    return [f"{L}: {ln}" for ln in r[1].splitlines() if ln.strip()]


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


def op(name, what, data=None, timeout=600, job=None, say=None, script="site_ops.py", bundle=None):
    """Run one site_ops.py operation in the bundle in use on that machine (data as JSON on stdin) → its result.
    Failure → RuntimeError; the bundle there has no such script or operation → OldBundle. Local machines (ssh=None) run the
    bundle in their own root too, same as remote ones.
    job: Slurm srun args (machines.py transfer_job) to run on a CPU node; no slot → NoSlot.
    say: if given, pass remote progress (stderr) to it line by line while running.
    script: another script under queue/ in the bundle with the same calling convention (migrate.py root).
    bundle: run in code/<bundle> instead of the bundle in use (prefix)."""
    m = conf(name)
    sh = cfg.bundle_rel("queue", script)
    check_op = ("import ast, sys\n"
                "p, op = sys.argv[1], 'op_' + sys.argv[2]\n"
                "t = ast.parse(open(p).read())\n"
                "sys.exit(0 if any(isinstance(n, ast.FunctionDef) and n.name == op for n in t.body) else 1)")
    guard = f"[ -f {shlex.quote(sh)} ]"
    if script == "site_ops.py":
        guard = f"{{ {guard} && python3 -c {shlex.quote(check_op)} {shlex.quote(sh)} {shlex.quote(what)}; }}"
    guard += f" || {{ basename \"$(readlink -f .)\"; exit {proto.EXIT['old_bundle']}; }}"
    cmd = f"{prefix(name, guard, bundle)} {py(name)} {sh} {what}"
    if job:
        cmd = f"srun -I{JOB_WAIT} {job} -J {SL.jobname(m.get('job_names'))} bash -c {shlex.quote(cmd)}"
        timeout += JOB_WAIT
    argv, inp = site_argv(name, cmd, timeout), json.dumps(data)
    rc, out, err = run_lines(argv, inp, timeout, say) if say else C.run(argv, None, timeout, input=inp)
    if rc == proto.EXIT["old_bundle"]:
        raise OldBundle(out.strip().splitlines()[-1] if out.strip() else "", script, what)
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
