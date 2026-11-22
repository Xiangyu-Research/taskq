#!/usr/bin/env python3
"""Tests for the resident agent (sync/agent.py) and its hub side (sync/agent_box.py, xssh): the file server on 127.0.0.1, an agent
installed under a temp HOME through a fake ssh, commands through xssh. Run on the hub."""
import atexit
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(CORE, "queue", "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, HERE)
import agent as A  # noqa: E402

T = tempfile.mkdtemp(prefix="test_agent_")
os.chmod(T, 0o700)
HOME = os.path.join(T, "home")
os.makedirs(HOME)
SRV = os.path.join(T, "tunnel_srv")
MB = os.path.join(SRV, "agent", "site-pueue")
XSSH = os.path.join(HERE, "xssh")
HOST = "fake-pueue-host"
FAKE = os.path.join(T, "fake_ssh")
with open(FAKE, "w") as f:  # drop ssh options, then the host; run the rest in HOME (marks that the real ssh was used)
    f.write('#!/bin/bash\nwhile [[ $1 == -* ]]; do [[ $1 == -o ]] && shift; shift; done\nshift\ntouch "$HOME/used_ssh"\n'
            'cd "$HOME" && exec bash -c "$*"\n')
os.chmod(FAKE, 0o755)
with open(os.path.join(T, "sitecustomize.py"), "w") as f:  # site-pueue goes through its agent in child processes
    f.write(f"import sys\nsys.path.insert(0, {fixture.CONFIG!r})\nimport machines\nmachines.MACHINES['site-pueue']['agent'] = True\n")
PORT = fixture.free_port(32000, 32700)
CODE = os.path.join(T, "agent.py")  # the agent.py the hub serves (a copy: the update test changes it)
shutil.copy(os.path.join(HERE, "agent.py"), CODE)
ENV = dict(os.environ, HOME=HOME, PYTHONPATH=T, TUNNEL_LOCK=os.path.join(T, "lock"), TUNNEL_BIND=f"127.0.0.1:{PORT}",
           TASKQ_AGENT_URL=f"http://127.0.0.1:{PORT}", TASKQ_REAL_SSH=FAKE, TASKQ_AGENT_HOME=HOME, TASKQ_AGENT_CODE=CODE, TASKQ_AGENT_GONE="3",
           TASKQ_AGENT_ALIVE="1", TASKQ_AGENT_WAIT="5", TASKQ_AGENT_CLAIM="4", TASKQ_AGENT_STALE="8", TUNNEL_IDLE="3")
FAILS = []


def pid_of(path):
    try:
        return int(open(path).read().split()[0])
    except (OSError, ValueError, IndexError):
        return None


def cleanup():  # stop the agent and the server this test started (only these pids), remove the temp dir
    for p in (os.path.join(HOME, ".local/state/taskq/agent-site-pueue.lock"), os.path.join(SRV, "serve.pid")):
        pid = pid_of(p)
        if pid:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    shutil.rmtree(T, ignore_errors=True)


atexit.register(cleanup)


def ok(name, cond, detail=""):
    print(("PASS:" if cond else "FAIL:") + name + ("" if cond else f" {detail}"))
    if not cond:
        FAILS.append(name)


def x(*args, inp=None, timeout=60, binary=False):
    return subprocess.run([XSSH, "-o", "BatchMode=yes", *args], input=inp, capture_output=True, text=not binary, env=ENV, timeout=timeout)


def box(*args):
    return subprocess.run([sys.executable, os.path.join(HERE, "agent_box.py"), *args], capture_output=True, text=True, env=ENV, timeout=120)


def remote_procs(word):
    out = subprocess.run(["ps", "-eo", "pid,args"], capture_output=True, text=True).stdout
    return [ln for ln in out.splitlines() if ln.split(None, 1)[-1].strip() == f"sleep {word}"]


def signed(method, path, body=b"", key=None, ts=None, nonce=None):
    key = key or open(os.path.join(MB, "key")).read().strip()
    ts = str(int(time.time())) if ts is None else ts
    nonce = nonce or os.urandom(16).hex()
    full = "/agent/site-pueue" + path
    h = {"User-Agent": A.UA, "X-Agent-Ts": ts, "X-Agent-Nonce": nonce,
         "X-Agent-Sig": A.sign(key, "req", "site-pueue", ts, nonce, method, full, A.sha256(body))}
    req = urllib.request.Request(f"http://127.0.0.1:{PORT}{full}", data=body if method == "POST" else None, headers=h, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, nonce
    except urllib.error.HTTPError as e:
        return e.code, nonce


def main():
    # before install: no agent, so site-pueue (agent=True) has no mailbox -> 255 at once, without falling back to ssh
    r = box("status")
    ok("status before install lists the enabled machine as not installed", "not installed" in r.stdout, r.stdout + r.stderr)

    r = box("install", "site-pueue", "--no-cron")
    ok("install starts the agent and sees it call", r.returncode == 0 and "calling the hub" in r.stdout, r.stdout + r.stderr)
    ok("install puts the agent and its private config in HOME", os.path.isfile(os.path.join(HOME, ".local/share/taskq/agent.py")) and
       os.stat(os.path.join(HOME, ".config/taskq/agent-site-pueue.json")).st_mode & 0o077 == 0)
    os.unlink(os.path.join(HOME, "used_ssh"))  # install itself uses ssh (the bootstrap); nothing after it may

    r = x(HOST, "echo out; echo err >&2; exit 7")
    ok("command runs through the agent: stdout, stderr, exit code", (r.stdout, r.stderr, r.returncode) == ("out\n", "err\n", 7), repr(r))
    ok("the real ssh is not used for an agent machine", not os.path.exists(os.path.join(HOME, "used_ssh")))

    r = x(HOST, "pwd; echo $USER; echo $BASH_ENV")
    lines = r.stdout.splitlines()
    ok("command runs in the home dir, with USER, without BASH_ENV leaking", lines[:1] == [HOME] and lines[1:2] != [""] and lines[2:] == [""], r.stdout)

    big = os.urandom(20 << 20)
    r = x(HOST, "sha256sum | cut -c1-64", inp=big, binary=True)
    ok("20 MiB stdin arrives intact", r.stdout.decode().strip() == hashlib.sha256(big).hexdigest(), r.stderr)
    f = os.path.join(T, "big")
    open(f, "wb").write(big)
    r = x(HOST, f"cat {f}", binary=True)
    ok("20 MiB stdout arrives intact", r.stdout == big and r.returncode == 0)

    r = x("-n", HOST, "cat; echo done", inp="ignored")
    ok("-n: no stdin", r.stdout == "done\n", repr(r.stdout))

    src, dst = os.path.join(T, "src"), os.path.join(T, "dst")
    os.makedirs(os.path.join(src, "a"))
    open(os.path.join(src, "a", "f1"), "wb").write(os.urandom(3 << 20))
    open(os.path.join(src, "f2"), "w").write("x\n")
    r = subprocess.run(["rsync", "-a", "-e", XSSH, src + "/", f"{HOST}:{dst}/"], capture_output=True, text=True, env=ENV, timeout=60)
    same = r.returncode == 0 and subprocess.run(["diff", "-r", src, dst]).returncode == 0
    ok("rsync -e xssh works through the agent (both directions at once)", same, r.stderr)

    ok("-O check answers for the agent", x("-O", "check", HOST).returncode == 0)

    # signatures: wrong key, replayed nonce, old timestamp are refused; a good one passes
    st, n = signed("POST", "/alive", b"[]")
    ok("signed request accepted", st == 200, st)
    ok("replayed nonce refused", signed("POST", "/alive", b"[]", nonce=n)[0] == 403)
    ok("wrong key refused", signed("POST", "/alive", b"[]", key="0" * 64)[0] == 403)
    ok("old timestamp refused", signed("POST", "/alive", b"[]", ts=str(int(time.time()) - 1000))[0] == 403)

    # a command with a bad hub signature is not run
    key = open(os.path.join(MB, "key")).read().strip()
    cid = os.urandom(12).hex()
    mark = os.path.join(T, "forged")
    for ext in (".alive", ".in", ".eof"):
        open(os.path.join(MB, "run", cid + ext), "w").close()
    spec = dict(id=cid, cmd=f"touch {mark}", ts=str(int(time.time())), sig=A.sign("0" * 64, "cmd", "site-pueue", cid, "0", "x"))
    json.dump(spec, open(os.path.join(MB, "inbox", cid + ".json"), "w"))
    time.sleep(3)
    ok("a command without the hub's signature is refused", not os.path.exists(mark))
    del key

    # the caller killed (SIGKILL, no chance to cancel): the command is stopped once its .alive goes stale
    p = subprocess.Popen([XSSH, HOST, "sleep 3171"], env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.5)
    ok("long command running remotely", bool(remote_procs(3171)))
    p.kill()
    p.wait()
    end = time.time() + 20
    while remote_procs(3171) and time.time() < end:
        time.sleep(0.5)
    ok("killed caller -> remote command stopped", not remote_procs(3171))

    # subprocess timeout on the caller side (SIGTERM path through a shell's timeout)
    r = subprocess.run(["timeout", "2", XSSH, HOST, "sleep 3172"], env=ENV, capture_output=True)
    end = time.time() + 20
    while remote_procs(3172) and time.time() < end:
        time.sleep(0.5)
    ok("timed-out caller -> remote command stopped", r.returncode in (124, 255) and not remote_procs(3172), r.returncode)

    # self-update: the hub's agent.py changes -> the agent fetches, checks and restarts on it
    pid0 = pid_of(os.path.join(HOME, ".local/state/taskq/agent-site-pueue.lock"))
    open(CODE, "ab").write(b"\n# test_agent update marker\n")
    want = hashlib.sha256(open(CODE, "rb").read()).hexdigest()
    end = time.time() + 30
    info = {}
    while time.time() < end:
        try:
            info = json.load(open(os.path.join(MB, "info.json")))
        except (OSError, ValueError):
            pass
        if info.get("sha") == want:
            break
        time.sleep(0.5)
    ok("agent updates itself to the hub's agent.py", info.get("sha") == want, info)
    ok("updated agent kept its lock (same pid after exec)", pid_of(os.path.join(HOME, ".local/state/taskq/agent-site-pueue.lock")) == pid0)
    r = x(HOST, "echo after-update")
    ok("commands still run after the update", r.stdout == "after-update\n", repr(r))

    r = box("status", "--json")
    rows = json.loads(r.stdout or "[]")
    ok("status --json: up, enabled, installed", rows and rows[0]["up"] and rows[0]["enabled"] and rows[0]["installed"], r.stdout)

    # agent stopped: once its heartbeat is stale, xssh fails at once with 255 (like ssh that cannot connect)
    subprocess.run([sys.executable, os.path.join(HOME, ".local/share/taskq/agent.py"), "stop", "site-pueue"], env=ENV, timeout=30)
    os.utime(os.path.join(MB, "seen"), (time.time() - 100, time.time() - 100))
    t0 = time.time()
    r = x(HOST, "echo should-not-run", timeout=120)
    ok("agent down -> 255 quickly", r.returncode == 255 and "not seen" in r.stderr and time.time() - t0 < 30,
       (r.returncode, r.stderr, time.time() - t0))
    ok("-O check fails while the agent is down", x("-O", "check", HOST).returncode == 255)
    r = box("start", "site-pueue")
    t0 = time.time()
    r = x(HOST, "echo back")
    ok("start brings it back", r.stdout == "back\n", repr(r))

    # machines without agent=True still go through the real ssh
    r = x("fake-slurm-login", "echo via-ssh")
    ok("a machine without agent=True uses the real ssh", r.stdout == "via-ssh\n" and os.path.exists(os.path.join(HOME, "used_ssh")), repr(r))

    # relay_remote.site_argv routes the hub's own machine through its agent with --machine when agent=True
    sys.path.insert(0, os.path.join(CORE, "queue"))
    import relay_remote as RM
    RM.M.MACHINES["hub"]["agent"] = True
    argv = RM.site_argv("hub", "true")
    ok("site_argv: hub machine with agent=True -> xssh --machine", argv[:3] == [RM.C.XSSH, "--machine", "hub"], argv)

    # store operations run from the hub machine's bundle (TASKQ_ROOT set) use the same file server dir as the hub's commands
    probe = "import sys; sys.path.insert(0, sys.argv[1]); import tunnel; print(tunnel.srv_dir(), tunnel.lock_path())"
    env = {k: v for k, v in ENV.items() if k not in ("TUNNEL_LOCK", "TASKQ_ROOT", "TASKQ_STATE")}
    st = os.path.join(T, "acct_state")
    os.makedirs(st, mode=0o700, exist_ok=True)
    dirs = [subprocess.run([sys.executable, "-c", probe, os.path.join(CORE, "sync")], capture_output=True, text=True,
                           env=dict(env, TASKQ_STATE=st, **extra)) for extra in ({}, {"TASKQ_ROOT": os.path.join(T, "someroot")})]
    ok("file server dir and transfer locks do not depend on TASKQ_ROOT",
       dirs[0].returncode == 0 and dirs[0].stdout == dirs[1].stdout and dirs[0].stdout.startswith(st), [r.stdout + r.stderr for r in dirs])

    print(f"\n{len(FAILS)} failed" if FAILS else "\nall passed")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
