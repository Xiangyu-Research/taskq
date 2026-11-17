#!/usr/bin/env python3
"""Real ssh instead of a fake one: the commands the core sends over ssh, through a real sshd that logs into this same machine.
Skipped (SKIP line, exit 77) unless TQTEST_SSH_HOST names one: an ssh destination (an alias in ~/.ssh/config works; ssh reads the
real user's config even under the test's temp HOME) that reaches this machine as this user with key auth and no prompt, such as a
local sshd on 127.0.0.1 (CI's ssh job starts one). Set but unusable (unreachable, asks for a password, another machine) -> fail.

  common.run       quoting ($, quotes, newlines; ~ expanded on the other side), stdin, exit codes, ssh failure 255, the remote
                   timeout ends a stuck command over there by itself
  health.run_on    a script on stdin through bash -s
  relay_remote     rsh; op (site_ops.py of a hand-built bundle under a temp root: probe; an operation it lacks -> OldBundle);
                   rtq (taskq.py add --spec-stdin and status --json in that bundle, the way the hub sends tasks)
  tunnel.py rsync  plain rsync over ssh for a host without a tunnel, both directions, names with spaces (needs rsync)

Uses the production ssh command lines (common.SSH; RELAY_SSH, TUNNEL_SSH and RSYNC_RSH unset), not test options. Writes only
temp dirs on this machine (also reached through ssh); kills nothing.
    TQTEST_SSH_HOST=<destination> python3 queue/test_real_ssh.py
"""
import hashlib
import json
import os
import secrets
import shlex
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)
HOST = os.environ.get("TQTEST_SSH_HOST", "").strip()
if not HOST:
    print("SKIP:no real sshd to test against (TQTEST_SSH_HOST unset)")
    sys.exit(77)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
# fixture.use() puts noremote/ (ssh that always fails) before the real ssh; this test is the one place that wants the real one
os.environ["PATH"] = os.pathsep.join(p for p in os.environ["PATH"].split(os.pathsep) if p != fixture.NOREMOTE)
for v in ("RELAY_SSH", "TUNNEL_SSH", "RSYNC_RSH"):
    os.environ.pop(v, None)
sys.path.insert(0, HERE)
import cfg  # noqa: E402

cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
sys.path.insert(0, os.path.join(CORE, "monitor"))
import common as C  # noqa: E402
import proto  # noqa: E402
import relay_remote as RM  # noqa: E402
import remote as R  # noqa: E402
import spec as S  # noqa: E402

T = fixture.tmpdir(prefix="test_real_ssh_")
fails = 0


def ok(what, cond):
    global fails
    print(("PASS:" if cond else "FAIL:") + what)
    fails += 0 if cond else 1


# ---------------- the destination is this machine ----------------
mark = os.path.join(T, "this machine")
token = secrets.token_hex(16)
with open(mark, "w") as f:
    f.write(token)
rc, out, err = C.run(f"cat {shlex.quote(mark)}", HOST, timeout=60)
if rc != 0 or out != token:
    print(f"FAIL:TQTEST_SSH_HOST={HOST} must log into this machine as this user without a prompt "
          f"(exit {rc}: {(err or out).strip()[-300:]})")
    sys.exit(1)
ok(f"{HOST} reaches this machine (reads a file just written here) with the production ssh options {C.SSH[1:]}", True)

# ---------------- common.run ----------------
tricky = "x='a b'; printf '%s|' \"$x\" '$HOME' \"it's\" \"$((6*7))\"\necho\necho line2"
lo, rem = C.run(tricky), C.run(tricky, HOST)
ok("common.run: quotes, $ and newlines come out the same as running locally", rem[0] == 0 and rem[1] == lo[1] == "a b|$HOME|it's|42|\nline2\n")
rc, home, _ = C.run('printf %s "$HOME"', HOST)
rc2, out, _ = C.run("printf %s " + R.q("~/a b"), HOST)
ok("common.run: a leading ~ (remote.q) is expanded by the other side, the rest stays quoted", rc == rc2 == 0 and home.startswith("/")
   and out == home + "/a b")
data = "".join(f"line {i} ü\n" for i in range(20000))
rc, out, _ = C.run("sha256sum", HOST, input=data)
ok("common.run: stdin reaches the other side whole", rc == 0 and out.split()[0] == hashlib.sha256(data.encode()).hexdigest())
ok("common.run: exit codes pass through", [C.run(f"exit {n}", HOST)[0] for n in (0, 1, 7)] == [0, 1, 7])
ok("common.run: an ssh destination that does not resolve -> 255", C.run("true", "taskq-no-such-host.invalid", timeout=60)[0] == 255)
nap = f"sleep 300.{os.getpid()}"  # a command line only this test runs
t0 = time.time()
rc, out, _ = C.run(f"{nap}; echo late", HOST, timeout=25)
took = time.time() - t0
left = subprocess.run(["pgrep", "-f", "-x", nap], capture_output=True, text=True).stdout.split()
ok(f"common.run: the remote timeout ends a stuck command over there ({int(took)} s, exit 124), nothing left running",
   rc == proto.EXIT["timeout"] and "late" not in out and took < 24 and not left)

# ---------------- health.run_on ----------------
import health as H  # noqa: E402

rc, out = H.run_on({"ssh": HOST}, 'a=$(echo "hi there")\necho "A|$a"\necho "B|$((6*7))"\n', timeout=60)
ok("health.run_on: a script on stdin through bash -s", rc == 0 and out == "A|hi there\nB|42\n")

# ---------------- relay_remote: a hand-built bundle under a temp root, reached over ssh ----------------
NAME = next(n for n, m in RM.M.MACHINES.items() if m.get("scheduler") == "pueue")
ROOT = os.path.join(T, "root dir")  # a space in the root: quoting in prefix() and site_argv()
conda = fixture.store_bundle(ROOT)
real_conf = RM.conf
RM.conf = lambda n: dict(real_conf(n), ssh=HOST, root=ROOT, conda=(conda, "base0"), ssh_master=False) if n == NAME else real_conf(n)
rc, out, err = RM.rsh(NAME, 'printf "%s|%s" "$(pwd)" "$PATH"', input="")
ok("relay_remote.rsh: runs over ssh in the login dir", rc == 0 and out.split("|")[0] == home)
DATA = os.path.join(T, "data")
for p in ("a/1.txt", "a/x y.txt", "b/2.txt"):
    os.makedirs(os.path.dirname(os.path.join(DATA, p)), exist_ok=True)
    with open(os.path.join(DATA, p), "w") as f:
        f.write("12345")
res = RM.op(NAME, "probe", dict(pats=["a/*.txt", "none/*"], data=DATA, size=True), timeout=120)
ok("relay_remote.op probe: site_ops.py in the bundle under the root, arguments as JSON on stdin, result read back",
   res["a/*.txt"]["n"] == 2 and res["a/*.txt"]["size"] > 0 and res["a/*.txt"]["newest"] and res["none/*"]["n"] == 0)
try:
    RM.op(NAME, "no_such_operation", {}, timeout=120)
    got = None
except RM.OldBundle as e:
    got = e
ok("relay_remote.op: an operation the bundle there lacks -> OldBundle", got is not None)
for d in ("queue", "data"):  # an executor root as setup leaves it
    os.makedirs(os.path.join(ROOT, proto.ROOT[d]))
open(os.path.join(ROOT, proto.ROOT["queue"], "tasks.jsonl"), "w").close()
sp = S.sendable(S.resolve("command", dict(key="ssh/a", cmd="echo 'hi there' > out.txt", env="base", out="out/ssh", done="out.txt",
                                          device="cpu")))
r1 = RM.rtq(NAME, "add --nice 0 --spec-stdin", input=json.dumps(sp))
r2 = RM.rtq(NAME, "status --json")
st = S.json_in(r2[1]) if r2 else None
ok("relay_remote.rtq: taskq.py add --spec-stdin over ssh (spec on stdin), status --json reads it back",
   r1 and r1[0] == 0 and r2 and r2[0] == 0 and st and [t["key"] for t in st["tasks"]] == ["ssh/a"]
   and st["tasks"][0]["spec"]["cmd"] == sp["cmd"])

# ---------------- tunnel.py rsync: plain rsync over ssh ----------------
if not shutil.which("rsync"):
    print("SKIP:no rsync on PATH: rsync over ssh not checked")
else:
    src, back = os.path.join(T, "src dir"), os.path.join(T, "back")
    files = {"a/b c.bin": os.urandom(300_000), "top.txt": b"top\n", "d e/f.txt": b"x" * 10}
    for p, b in files.items():
        os.makedirs(os.path.dirname(os.path.join(src, p)), exist_ok=True)
        with open(os.path.join(src, p), "wb") as f:
            f.write(b)
    dst = os.path.join(T, "dst dir")
    tun = [sys.executable, os.path.join(CORE, "sync", "tunnel.py"), "rsync", "-rlt", "--partial", "-s"]  # -s: names with spaces as they are
    r = subprocess.run(tun + [src + "/", f"{HOST}:{dst}/"], capture_output=True, text=True, timeout=300)
    r2 = subprocess.run(tun + [f"{HOST}:{dst}/", back + "/"], capture_output=True, text=True, timeout=300)

    def same(d):
        try:
            return all(open(os.path.join(d, p), "rb").read() == b for p, b in files.items())
        except OSError:
            return False
    ok("tunnel.py rsync, host without a tunnel: plain rsync over ssh there and back, names with spaces",
       r.returncode == 0 and r2.returncode == 0 and same(dst) and same(back) and "tunnel transfer" not in r.stdout + r2.stdout)

sys.exit(1 if fails else 0)
