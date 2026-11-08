#!/usr/bin/env python3
"""Build an env from <config dir>/specs/<name>.yml on a machine's CPU only (a Slurm CPU-partition srun, or a nice process
that cannot see the GPUs), then run <name>.check.py in it. Locks from envlock.py make the build offline from the hub's
wheelhouse (blobs on executors), otherwise it installs from the network. The env goes into <root>/<name>-<hash>/, with
<name> a symlink, and <name>.ok / .failed / .building / .log in the state dir; a new hash leaves the old env alone.
"""
import json
import os
import re
import shlex
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of this scheduler code
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg  # noqa: E402
REPO = cfg.REPO  # the user's repo root
cfg.use_config()
import proto  # noqa: E402
from proto import ENV_STATE as ES  # noqa: E402
from proto import ROOT  # noqa: E402
sys.path.insert(0, os.path.join(CORE, "monitor"))
import mtab as M  # noqa: E402

DEACT = 'if [[ -n ${VIRTUAL_ENV:-} ]]; then PATH=${PATH//"$VIRTUAL_ENV/bin:"/}; unset VIRTUAL_ENV; fi'
BUILD_MIN = 60      # time limit for one build over there (minutes): on Slurm the job's limit (short jobs schedule sooner; a build takes a few minutes), on pueue a timeout
BUILD_TIMEOUT = 4 * 3600  # how long the hub waits for one build at most
STALE_S = BUILD_TIMEOUT  # .building untouched this long counts as stopped
RETRY_S = 6 * 3600  # a build skipped for lack of space is retried after this long
SPACE_X = 2.5       # a built env is about this many times the lock's package bytes
NAME_RE = re.compile(r"[A-Za-z0-9._+!-]+")
SHA_RE = re.compile(r"[0-9a-f]{64}")


class EnvError(Exception):
    pass


def load(env):
    import yaml
    p = os.path.join(M.SPECS, f"{env}.yml")
    if not os.path.isfile(p):
        raise EnvError(f"no env spec {os.path.relpath(p, REPO)}")
    with open(p, encoding="utf-8") as f:
        sp = yaml.safe_load(f) or {}
    if not isinstance(sp, dict):
        raise EnvError(f"{env}.yml is not a table")
    bad = set(sp) - {"python", "channels", "override_channels", "conda", "base", "torch", "pip", "pip_no_deps"}
    if bad:
        raise EnvError(f"unknown fields in {env}.yml: {', '.join(sorted(bad))}")
    if sp.get("base") and (sp.get("python") or sp.get("conda") or sp.get("torch")):
        raise EnvError(f"{env}.yml: with base (a venv) there can be no python, conda or torch")
    if not sp.get("base") and not (sp.get("python") or sp.get("conda")):
        raise EnvError(f"{env}.yml: write base (a venv), or python / conda (a conda env)")
    return sp


def check_file(env):
    p = os.path.join(M.SPECS, f"{env}.check.py")
    if not os.path.isfile(p):
        raise EnvError(f"no check {os.path.relpath(p, REPO)}")
    return p


def variant(e):
    return e[1] if isinstance(e, (tuple, list)) and len(e) > 1 else "default"


def variant_of(name, env):
    """Which variant of env this machine uses: mtab.spec_variant for a "spec" entry; default for entries not yet "spec"
    (prebuild) and for unknown envs."""
    e = (M.machine(name).get("envs") or {}).get(env)
    return M.spec_variant(name, env) if M.is_spec(e) else "default"


def vhash(env, v):
    """Hash of the spec plus this variant's lock: what .ok records and states compare against."""
    return M.spec_hash(env, v)


def entry(name, env):
    e = (M.machine(name).get("envs") or {}).get(env)
    if e is None:
        raise EnvError(f"{name}'s envs has no {env}")
    return e


def sh_path(p):
    """A path starting with ~ → shell form "$HOME"/... (~ does not expand inside quotes)."""
    p = p.rstrip("/")
    if p == "~" or p.startswith("~/"):
        return '"$HOME"' + shlex.quote(p[1:]) if len(p) > 1 else '"$HOME"'
    return shlex.quote(p)


def state_dir(m):
    return m["root"].rstrip("/") + "/" + ROOT["envs_ok"]


def base_activation(name, base):
    """Activation of a venv's base env (used over there when building, written into .activate). A "spec" base uses its .activate."""
    a = M.activate(name, base)
    if a is None:
        raise EnvError(f"{name}'s envs has no base env {base}")
    return a


def own_base(name, env, v):
    """Should the venv env's base be the one built from its spec here (under env_root)? Yes unless the entry is "spec",
    the lock records no base, and the base entry is conda / venv (then use that entry)."""
    envs = M.machine(name).get("envs") or {}
    b = load(env).get("base")
    lk = load_lock(env, v)
    return not (M.is_spec(envs.get(env)) and not (lk and lk.get("base")) and b in envs and not M.is_spec(envs[b]))


def spec_activation(name, env):
    """Activation of an env built from its spec under env_root (ignores the machine entry: used for prebuilds and when
    the lock records a base)."""
    return "\n".join([DEACT, f"source {sh_path(M.env_root(name))}/{shlex.quote(env)}/.activate"])


def heredoc(tag, text):
    assert tag not in text
    return f"<<'{tag}'\n{text.rstrip(chr(10))}\n{tag}"


def pip_lines(pkgs):
    """Replace placeholders like {torch_mm} by shell variables (computed at build time from the env's torch and Python)."""
    return [p.replace("{torch_mm}", "${SPEC_TORCH_MM}").replace("{abi}", "${SPEC_ABI}").replace("{cp}", "${SPEC_CP}")
            for p in pkgs]


# ---------------- lock files and wheelhouse ----------------
def load_lock(env, v):
    """<name>.<variant>.lock.json → dict; none → None; wrong format → EnvError."""
    p = M.lock_path(env, v)
    if not os.path.isfile(p):
        return None
    fn = os.path.basename(p)
    try:
        lk = json.load(open(p, encoding="utf-8"))
    except ValueError as e:
        raise EnvError(f"cannot read {fn}: {e}") from None
    if not isinstance(lk, dict) or lk.get("format") != proto.LOCK_FORMAT or lk.get("name") != env or lk.get("variant") != v:
        raise EnvError(f"{fn}: format is not {proto.LOCK_FORMAT}, or name / variant do not match")
    for x in lock_items(lk):
        if not (SHA_RE.fullmatch(str(x.get("sha256"))) and NAME_RE.fullmatch(str(x.get("fn"))) and isinstance(x.get("size"), int)):
            raise EnvError(f"{fn}: bad sha256, fn or size for {x.get('name')}")
    for x in lk.get("conda") or []:
        if not re.fullmatch(r"[0-9a-f]{32}", str(x.get("md5"))):
            raise EnvError(f"{fn}: bad md5 for {x.get('name')}")
    for x in lk.get("pip") or []:
        if not (NAME_RE.fullmatch(str(x.get("name"))) and NAME_RE.fullmatch(str(x.get("version")))):
            raise EnvError(f"{fn}: bad name or version for pip package {x.get('name')}")
    if lk.get("conda") and lk.get("base"):
        raise EnvError(f"{fn}: an env with a base (venv) cannot have conda packages")
    return lk


def lock_items(lk):
    return list(lk.get("conda") or []) + list(lk.get("pip") or [])


def wheelhouse():
    return cfg.state(proto.WHEELHOUSE)


def lock_files(lk, wh=None):
    """The lock's packages → {sha256: path in the wheelhouse (proto.wheel)}; missing → EnvError. wh: another wheelhouse (tests)."""
    out = {x["sha256"]: os.path.join(wh, x["sha256"][:2], x["sha256"]) if wh else cfg.state(proto.wheel(x["sha256"])) for x in lock_items(lk)}
    miss = [x["fn"] for x in lock_items(lk) if not os.path.isfile(out[x["sha256"]])]
    if miss:
        raise EnvError(f"wheelhouse ({wh or wheelhouse()}) is missing {len(miss)} packages: {', '.join(miss[:5])}")
    return out


def lock_need(lk):
    """Bytes needed to build this lock: the built env (packages x SPACE_X) plus one temporary copy."""
    total = sum({x["sha256"]: x["size"] for x in lock_items(lk)}.values())
    return int(total * SPACE_X + total)


# ---------------- the build script ----------------
def wheel_name(x):
    """File name a lock wheel gets in the temporary dir: the version part becomes the metadata version (x["version"]).
    Some wheel file names carry a local version (mamba_ssm-2.3.0+cu12torch2.5cxx11abiFALSE-...) while the metadata says
    2.3.0, and pip refuses to install them when looking by file name; content and sha256 stay the same."""
    fn = x["fn"]
    if not fn.endswith(".whl"):
        return fn
    parts = fn[:-4].split("-")
    if len(parts) >= 5 and parts[1] != x["version"]:
        parts[1] = str(x["version"]).replace("-", "_")
    return "-".join(parts) + ".whl"


def build_script(name, env, v=None, store=None):
    """→ the bash that builds env on that machine (it carries the check and the marker files). v: variant (default from
    the table entry); store: where the packages are over there when there is a lock (<first two>/<sha256>: the
    executor's blobs, the hub's wheelhouse). Also builds entries not yet "spec" in the table (prebuild, sending is
    unaffected)."""
    m = M.machine(name)
    v = v or variant_of(name, env)
    sp, h = load(env), vhash(env, v)
    lk = load_lock(env, v)
    if lk and store is None:
        raise EnvError(f"{env} has a lock file, so the package dir is needed")
    chk = open(check_file(env), encoding="utf-8").read()
    root, sd = sh_path(M.env_root(name)), sh_path(state_dir(m))
    conda = sh_path(m["conda"][0]) if m.get("conda") else None
    L = ["#!/bin/bash", "set -euo pipefail", "export CUDA_VISIBLE_DEVICES=", f"ROOT={root}", f"SD={sd}",
         f"N={shlex.quote(env)}", f"H={h}", 'P="$ROOT/$N-${H:0:12}"', "T=", 'mkdir -p "$ROOT" "$SD"',
         'export CONDA_PKGS_DIRS="$ROOT/.pkgs" PIP_CACHE_DIR="$ROOT/.pip-cache"',
         'exec > >(tee -a "$SD/$N.log") 2>&1', f'echo "== $(date "+%F %T") building $N ({v}, $H) on $(hostname -s)"',
         'echo "$H $(date +%s)" > "$SD/$N.building"', 'rm -f "$SD/$N.failed"',
         "trap 'rc=$?; if [[ $rc != 0 ]]; then echo \"$H $(date +%s) rc $rc\" > \"$SD/$N.failed\"; fi; rm -f \"$SD/$N.building\"; "
         "if [[ -n $T ]]; then rm -rf \"$T\"; fi; exit $rc' EXIT"]
    if lk:
        rows = [f"pkgs {x['sha256']} {x['fn']} {x['md5']}" for x in lk.get("conda") or []] + \
               [f"wheels {x['sha256']} {wheel_name(x)} -" for x in lk.get("pip") or []]
        req = [f"{x['name']}=={x['version']} --hash=sha256:{x['sha256']}" for x in lk.get("pip") or []]
        L += [f"BL={sh_path(store)}", 'T="$ROOT/.tmp-$N-${H:0:12}-$$"',
              f'find "$ROOT" -maxdepth 1 -name ".tmp-$N-*" -mmin +{BUILD_MIN + 60} -exec rm -rf {{}} + 2>/dev/null || true',
              'mkdir -p "$T/pkgs" "$T/wheels"', 'echo "@EXPLICIT" > "$T/explicit.txt"',
              'export CONDA_PKGS_DIRS="$T/pkgs" CONDA_OFFLINE=1 PIP_NO_INDEX=1 PIP_CONFIG_FILE=/dev/null',
              "while read -r k h fn md5; do\n"
              '  s="$BL/${h:0:2}/$h"\n'
              '  if [[ ! -f $s ]]; then echo "missing package $fn ($s)"; exit 1; fi\n'
              '  ln -f "$s" "$T/$k/$fn" 2>/dev/null || ln -sf "$s" "$T/$k/$fn"\n'
              '  if [[ $k == pkgs ]]; then echo "file://$T/pkgs/$fn#$md5" >> "$T/explicit.txt"; fi\n'
              "done " + heredoc("FILES_EOF", "\n".join(rows) or "#"),
              'cat > "$T/req.txt" ' + heredoc("REQ_EOF", "\n".join(req)),
              'if [[ ! -e $P/.built && $(readlink "$ROOT/$N" || true) != "$(basename "$P")" ]]; then rm -rf "$P"; fi']
    if sp.get("base"):
        act = spec_activation(name, sp["base"]) if own_base(name, env, v) else base_activation(name, sp["base"])
        L += [act, 'if [[ ! -x $P/bin/python ]]; then python -m venv --system-site-packages "$P"; fi',
              f'printf \'%s\\n\' {shlex.quote(act)} "source \'$P/bin/activate\'" > "$P/.activate"']
    else:
        if conda is None:
            raise EnvError(f"{name}'s entry has no conda, cannot build a conda env")
        if lk:
            create = 'conda create --offline -y -p "$P" --file "$T/explicit.txt" < /dev/null'
        else:
            chans = " ".join(f"-c {shlex.quote(c)}" for c in sp.get("channels") or [])
            if sp.get("override_channels"):
                chans += " --override-channels"
            pk = ([f"python={sp['python']}"] if sp.get("python") else []) + list(sp.get("conda") or [])
            create = f'conda create -y -p "$P" {chans} {" ".join(shlex.quote(x) for x in pk)} < /dev/null'
        L += [DEACT, f"CONDA_SH={conda}/etc/profile.d/conda.sh", 'source "$CONDA_SH"',
              f"if [[ ! -d $P/conda-meta ]]; then {create}; fi",
              f'printf \'%s\\n\' {shlex.quote(DEACT)} "source \'$CONDA_SH\'" "conda activate \'$P\'" > "$P/.activate"']
    L += ['source "$P/.activate"']
    if lk and lk.get("pip"):
        L += ['python -m pip install --disable-pip-version-check --no-index --no-deps --require-hashes --no-build-isolation '
              '--find-links "$T/wheels" -r "$T/req.txt"']
    elif not lk:
        L += online_lines(env, sp, v)
    code = M.code(name)
    L += [f"python3 - {sh_path(code) if code else ''} {heredoc('CHECK_EOF', chk)}",
          'echo "$H" > "$P/.built"',
          'ln -sfn "$(basename "$P")" "$ROOT/.$N.new" && mv -T "$ROOT/.$N.new" "$ROOT/$N"',
          'echo "$H" > "$SD/$N.ok"', 'echo "== $(date "+%F %T") $N built and checked"']
    return "\n".join(L) + "\n"


def online_lines(env, sp, v):
    """The lines that install torch and pip packages from the network when there is no lock."""
    L = []
    tv = sp.get("torch") or {}
    if tv:
        t = tv.get(v)
        if not t:
            raise EnvError(f"{env}.yml's torch has no variant {v} (have: {', '.join(tv)})")
        L += [f"python -m pip install --index-url {shlex.quote(t['index_url'])} {' '.join(shlex.quote(x) for x in t['packages'])}"]
    if sp.get("pip") or sp.get("pip_no_deps"):
        L += ["eval \"$(python - <<'PY_EOF'\n"
              "import sys\n"
              "try:\n    import torch\n    v = torch.__version__.split('+')[0].split('.')\n"
              "    print(f'SPEC_TORCH_MM={v[0]}.{v[1]}'); print('SPEC_ABI=' + ('TRUE' if torch._C._GLIBCXX_USE_CXX11_ABI else 'FALSE'))\n"
              "except ImportError:\n    print('SPEC_TORCH_MM=; SPEC_ABI=')\n"
              "print(f'SPEC_CP=cp{sys.version_info[0]}{sys.version_info[1]}')\n"
              "PY_EOF\n)\""]
    if sp.get("pip"):
        L += [f'python -m pip install {" ".join(chr(34) + x + chr(34) for x in pip_lines(sp["pip"]))}']
    if sp.get("pip_no_deps"):
        L += [f'python -m pip install --no-deps {" ".join(chr(34) + x + chr(34) for x in pip_lines(sp["pip_no_deps"]))}']
    return L


def runner(name):
    """→ (the command to run on that machine, ssh name (None = this machine)). The command saves stdin to a temp file in
    the state dir before running it (conda and pip in the build must not read the rest of the script), and runs on the
    CPU only: Slurm uses the entry's env_job (else transfer_job; BUILD_MIN is added when it has no time limit) in a
    CPU-partition srun (queue and wait, no -I); anything else uses nice, sees no GPUs and stops after BUILD_MIN minutes."""
    m = M.machine(name)
    if m["scheduler"] == "slurm":
        job = m.get("env_job") or m.get("transfer_job")
        if not job:
            raise EnvError(f"{name}'s entry has no env_job (the CPU partition's srun options)")
        args = shlex.split(job)
        if any(a in ("--gres", "-G") or a.startswith(("--gres=", "--gpus", "-G")) or "gpu" in a.lower() for a in args):
            raise EnvError(f"{name}'s env_job asks for a GPU: {job}")
        timed = any(a in ("-t", "--time") or a.startswith("--time=") for a in args)
        # keep waiting in the queue (no -I: giving up and resubmitting resets the wait time and schedules worse); the hub waits at most BUILD_TIMEOUT
        launch = f"srun {job}" + ("" if timed else f" -t {BUILD_MIN}")
    else:
        launch = f"timeout -k 60 {BUILD_MIN}m nice -n 10 env CUDA_VISIBLE_DEVICES="
    sd = sh_path(state_dir(m))
    cmd = (f'mkdir -p {sd} && f=$(mktemp {sd}/.build.XXXXXX) && cat > "$f" && {{ {launch} bash "$f" < /dev/null; rc=$?; '
           f'rm -f "$f"; exit $rc; }}')
    return cmd, m.get("ssh")


def mark(name, env, h, mode, need=0, run_op=None):
    """Before a build (with a lock) run site_ops' env_mark on that machine → dict(ok, free, need): check the free space
    against what is needed (with a cap, and env_root under the root, from the cap minus what is used); enough writes
    .building, not enough writes .failed; clear removes the .building written this time. run_op replaces
    relay_remote.op (tests)."""
    import relay_remote as RM
    m = M.machine(name)
    cap = m.get("cap") if m.get("root") and M.env_root(name).startswith(m["root"].rstrip("/") + "/") else None
    data = dict(env_root=M.env_root(name), sd=state_dir(m), env=env, hash=h, mode=mode, need=int(need), cap=cap)
    try:
        return (run_op or RM.op)(name, "env_mark", data)
    except RuntimeError as e:
        raise EnvError(f"could not check the free space on {name}: {e}") from None


def build(name, env, dry=False, run=None, timeout=BUILD_TIMEOUT, v=None, say=print, run_op=None):
    """Build env on that machine → (exit code, the last lines of output). dry: return the command and script only. v:
    variant (default from the table entry). With a lock: check the space first (not enough → no build, write .failed,
    exit 1), and an executor first gets the packages into its blobs (pinned until the build ends; no CPU job slot →
    EXIT["stage_later"]). A machine with an ssh master that is down → EnvError (no fresh connection)."""
    import relay_remote as RM
    m = M.machine(name)
    v = v or variant_of(name, env)
    lk = load_lock(env, v)
    relay = M.is_exec(name)
    if lk and m.get("ssh") and not relay:
        raise EnvError(f"{env} has a lock file, so it only builds on the hub and on executors; {name} is neither")
    store = (RM.root(name) + "/" + ROOT["blobs"] if relay else wheelhouse()) if lk else None
    files = lock_files(lk) if lk else {}
    script = build_script(name, env, v, store)
    cmd, host = runner(name)
    if dry:
        pre = f"first put {len(files)} packages into {store}\n" if lk and relay else ""
        return 0, pre + f"run on {host or 'this machine'}: {cmd}\n" + script
    if RM.master_down(m):
        raise EnvError(f"the ssh master to {name} is down, not building this time")
    import common as C
    run = run or C.run
    h = vhash(env, v)

    def go():
        rc, out, err = run(cmd, host, timeout, input=script) if host else run(["bash", "-c", cmd], None, timeout, input=script)
        return rc, "\n".join((out + err).strip().splitlines()[-15:])
    if not lk:
        return go()
    r = mark(name, env, h, "reserve", lock_need(lk), run_op)
    if not r.get("ok"):
        return 1, f"{name} has no space, did not build {env}: needs {r['need'] / 1e9:.1f} GB, {r['free'] / 1e9:.1f} GB free"
    if not relay:
        return go()
    import relay_transfer as RT
    try:
        with RT.pinned(name, f"env.{env}", files, say):
            RT.ensure_blobs(name, {h: [None, os.path.getsize(p), p] for h, p in files.items()}, say, src=M.hub(), what=f"{name}'s env {env}")
            return go()
    except RM.NoSlot as e:
        mark(name, env, h, "clear", run_op=run_op)
        return proto.EXIT["stage_later"], f"{e}; not building this time, will retry on the next check"
    except (RT.RN.RelayError, RuntimeError, OSError) as e:
        mark(name, env, h, "clear", run_op=run_op)
        raise EnvError(f"packages were not put on {name}: {e}") from None


# ---------------- states ----------------
STATUS_SH = r'''for n in "$@"; do
  ok=$(head -n1 "$SD/$n.ok" 2>/dev/null | tr -d '|' || true); fl=$(head -n1 "$SD/$n.failed" 2>/dev/null | tr -d '|' || true)
  bd=$(head -n1 "$SD/$n.building" 2>/dev/null | tr -d '|' || true); ln=0; [[ -e $ROOT/$n/.activate ]] && ln=1
  lg=; [[ -n $fl ]] && lg=$(tail -c 2000 "$SD/$n.log" 2>/dev/null | base64 -w0 || true)
  echo "ENV|$n|$ok|$fl|$bd|$ln|$lg"
done
echo "NOW|$(date +%s)"
'''


def rows_of(txt):
    """STATUS_SH's output → ({env: dict(ok, failed, building, active, log)}, the time over there)."""
    import base64
    now, rows = time.time(), {}
    for ln in txt.splitlines():
        p = ln.split("|")
        if p[0] == "NOW" and len(p) > 1 and p[1].isdigit():
            now = int(p[1])
        elif p[0] == "ENV" and len(p) in (6, 7):
            lg = ""
            if len(p) == 7 and p[6]:
                try:
                    lg = base64.b64decode(p[6]).decode(errors="replace")
                except ValueError:
                    pass
            rows[p[1]] = dict(ok=p[2], failed=p[3], building=p[4], active=p[5] == "1", log=lg)
    return rows, now


def classify(rows, want, now):
    """{env: the remote marker files} + {env: variant} → {env: state} (values in proto.ENV_STATE)."""
    out = {}
    for k, v in want.items():
        r = rows.get(k) or {}
        h, bd, fl = vhash(k, v), (r.get("building") or "").split(), (r.get("failed") or "").split()
        if bd:
            age = now - int(bd[1]) if len(bd) > 1 and bd[1].isdigit() else 0
            out[k] = ES["building"] if age < STALE_S else ES["stopped"]
        elif (r.get("ok") or "").strip() == h and r.get("active"):
            out[k] = ES["ok"]
        elif fl and fl[0] == h:
            out[k] = ES["nospace"] if len(fl) > 2 and fl[2] == "nospace" else ES["failed"]
        elif r.get("ok"):
            out[k] = ES["changed"]
        else:
            out[k] = ES["missing"]
    return out


def read_rows(name, envs_, run=None):
    """→ (the remote marker files, the time over there); unreachable (or the ssh master down) → None."""
    m = M.machine(name)
    if not envs_:
        return {}, time.time()
    import relay_remote as RM
    if run is None and RM.master_down(m):
        return None
    import common as C
    run = run or C.run
    sh = f"ROOT={sh_path(M.env_root(name))}; SD={sh_path(state_dir(m))}\n" + STATUS_SH
    cmd = f"bash -s -- {' '.join(shlex.quote(s) for s in envs_)}"
    rc, txt, _ = run(cmd, m.get("ssh"), 120, input=sh) if m.get("ssh") else run(["bash", "-s", "--", *envs_], None, 120, input=sh)
    return None if rc else rows_of(txt)


def states(name, run=None):
    """→ {env: state}, read only. conda / venv entries: not spec-built. Unreachable → None."""
    m = M.machine(name)
    envs = m.get("envs") or {}
    out = {k: ES["not_spec"] for k, e in envs.items() if not M.is_spec(e)}
    want = {k: M.spec_variant(name, k) for k, e in envs.items() if M.is_spec(e)}
    if not want:
        return out
    got = read_rows(name, list(want), run)
    if got is None:
        return None
    out.update(classify(got[0], want, got[1]))
    return out


def oks_of(states_, name):
    """name's states() → ready_envs' {env: hash} (only ok ones)."""
    return {k: M.spec_hash(k, M.spec_variant(name, k)) for k, v in (states_ or {}).items() if v == ES["ok"]}


# ---------------- automatic builds (the hub's health check calls this through relay.py) ----------------
def wanted(name, extra=()):
    """{env: variant} to build on this machine (base envs first): the entries' "spec" envs plus extra (prebuild: env name
    or (env, variant)); a venv's base also counts when it has a spec. Envs without a spec do not."""
    out = {}
    pre = [(x, variant_of(name, x)) if isinstance(x, str) else (x[0], x[1] if len(x) > 1 else "default") for x in extra]
    todo = [(k, M.spec_variant(name, k)) for k, e in (M.machine(name).get("envs") or {}).items() if M.is_spec(e)] + pre

    def add(k, v, depth=0):
        if k in out or depth > 5 or M.spec_hash(k) is None:
            return
        try:
            b = load(k).get("base")
            if b and own_base(name, k, v):
                add(b, variant_of(name, b), depth + 1)
        except Exception:  # noqa: BLE001  a bad spec or lock: problems reports it
            pass
        out[k] = v
    for k, v in todo:
        add(k, v)
    return out


def next_build(want, st, rows, now, skip=()):
    """→ the next (env, variant) to build; none → None. One build per machine at a time: none while one is building;
    wait until the base env is ok; a failed check is not rebuilt automatically (only a spec change, hence a new hash,
    does that); a build skipped for lack of space is retried after RETRY_S; skip (cannot build, e.g. wheelhouse missing
    packages) is not built."""
    if any(s == ES["building"] for s in st.values()):
        return None
    for k, v in want.items():
        s = st.get(k)
        if s in (ES["ok"], ES["failed"]) or k in skip:
            continue
        if s == ES["nospace"]:
            fl = ((rows.get(k) or {}).get("failed") or "").split()
            if len(fl) > 1 and fl[1].isdigit() and now - int(fl[1]) < RETRY_S:
                continue
        try:
            b = load(k).get("base")
        except Exception:  # noqa: BLE001
            continue
        if b in want and st.get(b) != ES["ok"]:
            continue
        return k, v
    return None


def problems(name, want, st, rows):
    """→ [(env, explanation)]: check failed (with the last lines of the log), no space, wheelhouse missing packages,
    bad lock file."""
    out = []
    for k, v in want.items():
        r = rows.get(k) or {}
        if st.get(k) == ES["failed"]:
            tail = "\n".join((r.get("log") or "").strip().splitlines()[-8:])
            out.append((k, f"building env {k} ({v}) on {name} failed: {(r.get('failed') or '').strip()}" + (f"\n{tail}" if tail else "")))
        elif st.get(k) == ES["nospace"]:
            fl = (r.get("failed") or "").split()
            gb = lambda x: f"{int(x) / 1e9:.1f} GB" if x.isdigit() else x  # noqa: E731
            out.append((k, f"{name} has no space, did not build env {k} ({v})" + (f": needs {gb(fl[3])}, {gb(fl[4])} free" if len(fl) > 4 else "")))
        if st.get(k) != ES["ok"]:
            try:
                load(k)
                lk = load_lock(k, v)
                if lk:
                    lock_files(lk)
            except Exception as e:  # noqa: BLE001
                out.append((k, f"cannot build env {k} ({v}) on {name}: {e}"))
    return out
