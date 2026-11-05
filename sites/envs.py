#!/usr/bin/env python3
"""按 envs/specs/<名>.yml 在一台机器上建环境（2026-10-31）：box 生成一段 bash，交给那台机器在 CPU 上跑（Slurm：CPU 分区的
srun；pueue 类：nice、看不到卡的普通进程），不占 GPU。

描述的写法（envs/specs/README.md）：
  python / channels / override_channels / conda   conda 环境：conda create -p <目录> python=… <conda 包>
  base                                            venv 建在另一个环境上（--system-site-packages），先激活那个
  torch: {变体: {index_url, packages}}            先从 index_url 装 torch（default，或机器条目 ("spec", 变体) 选的）
  pip / pip_no_deps                               再装的 pip 包；{torch_mm}、{abi}、{cp} 按环境里的 torch、Python 填
检查：envs/specs/<名>.check.py（在建好的环境里跑，参数是那台的代码目录）。

那边的布局（root = 机器条目的 env_root）：<root>/<名>-<哈希前 12 位>/ 是这一版环境，<root>/<名> 是指向它的软链接（检查过了才换），
<root>/<名>/.activate 是激活语句；状态目录里 <名>.ok、<名>.failed、<名>.building、<名>.log。
哈希是描述加这台用的变体的锁（mtab.spec_hash、spec_variant）。
描述一改哈希就变：新版建在新目录里，建好检查过才换软链接、写 .ok，旧版留着，在跑的任务不受影响。
.ok 的内容是哈希（一行）；.failed 是"<哈希> <时间> rc <退出码>"或"<哈希> <时间> nospace <要的字节> <剩的字节>"。

有锁文件 <名>.<变体>.lock.json（11/5，sites/envlock.py 写）时不联网建：锁里的 conda 包、pip 包按 sha256 存在 box 的
wheelhouse（状态文件夹下的 proto.wheel(sha256)）；执行机先经 relay 的 ensure_blobs 放进根目录的 blobs（建的时候钉住，清理不删），
那边硬链接成原来的文件名，conda create --offline --file（@EXPLICIT）、pip install --no-index --require-hashes；box 直接用 wheelhouse。
建之前按锁里的大小查空间（包 × SPACE_X，加一份临时的；有 cap 的执行机按 cap 减已占的算），不够不建、写 .failed。
"""
import json
import os
import re
import shlex
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
cfg.use_config()
import proto  # noqa: E402
from proto import ROOT  # noqa: E402
sys.path.insert(0, os.path.join(CORE, "monitor"))
import mtab as M  # noqa: E402

DEACT = 'if [[ -n ${VIRTUAL_ENV:-} ]]; then PATH=${PATH//"$VIRTUAL_ENV/bin:"/}; unset VIRTUAL_ENV; fi'
BUILD_MIN = 180     # 那边建一次的时限（分钟）：Slurm 是作业时限，pueue 类是 timeout
BUILD_TIMEOUT = 4 * 3600  # box 上等一次建环境最多这么久
STALE_S = BUILD_TIMEOUT  # .building 这么久没动就当已经停了
RETRY_S = 6 * 3600  # 空间不够没建的，这么久以后自动再试
SPACE_X = 2.5       # 装好的环境约为锁里的包的几倍
NAME_RE = re.compile(r"[A-Za-z0-9._+!-]+")
SHA_RE = re.compile(r"[0-9a-f]{64}")


class EnvError(Exception):
    pass


def load(env):
    import yaml
    p = os.path.join(M.SPECS, f"{env}.yml")
    if not os.path.isfile(p):
        raise EnvError(f"没有环境描述 {os.path.relpath(p, REPO)}")
    with open(p, encoding="utf-8") as f:
        sp = yaml.safe_load(f) or {}
    if not isinstance(sp, dict):
        raise EnvError(f"{env}.yml 不是一张表")
    bad = set(sp) - {"python", "channels", "override_channels", "conda", "base", "torch", "pip", "pip_no_deps"}
    if bad:
        raise EnvError(f"{env}.yml 里不认识的字段：{'、'.join(sorted(bad))}")
    if sp.get("base") and (sp.get("python") or sp.get("conda") or sp.get("torch")):
        raise EnvError(f"{env}.yml：有 base 的（venv）不能再写 python、conda、torch")
    if not sp.get("base") and not (sp.get("python") or sp.get("conda")):
        raise EnvError(f"{env}.yml：要写 base（venv），或 python / conda（conda 环境）")
    return sp


def check_file(env):
    p = os.path.join(M.SPECS, f"{env}.check.py")
    if not os.path.isfile(p):
        raise EnvError(f"没有检查 {os.path.relpath(p, REPO)}")
    return p


def variant(e):
    return e[1] if isinstance(e, (tuple, list)) and len(e) > 1 else "default"


def variant_of(name, env):
    """这台机器上 env 用的变体：条目是 "spec" 的按 mtab.spec_variant；还不是 "spec" 的（预建）、没有这个环境的 → default。"""
    e = (M.machine(name).get("envs") or {}).get(env)
    return M.spec_variant(name, env) if M.is_spec(e) else "default"


def vhash(env, v):
    """描述加这个变体的锁的哈希：.ok 里写的、状态比对的都是它。"""
    return M.spec_hash(env, v)


def entry(name, env):
    e = (M.machine(name).get("envs") or {}).get(env)
    if e is None:
        raise EnvError(f"{name} 的 envs 里没有 {env}")
    return e


def sh_path(p):
    """~ 开头的路径 → "$HOME"/… 的 shell 写法（引号里不展开 ~）。"""
    p = p.rstrip("/")
    if p == "~" or p.startswith("~/"):
        return '"$HOME"' + shlex.quote(p[1:]) if len(p) > 1 else '"$HOME"'
    return shlex.quote(p)


def state_dir(m):
    return m["root"].rstrip("/") + "/" + ROOT["envs_ok"] if m.get("root") else cfg.state("envs", expand=False)


def code_dir(m):
    return m["root"].rstrip("/") + "/" + ROOT["current"] if m.get("root") else (m.get("code") or m.get("repo"))


def base_activation(name, base):
    """venv 的基础环境的激活语句（那边建环境时用，写进 .activate）。基础环境是 "spec" 的用它的 .activate。"""
    a = M.activate(name, base)
    if a is None:
        raise EnvError(f"{name} 的 envs 里没有基础环境 {base}")
    return a


def own_base(name, env, v):
    """venv env 的基础环境用不用这台按描述建的那个（env_root 下）：机器条目里 env 是 "spec"、锁里没写 base、基础环境的条目是
    conda / venv 时用条目的，其余（预建、锁里写了 base、基础环境也是 "spec" 或条目里没有）用按描述建的。"""
    envs = M.machine(name).get("envs") or {}
    b = load(env).get("base")
    lk = load_lock(env, v)
    return not (M.is_spec(envs.get(env)) and not (lk and lk.get("base")) and b in envs and not M.is_spec(envs[b]))


def spec_activation(name, env):
    """按描述建在 env_root 下的 env 的激活语句（不看机器条目：预建、锁里写了 base 时用）。"""
    return "\n".join([DEACT, f"source {sh_path(M.env_root(M.machine(name)))}/{shlex.quote(env)}/.activate"])


def heredoc(tag, text):
    assert tag not in text
    return f"<<'{tag}'\n{text.rstrip(chr(10))}\n{tag}"


def pip_lines(pkgs):
    """{torch_mm} 这类占位换成 shell 变量（建环境时按环境里的 torch、Python 算）。"""
    return [p.replace("{torch_mm}", "${SPEC_TORCH_MM}").replace("{abi}", "${SPEC_ABI}").replace("{cp}", "${SPEC_CP}")
            for p in pkgs]


# ---------------- 锁文件和 wheelhouse ----------------
def load_lock(env, v):
    """<名>.<变体>.lock.json → dict；没有 → None；格式不对 → EnvError。"""
    p = M.lock_path(env, v)
    if not os.path.isfile(p):
        return None
    fn = os.path.basename(p)
    try:
        lk = json.load(open(p, encoding="utf-8"))
    except ValueError as e:
        raise EnvError(f"{fn} 读不了：{e}") from None
    if not isinstance(lk, dict) or lk.get("format") != proto.LOCK_FORMAT or lk.get("name") != env or lk.get("variant") != v:
        raise EnvError(f"{fn}：format 不是 {proto.LOCK_FORMAT}，或 name、variant 对不上")
    for x in lock_items(lk):
        if not (SHA_RE.fullmatch(str(x.get("sha256"))) and NAME_RE.fullmatch(str(x.get("fn"))) and isinstance(x.get("size"), int)):
            raise EnvError(f"{fn}：{x.get('name')} 的 sha256、fn、size 不对")
    for x in lk.get("conda") or []:
        if not re.fullmatch(r"[0-9a-f]{32}", str(x.get("md5"))):
            raise EnvError(f"{fn}：{x.get('name')} 的 md5 不对")
    for x in lk.get("pip") or []:
        if not (NAME_RE.fullmatch(str(x.get("name"))) and NAME_RE.fullmatch(str(x.get("version")))):
            raise EnvError(f"{fn}：pip 包 {x.get('name')} 的名字、版本不对")
    if lk.get("conda") and lk.get("base"):
        raise EnvError(f"{fn}：有 base 的（venv）不能有 conda 包")
    return lk


def lock_items(lk):
    return list(lk.get("conda") or []) + list(lk.get("pip") or [])


def wheelhouse():
    return cfg.state(proto.WHEELHOUSE)


def lock_files(lk, wh=None):
    """锁里的包 → {sha256: wheelhouse 里的路径（proto.wheel）}；缺的 → EnvError。wh：别的 wheelhouse（测试用）。"""
    out = {x["sha256"]: os.path.join(wh, x["sha256"][:2], x["sha256"]) if wh else cfg.state(proto.wheel(x["sha256"])) for x in lock_items(lk)}
    miss = [x["fn"] for x in lock_items(lk) if not os.path.isfile(out[x["sha256"]])]
    if miss:
        raise EnvError(f"wheelhouse（{wh or wheelhouse()}）里缺 {len(miss)} 个包：{'、'.join(miss[:5])}")
    return out


def lock_need(lk):
    """建这个锁要的空间（字节）：装好的环境（包 × SPACE_X）加一份临时的。"""
    total = sum({x["sha256"]: x["size"] for x in lock_items(lk)}.values())
    return int(total * SPACE_X + total)


# ---------------- 建环境的脚本 ----------------
def build_script(name, env, v=None, store=None):
    """→ 在那台机器上建 env 的 bash（自己带检查和状态文件）。v：变体（默认按机器条目）；store：有锁时那边放包的目录
    （<前两位>/<sha256>：执行机的 blobs、box 的 wheelhouse）。机器条目还不是 "spec" 的也能建（预建，不影响送任务）。"""
    m = M.machine(name)
    v = v or variant_of(name, env)
    sp, h = load(env), vhash(env, v)
    lk = load_lock(env, v)
    if lk and store is None:
        raise EnvError(f"{env} 有锁文件，要给放包的目录")
    chk = open(check_file(env), encoding="utf-8").read()
    root, sd = sh_path(M.env_root(m)), sh_path(state_dir(m))
    conda = sh_path(m["conda"][0]) if m.get("conda") else None
    L = ["#!/bin/bash", "set -euo pipefail", "export CUDA_VISIBLE_DEVICES=", f"ROOT={root}", f"SD={sd}",
         f"N={shlex.quote(env)}", f"H={h}", 'P="$ROOT/$N-${H:0:12}"', "T=", 'mkdir -p "$ROOT" "$SD"',
         'export CONDA_PKGS_DIRS="$ROOT/.pkgs" PIP_CACHE_DIR="$ROOT/.pip-cache"',
         'exec > >(tee -a "$SD/$N.log") 2>&1', f'echo "== $(date "+%F %T") 建 $N（{v}，$H）在 $(hostname -s)"',
         'echo "$H $(date +%s)" > "$SD/$N.building"', 'rm -f "$SD/$N.failed"',
         "trap 'rc=$?; if [[ $rc != 0 ]]; then echo \"$H $(date +%s) rc $rc\" > \"$SD/$N.failed\"; fi; rm -f \"$SD/$N.building\"; "
         "if [[ -n $T ]]; then rm -rf \"$T\"; fi; exit $rc' EXIT"]
    if lk:
        rows = [f"pkgs {x['sha256']} {x['fn']} {x['md5']}" for x in lk.get("conda") or []] + \
               [f"wheels {x['sha256']} {x['fn']} -" for x in lk.get("pip") or []]
        req = [f"{x['name']}=={x['version']} --hash=sha256:{x['sha256']}" for x in lk.get("pip") or []]
        L += [f"BL={sh_path(store)}", 'T="$ROOT/.tmp-$N-${H:0:12}-$$"',
              f'find "$ROOT" -maxdepth 1 -name ".tmp-$N-*" -mmin +{BUILD_MIN + 60} -exec rm -rf {{}} + 2>/dev/null || true',
              'mkdir -p "$T/pkgs" "$T/wheels"', 'echo "@EXPLICIT" > "$T/explicit.txt"',
              'export CONDA_PKGS_DIRS="$T/pkgs" CONDA_OFFLINE=1 PIP_NO_INDEX=1 PIP_CONFIG_FILE=/dev/null',
              "while read -r k h fn md5; do\n"
              '  s="$BL/${h:0:2}/$h"\n'
              '  if [[ ! -f $s ]]; then echo "缺包 $fn（$s）"; exit 1; fi\n'
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
            raise EnvError(f"{name} 的条目里没有 conda，建不了 conda 环境")
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
    code = code_dir(m)
    L += [f"python3 - {sh_path(code) if code else ''} {heredoc('CHECK_EOF', chk)}",
          'echo "$H" > "$P/.built"',
          'ln -sfn "$(basename "$P")" "$ROOT/.$N.new" && mv -T "$ROOT/.$N.new" "$ROOT/$N"',
          'echo "$H" > "$SD/$N.ok"', 'echo "== $(date "+%F %T") $N 建好、检查过"']
    return "\n".join(L) + "\n"


def online_lines(env, sp, v):
    """没有锁时联网装 torch 和 pip 包的几行。"""
    L = []
    tv = sp.get("torch") or {}
    if tv:
        t = tv.get(v)
        if not t:
            raise EnvError(f"{env}.yml 的 torch 里没有变体 {v}（有：{'、'.join(tv)}）")
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
    """→ (在那台机器上跑的命令, ssh 名（None = 本机）)。命令把标准输入存成状态目录里的临时文件再跑（建环境的 conda、pip 不会读到
    脚本后面的内容），只在 CPU 上跑：Slurm 用条目的 env_job（没有就用 transfer_job；没写时限时加 BUILD_MIN）在 CPU 分区 srun，
    其余 nice、看不到卡、最多 BUILD_MIN 分钟。"""
    m = M.machine(name)
    if m["scheduler"] == "slurm":
        job = m.get("env_job") or m.get("transfer_job")
        if not job:
            raise EnvError(f"{name} 的条目里没有 env_job（CPU 分区的 srun 参数）")
        args = shlex.split(job)
        if any(a in ("--gres", "-G") or a.startswith(("--gres=", "--gpus", "-G")) or "gpu" in a.lower() for a in args):
            raise EnvError(f"{name} 的 env_job 要了 GPU：{job}")
        timed = any(a in ("-t", "--time") or a.startswith("--time=") for a in args)
        launch = f"srun -I600 {job}" + ("" if timed else f" -t {BUILD_MIN}")
    else:
        launch = f"timeout -k 60 {BUILD_MIN}m nice -n 10 env CUDA_VISIBLE_DEVICES="
    sd = sh_path(state_dir(m))
    cmd = (f'mkdir -p {sd} && f=$(mktemp {sd}/.build.XXXXXX) && cat > "$f" && {{ {launch} bash "$f" < /dev/null; rc=$?; '
           f'rm -f "$f"; exit $rc; }}')
    return cmd, m.get("ssh")


# 建之前（有锁时）：按要的空间查那边剩多少（有 cap、env_root 在根目录下的按 cap 减已占的算），够就写 .building，不够写 .failed；
# clear：撤掉这次写的 .building（没开始建就停下时）
MARK_PY = """import shutil,time
E=os.path.expanduser(A['env_root'])
SD=os.path.expanduser(A['sd'])
n=A['env']
os.makedirs(SD,exist_ok=True)
if A['mode']=='clear':
    p=os.path.join(SD,n+'.building')
    try:
        if open(p).read().split()[0]==A['hash']:
            os.remove(p)
    except (OSError,IndexError):
        pass
    json_out(dict(ok=True))
    sys.exit(0)
p=E
while not os.path.isdir(p):
    p=os.path.dirname(p)
free=shutil.disk_usage(p).free
if A.get('cap'):
    used,_=usage(R)
    free=min(free,max(0,int(A['cap']*1e9)-used))
now=int(time.time())
ok=free>=A['need']
with open(os.path.join(SD,n+('.building' if ok else '.failed')),'w') as f:
    f.write('%s %d\\n'%(A['hash'],now) if ok else '%s %d nospace %d %d\\n'%(A['hash'],now,A['need'],free))
json_out(dict(ok=ok,free=free,need=A['need']))
"""


def mark(name, env, h, mode, need=0, run_py=None):
    """在那台机器上跑 MARK_PY → dict(ok, free, need)。执行机经 relay 的 rpy；本机直接跑。"""
    import relay_remote as RM
    m = M.machine(name)
    cap = m.get("cap") if m.get("root") and M.env_root(m).startswith(m["root"].rstrip("/") + "/") else None
    data = dict(env_root=M.env_root(m), sd=state_dir(m), env=env, hash=h, mode=mode, need=int(need), cap=cap)
    code = RM.HEAD + RM.USAGE + MARK_PY
    if run_py:
        return run_py(name, code, data)
    if name in M.RELAY:
        return RM.rpy(name, code, data)
    r = subprocess.run([sys.executable, "-c", code, os.path.expanduser(m.get("root") or M.env_root(m))], input=json.dumps(data),
                       capture_output=True, text=True, timeout=600)
    import spec as S
    res = S.json_in(r.stdout)
    if r.returncode or res is None:
        raise EnvError(f"查 {name} 的空间出错：{(r.stderr or r.stdout).strip()[-300:]}")
    return res


def build(name, env, dry=False, run=None, timeout=BUILD_TIMEOUT, v=None, say=print, run_py=None):
    """在那台机器上建 env → (退出码, 输出的最后几行)。dry：只返回要跑的命令和脚本，不跑。v：变体（默认按机器条目）。
    有锁时：先查空间（不够 → 不建、写 .failed、退出码 1），执行机先把包放进那边的 blobs（钉住到建完；CPU 作业排不上 →
    EXIT["stage_later"]）。要 ssh 长连接的机器、长连接断着 → EnvError（不另起连接）。"""
    import relay_remote as RM
    m = M.machine(name)
    v = v or variant_of(name, env)
    lk = load_lock(env, v)
    relay = name in M.RELAY
    if lk and m.get("ssh") and not relay:
        raise EnvError(f"{env} 有锁文件，只在 box 和执行机（RELAY）上建；{name} 两样都不是")
    store = (RM.root(name) + "/" + ROOT["blobs"] if relay else wheelhouse()) if lk else None
    files = lock_files(lk) if lk else {}
    script = build_script(name, env, v, store)
    cmd, host = runner(name)
    if dry:
        pre = f"先把 {len(files)} 个包放进 {store}\n" if lk and relay else ""
        return 0, pre + f"在 {host or '本机'} 上跑：{cmd}\n" + script
    if RM.master_down(m):
        raise EnvError(f"{name} 的 ssh 长连接断着，这次不建")
    import common as C
    run = run or C.run
    h = vhash(env, v)

    def go():
        rc, out, err = run(cmd, host, timeout, input=script) if host else run(["bash", "-c", cmd], None, timeout, input=script)
        return rc, "\n".join((out + err).strip().splitlines()[-15:])
    if not lk:
        return go()
    r = mark(name, env, h, "reserve", lock_need(lk), run_py)
    if not r.get("ok"):
        return 1, f"{name} 空间不够，没建 {env}：要 {r['need'] / 1e9:.1f} GB，剩 {r['free'] / 1e9:.1f} GB"
    if not relay:
        return go()
    import relay_transfer as RT
    try:
        with RT.pinned(name, f"env.{env}", files, say):
            RT.ensure_blobs(name, files, say, what=f"{name} 的环境 {env}")
            return go()
    except RM.NoSlot as e:
        mark(name, env, h, "clear", run_py=run_py)
        return proto.EXIT["stage_later"], f"{e}；这次不建，下次检查再来"
    except (RT.RN.RelayError, RuntimeError, OSError) as e:
        mark(name, env, h, "clear", run_py=run_py)
        raise EnvError(f"包没放到 {name}：{e}") from None


# ---------------- 状态 ----------------
STATUS_SH = r'''for n in "$@"; do
  ok=$(head -n1 "$SD/$n.ok" 2>/dev/null | tr -d '|' || true); fl=$(head -n1 "$SD/$n.failed" 2>/dev/null | tr -d '|' || true)
  bd=$(head -n1 "$SD/$n.building" 2>/dev/null | tr -d '|' || true); ln=0; [[ -e $ROOT/$n/.activate ]] && ln=1
  lg=; [[ -n $fl ]] && lg=$(tail -c 2000 "$SD/$n.log" 2>/dev/null | base64 -w0 || true)
  echo "ENV|$n|$ok|$fl|$bd|$ln|$lg"
done
echo "NOW|$(date +%s)"
'''


def rows_of(txt):
    """STATUS_SH 的输出 → ({环境: dict(ok, failed, building, active, log)}, 那边的时间)。"""
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
    """{环境: 那边的标志文件} + {环境: 变体} → {环境: 状态}。状态：ok、描述变了、没建、检查失败、空间不够、在建、在建（可能已停）。"""
    out = {}
    for k, v in want.items():
        r = rows.get(k) or {}
        h, bd, fl = vhash(k, v), (r.get("building") or "").split(), (r.get("failed") or "").split()
        if bd:
            age = now - int(bd[1]) if len(bd) > 1 and bd[1].isdigit() else 0
            out[k] = "在建" if age < STALE_S else "在建（可能已停）"
        elif (r.get("ok") or "").strip() == h and r.get("active"):
            out[k] = "ok"
        elif fl and fl[0] == h:
            out[k] = "空间不够" if len(fl) > 2 and fl[2] == "nospace" else "检查失败"
        elif r.get("ok"):
            out[k] = "描述变了"
        else:
            out[k] = "没建"
    return out


def read_rows(name, envs_, run=None):
    """→ (那边的标志文件, 那边的时间)；连不上（或要的 ssh 长连接断着）→ None。"""
    m = M.machine(name)
    if not envs_:
        return {}, time.time()
    import relay_remote as RM
    if run is None and RM.master_down(m):
        return None
    import common as C
    run = run or C.run
    sh = f"ROOT={sh_path(M.env_root(m))}; SD={sh_path(state_dir(m))}\n" + STATUS_SH
    cmd = f"bash -s -- {' '.join(shlex.quote(s) for s in envs_)}"
    rc, txt, _ = run(cmd, m.get("ssh"), 120, input=sh) if m.get("ssh") else run(["bash", "-s", "--", *envs_], None, 120, input=sh)
    return None if rc else rows_of(txt)


def states(name, run=None):
    """→ {环境: 状态}，只读。机器条目里是 conda、venv 的：不是按描述建的。连不上 → None。"""
    m = M.machine(name)
    envs = m.get("envs") or {}
    out = {k: "不是按描述建的" for k, e in envs.items() if not M.is_spec(e)}
    want = {k: M.spec_variant(name, k) for k, e in envs.items() if M.is_spec(e)}
    if not want:
        return out
    got = read_rows(name, list(want), run)
    if got is None:
        return None
    out.update(classify(got[0], want, got[1]))
    return out


def oks_of(states_, name):
    """name 的 states() 的结果 → ready_envs 用的 {环境: 哈希}（只放 ok 的）。"""
    return {k: M.spec_hash(k, M.spec_variant(name, k)) for k, v in (states_ or {}).items() if v == "ok"}


# ---------------- 自动建（box 的健康检查经 relay.py 调用） ----------------
def wanted(name, extra=()):
    """这台机器要建的 {环境: 变体}（基础环境在前）：条目里 "spec" 的，加上 extra（预建：环境名或 (环境, 变体)）；
    venv 的基础环境有描述的也算。没有描述的不算。"""
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
        except Exception:  # noqa: BLE001  描述、锁写错了：problems 报
            pass
        out[k] = v
    for k, v in todo:
        add(k, v)
    return out


def next_build(want, st, rows, now, skip=()):
    """→ 下一个要建的 (环境, 变体)；不建 → None。一台一次只建一个：有在建的就不建；基础环境没好的先等；
    检查失败的不自动重建（描述改了哈希变了才再建）；空间不够的 RETRY_S 后再试；skip（建不了的，如 wheelhouse 缺包）不建。"""
    if any(s == "在建" for s in st.values()):
        return None
    for k, v in want.items():
        s = st.get(k)
        if s in ("ok", "检查失败") or k in skip:
            continue
        if s == "空间不够":
            fl = ((rows.get(k) or {}).get("failed") or "").split()
            if len(fl) > 1 and fl[1].isdigit() and now - int(fl[1]) < RETRY_S:
                continue
        try:
            b = load(k).get("base")
        except Exception:  # noqa: BLE001
            continue
        if b in want and st.get(b) != "ok":
            continue
        return k, v
    return None


def problems(name, want, st, rows):
    """→ [(环境, 说明)]：检查失败（带日志最后几行）、空间不够、wheelhouse 里缺包、锁文件不对。"""
    out = []
    for k, v in want.items():
        r = rows.get(k) or {}
        if st.get(k) == "检查失败":
            tail = "\n".join((r.get("log") or "").strip().splitlines()[-8:])
            out.append((k, f"{name} 上建环境 {k}（{v}）失败：{(r.get('failed') or '').strip()}" + (f"\n{tail}" if tail else "")))
        elif st.get(k) == "空间不够":
            fl = (r.get("failed") or "").split()
            gb = lambda x: f"{int(x) / 1e9:.1f} GB" if x.isdigit() else x  # noqa: E731
            out.append((k, f"{name} 空间不够，没建环境 {k}（{v}）" + (f"：要 {gb(fl[3])}，剩 {gb(fl[4])}" if len(fl) > 4 else "")))
        if st.get(k) != "ok":
            try:
                load(k)
                lk = load_lock(k, v)
                if lk:
                    lock_files(lk)
            except Exception as e:  # noqa: BLE001
                out.append((k, f"{name} 的环境 {k}（{v}）建不了：{e}"))
    return out
