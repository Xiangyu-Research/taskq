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
<root>/<名>/.activate 是激活语句；状态目录里 <名>.ok（检查过的描述的 sha256）、<名>.failed、<名>.building、<名>.log。
描述一改哈希就变：新版建在新目录里，建好检查过才换软链接、写 .ok，旧版留着，在跑的任务不受影响。
"""
import os
import shlex
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "scripts", "queue"))
import cfg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(REPO, "scripts", "monitor"))
import machines as M  # noqa: E402

DEACT = 'if [[ -n ${VIRTUAL_ENV:-} ]]; then PATH=${PATH//"$VIRTUAL_ENV/bin:"/}; unset VIRTUAL_ENV; fi'
STALE_S = 6 * 3600  # .building 这么久没动就当已经停了
BUILD_MIN = 180     # Slurm 上建环境的时限（分钟）


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
    return m["root"].rstrip("/") + "/state/envs" if m.get("root") else cfg.state("envs", expand=False)


def code_dir(m):
    return m["root"].rstrip("/") + "/code/current" if m.get("root") else (m.get("code") or m.get("repo"))


def base_activation(name, base):
    """venv 的基础环境的激活语句（那边建环境时用，写进 .activate）。基础环境是 "spec" 的用它的 .activate。"""
    a = M.activate(name, base)
    if a is None:
        raise EnvError(f"{name} 的 envs 里没有基础环境 {base}")
    return a


def heredoc(tag, text):
    assert tag not in text
    return f"<<'{tag}'\n{text.rstrip(chr(10))}\n{tag}"


def pip_lines(pkgs):
    """{torch_mm} 这类占位换成 shell 变量（建环境时按环境里的 torch、Python 算）。"""
    return [p.replace("{torch_mm}", "${SPEC_TORCH_MM}").replace("{abi}", "${SPEC_ABI}").replace("{cp}", "${SPEC_CP}")
            for p in pkgs]


def build_script(name, env):
    """→ 在那台机器上建 env 的 bash（自己带检查和状态文件）。"""
    m = M.machine(name)
    e = entry(name, env)
    if not M.is_spec(e):
        raise EnvError(f"{name} 的 {env} 不是按描述建的（{e}），不用建")
    sp, h = load(env), M.spec_hash(env)
    chk = open(check_file(env), encoding="utf-8").read()
    root, sd = sh_path(M.env_root(m)), sh_path(state_dir(m))
    conda = sh_path(m["conda"][0]) if m.get("conda") else None
    L = ["#!/bin/bash", "set -euo pipefail", "export CUDA_VISIBLE_DEVICES=", f"ROOT={root}", f"SD={sd}",
         f"N={shlex.quote(env)}", f"H={h}", 'P="$ROOT/$N-${H:0:12}"', 'mkdir -p "$ROOT" "$SD"',
         'export CONDA_PKGS_DIRS="$ROOT/.pkgs" PIP_CACHE_DIR="$ROOT/.pip-cache"',
         'exec > >(tee -a "$SD/$N.log") 2>&1', 'echo "== $(date "+%F %T") 建 $N（$H）在 $(hostname -s)"',
         'echo "$H $(date +%s)" > "$SD/$N.building"', 'rm -f "$SD/$N.failed"',
         'trap \'rc=$?; if [[ $rc != 0 ]]; then echo "$H" > "$SD/$N.failed"; fi; rm -f "$SD/$N.building"; exit $rc\' EXIT']
    if sp.get("base"):
        act = base_activation(name, sp["base"])
        L += [act, 'if [[ ! -x $P/bin/python ]]; then python -m venv --system-site-packages "$P"; fi',
              f'printf \'%s\\n\' {shlex.quote(act)} "source \'$P/bin/activate\'" > "$P/.activate"']
    else:
        if conda is None:
            raise EnvError(f"{name} 的条目里没有 conda，建不了 conda 环境")
        chans = " ".join(f"-c {shlex.quote(c)}" for c in sp.get("channels") or [])
        if sp.get("override_channels"):
            chans += " --override-channels"
        pk = ([f"python={sp['python']}"] if sp.get("python") else []) + list(sp.get("conda") or [])
        L += [DEACT, f"CONDA_SH={conda}/etc/profile.d/conda.sh", 'source "$CONDA_SH"',
              f'if [[ ! -d $P/conda-meta ]]; then conda create -y -p "$P" {chans} {" ".join(shlex.quote(x) for x in pk)} < /dev/null; fi',
              f'printf \'%s\\n\' {shlex.quote(DEACT)} "source \'$CONDA_SH\'" "conda activate \'$P\'" > "$P/.activate"']
    L += ['source "$P/.activate"']
    tv = sp.get("torch") or {}
    if tv:
        t = tv.get(variant(e))
        if not t:
            raise EnvError(f"{env}.yml 的 torch 里没有变体 {variant(e)}（有：{'、'.join(tv)}）")
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
    code = code_dir(m)
    L += [f"python3 - {sh_path(code) if code else ''} {heredoc('CHECK_EOF', chk)}",
          'ln -sfn "$(basename "$P")" "$ROOT/.$N.new" && mv -T "$ROOT/.$N.new" "$ROOT/$N"',
          'echo "$H" > "$SD/$N.ok"', 'echo "== $(date "+%F %T") $N 建好、检查过"']
    return "\n".join(L) + "\n"


def runner(name):
    """→ (在那台机器上跑的命令, ssh 名（None = 本机）)。命令把标准输入存成状态目录里的临时文件再跑（建环境的 conda、pip 不会读到
    脚本后面的内容），只在 CPU 上跑：Slurm 用条目的 env_job（没有就用 transfer_job；没写时限时加 BUILD_MIN）在 CPU 分区 srun，其余 nice、看不到卡。"""
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
        launch = "nice -n 10 env CUDA_VISIBLE_DEVICES="
    sd = sh_path(state_dir(m))
    cmd = (f'mkdir -p {sd} && f=$(mktemp {sd}/.build.XXXXXX) && cat > "$f" && {{ {launch} bash "$f" < /dev/null; rc=$?; '
           f'rm -f "$f"; exit $rc; }}')
    return cmd, m.get("ssh")


def build(name, env, dry=False, run=None, timeout=4 * 3600):
    """在那台机器上建 env → (退出码, 输出的最后几行)。dry：只返回要跑的命令和脚本，不跑。"""
    script = build_script(name, env)
    cmd, host = runner(name)
    if dry:
        return 0, f"在 {host or '本机'} 上跑：{cmd}\n" + script
    import common as C
    run = run or C.run
    rc, out, err = run(cmd, host, timeout, input=script) if host else run(["bash", "-c", cmd], None, timeout, input=script)
    return rc, "\n".join((out + err).strip().splitlines()[-15:])


STATUS_SH = r'''for n in "$@"; do
  ok=$(cat "$SD/$n.ok" 2>/dev/null || true); fl=$(cat "$SD/$n.failed" 2>/dev/null || true)
  bd=$(cat "$SD/$n.building" 2>/dev/null || true); ln=0; [[ -e $ROOT/$n/.activate ]] && ln=1
  echo "ENV|$n|$ok|$fl|$bd|$ln"
done
echo "NOW|$(date +%s)"
'''


def states(name, run=None):
    """→ {环境: 状态}，只读。状态：ok、描述变了、没建、检查失败、在建、在建（可能已停）、不是按描述建的。连不上 → None。"""
    m = M.machine(name)
    envs = m.get("envs") or {}
    out = {k: "不是按描述建的" for k, e in envs.items() if not M.is_spec(e)}
    specs = [k for k, e in envs.items() if M.is_spec(e)]
    if not specs:
        return out
    import common as C
    run = run or C.run
    sh = f"ROOT={sh_path(M.env_root(m))}; SD={sh_path(state_dir(m))}\n" + STATUS_SH
    cmd = f"bash -s -- {' '.join(shlex.quote(s) for s in specs)}"
    rc, txt, _ = run(cmd, m.get("ssh"), 120, input=sh) if m.get("ssh") else run(["bash", "-s", "--", *specs], None, 120, input=sh)
    if rc:
        return None
    now = time.time()
    rows = {}
    for ln in txt.splitlines():
        p = ln.split("|")
        if p[0] == "NOW" and len(p) > 1 and p[1].isdigit():
            now = int(p[1])
        elif p[0] == "ENV" and len(p) == 6:
            rows[p[1]] = p[2:]
    for k in specs:
        ok, fl, bd, ln = rows.get(k, ["", "", "", "0"])
        h = M.spec_hash(k)
        if bd:
            t = bd.split()
            age = now - int(t[1]) if len(t) > 1 and t[1].isdigit() else 0
            out[k] = "在建" if age < STALE_S else "在建（可能已停）"
        elif ok and ok == h and ln == "1":
            out[k] = "ok"
        elif fl and fl == h:
            out[k] = "检查失败"
        elif ok:
            out[k] = "描述变了"
        else:
            out[k] = "没建"
    return out


def oks_of(states_):
    """states() 的结果 → ready_envs 用的 {环境: 哈希}（只放 ok 的）。"""
    return {k: M.spec_hash(k) for k, v in (states_ or {}).items() if v == "ok"}
