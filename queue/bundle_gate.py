#!/usr/bin/env python3
"""代码包闸门：把任务说明送给执行机之前，用那台在用的代码包（hub 上 relay_bundle.OUT 下同名的那份）在本机跑一遍
执行机对说明做的事（CMDS），跑不过就不送：任务记成 held（proto.HELD["bundle"]），报 bundlegate。
在用的包：那边 code/current 指的，加上 Slurm 里在跑的 worker 的（consume.py 要领新任务时才换到 current，之前仍用开始时的包；
worker 每次心跳 touch 自己的包，STALE_BEAT 内动过的算在用）。pueue 的 worker 代码变了就不再领新的，只看 current。
不知道那台在用哪个包、hub 上没有这个包、跑超时 → 放行。结果按 (包名, 说明的哈希) 在进程里记着。
记录在 relay 的状态 held（<名字> → {key: dict(why, bundle, err, since)}），那台在用的包变了以后重新核对。

    python3 queue/bundle_gate.py <包名或包的路径> < 说明.json     跑得了退出 0；跑不了打印原因、退出 1
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

TIMEOUT = 10  # 每条命令最多几秒；超时放行
# 执行机对送去的说明做的：run_task.sh、worker（consume.py）的 spec.py shell / done --spec（taskq.py add --spec-stdin 的解析同 shell）
CMDS = dict(shell=(0,), done=(proto.EXIT["done_yes"], proto.EXIT["done_no"]))  # 命令 → 算跑得了的退出码
SPEC_PY = cfg.bundle_rel("queue", "spec.py")
WHY = proto.HELD["bundle"]
_CACHE = {}


def spec_hash(spec):
    return hashlib.sha256(json.dumps(spec, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def bundle_dir(bid, out=None):
    """包名 → hub 上那份包的路径；没有 → None。"""
    if not bid or "/" in bid or bid.startswith("."):
        return None
    d = os.path.join(os.path.expanduser(out or RB.OUT), bid)
    return d if os.path.isfile(os.path.join(d, SPEC_PY)) else None


def run(d, spec):
    """在包 d 里跑 CMDS → (结果能不能记着, 跑不过的原因或 None)。数据目录指到包本身（只读，done 找不到结果 = 没做完）。"""
    env = {k: v for k, v in os.environ.items() if not k.startswith("TASKQ_") and k not in ("PYTHONPATH", "PYTHONHOME")}
    env.update(TASKQ_DATA=d, PYTHONDONTWRITEBYTECODE="1")  # 包会原样推过去：不在里面写 __pycache__
    arg = json.dumps(spec, ensure_ascii=False)
    for c, good in CMDS.items():
        try:
            r = subprocess.run([sys.executable, os.path.join(d, SPEC_PY), c, "--spec", arg], cwd=d, env=env,
                               capture_output=True, text=True, timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            return False, None
        if r.returncode not in good or "Traceback" in r.stderr:
            last = (r.stderr.strip() or r.stdout.strip()).splitlines()[-1:]
            return True, f"spec.py {c} 退出 {r.returncode}：{(last[0] if last else '')[-300:]}"
    return True, None


def check(spec, bid, out=None):
    """包 bid 跑得了送去的说明 spec → None；跑不了 → 原因。不知道包、hub 上没有这个包、没有说明 → None（放行）。"""
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
    """这台要跑得了说明的包：current，Slurm 的再加上在跑的 worker 的（live）。"""
    ids = [cur] + (list(live or ()) if RM.conf(name)["scheduler"] == "slurm" else [])
    return list(dict.fromkeys(x for x in ids if x))


def check_all(spec, ids, out=None):
    """→ (跑不了的包, 原因)；都跑得了 → None。"""
    for bid in ids:
        err = check(spec, bid, out)
        if err:
            return bid, err
    return None


def current(name):
    """那边在用的包（in_use 的参数：current, live）；连不上 → None。"""
    c = RM.conf(name)
    if c.get("ssh_master") and subprocess.run(RM.ssh_argv()[:1] + ["-O", "check", c["ssh"]], capture_output=True, timeout=30).returncode:
        return None  # 长连接断着：不另起连接（中转执行机认证失败多了会被锁）
    d = f"{R.q(RM.root(name))}/{proto.ROOT['code']}"
    cur = f"{R.q(RM.root(name))}/{proto.ROOT['current']}"
    rc, out, _ = RM.rsh(name, f'[ -e {cur} ] && basename "$(readlink -f {cur})"; echo; [ -d {d} ] && '
                              f"find {d} -mindepth 1 -maxdepth 1 -type d -mmin -{proto.STALE_BEAT // 60} ! -name '*.tmp*' -printf '%f\\n'",
                        timeout=30)
    if rc not in (0, 1):
        return None
    head, _, rest = out.partition("\n")
    return head.strip(), rest.split()


# ---------------- 记录（relay 的状态 held） ----------------
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
        print(f"hub 上没有包 {argv[0]}（{RB.OUT}）：放行", file=sys.stderr)
        return 0
    _, err = run(os.path.abspath(d), json.loads(sys.stdin.read()))
    if err:
        print(err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
