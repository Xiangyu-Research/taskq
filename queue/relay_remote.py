"""hub 在执行机、store 上跑东西：rsh（一条 shell 命令）、rtq（那边的 taskq.py）、op（site_ops.py 的一个操作）、store_op。
hub 上的记账见 relay_store.py，传数据和拉回见 relay_transfer.py，每次检查的计划见 relay.py。

那边的根目录 R（机器表的 root）的布局、标志文件、时间常数都在 proto.py（ROOT、MKDIRS、blob、QUEUE、claim_alive）：
代码包在 code/<包名>，code/current 指向在用的那个；queue 是它自己的任务队列（key 和总队列相同）；blobs/<h[:2]>/<h> 是按 sha256
存的数据文件；data 是数据目录，相对路径和 store 的数据目录一样：每个任务的输入是指向 blobs 的软链接（和插件生成的小文件，sites/names.py），
产出照常写。hub 上 state.json 记下每个任务放了哪些路径，删一个任务时只删别的任务不用的。
操作都跑那台在用的代码包（cd 进 code/current），本机的（ssh=None）也一样：store 也有根目录、也推代码包。
"""
import json
import os
import re
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
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

JOB_WAIT = 600  # 在 CPU 作业里跑（machines.py 的 transfer_job）时最多等这么久排上


class NoSlot(RuntimeError):
    """CPU 作业这次排不上。"""


ERR_LINE = re.compile(r"(?:[\w.]*(?:Error|Exception)\b|错误|[\w.-]+: (?:error|错误))")  # start of an error line: exception name (last line of a Traceback), the Chinese word for error, "<program>: error"; the Chinese alternative stays because other programs print it, not our format


def err_tail(text, n=300, own=()):
    """那边出错时的输出 → 给人看的一段：最后 n 字；最后一条错误行（ERR_LINE 开头的，或以 own 里的某个开头：site_ops 自己报错的
    "<操作>: 原因"）不在这里面时（被后面的进度输出挤掉了）放在前面。"""
    t = (text or "").strip()
    tail = t[-n:]
    hit = next((x for x in (ln.strip() for ln in reversed(t.splitlines())) if ERR_LINE.match(x) or x.startswith(tuple(own))), None)
    return f"{hit[:n]} …… {tail}" if hit and hit not in tail else tail


def conf(name):
    return M.machine(name)


def root(name):
    return conf(name)["root"].rstrip("/")


def py(name):
    """跑 site_ops 的 python：机器表有 conda 时用它的，没有时用那边 PATH 上的 python3（site_ops 只用标准库）。"""
    c = conf(name).get("conda")
    return c[0] + "/bin/python3" if c else "python3"


def ssh_argv():
    return shlex.split(os.environ.get("RELAY_SSH", "ssh")) + C.SSH[1:]  # RELAY_SSH 只给测试用


def master_down(m):
    """机器条目 m 要用 ssh 长连接（ssh_master）、长连接断着 → True：不另起连接（中转执行机认证失败多了会被锁）。"""
    import subprocess
    return bool(m.get("ssh_master") and m.get("ssh")) and subprocess.run(
        ssh_argv()[:1] + ["-O", "check", m["ssh"]], capture_output=True, timeout=30).returncode != 0


def site_argv(name, cmd, timeout=300):
    """在执行机上跑一条 shell 命令的参数表：经 ssh（那边也限时，common.remote_timeout）；本机执行机（ssh=None）在家目录里
    本机跑，去掉 hub 才有的变量（proto.HUB_ONLY_ENV），和 ssh 登录过去时一样。"""
    host = conf(name)["ssh"]
    if host is None:
        return ["env", *[a for v in proto.HUB_ONLY_ENV for a in ("-u", v)], "bash", "-c", f"cd ~ && {cmd}"]
    return ssh_argv() + [host, C.remote_timeout(f"bash -c {shlex.quote(cmd)}", timeout)]


def rsh(name, cmd, input=None, timeout=300):
    return C.run(site_argv(name, cmd, timeout), None, timeout, input=input)


def prefix(name):
    """那边命令的前缀：进代码包、设 TASKQ_ROOT 和 TASKQ_MACHINE（队列、状态、数据目录由它们算，cfg.py）。"""
    r = R.q(root(name))
    return f"cd {r}/{proto.ROOT['current']} && TASKQ_ROOT={r} TASKQ_MACHINE={shlex.quote(name)}"


def rtq(name, args, timeout=180, input=None):
    """那边的 taskq.py <args>（input：标准输入）→ (退出码, 输出)；连不上 → None。"""
    rc, out, err = rsh(name, f"{prefix(name)} {py(name)} {cfg.bundle_rel('queue', 'taskq.py')} {args}", input=input, timeout=timeout)
    return None if rc in (proto.EXIT["timeout"], 255) else (rc, (out + err).strip())


def run_lines(argv, input, timeout, say):
    """跑命令 → (退出码, 标准输出, 标准错误)；标准错误一行一行交给 say（长的传输、归档边跑边看进度）。超时 → 124。
    标准输入、标准输出、标准错误各由一个线程读写（communicate 也会读标准错误，和逐行读的抢着读会丢行）。"""
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
        return 124, "".join(out), "".join(err) + f"\n超过 {timeout} 秒"
    return rc, "".join(out), "".join(err)


def op(name, what, data=None, timeout=600, job=None, say=None, script="site_ops.py"):
    """在那台跑它在用的代码包里 site_ops.py 的一个操作（data 以 JSON 经标准输入）→ 它的结果。出错 → RuntimeError。
    本机的（ssh=None）也跑它根目录里的代码包，和远程的一样。job：Slurm 的 srun 参数（machines.py 的 transfer_job），
    在 CPU 节点上跑；排不上 → NoSlot。say：给了就边跑边把那边的进度（stderr）一行行交给它。
    script：代码包里 queue/ 下的另一个同样调用方式的脚本（migrate.py root）。"""
    m = conf(name)
    cmd = f"{prefix(name)} {py(name)} {cfg.bundle_rel('queue', script)} {what}"
    if job:
        cmd = f"srun -I{JOB_WAIT} {job} -J {SL.jobname(m.get('job_names'))} bash -c {shlex.quote(cmd)}"
        timeout += JOB_WAIT
    argv, inp = site_argv(name, cmd, timeout), json.dumps(data)
    rc, out, err = run_lines(argv, inp, timeout, say) if say else C.run(argv, None, timeout, input=inp)
    if job and rc and S.json_in(out) is None and any(x in err for x in ("Unable to allocate", "Immediate", "revoked")):
        raise NoSlot(f"{m['label']} 的 CPU 作业排不上：{err_tail(err, 200)}")
    res = S.json_in(out)
    if rc or res is None:
        raise RuntimeError(f"{m['label']} 上 {what} 出错（{rc}）：{err_tail(err or out, own=(f"{what}: ",))}")
    return res


def store_op(what, data, timeout=600, say=None):
    """在 store 上跑 site_ops.py 的一个 store 操作，数据目录是 store 的（machines.py 的 data）。"""
    return op(M.store(), what, dict(data, data=M.data()), timeout, say=say)


def same_disk_srcs(name):
    """收进 blobs 时先找的、这台本机已有的数据目录（同一个盘上内容对得上的硬链接，不重传）：它自己的数据目录，
    它兼 store 时再加 store 的数据目录。"""
    return [f"{root(name)}/{proto.ROOT['data']}"] + ([M.data()] if name == M.store() else [])
