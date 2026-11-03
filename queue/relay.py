#!/usr/bin/env python3
"""relay 执行机（envs/machines.py 的 RELAY：lab1、lab0、dicc1、dicc2）的接力：由 box 替它在总队列（box 上的任务队列，10/31 起）
领任务（GPU 或 CPU，按它有的 worker 种类和环境）、把输入放到和 box 一样的相对路径、放进它自己的任务队列，做完拉回、归档。
执行机和 box 用同样的名字和路径（11/1 起不换名）；执行机之间不直接通信，一切经 box。

那边（根目录 R，machines.py 的 root）：
  R/code/<包名>、R/code/current   scripts/queue/relay_bundle.py 打的代码包（origin/main 变了就重打、切换 current；旧包留最新 3 个，
                                  4 天以上的删）；worker 和 DICC 同一套脚本（作业名、资源、脚本名分开，见 machines.py 的 RELAY）
  R/queue/                        它自己的任务队列（taskq.py，key 和总队列相同）
  R/blobs/<id[:2]>/<id>           数据文件，id = sha256（内容相同的只传一次）
  R/data/                         数据目录，相对路径和 box 的一样：每个任务的输入是指向 blobs 的软链接（和插件生成的小文件，
                                  scripts/sites/names.py），产出照常写。state.json 记下每个任务放了哪些路径（视图、输入、产出），
                                  删一个任务时只删别的任务不用的（相同、上级、下级都算在用）
每次健康检查（scripts/monitor/health.py，每 30 分钟）经 scripts/queue/feed.py 的 run_all 调用这里的 run_all（送任务只有一个入口，10/31）：
  1. ssh 一次读那边的队列、各任务进度、作业、剩余空间（写到 ~/.cache/phd/relay/status.json，登记表用）；
  2. 总队列里 worker 是它的每个任务：
     - 还没放进那边的队列 → box 的 wait 组排 relay.py stage（补传缺的数据、建软链接、加进那边的队列；要放的路径和那边没做完的
       任务的产出重叠时不传、报出来）；
     - 记录上有 redo（做法或输入变了、11/1 迁移）→ 那边没人领着时删掉那边的任务和它独占的路径，下次检查重新传；
     - 在那边排着、在训 → 续上（beat）；
     - 那边做完 → box 的 wait 组排 relay.py fetch（拉回、核对）；
     - 总队列显示训完（box 上有了）→ 删掉那边的任务和结果（11/3 定：执行机的盘只用来中转）；要归档的，备份机能用时 box 的 wait 组
       单独排 archive_task.sh box（relay_archive，备份到 envs/machines.py 角色 backup 的那台），归档成功才交还；备份机停用
       （envs/policy.py 的 OFFLINE）时不等，直接交还；只是这次连不上时下次检查再排；
     - 那边失败：没有进度 → 交还（--skip，它以后不再领），删掉那边的任务；有进度 → 领取保留、报出来；
     - 总队列里停了（taskq.py stop）→ 那边也停，那边不再领着以后交还（那边的结果留着）；撤销停止后再领到时那边接着训；
     - 传数据、拉回出错、验证集对不上 → 领取保留、报出来（处理后 pueue remove 那个 box 任务，下次检查重排）。
  3. 那边排着没人领的任务少于 envs/policy.py 的 READY[名字] 时，按 refill.py 的挑法领下一个任务（只领插件接受的做法，
     数据传过去后那边至少还剩 HARD_GB）；
  4. 那边排着和在跑的 worker 作业不够、又有要卡的任务（备好的或在训的）时排满（refill.py 的 topup_plan，QOS 和个数见 SLURM[名字]["gpu"]）。
开关 envs/policy.py 的 AUTO[名字]：关着时第 3、4 步只记进 ~/relay.log（影子），已经领着的任务照常推进。
按时段的执行机（envs/policy.py 的 SHIFT，scripts/queue/shift.py）时段外不做第 3、4 步。同一个盘上的几台（lab0、lab1）：
每次检查按整块盘的剩余算，前一台这次计划传的从后一台的剩余里扣掉；总队列里改分给别的执行机的任务，那边的队列里删掉任务
（结果留着，接手的那台收编存档，见 machines.py 的 adopt），总队列里训完了再删结果。
连不上它时只续总队列的领取；领着任务、连不上 2 小时以上报出来。

    python3 scripts/queue/relay.py                         看现在会做什么（不改任何东西）
    python3 scripts/queue/relay.py stage <名字> <key> --nice N [--no-add]   传数据、加进那边的队列（--no-add：只传数据、撤掉那边"缺输入"的标记）
    python3 scripts/queue/relay.py fetch <名字> <key>      拉回做完的任务、核对，放进 box 的数据目录
    python3 scripts/queue/relay.py bundle <名字> [--commit C]   打包、推过去、切换 current
    python3 scripts/queue/relay.py status <名字>           那边的队列
"""
import argparse
import base64
import fcntl
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(REPO, "scripts", "sync"))
sys.path.insert(0, os.path.join(REPO, "scripts", "monitor"))
sys.path.insert(0, os.path.join(REPO, "scripts", "sites"))
import machines as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402
import relay_gc  # noqa: E402
import taskq  # noqa: E402
import refill  # noqa: E402
import remote as R  # noqa: E402
import pueue as PQ  # noqa: E402
import slurm as SL  # noqa: E402
import relay_plugins as RN  # noqa: E402
import relay_bundle as RB  # noqa: E402
import names as N  # noqa: E402
import shift  # noqa: E402
from common import write_json  # noqa: E402

DIR = RN.RELAY_DIR
STATE = os.path.join(DIR, "state.json")
STATUS = os.path.join(DIR, "status.json")
HASHES = os.path.join(DIR, "hashes.json")
LOG = os.path.expanduser(os.environ.get("RELAY_LOG", "~/relay.log"))
HARD_GB = 100  # 领之前：传过去以后那边至少还剩这么多
DOWN_S = 2 * 3600  # 领着任务、连不上这么久就报
LATE_S = 3600  # 拉回、归档成功这么久了 box 上还不算训完，就报
LIVE = refill.LIVE
VIEW_KEEP_S = 6 * 3600  # 建好这么久以内的视图不清理（任务刚传完、还没加进队列或刚交还）
STATUS_S = 90  # 读状态的超时：那边的存储挂住时跳过这台，不等
JOB_WAIT = 600  # 在 CPU 作业里跑（machines.py 的 adopt_job）时最多等这么久排上
NOSLOT_RC = 75  # relay.py stage 这次排不上 CPU 作业的退出码：下次检查再传，不报
NOSLOT_ALERT_S = 3 * 3600  # 领了这么久还一直排不上就报
PLANNED = {}  # 这次检查里各台已经计划传的 GB，按 (ssh, 盘) 记：同一个盘上的几台合起来算


class NoSlot(RuntimeError):
    """CPU 作业这次排不上。"""


def conf(name):
    return M.RELAY[name]


def root(name):
    return conf(name)["root"].rstrip("/")


def py(name):
    return conf(name)["conda"][0] + "/bin/python3"


def box_data():
    return M.expand(M.MACHINES[M.hub()]["data"])


def ssh_argv():
    return shlex.split(os.environ.get("RELAY_SSH", "ssh")) + C.SSH[1:]  # RELAY_SSH 只给测试用


# ---------------- 状态文件 ----------------
def update_state(fn):
    os.makedirs(DIR, exist_ok=True)
    with open(STATE + ".lock", "a") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        st = load_state()
        out = fn(st)
        write_json(STATE, st)
    return out


def load_state():
    try:
        with open(STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def keys_of(st, name):
    """key → dict(recipe, params, data, staged, views, inputs, outs, cleanup)。inputs：放过去的文件、视图里的除外（相对 R/data）；
    outs：产出的文件夹。redo：下次检查先删掉那边的任务和这些路径，再重新传（11/1 迁移时给换名的老记录打上，老记录的 alias 是那边的 key）。"""
    return (st.get("keys") or {}).get(name) or {}


def rkey(key, k):
    """那边队列里的 key：就是 key（11/1 起不换名）；11/1 前的老记录是别名，只用来删它们。"""
    return (k or {}).get("alias") or key


def paths_of(k):
    """一个任务在那边占的路径（相对 R/data）：视图、输入、产出、要清理的。"""
    return set(views_of(k)) | set(k.get("inputs") or []) | set(k.get("outs") or []) | set(k.get("cleanup") or [])


def overlaps(p, used, anc):
    """p 和 used 里的某个路径相同、是它的上级或下级 → True。anc：used 里各路径的所有上级。"""
    if p in used or p in anc:
        return True
    parts = p.split("/")
    return any("/".join(parts[:i]) in used for i in range(1, len(parts)))


def ancestors(paths):
    return {"/".join(q.split("/")[:i]) for q in paths for i in range(1, len(q.split("/")))}


def droppable(keys, key, paths):
    """删 key 时 paths 里能删的：别的任务（keys 里的其余记录）还用着的（相同、上级或下级）不删。"""
    used = set().union(*[paths_of(k) for r, k in keys.items() if r != key]) if len(keys) > 1 else set()
    anc = ancestors(used)
    return sorted(p for p in paths if not overlaps(p, used, anc))


def views_of(k):
    """一个任务的视图文件夹（旧记录只有 view 一项）。"""
    return list(k.get("views") or ([k["view"]] if k.get("view") else []))


def safe_rel(rel):
    """相对 R/data 的路径：不能是绝对路径、不能有 ..、至少两层（不删到 data 下的第一层）。"""
    r = os.path.normpath(rel)
    if r.startswith("/") or ".." in r.split("/") or len(r.split("/")) < 2:
        raise RN.RelayError(f"路径不对：{rel}")
    return r


def relayable(i):
    """refill.info 的结果 → 能不能领给它：做法能翻译（按本机的 recipes.py 算，总队列那边的代码可能还没更新），插件接受这个任务
    （scripts/sites/names.py 的 accepts，如 nnU-Net 的训练器不读仓库里的病例子集）。"""
    sp = i["spec"]
    rec, params = i["recipe"] or sp.get("recipe"), i["params"] or sp.get("params") or {}
    try:
        relay = N.plugin_of(S.resolve(rec, params))
    except S.SpecError:
        relay = N.plugin_of(sp)
    return N.accepts(relay, params)


# ---------------- 在那边跑 ----------------
def rsh(name, cmd, input=None, timeout=300):
    return C.run(cmd, conf(name)["ssh"], timeout, input=input, ssh=ssh_argv())


def prefix(name):
    r = R.q(root(name))
    return f"cd {r}/code/current && TASKQ_DIR={r}/queue TASKQ_DATA={r}/data"


def rtq(name, args, timeout=180):
    """那边的 taskq.py <args> → (退出码, 输出)；连不上 → None。"""
    rc, out, err = rsh(name, f"{prefix(name)} {py(name)} scripts/queue/taskq.py {args}", timeout=timeout)
    return None if rc in (124, 255) else (rc, (out + err).strip())


def rpy(name, code, data=None, timeout=600, job=None):
    """在那边跑一段 Python（第一个参数是根目录，data 以 JSON 经标准输入）→ 它 json_out 的结果。出错 → RuntimeError。
    job：Slurm 的 srun 参数（machines.py 的 adopt_job），在 CPU 节点上跑；排不上 → NoSlot。"""
    cmd = f"{prefix(name)} {py(name)} -c {shlex.quote(code)} {R.q(root(name))}"
    if job:
        cmd = f"srun -I{JOB_WAIT} {job} -J {SL.jobname()} bash -c {shlex.quote(cmd)}"
        timeout += JOB_WAIT
    rc, out, err = rsh(name, cmd, input=json.dumps(data), timeout=timeout)
    if job and rc and S.json_in(out) is None and any(x in err for x in ("Unable to allocate", "Immediate", "revoked")):
        raise NoSlot(f"{conf(name)['label']} 的 CPU 作业排不上：{err.strip()[-200:]}")
    res = S.json_in(out)
    if rc or res is None:
        raise RuntimeError(f"{conf(name)['label']} 上出错（{rc}）：{(err or out).strip()[-300:]}")
    return res


HEAD = "import json,os,sys\nsys.path.insert(0,'scripts/queue')\nimport spec\nR=sys.argv[1]\nA=json.load(sys.stdin)\n"

STATUS_PY = HEAD + """import shutil,subprocess,taskq
st=taskq.local_status()
sp=[{k:t['spec'].get(k) for k in spec.COLLECT_FIELDS} for t in st['tasks'] if t.get('spec')]
col=spec.collect(sp,os.environ['TASKQ_DATA'])
raw=None
try:
    if A.get('sched')=='pueue':
        q=subprocess.run([os.path.expanduser(A['pueue']),'status','--json'],capture_output=True,text=True,timeout=60)
    else:
        q=subprocess.run(['squeue','-u',os.environ.get('USER',''),'-h','-o','%i|%j|%T|%M|%q|%R'],capture_output=True,text=True,timeout=60)
    raw=q.stdout if q.returncode==0 else None
except Exception:
    raw=None
ed=os.path.join(R,'state','envs')
oks={f[:-3]:open(os.path.join(ed,f)).read().strip() for f in (os.listdir(ed) if os.path.isdir(ed) else []) if f.endswith('.ok')}
free=shutil.disk_usage(R).free
if A.get('cap'):
    used,seen=0,set()
    for dp,dns,fns in os.walk(R):
        for f in fns:
            x=os.lstat(os.path.join(dp,f))
            if x.st_nlink>1:
                if (x.st_dev,x.st_ino) in seen:
                    continue
                seen.add((x.st_dev,x.st_ino))
            if not os.path.islink(os.path.join(dp,f)):
                used+=x.st_blocks*512
    free=min(free,max(0,int(A['cap']*1e9)-used))
spec.json_out(dict(taskq=st,tasks=col.get('tasks',col.get('folds')),sched_raw=raw,free=free,dev=os.stat(R).st_dev,envs_ok=oks,
                   bundle=os.path.basename(os.path.realpath(os.path.join(R,'code','current')))))
"""

MISSING_PY = HEAD + "spec.json_out([i for i in A if not os.path.isfile(os.path.join(R,'blobs',i[:2],i))])\n"

PIN_PY = HEAD + """d=os.path.join(R,'state','pins')
os.makedirs(d,exist_ok=True)
p=os.path.join(d,A['key']+'.json')
if A.get('hashes') is None:
    if os.path.exists(p):
        os.remove(p)
else:
    with open(p+'.tmp','w') as f:
        json.dump(A['hashes'],f)
    os.replace(p+'.tmp',p)
spec.json_out(1)
"""

ADOPT_PY = HEAD + """import hashlib
def sha(p):
    x=hashlib.sha256()
    with open(p,'rb') as f:
        for b in iter(lambda:f.read(1<<22),b''):
            x.update(b)
    return x.hexdigest()
got=[]
for h,(rel,size) in A['files'].items():
    t=os.path.join(R,'blobs',h[:2],h)
    for s in A['srcs']:
        base=os.path.expanduser(s['path'])
        p=os.path.join(base,'blobs',h[:2],h) if s.get('site') else (os.path.join(base,rel) if rel else None)
        try:
            if not p or os.path.getsize(p)!=size:
                continue
        except OSError:
            continue
        if not s.get('site') and sha(p)!=h:
            continue
        os.makedirs(os.path.dirname(t),exist_ok=True)
        if not os.path.exists(t):
            try:
                os.link(os.path.realpath(p),t)
            except OSError:
                continue
        got.append(h)
        break
spec.json_out(got)
"""

VIEW_PY = HEAD + """import base64,fcntl
D=os.path.join(R,'data')
os.makedirs(os.path.join(R,'state'),exist_ok=True)
L=open(os.path.join(R,'state','view.lock'),'w')
fcntl.flock(L,fcntl.LOCK_EX)
def rel_ok(r):
    r=os.path.normpath(r)
    if r.startswith('/') or '..' in r.split('/'):
        sys.exit('bad path '+r)
    return r
views=[rel_ok(v) for v in A['views']]
if any(len(v.split('/'))<2 for v in views):
    sys.exit('bad views '+repr(views))
incs=[]
for v in views:
    d=os.path.join(D,v)
    os.makedirs(d,exist_ok=True)
    inc=os.path.join(d,'.incomplete')
    open(inc,'w').close()
    incs.append(inc)
want=set(incs)
def put(p,f):
    os.makedirs(os.path.dirname(p),exist_ok=True)
    tmp=p+'.relaytmp'
    if os.path.lexists(tmp):
        os.remove(tmp)
    f(tmp)
    os.replace(tmp,p)
for rel,h in A['links'].items():
    p=os.path.join(D,rel_ok(rel))
    t=os.path.join(R,'blobs',h[:2],h)
    want.add(p)
    if not os.path.isfile(t):
        sys.exit('missing '+h)
    if not (os.path.islink(p) and os.readlink(p)==t):
        put(p,lambda x:os.symlink(t,x))
for rel,b in A['texts'].items():
    p=os.path.join(D,rel_ok(rel))
    want.add(p)
    def w(x,b=b):
        with open(x,'wb') as f:
            f.write(base64.b64decode(b))
    put(p,w)
B=os.path.realpath(os.path.join(R,'blobs'))+'/'
for v in A.get('clean',views):
    for dp,_,fs in os.walk(os.path.join(D,rel_ok(v))):
        for f in fs:
            p=os.path.join(dp,f)
            if p not in want and os.path.islink(p) and os.path.realpath(p).startswith(B):
                os.remove(p)
for inc in incs:
    os.remove(inc)
spec.json_out(len(want)-len(incs))
"""

LIST_PY = HEAD + """import fnmatch,glob
D=os.path.join(R,'data')
B=os.path.realpath(os.path.join(R,'blobs'))+'/'
T=A['spec']
ex=(T.get('fetch') or {}).get('exclude') or []
items=[]
def add(p):
    if os.path.isfile(p) and not os.path.realpath(p).startswith(B):
        st=os.stat(p)
        items.append([os.path.relpath(p,D),st.st_size,st.st_mtime_ns])
def bad(e,top=None):
    if not (isinstance(e,FileNotFoundError) and e.filename==top):
        raise e
for o in A['outs']:
    top=os.path.join(D,o)
    for dp,_,fs in os.walk(top,onerror=lambda e:bad(e,top)):
        for f in fs:
            if not any(fnmatch.fnmatch(f,x) for x in ex):
                add(os.path.join(dp,f))
for x in (T.get('fetch') or {}).get('extra') or []:
    for p in glob.glob(os.path.join(os.path.dirname(os.path.join(D,A['outs'][0])),x)):
        add(p)
for p in glob.glob(os.path.join(D,'logs','tasks',glob.escape(spec.safe(A['key']))+'__*')):
    add(p)
spec.json_out(dict(done=spec.is_done(T,D),items=items))
"""

DROP_PY = HEAD + """import glob,shutil
D=os.path.join(R,'data')
n=0
for rel in A['paths']:
    r=os.path.normpath(rel)
    if r.startswith('/') or '..' in r.split('/') or len(r.split('/'))<2:
        continue
    p=os.path.join(D,r)
    if os.path.islink(p) or os.path.isfile(p):
        os.remove(p)
    elif os.path.isdir(p):
        shutil.rmtree(p)
    else:
        continue
    n+=1
    d=os.path.dirname(p)
    while len(os.path.relpath(d,D).split('/'))>=2:
        try:
            os.rmdir(d)
        except OSError:
            break
        d=os.path.dirname(d)
for p in glob.glob(os.path.join(D,'logs','tasks',glob.escape(A['logs'])+'__*')):
    os.remove(p)
spec.json_out(n)
"""

STATES_PY = HEAD + "import taskq\nspec.json_out({t['key']:t['state'] for t in taskq.local_status()['tasks']})\n"


ADOPT_OUT_PY = HEAD + """import glob,shutil
new=os.path.join(R,'data',A['out'])
def mt(d):
    ts=[os.path.getmtime(p) for g in A['resume'] for p in glob.glob(os.path.join(glob.escape(d),g)) if os.path.isfile(p)]
    return max(ts) if ts else None
best,bt=None,mt(new)
if A['done'] and all(any(os.path.exists(os.path.join(new,x)) for x in it.split('|')) for it in A['done']):
    bt=float('inf')
for d in A['srcs']:
    d=os.path.expanduser(d)
    if os.path.realpath(d)==os.path.realpath(new):
        continue
    m=mt(d)
    if m is not None and (bt is None or m>bt):
        best,bt=d,m
n=0
if best:
    os.makedirs(new,exist_ok=True)
    rs={os.path.basename(p) for g in A['resume'] for p in glob.glob(os.path.join(glob.escape(best),g))}
    for f in sorted(os.listdir(best),key=lambda f:f in rs):
        if os.path.isfile(os.path.join(best,f)):
            shutil.copy2(os.path.join(best,f),os.path.join(new,f+'.relaytmp'))
            os.replace(os.path.join(new,f+'.relaytmp'),os.path.join(new,f))
            n+=1
spec.json_out(dict(n=n,src=best))
"""


def adopt_srcs(name):
    """收编来源（machines.py 的 adopt）：同一个盘上别的执行机的根目录在前（按哈希直接认，不重算），老数据目录在后。"""
    return sorted(M.adopt_sources(name), key=lambda x: not x["site"])


def remote_status(name):
    """→ 那边的状态 dict；连不上或出错 → None。"""
    c = conf(name)
    if c.get("ssh_master") and subprocess.run(ssh_argv()[:1] + ["-O", "check", c["ssh"]], capture_output=True, timeout=30).returncode:
        return None  # 长连接断着：不另起连接（中转执行机认证失败多了会被锁）
    try:
        rs = rpy(name, STATUS_PY, dict(sched=c["scheduler"], pueue=c.get("pueue"), cap=c.get("cap")), timeout=STATUS_S)
    except RuntimeError:
        return None
    return parse_sched(name, rs)


def undeployed(name):
    """连得上、但根目录里还没有代码包（新机器第一次）→ True：先部署，再读状态。"""
    c = conf(name)
    if c.get("ssh_master") and subprocess.run(ssh_argv()[:1] + ["-O", "check", c["ssh"]], capture_output=True, timeout=30).returncode:
        return False
    return rsh(name, f"test -e {R.q(root(name))}/code/current", timeout=30)[0] == 1


def parse_sched(name, rs):
    """状态脚本读回的调度器原始输出 → rs["jobs"]（pueue：各种 worker 所在组的任务）或 rs["squeue"]（Slurm）；读不出 → None。
    一次 ssh 读完、在 box 上用适配器解析（scripts/sites/pueue.py、slurm.py）：每台每次只连一次，状态是同一时刻的。"""
    raw, c = rs.pop("sched_raw", None), conf(name)
    rs["jobs"] = rs["squeue"] = None
    if raw is not None and c["scheduler"] == "pueue":
        rs["jobs"] = PQ.parse(raw, [M.worker_of(name, k)["group"] for k in M.kinds(name)])
    elif raw is not None:
        rs["squeue"] = SL.squeue(raw)
    return rs


# ---------------- 代码包 ----------------
def deploy(name, bid, path):
    """把包推过去、切换 current（原子的 mv -T）；第一次时建目录、开队列。清理旧包时留下排着、在跑的作业还在用的（适配器的 busy_cmd），读不到就不清理。"""
    r = R.q(root(name))
    b = shlex.quote(bid)
    tar = subprocess.run(["tar", "-C", path, "-cf", "-", "."], capture_output=True, check=True).stdout
    cmd = (f"set -e; R={r}; mkdir -p $R/code $R/queue $R/blobs $R/tmp $R/state $R/data/logs/tasks $R/data/logs/workers; "
           f"[ -e $R/queue/tasks.jsonl ] || {{ : > $R/queue/tasks.jsonl; touch $R/queue/ENABLED; }}; cd $R/code; "
           f"if [ ! -d {b} ]; then rm -rf {b}.tmp; mkdir {b}.tmp; tar -xf - -C {b}.tmp; mv -T {b}.tmp {b}; fi; "
           f"ln -sfn {b} current.tmp; mv -T current.tmp current; "
           f"if busy=$({(SL if conf(name)['scheduler'] == 'slurm' else PQ).busy_cmd()}); then busy=$(printf '%s\\n' \"$busy\" | sed 's#$#/#'); "
           "ls -1t | grep -vx current | grep -v '\\.tmp$' | tail -n +4 | "
           "while read -r d; do case \"$busy\" in *\"/code/$d/\"*) continue;; esac; "
           "find \"$d\" -maxdepth 0 -mtime +4 -exec rm -rf {} +; done; fi")
    r2 = subprocess.run(ssh_argv() + [conf(name)["ssh"], C.remote_timeout(f"bash -c {shlex.quote(cmd)}", 600)], input=tar,
                        capture_output=True, timeout=600)
    if r2.returncode:
        raise RuntimeError(f"推代码包 {bid} 到 {conf(name)['label']} 出错：{r2.stderr.decode(errors='replace')[-300:]}")


def bundle(name, commit="origin/main", have=None, run_build=None):
    """origin/main 打的包和那边的 current 不同就推过去 → 说明（没变 → None）。"""
    bid, path = (run_build or RB.build)(name, commit)
    if bid == have:
        return None
    deploy(name, bid, path)
    return f"{conf(name)['label']} 的代码换成 {bid}（{commit}）"


# ---------------- 传数据（box 的 wait 组里跑） ----------------
def sha256s(paths):
    """真实路径 → sha256，按（路径、大小、修改时间）缓存在 HASHES。"""
    try:
        cache = json.load(open(HASHES))
    except (OSError, ValueError):
        cache = {}
    out, new = {}, False
    for p in paths:
        st = os.stat(p)
        k = f"{p}|{st.st_size}|{st.st_mtime_ns}"
        if k not in cache:
            h = hashlib.sha256()
            with open(p, "rb") as f:
                for blk in iter(lambda: f.read(1 << 22), b""):
                    h.update(blk)
            cache[k], new = h.hexdigest(), True
        out[p] = cache[k]
    if new:
        write_json(HASHES, cache, indent=None)
    return out


def lookup(key):
    s = S.lookup(key)
    if s is None:
        raise RN.RelayError(f"实验表和总队列里都没有 {key}")
    return s if s.get("exp") else S.resolve(s["recipe"], s["params"])


def stage(name, key, nice=None, add=True, say=print):
    """box 上的数据 → 那边（和 box 一样的相对路径）；add 时再加进那边的队列。→ key。"""
    real, D = lookup(key), box_data()
    ex = N.export(real, D)
    hs = sha256s(sorted({p for _, p in ex["files"]}))
    links = {rel: hs[p] for rel, p in ex["files"]}
    pin = S.safe(key)
    rpy(name, PIN_PY, dict(key=pin, hashes=sorted(set(links.values()))))  # 视图建好前那边的清理不删这些块（11/1）
    try:
        return _stage(name, key, nice, add, say, real, D, ex, hs, links)
    finally:
        try:
            rpy(name, PIN_PY, dict(key=pin, hashes=None))
        except RuntimeError as e:
            say(f"撤掉 {key} 的钉住记录没成（{e}），清理 12 小时后不再认它")


def check_overlap(name, key, paths):
    """这个任务要放的路径和同一台上别的没做完的任务的产出重叠（相同、上级或下级）→ RelayError，不传。"""
    keys = keys_of(load_state(), name)
    hit = {}
    for r, k in keys.items():
        if r == key:
            continue
        outs = set(k.get("outs") or [])
        anc = ancestors(outs)
        if any(overlaps(p, outs, anc) for p in paths):
            hit[rkey(r, k)] = r
    if not hit:
        return
    states = rpy(name, STATES_PY, None)
    busy = [r for rk_, r in hit.items() if states.get(rk_) not in (None, "done")]
    if busy:
        raise RN.RelayError(f"{key} 要放的输入和 {conf(name)['label']} 上还没做完的 {', '.join(busy)} 的产出重叠，先不传")


def _stage(name, key, nice, add, say, real, D, ex, hs, links):
    import tunnel
    views = [safe_rel(v) for v in ex["views"]]
    inputs = sorted({safe_rel(r) for r in list(links) + list(ex["texts"])} - {r for r in links if any(r.startswith(v + "/") for v in views)})
    outs = [safe_rel(o) for o in ex.get("outs") or N.outs(real)]
    cleanup = [safe_rel(c) for c in ex["cleanup"]]
    check_overlap(name, key, views + inputs)
    miss = rpy(name, MISSING_PY, sorted(set(links.values())))
    say(f"{key}：{len(links)} 个文件，那边缺 {len(miss)} 个")
    srcs, job = adopt_srcs(name), conf(name).get("adopt_job")
    if miss:  # 那边老数据目录、同一个盘上别的执行机、它自己的数据目录里内容一样的文件硬链接进 blobs，不重传
        by = {h: p for p, h in hs.items()}
        files = {h: (os.path.relpath(by[h], D) if by[h].startswith(D.rstrip("/") + "/") else None, os.path.getsize(by[h])) for h in miss}
        try:
            got = set(rpy(name, ADOPT_PY, dict(srcs=srcs + [dict(path=root(name) + "/data", site=False)], files=files),
                          timeout=3 * 3600, job=job))
        except NoSlot as e:
            say(f"{e}；这次不传，下次检查再来")
            return None
        miss = [h for h in miss if h not in got]
        say(f"那边已有的数据里收编了 {len(got)} 个，还缺 {len(miss)} 个")
    if miss:
        by = {h: p for p, h in hs.items()}
        os.makedirs(DIR, exist_ok=True)
        tmp = tempfile.mkdtemp(prefix="push_", dir=DIR)
        try:
            for h in miss:
                os.makedirs(os.path.join(tmp, h[:2]), exist_ok=True)
                os.symlink(by[h], os.path.join(tmp, h[:2], h))
            bad = tunnel.push(conf(name)["ssh"], tmp, root(name) + "/blobs", [f"{h[:2]}/{h}" for h in miss])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        if bad:
            raise RN.RelayError(f"{len(bad)} 个文件没传到或对不上（下次重传）")
    now = time.time()
    update_state(lambda st: st.setdefault("views", {}).setdefault(name, {}).update({v: now for v in views}))
    texts = {k: (v if isinstance(v, bytes) else v.encode()) for k, v in ex["texts"].items()}
    others = [k for r, k in keys_of(load_state(), name).items() if r != key]
    used = set().union(*[paths_of(k) for k in others]) if others else set()
    clean = [v for v in views if not overlaps(v, used, ancestors(used))]  # 视图里多出来的旧链接只在别的任务不用这个视图时删
    n = rpy(name, VIEW_PY, dict(views=views, clean=clean, links=links,
                                texts={k: base64.b64encode(v).decode() for k, v in texts.items()}))
    say(f"那边的输入放好了（{n} 个文件，视图 {'、'.join(views) or '无'}）")
    if add and srcs and real.get("out") and real.get("resume"):  # 老数据目录、别的执行机里训到一半的存档（任务说明的 resume，最新的那份）拷过来，接着训
        dirs = [f"{x['path']}/data/{real['out']}" if x["site"] else f"{x['path']}/{real['out']}" for x in srcs]
        try:
            r = rpy(name, ADOPT_OUT_PY, dict(srcs=dirs, out=real["out"], resume=real["resume"], done=real.get("done") or []), job=job)
        except NoSlot as e:
            say(f"{e}；这次不加进队列，下次检查再来")
            return None
        if r["n"]:
            say(f"{r['src']} 的存档拷过来了（{r['n']} 个文件），接着训")

    def rec(st):
        st.setdefault("keys", {}).setdefault(name, {})[key] = dict(
            recipe=real["recipe"], params=ex["params"], data=real.get("data") or [], staged=time.time(), views=views,
            inputs=inputs, outs=outs, cleanup=cleanup)
    update_state(rec)
    if add:
        r = rtq(name, f"add --nice {int(nice)} --recipe " + " ".join(shlex.quote(x) for x in S.kv_args(real["recipe"], ex["params"])))
        if r is None or r[0]:
            raise RuntimeError(f"加进 {conf(name)['label']} 的队列出错：{(r or [0, 'ssh 不通'])[1][-300:]}")
        say(r[1])
    else:  # 补传（那边查到缺输入）：撤掉标记，那边接着领
        r = rtq(name, f"input-ok {shlex.quote(key)}")
        if r is None or r[0]:
            raise RuntimeError(f"{conf(name)['label']} 上撤掉缺输入的标记出错：{(r or [0, 'ssh 不通'])[1][-300:]}")
        say(r[1] or "那边没有缺输入的标记")
    return key


# ---------------- 拉回（box 的 wait 组里跑） ----------------
def place(src, D, written, real):
    """核对过的结果（src 下，和数据目录一样的相对路径）挪进数据目录：普通文件 → out 上一级里 fetch.extra 的文件（已有的不动）→
    MANIFEST.json → 做完的标志（按 done 倒着放，第一项最后放）。"""
    import fnmatch
    rout = real["out"].strip("/")
    par = os.path.dirname(rout)
    extra = (real.get("fetch") or {}).get("extra") or []
    rank = {}
    for n, item in enumerate(real["done"]):
        for alt in item.split("|"):
            rank[f"{rout}/{alt}"] = n
    done = sorted((w for w in written if w in rank), key=lambda w: -rank[w])
    parent = [w for w in written if w not in rank and os.path.dirname(w) == par
              and any(fnmatch.fnmatch(os.path.basename(w), x) for x in extra)]
    man = [w for w in written if w == f"{rout}/MANIFEST.json" and w not in rank]
    first = [w for w in written if w not in rank and w not in parent and w not in man]

    def mv(w):
        p = os.path.join(D, w)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        os.replace(os.path.join(src, w), p)
    for w in first:
        mv(w)
    for w in parent:
        if not os.path.exists(os.path.join(D, w)):
            mv(w)
    for w in man + done:
        mv(w)


def fetch(name, key, say=print):
    import stat
    import tunnel
    real, D = lookup(key), box_data()
    k = keys_of(load_state(), name).get(key)
    if not k:
        raise RN.RelayError(f"{key} 没有传给 {conf(name)['label']} 的记录")
    res = rpy(name, LIST_PY, dict(spec=real, outs=k.get("outs") or N.outs(real), key=rkey(key, k)))
    if not res["done"]:
        raise RN.RelayError(f"{conf(name)['label']} 上 {key} 还没做完")
    inbox = os.path.join(D, ".relay_in", S.safe(key))
    out = inbox + ".real"
    shutil.rmtree(out, ignore_errors=True)
    # 上次拉到一半的（11/2）：清单里有、大小和修改时间都对上的留着（隧道核对过 MD5 才放进来），只拉其余的；别的删掉
    want = {i[0]: list(i[1:]) for i in res["items"]}
    for dp, _, fs in os.walk(inbox):
        for f in fs:
            p = os.path.join(dp, f)
            st = os.lstat(p)
            rel = os.path.relpath(p, inbox)
            if rel in want and stat.S_ISREG(st.st_mode) and [st.st_size, st.st_mtime_ns][:len(want[rel])] == want[rel]:
                want.pop(rel)
            else:
                os.remove(p)
    todo = [(r, v[0]) for r, v in want.items()]
    bad = tunnel.pull(conf(name)["ssh"], root(name) + "/data", inbox, todo) if todo else []
    if bad:
        raise RN.RelayError(f"{len(bad)} 个文件没拉到（{bad[:3]}）")
    written = N.import_(real, inbox, out, D)
    place(out, D, written, real)
    for d in (inbox, out):
        shutil.rmtree(d, ignore_errors=True)
    say(f"{key} 拉回了 {len(written)} 个文件")


# ---------------- 每次检查 ----------------
def box_task(s, step, key, since=0):
    """box 上 step 这一步、这个 key 的最新任务（任务记录在 since 之后加的）→ pueue 任务或 None。"""
    hub, last = M.hub(), None
    for t in sorted((s["pueue"].get(hub) or {}).get("tasks", []), key=lambda t: t["id"]):
        r = C.identify(hub, t, s.get("records") or [])
        if r and r["step"] == step and r["key"] == key and r.get("added", 0) >= since - 60:
            last = t
    return last


def progress(f):
    """训完、有做完的轮数，或者有存档（任务说明的 resume）→ 有进度。"""
    return bool(f) and bool(f.get("final") or f.get("resumable")
                            or any((r.get("epochs_done") or 0) > 0 for r in f.get("runs") or []))


def plan(s, name, rs, st, now=None):
    """→ (动作, 卡住的 {问题编号: 说明})。rs：那边的状态（None = 连不上）；st：load_state()。
    动作 dict(op=beat|release|stage|restage|redo|fetch|drop|unq|rstop|rresume|rnice|archive, key, …)；rkey：那边队列里的 key。"""
    now = now or time.time()
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    L, keys = conf(name)["label"], keys_of(st, name)
    mine = [t for t in (mtq or {}).get("tasks", []) if t.get("worker") == name]
    acts, stuck = [], {}
    if not mtq or not mtq.get("enabled"):
        return [], {}
    if rs is not None and "error" in (s.get("pueue", {}).get(M.hub()) or {"error": 1}):
        rs = None  # box 的 pueue 读不到：在跑的拉回、归档看不见，不能当成没有，只续约（11/3）
    if rs is None:
        acts = [dict(op="beat", key=t["key"]) for t in mine if t["state"] != "done"]
        last = (st.get("last_ok") or {}).get(name, 0)
        if acts and now - last > DOWN_S:
            stuck[f"site:{name}:down"] = (f"{L} 读不到状态{f' {(now - last) / 3600:.0f} 小时' if last else ''}（ssh 不通，或那边的存储挂住），"
                                           f"领着 {len(acts)} 个任务，那边的进度看不到")
        return acts, stuck
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}
    tasks = rs.get("tasks") or {}
    owned = set()
    for t in mine:
        key = t["key"]
        k = keys.get(key)
        a = rt.get(rkey(key, k)) if k else None
        if a:
            owned.add(a["key"])
        if k and k.get("redo") and t["state"] != "done":  # 删掉那边的任务和路径，下次检查按现在的做法重新传；那边领着时先停
            busy = bool(a and a.get("worker"))
            acts += [dict(op="beat", key=key), dict(op="redo", key=key, rkey=rkey(key, k), busy=busy)]
            if busy and a["state"] != "stopped":
                acts.append(dict(op="rstop", key=key, rkey=a["key"]))
            continue
        if t["state"] == "done":
            ft = box_task(s, "relay_fetch", key, t.get("since") or 0)
            at = box_task(s, "relay_archive", key, t.get("since") or 0)
            if any(x is not None and x["status"] in LIVE for x in (ft, at)):  # box 上有了，但拉回、归档还没做完：等
                acts.append(dict(op="beat", key=key))
                continue
            # 总队列显示训完：box 上有了（结果本身，不看拉回记录）。只有单独的拉回出错时才报（11/3 起拉回不带归档）
            fcmd = (ft or {}).get("command") or ""
            if ft is not None and ft["status"] != "Success" and "archive_task.sh" not in fcmd:
                acts.append(dict(op="beat", key=key))
                stuck[f"relayfetch:{key}"] = (f"从 {L} 拉回 {key} 的 box 任务 {ft['id']} 出错了（{ft['status']}），领取保留；"
                                              f"看 pueue log {ft['id']}，处理后 pueue remove {ft['id']}（下次检查重排）")
                continue
            if k:  # box 上有了就删那边的任务和结果，不等归档（11/3 定：执行机的盘只用来中转）
                acts.append(dict(op="drop", key=key, rkey=rkey(key, k)))
            try:
                want = ft is not None and bool(lookup(key).get("archive"))
            except (RN.RelayError, S.SpecError):
                want = False
            archived = (at is not None and at["status"] == "Success") or (ft is not None and ft["status"] == "Success" and "archive_task.sh" in fcmd)
            if want and not archived:
                if at is not None:
                    acts.append(dict(op="beat", key=key))
                    stuck[f"relayfetch:{key}"] = (f"{key} 归档到备份机的 box 任务 {at['id']} 出错了（{at['status']}），box 上有了、领取保留；"
                                                  f"看 pueue log {at['id']}，处理后 pueue remove {at['id']}（下次检查重排）")
                    continue
                if not C.backup_down():  # 备份机能用：box 单独排归档，归档成功才交还；只是这次连不上时下次再排
                    acts.append(dict(op="beat", key=key))
                    if not C.backup_down(s.get("ok")):
                        acts.append(dict(op="archive", key=key))
                    continue
            acts.append(dict(op="release", key=key, why="训完了，box 上有了" + ("、归档了" if archived else "（没有归档）")))
            continue
        acts.append(dict(op="beat", key=key))
        if t["state"] == "stopped" and not (a and a["state"] == "done"):
            if a and a["state"] != "stopped":
                acts.append(dict(op="rstop", key=key, rkey=a["key"]))
            elif not (a and a.get("worker")):  # 那边不再领着（或者还没放过去）
                acts[-1] = dict(op="release", key=key, why="停止（taskq.py stop）" + ("，那边的结果留着" if a else ""))
            continue
        if a and a["state"] not in ("done", "failed", "bad") and a.get("nice") is not None and a["nice"] != t["nice"]:
            acts.append(dict(op="rnice", key=key, rkey=a["key"], nice=t["nice"]))  # 总队列里改了档位（set-nice）：那边跟着改（11/3）
        since = t.get("since") or 0
        if a is None:
            sg = box_task(s, "relay_stage", key, since)
            noslot = sg is not None and sg["status"] not in LIVE and sg.get("code") == NOSLOT_RC
            if sg is None or sg["status"] == "Success" or noslot:
                acts.append(dict(op="stage", key=key, nice=t["nice"]))
                if noslot and now - since > NOSLOT_ALERT_S:
                    stuck[f"site:{name}:stage"] = (f"{L} 的 CPU 作业 {(now - since) / 3600:.0f} 小时都排不上，{key} 的数据传不过去"
                                                   f"（box 任务 {sg['id']}，每次检查再试）")
            elif sg["status"] not in LIVE:
                stuck[f"relaystage:{key}"] = (f"给 {L} 传 {key} 的 box 任务 {sg['id']} 出错了（{sg['status']}），领取保留；"
                                              f"看 pueue log {sg['id']}，处理后 pueue remove {sg['id']}（下次检查重传）")
            continue
        ast = a["state"]
        if ast == "done" and a.get("worker"):
            pass  # 做完的标志有了、worker 还领着：训练命令还在写结果，只续约，交还以后再拉回（11/3）
        elif ast == "done":
            ft = box_task(s, "relay_fetch", key, since)
            if ft is None:
                acts.append(dict(op="fetch", key=key))
            elif ft["status"] == "Success":
                end = C.local_time(ft.get("end"))
                if end is not None and now - end.timestamp() > LATE_S:
                    stuck[f"relayfetch:{key}"] = (f"{L} 训完的 {key} 拉回并归档成功了（box 任务 {ft['id']}），box 上却还不算训完；"
                                                  f"看 box 上的结果文件夹")
            elif ft["status"] not in LIVE:
                stuck[f"relayfetch:{key}"] = (f"从 {L} 拉回 {key} 的 box 任务 {ft['id']} 出错了（{ft['status']}），领取保留；"
                                              f"看 pueue log {ft['id']}，处理后 pueue remove {ft['id']}（下次检查重排）")
        elif ast == "stopped":
            acts.append(dict(op="rresume", key=key, rkey=a["key"]))
        elif ast == "input":  # 那边开训前查到输入不全（taskq.py missing）：只补传缺的、重建视图，再撤掉标记（11/1）
            sg = box_task(s, "relay_stage", key, a.get("input_since") or since)
            if sg is None or sg["status"] == "Success" or (sg["status"] not in LIVE and sg.get("code") == NOSLOT_RC):
                acts.append(dict(op="restage", key=key))
            elif sg["status"] not in LIVE:
                stuck[f"relaystage:{key}"] = (f"给 {L} 补传 {key} 缺的输入的 box 任务 {sg['id']} 出错了（{sg['status']}），领取保留；"
                                              f"看 pueue log {sg['id']}，处理后 pueue remove {sg['id']}（下次检查重传）")
        elif ast == "failed":
            if progress(tasks.get(a["key"])):
                stuck[f"site:{name}:fail:{key}"] = (f"{L} 上 {key}（{a['key']}）训练失败 {a.get('failures')} 次，有进度，领取保留；"
                                                    f"看那边的 logs/tasks/；放弃：taskq.py release --worker {name} {key}")
            else:
                acts[-1] = dict(op="release", key=key, skip=f"{L} 上训练失败", why=f"{L} 上训练失败、没有进度，交还，{L} 不再领它")
                acts.append(dict(op="drop", key=key, rkey=a["key"]))
        elif ast in ("bad", "blocked"):
            stuck[f"site:{name}:fail:{key}"] = f"{L} 队列里的 {a['key']}（{key}）{ast}：{a.get('note')}"
    rev = {rkey(k, v): k for k, v in keys.items()}
    mt = {t["key"]: t for t in (mtq or {}).get("tasks", [])}
    mk = {t["key"] for t in mine}
    for real, k in keys.items():  # 不归它了、要重新传的旧记录：那边没人领着就删掉（任务、独占的路径、记录）
        a = rt.get(rkey(real, k))
        if k.get("redo") and real not in mk:
            owned.add(rkey(real, k))
            if not (a and a.get("worker")):
                acts.append(dict(op="drop", key=real, rkey=rkey(real, k), gone=a is None))
            elif a["state"] != "stopped":
                acts.append(dict(op="rstop", key=real, rkey=a["key"]))
    for a in rs["taskq"]["tasks"]:
        if a["key"] in owned:
            continue
        real = rev.get(a["key"])
        m = mt.get(real) if real else None
        if real is None:
            stuck[f"site:{name}:orphan:{a['key']}"] = f"{L} 队列里的 {a['key']} 认不出是哪个任务（{DIR}/state.json 里没有）"
        elif a["state"] in ("claimed", "busy"):
            stuck[f"site:{name}:orphan:{real}"] = f"{L} 在训 {real}，但总队列里不是它领着；看总队列和 {L} 的队列"
        elif m is None or m["state"] == "done":
            acts.append(dict(op="drop", key=real, rkey=a["key"]))
        elif m.get("worker") and m["worker"] != name:  # 改分给了别的执行机（那边停着的也算，11/2）：结果留给它收编
            acts.append(dict(op="unq", key=real, rkey=a["key"], to=m["worker"]))
        elif a["state"] != "stopped":
            acts.append(dict(op="rstop", key=real, rkey=a["key"]))
    for real, k in keys.items():  # 那边队列里已经删了（unq）、只剩结果的任务：总队列里训完了再删结果
        m = mt.get(real)
        if not k.get("redo") and real not in mk and rkey(real, k) not in rt and k.get("unq") and (m is None or m["state"] == "done"):
            acts.append(dict(op="drop", key=real, rkey=rkey(real, k), gone=True))
    if rs["taskq"].get("paused"):
        stuck[f"site:{name}:fail"] = f"{L} 的任务队列 3 小时内失败 {rs['taskq'].get('recent_failures')} 次，暂停领任务"
    return acts, stuck


def fill(s, name, rs, st, acts):
    """那边备好、没人领的任务不够 READY 个时领下一个任务（每种 worker 各备 READY 个，envs/machines.py 的 kinds）→ take 动作；
    不领 → ([], 原因)。"""
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    if not mtq or not mtq.get("enabled") or mtq.get("paused"):
        return [], "总队列关着或暂停中"
    if not shift.allowed(name):
        return [], "不在时段里（envs/policy.py 的 SHIFT）"
    if "error" in (s.get("pueue", {}).get(M.hub()) or {"error": 1}):  # 传数据要在 box 的 pueue 排，在跑的看不见（11/3）
        return [], "box 的 pueue 读不到"
    if any(n not in s["src"] and n not in P.OFFLINE for n in M.names("train")):  # 暂停检查的机器不算
        return [], "有训练机的进度读不到"
    want, kinds = P.READY.get(name, 1), M.kinds(name)
    keys = keys_of(st, name)
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}

    def remote(t):
        return rt.get(rkey(t["key"], keys[t["key"]])) if t["key"] in keys else None

    def waiting(t):  # 领着、还没开训：在传数据，在那边排着，或那边停着、这次检查要撤销停止的（11/2）
        a = remote(t)
        return a is None or a["state"] in ("free", "stale", "waiting", "stopped", "input")

    def idle(t):  # 那边备好了、没人领：可以让给更优先的任务（11/2）
        a = remote(t)
        return a is not None and a["state"] == "free" and not a.get("worker")

    def kind(t):
        return refill.info(mtq, t["key"])["spec"].get("device", "gpu")
    have, held = {k: 0 for k in kinds}, []
    for t in mtq.get("tasks", []):
        if t.get("worker") == name and t["state"] not in ("done", "stopped") and waiting(t):
            have[kind(t)] = have.get(kind(t), 0) + 1
            held.append(t)
    ready = {p for k in keys.values() for p in k.get("data") or []}
    cands = [t for t in refill.candidates(s, name, mtq, ready, rs.get("envs_ok"), kinds=kinds)
             if relayable(refill.info(mtq, t["key"]))]
    urgent = [t for t in cands if t["nice"] < 0]  # 第 ⓪ 档（训完后的推理等）不受 READY 限制（11/1）
    room = {k: want - have[k] for k in kinds}
    disk = (conf(name)["ssh"], rs.get("dev"))
    free = rs.get("free", 0) / 1e9 - PLANNED.get(disk, 0)
    hard = (conf(name).get("budget") or (None, HARD_GB))[1]
    out, took = [], set()
    for k in kinds:  # 备满了、总队列里有更优先的：每次检查把一个最靠后、备好没人领的交还，换它进来（11/2）
        idle_k = sorted((t for t in held if kind(t) == k and idle(t)), key=lambda t: (-t["nice"], -t["order"]))
        better = [t for t in cands if kind(t) == k and t["nice"] >= 0 and idle_k and t["nice"] < idle_k[0]["nice"]]
        if room[k] <= 0 and better and refill.next_pick(mtq, better, ready, free, hard, took)[0] is not None:
            w = idle_k[0]
            out += [dict(op="drop", key=w["key"], rkey=rkey(w["key"], keys[w["key"]])),
                    dict(op="release", key=w["key"], why=f"让给更优先的 {better[0]['key']}（档位 {better[0]['nice']} < {w['nice']}）")]
            room[k] = 1
    if all(n <= 0 for n in room.values()) and not urgent:
        return [], f"那边排着 {sum(have.values())} 个"
    for pool, n in [(urgent, len(urgent))] + [([t for t in cands if kind(t) == k], room[k]) for k in kinds]:
        for _ in range(n):
            t, need, gb, _ = refill.next_pick(mtq, pool, ready, free, hard, took)
            if t is None:
                break
            free -= gb
            PLANNED[disk] = PLANNED.get(disk, 0) + gb
            took.add(t["key"])
            out.append(dict(op="take", key=t["key"], nice=t["nice"]))
    if out:
        return out, ""
    open_ = urgent or any(room[k] > 0 and kind(t) == k for t in cands for k in kinds)
    return out, ("队列里没有能领的任务" if not cands else f"放不下（那边剩 {rs.get('free', 0) / 1e9:.0f} GB）" if open_
                 else f"那边排着 {sum(have.values())} 个")


def kname(kind):
    return "" if kind == "gpu" else f"{kind.upper()} "


def wname(kind):
    return f"{kname(kind)}worker"


def kkey(base, kind):
    """问题编号：GPU 的照旧，别的种类后面加 :<种类>。"""
    return base if kind == "gpu" else f"{base}:{kind}"


def sub_kind(x):
    """topup 返回的一项 → 种类。"""
    return x[1] if x[0] == "pueue" else "cpu" if x[3] == refill.CPU_FILE else "gpu"


def topup(name, rs, run=None):
    """→ 要开的 worker, 不补的原因。"要几个"只在这里算，适配器只读状态、照着开。按这台能跑的种类
    （envs/machines.py 的 kinds）各算各的：
    pueue 类：这种 worker 没有活着的、上一个没失败、那边有备好没人领的这种任务时开一个 → ("pueue", 种类, 1)；
    Slurm 类：envs/policy.py 的 SLURM 里这台能跑的种类，按 refill.slurm_plan 交 → (QOS, 时限, 交几个, sbatch 文件)。
    run：fn(名字, 命令) → (退出码, 输出, 错误)，测试替换。"""
    if not shift.allowed(name):
        return [], "不在时段里（envs/policy.py 的 SHIFT）"
    if conf(name)["scheduler"] == "pueue":
        if rs.get("jobs") is None:
            return [], "读不到 pueue"
        subs, why = [], []
        for k in M.kinds(name):
            mine = PQ.workers(name, rs["jobs"], k)
            if any(j[2] != "Done" for j in mine):
                why.append(f"{wname(k)} 在")
            elif mine and mine[-1][3] not in ("Success", "Killed"):
                why.append(f"上一个 {wname(k)}（pueue 任务 {mine[-1][0]}）失败了（{mine[-1][3]}）")
            elif not refill.unclaimed(rs["taskq"], k):
                why.append(f"那边没有备好、没人领的{' ' + kname(k) if kname(k) else ''}任务")  # worker 空闲 30 分钟自己退出，有了再开
            else:
                subs.append(("pueue", k, 1))
        return subs, "；".join(why)
    conf_ = {k: v for k, v in (P.SLURM.get(name) or {}).items() if k in M.kinds(name)}
    if not conf_:
        return [], "envs/policy.py 的 SLURM 里没有它"
    st = SL.status(name, lambda c: (lambda r: (r[0], r[1] + r[2]))((run or rsh)(name, c)))
    if st is None:
        return [], "读不到作业数"
    return refill.slurm_plan(rs["taskq"], st, conf_)


def label(op, key):
    return f"{op}:{key}"


def execute(name, acts, s, run_add=None):
    """做动作 → [说明]。出错的那一步写进说明，后面的照做。"""
    L, msgs = conf(name)["label"], []
    add = run_add or refill.add

    def master(args):
        r = taskq.central(args)
        return r[0], r[1] + r[2]
    beats = [a["key"] for a in acts if a["op"] == "beat"]
    if beats:
        r = master(f"beat --worker {name} " + " ".join(shlex.quote(k) for k in beats))
        if r is None or r[0]:
            msgs.append(f"{L} 的领取续不上（{(r or [0, 'ssh 不通'])[1][-150:]}）")
    kept = set()  # 那边没删掉的：总队列里不交还（交还了别处会再领，两处一起训）
    for a in acts:
        k = a["key"]
        try:
            if a["op"] == "release" and k in kept:
                msgs.append(f"{L} 上 {k} 没删掉，先不交还")
            elif a["op"] == "release":
                r = master(f"release --worker {name} {shlex.quote(k)}" + (f" --skip {shlex.quote(a['skip'])}" if a.get("skip") else ""))
                msgs.append(f"{L} 交还 {k}：{a['why']}" if r and r[0] == 0 else f"{L} 交还 {k} 出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
            elif a["op"] == "take":
                r = master(f"take --worker {name} --assign {shlex.quote(k)}")
                if r is None or r[0]:
                    msgs.append(f"{L} 没领到 {k}：{(r or [0, 'ssh 不通'])[1].strip()[-150:]}")
                    continue
                msgs.append(f"{L} 领了 {k}（档位 {a['nice']}）")
                for t in (s.get("taskq", {}).get(taskq.queue_host()) or {}).get("tasks", []):  # 这次检查里后面的执行机别再挑它
                    if t["key"] == k:
                        t.update(worker=name, assigned=name, state="claimed")
                a = dict(a, op="stage")
            if a["op"] in ("stage", "restage"):
                lab = label("relay_stage", k)
                cmd = f"python3 scripts/queue/relay.py stage {name} {shlex.quote(k)} " + (
                    "--no-add" if a["op"] == "restage" else f"--nice {int(a['nice'])}")
                tid = add(M.hub(), lab, "relay_stage", k, f"wait -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group="wait")
                msgs.append(f"{L} 的 {k} 排了{'补传缺的输入' if a['op'] == 'restage' else '传数据'}（box 任务 {tid}）")
            elif a["op"] == "fetch":
                lab = label("relay_fetch", k)
                cmd = f"python3 scripts/queue/relay.py fetch {name} {shlex.quote(k)}"
                tid = add(M.hub(), lab, "relay_fetch", k, f"wait -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group="wait")
                msgs.append(f"{L} 训完了 {k}，排了拉回（box 任务 {tid}）")
            elif a["op"] == "archive":
                lab = label("relay_archive", k)
                cmd = f"bash scripts/queue/archive_task.sh {M.hub()} {shlex.quote(k)}"
                tid = add(M.hub(), lab, "relay_archive", k, f"wait -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group="wait")
                msgs.append(f"{L} 训完的 {k} 排了归档（box 任务 {tid}）")
            elif a["op"] == "unq":
                r = rtq(name, f"remove {shlex.quote(a['rkey'])}")
                if r and r[0] == 0:
                    update_state(lambda st: ((st.get("keys") or {}).get(name, {}).get(k) or {}).update(unq=time.time()))
                    msgs.append(f"{L} 的队列里删了 {k}（总队列里分给了 {a['to']}，结果留着给它收编）")
                else:
                    msgs.append(f"{L} 的队列里删 {k} 出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
            elif a["op"] in ("drop", "redo"):
                if a["op"] == "redo" and a.get("busy"):
                    msgs.append(f"{L} 上 {k} 正被领着，等它停下再重新传")
                    continue
                err = drop_remote(name, k, a["rkey"], gone=a.get("gone"))
                if err:
                    msgs.append(f"{L} 上删 {k} 出错（{err}）")
                    kept.add(k)
                else:
                    msgs.append(f"{L} 上删了 {k} 的任务和结果" if a["op"] == "drop" else f"{L} 上删了 {k} 的旧任务和路径，下次检查重新传")
            elif a["op"] == "rnice":
                r = rtq(name, f"set-nice --nice {int(a['nice'])} {shlex.quote(a['rkey'])}")
                msgs.append(f"{L} 上 {k} 改到档位 {a['nice']}" if r and r[0] == 0 else f"{L} 上 {k} 改档位出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
            elif a["op"] in ("rstop", "rresume"):
                r = rtq(name, ("stop --why 总队列里停了 " if a["op"] == "rstop" else "resume ") + shlex.quote(a["rkey"]))
                what = "停" if a["op"] == "rstop" else "撤销停止"
                msgs.append(f"{L} 上{what} {k}" if r and r[0] == 0 else f"{L} 上{what} {k} 出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
        except Exception as e:  # noqa: BLE001
            msgs.append(f"{L} {a['op']} {k} 出错：{e}")
            if a["op"] in ("drop", "redo"):
                kept.add(k)
    return msgs


def drop_remote(name, key, rk, gone=False):
    """删那边的任务（taskq.py remove 在锁里查过没人领着），再删这个任务占的、别的任务不用的路径和日志，最后删记录 → 出错说明或 None。"""
    if not gone:
        r = rtq(name, f"remove {shlex.quote(rk)}")
        if r and r[0] and "不删" not in r[1] and "队列里没有" in r[1]:
            r = (0, r[1])  # 上次删了任务、没删完路径
        if not r or r[0]:
            return f"任务没删掉，路径留着：{(r or [0, 'ssh 不通'])[1][-150:]}"
    keys = keys_of(load_state(), name)
    paths = droppable(keys, key, paths_of(keys.get(key) or {}))
    rpy(name, DROP_PY, dict(paths=paths, logs=S.safe(rk)))
    update_state(lambda st: (st.get("keys") or {}).get(name, {}).pop(key, None))
    return None


def submit(name, subs):
    """照 topup 算好的开 worker（scripts/sites/pueue.py、slurm.py）→ (说明, 出错的 {问题编号: 说明})。"""
    L, msgs, stuck = conf(name)["label"], [], {}
    run = lambda c: (lambda r: (r[0], r[1] + r[2]))(rsh(name, c))  # noqa: E731
    if conf(name)["scheduler"] == "pueue":
        for _, k, n in subs:
            for _ in range(n):
                ok, out = PQ.start_worker(name, run, k)
                if ok:
                    msgs.append(f"{L} 开了 {wname(k)}（{out[-80:]}）")
                else:
                    stuck[kkey(f"site:{name}:worker", k)] = f"{L} 开 {wname(k)} 出错：{out}"
        return msgs, stuck
    cwd = R.q(root(name)) + "/code/current/scripts/queue"
    for r in SL.start_workers(cwd, subs, run):
        k = sub_kind((r["qos"], r["time"], r["n"], r["file"]))
        if r["err"] is not None:
            stuck[kkey(f"topup:{name}:{r['qos']}", k)] = f"{L} {r['qos']} 补交 {wname(k)} 作业出错：{r['err']}"
        if r["ids"]:
            msgs.append(f"{L} {r['qos']} 交了 {len(r['ids'])} 个 {wname(k)} 作业 {'、'.join(r['ids'])}（每个 {r['time']}）")
    return msgs, stuck


def write_status(name, rs, st):
    keys = keys_of(st, name)
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}
    out = {}
    for real, k in keys.items():
        a = rt.get(rkey(real, k))
        if a:
            f = (rs.get("tasks") or {}).get(rkey(real, k)) or {}
            out[real] = dict(state=a["state"], worker=a.get("worker"), runs=f.get("runs") or [], final=f.get("final"),
                               debug=f.get("debug") or {})
    try:
        allst = json.load(open(STATUS))
    except (OSError, ValueError):
        allst = {}
    allst[name] = dict(time=time.time(), label=conf(name)["label"], tasks=out, squeue=rs.get("squeue"), free=rs.get("free"),
                       bundle=rs.get("bundle"))
    write_json(STATUS, allst)


def log(lines):
    if lines:
        with open(LOG, "a") as fo:
            fo.writelines(f"{time.strftime('%F %T')} {x}\n" for x in lines)


def run_all(s, dry=False):
    """health.py 调用 → (说明, 卡住的 {问题编号: 说明})。"""
    events, stuck = [], {}
    PLANNED.clear()
    for name in M.RELAY:
        if name in P.OFFLINE:
            continue
        try:
            ev, st_ = run_one(s, name, dry)
        except Exception as e:  # noqa: BLE001
            ev, st_ = [], {f"relay:{name}": f"scripts/queue/relay.py（{name}）出错：{type(e).__name__}: {e}"[:300]}
        events += ev
        stuck.update(st_)
    return events, stuck


def run_one(s, name, dry):
    live = bool(P.AUTO.get(name))
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    mine = [t for t in (mtq or {}).get("tasks", []) if t.get("worker") == name]
    L, st = conf(name)["label"], load_state()
    if not mine and not live and not dry:
        return [], {}
    if not s["ok"].get(M.hub()) or not mtq:
        return [], {}
    rs = remote_status(name)
    notes = []
    fresh = rs is None and not dry and (live or mine) and undeployed(name)
    if fresh:
        rs = {"bundle": None}
    if not dry and (live or mine) and rs is not None:
        try:
            commit = subprocess.run(["git", "-C", REPO, "rev-parse", "origin/main"], capture_output=True, text=True).stdout.strip()
            if fresh or rs.get("bundle") != st.get("bundle_for", {}).get(f"{name}:{commit}"):  # 每台的包不同（机器条目、开关）
                bid, path = RB.build(name, commit or "origin/main")
                update_state(lambda x: x.setdefault("bundle_for", {}).update({f"{name}:{commit}": bid}))
                if rs.get("bundle") != bid:
                    try:
                        deploy(name, bid, path)
                        notes.append(f"{L} 的代码换成 {bid}")
                        rs = remote_status(name)
                    except RuntimeError as e:
                        notes.append(str(e))
                        rs = None  # 那边还是旧包，和这边的命令、路径可能对不上：这次当连不上，只续约，下次再推（11/3）
        except Exception as e:  # noqa: BLE001
            notes.append(f"{L} 打包出错：{e}")
    if fresh and rs == {"bundle": None}:
        rs = None
    if rs is not None and not dry:
        update_state(lambda x: x.setdefault("last_ok", {}).update({name: time.time()}))
        write_status(name, rs, load_state())
    if rs is not None and not dry and live and conf(name).get("budget"):
        try:
            st = load_state()
            reg = (st.get("views") or {}).get(name) or {}
            keep = {v for k in keys_of(st, name).values() for v in (k.get("outs") or [] if k.get("unq") else paths_of(k))}
            keep |= {v for v, t in reg.items() if time.time() - t < VIEW_KEEP_S}
            g = relay_gc.gc(name, keep, rpy, views=reg, siblings=[x["path"] for x in M.adopt_sources(name) if x["site"]],
                            free=rs.get("free") if conf(name).get("cap") else None)
            if g and g["removed_views"]:
                update_state(lambda x: [((x.get("views") or {}).get(name) or {}).pop(v, None) for v in g["removed_views"]])
            if g and (g["removed_views"] or g["removed_blobs"]):
                notes.append(f"{L} 清理：删了 {len(g['removed_views'])} 个数据集视图、{g['removed_blobs']} 个 blob，"
                             f"放出 {g['freed'] / 1e9:.0f} GB，剩 {g['free'] / 1e9:.0f} GB")
                rs = remote_status(name) or rs
        except Exception as e:  # noqa: BLE001
            notes.append(f"{L} 清理出错：{e}")
    acts, stuck = plan(s, name, rs, st)
    if not live:  # 开关关着：不传、不重传、不让那边接着训；训完的照常拉回、交还
        acts = [a for a in acts if a["op"] not in ("stage", "restage", "redo", "rresume")]
    more, why = (fill(s, name, rs, st, acts) if rs is not None else ([], "连不上"))
    subs, swhy = (topup(name, rs) if rs is not None else ([], "连不上"))
    for part in swhy.split("；"):
        k = next((k for k in M.kinds(name) if part.startswith(f"上一个 {wname(k)}（")), None)
        if k:
            stuck[kkey(f"site:{name}:worker", k)] = f"{L} {part}；看那边的 pueue log，处理后 pueue remove 那个任务（下次检查重开）"
    todo = [a for a in acts if a["op"] != "beat"]
    desc = "；".join(f"{a['op']} {a['key']}" for a in todo + more)
    sdesc = "；".join(f"交 {x[0]} {x[2]} 个 {wname(sub_kind(x))}" for x in subs)
    if dry:
        return [f"{L}：会 {desc}" if desc else f"{L}：不动（{why}）",
                f"{L}：会 {sdesc}" if sdesc else f"{L}：不交作业（{swhy}）"] + notes, stuck
    msgs = notes + execute(name, acts, s)
    if live:
        msgs += execute(name, more, s)
        m2, st2 = submit(name, subs)
        msgs += m2
        stuck.update(st2)
    elif (more or subs) and C.changed(f"relay_{name}", desc + sdesc):
        log([f"（开关没开）{L} 会：{'；'.join(x for x in (desc, sdesc) if x)}"])
    log(msgs)
    return msgs, stuck


def main(argv):
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")
    p = sub.add_parser("stage")
    p.add_argument("name")
    p.add_argument("key")
    p.add_argument("--nice", type=int)
    p.add_argument("--no-add", action="store_true")
    p = sub.add_parser("fetch")
    p.add_argument("name")
    p.add_argument("key")
    p = sub.add_parser("bundle")
    p.add_argument("name")
    p.add_argument("--commit", default="origin/main")
    p = sub.add_parser("status")
    p.add_argument("name")
    a = ap.parse_args(argv)
    if a.cmd == "stage":
        if a.nice is None and not a.no_add:
            ap.error("要给 --nice（或 --no-add）")
        if stage(a.name, a.key, a.nice, add=not a.no_add) is None:
            return NOSLOT_RC
    elif a.cmd == "fetch":
        fetch(a.name, a.key)
    elif a.cmd == "bundle":
        rs = remote_status(a.name)
        print(bundle(a.name, a.commit, (rs or {}).get("bundle")) or "没变")
    elif a.cmd == "status":
        rs = remote_status(a.name)
        if rs is None:
            print("读不到")
            return 1
        for t in rs["taskq"]["tasks"]:
            print(f"  {t['nice']:>5} {t['key']:50} {t['state']} {t['note']}")
        print(f"剩 {rs['free'] / 1e9:.0f} GB；代码 {rs['bundle']}；作业 {rs['squeue']}")
    else:
        import health as H
        ev, st = run_all(H.gather(), dry=True)
        print("\n".join(ev + [f"卡住：{v}" for v in st.values()]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
