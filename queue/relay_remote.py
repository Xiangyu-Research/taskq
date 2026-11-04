"""relay 执行机那边：经 ssh 跑命令（rsh、那边的 taskq.py、一段 Python）和发过去跑的小程序（状态、收编、视图、列结果、删除、清理）。
box 上的记账见 relay_store.py，传数据和拉回见 relay_transfer.py，每次检查的计划见 relay.py。

那边（根目录 R，machines.py 的 root）：
  R/code/<包名>、R/code/current   scripts/taskq/queue/relay_bundle.py 打的代码包（origin/main 变了就重打、切换 current；旧包留最新 3 个，
                                  4 天以上的删）；worker 和 DICC 同一套脚本（作业名、资源、脚本名分开，见 machines.py 的 RELAY）
  R/queue/                        它自己的任务队列（taskq.py，key 和总队列相同）
  R/blobs/<id[:2]>/<id>           数据文件，id = sha256（内容相同的只传一次）
  R/data/                         数据目录，相对路径和 box 的一样：每个任务的输入是指向 blobs 的软链接（和插件生成的小文件，
                                  scripts/taskq/sites/names.py），产出照常写。state.json 记下每个任务放了哪些路径（视图、输入、产出），
                                  删一个任务时只删别的任务不用的（相同、上级、下级都算在用）
"""
import json
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
cfg.use_config()
sys.path.insert(0, os.path.join(CORE, "sites"))
import machines as M  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402
import remote as R  # noqa: E402
import slurm as SL  # noqa: E402

JOB_WAIT = 600  # 在 CPU 作业里跑（machines.py 的 adopt_job）时最多等这么久排上


class NoSlot(RuntimeError):
    """CPU 作业这次排不上。"""


def conf(name):
    return M.RELAY[name]


def root(name):
    return conf(name)["root"].rstrip("/")


def py(name):
    return conf(name)["conda"][0] + "/bin/python3"


def ssh_argv():
    return shlex.split(os.environ.get("RELAY_SSH", "ssh")) + C.SSH[1:]  # RELAY_SSH 只给测试用


def rsh(name, cmd, input=None, timeout=300):
    return C.run(cmd, conf(name)["ssh"], timeout, input=input, ssh=ssh_argv())


def prefix(name):
    r = R.q(root(name))
    return f"cd {r}/code/current && TASKQ_DIR={r}/queue TASKQ_DATA={r}/data"


def rtq(name, args, timeout=180):
    """那边的 taskq.py <args> → (退出码, 输出)；连不上 → None。"""
    rc, out, err = rsh(name, f"{prefix(name)} {py(name)} scripts/taskq/queue/taskq.py {args}", timeout=timeout)
    return None if rc in (124, 255) else (rc, (out + err).strip())


def rpy(name, code, data=None, timeout=600, job=None):
    """在那边跑一段 Python（第一个参数是根目录，data 以 JSON 经标准输入）→ 它 json_out 的结果。出错 → RuntimeError。
    job：Slurm 的 srun 参数（machines.py 的 adopt_job），在 CPU 节点上跑；排不上 → NoSlot。"""
    cmd = f"{prefix(name)} {py(name)} -c {shlex.quote(code)} {R.q(root(name))}"
    if job:
        cmd = f"srun -I{JOB_WAIT} {job} -J {SL.jobname(conf(name).get('job_names'))} bash -c {shlex.quote(cmd)}"
        timeout += JOB_WAIT
    rc, out, err = rsh(name, cmd, input=json.dumps(data), timeout=timeout)
    if job and rc and S.json_in(out) is None and any(x in err for x in ("Unable to allocate", "Immediate", "revoked")):
        raise NoSlot(f"{conf(name)['label']} 的 CPU 作业排不上：{err.strip()[-200:]}")
    res = S.json_in(out)
    if rc or res is None:
        raise RuntimeError(f"{conf(name)['label']} 上出错（{rc}）：{(err or out).strip()[-300:]}")
    return res


HEAD = "import json,os,sys\nsys.path[:0]=['scripts/taskq/queue','scripts/queue']\nimport spec\nR=sys.argv[1]\nA=json.load(sys.stdin)\n"

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
out_used=None
if A.get('cap'):
    used,seen,out_used=0,set(),0
    for dp,dns,fns in os.walk(R):
        top=os.path.relpath(dp,R).split(os.sep)[0]
        for f in fns:
            x=os.lstat(os.path.join(dp,f))
            if x.st_nlink>1:
                if (x.st_dev,x.st_ino) in seen:
                    continue
                seen.add((x.st_dev,x.st_ino))
            if not os.path.islink(os.path.join(dp,f)):
                used+=x.st_blocks*512
                if top not in ('blobs','code'):
                    out_used+=x.st_blocks*512
    free=min(free,max(0,int(A['cap']*1e9)-used))
B=os.path.realpath(os.path.join(R,'blobs'))
bdev=os.stat(B).st_dev if os.path.isdir(B) else os.stat(R).st_dev
spec.json_out(dict(taskq=st,tasks=col.get('tasks'),sched_raw=raw,free=free,dev=os.stat(R).st_dev,envs_ok=oks,out_used=out_used,
                   blob_dev=bdev,blob_free=free if bdev==os.stat(R).st_dev else shutil.disk_usage(B).free,
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


# 清理（relay_store.gc 发过去跑）
GC_PY = """import json,os,shutil,stat,sys
sys.path[:0]=['scripts/taskq/queue','scripts/queue']
import spec
R=os.path.realpath(sys.argv[1])
A=json.load(sys.stdin)
soft=int(A['soft_gb']*1e9)
D=os.path.join(R,'data')
B=os.path.join(R,'blobs')
def inside(p,base):
    p=os.path.realpath(p)
    return p==base or p.startswith(base.rstrip('/')+'/')
free0=A['free'] if A.get('free') is not None else shutil.disk_usage(R).free
out=dict(freed=0,removed_views=[],removed_blobs=0,free=free0)
def free():
    return free0+out['freed']
def rmtree(d):
    if os.path.islink(d):
        os.remove(d)
        return
    if not inside(d,D):
        return
    for dp,dns,fns in os.walk(d,topdown=False):
        for f in fns:
            p=os.path.join(dp,f)
            st=os.lstat(p)
            os.remove(p)
            if stat.S_ISREG(st.st_mode) and st.st_nlink==1:
                out['freed']+=st.st_size
        for n in dns:
            p=os.path.join(dp,n)
            if os.path.islink(p):
                os.remove(p)
            else:
                os.rmdir(p)
    os.rmdir(d)
if free()<soft:
    keep={os.path.normpath(k) for k in A.get('keep') or []}
    cand=set()
    for v in A.get('views') or []:
        v=os.path.normpath(v)
        if not v.startswith('/') and '..' not in v.split('/') and len(v.split('/'))>=2:
            cand.add(v)
    for sc in A.get('scan') or []:
        P=os.path.join(D,sc)
        if os.path.isdir(P) and inside(P,D):
            cand|={os.path.relpath(os.path.join(P,n),D) for n in os.listdir(P)}
    for rel in sorted(cand):
        d=os.path.join(D,rel)
        if rel in keep or any(k.startswith(rel+'/') or rel.startswith(k+'/') for k in keep):
            continue
        if not os.path.lexists(d):
            out['removed_views'].append(rel)
            continue
        if os.path.exists(os.path.join(d,'.incomplete')) or not inside(d,D):
            continue
        rmtree(d)
        out['removed_views'].append(rel)
import time
PD=os.path.join(R,'state','pins')
def pins():
    s=set()
    for f in (os.listdir(PD) if os.path.isdir(PD) else []):
        try:
            p=os.path.join(PD,f)
            if f.endswith('.json') and time.time()-os.path.getmtime(p)<12*3600:
                s|=set(json.load(open(p)))
        except (OSError,ValueError):
            continue
    return s
pinned=pins()
def skip(p,f):
    if f in pinned:
        return True
    if '.' in f:
        try:
            return time.time()-os.lstat(p).st_mtime<86400
        except OSError:
            return True
    return False
if free()<soft and os.path.isdir(B) and inside(B,R):
    used=set()
    for dp,dns,fns in os.walk(D):
        for f in fns+dns:
            p=os.path.join(dp,f)
            if os.path.islink(p):
                used.add(os.path.realpath(p))
    cand=[]
    for dp,_,fns in os.walk(B):
        for f in fns:
            p=os.path.normpath(os.path.join(dp,f))
            if os.path.realpath(p) in used or os.path.islink(p) or skip(p,f):
                continue
            try:
                st=os.lstat(p)
            except OSError:
                continue
            if st.st_nlink!=1:
                continue
            cand.append((max(st.st_atime,st.st_mtime),p,st.st_size))
    for _,p,sz in sorted(cand):
        if free()>=soft:
            break
        if not inside(p,B) or os.path.basename(p) in pins():
            continue
        try:
            os.remove(p)
        except OSError:
            continue
        out['freed']+=sz
        out['removed_blobs']+=1
if free()<soft and os.path.isdir(B) and inside(B,R):
    sib=[os.path.join(os.path.realpath(os.path.expanduser(x)),'blobs') for x in A.get('siblings') or []]
    for dp,_,fns in os.walk(B):
        for f in fns:
            p=os.path.normpath(os.path.join(dp,f))
            if os.path.realpath(p) in used or os.path.islink(p) or not inside(p,B) or skip(p,f):
                continue
            try:
                st=os.lstat(p)
            except OSError:
                continue
            if st.st_nlink!=2:
                continue
            for sb in sib:
                try:
                    o=os.lstat(os.path.join(sb,f[:2],f))
                except OSError:
                    continue
                if o.st_ino==st.st_ino and o.st_dev==st.st_dev:
                    if f in pins():
                        break
                    os.remove(p)
                    out['unlinked']=out.get('unlinked',0)+1
                    break
out['free']=free()
spec.json_out(out)
"""
