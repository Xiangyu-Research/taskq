"""relay 执行机那边：经 ssh 跑命令（rsh、那边的 taskq.py、一段 Python）和发过去跑的小程序（状态、收编、视图、列结果、删除、清理）。
hub 上的记账见 relay_store.py，传数据和拉回见 relay_transfer.py，每次检查的计划见 relay.py。

那边的根目录 R（machines.py 的 root）的布局、标志文件、时间常数都在 proto.py（ROOT、MKDIRS、blob、QUEUE、claim_alive）：
代码包在 code/<包名>，code/current 指向在用的那个（relay_bundle.py 打包，relay.py deploy 推过去、旧包留最新 3 个，4 天以上的删）；
queue 是它自己的任务队列（taskq.py，key 和总队列相同）；blobs/<h[:2]>/<h> 是按 sha256 存的数据文件（内容相同的只传一次）；
data 是数据目录，相对路径和 hub 的一样：每个任务的输入是指向 blobs 的软链接（和插件生成的小文件，sites/names.py），
产出照常写。hub 上 state.json 记下每个任务放了哪些路径（视图、输入、产出），删一个任务时只删别的任务不用的（相同、上级、下级都算在用）。

发过去的小程序在那边的代码包里跑（那边可能是旧包，没有 proto.py）：用到的协议常数在这里嵌进去（PRE），不在那边 import 调度的文件；
只有读状态（STATUS_PY、STATES_PY）要那边的队列和任务说明：那边的包有 taskq.py status --json、spec.py collect --json 就用这两个
带版本号的命令，没有时（旧包）退回 import taskq、spec（11/4）。
"""
import ast
import inspect
import json
import os
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


def master_down(m):
    """机器条目 m 要用 ssh 长连接（ssh_master）、长连接断着 → True：不另起连接（中转执行机认证失败多了会被锁）。"""
    import subprocess
    return bool(m.get("ssh_master") and m.get("ssh")) and subprocess.run(
        ssh_argv()[:1] + ["-O", "check", m["ssh"]], capture_output=True, timeout=30).returncode != 0


def site_argv(name, cmd, timeout=300):
    """在执行机上跑一条 shell 命令的参数表：经 ssh（那边也限时，common.remote_timeout）；本机执行机（ssh=None，11/7）在家目录里
    本机跑，去掉 hub 才有的变量（proto.HUB_ONLY_ENV），和 ssh 登录过去时一样。"""
    host = conf(name)["ssh"]
    if host is None:
        return ["env", *[a for v in proto.HUB_ONLY_ENV for a in ("-u", v)], "bash", "-c", f"cd ~ && {cmd}"]
    return ssh_argv() + [host, C.remote_timeout(f"bash -c {shlex.quote(cmd)}", timeout)]


def rsh(name, cmd, input=None, timeout=300):
    return C.run(site_argv(name, cmd, timeout), None, timeout, input=input)


def prefix(name):
    r, D = R.q(root(name)), proto.ROOT
    return f"cd {r}/{D['current']} && TASKQ_DIR={r}/{D['queue']} TASKQ_DATA={r}/{D['data']}"


def rtq(name, args, timeout=180, input=None):
    """那边的 taskq.py <args>（input：标准输入）→ (退出码, 输出)；连不上 → None。"""
    rc, out, err = rsh(name, f"{prefix(name)} {py(name)} {cfg.bundle_rel('queue', 'taskq.py')} {args}", input=input, timeout=timeout)
    return None if rc in (proto.EXIT["timeout"], 255) else (rc, (out + err).strip())


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


def src(fn):
    """函数的源码（去掉说明和注释）：嵌进发过去的小程序。"""
    tree = ast.parse(inspect.getsource(fn))
    body = tree.body[0].body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
        del body[0]
    return ast.unparse(tree) + "\n"


# 小程序用的协议常数（proto.py）：R_<根目录下的>、Q_<队列目录下的>；CODEQ：代码包里的调度代码（proto.BUNDLE_CORE）
CONST = dict(MARK=proto.JSON_MARK, VERSION=proto.VERSION, SAFE=proto.SAFE, INCOMPLETE=proto.INCOMPLETE, TASK_LOGS=proto.TASK_LOGS,
             CLAIM_INFO=proto.CLAIM_INFO, HALF_CLAIM=proto.HALF_CLAIM, STALE_BEAT=proto.STALE_BEAT, FRESH_LOG=proto.FRESH_LOG,
             COLLECT_FIELDS=proto.COLLECT_FIELDS, CODEQ=cfg.bundle_rel("queue"),
             **{"R_" + k.upper(): v for k, v in proto.ROOT.items()}, **{"Q_" + k.upper(): v for k, v in proto.QUEUE.items()})
PRE = ("import json,os,sys\n" + "".join(f"{k}={v!r}\n" for k, v in CONST.items())
       + "".join(src(f) for f in (proto.blob, proto.safe, proto.claim_alive))
       + "def json_out(x):\n    print(MARK+json.dumps(x),flush=True)\n")
HEAD = PRE + "R=sys.argv[1]\nA=json.load(sys.stdin)\n"

# 根目录已占的（有 cap 的执行机按 cap 减去它算剩余）：硬链接只算一次、软链接不算 → (总共, 其中 blobs、code 以外的)
USAGE = """def usage(R):
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
                if top not in (R_BLOBS,R_CODE):
                    out_used+=x.st_blocks*512
    return used,out_used
"""

# 读那边的队列和各任务的情况：有带版本号的命令就用（taskq.py status --json --internal（冒烟任务也要）、spec.py collect --json），旧包没有
# （退出码 2、没有 JSON 行；不认 --internal 的也是）时 import 那边的 taskq、spec；返回的结构两条路相同
QUERY = """import subprocess
def cmd(args,inp=None):
    if not os.path.isfile(os.path.join(CODEQ,args[0])):
        return None
    p=subprocess.run([sys.executable,os.path.join(CODEQ,args[0])]+args[1:],input=inp,capture_output=True,text=True)
    line=next((x[len(MARK):] for x in p.stdout.splitlines() if x.startswith(MARK)),None)
    if p.returncode==2 and line is None:
        return None
    try:
        r=json.loads(line) if p.returncode==0 and line is not None else None
    except ValueError:
        r=None
    if not isinstance(r,dict) or not isinstance(r.get('version'),int) or r['version']<1:
        sys.exit('%s: %s %s'%(' '.join(args),p.returncode,(p.stderr or p.stdout).strip()[-300:]))
    r.pop('version')
    return r
def old():
    sys.path[:0]=[CODEQ,'scripts/queue']
def local_status():
    r=cmd(['taskq.py','status','--json','--internal'])
    if r is not None:
        return r,True
    old()
    import taskq
    return taskq.local_status(),False
def collect(tasks,new):
    if new:
        sp=[{k:t['spec'].get(k) for k in COLLECT_FIELDS} for t in tasks if t.get('spec')]
        r=cmd(['spec.py','collect','--json'],json.dumps(dict(specs=sp,root=os.environ['TASKQ_DATA'],wait=None)))
        if r is not None:
            return r
    old()
    import spec
    sp=[{k:t['spec'].get(k) for k in spec.COLLECT_FIELDS} for t in tasks if t.get('spec')]
    return spec.collect(sp,os.environ['TASKQ_DATA'])
"""

STATUS_PY = HEAD + QUERY + USAGE + """import shutil,time
st,new=local_status()
col=collect(st['tasks'],new)
raw=None
try:
    if A.get('sched')=='pueue':
        q=subprocess.run([os.path.expanduser(A['pueue']),'status','--json'],capture_output=True,text=True,timeout=60)
    else:
        q=subprocess.run(['squeue','-u',os.environ.get('USER',''),'-h','-o','%i|%j|%T|%M|%q|%R'],capture_output=True,text=True,timeout=60)
    raw=q.stdout if q.returncode==0 else None
except Exception:
    raw=None
ed=os.path.join(R,R_ENVS_OK)
oks={f[:-3]:open(os.path.join(ed,f)).read().strip() for f in (os.listdir(ed) if os.path.isdir(ed) else []) if f.endswith('.ok')}
envs_st={}
er=os.path.expanduser(A.get('env_root') or os.path.join(R,R_ENVS))
for n in A.get('envs') or []:
    d=envs_st[n]=dict(active=os.path.exists(os.path.join(er,n,'.activate')),log='')
    for x in ('ok','failed','building'):
        try:
            d[x]=open(os.path.join(ed,n+'.'+x)).readline().strip()
        except OSError:
            d[x]=''
    if d['failed']:
        try:
            d['log']=open(os.path.join(ed,n+'.log'),'rb').read()[-2000:].decode('utf-8','replace')
        except OSError:
            pass
free=shutil.disk_usage(R).free
out_used=None
if A.get('cap'):
    used,out_used=usage(R)
    free=min(free,max(0,int(A['cap']*1e9)-used))
cdir=os.path.join(R,R_CODE)
live=[d for d in (os.listdir(cdir) if os.path.isdir(cdir) else []) if '.tmp' not in d and os.path.isdir(os.path.join(cdir,d))
      and time.time()-os.stat(os.path.join(cdir,d)).st_mtime<STALE_BEAT]
B=os.path.realpath(os.path.join(R,R_BLOBS))
bdev=os.stat(B).st_dev if os.path.isdir(B) else os.stat(R).st_dev
bfree=free
if bdev!=os.stat(R).st_dev:
    bfree=shutil.disk_usage(B).free
    if A.get('blob_cap'):
        bfree=min(bfree,max(0,int(A['blob_cap']*1e9)-usage(B)[0]))
json_out(dict(taskq=st,tasks=col.get('tasks'),sched_raw=raw,free=free,dev=os.stat(R).st_dev,envs_ok=oks,envs_st=envs_st,now=int(time.time()),out_used=out_used,
                   blob_dev=bdev,blob_free=bfree,
                   bundle=os.path.basename(os.path.realpath(os.path.join(R,R_CURRENT))),live=live))
"""

MISSING_PY = HEAD + "json_out([i for i in A if not os.path.isfile(os.path.join(R,blob(i)))])\n"

PIN_PY = HEAD + """d=os.path.join(R,R_PINS)
os.makedirs(d,exist_ok=True)
p=os.path.join(d,A['key']+'.json')
if A.get('hashes') is None:
    if os.path.exists(p):
        os.remove(p)
else:
    with open(p+'.tmp','w') as f:
        json.dump(A['hashes'],f)
    os.replace(p+'.tmp',p)
json_out(1)
"""

ADOPT_PY = HEAD + """import hashlib
def sha(p):
    x=hashlib.sha256()
    with open(p,'rb') as f:
        for b in iter(lambda:f.read(1<<22),b''):
            x.update(b)
    return x.hexdigest()
B=os.path.join(R,R_BLOBS)
bdev=os.stat(B if os.path.isdir(B) else R).st_dev
got=[]
for h,(rel,size) in A['files'].items():
    t=os.path.join(R,blob(h))
    for s in A['srcs']:
        base=os.path.expanduser(s['path'])
        p=os.path.join(base,blob(h)) if s.get('site') else (os.path.join(base,rel) if rel else None)
        try:
            if not p:
                continue
            x=os.stat(p)
            if x.st_size!=size or x.st_dev!=bdev:
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
json_out(got)
"""

VIEW_PY = HEAD + """import base64,fcntl
D=os.path.join(R,R_DATA)
os.makedirs(os.path.dirname(os.path.join(R,R_VIEW_LOCK)),exist_ok=True)
L=open(os.path.join(R,R_VIEW_LOCK),'w')
fcntl.flock(L,fcntl.LOCK_EX)
def rel_ok(r):
    r=os.path.normpath(r)
    if r.startswith('/') or '..' in r.split('/'):
        sys.exit('bad path '+r)
    return r
views=[rel_ok(v) for v in A['views']]
if any(len(v.split('/'))<2 for v in views):
    sys.exit('bad views '+repr(views))
for h in A['links'].values():
    if not os.path.isfile(os.path.join(R,blob(h))):
        sys.exit('missing '+h)
incs=[]
for v in views:
    d=os.path.join(D,v)
    os.makedirs(d,exist_ok=True)
    inc=os.path.join(d,INCOMPLETE)
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
    t=os.path.join(R,blob(h))
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
B=os.path.realpath(os.path.join(R,R_BLOBS))+'/'
for v in A.get('clean',views):
    for dp,_,fs in os.walk(os.path.join(D,rel_ok(v))):
        for f in fs:
            p=os.path.join(dp,f)
            if p not in want and os.path.islink(p) and os.path.realpath(p).startswith(B):
                os.remove(p)
for inc in incs:
    os.remove(inc)
json_out(len(want)-len(incs))
"""

LIST_PY = HEAD + """import fnmatch,glob
D=os.path.join(R,R_DATA)
B=os.path.realpath(os.path.join(R,R_BLOBS))+'/'
ex=A['exclude']
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
for x in A['extra']:
    for p in glob.glob(os.path.join(os.path.dirname(os.path.join(D,A['outs'][0])),x)):
        add(p)
pre=safe(A['key'])+SAFE
for p in glob.glob(os.path.join(D,TASK_LOGS,glob.escape(pre)+'*')):
    if SAFE not in os.path.basename(p)[len(pre):]:
        add(p)
def have(p):
    try:
        os.stat(p)
        return True
    except (FileNotFoundError,NotADirectoryError):
        return False
os.stat(D)
json_out(dict(done=all(any(have(os.path.join(D,A['out'],a)) for a in it) for it in A['done']),items=items))
"""

DROP_PY = HEAD + """import glob,shutil
D=os.path.join(R,R_DATA)
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
pre=A['logs']+SAFE
for p in glob.glob(os.path.join(D,TASK_LOGS,glob.escape(pre)+'*')):
    if SAFE not in os.path.basename(p)[len(pre):]:
        os.remove(p)
json_out(n)
"""

STATES_PY = HEAD + QUERY + "json_out({t['key']:t['state'] for t in local_status()[0]['tasks']})\n"


# claims：同一个盘上别的执行机里这个任务的领取 [dict(claim=领取目录, out=那边的结果目录)]，log：任务说明的 log；
# 还活着（和 taskq.py 同一条规则，proto.claim_alive）→ busy，存档可能写到一半
ADOPT_OUT_PY = HEAD + """import glob,shutil,time
def age(d):
    try:
        ps=glob.glob(os.path.join(glob.escape(os.path.expanduser(d)),A['log'])) if A.get('log') and d else []
        return time.time()-max(os.path.getmtime(p) for p in ps) if ps else None
    except OSError:
        return None
for c in A.get('claims') or []:
    d=os.path.expanduser(c['claim'])
    try:
        made=os.stat(d).st_mtime
    except OSError:
        continue
    try:
        ci=json.load(open(os.path.join(d,CLAIM_INFO)))
    except Exception:
        ci=None
    if not isinstance(ci,dict):
        ci=None
    if claim_alive(ci,made,time.time(),age(c.get('out'))):
        json_out(dict(n=0,src=None,busy=d))
        sys.exit()
new=os.path.join(R,R_DATA,A['out'])
def mt(d):
    ts=[os.path.getmtime(p) for g in A['resume'] for p in glob.glob(os.path.join(glob.escape(d),g)) if os.path.isfile(p)]
    return max(ts) if ts else None
best,bt=None,mt(new)
if A['done'] and all(any(os.path.exists(os.path.join(new,x)) for x in it) for it in A['done']):
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
json_out(dict(n=n,src=best))
"""


def adopt_srcs(name):
    """收编来源（machines.py 的 adopt）：同一个盘上别的执行机的根目录在前（按哈希直接认，不重算），老数据目录在后。"""
    return sorted(M.adopt_sources(name), key=lambda x: not x["site"])


# 清理（relay_store.gc 发过去跑）
GC_PY = PRE + """import fcntl,shutil,stat
R=os.path.realpath(sys.argv[1])
A=json.load(sys.stdin)
soft=int(A['soft_gb']*1e9)
D=os.path.join(R,R_DATA)
B=os.path.join(R,R_BLOBS)
BR=os.path.realpath(B)
def inside(p,base):
    p=os.path.realpath(p)
    return p==base or p.startswith(base.rstrip('/')+'/')
def isblob(dp,f):
    return len(f)>2 and os.path.normpath(dp)==os.path.dirname(os.path.join(R,blob(f)))
blobs_ok=os.path.isdir(B) and (inside(B,R) or os.path.islink(B) and not inside(R,BR))
free0=A['free'] if A.get('free') is not None else shutil.disk_usage(R).free
bfree0=A.get('blob_free')
out=dict(freed=0,removed_views=[],removed_blobs=0,free=free0)
_bf=[0]
def free():
    return free0+out['freed']-(_bf[0] if bfree0 is not None else 0)
def bfree():
    return free() if bfree0 is None else bfree0+_bf[0]
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
if free()<soft or bfree()<soft:
    os.makedirs(os.path.dirname(os.path.join(R,R_VIEW_LOCK)),exist_ok=True)
    L=open(os.path.join(R,R_VIEW_LOCK),'w')
    fcntl.flock(L,fcntl.LOCK_EX)
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
        if os.path.exists(os.path.join(d,INCOMPLETE)) or not inside(d,D):
            continue
        rmtree(d)
        out['removed_views'].append(rel)
    L.close()
import time
PD=os.path.join(R,R_PINS)
_pc=[None,set()]
def pins():
    try:
        st=os.stat(PD)
    except OSError:
        return set()
    k=(st.st_ino,st.st_mtime_ns)
    if _pc[0]==k and time.time()-st.st_mtime>2:
        return _pc[1]
    s=set()
    for f in os.listdir(PD):
        try:
            p=os.path.join(PD,f)
            if f.endswith('.json') and time.time()-os.path.getmtime(p)<12*3600:
                s|=set(json.load(open(p)))
        except (OSError,ValueError):
            continue
    _pc[0]=k
    _pc[1]=s
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
if bfree()<soft and blobs_ok:
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
            if not isblob(dp,f) or os.path.realpath(p) in used or os.path.islink(p) or skip(p,f):
                continue
            try:
                st=os.lstat(p)
            except OSError:
                continue
            if st.st_nlink!=1:
                continue
            cand.append((max(st.st_atime,st.st_mtime),p,st.st_size))
    for _,p,sz in sorted(cand):
        if bfree()>=soft:
            break
        if not inside(p,BR) or os.path.basename(p) in pins():
            continue
        try:
            os.remove(p)
        except OSError:
            continue
        out['freed']+=sz
        _bf[0]+=sz
        out['removed_blobs']+=1
if bfree()<soft and blobs_ok:
    sib=[os.path.join(os.path.realpath(os.path.expanduser(x)),R_BLOBS) for x in A.get('siblings') or []]
    for dp,_,fns in os.walk(B):
        for f in fns:
            p=os.path.normpath(os.path.join(dp,f))
            if not isblob(dp,f) or os.path.realpath(p) in used or os.path.islink(p) or not inside(p,BR) or skip(p,f):
                continue
            try:
                st=os.lstat(p)
            except OSError:
                continue
            if st.st_nlink!=2:
                continue
            for sb in sib:
                try:
                    o=os.lstat(os.path.join(os.path.dirname(sb),blob(f)))
                except OSError:
                    continue
                if o.st_ino==st.st_ino and o.st_dev==st.st_dev:
                    if f in pins():
                        break
                    os.remove(p)
                    out['unlinked']=out.get('unlinked',0)+1
                    break
out['free']=free()
if bfree0 is not None:
    out['blob_free']=bfree()
json_out(out)
"""
