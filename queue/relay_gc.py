"""有盘空间预算的 relay 机器（envs/machines.py RELAY 条目的 budget）的清理（2026-10-31）：剩余低于软线时，先删总队列不再要的
视图（state 里登记过的各任务的视图，加上内容插件建视图的文件夹（scripts/sites/names.py 的 VIEW_ROOTS）下的目录；不在 keep 里、没有 .incomplete（正在建）的），再按最久没用先删没有软链接指向的 blob（只删 nlink == 1 的，
硬链接收编、老目录还有一份的不碰），直到剩余回到软线以上。还不够时，自己不用、另一个链接在同一个盘上别的执行机（siblings）
blobs 里的 blob 删掉自己这个链接（那台不用了它下次清理时删）。只碰根目录 R 里面的东西。
根目录经软链接到达时，视图里的软链接指的是软链接的路径，比较时都换成真实路径。"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))

GC_PY = """import json,os,shutil,stat,sys
sys.path.insert(0,'scripts/queue')
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
            if os.path.realpath(p) in used or os.path.islink(p):
                continue
            st=os.lstat(p)
            if st.st_nlink!=1:
                continue
            cand.append((max(st.st_atime,st.st_mtime),p,st.st_size))
    for _,p,sz in sorted(cand):
        if free()>=soft:
            break
        if not inside(p,B):
            continue
        os.remove(p)
        out['freed']+=sz
        out['removed_blobs']+=1
if free()<soft and os.path.isdir(B) and inside(B,R):
    sib=[os.path.join(os.path.realpath(os.path.expanduser(x)),'blobs') for x in A.get('siblings') or []]
    for dp,_,fns in os.walk(B):
        for f in fns:
            p=os.path.normpath(os.path.join(dp,f))
            if os.path.realpath(p) in used or os.path.islink(p) or not inside(p,B):
                continue
            st=os.lstat(p)
            if st.st_nlink!=2:
                continue
            for sb in sib:
                try:
                    o=os.lstat(os.path.join(sb,f[:2],f))
                except OSError:
                    continue
                if o.st_ino==st.st_ino and o.st_dev==st.st_dev:
                    os.remove(p)
                    out['unlinked']=out.get('unlinked',0)+1
                    break
out['free']=free()
spec.json_out(out)
"""


def budget(name):
    sys.path.insert(0, os.path.join(REPO, "envs"))
    import machines as M
    return M.machine(name).get("budget")


def gc(name, keep_dirs, rpy, soft_gb=None, free=None, views=(), siblings=()):
    """name 这台的清理 → dict(freed, removed_views, removed_blobs, free)；没有 budget → None。
    keep_dirs：分给这台的任务要用的、刚建的视图（相对 R/data）。views：登记过的视图（relay.py 的 state）。siblings：同一个盘上别的执行机的根目录。rpy：relay.rpy。
    free 只给测试用。removed_views 里也有登记了但那边已经没有的视图（登记表跟着去掉）。"""
    if soft_gb is None:
        b = budget(name)
        if not b:
            return None
        soft_gb = b[0]
    sys.path.insert(0, os.path.join(REPO, "scripts", "sites"))
    import names as N
    return rpy(name, GC_PY, dict(soft_gb=soft_gb, keep=sorted(keep_dirs), free=free, views=sorted(views), scan=N.VIEW_ROOTS,
                                 siblings=list(siblings)))
