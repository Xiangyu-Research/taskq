"""有盘空间预算的 relay 机器（envs/machines.py RELAY 条目的 budget）的清理（2026-10-31）：剩余低于软线时，先删总队列不再要的
数据集视图（R/data/nnUNet/preprocessed/ 下不在 keep 里的目录），再按最久没用先删没有软链接指向的 blob（只删 nlink == 1 的，
硬链接收编、老目录还有一份的不碰），直到剩余回到软线以上。只碰根目录 R 里面的东西。"""
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
P=os.path.join(D,'nnUNet','preprocessed')
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
    if os.path.isdir(P) and inside(P,D):
        for n in sorted(os.listdir(P)):
            rel=os.path.relpath(os.path.join(P,n),D)
            if rel in keep:
                continue
            rmtree(os.path.join(P,n))
            out['removed_views'].append(rel)
if free()<soft and os.path.isdir(B) and inside(B,R):
    used=set()
    for dp,dns,fns in os.walk(D):
        for f in fns+dns:
            p=os.path.join(dp,f)
            if os.path.islink(p):
                t=os.readlink(p)
                t=os.path.normpath(t if os.path.isabs(t) else os.path.join(dp,t))
                used.add(t)
    cand=[]
    for dp,_,fns in os.walk(B):
        for f in fns:
            p=os.path.normpath(os.path.join(dp,f))
            if p in used or os.path.islink(p):
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
out['free']=free()
spec.json_out(out)
"""


def budget(name):
    sys.path.insert(0, os.path.join(REPO, "envs"))
    import machines as M
    return M.machine(name).get("budget")


def gc(name, keep_dirs, rpy, soft_gb=None, free=None):
    """name 这台的清理 → dict(freed, removed_views, removed_blobs, free)；没有 budget → None。
    keep_dirs：总队列里分给这台、没训完的任务要用的别名数据集目录（相对 R/data）。rpy：relay.rpy。free 只给测试用。"""
    if soft_gb is None:
        b = budget(name)
        if not b:
            return None
        soft_gb = b[0]
    return rpy(name, GC_PY, dict(soft_gb=soft_gb, keep=sorted(keep_dirs), free=free))
