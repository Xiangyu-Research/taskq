#!/usr/bin/env python3
"""任务的输入：开训前查全没有；执行机允许时在节点本地盘建一份这个任务私有的视图，开训后在后台把 blob 拷过去。

  inputs.py prepare <数据目录> <任务文件名> <本地根目录或空> <输入>...
      查每个输入（文件或文件夹；链接要指得到文件；文件夹里任一层有 .incomplete = 建到一半，读不了也算缺）。不全 → 退出码 4，标准错误里写缺几个、举几个例子。
      给了本地根目录时：在 <根>/views/<任务文件名>/ 下照数据目录的相对路径建一份只有链接的树，指向原处的文件（已经拷到本地的指本地），
      要拷的写进 <根>/views/<任务文件名>.todo.json，打印这份视图的根；没给时打印数据目录。命令从打印的目录读（$TASK_INPUTS_ROOT）。
      先删本节点上自己的、作业已经结束的本地副本（<根的上一级>/<proto.STAGE_DIR_PREFIX><作业号>）。
  inputs.py copy <本地根目录> <任务文件名>
      照清单把 blob 拷到 <根>/blobs/（布局同执行机，proto.blob；本节点别的作业已有的硬链接过来），边拷边核对 sha256，
      再把视图里对应的链接换过去：换之前读原处，换之后读本地。拷完至少还剩 STAGE_KEEP_GB（默认 300）GB
      （能从本节点别的作业硬链接来的不算），不够就不拷。本节点的拷贝排队（一把锁），一个查完拷完下一个再查。核对不上、拷不了的留着读原处。
只拷 blob（真实路径是执行机 blobs 里的 <h2>/<h>，按内容认）；别的文件（执行机自己做出的产出）读原处。
退出码：0 好了；4 缺输入；其他 = 出错（run_task.sh 照旧读原处）。
"""
import fcntl
import glob
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import proto  # noqa: E402
import common as C  # noqa: E402
import site_ops as SO  # noqa: E402

MISSING = proto.EXIT["run_missing"]
PAR = int(os.environ.get("STAGE_PAR", 16))
KEEP_GB = float(os.environ.get("STAGE_KEEP_GB", 300))


def entries(data, inp):
    """一个输入 → ([(相对数据目录的路径, 绝对路径)], [缺的说明])。带通配符的展开成匹配到的文件，一个都没有算缺。"""
    rel = os.path.relpath(inp, data)
    if rel.startswith(".."):
        return [], [f"{inp} 不在数据目录里"]
    if glob.has_magic(rel):
        hits = sorted(glob.glob(inp))
        if not hits:
            return [], [f"{rel}（一个都没有）"]
        out, miss = [], []
        for h in hits:
            e, m = entries(data, h)
            out += e
            miss += m
        return out, miss
    if os.path.isdir(inp):
        out, bad = [], []
        for d, ds, fs in os.walk(inp, onerror=lambda e: bad.append(f"{os.path.relpath(e.filename, data)}（读不了）"), followlinks=True):
            if proto.INCOMPLETE in fs or proto.INCOMPLETE in ds:  # 任一层建到一半
                return [], [f"{os.path.relpath(d, data)}（建到一半）"]
            for f in fs:
                p = os.path.join(d, f)
                out.append((os.path.relpath(p, data), p))
        return ([], bad) if bad else (out, [])
    if os.path.lexists(inp):
        return [(rel, inp)], []
    return [], [f"{rel}（没有）"]


def look(p):
    """→ (大小, 真实路径, sha256 或 None)；指不到文件 → None。sha256：真实路径是 blob（<h2>/<h>）时的 h。"""
    try:
        st = os.stat(p)
    except OSError:
        return None
    if not os.path.isfile(p):
        return None
    real = os.path.realpath(p)
    h = os.path.basename(real)
    is_blob = SO.HASH_RE.fullmatch(h) and os.path.basename(os.path.dirname(real)) == h[:2]
    return st.st_size, real, h if is_blob else None


def live_jobs():
    """本账号在 Slurm 里还在的作业号；查不了 → None（这时不删别的作业的目录）。
    作业数组的子任务 %i 显示成 <数组号>_<序号>，SLURM_JOB_ID（本地目录的名字）是 %A，两样都收。"""
    if not shutil.which("squeue"):
        return None
    try:
        r = subprocess.run(["squeue", "-h", "-u", os.environ.get("USER", ""), "-o", "%A %i"], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None
    if r.returncode:
        return None
    return {x.split("_")[0] for x in r.stdout.split()}


def jobs_dirs(root):
    """本节点上（同一个上一级文件夹里）各作业的本地副本 → {作业号: 文件夹}。"""
    out = {}
    for d in glob.glob(os.path.join(os.path.dirname(root), proto.STAGE_DIR_PREFIX + "*")):
        j = os.path.basename(d)[len(proto.STAGE_DIR_PREFIX):]
        if j.isdigit() and os.path.isdir(d):
            out[j] = d
    return out


def clean_old(root):
    live = live_jobs()
    if live is None:
        return
    for j, d in jobs_dirs(root).items():
        try:
            mine = os.stat(d).st_uid == os.getuid()
        except OSError:
            continue
        if d != root and j not in live and mine:
            print(f"删掉已结束作业 {j} 的本地副本 {d}", file=sys.stderr)
            shutil.rmtree(d, ignore_errors=True)


def prepare(data, key, root, inputs):
    data = os.path.abspath(data)
    found, miss = [], []
    for inp in inputs:
        e, m = entries(data, os.path.abspath(inp))
        found += e
        miss += m
    with ThreadPoolExecutor(PAR) as ex:
        info = list(ex.map(lambda e: look(e[1]), found))
    miss += [f"{r}（指不到文件）" for (r, _), x in zip(found, info) if x is None]
    if miss:
        print(f"缺输入：{len(miss)} 个（{'；'.join(miss[:5])}{' …' if len(miss) > 5 else ''}）", file=sys.stderr)
        return MISSING
    if not root:
        print(data)
        return 0
    root = os.path.abspath(root)
    clean_old(root)
    view = os.path.join(root, "views", key)
    shutil.rmtree(view, ignore_errors=True)
    todo = []
    for (rel, _), (size, real, h) in zip(found, info):
        p = os.path.join(view, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        local = os.path.join(root, proto.blob(h)) if h else None
        if local and _size(local) == size:
            os.symlink(local, p)
        else:
            os.symlink(real, p)
            if h:
                todo.append([rel, real, size, h])
    C.atomic_write(view + ".todo.json", json.dumps(todo))
    print(view)
    return 0


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def copy(root, key):
    t0 = time.time()
    root = os.path.abspath(root)
    view = os.path.join(root, "views", key)
    todo = json.load(open(view + ".todo.json"))
    for p in glob.glob(os.path.join(root, "blobs", "*", "*" + proto.TMP)):  # 被停掉的拷贝留下的（<h>.<进程号>.tmp）
        try:
            if not alive(int(p.rsplit(".", 2)[1])):
                os.remove(p)
        except (ValueError, IndexError, OSError):
            pass
    by = {}
    for rel, real, size, h in todo:
        by.setdefault(h, [real, size, []])[2].append(rel)
    others = [d for d in jobs_dirs(root).values() if d != root]
    with open(os.path.join(os.path.dirname(root), f".{proto.STAGE_DIR_PREFIX}{os.getuid()}.lock"), "a") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)  # 本节点的拷贝一个一个来：查空间和拷贝之间别的拷贝不插进来
        need = sum(size for h, (_, size, _) in by.items()
                   if _size(os.path.join(root, proto.blob(h))) != size and not any(_size(os.path.join(o, proto.blob(h))) == size for o in others))
        free = shutil.disk_usage(root).free
        if free - need < KEEP_GB * 1024 ** 3:
            print(f"本地盘剩 {free / 1024 ** 3:.0f} GB，拷 {need / 1024 ** 3:.0f} GB 后不到 {KEEP_GB:.0f} GB，不拷，读原处")
            return 0
        return _copy(root, view, by, others, t0)


def _size(p):
    try:
        return os.path.getsize(p)
    except OSError:
        return None


def _copy(root, view, by, others, t0):
    n = dict(copied=0, linked=0, had=0, bytes=0)
    bad = []

    def one(item):
        h, (real, size, rels) = item
        dst = os.path.join(root, proto.blob(h))
        if _size(dst) == size:
            n["had"] += 1
        else:
            src = next((q for q in (os.path.join(o, proto.blob(h)) for o in others) if _size(q) == size), None)
            try:
                if src and C.atomic_copy(src, dst, link=True, size=size):
                    n["linked"] += 1
                else:
                    C.atomic_copy(real, dst, size=size, sha=h)
                    n["copied"] += 1
                    n["bytes"] += size
            except OSError as e:
                bad.append(f"{os.path.basename(rels[0])}：{e}")
                return
        for rel in rels:
            C.atomic_link(dst, os.path.join(view, rel))
    with ThreadPoolExecutor(min(PAR, 8)) as ex:
        list(ex.map(one, sorted(by.items(), key=lambda x: x[1][2][0])))
    print(f"{time.strftime('%F %T')} 输入拷到本地 {view}：{len(by) - len(bad)}/{len(by)} 个文件（新拷 {n['copied']} 个 "
          f"{n['bytes'] / 1024 ** 3:.1f} GB，硬链接 {n['linked']} 个，已有 {n['had']} 个），{time.time() - t0:.0f} 秒"
          + (f"；{len(bad)} 个没拷成，读原处（{'；'.join(bad[:3])}）" if bad else ""))
    return 0


def main(argv):
    if argv[:1] == ["prepare"] and len(argv) >= 4:
        return prepare(argv[1], argv[2], argv[3], argv[4:])
    if argv[:1] == ["copy"] and len(argv) == 3:
        return copy(argv[1], argv[2])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
