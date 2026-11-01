#!/usr/bin/env python3
"""任务的输入（2026-11-01）：开训前查全没有；执行机允许时在节点本地盘建一份这个任务私有的视图，开训后在后台拷过去。

  inputs.py prepare <数据目录> <任务文件名> <本地根目录或空> <输入>...
      查每个输入（文件或文件夹；链接要指得到文件；文件夹里有 .incomplete = 建到一半）。不全 → 退出码 4，标准错误里写缺几个、举几个例子。
      给了本地根目录时：在 <根>/views/<任务文件名>/ 下照数据目录的相对路径建一份只有链接的树，指向原处的文件（已经拷到本地的指本地），
      要拷的写进 <根>/views/<任务文件名>.todo.json，打印这份视图的根；没给时打印数据目录。命令从打印的目录读（$TASK_INPUTS_ROOT）。
      先删本节点上自己的、作业已经结束的 phd_stage_<作业号>。
  inputs.py copy <本地根目录> <任务文件名>
      照清单把文件拷到 <根>/blobs/（本节点别的作业已有的硬链接过来），拷完核对（文件名是 sha256 的按内容，其他按大小），
      再替换视图里对应的链接（新建链接再 rename 盖过去）：换之前读原处，换之后读本地。拷完至少还剩 STAGE_KEEP_GB（默认 300）GB，
      不够就不拷。核对不上、拷不了的留着读原处。
退出码：0 好了；4 缺输入；其他 = 出错（run_task.sh 照旧读原处）。
"""
import fcntl
import glob
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

MISSING = 4
PAR = int(os.environ.get("STAGE_PAR", 16))
KEEP_GB = float(os.environ.get("STAGE_KEEP_GB", 300))
HEX = re.compile(r"[0-9a-f]{64}")
PREFIX = "phd_stage_"
PARTIAL = ".partial."


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
        if os.path.exists(os.path.join(inp, ".incomplete")):
            return [], [f"{rel}（建到一半）"]
        out = []
        for d, _, fs in os.walk(inp, followlinks=True):
            for f in fs:
                p = os.path.join(d, f)
                out.append((os.path.relpath(p, data), p))
        return out, []
    if os.path.lexists(inp):
        return [(rel, inp)], []
    return [], [f"{rel}（没有）"]


def look(p):
    """→ (大小, 真实路径, 改动时间 ns, inode)；指不到文件 → None。"""
    try:
        st = os.stat(p)
    except OSError:
        return None
    return (st.st_size, os.path.realpath(p), st.st_mtime_ns, st.st_ino) if os.path.isfile(p) else None


def blob_id(real, size, mtime=0, ino=0):
    """sha256 名的文件按名字；其余按路径、大小、改动时间和 inode（同一路径内容换了不复用旧副本，11/1）。"""
    b = os.path.basename(real)
    return b if HEX.fullmatch(b) else "p" + hashlib.sha1(f"{real}\0{size}\0{mtime}\0{ino}".encode()).hexdigest()


def live_jobs():
    """本账号在 Slurm 里还在的作业号；查不了 → None（这时不删别的作业的目录）。"""
    if not shutil.which("squeue"):
        return None
    try:
        r = subprocess.run(["squeue", "-h", "-u", os.environ.get("USER", ""), "-o", "%i"], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None
    if r.returncode:
        return None
    return {x.split("_")[0] for x in r.stdout.split()}


def clean_old(root):
    live = live_jobs()
    if live is None:
        return
    for d in glob.glob(os.path.join(os.path.dirname(root), PREFIX + "*")):
        j = os.path.basename(d)[len(PREFIX):]
        try:
            mine = os.stat(d).st_uid == os.getuid()
        except OSError:
            continue
        if d != root and j.isdigit() and j not in live and mine:
            print(f"删掉已结束作业 {j} 的本地副本 {d}", file=sys.stderr)
            shutil.rmtree(d, ignore_errors=True)


def put_link(p, target):
    tmp = p + ".taskq_tmp"
    if os.path.lexists(tmp):
        os.remove(tmp)
    os.symlink(target, tmp)
    os.replace(tmp, p)


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
    blobs = os.path.join(root, "blobs")
    shutil.rmtree(view, ignore_errors=True)
    os.makedirs(blobs, exist_ok=True)
    todo = []
    for (rel, _), (size, real, mtime, ino) in zip(found, info):
        bid = blob_id(real, size, mtime, ino)
        local = os.path.join(blobs, bid)
        p = os.path.join(view, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        if os.path.isfile(local) and os.path.getsize(local) == size:
            os.symlink(local, p)
        else:
            os.symlink(real, p)
            todo.append([rel, real, size, bid])
    with open(view + ".todo.json.tmp", "w") as f:
        json.dump(todo, f)
    os.replace(view + ".todo.json.tmp", view + ".todo.json")
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


def copy_one(src, dst, size, check):
    """src → dst（先写 dst.partial.<pid>，核对后改名）→ 说明（出错时）或 None。"""
    tmp = f"{dst}{PARTIAL}{os.getpid()}"
    h = hashlib.sha256() if check else None
    try:
        with open(src, "rb") as fi, open(tmp, "wb") as fo:
            for b in iter(lambda: fi.read(1 << 22), b""):
                fo.write(b)
                if h:
                    h.update(b)
        got = os.path.getsize(tmp)
        if got != size:
            raise OSError(f"大小 {got}，应为 {size}")
        if h and h.hexdigest() != os.path.basename(dst):
            raise OSError("内容和文件名的 sha256 对不上")
        os.replace(tmp, dst)
        return None
    except OSError as e:
        try:
            os.remove(tmp)
        except OSError:
            pass
        return f"{os.path.basename(src)}：{e}"


def copy(root, key):
    t0 = time.time()
    root = os.path.abspath(root)
    view = os.path.join(root, "views", key)
    blobs = os.path.join(root, "blobs")
    todo = json.load(open(view + ".todo.json"))
    for p in glob.glob(os.path.join(blobs, "*" + PARTIAL + "*")):  # 被停掉的拷贝留下的
        try:
            if not alive(int(p.rsplit(".", 1)[1])):
                os.remove(p)
        except (ValueError, OSError):
            pass
    by = {}
    for rel, real, size, bid in todo:
        by.setdefault(bid, [real, size, []])[2].append(rel)
    need = sum(size for bid, (_, size, _) in by.items() if not os.path.isfile(os.path.join(blobs, bid)))
    free = shutil.disk_usage(root).free
    if free - need < KEEP_GB * 1024 ** 3:
        print(f"本地盘剩 {free / 1024 ** 3:.0f} GB，拷 {need / 1024 ** 3:.0f} GB 后不到 {KEEP_GB:.0f} GB，不拷，读原处")
        return 0
    others = [d for d in glob.glob(os.path.join(os.path.dirname(root), PREFIX + "*", "blobs")) if d != blobs]
    n = dict(copied=0, linked=0, had=0, bytes=0)
    bad = []

    def one(item):
        bid, (real, size, rels) = item
        dst = os.path.join(blobs, bid)
        with open(dst + ".lock", "a") as lk:
            fcntl.flock(lk, fcntl.LOCK_EX)
            if os.path.isfile(dst) and os.path.getsize(dst) == size:
                n["had"] += 1
            else:
                for o in others:
                    q = os.path.join(o, bid)
                    try:
                        if os.path.getsize(q) == size:
                            os.link(q, dst + ".taskq_tmp")
                            os.replace(dst + ".taskq_tmp", dst)
                            n["linked"] += 1
                            break
                    except OSError:
                        continue
                else:
                    err = copy_one(real, dst, size, bool(HEX.fullmatch(bid)))
                    if err:
                        bad.append(err)
                        return
                    n["copied"] += 1
                    n["bytes"] += size
        for rel in rels:
            put_link(os.path.join(view, rel), dst)
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
