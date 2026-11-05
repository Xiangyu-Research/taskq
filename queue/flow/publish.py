"""发布（2026-11-05，P5）：stage 方式的尝试在自己的工作目录里写完，由这里搬进数据目录；relay 拉回也可以走这里。
  1. 工作目录和数据目录要在同一个文件系统（st_dev 相同，改名是原子的），不同 → 拒绝（PublishError）。
  2. 每个要放的文件分类：目标不存在 → 新放；目标归这个实例（台账里最长前缀是它）、且台账记着的指纹对得上 → 旧的归档后换上；
     归别的实例、不在台账里、或和台账记的不一样（人改过、别人写的）→ 冲突。有一个冲突就一个都不动，台账记 publish_blocked，
     由 taskq publish <实例> --overwrite（冲突的也归档后换上）或 --accept-theirs（冲突的留着人家的，其余照放）处理。
  3. 先把每一步改名写进 journal，再做：旧的归档 → 普通文件 → MANIFEST 一类 → 标志文件最后；然后在一个事务里更新台账、删 journal。
  4. 中途断了：resume() 照 journal 把剩下的做完。每一步"目标已经在"就算做过（归档在放新的之前，所以不会把新的当旧的挪走）。
  5. 上一版有、这一版没有、归这个实例的文件也归档（不和新的混在一起）。
归档在 <数据目录>/.taskq/old/<实例>/<时间>/<相对路径>。"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import cfg  # noqa: E402
import proto as PR  # noqa: E402
from flow import ledger as LG  # noqa: E402

OLD = PR.PUBLISH_OLD


class PublishError(Exception):
    pass


def journal_dir():
    return cfg.state(PR.PUBLISH_JOURNAL)


def safe(iid):
    return iid.replace("/", "%").replace(":", "@")


def files_in(d):
    out = []
    for p, _, fs in os.walk(d):
        out += [os.path.relpath(os.path.join(p, f), d) for f in fs]
    return sorted(out)


def owner(db, rel):
    """台账里管着 rel 的那一行（路径相同，或是它最长的上级目录）；没有 → None。"""
    best = None
    for r in db.execute("SELECT * FROM output WHERE state=? AND (path=? OR ? LIKE path || '/%')", (PR.O_LIVE, rel, rel)):
        if best is None or len(r["path"]) > len(best["path"]):
            best = r
    return best


def ours(db, iid, rel, path):
    """目标归这个实例、台账记着的指纹对得上 → (True, '')；否则 (False, 为什么)。
    台账记的是文件 → 指纹要一样；记的是上级目录（老的 adopt、声明的产出是目录）→ 目录里的文件算它的。"""
    r = owner(db, rel)
    if r is None:
        return False, "不在台账里"
    if r["inst"] != iid:
        return False, f"归 {r['inst']}"
    if r["path"] == rel:
        return (False, "和台账记的不一样") if LG.differ(dict(r), LG.fingerprint(path)) else (True, "")
    # 按上级目录算到这个实例名下：它的文件逐个登记过（发布、taskq run 记的）就要逐个对上，只记了目录（老的 adopt）才算它的
    if db.execute("SELECT 1 FROM output WHERE inst=? AND state=? AND role='file' AND path LIKE ? || '/%' LIMIT 1",
                  (iid, PR.O_LIVE, r["path"])).fetchone():
        return False, "不在台账里"
    return True, ""


def rank(rel, marker):
    if marker and rel == marker:
        return 3
    return 2 if os.path.basename(rel).startswith("MANIFEST") else 1


def plan(i, work, root, db, dest=None, overwrite=False, accept_theirs=False):
    """→ (ops, conflicts)。ops：[(op, src, dst)]（绝对路径），op = archive | put；dest：放到数据目录下哪里（相对，默认第一个产出）。"""
    dest = os.path.normpath(dest or (i.outputs[0] if i.outputs else ""))
    if dest in ("", "."):
        raise PublishError(f"{i.id} 没有声明产出，不知道放到哪里")
    probe = os.path.join(root, dest)
    while not os.path.exists(probe):
        probe = os.path.dirname(probe)
    if os.stat(work).st_dev != os.stat(probe).st_dev:
        raise PublishError(f"工作目录 {work} 和 {probe} 不在同一个文件系统，不能原子地放进去")
    old = os.path.join(root, OLD, safe(i.id), time.strftime("%Y%m%d-%H%M%S") + f".{time.time_ns() % 10**9:09d}")
    marker = os.path.relpath(i.marker, dest) if i.marker and i.marker.startswith(dest.rstrip("/") + "/") else None
    new = files_in(work)
    rel = {f: os.path.normpath(os.path.join(dest, f)) for f in new}
    archive, theirs, conflicts = [], set(), []
    for f in new:
        dst = os.path.join(root, rel[f])
        if not os.path.lexists(dst):
            continue
        mine, why = ours(db, i.id, rel[f], dst)
        if mine or overwrite:
            archive.append(rel[f])
        elif accept_theirs:
            theirs.add(f)
        else:
            conflicts.append(dict(path=rel[f], why=why))
    if conflicts:
        return [], conflicts
    target = os.path.join(root, dest)
    if os.path.isdir(target):  # 上一版有、这一版没有的
        keep = set(rel.values())
        for f in files_in(target):
            r = os.path.normpath(os.path.join(dest, f))
            if r not in keep and ours(db, i.id, r, os.path.join(root, r))[0]:
                archive.append(r)
    ops = [("archive", os.path.join(root, r), os.path.join(old, r)) for r in sorted(set(archive))]
    puts = sorted((f for f in new if f not in theirs), key=lambda f: (rank(f, marker), f))
    ops += [("put", os.path.join(work, f), os.path.join(root, rel[f])) for f in puts]
    return ops, []


def do(op, src, dst):
    """做一步；目标已经在 → 做过了。"""
    if os.path.lexists(dst):
        return
    if not os.path.lexists(src):
        raise PublishError(f"{src} 不见了，{dst} 也没有")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    os.replace(src, dst)


def drop_empty(d):
    """删 d 下面的空子目录（d 本身留着）。"""
    for p, _, _ in sorted(os.walk(d), key=lambda x: -len(x[0])):
        if p == d:
            continue
        try:
            os.rmdir(p)
        except OSError:
            pass


def run_ops(i, root, db, jp, j):
    for op in j["ops"]:
        do(*op)
        if j.get("crash_after") is not None:  # 测试用：模拟做到一半断电
            j["crash_after"] -= 1
            if j["crash_after"] <= 0:
                raise RuntimeError("模拟断电")
    files = [os.path.relpath(dst, root) for op, _, dst in j["ops"] if op == "put"]
    gone = [os.path.relpath(src, root) for op, src, _ in j["ops"] if op == "archive"]
    aid = j.get("attempt") or LG.begin(db, i, source="publish")
    LG.finish(db, aid, i, PR.A_OK, root, files=files, retire=gone)
    os.remove(jp)
    drop_empty(j["work"])


def publish(i, work, root, db, aid=None, dest=None, overwrite=False, accept_theirs=False, crash_after=None):
    """→ dict(state, ops, conflicts)。"""
    ops, conflicts = plan(i, work, root, db, dest, overwrite, accept_theirs)
    if conflicts:
        LG.record(db, i, PR.A_BLOCKED, "publish", root,
                  note=json.dumps(dict(work=work, dest=dest, attempt=aid, conflicts=conflicts[:20]), ensure_ascii=False))
        return dict(state=PR.A_BLOCKED, ops=[], conflicts=conflicts)
    moving = {src for op, src, _ in ops if op == "archive"}
    late = [d for _, _, d in ops if d not in moving and os.path.lexists(d)]
    if late:  # 排完计划以后有人写了（目标已在会被 resume 当成做过）
        raise PublishError(f"{i.id} 的目标刚被别人写了：{late[:3]}")
    os.makedirs(journal_dir(), exist_ok=True)
    jp = os.path.join(journal_dir(), safe(i.id) + ".json")
    if os.path.exists(jp):
        raise PublishError(f"{i.id} 上一次发布还没做完（{jp}），先 resume")
    j = dict(inst=i.id, work=work, root=root, ops=ops, attempt=aid, t=time.time())
    tmp = jp + ".tmp"
    with open(tmp, "w") as f:
        json.dump(j, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, jp)
    j["crash_after"] = crash_after
    run_ops(i, root, db, jp, j)
    return dict(state=PR.A_OK, ops=ops, conflicts=[])


def resume(flow, db):
    """照 journal 把断了的发布做完 → [做完的实例 id]。"""
    try:
        names = sorted(n for n in os.listdir(journal_dir()) if n.endswith(".json"))
    except FileNotFoundError:
        return []
    done = []
    for n in names:
        jp = os.path.join(journal_dir(), n)
        j = json.load(open(jp))
        i = flow.instances.get(j["inst"])
        if i is not None:
            run_ops(i, j["root"], db, jp, j)
            done.append(i.id)
    return done


def blocked(db):
    """最近一次尝试是 publish_blocked 的实例 → {实例: 记下的 dict(work, dest, attempt, conflicts)}。"""
    out = {}
    for r in db.execute("SELECT a.inst, a.note FROM attempt a JOIN (SELECT inst, max(id) m FROM attempt GROUP BY inst) l "
                        "ON a.id=l.m WHERE a.state=?", (PR.A_BLOCKED,)):
        try:
            out[r["inst"]] = json.loads(r["note"] or "{}")
        except ValueError:
            out[r["inst"]] = {}
    return out
