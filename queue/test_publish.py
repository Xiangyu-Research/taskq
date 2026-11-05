#!/usr/bin/env python3
"""flow/publish.py 的测试（2026-11-05）：临时目录里的数据目录、工作目录和台账。
新放；旧版归档后换上（含上一版有、这一版没有的）；冲突时一个都不动并记 publish_blocked；--overwrite、--accept-theirs；
别的实例的嵌套产出不碰；标志文件最后放；每一步之后断电，resume 以后目录和台账都和一次做完一样。
    python3 queue/test_publish.py
"""
import os
import shutil
import sys
import tempfile
from types import SimpleNamespace

HERE = os.path.dirname(os.path.abspath(__file__))
T = tempfile.mkdtemp(prefix="test_publish_")
os.environ["TASKQ_STATE"] = os.path.join(T, "state")
sys.path.insert(0, HERE)
import proto as PR  # noqa: E402
from flow import ledger as LG  # noqa: E402
from flow import publish as PB  # noqa: E402
from flow.model import Instance  # noqa: E402

fails = []


def ok(cond, what):
    if not cond:
        fails.append(what)


A = Instance(id="step:a_f0", rule="step", key="a/0", outputs=["out/a"], marker="out/a/done.json")
B = Instance(id="sub:a_f0", rule="sub", key="a/0", outputs=["out/a/sub"])
FLOW = SimpleNamespace(instances={A.id: A, B.id: B})


def write(base, rel, text):
    p = os.path.join(base, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        f.write(text)
    return p


def read(base, rel):
    try:
        return open(os.path.join(base, rel)).read()
    except FileNotFoundError:
        return None


def fresh(name):
    d = os.path.join(T, name)
    root, work = os.path.join(d, "data"), os.path.join(d, "work")
    os.makedirs(root)
    os.makedirs(work)
    db = LG.connect(os.path.join(d, "ledger.db"))
    return root, work, db


def stage(work, files):
    for rel, text in files.items():
        write(work, rel, text)


def live(db, iid):
    return {r["path"]: r["role"] for r in db.execute("SELECT * FROM output WHERE inst=? AND state=?", (iid, PR.O_LIVE))}


def olds(root):
    return sorted(f.split("/", 3)[3] for f in PB.files_in(os.path.join(root, PB.OLD, PB.safe(A.id))))


try:
    # 1. 新放：标志文件最后，MANIFEST 在普通文件之后；工作目录清空
    root, work, db = fresh("new")
    stage(work, {"done.json": "1", "MANIFEST.json": "m", "x.npz": "x1", "y/z.txt": "z1"})
    r = PB.publish(A, work, root, db)
    order = [os.path.relpath(d, os.path.join(root, "out/a")) for op, _, d in r["ops"] if op == "put"]
    ok(r["state"] == PR.A_OK, f"新放：状态 {r['state']}")
    ok(order == ["x.npz", "y/z.txt", "MANIFEST.json", "done.json"], f"新放：顺序 {order}")
    ok(read(root, "out/a/y/z.txt") == "z1" and not os.listdir(work), "新放：文件到位、工作目录清空")
    lv = live(db, A.id)
    ok(lv.get("out/a/done.json") == "marker" and "out/a/x.npz" in lv, f"新放：台账 {lv}")
    ok(not os.listdir(PB.journal_dir()), "新放：journal 删了")

    # 2. 再发一版：旧的归档；上一版有、这一版没有的（y/z.txt）也归档
    stage(work, {"done.json": "2", "x.npz": "x2", "MANIFEST.json": "m2"})
    r = PB.publish(A, work, root, db)
    ok(r["state"] == PR.A_OK, f"第二版：状态 {r['state']}")
    ok(read(root, "out/a/x.npz") == "x2" and read(root, "out/a/y/z.txt") is None, "第二版：换上了，旧的没留着混在一起")
    ok(olds(root) == ["MANIFEST.json", "done.json", "x.npz", "y/z.txt"], f"第二版：归档 {olds(root)}")
    st = dict(db.execute("SELECT state, count(*) FROM output WHERE inst=? GROUP BY state", (A.id,)).fetchall())
    ok(st.get(PR.O_RETIRED, 0) >= 3 and "out/a/y/z.txt" not in live(db, A.id), f"第二版：台账 {st}")

    # 3. 冲突：人改了 x.npz、又放了一个台账里没有的 note.txt → 一个都不动，记 publish_blocked
    write(root, "out/a/x.npz", "human edit, longer")
    write(root, "out/a/note.txt", "mine")
    stage(work, {"done.json": "3", "x.npz": "x3", "note.txt": "theirs", "w.txt": "w3"})
    r = PB.publish(A, work, root, db)
    why = {c["path"]: c["why"] for c in r["conflicts"]}
    ok(r["state"] == PR.A_BLOCKED and set(why) == {"out/a/x.npz", "out/a/note.txt"}, f"冲突：{r}")
    ok(read(root, "out/a/w.txt") is None and read(root, "out/a/done.json") == "2", "冲突：一个都没动")
    ok(LG.last_attempt(db, A.id)["state"] == PR.A_BLOCKED, "冲突：台账记 publish_blocked")
    bl = PB.blocked(db)
    ok(set(bl) == {A.id} and bl[A.id]["work"] == work and len(bl[A.id]["conflicts"]) == 2, f"冲突：blocked() {bl}")

    # 4. --accept-theirs：人的留着，其余照放
    r = PB.publish(A, work, root, db, accept_theirs=True)
    ok(r["state"] == PR.A_OK, f"accept-theirs：{r['state']}")
    ok(read(root, "out/a/x.npz") == "human edit, longer" and read(root, "out/a/note.txt") == "mine", "accept-theirs：人的没动")
    ok(read(root, "out/a/w.txt") == "w3" and read(root, "out/a/done.json") == "3", "accept-theirs：其余放了")
    ok(not PB.blocked(db), "accept-theirs：不再算被挡住")
    shutil.rmtree(work)
    os.makedirs(work)

    # 5. --overwrite：人的归档后换上
    write(root, "out/a/note.txt", "mine again")
    stage(work, {"done.json": "4", "note.txt": "new"})
    r = PB.publish(A, work, root, db, overwrite=True)
    ok(r["state"] == PR.A_OK and read(root, "out/a/note.txt") == "new", f"overwrite：{r['state']}")
    ok(any(open(os.path.join(root, PB.OLD, PB.safe(A.id), s, "out/a/note.txt")).read() == "mine again"
           for s in os.listdir(os.path.join(root, PB.OLD, PB.safe(A.id)))
           if os.path.exists(os.path.join(root, PB.OLD, PB.safe(A.id), s, "out/a/note.txt"))), "overwrite：人的归档了")

    # 6. 别的实例的嵌套产出：发 A 时 out/a/sub/ 下的东西归 B，不当成 A 上一版的归档；A 要写进去 → 冲突
    root, work, db = fresh("nested")
    wb = os.path.join(T, "nested", "work_b")
    os.makedirs(wb)
    stage(wb, {"s.txt": "b1"})
    PB.publish(B, wb, root, db)
    stage(work, {"done.json": "1", "x.npz": "x"})
    PB.publish(A, work, root, db)
    stage(work, {"done.json": "2"})
    PB.publish(A, work, root, db)
    ok(read(root, "out/a/sub/s.txt") == "b1", "嵌套：B 的产出没被 A 归档")
    stage(work, {"done.json": "3", "sub/s.txt": "a!"})
    r = PB.publish(A, work, root, db)
    ok(r["state"] == PR.A_BLOCKED and r["conflicts"][0]["why"] == f"归 {B.id}", f"嵌套：A 写 B 的文件 → 冲突 {r['conflicts']}")
    shutil.rmtree(work)
    os.makedirs(work)

    # 7. 每一步之后断电，resume 以后和一次做完一样
    def final(n):
        root, work, db = fresh(f"crash{n}")
        stage(work, {"done.json": "1", "x.npz": "x1", "y.txt": "y1"})
        PB.publish(A, work, root, db)
        stage(work, {"done.json": "2", "x.npz": "x2", "MANIFEST.json": "m"})
        if n is None:
            PB.publish(A, work, root, db)
        else:
            try:
                PB.publish(A, work, root, db, crash_after=n)
            except RuntimeError:
                pass
            ok(PB.resume(FLOW, db) == [A.id], f"断电 {n}：resume 做了")
        files = {f: read(root, os.path.join("out/a", f)) for f in PB.files_in(os.path.join(root, "out/a"))}
        return files, olds(root), sorted(live(db, A.id)), os.listdir(work), os.listdir(PB.journal_dir())

    want = final(None)
    ok(want[0] == {"done.json": "2", "x.npz": "x2", "MANIFEST.json": "m"}, f"断电基准：{want[0]}")
    n_ops = 6  # 归档 3 + 放 3
    for n in range(1, n_ops + 1):
        got = final(n)
        ok(got == want, f"断电 {n}：{got} ≠ {want}")

    # 8. 跨文件系统 → 拒绝（/dev/shm 是 tmpfs，和临时目录不同时才测）
    if os.path.isdir("/dev/shm") and os.stat("/dev/shm").st_dev != os.stat(T).st_dev:
        w = tempfile.mkdtemp(prefix="test_publish_", dir="/dev/shm")
        try:
            write(w, "done.json", "1")
            try:
                PB.publish(A, w, root, db)
                ok(False, "跨文件系统：没拒绝")
            except PB.PublishError:
                pass
        finally:
            shutil.rmtree(w, ignore_errors=True)
finally:
    shutil.rmtree(T, ignore_errors=True)

for f in fails:
    print("失败：" + f)
print("test_publish：" + ("全部通过" if not fails else f"{len(fails)} 项失败"))
sys.exit(1 if fails else 0)
