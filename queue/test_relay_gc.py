#!/usr/bin/env python3
"""relay_gc.py 的测试（2026-10-31）：在临时目录造一个假根目录，本地跑 GC_PY（同 relay.rpy 的调用方式），剩余用 free 参数假装。
    python3 scripts/taskq/queue/test_relay_gc.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg as _cfg  # noqa: E402
REPO = _cfg.REPO  # 使用者的仓库根（cfg.py）
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, HERE)
import relay_gc as G  # noqa: E402
import spec as S  # noqa: E402

GB = 10 ** 9


def ok(msg):
    print(f"通过：{msg}")


def local_rpy(root):
    def rpy(name, code, data=None, timeout=600):
        r = subprocess.run([sys.executable, "-c", code, root], input=json.dumps(data), capture_output=True, text=True,
                           cwd=REPO, timeout=timeout)
        res = S.json_in(r.stdout)
        if r.returncode or res is None:
            raise RuntimeError(r.stderr[-500:])
        return res
    return rpy


def blob(R, h, size):
    p = os.path.join(R, "blobs", h[:2], h)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb") as f:
        f.truncate(size)
    return p


def build(tmp):
    """→ (R, 外面的文件)。视图 ds_keep（指 aa1）、ds_old（指 bb1、cc1(硬链接)、带一个 2000 字节的缓存文件和一个指向 R 外面的链接）；
    blob dd1、ee1 没人指（dd1 更旧）；ff1 有硬链接在老目录。"""
    R = os.path.join(tmp, "R")
    outside = os.path.join(tmp, "old", "x.prob")
    os.makedirs(os.path.dirname(outside))
    with open(outside, "wb") as f:
        f.write(b"x" * 500)
    pre = os.path.join(R, "data", "prepared")
    for ds in ("ds_keep", "ds_old"):
        os.makedirs(os.path.join(pre, ds, "PlanM_full"))
    a = blob(R, "aa1", 1000)
    os.symlink(a, os.path.join(pre, "ds_keep", "PlanM_full", "c1.prob"))
    b = blob(R, "bb1", 1000)
    os.symlink(b, os.path.join(pre, "ds_old", "PlanM_full", "c1.prob"))
    c = blob(R, "cc1", 1000)
    os.link(c, os.path.join(tmp, "old", "cc1_copy"))
    os.symlink(c, os.path.join(pre, "ds_old", "PlanM_full", "c2.prob"))
    with open(os.path.join(pre, "ds_old", "PlanM_full", "c1.npy"), "wb") as f:
        f.truncate(2000)
    with open(os.path.join(pre, "ds_old", "dataset.json"), "w") as f:
        f.write("{}")
    os.symlink(outside, os.path.join(pre, "ds_old", "ext.prob"))
    d = blob(R, "dd1", 3000)
    os.utime(d, (1000, 1000))
    e = blob(R, "ee1", 3000)
    os.utime(e, (2000, 2000))
    f_ = blob(R, "ff1", 5000)
    os.link(f_, os.path.join(tmp, "old", "ff1_copy"))
    os.utime(f_, (10, 10))
    res = os.path.join(R, "data", "results", "ds_keep", "frag_0")
    os.makedirs(res)
    os.symlink(e, os.path.join(res, "uses_ee1"))
    return R, outside


def exists(R, h):
    return os.path.exists(os.path.join(R, "blobs", h[:2], h))


def main():
    tmp = tempfile.mkdtemp(prefix="test_relay_gc_")
    try:
        R, outside = build(tmp)
        rpy = local_rpy(R)
        keep = ["prepared/ds_keep"]
        r = G.gc("x", keep, rpy, soft_gb=100, free=200 * GB)
        assert r["freed"] == 0 and not r["removed_views"] and r["removed_blobs"] == 0, r
        assert os.path.isdir(os.path.join(R, "data", "prepared", "ds_old"))
        ok("剩余高于软线：什么都不动")

        soft = 1e-6  # 1000 字节
        r = G.gc("x", keep, rpy, soft_gb=soft, free=0)
        assert r["removed_views"] == ["prepared/ds_old"], r
        assert os.path.isdir(os.path.join(R, "data", "prepared", "ds_keep"))
        assert not os.path.exists(os.path.join(R, "data", "prepared", "ds_old"))
        assert r["freed"] == 2002 + 0 and r["removed_blobs"] == 0, r  # 缓存 2000 + dataset.json 2；视图删完已到软线
        assert all(exists(R, h) for h in ("aa1", "bb1", "cc1", "dd1", "ee1", "ff1"))
        assert os.path.getsize(outside) == 500
        ok("低于软线先删不在 keep 里的视图（不跟着软链接删目标），够了就不删 blob")

        R, outside = build(tempfile.mkdtemp(dir=tmp))
        rpy = local_rpy(R)
        r = G.gc("x", keep, rpy, soft_gb=5000 / GB, free=0)
        assert r["removed_views"] == ["prepared/ds_old"], r
        assert not exists(R, "dd1") and exists(R, "bb1"), r
        assert exists(R, "aa1") and exists(R, "ee1"), "在用的 blob 不删"
        assert exists(R, "cc1") and exists(R, "ff1"), "nlink > 1 的不删"
        assert os.path.getsize(outside) == 500
        assert r["freed"] == 2002 + 3000 and r["removed_blobs"] == 1, r
        ok("再删没人指的 blob，最旧的先删，在用的和硬链接收编的不碰，R 外面的不碰")

        R, _ = build(tempfile.mkdtemp(dir=tmp))
        os.makedirs(os.path.join(R, "state", "pins"))
        with open(os.path.join(R, "state", "pins", "t__a.json"), "w") as f:
            json.dump(["dd1"], f)
        part = blob(R, "gg1.tunnelpart", 4000)
        r = G.gc("x", keep, local_rpy(R), soft_gb=1.0, free=0)
        assert exists(R, "dd1") and os.path.exists(part) and not exists(R, "bb1"), r
        os.utime(os.path.join(R, "state", "pins", "t__a.json"), (1000, 1000))
        os.utime(part, (1000, 1000))
        r = G.gc("x", keep, local_rpy(R), soft_gb=1.0, free=0)
        assert not exists(R, "dd1") and not os.path.exists(part), r
        ok("正在传的任务钉住的块、传到一半的临时文件不删；钉住记录过了 12 小时、临时文件放了一天以上就不再认")

        R, _ = build(tempfile.mkdtemp(dir=tmp))
        r = G.gc("x", keep, local_rpy(R), soft_gb=1.0, free=0)
        assert exists(R, "aa1") and exists(R, "ee1") and exists(R, "cc1") and exists(R, "ff1")
        assert not exists(R, "bb1") and not exists(R, "dd1") and r["removed_blobs"] == 2
        ok("释放不到软线时删光能删的就停，不碰在用的")

        R, _ = build(tempfile.mkdtemp(dir=tmp))
        D = os.path.join(R, "data")
        for v in ("in/aaa", "in/bbb", "in/ccc", "models/a/td/frag_0"):
            os.makedirs(os.path.join(D, v))
            os.symlink(blob(R, v.replace("/", "_"), 10), os.path.join(D, v, "f.dat"))
        open(os.path.join(D, "in/ccc/.incomplete"), "w").close()
        reg = ["in/aaa", "in/bbb", "in/ccc", "in/gone", "models/a/td/frag_0", "../x", "in"]
        r = G.gc("x", keep + ["in/bbb"], local_rpy(R), soft_gb=1e-6, free=0, views=reg)
        assert r["removed_views"] == ["in/aaa", "in/gone", "models/a/td/frag_0", "prepared/ds_old"], r
        assert not os.path.exists(os.path.join(D, "in/aaa")) and os.path.exists(os.path.join(D, "in/bbb/f.dat"))
        assert os.path.exists(os.path.join(D, "in/ccc/f.dat")) and os.path.isdir(os.path.join(D, "models/a/td"))
        assert os.path.isdir(os.path.dirname(R)) and os.path.isdir(os.path.join(D, "in"))
        ok("登记过的任务视图也清理：keep 里的、正在建的（.incomplete）不删，那边已经没有的报出来让登记表去掉，越界的、只有一层的不认")

        # 根目录经软链接到达（视图里的软链接指向软链接的路径）：在用的 blob 照样认得出来
        real = tempfile.mkdtemp(dir=tmp)
        R, _ = build(real)
        link = os.path.join(tmp, "Rlink")
        os.symlink(R, link)
        pre = os.path.join(R, "data", "prepared", "ds_keep", "PlanM_full", "c1.prob")
        os.remove(pre)
        os.symlink(os.path.join(link, "blobs", "aa", "aa1"), pre)
        r = G.gc("x", keep, local_rpy(link), soft_gb=1.0, free=0)
        assert exists(R, "aa1") and exists(R, "ee1") and not exists(R, "dd1"), r
        ok("根目录经软链接到达时，在用的 blob 也不删")

        # 同一个盘上的另一台执行机（lab0 / lab1）：两边的 blobs 互相硬链接收编
        R, _ = build(tempfile.mkdtemp(dir=tmp))
        sib = tempfile.mkdtemp(dir=tmp)
        for h in ("gg1", "hh1", "ii1"):
            os.makedirs(os.path.join(sib, "blobs", h[:2]), exist_ok=True)
        g = blob(R, "gg1", 100)
        os.link(g, os.path.join(sib, "blobs", "gg", "gg1"))
        h_ = blob(R, "hh1", 100)
        os.link(h_, os.path.join(tmp, "old", "hh1_copy"))
        i_ = blob(R, "ii1", 100)
        os.link(i_, os.path.join(sib, "blobs", "ii", "ii1"))
        os.symlink(i_, os.path.join(R, "data", "prepared", "ds_keep", "PlanM_full", "c9.prob"))
        r = G.gc("x", keep, local_rpy(R), soft_gb=1.0, free=0, siblings=[sib])
        assert not exists(R, "gg1") and os.path.exists(os.path.join(sib, "blobs", "gg", "gg1")), r
        assert exists(R, "hh1") and exists(R, "ii1") and r.get("unlinked") == 1, r
        ok("还不够时：自己不用、另一个链接在同一个盘上别的执行机 blobs 里的，只删自己这个链接；老目录的硬链接、自己在用的不碰")

        # 变异审查（11/3）：keep 是视图里更深的一层（或更上的一层）时，包含它的视图 / 它下面的视图也不删
        R, _ = build(tempfile.mkdtemp(dir=tmp))
        D = os.path.join(R, "data")
        os.makedirs(os.path.join(D, "in/up/sub"))
        os.symlink(blob(R, "up1", 10), os.path.join(D, "in/up/sub/f.dat"))
        r = G.gc("x", ["prepared/ds_keep/PlanM_full", "in/up/sub"], local_rpy(R), soft_gb=1e-6, free=0, views=["in/up"])
        assert os.path.exists(os.path.join(D, "prepared/ds_keep/PlanM_full/c1.prob")), r
        assert os.path.exists(os.path.join(D, "in/up/sub/f.dat")), r
        assert "prepared/ds_old" in r["removed_views"] and "prepared/ds_keep" not in r["removed_views"], r
        r = G.gc("x", ["prepared"], local_rpy(R), soft_gb=1e-6, free=0)
        assert os.path.isdir(os.path.join(D, "prepared/ds_keep")), r
        ok("keep 是视图下面一层或上面一层：那个视图也不删")

        b0, G.budget = G.budget, lambda n: None
        try:
            assert G.gc("dicc2", keep, lambda *a, **k: 1 / 0) is None
        finally:
            G.budget = b0
        ok("没有 budget 的机器不清理")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("全部通过")


if __name__ == "__main__":
    main()
