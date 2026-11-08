#!/usr/bin/env python3
"""Tests for relay_store.py cleanup (gc): build a fake root in a temp dir, run site_ops.py gc locally (as relay_remote.op
calls it), and fake free space with the free argument.
    python3 queue/test_relay_store.py
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # root of the scheduler code
sys.path.insert(0, os.path.join(CORE, "queue"))
import cfg as _cfg  # noqa: E402
REPO = _cfg.REPO  # project repo root (cfg.py)
sys.path.insert(0, os.path.join(HERE, "testdata"))
import fixture  # noqa: E402

fixture.use()
sys.path.insert(0, HERE)
import relay_store as G  # noqa: E402
import spec as S  # noqa: E402
import proto  # noqa: E402

GB = 10 ** 9


def ok(msg):
    print(f"PASS:{msg}")


SITE_OPS = os.path.join(HERE, "site_ops.py")


def H(name):
    """Test blob name (aa1, dd1, ...) → its sha256 name (blobs only accept 64 lowercase hex chars)."""
    return hashlib.sha256(name.encode()).hexdigest()


def local_rpy(root):
    """Run a site_ops.py operation locally with root as the root (same as relay_remote.op)."""
    def rpy(name, what, data=None, timeout=600):
        r = subprocess.run([sys.executable, SITE_OPS, what], input=json.dumps(data), capture_output=True, text=True,
                           cwd=REPO, timeout=timeout, env=dict(os.environ, TASKQ_ROOT=root))
        res = S.json_in(r.stdout)
        if r.returncode or res is None:
            raise RuntimeError(r.stderr[-500:])
        return res
    return rpy


def blob(R, name, size, suffix=""):
    h = H(name)
    p = os.path.join(R, "blobs", h[:2], h + suffix)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb") as f:
        f.truncate(size)
    return p


def build(tmp):
    """→ (R, a file outside). Views ds_keep (points at aa1) and ds_old (points at bb1, cc1 (hard-linked), holds a 2000-byte
    cache file and a link to a file outside R); blobs dd1 and ee1 are unreferenced (dd1 is older); ff1 has a hard link in an
    old dir."""
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


def exists(R, name):
    h = H(name)
    return os.path.exists(os.path.join(R, "blobs", h[:2], h))


def main():
    tmp = fixture.tmpdir(prefix="test_relay_store_")
    try:
        R, outside = build(tmp)
        rpy = local_rpy(R)
        keep = ["prepared/ds_keep"]
        r = G.gc("x", keep, rpy, soft_gb=100, free=200 * GB)
        assert r["freed"] == 0 and not r["removed_views"] and r["removed_blobs"] == 0, r
        assert os.path.isdir(os.path.join(R, "data", "prepared", "ds_old"))
        ok("free space above the soft limit: change nothing")

        soft = 1e-6  # 1000 bytes
        r = G.gc("x", keep, rpy, soft_gb=soft, free=0)
        assert r["removed_views"] == ["prepared/ds_old"], r
        assert os.path.isdir(os.path.join(R, "data", "prepared", "ds_keep"))
        assert not os.path.exists(os.path.join(R, "data", "prepared", "ds_old"))
        assert r["freed"] == 2002 + 0 and r["removed_blobs"] == 0, r  # cache 2000 + dataset.json 2; removing views is already above the soft limit
        assert all(exists(R, h) for h in ("aa1", "bb1", "cc1", "dd1", "ee1", "ff1"))
        assert os.path.getsize(outside) == 500
        ok("below the soft limit, remove views not in keep first (never follow links to delete targets); stop before removing blobs")

        R, outside = build(fixture.tmpdir(dir=tmp))
        rpy = local_rpy(R)
        r = G.gc("x", keep, rpy, soft_gb=5000 / GB, free=0)
        assert r["removed_views"] == ["prepared/ds_old"], r
        assert not exists(R, "dd1") and exists(R, "bb1"), r
        assert exists(R, "aa1") and exists(R, "ee1"), "blobs in use are kept"
        assert exists(R, "cc1") and exists(R, "ff1"), "nlink > 1 is kept"
        assert os.path.getsize(outside) == 500
        assert r["freed"] == 2002 + 3000 and r["removed_blobs"] == 1, r
        ok("then remove unreferenced blobs, oldest first; keep ones in use or hard-linked elsewhere, and never touch outside R")

        R, _ = build(fixture.tmpdir(dir=tmp))
        os.makedirs(os.path.join(R, "state", "pins"))
        with open(os.path.join(R, "state", "pins", "t__a.json"), "w") as f:
            json.dump([H("dd1")], f)
        part = blob(R, "gg1", 4000, ".tunnelpart")
        r = G.gc("x", keep, local_rpy(R), soft_gb=1.0, free=0)
        assert exists(R, "dd1") and os.path.exists(part) and not exists(R, "bb1"), r
        os.utime(os.path.join(R, "state", "pins", "t__a.json"), (1000, 1000))
        os.utime(part, (1000, 1000))
        r = G.gc("x", keep, local_rpy(R), soft_gb=1.0, free=0)
        assert not exists(R, "dd1") and not os.path.exists(part), r
        ok("keep blobs pinned by a staging task and half-transferred temp files; stop honouring pins after 12 hours and temp files after a day")

        # blobs on another disk with blob_cap: removing blobs looks only at blob_free, removing views at either side; removed blobs do not add to root free space
        R, _ = build(fixture.tmpdir(dir=tmp))
        r = G.gc("x", keep, local_rpy(R), soft_gb=5000 / GB, free=200 * GB, blob_free=0)
        assert r["removed_views"] == ["prepared/ds_old"] and not exists(R, "dd1"), r
        assert r["free"] == 200 * GB + 2002 and r["blob_free"] == r["freed"] - 2002 and r["removed_blobs"] >= 1, r
        assert exists(R, "aa1") and exists(R, "ee1") and exists(R, "cc1") and exists(R, "ff1"), r
        ok("with blob_free: blob side below the soft limit → remove views and unreferenced blobs; root free space does not include removed blobs")
        R, _ = build(fixture.tmpdir(dir=tmp))
        r = G.gc("x", keep, local_rpy(R), soft_gb=5000 / GB, free=0, blob_free=200 * GB)
        assert r["removed_views"] == ["prepared/ds_old"] and r["removed_blobs"] == 0 and exists(R, "dd1"), r
        ok("with blob_free: only root is below the soft limit → remove views, keep blobs (removing them would not free root space)")

        R, _ = build(fixture.tmpdir(dir=tmp))
        r = G.gc("x", keep, local_rpy(R), soft_gb=1.0, free=0)
        assert exists(R, "aa1") and exists(R, "ee1") and exists(R, "cc1") and exists(R, "ff1")
        assert not exists(R, "bb1") and not exists(R, "dd1") and r["removed_blobs"] == 2
        ok("when the soft limit cannot be reached, remove what can be removed and stop; keep ones in use")

        R, _ = build(fixture.tmpdir(dir=tmp))
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
        ok("registered task views are cleaned too: keep ones in keep and half-built ones (.incomplete); report views already gone so the registry drops them; reject out-of-bounds and single-level paths")

        # root reached through a symlink (view links point at the symlink path): blobs in use are still recognized
        real = fixture.tmpdir(dir=tmp)
        R, _ = build(real)
        link = os.path.join(tmp, "Rlink")
        os.symlink(R, link)
        pre = os.path.join(R, "data", "prepared", "ds_keep", "PlanM_full", "c1.prob")
        os.remove(pre)
        os.symlink(os.path.join(link, "blobs", H("aa1")[:2], H("aa1")), pre)
        r = G.gc("x", keep, local_rpy(link), soft_gb=1.0, free=0)
        assert exists(R, "aa1") and exists(R, "ee1") and not exists(R, "dd1"), r
        ok("blobs in use are kept when the root is reached through a symlink")

        # when keep is a deeper (or higher) level in a view, neither that view nor views under it are removed
        R, _ = build(fixture.tmpdir(dir=tmp))
        D = os.path.join(R, "data")
        os.makedirs(os.path.join(D, "in/up/sub"))
        os.symlink(blob(R, "up1", 10), os.path.join(D, "in/up/sub/f.dat"))
        r = G.gc("x", ["prepared/ds_keep/PlanM_full", "in/up/sub"], local_rpy(R), soft_gb=1e-6, free=0, views=["in/up"])
        assert os.path.exists(os.path.join(D, "prepared/ds_keep/PlanM_full/c1.prob")), r
        assert os.path.exists(os.path.join(D, "in/up/sub/f.dat")), r
        assert "prepared/ds_old" in r["removed_views"] and "prepared/ds_keep" not in r["removed_views"], r
        r = G.gc("x", ["prepared"], local_rpy(R), soft_gb=1e-6, free=0)
        assert os.path.isdir(os.path.join(D, "prepared/ds_keep")), r
        ok("keep is a level below or above a view: that view is kept")

        # Slurm: R/blobs is a symlink to another disk (/scr): remove unreferenced, unpinned blobs per the budget as usual; never touch things outside the blob dirs, not named by hash, or links
        R, _ = build(fixture.tmpdir(dir=tmp))
        scr = fixture.tmpdir(dir=tmp)
        os.rmdir(scr)
        os.rename(os.path.join(R, "blobs"), scr)
        os.symlink(scr, os.path.join(R, "blobs"))
        os.makedirs(os.path.join(R, "state", "pins"))
        with open(os.path.join(R, "state", "pins", "t__a.json"), "w") as f:
            json.dump([H("ee1")], f)
        os.remove(os.path.join(R, "data", "results", "ds_keep", "frag_0", "uses_ee1"))
        d2 = H("dd1")[:2]
        stray = [os.path.join(scr, "notes.txt"), os.path.join(scr, d2, "x1"), os.path.join(scr, d2, "sub", H("dd2"))]
        for p_ in stray:
            os.makedirs(os.path.dirname(p_), exist_ok=True)
            open(p_, "wb").write(b"z" * 100)
            os.utime(p_, (1, 1))
        os.symlink(outside, os.path.join(scr, d2, H("dd9")))
        r = G.gc("x", keep, local_rpy(R), soft_gb=1.0, free=0)
        assert not exists(R, "dd1") and not exists(R, "bb1") and r["removed_blobs"] == 2, r
        assert exists(R, "aa1") and exists(R, "ee1") and exists(R, "cc1") and exists(R, "ff1"), r
        assert all(os.path.exists(p_) for p_ in stray) and os.path.getsize(outside) == 500, r
        ok("blob dir is a symlink to another disk: remove unreferenced, unpinned blobs as usual; keep ones in use, pinned or hard-linked, and never touch non-hash names or link targets")

        # removing views and building views (site_ops view) take the same lock
        import fcntl
        import time
        R, _ = build(fixture.tmpdir(dir=tmp))
        os.makedirs(os.path.join(R, "state"))
        lk = open(os.path.join(R, "state", "view.lock"), "w")
        fcntl.flock(lk, fcntl.LOCK_EX)
        p = subprocess.Popen([sys.executable, SITE_OPS, "gc"], cwd=REPO, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                             env=dict(os.environ, TASKQ_ROOT=R))
        p.stdin.write(json.dumps(dict(soft_gb=1e-6, keep=keep, free=0, views=[], scan=["prepared"])))
        p.stdin.close()
        time.sleep(1)
        held = p.poll() is None and os.path.isdir(os.path.join(R, "data", "prepared", "ds_old"))
        fcntl.flock(lk, fcntl.LOCK_UN)
        lk.close()
        p.wait(timeout=60)
        assert held and p.returncode == 0 and not os.path.exists(os.path.join(R, "data", "prepared", "ds_old")), held
        ok("cleanup waits for the view lock (view.lock) when removing views")

        gc_pins_read_once(tmp)
        gc_envs(tmp)

        b0, G.budget = G.budget, lambda n: None
        try:
            assert G.gc("site-slurm2", keep, lambda *a, **k: 1 / 0) is None
        finally:
            G.budget = b0
        ok("machines without budget are not cleaned")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("all passed")


def gc_envs(tmp):
    """While an env is being built: wheels and conda packages it uses are pinned in pins/env.<env>.json and kept; built envs
    under R/envs are never touched by cleanup."""
    R = os.path.join(tmp, "Renv")
    os.makedirs(os.path.join(R, "data", "prepared"))
    h1, h2 = H("a1"), H("b2")
    for n in ("a1", "b2"):
        os.utime(blob(R, n, GB // 100), (1000, 1000))
    envd = os.path.join(R, "envs", "base")
    os.makedirs(os.path.join(envd, "lib"))
    for f in ("lib/big.so", ".ok"):
        with open(os.path.join(envd, f), "wb") as fh:
            fh.truncate(GB // 50)
        os.utime(os.path.join(envd, f), (1000, 1000))
    local_rpy(R)("x", "pin", dict(key=proto.safe("env.base"), hashes=[h1]))
    r = G.gc("x", [], local_rpy(R), soft_gb=1e-6, free=0)
    assert os.path.exists(os.path.join(R, "blobs", h1[:2], h1)) and not os.path.exists(os.path.join(R, "blobs", h2[:2], h2)), r
    assert sorted(os.listdir(envd)) == [".ok", "lib"] and os.path.getsize(os.path.join(envd, "lib", "big.so")) == GB // 50, r
    ok("GC: blobs pinned by an env build are kept, unpinned ones removed; built envs under R/envs (however large or old) are never touched")


def gc_pins_read_once(tmp):
    """Check pins before removing each blob: do not reread when pins did not change (GC is slow with hundreds of blobs and
    dozens of pins); pinned ones are still kept."""
    import time
    R = os.path.join(tmp, "Rpins")
    os.makedirs(os.path.join(R, "data", "prepared"))
    hs = [H(f"p{i}") for i in range(200)]
    for i in range(200):
        os.utime(blob(R, f"p{i}", 10), (1000, 1000))
    pd = os.path.join(R, "state", "pins")
    os.makedirs(pd)
    for i in range(20):
        with open(os.path.join(pd, f"t__{i}.json"), "w") as f:
            json.dump(hs[i * 5:i * 5 + 1], f)
    old = time.time() - 60
    os.utime(pd, (old, old))
    hook = ("import json\n_l=json.load\n_n=[0]\n"
            "def _load(*a,**k):\n    _n[0]+=1\n    return _l(*a,**k)\n"
            "json.load=_load\nimport atexit,sys\natexit.register(lambda: sys.stderr.write('LOADS=%d\\n'%_n[0]))\n"
            "import runpy\nsys.argv=[sys.argv[1],'gc']\nrunpy.run_path(sys.argv[0],run_name='__main__')")
    r = subprocess.run([sys.executable, "-c", hook, SITE_OPS], cwd=REPO, capture_output=True, text=True, env=dict(os.environ, TASKQ_ROOT=R),
                       input=json.dumps(dict(soft_gb=1e6, keep=[], free=0, views=[], scan=["prepared"])))
    assert r.returncode == 0, r.stderr
    loads = int(r.stderr.split("LOADS=")[1].split()[0])
    left = [h for h in hs if os.path.exists(os.path.join(R, "blobs", h[:2], h))]
    assert left == hs[0:100:5], left
    assert loads <= 3 * 20, f"read pins {loads} times (20 pins, 180 blobs removed)"
    ok("GC: pins are not reread when unchanged, and pinned blobs are still kept")


if __name__ == "__main__":
    main()
