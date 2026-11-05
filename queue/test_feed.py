#!/usr/bin/env python3
"""feed.py 的测试（2026-10-31）：run_all 转给替身 relay.py，nobody 用构造的总队列。不连任何机器。
  python3 queue/test_feed.py"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata"))
import fixture  # noqa: E402
T = fixture.tmpdir()
os.environ.update(FEED_LOG=os.path.join(T, "feed.log"))
sys.path.insert(0, HERE)

fixture.use()
import feed as F  # noqa: E402

fails = 0


def ok(msg, cond):
    global fails
    print(("通过：" if cond else "失败：") + msg)
    fails += not cond


def ct(key, state="free", assigned=None):
    return dict(key=key, state=state, assigned=assigned)


# ---------------- 分给了不是执行机的名字（执行机改名、撤掉以后留下的）----------------
q_ = dict(tasks=[ct("a/x/0", "claimed", assigned="pueue-host"), ct("a/x/1", "claimed", assigned="site-pueue"),
                 ct("a/x/2", "claimed", assigned="site-slurm"), ct("a/x/3", "done", assigned="old")])
nb = F.nobody(q_)
ok("分给 pueue-host（不是执行机）的报 feed:nobody:pueue-host，带交还的命令；分给 relay 执行机的不报；做完的不报",
   list(nb) == ["feed:nobody:pueue-host"] and "release --worker pueue-host" in F.alert_msg(nb["feed:nobody:pueue-host"])
   and "a/x/0" in F.alert_msg(nb["feed:nobody:pueue-host"]))
ok("feed:nobody 带结构化数据（11/4）：worker、n、keys", nb["feed:nobody:pueue-host"]["data"] == dict(worker="pueue-host", n=1, keys=["a/x/0"]))
ok("总队列读不到：不报", F.nobody(None) == {})

# ---------------- 送任务只有一个入口（10/31）：feed.run_all 转给 relay.py ----------------
rl, rcalls = types.ModuleType("relay"), []
rl.run_all = lambda s, dry=False: (rcalls.append(dry), (["site-pueue 领了 a/x/0"], {"site:site-pueue:x": "y"}))[1]
sys.modules["relay"] = rl
hub = F.M.hub()
ev, st = F.run_all(dict(taskq={hub: q_}, ok={}), dry=False)
ok("转给 relay.run_all，说明和问题并进来，加上 feed:nobody，feed.log 里有一行指向 ~/relay.log",
   rcalls == [False] and ev == ["site-pueue 领了 a/x/0"] and set(st) == {"site:site-pueue:x", "feed:nobody:pueue-host"}
   and "见 ~/relay.log" in open(F.LOG).read())
n0 = len(open(F.LOG).read())
ev, st = F.run_all(dict(taskq={}, ok={}), dry=True)
ok("dry：照样问 relay（带 dry），不写 feed.log", rcalls == [False, True] and len(open(F.LOG).read()) == n0)
rl.run_all = lambda s, dry=False: 1 / 0
ev, st = F.run_all(dict(taskq={}, ok={}), dry=True)
ok("relay.py 出错：报 relay:error", list(st) == ["relay:error"] and "ZeroDivisionError" in st["relay:error"])
del sys.modules["relay"]

print("全部通过" if fails == 0 else f"{fails} 项失败")
sys.exit(1 if fails else 0)
