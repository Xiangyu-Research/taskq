#!/usr/bin/env python3
"""中转执行机（envs/machines.py 的 RELAY，现在是 dicc2；10/30）的接力：由 box 替它在总队列（box 上的任务队列，10/31 起）领任务、
把数据换成别名传过去、放进它自己的任务队列，训完拉回、换回真名、归档。两边不直接通信，一切经 box。

那边（根目录 R，machines.py 的 root）：
  R/code/<包名>、R/code/current   scripts/queue/relay_bundle.py 打的代码包（origin/main 变了就重打、切换 current；旧包留最新 3 个，
                                  4 天以上的删）；worker 和 DICC 同一套脚本（作业名、资源、脚本名分开，见 machines.py 的 RELAY）
  R/queue/                        它自己的任务队列（原版 taskq.py，任务用别名）
  R/blobs/<id[:2]>/<id>           数据文件，id = sha256（内容相同的只传一次）
  R/data/                         数据目录：数据集是指向 blobs 的软链接 + 改写过的 JSON（scripts/queue/relay_names.py），训练结果用别名
每次健康检查（scripts/monitor/health.py，每 30 分钟）调用 run_all：
  1. ssh 一次读那边的队列、各折进度、作业、剩余空间（写到 ~/.cache/phd/relay/status.json，登记表用）；
  2. 总队列里 worker 是它的每一折：
     - 还没放进那边的队列 → box 的 wait 组排 relay.py stage（翻译、补传缺的数据、建软链接、加进那边的队列）；
     - 在那边排着、在训 → 续上（beat）；
     - 那边训完 → box 的 wait 组排 relay.py fetch && archive_task.sh box（拉回、换回真名、备份到 DICC）；
     - 总队列显示训完（box 上有了），而且拉回、归档的 box 任务成功了 → 交还，删掉那边的任务和结果；
     - 那边失败：没有进度 → 交还（--skip，它以后不再领），删掉那边的任务；有进度 → 领取保留、报出来；
     - 总队列里停了（taskq.py stop）→ 那边也停，那边不再领着以后交还（那边的结果留着）；撤销停止后再领到时那边接着训；
     - 传数据、拉回出错、验证集对不上 → 领取保留、报出来（处理后 pueue remove 那个 box 任务，下次检查重排）。
  3. 那边排着没人领的任务少于 envs/policy.py 的 READY[名字] 时，按 refill.py 的挑法领下一折（只领能翻译的做法，
     数据传过去后那边至少还剩 HARD_GB）；
  4. 那边排着和在跑的 worker 作业不够时补交（refill.py 的 topup_plan，QOS 和个数见 SLURM[名字]["gpu"]）。
开关 envs/policy.py 的 AUTO[名字]：关着时第 3、4 步只记进 ~/relay.log（影子），已经领着的折照常推进。
连不上它时只续总队列的领取；领着折、连不上 2 小时以上报出来。

    python3 scripts/queue/relay.py                         看现在会做什么（不改任何东西）
    python3 scripts/queue/relay.py stage <名字> <key> --nice N [--no-add]   传数据、加进那边的队列（--no-add：只传数据）
    python3 scripts/queue/relay.py fetch <名字> <key>      拉回训完的一折、换回真名，放进 box 的数据目录
    python3 scripts/queue/relay.py bundle <名字> [--commit C]   打包、推过去、切换 current
    python3 scripts/queue/relay.py status <名字>           那边的队列
"""
import argparse
import base64
import fcntl
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "envs"))
sys.path.insert(0, os.path.join(REPO, "scripts", "sync"))
sys.path.insert(0, os.path.join(REPO, "scripts", "monitor"))
import machines as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402
import relay_gc  # noqa: E402
import taskq  # noqa: E402
import refill  # noqa: E402
import relay_names as RN  # noqa: E402
import relay_bundle as RB  # noqa: E402
from common import write_json  # noqa: E402

DIR = RN.RELAY_DIR
STATE = os.path.join(DIR, "state.json")
STATUS = os.path.join(DIR, "status.json")
HASHES = os.path.join(DIR, "hashes.json")
LOG = os.path.expanduser(os.environ.get("RELAY_LOG", "~/relay.log"))
HARD_GB = 100  # 领之前：传过去以后那边至少还剩这么多
DOWN_S = 2 * 3600  # 领着折、连不上这么久就报
LATE_S = 3600  # 拉回、归档成功这么久了 box 上还不算训完，就报
LIVE = refill.LIVE


def conf(name):
    return M.RELAY[name]


def root(name):
    return conf(name)["root"].rstrip("/")


def py(name):
    return conf(name)["conda"][0] + "/bin/python3"


def box_data():
    return M.expand(M.MACHINES[M.hub()]["data"])


def ssh_argv():
    return shlex.split(os.environ.get("RELAY_SSH", "ssh")) + C.SSH[1:]  # RELAY_SSH 只给测试用


# ---------------- 状态文件 ----------------
def update_state(fn):
    os.makedirs(DIR, exist_ok=True)
    with open(STATE + ".lock", "a") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        st = load_state()
        out = fn(st)
        write_json(STATE, st)
    return out


def load_state():
    try:
        with open(STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def keys_of(st, name):
    """真 key → dict(alias, recipe, params, data（真的数据路径）, staged)。"""
    return (st.get("keys") or {}).get(name) or {}


def relayable(i):
    """refill.info 的结果 → 能不能领给它：做法能翻译（按本机的 recipes.py 算，总队列那边的代码可能还没更新），训练器不读仓库里的病例子集。"""
    sp = i["spec"]
    rec, params = i["recipe"] or sp.get("recipe"), i["params"] or sp.get("params") or {}
    try:
        relay = S.resolve(rec, params).get("relay")
    except S.SpecError:
        relay = sp.get("relay")
    return relay in RN.TRANSLATORS and not repo_files(params.get("trainer"))


def repo_files(trainer):
    """训练器要读仓库里的病例子集（SPLITS_DIR，如 n470、frac）→ True：那边没有、也没换名，不领。"""
    d = os.path.join(REPO, "research", "common", "nnunet", "trainers")
    for f in os.listdir(d) if trainer else []:
        if f.endswith(".py"):
            src = open(os.path.join(d, f), encoding="utf-8").read()
            if f"class {trainer}(" in src:
                return "SPLITS_DIR" in src
    return False


# ---------------- 在那边跑 ----------------
def rsh(name, cmd, input=None, timeout=300):
    return C.run(cmd, conf(name)["ssh"], timeout, input=input, ssh=ssh_argv())


def prefix(name):
    r = shlex.quote(root(name))
    return f"cd {r}/code/current && TASKQ_DIR={r}/queue {RB.rename('BRAIN_TUMOR')}={r}/data"


def rtq(name, args, timeout=180):
    """那边的 taskq.py <args> → (退出码, 输出)；连不上 → None。"""
    rc, out, err = rsh(name, f"{prefix(name)} {py(name)} scripts/queue/taskq.py {args}", timeout=timeout)
    return None if rc in (124, 255) else (rc, (out + err).strip())


def rpy(name, code, data=None, timeout=600):
    """在那边跑一段 Python（第一个参数是根目录，data 以 JSON 经标准输入）→ 它 json_out 的结果。出错 → RuntimeError。"""
    rc, out, err = rsh(name, f"{prefix(name)} {py(name)} -c {shlex.quote(code)} {shlex.quote(root(name))}",
                       input=json.dumps(data), timeout=timeout)
    res = S.json_in(out)
    if rc or res is None:
        raise RuntimeError(f"{conf(name)['label']} 上出错（{rc}）：{(err or out).strip()[-300:]}")
    return res


HEAD = "import json,os,sys\nsys.path.insert(0,'scripts/queue')\nimport spec\nR=sys.argv[1]\nA=json.load(sys.stdin)\n"

STATUS_PY = HEAD + """import shutil,subprocess,taskq
st=taskq.local_status()
sp=[{k:t['spec'].get(k) for k in spec.COLLECT_FIELDS} for t in st['tasks'] if t.get('spec')]
col=spec.collect(sp,os.environ['@DATA@'])
sq=jobs=None
try:
    if A.get('sched')=='pueue':
        q=subprocess.run([os.path.expanduser(A['pueue']),'status','--json'],capture_output=True,text=True,timeout=60)
        jobs=[]
        for i,t in sorted(json.loads(q.stdout)['tasks'].items(),key=lambda x:int(x[0])):
            s_=t['status']
            st_=s_ if isinstance(s_,str) else list(s_)[0]
            res=(s_.get('Done') or {}).get('result') if isinstance(s_,dict) and isinstance(s_.get('Done'),dict) else None
            if t['group']==A['group']:
                jobs.append([int(i),t.get('label') or '',st_,str(res) if res is not None else None])
    else:
        q=subprocess.run(['squeue','-u',os.environ.get('USER',''),'-h','-o','%i|%j|%T|%M|%q|%R'],capture_output=True,text=True,timeout=60)
        sq=[l.split('|') for l in q.stdout.splitlines()] if q.returncode==0 else None
except Exception:
    sq=jobs=None
spec.json_out(dict(taskq=st,folds=col['folds'],squeue=sq,jobs=jobs,free=shutil.disk_usage(R).free,
                   bundle=os.path.basename(os.path.realpath(os.path.join(R,'code','current')))))
"""

STATUS_PY = STATUS_PY.replace("@DATA@", RB.rename("BRAIN_TUMOR"))

MISSING_PY = HEAD + "spec.json_out([i for i in A if not os.path.isfile(os.path.join(R,'blobs',i[:2],i))])\n"

ADOPT_PY = HEAD + """import hashlib
old=os.path.expanduser(A['old'])
got=[]
for h,(rel,size) in A['files'].items():
    p=os.path.join(old,rel)
    try:
        if os.path.getsize(p)!=size:
            continue
    except OSError:
        continue
    x=hashlib.sha256()
    with open(p,'rb') as f:
        for b in iter(lambda:f.read(1<<22),b''):
            x.update(b)
    if x.hexdigest()!=h:
        continue
    t=os.path.join(R,'blobs',h[:2],h)
    os.makedirs(os.path.dirname(t),exist_ok=True)
    if not os.path.exists(t):
        try:
            os.link(os.path.realpath(p),t)
        except OSError:
            continue
    got.append(h)
spec.json_out(got)
"""

VIEW_PY = HEAD + """import base64
D=os.path.join(R,'data')
d=os.path.join(D,A['dir'])
os.makedirs(d,exist_ok=True)
inc=os.path.join(d,'.incomplete')
open(inc,'w').close()
want={inc}
def put(p,f):
    os.makedirs(os.path.dirname(p),exist_ok=True)
    tmp=p+'.relaytmp'
    if os.path.lexists(tmp):
        os.remove(tmp)
    f(tmp)
    os.replace(tmp,p)
for rel,h in A['links'].items():
    p=os.path.join(D,rel)
    t=os.path.join(R,'blobs',h[:2],h)
    want.add(p)
    if not os.path.isfile(t):
        sys.exit('missing '+h)
    if not (os.path.islink(p) and os.readlink(p)==t):
        put(p,lambda x:os.symlink(t,x))
for rel,b in A['texts'].items():
    p=os.path.join(D,rel)
    want.add(p)
    def w(x,b=b):
        with open(x,'wb') as f:
            f.write(base64.b64decode(b))
    put(p,w)
for dp,_,fs in os.walk(d):
    for f in fs:
        if os.path.join(dp,f) not in want:
            os.remove(os.path.join(dp,f))
os.remove(inc)
spec.json_out(len(want)-1)
"""

LIST_PY = HEAD + """import fnmatch,glob
D=os.path.join(R,'data')
fd=spec.out_dir(A,D)
ex=(A.get('fetch') or {}).get('exclude') or []
items=[]
def add(p):
    if os.path.isfile(p):
        items.append([os.path.relpath(p,D),os.path.getsize(p)])
for dp,_,fs in os.walk(fd):
    for f in fs:
        if not any(fnmatch.fnmatch(f,x) for x in ex):
            add(os.path.join(dp,f))
for x in (A.get('fetch') or {}).get('extra') or []:
    for p in glob.glob(os.path.join(os.path.dirname(fd),x)):
        add(p)
for p in glob.glob(os.path.join(D,'logs','tasks',glob.escape(spec.safe(A['key']))+'__*')):
    add(p)
spec.json_out(dict(done=spec.is_done(A,D),items=items))
"""


def remote_status(name):
    """→ 那边的状态 dict；连不上或出错 → None。"""
    c = conf(name)
    try:
        return rpy(name, STATUS_PY, dict(sched=c["scheduler"], pueue=c.get("pueue"), group=(c.get("worker") or {}).get("group")),
                   timeout=300)
    except RuntimeError:
        return None


# ---------------- 代码包 ----------------
def deploy(name, bid, path):
    """把包推过去、切换 current（原子的 mv -T）；第一次时建目录、开队列。"""
    r = shlex.quote(root(name))
    b = shlex.quote(bid)
    tar = subprocess.run(["tar", "-C", path, "-cf", "-", "."], capture_output=True, check=True).stdout
    cmd = (f"set -e; R={r}; mkdir -p $R/code $R/queue $R/blobs $R/tmp $R/state $R/data/logs/tasks $R/data/logs/workers; "
           f"[ -e $R/queue/tasks.jsonl ] || {{ : > $R/queue/tasks.jsonl; touch $R/queue/ENABLED; }}; cd $R/code; "
           f"if [ ! -d {b} ]; then rm -rf {b}.tmp; mkdir {b}.tmp; tar -xf - -C {b}.tmp; mv -T {b}.tmp {b}; fi; "
           f"ln -sfn {b} current.tmp; mv -T current.tmp current; "
           "ls -1t | grep -vx current | grep -v '\\.tmp$' | tail -n +4 | "
           "while read -r d; do find \"$d\" -maxdepth 0 -mtime +4 -exec rm -rf {} +; done")
    r2 = subprocess.run(ssh_argv() + [conf(name)["ssh"], cmd], input=tar, capture_output=True, timeout=600)
    if r2.returncode:
        raise RuntimeError(f"推代码包 {bid} 到 {conf(name)['label']} 出错：{r2.stderr.decode(errors='replace')[-300:]}")


def bundle(name, commit="origin/main", have=None, run_build=None):
    """origin/main 打的包和那边的 current 不同就推过去 → 说明（没变 → None）。"""
    bid, path = (run_build or RB.build)(name, commit)
    if bid == have:
        return None
    deploy(name, bid, path)
    return f"{conf(name)['label']} 的代码换成 {bid}（{commit}）"


# ---------------- 传数据（box 的 wait 组里跑） ----------------
def sha256s(paths):
    """真实路径 → sha256，按（路径、大小、修改时间）缓存在 HASHES。"""
    try:
        cache = json.load(open(HASHES))
    except (OSError, ValueError):
        cache = {}
    out, new = {}, False
    for p in paths:
        st = os.stat(p)
        k = f"{p}|{st.st_size}|{st.st_mtime_ns}"
        if k not in cache:
            h = hashlib.sha256()
            with open(p, "rb") as f:
                for blk in iter(lambda: f.read(1 << 22), b""):
                    h.update(blk)
            cache[k], new = h.hexdigest(), True
        out[p] = cache[k]
    if new:
        write_json(HASHES, cache, indent=None)
    return out


def lookup(key):
    s = S.lookup(key)
    if s is None:
        raise RN.RelayError(f"实验表和总队列里都没有 {key}")
    return s if s.get("exp") else S.resolve(s["recipe"], s["params"])


def stage(name, key, nice=None, add=True, say=print):
    """box 上的数据 → 那边（别名）；add 时再加进那边的队列。→ 别名的 key。"""
    import tunnel
    real, D = lookup(key), box_data()
    ex = RN.export(real, D, RN.Names())
    alias = S.resolve(real["recipe"], ex["params"])
    hs = sha256s(sorted({p for _, p in ex["files"]}))
    links = {rel: hs[p] for rel, p in ex["files"]}
    miss = rpy(name, MISSING_PY, sorted(set(links.values())))
    say(f"{key} → {alias['key']}：{len(links)} 个文件，那边缺 {len(miss)} 个")
    if miss and conf(name).get("adopt"):  # 那边老数据目录里内容一样的文件硬链接进 blobs，不重传
        by = {h: p for p, h in hs.items()}
        files = {h: (os.path.relpath(by[h], D), os.path.getsize(by[h])) for h in miss if by[h].startswith(D.rstrip("/") + "/")}
        got = set(rpy(name, ADOPT_PY, dict(old=conf(name)["adopt"], files=files), timeout=3 * 3600)) if files else set()
        miss = [h for h in miss if h not in got]
        say(f"那边老数据目录里收编了 {len(got)} 个，还缺 {len(miss)} 个")
    if miss:
        by = {h: p for p, h in hs.items()}
        os.makedirs(DIR, exist_ok=True)
        tmp = tempfile.mkdtemp(prefix="push_", dir=DIR)
        try:
            for h in miss:
                os.makedirs(os.path.join(tmp, h[:2]), exist_ok=True)
                os.symlink(by[h], os.path.join(tmp, h[:2], h))
            bad = tunnel.push(conf(name)["ssh"], tmp, root(name) + "/blobs", [f"{h[:2]}/{h}" for h in miss])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        if bad:
            raise RN.RelayError(f"{len(bad)} 个文件没传到或对不上（下次重传）")
    top = os.path.commonpath(list(links) + list(ex["texts"]))
    n = rpy(name, VIEW_PY, dict(dir=top, links=links, texts={k: base64.b64encode(v).decode() for k, v in ex["texts"].items()}))
    say(f"那边的 {top} 建好了（{n} 个文件）")

    def rec(st):
        st.setdefault("keys", {}).setdefault(name, {})[key] = dict(
            alias=alias["key"], recipe=real["recipe"], params=ex["params"], data=real.get("data") or [], staged=time.time(), view=top)
    update_state(rec)
    if add:
        r = rtq(name, f"add --nice {int(nice)} --recipe " + " ".join(shlex.quote(RB.rename(x)) for x in S.kv_args(real["recipe"], ex["params"])))
        if r is None or r[0]:
            raise RuntimeError(f"加进 {conf(name)['label']} 的队列出错：{(r or [0, 'ssh 不通'])[1][-300:]}")
        say(r[1])
    return alias["key"]


# ---------------- 拉回（box 的 wait 组里跑） ----------------
def place(src, D, written, real):
    """换回真名的结果（src 下）挪进数据目录：普通文件 → 上一级的 JSON（已有的不动）→ 训完的标志（summary.json 最后）。"""
    rout = real["out"]
    done = {f"{rout}/{alt}" for item in real["done"] for alt in item.split("|")}
    parent = [w for w in written if os.path.dirname(w) == os.path.dirname(rout)]
    last = sorted((w for w in written if w in done), key=lambda w: w.endswith("summary.json"))
    first = [w for w in written if w not in done and w not in parent]

    def mv(w):
        p = os.path.join(D, w)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        os.replace(os.path.join(src, w), p)
    for w in first:
        mv(w)
    for w in parent:
        if not os.path.exists(os.path.join(D, w)):
            mv(w)
    for w in last:
        mv(w)


def fetch(name, key, say=print):
    import tunnel
    real, D = lookup(key), box_data()
    k = keys_of(load_state(), name).get(key)
    if not k:
        raise RN.RelayError(f"{key} 没有传给 {conf(name)['label']} 的记录")
    alias = S.resolve(k["recipe"], k["params"])
    res = rpy(name, LIST_PY, alias)
    if not res["done"]:
        raise RN.RelayError(f"{conf(name)['label']} 上 {alias['key']} 还没训完")
    inbox = os.path.join(D, ".relay_in", S.safe(alias["key"]))
    out = inbox + ".real"
    for d in (inbox, out):
        shutil.rmtree(d, ignore_errors=True)
    bad = tunnel.pull(conf(name)["ssh"], root(name) + "/data", inbox, res["items"])
    if bad:
        raise RN.RelayError(f"{len(bad)} 个文件没拉到（{bad[:3]}）")
    written = RN.import_(alias, real, inbox, out, D, RN.Names())
    place(out, D, written, real)
    for d in (inbox, out):
        shutil.rmtree(d, ignore_errors=True)
    say(f"{key} 拉回了 {len(written)} 个文件")


# ---------------- 每次检查 ----------------
def box_task(s, step, key, since=0):
    """box 上 step 这一步、这个 key 的最新任务（任务记录在 since 之后加的）→ pueue 任务或 None。"""
    hub, last = M.hub(), None
    for t in sorted((s["pueue"].get(hub) or {}).get("tasks", []), key=lambda t: t["id"]):
        r = C.identify(hub, t, s.get("records") or [])
        if r and r["step"] == step and r["key"] == key and r.get("added", 0) >= since - 60:
            last = t
    return last


def progress(f):
    return bool(f) and (f.get("final") or any((r.get("epochs_done") or 0) > 0 for r in f.get("runs") or []))


def plan(s, name, rs, st, now=None):
    """→ (动作, 卡住的 {问题编号: 说明})。rs：那边的状态（None = 连不上）；st：load_state()。
    动作 dict(op=beat|release|stage|fetch|drop|rstop|rresume, key, …)。"""
    now = now or time.time()
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    L, keys = conf(name)["label"], keys_of(st, name)
    mine = [t for t in (mtq or {}).get("tasks", []) if t.get("worker") == name]
    acts, stuck = [], {}
    if not mtq or not mtq.get("enabled"):
        return [], {}
    if rs is None:
        acts = [dict(op="beat", key=t["key"]) for t in mine if t["state"] != "done"]
        last = (st.get("last_ok") or {}).get(name, 0)
        if acts and now - last > DOWN_S:
            stuck[f"site:{name}:down"] = (f"{L} 读不到状态{f' {(now - last) / 3600:.0f} 小时' if last else ''}（ssh 不通，或那边的存储挂住），"
                                           f"领着 {len(acts)} 折，那边的进度看不到")
        return acts, stuck
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}
    folds = rs.get("folds") or {}
    owned = set()
    for t in mine:
        key = t["key"]
        k = keys.get(key)
        a = rt.get(k["alias"]) if k else None
        if a:
            owned.add(a["key"])
        if t["state"] == "done":
            ft = box_task(s, "relay_fetch", key, t.get("since") or 0)
            if ft is not None and ft["status"] in LIVE:  # box 上有了，但拉回、归档还没做完：等
                acts.append(dict(op="beat", key=key))
                continue
            if ft is not None and ft["status"] != "Success":
                acts.append(dict(op="beat", key=key))
                stuck[f"relayfetch:{key}"] = (f"从 {L} 拉回、归档 {key} 的 box 任务 {ft['id']} 出错了（{ft['status']}），box 上有了、领取保留；"
                                              f"看 pueue log {ft['id']}，处理后 pueue remove {ft['id']}（下次检查重排）")
                continue
            acts.append(dict(op="release", key=key, why="训完了，box 上有了、归档到 DICC 了"))
            if a:
                acts.append(dict(op="drop", key=key, alias=a["key"], out=a["spec"]["out"]))
            continue
        acts.append(dict(op="beat", key=key))
        if t["state"] == "stopped" and not (a and a["state"] == "done"):
            if a and a["state"] != "stopped":
                acts.append(dict(op="rstop", key=key, alias=a["key"]))
            elif not (a and a.get("worker")):  # 那边不再领着（或者还没放过去）
                acts[-1] = dict(op="release", key=key, why="停止（taskq.py stop）" + ("，那边的结果留着" if a else ""))
            continue
        since = t.get("since") or 0
        if a is None:
            sg = box_task(s, "relay_stage", key, since)
            if sg is None or sg["status"] == "Success":
                acts.append(dict(op="stage", key=key, nice=t["nice"]))
            elif sg["status"] not in LIVE:
                stuck[f"relaystage:{key}"] = (f"给 {L} 传 {key} 的 box 任务 {sg['id']} 出错了（{sg['status']}），领取保留；"
                                              f"看 pueue log {sg['id']}，处理后 pueue remove {sg['id']}（下次检查重传）")
            continue
        ast = a["state"]
        if ast == "done":
            ft = box_task(s, "relay_fetch", key, since)
            if ft is None:
                acts.append(dict(op="fetch", key=key))
            elif ft["status"] == "Success":
                end = C.local_time(ft.get("end"))
                if end is not None and now - end.timestamp() > LATE_S:
                    stuck[f"relayfetch:{key}"] = (f"{L} 训完的 {key} 拉回并归档成功了（box 任务 {ft['id']}），box 上却还不算训完；"
                                                  f"看 box 上的结果文件夹")
            elif ft["status"] not in LIVE:
                stuck[f"relayfetch:{key}"] = (f"从 {L} 拉回 {key} 的 box 任务 {ft['id']} 出错了（{ft['status']}），领取保留；"
                                              f"看 pueue log {ft['id']}，处理后 pueue remove {ft['id']}（下次检查重排）")
        elif ast == "stopped":
            acts.append(dict(op="rresume", key=key, alias=a["key"]))
        elif ast == "failed":
            if progress(folds.get(a["key"])):
                stuck[f"site:{name}:fail:{key}"] = (f"{L} 上 {key}（{a['key']}）训练失败 {a.get('failures')} 次，有进度，领取保留；"
                                                    f"看那边的 logs/tasks/；放弃：taskq.py release --worker {name} {key}")
            else:
                acts[-1] = dict(op="release", key=key, skip=f"{L} 上训练失败", why=f"{L} 上训练失败、没有进度，交还，{L} 不再领它")
                acts.append(dict(op="drop", key=key, alias=a["key"], out=a["spec"]["out"]))
        elif ast in ("bad", "blocked"):
            stuck[f"site:{name}:fail:{key}"] = f"{L} 队列里的 {a['key']}（{key}）{ast}：{a.get('note')}"
    rev = {v["alias"]: k for k, v in keys.items()}
    mt = {t["key"]: t for t in (mtq or {}).get("tasks", [])}
    for a in rs["taskq"]["tasks"]:
        if a["key"] in owned:
            continue
        real = rev.get(a["key"])
        m = mt.get(real) if real else None
        if real is None:
            stuck[f"site:{name}:orphan:{a['key']}"] = f"{L} 队列里的 {a['key']} 认不出是哪一折（{DIR}/state.json 里没有）"
        elif a["state"] in ("claimed", "busy"):
            stuck[f"site:{name}:orphan:{real}"] = f"{L} 在训 {real}，但总队列里不是它领着；看总队列和 {L} 的队列"
        elif m is None or m["state"] == "done":
            acts.append(dict(op="drop", key=real, alias=a["key"], out=a["spec"]["out"]))
        elif a["state"] != "stopped":
            acts.append(dict(op="rstop", key=real, alias=a["key"]))
    if rs["taskq"].get("paused"):
        stuck[f"site:{name}:fail"] = f"{L} 的任务队列 3 小时内失败 {rs['taskq'].get('recent_failures')} 次，暂停领任务"
    return acts, stuck


def fill(s, name, rs, st, acts):
    """那边备好、没人领的任务不够 READY 个时领下一折 → take 动作；不领 → ([], 原因)。"""
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    if not mtq or not mtq.get("enabled") or mtq.get("paused"):
        return [], "总队列关着或暂停中"
    if any(n not in s["src"] for n in M.names("train")):
        return [], "有训练机的进度读不到"
    want = P.READY.get(name, 1)
    keys = keys_of(st, name)
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}

    def waiting(t):  # 领着、还没开训：在传数据，或在那边排着
        a = rt.get(keys[t["key"]]["alias"]) if t["key"] in keys else None
        return a is None or a["state"] in ("free", "stale", "waiting")
    have = sum(1 for t in mtq.get("tasks", []) if t.get("worker") == name
               and t["state"] not in ("done", "stopped") and waiting(t))
    if have >= want:
        return [], f"那边排着 {have} 个"
    ready = {p for k in keys.values() for p in k.get("data") or []}
    cands = [t for t in refill.candidates(s, name, mtq, ready)
             if relayable(refill.info(mtq, t["key"]))]
    free = rs.get("free", 0) / 1e9
    hard = (conf(name).get("budget") or (None, HARD_GB))[1]
    out, took = [], set()
    for _ in range(want - have):
        t, need, gb, _ = refill.next_pick(mtq, cands, ready, free, hard, took)
        if t is None:
            break
        free -= gb
        took.add(t["key"])
        out.append(dict(op="take", key=t["key"], nice=t["nice"]))
    return out, "" if out else ("队列里没有能领的折" if not cands else f"放不下（那边剩 {rs.get('free', 0) / 1e9:.0f} GB）")


def worker_jobs(name, rs):
    w = conf(name)["worker"]
    return [j for j in rs.get("jobs") or [] if j[1] == f"worker:{w['id']}"]


def topup(name, rs, run=None):
    """→ [(QOS, 时限, 补几个)], 不补的原因。pueue 类：组里没有活着的 worker 时开一个 → [("pueue", None, 1)]。"""
    if conf(name)["scheduler"] == "pueue":
        if rs.get("jobs") is None:
            return [], "读不到 pueue"
        mine = worker_jobs(name, rs)
        if any(j[2] != "Done" for j in mine):
            return [], "worker 在"
        if mine and mine[-1][3] not in ("Success", "Killed"):
            return [], f"上一个 worker（pueue 任务 {mine[-1][0]}）失败了（{mine[-1][3]}）"
        return [("pueue", None, 1)], ""
    conf_ = P.SLURM.get(name) or {}
    if not conf_.get("gpu"):
        return [], "envs/policy.py 的 SLURM 里没有它"
    rc, out, _ = (run or rsh)(name, refill.topup_probe(name))
    if rc:
        return [], "读不到作业数"
    counts = {}
    for ln in out.splitlines():
        p = ln.split("|")
        if len(p) == 4 and p[0] == "QOS" and p[3].isdigit():
            counts[p[1]] = (int(p[2]) if p[2].isdigit() else None, int(p[3]))
    subs, why = refill.topup_plan(rs["taskq"], counts, conf_["gpu"])
    return [(q, t, n) for q, t, n, _ in subs], why


def label(op, key):
    return f"{op}:{key}"


def execute(name, acts, s, run_add=None):
    """做动作 → [说明]。出错的那一步写进说明，后面的照做。"""
    L, msgs = conf(name)["label"], []
    add = run_add or refill.add

    def master(args):
        r = taskq.central(args)
        return r[0], r[1] + r[2]
    beats = [a["key"] for a in acts if a["op"] == "beat"]
    if beats:
        r = master(f"beat --worker {name} " + " ".join(shlex.quote(k) for k in beats))
        if r is None or r[0]:
            msgs.append(f"{L} 的领取续不上（{(r or [0, 'ssh 不通'])[1][-150:]}）")
    for a in acts:
        k = a["key"]
        try:
            if a["op"] == "release":
                r = master(f"release --worker {name} {shlex.quote(k)}" + (f" --skip {shlex.quote(a['skip'])}" if a.get("skip") else ""))
                msgs.append(f"{L} 交还 {k}：{a['why']}" if r and r[0] == 0 else f"{L} 交还 {k} 出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
            elif a["op"] == "take":
                r = master(f"take --worker {name} --assign {shlex.quote(k)}")
                if r is None or r[0]:
                    msgs.append(f"{L} 没领到 {k}：{(r or [0, 'ssh 不通'])[1].strip()[-150:]}")
                    continue
                msgs.append(f"{L} 领了 {k}（档位 {a['nice']}）")
                a = dict(a, op="stage")
            if a["op"] == "stage":
                lab = label("relay_stage", k)
                cmd = f"python3 scripts/queue/relay.py stage {name} {shlex.quote(k)} --nice {int(a['nice'])}"
                tid = add(M.hub(), lab, "relay_stage", k, f"wait -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group="wait")
                msgs.append(f"{L} 的 {k} 排了传数据（box 任务 {tid}）")
            elif a["op"] == "fetch":
                lab = label("relay_fetch", k)
                cmd = (f"python3 scripts/queue/relay.py fetch {name} {shlex.quote(k)} && "
                       f"bash scripts/queue/archive_task.sh {M.hub()} {shlex.quote(k)}")
                tid = add(M.hub(), lab, "relay_fetch", k, f"wait -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group="wait")
                msgs.append(f"{L} 训完了 {k}，排了拉回、归档（box 任务 {tid}）")
            elif a["op"] == "drop":
                r = rtq(name, f"remove {shlex.quote(a['alias'])}")
                rr = shlex.quote(root(name))
                rc, out, err = rsh(name, f"rm -rf {rr}/data/{shlex.quote(a['out'])} {rr}/data/logs/tasks/{shlex.quote(S.safe(a['alias']))}__*")
                if r and r[0] == 0 and rc == 0:
                    update_state(lambda st: (st.get("keys") or {}).get(name, {}).pop(k, None))
                    msgs.append(f"{L} 上删了 {k} 的任务和结果")
                else:
                    msgs.append(f"{L} 上删 {k} 出错：{(r or [0, 'ssh 不通'])[1][-150:]} {err[-150:]}")
            elif a["op"] in ("rstop", "rresume"):
                r = rtq(name, ("stop --why 总队列里停了 " if a["op"] == "rstop" else "resume ") + shlex.quote(a["alias"]))
                what = "停" if a["op"] == "rstop" else "撤销停止"
                msgs.append(f"{L} 上{what} {k}" if r and r[0] == 0 else f"{L} 上{what} {k} 出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
        except Exception as e:  # noqa: BLE001
            msgs.append(f"{L} {a['op']} {k} 出错：{e}")
    return msgs


def submit(name, subs):
    L, msgs, stuck = conf(name)["label"], [], {}
    if conf(name)["scheduler"] == "pueue":
        c, r = conf(name), shlex.quote(root(name))
        w, gpu = c["worker"], c["groups"][c["worker"]["group"]][1]
        run = (f"R={r}; REPO=$(readlink -f $R/code/current); export TASKQ_DIR=$R/queue WORKER_ID={w['id']} CUDA_VISIBLE_DEVICES={gpu}; "
               f"exec bash $REPO/scripts/queue/{RB.rename('worker_start.sh')}")
        for _ in subs:
            rc, out, err = rsh(name, f"{c['pueue']} add -g {w['group']} -l worker:{w['id']} -- bash -c {shlex.quote(run)}")
            if rc:
                stuck[f"site:{name}:worker"] = f"{L} 开 worker 出错：{(out + err)[-200:]}"
            else:
                msgs.append(f"{L} 开了 worker（{out.strip()[-80:]}）")
        return msgs, stuck
    for q, t, n in subs:
        ids = []
        for _ in range(n):
            rc, out, err = rsh(name, f"cd {shlex.quote(root(name))}/code/current/scripts/queue && sbatch --parsable --qos={q} --time={t} worker.sbatch")
            if rc:
                stuck[f"topup:{name}:{q}"] = f"{L} {q} 补交 worker 作业出错：{(out + err)[-200:]}"
                break
            ids.append(out.strip().splitlines()[-1])
        if ids:
            msgs.append(f"{L} {q} 交了 {len(ids)} 个 worker 作业 {'、'.join(ids)}（每个 {t}）")
    return msgs, stuck


def write_status(name, rs, st):
    keys = keys_of(st, name)
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}
    folds = {}
    for real, k in keys.items():
        a = rt.get(k["alias"])
        if a:
            f = (rs.get("folds") or {}).get(k["alias"]) or {}
            folds[real] = dict(state=a["state"], worker=a.get("worker"), runs=f.get("runs") or [], final=f.get("final"),
                               debug=f.get("debug") or {})
    try:
        allst = json.load(open(STATUS))
    except (OSError, ValueError):
        allst = {}
    allst[name] = dict(time=time.time(), label=conf(name)["label"], folds=folds, squeue=rs.get("squeue"), free=rs.get("free"),
                       bundle=rs.get("bundle"))
    write_json(STATUS, allst)


def log(lines):
    if lines:
        with open(LOG, "a") as fo:
            fo.writelines(f"{time.strftime('%F %T')} {x}\n" for x in lines)


def run_all(s, dry=False):
    """health.py 调用 → (说明, 卡住的 {问题编号: 说明})。"""
    events, stuck = [], {}
    for name in M.RELAY:
        if name in P.OFFLINE:
            continue
        try:
            ev, st_ = run_one(s, name, dry)
        except Exception as e:  # noqa: BLE001
            ev, st_ = [], {f"relay:{name}": f"scripts/queue/relay.py（{name}）出错：{type(e).__name__}: {e}"[:300]}
        events += ev
        stuck.update(st_)
    return events, stuck


def run_one(s, name, dry):
    live = bool(P.AUTO.get(name))
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    mine = [t for t in (mtq or {}).get("tasks", []) if t.get("worker") == name]
    L, st = conf(name)["label"], load_state()
    if not mine and not live and not dry:
        return [], {}
    if not s["ok"].get(M.hub()) or not mtq:
        return [], {}
    rs = remote_status(name)
    notes = []
    if not dry and (live or mine) and rs is not None:
        try:
            commit = subprocess.run(["git", "-C", REPO, "rev-parse", "origin/main"], capture_output=True, text=True).stdout.strip()
            if rs.get("bundle") != st.get("bundle_for", {}).get(commit):
                bid, path = RB.build(name, commit or "origin/main")
                update_state(lambda x: x.setdefault("bundle_for", {}).update({commit: bid}))
                if rs.get("bundle") != bid:
                    try:
                        deploy(name, bid, path)
                        notes.append(f"{L} 的代码换成 {bid}")
                        rs = remote_status(name)
                    except RuntimeError as e:
                        notes.append(str(e))
        except Exception as e:  # noqa: BLE001
            notes.append(f"{L} 打包出错：{e}")
    if rs is not None and not dry:
        update_state(lambda x: x.setdefault("last_ok", {}).update({name: time.time()}))
        write_status(name, rs, load_state())
    if rs is not None and not dry and live and conf(name).get("budget"):
        try:
            g = relay_gc.gc(name, [k["view"] for k in keys_of(load_state(), name).values() if k.get("view")], rpy)
            if g and (g["removed_views"] or g["removed_blobs"]):
                notes.append(f"{L} 清理：删了 {len(g['removed_views'])} 个数据集视图、{g['removed_blobs']} 个 blob，"
                             f"放出 {g['freed'] / 1e9:.0f} GB，剩 {g['free'] / 1e9:.0f} GB")
                rs = remote_status(name) or rs
        except Exception as e:  # noqa: BLE001
            notes.append(f"{L} 清理出错：{e}")
    acts, stuck = plan(s, name, rs, st)
    more, why = (fill(s, name, rs, st, acts) if rs is not None else ([], "连不上"))
    subs, swhy = (topup(name, rs) if rs is not None else ([], "连不上"))
    if swhy.startswith("上一个 worker"):
        stuck[f"site:{name}:worker"] = f"{L} {swhy}；看那边的 pueue log，处理后 pueue remove 那个任务（下次检查重开）"
    todo = [a for a in acts if a["op"] != "beat"]
    desc = "；".join(f"{a['op']} {a['key']}" for a in todo + more)
    sdesc = "；".join(f"交 {q} {n} 个" for q, _, n in subs)
    if dry:
        return [f"{L}：会 {desc}" if desc else f"{L}：不动（{why}）",
                f"{L}：会 {sdesc}" if sdesc else f"{L}：不交作业（{swhy}）"] + notes, stuck
    msgs = notes + execute(name, acts, s)
    if live:
        msgs += execute(name, more, s)
        m2, st2 = submit(name, subs)
        msgs += m2
        stuck.update(st2)
    elif (more or subs) and C.changed(f"relay_{name}", desc + sdesc):
        log([f"（开关没开）{L} 会：{'；'.join(x for x in (desc, sdesc) if x)}"])
    log(msgs)
    return msgs, stuck


def main(argv):
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")
    p = sub.add_parser("stage")
    p.add_argument("name")
    p.add_argument("key")
    p.add_argument("--nice", type=int)
    p.add_argument("--no-add", action="store_true")
    p = sub.add_parser("fetch")
    p.add_argument("name")
    p.add_argument("key")
    p = sub.add_parser("bundle")
    p.add_argument("name")
    p.add_argument("--commit", default="origin/main")
    p = sub.add_parser("status")
    p.add_argument("name")
    a = ap.parse_args(argv)
    if a.cmd == "stage":
        if a.nice is None and not a.no_add:
            ap.error("要给 --nice（或 --no-add）")
        stage(a.name, a.key, a.nice, add=not a.no_add)
    elif a.cmd == "fetch":
        fetch(a.name, a.key)
    elif a.cmd == "bundle":
        rs = remote_status(a.name)
        print(bundle(a.name, a.commit, (rs or {}).get("bundle")) or "没变")
    elif a.cmd == "status":
        rs = remote_status(a.name)
        if rs is None:
            print("读不到")
            return 1
        for t in rs["taskq"]["tasks"]:
            print(f"  {t['nice']:>5} {t['key']:50} {t['state']} {t['note']}")
        print(f"剩 {rs['free'] / 1e9:.0f} GB；代码 {rs['bundle']}；作业 {rs['squeue']}")
    else:
        import health as H
        ev, st = run_all(H.gather(), dry=True)
        print("\n".join(ev + [f"卡住：{v}" for v in st.values()]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
