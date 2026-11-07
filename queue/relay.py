#!/usr/bin/env python3
"""执行机（machines.py 里有 root 和 scheduler 的机器，pueue 和 Slurm 都是）的接力：由 hub 替它在总队列（hub 上的任务队列）
领任务（GPU 或 CPU，按它有的 worker 种类和环境）、把输入从 store 放到和 store 的数据目录一样的相对路径、放进它自己的任务队列，
做完拉回 store、归档。hub 只统筹：数据在 store 和执行机之间直接传（relay_transfer.py），执行机之间不直接通信。
这里是每次检查的计划和命令行；和那边说话、发过去跑的小程序在 relay_remote.py（那边根目录的布局也在那里），hub 上的记账和那边的清理
在 relay_store.py，传数据、拉回、删那边的任务在 relay_transfer.py。

每次健康检查（monitor/health.py，每 30 分钟）经 queue/feed.py 的 run_all 调用这里的 run_all（送任务只有这一个入口）：
  1. ssh 一次读那边的队列、各任务进度、作业、剩余空间（写到状态目录的 relay/status.json，如 ~/.local/state/taskq/relay/status.json，使用者的状态页面可以读）；
  2. 总队列里 worker 是它的每个任务：
     - 还没放进那边的队列 → hub 的核心组（proto.IO_GROUP）排 relay.py stage（store 补传缺的数据、建软链接、加进那边的队列；要放的路径和
       那边没做完的任务的产出重叠时不传、报出来）；
     - 在那边排着、在训 → 续上（beat）；
     - 那边做完 → hub 的核心组排 relay.py fetch（拉回 store、核对）；
     - 总队列显示训完（store 上有了）→ 删掉那边的任务和结果（执行机的盘只用来中转）；要归档的，备份机能用时 hub 的核心组
       单独排 relay.py archive（relay_archive：store 备份到 machines.py 里角色 backup 的那台），归档成功才交还；备份机停用
       （policy.py 的 PAUSE）时不等，直接交还；只是这次连不上时下次检查再排；
     - 那边失败：没有进度 → 交还（--skip，它以后不再领），删掉那边的任务；有进度 → 领取保留、报出来；
     - 总队列里停了（taskq.py stop）→ 那边也停，那边不再领着以后交还（那边的结果留着）；撤销停止后再领到时那边接着训；
     - 传数据、拉回出错、验证集对不上 → 领取保留、报出来（处理后 pueue remove 那个 hub 任务，下次检查重排）。
  3. 各台都读完以后，总队列里没人领的任务一次分给各执行机（refill.choose：能去的执行机按 PLACE 的偏好，有进度的那台先、
     输入已经在那边的先；要有这种 worker 的空位（备着没人领的不到 READY[名字] 个，档位 < 0 的不占）、环境就绪、
     放得下（输入 + 产出 out_gb 后盘上至少还剩 budget 的第二项）、没跳过过它、插件接受、代码包和冒烟闸门不挡）；
     能去的都跳过了的报 tqblocked；
  4. 那边排着和在跑的 worker 作业不够、又有要卡的任务（备好的或在训的）时排满（refill.py 的 topup_plan，QOS 和个数见 SLURM[名字]["gpu"]）。
policy.py 的 PAUSE 里暂停着的执行机不连、不派任务，到期自动恢复。带 hours 的 worker 只在时段里开（machines.py 的 workers），
所有 worker 都不在时段里时不领新任务。总队列里改分给别的执行机的任务，那边的任务和结果都删掉。
连不上它时只续总队列的领取；领着任务、连不上 2 小时以上报出来。

    python3 queue/relay.py                         看现在会做什么（不改任何东西；taskq plan 连同训完以后一起看）
    taskq internal stage <名字> <key> --nice N [--no-add]   传数据、加进那边的队列（--no-add：只传数据、撤掉那边"缺输入"的标记）
    taskq internal fetch <名字> <key>              拉回做完的任务、核对，放进 store 的数据目录
    taskq internal archive <key>                   store 上这个任务的结果备份到备份机、核对
    taskq site bundle <名字> [--commit C]          打包、推过去、切换 current
    taskq site status <名字>                       那边的队列
    taskq site resend <名字>                       传过去的任务按现在的做法重新解析，换掉那边队列里的说明
"""
import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.dirname(HERE)  # 调度代码的根
sys.path.insert(0, HERE)
import cfg  # noqa: E402
REPO = cfg.REPO  # 使用者的仓库根
cfg.use_config()
import proto  # noqa: E402
from proto import ROOT, alert, alert_id, alert_msg  # noqa: E402
sys.path.insert(0, os.path.join(CORE, "sync"))
sys.path.insert(0, os.path.join(CORE, "monitor"))
sys.path.insert(0, os.path.join(CORE, "sites"))
import mtab as M  # noqa: E402
import policy as P  # noqa: E402
import spec as S  # noqa: E402
import common as C  # noqa: E402
import taskq  # noqa: E402
import refill  # noqa: E402
import remote as R  # noqa: E402
import pueue as PQ  # noqa: E402
import slurm as SL  # noqa: E402
import relay_plugins as RN  # noqa: E402
import relay_bundle as RB  # noqa: E402
import relay_remote as RM  # noqa: E402
import relay_store as ST  # noqa: E402
import relay_transfer as RT  # noqa: E402
import bundle_gate as BG  # noqa: E402
import smoke_gate as SG  # noqa: E402
import names as N  # noqa: E402
import envs as EV  # noqa: E402
from common import write_json  # noqa: E402

STATUS = os.path.join(ST.DIR, "status.json")
DOWN_S = 2 * 3600  # 领着任务、连不上这么久就报
LATE_S = 3600  # 拉回、归档成功这么久了 hub 上还不算训完，就报
LIVE = refill.LIVE
VIEW_KEEP_S = 6 * 3600  # 建好这么久以内的视图不清理（任务刚传完、还没加进队列或刚交还）
STATUS_S = 90  # 读状态的超时：那边的存储挂住时跳过这台，不等
NOSLOT_RC = proto.EXIT["stage_later"]  # relay.py stage 这次排不上 CPU 作业（或存档的来源那边还领着）的退出码：下次检查再传，不报
NOSLOT_ALERT_S = 3 * 3600  # 领了这么久还一直排不上就报


def relayable(i):
    """refill.info 的结果 → 能不能领给它：做法能翻译（按本机的 recipes.py 算，总队列那边的代码可能还没更新），插件接受这个任务
    （sites/names.py 的 accepts，如插件不接受要读仓库里文件的任务）。"""
    sp = i["spec"]
    rec, params = i["recipe"] or sp.get("recipe"), i["params"] or sp.get("params") or {}
    try:
        relay = N.plugin_of(S.resolve(rec, params))
    except S.SpecError:
        relay = N.plugin_of(sp)
    return N.accepts(relay, params)


def gate(name, rs, mtq, key):
    """这台在用的代码包跑不了 key 的说明 → (包名, 原因)；跑得了、不知道 → None（queue/bundle_gate.py；送的是 stage 时再核对一次）。"""
    ids = BG.in_use(name, rs.get("bundle"), rs.get("live"))
    return BG.check_all(S.sendable(refill.info(mtq, key)["spec"]), ids) if ids else None


# ---------------- 那边的状态 ----------------
def remote_status(name):
    """→ 那边的状态 dict；连不上或出错 → None。"""
    c = RM.conf(name)
    if RM.master_down(c):
        return None
    try:
        rs = RM.op(name, "status", dict(sched=c["scheduler"], pueue=c.get("pueue"), cap=c.get("cap"), blob_cap=c.get("blob_cap"), env_root=M.env_root(name),
                                             envs=sorted(EV.wanted(name, envs_switch(name)[1]))), timeout=STATUS_S)
    except RuntimeError:
        return None
    return parse_sched(name, rs)


def current_bundle(name):
    """读不到状态时看那边的 code/current → 包名；还没有代码包（新机器第一次）→ ""；连不上 → None。"""
    c = RM.conf(name)
    if RM.master_down(c):
        return None
    cur = f"{R.q(RM.root(name))}/{ROOT['current']}"
    rc, out, _ = RM.rsh(name, f'if [ -e {cur} ]; then basename "$(readlink -f {cur})"; fi', timeout=30)
    return out.strip() if rc == 0 else None


def parse_sched(name, rs):
    """状态脚本读回的调度器原始输出 → rs["jobs"]（pueue：各 worker 所在组的任务）或 rs["squeue"]（Slurm）；读不出 → None。
    一次 ssh 读完、在 hub 上用适配器解析（sites/pueue.py、slurm.py）：每台每次只连一次，状态是同一时刻的。"""
    raw, c = rs.pop("sched_raw", None), RM.conf(name)
    rs["jobs"] = rs["squeue"] = None
    if raw is not None and c["scheduler"] == "pueue":
        rs["jobs"] = PQ.parse(raw, [w["group"] for w in M.workers(name)])
    elif raw is not None:
        rs["squeue"] = SL.squeue(raw)
    return rs


# ---------------- 代码包 ----------------
def claim_times(rs):
    """那边的状态 → 还领着的任务各自领取的时间（[] = 没有）；状态里没有队列（读不到）→ None。"""
    tasks = ((rs or {}).get("taskq") or {}).get("tasks")
    return None if tasks is None else [t["since"] for t in tasks if t.get("since")]


def deploy(name, bid, path, claims=None):
    """把包推过去、切换 current（原子的 mv -T）；第一次时建目录、开队列。
    整份收下、sha256 对上才解开（ssh 断在两个文件之间时 tar 不报错）；同一台同时只推一个（flock），临时目录各用各的。
    清理旧包（claims：那边还领着的任务的领取时间，claim_times）：留下 current、最近的 proto.CODE_KEEP 个、CODE_KEEP_S 内碰过的
    （在跑的 worker 每圈碰自己的包），和最早的领取前 CODE_CLAIM_SLACK 起碰过的（worker 换包后，它交给新包接着管的任务还从领取时
    的那个包跑：那个包最后被碰是在领取之后）。claims 是 None（不知道那边有什么在跑）时不删旧包。
    排着的 Slurm 作业不占着旧包：交的是 code/current 下的路径，作业开始时才解析（sites/slurm.py）。
    被打断的部署留下的临时文件 1 天后删。"""
    r = R.q(RM.root(name))
    b = shlex.quote(bid)
    tar = subprocess.run(["tar", "-C", path, "-cf", "-", "."], capture_output=True, check=True).stdout
    sha = hashlib.sha256(tar).hexdigest()
    cur = os.path.relpath(ROOT["current"], ROOT["code"])  # code/ 下的名字
    tq, en = f"$R/{ROOT['queue']}/{proto.QUEUE['tasks']}", f"$R/{ROOT['queue']}/{proto.QUEUE['enabled']}"
    cmd = (f"set -e; R={r}; mkdir -p {' '.join('$R/' + d for d in proto.MKDIRS)}; "
           f"[ -e {tq} ] || {{ : > {tq}; touch {en}; }}; cd $R/{ROOT['code']}; "
           f"exec 9> $R/{ROOT['deploy_lock']}; flock -w 300 9; "
           "find . -maxdepth 1 -name '*.tmp.*' -mtime +0 -exec rm -rf {} + || true; "  # 以前被打断的部署留下的（锁里没有别的在推）
           f"t={b}.tmp.$$; rm -rf $t $t.tar; cat > $t.tar; "
           f"if [ \"$(sha256sum < $t.tar | cut -d' ' -f1)\" != {sha} ]; then rm -f $t.tar; echo '代码包没传完整' >&2; exit 3; fi; "
           f"if [ ! -d {b} ]; then mkdir $t; tar -xf $t.tar -C $t; mv -T $t {b}; fi; rm -f $t.tar; "
           f"touch {b}; ln -sfn {b} {cur}.tmp.$$; mv -T {cur}.tmp.$$ {cur}")
    if claims is not None:
        floor = int(min([time.time() - proto.CODE_KEEP_S] + [c - proto.CODE_CLAIM_SLACK for c in claims]))
        cmd += (f"; ls -1t | grep -vx {cur} | grep -v '\\.tmp' | tail -n +{proto.CODE_KEEP + 1} | "
                f"while read -r d; do if [ \"$d\" != {b} ] && [ \"$(stat -c %Y \"$d\")\" -lt {floor} ]; then rm -rf \"$d\"; fi; done")
    r2 = subprocess.run(RM.site_argv(name, cmd, 600), input=tar, capture_output=True, timeout=600)
    if r2.returncode:
        raise RuntimeError(f"推代码包 {bid} 到 {RM.conf(name)['label']} 出错：{RM.err_tail(r2.stderr.decode(errors='replace'))}")


BUNDLE_REF = "origin/main"  # 打代码包用的提交（测试里钉住，别处推送 main 时不跟着换）


def wanted_bundle():
    """各执行机该用的代码包（所有执行机同一个）→ (包名, hub 上的路径)：使用者仓库 BUNDLE_REF、核心仓库 RB.CORE_REF 的提交建的；
    两边的提交都没变、包还在时用记下的（state.json 的 bundle_for），不重新打。"""
    commit = subprocess.run(["git", "-C", REPO, "rev-parse", BUNDLE_REF], capture_output=True, text=True).stdout.strip()
    core = subprocess.run(["git", "-C", cfg.CORE, "rev-parse", RB.CORE_REF], capture_output=True, text=True).stdout.strip()
    key = f"{commit}:{core}"
    bid = (ST.load_state().get("bundle_for") or {}).get(key)
    path = os.path.join(os.path.expanduser(RB.OUT), bid) if bid else None
    if not path or not os.path.isdir(path):
        bid, path = RB.build(commit or BUNDLE_REF, core=core or RB.CORE_REF)
        ST.update_state(lambda x: x.update(bundle_for={key: bid}))
    return bid, path


def bundle(name, have=None, claims=None):
    """该用的包和那边的 current 不同就推过去（claims 见 deploy）→ 说明（没变 → None）。"""
    bid, path = wanted_bundle()
    if bid == have:
        return None
    deploy(name, bid, path, claims)
    return f"{RM.conf(name)['label']} 的代码换成 {bid}"


def code_only():
    """有根目录、不是执行机的机器（store；锁了环境时的 hub）：它们也跑 site_ops 的操作，代码包和执行机同一个。"""
    return [n for n in M.MACHINES if M.MACHINES[n].get("root") and not M.is_exec(n)]


def update_code(stuck):
    """code_only 的机器的 current 和该用的包不同就推过去 → 说明。"""
    out = []
    for name in code_only():
        if M.paused(name):
            continue
        try:
            cur = current_bundle(name)
            if cur is None:
                stuck[alert_id("code", name)] = f"{RM.conf(name)['label']} 连不上，读不到它的代码包"
                continue
            msg = bundle(name, cur, claims=[])  # 不是执行机：没有 worker，没有领着的任务
            out += [msg] if msg else []
        except Exception as e:  # noqa: BLE001
            stuck[alert_id("code", name)] = f"{RM.conf(name)['label']} 打包或推代码包出错：{e}"[:300]
    return out


def prune_bundles(bid):
    """hub 上的包只留该用的、各执行机在用的（status.json 的 bundle、live）、送任务闸门挡着的（held）和最近的几个（relay_bundle.prune）。"""
    try:
        allst = json.load(open(STATUS))
    except (OSError, ValueError):
        return []
    keep = {bid}
    for d in allst.values():
        keep |= {d.get("bundle")} | set(d.get("live") or ()) | {h.get("bundle") for h in (d.get("held") or {}).values()}
    return RB.prune(keep - {None})


# ---------------- 每次检查 ----------------
def io_task(s, step, key, since=0):
    """hub 的核心组（proto.IO_GROUP）里 step 这一步、这个 key 的最新任务（任务记录在 since 之后加的）→ pueue 任务或 None。"""
    hub, last = M.hub(), None
    for t in sorted((s["pueue"].get(hub) or {}).get("tasks", []), key=lambda t: t["id"]):
        r = C.identify(hub, t, s.get("records") or [])
        if r and r["step"] == step and r["key"] == key and r.get("added", 0) >= since - 60:
            out.append(t)
    return out


def box_task(s, step, key, since=0):
    """box_tasks 里最新的一个，或 None。"""
    return (box_tasks(s, step, key, since) or [None])[-1]


def again(s, step, key, since=0):
    """最新的这一步出错了（不是手动停的），而且出错的还不到 XFER_TRIES 次 → True：再排一次，不报。"""
    def failed(t):
        return (t["status"] not in LIVE and t["status"] != "Success" and t.get("code") != NOSLOT_RC
                and not (t.get("result") == "Killed" or t.get("code") == 143))
    ts = box_tasks(s, step, key, since)
    return bool(ts) and failed(ts[-1]) and sum(map(failed, ts)) < XFER_TRIES


def smoke_step(name, mtq, t, a, k, now):
    """冒烟任务（proto.SMOKE_PREFIX）：那边做完 / 失败了，或者已经记了结果 → 记结果、删那边的、从总队列里去掉（不拉回、不归档）；
    别的情况 → None（照常传、续约）。"""
    key = t["key"]
    over = SG.finished(name, key)
    res = None
    if a is not None:
        held = a.get("worker") and (now - (a.get("beat") or 0) < proto.STALE_BEAT
                                    or (a.get("task_log_age") is not None and a["task_log_age"] < proto.FRESH_LOG))
        if a["state"] == "done" and not held:
            res = (True, "")
        elif a["state"] in (proto.S_FAILED, proto.S_BAD):
            res = (False, f"{a['state']}：{a.get('note') or ''}")
    if res is None and t["state"] == "done":
        res = (True, "")
    if res is None and not over:
        return None
    ops = [dict(op="beat", key=key)]
    if res is not None and not over:
        ops.append(dict(op="smoke_rec", key=key, rkey=key, ok=res[0], why=res[1]))
    if k:
        sp = refill.info(mtq, key)["spec"]
        ops.append(dict(op="drop", key=key, rkey=key, cleanup=SG.smoke_dirs(sp, SG.sid_of(key)) if sp else []))
    ops.append(dict(op="smoke_end", key=key))
    return ops


def plan(s, name, rs, st, now=None):
    """→ (动作, 卡住的 {问题编号: 说明})。rs：那边的状态（None = 连不上）；st：load_state()。
    动作 dict(op=beat|release|stage|restage|fetch|drop|rstop|rresume|rnice|archive|hold|unhold, key, …)；rkey：那边队列里的 key。"""
    now = now or time.time()
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    L, keys = RM.conf(name)["label"], ST.keys_of(st, name)
    mine = [t for t in (mtq or {}).get("tasks", []) if t.get("worker") == name]
    acts, stuck = [], {}
    if not mtq or not mtq.get("enabled"):
        return [], {}
    if rs is not None and "error" in (s.get("pueue", {}).get(M.hub()) or {"error": 1}):
        rs = None  # hub 的 pueue 读不到：在跑的拉回、归档看不见，不能当成没有，只续约
    if rs is None:
        acts = [dict(op="beat", key=t["key"]) for t in mine if t["state"] != "done"]
        last = (st.get("last_ok") or {}).get(name, 0)
        if acts and now - last > DOWN_S:
            stuck[alert_id("site", name, "down")] = alert(
                f"{L} 读不到状态{f' {(now - last) / 3600:.0f} 小时' if last else ''}（ssh 不通，或那边的存储挂住），"
                f"领着 {len(acts)} 个任务，那边的进度看不到", hours=round((now - last) / 3600, 1) if last else None, tasks=len(acts))
        return acts, stuck
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}
    tasks = rs.get("tasks") or {}
    owned, holds, gated = set(), BG.held(st, name), set()
    for t in mine:
        key = t["key"]
        k = keys.get(key)
        a = rt.get(key) if k else None
        if a:
            owned.add(a["key"])
        if proto.is_smoke(key):
            sm = smoke_step(name, mtq, t, a, k, now)
            if sm is not None:
                acts += sm
                continue
        if t["state"] == "done":
            ft = io_task(s, "relay_fetch", key, t.get("since") or 0)
            at = io_task(s, "relay_archive", key, t.get("since") or 0)
            if any(x is not None and x["status"] in LIVE for x in (ft, at)):  # store 上有了，但拉回、归档还没做完：等
                acts.append(dict(op="beat", key=key))
                continue
            # 总队列显示训完：store 上有了（结果本身，不看拉回记录）。拉回出错时报
            if ft is not None and ft["status"] != "Success":
                acts.append(dict(op="beat", key=key))
                stuck[alert_id("relayfetch", key)] = alert(f"从 {L} 拉回 {key} 的 {M.hub()} 任务 {ft['id']} 出错了（{ft['status']}），领取保留；"
                                                           f"看 pueue log {ft['id']}，处理后 pueue remove {ft['id']}（下次检查重排）",
                                                           machine=name, step="fetch", task=ft["id"], status=ft["status"])
                continue
            if k:  # store 上有了就删那边的任务和结果，不等归档（执行机的盘只用来中转）
                acts.append(dict(op="drop", key=key, rkey=key))
            try:
                want = ft is not None and bool(RT.lookup(key).get("archive"))
            except (RN.RelayError, S.SpecError):
                want = False
            archived = at is not None and at["status"] == "Success"
            if want and not archived:
                if at is not None:
                    acts.append(dict(op="beat", key=key))
                    stuck[alert_id("relayfetch", key)] = alert(f"{key} 归档到备份机的 {M.hub()} 任务 {at['id']} 出错了（{at['status']}），{M.hub()} 上有了、领取保留；"
                                                               f"看 pueue log {at['id']}，处理后 pueue remove {at['id']}（下次检查重排）",
                                                               machine=name, step="archive", task=at["id"], status=at["status"])
                    continue
                if not C.backup_down():  # 备份机能用：hub 单独排归档，归档成功才交还；只是这次连不上时下次再排
                    acts.append(dict(op="beat", key=key))
                    if not C.backup_down(s.get("ok")):
                        acts.append(dict(op="archive", key=key))
                    continue
            acts.append(dict(op="release", key=key, why=f"训完了，{M.hub()} 上有了" + ("、归档了" if archived else "（没有归档）")))
            sp = refill.info(mtq, key)["spec"]
            try:
                if sp and sp.get("recipe") and SG.on():
                    acts.append(dict(op="smoke_real", key=key, mark=SG.mark(name, sp)))
            except ValueError:
                pass
            continue
        acts.append(dict(op="beat", key=key))
        if t["state"] == "stopped" and not (a and a["state"] == "done"):
            if a and a["state"] != "stopped":
                acts.append(dict(op="rstop", key=key, rkey=a["key"]))
            elif not (a and a.get("worker")):  # 那边不再领着（或者还没放过去）
                acts[-1] = dict(op="release", key=key, why="停止（taskq.py stop）" + ("，那边的结果留着" if a else ""))
            continue
        if a and a["state"] not in ("done", "failed", "bad") and a.get("nice") is not None and a["nice"] != t["nice"]:
            acts.append(dict(op="rnice", key=key, rkey=a["key"], nice=t["nice"]))  # 总队列里改了档位（set-nice）：那边跟着改
        since = t.get("since") or 0
        if a is None:
            sg = io_task(s, "relay_stage", key, since)
            noslot = sg is not None and sg["status"] not in LIVE and sg.get("code") == NOSLOT_RC
            if sg is None or sg["status"] == "Success" or noslot or again(s, "relay_stage", key, since):
                g, h = gate(name, rs, mtq, key), holds.get(key) or {}
                if not g and h.get("bundle") in BG.in_use(name, rs.get("bundle"), rs.get("live")):
                    g = (h["bundle"], h.get("err"))  # stage 按要送的说明挡下的：同一个包还在用就不再排
                if g:  # 那边在用的代码包跑不了这个说明：先不送，领取保留，那边换了包再核对
                    gated.add(key)
                    if h.get("bundle") != g[0]:
                        acts.append(dict(op="hold", key=key, bundle=g[0], err=g[1]))
                    stuck[alert_id("bundlegate", name, key)] = alert(
                        f"{L} 在用的代码包 {g[0]} 跑不了 {key} 的说明（{g[1]}），先不送、领取保留；那边换了包以后再送",
                        machine=name, key=key, bundle=g[0], err=g[1])
                    continue
                acts.append(dict(op="stage", key=key, nice=t["nice"]))
                if noslot and now - since > NOSLOT_ALERT_S:
                    stuck[alert_id("site", name, "stage")] = alert(
                        f"{L} 的 {key} {(now - since) / 3600:.0f} 小时都没传过去（CPU 作业排不上，或存档的来源那边还领着）"
                        f"（{M.hub()} 任务 {sg['id']}，每次检查再试）", key=key, hours=round((now - since) / 3600, 1), task=sg["id"])
            elif sg["status"] not in LIVE:
                stuck[alert_id("relaystage", key)] = alert(f"给 {L} 传 {key} 的 {M.hub()} 任务 {sg['id']} 出错了（{sg['status']}），领取保留；"
                                                           f"看 pueue log {sg['id']}，处理后 pueue remove {sg['id']}（下次检查重传）",
                                                           machine=name, step="stage", task=sg["id"], status=sg["status"])
            continue
        ast = a["state"]
        held = a.get("worker") and (now - (a.get("beat") or 0) < proto.STALE_BEAT
                                    or (a.get("task_log_age") is not None and a["task_log_age"] < proto.FRESH_LOG))
        if ast == "done" and held:
            pass  # 做完的标志有了、worker 还领着：训练命令还在写结果，只续约，交还以后再拉回；领取过期了（worker 没交还就没了）照常拉回
        elif ast == "done":
            ft = io_task(s, "relay_fetch", key, since)
            if ft is None:
                acts.append(dict(op="fetch", key=key))
            elif ft["status"] == "Success":
                end = C.local_time(ft.get("end"))
                if end is not None and now - end.timestamp() > LATE_S:
                    stuck[alert_id("relayfetch", key)] = alert(f"{L} 训完的 {key} 拉回并归档成功了（{M.hub()} 任务 {ft['id']}），{M.hub()} 上却还不算训完；"
                                                               f"看 {M.hub()} 上的结果文件夹", machine=name, step="late", task=ft["id"], status=ft["status"])
            elif ft["status"] not in LIVE:
                stuck[alert_id("relayfetch", key)] = alert(f"从 {L} 拉回 {key} 的 {M.hub()} 任务 {ft['id']} 出错了（{ft['status']}），领取保留；"
                                                           f"看 pueue log {ft['id']}，处理后 pueue remove {ft['id']}（下次检查重排）",
                                                           machine=name, step="fetch", task=ft["id"], status=ft["status"])
        elif ast == "stopped":
            acts.append(dict(op="rresume", key=key, rkey=a["key"]))
        elif ast == "input":  # 那边开训前查到输入不全（taskq.py missing）：只补传缺的、重建视图，再撤掉标记
            sg = io_task(s, "relay_stage", key, a.get("input_since") or since)
            if sg is None or sg["status"] == "Success" or (sg["status"] not in LIVE and sg.get("code") == NOSLOT_RC):
                acts.append(dict(op="restage", key=key))
            elif sg["status"] not in LIVE:
                stuck[alert_id("relaystage", key)] = alert(f"给 {L} 补传 {key} 缺的输入的 {M.hub()} 任务 {sg['id']} 出错了（{sg['status']}），领取保留；"
                                                           f"看 pueue log {sg['id']}，处理后 pueue remove {sg['id']}（下次检查重传）",
                                                           machine=name, step="restage", task=sg["id"], status=sg["status"])
        elif ast == "failed":
            if refill.site_progress(tasks.get(a["key"])):
                stuck[alert_id("site", name, "fail", key)] = alert(
                    f"{L} 上 {key}（{a['key']}）训练失败 {a.get('failures')} 次，有进度，领取保留；"
                    f"看那边的 {proto.TASK_LOGS}/；放弃：taskq.py release --worker {name} {key}",
                    state="failed", progress=True, failures=a.get("failures"))
            else:
                acts[-1] = dict(op="release", key=key, skip=f"{L} 上训练失败", why=f"{L} 上训练失败、没有进度，交还，{L} 不再领它")
                acts.append(dict(op="drop", key=key, rkey=a["key"]))
        elif ast == proto.S_BAD:
            stuck[alert_id("site", name, "fail", key)] = alert(f"{L} 队列里的 {a['key']}（{key}）{ast}：{a.get('note')}",
                                                               state=ast, progress=False, failures=a.get("failures"))
    mt = {t["key"]: t for t in (mtq or {}).get("tasks", [])}
    mk = {t["key"] for t in mine}
    for a in rs["taskq"]["tasks"]:
        if a["key"] in owned:
            continue
        real = a["key"] if a["key"] in keys else None
        m = mt.get(real) if real else None
        if real is None:
            stuck[alert_id("site", name, "orphan", a["key"])] = alert(f"{L} 队列里的 {a['key']} 认不出是哪个任务（{ST.DIR}/state.json 里没有）",
                                                                      unknown=True, state=a["state"])
        elif a["state"] in ("claimed", "busy"):
            stuck[alert_id("site", name, "orphan", real)] = alert(f"{L} 在训 {real}，但总队列里不是它领着；看总队列和 {L} 的队列",
                                                                  unknown=False, state=a["state"], assigned=(m or {}).get("worker"))
        elif m is None or m["state"] == "done" or m.get("worker") and m["worker"] != name:  # 训完了、改分给了别的执行机：删掉
            acts.append(dict(op="drop", key=real, rkey=a["key"]))
        elif a["state"] != "stopped":
            acts.append(dict(op="rstop", key=real, rkey=a["key"]))
        elif m["state"] != proto.S_STOPPED and not m.get("worker") and not refill.site_progress(tasks.get(a["key"])):
            acts.append(dict(op="drop", key=real, rkey=a["key"]))  # 总队列里恢复了、没分给谁，那边停着的没进度 → 删
    for real, k in keys.items():  # 那边的任务删了、路径没删掉的（drop_remote 记了 dropped）：再删
        if real not in mk and real not in rt and k.get("dropped"):
            acts.append(dict(op="drop", key=real, rkey=real, gone=True))
    acts += [dict(op="unhold", key=k) for k in holds if k not in gated]
    if rs["taskq"].get("paused"):
        stuck[alert_id("site", name, "fail")] = alert(f"{L} 的任务队列 3 小时内失败 {rs['taskq'].get('recent_failures')} 次，暂停领任务",
                                                      paused=True, failures=rs["taskq"].get("recent_failures"))
    return acts, stuck


def site_for(name, rs, st, mtq):
    """这台这一轮的余量 → (refill.Site, 冒烟闸门) 或 (None, 不派的原因)。
    备着的（领着、还没开训：在传数据，在那边排着，或那边停着、这次要撤销停止的）每种 worker 各算 READY 个；
    那边每个任务（排着、在训、训完没拉回的，加领着还没传过去的）的产出各留 out_gb，加回它们已经写出的（out_used）。"""
    if not any(M.working(name, w) for w in M.workers(name)):
        return None, "所有 worker 都不在时段里（machines.py 的 hours）"
    c, kinds, keys = RM.conf(name), M.kinds(name), ST.keys_of(st, name)
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}
    spec = {t["key"]: t.get("spec") or {} for t in rs["taskq"]["tasks"]}
    spec.update({t["key"]: t.get("spec") or {} for t in mtq.get("tasks", [])})

    def remote(t):
        return rt.get(t["key"]) if t["key"] in keys else None

    def waiting(t):
        a = remote(t)
        return a is None or a["state"] in (proto.S_FREE, proto.S_STALE, proto.S_STOPPED, proto.S_INPUT)

    def idle(t):  # 那边备好了、没人领、没有进度：可以让给更优先的任务（有进度的不让）
        a = remote(t)
        return a is not None and a["state"] == proto.S_FREE and not a.get("worker") and not refill.site_progress((rs.get("tasks") or {}).get(t["key"]))
    have, mine = {k: 0 for k in kinds}, []
    for t in mtq.get("tasks", []):
        if t.get("worker") == name and t["state"] not in ("done", "stopped") and waiting(t):
            k = refill.kind_of(spec[t["key"]])
            have[k] = have.get(k, 0) + 1
            mine.append(t)
    on = set(rt) | {t["key"] for t in mine if remote(t) is None}
    root_gb = (rs.get("free", 0) + (rs.get("out_used") or 0)) / 1e9 - sum(refill.out_gb(spec.get(k) or {}) for k in on)
    dev, bdev = rs.get("dev"), rs.get("blob_dev", rs.get("dev"))
    rootk = (name, "cap") if c.get("cap") else (c["ssh"], dev)  # 有 cap 的是这台自己的额度；否则同一个盘上的几台合起来扣
    blobk = rootk if bdev == dev else (name, "blob_cap") if c.get("blob_cap") else (c["ssh"], bdev)
    idle_by = {k: [dict(key=t["key"], nice=t["nice"], out=refill.out_gb(spec[t["key"]]))
                   for t in sorted((t for t in mine if refill.kind_of(spec[t["key"]]) == k and idle(t)), key=lambda t: (-t["nice"], -t["order"]))]
               for k in kinds}
    sg = SG.Gate(name, rs, mtq)

    def block(key, sp):
        g = gate(name, rs, mtq, key)
        return f"在用的代码包 {g[0]} 跑不了这个说明（{g[1]}）" if g else sg.verdict(key, sp)
    site = refill.Site(name, {k: R.ready(name) - have[k] for k in kinds}, {p for k in keys.values() for p in k.get("inputs") or []},
                       M.ready_envs(name, rs.get("envs_ok")), (rootk, root_gb), (blobk, rs.get("blob_free", rs.get("free", 0)) / 1e9),
                       c["budget"][1], block=block, took=sg.took, idle=idle_by)
    return site, sg


def held_all(ctxs):
    """各执行机上做了一半的任务 → {key: [执行机]}（refill.holders）：这一轮读到的用读到的，别的用上次检查写的 status.json；
    暂停着（policy.py 的 PAUSE）、不是执行机的不算（它的任务放开给别的，从头做）。"""
    try:
        allst = json.load(open(STATUS))
    except (OSError, ValueError):
        allst = {}
    views = {n: (d or {}).get("tasks") or {} for n, d in allst.items()}
    views.update({n: site_view(n, c["rs"], c["st"]) for n, c in ctxs.items() if c.get("rs") is not None})
    return refill.holders(views, lambda n: M.is_exec(n) and not M.paused(n))


def assign(s, ctxs, stuck=None, notes=None):
    """总队列里能派的任务一次分给各执行机（refill.choose）。ctxs {执行机: dict(rs=那边的状态或 None, st=load_state())}。
    → ({执行机: [动作]}（take，备满时让位的 drop + release，冒烟闸门的 smoke_add 等）, {执行机: 没领的原因})。
    能去的执行机都跳过了的任务报 tqblocked；冒烟闸门的报警记进 stuck，开关关着时的影子记进 notes。"""
    stuck = {} if stuck is None else stuck
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    if not mtq or not mtq.get("enabled") or mtq.get("paused"):
        return {}, {n: "总队列关着或暂停中" for n in ctxs}
    if "error" in (s.get("pueue", {}).get(M.hub()) or {"error": 1}):  # 传数据要在 hub 的 pueue 排，在跑的看不见
        return {}, {n: f"{M.hub()} 的 pueue 读不到" for n in ctxs}
    tasks = [t for t in mtq.get("tasks", []) if t["state"] == proto.S_FREE and not t.get("bad") and t.get("spec")
             and relayable(refill.info(mtq, t["key"])) and not refill.progress(s, t["key"])]
    specs = {t["key"]: (t.get("recipe"), t["spec"]) for t in tasks}
    sites, gates, why = {}, {}, {}
    for n, c in ctxs.items():
        if c.get("rs") is None:
            why[n] = "连不上"
            continue
        site, sg = site_for(n, c["rs"], c["st"], mtq)
        if site is None:
            why[n] = sg
            continue
        sites[n], gates[n] = site, sg
        for t in tasks:  # 冒烟闸门：这台能去、种类和环境对得上的，没过冒烟的排冒烟、报警（不管这轮有没有空位）
            rec, sp = specs[t["key"]]
            if n in refill.places(rec, sp) and n not in (t.get("skipped_by") or []) and refill.kind_of(sp) in site.room \
                    and set(S.envs_of(sp)) <= site.envs:
                sg.verdict(t["key"], sp)
    need = {p for t in tasks for p in specs[t["key"]][1].get("inputs") or [] if any(p not in x.ready for x in sites.values())}
    try:
        sizes = refill.data_sizes(need, lambda pats: RM.store_op("probe", dict(pats=pats, size=True)))
    except RuntimeError as e:
        stuck[alert_id("relay", "store")] = f"store 上量不了要传的输入（{e}），这一轮不派任务"
        return {}, {n: "store 上量不了输入" for n in ctxs}
    res = refill.choose(tasks, sites, specs, held_all(ctxs), sizes)
    for k in res["nowhere"]:
        stuck[alert_id("tqblocked", k)] = alert(f"{k}：能去的执行机（{'、'.join(refill.places(*specs[k]))}）都交还过它、跳过了，不会自己开始；"
                                                f"看各台交还的原因，处理好后 taskq unskip {k}", key=k)
    for k in res["noroom"]:
        stuck[alert_id("tqroom", k)] = alert(f"{k}：能去的执行机上都放不下（{res['why'][k]}）；腾出地方，或改小这一步的 out_gb", after=60, key=k)
    out = {}
    for n, site in sites.items():
        ops = []
        for w, t in site.swaps:
            ops += [dict(op="drop", key=w["key"], rkey=w["key"]),
                    dict(op="release", key=w["key"], why=f"让给更优先的 {t['key']}（档位 {t['nice']} < {w['nice']}）")]
        ops += [dict(op="take", key=t["key"], nice=t["nice"]) for t in site.take]
        sg = gates[n]
        stuck.update(sg.alerts)
        if notes is not None:
            notes.setdefault(n, []).extend(sg.notes)
        out[n] = ops + sg.ops()
        if not out[n]:
            mine = sorted({p[len(n) + 1:] for r in res["why"].values() for p in r.split("；") if p.startswith(f"{n}：")})
            why[n] = "；".join(mine[:3]) or "队列里没有能派给它的任务"
    return out, why


def kname(kind):
    return "" if kind == "gpu" else f"{kind.upper()} "


def wname(kind):
    return f"{kname(kind)}worker"


def kkey(kind, *args, wkind="gpu"):
    """问题编号：GPU 的照旧，别的种类后面加 :<种类>。"""
    return alert_id(kind, *args, None if wkind == "gpu" else wkind)


def sub_kind(x):
    """topup 返回的一项 → 种类。"""
    return x[1]["kind"] if x[0] == "pueue" else x[3]


def topup(name, rs, run=None):
    """→ 要开的 worker, 不补的原因。"要几个"只在这里算，适配器只读状态、照着开：
    pueue 机器：machines.py 的每个 worker 在时段里、没有活着的、上一个没失败、那边有备好没人领的这种任务时开 → ("pueue", worker)；
    Slurm 机器：policy.py 的 SLURM 里这台能跑的种类，按 refill.slurm_plan 交 → (QOS, 时限, 交几个, sbatch 文件)。
    run：fn(名字, 命令) → (退出码, 输出, 错误)，测试替换。"""
    if RM.conf(name)["scheduler"] == "pueue":
        if rs.get("jobs") is None:
            return [], "读不到 pueue"
        subs, why = [], []
        for w in M.workers(name):
            mine = PQ.workers(w, rs["jobs"])
            if any(j[2] != "Done" for j in mine):
                why.append(f"{w['id']} 在")
            elif mine and mine[-1][3] not in ("Success", "Killed"):
                why.append(f"上一个 {w['id']}（pueue 任务 {mine[-1][0]}）失败了（{mine[-1][3]}）")
            elif not M.working(name, w):
                why.append(f"{w['id']} 不在时段里")
            elif not refill.unclaimed(rs["taskq"], w["kind"]):
                why.append(f"那边没有备好、没人领的{' ' + kname(w['kind']) if kname(w['kind']) else ''}任务给 {w['id']}")  # worker 空闲 30 分钟自己退出，有了再开
            else:
                subs.append(("pueue", w))
        return subs, "；".join(why)
    conf_ = {k: v for k, v in (P.SLURM.get(name) or {}).items() if k in M.kinds(name)}
    if not conf_:
        return [], "policy.py 的 SLURM 里没有它"
    st = SL.status(name, lambda c: (lambda r: (r[0], r[1] + r[2]))((run or RM.rsh)(name, c)))
    if st is None:
        return [], "读不到作业数"
    return refill.slurm_plan(rs["taskq"], st, conf_)


def label(op, key):
    return f"{op}:{key}"


def smoke_tail(name, rkey, n=20):
    """冒烟任务失败的原因：那边失败记录的最后几行、最新一段日志的最后 n 行；读不到 → ""。"""
    r, sk = R.q(RM.root(name)), shlex.quote(proto.safe(rkey))
    cmd = (f"tail -n 3 {r}/{ROOT['queue']}/{proto.QUEUE['failures']}/{sk} 2>/dev/null; "
           f"f=$(ls -t {r}/{ROOT['data']}/{proto.TASK_LOGS}/{sk}{proto.SAFE}* 2>/dev/null | head -1); "
           f'[ -n "$f" ] && tail -n {n} "$f"')
    try:
        rc, out, _ = RM.rsh(name, cmd, timeout=60)
    except Exception:  # noqa: BLE001
        return ""
    return out.strip()


def execute(name, acts, s, run_add=None, stuck=None):
    """做动作 → [说明]。出错的那一步写进说明，后面的照做。在 hub 上加任务（传数据、拉回、归档）出错的记进 stuck，
    下次检查照常重试，加上了报警就消失。"""
    L, msgs = RM.conf(name)["label"], []
    stuck = {} if stuck is None else stuck
    raw_add = run_add or refill.add
    what = dict(relay_stage="传数据", relay_fetch="拉回", relay_archive="归档")

    def add(host, lab, step, k, args, **kw):
        try:
            return raw_add(host, lab, step, k, args, **kw)
        except Exception as e:  # noqa: BLE001
            stuck[alert_id("relayadd", name, step, k)] = alert(f"{L} 的 {k}：在 {M.hub()} 上排{what[step]}的任务失败（{RM.err_tail(str(e), 200)}），下次检查重试",
                                                                machine=name, step=step, key=k)
            raise

    def master(args):
        r = taskq.central(args)
        return r[0], r[1] + r[2]
    beats = [a["key"] for a in acts if a["op"] == "beat"]
    lost = set()  # 总队列里已经不是分给它的（别处交还或改分了）：这一轮不再替它动这些
    if beats:
        rc, out, err = taskq.central(["beat", "--strict", "--worker", name] + beats)
        if rc == proto.EXIT["lost"]:
            lost = set(out.split()) & set(beats)
            msgs.append(f"{L} 的领取已经不是它的：{', '.join(sorted(lost))}，这一轮不动")
        elif rc:
            msgs.append(f"{L} 的领取续不上（{RM.err_tail(out + err, 150)}）")
    kept = set()  # 那边没删掉的：总队列里不交还（交还了别处会再领，两处一起训）
    for a in acts:
        k = a["key"]
        if k in lost:
            continue
        try:
            if a["op"] == "smoke_add":  # 加进总队列，接着领过来（take → stage）
                ok, m = SG.enqueue(name, a)
                msgs.append(m)
                if ok is None:  # 已经有排着的
                    continue
                if not ok:
                    stuck[alert_id("smoke", name, a["mark"]["kid"])] = alert(m, machine=name, kind=a["mark"]["kind"], why=m, key=k)
                    continue
                a = dict(op="take", key=k, nice=proto.SMOKE_NICE)
            if a["op"] in ("release", "smoke_end") and k in kept:
                msgs.append(f"{L} 上 {k} 没删掉，先不交还")
            elif a["op"] == "smoke_fail":
                SG.record(name, a["mark"], state="failed", src="smoke", key=None, bundle=None, why=a["why"][-600:])
                msgs.append(f"{L} 上 {a['mark']['kind']} 冒烟没过：{a['why'][-200:]}")
            elif a["op"] == "smoke_rec":
                why = a["why"] if a["ok"] else (a["why"] + "\n" + smoke_tail(name, a["rkey"])).strip()
                SG.finish(name, k, a["ok"], why)
                msgs.append(f"{L} 上冒烟 {k} {'过了' if a['ok'] else '没过：' + why[-200:]}")
            elif a["op"] == "smoke_end":
                master(f"release --worker {name} {shlex.quote(k)}")
                r = master(f"remove {shlex.quote(k)}")
                msgs.append(f"{L} 的冒烟 {k} 从总队列里去掉了" if r[0] in (0, proto.EXIT["gone"]) else f"{L} 的冒烟 {k} 去不掉：{RM.err_tail(r[1], 150)}")
            elif a["op"] == "smoke_real":
                if SG.real_done(name, a["mark"]):
                    msgs.append(f"{L} 上 {a['mark']['kind']} 的真任务 {k} 训完了，记成过了冒烟")
            elif a["op"] == "release":
                r = master(f"release --worker {name} {shlex.quote(k)}" + (f" --skip {shlex.quote(a['skip'])}" if a.get("skip") else ""))
                msgs.append(f"{L} 交还 {k}：{a['why']}" if r and r[0] == 0 else f"{L} 交还 {k} 出错：{RM.err_tail((r or [0, 'ssh 不通'])[1], 150)}")
            elif a["op"] == "take":
                r = master(f"take --worker {name} --assign {shlex.quote(k)}")
                if r is None or r[0]:
                    msgs.append(f"{L} 没领到 {k}：{RM.err_tail((r or [0, 'ssh 不通'])[1], 150)}")
                    continue
                msgs.append(f"{L} 领了 {k}（档位 {a['nice']}）")
                for t in (s.get("taskq", {}).get(taskq.queue_host()) or {}).get("tasks", []):  # 这次检查里后面的执行机别再挑它
                    if t["key"] == k:
                        t.update(worker=name, assigned=name, state="claimed")
                a = dict(a, op="stage")
            if a["op"] in ("stage", "restage"):
                lab = label("relay_stage", k)
                cmd = f"python3 {shlex.quote(cfg.local('queue', 'relay.py'))} stage {name} {shlex.quote(k)} " + (
                    "--no-add" if a["op"] == "restage" else f"--nice {int(a['nice'])}")
                tid = add(M.hub(), lab, "relay_stage", k, f"{proto.IO_GROUP} -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group=proto.IO_GROUP)
                msgs.append(f"{L} 的 {k} 排了{'补传缺的输入' if a['op'] == 'restage' else '传数据'}（{M.hub()} 任务 {tid}）")
            elif a["op"] == "fetch":
                lab = label("relay_fetch", k)
                cmd = f"python3 {shlex.quote(cfg.local('queue', 'relay.py'))} fetch {name} {shlex.quote(k)}"
                tid = add(M.hub(), lab, "relay_fetch", k, f"{proto.IO_GROUP} -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group=proto.IO_GROUP)
                msgs.append(f"{L} 训完了 {k}，排了拉回（{M.hub()} 任务 {tid}）")
            elif a["op"] == "archive":
                lab = label("relay_archive", k)
                cmd = f"python3 {shlex.quote(cfg.local('queue', 'relay.py'))} archive {shlex.quote(k)}"
                tid = add(M.hub(), lab, "relay_archive", k, f"{proto.IO_GROUP} -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group=proto.IO_GROUP)
                msgs.append(f"{L} 训完的 {k} 排了归档（{M.hub()} 任务 {tid}）")
            elif a["op"] == "drop":
                if a.get("cleanup"):
                    ST.update_state(lambda st: ((st.get("keys") or {}).get(name, {}).get(k) or {}).update(
                        cleanup=sorted(set(((st.get("keys") or {}).get(name, {}).get(k) or {}).get("cleanup") or []) | set(a["cleanup"]))))
                try:
                    err = RT.drop_remote(name, k, a["rkey"], gone=a.get("gone"))
                except RT.PathsLeft as e:  # 任务已经删了，不会再训：照常交还，路径下一轮再删
                    msgs.append(f"{L} 上删了 {k} 的任务，结果没删掉（{e}），下次检查再删")
                    continue
                if err:
                    msgs.append(f"{L} 上删 {k} 出错（{err}）")
                    kept.add(k)
                else:
                    msgs.append(f"{L} 上删了 {k} 的任务和结果")
            elif a["op"] == "hold":
                BG.hold(name, k, a["bundle"], a["err"])
                msgs.append(f"{L} 在用的代码包 {a['bundle']} 跑不了 {k} 的说明，先不送：{a['err']}")
            elif a["op"] == "unhold":
                BG.unhold(name, k)
                msgs.append(f"{L} 的 {k} 不再挡着（代码包换了，或任务不再等着送）")
            elif a["op"] == "rnice":
                r = RM.rtq(name, f"set-nice --nice {int(a['nice'])} {shlex.quote(a['rkey'])}")
                msgs.append(f"{L} 上 {k} 改到档位 {a['nice']}" if r and r[0] == 0 else f"{L} 上 {k} 改档位出错：{RM.err_tail((r or [0, 'ssh 不通'])[1], 150)}")
            elif a["op"] in ("rstop", "rresume"):
                r = RM.rtq(name, ("stop --why 总队列里停了 " if a["op"] == "rstop" else "resume ") + shlex.quote(a["rkey"]))
                what = "停" if a["op"] == "rstop" else "撤销停止"
                msgs.append(f"{L} 上{what} {k}" if r and r[0] == 0 else f"{L} 上{what} {k} 出错：{RM.err_tail((r or [0, 'ssh 不通'])[1], 150)}")
        except Exception as e:  # noqa: BLE001
            msgs.append(f"{L} {a['op']} {k} 出错：{e}")
            if a["op"] == "drop":
                kept.add(k)
    return msgs


def submit(name, subs):
    """照 topup 算好的开 worker（sites/pueue.py、slurm.py）→ (说明, 出错的 {问题编号: 说明})。"""
    L, msgs, stuck = RM.conf(name)["label"], [], {}

    def run(c):
        r = RM.rsh(name, c)
        return r[0], r[1] + r[2]
    if RM.conf(name)["scheduler"] == "pueue":
        for _, w in subs:
            ok, out = PQ.start_worker(name, run, w)
            if ok:
                msgs.append(f"{L} 开了 {w['id']}（{out[-80:]}）")
            else:
                stuck[alert_id("site", name, "worker", w["id"])] = alert(f"{L} 开 {w['id']} 出错：{out}", worker=w["id"], start=True)
        return msgs, stuck
    for r in SL.start_workers(name, subs, run):
        k = r["kind"]
        if r["err"] is not None:
            stuck[kkey("topup", name, r["qos"], wkind=k)] = alert(f"{L} {r['qos']} 补交 {wname(k)} 作业出错：{r['err']}", qos=r["qos"], kind=k)
        if r["ids"]:
            msgs.append(f"{L} {r['qos']} 交了 {len(r['ids'])} 个 {wname(k)} 作业 {'、'.join(r['ids'])}（每个 {r['time']}）")
    return msgs, stuck


def site_view(name, rs, st):
    """那边的状态 → {真 key: dict(state, worker, runs, final, resumable, debug)}（status.json 的 tasks）。"""
    rt = {t["key"]: t for t in (rs.get("taskq") or {}).get("tasks") or []}
    out = {}
    for real in ST.keys_of(st, name):
        a = rt.get(real)
        if a:
            f = (rs.get("tasks") or {}).get(real) or {}
            out[real] = dict(state=a["state"], worker=a.get("worker"), runs=f.get("runs") or [], final=f.get("final"),
                             resumable=f.get("resumable"), ran=a.get("progress"), debug=f.get("debug") or {})
    return out


def write_status(name, rs, st):
    out = site_view(name, rs, st)
    try:
        allst = json.load(open(STATUS))
    except (OSError, ValueError):
        allst = {}
    allst[name] = dict(time=time.time(), label=RM.conf(name)["label"], tasks=out, squeue=rs.get("squeue"), free=rs.get("free"),
                       bundle=rs.get("bundle"), live=rs.get("live"), held=BG.held(st, name), envs_ok=rs.get("envs_ok"))
    write_json(STATUS, allst)


def log(lines):
    import events
    events.lines("relay", lines)


def envs_switch(name):
    """policy.py 的 AUTO["envs"] → (开着没有, 这台另外预建的 [环境 或 (环境, 变体)])。"""
    a = P.AUTO.get("envs")
    if isinstance(a, dict):
        return True, list(a.get(name) or [])
    return bool(a), []


def env_builds(s, name, rows, now, dry, stuck):
    """按描述建的环境（sites/envs.py）：没建、描述变了、在建停了、空间不够过了 RETRY_S 的，在 hub 的核心组（proto.IO_GROUP）排一个
    taskq site env（一台一次一个；hub 上已经有这台的就不排）；建坏的、空间不够、wheelhouse 缺包的报 site:<机器>:env:<环境>。
    开关 AUTO["envs"] 关着时只记进 ~/relay.log。rows、now：那边的标志文件和时间（envs.read_rows 或状态程序的 envs_st）。→ 说明"""
    on, extra = envs_switch(name)
    want = EV.wanted(name, extra)
    if not want:
        return []
    st = EV.classify(rows, want, now)
    probs = EV.problems(name, want, st, rows)
    if on:
        for env, msg in probs:
            stuck[alert_id("site", name, "env", env)] = alert(msg[:1500], machine=name, sub="env", key=env, state=st.get(env))
    nb = EV.next_build(want, st, rows, now, skip={k for k, _ in probs if st.get(k) not in ("检查失败", "空间不够")})
    if nb is None:
        return []
    env, v = nb
    hub, L = M.hub(), M.machine(name)["label"]
    d = (s.get("pueue") or {}).get(hub) or {}
    if "tasks" not in d:  # 读不到 hub 的 pueue：不知道排没排过，这次不排
        return []
    lab = f"{proto.ENV_BUILD}:{name}:{env}"
    if any((t.get("label") or "").startswith(f"{proto.ENV_BUILD}:{name}:") and t.get("status") in refill.LIVE for t in d["tasks"]):
        return []
    what = f"{L} 的环境 {env}（{v}，{st[env]}）"
    if dry:
        return [f"会建 {what}"]
    if not on:
        if C.changed(f"envs_{name}", lab):
            log([f"（开关没开）会建 {what}"])
        return []
    cmd = f"python3 {shlex.quote(cfg.local('sites', 'envs_cli.py'))} env {name} {shlex.quote(env)} --variant {shlex.quote(v)}"
    try:
        tid = refill.add(hub, lab, proto.ENV_BUILD, f"{name}:{env}",
                         f"{proto.IO_GROUP} -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)), to=name, group=proto.IO_GROUP)
    except Exception as e:  # noqa: BLE001
        stuck[alert_id("relayadd", name, proto.ENV_BUILD, env)] = alert(f"{L} 的环境 {env}：在 {hub} 上排建环境的任务失败（{RM.err_tail(str(e), 200)}），下次检查重试",
                                                                         machine=name, step=proto.ENV_BUILD, key=env)
        return []
    return [f"排了建 {what}（{M.hub()} 任务 {tid}）"]


def run_all(s, dry=False):
    """health.py 调用 → (说明, 卡住的 {问题编号: 说明})。先读、整理每台执行机（prepare），再把总队列里能派的任务一次分给各台
    （assign），最后每台照着做（finish）。"""
    events, stuck, ctxs = [], {}, {}
    if not dry:  # 先换包：store、执行机上的操作（总队列的状态、收集）都用它，读不到总队列时也换（新装、换格式后自己接上）
        events += update_code(stuck)
    if not s["ok"].get(M.hub()) or not s.get("taskq", {}).get(taskq.queue_host()):
        return events, stuck
    for name in M.executors():
        if M.paused(name):
            continue
        try:
            ctxs[name] = prepare(s, name, dry)
        except Exception as e:  # noqa: BLE001
            stuck[alert_id("relay", name)] = f"queue/relay.py（{name}）出错：{type(e).__name__}: {e}"[:300]
    snotes = {}
    try:
        more, why = assign(s, ctxs, stuck, snotes)
    except Exception as e:  # noqa: BLE001
        more, why = {}, {n: "派任务出错" for n in ctxs}
        stuck[alert_id("relay", "assign")] = f"queue/relay.py 派任务出错：{type(e).__name__}: {e}"[:300]
    for name, c in ctxs.items():
        try:
            ev, st_ = finish(s, name, c, more.get(name, []), why.get(name, ""), snotes.get(name) or [], dry)
        except Exception as e:  # noqa: BLE001
            ev, st_ = [], {alert_id("relay", name): f"queue/relay.py（{name}）出错：{type(e).__name__}: {e}"[:300]}
        events += ev
        stuck.update(st_)
    if not dry:
        try:
            prune_bundles(wanted_bundle()[0])
        except Exception as e:  # noqa: BLE001
            events.append(f"清理 hub 上的旧代码包出错：{e}")
    return events, stuck


def prepare(s, name, dry):
    """读那边的状态（读不到、代码包不同时先部署再读）、清理、算这台的动作 → dict(rs, st, acts, stuck, notes)。"""
    st, L = ST.load_state(), RM.conf(name)["label"]
    rs = remote_status(name)
    notes = []
    # 读不到状态、但连得上：还没有代码包（新机器），或那边的旧包跑不了这边新改的状态程序 → 包不同时先部署，再读一次
    cur = current_bundle(name) if rs is None and not dry else None
    ph = {"bundle": cur or None} if cur is not None else None
    if ph is not None:
        rs = ph
    if not dry and rs is not None:
        try:
            bid, path = wanted_bundle()
            if rs.get("bundle") != bid:
                try:
                    deploy(name, bid, path, claim_times(rs))
                    notes.append(f"{L} 的代码换成 {bid}")
                    rs = remote_status(name)
                except RuntimeError as e:
                    notes.append(str(e))
                    rs = None  # 那边还是旧包，和这边的命令、路径可能对不上：这次当连不上，只续约，下次再推
        except Exception as e:  # noqa: BLE001
            notes.append(f"{L} 打包或推代码包出错：{e}")
            rs = None  # 同上：超时、打包出错时那边也还是旧包
    if ph is not None and rs is ph:  # 没有部署（包相同）或部署后仍读不到
        rs = None
    if rs is not None and not dry:
        ST.update_state(lambda x: x.setdefault("last_ok", {}).update({name: time.time()}))
        write_status(name, rs, ST.load_state())
    if rs is not None and not dry:
        try:
            st = ST.load_state()
            reg = (st.get("views") or {}).get(name) or {}
            keep = {v for k in ST.keys_of(st, name).values() for v in ST.paths_of(k)}
            keep |= {v for v, t in reg.items() if time.time() - t < VIEW_KEEP_S}
            g = ST.gc(name, keep, RM.op, views=reg, free=rs.get("free") if RM.conf(name).get("cap") else None,
                      blob_free=rs.get("blob_free") if RM.conf(name).get("blob_cap") else None)
            if g and g["removed_views"]:
                ST.update_state(lambda x: [((x.get("views") or {}).get(name) or {}).pop(v, None) for v in g["removed_views"]])
            if g and (g["removed_views"] or g["removed_blobs"]):
                notes.append(f"{L} 清理：删了 {len(g['removed_views'])} 个数据集视图、{g['removed_blobs']} 个 blob，"
                             f"放出 {g['freed'] / 1e9:.0f} GB，剩 {g['free'] / 1e9:.0f} GB")
                rs = remote_status(name) or rs
        except Exception as e:  # noqa: BLE001
            notes.append(f"{L} 清理出错：{e}")
    st = ST.load_state()
    acts, stuck = plan(s, name, rs, st)
    if rs is not None and "envs_st" in rs:
        notes += env_builds(s, name, rs["envs_st"], rs["now"], dry, stuck)
    return dict(rs=rs, st=st, acts=acts, stuck=stuck, notes=notes)


def finish(s, name, c, more, why, snotes, dry):
    """照 prepare 算的动作和 assign 分来的任务做、补 worker → (说明, 卡住的)。"""
    L, rs, acts, stuck, notes = RM.conf(name)["label"], c["rs"], c["acts"], c["stuck"], c["notes"]
    if snotes and not dry and C.changed(f"smoke_{name}", snotes):
        log(snotes)
    subs, swhy = (topup(name, rs) if rs is not None else ([], "连不上"))
    for part in swhy.split("；"):
        w = next((w for w in M.workers(name) if part.startswith(f"上一个 {w['id']}（")), None)
        if w:
            stuck[alert_id("site", name, "worker", w["id"])] = alert(f"{L} {part}；看那边的 pueue log，处理后 pueue remove 那个任务（下次检查重开）",
                                                                     worker=w["id"], start=False)
    todo = [a for a in acts if a["op"] != "beat"]
    desc = "；".join(f"{a['op']} {a['key']}" for a in todo + more)
    sdesc = "；".join(f"开 {x[1]['id']}" if x[0] == "pueue" else f"交 {x[0]} {x[2]} 个 {wname(sub_kind(x))}" for x in subs)
    if dry:
        return [f"{L}：会 {desc}" if desc else f"{L}：不动（{why}）",
                f"{L}：会 {sdesc}" if sdesc else f"{L}：不交作业（{swhy}）"] + notes, stuck
    msgs = notes + execute(name, acts, s, stuck=stuck) + execute(name, more, s, stuck=stuck)
    m2, st2 = submit(name, subs)
    msgs += m2
    stuck.update(st2)
    log(msgs)
    return msgs, stuck


def main(argv):
    ap = argparse.ArgumentParser(description="执行机的接力。不带命令：只看每台执行机现在会做什么")
    ap.add_argument("-n", "--dry-run", "--dry", action="store_true", help="只看（不带命令时默认就是）")
    sub = ap.add_subparsers(dest="cmd")
    p = sub.add_parser("stage", help="传数据、加进那边的队列")
    p.add_argument("name")
    p.add_argument("key")
    p.add_argument("--nice", type=int)
    p.add_argument("--no-add", action="store_true")
    p = sub.add_parser("fetch", help="拉回做完的任务、核对，放进 store 的数据目录")
    p.add_argument("name")
    p.add_argument("key")
    p = sub.add_parser("archive", help="store 上这个任务的结果备份到备份机、核对")
    p.add_argument("key")
    p = sub.add_parser("bundle", help="把该用的代码包推过去、切换 current（执行机，或有根目录的 store、hub）")
    p.add_argument("name")
    p = sub.add_parser("status", help="那边的队列")
    p.add_argument("name")
    p = sub.add_parser("resend", help="传过去的任务按现在的做法重新解析，换掉那边队列里的说明")
    p.add_argument("name")
    a = ap.parse_args(argv)
    if a.cmd == "archive":
        try:
            RT.archive(a.key)
        except (RN.RelayError, S.SpecError, RuntimeError) as e:
            print(f"{ap.prog} archive {a.key}：{e}", file=sys.stderr)
            return proto.EXIT["gone"] if isinstance(e, RT.Gone) else 1
        return 0
    if a.cmd == "bundle" and a.name in code_only():
        print(bundle(a.name, current_bundle(a.name), claims=[]) or "没变")
        return 0
    if a.cmd and not M.is_exec(a.name):
        print(f"{ap.prog} {a.cmd}：没有执行机 {a.name}（有：{'、'.join(M.executors()) or '无'}）", file=sys.stderr)
        return 2 if a.cmd == "stage" else proto.EXIT["gone"]
    if a.cmd in ("stage", "fetch"):
        if a.cmd == "stage" and a.nice is None and not a.no_add:
            ap.error("要给 --nice（或 --no-add）")
        try:
            if a.cmd == "fetch":
                RT.fetch(a.name, a.key)
            elif RT.stage(a.name, a.key, a.nice, add=not a.no_add) is None:
                return NOSLOT_RC
        except (RN.RelayError, S.SpecError, RuntimeError, OSError, subprocess.SubprocessError) as e:
            if a.cmd == "stage":
                print(f"relay.py {a.cmd} {a.name} {a.key}：{type(e).__name__}: {e}", file=sys.stderr)
                return 1
            print(f"{ap.prog} {a.cmd} {a.name}：{e}", file=sys.stderr)
            return proto.EXIT["gone"] if isinstance(e, RT.Gone) else 1
    elif a.cmd == "bundle":
        rs = remote_status(a.name)
        print(bundle(a.name, (rs or {}).get("bundle"), claim_times(rs)) or "没变")
    elif a.cmd == "resend":
        RT.resend(a.name)
    elif a.cmd == "status":
        rs = remote_status(a.name)
        if rs is None:
            print(f"读不到 {a.name}", file=sys.stderr)
            return 1
        for t in rs["taskq"]["tasks"]:
            print(f"  {t['nice']:>5} {t['key']:50} {t['state']} {t['note']}")
        print(f"剩 {rs['free'] / 1e9:.0f} GB；代码 {rs['bundle']}；作业 {rs['squeue']}")
    else:
        import health as H
        ev, st = run_all(H.gather(), dry=True)
        print("\n".join(ev + [f"卡住：{alert_msg(v)}" for v in st.values()]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
