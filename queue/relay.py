#!/usr/bin/env python3
"""relay 执行机（envs/machines.py 的 RELAY：lab1、lab0、dicc1、dicc2）的接力：由 box 替它在总队列（box 上的任务队列，10/31 起）
领任务（GPU 或 CPU，按它有的 worker 种类和环境）、把输入放到和 box 一样的相对路径、放进它自己的任务队列，做完拉回、归档。
执行机和 box 用同样的名字和路径；执行机之间不直接通信，一切经 box。
这里是每次检查的计划和命令行；和那边说话、发过去跑的小程序在 relay_remote.py（那边根目录的布局也在那里），box 上的记账和那边的清理
在 relay_store.py，传数据、拉回、删那边的任务在 relay_transfer.py。

每次健康检查（monitor/health.py，每 30 分钟）经 queue/feed.py 的 run_all 调用这里的 run_all（送任务只有一个入口，10/31）：
  1. ssh 一次读那边的队列、各任务进度、作业、剩余空间（写到 ~/.cache/phd/relay/status.json，登记表用）；
  2. 总队列里 worker 是它的每个任务：
     - 还没放进那边的队列 → box 的 wait 组排 relay.py stage（补传缺的数据、建软链接、加进那边的队列；要放的路径和那边没做完的
       任务的产出重叠时不传、报出来）；
     - 在那边排着、在训 → 续上（beat）；
     - 那边做完 → box 的 wait 组排 relay.py fetch（拉回、核对）；
     - 总队列显示训完（box 上有了）→ 删掉那边的任务和结果（11/3 定：执行机的盘只用来中转）；要归档的，备份机能用时 box 的 wait 组
       单独排 archive_task.sh box（relay_archive，备份到 envs/machines.py 角色 backup 的那台），归档成功才交还；备份机停用
       （envs/policy.py 的 OFFLINE）时不等，直接交还；只是这次连不上时下次检查再排；
     - 那边失败：没有进度 → 交还（--skip，它以后不再领），删掉那边的任务；有进度 → 领取保留、报出来；
     - 总队列里停了（taskq.py stop）→ 那边也停，那边不再领着以后交还（那边的结果留着）；撤销停止后再领到时那边接着训；
     - 传数据、拉回出错、验证集对不上 → 领取保留、报出来（处理后 pueue remove 那个 box 任务，下次检查重排）。
  3. 那边排着没人领的任务少于 envs/policy.py 的 READY[名字] 时，按 refill.py 的挑法领下一个任务（只领插件接受的做法，
     数据传过去后放 blobs 的盘至少还剩 budget 的第二项（没有 budget 时 HARD_GB）；有 cap 的执行机，那边每个任务的输出另留
     refill.TRAIN_OUT_GB，cap 里留不出来就不领。blobs 可以是指到别的盘的链接（DICC：/scr，11/4），输出和队列在根目录）；
  4. 那边排着和在跑的 worker 作业不够、又有要卡的任务（备好的或在训的）时排满（refill.py 的 topup_plan，QOS 和个数见 SLURM[名字]["gpu"]）。
开关 envs/policy.py 的 AUTO[名字]：关着时第 3、4 步只记进 ~/relay.log（影子），已经领着的任务照常推进。
按时段的执行机（envs/policy.py 的 SHIFT，queue/shift.py）时段外不做第 3、4 步。同一个盘上的几台（lab0、lab1）：
每次检查按整块盘的剩余算，前一台这次计划传的从后一台的剩余里扣掉；总队列里改分给别的执行机的任务，那边的队列里删掉任务
（结果留着，接手的那台收编存档，见 machines.py 的 adopt），总队列里训完了再删结果。
连不上它时只续总队列的领取；领着任务、连不上 2 小时以上报出来。

    python3 queue/relay.py                         看现在会做什么（不改任何东西；taskq plan 连同按时段开关、训完以后一起看）
    taskq internal stage <名字> <key> --nice N [--no-add]   传数据、加进那边的队列（--no-add：只传数据、撤掉那边"缺输入"的标记）
    taskq internal fetch <名字> <key>              拉回做完的任务、核对，放进 box 的数据目录
    taskq site bundle <名字> [--commit C]          打包、推过去、切换 current
    taskq site status <名字>                       那边的队列
    taskq site resend <名字>                       传过去的任务按现在的做法重新解析，换掉那边队列里的说明（11/4）
（原来的 python3 queue/relay.py stage|fetch|bundle|status|resend … 照旧能用。）
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
import shift  # noqa: E402
from common import write_json  # noqa: E402

STATUS = os.path.join(ST.DIR, "status.json")
LOG = cfg.log("relay")
HARD_GB = 100  # 领之前：传过去以后那边至少还剩这么多
DOWN_S = 2 * 3600  # 领着任务、连不上这么久就报
LATE_S = 3600  # 拉回、归档成功这么久了 box 上还不算训完，就报
LIVE = refill.LIVE
VIEW_KEEP_S = 6 * 3600  # 建好这么久以内的视图不清理（任务刚传完、还没加进队列或刚交还）
STATUS_S = 90  # 读状态的超时：那边的存储挂住时跳过这台，不等
NOSLOT_RC = proto.EXIT["stage_later"]  # relay.py stage 这次排不上 CPU 作业（或存档的来源那边还领着）的退出码：下次检查再传，不报
NOSLOT_ALERT_S = 3 * 3600  # 领了这么久还一直排不上就报
PLANNED = {}  # 这次检查里各台已经计划传的 GB，按 (ssh, 盘) 记：同一个盘上的几台合起来算


def relayable(i):
    """refill.info 的结果 → 能不能领给它：做法能翻译（按本机的 recipes.py 算，总队列那边的代码可能还没更新），插件接受这个任务
    （sites/names.py 的 accepts，如 nnU-Net 的训练器不读仓库里的病例子集）。"""
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
    if c.get("ssh_master") and subprocess.run(RM.ssh_argv()[:1] + ["-O", "check", c["ssh"]], capture_output=True, timeout=30).returncode:
        return None  # 长连接断着：不另起连接（中转执行机认证失败多了会被锁）
    try:
        rs = RM.rpy(name, RM.STATUS_PY, dict(sched=c["scheduler"], pueue=c.get("pueue"), cap=c.get("cap")), timeout=STATUS_S)
    except RuntimeError:
        return None
    return parse_sched(name, rs)


def current_bundle(name):
    """读不到状态时看那边的 code/current → 包名；还没有代码包（新机器第一次）→ ""；连不上 → None。"""
    c = RM.conf(name)
    if c.get("ssh_master") and subprocess.run(RM.ssh_argv()[:1] + ["-O", "check", c["ssh"]], capture_output=True, timeout=30).returncode:
        return None
    cur = f"{R.q(RM.root(name))}/{ROOT['current']}"
    rc, out, _ = RM.rsh(name, f'if [ -e {cur} ]; then basename "$(readlink -f {cur})"; fi', timeout=30)
    return out.strip() if rc == 0 else None


def parse_sched(name, rs):
    """状态脚本读回的调度器原始输出 → rs["jobs"]（pueue：各种 worker 所在组的任务）或 rs["squeue"]（Slurm）；读不出 → None。
    一次 ssh 读完、在 box 上用适配器解析（sites/pueue.py、slurm.py）：每台每次只连一次，状态是同一时刻的。"""
    raw, c = rs.pop("sched_raw", None), RM.conf(name)
    rs["jobs"] = rs["squeue"] = None
    if raw is not None and c["scheduler"] == "pueue":
        rs["jobs"] = PQ.parse(raw, [M.worker_of(name, k)["group"] for k in M.kinds(name)])
    elif raw is not None:
        rs["squeue"] = SL.squeue(raw)
    return rs


# ---------------- 代码包 ----------------
def deploy(name, bid, path):
    """把包推过去、切换 current（原子的 mv -T）；第一次时建目录、开队列。
    整份收下、sha256 对上才解开（ssh 断在两个文件之间时 tar 不报错）；同一台同时只推一个（flock），临时目录各用各的。
    清理旧包时留下刚切过去的、排着和在跑的作业还在用的（适配器的 busy_cmd），读不到就不清理；被打断的部署留下的临时文件 1 天后删。"""
    r = R.q(RM.root(name))
    b = shlex.quote(bid)
    tar = subprocess.run(["tar", "-C", path, "-cf", "-", "."], capture_output=True, check=True).stdout
    sha = hashlib.sha256(tar).hexdigest()
    busy = (SL if RM.conf(name)["scheduler"] == "slurm" else PQ).busy_cmd()
    cur = os.path.relpath(ROOT["current"], ROOT["code"])  # code/ 下的名字
    tq, en = f"$R/{ROOT['queue']}/{proto.QUEUE['tasks']}", f"$R/{ROOT['queue']}/{proto.QUEUE['enabled']}"
    cmd = (f"set -e; R={r}; mkdir -p {' '.join('$R/' + d for d in proto.MKDIRS)}; "
           f"[ -e {tq} ] || {{ : > {tq}; touch {en}; }}; cd $R/{ROOT['code']}; "
           f"exec 9> $R/{ROOT['deploy_lock']}; flock -w 300 9; "
           "find . -maxdepth 1 -name '*.tmp.*' -mtime +0 -exec rm -rf {} + || true; "  # 以前被打断的部署留下的（锁里没有别的在推）
           f"t={b}.tmp.$$; rm -rf $t $t.tar; cat > $t.tar; "
           f"if [ \"$(sha256sum < $t.tar | cut -d' ' -f1)\" != {sha} ]; then rm -f $t.tar; echo '代码包没传完整' >&2; exit 3; fi; "
           f"if [ ! -d {b} ]; then mkdir $t; tar -xf $t.tar -C $t; mv -T $t {b}; fi; rm -f $t.tar; "
           f"touch {b}; ln -sfn {b} {cur}.tmp.$$; mv -T {cur}.tmp.$$ {cur}; "
           f"if busy=$({busy}); then busy=$(printf '%s\\n' \"$busy\" | sed 's#$#/#'); "
           f"ls -1t | grep -vx {cur} | grep -v '\\.tmp' | tail -n +4 | "
           f"while read -r d; do [ \"$d\" = {b} ] && continue; case \"$busy\" in *\"/{ROOT['code']}/$d/\"*) continue;; esac; "
           "find \"$d\" -maxdepth 0 -mtime +4 -exec rm -rf {} +; done; fi")
    r2 = subprocess.run(RM.ssh_argv() + [RM.conf(name)["ssh"], C.remote_timeout(f"bash -c {shlex.quote(cmd)}", 600)], input=tar,
                        capture_output=True, timeout=600)
    if r2.returncode:
        raise RuntimeError(f"推代码包 {bid} 到 {RM.conf(name)['label']} 出错：{r2.stderr.decode(errors='replace')[-300:]}")


BUNDLE_REF = "origin/main"  # 打代码包用的提交（测试里钉住，别处推送 main 时不跟着换）


def bundle(name, commit="origin/main", have=None, run_build=None):
    """origin/main 打的包和那边的 current 不同就推过去 → 说明（没变 → None）。"""
    bid, path = (run_build or RB.build)(name, commit)
    if bid == have:
        return None
    deploy(name, bid, path)
    return f"{RM.conf(name)['label']} 的代码换成 {bid}（{commit}）"


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
    """训完、有做完的轮数，或者有存档（任务说明的 resume）→ 有进度。"""
    return bool(f) and bool(f.get("final") or f.get("resumable")
                            or any((r.get("epochs_done") or 0) > 0 for r in f.get("runs") or []))


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
        elif a["state"] in ("failed", "bad", "blocked"):
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
    动作 dict(op=beat|release|stage|restage|fetch|drop|unq|rstop|rresume|rnice|archive|hold|unhold, key, …)；rkey：那边队列里的 key。"""
    now = now or time.time()
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    L, keys = RM.conf(name)["label"], ST.keys_of(st, name)
    mine = [t for t in (mtq or {}).get("tasks", []) if t.get("worker") == name]
    acts, stuck = [], {}
    if not mtq or not mtq.get("enabled"):
        return [], {}
    if rs is not None and "error" in (s.get("pueue", {}).get(M.hub()) or {"error": 1}):
        rs = None  # box 的 pueue 读不到：在跑的拉回、归档看不见，不能当成没有，只续约（11/3）
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
            ft = box_task(s, "relay_fetch", key, t.get("since") or 0)
            at = box_task(s, "relay_archive", key, t.get("since") or 0)
            if any(x is not None and x["status"] in LIVE for x in (ft, at)):  # box 上有了，但拉回、归档还没做完：等
                acts.append(dict(op="beat", key=key))
                continue
            # 总队列显示训完：box 上有了（结果本身，不看拉回记录）。只有单独的拉回出错时才报（11/3 起拉回不带归档）
            fcmd = (ft or {}).get("command") or ""
            if ft is not None and ft["status"] != "Success" and "archive_task.sh" not in fcmd:
                acts.append(dict(op="beat", key=key))
                stuck[alert_id("relayfetch", key)] = alert(f"从 {L} 拉回 {key} 的 box 任务 {ft['id']} 出错了（{ft['status']}），领取保留；"
                                                           f"看 pueue log {ft['id']}，处理后 pueue remove {ft['id']}（下次检查重排）",
                                                           machine=name, step="fetch", task=ft["id"], status=ft["status"])
                continue
            if k:  # box 上有了就删那边的任务和结果，不等归档（11/3 定：执行机的盘只用来中转）
                acts.append(dict(op="drop", key=key, rkey=key))
            try:
                want = ft is not None and bool(RT.lookup(key).get("archive"))
            except (RN.RelayError, S.SpecError):
                want = False
            archived = (at is not None and at["status"] == "Success") or (ft is not None and ft["status"] == "Success" and "archive_task.sh" in fcmd)
            if want and not archived:
                if at is not None:
                    acts.append(dict(op="beat", key=key))
                    stuck[alert_id("relayfetch", key)] = alert(f"{key} 归档到备份机的 box 任务 {at['id']} 出错了（{at['status']}），box 上有了、领取保留；"
                                                               f"看 pueue log {at['id']}，处理后 pueue remove {at['id']}（下次检查重排）",
                                                               machine=name, step="archive", task=at["id"], status=at["status"])
                    continue
                if not C.backup_down():  # 备份机能用：box 单独排归档，归档成功才交还；只是这次连不上时下次再排
                    acts.append(dict(op="beat", key=key))
                    if not C.backup_down(s.get("ok")):
                        acts.append(dict(op="archive", key=key))
                    continue
            acts.append(dict(op="release", key=key, why="训完了，box 上有了" + ("、归档了" if archived else "（没有归档）")))
            sp = refill.info(mtq, key)["spec"]
            try:
                if sp and sp.get("recipe") and SG.on():
                    acts.append(dict(op="smoke_real", key=key, mark=SG.mark(name, sp, rs.get("envs_ok"))))
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
            acts.append(dict(op="rnice", key=key, rkey=a["key"], nice=t["nice"]))  # 总队列里改了档位（set-nice）：那边跟着改（11/3）
        since = t.get("since") or 0
        if a is None:
            sg = box_task(s, "relay_stage", key, since)
            noslot = sg is not None and sg["status"] not in LIVE and sg.get("code") == NOSLOT_RC
            if sg is None or sg["status"] == "Success" or noslot:
                g, h = gate(name, rs, mtq, key), holds.get(key) or {}
                if not g and h.get("bundle") in BG.in_use(name, rs.get("bundle"), rs.get("live")):
                    g = (h["bundle"], h.get("err"))  # stage 按要送的说明挡下的：同一个包还在用就不再排
                if g:  # 那边在用的代码包跑不了这个说明：先不送，领取保留，那边换了包再核对（11/5）
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
                        f"（box 任务 {sg['id']}，每次检查再试）", key=key, hours=round((now - since) / 3600, 1), task=sg["id"])
            elif sg["status"] not in LIVE:
                stuck[alert_id("relaystage", key)] = alert(f"给 {L} 传 {key} 的 box 任务 {sg['id']} 出错了（{sg['status']}），领取保留；"
                                                           f"看 pueue log {sg['id']}，处理后 pueue remove {sg['id']}（下次检查重传）",
                                                           machine=name, step="stage", task=sg["id"], status=sg["status"])
            continue
        ast = a["state"]
        held = a.get("worker") and (now - (a.get("beat") or 0) < proto.STALE_BEAT
                                    or (a.get("task_log_age") is not None and a["task_log_age"] < proto.FRESH_LOG))
        if ast == "done" and held:
            pass  # 做完的标志有了、worker 还领着：训练命令还在写结果，只续约，交还以后再拉回（11/3）；领取过期了（worker 没交还就没了）照常拉回
        elif ast == "done":
            ft = box_task(s, "relay_fetch", key, since)
            if ft is None:
                acts.append(dict(op="fetch", key=key))
            elif ft["status"] == "Success":
                end = C.local_time(ft.get("end"))
                if end is not None and now - end.timestamp() > LATE_S:
                    stuck[alert_id("relayfetch", key)] = alert(f"{L} 训完的 {key} 拉回并归档成功了（box 任务 {ft['id']}），box 上却还不算训完；"
                                                               f"看 box 上的结果文件夹", machine=name, step="late", task=ft["id"], status=ft["status"])
            elif ft["status"] not in LIVE:
                stuck[alert_id("relayfetch", key)] = alert(f"从 {L} 拉回 {key} 的 box 任务 {ft['id']} 出错了（{ft['status']}），领取保留；"
                                                           f"看 pueue log {ft['id']}，处理后 pueue remove {ft['id']}（下次检查重排）",
                                                           machine=name, step="fetch", task=ft["id"], status=ft["status"])
        elif ast == "stopped":
            acts.append(dict(op="rresume", key=key, rkey=a["key"]))
        elif ast == "input":  # 那边开训前查到输入不全（taskq.py missing）：只补传缺的、重建视图，再撤掉标记（11/1）
            sg = box_task(s, "relay_stage", key, a.get("input_since") or since)
            if sg is None or sg["status"] == "Success" or (sg["status"] not in LIVE and sg.get("code") == NOSLOT_RC):
                acts.append(dict(op="restage", key=key))
            elif sg["status"] not in LIVE:
                stuck[alert_id("relaystage", key)] = alert(f"给 {L} 补传 {key} 缺的输入的 box 任务 {sg['id']} 出错了（{sg['status']}），领取保留；"
                                                           f"看 pueue log {sg['id']}，处理后 pueue remove {sg['id']}（下次检查重传）",
                                                           machine=name, step="restage", task=sg["id"], status=sg["status"])
        elif ast == "failed":
            if progress(tasks.get(a["key"])):
                stuck[alert_id("site", name, "fail", key)] = alert(
                    f"{L} 上 {key}（{a['key']}）训练失败 {a.get('failures')} 次，有进度，领取保留；"
                    f"看那边的 {proto.TASK_LOGS}/；放弃：taskq.py release --worker {name} {key}",
                    state="failed", progress=True, failures=a.get("failures"))
            else:
                acts[-1] = dict(op="release", key=key, skip=f"{L} 上训练失败", why=f"{L} 上训练失败、没有进度，交还，{L} 不再领它")
                acts.append(dict(op="drop", key=key, rkey=a["key"]))
        elif ast in ("bad", "blocked"):
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
        elif m is None or m["state"] == "done":
            acts.append(dict(op="drop", key=real, rkey=a["key"]))
        elif m.get("worker") and m["worker"] != name:  # 改分给了别的执行机（那边停着的也算，11/2）：结果留给它收编
            acts.append(dict(op="unq", key=real, rkey=a["key"], to=m["worker"]))
        elif a["state"] != "stopped":
            acts.append(dict(op="rstop", key=real, rkey=a["key"]))
    for real, k in keys.items():  # 那边队列里已经删了（unq）、只剩结果的任务：总队列里训完了再删结果
        m = mt.get(real)
        if real not in mk and real not in rt and (k.get("dropped") or k.get("unq") and (m is None or m["state"] == "done")):
            acts.append(dict(op="drop", key=real, rkey=real, gone=True))
    acts += [dict(op="unhold", key=k) for k in holds if k not in gated]
    if rs["taskq"].get("paused"):
        stuck[alert_id("site", name, "fail")] = alert(f"{L} 的任务队列 3 小时内失败 {rs['taskq'].get('recent_failures')} 次，暂停领任务",
                                                      paused=True, failures=rs["taskq"].get("recent_failures"))
    return acts, stuck


def fill(s, name, rs, st, acts, stuck=None, notes=None):
    """那边备好、没人领的任务不够 READY 个时领下一个任务（每种 worker 各备 READY 个，envs/machines.py 的 kinds）→ take 动作；
    不领 → ([], 原因)。冒烟闸门（smoke_gate.py）挡下的不领，要排的冒烟是 smoke_add 动作；报警记进 stuck，开关关着时的影子记进 notes。"""
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    if not mtq or not mtq.get("enabled") or mtq.get("paused"):
        return [], "总队列关着或暂停中"
    if not shift.allowed(name):
        return [], "不在时段里（envs/policy.py 的 SHIFT）"
    if "error" in (s.get("pueue", {}).get(M.hub()) or {"error": 1}):  # 传数据要在 box 的 pueue 排，在跑的看不见（11/3）
        return [], "box 的 pueue 读不到"
    want, kinds = R.ready(name), M.kinds(name)
    keys = ST.keys_of(st, name)
    rt = {t["key"]: t for t in rs["taskq"]["tasks"]}

    def remote(t):
        return rt.get(t["key"]) if t["key"] in keys else None

    def waiting(t):  # 领着、还没开训：在传数据，在那边排着，或那边停着、这次检查要撤销停止的（11/2）
        a = remote(t)
        return a is None or a["state"] in ("free", "stale", "waiting", "stopped", "input")

    def idle(t):  # 那边备好了、没人领、没有进度：可以让给更优先的任务（11/2；有进度的不让，11/4）
        a = remote(t)
        return a is not None and a["state"] == "free" and not a.get("worker") and not progress((rs.get("tasks") or {}).get(t["key"]))

    def kind(t):
        return refill.info(mtq, t["key"])["spec"].get("device", "gpu")
    have, held = {k: 0 for k in kinds}, []
    for t in mtq.get("tasks", []):
        if t.get("worker") == name and t["state"] not in ("done", "stopped") and waiting(t):
            have[kind(t)] = have.get(kind(t), 0) + 1
            held.append(t)
    ready = {p for k in keys.values() for p in k.get("data") or []}
    cands = [t for t in refill.candidates(s, name, mtq, ready, rs.get("envs_ok"), kinds=kinds)
             if relayable(refill.info(mtq, t["key"]))]
    sg = SG.Gate(name, rs, mtq)
    for t in cands:  # 冒烟闸门（smoke_gate.py）：每种没过的排冒烟、报警；领的时候再按这一轮已经领的核对一次（canary 一次只送一个）
        sg.verdict(t["key"], refill.info(mtq, t["key"])["spec"])

    def blocked(key):
        return gate(name, rs, mtq, key) or sg.verdict(key, refill.info(mtq, key)["spec"])

    def finish(out, why):
        if stuck is not None:
            stuck.update(sg.alerts)
        if notes is not None:
            notes.extend(sg.notes)
        return out + sg.ops(), why
    urgent = [t for t in cands if t["nice"] < 0]  # 第 ⓪ 档（训完后的推理等）不受 READY 限制（11/1）
    room = {k: want - have[k] for k in kinds}
    same = rs.get("blob_dev", rs.get("dev")) == rs.get("dev")
    disk = (RM.conf(name)["ssh"], rs.get("blob_dev", rs.get("dev")))
    free = rs.get("blob_free", rs.get("free", 0)) / 1e9 - PLANNED.get(disk, 0)
    hard = (RM.conf(name).get("budget") or (None, HARD_GB))[1]
    # 有 cap 的（home）：那边每个任务（排着、在训、训完没拉回的，加这次领的）的输出各留 TRAIN_OUT_GB（11/4）
    spare = None
    if RM.conf(name).get("cap") and rs.get("out_used") is not None:
        n = len(rt) + sum(1 for t in held if remote(t) is None)
        spare = (rs.get("free", 0) + rs["out_used"]) / 1e9 - n * refill.TRAIN_OUT_GB - (PLANNED.get(disk, 0) if same else 0)
    out, took = [], set()
    for k in kinds:  # 备满了、总队列里有更优先的：每次检查把一个最靠后、备好没人领的交还，换它进来（11/2）
        idle_k = sorted((t for t in held if kind(t) == k and idle(t)), key=lambda t: (-t["nice"], -t["order"]))
        better = [t for t in cands if kind(t) == k and t["nice"] >= 0 and idle_k and t["nice"] < idle_k[0]["nice"]]
        pick = refill.next_pick(mtq, better, ready, free, hard, took)[0] if room[k] <= 0 and better else None
        if pick is not None and not blocked(pick["key"]):
            w = idle_k[0]
            out += [dict(op="drop", key=w["key"], rkey=w["key"]),
                    dict(op="release", key=w["key"], why=f"让给更优先的 {better[0]['key']}（档位 {better[0]['nice']} < {w['nice']}）")]
            room[k] = 1
            if spare is not None:
                spare += refill.TRAIN_OUT_GB
    if all(n <= 0 for n in room.values()) and not urgent:
        return finish(out, f"那边排着 {sum(have.values())} 个")
    skip = set()  # 那边在用的代码包跑不了说明的、冒烟闸门挡下的：这次不领
    for pool, n in [(urgent, len(urgent))] + [([t for t in cands if kind(t) == k], room[k]) for k in kinds]:
        for _ in range(n):
            while True:
                pool = [x for x in pool if x["key"] not in skip]
                t, need, gb, _ = refill.next_pick(mtq, pool, ready, free, hard, took)
                if t is None or not blocked(t["key"]):
                    break
                skip.add(t["key"])
            if t is None or spare is not None and spare - refill.TRAIN_OUT_GB - (gb if same else 0) < 0:
                break
            if spare is not None:
                spare -= refill.TRAIN_OUT_GB + (gb if same else 0)
            free -= gb
            PLANNED[disk] = PLANNED.get(disk, 0) + gb
            took.add(t["key"])
            sg.took(t["key"], refill.info(mtq, t["key"])["spec"])
            out.append(dict(op="take", key=t["key"], nice=t["nice"]))
    if out:
        return finish(out, "")
    open_ = urgent or any(room[k] > 0 and kind(t) == k for t in cands for k in kinds)
    if cands and open_ and spare is not None and spare < refill.TRAIN_OUT_GB:
        return finish(out, (f"放不下（那边 {len(rt)} 个任务的输出各留 {refill.TRAIN_OUT_GB} GB 后，"
                            f"cap 里只剩 {max(spare, 0):.0f} GB）"))
    if skip:
        return finish(out, f"那边在用的代码包跑不了、或冒烟闸门挡下 {len(skip)} 个任务（{'、'.join(sorted(skip)[:3])}）")
    return finish(out, ("队列里没有能领的任务" if not cands else f"放不下（那边剩 {free + PLANNED.get(disk, 0):.0f} GB）" if open_
                        else f"那边排着 {sum(have.values())} 个"))


def kname(kind):
    return "" if kind == "gpu" else f"{kind.upper()} "


def wname(kind):
    return f"{kname(kind)}worker"


def kkey(kind, *args, wkind="gpu"):
    """问题编号：GPU 的照旧，别的种类后面加 :<种类>。"""
    return alert_id(kind, *args, None if wkind == "gpu" else wkind)


def sub_kind(x):
    """topup 返回的一项 → 种类。"""
    return x[1] if x[0] == "pueue" else "cpu" if x[3] == refill.CPU_FILE else "gpu"


def topup(name, rs, run=None):
    """→ 要开的 worker, 不补的原因。"要几个"只在这里算，适配器只读状态、照着开。按这台能跑的种类
    （envs/machines.py 的 kinds）各算各的：
    pueue 类：这种 worker 没有活着的、上一个没失败、那边有备好没人领的这种任务时开一个 → ("pueue", 种类, 1)；
    Slurm 类：envs/policy.py 的 SLURM 里这台能跑的种类，按 refill.slurm_plan 交 → (QOS, 时限, 交几个, sbatch 文件)。
    run：fn(名字, 命令) → (退出码, 输出, 错误)，测试替换。"""
    if not shift.allowed(name):
        return [], "不在时段里（envs/policy.py 的 SHIFT）"
    if RM.conf(name)["scheduler"] == "pueue":
        if rs.get("jobs") is None:
            return [], "读不到 pueue"
        subs, why = [], []
        for k in M.kinds(name):
            mine = PQ.workers(name, rs["jobs"], k)
            if any(j[2] != "Done" for j in mine):
                why.append(f"{wname(k)} 在")
            elif mine and mine[-1][3] not in ("Success", "Killed"):
                why.append(f"上一个 {wname(k)}（pueue 任务 {mine[-1][0]}）失败了（{mine[-1][3]}）")
            elif not refill.unclaimed(rs["taskq"], k):
                why.append(f"那边没有备好、没人领的{' ' + kname(k) if kname(k) else ''}任务")  # worker 空闲 30 分钟自己退出，有了再开
            else:
                subs.append(("pueue", k, 1))
        return subs, "；".join(why)
    conf_ = {k: v for k, v in (P.SLURM.get(name) or {}).items() if k in M.kinds(name)}
    if not conf_:
        return [], "envs/policy.py 的 SLURM 里没有它"
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
    """做动作 → [说明]。出错的那一步写进说明，后面的照做。在 box 上加任务（传数据、拉回、归档）出错的记进 stuck，
    下次检查照常重试，加上了报警就消失。"""
    L, msgs = RM.conf(name)["label"], []
    stuck = {} if stuck is None else stuck
    raw_add = run_add or refill.add
    what = dict(relay_stage="传数据", relay_fetch="拉回", relay_archive="归档")

    def add(host, lab, step, k, args, **kw):
        try:
            return raw_add(host, lab, step, k, args, **kw)
        except Exception as e:  # noqa: BLE001
            stuck[alert_id("relayadd", name, step, k)] = alert(f"{L} 的 {k}：在 box 上排{what[step]}的任务失败（{str(e)[-200:]}），下次检查重试",
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
            msgs.append(f"{L} 的领取续不上（{(out + err)[-150:]}）")
    kept = set()  # 那边没删掉的：总队列里不交还（交还了别处会再领，两处一起训）
    for a in acts:
        k = a["key"]
        if k in lost:
            continue
        try:
            if a["op"] == "smoke_add":  # 加进总队列，接着领过来（take → stage）
                ok, m = SG.enqueue(name, a)
                msgs.append(m)
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
                msgs.append(f"{L} 的冒烟 {k} 从总队列里去掉了" if r[0] in (0, proto.EXIT["gone"]) else f"{L} 的冒烟 {k} 去不掉：{r[1][-150:]}")
            elif a["op"] == "smoke_real":
                if SG.real_done(name, a["mark"]):
                    msgs.append(f"{L} 上 {a['mark']['kind']} 的真任务 {k} 训完了，记成过了冒烟")
            elif a["op"] == "release":
                r = master(f"release --worker {name} {shlex.quote(k)}" + (f" --skip {shlex.quote(a['skip'])}" if a.get("skip") else ""))
                msgs.append(f"{L} 交还 {k}：{a['why']}" if r and r[0] == 0 else f"{L} 交还 {k} 出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
            elif a["op"] == "take":
                r = master(f"take --worker {name} --assign {shlex.quote(k)}")
                if r is None or r[0]:
                    msgs.append(f"{L} 没领到 {k}：{(r or [0, 'ssh 不通'])[1].strip()[-150:]}")
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
                tid = add(M.hub(), lab, "relay_stage", k, f"wait -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group="wait")
                msgs.append(f"{L} 的 {k} 排了{'补传缺的输入' if a['op'] == 'restage' else '传数据'}（box 任务 {tid}）")
            elif a["op"] == "fetch":
                lab = label("relay_fetch", k)
                cmd = f"python3 {shlex.quote(cfg.local('queue', 'relay.py'))} fetch {name} {shlex.quote(k)}"
                tid = add(M.hub(), lab, "relay_fetch", k, f"wait -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group="wait")
                msgs.append(f"{L} 训完了 {k}，排了拉回（box 任务 {tid}）")
            elif a["op"] == "archive":
                lab = label("relay_archive", k)
                cmd = f"bash {shlex.quote(cfg.local('queue', 'archive_task.sh'))} {M.hub()} {shlex.quote(k)}"
                tid = add(M.hub(), lab, "relay_archive", k, f"wait -p --label {shlex.quote(lab)} -- " + shlex.quote(refill.then_check(cmd, lab)),
                          to=name, group="wait")
                msgs.append(f"{L} 训完的 {k} 排了归档（box 任务 {tid}）")
            elif a["op"] == "unq":
                r = RM.rtq(name, f"remove {shlex.quote(a['rkey'])}")
                if r and r[0] in (0, proto.EXIT["gone"]):  # gone：那边已经没有了
                    ST.update_state(lambda st: ((st.get("keys") or {}).get(name, {}).get(k) or {}).update(unq=time.time()))
                    msgs.append(f"{L} 的队列里删了 {k}（总队列里分给了 {a['to']}，结果留着给它收编）")
                else:
                    msgs.append(f"{L} 的队列里删 {k} 出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
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
                msgs.append(f"{L} 上 {k} 改到档位 {a['nice']}" if r and r[0] == 0 else f"{L} 上 {k} 改档位出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
            elif a["op"] in ("rstop", "rresume"):
                r = RM.rtq(name, ("stop --why 总队列里停了 " if a["op"] == "rstop" else "resume ") + shlex.quote(a["rkey"]))
                what = "停" if a["op"] == "rstop" else "撤销停止"
                msgs.append(f"{L} 上{what} {k}" if r and r[0] == 0 else f"{L} 上{what} {k} 出错：{(r or [0, 'ssh 不通'])[1][-150:]}")
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
        for _, k, n in subs:
            for _ in range(n):
                ok, out = PQ.start_worker(name, run, k)
                if ok:
                    msgs.append(f"{L} 开了 {wname(k)}（{out[-80:]}）")
                else:
                    stuck[kkey("site", name, "worker", wkind=k)] = alert(f"{L} 开 {wname(k)} 出错：{out}", kind=k, start=True)
        return msgs, stuck
    cwd = f"{R.q(RM.root(name))}/{ROOT['current']}/{cfg.bundle_rel('queue')}"
    for r in SL.start_workers(cwd, subs, run, RM.conf(name).get("job_names")):
        k = sub_kind((r["qos"], r["time"], r["n"], r["file"]))
        if r["err"] is not None:
            stuck[kkey("topup", name, r["qos"], wkind=k)] = alert(f"{L} {r['qos']} 补交 {wname(k)} 作业出错：{r['err']}", qos=r["qos"], kind=k)
        if r["ids"]:
            msgs.append(f"{L} {r['qos']} 交了 {len(r['ids'])} 个 {wname(k)} 作业 {'、'.join(r['ids'])}（每个 {r['time']}）")
    return msgs, stuck


def site_view(name, rs, st):
    """那边的状态 → {真 key: dict(state, worker, runs, final, resumable, debug)}（status.json 的 tasks；shift.py 交班时现读）。"""
    rt = {t["key"]: t for t in (rs.get("taskq") or {}).get("tasks") or []}
    out = {}
    for real in ST.keys_of(st, name):
        a = rt.get(real)
        if a:
            f = (rs.get("tasks") or {}).get(real) or {}
            out[real] = dict(state=a["state"], worker=a.get("worker"), runs=f.get("runs") or [], final=f.get("final"),
                             resumable=f.get("resumable"), debug=f.get("debug") or {})
    return out


def write_status(name, rs, st):
    out = site_view(name, rs, st)
    try:
        allst = json.load(open(STATUS))
    except (OSError, ValueError):
        allst = {}
    allst[name] = dict(time=time.time(), label=RM.conf(name)["label"], tasks=out, squeue=rs.get("squeue"), free=rs.get("free"),
                       bundle=rs.get("bundle"), held=BG.held(st, name), envs_ok=rs.get("envs_ok"))
    write_json(STATUS, allst)


def log(lines):
    if lines:
        with open(LOG, "a") as fo:
            fo.writelines(f"{time.strftime('%F %T')} {x}\n" for x in lines)


def run_all(s, dry=False):
    """health.py 调用 → (说明, 卡住的 {问题编号: 说明})。"""
    events, stuck = [], {}
    PLANNED.clear()
    for name in M.RELAY:
        if name in P.OFFLINE:
            continue
        try:
            ev, st_ = run_one(s, name, dry)
        except Exception as e:  # noqa: BLE001
            ev, st_ = [], {alert_id("relay", name): f"queue/relay.py（{name}）出错：{type(e).__name__}: {e}"[:300]}
        events += ev
        stuck.update(st_)
    return events, stuck


def run_one(s, name, dry):
    live = bool(P.AUTO.get(name))
    mtq = s.get("taskq", {}).get(taskq.queue_host())
    mine = [t for t in (mtq or {}).get("tasks", []) if t.get("worker") == name]
    L, st = RM.conf(name)["label"], ST.load_state()
    if not mine and not live and not dry:
        return [], {}
    if not s["ok"].get(M.hub()) or not mtq:
        return [], {}
    rs = remote_status(name)
    notes = []
    # 读不到状态、但连得上：还没有代码包（新机器），或那边的旧包跑不了这边新改的状态程序（11/4）→ 包不同时先部署，再读一次
    cur = current_bundle(name) if rs is None and not dry and (live or mine) else None
    ph = {"bundle": cur or None} if cur is not None else None
    if ph is not None:
        rs = ph
    if not dry and (live or mine) and rs is not None:
        try:
            commit = subprocess.run(["git", "-C", REPO, "rev-parse", BUNDLE_REF], capture_output=True, text=True).stdout.strip()
            core = subprocess.run(["git", "-C", cfg.CORE, "rev-parse", RB.CORE_REF], capture_output=True, text=True).stdout.strip()
            key = f"{name}:{commit}:{core}"  # 核心有自己的仓库（11/4）：两边哪个换了提交都重新打包
            if cur == "" or rs.get("bundle") != st.get("bundle_for", {}).get(key):  # 每台的包不同（机器条目、开关）
                bid, path = RB.build(name, commit or "origin/main", core=core or RB.CORE_REF)
                ST.update_state(lambda x: x.setdefault("bundle_for", {}).update({key: bid}))
                if rs.get("bundle") != bid:
                    try:
                        deploy(name, bid, path)
                        notes.append(f"{L} 的代码换成 {bid}")
                        rs = remote_status(name)
                    except RuntimeError as e:
                        notes.append(str(e))
                        rs = None  # 那边还是旧包，和这边的命令、路径可能对不上：这次当连不上，只续约，下次再推（11/3）
        except Exception as e:  # noqa: BLE001
            notes.append(f"{L} 打包或推代码包出错：{e}")
            rs = None  # 同上：超时、打包出错时那边也还是旧包
    if ph is not None and rs is ph:  # 没有部署（包相同）或部署后仍读不到
        rs = None
    if rs is not None and not dry:
        ST.update_state(lambda x: x.setdefault("last_ok", {}).update({name: time.time()}))
        write_status(name, rs, ST.load_state())
    if rs is not None and not dry and live and RM.conf(name).get("budget"):
        try:
            st = ST.load_state()
            reg = (st.get("views") or {}).get(name) or {}
            keep = {v for k in ST.keys_of(st, name).values() for v in (k.get("outs") or [] if k.get("unq") else ST.paths_of(k))}
            keep |= {v for v, t in reg.items() if time.time() - t < VIEW_KEEP_S}
            g = ST.gc(name, keep, RM.rpy, views=reg, siblings=[x["path"] for x in M.adopt_sources(name) if x["site"]],
                      free=rs.get("free") if RM.conf(name).get("cap") else None)
            if g and g["removed_views"]:
                ST.update_state(lambda x: [((x.get("views") or {}).get(name) or {}).pop(v, None) for v in g["removed_views"]])
            if g and (g["removed_views"] or g["removed_blobs"]):
                notes.append(f"{L} 清理：删了 {len(g['removed_views'])} 个数据集视图、{g['removed_blobs']} 个 blob，"
                             f"放出 {g['freed'] / 1e9:.0f} GB，剩 {g['free'] / 1e9:.0f} GB")
                rs = remote_status(name) or rs
        except Exception as e:  # noqa: BLE001
            notes.append(f"{L} 清理出错：{e}")
    acts, stuck = plan(s, name, rs, st)
    if not live:  # 开关关着：不传、不重传、不让那边接着训；训完的照常拉回、交还
        acts = [a for a in acts if a["op"] not in ("stage", "restage", "rresume")]
    snotes = []
    more, why = (fill(s, name, rs, st, acts, stuck, snotes) if rs is not None else ([], "连不上"))
    if snotes and not dry and C.changed(f"smoke_{name}", snotes):
        log(snotes)
    subs, swhy = (topup(name, rs) if rs is not None else ([], "连不上"))
    for part in swhy.split("；"):
        k = next((k for k in M.kinds(name) if part.startswith(f"上一个 {wname(k)}（")), None)
        if k:
            stuck[kkey("site", name, "worker", wkind=k)] = alert(f"{L} {part}；看那边的 pueue log，处理后 pueue remove 那个任务（下次检查重开）",
                                                                 kind=k, start=False)
    todo = [a for a in acts if a["op"] != "beat"]
    desc = "；".join(f"{a['op']} {a['key']}" for a in todo + more)
    sdesc = "；".join(f"交 {x[0]} {x[2]} 个 {wname(sub_kind(x))}" for x in subs)
    if dry:
        return [f"{L}：会 {desc}" if desc else f"{L}：不动（{why}）",
                f"{L}：会 {sdesc}" if sdesc else f"{L}：不交作业（{swhy}）"] + notes, stuck
    msgs = notes + execute(name, acts, s, stuck=stuck)
    if live:
        msgs += execute(name, more, s, stuck=stuck)
        m2, st2 = submit(name, subs)
        msgs += m2
        stuck.update(st2)
    elif (more or subs) and C.changed(f"relay_{name}", desc + sdesc):
        log([f"（开关没开）{L} 会：{'；'.join(x for x in (desc, sdesc) if x)}"])
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
    p = sub.add_parser("fetch", help="拉回做完的任务、核对，放进 box 的数据目录")
    p.add_argument("name")
    p.add_argument("key")
    p = sub.add_parser("bundle", help="打包、推过去、切换 current")
    p.add_argument("name")
    p.add_argument("--commit", default="origin/main")
    p = sub.add_parser("status", help="那边的队列")
    p.add_argument("name")
    p = sub.add_parser("resend", help="传过去的任务按现在的做法重新解析，换掉那边队列里的说明")
    p.add_argument("name")
    a = ap.parse_args(argv)
    if a.cmd and a.name not in M.RELAY:
        print(f"{ap.prog} {a.cmd}：没有执行机 {a.name}（有：{'、'.join(M.RELAY) or '无'}）", file=sys.stderr)
        return 2 if a.cmd == "stage" else proto.EXIT["gone"]  # stage 的退出码冻结
    if a.cmd in ("stage", "fetch"):
        if a.cmd == "stage" and a.nice is None and not a.no_add:
            ap.error("要给 --nice（或 --no-add）")
        try:
            if a.cmd == "fetch":
                RT.fetch(a.name, a.key)
            elif RT.stage(a.name, a.key, a.nice, add=not a.no_add) is None:
                return NOSLOT_RC
        except (RN.RelayError, S.SpecError, RuntimeError, OSError, subprocess.SubprocessError) as e:
            if a.cmd == "stage":  # 输出、退出码冻结
                print(f"relay.py {a.cmd} {a.name} {a.key}：{type(e).__name__}: {e}", file=sys.stderr)
                return 1
            print(f"{ap.prog} {a.cmd} {a.name}：{e}", file=sys.stderr)
            return proto.EXIT["gone"] if isinstance(e, RT.Gone) else 1
    elif a.cmd == "bundle":
        rs = remote_status(a.name)
        print(bundle(a.name, a.commit, (rs or {}).get("bundle")) or "没变")
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
