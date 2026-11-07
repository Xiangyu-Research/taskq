"""常驻的 worker（pueue 组里，不在 Slurm 里）：WORKER_ID 当名字，最多用 TASKQ_CPUS 个核，同时训 TASKQ_SLOTS 个（机器表 worker 的 slots），
每次开训另起分段日志的作业名；领不到、出错、失败太多都不马上退出，手上没有在训的满 IDLE_EXIT 秒才退出（退出码 0）。"""
import os


def active(w):
    return not os.environ.get("SLURM_JOB_ID")


def setup(w):
    e = os.environ
    c = e.get("TASKQ_CPUS", "")
    if c.isdigit() and 0 < int(c) < w.ncpu:
        w.ncpu = int(c)
    s = e.get("TASKQ_SLOTS")
    w.slots = int(s) if str(s).isdigit() else 1
    cvd = e.get("CUDA_VISIBLE_DEVICES", "")
    if cvd.isdigit():
        w.nvsel = ["-i", cvd]
    w.tag_each = True
    w.restart_on_change = True
    w.said_idle = None
    w.idle_mark = None


def idle(w, why, now, since, err_since):
    if w.idle_mark != since:
        w.idle_mark, w.said_idle = since, None
    msg = "own" if w.own >= w.MAX_OWN_FAIL else why
    if w.zomb:
        msg = "zomb"
    if msg not in ("own", "zomb", "admit") and now - since >= w.IDLE_EXIT:
        w.say(f"手上没有在训的已经 {int((now - since) // 60)} 分钟，退出（有备好的任务时下次检查再开）")
        return None
    if msg != w.said_idle:
        text = {"zomb": f"还有停不下来的训练（{' '.join(r.key for r in w.zomb.values())}），等它停下（接着心跳），先不退出",
                "own": f"本 worker {w.FAIL_WINDOW // 60} 分钟内有 {w.own} 个任务开始不久就失败，先不领，过了时间窗再领",
                "nothing": f"没有能领的任务，每 {w.IDLE_POLL} 秒再看",
                "error": f"没有能领的任务，每 {w.IDLE_POLL} 秒再看",
                "paused": "队列暂停领任务（3 小时内失败太多次），空着等",
                "wait": f"排在前面的任务刚停下，等它（最多到开始后 {w.PATIENCE // 60} 分钟）"}.get(msg)
        if text:
            w.say(text)
        w.said_idle = msg
    return w.IDLE_POLL
