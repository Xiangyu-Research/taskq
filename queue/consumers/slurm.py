"""Slurm 作业里的 worker：作业号当 worker 名，按分到的 CPU 和内存定同时训几个，时限，本地暂存目录，空闲 IDLE_MAX 秒退出。"""
import os
import re
import shutil
import subprocess
import time

import proto


def active(w):
    return bool(os.environ.get("SLURM_JOB_ID"))


def setup(w):
    e = os.environ
    w.W = e["SLURM_JOB_ID"]
    w.mem_mb = int(e.get("SLURM_MEM_PER_NODE") or 0) or int(e.get("SLURM_MEM_PER_CPU") or 0) * w.ncpu
    big = w.ncpu >= 24 and (w.mem_mb == 0 or w.mem_mb >= 56 * 1024)
    w.slots = int(e.get("TASKQ_SLOTS") or (2 if big else 1))
    w.term_rc, w.term_why = 143, "多半是到时限"
    w.end_text = "查不到，之后再查"
    w.get_end = end_time
    w.stage_root = os.path.join(e.get("TMPDIR") or "/tmp", proto.STAGE_PREFIX + w.W)
    w.said = set()
    w.idle_mark = None


def end_time(w):
    """作业的结束时刻（epoch 秒）；查不到 → 0，之后每轮再查。"""
    e = os.environ
    ids = [w.W] + ([f"{e['SLURM_ARRAY_JOB_ID']}_{e.get('SLURM_ARRAY_TASK_ID', '')}"] if e.get("SLURM_ARRAY_JOB_ID") else [])
    for i in ids:
        try:
            out = subprocess.run(["squeue", "-j", i, "-h", "-o", "%e"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                 text=True, timeout=60).stdout
        except (OSError, subprocess.SubprocessError):
            continue
        s = (out.split("\n") or [""])[0].strip()
        if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d", s):
            continue
        t = time.mktime(time.strptime(s, "%Y-%m-%dT%H:%M:%S"))
        if t > w.t0:
            return int(t)
    return 0


def env(w, env):
    env["TASKQ_STAGE_ROOT"] = w.stage_root


def idle(w, why, now, since, err_since):
    """手上没有在训的：到了该结束的时候 → None，否则 → 隔多久再看。"""
    if w.idle_mark != since:
        w.idle_mark, w.said = since, set()
    m = lambda s: int(s // 60)  # noqa: E731
    if w.own >= w.MAX_OWN_FAIL:
        w.say("本作业失败太多次，作业结束")
    elif w.left() <= w.MIN_LEFT:
        w.say(f"离时限不到 {m(w.MIN_LEFT)} 分钟，作业结束")
    elif why == "nothing" and w.device == "cpu":
        w.say("没有在训的，也没有能领的任务，作业结束")
    elif why == "nothing" and now - since >= w.IDLE_MAX:
        w.say(f"空等 {m(now - since)} 分钟没有能领的任务，作业结束、把卡还回去")
    elif why == "error" and now - err_since >= w.ERR_MAX:
        w.say(f"队列连续出错 {m(now - err_since)} 分钟（{w.err}），结束作业、把卡还回去")
    elif now - since >= w.IDLE_MAX:
        w.say(f"手上没有在训的已经 {m(now - since)} 分钟（{w.blocked if why == 'admit' else why or ''}），结束作业、把卡还回去（DICC：GPU 使用率连续 1 小时低于 40% 会被取消）")
    else:
        msg = {"nothing": f"没有能领的任务，空着等，每 {w.POLL} 秒再看，最多 {m(w.IDLE_MAX)} 分钟",
               "paused": f"队列暂停领任务（3 小时内失败太多次），空着等，最多 {m(w.IDLE_MAX)} 分钟",
               "wait": f"排在前面的任务刚停下，等它（最多到开始后 {m(w.PATIENCE)} 分钟）"}.get(why)
        if msg and why not in w.said:
            w.say(msg)
            w.said.add(why)
        return w.POLL
    return None


def on_exit(w):
    shutil.rmtree(w.stage_root, ignore_errors=True)
