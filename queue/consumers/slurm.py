"""Worker in a Slurm job: the job id is the worker name, slots come from the CPUs and memory allocated, plus time limit, node-local staging dir, and exit after IDLE_MAX seconds idle."""
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
    w.term_rc, w.term_why = 143, "likely the time limit"
    w.end_text = "not known yet, checking later"
    w.get_end = end_time
    w.stage_root = os.path.join(e.get("TMPDIR") or "/tmp", proto.STAGE_DIR_PREFIX + w.W)
    w.said = set()
    w.idle_mark = None


def end_time(w):
    """Job end time (epoch seconds); unknown → 0, checked again each round."""
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
    """Nothing running: None when it is time to end, else how long to wait before checking again."""
    if w.idle_mark != since:
        w.idle_mark, w.said = since, set()
    m = lambda s: int(s // 60)  # noqa: E731
    if w.own >= w.MAX_OWN_FAIL:
        w.say("too many failures on this job; ending the job")
    elif w.left() <= w.MIN_LEFT:
        w.say(f"less than {m(w.MIN_LEFT)} minutes left; ending the job")
    elif why == "nothing" and w.device == "cpu":
        w.say("nothing running and no claimable tasks; ending the job")
    elif why == "nothing" and now - since >= w.IDLE_MAX:
        w.say(f"waited {m(now - since)} minutes with no claimable tasks; ending the job and giving the GPU back")
    elif why == "error" and now - err_since >= w.ERR_MAX:
        w.say(f"queue kept failing for {m(now - err_since)} minutes ({w.err}); ending the job and giving the GPU back")
    elif now - since >= w.IDLE_MAX:
        w.say(f"nothing running for {m(now - since)} minutes ({w.blocked if why == 'admit' else why or ''}); ending the job and giving the GPU back (some clusters cancel jobs whose GPU utilization stays low)")
    else:
        msg = {"nothing": f"no claimable tasks; waiting, checking every {w.POLL} seconds, at most {m(w.IDLE_MAX)} minutes",
               "paused": f"the queue is paused (too many failures within 3 hours); waiting, at most {m(w.IDLE_MAX)} minutes",
               "wait": f"a task ahead just stopped; waiting for it (at most {m(w.PATIENCE)} minutes after start)"}.get(why)
        if msg and why not in w.said:
            w.say(msg)
            w.said.add(why)
        return w.POLL
    return None


def on_exit(w):
    shutil.rmtree(w.stage_root, ignore_errors=True)
