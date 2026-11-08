"""Resident worker (in a pueue group, not in Slurm): WORKER_ID is the name, uses at most TASKQ_CPUS cores, runs TASKQ_SLOTS trainings at once (the machine table worker's slots), with a job name per start for the part log.
No claims, errors or too many failures do not exit at once; it exits 0 only after IDLE_EXIT seconds with nothing running."""
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
        w.say(f"nothing running for {int((now - since) // 60)} minutes; exiting (the next check starts it again when there are tasks ready)")
        return None
    if msg != w.said_idle:
        text = {"zomb": f"unstoppable training still running ({' '.join(r.key for r in w.zomb.values())}); waiting for it to stop (still heartbeating), not exiting yet",
                "own": f"this worker had {w.own} tasks fail shortly after starting within {w.FAIL_WINDOW // 60} minutes; not claiming until the window passes",
                "nothing": f"no claimable tasks; checking again every {w.IDLE_POLL} seconds",
                "error": f"no claimable tasks; checking again every {w.IDLE_POLL} seconds",
                "paused": "the queue is paused (too many failures within 3 hours); waiting",
                "wait": f"a task ahead just stopped; waiting for it (at most {w.PATIENCE // 60} minutes after start)"}.get(msg)
        if text:
            w.say(text)
        w.said_idle = msg
    return w.IDLE_POLL
