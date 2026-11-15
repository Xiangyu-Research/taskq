"""Where each task is, in plain words (proto.TASK_WORDS): the one rule taskq status, taskq status --all --json and the progress
snapshot each health round use.

    compute(queue, sites, stored, now) -> {key: dict(key, step, word, where, since, frac, eta, last, epochs, total)}

queue: hub queue tasks (taskq.local_status()["tasks"]); sites: what relay last read on each executor (hubstate.sites); stored:
keys whose results are on the store (events.stored). A task claimed for an executor gets its word from what that executor
reported: not there yet -> being sent (held when the send gate holds it), claimed there -> running (overdue when past its
estimated finish by proto.SUMMARY_OVERDUE), done there -> coming back, otherwise waiting there. Any other task's word follows its
hub queue state; not in the queue -> done when its results are on the store, otherwise not queued yet. Progress (frac, eta,
last, epochs, total) is spec.progress_of on the executor's record, in local time.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cfg  # noqa: E402
import proto as P  # noqa: E402
import spec as S  # noqa: E402

W = P.TASK_WORDS
OVERDUE, RUNNING, SENDING, HELD, WAITING, BACK = W["overdue"], W["running"], W["sending"], W["held"], W["waiting"], W["back"]
FAILED, MISSING, STOPPED, BAD, NOT_SENT, NOT_QUEUED, DONE = (W["failed"], W["missing"], W["stopped"], W["bad"], W["not_sent"],
                                                             W["not_queued"], W["done"])
ORDER = tuple(W.values())
# hub queue state -> word; claimed by a worker on the hub itself, or unclaimed but its log still changing (busy), it is running
HUB_WORDS = {P.S_FAILED: FAILED, P.S_INPUT: MISSING, P.S_STOPPED: STOPPED, P.S_BAD: BAD, P.S_DONE: DONE,
             P.S_FREE: NOT_SENT, P.S_STALE: NOT_SENT, P.S_BUSY: RUNNING, P.S_CLAIMED: RUNNING}


def compute(queue, sites, stored, now):
    M = cfg.mtab()
    qt = {t["key"]: t for t in queue or [] if not P.is_smoke(t["key"])}
    tasks = {}
    for t in S.all_tasks(list(qt.values())):
        k, sp = t["key"], t["spec"]
        x = qt.get(k)
        word = HUB_WORDS.get(x.get("state"), NOT_QUEUED) if x else DONE if k in stored else NOT_QUEUED
        tasks[k] = dict(key=k, step=sp.get("step") or sp.get("recipe") or "?", word=word,
                        where=(x.get("assigned") or x.get("host")) if word == RUNNING else None, since=None, frac=None, eta=None,
                        last=None, epochs=None, total=None)
    for k, x in qt.items():
        n = x.get("assigned")
        if x.get("state") != P.S_CLAIMED or k not in tasks or not M.is_exec(n):
            continue
        r = sites.get(n) if isinstance(sites.get(n), dict) else {}
        t = tasks[k]
        t["where"] = n
        f = (r.get("tasks") or {}).get(k)
        if f is None:
            t["word"] = HELD if k in (r.get("held") or {}) else SENDING
            continue
        st = f.get("state")
        if st == P.S_DONE:
            t["word"] = BACK
        elif st == P.S_CLAIMED:
            t["word"], t["since"] = RUNNING, f.get("since")
            pg = S.progress_of(f, M.tz_of(n))
            t.update(frac=pg["frac"], eta=pg["eta"], last=pg["last"] if not f.get("final") else None, epochs=pg["epochs"], total=pg["total"])
            if t["eta"] and t["eta"].timestamp() < now.timestamp() - P.SUMMARY_OVERDUE:
                t["word"] = OVERDUE
        else:
            t["word"] = WAITING
    return tasks
