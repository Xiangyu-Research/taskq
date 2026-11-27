#!/usr/bin/env python3
"""Push to the channel configured for the phone (ntfy); each push is also logged as an event (source notify).

    taskq notify [--title T] [--priority P] [--tags X] [--kind K] [--data JSON] <text...>   (reads stdin when no text)

Channel name: in the file policy.py OUTPUTS["topic"] points to, default ~/.config/taskq/ntfy_topic (not in the repo).
Server: OUTPUTS["push_url"] (an ntfy server, default https://ntfy.sh; https only, except on this machine). Access token: in the
file OUTPUTS["push_token"] points to, sent as Authorization: Bearer when the file exists (a protected topic: only token holders
read or post). OUTPUTS["push_detail"] False: the message says only how many lines there are, never task names, paths or errors.
Title prefix: OUTPUTS["name"] (default taskq).
"""
import argparse
import json
import os
import sys
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import cfg  # noqa: E402

def _out(key):
    """OUTPUTS[key]; policy.py cannot be read -> the schema default (a broken config must still reach the phone)."""
    try:
        return cfg.outputs(key)
    except Exception:  # noqa: BLE001
        from schema import OUTPUT_FIELDS
        return OUTPUT_FIELDS[key][1]


def topic_file():
    return os.path.expanduser(_out("topic"))


def name():
    return _out("name")


def push(msg, title=None, priority="default", tags=""):
    topic = open(topic_file()).read().strip()
    base = str(_out("push_url")).rstrip("/")
    u = urllib.parse.urlparse(base)
    if u.scheme != "https" and not (u.scheme == "http" and u.hostname in ("localhost", "127.0.0.1", "::1")):
        raise OSError(f"push_url {base} is not https (the message and the topic would travel in the clear)")
    if not _out("push_detail"):
        n = msg.count("\n") + 1
        msg = f"{n} item{'s' if n > 1 else ''} need{'' if n > 1 else 's'} attention; details on the hub (taskq problems, taskq events)"
    q = urllib.parse.urlencode({"title": title or f"{name()} job", "priority": priority, "tags": tags})
    head = {}
    tf = os.path.expanduser(str(_out("push_token")))
    if os.path.isfile(tf):
        head["Authorization"] = "Bearer " + open(tf).read().strip()
    req = urllib.request.Request(f"{base}/{urllib.parse.quote(topic)}?{q}", data=msg.encode(), method="POST", headers=head)
    urllib.request.urlopen(req, timeout=30).read()


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq notify", description="Push a message to the phone (ntfy) and log it in the event log")
    ap.add_argument("text", nargs="*", help="message (reads stdin when omitted)")
    ap.add_argument("--title", help="default '<name> job'")
    ap.add_argument("--priority", default="default", help="ntfy priority: min, low, default, high, urgent")
    ap.add_argument("--tags", default="", help="ntfy tags, comma separated")
    ap.add_argument("--kind", help="event kind to log it as (optional)")
    ap.add_argument("--data", help="JSON object stored with the event (e.g. numbers the next report compares against)")
    a = ap.parse_intermixed_args(argv)  # text on both sides of options (plain parse_args before Python 3.13 rejects the part after)
    text = " ".join(a.text).strip()
    if not a.text and not sys.stdin.isatty():
        text = sys.stdin.read().strip()
    if not text:
        print("taskq notify: no text", file=sys.stderr)
        return 2
    try:
        extra = json.loads(a.data) if a.data else {}
    except ValueError as e:
        print(f"taskq notify: --data: {e}", file=sys.stderr)
        return 2
    import db as D
    if cfg.refuse_in_task("notify", D.path()):
        return 2
    try:
        push(text, title=a.title, priority=a.priority, tags=a.tags)
    except OSError as e:
        print(f"taskq notify: push failed: {e}", file=sys.stderr)
        return 1
    import events
    events.add("notify", text, kind=a.kind, data=dict(extra, title=a.title or f"{name()} job", priority=a.priority))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
