#!/usr/bin/env python3
"""Push to the channel configured for the phone (ntfy); each push is also logged as an event (source notify).

    taskq notify [--title T] [--priority P] [--tags X] [--kind K] [--data JSON] <text...>   (reads stdin when no text)

Channel name: in the file policy.py OUTPUTS["topic"] points to, default ~/.config/taskq/ntfy_topic (not in the repo).
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

def topic_file():
    return os.path.expanduser(cfg.outputs("topic"))


def name():
    return cfg.outputs("name")


def push(msg, title=None, priority="default", tags=""):
    topic = open(topic_file()).read().strip()
    q = urllib.parse.urlencode({"title": title or f"{name()} job", "priority": priority, "tags": tags})
    req = urllib.request.Request(f"https://ntfy.sh/{topic}?{q}", data=msg.encode(), method="POST")
    urllib.request.urlopen(req, timeout=30).read()


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq notify", description="Push a message to the phone (ntfy) and log it in the event log")
    ap.add_argument("text", nargs="*", help="message (reads stdin when omitted)")
    ap.add_argument("--title", help="default '<name> job'")
    ap.add_argument("--priority", default="default", help="ntfy priority: min, low, default, high, urgent")
    ap.add_argument("--tags", default="", help="ntfy tags, comma separated")
    ap.add_argument("--kind", help="event kind to log it as (optional)")
    ap.add_argument("--data", help="JSON object stored with the event (e.g. numbers the next report compares against)")
    a = ap.parse_args(argv)
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
