#!/usr/bin/env python3
"""Phone push (ntfy) on the configured channel; each push is also an event (source notify).

    taskq notify [--title T] [--priority P] [--tags X] [--kind K] [--data JSON] <text…>   (text from stdin when not given)

Channel name: the file at policy.py OUTPUTS["topic"], default ~/.config/taskq/ntfy_topic (kept out of the repo).
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

TOPIC_DEFAULT = "~/.config/taskq/ntfy_topic"


def topic_file():
    return os.path.expanduser(cfg.outputs("topic") or TOPIC_DEFAULT)


def name():
    return cfg.outputs("name") or "taskq"


def push(msg, title=None, priority="default", tags=""):
    topic = open(topic_file()).read().strip()
    q = urllib.parse.urlencode({"title": title or f"{name()} 作业", "priority": priority, "tags": tags})
    req = urllib.request.Request(f"https://ntfy.sh/{topic}?{q}", data=msg.encode(), method="POST")
    urllib.request.urlopen(req, timeout=30).read()


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq notify", description="Push a message to the phone (ntfy) and log it as an event")
    ap.add_argument("text", nargs="*", help="the message (read from stdin when not given)")
    ap.add_argument("--title", help="default: '<name> 作业'")
    ap.add_argument("--priority", default="default", help="ntfy priority: min, low, default, high, urgent")
    ap.add_argument("--tags", default="", help="ntfy tags, comma separated")
    ap.add_argument("--kind", help="event kind to log it under (default: none)")
    ap.add_argument("--data", help="a JSON object stored with the event (e.g. what the next report compares with)")
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
    events.add("notify", text, kind=a.kind, data=dict(extra, title=a.title or f"{name()} 作业", priority=a.priority))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
