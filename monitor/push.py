#!/usr/bin/env python3
"""推送到手机（ntfy）上配置好的频道；每条推送也记成一条事件（source notify）。

    taskq notify [--title T] [--priority P] [--tags X] [--kind K] [--data JSON] <文字…>   （不给文字时从标准输入读）

频道名：放在 policy.py OUTPUTS["topic"] 指的文件里，默认 ~/.config/taskq/ntfy_topic（不进仓库）。
标题前缀：OUTPUTS["name"]（默认 taskq）。
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
    q = urllib.parse.urlencode({"title": title or f"{name()} 作业", "priority": priority, "tags": tags})
    req = urllib.request.Request(f"https://ntfy.sh/{topic}?{q}", data=msg.encode(), method="POST")
    urllib.request.urlopen(req, timeout=30).read()


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq notify", description="推送一条消息到手机（ntfy），并记进事件日志")
    ap.add_argument("text", nargs="*", help="消息（不给时从标准输入读）")
    ap.add_argument("--title", help="默认 '<name> 作业'")
    ap.add_argument("--priority", default="default", help="ntfy 的优先级：min、low、default、high、urgent")
    ap.add_argument("--tags", default="", help="ntfy 的标签，逗号分隔")
    ap.add_argument("--kind", help="记成哪种事件（kind；默认不填）")
    ap.add_argument("--data", help="和事件一起存的 JSON 对象（例如下一次报告要拿来比的数）")
    a = ap.parse_args(argv)
    text = " ".join(a.text).strip()
    if not a.text and not sys.stdin.isatty():
        text = sys.stdin.read().strip()
    if not text:
        print("taskq notify: 没有文字", file=sys.stderr)
        return 2
    try:
        extra = json.loads(a.data) if a.data else {}
    except ValueError as e:
        print(f"taskq notify: --data: {e}", file=sys.stderr)
        return 2
    try:
        push(text, title=a.title, priority=a.priority, tags=a.tags)
    except OSError as e:
        print(f"taskq notify: 推送失败：{e}", file=sys.stderr)
        return 1
    import events
    events.add("notify", text, kind=a.kind, data=dict(extra, title=a.title or f"{name()} 作业", priority=a.priority))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
