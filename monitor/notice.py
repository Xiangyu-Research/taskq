#!/usr/bin/env python3
"""项目挂在健康检查上的提醒（2026-11-05）：挂着的每一条，健康检查报一个问题 notice:<名字>（进"需要处理"、照常推送一次），
撤掉后问题自动消失。存在健康检查的状态文件夹下 notices/<名字>.txt，内容就是提醒的文字。

    taskq notice set <名字> [文字…]     挂上或改文字（不给文字时从标准输入读）
    taskq notice clear <名字>…          撤掉（没有的不算错）
    taskq notice list [--json]           挂着的；--json：一行 JSON（proto.NOTICE_TOP，每条 proto.NOTICE_FIELDS）
名字只能用字母、数字、_、-。
"""
import argparse
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "queue"))
import cfg  # noqa: E402
import proto  # noqa: E402


def notices_dir():
    """健康检查的状态文件夹（policy.py 的 OUTPUTS["health"]，没写时状态文件夹下的 health）下的 notices/。"""
    return os.path.join(os.path.expanduser(cfg.outputs("health") or cfg.state("health", expand=False)), proto.NOTICES)


def valid(name):
    return bool(re.fullmatch(proto.NOTICE_NAME, name or ""))


def read(d=None):
    """→ {名字: dict(text, since)}，按名字排好；文件夹不在 → {}。"""
    d = d or notices_dir()
    out = {}
    try:
        names = sorted(os.listdir(d))
    except FileNotFoundError:
        return out
    for f in names:
        name, ext = os.path.splitext(f)
        if ext != ".txt" or not valid(name):
            continue
        p = os.path.join(d, f)
        try:
            text = open(p, encoding="utf-8").read().strip()
            since = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(p)))
        except OSError:
            continue
        out[name] = dict(text=text or name, since=since)
    return out


def put(name, text, d=None):
    d = d or notices_dir()
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, name + ".txt")
    tmp = f"{p}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text.strip() + "\n")
    os.replace(tmp, p)


def drop(name, d=None):
    """→ 撤掉了没有。"""
    try:
        os.remove(os.path.join(d or notices_dir(), name + ".txt"))
        return True
    except FileNotFoundError:
        return False


def main(argv):
    ap = argparse.ArgumentParser(prog="taskq notice", description="挂在健康检查上的提醒")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("set", help="挂上或改文字")
    p.add_argument("name")
    p.add_argument("text", nargs="*", help="提醒的文字（不给时从标准输入读）")
    p = sub.add_parser("clear", help="撤掉")
    p.add_argument("names", nargs="+")
    p = sub.add_parser("list", help="挂着的")
    p.add_argument("--json", action="store_true", help="一行 JSON（proto.NOTICE_TOP、NOTICE_FIELDS）")
    a = ap.parse_args(argv)
    bad = [n for n in ([a.name] if a.cmd == "set" else a.names if a.cmd == "clear" else []) if not valid(n)]
    if bad:
        print(f"名字只能用字母、数字、_、-：{'、'.join(bad)}", file=sys.stderr)
        return 2
    if a.cmd == "set":
        text = " ".join(a.text).strip()
        if not a.text and not sys.stdin.isatty():
            text = sys.stdin.read().strip()
        if not text:
            print("要给提醒的文字（参数或标准输入）", file=sys.stderr)
            return 2
        put(a.name, text)
        print(f"挂上了 {a.name}：{text}")
        return 0
    if a.cmd == "clear":
        for n in a.names:
            print(f"撤掉了 {n}" if drop(n) else f"没有挂着 {n}")
        return 0
    got = read()
    if a.json:
        print(json.dumps(dict(version=proto.VERSION, notices=[dict(name=n, **v) for n, v in got.items()]), ensure_ascii=False))
        return 0
    for n, v in got.items():
        print(f"{n}（{v['since']}）：{v['text']}")
    if not got:
        print("没有挂着的提醒")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
