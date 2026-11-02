"""测试用的进度和设置读法（2026-11-02）：形状同真的领域插件（日志里 "Epoch N" 开始一轮、": Epoch time:" 训完一轮；
设置在 debug.json）。做法里写 progress="toy_log"、info="toy_debug"。只用标准库。"""
import json
import os
import re

TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
EPOCH_DONE = ": Epoch time:"


def toy_log(path):
    """train_log_*.txt（一个文件是一次启动）→ 开始、最后更新、训完的轮数；tl：每训完 10 轮记一个 [时间, 已训完轮数]。"""
    start = end = None
    done = 0
    cur = None
    tl, last = [], None
    with open(path, errors="replace") as f:
        for line in f:
            m = TS.match(line)
            if m:
                start = start or m.group(1)
                end = m.group(1)
            e = re.search(r": Epoch (\d+)\s*$", line)
            if e:
                cur = int(e.group(1))
            elif cur is not None and EPOCH_DONE in line:
                done = max(done, cur + 1)
                if m and (cur + 1) % 10 == 0:
                    tl.append([m.group(1), cur + 1])
                last = [m.group(1), cur + 1] if m else None
    if done and (not tl or tl[-1][1] != done) and last:
        tl.append(last)
    return dict(start=start, end=end, epochs_done=done, tl=tl)


def toy_debug(fd):
    """debug.json → 登记表的参数列。"""
    try:
        d = json.load(open(os.path.join(fd, "debug.json")))
    except Exception:  # noqa: BLE001
        return {}
    return {"initial_lr": d.get("initial_lr"), "num_epochs": d.get("num_epochs"), "batch_size": d.get("batch_size"),
            "机器": d.get("hostname"), "GPU": d.get("gpu_name")}


PROGRESS = {"toy_log": toy_log}
INFO = {"toy_debug": toy_debug}
TRAINED = [EPOCH_DONE]
