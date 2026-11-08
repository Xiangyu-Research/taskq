"""核心的每个文件都不写领域词（具体的模型、训练器、数据），也不写使用者的路径、仓库；非测试文件不写日期（只写现在是什么）。"""
import os
import re
import subprocess
import sys

CORE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 调度代码的根
ROOT = CORE  # 清单里的路径相对它

# 整个核心（git 跟踪的和新加还没提交的文本文件）都查；只有这个测试自己写着这些词
CLEAN = sorted(f for f in subprocess.run(["git", "-C", CORE, "ls-files", "-co", "--exclude-standard"], capture_output=True, text=True,
                                         check=True).stdout.split()
               if f != "queue/test_core_clean.py" and os.path.isfile(os.path.join(CORE, f)) and not f.endswith((".png", ".pyc")))
# 后两组：使用者的运行记录格式（runlog 的 .run.json）、nnU-Net 结果文件夹的布局（<训练器>__<plans>__<配置>）
WORDS = re.compile(r"nnunet|nnU-?Net|BRAIN_TUMOR|\bfold|RQ[0-9]|Dataset[0-9]|trainer|research/|segment|lesion|tumou?r|\bMRI\b"
                   r"|\brunlog\b|\.run\.json|__<?\{?plans\}?>?__", re.I)

FIXTURE = "queue/testdata"
ALLOW = {"install.sh": "Xiangyu-Research/taskq", "README.md": "Xiangyu-Research/taskq"}  # 核心自己的仓库地址

bad = []
for f in CLEAN:
    for i, line in enumerate(open(os.path.join(ROOT, f), encoding="utf-8", errors="replace"), 1):
        line = line.replace(ALLOW[f], "") if f in ALLOW else line
        m = WORDS.search(line)
        if m:
            bad.append(f"{f}:{i}: {m.group(0)}")
print("\n".join(bad) if bad else f"PASS:{len(CLEAN)} 个文件都没有领域词")

# 核心（testdata 外）不直接用使用者的路径、仓库
HOST = re.compile(r"scripts/monitor|Xiangyu-Research/PhD|\.cache/phd|/work/PhD")
seen = {}
for f in CLEAN:
    if f.startswith(FIXTURE + "/"):
        continue
    n = sum(len(HOST.findall(line)) for line in open(os.path.join(ROOT, f), encoding="utf-8", errors="replace"))
    if n:
        seen[f] = n
print("\n".join(f"用了使用者的路径：{p}：{n} 处" for p, n in sorted(seen.items())) or "PASS:核心里没有使用者的路径")

# 核心的非测试文件（test_* 和 testdata 外）不写日期："（11/4）""10/31 起""2026-11-04"这类标注只说明了什么时候改的，
# 注释、说明只写现在是什么、为什么。月/日只认 1–12 / 1–31、前后不挨着字母数字和 / . : -（路径、版本号、比例不算）
DATE = re.compile(r"(?<![\w/.:-])(?:1[0-2]|[1-9])/(?:3[01]|[12][0-9]|[1-9])(?![\w/.])|(?<![0-9])20[0-9]{2}-[01][0-9]-[0-3][0-9](?![0-9])"
                  r"|[0-9]{1,2}月[0-9]{1,2}日|20[0-9]{2} ?年")
DATE_OK = {}  # 功能要的日期（格式说明、配置示例的值）：文件 → 放行的原文，像上面的 ALLOW
dated = []
for f in CLEAN:
    if os.path.basename(f).startswith("test_") or f.startswith(FIXTURE + "/"):
        continue
    for i, line in enumerate(open(os.path.join(ROOT, f), encoding="utf-8", errors="replace"), 1):
        line = line.replace(DATE_OK[f], "") if f in DATE_OK else line
        m = DATE.search(line)
        if m:
            dated.append(f"{f}:{i}: {m.group(0)}")
print("\n".join(f"写了日期：{h}" for h in dated) or "PASS:核心的非测试文件里没有日期")
sys.exit(1 if bad or seen or dated else 0)
