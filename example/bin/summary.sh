#!/bin/bash
# 示例的训完以后的步骤：把一个任务的结果写成一行摘要。
#   bash bin/summary.sh <结果文件夹>
set -euo pipefail
echo "$(basename "$1")：$(cat "$1/ckpt.json")" > "$1/summary.txt"
