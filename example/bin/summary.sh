#!/bin/bash
# Example downstream step: write one task's result as one summary line.
#   bash bin/summary.sh <result dir> <summary dir>
set -euo pipefail
mkdir -p "$2"
echo "$(basename "$1"): $(cat "$1/ckpt.json")" > "$2/summary.txt"
