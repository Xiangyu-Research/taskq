#!/bin/bash
# 装 pueue / pueued（静态编译的单文件，放 ~/.local/bin，不需要 root），box 和 lab 都用这个。
#   bash scripts/taskq/queue/install_pueue.sh [版本，默认 v4.0.4]
set -euo pipefail
V=${1:-v4.0.4}
D=~/.local/bin
mkdir -p $D
for b in pueue pueued; do
  curl -fsSL -o $D/$b.tmp https://github.com/Nukesor/pueue/releases/download/$V/$b-x86_64-unknown-linux-musl
  chmod +x $D/$b.tmp && mv $D/$b.tmp $D/$b
done
$D/pueue --version
