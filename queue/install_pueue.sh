#!/bin/bash
# 装 pueue / pueued（静态编译的单文件，不需要 root）：放在机器表里这台的 pueue（默认 ~/.local/bin/pueue）所在的目录。
#   bash queue/install_pueue.sh [版本，默认 v4.0.4]
set -euo pipefail
V=${1:-v4.0.4}
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)
D=$(dirname "$(python3 "$CORE/sites/mtab.py" get . pueue)")
mkdir -p $D
for b in pueue pueued; do
  curl -fsSL -o $D/$b.tmp https://github.com/Nukesor/pueue/releases/download/$V/$b-x86_64-unknown-linux-musl
  chmod +x $D/$b.tmp && mv $D/$b.tmp $D/$b
done
$D/pueue --version
