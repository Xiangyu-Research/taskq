#!/bin/bash
# Install pueue / pueued (statically built single files, no root needed) into the dir of this machine's pueue in the machine
# table (default ~/.local/bin/pueue). Each download is checked against its sha256 before it is installed: the default version's
# are pinned below; any other version needs both given on the command line (from a release you trust).
#   bash queue/install_pueue.sh [version, default v4.0.4] [sha256 of pueue] [sha256 of pueued]
set -euo pipefail
DEFAULT=v4.0.4
V=${1:-$DEFAULT}
declare -A SUM
if [[ $V == "$DEFAULT" ]]; then
  SUM[pueue]=c1b10d7e4e62211075ddd0e1dc3e8cbfc5a43d662cb3be7402a28504e23fcb51
  SUM[pueued]=5afeff6adbafb909e8d54e2caff158e6966c2adffa2c09e60fd631cc51b60390
fi
[[ -n ${2:-} ]] && SUM[pueue]=$2
[[ -n ${3:-} ]] && SUM[pueued]=$3
for b in pueue pueued; do
  [[ ${SUM[$b]:-} =~ ^[0-9a-f]{64}$ ]] || {
    echo "pueue $V: no sha256 known for $b; give both: bash $0 $V <sha256 of pueue> <sha256 of pueued>" >&2; exit 1; }
done
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)
D=$(dirname "$(python3 "$CORE/sites/mtab.py" get . pueue)")
mkdir -p "$D"
for b in pueue pueued; do
  curl -fsSL -o "$D/$b.tmp" "https://github.com/Nukesor/pueue/releases/download/$V/$b-x86_64-unknown-linux-musl"
  got=$(sha256sum "$D/$b.tmp" | cut -d' ' -f1)
  if [[ $got != "${SUM[$b]}" ]]; then
    rm -f "$D/pueue.tmp" "$D/pueued.tmp"
    echo "pueue $V: $b has sha256 $got, expected ${SUM[$b]}; not installed" >&2; exit 1
  fi
done
# both checked: only now replace the installed ones
for b in pueue pueued; do
  chmod +x "$D/$b.tmp" && mv "$D/$b.tmp" "$D/$b"
done
"$D/pueue" --version
